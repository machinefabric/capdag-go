package bifaci

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testHostManifest = `{"name":"Test","version":"1.0","caps":[{"urn":"cap:in=media:;out=media:"}]}`

// simulateCartridge runs a fake cartridge: handshake + handler on the cartridge side of a pipe.
// handler receives the FrameReader/FrameWriter after handshake and can read/write frames.
func simulateCartridge(t *testing.T, cartridgeRead, cartridgeWrite net.Conn, manifest string, handler func(*FrameReader, *FrameWriter)) {
	t.Helper()
	reader := NewFrameReader(cartridgeRead)
	writer := NewFrameWriter(cartridgeWrite)

	limits, err := HandshakeAccept(reader, writer, []byte(manifest))
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	if handler != nil {
		handler(reader, writer)
	}
}

// TEST413: RegisterCartridge adds entries to capTable
func Test413_register_cartridge_adds_cap_table(t *testing.T) {
	host := NewCartridgeHost()
	host.RegisterCartridge("/path/to/converter", []string{"cap:op=convert", "cap:op=analyze"})

	host.mu.Lock()
	defer host.mu.Unlock()

	assert.Equal(t, 2, len(host.capTable), "must have 2 cap table entries")
	assert.Equal(t, "cap:op=convert", host.capTable[0].capUrn)
	assert.Equal(t, 0, host.capTable[0].cartridgeIdx)
	assert.Equal(t, "cap:op=analyze", host.capTable[1].capUrn)
	assert.Equal(t, 0, host.capTable[1].cartridgeIdx)

	assert.Equal(t, 1, len(host.cartridges))
	assert.False(t, host.cartridges[0].running, "registered cartridge must not be running")
}

// TEST414: Capabilities() returns nil when no cartridges are running
func Test414_capabilities_empty_initially(t *testing.T) {
	// Case 1: No cartridges at all
	host := NewCartridgeHost()
	assert.Nil(t, host.Capabilities(), "no cartridges → nil capabilities")

	// Case 2: Cartridge registered but not running
	host.RegisterCartridge("/path/to/cartridge", []string{"cap:op=test"})
	assert.Nil(t, host.Capabilities(), "registered but not running → nil capabilities")
}

// TEST415: REQ for known cap triggers spawn (expect error for non-existent binary)
func Test415_req_triggers_spawn(t *testing.T) {
	host := NewCartridgeHost()
	host.RegisterCartridge("/nonexistent/cartridge/binary", []string{"cap:op=test"})

	// Set up relay pipes
	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()
	defer relayRead.Close()
	defer relayWrite.Close()

	// Engine sends REQ then closes
	go func() {
		writer := NewFrameWriter(engineWrite)
		reqId := NewMessageIdRandom()
		req := NewReq(reqId, "cap:op=test", []byte("hello"), "text/plain")
		writer.WriteFrame(req)

		// Read the ERR response
		reader := NewFrameReader(engineRead)
		frame, err := reader.ReadFrame()
		if err == nil {
			assert.Equal(t, FrameTypeErr, frame.FrameType)
			errCode := frame.ErrorCode()
			assert.Equal(t, "SPAWN_FAILED", errCode, "spawn of nonexistent binary must fail")
		}

		// Close relay to end Run()
		engineWrite.Close()
		engineRead.Close()
	}()

	err := host.Run(relayRead, relayWrite, nil)
	// Run returns when relay closes — nil is normal EOF
	_ = err
}

// TEST416: AttachCartridge performs HELLO handshake, extracts manifest, updates capabilities
func Test416_attach_cartridge_handshake(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","caps":[{"urn":"cap:in=media:;out=media:"}]}`

	hostRead, cartridgeWrite := net.Pipe()
	cartridgeRead, hostWrite := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeRead, cartridgeWrite, manifest, nil)
		cartridgeRead.Close()
		cartridgeWrite.Close()
	}()

	host := NewCartridgeHost()
	idx, err := host.AttachCartridge(hostRead, hostWrite)
	require.NoError(t, err)

	assert.Equal(t, 0, idx, "first attached cartridge is index 0")

	host.mu.Lock()
	assert.True(t, host.cartridges[0].running, "attached cartridge must be running")
	assert.Equal(t, []string{"cap:in=media:;out=media:"}, host.cartridges[0].caps)
	host.mu.Unlock()

	caps := host.Capabilities()
	assert.NotNil(t, caps, "running cartridge must produce capabilities")
	assert.Contains(t, string(caps), "cap:in=media:;out=media:")

	// Clean up
	hostRead.Close()
	hostWrite.Close()
	wg.Wait()
}

// TEST417: Route REQ to correct cartridge by cap_urn (two cartridges)
func Test417_route_req_by_cap_urn(t *testing.T) {
	manifestA := `{"name":"CartridgeA","version":"1.0","caps":[{"urn":"cap:op=convert"}]}`
	manifestB := `{"name":"CartridgeB","version":"1.0","caps":[{"urn":"cap:op=analyze"}]}`

	// Cartridge A pipes
	hostReadA, cartridgeWriteA := net.Pipe()
	cartridgeReadA, hostWriteA := net.Pipe()

	// Cartridge B pipes
	hostReadB, cartridgeWriteB := net.Pipe()
	cartridgeReadB, hostWriteB := net.Pipe()

	var wg sync.WaitGroup

	// Cartridge A: reads REQ+stream, responds with "converted"
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadA, cartridgeWriteA, manifestA, func(r *FrameReader, w *FrameWriter) {
			// Read REQ
			frame, err := r.ReadFrame()
			require.NoError(t, err)
			assert.Equal(t, FrameTypeReq, frame.FrameType)
			reqId := frame.Id

			// Read until END
			for {
				f, err := r.ReadFrame()
				if err != nil {
					break
				}
				if f.FrameType == FrameTypeEnd {
					break
				}
			}

			// Respond
			w.WriteFrame(NewEnd(reqId, []byte("converted")))
		})
		cartridgeReadA.Close()
		cartridgeWriteA.Close()
	}()

	// Cartridge B: just does handshake, expects no REQs, waits for close
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadB, cartridgeWriteB, manifestB, func(r *FrameReader, w *FrameWriter) {
			// Should get EOF (no frames sent to B)
			_, err := r.ReadFrame()
			assert.Error(t, err, "cartridge B must get EOF, not a frame")
		})
		cartridgeReadB.Close()
		cartridgeWriteB.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadA, hostWriteA)
	require.NoError(t, err)
	_, err = host.AttachCartridge(hostReadB, hostWriteB)
	require.NoError(t, err)

	// Relay pipes
	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	// Engine: send REQ for cap:op=convert, read response
	wg.Add(1)
	go func() {
		defer wg.Done()
		writer := NewFrameWriter(engineWrite)
		reader := NewFrameReader(engineRead)

		reqId := NewMessageIdRandom()
		writer.WriteFrame(NewReq(reqId, "cap:op=convert", []byte{}, "text/plain"))
		writer.WriteFrame(NewEnd(reqId, nil))

		// Read response
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		assert.Equal(t, FrameTypeEnd, frame.FrameType)
		assert.Equal(t, []byte("converted"), frame.Payload)

		// Close relay
		engineWrite.Close()
		engineRead.Close()
	}()

	host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()

	// Close host connections to Cartridge B to unblock its goroutine
	hostReadB.Close()
	hostWriteB.Close()
	hostReadA.Close()
	hostWriteA.Close()

	wg.Wait()
}

// TEST418: Route STREAM_START/CHUNK/STREAM_END/END by req_id
func Test418_route_continuation_by_req_id(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","caps":[{"urn":"cap:op=cont"}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			// Read REQ
			req, err := r.ReadFrame()
			require.NoError(t, err)
			assert.Equal(t, FrameTypeReq, req.FrameType)
			reqId := req.Id

			// Read STREAM_START
			ss, err := r.ReadFrame()
			require.NoError(t, err)
			assert.Equal(t, FrameTypeStreamStart, ss.FrameType)
			assert.Equal(t, reqId.ToString(), ss.Id.ToString())

			// Read CHUNK
			chunk, err := r.ReadFrame()
			require.NoError(t, err)
			assert.Equal(t, FrameTypeChunk, chunk.FrameType)
			assert.Equal(t, reqId.ToString(), chunk.Id.ToString())
			assert.Equal(t, []byte("payload-data"), chunk.Payload)

			// Read STREAM_END
			se, err := r.ReadFrame()
			require.NoError(t, err)
			assert.Equal(t, FrameTypeStreamEnd, se.FrameType)

			// Read END
			end, err := r.ReadFrame()
			require.NoError(t, err)
			assert.Equal(t, FrameTypeEnd, end.FrameType)

			// Respond
			w.WriteFrame(NewEnd(reqId, []byte("ok")))
		})
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	wg.Add(1)
	go func() {
		defer wg.Done()
		writer := NewFrameWriter(engineWrite)
		reader := NewFrameReader(engineRead)

		reqId := NewMessageIdRandom()
		writer.WriteFrame(NewReq(reqId, "cap:op=cont", []byte{}, "text/plain"))
		writer.WriteFrame(NewStreamStart(reqId, "arg-0", "media:"))
		payload := []byte("payload-data")
		checksum := ComputeChecksum(payload)
		writer.WriteFrame(NewChunk(reqId, "arg-0", 0, payload, 0, checksum))
		writer.WriteFrame(NewStreamEnd(reqId, "arg-0", 1))
		writer.WriteFrame(NewEnd(reqId, nil))

		// Read response
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		assert.Equal(t, FrameTypeEnd, frame.FrameType)
		assert.Equal(t, []byte("ok"), frame.Payload)

		engineWrite.Close()
		engineRead.Close()
	}()

	host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()
	hostReadP.Close()
	hostWriteP.Close()
	wg.Wait()
}

// TEST419: Cartridge HEARTBEAT handled locally (not forwarded to relay)
func Test419_heartbeat_local_handling(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","caps":[{"urn":"cap:op=hb"}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			// Send heartbeat
			hbId := NewMessageIdRandom()
			w.WriteFrame(NewHeartbeat(hbId))

			// Read heartbeat response from host
			resp, err := r.ReadFrame()
			require.NoError(t, err)
			assert.Equal(t, FrameTypeHeartbeat, resp.FrameType)
			assert.Equal(t, hbId.ToString(), resp.Id.ToString())

			// Now send a LOG to give engine something to read
			logId := NewMessageIdRandom()
			w.WriteFrame(NewLog(logId, "info", "heartbeat was answered"))
		})
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	var receivedTypes []FrameType

	wg.Add(1)
	go func() {
		defer wg.Done()
		reader := NewFrameReader(engineRead)
		for {
			frame, err := reader.ReadFrame()
			if err != nil {
				break
			}
			receivedTypes = append(receivedTypes, frame.FrameType)
		}
	}()

	// Let the host run for a short time to process events
	go func() {
		time.Sleep(500 * time.Millisecond)
		engineWrite.Close()
		engineRead.Close()
	}()

	host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()
	hostReadP.Close()
	hostWriteP.Close()
	wg.Wait()

	// HEARTBEAT must NOT appear in relay
	for _, ft := range receivedTypes {
		assert.NotEqual(t, FrameTypeHeartbeat, ft, "heartbeat must not be forwarded to relay")
	}
	// LOG must appear (proving the relay did receive forwarded frames)
	found := false
	for _, ft := range receivedTypes {
		if ft == FrameTypeLog {
			found = true
		}
	}
	assert.True(t, found, "LOG must be forwarded to relay")
}

// TEST420: Cartridge non-HELLO/non-HB frames forwarded to relay
func Test420_cartridge_frames_forwarded_to_relay(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","caps":[{"urn":"cap:op=fwd"}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			// Read REQ from host
			req, err := r.ReadFrame()
			if err != nil {
				return
			}
			reqId := req.Id

			// Read END
			r.ReadFrame()

			// Send diverse frame types
			w.WriteFrame(NewLog(reqId, "info", "processing"))
			w.WriteFrame(NewStreamStart(reqId, "output", "media:"))
			payload := []byte("data")
			checksum := ComputeChecksum(payload)
			w.WriteFrame(NewChunk(reqId, "output", 0, payload, 0, checksum))
			w.WriteFrame(NewStreamEnd(reqId, "output", 1))
			w.WriteFrame(NewEnd(reqId, nil))
		})
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	var receivedTypes []FrameType

	wg.Add(1)
	go func() {
		defer wg.Done()
		writer := NewFrameWriter(engineWrite)
		reader := NewFrameReader(engineRead)

		// Send REQ + END
		reqId := NewMessageIdRandom()
		writer.WriteFrame(NewReq(reqId, "cap:op=fwd", []byte{}, "text/plain"))
		writer.WriteFrame(NewEnd(reqId, nil))

		// Read all forwarded frames
		for {
			frame, err := reader.ReadFrame()
			if err != nil {
				break
			}
			receivedTypes = append(receivedTypes, frame.FrameType)
			if frame.FrameType == FrameTypeEnd {
				break
			}
		}

		engineWrite.Close()
		engineRead.Close()
	}()

	host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()
	hostReadP.Close()
	hostWriteP.Close()
	wg.Wait()

	// Verify forwarded types
	typeSet := make(map[FrameType]bool)
	for _, ft := range receivedTypes {
		typeSet[ft] = true
	}
	assert.True(t, typeSet[FrameTypeLog], "LOG must be forwarded")
	assert.True(t, typeSet[FrameTypeStreamStart], "STREAM_START must be forwarded")
	assert.True(t, typeSet[FrameTypeChunk], "CHUNK must be forwarded")
	assert.True(t, typeSet[FrameTypeEnd], "END must be forwarded")
}

// TEST421: Cartridge death updates capability list (removes dead cartridge's caps)
func Test421_cartridge_death_updates_caps(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","caps":[{"urn":"cap:op=die"}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup

	// Cartridge: handshake then immediately die
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, nil)
		// Die immediately after handshake
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	// Before death: caps must be present
	caps := host.Capabilities()
	assert.NotNil(t, caps)
	assert.Contains(t, string(caps), "cap:op=die")

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	// Let host process the death event briefly
	go func() {
		time.Sleep(500 * time.Millisecond)
		engineWrite.Close()
		engineRead.Close()
	}()

	host.Run(relayRead, relayWrite, nil)

	// After death: caps must be gone
	capsAfter := host.Capabilities()
	if capsAfter != nil {
		var parsed map[string][]string
		json.Unmarshal(capsAfter, &parsed)
		assert.Empty(t, parsed["caps"], "dead cartridge caps must be removed")
	}

	relayRead.Close()
	relayWrite.Close()
	hostReadP.Close()
	hostWriteP.Close()
	wg.Wait()
}

// TEST422: Cartridge death sends ERR for all pending requests
func Test422_cartridge_death_sends_err(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","caps":[{"urn":"cap:op=die"}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup

	// Cartridge: handshake, read REQ, then die
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			// Read REQ
			r.ReadFrame()
			// Die immediately without responding
			cartridgeReadP.Close()
			cartridgeWriteP.Close()
		})
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	var errFrame *Frame

	wg.Add(1)
	go func() {
		defer wg.Done()
		writer := NewFrameWriter(engineWrite)
		reader := NewFrameReader(engineRead)

		// Send REQ + END
		reqId := NewMessageIdRandom()
		writer.WriteFrame(NewReq(reqId, "cap:op=die", []byte("hello"), "text/plain"))
		writer.WriteFrame(NewEnd(reqId, nil))

		// Wait for ERR
		for {
			frame, err := reader.ReadFrame()
			if err != nil {
				break
			}
			if frame.FrameType == FrameTypeErr {
				errFrame = frame
				break
			}
		}

		engineWrite.Close()
		engineRead.Close()
	}()

	host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()
	hostReadP.Close()
	hostWriteP.Close()
	wg.Wait()

	require.NotNil(t, errFrame, "must receive ERR when cartridge dies with pending request")
	assert.Equal(t, "PLUGIN_DIED", errFrame.ErrorCode())
}

// TEST423: Multiple cartridges with distinct caps route independently
func Test423_multi_cartridge_distinct_caps(t *testing.T) {
	manifestA := `{"name":"CartridgeA","version":"1.0","caps":[{"urn":"cap:op=alpha"}]}`
	manifestB := `{"name":"CartridgeB","version":"1.0","caps":[{"urn":"cap:op=beta"}]}`

	// Cartridge A pipes
	hostReadA, cartridgeWriteA := net.Pipe()
	cartridgeReadA, hostWriteA := net.Pipe()

	// Cartridge B pipes
	hostReadB, cartridgeWriteB := net.Pipe()
	cartridgeReadB, hostWriteB := net.Pipe()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadA, cartridgeWriteA, manifestA, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			if err != nil {
				return
			}
			// Read until END
			for {
				f, err := r.ReadFrame()
				if err != nil || f.FrameType == FrameTypeEnd {
					break
				}
			}
			w.WriteFrame(NewEnd(req.Id, []byte("from-A")))
		})
		cartridgeReadA.Close()
		cartridgeWriteA.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadB, cartridgeWriteB, manifestB, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			if err != nil {
				return
			}
			for {
				f, err := r.ReadFrame()
				if err != nil || f.FrameType == FrameTypeEnd {
					break
				}
			}
			w.WriteFrame(NewEnd(req.Id, []byte("from-B")))
		})
		cartridgeReadB.Close()
		cartridgeWriteB.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadA, hostWriteA)
	require.NoError(t, err)
	_, err = host.AttachCartridge(hostReadB, hostWriteB)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	responses := make(map[string][]byte)
	var mu sync.Mutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		writer := NewFrameWriter(engineWrite)
		reader := NewFrameReader(engineRead)

		// Send REQ for alpha
		alphaId := NewMessageIdRandom()
		writer.WriteFrame(NewReq(alphaId, "cap:op=alpha", []byte{}, "text/plain"))
		writer.WriteFrame(NewEnd(alphaId, nil))

		// Send REQ for beta
		betaId := NewMessageIdRandom()
		writer.WriteFrame(NewReq(betaId, "cap:op=beta", []byte{}, "text/plain"))
		writer.WriteFrame(NewEnd(betaId, nil))

		// Read 2 responses
		for i := 0; i < 2; i++ {
			frame, err := reader.ReadFrame()
			if err != nil {
				break
			}
			if frame.FrameType == FrameTypeEnd {
				idStr := frame.Id.ToString()
				mu.Lock()
				if idStr == alphaId.ToString() {
					responses["alpha"] = frame.Payload
				} else if idStr == betaId.ToString() {
					responses["beta"] = frame.Payload
				}
				mu.Unlock()
			}
		}

		engineWrite.Close()
		engineRead.Close()
	}()

	host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()
	hostReadA.Close()
	hostWriteA.Close()
	hostReadB.Close()
	hostWriteB.Close()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []byte("from-A"), responses["alpha"])
	assert.Equal(t, []byte("from-B"), responses["beta"])
}

// TEST424: Concurrent requests to same cartridge handled independently
func Test424_concurrent_requests_same_cartridge(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","caps":[{"urn":"cap:op=conc"}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			// Read both REQs and ENDs, respond to each
			var reqIds []MessageId

			// Read REQ 0
			req0, err := r.ReadFrame()
			if err != nil {
				return
			}
			reqIds = append(reqIds, req0.Id)

			// Read END for req 0
			r.ReadFrame()

			// Read REQ 1
			req1, err := r.ReadFrame()
			if err != nil {
				return
			}
			reqIds = append(reqIds, req1.Id)

			// Read END for req 1
			r.ReadFrame()

			// Respond to each
			w.WriteFrame(NewEnd(reqIds[0], []byte("response-0")))
			w.WriteFrame(NewEnd(reqIds[1], []byte("response-1")))
		})
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	responses := make(map[string][]byte)
	var mu sync.Mutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		writer := NewFrameWriter(engineWrite)
		reader := NewFrameReader(engineRead)

		// Send two concurrent REQs
		id0 := NewMessageIdRandom()
		id1 := NewMessageIdRandom()

		writer.WriteFrame(NewReq(id0, "cap:op=conc", []byte{}, "text/plain"))
		writer.WriteFrame(NewEnd(id0, nil))

		writer.WriteFrame(NewReq(id1, "cap:op=conc", []byte{}, "text/plain"))
		writer.WriteFrame(NewEnd(id1, nil))

		// Read both responses
		for i := 0; i < 2; i++ {
			frame, err := reader.ReadFrame()
			if err != nil {
				break
			}
			if frame.FrameType == FrameTypeEnd {
				idStr := frame.Id.ToString()
				mu.Lock()
				if idStr == id0.ToString() {
					responses["0"] = frame.Payload
				} else if idStr == id1.ToString() {
					responses["1"] = frame.Payload
				}
				mu.Unlock()
			}
		}

		engineWrite.Close()
		engineRead.Close()
	}()

	host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()
	hostReadP.Close()
	hostWriteP.Close()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []byte("response-0"), responses["0"])
	assert.Equal(t, []byte("response-1"), responses["1"])
}

// TEST425: FindCartridgeForCap returns false for unknown cap
func Test425_find_cartridge_for_cap_unknown(t *testing.T) {
	host := NewCartridgeHost()
	host.RegisterCartridge("/path/to/cartridge", []string{"cap:op=known"})

	idx, found := host.FindCartridgeForCap("cap:op=known")
	assert.True(t, found, "known cap must be found")
	assert.Equal(t, 0, idx)

	_, found = host.FindCartridgeForCap("cap:op=unknown")
	assert.False(t, found, "unknown cap must not be found")
}
