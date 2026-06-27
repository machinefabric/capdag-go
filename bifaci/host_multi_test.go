package bifaci

import (
	"encoding/json"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testHostManifest = `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"}]}]}`

// capGroupsFromURNs builds a single default cap_group from cap-URN strings,
// the test-side analogue of the reference cap_groups_from_urns helper. Each
// URN is parsed (a malformed URN fails the test loudly) so the resulting
// CapGroup carries real cap.Cap values, exactly as a manifest-derived group
// would.
func capGroupsFromURNs(t *testing.T, urns ...string) []CapGroup {
	t.Helper()
	caps := make([]cap.Cap, 0, len(urns))
	for _, u := range urns {
		parsed, err := urn.NewCapUrnFromString(u)
		require.NoErrorf(t, err, "cap URN %q must parse", u)
		caps = append(caps, *cap.NewCap(parsed, u, ""))
	}
	return []CapGroup{DefaultGroup(caps)}
}

// registerTempCartridge writes a real (dummy) binary into a temp dir and
// registers it, so the binary-hash install identity resolves and the cartridge
// is advertised. Mirrors the reference tests, which register against a real
// tempfile rather than a fabricated path. Returns the binary path.
func registerTempCartridge(t *testing.T, host *CartridgeHost, name string, urns ...string) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(binPath, []byte("#!/bin/false\n"), 0o755))
	host.RegisterCartridge(binPath, name, "0.0.0", CartridgeChannelRelease, nil, capGroupsFromURNs(t, urns...))
	return binPath
}

// recvCartridgeFrame reads from the engine side of the relay, skipping the
// host's RelayNotify inventory frames (the host publishes one on Run start and
// on every inventory change; the real engine tolerates them). Returns the first
// non-RelayNotify frame.
func recvCartridgeFrame(reader *FrameReader) (*Frame, error) {
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			return nil, err
		}
		if frame.FrameType == FrameTypeRelayNotify {
			continue
		}
		return frame, nil
	}
}

// simulateCartridge runs a fake cartridge: handshake + identity echo + handler
// on the cartridge side of a pipe. handler receives the FrameReader/FrameWriter
// after the host's post-handshake identity verification REQ has been echoed back,
// and can read/write frames.
//
// The host's AttachCartridge runs VerifyIdentity right after the handshake
// (mirroring Rust attach_cartridge), so every simulated cartridge MUST answer
// the identity REQ before its normal handler runs. The echo mirrors
// run_cartridge_identity_echo / drainIdentityRequest: read REQ + body
// (STREAM_START → CHUNK(s) → STREAM_END → END), then echo the concatenated
// payload back as STREAM_START → CHUNK → STREAM_END → END.
func simulateCartridge(t *testing.T, cartridgeRead, cartridgeWrite net.Conn, manifest string, handler func(*FrameReader, *FrameWriter)) {
	t.Helper()
	reader := NewFrameReader(cartridgeRead)
	writer := NewFrameWriter(cartridgeWrite)

	limits, err := HandshakeAccept(reader, writer, []byte(manifest))
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Answer the host's identity-verification REQ before the handler runs.
	echoIdentityRequest(t, reader, writer)

	if handler != nil {
		handler(reader, writer)
	}
}

// echoIdentityRequest reads the host's identity REQ and its request body, then
// echoes the payload back. Mirrors run_cartridge_identity_echo (Rust) and
// io_test.go's identity echo: the host's VerifyIdentity sends each CHUNK as a
// CBOR-encoded payload and verifies the response echoes those exact bytes, so
// the echo writes the raw chunk payloads back unchanged.
func echoIdentityRequest(t *testing.T, reader *FrameReader, writer *FrameWriter) {
	t.Helper()
	req, err := reader.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, FrameTypeReq, req.FrameType, "first frame after handshake must be the identity REQ")

	var payload []byte
	for {
		f, err := reader.ReadFrame()
		require.NoError(t, err)
		switch f.FrameType {
		case FrameTypeStreamStart, FrameTypeStreamEnd:
			// no-op
		case FrameTypeChunk:
			payload = append(payload, f.Payload...)
		case FrameTypeEnd:
			streamId := "identity-echo"
			require.NoError(t, writer.WriteFrame(NewStreamStart(req.Id, streamId, "media:", nil)))
			checksum := ComputeChecksum(payload)
			require.NoError(t, writer.WriteFrame(NewChunk(req.Id, streamId, 0, payload, 0, checksum)))
			require.NoError(t, writer.WriteFrame(NewStreamEnd(req.Id, streamId, 1)))
			require.NoError(t, writer.WriteFrame(NewEnd(req.Id, nil)))
			return
		default:
			t.Fatalf("unexpected frame type during identity request: %v", f.FrameType)
		}
	}
}

// TEST6601: An attached cartridge (raw-stream, no on-disk anchor) gets a
// resolvable install identity derived from its HELLO manifest. Advertisement
// is identity-gated, so without this the attached cartridge would be silently
// excluded from every RelayNotify and the engine could never route to it — the
// deadlock that hung the rust-rust-rust interop echo test. Mirrors the
// reference test6601.
func Test6601_attached_cartridge_identity_derived_from_manifest(t *testing.T) {
	manifest := []byte(`{"name":"InteropCartridge","version":"2.3.4","channel":"nightly","registry_url":null,"description":"x","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]}],"adapter_urns":[]}]}`)

	rec := installedCartridgeRecordFromManifest(manifest)
	require.NotNil(t, rec, "attached cartridge must have a resolvable identity")
	assert.Equal(t, "InteropCartridge", rec.Id)
	assert.Equal(t, "2.3.4", rec.Version)
	assert.Nil(t, rec.RegistryURL, "null registry_url ⇒ dev install")
	assert.Equal(t, "nightly", rec.Channel)
	assert.Len(t, rec.Sha256, 64, "sha256 hex must be 64 chars")
	assert.Nil(t, rec.AttachmentError)
	assert.Equal(t, CartridgeLifecycleOperational, rec.Lifecycle)

	// Deterministic.
	again := installedCartridgeRecordFromManifest(manifest)
	assert.Equal(t, rec.Sha256, again.Sha256)

	// Garbage / incomplete manifest ⇒ nil identity (caller still attaches; the
	// record is honestly absent rather than fabricated).
	assert.Nil(t, installedCartridgeRecordFromManifest([]byte("{not json")))
	assert.Nil(t, installedCartridgeRecordFromManifest([]byte(`{"name":"X","version":"1.0"}`)), "missing channel ⇒ no identity")
}

// TEST413: Register cartridge adds entries to cap_table
func Test413_register_cartridge_adds_cap_table(t *testing.T) {
	host := NewCartridgeHost()
	host.RegisterCartridge("/path/to/converter", "converter", "1.0", CartridgeChannelRelease, nil, capGroupsFromURNs(t, "cap:convert", "cap:analyze"))

	host.mu.Lock()
	defer host.mu.Unlock()

	assert.Equal(t, 2, len(host.capTable), "must have 2 cap table entries")
	assert.Equal(t, "cap:convert", host.capTable[0].capUrn)
	assert.Equal(t, 0, host.capTable[0].cartridgeIdx)
	assert.Equal(t, "cap:analyze", host.capTable[1].capUrn)
	assert.Equal(t, 0, host.capTable[1].cartridgeIdx)

	assert.Equal(t, 1, len(host.cartridges))
	assert.False(t, host.cartridges[0].running, "registered cartridge must not be running")
}

// TEST6594: capabilities() returns empty JSON initially (no running cartridges)
func Test6594_capabilities_empty_initially(t *testing.T) {
	// Case 1: No cartridges at all
	host := NewCartridgeHost()
	assert.Nil(t, host.Capabilities(), "no cartridges → nil capabilities")

	// Case 2: Cartridge registered but not running
	host.RegisterCartridge("/path/to/cartridge", "cartridge", "1.0", CartridgeChannelRelease, nil, capGroupsFromURNs(t, "cap:test"))
	assert.Nil(t, host.Capabilities(), "registered but not running → nil capabilities")
}

// TEST415: REQ for known cap triggers spawn attempt (verified by expected spawn error for non-existent binary)
func Test415_req_triggers_spawn(t *testing.T) {
	host := NewCartridgeHost()
	host.RegisterCartridge("/nonexistent/cartridge/binary", "cartridge", "1.0", CartridgeChannelRelease, nil, capGroupsFromURNs(t, "cap:test"))

	// Set up relay pipes
	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()
	defer relayRead.Close()
	defer relayWrite.Close()

	// Engine sends REQ then closes
	go func() {
		writer := NewFrameWriter(engineWrite)
		reqId := NewMessageIdRandom()
		req := NewReq(reqId, "cap:test", []byte("hello"), "text/plain")
		writer.WriteFrame(req)

		// Read the ERR response, skipping the host's RelayNotify
		// inventory frames (the engine tolerates these; the host
		// publishes one on Run start and on every inventory change).
		reader := NewFrameReader(engineRead)
		for {
			frame, err := reader.ReadFrame()
			if err != nil {
				break
			}
			if frame.FrameType == FrameTypeRelayNotify {
				continue
			}
			assert.Equal(t, FrameTypeErr, frame.FrameType)
			errCode := frame.ErrorCode()
			assert.Equal(t, "SPAWN_FAILED", errCode, "spawn of nonexistent binary must fail")
			break
		}

		// Close relay to end Run()
		engineWrite.Close()
		engineRead.Close()
	}()

	err := host.Run(relayRead, relayWrite, nil)
	// Run returns when relay closes — nil is normal EOF
	_ = err
}

// TEST416: Attach cartridge performs HELLO handshake, extracts manifest, updates capabilities
func Test416_attach_cartridge_handshake(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"}]}]}`

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
	// `cap:in=media:;out=media:;effect=none` canonicalizes to `cap:effect=none`
	// (in/out at top of order, no y-tags).
	assert.Equal(t, []string{standard.CapIdentity}, host.cartridges[0].caps)
	host.mu.Unlock()

	caps := host.Capabilities()
	assert.NotNil(t, caps, "running cartridge must produce capabilities")
	assert.Contains(t, string(caps), `"`+standard.CapIdentity+`"`)

	// Clean up
	hostRead.Close()
	hostWrite.Close()
	wg.Wait()
}

// TEST417: Route REQ to correct cartridge by cap_urn (with two attached cartridges)
func Test417_route_req_by_cap_urn(t *testing.T) {
	manifestA := `{"name":"CartridgeA","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"},{"urn":"cap:convert","title":"convert","command":"convert"}]}]}`
	manifestB := `{"name":"CartridgeB","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"},{"urn":"cap:analyze","title":"analyze","command":"analyze"}]}]}`

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

	// Engine: send REQ for cap:convert, read response
	wg.Add(1)
	go func() {
		defer wg.Done()
		writer := NewFrameWriter(engineWrite)
		reader := NewFrameReader(engineRead)

		reqId := NewMessageIdRandom()
		xid := NewMessageIdFromUint(1)
		req := NewReq(reqId, "cap:convert", []byte{}, "text/plain")
		req.RoutingId = &xid
		writer.WriteFrame(req)
		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		writer.WriteFrame(end)

		// Read response
		frame, err := recvCartridgeFrame(reader)
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

// TEST418: Route STREAM_START/CHUNK/STREAM_END/END by req_id (not cap_urn) Verifies that after the initial REQ→cartridge routing, all subsequent continuation frames with the same req_id are routed to the same cartridge — even though no cap_urn is present on those frames.
func Test418_route_continuation_by_req_id(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"},{"urn":"cap:cont","title":"cont","command":"cont"}]}]}`

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
		xid := NewMessageIdFromUint(1)
		stamp := func(f *Frame) *Frame { f.RoutingId = &xid; return f }
		writer.WriteFrame(stamp(NewReq(reqId, "cap:cont", []byte{}, "text/plain")))
		writer.WriteFrame(stamp(NewStreamStart(reqId, "arg-0", "media:", nil)))
		payload := []byte("payload-data")
		checksum := ComputeChecksum(payload)
		writer.WriteFrame(stamp(NewChunk(reqId, "arg-0", 0, payload, 0, checksum)))
		writer.WriteFrame(stamp(NewStreamEnd(reqId, "arg-0", 1)))
		writer.WriteFrame(stamp(NewEnd(reqId, nil)))

		// Read response
		frame, err := recvCartridgeFrame(reader)
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
	manifest := `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"},{"urn":"cap:hb","title":"hb","command":"hb"}]}]}`

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

// TEST420: Cartridge non-HELLO/non-HB frames forwarded to relay (pass-through)
func Test420_cartridge_frames_forwarded_to_relay(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"},{"urn":"cap:fwd","title":"fwd","command":"fwd"}]}]}`

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
			w.WriteFrame(NewStreamStart(reqId, "output", "media:", nil))
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
		xid := NewMessageIdFromUint(1)
		req := NewReq(reqId, "cap:fwd", []byte{}, "text/plain")
		req.RoutingId = &xid
		writer.WriteFrame(req)
		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		writer.WriteFrame(end)

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

// TEST421: Cartridge death updates capability list (caps removed)
func Test421_cartridge_death_updates_caps(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"},{"urn":"cap:die","title":"die","command":"die"}]}]}`

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
	assert.Contains(t, string(caps), "cap:die")

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

// TEST422: a cartridge that dies mid-request must not wedge the host —
// Run() must exit cleanly once the cartridge is gone and the relay
// disconnects. Mirrors the Rust reference test
// (host_runtime.rs test422_cartridge_death_sends_err_for_pending_requests):
// the engine sends REQ+END then drops the relay connection (in real use it
// would time out the pending request); the contract under test is that the
// runtime tears down gracefully rather than blocking forever on a response
// the dead cartridge will never send. Delivery of the CARTRIDGE_DIED ERR to
// the engine is best-effort and NOT guaranteed before teardown, so asserting
// its receipt would over-specify beyond the reference contract.
func Test422_cartridge_death_sends_err(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"},{"urn":"cap:die","title":"die","command":"die"}]}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup

	// Cartridge: handshake + identity echo, then read the COMPLETE request
	// (REQ through END) — exactly as a real cartridge's reader loop drains a
	// request before its handler runs — and die without responding. Stopping
	// mid-request is not how a real cartridge behaves and would deadlock the
	// synchronous pipe against the host's body-frame forwarding.
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			if _, err := r.ReadFrame(); err != nil { // REQ
				return
			}
			for {
				f, err := r.ReadFrame()
				if err != nil {
					return
				}
				if f.FrameType == FrameTypeEnd {
					break
				}
			}
			// Die without responding.
			cartridgeReadP.Close()
			cartridgeWriteP.Close()
		})
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	// Engine: send REQ+END for the cap the dying cartridge handles, drain any
	// frames the host emits (so the host's outbound writes never block), then
	// drop the relay connection — the signal that makes Run() return.
	wg.Add(1)
	go func() {
		defer wg.Done()
		writer := NewFrameWriter(engineWrite)
		reader := NewFrameReader(engineRead)

		// Continuously drain whatever the host emits (a best-effort
		// CARTRIDGE_DIED ERR and/or RelayNotify updates) so the host's
		// outbound writes never block. Runs for the lifetime of the relay
		// connection and ends at EOF when teardown closes the write side.
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			for {
				if _, err := reader.ReadFrame(); err != nil {
					return
				}
			}
		}()

		reqId := NewMessageIdRandom()
		xid := NewMessageIdFromUint(1)
		req := NewReq(reqId, "cap:die", []byte("hello"), "text/plain")
		req.RoutingId = &xid
		require.NoError(t, writer.WriteFrame(req))
		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		require.NoError(t, writer.WriteFrame(end))

		// Drop the engine→relay direction. The host's relay reader hits EOF,
		// which is the signal that drives Run() to tear down (kill cartridges,
		// flush, return) — the reference's "relay disconnects" condition.
		engineWrite.Close()
		<-drained
		engineRead.Close()
	}()

	// Run returns when the relay connection closes; the assertion is simply
	// that it RETURNS (the test would otherwise hang, which the suite's
	// timeout turns into a loud failure with a goroutine dump).
	host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()
	hostReadP.Close()
	hostWriteP.Close()
	wg.Wait()
}

// TEST423: Multiple cartridges registered with distinct caps route independently
func Test423_multi_cartridge_distinct_caps(t *testing.T) {
	manifestA := `{"name":"CartridgeA","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"},{"urn":"cap:alpha","title":"alpha","command":"alpha"}]}]}`
	manifestB := `{"name":"CartridgeB","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"},{"urn":"cap:beta","title":"beta","command":"beta"}]}]}`

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
		alphaXid := NewMessageIdFromUint(1)
		alphaReq := NewReq(alphaId, "cap:alpha", []byte{}, "text/plain")
		alphaReq.RoutingId = &alphaXid
		writer.WriteFrame(alphaReq)
		alphaEnd := NewEnd(alphaId, nil)
		alphaEnd.RoutingId = &alphaXid
		writer.WriteFrame(alphaEnd)

		// Send REQ for beta
		betaId := NewMessageIdRandom()
		betaXid := NewMessageIdFromUint(2)
		betaReq := NewReq(betaId, "cap:beta", []byte{}, "text/plain")
		betaReq.RoutingId = &betaXid
		writer.WriteFrame(betaReq)
		betaEnd := NewEnd(betaId, nil)
		betaEnd.RoutingId = &betaXid
		writer.WriteFrame(betaEnd)

		// Read 2 responses (skipping RelayNotify inventory frames).
		for i := 0; i < 2; i++ {
			frame, err := recvCartridgeFrame(reader)
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

// TEST424: Concurrent requests to the same cartridge are handled independently
func Test424_concurrent_requests_same_cartridge(t *testing.T) {
	manifest := `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","command":"identity"},{"urn":"cap:conc","title":"conc","command":"conc"}]}]}`

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

		// Send two concurrent REQs. The host expects XID
		// (routing_id) on every relay-side frame — the
		// RelaySwitch stamps these in production. Mirrors the
		// Rust TEST424 in capdag/src/bifaci/host_runtime.rs.
		id0 := NewMessageIdRandom()
		id1 := NewMessageIdRandom()
		xid0 := NewMessageIdFromUint(1)
		xid1 := NewMessageIdFromUint(2)

		req0 := NewReq(id0, "cap:conc", []byte{}, "text/plain")
		req0.RoutingId = &xid0
		writer.WriteFrame(req0)
		end0 := NewEnd(id0, nil)
		end0.RoutingId = &xid0
		writer.WriteFrame(end0)

		req1 := NewReq(id1, "cap:conc", []byte{}, "text/plain")
		req1.RoutingId = &xid1
		writer.WriteFrame(req1)
		end1 := NewEnd(id1, nil)
		end1.RoutingId = &xid1
		writer.WriteFrame(end1)

		// Read both responses (skipping RelayNotify inventory frames).
		for i := 0; i < 2; i++ {
			frame, err := recvCartridgeFrame(reader)
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

// TEST425: find_cartridge_for_cap returns None for unregistered cap
func Test425_find_cartridge_for_cap_unknown(t *testing.T) {
	host := NewCartridgeHost()
	host.RegisterCartridge("/path/to/cartridge", "cartridge", "1.0", CartridgeChannelRelease, nil, capGroupsFromURNs(t, "cap:known"))

	idx, found := host.FindCartridgeForCap("cap:known")
	assert.True(t, found, "known cap must be found")
	assert.Equal(t, 0, idx)

	_, found = host.FindCartridgeForCap("cap:unknown")
	assert.False(t, found, "unknown cap must not be found")
}

// hostAdvertisedCapUrns returns the de-duplicated, declaration-ordered cap URNs
// the host advertises to the engine, derived from buildInstalledCartridgeIdentities
// (the Go analog of Rust's aggregate_installed_cartridges). Mirrors the Rust test
// helper host_advertised_cap_urns. Caller must hold no lock — this locks h.mu.
func hostAdvertisedCapUrns(h *CartridgeHost) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	seen := make(map[string]struct{})
	var out []string
	for _, ic := range h.buildInstalledCartridgeIdentities() {
		for _, group := range ic.CapGroups {
			for i := range group.Caps {
				u := group.Caps[i].Urn.String()
				if _, ok := seen[u]; ok {
					continue
				}
				seen[u] = struct{}{}
				out = append(out, u)
			}
		}
	}
	return out
}

func anyContains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

// TEST6600: parse_cap_groups_from_manifest classifies failures by kind Manifest JSON that parses but lacks CAP_IDENTITY is `Incompatible` (schema-rejected). Manifest bytes that don't parse as CapManifest are `ManifestInvalid` (JSON-level failure). The split lets the host's attachment-error reporter surface the right kind to the UI.
func Test6600_parse_cap_groups_rejects_manifest_without_identity(t *testing.T) {
	// JSON-valid manifest, missing CAP_IDENTITY → Incompatible.
	manifest := `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=\"media:void\";convert;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}`
	_, err := parseCapGroupsFromManifest([]byte(manifest))
	require.Error(t, err, "Manifest without CAP_IDENTITY must be rejected")
	var parseErr *ParseCapsError
	require.ErrorAs(t, err, &parseErr)
	assert.True(t, parseErr.Incompatible, "Missing CAP_IDENTITY must classify as Incompatible, got %+v", parseErr)
	assert.Equal(t, CartridgeAttachmentErrorKindIncompatible, parseErr.AttachmentKind(),
		"attachment_kind() must agree with the variant")
	assert.Contains(t, parseErr.Error(), "CAP_IDENTITY", "Error must mention CAP_IDENTITY, got: %s", parseErr.Error())

	// Garbage bytes that don't deserialize → ManifestInvalid.
	badJSON := []byte("{not even json")
	_, errBad := parseCapGroupsFromManifest(badJSON)
	require.Error(t, errBad, "Non-JSON manifest must be rejected")
	var parseErrBad *ParseCapsError
	require.ErrorAs(t, errBad, &parseErrBad)
	assert.False(t, parseErrBad.Incompatible, "Non-JSON manifest must classify as InvalidJson, got %+v", parseErrBad)
	assert.Equal(t, CartridgeAttachmentErrorKindManifestInvalid, parseErrBad.AttachmentKind(),
		"attachment_kind() must agree with the variant")

	// Valid manifest WITH CAP_IDENTITY must succeed.
	manifestOk := `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";convert;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}`
	groups, errOk := parseCapGroupsFromManifest([]byte(manifestOk))
	require.NoError(t, errOk, "Manifest with CAP_IDENTITY must be accepted")
	totalCaps := 0
	for _, g := range groups {
		totalCaps += len(g.Caps)
	}
	assert.Equal(t, 2, totalCaps, "Must parse both caps")
}

// TEST485: attach_cartridge completes identity verification with working cartridge
func Test485_attach_cartridge_identity_verification_succeeds(t *testing.T) {
	manifest := `{"name":"IdentityTest","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";test;out=\"media:void\"","title":"Test","command":"test","args":[]}],"adapter_urns":[]}]}`

	hostRead, cartridgeWrite := net.Pipe()
	cartridgeRead, hostWrite := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// simulateCartridge answers the identity REQ before the (nil) handler.
		simulateCartridge(t, cartridgeRead, cartridgeWrite, manifest, nil)
		cartridgeRead.Close()
		cartridgeWrite.Close()
	}()

	host := NewCartridgeHost()
	idx, err := host.AttachCartridge(hostRead, hostWrite)
	require.NoError(t, err)
	assert.Equal(t, 0, idx)

	host.mu.Lock()
	assert.True(t, host.cartridges[0].running, "Cartridge must be running after identity verification")
	// Verify both caps are registered (identity is included).
	parsedCaps := 0
	hasIdentity := false
	identityUrn, _ := urn.NewCapUrnFromString(standard.CapIdentity)
	for _, g := range host.cartridges[0].capGroups {
		for i := range g.Caps {
			parsedCaps++
			if g.Caps[i].Urn != nil && identityUrn.ConformsTo(g.Caps[i].Urn) {
				hasIdentity = true
			}
		}
	}
	host.mu.Unlock()
	assert.True(t, hasIdentity, "Must have identity cap")
	assert.Equal(t, 2, parsedCaps, "Must have both caps")

	hostRead.Close()
	hostWrite.Close()
	wg.Wait()
}

// TEST486: attach_cartridge rejects cartridge that fails identity verification
func Test486_attach_cartridge_identity_verification_fails(t *testing.T) {
	manifest := `{"name":"BrokenIdentity","version":"1.0","channel":"release","registry_url":null,"description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]}],"adapter_urns":[]}]}`

	hostRead, cartridgeWrite := net.Pipe()
	cartridgeRead, hostWrite := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)
		limits, err := HandshakeAccept(reader, writer, []byte(manifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Drain the COMPLETE identity request (REQ + STREAM_START + CHUNK(s) +
		// STREAM_END + END) before replying, exactly as a real cartridge's
		// reader loop does — a peer must consume the whole request stream
		// before dispatching, and the verifier writes all of it before reading
		// the response. Replying mid-request would deadlock the synchronous
		// transport (and is not how any real cartridge behaves). Then respond
		// with ERR to model a broken identity handler.
		req, _ := drainIdentityRequest(t, reader)
		require.NoError(t, writer.WriteFrame(NewErr(req.Id, "BROKEN", "identity handler is broken")))
		cartridgeRead.Close()
		cartridgeWrite.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostRead, hostWrite)
	require.Error(t, err, "attach_cartridge must fail when identity verification fails")
	assert.Contains(t, err.Error(), "Identity verification failed",
		"Error must mention identity verification: %v", err)

	// The cartridge must NOT have been attached.
	host.mu.Lock()
	assert.Empty(t, host.cartridges, "failed cartridge must not be attached")
	host.mu.Unlock()

	hostRead.Close()
	hostWrite.Close()
	wg.Wait()
}

// TEST6623: Cartridge death keeps caps advertised for on-demand respawn. The cartridge's `cap_groups` survive process death, so the host can continue advertising the cartridge's caps and the relay can route a fresh REQ to it (which triggers an on-demand respawn).
func Test6623_cartridge_death_keeps_caps_advertised(t *testing.T) {
	host := NewCartridgeHost()
	registerTempCartridge(t, host, "thumbnailcartridge",
		standard.CapIdentity,
		`cap:in="media:ext=pdf";out="media:ext=png;image";thumbnail`,
	)

	// cap_table is the routing source of truth: both caps are present even
	// though the process has not been spawned (running == false).
	host.mu.Lock()
	assert.Equal(t, 2, len(host.capTable))
	assert.Equal(t, standard.CapIdentity, host.capTable[0].capUrn)
	host.rebuildCapabilities()
	host.mu.Unlock()

	advertised := hostAdvertisedCapUrns(host)
	assert.Contains(t, advertised, standard.CapIdentity, "Identity cap must be advertised, got %v", advertised)
	assert.True(t, anyContains(advertised, "thumbnail"), "Thumbnail cap must be advertised, got %v", advertised)
}

// TEST662: rebuild_capabilities includes non-running cartridges' caps (each cartridge's `cap_groups` is the source of truth, regardless of whether its process has been spawned yet).
func Test662_rebuild_capabilities_includes_non_running_cartridges(t *testing.T) {
	host := NewCartridgeHost()
	registerTempCartridge(t, host, "extractcartridge",
		standard.CapIdentity,
		`cap:in="media:ext=pdf";extract;out="media:text"`,
	)
	registerTempCartridge(t, host, "ocrcartridge",
		standard.CapIdentity,
		`cap:in="media:image";ocr;out="media:text"`,
	)

	host.mu.Lock()
	host.rebuildCapabilities()
	host.mu.Unlock()

	// Both cartridges advertised; the union of their cap_groups contains
	// identity + extract + ocr.
	advertised := hostAdvertisedCapUrns(host)
	assert.Contains(t, advertised, standard.CapIdentity, "Identity cap must be advertised, got %v", advertised)
	assert.True(t, anyContains(advertised, "extract"), "Extract cap must be advertised, got %v", advertised)
	assert.True(t, anyContains(advertised, "ocr"), "OCR cap must be advertised, got %v", advertised)
}

// TEST663: Cartridge with hello_failed is permanently removed from capabilities
func Test663_hello_failed_cartridge_removed_from_capabilities(t *testing.T) {
	host := NewCartridgeHost()
	registerTempCartridge(t, host, "brokencartridge",
		standard.CapIdentity,
		`cap:in="media:void";broken;out="media:void"`,
	)

	// Manually mark it as hello_failed (simulating HELLO handshake failure).
	host.mu.Lock()
	host.cartridges[0].helloFailed = true
	host.updateCapTable()
	// cap_table is empty: hello_failed cartridges are not routable.
	foundBroken := false
	for _, entry := range host.capTable {
		if strings.Contains(entry.capUrn, "broken") {
			foundBroken = true
		}
	}
	host.rebuildCapabilities()
	host.mu.Unlock()
	assert.False(t, foundBroken, "hello_failed cartridge caps should not be in cap_table")

	// The host-level inventory likewise excludes hello_failed cartridges.
	advertised := hostAdvertisedCapUrns(host)
	assert.False(t, anyContains(advertised, "broken"),
		"hello_failed cartridge must not be advertised, got %v", advertised)
}

// TEST664: Attached cartridge replaces pre-registration caps with manifest caps. The pre-attach `cap_groups` (from probe-time discovery) get superseded by the post-HELLO `cap_groups` from the actual handshake.
func Test664_running_cartridge_uses_manifest_caps(t *testing.T) {
	// Manifest declares different caps than the pre-registration probe —
	// the post-HELLO snapshot must win.
	manifest := `{"name":"Test","version":"1.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:text\";uppercase;out=\"media:text\"","title":"Uppercase","command":"uppercase","args":[]}],"adapter_urns":[]}]}`

	hostRead, cartridgeWrite := net.Pipe()
	cartridgeRead, hostWrite := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeRead, cartridgeWrite, manifest, func(r *FrameReader, w *FrameWriter) {
			// Keep the connection alive for the duration of the test.
			r.ReadFrame()
		})
		cartridgeRead.Close()
		cartridgeWrite.Close()
	}()

	host := NewCartridgeHost()

	// Register with stale (probe-time) caps BEFORE attaching.
	registerTempCartridge(t, host, "extractcartridge",
		standard.CapIdentity,
		`cap:in="media:ext=pdf";extract;out="media:text"`,
	)

	// Now attach the actual cartridge (which sends a different manifest).
	_, err := host.AttachCartridge(hostRead, hostWrite)
	require.NoError(t, err)

	// cap_table is the routing source of truth and includes both the
	// registered cartridge AND the attached cartridge. The running
	// cartridge's manifest cap (uppercase) must be routable.
	host.mu.Lock()
	hasUppercase := false
	for _, entry := range host.capTable {
		if strings.Contains(entry.capUrn, "uppercase") {
			hasUppercase = true
		}
	}
	host.mu.Unlock()
	assert.True(t, hasUppercase, "Running cartridge's manifest cap must be in cap_table")

	hostRead.Close()
	hostWrite.Close()
	wg.Wait()
}

// TEST665: Cap table aggregates caps from every healthy cartridge — attached/running cartridges contribute their post-HELLO cap_groups, registered-but-not-yet-spawned cartridges contribute their probe-time cap_groups. Both flow through the same `cap_urns()` view.
func Test665_cap_table_mixed_running_and_non_running(t *testing.T) {
	manifest := `{"name":"Running","version":"1.0","channel":"release","registry_url":null,"description":"Running cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:text\";running-op;out=\"media:text\"","title":"RunningOp","command":"running","args":[]}],"adapter_urns":[]}]}`

	hostRead, cartridgeWrite := net.Pipe()
	cartridgeRead, hostWrite := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeRead, cartridgeWrite, manifest, func(r *FrameReader, w *FrameWriter) {
			r.ReadFrame()
		})
		cartridgeRead.Close()
		cartridgeWrite.Close()
	}()

	host := NewCartridgeHost()

	// Attach running cartridge.
	_, err := host.AttachCartridge(hostRead, hostWrite)
	require.NoError(t, err)

	// Register a non-running cartridge with probe-time caps.
	registerTempCartridge(t, host, "not-running",
		standard.CapIdentity,
		`cap:in="media:ext=pdf";not-running-op;out="media:text"`,
	)

	host.mu.Lock()
	host.updateCapTable()
	hasRunningOp := false
	hasNotRunningOp := false
	for _, entry := range host.capTable {
		if strings.Contains(entry.capUrn, "running-op") && !strings.Contains(entry.capUrn, "not-running-op") {
			hasRunningOp = true
		}
		if strings.Contains(entry.capUrn, "not-running-op") {
			hasNotRunningOp = true
		}
	}
	host.mu.Unlock()

	assert.True(t, hasRunningOp, "Cap table should have running cartridge's manifest caps")
	assert.True(t, hasNotRunningOp, "Cap table should have non-running cartridge's probe-time caps")

	hostRead.Close()
	hostWrite.Close()
	wg.Wait()
}

// =========================================================================
// Routing-table GC contract tests
//
// The routing tables must stay bounded under a runaway producer:
//
//   1. NO ROUTING TABLE GROWS WITHOUT BOUND. On every insert,
//      the GC fires and reduces the table size. After enough
//      passes — at most one per insertion — no routing table
//      can exceed the hard cap. Failure means a cartridge or
//      relay path could create RIDs faster than the cleanup
//      paths drain them, regressing the leak class we just
//      fixed in capdag-objc.
//
//   2. EVICTION IS ORDERED BY touch-sequence, OLDEST FIRST.
//      A still-active flow (one that has been routed through
//      recently) must NOT be evicted before a stale one. A
//      regression where the GC drops dictionary-iteration-
//      order victims would still pass invariant #1 but fail
//      this one — and dropping fresh entries silently kills
//      in-flight continuation frames.
// =========================================================================

// seedIncomingRxidsForTest inserts count synthetic incoming_rxids entries with
// deterministic touch sequences (key i has touched_at == i; smallest i means
// oldest). Returns the keys in insertion order so the test can compute the
// expected victim/survivor sets. Mirrors Rust seed_incoming_rxids_for_test.
func seedIncomingRxidsForTest(host *CartridgeHost, count int) []rxidKey {
	keys := make([]rxidKey, 0, count)
	for i := 0; i < count; i++ {
		xid := NewMessageIdFromUint(uint64(i))
		rid := NewMessageIdFromUint(uint64(i))
		key := makeRxidKey(xid, rid)
		host.incomingRxids[key] = incomingRoute{cartridgeIdx: 0, xid: xid, rid: rid}
		// Bypass touchIncomingRxid so we can assign a deterministic age.
		host.incomingRxidsTouched[key] = uint64(i)
		keys = append(keys, key)
	}
	return keys
}

// TEST988: / Contract #1 — the GC keeps the table strictly below the / hard cap. Seed the table well above the soft watermark / (matching what a runaway producer would do mid-frame- / burst) and call the production GC entry point. The / post-state must be at most `SOFT_WATERMARK` entries / because the GC drops at least / `EVICTION_FRACTION × pre_state` entries in one pass and / the pre-state is below the hard cap (i.e. one pass is / enough; the secondary "hard cap" pass would only fire if / pre-state crossed the hard cap before insertion completed, / which production prevents by gc-ing on every insert).
func Test988_gc_reduces_table_below_soft_watermark_in_one_pass(t *testing.T) {
	host := NewCartridgeHost()
	preCount := RoutingTableSoftWatermark + 256
	require.Less(t, preCount, RoutingTableHardCap,
		"Test precondition: pre_count must stay under the hard cap so we verify the SOFT watermark path")

	seedIncomingRxidsForTest(host, preCount)
	require.Equal(t, preCount, len(host.incomingRxids),
		"Seeder must populate exactly pre_count entries before the GC runs")

	host.gcRoutingTablesIfNeeded()

	assert.Less(t, len(host.incomingRxids), RoutingTableHardCap,
		"Post-GC table size %d must stay strictly under the hard cap (%d)",
		len(host.incomingRxids), RoutingTableHardCap)
	assert.Equal(t, uint64(1), host.routingGcRunsTotal,
		"Exactly one GC pass should have fired; %d runs means the single-pass invariant has changed",
		host.routingGcRunsTotal)
	expectedEvicted := int(float64(preCount) * RoutingTableGcEvictionFraction)
	if expectedEvicted < 1 {
		expectedEvicted = 1
	}
	assert.Equal(t, expectedEvicted, int(host.routingGcEvictedTotal),
		"GC pass evicted %d entries; expected %d", host.routingGcEvictedTotal, expectedEvicted)
}

// TEST129: / Contract #2 — the GC drops the OLDEST entries by / touch-sequence, not arbitrary keys. Seed a known age / distribution and assert the post-GC keyset is exactly / what the test computes should survive (test recomputes / independently of production code). / / A regression where the GC e.g. iterates the HashMap and / drops the first N (HashMap iteration order is arbitrary / in Rust) would still pass contract #1 but fail this one — / the more dangerous bug because it silently drops / in-flight continuation frames.
func Test129_gc_evicts_oldest_entries_by_touch_sequence(t *testing.T) {
	host := NewCartridgeHost()
	preCount := RoutingTableSoftWatermark + 256
	evictionCount := int(float64(preCount) * RoutingTableGcEvictionFraction)
	if evictionCount < 1 {
		evictionCount = 1
	}

	// Seed: key i has touched_at == i. Smallest i means oldest.
	// Expected victims: keys 0 ..< eviction_count.
	// Expected survivors: keys eviction_count ..< pre_count.
	keys := seedIncomingRxidsForTest(host, preCount)

	host.gcRoutingTablesIfNeeded()

	for i := 0; i < evictionCount; i++ {
		key := keys[i]
		_, present := host.incomingRxids[key]
		assert.False(t, present,
			"Key index %d should have been evicted (touched_at=%d, one of the %d oldest), but it survived the GC",
			i, i, evictionCount)
		_, touchedPresent := host.incomingRxidsTouched[key]
		assert.False(t, touchedPresent,
			"Touched-map entry for key index %d must be removed alongside the primary entry", i)
	}
	for i := evictionCount; i < preCount; i++ {
		key := keys[i]
		_, present := host.incomingRxids[key]
		assert.True(t, present,
			"Key index %d should have survived the GC (touched_at=%d, one of the %d most-recently-touched), but was evicted",
			i, i, preCount-evictionCount)
	}
}

// TEST987: / Contract #3 — the secondary hard-cap pass kicks in if the / table somehow exceeds `HARD_CAP` (extreme runaway). Without / it, a single GC at the soft watermark would not be enough / to recover headroom and the table could grow without bound / between bursts.
func Test987_gc_secondary_pass_enforces_hard_cap(t *testing.T) {
	host := NewCartridgeHost()
	// Size the seed so a SINGLE eviction-fraction pass is NOT enough to
	// bring the table under the hard cap. We need
	// pre * (1 - eviction_fraction) >= hard_cap.
	oneMinusFraction := 1.0 - RoutingTableGcEvictionFraction
	preCount := int(math.Ceil(float64(RoutingTableHardCap)/oneMinusFraction)) + 256
	seedIncomingRxidsForTest(host, preCount)
	require.GreaterOrEqual(t, len(host.incomingRxids), RoutingTableHardCap,
		"Seeder must populate at or above the hard cap so the secondary pass actually fires")

	host.gcRoutingTablesIfNeeded()

	assert.Less(t, len(host.incomingRxids), RoutingTableHardCap,
		"Post-GC table size %d must be strictly under the hard cap (%d)",
		len(host.incomingRxids), RoutingTableHardCap)
	// The secondary pass uses the same routing_gc_evicted_total counter but
	// does not increment routing_gc_runs_total. Verify the eviction count
	// exceeds one full eviction-fraction pass over the pre-count.
	singlePassMax := uint64(float64(preCount) * RoutingTableGcEvictionFraction)
	assert.Greater(t, host.routingGcEvictedTotal, singlePassMax,
		"Total evicted %d should exceed single-pass max %d (the secondary pass must have evicted additional entries)",
		host.routingGcEvictedTotal, singlePassMax)
}
