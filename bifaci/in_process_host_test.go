package bifaci

import (
	"encoding/json"
	"strings"
	"testing"

	cbor2 "github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// echoHandler accumulates input and echoes raw bytes back.
type echoHandler struct{}

func (echoHandler) HandleRequest(_ string, input <-chan Frame, output *ResponseWriter, _ PeerInvoker) {
	args, meta, err := AccumulateInput(input)
	if err != nil {
		output.EmitError("ACCUMULATE_ERROR", err.Error())
		return
	}
	var data []byte
	for _, a := range args {
		data = append(data, a.Value...)
	}
	output.EmitResponseWithMeta("media:", data, meta)
}

func makeTestCap(t *testing.T, urnStr string) cap.Cap {
	t.Helper()
	capUrn, err := urn.NewCapUrnFromString(urnStr)
	require.NoError(t, err)
	return cap.Cap{
		Urn:      capUrn,
		Version:  1,
		Title:    "test",
		Command:  "",
		Metadata: map[string]string{},
		Args:     []cap.CapArg{},
	}
}

// cborBytesPayload builds a CBOR-encoded chunk payload from raw bytes
// (matching BuildRequestFrames).
func cborBytesPayload(t *testing.T, data []byte) []byte {
	t.Helper()
	buf, err := cbor2.Marshal(data)
	require.NoError(t, err)
	return buf
}

// TEST6748: InProcessCartridgeHost routes REQ to matching handler and returns response
func Test6748_RoutesReqToHandler(t *testing.T) {
	capUrn := `cap:in="media:text";echo;out="media:text"`
	c := makeTestCap(t, capUrn)
	handlers := []HandlerRegistration{{
		Name:    "echo",
		Caps:    []cap.Cap{c},
		Handler: echoHandler{},
	}}

	host := NewInProcessCartridgeHost(InProcessHostIdentityForTest("in-process-test"), handlers)

	hostConn, testConn := createSocketPair(t)
	defer hostConn.Close()
	defer testConn.Close()

	hostDone := make(chan error, 1)
	go func() { hostDone <- host.Run(hostConn, hostConn) }()

	reader := NewFrameReader(testConn)
	writer := NewFrameWriter(testConn)

	// First frame should be RelayNotify with manifest
	notify, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeRelayNotify, notify.FrameType)
	manifest := notify.RelayNotifyManifest()
	require.NotNil(t, manifest)
	var payload RelayNotifyCapabilitiesPayload
	require.NoError(t, json.Unmarshal(manifest, &payload))
	caps := payload.CapURNs()
	assert.GreaterOrEqual(t, len(caps), 2) // identity + echo cap
	assert.Equal(t, standard.CapIdentity, caps[0])
	// The InProcessCartridgeHost wraps its handlers in one synthetic
	// installed-cartridge entry so the wire schema (cap_groups inside
	// installed_cartridges) is satisfied.
	assert.Len(t, payload.InstalledCartridges, 1)
	// The in-process host declares whatever identity its embedder supplied;
	// the manifest round-trips that id verbatim, no synthetic placeholder.
	assert.Equal(t, "in-process-test", payload.InstalledCartridges[0].Id)

	// Send a REQ + STREAM_START + CHUNK (CBOR-encoded) + STREAM_END + END
	rid := NewMessageIdRandom()
	req := NewReq(rid, capUrn, []byte{}, "application/cbor")
	xid := NewMessageIdFromUint(1)
	req.RoutingId = &xid
	require.NoError(t, writer.WriteFrame(req))

	ss := NewStreamStart(rid, "arg0", "media:text", nil)
	require.NoError(t, writer.WriteFrame(ss))

	payloadBytes := cborBytesPayload(t, []byte("hello world"))
	checksum := ComputeChecksum(payloadBytes)
	chunk := NewChunk(rid, "arg0", 0, payloadBytes, 0, checksum)
	require.NoError(t, writer.WriteFrame(chunk))

	se := NewStreamEnd(rid, "arg0", 1)
	require.NoError(t, writer.WriteFrame(se))

	end := NewEnd(rid, nil)
	require.NoError(t, writer.WriteFrame(end))

	// Read response: STREAM_START + CHUNK (CBOR-encoded) + STREAM_END + END
	respSs, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeStreamStart, respSs.FrameType)
	assert.True(t, respSs.Id.Equals(rid))
	require.NotNil(t, respSs.StreamId)
	assert.Equal(t, "result", *respSs.StreamId)

	respChunk, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeChunk, respChunk.FrameType)
	respData, err := DecodeChunkPayload(respChunk.Payload)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello world"), respData)

	respSe, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeStreamEnd, respSe.FrameType)

	respEnd, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeEnd, respEnd.FrameType)

	testConn.Close()
	require.NoError(t, <-hostDone)
}

// TEST6749: InProcessCartridgeHost handles identity verification (echo nonce)
func Test6749_IdentityVerification(t *testing.T) {
	host := NewInProcessCartridgeHost(InProcessHostIdentityForTest("in-process-test"), nil)

	hostConn, testConn := createSocketPair(t)
	defer hostConn.Close()
	defer testConn.Close()

	hostDone := make(chan error, 1)
	go func() { hostDone <- host.Run(hostConn, hostConn) }()

	reader := NewFrameReader(testConn)
	writer := NewFrameWriter(testConn)

	// Skip RelayNotify
	_, err := reader.ReadFrame()
	require.NoError(t, err)

	// Send identity verification
	rid := NewMessageIdRandom()
	req := NewReq(rid, standard.CapIdentity, []byte{}, "application/cbor")
	xid := NewMessageIdFromUint(0)
	req.RoutingId = &xid
	require.NoError(t, writer.WriteFrame(req))

	// Send nonce via stream (already CBOR-encoded by identityNonce)
	nonce := identityNonce()
	ss := NewStreamStart(rid, "identity-verify", "media:", nil)
	require.NoError(t, writer.WriteFrame(ss))

	checksum := ComputeChecksum(nonce)
	chunk := NewChunk(rid, "identity-verify", 0, nonce, 0, checksum)
	require.NoError(t, writer.WriteFrame(chunk))

	se := NewStreamEnd(rid, "identity-verify", 1)
	require.NoError(t, writer.WriteFrame(se))

	end := NewEnd(rid, nil)
	require.NoError(t, writer.WriteFrame(end))

	// Read echoed response — identity echoes raw bytes (no CBOR decode/encode)
	respSs, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeStreamStart, respSs.FrameType)

	respChunk, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeChunk, respChunk.FrameType)
	assert.Equal(t, nonce, respChunk.Payload)

	respSe, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeStreamEnd, respSe.FrameType)

	respEnd, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeEnd, respEnd.FrameType)

	testConn.Close()
	require.NoError(t, <-hostDone)
}

// TEST6750: InProcessCartridgeHost returns NO_HANDLER for unregistered cap
func Test6750_NoHandlerReturnsErr(t *testing.T) {
	host := NewInProcessCartridgeHost(InProcessHostIdentityForTest("in-process-test"), nil)

	hostConn, testConn := createSocketPair(t)
	defer hostConn.Close()
	defer testConn.Close()

	hostDone := make(chan error, 1)
	go func() { hostDone <- host.Run(hostConn, hostConn) }()

	reader := NewFrameReader(testConn)
	writer := NewFrameWriter(testConn)

	// Skip RelayNotify
	_, err := reader.ReadFrame()
	require.NoError(t, err)

	rid := NewMessageIdRandom()
	req := NewReq(rid, `cap:in="media:ext=pdf";unknown;out="media:text"`, []byte{}, "application/cbor")
	xid := NewMessageIdFromUint(1)
	req.RoutingId = &xid
	require.NoError(t, writer.WriteFrame(req))

	// Should get ERR back
	errFrame, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeErr, errFrame.FrameType)
	assert.True(t, errFrame.Id.Equals(rid))
	assert.Equal(t, "NO_HANDLER", errFrame.ErrorCode())

	testConn.Close()
	require.NoError(t, <-hostDone)
}

// TEST6751: InProcessCartridgeHost manifest includes identity cap and handler caps
func Test6751_ManifestIncludesAllCaps(t *testing.T) {
	capUrn := `cap:in="media:ext=pdf";thumbnail;out="media:ext=png;image"`
	c := makeTestCap(t, capUrn)
	host := NewInProcessCartridgeHost(InProcessHostIdentityForTest("thumb-host"), []HandlerRegistration{{
		Name:    "thumb",
		Caps:    []cap.Cap{c},
		Handler: echoHandler{},
	}})

	manifest := host.BuildManifest()
	var payload RelayNotifyCapabilitiesPayload
	require.NoError(t, json.Unmarshal(manifest, &payload))
	caps := payload.CapURNs()
	assert.Equal(t, standard.CapIdentity, caps[0])
	hasThumbnail := false
	for _, u := range caps {
		if strings.Contains(u, "thumbnail") {
			hasThumbnail = true
			break
		}
	}
	assert.True(t, hasThumbnail)
	// The InProcessCartridgeHost wraps the handlers in a single
	// installed-cartridge entry whose identity comes from the
	// InProcessHostIdentity the test passed at construction.
	assert.Len(t, payload.InstalledCartridges, 1)
	assert.Equal(t, "thumb-host", payload.InstalledCartridges[0].Id)
	assert.Len(t, payload.InstalledCartridges[0].CapGroups, 1)
}

// TEST658: InProcessCartridgeHost handles heartbeat by echoing same ID
func Test658_HeartbeatResponse(t *testing.T) {
	host := NewInProcessCartridgeHost(InProcessHostIdentityForTest("in-process-test"), nil)

	hostConn, testConn := createSocketPair(t)
	defer hostConn.Close()
	defer testConn.Close()

	hostDone := make(chan error, 1)
	go func() { hostDone <- host.Run(hostConn, hostConn) }()

	reader := NewFrameReader(testConn)
	writer := NewFrameWriter(testConn)

	// Skip RelayNotify
	_, err := reader.ReadFrame()
	require.NoError(t, err)

	hbId := NewMessageIdRandom()
	hb := NewHeartbeat(hbId)
	require.NoError(t, writer.WriteFrame(hb))

	resp, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeHeartbeat, resp.FrameType)
	assert.True(t, resp.Id.Equals(hbId))

	testConn.Close()
	require.NoError(t, <-hostDone)
}

// failHandler always fails.
type failHandler struct{}

func (failHandler) HandleRequest(_ string, input <-chan Frame, output *ResponseWriter, _ PeerInvoker) {
	// Drain input
	for frame := range input {
		if frame.FrameType == FrameTypeEnd {
			break
		}
	}
	output.EmitError("PROVIDER_ERROR", "provider crashed")
}

// TEST659: InProcessCartridgeHost handler error returns ERR frame
func Test659_HandlerErrorReturnsErrFrame(t *testing.T) {
	capUrn := `cap:in="media:void";fail;out="media:void"`
	c := makeTestCap(t, capUrn)
	host := NewInProcessCartridgeHost(InProcessHostIdentityForTest("fail-host"), []HandlerRegistration{{
		Name:    "fail",
		Caps:    []cap.Cap{c},
		Handler: failHandler{},
	}})

	hostConn, testConn := createSocketPair(t)
	defer hostConn.Close()
	defer testConn.Close()

	hostDone := make(chan error, 1)
	go func() { hostDone <- host.Run(hostConn, hostConn) }()

	reader := NewFrameReader(testConn)
	writer := NewFrameWriter(testConn)

	// Skip RelayNotify
	_, err := reader.ReadFrame()
	require.NoError(t, err)

	// Send REQ + END (no streams, void input)
	rid := NewMessageIdRandom()
	req := NewReq(rid, capUrn, []byte{}, "application/cbor")
	xid := NewMessageIdFromUint(1)
	req.RoutingId = &xid
	require.NoError(t, writer.WriteFrame(req))

	end := NewEnd(rid, nil)
	require.NoError(t, writer.WriteFrame(end))

	// Should get ERR frame
	errFrame, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeErr, errFrame.FrameType)
	assert.True(t, errFrame.Id.Equals(rid))
	assert.Equal(t, "PROVIDER_ERROR", errFrame.ErrorCode())
	assert.Contains(t, errFrame.ErrorMessage(), "provider crashed")

	testConn.Close()
	require.NoError(t, <-hostDone)
}

// TEST660: InProcessCartridgeHost closest-specificity routing prefers specific over identity
func Test660_ClosestSpecificityRouting(t *testing.T) {
	specificUrn := `cap:in="media:ext=pdf";thumbnail;out="media:ext=png;image"`
	genericUrn := `cap:in="media:image";thumbnail;out="media:ext=png;image"`

	specificCap := makeTestCap(t, specificUrn)
	genericCap := makeTestCap(t, genericUrn)

	handlers := []handlerEntry{
		{
			name:    "generic",
			caps:    []cap.Cap{genericCap},
			handler: taggedHandler{"generic"},
		},
		{
			name:    "specific",
			caps:    []cap.Cap{specificCap},
			handler: taggedHandler{"specific"},
		},
	}

	capTable := buildCapTable(handlers)

	// Request for pdf thumbnail should match specific (pdf, specificity 3) over
	// generic (image, specificity 2)
	idx, ok := findHandlerForCap(capTable, `cap:in="media:ext=pdf";thumbnail;out="media:ext=png;image"`)
	assert.True(t, ok)
	assert.Equal(t, 1, idx) // specific handler
}

// taggedHandler tags its output with its name.
type taggedHandler struct {
	tag string
}

func (h taggedHandler) HandleRequest(_ string, input <-chan Frame, output *ResponseWriter, _ PeerInvoker) {
	// Drain input
	for frame := range input {
		if frame.FrameType == FrameTypeEnd {
			break
		}
	}
	output.EmitResponse("media:text", []byte(h.tag))
}
