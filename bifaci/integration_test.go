package bifaci

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"

	cbor2 "github.com/fxamacker/cbor/v2"
	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create test registry
func createTestRegistry(t *testing.T) *media.FabricRegistry {
	t.Helper()
	registry, err := media.NewFabricRegistry()
	require.NoError(t, err)
	for _, def := range []media.MediaDef{
		{Urn: "media:enc=utf-8", MediaType: "text/plain", ProfileURI: media.ProfileStr},
		{Urn: "media:record;enc=utf-8", MediaType: "application/json", ProfileURI: media.ProfileObj},
		{Urn: "media:fmt=json;record", MediaType: "application/json", ProfileURI: media.ProfileObj},
		{Urn: "media:", MediaType: "application/octet-stream"},
		{Urn: "media:void", MediaType: "application/x-void", ProfileURI: media.ProfileVoid},
	} {
		registry.AddSpec(def.ToStored())
	}
	return registry
}

// Test helper for integration tests - use proper media URNs with tags
func intTestUrn(tags string) string {
	if tags == "" {
		return `cap:in="media:void";out="media:fmt=json;record"`
	}
	return `cap:in="media:void";out="media:fmt=json;record";` + tags
}

// Test6428_IntegrationVersionlessCapCreation verifies caps can be created without version fields
func Test6428_IntegrationVersionlessCapCreation(t *testing.T) {
	// Test case 1: Create cap without version parameter
	// Use type=data_processing key=value instead of flag
	capUrn, err := urn.NewCapUrnFromString(intTestUrn("transform;format=json;type=data_processing"))
	require.NoError(t, err)

	capDef := cap.NewCap(capUrn, "Data Transformer", "transform-command")

	// Verify the cap has direction specs in canonical form
	assert.Contains(t, capDef.UrnString(), `in=media:void`)
	assert.Contains(t, capDef.UrnString(), `out="media:fmt=json;record"`)
	assert.Equal(t, "transform-command", capDef.Command)

	// Test case 2: Create cap with description but no version
	capDef2 := cap.NewCapWithDescription(capUrn, "Data Transformer", "transform-command", "Transforms data")
	assert.NotNil(t, capDef2.CapDescription)
	assert.Equal(t, "Transforms data", *capDef2.CapDescription)

	// Test case 3: Verify caps can be compared without version
	assert.True(t, capDef.Equals(capDef))

	// Different caps should not be equal
	urn2, _ := urn.NewCapUrnFromString(intTestUrn("generate;format=pdf"))
	capDef3 := cap.NewCap(urn2, "PDF Generator", "generate-command")
	assert.False(t, capDef.Equals(capDef3))
}

// Test6431_IntegrationCaseInsensitiveUrns verifies URNs are case-insensitive
func Test6431_IntegrationCaseInsensitiveUrns(t *testing.T) {
	// Test case 1: Different case inputs should produce same URN.
	// Both URNs use the same tag shape (a `transform` marker plus
	// keyed `format=json` and `type=data_processing`). Only the case
	// of keys / values differs.
	urn1, err := urn.NewCapUrnFromString(intTestUrn("Transform;FORMAT=JSON;Type=Data_Processing"))
	require.NoError(t, err)

	urn2, err := urn.NewCapUrnFromString(intTestUrn("transform;format=json;type=data_processing"))
	require.NoError(t, err)

	// URNs should be equal (case-insensitive keys and unquoted values)
	assert.True(t, urn1.Equals(urn2))
	assert.Equal(t, urn1.ToString(), urn2.ToString())

	// Test case 2: Case-insensitive marker tag lookup.
	assert.True(t, urn1.HasMarkerTag("transform"))
	assert.True(t, urn1.HasMarkerTag("TRANSFORM"))
	assert.True(t, urn1.HasMarkerTag("Transform"))

	// Test case 3: Case-insensitive keyed tag lookup with case-sensitive values.
	assert.True(t, urn1.HasTag("FORMAT", "json"))
	assert.True(t, urn1.HasTag("format", "json"))
	assert.True(t, urn1.HasTag("Format", "json"))
	// Value comparison is case-sensitive — the URN's value is "json" (lowercased
	// because it was unquoted), so an uppercase comparison fails.
	assert.False(t, urn1.HasTag("format", "JSON"))

	// Test case 4: Builder preserves value case
	urn3, err := urn.NewCapUrnBuilder().
		InSpec(standard.MediaVoid).
		OutSpec(standard.MediaJSON).
		Tag("OP", "Transform").
		Tag("Format", "JSON").
		Build()
	require.NoError(t, err)

	assert.True(t, urn3.HasTag("op", "Transform"))
	assert.True(t, urn3.HasTag("format", "JSON"))
}

// TestIntegrationCallerAndResponseSystem verifies the caller and response system

// Test6433_IntegrationCapValidation verifies cap schema validation
func Test6433_IntegrationCapValidation(t *testing.T) {
	registry := createTestRegistry(t)
	coordinator := cap.NewCapValidationCoordinator()

	// Create a cap with arguments - use proper tags
	urn, err := urn.NewCapUrnFromString(`cap:in="media:void";process;out="media:fmt=json;record";target=data`)
	require.NoError(t, err)

	capDef := cap.NewCap(urn, "Data Processor", "process-data")

	// Seed the registry for resolution
	for _, def := range []media.MediaDef{
		{Urn: standard.MediaJSON, MediaType: "application/json", ProfileURI: media.ProfileObj},
		{Urn: standard.MediaString, MediaType: "text/plain", ProfileURI: media.ProfileStr},
	} {
		registry.AddSpec(def.ToStored())
	}

	// Add required string argument using new architecture
	cliFlag1 := "--input"
	pos1 := 0
	capDef.AddArg(cap.CapArg{
		MediaUrn:       standard.MediaString,
		Required:       true,
		Sources:        []cap.ArgSource{{CliFlag: &cliFlag1}, {Position: &pos1}},
		ArgDescription: cap.StringPtr("Input path"),
	})

	// Set output
	capDef.SetOutput(cap.NewCapOutput(standard.MediaJSON, "Processing result"))

	// Register cap
	coordinator.RegisterCap(capDef)

	// Test valid inputs - string for MediaString
	err = coordinator.ValidateInputs(capDef.UrnString(), []interface{}{"test.txt"}, registry)
	assert.NoError(t, err)

	// Test missing required argument
	err = coordinator.ValidateInputs(capDef.UrnString(), []interface{}{}, registry)
	assert.Error(t, err)
}

// Test0183_IntegrationMediaUrnResolution verifies media URN resolution
func Test0183_IntegrationMediaUrnResolution(t *testing.T) {
	registry := createTestRegistry(t)

	// Seed registry with the specs we resolve below.
	for _, def := range []media.MediaDef{
		{Urn: standard.MediaString, MediaType: "text/plain", ProfileURI: media.ProfileStr},
		{Urn: standard.MediaJSON, MediaType: "application/json", ProfileURI: media.ProfileObj},
		{Urn: standard.MediaIdentity, MediaType: "application/octet-stream"},
	} {
		registry.AddSpec(def.ToStored())
	}

	// Test string media URN resolution
	resolved, err := media.ResolveMediaUrn(standard.MediaString, registry)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", resolved.MediaType)
	assert.Equal(t, media.ProfileStr, resolved.ProfileURI)
	assert.True(t, resolved.HasEncoding(), "string media is text-representable")
	assert.False(t, resolved.IsJSON())

	// Test JSON media URN
	resolved, err = media.ResolveMediaUrn(standard.MediaJSON, registry)
	require.NoError(t, err)
	assert.Equal(t, "application/json", resolved.MediaType)
	assert.True(t, resolved.IsRecord())
	assert.True(t, resolved.IsStructured())
	assert.True(t, resolved.IsJSON()) // MediaJSON has json marker tag

	// Test opaque-bytes media URN (no enc= tag)
	resolved, err = media.ResolveMediaUrn(standard.MediaIdentity, registry)
	require.NoError(t, err)
	assert.False(t, resolved.HasEncoding(), "identity media carries no enc= tag")

	// Test custom media URN resolution
	registry.AddSpec(media.MediaDef{
		Urn:        "media:custom;enc=utf-8",
		MediaType:  "text/html",
		ProfileURI: "https://example.com/schema/html",
	}.ToStored())

	resolved, err = media.ResolveMediaUrn("media:custom;enc=utf-8", registry)
	require.NoError(t, err)
	assert.Equal(t, "text/html", resolved.MediaType)
	assert.Equal(t, "https://example.com/schema/html", resolved.ProfileURI)

	// Test unknown media URN fails
	_, err = media.ResolveMediaUrn("media:unknown", registry)
	assert.Error(t, err)
}

// Test0209_IntegrationMediaDefConstruction verifies media.MediaDef construction
func Test0209_IntegrationMediaDefConstruction(t *testing.T) {
	// Test basic construction
	def := media.NewMediaDef("media:test;enc=utf-8", "text/plain", "https://capdag.com/schema/str")
	assert.Equal(t, "media:test;enc=utf-8", def.Urn)
	assert.Equal(t, "text/plain", def.MediaType)
	assert.Equal(t, "https://capdag.com/schema/str", def.ProfileURI)

	// Test with title
	defWithTitle := media.NewMediaDefWithTitle("media:test;enc=utf-8", "text/plain", "https://example.com/schema", "Test Title")
	assert.Equal(t, "Test Title", defWithTitle.Title)

	// Test object form with schema
	schema := map[string]interface{}{"type": "object"}
	schemaDef := media.NewMediaDefWithSchema("media:test;fmt=json", "application/json", "https://example.com/schema", schema)
	assert.NotNil(t, schemaDef.Schema)
}

// CBOR Integration Tests (TEST284-303)
// These tests verify the CBOR cartridge communication protocol between host and cartridge

const testCBORManifest = `{"name":"TestCartridge","version":"1.0.0","channel":"release","description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=\"media:void\";test;out=\"media:void\"","title":"Test","command":"test"}]}]}`

// createPipePair creates a pair of connected Unix socket streams for testing
func createPipePair(t *testing.T) (hostWrite, cartridgeRead, cartridgeWrite, hostRead net.Conn) {
	// Create two socket pairs
	hostWriteConn, cartridgeReadConn := createSocketPair(t)
	cartridgeWriteConn, hostReadConn := createSocketPair(t)
	return hostWriteConn, cartridgeReadConn, cartridgeWriteConn, hostReadConn
}

func createSocketPair(t *testing.T) (net.Conn, net.Conn) {
	// Use socketpair for bidirectional communication
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	require.NoError(t, err)

	file1 := os.NewFile(uintptr(fds[0]), "socket1")
	file2 := os.NewFile(uintptr(fds[1]), "socket2")

	conn1, err := net.FileConn(file1)
	require.NoError(t, err)
	conn2, err := net.FileConn(file2)
	require.NoError(t, err)

	file1.Close()
	file2.Close()

	return conn1, conn2
}

// TEST284: Handshake exchanges HELLO frames, negotiates limits
func Test284_HandshakeHostCartridge(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var cartridgeLimits Limits
	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		assert.True(t, limits.MaxFrame > 0)
		assert.True(t, limits.MaxChunk > 0)
		cartridgeLimits = limits
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	manifest, hostLimits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)

	// Verify manifest received
	assert.Equal(t, []byte(testCBORManifest), manifest)

	wg.Wait()

	// Both should have negotiated the same limits
	assert.Equal(t, hostLimits.MaxFrame, cartridgeLimits.MaxFrame)
	assert.Equal(t, hostLimits.MaxChunk, cartridgeLimits.MaxChunk)
}

// TEST285: Simple request-response flow (REQ → END with payload)
func Test285_RequestResponseSimple(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		// Handshake
		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		assert.Equal(t, FrameTypeReq, frame.FrameType)
		assert.NotNil(t, frame.Cap)
		assert.Equal(t, "cap:echo", *frame.Cap)
		assert.Equal(t, []byte("hello"), frame.Payload)

		// Send response
		response := NewEnd(frame.Id, []byte("hello back"))
		err = writer.WriteFrame(response)
		require.NoError(t, err)
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	manifest, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	assert.Equal(t, []byte(testCBORManifest), manifest)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:echo", []byte("hello"), "application/json")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Read response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeEnd, response.FrameType)
	assert.Equal(t, []byte("hello back"), response.Payload)

	wg.Wait()
}

// TEST286: Streaming response with multiple CHUNK frames
func Test286_StreamingChunks(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		requestID := frame.Id

		// Send 3 chunks
		chunks := [][]byte{[]byte("chunk1"), []byte("chunk2"), []byte("chunk3")}
		for i, chunk := range chunks {
			chunkIndex := uint64(i)
			checksum := ComputeChecksum(chunk)
			chunkFrame := NewChunk(requestID, "response", uint64(i), chunk, chunkIndex, checksum)
			if i == 0 {
				totalLen := uint64(18)
				chunkFrame.Len = &totalLen // total length
			}
			if i == len(chunks)-1 {
				eof := true
				chunkFrame.Eof = &eof
			}
			err = writer.WriteFrame(chunkFrame)
			require.NoError(t, err)
		}
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:stream", []byte("go"), "application/json")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Collect chunks
	var chunks [][]byte
	for i := 0; i < 3; i++ {
		chunk, err := reader.ReadFrame()
		require.NoError(t, err)
		assert.Equal(t, FrameTypeChunk, chunk.FrameType)
		chunks = append(chunks, chunk.Payload)
	}

	assert.Equal(t, 3, len(chunks))
	assert.Equal(t, []byte("chunk1"), chunks[0])
	assert.Equal(t, []byte("chunk2"), chunks[1])
	assert.Equal(t, []byte("chunk3"), chunks[2])

	wg.Wait()
}

// TEST287: Host-initiated heartbeat
func Test287_HeartbeatFromHost(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	done := make(chan bool)

	// Cartridge side
	go func() {
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read heartbeat
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		assert.Equal(t, FrameTypeHeartbeat, frame.FrameType)

		// Respond with heartbeat
		response := NewHeartbeat(frame.Id)
		err = writer.WriteFrame(response)
		require.NoError(t, err)

		done <- true
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send heartbeat
	heartbeatID := NewMessageIdRandom()
	heartbeat := NewHeartbeat(heartbeatID)
	err = writer.WriteFrame(heartbeat)
	require.NoError(t, err)

	// Wait for cartridge to finish
	<-done

	// Read heartbeat response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeHeartbeat, response.FrameType)
	assert.Equal(t, heartbeatID.ToString(), response.Id.ToString())
}

// Mirror-specific coverage: Test cartridge ERR frame is received by host as error
func Test0265_CartridgeErrorResponse(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)

		// Send error
		errFrame := NewErr(frame.Id, "NOT_FOUND", "cap.Cap not found: cap:missing")
		err = writer.WriteFrame(errFrame)
		require.NoError(t, err)
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:missing", []byte(""), "application/json")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Read error response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeErr, response.FrameType)
	assert.Equal(t, "NOT_FOUND", response.ErrorCode())
	assert.Contains(t, response.ErrorMessage(), "cap.Cap not found")

	wg.Wait()
}

// Mirror-specific coverage: Test LOG frames sent during a request are transparently skipped by host
func Test6524_LogFramesDuringRequest(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		requestID := frame.Id

		// Send log frames
		log1 := NewLog(requestID, "info", "Processing started")
		err = writer.WriteFrame(log1)
		require.NoError(t, err)

		log2 := NewLog(requestID, "debug", "Step 1 complete")
		err = writer.WriteFrame(log2)
		require.NoError(t, err)

		// Send final response
		response := NewEnd(requestID, []byte("done"))
		err = writer.WriteFrame(response)
		require.NoError(t, err)
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", []byte(""), "application/json")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Read frames until END (skipping LOG frames)
	for {
		frame, err := reader.ReadFrame()
		require.NoError(t, err)

		if frame.FrameType == FrameTypeLog {
			// Skip log frames
			continue
		}

		if frame.FrameType == FrameTypeEnd {
			assert.Equal(t, []byte("done"), frame.Payload)
			break
		}
	}

	wg.Wait()
}

// TEST290: Limit negotiation picks minimum
func Test290_LimitsNegotiation(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var cartridgeLimits Limits
	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		// Handshake
		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		cartridgeLimits = limits
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, hostLimits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)

	wg.Wait()

	// Both should have negotiated the same limits (default limits in this case)
	assert.Equal(t, hostLimits.MaxFrame, cartridgeLimits.MaxFrame)
	assert.Equal(t, hostLimits.MaxChunk, cartridgeLimits.MaxChunk)
	assert.True(t, hostLimits.MaxFrame > 0)
	assert.True(t, hostLimits.MaxChunk > 0)
}

// TEST291: Binary payload roundtrip (all 256 byte values)
func Test291_BinaryPayloadRoundtrip(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	// Create binary test data with all byte values
	binaryData := make([]byte, 256)
	for i := 0; i < 256; i++ {
		binaryData[i] = byte(i)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		payload := frame.Payload

		// Verify all bytes
		assert.Equal(t, 256, len(payload))
		for i := 0; i < 256; i++ {
			assert.Equal(t, byte(i), payload[i], "Byte mismatch at position %d", i)
		}

		// Echo back
		response := NewEnd(frame.Id, payload)
		err = writer.WriteFrame(response)
		require.NoError(t, err)
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send binary data
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:binary", binaryData, "application/octet-stream")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Read response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	result := response.Payload

	// Verify response
	assert.Equal(t, 256, len(result))
	for i := 0; i < 256; i++ {
		assert.Equal(t, byte(i), result[i], "Response byte mismatch at position %d", i)
	}

	wg.Wait()
}

// TEST292: Sequential requests get distinct MessageIds
func Test292_MessageIdUniqueness(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var receivedIDs []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read 3 requests
		for i := 0; i < 3; i++ {
			frame, err := reader.ReadFrame()
			require.NoError(t, err)

			mu.Lock()
			receivedIDs = append(receivedIDs, frame.Id.ToString())
			mu.Unlock()

			response := NewEnd(frame.Id, []byte("ok"))
			err = writer.WriteFrame(response)
			require.NoError(t, err)
		}
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send 3 requests
	for i := 0; i < 3; i++ {
		requestID := NewMessageIdRandom()
		request := NewReq(requestID, "cap:test", []byte(""), "application/json")
		err = writer.WriteFrame(request)
		require.NoError(t, err)

		// Read response
		_, err = reader.ReadFrame()
		require.NoError(t, err)
	}

	wg.Wait()

	// Verify IDs are unique
	assert.Equal(t, 3, len(receivedIDs))
	for i := 0; i < len(receivedIDs); i++ {
		for j := i + 1; j < len(receivedIDs); j++ {
			assert.NotEqual(t, receivedIDs[i], receivedIDs[j], "IDs should be unique")
		}
	}
}

// TEST293: Test CartridgeRuntime Op registration and lookup by exact and non-existent cap URN
func Test293_CartridgeRuntimeHandlerRegistration(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testCBORManifest))
	require.NoError(t, err)

	runtime.Register(standard.CapIdentity,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			payload, err := CollectFirstArg(frames)
			if err != nil {
				return err
			}
			return emitter.EmitCbor(payload)
		})

	runtime.Register(`cap:in="media:void";transform;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			return emitter.EmitCbor("transformed")
		})

	// Exact match
	assert.NotNil(t, runtime.FindHandler(standard.CapIdentity))
	assert.NotNil(t, runtime.FindHandler(`cap:in="media:void";transform;out="media:void"`))

	// Non-existent
	assert.Nil(t, runtime.FindHandler(`cap:in="media:void";unknown;out="media:void"`))
}

// Mirror-specific coverage: Test cartridge-initiated heartbeat mid-stream is handled transparently by host
func Test0267_HeartbeatDuringStreaming(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		requestID := frame.Id

		// Send chunk 1
		chunkIndex := uint64(0)
		checksum := ComputeChecksum([]byte("part1"))
		chunk1 := NewChunk(requestID, "response", 0, []byte("part1"), chunkIndex, checksum)
		err = writer.WriteFrame(chunk1)
		require.NoError(t, err)

		// Send heartbeat
		heartbeatID := NewMessageIdRandom()
		heartbeat := NewHeartbeat(heartbeatID)
		err = writer.WriteFrame(heartbeat)
		require.NoError(t, err)

		// Wait for heartbeat response
		hbResponse, err := reader.ReadFrame()
		require.NoError(t, err)
		assert.Equal(t, FrameTypeHeartbeat, hbResponse.FrameType)
		assert.Equal(t, heartbeatID.ToString(), hbResponse.Id.ToString())

		// Send final chunk
		chunkIndex = uint64(1)
		checksum = ComputeChecksum([]byte("part2"))
		chunk2 := NewChunk(requestID, "response", 1, []byte("part2"), chunkIndex, checksum)
		eof := true
		chunk2.Eof = &eof
		err = writer.WriteFrame(chunk2)
		require.NoError(t, err)
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:stream", []byte(""), "application/json")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Collect chunks, handling heartbeat mid-stream
	var chunks [][]byte
	for {
		frame, err := reader.ReadFrame()
		require.NoError(t, err)

		if frame.FrameType == FrameTypeHeartbeat {
			// Respond to heartbeat
			hbResponse := NewHeartbeat(frame.Id)
			err = writer.WriteFrame(hbResponse)
			require.NoError(t, err)
			continue
		}

		if frame.FrameType == FrameTypeChunk {
			chunks = append(chunks, frame.Payload)
			if frame.Eof != nil && *frame.Eof {
				break
			}
		}
	}

	assert.Equal(t, 2, len(chunks))
	assert.Equal(t, []byte("part1"), chunks[0])
	assert.Equal(t, []byte("part2"), chunks[1])

	wg.Wait()
}

// Mirror-specific coverage: Test host does not echo back cartridge's heartbeat response (no infinite ping-pong)
func Test6526_HostInitiatedHeartbeatNoPingPong(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	done := make(chan bool)

	// Cartridge side
	go func() {
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		requestFrame, err := reader.ReadFrame()
		require.NoError(t, err)
		assert.Equal(t, FrameTypeReq, requestFrame.FrameType)
		requestID := requestFrame.Id

		// Read heartbeat from host
		heartbeatFrame, err := reader.ReadFrame()
		require.NoError(t, err)
		assert.Equal(t, FrameTypeHeartbeat, heartbeatFrame.FrameType)
		heartbeatID := heartbeatFrame.Id

		// Respond to heartbeat
		hbResponse := NewHeartbeat(heartbeatID)
		err = writer.WriteFrame(hbResponse)
		require.NoError(t, err)

		// Send request response using END frame
		response := NewEnd(requestID, []byte("done"))
		err = writer.WriteFrame(response)
		require.NoError(t, err)

		done <- true
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", []byte(""), "application/json")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Send heartbeat
	heartbeatID := NewMessageIdRandom()
	heartbeat := NewHeartbeat(heartbeatID)
	err = writer.WriteFrame(heartbeat)
	require.NoError(t, err)

	// Read heartbeat response
	hbResponse, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeHeartbeat, hbResponse.FrameType)

	// Read request response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeEnd, response.FrameType)
	assert.Equal(t, []byte("done"), response.Payload)

	<-done
}

// Mirror-specific coverage: Test host call with unified CBOR arguments sends correct content_type and payload
func Test0269_ArgumentsRoundtrip(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)

		// Verify content type
		require.NotNil(t, frame.ContentType)
		assert.Equal(t, "application/cbor", *frame.ContentType, "arguments must use application/cbor")

		// Parse CBOR arguments
		var args []map[string]interface{}
		err = DecodeCBORValue(frame.Payload, &args)
		require.NoError(t, err)
		assert.Equal(t, 1, len(args), "should have exactly one argument")

		// Extract value from first argument
		value := args[0]["value"].([]byte)

		// Echo back
		response := NewEnd(frame.Id, value)
		err = writer.WriteFrame(response)
		require.NoError(t, err)
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Create arguments
	args := []cap.CapArgumentValue{
		cap.NewCapArgumentValueFromStr("media:model-spec;enc=utf-8", "gpt-4"),
	}

	// Encode arguments to CBOR
	argsData, err := EncodeCapArgumentValues(args)
	require.NoError(t, err)

	// Send request with CBOR arguments
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", argsData, "application/cbor")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Read response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, []byte("gpt-4"), response.Payload)

	wg.Wait()
}

// Mirror-specific coverage: Test host receives error when cartridge closes connection unexpectedly
func Test6528_CartridgeSuddenDisconnect(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request but don't respond - just close
		_, err = reader.ReadFrame()
		require.NoError(t, err)

		// Close connection
		cartridgeRead.Close()
		cartridgeWrite.Close()
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", []byte(""), "application/json")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Try to read response - should fail with EOF
	_, err = reader.ReadFrame()
	assert.Error(t, err, "must fail when cartridge disconnects")
	assert.Equal(t, io.EOF, err)

	wg.Wait()
}

// TEST299: Empty payload request/response roundtrip
func Test299_EmptyPayloadRoundtrip(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		assert.Empty(t, frame.Payload, "empty payload must arrive empty")

		// Send empty response
		response := NewEnd(frame.Id, []byte{})
		err = writer.WriteFrame(response)
		require.NoError(t, err)
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send empty request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:empty", []byte{}, "application/json")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Read response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Empty(t, response.Payload)

	wg.Wait()
}

// Mirror-specific coverage: Test END frame without payload is handled as complete response with empty data
func Test6529_EndFrameNoPayload(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)

		// Send END with nil payload
		response := NewEnd(frame.Id, nil)
		err = writer.WriteFrame(response)
		require.NoError(t, err)
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", []byte(""), "application/json")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Read response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, FrameTypeEnd, response.FrameType)
	// END with nil payload should be handled cleanly

	wg.Wait()
}

// Mirror-specific coverage: Test streaming response sequence numbers are contiguous and start from 0
func Test6530_StreamingSequenceNumbers(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		requestID := frame.Id

		// Send 5 chunks with explicit sequence numbers
		for seq := uint64(0); seq < 5; seq++ {
			payload := []byte(string(rune('0' + seq)))
			chunkIndex := seq
			checksum := ComputeChecksum(payload)
			chunk := NewChunk(requestID, "output", seq, payload, chunkIndex, checksum)
			if seq == 4 {
				eof := true
				chunk.Eof = &eof
			}
			err = writer.WriteFrame(chunk)
			require.NoError(t, err)
		}
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", []byte(""), "text/plain")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Collect chunks
	var chunks []*Frame
	for i := 0; i < 5; i++ {
		chunk, err := reader.ReadFrame()
		require.NoError(t, err)
		chunks = append(chunks, chunk)
	}

	// Verify sequence numbers
	assert.Equal(t, 5, len(chunks))
	for i, chunk := range chunks {
		assert.Equal(t, uint64(i), chunk.Seq, "chunk seq must be contiguous from 0")
	}
	assert.NotNil(t, chunks[4].Eof)
	assert.True(t, *chunks[4].Eof)

	wg.Wait()
}

// Mirror-specific coverage: Test host request on a closed host returns error
func Test6531_RequestAfterShutdown(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		_, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)

		// Close immediately
		cartridgeRead.Close()
		cartridgeWrite.Close()
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	wg.Wait()

	// Close host connections
	hostWrite.Close()
	hostRead.Close()

	// Try to send request on closed connection - should fail
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", []byte(""), "application/json")
	err = writer.WriteFrame(request)
	assert.Error(t, err, "must fail on closed connection")
}

// Mirror-specific coverage: Test multiple arguments are correctly serialized in CBOR payload
func Test6532_ArgumentsMultiple(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	// Cartridge side
	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		// Read request
		frame, err := reader.ReadFrame()
		require.NoError(t, err)

		// Parse CBOR arguments
		var args []map[string]interface{}
		err = DecodeCBORValue(frame.Payload, &args)
		require.NoError(t, err)
		assert.Equal(t, 2, len(args), "should have 2 arguments")

		// Send response
		responseMsg := []byte("got 2 args")
		response := NewEnd(frame.Id, responseMsg)
		err = writer.WriteFrame(response)
		require.NoError(t, err)
	}()

	// Host side
	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Create multiple arguments
	args := []cap.CapArgumentValue{
		cap.NewCapArgumentValueFromStr("media:model-spec;enc=utf-8", "gpt-4"),
		cap.NewCapArgumentValue("media:ext=pdf", []byte{0x89, 0x50, 0x4E, 0x47}),
	}

	// Encode arguments to CBOR
	argsData, err := EncodeCapArgumentValues(args)
	require.NoError(t, err)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", argsData, "application/cbor")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Read response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, []byte("got 2 args"), response.Payload)

	wg.Wait()
}

// Mirror-specific coverage: Test auto-chunking splits payload larger than max_chunk into CHUNK frames + END frame,
// and host concatenated() reassembles the full original data
func Test6533_AutoChunkingReassembly(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		frame, err := reader.ReadFrame()
		require.NoError(t, err)

		// Simulate auto-chunking: 250 bytes with max_chunk=100
		maxChunk := 100
		data := make([]byte, 250)
		for i := range data {
			data[i] = byte(i % 256)
		}

		// Use WriteResponseWithChunking to do the splitting
		writer.SetLimits(Limits{MaxFrame: DefaultMaxFrame, MaxChunk: maxChunk})
		err = writer.WriteResponseWithChunking(frame.Id, "response", "application/octet-stream", data)
		require.NoError(t, err)
	}()

	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", nil, "text/plain")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Collect all frames until END
	var frames []*Frame
	for {
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		frames = append(frames, frame)
		if frame.FrameType == FrameTypeEnd {
			break
		}
	}

	// Protocol v2: STREAM_START + CHUNK(100) + CHUNK(100) + CHUNK(50) + STREAM_END + END
	assert.Equal(t, 6, len(frames), "250 bytes: STREAM_START + 3 CHUNK + STREAM_END + END")
	assert.Equal(t, FrameTypeStreamStart, frames[0].FrameType)
	assert.Equal(t, FrameTypeChunk, frames[1].FrameType)
	assert.Equal(t, FrameTypeChunk, frames[2].FrameType)
	assert.Equal(t, FrameTypeChunk, frames[3].FrameType)
	assert.Equal(t, FrameTypeStreamEnd, frames[4].FrameType)
	assert.Equal(t, FrameTypeEnd, frames[5].FrameType)

	// Reassemble CHUNK payloads only (not STREAM_START/END/END)
	var reassembled []byte
	for _, f := range frames {
		if f.FrameType == FrameTypeChunk {
			reassembled = append(reassembled, f.Payload...)
		}
	}
	expected := make([]byte, 250)
	for i := range expected {
		expected[i] = byte(i % 256)
	}
	assert.Equal(t, expected, reassembled, "concatenated chunks must match original data")

	wg.Wait()
}

// Mirror-specific coverage: Test payload exactly equal to max_chunk produces single END frame (no CHUNK frames)
func Test6535_ExactMaxChunkSingleEnd(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		frame, err := reader.ReadFrame()
		require.NoError(t, err)

		// Payload exactly max_chunk → single END
		data := make([]byte, 100)
		for i := range data {
			data[i] = 0xAB
		}
		writer.SetLimits(Limits{MaxFrame: DefaultMaxFrame, MaxChunk: 100})
		err = writer.WriteResponseWithChunking(frame.Id, "response", "application/octet-stream", data)
		require.NoError(t, err)
	}()

	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", nil, "text/plain")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Protocol v2: STREAM_START + CHUNK(100) + STREAM_END + END
	// Read all 4 frames
	var frames []*Frame
	for i := 0; i < 4; i++ {
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		frames = append(frames, frame)
	}

	assert.Equal(t, FrameTypeStreamStart, frames[0].FrameType)
	assert.Equal(t, FrameTypeChunk, frames[1].FrameType)
	assert.Equal(t, 100, len(frames[1].Payload), "CHUNK should have full 100 bytes")
	assert.Equal(t, FrameTypeStreamEnd, frames[2].FrameType)
	assert.Equal(t, FrameTypeEnd, frames[3].FrameType)

	wg.Wait()
}

// Mirror-specific coverage: Test payload of max_chunk + 1 produces exactly one CHUNK frame + one END frame
func Test6537_MaxChunkPlusOneSplitsIntoTwo(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		frame, err := reader.ReadFrame()
		require.NoError(t, err)

		// max_chunk=100, payload=101 → CHUNK(100) + END(1)
		data := make([]byte, 101)
		for i := range data {
			data[i] = byte(i)
		}
		writer.SetLimits(Limits{MaxFrame: DefaultMaxFrame, MaxChunk: 100})
		err = writer.WriteResponseWithChunking(frame.Id, "response", "application/octet-stream", data)
		require.NoError(t, err)
	}()

	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", nil, "text/plain")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Protocol v2: STREAM_START + CHUNK(100) + CHUNK(1) + STREAM_END + END
	var frames []*Frame
	for i := 0; i < 5; i++ {
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		frames = append(frames, frame)
	}

	assert.Equal(t, FrameTypeStreamStart, frames[0].FrameType)
	assert.Equal(t, FrameTypeChunk, frames[1].FrameType)
	assert.Equal(t, 100, len(frames[1].Payload))
	assert.Equal(t, FrameTypeChunk, frames[2].FrameType)
	assert.Equal(t, 1, len(frames[2].Payload))
	assert.Equal(t, FrameTypeStreamEnd, frames[3].FrameType)
	assert.Equal(t, FrameTypeEnd, frames[4].FrameType)

	// Verify reassembled data from CHUNKs
	var reassembled []byte
	for _, f := range frames {
		if f.FrameType == FrameTypeChunk {
			reassembled = append(reassembled, f.Payload...)
		}
	}
	expected := make([]byte, 101)
	for i := range expected {
		expected[i] = byte(i)
	}
	assert.Equal(t, expected, reassembled)

	wg.Wait()
}

// Mirror-specific coverage: Test that concatenated() returns full payload while final_payload() returns only last chunk
func Test6207_ConcatenatedVsFinalPayloadDivergence(t *testing.T) {
	chunks := []*ResponseChunk{
		{Payload: []byte("AAAA"), Seq: 0, IsEof: false},
		{Payload: []byte("BBBB"), Seq: 1, IsEof: false},
		{Payload: []byte("CCCC"), Seq: 2, IsEof: true},
	}

	response := &CartridgeResponse{
		Type:      CartridgeResponseTypeStreaming,
		Streaming: chunks,
	}

	// concatenated() returns ALL chunk data joined
	assert.Equal(t, "AAAABBBBCCCC", string(response.Concatenated()))

	// FinalPayload() returns ONLY the last chunk's data
	assert.Equal(t, "CCCC", string(response.FinalPayload()))

	// They must NOT be equal (this is the divergence the large_payload bug exposed)
	assert.NotEqual(t, response.Concatenated(), response.FinalPayload(),
		"concatenated and final_payload must diverge for multi-chunk responses")
}

// Mirror-specific coverage: Test auto-chunking preserves data integrity across chunk boundaries for 3x max_chunk payload
func Test6330_ChunkingDataIntegrity3x(t *testing.T) {
	hostWrite, cartridgeRead, cartridgeWrite, hostRead := createPipePair(t)
	defer hostWrite.Close()
	defer cartridgeRead.Close()
	defer cartridgeWrite.Close()
	defer hostRead.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	pattern := []byte("ABCDEFGHIJ")
	expected := make([]byte, 300)
	for i := range expected {
		expected[i] = pattern[i%len(pattern)]
	}

	go func() {
		defer wg.Done()
		reader := NewFrameReader(cartridgeRead)
		writer := NewFrameWriter(cartridgeWrite)

		limits, err := HandshakeAccept(reader, writer, []byte(testCBORManifest))
		require.NoError(t, err)
		reader.SetLimits(limits)
		writer.SetLimits(limits)

		frame, err := reader.ReadFrame()
		require.NoError(t, err)

		// 300 bytes with max_chunk=100 → CHUNK(100) + CHUNK(100) + END(100)
		writer.SetLimits(Limits{MaxFrame: DefaultMaxFrame, MaxChunk: 100})
		err = writer.WriteResponseWithChunking(frame.Id, "response", "application/octet-stream", expected)
		require.NoError(t, err)
	}()

	reader := NewFrameReader(hostRead)
	writer := NewFrameWriter(hostWrite)

	_, limits, err := HandshakeInitiate(reader, writer)
	require.NoError(t, err)
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:test", nil, "text/plain")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Collect all frames
	var frames []*Frame
	for {
		frame, err := reader.ReadFrame()
		require.NoError(t, err)
		frames = append(frames, frame)
		if frame.FrameType == FrameTypeEnd {
			break
		}
	}

	// Protocol v2: STREAM_START + CHUNK(100) + CHUNK(100) + CHUNK(100) + STREAM_END + END
	assert.Equal(t, 6, len(frames), "300 bytes: STREAM_START + 3 CHUNK + STREAM_END + END")

	// Reassemble CHUNK payloads only
	var reassembled []byte
	for _, f := range frames {
		if f.FrameType == FrameTypeChunk {
			reassembled = append(reassembled, f.Payload...)
		}
	}
	assert.Equal(t, 300, len(reassembled))
	assert.Equal(t, expected, reassembled, "pattern must be preserved across chunk boundaries")

	wg.Wait()
}

// =============================================================================
// Full-path host-runtime integration tests (engine → host → cartridge → back).
//
// Mirrors capdag/src/bifaci/integration_tests.rs. The Rust CartridgeHostRuntime
// maps to the Go CartridgeHost (AttachCartridge + Run). Cartridge simulators do
// handshake on their side, then handle the routed REQ and stream a response. The
// engine writes REQs (with routing_id/XID, as the RelaySwitch stamps in prod)
// over the relay pipe and reads responses, skipping the host's RelayNotify
// inventory frames via recvCartridgeFrame (defined in host_multi_test.go).
//
// Rust's attach_cartridge() runs verify_identity() inline, so the Rust tests use
// a cartridge_handshake_with_identity helper that answers an identity REQ during
// attach. The Go CartridgeHost.AttachCartridge now mirrors this: it runs
// VerifyIdentity after the handshake, and simulateCartridge answers the identity
// REQ (echoIdentityRequest) before invoking the test handler. The handlers below
// therefore read the real routed REQ first; the request/response assertions are
// mirrored 1:1.
// =============================================================================

// TEST1122: Full path: engine REQ → runtime → cartridge → response back through relay
func Test1122_FullPathEngineReqToCartridgeResponse(t *testing.T) {
	manifest := `{"name":"EchoCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Echo test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Test","command":"test","args":[]}]}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			require.NoError(t, err)
			assert.Equal(t, FrameTypeReq, req.FrameType)
			require.NotNil(t, req.Cap)
			assert.Equal(t, standard.CapIdentity, *req.Cap)

			// Collect the request argument data (CHUNK payloads) until END.
			var argData []byte
			for {
				f, err := r.ReadFrame()
				require.NoError(t, err)
				if f.FrameType == FrameTypeChunk {
					argData = append(argData, f.Payload...)
				}
				if f.FrameType == FrameTypeEnd {
					break
				}
			}

			seq := NewSeqAssigner()
			sid := "resp"
			start := NewStreamStart(req.Id, sid, "media:", nil)
			seq.Assign(start)
			require.NoError(t, w.WriteFrame(start))
			checksum := ComputeChecksum(argData)
			chunk := NewChunk(req.Id, sid, 0, argData, 0, checksum)
			seq.Assign(chunk)
			require.NoError(t, w.WriteFrame(chunk))
			streamEnd := NewStreamEnd(req.Id, sid, 1)
			seq.Assign(streamEnd)
			require.NoError(t, w.WriteFrame(streamEnd))
			end := NewEnd(req.Id, nil)
			seq.Assign(end)
			require.NoError(t, w.WriteFrame(end))
			seq.Remove(FlowKeyFromFrame(end))
		})
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	var response []byte
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := NewFrameWriter(engineWrite)
		r := NewFrameReader(engineRead)

		seq := NewSeqAssigner()
		reqId := NewMessageIdRandom()
		xid := NewMessageIdFromUint(1)
		sid := NewMessageIdRandom().ToString()

		reqFrame := NewReq(reqId, standard.CapIdentity, []byte{}, "text/plain")
		reqFrame.RoutingId = &xid
		seq.Assign(reqFrame)
		require.NoError(t, w.WriteFrame(reqFrame))

		streamStart := NewStreamStart(reqId, sid, "media:", nil)
		streamStart.RoutingId = &xid
		seq.Assign(streamStart)
		require.NoError(t, w.WriteFrame(streamStart))

		payload := []byte("hello world")
		checksum := ComputeChecksum(payload)
		chunk := NewChunk(reqId, sid, 0, payload, 0, checksum)
		chunk.RoutingId = &xid
		seq.Assign(chunk)
		require.NoError(t, w.WriteFrame(chunk))

		streamEnd := NewStreamEnd(reqId, sid, 1)
		streamEnd.RoutingId = &xid
		seq.Assign(streamEnd)
		require.NoError(t, w.WriteFrame(streamEnd))

		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		seq.Assign(end)
		require.NoError(t, w.WriteFrame(end))
		seq.Remove(FlowKeyFromFrame(end))

		// Read response, accumulating CHUNK payloads until END.
		for {
			frame, err := recvCartridgeFrame(r)
			if err != nil {
				break
			}
			if frame.FrameType == FrameTypeChunk {
				response = append(response, frame.Payload...)
			}
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

	assert.Equal(t, []byte("hello world"), response, "Cartridge should echo back the argument data")
}

// TEST1123: Cartridge ERR frame flows back to engine through relay
func Test1123_CartridgeErrorFlowsToEngine(t *testing.T) {
	manifest := `{"name":"ErrCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Error test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";fail;out=\"media:void\"","title":"Test","command":"test","args":[]}]}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			require.NoError(t, err)
			seq := NewSeqAssigner()
			errFrame := NewErr(req.Id, "FAIL_CODE", "Something went wrong")
			seq.Assign(errFrame)
			require.NoError(t, w.WriteFrame(errFrame))
			seq.Remove(FlowKeyFromFrame(errFrame))
		})
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	var errCode, errMsg string
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := NewFrameWriter(engineWrite)
		r := NewFrameReader(engineRead)

		seq := NewSeqAssigner()
		reqId := NewMessageIdRandom()
		xid := NewMessageIdFromUint(1)
		req := NewReq(reqId, `cap:in="media:void";fail;out="media:void"`, []byte{}, "text/plain")
		req.RoutingId = &xid
		seq.Assign(req)
		require.NoError(t, w.WriteFrame(req))
		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		seq.Assign(end)
		require.NoError(t, w.WriteFrame(end))
		seq.Remove(FlowKeyFromFrame(end))

		for {
			frame, err := recvCartridgeFrame(r)
			if err != nil {
				break
			}
			if frame.FrameType == FrameTypeErr {
				errCode = frame.ErrorCode()
				errMsg = frame.ErrorMessage()
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

	assert.Equal(t, "FAIL_CODE", errCode)
	assert.Equal(t, "Something went wrong", errMsg)
}

// TEST898: Binary data integrity through full relay path (256 byte values)
func Test898_BinaryIntegrityThroughRelay(t *testing.T) {
	manifest := `{"name":"BinCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Binary test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";binary;out=\"media:void\"","title":"Test","command":"test","args":[]}]}]}`

	binaryData := make([]byte, 256)
	for i := 0; i < 256; i++ {
		binaryData[i] = byte(i)
	}

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			require.NoError(t, err)

			var received []byte
			for {
				f, err := r.ReadFrame()
				require.NoError(t, err)
				if f.FrameType == FrameTypeChunk {
					received = append(received, f.Payload...)
				}
				if f.FrameType == FrameTypeEnd {
					break
				}
			}

			assert.Equal(t, 256, len(received), "Must receive all 256 bytes")
			for i, b := range received {
				assert.Equal(t, byte(i), b, "Byte mismatch at position %d", i)
			}

			seq := NewSeqAssigner()
			sid := "resp"
			start := NewStreamStart(req.Id, sid, "media:", nil)
			seq.Assign(start)
			require.NoError(t, w.WriteFrame(start))
			checksum := ComputeChecksum(received)
			chunk := NewChunk(req.Id, sid, 0, received, 0, checksum)
			seq.Assign(chunk)
			require.NoError(t, w.WriteFrame(chunk))
			streamEnd := NewStreamEnd(req.Id, sid, 1)
			seq.Assign(streamEnd)
			require.NoError(t, w.WriteFrame(streamEnd))
			end := NewEnd(req.Id, nil)
			seq.Assign(end)
			require.NoError(t, w.WriteFrame(end))
			seq.Remove(FlowKeyFromFrame(end))
		})
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	var response []byte
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := NewFrameWriter(engineWrite)
		r := NewFrameReader(engineRead)

		seq := NewSeqAssigner()
		reqId := NewMessageIdRandom()
		xid := NewMessageIdFromUint(1)
		sid := NewMessageIdRandom().ToString()
		req := NewReq(reqId, `cap:in="media:void";binary;out="media:void"`, []byte{}, "application/octet-stream")
		req.RoutingId = &xid
		seq.Assign(req)
		require.NoError(t, w.WriteFrame(req))
		streamStart := NewStreamStart(reqId, sid, "media:", nil)
		streamStart.RoutingId = &xid
		seq.Assign(streamStart)
		require.NoError(t, w.WriteFrame(streamStart))
		checksum := ComputeChecksum(binaryData)
		chunk := NewChunk(reqId, sid, 0, binaryData, 0, checksum)
		chunk.RoutingId = &xid
		seq.Assign(chunk)
		require.NoError(t, w.WriteFrame(chunk))
		streamEnd := NewStreamEnd(reqId, sid, 1)
		streamEnd.RoutingId = &xid
		seq.Assign(streamEnd)
		require.NoError(t, w.WriteFrame(streamEnd))
		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		seq.Assign(end)
		require.NoError(t, w.WriteFrame(end))
		seq.Remove(FlowKeyFromFrame(end))

		for {
			frame, err := recvCartridgeFrame(r)
			if err != nil {
				break
			}
			if frame.FrameType == FrameTypeChunk {
				response = append(response, frame.Payload...)
			}
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

	assert.Equal(t, 256, len(response))
	for i, b := range response {
		assert.Equal(t, byte(i), b, "Response byte mismatch at position %d", i)
	}
}

// TEST899: Streaming chunks flow through relay without accumulation
func Test899_StreamingChunksThroughRelay(t *testing.T) {
	manifest := `{"name":"StreamCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Streaming test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";stream;out=\"media:void\"","title":"Test","command":"test","args":[]}]}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			require.NoError(t, err)

			for {
				f, err := r.ReadFrame()
				require.NoError(t, err)
				if f.FrameType == FrameTypeEnd {
					break
				}
			}

			sid := "resp"
			seq := NewSeqAssigner()
			start := NewStreamStart(req.Id, sid, "media:", nil)
			seq.Assign(start)
			require.NoError(t, w.WriteFrame(start))
			for idx := uint64(0); idx < 5; idx++ {
				data := []byte(fmt.Sprintf("chunk%d", idx))
				checksum := ComputeChecksum(data)
				chunk := NewChunk(req.Id, sid, 0, data, idx, checksum)
				seq.Assign(chunk)
				require.NoError(t, w.WriteFrame(chunk))
			}
			streamEnd := NewStreamEnd(req.Id, sid, 5)
			seq.Assign(streamEnd)
			require.NoError(t, w.WriteFrame(streamEnd))
			end := NewEnd(req.Id, nil)
			seq.Assign(end)
			require.NoError(t, w.WriteFrame(end))
		})
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	type chunkPair struct {
		seq  uint64
		data []byte
	}
	var chunks []chunkPair
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := NewFrameWriter(engineWrite)
		r := NewFrameReader(engineRead)

		seq := NewSeqAssigner()
		reqId := NewMessageIdRandom()
		xid := NewMessageIdFromUint(1)
		req := NewReq(reqId, `cap:in="media:void";stream;out="media:void"`, []byte{}, "text/plain")
		req.RoutingId = &xid
		seq.Assign(req)
		require.NoError(t, w.WriteFrame(req))
		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		seq.Assign(end)
		require.NoError(t, w.WriteFrame(end))
		seq.Remove(FlowKeyFromFrame(end))

		for {
			frame, err := recvCartridgeFrame(r)
			if err != nil {
				break
			}
			if frame.FrameType == FrameTypeChunk {
				chunks = append(chunks, chunkPair{frame.Seq, frame.Payload})
			}
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

	assert.Equal(t, 5, len(chunks), "All 5 chunks must arrive")
	for i, c := range chunks {
		assert.Equal(t, uint64(i+1), c.seq, "Chunk seq must be contiguous from 1 (StreamStart takes seq 0)")
		assert.Equal(t, []byte(fmt.Sprintf("chunk%d", i)), c.data, "Chunk data must match")
	}
}

// TEST900: Two cartridges routed independently by cap_urn
func Test900_TwoCartridgesRoutedIndependently(t *testing.T) {
	manifestA := `{"name":"CartridgeA","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge A","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";alpha;out=\"media:void\"","title":"Test","command":"test","args":[]}]}]}`
	manifestB := `{"name":"CartridgeB","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge B","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";beta;out=\"media:void\"","title":"Test","command":"test","args":[]}]}]}`

	hostReadA, cartridgeWriteA := net.Pipe()
	cartridgeReadA, hostWriteA := net.Pipe()
	hostReadB, cartridgeWriteB := net.Pipe()
	cartridgeReadB, hostWriteB := net.Pipe()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadA, cartridgeWriteA, manifestA, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			require.NoError(t, err)
			require.NotNil(t, req.Cap)
			assert.Equal(t, `cap:in="media:void";alpha;out="media:void"`, *req.Cap, "Cartridge A must receive alpha REQ")
			for {
				f, err := r.ReadFrame()
				require.NoError(t, err)
				if f.FrameType == FrameTypeEnd {
					break
				}
			}
			seq := NewSeqAssigner()
			sid := "a"
			start := NewStreamStart(req.Id, sid, "media:", nil)
			seq.Assign(start)
			require.NoError(t, w.WriteFrame(start))
			payload := []byte("from-alpha")
			checksum := ComputeChecksum(payload)
			chunk := NewChunk(req.Id, sid, 0, payload, 0, checksum)
			seq.Assign(chunk)
			require.NoError(t, w.WriteFrame(chunk))
			streamEnd := NewStreamEnd(req.Id, sid, 1)
			seq.Assign(streamEnd)
			require.NoError(t, w.WriteFrame(streamEnd))
			end := NewEnd(req.Id, nil)
			seq.Assign(end)
			require.NoError(t, w.WriteFrame(end))
			seq.Remove(FlowKeyFromFrame(end))
		})
		cartridgeReadA.Close()
		cartridgeWriteA.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadB, cartridgeWriteB, manifestB, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			require.NoError(t, err)
			require.NotNil(t, req.Cap)
			assert.Equal(t, `cap:in="media:void";beta;out="media:void"`, *req.Cap, "Cartridge B must receive beta REQ")
			for {
				f, err := r.ReadFrame()
				require.NoError(t, err)
				if f.FrameType == FrameTypeEnd {
					break
				}
			}
			seq := NewSeqAssigner()
			sid := "b"
			start := NewStreamStart(req.Id, sid, "media:", nil)
			seq.Assign(start)
			require.NoError(t, w.WriteFrame(start))
			payload := []byte("from-beta")
			checksum := ComputeChecksum(payload)
			chunk := NewChunk(req.Id, sid, 0, payload, 0, checksum)
			seq.Assign(chunk)
			require.NoError(t, w.WriteFrame(chunk))
			streamEnd := NewStreamEnd(req.Id, sid, 1)
			seq.Assign(streamEnd)
			require.NoError(t, w.WriteFrame(streamEnd))
			end := NewEnd(req.Id, nil)
			seq.Assign(end)
			require.NoError(t, w.WriteFrame(end))
			seq.Remove(FlowKeyFromFrame(end))
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

	alphaId := NewMessageIdRandom()
	betaId := NewMessageIdRandom()
	var alphaData, betaData []byte
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := NewFrameWriter(engineWrite)
		r := NewFrameReader(engineRead)

		seq := NewSeqAssigner()
		xidAlpha := NewMessageIdFromUint(1)
		xidBeta := NewMessageIdFromUint(2)

		reqAlpha := NewReq(alphaId, `cap:in="media:void";alpha;out="media:void"`, []byte{}, "text/plain")
		reqAlpha.RoutingId = &xidAlpha
		seq.Assign(reqAlpha)
		require.NoError(t, w.WriteFrame(reqAlpha))
		endAlpha := NewEnd(alphaId, nil)
		endAlpha.RoutingId = &xidAlpha
		seq.Assign(endAlpha)
		require.NoError(t, w.WriteFrame(endAlpha))
		seq.Remove(FlowKeyFromFrame(endAlpha))

		reqBeta := NewReq(betaId, `cap:in="media:void";beta;out="media:void"`, []byte{}, "text/plain")
		reqBeta.RoutingId = &xidBeta
		seq.Assign(reqBeta)
		require.NoError(t, w.WriteFrame(reqBeta))
		endBeta := NewEnd(betaId, nil)
		endBeta.RoutingId = &xidBeta
		seq.Assign(endBeta)
		require.NoError(t, w.WriteFrame(endBeta))
		seq.Remove(FlowKeyFromFrame(endBeta))

		endsReceived := 0
		for {
			f, err := recvCartridgeFrame(r)
			if err != nil {
				break
			}
			if f.FrameType == FrameTypeChunk {
				if f.Id.ToString() == alphaId.ToString() {
					alphaData = append(alphaData, f.Payload...)
				} else if f.Id.ToString() == betaId.ToString() {
					betaData = append(betaData, f.Payload...)
				}
			}
			if f.FrameType == FrameTypeEnd {
				endsReceived++
				if endsReceived >= 2 {
					break
				}
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

	assert.Equal(t, []byte("from-alpha"), alphaData, "Alpha response must come from Cartridge A")
	assert.Equal(t, []byte("from-beta"), betaData, "Beta response must come from Cartridge B")
}

// TEST901: REQ for unknown cap returns ERR frame (not fatal)
func Test901_ReqForUnknownCapReturnsErrFrame(t *testing.T) {
	manifest := `{"name":"OneCartridge","version":"1.0","channel":"release","registry_url":null,"description":"Known cap cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";known;out=\"media:void\"","title":"Test","command":"test","args":[]}]}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Cartridge waits for EOF — no REQ should arrive since the cap is unknown.
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			frame, err := r.ReadFrame()
			if err == nil {
				assert.Fail(t, "Cartridge should not receive frames for unknown cap", "got %v", frame.FrameType)
			}
		})
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	reqId := NewMessageIdRandom()
	var errFrame *Frame
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := NewFrameWriter(engineWrite)
		r := NewFrameReader(engineRead)

		seq := NewSeqAssigner()
		xid := NewMessageIdFromUint(1)
		req := NewReq(reqId, `cap:in="media:void";unknown;out="media:void"`, []byte{}, "text/plain")
		req.RoutingId = &xid
		seq.Assign(req)
		require.NoError(t, w.WriteFrame(req))
		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		seq.Assign(end)
		require.NoError(t, w.WriteFrame(end))
		seq.Remove(FlowKeyFromFrame(end))

		// Read the ERR frame from the host (skipping RelayNotify inventory frames).
		frame, err := recvCartridgeFrame(r)
		require.NoError(t, err)
		errFrame = frame

		engineWrite.Close()
		engineRead.Close()
	}()

	// Host run should NOT fail — it sends an ERR frame and continues.
	host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()
	hostReadP.Close()
	hostWriteP.Close()
	wg.Wait()

	require.NotNil(t, errFrame, "Should get ERR for unknown cap")
	assert.Equal(t, FrameTypeErr, errFrame.FrameType, "Should get ERR for unknown cap")
	assert.Equal(t, reqId.ToString(), errFrame.Id.ToString(), "ERR should reference the original request ID")
	assert.Equal(t, "NO_HANDLER", errFrame.ErrorCode(), "Error code should be NO_HANDLER")
}

// TEST489: Full path identity verification: engine → host (AttachCartridge) → cartridge
//
// In both the Rust and Go mirrors, attach_cartridge runs identity verification
// end-to-end (simulateCartridge answers the identity REQ during attach); this
// then verifies that after attach the cartridge is live and handles a real
// request through the full relay path.
func Test489_FullPathIdentityVerification(t *testing.T) {
	manifest := `{"name":"IdentityE2E","version":"1.0","channel":"release","registry_url":null,"description":"Identity test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";test;out=\"media:void\"","title":"Test","command":"test","args":[]}]}]}`

	hostReadP, cartridgeWriteP := net.Pipe()
	cartridgeReadP, hostWriteP := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadP, cartridgeWriteP, manifest, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			require.NoError(t, err)
			assert.Equal(t, FrameTypeReq, req.FrameType, "Must receive real REQ")

			for {
				f, err := r.ReadFrame()
				require.NoError(t, err)
				if f.FrameType == FrameTypeEnd {
					break
				}
			}

			seq := NewSeqAssigner()
			sid := "resp"
			ss := NewStreamStart(req.Id, sid, "media:", nil)
			seq.Assign(ss)
			require.NoError(t, w.WriteFrame(ss))
			payload := []byte("verified-and-working")
			checksum := ComputeChecksum(payload)
			chunk := NewChunk(req.Id, sid, 0, payload, 0, checksum)
			seq.Assign(chunk)
			require.NoError(t, w.WriteFrame(chunk))
			se := NewStreamEnd(req.Id, sid, 1)
			seq.Assign(se)
			require.NoError(t, w.WriteFrame(se))
			end := NewEnd(req.Id, nil)
			seq.Assign(end)
			require.NoError(t, w.WriteFrame(end))
		})
		cartridgeReadP.Close()
		cartridgeWriteP.Close()
	}()

	host := NewCartridgeHost()
	_, err := host.AttachCartridge(hostReadP, hostWriteP)
	require.NoError(t, err)

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	var response []byte
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := NewFrameWriter(engineWrite)
		r := NewFrameReader(engineRead)

		seq := NewSeqAssigner()
		reqId := NewMessageIdRandom()
		xid := NewMessageIdFromUint(1)
		sid := NewMessageIdRandom().ToString()

		req := NewReq(reqId, `cap:in="media:void";test;out="media:void"`, []byte{}, "text/plain")
		req.RoutingId = &xid
		seq.Assign(req)
		require.NoError(t, w.WriteFrame(req))
		ss := NewStreamStart(reqId, sid, "media:", nil)
		ss.RoutingId = &xid
		seq.Assign(ss)
		require.NoError(t, w.WriteFrame(ss))
		payload := []byte("test-data")
		checksum := ComputeChecksum(payload)
		chunk := NewChunk(reqId, sid, 0, payload, 0, checksum)
		chunk.RoutingId = &xid
		seq.Assign(chunk)
		require.NoError(t, w.WriteFrame(chunk))
		se := NewStreamEnd(reqId, sid, 1)
		se.RoutingId = &xid
		seq.Assign(se)
		require.NoError(t, w.WriteFrame(se))
		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		seq.Assign(end)
		require.NoError(t, w.WriteFrame(end))

		for {
			frame, err := recvCartridgeFrame(r)
			if err != nil {
				break
			}
			if frame.FrameType == FrameTypeChunk {
				response = append(response, frame.Payload...)
			}
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

	assert.Equal(t, []byte("verified-and-working"), response, "Cartridge must respond after attach")
}

// TEST490: Identity verification with multiple cartridges through single relay
//
// Both cartridges must be live and routed independently after attach. Each
// cartridge answers the identity REQ during attach (simulateCartridge), matching
// Rust's attach_cartridge identity verification.
func Test490_IdentityVerificationMultipleCartridges(t *testing.T) {
	manifestA := `{"name":"CartridgeA","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge A","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";alpha;out=\"media:void\"","title":"Alpha","command":"alpha","args":[]}]}]}`
	manifestB := `{"name":"CartridgeB","version":"1.0","channel":"release","registry_url":null,"description":"Cartridge B","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity","args":[]},{"urn":"cap:in=\"media:void\";beta;out=\"media:void\"","title":"Beta","command":"beta","args":[]}]}]}`

	hostReadA, cartridgeWriteA := net.Pipe()
	cartridgeReadA, hostWriteA := net.Pipe()
	hostReadB, cartridgeWriteB := net.Pipe()
	cartridgeReadB, hostWriteB := net.Pipe()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadA, cartridgeWriteA, manifestA, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			require.NoError(t, err)
			require.NotNil(t, req.Cap)
			assert.Equal(t, `cap:in="media:void";alpha;out="media:void"`, *req.Cap)
			for {
				f, err := r.ReadFrame()
				require.NoError(t, err)
				if f.FrameType == FrameTypeEnd {
					break
				}
			}
			seq := NewSeqAssigner()
			sid := "a"
			ss := NewStreamStart(req.Id, sid, "media:", nil)
			seq.Assign(ss)
			require.NoError(t, w.WriteFrame(ss))
			payload := []byte("from-alpha")
			checksum := ComputeChecksum(payload)
			chunk := NewChunk(req.Id, sid, 0, payload, 0, checksum)
			seq.Assign(chunk)
			require.NoError(t, w.WriteFrame(chunk))
			se := NewStreamEnd(req.Id, sid, 1)
			seq.Assign(se)
			require.NoError(t, w.WriteFrame(se))
			end := NewEnd(req.Id, nil)
			seq.Assign(end)
			require.NoError(t, w.WriteFrame(end))
		})
		cartridgeReadA.Close()
		cartridgeWriteA.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateCartridge(t, cartridgeReadB, cartridgeWriteB, manifestB, func(r *FrameReader, w *FrameWriter) {
			req, err := r.ReadFrame()
			require.NoError(t, err)
			require.NotNil(t, req.Cap)
			assert.Equal(t, `cap:in="media:void";beta;out="media:void"`, *req.Cap)
			for {
				f, err := r.ReadFrame()
				require.NoError(t, err)
				if f.FrameType == FrameTypeEnd {
					break
				}
			}
			seq := NewSeqAssigner()
			sid := "b"
			ss := NewStreamStart(req.Id, sid, "media:", nil)
			seq.Assign(ss)
			require.NoError(t, w.WriteFrame(ss))
			payload := []byte("from-beta")
			checksum := ComputeChecksum(payload)
			chunk := NewChunk(req.Id, sid, 0, payload, 0, checksum)
			seq.Assign(chunk)
			require.NoError(t, w.WriteFrame(chunk))
			se := NewStreamEnd(req.Id, sid, 1)
			seq.Assign(se)
			require.NoError(t, w.WriteFrame(se))
			end := NewEnd(req.Id, nil)
			seq.Assign(end)
			require.NoError(t, w.WriteFrame(end))
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

	var respAlpha, respBeta []byte
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := NewFrameWriter(engineWrite)
		r := NewFrameReader(engineRead)
		seq := NewSeqAssigner()
		xid := NewMessageIdFromUint(1)

		// Alpha request
		reqId := NewMessageIdRandom()
		sid := NewMessageIdRandom().ToString()
		req := NewReq(reqId, `cap:in="media:void";alpha;out="media:void"`, []byte{}, "text/plain")
		req.RoutingId = &xid
		seq.Assign(req)
		require.NoError(t, w.WriteFrame(req))
		ss := NewStreamStart(reqId, sid, "media:", nil)
		ss.RoutingId = &xid
		seq.Assign(ss)
		require.NoError(t, w.WriteFrame(ss))
		payloadA := []byte("alpha-data")
		checksum := ComputeChecksum(payloadA)
		chunk := NewChunk(reqId, sid, 0, payloadA, 0, checksum)
		chunk.RoutingId = &xid
		seq.Assign(chunk)
		require.NoError(t, w.WriteFrame(chunk))
		se := NewStreamEnd(reqId, sid, 1)
		se.RoutingId = &xid
		seq.Assign(se)
		require.NoError(t, w.WriteFrame(se))
		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		seq.Assign(end)
		require.NoError(t, w.WriteFrame(end))

		for {
			f, err := recvCartridgeFrame(r)
			if err != nil {
				break
			}
			if f.FrameType == FrameTypeChunk {
				respAlpha = append(respAlpha, f.Payload...)
			}
			if f.FrameType == FrameTypeEnd {
				break
			}
		}

		// Beta request
		reqId2 := NewMessageIdRandom()
		xid2 := NewMessageIdFromUint(2)
		sid2 := NewMessageIdRandom().ToString()
		req2 := NewReq(reqId2, `cap:in="media:void";beta;out="media:void"`, []byte{}, "text/plain")
		req2.RoutingId = &xid2
		seq.Assign(req2)
		require.NoError(t, w.WriteFrame(req2))
		ss2 := NewStreamStart(reqId2, sid2, "media:", nil)
		ss2.RoutingId = &xid2
		seq.Assign(ss2)
		require.NoError(t, w.WriteFrame(ss2))
		payloadB := []byte("beta-data")
		checksum2 := ComputeChecksum(payloadB)
		chunk2 := NewChunk(reqId2, sid2, 0, payloadB, 0, checksum2)
		chunk2.RoutingId = &xid2
		seq.Assign(chunk2)
		require.NoError(t, w.WriteFrame(chunk2))
		se2 := NewStreamEnd(reqId2, sid2, 1)
		se2.RoutingId = &xid2
		seq.Assign(se2)
		require.NoError(t, w.WriteFrame(se2))
		end2 := NewEnd(reqId2, nil)
		end2.RoutingId = &xid2
		seq.Assign(end2)
		require.NoError(t, w.WriteFrame(end2))

		for {
			f, err := recvCartridgeFrame(r)
			if err != nil {
				break
			}
			if f.FrameType == FrameTypeChunk {
				respBeta = append(respBeta, f.Payload...)
			}
			if f.FrameType == FrameTypeEnd {
				break
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

	assert.Equal(t, []byte("from-alpha"), respAlpha, "Alpha cartridge must respond correctly after attach")
	assert.Equal(t, []byte("from-beta"), respBeta, "Beta cartridge must respond correctly after attach")
}

// Helper functions

// DecodeCBORValue decodes CBOR bytes to any interface{}
func DecodeCBORValue(data []byte, v interface{}) error {
	return cbor2.Unmarshal(data, v)
}

// EncodeCapArgumentValues encodes cap.CapArgumentValue slice to CBOR
func EncodeCapArgumentValues(args []cap.CapArgumentValue) ([]byte, error) {
	// Convert to CBOR-friendly format
	var cborArgs []map[string]interface{}
	for _, arg := range args {
		argMap := map[string]interface{}{
			"media_urn": arg.MediaUrn,
			"value":     arg.Value,
		}
		cborArgs = append(cborArgs, argMap)
	}

	return cbor2.Marshal(cborArgs)
}
