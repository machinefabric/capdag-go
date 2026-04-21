package bifaci

import (
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
	cbor2 "github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create test registry
func createTestRegistry(t *testing.T) *media.MediaUrnRegistry {
	t.Helper()
	registry, err := media.NewMediaUrnRegistry()
	require.NoError(t, err)
	return registry
}

// Test helper for integration tests - use proper media URNs with tags
func intTestUrn(tags string) string {
	if tags == "" {
		return `cap:in="media:void";out="media:json;record;textable"`
	}
	return `cap:in="media:void";out="media:json;record;textable";` + tags
}


// TestIntegrationVersionlessCapCreation verifies caps can be created without version fields
func TestIntegrationVersionlessCapCreation(t *testing.T) {
	// Test case 1: Create cap without version parameter
	// Use type=data_processing key=value instead of flag
	capUrn, err := urn.NewCapUrnFromString(intTestUrn("op=transform;format=json;type=data_processing"))
	require.NoError(t, err)

	capDef := cap.NewCap(capUrn, "Data Transformer", "transform-command")

	// Verify the cap has direction specs in canonical form
	assert.Contains(t, capDef.UrnString(), `in=media:void`)
	assert.Contains(t, capDef.UrnString(), `out="media:json;record;textable"`)
	assert.Equal(t, "transform-command", capDef.Command)

	// Test case 2: Create cap with description but no version
	capDef2 := cap.NewCapWithDescription(capUrn, "Data Transformer", "transform-command", "Transforms data")
	assert.NotNil(t, capDef2.CapDescription)
	assert.Equal(t, "Transforms data", *capDef2.CapDescription)

	// Test case 3: Verify caps can be compared without version
	assert.True(t, capDef.Equals(capDef))

	// Different caps should not be equal
	urn2, _ := urn.NewCapUrnFromString(intTestUrn("op=generate;format=pdf"))
	capDef3 := cap.NewCap(urn2, "PDF Generator", "generate-command")
	assert.False(t, capDef.Equals(capDef3))
}

// TestIntegrationCaseInsensitiveUrns verifies URNs are case-insensitive
func TestIntegrationCaseInsensitiveUrns(t *testing.T) {
	// Test case 1: Different case inputs should produce same URN
	urn1, err := urn.NewCapUrnFromString(intTestUrn("OP=Transform;FORMAT=JSON;Type=Data_Processing"))
	require.NoError(t, err)

	urn2, err := urn.NewCapUrnFromString(intTestUrn("op=transform;format=json;type=data_processing"))
	require.NoError(t, err)

	// URNs should be equal (case-insensitive keys and unquoted values)
	assert.True(t, urn1.Equals(urn2))
	assert.Equal(t, urn1.ToString(), urn2.ToString())

	// Test case 2: Case-insensitive tag operations
	op, exists := urn1.GetTag("OP")
	assert.True(t, exists)
	assert.Equal(t, "transform", op) // Should be normalized to lowercase

	op2, exists := urn1.GetTag("op")
	assert.True(t, exists)
	assert.Equal(t, "transform", op2)

	// Test case 3: HasTag - keys case-insensitive, values case-sensitive
	assert.True(t, urn1.HasTag("OP", "transform"))
	assert.True(t, urn1.HasTag("op", "transform"))
	assert.True(t, urn1.HasTag("Op", "transform"))
	assert.False(t, urn1.HasTag("op", "TRANSFORM"))

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

// TestIntegrationCapValidation verifies cap schema validation
func TestIntegrationCapValidation(t *testing.T) {
	registry := createTestRegistry(t)
	coordinator := cap.NewCapValidationCoordinator()

	// Create a cap with arguments - use proper tags
	urn, err := urn.NewCapUrnFromString(`cap:in="media:void";op=process;out="media:json;record;textable";target=data`)
	require.NoError(t, err)

	capDef := cap.NewCap(urn, "Data Processor", "process-data")

	// Add mediaSpecs for resolution
	capDef.SetMediaSpecs([]media.MediaSpecDef{
		{Urn: standard.MediaJSON, MediaType: "application/json", ProfileURI: media.ProfileObj},
		{Urn: standard.MediaString, MediaType: "text/plain", ProfileURI: media.ProfileStr},
	})

	// Add required string argument using new architecture
	cliFlag1 := "--input"
	pos1 := 0
	capDef.AddArg(cap.CapArg{
		MediaUrn:       standard.MediaString,
		Required:       true,
		Sources:        []cap.ArgSource{{CliFlag: &cliFlag1}, {Position: &pos1}},
		ArgDescription: "Input path",
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

// TestIntegrationMediaUrnResolution verifies media URN resolution
func TestIntegrationMediaUrnResolution(t *testing.T) {
	registry := createTestRegistry(t)

	// mediaSpecs for resolution - no built-in resolution, must provide specs
	mediaSpecs := []media.MediaSpecDef{
		{Urn: standard.MediaString, MediaType: "text/plain", ProfileURI: media.ProfileStr},
		{Urn: standard.MediaJSON, MediaType: "application/json", ProfileURI: media.ProfileObj},
		{Urn: standard.MediaIdentity, MediaType: "application/octet-stream"},
	}

	// Test string media URN resolution
	resolved, err := media.ResolveMediaUrn(standard.MediaString, mediaSpecs, registry)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", resolved.MediaType)
	assert.Equal(t, media.ProfileStr, resolved.ProfileURI)
	assert.False(t, resolved.IsBinary())
	assert.False(t, resolved.IsJSON())
	assert.True(t, resolved.IsText())

	// Test JSON media URN
	resolved, err = media.ResolveMediaUrn(standard.MediaJSON, mediaSpecs, registry)
	require.NoError(t, err)
	assert.Equal(t, "application/json", resolved.MediaType)
	assert.True(t, resolved.IsRecord())
	assert.True(t, resolved.IsStructured())
	assert.True(t, resolved.IsJSON()) // MediaJSON has json marker tag

	// Test binary media URN
	resolved, err = media.ResolveMediaUrn(standard.MediaIdentity, mediaSpecs, registry)
	require.NoError(t, err)
	assert.True(t, resolved.IsBinary())

	// Test custom media URN resolution
	customSpecs := []media.MediaSpecDef{
		{Urn: "media:custom;textable", MediaType: "text/html", ProfileURI: "https://example.com/schema/html"},
	}

	resolved, err = media.ResolveMediaUrn("media:custom;textable", customSpecs, registry)
	require.NoError(t, err)
	assert.Equal(t, "text/html", resolved.MediaType)
	assert.Equal(t, "https://example.com/schema/html", resolved.ProfileURI)

	// Test unknown media URN fails
	_, err = media.ResolveMediaUrn("media:unknown", nil, registry)
	assert.Error(t, err)
}

// TestIntegrationMediaSpecDefConstruction verifies media.MediaSpecDef construction
func TestIntegrationMediaSpecDefConstruction(t *testing.T) {
	// Test basic construction
	def := media.NewMediaSpecDef("media:test;textable", "text/plain", "https://capdag.com/schema/str")
	assert.Equal(t, "media:test;textable", def.Urn)
	assert.Equal(t, "text/plain", def.MediaType)
	assert.Equal(t, "https://capdag.com/schema/str", def.ProfileURI)

	// Test with title
	defWithTitle := media.NewMediaSpecDefWithTitle("media:test;textable", "text/plain", "https://example.com/schema", "Test Title")
	assert.Equal(t, "Test Title", defWithTitle.Title)

	// Test object form with schema
	schema := map[string]interface{}{"type": "object"}
	schemaDef := media.NewMediaSpecDefWithSchema("media:test;json", "application/json", "https://example.com/schema", schema)
	assert.NotNil(t, schemaDef.Schema)
}

// CBOR Integration Tests (TEST284-303)
// These tests verify the CBOR cartridge communication protocol between host and cartridge

const testCBORManifest = `{"name":"TestCartridge","version":"1.0.0","description":"Test cartridge","caps":[{"urn":"cap:in=\"media:void\";op=test;out=\"media:void\"","title":"Test","command":"test"}]}`

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
		assert.Equal(t, "cap:in=media:;out=media:", *frame.Cap)
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
	request := NewReq(requestID, "cap:in=media:;out=media:", []byte("hello"), "application/json")
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
	request := NewReq(requestID, "cap:op=stream", []byte("go"), "application/json")
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
func TestCartridgeErrorResponse(t *testing.T) {
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
		errFrame := NewErr(frame.Id, "NOT_FOUND", "cap.Cap not found: cap:op=missing")
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
	request := NewReq(requestID, "cap:op=missing", []byte(""), "application/json")
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
func TestLogFramesDuringRequest(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=test", []byte(""), "application/json")
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
	request := NewReq(requestID, "cap:op=binary", binaryData, "application/octet-stream")
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
		request := NewReq(requestID, "cap:op=test", []byte(""), "application/json")
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

	runtime.Register(`cap:in="media:void";op=transform;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			return emitter.EmitCbor("transformed")
		})

	// Exact match
	assert.NotNil(t, runtime.FindHandler(standard.CapIdentity))
	assert.NotNil(t, runtime.FindHandler(`cap:in="media:void";op=transform;out="media:void"`))

	// Non-existent
	assert.Nil(t, runtime.FindHandler(`cap:in="media:void";op=unknown;out="media:void"`))
}

// Mirror-specific coverage: Test cartridge-initiated heartbeat mid-stream is handled transparently by host
func TestHeartbeatDuringStreaming(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=stream", []byte(""), "application/json")
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
func TestHostInitiatedHeartbeatNoPingPong(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=test", []byte(""), "application/json")
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
func TestArgumentsRoundtrip(t *testing.T) {
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
		cap.NewCapArgumentValueFromStr("media:model-spec;textable", "gpt-4"),
	}

	// Encode arguments to CBOR
	argsData, err := EncodeCapArgumentValues(args)
	require.NoError(t, err)

	// Send request with CBOR arguments
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:op=test", argsData, "application/cbor")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Read response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Equal(t, []byte("gpt-4"), response.Payload)

	wg.Wait()
}

// Mirror-specific coverage: Test host receives error when cartridge closes connection unexpectedly
func TestCartridgeSuddenDisconnect(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=test", []byte(""), "application/json")
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
	request := NewReq(requestID, "cap:op=empty", []byte{}, "application/json")
	err = writer.WriteFrame(request)
	require.NoError(t, err)

	// Read response
	response, err := reader.ReadFrame()
	require.NoError(t, err)
	assert.Empty(t, response.Payload)

	wg.Wait()
}

// Mirror-specific coverage: Test END frame without payload is handled as complete response with empty data
func TestEndFrameNoPayload(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=test", []byte(""), "application/json")
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
func TestStreamingSequenceNumbers(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=test", []byte(""), "text/plain")
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
func TestRequestAfterShutdown(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=test", []byte(""), "application/json")
	err = writer.WriteFrame(request)
	assert.Error(t, err, "must fail on closed connection")
}

// Mirror-specific coverage: Test multiple arguments are correctly serialized in CBOR payload
func TestArgumentsMultiple(t *testing.T) {
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
		cap.NewCapArgumentValueFromStr("media:model-spec;textable", "gpt-4"),
		cap.NewCapArgumentValue("media:pdf", []byte{0x89, 0x50, 0x4E, 0x47}),
	}

	// Encode arguments to CBOR
	argsData, err := EncodeCapArgumentValues(args)
	require.NoError(t, err)

	// Send request
	requestID := NewMessageIdRandom()
	request := NewReq(requestID, "cap:op=test", argsData, "application/cbor")
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
func TestAutoChunkingReassembly(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=test", nil, "text/plain")
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
func TestExactMaxChunkSingleEnd(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=test", nil, "text/plain")
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
func TestMaxChunkPlusOneSplitsIntoTwo(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=test", nil, "text/plain")
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
func TestConcatenatedVsFinalPayloadDivergence(t *testing.T) {
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
func TestChunkingDataIntegrity3x(t *testing.T) {
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
	request := NewReq(requestID, "cap:op=test", nil, "text/plain")
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
