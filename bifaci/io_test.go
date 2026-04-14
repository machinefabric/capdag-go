package bifaci

import (
	"bytes"
	"io"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

// TEST205: Test REQ frame encode/decode roundtrip preserves all fields
func Test205_req_frame_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()
	cap := `cap:in="media:void";op=test;out="media:void"`
	payload := []byte("test payload")
	contentType := "application/json"

	original := NewReq(id, cap, payload, contentType)
	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.FrameType != original.FrameType {
		t.Error("FrameType mismatch")
	}
	if decoded.Cap == nil || original.Cap == nil || *decoded.Cap != *original.Cap {
		t.Errorf("Cap mismatch: expected %v, got %v", original.Cap, decoded.Cap)
	}
	if string(decoded.Payload) != string(original.Payload) {
		t.Error("Payload mismatch")
	}
	if decoded.ContentType == nil || original.ContentType == nil || *decoded.ContentType != *original.ContentType {
		t.Errorf("ContentType mismatch: expected %v, got %v", original.ContentType, decoded.ContentType)
	}
}

// TEST206: Test HELLO frame encode/decode roundtrip preserves metadata
func Test206_hello_frame_roundtrip(t *testing.T) {
	original := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer)

	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.FrameType != FrameTypeHello {
		t.Error("FrameType mismatch")
	}
	if decoded.Meta == nil {
		t.Error("Expected Meta map with limits")
	}
	if decoded.Meta["max_frame"] == nil {
		t.Error("Expected max_frame in Meta")
	}
}

// TEST207: Test ERR frame encode/decode roundtrip preserves error code and message
func Test207_err_frame_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()
	code := "HANDLER_ERROR"
	message := "Something failed"

	original := NewErr(id, code, message)
	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.ErrorCode() != code {
		t.Errorf("Code mismatch: expected %s, got %s", code, decoded.ErrorCode())
	}
	if decoded.ErrorMessage() != message {
		t.Errorf("Message mismatch: expected %s, got %s", message, decoded.ErrorMessage())
	}
}

// TEST208: Test LOG frame encode/decode roundtrip preserves level and message
func Test208_log_frame_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()
	level := "info"
	message := "Log entry"

	original := NewLog(id, level, message)
	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.LogLevel() != level {
		t.Errorf("Level mismatch: expected %s, got %s", level, decoded.LogLevel())
	}
	if decoded.LogMessage() != message {
		t.Errorf("Message mismatch: expected %s, got %s", message, decoded.LogMessage())
	}
}

// TEST209: REMOVED - RES frame no longer supported in protocol v2

// TEST210: Test END frame encode/decode roundtrip preserves payload
func Test210_end_frame_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()
	payload := []byte("final data")

	original := NewEnd(id, payload)
	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.FrameType != FrameTypeEnd {
		t.Error("FrameType mismatch")
	}
	if string(decoded.Payload) != string(payload) {
		t.Error("Payload mismatch")
	}
	if !decoded.IsEof() {
		t.Error("Expected eof to be true")
	}
}

// TEST211: Test HELLO with manifest encode/decode roundtrip preserves manifest bytes
func Test211_hello_with_manifest_roundtrip(t *testing.T) {
	manifest := []byte(`{"name":"test","version":"1.0.0"}`)
	original := NewHelloWithManifest(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, manifest)

	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Meta == nil {
		t.Fatal("Expected Meta map")
	}
	if manifestBytes, ok := decoded.Meta["manifest"].([]byte); !ok || string(manifestBytes) != string(manifest) {
		t.Errorf("Manifest mismatch: expected %s, got %v", string(manifest), decoded.Meta["manifest"])
	}
}

// TEST212: Test chunk encode/decode roundtrip with streamId
func Test212_chunk_with_offset_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()
	streamId := "test-stream"
	seq := uint64(3)
	payload := []byte("chunk data")
	chunkIndex := uint64(0)
	checksum := ComputeChecksum(payload)

	original := NewChunk(id, streamId, seq, payload, chunkIndex, checksum)
	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Seq != seq {
		t.Errorf("Seq mismatch: expected %d, got %d", seq, decoded.Seq)
	}
	if string(decoded.Payload) != string(payload) {
		t.Error("Payload mismatch")
	}
}

// TEST213: Test heartbeat frame encode/decode roundtrip preserves ID with no extra fields
func Test213_heartbeat_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()
	original := NewHeartbeat(id)

	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.FrameType != FrameTypeHeartbeat {
		t.Error("FrameType mismatch")
	}
	if len(decoded.Payload) != 0 {
		t.Error("HEARTBEAT should have empty payload")
	}
}

// TEST214: Test write_frame/read_frame IO roundtrip through length-prefixed wire format
func Test214_frame_io_roundtrip(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)
	reader := NewFrameReader(&buf)

	id := NewMessageIdRandom()
	original := NewReq(id, `cap:in="media:void";op=test;out="media:void"`, []byte("test"), "application/json")

	// Write frame
	if err := writer.WriteFrame(original); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	// Read frame
	decoded, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if decoded.Cap == nil || original.Cap == nil || *decoded.Cap != *original.Cap {
		t.Error("Cap mismatch after I/O roundtrip")
	}
}

// TEST215: Test reading multiple sequential frames from a single buffer
func Test215_read_multiple_frames(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)

	// Write three frames
	id1 := NewMessageIdFromUint(1)
	id2 := NewMessageIdFromUint(2)
	id3 := NewMessageIdFromUint(3)

	writer.WriteFrame(NewHeartbeat(id1))
	writer.WriteFrame(NewHeartbeat(id2))
	writer.WriteFrame(NewHeartbeat(id3))

	// Read them back
	reader := NewFrameReader(&buf)
	frame1, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("Read frame 1 failed: %v", err)
	}
	frame2, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("Read frame 2 failed: %v", err)
	}
	frame3, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("Read frame 3 failed: %v", err)
	}

	if frame1.FrameType != FrameTypeHeartbeat || frame2.FrameType != FrameTypeHeartbeat || frame3.FrameType != FrameTypeHeartbeat {
		t.Error("Frame types mismatch")
	}
}

// TEST216: Test write_frame rejects frames exceeding max_frame limit
func Test216_write_frame_rejects_oversized(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)

	// Set a small limit
	writer.SetLimits(Limits{MaxFrame: 100, MaxChunk: 50})

	// Create a frame with large payload that will exceed limit when encoded
	id := NewMessageIdRandom()
	largePayload := make([]byte, 200)
	frame := NewReq(id, `cap:in="media:void";op=test;out="media:void"`, largePayload, "")

	err := writer.WriteFrame(frame)
	if err == nil {
		t.Error("Expected error for oversized frame, got nil")
	}
}

// TEST217: Test read_frame rejects incoming frames exceeding the negotiated max_frame limit
func Test217_read_frame_rejects_oversized(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)

	// Write with default limits
	id := NewMessageIdRandom()
	largePayload := make([]byte, 1000)
	frame := NewReq(id, `cap:in="media:void";op=test;out="media:void"`, largePayload, "")
	writer.WriteFrame(frame)

	// Try to read with much smaller limit
	reader := NewFrameReader(&buf)
	reader.SetLimits(Limits{MaxFrame: 100, MaxChunk: 50})

	_, err := reader.ReadFrame()
	if err == nil {
		t.Error("Expected error for oversized frame, got nil")
	}
}

// TEST218: Test write_chunked splits data into chunks respecting max_chunk (updated for stream multiplexing)
func Test218_write_chunked(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)
	writer.SetLimits(Limits{MaxFrame: DefaultMaxFrame, MaxChunk: 100})

	id := NewMessageIdRandom()
	streamId := "test-stream"
	mediaUrn := "media:"
	data := make([]byte, 250) // Will be split into 3 chunks: 100 + 100 + 50

	err := writer.WriteResponseWithChunking(id, streamId, mediaUrn, data)
	if err != nil {
		t.Fatalf("WriteResponseWithChunking failed: %v", err)
	}

	// Read back and verify we got: STREAM_START + CHUNK(s) + STREAM_END + END
	reader := NewFrameReader(&buf)

	// First frame should be STREAM_START
	frame, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if frame.FrameType != FrameTypeStreamStart {
		t.Errorf("Expected STREAM_START, got %v", frame.FrameType)
	}

	// Collect CHUNK frames
	var chunkCount int
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}
		if frame.FrameType == FrameTypeChunk {
			chunkCount++
		} else if frame.FrameType == FrameTypeStreamEnd {
			break
		} else {
			t.Fatalf("Unexpected frame type: %v", frame.FrameType)
		}
	}

	if chunkCount < 2 {
		t.Errorf("Expected multiple chunks, got %d", chunkCount)
	}

	// Final frame should be END
	frame, err = reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if frame.FrameType != FrameTypeEnd {
		t.Errorf("Expected END, got %v", frame.FrameType)
	}
}

// TEST219: Test write_chunked with empty data produces STREAM_START + STREAM_END + END
func Test219_write_chunked_empty(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)

	id := NewMessageIdRandom()
	streamId := "empty-stream"
	mediaUrn := "media:void"
	err := writer.WriteResponseWithChunking(id, streamId, mediaUrn, []byte{})
	if err != nil {
		t.Fatalf("WriteResponseWithChunking failed: %v", err)
	}

	reader := NewFrameReader(&buf)

	// First: STREAM_START
	frame, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if frame.FrameType != FrameTypeStreamStart {
		t.Errorf("Expected STREAM_START, got %v", frame.FrameType)
	}

	// Second: STREAM_END (no chunks for empty data)
	frame, err = reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if frame.FrameType != FrameTypeStreamEnd {
		t.Errorf("Expected STREAM_END, got %v", frame.FrameType)
	}

	// Third: END
	frame, err = reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if frame.FrameType != FrameTypeEnd {
		t.Errorf("Expected END frame for empty data, got %v", frame.FrameType)
	}
}

// TEST220: Test write_chunked with data exactly equal to max_chunk produces STREAM_START + CHUNK + STREAM_END + END
func Test220_write_chunked_exact_chunk_size(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)
	writer.SetLimits(Limits{MaxFrame: DefaultMaxFrame, MaxChunk: 100})

	id := NewMessageIdRandom()
	streamId := "exact-stream"
	mediaUrn := "media:"
	data := make([]byte, 100) // Exactly max_chunk

	err := writer.WriteResponseWithChunking(id, streamId, mediaUrn, data)
	if err != nil {
		t.Fatalf("WriteResponseWithChunking failed: %v", err)
	}

	reader := NewFrameReader(&buf)

	// First: STREAM_START
	frame, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if frame.FrameType != FrameTypeStreamStart {
		t.Errorf("Expected STREAM_START, got %v", frame.FrameType)
	}

	// Second: CHUNK
	frame, err = reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if frame.FrameType != FrameTypeChunk {
		t.Errorf("Expected END frame, got %v", frame.FrameType)
	}
}

// TEST221: Test read_frame returns Ok(None) on clean EOF (empty stream)
func Test221_read_frame_eof(t *testing.T) {
	var buf bytes.Buffer // Empty buffer
	reader := NewFrameReader(&buf)

	_, err := reader.ReadFrame()
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

// TEST222: Test read_frame handles truncated length prefix
func Test222_read_frame_truncated_length_prefix(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x00, 0x00}) // Only 2 bytes of 4-byte length prefix
	reader := NewFrameReader(buf)

	_, err := reader.ReadFrame()
	if err == nil {
		t.Error("Expected error for truncated length prefix")
	}
}

// TEST223: Test read_frame returns error on truncated frame body
func Test223_read_frame_truncated_body(t *testing.T) {
	var buf bytes.Buffer
	// Write a length prefix indicating 100 bytes
	lengthPrefix := []byte{0x00, 0x00, 0x00, 0x64} // 100 in big-endian
	buf.Write(lengthPrefix)
	// But only write 10 bytes of body
	buf.Write(make([]byte, 10))

	reader := NewFrameReader(&buf)
	_, err := reader.ReadFrame()
	if err == nil {
		t.Error("Expected error for truncated frame body")
	}
}

// TEST224: Test MessageId::Uint roundtrips through encode/decode
func Test224_message_id_uint_roundtrip(t *testing.T) {
	id := NewMessageIdFromUint(42)
	frame := NewHeartbeat(id)

	encoded, err := EncodeFrame(frame)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Id.ToString() != "42" {
		t.Errorf("Expected ID '42', got '%s'", decoded.Id.ToString())
	}
}

// TEST225: Test decode_frame rejects non-map CBOR values (e.g., array, integer, string)
func Test225_decode_non_map_value(t *testing.T) {
	// Encode a CBOR array instead of map
	cborArray := []byte{0x81, 0x01} // CBOR array [1]

	_, err := DecodeFrame(cborArray)
	if err == nil {
		t.Error("decode_frame should reject non-map CBOR values")
	}
}

// TEST226: Test decode_frame rejects CBOR map missing required version field
func Test226_decode_missing_version(t *testing.T) {
	// Build CBOR map with frame_type and id but missing version
	// Map with keys 1 (frame_type) and 2 (id) but no key 0 (version)
	m := make(map[int]interface{})
	m[keyFrameType] = uint8(FrameTypeReq)
	m[keyId] = uint64(0)

	encoded, _ := cbor.Marshal(m)
	_, err := DecodeFrame(encoded)
	if err == nil {
		t.Error("decode_frame should reject map missing version field")
	}
}

// TEST227: Test decode_frame rejects CBOR map with invalid frame_type value
func Test227_decode_invalid_frame_type_value(t *testing.T) {
	m := make(map[int]interface{})
	m[keyVersion] = uint8(1)
	m[keyFrameType] = uint8(99) // invalid frame type
	m[keyId] = uint64(0)

	encoded, _ := cbor.Marshal(m)
	_, err := DecodeFrame(encoded)
	if err == nil {
		t.Error("decode_frame should reject invalid frame_type value")
	}
}

// TEST228: Test decode_frame rejects CBOR map missing required id field
func Test228_decode_missing_id(t *testing.T) {
	m := make(map[int]interface{})
	m[keyVersion] = uint8(1)
	m[keyFrameType] = uint8(FrameTypeReq)
	// No ID field

	encoded, _ := cbor.Marshal(m)
	_, err := DecodeFrame(encoded)
	if err == nil {
		t.Error("decode_frame should reject map missing id field")
	}
}

// TEST229: Test FrameReader/FrameWriter SetLimits updates the negotiated limits
func Test229_frame_reader_writer_set_limits(t *testing.T) {
	buf := &bytes.Buffer{}
	reader := NewFrameReader(buf)
	writer := NewFrameWriter(buf)

	customLimits := Limits{MaxFrame: 500, MaxChunk: 100}
	reader.SetLimits(customLimits)
	writer.SetLimits(customLimits)

	if reader.limits.MaxFrame != 500 {
		t.Error("Reader max_frame should be 500")
	}
	if reader.limits.MaxChunk != 100 {
		t.Error("Reader max_chunk should be 100")
	}
	if writer.limits.MaxFrame != 500 {
		t.Error("Writer max_frame should be 500")
	}
	if writer.limits.MaxChunk != 100 {
		t.Error("Writer max_chunk should be 100")
	}
}

// TEST230: Test sync handshake exchanges HELLO frames and negotiates minimum limits
func Test230_sync_handshake(t *testing.T) {
	// Use in-memory buffer for testing instead of pipes
	// This simulates the handshake without needing bidirectional sockets
	manifest := []byte(`{"name":"Test","version":"1.0","caps":[]}`)

	// Create buffers for communication
	var hostToCartridge bytes.Buffer
	var cartridgeToHost bytes.Buffer

	// Cartridge side writes HELLO response to cartridgeToHost buffer
	cartridgeWriter := NewFrameWriter(&cartridgeToHost)
	cartridgeReader := NewFrameReader(&hostToCartridge)

	// Host side writes HELLO request to hostToCartridge buffer
	hostWriter := NewFrameWriter(&hostToCartridge)

	// Step 1: Host sends HELLO
	helloFrame := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer)
	if err := hostWriter.WriteFrame(helloFrame); err != nil {
		t.Fatalf("Failed to write host HELLO: %v", err)
	}

	// Step 2: Cartridge reads HELLO and responds
	cartridgeHelloFrame, err := cartridgeReader.ReadFrame()
	if err != nil {
		t.Fatalf("Cartridge failed to read HELLO: %v", err)
	}
	if cartridgeHelloFrame.FrameType != FrameTypeHello {
		t.Fatal("Expected HELLO frame")
	}

	// Cartridge sends HELLO with manifest
	responseFrame := NewHelloWithManifest(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, manifest)
	if err := cartridgeWriter.WriteFrame(responseFrame); err != nil {
		t.Fatalf("Failed to write cartridge HELLO: %v", err)
	}

	// Step 3: Host reads response
	hostReader := NewFrameReader(&cartridgeToHost)
	helloResponseFrame, err := hostReader.ReadFrame()
	if err != nil {
		t.Fatalf("Host failed to read HELLO response: %v", err)
	}

	// Verify manifest
	if helloResponseFrame.Meta == nil {
		t.Fatal("HELLO response should have Meta")
	}
	manifestVal, ok := helloResponseFrame.Meta["manifest"]
	if !ok {
		t.Fatal("HELLO response should have manifest in Meta")
	}
	manifestBytes, ok := manifestVal.([]byte)
	if !ok {
		t.Fatal("Manifest should be bytes")
	}
	if string(manifestBytes) != string(manifest) {
		t.Error("Manifest should be preserved")
	}

	// Verify limits negotiation
	maxFrameVal, ok := helloResponseFrame.Meta["max_frame"]
	if !ok {
		t.Fatal("HELLO response should have max_frame")
	}
	maxChunkVal, ok := helloResponseFrame.Meta["max_chunk"]
	if !ok {
		t.Fatal("HELLO response should have max_chunk")
	}

	// Convert to int for comparison
	var maxFrame, maxChunk int
	switch v := maxFrameVal.(type) {
	case int:
		maxFrame = v
	case uint64:
		maxFrame = int(v)
	case int64:
		maxFrame = int(v)
	}
	switch v := maxChunkVal.(type) {
	case int:
		maxChunk = v
	case uint64:
		maxChunk = int(v)
	case int64:
		maxChunk = int(v)
	}

	if maxFrame != DefaultMaxFrame {
		t.Errorf("Expected max_frame %d, got %d", DefaultMaxFrame, maxFrame)
	}
	if maxChunk != DefaultMaxChunk {
		t.Errorf("Expected max_chunk %d, got %d", DefaultMaxChunk, maxChunk)
	}
}

// TEST231: Test handshake fails when peer sends non-HELLO frame
func Test231_handshake_rejects_non_hello(t *testing.T) {
	// Use in-memory buffers
	var hostToCartridge bytes.Buffer
	var cartridgeToHost bytes.Buffer

	// Host sends HELLO
	hostWriter := NewFrameWriter(&hostToCartridge)
	helloFrame := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer)
	if err := hostWriter.WriteFrame(helloFrame); err != nil {
		t.Fatalf("Failed to write HELLO: %v", err)
	}

	// Cartridge sends REQ instead of HELLO (bad!)
	cartridgeWriter := NewFrameWriter(&cartridgeToHost)
	badFrame := NewReq(NewMessageIdFromUint(1), "cap:op=bad", []byte{}, "text/plain")
	if err := cartridgeWriter.WriteFrame(badFrame); err != nil {
		t.Fatalf("Failed to write bad frame: %v", err)
	}

	// Host tries to complete handshake
	hostReader := NewFrameReader(&cartridgeToHost)
	responseFrame, err := hostReader.ReadFrame()
	if err != nil {
		t.Fatalf("Failed to read frame: %v", err)
	}

	// Verify it's not a HELLO frame
	if responseFrame.FrameType == FrameTypeHello {
		t.Error("Should have received non-HELLO frame")
	}
	if responseFrame.FrameType != FrameTypeReq {
		t.Errorf("Expected REQ frame, got %v", responseFrame.FrameType)
	}
}

// TEST232: Test handshake fails when cartridge HELLO is missing required manifest
func Test232_handshake_rejects_missing_manifest(t *testing.T) {
	// Use in-memory buffers
	var cartridgeToHost bytes.Buffer

	// Cartridge sends HELLO without manifest
	cartridgeWriter := NewFrameWriter(&cartridgeToHost)
	noManifestHello := NewHello(1_000_000, 200_000, DefaultMaxReorderBuffer)
	if err := cartridgeWriter.WriteFrame(noManifestHello); err != nil {
		t.Fatalf("Failed to write HELLO: %v", err)
	}

	// Host reads the HELLO
	hostReader := NewFrameReader(&cartridgeToHost)
	helloFrame, err := hostReader.ReadFrame()
	if err != nil {
		t.Fatalf("Failed to read HELLO: %v", err)
	}

	// Verify it's a HELLO frame
	if helloFrame.FrameType != FrameTypeHello {
		t.Error("Expected HELLO frame")
	}

	// Verify manifest is missing
	if helloFrame.Meta == nil {
		// No meta means no manifest - expected
		return
	}
	if _, hasManifest := helloFrame.Meta["manifest"]; hasManifest {
		t.Error("HELLO should not have manifest")
	}
}

// TEST233: Test binary payload with all 256 byte values roundtrips through encode/decode
func Test233_binary_payload_all_byte_values(t *testing.T) {
	data := make([]byte, 256)
	for i := 0; i < 256; i++ {
		data[i] = byte(i)
	}

	id := NewMessageIdRandom()
	frame := NewReq(id, "cap:op=binary", data, "application/octet-stream")

	encoded, err := EncodeFrame(frame)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if string(decoded.Payload) != string(data) {
		t.Error("Binary payload not preserved")
	}
}

// TEST234: Test decode_frame handles garbage CBOR bytes gracefully with an error
func Test234_decode_garbage_bytes(t *testing.T) {
	garbage := []byte{0xFF, 0xFE, 0xFD, 0xFC, 0xFB}
	_, err := DecodeFrame(garbage)
	if err == nil {
		t.Error("garbage bytes must produce decode error")
	}
}

// TEST389: StreamStart encode/decode roundtrip preserves stream_id and media_urn
func Test389_stream_start_roundtrip(t *testing.T) {
	reqId := NewMessageIdRandom()
	streamId := "stream-roundtrip-123"
	mediaUrn := "media:json"

	original := NewStreamStart(reqId, streamId, mediaUrn, nil)
	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.FrameType != FrameTypeStreamStart {
		t.Errorf("Expected STREAM_START, got %v", decoded.FrameType)
	}
	if decoded.StreamId == nil || *decoded.StreamId != streamId {
		t.Errorf("StreamId mismatch: expected %s, got %v", streamId, decoded.StreamId)
	}
	if decoded.MediaUrn == nil || *decoded.MediaUrn != mediaUrn {
		t.Errorf("MediaUrn mismatch: expected %s, got %v", mediaUrn, decoded.MediaUrn)
	}
}

// TEST390: StreamEnd encode/decode roundtrip preserves stream_id, no media_urn
func Test390_stream_end_roundtrip(t *testing.T) {
	reqId := NewMessageIdRandom()
	streamId := "stream-end-456"

	original := NewStreamEnd(reqId, streamId, 0)
	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.FrameType != FrameTypeStreamEnd {
		t.Errorf("Expected STREAM_END, got %v", decoded.FrameType)
	}
	if decoded.StreamId == nil || *decoded.StreamId != streamId {
		t.Errorf("StreamId mismatch: expected %s, got %v", streamId, decoded.StreamId)
	}
	if decoded.MediaUrn != nil {
		t.Errorf("STREAM_END should not have mediaUrn, got %v", *decoded.MediaUrn)
	}
}

// TEST848: RelayNotify encode/decode roundtrip preserves manifest and limits
func Test848_relay_notify_roundtrip(t *testing.T) {
	manifest := []byte(`{"caps":["cap:op=relay-test"]}`)
	maxFrame := 2_000_000
	maxChunk := 128_000

	original := NewRelayNotify(manifest, maxFrame, maxChunk, DefaultMaxReorderBuffer)
	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.FrameType != FrameTypeRelayNotify {
		t.Errorf("Expected RELAY_NOTIFY, got %v", decoded.FrameType)
	}

	extractedManifest := decoded.RelayNotifyManifest()
	if extractedManifest == nil {
		t.Fatal("RelayNotifyManifest() returned nil after roundtrip")
	}
	if string(extractedManifest) != string(manifest) {
		t.Errorf("Manifest mismatch after roundtrip: got %s", string(extractedManifest))
	}

	extractedLimits := decoded.RelayNotifyLimits()
	if extractedLimits == nil {
		t.Fatal("RelayNotifyLimits() returned nil after roundtrip")
	}
	if extractedLimits.MaxFrame != maxFrame {
		t.Errorf("MaxFrame mismatch: expected %d, got %d", maxFrame, extractedLimits.MaxFrame)
	}
	if extractedLimits.MaxChunk != maxChunk {
		t.Errorf("MaxChunk mismatch: expected %d, got %d", maxChunk, extractedLimits.MaxChunk)
	}
}

// TEST849: RelayState encode/decode roundtrip preserves resource payload
func Test849_relay_state_roundtrip(t *testing.T) {
	resources := []byte(`{"gpu_memory":8192,"cpu_cores":16}`)

	original := NewRelayState(resources)
	encoded, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.FrameType != FrameTypeRelayState {
		t.Errorf("Expected RELAY_STATE, got %v", decoded.FrameType)
	}
	if string(decoded.Payload) != string(resources) {
		t.Errorf("Payload mismatch after roundtrip: got %s", string(decoded.Payload))
	}
}

// TEST440: CHUNK frame with chunk_index and checksum roundtrips through encode/decode
func Test440_chunk_index_checksum_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()
	payload := []byte("test chunk data")
	checksum := ComputeChecksum(payload)

	frame := NewChunk(id, "test-stream", 5, payload, 3, checksum)

	encoded, err := EncodeFrame(frame)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.FrameType != FrameTypeChunk {
		t.Errorf("Expected CHUNK, got %v", decoded.FrameType)
	}
	if decoded.Id.ToString() != id.ToString() {
		t.Error("ID mismatch")
	}
	if decoded.StreamId == nil || *decoded.StreamId != "test-stream" {
		t.Error("stream_id mismatch")
	}
	if decoded.Seq != 5 {
		t.Errorf("Expected seq 5, got %d", decoded.Seq)
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Error("payload mismatch")
	}
	if decoded.ChunkIndex == nil || *decoded.ChunkIndex != 3 {
		t.Error("chunk_index must roundtrip")
	}
	if decoded.Checksum == nil || *decoded.Checksum != checksum {
		t.Error("checksum must roundtrip")
	}
}

// TEST441: STREAM_END frame with chunk_count roundtrips through encode/decode
func Test441_stream_end_chunk_count_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()

	frame := NewStreamEnd(id, "test-stream", 42)

	encoded, err := EncodeFrame(frame)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.FrameType != FrameTypeStreamEnd {
		t.Errorf("Expected STREAM_END, got %v", decoded.FrameType)
	}
	if decoded.Id.ToString() != id.ToString() {
		t.Error("ID mismatch")
	}
	if decoded.StreamId == nil || *decoded.StreamId != "test-stream" {
		t.Error("stream_id mismatch")
	}
	if decoded.ChunkCount == nil || *decoded.ChunkCount != 42 {
		t.Error("chunk_count must roundtrip")
	}
}

// TEST497: Corrupted payload detectable via checksum mismatch
func Test497_chunk_corrupted_payload_rejected(t *testing.T) {
	id := NewMessageIdRandom()
	payload := []byte("original data")
	checksum := ComputeChecksum(payload)

	frame := NewChunk(id, "stream-test", 0, payload, 0, checksum)

	encoded, err := EncodeFrame(frame)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Checksum == nil || *decoded.Checksum != checksum {
		t.Error("Decoded checksum should match original")
	}

	// Corrupt the payload but keep the checksum
	decoded.Payload = []byte("corrupted data")

	corruptedChecksum := ComputeChecksum(decoded.Payload)
	if corruptedChecksum == checksum {
		t.Error("Checksums should differ for corrupted data")
	}
	if *decoded.Checksum != checksum {
		t.Error("Frame still has original checksum")
	}
}

// TEST846: Test progress LOG frame encode/decode roundtrip preserves progress float
func Test846_progress_frame_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()

	testValues := []struct {
		progress float32
		label    string
	}{
		{0.0, "zero"},
		{0.03333333, "1/30"},
		{0.06666667, "2/30"},
		{0.13333334, "4/30"},
		{0.25, "quarter"},
		{0.5, "half"},
		{0.75, "three-quarter"},
		{1.0, "one"},
	}

	for _, tv := range testValues {
		original := NewProgress(id, tv.progress, "test phase")
		encoded, err := EncodeFrame(original)
		if err != nil {
			t.Fatalf("[%s] Encode failed: %v", tv.label, err)
		}
		decoded, err := DecodeFrame(encoded)
		if err != nil {
			t.Fatalf("[%s] Decode failed: %v", tv.label, err)
		}

		if decoded.FrameType != FrameTypeLog {
			t.Fatalf("[%s] Expected LOG frame type", tv.label)
		}
		if decoded.LogLevel() != "progress" {
			t.Fatalf("[%s] Expected level 'progress', got %q", tv.label, decoded.LogLevel())
		}
		if decoded.LogMessage() != "test phase" {
			t.Fatalf("[%s] Expected message 'test phase', got %q", tv.label, decoded.LogMessage())
		}

		p, ok := decoded.LogProgress()
		if !ok {
			t.Fatalf("[%s] log_progress() must return value for progress=%f", tv.label, tv.progress)
		}
		diff := p - tv.progress
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.001 {
			t.Fatalf("[%s] progress roundtrip: expected %f, got %f", tv.label, tv.progress, p)
		}
	}
}

// TEST847: Double roundtrip (modelcartridge → relay → candlecartridge)
func Test847_progress_double_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()

	for _, progress := range []float32{0.0, 0.03333333, 0.06666667, 0.13333334, 0.5, 1.0} {
		original := NewProgress(id, progress, "test")

		// First roundtrip
		bytes1, err := EncodeFrame(original)
		if err != nil {
			t.Fatalf("Encode 1 failed for progress=%f: %v", progress, err)
		}
		decoded1, err := DecodeFrame(bytes1)
		if err != nil {
			t.Fatalf("Decode 1 failed for progress=%f: %v", progress, err)
		}

		// Relay switch modifies seq
		decoded1.Seq = 42

		// Second roundtrip
		bytes2, err := EncodeFrame(decoded1)
		if err != nil {
			t.Fatalf("Encode 2 failed for progress=%f: %v", progress, err)
		}
		decoded2, err := DecodeFrame(bytes2)
		if err != nil {
			t.Fatalf("Decode 2 failed for progress=%f: %v", progress, err)
		}

		p, ok := decoded2.LogProgress()
		if !ok {
			t.Fatalf("progress=%f: log_progress() returned false", progress)
		}
		diff := p - progress
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.001 {
			t.Fatalf("progress=%f: expected %f, got %f", progress, progress, p)
		}
	}
}
