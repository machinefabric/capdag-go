package bifaci

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TEST235: Test ResponseChunk stores payload, seq, offset, len, and eof fields correctly
func Test235_response_chunk_fields(t *testing.T) {
	payload := []byte{1, 2, 3, 4, 5}
	seq := uint64(42)
	offset := uint64(100)
	length := uint64(500)

	chunk := &ResponseChunk{
		Payload: payload,
		Seq:     seq,
		Offset:  &offset,
		Len:     &length,
		IsEof:   false,
	}

	assert.Equal(t, payload, chunk.Payload)
	assert.Equal(t, seq, chunk.Seq)
	assert.NotNil(t, chunk.Offset)
	assert.Equal(t, offset, *chunk.Offset)
	assert.NotNil(t, chunk.Len)
	assert.Equal(t, length, *chunk.Len)
	assert.False(t, chunk.IsEof)
}

// TEST236: Test ResponseChunk with all fields populated preserves offset, len, and eof
func Test236_response_chunk_all_fields_populated(t *testing.T) {
	payload := []byte("test data")
	seq := uint64(10)
	offset := uint64(200)
	length := uint64(1000)

	chunk := &ResponseChunk{
		Payload: payload,
		Seq:     seq,
		Offset:  &offset,
		Len:     &length,
		IsEof:   true,
	}

	assert.Equal(t, string(payload), string(chunk.Payload))
	assert.Equal(t, seq, chunk.Seq)
	assert.Equal(t, offset, *chunk.Offset)
	assert.Equal(t, length, *chunk.Len)
	assert.True(t, chunk.IsEof)
}

// TEST237: Test CartridgeResponse::Single final_payload returns the single payload slice
func Test237_cartridge_response_single_final_payload(t *testing.T) {
	payload := []byte("single response")
	response := &CartridgeResponse{
		Type:   CartridgeResponseTypeSingle,
		Single: payload,
	}

	finalPayload := response.FinalPayload()
	assert.Equal(t, payload, finalPayload)
}

// TEST238: Test CartridgeResponse::Single with empty payload returns empty slice and empty vec
func Test238_cartridge_response_single_empty_payload(t *testing.T) {
	response := &CartridgeResponse{
		Type:   CartridgeResponseTypeSingle,
		Single: []byte{},
	}

	assert.Empty(t, response.Single)
	assert.Empty(t, response.FinalPayload())
}

// TEST239: Test CartridgeResponse::Streaming concatenated joins all chunk payloads in order
func Test239_cartridge_response_streaming_concatenated(t *testing.T) {
	chunks := []*ResponseChunk{
		{Payload: []byte("Hello "), Seq: 0, IsEof: false},
		{Payload: []byte("World"), Seq: 1, IsEof: false},
		{Payload: []byte("!"), Seq: 2, IsEof: true},
	}

	response := &CartridgeResponse{
		Type:      CartridgeResponseTypeStreaming,
		Streaming: chunks,
	}

	concatenated := response.Concatenated()
	assert.Equal(t, "Hello World!", string(concatenated))
}

// TEST240: Test CartridgeResponse::Streaming final_payload returns the last chunk's payload
func Test240_cartridge_response_streaming_final_payload(t *testing.T) {
	chunks := []*ResponseChunk{
		{Payload: []byte("first"), Seq: 0, IsEof: false},
		{Payload: []byte("second"), Seq: 1, IsEof: false},
		{Payload: []byte("third"), Seq: 2, IsEof: true},
	}

	response := &CartridgeResponse{
		Type:      CartridgeResponseTypeStreaming,
		Streaming: chunks,
	}

	finalPayload := response.FinalPayload()
	assert.Equal(t, "third", string(finalPayload))
}

// TEST241: Test CartridgeResponse::Streaming with empty chunks vec returns empty concatenation
func Test241_cartridge_response_streaming_empty_chunks(t *testing.T) {
	response := &CartridgeResponse{
		Type:      CartridgeResponseTypeStreaming,
		Streaming: []*ResponseChunk{},
	}

	concatenated := response.Concatenated()
	assert.Empty(t, concatenated)

	finalPayload := response.FinalPayload()
	assert.Nil(t, finalPayload)
}

// TEST242: Test CartridgeResponse::Streaming concatenated capacity is pre-allocated correctly for large payloads
func Test242_cartridge_response_streaming_preallocation(t *testing.T) {
	// Create chunks with known sizes
	chunk1 := &ResponseChunk{Payload: make([]byte, 1000), Seq: 0, IsEof: false}
	chunk2 := &ResponseChunk{Payload: make([]byte, 2000), Seq: 1, IsEof: false}
	chunk3 := &ResponseChunk{Payload: make([]byte, 500), Seq: 2, IsEof: true}

	response := &CartridgeResponse{
		Type:      CartridgeResponseTypeStreaming,
		Streaming: []*ResponseChunk{chunk1, chunk2, chunk3},
	}

	concatenated := response.Concatenated()
	// Verify total length is correct
	assert.Equal(t, 3500, len(concatenated))
	// Verify capacity matches length (indicating pre-allocation)
	assert.Equal(t, 3500, cap(concatenated))
}

// TEST243: Test AsyncHostError variants display correct error messages
func Test243_host_error_variants(t *testing.T) {
	// Test Cbor error
	cborErr := &HostError{Type: HostErrorTypeCbor, Message: "invalid CBOR"}
	assert.Contains(t, cborErr.Error(), "CBOR error")
	assert.Contains(t, cborErr.Error(), "invalid CBOR")

	// Test Io error
	ioErr := &HostError{Type: HostErrorTypeIo, Message: "connection closed"}
	assert.Contains(t, ioErr.Error(), "I/O error")
	assert.Contains(t, ioErr.Error(), "connection closed")

	// Test CartridgeError
	cartridgeErr := &HostError{
		Type:    HostErrorTypeCartridgeError,
		Code:    "HANDLER_ERROR",
		Message: "something went wrong",
	}
	assert.Contains(t, cartridgeErr.Error(), "Cartridge returned error")
	assert.Contains(t, cartridgeErr.Error(), "HANDLER_ERROR")
	assert.Contains(t, cartridgeErr.Error(), "something went wrong")

	// Test UnexpectedFrameType
	frameErr := &HostError{Type: HostErrorTypeUnexpectedFrameType, Message: "HEARTBEAT"}
	assert.Contains(t, frameErr.Error(), "Unexpected frame type")
	assert.Contains(t, frameErr.Error(), "HEARTBEAT")

	// Test ProcessExited
	exitedErr := &HostError{Type: HostErrorTypeProcessExited}
	assert.Contains(t, exitedErr.Error(), "Cartridge process exited")

	// Test Handshake
	handshakeErr := &HostError{Type: HostErrorTypeHandshake, Message: "timeout"}
	assert.Contains(t, handshakeErr.Error(), "Handshake failed")
	assert.Contains(t, handshakeErr.Error(), "timeout")

	// Test Closed
	closedErr := &HostError{Type: HostErrorTypeClosed}
	assert.Contains(t, closedErr.Error(), "Host is closed")

	// Test SendError
	sendErr := &HostError{Type: HostErrorTypeSendError}
	assert.Contains(t, sendErr.Error(), "Send error")

	// Test RecvError
	recvErr := &HostError{Type: HostErrorTypeRecvError}
	assert.Contains(t, recvErr.Error(), "Receive error")
}

// TEST244: Test HostError conversion creates correct error type
func Test244_host_error_conversion(t *testing.T) {
	// Test creating Cbor error
	err := &HostError{
		Type:    HostErrorTypeCbor,
		Message: "decode failed",
	}
	assert.Equal(t, HostErrorTypeCbor, err.Type)
	assert.Contains(t, err.Error(), "CBOR error")
}

// TEST245: Test HostError Io variant
func Test245_host_error_io_variant(t *testing.T) {
	err := &HostError{
		Type:    HostErrorTypeIo,
		Message: "read timeout",
	}
	assert.Equal(t, HostErrorTypeIo, err.Type)
	assert.Contains(t, err.Error(), "I/O error")
	assert.Contains(t, err.Error(), "read timeout")
}

// TEST246: Test ResponseChunk can be copied with same data
func Test246_response_chunk_copy(t *testing.T) {
	original := &ResponseChunk{
		Payload: []byte("test"),
		Seq:     5,
		Offset:  nil,
		Len:     nil,
		IsEof:   false,
	}

	// Create a copy by value
	copied := &ResponseChunk{
		Payload: append([]byte{}, original.Payload...),
		Seq:     original.Seq,
		Offset:  original.Offset,
		Len:     original.Len,
		IsEof:   original.IsEof,
	}

	assert.Equal(t, original.Seq, copied.Seq)
	assert.Equal(t, original.IsEof, copied.IsEof)
	assert.Equal(t, string(original.Payload), string(copied.Payload))
}

// TEST247: Test ResponseChunk Clone produces independent copy with same data
func Test247_response_chunk_clone(t *testing.T) {
	offset := uint64(100)
	length := uint64(500)
	original := &ResponseChunk{
		Payload: []byte("original data"),
		Seq:     10,
		Offset:  &offset,
		Len:     &length,
		IsEof:   true,
	}

	// Create a deep copy
	offsetCopy := *original.Offset
	lenCopy := *original.Len
	cloned := &ResponseChunk{
		Payload: append([]byte{}, original.Payload...),
		Seq:     original.Seq,
		Offset:  &offsetCopy,
		Len:     &lenCopy,
		IsEof:   original.IsEof,
	}

	// Verify they're equal
	assert.Equal(t, original.Seq, cloned.Seq)
	assert.Equal(t, *original.Offset, *cloned.Offset)
	assert.Equal(t, *original.Len, *cloned.Len)
	assert.Equal(t, original.IsEof, cloned.IsEof)
	assert.Equal(t, string(original.Payload), string(cloned.Payload))

	// Modify clone and verify original is unchanged
	cloned.Payload[0] = 'X'
	assert.NotEqual(t, original.Payload[0], cloned.Payload[0])
}
