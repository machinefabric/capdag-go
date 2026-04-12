package bifaci

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	cbor2 "github.com/fxamacker/cbor/v2"
)

// FrameReader reads length-prefixed CBOR frames from a stream
type FrameReader struct {
	reader io.Reader
	limits Limits
}

// NewFrameReader creates a new FrameReader
func NewFrameReader(r io.Reader) *FrameReader {
	return &FrameReader{
		reader: r,
		limits: DefaultLimits(),
	}
}

// SetLimits updates the reader's limits
func (fr *FrameReader) SetLimits(limits Limits) {
	fr.limits = limits
}

// ReadFrame reads a single frame from the stream
func (fr *FrameReader) ReadFrame() (*Frame, error) {
	// Read 4-byte length prefix (big-endian)
	var lengthBuf [4]byte
	if _, err := io.ReadFull(fr.reader, lengthBuf[:]); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBuf[:])

	// Enforce max_frame limit
	if int(length) > fr.limits.MaxFrame {
		return nil, fmt.Errorf("frame size %d exceeds max_frame limit %d", length, fr.limits.MaxFrame)
	}

	// Hard limit check
	if int(length) > MaxFrameHardLimit {
		return nil, fmt.Errorf("frame size %d exceeds hard limit %d", length, MaxFrameHardLimit)
	}

	// Read CBOR payload
	frameBuf := make([]byte, length)
	if _, err := io.ReadFull(fr.reader, frameBuf); err != nil {
		return nil, err
	}

	// Decode frame
	return DecodeFrame(frameBuf)
}

// FrameWriter writes length-prefixed CBOR frames to a stream
type FrameWriter struct {
	writer io.Writer
	limits Limits
}

// NewFrameWriter creates a new FrameWriter
func NewFrameWriter(w io.Writer) *FrameWriter {
	return &FrameWriter{
		writer: w,
		limits: DefaultLimits(),
	}
}

// SetLimits updates the writer's limits
func (fw *FrameWriter) SetLimits(limits Limits) {
	fw.limits = limits
}

// WriteFrame writes a single frame to the stream
func (fw *FrameWriter) WriteFrame(frame *Frame) error {
	// Encode frame to CBOR
	frameBuf, err := EncodeFrame(frame)
	if err != nil {
		return err
	}

	// Enforce max_frame limit
	if len(frameBuf) > fw.limits.MaxFrame {
		return fmt.Errorf("encoded frame size %d exceeds max_frame limit %d", len(frameBuf), fw.limits.MaxFrame)
	}

	// Hard limit check
	if len(frameBuf) > MaxFrameHardLimit {
		return fmt.Errorf("encoded frame size %d exceeds hard limit %d", len(frameBuf), MaxFrameHardLimit)
	}

	// Write 4-byte length prefix (big-endian)
	var lengthBuf [4]byte
	binary.BigEndian.PutUint32(lengthBuf[:], uint32(len(frameBuf)))
	if _, err := fw.writer.Write(lengthBuf[:]); err != nil {
		return err
	}

	// Write CBOR payload
	if _, err := fw.writer.Write(frameBuf); err != nil {
		return err
	}

	return nil
}

// WriteResponseWithChunking writes a response with automatic chunking for large payloads.
// Uses stream multiplexing protocol: STREAM_START + CHUNK + STREAM_END + END
func (fw *FrameWriter) WriteResponseWithChunking(requestId MessageId, streamId string, mediaUrn string, payload []byte) error {
	// Send STREAM_START
	startFrame := NewStreamStart(requestId, streamId, mediaUrn)
	if err := fw.WriteFrame(startFrame); err != nil {
		return err
	}

	// Send CHUNKs if payload is large
	chunkIndex := uint64(0)
	if len(payload) > 0 {
		offset := 0
		seq := uint64(0)

		for offset < len(payload) {
			remaining := len(payload) - offset
			chunkSize := min(remaining, fw.limits.MaxChunk)
			chunkData := payload[offset : offset+chunkSize]

			checksum := ComputeChecksum(chunkData)
			frame := NewChunk(requestId, streamId, seq, chunkData, chunkIndex, checksum)
			if err := fw.WriteFrame(frame); err != nil {
				return err
			}

			offset += chunkSize
			seq++
			chunkIndex++
		}
	}

	// Send STREAM_END
	endStreamFrame := NewStreamEnd(requestId, streamId, chunkIndex)
	if err := fw.WriteFrame(endStreamFrame); err != nil {
		return err
	}

	// Send END
	endFrame := NewEnd(requestId, nil)
	return fw.WriteFrame(endFrame)

	return nil
}

// HandshakeAccept performs handshake from cartridge side
func HandshakeAccept(reader *FrameReader, writer *FrameWriter, manifestData []byte) (Limits, error) {
	// 1. Read HELLO from host
	helloFrame, err := reader.ReadFrame()
	if err != nil {
		return Limits{}, fmt.Errorf("failed to read HELLO: %w", err)
	}

	if helloFrame.FrameType != FrameTypeHello {
		return Limits{}, errors.New("expected HELLO frame")
	}

	// 2. Decode host limits from Meta map
	var hostLimits Limits
	if helloFrame.Meta != nil {
		hostLimits.MaxFrame = extractIntFromMeta(helloFrame.Meta, "max_frame")
		hostLimits.MaxChunk = extractIntFromMeta(helloFrame.Meta, "max_chunk")
		hostLimits.MaxReorderBuffer = extractIntFromMeta(helloFrame.Meta, "max_reorder_buffer")
	}
	if hostLimits.MaxFrame == 0 || hostLimits.MaxChunk == 0 {
		hostLimits = DefaultLimits()
	}
	if hostLimits.MaxReorderBuffer == 0 {
		hostLimits.MaxReorderBuffer = DefaultMaxReorderBuffer
	}

	// 3. Send HELLO back with manifest
	responseFrame := NewHelloWithManifest(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, manifestData)
	if err := writer.WriteFrame(responseFrame); err != nil {
		return Limits{}, fmt.Errorf("failed to write HELLO response: %w", err)
	}

	// 4. Negotiate limits (min of both sides)
	negotiated := NegotiateLimits(DefaultLimits(), hostLimits)

	return negotiated, nil
}

// HandshakeInitiate performs handshake from host side
func HandshakeInitiate(reader *FrameReader, writer *FrameWriter) ([]byte, Limits, error) {
	// 1. Send HELLO with our limits
	helloFrame := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer)
	if err := writer.WriteFrame(helloFrame); err != nil {
		return nil, Limits{}, fmt.Errorf("failed to write HELLO: %w", err)
	}

	// 2. Read HELLO response with manifest
	responseFrame, err := reader.ReadFrame()
	if err != nil {
		return nil, Limits{}, fmt.Errorf("failed to read HELLO response: %w", err)
	}

	if responseFrame.FrameType != FrameTypeHello {
		return nil, Limits{}, errors.New("expected HELLO response")
	}

	// 3. Extract manifest from Meta map
	var manifestData []byte
	if responseFrame.Meta != nil {
		if manifest, ok := responseFrame.Meta["manifest"].([]byte); ok {
			manifestData = manifest
		}
	}

	// 4. Extract cartridge limits from Meta map
	var cartridgeLimits Limits
	if responseFrame.Meta != nil {
		cartridgeLimits.MaxFrame = extractIntFromMeta(responseFrame.Meta, "max_frame")
		cartridgeLimits.MaxChunk = extractIntFromMeta(responseFrame.Meta, "max_chunk")
		cartridgeLimits.MaxReorderBuffer = extractIntFromMeta(responseFrame.Meta, "max_reorder_buffer")
	}
	if cartridgeLimits.MaxFrame == 0 || cartridgeLimits.MaxChunk == 0 {
		cartridgeLimits = DefaultLimits()
	}
	if cartridgeLimits.MaxReorderBuffer == 0 {
		cartridgeLimits.MaxReorderBuffer = DefaultMaxReorderBuffer
	}

	// 5. Negotiate limits
	negotiated := NegotiateLimits(DefaultLimits(), cartridgeLimits)

	return manifestData, negotiated, nil
}

// EncodeCBOR encodes Limits to CBOR
func EncodeCBOR(limits Limits) ([]byte, error) {
	m := map[string]int{
		"max_frame":          limits.MaxFrame,
		"max_chunk":          limits.MaxChunk,
		"max_reorder_buffer": limits.MaxReorderBuffer,
	}
	return cbor2.Marshal(m)
}

// DecodeCBOR decodes CBOR to Limits
func DecodeCBOR(data []byte, limits *Limits) error {
	var m map[string]interface{}
	if err := cbor2.Unmarshal(data, &m); err != nil {
		return err
	}

	if maxFrameVal, ok := m["max_frame"]; ok {
		if maxFrame, ok := maxFrameVal.(uint64); ok {
			limits.MaxFrame = int(maxFrame)
		}
	}
	if maxChunkVal, ok := m["max_chunk"]; ok {
		if maxChunk, ok := maxChunkVal.(uint64); ok {
			limits.MaxChunk = int(maxChunk)
		}
	}

	return nil
}
