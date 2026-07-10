package bifaci

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	cbor2 "github.com/fxamacker/cbor/v2"
	"github.com/machinefabric/capdag-go/standard"
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
	startFrame := NewStreamStart(requestId, streamId, mediaUrn, nil)
	if err := fw.WriteFrame(startFrame); err != nil {
		return err
	}

	// Send CHUNKs if payload is large.
	//
	// Every CHUNK is emitted with seq=0. Sequence numbers are NOT a
	// per-chunk ordinal — they are assigned later by the SeqAssigner at
	// the output/multiplexing stage, where they order frames ACROSS
	// interleaved flows. Ordering WITHIN this stream is carried by
	// chunk_index (0, 1, 2, …). Baking an incrementing seq here would
	// double-assign and corrupt the reorder buffer's sequencing once the
	// frames pass through the SeqAssigner. Mirrors Rust write_chunked.
	chunkIndex := uint64(0)
	if len(payload) > 0 {
		offset := 0

		for offset < len(payload) {
			remaining := len(payload) - offset
			chunkSize := min(remaining, fw.limits.MaxChunk)
			chunkData := payload[offset : offset+chunkSize]

			checksum := ComputeChecksum(chunkData)
			frame := NewChunk(requestId, streamId, 0, chunkData, chunkIndex, checksum)
			if err := fw.WriteFrame(frame); err != nil {
				return err
			}

			offset += chunkSize
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

	// Protocol version must match exactly (L1). No cross-version operation.
	// (matches Rust handshake_accept)
	theirVersion := helloVersion(helloFrame)
	if theirVersion != ProtocolVersion {
		return Limits{}, fmt.Errorf("protocol version mismatch: ours %d, theirs %d", ProtocolVersion, theirVersion)
	}

	// 2. Decode host limits from Meta map
	var hostLimits Limits
	if helloFrame.Meta != nil {
		hostLimits.MaxFrame = extractIntFromMeta(helloFrame.Meta, "max_frame")
		hostLimits.MaxChunk = extractIntFromMeta(helloFrame.Meta, "max_chunk")
		hostLimits.MaxReorderBuffer = extractIntFromMeta(helloFrame.Meta, "max_reorder_buffer")
		hostLimits.InitialCredit = extractIntFromMeta(helloFrame.Meta, "initial_credit")
	}
	// Per-field defaulting: each absent limit falls back to its own default,
	// exactly like Rust's hello_max_frame().unwrap_or(DEFAULT_MAX_FRAME) etc.
	// A HELLO that carries one field but omits another keeps the carried value.
	if hostLimits.MaxFrame == 0 {
		hostLimits.MaxFrame = DefaultMaxFrame
	}
	if hostLimits.MaxChunk == 0 {
		hostLimits.MaxChunk = DefaultMaxChunk
	}
	if hostLimits.MaxReorderBuffer == 0 {
		hostLimits.MaxReorderBuffer = DefaultMaxReorderBuffer
	}
	if hostLimits.InitialCredit == 0 {
		hostLimits.InitialCredit = DefaultInitialCredit
	}

	// 3. Send HELLO back with manifest
	responseFrame := NewHelloWithManifest(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit, manifestData)
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
	helloFrame := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)
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

	// Protocol version must match exactly (L1). No cross-version operation.
	// (matches Rust handshake)
	theirVersion := helloVersion(responseFrame)
	if theirVersion != ProtocolVersion {
		return nil, Limits{}, fmt.Errorf("protocol version mismatch: ours %d, theirs %d", ProtocolVersion, theirVersion)
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
		cartridgeLimits.InitialCredit = extractIntFromMeta(responseFrame.Meta, "initial_credit")
	}
	// Per-field defaulting: each absent limit falls back to its own default,
	// exactly like Rust's hello_max_frame().unwrap_or(DEFAULT_MAX_FRAME) etc.
	// A HELLO that carries one field but omits another keeps the carried value.
	if cartridgeLimits.MaxFrame == 0 {
		cartridgeLimits.MaxFrame = DefaultMaxFrame
	}
	if cartridgeLimits.MaxChunk == 0 {
		cartridgeLimits.MaxChunk = DefaultMaxChunk
	}
	if cartridgeLimits.MaxReorderBuffer == 0 {
		cartridgeLimits.MaxReorderBuffer = DefaultMaxReorderBuffer
	}
	if cartridgeLimits.InitialCredit == 0 {
		cartridgeLimits.InitialCredit = DefaultInitialCredit
	}

	// 5. Negotiate limits
	negotiated := NegotiateLimits(DefaultLimits(), cartridgeLimits)

	return manifestData, negotiated, nil
}

// helloVersion reads the protocol version a HELLO frame is proposing: the
// "version" meta key when present, falling back to the frame's wire-level
// version field. Mirrors Rust Frame::hello_version().unwrap_or(frame.version),
// which is why handshake rejection must consult it rather than the wire field
// alone — a HELLO's meta is caller-supplied and can diverge from whatever the
// transport happened to stamp on the envelope. (matches Rust hello_version)
func helloVersion(f *Frame) uint8 {
	if f.Meta != nil {
		if v, ok := f.Meta["version"]; ok {
			switch n := v.(type) {
			case uint8:
				return n
			case int:
				return uint8(n)
			case int64:
				return uint8(n)
			case uint64:
				return uint8(n)
			case float64:
				return uint8(n)
			}
		}
	}
	return f.Version
}

// =============================================================================
// IDENTITY VERIFICATION
// =============================================================================

// identityNonce returns the CBOR-encoded Text("bifaci") — a deterministic
// 7-byte nonce for identity verification. (matches Rust identity_nonce)
func identityNonce() []byte {
	buf, err := cbor2.Marshal("bifaci")
	if err != nil {
		panic("BUG: failed to encode identity nonce")
	}
	return buf
}

// VerifyIdentity verifies a connection by invoking the identity capability.
//
// Sends a REQ with CAP_IDENTITY carrying the "bifaci" nonce with proper
// XID and seq assignment, then verifies the response echoes it back unchanged.
// This proves the entire protocol stack works end-to-end before the connection
// is considered live.
//
// Must be called after handshake, before any other traffic.
// (matches Rust verify_identity)
func VerifyIdentity(reader *FrameReader, writer *FrameWriter) error {
	nonce := identityNonce()
	reqId := NewMessageIdRandom()
	streamId := "identity-verify"
	xid := NewMessageIdFromUint(0)
	seq := NewSeqAssigner()

	// Send REQ (empty payload) with XID + seq
	req := NewReq(reqId, standard.CapIdentity, []byte{}, "application/cbor")
	req.RoutingId = &xid
	seq.Assign(req)
	if err := writer.WriteFrame(req); err != nil {
		return err
	}

	// Send request body: STREAM_START → CHUNK → STREAM_END → END
	streamStart := NewStreamStart(reqId, streamId, "media:", nil)
	streamStart.RoutingId = &xid
	seq.Assign(streamStart)
	if err := writer.WriteFrame(streamStart); err != nil {
		return err
	}

	// CBOR-encode nonce before checksumming (protocol v2: CHUNK payload = CBOR-encoded data)
	cborNonce, err := cbor2.Marshal(nonce)
	if err != nil {
		return fmt.Errorf("BUG: failed to CBOR-encode nonce: %w", err)
	}
	checksum := ComputeChecksum(cborNonce)
	chunk := NewChunk(reqId, streamId, 0, cborNonce, 0, checksum)
	chunk.RoutingId = &xid
	seq.Assign(chunk)
	if err := writer.WriteFrame(chunk); err != nil {
		return err
	}

	streamEnd := NewStreamEnd(reqId, streamId, 1)
	streamEnd.RoutingId = &xid
	seq.Assign(streamEnd)
	if err := writer.WriteFrame(streamEnd); err != nil {
		return err
	}

	end := NewEnd(reqId, nil)
	end.RoutingId = &xid
	seq.Assign(end)
	if err := writer.WriteFrame(end); err != nil {
		return err
	}

	// Read response — expect STREAM_START → CHUNK(s) → STREAM_END → END
	// Each CHUNK payload is CBOR-encoded (protocol v2), decode each and concatenate
	var cborChunks [][]byte
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			if err == io.EOF {
				return errors.New("Connection closed during identity verification")
			}
			return err
		}

		switch frame.FrameType {
		case FrameTypeStreamStart:
			// no-op
		case FrameTypeChunk:
			if frame.Payload != nil {
				var decoded []byte
				if err := cbor2.Unmarshal(frame.Payload, &decoded); err != nil {
					return fmt.Errorf("Failed to decode CBOR chunk: %w", err)
				}
				cborChunks = append(cborChunks, decoded)
			}
		case FrameTypeStreamEnd:
			// no-op
		case FrameTypeEnd:
			// Concatenate all decoded chunks
			var accumulated []byte
			for _, c := range cborChunks {
				accumulated = append(accumulated, c...)
			}
			if !bytesEqual(accumulated, nonce) {
				return fmt.Errorf(
					"Identity verification failed: payload mismatch (expected %d bytes, got %d)",
					len(nonce), len(accumulated),
				)
			}
			return nil
		case FrameTypeErr:
			code := frame.ErrorCode()
			if code == "" {
				code = "UNKNOWN"
			}
			msg := frame.ErrorMessage()
			if msg == "" {
				msg = "no message"
			}
			return fmt.Errorf("Identity verification failed: [%s] %s", code, msg)
		default:
			return fmt.Errorf("Identity verification: unexpected frame type %v", frame.FrameType)
		}
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
