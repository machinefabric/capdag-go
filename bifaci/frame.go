package bifaci

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// Protocol version. Version 3: credit-based per-stream flow control, unbounded
// streams, terminal metadata on END (final progress rides in the terminal frame),
// counted drops, handshake version enforcement. Version 2 handshakes are rejected.
const ProtocolVersion uint8 = 3

// Default maximum frame size (3.5 MB) - safe margin below 3.75MB limit
// Larger payloads automatically use CHUNK frames
const DefaultMaxFrame int = 3_670_016

// Default maximum chunk size (256 KB)
const DefaultMaxChunk int = 262_144

// Hard limit on frame size (16 MB) - prevents DoS
const MaxFrameHardLimit int = 16_777_216

// FrameType represents the type of CBOR frame
type FrameType uint8

const (
	FrameTypeHello FrameType = 0 // MUST be 0 - matches Rust
	FrameTypeReq   FrameType = 1
	// Res = 2 REMOVED - old single-response protocol no longer supported
	FrameTypeChunk       FrameType = 3
	FrameTypeEnd         FrameType = 4
	FrameTypeLog         FrameType = 5 // MUST be 5 - matches Rust
	FrameTypeErr         FrameType = 6 // MUST be 6 - matches Rust
	FrameTypeHeartbeat   FrameType = 7
	FrameTypeStreamStart FrameType = 8  // Announce new stream for a request (multiplexed streaming)
	FrameTypeStreamEnd   FrameType = 9  // End a specific stream (multiplexed streaming)
	FrameTypeRelayNotify FrameType = 10 // Relay capability advertisement (slave → master)
	FrameTypeRelayState  FrameType = 11 // Relay host system resources + cap demands (master → slave)
	FrameTypeCancel      FrameType = 12 // Cancel a running request
	// FrameTypeCredit grants per-stream flow-control credit (protocol v3, L9/L10).
	// Non-flow (see IsFlowFrame): bypasses seq assignment and the reorder buffer.
	// NOTE: wire-level CBOR encode/decode support (including HELLO's negotiated
	// initial_credit and the protocol v3 version bump) is ported by the frame/codec
	// subsystem task, not here; this credit-subsystem port only needs the
	// in-memory discriminant, the credit_dir/credit accessors, and the constructor
	// that the CreditRouter and its parity tests exercise directly on Frame values.
	FrameTypeCredit FrameType = 13
)

// String returns the frame type name
func (ft FrameType) String() string {
	switch ft {
	case FrameTypeReq:
		return "REQ"
	// Res REMOVED - old protocol no longer supported
	case FrameTypeChunk:
		return "CHUNK"
	case FrameTypeEnd:
		return "END"
	case FrameTypeErr:
		return "ERR"
	case FrameTypeLog:
		return "LOG"
	case FrameTypeHeartbeat:
		return "HEARTBEAT"
	case FrameTypeHello:
		return "HELLO"
	case FrameTypeStreamStart:
		return "STREAM_START"
	case FrameTypeStreamEnd:
		return "STREAM_END"
	case FrameTypeRelayNotify:
		return "RELAY_NOTIFY"
	case FrameTypeRelayState:
		return "RELAY_STATE"
	case FrameTypeCancel:
		return "CANCEL"
	case FrameTypeCredit:
		return "CREDIT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", ft)
	}
}

// CreditDirection identifies which side's stream a CREDIT frame credits
// (L11 routing discriminator). Request credits a request-direction stream
// (arguments flowing toward the handler): the grant travels toward the
// REQUESTER. Response credits a response-direction stream (results flowing
// back to the caller): the grant travels toward the HANDLER. Required on
// every CREDIT frame — (xid, rid) alone cannot disambiguate direction when
// both sides of a relay hop share the same request/routing id.
// (matches Rust CreditDirection)
type CreditDirection uint8

const (
	CreditDirectionRequest CreditDirection = iota
	CreditDirectionResponse
)

// String returns the wire string name ("request"/"response"), matching Rust's
// CreditDirection::as_str.
func (d CreditDirection) String() string {
	switch d {
	case CreditDirectionRequest:
		return "request"
	case CreditDirectionResponse:
		return "response"
	default:
		return "unknown"
	}
}

// CreditDirectionFromString parses a CreditDirection from its wire string name.
// Returns false if the string is not a recognized direction (matches Rust
// CreditDirection::from_str_name).
func CreditDirectionFromString(s string) (CreditDirection, bool) {
	switch s {
	case "request":
		return CreditDirectionRequest, true
	case "response":
		return CreditDirectionResponse, true
	default:
		return 0, false
	}
}

// MessageId represents a unique message identifier (either UUID or uint64)
type MessageId struct {
	uuidBytes []byte  // 16 bytes for UUID variant
	uintValue *uint64 // For uint variant
}

// NewMessageIdFromUuid creates a MessageId from UUID bytes
func NewMessageIdFromUuid(uuidBytes []byte) (MessageId, error) {
	if len(uuidBytes) != 16 {
		return MessageId{}, errors.New("UUID must be exactly 16 bytes")
	}
	return MessageId{uuidBytes: uuidBytes}, nil
}

// NewMessageIdFromUuidString creates a MessageId from a UUID string.
func NewMessageIdFromUuidString(value string) (MessageId, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return MessageId{}, err
	}
	bytes, err := id.MarshalBinary()
	if err != nil {
		return MessageId{}, err
	}
	return MessageId{uuidBytes: bytes}, nil
}

// NewMessageIdFromUint creates a MessageId from a uint64
func NewMessageIdFromUint(value uint64) MessageId {
	return MessageId{uintValue: &value}
}

// NewMessageIdRandom creates a random UUID-based MessageId
func NewMessageIdRandom() MessageId {
	id := uuid.New()
	bytes, _ := id.MarshalBinary()
	return MessageId{uuidBytes: bytes}
}

// NewMessageIdDefault creates a default MessageId.
// Default MessageIds are UUID-based, matching the reference implementation.
func NewMessageIdDefault() MessageId {
	return NewMessageIdRandom()
}

// IsUuid returns true if this is a UUID-based ID
func (m MessageId) IsUuid() bool {
	return m.uuidBytes != nil
}

// ToUuidString returns UUID string representation (empty if uint variant)
func (m MessageId) ToUuidString() string {
	if m.uuidBytes != nil {
		id, err := uuid.FromBytes(m.uuidBytes)
		if err == nil {
			return id.String()
		}
	}
	return ""
}

// ToString returns string representation for both UUID and uint variants
func (m MessageId) ToString() string {
	if m.uuidBytes != nil {
		return m.ToUuidString()
	}
	if m.uintValue != nil {
		return fmt.Sprintf("%d", *m.uintValue)
	}
	return "0"
}

// AsBytes returns bytes for comparison
func (m MessageId) AsBytes() []byte {
	if m.uuidBytes != nil {
		return m.uuidBytes
	}
	if m.uintValue != nil {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, *m.uintValue)
		return buf
	}
	return make([]byte, 8)
}

// Equals checks if two MessageIds are equal
func (m MessageId) Equals(other MessageId) bool {
	// Both UUID
	if m.uuidBytes != nil && other.uuidBytes != nil {
		return string(m.uuidBytes) == string(other.uuidBytes)
	}
	// Both uint
	if m.uintValue != nil && other.uintValue != nil {
		return *m.uintValue == *other.uintValue
	}
	// Different types
	return false
}

// Frame represents a CBOR protocol frame
// This structure MUST match the Rust Frame structure exactly
type Frame struct {
	Version     uint8                  // Protocol version (always ProtocolVersion)
	FrameType   FrameType              // Frame type discriminator
	Id          MessageId              // Message ID for correlation (request ID)
	StreamId    *string                // Stream ID for multiplexed streams (used in STREAM_START, CHUNK, STREAM_END)
	MediaUrn    *string                // Media URN for stream type identification (used in STREAM_START)
	Seq         uint64                 // Sequence number within a stream
	ContentType *string                // Content type of payload (MIME-like)
	Meta        map[string]interface{} // Metadata map (for ERR/LOG data, HELLO limits, etc.)
	Payload     []byte                 // Binary payload
	Len         *uint64                // Total length for chunked transfers (first chunk only)
	Offset      *uint64                // Byte offset in chunked stream
	Eof         *bool                  // End of stream marker
	Cap         *string                // Cap URN (for REQ frames)
	RoutingId   *MessageId             // Routing ID for relay (optional)
	ChunkIndex  *uint64                // Chunk index within stream (REQUIRED for CHUNK frames)
	ChunkCount  *uint64                // Total chunk count (REQUIRED for STREAM_END frames)
	Checksum    *uint64                // Payload checksum (FNV-1a hash, REQUIRED for CHUNK frames)
	IsSequence  *bool                  // Whether producer used emit_list_item (true) or write (false)
	ForceKill   *bool                  // Whether Cancel should force-kill the cartridge process
	Credit      *uint64                // Flow-control credit grant in CHUNK units (CREDIT frames only, protocol v3)
	// Unbounded marks a STREAM_START whose stream makes no length promise: its
	// STREAM_END may omit chunk_count and receivers must consume it
	// incrementally, never buffering to completion. Present on STREAM_START
	// frames only (protocol v3, L16). NOTE: wire-level CBOR encode/decode
	// support for this field (key 20) is ported by the frame/codec subsystem
	// task, not here; the request-state subsystem only needs the in-memory
	// flag and the constructors/accessor its parity tests exercise directly
	// on Frame values.
	Unbounded *bool
}

// New creates a new frame with required fields (matches Rust Frame::new)
func newFrame(frameType FrameType, id MessageId) *Frame {
	return &Frame{
		Version:   ProtocolVersion,
		FrameType: frameType,
		Id:        id,
		Seq:       0,
	}
}

// NewReq creates a REQ frame (matches Rust Frame::req)
func NewReq(id MessageId, capUrn string, payload []byte, contentType string) *Frame {
	frame := newFrame(FrameTypeReq, id)
	frame.Cap = &capUrn
	frame.Payload = payload
	frame.ContentType = &contentType
	return frame
}

// NewChunk creates a CHUNK frame for multiplexed streaming.
// Each chunk belongs to a specific stream within a request.
//
// Arguments:
//   - reqId: The request ID this chunk belongs to
//   - streamId: The stream ID this chunk belongs to
//   - seq: Sequence number within the stream
//   - payload: Chunk data
//
// (matches Rust Frame::chunk)
func NewChunk(reqId MessageId, streamId string, seq uint64, payload []byte, chunkIndex uint64, checksum uint64) *Frame {
	frame := newFrame(FrameTypeChunk, reqId)
	frame.StreamId = &streamId
	frame.Seq = seq
	frame.Payload = payload
	frame.ChunkIndex = &chunkIndex
	frame.Checksum = &checksum
	return frame
}

// NewChunkWithOffset creates a CHUNK frame with byte offset metadata.
// Offset is set on all chunks. Len is set only on the first chunk (chunkIndex == 0).
// Eof is set only when isLast is true.
func NewChunkWithOffset(
	reqId MessageId,
	streamId string,
	seq uint64,
	payload []byte,
	offset uint64,
	totalLen *uint64,
	isLast bool,
	chunkIndex uint64,
	checksum uint64,
) *Frame {
	frame := NewChunk(reqId, streamId, seq, payload, chunkIndex, checksum)
	frame.Offset = &offset
	if chunkIndex == 0 {
		frame.Len = totalLen
	}
	if isLast {
		eof := true
		frame.Eof = &eof
	}
	return frame
}

// NewStreamStart creates a STREAM_START frame to announce a new stream.
//
// Arguments:
//   - reqId: The request ID this stream belongs to
//   - streamId: Unique ID for this stream (UUID generated by sender)
//   - mediaUrn: Media URN identifying the stream's data type
//   - isSequence: Whether the producer uses emit_list_item (true) or write (false); nil if unknown
//
// (matches Rust Frame::stream_start)
func NewStreamStart(reqId MessageId, streamId string, mediaUrn string, isSequence *bool) *Frame {
	frame := newFrame(FrameTypeStreamStart, reqId)
	frame.StreamId = &streamId
	frame.MediaUrn = &mediaUrn
	frame.IsSequence = isSequence
	return frame
}

// NewStreamEnd creates a STREAM_END frame to end a specific stream.
// After this, any CHUNK for this streamId is a fatal protocol error.
//
// Arguments:
//   - reqId: The request ID this stream belongs to
//   - streamId: The stream being ended
//   - chunkCount: Total number of chunks sent in this stream (by source's reckoning)
//
// (matches Rust Frame::stream_end)
func NewStreamEnd(reqId MessageId, streamId string, chunkCount uint64) *Frame {
	frame := newFrame(FrameTypeStreamEnd, reqId)
	frame.StreamId = &streamId
	frame.ChunkCount = &chunkCount
	return frame
}

// NewStreamStartUnbounded creates a STREAM_START frame for an UNBOUNDED
// stream — one that makes no length promise. Its STREAM_END may omit
// chunk_count, and receivers must consume it incrementally (never buffer to
// completion). (matches Rust Frame::stream_start_unbounded)
func NewStreamStartUnbounded(reqId MessageId, streamId string, mediaUrn string, isSequence *bool) *Frame {
	frame := NewStreamStart(reqId, streamId, mediaUrn, isSequence)
	unbounded := true
	frame.Unbounded = &unbounded
	return frame
}

// IsUnbounded reports whether this STREAM_START announces an unbounded
// stream. Absent flag means bounded. (matches Rust Frame::is_unbounded)
func (f *Frame) IsUnbounded() bool {
	return f.Unbounded != nil && *f.Unbounded
}

// NewStreamEndUnbounded creates a STREAM_END frame for an unbounded stream —
// no chunk_count promise. Valid only for streams announced with
// NewStreamStartUnbounded. (matches Rust Frame::stream_end_unbounded)
func NewStreamEndUnbounded(reqId MessageId, streamId string) *Frame {
	frame := newFrame(FrameTypeStreamEnd, reqId)
	frame.StreamId = &streamId
	return frame
}

// NewEnd creates an END frame (matches Rust Frame::end)
func NewEnd(id MessageId, payload []byte) *Frame {
	frame := newFrame(FrameTypeEnd, id)
	if payload != nil {
		frame.Payload = payload
	}
	eof := true
	frame.Eof = &eof
	return frame
}

// NewFrame creates a new frame with required fields (exported version).
func NewFrame(frameType FrameType, id MessageId) *Frame {
	return newFrame(frameType, id)
}

// DefaultFrame creates the documented default frame shape: a REQ frame with a default MessageId.
func DefaultFrame() *Frame {
	return newFrame(FrameTypeReq, NewMessageIdDefault())
}

// EndOk creates an END frame with exit_code=0 (success).
func EndOk(id MessageId, finalPayload []byte) *Frame {
	frame := newFrame(FrameTypeEnd, id)
	if finalPayload != nil {
		frame.Payload = finalPayload
	}
	eof := true
	frame.Eof = &eof
	frame.Meta = map[string]interface{}{"exit_code": int64(0)}
	return frame
}

// EndOkWith creates an END frame with exit_code=0 (success) carrying terminal
// metadata. progress is the authoritative final progress value delivered with
// the terminal frame itself (so it can never race it); message is an optional
// final status message. A successful END without an explicit progress reads
// as 1.0 via FinalProgress. (matches Rust Frame::end_ok_with)
func EndOkWith(id MessageId, finalPayload []byte, progress *float64, message *string) *Frame {
	frame := EndOk(id, finalPayload)
	if progress != nil {
		frame.Meta["progress"] = *progress
	}
	if message != nil {
		frame.Meta["message"] = *message
	}
	return frame
}

// FinalProgress reads the final progress from an END frame's terminal
// metadata. Returns the explicit progress meta value when present; a
// successful END (exit_code=0) without an explicit value reads as 1.0.
// Non-END frames and unsuccessful ENDs without a value return nil.
// (matches Rust Frame::final_progress)
func (f *Frame) FinalProgress() *float64 {
	if f.FrameType != FrameTypeEnd {
		return nil
	}
	if f.Meta != nil {
		if v, ok := f.Meta["progress"]; ok {
			switch n := v.(type) {
			case float64:
				return &n
			case float32:
				p := float64(n)
				return &p
			case int64:
				p := float64(n)
				return &p
			case int:
				p := float64(n)
				return &p
			case uint64:
				p := float64(n)
				return &p
			}
		}
	}
	if code := f.ExitCode(); code != nil && *code == 0 {
		one := 1.0
		return &one
	}
	return nil
}

// FinalMessage reads the final status message from an END frame's terminal
// metadata. (matches Rust Frame::final_message)
func (f *Frame) FinalMessage() *string {
	if f.FrameType != FrameTypeEnd || f.Meta == nil {
		return nil
	}
	if s, ok := f.Meta["message"].(string); ok {
		return &s
	}
	return nil
}

// ExitCode returns the exit_code from the Meta map if present.
// Returns nil if Meta is nil or the key is absent.
func (f *Frame) ExitCode() *int64 {
	if f.Meta == nil {
		return nil
	}
	v, ok := f.Meta["exit_code"]
	if !ok {
		return nil
	}
	var code int64
	switch n := v.(type) {
	case int64:
		code = n
	case int:
		code = int64(n)
	case uint64:
		code = int64(n)
	case float64:
		code = int64(n)
	default:
		return nil
	}
	return &code
}

// NewCancelFrame creates a CANCEL frame targeting the given request ID.
// forceKill indicates whether the cartridge process should be force-killed.
func NewCancelFrame(targetRid MessageId, forceKill bool) *Frame {
	frame := newFrame(FrameTypeCancel, targetRid)
	frame.ForceKill = &forceKill
	return frame
}

// NewCredit creates a CREDIT frame granting per-stream flow-control credit to
// the sender of a stream (protocol v3).
//
// Arguments:
//   - targetRid: The request whose stream is being credited
//   - streamId: The stream being credited (nil credits the request's
//     sole/default stream)
//   - credits: Number of additional CHUNK frames the sender may emit
//   - direction: Which side's stream is being credited. Hosts route by
//     (rid, stream_id); the direction disambiguates which endpoint sent the
//     grant when both share the same rid across a relay hop.
//
// (matches Rust Frame::credit)
func NewCredit(targetRid MessageId, streamId *string, credits uint64, direction CreditDirection) *Frame {
	frame := newFrame(FrameTypeCredit, targetRid)
	frame.StreamId = streamId
	frame.Credit = &credits
	frame.Meta = map[string]interface{}{"credit_dir": direction.String()}
	return frame
}

// CreditCount reads the credit grant from a CREDIT frame. Returns nil for
// other frame types. (matches Rust Frame::credit_count)
func (f *Frame) CreditCount() *uint64 {
	if f.FrameType != FrameTypeCredit {
		return nil
	}
	return f.Credit
}

// CreditDirectionValue reads the direction of a CREDIT frame's grant. Returns
// nil for other frame types or a CREDIT frame without the mandatory direction
// (a protocol violation elsewhere validated). (matches Rust Frame::credit_direction)
func (f *Frame) CreditDirectionValue() *CreditDirection {
	if f.FrameType != FrameTypeCredit || f.Meta == nil {
		return nil
	}
	s, ok := f.Meta["credit_dir"].(string)
	if !ok {
		return nil
	}
	dir, ok := CreditDirectionFromString(s)
	if !ok {
		return nil
	}
	return &dir
}

// NewErr creates an ERR frame (matches Rust Frame::err). The class defaults
// to Internal — an error that reaches the wire without a declared class is
// the emitter's problem by definition; emitters with a classified error use
// NewErrClassified.
func NewErr(id MessageId, code string, message string) *Frame {
	return NewErrClassified(id, code, FailureClassInternal, message)
}

// NewErrClassified creates an ERR frame carrying the full failure identity:
// the emitter's machine-readable code (e.g. CONTEXT_OVERFLOW), the failure
// CLASS (whose problem it is — declared at the error's definition site, see
// FailureClass), and the human message. ERR meta contract (docs/12.2):
// "code" + "class" + "message", all text. (matches Rust Frame::err_classified)
func NewErrClassified(id MessageId, code string, class FailureClass, message string) *Frame {
	frame := newFrame(FrameTypeErr, id)
	frame.Meta = map[string]interface{}{
		"code":    code,
		"class":   class.String(),
		"message": message,
	}
	return frame
}

// NewLog creates a LOG frame (matches Rust Frame::log)
// level and message are stored in the Meta map
func NewLog(id MessageId, level string, message string) *Frame {
	frame := newFrame(FrameTypeLog, id)
	frame.Meta = map[string]interface{}{
		"level":   level,
		"message": message,
	}
	return frame
}

// NewProgress creates a LOG frame with progress (0.0-1.0) and a human-readable status message.
// Uses level="progress" with an additional "progress" key in metadata.
func NewProgress(id MessageId, progress float32, message string) *Frame {
	frame := newFrame(FrameTypeLog, id)
	frame.Meta = map[string]interface{}{
		"level":    "progress",
		"message":  message,
		"progress": float64(progress),
	}
	return frame
}

// NewHeartbeat creates a HEARTBEAT frame (matches Rust Frame::heartbeat)
func NewHeartbeat(id MessageId) *Frame {
	return newFrame(FrameTypeHeartbeat, id)
}

// NewHello creates a HELLO frame for handshake (host side - no manifest).
// initialCredit is the proposed initial per-stream credit window (protocol v3);
// it is negotiated by the peer to the element-wise minimum, same as the other
// three limits. Matches Rust Frame::hello.
func NewHello(maxFrame, maxChunk, maxReorderBuffer, initialCredit int) *Frame {
	frame := newFrame(FrameTypeHello, MessageId{uintValue: new(uint64)})
	frame.Meta = map[string]interface{}{
		"max_frame":          maxFrame,
		"max_chunk":          maxChunk,
		"max_reorder_buffer": maxReorderBuffer,
		"initial_credit":     initialCredit,
		"version":            ProtocolVersion,
	}
	return frame
}

// NewHelloWithManifest creates a HELLO frame with manifest (cartridge side).
// initialCredit is the proposed initial per-stream credit window (protocol v3);
// it is negotiated by the peer to the element-wise minimum, same as the other
// three limits. Matches Rust Frame::hello_with_manifest.
func NewHelloWithManifest(maxFrame, maxChunk, maxReorderBuffer, initialCredit int, manifest []byte) *Frame {
	frame := newFrame(FrameTypeHello, MessageId{uintValue: new(uint64)})
	frame.Meta = map[string]interface{}{
		"max_frame":          maxFrame,
		"max_chunk":          maxChunk,
		"max_reorder_buffer": maxReorderBuffer,
		"initial_credit":     initialCredit,
		"version":            ProtocolVersion,
		"manifest":           manifest,
	}
	return frame
}

// NewRelayNotify creates a RELAY_NOTIFY frame for capability advertisement (slave → master).
// Carries aggregate manifest + negotiated limits (including the per-stream
// initial_credit window, protocol v3). (matches Rust Frame::relay_notify)
func NewRelayNotify(manifest []byte, maxFrame, maxChunk, maxReorderBuffer, initialCredit int) *Frame {
	frame := newFrame(FrameTypeRelayNotify, MessageId{uintValue: new(uint64)})
	frame.Meta = map[string]interface{}{
		"manifest":           manifest,
		"max_frame":          maxFrame,
		"max_chunk":          maxChunk,
		"max_reorder_buffer": maxReorderBuffer,
		"initial_credit":     initialCredit,
	}
	return frame
}

// NewRelayState creates a RELAY_STATE frame for host system resources + cap demands (master → slave).
// Carries an opaque resource payload. (matches Rust Frame::relay_state)
func NewRelayState(resources []byte) *Frame {
	frame := newFrame(FrameTypeRelayState, MessageId{uintValue: new(uint64)})
	frame.Payload = resources
	return frame
}

// Helper methods to extract values from Meta map (matches Rust Frame::error_code, error_message, log_level, log_message)

// ErrorCode gets error code from ERR frame meta
func (f *Frame) ErrorCode() string {
	if f.FrameType != FrameTypeErr || f.Meta == nil {
		return ""
	}
	if code, ok := f.Meta["code"].(string); ok {
		return code
	}
	return ""
}

// ErrorClass gets the failure class from ERR frame meta. A frame without a
// "class" entry (or with an unknown token) classifies as Internal:
// unclassified means "the emitter's problem", never a guess about the user's
// input. Non-ERR frames also return Internal (the degenerate value, matching
// ErrorCode's "" sentinel — callers only read this on ERR frames).
// (matches Rust Frame::error_class)
func (f *Frame) ErrorClass() FailureClass {
	if f.FrameType != FrameTypeErr || f.Meta == nil {
		return FailureClassInternal
	}
	token, ok := f.Meta["class"].(string)
	if !ok {
		return FailureClassInternal
	}
	class, ok := FailureClassFromWire(token)
	if !ok {
		return FailureClassInternal
	}
	return class
}

// ErrorMessage gets error message from ERR frame meta
func (f *Frame) ErrorMessage() string {
	if f.FrameType != FrameTypeErr || f.Meta == nil {
		return ""
	}
	if msg, ok := f.Meta["message"].(string); ok {
		return msg
	}
	return ""
}

// LogLevel gets log level from LOG frame meta
func (f *Frame) LogLevel() string {
	if f.FrameType != FrameTypeLog || f.Meta == nil {
		return ""
	}
	if level, ok := f.Meta["level"].(string); ok {
		return level
	}
	return ""
}

// LogMessage gets log message from LOG frame meta
func (f *Frame) LogMessage() string {
	if f.FrameType != FrameTypeLog || f.Meta == nil {
		return ""
	}
	if msg, ok := f.Meta["message"].(string); ok {
		return msg
	}
	return ""
}

// LogProgress gets progress value (0.0-1.0) if this is a LOG frame with level="progress".
// Returns (progress, true) if present, (0, false) otherwise.
func (f *Frame) LogProgress() (float32, bool) {
	if f.FrameType != FrameTypeLog || f.Meta == nil {
		return 0, false
	}
	level, ok := f.Meta["level"].(string)
	if !ok || level != "progress" {
		return 0, false
	}
	switch v := f.Meta["progress"].(type) {
	case float64:
		return float32(v), true
	case float32:
		return v, true
	case int:
		return float32(v), true
	case int64:
		return float32(v), true
	default:
		return 0, false
	}
}

// RelayNotifyManifest extracts manifest bytes from RelayNotify metadata.
// Returns nil if not a RelayNotify frame or no manifest present.
func (f *Frame) RelayNotifyManifest() []byte {
	if f.FrameType != FrameTypeRelayNotify || f.Meta == nil {
		return nil
	}
	if manifest, ok := f.Meta["manifest"].([]byte); ok {
		return manifest
	}
	return nil
}

// RelayNotifyLimits extracts Limits from RelayNotify metadata.
// Returns nil if not a RelayNotify frame or limits are missing.
func (f *Frame) RelayNotifyLimits() *Limits {
	if f.FrameType != FrameTypeRelayNotify || f.Meta == nil {
		return nil
	}
	maxFrame := extractIntFromMeta(f.Meta, "max_frame")
	maxChunk := extractIntFromMeta(f.Meta, "max_chunk")
	if maxFrame <= 0 || maxChunk <= 0 {
		return nil
	}
	maxReorderBuffer := extractIntFromMeta(f.Meta, "max_reorder_buffer")
	if maxReorderBuffer <= 0 {
		maxReorderBuffer = DefaultMaxReorderBuffer
	}
	initialCredit := extractIntFromMeta(f.Meta, "initial_credit")
	if initialCredit <= 0 {
		initialCredit = DefaultInitialCredit
	}
	return &Limits{MaxFrame: maxFrame, MaxChunk: maxChunk, MaxReorderBuffer: maxReorderBuffer, InitialCredit: initialCredit}
}

// extractIntFromMeta extracts an integer from a meta map, handling CBOR type variance.
// CBOR libraries may decode integers as int, int64, uint64, or float64.
func extractIntFromMeta(meta map[string]interface{}, key string) int {
	v, ok := meta[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case uint64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

// ComputeChecksum computes FNV-1a 64-bit hash of data (matches Rust Frame::compute_checksum)
func ComputeChecksum(data []byte) uint64 {
	const FNV_OFFSET_BASIS = uint64(0xcbf29ce484222325)
	const FNV_PRIME = uint64(0x100000001b3)

	hash := FNV_OFFSET_BASIS
	for _, b := range data {
		hash ^= uint64(b)
		hash = hash * FNV_PRIME
	}
	return hash
}

// VerifyChunkChecksum verifies a CHUNK frame's checksum matches its payload.
// Returns nil if valid, error if checksum missing or mismatched.
func VerifyChunkChecksum(frame *Frame) error {
	if frame.Checksum == nil {
		return fmt.Errorf("CHUNK frame missing required checksum field")
	}
	if frame.Payload == nil {
		// Empty payload - checksum should be for empty data
		expected := ComputeChecksum([]byte{})
		if *frame.Checksum != expected {
			return fmt.Errorf("CHUNK checksum mismatch: expected %d, got %d (empty payload)", expected, *frame.Checksum)
		}
		return nil
	}
	expected := ComputeChecksum(frame.Payload)
	if *frame.Checksum != expected {
		return fmt.Errorf("CHUNK checksum mismatch: expected %d, got %d (payload %d bytes)", expected, *frame.Checksum, len(frame.Payload))
	}
	return nil
}

// IsEof checks if this is the final frame in a stream (matches Rust Frame::is_eof)
func (f *Frame) IsEof() bool {
	return f.Eof != nil && *f.Eof
}

// IsFlowFrame returns true if this frame type participates in flow ordering (seq tracking).
// Non-flow frames (Hello, Heartbeat, RelayNotify, RelayState, Cancel, Credit) bypass seq
// assignment and reorder buffers entirely — Credit in particular must never be delayed by a
// gapped flow, since a sender waiting on it would stall until the gap is filled.
// (matches Rust Frame::is_flow_frame)
func (f *Frame) IsFlowFrame() bool {
	switch f.FrameType {
	case FrameTypeHello, FrameTypeHeartbeat, FrameTypeRelayNotify, FrameTypeRelayState, FrameTypeCancel, FrameTypeCredit:
		return false
	default:
		return true
	}
}

// =============================================================================
// FLOW KEY — Composite key for frame ordering (RID + optional XID)
// =============================================================================

// FlowKey is a composite key identifying a frame flow for seq ordering.
// Absence of XID (RoutingId) is a valid separate flow from presence of XID.
// (matches Rust FlowKey)
type FlowKey struct {
	rid string // Serialized RID for map key
	xid string // Serialized XID for map key (empty = no XID)
}

// FlowKeyFromFrame extracts a FlowKey from a frame.
func FlowKeyFromFrame(frame *Frame) FlowKey {
	xid := ""
	if frame.RoutingId != nil {
		xid = frame.RoutingId.ToString()
	}
	return FlowKey{
		rid: frame.Id.ToString(),
		xid: xid,
	}
}

// =============================================================================
// SEQ ASSIGNER — Centralized seq assignment at output stages
// =============================================================================

// SeqAssigner assigns monotonically increasing seq numbers per FlowKey.
// Used at output stages (writer threads) to ensure each flow's frames
// carry a contiguous, gap-free seq sequence starting at 0.
// Non-flow frames (Hello, Heartbeat, RelayNotify, RelayState) are skipped.
// (matches Rust SeqAssigner)
type SeqAssigner struct {
	counters map[FlowKey]uint64
}

// NewSeqAssigner creates a new SeqAssigner.
func NewSeqAssigner() *SeqAssigner {
	return &SeqAssigner{
		counters: make(map[FlowKey]uint64),
	}
}

// Assign assigns the next seq number to a frame.
// Non-flow frames are left unchanged (seq stays 0).
func (sa *SeqAssigner) Assign(frame *Frame) {
	if !frame.IsFlowFrame() {
		return
	}
	key := FlowKeyFromFrame(frame)
	counter := sa.counters[key]
	frame.Seq = counter
	sa.counters[key] = counter + 1
}

// Remove removes tracking for a flow (call after END/ERR delivery).
func (sa *SeqAssigner) Remove(key FlowKey) {
	delete(sa.counters, key)
}

// =============================================================================
// REORDER BUFFER — Per-flow frame reordering at relay boundaries
// =============================================================================

// flowState holds per-flow state for the reorder buffer.
type flowState struct {
	expectedSeq uint64
	buffer      map[uint64]*Frame
}

// ReorderBuffer validates and reorders frames at relay boundaries.
// Keyed by FlowKey (RID + optional XID). Each flow tracks expected seq
// and buffers out-of-order frames until gaps are filled.
//
// Protocol errors:
// - Stale/duplicate seq (frame.seq < expected_seq)
// - Buffer overflow (buffered frames exceed MaxBufferPerFlow)
//
// (matches Rust ReorderBuffer)
type ReorderBuffer struct {
	flows            map[FlowKey]*flowState
	MaxBufferPerFlow int
}

// NewReorderBuffer creates a new ReorderBuffer with the given per-flow capacity.
func NewReorderBuffer(maxBufferPerFlow int) *ReorderBuffer {
	return &ReorderBuffer{
		flows:            make(map[FlowKey]*flowState),
		MaxBufferPerFlow: maxBufferPerFlow,
	}
}

// Accept accepts a frame into the reorder buffer.
// Returns a slice of frames ready for delivery (in seq order).
// Non-flow frames bypass reordering and are returned immediately.
// Returns error for stale/duplicate seq or buffer overflow.
func (rb *ReorderBuffer) Accept(frame *Frame) ([]*Frame, error) {
	if !frame.IsFlowFrame() {
		return []*Frame{frame}, nil
	}

	key := FlowKeyFromFrame(frame)
	state, exists := rb.flows[key]
	if !exists {
		state = &flowState{
			expectedSeq: 0,
			buffer:      make(map[uint64]*Frame),
		}
		rb.flows[key] = state
	}

	if frame.Seq == state.expectedSeq {
		// In-order: deliver this frame + drain consecutive buffered frames
		ready := []*Frame{frame}
		state.expectedSeq++
		for {
			buffered, ok := state.buffer[state.expectedSeq]
			if !ok {
				break
			}
			ready = append(ready, buffered)
			delete(state.buffer, state.expectedSeq)
			state.expectedSeq++
		}
		return ready, nil
	} else if frame.Seq > state.expectedSeq {
		// Out-of-order: buffer it
		if _, dup := state.buffer[frame.Seq]; dup {
			return nil, fmt.Errorf(
				"stale/duplicate seq: seq %d already buffered (expected >= %d)",
				frame.Seq, state.expectedSeq,
			)
		}
		if len(state.buffer) >= rb.MaxBufferPerFlow {
			return nil, fmt.Errorf(
				"reorder buffer overflow: flow has %d buffered frames (max %d), "+
					"expected seq %d but got seq %d",
				len(state.buffer), rb.MaxBufferPerFlow,
				state.expectedSeq, frame.Seq,
			)
		}
		state.buffer[frame.Seq] = frame
		return []*Frame{}, nil
	} else {
		// Stale or duplicate
		return nil, fmt.Errorf(
			"stale/duplicate seq: expected >= %d but got %d",
			state.expectedSeq, frame.Seq,
		)
	}
}

// CleanupFlow removes flow state after terminal frame delivery (END/ERR).
func (rb *ReorderBuffer) CleanupFlow(key FlowKey) {
	delete(rb.flows, key)
}

// =============================================================================
// DROP REASON — Why a frame was dropped instead of delivered (L8 observability)
// =============================================================================

// DropReason is why a frame was dropped instead of delivered. The shared
// vocabulary for counted drops across every runtime (cartridge writer, host,
// relay switch, executor); every dropped frame increments exactly one of
// these counters, observable via the protocol stats snapshots. Frames are
// never dropped silently. (matches Rust DropReason)
type DropReason uint8

const (
	// DropReasonPostTerminal: flow frame enqueued/received after the request's terminal (END/ERR) frame.
	DropReasonPostTerminal DropReason = iota
	// DropReasonNoRoute: flow frame for a request with no routing state (already released or never registered).
	DropReasonNoRoute
	// DropReasonChannelClosed: send attempted on a closed channel (receiver gone).
	DropReasonChannelClosed
	// DropReasonCreditViolation: CHUNK received beyond the granted credit window.
	DropReasonCreditViolation
	// DropReasonCancelled: frame discarded because its request was cancelled.
	DropReasonCancelled
	// DropReasonMasterDied: frame discarded because the owning master/host connection died.
	DropReasonMasterDied
)

// DropReasonAll is all variants, for counter arrays and snapshot serialization
// (matches Rust DropReason::ALL).
var DropReasonAll = []DropReason{
	DropReasonPostTerminal,
	DropReasonNoRoute,
	DropReasonChannelClosed,
	DropReasonCreditViolation,
	DropReasonCancelled,
	DropReasonMasterDied,
}

// AsStr returns the stable snake_case name (the wire/snapshot contract for
// mirrors). (matches Rust DropReason::as_str)
func (r DropReason) AsStr() string {
	switch r {
	case DropReasonPostTerminal:
		return "post_terminal"
	case DropReasonNoRoute:
		return "no_route"
	case DropReasonChannelClosed:
		return "channel_closed"
	case DropReasonCreditViolation:
		return "credit_violation"
	case DropReasonCancelled:
		return "cancelled"
	case DropReasonMasterDied:
		return "master_died"
	default:
		panic(fmt.Sprintf("BUG: DropReason %d not covered by AsStr", uint8(r)))
	}
}
