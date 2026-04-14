package bifaci

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// Protocol version. Version 2: Result-based emitters, negotiated chunk limits, per-request errors.
const ProtocolVersion uint8 = 2

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
	default:
		return fmt.Sprintf("UNKNOWN(%d)", ft)
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

// NewMessageIdDefault creates a default MessageId (uint 0)
func NewMessageIdDefault() MessageId {
	zero := uint64(0)
	return MessageId{uintValue: &zero}
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
	Version     uint8                  // Protocol version (always 2)
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

// NewErr creates an ERR frame (matches Rust Frame::err)
// code and message are stored in the Meta map
func NewErr(id MessageId, code string, message string) *Frame {
	frame := newFrame(FrameTypeErr, id)
	frame.Meta = map[string]interface{}{
		"code":    code,
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

// NewHello creates a HELLO frame for handshake (host side - no manifest)
// Matches Rust Frame::hello
func NewHello(maxFrame, maxChunk, maxReorderBuffer int) *Frame {
	frame := newFrame(FrameTypeHello, MessageId{uintValue: new(uint64)})
	frame.Meta = map[string]interface{}{
		"max_frame":          maxFrame,
		"max_chunk":          maxChunk,
		"max_reorder_buffer": maxReorderBuffer,
		"version":            ProtocolVersion,
	}
	return frame
}

// NewHelloWithManifest creates a HELLO frame with manifest (cartridge side)
// Matches Rust Frame::hello_with_manifest
func NewHelloWithManifest(maxFrame, maxChunk, maxReorderBuffer int, manifest []byte) *Frame {
	frame := newFrame(FrameTypeHello, MessageId{uintValue: new(uint64)})
	frame.Meta = map[string]interface{}{
		"max_frame":          maxFrame,
		"max_chunk":          maxChunk,
		"max_reorder_buffer": maxReorderBuffer,
		"version":            ProtocolVersion,
		"manifest":           manifest,
	}
	return frame
}

// NewRelayNotify creates a RELAY_NOTIFY frame for capability advertisement (slave → master).
// Carries aggregate manifest + negotiated limits. (matches Rust Frame::relay_notify)
func NewRelayNotify(manifest []byte, maxFrame, maxChunk, maxReorderBuffer int) *Frame {
	frame := newFrame(FrameTypeRelayNotify, MessageId{uintValue: new(uint64)})
	frame.Meta = map[string]interface{}{
		"manifest":           manifest,
		"max_frame":          maxFrame,
		"max_chunk":          maxChunk,
		"max_reorder_buffer": maxReorderBuffer,
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
	return &Limits{MaxFrame: maxFrame, MaxChunk: maxChunk, MaxReorderBuffer: maxReorderBuffer}
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
// Non-flow frames (Hello, Heartbeat, RelayNotify, RelayState, Cancel) bypass seq assignment
// and reorder buffers entirely. (matches Rust Frame::is_flow_frame)
func (f *Frame) IsFlowFrame() bool {
	switch f.FrameType {
	case FrameTypeHello, FrameTypeHeartbeat, FrameTypeRelayNotify, FrameTypeRelayState, FrameTypeCancel:
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
	flows             map[FlowKey]*flowState
	MaxBufferPerFlow  int
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
