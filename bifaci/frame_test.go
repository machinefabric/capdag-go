package bifaci

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST171: Test all FrameType discriminants roundtrip through u8 conversion preserving identity
func Test171_frame_type_roundtrip(t *testing.T) {
	types := []FrameType{
		FrameTypeReq,
		// Res REMOVED - old protocol no longer supported
		FrameTypeChunk,
		FrameTypeEnd,
		FrameTypeErr,
		FrameTypeLog,
		FrameTypeHeartbeat,
		FrameTypeHello,
		FrameTypeStreamStart,
		FrameTypeStreamEnd,
		FrameTypeCancel,
		FrameTypeCredit,
	}

	for _, ft := range types {
		asUint := uint8(ft)
		backToType := FrameType(asUint)
		if backToType != ft {
			t.Errorf("FrameType %v roundtrip failed: got %v", ft, backToType)
		}
	}
}

// TEST172: Test FrameType::from_u8 returns None for values outside the valid discriminant range
func Test172_frame_type_valid_range(t *testing.T) {
	validTypes := map[uint8]bool{
		0:  true,  // HELLO
		1:  true,  // REQ
		2:  false, // RES REMOVED - old protocol no longer supported
		3:  true,  // CHUNK
		4:  true,  // END
		5:  true,  // LOG
		6:  true,  // ERR
		7:  true,  // HEARTBEAT
		8:  true,  // STREAM_START
		9:  true,  // STREAM_END
		10: true,  // RELAY_NOTIFY
		11: true,  // RELAY_STATE
		12: true,  // CANCEL
		13: true,  // CREDIT (protocol v3)
	}

	for i := uint8(0); i <= 13; i++ {
		if expected, exists := validTypes[i]; exists && expected {
			ft := FrameType(i)
			if ft.String() == fmt.Sprintf("UNKNOWN(%d)", i) {
				t.Errorf("Expected %d to be a valid FrameType", i)
			}
		}
	}
	// 14 is one past Credit — must be invalid
	ft14 := FrameType(14)
	if ft14.String() != "UNKNOWN(14)" {
		t.Errorf("Expected 14 to be invalid, got %s", ft14.String())
	}
	if FrameType(100).String() != "UNKNOWN(100)" {
		t.Errorf("Expected 100 to be invalid")
	}
	if FrameType(255).String() != "UNKNOWN(255)" {
		t.Errorf("Expected 255 to be invalid")
	}
}

// TEST173: Test FrameType discriminant values match the wire protocol specification exactly
func Test173_frame_type_wire_protocol_values(t *testing.T) {
	if uint8(FrameTypeHello) != 0 {
		t.Errorf("HELLO must be 0, got %d", FrameTypeHello)
	}
	if uint8(FrameTypeReq) != 1 {
		t.Errorf("REQ must be 1, got %d", FrameTypeReq)
	}
	// Res = 2 REMOVED - old protocol no longer supported
	if uint8(FrameTypeChunk) != 3 {
		t.Errorf("CHUNK must be 3, got %d", FrameTypeChunk)
	}
	if uint8(FrameTypeEnd) != 4 {
		t.Errorf("END must be 4, got %d", FrameTypeEnd)
	}
	if uint8(FrameTypeLog) != 5 {
		t.Errorf("LOG must be 5, got %d", FrameTypeLog)
	}
	if uint8(FrameTypeErr) != 6 {
		t.Errorf("ERR must be 6, got %d", FrameTypeErr)
	}
	if uint8(FrameTypeHeartbeat) != 7 {
		t.Errorf("HEARTBEAT must be 7, got %d", FrameTypeHeartbeat)
	}
	if uint8(FrameTypeStreamStart) != 8 {
		t.Errorf("STREAM_START must be 8, got %d", FrameTypeStreamStart)
	}
	if uint8(FrameTypeStreamEnd) != 9 {
		t.Errorf("STREAM_END must be 9, got %d", FrameTypeStreamEnd)
	}
	if uint8(FrameTypeCancel) != 12 {
		t.Errorf("CANCEL must be 12, got %d", FrameTypeCancel)
	}
}

// TEST174: Test MessageId::new_uuid generates valid UUID that roundtrips through string conversion
func Test174_message_id_new_uuid_roundtrip(t *testing.T) {
	id := NewMessageIdRandom()
	if !id.IsUuid() {
		t.Fatal("Expected UUID variant")
	}

	uuidStr := id.ToUuidString()
	if uuidStr == "" {
		t.Fatal("Expected non-empty UUID string")
	}

	recovered, err := NewMessageIdFromUuidString(uuidStr)
	if err != nil {
		t.Fatalf("expected UUID string to parse: %v", err)
	}
	if !id.Equals(recovered) {
		t.Fatalf("roundtrip mismatch: original=%s recovered=%s", id.ToString(), recovered.ToString())
	}
}

// TEST175: Test two MessageId::new_uuid calls produce distinct IDs (no collisions)
func Test175_message_id_uuid_distinct(t *testing.T) {
	id1 := NewMessageIdRandom()
	id2 := NewMessageIdRandom()

	if id1.Equals(id2) {
		t.Error("Two random UUIDs should not be equal")
	}
}

// TEST176: Test MessageId::Uint does not produce a UUID string, to_uuid_string returns None
func Test176_message_id_uint_no_uuid_string(t *testing.T) {
	id := NewMessageIdFromUint(42)
	if id.IsUuid() {
		t.Fatal("Expected Uint variant, got UUID")
	}

	uuidStr := id.ToUuidString()
	if uuidStr != "" {
		t.Errorf("Uint variant should not produce UUID string, got %s", uuidStr)
	}
}

// TEST177: Test MessageId::from_uuid_str rejects invalid UUID strings
func Test177_message_id_from_invalid_uuid_str(t *testing.T) {
	invalid := []string{"not-a-uuid", "", "12345678"}
	for _, value := range invalid {
		if _, err := NewMessageIdFromUuidString(value); err == nil {
			t.Fatalf("expected invalid UUID string %q to fail", value)
		}
	}
}

// TEST178: Test MessageId::as_bytes produces correct byte representations for Uuid and Uint variants
func Test178_message_id_as_bytes(t *testing.T) {
	// UUID variant
	uuidBytes := make([]byte, 16)
	for i := 0; i < 16; i++ {
		uuidBytes[i] = byte(i)
	}
	id1, _ := NewMessageIdFromUuid(uuidBytes)
	bytes1 := id1.AsBytes()
	if len(bytes1) != 16 {
		t.Errorf("UUID bytes should be 16, got %d", len(bytes1))
	}

	// Uint variant
	id2 := NewMessageIdFromUint(42)
	bytes2 := id2.AsBytes()
	if len(bytes2) != 8 {
		t.Errorf("Uint bytes should be 8, got %d", len(bytes2))
	}
}

// TEST179: Test MessageId::default creates a UUID variant (not Uint)
func Test179_message_id_default(t *testing.T) {
	id := NewMessageIdDefault()
	if !id.IsUuid() {
		t.Fatal("default MessageId must be UUID")
	}
	if id.ToUuidString() == "" {
		t.Fatal("default UUID MessageId must render to UUID string")
	}
}

// TEST180: Test Frame::hello without manifest produces correct HELLO frame for host side
func Test180_frame_hello_without_manifest(t *testing.T) {
	frame := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)
	if frame.FrameType != FrameTypeHello {
		t.Errorf("Expected HELLO frame type, got %v", frame.FrameType)
	}
	// Host-side HELLO has limits in Meta, no manifest in payload
	if frame.Meta == nil {
		t.Error("Expected Meta map with limits")
	}
	if frame.Meta["max_frame"] == nil {
		t.Error("Expected max_frame in Meta")
	}
}

// TEST181: Test Frame::hello_with_manifest produces HELLO with manifest bytes for cartridge side
func Test181_frame_hello_with_manifest(t *testing.T) {
	manifest := []byte(`{"name":"test"}`)
	frame := NewHelloWithManifest(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit, manifest)
	if frame.FrameType != FrameTypeHello {
		t.Errorf("Expected HELLO frame type, got %v", frame.FrameType)
	}
	// Cartridge-side HELLO has limits AND manifest in Meta
	if frame.Meta == nil {
		t.Error("Expected Meta map")
	}
	if manifestBytes, ok := frame.Meta["manifest"].([]byte); !ok || string(manifestBytes) != string(manifest) {
		t.Errorf("Expected manifest in Meta, got %v", frame.Meta["manifest"])
	}
}

// TEST182: Test Frame::req stores cap URN, payload, and content_type correctly
func Test182_frame_req(t *testing.T) {
	id := NewMessageIdRandom()
	cap := `cap:in="media:void";test;out="media:void"`
	payload := []byte("request data")
	contentType := "application/json"

	frame := NewReq(id, cap, payload, contentType)

	if frame.FrameType != FrameTypeReq {
		t.Errorf("Expected REQ frame type, got %v", frame.FrameType)
	}
	if frame.Cap == nil || *frame.Cap != cap {
		t.Errorf("Expected cap %s, got %v", cap, frame.Cap)
	}
	if string(frame.Payload) != string(payload) {
		t.Error("Payload mismatch")
	}
	if frame.ContentType == nil || *frame.ContentType != contentType {
		t.Errorf("Expected content_type %s, got %v", contentType, frame.ContentType)
	}
}

// TEST183: REMOVED - RES frame no longer supported in protocol v2

// TEST184: Test Frame::chunk stores seq and payload for streaming (with stream_id)
func Test184_frame_chunk(t *testing.T) {
	id := NewMessageIdRandom()
	streamId := "stream-123"
	seq := uint64(5)
	payload := []byte("chunk data")
	chunkIndex := uint64(0)
	checksum := ComputeChecksum(payload)

	frame := NewChunk(id, streamId, seq, payload, chunkIndex, checksum)

	if frame.FrameType != FrameTypeChunk {
		t.Errorf("Expected CHUNK frame type, got %v", frame.FrameType)
	}
	if frame.StreamId == nil || *frame.StreamId != streamId {
		t.Errorf("Expected streamId %s, got %v", streamId, frame.StreamId)
	}
	if frame.Seq != seq {
		t.Errorf("Expected seq %d, got %d", seq, frame.Seq)
	}
	if string(frame.Payload) != string(payload) {
		t.Error("Payload mismatch")
	}
}

// TEST185: Test Frame::err stores error code and message in metadata
func Test185_frame_err(t *testing.T) {
	id := NewMessageIdRandom()
	code := "HANDLER_ERROR"
	message := "Something went wrong"

	frame := NewErr(id, code, message)

	if frame.FrameType != FrameTypeErr {
		t.Errorf("Expected ERR frame type, got %v", frame.FrameType)
	}
	if frame.ErrorCode() != code {
		t.Errorf("Expected code %s, got %s", code, frame.ErrorCode())
	}
	if frame.ErrorMessage() != message {
		t.Errorf("Expected message %s, got %s", message, frame.ErrorMessage())
	}
}

// TEST186: Test Frame::log stores level and message in metadata
func Test186_frame_log(t *testing.T) {
	id := NewMessageIdRandom()
	level := "info"
	message := "Log message"

	frame := NewLog(id, level, message)

	if frame.FrameType != FrameTypeLog {
		t.Errorf("Expected LOG frame type, got %v", frame.FrameType)
	}
	if frame.LogLevel() != level {
		t.Errorf("Expected level %s, got %s", level, frame.LogLevel())
	}
	if frame.LogMessage() != message {
		t.Errorf("Expected message %s, got %s", message, frame.LogMessage())
	}
}

// TEST187: Test Frame::end with payload sets eof and optional final payload
func Test187_frame_end_with_payload(t *testing.T) {
	id := NewMessageIdRandom()
	payload := []byte("final data")

	frame := NewEnd(id, payload)

	if frame.FrameType != FrameTypeEnd {
		t.Errorf("Expected END frame type, got %v", frame.FrameType)
	}
	if string(frame.Payload) != string(payload) {
		t.Error("Payload mismatch")
	}
	if !frame.IsEof() {
		t.Error("Expected eof to be true")
	}
}

// TEST188: Test Frame::end without payload still sets eof marker
func Test188_frame_end_without_payload(t *testing.T) {
	id := NewMessageIdRandom()
	frame := NewEnd(id, []byte{})

	if frame.FrameType != FrameTypeEnd {
		t.Errorf("Expected END frame type, got %v", frame.FrameType)
	}
	if len(frame.Payload) != 0 {
		t.Error("Expected empty payload")
	}
	if !frame.IsEof() {
		t.Error("Expected eof to be true")
	}
}

// TEST189: Test chunk_with_offset sets offset on all chunks but len only on seq=0 (with stream_id)
func Test189_frame_chunk_with_offset(t *testing.T) {
	id := NewMessageIdRandom()
	streamId := "stream-456"

	payload1 := []byte("data")
	checksum1 := ComputeChecksum(payload1)
	totalLen := uint64(1000)
	first := NewChunkWithOffset(id, streamId, 0, payload1, 0, &totalLen, false, 0, checksum1)
	if first.Seq != 0 {
		t.Fatalf("expected first chunk seq 0, got %d", first.Seq)
	}
	if first.Offset == nil || *first.Offset != 0 {
		t.Fatalf("expected first chunk offset 0, got %v", first.Offset)
	}
	if first.Len == nil || *first.Len != totalLen {
		t.Fatalf("expected first chunk len %d, got %v", totalLen, first.Len)
	}
	if first.IsEof() {
		t.Fatal("first chunk must not be EOF")
	}

	payload2 := []byte("mid")
	checksum2 := ComputeChecksum(payload2)
	midTotalLen := uint64(9999)
	mid := NewChunkWithOffset(id, streamId, 3, payload2, 500, &midTotalLen, false, 3, checksum2)
	if mid.Offset == nil || *mid.Offset != 500 {
		t.Fatalf("expected mid chunk offset 500, got %v", mid.Offset)
	}
	if mid.Len != nil {
		t.Fatalf("non-first chunk must not carry len, got %v", *mid.Len)
	}
	if mid.IsEof() {
		t.Fatal("mid chunk must not be EOF")
	}

	payload3 := []byte("last")
	checksum3 := ComputeChecksum(payload3)
	last := NewChunkWithOffset(id, streamId, 5, payload3, 900, nil, true, 5, checksum3)
	if last.Offset == nil || *last.Offset != 900 {
		t.Fatalf("expected last chunk offset 900, got %v", last.Offset)
	}
	if last.Len != nil {
		t.Fatalf("last non-first chunk must not carry len, got %v", *last.Len)
	}
	if !last.IsEof() {
		t.Fatal("last chunk must be EOF")
	}
}

// TEST190: Test Frame::heartbeat creates minimal frame with no payload or metadata
func Test190_frame_heartbeat(t *testing.T) {
	id := NewMessageIdRandom()
	frame := NewHeartbeat(id)

	if frame.FrameType != FrameTypeHeartbeat {
		t.Errorf("Expected HEARTBEAT frame type, got %v", frame.FrameType)
	}
	if len(frame.Payload) != 0 {
		t.Error("HEARTBEAT should have empty payload")
	}
	if frame.Cap != nil {
		t.Error("HEARTBEAT should have no cap")
	}
}

// TEST191: Test error_code and error_message return None for non-Err frame types
func Test191_error_accessors_on_non_err_frame(t *testing.T) {
	req := NewReq(NewMessageIdRandom(), "cap:test", []byte{}, "text/plain")
	if req.ErrorCode() != "" {
		t.Error("REQ must have no error_code")
	}
	if req.ErrorMessage() != "" {
		t.Error("REQ must have no error_message")
	}

	hello := NewHello(1000, 500, DefaultMaxReorderBuffer, DefaultInitialCredit)
	if hello.ErrorCode() != "" {
		t.Error("HELLO must have no error_code")
	}
}

// TEST192: Test log_level and log_message return None for non-Log frame types
func Test192_log_accessors_on_non_log_frame(t *testing.T) {
	req := NewReq(NewMessageIdRandom(), "cap:test", []byte{}, "text/plain")
	if req.LogLevel() != "" {
		t.Error("REQ must have no log_level")
	}
	if req.LogMessage() != "" {
		t.Error("REQ must have no log_message")
	}
}

// TEST193: Test hello_max_frame and hello_max_chunk return None for non-Hello frame types
func Test193_hello_accessors_on_non_hello_frame(t *testing.T) {
	err := NewErr(NewMessageIdRandom(), "E", "m")
	// ERR frames have no Meta with hello limits
	if err.Meta != nil {
		if _, hasMaxFrame := err.Meta["max_frame"]; hasMaxFrame {
			t.Error("ERR frame should not have max_frame in meta")
		}
	}
}

// TEST194: Test Frame::new sets version and defaults correctly, optional fields are None
func Test194_frame_new_defaults(t *testing.T) {
	id := NewMessageIdRandom()
	frame := newFrame(FrameTypeChunk, id)

	if frame.Version != ProtocolVersion {
		t.Errorf("Expected version %d, got %d", ProtocolVersion, frame.Version)
	}
	if frame.FrameType != FrameTypeChunk {
		t.Error("Frame type mismatch")
	}
	if !frame.Id.Equals(id) {
		t.Error("ID mismatch")
	}
	if frame.Seq != 0 {
		t.Error("Seq should be 0")
	}
	if frame.ContentType != nil {
		t.Error("ContentType should be nil")
	}
	if frame.Meta != nil {
		t.Error("Meta should be nil")
	}
	if frame.Payload != nil {
		t.Error("Payload should be nil")
	}
	if frame.Len != nil {
		t.Error("Len should be nil")
	}
	if frame.Offset != nil {
		t.Error("Offset should be nil")
	}
	if frame.Eof != nil {
		t.Error("Eof should be nil")
	}
	if frame.Cap != nil {
		t.Error("cap.Cap should be nil")
	}
}

// TEST195: Test Frame::default creates a Req frame (the documented default)
func Test195_frame_default_type(t *testing.T) {
	frame := DefaultFrame()
	if frame.FrameType != FrameTypeReq {
		t.Error("Expected REQ frame type")
	}
	if frame.Version != ProtocolVersion {
		t.Errorf("Expected version %d", ProtocolVersion)
	}
}

// TEST196: Test is_eof returns false when eof field is None (unset)
func Test196_is_eof_when_none(t *testing.T) {
	frame := newFrame(FrameTypeChunk, MessageId{uintValue: new(uint64)})
	if frame.IsEof() {
		t.Error("eof=nil must mean not EOF")
	}
}

// TEST197: Test is_eof returns false when eof field is explicitly Some(false)
func Test197_is_eof_when_false(t *testing.T) {
	frame := newFrame(FrameTypeChunk, MessageId{uintValue: new(uint64)})
	falseVal := false
	frame.Eof = &falseVal
	if frame.IsEof() {
		t.Error("eof=false must mean not EOF")
	}
}

// TEST198: Test Limits::default provides the documented default values
func Test198_limits_default(t *testing.T) {
	limits := DefaultLimits()
	if limits.MaxFrame != DefaultMaxFrame {
		t.Errorf("Expected max_frame %d, got %d", DefaultMaxFrame, limits.MaxFrame)
	}
	if limits.MaxChunk != DefaultMaxChunk {
		t.Errorf("Expected max_chunk %d, got %d", DefaultMaxChunk, limits.MaxChunk)
	}
	// Verify actual values match Rust constants
	if limits.MaxFrame != 3_670_016 {
		t.Error("default max_frame should be 3.5 MB")
	}
	if limits.MaxChunk != 262_144 {
		t.Error("default max_chunk should be 256 KB")
	}
	if limits.InitialCredit != 32 {
		t.Errorf("default initial_credit should be 32 chunks, got %d", limits.InitialCredit)
	}
}

// TEST199: Test PROTOCOL_VERSION is 3
func Test199_protocol_version_constant(t *testing.T) {
	if ProtocolVersion != 3 {
		t.Errorf("PROTOCOL_VERSION must be 3, got %d", ProtocolVersion)
	}
}

// TEST200: Test integer key constants match the protocol specification
func Test200_key_constants(t *testing.T) {
	if keyVersion != 0 {
		t.Errorf("keyVersion must be 0, got %d", keyVersion)
	}
	if keyFrameType != 1 {
		t.Errorf("keyFrameType must be 1, got %d", keyFrameType)
	}
	if keyId != 2 {
		t.Errorf("keyId must be 2, got %d", keyId)
	}
	if keySeq != 3 {
		t.Errorf("keySeq must be 3, got %d", keySeq)
	}
	if keyContentType != 4 {
		t.Errorf("keyContentType must be 4, got %d", keyContentType)
	}
	if keyMeta != 5 {
		t.Errorf("keyMeta must be 5, got %d", keyMeta)
	}
	if keyPayload != 6 {
		t.Errorf("keyPayload must be 6, got %d", keyPayload)
	}
	if keyLen != 7 {
		t.Errorf("keyLen must be 7, got %d", keyLen)
	}
	if keyOffset != 8 {
		t.Errorf("keyOffset must be 8, got %d", keyOffset)
	}
	if keyEof != 9 {
		t.Errorf("keyEof must be 9, got %d", keyEof)
	}
	if keyCap != 10 {
		t.Errorf("keyCap must be 10, got %d", keyCap)
	}
}

// TEST201: Test hello_with_manifest preserves binary manifest data (not just JSON text)
func Test201_hello_manifest_binary_data(t *testing.T) {
	binaryManifest := []byte{0x00, 0x01, 0xFF, 0xFE, 0x80}
	frame := NewHelloWithManifest(1000, 500, DefaultMaxReorderBuffer, DefaultInitialCredit, binaryManifest)

	// Extract manifest from meta
	if frame.Meta == nil {
		t.Fatal("Meta should not be nil")
	}
	manifestVal, ok := frame.Meta["manifest"]
	if !ok {
		t.Fatal("Meta should contain manifest key")
	}
	manifestBytes, ok := manifestVal.([]byte)
	if !ok {
		t.Fatal("Manifest should be bytes")
	}
	if string(manifestBytes) != string(binaryManifest) {
		t.Error("Binary manifest data not preserved")
	}
}

// TEST202: Test MessageId Eq/Hash semantics: equal UUIDs are equal, different ones are not
func Test202_message_id_equality_and_hash(t *testing.T) {
	id1 := MessageId{uuidBytes: []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}}
	id2 := MessageId{uuidBytes: []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}}
	id3 := MessageId{uuidBytes: []byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}}

	if !id1.Equals(id2) {
		t.Error("Equal UUID IDs should be equal")
	}
	if id1.Equals(id3) {
		t.Error("Different UUID IDs should not be equal")
	}

	val1 := uint64(42)
	val2 := uint64(42)
	val3 := uint64(43)
	uint1 := MessageId{uintValue: &val1}
	uint2 := MessageId{uintValue: &val2}
	uint3 := MessageId{uintValue: &val3}

	if !uint1.Equals(uint2) {
		t.Error("Equal Uint IDs should be equal")
	}
	if uint1.Equals(uint3) {
		t.Error("Different Uint IDs should not be equal")
	}
}

// TEST203: Test Uuid and Uint variants of MessageId are never equal even for coincidental byte values
func Test203_message_id_cross_variant_inequality(t *testing.T) {
	uuidId := MessageId{uuidBytes: make([]byte, 16)} // all zeros
	zero := uint64(0)
	uintId := MessageId{uintValue: &zero}

	if uuidId.Equals(uintId) {
		t.Error("Different variants must not be equal")
	}
}

// TEST204: Test Frame::req with empty payload stores Some(empty vec) not None
func Test204_req_frame_empty_payload(t *testing.T) {
	frame := NewReq(NewMessageIdRandom(), "cap:test", []byte{}, "text/plain")
	if frame.Payload == nil {
		t.Error("Empty payload should be empty slice, not nil")
	}
	if len(frame.Payload) != 0 {
		t.Error("Empty payload should have length 0")
	}
}

// TEST365: Frame::stream_start stores request_id, stream_id, and media_urn
func Test365_stream_start_frame(t *testing.T) {
	reqId := NewMessageIdRandom()
	streamId := "stream-abc-123"
	mediaUrn := "media:"

	frame := NewStreamStart(reqId, streamId, mediaUrn, nil)

	if frame.FrameType != FrameTypeStreamStart {
		t.Errorf("Expected STREAM_START frame type, got %v", frame.FrameType)
	}
	if frame.StreamId == nil || *frame.StreamId != streamId {
		t.Errorf("Expected streamId %s, got %v", streamId, frame.StreamId)
	}
	if frame.MediaUrn == nil || *frame.MediaUrn != mediaUrn {
		t.Errorf("Expected mediaUrn %s, got %v", mediaUrn, frame.MediaUrn)
	}
	if !frame.Id.Equals(reqId) {
		t.Error("Request ID mismatch")
	}
}

// TEST366: Frame::stream_end stores request_id and stream_id
func Test366_stream_end_frame(t *testing.T) {
	reqId := NewMessageIdRandom()
	streamId := "stream-xyz-456"

	frame := NewStreamEnd(reqId, streamId, 0)

	if frame.FrameType != FrameTypeStreamEnd {
		t.Errorf("Expected STREAM_END frame type, got %v", frame.FrameType)
	}
	if frame.StreamId == nil || *frame.StreamId != streamId {
		t.Errorf("Expected streamId %s, got %v", streamId, frame.StreamId)
	}
	if frame.MediaUrn != nil {
		t.Errorf("STREAM_END should not have mediaUrn, got %v", frame.MediaUrn)
	}
	if !frame.Id.Equals(reqId) {
		t.Error("Request ID mismatch")
	}
}

// TEST367: StreamStart frame with empty stream_id still constructs (validation happens elsewhere)
func Test367_stream_start_with_empty_stream_id(t *testing.T) {
	reqId := NewMessageIdRandom()
	streamId := ""
	mediaUrn := "media:fmt=json"

	frame := NewStreamStart(reqId, streamId, mediaUrn, nil)

	if frame.FrameType != FrameTypeStreamStart {
		t.Errorf("Expected STREAM_START frame type, got %v", frame.FrameType)
	}
	if frame.StreamId == nil {
		t.Error("StreamId should not be nil, even if empty")
	}
	if frame.MediaUrn == nil || *frame.MediaUrn != mediaUrn {
		t.Errorf("Expected mediaUrn %s, got %v", mediaUrn, frame.MediaUrn)
	}
}

// TEST368: StreamStart frame with empty media_urn still constructs (validation happens elsewhere)
func Test368_stream_start_with_empty_media_urn(t *testing.T) {
	reqId := NewMessageIdRandom()
	streamId := "stream-test"
	mediaUrn := ""

	frame := NewStreamStart(reqId, streamId, mediaUrn, nil)

	if frame.FrameType != FrameTypeStreamStart {
		t.Errorf("Expected STREAM_START frame type, got %v", frame.FrameType)
	}
	if frame.StreamId == nil || *frame.StreamId != streamId {
		t.Errorf("Expected streamId %s, got %v", streamId, frame.StreamId)
	}
	if frame.MediaUrn == nil {
		t.Error("MediaUrn should not be nil, even if empty")
	}
}

// TEST399: Verify RelayNotify frame type discriminant roundtrips through u8 (value 10)
func Test399_relay_notify_discriminant_roundtrip(t *testing.T) {
	ft := FrameTypeRelayNotify
	asUint := uint8(ft)
	if asUint != 10 {
		t.Errorf("RELAY_NOTIFY must be 10, got %d", asUint)
	}
	backToType := FrameType(asUint)
	if backToType != FrameTypeRelayNotify {
		t.Errorf("FrameType(10) must be RELAY_NOTIFY, got %v", backToType)
	}
}

// TEST400: Verify RelayState frame type discriminant roundtrips through u8 (value 11)
func Test400_relay_state_discriminant_roundtrip(t *testing.T) {
	ft := FrameTypeRelayState
	asUint := uint8(ft)
	if asUint != 11 {
		t.Errorf("RELAY_STATE must be 11, got %d", asUint)
	}
	backToType := FrameType(asUint)
	if backToType != FrameTypeRelayState {
		t.Errorf("FrameType(11) must be RELAY_STATE, got %v", backToType)
	}
}

// TEST401: Verify relay_notify factory stores manifest and limits, and accessors extract them
func Test401_relay_notify_factory_and_accessors(t *testing.T) {
	manifest := []byte(`{"caps":["cap:test"]}`)
	maxFrame := 2_000_000
	maxChunk := 128_000

	frame := NewRelayNotify(manifest, maxFrame, maxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)

	if frame.FrameType != FrameTypeRelayNotify {
		t.Errorf("Expected RELAY_NOTIFY, got %v", frame.FrameType)
	}

	// Test manifest accessor
	extractedManifest := frame.RelayNotifyManifest()
	if extractedManifest == nil {
		t.Fatal("RelayNotifyManifest() returned nil")
	}
	if string(extractedManifest) != string(manifest) {
		t.Errorf("Manifest mismatch: got %s", string(extractedManifest))
	}

	// Test limits accessor
	extractedLimits := frame.RelayNotifyLimits()
	if extractedLimits == nil {
		t.Fatal("RelayNotifyLimits() returned nil")
	}
	if extractedLimits.MaxFrame != maxFrame {
		t.Errorf("MaxFrame mismatch: expected %d, got %d", maxFrame, extractedLimits.MaxFrame)
	}
	if extractedLimits.MaxChunk != maxChunk {
		t.Errorf("MaxChunk mismatch: expected %d, got %d", maxChunk, extractedLimits.MaxChunk)
	}

	// Test accessors on wrong frame type return nil
	req := NewReq(NewMessageIdRandom(), "cap:test", []byte{}, "text/plain")
	if req.RelayNotifyManifest() != nil {
		t.Error("RelayNotifyManifest on REQ must return nil")
	}
	if req.RelayNotifyLimits() != nil {
		t.Error("RelayNotifyLimits on REQ must return nil")
	}
}

// TEST402: Verify relay_state factory stores resource payload in frame payload field
func Test402_relay_state_factory_and_payload(t *testing.T) {
	resources := []byte(`{"gpu_memory":8192}`)

	frame := NewRelayState(resources)

	if frame.FrameType != FrameTypeRelayState {
		t.Errorf("Expected RELAY_STATE, got %v", frame.FrameType)
	}
	if string(frame.Payload) != string(resources) {
		t.Errorf("Payload mismatch: got %s", string(frame.Payload))
	}
}

// TEST403: Verify from_u8 returns None for values past the last valid frame type
func Test403_frame_type_one_past_cancel(t *testing.T) {
	if FrameType(12) != FrameTypeCancel {
		t.Errorf("12 must be Cancel, got %s", FrameType(12).String())
	}
	if FrameType(13) != FrameTypeCredit {
		t.Errorf("13 must be Credit (v3), got %s", FrameType(13).String())
	}
	ft := FrameType(14)
	if ft.String() != fmt.Sprintf("UNKNOWN(%d)", 14) {
		t.Errorf("FrameType(14) must be unknown (past the last valid frame type), got %s", ft.String())
	}
	ft2 := FrameType(2)
	if ft2.String() != fmt.Sprintf("UNKNOWN(%d)", 2) {
		t.Errorf("FrameType(2) (old Res) must be permanently retired, got %s", ft2.String())
	}
}

// TEST667: verify_chunk_checksum detects corrupted payload
func Test667_verify_chunk_checksum_detects_corruption(t *testing.T) {
	id := NewMessageIdRandom()
	streamId := "stream-test"
	payload := []byte("original payload data")
	checksum := ComputeChecksum(payload)

	// Create valid chunk frame
	frame := NewChunk(id, streamId, 0, payload, 0, checksum)

	// Valid frame should pass verification
	if err := VerifyChunkChecksum(frame); err != nil {
		t.Errorf("Valid frame should pass verification: %v", err)
	}

	// Corrupt the payload (simulate transmission error)
	frame.Payload = []byte("corrupted payload!!")

	// Corrupted frame should fail verification
	err := VerifyChunkChecksum(frame)
	if err == nil {
		t.Error("Corrupted frame should fail verification")
	}
	if err != nil && !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("Error should mention checksum mismatch, got: %v", err)
	}

	// Missing checksum should fail
	frame.Checksum = nil
	err = VerifyChunkChecksum(frame)
	if err == nil {
		t.Error("Frame without checksum should fail verification")
	}
	if err != nil && !strings.Contains(err.Error(), "missing") {
		t.Errorf("Error should mention missing checksum, got: %v", err)
	}
}

// TEST436: Verify FNV-1a checksum function produces consistent results
func Test436_compute_checksum(t *testing.T) {
	data := []byte("hello world")
	cs1 := ComputeChecksum(data)
	cs2 := ComputeChecksum(data)
	if cs1 != cs2 {
		t.Error("Same data must produce identical checksums")
	}
	if cs1 == 0 {
		t.Error("Checksum for non-empty data must be non-zero")
	}
	different := ComputeChecksum([]byte("different data"))
	if cs1 == different {
		t.Error("Different data must produce different checksums")
	}
}

// TEST442: SeqAssigner assigns seq 0,1,2,3 for consecutive frames with same RID
func Test442_seq_assigner_monotonic_same_rid(t *testing.T) {
	assigner := NewSeqAssigner()
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:test", nil, "")
	f1 := NewStreamStart(rid, "s1", "media:", nil)
	f2 := NewChunk(rid, "s1", 0, []byte("data"), 0, 0)
	f3 := NewEnd(rid, nil)

	assigner.Assign(f0)
	assigner.Assign(f1)
	assigner.Assign(f2)
	assigner.Assign(f3)

	if f0.Seq != 0 {
		t.Errorf("Expected seq 0, got %d", f0.Seq)
	}
	if f1.Seq != 1 {
		t.Errorf("Expected seq 1, got %d", f1.Seq)
	}
	if f2.Seq != 2 {
		t.Errorf("Expected seq 2, got %d", f2.Seq)
	}
	if f3.Seq != 3 {
		t.Errorf("Expected seq 3, got %d", f3.Seq)
	}
}

// TEST443: SeqAssigner maintains independent counters for different RIDs
func Test443_seq_assigner_independent_rids(t *testing.T) {
	assigner := NewSeqAssigner()
	ridA := NewMessageIdRandom()
	ridB := NewMessageIdRandom()

	a0 := NewReq(ridA, "cap:a", nil, "")
	a1 := NewChunk(ridA, "", 0, nil, 0, 0)
	a2 := NewEnd(ridA, nil)
	b0 := NewReq(ridB, "cap:b", nil, "")
	b1 := NewChunk(ridB, "", 0, nil, 0, 0)

	assigner.Assign(a0)
	assigner.Assign(a1)
	assigner.Assign(a2)
	assigner.Assign(b0)
	assigner.Assign(b1)

	if a0.Seq != 0 || a1.Seq != 1 || a2.Seq != 2 {
		t.Errorf("RID A seq: expected 0,1,2 got %d,%d,%d", a0.Seq, a1.Seq, a2.Seq)
	}
	if b0.Seq != 0 || b1.Seq != 1 {
		t.Errorf("RID B seq: expected 0,1 got %d,%d", b0.Seq, b1.Seq)
	}
}

// TEST444: SeqAssigner skips non-flow frames (Heartbeat, RelayNotify, RelayState, Hello)
func Test444_seq_assigner_skips_non_flow(t *testing.T) {
	assigner := NewSeqAssigner()

	hello := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)
	hb := NewHeartbeat(NewMessageIdRandom())
	rn := NewRelayNotify(nil, DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)
	rs := NewRelayState(nil)

	assigner.Assign(hello)
	assigner.Assign(hb)
	assigner.Assign(rn)
	assigner.Assign(rs)

	if hello.Seq != 0 {
		t.Errorf("Hello seq should stay 0, got %d", hello.Seq)
	}
	if hb.Seq != 0 {
		t.Errorf("Heartbeat seq should stay 0, got %d", hb.Seq)
	}
	if rn.Seq != 0 {
		t.Errorf("RelayNotify seq should stay 0, got %d", rn.Seq)
	}
	if rs.Seq != 0 {
		t.Errorf("RelayState seq should stay 0, got %d", rs.Seq)
	}
}

// TEST445: SeqAssigner.remove with FlowKey(rid, None) resets that flow; FlowKey(rid, Some(xid)) is unaffected
func Test445_seq_assigner_remove_by_flow_key(t *testing.T) {
	assigner := NewSeqAssigner()
	rid := NewMessageIdRandom()
	xid := NewMessageIdRandom()

	// Flow 1: (rid, no xid) — assign seq 0, 1
	f1a := NewReq(rid, "cap:test", nil, "")
	f1b := NewChunk(rid, "", 0, nil, 0, 0)
	assigner.Assign(f1a)
	assigner.Assign(f1b)

	// Flow 2: (rid, xid) — assign seq 0, 1
	f2a := NewReq(rid, "cap:test", nil, "")
	f2a.RoutingId = &xid
	f2b := NewChunk(rid, "", 0, nil, 0, 0)
	f2b.RoutingId = &xid
	assigner.Assign(f2a)
	assigner.Assign(f2b)

	if f1a.Seq != 0 || f1b.Seq != 1 {
		t.Errorf("Flow 1 before remove: expected 0,1 got %d,%d", f1a.Seq, f1b.Seq)
	}
	if f2a.Seq != 0 || f2b.Seq != 1 {
		t.Errorf("Flow 2 before remove: expected 0,1 got %d,%d", f2a.Seq, f2b.Seq)
	}

	// Remove flow 1 only
	assigner.Remove(FlowKey{rid: rid.ToString(), xid: ""})

	// Flow 1 restarts at 0
	f1c := NewReq(rid, "cap:test", nil, "")
	assigner.Assign(f1c)
	if f1c.Seq != 0 {
		t.Errorf("Flow 1 after remove should restart at 0, got %d", f1c.Seq)
	}

	// Flow 2 continues at 2
	f2c := NewChunk(rid, "", 0, nil, 0, 0)
	f2c.RoutingId = &xid
	assigner.Assign(f2c)
	if f2c.Seq != 2 {
		t.Errorf("Flow 2 should continue at 2, got %d", f2c.Seq)
	}
}

// TEST860: Same RID with different XIDs get independent seq counters
func Test860_seq_assigner_same_rid_different_xids_independent(t *testing.T) {
	assigner := NewSeqAssigner()
	rid := NewMessageIdRandom()
	xidA := NewMessageIdRandom()
	xidB := NewMessageIdRandom()

	// Flow (rid, xidA)
	fA0 := NewReq(rid, "cap:a", nil, "")
	fA0.RoutingId = &xidA
	fA1 := NewChunk(rid, "", 0, nil, 0, 0)
	fA1.RoutingId = &xidA

	// Flow (rid, xidB)
	fB0 := NewReq(rid, "cap:b", nil, "")
	fB0.RoutingId = &xidB

	// Flow (rid, no xid)
	fNone0 := NewReq(rid, "cap:c", nil, "")

	assigner.Assign(fA0)
	assigner.Assign(fA1)
	assigner.Assign(fB0)
	assigner.Assign(fNone0)

	if fA0.Seq != 0 || fA1.Seq != 1 {
		t.Errorf("Flow A: expected 0,1 got %d,%d", fA0.Seq, fA1.Seq)
	}
	if fB0.Seq != 0 {
		t.Errorf("Flow B: expected 0, got %d", fB0.Seq)
	}
	if fNone0.Seq != 0 {
		t.Errorf("Flow None: expected 0, got %d", fNone0.Seq)
	}
}

// TEST446: SeqAssigner handles mixed frame types (REQ, CHUNK, LOG, END) for same RID
func Test446_seq_assigner_mixed_types(t *testing.T) {
	assigner := NewSeqAssigner()
	rid := NewMessageIdRandom()

	req := NewReq(rid, "cap:test", nil, "")
	log := NewLog(rid, "progress", "test")
	chunk := NewChunk(rid, "", 0, []byte("data"), 0, 0)
	end := NewEnd(rid, nil)

	assigner.Assign(req)
	assigner.Assign(log)
	assigner.Assign(chunk)
	assigner.Assign(end)

	if req.Seq != 0 || log.Seq != 1 || chunk.Seq != 2 || end.Seq != 3 {
		t.Errorf("Mixed types: expected 0,1,2,3 got %d,%d,%d,%d",
			req.Seq, log.Seq, chunk.Seq, end.Seq)
	}
}

// TEST447: FlowKey::from_frame extracts (rid, Some(xid)) when routing_id present
func Test447_flow_key_with_xid(t *testing.T) {
	rid := NewMessageIdRandom()
	xid := NewMessageIdRandom()

	frame := NewReq(rid, "cap:test", nil, "")
	frame.RoutingId = &xid

	key := FlowKeyFromFrame(frame)
	if key.rid != rid.ToString() {
		t.Error("FlowKey RID mismatch")
	}
	if key.xid != xid.ToString() {
		t.Error("FlowKey XID mismatch")
	}
}

// TEST448: FlowKey::from_frame extracts (rid, None) when routing_id absent
func Test448_flow_key_without_xid(t *testing.T) {
	rid := NewMessageIdRandom()
	frame := NewReq(rid, "cap:test", nil, "")

	key := FlowKeyFromFrame(frame)
	if key.rid != rid.ToString() {
		t.Error("FlowKey RID mismatch")
	}
	if key.xid != "" {
		t.Errorf("FlowKey XID should be empty, got %q", key.xid)
	}
}

// TEST449: FlowKey equality: same rid+xid equal, different xid different key
func Test449_flow_key_equality(t *testing.T) {
	rid := NewMessageIdRandom()
	xidA := NewMessageIdRandom()
	xidB := NewMessageIdRandom()

	key1 := FlowKey{rid: rid.ToString(), xid: xidA.ToString()}
	key2 := FlowKey{rid: rid.ToString(), xid: xidA.ToString()}
	key3 := FlowKey{rid: rid.ToString(), xid: xidB.ToString()}
	key4 := FlowKey{rid: rid.ToString(), xid: ""}

	if key1 != key2 {
		t.Error("Same rid+xid should be equal")
	}
	if key1 == key3 {
		t.Error("Different XIDs should not be equal")
	}
	if key1 == key4 {
		t.Error("Some(xid) vs None should not be equal")
	}
}

// TEST450: FlowKey hash: same keys hash equal (HashMap lookup)
func Test450_flow_key_hash_lookup(t *testing.T) {
	rid := NewMessageIdRandom()
	xid := NewMessageIdRandom()

	key1 := FlowKey{rid: rid.ToString(), xid: xid.ToString()}
	key2 := FlowKey{rid: rid.ToString(), xid: xid.ToString()}

	m := map[FlowKey]string{key1: "value"}
	if m[key2] != "value" {
		t.Error("Identical keys should hash to same bucket")
	}
}

// TEST451: ReorderBuffer in-order delivery: seq 0,1,2 delivered immediately
func Test451_reorder_buffer_in_order(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:test", nil, "")
	f0.Seq = 0
	f1 := NewChunk(rid, "", 0, nil, 0, 0)
	f1.Seq = 1
	f2 := NewEnd(rid, nil)
	f2.Seq = 2

	r0, err := rb.Accept(f0)
	require.NoError(t, err)
	assert.Len(t, r0, 1)

	r1, err := rb.Accept(f1)
	require.NoError(t, err)
	assert.Len(t, r1, 1)

	r2, err := rb.Accept(f2)
	require.NoError(t, err)
	assert.Len(t, r2, 1)
}

// TEST452: ReorderBuffer out-of-order: seq 1 then 0 delivers both in order
func Test452_reorder_buffer_out_of_order(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:test", nil, "")
	f0.Seq = 0
	f1 := NewChunk(rid, "", 0, nil, 0, 0)
	f1.Seq = 1

	// Submit seq=1 before seq=0
	r1, err := rb.Accept(f1)
	require.NoError(t, err)
	assert.Len(t, r1, 0, "seq=1 before seq=0 should be buffered")

	// Submit seq=0 — should release both
	r0, err := rb.Accept(f0)
	require.NoError(t, err)
	assert.Len(t, r0, 2, "seq=0 should release seq=0 and seq=1")
	assert.Equal(t, uint64(0), r0[0].Seq)
	assert.Equal(t, uint64(1), r0[1].Seq)
}

// TEST453: ReorderBuffer gap fill: seq 0,2,1 delivers 0, buffers 2, then delivers 1+2
func Test453_reorder_buffer_gap_fill(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:test", nil, "")
	f0.Seq = 0
	f1 := NewChunk(rid, "", 0, nil, 0, 0)
	f1.Seq = 1
	f2 := NewEnd(rid, nil)
	f2.Seq = 2

	r0, err := rb.Accept(f0)
	require.NoError(t, err)
	assert.Len(t, r0, 1, "seq=0 delivers immediately")

	r2, err := rb.Accept(f2)
	require.NoError(t, err)
	assert.Len(t, r2, 0, "seq=2 buffered (gap at seq=1)")

	r1, err := rb.Accept(f1)
	require.NoError(t, err)
	assert.Len(t, r1, 2, "seq=1 fills gap, releases seq=1 and seq=2")
	assert.Equal(t, uint64(1), r1[0].Seq)
	assert.Equal(t, uint64(2), r1[1].Seq)
}

// TEST454: ReorderBuffer stale seq is hard error
func Test454_reorder_buffer_stale_seq(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:test", nil, "")
	f0.Seq = 0
	f1 := NewChunk(rid, "", 0, nil, 0, 0)
	f1.Seq = 1

	rb.Accept(f0)
	rb.Accept(f1)

	// Submit stale seq=0 again
	stale := NewChunk(rid, "", 0, nil, 0, 0)
	stale.Seq = 0
	_, err := rb.Accept(stale)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stale")
}

// TEST455: ReorderBuffer overflow triggers protocol error
func Test455_reorder_buffer_overflow(t *testing.T) {
	rb := NewReorderBuffer(3) // max 3 buffered per flow
	rid := NewMessageIdRandom()

	// Submit seq 1,2,3,4 (never seq 0) — 4th should overflow
	for i := uint64(1); i <= 3; i++ {
		f := NewChunk(rid, "", 0, nil, 0, 0)
		f.Seq = i
		_, err := rb.Accept(f)
		require.NoError(t, err)
	}

	f4 := NewChunk(rid, "", 0, nil, 0, 0)
	f4.Seq = 4
	_, err := rb.Accept(f4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflow")
}

// TEST456: Multiple concurrent flows reorder independently
func Test456_reorder_buffer_independent_flows(t *testing.T) {
	rb := NewReorderBuffer(10)
	ridA := NewMessageIdRandom()
	ridB := NewMessageIdRandom()

	// Flow A: submit seq=1 (out of order)
	fA1 := NewChunk(ridA, "", 0, nil, 0, 0)
	fA1.Seq = 1
	rA1, err := rb.Accept(fA1)
	require.NoError(t, err)
	assert.Len(t, rA1, 0, "A seq=1 buffered")

	// Flow B: submit seq=0 (in order) — independent of A
	fB0 := NewReq(ridB, "cap:b", nil, "")
	fB0.Seq = 0
	rB0, err := rb.Accept(fB0)
	require.NoError(t, err)
	assert.Len(t, rB0, 1, "B seq=0 delivers immediately regardless of A's gap")

	// Flow A: submit seq=0 — releases both A frames
	fA0 := NewReq(ridA, "cap:a", nil, "")
	fA0.Seq = 0
	rA0, err := rb.Accept(fA0)
	require.NoError(t, err)
	assert.Len(t, rA0, 2, "A seq=0 releases seq=0 and seq=1")
}

// TEST457: cleanup_flow removes state; new frames start at seq 0
func Test457_reorder_buffer_cleanup(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:test", nil, "")
	f0.Seq = 0
	rb.Accept(f0)

	f1 := NewChunk(rid, "", 0, nil, 0, 0)
	f1.Seq = 1
	rb.Accept(f1)

	// Cleanup the flow
	key := FlowKeyFromFrame(f0)
	rb.CleanupFlow(key)

	// Same RID can start over at seq=0 without stale error
	f0b := NewReq(rid, "cap:test", nil, "")
	f0b.Seq = 0
	r, err := rb.Accept(f0b)
	require.NoError(t, err)
	assert.Len(t, r, 1)
}

// TEST458: Non-flow frames bypass reorder entirely
func Test458_reorder_buffer_non_flow_bypass(t *testing.T) {
	rb := NewReorderBuffer(10)

	hello := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)
	hb := NewHeartbeat(NewMessageIdRandom())
	rn := NewRelayNotify(nil, DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)
	rs := NewRelayState(nil)

	for _, frame := range []*Frame{hello, hb, rn, rs} {
		r, err := rb.Accept(frame)
		require.NoError(t, err)
		assert.Len(t, r, 1, "Non-flow frame should bypass reorder buffer")
	}
}

// TEST459: Terminal END frame flows through correctly
func Test459_reorder_buffer_end_frame(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:test", nil, "")
	f0.Seq = 0
	rb.Accept(f0)

	end := NewEnd(rid, nil)
	end.Seq = 1
	r, err := rb.Accept(end)
	require.NoError(t, err)
	assert.Len(t, r, 1)
	assert.Equal(t, FrameTypeEnd, r[0].FrameType)
	assert.Equal(t, uint64(1), r[0].Seq)
}

// TEST1162: Heartbeat frames preserve self-reported memory values stored in metadata.
func Test1162_heartbeat_frame_with_memory_meta(t *testing.T) {
	id := NewMessageIdRandom()
	frame := NewHeartbeat(id)

	// Simulate cartridge attaching memory info to heartbeat response
	frame.Meta = map[string]interface{}{
		"footprint_mb": int64(4096),
		"rss_mb":       int64(5120),
	}

	assert.Equal(t, FrameTypeHeartbeat, frame.FrameType)
	assert.Equal(t, id, frame.Id)

	// Verify memory values can be extracted
	footprint, ok := frame.Meta["footprint_mb"].(int64)
	assert.True(t, ok, "footprint_mb must be int64")
	assert.Equal(t, int64(4096), footprint, "Expected footprint_mb=4096")

	rss, ok := frame.Meta["rss_mb"].(int64)
	assert.True(t, ok, "rss_mb must be int64")
	assert.Equal(t, int64(5120), rss, "Expected rss_mb=5120")
}

// makeFlowFrame creates a test flow frame with the given seq and optional routing_id (XID).
// Used for reorder buffer tests — mirrors Rust's make_flow_frame helper.
func makeFlowFrame(rid MessageId, xid *MessageId, seq uint64) *Frame {
	f := NewReq(rid, "cap:test", nil, "")
	f.Seq = seq
	if xid != nil {
		f.RoutingId = xid
	}
	return f
}

// TEST491: Frame::chunk constructor requires and sets chunk_index and checksum
func Test491_chunk_requires_chunk_index_and_checksum(t *testing.T) {
	id := NewMessageIdRandom()
	payload := []byte("test data")
	checksum := ComputeChecksum(payload)

	frame := NewChunk(id, "stream-1", 0, payload, 5, checksum)

	assert.Equal(t, FrameTypeChunk, frame.FrameType)
	require.NotNil(t, frame.ChunkIndex, "chunk_index must be set")
	assert.Equal(t, uint64(5), *frame.ChunkIndex, "chunk_index must be 5")
	require.NotNil(t, frame.Checksum, "checksum must be set")
	assert.Equal(t, checksum, *frame.Checksum, "checksum must match computed value")
	assert.Equal(t, payload, frame.Payload)
}

// TEST492: Frame::stream_end constructor requires and sets chunk_count
func Test492_stream_end_requires_chunk_count(t *testing.T) {
	id := NewMessageIdRandom()

	frame := NewStreamEnd(id, "stream-1", 42)

	assert.Equal(t, FrameTypeStreamEnd, frame.FrameType)
	require.NotNil(t, frame.ChunkCount, "chunk_count must be set")
	assert.Equal(t, uint64(42), *frame.ChunkCount, "chunk_count must be 42")
	require.NotNil(t, frame.StreamId)
	assert.Equal(t, "stream-1", *frame.StreamId)
}

// TEST493: compute_checksum produces correct FNV-1a hash for known test vectors
func Test493_compute_checksum_fnv1a_test_vectors(t *testing.T) {
	assert.Equal(t, uint64(0xcbf29ce484222325), ComputeChecksum([]byte{}), "empty string hash")
	assert.Equal(t, uint64(0xaf63dc4c8601ec8c), ComputeChecksum([]byte("a")), "single byte 'a'")
	assert.Equal(t, uint64(0x85944171f73967e8), ComputeChecksum([]byte("foobar")), "foobar string")
}

// TEST494: compute_checksum is deterministic
func Test494_compute_checksum_deterministic(t *testing.T) {
	data := []byte("test data for hashing")
	hash1 := ComputeChecksum(data)
	hash2 := ComputeChecksum(data)
	hash3 := ComputeChecksum(data)

	assert.Equal(t, hash1, hash2)
	assert.Equal(t, hash2, hash3)
}

// TEST495: CBOR decode REJECTS CHUNK frame missing chunk_index field
func Test495_cbor_rejects_chunk_without_chunk_index(t *testing.T) {
	id := NewMessageIdRandom()
	payload := []byte("data")
	checksum := ComputeChecksum(payload)

	// Build a CHUNK frame without chunk_index
	frame := &Frame{
		FrameType: FrameTypeChunk,
		Id:        id,
		Payload:   payload,
		Checksum:  &checksum,
		// ChunkIndex deliberately nil
	}
	streamId := "s1"
	frame.StreamId = &streamId

	encoded, err := EncodeFrame(frame)
	require.NoError(t, err, "encoding should succeed even without chunk_index")

	_, decErr := DecodeFrame(encoded)
	require.Error(t, decErr, "decode must reject CHUNK without chunk_index")
	assert.True(t,
		strings.Contains(decErr.Error(), "chunk_index") || strings.Contains(decErr.Error(), "CHUNK"),
		"error must mention missing chunk_index: %s", decErr.Error(),
	)
}

// TEST496: CBOR decode REJECTS CHUNK frame missing checksum field
func Test496_cbor_rejects_chunk_without_checksum(t *testing.T) {
	id := NewMessageIdRandom()
	payload := []byte("data")
	chunkIndex := uint64(0)

	// Build a CHUNK frame without checksum
	frame := &Frame{
		FrameType:  FrameTypeChunk,
		Id:         id,
		Payload:    payload,
		ChunkIndex: &chunkIndex,
		// Checksum deliberately nil
	}
	streamId := "s1"
	frame.StreamId = &streamId

	encoded, err := EncodeFrame(frame)
	require.NoError(t, err, "encoding should succeed even without checksum")

	_, decErr := DecodeFrame(encoded)
	require.Error(t, decErr, "decode must reject CHUNK without checksum")
	assert.True(t,
		strings.Contains(decErr.Error(), "checksum") || strings.Contains(decErr.Error(), "CHUNK"),
		"error must mention missing checksum: %s", decErr.Error(),
	)
}

// TEST498: routing_id field roundtrips through CBOR encoding
func Test498_routing_id_cbor_roundtrip(t *testing.T) {
	reqId := NewMessageIdRandom()
	routingId := NewMessageIdRandom()

	frame := NewReq(reqId, `cap:in="media:void";test;out="media:void"`, nil, "text/plain")
	frame.RoutingId = &routingId

	encoded, err := EncodeFrame(frame)
	require.NoError(t, err)
	decoded, err := DecodeFrame(encoded)
	require.NoError(t, err)

	require.NotNil(t, decoded.RoutingId, "routing_id must roundtrip")
	assert.Equal(t, routingId, *decoded.RoutingId)
	assert.Equal(t, reqId, decoded.Id)
}

// TEST499: chunk_index and checksum roundtrip through CBOR encoding
func Test499_chunk_index_checksum_cbor_roundtrip(t *testing.T) {
	reqId := NewMessageIdRandom()
	payload := []byte("test payload")
	checksum := ComputeChecksum(payload)

	frame := NewChunk(reqId, "s1", 0, payload, 7, checksum)

	encoded, err := EncodeFrame(frame)
	require.NoError(t, err)
	decoded, err := DecodeFrame(encoded)
	require.NoError(t, err)

	require.NotNil(t, decoded.ChunkIndex, "chunk_index must roundtrip")
	assert.Equal(t, uint64(7), *decoded.ChunkIndex)
	require.NotNil(t, decoded.Checksum, "checksum must roundtrip")
	assert.Equal(t, checksum, *decoded.Checksum)
	assert.Equal(t, payload, decoded.Payload)
}

// TEST500: chunk_count roundtrips through CBOR encoding
func Test500_chunk_count_cbor_roundtrip(t *testing.T) {
	reqId := NewMessageIdRandom()

	frame := NewStreamEnd(reqId, "s1", 42)

	encoded, err := EncodeFrame(frame)
	require.NoError(t, err)
	decoded, err := DecodeFrame(encoded)
	require.NoError(t, err)

	require.NotNil(t, decoded.ChunkCount, "chunk_count must roundtrip")
	assert.Equal(t, uint64(42), *decoded.ChunkCount)
	require.NotNil(t, decoded.StreamId)
	assert.Equal(t, "s1", *decoded.StreamId)
}

// TEST501: Frame::new initializes new fields to None
func Test501_frame_new_initializes_optional_fields_none(t *testing.T) {
	frame := NewReq(NewMessageIdRandom(), "cap:test", nil, "")

	assert.Nil(t, frame.RoutingId)
	assert.Nil(t, frame.ChunkIndex)
	assert.Nil(t, frame.ChunkCount)
	assert.Nil(t, frame.Checksum)
}

// TEST6602: Codec key constants match protocol spec values
func Test6602_codec_key_constants(t *testing.T) {
	assert.Equal(t, 13, keyRoutingId)
	assert.Equal(t, 14, keyChunkIndex)
	assert.Equal(t, 15, keyChunkCount)
	assert.Equal(t, 16, keyChecksum)
}

// TEST503: compute_checksum handles empty data correctly
func Test503_compute_checksum_empty_data(t *testing.T) {
	hash := ComputeChecksum([]byte{})
	assert.Equal(t, uint64(0xcbf29ce484222325), hash, "empty data should produce FNV offset basis")
}

// TEST504: compute_checksum handles large payloads without overflow
func Test504_compute_checksum_large_payload(t *testing.T) {
	largeData := make([]byte, 1_000_000)
	for i := range largeData {
		largeData[i] = 0xAA
	}
	hash := ComputeChecksum(largeData)
	assert.NotEqual(t, uint64(0), hash, "large payload should produce non-zero hash")

	hash2 := ComputeChecksum(largeData)
	assert.Equal(t, hash, hash2, "large payload hash must be deterministic")
}

// TEST505: chunk_with_offset sets chunk_index correctly
func Test505_chunk_with_offset_sets_chunk_index(t *testing.T) {
	reqId := NewMessageIdRandom()
	payload := []byte("data")
	checksum := ComputeChecksum(payload)
	totalLen := uint64(10000)

	frame := NewChunkWithOffset(reqId, "s1", 0, payload, 1024, &totalLen, false, 5, checksum)

	require.NotNil(t, frame.ChunkIndex, "chunk_index must be set")
	assert.Equal(t, uint64(5), *frame.ChunkIndex)
	require.NotNil(t, frame.Checksum, "checksum must be set")
	assert.Equal(t, checksum, *frame.Checksum)
	require.NotNil(t, frame.Offset)
	assert.Equal(t, uint64(1024), *frame.Offset)
}

// TEST506: Different data produces different checksums
func Test506_compute_checksum_different_data_different_hash(t *testing.T) {
	hash1 := ComputeChecksum([]byte("hello"))
	hash2 := ComputeChecksum([]byte("world"))
	assert.NotEqual(t, hash1, hash2, "different data must produce different hashes")
}

// TEST507: ReorderBuffer isolates flows by XID (routing_id) - same RID different XIDs
func Test507_reorder_buffer_xid_isolation(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()
	xidA := NewMessageIdRandom()
	xidB := NewMessageIdRandom()

	// Flow A (rid, xidA): receive seq 1 first
	readyA1, err := rb.Accept(makeFlowFrame(rid, &xidA, 1))
	require.NoError(t, err)
	assert.Empty(t, readyA1, "xidA seq 1 buffered")

	// Flow B (rid, xidB): receive seq 0 (different flow, should deliver immediately)
	readyB0, err := rb.Accept(makeFlowFrame(rid, &xidB, 0))
	require.NoError(t, err)
	assert.Len(t, readyB0, 1, "xidB seq 0 delivers immediately")
	assert.Equal(t, uint64(0), readyB0[0].Seq)

	// Flow A: receive seq 0, should deliver 0+1
	readyA0, err := rb.Accept(makeFlowFrame(rid, &xidA, 0))
	require.NoError(t, err)
	assert.Len(t, readyA0, 2, "xidA delivers 0 and buffered 1")
	assert.Equal(t, uint64(0), readyA0[0].Seq)
	assert.Equal(t, uint64(1), readyA0[1].Seq)

	// Verify both flows are independent
	readyB1, err := rb.Accept(makeFlowFrame(rid, &xidB, 1))
	require.NoError(t, err)
	assert.Len(t, readyB1, 1)
}

// TEST508: ReorderBuffer rejects duplicate seq already in buffer
func Test508_reorder_buffer_duplicate_buffered_seq(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()

	rb.Accept(makeFlowFrame(rid, nil, 1))

	result, err := rb.Accept(makeFlowFrame(rid, nil, 1))
	assert.Error(t, err, "duplicate buffered seq must fail")
	assert.Nil(t, result)
	assert.True(t,
		strings.Contains(err.Error(), "stale") || strings.Contains(err.Error(), "duplicate"),
		"error must mention stale/duplicate: %s", err.Error(),
	)
}

// TEST509: ReorderBuffer handles large seq gaps without DOS
func Test509_reorder_buffer_large_gap_rejected(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()

	rb.Accept(makeFlowFrame(rid, nil, 0))

	// Buffer up to 64 frames (leaving 1 slot — seq 1 through seq 65)
	for seq := uint64(2); seq <= 65; seq++ {
		rb.Accept(makeFlowFrame(rid, nil, seq))
	}

	// This should overflow the buffer
	result, err := rb.Accept(makeFlowFrame(rid, nil, 66))
	assert.Error(t, err, "large gap causing buffer overflow must fail")
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "overflow", "error must mention overflow")
}

// TEST510: ReorderBuffer with multiple interleaved gaps fills correctly
func Test510_reorder_buffer_multiple_gaps(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()

	ready0, err := rb.Accept(makeFlowFrame(rid, nil, 0))
	require.NoError(t, err)
	assert.Len(t, ready0, 1)

	ready3, err := rb.Accept(makeFlowFrame(rid, nil, 3))
	require.NoError(t, err)
	assert.Empty(t, ready3, "seq 3 buffered")

	ready5, err := rb.Accept(makeFlowFrame(rid, nil, 5))
	require.NoError(t, err)
	assert.Empty(t, ready5, "seq 5 buffered")

	ready1, err := rb.Accept(makeFlowFrame(rid, nil, 1))
	require.NoError(t, err)
	assert.Len(t, ready1, 1, "only seq 1 delivered, still missing 2")

	ready2, err := rb.Accept(makeFlowFrame(rid, nil, 2))
	require.NoError(t, err)
	assert.Len(t, ready2, 2, "delivers 2 and 3")
	assert.Equal(t, uint64(2), ready2[0].Seq)
	assert.Equal(t, uint64(3), ready2[1].Seq)

	ready4, err := rb.Accept(makeFlowFrame(rid, nil, 4))
	require.NoError(t, err)
	assert.Len(t, ready4, 2, "delivers 4 and 5")
	assert.Equal(t, uint64(4), ready4[0].Seq)
	assert.Equal(t, uint64(5), ready4[1].Seq)
}

// TEST511: ReorderBuffer cleanup with buffered frames discards them
func Test511_reorder_buffer_cleanup_with_buffered_frames(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()

	rb.Accept(makeFlowFrame(rid, nil, 0))
	rb.Accept(makeFlowFrame(rid, nil, 2)) // buffered
	rb.Accept(makeFlowFrame(rid, nil, 3)) // buffered

	key := FlowKey{rid: rid.ToString(), xid: ""}
	rb.CleanupFlow(key)

	// After cleanup, seq 0 should work again (flow reset)
	ready, err := rb.Accept(makeFlowFrame(rid, nil, 0))
	require.NoError(t, err)
	assert.Len(t, ready, 1)
	assert.Equal(t, uint64(0), ready[0].Seq)

	// Buffered frames 2,3 were discarded; seq 1 is now expected
	ready1, err := rb.Accept(makeFlowFrame(rid, nil, 1))
	require.NoError(t, err)
	assert.Len(t, ready1, 1)
}

// TEST512: ReorderBuffer delivers burst of consecutive buffered frames
func Test512_reorder_buffer_burst_delivery(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()

	// Buffer seq 1-10 (all waiting for seq 0)
	for seq := uint64(1); seq <= 10; seq++ {
		ready, err := rb.Accept(makeFlowFrame(rid, nil, seq))
		require.NoError(t, err)
		assert.Empty(t, ready, "seq %d buffered", seq)
	}

	// Now send seq 0 — should deliver all 11 frames at once
	ready, err := rb.Accept(makeFlowFrame(rid, nil, 0))
	require.NoError(t, err)
	assert.Len(t, ready, 11, "delivers seq 0 plus 10 buffered frames")
	for i, frame := range ready {
		assert.Equal(t, uint64(i), frame.Seq, "frame %d has correct seq", i)
	}
}

// TEST513: ReorderBuffer different frame types in same flow maintain order
func Test513_reorder_buffer_mixed_types_same_flow(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()

	req := NewReq(rid, "cap:test", nil, "")
	req.Seq = 1
	log := NewLog(rid, "info", "test log")
	log.Seq = 2
	chunk := &Frame{FrameType: FrameTypeChunk, Id: rid}
	chunk.Seq = 0

	rb.Accept(req) // buffered
	rb.Accept(log) // buffered

	ready, err := rb.Accept(chunk)
	require.NoError(t, err)
	assert.Len(t, ready, 3, "all three frames delivered in order")
	assert.Equal(t, FrameTypeChunk, ready[0].FrameType)
	assert.Equal(t, FrameTypeReq, ready[1].FrameType)
	assert.Equal(t, FrameTypeLog, ready[2].FrameType)
}

// TEST514: ReorderBuffer with XID cleanup doesn't affect different XID
func Test514_reorder_buffer_xid_cleanup_isolation(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()
	xidA := NewMessageIdRandom()
	xidB := NewMessageIdRandom()

	rb.Accept(makeFlowFrame(rid, &xidA, 0))
	rb.Accept(makeFlowFrame(rid, &xidB, 0))

	// Cleanup flow A
	keyA := FlowKey{rid: rid.ToString(), xid: xidA.ToString()}
	rb.CleanupFlow(keyA)

	// Flow B should still expect seq 1
	ready, err := rb.Accept(makeFlowFrame(rid, &xidB, 1))
	require.NoError(t, err)
	assert.Len(t, ready, 1)
	assert.Equal(t, uint64(1), ready[0].Seq)

	// Flow A was reset, seq 0 works again
	readyA, err := rb.Accept(makeFlowFrame(rid, &xidA, 0))
	require.NoError(t, err)
	assert.Len(t, readyA, 1)
}

// TEST515: ReorderBuffer overflow error includes diagnostic information
func Test515_reorder_buffer_overflow_error_details(t *testing.T) {
	maxBuffer := 3
	rb := NewReorderBuffer(maxBuffer)
	rid := NewMessageIdRandom()

	// Fill buffer to capacity
	for seq := uint64(1); seq <= 3; seq++ {
		rb.Accept(makeFlowFrame(rid, nil, seq))
	}

	_, err := rb.Accept(makeFlowFrame(rid, nil, 4))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflow", "must mention overflow")
}

// TEST516: ReorderBuffer stale error includes diagnostic information
func Test516_reorder_buffer_stale_error_details(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()

	rb.Accept(makeFlowFrame(rid, nil, 0))
	rb.Accept(makeFlowFrame(rid, nil, 1))
	rb.Accept(makeFlowFrame(rid, nil, 2))

	// Send stale seq 1
	_, err := rb.Accept(makeFlowFrame(rid, nil, 1))
	require.Error(t, err)
	assert.True(t,
		strings.Contains(err.Error(), "stale") || strings.Contains(err.Error(), "duplicate"),
		"must mention stale/duplicate: %s", err.Error(),
	)
}

// TEST517: FlowKey with None XID differs from Some(xid)
func Test517_flow_key_none_vs_some_xid(t *testing.T) {
	rid := NewMessageIdRandom()
	xid := NewMessageIdRandom()

	keyNone := FlowKey{rid: rid.ToString(), xid: ""}
	keySome := FlowKey{rid: rid.ToString(), xid: xid.ToString()}

	assert.NotEqual(t, keyNone, keySome, "empty XID must differ from non-empty XID")

	// HashMap check
	m := map[FlowKey]bool{keyNone: true}
	assert.False(t, m[keySome], "keySome must not match keyNone in map")
}

// TEST518: ReorderBuffer handles zero-length ready vec correctly
func Test518_reorder_buffer_empty_ready_vec(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()

	// Send seq 1 first — should return empty slice (buffered)
	ready, err := rb.Accept(makeFlowFrame(rid, nil, 1))
	require.NoError(t, err)
	assert.Empty(t, ready, "buffered frame returns empty slice")
	assert.Equal(t, 0, len(ready))
}

// TEST519: ReorderBuffer state persists across accept calls
func Test519_reorder_buffer_state_persistence(t *testing.T) {
	rb := NewReorderBuffer(64)
	rid := NewMessageIdRandom()

	// First call: buffer seq 2
	rb.Accept(makeFlowFrame(rid, nil, 2))

	// Second call: send seq 1, still buffered (missing 0)
	ready, err := rb.Accept(makeFlowFrame(rid, nil, 1))
	require.NoError(t, err)
	assert.Empty(t, ready, "seq 1 buffered, still waiting for seq 0")

	// Third call: send seq 0, should deliver 0, 1, 2
	ready, err = rb.Accept(makeFlowFrame(rid, nil, 0))
	require.NoError(t, err)
	assert.Len(t, ready, 3, "state persisted correctly")
}

// TEST520: ReorderBuffer max_buffer_per_flow is per-flow not global
func Test520_reorder_buffer_per_flow_limit(t *testing.T) {
	rb := NewReorderBuffer(2) // max 2 buffered per flow
	ridA := NewMessageIdRandom()
	ridB := NewMessageIdRandom()

	// Flow A: buffer 2 frames (at limit)
	rb.Accept(makeFlowFrame(ridA, nil, 1))
	rb.Accept(makeFlowFrame(ridA, nil, 2))

	// Flow B: can still buffer 2 frames (separate limit)
	rb.Accept(makeFlowFrame(ridB, nil, 1))
	rb.Accept(makeFlowFrame(ridB, nil, 2))

	// Flow A: overflow
	_, errA := rb.Accept(makeFlowFrame(ridA, nil, 3))
	assert.Error(t, errA, "flow A overflows")

	// Flow B: also overflow
	_, errB := rb.Accept(makeFlowFrame(ridB, nil, 3))
	assert.Error(t, errB, "flow B also overflows")
}

// TEST521: RelayNotify CBOR roundtrip preserves manifest and limits
func Test521_relay_notify_cbor_roundtrip(t *testing.T) {
	manifest := []byte(`{"name":"Test","version":"1.0","description":"Test","cap_groups":[{"name":"default","caps":[{"urn":"cap:in=\"media:void\";convert;out=\"media:image\"","title":"Convert","aliases":["convert"]}]}]}`)

	frame := NewRelayNotify(manifest, 3_000_000, 256_000, 128, DefaultInitialCredit)
	encoded, err := EncodeFrame(frame)
	require.NoError(t, err)
	decoded, err := DecodeFrame(encoded)
	require.NoError(t, err)

	assert.Equal(t, FrameTypeRelayNotify, decoded.FrameType)
	assert.Equal(t, manifest, decoded.RelayNotifyManifest(), "manifest must roundtrip")

	limits := decoded.RelayNotifyLimits()
	require.NotNil(t, limits)
	assert.Equal(t, 3_000_000, limits.MaxFrame)
	assert.Equal(t, 256_000, limits.MaxChunk)
	assert.Equal(t, 128, limits.MaxReorderBuffer)
	assert.Equal(t, DefaultInitialCredit, limits.InitialCredit, "initial_credit must roundtrip through RelayNotify (v3)")
}

// TEST522: RelayState CBOR roundtrip preserves payload
func Test522_relay_state_cbor_roundtrip(t *testing.T) {
	stateData := []byte(`{"memory_mb":8192,"cpu_cores":16,"active_flows":42}`)

	frame := NewRelayState(stateData)
	encoded, err := EncodeFrame(frame)
	require.NoError(t, err)
	decoded, err := DecodeFrame(encoded)
	require.NoError(t, err)

	assert.Equal(t, FrameTypeRelayState, decoded.FrameType)
	assert.Equal(t, stateData, decoded.Payload, "state payload must roundtrip exactly")
	// RelayState uses Uint(0) ID
	assert.False(t, decoded.Id.IsUuid(), "RelayState must use uint ID, not UUID")
}

// TEST523: is_flow_frame returns false for RelayNotify
func Test523_relay_notify_not_flow_frame(t *testing.T) {
	frame := NewRelayNotify([]byte("test"), DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)
	assert.False(t, frame.IsFlowFrame(), "RelayNotify must not be a flow frame")
}

// TEST524: is_flow_frame returns false for RelayState
func Test524_relay_state_not_flow_frame(t *testing.T) {
	frame := NewRelayState([]byte("test"))
	assert.False(t, frame.IsFlowFrame(), "RelayState must not be a flow frame")
}

// TEST525: RelayNotify with empty manifest is valid
func Test525_relay_notify_empty_manifest(t *testing.T) {
	frame := NewRelayNotify([]byte{}, DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)
	assert.Equal(t, FrameTypeRelayNotify, frame.FrameType)
	assert.Equal(t, []byte{}, frame.RelayNotifyManifest())
}

// TEST526: RelayState with empty payload is valid
func Test526_relay_state_empty_payload(t *testing.T) {
	frame := NewRelayState([]byte{})
	assert.Equal(t, FrameTypeRelayState, frame.FrameType)
	assert.Equal(t, []byte{}, frame.Payload)
}

// TEST527: RelayNotify with large manifest roundtrips correctly
func Test527_relay_notify_large_manifest(t *testing.T) {
	largeManifest := `{"name":"Large","version":"1.0","description":"Large test","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","aliases":["identity"]}`
	for i := 0; i < 99; i++ {
		largeManifest += fmt.Sprintf(`,{"urn":"cap:in=\"media:void\";op%d;out=\"media:void\"","title":"Op%d","aliases":["op%d"]}`, i, i, i)
	}
	largeManifest += "]}]}"

	frame := NewRelayNotify([]byte(largeManifest), DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)
	encoded, err := EncodeFrame(frame)
	require.NoError(t, err)
	decoded, err := DecodeFrame(encoded)
	require.NoError(t, err)

	assert.Equal(t, []byte(largeManifest), decoded.RelayNotifyManifest())
}

// TEST528: RelayNotify and RelayState use MessageId::Uint(0)
func Test528_relay_frames_use_uint_zero_id(t *testing.T) {
	notify := NewRelayNotify([]byte("test"), DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, DefaultInitialCredit)
	state := NewRelayState([]byte("test"))

	assert.False(t, notify.Id.IsUuid(), "RelayNotify must use uint ID, not UUID")
	assert.False(t, state.Id.IsUuid(), "RelayState must use uint ID, not UUID")
	assert.Equal(t, notify.Id.ToString(), state.Id.ToString(), "both must use same sentinel ID")
}

// TEST460: Terminal ERR frame flows through correctly
func Test460_reorder_buffer_err_frame(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:test", nil, "")
	f0.Seq = 0
	rb.Accept(f0)

	errFrame := NewErr(rid, "ERR_TEST", "test error")
	errFrame.Seq = 1
	r, err := rb.Accept(errFrame)
	require.NoError(t, err)
	assert.Len(t, r, 1)
	assert.Equal(t, FrameTypeErr, r[0].FrameType)
	assert.Equal(t, uint64(1), r[0].Seq)
}

// TEST902: Verify FNV-1a checksum handles empty data
func Test902_compute_checksum_empty(t *testing.T) {
	checksum := ComputeChecksum([]byte(""))
	assert.Equal(t, uint64(0xcbf29ce484222325), checksum, "empty data produces FNV offset basis")
}

// TEST903: Verify CHUNK frame can store chunk_index and checksum fields
func Test903_chunk_with_chunk_index_and_checksum(t *testing.T) {
	uuidBytes := make([]byte, 16)
	for i := range uuidBytes {
		uuidBytes[i] = 1
	}
	id, err := NewMessageIdFromUuid(uuidBytes)
	require.NoError(t, err)
	streamId := "test-stream"
	payload := []byte("chunk data")
	checksum := ComputeChecksum(payload)

	frame := NewChunk(id, streamId, 5, payload, 3, checksum)

	assert.Equal(t, FrameTypeChunk, frame.FrameType)
	assert.True(t, frame.Id.Equals(id))
	require.NotNil(t, frame.StreamId)
	assert.Equal(t, streamId, *frame.StreamId)
	assert.Equal(t, uint64(5), frame.Seq)
	require.NotNil(t, frame.ChunkIndex, "chunk_index should be set")
	assert.Equal(t, uint64(3), *frame.ChunkIndex, "chunk_index should be set")
	require.NotNil(t, frame.Checksum, "checksum should be set")
	assert.Equal(t, checksum, *frame.Checksum, "checksum should be set")
}

// TEST904: Verify STREAM_END frame can store chunk_count field
func Test904_stream_end_with_chunk_count(t *testing.T) {
	uuidBytes := make([]byte, 16)
	for i := range uuidBytes {
		uuidBytes[i] = 1
	}
	id, err := NewMessageIdFromUuid(uuidBytes)
	require.NoError(t, err)
	streamId := "test-stream"

	frame := NewStreamEnd(id, streamId, 42)

	assert.Equal(t, FrameTypeStreamEnd, frame.FrameType)
	assert.True(t, frame.Id.Equals(id))
	require.NotNil(t, frame.StreamId)
	assert.Equal(t, streamId, *frame.StreamId)
	require.NotNil(t, frame.ChunkCount, "chunk_count should be set")
	assert.Equal(t, uint64(42), *frame.ChunkCount, "chunk_count should be set")
}

// TEST6672: CBOR decode ACCEPTS STREAM_END without chunk_count — unbounded streams make no length promise (v3, L16)
func Test6672_cbor_accepts_stream_end_without_chunk_count(t *testing.T) {
	reqId := NewMessageIdRandom()

	frame := NewStreamEndUnbounded(reqId, "s1")
	assert.Nil(t, frame.ChunkCount)

	encoded, err := EncodeFrame(frame)
	require.NoError(t, err, "encoding should succeed")

	decoded, decErr := DecodeFrame(encoded)
	require.NoError(t, decErr, "v3 accepts STREAM_END without chunk_count")
	assert.Equal(t, FrameTypeStreamEnd, decoded.FrameType)
	assert.Equal(t, reqId, decoded.Id)
	require.NotNil(t, decoded.StreamId)
	assert.Equal(t, "s1", *decoded.StreamId)
	assert.Nil(t, decoded.ChunkCount, "absent chunk_count must decode as nil, not a default")
}

// TEST7010: CREDIT frame round-trips encode/decode with rid, stream_id, and credit count
func Test7010_credit_frame_roundtrip(t *testing.T) {
	rid := NewMessageIdRandom()
	streamID := "s1"
	frame := NewCredit(rid, &streamID, 17, CreditDirectionResponse)
	require.NotNil(t, frame.CreditCount())
	assert.Equal(t, uint64(17), *frame.CreditCount())

	encoded, err := EncodeFrame(frame)
	require.NoError(t, err)
	decoded, err := DecodeFrame(encoded)
	require.NoError(t, err)
	assert.Equal(t, FrameTypeCredit, decoded.FrameType)
	assert.Equal(t, rid, decoded.Id)
	require.NotNil(t, decoded.StreamId)
	assert.Equal(t, "s1", *decoded.StreamId)
	require.NotNil(t, decoded.CreditCount())
	assert.Equal(t, uint64(17), *decoded.CreditCount())
	dir := decoded.CreditDirectionValue()
	require.NotNil(t, dir, "the routing direction must survive the wire (L11)")
	assert.Equal(t, CreditDirectionResponse, *dir)

	// Stream-less grant (request's sole stream) round-trips too
	frame2 := NewCredit(rid, nil, 3, CreditDirectionRequest)
	decoded2, err := DecodeFrame(mustEncode(t, frame2))
	require.NoError(t, err)
	assert.Nil(t, decoded2.StreamId)
	require.NotNil(t, decoded2.CreditCount())
	assert.Equal(t, uint64(3), *decoded2.CreditCount())
	dir2 := decoded2.CreditDirectionValue()
	require.NotNil(t, dir2)
	assert.Equal(t, CreditDirectionRequest, *dir2)

	// A Credit frame with no direction reports nil — hosts drop it as
	// unroutable (counted), since (xid, rid) alone cannot place it.
	dirless := NewFrame(FrameTypeCredit, NewMessageIdRandom())
	credit := uint64(1)
	dirless.Credit = &credit
	assert.Nil(t, dirless.CreditDirectionValue())

	// CreditCount is nil on non-Credit frames even if the field is set
	chunkish := NewFrame(FrameTypeLog, rid)
	c9 := uint64(9)
	chunkish.Credit = &c9
	assert.Nil(t, chunkish.CreditCount())
}

// TEST7011: CREDIT is a non-flow frame — no seq assigned, passes the reorder buffer untouched regardless of flow state
func Test7011_credit_is_non_flow(t *testing.T) {
	rid := NewMessageIdRandom()

	// SeqAssigner leaves Credit at seq 0 while flow frames advance
	assigner := NewSeqAssigner()
	chunk := NewChunk(rid, "s1", 0, []byte{1}, 0, 1)
	assigner.Assign(chunk)
	assert.Equal(t, uint64(0), chunk.Seq)
	streamID := "s1"
	credit := NewCredit(rid, &streamID, 4, CreditDirectionResponse)
	assigner.Assign(credit)
	assert.Equal(t, uint64(0), credit.Seq, "Credit must not consume a flow seq")
	chunk2 := NewChunk(rid, "s1", 0, []byte{2}, 1, 1)
	assigner.Assign(chunk2)
	assert.Equal(t, uint64(1), chunk2.Seq, "flow seq must be contiguous across a Credit")

	// ReorderBuffer returns Credit immediately even while the flow is gapped
	buffer := NewReorderBuffer(8)
	gapped := NewChunk(rid, "s1", 5, []byte{3}, 5, 1)
	gapped.Seq = 5
	delivered, err := buffer.Accept(gapped)
	require.NoError(t, err)
	assert.Empty(t, delivered, "out-of-order flow frame must be buffered")
	creditFrame := NewCredit(rid, &streamID, 4, CreditDirectionResponse)
	delivered2, err := buffer.Accept(creditFrame)
	require.NoError(t, err)
	require.Len(t, delivered2, 1, "Credit must bypass the reorder buffer and deliver immediately")
	assert.Equal(t, FrameTypeCredit, delivered2[0].FrameType)
}

// TEST7012: STREAM_START unbounded flag round-trips through CBOR; absent flag means bounded
func Test7012_stream_start_unbounded_roundtrip(t *testing.T) {
	rid := NewMessageIdRandom()
	isSeq := false
	bounded := NewStreamStart(rid, "s1", "media:enc=utf-8", &isSeq)
	assert.False(t, bounded.IsUnbounded())
	decoded, err := DecodeFrame(mustEncode(t, bounded))
	require.NoError(t, err)
	assert.False(t, decoded.IsUnbounded(), "absent flag must read as bounded")
	assert.Nil(t, decoded.Unbounded, "bounded frames omit the key")

	isSeqTrue := true
	unbounded := NewStreamStartUnbounded(rid, "s2", "media:enc=utf-8", &isSeqTrue)
	assert.True(t, unbounded.IsUnbounded())
	decoded2, err := DecodeFrame(mustEncode(t, unbounded))
	require.NoError(t, err)
	assert.True(t, decoded2.IsUnbounded())
	require.NotNil(t, decoded2.StreamId)
	assert.Equal(t, "s2", *decoded2.StreamId)
	require.NotNil(t, decoded2.IsSequence)
	assert.True(t, *decoded2.IsSequence)
}

// TEST7013: CBOR decode REJECTS a CREDIT frame missing its credit count
func Test7013_cbor_rejects_credit_without_count(t *testing.T) {
	frame := NewFrame(FrameTypeCredit, NewMessageIdRandom())
	streamID := "s1"
	frame.StreamId = &streamID
	// credit deliberately missing

	encoded, err := EncodeFrame(frame)
	require.NoError(t, err, "encoding should succeed")

	_, decErr := DecodeFrame(encoded)
	require.Error(t, decErr, "decode must reject CREDIT without a credit count")
	msg := decErr.Error()
	assert.True(t,
		strings.Contains(msg, "credit") || strings.Contains(msg, "CREDIT"),
		"error must name the missing field: %s", msg)
}

// TEST7026: An out-of-order terminal is buffered until the gap fills; buffered pre-terminal frames flush ahead of it in seq order, and only then may the flow be cleaned up
func Test7026_reorder_flushes_pre_terminal_before_cleanup(t *testing.T) {
	rid := NewMessageIdRandom()
	buffer := NewReorderBuffer(8)

	mk := func(seq uint64, ftype FrameType) *Frame {
		f := NewFrame(ftype, rid)
		f.Seq = seq
		return f
	}

	// seq 0 delivers immediately.
	delivered, err := buffer.Accept(mk(0, FrameTypeChunk))
	require.NoError(t, err)
	require.Len(t, delivered, 1)

	// seq 2 (chunk) and seq 3 (END) arrive out of order — both buffered,
	// nothing delivered, no premature cleanup possible.
	d2, err := buffer.Accept(mk(2, FrameTypeChunk))
	require.NoError(t, err)
	assert.Empty(t, d2)
	d3, err := buffer.Accept(mk(3, FrameTypeEnd))
	require.NoError(t, err)
	assert.Empty(t, d3)

	// The gap fills: seq 1 arrives → 1, 2, 3(END) all deliver in order.
	// The terminal is DELIVERED strictly after every pre-terminal frame.
	delivered4, err := buffer.Accept(mk(1, FrameTypeChunk))
	require.NoError(t, err)
	require.Len(t, delivered4, 3)
	seqs := []uint64{delivered4[0].Seq, delivered4[1].Seq, delivered4[2].Seq}
	assert.Equal(t, []uint64{1, 2, 3}, seqs)
	assert.Equal(t, FrameTypeEnd, delivered4[len(delivered4)-1].FrameType)

	// Cleanup after delivered terminal (as the relay does, post-drain): the
	// flow state resets and a fresh flow under the same key starts cleanly at seq 0.
	buffer.CleanupFlow(FlowKeyFromFrame(delivered4[2]))
	fresh, err := buffer.Accept(mk(0, FrameTypeChunk))
	require.NoError(t, err)
	require.Len(t, fresh, 1, "cleaned flow accepts a fresh seq 0")
}

// TEST7014: END terminal meta (progress, message) round-trips; successful END without progress reads as 1.0; failed END without progress reads as nil
func Test7014_end_terminal_meta_roundtrip(t *testing.T) {
	rid := NewMessageIdRandom()

	// Explicit terminal progress + message round-trip
	progress := 0.87
	message := "partial corpus"
	end := EndOkWith(rid, nil, &progress, &message)
	decoded, err := DecodeFrame(mustEncode(t, end))
	require.NoError(t, err)
	require.NotNil(t, decoded.FinalProgress())
	assert.InDelta(t, 0.87, *decoded.FinalProgress(), 1e-9)
	require.NotNil(t, decoded.FinalMessage())
	assert.Equal(t, "partial corpus", *decoded.FinalMessage())
	require.NotNil(t, decoded.ExitCode(), "end_ok_with implies success")
	assert.Equal(t, int64(0), *decoded.ExitCode())

	// Successful END with no explicit progress reads as 1.0
	end2 := EndOk(rid, nil)
	decoded2, err := DecodeFrame(mustEncode(t, end2))
	require.NoError(t, err)
	require.NotNil(t, decoded2.FinalProgress())
	assert.Equal(t, 1.0, *decoded2.FinalProgress())
	assert.Nil(t, decoded2.FinalMessage())

	// Non-successful END (no exit_code) with no explicit progress: nil —
	// failure must not synthesize a completion value.
	end3 := NewEnd(rid, nil)
	decoded3, err := DecodeFrame(mustEncode(t, end3))
	require.NoError(t, err)
	assert.Nil(t, decoded3.FinalProgress())

	// Non-END frames never report a final progress
	log := NewProgress(rid, 0.5, "halfway")
	assert.Nil(t, log.FinalProgress())
}

func mustEncode(t *testing.T, frame *Frame) []byte {
	t.Helper()
	encoded, err := EncodeFrame(frame)
	require.NoError(t, err)
	return encoded
}

// TEST502: Keys module has constants for new fields
func Test502_keys_module_new_field_constants(t *testing.T) {
	assert.Equal(t, 13, keyRoutingId)
	assert.Equal(t, 14, keyChunkIndex)
	assert.Equal(t, 15, keyChunkCount)
	assert.Equal(t, 16, keyChecksum)
	assert.Equal(t, 17, keyIsSequence)
}
