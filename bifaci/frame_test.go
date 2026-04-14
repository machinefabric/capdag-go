package bifaci

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST171: Test all FrameType discriminants roundtrip through uint8 conversion preserving identity
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
	}

	for _, ft := range types {
		asUint := uint8(ft)
		backToType := FrameType(asUint)
		if backToType != ft {
			t.Errorf("FrameType %v roundtrip failed: got %v", ft, backToType)
		}
	}
}

// TEST172: Test FrameType::from_u8 returns invalid for values outside the valid discriminant range
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
	}

	for i := uint8(0); i <= 12; i++ {
		if expected, exists := validTypes[i]; exists && expected {
			ft := FrameType(i)
			if ft.String() == fmt.Sprintf("UNKNOWN(%d)", i) {
				t.Errorf("Expected %d to be a valid FrameType", i)
			}
		}
	}
	// 13 is one past Cancel — must be invalid
	ft13 := FrameType(13)
	if ft13.String() != "UNKNOWN(13)" {
		t.Errorf("Expected 13 to be invalid, got %s", ft13.String())
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

	// Verify it's a valid UUID format
	_, err := uuid.Parse(uuidStr)
	if err != nil {
		t.Errorf("Invalid UUID string: %v", err)
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

// TEST176: Test MessageId::Uint does not produce a UUID string, to_uuid_string returns empty
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

// TEST177: Test MessageId::from_uuid_bytes rejects invalid UUID bytes
func Test177_message_id_from_uuid_invalid_bytes(t *testing.T) {
	// UUID must be exactly 16 bytes
	_, err := NewMessageIdFromUuid([]byte{1, 2, 3}) // too short
	if err == nil {
		t.Error("Expected error for invalid UUID length")
	}

	_, err = NewMessageIdFromUuid(make([]byte, 20)) // too long
	if err == nil {
		t.Error("Expected error for invalid UUID length")
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

// TEST179: Test MessageId::default creates appropriate variant
func Test179_message_id_default(t *testing.T) {
	id := NewMessageIdDefault()
	// Default is Uint 0
	if id.IsUuid() {
		t.Error("Default should be Uint variant")
	}
	if id.ToString() != "0" {
		t.Errorf("Default Uint should be 0, got %s", id.ToString())
	}
}

// TEST180: Test Frame::hello without manifest produces correct HELLO frame
func Test180_frame_hello_without_manifest(t *testing.T) {
	frame := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer)
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

// TEST181: Test Frame::hello_with_manifest produces HELLO with manifest bytes
func Test181_frame_hello_with_manifest(t *testing.T) {
	manifest := []byte(`{"name":"test"}`)
	frame := NewHelloWithManifest(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, manifest)
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
	cap := `cap:in="media:void";op=test;out="media:void"`
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

// TEST184: Test Frame::chunk stores seq and payload for streaming (updated for stream_id)
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

// TEST185: Test Frame::err stores error code and message
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

// TEST186: Test Frame::log stores level and message
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

// TEST187: Test Frame::end with payload sets final payload
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

// TEST188: Test Frame::end without payload
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

// TEST189: Test chunk with offset (future enhancement - not yet implemented)
func Test189_frame_chunk_with_offset(t *testing.T) {
	// This test documents expected behavior for offset/len fields
	// Currently not implemented in Go version
	t.Skip("Offset/len fields not yet implemented in Go version")
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

// TEST191: Test error_code and error_message return empty for non-Err frame types
func Test191_error_accessors_on_non_err_frame(t *testing.T) {
	req := NewReq(NewMessageIdRandom(), "cap:op=test", []byte{}, "text/plain")
	if req.ErrorCode() != "" {
		t.Error("REQ must have no error_code")
	}
	if req.ErrorMessage() != "" {
		t.Error("REQ must have no error_message")
	}

	hello := NewHello(1000, 500, DefaultMaxReorderBuffer)
	if hello.ErrorCode() != "" {
		t.Error("HELLO must have no error_code")
	}
}

// TEST192: Test log_level and log_message return empty for non-Log frame types
func Test192_log_accessors_on_non_log_frame(t *testing.T) {
	req := NewReq(NewMessageIdRandom(), "cap:op=test", []byte{}, "text/plain")
	if req.LogLevel() != "" {
		t.Error("REQ must have no log_level")
	}
	if req.LogMessage() != "" {
		t.Error("REQ must have no log_message")
	}
}

// TEST193: Test hello_max_frame and hello_max_chunk return appropriate values
func Test193_hello_accessors_on_non_hello_frame(t *testing.T) {
	err := NewErr(NewMessageIdRandom(), "E", "m")
	// ERR frames have no Meta with hello limits
	if err.Meta != nil {
		if _, hasMaxFrame := err.Meta["max_frame"]; hasMaxFrame {
			t.Error("ERR frame should not have max_frame in meta")
		}
	}
}

// TEST194: Test newFrame sets version and defaults correctly, optional fields are nil
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

// TEST195: Test default frame type (Go doesn't have Frame::default, skip or use REQ as default)
func Test195_frame_default_type(t *testing.T) {
	// Go doesn't have a Frame::default(), but we can verify REQ is a common default
	frame := NewReq(NewMessageIdDefault(), "cap:op=test", []byte{}, "text/plain")
	if frame.FrameType != FrameTypeReq {
		t.Error("Expected REQ frame type")
	}
	if frame.Version != ProtocolVersion {
		t.Errorf("Expected version %d", ProtocolVersion)
	}
}

// TEST196: Test IsEof returns false when eof field is nil (unset)
func Test196_is_eof_when_none(t *testing.T) {
	frame := newFrame(FrameTypeChunk, MessageId{uintValue: new(uint64)})
	if frame.IsEof() {
		t.Error("eof=nil must mean not EOF")
	}
}

// TEST197: Test IsEof returns false when eof field is explicitly false
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
}

// TEST199: Test PROTOCOL_VERSION is 2
func Test199_protocol_version_constant(t *testing.T) {
	if ProtocolVersion != 2 {
		t.Errorf("PROTOCOL_VERSION must be 2, got %d", ProtocolVersion)
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
	frame := NewHelloWithManifest(1000, 500, DefaultMaxReorderBuffer, binaryManifest)

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

// TEST202: Test MessageId Eq semantics: equal UUIDs are equal, different ones are not
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

// TEST204: Test Frame::req with empty payload stores empty slice not nil
func Test204_req_frame_empty_payload(t *testing.T) {
	frame := NewReq(NewMessageIdRandom(), "cap:op=test", []byte{}, "text/plain")
	if frame.Payload == nil {
		t.Error("Empty payload should be empty slice, not nil")
	}
	if len(frame.Payload) != 0 {
		t.Error("Empty payload should have length 0")
	}
}

// TEST365: Frame::stream_start stores reqId, streamId, mediaUrn
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

// TEST366: Frame::stream_end stores reqId, streamId
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

// TEST367: Frame::stream_start with empty streamId still constructs
func Test367_stream_start_with_empty_stream_id(t *testing.T) {
	reqId := NewMessageIdRandom()
	streamId := ""
	mediaUrn := "media:json"

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

// TEST368: Frame::stream_start with empty mediaUrn still constructs
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

// TEST399: RelayNotify discriminant roundtrips through uint8 conversion (value 10)
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

// TEST400: RelayState discriminant roundtrips through uint8 conversion (value 11)
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

// TEST401: relay_notify factory stores manifest and limits, accessors extract them correctly
func Test401_relay_notify_factory_and_accessors(t *testing.T) {
	manifest := []byte(`{"caps":["cap:op=test"]}`)
	maxFrame := 2_000_000
	maxChunk := 128_000

	frame := NewRelayNotify(manifest, maxFrame, maxChunk, DefaultMaxReorderBuffer)

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
	req := NewReq(NewMessageIdRandom(), "cap:op=test", []byte{}, "text/plain")
	if req.RelayNotifyManifest() != nil {
		t.Error("RelayNotifyManifest on REQ must return nil")
	}
	if req.RelayNotifyLimits() != nil {
		t.Error("RelayNotifyLimits on REQ must return nil")
	}
}

// TEST402: relay_state factory stores resource payload in Payload field
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

// TEST403: FrameType from value 13 is invalid (one past Cancel)
func Test403_frame_type_one_past_cancel(t *testing.T) {
	ft := FrameType(13)
	if ft.String() != fmt.Sprintf("UNKNOWN(%d)", 13) {
		t.Errorf("FrameType(13) must be unknown, got %s", ft.String())
	}
}

// TEST667: VerifyChunkChecksum detects corrupted payload
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

// TEST436: compute_checksum determinism and sensitivity
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

// TEST442: SeqAssigner assigns monotonically increasing seq for same RID
func Test442_seq_assigner_monotonic_same_rid(t *testing.T) {
	assigner := NewSeqAssigner()
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:op=test", nil, "")
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

// TEST443: SeqAssigner maintains independent per-RID counters
func Test443_seq_assigner_independent_rids(t *testing.T) {
	assigner := NewSeqAssigner()
	ridA := NewMessageIdRandom()
	ridB := NewMessageIdRandom()

	a0 := NewReq(ridA, "cap:op=a", nil, "")
	a1 := NewChunk(ridA, "", 0, nil, 0, 0)
	a2 := NewEnd(ridA, nil)
	b0 := NewReq(ridB, "cap:op=b", nil, "")
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

// TEST444: SeqAssigner skips non-flow frames
func Test444_seq_assigner_skips_non_flow(t *testing.T) {
	assigner := NewSeqAssigner()

	hello := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer)
	hb := NewHeartbeat(NewMessageIdRandom())
	rn := NewRelayNotify(nil, DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer)
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

// TEST445: SeqAssigner remove resets only the targeted flow key
func Test445_seq_assigner_remove_by_flow_key(t *testing.T) {
	assigner := NewSeqAssigner()
	rid := NewMessageIdRandom()
	xid := NewMessageIdRandom()

	// Flow 1: (rid, no xid) — assign seq 0, 1
	f1a := NewReq(rid, "cap:op=test", nil, "")
	f1b := NewChunk(rid, "", 0, nil, 0, 0)
	assigner.Assign(f1a)
	assigner.Assign(f1b)

	// Flow 2: (rid, xid) — assign seq 0, 1
	f2a := NewReq(rid, "cap:op=test", nil, "")
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
	f1c := NewReq(rid, "cap:op=test", nil, "")
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

// TEST860: Same RID different XIDs are independent flows
func Test860_seq_assigner_same_rid_different_xids_independent(t *testing.T) {
	assigner := NewSeqAssigner()
	rid := NewMessageIdRandom()
	xidA := NewMessageIdRandom()
	xidB := NewMessageIdRandom()

	// Flow (rid, xidA)
	fA0 := NewReq(rid, "cap:op=a", nil, "")
	fA0.RoutingId = &xidA
	fA1 := NewChunk(rid, "", 0, nil, 0, 0)
	fA1.RoutingId = &xidA

	// Flow (rid, xidB)
	fB0 := NewReq(rid, "cap:op=b", nil, "")
	fB0.RoutingId = &xidB

	// Flow (rid, no xid)
	fNone0 := NewReq(rid, "cap:op=c", nil, "")

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

// TEST446: SeqAssigner counts across mixed flow frame types
func Test446_seq_assigner_mixed_types(t *testing.T) {
	assigner := NewSeqAssigner()
	rid := NewMessageIdRandom()

	req := NewReq(rid, "cap:op=test", nil, "")
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

// TEST447: FlowKey with XID extracts correctly from frame
func Test447_flow_key_with_xid(t *testing.T) {
	rid := NewMessageIdRandom()
	xid := NewMessageIdRandom()

	frame := NewReq(rid, "cap:op=test", nil, "")
	frame.RoutingId = &xid

	key := FlowKeyFromFrame(frame)
	if key.rid != rid.ToString() {
		t.Error("FlowKey RID mismatch")
	}
	if key.xid != xid.ToString() {
		t.Error("FlowKey XID mismatch")
	}
}

// TEST448: FlowKey without XID has empty xid
func Test448_flow_key_without_xid(t *testing.T) {
	rid := NewMessageIdRandom()
	frame := NewReq(rid, "cap:op=test", nil, "")

	key := FlowKeyFromFrame(frame)
	if key.rid != rid.ToString() {
		t.Error("FlowKey RID mismatch")
	}
	if key.xid != "" {
		t.Errorf("FlowKey XID should be empty, got %q", key.xid)
	}
}

// TEST449: FlowKey equality semantics
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

// TEST450: FlowKey hash lookup in map
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

// TEST451: ReorderBuffer delivers frames immediately when in order
func Test451_reorder_buffer_in_order(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:op=test", nil, "")
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

// TEST452: ReorderBuffer holds out-of-order, releases when gap filled
func Test452_reorder_buffer_out_of_order(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:op=test", nil, "")
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

// TEST453: ReorderBuffer gap fill with arrival order 0, 2, 1
func Test453_reorder_buffer_gap_fill(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:op=test", nil, "")
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

// TEST454: ReorderBuffer rejects stale/duplicate seq
func Test454_reorder_buffer_stale_seq(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:op=test", nil, "")
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

// TEST455: ReorderBuffer overflow
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

// TEST456: ReorderBuffer independent flows
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
	fB0 := NewReq(ridB, "cap:op=b", nil, "")
	fB0.Seq = 0
	rB0, err := rb.Accept(fB0)
	require.NoError(t, err)
	assert.Len(t, rB0, 1, "B seq=0 delivers immediately regardless of A's gap")

	// Flow A: submit seq=0 — releases both A frames
	fA0 := NewReq(ridA, "cap:op=a", nil, "")
	fA0.Seq = 0
	rA0, err := rb.Accept(fA0)
	require.NoError(t, err)
	assert.Len(t, rA0, 2, "A seq=0 releases seq=0 and seq=1")
}

// TEST457: ReorderBuffer cleanup_flow resets state
func Test457_reorder_buffer_cleanup(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:op=test", nil, "")
	f0.Seq = 0
	rb.Accept(f0)

	f1 := NewChunk(rid, "", 0, nil, 0, 0)
	f1.Seq = 1
	rb.Accept(f1)

	// Cleanup the flow
	key := FlowKeyFromFrame(f0)
	rb.CleanupFlow(key)

	// Same RID can start over at seq=0 without stale error
	f0b := NewReq(rid, "cap:op=test", nil, "")
	f0b.Seq = 0
	r, err := rb.Accept(f0b)
	require.NoError(t, err)
	assert.Len(t, r, 1)
}

// TEST458: ReorderBuffer non-flow frames bypass reordering
func Test458_reorder_buffer_non_flow_bypass(t *testing.T) {
	rb := NewReorderBuffer(10)

	hello := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer)
	hb := NewHeartbeat(NewMessageIdRandom())
	rn := NewRelayNotify(nil, DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer)
	rs := NewRelayState(nil)

	for _, frame := range []*Frame{hello, hb, rn, rs} {
		r, err := rb.Accept(frame)
		require.NoError(t, err)
		assert.Len(t, r, 1, "Non-flow frame should bypass reorder buffer")
	}
}

// TEST459: ReorderBuffer handles END frame correctly
func Test459_reorder_buffer_end_frame(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:op=test", nil, "")
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

// TEST460: ReorderBuffer handles ERR frame correctly
func Test460_reorder_buffer_err_frame(t *testing.T) {
	rb := NewReorderBuffer(10)
	rid := NewMessageIdRandom()

	f0 := NewReq(rid, "cap:op=test", nil, "")
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
