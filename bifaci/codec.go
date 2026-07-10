package bifaci

import (
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// CBOR map keys (MUST match Rust implementation exactly)
// From capdag/src/cbor_frame.rs lines 10-22:
const (
	keyVersion     = 0  // version (u8, always 2)
	keyFrameType   = 1  // frame_type (u8)
	keyId          = 2  // id (bytes[16] or uint)
	keySeq         = 3  // seq (u64)
	keyContentType = 4  // content_type (tstr, optional)
	keyMeta        = 5  // meta (map, optional)
	keyPayload     = 6  // payload (bstr, optional)
	keyLen         = 7  // len (u64, optional - total payload length for chunked)
	keyOffset      = 8  // offset (u64, optional - byte offset in chunked stream)
	keyEof         = 9  // eof (bool, optional - true on final chunk)
	keyCap         = 10 // cap (tstr, optional - cap URN for requests)
	keyStreamId    = 11 // stream_id (tstr, optional - stream ID for multiplexed streaming)
	keyMediaUrn    = 12 // media_urn (tstr, optional - media URN for stream type)
	keyRoutingId   = 13 // routing_id (bytes[16] or uint, optional - relay routing)
	keyChunkIndex  = 14 // chunk_index (u64, REQUIRED for CHUNK frames)
	keyChunkCount  = 15 // chunk_count (u64, REQUIRED for STREAM_END frames)
	keyChecksum    = 16 // checksum (u64, REQUIRED for CHUNK frames - FNV-1a hash)
	keyIsSequence  = 17 // is_sequence (bool, optional - whether producer used emit_list_item)
	keyForceKill   = 18 // force_kill (bool, optional - whether Cancel should force-kill)
	keyCredit      = 19 // credit (u64, REQUIRED for CREDIT frames - flow-control grant in CHUNK units)
	keyUnbounded   = 20 // unbounded (bool, optional - STREAM_START makes no length promise)
)

// EncodeFrame encodes a Frame to CBOR bytes using integer keys (matches Rust)
func EncodeFrame(frame *Frame) ([]byte, error) {
	// Build CBOR map with integer keys matching Rust layout
	m := make(map[int]interface{})

	// 0: version (always 1)
	m[keyVersion] = uint8(ProtocolVersion)

	// 1: frame_type
	m[keyFrameType] = uint8(frame.FrameType)

	// 2: id (bytes[16] for UUID, uint64 for uint variant)
	if frame.Id.IsUuid() {
		m[keyId] = frame.Id.uuidBytes
	} else if frame.Id.uintValue != nil {
		m[keyId] = *frame.Id.uintValue
	} else {
		m[keyId] = uint64(0)
	}

	// 3: seq (for CHUNK frames)
	if frame.Seq != 0 {
		m[keySeq] = frame.Seq
	}

	// 4: content_type (optional)
	if frame.ContentType != nil && *frame.ContentType != "" {
		m[keyContentType] = *frame.ContentType
	}

	// 5: meta (optional)
	if frame.Meta != nil && len(frame.Meta) > 0 {
		m[keyMeta] = frame.Meta
	}

	// 6: payload (optional)
	if frame.Payload != nil {
		m[keyPayload] = frame.Payload
	}

	// 7: len (optional - for CHUNK frames)
	if frame.Len != nil {
		m[keyLen] = *frame.Len
	}

	// 8: offset (optional - for CHUNK frames)
	if frame.Offset != nil {
		m[keyOffset] = *frame.Offset
	}

	// 9: eof (optional)
	if frame.Eof != nil && *frame.Eof {
		m[keyEof] = true
	}

	// 10: cap (optional - for REQ frames)
	if frame.Cap != nil && *frame.Cap != "" {
		m[keyCap] = *frame.Cap
	}

	// 11: stream_id (optional - for STREAM_START, CHUNK, STREAM_END frames)
	if frame.StreamId != nil && *frame.StreamId != "" {
		m[keyStreamId] = *frame.StreamId
	}

	// 12: media_urn (optional - for STREAM_START frames)
	if frame.MediaUrn != nil && *frame.MediaUrn != "" {
		m[keyMediaUrn] = *frame.MediaUrn
	}

	// 13: routing_id (optional - for relay routing)
	if frame.RoutingId != nil {
		if frame.RoutingId.IsUuid() {
			m[keyRoutingId] = frame.RoutingId.uuidBytes
		} else if frame.RoutingId.uintValue != nil {
			m[keyRoutingId] = *frame.RoutingId.uintValue
		}
	}

	// 14: chunk_index (REQUIRED for CHUNK frames)
	if frame.ChunkIndex != nil {
		m[keyChunkIndex] = *frame.ChunkIndex
	}

	// 15: chunk_count (REQUIRED for STREAM_END frames)
	if frame.ChunkCount != nil {
		m[keyChunkCount] = *frame.ChunkCount
	}

	// 16: checksum (REQUIRED for CHUNK frames)
	if frame.Checksum != nil {
		m[keyChecksum] = *frame.Checksum
	}

	// 17: is_sequence (optional - for STREAM_START frames)
	if frame.IsSequence != nil {
		m[keyIsSequence] = *frame.IsSequence
	}

	// 18: force_kill (optional - for CANCEL frames)
	if frame.ForceKill != nil {
		m[keyForceKill] = *frame.ForceKill
	}

	// 19: credit (REQUIRED for CREDIT frames)
	if frame.Credit != nil {
		m[keyCredit] = *frame.Credit
	}

	// 20: unbounded (optional - for STREAM_START frames)
	if frame.Unbounded != nil {
		m[keyUnbounded] = *frame.Unbounded
	}

	return cbor.Marshal(m)
}

// DecodeFrame decodes CBOR bytes to a Frame using integer keys (matches Rust)
func DecodeFrame(data []byte) (*Frame, error) {
	var m map[int]interface{}
	if err := cbor.Unmarshal(data, &m); err != nil {
		return nil, err
	}

	frame := &Frame{}

	// 0: version (required - must be PROTOCOL_VERSION)
	verVal, ok := m[keyVersion]
	if !ok {
		return nil, errors.New("missing version (key 0)")
	}
	if ver, ok := verVal.(uint64); ok {
		frame.Version = uint8(ver)
		if frame.Version != ProtocolVersion {
			return nil, fmt.Errorf("invalid version %d, expected %d", frame.Version, ProtocolVersion)
		}
	} else {
		return nil, errors.New("version must be uint")
	}

	// 1: frame_type (required)
	ftVal, ok := m[keyFrameType]
	if !ok {
		return nil, errors.New("missing frame_type (key 1)")
	}
	if ft, ok := ftVal.(uint64); ok {
		frameType := FrameType(ft)
		// Validate frame type is in valid range (0-13, excluding removed value 2)
		if frameType < FrameTypeHello || frameType > FrameTypeCredit {
			return nil, fmt.Errorf("invalid frame_type %d", ft)
		}
		// Reject old RES frame type (2) - permanently retired
		if frameType == 2 {
			return nil, fmt.Errorf("frame_type 2 (RES) is permanently retired, not supported in protocol v3")
		}
		frame.FrameType = frameType
	} else {
		return nil, errors.New("frame_type must be uint")
	}

	// 2: id (required - can be bytes[16] for UUID or uint for uint64)
	idVal, ok := m[keyId]
	if !ok {
		return nil, errors.New("missing id (key 2)")
	}

	switch v := idVal.(type) {
	case []byte:
		// UUID variant
		if len(v) != 16 {
			return nil, errors.New("UUID id must be 16 bytes")
		}
		frame.Id = MessageId{uuidBytes: v}
	case uint64:
		// uint variant
		frame.Id = NewMessageIdFromUint(v)
	default:
		return nil, errors.New("id must be bytes[16] or uint")
	}

	// 3: seq (optional - for CHUNK frames)
	if seqVal, ok := m[keySeq]; ok {
		if seq, ok := seqVal.(uint64); ok {
			frame.Seq = seq
		}
	}

	// 4: content_type (optional)
	if ctVal, ok := m[keyContentType]; ok {
		if ct, ok := ctVal.(string); ok {
			frame.ContentType = &ct
		}
	}

	// 5: meta (optional)
	if metaVal, ok := m[keyMeta]; ok {
		if meta, ok := metaVal.(map[interface{}]interface{}); ok {
			// Convert map[interface{}]interface{} to map[string]interface{}
			frame.Meta = make(map[string]interface{})
			for k, v := range meta {
				if ks, ok := k.(string); ok {
					frame.Meta[ks] = v
				}
			}
		}
	}

	// 6: payload (optional)
	if payloadVal, ok := m[keyPayload]; ok {
		if payload, ok := payloadVal.([]byte); ok {
			frame.Payload = payload
		}
	}

	// 7: len (optional - for CHUNK frames)
	if lenVal, ok := m[keyLen]; ok {
		if l, ok := lenVal.(uint64); ok {
			frame.Len = &l
		}
	}

	// 8: offset (optional - for CHUNK frames)
	if offsetVal, ok := m[keyOffset]; ok {
		if offset, ok := offsetVal.(uint64); ok {
			frame.Offset = &offset
		}
	}

	// 9: eof (optional)
	if eofVal, ok := m[keyEof]; ok {
		if eof, ok := eofVal.(bool); ok {
			frame.Eof = &eof
		}
	}

	// 10: cap (optional - for REQ frames)
	if capVal, ok := m[keyCap]; ok {
		if cap, ok := capVal.(string); ok {
			frame.Cap = &cap
		}
	}

	// 11: stream_id (optional - for STREAM_START, CHUNK, STREAM_END frames)
	if streamIdVal, ok := m[keyStreamId]; ok {
		if streamId, ok := streamIdVal.(string); ok {
			frame.StreamId = &streamId
		}
	}

	// 12: media_urn (optional - for STREAM_START frames)
	if mediaUrnVal, ok := m[keyMediaUrn]; ok {
		if mediaUrn, ok := mediaUrnVal.(string); ok {
			frame.MediaUrn = &mediaUrn
		}
	}

	// 13: routing_id (optional - for relay routing)
	if routingIdVal, ok := m[keyRoutingId]; ok {
		switch v := routingIdVal.(type) {
		case []byte:
			if len(v) == 16 {
				rid, err := NewMessageIdFromUuid(v)
				if err == nil {
					frame.RoutingId = &rid
				}
			}
		case uint64:
			rid := NewMessageIdFromUint(v)
			frame.RoutingId = &rid
		}
	}

	// 14: chunk_index (REQUIRED for CHUNK frames)
	if chunkIndexVal, ok := m[keyChunkIndex]; ok {
		switch v := chunkIndexVal.(type) {
		case uint64:
			frame.ChunkIndex = &v
		case int64:
			u := uint64(v)
			frame.ChunkIndex = &u
		case int:
			u := uint64(v)
			frame.ChunkIndex = &u
		case uint:
			u := uint64(v)
			frame.ChunkIndex = &u
		}
	}

	// 15: chunk_count (REQUIRED for STREAM_END frames)
	if chunkCountVal, ok := m[keyChunkCount]; ok {
		switch v := chunkCountVal.(type) {
		case uint64:
			frame.ChunkCount = &v
		case int64:
			u := uint64(v)
			frame.ChunkCount = &u
		case int:
			u := uint64(v)
			frame.ChunkCount = &u
		case uint:
			u := uint64(v)
			frame.ChunkCount = &u
		}
	}

	// 16: checksum (REQUIRED for CHUNK frames)
	if checksumVal, ok := m[keyChecksum]; ok {
		switch v := checksumVal.(type) {
		case uint64:
			frame.Checksum = &v
		case int64:
			u := uint64(v)
			frame.Checksum = &u
		case int:
			u := uint64(v)
			frame.Checksum = &u
		case uint:
			u := uint64(v)
			frame.Checksum = &u
		}
	}

	// 17: is_sequence (optional - for STREAM_START frames)
	if isSequenceVal, ok := m[keyIsSequence]; ok {
		if isSequence, ok := isSequenceVal.(bool); ok {
			frame.IsSequence = &isSequence
		}
	}

	// 18: force_kill (optional - for CANCEL frames)
	if forceKillVal, ok := m[keyForceKill]; ok {
		if forceKill, ok := forceKillVal.(bool); ok {
			frame.ForceKill = &forceKill
		}
	}

	// 19: credit (REQUIRED for CREDIT frames)
	if creditVal, ok := m[keyCredit]; ok {
		switch v := creditVal.(type) {
		case uint64:
			frame.Credit = &v
		case int64:
			u := uint64(v)
			frame.Credit = &u
		case int:
			u := uint64(v)
			frame.Credit = &u
		case uint:
			u := uint64(v)
			frame.Credit = &u
		}
	}

	// 20: unbounded (optional - for STREAM_START frames)
	if unboundedVal, ok := m[keyUnbounded]; ok {
		if unbounded, ok := unboundedVal.(bool); ok {
			frame.Unbounded = &unbounded
		}
	}

	// Validate required fields based on frame type
	if frame.FrameType == FrameTypeChunk {
		if frame.ChunkIndex == nil {
			return nil, errors.New("CHUNK frame missing required field: chunk_index")
		}
		if frame.Checksum == nil {
			return nil, errors.New("CHUNK frame missing required field: checksum")
		}
	}
	// STREAM_END: chunk_count is optional on the wire — unbounded streams make
	// no length promise (protocol v3, L16). Bounded-stream receivers that want
	// to verify completeness check chunk_count when present.
	if frame.FrameType == FrameTypeCredit {
		if frame.Credit == nil {
			return nil, errors.New("CREDIT frame missing required field: credit")
		}
	}

	return frame, nil
}
