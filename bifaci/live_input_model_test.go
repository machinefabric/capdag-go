package bifaci

import (
	"strings"
	"sync"
	"testing"
	"time"

	cborlib "github.com/fxamacker/cbor/v2"
)

// =============================================================================
// live-input-model parity tests
//
// Ports capdag's cartridge_runtime.rs live-demux tests (TEST7070, TEST1300,
// TEST1301, TEST1302, TEST7073) to this mirror's architecture. The reference
// exposes a per-stream InputStream (is_unbounded()/recv()) built by
// demux_multi_stream operating on a raw per-request frame channel; this
// mirror's handlers instead consume a single flat live frame channel (see
// runCBORModeIO / unboundedFrameChan / seqReassembly), so these tests drive
// the SAME reference behavior — live incremental delivery, item-granular
// RFC 8742 fragment reassembly, hard truncation errors, fragment crediting,
// and buffering collectors refusing unbounded streams — through this
// mirror's equivalent surfaces: the full runCBORModeIO wire harness for the
// live-demux behaviors, and the frame_helpers.go / CollectStreams collectors
// directly for the L16 refusal.
// =============================================================================

// TEST7070: An unbounded input stream is consumed live — the handler
// observes early items while the producer is still emitting, and the stream
// reports itself unbounded. (Rust: test7070_unbounded_input_consumed_live)
func Test7070_unbounded_input_consumed_live(t *testing.T) {
	type observedItem struct {
		unbounded bool
		value     []byte
	}
	seen := make(chan observedItem, 8)

	rt, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("failed to create runtime: %v", err)
	}
	rt.Register(testCapUrn, func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
		unbounded := false
		for frame := range frames {
			switch frame.FrameType {
			case FrameTypeStreamStart:
				unbounded = frame.IsUnbounded()
			case FrameTypeChunk:
				if frame.Payload != nil {
					var v []byte
					if err := cborlib.Unmarshal(frame.Payload, &v); err == nil {
						seen <- observedItem{unbounded: unbounded, value: v}
					}
				}
			}
		}
		return nil
	})

	hostWriter, outFrames, _ := startTestCartridge(t, rt, DefaultInitialCredit)
	rid := NewMessageIdRandom()

	mkChunk := func(i uint64) *Frame {
		payload, err := cborlib.Marshal([]byte{byte(i)})
		if err != nil {
			t.Fatalf("cbor encode: %v", err)
		}
		checksum := ComputeChecksum(payload)
		return NewChunk(rid, "live", i, payload, i, checksum)
	}

	if err := hostWriter.WriteFrame(NewReq(rid, testCapUrn, nil, "application/cbor")); err != nil {
		t.Fatalf("REQ: %v", err)
	}
	seqTrue := true
	if err := hostWriter.WriteFrame(NewStreamStartUnbounded(rid, "live", "media:enc=utf-8", &seqTrue)); err != nil {
		t.Fatalf("STREAM_START: %v", err)
	}
	if err := hostWriter.WriteFrame(mkChunk(0)); err != nil {
		t.Fatalf("CHUNK 0: %v", err)
	}

	// The handler must observe item 0 while the producer has not produced
	// item 1 — no buffering-to-completion (L16). If this were still
	// buffer-then-dispatch, nothing would arrive here until STREAM_END/END
	// (which we have not sent yet).
	select {
	case item := <-seen:
		if !item.unbounded {
			t.Error("STREAM_START flag must surface as unbounded")
		}
		if len(item.value) != 1 || item.value[0] != 0 {
			t.Errorf("expected item 0, got %v", item.value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for live item 0 — handler is not observing items as they arrive")
	}

	// Producer continues; consumer keeps up item by item.
	if err := hostWriter.WriteFrame(mkChunk(1)); err != nil {
		t.Fatalf("CHUNK 1: %v", err)
	}
	select {
	case item := <-seen:
		if len(item.value) != 1 || item.value[0] != 1 {
			t.Errorf("expected item 1, got %v", item.value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for live item 1")
	}

	// The unbounded stream still ends cleanly — no chunk_count promise.
	if err := hostWriter.WriteFrame(NewStreamEndUnbounded(rid, "live")); err != nil {
		t.Fatalf("STREAM_END: %v", err)
	}
	if err := hostWriter.WriteFrame(NewEnd(rid, nil)); err != nil {
		t.Fatalf("END: %v", err)
	}

	deadline := time.After(3 * time.Second)
	for {
		select {
		case f, ok := <-outFrames:
			if !ok {
				t.Fatal("output closed before END")
			}
			if f.FrameType == FrameTypeEnd && f.Id.Equals(rid) {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for handler's END")
		}
	}
}

// TEST7073: Buffering collectors refuse unbounded streams with a hard error
// instead of buffering without bound. (Rust:
// test7073_collect_refuses_unbounded_streams)
func Test7073_collect_refuses_unbounded_streams(t *testing.T) {
	// Producer stays open (channel never closed, no STREAM_END/END sent) —
	// an unbounded collect would hang forever if it did not reject BEFORE
	// consuming.
	mkUnbounded := func() chan Frame {
		rid := NewMessageIdRandom()
		ch := make(chan Frame, 4)
		seqTrue := true
		ch <- *NewStreamStartUnbounded(rid, "s1", "media:enc=utf-8", &seqTrue)
		payload, err := cborlib.Marshal([]byte{1})
		if err != nil {
			t.Fatalf("cbor encode: %v", err)
		}
		ch <- *NewChunk(rid, "s1", 0, payload, 0, ComputeChecksum(payload))
		return ch
	}

	// withTimeout runs a collector call in its own goroutine so a bug that
	// fails to reject before ranging over the (never-closed) channel fails
	// this test instead of hanging the suite.
	withTimeout := func(name string, run func() error) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- run() }()
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("%s: must refuse an unbounded stream", name)
				return
			}
			if !strings.Contains(err.Error(), "unbounded") {
				t.Errorf("%s: expected an 'unbounded' error, got: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: timed out — must reject BEFORE consuming, not buffer forever", name)
		}
	}

	withTimeout("CollectFirstArg", func() error {
		_, err := CollectFirstArg(mkUnbounded())
		return err
	})
	withTimeout("CollectAllArgs", func() error {
		_, err := CollectAllArgs(mkUnbounded())
		return err
	})
	withTimeout("CollectArgsByMediaUrn", func() error {
		_, err := CollectArgsByMediaUrn(mkUnbounded(), "media:")
		return err
	})
	withTimeout("CollectStreams", func() error {
		_, err := CollectStreams(mkUnbounded())
		return err
	})
}

// TEST1300: A sequence item CBOR-encoded once and split across multiple
// CHUNK frames (the EmitListItem/emit_list_item framing) reassembles into
// exactly one delivered item carrying the first fragment's per-item
// metadata. This is the exact bug class that broke cap→cap forwarding of
// rendered page images (per-frame decoding fails with a CBOR truncation
// error on any item larger than one fragment). (Rust:
// test1300_sequence_item_fragments_reassemble_into_one_item)
func Test1300_sequence_item_fragments_reassemble_into_one_item(t *testing.T) {
	type captured struct {
		payload []byte
		meta    map[string]interface{}
	}
	var mu sync.Mutex
	var items []captured

	rt, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("failed to create runtime: %v", err)
	}
	rt.Register(testCapUrn, func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
		for frame := range frames {
			if frame.FrameType == FrameTypeChunk && frame.Payload != nil {
				mu.Lock()
				items = append(items, captured{payload: append([]byte{}, frame.Payload...), meta: frame.Meta})
				mu.Unlock()
			}
		}
		return nil
	})

	hostWriter, outFrames, _ := startTestCartridge(t, rt, DefaultInitialCredit)
	rid := NewMessageIdRandom()

	// One large item, encoded once, then fragmented — exactly what
	// EmitListItem does for an item bigger than one fragment.
	itemBytes := make([]byte, 20000)
	for i := range itemBytes {
		itemBytes[i] = byte(i % 251)
	}
	encoded, err := cborlib.Marshal(itemBytes)
	if err != nil {
		t.Fatalf("cbor encode: %v", err)
	}
	const fragmentSize = 4096
	if len(encoded) <= fragmentSize {
		t.Fatal("item must span multiple fragments")
	}

	if err := hostWriter.WriteFrame(NewReq(rid, testCapUrn, nil, "application/cbor")); err != nil {
		t.Fatalf("REQ: %v", err)
	}
	seqTrue := true
	if err := hostWriter.WriteFrame(NewStreamStart(rid, "s1", "media:ext=png;image", &seqTrue)); err != nil {
		t.Fatalf("STREAM_START: %v", err)
	}

	var frameIdx uint64
	for offset := 0; offset < len(encoded); offset += fragmentSize {
		end := offset + fragmentSize
		if end > len(encoded) {
			end = len(encoded)
		}
		fragment := encoded[offset:end]
		checksum := ComputeChecksum(fragment)
		frame := NewChunk(rid, "s1", frameIdx, fragment, frameIdx, checksum)
		if frameIdx == 0 {
			// emit_list_item puts per-item meta on the FIRST fragment only.
			frame.Meta = map[string]interface{}{"title": "page 1"}
		}
		if err := hostWriter.WriteFrame(frame); err != nil {
			t.Fatalf("CHUNK %d: %v", frameIdx, err)
		}
		frameIdx++
	}

	// A second, single-fragment item follows — reassembly must realign on
	// the item boundary, not swallow it into the first.
	second, err := cborlib.Marshal([]byte{7, 7, 7})
	if err != nil {
		t.Fatalf("cbor encode: %v", err)
	}
	if err := hostWriter.WriteFrame(NewChunk(rid, "s1", frameIdx, second, frameIdx, ComputeChecksum(second))); err != nil {
		t.Fatalf("CHUNK %d: %v", frameIdx, err)
	}
	frameIdx++

	if err := hostWriter.WriteFrame(NewStreamEnd(rid, "s1", frameIdx)); err != nil {
		t.Fatalf("STREAM_END: %v", err)
	}
	if err := hostWriter.WriteFrame(NewEnd(rid, nil)); err != nil {
		t.Fatalf("END: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case f, ok := <-outFrames:
			if !ok {
				t.Fatal("output closed before END")
			}
			if f.FrameType == FrameTypeEnd && f.Id.Equals(rid) {
				goto done
			}
		case <-deadline:
			t.Fatal("timed out waiting for handler's END")
		}
	}
done:

	mu.Lock()
	defer mu.Unlock()
	if len(items) != 2 {
		t.Fatalf("expected exactly 2 delivered items, got %d", len(items))
	}

	var v0 []byte
	if err := cborlib.Unmarshal(items[0].payload, &v0); err != nil {
		t.Fatalf("item 0 must decode as one complete CBOR value: %v", err)
	}
	if len(v0) != len(itemBytes) {
		t.Fatalf("item 0 must reassemble to the original %d-byte item, got %d bytes", len(itemBytes), len(v0))
	}
	for i := range v0 {
		if v0[i] != itemBytes[i] {
			t.Fatalf("item 0 content mismatch at byte %d", i)
			break
		}
	}
	if items[0].meta == nil || items[0].meta["title"] != "page 1" {
		t.Errorf("item 0 must carry the first fragment's meta, got %v", items[0].meta)
	}

	var v1 []byte
	if err := cborlib.Unmarshal(items[1].payload, &v1); err != nil {
		t.Fatalf("item 1 must decode as one complete CBOR value: %v", err)
	}
	if len(v1) != 3 || v1[0] != 7 || v1[1] != 7 || v1[2] != 7 {
		t.Errorf("item 1 must be [7,7,7], got %v", v1)
	}
	if len(items[1].meta) != 0 {
		t.Errorf("item 1 must carry no meta, got %v", items[1].meta)
	}
}

// TEST1301: A sequence stream that ENDs mid-item (trailing fragment bytes
// that never complete a CBOR item) surfaces a hard decode error instead of
// silently dropping the partial item. (Rust:
// test1301_sequence_stream_truncated_mid_item_fails_hard)
func Test1301_sequence_stream_truncated_mid_item_fails_hard(t *testing.T) {
	rt, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("failed to create runtime: %v", err)
	}
	rt.Register(testCapUrn, drainingHandler)

	hostWriter, outFrames, _ := startTestCartridge(t, rt, DefaultInitialCredit)
	rid := NewMessageIdRandom()

	big := make([]byte, 4096)
	for i := range big {
		big[i] = 42
	}
	encoded, err := cborlib.Marshal(big)
	if err != nil {
		t.Fatalf("cbor encode: %v", err)
	}
	// Send only a strict prefix of the item, then STREAM_END.
	payload := encoded[:len(encoded)/2]
	checksum := ComputeChecksum(payload)

	if err := hostWriter.WriteFrame(NewReq(rid, testCapUrn, nil, "application/cbor")); err != nil {
		t.Fatalf("REQ: %v", err)
	}
	seqTrue := true
	if err := hostWriter.WriteFrame(NewStreamStart(rid, "s1", "media:ext=png;image", &seqTrue)); err != nil {
		t.Fatalf("STREAM_START: %v", err)
	}
	if err := hostWriter.WriteFrame(NewChunk(rid, "s1", 0, payload, 0, checksum)); err != nil {
		t.Fatalf("CHUNK: %v", err)
	}
	if err := hostWriter.WriteFrame(NewStreamEnd(rid, "s1", 1)); err != nil {
		t.Fatalf("STREAM_END: %v", err)
	}
	if err := hostWriter.WriteFrame(NewEnd(rid, nil)); err != nil {
		t.Fatalf("END: %v", err)
	}

	deadline := time.After(3 * time.Second)
	for {
		select {
		case f, ok := <-outFrames:
			if !ok {
				t.Fatal("output closed before ERR")
			}
			if f.FrameType == FrameTypeErr && f.Id.Equals(rid) {
				msg := f.ErrorMessage()
				if !strings.Contains(msg, "mid-item") {
					t.Fatalf("expected a mid-item truncation error, got: %s", msg)
				}
				return
			}
			if f.FrameType == FrameTypeEnd && f.Id.Equals(rid) {
				t.Fatal("truncation must surface as ERR, not a successful END")
			}
		case <-deadline:
			t.Fatal("timed out waiting for ERR")
		}
	}
}

// TEST1302: Continuation fragments of a multi-frame sequence item are
// credited back as they arrive — one CREDIT grant per physical wire frame,
// not per logical item, so an item spanning more frames than the credit
// window can still finish arriving. This mirror's demux credits every
// accepted physical CHUNK (see runCBORModeIO's batched-grant bookkeeping),
// so this test asserts that item-granular reassembly does not collapse that
// per-frame accounting down to per-item. (Rust:
// test1302_sequence_fragment_frames_are_credited_on_arrival)
func Test1302_sequence_fragment_frames_are_credited_on_arrival(t *testing.T) {
	rt, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("failed to create runtime: %v", err)
	}
	rt.Register(testCapUrn, drainingHandler)

	// initial_credit=2 → creditBatch=max(2/2,1)=1: every accepted physical
	// fragment is immediately re-granted.
	hostWriter, outFrames, negotiated := startTestCartridge(t, rt, 2)
	if negotiated.InitialCredit != 2 {
		t.Fatalf("expected negotiated initial_credit 2, got %d", negotiated.InitialCredit)
	}
	rid := NewMessageIdRandom()

	itemBytes := make([]byte, 4*1024)
	for i := range itemBytes {
		itemBytes[i] = 9
	}
	encoded, err := cborlib.Marshal(itemBytes)
	if err != nil {
		t.Fatalf("cbor encode: %v", err)
	}
	fragmentSize := (len(encoded) + 3) / 4

	if err := hostWriter.WriteFrame(NewReq(rid, testCapUrn, nil, "application/cbor")); err != nil {
		t.Fatalf("REQ: %v", err)
	}
	seqTrue := true
	if err := hostWriter.WriteFrame(NewStreamStart(rid, "s1", "media:ext=png;image", &seqTrue)); err != nil {
		t.Fatalf("STREAM_START: %v", err)
	}

	var nFragments uint64
	for offset := 0; offset < len(encoded); offset += fragmentSize {
		end := offset + fragmentSize
		if end > len(encoded) {
			end = len(encoded)
		}
		fragment := encoded[offset:end]
		checksum := ComputeChecksum(fragment)
		if err := hostWriter.WriteFrame(NewChunk(rid, "s1", nFragments, fragment, nFragments, checksum)); err != nil {
			t.Fatalf("CHUNK %d: %v", nFragments, err)
		}
		nFragments++
	}
	if nFragments != 4 {
		t.Fatalf("expected 4 fragments, got %d", nFragments)
	}
	if err := hostWriter.WriteFrame(NewStreamEnd(rid, "s1", nFragments)); err != nil {
		t.Fatalf("STREAM_END: %v", err)
	}
	if err := hostWriter.WriteFrame(NewEnd(rid, nil)); err != nil {
		t.Fatalf("END: %v", err)
	}

	var grants uint64
	deadline := time.After(3 * time.Second)
	for {
		select {
		case f, ok := <-outFrames:
			if !ok {
				t.Fatal("output closed before END")
			}
			if f.FrameType == FrameTypeCredit && f.StreamId != nil && *f.StreamId == "s1" {
				if cc := f.CreditCount(); cc != nil {
					grants += *cc
				}
			}
			if f.FrameType == FrameTypeEnd && f.Id.Equals(rid) {
				goto done
			}
		case <-deadline:
			t.Fatal("timed out waiting for END")
		}
	}
done:

	if grants < nFragments-1 {
		t.Fatalf("expected at least %d fragment credits (one per physical frame), got %d", nFragments-1, grants)
	}
}
