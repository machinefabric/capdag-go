package bifaci

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	cborlib "github.com/fxamacker/cbor/v2"
)

// tryDecodeSequenceItem attempts to decode one self-delimiting CBOR item from
// the front of buf.
//
//   - ok=true, err=nil: one complete item; the returned bytes are exactly its
//     encoded bytes (their length is how many bytes were consumed from buf).
//   - ok=false, err=nil: buf holds only a prefix of an item; wait for more
//     fragment frames. CBOR definite-length encoding is prefix-free, so a
//     truncated item can never mis-decode as a complete one.
//   - err != nil: the bytes are not valid CBOR at all.
//
// Mirrors the reference's try_decode_sequence_item.
func tryDecodeSequenceItem(buf []byte) (item []byte, ok bool, err error) {
	if len(buf) == 0 {
		return nil, false, nil
	}
	var raw cborlib.RawMessage
	dec := cborlib.NewDecoder(bytes.NewReader(buf))
	decErr := dec.Decode(&raw)
	if decErr == nil {
		return []byte(raw), true, nil
	}
	if errors.Is(decErr, io.EOF) || errors.Is(decErr, io.ErrUnexpectedEOF) {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("CBOR decode error: %w", decErr)
}

// seqReassembly is per-stream reassembly state for one sequence-mode stream
// (is_sequence=true on STREAM_START). Sequence producers (EmitListItem)
// CBOR-encode each item once and split the encoded bytes across CHUNK frames
// at max_chunk boundaries — a frame payload is a raw RFC 8742 fragment, NOT a
// self-contained CBOR value. Decoding per frame fails with a CBOR truncation
// error on any item larger than max_chunk (the bug class that broke cap→cap
// forwarding of rendered page images — see TEST1300). seqReassembly buffers
// fragments and decodes at item granularity instead.
//
// Mirrors the reference's SeqReassembly.
type seqReassembly struct {
	// buf accumulates raw fragment bytes of the item currently being received.
	buf []byte
	// itemMeta is the per-item metadata carried on the item's FIRST fragment
	// frame only (the EmitListItem contract), held until the item completes.
	itemMeta map[string]interface{}
	// onFragment, if non-nil, is invoked once per continuation fragment (a
	// CHUNK frame after the current item's first fragment) as it is fed —
	// i.e. once per physical wire frame that is NOT the start of a new item.
	// Callers use this to keep credit accounting keyed on physical frames
	// even though delivery to the consumer is now item-granular (see
	// TEST1302).
	onFragment func()
}

// feed appends one CHUNK frame's raw payload to the reassembly buffer and
// invokes emit once per complete item decoded out of it (zero or more times
// — a single fragment can complete an item AND start the next, or a single
// frame can carry more than one small item). meta is that frame's per-chunk
// metadata; it is captured only when this frame starts a new item (buf was
// empty before this call), matching the EmitListItem contract of stamping
// metadata on an item's first fragment.
//
// Returns a hard error if the buffered bytes are not a valid CBOR prefix —
// never for a mere truncation, which is instead surfaced by atEnd once the
// stream closes with a non-empty buffer.
func (s *seqReassembly) feed(payload []byte, meta map[string]interface{}, emit func(itemBytes []byte, itemMeta map[string]interface{})) error {
	if len(s.buf) == 0 {
		s.itemMeta = meta
	} else if s.onFragment != nil {
		s.onFragment()
	}
	s.buf = append(s.buf, payload...)
	for {
		item, ok, err := tryDecodeSequenceItem(s.buf)
		if err != nil {
			s.buf = nil
			return err
		}
		if !ok {
			return nil
		}
		s.buf = s.buf[len(item):]
		m := s.itemMeta
		s.itemMeta = nil
		emit(item, m)
		if len(s.buf) == 0 {
			return nil
		}
	}
}

// atEnd reports a hard error if the stream ended (STREAM_END arrived) while
// the reassembly buffer still holds bytes — a sequence ending mid-item is a
// truncation and must surface as an error, never silently drop the partial
// item.
func (s *seqReassembly) atEnd() error {
	if len(s.buf) > 0 {
		return fmt.Errorf("sequence stream ended mid-item: %d trailing bytes do not form a complete CBOR item", len(s.buf))
	}
	return nil
}

// errStreamUnbounded returns the L16 refusal error for a buffering collector
// asked to consume a stream the sender declared unbounded (no length
// promise). Buffering an unbounded stream is unbounded memory; the failure
// must be explicit, never a silent OOM. Mirrors the reference's
// InputStream::check_bounded.
func errStreamUnbounded(method string) error {
	return fmt.Errorf(
		"%s refused: stream is unbounded (no length promise) — consume incrementally, not with a buffering collector (L16)",
		method)
}

// unboundedFrameChan is an effectively-unbounded, order-preserving pipe for
// Frame values: Send never blocks its caller. A background goroutine drains
// an internal, growable queue into the output channel returned by Chan.
//
// This is the load-bearing piece of the live per-request demux (replacing
// this mirror's former buffer-then-dispatch input model): the main read loop
// forwards each validated wire frame into a request's unboundedFrameChan the
// instant it arrives, and a handler goroutine (started immediately if
// capacity allows, or once a capacity slot frees) drains it — so a handler
// that is slow, or not yet dispatched at all, never blocks the read loop
// from processing frames for any OTHER request or stream. Mirrors the
// reference's use of unbounded channels at every hop of demux_multi_stream /
// demux_single_stream (tokio::sync::mpsc::unbounded_channel /
// crossbeam::unbounded) — the property preserved is that the wire reader
// never blocks on a slow or absent consumer (L16).
type unboundedFrameChan struct {
	mu     sync.Mutex
	queue  []Frame
	closed bool
	signal chan struct{}
	out    chan Frame
}

func newUnboundedFrameChan() *unboundedFrameChan {
	u := &unboundedFrameChan{
		signal: make(chan struct{}, 1),
		out:    make(chan Frame),
	}
	go u.pump()
	return u
}

func (u *unboundedFrameChan) pump() {
	for {
		u.mu.Lock()
		for len(u.queue) == 0 && !u.closed {
			u.mu.Unlock()
			<-u.signal
			u.mu.Lock()
		}
		if len(u.queue) == 0 && u.closed {
			u.mu.Unlock()
			close(u.out)
			return
		}
		f := u.queue[0]
		u.queue = u.queue[1:]
		u.mu.Unlock()
		u.out <- f
	}
}

// Send enqueues a frame without blocking the caller. A Send after Close is a
// no-op — the request has already ended.
func (u *unboundedFrameChan) Send(f Frame) {
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return
	}
	u.queue = append(u.queue, f)
	u.mu.Unlock()
	select {
	case u.signal <- struct{}{}:
	default:
	}
}

// Close signals that no more frames will be sent. Chan()'s channel closes
// once every already-queued frame has been drained. Idempotent.
func (u *unboundedFrameChan) Close() {
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return
	}
	u.closed = true
	u.mu.Unlock()
	select {
	case u.signal <- struct{}{}:
	default:
	}
}

// Chan returns the receive-only, in-order delivery channel.
func (u *unboundedFrameChan) Chan() <-chan Frame {
	return u.out
}
