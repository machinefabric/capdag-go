package bifaci

import (
	"fmt"
	"sync"
	"time"

	model "github.com/machinefabric/capdag-go/formal"
	lungo "github.com/machinefabric/lungo-go"
)

// =============================================================================
// Credit-based per-stream flow control (protocol v4).
//
// One credit = permission to send one CHUNK frame. A sender starts each stream
// with the negotiated initial_credit window and must wait when the window is
// exhausted; the receiving endpoint replenishes it with CREDIT frames as it
// consumes chunks (L9/L10 in the normative bifaci protocol documentation).
//
// CreditGate is deliberately built on a mutex + broadcast channel rather than
// a buffered "token channel" with a fixed capacity, because the window is
// replenishable to arbitrary values (grant(n) for any n, not just 1-at-a-time
// token production) and must support closing with an error that releases every
// waiter — semantics a Go channel-of-tokens cannot express on its own. The
// observable contract matches the Rust reference and the Swift/ObjC mirror
// exactly: acquire waits until credit is available or the gate closes; close
// releases all waiters with an error; grants never block.
// =============================================================================

// CreditClosed is returned to a credit waiter when its gate closes (request
// terminal, cancellation, or connection death) — the waiter must stop sending.
type CreditClosed struct {
	// Reason is a human-readable reason the gate closed (e.g. "CANCELLED", "END").
	Reason string
}

// Error implements the error interface.
func (e *CreditClosed) Error() string {
	return fmt.Sprintf("credit gate closed: %s", e.Reason)
}

// NegotiateInitialCredit is the credit window two ends start every stream
// with (L9): the smaller of the two proposals, decided by the proved model.
// ok is false when the window would be zero — under a zero window no chunk
// could be sent and, with nothing consumed, none would ever be granted: every
// stream would stop at its first chunk, for good.
func NegotiateInitialCredit(ours, theirs uint64) (window uint64, ok bool) {
	negotiated := decided(model.Negotiate(nat(ours), nat(theirs)))
	if !negotiated.Valid {
		return 0, false
	}
	return count(negotiated.Value), true
}

// CreditGate is a replenishable per-stream credit window for one sender.
//
//   - Acquire(1) before each CHUNK: returns immediately while the window is
//     open, blocks the calling goroutine when it is exhausted.
//   - Grant(n) when a CREDIT frame arrives: wakes waiters.
//   - Close(reason) on request terminal/cancel: releases all waiters with
//     CreditClosed (L13 — a credit-blocked sender must never hang).
//
// What an acquire, a grant and a close do to the window is the proved
// model's decision (formal/CapDAG/Bifaci/Credit.lean); the gate keeps the
// window and wakes whoever waits on it.
type CreditGate struct {
	mu    sync.Mutex
	state model.Gate
	// wake is closed (and replaced with a fresh channel) by Grant and Close to
	// broadcast to every goroutine parked in Acquire. A waiter captures the
	// current channel value BEFORE releasing the lock, so a grant/close that
	// lands between the window check and the wait can never be missed — the
	// same "register interest under the lock" discipline the Rust reference's
	// notified().enable() and the Swift mirror's continuation registration
	// closure both rely on.
	wake chan struct{}
}

// NewCreditGate creates a CreditGate with the given initial credit window.
func NewCreditGate(initialCredit uint64) *CreditGate {
	return &CreditGate{
		state: decided(model.GateOpened(nat(initialCredit))),
		wake:  make(chan struct{}),
	}
}

// acquireLocked asks the model for n credits. Caller must hold g.mu.
func (g *CreditGate) acquireLocked(n uint64) (bool, error) {
	switch answer := decided(model.GateAcquire(g.state, nat(n))).(type) {
	case model.AcquireAcquired:
		g.state = answer.Gate
		return true, nil
	case model.AcquireWait:
		return false, nil
	case model.AcquireClosed:
		return false, &CreditClosed{Reason: answer.Reason}
	default:
		panic(fmt.Sprintf("BUG: unknown answer to an acquire: %T", answer))
	}
}

// Acquire acquires n credits, blocking the calling goroutine if the window is
// exhausted. Returns *CreditClosed if the gate closes before (or while) waiting.
func (g *CreditGate) Acquire(n uint64) error {
	for {
		g.mu.Lock()
		acquired, err := g.acquireLocked(n)
		if err != nil || acquired {
			g.mu.Unlock()
			return err
		}
		wake := g.wake
		g.mu.Unlock()
		<-wake
	}
}

// TryAcquire is a non-waiting acquire. Returns false when the window is
// exhausted. Returns *CreditClosed if the gate is closed.
func (g *CreditGate) TryAcquire(n uint64) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.acquireLocked(n)
}

// BlockingAcquire is a blocking acquire for non-goroutine-friendly contexts
// (FFI callback threads). Spins on TryAcquire with a short park; the park
// interval is invisible to the protocol (only wall-clock throughput of a
// blocked sender).
func (g *CreditGate) BlockingAcquire(n uint64) error {
	for {
		ok, err := g.TryAcquire(n)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Grant replenishes the window by n chunks and wakes all waiters. Grants
// after close are no-ops.
func (g *CreditGate) Grant(n uint64) {
	g.mu.Lock()
	_ = n
	old := g.wake
	g.wake = make(chan struct{})
	g.mu.Unlock()
	close(old)
}

// Close closes the gate: all current and future acquires fail with CreditClosed.
func (g *CreditGate) Close(reason string) {
	g.mu.Lock()
	g.state = decided(model.Close(g.state, reason))
	old := g.wake
	g.wake = make(chan struct{})
	g.mu.Unlock()
	close(old)
}

// Available returns the currently available credit (diagnostic/stats).
func (g *CreditGate) Available() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return saturated(g.state.Available)
}

// IsClosed returns whether the gate has been closed.
func (g *CreditGate) IsClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state.Closed.Valid
}

// =============================================================================
// CreditWindow is the receiving end of one stream's credit window: what is
// left of what the sender was granted, and what this end has consumed and not
// yet granted back.
//
//   - Arrive() for each CHUNK: false is a CREDIT_VIOLATION — the sender sent
//     past its window (L12).
//   - Consumed() once the chunk is consumed: the grant that is now due, 0 when
//     the batch has not built up yet (L10: half the window, at least 1).
//   - Flush() when nothing more will be consumed for a while: whatever is
//     pending is granted, so a sender never waits on a batch that will not
//     fill.
//   - Continued() for a chunk that only continues an item: granted back at
//     once, since nothing can consume it before the item is whole.
//
// Every decision is the proved model's (formal/CapDAG/Bifaci/Credit.lean).
// =============================================================================
type CreditWindow struct {
	mu    sync.Mutex
	state model.Window
}

// NewCreditWindow opens a window of the negotiated initial credit.
func NewCreditWindow(initialCredit uint64) *CreditWindow {
	return &CreditWindow{state: decided(model.WindowOpened(nat(initialCredit)))}
}

// Arrive accounts for one arriving CHUNK. False: the chunk is beyond the
// granted window, and the window is unchanged.
func (w *CreditWindow) Arrive() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch arrival := decided(model.WindowArrive(w.state)).(type) {
	case model.CreditArrivalAccepted:
		w.state = arrival.Window
		return true
	case model.CreditArrivalViolation:
		return false
	default:
		panic(fmt.Sprintf("BUG: unknown answer to an arrival: %T", arrival))
	}
}

func (w *CreditWindow) granted(step model.Granted) uint64 {
	w.state = step.Window
	return count(step.Grant)
}

// Consumed accounts for one consumed chunk and returns the grant now due (0:
// none yet).
func (w *CreditWindow) Consumed() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.granted(decided(model.Consume(w.state)))
}

// Flush returns the grant for everything consumed and not yet granted (0:
// nothing is pending).
func (w *CreditWindow) Flush() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.granted(decided(model.Flush(w.state)))
}

// Continued accounts for a chunk that continues an item and returns the grant
// that gives it back at once.
func (w *CreditWindow) Continued() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.granted(decided(model.Continued(w.state)))
}

// Remaining is how many more chunks the sender may send before a grant.
func (w *CreditWindow) Remaining() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return saturated(w.state.Remaining)
}

// Pending is how many chunks were consumed and not yet granted back.
func (w *CreditWindow) Pending() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return count(w.state.Pending)
}

// =============================================================================
// CreditRouter routes inbound CREDIT frames to the gates of the streams they
// credit.
//
// Keyed by (rid, stream_id). A CREDIT frame with no stream_id credits the
// request's sole/default stream: it matches the request's single registered
// gate when exactly one exists.
// =============================================================================

// creditGateKey is the (rid, stream_id) key CreditRouter indexes gates by.
// hasStream distinguishes "no stream_id" (nil) from an empty-string stream_id,
// matching Rust's Option<String> exactly.
type creditGateKey struct {
	rid       string
	hasStream bool
	streamID  string
}

func creditGateKeyFor(rid MessageId, streamID *string) creditGateKey {
	if streamID == nil {
		return creditGateKey{rid: rid.ToString()}
	}
	return creditGateKey{rid: rid.ToString(), hasStream: true, streamID: *streamID}
}

// CreditRouter routes inbound CREDIT frames to the gates of the streams they
// credit. Safe for concurrent use.
type CreditRouter struct {
	mu    sync.Mutex
	gates map[creditGateKey]*CreditGate
}

// NewCreditRouter creates an empty CreditRouter.
func NewCreditRouter() *CreditRouter {
	return &CreditRouter{gates: make(map[creditGateKey]*CreditGate)}
}

// Register registers a gate for a stream a local sender is about to write.
func (r *CreditRouter) Register(rid MessageId, streamID *string, gate *CreditGate) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gates[creditGateKeyFor(rid, streamID)] = gate
}

// CloseRequest removes and closes every gate belonging to a request
// (terminal/cancel). Waiters blocked on those gates are released with
// CreditClosed (L13).
func (r *CreditRouter) CloseRequest(rid MessageId, reason string) {
	ridStr := rid.ToString()
	r.mu.Lock()
	var toClose []*CreditGate
	for key, gate := range r.gates {
		if key.rid == ridStr {
			toClose = append(toClose, gate)
			delete(r.gates, key)
		}
	}
	r.mu.Unlock()
	for _, gate := range toClose {
		gate.Close(reason)
	}
}

// Grant delivers a CREDIT frame's grant to the matching gate. Returns false
// when no gate matches (request finished or the sender is not
// credit-registered) — a correct no-op, since grants only unblock.
func (r *CreditRouter) Grant(frame *Frame) bool {
	if frame.FrameType != FrameTypeCredit {
		return false
	}
	credits := frame.CreditCount()
	if credits == nil {
		return false
	}

	// Which of the request's streams the grant is for is the model's
	// decision: the one it names, or — naming none — the only one there is.
	ridStr := frame.Id.ToString()
	named := lungo.None[string]()
	if frame.StreamId != nil {
		named = lungo.Some(*frame.StreamId)
	}
	r.mu.Lock()
	var streams []lungo.Option[string]
	for key := range r.gates {
		if key.rid != ridStr {
			continue
		}
		if key.hasStream {
			streams = append(streams, lungo.Some(key.streamID))
		} else {
			streams = append(streams, lungo.None[string]())
		}
	}
	var matched *CreditGate
	if target := decided(model.GrantTarget(streams, named)); target.Valid {
		key := creditGateKey{rid: ridStr}
		if target.Value.Valid {
			key = creditGateKey{rid: ridStr, hasStream: true, streamID: target.Value.Value}
		}
		matched = r.gates[key]
	}
	r.mu.Unlock()

	if matched == nil {
		return false
	}
	matched.Grant(*credits)
	return true
}

// Len returns the number of registered gates (diagnostic/stats).
func (r *CreditRouter) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.gates)
}

// IsEmpty returns whether no gates are registered.
func (r *CreditRouter) IsEmpty() bool {
	return r.Len() == 0
}
