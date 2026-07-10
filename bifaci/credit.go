package bifaci

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// =============================================================================
// Credit-based per-stream flow control (protocol v3).
//
// One credit = permission to send one CHUNK frame. A sender starts each stream
// with the negotiated initial_credit window and must wait when the window is
// exhausted; the receiving endpoint replenishes it with CREDIT frames as it
// consumes chunks (L9/L10 in docs/capdag-improvement/03-protocol-v3-design.md).
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

// CreditGate is a replenishable per-stream credit window for one sender.
//
//   - Acquire(1) before each CHUNK: returns immediately while the window is
//     open, blocks the calling goroutine when it is exhausted.
//   - Grant(n) when a CREDIT frame arrives: wakes waiters.
//   - Close(reason) on request terminal/cancel: releases all waiters with
//     CreditClosed (L13 — a credit-blocked sender must never hang).
type CreditGate struct {
	mu          sync.Mutex
	available   uint64
	closed      bool
	closeReason string
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
		available: initialCredit,
		wake:      make(chan struct{}),
	}
}

// Acquire acquires n credits, blocking the calling goroutine if the window is
// exhausted. Returns *CreditClosed if the gate closes before (or while) waiting.
func (g *CreditGate) Acquire(n uint64) error {
	for {
		g.mu.Lock()
		if g.closed {
			reason := g.closeReason
			g.mu.Unlock()
			return &CreditClosed{Reason: reason}
		}
		if g.available >= n {
			g.available -= n
			g.mu.Unlock()
			return nil
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
	if g.closed {
		return false, &CreditClosed{Reason: g.closeReason}
	}
	if g.available >= n {
		g.available -= n
		return true, nil
	}
	return false, nil
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
	if g.closed {
		g.mu.Unlock()
		return // grants after close are no-ops
	}
	if n > math.MaxUint64-g.available {
		g.available = math.MaxUint64 // saturating add
	} else {
		g.available += n
	}
	old := g.wake
	g.wake = make(chan struct{})
	g.mu.Unlock()
	close(old)
}

// Close closes the gate: all current and future acquires fail with CreditClosed.
func (g *CreditGate) Close(reason string) {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	g.closeReason = reason
	old := g.wake
	g.wake = make(chan struct{})
	g.mu.Unlock()
	close(old)
}

// Available returns the currently available credit (diagnostic/stats).
func (g *CreditGate) Available() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.available
}

// IsClosed returns whether the gate has been closed.
func (g *CreditGate) IsClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
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

	r.mu.Lock()
	if gate, ok := r.gates[creditGateKeyFor(frame.Id, frame.StreamId)]; ok {
		r.mu.Unlock()
		gate.Grant(*credits)
		return true
	}
	var matched *CreditGate
	if frame.StreamId == nil {
		// No stream_id on the grant: match the request's sole gate if exactly one.
		ridStr := frame.Id.ToString()
		count := 0
		for key, gate := range r.gates {
			if key.rid == ridStr {
				count++
				if count > 1 {
					matched = nil
					break
				}
				matched = gate
			}
		}
		if count != 1 {
			matched = nil
		}
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
