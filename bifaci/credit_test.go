package bifaci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST7015: CreditGate acquire succeeds immediately within the initial window and waits when exhausted until a grant arrives.
func Test7015_credit_gate_acquire_and_grant(t *testing.T) {
	gate := NewCreditGate(2)
	require.NoError(t, gate.Acquire(1))
	require.NoError(t, gate.Acquire(1))
	assert.Equal(t, uint64(0), gate.Available())

	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- gate.Acquire(1)
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-waiterDone:
		t.Fatal("acquire must wait at zero credit")
	default:
	}

	gate.Grant(1)
	select {
	case err := <-waiterDone:
		require.NoError(t, err, "waiter must wake on grant")
	case <-time.After(time.Second):
		t.Fatal("waiter must wake on grant")
	}
}

// TEST7016: CreditGate close releases blocked waiters with CreditClosed and fails all future acquires.
func Test7016_credit_gate_close_releases_waiters(t *testing.T) {
	gate := NewCreditGate(0)
	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- gate.Acquire(1)
	}()
	time.Sleep(50 * time.Millisecond)

	gate.Close("CANCELLED")
	var waiterErr error
	select {
	case waiterErr = <-waiterDone:
	case <-time.After(time.Second):
		t.Fatal("waiter must wake on close")
	}
	require.Error(t, waiterErr)
	closedErr, ok := waiterErr.(*CreditClosed)
	require.True(t, ok, "expected *CreditClosed, got %T", waiterErr)
	assert.Equal(t, "CANCELLED", closedErr.Reason)

	require.Error(t, gate.Acquire(1), "closed gate rejects acquire")
	gate.Grant(5) // no-op after close
	require.Error(t, gate.Acquire(1))
}

// TEST7017: CreditRouter routes grants by (rid, stream_id), falls back to a request's sole gate for stream-less grants, and reports unmatched grants.
func Test7017_credit_router_routing(t *testing.T) {
	router := NewCreditRouter()
	rid := NewMessageIdRandom()
	gate := NewCreditGate(0)
	s1 := "s1"
	router.Register(rid, &s1, gate)

	// Exact (rid, stream) match
	f := NewCredit(rid, &s1, 3, CreditDirectionResponse)
	assert.True(t, router.Grant(f))
	assert.Equal(t, uint64(3), gate.Available())

	// Stream-less grant matches the sole gate
	f = NewCredit(rid, nil, 2, CreditDirectionResponse)
	assert.True(t, router.Grant(f))
	assert.Equal(t, uint64(5), gate.Available())

	// Second gate makes a stream-less grant ambiguous -> unmatched
	gate2 := NewCreditGate(0)
	s2 := "s2"
	router.Register(rid, &s2, gate2)
	f = NewCredit(rid, nil, 1, CreditDirectionResponse)
	assert.False(t, router.Grant(f))

	// Unknown request -> unmatched no-op
	f = NewCredit(NewMessageIdRandom(), nil, 1, CreditDirectionResponse)
	assert.False(t, router.Grant(f))
}

// TEST7018: CreditRouter close_request closes and removes every gate of the request, releasing their waiters.
func Test7018_credit_router_close_request(t *testing.T) {
	router := NewCreditRouter()
	rid := NewMessageIdRandom()
	g1 := NewCreditGate(0)
	g2 := NewCreditGate(0)
	a := "a"
	b := "b"
	router.Register(rid, &a, g1)
	router.Register(rid, &b, g2)

	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- g1.Acquire(1)
	}()
	time.Sleep(50 * time.Millisecond)

	router.CloseRequest(rid, "END")
	assert.True(t, router.IsEmpty())
	assert.True(t, g2.IsClosed())

	var waiterErr error
	select {
	case waiterErr = <-waiterDone:
	case <-time.After(time.Second):
		t.Fatal("waiter must wake on close_request")
	}
	require.Error(t, waiterErr)
	closedErr, ok := waiterErr.(*CreditClosed)
	require.True(t, ok, "expected *CreditClosed, got %T", waiterErr)
	assert.Equal(t, "END", closedErr.Reason)
}
