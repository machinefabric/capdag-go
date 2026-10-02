package bifaci

import "fmt"

// DefaultMaxReorderBuffer is the default reorder buffer size (64 slots)
const DefaultMaxReorderBuffer int = 64

// DefaultInitialCredit is the default initial per-stream credit window in
// CHUNK frames. A sender may emit this many CHUNKs per stream before it must
// wait for a CREDIT grant. 32 chunks ≈ 8 MiB at the default max_chunk (256 KiB).
// (matches Rust DEFAULT_INITIAL_CREDIT)
const DefaultInitialCredit int = 32

// Limits represents protocol negotiation limits
type Limits struct {
	MaxFrame         int `cbor:"max_frame"`
	MaxChunk         int `cbor:"max_chunk"`
	MaxReorderBuffer int `cbor:"max_reorder_buffer"`
	// InitialCredit is the initial per-stream credit window in CHUNK frames (protocol v4).
	InitialCredit int `cbor:"initial_credit"`
}

// DefaultLimits returns the default protocol limits
func DefaultLimits() Limits {
	return Limits{
		MaxFrame:         DefaultMaxFrame,
		MaxChunk:         DefaultMaxChunk,
		MaxReorderBuffer: DefaultMaxReorderBuffer,
		InitialCredit:    DefaultInitialCredit,
	}
}

// NegotiateLimits returns the limits two ends share: the smaller of each
// proposal. The credit window is the proved model's decision
// (NegotiateInitialCredit): a window that negotiates to zero is refused,
// because no stream could ever move under it.
func NegotiateLimits(a, b Limits) (Limits, error) {
	if a.InitialCredit < 0 || b.InitialCredit < 0 {
		return Limits{}, fmt.Errorf(
			"protocol violation: initial_credit is negative (ours %d, theirs %d)",
			a.InitialCredit, b.InitialCredit)
	}
	window, ok := NegotiateInitialCredit(uint64(a.InitialCredit), uint64(b.InitialCredit))
	if !ok {
		return Limits{}, fmt.Errorf(
			"protocol violation: initial_credit negotiates to zero (ours %d, theirs %d) — a stream needs a window of at least one chunk",
			a.InitialCredit, b.InitialCredit)
	}
	return Limits{
		MaxFrame:         min(a.MaxFrame, b.MaxFrame),
		MaxChunk:         min(a.MaxChunk, b.MaxChunk),
		MaxReorderBuffer: min(a.MaxReorderBuffer, b.MaxReorderBuffer),
		InitialCredit:    int(window),
	}, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
