package bifaci

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
	// InitialCredit is the initial per-stream credit window in CHUNK frames (protocol v3).
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

// NegotiateLimits returns the minimum of two limit sets
func NegotiateLimits(a, b Limits) Limits {
	return Limits{
		MaxFrame:         min(a.MaxFrame, b.MaxFrame),
		MaxChunk:         min(a.MaxChunk, b.MaxChunk),
		MaxReorderBuffer: min(a.MaxReorderBuffer, b.MaxReorderBuffer),
		InitialCredit:    min(a.InitialCredit, b.InitialCredit),
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
