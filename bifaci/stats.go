package bifaci

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// =============================================================================
// Protocol observability primitives shared by every bifaci runtime.
//
// DropCounters is the L8 substrate: every frame a runtime drops increments
// exactly one DropReason counter — frames are never dropped silently. The
// counters are lock-free atomics so they can be bumped from writer
// goroutines, relay switch goroutines, and blocking contexts alike, and
// snapshot into JSON-serializable structs for the protocol stats surfaces.
// (matches Rust bifaci::stats)
// =============================================================================

// dropReasonIndex maps a DropReason to its slot in the counters array.
// Mirrors the Rust DropCounters::idx helper (linear scan over
// DropReason::ALL); an unmatched reason is an internal invariant violation,
// not a runtime condition callers can recover from.
func dropReasonIndex(reason DropReason) int {
	for i, r := range DropReasonAll {
		if r == reason {
			return i
		}
	}
	panic(fmt.Sprintf("BUG: DropReasonAll does not cover reason %d", uint8(reason)))
}

// dropReasonCount must equal len(DropReasonAll); checked in init() below so
// the fixed counters array (matching the Rust [AtomicU64; DropReason::ALL.len()]
// layout) can never silently drift from DropReasonAll.
const dropReasonCount = 6

func init() {
	if len(DropReasonAll) != dropReasonCount {
		panic(fmt.Sprintf(
			"BUG: DropReasonAll has %d entries, dropReasonCount const says %d — update stats.go",
			len(DropReasonAll), dropReasonCount,
		))
	}
}

// DropCounters holds per-reason dropped-frame counters (L8). Cheap to bump,
// snapshot on demand. Zero value is not usable; construct with
// NewDropCounters. (matches Rust DropCounters)
type DropCounters struct {
	counters [dropReasonCount]atomic.Uint64
}

// NewDropCounters creates a zeroed DropCounters. (matches Rust DropCounters::new)
func NewDropCounters() *DropCounters {
	return &DropCounters{}
}

// Record records one dropped frame. Returns the new total for that reason.
// (matches Rust DropCounters::record)
func (dc *DropCounters) Record(reason DropReason) uint64 {
	return dc.counters[dropReasonIndex(reason)].Add(1)
}

// Get returns the current count for one reason. (matches Rust DropCounters::get)
func (dc *DropCounters) Get(reason DropReason) uint64 {
	return dc.counters[dropReasonIndex(reason)].Load()
}

// Total returns the total drops across all reasons. (matches Rust DropCounters::total)
func (dc *DropCounters) Total() uint64 {
	var total uint64
	for i := range dc.counters {
		total += dc.counters[i].Load()
	}
	return total
}

// Snapshot returns a serializable snapshot keyed by the stable snake_case
// reason names — the field-name contract mirrors replicate. Zero-count
// reasons are omitted. (matches Rust DropCounters::snapshot)
func (dc *DropCounters) Snapshot() DropSnapshot {
	byReason := make(map[string]uint64)
	var total uint64
	for i, reason := range DropReasonAll {
		count := dc.counters[i].Load()
		total += count
		if count > 0 {
			byReason[reason.AsStr()] = count
		}
	}
	return DropSnapshot{
		Total:    total,
		ByReason: byReason,
	}
}

// DropSnapshot is a serializable view of the drop counters.
// (matches Rust DropSnapshot)
type DropSnapshot struct {
	Total uint64 `json:"total"`
	// ByReason maps reason name (snake_case) to count; zero-count reasons omitted.
	ByReason map[string]uint64 `json:"by_reason"`
}

// =============================================================================
// TERMINATED FLOWS — writer-side terminal gate (L4)
//
// After a flow's END/ERR is written, any later flow frame for the same
// FlowKey is post-terminal: it is dropped and counted instead of written.
// The set is capacity-bounded FIFO — with seq state already removed at the
// terminal, an evicted entry can only readmit a straggler that the receiving
// side's reorder/routing layers then reject; the cap bounds memory on
// long-lived cartridges, it does not change protocol correctness.
// (matches Rust TerminatedFlows)
// =============================================================================

// TerminatedFlows is the terminated-flow set for the writer-side terminal
// gate (L4). Safe for concurrent use.
type TerminatedFlows struct {
	mu    sync.Mutex
	order []FlowKey
	set   map[FlowKey]struct{}
	cap   int
}

// NewTerminatedFlows creates a TerminatedFlows bounded to cap entries.
// cap must be positive. (matches Rust TerminatedFlows::new)
func NewTerminatedFlows(cap int) *TerminatedFlows {
	if cap <= 0 {
		panic("BUG: TerminatedFlows cap must be positive")
	}
	return &TerminatedFlows{
		order: make([]FlowKey, 0, cap),
		set:   make(map[FlowKey]struct{}, cap),
		cap:   cap,
	}
}

// Insert marks a flow terminated. Evicts the oldest entry at capacity.
// Duplicate inserts are a no-op. (matches Rust TerminatedFlows::insert)
func (tf *TerminatedFlows) Insert(key FlowKey) {
	tf.mu.Lock()
	defer tf.mu.Unlock()
	if _, ok := tf.set[key]; ok {
		return
	}
	if len(tf.order) == tf.cap {
		oldest := tf.order[0]
		tf.order = tf.order[1:]
		delete(tf.set, oldest)
	}
	tf.order = append(tf.order, key)
	tf.set[key] = struct{}{}
}

// Contains reports whether this flow has already seen its terminal frame.
// (matches Rust TerminatedFlows::contains)
func (tf *TerminatedFlows) Contains(key FlowKey) bool {
	tf.mu.Lock()
	defer tf.mu.Unlock()
	_, ok := tf.set[key]
	return ok
}

// Len returns the number of tracked terminated flows.
// (matches Rust TerminatedFlows::len)
func (tf *TerminatedFlows) Len() int {
	tf.mu.Lock()
	defer tf.mu.Unlock()
	return len(tf.set)
}

// IsEmpty reports whether no flows are tracked as terminated.
// (matches Rust TerminatedFlows::is_empty)
func (tf *TerminatedFlows) IsEmpty() bool {
	return tf.Len() == 0
}
