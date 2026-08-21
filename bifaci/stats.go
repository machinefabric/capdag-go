package bifaci

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// =============================================================================
// Protocol observability primitives shared by every bifaci runtime.
//
// Two counter families, deliberately distinct because they mean opposite
// things:
//
//   - DropCounters is the L8 substrate for frames lost to something going
//     WRONG: every dropped frame increments exactly one DropReason ×
//     FrameType counter — frames are never dropped silently, and a non-zero
//     drop total is always worth investigating.
//   - StragglerCounters counts the benign teardown crossing: flow frames
//     that arrive after their request's terminal, which the protocol expects
//     (in-flight frames legally race END/ERR). Stragglers are moot by
//     protocol — nothing went wrong, no data was lost — and every stats
//     surface indicates them as benign, never as drops or failures.
//
// All counters are lock-free atomics so they can be bumped from writer
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
// the fixed counters array (matching the Rust [[AtomicU64; ..]; ..] layout)
// can never silently drift from DropReasonAll.
const dropReasonCount = 5

// frameTypeCount must equal len(FrameTypeAll); checked in init() below.
const frameTypeCount = 14

func init() {
	if len(DropReasonAll) != dropReasonCount {
		panic(fmt.Sprintf(
			"BUG: DropReasonAll has %d entries, dropReasonCount const says %d — update stats.go",
			len(DropReasonAll), dropReasonCount,
		))
	}
	if len(FrameTypeAll) != frameTypeCount {
		panic(fmt.Sprintf(
			"BUG: FrameTypeAll has %d entries, frameTypeCount const says %d — update stats.go",
			len(FrameTypeAll), frameTypeCount,
		))
	}
}

// frameTypeIndex maps a FrameType to its slot in the counters arrays.
func frameTypeIndex(frameType FrameType) int {
	for i, t := range FrameTypeAll {
		if t == frameType {
			return i
		}
	}
	panic(fmt.Sprintf("BUG: FrameTypeAll does not cover frame type %d", uint8(frameType)))
}

// DropCounters holds per-reason × per-frame-type dropped-frame counters
// (L8). Cheap to bump, snapshot on demand. Drops mean something went wrong —
// the benign post-terminal case is NOT recorded here (see
// StragglerCounters). Zero value is not usable; construct with
// NewDropCounters. (matches Rust DropCounters)
type DropCounters struct {
	counters [dropReasonCount][frameTypeCount]atomic.Uint64
}

// NewDropCounters creates a zeroed DropCounters. (matches Rust DropCounters::new)
func NewDropCounters() *DropCounters {
	return &DropCounters{}
}

// Record records one dropped frame of the given type. Returns the new total
// for that reason (across frame types). (matches Rust DropCounters::record)
func (dc *DropCounters) Record(reason DropReason, frameType FrameType) uint64 {
	dc.counters[dropReasonIndex(reason)][frameTypeIndex(frameType)].Add(1)
	return dc.Get(reason)
}

// Get returns the current count for one reason, summed across frame types.
// (matches Rust DropCounters::get)
func (dc *DropCounters) Get(reason DropReason) uint64 {
	var total uint64
	row := &dc.counters[dropReasonIndex(reason)]
	for i := range row {
		total += row[i].Load()
	}
	return total
}

// GetFrame returns the current count for one (reason, frame type) cell.
// (matches Rust DropCounters::get_frame)
func (dc *DropCounters) GetFrame(reason DropReason, frameType FrameType) uint64 {
	return dc.counters[dropReasonIndex(reason)][frameTypeIndex(frameType)].Load()
}

// Total returns the total drops across all reasons. (matches Rust DropCounters::total)
func (dc *DropCounters) Total() uint64 {
	var total uint64
	for _, reason := range DropReasonAll {
		total += dc.Get(reason)
	}
	return total
}

// Snapshot returns a serializable snapshot keyed by the stable snake_case
// reason names — the field-name contract mirrors replicate. ByReason carries
// per-reason totals; ByReasonFrameType breaks each reason down by the
// dropped frame's type. Zero-count entries are omitted from both.
// (matches Rust DropCounters::snapshot)
func (dc *DropCounters) Snapshot() DropSnapshot {
	byReason := make(map[string]uint64)
	byReasonFrameType := make(map[string]map[string]uint64)
	var total uint64
	for _, reason := range DropReasonAll {
		count := dc.Get(reason)
		total += count
		if count > 0 {
			byReason[reason.AsStr()] = count
			byFrame := make(map[string]uint64)
			for _, frameType := range FrameTypeAll {
				cell := dc.GetFrame(reason, frameType)
				if cell > 0 {
					byFrame[frameType.AsStr()] = cell
				}
			}
			byReasonFrameType[reason.AsStr()] = byFrame
		}
	}
	return DropSnapshot{
		Total:             total,
		ByReason:          byReason,
		ByReasonFrameType: byReasonFrameType,
	}
}

// DropSnapshot is a serializable view of the drop counters.
// (matches Rust DropSnapshot)
type DropSnapshot struct {
	Total uint64 `json:"total"`
	// ByReason maps reason name (snake_case) to count; zero-count reasons omitted.
	ByReason map[string]uint64 `json:"by_reason"`
	// ByReasonFrameType maps reason name to (frame type name → count); zero
	// cells omitted. Present reasons mirror ByReason.
	ByReasonFrameType map[string]map[string]uint64 `json:"by_reason_frame_type,omitempty"`
}

// StragglerCounters holds per-frame-type counters for BENIGN post-terminal
// stragglers.
//
// A straggler is a flow frame that arrives after its request's terminal
// (END/ERR) — the ordinary, protocol-legal teardown crossing (L13): a callee
// may END before draining its input, a final CREDIT grant may cross the
// terminal in flight. Nothing went wrong and no data was lost; the frame is
// simply moot. Counted per frame type so surfaces can say exactly what
// crossed ("late credit" vs "late chunk") — and always indicated as benign,
// never as a drop or failure. (matches Rust StragglerCounters)
type StragglerCounters struct {
	counters [frameTypeCount]atomic.Uint64
}

// NewStragglerCounters creates a zeroed StragglerCounters.
// (matches Rust StragglerCounters::new)
func NewStragglerCounters() *StragglerCounters {
	return &StragglerCounters{}
}

// Record records one benign post-terminal straggler. Returns the new total.
// (matches Rust StragglerCounters::record)
func (sc *StragglerCounters) Record(frameType FrameType) uint64 {
	sc.counters[frameTypeIndex(frameType)].Add(1)
	return sc.Total()
}

// Get returns the current count for one frame type.
// (matches Rust StragglerCounters::get)
func (sc *StragglerCounters) Get(frameType FrameType) uint64 {
	return sc.counters[frameTypeIndex(frameType)].Load()
}

// Total returns the total stragglers across all frame types.
// (matches Rust StragglerCounters::total)
func (sc *StragglerCounters) Total() uint64 {
	var total uint64
	for i := range sc.counters {
		total += sc.counters[i].Load()
	}
	return total
}

// Snapshot returns a serializable snapshot keyed by the stable snake_case
// frame-type names; zero-count types omitted.
// (matches Rust StragglerCounters::snapshot)
func (sc *StragglerCounters) Snapshot() StragglerSnapshot {
	byFrameType := make(map[string]uint64)
	var total uint64
	for i, frameType := range FrameTypeAll {
		count := sc.counters[i].Load()
		total += count
		if count > 0 {
			byFrameType[frameType.AsStr()] = count
		}
	}
	return StragglerSnapshot{
		Total:       total,
		ByFrameType: byFrameType,
	}
}

// StragglerSnapshot is a serializable view of the straggler counters —
// benign by definition. (matches Rust StragglerSnapshot)
type StragglerSnapshot struct {
	Total uint64 `json:"total"`
	// ByFrameType maps frame type name (snake_case) to count; zero-count
	// types omitted.
	ByFrameType map[string]uint64 `json:"by_frame_type"`
}

// =============================================================================
// TERMINATED FLOWS — writer-side terminal gate (L4)
//
// After a flow's END/ERR is written, any later flow frame for the same
// FlowKey is a benign post-terminal straggler: it is suppressed and counted
// as such (never a drop) instead of written.
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
