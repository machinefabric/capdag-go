package bifaci

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TEST7019: Drop counters record per-reason × per-frame-type exactly once
// per drop; the snapshot totals all of them, breaks each reason down by
// frame type, and omits zero-count entries.
func Test7019_drop_counters_record_and_snapshot(t *testing.T) {
	counters := NewDropCounters()
	assert.Equal(t, uint64(0), counters.Total())
	snapEmpty := counters.Snapshot()
	assert.Equal(t, uint64(0), snapEmpty.Total)
	assert.Empty(t, snapEmpty.ByReason)
	assert.Empty(t, snapEmpty.ByReasonFrameType)

	assert.Equal(t, uint64(1), counters.Record(DropReasonNoRoute, FrameTypeChunk))
	assert.Equal(t, uint64(2), counters.Record(DropReasonNoRoute, FrameTypeCredit))
	assert.Equal(t, uint64(1), counters.Record(DropReasonChannelClosed, FrameTypeLog))

	assert.Equal(t, uint64(2), counters.Get(DropReasonNoRoute))
	assert.Equal(t, uint64(1), counters.Get(DropReasonChannelClosed))
	assert.Equal(t, uint64(0), counters.Get(DropReasonCancelled))
	assert.Equal(t, uint64(1), counters.GetFrame(DropReasonNoRoute, FrameTypeChunk))
	assert.Equal(t, uint64(1), counters.GetFrame(DropReasonNoRoute, FrameTypeCredit))
	assert.Equal(t, uint64(0), counters.GetFrame(DropReasonNoRoute, FrameTypeEnd))
	assert.Equal(t, uint64(3), counters.Total())

	snap := counters.Snapshot()
	assert.Equal(t, uint64(3), snap.Total)
	assert.Equal(t, uint64(2), snap.ByReason["no_route"])
	assert.Equal(t, uint64(1), snap.ByReason["channel_closed"])
	_, hasCancelled := snap.ByReason["cancelled"]
	assert.False(t, hasCancelled, "zero-count reasons are omitted from the snapshot")
	noRoute := snap.ByReasonFrameType["no_route"]
	assert.Equal(t, uint64(1), noRoute["chunk"])
	assert.Equal(t, uint64(1), noRoute["credit"])
	_, hasEnd := noRoute["end"]
	assert.False(t, hasEnd, "zero-count frame types are omitted from the breakdown")
}

// TEST8127: Straggler counters — the benign post-terminal category is
// separate from drops, counted per frame type, and its snapshot names what
// crossed the terminal (late credit vs late chunk) while omitting zero-count
// types.
func Test8127_straggler_counters_record_and_snapshot(t *testing.T) {
	stragglers := NewStragglerCounters()
	assert.Equal(t, uint64(0), stragglers.Total())
	snapEmpty := stragglers.Snapshot()
	assert.Equal(t, uint64(0), snapEmpty.Total)
	assert.Empty(t, snapEmpty.ByFrameType)

	assert.Equal(t, uint64(1), stragglers.Record(FrameTypeCredit))
	assert.Equal(t, uint64(2), stragglers.Record(FrameTypeCredit))
	assert.Equal(t, uint64(3), stragglers.Record(FrameTypeChunk))

	assert.Equal(t, uint64(2), stragglers.Get(FrameTypeCredit))
	assert.Equal(t, uint64(1), stragglers.Get(FrameTypeChunk))
	assert.Equal(t, uint64(0), stragglers.Get(FrameTypeEnd))

	snap := stragglers.Snapshot()
	assert.Equal(t, uint64(3), snap.Total)
	assert.Equal(t, uint64(2), snap.ByFrameType["credit"])
	assert.Equal(t, uint64(1), snap.ByFrameType["chunk"])
	_, hasEnd := snap.ByFrameType["end"]
	assert.False(t, hasEnd, "zero-count frame types are omitted from the snapshot")
}

// TEST7029: TerminatedFlows membership is exact up to capacity and evicts
// strictly oldest-first beyond it.
func Test7029_terminated_flows_capacity_and_eviction(t *testing.T) {
	flows := NewTerminatedFlows(2)
	k := func(n uint64) FlowKey {
		return FlowKey{rid: NewMessageIdFromUint(n).ToString(), xid: ""}
	}

	flows.Insert(k(1))
	flows.Insert(k(1)) // duplicate insert is a no-op
	flows.Insert(k(2))
	assert.Equal(t, 2, flows.Len())
	assert.True(t, flows.Contains(k(1)) && flows.Contains(k(2)))

	flows.Insert(k(3)) // evicts k(1), the oldest
	assert.Equal(t, 2, flows.Len())
	assert.False(t, flows.Contains(k(1)))
	assert.True(t, flows.Contains(k(2)) && flows.Contains(k(3)))

	// XID-bearing key is a distinct flow from the bare-RID key
	withXid := FlowKey{rid: NewMessageIdFromUint(2).ToString(), xid: NewMessageIdFromUint(9).ToString()}
	assert.False(t, flows.Contains(withXid))
}
