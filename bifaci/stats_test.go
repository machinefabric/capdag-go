package bifaci

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TEST7019: Drop counters record per-reason exactly once per drop, and the
// snapshot omits zero-count reasons while totalling all of them.
func Test7019_drop_counters_record_and_snapshot(t *testing.T) {
	counters := NewDropCounters()
	assert.Equal(t, uint64(0), counters.Total())
	assert.Equal(t, DropSnapshot{Total: 0, ByReason: map[string]uint64{}}, counters.Snapshot())

	assert.Equal(t, uint64(1), counters.Record(DropReasonPostTerminal))
	assert.Equal(t, uint64(2), counters.Record(DropReasonPostTerminal))
	assert.Equal(t, uint64(1), counters.Record(DropReasonChannelClosed))

	assert.Equal(t, uint64(2), counters.Get(DropReasonPostTerminal))
	assert.Equal(t, uint64(1), counters.Get(DropReasonChannelClosed))
	assert.Equal(t, uint64(0), counters.Get(DropReasonNoRoute))
	assert.Equal(t, uint64(3), counters.Total())

	snap := counters.Snapshot()
	assert.Equal(t, uint64(3), snap.Total)
	assert.Equal(t, uint64(2), snap.ByReason["post_terminal"])
	assert.Equal(t, uint64(1), snap.ByReason["channel_closed"])
	_, hasNoRoute := snap.ByReason["no_route"]
	assert.False(t, hasNoRoute, "zero-count reasons are omitted from the snapshot")
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
