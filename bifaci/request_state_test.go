package bifaci

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// (test helpers mirror the Rust reference's `key`/`state` helpers in
// src/bifaci/request_state.rs's #[cfg(test)] module)

func rsKey(x, r uint64) RequestKey {
	return NewRequestKey(NewMessageIdFromUint(x), NewMessageIdFromUint(r))
}

func rsState(dest int, origin *int, isPeer bool) *RequestState {
	return NewRequestState(
		RequestRoutingEntry{SourceMasterIdx: origin, DestinationMasterIdx: dest},
		origin,
		nil,
		isPeer,
	)
}

func intPtr(n int) *int {
	return &n
}

func strPtr(s string) *string {
	return &s
}

// TEST7092: A request registered with its originating REQ's cap URN carries
// that identity through the ACTIVE snapshot and into the terminated ring —
// observability surfaces can always NAME a request (background chatter vs
// run traffic), never just show a bare rid. A request registered without one
// (pre-attribution mirror, unknown origin) snapshots with cap_urn null —
// absent, never invented.
func Test7092_cap_urn_attribution_survives_lifecycle(t *testing.T) {
	table := NewRequestTable()
	named := rsKey(1, 9)
	require.NoError(t, table.Register(named, rsState(0, intPtr(1), false).WithCapUrn(strPtr("cap:effect=none"))))
	anonymous := rsKey(2, 10)
	require.NoError(t, table.Register(anonymous, rsState(0, intPtr(1), true)))

	snapshot := table.Snapshot()
	byRid := func(rid string) *RequestSnapshot {
		for i := range snapshot.Active {
			if snapshot.Active[i].Rid == rid {
				return &snapshot.Active[i]
			}
		}
		t.Fatalf("no active snapshot for rid %s", rid)
		return nil
	}
	require.NotNil(t, byRid("9").CapUrn, "active snapshot names the request's cap")
	assert.Equal(t, "cap:effect=none", *byRid("9").CapUrn)
	assert.Nil(t, byRid("10").CapUrn, "unknown identity stays absent")

	require.NotNil(t, table.Terminate(named, TerminalKindEnd))
	snapshot = table.Snapshot()
	require.Len(t, snapshot.RecentTerminated, 1)
	require.NotNil(t, snapshot.RecentTerminated[0].CapUrn, "the terminated ring keeps the cap identity")
	assert.Equal(t, "cap:effect=none", *snapshot.RecentTerminated[0].CapUrn)
}

// TEST7087: Protocol stats snapshots serialize with stable field names — the
// snapshot shape is the mirror contract.
func Test7087_snapshot_field_names_are_stable(t *testing.T) {
	table := NewRequestTable()
	k := rsKey(1, 9)
	require.NoError(t, table.Register(k, rsState(0, intPtr(1), true)))
	rid := NewMessageIdFromUint(9)
	ss := NewStreamStart(rid, "s", "media:enc=utf-8", boolPtr(false))
	table.RecordFrame(k, FrameDirectionInbound, ss)

	raw, err := json.Marshal(table.Snapshot())
	require.NoError(t, err)
	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &doc))
	for _, field := range []string{"active", "recent_terminated", "total_registered", "terminated_by_kind"} {
		_, ok := doc[field]
		assert.True(t, ok, "missing top-level field %s", field)
	}

	active := doc["active"].([]interface{})
	require.Len(t, active, 1)
	req := active[0].(map[string]interface{})
	for _, field := range []string{
		"xid", "rid", "phase", "is_peer", "origin_master", "destination_master",
		"age_ms", "idle_ms", "children", "streams",
	} {
		_, ok := req[field]
		assert.True(t, ok, "missing request field %s", field)
	}
	assert.Equal(t, "streaming", req["phase"], "phase serializes snake_case")

	streams := req["streams"].([]interface{})
	require.Len(t, streams, 1)
	stream := streams[0].(map[string]interface{})
	for _, field := range []string{
		"stream_id", "frames_in", "frames_out", "bytes_in", "bytes_out",
		"chunks_in", "chunks_out", "credit_outstanding", "unbounded", "ended",
	} {
		_, ok := stream[field]
		assert.True(t, ok, "missing stream field %s", field)
	}

	require.NotNil(t, table.Terminate(k, TerminalKindMasterDied))
	raw, err = json.Marshal(table.Snapshot())
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &doc))
	recent := doc["recent_terminated"].([]interface{})
	require.Len(t, recent, 1)
	summary := recent[0].(map[string]interface{})
	for _, field := range []string{
		"xid", "rid", "kind", "is_peer", "lifetime_ms",
		"frames_in", "frames_out", "bytes_in", "bytes_out",
	} {
		_, ok := summary[field]
		assert.True(t, ok, "missing summary field %s", field)
	}
	assert.Equal(t, "master_died", summary["kind"], "kind serializes snake_case")
}

// TEST7088: last_activity is monotonic non-decreasing across a long-lived
// streaming request — idle time resets on every recorded frame and never
// runs backwards.
func Test7088_last_activity_monotonic(t *testing.T) {
	table := NewRequestTable()
	k := rsKey(1, 5)
	require.NoError(t, table.Register(k, rsState(0, nil, false)))
	rid := NewMessageIdFromUint(5)

	var lastActivityPoints []time.Time
	for i := uint64(0); i < 3; i++ {
		time.Sleep(15 * time.Millisecond)
		payload := make([]byte, 4)
		checksum := ComputeChecksum(payload)
		chunk := NewChunk(rid, "s", i, payload, i, checksum)
		table.RecordFrame(k, FrameDirectionInbound, chunk)
		entry := table.Get(k)
		require.NotNil(t, entry)
		assert.False(t, entry.LastActivity.Before(entry.CreatedAt), "activity never precedes creation")
		lastActivityPoints = append(lastActivityPoints, entry.LastActivity)
	}
	for i := 1; i < len(lastActivityPoints); i++ {
		assert.False(t, lastActivityPoints[i].Before(lastActivityPoints[i-1]), "last_activity must be monotonic non-decreasing")
	}
	// idle_ms in the snapshot reflects the LAST activity, not the first: it
	// must be (much) smaller than the request's age.
	time.Sleep(15 * time.Millisecond)
	snap := table.Snapshot()
	require.Len(t, snap.Active, 1)
	req := snap.Active[0]
	assert.LessOrEqual(t, req.IdleMs, req.AgeMs, "idle %dms cannot exceed age %dms", req.IdleMs, req.AgeMs)
	assert.GreaterOrEqual(t, req.AgeMs, uint64(45), "age accumulates across the request lifetime")
}

// TEST7030: A request registers exactly once and terminates exactly once —
// duplicate registration and double termination are rejected, and after
// terminate zero state remains for the key.
func Test7030_register_once_terminate_once(t *testing.T) {
	table := NewRequestTable()
	k := rsKey(1, 100)

	require.NoError(t, table.Register(k, rsState(0, nil, false)))
	assert.True(t, table.Contains(k))
	xid, ok := table.XidForRid(NewMessageIdFromUint(100))
	require.True(t, ok)
	assert.True(t, xid.Equals(NewMessageIdFromUint(1)))

	// Duplicate registration of a live key is a protocol violation.
	err := table.Register(k, rsState(0, nil, false))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")

	// Same RID under a different XID is rejected while live.
	err = table.Register(rsKey(2, 100), rsState(0, nil, false))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already indexed")

	removed := table.Terminate(k, TerminalKindEnd)
	require.NotNil(t, removed, "live entry")
	assert.False(t, removed.IsPeer)
	assert.False(t, table.Contains(k), "no entry remains after terminate")
	_, ok = table.XidForRid(NewMessageIdFromUint(100))
	assert.False(t, ok, "rid index removed with the entry (L7)")
	assert.Nil(t, table.Terminate(k, TerminalKindEnd), "termination happens exactly once")
}

// TEST7031: The rid index and the entry table never disagree across
// register/terminate cycles, and a terminated rid is immediately reusable.
func Test7031_rid_index_consistency(t *testing.T) {
	table := NewRequestTable()
	for round := uint64(0); round < 3; round++ {
		for n := uint64(0); n < 10; n++ {
			k := rsKey(round*100+n, n)
			require.NoError(t, table.Register(k, rsState(0, nil, false)))
		}
		for n := uint64(0); n < 10; n++ {
			k := rsKey(round*100+n, n)
			xid, ok := table.XidForRid(NewMessageIdFromUint(n))
			require.True(t, ok, "indexed")
			assert.True(t, xid.Equals(k.Xid), "index resolves to the live entry's xid")
			assert.True(t, table.Contains(NewRequestKey(xid, NewMessageIdFromUint(n))))
			require.NotNil(t, table.Terminate(k, TerminalKindEnd))
			_, ok = table.XidForRid(NewMessageIdFromUint(n))
			assert.False(t, ok)
		}
	}
	assert.True(t, table.IsEmpty())
	assert.Equal(t, uint64(30), table.Snapshot().TotalRegistered)
}

// TEST7032: RecordFrame accumulates per-stream frame/byte/chunk counters by
// direction, flips phase Created->Streaming on the first flow frame, and
// tracks unbounded/ended/credit stream markers.
func Test7032_record_frame_stats_and_phase(t *testing.T) {
	table := NewRequestTable()
	k := rsKey(1, 7)
	require.NoError(t, table.Register(k, rsState(0, nil, false)))
	assert.Equal(t, RequestPhaseCreated, table.Get(k).Phase)

	rid := NewMessageIdFromUint(7)
	ss := NewStreamStartUnbounded(rid, "s1", "media:enc=utf-8", nil)
	table.RecordFrame(k, FrameDirectionInbound, ss)
	assert.Equal(t, RequestPhaseStreaming, table.Get(k).Phase)

	payload := make([]byte, 100)
	checksum := ComputeChecksum(payload)
	chunk := NewChunk(rid, "s1", 0, payload, 0, checksum)
	table.RecordFrame(k, FrameDirectionInbound, chunk)
	table.RecordFrame(k, FrameDirectionOutbound, chunk)

	credit := NewCredit(rid, strPtr("s1"), 4, CreditDirectionResponse)
	table.RecordFrame(k, FrameDirectionOutbound, credit)

	se := NewStreamEndUnbounded(rid, "s1")
	table.RecordFrame(k, FrameDirectionInbound, se)

	entry := table.Get(k)
	require.NotNil(t, entry)
	s1, ok := entry.Streams[StreamKey{Present: true, ID: "s1"}]
	require.True(t, ok)
	assert.Equal(t, uint64(3), s1.FramesIn, "stream_start + chunk + stream_end")
	assert.Equal(t, uint64(2), s1.FramesOut, "chunk + credit")
	assert.Equal(t, uint64(1), s1.ChunksIn)
	assert.Equal(t, uint64(1), s1.ChunksOut)
	assert.Equal(t, uint64(100), s1.BytesIn)
	assert.Equal(t, uint64(100), s1.BytesOut)
	assert.True(t, s1.Unbounded)
	assert.True(t, s1.Ended)
	// +4 granted, -1 consumed inbound chunk
	assert.Equal(t, int64(3), s1.CreditOutstanding)
}

// TEST7033: Terminated requests leave a bounded ring of summaries carrying
// kind, lifetime, and flow totals, and the ring evicts oldest-first at
// capacity.
func Test7033_terminated_summaries_ring(t *testing.T) {
	table := NewRequestTable()
	for n := uint64(0); n < uint64(RecentTerminatedCap)+3; n++ {
		k := rsKey(n, n)
		require.NoError(t, table.Register(k, rsState(0, intPtr(2), true)))
		payload := make([]byte, 10)
		checksum := ComputeChecksum(payload)
		chunk := NewChunk(NewMessageIdFromUint(n), "s", 0, payload, 0, checksum)
		table.RecordFrame(k, FrameDirectionInbound, chunk)
		require.NotNil(t, table.Terminate(k, TerminalKindCancelled))
	}
	snap := table.Snapshot()
	require.Len(t, snap.RecentTerminated, RecentTerminatedCap)
	// Oldest evicted: first retained summary is rid "3"
	assert.Equal(t, NewMessageIdFromUint(3).ToString(), snap.RecentTerminated[0].Rid)
	last := snap.RecentTerminated[len(snap.RecentTerminated)-1]
	assert.Equal(t, TerminalKindCancelled, last.Kind)
	assert.True(t, last.IsPeer)
	assert.Equal(t, uint64(1), last.FramesIn)
	assert.Equal(t, uint64(10), last.BytesIn)
	assert.Equal(t, uint64(RecentTerminatedCap)+3, snap.TerminatedByKind["cancelled"])
}

func boolPtr(b bool) *bool {
	return &b
}
