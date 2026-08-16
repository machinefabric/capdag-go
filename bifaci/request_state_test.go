package bifaci

import (
	"encoding/json"
	"strings"
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

// rsTestInitialCredit is the ledger seed every test request negotiates —
// deliberately a small odd-sized window so seed arithmetic is visible in
// assertions.
const rsTestInitialCredit = 8

func rsState(dest int, origin *int, isPeer bool) *RequestState {
	return NewRequestState(
		RequestRoutingEntry{SourceMasterIdx: origin, DestinationMasterIdx: dest},
		origin,
		nil,
		isPeer,
		rsTestInitialCredit,
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
	// The ledger is the REMAINING WINDOW: seeded with the negotiated initial
	// credit, +4 granted, -1 per chunk in EITHER direction (the inbound chunk
	// and the outbound chunk each consumed one).
	assert.Equal(t, int64(rsTestInitialCredit)+4-2, s1.CreditOutstanding,
		"window = seed + grants - chunks")
}

// TEST8115: recently_terminated_rid discriminates the teardown race from
// genuine routing loss: true for a rid whose request just terminated, false
// for a rid the table never knew, false again once the summary is evicted
// past the ring's horizon — a pathologically late frame ages back into
// no_route, where something that stale belongs.
func Test8115_recently_terminated_rid_discriminates_and_ages_out(t *testing.T) {
	table := NewRequestTable()

	k := rsKey(1, 500)
	require.NoError(t, table.Register(k, rsState(0, nil, false)))
	assert.False(t, table.RecentlyTerminatedRid(NewMessageIdFromUint(500)),
		"a LIVE request is not recently terminated")
	require.NotNil(t, table.Terminate(k, TerminalKindEnd))
	assert.True(t, table.RecentlyTerminatedRid(NewMessageIdFromUint(500)),
		"a just-terminated rid must be in the ring")
	assert.False(t, table.RecentlyTerminatedRid(NewMessageIdFromUint(9999)),
		"an unknown rid is a genuine routing anomaly, never a benign straggler")

	// Push the ring past its horizon: rid 500's summary must age out.
	for n := uint64(1000); n < 1000+uint64(RecentTerminatedCap); n++ {
		k := rsKey(n, n)
		require.NoError(t, table.Register(k, rsState(0, nil, false)))
		require.NotNil(t, table.Terminate(k, TerminalKindEnd))
	}
	assert.False(t, table.RecentlyTerminatedRid(NewMessageIdFromUint(500)),
		"eviction past RecentTerminatedCap ends benign-straggler classification")
	assert.True(t, table.RecentlyTerminatedRid(NewMessageIdFromUint(1000+uint64(RecentTerminatedCap)-1)),
		"the newest termination is still in the ring")
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

// =============================================================================
// Admission control (mirrors Rust src/bifaci/request_state.rs tests)
// =============================================================================

func admissionTestKey() AdmissionKey {
	return AdmissionKey{
		MasterIdx:   0,
		RegistryURL: "",
		HasRegistry: false,
		Channel:     "release",
		Id:          "cartridge",
		Version:     "1.0.0",
		Sha256:      "sha",
	}
}

// TEST7110: admission is strict FIFO and a terminal request releases exactly one waiter.

func testPoolKey(install AdmissionKey, pool string) PoolKey {
	return PoolKey{Install: install, Pool: pool}
}

// allChain is the minimal chain: a cap addressed through its install's
// `all` pool.
func allChain(install AdmissionKey) []PoolKey {
	return []PoolKey{testPoolKey(install, "all")}
}

func Test7110_admission_fifo_releases_one_waiter(t *testing.T) {
	controller := NewAdmissionController()
	key := admissionTestKey()
	controller.ConfigurePools(key, map[string]uint64{"all": 1})

	first, err := controller.Acquire(allChain(key), nil)
	require.NoError(t, err)

	secondCh := make(chan *AdmissionPermit, 1)
	thirdCh := make(chan *AdmissionPermit, 1)
	go func() { p, _ := controller.Acquire(allChain(key), nil); secondCh <- p }()
	// Give the second waiter time to take its ticket before the third queues,
	// so the FIFO order under test is deterministic.
	time.Sleep(50 * time.Millisecond)
	go func() { p, _ := controller.Acquire(allChain(key), nil); thirdCh <- p }()
	time.Sleep(50 * time.Millisecond)

	select {
	case <-secondCh:
		t.Fatal("a waiter must not be admitted while the slot is held")
	case <-thirdCh:
		t.Fatal("a waiter must not be admitted while the slot is held")
	default:
	}

	first.Release()
	var second *AdmissionPermit
	select {
	case second = <-secondCh:
	case <-time.After(2 * time.Second):
		t.Fatal("second FIFO waiter must be admitted")
	}
	select {
	case <-thirdCh:
		t.Fatal("one release admits only one waiter")
	default:
	}
	second.Release()
	select {
	case <-thirdCh:
	case <-time.After(2 * time.Second):
		t.Fatal("third FIFO waiter must be admitted next")
	}
}

// TEST7111: cancelling a queued body removes its ticket; it cannot strand later ForEach bodies behind a dead queue head.
func Test7111_cancelled_admission_waiter_cannot_block_queue(t *testing.T) {
	controller := NewAdmissionController()
	key := admissionTestKey()
	controller.ConfigurePools(key, map[string]uint64{"all": 1})
	active, err := controller.Acquire(allChain(key), nil)
	require.NoError(t, err)

	cancel := make(chan struct{})
	cancelled := make(chan error, 1)
	go func() { _, err := controller.Acquire(allChain(key), cancel); cancelled <- err }()
	time.Sleep(50 * time.Millisecond)
	close(cancel)
	select {
	case err := <-cancelled:
		require.Error(t, err, "a cancelled waiter must report why it stopped waiting")
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled waiter must return")
	}

	nextCh := make(chan *AdmissionPermit, 1)
	go func() { p, _ := controller.Acquire(allChain(key), nil); nextCh <- p }()
	time.Sleep(50 * time.Millisecond)
	active.Release()
	select {
	case p := <-nextCh:
		require.NotNil(t, p, "the next body must be admitted, not stranded behind the cancelled ticket")
	case <-time.After(2 * time.Second):
		t.Fatal("the cancelled ticket stranded the queue")
	}
}

// TEST7112: the post-HELLO capacity update wakes already queued work. This is what changes an unstarted cartridge's one bootstrap slot to its authoritative runtime capacity without waiting for the first body to end.
func Test7112_capacity_reconfiguration_wakes_existing_waiters(t *testing.T) {
	controller := NewAdmissionController()
	key := admissionTestKey()
	controller.ConfigurePools(key, map[string]uint64{"all": 1})
	active, err := controller.Acquire(allChain(key), nil)
	require.NoError(t, err)

	waitingCh := make(chan *AdmissionPermit, 1)
	go func() { p, _ := controller.Acquire(allChain(key), nil); waitingCh <- p }()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-waitingCh:
		t.Fatal("a waiter must not be admitted at capacity 1 while the slot is held")
	default:
	}

	controller.ConfigurePools(key, map[string]uint64{"all": 0}) // unlimited
	select {
	case p := <-waitingCh:
		require.NotNil(t, p, "unlimited HELLO capacity must wake queued work")
	case <-time.After(2 * time.Second):
		t.Fatal("unlimited HELLO capacity must wake queued work")
	}
	active.Release()
}

// TEST7114: a cartridge that disappears and comes back does NOT terminally fail the work queued behind it. This is 17.2's "queued bodies are not assigned terminal failure from another body's process loss; once a replacement instance advertises capacity, subsequent queued work is admitted to that live instance". The regression this pins: a single failed registry-manifest fetch retired three live cartridges for ~24s, and every queued ForEach body was failed with "became unavailable while waiting for capacity" — 195 bodies lost to an outage that had already healed.
func Test7114_transient_unavailability_does_not_fail_queued_work(t *testing.T) {
	controller := NewAdmissionController()
	key := admissionTestKey()
	controller.ConfigurePools(key, map[string]uint64{"all": 1})
	active, err := controller.Acquire(allChain(key), nil)
	require.NoError(t, err)

	waitingCh := make(chan *AdmissionPermit, 1)
	errCh := make(chan error, 1)
	go func() {
		p, err := controller.Acquire(allChain(key), nil)
		waitingCh <- p
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)

	// The target vanishes from its host's inventory...
	controller.DisableMaster(key.MasterIdx)
	time.Sleep(50 * time.Millisecond)
	select {
	case <-waitingCh:
		t.Fatal("an outage inside the grace window must not fail queued work")
	default:
	}

	// ...and comes back, which is what must release the queue.
	controller.ConfigurePools(key, map[string]uint64{"all": 1})
	active.Release()
	select {
	case p := <-waitingCh:
		require.NoError(t, <-errCh)
		require.NotNil(t, p, "queued work must acquire the restored target")
	case <-time.After(2 * time.Second):
		t.Fatal("a restored admission target must admit the work queued on it")
	}
}

// TEST1943: the grace window is a BOUND, not a hang. A target that stays gone fails its queued work once the window expires, so a cartridge that is genuinely retired surfaces as a failure instead of stalling the run forever.
func Test1943_outage_outliving_the_grace_window_fails_queued_work(t *testing.T) {
	controller := NewAdmissionController()
	// Shorten the window so the expiry path is exercised without sleeping
	// through a real minute. Production uses AdmissionUnavailableGrace.
	controller.grace = 150 * time.Millisecond
	key := admissionTestKey()
	controller.ConfigurePools(key, map[string]uint64{"all": 1})
	active, err := controller.Acquire(allChain(key), nil)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() { _, err := controller.Acquire(allChain(key), nil); errCh <- err }()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-errCh:
		t.Fatal("the window must not expire early")
	default:
	}

	controller.DisableMaster(key.MasterIdx)
	select {
	case err := <-errCh:
		require.Error(t, err, "queued work must not acquire a target that never came back")
		assert.Contains(t, err.Error(), "unavailable for longer than",
			"the failure must name the outage, not a generic routing error")
	case <-time.After(3 * time.Second):
		t.Fatal("an expired grace window must wake queued work")
	}
	active.Release()
}

// TEST1524: chain admission is ATOMIC — a request is admitted only when
// EVERY pool in its chain has room, and holds all of them until release.
// A free singleton behind a full shared pool waits; releasing the shared
// pool's holder admits it.
func Test1524_chain_admission_is_atomic_across_pools(t *testing.T) {
	controller := NewAdmissionController()
	key := admissionTestKey()
	controller.ConfigurePools(key, map[string]uint64{
		"cap:a": 0, "cap:b": 0, "gpu": 1, "all": 0,
	})
	chainA := []PoolKey{testPoolKey(key, "cap:a"), testPoolKey(key, "gpu"), testPoolKey(key, "all")}
	chainB := []PoolKey{testPoolKey(key, "cap:b"), testPoolKey(key, "gpu"), testPoolKey(key, "all")}

	holder, err := controller.Acquire(chainA, nil)
	if err != nil {
		t.Fatalf("first chain must acquire: %v", err)
	}

	// cap:b's own singleton is free, but the shared "gpu" pool is full —
	// the whole chain must wait.
	admitted := make(chan *AdmissionPermit, 1)
	go func() { p, _ := controller.Acquire(chainB, nil); admitted <- p }()
	select {
	case <-admitted:
		t.Fatal("a full shared pool must block the whole chain")
	case <-time.After(100 * time.Millisecond):
	}

	holder.Release()
	select {
	case p := <-admitted:
		if p == nil {
			t.Fatal("queued chain must acquire all its pools")
		}
		p.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("releasing the shared pool must admit the queued chain")
	}
}

// TEST1525: pools are ISOLATED — saturating one cap's singleton does not
// block a different cap whose chain shares only unlimited pools.
func Test1525_disjoint_bounded_pools_admit_independently(t *testing.T) {
	controller := NewAdmissionController()
	key := admissionTestKey()
	controller.ConfigurePools(key, map[string]uint64{"cap:a": 1, "cap:b": 1, "all": 0})

	a, err := controller.Acquire([]PoolKey{testPoolKey(key, "cap:a"), testPoolKey(key, "all")}, nil)
	if err != nil {
		t.Fatalf("cap:a must acquire: %v", err)
	}
	// cap:a is saturated; cap:b must admit immediately.
	done := make(chan *AdmissionPermit, 1)
	go func() {
		p, _ := controller.Acquire([]PoolKey{testPoolKey(key, "cap:b"), testPoolKey(key, "all")}, nil)
		done <- p
	}()
	select {
	case p := <-done:
		if p == nil {
			t.Fatal("disjoint chain must acquire")
		}
		p.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("a saturated sibling singleton must not delay this cap")
	}
	a.Release()
}

// TEST1526: acquiring a chain naming a pool the install never advertised
// fails hard — an unknown pool is a protocol defect, never a free pass.
func Test1526_unknown_pool_in_chain_fails_hard(t *testing.T) {
	controller := NewAdmissionController()
	key := admissionTestKey()
	controller.ConfigurePools(key, map[string]uint64{"all": 0})
	_, err := controller.Acquire([]PoolKey{testPoolKey(key, "cap:ghost"), testPoolKey(key, "all")}, nil)
	if err == nil {
		t.Fatal("an unadvertised pool must refuse admission")
	}
	if !strings.Contains(err.Error(), "cap:ghost") {
		t.Fatalf("the failure must name the unknown pool: %v", err)
	}
}
