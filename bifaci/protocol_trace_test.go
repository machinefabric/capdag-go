package bifaci

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func emptyProtocolTraceStats(totalRegistered uint64) *RelaySwitchProtocolStats {
	return &RelaySwitchProtocolStats{
		Requests: RequestTableSnapshot{
			Active:           []RequestSnapshot{},
			RecentTerminated: []TerminatedSummary{},
			TotalRegistered:  totalRegistered,
			TerminatedByKind: map[string]uint64{},
		},
		Drops: NewDropCounters().Snapshot(),
		Hosts: map[string]HostProtocolStats{},
	}
}

// activeProtocolTraceStats builds a snapshot with one active request, so
// age/idle clocks are present to test that the fingerprint ignores them
// while flow counters are significant.
func activeProtocolTraceStats(ageMs, idleMs, bytesIn uint64) *RelaySwitchProtocolStats {
	streamID := "in"
	capURN := "cap:effect=none"
	return &RelaySwitchProtocolStats{
		Requests: RequestTableSnapshot{
			Active: []RequestSnapshot{
				{
					Xid:               "1",
					Rid:               "9",
					Phase:             RequestPhaseStreaming,
					IsPeer:            false,
					CapUrn:            &capURN,
					OriginMaster:      nil,
					DestinationMaster: 0,
					AgeMs:             ageMs,
					IdleMs:            idleMs,
					Children:          0,
					Streams: []StreamSnapshot{
						{
							StreamId: &streamID,
							Stats: StreamFlowStats{
								BytesIn: bytesIn,
							},
						},
					},
				},
			},
			RecentTerminated: []TerminatedSummary{},
			TotalRegistered:  1,
			TerminatedByKind: map[string]uint64{},
		},
		Drops: NewDropCounters().Snapshot(),
		Hosts: map[string]HostProtocolStats{},
	}
}

func protocolTraceTempPath(t *testing.T, tag string) string {
	t.Helper()
	return filepath.Join(os.TempDir(), fmt.Sprintf(
		"capdag-go-protocol-trace-%s-%d-%d.trace",
		tag, os.Getpid(), time.Now().UnixNano(),
	))
}

// TEST1312: Two snapshots recorded to a temp file produce exactly two JSONL
// lines, each carrying ts + segment + a round-tripped stats object
// (requests/drops).
func Test1312_record_appends_one_json_line_per_snapshot(t *testing.T) {
	path := protocolTraceTempPath(t, "roundtrip")
	sink, err := OpenProtocolTraceSink(path)
	require.NoError(t, err, "open sink")
	defer func() {
		sink.Close()
		os.Remove(path)
	}()

	require.NoError(t, sink.Record(emptyProtocolTraceStats(1), "seg-a"), "record 1")
	require.NoError(t, sink.Record(emptyProtocolTraceStats(2), "seg-b"), "record 2")

	contents, err := os.ReadFile(path)
	require.NoError(t, err, "read trace back")

	lines := strings.Split(strings.TrimRight(string(contents), "\n"), "\n")
	require.Len(t, lines, 2, "one JSONL line per recorded snapshot")

	var first map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first), "line 1 is JSON")
	_, tsIsNumber := first["ts"].(float64)
	assert.True(t, tsIsNumber, "ts is a unix-millis integer")
	assert.Equal(t, "seg-a", first["segment"])
	firstStats := first["stats"].(map[string]any)
	firstRequests := firstStats["requests"].(map[string]any)
	assert.Equal(t, float64(1), firstRequests["total_registered"])
	_, hasDrops := firstStats["drops"].(map[string]any)
	assert.True(t, hasDrops, "stats carries the requests + drops snapshots")

	var second map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &second), "line 2 is JSON")
	assert.Equal(t, "seg-b", second["segment"])
	secondStats := second["stats"].(map[string]any)
	secondRequests := secondStats["requests"].(map[string]any)
	assert.Equal(t, float64(2), secondRequests["total_registered"])
}

// TEST1313: Dedup: recording identical protocol state twice writes ONE line;
// a real change (a bumped counter, a moved stream byte) writes another. This
// is what keeps a stalled engine's repeated live samples from spamming the
// trace.
func Test1313_record_deduped_writes_only_on_change(t *testing.T) {
	path := protocolTraceTempPath(t, "dedup")
	sink, err := OpenProtocolTraceSink(path)
	require.NoError(t, err, "open sink")
	defer func() {
		sink.Close()
		os.Remove(path)
	}()

	require.NoError(t, sink.RecordDeduped(emptyProtocolTraceStats(1), "seg"), "first")
	// Identical state — must NOT write a second line.
	require.NoError(t, sink.RecordDeduped(emptyProtocolTraceStats(1), "seg"), "dup")
	// Changed counter — must write.
	require.NoError(t, sink.RecordDeduped(emptyProtocolTraceStats(2), "seg"), "changed")
	// A stream flow-counter change is also a transition.
	require.NoError(t, sink.RecordDeduped(activeProtocolTraceStats(10, 0, 512), "seg"), "active")

	contents, err := os.ReadFile(path)
	require.NoError(t, err, "read trace back")
	lines := strings.Split(strings.TrimRight(string(contents), "\n"), "\n")
	assert.Len(t, lines, 3, "identical samples dedup to one line; each real change adds one")
}

// TEST1314: The fingerprint EXCLUDES advancing clocks: two snapshots
// differing only in age_ms/idle_ms are the same transition, while a
// flow-counter change is a new one. If dedup keyed on the whole serialized
// stats, these clocks would defeat it and every sample would write.
func Test1314_fingerprint_ignores_advancing_clocks(t *testing.T) {
	a := activeProtocolTraceStats(1000, 10, 512)
	b := activeProtocolTraceStats(9000, 8010, 512) // only age/idle advanced
	assert.Equal(t, traceFingerprint(a), traceFingerprint(b),
		"age/idle advancement alone is not a transition")

	c := activeProtocolTraceStats(9000, 0, 1024) // bytes moved
	assert.NotEqual(t, traceFingerprint(a), traceFingerprint(c),
		"a flow-counter change is a transition")
}

// TEST1315: Requested diagnostics fail HARD, never silently: a write to an
// unwritable sink returns an error. /dev/full opens fine but every write is
// ENOSPC — the Linux-standard way to exercise a write failure
// deterministically.
func Test1315_record_to_unwritable_path_is_a_hard_error(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/dev/full is a Linux-specific ENOSPC device")
	}
	sink, err := OpenProtocolTraceSink("/dev/full")
	require.NoError(t, err, "/dev/full opens for append")
	defer sink.Close()

	err = sink.Record(emptyProtocolTraceStats(1), "seg")
	require.Error(t, err, "writing to /dev/full must fail, not silently drop")

	var traceErr *ProtocolTraceError
	require.ErrorAs(t, err, &traceErr)
	assert.Equal(t, ProtocolTraceErrorIo, traceErr.Kind,
		"an unwritable trace surfaces as an I/O error, got %v", err)
}
