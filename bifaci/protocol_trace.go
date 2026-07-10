package bifaci

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// =============================================================================
// Per-segment protocol trace sink for the reference runtime.
//
// The engine's dev trace (machfab's protocol_trace.go) samples a LONG-LIVED
// relay switch every 2s and writes transition-deduped JSONL. The capdag CLI
// runtime reuses a long-lived switch too, but the trace here is scoped PER
// SEGMENT: the segment executor both SAMPLES the switch live during the
// segment (a periodic sampler) and captures a final SNAPSHOT at teardown —
// every line carries the switch's RelaySwitchProtocolStats, the same
// information the Protocol Health view shows. Live sampling is what makes a
// HANGING segment observable: the last line written before the harness kills
// it shows the stalled active request with its per-stream credit/flow
// counters.
//
// Line schema (JSONL, one object per line):
//
//	{ "ts": <unix millis>, "segment": <label>, "stats": <RelaySwitchProtocolStats> }
//
// Lines are deduped by a transition fingerprint that EXCLUDES ever-advancing
// clocks (ages/idle/lifetime), so an idle or stalled engine does not spam
// identical samples — one line per protocol transition, mirroring machfab's
// trace_fingerprint.
//
// This is diagnostics the user explicitly asked for (a --trace/env path): the
// FINAL snapshot's serialize and I/O errors are HARD errors surfaced to the
// caller. A LIVE sample's write failure is the caller's responsibility to log
// and ignore — Record/RecordDeduped never silently drop a requested write; a
// mid-run trace hiccup that must not abort execution is a decision the
// segment executor makes at the call site, not something this sink hides.
// =============================================================================

// ProtocolTraceErrorKind categorises a ProtocolTraceError. Both Io and
// Serialize are hard errors: the trace was requested, so a write that cannot
// happen is reported, never dropped.
type ProtocolTraceErrorKind int

const (
	// ProtocolTraceErrorIo: the trace file could not be opened or written.
	ProtocolTraceErrorIo ProtocolTraceErrorKind = iota
	// ProtocolTraceErrorSerialize: the snapshot could not be serialized to JSON.
	ProtocolTraceErrorSerialize
	// ProtocolTraceErrorClock: the system clock is before the Unix epoch
	// (cannot timestamp the line). Structurally present for parity with the
	// Rust reference's SystemTimeError variant; Go's clock APIs do not
	// themselves fail, so this sink derives the same condition explicitly
	// (a negative Unix-millis reading) rather than silently encoding a
	// nonsensical timestamp.
	ProtocolTraceErrorClock
)

// ProtocolTraceError is a failure to write a protocol trace line.
// (matches Rust ProtocolTraceError)
type ProtocolTraceError struct {
	Kind ProtocolTraceErrorKind
	// Message carries the description for ProtocolTraceErrorClock, which has
	// no underlying stdlib error value.
	Message string
	// Err is the underlying cause for Io and Serialize.
	Err error
}

// Error implements the error interface.
func (e *ProtocolTraceError) Error() string {
	switch e.Kind {
	case ProtocolTraceErrorIo:
		return fmt.Sprintf("protocol trace I/O error: %v", e.Err)
	case ProtocolTraceErrorSerialize:
		return fmt.Sprintf("protocol trace serialize error: %v", e.Err)
	case ProtocolTraceErrorClock:
		return fmt.Sprintf("protocol trace clock error: %s", e.Message)
	default:
		return fmt.Sprintf("protocol trace error: %s", e.Message)
	}
}

// Unwrap exposes the underlying I/O or serialize error, if any.
func (e *ProtocolTraceError) Unwrap() error {
	return e.Err
}

// traceLine is one JSONL trace line. A dedicated struct (rather than an
// ad-hoc map) so a stats-serialization failure surfaces as a real error.
type traceLine struct {
	// Ts is the capture time, Unix milliseconds.
	Ts uint64 `json:"ts"`
	// Segment identifies the segment this snapshot belongs to (e.g. the
	// terminal cap URN).
	Segment string `json:"segment"`
	// Stats is the switch's full protocol snapshot for the segment.
	Stats *RelaySwitchProtocolStats `json:"stats"`
}

// ProtocolTraceSink is an append-only JSONL sink for per-segment protocol
// snapshots. Safe to share across goroutines (e.g. between the live sampler
// and the final-snapshot writer) — the dedup check and the write are guarded
// by one mutex so they stay atomic under concurrent callers.
// (matches Rust ProtocolTraceSink)
type ProtocolTraceSink struct {
	mu   sync.Mutex
	file *os.File
	// lastFingerprint is the fingerprint of the last line actually written;
	// nil before the first.
	lastFingerprint *string
}

// traceFingerprint computes the transition fingerprint: everything the
// snapshot says that MATTERS, EXCLUDING the ever-advancing clocks (a
// request's age_ms/idle_ms, a termination's lifetime_ms) which change every
// sample and would defeat dedup. Mirrors machfab's protocol_trace.go
// trace_fingerprint so both traces dedup on the same notion of "a protocol
// transition".
func traceFingerprint(stats *RelaySwitchProtocolStats) string {
	active := make([]map[string]any, 0, len(stats.Requests.Active))
	for _, r := range stats.Requests.Active {
		streams := make([]map[string]any, 0, len(r.Streams))
		for _, s := range r.Streams {
			streams = append(streams, map[string]any{
				"id":        s.StreamId,
				"fi":        s.Stats.FramesIn,
				"fo":        s.Stats.FramesOut,
				"bi":        s.Stats.BytesIn,
				"bo":        s.Stats.BytesOut,
				"credit":    s.Stats.CreditOutstanding,
				"unbounded": s.Stats.Unbounded,
				"ended":     s.Stats.Ended,
			})
		}
		active = append(active, map[string]any{
			"rid":      r.Rid,
			"cap":      r.CapUrn,
			"phase":    r.Phase,
			"children": r.Children,
			"streams":  streams,
		})
	}

	var lastTerminated any
	if n := len(stats.Requests.RecentTerminated); n > 0 {
		t := stats.Requests.RecentTerminated[n-1]
		lastTerminated = []any{t.Rid, t.Kind.AsStr()}
	}

	fingerprint := map[string]any{
		"total_registered":   stats.Requests.TotalRegistered,
		"terminated_by_kind": stats.Requests.TerminatedByKind,
		"terminated_len":     len(stats.Requests.RecentTerminated),
		"last_terminated":    lastTerminated,
		"drops":              stats.Drops,
		"hosts":              stats.Hosts,
		"active":             active,
	}
	// Every value above is a basic type, a slice/map of basic types, or an
	// existing snapshot struct with a total MarshalJSON — Marshal cannot fail
	// for this shape (no channels, funcs, or cycles). Mirrors the Rust
	// reference, where trace_fingerprint's serde_json::Value::to_string() is
	// likewise infallible.
	buf, _ := json.Marshal(fingerprint)
	return string(buf)
}

// OpenProtocolTraceSink opens path for append, creating it if absent. A
// failure to open (bad directory, no permission) is a hard error — the
// caller asked for a trace. (matches Rust ProtocolTraceSink::open)
func OpenProtocolTraceSink(path string) (*ProtocolTraceSink, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, &ProtocolTraceError{Kind: ProtocolTraceErrorIo, Err: err}
	}
	return &ProtocolTraceSink{file: file}, nil
}

// writeLine appends one JSONL line { ts, segment, stats }. The trace must be
// complete on disk even if the process is killed right after a failing
// segment; os.File.Write issues a direct, unbuffered syscall, so a successful
// return means the bytes left the process. Caller holds s.mu.
func (s *ProtocolTraceSink) writeLine(stats *RelaySwitchProtocolStats, segmentLabel string) error {
	millis := time.Now().UnixMilli()
	if millis < 0 {
		return &ProtocolTraceError{
			Kind:    ProtocolTraceErrorClock,
			Message: "system clock is before the Unix epoch",
		}
	}
	line := traceLine{
		Ts:      uint64(millis),
		Segment: segmentLabel,
		Stats:   stats,
	}
	buf, err := json.Marshal(&line)
	if err != nil {
		return &ProtocolTraceError{Kind: ProtocolTraceErrorSerialize, Err: err}
	}
	buf = append(buf, '\n')
	if _, err := s.file.Write(buf); err != nil {
		return &ProtocolTraceError{Kind: ProtocolTraceErrorIo, Err: err}
	}
	return nil
}

// Record appends one line unconditionally (no dedup). Serialize, clock, and
// I/O failures are returned to the caller: this is requested diagnostics,
// and a silently dropped line would hide the very problem the trace exposes.
// (matches Rust ProtocolTraceSink::record)
func (s *ProtocolTraceSink) Record(stats *RelaySwitchProtocolStats, segmentLabel string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeLine(stats, segmentLabel); err != nil {
		return err
	}
	// Keep the fingerprint coherent so a later RecordDeduped compares against
	// what is actually on disk.
	fp := traceFingerprint(stats)
	s.lastFingerprint = &fp
	return nil
}

// RecordDeduped appends one line ONLY when the protocol state changed since
// the last line written — so an idle or stalled engine leaves the trace
// silent instead of spamming identical samples. The fingerprint check and
// the write share s.mu, so concurrent samplers cannot interleave a
// duplicate. (matches Rust ProtocolTraceSink::record_deduped)
func (s *ProtocolTraceSink) RecordDeduped(stats *RelaySwitchProtocolStats, segmentLabel string) error {
	fingerprint := traceFingerprint(stats)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastFingerprint != nil && *s.lastFingerprint == fingerprint {
		return nil
	}
	if err := s.writeLine(stats, segmentLabel); err != nil {
		return err
	}
	s.lastFingerprint = &fingerprint
	return nil
}

// Close closes the underlying trace file. Go has no destructor equivalent to
// the Rust reference's Drop-on-scope-exit, so callers that open a sink must
// close it explicitly (e.g. via defer) once the segment finishes tracing.
func (s *ProtocolTraceSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
