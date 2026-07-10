package bifaci

// =============================================================================
// Unified per-request state for routing runtimes (protocol v3, L7/L8).
//
// One RequestState per in-flight request replaces the parallel routing maps
// (routing entry, origin, peer markers, parent->child links, response
// channel, rid->xid index) that previously had to be mutated consistently by
// hand. Registration and termination are single operations: a request is
// registered once and terminated once (End | Err | Cancelled | MasterDied);
// after Terminate returns, zero state for the key remains (L7).
//
// The table is also the observability substrate: per-stream flow counters,
// phase tracking, and a bounded ring of recently-terminated summaries feed
// the protocol stats snapshots (L8) without retaining routing state.
//
// (matches Rust src/bifaci/request_state.rs)
// =============================================================================

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// RequestKey is (XID, RID) — the unique key of a routed request.
// MessageId is not comparable (it carries a byte slice), so RequestKey
// cannot be used directly as a Go map key; RequestTable derives a string key
// via RequestKey.mapKey, mirroring the FlowKey pattern already established
// in frame_helpers.go/frame.go.
type RequestKey struct {
	Xid MessageId
	Rid MessageId
}

// NewRequestKey builds a RequestKey from its XID and RID.
func NewRequestKey(xid, rid MessageId) RequestKey {
	return RequestKey{Xid: xid, Rid: rid}
}

// mapKey derives the comparable string form of the key used to index the
// table's internal maps.
func (k RequestKey) mapKey() string {
	return k.Xid.ToString() + "\x1f" + k.Rid.ToString()
}

// RequestRoutingEntry is where a request came from and where it is going, as
// master indices. Named RequestRoutingEntry (not RoutingEntry) because the
// bifaci Go package is a single flat namespace and relay_switch.go already
// declares an unrelated (pre-v3) RoutingEntry for its own request routing
// map; Rust's module system lets request_state::RoutingEntry and
// relay_switch::RoutingEntry coexist, Go's flat package cannot.
// (matches Rust request_state::RoutingEntry)
type RequestRoutingEntry struct {
	// SourceMasterIdx is the master the request arrived from (nil = external
	// caller / engine).
	SourceMasterIdx *int
	// DestinationMasterIdx is the master the request was dispatched to.
	DestinationMasterIdx int
}

// TerminalKind is how a request's lifecycle ended. (matches Rust TerminalKind)
type TerminalKind uint8

const (
	TerminalKindEnd TerminalKind = iota
	TerminalKindErr
	TerminalKindCancelled
	TerminalKindMasterDied
)

// AsStr returns the stable snake_case name (matches Rust TerminalKind::as_str).
func (k TerminalKind) AsStr() string {
	switch k {
	case TerminalKindEnd:
		return "end"
	case TerminalKindErr:
		return "err"
	case TerminalKindCancelled:
		return "cancelled"
	case TerminalKindMasterDied:
		return "master_died"
	default:
		panic(fmt.Sprintf("BUG: TerminalKind %d not covered by AsStr", uint8(k)))
	}
}

// String implements fmt.Stringer.
func (k TerminalKind) String() string { return k.AsStr() }

// MarshalJSON serializes as the stable snake_case name — the snapshot shape
// is the mirror contract (TEST7087).
func (k TerminalKind) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.AsStr())
}

// UnmarshalJSON parses the stable snake_case name.
func (k *TerminalKind) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "end":
		*k = TerminalKindEnd
	case "err":
		*k = TerminalKindErr
	case "cancelled":
		*k = TerminalKindCancelled
	case "master_died":
		*k = TerminalKindMasterDied
	default:
		return fmt.Errorf("unknown TerminalKind %q", s)
	}
	return nil
}

// RequestPhase is the live phase of a request. Terminated never appears in
// the active table — termination removes the entry (L7) and leaves a
// TerminatedSummary in the recent ring instead. (matches Rust RequestPhase)
type RequestPhase uint8

const (
	// RequestPhaseCreated: registered; no flow frames observed yet.
	RequestPhaseCreated RequestPhase = iota
	// RequestPhaseStreaming: at least one flow frame has moved through the runtime.
	RequestPhaseStreaming
)

// AsStr returns the stable snake_case name.
func (p RequestPhase) AsStr() string {
	switch p {
	case RequestPhaseCreated:
		return "created"
	case RequestPhaseStreaming:
		return "streaming"
	default:
		panic(fmt.Sprintf("BUG: RequestPhase %d not covered by AsStr", uint8(p)))
	}
}

// String implements fmt.Stringer.
func (p RequestPhase) String() string { return p.AsStr() }

// MarshalJSON serializes as the stable snake_case name.
func (p RequestPhase) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.AsStr())
}

// UnmarshalJSON parses the stable snake_case name.
func (p *RequestPhase) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "created":
		*p = RequestPhaseCreated
	case "streaming":
		*p = RequestPhaseStreaming
	default:
		return fmt.Errorf("unknown RequestPhase %q", s)
	}
	return nil
}

// FrameDirection is the direction of a recorded frame relative to this
// runtime. (matches Rust FrameDirection)
type FrameDirection uint8

const (
	FrameDirectionInbound FrameDirection = iota
	FrameDirectionOutbound
)

// StreamKey is a comparable stand-in for Rust's Option<String> stream_id key
// (frames not tied to a specific stream — REQ, END, ERR, LOG — use the zero
// value with Present=false). Mirrors the creditGateKey pattern in credit.go.
type StreamKey struct {
	Present bool
	ID      string
}

// streamKeyFromPtr builds a StreamKey from a frame's *string stream id.
func streamKeyFromPtr(streamID *string) StreamKey {
	if streamID == nil {
		return StreamKey{}
	}
	return StreamKey{Present: true, ID: *streamID}
}

// ptr returns the *string form of this key (nil when not Present), for
// reconstructing snapshot stream_id fields.
func (k StreamKey) ptr() *string {
	if !k.Present {
		return nil
	}
	id := k.ID
	return &id
}

// StreamFlowStats is per-stream flow accounting. Keyed by stream_id (a
// StreamKey with Present=false = frames not tied to a specific stream: REQ,
// END, ERR, LOG). (matches Rust StreamFlowStats)
type StreamFlowStats struct {
	FramesIn  uint64 `json:"frames_in"`
	FramesOut uint64 `json:"frames_out"`
	BytesIn   uint64 `json:"bytes_in"`
	BytesOut  uint64 `json:"bytes_out"`
	ChunksIn  uint64 `json:"chunks_in"`
	ChunksOut uint64 `json:"chunks_out"`
	// CreditOutstanding is credits granted through this runtime minus chunks
	// that consumed them. Diagnostic — the endpoints hold the authoritative
	// windows.
	CreditOutstanding int64 `json:"credit_outstanding"`
	// Unbounded: stream announced with unbounded=true (no length promise).
	Unbounded bool `json:"unbounded"`
	// Ended: STREAM_END observed.
	Ended bool `json:"ended"`
}

// RequestState is everything a routing runtime knows about one in-flight
// request. (matches Rust RequestState)
type RequestState struct {
	Routing RequestRoutingEntry
	// Origin is the master index the response must return to (nil = external caller).
	Origin *int
	// ExternalChannel is the response delivery channel for externally-registered
	// requests (nil if this request has none).
	ExternalChannel chan<- Frame
	// IsPeer: whether this is a cartridge-initiated peer invocation.
	IsPeer bool
	// CapUrn is the cap URN of the originating REQ, when known at registration
	// — the request's nameable identity on the L8 surface. Without it a stats
	// snapshot shows only anonymous rids, making background chatter
	// indistinguishable from run traffic.
	CapUrn *string
	// Children: child peer calls spawned under this request (cancel cascade).
	Children []RequestKey
	Phase    RequestPhase
	// Streams: per-stream flow stats (StreamKey{Present:false} = non-stream frames).
	Streams      map[StreamKey]*StreamFlowStats
	CreatedAt    time.Time
	LastActivity time.Time
}

// NewRequestState creates a RequestState. (matches Rust RequestState::new)
func NewRequestState(routing RequestRoutingEntry, origin *int, externalChannel chan<- Frame, isPeer bool) *RequestState {
	now := time.Now()
	return &RequestState{
		Routing:         routing,
		Origin:          origin,
		ExternalChannel: externalChannel,
		IsPeer:          isPeer,
		Phase:           RequestPhaseCreated,
		Streams:         make(map[StreamKey]*StreamFlowStats),
		CreatedAt:       now,
		LastActivity:    now,
	}
}

// WithCapUrn attaches the originating REQ's cap URN — the request's nameable
// identity in observability surfaces. Returns the receiver for chaining
// (matches Rust RequestState::with_cap_urn's builder style).
func (s *RequestState) WithCapUrn(capUrn *string) *RequestState {
	s.CapUrn = capUrn
	return s
}

// record accounts for one frame moving through the runtime for this request.
func (s *RequestState) record(direction FrameDirection, frame *Frame) {
	s.LastActivity = time.Now()
	if frame.IsFlowFrame() {
		s.Phase = RequestPhaseStreaming
	}
	key := streamKeyFromPtr(frame.StreamId)
	stats, ok := s.Streams[key]
	if !ok {
		stats = &StreamFlowStats{}
		s.Streams[key] = stats
	}
	bytes := uint64(len(frame.Payload))
	switch direction {
	case FrameDirectionInbound:
		stats.FramesIn++
		stats.BytesIn += bytes
		if frame.FrameType == FrameTypeChunk {
			stats.ChunksIn++
			stats.CreditOutstanding--
		}
	case FrameDirectionOutbound:
		stats.FramesOut++
		stats.BytesOut += bytes
		if frame.FrameType == FrameTypeChunk {
			stats.ChunksOut++
		}
	}
	switch frame.FrameType {
	case FrameTypeStreamStart:
		if frame.IsUnbounded() {
			stats.Unbounded = true
		}
	case FrameTypeStreamEnd:
		stats.Ended = true
	case FrameTypeCredit:
		if cc := frame.CreditCount(); cc != nil {
			stats.CreditOutstanding += int64(*cc)
		}
	}
}

// TerminatedSummary is a summary of a finished request, retained in a
// bounded ring for stats. (matches Rust TerminatedSummary)
type TerminatedSummary struct {
	Xid        string       `json:"xid"`
	Rid        string       `json:"rid"`
	Kind       TerminalKind `json:"kind"`
	IsPeer     bool         `json:"is_peer"`
	CapUrn     *string      `json:"cap_urn"`
	LifetimeMs uint64       `json:"lifetime_ms"`
	FramesIn   uint64       `json:"frames_in"`
	FramesOut  uint64       `json:"frames_out"`
	BytesIn    uint64       `json:"bytes_in"`
	BytesOut   uint64       `json:"bytes_out"`
}

// RecentTerminatedCap is how many terminated-request summaries the ring
// retains. (matches Rust RECENT_TERMINATED_CAP)
const RecentTerminatedCap = 64

// TerminateObserver is called with every termination's summary, synchronously
// under the table's guard (whatever lock the owning runtime uses) —
// observers must be cheap and non-blocking (an engine aggregating per-run
// history, a test recorder). The bounded ring serves polling; this hook
// serves accumulation that must not miss terminations between polls (the
// ring evicts at RecentTerminatedCap).
type TerminateObserver func(*TerminatedSummary)

// requestEntry pairs a live RequestState with the RequestKey it was
// registered under, so RequestTable.Keys/KeysWhere can reconstruct the
// original (XID, RID) MessageId pair from the string-keyed internal map.
type requestEntry struct {
	key   RequestKey
	state *RequestState
}

// RequestTable is the unified request table (L7): one entry per in-flight
// request, one registration, one termination, plus the rid->xid secondary
// index and the recently-terminated ring.
//
// NOT internally synchronized — the owning runtime guards it with its own
// lock, mirroring the Rust reference (RequestTable is a plain struct wrapped
// externally in a lock) and the Swift/ObjC mirror. (matches Rust RequestTable)
type RequestTable struct {
	entries map[string]*requestEntry
	// ridIndex maps a RID's string form to the XID it is currently indexed
	// to — the secondary index for continuation frames arriving without a
	// full RequestKey.
	ridIndex          map[string]MessageId
	recentTerminated  []TerminatedSummary
	totalRegistered   uint64
	terminatedByKind  map[string]uint64
	terminateObserver TerminateObserver
}

// NewRequestTable creates an empty RequestTable. (matches Rust RequestTable::new)
func NewRequestTable() *RequestTable {
	return &RequestTable{
		entries:          make(map[string]*requestEntry),
		ridIndex:         make(map[string]MessageId),
		terminatedByKind: make(map[string]uint64),
	}
}

// Register registers a request. A request is registered exactly once (L7):
// re-registering a live key, or a RID already indexed to a different XID, is
// a protocol violation and is rejected. (matches Rust RequestTable::register)
func (t *RequestTable) Register(key RequestKey, state *RequestState) error {
	mk := key.mapKey()
	if _, exists := t.entries[mk]; exists {
		return fmt.Errorf(
			"request (%s, %s) already registered — a request is registered exactly once (L7)",
			key.Xid.ToString(), key.Rid.ToString(),
		)
	}
	ridKey := key.Rid.ToString()
	if existingXid, ok := t.ridIndex[ridKey]; ok {
		if !existingXid.Equals(key.Xid) {
			return fmt.Errorf(
				"rid %s already indexed to xid %s — cannot re-index to xid %s (L7)",
				ridKey, existingXid.ToString(), key.Xid.ToString(),
			)
		}
	}
	t.ridIndex[ridKey] = key.Xid
	t.entries[mk] = &requestEntry{key: key, state: state}
	t.totalRegistered++
	return nil
}

// Get returns the live state for key, or nil if not registered.
// (matches Rust RequestTable::get)
func (t *RequestTable) Get(key RequestKey) *RequestState {
	e, ok := t.entries[key.mapKey()]
	if !ok {
		return nil
	}
	return e.state
}

// Contains reports whether key is live. (matches Rust RequestTable::contains)
func (t *RequestTable) Contains(key RequestKey) bool {
	_, ok := t.entries[key.mapKey()]
	return ok
}

// XidForRid looks up the XID a bare RID belongs to (continuation frames
// arriving without routing IDs). (matches Rust RequestTable::xid_for_rid)
func (t *RequestTable) XidForRid(rid MessageId) (MessageId, bool) {
	xid, ok := t.ridIndex[rid.ToString()]
	return xid, ok
}

// Terminate terminates a request: removes the entry and its rid index
// atomically, records a summary, and returns the removed state (children for
// cancel cascades, the external channel for final delivery). After this
// returns, zero state for the key remains (L7). Returns nil if the key is
// not live (already terminated — termination happens exactly once).
// (matches Rust RequestTable::terminate)
func (t *RequestTable) Terminate(key RequestKey, kind TerminalKind) *RequestState {
	mk := key.mapKey()
	e, ok := t.entries[mk]
	if !ok {
		return nil
	}
	delete(t.entries, mk)
	// Only remove the rid index if it points at THIS xid — a re-used RID
	// under another XID (never valid per Register, but defensive against the
	// impossible) must not lose its index.
	ridKey := key.Rid.ToString()
	if existingXid, ok := t.ridIndex[ridKey]; ok && existingXid.Equals(key.Xid) {
		delete(t.ridIndex, ridKey)
	}

	state := e.state
	var framesIn, framesOut, bytesIn, bytesOut uint64
	for _, s := range state.Streams {
		framesIn += s.FramesIn
		framesOut += s.FramesOut
		bytesIn += s.BytesIn
		bytesOut += s.BytesOut
	}
	if len(t.recentTerminated) >= RecentTerminatedCap {
		// Evict oldest-first. Copies into a fresh backing array rather than
		// re-slicing in place so the bounded ring never pins an
		// ever-growing underlying array.
		t.recentTerminated = append([]TerminatedSummary{}, t.recentTerminated[1:]...)
	}
	summary := TerminatedSummary{
		Xid:        key.Xid.ToString(),
		Rid:        key.Rid.ToString(),
		Kind:       kind,
		IsPeer:     state.IsPeer,
		CapUrn:     state.CapUrn,
		LifetimeMs: uint64(time.Since(state.CreatedAt).Milliseconds()),
		FramesIn:   framesIn,
		FramesOut:  framesOut,
		BytesIn:    bytesIn,
		BytesOut:   bytesOut,
	}
	t.recentTerminated = append(t.recentTerminated, summary)
	t.terminatedByKind[kind.AsStr()]++
	if t.terminateObserver != nil {
		t.terminateObserver(&t.recentTerminated[len(t.recentTerminated)-1])
	}
	return state
}

// SetTerminateObserver installs the termination observer (see
// TerminateObserver's docs). One observer; installing replaces any previous
// one. (matches Rust RequestTable::set_terminate_observer)
func (t *RequestTable) SetTerminateObserver(observer TerminateObserver) {
	t.terminateObserver = observer
}

// RecordFrame records a frame moving through the runtime for this request.
// Unknown keys are ignored — the caller decides whether that is a counted
// drop (it is, at the routing layer) — recording is accounting, not routing.
// (matches Rust RequestTable::record_frame)
func (t *RequestTable) RecordFrame(key RequestKey, direction FrameDirection, frame *Frame) {
	if e, ok := t.entries[key.mapKey()]; ok {
		e.state.record(direction, frame)
	}
}

// LinkChild registers a child peer call under its parent (cancel cascade).
// (matches Rust RequestTable::link_child)
func (t *RequestTable) LinkChild(parent RequestKey, child RequestKey) {
	if e, ok := t.entries[parent.mapKey()]; ok {
		e.state.Children = append(e.state.Children, child)
	}
}

// Keys returns the keys of all live requests (for sweeps). A fresh slice, so
// the caller can mutate the table while iterating.
// (matches Rust RequestTable::keys)
func (t *RequestTable) Keys() []RequestKey {
	keys := make([]RequestKey, 0, len(t.entries))
	for _, e := range t.entries {
		keys = append(keys, e.key)
	}
	return keys
}

// KeysWhere returns the keys of live requests matching a predicate on their
// state. (matches Rust RequestTable::keys_where)
func (t *RequestTable) KeysWhere(pred func(*RequestState) bool) []RequestKey {
	var keys []RequestKey
	for _, e := range t.entries {
		if pred(e.state) {
			keys = append(keys, e.key)
		}
	}
	return keys
}

// Len returns the number of live requests. (matches Rust RequestTable::len)
func (t *RequestTable) Len() int {
	return len(t.entries)
}

// IsEmpty reports whether the table has no live requests.
// (matches Rust RequestTable::is_empty)
func (t *RequestTable) IsEmpty() bool {
	return len(t.entries) == 0
}

// StreamSnapshot is one stream's stats in a snapshot. Serializes stream_id
// alongside the flattened StreamFlowStats fields (Go's encoding/json has no
// serde(flatten) equivalent, so MarshalJSON does it by hand — mirrors the
// Swift port's manual Codable implementation for the same reason).
// (matches Rust StreamSnapshot)
type StreamSnapshot struct {
	StreamId *string
	Stats    StreamFlowStats
}

// MarshalJSON flattens Stats' fields alongside stream_id.
func (s StreamSnapshot) MarshalJSON() ([]byte, error) {
	type flattened struct {
		StreamId          *string `json:"stream_id"`
		FramesIn          uint64  `json:"frames_in"`
		FramesOut         uint64  `json:"frames_out"`
		BytesIn           uint64  `json:"bytes_in"`
		BytesOut          uint64  `json:"bytes_out"`
		ChunksIn          uint64  `json:"chunks_in"`
		ChunksOut         uint64  `json:"chunks_out"`
		CreditOutstanding int64   `json:"credit_outstanding"`
		Unbounded         bool    `json:"unbounded"`
		Ended             bool    `json:"ended"`
	}
	return json.Marshal(flattened{
		StreamId:          s.StreamId,
		FramesIn:          s.Stats.FramesIn,
		FramesOut:         s.Stats.FramesOut,
		BytesIn:           s.Stats.BytesIn,
		BytesOut:          s.Stats.BytesOut,
		ChunksIn:          s.Stats.ChunksIn,
		ChunksOut:         s.Stats.ChunksOut,
		CreditOutstanding: s.Stats.CreditOutstanding,
		Unbounded:         s.Stats.Unbounded,
		Ended:             s.Stats.Ended,
	})
}

// UnmarshalJSON un-flattens stream_id + StreamFlowStats fields.
func (s *StreamSnapshot) UnmarshalJSON(data []byte) error {
	type flattened struct {
		StreamId          *string `json:"stream_id"`
		FramesIn          uint64  `json:"frames_in"`
		FramesOut         uint64  `json:"frames_out"`
		BytesIn           uint64  `json:"bytes_in"`
		BytesOut          uint64  `json:"bytes_out"`
		ChunksIn          uint64  `json:"chunks_in"`
		ChunksOut         uint64  `json:"chunks_out"`
		CreditOutstanding int64   `json:"credit_outstanding"`
		Unbounded         bool    `json:"unbounded"`
		Ended             bool    `json:"ended"`
	}
	var f flattened
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	s.StreamId = f.StreamId
	s.Stats = StreamFlowStats{
		FramesIn:          f.FramesIn,
		FramesOut:         f.FramesOut,
		BytesIn:           f.BytesIn,
		BytesOut:          f.BytesOut,
		ChunksIn:          f.ChunksIn,
		ChunksOut:         f.ChunksOut,
		CreditOutstanding: f.CreditOutstanding,
		Unbounded:         f.Unbounded,
		Ended:             f.Ended,
	}
	return nil
}

// RequestSnapshot is one live request in a snapshot. (matches Rust RequestSnapshot)
type RequestSnapshot struct {
	Xid    string       `json:"xid"`
	Rid    string       `json:"rid"`
	Phase  RequestPhase `json:"phase"`
	IsPeer bool         `json:"is_peer"`
	// CapUrn is the cap URN of the originating REQ — the request's nameable
	// identity. Absent when unknown, never invented.
	CapUrn            *string          `json:"cap_urn"`
	OriginMaster      *int             `json:"origin_master"`
	DestinationMaster int              `json:"destination_master"`
	AgeMs             uint64           `json:"age_ms"`
	IdleMs            uint64           `json:"idle_ms"`
	Children          uint64           `json:"children"`
	Streams           []StreamSnapshot `json:"streams"`
}

// RequestTableSnapshot is the full table snapshot: the L8 observability
// surface for request state. (matches Rust RequestTableSnapshot)
type RequestTableSnapshot struct {
	Active           []RequestSnapshot   `json:"active"`
	RecentTerminated []TerminatedSummary `json:"recent_terminated"`
	TotalRegistered  uint64              `json:"total_registered"`
	TerminatedByKind map[string]uint64   `json:"terminated_by_kind"`
}

// Snapshot returns a serializable snapshot of the table: live requests +
// recent terminations + lifetime totals. Field names are the mirror
// contract (TEST7087). (matches Rust RequestTable::snapshot)
func (t *RequestTable) Snapshot() RequestTableSnapshot {
	active := make([]RequestSnapshot, 0, len(t.entries))
	for _, e := range t.entries {
		s := e.state
		streams := make([]StreamSnapshot, 0, len(s.Streams))
		for sk, stats := range s.Streams {
			streams = append(streams, StreamSnapshot{StreamId: sk.ptr(), Stats: *stats})
		}
		active = append(active, RequestSnapshot{
			Xid:               e.key.Xid.ToString(),
			Rid:               e.key.Rid.ToString(),
			Phase:             s.Phase,
			IsPeer:            s.IsPeer,
			CapUrn:            s.CapUrn,
			OriginMaster:      s.Origin,
			DestinationMaster: s.Routing.DestinationMasterIdx,
			AgeMs:             uint64(time.Since(s.CreatedAt).Milliseconds()),
			IdleMs:            uint64(time.Since(s.LastActivity).Milliseconds()),
			Children:          uint64(len(s.Children)),
			Streams:           streams,
		})
	}
	sort.Slice(active, func(i, j int) bool { return active[i].Rid < active[j].Rid })

	recent := make([]TerminatedSummary, len(t.recentTerminated))
	copy(recent, t.recentTerminated)

	terminatedByKind := make(map[string]uint64, len(t.terminatedByKind))
	for k, v := range t.terminatedByKind {
		terminatedByKind[k] = v
	}

	return RequestTableSnapshot{
		Active:           active,
		RecentTerminated: recent,
		TotalRegistered:  t.totalRegistered,
		TerminatedByKind: terminatedByKind,
	}
}

// String implements fmt.Stringer with a compact debug view (matches Rust's
// custom Debug impl: entry/recent-terminated counts + total registered,
// never the full contents).
func (t *RequestTable) String() string {
	return fmt.Sprintf(
		"RequestTable{entries: %d, recent_terminated: %d, total_registered: %d}",
		len(t.entries), len(t.recentTerminated), t.totalRegistered,
	)
}
