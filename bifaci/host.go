package bifaci

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// ResponseChunk represents a response chunk from a cartridge (matches Rust ResponseChunk)
type ResponseChunk struct {
	Payload []byte
	Seq     uint64
	Offset  *uint64
	Len     *uint64
	IsEof   bool
}

// CartridgeResponseType indicates whether a response is single or streaming
type CartridgeResponseType int

const (
	CartridgeResponseTypeSingle CartridgeResponseType = iota
	CartridgeResponseTypeStreaming
)

// CartridgeResponse represents a complete response from a cartridge
type CartridgeResponse struct {
	Type      CartridgeResponseType
	Single    []byte
	Streaming []*ResponseChunk
}

// FinalPayload gets the final payload
func (pr *CartridgeResponse) FinalPayload() []byte {
	switch pr.Type {
	case CartridgeResponseTypeSingle:
		return pr.Single
	case CartridgeResponseTypeStreaming:
		if len(pr.Streaming) > 0 {
			return pr.Streaming[len(pr.Streaming)-1].Payload
		}
		return nil
	default:
		return nil
	}
}

// Concatenated concatenates all payloads into a single buffer
func (pr *CartridgeResponse) Concatenated() []byte {
	switch pr.Type {
	case CartridgeResponseTypeSingle:
		result := make([]byte, len(pr.Single))
		copy(result, pr.Single)
		return result
	case CartridgeResponseTypeStreaming:
		totalLen := 0
		for _, chunk := range pr.Streaming {
			totalLen += len(chunk.Payload)
		}
		result := make([]byte, 0, totalLen)
		for _, chunk := range pr.Streaming {
			result = append(result, chunk.Payload...)
		}
		return result
	default:
		return nil
	}
}

// HostError represents errors from the cartridge host
type HostError struct {
	Type    HostErrorType
	Message string
	Code    string
}

type HostErrorType int

const (
	HostErrorTypeCbor HostErrorType = iota
	HostErrorTypeIo
	HostErrorTypeCartridgeError
	HostErrorTypeUnexpectedFrameType
	HostErrorTypeProcessExited
	HostErrorTypeHandshake
	HostErrorTypeClosed
	HostErrorTypeSendError
	HostErrorTypeRecvError
	HostErrorTypePeerInvokeNotSupported
)

func (e *HostError) Error() string {
	switch e.Type {
	case HostErrorTypeCbor:
		return fmt.Sprintf("CBOR error: %s", e.Message)
	case HostErrorTypeIo:
		return fmt.Sprintf("I/O error: %s", e.Message)
	case HostErrorTypeCartridgeError:
		return fmt.Sprintf("Cartridge returned error: [%s] %s", e.Code, e.Message)
	case HostErrorTypeUnexpectedFrameType:
		return fmt.Sprintf("Unexpected frame type: %s", e.Message)
	case HostErrorTypeProcessExited:
		return "Cartridge process exited unexpectedly"
	case HostErrorTypeHandshake:
		return fmt.Sprintf("Handshake failed: %s", e.Message)
	case HostErrorTypeClosed:
		return "Host is closed"
	case HostErrorTypeSendError:
		return "Send error: channel closed"
	case HostErrorTypeRecvError:
		return "Receive error: channel closed"
	case HostErrorTypePeerInvokeNotSupported:
		return fmt.Sprintf("Peer invoke not supported: %s", e.Message)
	default:
		return fmt.Sprintf("Unknown error: %s", e.Message)
	}
}

// =========================================================================
// Multi-cartridge host
// =========================================================================

// cartridgeEvent is an internal event from a cartridge reader goroutine.
type cartridgeEvent struct {
	cartridgeIdx int
	frame        *Frame
	isDeath      bool
}

// capTableEntry maps a cap URN to a cartridge index.
type capTableEntry struct {
	capUrn       string
	cartridgeIdx int
}

// rxidKey composes the (XID, RID) tuple used to route incoming
// requests from the relay. Mirrors the Rust host's
// `incoming_rxids: HashMap<(MessageId, MessageId), usize>` — XID
// (routing_id, assigned by the RelaySwitch) plus RID (the
// engine-side request id) together identify a request body
// uniquely. Composing them as a single string lets us use Go's
// map[string] without a separate hash impl.
//
// We deliberately do NOT lose the typed MessageId values: the lookup
// key is just a string, but the map VALUE carries the original IDs so
// `handleCartridgeDeath` can synthesize an ERR with the correct rid
// and re-stamp the original xid without re-parsing the key (which
// would fail for the uint MessageId variant — UUIDs are not the only
// legal id form, and silently dropping non-UUID entries would mean a
// pending request that never gets its terminal ERR).
type rxidKey struct {
	xid string
	rid string
}

func makeRxidKey(xid, rid MessageId) rxidKey {
	return rxidKey{xid: xid.ToString(), rid: rid.ToString()}
}

// incomingRoute is the value stored in `incomingRxids`: which cartridge
// is handling the request, plus the original typed XID/RID needed to
// synthesize a terminal ERR frame on cartridge death.
type incomingRoute struct {
	cartridgeIdx int
	xid          MessageId
	rid          MessageId
}

// outgoingRoute is the value stored in `outgoingRids`: which cartridge
// initiated the peer request, plus the typed RID for the same reason.
type outgoingRoute struct {
	cartridgeIdx int
	rid          MessageId
}

// ManagedCartridge represents a cartridge managed by the CartridgeHost.
type ManagedCartridge struct {
	path     string
	cmd      *exec.Cmd
	writerCh chan *Frame
	manifest []byte
	limits   Limits
	// caps is the flat URN view derived from capGroups (kept in sync
	// alongside it to avoid recomputing on every cap-table rebuild).
	caps []string
	// capGroups is the cartridge's manifest cap-groups, parsed at
	// HELLO time. This is the source of truth on the wire; the engine
	// reads `installed_cartridges[*].cap_groups` and computes its own
	// flat list.
	capGroups   []CapGroup
	running     bool
	helloFailed bool
	// removed marks a cartridge retired by a roster sync (the install was
	// removed/replaced on disk). A removed cartridge disappears from the
	// inventory entirely — unlike helloFailed, which stays visible
	// carrying an attachment error. Cartridge slots are never physically
	// removed (routing state holds indices), so this flag is the
	// retirement mechanism. Mirrors Rust ManagedCartridge::removed /
	// Swift isRemoved.
	removed                  bool
	LastHeartbeatUnixSeconds *int64
	RestartCount             uint64
	// protocolDropsTotal is the cumulative protocol drop count
	// self-reported by the cartridge as `drops_total` in heartbeat
	// response meta (writer-gate post-terminal drops, closed-channel
	// sends, …). nil until the first heartbeat round-trip carries the
	// counter. Survives across readings (each heartbeat carries the
	// cartridge's running total). Mirrors Rust protocol_drops_total.
	protocolDropsTotal *uint64
	// pendingHeartbeats tracks health probes this host has sent to the
	// cartridge (id string → sent time), so a later HEARTBEAT frame from
	// the cartridge can be told apart from a cartridge-initiated
	// heartbeat. Mirrors Rust ManagedCartridge::pending_heartbeats.
	pendingHeartbeats map[string]time.Time
	// installedIdentity is the resolvable (registry_url, channel, id,
	// version) identity of the cartridge. Every registration path stamps
	// one: RegisterCartridgeDir from the directory-tree hash,
	// RegisterCartridge from the binary hash, and AttachCartridge from the
	// HELLO manifest. It gates this cartridge's appearance in the
	// RelayNotify inventory — a nil identity (binary unreadable / manifest
	// unparseable) means the cartridge is dropped from advertisement rather
	// than exposed under a fabricated id.
	installedIdentity *InstalledCartridgeRecord
	// cartridgeDir is the version directory a dir-registered cartridge
	// was created from (empty for binary/attached cartridges). Its
	// presence marks a cartridge as roster-managed (isRegisteredDir).
	cartridgeDir string
}

// installedCartridgeRecord returns the cartridge's resolvable identity, or nil
// if it has none (e.g. an attached/internal provider with no on-disk anchor).
func (c *ManagedCartridge) installedCartridgeRecord() *InstalledCartridgeRecord {
	return c.installedIdentity
}

// isRegisteredDir reports whether this cartridge was registered from a version
// directory (the lazily-spawned, dir-backed kind). Distinguishes roster-managed
// installs from attached/internal providers during a SyncRoster.
func (c *ManagedCartridge) isRegisteredDir() bool {
	return c.cartridgeDir != ""
}

// capURNsFromGroups is the flat de-duplicated cap-URN view derived from a
// cartridge's cap_groups, preserving manifest declaration order.
func capURNsFromGroups(groups []CapGroup) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, group := range groups {
		for _, c := range group.Caps {
			u := c.Urn.String()
			if u == "" {
				continue
			}
			if _, ok := seen[u]; ok {
				continue
			}
			seen[u] = struct{}{}
			out = append(out, u)
		}
	}
	return out
}

// CartridgeHost manages N cartridge binaries with cap-based routing.
//
// Cartridges are either registered (for on-demand spawning) or attached
// (pre-connected). REQ frames from the relay are routed to the correct
// cartridge by cap URN. Continuation frames (STREAM_START, CHUNK,
// STREAM_END, END) are routed by request ID.
type CartridgeHost struct {
	cartridges []*ManagedCartridge
	capTable   []capTableEntry

	// Routing tables — mirror of the Rust `CartridgeHostRuntime`
	// design (capdag/src/bifaci/host_runtime.rs). Two independent
	// maps so self-loop peer requests (where the requesting and
	// answering cartridge are behind the same relay connection)
	// can be routed correctly:
	//
	//   outgoingRids[rid]            = cartridge that SENT the peer REQ.
	//                                  Keyed by RID alone — the relay
	//                                  hasn't assigned an XID for
	//                                  cartridge-initiated requests.
	//                                  Used to deliver the peer
	//                                  RESPONSE (frames coming back
	//                                  from the relay) to the
	//                                  requester.
	//
	//   incomingRxids[(xid, rid)]    = cartridge that RECEIVED a
	//                                  request from the relay.
	//                                  Keyed by (XID, RID) because
	//                                  for self-loop peers the same
	//                                  RID also exists in
	//                                  outgoingRids; the XID from
	//                                  the RelaySwitch disambiguates.
	//                                  Used to deliver the request
	//                                  BODY (continuation frames
	//                                  from the relay) to the
	//                                  handler.
	//
	// Phase tracking: when the request body END arrives from the
	// relay, the `incomingRxids` entry is removed — subsequent
	// frames with the same (XID, RID) fall through to
	// `outgoingRids` and are routed as peer responses. Frame
	// ordering on a single socket guarantees END is last for the
	// body phase, so the transition is unambiguous.
	outgoingRids  map[string]outgoingRoute  // rid string → route (carries typed RID)
	incomingRxids map[rxidKey]incomingRoute // (xid, rid) → route (carries typed XID/RID)

	// incomingBodyDone marks keys in incomingRxids whose REQUEST BODY has
	// completed (body END routed to the handler) but whose RESPONSE has
	// not yet terminated. Protocol v3 keeps the incomingRxids entry alive
	// through this phase — engine→cartridge CREDIT grants for the
	// handler's OUTPUT arrive throughout it (removing the entry at body
	// END, as the pre-v3 code did, silently kills every output grant and
	// deadlocks any response larger than the initial window). Data
	// frames arriving from the relay during this phase are self-loop
	// peer responses and fall through to outgoingRids as before. Cleared
	// with the entry. Mirrors Rust incoming_body_done.
	incomingBodyDone map[rxidKey]struct{}
	// incomingResponseDone marks keys whose handler RESPONSE terminal
	// already passed outbound while the request body was still open
	// (response-first race). When the body END later arrives, the entry
	// is released immediately instead of being marked body-done. Cleared
	// with the entry. Mirrors Rust incoming_response_done.
	incomingResponseDone map[rxidKey]struct{}

	// drops is the dropped-frame accounting (L8): unroutable
	// continuations and frames for dead cartridges are counted drops,
	// never silent losses. Mirrors Rust CartridgeHostRuntime.drops.
	drops *DropCounters

	// staticInventoryRecords are inventory records this host does NOT
	// manage as processes — discovery outcomes such as incompatible
	// installs (verdict-rejected, wrong manifest version, quarantined).
	// Merged into every capabilities advertisement so a host-originated
	// RelayNotify can never erase them from the engine's inventory.
	// Mirrors Rust CartridgeHostRuntime.static_inventory_records.
	staticInventoryRecords []InstalledCartridgeRecord

	// Routing-table GC bookkeeping — mirror of the Rust runtime's
	// touch-sequence-based garbage collector
	// (capdag/src/bifaci/host_runtime.rs). The `*Touched` maps stamp
	// each routing entry with a monotonic touch sequence so the GC can
	// evict the oldest entries first when a table crosses its soft
	// watermark, keeping the routing tables bounded under a runaway
	// producer without dropping fresh (still-streaming) flows.
	incomingRxidsTouched  map[rxidKey]uint64
	outgoingRidsTouched   map[string]uint64
	routingTouchSeq       uint64
	routingGcRunsTotal    uint64
	routingGcEvictedTotal uint64

	capabilities []byte
	eventCh      chan cartridgeEvent
	// commandCh delivers HostCommands (e.g. SyncRoster) from external
	// callers into the Run loop, where they are applied under the host
	// mutex alongside relay/cartridge events. Buffered so a caller's
	// send never blocks the caller for the common single-command case.
	commandCh chan hostCommand
	mu        sync.Mutex

	// Observer receives lifecycle notifications for cartridges.
	// May be nil.
	Observer CartridgeHostObserver
}

// NewCartridgeHost creates a new multi-cartridge host.
func NewCartridgeHost() *CartridgeHost {
	return &CartridgeHost{
		outgoingRids:         make(map[string]outgoingRoute),
		incomingRxids:        make(map[rxidKey]incomingRoute),
		incomingRxidsTouched: make(map[rxidKey]uint64),
		outgoingRidsTouched:  make(map[string]uint64),
		incomingBodyDone:     make(map[rxidKey]struct{}),
		incomingResponseDone: make(map[rxidKey]struct{}),
		drops:                NewDropCounters(),
		eventCh:              make(chan cartridgeEvent, 256),
		commandCh:            make(chan hostCommand, 16),
	}
}

// Routing-table GC tuning constants — mirror of the Rust runtime
// (capdag/src/bifaci/host_runtime.rs). The routing maps are bounded by
// ROUTING_TABLE_HARD_CAP; the GC fires when a table crosses the soft
// watermark and evicts the oldest ROUTING_TABLE_GC_EVICTION_FRACTION by
// touch-sequence, with a secondary hard-cap pass for extreme runaway.
const (
	// ROUTING_TABLE_HARD_CAP — the absolute ceiling for any routing
	// table. ~14× the ~568 entries observed across a long session.
	RoutingTableHardCap = 8192
	// ROUTING_TABLE_SOFT_WATERMARK — when an insertion brings a table
	// at or above this size, the GC fires. ~80% of the hard cap.
	RoutingTableSoftWatermark = 6553
	// ROUTING_TABLE_GC_EVICTION_FRACTION — fraction of entries to drop
	// in one GC pass.
	RoutingTableGcEvictionFraction = 0.25
)

// touchIncomingRxid stamps key in incomingRxidsTouched with a fresh touch
// sequence. Called both on insert and on every read that hits the entry, so a
// still-streaming flow stays "fresh" for the GC. Caller must hold h.mu.
func (h *CartridgeHost) touchIncomingRxid(key rxidKey) {
	h.routingTouchSeq++
	h.incomingRxidsTouched[key] = h.routingTouchSeq
}

// touchOutgoingRid stamps rid in outgoingRidsTouched with a fresh touch
// sequence. Caller must hold h.mu.
func (h *CartridgeHost) touchOutgoingRid(rid string) {
	h.routingTouchSeq++
	h.outgoingRidsTouched[rid] = h.routingTouchSeq
}

// gcRoutingTablesIfNeeded runs the GC if any routing table has crossed its soft
// watermark. Each table is GC'd independently (their key sets don't overlap so
// there's no benefit to ganging them). Caller must hold h.mu. Mirrors the Rust
// gc_routing_tables_if_needed.
func (h *CartridgeHost) gcRoutingTablesIfNeeded() {
	if len(h.incomingRxids) >= RoutingTableSoftWatermark {
		gcRoutingTable(h.incomingRxids, h.incomingRxidsTouched, &h.routingGcRunsTotal, &h.routingGcEvictedTotal)
	}
	if len(h.outgoingRids) >= RoutingTableSoftWatermark {
		gcRoutingTable(h.outgoingRids, h.outgoingRidsTouched, &h.routingGcRunsTotal, &h.routingGcEvictedTotal)
	}
}

// gcRoutingTable runs a single GC pass: drop the oldest
// ROUTING_TABLE_GC_EVICTION_FRACTION of primary (and its matching touched
// entries) by touch-sequence ascending. Keys missing from touched are treated
// as oldest (sequence = 0). A secondary hard-cap pass evicts more aggressively
// if the table somehow remains at or above the hard cap. Mirrors the Rust
// generic gc_routing_table. The runs counter is bumped once per primary pass;
// the evicted counter accumulates across both passes.
func gcRoutingTable[K comparable, V any](primary map[K]V, touched map[K]uint64, runsTotal, evictedTotal *uint64) {
	beforeCount := len(primary)
	evictCount := int(float64(beforeCount) * RoutingTableGcEvictionFraction)
	if evictCount < 1 {
		evictCount = 1
	}

	evictOldest(primary, touched, evictCount)
	*runsTotal++
	*evictedTotal += uint64(evictCount)

	// Secondary "hard cap" pass: if still at or above the hard cap
	// (extreme runaway), evict more aggressively until back under the
	// soft watermark. Bounded loop — runs at most a couple of
	// iterations even at pathological growth.
	for len(primary) >= RoutingTableHardCap {
		extraEvict := len(primary) - RoutingTableSoftWatermark
		if extraEvict < 1 {
			extraEvict = 1
		}
		evictOldest(primary, touched, extraEvict)
		*evictedTotal += uint64(extraEvict)
	}
}

// evictOldest removes the n entries with the smallest touch sequence from
// primary (and their touched entries). Keys missing from touched sort as 0
// (oldest).
func evictOldest[K comparable, V any](primary map[K]V, touched map[K]uint64, n int) {
	type cand struct {
		key K
		at  uint64
	}
	candidates := make([]cand, 0, len(primary))
	for k := range primary {
		candidates = append(candidates, cand{key: k, at: touched[k]})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].at < candidates[j].at
	})
	if n > len(candidates) {
		n = len(candidates)
	}
	for _, c := range candidates[:n] {
		delete(primary, c.key)
		delete(touched, c.key)
	}
}

// RegisteredDirSpec describes a directory-registered cartridge in a roster sync.
// Mirrors the parameters of RegisterCartridgeDir so a caller can describe the
// full desired registered-dir set without reaching into host internals.
type RegisteredDirSpec struct {
	EntryPoint  string
	VersionDir  string
	Id          string
	Channel     CartridgeChannel
	RegistryURL *string
	Version     string
	CapGroups   []CapGroup
}

// hostCommand is a command applied inside the Run loop. Exactly one field is set.
type hostCommand struct {
	syncRoster []RegisteredDirSpec
	isSync     bool
}

// CartridgeProcessHandle is a thread-safe handle for sending commands to a
// running CartridgeHost. Obtained via ProcessHandle() before calling Run().
type CartridgeProcessHandle struct {
	commandCh chan hostCommand
}

// ProcessHandle returns a handle for sending commands (e.g. SyncRoster) to this
// host while its Run loop is executing.
func (h *CartridgeHost) ProcessHandle() *CartridgeProcessHandle {
	return &CartridgeProcessHandle{commandCh: h.commandCh}
}

// SyncRoster replaces the live registered-dir roster (see syncRegisteredRoster).
// Blocks until the command is accepted by the Run loop. Returns an error only
// if the host's Run loop is not consuming commands.
func (p *CartridgeProcessHandle) SyncRoster(cartridges []RegisteredDirSpec) error {
	p.commandCh <- hostCommand{syncRoster: cartridges, isSync: true}
	return nil
}

// SetStaticInventoryRecords provides inventory records for cartridges this
// host does NOT manage as processes — discovery outcomes such as
// incompatible installs, carrying their AttachmentError. They are merged
// into every capabilities advertisement (initial and republished), so the
// engine's inventory — and therefore the UI — always shows every on-disk
// cartridge with its status. Mirrors Rust set_static_inventory_records.
func (h *CartridgeHost) SetStaticInventoryRecords(records []InstalledCartridgeRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.staticInventoryRecords = records
}

// HostProtocolStats is the host runtime's protocol observability snapshot
// (L8): per-reason drop counters and routing-table sizes. Field names are
// the mirror contract. Mirrors Rust HostProtocolStats / Swift's
// HostProtocolStats.
//
// Go's CartridgeHost does not maintain incoming_to_peer_rids (peer-call
// cancel-cascade linkage) or outgoing_max_seq (per-flow max-seen-seq
// bookkeeping) — those routing tables don't exist in this simpler,
// channel-based routing design, so the two corresponding Rust/Swift fields
// have no honest value to report here and are omitted rather than
// fabricated as a permanent zero.
type HostProtocolStats struct {
	Drops                 DropSnapshot `json:"drops"`
	OutgoingRids          int          `json:"outgoing_rids"`
	IncomingRxids         int          `json:"incoming_rxids"`
	RoutingGcRunsTotal    uint64       `json:"routing_gc_runs_total"`
	RoutingGcEvictedTotal uint64       `json:"routing_gc_evicted_total"`
}

// ProtocolStats returns the protocol observability snapshot (L8): drop
// counters and routing-table sizes for this host.
func (h *CartridgeHost) ProtocolStats() HostProtocolStats {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.protocolStatsLocked()
}

// protocolStatsLocked builds the snapshot. Caller must hold h.mu.
func (h *CartridgeHost) protocolStatsLocked() HostProtocolStats {
	return HostProtocolStats{
		Drops:                 h.drops.Snapshot(),
		OutgoingRids:          len(h.outgoingRids),
		IncomingRxids:         len(h.incomingRxids),
		RoutingGcRunsTotal:    h.routingGcRunsTotal,
		RoutingGcEvictedTotal: h.routingGcEvictedTotal,
	}
}

// installedCartridgeRecordFromBinary builds the install identity for a
// cartridge registered by binary path (on-demand spawn). The identity tuple
// (registry_url, channel, id, version) comes from the cartridge's manifest
// (supplied by the caller — the binary path has no bearing on them); the
// sha256 is taken over the binary bytes. Mirrors the reference
// installed_cartridge_record_from_binary. Returns nil if the binary is
// unreadable (the cartridge is then dropped from advertisement rather than
// advertised without a resolvable identity).
func installedCartridgeRecordFromBinary(path, name, version string, channel CartridgeChannel, registryURL *string) *InstalledCartridgeRecord {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	digest := sha256.Sum256(data)
	return &InstalledCartridgeRecord{
		RegistryURL: registryURL,
		Id:          name,
		Channel:     string(channel),
		Version:     version,
		Sha256:      hex.EncodeToString(digest[:]),
		// On-demand binary: not yet probed/verified at registration time.
		Lifecycle: CartridgeLifecycleDiscovered,
	}
}

// RegisterCartridge registers a cartridge binary for on-demand spawning.
// The cartridge is not spawned until a REQ arrives for one of its caps.
//
// The install identity is stamped from the binary: (name, version, channel,
// registryURL) come from the cartridge's own manifest and the sha256 is over
// the binary bytes. Mirrors the reference register_cartridge /
// new_registered_binary — advertisement is identity-gated, so a cartridge with
// no resolvable identity is dropped from every RelayNotify, never silently
// advertised under a synthetic id.
//
// capGroups is the cartridge's manifest cap-group structure (captured at
// probe-time HELLO during discovery); the flat cap-URN routing view is derived
// from it via ManagedCartridge.cap_urns, so no parallel knownCaps list is kept.
func (h *CartridgeHost) RegisterCartridge(path, name, version string, channel CartridgeChannel, registryURL *string, capGroups []CapGroup) {
	h.mu.Lock()
	defer h.mu.Unlock()

	cartridgeIdx := len(h.cartridges)
	cartridge := &ManagedCartridge{
		path:              path,
		capGroups:         capGroups,
		caps:              capURNsFromGroups(capGroups),
		running:           false,
		limits:            DefaultLimits(),
		installedIdentity: installedCartridgeRecordFromBinary(path, name, version, channel, registryURL),
		pendingHeartbeats: make(map[string]time.Time),
	}
	h.cartridges = append(h.cartridges, cartridge)

	for _, capURN := range cartridge.caps {
		h.capTable = append(h.capTable, capTableEntry{capUrn: capURN, cartridgeIdx: cartridgeIdx})
	}
}

// RegisterCartridgeDir registers a cartridge discovered as a version directory
// for on-demand spawning, stamping its full (registry_url, channel, id,
// version) identity so it appears in the engine's RelayNotify with the
// (registry, channel) provenance preserved end-to-end. The cartridge is not
// spawned until a REQ arrives for one of its caps; its cap_groups (already
// probed during discovery) are registered into the cap table immediately.
//
// The version directory is hashed at registration time. If the directory is not
// hashable the cartridge is recorded with an EntryPointMissing attachment error
// and hello_failed so it drops out of the cap table / inventory — mirroring the
// reference new_registered_dir constructor.
func (h *CartridgeHost) RegisterCartridgeDir(spec RegisteredDirSpec) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.registerCartridgeDirLocked(spec)
}

func (h *CartridgeHost) registerCartridgeDirLocked(spec RegisteredDirSpec) {
	cartridgeIdx := len(h.cartridges)

	cartridge := &ManagedCartridge{
		path:              spec.EntryPoint,
		cartridgeDir:      spec.VersionDir,
		capGroups:         spec.CapGroups,
		caps:              capURNsFromGroups(spec.CapGroups),
		running:           false,
		limits:            DefaultLimits(),
		pendingHeartbeats: make(map[string]time.Time),
	}

	sha256, err := HashCartridgeDirectory(spec.VersionDir)
	if err != nil {
		detectedAt := unixSecondsNow()
		cartridge.helloFailed = true
		cartridge.installedIdentity = &InstalledCartridgeRecord{
			RegistryURL: spec.RegistryURL,
			Id:          spec.Id,
			Channel:     string(spec.Channel),
			Version:     spec.Version,
			Sha256:      "",
			AttachmentError: &CartridgeAttachmentError{
				Kind:                  CartridgeAttachmentErrorKindEntryPointMissing,
				Message:               fmt.Sprintf("Cartridge directory not hashable at '%s': %v", spec.VersionDir, err),
				DetectedAtUnixSeconds: detectedAt,
			},
			Lifecycle: CartridgeLifecycleDiscovered,
		}
	} else {
		cartridge.installedIdentity = &InstalledCartridgeRecord{
			RegistryURL: spec.RegistryURL,
			Id:          spec.Id,
			Channel:     string(spec.Channel),
			Version:     spec.Version,
			Sha256:      sha256,
			// Engine-spawned external providers are operational by
			// construction: discovery validated the install context and
			// probed the cartridge before this registration. There is no
			// separate inspecting/verifying phase on this path.
			Lifecycle: CartridgeLifecycleOperational,
		}
	}

	h.cartridges = append(h.cartridges, cartridge)
	if !cartridge.helloFailed {
		for _, capURN := range cartridge.caps {
			h.capTable = append(h.capTable, capTableEntry{capUrn: capURN, cartridgeIdx: cartridgeIdx})
		}
	}
}

// installedCartridgeRecordFromManifest builds the install identity for a
// cartridge attached over raw streams (no on-disk anchor: the
// dev/host-embedded/interop path).
//
// Advertisement is identity-gated — a cartridge with no installedIdentity is
// silently dropped from every RelayNotify (see buildInstalledCartridgeIdentities),
// so an attached cartridge MUST carry a resolvable identity or the host
// advertises an empty inventory and the engine can never route to it. An
// attached cartridge has already completed HELLO + identity verification by the
// time this is called, so it is operational by construction; its identity is
// sourced from the manifest it sent during HELLO (the same
// (registry_url, channel, id, version) tuple a registered install carries), with
// the sha256 taken over the manifest bytes (the only stable artefact available
// without a file on disk). Mirrors the reference
// installed_cartridge_record_from_manifest. Returns nil if the manifest does not
// parse (the caller still attaches; the record is honestly absent).
func installedCartridgeRecordFromManifest(manifest []byte) *InstalledCartridgeRecord {
	var parsed CapManifest
	if err := json.Unmarshal(manifest, &parsed); err != nil {
		return nil
	}
	digest := sha256.Sum256(manifest)
	return &InstalledCartridgeRecord{
		RegistryURL: parsed.RegistryURL,
		Id:          parsed.Name,
		Channel:     parsed.Channel,
		Version:     parsed.Version,
		Sha256:      hex.EncodeToString(digest[:]),
		// Attached ⇒ HELLO + identity verification already succeeded.
		Lifecycle: CartridgeLifecycleOperational,
	}
}

// AttachCartridge attaches a pre-connected cartridge (already running).
// Performs HELLO handshake immediately and returns the cartridge index.
func (h *CartridgeHost) AttachCartridge(cartridgeRead io.Reader, cartridgeWrite io.Writer) (int, error) {
	reader := NewFrameReader(cartridgeRead)
	writer := NewFrameWriter(cartridgeWrite)

	manifest, limits, err := HandshakeInitiate(reader, writer)
	if err != nil {
		return -1, fmt.Errorf("handshake failed: %w", err)
	}

	reader.SetLimits(limits)
	writer.SetLimits(limits)

	capGroups, err := parseCapGroupsFromManifest(manifest)
	if err != nil {
		return -1, fmt.Errorf("failed to parse manifest: %w", err)
	}
	caps := flattenCapURNs(capGroups)

	// Verify identity — proves the protocol stack works end-to-end before
	// the cartridge is considered live. Mirrors Rust attach_cartridge,
	// which runs verify_identity after the manifest parse and before the
	// cartridge is recorded as running. A failure rejects the cartridge:
	// it is never appended to the host and the error is returned.
	if err := VerifyIdentity(reader, writer); err != nil {
		return -1, fmt.Errorf("Identity verification failed: %w", err)
	}

	h.mu.Lock()
	cartridgeIdx := len(h.cartridges)

	writerCh := make(chan *Frame, 64)
	cartridge := &ManagedCartridge{
		writerCh:  writerCh,
		manifest:  manifest,
		limits:    limits,
		caps:      caps,
		capGroups: capGroups,
		running:   true,
		// Derive the install identity from the HELLO manifest. Advertisement
		// is identity-gated, so without this the attached cartridge is
		// silently excluded from every RelayNotify and the engine can never
		// route to it (the dev/interop relay path).
		installedIdentity: installedCartridgeRecordFromManifest(manifest),
		pendingHeartbeats: make(map[string]time.Time),
	}
	h.cartridges = append(h.cartridges, cartridge)

	for _, cap := range caps {
		h.capTable = append(h.capTable, capTableEntry{capUrn: cap, cartridgeIdx: cartridgeIdx})
	}
	h.rebuildCapabilities()
	h.mu.Unlock()

	go h.writerLoop(writer, writerCh)
	go h.readerLoop(cartridgeIdx, reader)

	return cartridgeIdx, nil
}

// Capabilities returns the aggregate capabilities of all running cartridges as JSON.
func (h *CartridgeHost) Capabilities() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.capabilities
}

// FindCartridgeForCap finds the cartridge index that can handle a given cap URN.
// Returns (cartridgeIdx, true) if found, (-1, false) if not.
func (h *CartridgeHost) FindCartridgeForCap(capUrn string) (int, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.findCartridgeForCapLocked(capUrn)
}

func (h *CartridgeHost) findCartridgeForCapLocked(capUrn string) (int, bool) {
	// URN-level matching: use is_dispatchable (provider can handle request).
	// No exact-string-match short-circuit — provider selection is purely
	// order-theoretic (specificity distance), mirroring the Rust reference
	// find_cartridge_for_cap. A conformance-equivalent provider registered
	// earlier must win over a later identical-string one, which a string
	// fast-path would wrongly override.
	requestUrn, err := urn.NewCapUrnFromString(capUrn)
	if err != nil {
		return -1, false
	}

	requestSpecificity := requestUrn.Specificity()

	type matchEntry struct {
		cartridgeIdx   int
		signedDistance int
	}
	var matches []matchEntry

	for _, entry := range h.capTable {
		registeredUrn, err := urn.NewCapUrnFromString(entry.capUrn)
		if err != nil {
			continue
		}
		// Use is_dispatchable: can this provider handle this request?
		if registeredUrn.IsDispatchable(requestUrn) {
			specificity := registeredUrn.Specificity()
			signedDistance := specificity - requestSpecificity
			matches = append(matches, matchEntry{entry.cartridgeIdx, signedDistance})
		}
	}

	if len(matches) == 0 {
		return -1, false
	}

	// Rank: non-negative distance (refinement/exact) before negative (fallback),
	// then by smallest absolute distance
	sort.SliceStable(matches, func(i, j int) bool {
		iGroup := 0
		if matches[i].signedDistance < 0 {
			iGroup = 1
		}
		jGroup := 0
		if matches[j].signedDistance < 0 {
			jGroup = 1
		}
		if iGroup != jGroup {
			return iGroup < jGroup
		}
		iAbs := matches[i].signedDistance
		if iAbs < 0 {
			iAbs = -iAbs
		}
		jAbs := matches[j].signedDistance
		if jAbs < 0 {
			jAbs = -jAbs
		}
		return iAbs < jAbs
	})

	return matches[0].cartridgeIdx, true
}

// relayOutbound is an async sink for frames the host sends to the relay. A
// dedicated writer goroutine drains the buffered channel, so the host event
// loop never blocks on relay-read backpressure when it publishes a frame
// (errors, forwarded cartridge frames, RelayNotify inventory). This mirrors the
// reference runtime's unbounded `outbound_tx` mpsc channel — decoupling event
// processing from relay write latency keeps frame ordering deterministic and
// prevents head-of-line blocking from perturbing request/death routing.
type relayOutbound struct {
	ch chan *Frame
}

// WriteFrame enqueues a frame for the relay. Drops the frame only if the writer
// goroutine has already exited (relay closed) — there is nothing to send to.
func (o *relayOutbound) WriteFrame(frame *Frame) {
	defer func() { _ = recover() }() // send on closed channel after relay teardown
	o.ch <- frame
}

// Run runs the main event loop, reading from relay and cartridges.
// Blocks until relay closes or a fatal error occurs.
func (h *CartridgeHost) Run(relayRead io.Reader, relayWrite io.Writer, resourceFn func() []byte) error {
	relayReader := NewFrameReader(relayRead)
	relayWriter := NewFrameWriter(relayWrite)

	relayCh := make(chan *Frame, 64)
	relayDone := make(chan error, 1)
	go func() {
		for {
			frame, err := relayReader.ReadFrame()
			if err != nil {
				if err == io.EOF {
					relayDone <- nil
				} else {
					relayDone <- err
				}
				close(relayCh)
				return
			}
			relayCh <- frame
		}
	}()

	// Async outbound writer: drains queued frames to the relay so the event
	// loop never blocks behind a slow relay reader.
	out := &relayOutbound{ch: make(chan *Frame, 256)}
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for frame := range out.ch {
			if err := relayWriter.WriteFrame(frame); err != nil {
				// Relay closed mid-write. Drain remaining queued frames
				// without writing so WriteFrame senders never block, then
				// exit when the channel is closed at teardown.
				for range out.ch {
				}
				return
			}
		}
	}()

	// Send the initial RelayNotify so the relay/engine knows about
	// pre-registered cartridges (and an empty inventory when none are
	// registered yet), exactly like the reference run loop. Without this
	// a SyncRoster-added cartridge would be the engine's FIRST
	// RelayNotify, hiding the "absent → present" transition.
	h.mu.Lock()
	h.rebuildCapabilitiesLocked(out)
	h.mu.Unlock()

	// Periodic RelayNotify republish. The initial advertisement above (and
	// the one the RelaySlave injects on connect) is consumed by the relay
	// switch during its identity-verification handshake, so the engine only
	// learns the live inventory from a RelayNotify republished *after* the
	// switch finishes initializing. Republishing on a timer guarantees one
	// lands post-handshake (and keeps request/heartbeat stats fresh
	// thereafter). Only fires when a cartridge is running, keeping idle hosts
	// quiet. Mirrors the reference run loop's stats_interval
	// (capdag/src/bifaci/host_runtime.rs).
	statsTicker := time.NewTicker(2 * time.Second)
	defer statsTicker.Stop()

	for {
		select {
		case frame, ok := <-relayCh:
			if !ok {
				err := <-relayDone
				h.killAllCartridges()
				close(out.ch)
				<-writerDone
				return err
			}
			if ferr := h.handleRelayFrame(frame, out); ferr != nil {
				h.killAllCartridges()
				close(out.ch)
				<-writerDone
				return ferr
			}

		case event := <-h.eventCh:
			if event.isDeath {
				h.handleCartridgeDeath(event.cartridgeIdx, out)
			} else if event.frame != nil {
				h.handleCartridgeFrame(event.cartridgeIdx, event.frame, out)
			}

		case cmd := <-h.commandCh:
			if cmd.isSync {
				h.syncRegisteredRoster(cmd.syncRoster, out)
			}

		case <-statsTicker.C:
			h.mu.Lock()
			anyRunning := false
			for _, c := range h.cartridges {
				if c.running {
					anyRunning = true
					break
				}
			}
			if anyRunning {
				h.rebuildCapabilitiesLocked(out)
			}
			h.mu.Unlock()
		}
	}
}

// syncRegisteredRoster replaces the live registered-dir roster with a
// freshly-discovered set and re-publishes RelayNotify, so the engine sees
// added/removed cartridges without reconnecting — the equivalent of the macOS
// XPC service's syncDiscoveryOutcomes after a rescan. Running cartridges no
// longer in the set are killed; survivors keep their live process and stats.
// Only dir-registered cartridges are touched; attached/internal providers are
// not part of a dir roster sync.
func (h *CartridgeHost) syncRegisteredRoster(desired []RegisteredDirSpec, relayWriter *relayOutbound) {
	h.mu.Lock()
	defer h.mu.Unlock()

	type identity struct {
		registryURL string
		hasURL      bool
		channel     CartridgeChannel
		id          string
		version     string
	}
	recIdentity := func(rec *InstalledCartridgeRecord) identity {
		id := identity{channel: CartridgeChannel(rec.Channel), id: rec.Id, version: rec.Version}
		if rec.RegistryURL != nil {
			id.registryURL = *rec.RegistryURL
			id.hasURL = true
		}
		return id
	}
	specIdentity := func(s *RegisteredDirSpec) identity {
		id := identity{channel: s.Channel, id: s.Id, version: s.Version}
		if s.RegistryURL != nil {
			id.registryURL = *s.RegistryURL
			id.hasURL = true
		}
		return id
	}

	desiredKeys := make(map[identity]struct{}, len(desired))
	for i := range desired {
		desiredKeys[specIdentity(&desired[i])] = struct{}{}
	}

	// Retire registered-dir cartridges no longer desired.
	for idx := range h.cartridges {
		cartridge := h.cartridges[idx]
		if cartridge.removed {
			continue
		}
		rec := cartridge.installedCartridgeRecord()
		if rec == nil {
			continue // no resolvable identity (e.g. internal provider) — leave it
		}
		if !cartridge.isRegisteredDir() {
			continue // attached/internal providers are not part of a dir roster sync
		}
		if _, ok := desiredKeys[recIdentity(rec)]; ok {
			continue // still desired — keep, preserving any live process
		}
		if cartridge.running {
			if cartridge.writerCh != nil {
				close(cartridge.writerCh)
				cartridge.writerCh = nil
			}
			if cartridge.cmd != nil && cartridge.cmd.Process != nil {
				cartridge.cmd.Process.Kill()
				cartridge.cmd = nil
			}
			cartridge.running = false
		}
		cartridge.removed = true     // retire: drop from cap table + inventory
		cartridge.helloFailed = true // keep out of dispatch/spawn paths
	}

	// Add newly-desired specs not already registered.
	presentKeys := make(map[identity]struct{})
	for idx := range h.cartridges {
		cartridge := h.cartridges[idx]
		if cartridge.helloFailed {
			continue
		}
		if rec := cartridge.installedCartridgeRecord(); rec != nil {
			presentKeys[recIdentity(rec)] = struct{}{}
		}
	}
	for i := range desired {
		if _, ok := presentKeys[specIdentity(&desired[i])]; ok {
			continue
		}
		h.registerCartridgeDirLocked(desired[i])
	}

	h.updateCapTable()
	h.rebuildCapabilitiesLocked(relayWriter)
}

// handleRelayFrame routes an incoming frame from the relay to the
// correct cartridge. Mirrors the Rust `handle_relay_frame` design
// in capdag/src/bifaci/host_runtime.rs.
//
// PATH B (REQ from relay): cap dispatch picks a cartridge, the
// (XID, RID) → cartridge_idx mapping is recorded in
// `incomingRxids`, and the frame is forwarded to the cartridge
// (still carrying the XID).
//
// PATH C (continuation frames from relay): route by checking
// `incomingRxids[(xid, rid)]` first (request body phase) and
// falling back to `outgoingRids[rid]` (peer response phase). For
// self-loop peer requests the same RID exists in both maps; the
// XID disambiguates because the body's END (which removes
// `incomingRxids[(xid, rid)]`) always precedes the peer response's
// frames on a single ordered relay socket.
func (h *CartridgeHost) handleRelayFrame(frame *Frame, relayWriter *relayOutbound) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch frame.FrameType {
	case FrameTypeReq:
		// All frames from the relay carry an XID assigned by the
		// RelaySwitch. Cartridge-bound REQs without one are a
		// protocol error from upstream.
		if frame.RoutingId == nil {
			return fmt.Errorf("REQ from relay missing XID — RelaySwitch must stamp routing_id")
		}
		xid := *frame.RoutingId

		capUrn := ""
		if frame.Cap != nil {
			capUrn = *frame.Cap
		}

		cartridgeIdx, found := h.findCartridgeForCapLocked(capUrn)
		if !found {
			// No dispatchable cartridge for a planned cap is a
			// deployment/manifest mismatch — Environment.
			errFrame := NewErrClassified(frame.Id, "NO_HANDLER", FailureClassEnvironment, fmt.Sprintf("no cartridge handles cap: %s", capUrn))
			errFrame.RoutingId = frame.RoutingId
			relayWriter.WriteFrame(errFrame)
			return nil
		}

		cartridge := h.cartridges[cartridgeIdx]
		if !cartridge.running {
			if cartridge.helloFailed {
				// A cartridge that cannot be started is a broken runtime
				// deployment — Environment.
				errFrame := NewErrClassified(frame.Id, "SPAWN_FAILED", FailureClassEnvironment, "cartridge previously failed to start")
				errFrame.RoutingId = frame.RoutingId
				relayWriter.WriteFrame(errFrame)
				return nil
			}
			if err := h.spawnCartridgeLocked(cartridgeIdx); err != nil {
				errFrame := NewErrClassified(frame.Id, "SPAWN_FAILED", FailureClassEnvironment, err.Error())
				errFrame.RoutingId = frame.RoutingId
				relayWriter.WriteFrame(errFrame)
				return nil
			}
		}

		// Record (XID, RID) → cartridge for routing the request
		// body's continuation frames. The cartridge receives the
		// REQ with XID intact so its response frames carry the
		// same XID back to the relay.
		key := makeRxidKey(xid, frame.Id)
		h.incomingRxids[key] = incomingRoute{
			cartridgeIdx: cartridgeIdx,
			xid:          xid,
			rid:          frame.Id,
		}
		h.touchIncomingRxid(key)
		h.gcRoutingTablesIfNeeded()
		if err := h.sendToCartridge(cartridgeIdx, frame); err != nil {
			// Cartridge died between dispatch and delivery. Synthesize
			// a terminal ERR back to the relay so the engine doesn't
			// wait forever, and tear down the routing entry we just
			// created — `handleCartridgeDeath` won't see it because
			// the death event may already have been processed.
			// Mirrors Rust host_runtime.rs:1438.
			delete(h.incomingRxids, key)
			delete(h.incomingRxidsTouched, key)
			// A dead cartridge process is a runtime-environment failure —
			// Environment (docs/failure-taxonomy.md).
			errFrame := NewErrClassified(frame.Id, "CARTRIDGE_DIED", FailureClassEnvironment, err.Error())
			errFrame.RoutingId = frame.RoutingId
			relayWriter.WriteFrame(errFrame)
		}

	case FrameTypeStreamStart, FrameTypeChunk, FrameTypeStreamEnd, FrameTypeEnd, FrameTypeErr, FrameTypeCredit:
		// Continuation frame from the relay. CREDIT rides the same route
		// as data continuations (protocol v3, L11): it targets whichever
		// cartridge is sending the credited stream — the handler
		// cartridge for a normal request (via incomingRxids) or the
		// requester cartridge for a peer call's argument streams (via
		// outgoingRids). Two possibilities for data/terminal frames:
		//   1. Body phase — `incomingRxids[(xid, rid)]` says which
		//      cartridge is handling the original request. Body END no
		//      longer drops the entry (v3) — see incomingBodyDone below.
		//   2. Response phase — `outgoingRids[rid]` says which
		//      cartridge sent the peer REQ; the relay is now
		//      delivering the response back. END/ERR here marks
		//      the end of the response and drops the entry.
		//
		// MUST have XID. Frames from the relay without XID are a
		// protocol violation upstream.
		if frame.RoutingId == nil {
			return fmt.Errorf("%v from relay missing XID — all frames from relay must have XID", frame.FrameType)
		}
		xid := *frame.RoutingId
		key := makeRxidKey(xid, frame.Id)

		// Route selection:
		//   - CREDIT routes by its mandatory direction (L11): a
		//     `response` grant credits the HANDLER's output → incoming
		//     side; a `request` grant credits the REQUESTER's argument
		//     streams → outgoing side. The (xid, rid) key alone cannot
		//     distinguish these for self-loop peer calls.
		//   - Data/terminal frames prefer the incoming side while the
		//     request body is still flowing; after body END they are
		//     self-loop peer responses and fall through to outgoing.
		var preferIncoming bool
		if frame.FrameType == FrameTypeCredit {
			dir := frame.CreditDirectionValue()
			if dir == nil {
				// Dropped: v3 requires credit_dir on every CREDIT frame —
				// never a silent loss (no_route, L6/L8).
				h.drops.Record(DropReasonNoRoute)
				return nil
			}
			preferIncoming = *dir == CreditDirectionResponse
		} else {
			_, bodyDone := h.incomingBodyDone[key]
			preferIncoming = !bodyDone
		}

		var (
			cartridgeIdx      int
			routedViaIncoming bool
			haveRoute         bool
		)
		if preferIncoming {
			if route, ok := h.incomingRxids[key]; ok {
				h.touchIncomingRxid(key)
				cartridgeIdx = route.cartridgeIdx
				routedViaIncoming = true
				haveRoute = true
			}
		}
		if !haveRoute {
			if route, ok := h.outgoingRids[frame.Id.ToString()]; ok {
				h.touchOutgoingRid(frame.Id.ToString())
				cartridgeIdx = route.cartridgeIdx
				routedViaIncoming = false
				haveRoute = true
			}
		}
		if !haveRoute {
			// Fallback: no outgoing entry, so this cannot be a
			// self-loop peer response — route to the handler even
			// post-body-END (defensive; normal requests only ever
			// see CREDIT here, handled above).
			if route, ok := h.incomingRxids[key]; ok {
				h.touchIncomingRxid(key)
				cartridgeIdx = route.cartridgeIdx
				routedViaIncoming = true
				haveRoute = true
			}
		}
		if !haveRoute {
			// No routing — the request was already torn down (e.g.
			// after cartridge death). A counted no_route drop (L6/L8),
			// never a silent loss.
			h.drops.Record(DropReasonNoRoute)
			return nil
		}

		isTerminal := frame.FrameType == FrameTypeEnd || frame.FrameType == FrameTypeErr

		if err := h.sendToCartridge(cartridgeIdx, frame); err != nil {
			// Cartridge died while we were routing a body- or
			// response-phase continuation. Synthesize a terminal
			// ERR so the engine sees a defined outcome rather than
			// silently losing the in-flight request. Tear down the
			// matching routing entry whether or not this frame was
			// itself terminal — there is nothing left to route.
			// Mirrors Rust host_runtime.rs:1438-1465.
			if routedViaIncoming {
				delete(h.incomingRxids, key)
				delete(h.incomingRxidsTouched, key)
				delete(h.incomingBodyDone, key)
				delete(h.incomingResponseDone, key)
			} else {
				delete(h.outgoingRids, frame.Id.ToString())
				delete(h.outgoingRidsTouched, frame.Id.ToString())
			}
			errFrame := NewErrClassified(frame.Id, "CARTRIDGE_DIED", FailureClassEnvironment, err.Error())
			errFrame.RoutingId = frame.RoutingId
			relayWriter.WriteFrame(errFrame)
			return nil
		}

		// Terminal bookkeeping.
		//   - Via incomingRxids: the REQUEST BODY completed. The entry
		//     STAYS — the handler's response is still flowing and its
		//     output CREDIT grants route through it (v3). It is removed
		//     when the handler's response terminal passes outbound
		//     (handleCartridgeFrame) or on cartridge death.
		//   - Via outgoingRids: a peer RESPONSE completed — clean up.
		if isTerminal {
			if routedViaIncoming {
				if _, responseAlreadyDone := h.incomingResponseDone[key]; responseAlreadyDone {
					// Response already terminated (response-first
					// race): the request is fully over — release.
					delete(h.incomingResponseDone, key)
					delete(h.incomingRxids, key)
					delete(h.incomingRxidsTouched, key)
				} else {
					h.incomingBodyDone[key] = struct{}{}
				}
			} else {
				// Peer response phase done. Drop the requester's
				// entry so the next REQ with the same RID
				// (extremely unlikely with UUIDs but possible
				// with deterministic id allocators) starts fresh.
				delete(h.outgoingRids, frame.Id.ToString())
				delete(h.outgoingRidsTouched, frame.Id.ToString())
			}
		}

	case FrameTypeHeartbeat:
		// Engine-level heartbeat — not forwarded to cartridges.
		return nil

	case FrameTypeHello:
		return fmt.Errorf("unexpected HELLO from relay")

	case FrameTypeRelayNotify, FrameTypeRelayState:
		return fmt.Errorf("relay frame %v reached cartridge host", frame.FrameType)

	case FrameTypeLog:
		// LOG frames from peer responses — route to the cartridge
		// that made the peer request, identified by
		// `outgoingRids[rid]`. Mirrors Rust handling. LOG is a
		// best-effort side channel: if the requesting cartridge has
		// died, the LOG line is simply lost — no terminal ERR is
		// synthesized for a LOG, since LOG is not part of the
		// request-lifecycle frame set.
		if route, ok := h.outgoingRids[frame.Id.ToString()]; ok {
			_ = h.sendToCartridge(route.cartridgeIdx, frame)
		}
		return nil
	}

	return nil
}

// handleCartridgeFrame processes a frame from a cartridge. Mirrors the
// Rust `handle_cartridge_frame` design in capdag/src/bifaci/host_runtime.rs.
//
// PATH A (REQ from cartridge — peer invoke): MUST NOT carry an XID
// (cartridges never assign XIDs; the RelaySwitch does). Recorded in
// `outgoingRids[rid]` so the eventual peer response can be routed
// back to the requesting cartridge. Forwarded as-is.
//
// PATH A (continuation frames from cartridge): forwarded as-is. No
// routing-table cleanup happens here — `incomingRxids` is cleared
// only when the request BODY's END arrives from the relay (in
// `handleRelayFrame`), because cartridge response END and relay
// body END race independently.
func (h *CartridgeHost) handleCartridgeFrame(cartridgeIdx int, frame *Frame, relayWriter *relayOutbound) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch frame.FrameType {
	case FrameTypeHeartbeat:
		cartridge := h.cartridges[cartridgeIdx]
		probeKey := frame.Id.ToString()
		if _, isOurProbe := cartridge.pendingHeartbeats[probeKey]; isOurProbe {
			delete(cartridge.pendingHeartbeats, probeKey)
			// Response to our health probe — cartridge is alive.
			// Cumulative protocol drop counter (L8). The reading is the
			// cartridge's running total — stored as-is, never merged or
			// maxed. Mirrors Rust's ingestion of `drops_total` from
			// heartbeat response meta.
			if frame.Meta != nil {
				if v, ok := extractUint64FromMeta(frame.Meta, "drops_total"); ok {
					cartridge.protocolDropsTotal = &v
				}
			}
			// Stamp the round-trip completion timestamp so the
			// runtime-stats snapshot can surface heartbeat age to the UI.
			now := unixSecondsNow()
			cartridge.LastHeartbeatUnixSeconds = &now
		} else {
			// Cartridge-initiated heartbeat — respond immediately.
			// Best-effort: if the cartridge has already died, the
			// heartbeat reply is dropped and the death will surface on
			// the next reader-loop iteration.
			response := NewHeartbeat(frame.Id)
			_ = h.sendToCartridge(cartridgeIdx, response)
		}

	case FrameTypeHello:
		// HELLO post-handshake — protocol violation, ignore.
		return

	case FrameTypeRelayNotify, FrameTypeRelayState:
		// Cartridges must never send relay frames.
		return

	case FrameTypeReq:
		// PATH A: peer invoke. Must not carry XID.
		if frame.RoutingId != nil {
			return
		}
		h.outgoingRids[frame.Id.ToString()] = outgoingRoute{
			cartridgeIdx: cartridgeIdx,
			rid:          frame.Id,
		}
		h.touchOutgoingRid(frame.Id.ToString())
		h.gcRoutingTablesIfNeeded()
		relayWriter.WriteFrame(frame)

	default:
		// Continuation frames (StreamStart/Chunk/StreamEnd/End/Err/Log).
		// Forward as-is, with whatever XID the cartridge stamped (it
		// echoes back the XID it received on the inbound REQ).
		isTerminal := frame.FrameType == FrameTypeEnd || frame.FrameType == FrameTypeErr
		if isTerminal && frame.RoutingId != nil {
			// The handler's RESPONSE terminal is the request's true end
			// at this host (v3): once the body has completed too,
			// release the incoming routing entry and its body-done
			// marker. If the response terminates BEFORE the body END
			// arrives (response-first race), remember it so the body
			// END releases the entry immediately.
			key := makeRxidKey(*frame.RoutingId, frame.Id)
			if _, bodyDone := h.incomingBodyDone[key]; bodyDone {
				delete(h.incomingBodyDone, key)
				delete(h.incomingRxids, key)
				delete(h.incomingRxidsTouched, key)
			} else if _, stillIncoming := h.incomingRxids[key]; stillIncoming {
				h.incomingResponseDone[key] = struct{}{}
			}
		}
		relayWriter.WriteFrame(frame)
	}
}

// extractUint64FromMeta reads an unsigned integer from a frame meta map,
// handling CBOR type variance (int, int64, uint64, float64). Returns
// (0, false) if the key is absent — the caller MUST treat absence as "no
// reading", never a fabricated zero (mirrors extractIntFromMeta's type
// handling, but preserves presence so callers can distinguish "not yet
// measured" from "measured as zero").
func extractUint64FromMeta(meta map[string]interface{}, key string) (uint64, bool) {
	v, ok := meta[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return uint64(n), true
	case int64:
		return uint64(n), true
	case uint64:
		return n, true
	case float64:
		return uint64(n), true
	default:
		return 0, false
	}
}

// handleCartridgeDeath processes a cartridge death event.
func (h *CartridgeHost) handleCartridgeDeath(cartridgeIdx int, relayWriter *relayOutbound) {
	h.mu.Lock()
	defer h.mu.Unlock()

	cartridge := h.cartridges[cartridgeIdx]
	cartridge.running = false

	if cartridge.writerCh != nil {
		close(cartridge.writerCh)
		cartridge.writerCh = nil
	}

	if cartridge.cmd != nil && cartridge.cmd.Process != nil {
		cartridge.cmd.Process.Kill()
		cartridge.cmd = nil
	}

	// Send ERR for all pending requests this cartridge was involved
	// in — both the request bodies it was handling (incomingRxids)
	// and the peer requests it had outstanding (outgoingRids).
	// Mirrors Rust's heartbeat-timeout cleanup in
	// capdag/src/bifaci/host_runtime.rs:1950. The typed XID/RID are
	// stored on the route struct so this path can synthesize a
	// terminal ERR with the correct id form (UUID or uint) without
	// re-parsing the lookup key — re-parsing would lose the uint
	// variant and silently drop the request, leaving the engine
	// blocked forever on a frame that never arrives.
	var incomingKeys []rxidKey
	for key, route := range h.incomingRxids {
		if route.cartridgeIdx != cartridgeIdx {
			continue
		}
		errFrame := NewErrClassified(route.rid, "CARTRIDGE_DIED", FailureClassEnvironment, fmt.Sprintf("cartridge %d died", cartridgeIdx))
		xid := route.xid
		errFrame.RoutingId = &xid
		relayWriter.WriteFrame(errFrame)
		incomingKeys = append(incomingKeys, key)
	}
	for _, key := range incomingKeys {
		delete(h.incomingRxids, key)
		delete(h.incomingRxidsTouched, key)
		delete(h.incomingBodyDone, key)
		delete(h.incomingResponseDone, key)
	}

	var outgoingKeys []string
	for key, route := range h.outgoingRids {
		if route.cartridgeIdx != cartridgeIdx {
			continue
		}
		errFrame := NewErrClassified(route.rid, "CARTRIDGE_DIED", FailureClassEnvironment, fmt.Sprintf("cartridge %d died", cartridgeIdx))
		relayWriter.WriteFrame(errFrame)
		outgoingKeys = append(outgoingKeys, key)
	}
	for _, key := range outgoingKeys {
		delete(h.outgoingRids, key)
		delete(h.outgoingRidsTouched, key)
	}

	h.updateCapTable()
	// Republish the inventory so the engine sees the cartridge leave (and any
	// surviving cartridges' updated running state).
	h.rebuildCapabilitiesLocked(relayWriter)
}

// sendToCartridge sends a frame to a cartridge via its writer
// channel. Returns a non-nil error if the cartridge is unreachable
// (already dead, never spawned, or writerCh closed by the death
// handler). The caller is responsible for synthesizing a terminal
// ERR back to the relay when this fails on a request-body frame —
// see `handleRelayFrame`. Mirrors the Rust `send_to_cartridge`
// semantics in capdag/src/bifaci/host_runtime.rs:1438.
func (h *CartridgeHost) sendToCartridge(cartridgeIdx int, frame *Frame) error {
	cartridge := h.cartridges[cartridgeIdx]
	if cartridge.writerCh == nil || !cartridge.running {
		return fmt.Errorf("cartridge %d is not running", cartridgeIdx)
	}
	select {
	case cartridge.writerCh <- frame:
		return nil
	default:
		// Channel full and stuck — treat as dead so the engine sees
		// a terminal ERR rather than waiting forever for a frame
		// that will never be processed.
		return fmt.Errorf("cartridge %d writer channel full", cartridgeIdx)
	}
}

// writerLoop reads frames from the channel and writes them to the cartridge.
func (h *CartridgeHost) writerLoop(writer *FrameWriter, ch chan *Frame) {
	for frame := range ch {
		if err := writer.WriteFrame(frame); err != nil {
			return
		}
	}
}

// readerLoop reads frames from a cartridge and sends events to the event channel.
func (h *CartridgeHost) readerLoop(cartridgeIdx int, reader *FrameReader) {
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			h.eventCh <- cartridgeEvent{cartridgeIdx: cartridgeIdx, isDeath: true}
			return
		}
		h.eventCh <- cartridgeEvent{cartridgeIdx: cartridgeIdx, frame: frame}
	}
}

// spawnCartridgeLocked spawns a registered cartridge process (caller must hold mu).
func (h *CartridgeHost) spawnCartridgeLocked(cartridgeIdx int) error {
	cartridge := h.cartridges[cartridgeIdx]
	if cartridge.path == "" {
		cartridge.helloFailed = true
		return fmt.Errorf("cartridge has no path")
	}

	cmd := exec.Command(cartridge.path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cartridge.helloFailed = true
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cartridge.helloFailed = true
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cartridge.helloFailed = true
		return fmt.Errorf("failed to start cartridge: %w", err)
	}
	cartridge.cmd = cmd

	reader := NewFrameReader(stdout)
	writer := NewFrameWriter(stdin)

	manifest, limits, err := HandshakeInitiate(reader, writer)
	if err != nil {
		cartridge.helloFailed = true
		cmd.Process.Kill()
		return fmt.Errorf("handshake failed: %w", err)
	}

	reader.SetLimits(limits)
	writer.SetLimits(limits)

	capGroups, parseErr := parseCapGroupsFromManifest(manifest)
	if parseErr != nil {
		cartridge.helloFailed = true
		cmd.Process.Kill()
		return fmt.Errorf("failed to parse manifest: %w", parseErr)
	}

	cartridge.manifest = manifest
	cartridge.limits = limits
	cartridge.capGroups = capGroups
	cartridge.caps = flattenCapURNs(capGroups)
	cartridge.running = true

	writerCh := make(chan *Frame, 64)
	cartridge.writerCh = writerCh

	h.updateCapTable()
	h.rebuildCapabilities()

	go h.writerLoop(writer, writerCh)
	go h.readerLoop(cartridgeIdx, reader)

	return nil
}

// updateCapTable rebuilds the cap table from all cartridges.
func (h *CartridgeHost) updateCapTable() {
	h.capTable = nil
	for idx, cartridge := range h.cartridges {
		if cartridge.helloFailed {
			continue
		}
		for _, capURN := range cartridge.caps {
			h.capTable = append(h.capTable, capTableEntry{capUrn: capURN, cartridgeIdx: idx})
		}
	}
}

// buildInstalledCartridgeIdentities builds the installed-cartridge inventory
// the host advertises to the engine. Cartridges that have permanently failed
// HELLO are filtered out, and so are cartridges with no resolvable
// installedIdentity — identity gates advertisement, mirroring the reference
// build_installed_cartridge_identities. Every registration path
// (RegisterCartridge / RegisterCartridgeDir / AttachCartridge) now stamps a
// real (registry_url, channel, id, version) identity, so a nil identity here
// means a genuinely anchorless cartridge that is correctly dropped rather than
// advertised under a fabricated id. The base identity is overlaid with the live
// cap_groups + runtime stats before emission.
func (h *CartridgeHost) buildInstalledCartridgeIdentities() []InstalledCartridgeRecord {
	activeCounts := make(map[int]uint64)
	for _, route := range h.incomingRxids {
		activeCounts[route.cartridgeIdx]++
	}
	peerCounts := make(map[int]uint64)
	for _, route := range h.outgoingRids {
		peerCounts[route.cartridgeIdx]++
	}

	var installed []InstalledCartridgeRecord
	for idx, cartridge := range h.cartridges {
		// Retired installs are gone from the inventory entirely —
		// retirement is not a failure, there is nothing to report.
		// Mirrors the reference build_installed_cartridge_identities.
		if cartridge.removed {
			continue
		}

		// cap_groups is the manifest-derived source of truth, captured at
		// probe-time HELLO for both the dir-registered and binary-registered
		// paths.
		capGroups := cartridge.capGroups

		stats := &CartridgeRuntimeStats{
			Running:                  cartridge.running,
			ActiveRequestCount:       activeCounts[idx],
			PeerRequestCount:         peerCounts[idx],
			LastHeartbeatUnixSeconds: cartridge.LastHeartbeatUnixSeconds,
			RestartCount:             cartridge.RestartCount,
			ProtocolDropsTotal:       cartridge.protocolDropsTotal,
		}
		if cartridge.cmd != nil && cartridge.cmd.Process != nil {
			pid := uint32(cartridge.cmd.Process.Pid)
			stats.PID = &pid
		}

		// Identity gates advertisement. A cartridge with no resolvable
		// installedIdentity is NOT part of the inventory the engine can route
		// to and is dropped — mirroring the reference
		// build_installed_cartridge_identities. (Attached cartridges get a
		// manifest-derived identity at attach time, so a nil identity here
		// means a genuinely anchorless cartridge; we expose that by dropping
		// it rather than fabricating a synthetic `cartridge-N` record that
		// would hide the gap.)
		rec := cartridge.installedCartridgeRecord()
		if rec == nil {
			continue
		}
		// Copy the base identity and overlay the runtime stats.
		out := *rec
		out.RuntimeStats = stats

		// A cartridge whose HELLO permanently failed (e.g. a pre-v3 binary
		// hard-rejected by the version check) stays IN the inventory with
		// an attachment error — never silently absent. It carries no
		// cap_groups, so it is never routable. Mirrors the reference
		// build_installed_cartridge_identities.
		if cartridge.helloFailed {
			out.CapGroups = nil
			out.AttachmentError = &CartridgeAttachmentError{
				Kind: CartridgeAttachmentErrorKindHandshakeFailed,
				Message: "HELLO handshake failed (protocol version mismatch or " +
					"malformed manifest) — rebuild the cartridge against the " +
					"current protocol",
				DetectedAtUnixSeconds: unixSecondsNow(),
			}
			installed = append(installed, out)
			continue
		}

		// Healthy: overlay the live cap_groups (source of truth from HELLO).
		out.CapGroups = capGroups
		installed = append(installed, out)
	}
	// Discovery outcomes the host doesn't manage (incompatible installs)
	// ride every advertisement so no republish can erase them.
	installed = append(installed, h.staticInventoryRecords...)
	return installed
}

// rebuildCapabilities rebuilds the aggregate capabilities JSON without
// publishing a RelayNotify frame (the initialization path — mirrors the
// reference rebuild_capabilities(None)).
func (h *CartridgeHost) rebuildCapabilities() {
	h.rebuildCapabilitiesLocked(nil)
}

// rebuildCapabilitiesLocked rebuilds the aggregate capabilities JSON and, when
// relayWriter is non-nil, publishes a RelayNotify frame so the relay/engine
// tracks capability changes dynamically as cartridges connect / disconnect /
// fail / are roster-synced. Caller must hold h.mu.
func (h *CartridgeHost) rebuildCapabilitiesLocked(relayWriter *relayOutbound) {
	installed := h.buildInstalledCartridgeIdentities()

	if len(installed) == 0 {
		h.capabilities = nil
	} else {
		payload := RelayNotifyCapabilitiesPayload{InstalledCartridges: installed}
		capsJSON, err := json.Marshal(payload)
		if err != nil {
			h.capabilities = nil
		} else {
			h.capabilities = capsJSON
		}
	}

	if relayWriter == nil {
		return
	}
	// Publish a RelayNotify so the engine sees the current inventory. An empty
	// roster is published as an empty installed_cartridges list (not skipped)
	// so the engine can observe a cartridge being retired.
	payload := RelayNotifyCapabilitiesPayload{InstalledCartridges: installed}
	if installed == nil {
		payload.InstalledCartridges = []InstalledCartridgeRecord{}
	}
	notifyBytes, err := json.Marshal(payload)
	if err != nil {
		return
	}
	// Advertise the host's REAL aggregate limits — the element-wise minimum
	// over every running cartridge's negotiated handshake limits. Sending
	// defaults here would clobber genuine negotiations on every republish.
	// Mirrors Rust CartridgeHostRuntime::rebuild_capabilities.
	limits := h.aggregateLimits()
	frame := NewRelayNotify(notifyBytes, limits.MaxFrame, limits.MaxChunk, limits.MaxReorderBuffer, limits.InitialCredit)
	relayWriter.WriteFrame(frame)
}

// aggregateLimits computes the element-wise minimum over the negotiated
// limits of every running cartridge; defaults when none are running. This is
// what the host is actually able to honor across its fleet. Caller must hold
// h.mu. Mirrors Rust CartridgeHostRuntime::aggregate_limits.
func (h *CartridgeHost) aggregateLimits() Limits {
	limits := DefaultLimits()
	for _, cartridge := range h.cartridges {
		if !cartridge.running {
			continue
		}
		limits.MaxFrame = min(limits.MaxFrame, cartridge.limits.MaxFrame)
		limits.MaxChunk = min(limits.MaxChunk, cartridge.limits.MaxChunk)
		limits.MaxReorderBuffer = min(limits.MaxReorderBuffer, cartridge.limits.MaxReorderBuffer)
		limits.InitialCredit = min(limits.InitialCredit, cartridge.limits.InitialCredit)
	}
	return limits
}

// killAllCartridges stops all managed cartridges.
func (h *CartridgeHost) killAllCartridges() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, cartridge := range h.cartridges {
		if cartridge.writerCh != nil {
			close(cartridge.writerCh)
			cartridge.writerCh = nil
		}
		if cartridge.cmd != nil && cartridge.cmd.Process != nil {
			cartridge.cmd.Process.Kill()
		}
		cartridge.running = false
	}
}

// ParseCapsError is the reason a manifest was rejected by
// parseCapGroupsFromManifest. It carries the specific failure mode so the
// caller can pick the right CartridgeAttachmentErrorKind — ManifestInvalid when
// the JSON itself is malformed, Incompatible when the JSON parses but violates
// the cartridge schema (missing CAP_IDENTITY, old shape, etc.). Mirrors the
// Rust ParseCapsError enum in capdag/src/bifaci/host_runtime.rs.
type ParseCapsError struct {
	// Incompatible is true when the JSON parsed but the manifest is
	// structurally incompatible with the host's expectations (e.g.
	// missing CAP_IDENTITY). When false, the JSON itself failed to parse.
	Incompatible bool
	Message      string
}

func (e *ParseCapsError) Error() string {
	return e.Message
}

// AttachmentKind maps the parse failure to a CartridgeAttachmentErrorKind:
// ManifestInvalid for JSON-level failures, Incompatible for schema failures.
// Mirrors Rust ParseCapsError::attachment_kind.
func (e *ParseCapsError) AttachmentKind() CartridgeAttachmentErrorKind {
	if e.Incompatible {
		return CartridgeAttachmentErrorKindIncompatible
	}
	return CartridgeAttachmentErrorKindManifestInvalid
}

// parseCapGroupsFromManifest parses the cartridge's cap_groups from a
// JSON manifest. Returns the full CapGroup slice — the engine needs it
// to register adapter URNs per-cartridge, and the flat cap-urn list is
// derived from it.
//
// A manifest that does not parse as JSON is rejected as a JSON-level failure
// (ManifestInvalid); a manifest that parses but does not declare CAP_IDENTITY
// is rejected as Incompatible. Both are returned as *ParseCapsError so the
// caller can surface the right attachment-error kind. Mirrors the Rust
// parse_cap_groups_from_manifest.
func parseCapGroupsFromManifest(manifest []byte) ([]CapGroup, error) {
	if len(manifest) == 0 {
		return nil, nil
	}

	var parsed struct {
		CapGroups []CapGroup `json:"cap_groups"`
	}

	if err := json.Unmarshal(manifest, &parsed); err != nil {
		return nil, &ParseCapsError{
			Incompatible: false,
			Message:      fmt.Sprintf("Invalid CapManifest from cartridge: %v", err),
		}
	}

	if len(parsed.CapGroups) == 0 {
		return nil, &ParseCapsError{
			Incompatible: false,
			Message:      "manifest missing required cap_groups array",
		}
	}

	// The manifest must declare the standard identity cap. The host's
	// identity-verification handshake depends on it, and a manifest that
	// omits it is structurally incompatible (Incompatible, not
	// ManifestInvalid).
	identityUrn, err := urn.NewCapUrnFromString(standard.CapIdentity)
	if err != nil {
		panic(fmt.Sprintf("BUG: CAP_IDENTITY constant is invalid: %v", err))
	}
	hasIdentity := false
	for _, group := range parsed.CapGroups {
		for i := range group.Caps {
			capUrn := group.Caps[i].Urn
			if capUrn != nil && identityUrn.ConformsTo(capUrn) {
				hasIdentity = true
				break
			}
		}
		if hasIdentity {
			break
		}
	}
	if !hasIdentity {
		return nil, &ParseCapsError{
			Incompatible: true,
			Message:      fmt.Sprintf("Cartridge manifest missing required CAP_IDENTITY (%s)", standard.CapIdentity),
		}
	}

	return parsed.CapGroups, nil
}

// flattenCapURNs walks a slice of CapGroups and returns the flat list of
// cap URN strings, preserving order.
func flattenCapURNs(groups []CapGroup) []string {
	var caps []string
	for _, group := range groups {
		for _, c := range group.Caps {
			urn := c.Urn.String()
			if urn != "" {
				caps = append(caps, urn)
			}
		}
	}
	return caps
}
