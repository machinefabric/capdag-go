package bifaci

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	cbor2 "github.com/fxamacker/cbor/v2"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// RelaySwitchError represents errors from relay switch operations
type RelaySwitchError struct {
	Type    RelaySwitchErrorType
	Message string
}

type RelaySwitchErrorType int

const (
	RelaySwitchErrorTypeCbor RelaySwitchErrorType = iota
	RelaySwitchErrorTypeIO
	RelaySwitchErrorTypeNoHandler
	RelaySwitchErrorTypeUnknownRequest
	RelaySwitchErrorTypeProtocol
	RelaySwitchErrorTypeAllMastersUnhealthy
)

func (e *RelaySwitchError) Error() string {
	switch e.Type {
	case RelaySwitchErrorTypeCbor:
		return fmt.Sprintf("relay switch CBOR error: %s", e.Message)
	case RelaySwitchErrorTypeIO:
		return fmt.Sprintf("relay switch I/O error: %s", e.Message)
	case RelaySwitchErrorTypeNoHandler:
		return fmt.Sprintf("no handler found for cap: %s", e.Message)
	case RelaySwitchErrorTypeUnknownRequest:
		return fmt.Sprintf("unknown request ID: %s", e.Message)
	case RelaySwitchErrorTypeProtocol:
		return fmt.Sprintf("protocol violation: %s", e.Message)
	case RelaySwitchErrorTypeAllMastersUnhealthy:
		return "all masters are unhealthy"
	default:
		return fmt.Sprintf("relay switch error: %s", e.Message)
	}
}

// CartridgeAttachmentErrorKind describes why a cartridge failed to attach.
type CartridgeAttachmentErrorKind string

const (
	CartridgeAttachmentErrorKindIncompatible      CartridgeAttachmentErrorKind = "incompatible"
	CartridgeAttachmentErrorKindManifestInvalid   CartridgeAttachmentErrorKind = "manifest_invalid"
	CartridgeAttachmentErrorKindHandshakeFailed   CartridgeAttachmentErrorKind = "handshake_failed"
	CartridgeAttachmentErrorKindIdentityRejected  CartridgeAttachmentErrorKind = "identity_rejected"
	CartridgeAttachmentErrorKindEntryPointMissing CartridgeAttachmentErrorKind = "entry_point_missing"
	CartridgeAttachmentErrorKindQuarantined       CartridgeAttachmentErrorKind = "quarantined"
	// CartridgeAttachmentErrorKindBadInstallation: the on-disk install
	// context (slug folder, channel folder, name/version directory
	// components) disagrees with what cartridge.json declares. The
	// cartridge is structurally well-formed but cannot be trusted
	// because its placement on disk does not match what it claims to
	// be. Hosts grace-period the offending directory and then delete
	// it; the record is surfaced so the operator sees what landed
	// where before it disappears.
	CartridgeAttachmentErrorKindBadInstallation CartridgeAttachmentErrorKind = "bad_installation"
	// CartridgeAttachmentErrorKindDisabled: the operator explicitly
	// disabled this cartridge through the host UI. The cartridge is
	// on disk and would otherwise have attached cleanly; the host
	// treats it as if the binary were yanked out of the system.
	// Re-enabling is a UI-driven operator action. Enforced at the
	// host level (machfab-mac's XPC service); the engine doesn't act
	// on it differently from any other failed attachment, but
	// preserves the kind so consumers can render the right reason
	// and offer the right recovery action.
	CartridgeAttachmentErrorKindDisabled CartridgeAttachmentErrorKind = "disabled"
	// CartridgeAttachmentErrorKindRegistryUnreachable: the cartridge
	// declares a non-null registry_url, but the host could not reach
	// that registry to verify the cartridge is listed. Distinct from
	// BadInstallation (= registry confirmed the version is missing) —
	// Unreachable means we don't know. Recovery action is "check
	// network + retry" rather than "rebuild as dev". The cartridge
	// is held back from attaching until verification succeeds; the
	// UI shows the actionable reason.
	CartridgeAttachmentErrorKindRegistryUnreachable CartridgeAttachmentErrorKind = "registry_unreachable"
	// CartridgeAttachmentErrorKindFabricManifestVersionMismatch: the
	// cartridge was built against a different fabric registry manifest
	// version than this engine is pinned to. Both engine and cartridge
	// bake their fabric manifest version at build time from
	// MFR_FABRIC_MANIFEST_VERSION (sourced from
	// fabric/manifest-version.txt); the engine refuses to load any
	// cartridge whose baked version does not match its own. Recovery is
	// "rebuild the cartridge against the engine's fabric manifest
	// version" — there is no in-engine fallback because URN resolution
	// between mismatched versions is fundamentally unsafe (cap and media
	// definitions may have changed shape across manifest versions).
	CartridgeAttachmentErrorKindFabricManifestVersionMismatch CartridgeAttachmentErrorKind = "fabric_manifest_version_mismatch"
)

// CartridgeAttachmentError carries the details of a failed cartridge attachment.
type CartridgeAttachmentError struct {
	Kind                  CartridgeAttachmentErrorKind `json:"kind"`
	Message               string                       `json:"message"`
	DetectedAtUnixSeconds int64                        `json:"detected_at_unix_seconds"`
}

// CartridgeLifecycle is the positive lifecycle phase that runs
// BEFORE a cartridge becomes dispatchable. See
// `machfab-mac/docs/cartridge state machine.md` for the canonical
// state diagram. Mutually exclusive with the AttachmentError on
// InstalledCartridgeRecord: when the cartridge has a failed
// terminal classification, AttachmentError is set and Lifecycle is
// irrelevant. When AttachmentError is nil, Lifecycle carries the
// in-progress phase and the cartridge is dispatchable iff
// Lifecycle == CartridgeLifecycleOperational.
type CartridgeLifecycle string

const (
	// CartridgeLifecycleDiscovered: discovery scan has found the
	// version directory and is about to inspect it. Transient.
	CartridgeLifecycleDiscovered CartridgeLifecycle = "discovered"
	// CartridgeLifecycleInspecting: reading cartridge.json,
	// computing directory hash, validating on-disk install
	// context. Hashing can take seconds for large model
	// cartridges; runs on a background queue so other
	// cartridges' inspections proceed in parallel.
	CartridgeLifecycleInspecting CartridgeLifecycle = "inspecting"
	// CartridgeLifecycleVerifying: inspection succeeded; awaiting
	// a verdict from the registry verifier service. Skipped for
	// dev cartridges (registry_url == nil) and bundle cartridges.
	CartridgeLifecycleVerifying CartridgeLifecycle = "verifying"
	// CartridgeLifecycleOperational: cleared every gate. Caps are
	// registered with the engine and dispatch can route requests
	// to this cartridge.
	CartridgeLifecycleOperational CartridgeLifecycle = "operational"
)

// CartridgeRuntimeStats holds live statistics for a managed cartridge.
type CartridgeRuntimeStats struct {
	Running                  bool    `json:"running"`
	PID                      *uint32 `json:"pid,omitempty"`
	ActiveRequestCount       uint64  `json:"active_request_count"`
	PeerRequestCount         uint64  `json:"peer_request_count"`
	MemoryFootprintMB        uint64  `json:"memory_footprint_mb"`
	MemoryRSSMB              uint64  `json:"memory_rss_mb"`
	LastHeartbeatUnixSeconds *int64  `json:"last_heartbeat_unix_seconds,omitempty"`
	RestartCount             uint64  `json:"restart_count"`
	// ProtocolDropsTotal is the cumulative protocol-level frame drop count
	// self-reported by the cartridge as `drops_total` on every heartbeat
	// response meta (L8: every drop is countable end-to-end). nil until
	// the first heartbeat round-trip delivers a reading. Mirrors Rust
	// CartridgeRuntimeStats::protocol_drops_total.
	ProtocolDropsTotal *uint64 `json:"protocol_drops_total,omitempty"`
}

// NotRunning returns a CartridgeRuntimeStats representing a stopped cartridge.
func NotRunning() CartridgeRuntimeStats {
	return CartridgeRuntimeStats{Running: false}
}

// InstalledCartridgeRecord represents the identity of an installed
// cartridge. `(RegistryURL, Channel, Id, Version)` is the
// cartridge's full identity — installs of the same id from
// different registries × channels are distinct artifacts that
// coexist on disk under different top-level slug folders.
//
// RegistryURL is `*string` (Go's nullable form). nil ⇔ dev install
// (cartridge built locally without MFR_CARTRIDGE_REGISTRY_URL); non-nil ⇔
// the verbatim URL the cartridge was published from. Compared
// byte-wise; never normalized. The JSON field is required-but-
// nullable: missing key is a parse error so old-schema payloads
// surface immediately.
type InstalledCartridgeRecord struct {
	RegistryURL *string `json:"registry_url"`
	Id          string  `json:"id"`
	Channel     string  `json:"channel"`
	Version     string  `json:"version"`
	Sha256      string  `json:"sha256"`
	// CapGroups carries the cartridge's manifest cap_groups so the
	// engine can register content-inspection adapters per cartridge.
	// Empty when the cartridge failed attachment before its manifest
	// could be parsed; the flat cap-urn snapshot is computed from
	// these groups, not stored separately on the wire.
	CapGroups       []CapGroup                `json:"cap_groups,omitempty"`
	AttachmentError *CartridgeAttachmentError `json:"attachment_error,omitempty"`
	RuntimeStats    *CartridgeRuntimeStats    `json:"runtime_stats,omitempty"`
	// Lifecycle is the positive lifecycle phase. Mutually
	// exclusive with AttachmentError: when AttachmentError != nil
	// this field is irrelevant. When AttachmentError == nil, the
	// cartridge is dispatchable iff Lifecycle ==
	// CartridgeLifecycleOperational. Defaults (empty string) to
	// CartridgeLifecycleDiscovered on the wire so a producer that
	// forgets to set it never accidentally appears as Operational.
	Lifecycle CartridgeLifecycle `json:"lifecycle,omitempty"`
}

// EffectiveLifecycle returns the lifecycle phase, defaulting to
// CartridgeLifecycleDiscovered when the field is empty (producer
// forgot to set it). Callers SHOULD use this rather than reading
// Lifecycle directly so an unset field cannot be mistaken for
// CartridgeLifecycleOperational.
func (i *InstalledCartridgeRecord) EffectiveLifecycle() CartridgeLifecycle {
	if i.Lifecycle == "" {
		return CartridgeLifecycleDiscovered
	}
	return i.Lifecycle
}

// CapURNs returns the flat de-duplicated cap-URN list across this
// cartridge's groups, preserving first-seen order. Computed view.
func (i *InstalledCartridgeRecord) CapURNs() []string {
	seen := make(map[string]struct{}, 0)
	out := make([]string, 0)
	for _, group := range i.CapGroups {
		for _, c := range group.Caps {
			urn := c.Urn.String()
			if _, ok := seen[urn]; ok {
				continue
			}
			seen[urn] = struct{}{}
			out = append(out, urn)
		}
	}
	return out
}

// UnmarshalJSON enforces "required-but-nullable" for RegistryURL
// (see CartridgeJson.UnmarshalJSON for the same pattern). Missing
// key is rejected.
func (i *InstalledCartridgeRecord) UnmarshalJSON(data []byte) error {
	type rawIdentity InstalledCartridgeRecord
	var raw rawIdentity
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &asMap); err != nil {
		return err
	}
	if _, present := asMap["registry_url"]; !present {
		return errors.New(
			"InstalledCartridgeRecord is missing required `registry_url` field. " +
				"It must be present, with value null for dev installs or " +
				"a URL string for registry installs.")
	}
	*i = InstalledCartridgeRecord(raw)
	return nil
}

// RegistrySlug returns the on-disk slug derived from RegistryURL.
// nil → DevSlug; non-nil → SlugFor(*RegistryURL).
func (i *InstalledCartridgeRecord) RegistrySlug() string {
	return SlugFor(i.RegistryURL)
}

// RelayNotifyCapabilitiesPayload is the parsed payload from RelayNotify frames.
// The flat cap-urn list is no longer carried on the wire — consumers derive
// it from `InstalledCartridges[*].CapGroups` via `CapURNs()`.
type RelayNotifyCapabilitiesPayload struct {
	InstalledCartridges []InstalledCartridgeRecord `json:"installed_cartridges"`
	// HostProtocolStats carries the host's protocol-level observability
	// snapshot (L8): drop counters and routing-table sizes. Refreshed with
	// each stats republish so the engine can surface the state of
	// communications per host. Absent on some advertisements (the initial
	// capability advertisement typically omits it) — a per-republish
	// refresh, not a requirement. Mirrors Rust's
	// RelayNotifyCapabilitiesPayload::host_protocol_stats.
	HostProtocolStats *HostProtocolStats `json:"host_protocol_stats,omitempty"`
}

// CapURNs returns the flat de-duplicated cap-URN union across every
// cartridge in the payload, preserving first-seen order.
func (p *RelayNotifyCapabilitiesPayload) CapURNs() []string {
	seen := make(map[string]struct{}, 0)
	out := make([]string, 0)
	for idx := range p.InstalledCartridges {
		for _, urn := range p.InstalledCartridges[idx].CapURNs() {
			if _, ok := seen[urn]; ok {
				continue
			}
			seen[urn] = struct{}{}
			out = append(out, urn)
		}
	}
	return out
}

// WithHostProtocolStats attaches the host's protocol stats snapshot,
// returning the updated payload. Mirrors Rust's
// RelayNotifyCapabilitiesPayload::with_host_protocol_stats.
func (p RelayNotifyCapabilitiesPayload) WithHostProtocolStats(stats HostProtocolStats) RelayNotifyCapabilitiesPayload {
	p.HostProtocolStats = &stats
	return p
}

// MasterConnection represents a connection to a single RelayMaster.
//
// `id` is the stable identity of this slot. Reattach-by-id matches
// against it on subsequent reconnects so the slot index stays
// constant across the death-and-reconnect cycle. Once set at slot
// creation it is never overwritten.
type MasterConnection struct {
	id                  string
	socketWriter        *FrameWriter
	manifest            []byte
	limits              Limits
	caps                []string
	installedCartridges []InstalledCartridgeRecord
	// hostProtocolStats is the latest per-host protocol stats (drops,
	// routing-table sizes) reported by this master's RelayNotify. nil until
	// the first advertisement that carries them. Previously this payload
	// field was parsed and silently discarded — retained here so
	// ProtocolStats can name the host behind a drop. Mirrors Rust
	// MasterConnection.host_protocol_stats.
	hostProtocolStats *HostProtocolStats
	healthy           bool
	// lastError carries the most recent attachment / identity-probe
	// failure reason for this slot (nil when none). Set when a
	// synchronous or deferred identity probe fails, cleared when a
	// deferred probe later passes. Surfaced on the inventory view so
	// the engine can report WHY a master with visible cartridges is
	// not routable. Mutated only under RelaySwitch.mu.
	lastError *string
}

// RelaySwitch is a cap-aware routing multiplexer for multiple RelayMasters
type RelaySwitch struct {
	masters  []*MasterConnection
	capTable []CapTableEntry
	// requests is the unified per-request state (L7): routing, origin, peer
	// markers, cancel-cascade children, external response channel (used by
	// the deferred runtime identity probe — this mirror has no execute_cap-
	// style external caller API), per-stream flow stats, and the rid→xid
	// index — one entry, one registration, one termination. Replaces the
	// pre-v3 requestRouting/peerRequests/peerCallParents/
	// externalResponseChannels maps. Guarded by mu (the table itself is
	// unsynchronized, mirroring the Rust reference's RwLock<RequestTable>
	// and the Swift mirror's lock-guarded RequestTable).
	requests *RequestTable
	// drops is dropped-frame accounting (L8): unroutable/post-terminal
	// frames are counted drops, never silent losses and never protocol
	// errors.
	drops                        *DropCounters
	capabilities                 []byte
	aggregateInstalledCartridges []InstalledCartridgeRecord
	negotiatedLimits             Limits
	frameRx                      chan MasterFrame
	// expectedMasterCount is the number of cardinality slots the
	// engine intends to wire up. Set once via SetExpectedMasterCount
	// shortly after construction (the engine knows the count only
	// after it decides how many providers to register). Defaults to
	// 0; AllMastersReady returns false until it is declared so an
	// engine that forgets to declare it hangs at "configuring"
	// rather than advancing to "ready" prematurely.
	expectedMasterCount int
	mu                  sync.Mutex
	// pendingProbes queues master indices whose advertised cap set
	// transitioned empty → non-empty in the last RelayNotify update and
	// therefore need an end-to-end identity probe before their caps may
	// become routable. handleMasterFrame enqueues; the probe-driver
	// goroutine (started by StartBackgroundPump) drains and runs the
	// probe. Buffered + non-blocking enqueue so the frame loop never
	// stalls while holding sw.mu.
	pendingProbes chan int
	// capWatch broadcasts the routable (health-filtered) capabilities
	// wire bytes. Subscribers receive the current snapshot on subscribe
	// and a fresh one whenever the routable set changes — the engine
	// readiness signal that flips when a deferred probe makes a master's
	// caps routable. send_replace semantics: the value persists even
	// with zero current receivers.
	capWatch *capWatch
	// xidCounter mints unique routing ids (XIDs) for relay-internal
	// requests such as the deferred identity probe.
	xidCounter uint64
	// bgPumpStarted guards StartBackgroundPump idempotency.
	bgPumpStarted bool
	// addMasterMu serialises AddMaster across the whole switch.
	// `masterIdx` is the routing key for capTable / requestRouting;
	// it must be decided once per slot and stay stable for the slot's
	// lifetime. Concurrent AddMaster calls would race on `len(masters)`
	// — two appenders could both decide they are slot N. The mutex
	// covers the I/O too (RelayNotify read + identity probe) so the
	// reattach branch sees a stable view of `masters` for the
	// duration; contention is bounded by the small slot count.
	addMasterMu sync.Mutex
}

type CapTableEntry struct {
	CapURN    string
	MasterIdx int
}

// capWatch is a single-value broadcast channel modelled on tokio's
// `watch`. It holds the latest routable-capabilities snapshot; any
// number of receivers observe the current value on subscribe and block
// for the next change. `store` mirrors `watch::send_replace`: it always
// records the new value (so it persists even when there are momentarily
// zero receivers) and wakes every waiter.
type capWatch struct {
	mu      sync.Mutex
	cond    *sync.Cond
	value   []byte
	version uint64
}

func newCapWatch() *capWatch {
	w := &capWatch{}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *capWatch) store(v []byte) {
	w.mu.Lock()
	w.value = append([]byte(nil), v...)
	w.version++
	w.cond.Broadcast()
	w.mu.Unlock()
}

// CapabilitiesReceiver observes routable-capability snapshots published
// by a RelaySwitch.
type CapabilitiesReceiver struct {
	w        *capWatch
	lastSeen uint64
}

func (w *capWatch) subscribe() *CapabilitiesReceiver {
	w.mu.Lock()
	defer w.mu.Unlock()
	return &CapabilitiesReceiver{w: w, lastSeen: w.version}
}

// Borrow returns a copy of the current routable-capabilities snapshot
// without consuming a change notification.
func (r *CapabilitiesReceiver) Borrow() []byte {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	return append([]byte(nil), r.w.value...)
}

// Changed blocks until the snapshot changes since this receiver last
// observed it, then returns the new value.
func (r *CapabilitiesReceiver) Changed() []byte {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	for r.w.version == r.lastSeen {
		r.w.cond.Wait()
	}
	r.lastSeen = r.w.version
	return append([]byte(nil), r.w.value...)
}

type MasterFrame struct {
	MasterIdx int
	Frame     *Frame
	Err       error
}

// NewRelaySwitch creates a new RelaySwitch with the given socket pairs.
//
// Each `SocketPair.ID` is the stable identity of the cardinality slot
// it fills. `AddMaster` uses the id to reattach a reconnecting host
// to the same slot index; duplicate ids in the constructor list are
// a wiring bug and surface as a hard `ProtocolError` (without this
// guard the first reconnect would reattach to whichever slot is
// found first by the linear scan, leaving the other stuck unhealthy
// forever — the exact bug class this contract closes).
func NewRelaySwitch(sockets []SocketPair) (*RelaySwitch, error) {
	if len(sockets) == 0 {
		return nil, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: "RelaySwitch requires at least one master",
		}
	}

	// Reject duplicate ids up front.
	seen := make(map[string]bool, len(sockets))
	for _, sp := range sockets {
		if seen[sp.ID] {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: fmt.Sprintf("NewRelaySwitch: duplicate master id %q in cardinality list — each slot must have a unique stable id", sp.ID),
			}
		}
		seen[sp.ID] = true
	}

	frameRx := make(chan MasterFrame, 100)
	var masters []*MasterConnection

	// pendingReader carries a verified master's reader to phase 2 so
	// reader goroutines are spawned only after every master has
	// cleared identity verification — mirroring the Rust two-phase
	// constructor.
	type pendingReader struct {
		masterIdx    int
		socketReader *FrameReader
	}
	var pendingReaders []pendingReader

	// Phase 1: For each master, read RelayNotify and verify identity.
	// Reader goroutines are spawned only after verification succeeds.
	for masterIdx, sockPair := range sockets {
		socketReader := NewFrameReader(sockPair.Read)
		socketWriter := NewFrameWriter(sockPair.Write)

		// Perform handshake (read initial RelayNotify)
		frame, err := socketReader.ReadFrame()
		if err != nil {
			return nil, err
		}
		if frame == nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: "relay connection closed before receiving RelayNotify",
			}
		}
		if frame.FrameType != FrameTypeRelayNotify {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: fmt.Sprintf("expected RelayNotify, got %d", frame.FrameType),
			}
		}

		manifest := frame.RelayNotifyManifest()
		if manifest == nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: "RelayNotify missing manifest",
			}
		}

		limits := frame.RelayNotifyLimits()
		if limits == nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: "RelayNotify missing limits",
			}
		}

		payload, err := parseRelayNotifyPayload(manifest)
		if err != nil {
			return nil, err
		}

		caps := payload.CapURNs()

		// End-to-end identity verification. The probe only makes sense
		// when the host has at least one advertised cap — an empty cap
		// list means "no cartridges attached successfully" and there is
		// no handler chain to test. The master still joins so its
		// installed_cartridges attachment errors reach the engine.
		if len(caps) > 0 {
			if err := VerifyIdentity(socketReader, socketWriter); err != nil {
				return nil, &RelaySwitchError{
					Type: RelaySwitchErrorTypeProtocol,
					Message: fmt.Sprintf(
						"master %d: identity verification failed: %v",
						masterIdx, err,
					),
				}
			}
		}

		pendingReaders = append(pendingReaders, pendingReader{
			masterIdx:    masterIdx,
			socketReader: socketReader,
		})

		masters = append(masters, &MasterConnection{
			id:                  sockPair.ID,
			socketWriter:        socketWriter,
			manifest:            manifest,
			limits:              *limits,
			caps:                caps,
			installedCartridges: payload.InstalledCartridges,
			hostProtocolStats:   payload.HostProtocolStats,
			healthy:             true,
		})
	}

	// Phase 2: All masters verified — spawn reader goroutines.
	for _, pr := range pendingReaders {
		idx := pr.masterIdx
		socketReader := pr.socketReader
		go func() {
			for {
				frame, err := socketReader.ReadFrame()
				if err != nil {
					frameRx <- MasterFrame{MasterIdx: idx, Frame: nil, Err: err}
					return
				}
				if frame == nil {
					frameRx <- MasterFrame{MasterIdx: idx, Frame: nil, Err: fmt.Errorf("EOF")}
					return
				}

				frameRx <- MasterFrame{MasterIdx: idx, Frame: frame, Err: nil}
			}
		}()
	}

	sw := &RelaySwitch{
		masters:                      masters,
		capTable:                     []CapTableEntry{},
		requests:                     NewRequestTable(),
		drops:                        NewDropCounters(),
		aggregateInstalledCartridges: []InstalledCartridgeRecord{},
		frameRx:                      frameRx,
		pendingProbes:                make(chan int, 256),
		capWatch:                     newCapWatch(),
	}

	sw.rebuildCapTable()
	// rebuildCapabilities (routable, health-filtered) and
	// rebuildInstalledCartridges (inventory, unfiltered) are independent:
	// each reads the per-master state directly. Order between them no
	// longer matters.
	sw.rebuildInstalledCartridges()
	sw.rebuildCapabilities()
	sw.rebuildLimits()

	return sw, nil
}

// AddMaster attaches a (re)connecting host to a slot.
//
// `sockPair.ID` is the stable identity of the cardinality slot:
//
//   - Existing slot, currently UNHEALTHY → reattach in place at the
//     existing slot index. The dead master's reader goroutine has
//     already exited on EOF; the new connection installs a fresh
//     writer + reader goroutine and clears the unhealthy flag.
//     `requestRouting` and `capTable` entries keyed by `masterIdx`
//     stay coherent because the index does not change.
//   - Existing slot, currently HEALTHY → caller bug
//     (the same master must not be added twice). Surface as a
//     `RelaySwitchError` so the wiring mistake is fixed instead of
//     silently growing zombie slots.
//   - No existing slot with that id → append a fresh slot at
//     `len(masters)`. The reader goroutine is spawned with that
//     index baked in.
//
// Returns the slot index (stable across reattach).
func (sw *RelaySwitch) AddMaster(sockPair SocketPair) (int, error) {
	sw.addMasterMu.Lock()
	defer sw.addMasterMu.Unlock()

	// Existing-slot lookup under the inner mutex so the linear scan
	// observes a stable `masters`.
	sw.mu.Lock()
	existingIdx := -1
	for i, m := range sw.masters {
		if m.id == sockPair.ID {
			if m.healthy {
				sw.mu.Unlock()
				return 0, &RelaySwitchError{
					Type: RelaySwitchErrorTypeProtocol,
					Message: fmt.Sprintf(
						"AddMaster: id %q is already attached to a healthy slot at index %d — "+
							"cardinality violation (each id may only be attached once at a time)",
						sockPair.ID, i,
					),
				}
			}
			existingIdx = i
			break
		}
	}

	// Reserve the slot index. For the append case this is the
	// current length under `addMasterMu`; for reattach it is the
	// existing slot index. The reader goroutine captures this
	// value so per-frame routing always carries the right index.
	var masterIdx int
	if existingIdx >= 0 {
		masterIdx = existingIdx
	} else {
		masterIdx = len(sw.masters)
	}
	sw.mu.Unlock()

	// Handshake: read RelayNotify.
	socketReader := NewFrameReader(sockPair.Read)
	socketWriter := NewFrameWriter(sockPair.Write)
	frame, err := socketReader.ReadFrame()
	if err != nil {
		return 0, err
	}
	if frame == nil {
		return 0, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: "AddMaster: relay connection closed before receiving RelayNotify",
		}
	}
	if frame.FrameType != FrameTypeRelayNotify {
		return 0, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: fmt.Sprintf("AddMaster: expected RelayNotify, got %d", frame.FrameType),
		}
	}
	manifest := frame.RelayNotifyManifest()
	if manifest == nil {
		return 0, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: "AddMaster: RelayNotify missing manifest",
		}
	}
	limits := frame.RelayNotifyLimits()
	if limits == nil {
		return 0, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: "AddMaster: RelayNotify missing limits",
		}
	}
	payload, err := parseRelayNotifyPayload(manifest)
	if err != nil {
		return 0, err
	}

	caps := payload.CapURNs()

	// End-to-end identity verification — the runtime counterpart of the
	// synchronous probe NewRelaySwitch runs. Mirrors Rust add_master:
	// whenever the (re)attaching host advertises at least one cap, prove
	// its handler chain answers identity end-to-end BEFORE its caps
	// become routable. The probe must run on the synchronous socket here
	// (before the reader goroutine is spawned) so its reply frames are
	// not stolen by the per-master reader.
	//
	// On failure the master still joins, but as UNHEALTHY: its
	// installed_cartridges stay visible to the inventory aggregate while
	// its caps are held out of the routing table. We do NOT error out —
	// that would make the whole reattach fail and hide the inventory.
	var identityFailure *string
	if len(caps) > 0 {
		if verr := VerifyIdentity(socketReader, socketWriter); verr != nil {
			msg := fmt.Sprintf("AddMaster master %d: identity verification failed: %v", masterIdx, verr)
			identityFailure = &msg
		}
	}
	healthyAtRegister := identityFailure == nil

	// Spawn reader goroutine bound to masterIdx.
	idx := masterIdx
	frameRx := sw.frameRx
	go func() {
		for {
			f, err := socketReader.ReadFrame()
			if err != nil {
				frameRx <- MasterFrame{MasterIdx: idx, Frame: nil, Err: err}
				return
			}
			if f == nil {
				frameRx <- MasterFrame{MasterIdx: idx, Frame: nil, Err: fmt.Errorf("EOF")}
				return
			}
			frameRx <- MasterFrame{MasterIdx: idx, Frame: f, Err: nil}
		}
	}()

	// Commit the connection state into the slot.
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if existingIdx < 0 {
		// Append. The captured `masterIdx` MUST equal the new
		// length; if not, a concurrent appender bypassed
		// `addMasterMu`, which is a protocol violation.
		if len(sw.masters) != masterIdx {
			return 0, &RelaySwitchError{
				Type: RelaySwitchErrorTypeProtocol,
				Message: fmt.Sprintf(
					"AddMaster: append-index race for id %q: reserved %d but len(masters) is now %d "+
						"(a concurrent caller bypassed addMasterMu)",
					sockPair.ID, masterIdx, len(sw.masters),
				),
			}
		}
		sw.masters = append(sw.masters, &MasterConnection{
			id:                  sockPair.ID,
			socketWriter:        socketWriter,
			manifest:            manifest,
			limits:              *limits,
			caps:                caps,
			installedCartridges: payload.InstalledCartridges,
			hostProtocolStats:   payload.HostProtocolStats,
			healthy:             healthyAtRegister,
			lastError:           identityFailure,
		})
	} else {
		slot := sw.masters[masterIdx]
		if slot.id != sockPair.ID {
			return 0, &RelaySwitchError{
				Type: RelaySwitchErrorTypeProtocol,
				Message: fmt.Sprintf(
					"AddMaster: reattach-id mismatch at index %d: expected %q but found %q",
					masterIdx, sockPair.ID, slot.id,
				),
			}
		}
		// In-place mutation. The dead master's reader goroutine
		// has already exited on EOF (Go goroutines aren't
		// cancellable; we rely on the natural EOF exit).
		slot.socketWriter = socketWriter
		slot.manifest = manifest
		slot.limits = *limits
		slot.caps = caps
		slot.installedCartridges = payload.InstalledCartridges
		slot.hostProtocolStats = payload.HostProtocolStats
		slot.healthy = healthyAtRegister
		slot.lastError = identityFailure
	}

	sw.rebuildCapTable()
	sw.rebuildInstalledCartridges()
	sw.rebuildCapabilities()
	sw.rebuildLimits()

	return masterIdx, nil
}

// SocketPair carries a relay master connection plus the stable
// identity (`ID`) of the cardinality slot it fills. Reattach-by-id
// in `AddMaster` matches against this id so a reconnecting host
// lands back in the same slot index — preserving routing entries
// keyed by index. Re-adding the same id while the slot is still
// healthy is a wiring bug and is rejected.
type SocketPair struct {
	ID    string
	Read  net.Conn
	Write net.Conn
}

// Capabilities returns the aggregate capabilities of all healthy masters
func (sw *RelaySwitch) Capabilities() []byte {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	result := make([]byte, len(sw.capabilities))
	copy(result, sw.capabilities)
	return result
}

// InstalledCartridges returns the aggregate installed cartridge identities of all healthy masters
func (sw *RelaySwitch) InstalledCartridges() []InstalledCartridgeRecord {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	result := make([]InstalledCartridgeRecord, len(sw.aggregateInstalledCartridges))
	copy(result, sw.aggregateInstalledCartridges)
	return result
}

// Limits returns the negotiated limits (minimum across all masters)
func (sw *RelaySwitch) Limits() Limits {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.negotiatedLimits
}

// SubscribeCapabilities subscribes to changes in the *routable* capability
// set. The returned receiver yields the current capabilities snapshot
// immediately (Borrow) and a fresh snapshot every time the routable set
// changes — including when a deferred identity probe completes and a
// previously-unhealthy master's caps become routable. An engine-facing
// relay uses this to advertise readiness tied to master health, not to
// mere inventory presence. Mirrors Rust's `subscribe_capabilities`.
func (sw *RelaySwitch) SubscribeCapabilities() *CapabilitiesReceiver {
	return sw.capWatch.subscribe()
}

// RelaySwitchProtocolStats is the switch's protocol observability snapshot
// (L8): live request state, recent terminations, and per-reason drop
// counters. Field names are the mirror contract (TEST7087).
//
// Hosts carries the per-master HostProtocolStats drawn from each host's
// latest RelayNotify (RelayNotifyCapabilitiesPayload.HostProtocolStats /
// MasterConnection.hostProtocolStats), keyed by master id. A master that has
// not yet advertised host stats is absent — never a zeroed placeholder. Note
// this mirror's HostProtocolStats (bifaci/host.go) omits the Rust/Swift
// incoming_to_peer_rids and outgoing_max_seq fields — CartridgeHost's
// simpler channel-based routing has no honest value for them (see the
// doc-comment on HostProtocolStats). (matches Rust RelaySwitchProtocolStats)
type RelaySwitchProtocolStats struct {
	Requests RequestTableSnapshot         `json:"requests"`
	Drops    DropSnapshot                 `json:"drops"`
	Hosts    map[string]HostProtocolStats `json:"hosts"`
}

// ProtocolStats returns the switch's protocol observability snapshot (L8):
// every live request's phase, age, per-stream flow counters, and children;
// the recently-terminated ring; and the per-reason drop totals.
func (sw *RelaySwitch) ProtocolStats() RelaySwitchProtocolStats {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	// Per-master host protocol stats, keyed by master id, as reported in
	// each host's latest RelayNotify. A master that has not yet advertised
	// host stats is absent — never a zeroed placeholder.
	hosts := make(map[string]HostProtocolStats)
	for _, m := range sw.masters {
		if m.hostProtocolStats != nil {
			hosts[m.id] = *m.hostProtocolStats
		}
	}
	return RelaySwitchProtocolStats{
		Requests: sw.requests.Snapshot(),
		Drops:    sw.drops.Snapshot(),
		Hosts:    hosts,
	}
}

// SetTerminateObserver installs a termination observer on the request table
// (L8): called with every termination's summary, under sw.mu — must be
// cheap. Lets a caller accumulate complete per-run history without missing
// terminations between ProtocolStats polls (the ring evicts at
// RecentTerminatedCap).
func (sw *RelaySwitch) SetTerminateObserver(observer TerminateObserver) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.requests.SetTerminateObserver(observer)
}

// StartBackgroundPump spawns the persistent frame-drain pump and the
// runtime identity-probe driver. The pump consumes frames from every
// master through handleMasterFrame (the same dispatch path the
// per-execution ReadFromMasters callers use); pass-through frames it
// returns have no owner in the background path and are discarded. The
// probe driver drains pendingProbes and runs an end-to-end identity probe
// against each master that transitioned empty → non-empty caps, gating
// cap-table publication on probe success. Idempotent — a second call is a
// no-op. Mirrors Rust's start_background_pump + spawn_identity_probe_driver.
func (sw *RelaySwitch) StartBackgroundPump() {
	sw.mu.Lock()
	if sw.bgPumpStarted {
		sw.mu.Unlock()
		return
	}
	sw.bgPumpStarted = true
	sw.mu.Unlock()

	go func() {
		for {
			// Pass-through frames and pass-through errors have no owner
			// in the background path; keep draining so RelayNotify
			// capability updates and probe responses are serviced.
			if _, err := sw.ReadFromMasters(); err != nil {
				continue
			}
		}
	}()

	go sw.runProbeDriver()
}

// runProbeDriver serially drains pendingProbes and probes each named
// master. On success it flips the master healthy and rebuilds the cap
// table so its caps become routable; on failure it keeps the master
// unhealthy and stamps lastError. Mirrors Rust's spawn_identity_probe_driver.
func (sw *RelaySwitch) runProbeDriver() {
	for masterIdx := range sw.pendingProbes {
		err := sw.runIdentityProbeViaRelay(masterIdx)

		sw.mu.Lock()
		if masterIdx >= 0 && masterIdx < len(sw.masters) {
			if err == nil {
				sw.masters[masterIdx].healthy = true
				sw.masters[masterIdx].lastError = nil
			} else {
				sw.masters[masterIdx].healthy = false
				msg := err.Error()
				sw.masters[masterIdx].lastError = &msg
			}
			// Only the health-filtered surfaces depend on this flip;
			// inventory (unfiltered) is unchanged. Matches the Rust
			// driver, which rebuilds cap_table + capabilities only.
			sw.rebuildCapTable()
			sw.rebuildCapabilities()
		}
		sw.mu.Unlock()
	}
}

// runIdentityProbeViaRelay runs an end-to-end CAP_IDENTITY probe against a
// single master through the relay's normal master writer + reader path. The
// probe is registered on the unified request table (L7) exactly like any
// other request — origin nil (external), destination masterIdx, external
// channel `ch` — so its reply frames route back through handleMasterFrame's
// normal response-continuation path (RouteBack::External) instead of a
// bespoke interception map. Whatever the outcome (success, failure, or
// timeout) the entry is unconditionally terminated on exit — the success and
// failure paths already self-terminate via handleMasterFrame's End/Err
// handling; the final Terminate call here only matters for the timeout case,
// where it is the sole cleanup path. On success returns nil; on failure
// returns a typed error suitable for MasterConnection.lastError. Mirrors
// Rust's run_identity_probe_via_relay + register_external.
func (sw *RelaySwitch) runIdentityProbeViaRelay(masterIdx int) error {
	const probeTimeout = 10 * time.Second

	rid := NewMessageIdRandom()
	xid := NewMessageIdFromUint(atomic.AddUint64(&sw.xidCounter, 1))
	key := RequestKey{Xid: xid, Rid: rid}

	nonce := identityNonce()
	cborNonce, err := cbor2.Marshal(nonce)
	if err != nil {
		return fmt.Errorf("BUG: failed to CBOR-encode identity nonce: %w", err)
	}
	streamID := "identity-verify-runtime"

	// Buffered so handleMasterFrame's delivery (under sw.mu) never blocks
	// for the bounded probe response (REQ echo is STREAM_START + CHUNK +
	// STREAM_END + END, or a single ERR).
	ch := make(chan Frame, 64)

	// Register the request and send all five probe frames under sw.mu so
	// the writes don't interleave with other master writers (the relay
	// serialises every switch→master write through sw.mu).
	sw.mu.Lock()
	if masterIdx < 0 || masterIdx >= len(sw.masters) {
		sw.mu.Unlock()
		return fmt.Errorf("runtime identity probe: master index %d out of range", masterIdx)
	}
	probeCap := standard.CapIdentity
	state := NewRequestState(
		RequestRoutingEntry{SourceMasterIdx: nil, DestinationMasterIdx: masterIdx},
		nil,
		ch,
		false,
	).WithCapUrn(&probeCap)
	if regErr := sw.requests.Register(key, state); regErr != nil {
		sw.mu.Unlock()
		return fmt.Errorf("runtime identity probe registration failed: %w", regErr)
	}
	writer := sw.masters[masterIdx].socketWriter

	seq := NewSeqAssigner()
	req := NewReq(rid, standard.CapIdentity, []byte{}, "application/cbor")
	req.RoutingId = &xid
	seq.Assign(req)
	ss := NewStreamStart(rid, streamID, "media:", nil)
	ss.RoutingId = &xid
	seq.Assign(ss)
	checksum := ComputeChecksum(cborNonce)
	chunk := NewChunk(rid, streamID, 0, cborNonce, 0, checksum)
	chunk.RoutingId = &xid
	seq.Assign(chunk)
	se := NewStreamEnd(rid, streamID, 1)
	se.RoutingId = &xid
	seq.Assign(se)
	end := NewEnd(rid, nil)
	end.RoutingId = &xid
	seq.Assign(end)

	var sendErr error
	for _, fr := range []*Frame{req, ss, chunk, se, end} {
		if werr := writer.WriteFrame(fr); werr != nil {
			sendErr = werr
			break
		}
	}
	sw.mu.Unlock()

	probeErr := func() error {
		if sendErr != nil {
			return fmt.Errorf("identity probe send failed: %w", sendErr)
		}

		timeout := time.After(probeTimeout)
		var accumulated []byte
		for {
			select {
			case <-timeout:
				return fmt.Errorf("runtime identity probe timed out after %v", probeTimeout)
			case fr, ok := <-ch:
				if !ok {
					return fmt.Errorf("runtime identity probe channel closed before END")
				}
				switch fr.FrameType {
				case FrameTypeStreamStart, FrameTypeStreamEnd:
					// no-op
				case FrameTypeChunk:
					if fr.Payload != nil {
						var decoded []byte
						if derr := cbor2.Unmarshal(fr.Payload, &decoded); derr != nil {
							return fmt.Errorf("identity probe: failed to decode CBOR chunk: %w", derr)
						}
						accumulated = append(accumulated, decoded...)
					}
				case FrameTypeEnd:
					if !bytesEqual(accumulated, nonce) {
						return fmt.Errorf(
							"identity probe payload mismatch (expected %d bytes, got %d)",
							len(nonce), len(accumulated),
						)
					}
					return nil
				case FrameTypeErr:
					code := fr.ErrorCode()
					if code == "" {
						code = "UNKNOWN"
					}
					msg := fr.ErrorMessage()
					if msg == "" {
						msg = "no message"
					}
					return fmt.Errorf("identity probe failed: [%s] %s", code, msg)
				case FrameTypeLog, FrameTypeCredit, FrameTypeHeartbeat:
					// Control/side-channel frames are legal ANYWHERE during
					// the probe (spec 12.4: LOG interleaves without affecting
					// data flow; CREDIT/HEARTBEAT are the control plane the
					// writer gate itself exempts, L4). A v3 cartridge
					// crediting its probe input as it consumes (L10) must
					// not fail identity verification.
				default:
					return fmt.Errorf("identity probe: unexpected frame type %v", fr.FrameType)
				}
			}
		}
	}()

	// Always terminate the request on exit — whether the probe succeeded,
	// failed, or timed out. Leaking the entry would waste memory and
	// confuse introspection over time (L7).
	kind := TerminalKindEnd
	if probeErr != nil {
		kind = TerminalKindErr
	}
	sw.mu.Lock()
	sw.requests.Terminate(key, kind)
	sw.mu.Unlock()

	return probeErr
}

// SetExpectedMasterCount declares how many cardinality slots the
// engine intends to wire up. The host's ".configuring → .ready"
// advance is gated on AllMastersReady, which returns false until this
// count is both declared (non-zero) and met. Both editions expect 2
// masters (internal + external/XPC); set once at engine boot from the
// same call site that registers the providers.
func (sw *RelaySwitch) SetExpectedMasterCount(expected int) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.expectedMasterCount = expected
}

// AllMastersReady reports true when:
//
//  1. The number of connected masters is at least
//     expectedMasterCount (declared via SetExpectedMasterCount), AND
//  2. Every connected master is healthy.
//
// Cap-set non-emptiness is intentionally NOT required. A master can be
// healthy and connected with zero caps while its cartridges are still
// inspecting / verifying — see `machfab-mac/docs/cartridge state
// machine.md`. Tying readiness to caps would mean the splash screen
// waits for every cartridge to clear inspection + verification, which
// can take many seconds for large model cartridges + slow registry
// fetches. Caps register incrementally as cartridges progress to
// Operational; the dispatch table grows under the engine over time.
//
// When expectedMasterCount is 0 the engine never declared a count, so
// this returns false — treat as not-yet-configured rather than guess.
func (sw *RelaySwitch) AllMastersReady() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	expected := sw.expectedMasterCount
	if expected == 0 {
		// Engine never declared an expected count — treat as
		// not-yet-configured rather than guess. Caller bug.
		return false
	}
	if len(sw.masters) < expected {
		return false
	}
	for _, master := range sw.masters {
		// An unhealthy master is by definition not ready.
		if !master.healthy {
			return false
		}
	}
	return true
}

// deliverExternal attempts a non-blocking send onto an external response
// channel (the deferred runtime identity probe is this mirror's only
// consumer — there is no execute_cap-style external-caller API). Returns
// false if the buffer is full, which the caller counts as a channel_closed
// drop for observability rather than blocking indefinitely while holding
// sw.mu. In this port's usage (bounded identity-probe response streams)
// the buffer should never actually fill.
func deliverExternal(ch chan<- Frame, frame Frame) bool {
	select {
	case ch <- frame:
		return true
	default:
		return false
	}
}

// CancelRequest cancels a specific in-flight request by request ID.
//
//  1. Looks up RID → XID → routing destination.
//  2. Terminates the request (Cancelled) FIRST — one atomic removal yields
//     the destination, the children for the cascade, and the external
//     channel for the final ERR (L7). A concurrent terminal for the same
//     key loses the race and is simply a no-op here (Terminate returns nil).
//  3. Sends a Cancel frame to the destination master.
//  4. Recursively cancels the child peer calls recorded on the entry.
//  5. Sends ERR "CANCELLED" to the external response channel if present.
func (sw *RelaySwitch) CancelRequest(rid MessageId, forceKill bool) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.cancelRequestLocked(rid, forceKill)
}

// cancelRequestLocked must be called with sw.mu held. Recurses directly
// (without re-locking) to cancel child peer calls.
func (sw *RelaySwitch) cancelRequestLocked(rid MessageId, forceKill bool) {
	xid, ok := sw.requests.XidForRid(rid)
	if !ok {
		return
	}
	key := RequestKey{Xid: xid, Rid: rid}

	state := sw.requests.Terminate(key, TerminalKindCancelled)
	if state == nil {
		return
	}

	// Send Cancel frame to destination.
	cancelFrame := NewCancelFrame(rid, forceKill)
	cancelFrame.RoutingId = &xid
	_ = sw.masters[state.Routing.DestinationMasterIdx].socketWriter.WriteFrame(cancelFrame)

	// Recursively cancel children.
	for _, child := range state.Children {
		sw.cancelRequestLocked(child.Rid, forceKill)
	}

	// Send ERR "CANCELLED" to the external response channel if present.
	// Best-effort: the request is already gone, so a failed final delivery
	// here is not itself counted as a drop — mirrors Rust's `let _ =
	// tx.send(err_frame)` and Swift's `_ = channel(errFrame)`.
	if state.ExternalChannel != nil {
		errFrame := NewErr(rid, "CANCELLED", "Request cancelled")
		errFrame.RoutingId = &xid
		deliverExternal(state.ExternalChannel, *errFrame)
	}
}

// CancelAllRequests cancels all external-origin (engine-initiated) in-flight requests.
// Returns the list of cancelled request IDs.
func (sw *RelaySwitch) CancelAllRequests(forceKill bool) []MessageId {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Snapshot all external-origin (origin == nil) request RIDs before
	// mutating the table.
	keys := sw.requests.KeysWhere(func(s *RequestState) bool { return s.Origin == nil })
	rids := make([]MessageId, len(keys))
	for i, k := range keys {
		rids[i] = k.Rid
	}

	for _, rid := range rids {
		sw.cancelRequestLocked(rid, forceKill)
	}

	return rids
}

// SendToMaster sends a frame to the appropriate master
//
// preferredCap: when non-nil, uses comparable routing and prefers
// the master whose registered cap is equivalent to this URN.
// When nil, uses standard accepts + closest-specificity routing.
func (sw *RelaySwitch) SendToMaster(frame *Frame, preferredCap *string) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	switch frame.FrameType {
	case FrameTypeReq:
		if frame.Cap == nil {
			return &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: "REQ frame missing cap URN",
			}
		}

		destIdx, err := sw.findMasterForCap(*frame.Cap, preferredCap)
		if err != nil {
			return err
		}

		// No XID: first arrival at the RelaySwitch — assign and register
		// (origin nil = external caller via SendToMaster, no response
		// channel; responses return via ReadFromMasters), then forward
		// (L7). Every frame the switch emits toward a master must carry an
		// XID — the host runtime's path-C invariant.
		xid := NewMessageIdFromUint(atomic.AddUint64(&sw.xidCounter, 1))
		frame.RoutingId = &xid
		key := RequestKey{Xid: xid, Rid: frame.Id}

		state := NewRequestState(
			RequestRoutingEntry{SourceMasterIdx: nil, DestinationMasterIdx: destIdx},
			nil,
			nil,
			false,
		).WithCapUrn(frame.Cap)
		if regErr := sw.requests.Register(key, state); regErr != nil {
			return &RelaySwitchError{Type: RelaySwitchErrorTypeProtocol, Message: regErr.Error()}
		}

		return sw.masters[destIdx].socketWriter.WriteFrame(frame)

	case FrameTypeStreamStart, FrameTypeChunk, FrameTypeStreamEnd,
		FrameTypeEnd, FrameTypeErr, FrameTypeCancel, FrameTypeCredit:
		// Continuation/control frames from the engine: look up XID from RID
		// if missing, then the destination. Unknown RID is a hard error
		// back to the caller: the engine is a direct API client and must
		// observe that the request no longer exists (already terminated)
		// so it stops sending.
		var xid MessageId
		if frame.RoutingId != nil {
			xid = *frame.RoutingId
		} else {
			foundXid, ok := sw.requests.XidForRid(frame.Id)
			if !ok {
				return &RelaySwitchError{
					Type:    RelaySwitchErrorTypeUnknownRequest,
					Message: frame.Id.ToString(),
				}
			}
			xid = foundXid
		}
		key := RequestKey{Xid: xid, Rid: frame.Id}
		state := sw.requests.Get(key)
		if state == nil {
			return &RelaySwitchError{
				Type:    RelaySwitchErrorTypeUnknownRequest,
				Message: frame.Id.ToString(),
			}
		}
		frame.RoutingId = &xid

		return sw.masters[state.Routing.DestinationMasterIdx].socketWriter.WriteFrame(frame)

	default:
		return &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: fmt.Sprintf("unexpected frame type from engine: %d", frame.FrameType),
		}
	}
}

// ReadFromMasters blocks until a frame is available from any master
func (sw *RelaySwitch) ReadFromMasters() (*Frame, error) {
	for {
		masterFrame := <-sw.frameRx

		if masterFrame.Err != nil {
			sw.mu.Lock()
			sw.handleMasterDeath(masterFrame.MasterIdx)
			sw.mu.Unlock()
			continue
		}

		if masterFrame.Frame == nil {
			// EOF
			sw.mu.Lock()
			sw.handleMasterDeath(masterFrame.MasterIdx)
			sw.mu.Unlock()
			continue
		}

		sw.mu.Lock()
		resultFrame, err := sw.handleMasterFrame(masterFrame.MasterIdx, masterFrame.Frame)
		sw.mu.Unlock()

		if err != nil {
			return nil, err
		}

		if resultFrame != nil {
			return resultFrame, nil
		}
		// Peer request handled internally, continue reading
	}
}

// findMasterForCap finds which master handles a given cap URN
//
// preferredCap: when non-nil, uses comparable matching (broader) and prefers
// masters whose registered cap is equivalent to this URN.
// When nil, uses standard accepts + closest-specificity routing.
func (sw *RelaySwitch) findMasterForCap(capURN string, preferredCap *string) (int, error) {
	requestURN, err := urn.NewCapUrnFromString(capURN)
	if err != nil {
		return 0, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeNoHandler,
			Message: capURN,
		}
	}

	requestSpecificity := requestURN.Specificity()

	// Parse preferred cap URN if provided
	var preferredURN *urn.CapUrn
	if preferredCap != nil {
		pURN, err := urn.NewCapUrnFromString(*preferredCap)
		if err == nil {
			preferredURN = pURN
		}
	}

	// Collect ALL dispatchable masters with their signed distance scores
	type match struct {
		masterIdx      int
		signedDistance int
		isPreferred    bool
	}
	var matches []match

	for _, entry := range sw.capTable {
		registeredURN, err := urn.NewCapUrnFromString(entry.CapURN)
		if err != nil {
			continue
		}

		// Use is_dispatchable: can this provider handle this request?
		if registeredURN.IsDispatchable(requestURN) {
			specificity := registeredURN.Specificity()
			signedDistance := specificity - requestSpecificity
			// Check if this registered cap is equivalent to the preferred cap
			isPreferred := false
			if preferredURN != nil {
				isPreferred = preferredURN.IsEquivalent(registeredURN)
			}
			matches = append(matches, match{
				masterIdx:      entry.MasterIdx,
				signedDistance: signedDistance,
				isPreferred:    isPreferred,
			})
		}
	}

	if len(matches) == 0 {
		return 0, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeNoHandler,
			Message: capURN,
		}
	}

	// If any match is preferred, pick the first preferred match
	for _, m := range matches {
		if m.isPreferred {
			return m.masterIdx, nil
		}
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

	return matches[0].masterIdx, nil
}

// handleMasterFrame handles a frame from a master
// parentRidFromMeta extracts the "parent_rid" cancel-cascade link from a
// peer REQ frame's meta, if present. Mirrors Rust's parent_rid parsing in
// handle_master_frame: a 16-byte UUID or an unsigned integer RID.
func parentRidFromMeta(frame *Frame) (MessageId, bool) {
	if frame.Meta == nil {
		return MessageId{}, false
	}
	v, ok := frame.Meta["parent_rid"]
	if !ok {
		return MessageId{}, false
	}
	switch pv := v.(type) {
	case []byte:
		if len(pv) == 16 {
			return MessageId{uuidBytes: pv}, true
		}
	case uint64:
		return NewMessageIdFromUint(pv), true
	}
	return MessageId{}, false
}

func (sw *RelaySwitch) handleMasterFrame(sourceIdx int, frame *Frame) (*Frame, error) {
	switch frame.FrameType {
	case FrameTypeReq:
		// Peer request (cartridge → cartridge via switch).
		if frame.Cap == nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: "REQ frame missing cap URN",
			}
		}
		if frame.RoutingId != nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: "REQ from cartridge should not have XID",
			}
		}

		// Validate XID-absence and assign the XID FIRST, before any
		// dispatch-failure path: every frame the switch emits toward a
		// master must carry an XID (the host runtime's path-C invariant),
		// including the synthetic ERR produced below for an unhandled cap.
		xid := NewMessageIdFromUint(atomic.AddUint64(&sw.xidCounter, 1))
		frame.RoutingId = &xid

		// Find destination master (no preference for peer requests).
		destIdx, err := sw.findMasterForCap(*frame.Cap, nil)
		if err != nil {
			// No handler registered for this cap. Rather than erroring —
			// which would abort the pump and leave the caller hanging
			// until its own activity timeout — send an ERR frame straight
			// back to the source master so the peer call fails fast with a
			// clear error. Stamp the synthetic XID assigned above so the
			// receiving cartridge host runtime accepts it (path-C
			// invariant). Mirrors Rust's handle_master_frame NO_HANDLER
			// branch (Ok(None) + ERR to caller).
			errFrame := NewErr(frame.Id, "NO_HANDLER", fmt.Sprintf("No handler found for cap: %s", *frame.Cap))
			errFrame.RoutingId = &xid
			_ = sw.masters[sourceIdx].socketWriter.WriteFrame(errFrame)
			return nil, nil
		}

		rid := frame.Id
		key := RequestKey{Xid: xid, Rid: rid}
		srcIdx := sourceIdx
		state := NewRequestState(
			RequestRoutingEntry{SourceMasterIdx: &srcIdx, DestinationMasterIdx: destIdx},
			&srcIdx,
			nil,
			true,
		).WithCapUrn(frame.Cap)
		if regErr := sw.requests.Register(key, state); regErr != nil {
			return nil, &RelaySwitchError{Type: RelaySwitchErrorTypeProtocol, Message: regErr.Error()}
		}

		// Track parent→child for the cancel cascade.
		if parentRid, ok := parentRidFromMeta(frame); ok {
			if parentXid, ok := sw.requests.XidForRid(parentRid); ok {
				sw.requests.LinkChild(RequestKey{Xid: parentXid, Rid: parentRid}, key)
			}
		}

		// Forward to destination with XID.
		if err := sw.masters[destIdx].socketWriter.WriteFrame(frame); err != nil {
			return nil, err
		}

		return nil, nil // Internal routing — do not return to engine.

	case FrameTypeStreamStart, FrameTypeChunk, FrameTypeStreamEnd,
		FrameTypeEnd, FrameTypeErr, FrameTypeLog, FrameTypeCredit:
		// Branch based on XID presence to distinguish response vs
		// request-continuation direction.
		if frame.RoutingId != nil {
			// ========================================
			// HAS XID = RESPONSE CONTINUATION
			// ========================================
			xid := *frame.RoutingId
			rid := frame.Id
			key := RequestKey{Xid: xid, Rid: rid}
			isTerminal := frame.FrameType == FrameTypeEnd || frame.FrameType == FrameTypeErr

			// Record flow stats, resolve the return path, and — on
			// terminal — remove the whole entry atomically (L7). A frame
			// for a released key is a counted no_route drop, never a
			// protocol error and never silent (L8).
			sw.requests.RecordFrame(key, FrameDirectionInbound, frame)

			var state *RequestState
			if isTerminal {
				kind := TerminalKindEnd
				if frame.FrameType == FrameTypeErr {
					kind = TerminalKindErr
				}
				state = sw.requests.Terminate(key, kind)
				if state == nil {
					sw.drops.Record(DropReasonNoRoute)
					return nil, nil
				}
			} else {
				state = sw.requests.Get(key)
				if state == nil {
					sw.drops.Record(DropReasonNoRoute)
					return nil, nil
				}
			}

			if state.Origin == nil {
				if state.ExternalChannel != nil {
					// Deliver to the external response channel (keep XID).
					if !deliverExternal(state.ExternalChannel, *frame) {
						sw.drops.Record(DropReasonChannelClosed)
						// A dead consumer on a LIVE request means the
						// caller abandoned it. Nobody can ever read this
						// response — cancel upstream so the cartridge
						// stops producing for a dead channel. Terminal
						// frames need no cancel: the entry is already
						// terminated.
						if !isTerminal {
							sw.cancelRequestLocked(rid, false)
						}
					}
					return nil, nil
				}
				// No response channel (sent via SendToMaster, not a
				// registered external caller). Strip XID and return to
				// caller (final leg).
				frame.RoutingId = nil
				return frame, nil
			}

			// Route back to source master — KEEP XID.
			if err := sw.masters[*state.Origin].socketWriter.WriteFrame(frame); err != nil {
				return nil, err
			}
			return nil, nil
		}

		// ========================================
		// NO XID = REQUEST CONTINUATION
		// ========================================
		// Frame has no XID, so it's a request continuation (peer-call
		// argument streams / grants) flowing to the destination. An
		// unknown RID means the request already terminated: counted drop
		// (L6), not an error.
		rid := frame.Id
		xid, ok := sw.requests.XidForRid(rid)
		if !ok {
			sw.drops.Record(DropReasonNoRoute)
			return nil, nil
		}
		key := RequestKey{Xid: xid, Rid: rid}
		sw.requests.RecordFrame(key, FrameDirectionInbound, frame)
		state := sw.requests.Get(key)
		if state == nil {
			sw.drops.Record(DropReasonNoRoute)
			return nil, nil
		}

		// Add XID to frame for forwarding.
		frame.RoutingId = &xid
		if err := sw.masters[state.Routing.DestinationMasterIdx].socketWriter.WriteFrame(frame); err != nil {
			return nil, err
		}
		return nil, nil

	case FrameTypeCancel:
		// Cancel from cartridge — route to destination like a continuation
		// frame. Cartridge is cancelling its own peer call. Unknown RID
		// means the request already completed: a well-defined no-op.
		rid := frame.Id
		var xid MessageId
		var ok bool
		if frame.RoutingId != nil {
			xid = *frame.RoutingId
			ok = true
		} else {
			xid, ok = sw.requests.XidForRid(rid)
		}
		if !ok {
			return nil, nil
		}
		key := RequestKey{Xid: xid, Rid: rid}
		state := sw.requests.Get(key)
		if state == nil {
			return nil, nil
		}
		frame.RoutingId = &xid
		if err := sw.masters[state.Routing.DestinationMasterIdx].socketWriter.WriteFrame(frame); err != nil {
			return nil, err
		}
		return nil, nil

	case FrameTypeRelayNotify:
		// Capability update from host — update our cap table
		manifest := frame.RelayNotifyManifest()
		if manifest == nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: "RelayNotify has no payload",
			}
		}

		payload, err := parseRelayNotifyPayload(manifest)
		if err != nil {
			return nil, err
		}
		newCaps := payload.CapURNs()

		if sourceIdx < 0 || sourceIdx >= len(sw.masters) {
			// No master at this index — the slot's reader will exit on
			// its own. Forward the frame to the engine (visibility) and
			// drop the update, matching Rust.
			return frame, nil
		}

		// Detect the empty → non-empty cap transition. The initial
		// RelayNotify (during construction / AddMaster) skipped the
		// identity probe when caps were empty; if the host now advertises
		// a real handler chain we must probe it end-to-end before its
		// caps become routable. The master is held UNHEALTHY (so the
		// cap_table rebuild below excludes it) until the probe driver
		// confirms identity. Mirrors Rust's RelayNotify branch.
		priorCapsEmpty := len(sw.masters[sourceIdx].caps) == 0
		probeRequired := priorCapsEmpty && len(newCaps) > 0

		// Apply the update. installed_cartridges and limits are
		// observation-only inventory the engine wants immediately; caps
		// are written too so update lookups stay consistent, but when a
		// probe is required we mark the master unhealthy below so the
		// cap_table rebuild excludes its caps.
		sw.masters[sourceIdx].caps = newCaps
		sw.masters[sourceIdx].installedCartridges = payload.InstalledCartridges
		sw.masters[sourceIdx].hostProtocolStats = payload.HostProtocolStats
		sw.masters[sourceIdx].manifest = manifest
		if limits := frame.RelayNotifyLimits(); limits != nil {
			sw.masters[sourceIdx].limits = *limits
		}
		if probeRequired {
			sw.masters[sourceIdx].healthy = false
			msg := "runtime identity probe pending — caps held back from routing"
			sw.masters[sourceIdx].lastError = &msg
		}

		// Rebuild aggregate cap table, inventory, the wire-byte snapshot,
		// and limits. cap_table / capabilities are health-filtered so an
		// unhealthy master's caps don't surface as dispatch targets until
		// the probe driver flips it healthy; inventory is NOT filtered.
		sw.rebuildCapTable()
		sw.rebuildInstalledCartridges()
		sw.rebuildCapabilities()
		sw.rebuildLimits()

		if probeRequired {
			// Hand off to the probe driver without blocking under sw.mu.
			select {
			case sw.pendingProbes <- sourceIdx:
			default:
				go func(i int) { sw.pendingProbes <- i }(sourceIdx)
			}
		}

		// Pass through to the engine for visibility (matches Rust, which
		// returns Ok(Some(frame))).
		return frame, nil

	default:
		return frame, nil
	}
}

// handleMasterDeath handles master death: marks the slot unhealthy, terminates
// every pending request routed to it (MasterDied) with a synthetic ERR
// delivered to whoever was waiting, and rebuilds the health-filtered tables.
func (sw *RelaySwitch) handleMasterDeath(masterIdx int) {
	if !sw.masters[masterIdx].healthy {
		return
	}

	sw.masters[masterIdx].healthy = false

	// Find all pending requests routed to this master.
	deadKeys := sw.requests.KeysWhere(func(s *RequestState) bool {
		return s.Routing.DestinationMasterIdx == masterIdx
	})

	// Terminate each pending request (MasterDied) and deliver a synthetic
	// ERR to whoever was waiting on it. Terminate atomically removes ALL
	// state for the key (L7) and hands back the origin + channel needed for
	// delivery.
	for _, key := range deadKeys {
		state := sw.requests.Terminate(key, TerminalKindMasterDied)
		if state == nil {
			continue // raced another terminal — already fully cleaned up
		}

		xid := key.Xid
		errFrame := NewErr(key.Rid, "MASTER_DIED", fmt.Sprintf("Relay master %d connection closed", masterIdx))
		errFrame.RoutingId = &xid

		if state.Origin == nil {
			// External caller — send to response channel if present.
			// Best-effort, like the cancelled-CANCELLED delivery above: not
			// itself counted as a drop. Mirrors Rust's `let _ = tx.send(...)`.
			if state.ExternalChannel != nil {
				deliverExternal(state.ExternalChannel, *errFrame)
			}
		} else {
			srcIdx := *state.Origin
			if srcIdx >= 0 && srcIdx < len(sw.masters) && sw.masters[srcIdx].healthy {
				_ = sw.masters[srcIdx].socketWriter.WriteFrame(errFrame)
			}
		}
	}

	sw.rebuildCapTable()
	sw.rebuildCapabilities()
	sw.rebuildInstalledCartridges()
	sw.rebuildLimits()
}

// rebuildCapTable rebuilds the cap table from all healthy masters
func (sw *RelaySwitch) rebuildCapTable() {
	sw.capTable = []CapTableEntry{}
	for idx, master := range sw.masters {
		if master.healthy {
			for _, cap := range master.caps {
				sw.capTable = append(sw.capTable, CapTableEntry{
					CapURN:    cap,
					MasterIdx: idx,
				})
			}
		}
	}
}

// installedIdentityKey is the full-identity dedup key for an installed
// cartridge: (registry_url, channel, id, version, sha256). Two installs
// of the same id+version from different registries (or channels) are
// distinct cartridges with their own attached process and on-disk tree;
// collapsing them would make the second one invisible to the engine.
// registry_url is nullable; a nil URL is distinguished from the empty
// string so a dev install never collides with a "" registry.
func installedIdentityKey(ic InstalledCartridgeRecord) string {
	reg := "\x00nil"
	if ic.RegistryURL != nil {
		reg = "\x01" + *ic.RegistryURL
	}
	return reg + "\x1f" + ic.Channel + "\x1f" + ic.Id + "\x1f" + ic.Version + "\x1f" + ic.Sha256
}

// rebuildInstalledCartridges rebuilds the aggregate installed-cartridge
// INVENTORY view. This is deliberately NOT health-filtered: it is what is
// physically installed and known to ANY master, regardless of current
// per-master reachability. Filtering by master health caused the "all
// cartridges disappeared" symptom on every transient master flap. The
// reachability story lives in RuntimeStats.Running per cartridge, not in
// whether the parent master happens to be unhealthy at this tick. Dedup
// is on the full identity tuple.
func (sw *RelaySwitch) rebuildInstalledCartridges() {
	seen := make(map[string]bool)
	result := []InstalledCartridgeRecord{}
	for _, master := range sw.masters {
		for _, ic := range master.installedCartridges {
			key := installedIdentityKey(ic)
			if !seen[key] {
				seen[key] = true
				result = append(result, ic)
			}
		}
	}
	sw.aggregateInstalledCartridges = result
}

// rebuildCapabilities rebuilds the ROUTABLE capabilities wire bytes — the
// union of every HEALTHY master's installed cartridges (so a cap only
// surfaces here once its master is identity-verified and routable). This
// is the engine-readiness signal, distinct from the inventory aggregate
// (which is deliberately NOT health-filtered). On an actual change it
// publishes the new snapshot to the capabilities watch via send_replace
// semantics, so a deferred probe completing — which flips a master
// healthy and adds its caps here — wakes subscribers without a notify
// storm from unrelated rebuilds.
func (sw *RelaySwitch) rebuildCapabilities() {
	seen := make(map[string]bool)
	routable := []InstalledCartridgeRecord{}
	for _, master := range sw.masters {
		if !master.healthy {
			continue
		}
		for _, ic := range master.installedCartridges {
			key := installedIdentityKey(ic)
			if !seen[key] {
				seen[key] = true
				routable = append(routable, ic)
			}
		}
	}
	manifest := RelayNotifyCapabilitiesPayload{InstalledCartridges: routable}
	data, _ := json.Marshal(manifest)

	if !bytes.Equal(data, sw.capabilities) {
		sw.capabilities = data
		sw.capWatch.store(data)
	}
}

// rebuildLimits rebuilds negotiated limits
func (sw *RelaySwitch) rebuildLimits() {
	maxInt := int(^uint(0) >> 1) // Max int
	minFrame := maxInt
	minChunk := maxInt
	minInitialCredit := maxInt

	for _, master := range sw.masters {
		if master.healthy {
			if master.limits.MaxFrame < minFrame {
				minFrame = master.limits.MaxFrame
			}
			if master.limits.MaxChunk < minChunk {
				minChunk = master.limits.MaxChunk
			}
			if master.limits.InitialCredit < minInitialCredit {
				minInitialCredit = master.limits.InitialCredit
			}
		}
	}

	if minFrame == maxInt {
		minFrame = DefaultMaxFrame
	}
	if minChunk == maxInt {
		minChunk = DefaultMaxChunk
	}
	if minInitialCredit == maxInt {
		minInitialCredit = DefaultInitialCredit
	}

	sw.negotiatedLimits = Limits{
		MaxFrame:      minFrame,
		MaxChunk:      minChunk,
		InitialCredit: minInitialCredit,
	}
}

// parseRelayNotifyPayload parses caps and installed_cartridges from a RelayNotify manifest payload.
// The payload JSON must contain:
//   - "installed_cartridges": []InstalledCartridgeRecord (required, may be empty)
//
// The flat cap-urn list is no longer carried on the wire — callers
// derive it from `payload.CapURNs()`.
func parseRelayNotifyPayload(manifest []byte) (*RelayNotifyCapabilitiesPayload, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(manifest, &raw); err != nil {
		return nil, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: fmt.Sprintf("invalid manifest JSON: %v", err),
		}
	}

	icRaw, ok := raw["installed_cartridges"]
	if !ok {
		return nil, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: "manifest missing required installed_cartridges array",
		}
	}

	var installedCartridges []InstalledCartridgeRecord
	if err := json.Unmarshal(icRaw, &installedCartridges); err != nil {
		return nil, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: fmt.Sprintf("invalid installed_cartridges field: %v", err),
		}
	}

	if installedCartridges == nil {
		installedCartridges = []InstalledCartridgeRecord{}
	}

	payload := &RelayNotifyCapabilitiesPayload{
		InstalledCartridges: installedCartridges,
	}

	// host_protocol_stats is optional (a per-republish refresh, not a
	// requirement) — absent or explicit null both mean "no stats yet".
	if hpsRaw, ok := raw["host_protocol_stats"]; ok && string(hpsRaw) != "null" {
		var hps HostProtocolStats
		if err := json.Unmarshal(hpsRaw, &hps); err != nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: fmt.Sprintf("invalid host_protocol_stats field: %v", err),
			}
		}
		payload.HostProtocolStats = &hps
	}

	return payload, nil
}
