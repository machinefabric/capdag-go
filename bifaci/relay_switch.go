package bifaci

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"

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

// RoutingEntry tracks request source and destination
type RoutingEntry struct {
	SourceMasterIdx      int
	DestinationMasterIdx int
	RequestId            MessageId // original MessageId for cancel frames
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
	healthy             bool
}

// peerCallChild stores a child peer-call routing key for cancel cascading
type peerCallChild struct {
	key string
}

// RelaySwitch is a cap-aware routing multiplexer for multiple RelayMasters
type RelaySwitch struct {
	masters                      []*MasterConnection
	capTable                     []CapTableEntry
	requestRouting               map[string]*RoutingEntry
	peerRequests                 map[string]bool
	peerCallParents              map[string][]peerCallChild // parent key → list of child peer calls
	capabilities                 []byte
	aggregateInstalledCartridges []InstalledCartridgeRecord
	negotiatedLimits             Limits
	frameRx                      chan MasterFrame
	mu                           sync.Mutex
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

type MasterFrame struct {
	MasterIdx int
	Frame     *Frame
	Err       error
}

// ENGINE_SOURCE sentinel value for engine-initiated requests
const ENGINE_SOURCE = -1

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

	// Connect to all masters and spawn reader goroutines
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

		// Spawn reader goroutine
		idx := masterIdx
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

		masters = append(masters, &MasterConnection{
			id:                  sockPair.ID,
			socketWriter:        socketWriter,
			manifest:            manifest,
			limits:              *limits,
			caps:                payload.CapURNs(),
			installedCartridges: payload.InstalledCartridges,
			healthy:             true,
		})
	}

	sw := &RelaySwitch{
		masters:                      masters,
		capTable:                     []CapTableEntry{},
		requestRouting:               make(map[string]*RoutingEntry),
		peerRequests:                 make(map[string]bool),
		peerCallParents:              make(map[string][]peerCallChild),
		aggregateInstalledCartridges: []InstalledCartridgeRecord{},
		frameRx:                      frameRx,
	}

	sw.rebuildCapTable()
	// rebuildInstalledCartridges MUST run before rebuildCapabilities:
	// rebuildCapabilities snapshots aggregateInstalledCartridges into
	// the wire-bytes that consumers read via Capabilities(), so the
	// aggregate must be current when that snapshot is taken.
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
			caps:                payload.CapURNs(),
			installedCartridges: payload.InstalledCartridges,
			healthy:             true,
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
		slot.caps = payload.CapURNs()
		slot.installedCartridges = payload.InstalledCartridges
		slot.healthy = true
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

// CancelRequest cancels a specific in-flight request by request ID.
//
// Sends Cancel frame to the destination master, cascades to child peer calls,
// and cleans up all routing maps.
func (sw *RelaySwitch) CancelRequest(rid MessageId, forceKill bool) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.cancelRequestLocked(rid.ToString(), forceKill)
}

// cancelRequestLocked must be called with sw.mu held.
// ridKey is the string form of rid for map lookups.
func (sw *RelaySwitch) cancelRequestLocked(ridKey string, forceKill bool) {
	entry, ok := sw.requestRouting[ridKey]
	if !ok {
		return
	}

	destIdx := entry.DestinationMasterIdx
	rid := entry.RequestId

	// Build and send cancel frame to destination
	cancelFrame := NewCancelFrame(rid, forceKill)
	_ = sw.masters[destIdx].socketWriter.WriteFrame(cancelFrame)

	// Collect child peer calls for recursive cancel
	children := sw.peerCallParents[ridKey]
	delete(sw.peerCallParents, ridKey)

	// Recursively cancel children
	for _, child := range children {
		sw.cancelRequestLocked(child.key, forceKill)
	}

	// Cleanup routing maps
	delete(sw.requestRouting, ridKey)
	delete(sw.peerRequests, ridKey)
}

// CancelAllRequests cancels all external-origin (engine-initiated) in-flight requests.
// Returns the list of cancelled request IDs.
func (sw *RelaySwitch) CancelAllRequests(forceKill bool) []MessageId {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Snapshot all engine-origin entries before mutating
	type entry struct {
		key string
		rid MessageId
	}
	var entries []entry
	for key, e := range sw.requestRouting {
		if e.SourceMasterIdx == ENGINE_SOURCE {
			entries = append(entries, entry{key: key, rid: e.RequestId})
		}
	}

	for _, e := range entries {
		sw.cancelRequestLocked(e.key, forceKill)
	}

	rids := make([]MessageId, len(entries))
	for i, e := range entries {
		rids[i] = e.rid
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

		sw.requestRouting[frame.Id.ToString()] = &RoutingEntry{
			SourceMasterIdx:      ENGINE_SOURCE,
			DestinationMasterIdx: destIdx,
			RequestId:            frame.Id,
		}

		return sw.masters[destIdx].socketWriter.WriteFrame(frame)

	case FrameTypeStreamStart, FrameTypeChunk, FrameTypeStreamEnd,
		FrameTypeEnd, FrameTypeErr:
		entry, ok := sw.requestRouting[frame.Id.ToString()]
		if !ok {
			return &RelaySwitchError{
				Type:    RelaySwitchErrorTypeUnknownRequest,
				Message: frame.Id.ToString(),
			}
		}

		destIdx := entry.DestinationMasterIdx
		err := sw.masters[destIdx].socketWriter.WriteFrame(frame)
		if err != nil {
			return err
		}

		// Cleanup on terminal frames for peer responses
		isTerminal := frame.FrameType == FrameTypeEnd || frame.FrameType == FrameTypeErr
		if isTerminal && sw.peerRequests[frame.Id.ToString()] {
			delete(sw.requestRouting, frame.Id.ToString())
			delete(sw.peerRequests, frame.Id.ToString())
		}

		return nil

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
		signedDistance  int
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
				masterIdx:     entry.MasterIdx,
				signedDistance: signedDistance,
				isPreferred:   isPreferred,
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
func (sw *RelaySwitch) handleMasterFrame(sourceIdx int, frame *Frame) (*Frame, error) {
	switch frame.FrameType {
	case FrameTypeReq:
		// Peer request
		if frame.Cap == nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: "REQ frame missing cap URN",
			}
		}

		// Peer request (no preference)
		destIdx, err := sw.findMasterForCap(*frame.Cap, nil)
		if err != nil {
			return nil, err
		}

		sw.requestRouting[frame.Id.ToString()] = &RoutingEntry{
			SourceMasterIdx:      sourceIdx,
			DestinationMasterIdx: destIdx,
			RequestId:            frame.Id,
		}
		sw.peerRequests[frame.Id.ToString()] = true

		err = sw.masters[destIdx].socketWriter.WriteFrame(frame)
		if err != nil {
			return nil, err
		}

		return nil, nil // Internal routing

	case FrameTypeStreamStart, FrameTypeChunk, FrameTypeStreamEnd,
		FrameTypeEnd, FrameTypeErr, FrameTypeLog:
		entry, ok := sw.requestRouting[frame.Id.ToString()]
		if ok && entry.SourceMasterIdx != ENGINE_SOURCE {
			// Response to peer request
			destIdx := entry.SourceMasterIdx
			isTerminal := frame.FrameType == FrameTypeEnd || frame.FrameType == FrameTypeErr

			err := sw.masters[destIdx].socketWriter.WriteFrame(frame)
			if err != nil {
				return nil, err
			}

			if isTerminal && !sw.peerRequests[frame.Id.ToString()] {
				delete(sw.requestRouting, frame.Id.ToString())
			}

			return nil, nil
		}

		// Response to engine request
		isTerminal := frame.FrameType == FrameTypeEnd || frame.FrameType == FrameTypeErr
		if isTerminal && !sw.peerRequests[frame.Id.ToString()] {
			delete(sw.requestRouting, frame.Id.ToString())
		}

		return frame, nil

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

		// Update master's caps, installed cartridges, and limits
		if sourceIdx >= 0 && sourceIdx < len(sw.masters) {
			sw.masters[sourceIdx].caps = payload.CapURNs()
			sw.masters[sourceIdx].installedCartridges = payload.InstalledCartridges
			sw.masters[sourceIdx].manifest = manifest
			// Extract and update limits from RelayNotify
			if limits := frame.RelayNotifyLimits(); limits != nil {
				sw.masters[sourceIdx].limits = *limits
			}
		}

		// Rebuild aggregate capability table, installed cartridges, then
		// the wire-byte snapshot. rebuildCapabilities must run last
		// because it serializes aggregateInstalledCartridges, which
		// rebuildInstalledCartridges populated above.
		sw.rebuildCapTable()
		sw.rebuildInstalledCartridges()
		sw.rebuildCapabilities()

		// RelayNotify is consumed internally, don't forward to engine
		return nil, nil

	default:
		return frame, nil
	}
}

// handleMasterDeath handles master death
func (sw *RelaySwitch) handleMasterDeath(masterIdx int) {
	if !sw.masters[masterIdx].healthy {
		return
	}

	sw.masters[masterIdx].healthy = false

	// Cleanup routing
	for reqID, entry := range sw.requestRouting {
		if entry.DestinationMasterIdx == masterIdx {
			delete(sw.requestRouting, reqID)
			delete(sw.peerRequests, reqID)
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

// rebuildInstalledCartridges rebuilds aggregate installed cartridge identities
func (sw *RelaySwitch) rebuildInstalledCartridges() {
	seen := make(map[string]bool)
	result := []InstalledCartridgeRecord{}
	for _, master := range sw.masters {
		if master.healthy {
			for _, ic := range master.installedCartridges {
				key := ic.Id + "@" + ic.Version
				if !seen[key] {
					seen[key] = true
					result = append(result, ic)
				}
			}
		}
	}
	sw.aggregateInstalledCartridges = result
}

// rebuildCapabilities rebuilds aggregate capabilities. The wire payload
// now carries caps inside `installed_cartridges[*].cap_groups`; we
// republish the union of every healthy master's installed cartridges so
// the engine sees one combined view.
func (sw *RelaySwitch) rebuildCapabilities() {
	manifest := RelayNotifyCapabilitiesPayload{
		InstalledCartridges: append([]InstalledCartridgeRecord{}, sw.aggregateInstalledCartridges...),
	}
	data, _ := json.Marshal(manifest)
	sw.capabilities = data
}

// rebuildLimits rebuilds negotiated limits
func (sw *RelaySwitch) rebuildLimits() {
	minFrame := int(^uint(0) >> 1) // Max int
	minChunk := int(^uint(0) >> 1)

	for _, master := range sw.masters {
		if master.healthy {
			if master.limits.MaxFrame < minFrame {
				minFrame = master.limits.MaxFrame
			}
			if master.limits.MaxChunk < minChunk {
				minChunk = master.limits.MaxChunk
			}
		}
	}

	if minFrame == int(^uint(0)>>1) {
		minFrame = DefaultMaxFrame
	}
	if minChunk == int(^uint(0)>>1) {
		minChunk = DefaultMaxChunk
	}

	sw.negotiatedLimits = Limits{
		MaxFrame: minFrame,
		MaxChunk: minChunk,
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

	return &RelayNotifyCapabilitiesPayload{
		InstalledCartridges: installedCartridges,
	}, nil
}
