package bifaci

import (
	"encoding/json"
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

// InstalledCartridgeIdentity represents the identity of an installed cartridge
type InstalledCartridgeIdentity struct {
	Id      string `json:"id"`
	Version string `json:"version"`
	Sha256  string `json:"sha256"`
}

// relayNotifyCapabilitiesPayload is the parsed payload from RelayNotify frames
type relayNotifyCapabilitiesPayload struct {
	Caps                 []string                     `json:"caps"`
	InstalledCartridges  []InstalledCartridgeIdentity `json:"installed_cartridges"`
}

// RoutingEntry tracks request source and destination
type RoutingEntry struct {
	SourceMasterIdx      int
	DestinationMasterIdx int
	RequestId            MessageId // original MessageId for cancel frames
}

// MasterConnection represents a connection to a single RelayMaster
type MasterConnection struct {
	socketWriter        *FrameWriter
	manifest            []byte
	limits              Limits
	caps                []string
	installedCartridges []InstalledCartridgeIdentity
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
	aggregateInstalledCartridges []InstalledCartridgeIdentity
	negotiatedLimits             Limits
	frameRx                      chan MasterFrame
	mu                           sync.Mutex
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

// NewRelaySwitch creates a new RelaySwitch with the given socket pairs
func NewRelaySwitch(sockets []SocketPair) (*RelaySwitch, error) {
	if len(sockets) == 0 {
		return nil, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: "RelaySwitch requires at least one master",
		}
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
			socketWriter:        socketWriter,
			manifest:            manifest,
			limits:              *limits,
			caps:                payload.Caps,
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
		aggregateInstalledCartridges: []InstalledCartridgeIdentity{},
		frameRx:                      frameRx,
	}

	sw.rebuildCapTable()
	sw.rebuildCapabilities()
	sw.rebuildInstalledCartridges()
	sw.rebuildLimits()

	return sw, nil
}

type SocketPair struct {
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
func (sw *RelaySwitch) InstalledCartridges() []InstalledCartridgeIdentity {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	result := make([]InstalledCartridgeIdentity, len(sw.aggregateInstalledCartridges))
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
			sw.masters[sourceIdx].caps = payload.Caps
			sw.masters[sourceIdx].installedCartridges = payload.InstalledCartridges
			sw.masters[sourceIdx].manifest = manifest
			// Extract and update limits from RelayNotify
			if limits := frame.RelayNotifyLimits(); limits != nil {
				sw.masters[sourceIdx].limits = *limits
			}
		}

		// Rebuild aggregate capability table and installed cartridges
		sw.rebuildCapTable()
		sw.rebuildInstalledCartridges()

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
	result := []InstalledCartridgeIdentity{}
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

// rebuildCapabilities rebuilds aggregate capabilities
func (sw *RelaySwitch) rebuildCapabilities() {
	capSet := make(map[string]bool)
	for _, master := range sw.masters {
		if master.healthy {
			for _, cap := range master.caps {
				capSet[cap] = true
			}
		}
	}

	caps := []string{}
	for cap := range capSet {
		caps = append(caps, cap)
	}

	manifest := map[string]interface{}{
		"capabilities": caps,
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
// The payload JSON may contain:
//   - "caps": []string  (the capability URN list)
//   - "installed_cartridges": []InstalledCartridgeIdentity (optional)
//
// For backward compatibility, if "caps" is absent but "capabilities" is present, it is used.
func parseRelayNotifyPayload(manifest []byte) (*relayNotifyCapabilitiesPayload, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(manifest, &raw); err != nil {
		return nil, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: fmt.Sprintf("invalid manifest JSON: %v", err),
		}
	}

	var caps []string

	// Try "caps" first (new format), fall back to "capabilities" (old format)
	if capsRaw, ok := raw["caps"]; ok {
		if err := json.Unmarshal(capsRaw, &caps); err != nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: fmt.Sprintf("invalid caps field: %v", err),
			}
		}
	} else if capsRaw, ok := raw["capabilities"]; ok {
		if err := json.Unmarshal(capsRaw, &caps); err != nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: fmt.Sprintf("invalid capabilities field: %v", err),
			}
		}
	} else {
		return nil, &RelaySwitchError{
			Type:    RelaySwitchErrorTypeProtocol,
			Message: "manifest missing caps/capabilities array",
		}
	}

	var installedCartridges []InstalledCartridgeIdentity
	if icRaw, ok := raw["installed_cartridges"]; ok {
		if err := json.Unmarshal(icRaw, &installedCartridges); err != nil {
			return nil, &RelaySwitchError{
				Type:    RelaySwitchErrorTypeProtocol,
				Message: fmt.Sprintf("invalid installed_cartridges field: %v", err),
			}
		}
	}

	if installedCartridges == nil {
		installedCartridges = []InstalledCartridgeIdentity{}
	}

	return &relayNotifyCapabilitiesPayload{
		Caps:                caps,
		InstalledCartridges: installedCartridges,
	}, nil
}
