package bifaci

import (
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"sync"

	"github.com/machinefabric/capdag-go/urn"
)

// CapHandler is a function that handles a peer invoke request.
// It receives the concatenated payload bytes and returns response bytes.
type CapHandler func(payload []byte) ([]byte, error)

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

// routingEntry tracks a routed request with its original MessageId.
type routingEntry struct {
	cartridgeIdx int
	msgId        MessageId
}

// ManagedCartridge represents a cartridge managed by the CartridgeHost.
type ManagedCartridge struct {
	path        string
	cmd         *exec.Cmd
	writerCh    chan *Frame
	manifest    []byte
	limits      Limits
	caps        []string
	knownCaps   []string
	running     bool
	helloFailed bool
}

// CartridgeHost manages N cartridge binaries with cap-based routing.
//
// Cartridges are either registered (for on-demand spawning) or attached
// (pre-connected). REQ frames from the relay are routed to the correct
// cartridge by cap URN. Continuation frames (STREAM_START, CHUNK,
// STREAM_END, END) are routed by request ID.
type CartridgeHost struct {
	cartridges     []*ManagedCartridge
	capTable       []capTableEntry
	requestRouting map[string]routingEntry // reqId string → routing info
	peerRequests   map[string]bool         // cartridge-initiated reqIds
	capabilities   []byte
	eventCh        chan cartridgeEvent
	mu             sync.Mutex
}

// NewCartridgeHost creates a new multi-cartridge host.
func NewCartridgeHost() *CartridgeHost {
	return &CartridgeHost{
		requestRouting: make(map[string]routingEntry),
		peerRequests:   make(map[string]bool),
		eventCh:        make(chan cartridgeEvent, 256),
	}
}

// RegisterCartridge registers a cartridge binary for on-demand spawning.
// The cartridge is not spawned until a REQ arrives for one of its known caps.
func (h *CartridgeHost) RegisterCartridge(path string, knownCaps []string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	cartridgeIdx := len(h.cartridges)
	h.cartridges = append(h.cartridges, &ManagedCartridge{
		path:      path,
		knownCaps: knownCaps,
		running:   false,
		limits:    DefaultLimits(),
	})

	for _, cap := range knownCaps {
		h.capTable = append(h.capTable, capTableEntry{capUrn: cap, cartridgeIdx: cartridgeIdx})
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

	caps, err := parseCapsFromManifest(manifest)
	if err != nil {
		return -1, fmt.Errorf("failed to parse manifest: %w", err)
	}

	h.mu.Lock()
	cartridgeIdx := len(h.cartridges)

	writerCh := make(chan *Frame, 64)
	cartridge := &ManagedCartridge{
		writerCh: writerCh,
		manifest: manifest,
		limits:   limits,
		caps:     caps,
		running:  true,
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
	// Exact match first
	for _, entry := range h.capTable {
		if entry.capUrn == capUrn {
			return entry.cartridgeIdx, true
		}
	}

	// URN-level matching: use is_dispatchable (provider can handle request)
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

	for {
		select {
		case frame, ok := <-relayCh:
			if !ok {
				err := <-relayDone
				h.killAllCartridges()
				return err
			}
			if err := h.handleRelayFrame(frame, relayWriter); err != nil {
				h.killAllCartridges()
				return err
			}

		case event := <-h.eventCh:
			if event.isDeath {
				h.handleCartridgeDeath(event.cartridgeIdx, relayWriter)
			} else if event.frame != nil {
				h.handleCartridgeFrame(event.cartridgeIdx, event.frame, relayWriter)
			}
		}
	}
}

// handleRelayFrame routes an incoming frame from the relay to the correct cartridge.
func (h *CartridgeHost) handleRelayFrame(frame *Frame, relayWriter *FrameWriter) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	idKey := frame.Id.ToString()

	switch frame.FrameType {
	case FrameTypeReq:
		capUrn := ""
		if frame.Cap != nil {
			capUrn = *frame.Cap
		}

		cartridgeIdx, found := h.findCartridgeForCapLocked(capUrn)
		if !found {
			errFrame := NewErr(frame.Id, "NO_HANDLER", fmt.Sprintf("no cartridge handles cap: %s", capUrn))
			relayWriter.WriteFrame(errFrame)
			return nil
		}

		cartridge := h.cartridges[cartridgeIdx]
		if !cartridge.running {
			if cartridge.helloFailed {
				errFrame := NewErr(frame.Id, "SPAWN_FAILED", "cartridge previously failed to start")
				relayWriter.WriteFrame(errFrame)
				return nil
			}
			if err := h.spawnCartridgeLocked(cartridgeIdx); err != nil {
				errFrame := NewErr(frame.Id, "SPAWN_FAILED", err.Error())
				relayWriter.WriteFrame(errFrame)
				return nil
			}
		}

		h.requestRouting[idKey] = routingEntry{cartridgeIdx: cartridgeIdx, msgId: frame.Id}
		h.sendToCartridge(cartridgeIdx, frame)

	case FrameTypeStreamStart, FrameTypeChunk, FrameTypeStreamEnd:
		if entry, ok := h.requestRouting[idKey]; ok {
			h.sendToCartridge(entry.cartridgeIdx, frame)
		}

	case FrameTypeEnd, FrameTypeErr:
		if entry, ok := h.requestRouting[idKey]; ok {
			h.sendToCartridge(entry.cartridgeIdx, frame)
			// Only remove routing on terminal frames if this is a PEER response
			// (engine responding to a cartridge's peer invoke). For engine-initiated
			// requests, the relay END is just the end of the request body — the
			// cartridge still needs to respond, so routing must survive.
			if h.peerRequests[idKey] {
				delete(h.requestRouting, idKey)
				delete(h.peerRequests, idKey)
			}
		}

	case FrameTypeHeartbeat:
		// Engine-level heartbeat — not forwarded to cartridges
		return nil

	case FrameTypeHello:
		return fmt.Errorf("unexpected HELLO from relay")

	case FrameTypeRelayNotify, FrameTypeRelayState:
		return fmt.Errorf("relay frame %v reached cartridge host", frame.FrameType)
	}

	return nil
}

// handleCartridgeFrame processes a frame from a cartridge.
func (h *CartridgeHost) handleCartridgeFrame(cartridgeIdx int, frame *Frame, relayWriter *FrameWriter) {
	h.mu.Lock()
	defer h.mu.Unlock()

	idKey := frame.Id.ToString()

	switch frame.FrameType {
	case FrameTypeHeartbeat:
		// Respond to cartridge heartbeat locally — don't forward
		response := NewHeartbeat(frame.Id)
		h.sendToCartridge(cartridgeIdx, response)

	case FrameTypeHello:
		// HELLO post-handshake — protocol violation, ignore
		return

	case FrameTypeReq:
		// Cartridge is invoking a peer cap (sending request to engine)
		h.requestRouting[idKey] = routingEntry{cartridgeIdx: cartridgeIdx, msgId: frame.Id}
		h.peerRequests[idKey] = true
		relayWriter.WriteFrame(frame)

	case FrameTypeLog:
		relayWriter.WriteFrame(frame)

	case FrameTypeStreamStart, FrameTypeChunk, FrameTypeStreamEnd:
		relayWriter.WriteFrame(frame)

	case FrameTypeEnd:
		relayWriter.WriteFrame(frame)
		if !h.peerRequests[idKey] {
			delete(h.requestRouting, idKey)
		}

	case FrameTypeErr:
		relayWriter.WriteFrame(frame)
		delete(h.requestRouting, idKey)
		delete(h.peerRequests, idKey)
	}
}

// handleCartridgeDeath processes a cartridge death event.
func (h *CartridgeHost) handleCartridgeDeath(cartridgeIdx int, relayWriter *FrameWriter) {
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

	// Send ERR for all pending requests routed to this cartridge
	var failedEntries []routingEntry
	var failedKeys []string
	for reqId, entry := range h.requestRouting {
		if entry.cartridgeIdx == cartridgeIdx {
			failedEntries = append(failedEntries, entry)
			failedKeys = append(failedKeys, reqId)
		}
	}

	for i, key := range failedKeys {
		errFrame := NewErr(failedEntries[i].msgId, "CARTRIDGE_DIED", fmt.Sprintf("cartridge %d died", cartridgeIdx))
		relayWriter.WriteFrame(errFrame)
		delete(h.requestRouting, key)
		delete(h.peerRequests, key)
	}

	h.updateCapTable()
	h.rebuildCapabilities()
}

// sendToCartridge sends a frame to a cartridge via its writer channel.
func (h *CartridgeHost) sendToCartridge(cartridgeIdx int, frame *Frame) {
	cartridge := h.cartridges[cartridgeIdx]
	if cartridge.writerCh != nil {
		select {
		case cartridge.writerCh <- frame:
		default:
			// Channel full — cartridge probably dead, frame dropped
		}
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

	caps, parseErr := parseCapsFromManifest(manifest)
	if parseErr != nil {
		cartridge.helloFailed = true
		cmd.Process.Kill()
		return fmt.Errorf("failed to parse manifest: %w", parseErr)
	}

	cartridge.manifest = manifest
	cartridge.limits = limits
	cartridge.caps = caps
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
		caps := cartridge.knownCaps
		if cartridge.running && len(cartridge.caps) > 0 {
			caps = cartridge.caps
		}
		for _, cap := range caps {
			h.capTable = append(h.capTable, capTableEntry{capUrn: cap, cartridgeIdx: idx})
		}
	}
}

// rebuildCapabilities rebuilds the aggregate capabilities JSON.
func (h *CartridgeHost) rebuildCapabilities() {
	var allCaps []string
	for _, cartridge := range h.cartridges {
		if cartridge.running {
			allCaps = append(allCaps, cartridge.caps...)
		}
	}

	if len(allCaps) == 0 {
		h.capabilities = nil
		return
	}

	// Serialize as JSON array of URN strings (NOT a map)
	capsJSON, err := json.Marshal(allCaps)
	if err != nil {
		h.capabilities = nil
		return
	}
	h.capabilities = capsJSON
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

// parseCapsFromManifest parses cap URNs from a JSON manifest.
// Expected format: {"caps": [{"urn": "cap:op=test", ...}, ...]}
func parseCapsFromManifest(manifest []byte) ([]string, error) {
	if len(manifest) == 0 {
		return nil, nil
	}

	var parsed struct {
		Caps []struct {
			Urn string `json:"urn"`
		} `json:"caps"`
	}

	if err := json.Unmarshal(manifest, &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse manifest JSON: %w", err)
	}

	var caps []string
	for _, cap := range parsed.Caps {
		if cap.Urn != "" {
			caps = append(caps, cap.Urn)
		}
	}

	return caps, nil
}
