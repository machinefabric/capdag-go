// In-Process Cartridge Host — Direct dispatch to FrameHandler implementations.
//
// Sits where CartridgeRuntime sits (connected to a RelaySlave via a local socket
// pair), but routes requests to FrameHandler implementations instead of cartridge
// binaries.
//
// # Architecture
//
//	RelaySlave ←→ InProcessCartridgeHost ←→ Handler A (streaming frames)
//	                                    ←→ Handler B (streaming frames)
//	                                    ←→ Handler C (streaming frames)
//
// # Design
//
// The host does NOT accumulate data. On REQ, it spawns a handler goroutine with
// channels for frame I/O. All continuation frames (STREAM_START, CHUNK,
// STREAM_END, END) are forwarded to the handler. The handler processes frames
// natively — streaming or accumulating as it sees fit.
//
// This matches how real cartridges work: CartridgeRuntime forwards frames to
// handlers, and each handler decides how to consume/produce data.
package bifaci

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"

	cborlib "github.com/fxamacker/cbor/v2"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// =============================================================================
// FRAME HANDLER INTERFACE
// =============================================================================

// FrameHandler handles streaming frame-based requests.
//
// Handlers receive input frames (STREAM_START, CHUNK, STREAM_END, END) via a
// channel and send response frames via a ResponseWriter. The host never
// accumulates — handlers decide how to process input (stream or accumulate).
//
// Handlers can invoke other caps via peer (a PeerInvoker). For handlers that
// don't need streaming, use AccumulateInput to collect all input streams into
// []cap.CapArgumentValue.
type FrameHandler interface {
	// HandleRequest handles a streaming request.
	//
	// Called in a dedicated goroutine for each incoming request. The handler
	// reads input frames from input and sends response frames via output.
	// The handler can invoke other caps via peer.
	//
	// The REQ frame has already been consumed by the host. input receives:
	// STREAM_START, CHUNK, STREAM_END (per argument stream), then END.
	//
	// The handler MUST send a complete response: either response frames
	// (STREAM_START + CHUNK(s) + STREAM_END + END) or an error (via
	// output.EmitError).
	HandleRequest(capUrn string, input <-chan Frame, output *ResponseWriter, peer PeerInvoker)
}

// =============================================================================
// RESPONSE WRITER
// =============================================================================

// ResponseWriter wraps an output channel with automatic request_id and
// routing_id stamping.
//
// All frames sent via ResponseWriter get the correct request_id and routing_id
// for relay routing. Seq is left at 0 — the wire writer's SeqAssigner handles it.
type ResponseWriter struct {
	requestId MessageId
	routingId *MessageId
	tx        chan<- Frame
	maxChunk  int
}

func newResponseWriter(requestId MessageId, routingId *MessageId, tx chan<- Frame, maxChunk int) *ResponseWriter {
	return &ResponseWriter{
		requestId: requestId,
		routingId: routingId,
		tx:        tx,
		maxChunk:  maxChunk,
	}
}

// Send sends a frame, stamping it with the request_id and routing_id.
func (w *ResponseWriter) Send(frame Frame) {
	frame.Id = w.requestId
	frame.RoutingId = w.routingId
	frame.Seq = 0 // SeqAssigner handles this
	w.tx <- frame
}

// MaxChunk returns the max chunk size for this connection.
func (w *ResponseWriter) MaxChunk() int {
	return w.maxChunk
}

// EmitResponseWithMeta sends a complete data response with metadata on
// STREAM_START: STREAM_START + CBOR-encoded CHUNK(s) + STREAM_END + END.
func (w *ResponseWriter) EmitResponseWithMeta(mediaUrn string, data []byte, meta map[string]interface{}) {
	streamId := "result"

	start := NewStreamStart(NewMessageIdFromUint(0), streamId, mediaUrn, nil)
	start.Meta = meta
	w.Send(*start)

	if len(data) == 0 {
		cborPayload, err := cborlib.Marshal([]byte{})
		if err != nil {
			panic(fmt.Sprintf("BUG: CBOR encode empty bytes: %v", err))
		}
		checksum := ComputeChecksum(cborPayload)
		w.Send(*NewChunk(NewMessageIdFromUint(0), streamId, 0, cborPayload, 0, checksum))
		w.Send(*NewStreamEnd(NewMessageIdFromUint(0), streamId, 1))
	} else {
		chunkCount := uint64((len(data) + w.maxChunk - 1) / w.maxChunk)
		idx := uint64(0)
		for offset := 0; offset < len(data); offset += w.maxChunk {
			end := offset + w.maxChunk
			if end > len(data) {
				end = len(data)
			}
			chunkData := data[offset:end]
			cborPayload, err := cborlib.Marshal(chunkData)
			if err != nil {
				panic(fmt.Sprintf("BUG: CBOR encode chunk bytes: %v", err))
			}
			checksum := ComputeChecksum(cborPayload)
			w.Send(*NewChunk(NewMessageIdFromUint(0), streamId, 0, cborPayload, idx, checksum))
			idx++
		}
		w.Send(*NewStreamEnd(NewMessageIdFromUint(0), streamId, chunkCount))
	}

	w.Send(*EndOk(NewMessageIdFromUint(0), nil))
}

// EmitResponse sends a complete data response: STREAM_START + CBOR-encoded
// CHUNK(s) + STREAM_END + END.
func (w *ResponseWriter) EmitResponse(mediaUrn string, data []byte) {
	w.EmitResponseWithMeta(mediaUrn, data, nil)
}

// EmitError sends an error response.
func (w *ResponseWriter) EmitError(code, message string) {
	w.Send(*NewErr(NewMessageIdFromUint(0), code, AttributionClassInternal, message, nil))
}

// =============================================================================
// INPUT ACCUMULATION UTILITY
// =============================================================================

// AccumulateInput accumulates all input streams from a frame channel into
// CapArgumentValues.
//
// Reads frames until END. CBOR-decodes chunk payloads to extract raw bytes.
// For handlers that don't need streaming — they accumulate all input, process,
// then emit a response.
//
// Returns (args, meta) where meta is the stream metadata from the first input
// stream's STREAM_START frame. Returns an error on CBOR decode failure
// (protocol violation).
func AccumulateInput(input <-chan Frame) ([]cap.CapArgumentValue, map[string]interface{}, error) {
	type streamEntry struct {
		mediaUrn string
		data     []byte
	}
	var streams []streamEntry
	active := make(map[string]int)
	var requestMeta map[string]interface{}

	for frame := range input {
		switch frame.FrameType {
		case FrameTypeStreamStart:
			sid := ""
			if frame.StreamId != nil {
				sid = *frame.StreamId
			}
			mediaUrn := ""
			if frame.MediaUrn != nil {
				mediaUrn = *frame.MediaUrn
			}
			// Capture meta from the first input stream
			if requestMeta == nil {
				requestMeta = frame.Meta
			}
			idx := len(streams)
			streams = append(streams, streamEntry{mediaUrn: mediaUrn})
			active[sid] = idx
		case FrameTypeChunk:
			sid := ""
			if frame.StreamId != nil {
				sid = *frame.StreamId
			}
			if idx, ok := active[sid]; ok {
				if frame.Payload != nil {
					// CBOR-decode chunk payload to extract raw bytes. Accept ONLY
					// Bytes/Text (the accumulate contract) and fail hard on any
					// other CBOR type — never coerce a number/bool to a string.
					// Mirrors the Rust accumulate_input inline match exactly.
					var value interface{}
					if err := cborlib.Unmarshal(frame.Payload, &value); err != nil {
						return nil, nil, fmt.Errorf(
							"chunk payload is not valid CBOR (stream=%s, %d bytes): %w",
							sid, len(frame.Payload), err)
					}
					switch v := value.(type) {
					case []byte:
						streams[idx].data = append(streams[idx].data, v...)
					case string:
						streams[idx].data = append(streams[idx].data, []byte(v)...)
					default:
						return nil, nil, fmt.Errorf(
							"unexpected CBOR type in chunk payload: %T", value)
					}
				}
			}
		case FrameTypeStreamEnd:
			// nothing to do
		case FrameTypeEnd:
			goto done
		default:
			// ignore unexpected frame types
		}
	}
done:
	args := make([]cap.CapArgumentValue, 0, len(streams))
	for _, s := range streams {
		args = append(args, cap.NewCapArgumentValue(s.mediaUrn, s.data))
	}
	return args, requestMeta, nil
}

// =============================================================================
// BUILT-IN IDENTITY HANDLER
// =============================================================================

// identityHandler is a raw byte passthrough (no CBOR decode/encode).
//
// Echoes all accumulated chunk payloads back as-is. This is the protocol-level
// identity verification — it proves the transport works end-to-end.
type identityHandler struct{}

func (identityHandler) HandleRequest(_ string, input <-chan Frame, output *ResponseWriter, _ PeerInvoker) {
	// Accumulate raw payload bytes (no CBOR decode — identity is raw passthrough)
	var data []byte
	for frame := range input {
		switch frame.FrameType {
		case FrameTypeChunk:
			if frame.Payload != nil {
				data = append(data, frame.Payload...)
			}
		case FrameTypeEnd:
			goto done
		default:
			// STREAM_START, STREAM_END — skip
		}
	}
done:
	// Echo back as a single stream (raw bytes, no CBOR encode)
	streamId := "identity"
	output.Send(*NewStreamStart(NewMessageIdFromUint(0), streamId, "media:", nil))

	checksum := ComputeChecksum(data)
	output.Send(*NewChunk(NewMessageIdFromUint(0), streamId, 0, data, 0, checksum))

	output.Send(*NewStreamEnd(NewMessageIdFromUint(0), streamId, 1))
	output.Send(*EndOk(NewMessageIdFromUint(0), nil))
}

// =============================================================================
// IN-PROCESS CARTRIDGE HOST
// =============================================================================

// HandlerRegistration is a (name, caps, handler) tuple registered with an
// InProcessCartridgeHost.
type HandlerRegistration struct {
	Name    string
	Caps    []cap.Cap
	Handler FrameHandler
}

// handlerEntry is an entry for a registered in-process handler.
type handlerEntry struct {
	name    string
	caps    []cap.Cap
	handler FrameHandler
}

// inProcessCapTableEntry maps a cap URN string to a handler index.
type inProcessCapTableEntry struct {
	capUrn     string
	handlerIdx int
}

// InProcessHostIdentity declares the identity values an InProcessCartridgeHost
// advertises in its RelayNotify payload. The host has no on-disk cartridge
// directory, so the embedding application must supply the same four-tuple
// identity (registry_url, channel, id, version) it would have read from a
// cartridge.json — plus a content-derived sha256 so the engine treats the
// in-process cartridge indistinguishably from any other installed cartridge.
type InProcessHostIdentity struct {
	// RegistryURL the embedding binary was built for. nil ⇔ dev build.
	RegistryURL *string
	// Channel the embedding binary was built for.
	Channel CartridgeChannel
	// Id is the stable id for this host.
	Id string
	// Version of the embedding binary.
	Version string
	// Sha256 is a content-derived hash of the host. The engine asserts this is
	// non-empty.
	Sha256 string
}

// InProcessHostIdentityForTest returns identity values for unit/integration
// tests. Carries a fixed-bytes sha256 so the engine's non-empty-hash assertion
// passes; channel and version are stable test defaults.
func InProcessHostIdentityForTest(id string) InProcessHostIdentity {
	return InProcessHostIdentity{
		RegistryURL: nil,
		Channel:     CartridgeChannelRelease,
		Id:          id,
		Version:     "0.0.0-test",
		Sha256:      "0000000000000000000000000000000000000000000000000000000000000000",
	}
}

// InProcessCartridgeHost is a cartridge host that dispatches to in-process
// FrameHandler implementations.
//
// Speaks the Frame protocol to a RelaySlave, but routes requests to FrameHandler
// implementations via frame channels — no accumulation at the host level,
// handlers own the streaming.
type InProcessCartridgeHost struct {
	identity InProcessHostIdentity
	handlers []handlerEntry
}

// NewInProcessCartridgeHost creates a new in-process cartridge host with the
// given handlers.
//
// identity declares the host's on-the-wire identity (the embedding binary
// supplies it; see InProcessHostIdentity). Each handler is a (name, caps,
// handler) registration.
func NewInProcessCartridgeHost(identity InProcessHostIdentity, handlers []HandlerRegistration) *InProcessCartridgeHost {
	entries := make([]handlerEntry, 0, len(handlers))
	for _, h := range handlers {
		entries = append(entries, handlerEntry{
			name:    h.Name,
			caps:    h.Caps,
			handler: h.Handler,
		})
	}
	return &InProcessCartridgeHost{
		identity: identity,
		handlers: entries,
	}
}

// inProcessIdentityCap constructs the canonical identity Cap definition that the
// manifest must advertise. Mirrors the Rust standard::caps::identity_cap().
func inProcessIdentityCap() cap.Cap {
	identityUrn, err := urn.NewCapUrnFromString(standard.CapIdentity)
	if err != nil {
		panic("BUG: failed to parse CAP_IDENTITY URN: " + err.Error())
	}
	return *cap.NewCap(identityUrn, "Identity", []string{"identity"})
}

// BuildManifest builds the aggregate RelayNotify manifest payload bytes.
//
// The host has no on-disk cartridge directory, but the embedder supplies an
// InProcessHostIdentity that mirrors a real cartridge install's identity tuple.
// We assemble one InstalledCartridgeRecord from that and put every
// handler-contributed cap into its lone cap group. The wire format is symmetric
// with out-of-process hosts: the engine reads cap_groups from
// installed_cartridges and derives the flat cap list itself.
func (h *InProcessCartridgeHost) BuildManifest() []byte {
	// Collect all handler caps; prepend CAP_IDENTITY exactly once.
	caps := []cap.Cap{inProcessIdentityCap()}
	for _, entry := range h.handlers {
		for _, c := range entry.caps {
			if c.Urn.String() != standard.CapIdentity {
				caps = append(caps, c)
			}
		}
	}

	capUrnList := make([]string, 0, len(caps))
	for _, c := range caps {
		capUrnList = append(capUrnList, c.Urn.String())
	}

	pid := uint32(os.Getpid())
	cartridge := InstalledCartridgeRecord{
		RegistryURL: h.identity.RegistryURL,
		Channel:     string(h.identity.Channel),
		Id:          h.identity.Id,
		Version:     h.identity.Version,
		Sha256:      h.identity.Sha256,
		CapGroups: []CapGroup{{
			Name:        h.identity.Id,
			Caps:        caps,
			AdapterUrns: []string{},
		}},
		RuntimeStats: &CartridgeRuntimeStats{
			Running: true,
			// In-process handlers are task-backed and have no fixed
			// concurrency ceiling: every pool is unlimited, at rest at
			// manifest-build time.
			Pools:              inProcessPoolStates(capUrnList, nil),
			PID:                &pid,
			ActiveRequestCount: 0,
			PeerRequestCount:   0,
			MemoryFootprintMB:  0,
			MemoryRSSMB:        0,
			RestartCount:       0,
		},
		// In-process cartridges have no on-disk presence to inspect and no
		// registry to verify against — the embedder constructed them directly.
		// They are operational from the moment the host advertises them.
		Lifecycle: CartridgeLifecycleOperational,
	}

	payload := RelayNotifyCapabilitiesPayload{
		InstalledCartridges: []InstalledCartridgeRecord{cartridge},
	}
	out, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("BUG: InProcessCartridgeHost RelayNotify payload must serialize: %v", err))
	}
	return out
}

// buildCapTable builds the cap table for routing: flat list of (cap_urn,
// handler_idx).
func buildCapTable(handlers []handlerEntry) []inProcessCapTableEntry {
	var table []inProcessCapTableEntry
	for idx, entry := range handlers {
		for _, c := range entry.caps {
			table = append(table, inProcessCapTableEntry{capUrn: c.Urn.String(), handlerIdx: idx})
		}
	}
	return table
}

// findHandlerForCap finds the best handler for a cap URN.
//
// Uses IsDispatchable(candidate, request) to find handlers that can legally
// handle the request, then ranks by specificity.
//
// Ranking prefers:
//  1. Equivalent matches (distance 0)
//  2. More specific candidates (positive distance) - refinements
//  3. More generic candidates (negative distance) - fallbacks
//
// Returns the handler index and true, or 0 and false when no handler matches.
func findHandlerForCap(capTable []inProcessCapTableEntry, capUrn string) (int, bool) {
	requestUrn, err := urn.NewCapUrnFromString(capUrn)
	if err != nil {
		return 0, false
	}

	requestSpecificity := requestUrn.Specificity()

	var matches []capMatch

	for _, entry := range capTable {
		registeredUrn, err := urn.NewCapUrnFromString(entry.capUrn)
		if err != nil {
			continue
		}
		// Use IsDispatchable: can this candidate handle this request?
		if registeredUrn.IsDispatchable(requestUrn) {
			specificity := registeredUrn.Specificity()
			signedDistance := specificity - requestSpecificity
			matches = append(matches, capMatch{handlerIdx: entry.handlerIdx, signedDistance: signedDistance})
		}
	}

	if len(matches) == 0 {
		return 0, false
	}

	// Ranking: prefer equivalent (0), then more specific (+), then more generic (-).
	// Preserve insertion order among equal-distance candidates.
	best := matches[0]
	for _, m := range matches[1:] {
		if rankLess(m, best) {
			best = m
		}
	}
	return best.handlerIdx, true
}

// capMatch is a candidate handler match with its signed specificity distance
// relative to the request URN.
type capMatch struct {
	handlerIdx     int
	signedDistance int
}

// rankLess reports whether a should be ranked before b (mirrors the Rust
// sort_by comparator in find_handler_for_cap).
func rankLess(a, b capMatch) bool {
	aNonNeg := a.signedDistance >= 0
	bNonNeg := b.signedDistance >= 0
	// First: non-negative distances before negative.
	switch {
	case aNonNeg && !bNonNeg:
		return true
	case !aNonNeg && bNonNeg:
		return false
	default:
		// Same sign: prefer smaller absolute distance.
		return absInt(a.signedDistance) < absInt(b.signedDistance)
	}
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// inProcessPoolStates is the host's full pool-state map: one at-rest
// singleton per advertised cap plus the mandatory `all` pool. In-process
// handlers are task-backed with no fixed concurrency ceiling, so every
// capacity is 0 (unlimited) and nothing ever queues; configured carries the
// operator overlay delivered via heartbeat probes (the ENGINE's admission
// enforces it — see the Rust in-process host). (matches Rust
// pool_states_snapshot)
func inProcessPoolStates(capUrns []string, configured map[string]uint64) PoolStates {
	states := make(PoolStates, len(capUrns)+1)
	for _, capUrn := range capUrns {
		state := PoolState{}
		if c, ok := configured[capUrn]; ok {
			state.Configured = c
		}
		states[capUrn] = state
	}
	all := PoolState{Caps: append([]string(nil), capUrns...)}
	if c, ok := configured[PoolAll]; ok {
		all.Configured = c
	}
	states[PoolAll] = all
	return states
}

// Run runs the host. Returns when the local connection closes.
//
// localRead / localWrite connect to the RelaySlave's local side.
func (h *InProcessCartridgeHost) Run(localRead io.Reader, localWrite io.Writer) error {
	reader := NewFrameReader(localRead)

	// Writer runs in a separate goroutine with SeqAssigner.
	writeTx := make(chan Frame, 256)
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		writer := NewFrameWriter(localWrite)
		seqAssigner := NewSeqAssigner()
		for frame := range writeTx {
			seqAssigner.Assign(&frame)
			if err := writer.WriteFrame(&frame); err != nil {
				return
			}
			if frame.FrameType == FrameTypeEnd || frame.FrameType == FrameTypeErr {
				seqAssigner.Remove(FlowKeyFromFrame(&frame))
			}
		}
	}()

	// Send initial RelayNotify with aggregate caps.
	manifest := h.BuildManifest()
	limits := DefaultLimits()
	notify := NewRelayNotify(manifest, limits.MaxFrame, limits.MaxChunk, limits.MaxReorderBuffer, limits.InitialCredit)
	writeTx <- *notify

	capTable := buildCapTable(h.handlers)

	// Advertised caps (canonical URNs) — the singleton pool roster — and
	// the operator `configured` overlay delivered via heartbeat probes.
	advertisedCaps := []string{standard.CapIdentity}
	for _, entry := range h.handlers {
		for _, c := range entry.caps {
			capUrn := c.Urn.String()
			if !slices.Contains(advertisedCaps, capUrn) {
				advertisedCaps = append(advertisedCaps, capUrn)
			}
		}
	}
	configuredOverlay := make(DesiredCapacities)

	// Active request channels: request_id → input_tx for forwarding frames to
	// handler. Keyed by MessageId.ToString() because MessageId is not comparable.
	active := make(map[string]chan Frame)
	identity := identityHandler{}
	maxChunk := DefaultLimits().MaxChunk

	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			break
		}

		switch frame.FrameType {
		case FrameTypeReq:
			rid := frame.Id
			xid := frame.RoutingId
			if frame.Cap == nil {
				errFrame := NewErr(rid, "PROTOCOL_ERROR", AttributionClassInternal, "REQ missing cap URN", nil)
				errFrame.RoutingId = xid
				writeTx <- *errFrame
				continue
			}
			capUrn := *frame.Cap

			// Identity cap is CAP_IDENTITY / `cap:effect=none` — exact string
			// match, NOT conforms_to.
			isIdentity := capUrn == standard.CapIdentity

			var handler FrameHandler
			if isIdentity {
				handler = identity
			} else {
				idx, ok := findHandlerForCap(capTable, capUrn)
				if !ok {
					// No registered handler for a dispatched cap is a
					// deployment mismatch — Environment.
					errFrame := NewErr(rid, "NO_HANDLER", AttributionClassEnvironment, fmt.Sprintf("no handler for cap: %s", capUrn), nil)
					errFrame.RoutingId = xid
					writeTx <- *errFrame
					continue
				}
				handler = h.handlers[idx].handler
			}

			// Create channel for forwarding frames to handler.
			inputTx := make(chan Frame, 256)
			active[rid.ToString()] = inputTx

			output := newResponseWriter(rid, xid, writeTx, maxChunk)
			capUrnOwned := capUrn
			peer := PeerInvoker(&noPeerInvoker{})
			go handler.HandleRequest(capUrnOwned, inputTx, output, peer)

		case FrameTypeStreamStart, FrameTypeChunk, FrameTypeStreamEnd, FrameTypeLog:
			// Continuation frames: forward to active request.
			if tx, ok := active[frame.Id.ToString()]; ok {
				tx <- *frame
			}

		case FrameTypeEnd:
			key := frame.Id.ToString()
			if tx, ok := active[key]; ok {
				tx <- *frame
				close(tx)
				delete(active, key)
			}

		case FrameTypeErr:
			key := frame.Id.ToString()
			if tx, ok := active[key]; ok {
				tx <- *frame
				close(tx)
				delete(active, key)
			}

		case FrameTypeCloseStream:
			// An in-process handler holds no live feed taps: a CloseStream
			// has nothing to close and never aborts the request — it is
			// logged and the request continues.
			fmt.Fprintf(os.Stderr, "[InProcessHost] CloseStream for request %s with no live feed — nothing to close, request continues\n", frame.Id.ToString())

		case FrameTypeCancel:
			targetRid := frame.Id
			xid := frame.RoutingId
			key := targetRid.ToString()
			// The attribution rides in Meta like an ERR's; an unattributed
			// Cancel is still a cancel.
			reason, _ := frame.CancelReason()
			// Drop active sender → handler's input recv returns closed.
			if tx, ok := active[key]; ok {
				close(tx)
				delete(active, key)
			}
			// Terminal ERR in the cancel's own attribution.
			errFrame := NewErr(targetRid, reason.TerminalCode(), reason.TerminalClass(), reason.TerminalMessage(), nil)
			errFrame.RoutingId = xid
			writeTx <- *errFrame

		case FrameTypeHeartbeat:
			// The heartbeat is the capacity CONFIG channel (see the
			// cartridge runtime): a probe may carry the operator's desired
			// configured values. Validate the whole batch against the pool
			// roster first — an unknown pool refuses it all with an ERR
			// naming it.
			if desiredBytes := frame.DesiredCapacityBytes(); desiredBytes != nil {
				desired, decodeErr := DecodeDesired(desiredBytes)
				if decodeErr != nil {
					errFrame := NewErr(frame.Id, "UNKNOWN_POOL", AttributionClassInternal, fmt.Sprintf("desired capacities refused: %v", decodeErr), nil)
					writeTx <- *errFrame
					continue
				}
				refused := false
				for name := range desired {
					if name != PoolAll && !slices.Contains(advertisedCaps, name) {
						errFrame := NewErr(frame.Id, "UNKNOWN_POOL", AttributionClassInternal, fmt.Sprintf("desired capacities refused: unknown pool '%s'", name), nil)
						writeTx <- *errFrame
						refused = true
						break
					}
				}
				if refused {
					continue
				}
				for name, configured := range desired {
					configuredOverlay[name] = configured
				}
			}
			response := NewHeartbeat(frame.Id)
			response.Meta = map[string]interface{}{
				MetaPools: EncodePoolStates(inProcessPoolStates(advertisedCaps, configuredOverlay)),
			}
			writeTx <- *response

		default:
			// RelayNotify, RelayState, etc. — not expected from relay side.
		}
	}

	// Drop all active channels to signal handlers to exit.
	for key, tx := range active {
		close(tx)
		delete(active, key)
	}

	close(writeTx)
	writerWg.Wait()
	return nil
}
