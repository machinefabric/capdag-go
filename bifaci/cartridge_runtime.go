package bifaci

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	cborlib "github.com/fxamacker/cbor/v2"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// MediaFilePath is the canonical file-path media URN. There is a single
// file-path media URN; cardinality (single file vs many) lives on the arg
// definition's is_sequence flag, not on URN tags. Matches Rust's
// MEDIA_FILE_PATH constant.
const MediaFilePath = "media:enc=utf-8;file-path"

// StreamEmitter allows handlers to emit CBOR values and logs.
// Handlers emit CBOR values via EmitCbor() or logs via EmitLog().
// The value is CBOR-encoded once and sent as raw CBOR bytes in CHUNK frames.
// No double-encoding: one CBOR layer from handler to consumer.
type StreamEmitter interface {
	// EmitCbor emits a CBOR value as output.
	// The value is CBOR-encoded once and sent as raw CBOR bytes in CHUNK frames.
	EmitCbor(value interface{}) error
	// Write writes raw bytes as output, split into max_chunk-sized CHUNK frames.
	// Unlike EmitCbor which CBOR-encodes the value, this sends raw bytes directly.
	Write(data []byte) error
	// EmitListItem emits a single CBOR value as one item in an RFC 8742 CBOR sequence.
	// For list outputs: CBOR-encodes the value, then splits across chunk frames.
	// The receiver concatenates raw payloads to reconstruct the CBOR sequence.
	EmitListItem(value interface{}) error
	// EmitLog emits a log message at the given level.
	// Sends a LOG frame (side-channel, does not affect response stream).
	EmitLog(level, message string)
	// Progress emits a progress update (0.0-1.0) with a human-readable status message.
	Progress(progress float32, message string)
}

// PeerInvoker allows handlers to invoke caps on the peer (host).
// Spawns a goroutine that receives response frames and forwards them to a channel.
// Returns a channel that yields bare CBOR Frame objects (STREAM_START, CHUNK,
// STREAM_END, END, ERR) as they arrive from the host. The consumer processes
// frames directly - no decoding, no wrapper types.
type PeerInvoker interface {
	Invoke(capUrn string, arguments []cap.CapArgumentValue) (<-chan Frame, error)
}

// PeerResponseItem is a single item from a peer response — either decoded data or a LOG frame.
//
// PeerResponse.Recv() yields these interleaved in arrival order. Handlers
// match on each variant to decide how to react (e.g., forward progress, accumulate data).
type PeerResponseItem struct {
	// DataValue holds the decoded CBOR value (nil if this is a LOG item or error)
	DataValue interface{}
	// DataErr holds an error if this is a Data(Err) item
	DataErr error
	// LogFrame holds the LOG frame if this is a Log item (nil for Data items)
	LogFrame *Frame
	// IsDataItem is true if this is a Data item, false for Log
	IsDataItem bool
}

// PeerResponse yields both data items and LOG frames from a peer call.
//
// LOG frames are delivered in real-time as they arrive (not buffered until data starts).
// For callers that don't care about LOG frames, CollectBytes() and CollectValue()
// silently discard them and return only data.
type PeerResponse struct {
	ch <-chan PeerResponseItem
}

// Recv receives the next item (data or LOG) from the peer response.
// Returns the item and true, or zero-value and false when the stream ends.
func (pr *PeerResponse) Recv() (PeerResponseItem, bool) {
	item, ok := <-pr.ch
	return item, ok
}

// CollectBytes collects all data chunks into a single byte slice, discarding LOG frames.
func (pr *PeerResponse) CollectBytes() ([]byte, error) {
	var result []byte
	for item := range pr.ch {
		if item.LogFrame != nil {
			continue // Discard LOG frames
		}
		if item.DataErr != nil {
			return nil, item.DataErr
		}
		switch v := item.DataValue.(type) {
		case []byte:
			result = append(result, v...)
		case string:
			result = append(result, []byte(v)...)
		default:
			encoded, err := cborlib.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("failed to encode CBOR: %w", err)
			}
			result = append(result, encoded...)
		}
	}
	return result, nil
}

// CollectValue collects a single CBOR data value (expects exactly one data chunk), discarding LOG frames.
func (pr *PeerResponse) CollectValue() (interface{}, error) {
	for item := range pr.ch {
		if item.LogFrame != nil {
			continue // Discard LOG frames
		}
		if item.DataErr != nil {
			return nil, item.DataErr
		}
		return item.DataValue, nil
	}
	return nil, errors.New("peer response ended without data")
}

// DemuxPeerResponse converts a raw Frame channel into a PeerResponse that yields
// PeerResponseItems (Data or Log). Returns immediately so LOG frames can be consumed
// before data arrives (critical for keeping the engine's activity timer alive).
func DemuxPeerResponse(rawFrames <-chan Frame) *PeerResponse {
	itemCh := make(chan PeerResponseItem, 256)

	go func() {
		defer close(itemCh)
		for frame := range rawFrames {
			switch frame.FrameType {
			case FrameTypeStreamStart:
				// Structural frame — no item to deliver
			case FrameTypeChunk:
				if frame.Payload != nil {
					// Verify checksum
					if frame.Checksum == nil {
						itemCh <- PeerResponseItem{
							IsDataItem: true,
							DataErr:    errors.New("CHUNK frame missing required checksum field"),
						}
						continue
					}
					actual := ComputeChecksum(frame.Payload)
					if actual != *frame.Checksum {
						itemCh <- PeerResponseItem{
							IsDataItem: true,
							DataErr:    fmt.Errorf("checksum mismatch: expected=%d, actual=%d", *frame.Checksum, actual),
						}
						continue
					}
					var value interface{}
					if err := cborlib.Unmarshal(frame.Payload, &value); err != nil {
						itemCh <- PeerResponseItem{
							IsDataItem: true,
							DataErr:    fmt.Errorf("CBOR decode error: %w", err),
						}
					} else {
						itemCh <- PeerResponseItem{
							IsDataItem: true,
							DataValue:  value,
						}
					}
				}
			case FrameTypeLog:
				f := frame // copy
				itemCh <- PeerResponseItem{LogFrame: &f}
			case FrameTypeStreamEnd, FrameTypeEnd:
				return
			case FrameTypeErr:
				code := "UNKNOWN"
				message := "Unknown error"
				if c := frame.ErrorCode(); c != "" {
					code = c
				}
				if m := frame.ErrorMessage(); m != "" {
					message = m
				}
				itemCh <- PeerResponseItem{
					IsDataItem: true,
					DataErr:    fmt.Errorf("remote error: [%s] %s", code, message),
				}
				return
			}
		}
	}()

	return &PeerResponse{ch: itemCh}
}

// ProgressSender is a detached progress/log emitter that can be used from goroutines.
//
// Holds a *syncFrameWriter and the request routing info needed to construct LOG frames.
// Thread-safe by construction (delegates to syncFrameWriter which has a mutex).
type ProgressSender struct {
	writer    *syncFrameWriter
	requestID MessageId
	routingId *MessageId
}

// Progress emits a progress update (0.0–1.0) with a human-readable status message.
func (ps *ProgressSender) Progress(progress float32, message string) {
	frame := NewProgress(ps.requestID, progress, message)
	frame.RoutingId = ps.routingId
	ps.writer.WriteFrame(frame)
}

// Log emits a log message.
func (ps *ProgressSender) Log(level, message string) {
	frame := NewLog(ps.requestID, level, message)
	frame.RoutingId = ps.routingId
	ps.writer.WriteFrame(frame)
}

// StreamChunk removed - handlers now receive bare CBOR Frame objects directly

// HandlerFunc is the function signature for cap handlers.
// Receives bare CBOR Frame objects for both input arguments and peer responses.
// Handler has full streaming control - decides when to consume frames and when to produce output.
type HandlerFunc func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error

// CartridgeRuntime handles all I/O for cartridge binaries
type CartridgeRuntime struct {
	handlers     map[string]HandlerFunc
	manifestData []byte
	manifest     *CapManifest
	limits       Limits
	mu           sync.RWMutex
}

// NewCartridgeRuntime creates a new cartridge runtime with the required manifest JSON
func NewCartridgeRuntime(manifestJSON []byte) (*CartridgeRuntime, error) {
	// Try to parse the manifest for CLI mode support
	var manifest CapManifest
	parseErr := json.Unmarshal(manifestJSON, &manifest)

	runtime := &CartridgeRuntime{
		handlers:     make(map[string]HandlerFunc),
		manifestData: manifestJSON,
		limits:       DefaultLimits(),
	}

	if parseErr == nil {
		runtime.manifest = &manifest
	}

	return runtime, nil
}

// NewCartridgeRuntimeWithManifest creates a new cartridge runtime with a pre-built CapManifest
// IMPORTANT: Manifest MUST declare CAP_IDENTITY - fails hard if missing
func NewCartridgeRuntimeWithManifest(manifest *CapManifest) (*CartridgeRuntime, error) {
	// Validate manifest - FAIL HARD if CAP_IDENTITY not declared
	identityUrn, err := urn.NewCapUrnFromString(standard.CapIdentity)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CAP_IDENTITY URN: %w", err)
	}

	hasIdentity := false
	for _, cap := range manifest.AllCaps() {
		if identityUrn.IsEquivalent(cap.Urn) {
			hasIdentity = true
			break
		}
	}

	if !hasIdentity {
		return nil, fmt.Errorf(
			"manifest validation failed - cartridge MUST declare CAP_IDENTITY (cap:effect=none). " +
			"All cartridges must explicitly declare capabilities, no implicit fallbacks allowed",
		)
	}

	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}

	runtime := &CartridgeRuntime{
		handlers:     make(map[string]HandlerFunc),
		manifestData: manifestData,
		manifest:     manifest,
		limits:       DefaultLimits(),
	}

	// Auto-register identity handler if not already registered
	runtime.autoRegisterIdentity()

	// Auto-register adapter selection handler if not already registered
	runtime.autoRegisterAdapterSelection()

	return runtime, nil
}

// autoRegisterIdentity registers a default identity handler if none exists
func (pr *CartridgeRuntime) autoRegisterIdentity() {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	// Check if identity handler already registered
	if _, exists := pr.handlers[standard.CapIdentity]; !exists {
		// Register default identity handler (echo - returns input as-is)
		pr.handlers[standard.CapIdentity] = func(input <-chan Frame, output StreamEmitter, peer PeerInvoker) error {
			// Collect all incoming frames
			var chunks []interface{}
			for frame := range input {
				switch frame.FrameType {
				case FrameTypeChunk:
					// Verify checksum (protocol v2 integrity check)
					if err := VerifyChunkChecksum(&frame); err != nil {
						return fmt.Errorf("corrupted data: %w", err)
					}
					if frame.Payload != nil {
						// Decode each chunk as CBOR
						var value interface{}
						if err := cborlib.Unmarshal(frame.Payload, &value); err != nil {
							return err
						}
						chunks = append(chunks, value)
					}
				case FrameTypeEnd:
					goto done
				}
			}
		done:
			// Echo back - emit single value or concatenated chunks
			if len(chunks) == 0 {
				return output.EmitCbor([]byte{})
			} else if len(chunks) == 1 {
				return output.EmitCbor(chunks[0])
			} else {
				// Multiple chunks - try to concatenate if bytes/string, otherwise array
				switch chunks[0].(type) {
				case []byte:
					var result []byte
					for _, chunk := range chunks {
						if b, ok := chunk.([]byte); ok {
							result = append(result, b...)
						}
					}
					return output.EmitCbor(result)
				case string:
					var result string
					for _, chunk := range chunks {
						if s, ok := chunk.(string); ok {
							result += s
						}
					}
					return output.EmitCbor(result)
				default:
					return output.EmitCbor(chunks)
				}
			}
		}
	}
}

// autoRegisterAdapterSelection registers a default adapter-selection handler if none exists.
// The default implementation drains input and returns an empty END (no match found).
// Cartridge authors can override this by calling Register after construction.
func (pr *CartridgeRuntime) autoRegisterAdapterSelection() {
	const capAdapterSelection = `cap:in="media:";out="media:adapter-selection;fmt=json;record"`
	pr.mu.Lock()
	defer pr.mu.Unlock()

	if _, exists := pr.handlers[capAdapterSelection]; !exists {
		pr.handlers[capAdapterSelection] = func(input <-chan Frame, output StreamEmitter, peer PeerInvoker) error {
			// Drain input frames
			for range input {
			}
			// Return empty END (no adapter match)
			return nil
		}
	}
}

// Register registers a handler for a cap URN
func (pr *CartridgeRuntime) Register(capUrn string, handler HandlerFunc) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.handlers[capUrn] = handler
}

// Request bundles the handler's input frames, output emitter, and peer invoker into a
// single object. Struct-based handlers (CapHandler) receive a *Request instead of the
// three separate HandlerFunc parameters. Mirrors the Rust capdag Request type.
type Request struct {
	frames  <-chan Frame
	emitter StreamEmitter
	peer    PeerInvoker
}

// Frames returns the input frame channel. The handler owns the channel and must consume
// all frames (including the terminal END frame) before returning.
func (r *Request) Frames() <-chan Frame { return r.frames }

// Output returns the StreamEmitter for producing output CBOR values and log messages.
func (r *Request) Output() StreamEmitter { return r.emitter }

// Peer returns the PeerInvoker for calling capabilities on the host.
func (r *Request) Peer() PeerInvoker { return r.peer }

// CapOp is the interface for struct-based cartridge cap handlers. Implement Perform to handle
// a capability invocation. Mirrors the Rust Op<()> pattern: input/output/peer are accessed
// through *Request rather than as separate parameters.
type CapOp interface {
	Perform(req *Request) error
}

// RegisterOp registers a CapOp for a cap URN.
// Bridges the struct-based CapOp interface to the function-based HandlerFunc.
func (pr *CartridgeRuntime) RegisterOp(capUrn string, op CapOp) {
	pr.Register(capUrn, func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
		return op.Perform(&Request{frames: frames, emitter: emitter, peer: peer})
	})
}

// FindHandler finds a handler for a cap URN (exact match or is_dispatchable pattern match).
//
// Uses is_dispatchable(provider, request): can this registered handler dispatch
// the incoming request? Mirrors Rust exactly:
//
//	registered_urn.is_dispatchable(&request_urn)
//
// Ranks by: non-negative signed distance (refinement/exact) first,
// then by smallest absolute distance. This prevents identity handlers
// from stealing routes from specific handlers.
func (pr *CartridgeRuntime) FindHandler(capUrn string) HandlerFunc {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	// First try exact match
	if handler, ok := pr.handlers[capUrn]; ok {
		return handler
	}

	// Then try pattern matching via CapUrn
	requestUrn, err := urn.NewCapUrnFromString(capUrn)
	if err != nil {
		return nil
	}

	requestSpecificity := requestUrn.Specificity()

	type handlerMatch struct {
		handler       HandlerFunc
		signedDistance int
	}
	var matches []handlerMatch

	for pattern, handler := range pr.handlers {
		registeredUrn, err := urn.NewCapUrnFromString(pattern)
		if err != nil {
			continue
		}
		// Use is_dispatchable: can this provider handle this request?
		if registeredUrn.IsDispatchable(requestUrn) {
			specificity := registeredUrn.Specificity()
			signedDistance := specificity - requestSpecificity
			matches = append(matches, handlerMatch{handler, signedDistance})
		}
	}

	if len(matches) == 0 {
		return nil
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

	return matches[0].handler
}

// Run runs the cartridge runtime (automatic mode detection)
func (pr *CartridgeRuntime) Run() error {
	args := os.Args

	// No CLI arguments at all → Cartridge CBOR mode
	if len(args) == 1 {
		return pr.runCBORMode()
	}

	// Any CLI arguments → CLI mode
	return pr.runCLIMode(args)
}

// runCBORMode runs in Cartridge CBOR mode - binary frame protocol via stdin/stdout
func (pr *CartridgeRuntime) runCBORMode() error {
	reader := NewFrameReader(os.Stdin)
	rawWriter := NewFrameWriter(os.Stdout)

	// Perform handshake - send our manifest in the HELLO response
	// Handshake is single-threaded so raw writer is safe here
	negotiatedLimits, err := HandshakeAccept(reader, rawWriter, pr.manifestData)
	if err != nil {
		return fmt.Errorf("handshake failed: %w", err)
	}

	reader.SetLimits(negotiatedLimits)
	rawWriter.SetLimits(negotiatedLimits)

	// Wrap writer for thread-safe concurrent access from handler goroutines
	writer := newSyncFrameWriter(rawWriter)

	pr.mu.Lock()
	pr.limits = negotiatedLimits
	pr.mu.Unlock()

	// Track pending peer requests (cartridge invoking host caps)
	// Key is MessageId.ToString() because MessageId contains []byte which is not comparable
	pendingPeerRequests := &sync.Map{} // map[string]*pendingPeerRequest

	// Track incoming requests that are being chunked
	// Protocol v2: Stream tracking for incoming request streams
	type pendingStream struct {
		mediaUrn string
		chunks   [][]byte
		complete bool
	}

	type streamEntry struct {
		streamID string
		stream   *pendingStream
	}

	type pendingIncomingRequest struct {
		capUrn    string
		handler   HandlerFunc
		routingId *MessageId    // XID from the REQ frame (preserved for response routing)
		streams   []streamEntry // Ordered list of streams
		ended     bool          // True after END frame - any stream activity after is FATAL
	}
	pendingIncoming := make(map[string]*pendingIncomingRequest)
	pendingIncomingMu := &sync.Mutex{}

	// Track active handler goroutines for cleanup
	var activeHandlers sync.WaitGroup

	// Main event loop
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			if err == io.EOF {
				break // stdin closed, exit cleanly
			}
			return fmt.Errorf("failed to read frame: %w", err)
		}

		switch frame.FrameType {
		case FrameTypeReq:
			// Extract routing_id (XID) FIRST — all error paths must include it
			routingId := frame.RoutingId

			if frame.Cap == nil || *frame.Cap == "" {
				errFrame := NewErr(frame.Id, "INVALID_REQUEST", "Request missing cap URN")
				errFrame.RoutingId = routingId
				if writeErr := writer.WriteFrame(errFrame); writeErr != nil {
					fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", writeErr)
				}
				continue
			}

			capUrn := *frame.Cap
			rawPayload := frame.Payload

			// Protocol v2: REQ must have empty payload - arguments come as streams
			if len(rawPayload) > 0 {
				errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", "REQ frame must have empty payload - use STREAM_START for arguments")
				errFrame.RoutingId = routingId
				if err := writer.WriteFrame(errFrame); err != nil {
					fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write PROTOCOL_ERROR: %v\n", err)
				}
				continue
			}

			// Find handler
			handler := pr.FindHandler(capUrn)
			if handler == nil {
				errFrame := NewErr(frame.Id, "NO_HANDLER", fmt.Sprintf("No handler registered for cap: %s", capUrn))
				errFrame.RoutingId = routingId
				if writeErr := writer.WriteFrame(errFrame); writeErr != nil {
					fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", writeErr)
				}
				continue
			}

			// Start tracking this request - streams will be added via STREAM_START
			pendingIncomingMu.Lock()
			pendingIncoming[frame.Id.ToString()] = &pendingIncomingRequest{
				capUrn:    capUrn,
				handler:   handler,
				routingId: frame.RoutingId, // Preserve XID for response routing
				streams:   []streamEntry{}, // Streams added via STREAM_START
				ended:     false,
			}
			pendingIncomingMu.Unlock()
			fmt.Fprintf(os.Stderr, "[CartridgeRuntime] REQ: req_id=%s cap=%s - waiting for streams\n", frame.Id.ToString(), capUrn)
			continue // Wait for STREAM_START/CHUNK/STREAM_END/END frames

		case FrameTypeHeartbeat:
			// Respond to heartbeat immediately - never blocked by handlers
			response := NewHeartbeat(frame.Id)
			if err := writer.WriteFrame(response); err != nil {
				return fmt.Errorf("failed to write heartbeat response: %w", err)
			}

		case FrameTypeHello:
			// Unexpected HELLO after handshake - protocol error
			errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", "Unexpected HELLO after handshake")
			if err := writer.WriteFrame(errFrame); err != nil {
				return fmt.Errorf("failed to write error: %w", err)
			}

		case FrameTypeChunk:
			// Protocol v2: CHUNK must have stream_id
			if frame.StreamId == nil {
				errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", "CHUNK frame missing stream_id")
				if err := writer.WriteFrame(errFrame); err != nil {
					fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
				}
				continue
			}

			// Verify checksum (protocol v2 integrity check)
			if err := VerifyChunkChecksum(frame); err != nil {
				errFrame := NewErr(frame.Id, "CORRUPTED_DATA", err.Error())
				if err := writer.WriteFrame(errFrame); err != nil {
					fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
				}
				continue
			}

			streamID := *frame.StreamId

			// Check if this is a chunk for an incoming request
			pendingIncomingMu.Lock()
			if pendingReq, exists := pendingIncoming[frame.Id.ToString()]; exists {
				// FAIL HARD: Request already ended
				if pendingReq.ended {
					delete(pendingIncoming, frame.Id.ToString())
					pendingIncomingMu.Unlock()
					errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", "CHUNK after request END")
					if err := writer.WriteFrame(errFrame); err != nil {
						fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
					}
					continue
				}

				// FAIL HARD: Unknown or inactive stream
				var foundStream *pendingStream
				for i := range pendingReq.streams {
					if pendingReq.streams[i].streamID == streamID {
						foundStream = pendingReq.streams[i].stream
						break
					}
				}

				if foundStream == nil {
					delete(pendingIncoming, frame.Id.ToString())
					pendingIncomingMu.Unlock()
					errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", fmt.Sprintf("CHUNK for unknown stream_id: %s", streamID))
					if err := writer.WriteFrame(errFrame); err != nil {
						fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
					}
					continue
				}

				if foundStream.complete {
					delete(pendingIncoming, frame.Id.ToString())
					pendingIncomingMu.Unlock()
					errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", fmt.Sprintf("CHUNK for ended stream: %s", streamID))
					if err := writer.WriteFrame(errFrame); err != nil {
						fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
					}
					continue
				}

				// ✅ Valid chunk for active stream
				if frame.Payload != nil {
					foundStream.chunks = append(foundStream.chunks, frame.Payload)
				}
				pendingIncomingMu.Unlock()
				continue // Wait for more chunks or STREAM_END
			}
			pendingIncomingMu.Unlock()

			// Not an incoming request chunk - must be a peer response chunk
			// Forward bare Frame object to handler - no wrapping, no decoding
			idKey := frame.Id.ToString()
			if pending, ok := pendingPeerRequests.Load(idKey); ok {
				pendingReq := pending.(*pendingPeerRequest)
				pendingReq.sender <- *frame
			}

		case FrameTypeEnd:
			// Protocol v2: END frame marks the end of all streams for this request
			pendingIncomingMu.Lock()
			pendingReq, exists := pendingIncoming[frame.Id.ToString()]
			if exists {
				pendingReq.ended = true
				delete(pendingIncoming, frame.Id.ToString())
			}
			pendingIncomingMu.Unlock()

			if exists {
				// Build frame channel with all incoming frames in order
				// Protocol v2: Send STREAM_START → CHUNK(s) → STREAM_END for each stream, then END
				requestID := frame.Id
				handler := pendingReq.handler
				capUrn := pendingReq.capUrn

				// Create buffered channel for input frames
				framesChan := make(chan Frame, 64)

				activeHandlers.Add(1)
				go func() {
					defer activeHandlers.Done()
					defer close(framesChan)

					// Generate unique stream ID for response
					streamID := fmt.Sprintf("resp-%s", requestID.ToString()[:8])
					mediaUrn := "media:" // Default output media URN

					// Create emitter with stream multiplexing (preserve routing_id for response routing)
					emitter := newThreadSafeEmitter(writer, requestID, pendingReq.routingId, streamID, mediaUrn, negotiatedLimits.MaxChunk)
					peerInvoker := newPeerInvokerImpl(writer, pendingPeerRequests, negotiatedLimits.MaxChunk)

					fmt.Fprintf(os.Stderr, "[CartridgeRuntime] END: Invoking handler for cap=%s with %d streams\n", capUrn, len(pendingReq.streams))

					// Send all frames to channel: STREAM_START → CHUNK(s) → STREAM_END per stream, then END
					go func() {
						for _, entry := range pendingReq.streams {
							// STREAM_START
							startFrame := NewStreamStart(requestID, entry.streamID, entry.stream.mediaUrn, nil)
							framesChan <- *startFrame

							// CHUNKs
							for seq, chunk := range entry.stream.chunks {
								checksum := ComputeChecksum(chunk)
								chunkFrame := NewChunk(requestID, entry.streamID, uint64(seq), chunk, uint64(seq), checksum)
								framesChan <- *chunkFrame
							}

							// STREAM_END
							endStreamFrame := NewStreamEnd(requestID, entry.streamID, uint64(len(entry.stream.chunks)))
							framesChan <- *endStreamFrame
						}

						// END frame
						framesChan <- *frame
					}()

					// Invoke handler with frame channel
					err := handler(framesChan, emitter, peerInvoker)
					if err != nil {
						errFrame := NewErr(requestID, "HANDLER_ERROR", err.Error())
						errFrame.RoutingId = pendingReq.routingId
						if writeErr := writer.WriteFrame(errFrame); writeErr != nil {
							fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", writeErr)
						}
						return
					}

					// Finalize sends STREAM_END + END frames
					emitter.Finalize()
				}()

				continue
			}

			// Not an incoming request end - must be a peer response end
			// Closing the channel signals completion to the handler
			idKey := frame.Id.ToString()
			if pending, ok := pendingPeerRequests.LoadAndDelete(idKey); ok {
				pendingReq := pending.(*pendingPeerRequest)
				close(pendingReq.sender)
			}

		// RES frame REMOVED - old protocol no longer supported
		// Peer invoke responses now use stream multiplexing (handled by END case above)

		case FrameTypeErr:
			// Error frame from host - could be response to peer request
			// Forward bare ERR frame to handler - handler extracts error details
			idKey := frame.Id.ToString()
			if pending, ok := pendingPeerRequests.LoadAndDelete(idKey); ok {
				pendingReq := pending.(*pendingPeerRequest)
				pendingReq.sender <- *frame
				close(pendingReq.sender)
			}

		case FrameTypeLog:
			// Route LOG frames to peer response channels.
			// During peer calls, the peer sends LOG frames (progress, status)
			// that the handler needs to receive in real-time for activity
			// timeout prevention and progress forwarding.
			idKey := frame.Id.ToString()
			if pending, ok := pendingPeerRequests.Load(idKey); ok {
				pendingReq := pending.(*pendingPeerRequest)
				pendingReq.sender <- *frame
			}

		case FrameTypeStreamStart:
			// Protocol v2: A new stream is starting for a request
			if frame.StreamId == nil {
				errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", "STREAM_START missing stream_id")
				if err := writer.WriteFrame(errFrame); err != nil {
					fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
				}
				continue
			}

			if frame.MediaUrn == nil {
				errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", "STREAM_START missing media_urn")
				if err := writer.WriteFrame(errFrame); err != nil {
					fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
				}
				continue
			}

			streamID := *frame.StreamId
			mediaUrn := *frame.MediaUrn

			fmt.Fprintf(os.Stderr, "[CartridgeRuntime] STREAM_START: req_id=%s stream_id=%s media_urn=%s\n",
				frame.Id.ToString(), streamID, mediaUrn)

			// STRICT: Add stream with validation
			pendingIncomingMu.Lock()
			if pendingReq, exists := pendingIncoming[frame.Id.ToString()]; exists {
				// FAIL HARD: Request already ended
				if pendingReq.ended {
					delete(pendingIncoming, frame.Id.ToString())
					pendingIncomingMu.Unlock()
					errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", "STREAM_START after request END")
					if err := writer.WriteFrame(errFrame); err != nil {
						fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
					}
					continue
				}

				// FAIL HARD: Duplicate stream_id
				for _, entry := range pendingReq.streams {
					if entry.streamID == streamID {
						delete(pendingIncoming, frame.Id.ToString())
						pendingIncomingMu.Unlock()
						errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", fmt.Sprintf("Duplicate stream_id: %s", streamID))
						if err := writer.WriteFrame(errFrame); err != nil {
							fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
						}
						continue
					}
				}

				// ✅ Add new stream
				pendingReq.streams = append(pendingReq.streams, streamEntry{
					streamID: streamID,
					stream: &pendingStream{
						mediaUrn: mediaUrn,
						chunks:   [][]byte{},
						complete: false,
					},
				})
				pendingIncomingMu.Unlock()
				fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Incoming stream started: %s\n", streamID)
				continue
			}
			pendingIncomingMu.Unlock()

			// Not an incoming request — check if it's a peer response stream
			idKey := frame.Id.ToString()
			if pending, ok := pendingPeerRequests.Load(idKey); ok {
				pendingReq := pending.(*pendingPeerRequest)
				pendingReq.streams[streamID] = mediaUrn
				// Forward bare STREAM_START frame to handler
				pendingReq.sender <- *frame
			} else {
				fmt.Fprintf(os.Stderr, "[CartridgeRuntime] STREAM_START for unknown request_id: %s\n", frame.Id.ToString())
			}

		case FrameTypeStreamEnd:
			// Protocol v2: A stream has ended for a request
			if frame.StreamId == nil {
				errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", "STREAM_END missing stream_id")
				if err := writer.WriteFrame(errFrame); err != nil {
					fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
				}
				continue
			}

			streamID := *frame.StreamId
			fmt.Fprintf(os.Stderr, "[CartridgeRuntime] STREAM_END: stream_id=%s\n", streamID)

			// STRICT: Mark stream as complete with validation
			pendingIncomingMu.Lock()
			if pendingReq, exists := pendingIncoming[frame.Id.ToString()]; exists {
				// Find and mark stream as complete
				found := false
				for i := range pendingReq.streams {
					if pendingReq.streams[i].streamID == streamID {
						pendingReq.streams[i].stream.complete = true
						found = true
						fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Incoming stream marked complete: %s\n", streamID)
						break
					}
				}

				if !found {
					// FAIL HARD: STREAM_END for unknown stream
					delete(pendingIncoming, frame.Id.ToString())
					pendingIncomingMu.Unlock()
					errFrame := NewErr(frame.Id, "PROTOCOL_ERROR", fmt.Sprintf("STREAM_END for unknown stream_id: %s", streamID))
					if err := writer.WriteFrame(errFrame); err != nil {
						fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write error: %v\n", err)
					}
					continue
				}
				pendingIncomingMu.Unlock()
				continue
			}
			pendingIncomingMu.Unlock()

			// Not an incoming request stream — check if it's a peer response stream end
			idKey := frame.Id.ToString()
			if pending, ok := pendingPeerRequests.Load(idKey); ok {
				pendingReq := pending.(*pendingPeerRequest)
				// Forward bare STREAM_END frame to handler
				pendingReq.sender <- *frame
			} else {
				fmt.Fprintf(os.Stderr, "[CartridgeRuntime] STREAM_END for unknown request_id: %s\n", frame.Id.ToString())
			}

		case FrameTypeRelayNotify, FrameTypeRelayState:
			// Relay-level frames must never reach a cartridge runtime.
			// If they do, it's a bug in the relay layer — fail hard.
			return fmt.Errorf("relay frame %v must not reach cartridge runtime", frame.FrameType)
		}
	}

	// Wait for all active handlers to complete before exiting
	activeHandlers.Wait()

	return nil
}

// runCLIMode runs in CLI mode - parse arguments and invoke handler
func (pr *CartridgeRuntime) runCLIMode(args []string) error {
	if pr.manifest == nil {
		return errors.New("failed to parse manifest for CLI mode")
	}

	// Handle --help at top level
	if len(args) == 2 && (args[1] == "--help" || args[1] == "-h") {
		pr.printHelp()
		return nil
	}

	subcommand := args[1]

	// Handle manifest subcommand (always provided by runtime)
	if subcommand == "manifest" {
		prettyJSON, err := json.MarshalIndent(pr.manifest, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal manifest: %w", err)
		}
		fmt.Println(string(prettyJSON))
		return nil
	}

	// Handle subcommand --help
	if len(args) == 3 && (args[2] == "--help" || args[2] == "-h") {
		if cap := pr.findCapByCommand(subcommand); cap != nil {
			pr.printCapHelp(cap)
			return nil
		}
	}

	// Find cap by command name
	cap := pr.findCapByCommand(subcommand)
	if cap == nil {
		return fmt.Errorf("unknown subcommand '%s'. Run with --help to see available commands", subcommand)
	}

	// Find handler
	handler := pr.FindHandler(cap.UrnString())
	if handler == nil {
		return fmt.Errorf("no handler registered for cap '%s'", cap.UrnString())
	}

	// Build raw CBOR arguments payload (file-path values still raw strings).
	rawPayload, err := pr.buildPayloadFromCLI(cap, args[2:])
	if err != nil {
		return fmt.Errorf("failed to build payload: %w", err)
	}

	// CLI-mode foreach iteration. If any file-path arg with is_sequence=false
	// resolved to multiple files, this returns one per-iteration payload per
	// resolved file. Otherwise it returns the single original payload.
	iterations, err := buildCliForeachIterations(rawPayload, cap)
	if err != nil {
		return err
	}
	for _, perIter := range iterations {
		payload, err := extractEffectivePayload(perIter, "application/cbor", cap, true)
		if err != nil {
			return err
		}
		if err := pr.dispatchCliPayload(cap, handler, payload); err != nil {
			return err
		}
	}
	return nil
}

// dispatchCliPayload delivers one CLI-mode invocation: takes the (already
// file-path-resolved) CBOR arguments payload, builds simulated input frames,
// and runs the handler to completion.
//
// Mirrors capdag/src/bifaci/cartridge_runtime.rs::dispatch_cli_payload.
func (pr *CartridgeRuntime) dispatchCliPayload(capDef *cap.Cap, handler HandlerFunc, payload []byte) error {
	framesChan := make(chan Frame, 32)
	requestID := NewMessageIdDefault()

	go func() {
		defer close(framesChan)

		var arguments []interface{}
		if len(payload) > 0 {
			if err := cborlib.Unmarshal(payload, &arguments); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode CBOR arguments: %v\n", err)
				return
			}
		}

		for i, arg := range arguments {
			argMap, ok := arg.(map[interface{}]interface{})
			if !ok {
				continue
			}

			var mediaUrn string
			var value interface{}
			for k, v := range argMap {
				key, ok := k.(string)
				if !ok {
					continue
				}
				if key == "media_urn" {
					if s, ok := v.(string); ok {
						mediaUrn = s
					}
				} else if key == "value" {
					value = v
				}
			}
			if mediaUrn == "" || value == nil {
				continue
			}

			streamID := fmt.Sprintf("arg-%d", i)

			startFrame := NewStreamStart(requestID, streamID, mediaUrn, nil)
			framesChan <- *startFrame

			cborValue, err := cborlib.Marshal(value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to encode argument value: %v\n", err)
				continue
			}
			checksum := ComputeChecksum(cborValue)
			chunkFrame := NewChunk(requestID, streamID, 0, cborValue, 0, checksum)
			framesChan <- *chunkFrame

			endStreamFrame := NewStreamEnd(requestID, streamID, 1)
			framesChan <- *endStreamFrame
		}

		endFrame := NewEnd(requestID, nil)
		framesChan <- *endFrame
	}()

	emitter := &cliStreamEmitter{}
	peer := &noPeerInvoker{}

	if err := handler(framesChan, emitter, peer); err != nil {
		errorJSON, _ := json.Marshal(map[string]string{
			"error": err.Error(),
			"code":  "HANDLER_ERROR",
		})
		fmt.Fprintln(os.Stderr, string(errorJSON))
		return err
	}
	return nil
}

// findCapByCommand finds a cap by its command name
func (pr *CartridgeRuntime) findCapByCommand(commandName string) *cap.Cap {
	if pr.manifest == nil {
		return nil
	}
	for i := range pr.manifest.CapGroups {
		for j := range pr.manifest.CapGroups[i].Caps {
			if pr.manifest.CapGroups[i].Caps[j].Command == commandName {
				return &pr.manifest.CapGroups[i].Caps[j]
			}
		}
	}
	return nil
}

// printHelp prints help message showing all available subcommands
func (pr *CartridgeRuntime) printHelp() {
	if pr.manifest == nil {
		return
	}

	fmt.Fprintf(os.Stderr, "%s v%s\n", pr.manifest.Name, pr.manifest.Version)
	fmt.Fprintf(os.Stderr, "%s\n\n", pr.manifest.Description)
	fmt.Fprintf(os.Stderr, "USAGE:\n")
	fmt.Fprintf(os.Stderr, "    %s <COMMAND> [OPTIONS]\n\n", pr.manifest.Name)
	fmt.Fprintf(os.Stderr, "COMMANDS:\n")
	fmt.Fprintf(os.Stderr, "    manifest    Output the cartridge manifest as JSON\n")

	for _, cap := range pr.manifest.AllCaps() {
		desc := cap.Title
		if cap.CapDescription != nil {
			desc = *cap.CapDescription
		}
		fmt.Fprintf(os.Stderr, "    %-12s %s\n", cap.Command, desc)
	}

	fmt.Fprintf(os.Stderr, "\nRun '%s <COMMAND> --help' for more information on a command.\n", pr.manifest.Name)
}

// printCapHelp prints help for a specific cap
func (pr *CartridgeRuntime) printCapHelp(capDef *cap.Cap) {
	fmt.Fprintf(os.Stderr, "%s\n", capDef.Title)
	if capDef.CapDescription != nil {
		fmt.Fprintf(os.Stderr, "%s\n", *capDef.CapDescription)
	}
	fmt.Fprintf(os.Stderr, "\nUSAGE:\n")
	fmt.Fprintf(os.Stderr, "    cartridge %s [OPTIONS]\n\n", capDef.Command)
}

// extractEffectivePayload extracts the effective payload from a REQ frame.
//
// Mirrors capdag/src/bifaci/cartridge_runtime.rs::extract_effective_payload.
//
// When content_type is "application/cbor", decodes the CBOR arguments,
// performs file-path auto-conversion (reading file bytes and relabeling
// the arg's media_urn to the stdin source's target URN), validates that
// at least one argument matches the cap's declared in= spec (unless the
// cap takes media:void), and returns the re-serialized CBOR array.
func extractEffectivePayload(payload []byte, contentType string, capDef *cap.Cap, isCliMode bool) ([]byte, error) {
	// Not CBOR arguments - return raw payload
	if contentType != "application/cbor" {
		return payload, nil
	}

	// Parse cap URN to get expected input media URN
	capUrnParsed, err := urn.NewCapUrnFromString(capDef.UrnString())
	if err != nil {
		return nil, fmt.Errorf("Invalid cap URN: %w", err)
	}
	expectedInSpec := capUrnParsed.InSpec()
	var expectedMediaUrn *urn.MediaUrn
	if parsed, parseErr := urn.NewMediaUrnFromString(expectedInSpec); parseErr == nil {
		expectedMediaUrn = parsed
	}

	// Build an arg-definition lookup: parsed MediaUrn → (stdin target URN,
	// is_sequence flag). File-path conversion consults this to decide whether
	// to emit a single file's bytes or a sequence of files, and what URN to
	// relabel the stream with so downstream handlers see the target media
	// type rather than the raw `media:file-path` input.
	type argDefInfo struct {
		stdinTarget *string
		isSequence  bool
	}
	type argDefEntry struct {
		urn  *urn.MediaUrn
		info argDefInfo
	}
	var argDefs []argDefEntry
	for _, a := range capDef.GetArgs() {
		parsed, perr := urn.NewMediaUrnFromString(a.MediaUrn)
		if perr != nil {
			continue
		}
		var stdinTarget *string
		for i := range a.Sources {
			if a.Sources[i].Stdin != nil {
				s := *a.Sources[i].Stdin
				stdinTarget = &s
				break
			}
		}
		argDefs = append(argDefs, argDefEntry{
			urn: parsed,
			info: argDefInfo{
				stdinTarget: stdinTarget,
				isSequence:  a.IsSequence,
			},
		})
	}

	// Parse the CBOR payload as an array of argument maps
	var arguments []interface{}
	if err := cborlib.Unmarshal(payload, &arguments); err != nil {
		return nil, fmt.Errorf("Failed to parse CBOR arguments: %w", err)
	}

	// File-path auto-conversion.
	//
	// When an arg's media URN is a specialization of `media:file-path`, the
	// incoming value is treated as one or more filesystem paths (literal or
	// glob) that the runtime reads and turns into file-bytes.
	//
	// Cardinality is driven exclusively by the arg definition's `is_sequence`
	// flag — URN tags carry semantic shape only.
	//
	// - is_sequence = true  → emit a CBOR Array of file bytes.
	// - is_sequence = false → expand to exactly one file and emit a single
	//   CBOR Bytes. More than one resolved file is a configuration error
	//   at this layer — CLI-mode dispatch is responsible for iterating the
	//   handler when it detects a glob-to-many against a scalar arg.
	filePathBase, err := urn.NewMediaUrnFromString("media:file-path")
	if err != nil {
		return nil, fmt.Errorf("Invalid file-path base pattern: %w", err)
	}

	for argIdx, arg := range arguments {
		argMap, ok := arg.(map[interface{}]interface{})
		if !ok {
			continue
		}
		var urnStr string
		var value interface{}
		hasUrn := false
		hasValue := false
		for k, v := range argMap {
			key, ok := k.(string)
			if !ok {
				continue
			}
			switch key {
			case "media_urn":
				if s, ok := v.(string); ok {
					urnStr = s
					hasUrn = true
				}
			case "value":
				value = v
				hasValue = true
			}
		}
		if !hasUrn || !hasValue {
			continue
		}

		argUrn, parseErr := urn.NewMediaUrnFromString(urnStr)
		if parseErr != nil {
			return nil, fmt.Errorf("Invalid argument media URN '%s': %w", urnStr, parseErr)
		}

		if !filePathBase.Accepts(argUrn) {
			continue
		}

		// Look up the cap's arg definition by URN equivalence (NOT string
		// compare) — the arg we received may carry the same tags in a
		// different textual order.
		var matchedInfo *argDefInfo
		for i := range argDefs {
			if argDefs[i].urn.IsEquivalent(argUrn) {
				matchedInfo = &argDefs[i].info
				break
			}
		}
		if matchedInfo == nil {
			// File-path arg with no matching definition: leave it alone.
			continue
		}

		// Args without a stdin source pass the path bytes through verbatim
		// — the handler reads them itself (rare but legal).
		if matchedInfo.stdinTarget == nil {
			continue
		}
		stdinTarget := *matchedInfo.stdinTarget

		paths, err := expandFilePathValue(value, urnStr, isCliMode)
		if err != nil {
			return nil, err
		}

		if !matchedInfo.isSequence {
			if len(paths) != 1 {
				return nil, fmt.Errorf(
					"File-path arg '%s' declared is_sequence=false resolved to %d files; "+
						"expected exactly 1. CLI-mode dispatch should have iterated the "+
						"handler across the expanded files before calling the runtime.",
					urnStr, len(paths))
			}
			fileBytes, err := os.ReadFile(paths[0])
			if err != nil {
				return nil, fmt.Errorf("Failed to read file '%s': %w", paths[0], err)
			}
			replaceArgValue(argMap, fileBytes, stdinTarget)
		} else {
			items := make([]interface{}, 0, len(paths))
			for _, p := range paths {
				fileBytes, err := os.ReadFile(p)
				if err != nil {
					return nil, fmt.Errorf("Failed to read file '%s': %w", p, err)
				}
				items = append(items, fileBytes)
			}
			replaceArgValue(argMap, items, stdinTarget)
		}

		_ = argIdx
	}

	// Validate: at least ONE argument must match the cap's declared in=spec,
	// unless the cap takes no input (in=media:void). After file-path
	// auto-conversion, an arg's media_urn may have been relabeled to the
	// arg-def's stdin-source target rather than the original
	// `media:file-path;...`, so we also accept any stdin-source target URN
	// as a valid match.
	voidUrn, err := urn.NewMediaUrnFromString("media:void")
	if err != nil {
		return nil, fmt.Errorf("Invalid void URN literal: %w", err)
	}
	isVoidInput := expectedMediaUrn != nil && expectedMediaUrn.IsEquivalent(voidUrn)

	if !isVoidInput {
		// Collect all valid target URNs: in_spec + every arg-def's stdin
		// source target.
		var validTargets []*urn.MediaUrn
		if expectedMediaUrn != nil {
			validTargets = append(validTargets, expectedMediaUrn)
		}
		for _, ad := range argDefs {
			if ad.info.stdinTarget != nil {
				if t, perr := urn.NewMediaUrnFromString(*ad.info.stdinTarget); perr == nil {
					validTargets = append(validTargets, t)
				}
			}
		}

		foundMatchingArg := false
		for _, arg := range arguments {
			argMap, ok := arg.(map[interface{}]interface{})
			if !ok {
				continue
			}
			for k, v := range argMap {
				key, ok := k.(string)
				if !ok || key != "media_urn" {
					continue
				}
				urnStr, ok := v.(string)
				if !ok {
					continue
				}
				argUrn, perr := urn.NewMediaUrnFromString(urnStr)
				if perr != nil {
					continue
				}
				for _, target := range validTargets {
					// Use is_comparable for discovery: are they on the same chain?
					if argUrn.IsComparable(target) {
						foundMatchingArg = true
						break
					}
				}
				if foundMatchingArg {
					break
				}
			}
			if foundMatchingArg {
				break
			}
		}

		if !foundMatchingArg {
			return nil, fmt.Errorf(
				"No argument found matching expected input media type '%s' in CBOR arguments",
				expectedInSpec)
		}
	}

	// After file-path conversion and validation, return the full CBOR array.
	// Handler will parse it and extract arguments by matching against in_spec.
	serialized, err := cborlib.Marshal(arguments)
	if err != nil {
		return nil, fmt.Errorf("Failed to serialize modified CBOR: %w", err)
	}
	return serialized, nil
}

// replaceArgValue replaces an argument map's "value" and "media_urn" entries
// in place. Used by extractEffectivePayload after reading file bytes so the
// downstream handler sees the post-conversion URN, not the original
// `media:file-path`. Mirrors Rust replace_arg_value.
func replaceArgValue(argMap map[interface{}]interface{}, newValue interface{}, newUrn string) {
	argMap["value"] = newValue
	argMap["media_urn"] = newUrn
}

// expandFilePathValue expands a file-path arg value into a concrete list of
// filesystem paths.
//
// Mirrors capdag/src/bifaci/cartridge_runtime.rs::expand_file_path_value.
//
// The incoming value may be:
//   - bytes/string containing a single path or a single glob pattern
//   - array of bytes/strings, each a path or a glob (CBOR mode only)
//
// Globs (detected via `*`, `?`, or `[`) are expanded and the results filtered
// to regular files. Literal paths must exist and point at a regular file.
// Returns at least one path on success; empty matches fail hard so the caller
// never has to guard against a silently-empty list.
func expandFilePathValue(value interface{}, urnStr string, isCliMode bool) ([]string, error) {
	var rawPaths []string
	switch v := value.(type) {
	case []byte:
		rawPaths = []string{string(v)}
	case string:
		rawPaths = []string{v}
	case []interface{}:
		if isCliMode {
			return nil, fmt.Errorf(
				"File-path arg '%s' received a CBOR Array value in CLI mode; CLI "+
					"dispatch must expand globs before calling into the runtime",
				urnStr)
		}
		rawPaths = make([]string, 0, len(v))
		for _, item := range v {
			switch s := item.(type) {
			case string:
				rawPaths = append(rawPaths, s)
			case []byte:
				rawPaths = append(rawPaths, string(s))
			default:
				return nil, fmt.Errorf(
					"File-path arg '%s' array contained an unsupported CBOR item: %T",
					urnStr, item)
			}
		}
	default:
		return nil, fmt.Errorf(
			"File-path arg '%s' value must be Bytes, Text, or (CBOR mode) Array — got %T",
			urnStr, value)
	}

	var resolved []string
	for _, raw := range rawPaths {
		isGlob := containsAny(raw, "*?[")
		if isGlob {
			matches, err := filepath.Glob(raw)
			if err != nil {
				return nil, fmt.Errorf("Invalid glob pattern '%s': %w", raw, err)
			}
			before := len(resolved)
			for _, p := range matches {
				info, err := os.Stat(p)
				if err != nil {
					continue
				}
				if info.Mode().IsRegular() {
					resolved = append(resolved, p)
				}
			}
			if len(resolved) == before {
				return nil, fmt.Errorf("No files matched glob pattern '%s'", raw)
			}
		} else {
			info, err := os.Stat(raw)
			if err != nil {
				if os.IsNotExist(err) {
					return nil, fmt.Errorf("File not found: '%s'", raw)
				}
				return nil, fmt.Errorf("Failed to stat '%s': %w", raw, err)
			}
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("Path is not a regular file: '%s'", raw)
			}
			resolved = append(resolved, raw)
		}
	}

	return resolved, nil
}

// buildCliForeachIterations computes the per-iteration CBOR argument payloads
// for a CLI invocation.
//
// Mirrors capdag/src/bifaci/cartridge_runtime.rs::build_cli_foreach_iterations.
//
// The input is the raw payload produced by buildPayloadFromCLI — a CBOR array
// of {media_urn, value} maps where file-path values are still raw path or glob
// strings.
//
// Rules:
//   - An arg whose media URN specializes `media:file-path` is iterable iff its
//     arg-definition declares is_sequence = false AND its raw value expands to
//     more than one concrete file.
//   - Zero iterable args → return the payload unchanged (single iteration).
//   - One iterable arg → return one payload per expanded file, each with the
//     iterable arg's value replaced by that single path as a string value.
//     extractEffectivePayload then reads the single file and emits bytes.
//   - Two or more iterable args → hard error: the ForEach axis is ambiguous
//     and there is no user-specified policy for a cartesian product.
func buildCliForeachIterations(rawPayload []byte, capDef *cap.Cap) ([][]byte, error) {
	filePathBase, err := urn.NewMediaUrnFromString("media:file-path")
	if err != nil {
		return nil, fmt.Errorf("Invalid file-path base pattern: %w", err)
	}

	var arguments []interface{}
	if err := cborlib.Unmarshal(rawPayload, &arguments); err != nil {
		return nil, fmt.Errorf("Failed to parse CBOR arguments: %w", err)
	}

	type argDefEntry struct {
		urn        *urn.MediaUrn
		isSequence bool
	}
	var argDefs []argDefEntry
	for _, a := range capDef.GetArgs() {
		parsed, perr := urn.NewMediaUrnFromString(a.MediaUrn)
		if perr != nil {
			continue
		}
		argDefs = append(argDefs, argDefEntry{urn: parsed, isSequence: a.IsSequence})
	}

	type iterableEntry struct {
		idx   int
		paths []string
	}
	var iterable *iterableEntry

	for idx, arg := range arguments {
		argMap, ok := arg.(map[interface{}]interface{})
		if !ok {
			continue
		}
		var urnStr string
		var value interface{}
		hasUrn, hasValue := false, false
		for k, v := range argMap {
			key, ok := k.(string)
			if !ok {
				continue
			}
			switch key {
			case "media_urn":
				if s, ok := v.(string); ok {
					urnStr = s
					hasUrn = true
				}
			case "value":
				value = v
				hasValue = true
			}
		}
		if !hasUrn || !hasValue {
			continue
		}

		argUrn, parseErr := urn.NewMediaUrnFromString(urnStr)
		if parseErr != nil {
			return nil, fmt.Errorf("Invalid argument media URN '%s': %w", urnStr, parseErr)
		}
		if !filePathBase.Accepts(argUrn) {
			continue
		}

		isSequenceArg := false
		for _, ad := range argDefs {
			if ad.urn.IsEquivalent(argUrn) {
				isSequenceArg = ad.isSequence
				break
			}
		}
		if isSequenceArg {
			// Sequence args take multiple files as-is; no ForEach iteration.
			continue
		}

		paths, err := expandFilePathValue(value, urnStr, true)
		if err != nil {
			return nil, err
		}
		if len(paths) <= 1 {
			continue
		}

		if iterable != nil {
			return nil, fmt.Errorf(
				"Multiple file-path arguments with is_sequence=false each resolved " +
					"to more than one file; the ForEach axis is ambiguous. Declare at " +
					"most one such arg as scalar, or mark additional args as " +
					"is_sequence=true.")
		}
		iterable = &iterableEntry{idx: idx, paths: paths}
	}

	if iterable == nil {
		return [][]byte{rawPayload}, nil
	}

	out := make([][]byte, 0, len(iterable.paths))
	for _, path := range iterable.paths {
		// Deep-clone the arguments slice and replace value at idx
		argsForIter := make([]interface{}, len(arguments))
		for i, a := range arguments {
			if i == iterable.idx {
				if origMap, ok := a.(map[interface{}]interface{}); ok {
					newMap := make(map[interface{}]interface{}, len(origMap))
					for k, v := range origMap {
						newMap[k] = v
					}
					newMap["value"] = path
					argsForIter[i] = newMap
					continue
				}
			}
			argsForIter[i] = a
		}
		buf, err := cborlib.Marshal(argsForIter)
		if err != nil {
			return nil, fmt.Errorf("Failed to re-encode iter payload: %w", err)
		}
		out = append(out, buf)
	}

	return out, nil
}

// toBytes converts a CBOR-decoded value to []byte
func toBytes(v interface{}) []byte {
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	default:
		// Try JSON encoding as fallback
		data, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		return data
	}
}

// syncFrameWriter wraps FrameWriter with a mutex for concurrent access and
// centralized seq assignment. All frames pass through the SeqAssigner before
// writing, ensuring monotonically increasing seq per flow (RID + XID).
// (matches Rust CartridgeRuntime writer thread with SeqAssigner)
type syncFrameWriter struct {
	mu          sync.Mutex
	writer      *FrameWriter
	seqAssigner *SeqAssigner
}

func newSyncFrameWriter(w *FrameWriter) *syncFrameWriter {
	return &syncFrameWriter{
		writer:      w,
		seqAssigner: NewSeqAssigner(),
	}
}

func (s *syncFrameWriter) WriteFrame(frame *Frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Centralized seq assignment — all flow frames get monotonic seq per flow
	s.seqAssigner.Assign(frame)
	err := s.writer.WriteFrame(frame)
	// Clean up flow tracking after terminal frames
	if err == nil && (frame.FrameType == FrameTypeEnd || frame.FrameType == FrameTypeErr) {
		key := FlowKeyFromFrame(frame)
		s.seqAssigner.Remove(key)
	}
	return err
}

func (s *syncFrameWriter) SetLimits(limits Limits) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer.SetLimits(limits)
}

// threadSafeEmitter implements StreamEmitter with thread-safe writes using stream multiplexing
type threadSafeEmitter struct {
	writer        *syncFrameWriter
	requestID     MessageId
	routingId     *MessageId // XID from incoming request (preserved for response routing)
	streamID      string     // Response stream ID
	mediaUrn      string     // Response media URN
	streamStarted bool       // Track if STREAM_START was sent
	seq           uint64
	chunkIndex    uint64 // Track chunk index (required by protocol)
	seqMu         sync.Mutex
	maxChunk      int
}

func newThreadSafeEmitter(writer *syncFrameWriter, requestID MessageId, routingId *MessageId, streamID string, mediaUrn string, maxChunk int) *threadSafeEmitter {
	return &threadSafeEmitter{
		writer:        writer,
		requestID:     requestID,
		routingId:     routingId,
		streamID:      streamID,
		mediaUrn:      mediaUrn,
		streamStarted: false,
		maxChunk:      maxChunk,
	}
}

func (e *threadSafeEmitter) EmitCbor(value interface{}) error {
	e.seqMu.Lock()
	defer e.seqMu.Unlock()

	// CHUNK payloads = complete, independently decodable CBOR values
	//
	// Streams might never end (logs, video, real-time data), so each CHUNK must be
	// processable immediately without waiting for END frame.
	//
	// For []byte/string: split raw data, encode each chunk as complete value
	// For other types: encode once (typically small)
	//
	// Each CHUNK payload can be decoded independently: cbor2.loads(chunk.payload)

	// STREAM MULTIPLEXING: Send STREAM_START before first chunk
	if !e.streamStarted {
		e.streamStarted = true
		startFrame := NewStreamStart(e.requestID, e.streamID, e.mediaUrn, nil)
		startFrame.RoutingId = e.routingId
		if err := e.writer.WriteFrame(startFrame); err != nil {
			return fmt.Errorf("failed to write STREAM_START: %w", err)
		}
	}

	// Split large byte/text data, encode each chunk as complete CBOR value
	if byteSlice, ok := value.([]byte); ok {
		// Split bytes BEFORE encoding, encode each chunk as []byte
		offset := 0
		for offset < len(byteSlice) {
			chunkSize := len(byteSlice) - offset
			if chunkSize > e.maxChunk {
				chunkSize = e.maxChunk
			}
			chunkBytes := byteSlice[offset : offset+chunkSize]

			// Encode as complete []byte - independently decodable
			cborPayload, err := cborlib.Marshal(chunkBytes)
			if err != nil {
				return fmt.Errorf("failed to encode chunk: %w", err)
			}

			currentSeq := e.seq
			e.seq++
			currentIndex := e.chunkIndex
			e.chunkIndex++
			checksum := ComputeChecksum(cborPayload)

			frame := NewChunk(e.requestID, e.streamID, currentSeq, cborPayload, currentIndex, checksum)
			frame.RoutingId = e.routingId
			if err := e.writer.WriteFrame(frame); err != nil {
				return fmt.Errorf("failed to write chunk: %w", err)
			}

			offset += chunkSize
		}
	} else if str, ok := value.(string); ok {
		// Split string BEFORE encoding, encode each chunk as string
		strBytes := []byte(str)
		offset := 0
		for offset < len(strBytes) {
			chunkSize := len(strBytes) - offset
			if chunkSize > e.maxChunk {
				chunkSize = e.maxChunk
			}
			// Ensure we split on UTF-8 character boundaries
			for chunkSize > 0 && offset+chunkSize < len(strBytes) && (strBytes[offset+chunkSize]&0xC0) == 0x80 {
				chunkSize--
			}
			if chunkSize == 0 {
				return fmt.Errorf("cannot split string on character boundary")
			}

			chunkStr := string(strBytes[offset : offset+chunkSize])

			// Encode as complete string - independently decodable
			cborPayload, err := cborlib.Marshal(chunkStr)
			if err != nil {
				return fmt.Errorf("failed to encode chunk: %w", err)
			}

			currentSeq := e.seq
			e.seq++
			currentIndex := e.chunkIndex
			e.chunkIndex++
			checksum := ComputeChecksum(cborPayload)

			frame := NewChunk(e.requestID, e.streamID, currentSeq, cborPayload, currentIndex, checksum)
			frame.RoutingId = e.routingId
			if err := e.writer.WriteFrame(frame); err != nil {
				return fmt.Errorf("failed to write chunk: %w", err)
			}

			offset += chunkSize
		}
	} else if slice, ok := value.([]interface{}); ok {
		// Array: send each element as independent CBOR chunk
		// Allows receiver to reconstruct elements without waiting for entire array
		for _, element := range slice {
			// Encode each element as complete CBOR value
			cborPayload, err := cborlib.Marshal(element)
			if err != nil {
				return fmt.Errorf("failed to encode array element: %w", err)
			}

			currentSeq := e.seq
			e.seq++
			currentIndex := e.chunkIndex
			e.chunkIndex++
			checksum := ComputeChecksum(cborPayload)

			frame := NewChunk(e.requestID, e.streamID, currentSeq, cborPayload, currentIndex, checksum)
			frame.RoutingId = e.routingId
			if err := e.writer.WriteFrame(frame); err != nil {
				return fmt.Errorf("failed to write chunk: %w", err)
			}
		}
	} else if m, ok := value.(map[interface{}]interface{}); ok {
		// Map: send each entry as independent CBOR chunk
		// Receiver must wait for all entries before reconstructing map
		for key, val := range m {
			// Encode each key-value pair as a 2-element array: [key, value]
			entry := []interface{}{key, val}
			cborPayload, err := cborlib.Marshal(entry)
			if err != nil {
				return fmt.Errorf("failed to encode map entry: %w", err)
			}

			currentSeq := e.seq
			e.seq++
			currentIndex := e.chunkIndex
			e.chunkIndex++
			checksum := ComputeChecksum(cborPayload)

			frame := NewChunk(e.requestID, e.streamID, currentSeq, cborPayload, currentIndex, checksum)
			frame.RoutingId = e.routingId
			if err := e.writer.WriteFrame(frame); err != nil {
				return fmt.Errorf("failed to write chunk: %w", err)
			}
		}
	} else {
		// For other types (int, float, bool, nil): encode as single chunk
		// These have single-value semantics and are typically small
		cborPayload, err := cborlib.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to CBOR-encode value: %w", err)
		}

		currentSeq := e.seq
		e.seq++
		currentIndex := e.chunkIndex
		e.chunkIndex++
		checksum := ComputeChecksum(cborPayload)

		frame := NewChunk(e.requestID, e.streamID, currentSeq, cborPayload, currentIndex, checksum)
		frame.RoutingId = e.routingId
		if err := e.writer.WriteFrame(frame); err != nil {
			return fmt.Errorf("failed to write chunk: %w", err)
		}
	}

	return nil
}

// Finalize sends STREAM_END + END frames to complete the response
func (e *threadSafeEmitter) Finalize() {
	e.seqMu.Lock()
	defer e.seqMu.Unlock()

	// If no chunks were sent, still send STREAM_START to keep protocol consistent
	if !e.streamStarted {
		e.streamStarted = true
		startFrame := NewStreamStart(e.requestID, e.streamID, e.mediaUrn, nil)
		startFrame.RoutingId = e.routingId
		if err := e.writer.WriteFrame(startFrame); err != nil {
			fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write STREAM_START: %v\n", err)
			return
		}
	}

	// STREAM_END: Close this stream
	streamEndFrame := NewStreamEnd(e.requestID, e.streamID, e.chunkIndex)
	streamEndFrame.RoutingId = e.routingId
	if err := e.writer.WriteFrame(streamEndFrame); err != nil {
		fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write STREAM_END: %v\n", err)
		return
	}

	// END: Close the entire request
	endFrame := NewEnd(e.requestID, nil)
	endFrame.RoutingId = e.routingId
	if err := e.writer.WriteFrame(endFrame); err != nil {
		fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write END: %v\n", err)
	}
}

func (e *threadSafeEmitter) Write(data []byte) error {
	e.seqMu.Lock()
	defer e.seqMu.Unlock()

	if !e.streamStarted {
		e.streamStarted = true
		startFrame := NewStreamStart(e.requestID, e.streamID, e.mediaUrn, nil)
		startFrame.RoutingId = e.routingId
		if err := e.writer.WriteFrame(startFrame); err != nil {
			return fmt.Errorf("failed to write STREAM_START: %w", err)
		}
	}

	offset := 0
	for offset < len(data) {
		chunkSize := len(data) - offset
		if chunkSize > e.maxChunk {
			chunkSize = e.maxChunk
		}
		chunkPayload := data[offset : offset+chunkSize]

		currentSeq := e.seq
		e.seq++
		currentIndex := e.chunkIndex
		e.chunkIndex++
		checksum := ComputeChecksum(chunkPayload)

		frame := NewChunk(e.requestID, e.streamID, currentSeq, chunkPayload, currentIndex, checksum)
		frame.RoutingId = e.routingId
		if err := e.writer.WriteFrame(frame); err != nil {
			return fmt.Errorf("failed to write chunk: %w", err)
		}

		offset += chunkSize
	}

	return nil
}

func (e *threadSafeEmitter) EmitListItem(value interface{}) error {
	e.seqMu.Lock()
	defer e.seqMu.Unlock()

	if !e.streamStarted {
		e.streamStarted = true
		startFrame := NewStreamStart(e.requestID, e.streamID, e.mediaUrn, nil)
		startFrame.RoutingId = e.routingId
		if err := e.writer.WriteFrame(startFrame); err != nil {
			return fmt.Errorf("failed to write STREAM_START: %w", err)
		}
	}

	cborBytes, err := cborlib.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to encode CBOR: %w", err)
	}

	offset := 0
	for offset < len(cborBytes) {
		chunkSize := len(cborBytes) - offset
		if chunkSize > e.maxChunk {
			chunkSize = e.maxChunk
		}
		chunkPayload := cborBytes[offset : offset+chunkSize]

		currentSeq := e.seq
		e.seq++
		currentIndex := e.chunkIndex
		e.chunkIndex++
		checksum := ComputeChecksum(chunkPayload)

		frame := NewChunk(e.requestID, e.streamID, currentSeq, chunkPayload, currentIndex, checksum)
		frame.RoutingId = e.routingId
		if err := e.writer.WriteFrame(frame); err != nil {
			return fmt.Errorf("failed to write chunk: %w", err)
		}

		offset += chunkSize
	}

	return nil
}

func (e *threadSafeEmitter) EmitLog(level, message string) {
	frame := NewLog(e.requestID, level, message)
	frame.RoutingId = e.routingId
	if err := e.writer.WriteFrame(frame); err != nil {
		fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write log: %v\n", err)
	}
}

func (e *threadSafeEmitter) Progress(progress float32, message string) {
	frame := NewProgress(e.requestID, progress, message)
	frame.RoutingId = e.routingId
	if err := e.writer.WriteFrame(frame); err != nil {
		fmt.Fprintf(os.Stderr, "[CartridgeRuntime] Failed to write progress: %v\n", err)
	}
}

// NewProgressSender creates a detached progress sender that can be used from goroutines.
//
// The returned ProgressSender is safe for concurrent use and can emit progress
// and log frames from any goroutine without holding a reference to this emitter.
func (e *threadSafeEmitter) NewProgressSender() *ProgressSender {
	return &ProgressSender{
		writer:    e.writer,
		requestID: e.requestID,
		routingId: e.routingId,
	}
}

// cliStreamEmitter implements StreamEmitter for CLI mode
type cliStreamEmitter struct{}

func (e *cliStreamEmitter) EmitCbor(value interface{}) error {
	// In CLI mode: extract raw bytes/text from value and emit to stdout
	// Supported types: []byte, string, map (extract "value" field)
	// NO FALLBACK - fail hard if unsupported type

	switch v := value.(type) {
	case []byte:
		// Raw bytes - write directly
		os.Stdout.Write(v)
	case string:
		// Text - write as bytes
		os.Stdout.WriteString(v)
	case map[string]interface{}:
		// Map - extract "value" field
		if val, ok := v["value"]; ok {
			return e.EmitCbor(val) // Recursive call
		}
		return fmt.Errorf("Map value has no 'value' field in CLI mode")
	default:
		return fmt.Errorf("Unsupported type in CLI mode: %T (expected []byte, string, or map)", value)
	}
	return nil
}

func (e *cliStreamEmitter) Write(data []byte) error {
	_, err := os.Stdout.Write(data)
	return err
}

func (e *cliStreamEmitter) EmitListItem(value interface{}) error {
	// In CLI mode, emit list items as individual lines of JSON
	return e.EmitCbor(value)
}

func (e *cliStreamEmitter) EmitLog(level, message string) {
	fmt.Fprintf(os.Stderr, "[%s] %s\n", level, message)
}

func (e *cliStreamEmitter) Progress(progress float32, message string) {
	fmt.Fprintf(os.Stderr, "[PROGRESS %.0f%%] %s\n", progress*100, message)
}

// pendingPeerRequest tracks a pending peer request.
// The reader loop forwards response frames to the channel.
type pendingPeerRequest struct {
	sender  chan Frame        // Channel to send response frames to handler
	streams map[string]string // stream_id → media_urn mapping
	ended   bool              // true after END frame (close channel)
}

// peerInvokerImpl implements PeerInvoker
type peerInvokerImpl struct {
	writer          *syncFrameWriter
	pendingRequests *sync.Map
	maxChunk        int
}

func newPeerInvokerImpl(writer *syncFrameWriter, pendingRequests *sync.Map, maxChunk int) *peerInvokerImpl {
	return &peerInvokerImpl{
		writer:          writer,
		pendingRequests: pendingRequests,
		maxChunk:        maxChunk,
	}
}

func (p *peerInvokerImpl) Invoke(capUrn string, arguments []cap.CapArgumentValue) (<-chan Frame, error) {
	// Generate a new message ID for this request
	requestID := NewMessageIdRandom()

	// Create a buffered channel for response frames
	sender := make(chan Frame, 64)

	// Register the pending request before sending
	p.pendingRequests.Store(requestID.ToString(), &pendingPeerRequest{
		sender:  sender,
		streams: make(map[string]string),
		ended:   false,
	})

	maxChunk := p.maxChunk

	// Protocol v2: REQ(empty) + STREAM_START + CHUNK(s) + STREAM_END + END per argument

	// 1. REQ with empty payload
	reqFrame := NewReq(requestID, capUrn, nil, "application/cbor")
	if err := p.writer.WriteFrame(reqFrame); err != nil {
		p.pendingRequests.Delete(requestID.ToString())
		return nil, fmt.Errorf("failed to send REQ frame: %w", err)
	}

	// 2. Each argument as an independent stream
	for _, arg := range arguments {
		streamID := fmt.Sprintf("peer-%s", NewMessageIdRandom().ToString()[:8])

		// STREAM_START
		startFrame := NewStreamStart(requestID, streamID, arg.MediaUrn, nil)
		if err := p.writer.WriteFrame(startFrame); err != nil {
			p.pendingRequests.Delete(requestID.ToString())
			return nil, fmt.Errorf("failed to send STREAM_START: %w", err)
		}

		// CHUNK(s): Send argument data as CBOR-encoded chunks
		// Each CHUNK payload MUST be independently decodable CBOR
		offset := 0
		seq := uint64(0)
		chunkIndex := uint64(0)
		for offset < len(arg.Value) {
			chunkSize := len(arg.Value) - offset
			if chunkSize > maxChunk {
				chunkSize = maxChunk
			}
			chunkBytes := arg.Value[offset : offset+chunkSize]

			// CBOR-encode chunk as []byte - independently decodable
			cborPayload, err := cborlib.Marshal(chunkBytes)
			if err != nil {
				p.pendingRequests.Delete(requestID.ToString())
				return nil, fmt.Errorf("failed to encode chunk: %w", err)
			}

			checksum := ComputeChecksum(cborPayload)
			chunkFrame := NewChunk(requestID, streamID, seq, cborPayload, chunkIndex, checksum)
			if err := p.writer.WriteFrame(chunkFrame); err != nil {
				p.pendingRequests.Delete(requestID.ToString())
				return nil, fmt.Errorf("failed to send CHUNK: %w", err)
			}
			offset += chunkSize
			seq++
			chunkIndex++
		}

		// STREAM_END
		endFrame := NewStreamEnd(requestID, streamID, chunkIndex)
		if err := p.writer.WriteFrame(endFrame); err != nil {
			p.pendingRequests.Delete(requestID.ToString())
			return nil, fmt.Errorf("failed to send STREAM_END: %w", err)
		}
	}

	// 3. END
	endFrame := NewEnd(requestID, nil)
	if err := p.writer.WriteFrame(endFrame); err != nil {
		p.pendingRequests.Delete(requestID.ToString())
		return nil, fmt.Errorf("failed to send END: %w", err)
	}

	return sender, nil
}

// noPeerInvoker is a no-op PeerInvoker that always returns an error
type noPeerInvoker struct{}

func (n *noPeerInvoker) Invoke(capUrn string, arguments []cap.CapArgumentValue) (<-chan Frame, error) {
	return nil, errors.New("peer invocation not supported in this context")
}

// Limits returns the current protocol limits
func (pr *CartridgeRuntime) Limits() Limits {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	return pr.limits
}

// buildPayloadFromStreamingReader builds CBOR payload from streaming reader (testable version).
//
// This simulates the CBOR chunked request flow for CLI piped stdin:
// - Pure binary chunks from reader
// - Accumulated in chunks (respecting max_chunk size)
// - Built into CBOR arguments array (same format as CBOR mode)
//
// This makes all 4 modes use the SAME payload format:
// - CLI file path → read file → payload
// - CLI piped binary → chunk reader → payload
// - CBOR chunked → payload
// - CBOR file path → auto-convert → payload
func (pr *CartridgeRuntime) buildPayloadFromStreamingReader(capDef *cap.Cap, reader io.Reader, maxChunk int) ([]byte, error) {
	// Accumulate chunks
	var chunks [][]byte
	totalBytes := 0

	for {
		buffer := make([]byte, maxChunk)
		n, err := reader.Read(buffer)
		if n > 0 {
			buffer = buffer[:n]
			chunks = append(chunks, buffer)
			totalBytes += n
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	// Concatenate chunks
	completePayload := make([]byte, 0, totalBytes)
	for _, chunk := range chunks {
		completePayload = append(completePayload, chunk...)
	}

	// Build CBOR arguments array (same format as CBOR mode)
	capUrn, err := urn.NewCapUrnFromString(capDef.UrnString())
	if err != nil {
		return nil, fmt.Errorf("invalid cap URN: %w", err)
	}
	expectedMediaUrn := capUrn.InSpec()

	arg := cap.CapArgumentValue{
		MediaUrn: expectedMediaUrn,
		Value:    completePayload,
	}

	// Encode as CBOR array
	cborArgs := []interface{}{
		map[string]interface{}{
			"media_urn": arg.MediaUrn,
			"value":     arg.Value,
		},
	}

	payload, err := cborlib.Marshal(cborArgs)
	if err != nil {
		return nil, fmt.Errorf("failed to encode CBOR payload: %w", err)
	}

	return payload, nil
}

// buildPayloadFromCLI builds the raw CBOR arguments payload from CLI args.
//
// Mirrors capdag/src/bifaci/cartridge_runtime.rs::build_payload_from_cli.
//
// File-path values stay as raw path/glob strings here — file reading and
// glob expansion happen later in extractEffectivePayload (after CLI-mode
// foreach iteration via buildCliForeachIterations).
func (pr *CartridgeRuntime) buildPayloadFromCLI(capDef *cap.Cap, cliArgs []string) ([]byte, error) {
	// Check for stdin data if cap accepts stdin.
	// Non-blocking check - if no data ready immediately, returns nil.
	var stdinData []byte
	if capDef.AcceptsStdin() {
		var err error
		stdinData, err = pr.readStdinIfAvailable()
		if err != nil {
			return nil, fmt.Errorf("failed to read stdin: %w", err)
		}
	}

	var arguments []cap.CapArgumentValue

	// Process each cap argument
	for i := range capDef.Args {
		argDef := &capDef.Args[i]

		value, cameFromStdin, err := pr.extractArgValue(argDef, cliArgs, stdinData)
		if err != nil {
			return nil, err
		}

		if value != nil {
			// Determine media_urn: if value came from stdin source, use stdin's media_urn.
			// Otherwise use arg's media_urn as-is (file-path conversion happens later).
			mediaUrn := argDef.MediaUrn
			if cameFromStdin {
				for j := range argDef.Sources {
					if argDef.Sources[j].Stdin != nil {
						mediaUrn = *argDef.Sources[j].Stdin
						break
					}
				}
			}
			arguments = append(arguments, cap.CapArgumentValue{
				MediaUrn: mediaUrn,
				Value:    value,
			})
		} else if argDef.Required {
			return nil, fmt.Errorf("Required argument '%s' not provided", argDef.MediaUrn)
		}
	}

	// If no arguments are defined but stdin data exists, use it as raw payload.
	if len(capDef.Args) == 0 {
		if stdinData != nil {
			return stdinData, nil
		}
		return []byte{}, nil
	}

	// Build CBOR arguments array (same format as CBOR mode)
	if len(arguments) > 0 {
		cborArgs := make([]interface{}, len(arguments))
		for i, arg := range arguments {
			cborArgs[i] = map[string]interface{}{
				"media_urn": arg.MediaUrn,
				"value":     arg.Value,
			}
		}
		payload, err := cborlib.Marshal(cborArgs)
		if err != nil {
			return nil, fmt.Errorf("Failed to encode CBOR payload: %w", err)
		}
		return payload, nil
	}

	return []byte{}, nil
}

// extractArgValue extracts a single argument value from CLI args or stdin.
// Handles automatic file-path to bytes conversion when appropriate.
// extractArgValue extracts a single argument value from CLI args or stdin.
//
// Mirrors capdag/src/bifaci/cartridge_runtime.rs::extract_arg_value.
//
// Returns (value, cameFromStdin, error). RAW values only — file-path
// auto-conversion happens later in extractEffectivePayload, after CLI-mode
// foreach iteration.
func (pr *CartridgeRuntime) extractArgValue(argDef *cap.CapArg, cliArgs []string, stdinData []byte) ([]byte, bool, error) {
	for i := range argDef.Sources {
		source := &argDef.Sources[i]

		if source.CliFlag != nil {
			if value, found := pr.getCliFlagValue(cliArgs, *source.CliFlag); found {
				return []byte(value), false, nil
			}
		} else if source.Position != nil {
			positional := pr.getPositionalArgs(cliArgs)
			if *source.Position < len(positional) {
				return []byte(positional[*source.Position]), false, nil
			}
		} else if source.Stdin != nil {
			if stdinData != nil && len(stdinData) > 0 {
				return stdinData, true, nil
			}
		}
	}

	// Try default value.
	//
	// The wire contract for an arg stream is "bytes of the typed
	// media URN". For a `media:enc=utf-8`-shaped arg that's plain
	// UTF-8 text — NOT a JSON-encoded form. A naive `json.Marshal`
	// of the `RawMessage` would corrupt every string default by
	// keeping the surrounding `"…"` quotes, which the handler's
	// UTF-8 decode would surface as a literal quoted string and
	// downstream parsers (model-spec, system-prompt, etc.) would
	// silently choke. Encode each scalar as its lexical wire form
	// matching exactly what the same value typed at the CLI flag
	// would produce. Composite defaults (array, object) ARE JSON
	// on the wire by design and pass through unchanged.
	if argDef.DefaultValue != nil {
		raw, err := json.Marshal(argDef.DefaultValue)
		if err != nil {
			return nil, false, fmt.Errorf("failed to encode default value as JSON: %w", err)
		}
		// Peek the first non-whitespace byte of the canonical JSON
		// encoding to dispatch to the wire form.
		// We also trim trailing whitespace so a number/bool default
		// with stray formatting becomes a clean lexical token on
		// the wire.
		stripped := raw
		for len(stripped) > 0 {
			c := stripped[0]
			if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
				stripped = stripped[1:]
				continue
			}
			break
		}
		for len(stripped) > 0 {
			c := stripped[len(stripped)-1]
			if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
				stripped = stripped[:len(stripped)-1]
				continue
			}
			break
		}
		if len(stripped) == 0 {
			// Whitespace-only or empty default — treat as
			// "no default supplied".
			return nil, false, nil
		}
		switch stripped[0] {
		case '"':
			// String value. JSON-decode to recover the un-quoted
			// text and emit raw UTF-8 bytes.
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, false, fmt.Errorf("failed to decode string default: %w", err)
			}
			return []byte(s), false, nil
		case '{', '[':
			// Object / array — composite default, stays JSON on
			// the wire.
			return raw, false, nil
		case 'n':
			// `null` — semantically equivalent to "no default
			// supplied". Emit empty bytes; mirrors the Rust
			// runtime's null arm.
			return []byte{}, false, nil
		default:
			// Number, `true`, `false`. JSON's lexical form for
			// these matches the CLI/text wire form; pass
			// through unchanged. Trim leading/trailing whitespace
			// so the caller sees a clean lexical token.
			return stripped, false, nil
		}
	}

	return nil, false, nil
}

// getCliFlagValue gets the value for a CLI flag (e.g., --model "value")
func (pr *CartridgeRuntime) getCliFlagValue(args []string, flag string) (string, bool) {
	for i := 0; i < len(args); i++ {
		if args[i] == flag {
			if i+1 < len(args) {
				return args[i+1], true
			}
			return "", false
		}
		// Handle --flag=value format
		if len(args[i]) > len(flag) && args[i][:len(flag)] == flag && args[i][len(flag)] == '=' {
			return args[i][len(flag)+1:], true
		}
	}
	return "", false
}

// getPositionalArgs gets positional arguments (non-flag arguments)
func (pr *CartridgeRuntime) getPositionalArgs(args []string) []string {
	var positional []string
	skipNext := false

	for _, arg := range args {
		if skipNext {
			skipNext = false
			continue
		}
		if len(arg) > 0 && arg[0] == '-' {
			// This is a flag - skip its value too if not --flag=value format
			if !contains(arg, '=') {
				skipNext = true
			}
		} else {
			positional = append(positional, arg)
		}
	}
	return positional
}

// contains checks if a string contains a character
func contains(s string, c byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return true
		}
	}
	return false
}

// readStdinIfAvailable reads stdin if data is available (non-blocking check).
// Returns nil immediately if stdin is a terminal or no data is ready.
func (pr *CartridgeRuntime) readStdinIfAvailable() ([]byte, error) {
	// Check if stdin is a terminal (interactive)
	stat, err := os.Stdin.Stat()
	if err != nil {
		return nil, err
	}

	// Don't read from stdin if it's a terminal (interactive)
	if (stat.Mode() & os.ModeCharDevice) != 0 {
		return nil, nil
	}

	// Non-blocking check: try reading with immediate timeout
	// Use a goroutine with select and timeout to avoid blocking
	type result struct {
		data []byte
		err  error
	}

	done := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(os.Stdin)
		done <- result{data, err}
	}()

	// Wait up to 100ms for data - if nothing arrives, assume no stdin data
	select {
	case res := <-done:
		if res.err != nil {
			return nil, res.err
		}
		if len(res.data) == 0 {
			return nil, nil
		}
		return res.data, nil
	case <-time.After(100 * time.Millisecond):
		// No data ready - return nil immediately
		return nil, nil
	}
}

// containsAny checks if string contains any of the given characters
func containsAny(s string, chars string) bool {
	for i := 0; i < len(s); i++ {
		for j := 0; j < len(chars); j++ {
			if s[i] == chars[j] {
				return true
			}
		}
	}
	return false
}

// ============================================================================
// Stream Helper Functions
// ============================================================================

// CollectStreams collects each stream individually into a slice of (mediaUrn, bytes) pairs.
// Each stream's bytes are accumulated separately — NOT concatenated.
// Use FindStream() helpers to retrieve args by URN pattern matching.
func CollectStreams(frames <-chan Frame) ([]struct {
	MediaUrn string
	Data     []byte
}, error) {
	streams := make(map[string]struct {
		MediaUrn string
		Chunks   [][]byte
	})
	var result []struct {
		MediaUrn string
		Data     []byte
	}

	for frame := range frames {
		switch frame.FrameType {
		case FrameTypeStreamStart:
			if frame.StreamId != nil && frame.MediaUrn != nil {
				streams[*frame.StreamId] = struct {
					MediaUrn string
					Chunks   [][]byte
				}{MediaUrn: *frame.MediaUrn, Chunks: [][]byte{}}
			}

		case FrameTypeChunk:
			// Verify checksum (protocol v2 integrity check)
			if err := VerifyChunkChecksum(&frame); err != nil {
				return nil, fmt.Errorf("corrupted data: %w", err)
			}
			if frame.StreamId != nil {
				if stream, ok := streams[*frame.StreamId]; ok {
					stream.Chunks = append(stream.Chunks, frame.Payload)
					streams[*frame.StreamId] = stream
				}
			}

		case FrameTypeStreamEnd:
			if frame.StreamId != nil {
				if stream, ok := streams[*frame.StreamId]; ok {
					var combined []byte
					for _, chunk := range stream.Chunks {
						combined = append(combined, chunk...)
					}
					result = append(result, struct {
						MediaUrn string
						Data     []byte
					}{MediaUrn: stream.MediaUrn, Data: combined})
					delete(streams, *frame.StreamId)
				}
			}

		case FrameTypeEnd:
			return result, nil

		case FrameTypeErr:
			code := frame.ErrorCode()
			msg := frame.ErrorMessage()
			if code == "" {
				code = "UNKNOWN"
			}
			if msg == "" {
				msg = "Unknown error"
			}
			return nil, fmt.Errorf("error: [%s] %s", code, msg)
		}
	}

	return result, nil
}

// FindStream finds a stream's bytes by exact URN equivalence.
// Uses MediaUrn.IsEquivalent() — matches only if both URNs have the
// exact same tag set (order-independent).
func FindStream(streams []struct {
	MediaUrn string
	Data     []byte
}, mediaUrn string) ([]byte, error) {
	targetUrn, err := urn.NewMediaUrnFromString(mediaUrn)
	if err != nil {
		return nil, err
	}

	for _, stream := range streams {
		streamUrn, err := urn.NewMediaUrnFromString(stream.MediaUrn)
		if err != nil {
			continue
		}
		// Use IsEquivalent: both URNs are concrete, exact tag-set match required
		if targetUrn.IsEquivalent(streamUrn) {
			return stream.Data, nil
		}
	}

	return nil, nil
}

// FindStreamStr is like FindStream but returns a UTF-8 string.
func FindStreamStr(streams []struct {
	MediaUrn string
	Data     []byte
}, mediaUrn string) (string, error) {
	data, err := FindStream(streams, mediaUrn)
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", nil
	}
	return string(data), nil
}

// RequireStream is like FindStream but fails hard if not found.
func RequireStream(streams []struct {
	MediaUrn string
	Data     []byte
}, mediaUrn string) ([]byte, error) {
	data, err := FindStream(streams, mediaUrn)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("missing required arg: %s", mediaUrn)
	}
	return data, nil
}

// RequireStreamStr is like RequireStream but returns a UTF-8 string.
func RequireStreamStr(streams []struct {
	MediaUrn string
	Data     []byte
}, mediaUrn string) (string, error) {
	data, err := RequireStream(streams, mediaUrn)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
