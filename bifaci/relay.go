package bifaci

import (
	"fmt"
	"io"
	"sync"
)

// RelayError represents errors from relay operations
type RelayError struct {
	Type    RelayErrorType
	Message string
}

type RelayErrorType int

const (
	RelayErrorTypeSocketClosed RelayErrorType = iota
	RelayErrorTypeLocalClosed
	RelayErrorTypeIO
	RelayErrorTypeProtocol
)

func (e *RelayError) Error() string {
	switch e.Type {
	case RelayErrorTypeSocketClosed:
		return "relay: socket closed"
	case RelayErrorTypeLocalClosed:
		return "relay: local closed"
	case RelayErrorTypeIO:
		return fmt.Sprintf("relay I/O error: %s", e.Message)
	case RelayErrorTypeProtocol:
		return fmt.Sprintf("relay protocol error: %s", e.Message)
	default:
		return fmt.Sprintf("relay error: %s", e.Message)
	}
}

// RelaySlave is the slave endpoint of the CBOR frame relay.
// Sits inside the cartridge host process. Bridges between a socket connection
// (to the RelayMaster in the engine) and local I/O (to/from the cartridge host runtime).
//
// Two relay-specific frame types are intercepted and never leaked through:
// - RelayNotify (slave -> master): Capability advertisement
// - RelayState (master -> slave): Host system resources
//
// All other frames pass through transparently in both directions.
type RelaySlave struct {
	localReader     *FrameReader
	localWriter     *FrameWriter
	resourceState   []byte
	resourceStateMu sync.Mutex
}

// NewRelaySlave creates a new relay slave with local I/O streams (to/from CartridgeHostRuntime).
func NewRelaySlave(localRead io.Reader, localWrite io.Writer) *RelaySlave {
	return &RelaySlave{
		localReader:   NewFrameReader(localRead),
		localWriter:   NewFrameWriter(localWrite),
		resourceState: nil,
	}
}

// ResourceState returns the latest resource state payload received from the master.
func (rs *RelaySlave) ResourceState() []byte {
	rs.resourceStateMu.Lock()
	defer rs.resourceStateMu.Unlock()
	if rs.resourceState == nil {
		return nil
	}
	result := make([]byte, len(rs.resourceState))
	copy(result, rs.resourceState)
	return result
}

// Run runs the relay bidirectionally. Blocks until one side closes or an error occurs.
//
// Uses two goroutines for true bidirectional forwarding:
// - goroutine 1 (socket -> local): RelayState stored (not forwarded); RelayNotify dropped; others forwarded
// - goroutine 2 (local -> socket): RelayNotify/RelayState dropped; others forwarded
//
// When either direction closes, both goroutines stop.
func (rs *RelaySlave) Run(socketRead io.Reader, socketWrite io.Writer, initialNotify *RelayNotifyParams) error {
	socketReader := NewFrameReader(socketRead)
	socketWriter := NewFrameWriter(socketWrite)

	// Send initial RelayNotify if provided
	if initialNotify != nil {
		if err := SendNotify(socketWriter, initialNotify.Manifest, initialNotify.Limits); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	// Goroutine 1: socket -> local (master -> slave direction)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			frame, err := socketReader.ReadFrame()
			if err != nil {
				if err == io.EOF || isClosedError(err) {
					errCh <- nil
					return
				}
				errCh <- err
				return
			}

			if frame.FrameType == FrameTypeRelayState {
				// Intercept: store resource state, don't forward
				if frame.Payload != nil {
					rs.resourceStateMu.Lock()
					rs.resourceState = make([]byte, len(frame.Payload))
					copy(rs.resourceState, frame.Payload)
					rs.resourceStateMu.Unlock()
				}
			} else if frame.FrameType == FrameTypeRelayNotify {
				// RelayNotify from master? Protocol error — drop silently
			} else {
				// Pass through to local side
				if err := rs.localWriter.WriteFrame(frame); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	// Goroutine 2: local -> socket (slave -> master direction)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			frame, err := rs.localReader.ReadFrame()
			if err != nil {
				if err == io.EOF || isClosedError(err) {
					errCh <- nil
					return
				}
				errCh <- err
				return
			}

			if frame.FrameType == FrameTypeRelayNotify || frame.FrameType == FrameTypeRelayState {
				// Relay frames from local side should not happen — drop
			} else {
				// Pass through to socket
				if err := socketWriter.WriteFrame(frame); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	// Wait for first goroutine to finish (either direction)
	firstErr := <-errCh

	// The other goroutine will eventually stop when its reader gets EOF
	// (since we can't close the readers from here, we just wait)
	wg.Wait()

	return firstErr
}

// RelayNotifyParams holds parameters for sending a RelayNotify frame.
type RelayNotifyParams struct {
	Manifest []byte
	Limits   Limits
}

// SendNotify sends a RelayNotify frame to the socket writer.
// Used when capabilities change (cartridge discovered, cartridge died).
func SendNotify(socketWriter *FrameWriter, manifest []byte, limits Limits) error {
	frame := NewRelayNotify(manifest, limits.MaxFrame, limits.MaxChunk, limits.MaxReorderBuffer)
	return socketWriter.WriteFrame(frame)
}

// RelayMaster is the master endpoint of the relay. Sits in the engine process.
//
// - Reads frames from the socket (from slave): RelayNotify -> update internal state; others -> return to caller
// - Can send RelayState frames to the slave
type RelayMaster struct {
	manifest []byte
	limits   Limits
}

// ConnectRelayMaster connects to a relay slave by reading the initial RelayNotify frame.
// The slave MUST send a RelayNotify as its first frame after connection.
func ConnectRelayMaster(socketReader *FrameReader) (*RelayMaster, error) {
	frame, err := socketReader.ReadFrame()
	if err != nil {
		if err == io.EOF {
			return nil, &RelayError{Type: RelayErrorTypeSocketClosed, Message: "connection closed before receiving RelayNotify"}
		}
		return nil, &RelayError{Type: RelayErrorTypeIO, Message: err.Error()}
	}

	if frame.FrameType != FrameTypeRelayNotify {
		return nil, &RelayError{
			Type:    RelayErrorTypeProtocol,
			Message: fmt.Sprintf("expected RelayNotify, got %v", frame.FrameType),
		}
	}

	manifest := frame.RelayNotifyManifest()
	if manifest == nil {
		return nil, &RelayError{Type: RelayErrorTypeProtocol, Message: "RelayNotify missing manifest"}
	}

	limits := frame.RelayNotifyLimits()
	if limits == nil {
		return nil, &RelayError{Type: RelayErrorTypeProtocol, Message: "RelayNotify missing limits"}
	}

	return &RelayMaster{
		manifest: manifest,
		limits:   *limits,
	}, nil
}

// Manifest returns the aggregate manifest from the slave.
func (rm *RelayMaster) Manifest() []byte {
	return rm.manifest
}

// Limits returns the negotiated limits from the slave.
func (rm *RelayMaster) Limits() Limits {
	return rm.limits
}

// SendRelayState sends a RelayState frame to the slave with host system resource info.
func SendRelayState(socketWriter *FrameWriter, resources []byte) error {
	frame := NewRelayState(resources)
	return socketWriter.WriteFrame(frame)
}

// ReadFrame reads the next non-relay frame from the socket.
// RelayNotify frames are intercepted: manifest and limits are updated.
// All other frames are returned to the caller.
// Returns nil on EOF.
func (rm *RelayMaster) ReadFrame(socketReader *FrameReader) (*Frame, error) {
	for {
		frame, err := socketReader.ReadFrame()
		if err != nil {
			if err == io.EOF {
				return nil, nil
			}
			return nil, err
		}

		if frame.FrameType == FrameTypeRelayNotify {
			// Intercept: update manifest and limits
			if m := frame.RelayNotifyManifest(); m != nil {
				rm.manifest = m
			}
			if l := frame.RelayNotifyLimits(); l != nil {
				rm.limits = *l
			}
			continue // Don't return relay frames to caller
		} else if frame.FrameType == FrameTypeRelayState {
			// RelayState from slave? Protocol error — drop
			continue
		}

		return frame, nil
	}
}

// isClosedError checks if an error indicates a closed connection
func isClosedError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return msg == "io: read/write on closed pipe" ||
		msg == "EOF" ||
		msg == "read/write on closed pipe"
}
