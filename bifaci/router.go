// Cap Router - Pluggable routing for peer invoke requests
//
// When a cartridge sends a peer invoke REQ (calling another cap), the host needs to route
// that request to an appropriate handler. This module provides interfaces for different
// routing strategies.

package bifaci

// PeerRequestHandle is the handle for an active peer invoke request.
// The CartridgeHostRuntime creates this by calling router.BeginRequest(), then forwards
// incoming frames (STREAM_START, CHUNK, STREAM_END, END) to the handle.
type PeerRequestHandle interface {
	// ForwardFrame sends a frame (STREAM_START, CHUNK, STREAM_END, or END) to the target.
	ForwardFrame(frame Frame)

	// ResponseChannel returns a channel that yields response chunks from the target cartridge.
	ResponseChannel() <-chan ResponseChunkResult
}

// ResponseChunkResult wraps a response chunk or error from a peer invoke.
type ResponseChunkResult struct {
	Chunk *ResponseChunk
	Err   error
}

// CapRouter routes cap invocation requests to appropriate handlers.
// When a cartridge issues a peer invoke, the host receives a REQ frame and calls BeginRequest().
// The router returns a handle that the host uses to forward incoming argument streams
// and receive responses.
type CapRouter interface {
	// BeginRequest starts routing a peer invoke request.
	// cap_urn is the requested cap URN, req_id is the 16-byte request ID.
	// Returns a PeerRequestHandle or an error.
	BeginRequest(capUrn string, reqId [16]byte) (PeerRequestHandle, error)
}

// NoPeerRouter is a no-op router that rejects all peer invoke requests.
type NoPeerRouter struct{}

// BeginRequest always returns PeerInvokeNotSupported error.
func (r *NoPeerRouter) BeginRequest(capUrn string, reqId [16]byte) (PeerRequestHandle, error) {
	return nil, &HostError{
		Type:    HostErrorTypePeerInvokeNotSupported,
		Message: capUrn,
	}
}
