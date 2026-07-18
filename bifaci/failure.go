package bifaci

import (
	"errors"
	"fmt"
)

// =============================================================================
// FAILURE TAXONOMY — whose problem a failure is (docs/failure-taxonomy.md)
// =============================================================================

// FailureClass is whose problem a failure is. Declared at the error's
// DEFINITION site and carried structurally through every hop — no layer ever
// infers another layer's class from message text. The bifaci ERR frame
// carries the class over the wire (meta key "class"); all four language
// runtimes share the same token vocabulary. An error that reaches a boundary
// without a declared class is FailureClassInternal — unclassified means
// "ours", never a guess. (matches Rust capdag::FailureClass, re-exported
// from ops::failure)
type FailureClass uint8

const (
	// FailureClassInternal: everything else — a defect in the engine or a
	// cartridge. Ours, said plainly. Retryable (races un-race), but never
	// blamed on the user. The zero value, mirroring Rust's Default: an error
	// constructed without a declared class is unclassified, and unclassified
	// means "ours".
	FailureClassInternal FailureClass = iota
	// FailureClassInput: deterministic on the INPUT (context overflow,
	// invalid request, unsupported format). The user's to fix; retrying can
	// never succeed — tasks failing with this class are marked permanently
	// failed.
	FailureClassInput
	// FailureClassResource: a compute resource was exhausted (GPU VRAM, host
	// memory). Often transient (another process holding memory) — retryable.
	FailureClassResource
	// FailureClassEnvironment: the environment failed (network, registry,
	// model download/integrity, cartridge process death). Transient by
	// nature — retryable.
	FailureClassEnvironment
)

// String returns the wire token — used in the ERR frame meta, the
// machine_runs columns, the gRPC proto, and the loom. One vocabulary
// everywhere. (matches Rust FailureClass::as_str)
func (c FailureClass) String() string {
	switch c {
	case FailureClassInput:
		return "input"
	case FailureClassResource:
		return "resource"
	case FailureClassEnvironment:
		return "environment"
	case FailureClassInternal:
		return "internal"
	default:
		panic(fmt.Sprintf("BUG: FailureClass %d not covered by String", uint8(c)))
	}
}

// FailureClassFromWire parses a wire token. Returns false for unknown tokens
// — a PROTOCOL error, not a fallback case: the caller decides whether to
// fail hard or treat the frame as unclassified (Frame.ErrorClass applies the
// receiver's Internal fallback). (matches Rust FailureClass::from_wire)
func FailureClassFromWire(token string) (FailureClass, bool) {
	switch token {
	case "input":
		return FailureClassInput, true
	case "resource":
		return FailureClassResource, true
	case "environment":
		return FailureClassEnvironment, true
	case "internal":
		return FailureClassInternal, true
	default:
		return FailureClassInternal, false
	}
}

// IsPermanent reports whether retrying can NEVER succeed: the failure is a
// deterministic function of the input. Resource/environment/internal stay
// retryable (memory frees up, networks recover, races un-race).
// (matches Rust FailureClass::is_permanent)
func (c FailureClass) IsPermanent() bool {
	return c == FailureClassInput
}

// ClassifiedError is a handler failure carrying its FULL identity: the
// machine-readable code the handler's typed error declares, the failure
// class it declares (whose problem it is — declared at the error's
// definition site), and the human message. Handlers return this (directly or
// wrapped, extracted via errors.As) instead of folding the code into message
// text; the terminal ERR frame then carries all three fields to the engine.
// Failures that stay a plain error classify as Internal at the frame
// boundary. ArgUrn is the media URN of the argument the failure is
// attributed to, declared at the emit source alongside the class
// (docs/failure-taxonomy.md); nil when the failure has no attribution.
// (matches Rust RuntimeError::Classified)
type ClassifiedError struct {
	Code    string
	Class   FailureClass
	Message string
	ArgUrn  *string
}

func (e *ClassifiedError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// FailureArgUrn is the media URN of the argument the failure is attributed
// to, declared at the emit source; nil when there is no attribution.
func (e *ClassifiedError) FailureArgUrn() *string {
	return e.ArgUrn
}

// RemoteError is the peer's ERR frame, kept STRUCTURAL: its machine-readable
// code, the failure class the peer's frame declared
// (docs/failure-taxonomy.md), its message — never folded into prose — and
// the media URN of the argument the peer's frame attributed the failure to
// (nil when the frame carried no attribution).
// (matches Rust StreamError::RemoteError)
type RemoteError struct {
	Code    string
	Class   FailureClass
	Message string
	ArgUrn  *string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("remote error [%s]: %s", e.Code, e.Message)
}

// FailureArgUrn is the media URN of the argument the peer's frame attributed
// the failure to; nil when the frame carried no attribution.
func (e *RemoteError) FailureArgUrn() *string {
	return e.ArgUrn
}

// remoteErrorFromErrFrame reads an incoming ERR frame's declared identity
// into a RemoteError: code (missing → "UNKNOWN"), class (missing or unknown
// token → Internal, the taxonomy's receiver rule), and message (missing →
// "Unknown error"). (matches the Rust demux paths' error_code/error_class/
// error_message receipt)
func remoteErrorFromErrFrame(f *Frame) *RemoteError {
	code := f.ErrorCode()
	if code == "" {
		code = "UNKNOWN"
	}
	message := f.ErrorMessage()
	if message == "" {
		message = "Unknown error"
	}
	return &RemoteError{Code: code, Class: f.ErrorClass(), Message: message, ArgUrn: f.ErrorArgUrn()}
}

// classifyHandlerError resolves the identity a failed handler's terminal ERR
// frame declares (docs/failure-taxonomy.md): the code, class, and argument
// attribution from the emit source when the error chain carries a
// ClassifiedError (or a peer's RemoteError propagated as-is),
// HANDLER_ERROR/Internal without attribution when the handler never declared
// one. (matches Rust RuntimeError's failure_code()/failure_class()/
// failure_reason()/failure_arg_urn() at the frame-emit boundary)
func classifyHandlerError(err error) (code string, class FailureClass, message string, argUrn *string) {
	var classified *ClassifiedError
	if errors.As(err, &classified) {
		return classified.Code, classified.Class, classified.Message, classified.ArgUrn
	}
	var remote *RemoteError
	if errors.As(err, &remote) {
		return remote.Code, remote.Class, remote.Message, remote.ArgUrn
	}
	return "HANDLER_ERROR", FailureClassInternal, err.Error(), nil
}
