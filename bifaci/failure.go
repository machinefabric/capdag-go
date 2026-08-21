package bifaci

import (
	"encoding/json"
	"errors"
	"fmt"
)

// =============================================================================
// DIAGNOSTIC ATTRIBUTION — whose domain a diagnostic belongs to
// =============================================================================

// AttributionClass is declared at the diagnostic definition or emit site and
// carried structurally through every hop. Bifaci ERR and non-progress LOG
// frames carry it in the mandatory "attribution_class" metadata key. Receivers
// never infer it from prose or manufacture a value for malformed frames.
type AttributionClass uint8

const (
	// AttributionClassInternal: everything else — a defect in the engine or a
	// cartridge. Ours, said plainly. Retryable (races un-race), but never
	// blamed on the user. The zero value, mirroring Rust's Default: an error
	// constructed without a declared class is unclassified, and unclassified
	// means "ours".
	AttributionClassInternal AttributionClass = iota
	// AttributionClassInput: deterministic on the INPUT (context overflow,
	// invalid request, unsupported format). The user's to fix; retrying can
	// never succeed — tasks failing with this class are marked permanently
	// failed.
	AttributionClassInput
	// AttributionClassResource: a compute resource was exhausted (GPU VRAM, host
	// memory). Often transient (another process holding memory) — retryable.
	AttributionClassResource
	// AttributionClassEnvironment: the environment failed (network, registry,
	// model download/integrity, cartridge process death). Transient by
	// nature — retryable.
	AttributionClassEnvironment
	// AttributionClassUser: the USER decided it — an operator cancelled the
	// run. Not a failure at all; never retried automatically, never a
	// defect, and the one class under which "cancelled" is the truth.
	AttributionClassUser
)

// String returns the wire token — used in the ERR frame meta, the
// machine_runs columns, the gRPC proto, and the loom. One vocabulary
// everywhere. (matches Rust AttributionClass::as_str)
func (c AttributionClass) String() string {
	switch c {
	case AttributionClassInput:
		return "input"
	case AttributionClassResource:
		return "resource"
	case AttributionClassEnvironment:
		return "environment"
	case AttributionClassInternal:
		return "internal"
	case AttributionClassUser:
		return "user"
	default:
		panic(fmt.Sprintf("BUG: AttributionClass %d not covered by String", uint8(c)))
	}
}

// AttributionClassFromWire parses a wire token. False means the frame is a
// protocol violation; callers must reject it rather than substitute Internal.
func AttributionClassFromWire(token string) (AttributionClass, bool) {
	switch token {
	case "input":
		return AttributionClassInput, true
	case "resource":
		return AttributionClassResource, true
	case "environment":
		return AttributionClassEnvironment, true
	case "internal":
		return AttributionClassInternal, true
	case "user":
		return AttributionClassUser, true
	default:
		return AttributionClassInternal, false
	}
}

// IsPermanent reports whether retrying can NEVER succeed: the failure is a
// deterministic function of the input, or the user chose to end it.
// Resource/environment/internal stay retryable (memory frees up, networks
// recover, races un-race). (matches Rust AttributionClass::is_permanent)
func (c AttributionClass) IsPermanent() bool {
	return c == AttributionClassInput || c == AttributionClassUser
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
	Class   AttributionClass
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
	Class   AttributionClass
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

// errorFromErrFrame reads an incoming ERR frame's complete declared identity.
// Missing fields are protocol violations; receivers never manufacture an
// identity for malformed frames.
func errorFromErrFrame(f *Frame) error {
	code := f.ErrorCode()
	if code == "" {
		return errors.New("invalid ERR frame: missing required text code")
	}
	message := f.ErrorMessage()
	if message == "" {
		return errors.New("invalid ERR frame: missing required text message")
	}
	class, err := f.AttributionClass()
	if err != nil {
		return fmt.Errorf("invalid ERR frame: %w", err)
	}
	argUrn, err := f.AttributionArgUrn()
	if err != nil {
		return fmt.Errorf("invalid ERR frame: %w", err)
	}
	return &RemoteError{Code: code, Class: class, Message: message, ArgUrn: argUrn}
}

// classifyHandlerError resolves the identity a failed handler's terminal ERR
// frame declares (docs/failure-taxonomy.md): the code, class, and argument
// attribution from the emit source when the error chain carries a
// ClassifiedError (or a peer's RemoteError propagated as-is),
// HANDLER_ERROR/Internal without attribution when the handler never declared
// one. (matches Rust RuntimeError's failure_code()/attribution_class()/
// failure_reason()/failure_arg_urn() at the frame-emit boundary)
func classifyHandlerError(err error) (code string, class AttributionClass, message string, argUrn *string) {
	var classified *ClassifiedError
	if errors.As(err, &classified) {
		return classified.Code, classified.Class, classified.Message, classified.ArgUrn
	}
	var remote *RemoteError
	if errors.As(err, &remote) {
		return remote.Code, remote.Class, remote.Message, remote.ArgUrn
	}
	return "HANDLER_ERROR", AttributionClassInternal, err.Error(), nil
}

// MarshalJSON serializes an AttributionClass as its wire token — the snapshot
// contract for mirrors and traces.
func (c AttributionClass) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

// UnmarshalJSON parses the wire token; an unknown token is an error, never a
// substituted class.
func (c *AttributionClass) UnmarshalJSON(data []byte) error {
	var token string
	if err := json.Unmarshal(data, &token); err != nil {
		return err
	}
	parsed, ok := AttributionClassFromWire(token)
	if !ok {
		return fmt.Errorf("unknown attribution_class %q", token)
	}
	*c = parsed
	return nil
}
