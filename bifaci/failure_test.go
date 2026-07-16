package bifaci

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TEST1730: the wire vocabulary round-trips exactly and rejects unknowns.
// (mirrors Rust ops/src/failure.rs TEST1730)
func Test1730_failure_class_wire_tokens_round_trip(t *testing.T) {
	for _, class := range []FailureClass{
		FailureClassInput,
		FailureClassResource,
		FailureClassEnvironment,
		FailureClassInternal,
	} {
		parsed, ok := FailureClassFromWire(class.String())
		assert.True(t, ok, "token %q must parse", class.String())
		assert.Equal(t, class, parsed)
	}
	_, ok := FailureClassFromWire("user-error")
	assert.False(t, ok, "unknown token must be rejected by the parse")
	_, ok = FailureClassFromWire("")
	assert.False(t, ok, "empty token must be rejected by the parse")
}

// TEST1731: only Input is permanent — the retry machinery keys on this.
// (mirrors Rust ops/src/failure.rs TEST1731)
func Test1731_only_input_is_permanent(t *testing.T) {
	assert.True(t, FailureClassInput.IsPermanent())
	assert.False(t, FailureClassResource.IsPermanent())
	assert.False(t, FailureClassEnvironment.IsPermanent())
	assert.False(t, FailureClassInternal.IsPermanent())
}

// TEST1735: the handler-failure ERR emit resolves the declared identity from
// the error chain — ClassifiedError and a propagated peer RemoteError keep
// their code/class through fmt.Errorf wrapping; an undeclared error is
// HANDLER_ERROR/internal. (mirrors Rust RuntimeError::failure_code/
// failure_class/failure_reason at the frame-emit boundary)
func Test1735_classify_handler_error_reads_the_chain(t *testing.T) {
	classified := &ClassifiedError{Code: "CONTEXT_OVERFLOW", Class: FailureClassInput, Message: "prompt exceeds context"}
	code, class, message := classifyHandlerError(fmt.Errorf("op failed: %w", classified))
	assert.Equal(t, "CONTEXT_OVERFLOW", code)
	assert.Equal(t, FailureClassInput, class)
	assert.Equal(t, "prompt exceeds context", message)

	remote := &RemoteError{Code: "OOM_KILLED", Class: FailureClassResource, Message: "peer ran out of memory"}
	code, class, message = classifyHandlerError(fmt.Errorf("peer call failed: %w", remote))
	assert.Equal(t, "OOM_KILLED", code)
	assert.Equal(t, FailureClassResource, class)
	assert.Equal(t, "peer ran out of memory", message)

	code, class, message = classifyHandlerError(errors.New("something broke"))
	assert.Equal(t, "HANDLER_ERROR", code)
	assert.Equal(t, FailureClassInternal, class)
	assert.Equal(t, "something broke", message)
}
