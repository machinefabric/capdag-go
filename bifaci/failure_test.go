package bifaci

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TEST1730: the wire vocabulary round-trips exactly and rejects unknowns.
// (mirrors Rust ops/src/failure.rs TEST1730)
func Test1730_attribution_class_wire_tokens_round_trip(t *testing.T) {
	for _, class := range []AttributionClass{
		AttributionClassInput,
		AttributionClassResource,
		AttributionClassEnvironment,
		AttributionClassInternal,
		AttributionClassUser,
	} {
		parsed, ok := AttributionClassFromWire(class.String())
		assert.True(t, ok, "token %q must parse", class.String())
		assert.Equal(t, class, parsed)
	}
	_, ok := AttributionClassFromWire("user-error")
	assert.False(t, ok, "unknown token must be rejected by the parse")
	_, ok = AttributionClassFromWire("")
	assert.False(t, ok, "empty token must be rejected by the parse")
}

// TEST1731: Input and User are permanent — the retry machinery keys on this
// (a deterministic input failure, or a human's decision, is never retried
// automatically). (mirrors Rust ops/src/failure.rs TEST1731)
func Test1731_only_input_and_user_are_permanent(t *testing.T) {
	assert.True(t, AttributionClassInput.IsPermanent())
	assert.True(t, AttributionClassUser.IsPermanent())
	assert.False(t, AttributionClassResource.IsPermanent())
	assert.False(t, AttributionClassEnvironment.IsPermanent())
	assert.False(t, AttributionClassInternal.IsPermanent())
}

// TEST1735: the handler-failure ERR emit resolves the declared identity from
// the error chain — ClassifiedError and a propagated peer RemoteError keep
// their code/class, argument attribution and hint through fmt.Errorf
// wrapping; an undeclared error is HANDLER_ERROR/internal with neither.
// (mirrors Rust RuntimeError::failure_code/attribution_class/failure_reason/
// failure_hint at the frame-emit boundary)
func Test1735_classify_handler_error_reads_the_chain(t *testing.T) {
	promptUrn := "media:prompt;str;utf8"
	classified := &ClassifiedError{Code: "CONTEXT_OVERFLOW", Class: AttributionClassInput, Message: "prompt exceeds context", ArgUrn: &promptUrn}
	code, class, message, argUrn, hint := classifyHandlerError(fmt.Errorf("op failed: %w", classified))
	assert.Equal(t, "CONTEXT_OVERFLOW", code)
	assert.Equal(t, AttributionClassInput, class)
	assert.Equal(t, "prompt exceeds context", message)
	if assert.NotNil(t, argUrn, "a classified error's declared arg attribution must survive wrapping") {
		assert.Equal(t, promptUrn, *argUrn)
	}
	assert.Nil(t, hint, "a classified error with no hint gives none")

	install := "capdag cartridge install ggufcartridge"
	remote := &RemoteError{Code: "CARTRIDGE_INSTALL_FAILED", Class: AttributionClassEnvironment,
		Message: "ggufcartridge is not installed, and installing it failed", Hint: &install}
	code, class, message, argUrn, hint = classifyHandlerError(fmt.Errorf("peer call failed: %w", remote))
	assert.Equal(t, "CARTRIDGE_INSTALL_FAILED", code)
	assert.Equal(t, AttributionClassEnvironment, class)
	assert.Equal(t, "ggufcartridge is not installed, and installing it failed", message)
	assert.Nil(t, argUrn, "a peer frame without attribution stays unattributed")
	if assert.NotNil(t, hint, "a forwarded peer error keeps the peer's hint") {
		assert.Equal(t, install, *hint)
	}

	code, class, message, argUrn, hint = classifyHandlerError(errors.New("something broke"))
	assert.Equal(t, "HANDLER_ERROR", code)
	assert.Equal(t, AttributionClassInternal, class)
	assert.Equal(t, "something broke", message)
	assert.Nil(t, argUrn)
	assert.Nil(t, hint)
}

// TEST7189: a handler that cannot go on without its peer's answer, and
// returns the peer's error as it came, ends its request with the peer's ERR —
// its code, class, message and hint — not a HANDLER_ERROR of its own.
// (mirrors Rust TEST7189)
func Test7189_a_forwarded_peer_error_ends_the_request_as_the_peers_err(t *testing.T) {
	install := "capdag cartridge install ggufcartridge"
	refused := &RemoteError{Code: "CARTRIDGE_INSTALL_FAILED", Class: AttributionClassEnvironment,
		Message: "ggufcartridge provides the inference cap and is not installed, and installing it failed",
		Hint:    &install}
	frame := handlerErrFrame(NewMessageIdRandom(), refused)
	assert.Equal(t, "CARTRIDGE_INSTALL_FAILED", frame.ErrorCode())
	class, err := frame.AttributionClass()
	assert.NoError(t, err)
	assert.Equal(t, AttributionClassEnvironment, class)
	assert.Equal(t, refused.Message, frame.ErrorMessage())
	hint, err := frame.ErrorHint()
	assert.NoError(t, err)
	if assert.NotNil(t, hint, "the handler's own ERR carries the peer's hint") {
		assert.Equal(t, install, *hint)
	}

	own := handlerErrFrame(NewMessageIdRandom(), errors.New("something broke"))
	assert.Equal(t, "HANDLER_ERROR", own.ErrorCode())
	none, err := own.ErrorHint()
	assert.NoError(t, err)
	assert.Nil(t, none)
}
