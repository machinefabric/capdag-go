package planner

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// OnError is what a step's lasting failure does to the run.
type OnError string

const (
	// OnErrorFail — the run fails. The default.
	OnErrorFail OnError = "fail"
	// OnErrorAbsent — the step's outputs are absent and the run goes on.
	OnErrorAbsent OnError = "absent"
)

// StepPolicy is what a step does when it fails
// (capdag/docs/09-machine-notation.md §Step policy). The zero value — no
// policy written — is to fail the run at once.
//
// It is written in notation on the step and carried by the step's node in a
// plan, so it lives where both can name it.
type StepPolicy struct {
	// Retry is the number of further attempts after a failure whose class is
	// retryable.
	Retry uint32
	// OnError is what a failure that stands (after its retries) does to the
	// run. The empty value is OnErrorFail.
	OnError OnError
}

// IsDefault reports whether this is the policy of a step that wrote none.
func (p StepPolicy) IsDefault() bool {
	return p.Retry == 0 && p.EffectiveOnError() == OnErrorFail
}

// EffectiveOnError is what a lasting failure does: OnError, with the empty
// value read as OnErrorFail.
func (p StepPolicy) EffectiveOnError() OnError {
	if p.OnError == "" {
		return OnErrorFail
	}
	return p.OnError
}

// Equal reports whether two policies make a step behave the same.
func (p StepPolicy) Equal(other StepPolicy) bool {
	return p.Retry == other.Retry && p.EffectiveOnError() == other.EffectiveOnError()
}

// ToNotation writes the policy as notation writes it after a step alias:
// `{retry=3}`, `{on_error=absent}`, both in that order, or nothing for the
// default.
func (p StepPolicy) ToNotation() string {
	var entries []string
	if p.Retry > 0 {
		entries = append(entries, "retry="+strconv.FormatUint(uint64(p.Retry), 10))
	}
	if p.EffectiveOnError() == OnErrorAbsent {
		entries = append(entries, "on_error=absent")
	}
	if len(entries) == 0 {
		return ""
	}
	return "{" + strings.Join(entries, ", ") + "}"
}

// stepPolicyWire is the policy as a plan carries it.
type stepPolicyWire struct {
	Retry   uint32   `json:"retry"`
	OnError *OnError `json:"on_error"`
}

// MarshalJSON writes both fields, the empty OnError as "fail".
func (p StepPolicy) MarshalJSON() ([]byte, error) {
	onError := p.EffectiveOnError()
	return json.Marshal(stepPolicyWire{Retry: p.Retry, OnError: &onError})
}

// UnmarshalJSON reads a policy. A field left out is its default; an
// `on_error` this library does not know is refused — a policy nobody here can
// follow is not a policy.
func (p *StepPolicy) UnmarshalJSON(data []byte) error {
	var wire stepPolicyWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	onError := OnErrorFail
	if wire.OnError != nil {
		onError = *wire.OnError
	}
	if onError != OnErrorFail && onError != OnErrorAbsent {
		return fmt.Errorf("unknown on_error: %q", string(onError))
	}
	*p = StepPolicy{Retry: wire.Retry, OnError: onError}
	return nil
}
