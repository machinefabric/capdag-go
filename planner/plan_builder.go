package planner

import (
	"encoding/json"
	"fmt"

	"github.com/machinefabric/capdag-go/media"
)

// ArgumentResolution says where a step's argument gets its value in a run of
// a plan. Mirrors Rust planner::plan_builder::ArgumentResolution; the analysis
// that produces it reads a plan, and plans are built by the reference only.
type ArgumentResolution int

const (
	// ResolutionFromInput: wired from an input of the machine (through a
	// map's item or a gather of inputs, when it is reached through one).
	ResolutionFromInput ArgumentResolution = iota
	// ResolutionFromStep: wired from what another step produces.
	ResolutionFromStep
	// ResolutionConstant: bound to a constant the machine writes.
	ResolutionConstant
	// ResolutionHasDefault: not bound by the machine; its definition gives a
	// default.
	ResolutionHasDefault
	// ResolutionRequiresUserInput: not bound by the machine and no default.
	ResolutionRequiresUserInput
)

var argumentResolutionNames = map[ArgumentResolution]string{
	ResolutionFromInput:         "from_input",
	ResolutionFromStep:          "from_step",
	ResolutionConstant:          "constant",
	ResolutionHasDefault:        "has_default",
	ResolutionRequiresUserInput: "requires_user_input",
}

// String returns the snake_case name. A value outside the enumeration is a
// programming error and panics rather than naming something it is not.
func (r ArgumentResolution) String() string {
	name, ok := argumentResolutionNames[r]
	if !ok {
		panic(fmt.Sprintf("ArgumentResolution %d is not a resolution", int(r)))
	}
	return name
}

// IsBound reports whether the machine itself binds the arg. A bound arg takes
// no value from the run.
func (r ArgumentResolution) IsBound() bool {
	return r == ResolutionFromInput || r == ResolutionFromStep || r == ResolutionConstant
}

// MarshalJSON implements json.Marshaler, serializing as a snake_case string
// mirroring serde's `rename_all = "snake_case"`.
func (r ArgumentResolution) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

// UnmarshalJSON implements json.Unmarshaler, parsing a snake_case string.
func (r *ArgumentResolution) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	for resolution, name := range argumentResolutionNames {
		if name == s {
			*r = resolution
			return nil
		}
	}
	return fmt.Errorf("unknown ArgumentResolution: %s", s)
}

// ArgumentInfo holds full argument metadata for one cap argument.
type ArgumentInfo struct {
	// Name is the arg's name in its cap definition: a label, unique within
	// the cap.
	Name         string             `json:"name"`
	MediaUrn     string             `json:"media_urn"`
	Description  string             `json:"description"`
	Resolution   ArgumentResolution `json:"resolution"`
	DefaultValue any                `json:"default_value,omitempty"`
	IsRequired   bool               `json:"is_required"`
	// IsSequence indicates whether this argument carries a sequence of items.
	IsSequence bool `json:"is_sequence"`
	// IsMainInput reports whether this argument is the cap's MAIN input: it
	// declares a Stdin source whose URN is tagged-URN-equivalent to the cap
	// URN's in= spec. Mirrors Rust planner::plan_builder::ArgumentInfo::is_main_input.
	IsMainInput bool `json:"is_main_input"`
	Validation  any  `json:"validation,omitempty"`
}

// StepArgumentRequirements holds argument info for one step of a plan.
type StepArgumentRequirements struct {
	CapUrn string `json:"cap_urn"`
	// TokenId is the token of the plan's cap node these requirements describe
	// — the ONLY address an argument value is ever bound to. A token exists
	// because a plan exists; it is never derived from a position, a notation
	// string, or anything else. Positions cannot address a step because a plan
	// is a DAG: parallel branches merging downstream have no ordinal, and two
	// identical caps on separate branches differ only by token.
	TokenId   StepToken       `json:"token_id"`
	Title     string          `json:"title"`
	Arguments []*ArgumentInfo `json:"arguments"`
	// Slots are the arguments the machine does not bind: the ones a run may
	// give a value (and must, when they have no default).
	Slots []*ArgumentInfo `json:"slots"`
	// SupportedModelTypes are the architecture identifiers (config.json
	// model_type) the cap can run. Empty when the cap declares no restriction.
	SupportedModelTypes []string `json:"supported_model_types"`
	// DefaultModelSpec is the default model spec literal declared in the cap's
	// capfab toml. Nil when the cap has no default model.
	DefaultModelSpec *string `json:"default_model_spec,omitempty"`
}

// PlanArgumentRequirements holds argument info for a whole plan: one entry per
// cap node, in the plan's topological order.
type PlanArgumentRequirements struct {
	Steps []*StepArgumentRequirements `json:"steps"`
	// CanExecuteWithoutInput reports whether a run of the plan needs no value
	// from the user: every required arg the machine leaves unbound has a
	// default.
	CanExecuteWithoutInput bool `json:"can_execute_without_input"`
}

// ValidationToJSON converts a MediaValidation to a JSON object, or nil if the
// validation has no constraints. Mirrors Rust's MachinePlanBuilder::validation_to_json.
func ValidationToJSON(v *media.MediaValidation) json.RawMessage {
	if v == nil {
		return nil
	}
	if v.Min == nil && v.Max == nil && v.MinLength == nil && v.MaxLength == nil &&
		v.Pattern == nil && len(v.AllowedValues) == 0 {
		return nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		panic("ValidationToJSON: failed to marshal MediaValidation: " + err.Error())
	}
	return data
}
