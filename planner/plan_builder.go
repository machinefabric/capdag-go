package planner

import (
	"encoding/json"
	"fmt"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

// ArgumentResolution describes how an argument will be resolved at execution time.
type ArgumentResolution int

const (
	ResolutionFromInputFile ArgumentResolution = iota
	ResolutionFromPreviousOutput
	ResolutionHasDefault
	ResolutionRequiresUserInput
)

// String returns the snake_case name.
func (r ArgumentResolution) String() string {
	switch r {
	case ResolutionFromInputFile:
		return "from_input_file"
	case ResolutionFromPreviousOutput:
		return "from_previous_output"
	case ResolutionHasDefault:
		return "has_default"
	case ResolutionRequiresUserInput:
		return "requires_user_input"
	default:
		return "requires_user_input"
	}
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
	switch s {
	case "from_input_file":
		*r = ResolutionFromInputFile
	case "from_previous_output":
		*r = ResolutionFromPreviousOutput
	case "has_default":
		*r = ResolutionHasDefault
	case "requires_user_input":
		*r = ResolutionRequiresUserInput
	default:
		return fmt.Errorf("unknown ArgumentResolution: %s", s)
	}
	return nil
}

// ArgumentInfo holds full argument metadata for one cap argument.
type ArgumentInfo struct {
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
	// URN's in= spec. Populated from CapArg.IsMainInput during
	// step-argument-requirements assembly. Mirrors Rust
	// planner::plan_builder::ArgumentInfo::is_main_input.
	IsMainInput bool `json:"is_main_input"`
	Validation  any  `json:"validation,omitempty"`
}

// StepArgumentRequirements holds argument info for one step in a path.
type StepArgumentRequirements struct {
	CapUrn string `json:"cap_urn"`
	// TokenId is the planner-minted StrandStep.TokenId of the step these
	// requirements describe — the ONLY address an argument value is ever bound
	// to. A token exists because a plan exists; it is never derived from a
	// position, a notation string, or anything else. Positions cannot address a
	// step because a strand is a DAG: parallel branches merging downstream have
	// no ordinal, and two identical caps on separate branches differ only by
	// token.
	TokenId   StepToken       `json:"token_id"`
	Title     string          `json:"title"`
	Arguments []*ArgumentInfo `json:"arguments"`
	Slots     []*ArgumentInfo `json:"slots"`
	// SupportedModelTypes are the architecture identifiers (config.json
	// model_type) the cap can run. Empty when the cap declares no restriction.
	SupportedModelTypes []string `json:"supported_model_types"`
	// DefaultModelSpec is the default model spec literal declared in the cap's
	// capfab toml. Nil when the cap has no default model.
	DefaultModelSpec *string `json:"default_model_spec,omitempty"`
}

// PathArgumentRequirements holds argument info for an entire path.
type PathArgumentRequirements struct {
	SourceMediaUrn         string                      `json:"source_media_urn"`
	TargetMediaUrn         string                      `json:"target_media_urn"`
	Steps                  []*StepArgumentRequirements `json:"steps"`
	CanExecuteWithoutInput bool                        `json:"can_execute_without_input"`
}

// MachinePlanBuilder builds execution plans from resolved paths.
type MachinePlanBuilder struct {
	fabricRegistry *fabric.FabricRegistry
}

// NewMachinePlanBuilder creates a new plan builder.
func NewMachinePlanBuilder(fabricRegistry *fabric.FabricRegistry) *MachinePlanBuilder {
	return &MachinePlanBuilder{
		fabricRegistry: fabricRegistry,
	}
}

// AnalyzePathArguments analyzes all argument requirements for a path.
func (b *MachinePlanBuilder) AnalyzePathArguments(
	path *Strand,
) (*PathArgumentRequirements, error) {
	caps := b.fabricRegistry.GetCachedCaps()

	var stepRequirements []*StepArgumentRequirements
	capStepIndex := 0

	for _, step := range path.Steps {
		if step.CapUrnVal == nil {
			continue
		}

		capUrnStr := step.CapUrnVal.String()
		c := findCapInList(caps, capUrnStr)
		if c == nil {
			return nil, NewNotFoundError(fmt.Sprintf("Cap not found: %s", capUrnStr))
		}

		inSpec := c.Urn.InSpec()
		outSpec := c.Urn.OutSpec()
		// Parse the cap's in= spec once for main-input detection. A spec
		// that fails to parse yields nil, and CapArg.IsMainInput(nil) is
		// false — no arg can be the main input of an unparseable in=.
		var inSpecUrn *urn.MediaUrn
		if parsed, err := urn.NewMediaUrnFromString(inSpec); err == nil {
			inSpecUrn = parsed
		}
		var arguments []*ArgumentInfo
		var slots []*ArgumentInfo

		for _, arg := range c.GetArgs() {
			resolution := determineResolutionWithIOCheck(
				arg.MediaUrn, inSpec, outSpec, capStepIndex, arg.DefaultValue)
			description := ""
			if arg.ArgDescription != nil {
				description = *arg.ArgDescription
			}

			argInfo := &ArgumentInfo{
				Name:         arg.MediaUrn,
				MediaUrn:     arg.MediaUrn,
				Description:  description,
				Resolution:   resolution,
				DefaultValue: arg.DefaultValue,
				IsRequired:   arg.Required,
				IsSequence:   arg.IsSequence,
				IsMainInput:  arg.IsMainInput(inSpecUrn),
			}

			isIOArg := resolution == ResolutionFromInputFile || resolution == ResolutionFromPreviousOutput
			if !isIOArg {
				slots = append(slots, argInfo)
			}
			arguments = append(arguments, argInfo)
		}

		stepRequirements = append(stepRequirements, &StepArgumentRequirements{
			CapUrn:              capUrnStr,
			TokenId:             step.TokenId,
			Title:               step.Title(),
			Arguments:           arguments,
			Slots:               slots,
			SupportedModelTypes: c.SupportedModelTypes,
			DefaultModelSpec:    c.DefaultModelSpec,
		})
		capStepIndex++
	}

	canExec := true
	for _, s := range stepRequirements {
		if len(s.Slots) > 0 {
			canExec = false
			break
		}
	}

	return &PathArgumentRequirements{
		SourceMediaUrn:         path.SourceMediaUrn.String(),
		TargetMediaUrn:         path.TargetMediaUrn.String(),
		Steps:                  stepRequirements,
		CanExecuteWithoutInput: canExec,
	}, nil
}

// determineResolutionWithIOCheck determines how an argument will be resolved.
func determineResolutionWithIOCheck(
	mediaUrnStr, inSpec, outSpec string,
	stepIndex int,
	defaultValue any,
) ArgumentResolution {
	// 1. Input spec match
	if mediaUrnStr == inSpec {
		if stepIndex == 0 {
			return ResolutionFromInputFile
		}
		return ResolutionFromPreviousOutput
	}

	// 2. Output spec match
	if mediaUrnStr == outSpec {
		return ResolutionFromPreviousOutput
	}

	// 3. File-path type
	mu, err := urn.NewMediaUrnFromString(mediaUrnStr)
	if err == nil && mu.IsFilePath() {
		if stepIndex == 0 {
			return ResolutionFromInputFile
		}
		return ResolutionFromPreviousOutput
	}

	// 4. Default or user input
	if defaultValue != nil {
		return ResolutionHasDefault
	}
	return ResolutionRequiresUserInput
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

// findCapInList finds a cap in a list by URN string.
func findCapInList(caps []*cap.Cap, capUrnStr string) *cap.Cap {
	for _, c := range caps {
		if c.Urn.String() == capUrnStr {
			return c
		}
	}
	return nil
}
