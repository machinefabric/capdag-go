package planner

import (
	"fmt"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/urn"
)

// ArgumentResolution describes how an argument will be resolved at execution time.
type ArgumentResolution int

const (
	ResolutionFromInputFile     ArgumentResolution = iota
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

// ArgumentInfo holds full argument metadata for one cap argument.
type ArgumentInfo struct {
	Name         string `json:"name"`
	MediaUrn     string `json:"media_urn"`
	Description  string `json:"description"`
	Resolution   ArgumentResolution `json:"resolution"`
	DefaultValue any    `json:"default_value,omitempty"`
	IsRequired   bool   `json:"is_required"`
	Validation   any    `json:"validation,omitempty"`
}

// StepArgumentRequirements holds argument info for one step in a path.
type StepArgumentRequirements struct {
	CapUrn    string          `json:"cap_urn"`
	StepIndex int             `json:"step_index"`
	Title     string          `json:"title"`
	Arguments []*ArgumentInfo `json:"arguments"`
	Slots     []*ArgumentInfo `json:"slots"`
}

// PathArgumentRequirements holds argument info for an entire path.
type PathArgumentRequirements struct {
	SourceSpec              string                      `json:"source_spec"`
	TargetSpec              string                      `json:"target_spec"`
	Steps                   []*StepArgumentRequirements `json:"steps"`
	CanExecuteWithoutInput  bool                        `json:"can_execute_without_input"`
}

// CapPlanBuilder builds execution plans from resolved paths.
type CapPlanBuilder struct {
	capRegistry   *cap.CapRegistry
}

// NewCapPlanBuilder creates a new plan builder.
func NewCapPlanBuilder(capRegistry *cap.CapRegistry) *CapPlanBuilder {
	return &CapPlanBuilder{
		capRegistry:   capRegistry,
	}
}

// findFilePathArg finds the first argument that is a file-path type media URN.
func findFilePathArg(c *cap.Cap) *string {
	for _, arg := range c.GetArgs() {
		mediaUrn, err := urn.NewMediaUrnFromString(arg.MediaUrn)
		if err != nil {
			continue
		}
		if mediaUrn.IsAnyFilePath() {
			return &arg.MediaUrn
		}
	}
	return nil
}

// isFilePathStdinChainable checks if the cap's file-path arg accepts stdin with matching in_spec.
func isFilePathStdinChainable(c *cap.Cap) bool {
	inSpec := c.Urn.InSpec()
	for _, arg := range c.GetArgs() {
		mediaUrn, err := urn.NewMediaUrnFromString(arg.MediaUrn)
		if err != nil {
			continue
		}
		if !mediaUrn.IsAnyFilePath() {
			continue
		}
		for _, source := range arg.Sources {
			if source.IsStdin() {
				stdinUrn := source.StdinMediaUrn()
				if stdinUrn != nil && *stdinUrn == inSpec {
					return true
				}
			}
		}
	}
	return false
}

// BuildPlanFromPath builds an execution plan from a resolved path.
func (b *CapPlanBuilder) BuildPlanFromPath(
	name string,
	path *CapChainPathInfo,
	inputCardinality InputCardinality,
) (*CapExecutionPlan, error) {
	plan := NewCapExecutionPlan(name)

	caps := b.capRegistry.GetCachedCaps()

	// Build file-path info: cap_urn -> (file_path_arg_name, stdin_chainable)
	type filePathEntry struct {
		argName        *string
		stdinChainable bool
	}
	filePathInfo := make(map[string]*filePathEntry)

	for _, step := range path.Steps {
		if step.CapUrnVal == nil {
			continue
		}
		capUrnStr := step.CapUrnVal.String()
		c := findCapInList(caps, capUrnStr)
		if c != nil {
			filePathInfo[capUrnStr] = &filePathEntry{
				argName:        findFilePathArg(c),
				stdinChainable: isFilePathStdinChainable(c),
			}
		}
	}

	sourceSpecStr := path.SourceSpec.String()
	inputSlotID := "input_slot"
	plan.AddNode(NewInputSlotNode(inputSlotID, "input", sourceSpecStr, inputCardinality))

	prevNodeID := inputSlotID
	capStepCount := 0
	var insideForEachBody *struct {
		idx    int
		nodeID string
	}
	var bodyEntry *string
	var bodyExit *string

	for i, step := range path.Steps {
		nodeID := fmt.Sprintf("step_%d", i)

		switch step.StepType {
		case StepTypeCap:
			capUrnStr := step.CapUrnVal.String()
			bindings := NewArgumentBindings()

			c := findCapInList(caps, capUrnStr)
			inSpec := ""
			outSpec := ""
			if c != nil {
				inSpec = c.Urn.InSpec()
				outSpec = c.Urn.OutSpec()
			}
			isInsideBody := insideForEachBody != nil

			// File-path arg binding
			if info, ok := filePathInfo[capUrnStr]; ok && info.argName != nil {
				argName := *info.argName
				if capStepCount == 0 && !isInsideBody {
					bindings.AddFilePath(argName)
				} else if info.stdinChainable {
					bindings.Add(argName, NewPreviousOutputBinding(prevNodeID, nil))
				} else {
					bindings.AddFilePath(argName)
				}
			}

			// Slot bindings for non-I/O args
			if c != nil {
				for _, arg := range c.GetArgs() {
					if arg.MediaUrn == inSpec || arg.MediaUrn == outSpec {
						continue
					}
					mu, err := urn.NewMediaUrnFromString(arg.MediaUrn)
					if err == nil && mu.IsAnyFilePath() {
						continue
					}
					if _, exists := bindings.Bindings[arg.MediaUrn]; exists {
						continue
					}
					bindings.Add(arg.MediaUrn, NewSlotBinding(arg.MediaUrn, nil))
				}
			}

			plan.AddNode(NewCapNodeWithBindings(nodeID, capUrnStr, bindings))
			plan.AddEdge(NewDirectEdge(prevNodeID, nodeID))

			if isInsideBody {
				if bodyEntry == nil {
					bodyEntry = &nodeID
				}
				s := nodeID
				bodyExit = &s
			} else {
				capStepCount++
			}

		case StepTypeForEach:
			// If already inside a ForEach body (nested), finalize the outer
			if insideForEachBody != nil {
				outerIdx := insideForEachBody.idx
				outerNodeID := insideForEachBody.nodeID

				if bodyEntry == nil {
					return nil, NewInvalidPathError(fmt.Sprintf(
						"Nested ForEach at step[%d] but outer ForEach at step[%d] ('%s') has no body caps.",
						i, outerIdx, outerNodeID))
				}

				outerEntry := *bodyEntry
				outerExit := prevNodeID
				if bodyExit != nil {
					outerExit = *bodyExit
				}
				outerForEachInput := inputSlotID
				if outerIdx > 0 {
					outerForEachInput = fmt.Sprintf("step_%d", outerIdx-1)
				}

				if outerForEachInput == outerEntry {
					return nil, NewInvalidPathError(fmt.Sprintf(
						"Outer ForEach at step[%d] ('%s') would create a cycle",
						outerIdx, outerNodeID))
				}

				plan.AddNode(NewForEachNode(outerNodeID, outerForEachInput, outerEntry, outerExit))
				plan.AddEdge(NewDirectEdge(outerForEachInput, outerNodeID))
				plan.AddEdge(NewIterationEdge(outerNodeID, outerEntry))
				prevNodeID = outerExit
			}

			insideForEachBody = &struct {
				idx    int
				nodeID string
			}{i, nodeID}
			bodyEntry = nil
			bodyExit = nil
			continue // skip prevNodeID = nodeID

		case StepTypeCollect:
			if insideForEachBody != nil {
				foreachIdx := insideForEachBody.idx
				foreachNodeID := insideForEachBody.nodeID

				entry := prevNodeID
				if bodyEntry != nil {
					entry = *bodyEntry
				}
				exitNode := prevNodeID
				if bodyExit != nil {
					exitNode = *bodyExit
				}
				foreachInput := inputSlotID
				if foreachIdx > 0 {
					foreachInput = fmt.Sprintf("step_%d", foreachIdx-1)
				}

				plan.AddNode(NewForEachNode(foreachNodeID, foreachInput, entry, exitNode))
				plan.AddEdge(NewDirectEdge(foreachInput, foreachNodeID))
				plan.AddEdge(NewIterationEdge(foreachNodeID, entry))

				plan.AddNode(NewCollectNode(nodeID, []string{exitNode}))
				plan.AddEdge(NewCollectionEdge(exitNode, nodeID))

				insideForEachBody = nil
				bodyEntry = nil
				bodyExit = nil
			} else {
				return nil, NewInvalidPathError("Collect step without matching ForEach")
			}

		case StepTypeWrapInList:
			itemSpec := ""
			listSpec := ""
			if step.ItemSpec != nil {
				itemSpec = step.ItemSpec.String()
			}
			if step.ListSpec != nil {
				listSpec = step.ListSpec.String()
			}
			plan.AddNode(NewWrapInListNode(nodeID, itemSpec, listSpec))
			plan.AddEdge(NewDirectEdge(prevNodeID, nodeID))
		}

		prevNodeID = nodeID
	}

	// Handle unclosed ForEach at end
	if insideForEachBody != nil {
		foreachIdx := insideForEachBody.idx
		foreachNodeID := insideForEachBody.nodeID

		if bodyEntry != nil {
			entry := *bodyEntry
			exitNode := prevNodeID
			if bodyExit != nil {
				exitNode = *bodyExit
			}
			foreachInput := inputSlotID
			if foreachIdx > 0 {
				foreachInput = fmt.Sprintf("step_%d", foreachIdx-1)
			}

			if foreachInput == entry {
				return nil, NewInvalidPathError(fmt.Sprintf(
					"ForEach at step[%d] ('%s') would create a cycle",
					foreachIdx, foreachNodeID))
			}

			plan.AddNode(NewForEachNode(foreachNodeID, foreachInput, entry, exitNode))
			plan.AddEdge(NewDirectEdge(foreachInput, foreachNodeID))
			plan.AddEdge(NewIterationEdge(foreachNodeID, entry))
			prevNodeID = exitNode
		}
	}

	// Output node
	plan.AddNode(NewOutputNode("output", "result", prevNodeID))
	plan.AddEdge(NewDirectEdge(prevNodeID, "output"))

	// Metadata
	plan.Metadata = map[string]any{
		"source_spec": sourceSpecStr,
		"target_spec": path.TargetSpec.String(),
	}

	// Validate
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	if _, err := plan.TopologicalOrder(); err != nil {
		return nil, NewInvalidPathError(fmt.Sprintf("Plan has cycle: %s", err.Error()))
	}

	return plan, nil
}

// AnalyzePathArguments analyzes all argument requirements for a path.
func (b *CapPlanBuilder) AnalyzePathArguments(
	path *CapChainPathInfo,
) (*PathArgumentRequirements, error) {
	caps := b.capRegistry.GetCachedCaps()

	var stepRequirements []*StepArgumentRequirements
	capStepIndex := 0

	for i, step := range path.Steps {
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
		var arguments []*ArgumentInfo
		var slots []*ArgumentInfo

		for _, arg := range c.GetArgs() {
			resolution := determineResolutionWithIOCheck(
				arg.MediaUrn, inSpec, outSpec, capStepIndex, arg.DefaultValue)

			argInfo := &ArgumentInfo{
				Name:         arg.MediaUrn,
				MediaUrn:     arg.MediaUrn,
				Description:  arg.ArgDescription,
				Resolution:   resolution,
				DefaultValue: arg.DefaultValue,
				IsRequired:   arg.Required,
			}

			isIOArg := resolution == ResolutionFromInputFile || resolution == ResolutionFromPreviousOutput
			if !isIOArg {
				slots = append(slots, argInfo)
			}
			arguments = append(arguments, argInfo)
		}

		stepRequirements = append(stepRequirements, &StepArgumentRequirements{
			CapUrn:    capUrnStr,
			StepIndex: i,
			Title:     step.Title(),
			Arguments: arguments,
			Slots:     slots,
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
		SourceSpec:             path.SourceSpec.String(),
		TargetSpec:             path.TargetSpec.String(),
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
	if err == nil && mu.IsAnyFilePath() {
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

// findCapInList finds a cap in a list by URN string.
func findCapInList(caps []*cap.Cap, capUrnStr string) *cap.Cap {
	for _, c := range caps {
		if c.Urn.String() == capUrnStr {
			return c
		}
	}
	return nil
}
