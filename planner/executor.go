package planner

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
)

// CapExecutorInterface defines the contract for executing individual caps.
type CapExecutorInterface interface {
	// ExecuteCap executes a cap and returns raw output bytes.
	ExecuteCap(capUrn string, arguments []cap.CapArgumentValue, preferredCap *string) ([]byte, error)
	// HasCap checks if a cap is available.
	HasCap(capUrn string) bool
	// GetCap gets the cap definition.
	GetCap(capUrn string) (*cap.Cap, error)
}

// CapSettingsProviderInterface defines the contract for providing cap settings.
type CapSettingsProviderInterface interface {
	// GetSettings returns settings for a specific cap.
	GetSettings(capUrn string) (map[string]json.RawMessage, error)
}

// MachineExecutor executes a MachinePlan node-by-node in topological order.
type MachineExecutor struct {
	executor         CapExecutorInterface
	plan             *MachinePlan
	inputFiles       []*CapInputFile
	slotValues       map[string][]byte
	settingsProvider CapSettingsProviderInterface
}

// NewMachineExecutor creates a new plan executor.
func NewMachineExecutor(
	executor CapExecutorInterface,
	plan *MachinePlan,
	inputFiles []*CapInputFile,
) *MachineExecutor {
	return &MachineExecutor{
		executor:   executor,
		plan:       plan,
		inputFiles: inputFiles,
		slotValues: make(map[string][]byte),
	}
}

// WithSlotValues sets raw bytes for named slots (builder pattern).
func (pe *MachineExecutor) WithSlotValues(slotValues map[string][]byte) *MachineExecutor {
	pe.slotValues = slotValues
	return pe
}

// WithSettingsProvider sets the settings provider (builder pattern).
func (pe *MachineExecutor) WithSettingsProvider(provider CapSettingsProviderInterface) *MachineExecutor {
	pe.settingsProvider = provider
	return pe
}

// Execute runs the plan and returns results.
func (pe *MachineExecutor) Execute() (*MachineResult, error) {
	start := time.Now()

	if err := pe.plan.Validate(); err != nil {
		return nil, err
	}

	order, err := pe.plan.TopologicalOrder()
	if err != nil {
		return nil, err
	}

	nodeResults := make(map[string]*NodeExecutionResult)
	nodeOutputs := make(map[string]any)

	for _, node := range order {
		execResult, outputVal, err := pe.executeNode(node, nodeResults, nodeOutputs)
		if err != nil {
			elapsedMs := uint64(time.Since(start).Milliseconds())
			return &MachineResult{
				Success:         false,
				NodeResults:     nodeResults,
				Outputs:         make(map[string]any),
				Error:           err.Error(),
				TotalDurationMs: elapsedMs,
			}, nil
		}

		nodeResults[node.ID] = execResult
		if outputVal != nil {
			nodeOutputs[node.ID] = outputVal
		}

		if !execResult.Success {
			elapsedMs := uint64(time.Since(start).Milliseconds())
			return &MachineResult{
				Success:         false,
				NodeResults:     nodeResults,
				Outputs:         make(map[string]any),
				Error:           execResult.Error,
				TotalDurationMs: elapsedMs,
			}, nil
		}
	}

	// Collect outputs from output nodes
	outputs := make(map[string]any)
	for _, outputNodeID := range pe.plan.OutputNodes {
		outputNode := pe.plan.GetNode(outputNodeID)
		if outputNode != nil && outputNode.NodeType.Kind == NodeKindOutput {
			source := outputNode.NodeType.SourceNode
			if val, ok := nodeOutputs[source]; ok {
				outputs[outputNode.NodeType.OutputName] = val
			}
		}
	}

	elapsedMs := uint64(time.Since(start).Milliseconds())
	return &MachineResult{
		Success:         true,
		NodeResults:     nodeResults,
		Outputs:         outputs,
		TotalDurationMs: elapsedMs,
	}, nil
}

func (pe *MachineExecutor) executeNode(
	node *MachineNode,
	_ map[string]*NodeExecutionResult,
	nodeOutputs map[string]any,
) (*NodeExecutionResult, any, error) {
	start := time.Now()

	switch node.NodeType.Kind {
	case NodeKindCap:
		return pe.executeMachineNode(
			node.ID,
			node.NodeType.CapUrn,
			node.NodeType.ArgBindings,
			node.NodeType.PreferredCap,
			nodeOutputs,
		)

	case NodeKindInputSlot:
		var output any
		if len(pe.inputFiles) == 1 {
			output = map[string]any{
				"file_path": pe.inputFiles[0].FilePath,
				"media_urn": pe.inputFiles[0].MediaUrn,
			}
		} else {
			items := make([]any, 0, len(pe.inputFiles))
			for _, f := range pe.inputFiles {
				items = append(items, map[string]any{
					"file_path": f.FilePath,
					"media_urn": f.MediaUrn,
				})
			}
			output = items
		}
		durationMs := uint64(time.Since(start).Milliseconds())
		return &NodeExecutionResult{
			NodeID:     node.ID,
			Success:    true,
			DurationMs: durationMs,
		}, output, nil

	case NodeKindOutput:
		sourceNode := node.NodeType.SourceNode
		output := nodeOutputs[sourceNode]
		durationMs := uint64(time.Since(start).Milliseconds())
		return &NodeExecutionResult{
			NodeID:     node.ID,
			Success:    true,
			DurationMs: durationMs,
		}, output, nil

	case NodeKindForEach:
		inputNode := node.NodeType.InputNode
		bodyEntry := node.NodeType.BodyEntry
		bodyExit := node.NodeType.BodyExit
		inputVal := nodeOutputs[inputNode]
		var items []any
		if arr, ok := inputVal.([]any); ok {
			items = arr
		} else if inputVal != nil {
			items = []any{inputVal}
		}
		output := map[string]any{
			"iteration_count": len(items),
			"items":           items,
			"body_entry":      bodyEntry,
			"body_exit":       bodyExit,
		}
		durationMs := uint64(time.Since(start).Milliseconds())
		return &NodeExecutionResult{
			NodeID:     node.ID,
			Success:    true,
			DurationMs: durationMs,
		}, output, nil

	case NodeKindCollect:
		var collected []any
		for _, inpID := range node.NodeType.InputNodes {
			val := nodeOutputs[inpID]
			if val == nil {
				continue
			}
			if arr, ok := val.([]any); ok {
				collected = append(collected, arr...)
			} else {
				collected = append(collected, val)
			}
		}
		output := map[string]any{
			"collected": collected,
			"count":     len(collected),
		}
		durationMs := uint64(time.Since(start).Milliseconds())
		return &NodeExecutionResult{
			NodeID:     node.ID,
			Success:    true,
			DurationMs: durationMs,
		}, output, nil

	case NodeKindMerge:
		var merged []any
		for _, inpID := range node.NodeType.InputNodes {
			if val, ok := nodeOutputs[inpID]; ok {
				merged = append(merged, val)
			}
		}
		output := map[string]any{
			"merged":   merged,
			"strategy": node.NodeType.MergeStrat.String(),
		}
		durationMs := uint64(time.Since(start).Milliseconds())
		return &NodeExecutionResult{
			NodeID:     node.ID,
			Success:    true,
			DurationMs: durationMs,
		}, output, nil

	case NodeKindSplit:
		inputNode := node.NodeType.InputNode
		inputVal := nodeOutputs[inputNode]
		output := map[string]any{
			"input":        inputVal,
			"output_count": node.NodeType.OutputCount,
		}
		durationMs := uint64(time.Since(start).Milliseconds())
		return &NodeExecutionResult{
			NodeID:     node.ID,
			Success:    true,
			DurationMs: durationMs,
		}, output, nil

	default:
		return nil, nil, NewInternalError(fmt.Sprintf("Unknown node kind: %d", node.NodeType.Kind))
	}
}

func (pe *MachineExecutor) executeMachineNode(
	nodeID, capUrn string,
	argBindings *ArgumentBindings,
	preferredCap *string,
	nodeOutputs map[string]any,
) (*NodeExecutionResult, any, error) {
	start := time.Now()

	// Check availability
	if !pe.executor.HasCap(capUrn) {
		durationMs := uint64(time.Since(start).Milliseconds())
		return &NodeExecutionResult{
			NodeID:     nodeID,
			Success:    false,
			Error:      fmt.Sprintf("No capability available for '%s'", capUrn),
			DurationMs: durationMs,
		}, nil, nil
	}

	// Get cap definition
	capDef, err := pe.executor.GetCap(capUrn)
	if err != nil {
		return nil, nil, err
	}
	capArgs := capDef.GetArgs()

	// Build arg defaults and required maps
	argDefaults := make(map[string]json.RawMessage)
	argRequired := make(map[string]bool)
	for _, arg := range capArgs {
		if arg.DefaultValue != nil {
			data, err := json.Marshal(arg.DefaultValue)
			if err == nil {
				argDefaults[arg.MediaUrn] = data
			}
		}
		argRequired[arg.MediaUrn] = arg.Required
	}

	// Load cap settings from provider
	var capSettingsMap map[string]map[string]json.RawMessage
	if pe.settingsProvider != nil {
		settings, err := pe.settingsProvider.GetSettings(capUrn)
		if err == nil && len(settings) > 0 {
			capSettingsMap = map[string]map[string]json.RawMessage{capUrn: settings}
		}
	}

	// Convert node outputs to json.RawMessage for context
	previousOutputs := make(map[string]json.RawMessage)
	for k, v := range nodeOutputs {
		data, err := json.Marshal(v)
		if err == nil {
			previousOutputs[k] = data
		}
	}

	// Build resolution context
	ctx := &ArgumentResolutionContext{
		InputFiles:      pe.inputFiles,
		PreviousOutputs: previousOutputs,
		CapSettings:     capSettingsMap,
	}
	if len(pe.slotValues) > 0 {
		ctx.SlotValues = pe.slotValues
	}

	// Resolve each binding
	var arguments []cap.CapArgumentValue
	if argBindings != nil {
		for name, binding := range argBindings.Bindings {
			isRequired := argRequired[name]
			resolved, err := ResolveBinding(binding, ctx, capUrn, nodeID, argDefaults[name], isRequired)
			if err != nil {
				return nil, nil, NewInternalError(fmt.Sprintf(
					"Failed to resolve argument '%s' for cap '%s': %s", name, capUrn, err.Error()))
			}
			if resolved != nil {
				argMediaUrn := name
				if resolved.Source == SourceArgInputFile {
					argMediaUrn = standard.MediaFilePath
				}
				arguments = append(arguments, cap.NewCapArgumentValue(argMediaUrn, resolved.Value))
			}
		}
	}

	// Implicit stdin injection
	stdinArgAlreadyBound := false
	hasFilePathBinding := false
	if argBindings != nil {
		for _, arg := range capArgs {
			hasStdinSource := false
			for _, s := range arg.Sources {
				if s.IsStdin() {
					hasStdinSource = true
					break
				}
			}
			if hasStdinSource {
				if _, bound := argBindings.Bindings[arg.MediaUrn]; bound {
					stdinArgAlreadyBound = true
				}
			}
		}
		for _, b := range argBindings.Bindings {
			if b.Kind == BindingInputFilePath {
				hasFilePathBinding = true
				break
			}
		}
	}

	if len(pe.inputFiles) > 0 && capDef.AcceptsStdin() && !stdinArgAlreadyBound && !hasFilePathBinding {
		inputFile := pe.inputFiles[0]
		stdinMediaUrn := inputFile.MediaUrn
		if su := capDef.GetStdinMediaUrn(); su != nil {
			stdinMediaUrn = *su
		}
		data, err := os.ReadFile(inputFile.FilePath)
		if err != nil {
			return nil, nil, NewInternalError(fmt.Sprintf("Failed to read input file: %s", err.Error()))
		}
		arguments = append(arguments, cap.NewCapArgumentValue(stdinMediaUrn, data))
	}

	// Execute
	responseBytes, err := pe.executor.ExecuteCap(capUrn, arguments, preferredCap)
	if err != nil {
		durationMs := uint64(time.Since(start).Milliseconds())
		return &NodeExecutionResult{
			NodeID:     nodeID,
			Success:    false,
			Error:      err.Error(),
			DurationMs: durationMs,
		}, nil, nil
	}

	// Process result
	durationMs := uint64(time.Since(start).Milliseconds())
	textOutput := string(responseBytes)

	var outputJSON any
	if err := json.Unmarshal(responseBytes, &outputJSON); err != nil {
		outputJSON = map[string]any{"text": textOutput}
	}

	return &NodeExecutionResult{
		NodeID:         nodeID,
		Success:        true,
		BinaryOutput:   responseBytes,
		MediaUrnOutput: "", // populated by pipeline executor from STREAM_START metadata
		DurationMs:     durationMs,
	}, outputJSON, nil
}

// ApplyEdgeType applies an edge type transformation to source output.
func ApplyEdgeType(sourceOutput any, edgeType *EdgeType) (any, error) {
	switch edgeType.Kind {
	case EdgeKindDirect, EdgeKindIteration, EdgeKindCollection:
		return sourceOutput, nil
	case EdgeKindJsonField:
		m, ok := sourceOutput.(map[string]any)
		if !ok {
			return nil, NewInternalError(fmt.Sprintf("Field '%s' not found in source output", edgeType.Field))
		}
		val, exists := m[edgeType.Field]
		if !exists {
			return nil, NewInternalError(fmt.Sprintf("Field '%s' not found in source output", edgeType.Field))
		}
		return val, nil
	case EdgeKindJsonPath:
		return ExtractJsonPath(sourceOutput, edgeType.Path)
	default:
		return nil, NewInternalError(fmt.Sprintf("Unknown edge type kind: %d", edgeType.Kind))
	}
}

// ExtractJsonPath navigates JSON by dot-separated path with optional array indexing.
func ExtractJsonPath(jsonVal any, path string) (any, error) {
	current := jsonVal
	for _, segment := range strings.Split(path, ".") {
		if bracketIdx := strings.Index(segment, "["); bracketIdx >= 0 {
			fieldName := segment[:bracketIdx]
			indexStr := strings.TrimRight(segment[bracketIdx+1:], "]")

			if fieldName != "" {
				m, ok := current.(map[string]any)
				if !ok {
					return nil, NewInternalError(fmt.Sprintf("Field '%s' not found in path", fieldName))
				}
				val, exists := m[fieldName]
				if !exists {
					return nil, NewInternalError(fmt.Sprintf("Field '%s' not found in path", fieldName))
				}
				current = val
			}

			var idx int
			if _, err := fmt.Sscanf(indexStr, "%d", &idx); err != nil {
				return nil, NewInternalError(fmt.Sprintf("Invalid array index: %s", indexStr))
			}
			arr, ok := current.([]any)
			if !ok || idx >= len(arr) {
				return nil, NewInternalError(fmt.Sprintf("Array index %d out of bounds", idx))
			}
			current = arr[idx]
		} else {
			m, ok := current.(map[string]any)
			if !ok {
				return nil, NewInternalError(fmt.Sprintf("Field '%s' not found in path", segment))
			}
			val, exists := m[segment]
			if !exists {
				return nil, NewInternalError(fmt.Sprintf("Field '%s' not found in path", segment))
			}
			current = val
		}
	}
	return current, nil
}
