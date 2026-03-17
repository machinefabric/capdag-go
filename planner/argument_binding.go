package planner

import (
	"encoding/json"
	"fmt"
	"path/filepath"
)

// SourceEntityType identifies the origin of a CapInputFile.
type SourceEntityType int

const (
	SourceListing   SourceEntityType = iota
	SourceCapOutput
	SourceChip
	SourceBlock
	SourceTemporary
)

// String returns the snake_case string for JSON serialization.
func (s SourceEntityType) String() string {
	switch s {
	case SourceListing:
		return "listing"
	case SourceCapOutput:
		return "cap_output"
	case SourceChip:
		return "chip"
	case SourceBlock:
		return "block"
	case SourceTemporary:
		return "temporary"
	default:
		return "listing"
	}
}

// MarshalJSON implements json.Marshaler.
func (s SourceEntityType) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *SourceEntityType) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	switch str {
	case "listing":
		*s = SourceListing
	case "cap_output":
		*s = SourceCapOutput
	case "chip":
		*s = SourceChip
	case "block":
		*s = SourceBlock
	case "temporary":
		*s = SourceTemporary
	default:
		return fmt.Errorf("unknown SourceEntityType: %s", str)
	}
	return nil
}

// CapFileMetadata holds optional metadata for a CapInputFile.
type CapFileMetadata struct {
	Filename  *string          `json:"filename,omitempty"`
	SizeBytes *uint64          `json:"size_bytes,omitempty"`
	MimeType  *string          `json:"mime_type,omitempty"`
	Extra     *json.RawMessage `json:"extra,omitempty"`
}

// CapInputFile is the uniform file representation passed to every cap.
// Caps never see listings, chips, or blocks directly.
type CapInputFile struct {
	FilePath         string            `json:"file_path"`
	MediaUrn         string            `json:"media_urn"`
	Metadata         *CapFileMetadata  `json:"metadata,omitempty"`
	SourceID         *string           `json:"source_id,omitempty"`
	SourceType       *SourceEntityType `json:"source_type,omitempty"`
	TrackedFileID    *string           `json:"tracked_file_id,omitempty"`
	SecurityBookmark []byte            `json:"-"` // runtime-only, never serialized
	OriginalPath     *string           `json:"original_path,omitempty"`
}

// NewCapInputFile creates a CapInputFile with just file_path and media_urn.
func NewCapInputFile(filePath, mediaUrn string) *CapInputFile {
	return &CapInputFile{
		FilePath: filePath,
		MediaUrn: mediaUrn,
	}
}

// CapInputFileFromListing creates a CapInputFile sourced from a listing.
func CapInputFileFromListing(listingID, filePath, mediaUrn string) *CapInputFile {
	st := SourceListing
	return &CapInputFile{
		FilePath:   filePath,
		MediaUrn:   mediaUrn,
		SourceID:   &listingID,
		SourceType: &st,
	}
}

// CapInputFileFromChip creates a CapInputFile sourced from a chip.
func CapInputFileFromChip(chipID, cachePath, mediaUrn string) *CapInputFile {
	st := SourceChip
	return &CapInputFile{
		FilePath:   cachePath,
		MediaUrn:   mediaUrn,
		SourceID:   &chipID,
		SourceType: &st,
	}
}

// CapInputFileFromCapOutput creates a CapInputFile sourced from cap output.
func CapInputFileFromCapOutput(outputPath, mediaUrn string) *CapInputFile {
	st := SourceCapOutput
	return &CapInputFile{
		FilePath:   outputPath,
		MediaUrn:   mediaUrn,
		SourceType: &st,
	}
}

// WithMetadata sets metadata on the file (builder pattern).
func (f *CapInputFile) WithMetadata(metadata *CapFileMetadata) *CapInputFile {
	f.Metadata = metadata
	return f
}

// WithFileReference sets tracked file reference fields (builder pattern).
func (f *CapInputFile) WithFileReference(trackedFileID string, securityBookmark []byte, originalPath string) *CapInputFile {
	f.TrackedFileID = &trackedFileID
	f.SecurityBookmark = securityBookmark
	f.OriginalPath = &originalPath
	return f
}

// Filename extracts the basename from the file path.
func (f *CapInputFile) Filename() *string {
	base := filepath.Base(f.FilePath)
	if base == "" || base == "." || base == "/" {
		return nil
	}
	return &base
}

// HasFileReference returns true if both TrackedFileID and SecurityBookmark are set.
func (f *CapInputFile) HasFileReference() bool {
	return f.TrackedFileID != nil && f.SecurityBookmark != nil
}

// ArgumentSource tags where a resolved argument value came from.
type ArgumentSource int

const (
	SourceArgInputFile       ArgumentSource = iota
	SourceArgPreviousOutput
	SourceArgCapDefault
	SourceArgCapSetting
	SourceArgLiteral
	SourceArgSlot
	SourceArgPlanMetadata
)

// ArgumentBindingKind identifies the type of argument binding.
type ArgumentBindingKind int

const (
	BindingInputFile     ArgumentBindingKind = iota
	BindingInputFilePath
	BindingInputMediaUrn
	BindingPreviousOutput
	BindingCapDefault
	BindingCapSetting
	BindingLiteral
	BindingSlot
	BindingPlanMetadata
)

// ArgumentBinding describes how to resolve one argument value.
type ArgumentBinding struct {
	Kind        ArgumentBindingKind
	Index       int              // for InputFile
	NodeID      string           // for PreviousOutput
	OutputField *string          // for PreviousOutput (optional)
	SettingUrn  string           // for CapSetting
	Value       json.RawMessage  // for Literal (JSON value)
	SlotName    string           // for Slot
	Schema      *json.RawMessage // for Slot (optional)
	Key         string           // for PlanMetadata
}

// NewInputFileBinding creates an InputFile binding.
func NewInputFileBinding(index int) *ArgumentBinding {
	return &ArgumentBinding{Kind: BindingInputFile, Index: index}
}

// NewInputFilePathBinding creates an InputFilePath binding.
func NewInputFilePathBinding() *ArgumentBinding {
	return &ArgumentBinding{Kind: BindingInputFilePath}
}

// NewInputMediaUrnBinding creates an InputMediaUrn binding.
func NewInputMediaUrnBinding() *ArgumentBinding {
	return &ArgumentBinding{Kind: BindingInputMediaUrn}
}

// NewPreviousOutputBinding creates a PreviousOutput binding.
func NewPreviousOutputBinding(nodeID string, outputField *string) *ArgumentBinding {
	return &ArgumentBinding{Kind: BindingPreviousOutput, NodeID: nodeID, OutputField: outputField}
}

// NewCapDefaultBinding creates a CapDefault binding.
func NewCapDefaultBinding() *ArgumentBinding {
	return &ArgumentBinding{Kind: BindingCapDefault}
}

// NewCapSettingBinding creates a CapSetting binding.
func NewCapSettingBinding(settingUrn string) *ArgumentBinding {
	return &ArgumentBinding{Kind: BindingCapSetting, SettingUrn: settingUrn}
}

// NewLiteralBinding creates a Literal binding from a JSON value.
func NewLiteralBinding(value json.RawMessage) *ArgumentBinding {
	return &ArgumentBinding{Kind: BindingLiteral, Value: value}
}

// NewLiteralStringBinding creates a Literal binding from a string.
func NewLiteralStringBinding(s string) *ArgumentBinding {
	data, _ := json.Marshal(s)
	return &ArgumentBinding{Kind: BindingLiteral, Value: data}
}

// NewLiteralNumberBinding creates a Literal binding from an int64.
func NewLiteralNumberBinding(n int64) *ArgumentBinding {
	data, _ := json.Marshal(n)
	return &ArgumentBinding{Kind: BindingLiteral, Value: data}
}

// NewLiteralBoolBinding creates a Literal binding from a bool.
func NewLiteralBoolBinding(b bool) *ArgumentBinding {
	data, _ := json.Marshal(b)
	return &ArgumentBinding{Kind: BindingLiteral, Value: data}
}

// NewSlotBinding creates a Slot binding.
func NewSlotBinding(name string, schema *json.RawMessage) *ArgumentBinding {
	return &ArgumentBinding{Kind: BindingSlot, SlotName: name, Schema: schema}
}

// NewPlanMetadataBinding creates a PlanMetadata binding.
func NewPlanMetadataBinding(key string) *ArgumentBinding {
	return &ArgumentBinding{Kind: BindingPlanMetadata, Key: key}
}

// RequiresInput returns true only for Slot bindings.
func (b *ArgumentBinding) RequiresInput() bool {
	return b.Kind == BindingSlot
}

// ReferencesPrevious returns true only for PreviousOutput bindings.
func (b *ArgumentBinding) ReferencesPrevious() bool {
	return b.Kind == BindingPreviousOutput
}

// MarshalJSON implements json.Marshaler for tagged serialization.
func (b *ArgumentBinding) MarshalJSON() ([]byte, error) {
	switch b.Kind {
	case BindingInputFile:
		return json.Marshal(struct {
			Type  string `json:"type"`
			Index int    `json:"index"`
		}{"input_file", b.Index})
	case BindingInputFilePath:
		return json.Marshal(struct {
			Type string `json:"type"`
		}{"input_file_path"})
	case BindingInputMediaUrn:
		return json.Marshal(struct {
			Type string `json:"type"`
		}{"input_media_urn"})
	case BindingPreviousOutput:
		type po struct {
			Type        string  `json:"type"`
			NodeID      string  `json:"node_id"`
			OutputField *string `json:"output_field,omitempty"`
		}
		return json.Marshal(po{"previous_output", b.NodeID, b.OutputField})
	case BindingCapDefault:
		return json.Marshal(struct {
			Type string `json:"type"`
		}{"cap_default"})
	case BindingCapSetting:
		return json.Marshal(struct {
			Type       string `json:"type"`
			SettingUrn string `json:"setting_urn"`
		}{"cap_setting", b.SettingUrn})
	case BindingLiteral:
		// Embed the raw JSON value
		result := fmt.Appendf(nil, `{"type":"literal","value":%s}`, string(b.Value))
		return result, nil
	case BindingSlot:
		type sl struct {
			Type   string           `json:"type"`
			Name   string           `json:"name"`
			Schema *json.RawMessage `json:"schema,omitempty"`
		}
		return json.Marshal(sl{"slot", b.SlotName, b.Schema})
	case BindingPlanMetadata:
		return json.Marshal(struct {
			Type string `json:"type"`
			Key  string `json:"key"`
		}{"plan_metadata", b.Key})
	default:
		return nil, fmt.Errorf("unknown binding kind: %d", b.Kind)
	}
}

// UnmarshalJSON implements json.Unmarshaler for tagged deserialization.
func (b *ArgumentBinding) UnmarshalJSON(data []byte) error {
	var typed struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &typed); err != nil {
		return err
	}
	switch typed.Type {
	case "input_file":
		var v struct {
			Index int `json:"index"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		b.Kind = BindingInputFile
		b.Index = v.Index
	case "input_file_path":
		b.Kind = BindingInputFilePath
	case "input_media_urn":
		b.Kind = BindingInputMediaUrn
	case "previous_output":
		var v struct {
			NodeID      string  `json:"node_id"`
			OutputField *string `json:"output_field"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		b.Kind = BindingPreviousOutput
		b.NodeID = v.NodeID
		b.OutputField = v.OutputField
	case "cap_default":
		b.Kind = BindingCapDefault
	case "cap_setting":
		var v struct {
			SettingUrn string `json:"setting_urn"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		b.Kind = BindingCapSetting
		b.SettingUrn = v.SettingUrn
	case "literal":
		var v struct {
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		b.Kind = BindingLiteral
		b.Value = v.Value
	case "slot":
		var v struct {
			Name   string           `json:"name"`
			Schema *json.RawMessage `json:"schema"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		b.Kind = BindingSlot
		b.SlotName = v.Name
		b.Schema = v.Schema
	case "plan_metadata":
		var v struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		b.Kind = BindingPlanMetadata
		b.Key = v.Key
	default:
		return fmt.Errorf("unknown ArgumentBinding type: %s", typed.Type)
	}
	return nil
}

// ResolvedArgument is a fully-resolved argument ready to pass to a cap.
type ResolvedArgument struct {
	Name   string
	Value  []byte
	Source ArgumentSource
}

// ArgumentResolutionContext holds the context for resolving argument bindings.
type ArgumentResolutionContext struct {
	InputFiles       []*CapInputFile
	CurrentFileIndex int
	PreviousOutputs  map[string]json.RawMessage
	PlanMetadata     map[string]json.RawMessage
	CapSettings      map[string]map[string]json.RawMessage // cap_urn -> setting_urn -> value
	SlotValues       map[string][]byte                     // "{node_id}:{slot_name}" -> raw bytes (e.g., "step_0:media:width")
}

// NewArgumentResolutionContext creates a minimal context with just input files.
func NewArgumentResolutionContext(inputFiles []*CapInputFile) *ArgumentResolutionContext {
	return &ArgumentResolutionContext{
		InputFiles:      inputFiles,
		PreviousOutputs: make(map[string]json.RawMessage),
	}
}

// CurrentFile returns the current file being processed, or nil.
func (c *ArgumentResolutionContext) CurrentFile() *CapInputFile {
	if c.CurrentFileIndex >= 0 && c.CurrentFileIndex < len(c.InputFiles) {
		return c.InputFiles[c.CurrentFileIndex]
	}
	return nil
}

// ArgumentBindings is a collection of named argument bindings for one cap node.
type ArgumentBindings struct {
	Bindings map[string]*ArgumentBinding `json:"bindings"`
}

// NewArgumentBindings creates an empty ArgumentBindings.
func NewArgumentBindings() *ArgumentBindings {
	return &ArgumentBindings{
		Bindings: make(map[string]*ArgumentBinding),
	}
}

// Add inserts a named binding.
func (ab *ArgumentBindings) Add(name string, binding *ArgumentBinding) {
	ab.Bindings[name] = binding
}

// AddFilePath inserts an InputFilePath binding.
func (ab *ArgumentBindings) AddFilePath(argName string) {
	ab.Bindings[argName] = NewInputFilePathBinding()
}

// AddLiteral inserts a Literal binding from a JSON value.
func (ab *ArgumentBindings) AddLiteral(argName string, value json.RawMessage) {
	ab.Bindings[argName] = NewLiteralBinding(value)
}

// HasUnresolvedSlots returns true if any binding requires external input.
func (ab *ArgumentBindings) HasUnresolvedSlots() bool {
	for _, b := range ab.Bindings {
		if b.RequiresInput() {
			return true
		}
	}
	return false
}

// GetUnresolvedSlots returns names of bindings that require external input.
func (ab *ArgumentBindings) GetUnresolvedSlots() []string {
	var result []string
	for name, b := range ab.Bindings {
		if b.RequiresInput() {
			result = append(result, name)
		}
	}
	return result
}

// ResolveAll resolves all bindings, returning resolved arguments.
// Optional slots with no value are silently skipped.
func (ab *ArgumentBindings) ResolveAll(
	ctx *ArgumentResolutionContext,
	capUrn string,
	nodeID string,
	capDefaults map[string]json.RawMessage,
	argRequired map[string]bool,
) ([]*ResolvedArgument, error) {
	var results []*ResolvedArgument
	for name, binding := range ab.Bindings {
		var defaultValue json.RawMessage
		if capDefaults != nil {
			defaultValue = capDefaults[name]
		}
		isRequired := true
		if argRequired != nil {
			if req, ok := argRequired[name]; ok {
				isRequired = req
			}
		}

		resolved, err := ResolveBinding(binding, ctx, capUrn, nodeID, defaultValue, isRequired)
		if err != nil {
			return nil, err
		}
		if resolved != nil {
			resolved.Name = name
			results = append(results, resolved)
		}
	}
	return results, nil
}

// CapChainInput is the input specification for a cap chain at execution start.
type CapChainInput struct {
	Files            []*CapInputFile  `json:"files"`
	ExpectedMediaUrn string           `json:"expected_media_urn"`
	Cardinality      InputCardinality `json:"cardinality"`
}

// NewSingleCapChainInput creates a CapChainInput for a single file.
func NewSingleCapChainInput(file *CapInputFile) *CapChainInput {
	return &CapChainInput{
		Files:            []*CapInputFile{file},
		ExpectedMediaUrn: file.MediaUrn,
		Cardinality:      CardinalitySingle,
	}
}

// NewSequenceCapChainInput creates a CapChainInput for a sequence of files.
func NewSequenceCapChainInput(files []*CapInputFile, mediaUrn string) *CapChainInput {
	return &CapChainInput{
		Files:            files,
		ExpectedMediaUrn: mediaUrn,
		Cardinality:      CardinalitySequence,
	}
}

// IsValid checks if the input satisfies its cardinality constraint.
func (ci *CapChainInput) IsValid() bool {
	if ci.Cardinality == CardinalitySingle {
		return len(ci.Files) == 1
	}
	// Sequence and AtLeastOne both require non-empty
	return len(ci.Files) > 0
}

// jsonValueToBytes converts a JSON value to bytes.
// Strings are decoded to raw UTF-8 bytes (no JSON quoting).
// Everything else is kept as JSON-encoded bytes.
func jsonValueToBytes(value json.RawMessage) []byte {
	if len(value) == 0 {
		return nil
	}

	// Try to decode as a string first
	var s string
	if err := json.Unmarshal(value, &s); err == nil {
		return []byte(s)
	}

	// Not a string — return the raw JSON bytes
	return []byte(value)
}

// ResolveBinding resolves a single argument binding to a concrete value.
// Returns nil for optional Slot bindings with no available value.
// Returns error for required bindings that cannot be resolved.
func ResolveBinding(
	binding *ArgumentBinding,
	ctx *ArgumentResolutionContext,
	capUrn string,
	nodeID string,
	defaultValue json.RawMessage,
	isRequired bool,
) (*ResolvedArgument, error) {

	switch binding.Kind {
	case BindingInputFile:
		if binding.Index >= len(ctx.InputFiles) {
			return nil, NewInternalError(fmt.Sprintf(
				"Input file index %d out of bounds (have %d files)",
				binding.Index, len(ctx.InputFiles)))
		}
		return &ResolvedArgument{
			Value:  []byte(ctx.InputFiles[binding.Index].FilePath),
			Source: SourceArgInputFile,
		}, nil

	case BindingInputFilePath:
		current := ctx.CurrentFile()
		if current == nil {
			return nil, NewInternalError("No current input file available")
		}
		return &ResolvedArgument{
			Value:  []byte(current.FilePath),
			Source: SourceArgInputFile,
		}, nil

	case BindingInputMediaUrn:
		current := ctx.CurrentFile()
		if current == nil {
			return nil, NewInternalError("No current input file available")
		}
		return &ResolvedArgument{
			Value:  []byte(current.MediaUrn),
			Source: SourceArgInputFile,
		}, nil

	case BindingPreviousOutput:
		outputVal, ok := ctx.PreviousOutputs[binding.NodeID]
		if !ok {
			return nil, NewInternalError(fmt.Sprintf(
				"No previous output for node %q", binding.NodeID))
		}
		if binding.OutputField != nil {
			// Extract a field from the JSON object
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(outputVal, &obj); err != nil {
				return nil, NewInternalError(fmt.Sprintf(
					"Output of node %q is not a JSON object", binding.NodeID))
			}
			fieldVal, ok := obj[*binding.OutputField]
			if !ok {
				return nil, NewInternalError(fmt.Sprintf(
					"Output field %q not found in output of node %q",
					*binding.OutputField, binding.NodeID))
			}
			outputVal = fieldVal
		}
		return &ResolvedArgument{
			Value:  jsonValueToBytes(outputVal),
			Source: SourceArgPreviousOutput,
		}, nil

	case BindingCapDefault:
		if defaultValue == nil {
			return nil, NewInternalError("No default value available for CapDefault binding")
		}
		return &ResolvedArgument{
			Value:  jsonValueToBytes(defaultValue),
			Source: SourceArgCapDefault,
		}, nil

	case BindingCapSetting:
		if ctx.CapSettings == nil {
			return nil, NewInternalError(fmt.Sprintf(
				"No settings available for cap %q", capUrn))
		}
		capSettingsMap, ok := ctx.CapSettings[capUrn]
		if !ok {
			return nil, NewInternalError(fmt.Sprintf(
				"No settings available for cap %q", capUrn))
		}
		settingVal, ok := capSettingsMap[binding.SettingUrn]
		if !ok {
			return nil, NewInternalError(fmt.Sprintf(
				"Setting %q not found for cap %q", binding.SettingUrn, capUrn))
		}
		return &ResolvedArgument{
			Value:  jsonValueToBytes(settingVal),
			Source: SourceArgCapSetting,
		}, nil

	case BindingLiteral:
		return &ResolvedArgument{
			Value:  jsonValueToBytes(binding.Value),
			Source: SourceArgLiteral,
		}, nil

	case BindingSlot:
		slotKey := nodeID + ":" + binding.SlotName

		// Priority 1: slot_values
		if ctx.SlotValues != nil {
			if val, ok := ctx.SlotValues[slotKey]; ok {
				return &ResolvedArgument{
					Value:  val,
					Source: SourceArgSlot,
				}, nil
			}
		}

		// Priority 2: cap_settings[cap_urn][slot_name]
		if ctx.CapSettings != nil {
			if capSettingsMap, ok := ctx.CapSettings[capUrn]; ok {
				if val, ok := capSettingsMap[binding.SlotName]; ok {
					return &ResolvedArgument{
						Value:  jsonValueToBytes(val),
						Source: SourceArgCapSetting,
					}, nil
				}
			}
		}

		// Priority 3: default_value
		if defaultValue != nil {
			return &ResolvedArgument{
				Value:  jsonValueToBytes(defaultValue),
				Source: SourceArgCapDefault,
			}, nil
		}

		// No value found
		if isRequired {
			return nil, NewInternalError(fmt.Sprintf(
				"Required slot %q has no value for cap %q", binding.SlotName, capUrn))
		}
		return nil, nil

	case BindingPlanMetadata:
		if ctx.PlanMetadata == nil {
			return nil, NewInternalError("No plan metadata available")
		}
		val, ok := ctx.PlanMetadata[binding.Key]
		if !ok {
			return nil, NewInternalError(fmt.Sprintf(
				"Plan metadata key %q not found", binding.Key))
		}
		return &ResolvedArgument{
			Value:  jsonValueToBytes(val),
			Source: SourceArgPlanMetadata,
		}, nil

	default:
		return nil, NewInternalError(fmt.Sprintf("Unknown binding kind: %d", binding.Kind))
	}
}
