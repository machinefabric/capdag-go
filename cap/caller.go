// Package capdag provides cap-based execution with strict input validation
package cap

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

// StdinSourceKind identifies the type of stdin source
type StdinSourceKind int

const (
	// StdinSourceKindData represents raw byte data for stdin
	StdinSourceKindData StdinSourceKind = iota
	// StdinSourceKindFileReference represents a file reference for stdin
	// Used for cartridges to read files locally instead of sending bytes over the wire
	StdinSourceKindFileReference
)

// StdinSource represents the source for stdin data.
// For cartridges (via gRPC/XPC), using FileReference avoids size limits
// by letting the receiving side read the file locally.
type StdinSource struct {
	Kind StdinSourceKind

	// Data is the raw byte data (used when Kind == StdinSourceKindData)
	Data []byte

	// FileReference fields (used when Kind == StdinSourceKindFileReference)
	TrackedFileID    string
	OriginalPath     string
	SecurityBookmark []byte
	MediaUrn         string
}

// NewStdinSourceFromData creates a StdinSource from raw bytes
func NewStdinSourceFromData(data []byte) *StdinSource {
	return &StdinSource{
		Kind: StdinSourceKindData,
		Data: data,
	}
}

// NewStdinSourceFromFileReference creates a StdinSource from a file reference
func NewStdinSourceFromFileReference(trackedFileID, originalPath string, securityBookmark []byte, mediaUrn string) *StdinSource {
	return &StdinSource{
		Kind:             StdinSourceKindFileReference,
		TrackedFileID:    trackedFileID,
		OriginalPath:     originalPath,
		SecurityBookmark: securityBookmark,
		MediaUrn:         mediaUrn,
	}
}

// IsData returns true if this is a data source
func (s *StdinSource) IsData() bool {
	return s != nil && s.Kind == StdinSourceKindData
}

// IsFileReference returns true if this is a file reference source
func (s *StdinSource) IsFileReference() bool {
	return s != nil && s.Kind == StdinSourceKindFileReference
}

// CapArgumentValue is a unified argument type — arguments are identified by media_urn.
// The cap definition's sources specify how to extract values (stdin, position, cli_flag).
type CapArgumentValue struct {
	// MediaUrn is the semantic identifier, e.g., "media:model-spec;textable"
	MediaUrn string
	// Value is the argument bytes (UTF-8 for text, raw for binary)
	Value []byte
}

// NewCapArgumentValue creates a new CapArgumentValue
func NewCapArgumentValue(mediaUrn string, value []byte) CapArgumentValue {
	return CapArgumentValue{
		MediaUrn: mediaUrn,
		Value:    value,
	}
}

// NewCapArgumentValueFromStr creates a new CapArgumentValue from a string value
func NewCapArgumentValueFromStr(mediaUrn string, value string) CapArgumentValue {
	return CapArgumentValue{
		MediaUrn: mediaUrn,
		Value:    []byte(value),
	}
}

// ValueAsStr returns the value as a UTF-8 string. Returns error for non-UTF-8 data.
func (a *CapArgumentValue) ValueAsStr() (string, error) {
	if !utf8.Valid(a.Value) {
		return "", fmt.Errorf("value contains invalid UTF-8 data")
	}
	return string(a.Value), nil
}

// String returns a string representation of the CapArgumentValue for debugging
func (a *CapArgumentValue) String() string {
	if utf8.Valid(a.Value) {
		return fmt.Sprintf("CapArgumentValue{MediaUrn: %q, Value: %q}", a.MediaUrn, string(a.Value))
	}
	return fmt.Sprintf("CapArgumentValue{MediaUrn: %q, Value: %d bytes}", a.MediaUrn, len(a.Value))
}

// CapCaller executes caps via host service with strict validation
type CapCaller struct {
	cap           string
	capSet        CapSet
	capDefinition *Cap
}

// CapSet defines the interface for cap host communication
type CapSet interface {
	ExecuteCap(
		ctx context.Context,
		capUrn string,
		arguments []CapArgumentValue,
	) (*HostResult, error)
}

// HostResult represents the result from cap execution
type HostResult struct {
	BinaryOutput []byte
	TextOutput   string
}

// NewCapCaller creates a new cap caller with validation
func NewCapCaller(cap string, capSet CapSet, capDefinition *Cap) *CapCaller {
	return &CapCaller{
		cap:           cap,
		capSet:        capSet,
		capDefinition: capDefinition,
	}
}

// Call executes the cap with arguments identified by media_urn.
// Validates arguments against cap definition before execution.
func (cc *CapCaller) Call(
	ctx context.Context,
	arguments []CapArgumentValue,
	registry *media.MediaUrnRegistry,
) (*ResponseWrapper, error) {
	// Validate arguments against cap definition
	if err := cc.validateArguments(arguments); err != nil {
		return nil, fmt.Errorf("argument validation failed for %s: %w", cc.cap, err)
	}

	// Execute via cap host method
	result, err := cc.capSet.ExecuteCap(
		ctx,
		cc.cap,
		arguments,
	)
	if err != nil {
		return nil, fmt.Errorf("cap execution failed: %w", err)
	}

	// Resolve output spec to determine response type - fail hard if resolution fails
	outputSpec, err := cc.resolveOutputSpec(registry)
	if err != nil {
		return nil, err
	}

	// Determine response type based on what was returned and resolved output spec
	var response *ResponseWrapper
	if len(result.BinaryOutput) > 0 {
		if !outputSpec.IsBinary() {
			return nil, fmt.Errorf("cap %s returned binary data but output spec '%s' (media type: %s) is not binary",
				cc.cap, outputSpec.SpecID, outputSpec.MediaType)
		}
		response = NewResponseWrapperFromBinary(result.BinaryOutput)
	} else if result.TextOutput != "" {
		if outputSpec.IsBinary() {
			return nil, fmt.Errorf("cap %s returned text data but output spec '%s' expects binary",
				cc.cap, outputSpec.SpecID)
		}
		// Structured data (map/list) can be serialized as JSON
		if outputSpec.IsStructured() {
			response = NewResponseWrapperFromJSON([]byte(result.TextOutput))
		} else {
			response = NewResponseWrapperFromText([]byte(result.TextOutput))
		}
	} else {
		return nil, fmt.Errorf("cap returned no output")
	}

	// Validate output against cap definition
	if err := cc.validateOutput(response, registry); err != nil {
		return nil, fmt.Errorf("output validation failed for %s: %w", cc.cap, err)
	}

	return response, nil
}

// resolveOutputSpec resolves the output media URN from the cap URN's out spec.
// This method fails hard if:
// - The cap URN is invalid
// - The media URN cannot be resolved (not in media_specs)
func (cc *CapCaller) resolveOutputSpec(registry *media.MediaUrnRegistry) (*media.ResolvedMediaSpec, error) {
	capUrn, err := urn.NewCapUrnFromString(cc.cap)
	if err != nil {
		return nil, fmt.Errorf("invalid cap URN '%s': %w", cc.cap, err)
	}

	// Get the output media URN - now always present since it's required in parsing
	mediaUrn := capUrn.OutSpec()

	// Resolve the media URN using the cap definition's media_specs
	resolved, err := media.ResolveMediaUrn(mediaUrn, cc.capDefinition.GetMediaSpecs(), registry)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve output media URN '%s' for cap '%s': %w - ensure media_specs contains this media URN", mediaUrn, cc.cap, err)
	}
	return resolved, nil
}

// validateArguments validates arguments against cap definition.
// Checks that all required arguments are provided (by media_urn) and rejects unknown arguments.
func (cc *CapCaller) validateArguments(arguments []CapArgumentValue) error {
	argDefs := cc.capDefinition.GetArgs()

	// Build set of provided media_urns
	providedUrns := make(map[string]bool)
	for _, arg := range arguments {
		providedUrns[arg.MediaUrn] = true
	}

	// Check all required arguments are provided
	for _, argDef := range argDefs {
		if argDef.Required && !providedUrns[argDef.MediaUrn] {
			return fmt.Errorf("missing required argument: %s", argDef.MediaUrn)
		}
	}

	// Check for unknown arguments
	knownUrns := make(map[string]bool)
	for _, argDef := range argDefs {
		knownUrns[argDef.MediaUrn] = true
	}

	for _, arg := range arguments {
		if !knownUrns[arg.MediaUrn] {
			return fmt.Errorf("unknown argument media_urn: %s (cap %s accepts: %v)",
				arg.MediaUrn, cc.cap, knownUrns)
		}
	}

	return nil
}

// validateOutput validates output against cap definition
func (cc *CapCaller) validateOutput(response *ResponseWrapper, registry *media.MediaUrnRegistry) error {
	// Resolve output spec - fail hard if resolution fails
	outputSpec, err := cc.resolveOutputSpec(registry)
	if err != nil {
		return err
	}

	// For binary outputs, check type compatibility
	if response.IsBinary() {
		// Binary validation already done in Call() before creating the response
		return nil
	}

	// For text/JSON outputs, parse and validate
	text, err := response.AsString()
	if err != nil {
		return fmt.Errorf("failed to convert output to string: %w", err)
	}

	var outputValue interface{}
	// For structured outputs (map/list), verify it's valid JSON
	if outputSpec.IsStructured() {
		if err := json.Unmarshal([]byte(text), &outputValue); err != nil {
			return fmt.Errorf("output is not valid JSON for cap %s: %w", cc.cap, err)
		}
	} else {
		outputValue = text
	}

	outputValidator := NewOutputValidator()
	return outputValidator.ValidateOutput(cc.capDefinition, outputValue, registry)
}
