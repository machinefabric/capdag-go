// Package capdag provides unified response wrapper for cartridge output handling with validation
package cap

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/machinefabric/capdag-go/media"
)

// ResponseWrapper provides unified response wrapper for all cartridge operations
// Provides type-safe deserialization of cartridge output
type ResponseWrapper struct {
	rawBytes    []byte
	contentType ResponseContentType
}

// ResponseContentType represents the type of content in the response
type ResponseContentType int

const (
	ResponseContentTypeJSON ResponseContentType = iota
	ResponseContentTypeText
	ResponseContentTypeBinary
)

// NewResponseWrapperFromJSON creates a response wrapper from JSON output
func NewResponseWrapperFromJSON(data []byte) *ResponseWrapper {
	return &ResponseWrapper{
		rawBytes:    data,
		contentType: ResponseContentTypeJSON,
	}
}

// NewResponseWrapperFromText creates a response wrapper from text output
func NewResponseWrapperFromText(data []byte) *ResponseWrapper {
	return &ResponseWrapper{
		rawBytes:    data,
		contentType: ResponseContentTypeText,
	}
}

// NewResponseWrapperFromBinary creates a response wrapper from binary output
func NewResponseWrapperFromBinary(data []byte) *ResponseWrapper {
	return &ResponseWrapper{
		rawBytes:    data,
		contentType: ResponseContentTypeBinary,
	}
}

// AsBytes returns the raw bytes
func (rw *ResponseWrapper) AsBytes() []byte {
	return rw.rawBytes
}

// AsString converts the response to string
func (rw *ResponseWrapper) AsString() (string, error) {
	if rw.contentType == ResponseContentTypeBinary {
		return "", fmt.Errorf("cannot convert binary response to string")
	}
	return string(rw.rawBytes), nil
}

// AsInt converts the response to integer
func (rw *ResponseWrapper) AsInt() (int64, error) {
	text, err := rw.AsString()
	if err != nil {
		return 0, err
	}

	text = strings.TrimSpace(text)

	// Try parsing as JSON number first
	var jsonVal interface{}
	if err := json.Unmarshal([]byte(text), &jsonVal); err == nil {
		if num, ok := jsonVal.(float64); ok {
			return int64(num), nil
		}
	}

	// Fall back to direct parsing
	return strconv.ParseInt(text, 10, 64)
}

// AsFloat converts the response to float
func (rw *ResponseWrapper) AsFloat() (float64, error) {
	text, err := rw.AsString()
	if err != nil {
		return 0, err
	}

	text = strings.TrimSpace(text)

	// Try parsing as JSON number first
	var jsonVal interface{}
	if err := json.Unmarshal([]byte(text), &jsonVal); err == nil {
		if num, ok := jsonVal.(float64); ok {
			return num, nil
		}
	}

	// Fall back to direct parsing
	return strconv.ParseFloat(text, 64)
}

// AsBool converts the response to boolean
func (rw *ResponseWrapper) AsBool() (bool, error) {
	text, err := rw.AsString()
	if err != nil {
		return false, err
	}

	text = strings.TrimSpace(strings.ToLower(text))

	switch text {
	case "true", "1", "yes", "y":
		return true, nil
	case "false", "0", "no", "n":
		return false, nil
	default:
		// Try parsing as JSON boolean
		var jsonVal interface{}
		if err := json.Unmarshal([]byte(text), &jsonVal); err == nil {
			if boolVal, ok := jsonVal.(bool); ok {
				return boolVal, nil
			}
		}
		return false, fmt.Errorf("failed to parse '%s' as boolean", text)
	}
}

// AsType deserializes the response to any type implementing json unmarshaling
func (rw *ResponseWrapper) AsType(target interface{}) error {
	switch rw.contentType {
	case ResponseContentTypeJSON:
		return json.Unmarshal(rw.rawBytes, target)
	case ResponseContentTypeText:
		// For text responses, try to deserialize the string directly
		text := string(rw.rawBytes)
		// Wrap in quotes to make it valid JSON string
		jsonStr := fmt.Sprintf(`"%s"`, strings.ReplaceAll(text, `"`, `\"`))
		return json.Unmarshal([]byte(jsonStr), target)
	case ResponseContentTypeBinary:
		return fmt.Errorf("cannot deserialize binary response to structured type")
	}
	return fmt.Errorf("unknown content type")
}

// IsEmpty checks if the response is empty
func (rw *ResponseWrapper) IsEmpty() bool {
	return len(rw.rawBytes) == 0
}

// Size returns the response size in bytes
func (rw *ResponseWrapper) Size() int {
	return len(rw.rawBytes)
}

// IsBinary checks if the response is binary
func (rw *ResponseWrapper) IsBinary() bool {
	return rw.contentType == ResponseContentTypeBinary
}

// IsJSON checks if the response is JSON
func (rw *ResponseWrapper) IsJSON() bool {
	return rw.contentType == ResponseContentTypeJSON
}

// IsText checks if the response is text
func (rw *ResponseWrapper) IsText() bool {
	return rw.contentType == ResponseContentTypeText
}

// ValidateAgainstCap validates response against cap output definition
func (rw *ResponseWrapper) ValidateAgainstCap(cap *Cap, registry *media.MediaUrnRegistry) error {
	// Convert response to interface{} for validation
	var value interface{}
	switch rw.contentType {
	case ResponseContentTypeJSON:
		text, err := rw.AsString()
		if err != nil {
			return fmt.Errorf("failed to convert response to string: %w", err)
		}
		if err := json.Unmarshal([]byte(text), &value); err != nil {
			return fmt.Errorf("failed to parse JSON: %w", err)
		}
	case ResponseContentTypeText:
		text, err := rw.AsString()
		if err != nil {
			return fmt.Errorf("failed to convert response to string: %w", err)
		}
		value = text
	case ResponseContentTypeBinary:
		// Binary outputs can't be validated as JSON, validate the response type instead
		if output := cap.GetOutput(); output != nil {
			resolved, err := output.Resolve(cap.GetMediaSpecs(), registry)
			if err != nil {
				return fmt.Errorf("failed to resolve output media URN '%s': %w", output.MediaUrn, err)
			}
			if !resolved.IsBinary() {
				return fmt.Errorf(
					"cap %s expects %s output but received binary data",
					cap.UrnString(),
					resolved.MediaType,
				)
			}
		}
		return nil
	}

	outputValidator := NewOutputValidator()
	return outputValidator.ValidateOutput(cap, value, registry)
}

// GetContentType returns the content type for validation purposes
func (rw *ResponseWrapper) GetContentType() string {
	switch rw.contentType {
	case ResponseContentTypeJSON:
		return "application/json"
	case ResponseContentTypeText:
		return "text/plain"
	case ResponseContentTypeBinary:
		return "application/octet-stream"
	}
	return "unknown"
}

// MatchesOutputType checks if response matches expected output type.
// Returns error if the output spec cannot be resolved - no fallbacks.
func (rw *ResponseWrapper) MatchesOutputType(cap *Cap, registry *media.MediaUrnRegistry) (bool, error) {
	output := cap.GetOutput()
	if output == nil {
		return false, fmt.Errorf("cap '%s' has no output definition", cap.UrnString())
	}

	// Resolve the output media URN to get the media type - fail hard if resolution fails
	resolved, err := output.Resolve(cap.GetMediaSpecs(), registry)
	if err != nil {
		return false, fmt.Errorf("failed to resolve output media URN '%s' for cap '%s': %w", output.MediaUrn, cap.UrnString(), err)
	}

	switch rw.contentType {
	case ResponseContentTypeJSON:
		// JSON response matches structured outputs (map/list)
		return resolved.IsStructured(), nil
	case ResponseContentTypeText:
		// Text response matches non-binary, non-structured outputs (scalars)
		return !resolved.IsBinary() && !resolved.IsStructured(), nil
	case ResponseContentTypeBinary:
		// Binary response matches binary outputs
		return resolved.IsBinary(), nil
	}

	return false, nil
}
