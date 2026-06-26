package cap

import (
	"encoding/json"
	"testing"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper for response wrapper tests - use proper media URNs with tags
func respTestUrn(tags string) string {
	if tags == "" {
		return `cap:in="media:void";out="media:fmt=json;record"`
	}
	return `cap:in="media:void";out="media:fmt=json;record";` + tags
}

// TEST168: Test ResponseWrapper from JSON deserializes to correct structured type
func Test168_response_wrapper_from_json(t *testing.T) {
	testData := map[string]interface{}{
		"name":  "test",
		"value": 42,
	}
	jsonBytes, err := json.Marshal(testData)
	require.NoError(t, err)

	response := NewResponseWrapperFromJSON(jsonBytes)

	assert.True(t, response.IsJSON())
	assert.False(t, response.IsText())
	assert.False(t, response.IsBinary())
	assert.Equal(t, len(jsonBytes), response.Size())

	var parsed map[string]interface{}
	err = response.AsType(&parsed)
	assert.NoError(t, err)
	assert.Equal(t, "test", parsed["name"])
	assert.Equal(t, float64(42), parsed["value"]) // JSON numbers are float64
}

// TEST0054: Response wrapper from text
func Test0054_ResponseWrapperFromText(t *testing.T) {
	testText := "Hello, World!"
	response := NewResponseWrapperFromText([]byte(testText))

	assert.False(t, response.IsJSON())
	assert.True(t, response.IsText())
	assert.False(t, response.IsBinary())

	result, err := response.AsString()
	assert.NoError(t, err)
	assert.Equal(t, testText, result)
}

// TEST170: Test ResponseWrapper from binary stores and retrieves raw bytes correctly
func Test170_response_wrapper_from_binary(t *testing.T) {
	testData := []byte{0x89, 0x50, 0x4E, 0x47} // PNG header
	response := NewResponseWrapperFromBinary(testData)

	assert.False(t, response.IsJSON())
	assert.False(t, response.IsText())
	assert.True(t, response.IsBinary())

	assert.Equal(t, testData, response.AsBytes())
	assert.Equal(t, len(testData), response.Size())

	// Should fail to convert to string
	_, err := response.AsString()
	assert.Error(t, err)
}

// TEST169: Test ResponseWrapper converts to primitive types integer, float, boolean, string
func Test169_response_wrapper_as_int(t *testing.T) {
	// Test from text
	response := NewResponseWrapperFromText([]byte("42"))
	result, err := response.AsInt()
	assert.NoError(t, err)
	assert.Equal(t, int64(42), result)

	// Test from JSON
	response2 := NewResponseWrapperFromJSON([]byte("123"))
	result2, err := response2.AsInt()
	assert.NoError(t, err)
	assert.Equal(t, int64(123), result2)

	// Test invalid conversion
	response3 := NewResponseWrapperFromText([]byte("not_a_number"))
	_, err = response3.AsInt()
	assert.Error(t, err)
}

// TEST0055: Response wrapper as float
func Test0055_ResponseWrapperAsFloat(t *testing.T) {
	// Test from text
	response := NewResponseWrapperFromText([]byte("3.14"))
	result, err := response.AsFloat()
	assert.NoError(t, err)
	assert.Equal(t, 3.14, result)

	// Test from JSON
	response2 := NewResponseWrapperFromJSON([]byte("2.71"))
	result2, err := response2.AsFloat()
	assert.NoError(t, err)
	assert.Equal(t, 2.71, result2)
}

// TEST0056: Response wrapper as bool
func Test0056_ResponseWrapperAsBool(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
		hasError bool
	}{
		{"true", true, false},
		{"false", false, false},
		{"1", true, false},
		{"0", false, false},
		{"yes", true, false},
		{"no", false, false},
		{"invalid", false, true},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			response := NewResponseWrapperFromText([]byte(tc.input))
			result, err := response.AsBool()

			if tc.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TEST0057: Response wrapper is empty
func Test0057_ResponseWrapperIsEmpty(t *testing.T) {
	// Empty response
	response := NewResponseWrapperFromText([]byte{})
	assert.True(t, response.IsEmpty())

	// Non-empty response
	response2 := NewResponseWrapperFromText([]byte("test"))
	assert.False(t, response2.IsEmpty())
}

// TEST0058: Response wrapper get content type
func Test0058_ResponseWrapperGetContentType(t *testing.T) {
	jsonResponse := NewResponseWrapperFromJSON([]byte("{}"))
	assert.Equal(t, "application/json", jsonResponse.GetContentType())

	textResponse := NewResponseWrapperFromText([]byte("test"))
	assert.Equal(t, "text/plain", textResponse.GetContentType())

	binaryResponse := NewResponseWrapperFromBinary([]byte{1, 2, 3})
	assert.Equal(t, "application/octet-stream", binaryResponse.GetContentType())
}

// TEST0059: Response wrapper matches output type
func Test0059_ResponseWrapperMatchesOutputType(t *testing.T) {
	registry := testRegistry(t)
	// Common mediaDefs for all caps - resolution requires this table
	// Use the constant values directly since the cap URNs reference these specific media URN strings
	// Seed the registry with the media defs the test caps reference.
	for _, def := range []media.MediaDef{
		{Urn: "media:enc=utf-8", MediaType: "text/plain", ProfileURI: media.ProfileStr},
		{Urn: "media:", MediaType: "application/octet-stream"},
		{Urn: "media:fmt=json;record", MediaType: "application/json", ProfileURI: media.ProfileObj},
		{Urn: "media:void", MediaType: "application/x-void", ProfileURI: media.ProfileVoid},
	} {
		registry.AddSpec(def.ToStored())
	}

	// Setup cap definitions with media URNs - all need in/out with proper tags
	stringCapUrn, err := urn.NewCapUrnFromString(`cap:in="media:void";test;out="media:enc=utf-8"`)
	require.NoError(t, err)
	stringCap := NewCap(stringCapUrn, "String Test", "test")
	// Use expanded URN form matching the cap's out spec for proper resolution
	stringCap.SetOutput(NewCapOutput("media:enc=utf-8", "String output"))

	binaryCapUrn, err := urn.NewCapUrnFromString(`cap:in="media:void";test;out="media:"`)
	require.NoError(t, err)
	binaryCap := NewCap(binaryCapUrn, "Binary Test", "test")
	binaryCap.SetOutput(NewCapOutput("media:", "Binary output"))

	jsonCapUrn, err := urn.NewCapUrnFromString(`cap:in="media:void";test;out="media:fmt=json;record"`)
	require.NoError(t, err)
	jsonCap := NewCap(jsonCapUrn, "JSON Test", "test")
	jsonCap.SetOutput(NewCapOutput("media:fmt=json;record", "JSON output"))

	// Test text response with string output type
	textResponse := NewResponseWrapperFromText([]byte("test"))
	matchStr, err := textResponse.MatchesOutputType(stringCap, registry)
	assert.NoError(t, err)
	assert.True(t, matchStr)
	matchBin, err := textResponse.MatchesOutputType(binaryCap, registry)
	assert.NoError(t, err)
	assert.False(t, matchBin)
	matchJson, err := textResponse.MatchesOutputType(jsonCap, registry)
	assert.NoError(t, err)
	assert.False(t, matchJson)

	// Test binary response with binary output type
	binaryResponse := NewResponseWrapperFromBinary([]byte{1, 2, 3})
	matchStr, err = binaryResponse.MatchesOutputType(stringCap, registry)
	assert.NoError(t, err)
	assert.False(t, matchStr)
	matchBin, err = binaryResponse.MatchesOutputType(binaryCap, registry)
	assert.NoError(t, err)
	assert.True(t, matchBin)
	matchJson, err = binaryResponse.MatchesOutputType(jsonCap, registry)
	assert.NoError(t, err)
	assert.False(t, matchJson)

	// Test JSON response (should match JSON types)
	jsonResponse := NewResponseWrapperFromJSON([]byte(`{"test": "value"}`))
	matchStr, err = jsonResponse.MatchesOutputType(stringCap, registry)
	assert.NoError(t, err)
	assert.False(t, matchStr)
	matchBin, err = jsonResponse.MatchesOutputType(binaryCap, registry)
	assert.NoError(t, err)
	assert.False(t, matchBin)
	matchJson, err = jsonResponse.MatchesOutputType(jsonCap, registry)
	assert.NoError(t, err)
	assert.True(t, matchJson)

	// Test cap with no output definition - MUST FAIL
	noOutputCapUrn, err := urn.NewCapUrnFromString(respTestUrn("test"))
	require.NoError(t, err)
	noOutputCap := NewCap(noOutputCapUrn, "No Output Test", "test")
	_, err = textResponse.MatchesOutputType(noOutputCap, registry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no output definition")

	// Test cap with unresolvable media URN - MUST FAIL
	badSpecCapUrn, err := urn.NewCapUrnFromString(respTestUrn("test"))
	require.NoError(t, err)
	badSpecCap := NewCap(badSpecCapUrn, "Bad Spec Test", "test")
	badSpecCap.SetOutput(NewCapOutput("media:unknown", "Unknown output"))
	_, err = textResponse.MatchesOutputType(badSpecCap, registry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve output media URN")
}

// TEST0069: Response wrapper validate against cap
func Test0069_ResponseWrapperValidateAgainstCap(t *testing.T) {
	registry := testRegistry(t)
	// Setup cap with output schema
	capUrn, err := urn.NewCapUrnFromString(respTestUrn("test"))
	require.NoError(t, err)
	cap := NewCap(capUrn, "Test Cap", "test")

	// Add custom spec with schema - needs map tag for JSON
	registry.AddSpec((media.NewMediaDefWithSchema(
		"media:result;enc=utf-8;record",
		"application/json",
		"https://example.com/schema/result",
		map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"status": map[string]interface{}{"type": "string"},
			},
			"required": []interface{}{"status"},
		},
	)).ToStored())

	cap.SetOutput(NewCapOutput("media:result;enc=utf-8;record", "Result output"))

	// Valid JSON response
	validResponse := NewResponseWrapperFromJSON([]byte(`{"status": "ok"}`))
	err = validResponse.ValidateAgainstCap(cap, registry)
	assert.NoError(t, err)

	// Invalid JSON response (missing required field)
	invalidResponse := NewResponseWrapperFromJSON([]byte(`{"other": "value"}`))
	err = invalidResponse.ValidateAgainstCap(cap, registry)
	assert.Error(t, err)
}

// TEST599: is_empty returns true for empty response, false for non-empty
func Test599_is_empty(t *testing.T) {
	emptyJSON := NewResponseWrapperFromJSON([]byte{})
	assert.True(t, emptyJSON.IsEmpty())

	emptyText := NewResponseWrapperFromText([]byte{})
	assert.True(t, emptyText.IsEmpty())

	emptyBinary := NewResponseWrapperFromBinary([]byte{})
	assert.True(t, emptyBinary.IsEmpty())

	nonEmpty := NewResponseWrapperFromText([]byte("x"))
	assert.False(t, nonEmpty.IsEmpty())
}

// TEST600: size returns exact byte count for all content types
func Test600_size(t *testing.T) {
	text := NewResponseWrapperFromText([]byte("hello"))
	assert.Equal(t, 5, text.Size())

	jsonResp := NewResponseWrapperFromJSON([]byte("{}"))
	assert.Equal(t, 2, jsonResp.Size())

	binary := NewResponseWrapperFromBinary(make([]byte, 1024))
	assert.Equal(t, 1024, binary.Size())

	empty := NewResponseWrapperFromText([]byte{})
	assert.Equal(t, 0, empty.Size())
}

// TEST601: get_content_type returns correct MIME type for each variant
func Test601_get_content_type(t *testing.T) {
	jsonResp := NewResponseWrapperFromJSON([]byte("{}"))
	assert.Equal(t, "application/json", jsonResp.GetContentType())

	text := NewResponseWrapperFromText([]byte("hello"))
	assert.Equal(t, "text/plain", text.GetContentType())

	binary := NewResponseWrapperFromBinary([]byte{0xFF})
	assert.Equal(t, "application/octet-stream", binary.GetContentType())
}

// TEST602: as_type on binary response returns error (cannot deserialize binary)
func Test602_as_type_binary_error(t *testing.T) {
	binary := NewResponseWrapperFromBinary([]byte{0x89, 0x50})
	var target map[string]any
	err := binary.AsType(&target)
	require.Error(t, err, "Binary responses must not be deserializable to structured types")
	assert.Contains(t, err.Error(), "binary", "Error should mention binary: %s", err.Error())
}

// TEST603: as_bool handles all accepted truthy/falsy variants and rejects garbage
func Test603_as_bool_edge_cases(t *testing.T) {
	// Truthy values
	for _, input := range []string{"true", "TRUE", "True", "1", "yes", "YES", "y", "Y"} {
		resp := NewResponseWrapperFromText([]byte(input))
		val, err := resp.AsBool()
		require.NoError(t, err, "'%s' should parse without error", input)
		assert.True(t, val, "'%s' should be truthy", input)
	}

	// Falsy values
	for _, input := range []string{"false", "FALSE", "False", "0", "no", "NO", "n", "N"} {
		resp := NewResponseWrapperFromText([]byte(input))
		val, err := resp.AsBool()
		require.NoError(t, err, "'%s' should parse without error", input)
		assert.False(t, val, "'%s' should be falsy", input)
	}

	// Garbage input should error
	garbage := NewResponseWrapperFromText([]byte("maybe"))
	_, err := garbage.AsBool()
	assert.Error(t, err)

	// Whitespace-padded should still work
	padded := NewResponseWrapperFromText([]byte("  true  "))
	val, err := padded.AsBool()
	require.NoError(t, err)
	assert.True(t, val)
}
