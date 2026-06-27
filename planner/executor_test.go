package planner

import (
	"testing"
)

// TEST804: Tests basic JSON path extraction with dot notation for nested objects Verifies that simple paths like "data.message" correctly extract values from nested JSON structures
func Test804_ExtractJsonPathSimple(t *testing.T) {
	value := map[string]any{
		"data": map[string]any{
			"message": "hello world",
		},
	}
	result, err := ExtractJsonPath(value, "data.message")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "hello world" {
		t.Fatalf("expected 'hello world', got %v", result)
	}
}

// TEST805: Tests JSON path extraction with array indexing syntax
func Test805_ExtractJsonPathWithArray(t *testing.T) {
	value := map[string]any{
		"items": []any{
			map[string]any{"name": "first"},
			map[string]any{"name": "second"},
		},
	}
	result, err := ExtractJsonPath(value, "items[0].name")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "first" {
		t.Fatalf("expected 'first', got %v", result)
	}
}

// TEST806: Tests error handling when JSON path references non-existent fields Verifies that accessing missing fields returns an appropriate error message
func Test806_ExtractJsonPathMissingField(t *testing.T) {
	value := map[string]any{"data": map[string]any{}}
	_, err := ExtractJsonPath(value, "data.nonexistent")
	if err == nil {
		t.Fatal("expected error for missing field")
	}
	errStr := err.Error()
	if !stringContains(errStr, "nonexistent") && !stringContains(errStr, "not found") && !stringContains(errStr, "Field") {
		t.Fatalf("error should mention missing field: %s", errStr)
	}
}

// TEST807: Tests EdgeType::Direct passes JSON values through unchanged Verifies that Direct edge type acts as a transparent passthrough without transformation
func Test807_ApplyEdgeTypeDirect(t *testing.T) {
	value := map[string]any{"test": "value"}
	result, err := ApplyEdgeType(value, DirectEdgeType())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m, ok := result.(map[string]any)
	if !ok || m["test"] != "value" {
		t.Fatalf("expected passthrough of value, got %v", result)
	}
}

// TEST808: Tests EdgeType::JsonField extracts specific top-level fields from JSON objects Verifies that JsonField edge type correctly isolates a single named field from the source output
func Test808_ApplyEdgeTypeJsonField(t *testing.T) {
	value := map[string]any{"test": "value", "other": "data"}
	result, err := ApplyEdgeType(value, JsonFieldEdgeType("test"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "value" {
		t.Fatalf("expected 'value', got %v", result)
	}
}

// TEST809: Tests EdgeType::JsonField error handling for missing fields Verifies that attempting to extract a non-existent field returns an error
func Test809_ApplyEdgeTypeJsonFieldMissing(t *testing.T) {
	value := map[string]any{"test": "value"}
	_, err := ApplyEdgeType(value, JsonFieldEdgeType("missing"))
	if err == nil {
		t.Fatal("expected error for missing field")
	}
}

// TEST810: Tests EdgeType::JsonPath extracts values using nested path expressions Verifies that JsonPath edge type correctly navigates through multiple levels like "data.nested.value"
func Test810_ApplyEdgeTypeJsonPath(t *testing.T) {
	value := map[string]any{
		"data": map[string]any{
			"nested": map[string]any{
				"value": 42,
			},
		},
	}
	result, err := ApplyEdgeType(value, JsonPathEdgeType("data.nested.value"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 42 {
		t.Fatalf("expected 42, got %v", result)
	}
}

// TEST811: Tests EdgeType::Iteration preserves array values for iterative processing Verifies that Iteration edge type passes through arrays unchanged to enable ForEach patterns
func Test811_ApplyEdgeTypeIteration(t *testing.T) {
	value := []any{1, 2, 3}
	result, err := ApplyEdgeType(value, IterationEdgeType())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := result.([]any)
	if !ok || len(arr) != 3 {
		t.Fatalf("expected array of 3 elements, got %v", result)
	}
}

// TEST812: Tests EdgeType::Collection preserves collected values without transformation Verifies that Collection edge type maintains structure for aggregation patterns
func Test812_ApplyEdgeTypeCollection(t *testing.T) {
	value := map[string]any{"collected": []any{1, 2, 3}}
	result, err := ApplyEdgeType(value, CollectionEdgeType())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	_ = m
}

// TEST813: Tests JSON path extraction through deeply nested object hierarchies (4+ levels) Verifies that paths can traverse multiple nested levels like "level1.level2.level3.level4.value"
func Test813_ExtractJsonPathDeeplyNested(t *testing.T) {
	value := map[string]any{
		"level1": map[string]any{
			"level2": map[string]any{
				"level3": map[string]any{
					"level4": map[string]any{
						"value": "deep",
					},
				},
			},
		},
	}
	result, err := ExtractJsonPath(value, "level1.level2.level3.level4.value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "deep" {
		t.Fatalf("expected 'deep', got %v", result)
	}
}

// TEST814: Tests error handling when array index exceeds available elements Verifies that out-of-bounds array access returns a descriptive error message
func Test814_ExtractJsonPathArrayOutOfBounds(t *testing.T) {
	value := map[string]any{
		"items": []any{map[string]any{"name": "first"}},
	}
	_, err := ExtractJsonPath(value, "items[5].name")
	if err == nil {
		t.Fatal("expected error for out-of-bounds index")
	}
	if !stringContains(err.Error(), "out of bounds") && !stringContains(err.Error(), "index") {
		t.Fatalf("error should mention out of bounds: %s", err.Error())
	}
}

// TEST815: Tests JSON path extraction with single-level paths (no nesting) Verifies that simple field names without dots correctly extract top-level values
func Test815_ExtractJsonPathSingleSegment(t *testing.T) {
	value := map[string]any{"value": 123}
	result, err := ExtractJsonPath(value, "value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 123 {
		t.Fatalf("expected 123, got %v", result)
	}
}

// TEST816: Tests JSON path extraction preserves special characters in string values Verifies that quotes, backslashes, and other special characters are correctly maintained
func Test816_ExtractJsonPathWithSpecialCharacters(t *testing.T) {
	msg := `hello "world" with 'quotes' and \ backslashes`
	value := map[string]any{
		"data": map[string]any{
			"message": msg,
		},
	}
	result, err := ExtractJsonPath(value, "data.message")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != msg {
		t.Fatalf("expected %q, got %v", msg, result)
	}
}

// TEST817: Tests JSON path extraction correctly handles explicit null values Verifies that null is returned as serde_json::Value::Null rather than an error
func Test817_ExtractJsonPathWithNullValue(t *testing.T) {
	value := map[string]any{
		"data": map[string]any{
			"nullable": nil,
		},
	}
	result, err := ExtractJsonPath(value, "data.nullable")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

// TEST818: Tests JSON path extraction correctly returns empty arrays Verifies that zero-length arrays are extracted as valid empty array values
func Test818_ExtractJsonPathWithEmptyArray(t *testing.T) {
	value := map[string]any{
		"data": map[string]any{
			"items": []any{},
		},
	}
	result, err := ExtractJsonPath(value, "data.items")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := result.([]any)
	if !ok || len(arr) != 0 {
		t.Fatalf("expected empty array, got %v", result)
	}
}

// TEST819: Tests JSON path extraction handles various numeric types correctly Verifies extraction of integers, floats, negative numbers, and zero
func Test819_ExtractJsonPathWithNumericTypes(t *testing.T) {
	value := map[string]any{
		"integers": 42,
		"floats":   3.14159,
		"negative": -100,
		"zero":     0,
	}
	tests := []struct {
		path     string
		expected any
	}{
		{"integers", 42},
		{"floats", 3.14159},
		{"negative", -100},
		{"zero", 0},
	}
	for _, tc := range tests {
		result, err := ExtractJsonPath(value, tc.path)
		if err != nil {
			t.Fatalf("path %q: unexpected error: %v", tc.path, err)
		}
		if result != tc.expected {
			t.Fatalf("path %q: expected %v, got %v", tc.path, tc.expected, result)
		}
	}
}

// TEST820: Tests JSON path extraction correctly handles boolean values Verifies that true and false are extracted as proper boolean JSON values
func Test820_ExtractJsonPathWithBoolean(t *testing.T) {
	value := map[string]any{
		"flags": map[string]any{
			"enabled":  true,
			"disabled": false,
		},
	}
	enabled, err := ExtractJsonPath(value, "flags.enabled")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if enabled != true {
		t.Fatalf("expected true, got %v", enabled)
	}
	disabled, err := ExtractJsonPath(value, "flags.disabled")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if disabled != false {
		t.Fatalf("expected false, got %v", disabled)
	}
}

// TEST821: Tests JSON path extraction with multi-dimensional arrays (matrix access) Verifies that nested array structures like "matrix[1]" correctly extract inner arrays
func Test821_ExtractJsonPathWithNestedArrays(t *testing.T) {
	value := map[string]any{
		"matrix": []any{
			[]any{1, 2, 3},
			[]any{4, 5, 6},
		},
	}
	result, err := ExtractJsonPath(value, "matrix[1]")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := result.([]any)
	if !ok || len(arr) != 3 {
		t.Fatalf("expected array [4,5,6], got %v", result)
	}
}

// TEST822: Tests error handling for non-numeric array indices Verifies that invalid indices like "items[abc]" return a descriptive parse error
func Test822_ExtractJsonPathInvalidArrayIndex(t *testing.T) {
	value := map[string]any{"items": []any{1, 2, 3}}
	_, err := ExtractJsonPath(value, "items[abc]")
	if err == nil {
		t.Fatal("expected error for non-numeric index")
	}
	if !stringContains(err.Error(), "Invalid array index") && !stringContains(err.Error(), "index") {
		t.Fatalf("error should mention invalid index: %s", err.Error())
	}
}
