// Package standard provides standard capability URN builders
package standard

import "fmt"

// =============================================================================
// STANDARD CAP URN CONSTANTS
// =============================================================================

// CapIdentity is the standard identity capability URN (short form)
// Accepts any media type as input and outputs the same type
// This expands to "cap:in=media:;out=media:" during parsing
const CapIdentity = "cap:"

// CapDiscard is the standard discard capability URN
// Accepts any media type as input and produces void output
const CapDiscard = "cap:in=media:;out=media:void"

// =============================================================================
// STANDARD CAP URN BUILDERS
// These return URN strings that can be parsed with urn.NewCapUrnFromString()
// =============================================================================

// LlmGenerateTextUrn builds the URN for generic text-generation capability.
func LlmGenerateTextUrn() string {
	return fmt.Sprintf(`cap:in="%s";llm;ml-model;op=generate_text;out="%s"`, MediaString, MediaString)
}

// ModelAvailabilityUrn builds a URN string for model-availability capability
func ModelAvailabilityUrn() string {
	return "cap:op=model-availability;in=media:model-spec;out=media:availability-output"
}

// ModelPathUrn builds a URN string for model-path capability
func ModelPathUrn() string {
	return "cap:op=model-path;in=media:model-spec;out=media:path-output"
}

// MediaUrnForType maps a type name to its media URN constant.
// Panics if type_name is unknown.
func MediaUrnForType(typeName string) string {
	switch typeName {
	case "string":
		return MediaString
	case "integer":
		return MediaInteger
	case "number":
		return MediaNumber
	case "boolean":
		return MediaBoolean
	case "object":
		return MediaObject
	case "string-list":
		return MediaStringList
	case "integer-list":
		return MediaIntegerList
	case "number-list":
		return MediaNumberList
	case "boolean-list":
		return MediaBooleanList
	case "object-list":
		return MediaObjectList
	default:
		panic(fmt.Sprintf("Unknown media type: %s. Valid types are: string, integer, number, boolean, object, string-list, integer-list, number-list, boolean-list, object-list", typeName))
	}
}

// CoercionUrn builds a coercion cap URN string given source and target types.
// The URN has op=coerce, in={sourceMedia}, out={targetMedia}.
// Panics if either type is unknown.
func CoercionUrn(sourceType, targetType string) string {
	inSpec := MediaUrnForType(sourceType)
	outSpec := MediaUrnForType(targetType)
	return fmt.Sprintf(`cap:in="%s";op=coerce;out="%s"`, inSpec, outSpec)
}

// AllCoercionPaths returns all valid coercion (source, target) pairs.
func AllCoercionPaths() [][2]string {
	return [][2]string{
		// To string (from all textable types)
		{"integer", "string"},
		{"number", "string"},
		{"boolean", "string"},
		{"object", "string"},
		{"string-list", "string"},
		{"integer-list", "string"},
		{"number-list", "string"},
		{"boolean-list", "string"},
		{"object-list", "string"},
		// To integer
		{"string", "integer"},
		{"number", "integer"},
		{"boolean", "integer"},
		// To number
		{"string", "number"},
		{"integer", "number"},
		{"boolean", "number"},
		// To object (wrap in object)
		{"string", "object"},
		{"integer", "object"},
		{"number", "object"},
		{"boolean", "object"},
	}
}

// FormatConversionUrn builds a URN for converting between formats.
func FormatConversionUrn(inMedia, outMedia string) string {
	return fmt.Sprintf(`cap:in="%s";op=convert_format;out="%s"`, inMedia, outMedia)
}

// FormatConversionPath describes a single format conversion path.
type FormatConversionPath struct {
	InMedia     string
	OutMedia    string
	Title       string
	Description string
}

// AllFormatConversionPaths returns all supported format conversion paths.
func AllFormatConversionPaths() []FormatConversionPath {
	return []FormatConversionPath{
		// JSON ↔ YAML
		{MediaJSONValue, MediaYAMLValue, "JSON Value → YAML Value", "Convert a JSON scalar or object to YAML"},
		{MediaYAMLValue, MediaJSONValue, "YAML Value → JSON Value", "Convert a YAML scalar or mapping to JSON"},
		{MediaJSONRecord, MediaYAMLRecord, "JSON Object → YAML Mapping", "Convert a JSON object to a YAML mapping"},
		{MediaYAMLRecord, MediaJSONRecord, "YAML Mapping → JSON Object", "Convert a YAML mapping to a JSON object"},
		{MediaJSONList, MediaYAMLList, "JSON Array → YAML Sequence", "Convert a JSON array to a YAML sequence"},
		{MediaYAMLList, MediaJSONList, "YAML Sequence → JSON Array", "Convert a YAML sequence to a JSON array"},
		{MediaJSONListRecord, MediaYAMLListRecord, "JSON Array of Objects → YAML Sequence of Mappings", "Convert a JSON array of objects to a YAML sequence of mappings"},
		{MediaYAMLListRecord, MediaJSONListRecord, "YAML Sequence of Mappings → JSON Array of Objects", "Convert a YAML sequence of mappings to a JSON array of objects"},
		// JSON list-record ↔ CSV
		{MediaJSONListRecord, MediaCSV, "JSON Array of Objects → CSV", "Convert a JSON array of objects to CSV with header row"},
		{MediaCSV, MediaJSONListRecord, "CSV → JSON Array of Objects", "Convert CSV with header row to a JSON array of objects"},
		// YAML list-record ↔ CSV
		{MediaYAMLListRecord, MediaCSV, "YAML Sequence of Mappings → CSV", "Convert a YAML sequence of mappings to CSV with header row"},
		{MediaCSV, MediaYAMLListRecord, "CSV → YAML Sequence of Mappings", "Convert CSV with header row to a YAML sequence of mappings"},
		// Textable list ↔ JSON list
		{MediaTextableList, MediaJSONList, "Textable List → JSON Array", "Convert a list of textable values to a JSON array"},
		{MediaJSONList, MediaTextableList, "JSON Array → Textable List", "Convert a JSON array to a list of textable values"},
		// Textable list ↔ YAML list
		{MediaTextableList, MediaYAMLList, "Textable List → YAML Sequence", "Convert a list of textable values to a YAML sequence"},
		{MediaYAMLList, MediaTextableList, "YAML Sequence → Textable List", "Convert a YAML sequence to a list of textable values"},
		// Textable list ↔ CSV
		{MediaTextableList, MediaCSVList, "Textable List → CSV List", "Convert a list of textable values to single-column CSV"},
		{MediaCSVList, MediaTextableList, "CSV List → Textable List", "Convert single-column CSV to a list of textable values"},
	}
}
