// Package standard provides standard capability URN builders
package standard

import "fmt"

// =============================================================================
// STANDARD CAP URN CONSTANTS
// =============================================================================

// CapIdentity is the standard identity capability URN.
// Accepts any media type as input and preserves the runtime media identity.
const CapIdentity = "cap:effect=none"

// CapDiscard is the standard discard capability URN
// Accepts any media type as input and produces void output
const CapDiscard = "cap:in=media:;out=media:void"

// CapAdapterSelection is the standard adapter-selection capability URN.
// Default implementation returns empty END (no match).
// Cartridges that inspect file content override this with a handler
// that returns {"media_urns": [...]}.
const CapAdapterSelection = `cap:in="media:";out="media:adapter-selection;fmt=json;record"`

// CapLookupCapFabric resolves a canonical cap URN to its full flattened
// cap definition by fetching from the public fabric registry. Implemented
// by fetchcartridge.
const CapLookupCapFabric = `cap:in="media:cap-urn;enc=utf-8";fabric;lookup-cap;out="media:cap-definition;fmt=json;record"`

// CapLookupMediaDefFabric resolves a canonical media URN to its full
// media definition by fetching from the public fabric registry.
// Implemented by fetchcartridge.
const CapLookupMediaDefFabric = `cap:in="media:enc=utf-8;media-urn";fabric;lookup-media-def;out="media:fmt=json;media-definition;record"`

// =============================================================================
// STANDARD CAP URN BUILDERS
// These return URN strings that can be parsed with urn.NewCapUrnFromString()
// =============================================================================

// LlmGenerateTextUrn builds the URN for generic text-generation capability.
func LlmGenerateTextUrn() string {
	return fmt.Sprintf(`cap:in="%s";llm;ml-model;generate-text;out="%s"`, MediaString, MediaString)
}

// ModelAvailabilityUrn builds a URN string for model-availability capability
func ModelAvailabilityUrn() string {
	return "cap:in=media:enc=utf-8;model-spec;model-availability;out=media:availability-output"
}

// ModelPathUrn builds a URN string for model-path capability
func ModelPathUrn() string {
	return "cap:in=media:enc=utf-8;model-spec;model-path;out=media:path-output"
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
	return fmt.Sprintf(`cap:in="%s";coerce;out="%s"`, inSpec, outSpec)
}

// AllCoercionPaths returns all valid coercion (source, target) pairs.
func AllCoercionPaths() [][2]string {
	return [][2]string{
		// To string (from all scalar/serializable types)
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
		// To boolean (strict spellings — see the reference
		// datacartridge coerce_to_boolean for the accepted set)
		{"string", "boolean"},
		{"integer", "boolean"},
		{"number", "boolean"},
		// To object (wrap in object)
		{"string", "object"},
		{"integer", "object"},
		{"number", "object"},
		{"boolean", "object"},
	}
}

// SameUrn builds the URN for the `same` semantic-equivalence capability.
// Two items + optional context in; a semantic-judgment record
// (`{same, confidence, reason}`) out. The first of the
// semantic-primitive family (docs/semantic-primitives.md).
func SameUrn(langCode string) string {
	return fmt.Sprintf(`cap:same;language=%s;constrained;in="%s";out="%s"`, langCode, MediaString, MediaSemanticJudgment)
}

// semanticJudgmentUrn builds the shared shape of the semantic-primitive
// judgment cap URNs (docs/semantic-primitives.md): a bare marker + language +
// constrained, text in, judgment-envelope out.
func semanticJudgmentUrn(marker, langCode string) string {
	return fmt.Sprintf(`cap:%s;language=%s;constrained;in="%s";out="%s"`, marker, langCode, MediaString, MediaSemanticJudgment)
}

// ClassifyUrn builds the URN for the `classify` closed-set labeling capability.
func ClassifyUrn(langCode string) string {
	return semanticJudgmentUrn("classify", langCode)
}

// ScoreUrn builds the URN for the `score` rubric-scoring capability.
func ScoreUrn(langCode string) string {
	return semanticJudgmentUrn("score", langCode)
}

// VerifyUrn builds the URN for the `verify` requirements-check capability.
func VerifyUrn(langCode string) string {
	return semanticJudgmentUrn("verify", langCode)
}

// RouteUrn builds the URN for the `route` dispatch-judgment capability.
func RouteUrn(langCode string) string {
	return semanticJudgmentUrn("route", langCode)
}

// NormalizeUrn builds the URN for the `normalize` entity-canonicalization capability.
func NormalizeUrn(langCode string) string {
	return semanticJudgmentUrn("normalize", langCode)
}

// ExtractUrn builds the URN for the `extract` schema-guided extraction capability
// (the judgment-envelope generalization of `generate-json`).
func ExtractUrn(langCode string) string {
	return semanticJudgmentUrn("extract", langCode)
}

// AskUrn builds the URN for the `ask` grounded question-answering capability.
func AskUrn(langCode string) string {
	return semanticJudgmentUrn("ask", langCode)
}

// ExplainUrn builds the URN for the `explain` opaque-data root-cause capability.
func ExplainUrn(langCode string) string {
	return semanticJudgmentUrn("explain", langCode)
}

// SummarizeUrn builds the URN for the `summarize` purpose-driven compression
// capability. Unlike the judgment caps its output is finalised plain text
// (the summary itself), not a judgment record.
func SummarizeUrn(langCode string) string {
	return fmt.Sprintf(`cap:summarize;language=%s;constrained;in="%s";out="%s"`, langCode, MediaString, MediaPlainText)
}

// FormatConversionUrn builds a URN for converting between formats.
func FormatConversionUrn(inMedia, outMedia string) string {
	return fmt.Sprintf(`cap:in="%s";convert-format;out="%s"`, inMedia, outMedia)
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
		// String list ↔ JSON list
		{MediaStringList, MediaJSONList, "String List → JSON Array", "Convert a list of string values to a JSON array"},
		{MediaJSONList, MediaStringList, "JSON Array → String List", "Convert a JSON array to a list of string values"},
		// String list ↔ YAML list
		{MediaStringList, MediaYAMLList, "String List → YAML Sequence", "Convert a list of string values to a YAML sequence"},
		{MediaYAMLList, MediaStringList, "YAML Sequence → String List", "Convert a YAML sequence to a list of string values"},
		// String list ↔ CSV
		{MediaStringList, MediaCSVList, "String List → CSV List", "Convert a list of string values to single-column CSV"},
		{MediaCSVList, MediaStringList, "CSV List → String List", "Convert single-column CSV to a list of string values"},
	}
}
