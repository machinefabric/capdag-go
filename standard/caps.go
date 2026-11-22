// Package standard provides standard capability URN builders
package standard

import "fmt"

// =============================================================================
// STANDARD CAP URN CONSTANTS
// =============================================================================

// CapIdentity is the standard identity capability URN.
// Accepts any media type as input and preserves the runtime media identity.
const CapIdentity = "cap:effect=none"

// CapDiscard is the standard discard capability URN.
// Accepts any media type as input and produces void output.
// Canonical form: default in= (media:) and default effect (declared)
// are omitted; only out=media:void remains.
const CapDiscard = "cap:out=media:void"

// CapAdapterSelection is the standard adapter-selection capability URN.
// Default implementation returns empty END (no match).
// Cartridges that inspect file content override this with a handler
// that returns {"media_urns": [...]}.
const CapAdapterSelection = `cap:in="media:";out="media:adapter-selection;enc=utf8;fmt=json;record"`

// CapLookupCapFabric resolves a canonical cap URN to its full flattened
// cap definition by fetching from the public fabric registry. Implemented
// by fetchcartridge.
const CapLookupCapFabric = `cap:in="media:cap-urn;enc=utf8";fabric;lookup-cap;out="media:cap-definition;enc=utf8;fmt=json;record"`

// CapLookupMediaDefFabric resolves a canonical media URN to its full
// media definition by fetching from the public fabric registry.
// Implemented by fetchcartridge.
const CapLookupMediaDefFabric = `cap:in="media:enc=utf8;media-urn";fabric;lookup-media-def;out="media:enc=utf8;fmt=json;media-definition;record"`

// =============================================================================
// STANDARD CAP URN BUILDERS
// These return URN strings that can be parsed with urn.NewCapUrnFromString()
// =============================================================================

// LlmGenerateTextUrn builds the URN for generic text-generation capability.
func LlmGenerateTextUrn() string {
	return fmt.Sprintf(`cap:in="%s";llm;ml-model;generate-text;out="%s"`, MediaPlainText, MediaTXT)
}

// ModelAvailabilityUrn builds a URN string for model-availability capability
func ModelAvailabilityUrn() string {
	return "cap:in=media:enc=utf8;model-spec;model-availability;out=media:availability-output"
}

// ModelPathUrn builds a URN string for model-path capability
func ModelPathUrn() string {
	return "cap:in=media:enc=utf8;model-spec;model-path;out=media:path-output"
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
	case "json-object":
		// What a coercion to an object gives: a JSON object, not any record.
		return MediaJSONRecord
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
		panic(fmt.Sprintf("Unknown media type: %s. Valid types are: string, integer, number, boolean, object, json-object, string-list, integer-list, number-list, boolean-list, object-list", typeName))
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
		// To a JSON object (wrap in object)
		{"string", "json-object"},
		{"integer", "json-object"},
		{"number", "json-object"},
		{"boolean", "json-object"},
	}
}

// SameUrn builds the URN for the `same` semantic-equivalence capability.
// Two items + optional context in; a semantic-judgment record
// (`{same, confidence, reason}`) out. The first of the
// semantic-primitive family (docs/semantic-primitives.md).
func SameUrn(langCode string) string {
	return fmt.Sprintf(`cap:same;language=%s;constrained;in="%s";out="%s"`, langCode, MediaPlainText, MediaSemanticJudgment)
}

// semanticJudgmentUrn builds the shared shape of the semantic-primitive
// judgment cap URNs (docs/semantic-primitives.md): a bare marker + language +
// constrained, text in, judgment-envelope out.
func semanticJudgmentUrn(marker, langCode string) string {
	return fmt.Sprintf(`cap:%s;language=%s;constrained;in="%s";out="%s"`, marker, langCode, MediaPlainText, MediaSemanticJudgment)
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
	return fmt.Sprintf(`cap:summarize;language=%s;constrained;in="%s";out="%s"`, langCode, MediaPlainText, MediaTXT)
}

// FormatConversionUrn builds a URN for converting between formats. patch makes
// it an effect=patch cap: what it gives is what it was given, with what its
// output changes.
func FormatConversionUrn(inMedia, outMedia string, patch bool) string {
	if patch {
		return fmt.Sprintf(`cap:effect=patch;in="%s";convert-format;out="%s"`, inMedia, outMedia)
	}
	return fmt.Sprintf(`cap:in="%s";convert-format;out="%s"`, inMedia, outMedia)
}

// MediaYAMLContent is what converting to YAML gives: YAML content, not a file.
const MediaYAMLContent = "media:enc=utf8;fmt=yaml;!ext"

// MediaJSONContent is what converting to JSON gives: JSON content, not a file.
const MediaJSONContent = "media:enc=utf8;fmt=json;!ext"

// FormatConversionPath describes a single format conversion path.
type FormatConversionPath struct {
	InMedia  string
	OutMedia string
	// Patch is set for a conversion that keeps the shape it is given (JSON ↔
	// YAML: a list stays a list, a record a record); otherwise the output names
	// what it gives.
	Patch       bool
	Title       string
	Description string
}

// AllFormatConversionPaths returns all supported format conversion paths.
func AllFormatConversionPaths() []FormatConversionPath {
	return []FormatConversionPath{
		// JSON ↔ YAML: one patch each way, keeping whatever shape it is given
		{MediaJSONValue, MediaYAMLContent, true, "Convert JSON to YAML", "Re-serialize any JSON value as YAML, keeping its shape"},
		{MediaYAMLValue, MediaJSONContent, true, "Convert YAML to JSON", "Re-serialize any YAML value as JSON, keeping its shape"},
		// JSON list-record ↔ CSV
		{MediaJSONListRecord, MediaCSV, false, "JSON Array of Objects → CSV", "Convert a JSON array of objects to CSV with header row"},
		{MediaCSV, MediaJSONListRecord, false, "CSV → JSON Array of Objects", "Convert CSV with header row to a JSON array of objects"},
		// YAML list-record ↔ CSV
		{MediaYAMLListRecord, MediaCSV, false, "YAML Sequence of Mappings → CSV", "Convert a YAML sequence of mappings to CSV with header row"},
		{MediaCSV, MediaYAMLListRecord, false, "CSV → YAML Sequence of Mappings", "Convert CSV with header row to a YAML sequence of mappings"},
		// String list ↔ JSON list
		{MediaStringList, MediaJSONList, false, "String List → JSON Array", "Convert a list of string values to a JSON array"},
		{MediaJSONList, MediaStringList, false, "JSON Array → String List", "Convert a JSON array to a list of string values"},
		// String list ↔ YAML list
		{MediaStringList, MediaYAMLList, false, "String List → YAML Sequence", "Convert a list of string values to a YAML sequence"},
		{MediaYAMLList, MediaStringList, false, "YAML Sequence → String List", "Convert a YAML sequence to a list of string values"},
		// String list ↔ CSV
		{MediaStringList, MediaCSVList, false, "String List → CSV List", "Convert a list of string values to single-column CSV"},
		{MediaCSVList, MediaStringList, false, "CSV List → String List", "Convert single-column CSV to a list of string values"},
	}
}
