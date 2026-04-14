package standard

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TEST307: Test ModelAvailabilityUrn builds valid cap URN with correct op and media specs
func Test307_model_availability_urn(t *testing.T) {
	urnStr := ModelAvailabilityUrn()
	assert.True(t, strings.Contains(urnStr, "op=model-availability"), "URN must have op=model-availability")
	assert.True(t, strings.Contains(urnStr, "in=media:model-spec"), "input must be model-spec")
	assert.True(t, strings.Contains(urnStr, "out=media:availability-output"), "output must be availability output")
}

// TEST308: Test ModelPathUrn builds valid cap URN with correct op and media specs
func Test308_model_path_urn(t *testing.T) {
	urnStr := ModelPathUrn()
	assert.True(t, strings.Contains(urnStr, "op=model-path"), "URN must have op=model-path")
	assert.True(t, strings.Contains(urnStr, "in=media:model-spec"), "input must be model-spec")
	assert.True(t, strings.Contains(urnStr, "out=media:path-output"), "output must be path output")
}

// TEST309: Test ModelAvailabilityUrn and ModelPathUrn produce distinct URNs
func Test309_model_availability_and_path_are_distinct(t *testing.T) {
	availStr := ModelAvailabilityUrn()
	pathStr := ModelPathUrn()
	assert.NotEqual(t, availStr, pathStr,
		"availability and path must be distinct cap URNs")
}

// TEST310: Test LlmGenerateTextUrn builds a cap URN with llm, ml-model, and op=generate_text
func Test310_llm_generate_text_urn_shape(t *testing.T) {
	urnStr := LlmGenerateTextUrn()
	assert.True(t, strings.HasPrefix(urnStr, "cap:"), "must be a cap URN")
	assert.True(t, strings.Contains(urnStr, "op=generate_text"), "must have op=generate_text")
	assert.True(t, strings.Contains(urnStr, "llm"), "must have llm tag")
	assert.True(t, strings.Contains(urnStr, "ml-model"), "must have ml-model tag")
	assert.True(t, strings.Contains(urnStr, "in="), "must have in spec")
	assert.True(t, strings.Contains(urnStr, "out="), "must have out spec")
}

// TEST312: Test all URN builders produce parseable cap URNs
func Test312_all_urn_builders_produce_valid_urns(t *testing.T) {
	// Each of these must not panic and must start with "cap:"
	availStr := ModelAvailabilityUrn()
	assert.True(t, strings.HasPrefix(availStr, "cap:"), "must be a cap URN")

	pathStr := ModelPathUrn()
	assert.True(t, strings.HasPrefix(pathStr, "cap:"), "must be a cap URN")

	llmStr := LlmGenerateTextUrn()
	assert.True(t, strings.HasPrefix(llmStr, "cap:"), "must be a cap URN")
}

// TEST473: CAP_DISCARD constant has correct format with wildcard input and void output
func Test473_cap_discard_parses_as_valid_urn(t *testing.T) {
	// Cannot import urn package from standard_test (import cycle),
	// so verify via string assertions on the constant value
	assert.True(t, strings.HasPrefix(CapDiscard, "cap:"), "CAP_DISCARD must be a cap URN")
	assert.True(t, strings.Contains(CapDiscard, "in=media:"), "CAP_DISCARD input must be wildcard media:")
	assert.True(t, strings.Contains(CapDiscard, "out=media:void"), "CAP_DISCARD output must be media:void")
}

// TEST474: CAP_DISCARD structure — wildcard input, void output
// NOTE: Full accepts() semantics tested in urn/cap_urn_test.go where urn package is available.
// Here we verify the structural properties that make discard work.
func Test474_cap_discard_structure(t *testing.T) {
	// Discard has no op tag — it's a pattern that accepts anything with void output
	assert.False(t, strings.Contains(CapDiscard, "op="),
		"CAP_DISCARD should have no op tag (accepts any op)")
	// Input is wildcard media: (accepts any input type)
	assert.True(t, strings.Contains(CapDiscard, "in=media:;") || strings.HasSuffix(CapDiscard, "in=media:"),
		"CAP_DISCARD input must be bare media: (wildcard)")
	// Output is specifically void
	assert.True(t, strings.Contains(CapDiscard, "out=media:void"),
		"CAP_DISCARD output must be media:void")
}

// TEST605: all_coercion_paths builds valid URNs with op=coerce
func Test605_all_coercion_paths_build_valid_urns(t *testing.T) {
	paths := AllCoercionPaths()
	assert.True(t, len(paths) > 0, "Coercion paths must not be empty")

	for _, pair := range paths {
		source, target := pair[0], pair[1]
		urnStr := CoercionUrn(source, target)
		assert.True(t, strings.Contains(urnStr, "op=coerce"),
			"Coercion URN for %s→%s must have op=coerce", source, target)

		// Verify it starts with cap: (valid cap URN prefix)
		assert.True(t, strings.HasPrefix(urnStr, "cap:"),
			"Coercion URN for %s→%s must start with cap:", source, target)
	}
}

// TEST606: coercion_urn in/out specs match the type's media URN constant
func Test606_coercion_urn_specs(t *testing.T) {
	urnStr := CoercionUrn("string", "integer")

	// in_spec should contain MEDIA_STRING
	assert.True(t, strings.Contains(urnStr, MediaString),
		"in_spec should contain '%s', got '%s'", MediaString, urnStr)

	// out_spec should contain MEDIA_INTEGER
	assert.True(t, strings.Contains(urnStr, MediaInteger),
		"out_spec should contain '%s', got '%s'", MediaInteger, urnStr)
}

// TEST850: all_format_conversion_paths returns non-empty list with valid URNs
func Test850_all_format_conversion_paths_build_valid_urns(t *testing.T) {
	paths := AllFormatConversionPaths()
	assert.True(t, len(paths) > 0, "Format conversion paths must not be empty")

	for _, p := range paths {
		urnStr := FormatConversionUrn(p.InMedia, p.OutMedia)
		assert.True(t, strings.HasPrefix(urnStr, "cap:"),
			"Format conversion URN must be a cap URN, got %s", urnStr)
		assert.True(t, strings.Contains(urnStr, "op=convert_format"),
			"Format conversion URN must have op=convert_format, got %s", urnStr)
	}
}

// TEST851: format_conversion_urn in/out specs are set correctly
func Test851_format_conversion_urn_specs(t *testing.T) {
	urnStr := FormatConversionUrn(MediaJSONRecord, MediaYAMLRecord)
	assert.True(t, strings.Contains(urnStr, "op=convert_format"), "must have op=convert_format")
	assert.True(t, strings.Contains(urnStr, "in="), "must have in spec")
	assert.True(t, strings.Contains(urnStr, "out="), "must have out spec")
	assert.True(t, strings.HasPrefix(urnStr, "cap:"), "must be a cap URN")
}
