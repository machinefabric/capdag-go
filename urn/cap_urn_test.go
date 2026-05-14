package urn

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/machinefabric/capdag-go/standard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// All cap URNs now require in and out specs. Use these helpers:
// Media URNs must be quoted in cap URNs because they contain semicolons
// Use proper tags for is_binary/is_json/is_text detection
func testUrn(tags string) string {
	// Use standard.MediaObject constant for consistent canonical form
	if tags == "" {
		return `cap:in="media:void";out="` + standard.MediaObject + `"`
	}
	return `cap:in="media:void";out="` + standard.MediaObject + `";` + tags
}

func testUrnWithIO(inSpec, outSpec, tags string) string {
	// Media URNs need quoting because they contain semicolons
	if tags == "" {
		return `cap:in="` + inSpec + `";out="` + outSpec + `"`
	}
	return `cap:in="` + inSpec + `";out="` + outSpec + `";` + tags
}

// TEST001: Test that cap URN is created with tags parsed correctly and direction specs accessible
func Test001_cap_urn_creation(t *testing.T) {
	capUrn, err := NewCapUrnFromString(testUrn("transform;format=json;type=data_processing"))

	assert.NoError(t, err)
	assert.NotNil(t, capUrn)

	capType, exists := capUrn.GetTag("type")
	assert.True(t, exists)
	assert.Equal(t, "data_processing", capType)

	assert.True(t, capUrn.HasMarkerTag("transform"))

	format, exists := capUrn.GetTag("format")
	assert.True(t, exists)
	assert.Equal(t, "json", format)

	// Direction specs are required and accessible
	assert.Equal(t, standard.MediaVoid, capUrn.InSpec())
	assert.Equal(t, standard.MediaObject, capUrn.OutSpec())
}

// TEST002: Test that missing 'in' or 'out' defaults to media: wildcard
func Test002_direction_specs_default_to_wildcard(t *testing.T) {
	// Missing 'in' defaults to wildcard "media:"
	cap1, err := NewCapUrnFromString(`cap:out="media:object";test`)
	assert.NoError(t, err)
	assert.Equal(t, "media:", cap1.InSpec())

	// Missing 'out' defaults to wildcard "media:"
	cap2, err := NewCapUrnFromString(`cap:in="media:void";test`)
	assert.NoError(t, err)
	assert.Equal(t, "media:", cap2.OutSpec())

	// Both present should succeed
	cap3, err := NewCapUrnFromString(`cap:in="media:void";out="media:object";test`)
	assert.NoError(t, err)
	assert.Equal(t, "media:void", cap3.InSpec())
	assert.Equal(t, "media:object", cap3.OutSpec())
}

// TEST003: Test that direction specs must match exactly, different in/out types don't match, wildcard matches any
func Test003_direction_matching(t *testing.T) {
	cap1, err := NewCapUrnFromString(`cap:in="media:string";out="media:object";test`)
	require.NoError(t, err)
	cap2, err := NewCapUrnFromString(`cap:in="media:string";out="media:object";test`)
	require.NoError(t, err)
	assert.True(t, cap1.Accepts(cap2))

	cap3, err := NewCapUrnFromString(`cap:in="media:binary";out="media:object";test`)
	require.NoError(t, err)
	assert.False(t, cap1.Accepts(cap3))

	cap4, err := NewCapUrnFromString(`cap:in="media:string";out="media:integer";test`)
	require.NoError(t, err)
	assert.False(t, cap1.Accepts(cap4))

	cap5, err := NewCapUrnFromString(`cap:in=*;out="media:object";test`)
	require.NoError(t, err)
	assert.False(t, cap1.Accepts(cap5))
	assert.True(t, cap5.Accepts(cap1))
}

// TEST004: Test that unquoted keys and values are normalized to lowercase
func Test004_unquoted_values_lowercased(t *testing.T) {
	// Marker `Generate` (value-less) and keyed tags `EXT=PDF`, `Target=Thumbnail`.
	// Unquoted keys AND values are lowercased; quoted values would keep case.
	cap, err := NewCapUrnFromString(testUrn("Generate;EXT=PDF;Target=Thumbnail"))
	require.NoError(t, err)

	assert.True(t, cap.HasMarkerTag("generate"))

	ext, exists := cap.GetTag("ext")
	assert.True(t, exists)
	assert.Equal(t, "pdf", ext)

	target, exists := cap.GetTag("target")
	assert.True(t, exists)
	assert.Equal(t, "thumbnail", target)

	// Key lookup is case-insensitive: uppercase variants of `ext`
	// resolve to the same keyed tag.
	extUpper, exists := cap.GetTag("EXT")
	assert.True(t, exists)
	assert.Equal(t, "pdf", extUpper)
}

// TEST005: Test that quoted values preserve case while unquoted are lowercased
func Test005_quoted_values_preserve_case(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn(`key="Value With Spaces"`))
	require.NoError(t, err)
	value, exists := cap.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, "Value With Spaces", value)

	cap2, err := NewCapUrnFromString(testUrn(`KEY="Value With Spaces"`))
	require.NoError(t, err)
	value2, exists := cap2.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, "Value With Spaces", value2)

	unquoted, err := NewCapUrnFromString(testUrn("key=UPPERCASE"))
	require.NoError(t, err)
	quoted, err := NewCapUrnFromString(testUrn(`key="UPPERCASE"`))
	require.NoError(t, err)

	unquotedVal, _ := unquoted.GetTag("key")
	quotedVal, _ := quoted.GetTag("key")
	assert.Equal(t, "uppercase", unquotedVal)
	assert.Equal(t, "UPPERCASE", quotedVal)
	assert.False(t, unquoted.Equals(quoted))
}

// TEST006: Test that quoted values can contain special characters (semicolons, equals, spaces)
func Test006_quoted_value_special_chars(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn(`key="value;with;semicolons"`))
	require.NoError(t, err)
	value, exists := cap.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, "value;with;semicolons", value)

	cap2, err := NewCapUrnFromString(testUrn(`key="value=with=equals"`))
	require.NoError(t, err)
	value2, exists := cap2.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, "value=with=equals", value2)

	cap3, err := NewCapUrnFromString(testUrn(`key="hello world"`))
	require.NoError(t, err)
	value3, exists := cap3.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, "hello world", value3)
}

// TEST007: Test that escape sequences in quoted values (\" and \\) are parsed correctly
func Test007_quoted_value_escape_sequences(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn(`key="value\"quoted\""`))
	require.NoError(t, err)
	value, exists := cap.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, `value"quoted"`, value)

	cap2, err := NewCapUrnFromString(testUrn(`key="path\\file"`))
	require.NoError(t, err)
	value2, exists := cap2.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, `path\file`, value2)

	cap3, err := NewCapUrnFromString(testUrn(`key="say \"hello\\world\""`))
	require.NoError(t, err)
	value3, exists := cap3.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, `say "hello\world"`, value3)
}

// TEST008: Test that mixed quoted and unquoted values in same URN parse correctly
func Test008_mixed_quoted_unquoted(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn(`a="Quoted";b=simple`))
	require.NoError(t, err)

	a, exists := cap.GetTag("a")
	assert.True(t, exists)
	assert.Equal(t, "Quoted", a)

	b, exists := cap.GetTag("b")
	assert.True(t, exists)
	assert.Equal(t, "simple", b)
}

// TEST009: Test that unterminated quote produces UnterminatedQuote error
func Test009_unterminated_quote_error(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn(`key="unterminated`))
	assert.Nil(t, cap)
	assert.Error(t, err)
	capError, ok := err.(*CapUrnError)
	assert.True(t, ok)
	assert.Equal(t, ErrorUnterminatedQuote, capError.Code)
}

// TEST010: Test that invalid escape sequences (like \n, \x) produce InvalidEscapeSequence error
func Test010_invalid_escape_sequence_error(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn(`key="bad\n"`))
	assert.Nil(t, cap)
	assert.Error(t, err)
	capError, ok := err.(*CapUrnError)
	assert.True(t, ok)
	assert.Equal(t, ErrorInvalidEscapeSequence, capError.Code)

	cap2, err := NewCapUrnFromString(testUrn(`key="bad\x"`))
	assert.Nil(t, cap2)
	assert.Error(t, err)
	capError2, ok := err.(*CapUrnError)
	assert.True(t, ok)
	assert.Equal(t, ErrorInvalidEscapeSequence, capError2.Code)
}

// TEST011: Test that serialization uses smart quoting (no quotes for simple lowercase, quotes for special chars/uppercase)
func Test011_serialization_smart_quoting(t *testing.T) {
	// Simple lowercase value — no quoting needed; media URNs without semicolons unquoted
	cap, err := NewCapUrnBuilder().
		InSpec(standard.MediaVoid).
		OutSpec(standard.MediaObject).
		Tag("key", "simple").
		Build()
	require.NoError(t, err)
	// MediaObject = "media:record" has no semicolon, no quoting needed
	assert.Equal(t, `cap:in=media:void;key=simple;out=media:record`, cap.ToString())

	// Value with spaces — must be quoted
	cap2, err := NewCapUrnBuilder().
		InSpec(standard.MediaVoid).
		OutSpec(standard.MediaObject).
		Tag("key", "has spaces").
		Build()
	require.NoError(t, err)
	assert.Equal(t, `cap:in=media:void;key="has spaces";out=media:record`, cap2.ToString())

	// Value with uppercase — must be quoted
	cap4, err := NewCapUrnBuilder().
		InSpec(standard.MediaVoid).
		OutSpec(standard.MediaObject).
		Tag("key", "HasUpper").
		Build()
	require.NoError(t, err)
	assert.Equal(t, `cap:in=media:void;key="HasUpper";out=media:record`, cap4.ToString())
}

// TEST012: Test that simple cap URN round-trips (parse -> serialize -> parse equals original)
func Test012_round_trip_simple(t *testing.T) {
	original := testUrn("generate;ext=pdf")
	cap, err := NewCapUrnFromString(original)
	require.NoError(t, err)
	serialized := cap.ToString()
	reparsed, err := NewCapUrnFromString(serialized)
	require.NoError(t, err)
	assert.True(t, cap.Equals(reparsed))
}

// TEST013: Test that quoted values round-trip preserving case and spaces
func Test013_round_trip_quoted(t *testing.T) {
	original := testUrn(`key="Value With Spaces"`)
	cap, err := NewCapUrnFromString(original)
	require.NoError(t, err)
	serialized := cap.ToString()
	reparsed, err := NewCapUrnFromString(serialized)
	require.NoError(t, err)
	assert.True(t, cap.Equals(reparsed))
	value, exists := reparsed.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, "Value With Spaces", value)
}

// TEST014: Test that escape sequences round-trip correctly
func Test014_round_trip_escapes(t *testing.T) {
	original := testUrn(`key="value\"with\\escapes"`)
	cap, err := NewCapUrnFromString(original)
	require.NoError(t, err)
	value, exists := cap.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, `value"with\escapes`, value)
	serialized := cap.ToString()
	reparsed, err := NewCapUrnFromString(serialized)
	require.NoError(t, err)
	assert.True(t, cap.Equals(reparsed))
}

// TEST015: Test that cap: prefix is required and case-insensitive
func Test015_cap_prefix_required(t *testing.T) {
	capUrn, err := NewCapUrnFromString(`in="media:void";out="media:object";generate`)
	assert.Nil(t, capUrn)
	assert.Error(t, err)
	assert.Equal(t, ErrorMissingCapPrefix, err.(*CapUrnError).Code)

	capUrn, err = NewCapUrnFromString(testUrn("generate;ext=pdf"))
	assert.NoError(t, err)
	assert.NotNil(t, capUrn)
	assert.True(t, capUrn.HasMarkerTag("generate"))

	capUrn, err = NewCapUrnFromString(`CAP:in="media:void";out="media:object";generate`)
	assert.NoError(t, err)
	assert.True(t, capUrn.HasMarkerTag("generate"))
}

// TEST016: Test that trailing semicolon is equivalent (same hash, same string, matches)
func Test016_trailing_semicolon_equivalence(t *testing.T) {
	cap1, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)

	cap2, err := NewCapUrnFromString(testUrn("generate;ext=pdf") + ";")
	require.NoError(t, err)

	assert.True(t, cap1.Equals(cap2))
	assert.Equal(t, cap1.ToString(), cap2.ToString())
	assert.True(t, cap1.Accepts(cap2))
	assert.True(t, cap2.Accepts(cap1))
}

// TEST939: The canonical form drops `in=media:` and `out=media:`
// segments. Every spelling of "the same cap with wildcard in/out"
// collapses to one byte-identical canonical string. This is the
// contract that makes registry lookups work: the cap-publisher hashes
// `<canonical-urn>` to compute the cache key, and every language port
// (Rust, Go, Python, JS, ObjC) must agree on the canonical form for
// cross-language lookups to land on the same key. A regression that
// emitted the wildcard segments would silently move the published cap
// to a different SHA-256 bucket, 404'ing every reader that hashes the
// canonical form.
func Test939_cap_urn_canonical_form_drops_wildcard_in_out(t *testing.T) {
	canonical := "cap:decimate-sequence"
	variants := []string{
		"cap:decimate-sequence",
		"cap:decimate-sequence;in=media:;out=media:",
		"cap:in=media:;out=media:;decimate-sequence",
		"cap:in=media:;decimate-sequence;out=media:",
	}
	for _, v := range variants {
		parsed, err := NewCapUrnFromString(v)
		require.NoError(t, err, "variant %q must parse", v)
		assert.Equal(t, canonical, parsed.ToString(),
			"input %q canonicalized to %q, expected %q — wildcard in/out segments must be elided so the registry SHA-256 key is stable across input spellings",
			v, parsed.ToString(), canonical)
	}
	// Bare-identity round-trip.
	identity, err := NewCapUrnFromString("cap:in=media:;out=media:")
	require.NoError(t, err)
	assert.Equal(t, "cap:", identity.ToString())
}

// TEST017: Test tag matching: exact match, subset match, wildcard match, value mismatch
func Test017_tag_matching(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("generate;ext=pdf;target=thumbnail"))
	require.NoError(t, err)

	// Exact match — both directions accept
	request1, err := NewCapUrnFromString(testUrn("generate;ext=pdf;target=thumbnail"))
	require.NoError(t, err)
	assert.True(t, cap.Accepts(request1))
	assert.True(t, request1.Accepts(cap))

	// Routing direction: request(generate) accepts cap(op,ext,target) — request only needs op
	request2, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)
	assert.True(t, request2.Accepts(cap))
	// Reverse: cap(op,ext,target) as pattern rejects request missing ext,target
	assert.False(t, cap.Accepts(request2))

	// Routing direction: request(ext=*) accepts cap(ext=pdf) — wildcard matches specific
	request3, err := NewCapUrnFromString(testUrn("ext=*"))
	require.NoError(t, err)
	assert.True(t, request3.Accepts(cap))

	// Conflicting value — neither direction accepts
	request4, err := NewCapUrnFromString(testUrn("extract"))
	require.NoError(t, err)
	assert.False(t, cap.Accepts(request4))
	assert.False(t, request4.Accepts(cap))
}

// TEST018: Test that quoted values with different case do NOT match (case-sensitive)
func Test018_matching_case_sensitive_values(t *testing.T) {
	cap1, err := NewCapUrnFromString(testUrn(`key="Value"`))
	require.NoError(t, err)
	cap2, err := NewCapUrnFromString(testUrn(`key="value"`))
	require.NoError(t, err)
	assert.False(t, cap1.Accepts(cap2))
	assert.False(t, cap2.Accepts(cap1))

	cap3, err := NewCapUrnFromString(testUrn(`key="Value"`))
	require.NoError(t, err)
	assert.True(t, cap1.Accepts(cap3))
}

// TEST019: Missing tag in instance causes rejection — pattern's tags are constraints
func Test019_missing_tag_handling(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)

	// cap(op) as pattern: instance(ext) missing op → reject
	request1, err := NewCapUrnFromString(testUrn("ext=pdf"))
	require.NoError(t, err)
	assert.False(t, cap.Accepts(request1))
	// request(ext) as pattern: instance(cap) missing ext → reject
	assert.False(t, request1.Accepts(cap))

	// Routing: request(op) accepts cap(op,ext) — instance has op → match
	cap2, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)
	request2, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)
	assert.True(t, request2.Accepts(cap2))
	// Reverse: cap(op,ext) as pattern rejects request missing ext
	assert.False(t, cap2.Accepts(request2))

	// cap(ext=*;generate) as pattern accepts request(ext=pdf;generate)
	cap3, err := NewCapUrnFromString(testUrn("ext=*;generate"))
	require.NoError(t, err)
	request3, err := NewCapUrnFromString(testUrn("ext=pdf;generate"))
	require.NoError(t, err)
	assert.True(t, cap3.Accepts(request3))
}

// TEST020: Specificity is the sum of per-tag truth-table scores
// across in/out/y. Marker tags (bare segments like `generate`) are
// must-have-any (score 2), exact `key=value` tags score 3,
// missing/`?` score 0, `!` scores 1.
//
// testUrn() builds "cap:in=media:void;out=media:record;<tags>" so
// the directional baseline is:
//   in:  media:void   -> {void=*}    -> 2
//   out: media:record -> {record=*}  -> 2
// Total directional baseline: 4.
func Test020_specificity(t *testing.T) {
	cap1, err := NewCapUrnFromString(testUrn("type=general"))
	require.NoError(t, err)

	cap2, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)

	cap3, err := NewCapUrnFromString(testUrn("op;ext=pdf"))
	require.NoError(t, err)

	// testUrn() prepends in="media:void" (1 marker, score 2) and
	// out="media:record" (1 marker, score 2). Cap-URN spec is
	// 10000*spec_U(out) + 100*spec_U(in) + spec_U(y).

	// out=2, in=2, y=4 (type=general exact)
	assert.Equal(t, 10000*2+100*2+4, cap1.Specificity())
	// out=2, in=2, y=2 (generate marker = must-have-any)
	assert.Equal(t, 10000*2+100*2+2, cap2.Specificity())
	// out=2, in=2, y=2+4 (op marker, ext=pdf exact)
	assert.Equal(t, 10000*2+100*2+6, cap3.Specificity())

	// Wildcard in direction normalizes to media: (no tags, score 0).
	cap4, err := NewCapUrnFromString(`cap:in=*;out="` + standard.MediaObject + `";test`)
	require.NoError(t, err)
	// out=2 (record=*), in=0 (media: empty), y=2 (test marker)
	assert.Equal(t, 10000*2+100*0+2, cap4.Specificity())
}

// TEST021: Test builder creates cap URN with correct tags and direction specs
func Test021_builder(t *testing.T) {
	cap, err := NewCapUrnBuilder().
		InSpec(standard.MediaVoid).
		OutSpec(standard.MediaObject).
		Marker("generate").
		Tag("target", "thumbnail").
		Tag("ext", "pdf").
		Build()
	require.NoError(t, err)

	assert.True(t, cap.HasMarkerTag("generate"))

	assert.Equal(t, standard.MediaVoid, cap.InSpec())
	assert.Equal(t, standard.MediaObject, cap.OutSpec())
}

// TEST022: Test builder requires both in_spec and out_spec
func Test022_builder_requires_direction(t *testing.T) {
	_, err := NewCapUrnBuilder().
		OutSpec(standard.MediaObject).
		Tag("op", "test").
		Build()
	assert.Error(t, err)

	_, err = NewCapUrnBuilder().
		InSpec(standard.MediaVoid).
		Tag("op", "test").
		Build()
	assert.Error(t, err)

	_, err = NewCapUrnBuilder().
		InSpec(standard.MediaVoid).
		OutSpec(standard.MediaObject).
		Build()
	assert.NoError(t, err)
}

// TEST023: Test builder lowercases keys but preserves value case
func Test023_builder_preserves_case(t *testing.T) {
	cap, err := NewCapUrnBuilder().
		InSpec(standard.MediaVoid).
		OutSpec(standard.MediaObject).
		Tag("KEY", "ValueWithCase").
		Build()
	require.NoError(t, err)

	value, exists := cap.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, "ValueWithCase", value)
}

// TEST024: Directional accepts — pattern's tags are constraints, instance must satisfy
func Test024_directional_accepts(t *testing.T) {
	cap1, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)

	cap2, err := NewCapUrnFromString(testUrn("generate;format=*"))
	require.NoError(t, err)

	cap3, err := NewCapUrnFromString(testUrn("type=image;extract"))
	require.NoError(t, err)

	assert.False(t, cap1.Accepts(cap2))
	assert.False(t, cap2.Accepts(cap1))

	assert.False(t, cap1.Accepts(cap3))
	assert.False(t, cap3.Accepts(cap1))

	// Routing: general request(op) accepts specific cap(op,ext) — instance has op → match
	cap4, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)
	assert.True(t, cap4.Accepts(cap1)) // cap4 only requires op, cap1 has it
	// Reverse: specific cap(op,ext) rejects general request missing ext
	assert.False(t, cap1.Accepts(cap4))

	cap5, err := NewCapUrnFromString(`cap:in="media:binary";out="media:object";generate`)
	require.NoError(t, err)
	assert.False(t, cap1.Accepts(cap5))
	assert.False(t, cap5.Accepts(cap1))
}

// TEST025: Test find_best_match returns most specific matching cap
func Test025_best_match(t *testing.T) {
	matcher := &CapMatcher{}

	caps := []*CapUrn{}

	cap1, err := NewCapUrnFromString(testUrn("op"))
	require.NoError(t, err)
	caps = append(caps, cap1)

	cap2, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)
	caps = append(caps, cap2)

	cap3, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)
	caps = append(caps, cap3)

	request, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)

	best := matcher.FindBestMatch(caps, request)
	require.NotNil(t, best)

	ext, exists := best.GetTag("ext")
	assert.True(t, exists)
	assert.Equal(t, "pdf", ext)
}

// TEST026: Test merge combines tags from both caps, subset keeps only specified tags
func Test026_merge_and_subset(t *testing.T) {
	cap1, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)

	cap2, err := NewCapUrnFromString(`cap:in="media:binary";out="media:integer";ext=pdf;output=binary`)
	require.NoError(t, err)

	merged := cap1.Merge(cap2)
	assert.Equal(t, "media:binary", merged.InSpec())
	assert.Equal(t, "media:integer", merged.OutSpec())

	assert.True(t, merged.HasMarkerTag("generate"))
	ext, _ := merged.GetTag("ext")
	assert.Equal(t, "pdf", ext)

	// Subset test
	cap, err := NewCapUrnFromString(testUrn("generate;ext=pdf;output=binary;target=thumbnail"))
	require.NoError(t, err)

	subset := cap.Subset([]string{"type", "ext"})
	extVal, exists := subset.GetTag("ext")
	assert.True(t, exists)
	assert.Equal(t, "pdf", extVal)

	// Subset keeps only "type" and "ext" — the `generate` marker is dropped.
	assert.False(t, subset.HasMarkerTag("generate"))

	assert.Equal(t, standard.MediaVoid, subset.InSpec())
	assert.Equal(t, standard.MediaObject, subset.OutSpec())
}

// TEST027: Test with_wildcard_tag sets tag to wildcard, including in/out
func Test027_wildcard_tag(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("ext=pdf"))
	require.NoError(t, err)

	wildcarded := cap.WithWildcardTag("ext")
	ext, exists := wildcarded.GetTag("ext")
	assert.True(t, exists)
	assert.Equal(t, "*", ext)

	wildcardIn := cap.WithWildcardTag("in")
	assert.Equal(t, "*", wildcardIn.InSpec())

	wildcardOut := cap.WithWildcardTag("out")
	assert.Equal(t, "*", wildcardOut.OutSpec())
}

// TEST028: Test empty cap URN defaults to media: wildcard
func Test028_empty_cap_urn_defaults_to_wildcard(t *testing.T) {
	// Empty cap URN defaults to media: for both in and out
	result, err := NewCapUrnFromString("cap:")
	require.NoError(t, err, "Empty cap should default to media: wildcard")
	require.NotNil(t, result)
	assert.Equal(t, "media:", result.InSpec())
	assert.Equal(t, "media:", result.OutSpec())
	assert.Equal(t, 0, len(result.tags))

	// Trailing semicolon also works
	result2, err := NewCapUrnFromString("cap:;")
	require.NoError(t, err, "cap:; should default to media: wildcard")
	require.NotNil(t, result2)
	assert.Equal(t, "media:", result2.InSpec())
	assert.Equal(t, "media:", result2.OutSpec())
}

// TEST029: Test minimal valid cap URN has just in and out, empty tags
func Test029_minimal_cap_urn(t *testing.T) {
	cap, err := NewCapUrnFromString(`cap:in="media:void";out="media:object"`)
	require.NoError(t, err)
	assert.Equal(t, "media:void", cap.InSpec())
	assert.Equal(t, "media:object", cap.OutSpec())
	assert.Equal(t, 0, len(cap.tags))
}

// TEST030: Test extended characters (forward slashes, colons) in tag values
func Test030_extended_character_support(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("url=https://example_org/api;path=/some/file"))
	assert.NoError(t, err)
	assert.NotNil(t, cap)

	urlVal, exists := cap.GetTag("url")
	assert.True(t, exists)
	assert.Equal(t, "https://example_org/api", urlVal)

	pathVal, exists := cap.GetTag("path")
	assert.True(t, exists)
	assert.Equal(t, "/some/file", pathVal)
}

// TEST031: Test wildcard rejected in keys but accepted in values
func Test031_wildcard_restrictions(t *testing.T) {
	invalidKey, err := NewCapUrnFromString(testUrn("*=value"))
	assert.Error(t, err)
	assert.Nil(t, invalidKey)
	capError, ok := err.(*CapUrnError)
	assert.True(t, ok)
	assert.Equal(t, ErrorInvalidCharacter, capError.Code)

	validValue, err := NewCapUrnFromString(testUrn("key=*"))
	assert.NoError(t, err)
	assert.NotNil(t, validValue)

	value, exists := validValue.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, "*", value)
}

// TEST032: Test duplicate keys are rejected with DuplicateKey error
func Test032_duplicate_key_rejection(t *testing.T) {
	duplicate, err := NewCapUrnFromString(testUrn("key=value1;key=value2"))
	assert.Error(t, err)
	assert.Nil(t, duplicate)
	capError, ok := err.(*CapUrnError)
	assert.True(t, ok)
	assert.Equal(t, ErrorDuplicateKey, capError.Code)
}

// TEST033: Test pure numeric keys rejected, mixed alphanumeric allowed, numeric values allowed
func Test033_numeric_key_restriction(t *testing.T) {
	numericKey, err := NewCapUrnFromString(testUrn("123=value"))
	assert.Error(t, err)
	assert.Nil(t, numericKey)
	capError, ok := err.(*CapUrnError)
	assert.True(t, ok)
	assert.Equal(t, ErrorNumericKey, capError.Code)

	mixedKey1, err := NewCapUrnFromString(testUrn("key123=value"))
	assert.NoError(t, err)
	assert.NotNil(t, mixedKey1)

	mixedKey2, err := NewCapUrnFromString(testUrn("123key=value"))
	assert.NoError(t, err)
	assert.NotNil(t, mixedKey2)

	numericValue, err := NewCapUrnFromString(testUrn("key=123"))
	assert.NoError(t, err)
	assert.NotNil(t, numericValue)

	value, exists := numericValue.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, "123", value)
}

// TEST034: Test empty values are rejected
func Test034_empty_value_error(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("key="))
	assert.Nil(t, cap)
	assert.Error(t, err)

	cap2, err := NewCapUrnFromString(testUrn("key=;other=value"))
	assert.Nil(t, cap2)
	assert.Error(t, err)
}

// TEST035: Test has_tag is case-sensitive for values, case-insensitive for keys, works for in/out
func Test035_has_tag_case_sensitive(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn(`key="Value"`))
	require.NoError(t, err)

	assert.True(t, cap.HasTag("key", "Value"))
	assert.False(t, cap.HasTag("key", "value"))
	assert.False(t, cap.HasTag("key", "VALUE"))
	assert.True(t, cap.HasTag("KEY", "Value"))
	assert.True(t, cap.HasTag("Key", "Value"))
	assert.True(t, cap.HasTag("in", standard.MediaVoid))
	assert.True(t, cap.HasTag("out", standard.MediaObject))
}

// TEST036: Test with_tag preserves value case
func Test036_with_tag_preserves_value(t *testing.T) {
	cap := NewCapUrn(standard.MediaVoid, standard.MediaObject, map[string]string{})
	modified := cap.WithTag("key", "ValueWithCase")

	value, exists := modified.GetTag("key")
	assert.True(t, exists)
	assert.Equal(t, "ValueWithCase", value)
}

// TEST037: Test with_tag rejects empty value
func Test037_with_tag_rejects_empty_value(t *testing.T) {
	cap := NewCapUrn(standard.MediaVoid, standard.MediaObject, map[string]string{})
	modified, err := cap.WithTagValidated("key", "")
	assert.Error(t, err, "with_tag should reject empty value")
	assert.Nil(t, modified)
}

// TEST038: Test semantic equivalence of unquoted and quoted simple lowercase values
func Test038_semantic_equivalence(t *testing.T) {
	unquoted, err := NewCapUrnFromString(testUrn("key=simple"))
	require.NoError(t, err)
	quoted, err := NewCapUrnFromString(testUrn(`key="simple"`))
	require.NoError(t, err)
	assert.True(t, unquoted.Equals(quoted))
}

// TEST039: Test get_tag returns direction specs (in/out) with case-insensitive lookup
func Test039_get_tag_returns_direction_specs(t *testing.T) {
	cap, err := NewCapUrnFromString(`cap:in="media:string";out="media:integer";test`)
	require.NoError(t, err)

	inVal, exists := cap.GetTag("in")
	assert.True(t, exists)
	assert.Equal(t, "media:string", inVal)

	outVal, exists := cap.GetTag("out")
	assert.True(t, exists)
	assert.Equal(t, "media:integer", outVal)

	assert.True(t, cap.HasMarkerTag("test"))

	inVal2, exists := cap.GetTag("IN")
	assert.True(t, exists)
	assert.Equal(t, "media:string", inVal2)

	outVal2, exists := cap.GetTag("OUT")
	assert.True(t, exists)
	assert.Equal(t, "media:integer", outVal2)
}

// TEST040: Matching semantics - exact match succeeds
func Test040_matching_semantics_exact_match(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)

	request, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)

	assert.True(t, cap.Accepts(request), "Test 1: Exact match should succeed")
}

// TEST041: Matching semantics - cap missing tag matches (implicit wildcard)
func Test041_matching_semantics_cap_missing_tag(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)

	request, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)

	// A general cap (op only) CAN handle specific requests (op + ext)
	// The cap doesn't constrain ext, so any ext is fine
	assert.True(t, cap.Accepts(request), "General cap accepts specific request")

	cap2, err := NewCapUrnFromString(testUrn("ext=*;generate"))
	require.NoError(t, err)
	assert.True(t, cap2.Accepts(request), "Cap with ext=* also accepts request with ext=pdf")
}

// TEST042: Pattern rejects instance missing required tags
func Test042_matching_semantics_cap_has_extra_tag(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("generate;ext=pdf;version=2"))
	require.NoError(t, err)

	request, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)

	// Cap requires version=2, but request doesn't have it - reject
	assert.False(t, cap.Accepts(request), "Specific cap rejects request missing required tag")

	// But a request WITH version=2 is accepted
	request2, err := NewCapUrnFromString(testUrn("generate;ext=pdf;version=2"))
	require.NoError(t, err)
	assert.True(t, cap.Accepts(request2), "Request with all required tags is accepted")
}

// TEST043: Matching semantics - request wildcard matches specific cap value
func Test043_matching_semantics_request_has_wildcard(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)

	request, err := NewCapUrnFromString(testUrn("generate;ext=*"))
	require.NoError(t, err)

	assert.True(t, cap.Accepts(request), "Test 4: Request wildcard should match")
}

// TEST044: Matching semantics - cap wildcard matches specific request value
func Test044_matching_semantics_cap_has_wildcard(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("generate;ext=*"))
	require.NoError(t, err)

	request, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)

	assert.True(t, cap.Accepts(request), "Test 5: Cap wildcard should match")
}

// TEST045: Matching semantics - value mismatch does not match
func Test045_matching_semantics_value_mismatch(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("generate;ext=pdf"))
	require.NoError(t, err)

	request, err := NewCapUrnFromString(testUrn("generate;ext=docx"))
	require.NoError(t, err)

	assert.False(t, cap.Accepts(request), "Test 6: Value mismatch should not match")
}

// TEST046: Matching semantics - fallback pattern (cap missing tag = implicit wildcard)
func Test046_matching_semantics_fallback_pattern(t *testing.T) {
	cap, err := NewCapUrnFromString(`cap:in="media:binary";generate-thumbnail;out="media:binary"`)
	require.NoError(t, err)

	request, err := NewCapUrnFromString(`cap:ext=wav;in="media:binary";generate-thumbnail;out="media:binary"`)
	require.NoError(t, err)

	// Cap missing ext DOES accept request with ext=wav - general caps accept specific requests
	assert.True(t, cap.Accepts(request), "Cap without ext accepts request with ext=wav (implicit wildcard)")

	capWithWildcard, err := NewCapUrnFromString(`cap:ext=*;in="media:binary";generate-thumbnail;out="media:binary"`)
	require.NoError(t, err)
	assert.True(t, capWithWildcard.Accepts(request), "Cap with ext=* also accepts request with ext=wav")
}

// TEST047: Matching semantics - thumbnail fallback with void input
func Test047_matching_semantics_thumbnail_void_input(t *testing.T) {
	outBin := "media:binary"
	cap, err := NewCapUrnFromString(fmt.Sprintf(`cap:in="%s";generate-thumbnail;out="%s"`, standard.MediaVoid, outBin))
	require.NoError(t, err)

	request, err := NewCapUrnFromString(fmt.Sprintf(`cap:ext=wav;in="%s";generate-thumbnail;out="%s"`, standard.MediaVoid, outBin))
	require.NoError(t, err)

	// Cap missing ext DOES accept request with ext=wav - general caps accept specific requests
	assert.True(t, cap.Accepts(request), "Cap without ext accepts request with ext=wav (implicit wildcard)")

	capWithWildcard, err := NewCapUrnFromString(fmt.Sprintf(`cap:ext=*;in="%s";generate-thumbnail;out="%s"`, standard.MediaVoid, outBin))
	require.NoError(t, err)
	assert.True(t, capWithWildcard.Accepts(request), "Cap with ext=* also accepts request with ext=wav")
}

// TEST048: Matching semantics - wildcard direction matches anything
func Test048_matching_semantics_wildcard_direction_matches_anything(t *testing.T) {
	cap, err := NewCapUrnFromString("cap:in=*;out=*")
	require.NoError(t, err)

	request, err := NewCapUrnFromString(`cap:in="media:string";generate;out="media:object";ext=pdf`)
	require.NoError(t, err)

	// Wildcard cap (no tags) accepts any request - this is the identity/universal cap
	assert.True(t, cap.Accepts(request), "Wildcard cap accepts request with any tags")

	request2, err := NewCapUrnFromString(`cap:in="media:string";out="media:object"`)
	require.NoError(t, err)
	assert.True(t, cap.Accepts(request2), "Wildcard cap also accepts simpler requests")
}

// TEST049: Non-overlapping tags — neither direction accepts
func Test049_matching_semantics_cross_dimension_independence(t *testing.T) {
	cap, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)

	request, err := NewCapUrnFromString(testUrn("ext=pdf"))
	require.NoError(t, err)

	assert.False(t, cap.Accepts(request), "Test 9: Cap missing ext should NOT match request with ext=pdf")

	cap2, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)
	request2, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)
	assert.True(t, cap2.Accepts(request2), "Test 9b: Same tags should match")
}

// TEST050: Matching semantics - direction mismatch prevents matching
func Test050_matching_semantics_direction_mismatch(t *testing.T) {
	cap, err := NewCapUrnFromString(`cap:in="media:string";generate;out="` + standard.MediaObject + `"`)
	require.NoError(t, err)

	request, err := NewCapUrnFromString(`cap:in="media:";generate;out="` + standard.MediaObject + `"`)
	require.NoError(t, err)

	assert.False(t, cap.Accepts(request), "Test 10: Direction mismatch should not match")
}

// TEST890: Semantic direction matching - generic provider matches specific request
func Test890_direction_semantic_matching(t *testing.T) {
	genericCap, err := NewCapUrnFromString(
		`cap:in="media:";generate-thumbnail;out="media:image;png;thumbnail"`,
	)
	require.NoError(t, err)
	pdfRequest, err := NewCapUrnFromString(
		`cap:in="media:pdf";generate-thumbnail;out="media:image;png;thumbnail"`,
	)
	require.NoError(t, err)
	assert.True(t, genericCap.Accepts(pdfRequest),
		"Generic provider must match specific pdf request")

	epubRequest, err := NewCapUrnFromString(
		`cap:in="media:epub";generate-thumbnail;out="media:image;png;thumbnail"`,
	)
	require.NoError(t, err)
	assert.True(t, genericCap.Accepts(epubRequest),
		"Generic provider must match epub request")

	pdfCap, err := NewCapUrnFromString(
		`cap:in="media:pdf";generate-thumbnail;out="media:image;png;thumbnail"`,
	)
	require.NoError(t, err)
	genericRequest, err := NewCapUrnFromString(
		`cap:in="media:";generate-thumbnail;out="media:image;png;thumbnail"`,
	)
	require.NoError(t, err)
	assert.False(t, pdfCap.Accepts(genericRequest),
		"Specific pdf cap must NOT match generic request")

	assert.False(t, pdfCap.Accepts(epubRequest),
		"PDF-specific cap must NOT match epub request (epub lacks pdf marker)")

	specificOutCap, err := NewCapUrnFromString(
		`cap:in="media:";generate-thumbnail;out="media:image;png;thumbnail"`,
	)
	require.NoError(t, err)
	genericOutRequest, err := NewCapUrnFromString(
		`cap:in="media:";generate-thumbnail;out="media:image"`,
	)
	require.NoError(t, err)
	assert.True(t, specificOutCap.Accepts(genericOutRequest),
		"Cap producing image;png;thumbnail must satisfy request for image")

	genericOutCap, err := NewCapUrnFromString(
		`cap:in="media:";generate-thumbnail;out="media:image"`,
	)
	require.NoError(t, err)
	specificOutRequest, err := NewCapUrnFromString(
		`cap:in="media:";generate-thumbnail;out="media:image;png;thumbnail"`,
	)
	require.NoError(t, err)
	assert.False(t, genericOutCap.Accepts(specificOutRequest),
		"Cap producing generic image must NOT satisfy request requiring image;png;thumbnail")
}

// TEST891: Semantic direction specificity — more constraints in
// either axis means a higher score under the truth-table-driven sum.
// media: (top, no tags) scores 0; each marker tag scores 2; each
// exact tag scores 3.
func Test891_direction_semantic_specificity(t *testing.T) {
	genericCap, err := NewCapUrnFromString(
		`cap:in="media:";generate-thumbnail;out="media:image;png;thumbnail"`,
	)
	require.NoError(t, err)
	specificCap, err := NewCapUrnFromString(
		`cap:in="media:pdf";generate-thumbnail;out="media:image;png;thumbnail"`,
	)
	require.NoError(t, err)

	// genericCap:
	//   out=media:image;png;thumbnail -> 2+2+2 = 6
	//   in=media:                     -> 0
	//   y: generate-thumbnail marker  -> 2
	//   spec_C = 10000*6 + 100*0 + 2 = 60002
	assert.Equal(t, 10000*6+100*0+2, genericCap.Specificity())
	// specificCap:
	//   out=media:image;png;thumbnail -> 6
	//   in=media:pdf                  -> 2
	//   y: generate-thumbnail marker  -> 2
	//   spec_C = 10000*6 + 100*2 + 2 = 60202
	assert.Equal(t, 10000*6+100*2+2, specificCap.Specificity())

	assert.True(t, specificCap.Specificity() > genericCap.Specificity(),
		"pdf cap must be more specific than wildcard cap")

	pdfRequest, err := NewCapUrnFromString(
		`cap:in="media:pdf";generate-thumbnail;out="media:image;png;thumbnail"`,
	)
	require.NoError(t, err)
	caps := []*CapUrn{genericCap, specificCap}
	matcher := &CapMatcher{}
	best := matcher.FindBestMatch(caps, pdfRequest)
	require.NotNil(t, best)
	assert.Equal(t, 10000*6+100*2+2, best.Specificity(),
		"CapMatcher must prefer the more specific pdf provider")
}

// TEST559: without_tag removes tag, ignores in/out, case-insensitive for keys
func Test559_without_tag(t *testing.T) {
	cap, err := NewCapUrnFromString(
		`cap:in="media:void";test;ext=pdf;out="media:void"`,
	)
	require.NoError(t, err)
	removed := cap.WithoutTag("ext")
	_, exists := removed.GetTag("ext")
	assert.False(t, exists)
	assert.True(t, removed.HasMarkerTag("test"))

	// Case-insensitive removal
	removed2 := cap.WithoutTag("EXT")
	_, exists = removed2.GetTag("ext")
	assert.False(t, exists)

	// Removing in/out is silently ignored
	same := cap.WithoutTag("in")
	assert.Equal(t, "media:void", same.InSpec())
	same2 := cap.WithoutTag("out")
	assert.Equal(t, "media:void", same2.OutSpec())

	// Removing non-existent tag is no-op
	same3 := cap.WithoutTag("nonexistent")
	assert.True(t, same3.Equals(cap))
}

// TEST560: with_in_spec and with_out_spec change direction specs
func Test560_with_in_out_spec(t *testing.T) {
	cap, err := NewCapUrnFromString(
		`cap:in="media:void";test;out="media:void"`,
	)
	require.NoError(t, err)

	changedIn := cap.WithInSpec("media:")
	assert.Equal(t, "media:", changedIn.InSpec())
	assert.Equal(t, "media:void", changedIn.OutSpec())
	assert.True(t, changedIn.HasMarkerTag("test"))

	changedOut := cap.WithOutSpec("media:string")
	assert.Equal(t, "media:void", changedOut.InSpec())
	assert.Equal(t, "media:string", changedOut.OutSpec())

	// Chain both
	changedBoth := cap.
		WithInSpec("media:pdf").
		WithOutSpec("media:txt;textable")
	assert.Equal(t, "media:pdf", changedBoth.InSpec())
	assert.Equal(t, "media:txt;textable", changedBoth.OutSpec())
}

// TEST563: CapMatcher::find_all_matches returns all matching caps sorted by specificity
func Test563_find_all_matches(t *testing.T) {
	caps := []*CapUrn{}
	c1, err := NewCapUrnFromString(`cap:in="media:void";test;out="media:void"`)
	require.NoError(t, err)
	caps = append(caps, c1)
	c2, err := NewCapUrnFromString(`cap:in="media:void";test;ext=pdf;out="media:void"`)
	require.NoError(t, err)
	caps = append(caps, c2)
	c3, err := NewCapUrnFromString(`cap:in="media:void";different;out="media:void"`)
	require.NoError(t, err)
	caps = append(caps, c3)

	request, err := NewCapUrnFromString(`cap:in="media:void";test;out="media:void"`)
	require.NoError(t, err)
	matcher := &CapMatcher{}
	matches := matcher.FindAllMatches(caps, request)

	// Should find 2 matches (test and test;ext=pdf), not different
	assert.Equal(t, 2, len(matches))
	// Sorted by specificity descending: ext=pdf first (more specific)
	assert.True(t, matches[0].Specificity() >= matches[1].Specificity())
	extVal, exists := matches[0].GetTag("ext")
	assert.True(t, exists)
	assert.Equal(t, "pdf", extVal)
}

// TEST564: CapMatcher::are_compatible detects bidirectional overlap
func Test564_are_compatible(t *testing.T) {
	caps1 := []*CapUrn{}
	c1, err := NewCapUrnFromString(`cap:in="media:void";test;out="media:void"`)
	require.NoError(t, err)
	caps1 = append(caps1, c1)

	caps2 := []*CapUrn{}
	c2, err := NewCapUrnFromString(`cap:in="media:void";test;ext=pdf;out="media:void"`)
	require.NoError(t, err)
	caps2 = append(caps2, c2)

	caps3 := []*CapUrn{}
	c3, err := NewCapUrnFromString(`cap:in="media:void";different;out="media:void"`)
	require.NoError(t, err)
	caps3 = append(caps3, c3)

	matcher := &CapMatcher{}

	// caps1 (test) accepts caps2 (test;ext=pdf) -> compatible
	assert.True(t, matcher.AreCompatible(caps1, caps2))

	// caps1 (test) vs caps3 (different) -> not compatible
	assert.False(t, matcher.AreCompatible(caps1, caps3))

	// Empty sets are not compatible
	assert.False(t, matcher.AreCompatible([]*CapUrn{}, caps1))
	assert.False(t, matcher.AreCompatible(caps1, []*CapUrn{}))
}

// TEST565: tags_to_string returns only tags portion without prefix
func Test565_tags_to_string(t *testing.T) {
	cap, err := NewCapUrnFromString(
		`cap:in="media:void";test;out="media:void"`,
	)
	require.NoError(t, err)
	tagsStr := cap.ToString()
	// The full string starts with "cap:"
	assert.True(t, len(tagsStr) > 4)
	assert.Contains(t, tagsStr, "test")
}

// TEST566: with_tag silently ignores in/out keys
func Test566_with_tag_ignores_in_out(t *testing.T) {
	cap, err := NewCapUrnFromString(
		`cap:in="media:void";test;out="media:void"`,
	)
	require.NoError(t, err)
	// Attempting to set in/out via WithTag is silently ignored
	same := cap.WithTag("in", "media:")
	assert.Equal(t, "media:void", same.InSpec(), "WithTag must not change InSpec")

	same2 := cap.WithTag("out", "media:")
	assert.Equal(t, "media:void", same2.OutSpec(), "WithTag must not change OutSpec")
}

// TEST567: conforms_to_str and accepts_str work with string arguments
func Test567_str_variants(t *testing.T) {
	cap, err := NewCapUrnFromString(
		`cap:in="media:void";test;out="media:void"`,
	)
	require.NoError(t, err)

	// AcceptsStr
	assert.True(t, cap.AcceptsStr(`cap:in="media:void";test;ext=pdf;out="media:void"`))
	assert.False(t, cap.AcceptsStr(`cap:in="media:void";different;out="media:void"`))

	// ConformsTo via AcceptsStr (cap.ConformsTo(pattern) == pattern.Accepts(cap))
	pattern, err := NewCapUrnFromString(`cap:in="media:void";test;out="media:void"`)
	require.NoError(t, err)
	assert.True(t, cap.ConformsTo(pattern))

	// Invalid URN string -> false
	assert.False(t, cap.AcceptsStr("invalid"))
}

// TEST639: cap: (empty) defaults to in=media:;out=media:
func Test639_wildcard_empty_cap_defaults_to_media_wildcard(t *testing.T) {
	// cap: without in/out defaults to wildcard media: for both
	// This matches Rust behavior - empty cap is identity/wildcard
	cap, err := NewCapUrnFromString("cap:")
	require.NoError(t, err)
	assert.Equal(t, "media:", cap.InSpec())
	assert.Equal(t, "media:", cap.OutSpec())
}

// TEST648: Wildcard in/out match specific caps
func Test648_wildcard_accepts_specific(t *testing.T) {
	// In Go, we can create wildcard caps via NewCapUrnFromTags
	wildcard, err := NewCapUrnFromTags(map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, "media:", wildcard.InSpec())
	assert.Equal(t, "media:", wildcard.OutSpec())

	specific, err := NewCapUrnFromString("cap:in=media:pdf;out=media:text")
	require.NoError(t, err)

	assert.True(t, wildcard.Accepts(specific), "Wildcard should accept specific")
	assert.True(t, specific.ConformsTo(wildcard), "Specific should conform to wildcard")
}

// TEST649: Specificity - wildcard has 0, specific has tag count
func Test649_specificity_scoring(t *testing.T) {
	wildcard, err := NewCapUrnFromTags(map[string]string{})
	require.NoError(t, err)

	specific, err := NewCapUrnFromString("cap:in=media:pdf;out=media:text")
	require.NoError(t, err)

	assert.Equal(t, 0, wildcard.Specificity(), "Wildcard cap should have zero specificity")
	assert.True(t, specific.Specificity() > 0, "Specific cap should have non-zero specificity")
}

// TEST651: All identity forms produce the same CapUrn
func Test651_identity_forms_equivalent(t *testing.T) {
	// In Go, the identity form is created via NewCapUrnFromTags with empty map
	identity1, err := NewCapUrnFromTags(map[string]string{})
	require.NoError(t, err)

	identity2, err := NewCapUrnFromTags(map[string]string{"in": "*", "out": "*"})
	require.NoError(t, err)

	assert.Equal(t, identity1.InSpec(), identity2.InSpec())
	assert.Equal(t, identity1.OutSpec(), identity2.OutSpec())
	assert.True(t, identity1.Equals(identity2))
}

// TEST652: CAP_IDENTITY constant matches identity caps regardless of string form
func Test652_cap_identity_constant_works(t *testing.T) {
	// In Go, standard.CapIdentity = "cap:" which fails parsing
	// Instead test via NewCapUrnFromTags
	identity, err := NewCapUrnFromTags(map[string]string{})
	require.NoError(t, err)

	specific, err := NewCapUrnFromString("cap:in=media:pdf;out=media:text;test")
	require.NoError(t, err)

	// Identity accepts everything (no constraints)
	assert.True(t, identity.Accepts(specific), "Identity accepts specific")
	assert.True(t, specific.ConformsTo(identity), "Specific conforms to identity")
}

// TEST653: Identity (no tags) does not match specific requests via routing
func Test653_identity_routing_isolation(t *testing.T) {
	identity, err := NewCapUrnFromTags(map[string]string{})
	require.NoError(t, err)

	specificRequest, err := NewCapUrnFromString(`cap:in="media:void";test;out="media:void"`)
	require.NoError(t, err)

	// Routing direction: request.Accepts(cap)
	// specificRequest (has test) does NOT accept identity (missing op) -> identity NOT routed
	assert.False(t, specificRequest.Accepts(identity),
		"Specific request must NOT accept identity (identity lacks test)")

	// But identity request (no constraints) DOES accept specific cap
	identityRequest, err := NewCapUrnFromTags(map[string]string{})
	require.NoError(t, err)
	assert.True(t, identityRequest.Accepts(specificRequest),
		"Identity request (no constraints) matches everything")
}

// TEST823: is_dispatchable — exact match provider dispatches request
func Test823_dispatch_exact_match(t *testing.T) {
	provider, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:record;textable"`)
	require.NoError(t, err)
	request, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:record;textable"`)
	require.NoError(t, err)
	assert.True(t, provider.IsDispatchable(request))
}

// TEST824: is_dispatchable — provider with broader input handles specific request (contravariance)
func Test824_dispatch_contravariant_input(t *testing.T) {
	provider, err := NewCapUrnFromString(`cap:in="media:";analyze;out="media:record;textable"`)
	require.NoError(t, err)
	request, err := NewCapUrnFromString(`cap:in="media:pdf";analyze;out="media:record;textable"`)
	require.NoError(t, err)
	assert.True(t, provider.IsDispatchable(request))
}

// TEST825: is_dispatchable — request with unconstrained input dispatches to specific provider media: on the request input axis means "unconstrained" — vacuously true
func Test825_dispatch_request_unconstrained_input(t *testing.T) {
	provider, err := NewCapUrnFromString(`cap:in="media:pdf";analyze;out="media:record;textable"`)
	require.NoError(t, err)
	request, err := NewCapUrnFromString(`cap:in="media:";analyze;out="media:record;textable"`)
	require.NoError(t, err)
	assert.True(t, provider.IsDispatchable(request),
		"Request in=media: is unconstrained — axis is vacuously true")
}

// TEST826: is_dispatchable — provider output must satisfy request output (covariance)
func Test826_dispatch_covariant_output(t *testing.T) {
	provider, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:record;textable"`)
	require.NoError(t, err)
	request, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:textable"`)
	require.NoError(t, err)
	assert.True(t, provider.IsDispatchable(request),
		"Provider output record;textable conforms to request output textable")
}

// TEST827: is_dispatchable — provider with generic output cannot satisfy specific request
func Test827_dispatch_generic_output_fails(t *testing.T) {
	provider, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:"`)
	require.NoError(t, err)
	request, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:record;textable"`)
	require.NoError(t, err)
	assert.False(t, provider.IsDispatchable(request),
		"Provider out=media: cannot guarantee specific output")
}

// TEST828: is_dispatchable — wildcard * tag in request, provider missing tag → reject
func Test828_dispatch_wildcard_requires_tag_presence(t *testing.T) {
	provider, err := NewCapUrnFromString(`cap:in="media:model-spec";run-inference;out="media:record;textable"`)
	require.NoError(t, err)
	request, err := NewCapUrnFromString(`cap:candle=*;in="media:model-spec";run-inference;out="media:record;textable"`)
	require.NoError(t, err)
	assert.False(t, provider.IsDispatchable(request),
		"Wildcard * means tag must be present — provider has no candle tag")
}

// TEST829: is_dispatchable — wildcard * tag in request, provider has tag → accept
func Test829_dispatch_wildcard_with_tag_present(t *testing.T) {
	provider, err := NewCapUrnFromString(`cap:candle=metal;in="media:model-spec";run-inference;out="media:record;textable"`)
	require.NoError(t, err)
	request, err := NewCapUrnFromString(`cap:candle=*;in="media:model-spec";run-inference;out="media:record;textable"`)
	require.NoError(t, err)
	assert.True(t, provider.IsDispatchable(request),
		"Provider has candle=metal, request has candle=* — tag present, any value OK")
}

// TEST830: is_dispatchable — provider extra tags are refinement, always OK
func Test830_dispatch_provider_extra_tags(t *testing.T) {
	provider, err := NewCapUrnFromString(`cap:candle=metal;in="media:model-spec";run-inference;out="media:record;textable"`)
	require.NoError(t, err)
	request, err := NewCapUrnFromString(`cap:in="media:model-spec";run-inference;out="media:record;textable"`)
	require.NoError(t, err)
	assert.True(t, provider.IsDispatchable(request),
		"Provider extra tag candle=metal is refinement — always OK")
}

// TEST831: is_dispatchable — cross-backend mismatch prevented
func Test831_dispatch_cross_backend_mismatch(t *testing.T) {
	ggufProvider, err := NewCapUrnFromString(`cap:gguf=q4_k_m;in="media:model-spec";run-inference;out="media:record;textable"`)
	require.NoError(t, err)
	candleRequest, err := NewCapUrnFromString(`cap:candle=*;in="media:model-spec";run-inference;out="media:record;textable"`)
	require.NoError(t, err)
	assert.False(t, ggufProvider.IsDispatchable(candleRequest),
		"GGUF provider has no candle tag — cross-backend mismatch")
}

// TEST832: is_dispatchable is NOT symmetric
func Test832_dispatch_asymmetric(t *testing.T) {
	broad, err := NewCapUrnFromString(`cap:in="media:";process;out="media:record;textable"`)
	require.NoError(t, err)
	narrow, err := NewCapUrnFromString(`cap:in="media:pdf";process;out="media:textable"`)
	require.NoError(t, err)
	assert.True(t, broad.IsDispatchable(narrow))
	assert.False(t, narrow.IsDispatchable(broad))
}

// TEST833: is_comparable — both directions checked
func Test833_comparable_symmetric(t *testing.T) {
	a, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:textable"`)
	require.NoError(t, err)
	b, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:record;textable"`)
	require.NoError(t, err)
	assert.True(t, a.IsComparable(b))
	assert.True(t, b.IsComparable(a))
}

// TEST834: is_comparable — unrelated caps are NOT comparable
func Test834_comparable_unrelated(t *testing.T) {
	a, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:textable"`)
	require.NoError(t, err)
	b, err := NewCapUrnFromString(`cap:in="media:audio";transcribe;out="media:record;textable"`)
	require.NoError(t, err)
	assert.False(t, a.IsComparable(b))
	assert.False(t, b.IsComparable(a))
}

// TEST835: is_equivalent — identical caps
func Test835_equivalent_identical(t *testing.T) {
	a, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:record;textable"`)
	require.NoError(t, err)
	b, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:record;textable"`)
	require.NoError(t, err)
	assert.True(t, a.IsEquivalent(b))
	assert.True(t, b.IsEquivalent(a))
}

// TEST836: is_equivalent — non-equivalent comparable caps
func Test836_equivalent_non_equivalent(t *testing.T) {
	a, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:textable"`)
	require.NoError(t, err)
	b, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:record;textable"`)
	require.NoError(t, err)
	assert.True(t, a.IsComparable(b))
	assert.False(t, a.IsEquivalent(b))
}

// TEST837: is_dispatchable — op tag mismatch rejects
func Test837_dispatch_op_mismatch(t *testing.T) {
	provider, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:record;textable"`)
	require.NoError(t, err)
	request, err := NewCapUrnFromString(`cap:in="media:pdf";summarize;out="media:record;textable"`)
	require.NoError(t, err)
	assert.False(t, provider.IsDispatchable(request))
}

// TEST838: is_dispatchable — request with wildcard output accepts any provider output
func Test838_dispatch_request_wildcard_output(t *testing.T) {
	provider, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:record;textable"`)
	require.NoError(t, err)
	request, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:"`)
	require.NoError(t, err)
	assert.True(t, provider.IsDispatchable(request),
		"Request out=media: is unconstrained — any provider output accepted")
}

// JSON serialization test (not numbered in Rust)
func TestCapUrn_JSONSerialization(t *testing.T) {
	original, err := NewCapUrnFromString(testUrn("generate"))
	require.NoError(t, err)

	data, err := json.Marshal(original)
	assert.NoError(t, err)
	assert.NotNil(t, data)

	var decoded CapUrn
	err = json.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.True(t, original.Equals(&decoded))
}

// TEST561: in_media_urn and out_media_urn parse direction specs into MediaUrn
func Test561_in_out_media_urn(t *testing.T) {
	cap, err := NewCapUrnFromString(`cap:in="media:pdf";extract;out="media:txt;textable"`)
	require.NoError(t, err)

	inUrn, err := cap.InMediaUrn()
	require.NoError(t, err)
	assert.True(t, inUrn.IsBinary())
	assert.True(t, inUrn.HasTag("pdf"))

	outUrn, err := cap.OutMediaUrn()
	require.NoError(t, err)
	assert.True(t, outUrn.IsTextable())
	assert.True(t, outUrn.HasTag("txt"))

	// Bare media: should parse as valid MediaUrn
	wildcardCap, err := NewCapUrnFromString("cap:")
	require.NoError(t, err)
	wildcardIn, err := wildcardCap.InMediaUrn()
	require.NoError(t, err, "bare media: should parse as valid MediaUrn")
	assert.NotNil(t, wildcardIn)
}

// TEST562: canonical_option returns None for None input, canonical string for Some
func Test562_canonical_option(t *testing.T) {
	// nil input -> (nil, nil)
	result, err := CanonicalOption(nil)
	require.NoError(t, err)
	assert.Nil(t, result)

	// Valid input -> canonical form
	input := `cap:test;in="media:void";out="media:void"`
	result, err = CanonicalOption(&input)
	require.NoError(t, err)
	require.NotNil(t, result)
	// Parse both and verify they represent the same cap
	original, err := NewCapUrnFromString(input)
	require.NoError(t, err)
	reparsed, err := NewCapUrnFromString(*result)
	require.NoError(t, err)
	assert.True(t, original.Equals(reparsed))

	// Invalid input -> error
	invalid := "invalid"
	result, err = CanonicalOption(&invalid)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TEST568: is_dispatchable with different tag order in output spec
func Test568_dispatch_output_tag_order(t *testing.T) {
	provider, err := NewCapUrnFromString(
		`cap:in="media:model-spec;textable";download-model;out="media:download-result;record;textable"`)
	require.NoError(t, err)

	request, err := NewCapUrnFromString(
		`cap:in="media:model-spec;textable";download-model;out="media:download-result;record;textable"`)
	require.NoError(t, err)

	// After parsing, both should be normalized to same canonical form
	assert.Equal(t, provider.OutSpec(), request.OutSpec(),
		"Output specs should be normalized to same canonical form")

	// And dispatch should work
	assert.True(t, provider.IsDispatchable(request),
		"Provider should dispatch request with same tags in different order")
}

// TEST640: cap:in defaults out to media:
func Test640_wildcard_002_in_only_defaults_out_to_media(t *testing.T) {
	cap, err := NewCapUrnFromString("cap:in")
	require.NoError(t, err, "in without out should default out to media:")
	assert.Equal(t, "media:", cap.InSpec())
	assert.Equal(t, "media:", cap.OutSpec())
}

// TEST641: cap:out defaults in to media:
func Test641_wildcard_003_out_only_defaults_in_to_media(t *testing.T) {
	cap, err := NewCapUrnFromString("cap:out")
	require.NoError(t, err, "out without in should default in to media:")
	assert.Equal(t, "media:", cap.InSpec())
	assert.Equal(t, "media:", cap.OutSpec())
}

// TEST642: cap:in;out both become media:
func Test642_wildcard_004_in_out_no_values_become_media(t *testing.T) {
	cap, err := NewCapUrnFromString("cap:in;out")
	require.NoError(t, err, "in;out should both become media:")
	assert.Equal(t, "media:", cap.InSpec())
	assert.Equal(t, "media:", cap.OutSpec())
}

// TEST643: cap:in=*;out=* becomes media:
func Test643_wildcard_005_explicit_asterisk_becomes_media(t *testing.T) {
	cap, err := NewCapUrnFromString("cap:in=*;out=*")
	require.NoError(t, err, "in=*;out=* should become media:")
	assert.Equal(t, "media:", cap.InSpec())
	assert.Equal(t, "media:", cap.OutSpec())
}

// TEST644: cap:in=media:;out=* has specific in, wildcard out
func Test644_wildcard_006_specific_in_wildcard_out(t *testing.T) {
	cap, err := NewCapUrnFromString("cap:in=media:;out=*")
	require.NoError(t, err)
	assert.Equal(t, "media:", cap.InSpec())
	assert.Equal(t, "media:", cap.OutSpec())
}

// TEST645: cap:in=*;out=media:text has wildcard in, specific out
func Test645_wildcard_007_wildcard_in_specific_out(t *testing.T) {
	cap, err := NewCapUrnFromString(`cap:in=*;out="media:text"`)
	require.NoError(t, err)
	assert.Equal(t, "media:", cap.InSpec())
	assert.Equal(t, "media:text", cap.OutSpec())
}

// TEST646: cap:in=foo fails (invalid media URN)
func Test646_wildcard_008_invalid_in_spec_fails(t *testing.T) {
	_, err := NewCapUrnFromString("cap:in=foo;out=media:")
	require.Error(t, err)
}

// TEST647: cap:in=media:;out=bar fails (invalid media URN)
func Test647_wildcard_009_invalid_out_spec_fails(t *testing.T) {
	_, err := NewCapUrnFromString("cap:in=media:;out=bar")
	require.Error(t, err)
}

// TEST650: cap:in=media:;out=media:;test preserves other tags
func Test650_wildcard_012_preserve_other_tags(t *testing.T) {
	cap, err := NewCapUrnFromString("cap:in;out;test")
	require.NoError(t, err)
	assert.Equal(t, "media:", cap.InSpec())
	assert.Equal(t, "media:", cap.OutSpec())
	assert.True(t, cap.HasMarkerTag("test"))
}

// -------------------------------------------------------------------
// CapKind classifier tests (test1800–test1805).
//
// Mirrored across every language port (Rust, Go, Python, Swift/ObjC,
// JS) under the SAME numbers. Any divergence is a wire-level
// inconsistency — the kind taxonomy is part of the protocol's public
// surface, not a per-port detail.
// -------------------------------------------------------------------

// TEST1800: Identity classifier — only the bare cap: form qualifies.
// `cap:` is the fully generic morphism on every axis; adding any
// tag (even one that doesn't constrain in/out) demotes the cap to
// Transform because the operation/metadata axis is no longer fully
// generic.
func Test1800_kind_identity_only_for_bare_cap(t *testing.T) {
	identity, err := NewCapUrnFromString("cap:")
	require.NoError(t, err)
	kind, err := identity.Kind()
	require.NoError(t, err)
	assert.Equal(t, CapKindIdentity, kind)

	for _, spelling := range []string{
		"cap:in=media:;out=media:",
		"cap:in=*;out=*",
		"cap:in=media:",
		"cap:out=media:",
	} {
		cap, err := NewCapUrnFromString(spelling)
		require.NoError(t, err, spelling)
		k, err := cap.Kind()
		require.NoError(t, err)
		assert.Equal(t, CapKindIdentity, k,
			"%s should classify as Identity (canonical form is `cap:`)", spelling)
	}

	withOp, err := NewCapUrnFromString("cap:passthrough")
	require.NoError(t, err)
	k, err := withOp.Kind()
	require.NoError(t, err)
	assert.Equal(t, CapKindTransform, k,
		"cap:passthrough specifies the operation axis — not Identity")
}

// TEST1801: Source classifier — in=media:void, out non-void. The y
// dimension may carry any tags; void on the input alone is what
// matters.
func Test1801_kind_source_when_input_is_void(t *testing.T) {
	warm, err := NewCapUrnFromString(`cap:in=media:void;out="media:model-artifact";warm`)
	require.NoError(t, err)
	k, err := warm.Kind()
	require.NoError(t, err)
	assert.Equal(t, CapKindSource, k)

	gen, err := NewCapUrnFromString("cap:in=media:void;out=media:textable")
	require.NoError(t, err)
	k, err = gen.Kind()
	require.NoError(t, err)
	assert.Equal(t, CapKindSource, k)
}

// TEST1802: Sink classifier — out=media:void, in non-void.
func Test1802_kind_sink_when_output_is_void(t *testing.T) {
	discard, err := NewCapUrnFromString("cap:discard;in=media:;out=media:void")
	require.NoError(t, err)
	k, err := discard.Kind()
	require.NoError(t, err)
	assert.Equal(t, CapKindSink, k)

	logCap, err := NewCapUrnFromString(`cap:in="media:json;textable";log;out=media:void`)
	require.NoError(t, err)
	k, err = logCap.Kind()
	require.NoError(t, err)
	assert.Equal(t, CapKindSink, k)
}

// TEST1803: Effect classifier — both sides void. Reads as `() → ()`.
func Test1803_kind_effect_when_both_sides_void(t *testing.T) {
	ping, err := NewCapUrnFromString("cap:in=media:void;out=media:void;ping")
	require.NoError(t, err)
	k, err := ping.Kind()
	require.NoError(t, err)
	assert.Equal(t, CapKindEffect, k)

	bare, err := NewCapUrnFromString("cap:in=media:void;out=media:void")
	require.NoError(t, err)
	k, err = bare.Kind()
	require.NoError(t, err)
	assert.Equal(t, CapKindEffect, k)
}

// TEST1804: Transform classifier — at least one side non-void, and
// the cap is not the bare identity. The default kind for ordinary
// data-processing caps.
func Test1804_kind_transform_for_normal_data_processors(t *testing.T) {
	extract, err := NewCapUrnFromString(`cap:extract;in=media:pdf;out="media:record;textable"`)
	require.NoError(t, err)
	k, err := extract.Kind()
	require.NoError(t, err)
	assert.Equal(t, CapKindTransform, k)

	labeled, err := NewCapUrnFromString("cap:passthrough;in=media:;out=media:")
	require.NoError(t, err)
	k, err = labeled.Kind()
	require.NoError(t, err)
	assert.Equal(t, CapKindTransform, k)
}

// TEST1805: Kind is invariant under canonicalization. The same
// morphism written in many surface forms must classify the same way
// once parsed. Pins the rule that kind is a property of the cap as
// a structured object, not of any particular spelling.
func Test1805_kind_invariant_under_canonical_spellings(t *testing.T) {
	cases := []struct {
		a, b     string
		expected CapKind
	}{
		{"cap:", "cap:in=media:;out=media:", CapKindIdentity},
		{
			"cap:extract;in=media:pdf;out=media:textable",
			`cap:extract;in="media:pdf";out="media:textable"`,
			CapKindTransform,
		},
		{
			"cap:in=media:void;out=media:textable;warm",
			"cap:warm;out=media:textable;in=media:void",
			CapKindSource,
		},
	}

	for _, c := range cases {
		capA, err := NewCapUrnFromString(c.a)
		require.NoError(t, err, c.a)
		capB, err := NewCapUrnFromString(c.b)
		require.NoError(t, err, c.b)
		kindA, err := capA.Kind()
		require.NoError(t, err)
		kindB, err := capB.Kind()
		require.NoError(t, err)
		assert.Equal(t, c.expected, kindA, "%s should classify as %s", c.a, c.expected)
		assert.Equal(t, c.expected, kindB, "%s should classify as %s", c.b, c.expected)
		assert.Equal(t, kindA, kindB,
			"%s and %s parse to the same cap and must classify identically", c.a, c.b)
	}
}

// -------------------------------------------------------------------
// Truth-table specificity tests (test1820–test1824).
//
// Mirrored across every language port (Rust, Go, Python, Swift/ObjC,
// JS) under the SAME numbers. Specificity must be the truth-table
// sum across all three axes using the six-form ladder:
//
//   ?x or missing     -> 0   (no constraint)
//   x?=v              -> 1   (absent OR not v)
//   x (=x=*) marker   -> 2   (must-have-any)
//   x!=v              -> 3   (present and not v)
//   x=v exact         -> 4   (must-have-this-value)
//   !x                -> 5   (must-not-have)
// -------------------------------------------------------------------

// TEST1820: A `?`-valued cap-tag scores 0. Same as missing.
func Test1820_specificity_question_is_zero(t *testing.T) {
	bare, err := NewCapUrnFromString("cap:")
	require.NoError(t, err)
	assert.Equal(t, 0, bare.Specificity())

	withQ, err := NewCapUrnFromString("cap:?target")
	require.NoError(t, err)
	assert.Equal(t, 0, withQ.Specificity(),
		"?x must score 0 (explicit no-constraint, same as missing)")
}

// TEST1821: A `!`-valued cap-tag scores 5 (top of negative chain).
func Test1821_specificity_must_not_have_is_five(t *testing.T) {
	cap, err := NewCapUrnFromString("cap:!constrained")
	require.NoError(t, err)
	assert.Equal(t, 5, cap.Specificity(),
		"!constrained (must-not-have) must score 5")
}

// TEST1822: A `*`-valued cap-tag (including bare markers) scores 2.
func Test1822_specificity_must_have_any_is_two(t *testing.T) {
	bareMarker, err := NewCapUrnFromString("cap:extract")
	require.NoError(t, err)
	assert.Equal(t, 2, bareMarker.Specificity(),
		"bare `extract` parses as extract=* (must-have-any) and scores 2")

	explicitStar, err := NewCapUrnFromString("cap:extract=*")
	require.NoError(t, err)
	assert.Equal(t, 2, explicitStar.Specificity(),
		"explicit key=* must score 2 (same as bare marker)")

	assert.Equal(t, bareMarker.Specificity(), explicitStar.Specificity(),
		"bare marker and explicit key=* are the same form and must score identically")
}

// TEST1823: An exact-valued cap-tag scores 4.
func Test1823_specificity_exact_value_is_four(t *testing.T) {
	cap, err := NewCapUrnFromString("cap:target=metadata")
	require.NoError(t, err)
	assert.Equal(t, 4, cap.Specificity(),
		"target=metadata (exact value) must score 4")
}

// TEST1824: All six forms compose additively on a single cap.
// y combining 0+1+2+3+4+5 must sum to 15.
func Test1824_specificity_combined_y_axis(t *testing.T) {
	cap, err := NewCapUrnFromString(
		"cap:!constrained;?target;extract;stage!=alpha;target2=metadata;ver?=draft")
	require.NoError(t, err)
	assert.Equal(t, 15, cap.Specificity(),
		"y combining all six forms (0+1+2+3+4+5) must sum to 15")
}

// -------------------------------------------------------------------
// Six-form canonicalization tests (test1830–test1835).
//
// Mirrored across every language port (Rust, Go, Python, Swift/ObjC,
// JS) under the SAME numbers. Each test enumerates the authored
// aliases for one canonical form and verifies that ALL of them parse
// to the same canonical serialization.
// -------------------------------------------------------------------

// TEST1830: ?x ≡ x? ≡ x=? all canonicalize to ?x.
func Test1830_canonicalize_no_constraint(t *testing.T) {
	canonical := "cap:?x"
	for _, input := range []string{"cap:?x", "cap:x?", "cap:x=?"} {
		cap, err := NewCapUrnFromString(input)
		require.NoError(t, err, "input %q must parse", input)
		assert.Equal(t, canonical, cap.String(),
			"input %q must canonicalize to %q", input, canonical)
	}
}

// TEST1831: ?x=v and x?=v both canonicalize to x?=v. The third
// hypothetical form `x=?v` is NOT recognized as a qualifier — a
// value starting with `?` is just an exact value beginning with
// a `?` character.
func Test1831_canonicalize_absent_or_not_value(t *testing.T) {
	canonical := "cap:x?=foo"
	for _, input := range []string{"cap:?x=foo", "cap:x?=foo"} {
		cap, err := NewCapUrnFromString(input)
		require.NoError(t, err, "input %q must parse", input)
		assert.Equal(t, canonical, cap.String(),
			"input %q must canonicalize to %q", input, canonical)
	}

	// `x=?foo` is a plain exact tag whose value is the string `?foo`
	// — NOT a canonicalization alias.
	exact, err := NewCapUrnFromString("cap:x=?foo")
	require.NoError(t, err)
	assert.Equal(t, "cap:x=?foo", exact.String())
	val, ok := exact.GetTag("x")
	assert.True(t, ok)
	assert.Equal(t, "?foo", val)
}

// TEST1832: x ≡ x=* both canonicalize to bare x.
func Test1832_canonicalize_must_have_any(t *testing.T) {
	canonical := "cap:x"
	for _, input := range []string{"cap:x", "cap:x=*"} {
		cap, err := NewCapUrnFromString(input)
		require.NoError(t, err, "input %q must parse", input)
		assert.Equal(t, canonical, cap.String(),
			"input %q must canonicalize to %q", input, canonical)
	}
}

// TEST1833: !x=v and x!=v both canonicalize to x!=v. The third
// hypothetical form `x=!v` is NOT recognized as a qualifier — a
// value starting with `!` is just an exact value beginning with
// a `!` character.
func Test1833_canonicalize_present_not_value(t *testing.T) {
	canonical := "cap:x!=foo"
	for _, input := range []string{"cap:!x=foo", "cap:x!=foo"} {
		cap, err := NewCapUrnFromString(input)
		require.NoError(t, err, "input %q must parse", input)
		assert.Equal(t, canonical, cap.String(),
			"input %q must canonicalize to %q", input, canonical)
	}

	// `x=!foo` is a plain exact tag whose value is the string `!foo`
	// — NOT a canonicalization alias.
	exact, err := NewCapUrnFromString("cap:x=!foo")
	require.NoError(t, err)
	assert.Equal(t, "cap:x=!foo", exact.String())
	val, ok := exact.GetTag("x")
	assert.True(t, ok)
	assert.Equal(t, "!foo", val)
}

// TEST1834: x=v stays as x=v.
func Test1834_canonicalize_exact_value(t *testing.T) {
	cap, err := NewCapUrnFromString("cap:x=foo")
	require.NoError(t, err)
	assert.Equal(t, "cap:x=foo", cap.String())
}

// TEST1835: !x ≡ x! ≡ x=! all canonicalize to !x.
func Test1835_canonicalize_must_not_have(t *testing.T) {
	canonical := "cap:!x"
	for _, input := range []string{"cap:!x", "cap:x!", "cap:x=!"} {
		cap, err := NewCapUrnFromString(input)
		require.NoError(t, err, "input %q must parse", input)
		assert.Equal(t, canonical, cap.String(),
			"input %q must canonicalize to %q", input, canonical)
	}
}

// TEST1842: Full 6×6 truth table — every cell must match the matrix
// in 04-PREDICATES.md §2.5.
func Test1842_truth_table_full_cross_product(t *testing.T) {
	forms := []string{"", "?x", "x?=v", "x", "x!=v", "x=v", "!x"}
	expected := [7][7]bool{
		//         miss   ?x    x?=v   x      x!=v   x=v    !x
		/*miss*/  {true, true, true, false, false, false, true},
		/*?x*/    {true, true, true, true, true, true, true},
		/*x?=v*/  {true, true, true, false, false, false, true},
		/*x*/     {true, true, true, true, true, true, false},
		/*x!=v*/  {true, true, true, true, true, false, false},
		/*x=v*/   {true, true, false, true, false, true, false},
		/*!x*/    {true, true, true, false, false, false, true},
	}
	for i, instForm := range forms {
		for j, pattForm := range forms {
			instStr := "cap:"
			if instForm != "" {
				instStr = "cap:" + instForm
			}
			pattStr := "cap:"
			if pattForm != "" {
				pattStr = "cap:" + pattForm
			}
			inst, err := NewCapUrnFromString(instStr)
			require.NoError(t, err)
			patt, err := NewCapUrnFromString(pattStr)
			require.NoError(t, err)
			actual := patt.Accepts(inst)
			assert.Equal(t, expected[i][j], actual,
				"cell (inst=%q, patt=%q) expected %v got %v",
				instForm, pattForm, expected[i][j], actual)
		}
	}
}

// TEST1843: Invalid qualifier combinations must be rejected.
func Test1843_reject_invalid_combinations(t *testing.T) {
	invalid := []string{
		"cap:?x?=v",
		"cap:!x!=v",
		"cap:?!x",
		"cap:!?x",
		"cap:?x=*",
		"cap:!x=*",
		"cap:?x=?",
		"cap:?x=!",
		"cap:!x=?",
		"cap:!x=!",
		"cap:?",
		"cap:!",
	}
	for _, input := range invalid {
		_, err := NewCapUrnFromString(input)
		assert.Error(t, err, "input %q must be rejected", input)
	}
}

// TEST1844: out-axis difference dominates combined in+y differences.
func Test1844_axis_weighting_out_dominates(t *testing.T) {
	bigOut, err := NewCapUrnFromString(`cap:in=media:;out="media:record;textable"`)
	require.NoError(t, err)
	bigInAndY, err := NewCapUrnFromString(
		"cap:in=media:pdf;out=media:record;!constrained;?target;extract;stage!=alpha;target2=metadata;ver?=draft")
	require.NoError(t, err)
	assert.Greater(t, bigOut.Specificity(), bigInAndY.Specificity(),
		"out-axis difference must dominate combined in+y differences")
}

// TEST1845: With equal out, in-axis dominates over y-axis.
func Test1845_axis_weighting_in_dominates_y(t *testing.T) {
	bigIn, err := NewCapUrnFromString("cap:in=media:pdf;out=media:record")
	require.NoError(t, err)
	bigY, err := NewCapUrnFromString(
		"cap:in=media:;out=media:record;!constrained;?target;extract;stage!=alpha;target2=metadata;ver?=draft")
	require.NoError(t, err)
	assert.Greater(t, bigIn.Specificity(), bigY.Specificity(),
		"in-axis difference must dominate y-axis")
}

// TEST1846: Decoded layout — 10000*out + 100*in + y.
func Test1846_axis_weighting_decoded_layout(t *testing.T) {
	cap, err := NewCapUrnFromString(
		`cap:in="media:a;b";out="media:a;b;c;d";extract`)
	require.NoError(t, err)
	// out=4 markers (8), in=2 markers (4), y=1 marker (2)
	// 10000*8 + 100*4 + 2 = 80402
	assert.Equal(t, 10000*8+100*4+2, cap.Specificity())
}
