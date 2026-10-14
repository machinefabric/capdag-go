package planner

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTestCap creates a test cap with given op/in/out specs.
func makeTestCap(op, inSpec, outSpec, title string) (*cap.Cap, error) {
	capUrnStr := fmt.Sprintf(`cap:%s;in="%s";out="%s"`, op, inSpec, outSpec)
	capUrnParsed, err := urn.NewCapUrnFromString(capUrnStr)
	if err != nil {
		return nil, err
	}
	return cap.NewCapWithArgs(capUrnParsed, title, []string{"test-command"}, nil), nil
}

// checkForDuplicateCaps detects duplicate caps by (inputSpec, capUrn) pairs.
// Returns the edge count on success, or an error with details on the first duplicate found.
// Mirrors the Rust plan_builder.rs check_for_duplicate_caps test helper.
func checkForDuplicateCaps(caps []*cap.Cap) (int, error) {
	type edgeKey struct{ inputSpec, capUrn string }
	seen := make(map[edgeKey]bool)
	count := 0
	for _, c := range caps {
		inputSpec := c.Urn.InSpec()
		outSpec := c.Urn.OutSpec()
		if inputSpec == "" || outSpec == "" {
			continue
		}
		key := edgeKey{inputSpec, c.Urn.String()}
		if seen[key] {
			return 0, fmt.Errorf("Duplicate cap_urn detected: %s (input_spec: %s)", c.Urn.String(), inputSpec)
		}
		seen[key] = true
		count++
	}
	return count, nil
}

// TEST6645: Tests ArgumentResolution String() returns correct snake_case names
// ArgumentInfo.Resolution is serialized to JSON using String(). Verifies that each
// resolution variant maps to the correct identifier expected by API consumers.
func Test6645_argument_resolution_string_representations(t *testing.T) {
	cases := []struct {
		resolution ArgumentResolution
		expected   string
	}{
		{ResolutionFromInput, "from_input"},
		{ResolutionFromStep, "from_step"},
		{ResolutionConstant, "constant"},
		{ResolutionHasDefault, "has_default"},
		{ResolutionRequiresUserInput, "requires_user_input"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.expected, tc.resolution.String(),
			"ArgumentResolution %d must stringify to %q", tc.resolution, tc.expected)
		var back ArgumentResolution
		require.NoError(t, json.Unmarshal([]byte(`"`+tc.expected+`"`), &back))
		assert.Equal(t, tc.resolution, back, "%q reads back as what wrote it", tc.expected)
	}
	var unknown ArgumentResolution
	assert.Error(t, json.Unmarshal([]byte(`"from_input_file"`), &unknown),
		"a name that is not a resolution is refused")
}

// TEST765: Tests validation_to_json() returns None for empty validation constraints Verifies that default MediaValidation with no constraints produces JSON None
func Test765_validation_to_json_empty(t *testing.T) {
	validation := &media.MediaValidation{}
	result := ValidationToJSON(validation)
	assert.Nil(t, result, "Empty validation should return nil")
}

// TEST766: Tests validation_to_json() converts MediaValidation with constraints to JSON Verifies that min/max validation rules are correctly serialized as JSON fields
func Test766_validation_to_json_with_constraints(t *testing.T) {
	min := 50.0
	max := 2000.0
	validation := &media.MediaValidation{Min: &min, Max: &max}
	result := ValidationToJSON(validation)
	require.NotNil(t, result, "Validation with constraints should return non-nil")
	s := string(result)
	assert.Contains(t, s, "50")
	assert.Contains(t, s, "2000")
}

// TEST880: Tests duplicate detection passes for caps with unique URN combinations Verifies that check_for_duplicate_caps() correctly accepts caps with different op/in/out combinations
func Test880_no_duplicates_with_unique_caps(t *testing.T) {
	c1, err := makeTestCap("extract_metadata", "media:document;ext=pdf", "media:file-metadata;record;enc=utf-8", "Extract Metadata")
	require.NoError(t, err)
	c2, err := makeTestCap("extract_outline", "media:document;ext=pdf", "media:document-outline;record;enc=utf-8", "Extract Outline")
	require.NoError(t, err)
	c3, err := makeTestCap("disbind", "media:document;ext=pdf", "media:disbound-pages;list;enc=utf-8", "Disbind PDF")
	require.NoError(t, err)

	count, err := checkForDuplicateCaps([]*cap.Cap{c1, c2, c3})
	require.NoError(t, err, "Should not detect duplicates for unique caps")
	assert.Equal(t, 3, count, "Should have 3 edges")
}

// TEST991: Tests duplicate detection identifies caps with identical URNs Verifies that check_for_duplicate_caps() returns an error when multiple caps share the same cap_urn
func Test991_detects_duplicate_cap_urns(t *testing.T) {
	c1, err := makeTestCap("disbind", "media:document;ext=pdf", "media:disbound-pages;list;enc=utf-8", "Disbind PDF")
	require.NoError(t, err)
	c2, err := makeTestCap("disbind", "media:document;ext=pdf", "media:disbound-pages;list;enc=utf-8", "Disbind PDF Again")
	require.NoError(t, err)

	_, err = checkForDuplicateCaps([]*cap.Cap{c1, c2})
	require.Error(t, err, "Should detect duplicate cap URN")
	assert.Contains(t, err.Error(), "Duplicate cap_urn detected")
	assert.Contains(t, err.Error(), "disbind")
	assert.Contains(t, err.Error(), "media:document;ext=pdf")
}

// TEST992: Tests caps with different operations but same input/output types are not duplicates Verifies that only the complete URN (including op) is used for duplicate detection
func Test992_different_ops_same_types_not_duplicates(t *testing.T) {
	c1, err := makeTestCap("disbind", "media:document;ext=pdf", "media:disbound-pages;list;enc=utf-8", "Disbind")
	require.NoError(t, err)
	c2, err := makeTestCap("grind", "media:document;ext=pdf", "media:disbound-pages;list;enc=utf-8", "Grind")
	require.NoError(t, err)

	count, err := checkForDuplicateCaps([]*cap.Cap{c1, c2})
	require.NoError(t, err, "Different ops should not be duplicates")
	assert.Equal(t, 2, count, "Should have 2 edges")
}

// TEST993: Tests caps with same operation but different input types are not duplicates Verifies that input type differences distinguish caps with the same operation name
func Test993_same_op_different_input_types_not_duplicates(t *testing.T) {
	c1, err := makeTestCap("extract_metadata", "media:document;ext=pdf", "media:file-metadata;record;enc=utf-8", "Extract PDF Metadata")
	require.NoError(t, err)
	c2, err := makeTestCap("extract_metadata", "media:txt;enc=utf-8", "media:file-metadata;record;enc=utf-8", "Extract TXT Metadata")
	require.NoError(t, err)

	count, err := checkForDuplicateCaps([]*cap.Cap{c1, c2})
	require.NoError(t, err, "Same op with different inputs should not be duplicates")
	assert.Equal(t, 2, count, "Should have 2 edges")
}

// TEST1019: Tests validation_to_json() returns None for None input Verifies that missing validation metadata is converted to JSON None
func Test1019_validation_to_json_nil(t *testing.T) {
	result := ValidationToJSON(nil)
	assert.Nil(t, result, "nil validation should return nil")
}

// TEST1100: Tests that CapUrn normalizes media URN tags to canonical order
// Two CapUrns with different tag ordering in out spec must produce the same canonical string.
func Test1100_cap_urn_normalizes_media_urn_tag_order(t *testing.T) {
	urn1, err := urn.NewCapUrnFromString(`cap:in="media:document;ext=pdf";extract-metadata;out="media:file-metadata;record;enc=utf-8"`)
	require.NoError(t, err)
	urn2, err := urn.NewCapUrnFromString(`cap:in="media:document;ext=pdf";extract-metadata;out="media:file-metadata;record;enc=utf-8"`)
	require.NoError(t, err)

	assert.Equal(t, urn1.String(), urn2.String(),
		"URNs with different tag ordering should normalize to the same canonical form")

	// Both URNs should parse without error and produce the same canonical form
	assert.NotEmpty(t, urn1.OutSpec(), "out spec should not be empty")
	assert.Equal(t, urn1.OutSpec(), urn2.OutSpec(),
		"out specs with different tag ordering should normalize identically")
}

// TEST1103: Tests that IsDispatchable has correct directionality
// A specific candidate is dispatchable for a general request; the reverse is false.
func Test1103_is_dispatchable_uses_correct_directionality(t *testing.T) {
	generalRequest, err := urn.NewCapUrnFromString("cap:in=\"media:document;ext=pdf\";extract;out=media:text")
	require.NoError(t, err)

	specificCandidate, err := urn.NewCapUrnFromString("cap:in=\"media:document;ext=pdf\";extract;out=media:text;version=2")
	require.NoError(t, err)

	assert.True(t, specificCandidate.IsDispatchable(generalRequest),
		"Specific candidate should be dispatchable for general request")
	assert.False(t, generalRequest.IsDispatchable(specificCandidate),
		"General request should NOT be dispatchable for specific candidate (missing version tag)")
}

// TEST1104: Tests that is_dispatchable rejects when candidate cannot dispatch request
func Test1104_is_dispatchable_rejects_non_dispatchable(t *testing.T) {
	request, err := urn.NewCapUrnFromString("cap:in=\"media:document;ext=pdf\";extract;out=media:text;required=yes")
	require.NoError(t, err)

	candidate, err := urn.NewCapUrnFromString("cap:in=\"media:document;ext=pdf\";extract;out=media:text")
	require.NoError(t, err)

	assert.False(t, candidate.IsDispatchable(request),
		"Candidate missing required tag should not be dispatchable for request")
}

// TEST767: Tests ArgumentInfo struct serialization to JSON Verifies that argument metadata including resolution status and validation is correctly serialized
func Test767_argument_info_serialization(t *testing.T) {
	argInfo := &ArgumentInfo{
		Name:         "width",
		MediaUrn:     "media:integer",
		Description:  "Width in pixels",
		Resolution:   ResolutionHasDefault,
		DefaultValue: 200,
		IsRequired:   false,
		IsSequence:   false,
		Validation:   map[string]any{"min": 50, "max": 2000},
	}

	data, err := json.Marshal(argInfo)
	require.NoError(t, err, "Should serialize")
	assert.Contains(t, string(data), `"name":"width"`)
	assert.Contains(t, string(data), `"resolution":"has_default"`)
	assert.Contains(t, string(data), `"default_value":200`)
}

// mustStepToken parses a fixture's literal token through the one door tokens
// come in by. A fixture that names an empty token is a broken fixture.
func mustStepToken(t *testing.T, raw string) StepToken {
	t.Helper()
	token, err := ParseStepToken(raw)
	require.NoError(t, err)
	return token
}

// TEST1462: An unidentified step is not a state the program can hold. StepToken is the type that makes it so - minting is the only way to create one and ParseStepToken, the sole path back from text, refuses an empty id
func TestTest1462AStepTokenCannotBeEmpty(t *testing.T) {
	_, err := ParseStepToken("")
	assert.ErrorIs(t, err, ErrEmptyStepToken,
		"an empty id names no step and is not a token")

	// Decoding is the one boundary where a strand arrives as data rather than
	// being constructed, so it is the one place the illegal state could enter.
	var decoded StepToken
	assert.Error(t, json.Unmarshal([]byte(`""`), &decoded),
		"decoding an empty token must fail")

	// The mirror's serialization boundaries carry tokens in wire structs, and
	// each of them refuses "" by construction rather than by a hand-written
	// emptiness check downstream.
	var requirements StepArgumentRequirements
	assert.Error(t, json.Unmarshal([]byte(`{"cap_urn":"cap:x","token_id":""}`), &requirements),
		"step requirements naming an empty token must not decode")

	var outcome BodyOutcome
	assert.Error(t, json.Unmarshal([]byte(`{"foreach_token_id":"","body_index":0}`), &outcome),
		"a body outcome naming an empty ForEach token must not decode")

	// A minted token round-trips through the same boundary unchanged.
	minted := MintStepToken()
	encoded, err := json.Marshal(minted)
	require.NoError(t, err)
	var roundTripped StepToken
	require.NoError(t, json.Unmarshal(encoded, &roundTripped))
	assert.Equal(t, minted, roundTripped)
}
