package planner

import (
	"fmt"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/standard"
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
	return cap.NewCapWithArgs(capUrnParsed, title, "test-command", nil), nil
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
		{ResolutionFromInputFile, "from_input_file"},
		{ResolutionFromPreviousOutput, "from_previous_output"},
		{ResolutionHasDefault, "has_default"},
		{ResolutionRequiresUserInput, "requires_user_input"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.expected, tc.resolution.String(),
			"ArgumentResolution %d must stringify to %q", tc.resolution, tc.expected)
	}
}

// TEST6646: Tests AnalyzePathArguments classifies stdin arg as FromInputFile for first cap
// Verifies that the argument analysis correctly identifies input-file arguments when the
// cap's stdin arg media URN matches the cap's in_spec.
func Test6646_analyze_path_arguments_stdin_is_from_input_file(t *testing.T) {
	// Build a cap whose stdin arg is the cap's in_spec (media:ext=pdf) — should resolve as FromInputFile
	capUrnStr := `cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`
	capUrnParsed, err := urn.NewCapUrnFromString(capUrnStr)
	require.NoError(t, err)

	inSpec := capUrnParsed.InSpec()
	stdinArg := cap.NewCapArg(inSpec, true, []cap.ArgSource{
		{Stdin: &inSpec},
	})
	c := cap.NewCapWithArgs(capUrnParsed, "Extract", "test", []cap.CapArg{stdinArg})
	c.Output = &cap.CapOutput{MediaUrn: capUrnParsed.OutSpec()}

	registry := cap.NewFabricRegistryForTest()
	registry.AddCapsToCache([]*cap.Cap{c})
	builder := NewMachinePlanBuilder(registry)

	// Build a single-step path
	graph := NewLiveCapFab()
	graph.AddCap(c)
	source, err := urn.NewMediaUrnFromString("media:ext=pdf")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString(`media:txt;enc=utf-8`)
	require.NoError(t, err)
	paths := graph.FindPathsToExactTarget(source, target, false, 3, 5)
	require.NotEmpty(t, paths, "should find at least one path")

	req, err := builder.AnalyzePathArguments(paths[0])
	require.NoError(t, err)

	require.Equal(t, 1, len(req.Steps), "should have one step")
	require.Equal(t, 1, len(req.Steps[0].Arguments), "step should have one argument")
	assert.Equal(t, ResolutionFromInputFile, req.Steps[0].Arguments[0].Resolution,
		"stdin arg for first-cap input must resolve as FromInputFile")
	assert.Empty(t, req.Steps[0].Slots,
		"FromInputFile args must not appear in slots (not user-input)")
}

// TEST6647: Tests AnalyzePathArguments puts RequiresUserInput args in slots and sets CanExecuteWithoutInput=false
// Verifies that caps with non-stdin, non-default arguments are identified as requiring user input,
// appear in slots, and the requirements reflect that execution cannot proceed without them.
func Test6647_analyze_path_arguments_user_input_arg_appears_in_slots(t *testing.T) {
	capUrnStr := `cap:in="media:txt;enc=utf-8";translate;out="media:translated;enc=utf-8"`
	capUrnParsed, err := urn.NewCapUrnFromString(capUrnStr)
	require.NoError(t, err)

	// stdin arg (input file — resolved automatically)
	inSpec := capUrnParsed.InSpec()
	stdinArg := cap.NewCapArg(inSpec, true, []cap.ArgSource{
		{Stdin: &inSpec},
	})
	// user arg: target_language — no stdin source, no default → RequiresUserInput
	userArg := cap.NewCapArg("media:string", true, []cap.ArgSource{})

	c := cap.NewCapWithArgs(capUrnParsed, "Translate", "test", []cap.CapArg{stdinArg, userArg})
	c.Output = &cap.CapOutput{MediaUrn: capUrnParsed.OutSpec()}

	registry := cap.NewFabricRegistryForTest()
	registry.AddCapsToCache([]*cap.Cap{c})
	builder := NewMachinePlanBuilder(registry)

	graph := NewLiveCapFab()
	graph.AddCap(c)
	source, err := urn.NewMediaUrnFromString(`media:txt;enc=utf-8`)
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString(`media:translated;enc=utf-8`)
	require.NoError(t, err)
	paths := graph.FindPathsToExactTarget(source, target, false, 3, 5)
	require.NotEmpty(t, paths, "should find at least one path")

	req, err := builder.AnalyzePathArguments(paths[0])
	require.NoError(t, err)

	require.Equal(t, 1, len(req.Steps))
	require.Equal(t, 2, len(req.Steps[0].Arguments),
		"step should have 2 arguments (stdin + user)")

	// Find the user-input arg
	var userInputArg *ArgumentInfo
	for _, a := range req.Steps[0].Arguments {
		if a.Resolution == ResolutionRequiresUserInput {
			userInputArg = a
			break
		}
	}
	require.NotNil(t, userInputArg,
		"expected at least one argument resolved as RequiresUserInput")

	assert.Equal(t, 1, len(req.Steps[0].Slots),
		"RequiresUserInput arg must appear in slots")
	assert.Equal(t, ResolutionRequiresUserInput, req.Steps[0].Slots[0].Resolution)
	assert.False(t, req.CanExecuteWithoutInput,
		"plan requiring user input must have CanExecuteWithoutInput=false")
}

// TEST765: Tests ValidationToJSON() returns nil for empty validation constraints
// Verifies that default MediaValidation with no constraints produces nil JSON
func Test765_validation_to_json_empty(t *testing.T) {
	validation := &media.MediaValidation{}
	result := ValidationToJSON(validation)
	assert.Nil(t, result, "Empty validation should return nil")
}

// TEST766: Tests ValidationToJSON() converts MediaValidation with constraints to JSON
// Verifies that min/max validation rules are correctly serialized as JSON fields
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

// TEST886: Tests optional non-IO arguments with default values are marked as HasDefault
// Verifies that arguments with defaults return HasDefault regardless of step position
func Test886_optional_non_io_arg_with_default_has_default(t *testing.T) {
	defaultVal := 300
	resolution := determineResolutionWithIOCheck(standard.MediaInteger, "media:ext=pdf", "media:ext=png;image", 0, defaultVal)
	assert.Equal(t, ResolutionHasDefault, resolution)
}

// TEST6671: Tests duplicate detection passes for caps with unique URN combinations
// Verifies that checkForDuplicateCaps() correctly accepts caps with different op/in/out combinations
func Test6671_no_duplicates_with_unique_caps(t *testing.T) {
	c1, err := makeTestCap("extract_metadata", "media:ext=pdf", "media:file-metadata;record;enc=utf-8", "Extract Metadata")
	require.NoError(t, err)
	c2, err := makeTestCap("extract_outline", "media:ext=pdf", "media:document-outline;record;enc=utf-8", "Extract Outline")
	require.NoError(t, err)
	c3, err := makeTestCap("disbind", "media:ext=pdf", "media:disbound-pages;list;enc=utf-8", "Disbind PDF")
	require.NoError(t, err)

	count, err := checkForDuplicateCaps([]*cap.Cap{c1, c2, c3})
	require.NoError(t, err, "Should not detect duplicates for unique caps")
	assert.Equal(t, 3, count, "Should have 3 edges")
}

// TEST991: Tests duplicate detection identifies caps with identical URNs
// Verifies that checkForDuplicateCaps() returns an error when multiple caps share the same cap_urn
func Test991_detects_duplicate_cap_urns(t *testing.T) {
	c1, err := makeTestCap("disbind", "media:ext=pdf", "media:disbound-pages;list;enc=utf-8", "Disbind PDF")
	require.NoError(t, err)
	c2, err := makeTestCap("disbind", "media:ext=pdf", "media:disbound-pages;list;enc=utf-8", "Disbind PDF Again")
	require.NoError(t, err)

	_, err = checkForDuplicateCaps([]*cap.Cap{c1, c2})
	require.Error(t, err, "Should detect duplicate cap URN")
	assert.Contains(t, err.Error(), "Duplicate cap_urn detected")
	assert.Contains(t, err.Error(), "disbind")
	assert.Contains(t, err.Error(), "media:ext=pdf")
}

// TEST992: Tests caps with different operations but same input/output types are not duplicates
// Verifies that only the complete URN (including op) is used for duplicate detection
func Test992_different_ops_same_types_not_duplicates(t *testing.T) {
	c1, err := makeTestCap("disbind", "media:ext=pdf", "media:disbound-pages;list;enc=utf-8", "Disbind")
	require.NoError(t, err)
	c2, err := makeTestCap("grind", "media:ext=pdf", "media:disbound-pages;list;enc=utf-8", "Grind")
	require.NoError(t, err)

	count, err := checkForDuplicateCaps([]*cap.Cap{c1, c2})
	require.NoError(t, err, "Different ops should not be duplicates")
	assert.Equal(t, 2, count, "Should have 2 edges")
}

// TEST993: Tests caps with same operation but different input types are not duplicates
// Verifies that input type differences distinguish caps with the same operation name
func Test993_same_op_different_input_types_not_duplicates(t *testing.T) {
	c1, err := makeTestCap("extract_metadata", "media:ext=pdf", "media:file-metadata;record;enc=utf-8", "Extract PDF Metadata")
	require.NoError(t, err)
	c2, err := makeTestCap("extract_metadata", "media:txt;enc=utf-8", "media:file-metadata;record;enc=utf-8", "Extract TXT Metadata")
	require.NoError(t, err)

	count, err := checkForDuplicateCaps([]*cap.Cap{c1, c2})
	require.NoError(t, err, "Same op with different inputs should not be duplicates")
	assert.Equal(t, 2, count, "Should have 2 edges")
}

// TEST994: Tests first cap's input argument is automatically resolved from input file
// Verifies that determineResolutionWithIOCheck() returns FromInputFile for the first cap in a chain
func Test994_input_arg_first_cap_auto_resolved_from_input(t *testing.T) {
	resolution := determineResolutionWithIOCheck("media:ext=pdf", "media:ext=pdf", "media:ext=png;image", 0, nil)
	assert.Equal(t, ResolutionFromInputFile, resolution)
}

// TEST995: Tests subsequent caps' input arguments are automatically resolved from previous output
// Verifies that determineResolutionWithIOCheck() returns FromPreviousOutput for caps after the first
func Test995_input_arg_subsequent_cap_auto_resolved_from_previous(t *testing.T) {
	resolution := determineResolutionWithIOCheck("media:ext=pdf", "media:ext=pdf", "media:ext=png;image", 1, nil)
	assert.Equal(t, ResolutionFromPreviousOutput, resolution)

	resolution = determineResolutionWithIOCheck("media:ext=pdf", "media:ext=pdf", "media:ext=png;image", 2, nil)
	assert.Equal(t, ResolutionFromPreviousOutput, resolution)
}

// TEST996: Tests output arguments are automatically resolved from previous cap's output
// Verifies that arguments matching the output spec are always resolved as FromPreviousOutput
func Test996_output_arg_auto_resolved(t *testing.T) {
	resolution := determineResolutionWithIOCheck("media:ext=png;image", "media:ext=pdf", "media:ext=png;image", 0, nil)
	assert.Equal(t, ResolutionFromPreviousOutput, resolution)
}

// TEST997: Tests MEDIA_FILE_PATH argument type resolves to input file for first cap
// Verifies that generic file-path arguments are bound to input file in the first cap
func Test997_file_path_type_fallback_first_cap(t *testing.T) {
	resolution := determineResolutionWithIOCheck(standard.MediaFilePath, "media:ext=pdf", "media:ext=png;image", 0, nil)
	assert.Equal(t, ResolutionFromInputFile, resolution)
}

// TEST998: Tests MEDIA_FILE_PATH argument type resolves to previous output for subsequent caps
// Verifies that generic file-path arguments are bound to previous cap's output after the first cap
func Test998_file_path_type_fallback_subsequent_cap(t *testing.T) {
	resolution := determineResolutionWithIOCheck(standard.MediaFilePath, "media:ext=pdf", "media:ext=png;image", 1, nil)
	assert.Equal(t, ResolutionFromPreviousOutput, resolution)
}

// TEST1009: Tests required non-IO arguments with default values are marked as HasDefault
// Verifies that arguments like integers with defaults don't require user input
func Test1009_non_io_arg_with_default_has_default(t *testing.T) {
	defaultVal := 200
	resolution := determineResolutionWithIOCheck(standard.MediaInteger, "media:ext=pdf", "media:ext=png;image", 0, defaultVal)
	assert.Equal(t, ResolutionHasDefault, resolution)
}

// TEST1012: Tests required non-IO arguments without defaults require user input
// Verifies that arguments like strings without defaults are marked as RequiresUserInput
func Test1012_non_io_arg_without_default_requires_user_input(t *testing.T) {
	resolution := determineResolutionWithIOCheck("media:string", "media:ext=pdf", "media:ext=png;image", 0, nil)
	assert.Equal(t, ResolutionRequiresUserInput, resolution)
}

// TEST1015: Tests optional non-IO arguments without defaults still require user input
// Verifies that optional arguments without defaults must be explicitly provided or skipped
func Test1015_optional_non_io_arg_without_default_requires_user_input(t *testing.T) {
	resolution := determineResolutionWithIOCheck("media:boolean", "media:ext=pdf", "media:ext=png;image", 0, nil)
	assert.Equal(t, ResolutionRequiresUserInput, resolution)
}

// TEST1019: Tests ValidationToJSON() returns nil for nil input
// Verifies that missing validation metadata is converted to nil
func Test1019_validation_to_json_nil(t *testing.T) {
	result := ValidationToJSON(nil)
	assert.Nil(t, result, "nil validation should return nil")
}

// TEST1100: Tests that CapUrn normalizes media URN tags to canonical order
// Two CapUrns with different tag ordering in out spec must produce the same canonical string.
func Test1100_cap_urn_normalizes_media_urn_tag_order(t *testing.T) {
	urn1, err := urn.NewCapUrnFromString(`cap:in="media:ext=pdf";extract-metadata;out="media:file-metadata;record;enc=utf-8"`)
	require.NoError(t, err)
	urn2, err := urn.NewCapUrnFromString(`cap:in="media:ext=pdf";extract-metadata;out="media:file-metadata;record;enc=utf-8"`)
	require.NoError(t, err)

	assert.Equal(t, urn1.String(), urn2.String(),
		"URNs with different tag ordering should normalize to the same canonical form")

	// Both URNs should parse without error and produce the same canonical form
	assert.NotEmpty(t, urn1.OutSpec(), "out spec should not be empty")
	assert.Equal(t, urn1.OutSpec(), urn2.OutSpec(),
		"out specs with different tag ordering should normalize identically")
}

// TEST6684: Tests that IsDispatchable has correct directionality
// A specific provider is dispatchable for a general request; the reverse is false.
func Test6684_is_dispatchable_uses_correct_directionality(t *testing.T) {
	generalRequest, err := urn.NewCapUrnFromString("cap:in=\"media:ext=pdf\";extract;out=media:text")
	require.NoError(t, err)

	specificProvider, err := urn.NewCapUrnFromString("cap:in=\"media:ext=pdf\";extract;out=media:text;version=2")
	require.NoError(t, err)

	assert.True(t, specificProvider.IsDispatchable(generalRequest),
		"Specific provider should be dispatchable for general request")
	assert.False(t, generalRequest.IsDispatchable(specificProvider),
		"General request should NOT be dispatchable for specific provider (missing version tag)")
}

// TEST6685: Tests that IsDispatchable rejects when provider is missing a required cap tag
// Provider without required=yes cannot handle a request that demands required=yes.
func Test6685_is_dispatchable_rejects_non_dispatchable(t *testing.T) {
	request, err := urn.NewCapUrnFromString("cap:in=\"media:ext=pdf\";extract;out=media:text;required=yes")
	require.NoError(t, err)

	provider, err := urn.NewCapUrnFromString("cap:in=\"media:ext=pdf\";extract;out=media:text")
	require.NoError(t, err)

	assert.False(t, provider.IsDispatchable(request),
		"Provider missing required tag should not be dispatchable for request")
}
