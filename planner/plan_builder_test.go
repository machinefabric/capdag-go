package planner

import (
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST767: Tests ArgumentResolution String() returns correct snake_case names
// ArgumentInfo.Resolution is serialized to JSON using String(). Verifies that each
// resolution variant maps to the correct identifier expected by API consumers.
func Test767_argument_resolution_string_representations(t *testing.T) {
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

// TEST768: Tests AnalyzePathArguments classifies stdin arg as FromInputFile for first cap
// Verifies that the argument analysis correctly identifies input-file arguments when the
// cap's stdin arg media URN matches the cap's in_spec.
func Test768_analyze_path_arguments_stdin_is_from_input_file(t *testing.T) {
	// Build a cap whose stdin arg is the cap's in_spec (media:pdf) — should resolve as FromInputFile
	capUrnStr := `cap:in="media:pdf";op=extract;out="media:txt;textable"`
	capUrnParsed, err := urn.NewCapUrnFromString(capUrnStr)
	require.NoError(t, err)

	inSpec := capUrnParsed.InSpec()
	stdinArg := cap.NewCapArg(inSpec, true, []cap.ArgSource{
		{Stdin: &inSpec},
	})
	c := cap.NewCapWithArgs(capUrnParsed, "Extract", "test", []cap.CapArg{stdinArg})
	c.Output = &cap.CapOutput{MediaUrn: capUrnParsed.OutSpec()}

	registry := cap.NewCapRegistryForTest()
	registry.AddCapsToCache([]*cap.Cap{c})
	builder := NewMachinePlanBuilder(registry)

	// Build a single-step path
	graph := NewLiveCapGraph()
	graph.AddCap(c)
	source, err := urn.NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString(`media:txt;textable`)
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

// TEST769: Tests AnalyzePathArguments puts RequiresUserInput args in slots and sets CanExecuteWithoutInput=false
// Verifies that caps with non-stdin, non-default arguments are identified as requiring user input,
// appear in slots, and the requirements reflect that execution cannot proceed without them.
func Test769_analyze_path_arguments_user_input_arg_appears_in_slots(t *testing.T) {
	capUrnStr := `cap:in="media:txt;textable";op=translate;out="media:translated;textable"`
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

	registry := cap.NewCapRegistryForTest()
	registry.AddCapsToCache([]*cap.Cap{c})
	builder := NewMachinePlanBuilder(registry)

	graph := NewLiveCapGraph()
	graph.AddCap(c)
	source, err := urn.NewMediaUrnFromString(`media:txt;textable`)
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString(`media:translated;textable`)
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
