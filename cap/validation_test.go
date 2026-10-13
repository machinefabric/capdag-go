package cap

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper matching Rust's test_urn() for validation tests
func valTestUrn(tags string) string {
	if tags == "" {
		return `cap:in="media:void";out="media:void"`
	}
	return `cap:in="media:void";out="media:void";` + tags
}

// Helper to create a Cap with the given args (matching Rust make_test_cap_with_args)
func makeTestCapWithArgs(t *testing.T, args []CapArg) *Cap {
	t.Helper()
	u, err := urn.NewCapUrnFromString(valTestUrn("test-cap;type=test"))
	require.NoError(t, err)
	return NewCapWithArgs(u, "Test Capability", []string{"test-command"}, args)
}

// Helper to create an ArgSource for stdin
func stdinSource(mediaUrn string) ArgSource {
	return ArgSource{Stdin: &mediaUrn}
}

// Helper to create an ArgSource for position
func positionSource(pos int) ArgSource {
	return ArgSource{Position: &pos}
}

// Helper to create an ArgSource for cli_flag
func cliFlagSource(flag string) ArgSource {
	return ArgSource{CliFlag: &flag}
}

// Helper to create a Cap with custom URN and given args
func makeTestCapWithUrnAndArgs(t *testing.T, capUrnStr string, args []CapArg) *Cap {
	t.Helper()
	u, err := urn.NewCapUrnFromString(capUrnStr)
	require.NoError(t, err)
	return NewCapWithArgs(u, "Test Capability", []string{"test-command"}, args)
}

// -------------------------------------------------------------------------
// Input validation tests (051-052)
// -------------------------------------------------------------------------

// TEST51: Test input validation succeeds with valid positional argument
func Test051_input_validation_success(t *testing.T) {
	u, err := urn.NewCapUrnFromString(valTestUrn("cap;type=test"))
	require.NoError(t, err)
	cap := NewCap(u, "Test Capability", []string{"test-command"})
	cap.AddArg(NewCapArg(standard.MediaString, "arg", true, []ArgSource{positionSource(0)}))

	inputArgs := []interface{}{"/path/to/file.txt"}

	validator := NewInputValidator()
	registry := newTestMediaDefSource()
	// The unified registry resolves the cap's referenced media URNs
	// at validation time; seed the spec the cap references.
	registry.AddSpec(media.MediaDef{
		Urn:        standard.MediaString,
		MediaType:  "text/plain",
		Title:      "String",
		ProfileURI: media.ProfileStr,
	}.ToStored())

	err = validator.ValidateArguments(cap, inputArgs, registry)
	assert.NoError(t, err)
}

// TEST52: Test input validation fails with MissingRequiredArgument when required arg missing
func Test052_input_validation_missing_required(t *testing.T) {
	u, err := urn.NewCapUrnFromString(valTestUrn("cap;type=test"))
	require.NoError(t, err)
	cap := NewCap(u, "Test Capability", []string{"test-command"})
	cap.AddArg(NewCapArg(standard.MediaString, "arg", true, []ArgSource{positionSource(0)}))

	inputArgs := []interface{}{} // Missing required argument

	validator := NewInputValidator()
	registry := newTestMediaDefSource()

	err = validator.ValidateArguments(cap, inputArgs, registry)
	require.Error(t, err)
	assert.Contains(t, err.Error(), standard.MediaString)
}

// TEST53: Test input validation fails with InvalidArgumentType when wrong type provided
func Test053_input_validation_wrong_type(t *testing.T) {
	u, err := urn.NewCapUrnFromString(valTestUrn("cap;type=test"))
	require.NoError(t, err)
	cap := NewCap(u, "Test Capability", []string{"test-command"})

	registry := newTestMediaDefSource()
	registry.AddSpec((media.NewMediaDefWithSchema(
		standard.MediaInteger,
		"text/plain",
		"https://capdag.com/schema/integer",
		map[string]interface{}{"type": "integer"},
	)).ToStored())
	cap.AddArg(NewCapArg(standard.MediaInteger, "arg", true, []ArgSource{positionSource(0)}))

	inputArgs := []interface{}{"not_a_number"}

	validator := NewInputValidator()

	err = validator.ValidateArguments(cap, inputArgs, registry)
	require.Error(t, err)

	validationErr, ok := err.(*ValidationError)
	require.True(t, ok, "expected ValidationError")
	assert.Equal(t, "InvalidArgumentType", validationErr.Type)
	assert.Equal(t, standard.MediaInteger, validationErr.ArgumentName)
}

// -------------------------------------------------------------------------
// Structural validation tests (578-590)
// -------------------------------------------------------------------------

// TEST578: RULE1 - duplicate media_urns rejected
func Test578_rule1_duplicate_media_urns(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, "arg", true, []ArgSource{positionSource(0)}),
		NewCapArg(standard.MediaString, "arg2", true, []ArgSource{positionSource(1)}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE1")
}

// TEST579: RULE2 - empty sources rejected
func Test579_rule2_empty_sources(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, "arg", true, []ArgSource{}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE2")
}

// TEST580: RULE3 - multiple stdin sources with different URNs rejected
func Test580_rule3_different_stdin_urns(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:enc=utf-8;ext=txt";test-cap;out="media:void";type=test`,
		[]CapArg{
			NewCapArg(standard.MediaString, "arg", true, []ArgSource{stdinSource("media:enc=utf-8;ext=txt")}),
			NewCapArg(standard.MediaInteger, "arg2", true, []ArgSource{stdinSource("media:")}),
		},
	)
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE3")
}

// TEST581: RULE3 - two stdin args are rejected even when their stdin URNs are identical
//
// The rule used to allow this ("multiple stdin sources with the same URN is OK"), on the
// reasoning that one stdin stream can only be one type. A cap has one main input: two args
// that both claim it leave nothing to say which of them a piped value is.
func Test581_rule3_identical_stdin_urns_are_still_two_main_inputs(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:enc=utf-8;ext=txt";test-cap;out="media:void";type=test`,
		[]CapArg{
			NewCapArg(standard.MediaString, "input", true, []ArgSource{stdinSource("media:enc=utf-8;ext=txt")}),
			NewCapArg(standard.MediaInteger, "other", true, []ArgSource{stdinSource("media:enc=utf-8;ext=txt")}),
		},
	)
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE3")
	assert.Contains(t, err.Error(), "both declare a stdin source")
}

// TEST582: RULE4 - duplicate source type in single arg rejected
func Test582_rule4_duplicate_source_type(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, "arg", true, []ArgSource{
			positionSource(0),
			positionSource(1), // same source type twice
		}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE4")
}

// TEST583: RULE5 - duplicate position across args rejected
func Test583_rule5_duplicate_position(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, "arg", true, []ArgSource{positionSource(0)}),
		NewCapArg(standard.MediaInteger, "arg2", true, []ArgSource{positionSource(0)}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE5")
}

// TEST584: RULE6 - position gap rejected (0, 2 without 1)
func Test584_rule6_position_gap(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, "arg", true, []ArgSource{positionSource(0)}),
		NewCapArg(standard.MediaInteger, "arg2", true, []ArgSource{positionSource(2)}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE6")
}

// TEST585: RULE6 - sequential positions (0, 1, 2) pass
func Test585_rule6_sequential_ok(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, "arg", true, []ArgSource{positionSource(0)}),
		NewCapArg(standard.MediaInteger, "arg2", true, []ArgSource{positionSource(1)}),
	})
	err := ValidateCapArgs(cap)
	assert.NoError(t, err, "Sequential positions should pass: %v", err)
}

// TEST586: RULE7 - arg with both position and cli_flag rejected
func Test586_rule7_position_and_cli_flag(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, "arg", true, []ArgSource{
			positionSource(0),
			cliFlagSource("--file"),
		}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE7")
}

// TEST587: RULE9 - duplicate cli_flag across args rejected
func Test587_rule9_duplicate_cli_flag(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, "arg", true, []ArgSource{cliFlagSource("--file")}),
		NewCapArg(standard.MediaInteger, "arg2", true, []ArgSource{cliFlagSource("--file")}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE9")
}

// TEST588: RULE10 - reserved cli_flags rejected
func Test588_rule10_reserved_cli_flags(t *testing.T) {
	for _, reserved := range ReservedCliFlags {
		cap := makeTestCapWithArgs(t, []CapArg{
			NewCapArg(standard.MediaString, "arg", true, []ArgSource{cliFlagSource(reserved)}),
		})
		err := ValidateCapArgs(cap)
		require.Error(t, err, "Reserved flag '%s' should be rejected", reserved)
		assert.True(t, strings.Contains(err.Error(), "RULE10"),
			"Error for '%s' should mention RULE10: %v", reserved, err)
	}
}

// TEST589: valid cap args with mixed sources pass all rules
func Test589_all_rules_pass(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:enc=utf-8;ext=txt";test-cap;out="media:void";type=test`,
		[]CapArg{
			NewCapArg(standard.MediaString, "arg", true, []ArgSource{
				positionSource(0),
				stdinSource("media:enc=utf-8;ext=txt"),
			}),
			NewCapArg(standard.MediaInteger, "arg2", false, []ArgSource{
				positionSource(1),
			}),
		},
	)
	err := ValidateCapArgs(cap)
	assert.NoError(t, err, "Valid cap args should pass: %v", err)
}

// TEST590: validate_cap_args accepts cap with only cli_flag sources (no positions)
func Test590_cli_flag_only_args(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, "arg", true, []ArgSource{cliFlagSource("--input")}),
		NewCapArg(standard.MediaInteger, "arg2", false, []ArgSource{cliFlagSource("--count")}),
	})
	err := ValidateCapArgs(cap)
	assert.NoError(t, err, "CLI-flag-only args should pass: %v", err)
}

// -------------------------------------------------------------------------
// RULE15 tests (1294-1297, 12705): the main input matches in=
// -------------------------------------------------------------------------

// TEST1294: RULE15 - void-input cap with stdin source rejected
func Test1294_rule15_void_input_with_stdin_rejected(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:void";test;out="media:fmt=json;record"`,
		[]CapArg{
			NewCapArg(standard.MediaString, "input", true, []ArgSource{stdinSource("media:enc=utf-8;ext=txt")}),
		},
	)
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE15")
}

// TEST1295: RULE15 - non-void-input cap without stdin source rejected
func Test1295_rule15_non_void_input_without_stdin_rejected(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt"`,
		[]CapArg{
			NewCapArg(standard.MediaString, "input", true, []ArgSource{positionSource(0)}),
		},
	)
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE15")
}

// TEST1296: RULE15 - void-input cap with only cli_flag sources passes
func Test1296_rule15_void_input_cli_only_ok(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:void";test;out="media:fmt=json;record"`,
		[]CapArg{
			NewCapArg(standard.MediaString, "arg", true, []ArgSource{cliFlagSource("--query")}),
		},
	)
	err := ValidateCapArgs(cap)
	assert.NoError(t, err, "Void-input cap with only cli_flag sources should pass: %v", err)
}

// TEST1953: RULE14 — `streaming: true` is accepted on the main input (the
// stdin arg equivalent to `in=`) and refused on any other argument: a side
// option has no wire stream, so it has nothing to consume incrementally, and
// the rule keeps the executor's hop rule one-dimensional.
func Test1953_rule14_streaming_only_on_main_input(t *testing.T) {
	main := NewCapArg("media:document;ext=pdf", "arg", true, []ArgSource{stdinSource("media:document;ext=pdf")})
	main.Streaming = true
	ok := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt"`,
		[]CapArg{main},
	)
	require.NoError(t, ValidateCapArgs(ok), "streaming main input must pass")
	inStreams, outStreams := ok.StreamingShape()
	assert.True(t, inStreams)
	assert.False(t, outStreams)

	main.Streaming = false
	side := NewCapArg(standard.MediaString, "arg2", false, []ArgSource{cliFlagSource("--lang")})
	side.Streaming = true
	bad := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt"`,
		[]CapArg{main, side},
	)
	err := ValidateCapArgs(bad)
	require.Error(t, err, "streaming side option must be refused")
	assert.Contains(t, err.Error(), "RULE14")
	assert.Contains(t, err.Error(), standard.MediaString)
}

// TEST1297: RULE15 - non-void-input cap with stdin source passes
func Test1297_rule15_non_void_input_with_stdin_ok(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt"`,
		[]CapArg{
			NewCapArg(standard.MediaString, "arg", true, []ArgSource{stdinSource("media:document;ext=pdf")}),
		},
	)
	err := ValidateCapArgs(cap)
	assert.NoError(t, err, "Non-void-input cap with stdin source should pass: %v", err)
}

// TEST1964: a definition field this capdag does not know is a NEWER fabric,
// not noise — parsing refuses it, naming the key, for arguments and outputs
// alike. The field being dropped is how a cartridge built on an older capdag
// once advertised a `streaming` input as bounded.
func Test1964_unknown_definition_field_is_refused(t *testing.T) {
	var arg CapArg
	err := json.Unmarshal([]byte(`{"media_urn":"media:enc=utf-8","name":"input","required":true,"sources":[{"stdin":"media:enc=utf-8"}],"chunking":"adaptive"}`), &arg)
	require.Error(t, err, "unknown arg field must be refused")
	assert.Contains(t, err.Error(), "chunking")
	require.NoError(t, json.Unmarshal([]byte(`{"media_urn":"media:enc=utf-8","name":"input","required":true,"sources":[{"stdin":"media:enc=utf-8"}],"streaming":true}`), &arg))
	assert.True(t, arg.Streaming)

	var out CapOutput
	err = json.Unmarshal([]byte(`{"media_urn":"media:enc=utf-8","name":"output","output_description":"x","windowed":true}`), &out)
	require.Error(t, err, "unknown output field must be refused")
	assert.Contains(t, err.Error(), "windowed")
}

// TEST12705: RULE15 - the stdin argument's URN is the cap's `in=`, as a
// tagged URN: the same tags in another order pass, another type does not.
func Test12705_rule15_the_stdin_argument_is_the_caps_input(t *testing.T) {
	capOf := func(stdin string) *Cap {
		return makeTestCapWithUrnAndArgs(t,
			`cap:in="media:document;ext=pdf";test;out="media:record"`,
			[]CapArg{NewCapArg("media:enc=utf-8;file-path", "document", true, []ArgSource{stdinSource(stdin)})},
		)
	}
	assert.NoError(t, ValidateCapArgs(capOf("media:document;ext=pdf")))
	assert.NoError(t, ValidateCapArgs(capOf("media:ext=pdf;document")), "tag order is not identity")
	err := ValidateCapArgs(capOf("media:document"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE15")
	assert.Contains(t, err.Error(), "media:document;ext=pdf")
}

// TEST12706: RULE17 - every argument has a well-formed name, unique in its cap.
func Test12706_rule17_argument_names(t *testing.T) {
	flag := func(mediaUrn, name, cliFlag string) CapArg {
		return NewCapArg(mediaUrn, name, false, []ArgSource{cliFlagSource(cliFlag)})
	}
	// Two args, two names: fine. A name says nothing of the URN it labels.
	ok := makeTestCapWithArgs(t, []CapArg{
		flag(standard.MediaString, "window", "--a"),
		flag(standard.MediaInteger, "benchmark", "--b"),
	})
	assert.NoError(t, ValidateCapArgs(ok))

	refused := func(args []CapArg) string {
		err := ValidateCapArgs(makeTestCapWithArgs(t, args))
		require.Error(t, err)
		return err.Error()
	}
	message := refused([]CapArg{flag(standard.MediaString, "", "--a")})
	assert.Contains(t, message, "RULE17")
	assert.Contains(t, message, "has no name")
	for _, bad := range []string{"9lives", "has space", "dotted.name", "a=b", "true", "false", "null", "-lead"} {
		message := refused([]CapArg{flag(standard.MediaString, bad, "--a")})
		assert.Contains(t, message, "RULE17", bad)
		assert.Contains(t, message, "not a valid name", bad)
	}
	for _, good := range []string{"x", "_x", "model-spec", "top_k", "A1"} {
		assert.NoError(t, ValidateCapArgs(makeTestCapWithArgs(t, []CapArg{flag(standard.MediaString, good, "--a")})), good)
	}
	message = refused([]CapArg{flag(standard.MediaString, "same", "--a"), flag(standard.MediaInteger, "same", "--b")})
	assert.Contains(t, message, "RULE17")
	assert.Contains(t, message, "share the name 'same'")
	assert.Contains(t, message, standard.MediaString, "both args are named by URN")
	assert.Contains(t, message, standard.MediaInteger, "both args are named by URN")
}

// TEST12707: RULE18 - an argument says what it accepts in one place; and what
// it accepts is not what it is delivered under.
func Test12707_rule18_accepts_and_the_two_urns_of_an_argument(t *testing.T) {
	series := "media:fmt=json;list;record;time-series"
	benchmark := NewCapArg(
		"media:benchmark;fmt=json;list;record;time-series", "benchmark",
		true,
		[]ArgSource{cliFlagSource("--benchmark")},
	).Accepting(series)
	// It accepts a plain series, and is delivered under its own URN: two
	// args accepting one type stay distinct on the wire.
	assert.Equal(t, series, benchmark.AcceptedUrn())
	assert.Equal(t, "media:benchmark;fmt=json;list;record;time-series", benchmark.StreamUrn())

	// The main input accepts, and is delivered under, its stdin URN.
	main := NewCapArg("media:enc=utf-8;file-path", "path", true, []ArgSource{stdinSource(series)})
	assert.Equal(t, series, main.AcceptedUrn())
	assert.Equal(t, series, main.StreamUrn())

	// An arg with neither accepts its own URN.
	plain := NewCapArg(standard.MediaInteger, "window", false, []ArgSource{cliFlagSource("--window")})
	assert.Equal(t, standard.MediaInteger, plain.AcceptedUrn())
	assert.Equal(t, standard.MediaInteger, plain.StreamUrn())

	capUrn := `cap:in="` + series + `";compare;out="media:record"`
	assert.NoError(t, ValidateCapArgs(makeTestCapWithUrnAndArgs(t, capUrn, []CapArg{main, benchmark})))

	// `accepts` beside a stdin source says it twice.
	err := ValidateCapArgs(makeTestCapWithUrnAndArgs(t, capUrn, []CapArg{main.Accepting(series)}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE18")
	assert.Contains(t, err.Error(), "media:enc=utf-8;file-path")
	// Declaring what one copy accepts does not change the argument it was made from.
	assert.Nil(t, main.Accepts)
}

func capWithOutputs(t *testing.T, outSpec string, outputs []CapOutput) *Cap {
	t.Helper()
	u, err := urn.NewCapUrnFromString(`cap:in="media:enc=utf-8";test;out="` + outSpec + `"`)
	require.NoError(t, err)
	c := NewCap(u, "Test", []string{"cmd"})
	c.SetOutputs(outputs)
	return c
}

// TEST12708: RULE19, RULE20 - outputs have well-formed, distinct names and
// distinct media URNs, compared as URNs.
func Test12708_rule19_rule20_output_names_and_identities(t *testing.T) {
	const regression = "media:fmt=json;record;regression"
	const residual = "media:fmt=json;list;record;residual"
	fit := func() CapOutput { return *NewCapOutput(regression, "fit", "the fit") }
	assert.NoError(t, ValidateCapOutputs(capWithOutputs(t, regression, []CapOutput{fit(), *NewCapOutput(residual, "residuals", "")})))

	refused := func(outputs []CapOutput) string {
		err := ValidateCapOutputs(capWithOutputs(t, regression, outputs))
		require.Error(t, err)
		return err.Error()
	}
	message := refused([]CapOutput{fit(), *NewCapOutput(residual, "", "")})
	assert.Contains(t, message, "RULE19")
	assert.Contains(t, message, "has no name")
	message = refused([]CapOutput{fit(), *NewCapOutput(residual, "null", "")})
	assert.Contains(t, message, "RULE19")
	assert.Contains(t, message, "not a valid name")
	message = refused([]CapOutput{fit(), *NewCapOutput(residual, "fit", "")})
	assert.Contains(t, message, "RULE19")
	assert.Contains(t, message, "share the name 'fit'")
	// The same URN with its tags in another order is the same output.
	message = refused([]CapOutput{fit(), *NewCapOutput("media:regression;record;fmt=json", "again", "")})
	assert.Contains(t, message, "RULE20")
}

// TEST12709: RULE21 - exactly one output is the main one, the cap's `out=`;
// a cap whose `out=` is void declares none.
func Test12709_rule21_exactly_one_main_output(t *testing.T) {
	const fit = "media:fmt=json;record;regression"
	none := capWithOutputs(t, fit, []CapOutput{*NewCapOutput("media:fmt=json;list;record;residual", "residuals", "")})
	err := ValidateCapOutputs(none)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE21")
	assert.Contains(t, err.Error(), "0 outputs")
	err = ValidateCapOutputs(capWithOutputs(t, fit, nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE21")

	one := capWithOutputs(t, fit, []CapOutput{*NewCapOutput("media:regression;fmt=json;record", "fit", "")})
	assert.NoError(t, ValidateCapOutputs(one), "the main output is found by URN equivalence")
	require.NotNil(t, one.MainOutput())
	assert.Equal(t, "fit", one.MainOutput().Name)

	void := capWithOutputs(t, "media:void", nil)
	assert.NoError(t, ValidateCapOutputs(void))
	assert.Nil(t, void.MainOutput())
	err = ValidateCapOutputs(capWithOutputs(t, "media:void", []CapOutput{*NewCapOutput("media:enc=utf-8", "text", "")}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE21")
	assert.Contains(t, err.Error(), "media:void")
}

// TEST12710: RULE22 - only a non-main output of a cap that takes an input
// may carry that input's type.
func Test12710_rule22_what_an_output_carries(t *testing.T) {
	router := capWithOutputs(t, "media:enc=utf-8;kept", []CapOutput{
		*NewCapOutput("media:enc=utf-8;kept", "yes", "").AsOptional(true),
		*NewCapOutput("media:enc=utf-8;dropped", "no", "").AsOptional(true).CarryingInput(),
	})
	assert.NoError(t, ValidateCapOutputs(router))

	mainCarries := capWithOutputs(t, "media:enc=utf-8;kept", []CapOutput{
		*NewCapOutput("media:enc=utf-8;kept", "yes", "").CarryingInput(),
	})
	err := ValidateCapOutputs(mainCarries)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE22")
	assert.Contains(t, err.Error(), "main output")

	u, uerr := urn.NewCapUrnFromString(`cap:in="media:void";now;out="media:enc=utf-8;stamp"`)
	require.NoError(t, uerr)
	source := NewCap(u, "Now", []string{"now"})
	source.SetOutputs([]CapOutput{
		*NewCapOutput("media:enc=utf-8;stamp", "stamp", ""),
		*NewCapOutput("media:enc=utf-8;echo", "echo", "").CarryingInput(),
	})
	err = ValidateCapOutputs(source)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE22")
	assert.Contains(t, err.Error(), "takes none")
}
