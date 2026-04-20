package cap

import (
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
	u, err := urn.NewCapUrnFromString(valTestUrn("op=test-cap;type=test"))
	require.NoError(t, err)
	return NewCapWithArgs(u, "Test Capability", "test-command", args)
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
	return NewCapWithArgs(u, "Test Capability", "test-command", args)
}

// -------------------------------------------------------------------------
// Input validation tests (051-052)
// -------------------------------------------------------------------------

// TEST051: Test input validation succeeds with valid positional argument
func Test051_input_validation_success(t *testing.T) {
	u, err := urn.NewCapUrnFromString(valTestUrn("op=cap;type=test"))
	require.NoError(t, err)
	cap := NewCap(u, "Test Capability", "test-command")
	cap.AddArg(NewCapArg(standard.MediaString, true, []ArgSource{positionSource(0)}))

	inputArgs := []interface{}{"/path/to/file.txt"}

	validator := NewInputValidator()
	registry, err := media.NewMediaUrnRegistry()
	require.NoError(t, err)

	err = validator.ValidateArguments(cap, inputArgs, registry)
	assert.NoError(t, err)
}

// TEST052: Test input validation fails with MissingRequiredArgument when required arg missing
func Test052_input_validation_missing_required(t *testing.T) {
	u, err := urn.NewCapUrnFromString(valTestUrn("op=cap;type=test"))
	require.NoError(t, err)
	cap := NewCap(u, "Test Capability", "test-command")
	cap.AddArg(NewCapArg(standard.MediaString, true, []ArgSource{positionSource(0)}))

	inputArgs := []interface{}{} // Missing required argument

	validator := NewInputValidator()
	registry, err := media.NewMediaUrnRegistry()
	require.NoError(t, err)

	err = validator.ValidateArguments(cap, inputArgs, registry)
	require.Error(t, err)
	assert.Contains(t, err.Error(), standard.MediaString)
}

// TEST053: Test input validation fails with InvalidArgumentType when wrong type provided
func Test053_input_validation_wrong_type(t *testing.T) {
	u, err := urn.NewCapUrnFromString(valTestUrn("op=cap;type=test"))
	require.NoError(t, err)
	cap := NewCap(u, "Test Capability", "test-command")

	cap.AddMediaSpec(media.NewMediaSpecDefWithSchema(
		standard.MediaInteger,
		"text/plain",
		"https://capdag.com/schema/integer",
		map[string]interface{}{"type": "integer"},
	))
	cap.AddArg(NewCapArg(standard.MediaInteger, true, []ArgSource{positionSource(0)}))

	inputArgs := []interface{}{"not_a_number"}

	validator := NewInputValidator()
	registry, err := media.NewMediaUrnRegistry()
	require.NoError(t, err)

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
		NewCapArg(standard.MediaString, true, []ArgSource{positionSource(0)}),
		NewCapArg(standard.MediaString, true, []ArgSource{positionSource(1)}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE1")
}

// TEST579: RULE2 - empty sources rejected
func Test579_rule2_empty_sources(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, true, []ArgSource{}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE2")
}

// TEST580: RULE3 - multiple stdin sources with different URNs rejected
func Test580_rule3_different_stdin_urns(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:txt;textable";op=test-cap;out="media:void";type=test`,
		[]CapArg{
			NewCapArg(standard.MediaString, true, []ArgSource{stdinSource("media:txt;textable")}),
			NewCapArg(standard.MediaInteger, true, []ArgSource{stdinSource("media:")}),
		},
	)
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE3")
}

// TEST581: RULE3 - multiple stdin sources with same URN is OK
func Test581_rule3_same_stdin_urns_ok(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:txt;textable";op=test-cap;out="media:void";type=test`,
		[]CapArg{
			NewCapArg(standard.MediaString, true, []ArgSource{stdinSource("media:txt;textable")}),
			NewCapArg(standard.MediaInteger, true, []ArgSource{stdinSource("media:txt;textable")}),
		},
	)
	err := ValidateCapArgs(cap)
	assert.NoError(t, err, "Same stdin URNs should be allowed: %v", err)
}

// TEST582: RULE4 - duplicate source type in single arg rejected
func Test582_rule4_duplicate_source_type(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, true, []ArgSource{
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
		NewCapArg(standard.MediaString, true, []ArgSource{positionSource(0)}),
		NewCapArg(standard.MediaInteger, true, []ArgSource{positionSource(0)}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE5")
}

// TEST584: RULE6 - position gap rejected (0, 2 without 1)
func Test584_rule6_position_gap(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, true, []ArgSource{positionSource(0)}),
		NewCapArg(standard.MediaInteger, true, []ArgSource{positionSource(2)}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE6")
}

// TEST585: RULE6 - sequential positions (0, 1, 2) pass
func Test585_rule6_sequential_ok(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, true, []ArgSource{positionSource(0)}),
		NewCapArg(standard.MediaInteger, true, []ArgSource{positionSource(1)}),
	})
	err := ValidateCapArgs(cap)
	assert.NoError(t, err, "Sequential positions should pass: %v", err)
}

// TEST586: RULE7 - arg with both position and cli_flag rejected
func Test586_rule7_position_and_cli_flag(t *testing.T) {
	cap := makeTestCapWithArgs(t, []CapArg{
		NewCapArg(standard.MediaString, true, []ArgSource{
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
		NewCapArg(standard.MediaString, true, []ArgSource{cliFlagSource("--file")}),
		NewCapArg(standard.MediaInteger, true, []ArgSource{cliFlagSource("--file")}),
	})
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE9")
}

// TEST588: RULE10 - reserved cli_flags rejected
func Test588_rule10_reserved_cli_flags(t *testing.T) {
	for _, reserved := range ReservedCliFlags {
		cap := makeTestCapWithArgs(t, []CapArg{
			NewCapArg(standard.MediaString, true, []ArgSource{cliFlagSource(reserved)}),
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
		`cap:in="media:txt;textable";op=test-cap;out="media:void";type=test`,
		[]CapArg{
			NewCapArg(standard.MediaString, true, []ArgSource{
				positionSource(0),
				stdinSource("media:txt;textable"),
			}),
			NewCapArg(standard.MediaInteger, false, []ArgSource{
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
		NewCapArg(standard.MediaString, true, []ArgSource{cliFlagSource("--input")}),
		NewCapArg(standard.MediaInteger, false, []ArgSource{cliFlagSource("--count")}),
	})
	err := ValidateCapArgs(cap)
	assert.NoError(t, err, "CLI-flag-only args should pass: %v", err)
}

// -------------------------------------------------------------------------
// RULE11 tests (1294-1297): Stdin source consistency with in= spec
// -------------------------------------------------------------------------

// TEST1294: RULE11 - void-input cap with stdin source rejected
func Test1294_rule11_void_input_with_stdin_rejected(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:void";op=test;out="media:json;record;textable"`,
		[]CapArg{
			NewCapArg(standard.MediaString, true, []ArgSource{stdinSource("media:txt;textable")}),
		},
	)
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE11")
}

// TEST1295: RULE11 - non-void-input cap without stdin source rejected
func Test1295_rule11_non_void_input_without_stdin_rejected(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:pdf";op=extract;out="media:txt;textable"`,
		[]CapArg{
			NewCapArg(standard.MediaString, true, []ArgSource{positionSource(0)}),
		},
	)
	err := ValidateCapArgs(cap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RULE11")
}

// TEST1296: RULE11 - void-input cap with only cli_flag sources passes
func Test1296_rule11_void_input_cli_flag_only_passes(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:void";op=test;out="media:json;record;textable"`,
		[]CapArg{
			NewCapArg(standard.MediaString, true, []ArgSource{cliFlagSource("--query")}),
		},
	)
	err := ValidateCapArgs(cap)
	assert.NoError(t, err, "Void-input cap with only cli_flag sources should pass: %v", err)
}

// TEST1297: RULE11 - non-void-input cap with stdin source passes
func Test1297_rule11_non_void_input_with_stdin_passes(t *testing.T) {
	cap := makeTestCapWithUrnAndArgs(t,
		`cap:in="media:pdf";op=extract;out="media:txt;textable"`,
		[]CapArg{
			NewCapArg(standard.MediaString, true, []ArgSource{stdinSource("media:pdf")}),
		},
	)
	err := ValidateCapArgs(cap)
	assert.NoError(t, err, "Non-void-input cap with stdin source should pass: %v", err)
}
