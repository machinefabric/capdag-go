package machine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func syntaxWirings(t *testing.T, input string) []syntaxWiring {
	t.Helper()
	statements, err := parseSyntax(input)
	require.Nil(t, err, "%q must parse: %v", input, err)
	var wirings []syntaxWiring
	for _, statement := range statements {
		if statement.kind == syntaxWiringStatement {
			wirings = append(wirings, statement.wiring)
		}
	}
	return wirings
}

func oneWiring(t *testing.T, input string) syntaxWiring {
	t.Helper()
	all := syntaxWirings(t, input)
	require.Len(t, all, 1, "%q", input)
	return all[0]
}

// part is one member or target as the tests read it: the name it bears ("" for
// none) and its node, or `=` and its constant's text.
type part struct{ name, value string }

func named(name, value string) part { return part{name, value} }
func bare(value string) part        { return part{"", value} }

func membersOf(w syntaxWiring) []part {
	parts := make([]part, len(w.sources))
	for i, member := range w.sources {
		value := member.node
		if member.isConstant {
			value = "=" + constantText(member.constant)
		}
		parts[i] = part{member.name, value}
		if !member.hasName {
			parts[i].name = ""
		}
	}
	return parts
}

func targetsOf(w syntaxWiring) []part {
	parts := make([]part, len(w.targets))
	for i, target := range w.targets {
		parts[i] = part{target.name, target.node}
	}
	return parts
}

// TEST12732: every form of a wiring's sources and targets reads as the spec
// writes it: a bare node, a group of nodes, named members, constants of every
// JSON kind, an empty group on either side, and several targets with at most
// one unnamed. Both statement forms read alike.
func Test12732_the_forms_of_a_wiring(t *testing.T) {
	w := oneWiring(t, "doc -> extract -> text")
	assert.Equal(t, []part{bare("doc")}, membersOf(w))
	assert.Equal(t, "extract", w.step)
	assert.Equal(t, []part{bare("text")}, targetsOf(w))
	assert.True(t, w.policy.IsDefault())

	w = oneWiring(t, "[(thumbnail, model_spec) -> describe -> description]")
	assert.Equal(t, []part{bare("thumbnail"), bare("model_spec")}, membersOf(w))

	w = oneWiring(t, "(path, benchmark=bench) -> compare -> result")
	assert.Equal(t, []part{bare("path"), named("benchmark", "bench")}, membersOf(w))

	w = oneWiring(t, `(prices, window=20, statistic="mean") -> rolling -> smooth`)
	assert.Equal(t, []part{bare("prices"), named("window", "=20"), named("statistic", `="mean"`)}, membersOf(w))

	// A source fed only by constants, and one fed nothing at all.
	w = oneWiring(t, `(reference="fred/UNRATE") -> fetch -> series`)
	assert.Equal(t, []part{named("reference", `="fred/UNRATE"`)}, membersOf(w))
	w = oneWiring(t, "() -> now -> stamp")
	assert.Empty(t, w.sources)
	assert.Equal(t, []part{bare("stamp")}, targetsOf(w))

	// A sink, and several outputs.
	w = oneWiring(t, "report -> publish -> ()")
	assert.Empty(t, w.targets)
	w = oneWiring(t, "obs -> regress -> (fit, residuals=res)")
	assert.Equal(t, []part{bare("fit"), named("residuals", "res")}, targetsOf(w))
	w = oneWiring(t, "item -> route -> (yes=kept, no=dropped)")
	assert.Equal(t, []part{named("yes", "kept"), named("no", "dropped")}, targetsOf(w))

	// A group of one is a group.
	assert.Equal(t, []part{bare("x")}, membersOf(oneWiring(t, "(x) -> f -> y")))
	assert.Equal(t, []part{bare("y")}, targetsOf(oneWiring(t, "x -> f -> (y)")))

	// Every JSON kind is a constant, nested ones too, and a string may hold
	// what would end a statement.
	w = oneWiring(t, `[(a=true, b=false, c=null, d=-1.5e3, e=[1, "two", {"k": [3]}], f={"x": {"y": null}}, g="a ] \" \n b") -> f -> y]`)
	want := []any{
		true,
		false,
		nil,
		float64(-1500),
		[]any{int64(1), "two", map[string]any{"k": []any{int64(3)}}},
		map[string]any{"x": map[string]any{"y": nil}},
		"a ] \" \n b",
	}
	require.Len(t, w.sources, len(want))
	for i, member := range w.sources {
		require.True(t, member.isConstant, "member %d is a constant", i)
		assert.Equal(t, want[i], member.constant, "member %d", i)
	}

	// A word that only starts like a constant is a node.
	assert.Equal(t, []part{named("a", "truely"), named("b", "nullable")},
		membersOf(oneWiring(t, "(a=truely, b=nullable) -> f -> y")))

	// Statements need no separator beyond their own shape.
	all := syntaxWirings(t, "a -> f -> b\n(b, k=1) -> g -> (c, d=e)\n[e -> h -> ()] [() -> i -> z]")
	require.Len(t, all, 4)
	assert.Equal(t, "g", all[1].step)
	assert.Equal(t, "i", all[3].step)
}

// TEST12734: the two headers. A cap header reads its URN to the end of the
// statement; a machine header names another machine by an opaque reference,
// written as a string with JSON's escapes; and the word `machine` is only a
// keyword where a header puts it.
func Test12734_cap_headers_and_machine_headers(t *testing.T) {
	statements, err := parseSyntax(
		"extract cap:in=\"media:document;ext=pdf\";extract;out=\"media:enc=utf-8\"\n" +
			"clean machine \"lib/clean \\\"v2\\\".machine\"\n" +
			"[machine cap:in=media:;machine;out=media:]\n" +
			"machine -> machine -> machinery",
	)
	require.Nil(t, err)
	require.Len(t, statements, 4)

	require.Equal(t, syntaxCapHeader, statements[0].kind)
	assert.Equal(t, "extract", statements[0].alias)
	assert.Equal(t, `cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8"`, statements[0].capUrn)

	require.Equal(t, syntaxMachineHeader, statements[1].kind)
	assert.Equal(t, "clean", statements[1].alias)
	assert.Equal(t, `lib/clean "v2".machine`, statements[1].reference, "the reference is unescaped")

	// An alias may be the word `machine`, and so may a node and a step.
	require.Equal(t, syntaxCapHeader, statements[2].kind)
	assert.Equal(t, "machine", statements[2].alias)
	require.Equal(t, syntaxWiringStatement, statements[3].kind)
	assert.Equal(t, []part{bare("machine")}, membersOf(statements[3].wiring))
	assert.Equal(t, "machine", statements[3].wiring.step)
	assert.Equal(t, []part{bare("machinery")}, targetsOf(statements[3].wiring))
	assert.Equal(t, []int{0, 1, 2, 3},
		[]int{statements[0].position, statements[1].position, statements[2].position, statements[3].position})
}

// TEST12735: a step's policy. It is read from the braces after the step,
// either key or both, in any order; and what it writes back is one form.
func Test12735_a_step_policy(t *testing.T) {
	policy := func(input string) StepPolicy { return oneWiring(t, input).policy }
	assert.True(t, policy("a -> f -> b").IsDefault())
	assert.True(t, policy("a -> f{} -> b").IsDefault())
	assert.Equal(t, StepPolicy{Retry: 3}, policy("a -> f{retry=3} -> b"))
	assert.Equal(t, StepPolicy{OnError: OnErrorAbsent}, policy("a -> f{on_error=absent} -> b"))
	both := StepPolicy{Retry: 2, OnError: OnErrorAbsent}
	assert.Equal(t, both, policy("a -> f{retry=2, on_error=absent} -> b"))
	assert.Equal(t, both, policy("a -> f { on_error = absent , retry = 2 } -> b"))
	assert.Equal(t, "f", oneWiring(t, "a -> f{retry=2} -> b").step, "the policy is not part of the step's name")

	assert.Equal(t, "", StepPolicy{}.ToNotation())
	assert.Equal(t, "{retry=3}", StepPolicy{Retry: 3}.ToNotation())
	assert.Equal(t, "{on_error=absent}", StepPolicy{OnError: OnErrorAbsent}.ToNotation())
	assert.Equal(t, "{retry=2, on_error=absent}", both.ToNotation())
	// A policy that says `fail` in so many words is the default, and writes
	// nothing.
	assert.True(t, StepPolicy{OnError: OnErrorFail}.IsDefault())
	assert.True(t, StepPolicy{OnError: OnErrorFail}.Equal(StepPolicy{}))
	assert.False(t, both.Equal(StepPolicy{Retry: 2}))
	assert.False(t, both.Equal(StepPolicy{Retry: 3, OnError: OnErrorAbsent}))
	// What is written back reads as what it was.
	for _, p := range []StepPolicy{{}, both, {Retry: 7}} {
		assert.True(t, p.Equal(policy(fmt.Sprintf("a -> f%s -> b", p.ToNotation()))), p.ToNotation())
	}
}

func refusedSyntax(t *testing.T, input string) *MachineSyntaxError {
	t.Helper()
	_, err := parseSyntax(input)
	require.NotNil(t, err, "%q must be refused", input)
	return err
}

// TEST12736: what the text alone decides is refused by name, with the
// statement it is in: a constant that names no arg, a node named as a
// constant is, a second unnamed target, and a policy outside its table.
func Test12736_what_the_text_alone_refuses(t *testing.T) {
	for _, input := range []string{"(prices, 20) -> rolling -> s", `("mean") -> rolling -> s`, "(true) -> f -> y", "(a, [1]) -> f -> y"} {
		assert.Equal(t, ErrUnnamedConstant, refusedSyntax(t, input).Kind, input)
	}
	unnamed := refusedSyntax(t, "a -> f -> b\n(prices, 20) -> rolling -> s")
	require.Equal(t, ErrUnnamedConstant, unnamed.Kind)
	assert.Contains(t, unnamed.Message, "constant 20 in statement 1 ")
	assert.Contains(t, unnamed.Message, "name=20")

	for _, each := range [][2]string{
		{"true -> f -> y", "true"},
		{"x -> f -> null", "null"},
		{"x -> f -> (out=false)", "false"},
		{"x -> f -> (a, b=null)", "null"},
	} {
		err := refusedSyntax(t, each[0])
		require.Equal(t, ErrReservedNodeName, err.Kind, each[0])
		assert.Contains(t, err.Message, "'"+each[1]+"'", each[0])
	}
	// As a member's value those words are constants, which is what they are.
	assert.Equal(t, []part{named("a", "=null")}, membersOf(oneWiring(t, "(a=null) -> f -> y")))

	several := refusedSyntax(t, "x -> f -> (a, b, c=d)")
	require.Equal(t, ErrSeveralUnnamedTargets, several.Kind)
	assert.Contains(t, several.Message, "(a, b)")

	for _, each := range [][2]string{
		{"a -> f{retries=3} -> b", "retries"},
		{"a -> f{retry=-1} -> b", "-1"},
		{"a -> f{retry=1.5} -> b", "1.5"},
		{"a -> f{retry=many} -> b", "many"},
		{"a -> f{on_error=ignore} -> b", "ignore"},
		{"a -> f{on_error=3} -> b", "3"},
		{"a -> f{retry=1, retry=2} -> b", "twice"},
	} {
		err := refusedSyntax(t, each[0])
		require.Equal(t, ErrInvalidPolicy, err.Kind, each[0])
		assert.Contains(t, err.Message, "step 'f'", each[0])
		assert.Contains(t, err.Message, each[1], each[0])
	}

	// What the grammar does not accept at all.
	for _, input := range []string{
		"a -> f", "a -> -> b", "(a,) -> f -> b", "a -> f -> (b", "(a b) -> f -> c", "x machine clean", "(a=) -> f -> b",
		// A string that is not a JSON string: an escape JSON does not have,
		// and a raw line break.
		`(a="\q") -> f -> b`, "(a=\"x\ny\") -> f -> b", `x machine "bad \q ref"`,
	} {
		assert.Equal(t, ErrParse, refusedSyntax(t, input).Kind, input)
	}
}

// TEST12832: a constant has one text, the same in every implementation: an
// object's keys in order, a whole number without a point, a float with one
// (or an exponent beyond sixteen digits and below five zeros), and only the
// escapes JSON requires. Two constants are equal as JSON values: `1` and `1.0`
// are not, and an object's key order is not part of it.
func Test12832_a_constant_has_one_text(t *testing.T) {
	text := func(written string) string {
		value, err := decodeConstant(written)
		require.NoError(t, err, written)
		return constantText(value)
	}
	// Each spelling below is what the reference writes for that text.
	for written, want := range map[string]string{
		`20`:                                 `20`,
		`-7`:                                 `-7`,
		`18446744073709551615`:               `18446744073709551615`,
		`18446744073709551616`:               `1.8446744073709552e+19`,
		`-9223372036854775809`:               `-9.223372036854776e+18`,
		`1.0`:                                `1.0`,
		`1.50`:                               `1.5`,
		`-1.5e3`:                             `-1500.0`,
		`1e3`:                                `1000.0`,
		`0.0001`:                             `0.0001`,
		`0.00001`:                            `0.00001`,
		`0.000001`:                           `1e-6`,
		`1.25e-7`:                            `1.25e-7`,
		`1e15`:                               `1000000000000000.0`,
		`1e16`:                               `1e+16`,
		`1234567890123456.0`:                 `1234567890123456.0`,
		`1e21`:                               `1e+21`,
		`1.5e300`:                            `1.5e+300`,
		`5e-324`:                             `5e-324`,
		`0.0`:                                `0.0`,
		`-0.0`:                               `-0.0`,
		`123.456`:                            `123.456`,
		`{"b": 1, "a": [true, null]}`:        `{"a":[true,null],"b":1}`,
		`"a<b>&c é"`:                         `"a<b>&c é"`,
		`"tab\there \u0001 \" \\ \/ \u007f"`: "\"tab\\there \\u0001 \\\" \\\\ / \u007f\"",
		`"line\u2028sep"`:                    "\"line\u2028sep\"",
	} {
		assert.Equal(t, want, text(written), written)
		// The text reads back as the value it was written from.
		again, err := decodeConstant(text(written))
		require.NoError(t, err, written)
		first, _ := decodeConstant(written)
		assert.True(t, constantsEqual(first, again), written)
	}

	value := func(written string) any {
		decoded, err := decodeConstant(written)
		require.NoError(t, err, written)
		return decoded
	}
	assert.True(t, constantsEqual(value(`{"a":1,"b":[2,"x"]}`), value(`{"b":[2,"x"],"a":1}`)))
	for _, pair := range [][2]string{
		{`1`, `1.0`}, {`1`, `2`}, {`"1"`, `1`}, {`null`, `false`}, {`[1,2]`, `[2,1]`}, {`[1]`, `[1,1]`},
		{`{"a":1}`, `{"a":2}`}, {`{"a":1}`, `{"b":1}`}, {`{"a":1}`, `{"a":1,"b":2}`}, {`true`, `false`}, {`"a"`, `"b"`},
		{`1.5`, `2.5`}, {`null`, `0`},
	} {
		assert.False(t, constantsEqual(value(pair[0]), value(pair[1])), "%s and %s", pair[0], pair[1])
		assert.False(t, constantsEqual(value(pair[1]), value(pair[0])), "%s and %s", pair[1], pair[0])
	}
	// A whole number is one value whichever of the two integer types holds it.
	assert.True(t, constantsEqual(int64(5), uint64(5)))
	assert.True(t, constantsEqual(uint64(5), int64(5)))
	assert.False(t, constantsEqual(int64(-5), uint64(5)))

	_, err := decodeConstant(`1 2`)
	assert.Error(t, err, "one value, not two")
	_, err = decodeConstant(`1e999`)
	assert.Error(t, err, "a number no float holds")
}
