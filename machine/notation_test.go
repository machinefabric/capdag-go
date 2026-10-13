package machine

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests of machine notation end to end through the parser and the resolver:
// named members, constants, taken outputs, sources and sinks, step policy,
// the URN of a node, and the canonical form
// (capdag/docs/09-machine-notation.md).

const (
	seriesMedia     = "media:fmt=json;list;record;time-series"
	benchmarkMedia  = "media:benchmark;fmt=json;list;record;time-series"
	windowMedia     = "media:integer;window"
	labelMedia      = "media:enc=utf-8;label"
	tagMedia        = "media:enc=utf-8;tag"
	comparisonMedia = "media:comparison;fmt=json;record"
	residualsMedia  = "media:fmt=json;list;record;residual"
	noteMedia       = "media:enc=utf-8;note"
	referenceMedia  = "media:enc=utf-8;series-ref"
)

// definition builds a cap and holds it to the definition rules: a fixture that
// breaks one is not a fixture.
func definition(capUrn, title string, args []cap.CapArg, outputs []cap.CapOutput) *cap.Cap {
	parsed, err := urn.NewCapUrnFromString(capUrn)
	if err != nil {
		panic(fmt.Sprintf("%s: %v", capUrn, err))
	}
	c := cap.NewCapWithArgs(parsed, title, []string{title}, args)
	c.Version = 1
	c.SetOutputs(outputs)
	if err := cap.ValidateCapDefinition(c); err != nil {
		panic(fmt.Sprintf("the fixture '%s' is not a valid definition: %v", title, err))
	}
	return c
}

func flagArg(mediaUrn, name string) cap.CapArg {
	flag := "--" + name
	return cap.NewCapArg(mediaUrn, name, false, []cap.ArgSource{{CliFlag: &flag}})
}

func stdinArg(mediaUrn, name string) cap.CapArg {
	stdin := mediaUrn
	return cap.NewCapArg(mediaUrn, name, true, []cap.ArgSource{{Stdin: &stdin}})
}

func outputOf(mediaUrn, name, description string) cap.CapOutput {
	return *cap.NewCapOutput(mediaUrn, name, description)
}

// compareCap: a main input and a second arg that accepts the same type, two
// constants' worth of options, a sequence arg, and three outputs.
func compareCap() *cap.Cap {
	tags := flagArg(tagMedia, "tags")
	tags.IsSequence = true
	residuals := outputOf(residualsMedia, "residuals", "Residuals")
	residuals.IsSequence = true
	return definition(
		fmt.Sprintf(`cap:in="%s";compare;out="%s"`, seriesMedia, comparisonMedia),
		"compare",
		[]cap.CapArg{
			stdinArg(seriesMedia, "series"),
			flagArg(benchmarkMedia, "benchmark").Accepting(seriesMedia),
			flagArg(windowMedia, "window"),
			flagArg(labelMedia, "label"),
			tags,
		},
		[]cap.CapOutput{
			residuals,
			outputOf(comparisonMedia, "comparison", "The comparison"),
			*cap.NewCapOutput(noteMedia, "note", "A note").AsOptional(true),
		},
	)
}

// fetchCap: a source. Its `in=` is void; it is given a reference and nothing else.
func fetchCap() *cap.Cap {
	return definition(
		fmt.Sprintf(`cap:in=media:void;fetch;out="%s"`, seriesMedia),
		"fetch",
		[]cap.CapArg{flagArg(referenceMedia, "reference")},
		[]cap.CapOutput{outputOf(seriesMedia, "series", "The series")},
	)
}

// publishCap: a sink. Its `out=` is void.
func publishCap() *cap.Cap {
	return definition(
		fmt.Sprintf(`cap:in="%s";publish;out=media:void`, comparisonMedia),
		"publish",
		[]cap.CapArg{stdinArg(comparisonMedia, "report")},
		nil,
	)
}

// meanCap: one value from a sequence of residuals — maps nothing.
func meanCap() *cap.Cap {
	rows := stdinArg(residualsMedia, "rows")
	rows.IsSequence = true
	return definition(
		fmt.Sprintf(`cap:in="%s";mean;out="media:mean;numeric"`, residualsMedia),
		"mean",
		[]cap.CapArg{rows},
		[]cap.CapOutput{outputOf("media:mean;numeric", "mean", "The mean")},
	)
}

// scoreCap: one value per residual — a sequence of them maps it.
func scoreCap() *cap.Cap {
	return definition(
		fmt.Sprintf(`cap:in="%s";score;out="media:numeric;score"`, residualsMedia),
		"score",
		[]cap.CapArg{stdinArg(residualsMedia, "row")},
		[]cap.CapOutput{outputOf("media:numeric;score", "score", "The score")},
	)
}

func mediaDef(mediaUrn string, validation *media.MediaValidation, schema any) media.StoredMediaDef {
	return media.StoredMediaDef{
		Urn:        mediaUrn,
		MediaType:  "application/json",
		Title:      mediaUrn,
		Schema:     schema,
		Validation: validation,
	}
}

// notationRegistry holds the caps above and the media definitions their
// constants are held to: a window of 1 to 365, a label that is a string of
// three allowed words, tags and references that are strings.
func notationRegistry() *fabric.FabricRegistry {
	registry := fabric.NewForTest()
	registry.AddCapsToCache([]*cap.Cap{compareCap(), fetchCap(), publishCap(), meanCap(), scoreCap()})
	minimum, maximum := 1.0, 365.0
	registry.AddSpec(mediaDef(windowMedia, &media.MediaValidation{Min: &minimum, Max: &maximum}, map[string]any{"type": "integer"}))
	registry.AddSpec(mediaDef(labelMedia, &media.MediaValidation{AllowedValues: []string{"mean", "median", "max"}}, map[string]any{"type": "string"}))
	registry.AddSpec(mediaDef(tagMedia, nil, map[string]any{"type": "string"}))
	registry.AddSpec(mediaDef(referenceMedia, nil, nil))
	return registry
}

const notationHeaders = `[compare cap:in="media:fmt=json;list;record;time-series";compare;out="media:comparison;fmt=json;record"]` +
	`[fetch cap:in=media:void;fetch;out="media:fmt=json;list;record;time-series"]` +
	`[publish cap:in="media:comparison;fmt=json;record";publish;out=media:void]` +
	`[mean cap:in="media:fmt=json;list;record;residual";mean;out="media:mean;numeric"]` +
	`[score cap:in="media:fmt=json;list;record;residual";score;out="media:numeric;score"]`

func parseNotation(wirings string) (*Machine, *MachineParseError) {
	return ParseMachine(notationHeaders+wirings, notationRegistry())
}

func machineOf(t *testing.T, wirings string) *Machine {
	t.Helper()
	m, err := parseNotation(wirings)
	require.Nil(t, err, "%s must resolve: %v", wirings, err)
	return m
}

func resolutionError(t *testing.T, wirings string) *MachineAbstractionError {
	t.Helper()
	_, err := parseNotation(wirings)
	require.NotNil(t, err, "%s must be refused", wirings)
	require.NotNil(t, err.Abstraction, "%s: expected a resolution error, got %v", wirings, err)
	return err.Abstraction
}

// onlyEdge is the one edge of a one-step machine.
func onlyEdge(t *testing.T, m *Machine) *MachineEdge {
	t.Helper()
	require.Equal(t, 1, m.StrandCount())
	require.Len(t, m.Strands()[0].Edges(), 1)
	return m.Strands()[0].Edges()[0]
}

// boundTo is what each arg of an edge is bound to, by the arg's name: a node's
// URN, or a constant.
type boundTo struct{ arg, source string }

func bindingsOf(m *Machine, edge *MachineEdge) []boundTo {
	strand := m.Strands()[0]
	bound := make([]boundTo, len(edge.Assignment))
	for i, b := range edge.Assignment {
		source := "constant " + constantText(b.Source.Constant)
		if !b.Source.IsConstant {
			source = fmt.Sprintf("node %d %s", b.Source.Node, strand.NodeUrn(b.Source.Node))
		}
		bound[i] = boundTo{b.ArgName, source}
	}
	return bound
}

func sourcesOf(bound []boundTo, arg string) []string {
	var sources []string
	for _, each := range bound {
		if each.arg == arg {
			sources = append(sources, each.source)
		}
	}
	return sources
}

func bindingNamed(t *testing.T, edge *MachineEdge, arg string) EdgeAssignmentBinding {
	t.Helper()
	for _, b := range edge.Assignment {
		if b.ArgName == arg {
			return b
		}
	}
	t.Fatalf("no binding of arg %q on %s", arg, edge)
	return EdgeAssignmentBinding{}
}

func equivalentNotation(t *testing.T, a, b string) bool {
	t.Helper()
	return machineOf(t, a).IsEquivalent(machineOf(t, b))
}

// TEST12737: a member that names its arg binds that arg, whatever its type
// could also fit. Two inputs of one type are told apart only so: unnamed they
// are refused, and naming the other one is another machine. A name is the
// definition's label for the arg; the binding records the arg's media URN.
func Test12737_a_named_member_binds_the_arg_it_names(t *testing.T) {
	text := "[(path, benchmark=bench) -> compare -> result]"
	m, names, parseErr := ParseMachineWithNodeNames(notationHeaders+text, notationRegistry())
	require.Nil(t, parseErr)
	edge := onlyEdge(t, m)
	require.Len(t, edge.Assignment, 2)
	// Both are inputs of the machine and both hold a series: the benchmark arg
	// ACCEPTS a series, and is identified by its own URN.
	series, benchmark := bindingNamed(t, edge, "series"), bindingNamed(t, edge, "benchmark")
	for _, each := range []EdgeAssignmentBinding{series, benchmark} {
		require.False(t, each.Source.IsConstant)
		assert.True(t, m.Strands()[0].NodeUrn(each.Source.Node).IsEquivalent(mediaUrn(seriesMedia)), each.ArgName)
	}
	assert.NotEqual(t, series.Source.Node, benchmark.Source.Node, "two nodes")
	assert.True(t, benchmark.CapArgMediaUrn.IsEquivalent(mediaUrn(benchmarkMedia)))
	assert.True(t, series.CapArgMediaUrn.IsEquivalent(mediaUrn(seriesMedia)))

	// Which node is the benchmark is what the machine says, and a host
	// addresses the two by the names as written.
	assert.Equal(t, names[0]["bench"], benchmark.Source.Node)
	assert.Equal(t, names[0]["path"], series.Source.Node)

	// Unnamed, nothing says which of the two series is the benchmark.
	untyped := resolutionError(t, "[(path, bench) -> compare -> result]")
	require.Equal(t, ErrAbstractionUntypedMachineInput, untyped.Kind)
	assert.Contains(t, untyped.Message, "(path, bench)")

	// The same two, produced by steps: each has a type, and it fits two args.
	produced := `[(reference="a") -> fetch -> a][(reference="b") -> fetch -> b]`
	assert.Equal(t, ErrAbstractionAmbiguousMachineNotation,
		resolutionError(t, produced+"[(a, b) -> compare -> result]").Kind)
	said := machineOf(t, produced+"[(a, benchmark=b) -> compare -> result]")
	assert.Len(t, said.Strands()[0].Edges(), 3)
	// An arg a member named is said: an unnamed member is not matched into it
	// as well. Two unnamed series beside a named benchmark have one arg left
	// between them, and that is what the refusal says — not that they tie.
	third := `[(reference="c") -> fetch -> c]`
	assert.Equal(t, ErrAbstractionUnmatchedSourceInCapArgs,
		resolutionError(t, produced+third+"[(a, c, benchmark=b) -> compare -> result]").Kind)

	// A name the cap does not declare, with the names it does.
	unknown := resolutionError(t, "[(path, baseline=bench) -> compare -> result]")
	require.Equal(t, ErrAbstractionUnknownArgName, unknown.Kind)
	assert.Contains(t, unknown.Message, "'baseline'")
	assert.Contains(t, unknown.Message, "(it declares: series, benchmark, window, label, tags)")
	// A name is not read out of a URN: `time-series` is a tag of the arg's
	// type, and no arg is called that.
	assert.Equal(t, ErrAbstractionUnknownArgName,
		resolutionError(t, "[(time-series=path) -> compare -> result]").Kind)
	// A produced node of another type, named into an arg that does not accept it.
	mismatch := resolutionError(t, "[path -> compare -> r][(series=p2, benchmark=r) -> compare -> r2]")
	require.Equal(t, ErrAbstractionSourceDoesNotConformToArg, mismatch.Kind)
	assert.Contains(t, mismatch.Message, "arg 'benchmark' accepts '"+mediaUrn(seriesMedia).String()+"'")
	assert.Contains(t, mismatch.Message, "its source is '"+mediaUrn(comparisonMedia).String()+"'")
	// An arg that takes one value, named twice.
	twice := resolutionError(t, "[(path, benchmark=b1, benchmark=b2) -> compare -> result]")
	require.Equal(t, ErrAbstractionArgBoundTwice, twice.Kind)
	assert.Contains(t, twice.Message, "arg 'benchmark' takes one value and is named by 2 members")
}

// TEST12738: a constant is the arg's value, written in the machine. It is held
// to the validation of the arg's media definition when the machine is
// resolved, and a sequence arg gathers the constants named into it, in order.
func Test12738_a_constant_is_bound_and_validated(t *testing.T) {
	m := machineOf(t, `[(path, window=20, label="mean", tags="a", tags="b") -> compare -> result]`)
	bound := bindingsOf(m, onlyEdge(t, m))
	assert.Equal(t, []string{"constant 20"}, sourcesOf(bound, "window"))
	assert.Equal(t, []string{`constant "mean"`}, sourcesOf(bound, "label"))
	assert.Equal(t, []string{`constant "a"`, `constant "b"`}, sourcesOf(bound, "tags"), "gathered in the order written")
	// A constant is no node: the machine has one input and one output.
	assert.Len(t, m.Strands()[0].Nodes(), 2)
	assert.Len(t, m.Strands()[0].InputAnchorIds(), 1)

	refused := func(wiring string) string {
		err := resolutionError(t, wiring)
		require.Equal(t, ErrAbstractionConstantFailsValidation, err.Kind, "%s: %v", wiring, err)
		return err.Message
	}
	// Outside the range the media definition gives.
	low := refused("[(path, window=0) -> compare -> result]")
	assert.Contains(t, low, "arg 'window' refuses the constant 0: ")
	assert.Contains(t, low, "minimum value 1")
	assert.Contains(t, refused("[(path, window=366) -> compare -> result]"), "maximum value 365")
	// Inside the range and not the kind of value the schema asks for.
	assert.Contains(t, refused("[(path, window=2.5) -> compare -> result]"), "schema")
	assert.Contains(t, refused(`[(path, window="20") -> compare -> result]`), "schema")
	// A word the definition does not allow.
	assert.Contains(t, refused(`[(path, label="mode") -> compare -> result]`), `allowed values: ["mean", "median", "max"]`)
	// An arg whose media definition is not at hand cannot be given a constant
	// nobody could check.
	unchecked := refused("[(path, benchmark=[1, 2]) -> compare -> result]")
	assert.Contains(t, unchecked, "arg 'benchmark' refuses the constant [1,2]")
	assert.Contains(t, unchecked, "not available")

	// The bounds themselves are allowed.
	machineOf(t, "[(path, window=1) -> compare -> result]")
	machineOf(t, "[(path, window=365) -> compare -> result]")

	// A constant is not a node. One input feeding two args, and the same input
	// feeding one of them beside a constant for the other, have the same nodes
	// and are different machines.
	withBenchmarkDef := notationRegistry()
	withBenchmarkDef.AddSpec(mediaDef(benchmarkMedia, nil, nil))
	inRegistry := func(wirings string) *Machine {
		parsed, err := ParseMachine(notationHeaders+wirings, withBenchmarkDef)
		require.Nil(t, err, "%s: %v", wirings, err)
		return parsed
	}
	twice := inRegistry("[(series=p, benchmark=p) -> compare -> r]")
	once := inRegistry("[(series=p, benchmark=[1, 2]) -> compare -> r]")
	assert.Equal(t, len(twice.Strands()[0].Nodes()), len(once.Strands()[0].Nodes()))
	assert.False(t, twice.IsEquivalent(once))
	assert.False(t, once.IsEquivalent(twice))

	// Two machines that give an arg different constants are different machines.
	assert.True(t, equivalentNotation(t, "[(path, window=20) -> compare -> r]", "[(p, window=20) -> compare -> out]"))
	assert.False(t, equivalentNotation(t, "[(path, window=20) -> compare -> r]", "[(path, window=21) -> compare -> r]"))
	assert.False(t, equivalentNotation(t, "[(path, window=20) -> compare -> r]", "[path -> compare -> r]"))
}

// TEST12739: the outputs a wiring takes. An unnamed target takes the step's
// main output, a named one the output of that name; one that is not taken is
// discarded; a sink takes none and a source is given nothing.
func Test12739_outputs_sources_and_sinks(t *testing.T) {
	both := machineOf(t, "[path -> compare -> (result, residuals=res)]")
	edge := onlyEdge(t, both)
	// Sorted by output URN, each node holding what its output is declared to be.
	require.Len(t, edge.Outputs, 2)
	isSequence := map[string]bool{}
	for _, output := range edge.Outputs {
		isSequence[output.OutputName] = both.Strands()[0].NodeIsSequence(output.Node)
	}
	assert.Equal(t, map[string]bool{"comparison": false, "residuals": true}, isSequence)
	main, taken := edge.MainTarget()
	require.True(t, taken, "the main output is taken")
	assert.True(t, both.Strands()[0].NodeUrn(main).IsEquivalent(mediaUrn(comparisonMedia)))
	assert.Len(t, both.Strands()[0].OutputAnchorIds(), 2)

	// The main output need not be taken, and the one that is, is still named.
	residualsOnly := machineOf(t, "[path -> compare -> (residuals=res)]")
	_, taken = onlyEdge(t, residualsOnly).MainTarget()
	assert.False(t, taken)
	assert.Len(t, onlyEdge(t, residualsOnly).Outputs, 1)
	assert.False(t, residualsOnly.IsEquivalent(both))
	// Which outputs are taken is not their order in the text.
	assert.True(t, equivalentNotation(t, "[path -> compare -> (result, residuals=res)]", "[p -> compare -> (residuals=r, out)]"))

	unknown := resolutionError(t, "[path -> compare -> (result, errors=e)]")
	require.Equal(t, ErrAbstractionUnknownOutputName, unknown.Kind)
	assert.Contains(t, unknown.Message, "'errors'")
	assert.Contains(t, unknown.Message, "(it declares: residuals, comparison, note)")
	assert.Equal(t, ErrAbstractionOutputTakenTwice,
		resolutionError(t, "[path -> compare -> (residuals=a, residuals=b)]").Kind)
	// The main output taken unnamed and by its name is the same output twice.
	assert.Equal(t, ErrAbstractionOutputTakenTwice,
		resolutionError(t, "[path -> compare -> (a, comparison=b)]").Kind)

	// A sink: nothing is taken, and its strand has no output.
	sink := machineOf(t, "[report -> publish -> ()]")
	assert.Empty(t, onlyEdge(t, sink).Outputs)
	assert.Empty(t, sink.Strands()[0].OutputAnchorIds())
	assert.Len(t, sink.Strands()[0].InputAnchorIds(), 1)
	// A sink has no main output to take.
	assert.Equal(t, ErrAbstractionNoMainOutput, resolutionError(t, "[report -> publish -> done]").Kind)

	// A source: given nothing, or only what its constants say.
	source := machineOf(t, "[() -> fetch -> s]")
	assert.Empty(t, onlyEdge(t, source).Assignment)
	assert.Empty(t, source.Strands()[0].InputAnchorIds())
	referenced := machineOf(t, `[(reference="fred/UNRATE") -> fetch -> s]`)
	assert.Equal(t, []boundTo{{"reference", `constant "fred/UNRATE"`}}, bindingsOf(referenced, onlyEdge(t, referenced)))
	assert.False(t, referenced.IsEquivalent(source))
	// A source cannot be given an input, and a cap that takes one is given it.
	assert.Equal(t, ErrAbstractionUntypedMachineInput, resolutionError(t, "[x -> fetch -> s]").Kind)
	unbound := resolutionError(t, "[() -> compare -> r]")
	require.Equal(t, ErrAbstractionMainInputUnbound, unbound.Kind)
	assert.Contains(t, unbound.Message, "('series')")
	assert.Equal(t, ErrAbstractionMainInputUnbound, resolutionError(t, "[(benchmark=b) -> compare -> r]").Kind)

	// What a sequence output feeds: a step that takes the sequence runs once,
	// one that takes a single value runs once per item.
	folded := machineOf(t, "[path -> compare -> (residuals=res)][res -> mean -> m]")
	mapped := machineOf(t, "[path -> compare -> (residuals=res)][res -> score -> s]")
	loops := func(m *Machine, step string) bool {
		for _, e := range m.Strands()[0].Edges() {
			if strings.Contains(e.CapUrn.String(), step) {
				return e.IsLoop
			}
		}
		t.Fatalf("no step %s", step)
		return false
	}
	assert.False(t, loops(folded, "mean"))
	assert.True(t, loops(mapped, "score"))
	assert.False(t, loops(mapped, "compare"))
}

// TEST12740: the media URN of a node comes from the wirings that touch it and
// never from where a member stands or which wiring comes first.
func Test12740_the_urn_of_a_node_is_position_free(t *testing.T) {
	// Members in either order, wirings in either order: one machine.
	a := "[(path, benchmark=bench, window=20) -> compare -> (r, residuals=res)][res -> score -> s]"
	b := "[res -> score -> s][(window=20, benchmark=bench, path) -> compare -> (residuals=res, r)]"
	assert.True(t, equivalentNotation(t, a, b))
	// And one text: the canonical notation names its nodes by what they are
	// for, not by which was written first.
	assert.Equal(t, machineOf(t, a).ToMachineNotation(), machineOf(t, b).ToMachineNotation())
	// The two inputs are not interchangeable: a machine that names a DIFFERENT
	// pairing of its nodes to a second step is another machine.
	assert.False(t, equivalentNotation(t,
		"[(path, benchmark=bench) -> compare -> (residuals=r)][(path, benchmark=bench) -> compare -> (note=n)]",
		"[(path, benchmark=bench) -> compare -> (residuals=r)][(bench, benchmark=path) -> compare -> (note=n)]",
	))

	// An input of the machine named into an arg holds what that arg accepts,
	// and the unnamed one beside it holds what the main input accepts.
	m, names, parseErr := ParseMachineWithNodeNames(notationHeaders+"[(benchmark=bench, path) -> compare -> r]", notationRegistry())
	require.Nil(t, parseErr)
	for _, name := range []string{"bench", "path"} {
		assert.True(t, m.Strands()[0].NodeUrn(names[0][name]).IsEquivalent(mediaUrn(seriesMedia)), name)
	}

	// Naming the main input is allowed; then an unnamed input beside it has no
	// arg anything decides.
	machineOf(t, "[(series=path, benchmark=bench) -> compare -> r]")
	assert.Equal(t, ErrAbstractionUntypedMachineInput,
		resolutionError(t, "[(series=path, bench) -> compare -> r]").Kind)

	// An input of the machine given two types that are not on one chain: it is
	// named into an arg that accepts a series and into one that accepts a
	// comparison.
	conflict := resolutionError(t, "[(series=x) -> compare -> r][(report=x) -> publish -> ()]")
	require.Equal(t, ErrAbstractionNodeMediaConflict, conflict.Kind)
	assert.Contains(t, conflict.Message, "node 'x'")
	// A node a step produces has that output's type, and a name does not
	// change it: named into an arg that does not accept it, it is refused there.
	assert.Equal(t, ErrAbstractionSourceDoesNotConformToArg,
		resolutionError(t, "[path -> compare -> r][(series=r) -> compare -> r2]").Kind)
}

// TEST12741: the canonical notation. Every binding and every taken output is
// written under its name, so nothing in it is matched by type; a machine
// written out and read back is the same machine, and the text does not change
// on a second pass. A policy is part of what a machine is.
func Test12741_canonical_notation_round_trips(t *testing.T) {
	for _, wirings := range []string{
		"[path -> compare -> result]",
		"[(path, benchmark=bench) -> compare -> result]",
		`[(path, window=20, label="max", tags="b", tags="a") -> compare -> (result, residuals=res, note=n)]`,
		`[(reference="fred/UNRATE") -> fetch{retry=3} -> s][s -> compare{retry=1, on_error=absent} -> (residuals=res)][res -> score -> x]`,
		"[() -> fetch -> s]",
		"[report -> publish{on_error=absent} -> ()]",
	} {
		first := machineOf(t, wirings)
		text := first.ToMachineNotation()
		second, err := FromString(text, notationRegistry())
		require.Nil(t, err, "canonical text must read back: %v\n%s", err, text)
		assert.True(t, first.IsEquivalent(second), "%s\n%s", wirings, text)
		assert.Equal(t, text, second.ToMachineNotation(), "a fixed point")
		lineBased := first.ToMachineNotationFormatted(NotationFormatLineBased)
		third, err := FromString(lineBased, notationRegistry())
		require.Nil(t, err, "%v\n%s", err, lineBased)
		assert.True(t, first.IsEquivalent(third), lineBased)
	}

	text := machineOf(t, `[(path, benchmark=bench, window=20) -> compare{retry=2} -> (result, residuals=res)]`).ToMachineNotation()
	// Named throughout, and the policy on the step.
	for _, part := range []string{"series=n", "benchmark=n", "window=20", "comparison=n", "residuals=n", "edge_0{retry=2}"} {
		assert.Contains(t, text, part)
	}
	// A gather keeps the order its members were written in.
	assert.Contains(t, machineOf(t, `[(path, tags="b", tags="a") -> compare -> r]`).ToMachineNotation(), `tags="b", tags="a"`)

	// The policy is read onto the edge, and two machines that differ in it differ.
	policy := func(wirings string) StepPolicy { return onlyEdge(t, machineOf(t, wirings)).Policy }
	assert.True(t, policy("[p -> compare -> r]").IsDefault())
	assert.Equal(t, StepPolicy{Retry: 2, OnError: OnErrorAbsent}, policy("[p -> compare{retry=2, on_error=absent} -> r]"))
	assert.False(t, equivalentNotation(t, "[p -> compare{retry=2} -> r]", "[p -> compare -> r]"))
	assert.False(t, equivalentNotation(t, "[p -> compare{retry=2} -> r]", "[p -> compare{retry=3} -> r]"))
	assert.False(t, equivalentNotation(t, "[p -> compare{on_error=absent} -> r]", "[p -> compare -> r]"))
	assert.True(t, equivalentNotation(t, "[p -> compare{retry=2} -> r]", "[q -> compare { retry = 2 } -> s]"))

	// Two machines of the same nodes, steps and anchors, whose steps take
	// their outputs differently: which step makes the residuals is part of
	// what each is.
	first := "[p -> compare -> r1][(p, window=20) -> compare -> (r2, residuals=x)]"
	second := "[p -> compare -> (r1, residuals=x)][(p, window=20) -> compare -> r2]"
	assert.False(t, equivalentNotation(t, first, second))
	assert.False(t, equivalentNotation(t, second, first))

	// A machine is not the machine it is the first steps of.
	one := "[p -> compare -> (residuals=res)]"
	two := "[p -> compare -> (residuals=res)][res -> score -> s]"
	assert.False(t, equivalentNotation(t, one, two))
	assert.False(t, equivalentNotation(t, two, one))
}

// TEST12742: a machine header names another machine. Read with nothing to
// resolve the reference against, it is refused by name rather than ignored.
func Test12742_a_machine_reference_needs_a_source(t *testing.T) {
	_, err := parseNotation(`[clean machine "lib/clean.machine"][path -> compare -> r]`)
	require.NotNil(t, err, "a machine reference cannot be resolved without a source")
	require.NotNil(t, err.Syntax, "%v", err)
	assert.Equal(t, ErrMachineReferenceWithoutSource, err.Syntax.Kind)
	assert.Contains(t, err.Syntax.Message, `machine header 'clean' refers to "lib/clean.machine"`)
}

// TEST12743: the render payload carries what the graph view needs of the new
// forms: each binding's arg name and its node or its constant, each taken
// output with its name, and the step's policy.
func Test12743_render_payload_carries_names_constants_and_outputs(t *testing.T) {
	m := machineOf(t, `[(path, window=20, label="max") -> compare{retry=2, on_error=absent} -> (result, residuals=res)]`)
	var payload struct {
		Strands []struct {
			Nodes []struct{ Id, Urn string }
			Edges []map[string]json.RawMessage
			Input []string `json:"input_anchor_nodes"`
		}
	}
	require.NoError(t, json.Unmarshal([]byte(m.ToRenderPayloadJSON()), &payload), "the payload is JSON")
	require.Len(t, payload.Strands, 1)
	require.Len(t, payload.Strands[0].Edges, 1)
	edge := payload.Strands[0].Edges[0]
	assert.Equal(t, "2", string(edge["retry"]))
	assert.Equal(t, `"absent"`, string(edge["on_error"]))
	assert.Equal(t, `"edge_0"`, string(edge["alias"]))
	assert.Equal(t, `false`, string(edge["is_loop"]))
	assert.Equal(t, "[]", string(edge["included_as"]), "a step the machine wrote itself")
	_, hasTarget := edge["target_node"]
	assert.False(t, hasTarget, "one target is not the shape any more")

	var assignment []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(edge["assignment"], &assignment))
	byArg := map[string]map[string]json.RawMessage{}
	for _, binding := range assignment {
		var name string
		require.NoError(t, json.Unmarshal(binding["arg_name"], &name))
		byArg[name] = binding
	}
	require.Len(t, byArg, 3)
	assert.Equal(t, "20", string(byArg["window"]["constant"]))
	assert.Equal(t, `"max"`, string(byArg["label"]["constant"]))
	_, windowHasNode := byArg["window"]["source_node"]
	assert.False(t, windowHasNode)
	assert.Equal(t, `"`+payload.Strands[0].Input[0]+`"`, string(byArg["series"]["source_node"]), "the series is the machine's one input")
	_, seriesHasConstant := byArg["series"]["constant"]
	assert.False(t, seriesHasConstant)
	assert.Equal(t, `"`+mediaUrn(windowMedia).String()+`"`, string(byArg["window"]["cap_arg_media_urn"]))

	var outputs []struct {
		Urn  string `json:"output_media_urn"`
		Name string `json:"output_name"`
		Node string
	}
	require.NoError(t, json.Unmarshal(edge["outputs"], &outputs))
	require.Len(t, outputs, 2)
	nodeUrn := map[string]string{}
	for _, node := range payload.Strands[0].Nodes {
		nodeUrn[node.Id] = node.Urn
	}
	for _, output := range outputs {
		// Each output goes to the node that holds its type.
		assert.Equal(t, output.Urn, nodeUrn[output.Node], output.Name)
	}
	assert.ElementsMatch(t, []string{"comparison", "residuals"}, []string{outputs[0].Name, outputs[1].Name})
}

// TEST12833: an output that carries the input holds the input's type, not the
// URN that names it: a router's branches are typed by what was routed, so a
// step that takes that type can be wired to one. Which branch a wiring takes
// is part of what the machine is, although both hold one type.
func Test12833_a_carried_output_holds_the_inputs_type(t *testing.T) {
	const (
		verdictMedia = "media:enc=utf-8;verdict"
		keptMedia    = "media:kept;marker"
		droppedMedia = "media:dropped;marker"
		routeUrn     = `cap:in="` + seriesMedia + `";route;out="` + verdictMedia + `"`
	)
	registry := notationRegistry()
	registry.AddCapsToCache([]*cap.Cap{definition(routeUrn, "route",
		[]cap.CapArg{stdinArg(seriesMedia, "series")},
		[]cap.CapOutput{
			outputOf(verdictMedia, "verdict", "The verdict"),
			*cap.NewCapOutput(keptMedia, "kept", "What was kept").AsOptional(true).CarryingInput(),
			*cap.NewCapOutput(droppedMedia, "dropped", "What was dropped").AsOptional(true).CarryingInput(),
		})})
	headers := notationHeaders + "[route " + routeUrn + "]"
	parse := func(wirings string) (*Machine, []StrandNodeNames) {
		m, names, err := ParseMachineWithNodeNames(headers+wirings, registry)
		require.Nil(t, err, "%s: %v", wirings, err)
		return m, names
	}

	m, names := parse("[s -> route -> (v, kept=k, dropped=d)][k -> compare -> r]")
	held := func(name string) string { return m.Strands()[0].NodeUrn(names[0][name]).String() }
	assert.Equal(t, mediaUrn(seriesMedia).String(), held("k"), "the branch holds what was routed")
	assert.Equal(t, mediaUrn(seriesMedia).String(), held("d"))
	assert.Equal(t, mediaUrn(verdictMedia).String(), held("v"), "the main output holds its own type")
	// The edge still records which output each is, by the output's own URN.
	route := oneEdgeOf(t, m, "route")
	taken := map[string]string{}
	for _, output := range route.Outputs {
		taken[output.OutputName] = output.OutputMediaUrn.String()
	}
	assert.Equal(t, map[string]string{
		"verdict": mediaUrn(verdictMedia).String(),
		"kept":    mediaUrn(keptMedia).String(),
		"dropped": mediaUrn(droppedMedia).String(),
	}, taken)

	// An output that carries nothing holds the URN it declares.
	plain, plainNames := parse("[p -> compare -> (note=n)]")
	assert.Equal(t, mediaUrn(noteMedia).String(), plain.Strands()[0].NodeUrn(plainNames[0]["n"]).String())

	// The two branches hold one type and are two outputs.
	keptOnly, _ := parse("[s -> route -> (kept=x)]")
	keptAgain, _ := parse("[t -> route -> (kept=y)]")
	droppedOnly, _ := parse("[s -> route -> (dropped=x)]")
	assert.True(t, keptOnly.IsEquivalent(keptAgain))
	assert.False(t, keptOnly.IsEquivalent(droppedOnly))
	assert.False(t, droppedOnly.IsEquivalent(keptOnly))
}

// TEST12834: the correspondence between the nodes of two strands is one to
// one in both directions: a pair made once is confirmed, and a node of either
// strand paired with a second node of the other is refused.
func Test12834_a_node_stands_for_one_node(t *testing.T) {
	nodes := newNodeBijection(3, 3)
	assert.True(t, nodes.bind(0, 2))
	assert.True(t, nodes.bind(0, 2), "the same pair again")
	assert.False(t, nodes.bind(0, 1), "one node of this strand, two of the other")
	assert.False(t, nodes.bind(1, 2), "two nodes of this strand, one of the other")
	// A refused pair leaves nothing behind: both nodes it named are still free.
	assert.True(t, nodes.bind(1, 1))
	assert.True(t, nodes.bind(2, 0))
	assert.True(t, nodes.bind(1, 1), "and a pair made stays made")
}
