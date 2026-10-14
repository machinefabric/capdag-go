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
// three allowed words, tags that are strings, and references of at most
// twelve bytes.
func notationRegistry() *fabric.FabricRegistry {
	registry := fabric.NewForTest()
	registry.AddCapsToCache([]*cap.Cap{compareCap(), fetchCap(), publishCap(), meanCap(), scoreCap()})
	minimum, maximum := 1.0, 365.0
	registry.AddSpec(mediaDef(windowMedia, &media.MediaValidation{Min: &minimum, Max: &maximum}, map[string]any{"type": "integer"}))
	registry.AddSpec(mediaDef(labelMedia, &media.MediaValidation{AllowedValues: []string{"mean", "median", "max"}}, map[string]any{"type": "string"}))
	registry.AddSpec(mediaDef(tagMedia, nil, map[string]any{"type": "string"}))
	twelve := 12
	registry.AddSpec(mediaDef(referenceMedia, &media.MediaValidation{MaxLength: &twelve}, nil))
	return registry
}

// titledRegistry is the notation registry with a media definition, and so a
// title, for every media a node of the tests' machines holds: what a render
// payload needs.
func titledRegistry() *fabric.FabricRegistry {
	registry := notationRegistry()
	for _, urn := range []string{seriesMedia, benchmarkMedia, comparisonMedia, residualsMedia, noteMedia, "media:mean;numeric", "media:numeric;score"} {
		registry.AddSpec(mediaDef(urn, nil, nil))
	}
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
	// Two inputs that hold one type come in the order their nodes were made.
	assert.Equal(t, []NodeId{names[0]["path"], names[0]["bench"]}, m.Strands()[0].InputAnchorIds())

	// Unnamed, nothing says which of the two series is the benchmark.
	untyped := resolutionError(t, "[(path, bench) -> compare -> result]")
	require.Equal(t, ErrAbstractionUntypedMachineInput, untyped.Kind)
	assert.Contains(t, untyped.Message, "(path, bench)")

	// The same two, produced by steps: each has a type, and it fits two args.
	produced := `[(reference="a") -> fetch -> a][(reference="b") -> fetch -> b]`
	// The refusal says which members to name, by the names the text gave
	// them, and the args each could be for.
	seriesUrn := mediaUrn(seriesMedia).String()
	ambiguous := resolutionError(t, produced+"[(a, b) -> compare -> result]")
	require.Equal(t, ErrAbstractionAmbiguousMachineNotation, ambiguous.Kind)
	assert.Equal(t, []UndecidedMember{
		{Node: "a", SourceUrn: seriesUrn, CouldBe: []string{"series", "benchmark"}},
		{Node: "b", SourceUrn: seriesUrn, CouldBe: []string{"series", "benchmark"}},
	}, ambiguous.Members)
	assert.Contains(t, ambiguous.Message, "name them: 'a' ("+seriesUrn+") could be series or benchmark; 'b' (")
	// A member is named by the name the text gave it, wherever it stands in
	// its group and whichever node was written first.
	swapped := resolutionError(t, produced+"[(b, a) -> compare -> result]")
	require.Equal(t, ErrAbstractionAmbiguousMachineNotation, swapped.Kind)
	require.Len(t, swapped.Members, 2)
	assert.Equal(t, []string{"b", "a"}, []string{swapped.Members[0].Node, swapped.Members[1].Node})
	// The args a member could be for are the ones no name took: with the
	// first of three bound by name, the other two are what is left to say.
	mixUrn := fmt.Sprintf(`cap:in="%s";mix;out="%s"`, seriesMedia, comparisonMedia)
	withMix := notationRegistry()
	withMix.AddCapsToCache([]*cap.Cap{definition(mixUrn, "mix",
		[]cap.CapArg{
			stdinArg(seriesMedia, "first"),
			flagArg("media:second;fmt=json;list;record;time-series", "second").Accepting(seriesMedia),
			flagArg("media:third;fmt=json;list;record;time-series", "third").Accepting(seriesMedia),
		},
		[]cap.CapOutput{outputOf(comparisonMedia, "mixed", "The mix")})})
	_, mixErr := ParseMachine(notationHeaders+"[mix "+mixUrn+"]"+produced+`[(reference="c") -> fetch -> c][(first=c, a, b) -> mix -> result]`, withMix)
	require.NotNil(t, mixErr)
	require.NotNil(t, mixErr.Abstraction, "%v", mixErr)
	assert.Equal(t, []UndecidedMember{
		{Node: "a", SourceUrn: seriesUrn, CouldBe: []string{"second", "third"}},
		{Node: "b", SourceUrn: seriesUrn, CouldBe: []string{"second", "third"}},
	}, mixErr.Abstraction.Members)
	said := machineOf(t, produced+"[(a, benchmark=b) -> compare -> result]")
	assert.Len(t, said.Strands()[0].Edges(), 3)
	// An arg a member named is said: an unnamed member is not matched into it
	// as well. Two unnamed series beside a named benchmark have one arg left
	// between them, and that is what the refusal says — not that they tie.
	third := `[(reference="c") -> fetch -> c]`
	assert.Equal(t, ErrAbstractionUnmatchedSourceInCapArgs,
		resolutionError(t, produced+third+"[(a, c, benchmark=b) -> compare -> result]").Kind)

	// A member that fits no arg at all is the one the refusal names, wherever
	// it stands: here the second, a comparison where a series is taken.
	unfit := resolutionError(t, produced+"[a -> compare -> made][(b, made) -> compare -> result]")
	require.Equal(t, ErrAbstractionUnmatchedSourceInCapArgs, unfit.Kind)
	assert.Contains(t, unfit.Message, "source URN '"+mediaUrn(comparisonMedia).String()+"'")

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

	// A string's length is its UTF-8 bytes: six two-byte characters fill a
	// reference of twelve, and a seventh breaks it.
	machineOf(t, `[(reference="éééééé") -> fetch -> s]`)
	assert.Contains(t, refused(`[(reference="ééééééé") -> fetch -> s]`), "maximum length 12")

	// A schema's required properties are held: a benchmark written as an
	// object says its points.
	withBenchmarkSchema := notationRegistry()
	withBenchmarkSchema.AddSpec(mediaDef(benchmarkMedia, nil, map[string]any{
		"type": "object", "required": []any{"points"},
		"properties": map[string]any{"points": map[string]any{"type": "array"}},
	}))
	_, perr := ParseMachine(notationHeaders+`[(path, benchmark={"points": [1, 2]}) -> compare -> r]`, withBenchmarkSchema)
	require.Nil(t, perr, "a benchmark that says its points: %v", perr)
	_, perr = ParseMachine(notationHeaders+`[(path, benchmark={"scale": 2}) -> compare -> r]`, withBenchmarkSchema)
	require.NotNil(t, perr, "a benchmark that does not say its points")
	require.NotNil(t, perr.Abstraction, "%v", perr)
	assert.Equal(t, ErrAbstractionConstantFailsValidation, perr.Abstraction.Kind)
	assert.Contains(t, perr.Abstraction.Message, "schema")

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
	// Whether a step maps is part of what the machine is: the same text, read
	// where the residuals are one value, is another machine.
	oneValue := notationRegistry()
	single := compareCap()
	single.OutputNamed("residuals").IsSequence = false
	oneValue.AddCapsToCache([]*cap.Cap{single})
	unmapped, parseErr := ParseMachine(notationHeaders+"[path -> compare -> (residuals=res)][res -> score -> s]", oneValue)
	require.Nil(t, parseErr)
	assert.False(t, loops(unmapped, "score"))
	assert.False(t, unmapped.IsEquivalent(mapped))
	assert.False(t, mapped.IsEquivalent(unmapped))
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

	// Two steps of one cap that differ only in what feeds an arg have one
	// order in the canonical text, whichever was written first: the one fed
	// by a node, then the one fed by a constant.
	withBenchmarkDef := notationRegistry()
	withBenchmarkDef.AddSpec(mediaDef(benchmarkMedia, nil, nil))
	byNode := "[(series=p, benchmark=q) -> compare -> (note=a)]"
	byConstant := "[(series=p, benchmark=[1, 2]) -> compare -> (note=b)]"
	var texts []string
	for _, wirings := range []string{byNode + byConstant, byConstant + byNode} {
		parsed, err := ParseMachine(notationHeaders+wirings, withBenchmarkDef)
		require.Nil(t, err, "%v", err)
		texts = append(texts, parsed.ToMachineNotation())
	}
	assert.Equal(t, texts[0], texts[1])
	assert.Less(t, strings.Index(texts[0], "benchmark=n"), strings.Index(texts[0], "benchmark=[1,2]"), texts[0])
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
			Nodes []struct{ Id, Urn, Title string }
			Edges []map[string]json.RawMessage
			Input []string `json:"input_anchor_nodes"`
		}
	}
	text, perr := m.ToRenderPayloadJSON(titledRegistry())
	require.Nil(t, perr)
	require.NoError(t, json.Unmarshal([]byte(text), &payload), "the payload is JSON")
	require.Len(t, payload.Strands, 1)
	require.Len(t, payload.Strands[0].Edges, 1)
	edge := payload.Strands[0].Edges[0]
	assert.Equal(t, "2", string(edge["retry"]))
	assert.Equal(t, `"absent"`, string(edge["on_error"]))
	assert.Equal(t, `"edge_0"`, string(edge["alias"]))
	assert.Equal(t, `"compare"`, string(edge["title"]), "the cap's title")
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
	for _, node := range payload.Strands[0].Nodes {
		assert.Equal(t, node.Urn, node.Title, "the media definition's title")
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

// TEST12838: what a machine sorts by URN, it sorts by the URN's parts — the
// prefix, then the tags by key and value; for a cap its input, its output, its
// effect and then its tags — and never by the URN's text. The two orders
// differ wherever one tag key is the beginning of another (`file`,
// `file-path`), and the canonical form is the same in every implementation
// only because all of them use the first.
func Test12838_urns_are_ordered_by_their_parts_not_their_text(t *testing.T) {
	standsBefore := func(first, second string) bool {
		return mediaUrn(first).Compare(mediaUrn(second)) < 0
	}
	const sample, path = "media:file;sample", "media:file-path"
	assert.True(t, standsBefore(sample, path))
	assert.False(t, standsBefore(path, sample))
	assert.Less(t, mediaUrn(path).String(), mediaUrn(sample).String(), "the text orders them the other way")
	// A URN that is the beginning of another stands first; a tag with any
	// value before the same tag with one; values in their own order.
	assert.True(t, standsBefore("media:file", "media:file;sample"))
	assert.True(t, standsBefore("media:enc", "media:enc=ascii"))
	assert.True(t, standsBefore("media:enc=ascii", "media:enc=utf-8"))
	assert.Zero(t, mediaUrn("media:sample;file").Compare(mediaUrn(sample)))

	capBefore := func(first, second string) bool {
		return capUrnVal(first).Compare(capUrnVal(second)) < 0
	}
	// A cap's input decides before anything else it says.
	assert.True(t, capBefore(`cap:in="`+sample+`";zz;out="media:x"`, `cap:in="`+path+`";aa;out="media:x"`))
	// Then its output — and only then: an input that stands first wins over
	// an output that stands last.
	assert.True(t, capBefore(`cap:in="media:x";zz;out="`+sample+`"`, `cap:in="media:x";aa;out="`+path+`"`))
	assert.True(t, capBefore(`cap:in="`+sample+`";op;out="`+path+`"`, `cap:in="`+path+`";op;out="`+sample+`"`))
	// Then its effect, in the order: declared, none, patch, unspecified.
	effects := []string{
		`cap:in="media:x";op;out="media:x"`,
		`cap:effect=none;in="media:x";op;out="media:x"`,
		`cap:effect=patch;in="media:x";op;out="media:x"`,
		`cap:effect=?;in="media:x";op;out="media:x"`,
	}
	for i := 0; i+1 < len(effects); i++ {
		assert.True(t, capBefore(effects[i], effects[i+1]), "%s before %s", effects[i], effects[i+1])
		assert.False(t, capBefore(effects[i+1], effects[i]))
		assert.True(t, capUrnVal(effects[i]).Less(capUrnVal(effects[i+1])))
	}
	// Then its other tags — after the effect, whatever they say.
	assert.True(t, capBefore(`cap:in="media:x";aa;out="media:x"`, `cap:in="media:x";zz;out="media:x"`))
	assert.True(t, capBefore(`cap:in="media:x";zz;out="media:x"`, `cap:effect=patch;in="media:x";aa;out="media:x"`))
	assert.Zero(t, capUrnVal(effects[2]).Compare(capUrnVal(effects[2])))

	// In a machine: the bindings of a step, the outputs it takes, the inputs
	// of the strand and the steps themselves all stand in that order.
	const result, where = "media:file;result", "media:file-path;written"
	pairUrn := `cap:in="` + sample + `";pair;out="` + result + `"`
	firstUrn := `cap:in="` + sample + `";zz-first;out="media:file;a"`
	secondUrn := `cap:in="` + sample + `";aa-second;out="` + path + `"`
	registry := fabric.NewForTest()
	registry.AddCapsToCache([]*cap.Cap{
		definition(pairUrn, "pair",
			[]cap.CapArg{stdinArg(sample, "sample"), flagArg(path, "path")},
			[]cap.CapOutput{outputOf(result, "result", ""), outputOf(where, "where", "")}),
		definition(firstUrn, "first", []cap.CapArg{stdinArg(sample, "sample")}, []cap.CapOutput{outputOf("media:file;a", "made", "")}),
		definition(secondUrn, "second", []cap.CapArg{stdinArg(sample, "sample")}, []cap.CapOutput{outputOf(path, "made", "")}),
	})

	m, names, parseErr := ParseMachineWithNodeNames("[pair "+pairUrn+"][(path=p, s) -> pair -> (where=w, r)]", registry)
	require.Nil(t, parseErr, "%v", parseErr)
	edge := onlyEdge(t, m)
	require.Len(t, edge.Assignment, 2)
	assert.Equal(t, []string{"sample", "path"}, []string{edge.Assignment[0].ArgName, edge.Assignment[1].ArgName})
	require.Len(t, edge.Outputs, 2)
	assert.Equal(t, []string{"result", "where"}, []string{edge.Outputs[0].OutputName, edge.Outputs[1].OutputName})
	assert.Equal(t, []NodeId{names[0]["s"], names[0]["p"]}, m.Strands()[0].InputAnchorIds())
	assert.Equal(t, []NodeId{names[0]["r"], names[0]["w"]}, m.Strands()[0].OutputAnchorIds())
	assert.Contains(t, m.ToMachineNotation(), "[(sample=n0, path=n1) -> edge_0 -> (result=n2, where=n3)]")

	headers := "[first " + firstUrn + "][second " + secondUrn + "]"
	for _, wirings := range []string{"[x -> first -> a][x -> second -> b]", "[x -> second -> b][x -> first -> a]"} {
		parsed, err := ParseMachine(headers+wirings, registry)
		require.Nil(t, err, "%v", err)
		steps := parsed.Strands()[0].Edges()
		require.Len(t, steps, 2)
		assert.Contains(t, steps[0].CapUrn.String(), "zz-first", wirings)
		assert.Contains(t, steps[1].CapUrn.String(), "aa-second", wirings)
	}
}
