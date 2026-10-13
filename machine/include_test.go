package machine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/fabric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests of machines in step position: a header names another machine, and its
// steps are written into the machine that includes it
// (capdag/docs/09-machine-notation.md, §Machines in step position).

const (
	// analyseMachine: two inputs of ONE type — `series`, and `bench` — and
	// two outputs: `cmp`, and `scores`, one per residual. `res` is a node
	// inside.
	analyseMachine = "[(series, benchmark=bench, window=20) -> compare -> (cmp, residuals=res)][res -> score -> scores]"
	// pubMachine: two inputs of different types, in two strands, and one output.
	pubMachine = "[report -> publish -> ()][res -> mean -> m]"
	// winMachine: an input that feeds an arg a constant can be written for.
	winMachine = "[(series, window=w) -> compare -> cmp]"
	// outerMachine: a machine that includes `analyse`.
	outerMachine = `[analyse machine "analyse"][(series=x, bench=y) -> analyse -> (cmp=c, scores=s)][c -> publish -> ()]`

	fetchTwo = `[(reference="a") -> fetch -> a][(reference="b") -> fetch -> b]`
)

func withHeaders(wirings string) string {
	return notationHeaders + wirings
}

// library holds the machines above, each under its name.
func library() *MapMachineSource {
	source := NewMapMachineSource()
	for name, wirings := range map[string]string{
		"analyse": analyseMachine, "pub": pubMachine, "win": winMachine, "outer": outerMachine,
	} {
		source.Insert(name, withHeaders(wirings))
	}
	return source
}

func parseIncluding(wirings string) (*Machine, []StrandNodeNames, *MachineParseError) {
	return ParseMachineWithSources(withHeaders(wirings), notationRegistry(), library(), nil)
}

func including(t *testing.T, wirings string) (*Machine, []StrandNodeNames) {
	t.Helper()
	m, names, err := parseIncluding(wirings)
	require.Nil(t, err, "%s must resolve: %v", wirings, err)
	return m, names
}

// flat resolves the same wirings with no machine in them, for what an
// inclusion must equal.
func flat(t *testing.T, wirings string) *Machine {
	t.Helper()
	m, err := ParseMachine(withHeaders(wirings), notationRegistry())
	require.Nil(t, err, "%s must resolve: %v", wirings, err)
	return m
}

func refusal(t *testing.T, wirings string, kind AbstractionErrorKind) *MachineAbstractionError {
	t.Helper()
	_, _, err := parseIncluding(wirings)
	require.NotNil(t, err, "%s must be refused", wirings)
	require.NotNil(t, err.Abstraction, "%s: expected a resolution error, got %v", wirings, err)
	require.Equal(t, kind, err.Abstraction.Kind, "%s: %v", wirings, err)
	return err.Abstraction
}

func allEdges(m *Machine) []*MachineEdge {
	var edges []*MachineEdge
	for _, strand := range m.Strands() {
		edges = append(edges, strand.Edges()...)
	}
	return edges
}

// edgesOf returns the edges whose cap URN bears the tag `step`.
func edgesOf(m *Machine, step string) []*MachineEdge {
	var edges []*MachineEdge
	for _, edge := range allEdges(m) {
		for _, tag := range strings.Split(strings.TrimPrefix(edge.CapUrn.String(), "cap:"), ";") {
			if tag == step {
				edges = append(edges, edge)
				break
			}
		}
	}
	return edges
}

func oneEdgeOf(t *testing.T, m *Machine, step string) *MachineEdge {
	t.Helper()
	edges := edgesOf(m, step)
	require.Len(t, edges, 1, "one %s step", step)
	return edges[0]
}

// namesOfNodes returns, sorted, the names of the nodes `ids` picks out of each
// strand.
func namesOfNodes(m *Machine, names []StrandNodeNames, ids func(*MachineStrand) []NodeId) []string {
	picked := []string{}
	for index, strand := range m.Strands() {
		wanted := map[NodeId]bool{}
		for _, id := range ids(strand) {
			wanted[id] = true
		}
		for name, id := range names[index] {
			if wanted[id] {
				picked = append(picked, name)
			}
		}
	}
	sort.Strings(picked)
	return picked
}

func outputAnchors(s *MachineStrand) []NodeId { return s.OutputAnchorIds() }
func inputAnchors(s *MachineStrand) []NodeId  { return s.InputAnchorIds() }

// TEST12803: a machine in step position is written out as its own steps: the
// including machine is the machine one would have written by hand with those
// steps in it. Its inputs are bound by the names its notation wrote — and by
// nothing else: binding the other way round is another machine. A node inside
// it is no input or output of the whole, and each of its steps says which
// machine step it came through.
func Test12803_an_included_machine_is_its_steps_written_in(t *testing.T) {
	inlined, names := including(t,
		`[analyse machine "analyse"]`+fetchTwo+`[(series=a, bench=b) -> analyse -> (cmp=c, scores=s)][c -> publish -> ()]`)
	byHand := flat(t, fetchTwo+"[(a, benchmark=b, window=20) -> compare -> (c, residuals=r)][r -> score -> s][c -> publish -> ()]")
	assert.True(t, inlined.IsEquivalent(byHand))
	assert.Len(t, allEdges(inlined), 5)

	swapped, _ := including(t,
		`[analyse machine "analyse"]`+fetchTwo+`[(series=b, bench=a) -> analyse -> (cmp=c, scores=s)][c -> publish -> ()]`)
	assert.False(t, swapped.IsEquivalent(byHand), "the names say which input each source is for")

	// What the whole gives and takes is what was written in it.
	assert.Equal(t, []string{"s"}, namesOfNodes(inlined, names, outputAnchors))
	assert.Empty(t, namesOfNodes(inlined, names, inputAnchors))
	// The node inside the included machine is a node of the whole, under a
	// name nothing written can bear.
	assert.Contains(t, names[0], "analyse#1/res")
	assert.NotContains(t, names[0], "res")

	for _, step := range []string{"compare", "score"} {
		assert.Equal(t, []string{"analyse#1"}, oneEdgeOf(t, inlined, step).IncludedAs, step)
	}
	for _, edge := range append(edgesOf(inlined, "fetch"), edgesOf(inlined, "publish")...) {
		assert.Empty(t, edge.IncludedAs, "a step the machine wrote itself")
	}

	// The graph view is told the same.
	var payload struct {
		Strands []struct {
			Edges []struct {
				IncludedAs []string `json:"included_as"`
			}
		}
	}
	require.NoError(t, json.Unmarshal([]byte(inlined.ToRenderPayloadJSON()), &payload))
	cameThrough := 0
	for _, edge := range payload.Strands[0].Edges {
		if len(edge.IncludedAs) == 1 && edge.IncludedAs[0] == "analyse#1" {
			cameThrough++
		}
	}
	assert.Equal(t, 2, cameThrough)
}

// TEST12804: a machine used twice is written in twice. Each use has nodes of
// its own: nothing of one use is a node of the other.
func Test12804_each_use_of_a_machine_has_nodes_of_its_own(t *testing.T) {
	inlined, names := including(t, `[analyse machine "analyse"]`+fetchTwo+
		`[(series=a, bench=b) -> analyse -> (cmp=c1, scores=s1)]`+
		`[(series=b, bench=a) -> analyse -> (cmp=c2, scores=s2)]`)
	assert.Len(t, allEdges(inlined), 6)
	require.Contains(t, names[0], "analyse#1/res")
	require.Contains(t, names[0], "analyse#2/res")
	first, second := names[0]["analyse#1/res"], names[0]["analyse#2/res"]
	assert.NotEqual(t, first, second)
	through := map[string]int{}
	for _, edge := range allEdges(inlined) {
		if len(edge.IncludedAs) == 1 {
			through[edge.IncludedAs[0]]++
		}
	}
	assert.Equal(t, map[string]int{"analyse#1": 2, "analyse#2": 2}, through)
	// Each use's scoring step reads its own residuals.
	var reads []NodeId
	for _, edge := range edgesOf(inlined, "score") {
		require.False(t, edge.Assignment[0].Source.IsConstant)
		reads = append(reads, edge.Assignment[0].Source.Node)
	}
	assert.ElementsMatch(t, []NodeId{first, second}, reads)
	assert.Equal(t, []string{"c1", "c2", "s1", "s2"}, namesOfNodes(inlined, names, outputAnchors))
}

// TEST12805: an unnamed member of a machine step is matched to an input by
// type alone, and an unnamed target takes the one output the other targets
// leave. Where the types leave more than one answer, or a member has no type
// to be matched by, or fits nothing, the wiring is refused by name.
func Test12805_unnamed_members_of_a_machine_step_are_matched_by_type(t *testing.T) {
	produce := `[(reference="a") -> fetch -> a][a -> compare -> (c, residuals=r)]`
	unnamed, _ := including(t, `[pub machine "pub"]`+produce+`[(c, r) -> pub -> m]`)
	named, _ := including(t, `[pub machine "pub"]`+produce+`[(res=r, report=c) -> pub -> (m=m)]`)
	byHand := flat(t, produce+"[c -> publish -> ()][r -> mean -> m]")
	assert.True(t, unnamed.IsEquivalent(named))
	assert.True(t, unnamed.IsEquivalent(byHand))

	// What one machine step makes is typed for the next: the comparison
	// `analyse` gives is matched to the input of `pub` that takes one.
	chained, _ := including(t, `[analyse machine "analyse"][pub machine "pub"]`+fetchTwo+
		`[(series=a, bench=b) -> analyse -> (cmp=c, scores=s)][a -> compare -> (c0, residuals=r)]`+
		`[(c, r) -> pub -> m]`)
	assert.True(t, chained.IsEquivalent(flat(t, fetchTwo+
		"[(a, benchmark=b, window=20) -> compare -> (c, residuals=r1)][r1 -> score -> s]"+
		"[a -> compare -> (c0, residuals=r)][c -> publish -> ()][r -> mean -> m]")))

	// `analyse` has two outputs: an unnamed target beside a named one takes
	// the other.
	oneNamed, _ := including(t, `[analyse machine "analyse"]`+fetchTwo+`[(series=a, bench=b) -> analyse -> (c, scores=s)]`)
	bothNamed, _ := including(t, `[analyse machine "analyse"]`+fetchTwo+`[(series=a, bench=b) -> analyse -> (cmp=c, scores=s)]`)
	assert.True(t, oneNamed.IsEquivalent(bothNamed))

	// Two sources of the one type its two inputs hold.
	ambiguous := refusal(t, `[analyse machine "analyse"]`+fetchTwo+`[(a, b) -> analyse -> (cmp=c, scores=s)]`,
		ErrAbstractionAmbiguousMachineMembers)
	assert.Contains(t, ambiguous.Message, "machine 'analyse'")
	// An input of the including machine, unnamed: nothing says what it is.
	untyped := refusal(t, `[analyse machine "analyse"]`+fetchTwo+`[(x, bench=b) -> analyse -> (cmp=c, scores=s)]`,
		ErrAbstractionUntypedMachineMember)
	assert.Contains(t, untyped.Message, "machine 'analyse'")
	assert.Contains(t, untyped.Message, "(x)")
	// A comparison, where the input left takes a series.
	unmatched := refusal(t, `[analyse machine "analyse"]`+produce+`[(c, bench=a) -> analyse -> (cmp=c2, scores=s)]`,
		ErrAbstractionUnmatchedMachineMember)
	assert.Contains(t, unmatched.Message, "a member of type '"+mediaUrn(comparisonMedia).String()+"' fits no input left of machine 'analyse'")
	// Two outputs left for one unnamed target, and none left for it.
	twoLeft := refusal(t, `[analyse machine "analyse"]`+fetchTwo+`[(series=a, bench=b) -> analyse -> c]`,
		ErrAbstractionUnnamedMachineTarget)
	assert.Contains(t, twoLeft.Message, "has 2 output(s) left to take (cmp, scores)")
	noneLeft := refusal(t, `[analyse machine "analyse"]`+fetchTwo+`[(series=a, bench=b) -> analyse -> (x, cmp=c, scores=s)]`,
		ErrAbstractionUnnamedMachineTarget)
	assert.Contains(t, noneLeft.Message, "has 0 output(s) left to take ()")
}

// TEST12806: what a machine step may not be given, each refused by name: a
// name that is no input or no output of it, an input given nothing or given
// twice, an output taken twice, a source of the wrong type, and a policy.
func Test12806_a_machine_step_is_held_to_the_machines_inputs_and_outputs(t *testing.T) {
	step := func(wiring string, kind AbstractionErrorKind) string {
		return refusal(t, `[analyse machine "analyse"]`+fetchTwo+`[a -> compare -> made]`+wiring, kind).Message
	}
	assert.Contains(t, step("[(serie=a, bench=b) -> analyse -> (cmp=c, scores=s)]", ErrAbstractionUnknownMachineInput),
		"machine 'analyse' has no input named 'serie' (its inputs: bench, series)")
	assert.Contains(t, step("[(series=a, bench=b) -> analyse -> (cmp=c, score=s)]", ErrAbstractionUnknownMachineOutput),
		"has no output named 'score' (its outputs: cmp, scores)")
	assert.Contains(t, step("[(series=a) -> analyse -> (cmp=c, scores=s)]", ErrAbstractionMachineInputBinding),
		"input 'bench' of machine 'analyse' is given 0 members")
	assert.Contains(t, step("[(series=a, series=b, bench=b) -> analyse -> (cmp=c, scores=s)]", ErrAbstractionMachineInputBinding),
		"input 'series' of machine 'analyse' is given 2 members")
	assert.Contains(t, step("[(series=a, bench=b) -> analyse -> (cmp=c, cmp=d)]", ErrAbstractionMachineOutputTakenTwice),
		"output 'cmp' of machine 'analyse' is taken twice")
	assert.Contains(t, step("[(series=made, bench=b) -> analyse -> (cmp=c, scores=s)]", ErrAbstractionSourceDoesNotConformToMachineInput),
		"input 'series' of machine 'analyse' holds '"+mediaUrn(seriesMedia).String()+"', and its source is '"+mediaUrn(comparisonMedia).String()+"'")
	assert.Contains(t, step("[(series=a, bench=b) -> analyse{retry=2} -> (cmp=c, scores=s)]", ErrAbstractionPolicyOnMachine),
		"'analyse' is a machine, and a policy is written on it")

	// A cap header and a machine header name steps in one space: one alias is
	// declared once, whichever kind declares it first.
	for _, headers := range []string{
		`[fetch machine "analyse"]`,
		`[twice machine "analyse"][twice machine "pub"]`,
	} {
		_, _, err := parseIncluding(headers + fetchTwo)
		require.NotNil(t, err, headers)
		require.NotNil(t, err.Syntax, "%s: %v", headers, err)
		assert.Equal(t, ErrDuplicateAlias, err.Syntax.Kind, headers)
	}
	// A machine header is a declaration like any other: with no wiring after
	// it, the machine has headers and no steps.
	_, _, err := ParseMachineWithSources(`[analyse machine "analyse"]`, notationRegistry(), library(), nil)
	require.NotNil(t, err)
	require.NotNil(t, err.Syntax, "%v", err)
	assert.Equal(t, ErrNoEdges, err.Syntax.Kind)
}

// TEST12807: a reference that does not resolve, a machine that is not a valid
// machine, and a circle of inclusions are each refused — a header nothing uses
// included. A circle is one failure of the whole, naming every machine on the
// way round, whichever machine the reading started from.
func Test12807_a_reference_must_resolve_to_a_valid_machine_and_never_to_itself(t *testing.T) {
	lost := refusal(t, `[lost machine "nowhere"][(reference="a") -> fetch -> a]`, ErrAbstractionUnknownMachineReference)
	assert.Contains(t, lost.Message, `machine header 'lost' refers to "nowhere", which does not resolve: `)
	assert.Contains(t, lost.Message, `no machine is held under "nowhere"`)
	// With nothing to resolve against at all, the refusal says that.
	_, _, err := ParseMachineWithSources(
		withHeaders(`[lost machine "nowhere"][(reference="a") -> fetch -> a]`), notationRegistry(), NoMachineSource{}, nil)
	require.NotNil(t, err)
	require.NotNil(t, err.Syntax, "%v", err)
	assert.Equal(t, ErrMachineReferenceWithoutSource, err.Syntax.Kind)

	source := library()
	source.Insert("broken", withHeaders("[p -> nothing -> q]"))
	source.Insert("a", withHeaders(`[inner machine "b"][(reference="x") -> fetch -> s]`))
	source.Insert("b", withHeaders(`[back machine "a"][(reference="y") -> fetch -> t]`))
	parseFrom := func(notation string, id *string) *MachineAbstractionError {
		_, _, err := ParseMachineWithSources(notation, notationRegistry(), source, id)
		require.NotNil(t, err)
		require.NotNil(t, err.Abstraction, "expected a resolution error, got %v", err)
		return err.Abstraction
	}

	invalid := parseFrom(withHeaders(`[bad machine "broken"][(reference="a") -> fetch -> a]`), nil)
	require.Equal(t, ErrAbstractionInvalidIncludedMachine, invalid.Kind)
	assert.Contains(t, invalid.Message, `machine 'bad' ("broken") is not a valid machine: `)
	require.NotNil(t, invalid.Cause)
	require.NotNil(t, invalid.Cause.Syntax)
	assert.Equal(t, ErrUndefinedAlias, invalid.Cause.Syntax.Kind)
	assert.Contains(t, invalid.Cause.Syntax.Message, "'nothing'")

	a := withHeaders(`[inner machine "b"][(reference="x") -> fetch -> s]`)
	id := "a"
	circle := parseFrom(a, &id)
	require.Equal(t, ErrAbstractionCyclicMachineReference, circle.Kind)
	assert.Equal(t, []string{"a", "b", "a"}, circle.Chain)
	assert.Contains(t, circle.Message, "a -> b -> a")
	// Read as notation the source does not know: the circle is found one step
	// later, and is the same circle.
	circle = parseFrom(a, nil)
	require.Equal(t, ErrAbstractionCyclicMachineReference, circle.Kind)
	assert.Equal(t, []string{"b", "a", "b"}, circle.Chain)
}

// TEST12808: a constant written for an input of a machine step is the value of
// every arg that input feeds inside, and is held to that arg's validation
// there.
func Test12808_a_constant_for_a_machine_input_reaches_the_arg_inside(t *testing.T) {
	fetch := `[(reference="a") -> fetch -> a]`
	inlined, _ := including(t, `[win machine "win"]`+fetch+`[(series=a, w=30) -> win -> c]`)
	assert.True(t, inlined.IsEquivalent(flat(t, fetch+"[(a, window=30) -> compare -> c]")))
	window := bindingNamed(t, oneEdgeOf(t, inlined, "compare"), "window")
	assert.True(t, window.Source.IsConstant)
	assert.Equal(t, int64(30), window.Source.Constant)

	refused := refusal(t, `[win machine "win"]`+fetch+`[(series=a, w=1000) -> win -> c]`, ErrAbstractionConstantFailsValidation)
	assert.Contains(t, refused.Message, "arg 'window' refuses the constant 1000")
}

// TEST12818: a policy written on a step inside an included machine is that
// step's policy in the whole: the step is attempted again, or its failure
// tolerated, exactly as it would be where it was written.
func Test12818_a_step_keeps_its_policy_where_it_is_included(t *testing.T) {
	source := library()
	source.Insert("careful", withHeaders("[(series, benchmark=bench) -> compare{retry=2, on_error=absent} -> cmp]"))
	inlined, _, err := ParseMachineWithSources(
		withHeaders(`[careful machine "careful"]`+fetchTwo+`[(series=a, bench=b) -> careful -> c]`),
		notationRegistry(), source, nil)
	require.Nil(t, err)
	assert.Equal(t, StepPolicy{Retry: 2, OnError: OnErrorAbsent}, oneEdgeOf(t, inlined, "compare").Policy)
	assert.True(t, inlined.IsEquivalent(flat(t, fetchTwo+"[(a, benchmark=b) -> compare{retry=2, on_error=absent} -> c]")))
	for _, fetch := range edgesOf(inlined, "fetch") {
		assert.True(t, fetch.Policy.IsDefault())
	}
}

// TEST12809: a machine that includes a machine that includes a machine is one
// flat machine: a step says every machine step it came through, outermost
// first. An output of an included machine that nothing takes is not an output
// of the whole — the step that makes it still runs.
func Test12809_inclusion_nests_and_an_untaken_output_is_not_the_wholes(t *testing.T) {
	inlined, names := including(t, `[deep machine "outer"]`+fetchTwo+`[(x=a, y=b) -> deep -> (s=out)]`)
	byHand := flat(t, fetchTwo+"[(a, benchmark=b, window=20) -> compare -> (c, residuals=r)][r -> score -> s][c -> publish -> ()]")
	assert.True(t, inlined.IsEquivalent(byHand))
	assert.Equal(t, []string{"deep#1", "analyse#1"}, oneEdgeOf(t, inlined, "compare").IncludedAs)
	assert.Equal(t, []string{"deep#1"}, oneEdgeOf(t, inlined, "publish").IncludedAs)
	assert.Equal(t, []string{"out"}, namesOfNodes(inlined, names, outputAnchors))

	partial, names := including(t, `[analyse machine "analyse"]`+fetchTwo+
		`[(series=a, bench=b) -> analyse -> (cmp=c)][c -> publish -> ()]`)
	assert.Empty(t, namesOfNodes(partial, names, outputAnchors))
	assert.Empty(t, oneEdgeOf(t, partial, "score").Outputs, "what it makes is taken by nothing")
	assert.True(t, partial.IsEquivalent(flat(t, fetchTwo+
		"[(a, benchmark=b, window=20) -> compare -> (c, residuals=r)][r -> score -> ()][c -> publish -> ()]")))
}

const (
	looseCapUrn = `cap:in="media:fmt=json;list;record;time-series";loose;out="media:enc=utf-8"`
	tightCapUrn = `cap:in="media:enc=utf-8;note";tight;out="media:tightened"`
)

// registryWithARefinement is the registry of the tests above, with a step
// whose output is declared more loosely than the step after it asks for.
func registryWithARefinement() *fabric.FabricRegistry {
	registry := notationRegistry()
	registry.AddCapsToCache([]*cap.Cap{
		definition(looseCapUrn, "loose",
			[]cap.CapArg{stdinArg(seriesMedia, "series")},
			[]cap.CapOutput{outputOf("media:enc=utf-8", "text", "Some text")}),
		definition(tightCapUrn, "tight",
			[]cap.CapArg{stdinArg(noteMedia, "note")},
			[]cap.CapOutput{outputOf("media:tightened", "tightened", "Tightened")}),
	})
	return registry
}

// TEST12810: an included machine is what it is alone. Inside `refine`, the
// second step asks for a tighter type than the first declares, which makes the
// node between them hold the tighter type; written into another machine that
// node holds it still, and the machine resolves as it did alone.
func Test12810_a_node_of_an_included_machine_holds_the_type_it_held_alone(t *testing.T) {
	registry := registryWithARefinement()
	headers := notationHeaders + "[loose " + looseCapUrn + "][tight " + tightCapUrn + "]"
	refine := headers + "[s -> loose -> t][t -> tight -> u]"
	alone, _, err := ParseMachineWithNodeNames(refine, registry)
	require.Nil(t, err, "the machine resolves alone: %v", err)
	note := mediaUrn(noteMedia)
	holdsNote := false
	for _, held := range alone.Strands()[0].Nodes() {
		holdsNote = holdsNote || held.IsEquivalent(note)
	}
	assert.True(t, holdsNote, "the node between the steps holds the tighter type")

	source := NewMapMachineSource()
	source.Insert("refine", refine)
	inlined, names, err := ParseMachineWithSources(
		headers+`[refine machine "refine"][(reference="a") -> fetch -> a][a -> refine -> u]`, registry, source, nil)
	require.Nil(t, err, "the machine resolves written into another: %v", err)
	require.Contains(t, names[0], "refine#1/t")
	assert.True(t, inlined.Strands()[0].NodeUrn(names[0]["refine#1/t"]).IsEquivalent(note))
	byHand, err := ParseMachine(headers+`[(reference="a") -> fetch -> a][a -> loose -> t][t -> tight -> u]`, registry)
	require.Nil(t, err)
	assert.True(t, inlined.IsEquivalent(byHand))
}

// TEST12811: machines in files. A reference is a path relative to the file
// that wrote it — not to where the reading started — and a machine is known by
// its file, however the path to it is spelled: a file that reaches itself
// through another spelling is a circle.
func Test12811_file_references_are_relative_to_the_including_file(t *testing.T) {
	root := t.TempDir()
	lib := filepath.Join(root, "lib")
	require.NoError(t, os.MkdirAll(lib, 0o755))
	write := func(path, notation string) {
		require.NoError(t, os.WriteFile(path, []byte(notation), 0o644))
	}
	top := filepath.Join(root, "top.machine")
	topNotation := withHeaders(`[deep machine "lib/outer.machine"]` + fetchTwo + `[(x=a, y=b) -> deep -> (s=out)]`)
	write(top, topNotation)
	// `outer` names `analyse` beside itself, in `lib/`.
	write(filepath.Join(lib, "outer.machine"), withHeaders(strings.Replace(outerMachine, `"analyse"`, `"analyse.machine"`, 1)))
	write(filepath.Join(lib, "analyse.machine"), withHeaders(analyseMachine))

	source := NewFileMachineSource(t.TempDir())
	id, sourceErr := FileMachineId(top)
	require.Nil(t, sourceErr)
	inlined, _, err := ParseMachineWithSources(topNotation, notationRegistry(), source, &id)
	require.Nil(t, err, "every reference is read relative to the file that wrote it: %v", err)
	assert.Len(t, allEdges(inlined), 5)

	// Notation from no file reads its references relative to the base.
	_, _, err = ParseMachineWithSources(topNotation, notationRegistry(), NewFileMachineSource(root), nil)
	require.Nil(t, err, "relative to the base: %v", err)
	_, _, err = ParseMachineWithSources(topNotation, notationRegistry(), source, nil)
	require.NotNil(t, err)
	require.NotNil(t, err.Abstraction, "%v", err)
	assert.Equal(t, ErrAbstractionUnknownMachineReference, err.Abstraction.Kind)
	assert.Contains(t, err.Abstraction.Message, `refers to "lib/outer.machine"`)
	assert.Contains(t, err.Abstraction.Message, "outer.machine: ")

	itself := filepath.Join(root, "itself.machine")
	itselfNotation := withHeaders(`[again machine "lib/../itself.machine"][(reference="a") -> fetch -> a]`)
	write(itself, itselfNotation)
	id, sourceErr = FileMachineId(itself)
	require.Nil(t, sourceErr)
	_, _, err = ParseMachineWithSources(itselfNotation, notationRegistry(), source, &id)
	require.NotNil(t, err)
	require.NotNil(t, err.Abstraction, "%v", err)
	require.Equal(t, ErrAbstractionCyclicMachineReference, err.Abstraction.Kind)
	assert.Equal(t, []string{id, id}, err.Abstraction.Chain)

	// A file that is not there has no id.
	_, sourceErr = FileMachineId(filepath.Join(root, "absent.machine"))
	require.NotNil(t, sourceErr)
	assert.False(t, sourceErr.NoSource)
	assert.Contains(t, sourceErr.Error(), "absent.machine")
}
