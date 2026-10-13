package machine

import (
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST1400: N sources gather into a single sequence arg when no injection
// exists. Three page sources feed a one-arg concat cap whose arg is a sequence
// — all three land on the same arg, in source-declaration order.
func Test1400_gather_n_sources_into_sequence_arg(t *testing.T) {
	sources := []*urn.MediaUrn{mediaUrn("media:enc=utf-8;page"), mediaUrn("media:enc=utf-8;page"), mediaUrn("media:enc=utf-8;page")}
	args := []*urn.MediaUrn{mediaUrn("media:enc=utf-8")}
	step := `cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt"`
	pairs, err := matchSourcesToArgs(sources, args, []bool{true}, -1, step, 0)
	require.Nil(t, err, "three sources must gather into the sequence arg")
	require.Len(t, pairs, 3, "one binding per gathered source")
	for _, pair := range pairs {
		assert.True(t, pair.arg.IsEquivalent(mediaUrn("media:enc=utf-8")))
		assert.True(t, pair.source.IsEquivalent(mediaUrn("media:enc=utf-8;page")))
	}
	// The same three with an arg that takes one value have nowhere to go.
	_, err = matchSourcesToArgs(sources, args, []bool{false}, -1, step, 0)
	require.NotNil(t, err)
	assert.Equal(t, ErrAbstractionUnmatchedSourceInCapArgs, err.Kind)
}

// TEST1401: product precedence — when a valid injection exists it wins, even
// though gathering everything into the sequence arg is also valid. s1 exactly
// fits the scalar page arg; s2 only fits the sequence arg.
func Test1401_product_precedence_over_gather(t *testing.T) {
	sources := []*urn.MediaUrn{mediaUrn("media:enc=utf-8;page"), mediaUrn("media:enc=utf-8;summary")}
	args := []*urn.MediaUrn{mediaUrn("media:enc=utf-8;page"), mediaUrn("media:enc=utf-8")}
	pairs, err := matchSourcesToArgs(sources, args, []bool{false, true}, -1,
		`cap:in="media:enc=utf-8;page";compose;out="media:enc=utf-8;ext=txt"`, 0)
	require.Nil(t, err, "unique injection must resolve")
	require.Len(t, pairs, 2)
	// The page source must claim the scalar page arg (injection), NOT be
	// gathered into the sequence arg alongside the summary.
	placed := map[string]string{}
	for _, pair := range pairs {
		placed[pair.arg.String()] = pair.source.String()
	}
	assert.Equal(t, map[string]string{
		mediaUrn("media:enc=utf-8;page").String(): mediaUrn("media:enc=utf-8;page").String(),
		mediaUrn("media:enc=utf-8").String():      mediaUrn("media:enc=utf-8;summary").String(),
	}, placed)

	// Two pages, a sequence arg that takes pages and a scalar arg that takes
	// any text: each page can take either arg, at one cost — a tie. Gathering
	// both into the sequence arg would be cheaper and unique, and it is not
	// what is asked: a tied product is ambiguous, it does not become a gather.
	pages := []*urn.MediaUrn{mediaUrn("media:enc=utf-8;page"), mediaUrn("media:enc=utf-8;page")}
	_, err = matchSourcesToArgs(pages, args, []bool{true, false}, -1, "compose", 0)
	require.NotNil(t, err)
	assert.Equal(t, ErrAbstractionAmbiguousMachineNotation, err.Kind)
}

// TEST1402: a gather tie between two sequence args is ambiguous, not silently
// ordered.
func Test1402_gather_ambiguity_and_scalar_pigeonhole_fail_hard(t *testing.T) {
	// Two equally-costed sequence args (each drops a different one of the
	// source's tags → the same absolute specificity gap) → every distribution
	// of the three sources ties → ambiguous.
	chunk := mediaUrn("media:chunk;enc=utf-8;page")
	sources := []*urn.MediaUrn{chunk, chunk, chunk}
	args := []*urn.MediaUrn{mediaUrn("media:enc=utf-8;page"), mediaUrn("media:chunk;enc=utf-8")}
	_, err := matchSourcesToArgs(sources, args, []bool{true, true}, -1, `cap:in="media:enc=utf-8";t;out="media:enc=utf-8"`, 0)
	require.NotNil(t, err)
	assert.Equal(t, ErrAbstractionAmbiguousMachineNotation, err.Kind, "tied gather distributions must be ambiguous, got %v", err)
}

// gatherFixture is two producers of a page and a `concat` whose one arg takes
// a sequence, wired `(page from a, page from b) -> concat`.
func gatherFixture(t *testing.T, firstProducesSequence bool) (*MachineStrand, *MachineAbstractionError) {
	t.Helper()
	const (
		urnA      = `cap:in="media:document;ext=pdf";op-a;out="media:enc=utf-8;page"`
		urnB      = `cap:in="media:ext=md";op-b;out="media:enc=utf-8;page"`
		urnConcat = `cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;ext=txt"`
	)
	capA := buildCap(urnA, "op_a", []string{"media:document;ext=pdf"}, "media:enc=utf-8;page")
	capA.Outputs[0].IsSequence = firstProducesSequence
	capB := buildCap(urnB, "op_b", []string{"media:ext=md"}, "media:enc=utf-8;page")
	concat := buildCap(urnConcat, "concat", []string{"media:enc=utf-8"}, "media:enc=utf-8;ext=txt")
	concat.Args[0].IsSequence = true
	registry := registryWith([]*cap.Cap{capA, capB, concat})

	// nodes: 0=pdf, 1=md, 2=page(from a), 3=page(from b), 4=txt
	nodes := []*urn.MediaUrn{
		mediaUrn("media:document;ext=pdf"),
		mediaUrn("media:ext=md"),
		mediaUrn("media:enc=utf-8;page"),
		mediaUrn("media:enc=utf-8;page"),
		mediaUrn("media:enc=utf-8;ext=txt"),
	}
	wirings := []preInternedWiring{
		{tokenId: mustStepToken(t, "tok-a"), capUrn: capUrnVal(urnA), sourceNodeIds: []NodeId{0}, targets: []wiringTarget{{node: 2}}},
		{tokenId: mustStepToken(t, "tok-b"), capUrn: capUrnVal(urnB), sourceNodeIds: []NodeId{1}, targets: []wiringTarget{{node: 3}}},
		{tokenId: mustStepToken(t, "tok-concat"), capUrn: capUrnVal(urnConcat), sourceNodeIds: []NodeId{2, 3}, targets: []wiringTarget{{node: 4}}},
	}
	return resolvePreInterned(nodes, wirings, registry, 0)
}

func concatEdge(t *testing.T, strand *MachineStrand) *MachineEdge {
	t.Helper()
	for _, edge := range strand.Edges() {
		if strings.Contains(edge.CapUrn.String(), "concat") {
			return edge
		}
	}
	t.Fatal("concat edge present")
	return nil
}

// TEST1403: resolvePreInterned emits N bindings on ONE sequence arg for a
// fan-in gather `(x, y) -> concat`, with IsLoop false (the gathered value IS
// the sequence the arg consumes) and stable source order.
func Test1403_resolve_gather_emits_n_bindings_on_sequence_arg(t *testing.T) {
	strand, err := gatherFixture(t, false)
	require.Nil(t, err, "gather wiring must resolve")
	edge := concatEdge(t, strand)
	require.Len(t, edge.Assignment, 2, "the sequence arg gathers BOTH producers as two bindings")
	// Both bindings target the same slot.
	assert.True(t, edge.Assignment[0].CapArgMediaUrn.IsEquivalent(edge.Assignment[1].CapArgMediaUrn))
	assert.Equal(t, "input", edge.Assignment[0].ArgName)
	assert.Equal(t, "input", edge.Assignment[1].ArgName)
	// Deterministic gather order = source declaration order (node 2 then 3).
	assert.Equal(t, NodeSource(2), edge.Assignment[0].Source)
	assert.Equal(t, NodeSource(3), edge.Assignment[1].Source)
	// The gathered value IS the sequence the arg consumes — no map.
	assert.False(t, edge.IsLoop, "gather-fed sequence arg must not loop")
	// Anchors: two inputs (pdf, md), one output (txt).
	assert.Len(t, strand.InputAnchors(), 2)
	assert.Len(t, strand.OutputAnchors(), 1)
	// The gather is written as its arg's name once per member, in item order.
	// Nodes are named in the order the canonical walk meets them: the pdf and
	// its page, the markdown and its page, then the text.
	text := fromResolvedStrands([]*MachineStrand{strand}).ToMachineNotation()
	assert.Contains(t, text, "[(input=n0) -> edge_0 -> (output=n1)][(input=n2) -> edge_1 -> (output=n3)]")
	assert.Contains(t, text, "[(input=n1, input=n3) -> edge_2 -> (output=n4)]")
}

// TEST1404: a sequence PRODUCER inside a gather resolves — the gather flattens
// at execution (each member contributes its item(s) in binding order). This is
// the batch-plus-single convergence shape: one leg yields a sequence, the
// other one item, and the fold consumes them all as one sequence.
func Test1404_sequence_producer_in_gather_flattens(t *testing.T) {
	strand, err := gatherFixture(t, true)
	require.Nil(t, err, "a gather with a sequence member must resolve (execution flattens)")
	edge := concatEdge(t, strand)
	assert.Len(t, edge.Assignment, 2, "both members bound to the sequence arg")
	assert.False(t, edge.IsLoop, "the gathered value IS the sequence the arg consumes — no map")
	assert.True(t, strand.NodeIsSequence(2), "the node op-a produces holds a sequence")
	assert.False(t, strand.NodeIsSequence(3))
}
