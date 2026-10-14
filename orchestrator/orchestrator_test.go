package orchestrator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/urn"
)

const (
	extractCap   = `cap:in="media:document;ext=pdf";extract;out="media:enc=utf-8;ext=txt"`
	summarizeCap = `cap:in="media:enc=utf-8;ext=txt";summarize;out="media:enc=utf-8;summary"`
	concatCap    = `cap:in="media:enc=utf-8";concat;out="media:enc=utf-8;joined"`
	splitCap     = `cap:in="media:enc=utf-8;ext=txt";split;out="media:enc=utf-8;head"`
	nowCap       = `cap:in="media:void";now;out="media:stamp"`
	contextUrn   = "media:context;enc=utf-8"
	restUrn      = "media:enc=utf-8;rest"
)

// testCap is the smallest definition of `capUrn`: its input on stdin unless
// it is void, and the output its URN states.
func testCap(t *testing.T, capUrn string) *cap.Cap {
	t.Helper()
	parsed, err := urn.NewCapUrnFromString(capUrn)
	require.NoError(t, err)
	c := cap.NewCap(parsed, "t", []string{"t"})
	if in := parsed.InSpec(); in != "media:void" {
		c.Args = append(c.Args, cap.NewCapArg(in, "input", true, []cap.ArgSource{{Stdin: &in}}))
	}
	c.AddOutput(*cap.NewCapOutput(parsed.OutSpec(), "output", "out"))
	return c
}

func testRegistry(t *testing.T) *fabric.FabricRegistry {
	t.Helper()
	registry := fabric.NewForTest()
	summarize := testCap(t, summarizeCap)
	flag := "--context"
	summarize.Args = append(summarize.Args, cap.NewCapArg(contextUrn, "context", false, []cap.ArgSource{{CliFlag: &flag}}))
	concat := testCap(t, concatCap)
	concat.Args[0].IsSequence = true
	split := testCap(t, splitCap)
	split.AddOutput(*cap.NewCapOutput(restUrn, "rest", "the rest"))
	registry.AddCapsToCache([]*cap.Cap{testCap(t, extractCap), summarize, concat, split, testCap(t, nowCap)})
	return registry
}

// testPlan: doc → extract → summarize (context ← now) → out; extract also
// feeds split, whose `rest` output is taken; concat gathers summarize and
// rest.
func testPlan(t *testing.T) *planner.MachinePlan {
	t.Helper()
	plan := planner.NewMachinePlan("convert")
	plan.AddNode(planner.NewInputSlotNode("doc", "input", "media:document;ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("extract", extractCap))
	plan.AddNode(planner.NewMachineNode("summarize", summarizeCap))
	plan.AddNode(planner.NewMachineNode("now", nowCap))
	plan.AddNode(planner.NewMachineNode("split", splitCap))
	plan.AddNode(planner.NewCapOutputNode("split.rest", "split", restUrn, "rest", false))
	plan.AddNode(planner.NewCollectNode("both", []string{"summarize", "split.rest"}, "media:enc=utf-8"))
	plan.AddNode(planner.NewMachineNode("concat", concatCap))
	plan.AddNode(planner.NewOutputNode("out", "result", "concat"))
	plan.AddEdge(planner.NewDirectEdge("doc", "extract"))
	plan.AddEdge(planner.NewDirectEdge("extract", "summarize"))
	plan.AddEdge(planner.NewArgEdge("now", "summarize", contextUrn))
	plan.AddEdge(planner.NewDirectEdge("extract", "split"))
	plan.AddEdge(planner.NewDirectEdge("split", "split.rest"))
	plan.AddEdge(planner.NewCollectionEdge("summarize", "both"))
	plan.AddEdge(planner.NewCollectionEdge("split.rest", "both"))
	plan.AddEdge(planner.NewDirectEdge("both", "concat"))
	plan.AddEdge(planner.NewDirectEdge("concat", "out"))
	require.NoError(t, plan.Validate())
	return plan
}

func stepOf(t *testing.T, graph *ResolvedGraph, node string) *ResolvedStep {
	t.Helper()
	for _, step := range graph.Steps {
		if step.Node == node {
			return step
		}
	}
	t.Fatalf("no step '%s'", node)
	return nil
}

func invalidGraphMessage(t *testing.T, err error) string {
	t.Helper()
	require.Error(t, err)
	var refused *ParseOrchestrationError
	require.True(t, errors.As(err, &refused), "%v", err)
	require.Equal(t, ErrInvalidGraph, refused.Kind, "%v", err)
	return refused.Message
}

// TEST1161: every step of a plan becomes one step of the graph, keyed by
// its node; its main input is the stream its cap's `in=` names and a
// named arg the stream the edge names; a step nothing is wired into has no
// inputs; a gather is one input with its members in order; an output of a
// step that something takes is listed with the node it is stored at.
func Test1161_a_plan_converts_to_steps_with_their_inputs_and_outputs(t *testing.T) {
	graph, err := PlanToResolvedGraph(testPlan(t), testRegistry(t))
	require.NoError(t, err)
	require.Len(t, graph.Steps, 5)
	require.NotNil(t, graph.GraphName)
	assert.Equal(t, "convert", *graph.GraphName)

	extract := stepOf(t, graph, "extract")
	assert.Equal(t, []StepInput{{StreamUrn: "media:document;ext=pdf", Source: NodeStepSource("doc")}}, extract.Inputs)
	assert.Empty(t, extract.Outputs)
	assert.Equal(t, "media:enc=utf-8;ext=txt", extract.OutMedia)

	summarize := stepOf(t, graph, "summarize")
	assert.Equal(t, []StepInput{
		{StreamUrn: "media:enc=utf-8;ext=txt", Source: NodeStepSource("extract")},
		{StreamUrn: contextUrn, Source: NodeStepSource("now")},
	}, summarize.Inputs)

	assert.Empty(t, stepOf(t, graph, "now").Inputs, "a source is sent no stream")

	split := stepOf(t, graph, "split")
	assert.Equal(t, []StepOutput{{OutputMediaUrn: restUrn, Node: "split.rest"}}, split.Outputs)
	assert.Equal(t, []string{"split", "split.rest"}, split.ProducedNodes())

	concat := stepOf(t, graph, "concat")
	assert.Equal(t, []StepInput{{
		StreamUrn: "media:enc=utf-8",
		Source:    GatherStepSource([]string{"summarize", "split.rest"}),
	}}, concat.Inputs)
	assert.Equal(t, []string{"summarize", "split.rest"}, concat.SourceNodes())

	// Every node a step reads or produces has its media; the plan's
	// input is there as what the graph is given.
	assert.Equal(t, "media:document;ext=pdf", graph.Nodes["doc"])
	assert.Equal(t, restUrn, graph.Nodes["split.rest"])
	assert.Equal(t, "media:enc=utf-8;joined", graph.Nodes["concat"])
	assert.NotContains(t, graph.Nodes, "both", "a gather is an input, not a node of the graph")
	assert.NotContains(t, graph.Nodes, "out")

	// A step alone: what it produces has its media whether or not a step of
	// the graph reads it, and what it reads is what the graph is given.
	alone, err := StepsToResolvedGraph(testPlan(t), []string{"split"}, testRegistry(t))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"split":      "media:enc=utf-8;head",
		"split.rest": restUrn,
		"extract":    "media:enc=utf-8;ext=txt",
	}, alone.Nodes)

	// And a step that reads a step's other output from outside the graph is
	// given it under that output's own type.
	gathering, err := StepsToResolvedGraph(testPlan(t), []string{"concat"}, testRegistry(t))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"concat":     "media:enc=utf-8;joined",
		"summarize":  "media:enc=utf-8;summary",
		"split.rest": restUrn,
	}, gathering.Nodes)
}

// TEST770: a plan with a map is not one graph, and the refusal names the
// map; the steps of one scope of it are, with what they read from outside
// (an item, a step of the enclosing scope) as the graph's inputs.
func Test770_a_plan_with_a_map_is_run_scope_by_scope(t *testing.T) {
	plan := planner.NewMachinePlan("mapped")
	plan.AddNode(planner.NewInputSlotNode("docs", "input", "media:document;ext=pdf", planner.CardinalitySequence))
	plan.AddNode(planner.NewMachineNode("now", nowCap))
	plan.AddNode(planner.NewForEachNode("each",
		[]planner.ForEachSource{{Node: "docs", Item: "doc"}},
		[]planner.NodeId{"doc", "extract", "summarize"}))
	plan.AddNode(planner.NewItemNode("doc", "each", "docs", "media:document;ext=pdf"))
	plan.AddNode(planner.NewMachineNode("extract", extractCap))
	plan.AddNode(planner.NewMachineNode("summarize", summarizeCap))
	plan.AddEdge(planner.NewDirectEdge("docs", "each"))
	plan.AddEdge(planner.NewIterationEdge("each", "doc"))
	plan.AddEdge(planner.NewDirectEdge("doc", "extract"))
	plan.AddEdge(planner.NewDirectEdge("extract", "summarize"))
	plan.AddEdge(planner.NewArgEdge("now", "summarize", contextUrn))
	require.NoError(t, plan.Validate())

	registry := testRegistry(t)
	_, err := PlanToResolvedGraph(plan, registry)
	assert.Contains(t, invalidGraphMessage(t, err), "has a map ('each')")

	body, err := StepsToResolvedGraph(plan, []string{"extract", "summarize"}, registry)
	require.NoError(t, err, "the body converts")
	require.Len(t, body.Steps, 2)
	assert.Equal(t, NodeStepSource("doc"), stepOf(t, body, "extract").Inputs[0].Source)
	assert.Equal(t, "media:document;ext=pdf", body.Nodes["doc"], "the item is what the body is given")
	assert.Equal(t, "media:stamp", body.Nodes["now"], "a step of the enclosing scope is given whole")

	// Not a step, and not in the plan.
	_, err = StepsToResolvedGraph(plan, []string{"each"}, registry)
	assert.Equal(t, "'each' is not a step of plan 'mapped'", invalidGraphMessage(t, err))
	_, err = StepsToResolvedGraph(plan, []string{"nowhere"}, registry)
	assert.Equal(t, "'nowhere' is not a node of plan 'mapped'", invalidGraphMessage(t, err))
}

// TEST771: a plan that wires a step in a way no invocation can be given is
// refused by what is wrong: two sources on one input stream (two main
// inputs; the same arg named by two edges, however its URN is spelled),
// a map wired straight into a step, a cap the registry does not have.
func Test771_a_step_no_invocation_can_be_given_is_refused(t *testing.T) {
	registry := testRegistry(t)
	refused := func(change func(plan *planner.MachinePlan), expect string) {
		t.Helper()
		plan := testPlan(t)
		change(plan)
		_, err := PlanToResolvedGraph(plan, registry)
		assert.Contains(t, invalidGraphMessage(t, err), expect)
	}
	refused(func(plan *planner.MachinePlan) {
		plan.AddEdge(planner.NewDirectEdge("now", "summarize"))
	}, "step 'summarize' is wired twice on input stream 'media:enc=utf-8;ext=txt'")
	// The same arg, its tags in another order.
	refused(func(plan *planner.MachinePlan) {
		plan.AddEdge(planner.NewArgEdge("doc", "summarize", "media:enc=utf-8;context"))
	}, "step 'summarize' is wired twice on input stream")
	refused(func(plan *planner.MachinePlan) {
		plan.AddEdge(planner.NewIterationEdge("now", "summarize"))
	}, "step 'summarize' has an incoming Iteration edge from 'now'")
	refused(func(plan *planner.MachinePlan) {
		plan.AddEdge(planner.NewCollectionEdge("now", "summarize"))
	}, "step 'summarize' has an incoming Collection edge from 'now'")
	refused(func(plan *planner.MachinePlan) {
		plan.AddEdge(planner.NewArgEdge("out", "extract", contextUrn))
	}, "step 'extract' is wired from 'out', which holds no value")
	refused(func(plan *planner.MachinePlan) {
		plan.AddEdge(planner.NewArgEdge("doc", "extract", "not a urn"))
	}, "step 'extract': input stream URN 'not a urn' is not a media URN")

	unknown := testPlan(t)
	unknown.AddNode(planner.NewMachineNode("mystery", `cap:in="media:x";mystery;out="media:y"`))
	_, err := PlanToResolvedGraph(unknown, registry)
	require.Error(t, err)
	var notFound *ParseOrchestrationError
	require.True(t, errors.As(err, &notFound))
	assert.Equal(t, ErrCapNotFound, notFound.Kind)
	assert.Contains(t, notFound.CapUrn, "mystery")
}

// TEST953: a step's policy and its token go with it into the graph.
func Test953_a_step_keeps_its_token_and_policy(t *testing.T) {
	plan := testPlan(t)
	policy := planner.StepPolicy{Retry: 2, OnError: planner.OnErrorAbsent}
	extract := plan.Nodes["extract"].NodeType
	extract.Policy = policy
	token := extract.TokenId
	require.False(t, token.IsZero())

	graph, err := PlanToResolvedGraph(plan, testRegistry(t))
	require.NoError(t, err)
	assert.True(t, stepOf(t, graph, "extract").Policy.Equal(policy))
	assert.Equal(t, token, stepOf(t, graph, "extract").TokenId)
	assert.True(t, stepOf(t, graph, "summarize").Policy.IsDefault())
	assert.NotEqual(t, token, stepOf(t, graph, "summarize").TokenId, "each step has its own")
}
