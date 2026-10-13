package orchestrator

import (
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/urn"
)

func buildTestCap(t *testing.T, capUrn string, title string) *cap.Cap {
	t.Helper()
	parsed, err := urn.NewCapUrnFromString(capUrn)
	if err != nil {
		t.Fatalf("parse cap urn: %v", err)
	}
	c := cap.NewCap(parsed, title, []string{"test-command"})
	c.Outputs = []cap.CapOutput{*&cap.CapOutput{
		MediaUrn:          parsed.OutSpec(),
		OutputDescription: title + " output",
	}}
	return c
}

func buildTestRegistry(t *testing.T, capUrns []string) *fabric.FabricRegistry {
	t.Helper()
	registry := fabric.NewForTest()
	caps := make([]*cap.Cap, 0, len(capUrns))
	for index, capUrn := range capUrns {
		caps = append(caps, buildTestCap(t, capUrn, "Test Cap "+string(rune('0'+index))))
	}
	registry.AddCapsToCache(caps)
	return registry
}

// TEST1142: ResolvedGraph.to_mermaid() renders node shapes, deduplicates edges, and escapes labels
func Test1142_resolved_graph_to_mermaid_renders_shapes_dedupes_edges_and_escapes(t *testing.T) {
	extractCap := buildTestCap(
		t,
		`cap:in="media:document;ext=pdf";extract;out="media:txt;enc=utf-8"`,
		`Extract "Title" <One>\path`,
	)
	embedCap := buildTestCap(
		t,
		`cap:in="media:txt;enc=utf-8";embed;out="media:embedding;record"`,
		"Embed",
	)

	graphName := "demo"
	graph := &ResolvedGraph{
		Nodes: map[string]string{
			"input":  "media:document;ext=pdf",
			"middle": "media:txt;enc=utf-8",
			"output": "media:embedding;record",
		},
		Edges: []*ResolvedEdge{
			{From: "input", To: "middle", CapUrn: extractCap.Urn.String(), Cap: extractCap, InMedia: "media:document;ext=pdf", OutMedia: "media:txt;enc=utf-8"},
			{From: "input", To: "middle", CapUrn: extractCap.Urn.String(), Cap: extractCap, InMedia: "media:document;ext=pdf", OutMedia: "media:txt;enc=utf-8"},
			{From: "middle", To: "output", CapUrn: embedCap.Urn.String(), Cap: embedCap, InMedia: "media:txt;enc=utf-8", OutMedia: "media:embedding;record"},
		},
		GraphName: &graphName,
	}

	mermaid := graph.ToMermaid()

	if !strings.HasPrefix(mermaid, "graph LR\n") {
		t.Fatalf("expected graph LR prefix, got: %s", mermaid)
	}
	if !strings.Contains(mermaid, `input(["input<br/><small>media:document;ext=pdf</small>"])`) {
		t.Fatalf("expected input node shape, got: %s", mermaid)
	}
	if !strings.Contains(mermaid, `middle["middle<br/><small>media:txt;enc=utf-8</small>"]`) {
		t.Fatalf("expected middle node shape, got: %s", mermaid)
	}
	if !strings.Contains(mermaid, `output(("output<br/><small>media:embedding;record</small>"))`) {
		t.Fatalf("expected output node shape, got: %s", mermaid)
	}
	if !strings.Contains(mermaid, `Extract #quot;Title#quot; &lt;One&gt;\\path`) {
		t.Fatalf("expected escaped label, got: %s", mermaid)
	}
	if strings.Count(mermaid, "input -->|") != 1 {
		t.Fatalf("expected deduplicated edge, got: %s", mermaid)
	}
}

// TEST1161: Converting a simple linear plan produces resolved edges for the cap-to-cap chain.
func Test1161_simple_linear_chain_conversion(t *testing.T) {
	registry := buildTestRegistry(t, []string{
		"cap:in=\"media:document;ext=pdf\";extract;out=media:text",
		"cap:in=media:text;summarize;out=media:summary",
	})

	plan := planner.NewMachinePlan("test_chain")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:document;ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", "cap:in=\"media:document;ext=pdf\";extract;out=media:text"))
	plan.AddNode(planner.NewMachineNode("cap_1", "cap:in=media:text;summarize;out=media:summary"))
	plan.AddNode(planner.NewOutputNode("output", "result", "cap_1"))
	plan.AddEdge(planner.NewDirectEdge("input", "cap_0"))
	plan.AddEdge(planner.NewDirectEdge("cap_0", "cap_1"))
	plan.AddEdge(planner.NewDirectEdge("cap_1", "output"))

	graph, err := PlanToResolvedGraph(plan, registry)
	if err != nil {
		t.Fatalf("plan conversion failed: %v", err)
	}
	if graph.GraphName == nil || *graph.GraphName != "test_chain" {
		t.Fatalf("unexpected graph name: %+v", graph.GraphName)
	}
	if graph.Nodes["input"] != "media:document;ext=pdf" || graph.Nodes["cap_0"] != "media:text" || graph.Nodes["cap_1"] != "media:summary" {
		t.Fatalf("unexpected nodes: %#v", graph.Nodes)
	}
	if len(graph.Edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(graph.Edges))
	}
	if graph.Edges[0].From != "input" || graph.Edges[0].To != "cap_0" {
		t.Fatalf("unexpected first edge: %+v", graph.Edges[0])
	}
	if graph.Edges[1].From != "cap_0" || graph.Edges[1].To != "cap_1" {
		t.Fatalf("unexpected second edge: %+v", graph.Edges[1])
	}
}

// TEST770: plan_to_resolved_graph rejects plans containing ForEach nodes
func Test770_rejects_foreach(t *testing.T) {
	registry := buildTestRegistry(t, []string{
		"cap:in=media:pdf;disbind;out=media:pdf-page",
		"cap:in=media:pdf-page;process;out=media:text",
	})

	plan := planner.NewMachinePlan("foreach_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:document;ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", "cap:in=media:pdf;disbind;out=media:pdf-page"))
	plan.AddNode(planner.NewForEachNode("foreach_0", "cap_0", "cap_1", "cap_1"))
	plan.AddNode(planner.NewMachineNode("cap_1", "cap:in=media:pdf-page;process;out=media:text"))
	plan.AddNode(planner.NewOutputNode("output", "result", "cap_1"))

	plan.AddEdge(planner.NewDirectEdge("input", "cap_0"))
	plan.AddEdge(planner.NewDirectEdge("cap_0", "foreach_0"))
	plan.AddEdge(planner.NewIterationEdge("foreach_0", "cap_1"))
	plan.AddEdge(planner.NewDirectEdge("cap_1", "output"))

	_, err := PlanToResolvedGraph(plan, registry)
	if err == nil {
		t.Fatal("Expected error for plan with ForEach node, got nil")
	}
	if !strings.Contains(err.Error(), "ForEach node") {
		t.Fatalf("Expected ForEach rejection, got: %v", err)
	}
	if !strings.Contains(err.Error(), "Decompose") {
		t.Fatalf("Expected mention of decomposition, got: %v", err)
	}
}

// TEST771: plan_to_resolved_graph rejects plans containing Collect nodes
func Test771_rejects_collect(t *testing.T) {
	registry := buildTestRegistry(t, []string{
		`cap:in="media:document;ext=pdf";disbind;out=media:pdf-page`,
		"cap:in=media:pdf-page;process;out=media:text",
	})

	plan := planner.NewMachinePlan("collect_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:document;ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", `cap:in="media:document;ext=pdf";disbind;out=media:pdf-page`))
	plan.AddNode(planner.NewForEachNode("foreach_0", "cap_0", "cap_1", "cap_1"))
	plan.AddNode(planner.NewMachineNode("cap_1", "cap:in=media:pdf-page;process;out=media:text"))
	plan.AddNode(planner.NewCollectNode("collect_0", []string{"cap_1"}))
	plan.AddNode(planner.NewOutputNode("output", "result", "collect_0"))

	plan.AddEdge(planner.NewDirectEdge("input", "cap_0"))
	plan.AddEdge(planner.NewDirectEdge("cap_0", "foreach_0"))
	plan.AddEdge(planner.NewIterationEdge("foreach_0", "cap_1"))
	plan.AddEdge(planner.NewCollectionEdge("cap_1", "collect_0"))
	plan.AddEdge(planner.NewDirectEdge("collect_0", "output"))

	_, err := PlanToResolvedGraph(plan, registry)
	if err == nil {
		t.Fatal("Expected error for plan with Collect node, got nil")
	}
	// Could hit either ForEach or Collect first depending on map iteration order
	if !strings.Contains(err.Error(), "ForEach node") && !strings.Contains(err.Error(), "Collect node") {
		t.Fatalf("Expected ForEach or Collect rejection, got: %v", err)
	}
}

// TEST953: Linear plans (no ForEach/Collect) still convert successfully
func Test953_linear_plan_still_works(t *testing.T) {
	registry := buildTestRegistry(t, []string{"cap:in=\"media:document;ext=pdf\";extract;out=media:text"})

	plan := planner.NewMachinePlan("linear_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:document;ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", "cap:in=\"media:document;ext=pdf\";extract;out=media:text"))
	plan.AddNode(planner.NewOutputNode("output", "result", "cap_0"))
	plan.AddEdge(planner.NewDirectEdge("input", "cap_0"))
	plan.AddEdge(planner.NewDirectEdge("cap_0", "output"))

	graph, err := PlanToResolvedGraph(plan, registry)
	if err != nil {
		t.Fatalf("Linear plan should still convert: %v", err)
	}
	if len(graph.Edges) != 1 {
		t.Fatalf("Expected 1 edge, got %d", len(graph.Edges))
	}
}

// TEST954: Standalone Collect nodes are handled as pass-through Plan: input → cap_0 → Collect → cap_1 → output The standalone Collect is transparent — the resolved edge from Collect to cap_1 should be rewritten to go from cap_0 to cap_1 directly.
func Test954_standalone_collect_passthrough(t *testing.T) {
	registry := buildTestRegistry(t, []string{
		`cap:in="media:document;ext=pdf";extract;out="media:text;enc=utf-8"`,
		`cap:in="media:list;text;enc=utf-8";embed;out="media:embedding-vector;record;enc=utf-8"`,
	})

	plan := planner.NewMachinePlan("collect_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:document;ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", `cap:in="media:document;ext=pdf";extract;out="media:text;enc=utf-8"`))

	// Standalone Collect: scalar→list with OutputMediaUrn set
	collectNode := planner.NewCollectNode("collect_0", []string{"cap_0"})
	outUrn := "media:list;text;enc=utf-8"
	collectNode.NodeType.OutputMediaUrn = &outUrn
	plan.AddNode(collectNode)

	plan.AddNode(planner.NewMachineNode("cap_1", `cap:in="media:list;text;enc=utf-8";embed;out="media:embedding-vector;record;enc=utf-8"`))
	plan.AddNode(planner.NewOutputNode("output", "result", "cap_1"))

	plan.AddEdge(planner.NewDirectEdge("input", "cap_0"))
	plan.AddEdge(planner.NewDirectEdge("cap_0", "collect_0"))
	plan.AddEdge(planner.NewDirectEdge("collect_0", "cap_1"))
	plan.AddEdge(planner.NewDirectEdge("cap_1", "output"))

	graph, err := PlanToResolvedGraph(plan, registry)
	if err != nil {
		t.Fatalf("Plan with standalone Collect should convert: %v", err)
	}

	// Two resolved edges: input→cap_0 and cap_0→cap_1 (Collect is transparent)
	if len(graph.Edges) != 2 {
		pairs := make([]string, len(graph.Edges))
		for i, e := range graph.Edges {
			pairs[i] = e.From + "→" + e.To
		}
		t.Fatalf("Expected 2 edges, got %d: %v", len(graph.Edges), pairs)
	}

	found := make(map[string]bool)
	for _, e := range graph.Edges {
		found[e.From+"→"+e.To] = true
	}
	if !found["input→cap_0"] {
		t.Errorf("Expected input→cap_0 edge, got: %v", found)
	}
}

// TEST6649: PlanToResolvedGraph rejects plans containing ForEach-paired Collect nodes
// Verifies that Collect nodes without OutputMediaUrn (ForEach-paired) are rejected
func Test6649_rejects_foreach_paired_collect(t *testing.T) {
	registry := buildTestRegistry(t, []string{
		"cap:in=media:pdf;disbind;out=media:pdf-page",
		"cap:in=media:pdf-page;process;out=media:text",
	})

	plan := planner.NewMachinePlan("collect_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:document;ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", "cap:in=media:pdf;disbind;out=media:pdf-page"))
	plan.AddNode(planner.NewForEachNode("foreach_0", "cap_0", "cap_1", "cap_1"))
	plan.AddNode(planner.NewMachineNode("cap_1", "cap:in=media:pdf-page;process;out=media:text"))
	plan.AddNode(planner.NewCollectNode("collect_0", []string{"cap_1"}))
	plan.AddNode(planner.NewOutputNode("output", "result", "collect_0"))

	plan.AddEdge(planner.NewDirectEdge("input", "cap_0"))
	plan.AddEdge(planner.NewDirectEdge("cap_0", "foreach_0"))
	plan.AddEdge(planner.NewIterationEdge("foreach_0", "cap_1"))
	plan.AddEdge(planner.NewCollectionEdge("cap_1", "collect_0"))
	plan.AddEdge(planner.NewDirectEdge("collect_0", "output"))

	_, err := PlanToResolvedGraph(plan, registry)
	if err == nil {
		t.Fatal("Expected error for plan with ForEach+Collect nodes, got nil")
	}
	// ForEach node is encountered first in typical iteration — but either rejection is valid
	if !strings.Contains(err.Error(), "ForEach node") && !strings.Contains(err.Error(), "Collect node") {
		t.Fatalf("Expected ForEach or Collect rejection, got: %v", err)
	}
}
