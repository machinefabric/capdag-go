package orchestrator

import (
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/urn"
)

func buildTestCap(t *testing.T, capUrn string, title string) *cap.Cap {
	t.Helper()
	parsed, err := urn.NewCapUrnFromString(capUrn)
	if err != nil {
		t.Fatalf("parse cap urn: %v", err)
	}
	c := cap.NewCap(parsed, title, "test-command")
	c.Output = &cap.CapOutput{
		MediaUrn:          parsed.OutSpec(),
		OutputDescription: title + " output",
	}
	return c
}

// buildTestCapWithStdin creates a test cap that includes a stdin arg for its in_spec.
// This is required for ParseMachineToCapDag which validates source URN conforms to cap args.
func buildTestCapWithStdin(t *testing.T, capUrn string, title string) *cap.Cap {
	t.Helper()
	parsed, err := urn.NewCapUrnFromString(capUrn)
	if err != nil {
		t.Fatalf("parse cap urn: %v", err)
	}
	inSpec := parsed.InSpec()
	stdinArg := cap.NewCapArg(inSpec, true, []cap.ArgSource{{Stdin: &inSpec}})
	c := cap.NewCapWithArgs(parsed, title, "test-command", []cap.CapArg{stdinArg})
	c.Output = &cap.CapOutput{
		MediaUrn:          parsed.OutSpec(),
		OutputDescription: title + " output",
	}
	return c
}

// buildParserTestRegistry creates a registry with caps that have stdin args (for machine parser tests).
func buildParserTestRegistry(t *testing.T, capUrns []string) *cap.CapRegistry {
	t.Helper()
	registry := cap.NewCapRegistryForTest()
	caps := make([]*cap.Cap, 0, len(capUrns))
	for index, capUrn := range capUrns {
		caps = append(caps, buildTestCapWithStdin(t, capUrn, "Test Cap "+string(rune('0'+index))))
	}
	registry.AddCapsToCache(caps)
	return registry
}

func buildTestRegistry(t *testing.T, capUrns []string) *cap.CapRegistry {
	t.Helper()
	registry := cap.NewCapRegistryForTest()
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
		`cap:in="media:pdf";op=extract;out="media:txt;textable"`,
		`Extract "Title" <One>\path`,
	)
	embedCap := buildTestCap(
		t,
		`cap:in="media:txt;textable";op=embed;out="media:embedding;record"`,
		"Embed",
	)

	graphName := "demo"
	graph := &ResolvedGraph{
		Nodes: map[string]string{
			"input":  "media:pdf",
			"middle": "media:txt;textable",
			"output": "media:embedding;record",
		},
		Edges: []*ResolvedEdge{
			{From: "input", To: "middle", CapUrn: extractCap.Urn.String(), Cap: extractCap, InMedia: "media:pdf", OutMedia: "media:txt;textable"},
			{From: "input", To: "middle", CapUrn: extractCap.Urn.String(), Cap: extractCap, InMedia: "media:pdf", OutMedia: "media:txt;textable"},
			{From: "middle", To: "output", CapUrn: embedCap.Urn.String(), Cap: embedCap, InMedia: "media:txt;textable", OutMedia: "media:embedding;record"},
		},
		GraphName: &graphName,
	}

	mermaid := graph.ToMermaid()

	if !strings.HasPrefix(mermaid, "graph LR\n") {
		t.Fatalf("expected graph LR prefix, got: %s", mermaid)
	}
	if !strings.Contains(mermaid, `input(["input<br/><small>media:pdf</small>"])`) {
		t.Fatalf("expected input node shape, got: %s", mermaid)
	}
	if !strings.Contains(mermaid, `middle["middle<br/><small>media:txt;textable</small>"]`) {
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
		"cap:in=media:pdf;op=extract;out=media:text",
		"cap:in=media:text;op=summarize;out=media:summary",
	})

	plan := planner.NewMachinePlan("test_chain")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", "cap:in=media:pdf;op=extract;out=media:text"))
	plan.AddNode(planner.NewMachineNode("cap_1", "cap:in=media:text;op=summarize;out=media:summary"))
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
	if graph.Nodes["input"] != "media:pdf" || graph.Nodes["cap_0"] != "media:text" || graph.Nodes["cap_1"] != "media:summary" {
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

// TEST770: PlanToResolvedGraph rejects plans containing ForEach nodes
// Verifies that plans requiring decomposition (ForEach) are rejected before conversion
func Test770_rejects_foreach(t *testing.T) {
	registry := buildTestRegistry(t, []string{
		"cap:in=media:pdf;op=disbind;out=media:pdf-page",
		"cap:in=media:pdf-page;op=process;out=media:text",
	})

	plan := planner.NewMachinePlan("foreach_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", "cap:in=media:pdf;op=disbind;out=media:pdf-page"))
	plan.AddNode(planner.NewForEachNode("foreach_0", "cap_0", "cap_1", "cap_1"))
	plan.AddNode(planner.NewMachineNode("cap_1", "cap:in=media:pdf-page;op=process;out=media:text"))
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

// TEST953: Linear plans (no ForEach/Collect) still convert successfully
func Test953_linear_plan_still_works(t *testing.T) {
	registry := buildTestRegistry(t, []string{"cap:in=media:pdf;op=extract;out=media:text"})

	plan := planner.NewMachinePlan("linear_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", "cap:in=media:pdf;op=extract;out=media:text"))
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

// TEST954: Standalone Collect nodes are handled as pass-through
// Plan: input → cap_0 → Collect → cap_1 → output
// The standalone Collect is transparent — the resolved edge from Collect to cap_1
// should be rewritten to go from cap_0 to cap_1 directly.
func Test954_standalone_collect_passthrough(t *testing.T) {
	registry := buildTestRegistry(t, []string{
		`cap:in=media:pdf;op=extract;out="media:text;textable"`,
		`cap:in="media:list;text;textable";op=embed;out="media:embedding-vector;record;textable"`,
	})

	plan := planner.NewMachinePlan("collect_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", `cap:in=media:pdf;op=extract;out="media:text;textable"`))

	// Standalone Collect: scalar→list with OutputMediaUrn set
	collectNode := planner.NewCollectNode("collect_0", []string{"cap_0"})
	outUrn := "media:list;text;textable"
	collectNode.NodeType.OutputMediaUrn = &outUrn
	plan.AddNode(collectNode)

	plan.AddNode(planner.NewMachineNode("cap_1", `cap:in="media:list;text;textable";op=embed;out="media:embedding-vector;record;textable"`))
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

// TEST1256: A single declared cap and one wiring parse into a two-node one-edge DAG.
func Test1256_parse_simple_machine(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:pdf";op=extract;out="media:txt;textable"`,
	})

	notation := `[extract cap:in="media:pdf";op=extract;out="media:txt;textable"][A -> extract -> B]`

	graph, err := ParseMachineToCapDag(notation, registry)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(graph.Nodes) != 2 {
		t.Fatalf("Expected 2 nodes, got %d: %v", len(graph.Nodes), graph.Nodes)
	}
	if len(graph.Edges) != 1 {
		t.Fatalf("Expected 1 edge, got %d", len(graph.Edges))
	}
	if _, ok := graph.Nodes["A"]; !ok {
		t.Errorf("Expected node A, got: %v", graph.Nodes)
	}
	if _, ok := graph.Nodes["B"]; !ok {
		t.Errorf("Expected node B, got: %v", graph.Nodes)
	}
}

// TEST1257: Two sequential wirings preserve the intermediate node media type.
func Test1257_parse_two_step_chain(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:pdf";op=extract;out="media:txt;textable"`,
		`cap:in="media:txt;textable";op=embed;out="media:embedding-vector;record;textable"`,
	})

	notation := `[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]` +
		`[embed cap:in="media:txt;textable";op=embed;out="media:embedding-vector;record;textable"]` +
		`[A -> extract -> B]` +
		`[B -> embed -> C]`

	graph, err := ParseMachineToCapDag(notation, registry)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(graph.Nodes) != 3 {
		t.Fatalf("Expected 3 nodes, got %d: %v", len(graph.Nodes), graph.Nodes)
	}
	if len(graph.Edges) != 2 {
		t.Fatalf("Expected 2 edges, got %d", len(graph.Edges))
	}
	// Intermediate node B must have the text media type
	nodeB, ok := graph.Nodes["B"]
	if !ok {
		t.Fatal("Expected node B")
	}
	if !strings.Contains(nodeB, "txt") {
		t.Errorf("Expected node B to be text media, got: %s", nodeB)
	}
}

// TEST1261: Parsing fails when a declared cap is absent from the registry.
// In Go the machine parser resolves caps before the orchestrator layer checks,
// so the error may be ErrMachineSyntaxParseFailed or ErrCapNotFound.
func Test1261_cap_not_found_in_registry(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{})

	notation := `[ex cap:in="media:unknown";op=test;out="media:unknown"][A -> ex -> B]`
	_, err := ParseMachineToCapDag(notation, registry)
	if err == nil {
		t.Fatal("Expected error for cap not in registry, got nil")
	}
	orchErr, ok := err.(*ParseOrchestrationError)
	if !ok {
		t.Fatalf("Expected *ParseOrchestrationError, got: %T %v", err, err)
	}
	if orchErr.Kind != ErrCapNotFound && orchErr.Kind != ErrMachineSyntaxParseFailed {
		t.Errorf("Expected ErrCapNotFound or ErrMachineSyntaxParseFailed, got: %v", orchErr.Kind)
	}
}

// TEST1262: Non-machine text fails with a machine syntax parse error.
func Test1262_invalid_machine_notation(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{})
	_, err := ParseMachineToCapDag("not valid", registry)
	if err == nil {
		t.Fatal("Expected error for invalid notation, got nil")
	}
	orchErr, ok := err.(*ParseOrchestrationError)
	if !ok {
		t.Fatalf("Expected *ParseOrchestrationError, got: %T %v", err, err)
	}
	if orchErr.Kind != ErrMachineSyntaxParseFailed {
		t.Errorf("Expected ErrMachineSyntaxParseFailed, got: %v", orchErr.Kind)
	}
}

// TEST1263: Cyclic wirings are rejected as non-DAG orchestrations.
// In Go the machine parser may reject cycles at the parse layer or the orchestrator layer.
func Test1263_cycle_detection(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:txt;textable";op=process;out="media:txt;textable"`,
	})

	notation := `[proc cap:in="media:txt;textable";op=process;out="media:txt;textable"]` +
		`[A -> proc -> B]` +
		`[B -> proc -> C]` +
		`[C -> proc -> A]`

	_, err := ParseMachineToCapDag(notation, registry)
	if err == nil {
		t.Fatal("Expected error for cyclic graph, got nil")
	}
	orchErr, ok := err.(*ParseOrchestrationError)
	if !ok {
		t.Fatalf("Expected *ParseOrchestrationError, got: %T %v", err, err)
	}
	if orchErr.Kind != ErrNotADag && orchErr.Kind != ErrMachineSyntaxParseFailed {
		t.Errorf("Expected ErrNotADag or ErrMachineSyntaxParseFailed, got: %v", orchErr.Kind)
	}
}

// TEST1264: Shared nodes with incompatible upstream and downstream media fail during parsing.
func Test1264_incompatible_media_types_at_shared_node(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:void";op=produce_pdf;out="media:pdf"`,
		`cap:in="media:audio;wav";op=transcribe;out="media:txt;textable"`,
	})

	notation := `[produce cap:in="media:void";op=produce_pdf;out="media:pdf"]` +
		`[transcribe cap:in="media:audio;wav";op=transcribe;out="media:txt;textable"]` +
		`[A -> produce -> B]` +
		`[B -> transcribe -> C]`

	_, err := ParseMachineToCapDag(notation, registry)
	if err == nil {
		t.Fatal("Expected error for incompatible media at shared node, got nil")
	}
	// Error should be a parse failure (media type conflict)
	if _, ok := err.(*ParseOrchestrationError); !ok {
		t.Fatalf("Expected *ParseOrchestrationError, got: %T %v", err, err)
	}
}

// TEST1265: Shared nodes accept compatible media URNs when one is a more specific form of the other.
func Test1265_compatible_media_urns_at_shared_node(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:pdf";op=thumbnail;out="media:image;png"`,
		`cap:in="media:image;png;bytes";op=embed_image;out="media:embedding-vector;record;textable"`,
	})

	notation := `[thumb cap:in="media:pdf";op=thumbnail;out="media:image;png"]` +
		`[embed_image cap:in="media:image;png;bytes";op=embed_image;out="media:embedding-vector;record;textable"]` +
		`[A -> thumb -> B]` +
		`[B -> embed_image -> C]`

	_, err := ParseMachineToCapDag(notation, registry)
	if err != nil {
		t.Fatalf("Compatible media URNs should not conflict: %v", err)
	}
}

// TEST1267: Record-shaped outputs can feed record-shaped inputs without error.
func Test1267_structure_match_both_record(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:void";op=produce;out="media:json;record;textable"`,
		`cap:in="media:json;record;textable";op=transform;out="media:result;record;textable"`,
	})

	notation := `[produce cap:in="media:void";op=produce;out="media:json;record;textable"]` +
		`[transform cap:in="media:json;record;textable";op=transform;out="media:result;record;textable"]` +
		`[A -> produce -> B]` +
		`[B -> transform -> C]`

	_, err := ParseMachineToCapDag(notation, registry)
	if err != nil {
		t.Fatalf("Record to record should be accepted: %v", err)
	}
}

// TEST1268: Opaque outputs can feed opaque inputs without triggering structure conflicts.
func Test1268_structure_match_both_opaque(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:void";op=produce;out="media:json;textable"`,
		`cap:in="media:json;textable";op=format;out="media:txt;textable"`,
	})

	notation := `[produce cap:in="media:void";op=produce;out="media:json;textable"]` +
		`[format cap:in="media:json;textable";op=format;out="media:txt;textable"]` +
		`[A -> produce -> B]` +
		`[B -> format -> C]`

	_, err := ParseMachineToCapDag(notation, registry)
	if err != nil {
		t.Fatalf("Opaque to opaque should be accepted: %v", err)
	}
}

// TEST1269: Multi-line machine notation parses successfully with the same semantics as inline notation.
func Test1269_parse_multiline_machine(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:pdf";op=extract;out="media:txt;textable"`,
	})

	notation := `
[extract cap:in="media:pdf";op=extract;out="media:txt;textable"]
[doc -> extract -> text]
`

	_, err := ParseMachineToCapDag(notation, registry)
	if err != nil {
		t.Fatalf("Multi-line parse failed: %v", err)
	}
}

// TEST771: PlanToResolvedGraph rejects plans containing ForEach-paired Collect nodes
// Verifies that Collect nodes without OutputMediaUrn (ForEach-paired) are rejected
func Test771_rejects_foreach_paired_collect(t *testing.T) {
	registry := buildTestRegistry(t, []string{
		"cap:in=media:pdf;op=disbind;out=media:pdf-page",
		"cap:in=media:pdf-page;op=process;out=media:text",
	})

	plan := planner.NewMachinePlan("collect_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", "cap:in=media:pdf;op=disbind;out=media:pdf-page"))
	plan.AddNode(planner.NewForEachNode("foreach_0", "cap_0", "cap_1", "cap_1"))
	plan.AddNode(planner.NewMachineNode("cap_1", "cap:in=media:pdf-page;op=process;out=media:text"))
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
