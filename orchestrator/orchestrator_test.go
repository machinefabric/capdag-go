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
	c := cap.NewCap(parsed, title, []string{"test-command"})
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
	c := cap.NewCapWithArgs(parsed, title, []string{"test-command"}, []cap.CapArg{stdinArg})
	c.Output = &cap.CapOutput{
		MediaUrn:          parsed.OutSpec(),
		OutputDescription: title + " output",
	}
	return c
}

// buildParserTestRegistry creates a registry with caps that have stdin args (for machine parser tests).
func buildParserTestRegistry(t *testing.T, capUrns []string) *cap.FabricRegistry {
	t.Helper()
	registry := cap.NewFabricRegistryForTest()
	caps := make([]*cap.Cap, 0, len(capUrns))
	for index, capUrn := range capUrns {
		caps = append(caps, buildTestCapWithStdin(t, capUrn, "Test Cap "+string(rune('0'+index))))
	}
	registry.AddCapsToCache(caps)
	return registry
}

func buildTestRegistry(t *testing.T, capUrns []string) *cap.FabricRegistry {
	t.Helper()
	registry := cap.NewFabricRegistryForTest()
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
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
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
			"input":  "media:ext=pdf",
			"middle": "media:txt;enc=utf-8",
			"output": "media:embedding;record",
		},
		Edges: []*ResolvedEdge{
			{From: "input", To: "middle", CapUrn: extractCap.Urn.String(), Cap: extractCap, InMedia: "media:ext=pdf", OutMedia: "media:txt;enc=utf-8"},
			{From: "input", To: "middle", CapUrn: extractCap.Urn.String(), Cap: extractCap, InMedia: "media:ext=pdf", OutMedia: "media:txt;enc=utf-8"},
			{From: "middle", To: "output", CapUrn: embedCap.Urn.String(), Cap: embedCap, InMedia: "media:txt;enc=utf-8", OutMedia: "media:embedding;record"},
		},
		GraphName: &graphName,
	}

	mermaid := graph.ToMermaid()

	if !strings.HasPrefix(mermaid, "graph LR\n") {
		t.Fatalf("expected graph LR prefix, got: %s", mermaid)
	}
	if !strings.Contains(mermaid, `input(["input<br/><small>media:ext=pdf</small>"])`) {
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
		"cap:in=\"media:ext=pdf\";extract;out=media:text",
		"cap:in=media:text;summarize;out=media:summary",
	})

	plan := planner.NewMachinePlan("test_chain")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", "cap:in=\"media:ext=pdf\";extract;out=media:text"))
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
	if graph.Nodes["input"] != "media:ext=pdf" || graph.Nodes["cap_0"] != "media:text" || graph.Nodes["cap_1"] != "media:summary" {
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
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:ext=pdf", planner.CardinalitySingle))
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
		`cap:in="media:ext=pdf";disbind;out=media:pdf-page`,
		"cap:in=media:pdf-page;process;out=media:text",
	})

	plan := planner.NewMachinePlan("collect_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", `cap:in="media:ext=pdf";disbind;out=media:pdf-page`))
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
	registry := buildTestRegistry(t, []string{"cap:in=\"media:ext=pdf\";extract;out=media:text"})

	plan := planner.NewMachinePlan("linear_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", "cap:in=\"media:ext=pdf\";extract;out=media:text"))
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
		`cap:in="media:ext=pdf";extract;out="media:text;enc=utf-8"`,
		`cap:in="media:list;text;enc=utf-8";embed;out="media:embedding-vector;record;enc=utf-8"`,
	})

	plan := planner.NewMachinePlan("collect_plan")
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:ext=pdf", planner.CardinalitySingle))
	plan.AddNode(planner.NewMachineNode("cap_0", `cap:in="media:ext=pdf";extract;out="media:text;enc=utf-8"`))

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

// TEST1256: A single declared cap and one wiring parse into a two-node one-edge DAG.
func Test1256_parse_simple_machine(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
	})

	notation := `[extract cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"][A -> extract -> B]`

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
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
		`cap:in="media:txt;enc=utf-8";embed;out="media:embedding-vector;record;enc=utf-8"`,
	})

	notation := `[extract cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"]` +
		`[embed cap:in="media:txt;enc=utf-8";embed;out="media:embedding-vector;record;enc=utf-8"]` +
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

// TEST1258: One source node can fan out into multiple caps and target nodes.
func Test1258_parse_fan_out(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:ext=pdf";extract-metadata;out="media:enc=utf-8;file-metadata;record"`,
		`cap:in="media:ext=pdf";extract-outline;out="media:document-outline;enc=utf-8;record"`,
		`cap:in="media:ext=pdf";generate-thumbnail;out="media:ext=png;image;thumbnail"`,
	})

	notation := `[meta cap:in="media:ext=pdf";extract-metadata;out="media:enc=utf-8;file-metadata;record"]` +
		`[outline cap:in="media:ext=pdf";extract-outline;out="media:document-outline;enc=utf-8;record"]` +
		`[thumb cap:in="media:ext=pdf";generate-thumbnail;out="media:ext=png;image;thumbnail"]` +
		`[doc -> meta -> metadata]` +
		`[doc -> outline -> outline_data]` +
		`[doc -> thumb -> thumbnail]`

	graph, err := ParseMachineToCapDag(notation, registry)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(graph.Nodes) != 4 { // doc + 3 targets
		t.Fatalf("Expected 4 nodes, got %d: %v", len(graph.Nodes), graph.Nodes)
	}
	if len(graph.Edges) != 3 {
		t.Fatalf("Expected 3 edges, got %d", len(graph.Edges))
	}
}

// TEST1259: Fan-in wiring resolves multiple upstream outputs into one multi-arg cap.
func Test1259_parse_fan_in(t *testing.T) {
	// The describe cap has TWO input args: image;png (the primary, declared
	// in= spec) and enc=utf-8;model-spec (a secondary fan-in input). The
	// resolver's matching assigns each source URN to the right arg slot.
	registry := cap.NewFabricRegistryForTest()
	thumb := buildTestCapWithStdin(t, `cap:in="media:ext=pdf";generate-thumbnail;out="media:ext=png;image;thumbnail"`, "thumb")
	modelDl := buildTestCapWithStdin(t, `cap:in="media:enc=utf-8;model-spec";download;out="media:enc=utf-8;model-spec"`, "download")
	// describe: two args (image;png and model-spec), each with a stdin source.
	describeUrn, err := urn.NewCapUrnFromString(`cap:in="media:ext=png;image";describe-image;out="media:enc=utf-8;image-description"`)
	if err != nil {
		t.Fatalf("parse describe urn: %v", err)
	}
	imgArgUrn := "media:ext=png;image"
	specArgUrn := "media:enc=utf-8;model-spec"
	describe := cap.NewCapWithArgs(describeUrn, "describe", []string{"test-command"}, []cap.CapArg{
		cap.NewCapArg(imgArgUrn, true, []cap.ArgSource{{Stdin: &imgArgUrn}}),
		cap.NewCapArg(specArgUrn, true, []cap.ArgSource{{Stdin: &specArgUrn}}),
	})
	describe.Output = &cap.CapOutput{MediaUrn: describeUrn.OutSpec(), OutputDescription: "describe output"}
	registry.AddCapsToCache([]*cap.Cap{thumb, modelDl, describe})

	notation := `[thumb cap:in="media:ext=pdf";generate-thumbnail;out="media:ext=png;image;thumbnail"]` +
		`[model_dl cap:in="media:enc=utf-8;model-spec";download;out="media:enc=utf-8;model-spec"]` +
		`[describe cap:in="media:ext=png;image";describe-image;out="media:enc=utf-8;image-description"]` +
		`[doc -> thumb -> thumbnail]` +
		`[spec_input -> model_dl -> model_spec]` +
		`[(thumbnail, model_spec) -> describe -> description]`

	graph, err := ParseMachineToCapDag(notation, registry)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	// Fan-in produces 2 resolved edges for the describe cap (one per source)
	// plus 2 edges for thumb and model_dl = 4 total.
	if len(graph.Edges) != 4 {
		t.Fatalf("Expected 4 edges, got %d", len(graph.Edges))
	}
}

// TEST1260: The LOOP keyword is retired from the grammar. A keyword-free wiring
// parses to a single edge; the old LOOP form no longer parses. ForEach is never
// authored — it is derived from cardinality in the resolver/realizer.
func Test1260_loop_keyword_retired(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:disbound-page;enc=utf-8";page-to-text;out="media:enc=utf-8;ext=txt"`,
	})

	header := `[p2t cap:in="media:disbound-page;enc=utf-8";page-to-text;out="media:enc=utf-8;ext=txt"]`

	// Keyword-free wiring parses to one edge.
	graph, err := ParseMachineToCapDag(header+`[pages -> p2t -> texts]`, registry)
	if err != nil {
		t.Fatalf("keyword-free wiring must parse: %v", err)
	}
	if len(graph.Edges) != 1 {
		t.Fatalf("Expected 1 edge, got %d", len(graph.Edges))
	}
	if len(graph.Nodes) != 2 {
		t.Fatalf("Expected 2 nodes, got %d: %v", len(graph.Nodes), graph.Nodes)
	}

	// The retired LOOP keyword must not parse as a valid wiring.
	if _, err := ParseMachineToCapDag(header+`[pages -> LOOP p2t -> texts]`, registry); err == nil {
		t.Fatal("the retired LOOP keyword must not parse as a valid wiring")
	}
}

// TEST1261: Parsing fails when a declared cap is absent from the registry.
// In Go the machine parser resolves caps before the orchestrator layer checks,
// so the error may be ErrMachineSyntaxParseFailed or ErrCapNotFound.
func Test1261_cap_not_found_in_registry(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{})

	notation := `[ex cap:in="media:unknown";test;out="media:unknown"][A -> ex -> B]`
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
		`cap:in="media:txt;enc=utf-8";process;out="media:txt;enc=utf-8"`,
	})

	notation := `[proc cap:in="media:txt;enc=utf-8";process;out="media:txt;enc=utf-8"]` +
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
		`cap:in="media:void";produce-pdf;out="media:ext=pdf"`,
		`cap:in="media:audio;ext=wav";transcribe;out="media:txt;enc=utf-8"`,
	})

	notation := `[produce cap:in="media:void";produce-pdf;out="media:ext=pdf"]` +
		`[transcribe cap:in="media:audio;ext=wav";transcribe;out="media:txt;enc=utf-8"]` +
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
		`cap:in="media:ext=pdf";thumbnail;out="media:ext=png;image"`,
		`cap:in="media:bytes;ext=png;image";embed-image;out="media:embedding-vector;record;enc=utf-8"`,
	})

	notation := `[thumb cap:in="media:ext=pdf";thumbnail;out="media:ext=png;image"]` +
		`[embed_image cap:in="media:bytes;ext=png;image";embed-image;out="media:embedding-vector;record;enc=utf-8"]` +
		`[A -> thumb -> B]` +
		`[B -> embed_image -> C]`

	_, err := ParseMachineToCapDag(notation, registry)
	if err != nil {
		t.Fatalf("Compatible media URNs should not conflict: %v", err)
	}
}

// TEST1266: Record-to-opaque structure mismatches are rejected once structure checking is enabled.
//
// Skipped, mirroring Rust's #[ignore = "structure mismatch detection between
// node media and cap input not yet implemented"]. The orchestrator keys node
// media on the strand's interned node URN (the resolver's source-to-arg
// assignment), so a single node carries one media URN: the produce edge's
// record output and the process edge's source both resolve to that same
// interned URN, and there is no separate opaque cap-input URN left to compare
// against. Detecting a record-vs-opaque mismatch between a node's media and a
// downstream cap's declared input requires comparing the node URN to the cap's
// in= spec — a check the resolver-based design does not yet perform.
func Test1266_structure_mismatch_record_to_opaque(t *testing.T) {
	t.Skip("structure mismatch detection between node media and cap input not yet implemented (parity with Rust test1266 #[ignore])")
	// Cap A outputs record (media:fmt=json;record); cap B inputs opaque
	// (media:fmt=json, no record). The machine parser's lexical IsComparable
	// passes (both on the fmt=json chain), so the orchestrator's
	// structure-compatibility check is what catches the mismatch.
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:void";produce;out="media:fmt=json;record"`,
		`cap:in="media:fmt=json";process;out="media:txt;enc=utf-8"`,
	})

	notation := `[produce cap:in="media:void";produce;out="media:fmt=json;record"]` +
		`[process cap:in="media:fmt=json";process;out="media:txt;enc=utf-8"]` +
		`[A -> produce -> B]` +
		`[B -> process -> C]`

	_, err := ParseMachineToCapDag(notation, registry)
	if err == nil {
		t.Fatal("Record to opaque structure mismatch must be detected, got nil")
	}
	orchErr, ok := err.(*ParseOrchestrationError)
	if !ok {
		t.Fatalf("Expected *ParseOrchestrationError, got: %T %v", err, err)
	}
	if orchErr.Kind != ErrStructureMismatch {
		t.Fatalf("Expected ErrStructureMismatch, got: %v (%v)", orchErr.Kind, err)
	}
}

// TEST1267: Record-shaped outputs can feed record-shaped inputs without error.
func Test1267_structure_match_both_record(t *testing.T) {
	registry := buildParserTestRegistry(t, []string{
		`cap:in="media:void";produce;out="media:fmt=json;record"`,
		`cap:in="media:fmt=json;record";transform;out="media:result;record;enc=utf-8"`,
	})

	notation := `[produce cap:in="media:void";produce;out="media:fmt=json;record"]` +
		`[transform cap:in="media:fmt=json;record";transform;out="media:result;record;enc=utf-8"]` +
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
		`cap:in="media:void";produce;out="media:fmt=json"`,
		`cap:in="media:fmt=json";format;out="media:txt;enc=utf-8"`,
	})

	notation := `[produce cap:in="media:void";produce;out="media:fmt=json"]` +
		`[format cap:in="media:fmt=json";format;out="media:txt;enc=utf-8"]` +
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
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
	})

	notation := `
[extract cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"]
[doc -> extract -> text]
`

	_, err := ParseMachineToCapDag(notation, registry)
	if err != nil {
		t.Fatalf("Multi-line parse failed: %v", err)
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
	plan.AddNode(planner.NewInputSlotNode("input", "input", "media:ext=pdf", planner.CardinalitySingle))
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

// =============================================================================
// Orchestrator integration ports — parse-only tests from
// tests/orchestrator_integration.rs. The execution-engine tests (execute_dag +
// testcartridge binary) are deferred and not ported here.
// =============================================================================

// createTestFabricRegistry builds a registry pre-loaded with the testcartridge
// synthetic caps the orchestrator integration parse tests depend on. Each cap
// carries a single stdin arg = its in_spec, matching the Rust helper
// build_testcartridge_cap so the resolver's source-to-arg matching works.
func createTestFabricRegistry(t *testing.T) *cap.FabricRegistry {
	t.Helper()
	return buildParserTestRegistry(t, []string{
		`cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2"`,
		`cap:in="media:enc=utf-8;node2";test-edge2;out="media:enc=utf-8;node3"`,
		`cap:in="media:enc=utf-8;node3";test-edge3;out="media:enc=utf-8;list;node4"`,
		`cap:in="media:enc=utf-8;list;node4";test-edge4;out="media:enc=utf-8;node5"`,
		`cap:in="media:enc=utf-8;node3";test-edge7;out="media:enc=utf-8;node6"`,
		`cap:in="media:enc=utf-8;node6";test-edge8;out="media:enc=utf-8;node7"`,
		`cap:in="media:enc=utf-8;node7";test-edge9;out="media:enc=utf-8;node8"`,
		`cap:in="media:enc=utf-8;node8";test-edge10;out="media:enc=utf-8;node1"`,
		`cap:in="media:void";test-large;out="media:"`,
		`cap:in="media:enc=utf-8;node1";test-peer;out="media:enc=utf-8;node3"`,
		`cap:in="media:enc=utf-8;node1";identity;out="media:enc=utf-8;node1"`,
	})
}

// TEST919: Parse simple machine notation graph with test-edge1
func Test919_parse_simple_testcartridge_graph(t *testing.T) {
	registry := createTestFabricRegistry(t)

	route := `
[test_edge1 cap:in="media:enc=utf-8;node1";test-edge1;out="media:enc=utf-8;node2"]
[A -> test_edge1 -> B]
`

	graph, err := ParseMachineToCapDag(route, registry)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if len(graph.Nodes) != 2 {
		t.Fatalf("Expected 2 nodes, got %d: %v", len(graph.Nodes), graph.Nodes)
	}
	if len(graph.Edges) != 1 {
		t.Fatalf("Expected 1 edge, got %d", len(graph.Edges))
	}

	nodeA, err := urn.NewMediaUrnFromString(graph.Nodes["A"])
	if err != nil {
		t.Fatalf("parse node A media: %v", err)
	}
	expectedA, _ := urn.NewMediaUrnFromString("media:enc=utf-8;node1")
	if !nodeA.IsEquivalent(expectedA) {
		t.Errorf("Node A: expected media:enc=utf-8;node1, got %s", graph.Nodes["A"])
	}
	nodeB, err := urn.NewMediaUrnFromString(graph.Nodes["B"])
	if err != nil {
		t.Fatalf("parse node B media: %v", err)
	}
	expectedB, _ := urn.NewMediaUrnFromString("media:enc=utf-8;node2")
	if !nodeB.IsEquivalent(expectedB) {
		t.Errorf("Node B: expected media:enc=utf-8;node2, got %s", graph.Nodes["B"])
	}
}

// TEST950: Validate that cycles are rejected
func Test950_reject_cycles(t *testing.T) {
	registry := createTestFabricRegistry(t)

	// Create a self-loop using identity cap
	route := `
[identity cap:in="media:enc=utf-8;node1";identity;out="media:enc=utf-8;node1"]
[A -> identity -> A]
`

	_, err := ParseMachineToCapDag(route, registry)
	if err == nil {
		t.Fatal("Should reject cycle")
	}
	orchErr, ok := err.(*ParseOrchestrationError)
	if !ok {
		t.Fatalf("Expected *ParseOrchestrationError, got: %T %v", err, err)
	}
	// In Go a cycle may be rejected at the machine-parser layer or the
	// orchestrator layer.
	if orchErr.Kind != ErrNotADag && orchErr.Kind != ErrMachineSyntaxParseFailed {
		t.Errorf("Expected NotADag (or MachineSyntaxParseFailed), got: %v", orchErr.Kind)
	}
}

// TEST949: Empty machine notation (no edges)
func Test949_empty_graph(t *testing.T) {
	registry := createTestFabricRegistry(t)

	_, err := ParseMachineToCapDag("", registry)
	if err == nil {
		t.Fatal("Should fail on empty machine notation")
	}
	orchErr, ok := err.(*ParseOrchestrationError)
	if !ok {
		t.Fatalf("Expected *ParseOrchestrationError, got: %T %v", err, err)
	}
	if orchErr.Kind != ErrMachineSyntaxParseFailed {
		t.Errorf("Expected MachineSyntaxParseFailed, got: %v", orchErr.Kind)
	}
}

// TEST948: Invalid cap URN in machine notation
func Test948_invalid_cap_urn(t *testing.T) {
	registry := createTestFabricRegistry(t)

	route := `[bad cap:INVALID][A -> bad -> B]`

	_, err := ParseMachineToCapDag(route, registry)
	if err == nil {
		t.Fatal("Should reject invalid cap URN")
	}
}

// TEST947: Cap not found in registry
func Test947_cap_not_found(t *testing.T) {
	registry := createTestFabricRegistry(t)

	route := `
[nonexistent cap:in="media:unknown";nonexistent;out="media:unknown"]
[A -> nonexistent -> B]
`

	_, err := ParseMachineToCapDag(route, registry)
	if err == nil {
		t.Fatal("Should fail when cap not found")
	}
	orchErr, ok := err.(*ParseOrchestrationError)
	if !ok {
		t.Fatalf("Expected *ParseOrchestrationError, got: %T %v", err, err)
	}
	// The parser resolves header caps and wraps lookup failure as a machine
	// syntax parse failure; ErrCapNotFound is also acceptable in Go.
	if orchErr.Kind != ErrMachineSyntaxParseFailed && orchErr.Kind != ErrCapNotFound {
		t.Errorf("Expected MachineSyntaxParseFailed or CapNotFound, got: %v", orchErr.Kind)
	}
}
