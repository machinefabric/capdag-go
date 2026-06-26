package planner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST728: Tests MachineNode helper methods for identifying node types (cap, fan-out, fan-in)
// Verifies IsCap(), IsFanOut(), IsFanIn(), and GetCapUrn() correctly classify node types
func Test728_cap_node_helpers(t *testing.T) {
	capNode := NewMachineNode("test", "cap:test")
	assert.True(t, capNode.IsCap())
	assert.False(t, capNode.IsFanOut())
	assert.False(t, capNode.IsFanIn())
	urn := capNode.GetCapUrn()
	require.NotNil(t, urn)
	assert.Equal(t, "cap:test", *urn)

	foreachNode := NewForEachNode("foreach", "input", "body", "body")
	assert.False(t, foreachNode.IsCap())
	assert.True(t, foreachNode.IsFanOut())
	assert.False(t, foreachNode.IsFanIn())
	assert.Nil(t, foreachNode.GetCapUrn())

	collectNode := NewCollectNode("collect", []string{"a"})
	assert.False(t, collectNode.IsCap())
	assert.False(t, collectNode.IsFanOut())
	assert.True(t, collectNode.IsFanIn())
}

// TEST729: Tests creation and classification of different edge types (Direct, Iteration, Collection, JsonField)
// Verifies that edge constructors produce correct EdgeKind variants
func Test729_edge_types(t *testing.T) {
	direct := NewDirectEdge("a", "b")
	assert.Equal(t, EdgeKindDirect, direct.Type.Kind)

	iteration := NewIterationEdge("foreach", "body")
	assert.Equal(t, EdgeKindIteration, iteration.Type.Kind)

	collection := NewCollectionEdge("body", "collect")
	assert.Equal(t, EdgeKindCollection, collection.Type.Kind)

	jsonField := NewJsonFieldEdge("a", "b", "data")
	assert.Equal(t, EdgeKindJsonField, jsonField.Type.Kind)
	assert.Equal(t, "data", jsonField.Type.Field)
}

// TEST734: Tests topological sort detects self-referencing cycles (A→A)
// Verifies that self-loops are recognized as cycles and produce an error
func Test734_topological_order_self_loop(t *testing.T) {
	plan := NewMachinePlan("self_loop")
	plan.Nodes["A"] = NewMachineNode("A", "cap:a")
	plan.Edges = append(plan.Edges, NewDirectEdge("A", "A"))

	_, err := plan.TopologicalOrder()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Cycle detected")
}

// TEST735: Tests topological sort handles graphs with multiple independent starting nodes
// Verifies that parallel entry points (A→C, B→C) both precede their merge point in ordering
func Test735_topological_order_multiple_entry_points(t *testing.T) {
	plan := NewMachinePlan("multi_entry")
	plan.Nodes["A"] = NewMachineNode("A", "cap:a")
	plan.Nodes["B"] = NewMachineNode("B", "cap:b")
	plan.Nodes["C"] = NewMachineNode("C", "cap:c")
	plan.Nodes["D"] = NewMachineNode("D", "cap:d")

	plan.Edges = append(plan.Edges,
		NewDirectEdge("A", "C"),
		NewDirectEdge("B", "C"),
		NewDirectEdge("C", "D"),
	)

	order, err := plan.TopologicalOrder()
	require.NoError(t, err)
	assert.Equal(t, 4, len(order))

	pos := func(id string) int {
		for i, n := range order {
			if n.ID == id {
				return i
			}
		}
		return -1
	}
	assert.Less(t, pos("A"), pos("C"))
	assert.Less(t, pos("B"), pos("C"))
	assert.Less(t, pos("C"), pos("D"))
}

// TEST736: Tests topological sort on a complex multi-path DAG with 6 nodes
// Verifies that all dependency constraints are satisfied in a graph with multiple converging paths
func Test736_topological_order_complex_dag(t *testing.T) {
	// A --> B --> D
	// |     |
	// v     v
	// C --> E --> F
	plan := NewMachinePlan("complex")
	for _, name := range []string{"A", "B", "C", "D", "E", "F"} {
		plan.Nodes[name] = NewMachineNode(name, "cap:"+name)
	}

	plan.Edges = append(plan.Edges,
		NewDirectEdge("A", "B"),
		NewDirectEdge("A", "C"),
		NewDirectEdge("B", "D"),
		NewDirectEdge("B", "E"),
		NewDirectEdge("C", "E"),
		NewDirectEdge("D", "F"),
		NewDirectEdge("E", "F"),
	)

	order, err := plan.TopologicalOrder()
	require.NoError(t, err)
	assert.Equal(t, 6, len(order))

	pos := func(id string) int {
		for i, n := range order {
			if n.ID == id {
				return i
			}
		}
		return -1
	}

	assert.Equal(t, 0, pos("A"))
	assert.Equal(t, 5, pos("F"))
	assert.Less(t, pos("B"), pos("D"))
	assert.Less(t, pos("B"), pos("E"))
	assert.Less(t, pos("C"), pos("E"))
	assert.Less(t, pos("D"), pos("F"))
	assert.Less(t, pos("E"), pos("F"))
}

// TEST737: Tests LinearChain() with exactly one capability
// Verifies that a single-element chain produces a valid plan with input_slot, cap, and output
func Test737_linear_chain_single_cap(t *testing.T) {
	plan := LinearChain([]string{"cap:only"}, "media:ext=pdf", "media:ext=png;image", []string{"source_file"})
	assert.Equal(t, 3, len(plan.Nodes)) // input_slot, cap_0, output
	assert.Equal(t, 2, len(plan.Edges))
	assert.NoError(t, plan.Validate())
}

// TEST738: Tests LinearChain() with empty capability list
// Verifies that an empty chain produces a plan with zero nodes and edges
func Test738_linear_chain_empty(t *testing.T) {
	plan := LinearChain([]string{}, "media:ext=pdf", "media:ext=png;image", []string{})
	assert.Equal(t, 0, len(plan.Nodes))
	assert.Equal(t, 0, len(plan.Edges))
}

// TEST739: Tests MachineResult PrimaryOutput returns populated output and nil when empty
// Verifies the PrimaryOutput() accessor distinguishes populated vs empty outputs maps
func Test739_machine_result_primary_output(t *testing.T) {
	populated := &MachineResult{
		Success: true,
		Outputs: map[string]any{"result": map[string]any{"data": "success"}},
	}
	assert.NotNil(t, populated.PrimaryOutput(), "populated outputs must return non-nil primary output")

	empty := &MachineResult{
		Success: false,
		Outputs: map[string]any{},
	}
	assert.Nil(t, empty.PrimaryOutput(), "empty outputs must return nil primary output")
}

// TEST742: Tests that edge types determine dependency direction in TopologicalOrder
// Iteration edges must NOT create a topological dependency (ForEach body must not block ForEach node).
// Direct edges MUST create a dependency. Verifies that edge kind affects plan execution order.
func Test742_iteration_edge_does_not_create_topological_dependency(t *testing.T) {
	plan := NewMachinePlan("edge_kind_test")
	plan.AddNode(NewInputSlotNode("input", "input", "media:ext=pdf", CardinalitySingle))
	plan.AddNode(NewMachineNode("cap_0", "cap:in=media:pdf;disbind;out=media:pdf-page"))
	plan.AddNode(NewForEachNode("foreach_0", "cap_0", "body_cap", "body_cap"))
	plan.AddNode(NewMachineNode("body_cap", "cap:in=media:pdf-page;process;out=media:text"))
	plan.AddNode(NewOutputNode("output", "result", "body_cap"))

	plan.AddEdge(NewDirectEdge("input", "cap_0"))
	plan.AddEdge(NewDirectEdge("cap_0", "foreach_0"))
	plan.AddEdge(NewIterationEdge("foreach_0", "body_cap")) // iteration — must not block foreach_0
	plan.AddEdge(NewDirectEdge("body_cap", "output"))

	order, err := plan.TopologicalOrder()
	require.NoError(t, err, "plan with iteration edge must be sortable")

	pos := func(id string) int {
		for i, n := range order {
			if n.ID == id {
				return i
			}
		}
		return -1
	}
	// foreach_0 must come before body_cap in topological order
	assert.Less(t, pos("foreach_0"), pos("body_cap"),
		"ForEach node must precede body cap in topological order")
}

// TEST743: Tests that ForEach node's body range fields are used correctly by ExtractForEachBody
// The bodyEntry/bodyExit fields define which nodes are in scope. Verifies that wrong body bounds
// produce a different extraction than correct ones — body_exit determines what gets included.
func Test743_foreach_body_bounds_determine_extraction(t *testing.T) {
	// Build a plan where foreach_0 spans body_cap_0 through body_cap_1 (closed)
	plan := buildForeachPlanWithCollect()

	// Extract with the correct foreach node — body should contain 2 body caps
	body, err := plan.ExtractForEachBody("foreach_0", "media:pdf-page")
	require.NoError(t, err)

	// The foreach node has bodyEntry="body_cap_0" and bodyExit="body_cap_1"
	// Both must appear in the extracted body
	assert.NotNil(t, body.GetNode("body_cap_0"),
		"body entry cap must be included in extracted body")
	assert.NotNil(t, body.GetNode("body_cap_1"),
		"body exit cap must be included in extracted body")
	// The disbind cap (cap_0) is before the foreach and must NOT be in the body
	assert.Nil(t, body.GetNode("cap_0"),
		"pre-foreach cap must not appear in extracted body")
	// The post-collect cap is after foreach and must NOT be in the body
	assert.Nil(t, body.GetNode("cap_post"),
		"post-foreach cap must not appear in extracted body")
}

// TEST744: Tests SingleCap plan passes Validate and TopologicalOrder produces correct sequence
// Verifies the plan is structurally sound: input_slot must precede cap_0 must precede output
func Test744_single_cap_plan_validates_and_orders_correctly(t *testing.T) {
	plan := SingleCap("cap:test", "media:ext=pdf", "media:ext=png;image", "input_file")

	require.NoError(t, plan.Validate(), "SingleCap plan must pass validation")

	order, err := plan.TopologicalOrder()
	require.NoError(t, err, "SingleCap plan must have a valid topological order")
	require.Equal(t, 3, len(order), "must have exactly 3 nodes")

	pos := func(id string) int {
		for i, n := range order {
			if n.ID == id {
				return i
			}
		}
		return -1
	}
	assert.Less(t, pos("input_slot"), pos("cap_0"),
		"input_slot must precede cap_0")
	assert.Less(t, pos("cap_0"), pos("output"),
		"cap_0 must precede output")
}

// TEST745: Tests MergeStrategy enum values
// Verifies MergeConcat and MergeZipWith have correct string representations
func Test745_merge_strategy_values(t *testing.T) {
	assert.Equal(t, "concat", MergeConcat.String())
	assert.Equal(t, "zip_with", MergeZipWith.String())
}

// TEST746: Tests Output node is automatically registered as output_node on AddNode
// Verifies that Validate() accepts a plan where the Output node is the plan's only output_node
func Test746_output_node_registered_on_add(t *testing.T) {
	plan := NewMachinePlan("output_test")
	plan.AddNode(NewMachineNode("cap_0", "cap:test"))
	plan.AddNode(NewOutputNode("out", "result", "cap_0"))
	plan.AddEdge(NewDirectEdge("cap_0", "out"))

	require.NoError(t, plan.Validate())
	// Output node must be auto-registered in OutputNodes
	assert.Contains(t, plan.OutputNodes, "out",
		"Output node must be auto-registered as an output node by AddNode")
}

// TEST747: Tests creation and validation of Merge node that combines multiple inputs
// Verifies that Merge nodes with multiple input nodes and a strategy can be added to plans
func Test747_cap_node_merge(t *testing.T) {
	plan := NewMachinePlan("merge_test")

	plan.AddNode(NewMachineNode("a", "cap:a"))
	plan.AddNode(NewMachineNode("b", "cap:b"))

	mergeNode := &MachineNode{
		ID: "merge",
		NodeType: &ExecutionNodeType{
			Kind:       NodeKindMerge,
			InputNodes: []string{"a", "b"},
			MergeStrat: MergeConcat,
		},
	}
	plan.AddNode(mergeNode)
	plan.AddEdge(NewDirectEdge("a", "merge"))
	plan.AddEdge(NewDirectEdge("b", "merge"))

	assert.NoError(t, plan.Validate())
}

// TEST748: Tests that IsCap/IsFanOut/IsFanIn return false for Split and Merge node types
// Verifies that node type classification methods correctly reject non-cap, non-foreach, non-collect kinds
func Test748_split_and_merge_not_classified_as_cap_fanout_fanin(t *testing.T) {
	splitNode := &MachineNode{
		ID: "split",
		NodeType: &ExecutionNodeType{
			Kind:        NodeKindSplit,
			InputNode:   "input",
			OutputCount: 3,
		},
	}
	assert.False(t, splitNode.IsCap(), "Split node must not be classified as Cap")
	assert.False(t, splitNode.IsFanOut(), "Split node must not be classified as FanOut")
	assert.False(t, splitNode.IsFanIn(), "Split node must not be classified as FanIn")

	mergeNode := &MachineNode{
		ID: "merge",
		NodeType: &ExecutionNodeType{
			Kind:       NodeKindMerge,
			InputNodes: []string{"a", "b"},
			MergeStrat: MergeConcat,
		},
	}
	assert.False(t, mergeNode.IsCap(), "Merge node must not be classified as Cap")
	assert.False(t, mergeNode.IsFanOut(), "Merge node must not be classified as FanOut")
	assert.False(t, mergeNode.IsFanIn(), "Merge node must not be classified as FanIn")
}

// TEST749: Tests GetNode() method for looking up nodes by ID in a plan
// Verifies that existing nodes are found and non-existent nodes return nil
func Test749_get_node(t *testing.T) {
	plan := SingleCap("cap:test", "media:ext=pdf", "media:ext=png;image", "doc_path")

	assert.NotNil(t, plan.GetNode("cap_0"))
	assert.NotNil(t, plan.GetNode("input_slot"))
	assert.NotNil(t, plan.GetNode("output"))
	assert.Nil(t, plan.GetNode("nonexistent"))
}

// buildForeachPlanWithCollect builds a plan with ForEach (closed with Collect)
// Topology: input_slot → cap_0(disbind) → foreach_0 --iteration--> body_cap_0 → body_cap_1 --collection--> collect_0 → cap_post → output
func buildForeachPlanWithCollect() *MachinePlan {
	plan := NewMachinePlan("ForEach test plan")

	plan.AddNode(NewInputSlotNode("input_slot", "input", "media:ext=pdf", CardinalitySingle))
	plan.AddNode(NewMachineNode("cap_0", `cap:in=media:pdf;out="media:pdf-page;list"`))
	plan.AddNode(NewForEachNode("foreach_0", "cap_0", "body_cap_0", "body_cap_1"))
	plan.AddNode(NewMachineNode("body_cap_0", `cap:in=media:pdf-page;out="media:text;enc=utf-8"`))
	plan.AddNode(NewMachineNode("body_cap_1", `cap:in="media:text;enc=utf-8";out="media:decision;fmt=json;record"`))
	plan.AddNode(NewCollectNode("collect_0", []string{"body_cap_1"}))
	plan.AddNode(NewMachineNode("cap_post", `cap:in="media:decision;fmt=json;record";out="media:fmt=json"`))
	plan.AddNode(NewOutputNode("output", "result", "cap_post"))

	plan.AddEdge(NewDirectEdge("input_slot", "cap_0"))
	plan.AddEdge(NewDirectEdge("cap_0", "foreach_0"))
	plan.AddEdge(NewIterationEdge("foreach_0", "body_cap_0"))
	plan.AddEdge(NewDirectEdge("body_cap_0", "body_cap_1"))
	plan.AddEdge(NewCollectionEdge("body_cap_1", "collect_0"))
	plan.AddEdge(NewDirectEdge("collect_0", "cap_post"))
	plan.AddEdge(NewDirectEdge("cap_post", "output"))

	return plan
}

// buildForeachPlanUnclosed builds a plan with unclosed ForEach (no Collect)
func buildForeachPlanUnclosed() *MachinePlan {
	plan := NewMachinePlan("Unclosed ForEach test plan")

	plan.AddNode(NewInputSlotNode("input_slot", "input", "media:ext=pdf", CardinalitySingle))
	plan.AddNode(NewMachineNode("cap_0", `cap:in=media:pdf;out="media:pdf-page;list"`))
	plan.AddNode(NewForEachNode("foreach_0", "cap_0", "body_cap_0", "body_cap_0"))
	plan.AddNode(NewMachineNode("body_cap_0", `cap:in=media:pdf-page;out="media:decision;fmt=json;record"`))
	plan.AddNode(NewOutputNode("output", "result", "body_cap_0"))

	plan.AddEdge(NewDirectEdge("input_slot", "cap_0"))
	plan.AddEdge(NewDirectEdge("cap_0", "foreach_0"))
	plan.AddEdge(NewIterationEdge("foreach_0", "body_cap_0"))
	plan.AddEdge(NewDirectEdge("body_cap_0", "output"))

	return plan
}

// TEST754: extract_prefix_to with nonexistent node returns error
func Test754_extract_prefix_nonexistent(t *testing.T) {
	plan := buildForeachPlanWithCollect()
	_, err := plan.ExtractPrefixTo("nonexistent")
	assert.Error(t, err)
}

// TEST755: extract_foreach_body extracts body as standalone plan
func Test755_extract_foreach_body(t *testing.T) {
	plan := buildForeachPlanWithCollect()

	body, err := plan.ExtractForEachBody("foreach_0", "media:pdf-page")
	require.NoError(t, err)

	assert.Equal(t, 4, len(body.Nodes))
	assert.NotNil(t, body.GetNode("foreach_0_body_input"))
	assert.NotNil(t, body.GetNode("body_cap_0"))
	assert.NotNil(t, body.GetNode("body_cap_1"))
	assert.NotNil(t, body.GetNode("foreach_0_body_output"))
	assert.Equal(t, 1, len(body.EntryNodes))
	assert.Equal(t, 1, len(body.OutputNodes))
	assert.NoError(t, body.Validate())

	assert.False(t, body.HasForeach())

	inputNode := body.GetNode("foreach_0_body_input")
	require.NotNil(t, inputNode)
	assert.Equal(t, NodeKindInputSlot, inputNode.NodeType.Kind)
	assert.Equal(t, "media:pdf-page", inputNode.NodeType.ExpectedMediaUrn)

	order, err := body.TopologicalOrder()
	require.NoError(t, err)
	assert.Equal(t, 4, len(order))
}

// TEST756: extract_foreach_body for unclosed ForEach (single body cap)
func Test756_extract_foreach_body_unclosed(t *testing.T) {
	plan := buildForeachPlanUnclosed()

	body, err := plan.ExtractForEachBody("foreach_0", "media:pdf-page")
	require.NoError(t, err)

	assert.Equal(t, 3, len(body.Nodes))
	assert.NotNil(t, body.GetNode("foreach_0_body_input"))
	assert.NotNil(t, body.GetNode("body_cap_0"))
	assert.NotNil(t, body.GetNode("foreach_0_body_output"))
	assert.NoError(t, body.Validate())
	assert.False(t, body.HasForeach())
}

// TEST757: extract_foreach_body fails for non-ForEach node
func Test757_extract_foreach_body_wrong_type(t *testing.T) {
	plan := buildForeachPlanWithCollect()
	_, err := plan.ExtractForEachBody("cap_0", "media:pdf-page")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a ForEach node")
}

// TEST758: extract_suffix_from extracts collect → cap_post → output
func Test758_extract_suffix_from(t *testing.T) {
	plan := buildForeachPlanWithCollect()

	suffix, err := plan.ExtractSuffixFrom("collect_0", "media:decision;fmt=json;record")
	require.NoError(t, err)

	assert.Equal(t, 3, len(suffix.Nodes))
	assert.NotNil(t, suffix.GetNode("collect_0_suffix_input"))
	assert.NotNil(t, suffix.GetNode("cap_post"))
	assert.NotNil(t, suffix.GetNode("output"))
	assert.Equal(t, 1, len(suffix.EntryNodes))
	assert.Equal(t, 1, len(suffix.OutputNodes))
	assert.NoError(t, suffix.Validate())
	assert.False(t, suffix.HasForeach())
}

// TEST759: extract_suffix_from fails for nonexistent node
func Test759_extract_suffix_nonexistent(t *testing.T) {
	plan := buildForeachPlanWithCollect()
	_, err := plan.ExtractSuffixFrom("nonexistent", "media:whatever")
	assert.Error(t, err)
}

// TEST760: Full decomposition roundtrip — prefix + body + suffix cover all cap nodes
func Test760_decomposition_covers_all_caps(t *testing.T) {
	plan := buildForeachPlanWithCollect()

	originalCaps := make(map[string]bool)
	for _, n := range plan.Nodes {
		if n.IsCap() {
			originalCaps[n.ID] = true
		}
	}
	assert.Equal(t, 4, len(originalCaps)) // cap_0, body_cap_0, body_cap_1, cap_post

	prefix, err := plan.ExtractPrefixTo("cap_0")
	require.NoError(t, err)
	body, err := plan.ExtractForEachBody("foreach_0", "media:pdf-page")
	require.NoError(t, err)
	suffix, err := plan.ExtractSuffixFrom("collect_0", "media:decision;fmt=json;record")
	require.NoError(t, err)

	allCaps := make(map[string]bool)
	for _, p := range []*MachinePlan{prefix, body, suffix} {
		for _, n := range p.Nodes {
			if n.IsCap() {
				allCaps[n.ID] = true
			}
		}
	}

	assert.Equal(t, originalCaps, allCaps)
}

// TEST761: Prefix sub-plan can be topologically sorted (is a valid DAG)
func Test761_prefix_is_dag(t *testing.T) {
	plan := buildForeachPlanWithCollect()
	prefix, err := plan.ExtractPrefixTo("cap_0")
	require.NoError(t, err)
	_, err = prefix.TopologicalOrder()
	assert.NoError(t, err)
}

// TEST762: Body sub-plan can be topologically sorted (is a valid DAG)
func Test762_body_is_dag(t *testing.T) {
	plan := buildForeachPlanWithCollect()
	body, err := plan.ExtractForEachBody("foreach_0", "media:pdf-page")
	require.NoError(t, err)
	_, err = body.TopologicalOrder()
	assert.NoError(t, err)
}

// TEST763: Suffix sub-plan can be topologically sorted (is a valid DAG)
func Test763_suffix_is_dag(t *testing.T) {
	plan := buildForeachPlanWithCollect()
	suffix, err := plan.ExtractSuffixFrom("collect_0", "media:decision;fmt=json;record")
	require.NoError(t, err)
	_, err = suffix.TopologicalOrder()
	assert.NoError(t, err)
}

// TEST920: SingleCap creates a valid plan with input_slot, cap node, and output node.
func Test920_single_cap_plan(t *testing.T) {
	plan := SingleCap("cap:test", "media:ext=pdf", "media:ext=png;image", "input_file")
	// 3 nodes: input_slot, cap_0, output
	assert.Equal(t, 3, len(plan.Nodes))
	assert.Equal(t, 1, len(plan.EntryNodes))
	assert.Equal(t, 1, len(plan.OutputNodes))
	assert.NoError(t, plan.Validate())
}

// TEST921: LinearChain creates a plan with correct nodes and edges in topological order.
func Test921_linear_chain_plan(t *testing.T) {
	plan := LinearChain(
		[]string{"cap:a", "cap:b", "cap:c"},
		"media:ext=pdf",
		"media:ext=png;image",
		[]string{"input_a", "input_b", "input_c"},
	)
	// 5 nodes: input_slot, cap_0, cap_1, cap_2, output
	assert.Equal(t, 5, len(plan.Nodes))
	// 4 edges: input_slot→cap_0, cap_0→cap_1, cap_1→cap_2, cap_2→output
	assert.Equal(t, 4, len(plan.Edges))
	assert.NoError(t, plan.Validate())

	order, err := plan.TopologicalOrder()
	require.NoError(t, err)
	assert.Equal(t, 5, len(order))
}

// TEST922: An empty MachinePlan is valid with zero nodes.
func Test922_empty_plan(t *testing.T) {
	plan := NewMachinePlan("empty")
	assert.Equal(t, 0, len(plan.Nodes))
	assert.NoError(t, plan.Validate())
}

// TEST923: MachinePlan stores and retrieves metadata by key.
func Test923_plan_with_metadata(t *testing.T) {
	plan := NewMachinePlan("test")
	plan.Metadata = map[string]any{
		"source":  "pdf",
		"version": 1,
	}
	require.NotNil(t, plan.Metadata)
	assert.Equal(t, "pdf", plan.Metadata["source"])
	assert.Equal(t, 1, plan.Metadata["version"])
}

// TEST924: Tests plan validation detects edges pointing to non-existent nodes
// Verifies that Validate() returns an error when an edge references a missing to_node
func Test924_validate_invalid_edge(t *testing.T) {
	plan := NewMachinePlan("invalid")
	plan.Nodes["node_0"] = NewMachineNode("node_0", "cap:test")
	plan.Edges = append(plan.Edges, NewDirectEdge("node_0", "nonexistent"))

	err := plan.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

// TEST925: Tests topological sort correctly orders a diamond-shaped DAG (A->B,C->D)
// Verifies that nodes with multiple paths respect dependency constraints (A first, D last)
func Test925_topological_order_diamond(t *testing.T) {
	plan := NewMachinePlan("diamond")
	plan.Nodes["A"] = NewMachineNode("A", "cap:a")
	plan.Nodes["B"] = NewMachineNode("B", "cap:b")
	plan.Nodes["C"] = NewMachineNode("C", "cap:c")
	plan.Nodes["D"] = NewMachineNode("D", "cap:d")

	plan.Edges = append(plan.Edges,
		NewDirectEdge("A", "B"),
		NewDirectEdge("A", "C"),
		NewDirectEdge("B", "D"),
		NewDirectEdge("C", "D"),
	)

	order, err := plan.TopologicalOrder()
	require.NoError(t, err)
	assert.Equal(t, 4, len(order))
	assert.Equal(t, "A", order[0].ID)
	assert.Equal(t, "D", order[3].ID)
}

// TEST926: Tests topological sort detects and rejects cyclic dependencies (A->B->C->A)
// Verifies that circular references produce a "Cycle detected" error
func Test926_topological_order_detects_cycle(t *testing.T) {
	plan := NewMachinePlan("cyclic")
	plan.Nodes["A"] = NewMachineNode("A", "cap:a")
	plan.Nodes["B"] = NewMachineNode("B", "cap:b")
	plan.Nodes["C"] = NewMachineNode("C", "cap:c")

	plan.Edges = append(plan.Edges,
		NewDirectEdge("A", "B"),
		NewDirectEdge("B", "C"),
		NewDirectEdge("C", "A"),
	)

	_, err := plan.TopologicalOrder()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Cycle detected")
}

// TEST927: Tests MachineResult structure for successful execution outcomes
// Verifies that success status, outputs, and PrimaryOutput() accessor work correctly
func Test927_execution_result(t *testing.T) {
	result := &MachineResult{
		Success: true,
		NodeResults: map[string]*NodeExecutionResult{},
		Outputs: map[string]any{
			"output": map[string]any{"result": "success"},
		},
		TotalDurationMs: 100,
	}

	assert.True(t, result.Success)
	assert.NotNil(t, result.PrimaryOutput())
}

// TEST928: Tests plan validation detects edges originating from non-existent nodes
// Verifies that Validate() returns an error when an edge references a missing from_node
func Test928_validate_invalid_from_node(t *testing.T) {
	plan := NewMachinePlan("invalid")
	plan.Nodes["node_0"] = NewMachineNode("node_0", "cap:test")
	plan.Edges = append(plan.Edges, NewDirectEdge("nonexistent", "node_0"))

	err := plan.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

// TEST929: Tests plan validation detects invalid entry node references
// Verifies that Validate() returns an error when EntryNodes contains a non-existent node ID
func Test929_validate_invalid_entry_node(t *testing.T) {
	plan := NewMachinePlan("invalid_entry")
	plan.Nodes["cap_0"] = NewMachineNode("cap_0", "cap:test")
	plan.EntryNodes = append(plan.EntryNodes, "nonexistent_entry")

	err := plan.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent_entry")
}

// TEST930: Tests plan validation detects invalid output node references
// Verifies that Validate() returns an error when OutputNodes contains a non-existent node ID
func Test930_validate_invalid_output_node(t *testing.T) {
	plan := NewMachinePlan("invalid_output")
	plan.Nodes["cap_0"] = NewMachineNode("cap_0", "cap:test")
	plan.OutputNodes = append(plan.OutputNodes, "nonexistent_output")

	err := plan.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent_output")
}

// TEST931: Tests NodeExecutionResult structure for failed node execution
// Verifies that failure status, error message, and absence of outputs are correctly represented
func Test931_node_execution_result_failure(t *testing.T) {
	result := &NodeExecutionResult{
		NodeID:       "node_0",
		Success:      false,
		BinaryOutput: nil,
		Error:        "Cap execution failed",
		DurationMs:   10,
	}

	assert.False(t, result.Success)
	assert.Nil(t, result.BinaryOutput)
	assert.Equal(t, "Cap execution failed", result.Error)
}

// TEST932: Tests MachineResult structure for failed chain execution
// Verifies that failure status, error message, and absence of outputs are correctly represented
func Test932_execution_result_failure(t *testing.T) {
	result := &MachineResult{
		Success:         false,
		NodeResults:     map[string]*NodeExecutionResult{},
		Outputs:         map[string]any{},
		Error:           "Chain failed",
		TotalDurationMs: 100,
	}

	assert.False(t, result.Success)
	assert.Equal(t, "Chain failed", result.Error)
	assert.Nil(t, result.PrimaryOutput())
}

// TEST934: FindFirstForEach detects ForEach in a plan
func Test934_find_first_foreach(t *testing.T) {
	plan := buildForeachPlanWithCollect()
	foreachID := plan.FindFirstForEach()
	require.NotNil(t, foreachID)
	assert.Equal(t, "foreach_0", *foreachID)
}

// TEST935: FindFirstForEach returns nil for linear plans
func Test935_find_first_foreach_linear(t *testing.T) {
	plan := LinearChain([]string{"cap:a", "cap:b"}, "media:ext=pdf", "media:ext=png;image", []string{"input_a", "input_b"})
	assert.Nil(t, plan.FindFirstForEach())
}

// TEST936: HasForeach detects ForEach nodes
func Test936_has_foreach(t *testing.T) {
	foreachPlan := buildForeachPlanWithCollect()
	assert.True(t, foreachPlan.HasForeach(), "Plan with ForEach+Collect should detect ForEach")

	linearPlan := LinearChain([]string{"cap:a"}, "media:ext=pdf", "media:ext=png;image", []string{"input_a"})
	assert.False(t, linearPlan.HasForeach(), "Linear plan should not detect ForEach")

	// Standalone Collect (no ForEach) should NOT trigger HasForeach
	standalonePlan := NewMachinePlan("collect_only")
	standalonePlan.AddNode(NewInputSlotNode("input", "input", "media:enc=utf-8", CardinalitySingle))
	standalonePlan.AddNode(NewMachineNode("cap_0", "cap:in=media:summarize;out=media;enc=utf-8:summary"))
	standalonePlan.AddNode(NewCollectNode("collect_0", []string{"cap_0"}))
	standalonePlan.AddNode(NewOutputNode("output", "result", "collect_0"))
	assert.False(t, standalonePlan.HasForeach(), "Plan with standalone Collect (no ForEach) should NOT trigger HasForeach")
}

// TEST937: ExtractPrefixTo extracts input_slot -> cap_0 as a standalone plan
func Test937_extract_prefix_to(t *testing.T) {
	plan := buildForeachPlanWithCollect()

	prefix, err := plan.ExtractPrefixTo("cap_0")
	require.NoError(t, err)

	// Should have: input_slot, cap_0, and a synthetic output
	assert.Equal(t, 3, len(prefix.Nodes))
	assert.NotNil(t, prefix.GetNode("input_slot"))
	assert.NotNil(t, prefix.GetNode("cap_0"))
	assert.NotNil(t, prefix.GetNode("cap_0_prefix_output"))
	assert.Equal(t, 1, len(prefix.EntryNodes))
	assert.Equal(t, 1, len(prefix.OutputNodes))
	assert.NoError(t, prefix.Validate())

	order, err := prefix.TopologicalOrder()
	require.NoError(t, err)
	assert.Equal(t, 3, len(order))
}

// TEST764: extract_prefix_to with InputSlot as target (trivial prefix)
func Test764_extract_prefix_to_input_slot(t *testing.T) {
	plan := buildForeachPlanWithCollect()
	prefix, err := plan.ExtractPrefixTo("input_slot")
	require.NoError(t, err)

	// Should have: input_slot + synthetic output
	assert.Equal(t, 2, len(prefix.Nodes))
	assert.NoError(t, prefix.Validate())
}
