package planner

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST728: Tests MachineNode helper methods for identifying node types (cap, fan-out, fan-in) Verifies is_cap(), is_fan_out(), is_fan_in(), and cap_urn() correctly classify node types
func Test728_cap_node_helpers(t *testing.T) {
	capNode := NewMachineNode("test", "cap:test")
	assert.True(t, capNode.IsCap())
	assert.False(t, capNode.IsFanOut())
	assert.False(t, capNode.IsFanIn())
	urn := capNode.GetCapUrn()
	require.NotNil(t, urn)
	assert.Equal(t, "cap:test", *urn)

	foreachNode := NewForEachNode("foreach",
		[]ForEachSource{{Node: "input", Item: "item"}},
		[]NodeId{"item", "body"})
	assert.False(t, foreachNode.IsCap())
	assert.True(t, foreachNode.IsFanOut())
	assert.False(t, foreachNode.IsFanIn())
	assert.Nil(t, foreachNode.GetCapUrn())

	collectNode := NewCollectNode("collect", []string{"a"}, "media:")
	assert.False(t, collectNode.IsCap())
	assert.False(t, collectNode.IsFanOut())
	assert.True(t, collectNode.IsFanIn())
}

// TEST729: Tests creation and classification of different edge types (Direct, Iteration, Collection, Arg) Verifies that edge constructors produce correct EdgeType variants
func Test729_edge_types(t *testing.T) {
	direct := NewDirectEdge("a", "b")
	assert.Equal(t, EdgeKindDirect, direct.Type.Kind)

	iteration := NewIterationEdge("foreach", "body")
	assert.Equal(t, EdgeKindIteration, iteration.Type.Kind)

	collection := NewCollectionEdge("body", "collect")
	assert.Equal(t, EdgeKindCollection, collection.Type.Kind)

	arg := NewArgEdge("a", "b", "media:data")
	assert.Equal(t, EdgeKindArg, arg.Type.Kind)
	assert.Equal(t, "media:data", arg.Type.ArgUrn)
	assert.Equal(t, "", direct.Type.ArgUrn, "only an arg edge names an argument")
}

// TEST734: Tests topological sort detects self-referencing cycles (A→A) Verifies that self-loops are recognized as cycles and produce an error
func Test734_topological_order_self_loop(t *testing.T) {
	plan := NewMachinePlan("self_loop")
	plan.Nodes["A"] = NewMachineNode("A", "cap:a")
	plan.Edges = append(plan.Edges, NewDirectEdge("A", "A"))

	_, err := plan.TopologicalOrder()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Cycle detected")
}

// TEST735: Tests topological sort handles graphs with multiple independent starting nodes Verifies that parallel entry points (A→C, B→C) both precede their merge point in ordering
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

// TEST736: Tests topological sort on a complex multi-path DAG with 6 nodes Verifies that all dependency constraints are satisfied in a graph with multiple converging paths
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

// TEST737: Tests linear_chain() with exactly one capability Verifies that a single-element chain produces a valid plan with input_slot, cap, and output
func Test737_linear_chain_single_cap(t *testing.T) {
	plan := LinearChain([]string{"cap:only"}, "media:document;ext=pdf", "media:ext=png;image", []string{"source_file"})
	assert.Equal(t, 3, len(plan.Nodes)) // input_slot, cap_0, output
	assert.Equal(t, 2, len(plan.Edges))
	assert.NoError(t, plan.Validate())
}

// TEST738: Tests linear_chain() with empty capability list Verifies that an empty chain produces a plan with zero nodes and edges
func Test738_linear_chain_empty(t *testing.T) {
	plan := LinearChain([]string{}, "media:document;ext=pdf", "media:ext=png;image", []string{})
	assert.Equal(t, 0, len(plan.Nodes))
	assert.Equal(t, 0, len(plan.Edges))
}

// TEST6634: Tests MachineResult PrimaryOutput returns populated output and nil when empty
// Verifies the PrimaryOutput() accessor distinguishes populated vs empty outputs maps
func Test6634_machine_result_primary_output(t *testing.T) {
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

// TEST6635: A map and its body sort in the order a run meets them: the
// sequence, the map, the item the body is given, then the body's step. The
// iteration edge from a map to its item is a dependency like any other.
func Test6635_a_map_precedes_its_item_and_its_body(t *testing.T) {
	plan := NewMachinePlan("edge_kind_test")
	plan.AddNode(NewInputSlotNode("input", "input", "media:document;ext=pdf", CardinalitySingle))
	plan.AddNode(NewMachineNode("cap_0", "cap:in=media:pdf;disbind;out=media:pdf-page"))
	plan.AddNode(NewForEachNode("foreach_0",
		[]ForEachSource{{Node: "cap_0", Item: "page"}},
		[]NodeId{"page", "body_cap"}))
	plan.AddNode(NewItemNode("page", "foreach_0", "cap_0", "media:pdf-page"))
	plan.AddNode(NewMachineNode("body_cap", "cap:in=media:pdf-page;process;out=media:text"))
	plan.AddNode(NewOutputNode("output", "result", "body_cap"))

	plan.AddEdge(NewDirectEdge("input", "cap_0"))
	plan.AddEdge(NewDirectEdge("cap_0", "foreach_0"))
	plan.AddEdge(NewIterationEdge("foreach_0", "page"))
	plan.AddEdge(NewDirectEdge("page", "body_cap"))
	plan.AddEdge(NewDirectEdge("body_cap", "output"))
	require.NoError(t, plan.Validate())

	order, err := plan.TopologicalOrder()
	require.NoError(t, err, "a plan with a map is sortable")

	pos := func(id string) int {
		for i, n := range order {
			if n.ID == id {
				return i
			}
		}
		return -1
	}
	assert.Less(t, pos("cap_0"), pos("foreach_0"), "the sequence precedes the map over it")
	assert.Less(t, pos("foreach_0"), pos("page"), "the map precedes its item")
	assert.Less(t, pos("page"), pos("body_cap"), "the item precedes the step it is given to")
}

// TEST6637: Tests SingleCap plan passes Validate and TopologicalOrder produces correct sequence
// Verifies the plan is structurally sound: input_slot must precede cap_0 must precede output
func Test6637_single_cap_plan_validates_and_orders_correctly(t *testing.T) {
	plan := SingleCap("cap:test", "media:document;ext=pdf", "media:ext=png;image", "input_file")

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

// TEST6639: Tests Output node is automatically registered as output_node on AddNode
// Verifies that Validate() accepts a plan where the Output node is the plan's only output_node
func Test6639_output_node_registered_on_add(t *testing.T) {
	plan := NewMachinePlan("output_test")
	plan.AddNode(NewMachineNode("cap_0", "cap:test"))
	plan.AddNode(NewOutputNode("out", "result", "cap_0"))
	plan.AddEdge(NewDirectEdge("cap_0", "out"))

	require.NoError(t, plan.Validate())
	// Output node must be auto-registered in OutputNodes
	assert.Contains(t, plan.OutputNodes, "out",
		"Output node must be auto-registered as an output node by AddNode")
}

// TEST749: Tests get_node() method for looking up nodes by ID in a plan Verifies that existing nodes are found and non-existent nodes return None
func Test749_get_node(t *testing.T) {
	plan := SingleCap("cap:test", "media:document;ext=pdf", "media:ext=png;image", "doc_path")

	assert.NotNil(t, plan.GetNode("cap_0"))
	assert.NotNil(t, plan.GetNode("input_slot"))
	assert.NotNil(t, plan.GetNode("output"))
	assert.Nil(t, plan.GetNode("nonexistent"))
}

// buildForeachPlan builds a plan with a map and a fold after it:
// input_slot → cap_0 (a sequence) → map over it [ body_cap_0 → body_cap_1 ] → cap_post
// (takes the sequence of body_cap_1 over the runs) → output.
func buildForeachPlan() *MachinePlan {
	plan := NewMachinePlan("ForEach test plan")
	plan.AddNode(NewInputSlotNode("input_slot", "input", "media:document;ext=pdf", CardinalitySingle))
	plan.AddNode(NewMachineNode("cap_0", `cap:in="media:document;ext=pdf";out="media:pdf-page;list"`))
	plan.AddNode(NewForEachNode("foreach_0",
		[]ForEachSource{{Node: "cap_0", Item: "foreach_0_item_0"}},
		[]NodeId{"foreach_0_item_0", "body_cap_0", "body_cap_1"}))
	plan.AddNode(NewItemNode("foreach_0_item_0", "foreach_0", "cap_0", "media:pdf-page"))
	plan.AddNode(NewMachineNode("body_cap_0", `cap:in=media:pdf-page;out="media:enc=utf-8;text"`))
	plan.AddNode(NewMachineNode("body_cap_1", `cap:in="media:enc=utf-8;text";out="media:decision;fmt=json;record"`))
	plan.AddNode(NewMachineNode("cap_post", `cap:in="media:decision;fmt=json;record";out="media:fmt=json"`))
	plan.AddNode(NewOutputNode("output", "result", "cap_post"))

	plan.AddEdge(NewDirectEdge("input_slot", "cap_0"))
	plan.AddEdge(NewDirectEdge("cap_0", "foreach_0"))
	plan.AddEdge(NewIterationEdge("foreach_0", "foreach_0_item_0"))
	plan.AddEdge(NewDirectEdge("foreach_0_item_0", "body_cap_0"))
	plan.AddEdge(NewDirectEdge("body_cap_0", "body_cap_1"))
	plan.AddEdge(NewDirectEdge("body_cap_1", "cap_post"))
	plan.AddEdge(NewDirectEdge("cap_post", "output"))
	return plan
}

// TEST920: Tests creation of a simple execution plan with a single capability Verifies that single_cap() generates a valid plan with input_slot, cap node, and output node
func Test920_single_cap_plan(t *testing.T) {
	plan := SingleCap("cap:test", "media:document;ext=pdf", "media:ext=png;image", "input_file")
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
		"media:document;ext=pdf",
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

// TEST924: Tests plan validation detects edges pointing to non-existent nodes Verifies that validate() returns an error when an edge references a missing to_node
func Test924_validate_invalid_edge(t *testing.T) {
	plan := NewMachinePlan("invalid")
	plan.Nodes["node_0"] = NewMachineNode("node_0", "cap:test")
	plan.Edges = append(plan.Edges, NewDirectEdge("node_0", "nonexistent"))

	err := plan.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

// TEST925: Tests topological sort correctly orders a diamond-shaped DAG (A->B,C->D) Verifies that nodes with multiple paths respect dependency constraints (A first, D last)
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

// TEST926: Tests topological sort detects and rejects cyclic dependencies (A->B->C->A) Verifies that circular references produce a "Cycle detected" error
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

// TEST927: Tests MachineResult structure for successful execution outcomes Verifies that success status, outputs, and primary_output() accessor work correctly
func Test927_execution_result(t *testing.T) {
	result := &MachineResult{
		Success:     true,
		NodeResults: map[string]*NodeExecutionResult{},
		Outputs: map[string]any{
			"output": map[string]any{"result": "success"},
		},
		TotalDurationMs: 100,
	}

	assert.True(t, result.Success)
	assert.NotNil(t, result.PrimaryOutput())
}

// TEST928: Tests plan validation detects edges originating from non-existent nodes Verifies that validate() returns an error when an edge references a missing from_node
func Test928_validate_invalid_from_node(t *testing.T) {
	plan := NewMachinePlan("invalid")
	plan.Nodes["node_0"] = NewMachineNode("node_0", "cap:test")
	plan.Edges = append(plan.Edges, NewDirectEdge("nonexistent", "node_0"))

	err := plan.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

// TEST929: Tests plan validation detects invalid entry node references Verifies that validate() returns an error when entry_nodes contains a non-existent node ID
func Test929_validate_invalid_entry_node(t *testing.T) {
	plan := NewMachinePlan("invalid_entry")
	plan.Nodes["cap_0"] = NewMachineNode("cap_0", "cap:test")
	plan.EntryNodes = append(plan.EntryNodes, "nonexistent_entry")

	err := plan.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent_entry")
}

// TEST930: Tests plan validation detects invalid output node references Verifies that validate() returns an error when output_nodes contains a non-existent node ID
func Test930_validate_invalid_output_node(t *testing.T) {
	plan := NewMachinePlan("invalid_output")
	plan.Nodes["cap_0"] = NewMachineNode("cap_0", "cap:test")
	plan.OutputNodes = append(plan.OutputNodes, "nonexistent_output")

	err := plan.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent_output")
}

// TEST931: Tests NodeExecutionResult structure for failed node execution Verifies that failure status, error message, and absence of outputs are correctly represented
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

// TEST932: Tests MachineResult structure for failed chain execution Verifies that failure status, error message, and absence of outputs are correctly represented
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

// TEST936: has_foreach detects a map, and a gather alone is not one.
func Test936_has_foreach(t *testing.T) {
	foreachPlan := buildForeachPlan()
	require.NoError(t, foreachPlan.Validate(), "the fixture is a valid plan")
	assert.True(t, foreachPlan.HasForeach())

	linearPlan := LinearChain([]string{"cap:a"}, "media:document;ext=pdf", "media:ext=png;image", []string{"input_a"})
	assert.False(t, linearPlan.HasForeach())

	gatherPlan := NewMachinePlan("gather_only")
	gatherPlan.AddNode(NewInputSlotNode("input", "input", "media:enc=utf-8", CardinalitySingle))
	gatherPlan.AddNode(NewMachineNode("cap_0", `cap:in="media:enc=utf-8";summarize;out=media:summary`))
	gatherPlan.AddNode(NewCollectNode("collect_0", []string{"cap_0"}, "media:summary"))
	gatherPlan.AddNode(NewOutputNode("output", "result", "collect_0"))
	require.NoError(t, gatherPlan.Validate())
	assert.False(t, gatherPlan.HasForeach())
}

// TEST12753: a plan says which map each node runs in — the map whose body
// lists it — and which nodes are inside a map at any depth.
func Test12753_a_plan_says_which_map_a_node_runs_in(t *testing.T) {
	plan := buildForeachPlan()
	enclosing := plan.EnclosingMaps()
	assert.Equal(t, "foreach_0", enclosing["body_cap_0"])
	assert.Equal(t, "foreach_0", enclosing["body_cap_1"])
	assert.Equal(t, "foreach_0", enclosing["foreach_0_item_0"])
	for _, outside := range []string{"input_slot", "cap_0", "foreach_0", "cap_post", "output"} {
		_, inside := enclosing[outside]
		assert.False(t, inside, "%s is the machine's own", outside)
	}

	// A map opened inside the body: its nodes are inside the outer map,
	// and the outer body lists the inner map, not the inner body.
	plan.AddNode(NewForEachNode("foreach_1",
		[]ForEachSource{{Node: "body_cap_0", Item: "foreach_1_item_0"}},
		[]NodeId{"foreach_1_item_0", "inner_cap"}))
	plan.AddNode(NewItemNode("foreach_1_item_0", "foreach_1", "body_cap_0", "media:enc=utf-8;text"))
	plan.AddNode(NewMachineNode("inner_cap", `cap:in="media:enc=utf-8;text";out=media:word`))
	outer := plan.Nodes["foreach_0"].NodeType
	outer.Body = append(outer.Body, "foreach_1")
	require.NoError(t, plan.Validate())
	enclosing = plan.EnclosingMaps()
	assert.Equal(t, "foreach_1", enclosing["inner_cap"])
	assert.Equal(t, "foreach_0", enclosing["foreach_1"])
	inside := plan.MapMembers("foreach_0")
	for _, id := range []string{"body_cap_0", "body_cap_1", "foreach_0_item_0", "foreach_1", "foreach_1_item_0", "inner_cap"} {
		assert.True(t, inside[id], "%s is inside the outer map", id)
	}
	assert.False(t, inside["cap_post"])
	assert.False(t, inside["foreach_0"])
	assert.Len(t, plan.MapMembers("foreach_1"), 2)
	assert.Empty(t, plan.MapMembers("cap_0"), "a step is no map")
}

// TEST12754: a plan whose nodes name what is not there, or name it as what
// it is not, is refused: a map that iterates nothing, an item its map does
// not list, a node in two bodies, an output node of something that is not
// a step, a gather of a missing node.
func Test12754_a_plan_that_names_what_is_not_there_is_refused(t *testing.T) {
	refused := func(change func(plan *MachinePlan), expect string) {
		t.Helper()
		plan := buildForeachPlan()
		change(plan)
		err := plan.Validate()
		require.Error(t, err, expect)
		assert.Contains(t, err.Error(), expect)
	}
	theMap := func(plan *MachinePlan) *ExecutionNodeType { return plan.Nodes["foreach_0"].NodeType }
	refused(func(plan *MachinePlan) { theMap(plan).Sources = nil }, "map 'foreach_0' iterates nothing")
	refused(func(plan *MachinePlan) {
		plan.AddNode(NewForEachNode("lonely", nil, nil))
	}, "map 'lonely' iterates nothing")
	refused(func(plan *MachinePlan) { theMap(plan).Sources[0].Item = "body_cap_0" }, "which is not that")
	refused(func(plan *MachinePlan) { theMap(plan).Sources[0].Node = "nowhere" },
		"node 'foreach_0' names source 'nowhere', which is not in the plan")
	refused(func(plan *MachinePlan) { theMap(plan).Sources[0].Item = "nowhere" },
		"node 'foreach_0' names item 'nowhere', which is not in the plan")
	// The item it lists is an item — of another source, or of another map.
	refused(func(plan *MachinePlan) { theMap(plan).Sources[0].Node = "input_slot" },
		"map 'foreach_0' lists 'foreach_0_item_0' as its item of 'input_slot', which is not that")
	refused(func(plan *MachinePlan) {
		plan.AddNode(NewForEachNode("second",
			[]ForEachSource{{Node: "cap_0", Item: "foreach_0_item_0"}},
			[]NodeId{"foreach_0_item_0"}))
	}, "map 'second' lists 'foreach_0_item_0' as its item of 'cap_0', which is not that")
	refused(func(plan *MachinePlan) {
		theMap(plan).Body = []NodeId{"body_cap_0", "body_cap_1"}
	}, "does not list its item 'foreach_0_item_0' in its body")
	refused(func(plan *MachinePlan) {
		plan.AddNode(NewForEachNode("second",
			[]ForEachSource{{Node: "cap_0", Item: "second_item"}},
			[]NodeId{"second_item", "body_cap_1"}))
		plan.AddNode(NewItemNode("second_item", "second", "cap_0", "media:pdf-page"))
	}, "node 'body_cap_1' is in the body of two maps")
	refused(func(plan *MachinePlan) {
		theMap(plan).Body = append(theMap(plan).Body, "foreach_0")
	}, "map 'foreach_0' lists itself in its body")
	refused(func(plan *MachinePlan) {
		plan.AddNode(NewCapOutputNode("o", "input_slot", "media:x", "x", false))
	}, "names 'input_slot' as its step, which is not a step")
	refused(func(plan *MachinePlan) {
		plan.AddNode(NewCapOutputNode("o", "nowhere", "media:x", "x", false))
	}, "node 'o' names step 'nowhere', which is not in the plan")
	refused(func(plan *MachinePlan) {
		plan.AddNode(NewCollectNode("g", []string{"nowhere"}, "media:"))
	}, "node 'g' names gathered node 'nowhere', which is not in the plan")
	refused(func(plan *MachinePlan) {
		plan.AddNode(NewOutputNode("o2", "result", "nowhere"))
	}, "node 'o2' names source 'nowhere', which is not in the plan")
	refused(func(plan *MachinePlan) {
		plan.AddNode(NewItemNode("stray", "foreach_0", "cap_0", "media:pdf-page"))
	}, "item node 'stray' names map 'foreach_0'")
	refused(func(plan *MachinePlan) {
		plan.AddNode(NewItemNode("stray", "cap_0", "input_slot", "media:pdf-page"))
	}, "item node 'stray' names map 'cap_0'")
	refused(func(plan *MachinePlan) {
		theMap(plan).Body = append(theMap(plan).Body, "nowhere")
	}, "node 'foreach_0' names body node 'nowhere', which is not in the plan")

	// A step's other output, of a step: accepted.
	plan := buildForeachPlan()
	plan.AddNode(NewCapOutputNode("o", "cap_0", "media:x", "x", false))
	require.NoError(t, plan.Validate())
}

// TEST739: Tests NodeExecutionResult structure for successful node execution Verifies that success status, outputs (binary and text), and error fields work correctly
func Test739_node_execution_result_success(t *testing.T) {
	result := &NodeExecutionResult{
		NodeID:           "node_0",
		Success:          true,
		BinaryOutput:     []byte{1, 2, 3},
		BinaryItems:      nil,
		SavedPaths:       nil,
		IsSequenceOutput: false,
		TotalBytes:       0,
		MediaUrnOutput:   "",
		Error:            "",
		DurationMs:       50,
	}

	assert.True(t, result.Success)
	assert.NotNil(t, result.BinaryOutput)
	assert.Equal(t, "", result.Error)
}

// TEST742: Tests EdgeType enum serialization and deserialization to/from JSON Verifies that every edge type round-trips by its name, and that a kind this library does not carry is refused
func Test742_edge_type_serialization(t *testing.T) {
	direct := DirectEdgeType()
	data, err := json.Marshal(direct)
	require.NoError(t, err)
	assert.Equal(t, `"direct"`, string(data))

	var deserialized EdgeType
	require.NoError(t, json.Unmarshal(data, &deserialized))
	assert.Equal(t, EdgeKindDirect, deserialized.Kind)

	for name, edgeType := range map[string]*EdgeType{
		`"direct"`: DirectEdgeType(), `"iteration"`: IterationEdgeType(), `"collection"`: CollectionEdgeType(),
		`{"arg":{"arg_urn":"media:data"}}`: ArgEdgeType("media:data"),
	} {
		data, err := json.Marshal(edgeType)
		require.NoError(t, err)
		assert.Equal(t, name, string(data))
		var back EdgeType
		require.NoError(t, json.Unmarshal(data, &back), name)
		assert.Equal(t, *edgeType, back, name)
	}
	// An arg edge that names no argument is not one.
	var unnamed EdgeType
	assert.Error(t, json.Unmarshal([]byte(`{"arg":{}}`), &unnamed))

	// An edge carries the whole value: a part of it is not something an
	// edge can be asked for, in either of the forms that once said so.
	for _, retired := range []string{`{"json_field":{"field":"data"}}`, `{"json_path":{"path":"a.b"}}`, `"json_field"`, `"sideways"`} {
		var refused EdgeType
		assert.Error(t, json.Unmarshal([]byte(retired), &refused), retired)
	}
}

// TEST743: Tests ExecutionNodeType enum serialization and deserialization to/from JSON Verifies that node types like Cap and ForEach correctly serialize with their fields
func Test743_execution_node_type_serialization(t *testing.T) {
	asValue := func(node *ExecutionNodeType) map[string]any {
		t.Helper()
		data, err := json.Marshal(node)
		require.NoError(t, err)
		var value map[string]any
		require.NoError(t, json.Unmarshal(data, &value))
		return value
	}
	back := func(node *ExecutionNodeType) *ExecutionNodeType {
		t.Helper()
		data, err := json.Marshal(node)
		require.NoError(t, err)
		var read ExecutionNodeType
		require.NoError(t, json.Unmarshal(data, &read))
		return &read
	}

	capNode := NewMachineNodeType("cap:test", NewArgumentBindings(), nil, mustStepToken(t, "tok-cap-test"))
	written := asValue(capNode)
	assert.Equal(t, "cap", written["node_type"])
	assert.Equal(t, "cap:test", written["cap_urn"])
	assert.Equal(t, "tok-cap-test", written["token_id"], "a step node carries its identity")
	assert.NotContains(t, written, "policy", "the default policy is not written")
	assert.Equal(t, capNode.TokenId, back(capNode).TokenId)
	// A step that wrote a policy carries it.
	capNode.Policy = StepPolicy{Retry: 2, OnError: OnErrorAbsent}
	assert.Equal(t, map[string]any{"retry": float64(2), "on_error": "absent"}, asValue(capNode)["policy"])
	assert.True(t, back(capNode).Policy.Equal(capNode.Policy))
	// A policy is written whole: retries alone still say what a failure does.
	retrying, err := json.Marshal(StepPolicy{Retry: 1})
	require.NoError(t, err)
	assert.JSONEq(t, `{"retry":1,"on_error":"fail"}`, string(retrying))
	var read StepPolicy
	require.NoError(t, json.Unmarshal([]byte(`{"retry":3}`), &read))
	assert.True(t, read.Equal(StepPolicy{Retry: 3, OnError: OnErrorFail}), "a field left out is its default")

	foreachNode := &ExecutionNodeType{
		Kind:    NodeKindForEach,
		TokenId: mustStepToken(t, "tok-foreach"),
		Sources: []ForEachSource{{Node: "input", Item: "item"}},
		Body:    []NodeId{"item", "body"},
	}
	assert.Equal(t, map[string]any{
		"node_type": "for_each",
		"token_id":  "tok-foreach",
		"sources":   []any{map[string]any{"node": "input", "item": "item"}},
		"body":      []any{"item", "body"},
	}, asValue(foreachNode))
	assert.Equal(t, foreachNode, back(foreachNode))

	outputNode := NewCapOutputNode("o", "tok-cap-test", "media:residuals", "residuals", false).NodeType
	assert.Equal(t, map[string]any{
		"node_type":        "cap_output",
		"cap_node":         "tok-cap-test",
		"output_media_urn": "media:residuals",
		"output_name":      "residuals",
		"carries_input":    false,
	}, asValue(outputNode))
	assert.Equal(t, outputNode, back(outputNode))
	carrying := NewCapOutputNode("o", "route", "media:dropped", "no", true).NodeType
	assert.True(t, back(carrying).CarriesInput)

	itemNode := NewItemNode("item", "each", "input", "media:page").NodeType
	assert.Equal(t, map[string]any{
		"node_type": "item", "foreach": "each", "source": "input", "media_urn": "media:page",
	}, asValue(itemNode))
	assert.Equal(t, itemNode, back(itemNode))

	gather := NewCollectNode("g", []string{"a", "b"}, "media:summary").NodeType
	assert.Equal(t, map[string]any{
		"node_type": "collect", "input_nodes": []any{"a", "b"}, "output_media_urn": "media:summary",
	}, asValue(gather))
	assert.Equal(t, gather, back(gather))

	// A node type nothing produces is not read, nor is a node missing a
	// field its kind has, nor a map in the shape it once had.
	for _, refused := range []string{
		`{"node_type":"merge","input_nodes":["a"],"merge_strategy":"concat"}`,
		`{"node_type":"split","input_node":"a","output_count":2}`,
		`{"node_type":"for_each","input_node":"a","body_entry":"b","body_exit":"b"}`,
		`{"node_type":"for_each","token_id":"t","sources":[{"node":"a","item":"i"}]}`,
		`{"node_type":"collect","input_nodes":["a"]}`,
		`{"node_type":"cap","cap_urn":"cap:test"}`,
		`{"node_type":"cap","token_id":"","cap_urn":"cap:test"}`,
		`{"node_type":"cap","token_id":"t","cap_urn":"cap:test","policy":{"on_error":"shrug"}}`,
		`{"node_type":"item","foreach":"each","source":"input"}`,
		`{"cap_urn":"cap:test"}`,
	} {
		var node ExecutionNodeType
		assert.Error(t, json.Unmarshal([]byte(refused), &node), refused)
	}
}

// TEST744: Tests MachinePlan serialization and deserialization to/from JSON Verifies that complete plans with nodes and edges correctly round-trip through JSON
func Test744_plan_serialization(t *testing.T) {
	plan := SingleCap("cap:test", "media:document;ext=pdf", "media:ext=png;image", "input_file")

	data, err := json.Marshal(plan)
	require.NoError(t, err)
	assert.Contains(t, string(data), "cap:test")
	assert.Contains(t, string(data), "input_slot")
	assert.Contains(t, string(data), "output")

	var deserialized MachinePlan
	require.NoError(t, json.Unmarshal(data, &deserialized))
	assert.Equal(t, len(plan.Nodes), len(deserialized.Nodes))
	assert.Equal(t, len(plan.Edges), len(deserialized.Edges))
}

// TEST746: Tests creation of Output node type that references a source node Verifies that MachineNode::output() correctly constructs an Output node with name and source
func Test746_cap_node_output(t *testing.T) {
	output := NewOutputNode("out", "result", "source")
	require.Equal(t, NodeKindOutput, output.NodeType.Kind)
	assert.Equal(t, "result", output.NodeType.OutputName)
	assert.Equal(t, "source", output.NodeType.SourceNode)
}

// TEST6525: Body outcomes preserve explicit null attribution coordinates and preview fields in JSON.
func Test6525_body_outcome_serialization_preserves_attribution_shape(t *testing.T) {
	outcome := BodyOutcome{BodyIndex: 0, Success: true, CapUrns: []string{}, SavedPaths: []string{}}
	data, err := json.Marshal(outcome)
	require.NoError(t, err)

	var encoded map[string]any
	require.NoError(t, json.Unmarshal(data, &encoded))
	require.Contains(t, encoded, "foreach_token_id")
	assert.Nil(t, encoded["foreach_token_id"])
	require.Contains(t, encoded, "failed_token_id")
	assert.Nil(t, encoded["failed_token_id"])
	require.Contains(t, encoded, "failed_arg_urn")
	assert.Nil(t, encoded["failed_arg_urn"])
	require.Contains(t, encoded, "item_preview_text")
	assert.Nil(t, encoded["item_preview_text"])
	assert.Equal(t, float64(0), encoded["item_byte_count"])

	var decoded BodyOutcome
	require.NoError(t, json.Unmarshal(data, &decoded))
	delete(encoded, "foreach_token_id")
	missing, err := json.Marshal(encoded)
	require.NoError(t, err)
	require.ErrorContains(t, json.Unmarshal(missing, &decoded), "foreach_token_id")

	data, err = json.Marshal(outcome)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &encoded))
	delete(encoded, "failed_token_id")
	missing, err = json.Marshal(encoded)
	require.NoError(t, err)
	require.ErrorContains(t, json.Unmarshal(missing, &decoded), "failed_token_id")

	data, err = json.Marshal(outcome)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &encoded))
	delete(encoded, "failed_arg_urn")
	missing, err = json.Marshal(encoded)
	require.NoError(t, err)
	require.ErrorContains(t, json.Unmarshal(missing, &decoded), "failed_arg_urn")

	nonzeroLinear := []byte(`{"body_index":1,"foreach_token_id":null,"success":true,"cap_urns":[],"failed_token_id":null,"failed_arg_urn":null,"saved_paths":[],"total_bytes":0,"duration_ms":0,"item_preview_text":null,"item_byte_count":0}`)
	require.ErrorContains(t, json.Unmarshal(nonzeroLinear, &decoded), "linear body outcome must use body_index 0")
	_, err = json.Marshal(BodyOutcome{BodyIndex: 1, Success: true})
	require.ErrorContains(t, err, "linear body outcome must use body_index 0")

	// The runs a body is inside are written as a list, empty for a body of
	// the machine's own map; a linear outcome is inside no map.
	assert.Equal(t, []any{}, encoded["enclosing"])
	outer := MapRun{ForEachTokenID: mustStepToken(t, "tok-outer"), BodyIndex: 2}
	_, err = json.Marshal(BodyOutcome{Success: true, Enclosing: []MapRun{outer}})
	require.ErrorContains(t, err, "a linear body outcome is inside no map")
	insideLinear := []byte(`{"body_index":0,"foreach_token_id":null,"enclosing":[{"foreach_token_id":"tok-outer","body_index":2}],"success":true,"cap_urns":[],"failed_token_id":null,"failed_arg_urn":null,"saved_paths":[],"total_bytes":0,"duration_ms":0,"item_preview_text":null,"item_byte_count":0}`)
	require.ErrorContains(t, json.Unmarshal(insideLinear, &decoded), "a linear body outcome is inside no map")

	foreachToken := mustStepToken(t, "tok-foreach")
	foreachOutcome := BodyOutcome{
		ForEachTokenID: &foreachToken,
		BodyIndex:      3,
		Success:        true,
		CapUrns:        []string{},
		SavedPaths:     []string{},
	}
	foreachData, err := json.Marshal(foreachOutcome)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(foreachData, &decoded))
	assert.Equal(t, 3, decoded.BodyIndex)
	require.NotNil(t, decoded.ForEachTokenID)
	assert.Equal(t, foreachToken, *decoded.ForEachTokenID)
	assert.Empty(t, decoded.Enclosing)

	// A body of a map opened inside another names the run it is inside.
	foreachOutcome.Enclosing = []MapRun{outer}
	nestedData, err := json.Marshal(foreachOutcome)
	require.NoError(t, err)
	assert.Contains(t, string(nestedData), `"enclosing":[{"foreach_token_id":"tok-outer","body_index":2}]`)
	require.NoError(t, json.Unmarshal(nestedData, &decoded))
	assert.Equal(t, []MapRun{outer}, decoded.Enclosing)
	// A run that names no map is not a run.
	var run MapRun
	assert.Error(t, json.Unmarshal([]byte(`{"body_index":2}`), &run))
	assert.Error(t, json.Unmarshal([]byte(`{"foreach_token_id":"","body_index":2}`), &run))
}
