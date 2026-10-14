package orchestrator

import (
	"fmt"
	"sort"

	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/urn"
)

// Steps of a MachinePlan as a ResolvedGraph.
//
// The plan is node-centric: steps, the values they read and produce, the maps
// they run in. An executor runs a ResolvedGraph: steps with their cap
// definitions, each saying which nodes its input streams are sent from and
// which nodes its outputs are stored at. This file is the bridge.
//
// A graph holds only steps. What else a plan has is read through them:
//
//   - an edge into a step is one of its inputs — the cap's `in=` stream for a
//     Direct edge, the named arg's stream for an Arg edge;
//   - a Collect wired into a step is that input's gather: its members, in
//     order;
//   - a CapOutput of a step is an output of it that something takes.
//
// Maps are not in a graph at all: a map's body runs once per item, and the
// steps of one scope are asked for at a time (StepsToResolvedGraph). A node a
// graph's steps read and none of them produces is an input of the graph,
// whatever it is in the plan — an input slot, an item, a step of another
// scope.

// edgeKindLabel names an edge kind in a refusal.
func edgeKindLabel(kind planner.EdgeKind) string {
	switch kind {
	case planner.EdgeKindDirect:
		return "Direct"
	case planner.EdgeKindArg:
		return "Arg"
	case planner.EdgeKindIteration:
		return "Iteration"
	case planner.EdgeKindCollection:
		return "Collection"
	default:
		return fmt.Sprintf("EdgeKind(%d)", kind)
	}
}

// nodeKindLabel names a node kind in a refusal.
func nodeKindLabel(kind planner.ExecutionNodeKind) string {
	switch kind {
	case planner.NodeKindCap:
		return "Cap"
	case planner.NodeKindCapOutput:
		return "CapOutput"
	case planner.NodeKindForEach:
		return "ForEach"
	case planner.NodeKindItem:
		return "Item"
	case planner.NodeKindCollect:
		return "Collect"
	case planner.NodeKindInputSlot:
		return "InputSlot"
	case planner.NodeKindOutput:
		return "Output"
	default:
		return fmt.Sprintf("ExecutionNodeKind(%d)", kind)
	}
}

func sortedNodeIDs(plan *planner.MachinePlan) []string {
	ids := make([]string, 0, len(plan.Nodes))
	for id := range plan.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// StepsToResolvedGraph returns the steps `stepIDs` of `plan` (ids of its Cap
// nodes) as a graph.
//
// Fails when an id is not a step of the plan, when a step's cap is not in the
// registry cache, or when the plan wires a step in a way no invocation can be
// given: two sources on one stream, or a map wired straight into a step.
func StepsToResolvedGraph(plan *planner.MachinePlan, stepIDs []string, registry *fabric.FabricRegistry) (*ResolvedGraph, error) {
	nodes := make(map[string]string)
	steps := make([]*ResolvedStep, 0, len(stepIDs))

	// A step's other taken outputs: the CapOutput nodes that name it.
	outputsOf := make(map[string][]StepOutput)
	for _, id := range sortedNodeIDs(plan) {
		nt := plan.Nodes[id].NodeType
		if nt.Kind == planner.NodeKindCapOutput {
			outputsOf[nt.CapNode] = append(outputsOf[nt.CapNode], StepOutput{
				OutputMediaUrn: nt.OutputMediaUrn,
				Node:           id,
			})
		}
	}

	for _, stepID := range stepIDs {
		node, ok := plan.Nodes[stepID]
		if !ok {
			return nil, invalidGraphError(fmt.Sprintf("'%s' is not a node of plan '%s'", stepID, plan.Name))
		}
		if node.NodeType.Kind != planner.NodeKindCap {
			return nil, invalidGraphError(fmt.Sprintf("'%s' is not a step of plan '%s'", stepID, plan.Name))
		}
		capUrn := node.NodeType.CapUrn
		capDef, ok := registry.GetCachedCap(capUrn)
		if !ok {
			return nil, capNotFoundError(capUrn)
		}

		// The streams the step is sent, in the order the plan wires them.
		var inputs []StepInput
		var taken []*urn.MediaUrn
		for _, edge := range plan.Edges {
			if edge.ToNode != stepID {
				continue
			}
			var streamUrn string
			switch edge.Type.Kind {
			case planner.EdgeKindDirect:
				streamUrn = capDef.Urn.InSpec()
			case planner.EdgeKindArg:
				streamUrn = edge.Type.ArgUrn
			default:
				return nil, invalidGraphError(fmt.Sprintf(
					"step '%s' has an incoming %s edge from '%s' — a step is wired by its main input and its args only",
					stepID, edgeKindLabel(edge.Type.Kind), edge.FromNode))
			}
			from, ok := plan.Nodes[edge.FromNode]
			if !ok {
				return nil, invalidGraphError(fmt.Sprintf(
					"step '%s' is wired from '%s', which is not in the plan", stepID, edge.FromNode))
			}
			var source StepSource
			switch from.NodeType.Kind {
			case planner.NodeKindCollect:
				source = GatherStepSource(append([]string(nil), from.NodeType.InputNodes...))
			case planner.NodeKindForEach, planner.NodeKindOutput:
				return nil, invalidGraphError(fmt.Sprintf(
					"step '%s' is wired from '%s', which holds no value", stepID, edge.FromNode))
			default:
				source = NodeStepSource(edge.FromNode)
			}
			stream, err := urn.NewMediaUrnFromString(streamUrn)
			if err != nil {
				return nil, invalidGraphError(fmt.Sprintf(
					"step '%s': input stream URN '%s' is not a media URN: %v", stepID, streamUrn, err))
			}
			for _, other := range taken {
				if other.IsEquivalent(stream) {
					return nil, invalidGraphError(fmt.Sprintf(
						"step '%s' is wired twice on input stream '%s' — one stream has one source; several values into one arg are a gather",
						stepID, streamUrn))
				}
			}
			taken = append(taken, stream)
			inputs = append(inputs, StepInput{StreamUrn: streamUrn, Source: source})
		}

		outMedia := capDef.Urn.OutSpec()
		nodes[stepID] = outMedia
		outputs := outputsOf[stepID]
		for _, output := range outputs {
			nodes[output.Node] = output.OutputMediaUrn
		}
		steps = append(steps, &ResolvedStep{
			TokenId:  node.NodeType.TokenId,
			Node:     stepID,
			CapUrn:   capUrn,
			Cap:      capDef,
			Inputs:   inputs,
			Outputs:  outputs,
			OutMedia: outMedia,
			Policy:   node.NodeType.Policy,
		})
	}

	// The nodes the graph is given: what its steps read and none produces.
	for _, step := range steps {
		for _, source := range step.SourceNodes() {
			if _, known := nodes[source]; known {
				continue
			}
			from, ok := plan.Nodes[source]
			if !ok {
				return nil, invalidGraphError(fmt.Sprintf("a step reads '%s', which is not in the plan", source))
			}
			switch from.NodeType.Kind {
			case planner.NodeKindInputSlot:
				nodes[source] = from.NodeType.ExpectedMediaUrn
			case planner.NodeKindItem:
				nodes[source] = from.NodeType.MediaUrn
			case planner.NodeKindCapOutput:
				nodes[source] = from.NodeType.OutputMediaUrn
			case planner.NodeKindCap:
				capDef, ok := registry.GetCachedCap(from.NodeType.CapUrn)
				if !ok {
					return nil, capNotFoundError(from.NodeType.CapUrn)
				}
				nodes[source] = capDef.Urn.OutSpec()
			default:
				return nil, invalidGraphError(fmt.Sprintf(
					"a step reads '%s', which holds no value (%s)", source, nodeKindLabel(from.NodeType.Kind)))
			}
		}
	}

	name := plan.Name
	return &ResolvedGraph{Nodes: nodes, Steps: steps, GraphName: &name}, nil
}

// PlanToResolvedGraph returns every step of a plan that has no map, as one
// graph.
//
// A plan with a map is not one graph: its body runs once per item. Asking for
// it here is refused, naming the map.
func PlanToResolvedGraph(plan *planner.MachinePlan, registry *fabric.FabricRegistry) (*ResolvedGraph, error) {
	var stepIDs []string
	for _, id := range sortedNodeIDs(plan) {
		switch plan.Nodes[id].NodeType.Kind {
		case planner.NodeKindCap:
			stepIDs = append(stepIDs, id)
		case planner.NodeKindForEach:
			return nil, invalidGraphError(fmt.Sprintf(
				"plan '%s' has a map ('%s'): its body runs once per item and is not one graph — run its scopes one at a time",
				plan.Name, id))
		}
	}
	return StepsToResolvedGraph(plan, stepIDs, registry)
}
