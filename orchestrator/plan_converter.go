package orchestrator

import (
	"fmt"

	"github.com/machinefabric/capdag-go/planner"
)

// PlanToResolvedGraph converts a MachinePlan to a ResolvedGraph for execution.
//
// This transforms the node-centric plan (where caps are nodes) into the
// edge-centric graph (where caps are edge labels) that execute_dag expects.
//
// ForEach/Collect/Merge/Split nodes are rejected — the caller must decompose
// first using ExtractPrefixTo/ExtractForEachBody/ExtractSuffixFrom.
func PlanToResolvedGraph(plan *planner.MachinePlan, registry CapRegistryTrait) (*ResolvedGraph, error) {
	nodes := make(map[string]string)
	var resolvedEdges []*ResolvedEdge

	// First pass: identify all data nodes and their media URNs
	for nodeID, node := range plan.Nodes {
		nt := node.NodeType

		switch nt.Kind {
		case planner.NodeKindInputSlot:
			nodes[nodeID] = nt.ExpectedMediaUrn

		case planner.NodeKindCap:
			capDef, err := registry.Lookup(nt.CapUrn)
			if err != nil {
				return nil, err
			}
			outMedia := capDef.Urn.OutSpec()
			nodes[nodeID] = outMedia

		case planner.NodeKindOutput:
			source, ok := plan.Nodes[nt.SourceNode]
			if ok && source.NodeType.Kind == planner.NodeKindCap {
				capDef, err := registry.Lookup(source.NodeType.CapUrn)
				if err != nil {
					return nil, err
				}
				nodes[nodeID] = capDef.Urn.OutSpec()
			}

		case planner.NodeKindWrapInList:
			nodes[nodeID] = nt.ListMediaUrn

		case planner.NodeKindForEach:
			return nil, invalidGraphError(fmt.Sprintf(
				"Plan contains ForEach node '%s'. Decompose the plan using "+
					"extract_prefix_to/extract_foreach_body/extract_suffix_from "+
					"before converting to ResolvedGraph.", nodeID))

		case planner.NodeKindCollect:
			return nil, invalidGraphError(fmt.Sprintf(
				"Plan contains Collect node '%s'. Decompose the plan using "+
					"extract_prefix_to/extract_foreach_body/extract_suffix_from "+
					"before converting to ResolvedGraph.", nodeID))

		case planner.NodeKindMerge:
			return nil, invalidGraphError(fmt.Sprintf(
				"Plan contains Merge node '%s' which is not yet supported for execution.", nodeID))

		case planner.NodeKindSplit:
			return nil, invalidGraphError(fmt.Sprintf(
				"Plan contains Split node '%s' which is not yet supported for execution.", nodeID))
		}
	}

	// Build a map from WrapInList nodes to their input predecessors.
	wrapPredecessors := make(map[string]string)
	for _, edge := range plan.Edges {
		if toNode, ok := plan.Nodes[edge.ToNode]; ok {
			if toNode.NodeType.Kind == planner.NodeKindWrapInList {
				wrapPredecessors[edge.ToNode] = edge.FromNode
			}
		}
	}

	// Second pass: convert edges that lead INTO Cap nodes into ResolvedEdges
	for _, edge := range plan.Edges {
		toNode, ok := plan.Nodes[edge.ToNode]
		if !ok {
			return nil, capNotFoundError(fmt.Sprintf("Node '%s' not found in plan", edge.ToNode))
		}

		// Only create ResolvedEdges for edges that point to Cap nodes
		if toNode.NodeType.Kind == planner.NodeKindCap {
			capUrn := toNode.NodeType.CapUrn
			capDef, err := registry.Lookup(capUrn)
			if err != nil {
				return nil, err
			}
			inMedia := capDef.Urn.InSpec()
			outMedia := capDef.Urn.OutSpec()

			// If the source is a WrapInList node, resolve through to the actual
			// data source. WrapInList is transparent.
			fromNode := edge.FromNode
			if pred, ok := wrapPredecessors[fromNode]; ok {
				fromNode = pred
			}

			resolvedEdges = append(resolvedEdges, &ResolvedEdge{
				From:     fromNode,
				To:       edge.ToNode,
				CapUrn:   capUrn,
				Cap:      capDef,
				InMedia:  inMedia,
				OutMedia: outMedia,
			})
		}
	}

	return &ResolvedGraph{
		Nodes:     nodes,
		Edges:     resolvedEdges,
		GraphName: &plan.Name,
	}, nil
}
