package orchestrator

import (
	"fmt"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/planner"
)

// PlanToResolvedGraph converts a MachinePlan to a ResolvedGraph for execution.
//
// This transforms the node-centric plan (where caps are nodes) into the
// edge-centric graph (where caps are edge labels) that execute_dag expects.
//
// Standalone Collect nodes (OutputMediaUrn != nil) are transparent pass-throughs.
// ForEach-paired Collect nodes (OutputMediaUrn == nil) are rejected.
// ForEach/Merge/Split nodes are also rejected — the caller must decompose
// first using ExtractPrefixTo/ExtractForEachBody/ExtractSuffixFrom.
//
// All cap lookups use GetCachedCap: caps must be pre-loaded into the registry
// cache before calling this function.
func PlanToResolvedGraph(plan *planner.MachinePlan, registry *cap.CapRegistry) (*ResolvedGraph, error) {
	nodes := make(map[string]string)
	var resolvedEdges []*ResolvedEdge

	// lookupCap resolves a cap URN from the registry cache.
	lookupCap := func(capUrn string) (*cap.Cap, error) {
		capDef, ok := registry.GetCachedCap(capUrn)
		if !ok {
			return nil, capNotFoundError(capUrn)
		}
		return capDef, nil
	}

	// First pass: identify all data nodes and their media URNs
	for nodeID, node := range plan.Nodes {
		nt := node.NodeType

		switch nt.Kind {
		case planner.NodeKindInputSlot:
			nodes[nodeID] = nt.ExpectedMediaUrn

		case planner.NodeKindCap:
			capDef, err := lookupCap(nt.CapUrn)
			if err != nil {
				return nil, err
			}
			outMedia := capDef.Urn.OutSpec()
			nodes[nodeID] = outMedia

		case planner.NodeKindOutput:
			source, ok := plan.Nodes[nt.SourceNode]
			if ok && source.NodeType.Kind == planner.NodeKindCap {
				capDef, err := lookupCap(source.NodeType.CapUrn)
				if err != nil {
					return nil, err
				}
				nodes[nodeID] = capDef.Urn.OutSpec()
			}

		case planner.NodeKindCollect:
			if nt.OutputMediaUrn != nil {
				// Standalone Collect (scalar→list): pass-through at execution time.
				// The data flows unchanged, only the type annotation changes.
				// Register the node with the list media URN so downstream edges can find data at it.
				nodes[nodeID] = *nt.OutputMediaUrn
			} else {
				// ForEach-paired Collect without OutputMediaUrn should not reach
				// plan_converter — the plan should have been decomposed first.
				return nil, invalidGraphError(fmt.Sprintf(
					"Plan contains ForEach-paired Collect node '%s'. Decompose the plan using "+
						"extract_prefix_to/extract_foreach_body/extract_suffix_from "+
						"before converting to ResolvedGraph.", nodeID))
			}

		case planner.NodeKindForEach:
			return nil, invalidGraphError(fmt.Sprintf(
				"Plan contains ForEach node '%s'. Decompose the plan using "+
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

	// Build a map from standalone Collect nodes to their input predecessors.
	// Standalone Collect is a pass-through: data at the predecessor flows through unchanged.
	// When an edge's from_node is a standalone Collect, we resolve it to the actual data source.
	collectPredecessors := make(map[string]string)
	for _, edge := range plan.Edges {
		if toNode, ok := plan.Nodes[edge.ToNode]; ok {
			if toNode.NodeType.Kind == planner.NodeKindCollect && toNode.NodeType.OutputMediaUrn != nil {
				collectPredecessors[edge.ToNode] = edge.FromNode
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
			capDef, err := lookupCap(capUrn)
			if err != nil {
				return nil, err
			}
			inMedia := capDef.Urn.InSpec()
			outMedia := capDef.Urn.OutSpec()

			// If the source is a standalone Collect node, resolve through to the
			// actual data source. Standalone Collect is transparent — data at the
			// predecessor flows unchanged through it.
			fromNode := edge.FromNode
			if pred, ok := collectPredecessors[fromNode]; ok {
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
