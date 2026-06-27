package orchestrator

import (
	"fmt"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/machine"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/urn"
)

// mediaUrnsCompatible checks if two media URNs are on the same specialization chain.
func mediaUrnsCompatible(a, b *urn.MediaUrn) (bool, error) {
	return a.IsComparable(b), nil
}

// checkStructureCompatibility checks if two media URNs have compatible structures.
func checkStructureCompatibility(source, target *urn.MediaUrn, nodeName string) error {
	sourceStructure := planner.StructureOpaque
	if source.IsRecord() {
		sourceStructure = planner.StructureRecord
	}

	targetStructure := planner.StructureOpaque
	if target.IsRecord() {
		targetStructure = planner.StructureRecord
	}

	if sourceStructure != targetStructure {
		return structureMismatchError(nodeName, sourceStructure, targetStructure)
	}
	return nil
}

// ParseMachineToCapDag parses machine notation and produces a validated orchestration graph.
//
// Each cap URN is resolved via the registry's GetCachedCap. Node media URNs are
// derived from the cap's in=/out= specs. Media type consistency and structure
// compatibility (record vs opaque) are validated at each node.
// Caps must be pre-loaded into the registry cache before calling this function.
func ParseMachineToCapDag(machineStr string, registry *cap.FabricRegistry) (*ResolvedGraph, error) {
	// Phase 1: Parse + resolve. The resolver does the syntactic parse, the
	// source-to-cap-arg matching (Hungarian minimum-cost bipartite matching by
	// media-URN conformance), cycle detection, and canonical edge ordering. It
	// also rejects unknown caps. The orchestrator's contributions are: the user
	// node-name keying, the per-binding ResolvedEdge shape the executor
	// consumes, and the structure-compatibility check (record vs opaque).
	m, strandNodeNames, parseErr := machine.ParseMachineWithNodeNames(machineStr, registry)
	if parseErr != nil {
		return nil, machineNotationParseFailedError(parseErr.Error())
	}

	if len(strandNodeNames) != len(m.Strands()) {
		return nil, machineNotationParseFailedError(fmt.Sprintf(
			"internal error: %d strand node-name maps but %d strands",
			len(strandNodeNames), len(m.Strands())))
	}

	// Phase 2: For each strand, build a reverse NodeId → user node name map so
	// we can produce ResolvedEdges keyed on names. Then walk the strand's edges
	// and emit one ResolvedEdge per binding (cap arg). The source URN of each
	// binding is taken from the strand's interned node URN (the resolver already
	// assigned each source NodeId to the correct cap arg by conformance), never
	// re-derived positionally.
	nodeMedia := make(map[string]*urn.MediaUrn)
	var resolvedEdges []*ResolvedEdge

	for strandIdx, strand := range m.Strands() {
		idToName := invertNodeNames(strandNodeNames[strandIdx])

		for _, edge := range strand.Edges() {
			capUrnStr := edge.CapUrn.String()
			capDef, ok := registry.GetCachedCap(capUrnStr)
			if !ok {
				return nil, capNotFoundError(capUrnStr)
			}

			// The cap's declared output URN is the data-type URN that flows out
			// of this cap on the wire; the target node carries it.
			capOutMedia, err := edge.CapUrn.OutMediaUrn()
			if err != nil {
				return nil, mediaUrnParseError(err.Error())
			}

			targetName, err := lookupNodeName(idToName, edge.Target)
			if err != nil {
				return nil, err
			}

			// The cap's in= spec is the stream label for input data on the wire.
			capInMedia, err := edge.CapUrn.InMediaUrn()
			if err != nil {
				return nil, mediaUrnParseError(err.Error())
			}

			for _, binding := range edge.Assignment {
				sourceName, err := lookupNodeName(idToName, binding.Source)
				if err != nil {
					return nil, err
				}
				sourceNodeUrn := strand.NodeUrn(binding.Source)

				// Source node media compatibility check.
				if existing, ok := nodeMedia[sourceName]; ok {
					compatible, _ := mediaUrnsCompatible(existing, sourceNodeUrn)
					if !compatible {
						return nil, nodeMediaConflictError(sourceName, existing.String(), sourceNodeUrn.String())
					}
					if err := checkStructureCompatibility(existing, sourceNodeUrn, sourceName); err != nil {
						return nil, err
					}
				} else {
					nodeMedia[sourceName] = sourceNodeUrn
				}

				// Target node media compatibility check.
				if existing, ok := nodeMedia[targetName]; ok {
					compatible, _ := mediaUrnsCompatible(existing, capOutMedia)
					if !compatible {
						return nil, nodeMediaConflictError(targetName, existing.String(), capOutMedia.String())
					}
					if err := checkStructureCompatibility(capOutMedia, existing, targetName); err != nil {
						return nil, err
					}
				} else {
					nodeMedia[targetName] = capOutMedia
				}

				resolvedEdges = append(resolvedEdges, &ResolvedEdge{
					From:     sourceName,
					To:       targetName,
					CapUrn:   capUrnStr,
					Cap:      capDef,
					InMedia:  capInMedia.String(),
					OutMedia: capOutMedia.String(),
				})
			}
		}
	}

	// Phase 3: DAG validation (cycle detection via topological sort)
	nodeMediaStrings := make(map[string]string)
	for k, v := range nodeMedia {
		nodeMediaStrings[k] = v.String()
	}

	if err := ValidateDag(nodeMediaStrings, resolvedEdges); err != nil {
		return nil, err
	}

	return &ResolvedGraph{
		Nodes: nodeMediaStrings,
		Edges: resolvedEdges,
	}, nil
}

// invertNodeNames inverts a per-strand user node-name → NodeId map into a
// NodeId → name map. The forward map is built by the machine parser when
// allocating NodeIds; the inverse lets the orchestrator label each binding with
// its user-written node name. Mirrors Rust's invert_node_names.
func invertNodeNames(nameToId map[string]machine.NodeId) map[machine.NodeId]string {
	out := make(map[machine.NodeId]string, len(nameToId))
	for name, id := range nameToId {
		out[id] = name
	}
	return out
}

// lookupNodeName resolves a NodeId to its user-written node name, failing hard
// if the parser left a NodeId without a name (an internal invariant violation).
// Mirrors Rust's lookup_node_name.
func lookupNodeName(idToName map[machine.NodeId]string, id machine.NodeId) (string, error) {
	name, ok := idToName[id]
	if !ok {
		return "", machineNotationParseFailedError(fmt.Sprintf(
			"internal error: NodeId %d has no user-written node name", id))
	}
	return name, nil
}
