package orchestrator

import (
	"fmt"
	"strings"

	peg "github.com/yhirose/go-peg"

	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/machine"
	"github.com/machinefabric/capdag-go/urn"
)

// orchestratorPegGrammar is the same grammar used by the machine parser.
// Duplicated here because the machine parser doesn't export it.
const orchestratorPegGrammar = `
  program     <- stmt* !.
  stmt        <- '[' inner ']'
  inner       <- wiring / header
  header      <- alias cap_urn
  wiring      <- source arrow loop_cap arrow alias
  source      <- group / alias_ref
  group       <- '(' alias_ref (',' alias_ref)+ ')'
  loop_cap    <- loop_keyword alias_ref / alias_ref
  loop_keyword <- 'LOOP'
  arrow       <- < '-'+ '>' >
  alias       <- < [a-zA-Z_] [a-zA-Z0-9_-]* >
  alias_ref   <- < [a-zA-Z_] [a-zA-Z0-9_-]* >
  cap_urn     <- < 'cap:' cap_urn_body* >
  cap_urn_body <- quoted_value / !']' .
  quoted_value <- '"' ('\\"' / '\\\\' / !'"' .)* '"'
  %whitespace <- [ \t\r\n]*
`

// wiringInfo holds the node names from a single wiring statement.
type wiringInfo struct {
	sourceNames []string
	targetName  string
}

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
// Each cap URN is resolved via the registry. Node media URNs are derived
// from the cap's in=/out= specs. Media type consistency and structure
// compatibility (record vs opaque) are validated at each node.
func ParseMachineToCapDag(machineStr string, registry CapRegistryTrait) (*ResolvedGraph, error) {
	// Step 1: Parse machine notation into a Machine.
	machine, err := machine.ParseMachine(machineStr)
	if err != nil {
		return nil, machineNotationParseFailedError(err.Error())
	}

	// Step 2: Extract node names from the machine notation.
	wirings, err := extractWiringInfo(machineStr)
	if err != nil {
		return nil, err
	}

	// Validate that wiring count matches edge count.
	if len(wirings) != machine.EdgeCount() {
		return nil, machineNotationParseFailedError(fmt.Sprintf(
			"internal error: %d wirings but %d edges — machine parser edge ordering invariant violated",
			len(wirings), machine.EdgeCount()))
	}

	// Step 3: For each edge, resolve cap via registry and build ResolvedEdge entries.
	nodeMedia := make(map[string]*urn.MediaUrn)
	var resolvedEdges []*ResolvedEdge

	edges := machine.Edges()
	for edgeIdx, edge := range edges {
		capUrnStr := edge.CapUrn.String()
		capDef, err := registry.Lookup(capUrnStr)
		if err != nil {
			return nil, err
		}

		capInMedia, err := edge.CapUrn.InMediaUrn()
		if err != nil {
			return nil, mediaUrnParseError(err.Error())
		}

		capOutMedia, err := edge.CapUrn.OutMediaUrn()
		if err != nil {
			return nil, mediaUrnParseError(err.Error())
		}

		wiring := wirings[edgeIdx]

		// Build resolved edges — one per source (fan-in produces multiple edges)
		for i, srcName := range wiring.sourceNames {
			var edgeInMedia *urn.MediaUrn
			if i == 0 {
				// Primary source: use cap's in= spec
				edgeInMedia = capInMedia
			} else {
				// Secondary source (fan-in): resolve from existing assignment
				// or from the cap's args list
				existing, hasExisting := nodeMedia[srcName]
				isWildcard := hasExisting && existing.String() == "media:"
				if hasExisting && !isWildcard {
					edgeInMedia = existing
				} else {
					// Resolve from cap.args — secondary sources map to args
					// beyond the primary in= spec (arg index i-1 for source i)
					argIdx := i - 1
					args := capDef.GetArgs()
					if argIdx < len(args) {
						argMedia, err := urn.NewMediaUrnFromString(args[argIdx].MediaUrn)
						if err == nil {
							edgeInMedia = argMedia
						}
					}
					if edgeInMedia == nil {
						return nil, machineNotationParseFailedError(fmt.Sprintf(
							"fan-in secondary source '%s' (index %d) has no media type and cap '%s' has no matching arg at index %d",
							srcName, i, capUrnStr, argIdx))
					}
				}
			}

			// Validate source node media compatibility
			if existing, ok := nodeMedia[srcName]; ok {
				compatible, _ := mediaUrnsCompatible(existing, edgeInMedia)
				if !compatible {
					return nil, nodeMediaConflictError(srcName, existing.String(), edgeInMedia.String())
				}
				if err := checkStructureCompatibility(existing, edgeInMedia, srcName); err != nil {
					return nil, err
				}
			} else {
				nodeMedia[srcName] = edgeInMedia
			}

			// Validate target node media compatibility
			if existing, ok := nodeMedia[wiring.targetName]; ok {
				compatible, _ := mediaUrnsCompatible(existing, capOutMedia)
				if !compatible {
					return nil, nodeMediaConflictError(wiring.targetName, existing.String(), capOutMedia.String())
				}
				if err := checkStructureCompatibility(capOutMedia, existing, wiring.targetName); err != nil {
					return nil, err
				}
			} else {
				nodeMedia[wiring.targetName] = capOutMedia
			}

			resolvedEdges = append(resolvedEdges, &ResolvedEdge{
				From:     srcName,
				To:       wiring.targetName,
				CapUrn:   capUrnStr,
				Cap:      capDef,
				InMedia:  edgeInMedia.String(),
				OutMedia: capOutMedia.String(),
			})
		}
	}

	// Step 4: DAG validation (cycle detection via topological sort)
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

// extractWiringInfo extracts wiring node names from machine notation via the PEG parser.
//
// The Machine model discards alias/node names. This function extracts
// them from wiring statements in order.
func extractWiringInfo(machineStr string) ([]wiringInfo, error) {
	parser, err := peg.NewParser(orchestratorPegGrammar)
	if err != nil {
		return nil, machineNotationParseFailedError(fmt.Sprintf("grammar compilation failed: %s", err))
	}
	parser.EnableAst()

	ast, err := parser.ParseAndGetAst(strings.TrimSpace(machineStr), nil)
	if err != nil {
		return nil, machineNotationParseFailedError(err.Error())
	}

	var wirings []wiringInfo

	// Walk program → stmt → inner → wiring
	for _, stmtNode := range ast.Nodes {
		if stmtNode.Name != "stmt" {
			continue
		}
		// stmt → inner
		if len(stmtNode.Nodes) == 0 {
			continue
		}
		innerNode := stmtNode.Nodes[0]
		if len(innerNode.Nodes) == 0 {
			continue
		}
		contentNode := innerNode.Nodes[0]

		if contentNode.Name != "wiring" {
			continue // Skip headers
		}

		// wiring = source arrow loop_cap arrow alias
		if len(contentNode.Nodes) < 5 {
			continue
		}

		// Parse source (single alias or group)
		sourceNode := contentNode.Nodes[0]
		sourceNames := extractSourceNames(sourceNode)

		// Target alias is the last child (index 4)
		targetName := contentNode.Nodes[4].Token

		wirings = append(wirings, wiringInfo{
			sourceNames: sourceNames,
			targetName:  targetName,
		})
	}

	return wirings, nil
}

// extractSourceNames extracts source node names from a source AST node.
func extractSourceNames(node *peg.Ast) []string {
	if len(node.Nodes) == 0 {
		// Single alias_ref
		return []string{node.Token}
	}

	inner := node.Nodes[0]
	if inner.Name == "group" {
		var names []string
		for _, child := range inner.Nodes {
			if child.Name == "alias_ref" || child.Name == "alias" {
				names = append(names, child.Token)
			}
		}
		return names
	}

	// alias_ref
	if inner.Token != "" {
		return []string{inner.Token}
	}
	return []string{node.Token}
}
