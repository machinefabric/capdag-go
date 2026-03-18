package route

import (
	"fmt"
	"strings"

	"github.com/machinefabric/capdag-go/urn"
)

// MachineEdge represents a single edge in the route graph.
//
// Each edge represents a capability that transforms one or more source
// media types into a target media type. The IsLoop flag indicates
// ForEach semantics (the capability is applied to each item in a list).
type MachineEdge struct {
	// Input media URN(s) — from connected cap's in-spec.
	// Multiple sources represent fan-in.
	Sources []*urn.MediaUrn
	// The capability URN (edge label).
	CapUrn *urn.CapUrn
	// Output media URN — from cap's out-spec.
	Target *urn.MediaUrn
	// Whether this edge has ForEach semantics.
	IsLoop bool
}

// IsEquivalent checks if two edges are semantically equivalent.
//
// Source order does not matter — fan-in sources are compared as sets.
func (e *MachineEdge) IsEquivalent(other *MachineEdge) bool {
	if e.IsLoop != other.IsLoop {
		return false
	}

	if !e.CapUrn.IsEquivalent(other.CapUrn) {
		return false
	}

	// Target equivalence
	if !e.Target.IsEquivalent(other.Target) {
		return false
	}

	// Source set equivalence — order-independent comparison
	if len(e.Sources) != len(other.Sources) {
		return false
	}

	matched := make([]bool, len(other.Sources))
	for _, selfSrc := range e.Sources {
		found := false
		for j, otherSrc := range other.Sources {
			if matched[j] {
				continue
			}
			if selfSrc.IsEquivalent(otherSrc) {
				matched[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// String returns a human-readable representation of the edge.
func (e *MachineEdge) String() string {
	sources := make([]string, len(e.Sources))
	for i, s := range e.Sources {
		sources[i] = s.String()
	}
	loopPrefix := ""
	if e.IsLoop {
		loopPrefix = "LOOP "
	}
	return fmt.Sprintf("(%s) -%s%s-> %s",
		strings.Join(sources, ", "),
		loopPrefix,
		e.CapUrn.String(),
		e.Target.String(),
	)
}

// Machine is the semantic model behind machine notation.
//
// The graph is a collection of directed edges where each edge is a capability
// that transforms source media types into a target media type.
//
// Two graphs are equivalent if they have the same set of edges, regardless
// of ordering.
type Machine struct {
	edges []*MachineEdge
}

// NewMachine creates a new route graph from a slice of edges.
func NewMachine(edges []*MachineEdge) *Machine {
	return &Machine{edges: edges}
}

// EmptyMachine creates an empty route graph.
func EmptyMachine() *Machine {
	return &Machine{edges: nil}
}

// Edges returns the edges of this graph.
func (g *Machine) Edges() []*MachineEdge {
	return g.edges
}

// EdgeCount returns the number of edges.
func (g *Machine) EdgeCount() int {
	return len(g.edges)
}

// IsEmpty checks if the graph has no edges.
func (g *Machine) IsEmpty() bool {
	return len(g.edges) == 0
}

// IsEquivalent checks if two route graphs are semantically equivalent.
func (g *Machine) IsEquivalent(other *Machine) bool {
	if len(g.edges) != len(other.edges) {
		return false
	}

	matched := make([]bool, len(other.edges))
	for _, selfEdge := range g.edges {
		found := false
		for j, otherEdge := range other.edges {
			if matched[j] {
				continue
			}
			if selfEdge.IsEquivalent(otherEdge) {
				matched[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// RootSources collects all unique source media URNs that are not
// produced as targets by any other edge.
func (g *Machine) RootSources() []*urn.MediaUrn {
	var roots []*urn.MediaUrn
	for _, edge := range g.edges {
		for _, src := range edge.Sources {
			isProduced := false
			for _, e := range g.edges {
				if e.Target.IsEquivalent(src) {
					isProduced = true
					break
				}
			}
			if !isProduced {
				alreadyAdded := false
				for _, r := range roots {
					if r.IsEquivalent(src) {
						alreadyAdded = true
						break
					}
				}
				if !alreadyAdded {
					roots = append(roots, src)
				}
			}
		}
	}
	return roots
}

// LeafTargets collects all unique target media URNs that are not
// consumed as sources by any other edge.
func (g *Machine) LeafTargets() []*urn.MediaUrn {
	var leaves []*urn.MediaUrn
	for _, edge := range g.edges {
		isConsumed := false
		for _, e := range g.edges {
			for _, s := range e.Sources {
				if s.IsEquivalent(edge.Target) {
					isConsumed = true
					break
				}
			}
			if isConsumed {
				break
			}
		}
		if !isConsumed {
			alreadyAdded := false
			for _, l := range leaves {
				if l.IsEquivalent(edge.Target) {
					alreadyAdded = true
					break
				}
			}
			if !alreadyAdded {
				leaves = append(leaves, edge.Target)
			}
		}
	}
	return leaves
}

// FromString parses machine notation into a Machine.
func FromString(input string) (*Machine, error) {
	return ParseMachine(input)
}

// String returns a human-readable representation.
func (g *Machine) String() string {
	if len(g.edges) == 0 {
		return "Machine(empty)"
	}
	return fmt.Sprintf("Machine(%d edges)", len(g.edges))
}
