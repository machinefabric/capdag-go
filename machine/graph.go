package machine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/urn"
)

// NodeId is an index into a MachineStrand's nodes slice.
// Dense, starts at 0, stable for the lifetime of the strand.
// Scoped to a single strand — two strands use disjoint NodeId spaces.
type NodeId = uint32

// EdgeAssignmentBinding records which cap argument (identified by its slot
// media URN) is fed by which data-position in the strand (NodeId).
//
// MachineEdge.Assignment holds these sorted by CapArgMediaUrn so that
// two semantically-equivalent edges produce identical assignment slices.
type EdgeAssignmentBinding struct {
	CapArgMediaUrn *urn.MediaUrn // slot identity per cap definition
	Source         NodeId        // which node feeds this arg
}

// MachineEdge is one resolved cap-step inside a MachineStrand.
//
// Assignment carries the explicit source-to-cap-arg mapping computed by the
// resolver: pairs of (cap arg slot URN, strand node ID). Sorted by
// CapArgMediaUrn for canonical comparison.
type MachineEdge struct {
	CapUrn     *urn.CapUrn
	Assignment []EdgeAssignmentBinding
	Target     NodeId
	IsLoop     bool
}

func (e *MachineEdge) String() string {
	assignments := make([]string, len(e.Assignment))
	for i, b := range e.Assignment {
		assignments[i] = fmt.Sprintf("%s<-#%d", b.CapArgMediaUrn, b.Source)
	}
	loopPrefix := ""
	if e.IsLoop {
		loopPrefix = "LOOP "
	}
	return fmt.Sprintf("%s%s (%s) -> #%d",
		loopPrefix,
		e.CapUrn,
		strings.Join(assignments, ", "),
		e.Target,
	)
}

// MachineStrand is one connected component of resolved cap edges with
// explicit anchor commitments.
//
// Built once via resolve functions. After construction the strand is immutable.
type MachineStrand struct {
	// Distinct data positions in this strand, indexed by NodeId.
	nodes []*urn.MediaUrn
	// Resolved cap-step edges in canonical topological order.
	edges []*MachineEdge
	// NodeIds of root nodes (no producer in this strand). Sorted.
	inputAnchorIds []NodeId
	// NodeIds of leaf nodes (no consumer in this strand). Sorted.
	outputAnchorIds []NodeId
}

// newMachineStrand constructs a MachineStrand from already-resolved fields.
// Used by resolve functions after building the canonical node and edge slices.
func newMachineStrand(
	nodes []*urn.MediaUrn,
	edges []*MachineEdge,
	inputAnchorIds []NodeId,
	outputAnchorIds []NodeId,
) *MachineStrand {
	return &MachineStrand{
		nodes:           nodes,
		edges:           edges,
		inputAnchorIds:  inputAnchorIds,
		outputAnchorIds: outputAnchorIds,
	}
}

// Nodes returns all distinct data positions, indexed by NodeId.
func (s *MachineStrand) Nodes() []*urn.MediaUrn {
	return s.nodes
}

// Edges returns the cap-step edges in canonical topological order.
func (s *MachineStrand) Edges() []*MachineEdge {
	return s.edges
}

// InputAnchorIds returns the NodeIds of root nodes.
func (s *MachineStrand) InputAnchorIds() []NodeId {
	return s.inputAnchorIds
}

// OutputAnchorIds returns the NodeIds of leaf nodes.
func (s *MachineStrand) OutputAnchorIds() []NodeId {
	return s.outputAnchorIds
}

// NodeUrn returns the MediaUrn for a NodeId. Panics if out of range.
func (s *MachineStrand) NodeUrn(id NodeId) *urn.MediaUrn {
	return s.nodes[id]
}

// InputAnchors returns the sorted multiset of input anchor URNs.
func (s *MachineStrand) InputAnchors() []*urn.MediaUrn {
	result := make([]*urn.MediaUrn, len(s.inputAnchorIds))
	for i, id := range s.inputAnchorIds {
		result[i] = s.nodes[id]
	}
	return result
}

// OutputAnchors returns the sorted multiset of output anchor URNs.
func (s *MachineStrand) OutputAnchors() []*urn.MediaUrn {
	result := make([]*urn.MediaUrn, len(s.outputAnchorIds))
	for i, id := range s.outputAnchorIds {
		result[i] = s.nodes[id]
	}
	return result
}

// IsEquivalent checks strict positional equivalence with another MachineStrand.
//
// Walks both strands in canonical edge order, building a bijection between
// NodeIds on the fly. Any anchor or edge mismatch fails the comparison.
func (s *MachineStrand) IsEquivalent(other *MachineStrand) bool {
	if len(s.nodes) != len(other.nodes) {
		return false
	}
	if len(s.edges) != len(other.edges) {
		return false
	}
	if len(s.inputAnchorIds) != len(other.inputAnchorIds) {
		return false
	}
	if len(s.outputAnchorIds) != len(other.outputAnchorIds) {
		return false
	}

	selfToOther := make([]int, len(s.nodes))
	otherToSelf := make([]int, len(other.nodes))
	for i := range selfToOther {
		selfToOther[i] = -1
	}
	for i := range otherToSelf {
		otherToSelf[i] = -1
	}

	bindNode := func(selfId, otherId NodeId) bool {
		// URNs must be equivalent.
		if !s.nodes[selfId].IsEquivalent(other.nodes[otherId]) {
			return false
		}
		si, oi := int(selfId), int(otherId)
		if selfToOther[si] == -1 && otherToSelf[oi] == -1 {
			selfToOther[si] = oi
			otherToSelf[oi] = si
			return true
		}
		if selfToOther[si] == oi && otherToSelf[oi] == si {
			return true
		}
		return false
	}

	// Compare anchors (sorted multisets).
	for i := range s.inputAnchorIds {
		if !bindNode(s.inputAnchorIds[i], other.inputAnchorIds[i]) {
			return false
		}
	}
	for i := range s.outputAnchorIds {
		if !bindNode(s.outputAnchorIds[i], other.outputAnchorIds[i]) {
			return false
		}
	}

	// Compare edges positionally.
	for i, se := range s.edges {
		oe := other.edges[i]
		if se.IsLoop != oe.IsLoop {
			return false
		}
		if !se.CapUrn.IsEquivalent(oe.CapUrn) {
			return false
		}
		if len(se.Assignment) != len(oe.Assignment) {
			return false
		}
		// Assignment is pre-sorted by CapArgMediaUrn — positional comparison is canonical.
		for j, sb := range se.Assignment {
			ob := oe.Assignment[j]
			if !sb.CapArgMediaUrn.IsEquivalent(ob.CapArgMediaUrn) {
				return false
			}
			if !bindNode(sb.Source, ob.Source) {
				return false
			}
		}
		if !bindNode(se.Target, oe.Target) {
			return false
		}
	}
	return true
}

// Machine is an ordered collection of resolved MachineStrands.
//
// Strand declaration order matters: the executor walks the strands in this
// order at runtime, and IsEquivalent compares strand-by-strand positionally.
type Machine struct {
	strands []*MachineStrand
}

// fromResolvedStrands constructs a Machine from already-resolved strands.
func fromResolvedStrands(strands []*MachineStrand) *Machine {
	return &Machine{strands: strands}
}

// FromStrand builds a Machine containing exactly one MachineStrand from a
// planner-produced Strand. The cap registry is required to look up each
// cap's args list for source-to-arg matching.
func FromStrand(strand *planner.Strand, registry *cap.FabricRegistry) (*Machine, *MachineAbstractionError) {
	resolved, err := resolveStrand(strand, registry, 0)
	if err != nil {
		return nil, err
	}
	return fromResolvedStrands([]*MachineStrand{resolved}), nil
}

// FromStrands builds a Machine containing N MachineStrands, one per input
// strand, in the given order. Each strand is resolved independently.
func FromStrands(strands []*planner.Strand, registry *cap.FabricRegistry) (*Machine, *MachineAbstractionError) {
	if len(strands) == 0 {
		return nil, noCapabilityStepsError()
	}
	resolved := make([]*MachineStrand, len(strands))
	for i, s := range strands {
		ms, err := resolveStrand(s, registry, i)
		if err != nil {
			return nil, err
		}
		resolved[i] = ms
	}
	return fromResolvedStrands(resolved), nil
}

// Strands returns all resolved strands in declaration order.
func (m *Machine) Strands() []*MachineStrand {
	return m.strands
}

// StrandCount returns the number of strands.
func (m *Machine) StrandCount() int {
	return len(m.strands)
}

// IsEmpty returns true if the machine has no strands.
func (m *Machine) IsEmpty() bool {
	return len(m.strands) == 0
}

// IsEquivalent checks strict positional equivalence with another Machine.
//
// Two Machines are equivalent iff they have the same number of strands and
// strands[i].IsEquivalent(other.strands[i]) for every i. Strand order matters.
func (m *Machine) IsEquivalent(other *Machine) bool {
	if len(m.strands) != len(other.strands) {
		return false
	}
	for i, s := range m.strands {
		if !s.IsEquivalent(other.strands[i]) {
			return false
		}
	}
	return true
}

// FromString parses machine notation into a Machine.
// Requires the cap registry for the resolution phase.
func FromString(input string, registry *cap.FabricRegistry) (*Machine, *MachineParseError) {
	return ParseMachine(input, registry)
}

// String returns a human-readable representation.
func (m *Machine) String() string {
	if len(m.strands) == 0 {
		return "Machine(empty)"
	}
	edgeCount := 0
	for _, s := range m.strands {
		edgeCount += len(s.edges)
	}
	return fmt.Sprintf("Machine(%d strands, %d edges)", len(m.strands), edgeCount)
}

// --- Serializer ---

// NotationFormat controls the serialization format for machine notation.
type NotationFormat int

const (
	NotationFormatBracketed NotationFormat = iota
	NotationFormatLineBased
)

// ToMachineNotation serializes this machine to canonical one-line bracketed notation.
func (m *Machine) ToMachineNotation() string {
	return m.ToMachineNotationFormatted(NotationFormatBracketed)
}

// ToMachineNotationMultiline serializes to multi-line notation.
func (m *Machine) ToMachineNotationMultiline() string {
	return m.ToMachineNotationFormatted(NotationFormatLineBased)
}

// ToMachineNotationFormatted serializes in the specified format.
func (m *Machine) ToMachineNotationFormatted(format NotationFormat) string {
	if m.IsEmpty() {
		return ""
	}

	var parts []string
	open, close_ := "", ""
	if format == NotationFormatBracketed {
		open, close_ = "[", "]"
	}

	edgeCounter := 0
	nodeCounter := 0

	for _, strand := range m.strands {
		// Build per-strand node name map (NodeId -> name).
		nodeNames := make(map[NodeId]string)

		assignNodeName := func(id NodeId) string {
			if name, ok := nodeNames[id]; ok {
				return name
			}
			name := fmt.Sprintf("n%d", nodeCounter)
			nodeCounter++
			nodeNames[id] = name
			return name
		}

		// Collect alias -> edge index mapping. The Rust reference
		// implementation uses pure-index aliases (`edge_<idx>`) for every
		// edge — there is no privileged tag (such as the legacy `op=…`
		// tag) we can derive a friendlier name from, so we mirror that
		// scheme here.
		aliases := make(map[string]int) // alias -> edge index within strand
		aliasOrder := make([]string, len(strand.edges))

		for eIdx := range strand.edges {
			alias := fmt.Sprintf("edge_%d", edgeCounter)
			aliases[alias] = eIdx
			aliasOrder[eIdx] = alias
			edgeCounter++
		}

		// Sort headers by alias for determinism.
		sortedAliases := make([]string, 0, len(aliases))
		for alias := range aliases {
			sortedAliases = append(sortedAliases, alias)
		}
		sort.Strings(sortedAliases)

		for _, alias := range sortedAliases {
			eIdx := aliases[alias]
			edge := strand.edges[eIdx]
			parts = append(parts, fmt.Sprintf("%s%s %s%s", open, alias, edge.CapUrn, close_))
		}

		// Emit wirings in canonical edge order.
		for eIdx, edge := range strand.edges {
			alias := aliasOrder[eIdx]
			loopPrefix := ""
			if edge.IsLoop {
				loopPrefix = "LOOP "
			}

			// Sort assignment by Source for wiring emission.
			sortedAssignment := make([]EdgeAssignmentBinding, len(edge.Assignment))
			copy(sortedAssignment, edge.Assignment)
			sort.Slice(sortedAssignment, func(a, b int) bool {
				return sortedAssignment[a].Source < sortedAssignment[b].Source
			})

			sources := make([]string, len(sortedAssignment))
			for i, b := range sortedAssignment {
				sources[i] = assignNodeName(b.Source)
			}
			targetName := assignNodeName(edge.Target)

			if len(sources) == 1 {
				parts = append(parts, fmt.Sprintf("%s%s -> %s%s -> %s%s",
					open, sources[0], loopPrefix, alias, targetName, close_))
			} else {
				group := strings.Join(sources, ", ")
				parts = append(parts, fmt.Sprintf("%s(%s) -> %s%s -> %s%s",
					open, group, loopPrefix, alias, targetName, close_))
			}
		}
	}

	if format == NotationFormatBracketed {
		return strings.Join(parts, "")
	}
	return strings.Join(parts, "\n")
}

// jsonEscape escapes only `\` and `"` in a string.
// MediaUrn and CapUrn produce ASCII-safe canonical text, so only
// these two metacharacters need escaping for valid JSON output.
func jsonEscape(s string) string {
	var out strings.Builder
	for _, c := range s {
		if c == '"' {
			out.WriteString(`\"`)
		} else if c == '\\' {
			out.WriteString(`\\`)
		} else {
			out.WriteRune(c)
		}
	}
	return out.String()
}

// ToRenderPayloadJSON serializes the machine to a JSON payload intended for
// rendering the graph in a UI. The format mirrors the Rust to_render_payload_json.
//
// Format: {"strands":[{"nodes":[...],"edges":[...],"input_anchor_nodes":[...],"output_anchor_nodes":[...]},...]}
func (m *Machine) ToRenderPayloadJSON() string {
	if m.IsEmpty() {
		return `{"strands":[]}`
	}

	// Build per-strand node and edge aliases. Counters are global across all
	// strands, matching the Rust serializer's behavior.
	type strandPlan struct {
		nodeNames  []string
		edgeAliases []string
	}
	plans := make([]strandPlan, len(m.strands))
	nextNode := 0
	nextAlias := 0
	for i, strand := range m.strands {
		nodeNames := make([]string, len(strand.nodes))
		for j := range strand.nodes {
			nodeNames[j] = fmt.Sprintf("n%d", nextNode)
			nextNode++
		}
		edgeAliases := make([]string, len(strand.edges))
		for j := range strand.edges {
			edgeAliases[j] = fmt.Sprintf("edge_%d", nextAlias)
			nextAlias++
		}
		plans[i] = strandPlan{nodeNames: nodeNames, edgeAliases: edgeAliases}
	}

	var out strings.Builder
	out.WriteString(`{"strands":[`)
	for sIdx, strand := range m.strands {
		if sIdx > 0 {
			out.WriteByte(',')
		}
		plan := plans[sIdx]
		out.WriteByte('{')

		// nodes
		out.WriteString(`"nodes":[`)
		for id, u := range strand.nodes {
			if id > 0 {
				out.WriteByte(',')
			}
			fmt.Fprintf(&out, `{"id":%q,"urn":%q}`, plan.nodeNames[id], u.String())
		}
		out.WriteString(`],`)

		// edges
		out.WriteString(`"edges":[`)
		for eIdx, edge := range strand.edges {
			if eIdx > 0 {
				out.WriteByte(',')
			}
			isLoopStr := "false"
			if edge.IsLoop {
				isLoopStr = "true"
			}
			fmt.Fprintf(&out, `{"alias":%q,"cap_urn":%q,"is_loop":%s,"assignment":[`,
				plan.edgeAliases[eIdx], edge.CapUrn.String(), isLoopStr)
			for bIdx, b := range edge.Assignment {
				if bIdx > 0 {
					out.WriteByte(',')
				}
				fmt.Fprintf(&out, `{"cap_arg_media_urn":%q,"source_node":%q}`,
					b.CapArgMediaUrn.String(), plan.nodeNames[b.Source])
			}
			fmt.Fprintf(&out, `],"target_node":%q}`, plan.nodeNames[edge.Target])
		}
		out.WriteString(`],`)

		// input_anchor_nodes
		out.WriteString(`"input_anchor_nodes":[`)
		for i, id := range strand.inputAnchorIds {
			if i > 0 {
				out.WriteByte(',')
			}
			fmt.Fprintf(&out, "%q", plan.nodeNames[id])
		}
		out.WriteString(`],`)

		// output_anchor_nodes
		out.WriteString(`"output_anchor_nodes":[`)
		for i, id := range strand.outputAnchorIds {
			if i > 0 {
				out.WriteByte(',')
			}
			fmt.Fprintf(&out, "%q", plan.nodeNames[id])
		}
		out.WriteString(`]`)

		out.WriteByte('}')
	}
	out.WriteString(`]}`)
	return out.String()
}
