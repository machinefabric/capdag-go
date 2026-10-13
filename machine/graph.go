package machine

import (
	"fmt"
	"strings"

	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/urn"
)

// NodeId is an index into a MachineStrand's nodes slice.
// Dense, starts at 0, stable for the lifetime of the strand.
// Scoped to a single strand — two strands use disjoint NodeId spaces.
type NodeId = uint32

// BindingSource is what feeds one cap argument of a step: a data position of
// the strand, or a constant written in the machine (any JSON value). A
// constant is part of the binding, not a node — nothing else can consume it.
type BindingSource struct {
	// IsConstant says which of the two this is.
	IsConstant bool
	// Node is the data position, when the source is one.
	Node NodeId
	// Constant is the value, when the source is a constant: nil, bool,
	// string, int64, uint64, float64, []any or map[string]any.
	Constant any
}

// NodeSource is a binding source that is a node of the strand.
func NodeSource(id NodeId) BindingSource {
	return BindingSource{Node: id}
}

// ConstantSource is a binding source that is a constant. The value is any
// JSON value in the form encoding/json decodes it to; whole numbers and
// floats are kept apart as notation keeps them.
func ConstantSource(value any) BindingSource {
	normalized, err := normalizeConstant(value)
	if err != nil {
		panic(fmt.Sprintf("a constant of a machine is a JSON value: %v", err))
	}
	return BindingSource{IsConstant: true, Constant: normalized}
}

// EdgeAssignmentBinding records which cap argument (identified by its slot
// media URN) is fed by what: a data position of the strand or a constant.
//
// MachineEdge.Assignment holds these sorted by CapArgMediaUrn so that two
// semantically-equivalent edges produce identical assignment slices.
type EdgeAssignmentBinding struct {
	CapArgMediaUrn *urn.MediaUrn // slot identity per cap definition
	// ArgName is the arg's name in the cap definition: the label notation
	// writes the binding under. It is carried so a machine can be written back
	// without the definitions, and it is NOT identity — CapArgMediaUrn is, and
	// equivalence never reads this.
	ArgName string
	Source  BindingSource
}

// EdgeOutput is one output of a step that its wiring takes, and the node it
// goes to.
//
// OutputMediaUrn is the output's identity per the cap definition (RULE20:
// unique among a cap's outputs). MachineEdge.Outputs holds these sorted by
// OutputMediaUrn, a canonical form like the assignment's.
type EdgeOutput struct {
	OutputMediaUrn *urn.MediaUrn
	// OutputName is the output's name in the cap definition: a label, carried
	// like EdgeAssignmentBinding.ArgName and never compared.
	OutputName string
	Node       NodeId
}

// MachineEdge is one resolved cap-step inside a MachineStrand.
//
// Assignment carries the explicit source-to-cap-arg mapping computed by the
// resolver, sorted by CapArgMediaUrn for canonical comparison.
type MachineEdge struct {
	CapUrn     *urn.CapUrn
	Assignment []EdgeAssignmentBinding
	// Outputs are the outputs the wiring takes, sorted by output media URN. An
	// output the cap declares and no entry names is discarded; an empty list
	// is a sink.
	Outputs []EdgeOutput
	// Policy is what the step does when it fails. Part of what the machine
	// is: two machines that differ in it behave differently.
	Policy StepPolicy
	// IncludedAs is the machine steps this step was included through,
	// outermost first, each as `alias#use`: the alias its header gave the
	// included machine and which use of it this is, counted from one in the
	// order written. Empty for a step the machine wrote itself. It says where
	// a step of the inlined whole came from — a label, like ArgName:
	// equivalence never reads it, and canonical notation does not write it.
	IncludedAs []string
	IsLoop     bool
	// TokenId is the stable identity of the originating resolved-strand step
	// (see planner.StrandStep.TokenId), carried onto the executable DAG unit so
	// a running cap can report *which* step it is — the key the run's live
	// updates route by. It is IDENTITY, not semantics: IsEquivalent
	// deliberately ignores it (two structurally-identical machines stay
	// equivalent even with different per-run TokenIds).
	TokenId planner.StepToken
}

// MainTarget returns the node the step's MAIN output goes to: the taken
// output whose URN is the cap URN's `out=`. False when the wiring does not
// take it.
func (e *MachineEdge) MainTarget() (NodeId, bool) {
	out, err := e.CapUrn.OutMediaUrn()
	if err != nil {
		return 0, false
	}
	for _, output := range e.Outputs {
		if output.OutputMediaUrn.IsEquivalent(out) {
			return output.Node, true
		}
	}
	return 0, false
}

// OutputNodes returns every node this step produces.
func (e *MachineEdge) OutputNodes() []NodeId {
	nodes := make([]NodeId, len(e.Outputs))
	for i, output := range e.Outputs {
		nodes[i] = output.Node
	}
	return nodes
}

// SourceNodes returns every node this step consumes. A constant is not a node.
func (e *MachineEdge) SourceNodes() []NodeId {
	var nodes []NodeId
	for _, binding := range e.Assignment {
		if !binding.Source.IsConstant {
			nodes = append(nodes, binding.Source.Node)
		}
	}
	return nodes
}

func (e *MachineEdge) String() string {
	assignments := make([]string, len(e.Assignment))
	for i, b := range e.Assignment {
		if b.Source.IsConstant {
			assignments[i] = fmt.Sprintf("%s<-%s", b.CapArgMediaUrn, constantText(b.Source.Constant))
		} else {
			assignments[i] = fmt.Sprintf("%s<-#%d", b.CapArgMediaUrn, b.Source.Node)
		}
	}
	outputs := make([]string, len(e.Outputs))
	for i, o := range e.Outputs {
		outputs[i] = fmt.Sprintf("%s->#%d", o.OutputMediaUrn, o.Node)
	}
	// Debug-only marker for a per-item map edge. Not notation syntax (the LOOP
	// keyword is retired); IsLoop is a derived cardinality property.
	loopPrefix := ""
	if e.IsLoop {
		loopPrefix = "map "
	}
	return fmt.Sprintf("%s%s%s (%s) -> (%s)",
		loopPrefix,
		e.CapUrn,
		e.Policy.ToNotation(),
		strings.Join(assignments, ", "),
		strings.Join(outputs, ", "),
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
	// Whether each node holds a sequence, as its producer's output declares (a
	// node nothing produces is a single value here: what a run gives it is the
	// run's to say). Indexed by NodeId. A sequence bound to an arg that takes a
	// single value is what makes a step run once per item.
	nodeIsSequence []bool
}

// newMachineStrand constructs a MachineStrand from already-resolved fields.
// Used by resolve functions after building the canonical node and edge slices.
func newMachineStrand(
	nodes []*urn.MediaUrn,
	edges []*MachineEdge,
	inputAnchorIds []NodeId,
	outputAnchorIds []NodeId,
	nodeIsSequence []bool,
) *MachineStrand {
	if len(nodes) != len(nodeIsSequence) {
		panic("every node of a strand has a cardinality")
	}
	return &MachineStrand{
		nodes:           nodes,
		edges:           edges,
		inputAnchorIds:  inputAnchorIds,
		outputAnchorIds: outputAnchorIds,
		nodeIsSequence:  nodeIsSequence,
	}
}

// NodeIsSequence reports whether node `id` holds a sequence, as its
// producer's output declares.
func (s *MachineStrand) NodeIsSequence(id NodeId) bool {
	return s.nodeIsSequence[id]
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

// IsEquivalent checks strict equivalence with another MachineStrand.
//
// Walks both strands in canonical edge order, building a bijection between
// NodeIds on the fly. Any edge mismatch (cap URN, policy, assignment, outputs,
// or IsLoop) fails the comparison, and so does an inconsistent bijection or
// two paired nodes that hold different types.
func (s *MachineStrand) IsEquivalent(other *MachineStrand) bool {
	if len(s.edges) != len(other.edges) {
		return false
	}

	nodeMap := newNodeBijection(len(s.nodes), len(other.nodes))
	bindNode := func(selfId, otherId NodeId) bool {
		// The two nodes must hold equivalent URNs, whatever the bijection
		// says: nodes that stand for each other carry one type.
		return s.nodes[selfId].IsEquivalent(other.nodes[otherId]) && nodeMap.bind(selfId, otherId)
	}

	// The edges decide which node is which: every node is a source or an
	// output of some edge, and an edge says what each of its nodes is FOR
	// (which arg it feeds, which output it is). Two inputs of one type are told
	// apart by that and by nothing else — not by the order they were first
	// written in, which is not part of what a machine is.
	for i, se := range s.edges {
		oe := other.edges[i]
		if se.IsLoop != oe.IsLoop || !se.Policy.Equal(oe.Policy) || !se.CapUrn.IsEquivalent(oe.CapUrn) {
			return false
		}
		if len(se.Assignment) != len(oe.Assignment) || len(se.Outputs) != len(oe.Outputs) {
			return false
		}
		// Assignment is pre-sorted by CapArgMediaUrn — positional comparison
		// is canonical.
		for j, sb := range se.Assignment {
			ob := oe.Assignment[j]
			if !sb.CapArgMediaUrn.IsEquivalent(ob.CapArgMediaUrn) {
				return false
			}
			switch {
			case sb.Source.IsConstant != ob.Source.IsConstant:
				return false
			case sb.Source.IsConstant:
				// Two machines that give an arg different constants behave
				// differently.
				if !constantsEqual(sb.Source.Constant, ob.Source.Constant) {
					return false
				}
			case !bindNode(sb.Source.Node, ob.Source.Node):
				return false
			}
		}
		// Sorted by output media URN, like the assignment.
		for j, so := range se.Outputs {
			oo := oe.Outputs[j]
			if !so.OutputMediaUrn.IsEquivalent(oo.OutputMediaUrn) || !bindNode(so.Node, oo.Node) {
				return false
			}
		}
	}

	// Every node of a strand is a source or an output of one of its edges, so
	// the walk has paired every node of each strand with one of the other:
	// the strands have the same nodes, and the same of them are anchors.
	return true
}

// nodeBijection maps the nodes of one strand to the nodes of another during an
// equivalence walk. Each bind records a new pair or confirms one already
// made; a node of either strand paired with two nodes of the other is refused.
type nodeBijection struct {
	selfToOther []int
	otherToSelf []int
}

const unbound = -1

func newNodeBijection(selfLen, otherLen int) *nodeBijection {
	b := &nodeBijection{selfToOther: make([]int, selfLen), otherToSelf: make([]int, otherLen)}
	for i := range b.selfToOther {
		b.selfToOther[i] = unbound
	}
	for i := range b.otherToSelf {
		b.otherToSelf[i] = unbound
	}
	return b
}

func (b *nodeBijection) bind(selfId, otherId NodeId) bool {
	si, oi := int(selfId), int(otherId)
	if b.selfToOther[si] != unbound && b.selfToOther[si] != oi {
		return false
	}
	if b.otherToSelf[oi] != unbound && b.otherToSelf[oi] != si {
		return false
	}
	b.selfToOther[si] = oi
	b.otherToSelf[oi] = si
	return true
}

// Machine is an ordered collection of resolved MachineStrands.
//
// Strand declaration order matters: IsEquivalent compares strand-by-strand
// positionally.
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
func FromStrand(strand *planner.Strand, registry *fabric.FabricRegistry) (*Machine, *MachineAbstractionError) {
	resolved, err := resolveStrand(strand, registry, 0)
	if err != nil {
		return nil, err
	}
	return fromResolvedStrands([]*MachineStrand{resolved}), nil
}

// FromStrands builds a Machine containing N MachineStrands, one per input
// strand, in the given order. Each strand is resolved independently.
func FromStrands(strands []*planner.Strand, registry *fabric.FabricRegistry) (*Machine, *MachineAbstractionError) {
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
func FromString(input string, registry *fabric.FabricRegistry) (*Machine, *MachineParseError) {
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
