package machine

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/planner"
)

// Machine notation serializer — the canonical text of a Machine.
//
// The serializer walks the strands in declaration order and emits one
// notation document covering all of them. Two strictly-equivalent Machines
// produce byte-identical notation, because both the canonical edge order
// within each strand and the allocation of step and node names are
// deterministic functions of the resolved structure.
//
// All headers come first, then all wirings. Steps are named `edge_<n>` in the
// order of the walk across all strands; nodes are named `n<n>` in the order
// the walk first meets them. Strand boundaries are unmarked: the parser
// recovers them from shared node names, and each strand's node names are a
// range of its own.

// NotationFormat controls the serialization format for machine notation.
type NotationFormat int

const (
	// NotationFormatBracketed wraps each statement in `[...]`: the canonical,
	// single-line form used as a stable identifier.
	NotationFormatBracketed NotationFormat = iota
	// NotationFormatLineBased writes one statement per line, no brackets.
	NotationFormatLineBased
)

// ToMachineNotation serializes this machine to canonical one-line bracketed
// notation. Caps are written by their canonical URN.
func (m *Machine) ToMachineNotation() string {
	return m.ToMachineNotationFormatted(NotationFormatBracketed)
}

// ToMachineNotationMultiline serializes to line-based notation.
func (m *Machine) ToMachineNotationMultiline() string {
	return m.ToMachineNotationFormatted(NotationFormatLineBased)
}

// ToMachineNotationFormatted serializes in the specified format, caps written
// by their canonical URN.
func (m *Machine) ToMachineNotationFormatted(format NotationFormat) string {
	return m.emitNotation(m.serializationPlan(nil), format)
}

// ToMachineNotationAliased serializes to machine notation writing each cap by
// its registered display alias (shortest name, ties alphabetical) when one
// exists, and by a header otherwise.
//
// This is the "store aliased" form: generated and persisted machines use it so
// the saved notation reads in terms of aliases. An aliased cap is referenced
// directly in the wiring's step position by its alias, with no header (the
// grammar permits an alias only there, never in a header). The parser resolves
// the aliases back to URNs on load, so the form round-trips.
func (m *Machine) ToMachineNotationAliased(registry *fabric.FabricRegistry, format NotationFormat) string {
	return m.emitNotation(m.serializationPlan(registry), format)
}

// strandPlan is one strand's names.
type strandPlan struct {
	// edgeTokens is the step-position token of each edge, in canonical edge
	// order: the cap's display alias when the plan aliases and one exists,
	// the synthetic `edge_N` otherwise.
	edgeTokens []string
	// needsHeader is true for a synthetic `edge_N`, which a header binds to
	// the cap URN.
	needsHeader []bool
	// nodeNames is the name of each node, indexed by NodeId.
	nodeNames []string
}

// serializationPlan names every step and node of the machine. With a registry
// a cap that has a display alias is written by it.
func (m *Machine) serializationPlan(registry *fabric.FabricRegistry) []strandPlan {
	plans := make([]strandPlan, len(m.strands))
	nextAlias, nextNode := 0, 0
	for i, strand := range m.strands {
		plan := strandPlan{
			edgeTokens:  make([]string, len(strand.edges)),
			needsHeader: make([]bool, len(strand.edges)),
			nodeNames:   make([]string, len(strand.nodes)),
		}
		for eIdx, edge := range strand.edges {
			plan.edgeTokens[eIdx] = fmt.Sprintf("edge_%d", nextAlias)
			plan.needsHeader[eIdx] = true
			if registry != nil {
				if alias, aliased := registry.DisplayAliasForURN(edge.CapUrn.String()); aliased {
					plan.edgeTokens[eIdx] = alias
					plan.needsHeader[eIdx] = false
				}
			}
			nextAlias++
		}
		// Nodes are named in the order the canonical edge walk first meets
		// them: each edge's sources in assignment order, then its outputs. The
		// name of a node is then a property of the machine, not of the order
		// its nodes happened to be allocated in — two inputs of one type get
		// the same names whichever was written first.
		named := make([]bool, len(strand.nodes))
		for _, edge := range strand.edges {
			for _, node := range append(edge.SourceNodes(), edge.OutputNodes()...) {
				if !named[node] {
					plan.nodeNames[node] = fmt.Sprintf("n%d", nextNode)
					named[node] = true
					nextNode++
				}
			}
		}
		for node, isNamed := range named {
			if !isNamed {
				panic(fmt.Sprintf("every node of a strand is a source or an output of an edge: node %d is neither", node))
			}
		}
		plans[i] = plan
	}
	return plans
}

// formatWiring writes one wiring statement for an edge, without brackets.
//
// Every binding is written under its arg's name and every taken output under
// its output's name, in the canonical (URN-sorted) order of the edge. Nothing
// is left for a reader of the text to match by type, so what is written
// resolves to the edge it was written from whatever other args the cap has. A
// gather writes its arg's name once per member, in item order.
//
// The per-item map (edge.IsLoop) is not written: it is a derived cardinality
// property, and re-parsing derives the same from the cap shapes.
func formatWiring(edge *MachineEdge, token string, plan strandPlan) string {
	members := make([]string, len(edge.Assignment))
	for i, binding := range edge.Assignment {
		if binding.Source.IsConstant {
			members[i] = binding.ArgName + "=" + constantText(binding.Source.Constant)
		} else {
			members[i] = binding.ArgName + "=" + plan.nodeNames[binding.Source.Node]
		}
	}
	targets := make([]string, len(edge.Outputs))
	for i, output := range edge.Outputs {
		targets[i] = output.OutputName + "=" + plan.nodeNames[output.Node]
	}
	return fmt.Sprintf("(%s) -> %s%s -> (%s)",
		strings.Join(members, ", "), token, edge.Policy.ToNotation(), strings.Join(targets, ", "))
}

func (m *Machine) emitNotation(plans []strandPlan, format NotationFormat) string {
	if m.IsEmpty() {
		return ""
	}
	open, closing, between := "[", "]", ""
	if format == NotationFormatLineBased {
		open, closing, between = "", "", "\n"
	}
	var statements []string
	// Headers across all strands — only for edges whose step token is a
	// synthetic `edge_N`.
	for sIdx, strand := range m.strands {
		for eIdx, edge := range strand.edges {
			if plans[sIdx].needsHeader[eIdx] {
				statements = append(statements, fmt.Sprintf("%s%s %s%s", open, plans[sIdx].edgeTokens[eIdx], edge.CapUrn, closing))
			}
		}
	}
	// Wirings across all strands, in canonical edge order.
	for sIdx, strand := range m.strands {
		for eIdx, edge := range strand.edges {
			statements = append(statements, open+formatWiring(edge, plans[sIdx].edgeTokens[eIdx], plans[sIdx])+closing)
		}
	}
	return strings.Join(statements, between)
}

// StrandToMachineNotation knits a planner-produced Strand into a single-strand
// Machine and serializes it to one-line bracketed machine notation writing
// each cap by its registered display alias (the "store aliased" form). This is
// the notation used for generated/discovered paths and for persistence.
//
// Alias-independent canonical identity, when needed, comes from
// Machine.ToMachineNotation() on the knitted machine — not this.
func StrandToMachineNotation(strand *planner.Strand, registry *fabric.FabricRegistry) (string, *MachineAbstractionError) {
	m, err := FromStrand(strand, registry)
	if err != nil {
		return "", err
	}
	return m.ToMachineNotationAliased(registry, NotationFormatBracketed), nil
}

// ToRenderPayloadJSON serializes the machine to the JSON payload a graph view
// draws from:
//
//	{"strands":[{"nodes":[{"id","urn"}],
//	             "edges":[{"alias","cap_urn","is_loop","retry","on_error","included_as",
//	                       "assignment":[{"cap_arg_media_urn","arg_name","source_node"|"constant"}],
//	                       "outputs":[{"output_media_urn","output_name","node"}]}],
//	             "input_anchor_nodes":[...],"output_anchor_nodes":[...]}]}
//
// Node names are those of the canonical notation, so a notation string and its
// render payload share node identities. Each step is labelled `edge_N`.
func (m *Machine) ToRenderPayloadJSON() string {
	return m.renderPayloadJSON(nil)
}

// ToRenderPayloadJSONAliased is ToRenderPayloadJSON labelling each step by the
// cap's display alias when one is registered (shortest, then alphabetical),
// and by the synthetic `edge_N` otherwise.
func (m *Machine) ToRenderPayloadJSONAliased(registry *fabric.FabricRegistry) string {
	return m.renderPayloadJSON(registry)
}

func (m *Machine) renderPayloadJSON(registry *fabric.FabricRegistry) string {
	if m.IsEmpty() {
		return `{"strands":[]}`
	}
	plans := m.serializationPlan(registry)

	var out bytes.Buffer
	text := func(value string) { writeConstantString(&out, value) }
	list := func(count int, each func(int)) {
		out.WriteByte('[')
		for i := 0; i < count; i++ {
			if i > 0 {
				out.WriteByte(',')
			}
			each(i)
		}
		out.WriteByte(']')
	}

	out.WriteString(`{"strands":`)
	list(len(m.strands), func(sIdx int) {
		strand, plan := m.strands[sIdx], plans[sIdx]
		out.WriteString(`{"nodes":`)
		list(len(strand.nodes), func(id int) {
			out.WriteString(`{"id":`)
			text(plan.nodeNames[id])
			out.WriteString(`,"urn":`)
			text(strand.nodes[id].String())
			out.WriteByte('}')
		})

		out.WriteString(`,"edges":`)
		list(len(strand.edges), func(eIdx int) {
			edge := strand.edges[eIdx]
			out.WriteString(`{"alias":`)
			text(plan.edgeTokens[eIdx])
			out.WriteString(`,"cap_urn":`)
			text(edge.CapUrn.String())
			out.WriteString(`,"is_loop":` + strconv.FormatBool(edge.IsLoop))
			out.WriteString(`,"retry":` + strconv.FormatUint(uint64(edge.Policy.Retry), 10))
			out.WriteString(`,"on_error":`)
			text(string(edge.Policy.EffectiveOnError()))
			// The machine steps the step was included through, outermost
			// first: what a view groups the steps of an included machine by.
			out.WriteString(`,"included_as":`)
			list(len(edge.IncludedAs), func(i int) { text(edge.IncludedAs[i]) })
			// Each binding says which arg (its URN and its name) and what
			// feeds it: a node of the strand, or a constant (a JSON value).
			out.WriteString(`,"assignment":`)
			list(len(edge.Assignment), func(i int) {
				binding := edge.Assignment[i]
				out.WriteString(`{"cap_arg_media_urn":`)
				text(binding.CapArgMediaUrn.String())
				out.WriteString(`,"arg_name":`)
				text(binding.ArgName)
				if binding.Source.IsConstant {
					out.WriteString(`,"constant":`)
					writeConstant(&out, binding.Source.Constant)
				} else {
					out.WriteString(`,"source_node":`)
					text(plan.nodeNames[binding.Source.Node])
				}
				out.WriteByte('}')
			})
			// Each output the step's wiring takes, and the node it goes to.
			out.WriteString(`,"outputs":`)
			list(len(edge.Outputs), func(i int) {
				output := edge.Outputs[i]
				out.WriteString(`{"output_media_urn":`)
				text(output.OutputMediaUrn.String())
				out.WriteString(`,"output_name":`)
				text(output.OutputName)
				out.WriteString(`,"node":`)
				text(plan.nodeNames[output.Node])
				out.WriteByte('}')
			})
			out.WriteByte('}')
		})

		out.WriteString(`,"input_anchor_nodes":`)
		list(len(strand.inputAnchorIds), func(i int) { text(plan.nodeNames[strand.inputAnchorIds[i]]) })
		out.WriteString(`,"output_anchor_nodes":`)
		list(len(strand.outputAnchorIds), func(i int) { text(plan.nodeNames[strand.outputAnchorIds[i]]) })
		out.WriteByte('}')
	})
	out.WriteByte('}')
	return out.String()
}
