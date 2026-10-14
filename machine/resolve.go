package machine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// Anchor realization for MachineStrands.
//
// This file turns either a planner-produced Strand or a parser-produced wiring
// set into a fully resolved MachineStrand: a connected sub-graph whose every
// edge has an explicit source-to-cap-arg assignment and names the outputs it
// takes, anchored by the input root URNs and output leaf URNs of the strand.
//
// # Binding a wiring's members
//
// Members that name an arg bind first, each to the arg of that name; the
// source must conform to what that arg ACCEPTS. A name written more than once
// gathers, and only a sequence arg may be gathered into. A constant is held to
// the validation of the arg's media definition.
//
// The members that name no arg are matched by type over the args no name
// bound, by minimum-cost assignment in two phases of strict precedence:
//
//   - Cost of pairing source s with arg a: spec(s) - spec(a) if s conforms
//     to what a accepts — how much more the source says than the arg asks;
//     otherwise the pair is impossible.
//   - Phase 1 — product: each source claims a distinct arg. Whenever such an
//     assignment exists, this phase decides.
//   - Phase 2 — gather: only when no such assignment exists, a sequence arg
//     may absorb several conforming sources (scalar args still take at most
//     one).
//   - The minimum-cost assignment of the deciding phase must be unique: a tie
//     is ErrAbstractionAmbiguousMachineNotation. Where a member stands in its
//     group is never a tiebreaker (it only fixes the item order within one
//     gathered arg).

// namedSource is a member that says which arg it is for, by the arg's NAME in
// the cap definition.
type namedSource struct {
	name   string
	source BindingSource
}

// wiringTarget is an output taken: the output's NAME in the cap definition
// (without a name, the main output) and the node it goes to.
type wiringTarget struct {
	name    string
	hasName bool
	node    NodeId
}

// preInternedWiring is one wiring after the caller has pre-interned its source
// and target slots into NodeIds against a parallel nodes slice.
//
// The resolver does no URN-based interning of its own: two distinct NodeIds
// whose URNs are equivalent stay distinct, which is what lets the notation
// parser honor the identity of node names.
type preInternedWiring struct {
	// tokenId is the originating step's stable identity, carried onto the
	// resulting MachineEdge (see MachineEdge.TokenId).
	tokenId planner.StepToken
	capUrn  *urn.CapUrn
	// namedSources bind first, in the order written.
	namedSources []namedSource
	// sourceNodeIds name no arg. Position carries no semantics — the matching
	// assigns each to a cap arg no named member bound.
	sourceNodeIds []NodeId
	// targets are the outputs taken. Empty: a sink.
	targets []wiringTarget
	// policy is what the step does when it fails.
	policy StepPolicy
	// includedAs is the machine steps this step was included through,
	// outermost first; empty for a step the machine wrote itself.
	includedAs []string
}

// resolveStrand converts a planner-produced Strand into a single MachineStrand.
//
// Every cap step names each of its inputs' producers explicitly (StrandInput or
// another step's TokenId) — there is no positional predecessor assumption, so
// fan-out and convergence both wire correctly. ForEach and Collect steps are
// elided here; IsLoop is derived from cardinality in resolvePreInterned.
func resolveStrand(
	strand *planner.Strand,
	registry *fabric.FabricRegistry,
	strandIndex int,
) (*MachineStrand, *MachineAbstractionError) {
	var nodes []*urn.MediaUrn
	var preInterned []preInternedWiring

	// Producer of each node, by producing step's stable TokenId -> its output
	// NodeId.
	producerNode := make(map[planner.StepToken]NodeId)
	// The single shared strand input anchor node, allocated on first
	// StrandInput reference and refined to the most specific consuming
	// FromSpec.
	var strandInputNode *NodeId

	for _, step := range strand.Steps {
		switch step.StepType {
		case planner.StepTypeCap:
			sourceNodeIds := make([]NodeId, 0, len(step.Inputs))
			for _, input := range step.Inputs {
				var sourceId NodeId
				switch input.Source.Kind {
				case planner.ArgSourceStrandInput:
					if strandInputNode != nil {
						id := *strandInputNode
						if step.FromSpec.Specificity() > nodes[id].Specificity() {
							nodes[id] = step.FromSpec
						}
						sourceId = id
					} else {
						id := NodeId(len(nodes))
						nodes = append(nodes, step.FromSpec)
						strandInputNode = &id
						sourceId = id
					}
				case planner.ArgSourceStep:
					id, ok := producerNode[input.Source.TokenId]
					if !ok {
						return nil, disconnectedStrandError(strandIndex)
					}
					sourceId = id
				}
				sourceNodeIds = append(sourceNodeIds, sourceId)
			}

			// Target: always a fresh position.
			targetId := NodeId(len(nodes))
			nodes = append(nodes, step.ToSpec)

			preInterned = append(preInterned, preInternedWiring{
				tokenId:       step.TokenId,
				capUrn:        step.CapUrnVal,
				sourceNodeIds: sourceNodeIds,
				// A strand step produces its cap's main output.
				targets: []wiringTarget{{node: targetId}},
			})

			producerNode[step.TokenId] = targetId

		case planner.StepTypeForEach, planner.StepTypeCollect:
			// Cardinality transitions are elided — caps name their producers
			// directly, and IsLoop is derived from cardinality in
			// resolvePreInterned, so these steps carry no wiring here.
		}
	}

	if len(preInterned) == 0 {
		return nil, noCapabilityStepsError()
	}

	return resolvePreInterned(nodes, preInterned, registry, strandIndex)
}

func mustMedia(text, what string) *urn.MediaUrn {
	parsed, err := urn.NewMediaUrnFromString(text)
	if err != nil {
		panic(fmt.Sprintf("cap registry invariant: %s is a valid MediaUrn: %q: %v", what, text, err))
	}
	return parsed
}

// resolvePreInterned resolves a pre-interned wiring set into a MachineStrand.
//
// The caller has already allocated NodeIds for every distinct data position.
//
// Steps:
//  0. The outputs each wiring takes, and with them the cardinality of every node.
//  1. Per-wiring binding of sources to cap args.
//  2. Which steps map (IterationOf).
//  3. Cycle detection and canonical edge ordering.
//  4. Anchor computation (NodeIds with no producer / no consumer in the strand).
func resolvePreInterned(
	nodes []*urn.MediaUrn,
	wirings []preInternedWiring,
	registry *fabric.FabricRegistry,
	strandIndex int,
) (*MachineStrand, *MachineAbstractionError) {
	return resolveWirings(nodes, nil, wirings, registry, strandIndex)
}

// resolveNamedWirings is resolvePreInterned for wirings read from notation:
// nodeNames is the name the text gave each node, indexed by NodeId, so that a
// refusal about a member can say which member.
func resolveNamedWirings(
	nodes []*urn.MediaUrn,
	nodeNames []string,
	wirings []preInternedWiring,
	registry *fabric.FabricRegistry,
	strandIndex int,
) (*MachineStrand, *MachineAbstractionError) {
	if len(nodes) != len(nodeNames) {
		panic("every node of a strand read from notation has a name")
	}
	return resolveWirings(nodes, nodeNames, wirings, registry, strandIndex)
}

func resolveWirings(
	nodes []*urn.MediaUrn,
	nodeNames []string,
	wirings []preInternedWiring,
	registry *fabric.FabricRegistry,
	strandIndex int,
) (*MachineStrand, *MachineAbstractionError) {
	if len(wirings) == 0 {
		return nil, noCapabilityStepsError()
	}

	// Step 0: the outputs each wiring takes, by the names its targets wrote,
	// and with them the cardinality of every node.
	//
	// A node holds a sequence, IN THE SCOPE IT LIVES IN, iff the output that
	// produces it is declared a sequence: one run of the step yields a
	// sequence. (A node a mapped step produces has one such value per item;
	// what it is from outside the map is IterationStructure's to say.) An
	// input of the machine is one value: notation has no run shape.
	nodeIsSequence := make([]bool, len(nodes))
	definitions := make([]*cap.Cap, len(wirings))
	wiringOutputs := make([][]EdgeOutput, len(wirings))
	for index, wiring := range wirings {
		capUrn := wiring.capUrn.String()
		definition, cached := registry.GetCachedCap(capUrn)
		if !cached {
			return nil, unknownCapError(capUrn)
		}
		definitions[index] = definition
		outputs := make([]EdgeOutput, 0, len(wiring.targets))
		for _, target := range wiring.targets {
			var output *cap.CapOutput
			if !target.hasName {
				// The unnamed target takes the main output.
				output = definition.MainOutput()
				if output == nil {
					return nil, noMainOutputError(strandIndex, capUrn)
				}
			} else {
				output = definition.OutputNamed(target.name)
				if output == nil {
					return nil, unknownOutputNameError(strandIndex, capUrn, target.name, outputNames(definition))
				}
			}
			outputMediaUrn := mustMedia(output.MediaUrn, "every output media_urn")
			for _, taken := range outputs {
				if taken.OutputMediaUrn.IsEquivalent(outputMediaUrn) {
					return nil, outputTakenTwiceError(strandIndex, capUrn, output.Name)
				}
			}
			nodeIsSequence[target.node] = output.IsSequence
			outputs = append(outputs, EdgeOutput{
				OutputMediaUrn: outputMediaUrn,
				OutputName:     output.Name,
				Node:           target.node,
			})
		}
		// Canonical order, like the assignment's.
		sort.SliceStable(outputs, func(a, b int) bool {
			return outputs[a].OutputMediaUrn.String() < outputs[b].OutputMediaUrn.String()
		})
		wiringOutputs[index] = outputs
	}

	// Step 1: per-wiring binding of sources to cap args. Named members bind
	// first; the rest are matched by type over the args no name bound.
	void := mustMedia(standard.MediaVoid, "media:void")
	shapes := make([]StepShape, len(wirings))
	assignments := make([][]EdgeAssignmentBinding, len(wirings))
	for index, wiring := range wirings {
		definition := definitions[index]
		capUrn := wiring.capUrn.String()
		capInSpec, err := urn.NewMediaUrnFromString(definition.Urn.InSpec())
		if err != nil {
			return nil, runtimeMediaInferenceError(strandIndex, capUrn, definition.Urn.InSpec(),
				fmt.Sprintf("cap `in=` is not a valid media URN: %v", err))
		}
		primaryArgIndex := -1
		if !capInSpec.IsEquivalent(void) {
			for i := range definition.Args {
				if definition.Args[i].IsMainInput(capInSpec) {
					primaryArgIndex = i
					break
				}
			}
			if primaryArgIndex < 0 {
				return nil, capDoesNotDeclareInputError(strandIndex, capUrn)
			}
		}

		// Every arg, by what it ACCEPTS (the type a source must conform to:
		// its stdin source URN for the main input, its `accepts` when it gives
		// one, its own URN otherwise) and by its SLOT (its media_urn, the
		// identity a binding records and the URN its stream is delivered
		// under). How an arg is supplied on a command line plays no part:
		// every arg can be bound by a wiring.
		argAccepts := make([]*urn.MediaUrn, len(definition.Args))
		argSlots := make([]*urn.MediaUrn, len(definition.Args))
		for i := range definition.Args {
			argAccepts[i] = mustMedia(definition.Args[i].AcceptedUrn(), "what every arg accepts")
			argSlots[i] = mustMedia(definition.Args[i].MediaUrn, "every cap arg media_urn")
		}

		// Named members bind first.
		bindings := make([]EdgeAssignmentBinding, 0, len(wiring.namedSources)+len(wiring.sourceNodeIds))
		namedCount := make([]int, len(definition.Args))
		for _, named := range wiring.namedSources {
			argIndex := -1
			for i := range definition.Args {
				if definition.Args[i].Name == named.name {
					argIndex = i
					break
				}
			}
			if argIndex < 0 {
				return nil, unknownArgNameError(strandIndex, capUrn, named.name, argNames(definition))
			}
			if named.source.IsConstant {
				if refused := checkConstant(registry, capUrn, &definition.Args[argIndex], named.source.Constant, strandIndex); refused != nil {
					return nil, refused
				}
			} else {
				sourceUrn := nodes[named.source.Node]
				if !sourceUrn.ConformsTo(argAccepts[argIndex]) {
					return nil, sourceDoesNotConformToArgError(strandIndex, capUrn, named.name,
						argAccepts[argIndex].String(), sourceUrn.String())
				}
			}
			namedCount[argIndex]++
			bindings = append(bindings, EdgeAssignmentBinding{
				CapArgMediaUrn: argSlots[argIndex],
				ArgName:        definition.Args[argIndex].Name,
				Source:         named.source,
			})
		}
		for i, count := range namedCount {
			if count > 1 && !definition.Args[i].IsSequence {
				return nil, argBoundTwiceError(strandIndex, capUrn, definition.Args[i].Name, count)
			}
		}

		// The unnamed members are matched by type over the args no name bound.
		var free []int
		for i := range definition.Args {
			if namedCount[i] == 0 {
				free = append(free, i)
			}
		}
		freeAccepts := make([]*urn.MediaUrn, len(free))
		freeIsSequence := make([]bool, len(free))
		// The main input, when no named member bound it.
		mustBind := -1
		for i, argIndex := range free {
			freeAccepts[i] = argAccepts[argIndex]
			freeIsSequence[i] = definition.Args[argIndex].IsSequence
			if argIndex == primaryArgIndex {
				mustBind = i
			}
		}
		sourceUrns := make([]*urn.MediaUrn, len(wiring.sourceNodeIds))
		for i, id := range wiring.sourceNodeIds {
			sourceUrns[i] = nodes[id]
		}

		if len(sourceUrns) > 0 {
			matched, failure := matchSourcesToArgs(sourceUrns, freeAccepts, freeIsSequence, mustBind)
			if failure != nil {
				if !failure.ambiguous {
					return nil, unmatchedSourceError(strandIndex, capUrn, sourceUrns[failure.source].String())
				}
				// Which members the types leave open, by the names the text
				// gave them, and the args each could go to.
				members := make([]UndecidedMember, len(failure.undecided))
				for i, each := range failure.undecided {
					members[i] = UndecidedMember{SourceUrn: sourceUrns[each.source].String()}
					if nodeNames != nil {
						members[i].Node = nodeNames[wiring.sourceNodeIds[each.source]]
					}
					for _, arg := range each.args {
						members[i].CouldBe = append(members[i].CouldBe, definition.Args[free[arg]].Name)
					}
				}
				return nil, ambiguousNotationError(strandIndex, capUrn, members)
			}
			// The matching answers in URNs. Each accepted URN is translated
			// back to its slot identity, and each source URN to its position
			// among the wiring's unnamed sources, walking the positions not
			// yet taken so that two sources that share a URN stay two.
			consumed := make([]bool, len(wiring.sourceNodeIds))
			for _, pair := range matched {
				argAt, sourceAt := -1, -1
				for i, accepts := range freeAccepts {
					if accepts.IsEquivalent(pair.arg) {
						argAt = i
						break
					}
				}
				for position, id := range wiring.sourceNodeIds {
					if !consumed[position] && nodes[id].IsEquivalent(pair.source) {
						sourceAt = position
						break
					}
				}
				if argAt < 0 || sourceAt < 0 {
					panic("the matching answered in a URN it was not given")
				}
				consumed[sourceAt] = true
				bindings = append(bindings, EdgeAssignmentBinding{
					CapArgMediaUrn: argSlots[free[argAt]],
					ArgName:        definition.Args[free[argAt]].Name,
					Source:         NodeSource(wiring.sourceNodeIds[sourceAt]),
				})
			}
		}

		// The bindings are in the order they were made: the named ones as
		// written, then the matched ones. To keep the canonical equivalence
		// comparison stable, re-sort by slot identity; the sort is stable, so
		// several bindings gathered onto one arg keep their declaration order
		// — the gathered sequence's item order.
		sort.SliceStable(bindings, func(a, b int) bool {
			return bindings[a].CapArgMediaUrn.String() < bindings[b].CapArgMediaUrn.String()
		})

		// A cap that takes an input is given it.
		if primaryArgIndex >= 0 {
			given := false
			for _, binding := range bindings {
				if binding.CapArgMediaUrn.IsEquivalent(argSlots[primaryArgIndex]) {
					given = true
				}
			}
			if !given {
				return nil, mainInputUnboundError(strandIndex, capUrn, definition.Args[primaryArgIndex].Name)
			}
		}

		// What the step is given, as far as iteration is concerned: each node
		// and whether the arg it is bound to takes a sequence. A constant is
		// one value wherever the step runs.
		shape := StepShape{}
		for _, binding := range bindings {
			if binding.Source.IsConstant {
				continue
			}
			for i, slot := range argSlots {
				if slot.IsEquivalent(binding.CapArgMediaUrn) {
					shape.Members = append(shape.Members, MemberShape{
						Node:          binding.Source.Node,
						ArgIsSequence: definition.Args[i].IsSequence,
					})
					break
				}
			}
		}
		for _, output := range wiringOutputs[index] {
			shape.Outputs = append(shape.Outputs, OutputShape{Node: output.Node, IsSequence: nodeIsSequence[output.Node]})
		}
		shapes[index] = shape
		assignments[index] = bindings
	}

	// Step 2: which steps map. Iteration is DERIVED from cardinality, never
	// authored (docs 09 §Iteration); IterationOf is the one place the rule is
	// computed. Notation has no run, so an input of the machine is one value
	// here.
	iteration, cyclic := IterationOf(shapes, func(NodeId) bool { return false })
	if cyclic != nil {
		return nil, cyclicStrandError(strandIndex)
	}

	indexedEdges := make([]*MachineEdge, len(wirings))
	for index, wiring := range wirings {
		indexedEdges[index] = &MachineEdge{
			TokenId:    wiring.tokenId,
			CapUrn:     wiring.capUrn,
			Assignment: assignments[index],
			Outputs:    wiringOutputs[index],
			Policy:     wiring.policy,
			IncludedAs: append([]string(nil), wiring.includedAs...),
			IsLoop:     iteration.Steps()[index].Maps,
		}
	}

	// Step 3: canonical edge order. The steps are acyclic: every one of them
	// was placed above.
	canonicalOrder := topoSort(indexedEdges, nodes)
	edges := make([]*MachineEdge, len(canonicalOrder))
	for i, idx := range canonicalOrder {
		edges[i] = indexedEdges[idx]
	}

	// Step 4: anchor computation.
	produced := make(map[NodeId]bool)
	consumed := make(map[NodeId]bool)
	for _, e := range edges {
		for _, node := range e.OutputNodes() {
			produced[node] = true
		}
		for _, node := range e.SourceNodes() {
			consumed[node] = true
		}
	}
	// Every node is a source or an output of some step — that is what makes
	// it a node of the strand. One nothing produces is an input of the
	// strand; one nothing consumes is an output of it.
	var inputAnchorIds []NodeId
	var outputAnchorIds []NodeId
	for id := NodeId(0); id < NodeId(len(nodes)); id++ {
		if !produced[id] && !consumed[id] {
			panic(fmt.Sprintf("node %d of strand %d is touched by no wiring", id, strandIndex))
		}
		if !produced[id] {
			inputAnchorIds = append(inputAnchorIds, id)
		}
		if !consumed[id] {
			outputAnchorIds = append(outputAnchorIds, id)
		}
	}
	// Sort anchors by canonical (URN, NodeId) order so the result is stable
	// across node-allocation orders that yield equivalent strands.
	byUrnThenId := func(ids []NodeId) {
		sort.Slice(ids, func(a, b int) bool {
			ua, ub := nodes[ids[a]].String(), nodes[ids[b]].String()
			if ua != ub {
				return ua < ub
			}
			return ids[a] < ids[b]
		})
	}
	byUrnThenId(inputAnchorIds)
	byUrnThenId(outputAnchorIds)

	return newMachineStrand(nodes, edges, inputAnchorIds, outputAnchorIds, nodeIsSequence), nil
}

// checkConstant holds a constant to the validation of the arg it is bound to:
// the rules and the schema the arg's media definition declares. A constant is
// a value like any other the arg is given; it is known when the machine is
// resolved, so that is when it is checked.
func checkConstant(
	registry *fabric.FabricRegistry,
	capUrn string,
	arg *cap.CapArg,
	value any,
	strandIndex int,
) *MachineAbstractionError {
	refused := func(reason string) *MachineAbstractionError {
		return constantFailsValidationError(strandIndex, capUrn, arg.Name, constantText(value), reason)
	}
	// Without the definition there is nothing to hold the constant to, and a
	// constant nobody checked is not bound.
	definition := registry.GetCachedMediaDef(arg.MediaUrn)
	if definition == nil {
		return refused(fmt.Sprintf(
			"the media definition of '%s' is not available, so the constant cannot be validated", arg.MediaUrn))
	}
	if definition.Validation != nil {
		rule, err := definition.Validation.Violation(value)
		if err != nil {
			return refused(err.Error())
		}
		if rule != "" {
			return refused("it breaks the rule: " + rule)
		}
	}
	if definition.Schema != nil {
		if details := cap.SchemaViolations(definition.Schema, value); len(details) > 0 {
			return refused("it does not match the schema: " + strings.Join(details, "; "))
		}
	}
	return nil
}

// matchedPair is one answer of the matching: an arg, by the URN it accepts,
// and the source placed on it.
type matchedPair struct {
	arg    *urn.MediaUrn
	source *urn.MediaUrn
}

// matchFailure is why the unnamed members of a wiring could not be placed. It
// speaks in positions — of the sources and args the matching was given —
// because what a source or an arg is CALLED is its caller's to say.
type matchFailure struct {
	// ambiguous: the types leave more than one answer, and undecided says
	// about which sources. Otherwise the source at `source` fits no arg left
	// for it.
	ambiguous bool
	source    int
	// undecided is each source the minimum-cost assignments place
	// differently, and the args they place it on, in the order the args were
	// given.
	undecided []undecidedSource
}

type undecidedSource struct {
	source int
	args   []int
}

// matchSourcesToArgs matches a wiring's sources to a cap's args by minimum
// total specificity-distance, with a uniqueness requirement.
//
// Two phases, in strict precedence order:
//
//  1. Product — each source claims a distinct arg. It wins whenever such an
//     assignment exists, including the hard failure on tied minimum-cost
//     assignments.
//  2. Gather — only when no such assignment exists (more sources than args, or
//     a Hall violation): a sequence arg may absorb several conforming sources;
//     a scalar arg still takes at most one. The minimum-cost such assignment
//     must be unique.
//
// `mustBind` is the index of the arg an assignment MUST bind — the cap's main
// input, when these sources are what can feed it — or -1.
//
// The pairs come sorted by arg URN (stably: several sources gathered into one
// arg keep their declaration order, which fixes the gathered sequence's item
// order).
func matchSourcesToArgs(
	sources []*urn.MediaUrn,
	args []*urn.MediaUrn,
	argIsSequence []bool,
	mustBind int,
) ([]matchedPair, *matchFailure) {
	hasSequenceArg := false
	for _, isSequence := range argIsSequence {
		hasSequenceArg = hasSequenceArg || isSequence
	}
	fitsSomething := func(source *urn.MediaUrn) bool {
		for _, arg := range args {
			if source.ConformsTo(arg) {
				return true
			}
		}
		return false
	}

	if len(sources) > len(args) && !hasSequenceArg {
		// Pigeonhole: at least one source has no arg slot and no sequence arg
		// exists to gather the surplus. The first source with no candidate
		// arg is the one named; when every source has one, at least one of
		// them MUST end up unmatched, and the first stands for it.
		for position, source := range sources {
			if !fitsSomething(source) {
				return nil, &matchFailure{source: position}
			}
		}
		return nil, &matchFailure{source: 0}
	}

	// cost[s][a] is the distance when sources[s] conforms to args[a], else
	// impossible.
	cost := make([][]int, len(sources))
	for s, source := range sources {
		if !fitsSomething(source) {
			return nil, &matchFailure{source: s}
		}
		cost[s] = make([]int, len(args))
		for a, arg := range args {
			cost[s][a] = impossible
			if source.ConformsTo(arg) {
				// The specificity distance: how much more the source says
				// than the arg asks. It is never negative — refining never
				// scores lower (04 §Predicates, tagScore_monotone).
				distance := source.Specificity() - arg.Specificity()
				if distance < 0 {
					panic(fmt.Sprintf("a source that conforms to an arg is never less specific than it: %s conforms to %s", source, arg))
				}
				cost[s][a] = distance
			}
		}
	}

	// An assignment binds the arg that MUST be bound. A cap that takes an
	// input is given it, so a way of placing the sources that leaves the main
	// input out is not an assignment at all; it does not tie with one that is.
	// This is what makes `series -> compare -> result` resolve when `compare`
	// has a second arg that accepts a series: the lone source is the main
	// input. It is a constraint, not a tiebreak: two sources that both fit the
	// main input and the other arg still tie.
	if mustBind >= 0 {
		reachable := false
		for s := range cost {
			reachable = reachable || cost[s][mustBind] != impossible
		}
		if !reachable {
			mustBind = -1
		}
	}

	search := assignmentSearch{cost: cost, mustBind: mustBind, bestCost: impossible}

	// Phase 1: product.
	if len(sources) <= len(args) {
		search.run(make([]bool, len(args)))
		if search.bestCost != impossible {
			// A tie among minimum-cost assignments is a hard ambiguity; it
			// never falls through to gather, which would mask it.
			return oneAssignment(search.best, sources, args)
		}
	}

	// Phase 2: gather.
	if !hasSequenceArg {
		// Every source has a candidate, but the candidates cannot all be
		// claimed by distinct args (a Hall violation). The first source stands
		// for the one that goes unmatched.
		return nil, &matchFailure{source: 0}
	}
	search.run(argIsSequence)
	if search.bestCost == impossible {
		return nil, &matchFailure{source: 0}
	}
	return oneAssignment(search.best, sources, args)
}

// oneAssignment is the one minimum-cost assignment, as pairs — or, when
// several tie, the sources they disagree about.
func oneAssignment(best [][]int, sources, args []*urn.MediaUrn) ([]matchedPair, *matchFailure) {
	if len(best) == 1 {
		return assignmentToPairs(best[0], sources, args), nil
	}
	failure := &matchFailure{ambiguous: true}
	for source := range sources {
		placed := map[int]bool{}
		for _, assignment := range best {
			placed[assignment[source]] = true
		}
		if len(placed) < 2 {
			continue
		}
		on := make([]int, 0, len(placed))
		for arg := range placed {
			on = append(on, arg)
		}
		sort.Ints(on)
		failure.undecided = append(failure.undecided, undecidedSource{source: source, args: on})
	}
	return nil, failure
}

// impossible marks a pairing that cannot be made, and a search that found no
// assignment.
const impossible = -1

// assignmentToPairs converts a source→arg assignment into pairs sorted by arg
// URN. The sort is stable and pairs are emitted in source order.
func assignmentToPairs(assignment []int, sources, args []*urn.MediaUrn) []matchedPair {
	pairs := make([]matchedPair, len(assignment))
	for s, a := range assignment {
		pairs[s] = matchedPair{arg: args[a], source: sources[s]}
	}
	sort.SliceStable(pairs, func(x, y int) bool { return pairs[x].arg.String() < pairs[y].arg.String() })
	return pairs
}

// assignmentSearch enumerates every source→arg assignment with a defined
// cost, keeping the minimum total cost and the assignments that reach it.
type assignmentSearch struct {
	cost     [][]int
	mustBind int
	bestCost int
	best     [][]int
}

// run enumerates under a capacity rule: canGather[a] gives arg `a` unbounded
// capacity (a sequence arg in the gather phase); otherwise the arg is claimed
// by at most one source.
func (s *assignmentSearch) run(canGather []bool) {
	args := 0
	if len(s.cost) > 0 {
		args = len(s.cost[0])
	}
	s.bestCost, s.best = impossible, nil
	s.place(0, make([]int, len(s.cost)), make([]bool, args), canGather)
}

func (s *assignmentSearch) place(source int, current []int, used, canGather []bool) {
	if source == len(s.cost) {
		// A placement that leaves out the arg that must be bound is not an
		// assignment.
		if s.mustBind >= 0 {
			bound := false
			for _, arg := range current {
				bound = bound || arg == s.mustBind
			}
			if !bound {
				return
			}
		}
		total := 0
		for at, arg := range current {
			total += s.cost[at][arg]
		}
		switch {
		case s.bestCost == impossible || total < s.bestCost:
			s.bestCost = total
			s.best = [][]int{append([]int(nil), current...)}
		case total == s.bestCost:
			s.best = append(s.best, append([]int(nil), current...))
		}
		return
	}
	for arg := range s.cost[source] {
		if (used[arg] && !canGather[arg]) || s.cost[source][arg] == impossible {
			continue
		}
		wasUsed := used[arg]
		used[arg] = true
		current[source] = arg
		s.place(source+1, current, used, canGather)
		used[arg] = wasUsed
	}
}

// topoSort performs Kahn's algorithm over the resolved data-flow dependency
// graph, returning the canonical ordering of edge indices.
//
// Edge B depends on edge A iff B is given a node A produces. The edges it is
// given are acyclic: steps that wait on one another are refused when the
// steps are placed (IterationOf), before any edge is made of them.
func topoSort(edges []*MachineEdge, nodes []*urn.MediaUrn) []int {
	n := len(edges)

	producersOf := make(map[NodeId][]int)
	for idx, e := range edges {
		for _, node := range e.OutputNodes() {
			producersOf[node] = append(producersOf[node], idx)
		}
	}

	indegree := make([]int, n)
	successors := make([][]int, n)
	for bIdx, b := range edges {
		for _, source := range b.SourceNodes() {
			for _, aIdx := range producersOf[source] {
				successors[aIdx] = append(successors[aIdx], bIdx)
				indegree[bIdx]++
			}
		}
	}

	result := make([]int, 0, n)
	ready := make([]int, 0)
	for i := 0; i < n; i++ {
		if indegree[i] == 0 {
			ready = append(ready, i)
		}
	}
	sortReady(ready, edges, nodes)

	for len(ready) > 0 {
		idx := ready[0]
		ready = ready[1:]
		result = append(result, idx)
		for _, succ := range successors[idx] {
			indegree[succ]--
			if indegree[succ] == 0 {
				ready = append(ready, succ)
				sortReady(ready, edges, nodes)
			}
		}
	}

	if len(result) < n {
		panic(fmt.Sprintf("%d of %d placed steps wait on one another", n-len(result), n))
	}
	return result
}

// sortReady sorts the ready set in canonical structural order so Kahn's
// algorithm produces a deterministic output. The order is:
//
//  1. cap URN
//  2. assignment (element-wise: the arg's slot URN, then the source — a node
//     by its URN, a constant by its text, after every node)
//  3. outputs (element-wise: the output's URN, then the URN of the node it
//     goes to)
//  4. the IsLoop flag
func sortReady(ready []int, edges []*MachineEdge, nodes []*urn.MediaUrn) {
	sourceKey := func(source BindingSource) (bool, string) {
		if source.IsConstant {
			return true, constantText(source.Constant)
		}
		return false, nodes[source.Node].String()
	}
	sort.SliceStable(ready, func(i, j int) bool {
		ea, eb := edges[ready[i]], edges[ready[j]]

		if capA, capB := ea.CapUrn.String(), eb.CapUrn.String(); capA != capB {
			return capA < capB
		}

		for k := 0; k < len(ea.Assignment) && k < len(eb.Assignment); k++ {
			ba, bb := ea.Assignment[k], eb.Assignment[k]
			if argA, argB := ba.CapArgMediaUrn.String(), bb.CapArgMediaUrn.String(); argA != argB {
				return argA < argB
			}
			constantA, keyA := sourceKey(ba.Source)
			constantB, keyB := sourceKey(bb.Source)
			if constantA != constantB {
				return !constantA
			}
			if keyA != keyB {
				return keyA < keyB
			}
		}
		if len(ea.Assignment) != len(eb.Assignment) {
			return len(ea.Assignment) < len(eb.Assignment)
		}

		for k := 0; k < len(ea.Outputs) && k < len(eb.Outputs); k++ {
			oa, ob := ea.Outputs[k], eb.Outputs[k]
			if outA, outB := oa.OutputMediaUrn.String(), ob.OutputMediaUrn.String(); outA != outB {
				return outA < outB
			}
			if nodeA, nodeB := nodes[oa.Node].String(), nodes[ob.Node].String(); nodeA != nodeB {
				return nodeA < nodeB
			}
		}
		if len(ea.Outputs) != len(eb.Outputs) {
			return len(ea.Outputs) < len(eb.Outputs)
		}

		return !ea.IsLoop && eb.IsLoop
	})
}
