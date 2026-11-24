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

// Machine notation parser: the grammar, then anchor realization.
//
// A two-phase pipeline:
//
//  1. Lexical / grammatical — syntax.go reads the text into statements.
//     Failures here surface as MachineSyntaxError.
//  2. Resolution — the wirings are partitioned into connected components (one
//     per maximal set of wirings sharing node names), each component is given
//     to resolvePreInterned, and the resulting MachineStrands are assembled
//     into a Machine in the order each component first appears. Failures here
//     surface as MachineAbstractionError.
//
// A wiring is `sources -> step -> targets`: members that may name their arg
// or be constants, a step with an optional policy, and targets that may name
// the output they take (capdag/docs/09-machine-notation.md).
//
// # The media URN of a node
//
// Every node has one URN, derived from the wirings that touch it; where a
// member stands in its group never decides it (09 §The URN of a node):
//
//   - a node some step produces has the URN of what that output carries;
//   - a node nothing produces, as a named member, has the accepted type of
//     the arg it names;
//   - as an unnamed member that nothing else gives a URN, it has the accepted
//     type of the step's main input — at most one such member per wiring, and
//     only while no named member bound the main input
//     (ErrAbstractionUntypedMachineInput otherwise);
//   - of two URNs given to one node, one must be inside the other
//     (ErrAbstractionNodeMediaConflict otherwise); the node has the narrower.

// rawValue is what a member of a wiring gives its arg: a node, by the name
// the text gives it, or a constant.
type rawValue struct {
	isConstant bool
	node       string
	constant   any
}

// rawMember is one member of a wiring's sources.
type rawMember struct {
	// name is the arg the member names, when it names one.
	name    string
	hasName bool
	value   rawValue
}

// rawTarget is one target: the output it names (without a name, the main
// output) and the node it goes to.
type rawTarget struct {
	name    string
	hasName bool
	node    string
}

// rawWiring is one wiring as it comes off the syntax tree, with raw alias
// names.
type rawWiring struct {
	// sources are the members, in the order written. The order decides
	// nothing but the item order of a gather.
	sources  []rawMember
	capAlias string
	policy   StepPolicy
	targets  []rawTarget
	// position is the index of this wiring's statement in the text. A step
	// written in from an included machine stands where the machine step stood.
	position int
	// includedAs is the machine steps this step was included through,
	// outermost first.
	includedAs []string
}

// sourceNodes returns the name of every node this wiring consumes, in the
// order written.
func (w *rawWiring) sourceNodes() []string {
	var nodes []string
	for _, member := range w.sources {
		if !member.value.isConstant {
			nodes = append(nodes, member.value.node)
		}
	}
	return nodes
}

// nodes returns the name of every node this wiring touches.
func (w *rawWiring) nodes() []string {
	nodes := w.sourceNodes()
	for _, target := range w.targets {
		nodes = append(nodes, target.node)
	}
	return nodes
}

// unnamedNodes returns the nodes of the members that name no arg.
func (w *rawWiring) unnamedNodes() []string {
	var nodes []string
	for _, member := range w.sources {
		if !member.hasName && !member.value.isConstant {
			nodes = append(nodes, member.value.node)
		}
	}
	return nodes
}

// StrandNodeNames maps each node name the text wrote to the NodeId the parser
// allocated for it, for one strand.
type StrandNodeNames = map[string]NodeId

// headerCap is the cap a step alias stands for, and the statement that says so.
type headerCap struct {
	capUrn   *urn.CapUrn
	position int
}

// ParseMachine parses machine notation into a Machine, discarding the
// per-strand node names.
func ParseMachine(input string, registry *fabric.FabricRegistry) (*Machine, *MachineParseError) {
	m, _, err := ParseMachineWithNodeNames(input, registry)
	return m, err
}

// ParseMachineWithNodeNames parses machine notation into a Machine and, per
// strand (in strand order), the names the text gave its nodes.
//
// The resolver works on NodeIds; a host addresses a machine's inputs and
// outputs by the names as written, and this is where it gets them.
func ParseMachineWithNodeNames(input string, registry *fabric.FabricRegistry) (*Machine, []StrandNodeNames, *MachineParseError) {
	return ParseMachineWithSources(input, registry, NoMachineSource{}, nil)
}

// ParseMachineWithSources is ParseMachineWithNodeNames, resolving the machines
// its machine headers name through `source`. `id` is what the source knows the
// machine being parsed by, when it knows it: the references it writes are
// resolved relative to it, and an inclusion of it from inside is a circle.
//
// What comes back is the inlined whole: every step of every included machine
// is a step of this one, and no reference is left
// (capdag/docs/09-machine-notation.md, §Machines in step position).
func ParseMachineWithSources(
	input string,
	registry *fabric.FabricRegistry,
	source MachineSource,
	id *string,
) (*Machine, []StrandNodeNames, *MachineParseError) {
	var including []string
	if id != nil {
		including = append(including, *id)
	}
	return parseProgram(input, registry, source, id, &including)
}

// parseProgram parses one machine. `including` is every machine whose parse
// this one is inside of, this one last when it has an id.
func parseProgram(
	input string,
	registry *fabric.FabricRegistry,
	source MachineSource,
	id *string,
	including *[]string,
) (*Machine, []StrandNodeNames, *MachineParseError) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, nil, syntaxParseError(emptyError())
	}

	// Phase 1: the text, read into statements.
	statements, syntaxErr := parseSyntax(input)
	if syntaxErr != nil {
		return nil, nil, syntaxParseError(syntaxErr)
	}

	// Phase 2: headers and wirings.
	type machineHeader struct{ alias, reference string }
	type declaration struct {
		alias    string
		position int
	}
	aliasMap := map[string]headerCap{}
	var machineHeaders []machineHeader
	// Every alias a header declares, in the order written: cap headers and
	// machine headers name steps in one space.
	var declared []declaration
	var wirings []rawWiring
	for _, statement := range statements {
		switch statement.kind {
		case syntaxCapHeader:
			parsed, err := urn.NewCapUrnFromString(statement.capUrn)
			if err != nil {
				return nil, nil, syntaxParseError(invalidCapUrnError(statement.alias, err.Error()))
			}
			declared = append(declared, declaration{statement.alias, statement.position})
			if _, again := aliasMap[statement.alias]; !again {
				aliasMap[statement.alias] = headerCap{capUrn: parsed, position: statement.position}
			}
		case syntaxMachineHeader:
			declared = append(declared, declaration{statement.alias, statement.position})
			machineHeaders = append(machineHeaders, machineHeader{statement.alias, statement.reference})
		case syntaxWiringStatement:
			wiring := rawWiring{
				capAlias: statement.wiring.step,
				policy:   statement.wiring.policy,
				position: statement.position,
			}
			for _, member := range statement.wiring.sources {
				wiring.sources = append(wiring.sources, rawMember{
					name:    member.name,
					hasName: member.hasName,
					value:   rawValue{isConstant: member.isConstant, node: member.node, constant: member.constant},
				})
			}
			for _, target := range statement.wiring.targets {
				wiring.targets = append(wiring.targets, rawTarget{name: target.name, hasName: target.hasName, node: target.node})
			}
			wirings = append(wirings, wiring)
		}
	}

	// Phase 3: no alias is declared twice.
	firstDeclared := map[string]int{}
	for _, each := range declared {
		if first, again := firstDeclared[each.alias]; again {
			return nil, nil, syntaxParseError(duplicateAliasError(each.alias, first))
		}
		firstDeclared[each.alias] = each.position
	}

	if len(wirings) == 0 && len(declared) > 0 {
		return nil, nil, syntaxParseError(noEdgesError())
	}
	if len(wirings) == 0 {
		return nil, nil, syntaxParseError(emptyError())
	}

	// Phase 3a: the machines the machine headers name, each resolved on its
	// own before anything here is. A machine is what it is alone: where it is
	// included changes nothing about it.
	machines := map[string]*includedMachine{}
	for _, header := range machineHeaders {
		loaded, sourceErr := source.Load(header.reference, id)
		if sourceErr != nil {
			if sourceErr.NoSource {
				return nil, nil, syntaxParseError(machineReferenceWithoutSourceError(header.alias, header.reference))
			}
			return nil, nil, abstractionParseError(unknownMachineReferenceError(header.alias, header.reference, sourceErr.Details))
		}
		for at, machine := range *including {
			if machine == loaded.Id {
				chain := append(append([]string(nil), (*including)[at:]...), loaded.Id)
				return nil, nil, abstractionParseError(cyclicMachineReferenceError(chain))
			}
		}
		*including = append(*including, loaded.Id)
		machine, names, parseErr := parseProgram(loaded.Notation, registry, source, &loaded.Id, including)
		*including = (*including)[:len(*including)-1]
		if parseErr != nil {
			// A circle is one fact about the whole, said once: it is not a
			// defect of each machine on the way round.
			if parseErr.Abstraction != nil && parseErr.Abstraction.Kind == ErrAbstractionCyclicMachineReference {
				return nil, nil, parseErr
			}
			return nil, nil, abstractionParseError(invalidIncludedMachineError(header.alias, header.reference, parseErr))
		}
		machines[header.alias] = includedMachineOf(machine, names)
	}

	// Phase 3b: cap-alias resolution against the fabric registry.
	//
	// A step name not defined by a local header is taken to be a fabric cap
	// alias and resolved through the registry — an identifier with no local
	// definition is resolved as an alias before it is declared undefined. The
	// resolved cap URN is injected into aliasMap as if a header had defined
	// it. Media URNs never appear in a wiring (they are implicit), so only cap
	// aliases are resolved here; an alias that points at a media URN in step
	// position is a hard error. The lookup is the synchronous in-memory cache
	// (ResolveAliasCached); a name that resolves to nothing is left for
	// Phase 4's undefined-alias error.
	seenUnresolved := map[string]bool{}
	for _, w := range wirings {
		if _, local := aliasMap[w.capAlias]; local {
			continue
		}
		if _, machine := machines[w.capAlias]; machine || seenUnresolved[w.capAlias] {
			continue
		}
		seenUnresolved[w.capAlias] = true
		// No alias by this name in the registry → Phase 4's undefined-alias
		// error. An alias the registry has and that could not be fetched is
		// reported as what kept it, never as undefined.
		target, known, err := registry.ResolveAliasCached(w.capAlias)
		if err != nil {
			return nil, nil, abstractionParseError(definitionUnavailableError(w.capAlias, err))
		}
		if !known {
			continue
		}
		resolved, err := urn.NewCapUrnFromString(target)
		if err != nil {
			return nil, nil, syntaxParseError(aliasNotACapError(w.capAlias, target))
		}
		// A synthetic position past every real statement.
		aliasMap[w.capAlias] = headerCap{capUrn: resolved, position: len(declared) + len(wirings) + len(aliasMap)}
	}

	// Phase 4: every step is defined, and no node bears a step's name.
	isStep := func(name string) bool {
		if _, local := aliasMap[name]; local {
			return true
		}
		_, machine := machines[name]
		return machine
	}
	for i := range wirings {
		w := &wirings[i]
		if !isStep(w.capAlias) {
			return nil, nil, syntaxParseError(undefinedAliasError(w.capAlias))
		}
		for _, node := range w.nodes() {
			if isStep(node) {
				return nil, nil, syntaxParseError(nodeAliasCollisionError(node, node))
			}
		}
	}

	// Phase 4a: every step that is a machine is written out as the steps of
	// that machine. From here on the wirings are of caps alone.
	includedMedia := map[string]*urn.MediaUrn{}
	for i := range wirings {
		if _, machine := machines[wirings[i].capAlias]; machine {
			written, err := includeMachines(wirings, aliasMap, machines, includedMedia, registry)
			if err != nil {
				return nil, nil, err
			}
			wirings = written
			break
		}
	}
	noMachines := map[string]*includedMachine{}

	// Phase 5: connected components, in the order each first appears.
	components := componentsOf(wirings)

	// Phase 6: per-component node typing, pre-interning and resolution.
	//
	// For each connected component (= strand), every node is given its URN
	// (typeNodes), then NodeIds are allocated in the order node names are
	// encountered (walking the wirings in their textual order). Two distinct
	// node names that happen to share a media URN stay distinct NodeIds —
	// that's the parser's identity contract.
	strands := make([]*MachineStrand, 0, len(components))
	strandNodeNames := make([]StrandNodeNames, 0, len(components))
	for strandIndex, memberIndices := range components {
		members := make([]*rawWiring, len(memberIndices))
		for i, index := range memberIndices {
			members[i] = &wirings[index]
		}

		nodeMedia, err := typeNodes(members, aliasMap, noMachines, includedMedia, registry, strandIndex)
		if err != nil {
			return nil, nil, err
		}

		var nodes []*urn.MediaUrn
		nameToId := StrandNodeNames{}
		intern := func(name string) NodeId {
			if id, known := nameToId[name]; known {
				return id
			}
			media, typed := nodeMedia[name]
			if !typed {
				panic("every node of the strand was given its URN: " + name)
			}
			id := NodeId(len(nodes))
			nodes = append(nodes, media)
			nameToId[name] = id
			return id
		}

		preInterned := make([]preInternedWiring, 0, len(members))
		for _, w := range members {
			wiring := preInternedWiring{
				// The notation path has no resolved-strand step to inherit
				// from, so each parsed wiring's edge gets its own freshly
				// minted stable identity.
				tokenId:    planner.MintStepToken(),
				capUrn:     aliasMap[w.capAlias].capUrn,
				policy:     w.policy,
				includedAs: append([]string(nil), w.includedAs...),
			}
			for _, member := range w.sources {
				switch {
				case member.hasName && member.value.isConstant:
					wiring.namedSources = append(wiring.namedSources, namedSource{name: member.name, source: ConstantSource(member.value.constant)})
				case member.hasName:
					wiring.namedSources = append(wiring.namedSources, namedSource{name: member.name, source: NodeSource(intern(member.value.node))})
				case member.value.isConstant:
					panic("the syntax refuses a constant that names no arg")
				default:
					wiring.sourceNodeIds = append(wiring.sourceNodeIds, intern(member.value.node))
				}
			}
			for _, target := range w.targets {
				wiring.targets = append(wiring.targets, wiringTarget{name: target.name, hasName: target.hasName, node: intern(target.node)})
			}
			preInterned = append(preInterned, wiring)
		}

		// The name the text gave each node, by NodeId: what a refusal about a
		// member names it by.
		namesById := make([]string, len(nodes))
		for name, id := range nameToId {
			namesById[id] = name
		}
		strand, absErr := resolveNamedWirings(nodes, namesById, preInterned, registry, strandIndex)
		if absErr != nil {
			return nil, nil, abstractionParseError(absErr)
		}
		strands = append(strands, strand)
		strandNodeNames = append(strandNodeNames, nameToId)
	}

	return fromResolvedStrands(strands), strandNodeNames, nil
}

// componentsOf returns the connected components of a wiring set: two wirings
// are in one iff a path of shared node names joins them. A wiring that touches
// no node at all (`() -> tick -> ()`) is a component of its own. Components
// come in the order each first appears; each lists its wirings in the order
// written.
func componentsOf(wirings []rawWiring) [][]int {
	n := len(wirings)
	union := newUnionFind(n)
	// Node name → the first wiring that touched it.
	nodeFirstWiring := map[string]int{}
	for index := range wirings {
		for _, node := range wirings[index].nodes() {
			if earlier, seen := nodeFirstWiring[node]; seen {
				union.union(earlier, index)
			} else {
				nodeFirstWiring[node] = index
			}
		}
	}
	groups := map[int][]int{}
	for index := 0; index < n; index++ {
		root := union.find(index)
		groups[root] = append(groups[root], index)
	}
	// Wirings were appended in ascending order, so each group is sorted and
	// its first member is its first appearance.
	components := make([][]int, 0, len(groups))
	for _, members := range groups {
		components = append(components, members)
	}
	sort.Slice(components, func(a, b int) bool { return components[a][0] < components[b][0] })
	return components
}

// includeMachines writes every step that is a machine out as the steps of that
// machine (09 §Machines in step position).
//
// A machine step binds like a cap step: a named member to the input of that
// name, the unnamed members to the inputs left by type — which is read off the
// wirings as they were written, machine steps and all, before anything is
// written out. A machine has no main input and no main output: an unnamed
// member with no type of its own has nothing to be matched by, and an unnamed
// target takes the one output the other targets leave.
//
// The steps written in are the included machine's as IT resolved them: each
// member under the name of the arg it was bound to, each node holding the type
// it held there (includedMedia). A node inside the included machine becomes a
// node of this one under a name nothing written can bear (`alias#use/name`);
// an input becomes what was bound to it; an output becomes the node that takes
// it, and an output nothing takes is not taken from the step that produces it.
func includeMachines(
	wirings []rawWiring,
	aliasMap map[string]headerCap,
	machines map[string]*includedMachine,
	includedMedia map[string]*urn.MediaUrn,
	registry *fabric.FabricRegistry,
) ([]rawWiring, *MachineParseError) {
	// What each machine step takes, by output name. Its unnamed target is
	// given its name here, so that everything after reads names alone.
	for i := range wirings {
		w := &wirings[i]
		machine, isMachine := machines[w.capAlias]
		if !isMachine {
			continue
		}
		if !w.policy.IsDefault() {
			return nil, abstractionParseError(policyOnMachineError(w.position, w.capAlias))
		}
		taken := map[string]bool{}
		for _, target := range w.targets {
			if !target.hasName {
				continue
			}
			if machine.output(target.name) == nil {
				return nil, abstractionParseError(unknownMachineOutputError(w.position, w.capAlias, target.name, machine.outputNames()))
			}
			if taken[target.name] {
				return nil, abstractionParseError(machineOutputTakenTwiceError(w.position, w.capAlias, target.name))
			}
			taken[target.name] = true
		}
		// The syntax allows one unnamed target.
		for t := range w.targets {
			if w.targets[t].hasName {
				continue
			}
			var left []string
			for _, output := range machine.outputNames() {
				if !taken[output] {
					left = append(left, output)
				}
			}
			if len(left) != 1 {
				return nil, abstractionParseError(unnamedMachineTargetError(w.position, w.capAlias, left))
			}
			w.targets[t].name = left[0]
			w.targets[t].hasName = true
		}
	}

	// The type of every node, as the wirings were written.
	mediaOf := make([]map[string]*urn.MediaUrn, len(wirings))
	for strandIndex, memberIndices := range componentsOf(wirings) {
		hasMachine := false
		for _, index := range memberIndices {
			if _, isMachine := machines[wirings[index].capAlias]; isMachine {
				hasMachine = true
			}
		}
		if !hasMachine {
			continue
		}
		members := make([]*rawWiring, len(memberIndices))
		for i, index := range memberIndices {
			members[i] = &wirings[index]
		}
		nodeMedia, err := typeNodes(members, aliasMap, machines, includedMedia, registry, strandIndex)
		if err != nil {
			return nil, err
		}
		for _, index := range memberIndices {
			if _, isMachine := machines[wirings[index].capAlias]; isMachine {
				mediaOf[index] = nodeMedia
			}
		}
	}

	// A node a cap produces from a declared output holds what that step gives,
	// a complete thing. Every other node holds a type: the one the text wrote,
	// the main output of an effect=none or effect=patch cap (which says how
	// what it gives follows from what it takes), or an output of a machine step.
	produced := map[string]bool{}
	for _, w := range wirings {
		if _, isMachine := machines[w.capAlias]; isMachine {
			continue
		}
		capUrn := aliasMap[w.capAlias].capUrn
		definition, err := registry.GetCachedCap(capUrn.String())
		if err != nil {
			return nil, abstractionParseError(definitionLookupError(capUrn.String(), err,
				func() *MachineAbstractionError { return unknownCapError(capUrn.String()) }))
		}
		for _, target := range w.targets {
			isMain := !target.hasName
			if main := definition.MainOutput(); target.hasName && main != nil {
				isMain = main.Name == target.name
			}
			if !isMain || mainOutputIsGiven(definition) {
				produced[target.node] = true
			}
		}
	}

	uses := map[string]int{}
	written := make([]rawWiring, 0, len(wirings))
	for index := range wirings {
		w := wirings[index]
		machine, isMachine := machines[w.capAlias]
		if !isMachine {
			written = append(written, w)
			continue
		}
		nodeMedia := mediaOf[index]
		if nodeMedia == nil {
			panic("a machine step's strand was typed above")
		}
		alias, position := w.capAlias, w.position

		// Named members bind first, each to the input it names.
		bound := map[string]rawValue{}
		for _, member := range w.sources {
			if !member.hasName {
				continue
			}
			accepts := machine.input(member.name)
			if accepts == nil {
				return nil, abstractionParseError(unknownMachineInputError(position, alias, member.name, machine.inputNames()))
			}
			if _, again := bound[member.name]; again {
				count := 0
				for _, other := range w.sources {
					if other.hasName && other.name == member.name {
						count++
					}
				}
				return nil, abstractionParseError(machineInputBindingError(position, alias, member.name, count))
			}
			if !member.value.isConstant {
				held := nodeMedia[member.value.node]
				if !sourceFits(held, produced[member.value.node], accepts) {
					return nil, abstractionParseError(sourceDoesNotConformToMachineInputError(
						position, alias, member.name, accepts.String(), held.String()))
				}
			}
			bound[member.name] = member.value
		}

		// The unnamed members, to the inputs left, by type alone.
		if unnamed := w.unnamedNodes(); len(unnamed) > 0 {
			var free []namedMedia
			for _, input := range machine.inputs {
				if _, isBound := bound[input.name]; !isBound {
					free = append(free, input)
				}
			}
			sources := make([]*urn.MediaUrn, len(unnamed))
			given := make([]bool, len(unnamed))
			for i, node := range unnamed {
				sources[i] = nodeMedia[node]
				given[i] = produced[node]
			}
			accepts := make([]*urn.MediaUrn, len(free))
			for i, input := range free {
				accepts[i] = input.media
			}
			pairs, failure := matchSourcesToArgs(sources, given, accepts, make([]bool, len(accepts)), -1)
			if failure != nil {
				if !failure.ambiguous {
					return nil, abstractionParseError(unmatchedMachineMemberError(position, alias, sources[failure.source].String()))
				}
				members := make([]UndecidedMember, len(failure.undecided))
				for i, each := range failure.undecided {
					members[i] = UndecidedMember{Node: unnamed[each.source], SourceUrn: sources[each.source].String()}
					for _, input := range each.args {
						members[i].CouldBe = append(members[i].CouldBe, free[input].name)
					}
				}
				return nil, abstractionParseError(ambiguousMachineMembersError(position, alias, members))
			}
			// The matching answers in types. Where it is unique no two
			// members and no two inputs it paired hold one type — two that
			// did would swap — so each pair names one member and one input.
			placed := make([]bool, len(unnamed))
			for _, pair := range pairs {
				input, member := -1, -1
				for i, candidate := range free {
					if _, isBound := bound[candidate.name]; !isBound && candidate.media.String() == pair.arg.String() {
						input = i
						break
					}
				}
				for i := range unnamed {
					if !placed[i] && sources[i].String() == pair.source.String() {
						member = i
						break
					}
				}
				if input < 0 || member < 0 {
					panic("the matching pairs every member with an input it was given")
				}
				placed[member] = true
				bound[free[input].name] = rawValue{node: unnamed[member]}
			}
		}
		for _, input := range machine.inputs {
			if _, isBound := bound[input.name]; !isBound {
				return nil, abstractionParseError(machineInputBindingError(position, alias, input.name, 0))
			}
		}

		// Output name → the node that takes it. Every target of a machine
		// step was named above.
		taken := map[string]string{}
		for _, target := range w.targets {
			taken[target.name] = target.node
		}

		// The steps, written in.
		uses[alias]++
		instance := fmt.Sprintf("%s#%d", alias, uses[alias])
		inside := func(node string) string { return instance + "/" + node }
		for _, held := range machine.nodeMedia {
			_, isTaken := taken[held.name]
			if machine.input(held.name) == nil && !isTaken {
				includedMedia[inside(held.name)] = held.media
			}
		}
		for stepIndex, step := range machine.steps {
			stepAlias := fmt.Sprintf("%s/%d", instance, stepIndex)
			aliasMap[stepAlias] = headerCap{capUrn: step.capUrn, position: position}
			// The step came through this use, and then through whatever uses
			// it had already come through inside the included machine.
			includedAs := append([]string{instance}, step.includedAs...)
			inlined := rawWiring{
				capAlias:   stepAlias,
				policy:     step.policy,
				position:   position,
				includedAs: includedAs,
			}
			for _, member := range step.members {
				value := rawValue{isConstant: member.value.isConstant, constant: member.value.constant}
				if !member.value.isConstant {
					if given, isInput := bound[member.value.node]; isInput {
						value = given
					} else {
						value.node = inside(member.value.node)
					}
				}
				inlined.sources = append(inlined.sources, rawMember{name: member.arg, hasName: true, value: value})
			}
			for _, target := range step.targets {
				taker, isTaken := taken[target.node]
				switch {
				case isTaken:
					inlined.targets = append(inlined.targets, rawTarget{name: target.output, hasName: true, node: taker})
				case machine.output(target.node) != nil:
					// An output of the machine that nothing here takes.
				default:
					inlined.targets = append(inlined.targets, rawTarget{name: target.output, hasName: true, node: inside(target.node)})
				}
			}
			written = append(written, inlined)
		}
	}
	return written, nil
}

// give gives `node` the URN `media`, beside any URN it was already given: one of
// the two must be inside the other, and the node holds the narrower. Two types
// can score the same and still differ (`ext=a|b` and `ext=a|b|c`), so
// specificity never decides it.
func give(nodeMedia map[string]*urn.MediaUrn, node string, media *urn.MediaUrn) *MachineParseError {
	existing, held := nodeMedia[node]
	if !held {
		nodeMedia[node] = media
		return nil
	}
	narrower, inside := narrowerOf(existing, media)
	if !inside {
		return abstractionParseError(nodeMediaConflictError(node, existing.String(), media.String()))
	}
	if narrower {
		nodeMedia[node] = media
	}
	return nil
}

// narrowerOf reports whether `given` should replace `held` as one node's type:
// (true, true) when `given` is strictly inside `held`, (false, true) when `held`
// is inside `given` (equivalent types included), and (false, false) when
// neither is inside the other.
func narrowerOf(held, given *urn.MediaUrn) (narrower bool, inside bool) {
	givenInside := given.ConformsTo(held)
	heldInside := held.ConformsTo(given)
	switch {
	case heldInside:
		return false, true
	case givenInside:
		return true, true
	default:
		return false, false
	}
}

// parsedStep is one cap step of a strand, as the node passes read it: its
// wiring, its cap and the media its main input accepts (nil when it takes none).
type parsedStep struct {
	wiring     *rawWiring
	capUrn     *urn.CapUrn
	definition *cap.Cap
	inMedia    *urn.MediaUrn
}

// binding is one place a node is bound into: what it is accepted as there,
// and, when the node is a cap step's main member, that step's index among the
// steps (-1 otherwise).
type binding struct {
	accepts *urn.MediaUrn
	mainOf  int
}

// nodeBindings lists every place each node is bound into: by name into a cap's
// arg or a machine's input, or unnamed into a cap's main input, as its one
// unnamed member or as one of the members its main input gathers when it
// takes a sequence. Only a member bound by the main arg's name, or the step's
// one unnamed member, is the step's main member.
func nodeBindings(wirings []*rawWiring, steps []parsedStep, machines map[string]*includedMachine) map[string][]binding {
	bindings := map[string][]binding{}
	for _, w := range wirings {
		machine, isMachine := machines[w.capAlias]
		if !isMachine {
			continue
		}
		for _, member := range w.sources {
			if !member.hasName || member.value.isConstant {
				continue
			}
			if accepts := machine.input(member.name); accepts != nil {
				bindings[member.value.node] = append(bindings[member.value.node], binding{accepts, -1})
			}
		}
	}
	for index, s := range steps {
		var mainArg *cap.CapArg
		if s.inMedia != nil {
			for i := range s.definition.Args {
				if s.definition.Args[i].IsMainInput(s.inMedia) {
					mainArg = &s.definition.Args[i]
					break
				}
			}
		}
		mainNamed := false
		for _, member := range s.wiring.sources {
			if !member.hasName || member.value.isConstant {
				continue
			}
			// A name no arg has was refused above.
			arg := s.definition.ArgNamed(member.name)
			if arg == nil {
				continue
			}
			mainOf := -1
			if mainArg != nil && mainArg.Name == arg.Name {
				mainOf = index
				mainNamed = true
			}
			bindings[member.value.node] = append(bindings[member.value.node], binding{definitionMedia(arg.AcceptedUrn()), mainOf})
		}
		if s.inMedia == nil || mainArg == nil || mainNamed {
			continue
		}
		unnamed := s.wiring.unnamedNodes()
		switch {
		case len(unnamed) == 1:
			bindings[unnamed[0]] = append(bindings[unnamed[0]], binding{s.inMedia, index})
		case mainArg.IsSequence:
			for _, node := range unnamed {
				bindings[node] = append(bindings[node], binding{s.inMedia, -1})
			}
		}
	}
	return bindings
}

// nodeNeed is what node must be for every step it is bound into: what each
// accepts it as, and, where it is a step's main member, what that step requires
// of what it is sent for what its own main output must be. nil when nothing
// binds it, or when none of those is inside every other. Worked out once per
// node (need); a node met again on its own way back is a cycle, which the
// resolver refuses, and says nothing here.
func nodeNeed(node string, bindings map[string][]binding, steps []parsedStep, need map[string]*urn.MediaUrn, visiting map[string]bool) *urn.MediaUrn {
	if known, ok := need[node]; ok {
		return known
	}
	if visiting[node] {
		return nil
	}
	visiting[node] = true
	var needs []*urn.MediaUrn
	for _, b := range bindings[node] {
		needs = append(needs, b.accepts)
		if b.mainOf < 0 {
			continue
		}
		s := steps[b.mainOf]
		for _, target := range s.wiring.targets {
			if !isMainTarget(s.definition, target) {
				continue
			}
			outNeed := nodeNeed(target.node, bindings, steps, need, visiting)
			if outNeed == nil {
				continue
			}
			if required, ok := s.capUrn.Requires(outNeed); ok {
				needs = append(needs, required)
			}
		}
	}
	delete(visiting, node)
	narrowest := narrowestInsideAll(needs)
	need[node] = narrowest
	return narrowest
}

// narrowestInsideAll is the one of types that is inside every other, or nil
// when there are none or no one of them is.
func narrowestInsideAll(types []*urn.MediaUrn) *urn.MediaUrn {
	if len(types) == 0 {
		return nil
	}
	narrowest := types[0]
	for _, t := range types[1:] {
		narrower, inside := narrowerOf(narrowest, t)
		if !inside {
			return nil
		}
		if narrower {
			narrowest = t
		}
	}
	for _, t := range types {
		if !narrowest.ConformsTo(t) {
			return nil
		}
	}
	return narrowest
}

// carriesInput says whether a target takes an output of the cap that carries
// the cap's input.
func carriesInput(definition *cap.Cap, target rawTarget) bool {
	if !target.hasName {
		return false
	}
	output := definition.OutputNamed(target.name)
	return output != nil && output.Carries != nil
}

// definitionMedia parses a media URN a cap definition holds.
func definitionMedia(text string) *urn.MediaUrn {
	parsed, err := urn.NewMediaUrnFromString(text)
	if err != nil {
		panic(fmt.Sprintf("cap registry invariant: a definition's media URNs are valid: %q: %v", text, err))
	}
	return parsed
}

// typeNodes gives every node of one strand its media URN (see the file
// documentation and 09 §The URN of a node). The result does not depend on the
// order of the wirings: what produces a node and what names it are read first,
// for every wiring; only then is each wiring asked for the one unnamed machine
// input its main input can type, against what the first two passes left
// untyped.
//
// A node written in from an included machine comes with the type it held there
// (includedMedia), given before anything else. A step that is a machine
// (machines) gives each node it produces the type of the output that node
// takes, and each input of this machine it is given BY NAME the type of that
// input; it has no main input to type an unnamed one by.
func typeNodes(
	wirings []*rawWiring,
	aliasMap map[string]headerCap,
	machines map[string]*includedMachine,
	includedMedia map[string]*urn.MediaUrn,
	registry *fabric.FabricRegistry,
	strandIndex int,
) (map[string]*urn.MediaUrn, *MachineParseError) {
	capOf := func(w *rawWiring) (*urn.CapUrn, *cap.Cap, *MachineParseError) {
		capUrn := aliasMap[w.capAlias].capUrn
		definition, err := registry.GetCachedCap(capUrn.String())
		if err != nil {
			return nil, nil, abstractionParseError(definitionLookupError(capUrn.String(), err,
				func() *MachineAbstractionError { return unknownCapError(capUrn.String()) }))
		}
		return capUrn, definition, nil
	}
	media := func(text string) *urn.MediaUrn {
		parsed, err := urn.NewMediaUrnFromString(text)
		if err != nil {
			panic(fmt.Sprintf("cap registry invariant: a definition's media URNs are valid: %q: %v", text, err))
		}
		return parsed
	}
	void := media(standard.MediaVoid)
	// What a cap's main input accepts: its `in=`. nil when it takes none.
	mainInputOf := func(w *rawWiring, capUrn *urn.CapUrn) (*urn.MediaUrn, *MachineParseError) {
		inMedia, err := capUrn.InMediaUrn()
		if err != nil {
			return nil, syntaxParseError(invalidMediaUrnError(w.capAlias, fmt.Sprintf("in= spec: %s", err)))
		}
		if inMedia.IsEquivalent(void) {
			return nil, nil
		}
		return inMedia, nil
	}

	nodeMedia := map[string]*urn.MediaUrn{}
	for _, w := range wirings {
		for _, node := range w.nodes() {
			if held, included := includedMedia[node]; included {
				if err := give(nodeMedia, node, held); err != nil {
					return nil, err
				}
			}
		}
	}

	// What produces a node, and — for a node nothing produces — what names it.
	produced := map[string]bool{}
	for _, w := range wirings {
		for _, target := range w.targets {
			produced[target.node] = true
		}
	}
	for _, w := range wirings {
		if machine, isMachine := machines[w.capAlias]; isMachine {
			for _, target := range w.targets {
				// includeMachines named every target, against the outputs.
				if err := give(nodeMedia, target.node, machine.output(target.name)); err != nil {
					return nil, err
				}
			}
			for _, member := range w.sources {
				if !member.hasName || member.value.isConstant {
					continue
				}
				accepts := machine.input(member.name)
				if accepts == nil {
					return nil, abstractionParseError(unknownMachineInputError(w.position, w.capAlias, member.name, machine.inputNames()))
				}
				if !produced[member.value.node] {
					if err := give(nodeMedia, member.value.node, accepts); err != nil {
						return nil, err
					}
				}
			}
			continue
		}
		capUrn, definition, err := capOf(w)
		if err != nil {
			return nil, err
		}
		for _, target := range w.targets {
			var held *urn.MediaUrn
			if !target.hasName {
				// The main output: the cap URN's `out=`. (A cap with none is
				// refused by the resolver, which holds the wiring to it.)
				out, outErr := capUrn.OutMediaUrn()
				if outErr != nil {
					return nil, syntaxParseError(invalidMediaUrnError(w.capAlias, fmt.Sprintf("out= spec: %s", outErr)))
				}
				held = out
			} else {
				output := definition.OutputNamed(target.name)
				if output == nil {
					return nil, abstractionParseError(unknownOutputNameError(strandIndex, capUrn.String(), target.name, outputNames(definition)))
				}
				held = media(output.MediaUrn)
				if output.Carries != nil {
					// An output that carries the input holds the input's type.
					inMedia, inErr := mainInputOf(w, capUrn)
					if inErr != nil {
						return nil, inErr
					}
					if inMedia != nil {
						held = inMedia
					}
				}
			}
			if err := give(nodeMedia, target.node, held); err != nil {
				return nil, err
			}
		}
		for _, member := range w.sources {
			if !member.hasName || member.value.isConstant {
				continue
			}
			arg := definition.ArgNamed(member.name)
			if arg == nil {
				return nil, abstractionParseError(unknownArgNameError(strandIndex, capUrn.String(), member.name, argNames(definition)))
			}
			// A node some step produces has that output's type; whether it
			// conforms to the arg it is named into is the resolver's to say
			// (ErrAbstractionSourceDoesNotConformToArg). The name types an
			// input of the machine, which has no other type.
			if !produced[member.value.node] {
				if err := give(nodeMedia, member.value.node, media(arg.AcceptedUrn())); err != nil {
					return nil, err
				}
			}
		}
	}

	// What a step's MAIN input says of the unnamed member that feeds it. A
	// step has one data input, so where exactly one unnamed member can be it,
	// that member's node is given the type the main input accepts:
	//
	//   - a member nothing above gave a URN is an input of the machine whose
	//     arg nothing says: one such member per wiring is the main input, and
	//     only that (ErrAbstractionUntypedMachineInput otherwise);
	//   - failing that, the one unnamed member whose URN is on the main
	//     input's specialization chain is refined to it — a consumer that asks
	//     for a tighter type than the producer declared states the type of
	//     what flows there, and the run holds the producer to it.
	//
	// Read against what the passes above left, for every wiring, before any
	// of it is applied: the order of the wirings changes nothing.
	type typed struct {
		node  string
		media *urn.MediaUrn
	}
	var fromMainInput []typed
	for _, w := range wirings {
		if _, isMachine := machines[w.capAlias]; isMachine {
			continue
		}
		unnamed := w.unnamedNodes()
		if len(unnamed) == 0 {
			continue
		}
		capUrn, definition, err := capOf(w)
		if err != nil {
			return nil, err
		}
		mainInput, err := mainInputOf(w, capUrn)
		if err != nil {
			return nil, err
		}
		mainNamed := false
		if mainInput != nil {
			for i := range definition.Args {
				if !definition.Args[i].IsMainInput(mainInput) {
					continue
				}
				for _, member := range w.sources {
					if member.hasName && member.name == definition.Args[i].Name {
						mainNamed = true
					}
				}
				break
			}
		}
		var untyped []string
		for _, node := range unnamed {
			if _, isTyped := nodeMedia[node]; !isTyped {
				untyped = append(untyped, node)
			}
		}
		switch {
		case len(untyped) == 0 && mainInput != nil && !mainNamed:
			var onChain []string
			for _, node := range unnamed {
				if nodeMedia[node].IsComparable(mainInput) {
					onChain = append(onChain, node)
				}
			}
			if len(onChain) == 1 {
				fromMainInput = append(fromMainInput, typed{onChain[0], mainInput})
			}
		case len(untyped) == 0:
		case len(untyped) == 1 && mainInput != nil && !mainNamed:
			fromMainInput = append(fromMainInput, typed{untyped[0], mainInput})
		default:
			return nil, abstractionParseError(untypedMachineInputError(w.position, capUrn.String(), untyped))
		}
	}
	for _, each := range fromMainInput {
		if err := give(nodeMedia, each.node, each.media); err != nil {
			return nil, err
		}
	}

	// The machine's text writes no types, so a node is read with the types its
	// steps need (09 §The URN of a node), in two passes that only narrow a node
	// along its chain; a type on no chain with the node's is left, for the
	// binding check to report:
	//
	//   - back: what a node must be is what every step it is bound into accepts
	//     there, and where it is a step's main member, what that step requires
	//     of what it is sent for what its own main output must be
	//     (CapUrn.Requires), all on one chain. An input of the machine is
	//     narrowed to it: a YAML input converted to JSON for a step that gathers
	//     JSON records that are not lists is a YAML record that is not a list.
	//   - forward: a step that passes on or patches what it is sent gives its
	//     main output's node what it gives for what its main member holds, when
	//     that is inside its input; an output that carries the input holds what
	//     the main member holds. Read again until nothing changes, so a chain of
	//     such steps is typed whatever order it is written in.
	var steps []parsedStep
	for _, w := range wirings {
		if _, isMachine := machines[w.capAlias]; isMachine {
			continue
		}
		capUrn, definition, err := capOf(w)
		if err != nil {
			return nil, err
		}
		inMedia, err := mainInputOf(w, capUrn)
		if err != nil {
			return nil, err
		}
		steps = append(steps, parsedStep{w, capUrn, definition, inMedia})
	}
	bindings := nodeBindings(wirings, steps, machines)
	need := map[string]*urn.MediaUrn{}
	for node := range bindings {
		nodeNeed(node, bindings, steps, need, map[string]bool{})
	}
	for node, must := range need {
		if must == nil || produced[node] {
			continue
		}
		if held, typed := nodeMedia[node]; typed {
			if narrower, inside := narrowerOf(held, must); inside && narrower {
				nodeMedia[node] = must
			}
		}
	}
	for pass := 0; pass <= len(wirings); pass++ {
		changed := false
		for i := range steps {
			s := &steps[i]
			if mainOutputIsGiven(s.definition) || s.inMedia == nil {
				continue
			}
			source := mainSource(s.wiring, s.definition, s.inMedia)
			if source == "" {
				continue
			}
			sent, typed := nodeMedia[source]
			// A member that is not inside the input is the resolver's to refuse.
			if !typed || !sent.ConformsTo(s.inMedia) {
				continue
			}
			given, inferErr := s.capUrn.InferRuntimeOutputMedia(sent)
			if inferErr != nil {
				return nil, abstractionParseError(runtimeMediaInferenceError(strandIndex, s.capUrn.String(), sent.String(), inferErr.Error()))
			}
			for _, target := range s.wiring.targets {
				var holds *urn.MediaUrn
				switch {
				case isMainTarget(s.definition, target):
					holds = given
				case carriesInput(s.definition, target):
					holds = sent
				default:
					continue
				}
				before := ""
				if held, ok := nodeMedia[target.node]; ok {
					before = held.String()
				}
				if err := give(nodeMedia, target.node, holds); err != nil {
					return nil, err
				}
				changed = changed || before != nodeMedia[target.node].String()
			}
		}
		if !changed {
			break
		}
	}

	// A machine step has no main input: an unnamed member that nothing above
	// gave a type has none to be matched by.
	for _, w := range wirings {
		if _, isMachine := machines[w.capAlias]; !isMachine {
			continue
		}
		var untyped []string
		for _, node := range w.unnamedNodes() {
			if _, isTyped := nodeMedia[node]; !isTyped {
				untyped = append(untyped, node)
			}
		}
		if len(untyped) > 0 {
			return nil, abstractionParseError(untypedMachineMemberError(w.position, w.capAlias, untyped))
		}
	}

	return nodeMedia, nil
}

// isMainTarget says whether a target, by the output name it wrote, takes the
// cap's main output.
func isMainTarget(definition *cap.Cap, target rawTarget) bool {
	if !target.hasName {
		return true
	}
	main := definition.MainOutput()
	return main != nil && main.Name == target.name
}

// mainSource is the node feeding a step's main input (inMedia): the member
// that names the main input's arg, or else the one member that names nothing.
// Empty when neither says.
func mainSource(w *rawWiring, definition *cap.Cap, inMedia *urn.MediaUrn) string {
	for i := range definition.Args {
		if !definition.Args[i].IsMainInput(inMedia) {
			continue
		}
		for _, member := range w.sources {
			if member.hasName && !member.value.isConstant && member.name == definition.Args[i].Name {
				return member.value.node
			}
		}
		break
	}
	if unnamed := w.unnamedNodes(); len(unnamed) == 1 {
		return unnamed[0]
	}
	return ""
}

func argNames(definition *cap.Cap) []string {
	names := make([]string, len(definition.Args))
	for i := range definition.Args {
		names[i] = definition.Args[i].Name
	}
	return names
}

func outputNames(definition *cap.Cap) []string {
	outputs := definition.GetOutputs()
	names := make([]string, len(outputs))
	for i := range outputs {
		names[i] = outputs[i].Name
	}
	return names
}

// unionFind is a tiny union-find structure for connected-components partition.
type unionFind struct {
	parent []int
	rank   []int
}

func newUnionFind(n int) *unionFind {
	parent := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	return &unionFind{parent: parent, rank: make([]int, n)}
}

func (uf *unionFind) find(x int) int {
	if uf.parent[x] != x {
		uf.parent[x] = uf.find(uf.parent[x]) // path compression
	}
	return uf.parent[x]
}

func (uf *unionFind) union(a, b int) {
	ra, rb := uf.find(a), uf.find(b)
	if ra == rb {
		return
	}
	if uf.rank[ra] < uf.rank[rb] {
		uf.parent[ra] = rb
	} else if uf.rank[ra] > uf.rank[rb] {
		uf.parent[rb] = ra
	} else {
		uf.parent[rb] = ra
		uf.rank[ra]++
	}
}
