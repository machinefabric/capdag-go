package machine

import "sort"

// Which steps of a machine run once per item, and inside which map.
//
// Notation never writes iteration: it follows from cardinality (docs 09
// §Iteration). This file is the one place that rule is computed. The resolver
// asks it which steps map. Notation has no run, so its inputs are single
// values; a caller that knows a run says which inputs it binds a sequence to.
//
// # The rule
//
// Every node lives in a scope: the machine itself, or a map. A node in a map
// has one value per run of that map. Seen from outside the map it is the
// sequence of those values in item order — and when the map is itself inside
// another, seen from outside both it is one flat sequence.
//
// A map is what it iterates: the sequences it zips, in the scope it is opened
// in. Two steps that map over the same sequences in the same scope run in the
// same map, so what each produces for an item can be given to a third step for
// that item.
//
// A step is first placed in the deepest scope its members ask for, when those
// scopes lie on one line of nesting; when they do not, in the scope that
// encloses them all. What a member asks for:
//
//   - bound to an arg that takes one value: the scope the node lives in;
//   - a sequence bound to an arg that takes a sequence: the scope it lives in
//     — the step is given that sequence whole;
//   - a single value bound to an arg that takes a sequence: the scope that
//     encloses the node's map — the step folds what the map collected. (A
//     single value of the machine itself asks for the machine.)
//
// From its placement, each member bound to an arg that takes one value is one
// of:
//
//   - a single value there: given as it is;
//   - the sequence a map the placement is inside iterates: given as that map's
//     item — the step joins the map;
//   - a sequence that lives in the placement, or was collected into it: mapped
//     — the step runs in the map over it, opened inside the placement. Several
//     mapped members are zipped;
//   - a sequence from outside that no enclosing map iterates. Mapping it here
//     would run every item of it for every item of the enclosing map: a cross
//     product nobody wrote. The step is placed further out instead — where
//     that sequence lives — and what it was given from the inner map is
//     collected and zipped with it.

// ScopeId names a scope of iteration: the machine itself (ScopeRoot) or one
// map.
type ScopeId = int

// ScopeRoot is the machine itself: what is not inside any map.
const ScopeRoot ScopeId = 0

// MemberShape is one node a step is given, and what the arg it is bound to
// takes.
type MemberShape struct {
	Node NodeId
	// ArgIsSequence: the arg takes a sequence (a member alone on it is given
	// whole; several on it are gathered).
	ArgIsSequence bool
}

// OutputShape is one node a step produces, and whether the output that
// produces it is declared a sequence.
type OutputShape struct {
	Node       NodeId
	IsSequence bool
}

// StepShape is a step, as far as iteration is concerned.
type StepShape struct {
	// Members are the nodes the step is given. A constant is not a member: it
	// is one value wherever the step runs.
	Members []MemberShape
	Outputs []OutputShape
}

// StepPlacement is where a step runs.
type StepPlacement struct {
	// PlacedIn is the scope the step is placed in: where it is given its
	// members.
	PlacedIn ScopeId
	// Map is the map the step runs in, when it maps: opened inside its
	// placement. Maps says whether it does.
	Map  ScopeId
	Maps bool
	// Mapped is the members the step maps over, zipped: distinct nodes, in
	// the order the step lists them. Empty exactly when Maps is false.
	Mapped []NodeId
}

// Scope returns the scope the step's invocations run in, and its outputs live
// in: the map it maps in, or its placement.
func (p StepPlacement) Scope() ScopeId {
	if p.Maps {
		return p.Map
	}
	return p.PlacedIn
}

// CyclicSteps names the steps a machine cannot place because they wait on one
// another.
type CyclicSteps struct {
	Steps []int
}

type iterationScope struct {
	// parent is the scope that encloses this one; the machine encloses itself.
	parent ScopeId
	// sources are the sequences a map zips, sorted; none for the machine.
	sources []NodeId
	// mappers are the steps that map in it, by index.
	mappers []int
}

// IterationStructure is the scopes of a machine and the place of every step
// and node in them.
type IterationStructure struct {
	scopes         []iterationScope
	steps          []StepPlacement
	nodeScope      map[NodeId]ScopeId
	nodeIsSequence map[NodeId]bool
}

// IterationOf places every step. `inputIsSequence` says, for a node no step
// produces, whether it holds a sequence.
func IterationOf(steps []StepShape, inputIsSequence func(NodeId) bool) (*IterationStructure, *CyclicSteps) {
	produced := map[NodeId]bool{}
	for _, step := range steps {
		for _, output := range step.Outputs {
			produced[output.Node] = true
		}
	}

	structure := &IterationStructure{
		scopes:         []iterationScope{{parent: ScopeRoot}},
		nodeScope:      map[NodeId]ScopeId{},
		nodeIsSequence: map[NodeId]bool{},
	}
	// An input of the machine lives in the machine.
	for _, step := range steps {
		for _, member := range step.Members {
			if !produced[member.Node] {
				structure.nodeScope[member.Node] = ScopeRoot
				structure.nodeIsSequence[member.Node] = inputIsSequence(member.Node)
			}
		}
	}

	// A step is placed once every node it is given has a place. The order
	// steps are placed in does not change where any of them lands: a placement
	// reads only its members, and a map is found by what it iterates.
	placements := make([]StepPlacement, len(steps))
	placed := make([]bool, len(steps))
	remaining := len(steps)
	for remaining > 0 {
		before := remaining
		for index, step := range steps {
			if placed[index] || !structure.allPlaced(step.Members) {
				continue
			}
			placement := structure.place(index, step)
			for _, output := range step.Outputs {
				structure.nodeScope[output.Node] = placement.Scope()
				structure.nodeIsSequence[output.Node] = output.IsSequence
			}
			placements[index] = placement
			placed[index] = true
			remaining--
		}
		if remaining == before {
			var waiting []int
			for index := range steps {
				if !placed[index] {
					waiting = append(waiting, index)
				}
			}
			return nil, &CyclicSteps{Steps: waiting}
		}
	}
	structure.steps = placements
	for i := range structure.scopes {
		sort.Ints(structure.scopes[i].mappers)
	}
	return structure, nil
}

func (s *IterationStructure) allPlaced(members []MemberShape) bool {
	for _, member := range members {
		if _, known := s.nodeScope[member.Node]; !known {
			return false
		}
	}
	return true
}

func (s *IterationStructure) place(index int, step StepShape) StepPlacement {
	// What each member asks for.
	asked := make([]ScopeId, len(step.Members))
	for i, member := range step.Members {
		scope := s.nodeScope[member.Node]
		if member.ArgIsSequence && !s.nodeIsSequence[member.Node] {
			scope = s.scopes[scope].parent
		}
		asked[i] = scope
	}
	deepest := ScopeRoot
	for i, scope := range asked {
		// Two different scopes of one depth are not on one line of nesting,
		// and then the deepest is not where the step is placed.
		if i == 0 || s.Depth(scope) > s.Depth(deepest) {
			deepest = scope
		}
	}
	onOneLine := true
	for _, scope := range asked {
		if !s.Encloses(scope, deepest) {
			onOneLine = false
		}
	}
	placedIn := deepest
	if !onOneLine {
		placedIn = asked[0]
		for _, scope := range asked[1:] {
			placedIn = s.CommonScope(placedIn, scope)
		}
	}

	// What each member bound to an arg that takes one value is from there. A
	// sequence from outside that no enclosing map iterates moves the placement
	// out to where it lives, and the members are read again.
	var mapped []NodeId
	for {
		mapped = nil
		moved := false
		for _, member := range step.Members {
			if member.ArgIsSequence {
				continue
			}
			livesIn := s.nodeScope[member.Node]
			if s.Encloses(livesIn, placedIn) {
				if !s.nodeIsSequence[member.Node] {
					continue // one value here
				}
				if livesIn != placedIn {
					if s.iteratedBetween(member.Node, placedIn, livesIn) {
						continue // the item of a map the step is inside
					}
					placedIn = livesIn
					moved = true
					break
				}
				// A sequence that lives here: mapped.
			}
			// Otherwise the node lives in a map the placement is not inside,
			// and the placement encloses that map (it encloses every scope a
			// member asked for): the node is seen here as what the map
			// collected — a sequence, mapped.
			if !containsNode(mapped, member.Node) {
				mapped = append(mapped, member.Node)
			}
		}
		if !moved {
			break
		}
	}

	placement := StepPlacement{PlacedIn: placedIn, Mapped: mapped}
	if len(mapped) == 0 {
		return placement
	}
	sources := append([]NodeId(nil), mapped...)
	sort.Slice(sources, func(a, b int) bool { return sources[a] < sources[b] })
	id := -1
	for i, scope := range s.scopes {
		// The machine itself zips nothing, so it is never the map sought.
		if scope.parent == placedIn && sameNodes(scope.sources, sources) {
			id = i
			break
		}
	}
	if id < 0 {
		s.scopes = append(s.scopes, iterationScope{parent: placedIn, sources: sources})
		id = len(s.scopes) - 1
	}
	s.scopes[id].mappers = append(s.scopes[id].mappers, index)
	placement.Map = id
	placement.Maps = true
	return placement
}

func containsNode(nodes []NodeId, node NodeId) bool {
	for _, each := range nodes {
		if each == node {
			return true
		}
	}
	return false
}

func sameNodes(a, b []NodeId) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// iteratedBetween reports whether a map from `inner` outward, short of
// `outer`, iterates `node`.
func (s *IterationStructure) iteratedBetween(node NodeId, inner, outer ScopeId) bool {
	for at := inner; at != outer; at = s.scopes[at].parent {
		if containsNode(s.scopes[at].sources, node) {
			return true
		}
	}
	return false
}

// Steps returns where each step runs, in the order the steps were given.
func (s *IterationStructure) Steps() []StepPlacement { return s.steps }

// ScopeCount returns how many scopes there are, the machine included.
func (s *IterationStructure) ScopeCount() int { return len(s.scopes) }

// Parent returns the scope that encloses `scope`. The machine encloses itself.
func (s *IterationStructure) Parent(scope ScopeId) ScopeId { return s.scopes[scope].parent }

// Sources returns the sequences map `scope` zips, in a fixed order; none for
// the machine.
func (s *IterationStructure) Sources(scope ScopeId) []NodeId { return s.scopes[scope].sources }

// OpenedBy returns the first step that maps in `scope`: the one the map is
// opened by. The machine is opened by none.
func (s *IterationStructure) OpenedBy(scope ScopeId) (int, bool) {
	if len(s.scopes[scope].mappers) == 0 {
		return 0, false
	}
	return s.scopes[scope].mappers[0], true
}

// ItemMap returns the map that gives a step running in `from` the ITEM of
// `node`: the innermost map at or around `from` that iterates it. When no map
// the step is inside iterates the node, the step is given it whole.
func (s *IterationStructure) ItemMap(node NodeId, from ScopeId) (ScopeId, bool) {
	for at := from; at != ScopeRoot; at = s.scopes[at].parent {
		if containsNode(s.scopes[at].sources, node) {
			return at, true
		}
	}
	return ScopeRoot, false
}

// NodeScope returns the scope a node lives in.
func (s *IterationStructure) NodeScope(node NodeId) ScopeId { return s.nodeScope[node] }

// Depth returns how many maps enclose `scope`, itself included.
func (s *IterationStructure) Depth(scope ScopeId) int {
	depth := 0
	for at := scope; at != ScopeRoot; at = s.scopes[at].parent {
		depth++
	}
	return depth
}

// Encloses reports whether `outer` is `inner` or encloses it.
func (s *IterationStructure) Encloses(outer, inner ScopeId) bool {
	for at := inner; ; at = s.scopes[at].parent {
		if at == outer {
			return true
		}
		if at == ScopeRoot {
			return false
		}
	}
}

// CommonScope returns the deepest scope that encloses both.
func (s *IterationStructure) CommonScope(a, b ScopeId) ScopeId {
	at := a
	for !s.Encloses(at, b) {
		at = s.scopes[at].parent
	}
	return at
}

// IsSequenceFrom reports whether `node` is a sequence as a step placed in
// `from` is given it: what it is in its own scope when that scope encloses
// `from`, and the collected sequence otherwise.
func (s *IterationStructure) IsSequenceFrom(node NodeId, from ScopeId) bool {
	if s.Encloses(s.nodeScope[node], from) {
		return s.nodeIsSequence[node]
	}
	return true
}
