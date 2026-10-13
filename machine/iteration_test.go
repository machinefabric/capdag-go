package machine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func member(node NodeId, argIsSequence bool) MemberShape {
	return MemberShape{Node: node, ArgIsSequence: argIsSequence}
}

func shape(members []MemberShape, outputs ...OutputShape) StepShape {
	return StepShape{Members: members, Outputs: outputs}
}

func single(node NodeId) OutputShape   { return OutputShape{Node: node} }
func sequence(node NodeId) OutputShape { return OutputShape{Node: node, IsSequence: true} }

func placedSteps(t *testing.T, steps []StepShape, sequences ...NodeId) *IterationStructure {
	t.Helper()
	structure, cyclic := IterationOf(steps, func(node NodeId) bool { return containsNode(sequences, node) })
	require.Nil(t, cyclic, "acyclic")
	return structure
}

// once is a step that runs once in `scope` and maps nothing.
func once(scope ScopeId) StepPlacement {
	return StepPlacement{PlacedIn: scope}
}

// mapOf is the map a step runs in; the step must map.
func mapOf(t *testing.T, s *IterationStructure, step int) ScopeId {
	t.Helper()
	placement := s.Steps()[step]
	require.True(t, placement.Maps, "step %d maps", step)
	require.NotEqual(t, ScopeRoot, placement.Map, "a map is never the machine itself")
	return placement.Map
}

// TEST12746: a step given single values runs once, in the machine; a step
// given a sequence on an arg that takes one value maps over it, and what it
// produces lives in that map. A sequence on an arg that takes a sequence is
// given whole and maps nothing.
func Test12746_a_sequence_on_a_scalar_arg_opens_a_map(t *testing.T) {
	// 0 -> a -> 1 ; 1 is a declared sequence ; 1 -> b -> 2 ; 1 -> count(seq arg) -> 3
	steps := []StepShape{
		shape([]MemberShape{member(0, false)}, sequence(1)),
		shape([]MemberShape{member(1, false)}, single(2)),
		shape([]MemberShape{member(1, true)}, single(3)),
	}
	s := placedSteps(t, steps)
	assert.Equal(t, once(ScopeRoot), s.Steps()[0])
	scope := mapOf(t, s, 1)
	assert.Equal(t, ScopeRoot, s.Steps()[1].PlacedIn)
	assert.Equal(t, []NodeId{1}, s.Steps()[1].Mapped)
	assert.Equal(t, scope, s.Steps()[1].Scope())
	assert.Equal(t, ScopeRoot, s.Parent(scope))
	assert.Equal(t, []NodeId{1}, s.Sources(scope))
	opener, opened := s.OpenedBy(scope)
	assert.True(t, opened)
	assert.Equal(t, 1, opener)
	_, opened = s.OpenedBy(ScopeRoot)
	assert.False(t, opened, "nothing opens the machine itself")
	assert.Equal(t, scope, s.NodeScope(2))
	assert.Equal(t, ScopeRoot, s.NodeScope(1))
	assert.Equal(t, once(ScopeRoot), s.Steps()[2])
	assert.Equal(t, ScopeRoot, s.Steps()[2].Scope())
	assert.Equal(t, 2, s.ScopeCount())

	// The same machine with a single value where the sequence was: no map.
	steps = []StepShape{
		shape([]MemberShape{member(0, false)}, single(1)),
		shape([]MemberShape{member(1, false)}, single(2)),
	}
	s = placedSteps(t, steps)
	assert.Equal(t, []StepPlacement{once(ScopeRoot), once(ScopeRoot)}, s.Steps())
	assert.Equal(t, 1, s.ScopeCount())
	// And a run that binds the machine's input to a sequence maps the first
	// step over it, the second joining that map.
	s = placedSteps(t, steps, 0)
	scope = mapOf(t, s, 0)
	assert.Equal(t, []NodeId{0}, s.Steps()[0].Mapped)
	assert.Equal(t, once(scope), s.Steps()[1])
	assert.Equal(t, scope, s.NodeScope(2))
}

// TEST12747: a step given a value a map produces joins that map; a step that
// takes a sequence and is given such a value folds what the map collected, in
// the scope that encloses the map. Where a step lands does not depend on the
// order the steps are given in.
func Test12747_a_step_joins_the_map_its_value_is_produced_in_and_a_fold_closes_it(t *testing.T) {
	// 0 (sequence) -> ocr -> 1 ; 1 -> summarize -> 2 ; 2 -> join(seq arg) -> 3 ; 3 -> title -> 4
	steps := []StepShape{
		shape([]MemberShape{member(0, false)}, single(1)),
		shape([]MemberShape{member(1, false)}, single(2)),
		shape([]MemberShape{member(2, true)}, single(3)),
		shape([]MemberShape{member(3, false)}, single(4)),
	}
	s := placedSteps(t, steps, 0)
	scope := mapOf(t, s, 0)
	assert.Equal(t, once(scope), s.Steps()[1])
	assert.Equal(t, once(ScopeRoot), s.Steps()[2])
	assert.Equal(t, once(ScopeRoot), s.Steps()[3])
	assert.Equal(t, ScopeRoot, s.NodeScope(3))

	reordered := []StepShape{steps[3], steps[2], steps[1], steps[0]}
	r := placedSteps(t, reordered, 0)
	scope = mapOf(t, r, 3)
	assert.Equal(t, once(scope), r.Steps()[2])
	assert.Equal(t, once(ScopeRoot), r.Steps()[1])
	assert.Equal(t, once(ScopeRoot), r.Steps()[0])
	opener, _ := r.OpenedBy(scope)
	assert.Equal(t, 3, opener)
}

// TEST12748: several sequences on args that take one value are zipped by one
// map; a single value beside them is given whole and does not move the step.
// The same node on two such args is one zipped member.
func Test12748_several_mapped_members_open_one_map(t *testing.T) {
	// 0, 1 sequences; 2 a single value. (1, b=0, c=2) -> compare -> 3
	steps := []StepShape{shape([]MemberShape{member(1, false), member(0, false), member(2, false)}, single(3))}
	s := placedSteps(t, steps, 0, 1)
	assert.Equal(t, ScopeRoot, s.Steps()[0].PlacedIn)
	assert.Equal(t, []NodeId{1, 0}, s.Steps()[0].Mapped, "in the order the step lists them")
	assert.Equal(t, []NodeId{0, 1}, s.Sources(mapOf(t, s, 0)), "a map's sources have one fixed order")
	assert.Equal(t, 2, s.ScopeCount())

	twice := []StepShape{shape([]MemberShape{member(0, false), member(0, false)}, single(1))}
	assert.Equal(t, []NodeId{0}, placedSteps(t, twice, 0).Steps()[0].Mapped)
}

// TEST12749: a sequence produced inside a map and bound to an arg that takes
// one value opens a map inside that map. A fold of what the inner map produces
// is placed in the outer map: one value per outer item. From the machine the
// inner value is one flat sequence.
func Test12749_a_map_nests_and_a_fold_closes_one_level(t *testing.T) {
	// 0 docs (sequence) -> split -> 1 pages (declared sequence, per doc)
	// 1 -> ocr -> 2 text ; 2 -> join(seq arg) -> 3 doc text ; 3 -> all(seq arg) -> 4
	steps := []StepShape{
		shape([]MemberShape{member(0, false)}, sequence(1)),
		shape([]MemberShape{member(1, false)}, single(2)),
		shape([]MemberShape{member(2, true)}, single(3)),
		shape([]MemberShape{member(3, true)}, single(4)),
	}
	s := placedSteps(t, steps, 0)
	outer, inner := mapOf(t, s, 0), mapOf(t, s, 1)
	assert.Equal(t, outer, s.Steps()[1].PlacedIn)
	assert.Equal(t, outer, s.Parent(inner))
	assert.Equal(t, 2, s.Depth(inner))
	assert.Equal(t, 1, s.Depth(outer))
	assert.Equal(t, 0, s.Depth(ScopeRoot))
	assert.Equal(t, once(outer), s.Steps()[2])
	assert.Equal(t, once(ScopeRoot), s.Steps()[3])
	assert.True(t, s.IsSequenceFrom(2, ScopeRoot))
	assert.True(t, s.IsSequenceFrom(2, outer))
	assert.False(t, s.IsSequenceFrom(2, inner))
	assert.True(t, s.Encloses(outer, inner))
	assert.False(t, s.Encloses(inner, outer))
	assert.True(t, s.Encloses(ScopeRoot, inner))

	// A step of the inner map that is also given the sequence the OUTER map
	// iterates takes the outer item — the doc its page came from — from inside
	// the inner map, two levels down.
	// (1, source=0) -> stamp -> 5
	steps = append(steps, shape([]MemberShape{member(1, false), member(0, false)}, single(5)))
	s = placedSteps(t, steps, 0)
	outer, inner = mapOf(t, s, 0), mapOf(t, s, 1)
	assert.Equal(t, outer, s.Steps()[4].PlacedIn)
	assert.Equal(t, inner, mapOf(t, s, 4), "it maps over the same pages: the same inner map")
	assert.Equal(t, []NodeId{1}, s.Steps()[4].Mapped)
	item, isItem := s.ItemMap(0, inner)
	assert.True(t, isItem)
	assert.Equal(t, outer, item, "the outer map's item, seen from the inner one")
	item, isItem = s.ItemMap(1, inner)
	assert.True(t, isItem)
	assert.Equal(t, inner, item)
	_, isItem = s.ItemMap(1, outer)
	assert.False(t, isItem, "from the outer map the pages are a sequence, given whole")
}

// TEST12750: two steps that map over the same sequence run in one map, so a
// third step given what each produced is given both for the same item — it
// joins the map and zips nothing. A step given a value of the map and the
// sequence the map iterates joins it too, taking the item.
func Test12750_steps_over_one_sequence_share_a_map(t *testing.T) {
	// 0 docs (sequence). 0 -> text -> 1 ; 0 -> meta -> 2 ; (1, meta=2) -> index -> 3
	// (1, original=0) -> compare -> 4
	steps := []StepShape{
		shape([]MemberShape{member(0, false)}, single(1)),
		shape([]MemberShape{member(0, false)}, single(2)),
		shape([]MemberShape{member(1, false), member(2, false)}, single(3)),
		shape([]MemberShape{member(1, false), member(0, false)}, single(4)),
	}
	s := placedSteps(t, steps, 0)
	scope := mapOf(t, s, 0)
	assert.Equal(t, scope, mapOf(t, s, 1), "the same map, not a second one over docs")
	opener, _ := s.OpenedBy(scope)
	assert.Equal(t, 0, opener)
	assert.Equal(t, 2, s.ScopeCount())
	assert.Equal(t, once(scope), s.Steps()[2])
	assert.Equal(t, once(scope), s.Steps()[3])
	// Inside the map the sequence it iterates is its item; a value the map
	// produces is not an item of anything, and from the machine the sequence
	// is given whole.
	item, isItem := s.ItemMap(0, scope)
	assert.True(t, isItem)
	assert.Equal(t, scope, item)
	_, isItem = s.ItemMap(1, scope)
	assert.False(t, isItem)
	_, isItem = s.ItemMap(0, ScopeRoot)
	assert.False(t, isItem)

	// A map is what it iterates IN THE SCOPE IT IS OPENED IN: the same
	// sequence mapped from two scopes is two maps.
	// 0 docs, 5 others: sequences. 0 -> split -> 1 (a declared sequence per doc)
	// 1 -> ocr -> 2: over each doc's pages, inside the map over docs.
	// 5 -> b -> 6 (a declared sequence per item of 5)
	// (1, all=6) -> t -> 7: neither map holds the other, so the step is placed
	// in the machine, mapping over every page there is, given 6 whole.
	steps = []StepShape{
		shape([]MemberShape{member(0, false)}, sequence(1)),
		shape([]MemberShape{member(1, false)}, single(2)),
		shape([]MemberShape{member(5, false)}, sequence(6)),
		shape([]MemberShape{member(1, false), member(6, true)}, single(7)),
	}
	s = placedSteps(t, steps, 0, 5)
	perDoc, flat := mapOf(t, s, 1), mapOf(t, s, 3)
	assert.Equal(t, []NodeId{1}, s.Sources(perDoc))
	assert.Equal(t, []NodeId{1}, s.Sources(flat))
	assert.Equal(t, mapOf(t, s, 0), s.Parent(perDoc))
	assert.Equal(t, ScopeRoot, s.Parent(flat))
	assert.NotEqual(t, perDoc, flat)
}

// TEST12751: there is no cross product. A step given a value of a map and a
// sequence the map does not iterate is not run for every pair: it is placed
// where that sequence lives, and the map's values are collected and zipped
// with it. Values of two maps that are not nested are zipped the same way,
// where both are seen.
func Test12751_an_outside_sequence_is_zipped_never_crossed(t *testing.T) {
	// 0 files, 5 keywords: sequences. 0 -> ocr -> 1 ; (1, k=5) -> tag -> 2
	steps := []StepShape{
		shape([]MemberShape{member(0, false)}, single(1)),
		shape([]MemberShape{member(1, false), member(5, false)}, single(2)),
	}
	s := placedSteps(t, steps, 0, 5)
	first := mapOf(t, s, 0)
	zip := mapOf(t, s, 1)
	assert.Equal(t, ScopeRoot, s.Steps()[1].PlacedIn, "not inside the map over files")
	assert.Equal(t, []NodeId{1, 5}, s.Steps()[1].Mapped)
	assert.Equal(t, ScopeRoot, s.Parent(zip))
	assert.NotEqual(t, first, zip)

	// 0, 1 sequences. 0 -> a -> 2 ; 1 -> b -> 3 ; (2, other=3) -> compare -> 4
	steps = []StepShape{
		shape([]MemberShape{member(0, false)}, single(2)),
		shape([]MemberShape{member(1, false)}, single(3)),
		shape([]MemberShape{member(2, false), member(3, false)}, single(4)),
	}
	s = placedSteps(t, steps, 0, 1)
	a, b := mapOf(t, s, 0), mapOf(t, s, 1)
	assert.NotEqual(t, a, b)
	assert.Equal(t, ScopeRoot, s.Steps()[2].PlacedIn)
	assert.Equal(t, []NodeId{2, 3}, s.Steps()[2].Mapped)
	assert.Equal(t, ScopeRoot, s.CommonScope(a, b))
	assert.Equal(t, a, s.CommonScope(a, a))

	// Inside a nested map the same holds one level down: a page's text and a
	// sequence of the machine are zipped in the machine, not run once per page
	// per item.
	// 0 docs (sequence), 9 stamps (sequence). 0 -> split -> 1 (declared sequence)
	// 1 -> ocr -> 2 ; (2, stamp=9) -> mark -> 3
	steps = []StepShape{
		shape([]MemberShape{member(0, false)}, sequence(1)),
		shape([]MemberShape{member(1, false)}, single(2)),
		shape([]MemberShape{member(2, false), member(9, false)}, single(3)),
	}
	s = placedSteps(t, steps, 0, 9)
	assert.Equal(t, ScopeRoot, s.Steps()[2].PlacedIn)
	assert.Equal(t, []NodeId{2, 9}, s.Steps()[2].Mapped)
}

// TEST12793: where a step lands does not depend on the order its members are
// listed in. A value of one map bound to an arg that takes one value, beside a
// sequence of an unrelated map bound to an arg that takes a sequence: neither
// map contains the other, so the step is placed where both are seen — mapping
// over the first, given the second whole — whichever is listed first.
func Test12793_placement_does_not_depend_on_member_order(t *testing.T) {
	// 0, 1 sequences. 0 -> a -> 2 (one value per item of 0)
	// 1 -> b -> 3 (a declared sequence per item of 1)
	// (2, all=3) -> t -> 4, and the same with its members swapped.
	for _, members := range [][]MemberShape{
		{member(2, false), member(3, true)},
		{member(3, true), member(2, false)},
	} {
		steps := []StepShape{
			shape([]MemberShape{member(0, false)}, single(2)),
			shape([]MemberShape{member(1, false)}, sequence(3)),
			shape(members, single(4)),
		}
		s := placedSteps(t, steps, 0, 1)
		assert.Equal(t, ScopeRoot, s.Steps()[2].PlacedIn, "%v", members)
		assert.Equal(t, []NodeId{2}, s.Steps()[2].Mapped)
		scope := mapOf(t, s, 2)
		assert.NotEqual(t, mapOf(t, s, 0), scope, "not the map the value was produced in")
		assert.Equal(t, ScopeRoot, s.Parent(scope))
	}
}

// TEST12752: steps that wait on one another cannot be placed, and the refusal
// names them.
func Test12752_steps_on_a_cycle_are_named(t *testing.T) {
	steps := []StepShape{
		shape([]MemberShape{member(0, false)}, single(1)),
		shape([]MemberShape{member(3, false)}, single(2)),
		shape([]MemberShape{member(2, false)}, single(3)),
	}
	structure, cyclic := IterationOf(steps, func(NodeId) bool { return false })
	assert.Nil(t, structure)
	require.NotNil(t, cyclic)
	assert.Equal(t, []int{1, 2}, cyclic.Steps)
}
