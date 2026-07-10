package machine

import (
	"sort"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/urn"
)

// preInternedWiring is one wiring after the caller has pre-interned its
// source and target slots into NodeIds against a parallel nodes slice.
type preInternedWiring struct {
	// tokenId is the originating resolved-strand step's stable identity,
	// carried onto the resulting MachineEdge (see MachineEdge.TokenId).
	tokenId       string
	capUrn        *urn.CapUrn
	sourceNodeIds []NodeId
	targetNodeId  NodeId
}

// resolveStrand converts a planner-produced Strand into a single MachineStrand.
//
// Every cap step names each of its inputs' producers explicitly (StrandInput or
// another step's TokenId) — there is no positional predecessor assumption, so
// fan-out and convergence both wire correctly. ForEach and Collect steps are
// elided here; IsLoop is derived from cardinality in resolvePreInterned, the
// single source of truth shared with path search.
func resolveStrand(
	strand *planner.Strand,
	registry *cap.FabricRegistry,
	strandIndex int,
) (*MachineStrand, *MachineAbstractionError) {
	var nodes []*urn.MediaUrn
	var preInterned []preInternedWiring

	// Producer of each node, by producing step's stable TokenId -> its output
	// NodeId. Explicit input sources (CapInput.Source) resolve against this — no
	// positional predecessor assumption, so fan-out and convergence both wire
	// correctly.
	producerNode := make(map[string]NodeId)
	// The single shared strand input anchor node, allocated on first StrandInput
	// reference and refined to the most specific consuming FromSpec.
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
				targetNodeId:  targetId,
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

// resolvePreInterned resolves a pre-interned wiring set into a MachineStrand.
//
// The caller has already allocated NodeIds for every distinct data position.
// The resolver does NOT touch the interning policy — two NodeIds whose URNs
// happen to be equivalent stay distinct.
//
// Steps:
//  1. Per-wiring source-to-cap-arg matching (brute-force minimum-cost, with uniqueness check).
//  2. Cycle detection via Kahn's algorithm over the resulting NodeId dependency graph.
//  3. Canonical edge ordering with a structural tiebreaker.
//  4. Anchor computation (NodeIds with no producer / no consumer in the strand).
func resolvePreInterned(
	nodes []*urn.MediaUrn,
	wirings []preInternedWiring,
	registry *cap.FabricRegistry,
	strandIndex int,
) (*MachineStrand, *MachineAbstractionError) {
	if len(wirings) == 0 {
		return nil, noCapabilityStepsError()
	}

	// Cardinality of every node, derived from the single canonical rule
	// (cap.SequenceShape): a node holds a sequence iff the cap that PRODUCES it
	// has a sequence output; root nodes (never a wiring target) are scalar.
	// IsLoop is DERIVED from cardinality, never authored. It replaces the
	// retired LOOP keyword.
	nodeIsSequence := make([]bool, len(nodes))
	for _, wiring := range wirings {
		capDef, ok := registry.GetCachedCap(wiring.capUrn.String())
		if !ok {
			return nil, unknownCapError(wiring.capUrn.String())
		}
		_, outputIsSequence := capDef.SequenceShape()
		nodeIsSequence[wiring.targetNodeId] = outputIsSequence
	}

	// Step 1: per-wiring source-to-cap-arg matching.
	indexedEdges := make([]*MachineEdge, 0, len(wirings))
	for _, wiring := range wirings {
		capDef, ok := registry.GetCachedCap(wiring.capUrn.String())
		if !ok {
			return nil, unknownCapError(wiring.capUrn.String())
		}

		// Wiring sources are matched against EVERY arg by its stream URN (its
		// Stdin source URN if it declares one, else its declared URN — a cap may
		// have no stdin at all). ALL args are producer-feedable: a producer is
		// anything that supplies a value (a prior cap's output, config, a
		// literal, …), so every arg is a candidate. The matcher assigns each
		// source to the arg it conforms to; args no source matches take their
		// value from config/defaults; genuine ambiguity is a hard failure (in
		// matchSourcesToArgs). stdin* names are historical — these are the
		// per-arg stream and slot URNs.
		var stdinArgUrns []*urn.MediaUrn
		var stdinArgSlotUrns []*urn.MediaUrn
		for _, arg := range capDef.Args {
			streamUrn, err := urn.NewMediaUrnFromString(arg.StreamUrn())
			if err != nil {
				panic("cap registry invariant: every arg stream URN is a valid MediaUrn: " + err.Error())
			}
			slotUrn, err := urn.NewMediaUrnFromString(arg.MediaUrn)
			if err != nil {
				panic("cap registry invariant: every cap arg media_urn is a valid MediaUrn: " + err.Error())
			}
			stdinArgUrns = append(stdinArgUrns, streamUrn)
			stdinArgSlotUrns = append(stdinArgSlotUrns, slotUrn)
		}

		// Pull source URNs from the nodes table.
		sourceUrns := make([]*urn.MediaUrn, len(wiring.sourceNodeIds))
		for i, id := range wiring.sourceNodeIds {
			sourceUrns[i] = nodes[id]
		}

		// Run bipartite matching.
		sortedAssignment, matchErr := matchSourcesToArgs(
			sourceUrns, stdinArgUrns, wiring.capUrn.String(), strandIndex,
		)
		if matchErr != nil {
			return nil, matchErr
		}

		// Build bindings: translate matched stdin URN back to slot identity.
		bindings := make([]EdgeAssignmentBinding, 0, len(sortedAssignment))
		consumedPositions := make([]bool, len(wiring.sourceNodeIds))
		for _, pair := range sortedAssignment {
			matchedStdinUrn := pair[0]
			sourceUrn := pair[1]

			// Find slot identity for this matched stdin URN.
			var slotUrn *urn.MediaUrn
			for j, su := range stdinArgUrns {
				if su.IsEquivalent(matchedStdinUrn) {
					slotUrn = stdinArgSlotUrns[j]
					break
				}
			}
			if slotUrn == nil {
				panic("matching returned a stdin URN not in the cap's stdin args list")
			}

			// Find the source NodeId by URN equivalence.
			chosenPos := -1
			for pos, sid := range wiring.sourceNodeIds {
				if consumedPositions[pos] {
					continue
				}
				if nodes[sid].IsEquivalent(sourceUrn) {
					chosenPos = pos
					break
				}
			}
			if chosenPos < 0 {
				panic("matching returned a source URN not in the wiring's source positions")
			}
			consumedPositions[chosenPos] = true
			bindings = append(bindings, EdgeAssignmentBinding{
				CapArgMediaUrn: slotUrn,
				Source:         wiring.sourceNodeIds[chosenPos],
			})
		}

		// Sort bindings by slot identity for canonical equivalence comparison.
		sort.Slice(bindings, func(a, b int) bool {
			return bindings[a].CapArgMediaUrn.String() < bindings[b].CapArgMediaUrn.String()
		})

		// Derive IsLoop from cardinality — the single ForEach rule
		// (cap.NeedsForeach): the primary data input (the first stdin arg)
		// carries a sequence but this cap consumes it as a scalar, so it maps
		// per-item. The primary stdin source node is the binding feeding the
		// first stdin arg's slot. A cap with no stdin arg (config-only) never
		// loops.
		primaryStdinSourceIsSequence := false
		if len(stdinArgSlotUrns) > 0 {
			primarySlot := stdinArgSlotUrns[0]
			for _, b := range bindings {
				if b.CapArgMediaUrn.IsEquivalent(primarySlot) {
					primaryStdinSourceIsSequence = nodeIsSequence[b.Source]
					break
				}
			}
		}
		isLoop := capDef.NeedsForeach(primaryStdinSourceIsSequence)

		indexedEdges = append(indexedEdges, &MachineEdge{
			TokenId:    wiring.tokenId,
			CapUrn:     wiring.capUrn,
			Assignment: bindings,
			Target:     wiring.targetNodeId,
			IsLoop:     isLoop,
		})
	}

	// Step 2: cycle detection + canonical edge order.
	canonicalOrder, cycleErr := topoSort(indexedEdges, nodes, strandIndex)
	if cycleErr != nil {
		return nil, cycleErr
	}
	edges := make([]*MachineEdge, len(canonicalOrder))
	for i, idx := range canonicalOrder {
		edges[i] = indexedEdges[idx]
	}

	// Step 3: anchor computation.
	producedNodeIds := make(map[NodeId]bool)
	consumedNodeIds := make(map[NodeId]bool)
	for _, e := range edges {
		producedNodeIds[e.Target] = true
		for _, b := range e.Assignment {
			consumedNodeIds[b.Source] = true
		}
	}

	var inputAnchorIds []NodeId
	var outputAnchorIds []NodeId
	for id := NodeId(0); id < NodeId(len(nodes)); id++ {
		if !producedNodeIds[id] && consumedNodeIds[id] {
			inputAnchorIds = append(inputAnchorIds, id)
		}
		if !consumedNodeIds[id] && producedNodeIds[id] {
			outputAnchorIds = append(outputAnchorIds, id)
		}
	}

	// Sort anchors by canonical (URN string, NodeId) order.
	sort.Slice(inputAnchorIds, func(a, b int) bool {
		ua, ub := nodes[inputAnchorIds[a]].String(), nodes[inputAnchorIds[b]].String()
		if ua != ub {
			return ua < ub
		}
		return inputAnchorIds[a] < inputAnchorIds[b]
	})
	sort.Slice(outputAnchorIds, func(a, b int) bool {
		ua, ub := nodes[outputAnchorIds[a]].String(), nodes[outputAnchorIds[b]].String()
		if ua != ub {
			return ua < ub
		}
		return outputAnchorIds[a] < outputAnchorIds[b]
	})

	return newMachineStrand(nodes, edges, inputAnchorIds, outputAnchorIds), nil
}

// matchSourcesToArgs matches a wiring's sources to a cap's stdin arg URNs by
// minimum total specificity-distance, with a uniqueness requirement.
//
// Returns matched pairs as [][2]*MediaUrn{capArgStdinUrn, sourceUrn}, sorted
// by capArgStdinUrn. Returns nil on error.
func matchSourcesToArgs(
	sources []*urn.MediaUrn,
	args []*urn.MediaUrn,
	capUrn string,
	strandIndex int,
) ([][2]*urn.MediaUrn, *MachineAbstractionError) {
	if len(sources) > len(args) {
		for _, source := range sources {
			hasCandidate := false
			for _, a := range args {
				if source.ConformsTo(a) {
					hasCandidate = true
					break
				}
			}
			if !hasCandidate {
				return nil, unmatchedSourceError(strandIndex, capUrn, source.String())
			}
		}
		return nil, unmatchedSourceError(strandIndex, capUrn, sources[0].String())
	}

	nSources := len(sources)
	nArgs := len(args)

	// cost[s][a] is the distance if sources[s] conforms to args[a], else -1.
	cost := make([][]int64, nSources)
	for s, source := range sources {
		cost[s] = make([]int64, nArgs)
		for a := range cost[s] {
			cost[s][a] = -1
		}
		hasAny := false
		for a, arg := range args {
			if source.ConformsTo(arg) {
				dist := int64(source.Specificity()) - int64(arg.Specificity())
				cost[s][a] = dist
				hasAny = true
			}
		}
		if !hasAny {
			return nil, unmatchedSourceError(strandIndex, capUrn, source.String())
		}
	}

	var bestCost *int64
	var bestAssignments [][]int

	current := make([]int, nSources)
	used := make([]bool, nArgs)
	enumerateMatchings(cost, 0, current, used, &bestCost, &bestAssignments)

	if bestCost == nil {
		return nil, unmatchedSourceError(strandIndex, capUrn, sources[0].String())
	}
	if len(bestAssignments) != 1 {
		return nil, ambiguousNotationError(strandIndex, capUrn)
	}

	assignment := bestAssignments[0]
	pairs := make([][2]*urn.MediaUrn, nSources)
	for s := range pairs {
		pairs[s] = [2]*urn.MediaUrn{args[assignment[s]], sources[s]}
	}
	// Sort pairs by cap arg URN string.
	sort.Slice(pairs, func(a, b int) bool {
		return pairs[a][0].String() < pairs[b][0].String()
	})
	return pairs, nil
}

// enumerateMatchings recursively enumerates all injections of sources into
// args with a defined cost, tracking the minimum total cost and the
// assignments that achieve it.
func enumerateMatchings(
	cost [][]int64,
	sIdx int,
	current []int,
	used []bool,
	bestCost **int64,
	bestAssignments *[][]int,
) {
	nSources := len(cost)
	if sIdx == nSources {
		total := int64(0)
		for s := 0; s < nSources; s++ {
			total += cost[s][current[s]]
		}
		if *bestCost == nil {
			c := total
			*bestCost = &c
			*bestAssignments = [][]int{append([]int{}, current...)}
		} else if total < **bestCost {
			c := total
			*bestCost = &c
			*bestAssignments = [][]int{append([]int{}, current...)}
		} else if total == **bestCost {
			*bestAssignments = append(*bestAssignments, append([]int{}, current...))
		}
		return
	}

	for aIdx := range cost[sIdx] {
		if used[aIdx] {
			continue
		}
		if cost[sIdx][aIdx] < 0 {
			continue
		}
		used[aIdx] = true
		current[sIdx] = aIdx
		enumerateMatchings(cost, sIdx+1, current, used, bestCost, bestAssignments)
		used[aIdx] = false
	}
}

// topoSort performs Kahn's algorithm over the resolved data-flow dependency
// graph, returning the canonical ordering of edge indices.
//
// Edge B depends on edge A iff some binding in B.Assignment has Source == A.Target.
func topoSort(
	edges []*MachineEdge,
	nodes []*urn.MediaUrn,
	strandIndex int,
) ([]int, *MachineAbstractionError) {
	n := len(edges)
	if n == 0 {
		return nil, nil
	}

	// Map NodeId -> list of edge indices that produce it as target.
	producersOf := make(map[NodeId][]int)
	for idx, e := range edges {
		producersOf[e.Target] = append(producersOf[e.Target], idx)
	}

	indegree := make([]int, n)
	successors := make([][]int, n)

	for bIdx, b := range edges {
		for _, binding := range b.Assignment {
			if producers, ok := producersOf[binding.Source]; ok {
				for _, aIdx := range producers {
					if aIdx == bIdx {
						continue
					}
					successors[aIdx] = append(successors[aIdx], bIdx)
					indegree[bIdx]++
				}
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
		return nil, cyclicStrandError(strandIndex)
	}
	return result, nil
}

// sortReady sorts the ready set in canonical structural order for deterministic
// Kahn's output.
func sortReady(ready []int, edges []*MachineEdge, nodes []*urn.MediaUrn) {
	sort.SliceStable(ready, func(i, j int) bool {
		ea, eb := edges[ready[i]], edges[ready[j]]

		capCmp := ea.CapUrn.String() < eb.CapUrn.String()
		capEq := ea.CapUrn.String() == eb.CapUrn.String()
		if !capEq {
			return capCmp
		}

		for k := range ea.Assignment {
			if k >= len(eb.Assignment) {
				return false
			}
			ba, bb := ea.Assignment[k], eb.Assignment[k]
			argA, argB := ba.CapArgMediaUrn.String(), bb.CapArgMediaUrn.String()
			if argA != argB {
				return argA < argB
			}
			srcA, srcB := nodes[ba.Source].String(), nodes[bb.Source].String()
			if srcA != srcB {
				return srcA < srcB
			}
		}
		if len(ea.Assignment) != len(eb.Assignment) {
			return len(ea.Assignment) < len(eb.Assignment)
		}

		tgtA, tgtB := nodes[ea.Target].String(), nodes[eb.Target].String()
		if tgtA != tgtB {
			return tgtA < tgtB
		}

		return !ea.IsLoop && eb.IsLoop
	})
}
