package machine

import (
	"fmt"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/urn"
)

// RealizeStrand realizes a resolved MachineStrand into an executable Strand of
// steps.
//
// This is the single, shared conversion from a resolved notation strand into
// the Strand the canonical plan builder consumes. It is the inverse of
// planner.Strand.Knit-equivalent knitting and the logic the engine's
// editor-run realization and the reference/CLI path both use — one
// implementation, no duplication.
//
// ## What it does
//
// Walking the strand in data-flow (dependency) order, it emits one Cap step
// per edge, instantiating the runtime media type through each cap's MAIN
// input (CapUrn.InferRuntimeOutputMedia), and inserts a ForEach step before
// any cap the resolver already marked IsLoop.
//
// A cap edge's resolver Assignment binds each wiring source to one of the
// cap's arguments by media URN. Exactly one of those is the cap's stdin
// (main) input — it threads the runtime media of the chain and is the step's
// FromSpec. Every OTHER binding is a convergence input: another cap's output
// routed into a non-main argument, recorded on the step as a CapInput. This is
// what lets a strand express a DAG (a cap with more than one incoming
// producer), not just a linear chain — the executable model the engine and
// reference path share.
//
// IsLoop is the single source of truth for cardinality: resolve.go derives it
// from cap.NeedsForeach (a sequence source feeding a scalar-input cap); this
// converter reads edge.IsLoop, never recomputing it.
//
// ## Invariants (enforced, no fallbacks)
//
//   - Exactly one stdin (main) input per cap. The cap definition declares one
//     Stdin argument; the resolver's assignment binds a source to it. A cap
//     with no stdin arg, or an edge with no binding to it, is a hard error.
//   - Convergence wires only cap outputs. A non-main argument fed by wiring
//     must be another cap's output. A raw input feeding a non-main arg is an
//     argument VALUE (default / setting / config / user input), delivered
//     through the value channel, never wired — a wiring source that is not a
//     producer is a hard error.
//   - Connected data-flow graph per strand. Every edge's sources must become
//     available (input anchors, or already-emitted producers); an
//     unreachable edge is a hard error.
//
// strandIndex is used only for diagnostics.
func RealizeStrand(
	machineStrand *MachineStrand,
	registry *cap.FabricRegistry,
	sourceUrn *urn.MediaUrn,
	strandIndex int,
) (*planner.Strand, *MachineAbstractionError) {
	// Per-node runtime media. A convergence strand fans out from its input and
	// converges at a multi-input cap, so each node carries its own runtime
	// media — there is no single linear thread. Input anchors carry the
	// concrete input media (sourceUrn); each emitted cap sets its target
	// node's media.
	nodeMedia := make(map[NodeId]*urn.MediaUrn)
	for _, anchor := range machineStrand.InputAnchorIds() {
		nodeMedia[anchor] = sourceUrn
	}
	// The step (by stable TokenId) that produced each node, for wiring
	// convergence args. Input anchors have no producing step.
	nodeProducer := make(map[NodeId]string)

	edges := machineStrand.Edges()
	emitted := make([]bool, len(edges))
	steps := make([]*planner.StrandStep, 0, len(edges)*2)

	// Emit edges in dependency order: an edge is emittable once every one of
	// its wiring sources has a known runtime media (its producer has been
	// emitted, or it is an input anchor). Fan-in is permitted — the
	// emittability test is over ALL sources, not a single one.
	for range edges {
		next := -1
		for idx, e := range edges {
			if emitted[idx] {
				continue
			}
			allKnown := true
			for _, b := range e.Assignment {
				if _, ok := nodeMedia[b.Source]; !ok {
					allKnown = false
					break
				}
			}
			if allKnown {
				next = idx
				break
			}
		}
		if next < 0 {
			return nil, disconnectedStrandError(strandIndex)
		}
		emitted[next] = true
		edge := edges[next]

		capDef, ok := registry.GetCachedCap(edge.CapUrn.String())
		if !ok {
			return nil, unknownCapError(edge.CapUrn.String())
		}
		inputIsSequence, outputIsSequence := capDef.SequenceShape()

		// The cap's MAIN input is the argument whose Stdin source URN is the
		// cap's in= (the one special input tag — a cap has exactly one in=).
		// Its slot media URN selects the primary binding in the resolver's
		// assignment. Every other stdin-declaring arg is a convergence input.
		// Compared by tagged-URN equivalence, never as strings; never by arg
		// position.
		inSpecUrn, err := urn.NewMediaUrnFromString(edge.CapUrn.InSpec())
		if err != nil {
			return nil, runtimeMediaInferenceError(
				strandIndex, edge.CapUrn.String(), edge.CapUrn.InSpec(),
				fmt.Sprintf("cap `in=` is not a valid media URN: %v", err),
			)
		}

		var stdinArgStr string
		haveStdinArg := false
		for _, arg := range capDef.Args {
			if arg.IsMainInput(inSpecUrn) {
				stdinArgStr = arg.MediaUrn
				haveStdinArg = true
				break
			}
		}
		if !haveStdinArg {
			return nil, capDoesNotDeclareInputError(strandIndex, edge.CapUrn.String())
		}
		stdinArgUrn, err := urn.NewMediaUrnFromString(stdinArgStr)
		if err != nil {
			return nil, runtimeMediaInferenceError(
				strandIndex, edge.CapUrn.String(), stdinArgStr,
				fmt.Sprintf("stdin arg URN is not a valid media URN: %v", err),
			)
		}

		var primary *EdgeAssignmentBinding
		for i := range edge.Assignment {
			if edge.Assignment[i].CapArgMediaUrn.IsEquivalent(stdinArgUrn) {
				primary = &edge.Assignment[i]
				break
			}
		}
		if primary == nil {
			return nil, noStdinBindingError(strandIndex, edge.CapUrn.String(), stdinArgStr)
		}

		primaryMedia, ok := nodeMedia[primary.Source]
		if !ok {
			panic("primary source media present: the edge was chosen emittable")
		}

		// ForEach synthesis — read the resolver's cardinality decision
		// (edge.IsLoop); the media URN is unchanged (a shape transition, not a
		// type transition).
		if edge.IsLoop {
			foreachStep := planner.NewStrandStep(planner.StepTypeForEach, primaryMedia, primaryMedia)
			foreachStep.MediaDef = primaryMedia
			steps = append(steps, foreachStep)
		}

		runtimeOut, inferErr := edge.CapUrn.InferRuntimeOutputMedia(primaryMedia)
		if inferErr != nil {
			return nil, runtimeMediaInferenceError(
				strandIndex, edge.CapUrn.String(), primaryMedia.String(), inferErr.Error(),
			)
		}

		// Build the full explicit input list. Each binding names its producer:
		// a produced node -> the producing step; an input anchor -> the strand
		// input. Only the PRIMARY (stdin) input may be fed by an input anchor;
		// a non-main arg fed by a non-producer is an argument VALUE, not a
		// wiring, and is exposed hard (see doc comment above).
		inputs := make([]planner.CapInput, 0, len(edge.Assignment))
		for _, b := range edge.Assignment {
			isPrimary := b.CapArgMediaUrn.IsEquivalent(stdinArgUrn)
			var source planner.ArgSourceRef
			if tok, ok := nodeProducer[b.Source]; ok {
				source = planner.NewArgSourceStep(tok)
			} else if isPrimary {
				source = planner.NewArgSourceStrandInput()
			} else {
				return nil, nonProducerSecondaryArgError(strandIndex, edge.CapUrn.String(), b.CapArgMediaUrn.String())
			}
			inputs = append(inputs, planner.CapInput{ArgUrn: b.CapArgMediaUrn, Source: source})
		}

		step := planner.NewStrandStep(planner.StepTypeCap, primaryMedia, runtimeOut)
		step.CapUrnVal = edge.CapUrn
		step.StepTitle = capDef.Title
		step.SpecificityVal = edge.CapUrn.Specificity()
		step.InputIsSequence = inputIsSequence
		step.OutputIsSequence = outputIsSequence
		step.Inputs = inputs
		// Preserve the resolved edge's stable identity so live updates map back
		// and so convergence args can reference this step as their producer.
		step.TokenId = edge.TokenId

		nodeMedia[edge.Target] = runtimeOut
		nodeProducer[edge.Target] = edge.TokenId
		steps = append(steps, step)
	}

	// The strand's realized target media is its output anchor's runtime media.
	// A well-formed strand has exactly one output anchor, produced by a cap
	// above; a missing anchor or media is a structural bug, exposed hard.
	outputAnchors := machineStrand.OutputAnchorIds()
	if len(outputAnchors) == 0 {
		return nil, disconnectedStrandError(strandIndex)
	}
	targetMediaUrn, ok := nodeMedia[outputAnchors[0]]
	if !ok {
		return nil, disconnectedStrandError(strandIndex)
	}

	capStepCount := 0
	for _, s := range steps {
		if s.StepType == planner.StepTypeCap {
			capStepCount++
		}
	}

	return &planner.Strand{
		Steps:          steps,
		SourceMediaUrn: sourceUrn,
		TargetMediaUrn: targetMediaUrn,
		TotalSteps:     len(steps),
		CapStepCount:   capStepCount,
		Description:    fmt.Sprintf("realized machine strand %d", strandIndex),
	}, nil
}
