package planner

import (
	"encoding/json"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTestCapForGraph creates a minimal cap for live_cap_graph tests.
func makeTestCapForGraph(inSpec, outSpec, op, title string) *cap.Cap {
	capUrn := urn.NewCapUrn(inSpec, outSpec, map[string]string{"op": op})
	return cap.NewCapWithArgs(capUrn, title, "test", nil)
}

// TEST772: Tests FindPathsToExactTarget() finds multi-step paths
// Verifies that paths through intermediate nodes are found correctly
func Test772_find_paths_finds_multi_step_paths(t *testing.T) {
	graph := NewLiveCapGraph()

	cap1 := makeTestCapForGraph("media:a", "media:b", "step1", "A to B")
	cap2 := makeTestCapForGraph("media:b", "media:c", "step2", "B to C")

	graph.AddCap(cap1)
	graph.AddCap(cap2)

	source, err := urn.NewMediaUrnFromString("media:a")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:c")
	require.NoError(t, err)

	paths := graph.FindPathsToExactTarget(source, target, false, 5, 10)

	assert.Equal(t, 1, len(paths), "Should find exactly one path through intermediate node")
	assert.Equal(t, 2, len(paths[0].Steps), "Path should have 2 steps (A->B, B->C)")
	assert.Equal(t, "A to B", paths[0].Steps[0].Title())
	assert.Equal(t, "B to C", paths[0].Steps[1].Title())
}

// TEST773: Tests FindPathsToExactTarget() returns empty when no path exists
// Verifies that pathfinding returns no paths when target is unreachable
func Test773_find_paths_returns_empty_when_no_path(t *testing.T) {
	graph := NewLiveCapGraph()

	cap1 := makeTestCapForGraph("media:a", "media:b", "step1", "A to B")
	graph.AddCap(cap1)

	source, err := urn.NewMediaUrnFromString("media:a")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:c")
	require.NoError(t, err)

	paths := graph.FindPathsToExactTarget(source, target, false, 5, 10)

	assert.Empty(t, paths, "Should find no paths when target is unreachable")
}

// TEST774: Tests GetReachableTargets() returns all reachable targets
// Verifies that reachable targets include direct cap targets
func Test774_get_reachable_targets_finds_all_targets(t *testing.T) {
	graph := NewLiveCapGraph()

	cap1 := makeTestCapForGraph("media:a", "media:b", "step1", "A to B")
	cap2 := makeTestCapForGraph("media:a", "media:d", "step3", "A to D")

	graph.AddCap(cap1)
	graph.AddCap(cap2)

	source, err := urn.NewMediaUrnFromString("media:a")
	require.NoError(t, err)

	targets := graph.GetReachableTargets(source, false, 5)

	mediaB, err := urn.NewMediaUrnFromString("media:b")
	require.NoError(t, err)
	mediaD, err := urn.NewMediaUrnFromString("media:d")
	require.NoError(t, err)

	reaches := func(needle *urn.MediaUrn) bool {
		for _, t := range targets {
			if t.MediaSpec.IsEquivalent(needle) {
				return true
			}
		}
		return false
	}

	assert.True(t, reaches(mediaB), "B should be reachable")
	assert.True(t, reaches(mediaD), "D should be reachable")
}

// TEST777: Tests type checking prevents using PDF-specific cap with PNG input
func Test777_type_mismatch_pdf_cap_does_not_match_png_input(t *testing.T) {
	graph := NewLiveCapGraph()
	pdfToText := makeTestCapForGraph("media:pdf", "media:textable", "pdf2text", "PDF to Text")
	graph.AddCap(pdfToText)

	source, err := urn.NewMediaUrnFromString("media:png")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)

	paths := graph.FindPathsToExactTarget(source, target, false, 5, 10)
	assert.Empty(t, paths, "Should NOT find path from PNG to text via PDF cap")
}

// TEST778: Tests type checking prevents using PNG-specific cap with PDF input
func Test778_type_mismatch_png_cap_does_not_match_pdf_input(t *testing.T) {
	graph := NewLiveCapGraph()
	pngToThumb := makeTestCapForGraph("media:png", "media:thumbnail", "png2thumb", "PNG to Thumbnail")
	graph.AddCap(pngToThumb)

	source, err := urn.NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:thumbnail")
	require.NoError(t, err)

	paths := graph.FindPathsToExactTarget(source, target, false, 5, 10)
	assert.Empty(t, paths, "Should NOT find path from PDF to thumbnail via PNG cap")
}

// TEST779: Tests get_reachable_targets() only returns targets reachable via type-compatible caps
func Test779_get_reachable_targets_respects_type_matching(t *testing.T) {
	graph := NewLiveCapGraph()
	pdfToText := makeTestCapForGraph("media:pdf", "media:textable", "pdf2text", "PDF to Text")
	pngToThumb := makeTestCapForGraph("media:png", "media:thumbnail", "png2thumb", "PNG to Thumbnail")
	graph.AddCap(pdfToText)
	graph.AddCap(pngToThumb)

	mediaTextable, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)
	mediaThumbnail, err := urn.NewMediaUrnFromString("media:thumbnail")
	require.NoError(t, err)

	reaches := func(targets []ReachableTargetInfo, needle *urn.MediaUrn) bool {
		for _, t := range targets {
			if t.MediaSpec.IsEquivalent(needle) {
				return true
			}
		}
		return false
	}

	// PNG should reach thumbnail but NOT textable
	pngSource, err := urn.NewMediaUrnFromString("media:png")
	require.NoError(t, err)
	pngTargets := graph.GetReachableTargets(pngSource, false, 5)
	assert.True(t, reaches(pngTargets, mediaThumbnail), "PNG should reach thumbnail")
	assert.False(t, reaches(pngTargets, mediaTextable), "PNG should NOT reach textable")

	// PDF should reach textable but NOT thumbnail
	pdfSource, err := urn.NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	pdfTargets := graph.GetReachableTargets(pdfSource, false, 5)
	assert.True(t, reaches(pdfTargets, mediaTextable), "PDF should reach textable")
	assert.False(t, reaches(pdfTargets, mediaThumbnail), "PDF should NOT reach thumbnail")
}

// TEST781: Tests find_paths_to_exact_target() enforces type compatibility across multi-step chains
func Test781_find_paths_respects_type_chain(t *testing.T) {
	graph := NewLiveCapGraph()
	resizePng := makeTestCapForGraph("media:png", "media:resized-png", "resize", "Resize PNG")
	toThumb := makeTestCapForGraph("media:resized-png", "media:thumbnail", "thumb", "To Thumbnail")
	graph.AddCap(resizePng)
	graph.AddCap(toThumb)

	pngSource, err := urn.NewMediaUrnFromString("media:png")
	require.NoError(t, err)
	thumbTarget, err := urn.NewMediaUrnFromString("media:thumbnail")
	require.NoError(t, err)
	pdfSource, err := urn.NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)

	// PNG should find path through resized-png to thumbnail
	pngPaths := graph.FindPathsToExactTarget(pngSource, thumbTarget, false, 5, 10)
	assert.Equal(t, 1, len(pngPaths), "Should find 1 path from PNG to thumbnail")
	if len(pngPaths) == 1 {
		assert.Equal(t, 2, len(pngPaths[0].Steps), "Path should have 2 steps")
	}

	// PDF should NOT find path to thumbnail (no PDF->resized-png cap)
	pdfPaths := graph.FindPathsToExactTarget(pdfSource, thumbTarget, false, 5, 10)
	assert.Empty(t, pdfPaths, "Should find NO paths from PDF to thumbnail (type mismatch)")
}

// TEST787: Tests find_paths_to_exact_target() sorts paths by length, preferring shorter ones
func Test787_find_paths_sorting_prefers_shorter(t *testing.T) {
	graph := NewLiveCapGraph()
	direct := makeTestCapForGraph("media:format-a", "media:format-c", "direct", "Direct")
	step1 := makeTestCapForGraph("media:format-a", "media:format-b", "step1", "Step 1")
	step2 := makeTestCapForGraph("media:format-b", "media:format-c", "step2", "Step 2")
	graph.AddCap(direct)
	graph.AddCap(step1)
	graph.AddCap(step2)

	source, err := urn.NewMediaUrnFromString("media:format-a")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:format-c")
	require.NoError(t, err)

	paths := graph.FindPathsToExactTarget(source, target, false, 5, 10)
	assert.GreaterOrEqual(t, len(paths), 2, "Should find at least 2 paths")
	if len(paths) >= 1 {
		assert.Equal(t, 1, len(paths[0].Steps), "Shortest path should be first (1 step)")
		assert.Equal(t, "Direct", paths[0].Steps[0].Title())
	}
}

// TEST788: ForEach is only synthesized when is_sequence=true
func Test788_foreach_only_with_sequence_input(t *testing.T) {
	graph := NewLiveCapGraph()
	disbind := makeTestCapForGraph("media:pdf", "media:page;textable", "disbind", "Disbind PDF")
	choose := makeTestCapForGraph("media:textable", "media:decision;json;record;textable", "choose", "Make a Decision")
	graph.SyncFromCaps([]*cap.Cap{disbind, choose})
	nodeCount, edgeCount := graph.Stats()
	assert.Equal(t, 2, edgeCount, "Graph should contain exactly 2 Cap edges")
	_ = nodeCount

	source, err := urn.NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:decision;json;record;textable")
	require.NoError(t, err)

	hasForEach := func(paths []*Strand) bool {
		for _, p := range paths {
			for _, s := range p.Steps {
				if s.StepType == StepTypeForEach {
					return true
				}
			}
		}
		return false
	}

	// Scalar input: no ForEach
	scalarPaths := graph.FindPathsToExactTarget(source, target, false, 10, 20)
	assert.False(t, hasForEach(scalarPaths), "Scalar input should NOT produce ForEach")
	assert.NotEmpty(t, scalarPaths, "Should find direct path disbind → choose")

	// Sequence input: ForEach should appear
	seqPaths := graph.FindPathsToExactTarget(source, target, true, 10, 20)
	assert.True(t, hasForEach(seqPaths), "Sequence input should produce ForEach step")
}

// TEST1111: ForEach works for user-provided list sources not in the graph.
// User provides media:list;textable;txt with is_sequence=true → ForEach+cap path found.
func Test1111_foreach_for_user_provided_list_source(t *testing.T) {
	graph := NewLiveCapGraph()

	// Cap: textable → decision (accepts singular textable)
	makeDecision := makeTestCapForGraph(
		"media:textable",
		"media:decision;json;record;textable",
		"make_decision",
		"Make Decision",
	)
	graph.SyncFromCaps([]*cap.Cap{makeDecision})

	source, err := urn.NewMediaUrnFromString("media:list;textable;txt")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:decision;json;record;textable")
	require.NoError(t, err)

	// User provides multiple files → is_sequence=true
	paths := graph.FindPathsToExactTarget(source, target, true, 10, 20)

	// Expected path: ForEach → make_decision
	var foundPath *Strand
	for _, p := range paths {
		if len(p.Steps) == 2 &&
			p.Steps[0].StepType == StepTypeForEach &&
			p.Steps[1].StepType == StepTypeCap {
			foundPath = p
			break
		}
	}
	require.NotNil(t, foundPath,
		"Should find path: ForEach → make_decision. User-provided list source media:list;textable;txt must be iterable. Found %d paths.",
		len(paths))

	// ForEach step media spec should be equivalent to source
	foreachStep := foundPath.Steps[0]
	assert.True(t, foreachStep.MediaSpec.IsEquivalent(source), "ForEach MediaSpec should be equivalent to source")
}

// TEST1112: Collect is not synthesized during path finding.
// Reaching a list target type requires the cap itself to output a list type.
func Test1112_no_collect_in_path_finding(t *testing.T) {
	graph := NewLiveCapGraph()

	summarize := makeTestCapForGraph(
		"media:textable",
		"media:summary;textable",
		"summarize",
		"Summarize",
	)
	graph.SyncFromCaps([]*cap.Cap{summarize})

	source, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)
	// list;summary;textable is a different semantic type — can't reach it
	// without a cap that outputs it or a Collect step (not synthesized)
	target, err := urn.NewMediaUrnFromString("media:list;summary;textable")
	require.NoError(t, err)

	paths := graph.FindPathsToExactTarget(source, target, false, 10, 20)
	assert.Empty(t, paths, "Should NOT find path to list type without a cap that produces it")
}

// TEST1113: Multi-cap path without Collect — Collect is not synthesized.
// PDF→disbind→page→summarize→summary. CapStepCount=2.
func Test1113_multi_cap_path_no_collect(t *testing.T) {
	graph := NewLiveCapGraph()

	disbind := makeTestCapForGraph("media:pdf", "media:page;textable", "disbind", "Disbind PDF")
	summarize := makeTestCapForGraph(
		"media:page;textable",
		"media:summary;textable",
		"summarize",
		"Summarize Page",
	)
	graph.SyncFromCaps([]*cap.Cap{disbind, summarize})

	source, err := urn.NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:summary;textable")
	require.NoError(t, err)

	paths := graph.FindPathsToExactTarget(source, target, false, 10, 20)
	require.NotEmpty(t, paths, "Should find direct cap path")
	assert.Equal(t, 2, paths[0].CapStepCount, "Should have 2 cap steps")
}

// TEST1114: Graph stores only Cap edges after SyncFromCaps.
// All stored edges must have IsCap() == true.
func Test1114_graph_stores_only_cap_edges(t *testing.T) {
	graph := NewLiveCapGraph()

	caps := []*cap.Cap{
		makeTestCapForGraph("media:pdf", "media:page;textable", "disbind", "Disbind"),
		makeTestCapForGraph("media:page;textable", "media:summary;textable", "summarize", "Summarize"),
		makeTestCapForGraph("media:textable", "media:decision;json;record;textable", "decide", "Decide"),
	}
	graph.SyncFromCaps(caps)

	assert.Equal(t, 3, len(graph.edges), "Should have exactly 3 Cap edges")
	for _, edge := range graph.edges {
		assert.True(t, edge.IsCap(),
			"Stored edge should be a Cap edge, not a cardinality transition")
	}
}

// TEST1115: ForEach is synthesized when is_sequence=true AND caps can consume items.
// getOutgoingEdges(source, true) → ForEach edge present, next_is_seq=false.
func Test1115_dynamic_foreach_with_is_sequence(t *testing.T) {
	graph := NewLiveCapGraph()

	// Need a cap that accepts the source type for ForEach to be synthesized
	c := makeTestCapForGraph(
		"media:textable",
		"media:summary;textable",
		"summarize",
		"Summarize",
	)
	graph.SyncFromCaps([]*cap.Cap{c})

	source, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)

	edges, outSeqs := graph.getOutgoingEdges(source, true)

	var foreachIdx = -1
	for i, e := range edges {
		if e.Type == EdgeTypeForEach {
			foreachIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, foreachIdx, 0, "Should synthesize ForEach when is_sequence=true and caps exist")
	assert.False(t, outSeqs[foreachIdx], "ForEach should flip is_sequence to false")

	fe := edges[foreachIdx]
	assert.True(t, fe.FromSpec.IsEquivalent(source), "ForEach FromSpec should be the source")
	assert.True(t, fe.ToSpec.IsEquivalent(source), "ForEach ToSpec should be the same URN")
}

// TEST1116: Collect is never synthesized during path finding.
// getOutgoingEdges for both scalar and sequence returns no Collect edges.
func Test1116_collect_never_synthesized(t *testing.T) {
	graph := NewLiveCapGraph()

	source, err := urn.NewMediaUrnFromString("media:page;textable")
	require.NoError(t, err)

	// Neither scalar nor sequence should produce Collect
	edgesScalar, _ := graph.getOutgoingEdges(source, false)
	for _, e := range edgesScalar {
		assert.NotEqual(t, EdgeTypeCollect, e.Type, "Should NOT synthesize Collect for scalar")
	}

	edgesSeq, _ := graph.getOutgoingEdges(source, true)
	for _, e := range edgesSeq {
		assert.NotEqual(t, EdgeTypeCollect, e.Type, "Should NOT synthesize Collect for sequence")
	}
}

// TEST1117: ForEach is NOT synthesized when is_sequence=false.
// Even with caps that could consume, ForEach requires is_sequence=true.
func Test1117_no_foreach_when_not_sequence(t *testing.T) {
	graph := NewLiveCapGraph()

	c := makeTestCapForGraph(
		"media:textable",
		"media:summary;textable",
		"summarize",
		"Summarize",
	)
	graph.SyncFromCaps([]*cap.Cap{c})

	source, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)

	edges, _ := graph.getOutgoingEdges(source, false)
	for _, e := range edges {
		assert.NotEqual(t, EdgeTypeForEach, e.Type, "Should NOT synthesize ForEach when is_sequence=false")
	}
}

// TEST1118: ForEach not synthesized without cap consumers even with is_sequence=true.
func Test1118_no_foreach_without_cap_consumers(t *testing.T) {
	graph := NewLiveCapGraph() // empty graph — no caps

	source, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)

	edges, _ := graph.getOutgoingEdges(source, true)
	for _, e := range edges {
		assert.NotEqual(t, EdgeTypeForEach, e.Type, "Should NOT synthesize ForEach without cap consumers")
	}
}

// TEST1289: BFS reachable targets includes the source itself when round-trip paths exist.
// A→B and B→A means A is reachable from A (via A→B→A).
func Test1289_bfs_reachable_includes_source_roundtrip(t *testing.T) {
	graph := NewLiveCapGraph()

	// textable → integer (coerce)
	graph.AddCap(makeTestCapForGraph(
		"media:textable",
		"media:integer;numeric;textable",
		"coerce_to_int",
		"Coerce to Integer",
	))
	// integer → textable (coerce back)
	graph.AddCap(makeTestCapForGraph(
		"media:integer;numeric;textable",
		"media:textable",
		"coerce_to_text",
		"Coerce to Text",
	))

	source, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)

	targets := graph.GetReachableTargets(source, false, 5)

	hasSelf := false
	for _, tgt := range targets {
		if tgt.MediaSpec.IsEquivalent(source) {
			hasSelf = true
			break
		}
	}
	assert.True(t, hasSelf, "BFS must find source as reachable target in round-trip graph")
}

// TEST1290: IDDFS find_paths_to_exact_target finds round-trip paths when source == target.
func Test1290_iddfs_finds_roundtrip_paths(t *testing.T) {
	graph := NewLiveCapGraph()

	// textable → integer
	graph.AddCap(makeTestCapForGraph(
		"media:textable",
		"media:integer;numeric;textable",
		"coerce_to_int",
		"Coerce to Integer",
	))
	// integer → textable
	graph.AddCap(makeTestCapForGraph(
		"media:integer;numeric;textable",
		"media:textable",
		"coerce_to_text",
		"Coerce to Text",
	))

	source, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)

	paths := graph.FindPathsToExactTarget(source, target, false, 5, 100)
	require.NotEmpty(t, paths, "IDDFS must find round-trip paths (textable→integer→textable). Got 0 paths.")

	// The shortest round-trip should be 2 steps
	shortest := paths[0]
	for _, p := range paths {
		if p.TotalSteps < shortest.TotalSteps {
			shortest = p
		}
	}
	assert.Equal(t, 2, shortest.TotalSteps, "Shortest round-trip should be 2 steps (coerce + coerce back)")
}

// TEST1291: IDDFS round-trip paths are also found with is_sequence=true.
func Test1291_iddfs_roundtrip_with_sequence(t *testing.T) {
	graph := NewLiveCapGraph()

	// textable → integer
	graph.AddCap(makeTestCapForGraph(
		"media:textable",
		"media:integer;numeric;textable",
		"coerce_to_int",
		"Coerce to Integer",
	))
	// integer → textable
	graph.AddCap(makeTestCapForGraph(
		"media:integer;numeric;textable",
		"media:textable",
		"coerce_to_text",
		"Coerce to Text",
	))

	source, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:textable")
	require.NoError(t, err)

	// With is_sequence=true, the path goes through ForEach first
	paths := graph.FindPathsToExactTarget(source, target, true, 5, 100)
	assert.NotEmpty(t, paths, "IDDFS must find round-trip paths even with is_sequence=true. Got 0 paths.")
}

// TEST1292: BFS and IDDFS agree that round-trip targets exist.
// If BFS says target X is reachable from source X, IDDFS must find at least one path.
func Test1292_bfs_iddfs_roundtrip_consistency(t *testing.T) {
	graph := NewLiveCapGraph()

	// Build a small graph: A→B, B→C, C→A
	graph.AddCap(makeTestCapForGraph("media:a", "media:b", "a_to_b", "A to B"))
	graph.AddCap(makeTestCapForGraph("media:b", "media:c", "b_to_c", "B to C"))
	graph.AddCap(makeTestCapForGraph("media:c", "media:a", "c_to_a", "C to A"))

	source, err := urn.NewMediaUrnFromString("media:a")
	require.NoError(t, err)

	// BFS should find source as reachable (via A→B→C→A)
	bfsTargets := graph.GetReachableTargets(source, false, 5)
	bfsHasSelf := false
	for _, tgt := range bfsTargets {
		if tgt.MediaSpec.IsEquivalent(source) {
			bfsHasSelf = true
			break
		}
	}
	assert.True(t, bfsHasSelf, "BFS must find A reachable from A in cyclic graph")

	// IDDFS must also find paths
	target, err := urn.NewMediaUrnFromString("media:a")
	require.NoError(t, err)
	iddfsPaths := graph.FindPathsToExactTarget(source, target, false, 5, 100)
	require.NotEmpty(t, iddfsPaths,
		"IDDFS must find round-trip paths when BFS says target is reachable. BFS found %d targets including self, IDDFS found 0 paths.",
		len(bfsTargets))

	// Shortest path should be 3 steps (A→B→C→A)
	shortest := iddfsPaths[0]
	for _, p := range iddfsPaths {
		if p.TotalSteps < shortest.TotalSteps {
			shortest = p
		}
	}
	assert.Equal(t, 3, shortest.TotalSteps)
}

// TEST1293: IDDFS round-trip does not produce paths with 0 cap steps.
// No round-trip should exist when there's no return edge.
func Test1293_roundtrip_requires_cap_steps(t *testing.T) {
	graph := NewLiveCapGraph()

	// Only one direction — no round trip possible
	graph.AddCap(makeTestCapForGraph("media:a", "media:b", "a_to_b", "A to B"))

	source, err := urn.NewMediaUrnFromString("media:a")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:a")
	require.NoError(t, err)

	paths := graph.FindPathsToExactTarget(source, target, false, 5, 100)
	assert.Empty(t, paths, "No round-trip should exist when there's no return edge. Got %d paths.", len(paths))
}

// TEST789: Tests that caps loaded from JSON have correct in_spec/out_spec
func Test789_cap_from_json_has_valid_specs(t *testing.T) {
	jsonStr := `{
		"urn": "cap:in=media:pdf;op=disbind;out=\"media:disbound-page;textable\"",
		"command": "disbind",
		"title": "Disbind PDF",
		"args": [],
		"output": null
	}`

	var c cap.Cap
	err := json.Unmarshal([]byte(jsonStr), &c)
	require.NoError(t, err, "Failed to parse cap JSON")

	inSpec := c.Urn.InSpec()
	outSpec := c.Urn.OutSpec()

	assert.NotEmpty(t, inSpec, "in_spec should not be empty")
	assert.NotEmpty(t, outSpec, "out_spec should not be empty")
	assert.Equal(t, "media:pdf", inSpec)
	assert.Contains(t, outSpec, "disbound-page")
}

// TEST790: Tests identity_urn is specific and doesn't match everything
func Test790_identity_urn_is_specific(t *testing.T) {
	// The identity CapUrn has wildcard in/out specs ("media:")
	identityUrn := urn.NewCapUrn("media:", "media:", map[string]string{})

	assert.Equal(t, "media:", identityUrn.InSpec())
	assert.Equal(t, "media:", identityUrn.OutSpec())

	// A specific cap should NOT be equivalent to identity
	specificCap, err := urn.NewCapUrnFromString(`cap:in=media:pdf;op=disbind;out="media:disbound-page;textable"`)
	require.NoError(t, err)

	assert.False(t, specificCap.IsEquivalent(identityUrn),
		"A specific disbind cap should NOT be equivalent to identity")
}

// TEST1150: Adding one cap creates one edge and makes its output reachable in one step.
func Test1150_add_cap_and_basic_traversal(t *testing.T) {
	graph := NewLiveCapGraph()
	c := makeTestCapForGraph("media:pdf", "media:extracted-text", "extract_text", "Extract Text")
	graph.AddCap(c)

	nodeCount, edgeCount := graph.Stats()
	assert.Equal(t, 1, edgeCount)
	assert.Equal(t, 2, nodeCount)

	source, err := urn.NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	targets := graph.GetReachableTargets(source, false, 5)

	extractedText, err := urn.NewMediaUrnFromString("media:extracted-text")
	require.NoError(t, err)

	var found *ReachableTargetInfo
	for i := range targets {
		if targets[i].MediaSpec.IsEquivalent(extractedText) {
			found = &targets[i]
			break
		}
	}
	require.NotNil(t, found, "extracted-text should be reachable")
	assert.Equal(t, 1, found.MinPathLength)
}

// TEST1151: Exact target lookup prefers the direct singular or list-producing path over longer alternatives.
func Test1151_exact_vs_conformance_matching(t *testing.T) {
	singular, err := urn.NewMediaUrnFromString("media:analysis-result")
	require.NoError(t, err)
	list, err := urn.NewMediaUrnFromString("media:analysis-result;list")
	require.NoError(t, err)

	// These should NOT be equivalent
	assert.False(t, singular.IsEquivalent(list), "singular and list should NOT be equivalent")
	assert.False(t, list.IsEquivalent(singular), "list and singular should NOT be equivalent")

	graph := NewLiveCapGraph()

	// pdf → result (singular)
	cap1 := makeTestCapForGraph("media:pdf", "media:analysis-result", "analyze", "Analyze PDF")
	graph.AddCap(cap1)

	// pdf → result;list (plural)
	cap2 := makeTestCapForGraph("media:pdf", "media:analysis-result;list", "analyze_multi", "Analyze PDF Multi")
	graph.AddCap(cap2)

	source, err := urn.NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)

	// Query for singular result — direct path should rank first
	targetSingular, err := urn.NewMediaUrnFromString("media:analysis-result")
	require.NoError(t, err)
	pathsSingular := graph.FindPathsToExactTarget(source, targetSingular, false, 5, 10)
	require.GreaterOrEqual(t, len(pathsSingular), 1, "singular query should find at least 1 path")
	assert.Equal(t, "Analyze PDF", pathsSingular[0].Steps[0].Title(),
		"First path should be the direct cap (fewer total steps)")

	// Query for list result — direct path should rank first
	targetPlural, err := urn.NewMediaUrnFromString("media:analysis-result;list")
	require.NoError(t, err)
	pathsPlural := graph.FindPathsToExactTarget(source, targetPlural, false, 5, 10)
	require.GreaterOrEqual(t, len(pathsPlural), 1, "list query should find at least 1 path")
	assert.Equal(t, "Analyze PDF Multi", pathsPlural[0].Steps[0].Title(),
		"First path should be the direct cap (fewer total steps)")
}

// TEST1152: Path finding returns the expected two-cap chain through an intermediate media type.
func Test1152_multi_step_path(t *testing.T) {
	graph := NewLiveCapGraph()

	cap1 := makeTestCapForGraph("media:pdf", "media:extracted-text", "extract", "Extract")
	cap2 := makeTestCapForGraph("media:extracted-text", "media:summary-text", "summarize", "Summarize")
	graph.AddCap(cap1)
	graph.AddCap(cap2)

	source, err := urn.NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:summary-text")
	require.NoError(t, err)

	paths := graph.FindPathsToExactTarget(source, target, false, 5, 10)
	require.Equal(t, 1, len(paths))
	assert.Equal(t, 2, paths[0].TotalSteps)
	assert.Equal(t, "Extract", paths[0].Steps[0].Title())
	assert.Equal(t, "Summarize", paths[0].Steps[1].Title())
}

// TEST1153: Repeated path searches return the same path order for the same graph and target.
func Test1153_deterministic_ordering(t *testing.T) {
	graph := NewLiveCapGraph()

	cap1 := makeTestCapForGraph("media:pdf", "media:extracted-text", "extract_a", "Extract A")
	cap2 := makeTestCapForGraph("media:pdf", "media:extracted-text", "extract_b", "Extract B")
	graph.AddCap(cap1)
	graph.AddCap(cap2)

	source, err := urn.NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	target, err := urn.NewMediaUrnFromString("media:extracted-text")
	require.NoError(t, err)

	paths1 := graph.FindPathsToExactTarget(source, target, false, 5, 10)
	paths2 := graph.FindPathsToExactTarget(source, target, false, 5, 10)

	require.Equal(t, len(paths1), len(paths2))
	for i := range paths1 {
		u1 := paths1[i].Steps[0].CapUrnVal
		u2 := paths2[i].Steps[0].CapUrnVal
		require.NotNil(t, u1)
		require.NotNil(t, u2)
		assert.True(t, u1.IsEquivalent(u2),
			"determinism: first cap URN differs across runs: %s vs %s", u1, u2)
	}
}

// TEST1154: SyncFromCaps replaces the existing graph contents with the new cap set.
func Test1154_sync_from_caps(t *testing.T) {
	graph := NewLiveCapGraph()

	caps := []*cap.Cap{
		makeTestCapForGraph("media:pdf", "media:extracted-text", "op1", "Op1"),
		makeTestCapForGraph("media:extracted-text", "media:summary-text", "op2", "Op2"),
	}
	graph.SyncFromCaps(caps)

	nodeCount, edgeCount := graph.Stats()
	assert.Equal(t, 2, edgeCount)
	assert.Equal(t, 3, nodeCount)

	// Sync again with different caps — should replace
	newCaps := []*cap.Cap{
		makeTestCapForGraph("media:image", "media:extracted-text", "ocr", "OCR"),
	}
	graph.SyncFromCaps(newCaps)

	nodeCount, edgeCount = graph.Stats()
	assert.Equal(t, 1, edgeCount)
	assert.Equal(t, 2, nodeCount)
}
