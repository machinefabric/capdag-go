package planner

import (
	"sort"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// LiveMachinePlanEdgeType identifies the type of edge in the capability graph.
type LiveMachinePlanEdgeType int

const (
	EdgeTypeCap LiveMachinePlanEdgeType = iota
	EdgeTypeForEach
	EdgeTypeCollect
)

// LiveMachinePlanEdge represents an edge in the live capability graph.
type LiveMachinePlanEdge struct {
	FromSpec         *urn.MediaUrn
	ToSpec           *urn.MediaUrn
	Type             LiveMachinePlanEdgeType
	CapUrnVal        *urn.CapUrn
	CapTitle         string
	SpecificityVal   int
	InputIsSequence  bool
	OutputIsSequence bool
}

// Title returns a human-readable title for this edge.
func (e *LiveMachinePlanEdge) Title() string {
	switch e.Type {
	case EdgeTypeCap:
		return e.CapTitle
	case EdgeTypeForEach:
		return "ForEach (iterate over list)"
	case EdgeTypeCollect:
		return "Collect (gather results)"
	}
	return ""
}

// Specificity returns the specificity of this edge.
func (e *LiveMachinePlanEdge) Specificity() int {
	if e.Type == EdgeTypeCap {
		return e.SpecificityVal
	}
	return 0
}

// IsCap checks if this is a cap edge.
func (e *LiveMachinePlanEdge) IsCap() bool {
	return e.Type == EdgeTypeCap
}

// GetCapUrn returns the cap URN if this is a cap edge.
func (e *LiveMachinePlanEdge) GetCapUrn() *urn.CapUrn {
	if e.Type == EdgeTypeCap {
		return e.CapUrnVal
	}
	return nil
}

// StrandStepType identifies the type of step in a capability chain path.
type StrandStepType int

const (
	StepTypeCap StrandStepType = iota
	StepTypeForEach
	StepTypeCollect
)

// ArgSourceKind identifies which of the two shapes an ArgSourceRef carries.
type ArgSourceKind int

const (
	// ArgSourceStrandInput — fed by the strand's input anchor — the strand's own
	// input flows into this arg.
	ArgSourceStrandInput ArgSourceKind = iota
	// ArgSourceStep — fed by another cap step's output, identified by that
	// step's stable TokenId.
	ArgSourceStep
)

// ArgSourceRef is the producer of one of a cap step's inputs.
//
// A Strand is a DAG of steps: an input is fed either by the strand's own input
// (an input anchor) or by another cap step's output. There are no positional
// assumptions — every input names its producer explicitly, so fan-out (one
// producer feeding several caps' main inputs) and convergence (one cap fed by
// several producers) are both expressible.
// Matches Rust: pub enum ArgSourceRef { StrandInput, Step { token_id: String } }
type ArgSourceRef struct {
	Kind ArgSourceKind
	// TokenId is the producing step's stable identity. Only meaningful when
	// Kind == ArgSourceStep.
	TokenId string
}

// NewArgSourceStrandInput builds an ArgSourceRef fed by the strand's own input.
func NewArgSourceStrandInput() ArgSourceRef {
	return ArgSourceRef{Kind: ArgSourceStrandInput}
}

// NewArgSourceStep builds an ArgSourceRef fed by another cap step's output.
func NewArgSourceStep(tokenId string) ArgSourceRef {
	return ArgSourceRef{Kind: ArgSourceStep, TokenId: tokenId}
}

// CapInput is one input to a cap step: the cap argument it feeds (by the
// argument's slot media URN) and the producer of that input.
//
// A cap step lists ALL of its data-flow inputs — the primary (stdin/main) input
// and every convergence input — with no positional ordering assumptions. The
// PRIMARY input is the one whose ArgUrn is the cap's stdin argument URN; it
// threads the step's FromSpec runtime media. Arg URNs into one cap are all
// distinct (RULE1: a cap's args have unique media URNs), so ArgUrn alone
// identifies the slot.
// Matches Rust: pub struct CapInput { pub arg_urn: MediaUrn, pub source: ArgSourceRef }
type CapInput struct {
	ArgUrn *urn.MediaUrn
	Source ArgSourceRef
}

// StrandStep contains information about a single step in a path.
type StrandStep struct {
	// TokenId is the stable per-step identity, minted once when the step is
	// created (the very source of a resolved strand). It is the single key that
	// ties this element of the realized graph to every live update the run
	// emits for it — so a repeated cap URN in one strand is never ambiguous.
	// Alias-free and notation-independent; it travels verbatim through
	// serialization, the run's persisted resolved strand, the render payload,
	// and every progress message.
	TokenId          string
	StepType         StrandStepType
	FromSpec         *urn.MediaUrn
	ToSpec           *urn.MediaUrn
	CapUrnVal        *urn.CapUrn
	StepTitle        string
	SpecificityVal   int
	MediaDef         *urn.MediaUrn
	InputIsSequence  bool
	OutputIsSequence bool
	// Inputs lists ALL of this cap step's data-flow inputs — the primary
	// (stdin) input plus any convergence inputs — each naming its producer
	// explicitly. Only meaningful when StepType == StepTypeCap. The primary is
	// the input whose ArgUrn is the cap's stdin argument URN.
	Inputs []CapInput
}

// NewStrandStep creates a step, minting its stable TokenId. This is the
// canonical way a step is born in production, so every step in a resolved
// strand carries an id from creation; deserialization preserves the stored id
// verbatim.
// Matches Rust: impl StrandStep { pub fn new(...) -> Self }
func NewStrandStep(stepType StrandStepType, fromSpec, toSpec *urn.MediaUrn) *StrandStep {
	return &StrandStep{
		TokenId:  uuid.New().String(),
		StepType: stepType,
		FromSpec: fromSpec,
		ToSpec:   toSpec,
	}
}

// Title returns the title for this step.
func (s *StrandStep) Title() string {
	switch s.StepType {
	case StepTypeCap:
		return s.StepTitle
	case StepTypeForEach:
		return "ForEach"
	case StepTypeCollect:
		return "Collect"
	}
	return ""
}

// Specificity returns the specificity of this step.
func (s *StrandStep) Specificity() int {
	if s.StepType == StepTypeCap {
		return s.SpecificityVal
	}
	return 0
}

// GetCapUrn returns the cap URN if this is a cap step.
func (s *StrandStep) GetCapUrn() *urn.CapUrn {
	if s.StepType == StepTypeCap {
		return s.CapUrnVal
	}
	return nil
}

// IsCap checks if this is a cap step.
func (s *StrandStep) IsCap() bool {
	return s.StepType == StepTypeCap
}

// Strand contains information about a complete capability chain path.
type Strand struct {
	Steps          []*StrandStep
	SourceMediaUrn *urn.MediaUrn
	TargetMediaUrn *urn.MediaUrn
	TotalSteps     int
	CapStepCount   int
	Description    string
}

// ReachableTargetInfo contains information about a reachable target.
type ReachableTargetInfo struct {
	MediaDef      *urn.MediaUrn
	DisplayName   string
	MinPathLength int
	PathCount     int
}

// PathFindingEvent is an event emitted during streaming path finding.
type PathFindingEvent interface {
	isPathFindingEvent()
}

// PathFindingEventDepthComplete is emitted when one IDDFS depth is complete.
type PathFindingEventDepthComplete struct {
	Depth         int
	MaxDepth      int
	NodesExplored int
	PathsFound    int
}

func (PathFindingEventDepthComplete) isPathFindingEvent() {}

// PathFindingEventPathFound is emitted when a path is found.
type PathFindingEventPathFound struct {
	Path *Strand
}

func (PathFindingEventPathFound) isPathFindingEvent() {}

// PathFindingEventComplete is emitted when path finding is fully done.
type PathFindingEventComplete struct {
	TotalPaths         int
	TotalNodesExplored int
}

func (PathFindingEventComplete) isPathFindingEvent() {}

// LiveCapFab is a precomputed graph of capabilities for path finding.
type LiveCapFab struct {
	edges        []*LiveMachinePlanEdge
	outgoing     map[string][]int
	incoming     map[string][]int
	nodes        map[string]bool
	capToEdges   map[string][]int
	bookendNodes map[string]bool
}

// NewLiveCapFab creates a new empty capability graph.
func NewLiveCapFab() *LiveCapFab {
	return &LiveCapFab{
		outgoing:     make(map[string][]int),
		incoming:     make(map[string][]int),
		nodes:        make(map[string]bool),
		capToEdges:   make(map[string][]int),
		bookendNodes: make(map[string]bool),
	}
}

// Clear clears the graph completely.
func (g *LiveCapFab) Clear() {
	g.edges = nil
	g.outgoing = make(map[string][]int)
	g.incoming = make(map[string][]int)
	g.nodes = make(map[string]bool)
	g.capToEdges = make(map[string][]int)
	g.bookendNodes = make(map[string]bool)
}

// Stats returns (node_count, edge_count).
func (g *LiveCapFab) Stats() (int, int) {
	return len(g.nodes), len(g.edges)
}

// SyncFromCaps rebuilds the graph from a list of Cap definitions.
func (g *LiveCapFab) SyncFromCaps(caps []*cap.Cap) {
	g.Clear()
	for _, c := range caps {
		g.AddCap(c)
	}
}

// refreshBookends recomputes the bookend node set as the intersection of the
// graph's nodes and the supplied bookend media-URN set. Bookend URNs are keyed
// by their canonical media-URN string. Mirrors Rust's LiveCapFab::refresh_bookends.
func (g *LiveCapFab) refreshBookends(bookendUrns map[string]bool) {
	g.bookendNodes = make(map[string]bool)
	for node := range g.nodes {
		if bookendUrns[node] {
			g.bookendNodes[node] = true
		}
	}
}

// SyncFromCapUrns rebuilds the graph from a list of cap URN strings using the
// registry to resolve each URN to its Cap definition.
//
// Caps are matched by equivalence (IsEquivalent): the cartridge's reported URN
// must have an exact semantic match in the registry. Identity caps are skipped.
// Unmatched URNs are dropped from this pass — a subsequent refresh picks them up
// once the registry cache contains them.
//
// Mirrors Rust's LiveCapFab::sync_from_cap_urns. The Rust fallback to an
// in-memory registry hydration path has no synchronous Go analog; Go's
// FabricRegistry.GetCachedCaps already returns the full cached set, so the
// equivalence lookup over that set is the complete match path.
func (g *LiveCapFab) SyncFromCapUrns(capUrns []string, registry *cap.FabricRegistry, bookendUrns map[string]bool) {
	g.Clear()

	allCaps := registry.GetCachedCaps()

	identityUrn, identityErr := urn.NewCapUrnFromString(standard.CapIdentity)

	for _, capUrnStr := range capUrns {
		capUrn, err := urn.NewCapUrnFromString(capUrnStr)
		if err != nil {
			// Cartridge reported an invalid cap URN — skip it.
			continue
		}

		// Skip identity caps — they don't contribute to path finding.
		if identityErr == nil && capUrn.IsEquivalent(identityUrn) {
			continue
		}

		// Find the exact matching Cap in the registry using IsEquivalent.
		var matchingCap *cap.Cap
		for _, registryCap := range allCaps {
			if capUrn.IsEquivalent(registryCap.Urn) {
				matchingCap = registryCap
				break
			}
		}

		if matchingCap != nil {
			g.AddCap(matchingCap)
		}
		// Unmatched URNs are dropped from this pass.
	}

	g.refreshBookends(bookendUrns)
}

// AddCap adds a capability as an edge in the graph.
func (g *LiveCapFab) AddCap(c *cap.Cap) {
	inSpecStr := c.Urn.InSpec()
	outSpecStr := c.Urn.OutSpec()

	if inSpecStr == "" || outSpecStr == "" {
		return
	}

	// Skip identity caps (passthrough caps that don't transform anything)
	identityUrn, err := urn.NewCapUrnFromString(standard.CapIdentity)
	if err == nil && c.Urn.IsEquivalent(identityUrn) {
		return
	}

	fromSpec, err := urn.NewMediaUrnFromString(inSpecStr)
	if err != nil {
		return
	}

	toSpec, err := urn.NewMediaUrnFromString(outSpecStr)
	if err != nil {
		return
	}

	fromCanonical := fromSpec.String()
	toCanonical := toSpec.String()
	capCanonical := c.Urn.String()

	// Determine InputIsSequence from the stdin arg's IsSequence field.
	inputIsSequence := false
	for _, arg := range c.Args {
		isStdin := false
		for _, src := range arg.Sources {
			if src.Stdin != nil {
				isStdin = true
				break
			}
		}
		if isStdin {
			inputIsSequence = arg.IsSequence
			break
		}
	}

	// Determine OutputIsSequence from the cap's Output field.
	outputIsSequence := false
	if c.Output != nil {
		outputIsSequence = c.Output.IsSequence
	}

	edgeIdx := len(g.edges)
	edge := &LiveMachinePlanEdge{
		FromSpec:         fromSpec,
		ToSpec:           toSpec,
		Type:             EdgeTypeCap,
		CapUrnVal:        c.Urn,
		CapTitle:         c.Title,
		SpecificityVal:   c.Urn.Specificity(),
		InputIsSequence:  inputIsSequence,
		OutputIsSequence: outputIsSequence,
	}
	g.edges = append(g.edges, edge)

	g.outgoing[fromCanonical] = append(g.outgoing[fromCanonical], edgeIdx)
	g.incoming[toCanonical] = append(g.incoming[toCanonical], edgeIdx)
	g.nodes[fromCanonical] = true
	g.nodes[toCanonical] = true
	g.capToEdges[capCanonical] = append(g.capToEdges[capCanonical], edgeIdx)
}

// getOutgoingEdges returns all edges reachable from source given isSequence context.
// Returns parallel slices: edges and their outgoing is_sequence states.
//
// For Cap edges:
//   - If isSequence && !edge.InputIsSequence: skip (sequence data needs ForEach first).
//   - Otherwise: include with outIsSeq = edge.OutputIsSequence.
//
// Synthesizes a ForEach edge when isSequence=true and there is at least one Cap edge
// with !InputIsSequence whose FromSpec source conforms to.
func (g *LiveCapFab) getOutgoingEdges(source *urn.MediaUrn, isSequence bool) ([]*LiveMachinePlanEdge, []bool) {
	var edges []*LiveMachinePlanEdge
	var outSeqs []bool

	needsForEach := false

	for _, edge := range g.edges {
		if edge.Type != EdgeTypeCap {
			continue
		}
		if !source.ConformsTo(edge.FromSpec) {
			continue
		}
		if isSequence && !edge.InputIsSequence {
			// Sequence data reaching a scalar cap — ForEach must be synthesized.
			needsForEach = true
			continue
		}
		edges = append(edges, edge)
		outSeqs = append(outSeqs, edge.OutputIsSequence)
	}

	// Synthesize a ForEach edge so path finding can iterate into scalar caps.
	if isSequence && needsForEach {
		synthetic := &LiveMachinePlanEdge{
			FromSpec: source,
			ToSpec:   source,
			Type:     EdgeTypeForEach,
		}
		edges = append(edges, synthetic)
		outSeqs = append(outSeqs, false)
	}

	return edges, outSeqs
}

// GetReachableTargets performs BFS from source and returns reachable targets.
func (g *LiveCapFab) GetReachableTargets(source *urn.MediaUrn, isSequence bool, maxDepth int) []ReachableTargetInfo {
	type visitKey struct {
		canonical  string
		isSequence bool
	}
	type queueItem struct {
		urn        *urn.MediaUrn
		isSequence bool
		depth      int
	}

	visited := make(map[string]*ReachableTargetInfo)
	visitedNodes := make(map[visitKey]bool)
	queue := []queueItem{}

	// Seed
	edges, outSeqs := g.getOutgoingEdges(source, isSequence)
	for i, edge := range edges {
		queue = append(queue, queueItem{urn: edge.ToSpec, isSequence: outSeqs[i], depth: 1})
	}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if item.depth > maxDepth {
			continue
		}

		vk := visitKey{canonical: item.urn.String(), isSequence: item.isSequence}
		if visitedNodes[vk] {
			// Already visited this (urn, isSequence) pair; still update path count.
			key := item.urn.String()
			if info, ok := visited[key]; ok {
				info.PathCount++
				if item.depth < info.MinPathLength {
					info.MinPathLength = item.depth
				}
			}
			continue
		}
		visitedNodes[vk] = true

		key := item.urn.String()
		if info, ok := visited[key]; ok {
			info.PathCount++
			if item.depth < info.MinPathLength {
				info.MinPathLength = item.depth
			}
		} else {
			visited[key] = &ReachableTargetInfo{
				MediaDef:      item.urn,
				DisplayName:   key,
				MinPathLength: item.depth,
				PathCount:     1,
			}
		}

		nextEdges, nextOutSeqs := g.getOutgoingEdges(item.urn, item.isSequence)
		for i, edge := range nextEdges {
			queue = append(queue, queueItem{urn: edge.ToSpec, isSequence: nextOutSeqs[i], depth: item.depth + 1})
		}
	}

	results := make([]ReachableTargetInfo, 0, len(visited))
	for _, info := range visited {
		results = append(results, *info)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].MinPathLength != results[j].MinPathLength {
			return results[i].MinPathLength < results[j].MinPathLength
		}
		return results[i].DisplayName < results[j].DisplayName
	})
	return results
}

// visitedKey is used to track (canonical_urn, isSequence) pairs during DFS.
type visitedKey struct {
	canonical  string
	isSequence bool
}

// FindPathsToExactTarget performs iterative deepening DFS to find paths to an exact target.
func (g *LiveCapFab) FindPathsToExactTarget(
	source, target *urn.MediaUrn,
	isSequence bool,
	maxDepth, maxPaths int,
) []*Strand {
	var results []*Strand

	for depth := 1; depth <= maxDepth; depth++ {
		if len(results) >= maxPaths {
			break
		}
		visited := make(map[visitedKey]bool)
		g.iddfsFind(source, source, target, isSequence, nil, visited, depth, maxPaths, &results)
	}

	// Sort: cap_step_count ascending, total_specificity descending, description lexicographic.
	sort.Slice(results, func(i, j int) bool {
		if results[i].CapStepCount != results[j].CapStepCount {
			return results[i].CapStepCount < results[j].CapStepCount
		}
		specI := 0
		for _, s := range results[i].Steps {
			specI += s.Specificity()
		}
		specJ := 0
		for _, s := range results[j].Steps {
			specJ += s.Specificity()
		}
		if specI != specJ {
			return specI > specJ // descending
		}
		return results[i].Description < results[j].Description
	})

	return results
}

// FindPathsStreaming performs iterative deepening DFS and streams events to onEvent.
// cancelled is an *int32 that can be set to 1 via atomic store to cancel the search.
func (g *LiveCapFab) FindPathsStreaming(
	source, target *urn.MediaUrn,
	isSequence bool,
	maxDepth, maxPaths int,
	cancelled *int32,
	onEvent func(PathFindingEvent),
) []*Strand {
	var results []*Strand
	totalNodesExplored := 0

	for depth := 1; depth <= maxDepth; depth++ {
		if len(results) >= maxPaths {
			break
		}
		if cancelled != nil && atomic.LoadInt32(cancelled) != 0 {
			break
		}

		visited := make(map[visitedKey]bool)
		pathsBefore := len(results)

		g.iddfsFind(source, source, target, isSequence, nil, visited, depth, maxPaths, &results)

		nodesThisDepth := len(visited)
		totalNodesExplored += nodesThisDepth
		pathsThisDepth := len(results) - pathsBefore

		onEvent(PathFindingEventDepthComplete{
			Depth:         depth,
			MaxDepth:      maxDepth,
			NodesExplored: nodesThisDepth,
			PathsFound:    pathsThisDepth,
		})
	}

	// Sort
	sort.Slice(results, func(i, j int) bool {
		if results[i].CapStepCount != results[j].CapStepCount {
			return results[i].CapStepCount < results[j].CapStepCount
		}
		specI := 0
		for _, s := range results[i].Steps {
			specI += s.Specificity()
		}
		specJ := 0
		for _, s := range results[j].Steps {
			specJ += s.Specificity()
		}
		if specI != specJ {
			return specI > specJ
		}
		return results[i].Description < results[j].Description
	})

	onEvent(PathFindingEventComplete{
		TotalPaths:         len(results),
		TotalNodesExplored: totalNodesExplored,
	})

	for _, p := range results {
		onEvent(PathFindingEventPathFound{Path: p})
	}

	return results
}

// iddfsFind performs depth-limited DFS from current toward target.
// originalSource is the root source used to construct Strand.SourceMediaUrn.
func (g *LiveCapFab) iddfsFind(
	originalSource *urn.MediaUrn,
	current *urn.MediaUrn,
	target *urn.MediaUrn,
	isSequence bool,
	path []*LiveMachinePlanEdge,
	visited map[visitedKey]bool,
	depthLimit int,
	maxPaths int,
	results *[]*Strand,
) {
	if len(*results) >= maxPaths {
		return
	}

	// Check if we've reached the EXACT target.
	// Skip this check at the starting node (empty path) — when source==target,
	// we still want to explore edges to find round-trip transformation paths.
	if current.IsEquivalent(target) && len(path) > 0 {
		// Only record the path when it exactly fills the depth budget for this IDDFS
		// iteration (depthLimit == 0). This mirrors Rust's `current_path.len() == depth_limit`
		// check and ensures each path is found exactly once — at the shallowest depth that
		// reaches it — preventing duplicates across outer loop iterations.
		if depthLimit == 0 {
			var steps []*StrandStep
			capCount := 0
			for _, edge := range path {
				step := edgeToStep(edge)
				steps = append(steps, step)
				if step.IsCap() {
					capCount++
				}
			}
			if capCount > 0 {
				var titles []string
				for _, s := range steps {
					if s.IsCap() {
						titles = append(titles, s.Title())
					}
				}
				*results = append(*results, &Strand{
					Steps:          steps,
					SourceMediaUrn: originalSource,
					TargetMediaUrn: target,
					TotalSteps:     len(steps),
					CapStepCount:   capCount,
					Description:    strings.Join(titles, " → "),
				})
			}
		}
		// For round-trip paths (source==target), don't return early —
		// continue exploring edges to find longer paths through this node.
		if !originalSource.IsEquivalent(target) {
			return
		}
	}

	if depthLimit == 0 {
		return
	}

	// For round-trip paths (source==target), don't mark target-equivalent nodes
	// as visited. This allows the DFS to return to the target through different
	// intermediate paths. Cycle prevention is handled by depth_limit.
	isRoundtrip := originalSource.IsEquivalent(target)
	vk := visitedKey{canonical: current.String(), isSequence: isSequence}
	if !(isRoundtrip && current.IsEquivalent(target)) {
		if visited[vk] {
			return
		}
		visited[vk] = true
		defer func() { delete(visited, vk) }()
	}

	edges, outSeqs := g.getOutgoingEdges(current, isSequence)
	for i, edge := range edges {
		// Make a fresh slice to avoid aliasing between recursive branches.
		newPath := make([]*LiveMachinePlanEdge, len(path)+1)
		copy(newPath, path)
		newPath[len(path)] = edge
		g.iddfsFind(originalSource, edge.ToSpec, target, outSeqs[i], newPath, visited, depthLimit-1, maxPaths, results)
		if len(*results) >= maxPaths {
			return
		}
	}
}

func edgeToStep(edge *LiveMachinePlanEdge) *StrandStep {
	switch edge.Type {
	case EdgeTypeCap:
		return &StrandStep{
			StepType:         StepTypeCap,
			FromSpec:         edge.FromSpec,
			ToSpec:           edge.ToSpec,
			CapUrnVal:        edge.CapUrnVal,
			StepTitle:        edge.CapTitle,
			SpecificityVal:   edge.SpecificityVal,
			InputIsSequence:  edge.InputIsSequence,
			OutputIsSequence: edge.OutputIsSequence,
		}
	case EdgeTypeForEach:
		return &StrandStep{
			StepType: StepTypeForEach,
			FromSpec: edge.FromSpec,
			ToSpec:   edge.ToSpec,
			MediaDef: edge.FromSpec,
		}
	case EdgeTypeCollect:
		return &StrandStep{
			StepType: StepTypeCollect,
			FromSpec: edge.FromSpec,
			ToSpec:   edge.ToSpec,
			MediaDef: edge.FromSpec,
		}
	}
	return nil
}
