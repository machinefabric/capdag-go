package planner

import (
	"sort"
	"strings"
	"sync/atomic"

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
	FromSpec        *urn.MediaUrn
	ToSpec          *urn.MediaUrn
	Type            LiveMachinePlanEdgeType
	CapUrnVal       *urn.CapUrn
	CapTitle        string
	SpecificityVal  int
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

// StrandStep contains information about a single step in a path.
type StrandStep struct {
	StepType         StrandStepType
	FromSpec         *urn.MediaUrn
	ToSpec           *urn.MediaUrn
	CapUrnVal        *urn.CapUrn
	StepTitle        string
	SpecificityVal   int
	MediaSpec        *urn.MediaUrn
	InputIsSequence  bool
	OutputIsSequence bool
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
	Steps        []*StrandStep
	SourceSpec   *urn.MediaUrn
	TargetSpec   *urn.MediaUrn
	TotalSteps   int
	CapStepCount int
	Description  string
}

// ReachableTargetInfo contains information about a reachable target.
type ReachableTargetInfo struct {
	MediaSpec     *urn.MediaUrn
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

// LiveCapGraph is a precomputed graph of capabilities for path finding.
type LiveCapGraph struct {
	edges      []*LiveMachinePlanEdge
	outgoing   map[string][]int
	incoming   map[string][]int
	nodes      map[string]bool
	capToEdges map[string][]int
}

// NewLiveCapGraph creates a new empty capability graph.
func NewLiveCapGraph() *LiveCapGraph {
	return &LiveCapGraph{
		outgoing:   make(map[string][]int),
		incoming:   make(map[string][]int),
		nodes:      make(map[string]bool),
		capToEdges: make(map[string][]int),
	}
}

// Clear clears the graph completely.
func (g *LiveCapGraph) Clear() {
	g.edges = nil
	g.outgoing = make(map[string][]int)
	g.incoming = make(map[string][]int)
	g.nodes = make(map[string]bool)
	g.capToEdges = make(map[string][]int)
}

// Stats returns (node_count, edge_count).
func (g *LiveCapGraph) Stats() (int, int) {
	return len(g.nodes), len(g.edges)
}

// SyncFromCaps rebuilds the graph from a list of Cap definitions.
func (g *LiveCapGraph) SyncFromCaps(caps []*cap.Cap) {
	g.Clear()
	for _, c := range caps {
		g.AddCap(c)
	}
}

// AddCap adds a capability as an edge in the graph.
func (g *LiveCapGraph) AddCap(c *cap.Cap) {
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
func (g *LiveCapGraph) getOutgoingEdges(source *urn.MediaUrn, isSequence bool) ([]*LiveMachinePlanEdge, []bool) {
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
func (g *LiveCapGraph) GetReachableTargets(source *urn.MediaUrn, isSequence bool, maxDepth int) []ReachableTargetInfo {
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
				MediaSpec:     item.urn,
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
func (g *LiveCapGraph) FindPathsToExactTarget(
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
func (g *LiveCapGraph) FindPathsStreaming(
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
// originalSource is the root source used to construct Strand.SourceSpec.
func (g *LiveCapGraph) iddfsFind(
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

	vk := visitedKey{canonical: current.String(), isSequence: isSequence}
	if visited[vk] {
		return
	}
	visited[vk] = true
	defer func() { delete(visited, vk) }()

	if current.IsEquivalent(target) {
		// Build steps from edge path
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
				Steps:        steps,
				SourceSpec:   originalSource,
				TargetSpec:   target,
				TotalSteps:   len(steps),
				CapStepCount: capCount,
				Description:  strings.Join(titles, " → "),
			})
		}
		return
	}

	if depthLimit == 0 {
		return
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
			StepType:  StepTypeForEach,
			FromSpec:  edge.FromSpec,
			ToSpec:    edge.ToSpec,
			MediaSpec: edge.FromSpec,
		}
	case EdgeTypeCollect:
		return &StrandStep{
			StepType:  StepTypeCollect,
			FromSpec:  edge.FromSpec,
			ToSpec:    edge.ToSpec,
			MediaSpec: edge.FromSpec,
		}
	}
	return nil
}
