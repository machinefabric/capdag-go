package planner

import (
	"sort"
	"strings"

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
	EdgeTypeWrapInList
)

// LiveMachinePlanEdge represents an edge in the live capability graph.
type LiveMachinePlanEdge struct {
	FromSpec          *urn.MediaUrn
	ToSpec            *urn.MediaUrn
	Type              LiveMachinePlanEdgeType
	CapUrnVal         *urn.CapUrn
	CapTitle          string
	SpecificityVal    int
	InputCardinality  InputCardinality
	OutputCardinality InputCardinality
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
	case EdgeTypeWrapInList:
		return "WrapInList (create single-item list)"
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
	StepTypeWrapInList
)

// StrandStep contains information about a single step in a path.
type StrandStep struct {
	StepType       StrandStepType
	FromSpec       *urn.MediaUrn
	ToSpec         *urn.MediaUrn
	CapUrnVal      *urn.CapUrn
	StepTitle      string
	SpecificityVal int
	ListSpec       *urn.MediaUrn
	ItemSpec       *urn.MediaUrn
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
	case StepTypeWrapInList:
		return "WrapInList"
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
	g.insertCardinalityTransitions()
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

	inputCard := CardinalityFromMediaUrn(fromCanonical)
	outputCard := CardinalityFromMediaUrn(toCanonical)

	edgeIdx := len(g.edges)
	edge := &LiveMachinePlanEdge{
		FromSpec:          fromSpec,
		ToSpec:            toSpec,
		Type:              EdgeTypeCap,
		CapUrnVal:         c.Urn,
		CapTitle:          c.Title,
		SpecificityVal:    c.Urn.Specificity(),
		InputCardinality:  inputCard,
		OutputCardinality: outputCard,
	}
	g.edges = append(g.edges, edge)

	g.outgoing[fromCanonical] = append(g.outgoing[fromCanonical], edgeIdx)
	g.incoming[toCanonical] = append(g.incoming[toCanonical], edgeIdx)
	g.nodes[fromCanonical] = true
	g.nodes[toCanonical] = true
	g.capToEdges[capCanonical] = append(g.capToEdges[capCanonical], edgeIdx)
}

func (g *LiveCapGraph) insertCardinalityTransitions() {
	// Collect existing list-type nodes
	var listNodes []string
	for n := range g.nodes {
		if strings.Contains(n, "list") {
			listNodes = append(listNodes, n)
		}
	}

	for _, listCanonical := range listNodes {
		listUrn, err := urn.NewMediaUrnFromString(listCanonical)
		if err != nil {
			continue
		}
		itemUrn := listUrn.WithoutList()
		itemCanonical := itemUrn.String()

		// ForEach: list → item
		foreachIdx := len(g.edges)
		g.edges = append(g.edges, &LiveMachinePlanEdge{
			FromSpec:          listUrn,
			ToSpec:            itemUrn,
			Type:              EdgeTypeForEach,
			InputCardinality:  CardinalitySequence,
			OutputCardinality: CardinalitySingle,
		})
		g.outgoing[listCanonical] = append(g.outgoing[listCanonical], foreachIdx)
		g.incoming[itemCanonical] = append(g.incoming[itemCanonical], foreachIdx)
		g.nodes[itemCanonical] = true

		// Collect: item → list
		collectIdx := len(g.edges)
		g.edges = append(g.edges, &LiveMachinePlanEdge{
			FromSpec:          itemUrn,
			ToSpec:            listUrn,
			Type:              EdgeTypeCollect,
			InputCardinality:  CardinalitySingle,
			OutputCardinality: CardinalitySequence,
		})
		g.outgoing[itemCanonical] = append(g.outgoing[itemCanonical], collectIdx)
		g.incoming[listCanonical] = append(g.incoming[listCanonical], collectIdx)
	}

	// WrapInList edges
	var nonListNodes []string
	for n := range g.nodes {
		if !strings.Contains(n, "list") {
			nonListNodes = append(nonListNodes, n)
		}
	}
	for _, itemCanonical := range nonListNodes {
		itemUrn, err := urn.NewMediaUrnFromString(itemCanonical)
		if err != nil {
			continue
		}
		listUrn := itemUrn.WithList()
		listCanonical := listUrn.String()

		if g.nodes[listCanonical] {
			wrapIdx := len(g.edges)
			g.edges = append(g.edges, &LiveMachinePlanEdge{
				FromSpec:          itemUrn,
				ToSpec:            listUrn,
				Type:              EdgeTypeWrapInList,
				InputCardinality:  CardinalitySingle,
				OutputCardinality: CardinalitySequence,
			})
			g.outgoing[itemCanonical] = append(g.outgoing[itemCanonical], wrapIdx)
			g.incoming[listCanonical] = append(g.incoming[listCanonical], wrapIdx)
		}
	}
}

// GetReachableTargets performs BFS from source and returns reachable targets.
func (g *LiveCapGraph) GetReachableTargets(source *urn.MediaUrn, maxDepth int) []ReachableTargetInfo {
	type queueItem struct {
		urn   *urn.MediaUrn
		depth int
	}

	visited := make(map[string]*ReachableTargetInfo)
	queue := []queueItem{}

	// Seed
	for _, edge := range g.getOutgoingEdges(source) {
		queue = append(queue, queueItem{urn: edge.ToSpec, depth: 1})
	}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if item.depth > maxDepth {
			continue
		}

		key := item.urn.String()
		if info, ok := visited[key]; ok {
			info.PathCount++
			if item.depth < info.MinPathLength {
				info.MinPathLength = item.depth
			}
			continue
		}

		visited[key] = &ReachableTargetInfo{
			MediaSpec:     item.urn,
			DisplayName:   key,
			MinPathLength: item.depth,
			PathCount:     1,
		}

		for _, edge := range g.getOutgoingEdges(item.urn) {
			queue = append(queue, queueItem{urn: edge.ToSpec, depth: item.depth + 1})
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

// FindPathsToExactTarget performs DFS to find paths to an exact target (is_equivalent).
func (g *LiveCapGraph) FindPathsToExactTarget(
	source, target *urn.MediaUrn,
	maxDepth, maxPaths int,
) []*Strand {
	var results []*Strand
	visitedEdges := make(map[int]bool)

	var dfs func(current *urn.MediaUrn, path []*LiveMachinePlanEdge, depth int)
	dfs = func(current *urn.MediaUrn, path []*LiveMachinePlanEdge, depth int) {
		if len(results) >= maxPaths || depth > maxDepth {
			return
		}

		if current.IsEquivalent(target) {
			// Build path info
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
				results = append(results, &Strand{
					Steps:        steps,
					SourceSpec:   source,
					TargetSpec:   target,
					TotalSteps:   len(steps),
					CapStepCount: capCount,
					Description:  strings.Join(titles, " → "),
				})
			}
			return
		}

		for i, edge := range g.edges {
			if visitedEdges[i] {
				continue
			}

			sourceIsList := current.IsList()
			edgeExpectsList := edge.FromSpec.IsList()

			switch edge.Type {
			case EdgeTypeCap:
				if edgeExpectsList != sourceIsList {
					continue
				}
			case EdgeTypeForEach:
				if !(sourceIsList && !edge.ToSpec.IsList()) {
					continue
				}
			case EdgeTypeCollect, EdgeTypeWrapInList:
				if !(!sourceIsList && edge.ToSpec.IsList()) {
					continue
				}
			}

			if !current.ConformsTo(edge.FromSpec) {
				continue
			}

			visitedEdges[i] = true
			dfs(edge.ToSpec, append(path, edge), depth+1)
			delete(visitedEdges, i)
		}
	}

	dfs(source, nil, 0)

	// Sort: cap_step_count ascending, total_specificity descending, cap URNs lexicographic
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

func (g *LiveCapGraph) getOutgoingEdges(source *urn.MediaUrn) []*LiveMachinePlanEdge {
	var results []*LiveMachinePlanEdge
	sourceIsList := source.IsList()

	for _, edge := range g.edges {
		edgeExpectsList := edge.FromSpec.IsList()

		switch edge.Type {
		case EdgeTypeCap:
			if edgeExpectsList != sourceIsList {
				continue
			}
		case EdgeTypeForEach:
			if !(sourceIsList && !edge.ToSpec.IsList()) {
				continue
			}
		case EdgeTypeCollect, EdgeTypeWrapInList:
			if !(!sourceIsList && edge.ToSpec.IsList()) {
				continue
			}
		}

		if source.ConformsTo(edge.FromSpec) {
			results = append(results, edge)
		}
	}
	return results
}

func edgeToStep(edge *LiveMachinePlanEdge) *StrandStep {
	switch edge.Type {
	case EdgeTypeCap:
		return &StrandStep{
			StepType:       StepTypeCap,
			FromSpec:       edge.FromSpec,
			ToSpec:         edge.ToSpec,
			CapUrnVal:      edge.CapUrnVal,
			StepTitle:      edge.CapTitle,
			SpecificityVal: edge.SpecificityVal,
		}
	case EdgeTypeForEach:
		return &StrandStep{
			StepType: StepTypeForEach,
			FromSpec: edge.FromSpec,
			ToSpec:   edge.ToSpec,
			ListSpec: edge.FromSpec,
			ItemSpec: edge.ToSpec,
		}
	case EdgeTypeCollect:
		return &StrandStep{
			StepType: StepTypeCollect,
			FromSpec: edge.FromSpec,
			ToSpec:   edge.ToSpec,
			ItemSpec: edge.FromSpec,
			ListSpec: edge.ToSpec,
		}
	case EdgeTypeWrapInList:
		return &StrandStep{
			StepType: StepTypeWrapInList,
			FromSpec: edge.FromSpec,
			ToSpec:   edge.ToSpec,
			ItemSpec: edge.FromSpec,
			ListSpec: edge.ToSpec,
		}
	}
	return nil
}
