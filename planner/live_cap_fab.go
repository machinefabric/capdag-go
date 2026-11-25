package planner

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/fabric"
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
	InputIsSequence bool
	// OutputIsSequence says whether the output this edge reaches produces a
	// sequence.
	OutputIsSequence bool
	// OutputUrn is the output this edge reaches, when it is not the cap's main
	// one: that output's media URN, its identity in the definition. Nil: the
	// main output.
	OutputUrn *urn.MediaUrn
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
// StepToken is the stable identity of one step of a resolved strand — the ONLY
// address by which a step, or an argument value destined for it, is ever named.
//
// The raw text is unexported so that an unidentified step is not a state the
// program can be in: there is exactly one way to make a token (MintStepToken)
// and exactly one way to recover one that was already minted (ParseStepToken,
// which refuses an empty id). UnmarshalJSON goes through ParseStepToken, so a
// persisted strand carrying "" fails to load rather than loading into a strand
// whose steps cannot be addressed.
//
// Nothing derives a token. Not from a position — a strand is a DAG, and parallel
// branches merging downstream have no ordinal, so two identical caps on separate
// branches differ only by token. Not from notation — a plan holds strictly more
// than the notation it was planned from, and reducing one back to the other
// discards exactly the identities this type exists to carry. A token comes from
// the plan that minted it or it does not exist.
//
// The struct is comparable, so a token works directly as a map key.
// Matches Rust: pub struct StepToken(String)
type StepToken struct {
	raw string
}

// ErrEmptyStepToken is the only way a StepToken can fail to exist.
var ErrEmptyStepToken = errors.New(
	"a strand step token_id is empty; a step without a token came from no plan and cannot be addressed",
)

// MintStepToken mints a fresh identity. This is how every step in production is
// born.
func MintStepToken() StepToken {
	return StepToken{raw: uuid.New().String()}
}

// ParseStepToken recovers an already-minted token — from decoding, from a
// protocol message, from a persisted run. An empty id is not a token: it names
// no step, so a value bound to it could never be delivered.
func ParseStepToken(raw string) (StepToken, error) {
	if raw == "" {
		return StepToken{}, ErrEmptyStepToken
	}
	return StepToken{raw: raw}, nil
}

// String returns the token's text, for protocol encoding and diagnostics.
func (t StepToken) String() string { return t.raw }

// IsZero reports whether this is the zero StepToken — the value a struct field
// holds before a token has been put in it. It is never a token.
func (t StepToken) IsZero() bool { return t.raw == "" }

// MarshalJSON encodes the token as its plain text.
func (t StepToken) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.raw)
}

// UnmarshalJSON decodes through ParseStepToken, so "" is refused here exactly as
// it is everywhere else a token crosses into the process.
func (t *StepToken) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := ParseStepToken(raw)
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}

// (an input anchor) or by another cap step's output. There are no positional
// assumptions — every input names its producer explicitly, so fan-out (one
// producer feeding several caps' main inputs) and convergence (one cap fed by
// several producers) are both expressible.
// Matches Rust: pub enum ArgSourceRef { StrandInput, Step { token_id: String } }
type ArgSourceRef struct {
	Kind ArgSourceKind
	// TokenId is the producing step's stable identity. Only meaningful when
	// Kind == ArgSourceStep.
	TokenId StepToken
}

// NewArgSourceStrandInput builds an ArgSourceRef fed by the strand's own input.
func NewArgSourceStrandInput() ArgSourceRef {
	return ArgSourceRef{Kind: ArgSourceStrandInput}
}

// NewArgSourceStep builds an ArgSourceRef fed by another cap step's output.
func NewArgSourceStep(tokenId StepToken) ArgSourceRef {
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
	TokenId         StepToken
	StepType        StrandStepType
	FromSpec        *urn.MediaUrn
	ToSpec          *urn.MediaUrn
	CapUrnVal       *urn.CapUrn
	StepTitle       string
	SpecificityVal  int
	MediaDef        *urn.MediaUrn
	InputIsSequence bool
	// OutputIsSequence says whether the output this step takes produces a
	// sequence.
	OutputIsSequence bool
	// OutputUrn is the output this step takes, when it is not the cap's main
	// one: that output's media URN, its identity in the definition (never its
	// name, which is a label). Nil: the main output. Only meaningful when
	// StepType == StepTypeCap.
	OutputUrn *urn.MediaUrn
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
		TokenId:  MintStepToken(),
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
	// TargetMediaUrn is what the path gives: the media its last node holds.
	// A path found for a target gives something inside it, not always the
	// target itself.
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
//
// Its indexes are keyed by a media or cap URN's canonical string, which is the
// URN's structural (tag-set) identity.
type LiveCapFab struct {
	edges      []*LiveMachinePlanEdge
	outgoing   map[string][]int
	incoming   map[string][]int
	nodes      map[string]*urn.MediaUrn
	capToEdges map[string][]int
	// bookendNodes holds the nodes that end on bookends (sources or targets of
	// a transmute), each with the bookends it ends on: one for each shape the
	// node may have, the published file type that shape is read complete
	// (MediaUrn.EndsOn). A node of one shape that is a file type ends on that
	// file type; a node any of whose shapes is no file type ends on none and
	// is not listed.
	//
	// Computed at sync time from the registry snapshot supplied to SyncFromCaps
	// / SyncFromCapUrns / SetBookends. The graph is rebuilt on every cap-set
	// change, so the bookend set tracks the live registry state exactly.
	bookendNodes map[string][]*urn.MediaUrn
}

// NewLiveCapFab creates a new empty capability graph.
func NewLiveCapFab() *LiveCapFab {
	return &LiveCapFab{
		outgoing:     make(map[string][]int),
		incoming:     make(map[string][]int),
		nodes:        make(map[string]*urn.MediaUrn),
		capToEdges:   make(map[string][]int),
		bookendNodes: make(map[string][]*urn.MediaUrn),
	}
}

// Clear clears the graph completely.
func (g *LiveCapFab) Clear() {
	g.edges = nil
	g.outgoing = make(map[string][]int)
	g.incoming = make(map[string][]int)
	g.nodes = make(map[string]*urn.MediaUrn)
	g.capToEdges = make(map[string][]int)
	g.bookendNodes = make(map[string][]*urn.MediaUrn)
}

// IsBookend reports whether the given node ends on bookends: every shape it
// may have is a file type, so whatever a route reaching it gives can sit on
// disk as a file.
//
// Strand bookends (transmute source or target) MUST end on bookends. Internal
// URNs that appear as cap inputs/outputs but do not name a concrete file
// format (wildcards like `media:enc=utf8`, primitives like
// `media:integer;numeric`) are tracked as nodes for path finding but end on no
// bookend and never appear as transmute sources/targets.
func (g *LiveCapFab) IsBookend(node *urn.MediaUrn) bool {
	_, ok := g.bookendNodes[node.String()]
	return ok
}

// EndsOn is the bookends node ends on, one for each shape it may have; empty
// when it ends on none.
func (g *LiveCapFab) EndsOn(node *urn.MediaUrn) []*urn.MediaUrn {
	return g.bookendNodes[node.String()]
}

// reaches reports whether a route reaching node reaches target: what the node
// holds, read complete, is inside the target, read open. A node that may also
// be something outside the target (…;?list;?record for a JSON record target)
// does not reach it: a possibility is no guarantee.
func reaches(node, target *urn.MediaUrn) bool {
	return node.Satisfies(target)
}

// Stats returns (node_count, edge_count).
func (g *LiveCapFab) Stats() (int, int) {
	return len(g.nodes), len(g.edges)
}

// SyncFromCaps rebuilds the graph from a list of Cap definitions.
//
// bookendUrns is the live snapshot of media-registry URNs whose stored
// definition is a bookend (FabricRegistry.BookendUrns). Each node in the graph
// is marked with the bookends among them it ends on, once per sync; lookups
// during traversal are map hits with no registry calls.
func (g *LiveCapFab) SyncFromCaps(caps []*cap.Cap, bookendUrns []*urn.MediaUrn) {
	g.Clear()
	for _, c := range caps {
		g.AddCap(c)
	}
	g.refreshBookends(bookendUrns)
}

// SetBookends recomputes which nodes end on bookends, and on which, from the
// supplied registry snapshot, so a graph built with AddCap can have its
// bookends set without a full sync.
func (g *LiveCapFab) SetBookends(bookendUrns []*urn.MediaUrn) {
	g.refreshBookends(bookendUrns)
}

// refreshBookends recomputes which nodes end on bookends, and on which, from
// the supplied registry snapshot. Mirrors Rust's LiveCapFab::refresh_bookends.
func (g *LiveCapFab) refreshBookends(bookendUrns []*urn.MediaUrn) {
	g.bookendNodes = nodesEndingOn(g.nodes, sortedDistinctMediaUrns(bookendUrns))
}

// nodesEndingOn is each of nodes that ends on bookends, with the bookends it
// ends on, each named once, keyed as nodes is.
func nodesEndingOn(nodes map[string]*urn.MediaUrn, bookends []*urn.MediaUrn) map[string][]*urn.MediaUrn {
	ending := make(map[string][]*urn.MediaUrn)
	for key, node := range nodes {
		ends, ok := node.EndsOn(bookends)
		if !ok {
			continue
		}
		ending[key] = sortedDistinctMediaUrns(ends)
	}
	return ending
}

// sortedDistinctMediaUrns is urns in structural order (MediaUrn.Compare), each
// named once.
func sortedDistinctMediaUrns(urns []*urn.MediaUrn) []*urn.MediaUrn {
	sorted := make([]*urn.MediaUrn, len(urns))
	copy(sorted, urns)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Compare(sorted[j]) < 0 })
	distinct := sorted[:0]
	for _, u := range sorted {
		if len(distinct) > 0 && distinct[len(distinct)-1].Compare(u) == 0 {
			continue
		}
		distinct = append(distinct, u)
	}
	return distinct
}

// SyncFromCapUrns rebuilds the graph from a list of cap URN strings using the
// registry to resolve each URN to its Cap definition.
//
// Caps are matched by equivalence (IsEquivalent): the cartridge's reported URN
// must have an exact semantic match in the registry. Identity caps are skipped.
// Unmatched URNs are dropped from this pass — a subsequent refresh picks them up
// once the registry cache contains them. bookendUrns is as SyncFromCaps takes
// it.
//
// Mirrors Rust's LiveCapFab::sync_from_cap_urns. The Rust fallback to an
// in-memory registry hydration path has no synchronous Go analog; Go's
// FabricRegistry.GetCachedCaps already returns the full cached set, so the
// equivalence lookup over that set is the complete match path.
func (g *LiveCapFab) SyncFromCapUrns(capUrns []string, registry *fabric.FabricRegistry, bookendUrns []*urn.MediaUrn) {
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

// NewLiveCapFabFromFabric is the graph of the whole fabric: every cap the
// registry's pinned manifest names (FabricRegistry.ManifestCaps), each node
// marked with the bookends it ends on: for each shape it may have, the media
// definition the manifest publishes for that shape, read complete, when that
// definition is a bookend (media.StoredMediaDef.IsBookend).
//
// Nothing comes from what the cache happens to hold: the caps not cached are
// fetched, and so is every definition a node's shape is. A cap or a definition
// that cannot be had is an error naming it, never a graph with a part of the
// fabric missing.
// Matches Rust: LiveCapFab::from_fabric
func NewLiveCapFabFromFabric(registry *fabric.FabricRegistry) (*LiveCapFab, error) {
	caps, err := registry.ManifestCaps()
	if err != nil {
		return nil, err
	}
	fab := NewLiveCapFab()
	for _, c := range caps {
		fab.AddCap(c)
	}
	manifestMedia, err := registry.ManifestMediaUrns()
	if err != nil {
		return nil, err
	}
	// `media:` takes anything and names no definition: nothing is a file of it.
	published := make([]*urn.MediaUrn, 0, len(manifestMedia))
	for _, mediaUrn := range manifestMedia {
		if !mediaUrn.IsTop() {
			published = append(published, mediaUrn)
		}
	}
	isBookend := make(map[string]bool)
	for key, shapes := range nodesEndingOn(fab.nodes, sortedDistinctMediaUrns(published)) {
		endsOnBookends := true
		for _, shape := range shapes {
			shapeKey := shape.String()
			bookend, known := isBookend[shapeKey]
			if !known {
				def, err := registry.GetMediaDef(shapeKey)
				if err != nil {
					return nil, err
				}
				bookend = def.IsBookend()
				isBookend[shapeKey] = bookend
			}
			endsOnBookends = endsOnBookends && bookend
		}
		if endsOnBookends {
			fab.bookendNodes[key] = shapes
		}
	}
	return fab, nil
}

// AddCap adds a capability as an edge in the graph.
func (g *LiveCapFab) AddCap(c *cap.Cap) {
	// Abstract caps are dispatch umbrellas — never backed by a cartridge and
	// never a runnable edge. Adding one would put an unbacked edge in the
	// graph (the planner could offer a path that fails at execution). They are
	// narrowed to a concrete specialization at the CLI layer instead.
	if c.IsAbstract {
		return
	}

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

	// A sink (`out=media:void`) produces nothing, so a path cannot go on
	// through it: it has no main output, and declares no other. Every type
	// conforms to `media:`, so an edge to `media:void` would let a path
	// continue from a value that never exists into any cap that takes anything.
	if toSpec.IsVoid() {
		return
	}

	inputIsSequence, outputIsSequence := c.SequenceShape()
	g.pushCapEdge(c, fromSpec, toSpec, inputIsSequence, outputIsSequence, nil)

	// Every other output the cap declares is reachable too: an edge from the
	// cap's input to that output's own type. An output that carries the input
	// unchanged (a router's) has no type of its own to reach.
	main := c.MainOutput()
	for _, output := range c.GetOutputs() {
		if output.Carries != nil {
			continue
		}
		if main != nil && main.MediaUrn == output.MediaUrn {
			continue
		}
		outputUrn, err := urn.NewMediaUrnFromString(output.MediaUrn)
		if err != nil {
			panic(fmt.Sprintf("cap registry invariant: output media_urn '%s' of '%s' is a valid MediaUrn: %v",
				output.MediaUrn, c.Urn, err))
		}
		g.pushCapEdge(c, fromSpec, outputUrn, inputIsSequence, output.IsSequence, outputUrn)
	}
}

// pushCapEdge stores one edge of `c`, from its input to one of its outputs.
func (g *LiveCapFab) pushCapEdge(c *cap.Cap, fromSpec, toSpec *urn.MediaUrn, inputIsSequence, outputIsSequence bool, outputUrn *urn.MediaUrn) {
	fromCanonical := fromSpec.String()
	toCanonical := toSpec.String()
	capCanonical := c.Urn.String()

	edgeIdx := len(g.edges)
	g.edges = append(g.edges, &LiveMachinePlanEdge{
		FromSpec:         fromSpec,
		ToSpec:           toSpec,
		Type:             EdgeTypeCap,
		CapUrnVal:        c.Urn,
		CapTitle:         c.Title,
		SpecificityVal:   c.Urn.Specificity(),
		InputIsSequence:  inputIsSequence,
		OutputIsSequence: outputIsSequence,
		OutputUrn:        outputUrn,
	})

	g.outgoing[fromCanonical] = append(g.outgoing[fromCanonical], edgeIdx)
	g.incoming[toCanonical] = append(g.incoming[toCanonical], edgeIdx)
	g.nodes[fromCanonical] = fromSpec
	g.nodes[toCanonical] = toSpec
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
// with !InputIsSequence whose FromSpec source satisfies.
//
// A source is what a step gave or a file's detected type, a complete thing:
// what it does not mention it does not have, so it takes an edge whose input it
// satisfies.
func (g *LiveCapFab) getOutgoingEdges(source *urn.MediaUrn, isSequence bool) ([]*LiveMachinePlanEdge, []bool) {
	var edges []*LiveMachinePlanEdge
	var outSeqs []bool

	needsForEach := false

	for _, edge := range g.edges {
		if edge.Type != EdgeTypeCap {
			continue
		}
		if !source.Satisfies(edge.FromSpec) {
			continue
		}
		if isSequence && !edge.InputIsSequence {
			// Sequence data reaching a scalar cap — ForEach must be synthesized.
			needsForEach = true
			continue
		}
		// The main output's runtime type follows the cap URN's effect: what the
		// cap gives for what it is sent (an effect=patch cap keeps what it does
		// not change). Any other output gives its own type.
		taken := edge
		if edge.OutputUrn == nil {
			given, err := edge.CapUrnVal.InferRuntimeOutputMedia(source)
			if err != nil {
				continue
			}
			runtime := *edge
			runtime.FromSpec = source
			runtime.ToSpec = given
			taken = &runtime
		}
		edges = append(edges, taken)
		outSeqs = append(outSeqs, taken.OutputIsSequence)
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

// GetReachableTargets performs BFS from source and returns the reachable
// targets: the bookends the reached nodes end on, one target for each, sorted
// by (MinPathLength, DisplayName).
func (g *LiveCapFab) GetReachableTargets(source *urn.MediaUrn, isSequence bool, maxDepth int) []ReachableTargetInfo {
	type visitKey struct {
		canonical      string
		isSequence     bool
		pendingForEach bool
	}
	type queueItem struct {
		urn            *urn.MediaUrn
		isSequence     bool
		pendingForEach bool
		depth          int
	}

	visited := make(map[string]*ReachableTargetInfo)
	visitedNodes := make(map[visitKey]bool)
	queue := []queueItem{}

	// Seed
	edges, outSeqs := g.getOutgoingEdges(source, isSequence)
	for i, edge := range edges {
		queue = append(queue, queueItem{
			urn:            edge.ToSpec,
			isSequence:     outSeqs[i],
			pendingForEach: edge.Type == EdgeTypeForEach,
			depth:          1,
		})
	}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if item.depth > maxDepth {
			continue
		}

		vk := visitKey{
			canonical:      item.urn.String(),
			isSequence:     item.isSequence,
			pendingForEach: item.pendingForEach,
		}
		if visitedNodes[vk] {
			if !item.pendingForEach {
				// Complete cap paths still contribute to the target's path count.
				key := item.urn.String()
				if info, ok := visited[key]; ok {
					info.PathCount++
					if item.depth < info.MinPathLength {
						info.MinPathLength = item.depth
					}
				}
			}
			continue
		}
		visitedNodes[vk] = true

		if !item.pendingForEach {
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
		}

		nextEdges, nextOutSeqs := g.getOutgoingEdges(item.urn, item.isSequence)
		for i, edge := range nextEdges {
			if item.pendingForEach && !canFollowForEach(edge) {
				continue
			}
			queue = append(queue, queueItem{
				urn:            edge.ToSpec,
				isSequence:     nextOutSeqs[i],
				pendingForEach: edge.Type == EdgeTypeForEach,
				depth:          item.depth + 1,
			})
		}
	}

	// The targets are the bookends the reached nodes end on. Reachability in
	// the cap graph includes URNs that exist only as cap-input wildcards (e.g.
	// `media:enc=utf8`, `media:integer;numeric`). Such URNs describe the value
	// type a cap accepts, not a file format that can sit on disk, so they are
	// never valid transmute targets. A node that may have several shapes
	// reaches each bookend it ends on, one target each. The bookends are
	// precomputed at sync time from the live media registry — no registry call
	// here.
	byBookend := make(map[string]*ReachableTargetInfo)
	for _, reached := range visited {
		for _, bookend := range g.EndsOn(reached.MediaDef) {
			key := bookend.String()
			if target, ok := byBookend[key]; ok {
				if reached.MinPathLength < target.MinPathLength {
					target.MinPathLength = reached.MinPathLength
				}
				target.PathCount += reached.PathCount
				continue
			}
			byBookend[key] = &ReachableTargetInfo{
				MediaDef:      bookend,
				DisplayName:   key,
				MinPathLength: reached.MinPathLength,
				PathCount:     reached.PathCount,
			}
		}
	}

	results := make([]ReachableTargetInfo, 0, len(byBookend))
	for _, info := range byBookend {
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

// searchState is a state of the path search: the media reached, whether it is
// a sequence, and whether the step that reached it was a ForEach (whose scalar
// cap must come next).
type searchState struct {
	canonical      string
	isSequence     bool
	pendingForEach bool
}

// distancesToExactTarget says how few steps each search state reachable from
// the source is from the target, for the states the target is reachable from
// within maxDepth of the source; nil when no step reaches the target at all.
// The states are found forward from the source over the edges getOutgoingEdges
// gives, and the distances backward over those same steps from the states that
// are the target. A state missing from the map cannot reach the target; one at
// distance d cannot reach it in fewer than d steps.
func (g *LiveCapFab) distancesToExactTarget(source, target *urn.MediaUrn, isSequence bool, maxDepth int) map[searchState]int {
	type queued struct {
		urn            *urn.MediaUrn
		isSequence     bool
		pendingForEach bool
		depth          int
	}
	start := searchState{canonical: source.String(), isSequence: isSequence}
	stepsInto := map[searchState][]searchState{}
	targetStates := map[searchState]bool{}
	seen := map[searchState]bool{start: true}
	queue := []queued{{urn: source, isSequence: isSequence}}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		if item.depth >= maxDepth {
			continue
		}
		from := searchState{canonical: item.urn.String(), isSequence: item.isSequence, pendingForEach: item.pendingForEach}
		edges, outSeqs := g.getOutgoingEdges(item.urn, item.isSequence)
		for i, edge := range edges {
			if item.pendingForEach && !canFollowForEach(edge) {
				continue
			}
			next := searchState{canonical: edge.ToSpec.String(), isSequence: outSeqs[i], pendingForEach: edge.Type == EdgeTypeForEach}
			stepsInto[next] = append(stepsInto[next], from)
			// The target: a node inside it, reached by a step, not waiting on
			// a ForEach's cap.
			if !next.pendingForEach && reaches(edge.ToSpec, target) {
				targetStates[next] = true
			}
			if !seen[next] {
				seen[next] = true
				queue = append(queue, queued{urn: edge.ToSpec, isSequence: next.isSequence, pendingForEach: next.pendingForEach, depth: item.depth + 1})
			}
		}
	}
	if len(targetStates) == 0 {
		return nil
	}
	distances := map[searchState]int{}
	var backward []searchState
	for state := range targetStates {
		distances[state] = 0
		backward = append(backward, state)
	}
	for len(backward) > 0 {
		state := backward[0]
		backward = backward[1:]
		for _, before := range stepsInto[state] {
			if _, known := distances[before]; !known {
				distances[before] = distances[state] + 1
				backward = append(backward, before)
			}
		}
	}
	return distances
}

// FindPathsToExactTarget performs iterative deepening DFS to find the routes to
// target, read open: a route ends where its node, read complete, is inside the
// target (reaches), and names what it gives as its TargetMediaUrn. It steps
// only into states the target is within reach of, and returns the routes in
// inRouteOrder.
func (g *LiveCapFab) FindPathsToExactTarget(
	source, target *urn.MediaUrn,
	isSequence bool,
	maxDepth, maxPaths int,
) []*Strand {
	distances := g.distancesToExactTarget(source, target, isSequence, maxDepth)
	if distances == nil {
		return nil
	}
	var results []*Strand

	for depth := 1; depth <= maxDepth; depth++ {
		if len(results) >= maxPaths {
			break
		}
		visited := make(map[visitedKey]bool)
		g.iddfsFind(source, source, target, isSequence, distances, nil, visited, depth, maxPaths, &results)
	}

	return inRouteOrder(target, results)
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
	distances := g.distancesToExactTarget(source, target, isSequence, maxDepth)
	if distances == nil {
		return nil
	}
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

		g.iddfsFind(source, source, target, isSequence, distances, nil, visited, depth, maxPaths, &results)

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

	results = inRouteOrder(target, results)

	onEvent(PathFindingEventComplete{
		TotalPaths:         len(results),
		TotalNodesExplored: totalNodesExplored,
	})

	for _, p := range results {
		onEvent(PathFindingEventPathFound{Path: p})
	}

	return results
}

// inRouteOrder orders the routes to target as the proved model does
// (urn.RoutesInOrder): the nearest to what was asked first, measured as
// dispatch ranks caps, then fewer cap steps, then the higher total
// specificity. Routes equal in all three keep comparePaths' structural order,
// which they are handed to the model in.
func inRouteOrder(target *urn.MediaUrn, routes []*Strand) []*Strand {
	sort.SliceStable(routes, func(i, j int) bool {
		return comparePaths(routes[i], routes[j]) < 0
	})
	caps := make([][]*urn.CapUrn, len(routes))
	gives := make([]*urn.MediaUrn, len(routes))
	for i, route := range routes {
		for _, step := range route.Steps {
			if step.IsCap() {
				caps[i] = append(caps[i], step.CapUrnVal)
			}
		}
		gives[i] = route.TargetMediaUrn
	}
	ordered := make([]*Strand, 0, len(routes))
	for _, index := range urn.RoutesInOrder(target, caps, gives) {
		ordered = append(ordered, routes[index])
	}
	return ordered
}

// comparePaths orders two paths: fewest cap steps first (ForEach and Collect
// do not count), then the highest sum of step specificities, then the steps
// compared structurally in turn (compareSteps), the shorter path first when
// one is a prefix of the other. No URN is compared as text.
func comparePaths(a, b *Strand) int {
	if a.CapStepCount != b.CapStepCount {
		if a.CapStepCount < b.CapStepCount {
			return -1
		}
		return 1
	}
	specA := 0
	for _, s := range a.Steps {
		specA += s.Specificity()
	}
	specB := 0
	for _, s := range b.Steps {
		specB += s.Specificity()
	}
	if specA != specB {
		if specA > specB {
			return -1
		}
		return 1
	}
	for i := 0; i < len(a.Steps) && i < len(b.Steps); i++ {
		if cmp := compareSteps(a.Steps[i], b.Steps[i]); cmp != 0 {
			return cmp
		}
	}
	return len(a.Steps) - len(b.Steps)
}

// compareSteps orders two strand steps structurally: a cap before a ForEach
// before a Collect; caps by CapUrn.Compare and then the output they take (the
// main output, nil, before a named one); ForEach and Collect by their media
// definition; then FromSpec and ToSpec by MediaUrn.Compare.
func compareSteps(a, b *StrandStep) int {
	if a.StepType != b.StepType {
		return stepRank(a.StepType) - stepRank(b.StepType)
	}
	switch a.StepType {
	case StepTypeCap:
		if cmp := a.CapUrnVal.Compare(b.CapUrnVal); cmp != 0 {
			return cmp
		}
		if cmp := a.OutputUrn.Compare(b.OutputUrn); cmp != 0 {
			return cmp
		}
	case StepTypeForEach, StepTypeCollect:
		if cmp := a.MediaDef.Compare(b.MediaDef); cmp != 0 {
			return cmp
		}
	}
	if cmp := a.FromSpec.Compare(b.FromSpec); cmp != 0 {
		return cmp
	}
	return a.ToSpec.Compare(b.ToSpec)
}

// stepRank is a step type's place in the structural step order.
func stepRank(stepType StrandStepType) int {
	switch stepType {
	case StepTypeCap:
		return 0
	case StepTypeForEach:
		return 1
	case StepTypeCollect:
		return 2
	default:
		panic(fmt.Sprintf("strand step invariant: no step type %d", stepType))
	}
}

// iddfsFind performs depth-limited DFS from current toward target.
// originalSource is the root source used to construct Strand.SourceMediaUrn.
// distances is distancesToExactTarget's: a step into a state the target is not
// within the steps left of is not taken, since no route through it ends at the
// target within the depth limit.
func (g *LiveCapFab) iddfsFind(
	originalSource *urn.MediaUrn,
	current *urn.MediaUrn,
	target *urn.MediaUrn,
	isSequence bool,
	distances map[searchState]int,
	path []*LiveMachinePlanEdge,
	visited map[visitedKey]bool,
	depthLimit int,
	maxPaths int,
	results *[]*Strand,
) {
	if len(*results) >= maxPaths {
		return
	}
	pendingForEach := len(path) > 0 && path[len(path)-1].Type == EdgeTypeForEach

	// Check if we've reached the target: the node is inside it (reaches).
	// Skip this check at the starting node (empty path) — when source==target,
	// we still want to explore edges to find round-trip transformation paths.
	if len(path) > 0 && reaches(current, target) {
		// Only record the path when it exactly fills the depth budget for this IDDFS
		// iteration (depthLimit == 0). This mirrors Rust's `current_path.len() == depth_limit`
		// check and ensures each path is found exactly once — at the shallowest depth that
		// reaches it — preventing duplicates across outer loop iterations.
		if depthLimit == 0 && !pendingForEach {
			var steps []*StrandStep
			capCount := 0
			// A found cap's single input is its MAIN input, fed by the
			// previous cap of the path, or by the strand input when it is the
			// first: the planner routes main input to output only.
			var previousCap *StrandStep
			for _, edge := range path {
				step := edgeToStep(edge)
				steps = append(steps, step)
				if step.IsCap() {
					source := NewArgSourceStrandInput()
					if previousCap != nil {
						source = NewArgSourceStep(previousCap.TokenId)
					}
					inSpec, err := urn.NewMediaUrnFromString(step.CapUrnVal.InSpec())
					if err != nil {
						panic(fmt.Sprintf("cap registry invariant: in= of '%s' is a valid MediaUrn: %v", step.CapUrnVal, err))
					}
					step.Inputs = []CapInput{{ArgUrn: inSpec, Source: source}}
					previousCap = step
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
					// What the route gives, inside the target asked for.
					TargetMediaUrn: current,
					TotalSteps:     len(steps),
					CapStepCount:   capCount,
					Description:    strings.Join(titles, " → "),
				})
			}
		}
		// For round-trip paths (source==target), don't return early —
		// continue exploring edges to find longer paths through this node.
		if !pendingForEach && !originalSource.IsEquivalent(target) {
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
	if !(isRoundtrip && reaches(current, target)) {
		if visited[vk] {
			return
		}
		visited[vk] = true
		defer func() { delete(visited, vk) }()
	}

	edges, outSeqs := g.getOutgoingEdges(current, isSequence)
	for i, edge := range edges {
		if pendingForEach && !canFollowForEach(edge) {
			continue
		}
		next := searchState{canonical: edge.ToSpec.String(), isSequence: outSeqs[i], pendingForEach: edge.Type == EdgeTypeForEach}
		if distance, reaches := distances[next]; !reaches || distance > depthLimit-1 {
			continue
		}
		// Make a fresh slice to avoid aliasing between recursive branches.
		newPath := make([]*LiveMachinePlanEdge, len(path)+1)
		copy(newPath, path)
		newPath[len(path)] = edge
		g.iddfsFind(originalSource, edge.ToSpec, target, outSeqs[i], distances, newPath, visited, depthLimit-1, maxPaths, results)
		if len(*results) >= maxPaths {
			return
		}
	}
}

func canFollowForEach(edge *LiveMachinePlanEdge) bool {
	return edge.Type == EdgeTypeCap && !edge.InputIsSequence
}

func edgeToStep(edge *LiveMachinePlanEdge) *StrandStep {
	switch edge.Type {
	case EdgeTypeCap:
		return &StrandStep{
			TokenId:          MintStepToken(),
			StepType:         StepTypeCap,
			FromSpec:         edge.FromSpec,
			ToSpec:           edge.ToSpec,
			CapUrnVal:        edge.CapUrnVal,
			StepTitle:        edge.CapTitle,
			SpecificityVal:   edge.SpecificityVal,
			InputIsSequence:  edge.InputIsSequence,
			OutputIsSequence: edge.OutputIsSequence,
			OutputUrn:        edge.OutputUrn,
		}
	case EdgeTypeForEach:
		return &StrandStep{
			TokenId:  MintStepToken(),
			StepType: StepTypeForEach,
			FromSpec: edge.FromSpec,
			ToSpec:   edge.ToSpec,
			MediaDef: edge.FromSpec,
		}
	case EdgeTypeCollect:
		return &StrandStep{
			TokenId:  MintStepToken(),
			StepType: StepTypeCollect,
			FromSpec: edge.FromSpec,
			ToSpec:   edge.ToSpec,
			MediaDef: edge.FromSpec,
		}
	}
	return nil
}
