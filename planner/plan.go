package planner

import (
	"encoding/json"
	"fmt"
	"sort"
)

// NodeId is a string alias for node identifiers.
type NodeId = string

// ExecutionNodeKind identifies the type of execution node.
type ExecutionNodeKind int

const (
	// NodeKindCap — execute a single cap.
	NodeKindCap ExecutionNodeKind = iota
	// NodeKindCapOutput — one output of a step other than its main one, as a
	// node of its own.
	NodeKindCapOutput
	// NodeKindForEach — a map: its body runs once per item of its sources,
	// zipped.
	NodeKindForEach
	// NodeKindItem — what one run of a map's body is given of one of the
	// map's sources.
	NodeKindItem
	// NodeKindCollect — a gather: several values delivered to one sequence
	// arg as one sequence, in order.
	NodeKindCollect
	// NodeKindInputSlot — entry point for user-provided files.
	NodeKindInputSlot
	// NodeKindOutput — terminal node marking a result.
	NodeKindOutput
)

// ForEachSource is one sequence a map iterates.
type ForEachSource struct {
	// Node is the node that holds the sequence, as the map's own scope sees it.
	Node NodeId `json:"node"`
	// Item is the Item node the body is given each item as.
	Item NodeId `json:"item"`
}

// ExecutionNodeType holds the variant data for an execution node. Only the
// fields of its Kind mean anything.
type ExecutionNodeType struct {
	Kind ExecutionNodeKind

	// Cap, ForEach: the stable identity of the step or the map (the
	// StrandStep.TokenId it came from). A run's progress is reported under
	// it, so it maps back to the graph element a client rendered.
	TokenId StepToken

	// Cap fields
	CapUrn       string
	ArgBindings  *ArgumentBindings
	PreferredCap *string
	// Policy is what the step does when it fails: how many further attempts
	// a retryable failure gets, and whether a failure that stands leaves the
	// step's outputs absent instead of failing the run. The zero value is to
	// fail at once.
	Policy StepPolicy

	// CapOutput fields. A step's main output is the step's node. Every other
	// output something takes is one of these: what consumes it wires from
	// this node, and its value in a run is stored under this node's id.
	//
	// CapNode is the step whose output this is.
	CapNode NodeId
	// CarriesInput: the output carries the step's input, type unchanged,
	// rather than its own URN (a router's outputs).
	CarriesInput bool

	// OutputMediaUrn is, for a CapOutput, the output's media URN in the cap's
	// definition (its identity, and what its response stream names); for a
	// Collect, the element type of the gathered sequence.
	OutputMediaUrn string

	// ForEach fields. Run `i` of the body is given item `i` of every source;
	// sources of unequal length fail the run. What a body node produces is,
	// from outside the map, the sequence of it over the runs in item order —
	// one flat sequence when the map is inside another.
	//
	// Sources are the sequences the map zips, each with the node the body is
	// given its item as. At least one.
	Sources []ForEachSource
	// Body is every node that runs once per item: the item nodes, the steps,
	// the outputs and gathers that belong to them, and the maps opened inside
	// this one (whose own bodies are theirs, not listed here).
	Body []NodeId

	// Item fields
	//
	// Foreach is the map this is an item of.
	Foreach NodeId
	// Source is the source node whose item it is.
	Source NodeId
	// MediaUrn is the item's media URN: the element type of the source
	// sequence.
	MediaUrn string

	// Collect fields: the nodes gathered, in the order they are delivered. A
	// single value contributes one item, a sequence its items; a member that
	// is absent contributes none.
	InputNodes []NodeId

	// InputSlot fields
	SlotName         string
	ExpectedMediaUrn string
	Cardinality      InputCardinality

	// Output fields. OutputName is also the output's name in the cap's
	// definition for a CapOutput (a label; unique per cap).
	OutputName string
	SourceNode NodeId
}

// NewMachineNodeType creates a Cap execution node type under `token`.
func NewMachineNodeType(capUrn string, bindings *ArgumentBindings, preferredCap *string, token StepToken) *ExecutionNodeType {
	if bindings == nil {
		bindings = NewArgumentBindings()
	}
	return &ExecutionNodeType{
		Kind:         NodeKindCap,
		TokenId:      token,
		CapUrn:       capUrn,
		ArgBindings:  bindings,
		PreferredCap: preferredCap,
	}
}

// NewInputSlotNodeType creates an InputSlot execution node type.
func NewInputSlotNodeType(slotName, expectedMediaUrn string, cardinality InputCardinality) *ExecutionNodeType {
	return &ExecutionNodeType{
		Kind:             NodeKindInputSlot,
		SlotName:         slotName,
		ExpectedMediaUrn: expectedMediaUrn,
		Cardinality:      cardinality,
	}
}

// NewOutputNodeType creates an Output execution node type.
func NewOutputNodeType(outputName, sourceNode string) *ExecutionNodeType {
	return &ExecutionNodeType{
		Kind:       NodeKindOutput,
		OutputName: outputName,
		SourceNode: sourceNode,
	}
}

// MachineNode is a node in the execution plan.
type MachineNode struct {
	ID          string
	NodeType    *ExecutionNodeType
	Description *string
}

// NewMachineNode creates a Cap node with empty bindings.
//
// Mints a fresh token: this constructor builds cap nodes for plans that are
// not derived from a resolved strand. A run whose progress must map back to a
// client-rendered strand uses NewMachineNodeWithBindingsToken to keep the
// strand's identity.
func NewMachineNode(id, capUrn string) *MachineNode {
	return &MachineNode{
		ID:       id,
		NodeType: NewMachineNodeType(capUrn, nil, nil, MintStepToken()),
	}
}

// NewMachineNodeWithBindings creates a Cap node with bindings. Mints a fresh
// token.
func NewMachineNodeWithBindings(id, capUrn string, bindings *ArgumentBindings) *MachineNode {
	return &MachineNode{
		ID:       id,
		NodeType: NewMachineNodeType(capUrn, bindings, nil, MintStepToken()),
	}
}

// NewMachineNodeWithBindingsToken creates a Cap node with bindings, keeping a
// strand step's identity.
func NewMachineNodeWithBindingsToken(id, capUrn string, bindings *ArgumentBindings, token StepToken) *MachineNode {
	return &MachineNode{
		ID:       id,
		NodeType: NewMachineNodeType(capUrn, bindings, nil, token),
	}
}

// NewMachineNodeWithPreference creates a Cap node with bindings and preferred
// cap. Mints a fresh token.
func NewMachineNodeWithPreference(id, capUrn string, bindings *ArgumentBindings, preferredCap *string) *MachineNode {
	return &MachineNode{
		ID:       id,
		NodeType: NewMachineNodeType(capUrn, bindings, preferredCap, MintStepToken()),
	}
}

// NewForEachNode creates a map node. Mints a fresh token; a run whose
// progress must map back to a rendered strand uses NewForEachNodeToken.
func NewForEachNode(id string, sources []ForEachSource, body []NodeId) *MachineNode {
	return NewForEachNodeToken(id, sources, body, MintStepToken())
}

// NewForEachNodeToken creates a map node, keeping the originating step's
// identity.
func NewForEachNodeToken(id string, sources []ForEachSource, body []NodeId, token StepToken) *MachineNode {
	desc := "Map: run the body once per item"
	return &MachineNode{
		ID: id,
		NodeType: &ExecutionNodeType{
			Kind:    NodeKindForEach,
			TokenId: token,
			Sources: sources,
			Body:    body,
		},
		Description: &desc,
	}
}

// NewItemNode creates the node a map's body is given one source's item as.
func NewItemNode(id, foreach, source, mediaUrn string) *MachineNode {
	desc := fmt.Sprintf("Item of %s", source)
	return &MachineNode{
		ID: id,
		NodeType: &ExecutionNodeType{
			Kind:     NodeKindItem,
			Foreach:  foreach,
			Source:   source,
			MediaUrn: mediaUrn,
		},
		Description: &desc,
	}
}

// NewCapOutputNode creates the node of an output of `capNode` other than its
// main one.
func NewCapOutputNode(id, capNode, outputMediaUrn, outputName string, carriesInput bool) *MachineNode {
	desc := fmt.Sprintf("Output: %s", outputName)
	return &MachineNode{
		ID: id,
		NodeType: &ExecutionNodeType{
			Kind:           NodeKindCapOutput,
			CapNode:        capNode,
			OutputMediaUrn: outputMediaUrn,
			OutputName:     outputName,
			CarriesInput:   carriesInput,
		},
		Description: &desc,
	}
}

// NewCollectNode creates a gather node.
func NewCollectNode(id string, inputNodes []string, outputMediaUrn string) *MachineNode {
	desc := "Gather: several values into one sequence"
	return &MachineNode{
		ID: id,
		NodeType: &ExecutionNodeType{
			Kind:           NodeKindCollect,
			InputNodes:     inputNodes,
			OutputMediaUrn: outputMediaUrn,
		},
		Description: &desc,
	}
}

// NewInputSlotNode creates an InputSlot node.
func NewInputSlotNode(id, slotName, mediaUrn string, cardinality InputCardinality) *MachineNode {
	desc := fmt.Sprintf("Input: %s", slotName)
	return &MachineNode{
		ID:          id,
		NodeType:    NewInputSlotNodeType(slotName, mediaUrn, cardinality),
		Description: &desc,
	}
}

// NewOutputNode creates an Output node.
func NewOutputNode(id, outputName, sourceNode string) *MachineNode {
	desc := fmt.Sprintf("Output: %s", outputName)
	return &MachineNode{
		ID:          id,
		NodeType:    NewOutputNodeType(outputName, sourceNode),
		Description: &desc,
	}
}

// IsCap returns true if this is a Cap node.
func (n *MachineNode) IsCap() bool { return n.NodeType.Kind == NodeKindCap }

// IsFanOut returns true if this is a ForEach node.
func (n *MachineNode) IsFanOut() bool { return n.NodeType.Kind == NodeKindForEach }

// IsFanIn returns true if this is a Collect node.
func (n *MachineNode) IsFanIn() bool { return n.NodeType.Kind == NodeKindCollect }

// GetCapUrn returns the cap URN if this is a Cap node.
func (n *MachineNode) GetCapUrn() *string {
	if n.NodeType.Kind == NodeKindCap {
		return &n.NodeType.CapUrn
	}
	return nil
}

// GetPreferredCap returns the preferred cap if this is a Cap node with one set.
func (n *MachineNode) GetPreferredCap() *string {
	if n.NodeType.Kind == NodeKindCap {
		return n.NodeType.PreferredCap
	}
	return nil
}

// EdgeKind identifies the type of edge.
type EdgeKind int

const (
	// EdgeKindDirect — into a step: its MAIN input. Into any other node:
	// what that node is of — a map's source, an output node's step, a plan
	// output's source.
	EdgeKindDirect EdgeKind = iota
	// EdgeKindArg — into a specific NON-main argument of the target step,
	// identified by the URN that argument's stream is delivered under.
	EdgeKindArg
	// EdgeKindIteration — from a map to one of its item nodes.
	EdgeKindIteration
	// EdgeKindCollection — from a gathered node into its gather.
	EdgeKindCollection
)

// EdgeType holds the edge type. An edge carries the whole value its source
// produced: there is no reading of a part of it.
type EdgeType struct {
	Kind EdgeKind
	// ArgUrn is, for an Arg edge, the URN the argument's stream is delivered
	// under.
	ArgUrn string
}

// DirectEdgeType creates a Direct edge type.
func DirectEdgeType() *EdgeType { return &EdgeType{Kind: EdgeKindDirect} }

// ArgEdgeType creates an Arg edge type.
func ArgEdgeType(argUrn string) *EdgeType { return &EdgeType{Kind: EdgeKindArg, ArgUrn: argUrn} }

// IterationEdgeType creates an Iteration edge type.
func IterationEdgeType() *EdgeType { return &EdgeType{Kind: EdgeKindIteration} }

// CollectionEdgeType creates a Collection edge type.
func CollectionEdgeType() *EdgeType { return &EdgeType{Kind: EdgeKindCollection} }

// MachinePlanEdge is a directed edge in the execution plan.
type MachinePlanEdge struct {
	FromNode string
	ToNode   string
	Type     *EdgeType
}

// NewDirectEdge creates a direct edge.
func NewDirectEdge(from, to string) *MachinePlanEdge {
	return &MachinePlanEdge{FromNode: from, ToNode: to, Type: DirectEdgeType()}
}

// NewArgEdge creates an edge feeding a specific non-main argument (by its
// media URN) of the target cap.
func NewArgEdge(from, to, argUrn string) *MachinePlanEdge {
	return &MachinePlanEdge{FromNode: from, ToNode: to, Type: ArgEdgeType(argUrn)}
}

// NewIterationEdge creates an iteration edge (map → item).
func NewIterationEdge(from, to string) *MachinePlanEdge {
	return &MachinePlanEdge{FromNode: from, ToNode: to, Type: IterationEdgeType()}
}

// NewCollectionEdge creates a collection edge (gathered node → gather).
func NewCollectionEdge(from, to string) *MachinePlanEdge {
	return &MachinePlanEdge{FromNode: from, ToNode: to, Type: CollectionEdgeType()}
}

// MachinePlan is the complete execution plan DAG.
type MachinePlan struct {
	Name        string
	Nodes       map[string]*MachineNode
	Edges       []*MachinePlanEdge
	EntryNodes  []string
	OutputNodes []string
	Metadata    map[string]any
}

// NewMachinePlan creates an empty plan.
func NewMachinePlan(name string) *MachinePlan {
	return &MachinePlan{
		Name:  name,
		Nodes: make(map[string]*MachineNode),
	}
}

// AddNode adds a node. InputSlot nodes are auto-registered as entry nodes,
// Output nodes as output nodes.
func (p *MachinePlan) AddNode(node *MachineNode) {
	p.Nodes[node.ID] = node
	switch node.NodeType.Kind {
	case NodeKindInputSlot:
		p.EntryNodes = append(p.EntryNodes, node.ID)
	case NodeKindOutput:
		p.OutputNodes = append(p.OutputNodes, node.ID)
	}
}

// AddEdge adds an edge to the plan.
func (p *MachinePlan) AddEdge(edge *MachinePlanEdge) {
	p.Edges = append(p.Edges, edge)
}

// GetNode returns a node by ID.
func (p *MachinePlan) GetNode(id string) *MachineNode {
	return p.Nodes[id]
}

// Validate checks plan structure: every edge, entry and output names a node
// of the plan, and what a node names is there and is what it is named as.
func (p *MachinePlan) Validate() error {
	for _, edge := range p.Edges {
		if _, ok := p.Nodes[edge.FromNode]; !ok {
			return NewInternalError(fmt.Sprintf("Edge from_node '%s' not found in plan", edge.FromNode))
		}
		if _, ok := p.Nodes[edge.ToNode]; !ok {
			return NewInternalError(fmt.Sprintf("Edge to_node '%s' not found in plan", edge.ToNode))
		}
	}
	for _, entryID := range p.EntryNodes {
		if _, ok := p.Nodes[entryID]; !ok {
			return NewInternalError(fmt.Sprintf("Entry node '%s' not found in plan", entryID))
		}
	}
	for _, outputID := range p.OutputNodes {
		if _, ok := p.Nodes[outputID]; !ok {
			return NewInternalError(fmt.Sprintf("Output node '%s' not found in plan", outputID))
		}
	}

	known := func(owner, what, id string) error {
		if _, ok := p.Nodes[id]; ok {
			return nil
		}
		return NewInternalError(fmt.Sprintf("node '%s' names %s '%s', which is not in the plan", owner, what, id))
	}
	inBody := make(map[string]string)
	// In node-id order, so the same plan is refused the same way every time.
	ids := make([]string, 0, len(p.Nodes))
	for id := range p.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		nt := p.Nodes[id].NodeType
		switch nt.Kind {
		case NodeKindCap, NodeKindInputSlot:
		case NodeKindOutput:
			if err := known(id, "source", nt.SourceNode); err != nil {
				return err
			}
		case NodeKindCollect:
			for _, input := range nt.InputNodes {
				if err := known(id, "gathered node", input); err != nil {
					return err
				}
			}
		case NodeKindCapOutput:
			if err := known(id, "step", nt.CapNode); err != nil {
				return err
			}
			if !p.Nodes[nt.CapNode].IsCap() {
				return NewInternalError(fmt.Sprintf(
					"output node '%s' names '%s' as its step, which is not a step", id, nt.CapNode))
			}
		case NodeKindItem:
			if err := known(id, "map", nt.Foreach); err != nil {
				return err
			}
			listed := false
			if of := p.Nodes[nt.Foreach].NodeType; of.Kind == NodeKindForEach {
				for _, source := range of.Sources {
					if source.Item == id && source.Node == nt.Source {
						listed = true
					}
				}
			}
			if !listed {
				return NewInternalError(fmt.Sprintf(
					"item node '%s' names map '%s' and source '%s', and that map does not list it so",
					id, nt.Foreach, nt.Source))
			}
		case NodeKindForEach:
			if len(nt.Sources) == 0 {
				return NewInternalError(fmt.Sprintf("map '%s' iterates nothing", id))
			}
			for _, source := range nt.Sources {
				if err := known(id, "source", source.Node); err != nil {
					return err
				}
				if err := known(id, "item", source.Item); err != nil {
					return err
				}
				item := p.Nodes[source.Item].NodeType
				if item.Kind != NodeKindItem || item.Foreach != id || item.Source != source.Node {
					return NewInternalError(fmt.Sprintf(
						"map '%s' lists '%s' as its item of '%s', which is not that", id, source.Item, source.Node))
				}
				inBodyOfThis := false
				for _, member := range nt.Body {
					if member == source.Item {
						inBodyOfThis = true
					}
				}
				if !inBodyOfThis {
					return NewInternalError(fmt.Sprintf(
						"map '%s' does not list its item '%s' in its body", id, source.Item))
				}
			}
			for _, member := range nt.Body {
				if err := known(id, "body node", member); err != nil {
					return err
				}
				if member == id {
					return NewInternalError(fmt.Sprintf("map '%s' lists itself in its body", id))
				}
				if other, taken := inBody[member]; taken {
					return NewInternalError(fmt.Sprintf(
						"node '%s' is in the body of two maps ('%s' and '%s')", member, other, id))
				}
				inBody[member] = id
			}
		default:
			return NewInternalError(fmt.Sprintf("node '%s' is of no kind a plan has", id))
		}
	}
	return nil
}

// EnclosingMaps returns the map each node runs in: node id → the id of the
// map whose body lists it. A node of the machine itself has no entry.
func (p *MachinePlan) EnclosingMaps() map[string]string {
	enclosing := make(map[string]string)
	for id, node := range p.Nodes {
		if node.NodeType.Kind != NodeKindForEach {
			continue
		}
		for _, member := range node.NodeType.Body {
			enclosing[member] = id
		}
	}
	return enclosing
}

// MapMembers returns every node inside map `foreachID`: its body, and the
// bodies of the maps opened inside it.
func (p *MachinePlan) MapMembers(foreachID string) map[string]bool {
	members := make(map[string]bool)
	pending := []string{foreachID}
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		node, ok := p.Nodes[id]
		if !ok || node.NodeType.Kind != NodeKindForEach {
			continue
		}
		for _, member := range node.NodeType.Body {
			if !members[member] {
				members[member] = true
				pending = append(pending, member)
			}
		}
	}
	return members
}

// TopologicalOrder returns nodes in topological order using Kahn's algorithm.
func (p *MachinePlan) TopologicalOrder() ([]*MachineNode, error) {
	inDegree := make(map[string]int)
	adj := make(map[string][]string)

	for id := range p.Nodes {
		inDegree[id] = 0
		adj[id] = nil
	}

	for _, edge := range p.Edges {
		if _, ok := inDegree[edge.ToNode]; ok {
			inDegree[edge.ToNode]++
		}
		if _, ok := adj[edge.FromNode]; ok {
			adj[edge.FromNode] = append(adj[edge.FromNode], edge.ToNode)
		}
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	var result []*MachineNode
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if node, ok := p.Nodes[id]; ok {
			result = append(result, node)
		}
		for _, neighbor := range adj[id] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(result) != len(p.Nodes) {
		return nil, NewInternalError("Cycle detected in execution plan")
	}
	return result, nil
}

// SingleCap creates a simple 3-node plan: input → cap → output.
func SingleCap(capUrn, inputMedia, _ string, filePathArgName string) *MachinePlan {
	plan := NewMachinePlan(fmt.Sprintf("single_%s", capUrn))
	plan.AddNode(NewInputSlotNode("input_slot", "input", inputMedia, CardinalitySingle))

	bindings := NewArgumentBindings()
	bindings.AddFilePath(filePathArgName)
	plan.AddNode(NewMachineNodeWithBindings("cap_0", capUrn, bindings))
	plan.AddNode(NewOutputNode("output", "result", "cap_0"))

	plan.AddEdge(NewDirectEdge("input_slot", "cap_0"))
	plan.AddEdge(NewDirectEdge("cap_0", "output"))
	return plan
}

// LinearChain creates a linear chain plan: input → cap_0 → ... → output.
func LinearChain(capUrns []string, inputMedia, _ string, filePathArgNames []string) *MachinePlan {
	plan := NewMachinePlan("linear_chain")
	if len(capUrns) == 0 {
		return plan
	}

	plan.AddNode(NewInputSlotNode("input_slot", "input", inputMedia, CardinalitySingle))

	prevID := "input_slot"
	for i, urn := range capUrns {
		nodeID := fmt.Sprintf("cap_%d", i)
		bindings := NewArgumentBindings()
		if i < len(filePathArgNames) {
			bindings.AddFilePath(filePathArgNames[i])
		}
		plan.AddNode(NewMachineNodeWithBindings(nodeID, urn, bindings))
		plan.AddEdge(NewDirectEdge(prevID, nodeID))
		prevID = nodeID
	}

	plan.AddNode(NewOutputNode("output", "result", prevID))
	plan.AddEdge(NewDirectEdge(prevID, "output"))
	return plan
}

// HasForeach reports whether this plan contains a map.
func (p *MachinePlan) HasForeach() bool {
	for _, node := range p.Nodes {
		if node.NodeType.Kind == NodeKindForEach {
			return true
		}
	}
	return false
}

// MapRun is one run of a map's body: which map, and which of its runs.
type MapRun struct {
	// ForEachTokenID is the stable identity of the map.
	ForEachTokenID StepToken `json:"foreach_token_id"`
	// BodyIndex is the index of the run within the map (0-based, item order).
	BodyIndex int `json:"body_index"`
}

// UnmarshalJSON reads a run. One that names no map is not a run.
func (r *MapRun) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, field := range []string{"foreach_token_id", "body_index"} {
		if _, present := fields[field]; !present {
			return fmt.Errorf("a map run is missing field `%s`", field)
		}
	}
	type mapRun MapRun
	var decoded mapRun
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.BodyIndex < 0 {
		return fmt.Errorf("a map run's body_index must be non-negative")
	}
	*r = MapRun(decoded)
	return nil
}

// BodyOutcome tracks what happened during a single ForEach body execution.
//
// For linear (non-ForEach) pipelines, a single BodyOutcome with BodyIndex=0
// represents the entire execution.
type BodyOutcome struct {
	// ForEachTokenID is the immutable ForEach StrandStep token that scopes
	// BodyIndex. Nil only for the single linear whole-run outcome.
	ForEachTokenID *StepToken `json:"foreach_token_id"`
	// BodyIndex is the index of this body within the ForEach (0-based). 0 for linear pipelines.
	BodyIndex int `json:"body_index"`
	// Enclosing is the runs this body is inside, when its map is opened
	// inside another's body: outermost first. Empty for a map of the machine
	// itself, and for a linear pipeline. A body index alone does not name a
	// run of a nested map — it recurs under every run of the map around it.
	Enclosing []MapRun `json:"enclosing"`
	// Success indicates whether this body completed successfully.
	Success bool `json:"success"`
	// CapUrns are the cap URNs in the body's execution pathway (in execution order).
	CapUrns []string `json:"cap_urns"`
	// FailedTokenID is the immutable StrandStep token that failed. Nil if no
	// single step is attributable or the body succeeded.
	FailedTokenID *StepToken `json:"failed_token_id"`
	// Error is the error message if the body failed.
	Error *string `json:"error"`
	// FailedArgURN is the argument media URN attributed by the emit source.
	FailedArgURN *string `json:"failed_arg_urn"`
	// Title is the human-readable title for this body, from stream metadata.
	Title *string `json:"title"`
	// SavedPaths are the file paths saved by this body's IncrementalWriter.
	SavedPaths []string `json:"saved_paths"`
	// TotalBytes is the total bytes written by this body.
	TotalBytes uint64 `json:"total_bytes"`
	// DurationMs is the execution duration in milliseconds.
	DurationMs uint64 `json:"duration_ms"`
	// ItemPreviewText is the bounded UTF-8 preview captured at a ForEach split.
	ItemPreviewText *string `json:"item_preview_text"`
	// ItemByteCount is the byte size of the body input at the split.
	ItemByteCount uint64 `json:"item_byte_count"`
}

func (b BodyOutcome) validate() error {
	if b.BodyIndex < 0 {
		return fmt.Errorf("body outcome body_index must be non-negative")
	}
	if b.ForEachTokenID == nil && b.BodyIndex != 0 {
		return fmt.Errorf("linear body outcome must use body_index 0")
	}
	if b.ForEachTokenID == nil && len(b.Enclosing) != 0 {
		return fmt.Errorf("a linear body outcome is inside no map")
	}
	for _, run := range b.Enclosing {
		if run.BodyIndex < 0 {
			return fmt.Errorf("body outcome enclosing body_index must be non-negative")
		}
	}
	// foreach_token_id and failed_token_id need no emptiness check here: a
	// StepToken cannot BE empty, and decoding one from "" already failed before
	// this outcome existed.
	if b.FailedArgURN != nil && *b.FailedArgURN == "" {
		return fmt.Errorf("body outcome failed_arg_urn must be null or non-empty")
	}
	return nil
}

// MarshalJSON prevents locally constructed outcomes from emitting an invalid
// execution coordinate. The wire always includes all nullable coordinate keys.
func (b BodyOutcome) MarshalJSON() ([]byte, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	type bodyOutcome BodyOutcome
	written := bodyOutcome(b)
	written.Enclosing = nonNil(b.Enclosing)
	return json.Marshal(written)
}

// UnmarshalJSON keeps the required-but-nullable execution-coordinate shape.
// A missing key is invalid. An explicit null ForEach token identifies the sole
// linear whole-run coordinate; null failure fields mean the emit source did not
// attribute the failure to a step or argument.
func (b *BodyOutcome) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, field := range []string{"foreach_token_id", "failed_token_id", "failed_arg_urn"} {
		if _, present := fields[field]; !present {
			return fmt.Errorf("body outcome missing required nullable field %q", field)
		}
	}

	type bodyOutcome BodyOutcome
	var decoded bodyOutcome
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if err := BodyOutcome(decoded).validate(); err != nil {
		return err
	}
	*b = BodyOutcome(decoded)
	return nil
}

// MarshalJSON serializes EdgeType as serde writes the reference's enum: a
// kind that carries nothing is its snake_case name ("direct"); an Arg edge is
// {"arg":{"arg_urn":"…"}}.
func (e *EdgeType) MarshalJSON() ([]byte, error) {
	switch e.Kind {
	case EdgeKindDirect:
		return json.Marshal("direct")
	case EdgeKindIteration:
		return json.Marshal("iteration")
	case EdgeKindCollection:
		return json.Marshal("collection")
	case EdgeKindArg:
		return json.Marshal(map[string]any{"arg": map[string]any{"arg_urn": e.ArgUrn}})
	default:
		return nil, fmt.Errorf("an edge of no kind a plan has: %d", e.Kind)
	}
}

// UnmarshalJSON reads an EdgeType. A kind this library does not know is
// refused: an edge of a kind nobody here can carry is not an edge.
func (e *EdgeType) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err == nil {
		switch name {
		case "direct":
			*e = EdgeType{Kind: EdgeKindDirect}
		case "iteration":
			*e = EdgeType{Kind: EdgeKindIteration}
		case "collection":
			*e = EdgeType{Kind: EdgeKindCollection}
		default:
			return fmt.Errorf("unknown EdgeType: %s", name)
		}
		return nil
	}
	var carrying map[string]json.RawMessage
	if err := json.Unmarshal(data, &carrying); err != nil || len(carrying) != 1 {
		return fmt.Errorf("an EdgeType is a name, or one kind with what it carries: %s", string(data))
	}
	body, isArg := carrying["arg"]
	if !isArg {
		for kind := range carrying {
			return fmt.Errorf("unknown EdgeType: %s", kind)
		}
	}
	var arg struct {
		ArgUrn *string `json:"arg_urn"`
	}
	if err := json.Unmarshal(body, &arg); err != nil {
		return err
	}
	if arg.ArgUrn == nil {
		return fmt.Errorf("an arg edge is missing field `arg_urn`")
	}
	*e = EdgeType{Kind: EdgeKindArg, ArgUrn: *arg.ArgUrn}
	return nil
}

// nodeKindName returns the snake_case wire name for an ExecutionNodeKind.
func nodeKindName(k ExecutionNodeKind) (string, error) {
	switch k {
	case NodeKindCap:
		return "cap", nil
	case NodeKindCapOutput:
		return "cap_output", nil
	case NodeKindForEach:
		return "for_each", nil
	case NodeKindItem:
		return "item", nil
	case NodeKindCollect:
		return "collect", nil
	case NodeKindInputSlot:
		return "input_slot", nil
	case NodeKindOutput:
		return "output", nil
	default:
		return "", fmt.Errorf("a node of no kind a plan has: %d", k)
	}
}

// MarshalJSON serializes ExecutionNodeType as an internally-tagged enum mirroring
// serde's `tag = "node_type", rename_all = "snake_case"`. The variant name is
// written under the "node_type" key and the variant fields are flattened in.
func (n *ExecutionNodeType) MarshalJSON() ([]byte, error) {
	name, err := nodeKindName(n.Kind)
	if err != nil {
		return nil, err
	}
	m := map[string]any{"node_type": name}
	switch n.Kind {
	case NodeKindCap:
		m["token_id"] = n.TokenId
		m["cap_urn"] = n.CapUrn
		bindings := n.ArgBindings
		if bindings == nil {
			bindings = NewArgumentBindings()
		}
		m["arg_bindings"] = bindings
		if n.PreferredCap != nil {
			m["preferred_cap"] = *n.PreferredCap
		}
		if !n.Policy.IsDefault() {
			m["policy"] = n.Policy
		}
	case NodeKindCapOutput:
		m["cap_node"] = n.CapNode
		m["output_media_urn"] = n.OutputMediaUrn
		m["output_name"] = n.OutputName
		m["carries_input"] = n.CarriesInput
	case NodeKindForEach:
		m["token_id"] = n.TokenId
		m["sources"] = nonNil(n.Sources)
		m["body"] = nonNil(n.Body)
	case NodeKindItem:
		m["foreach"] = n.Foreach
		m["source"] = n.Source
		m["media_urn"] = n.MediaUrn
	case NodeKindCollect:
		m["input_nodes"] = nonNil(n.InputNodes)
		m["output_media_urn"] = n.OutputMediaUrn
	case NodeKindInputSlot:
		m["slot_name"] = n.SlotName
		m["expected_media_urn"] = n.ExpectedMediaUrn
		m["cardinality"] = n.Cardinality
	case NodeKindOutput:
		m["output_name"] = n.OutputName
		m["source_node"] = n.SourceNode
	}
	return json.Marshal(m)
}

// nonNil is `items`, or an empty list where there are none: a list a plan
// carries is written as a list.
func nonNil[T any](items []T) []T {
	if items == nil {
		return []T{}
	}
	return items
}

// requireFields refuses a node that leaves out a field its kind has: a node
// read with a part of it missing is not the node that was written.
func requireFields(fields map[string]json.RawMessage, kind string, names ...string) error {
	for _, name := range names {
		if _, present := fields[name]; !present {
			return fmt.Errorf("a %s node is missing field `%s`", kind, name)
		}
	}
	return nil
}

// UnmarshalJSON deserializes the internally-tagged ExecutionNodeType form. A
// node type this library does not know is refused, and so is a node missing a
// field its kind has.
func (n *ExecutionNodeType) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	var kind string
	if raw, present := fields["node_type"]; !present {
		return fmt.Errorf("a plan node is missing field `node_type`")
	} else if err := json.Unmarshal(raw, &kind); err != nil {
		return err
	}
	switch kind {
	case "cap":
		if err := requireFields(fields, kind, "token_id", "cap_urn"); err != nil {
			return err
		}
		var v struct {
			TokenId      StepToken         `json:"token_id"`
			CapUrn       string            `json:"cap_urn"`
			ArgBindings  *ArgumentBindings `json:"arg_bindings"`
			PreferredCap *string           `json:"preferred_cap"`
			Policy       StepPolicy        `json:"policy"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		if v.ArgBindings == nil {
			v.ArgBindings = NewArgumentBindings()
		}
		*n = ExecutionNodeType{
			Kind: NodeKindCap, TokenId: v.TokenId, CapUrn: v.CapUrn,
			ArgBindings: v.ArgBindings, PreferredCap: v.PreferredCap, Policy: v.Policy,
		}
	case "cap_output":
		if err := requireFields(fields, kind, "cap_node", "output_media_urn", "output_name"); err != nil {
			return err
		}
		var v struct {
			CapNode        string `json:"cap_node"`
			OutputMediaUrn string `json:"output_media_urn"`
			OutputName     string `json:"output_name"`
			CarriesInput   bool   `json:"carries_input"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		*n = ExecutionNodeType{
			Kind: NodeKindCapOutput, CapNode: v.CapNode, OutputMediaUrn: v.OutputMediaUrn,
			OutputName: v.OutputName, CarriesInput: v.CarriesInput,
		}
	case "for_each":
		if err := requireFields(fields, kind, "token_id", "sources", "body"); err != nil {
			return err
		}
		var v struct {
			TokenId StepToken       `json:"token_id"`
			Sources []ForEachSource `json:"sources"`
			Body    []NodeId        `json:"body"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		*n = ExecutionNodeType{Kind: NodeKindForEach, TokenId: v.TokenId, Sources: v.Sources, Body: v.Body}
	case "item":
		if err := requireFields(fields, kind, "foreach", "source", "media_urn"); err != nil {
			return err
		}
		var v struct {
			Foreach  string `json:"foreach"`
			Source   string `json:"source"`
			MediaUrn string `json:"media_urn"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		*n = ExecutionNodeType{Kind: NodeKindItem, Foreach: v.Foreach, Source: v.Source, MediaUrn: v.MediaUrn}
	case "collect":
		if err := requireFields(fields, kind, "input_nodes", "output_media_urn"); err != nil {
			return err
		}
		var v struct {
			InputNodes     []string `json:"input_nodes"`
			OutputMediaUrn string   `json:"output_media_urn"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		*n = ExecutionNodeType{Kind: NodeKindCollect, InputNodes: v.InputNodes, OutputMediaUrn: v.OutputMediaUrn}
	case "input_slot":
		if err := requireFields(fields, kind, "slot_name", "expected_media_urn", "cardinality"); err != nil {
			return err
		}
		var v struct {
			SlotName         string           `json:"slot_name"`
			ExpectedMediaUrn string           `json:"expected_media_urn"`
			Cardinality      InputCardinality `json:"cardinality"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		*n = ExecutionNodeType{
			Kind: NodeKindInputSlot, SlotName: v.SlotName,
			ExpectedMediaUrn: v.ExpectedMediaUrn, Cardinality: v.Cardinality,
		}
	case "output":
		if err := requireFields(fields, kind, "output_name", "source_node"); err != nil {
			return err
		}
		var v struct {
			OutputName string `json:"output_name"`
			SourceNode string `json:"source_node"`
		}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		*n = ExecutionNodeType{Kind: NodeKindOutput, OutputName: v.OutputName, SourceNode: v.SourceNode}
	default:
		return fmt.Errorf("unknown ExecutionNodeType node_type: %s", kind)
	}
	return nil
}

// MarshalJSON serializes MachineNode mirroring the Rust struct: id, node_type
// (the internally-tagged ExecutionNodeType object), and an optional description.
func (n *MachineNode) MarshalJSON() ([]byte, error) {
	m := map[string]any{
		"id":        n.ID,
		"node_type": n.NodeType,
	}
	if n.Description != nil {
		m["description"] = *n.Description
	}
	return json.Marshal(m)
}

// UnmarshalJSON deserializes MachineNode.
func (n *MachineNode) UnmarshalJSON(data []byte) error {
	var v struct {
		ID          string             `json:"id"`
		NodeType    *ExecutionNodeType `json:"node_type"`
		Description *string            `json:"description"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	n.ID = v.ID
	n.NodeType = v.NodeType
	n.Description = v.Description
	return nil
}

// MarshalJSON serializes MachinePlanEdge: from_node, to_node, edge_type.
func (e *MachinePlanEdge) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"from_node": e.FromNode,
		"to_node":   e.ToNode,
		"edge_type": e.Type,
	})
}

// UnmarshalJSON deserializes MachinePlanEdge. Mirrors serde's `#[serde(default)]`
// on edge_type: an absent edge_type defaults to Direct.
func (e *MachinePlanEdge) UnmarshalJSON(data []byte) error {
	var v struct {
		FromNode string    `json:"from_node"`
		ToNode   string    `json:"to_node"`
		EdgeType *EdgeType `json:"edge_type"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	e.FromNode = v.FromNode
	e.ToNode = v.ToNode
	if v.EdgeType != nil {
		e.Type = v.EdgeType
	} else {
		e.Type = DirectEdgeType()
	}
	return nil
}

// MarshalJSON serializes MachinePlan: name, nodes, edges, entry_nodes,
// output_nodes, and optional metadata (omitted when nil, mirroring serde).
func (p *MachinePlan) MarshalJSON() ([]byte, error) {
	m := map[string]any{
		"name":         p.Name,
		"nodes":        p.Nodes,
		"edges":        p.Edges,
		"entry_nodes":  p.EntryNodes,
		"output_nodes": p.OutputNodes,
	}
	if p.Metadata != nil {
		m["metadata"] = p.Metadata
	}
	return json.Marshal(m)
}

// UnmarshalJSON deserializes MachinePlan.
func (p *MachinePlan) UnmarshalJSON(data []byte) error {
	var v struct {
		Name        string                  `json:"name"`
		Nodes       map[string]*MachineNode `json:"nodes"`
		Edges       []*MachinePlanEdge      `json:"edges"`
		EntryNodes  []string                `json:"entry_nodes"`
		OutputNodes []string                `json:"output_nodes"`
		Metadata    map[string]any          `json:"metadata"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	p.Name = v.Name
	if v.Nodes != nil {
		p.Nodes = v.Nodes
	} else {
		p.Nodes = make(map[string]*MachineNode)
	}
	p.Edges = v.Edges
	p.EntryNodes = v.EntryNodes
	p.OutputNodes = v.OutputNodes
	p.Metadata = v.Metadata
	return nil
}
