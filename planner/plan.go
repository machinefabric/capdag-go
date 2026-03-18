package planner

import (
	"encoding/json"
	"fmt"
)

// NodeId is a string alias for node identifiers.
type NodeId = string

// MergeStrategy defines how multiple inputs are merged.
type MergeStrategy int

const (
	MergeConcat         MergeStrategy = iota
	MergeZipWith
	MergeFirstSuccess
	MergeAllSuccessful
)

// String returns the snake_case name for serialization.
func (m MergeStrategy) String() string {
	switch m {
	case MergeConcat:
		return "concat"
	case MergeZipWith:
		return "zip_with"
	case MergeFirstSuccess:
		return "first_success"
	case MergeAllSuccessful:
		return "all_successful"
	default:
		return "concat"
	}
}

// MarshalJSON implements json.Marshaler.
func (m MergeStrategy) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.String())
}

// UnmarshalJSON implements json.Unmarshaler.
func (m *MergeStrategy) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "concat":
		*m = MergeConcat
	case "zip_with":
		*m = MergeZipWith
	case "first_success":
		*m = MergeFirstSuccess
	case "all_successful":
		*m = MergeAllSuccessful
	default:
		return fmt.Errorf("unknown MergeStrategy: %s", s)
	}
	return nil
}

// ExecutionNodeKind identifies the type of execution node.
type ExecutionNodeKind int

const (
	NodeKindCap ExecutionNodeKind = iota
	NodeKindForEach
	NodeKindCollect
	NodeKindMerge
	NodeKindSplit
	NodeKindWrapInList
	NodeKindInputSlot
	NodeKindOutput
)

// ExecutionNodeType holds the variant data for an execution node.
type ExecutionNodeType struct {
	Kind ExecutionNodeKind

	// Cap fields
	CapUrn       string
	ArgBindings  *ArgumentBindings
	PreferredCap *string

	// ForEach fields
	InputNode string
	BodyEntry string
	BodyExit  string

	// Collect fields
	InputNodes     []string
	OutputMediaUrn *string

	// Merge fields
	MergeStrat MergeStrategy

	// Split fields
	OutputCount int

	// WrapInList fields
	ItemMediaUrn string
	ListMediaUrn string

	// InputSlot fields
	SlotName         string
	ExpectedMediaUrn string
	Cardinality      InputCardinality

	// Output fields
	OutputName string
	SourceNode string
}

// NewMachineNodeType creates a Cap execution node type.
func NewMachineNodeType(capUrn string, bindings *ArgumentBindings, preferredCap *string) *ExecutionNodeType {
	if bindings == nil {
		bindings = NewArgumentBindings()
	}
	return &ExecutionNodeType{
		Kind:         NodeKindCap,
		CapUrn:       capUrn,
		ArgBindings:  bindings,
		PreferredCap: preferredCap,
	}
}

// NewForEachNodeType creates a ForEach execution node type.
func NewForEachNodeType(inputNode, bodyEntry, bodyExit string) *ExecutionNodeType {
	return &ExecutionNodeType{
		Kind:      NodeKindForEach,
		InputNode: inputNode,
		BodyEntry: bodyEntry,
		BodyExit:  bodyExit,
	}
}

// NewCollectNodeType creates a Collect execution node type.
func NewCollectNodeType(inputNodes []string, outputMediaUrn *string) *ExecutionNodeType {
	return &ExecutionNodeType{
		Kind:           NodeKindCollect,
		InputNodes:     inputNodes,
		OutputMediaUrn: outputMediaUrn,
	}
}

// NewMergeNodeType creates a Merge execution node type.
func NewMergeNodeType(inputNodes []string, strategy MergeStrategy) *ExecutionNodeType {
	return &ExecutionNodeType{
		Kind:       NodeKindMerge,
		InputNodes: inputNodes,
		MergeStrat: strategy,
	}
}

// NewSplitNodeType creates a Split execution node type.
func NewSplitNodeType(inputNode string, outputCount int) *ExecutionNodeType {
	return &ExecutionNodeType{
		Kind:        NodeKindSplit,
		InputNode:   inputNode,
		OutputCount: outputCount,
	}
}

// NewWrapInListNodeType creates a WrapInList execution node type.
func NewWrapInListNodeType(itemMediaUrn, listMediaUrn string) *ExecutionNodeType {
	return &ExecutionNodeType{
		Kind:         NodeKindWrapInList,
		ItemMediaUrn: itemMediaUrn,
		ListMediaUrn: listMediaUrn,
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
func NewMachineNode(id, capUrn string) *MachineNode {
	return &MachineNode{
		ID:       id,
		NodeType: NewMachineNodeType(capUrn, nil, nil),
	}
}

// NewMachineNodeWithBindings creates a Cap node with bindings.
func NewMachineNodeWithBindings(id, capUrn string, bindings *ArgumentBindings) *MachineNode {
	return &MachineNode{
		ID:       id,
		NodeType: NewMachineNodeType(capUrn, bindings, nil),
	}
}

// NewMachineNodeWithPreference creates a Cap node with bindings and preferred cap.
func NewMachineNodeWithPreference(id, capUrn string, bindings *ArgumentBindings, preferredCap *string) *MachineNode {
	return &MachineNode{
		ID:       id,
		NodeType: NewMachineNodeType(capUrn, bindings, preferredCap),
	}
}

// NewForEachNode creates a ForEach node.
func NewForEachNode(id, inputNode, bodyEntry, bodyExit string) *MachineNode {
	desc := "Fan-out: process each item in vector"
	return &MachineNode{
		ID:          id,
		NodeType:    NewForEachNodeType(inputNode, bodyEntry, bodyExit),
		Description: &desc,
	}
}

// NewCollectNode creates a Collect node.
func NewCollectNode(id string, inputNodes []string) *MachineNode {
	desc := "Fan-in: collect results into vector"
	return &MachineNode{
		ID:          id,
		NodeType:    NewCollectNodeType(inputNodes, nil),
		Description: &desc,
	}
}

// NewWrapInListNode creates a WrapInList node.
func NewWrapInListNode(id, itemMediaUrn, listMediaUrn string) *MachineNode {
	desc := "WrapInList: wrap scalar in list-of-one"
	return &MachineNode{
		ID:          id,
		NodeType:    NewWrapInListNodeType(itemMediaUrn, listMediaUrn),
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
	EdgeKindDirect EdgeKind = iota
	EdgeKindJsonField
	EdgeKindJsonPath
	EdgeKindIteration
	EdgeKindCollection
)

// EdgeType holds the edge type and variant data.
type EdgeType struct {
	Kind  EdgeKind
	Field string // for JsonField
	Path  string // for JsonPath
}

// DirectEdgeType creates a Direct edge type.
func DirectEdgeType() *EdgeType { return &EdgeType{Kind: EdgeKindDirect} }

// IterationEdgeType creates an Iteration edge type.
func IterationEdgeType() *EdgeType { return &EdgeType{Kind: EdgeKindIteration} }

// CollectionEdgeType creates a Collection edge type.
func CollectionEdgeType() *EdgeType { return &EdgeType{Kind: EdgeKindCollection} }

// JsonFieldEdgeType creates a JsonField edge type.
func JsonFieldEdgeType(field string) *EdgeType {
	return &EdgeType{Kind: EdgeKindJsonField, Field: field}
}

// JsonPathEdgeType creates a JsonPath edge type.
func JsonPathEdgeType(path string) *EdgeType {
	return &EdgeType{Kind: EdgeKindJsonPath, Path: path}
}

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

// NewIterationEdge creates an iteration edge.
func NewIterationEdge(from, to string) *MachinePlanEdge {
	return &MachinePlanEdge{FromNode: from, ToNode: to, Type: IterationEdgeType()}
}

// NewCollectionEdge creates a collection edge.
func NewCollectionEdge(from, to string) *MachinePlanEdge {
	return &MachinePlanEdge{FromNode: from, ToNode: to, Type: CollectionEdgeType()}
}

// NewJsonFieldEdge creates a JSON field edge.
func NewJsonFieldEdge(from, to, field string) *MachinePlanEdge {
	return &MachinePlanEdge{FromNode: from, ToNode: to, Type: JsonFieldEdgeType(field)}
}

// NewJsonPathEdge creates a JSON path edge.
func NewJsonPathEdge(from, to, path string) *MachinePlanEdge {
	return &MachinePlanEdge{FromNode: from, ToNode: to, Type: JsonPathEdgeType(path)}
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

// Validate checks plan structure. Returns error on invalid references.
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
	return nil
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

// FindFirstForEach finds the first ForEach node in topological order.
func (p *MachinePlan) FindFirstForEach() *string {
	order, err := p.TopologicalOrder()
	if err != nil {
		return nil
	}
	for _, node := range order {
		if node.NodeType.Kind == NodeKindForEach {
			return &node.ID
		}
	}
	return nil
}

// HasForEachOrCollect returns true if any node is ForEach or Collect.
func (p *MachinePlan) HasForEachOrCollect() bool {
	for _, node := range p.Nodes {
		if node.NodeType.Kind == NodeKindForEach || node.NodeType.Kind == NodeKindCollect {
			return true
		}
	}
	return false
}

// ExtractPrefixTo extracts ancestor subgraph up to and including targetNodeID.
func (p *MachinePlan) ExtractPrefixTo(targetNodeID string) (*MachinePlan, error) {
	if _, ok := p.Nodes[targetNodeID]; !ok {
		return nil, NewInternalError(fmt.Sprintf("Target node '%s' not found in plan", targetNodeID))
	}

	// Build reverse adjacency
	reverseAdj := make(map[string][]string)
	for id := range p.Nodes {
		reverseAdj[id] = nil
	}
	for _, edge := range p.Edges {
		reverseAdj[edge.ToNode] = append(reverseAdj[edge.ToNode], edge.FromNode)
	}

	// BFS backward from target
	ancestors := make(map[string]bool)
	queue := []string{targetNodeID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if ancestors[id] {
			continue
		}
		ancestors[id] = true
		for _, pred := range reverseAdj[id] {
			if !ancestors[pred] {
				queue = append(queue, pred)
			}
		}
	}

	subPlan := NewMachinePlan(p.Name + "_prefix")
	for id := range ancestors {
		node := p.Nodes[id]
		if node.NodeType.Kind == NodeKindOutput {
			continue
		}
		subPlan.AddNode(node)
	}

	for _, edge := range p.Edges {
		if ancestors[edge.FromNode] && ancestors[edge.ToNode] {
			fromNode := p.Nodes[edge.FromNode]
			toNode := p.Nodes[edge.ToNode]
			if fromNode.NodeType.Kind != NodeKindOutput && toNode.NodeType.Kind != NodeKindOutput {
				subPlan.AddEdge(edge)
			}
		}
	}

	outputID := targetNodeID + "_prefix_output"
	subPlan.AddNode(NewOutputNode(outputID, "prefix_result", targetNodeID))
	subPlan.AddEdge(NewDirectEdge(targetNodeID, outputID))

	if err := subPlan.Validate(); err != nil {
		return nil, err
	}
	return subPlan, nil
}

// ExtractForEachBody extracts the body of a ForEach node as a standalone plan.
func (p *MachinePlan) ExtractForEachBody(foreachNodeID, itemMediaUrn string) (*MachinePlan, error) {
	node, ok := p.Nodes[foreachNodeID]
	if !ok {
		return nil, NewInternalError(fmt.Sprintf("ForEach node '%s' not found in plan", foreachNodeID))
	}
	if node.NodeType.Kind != NodeKindForEach {
		return nil, NewInternalError(fmt.Sprintf("Node '%s' is not a ForEach node", foreachNodeID))
	}

	bodyEntry := node.NodeType.BodyEntry
	bodyExit := node.NodeType.BodyExit

	// Build forward adjacency
	forwardAdj := make(map[string][]string)
	for id := range p.Nodes {
		forwardAdj[id] = nil
	}
	for _, edge := range p.Edges {
		forwardAdj[edge.FromNode] = append(forwardAdj[edge.FromNode], edge.ToNode)
	}

	bodyNodes := make(map[string]bool)
	queue := []string{bodyEntry}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if bodyNodes[id] {
			continue
		}
		bodyNodes[id] = true
		if id == bodyExit {
			continue
		}
		origNode := p.Nodes[id]
		if origNode != nil && (origNode.NodeType.Kind == NodeKindOutput || origNode.NodeType.Kind == NodeKindCollect) {
			continue
		}
		for _, succ := range forwardAdj[id] {
			if !bodyNodes[succ] {
				queue = append(queue, succ)
			}
		}
	}
	bodyNodes[bodyExit] = true

	bodyPlan := NewMachinePlan(p.Name + "_foreach_body")

	inputID := foreachNodeID + "_body_input"
	bodyPlan.AddNode(NewInputSlotNode(inputID, "item_input", itemMediaUrn, CardinalitySingle))

	for id := range bodyNodes {
		if bodyNode, ok := p.Nodes[id]; ok {
			bodyPlan.AddNode(bodyNode)
		}
	}

	bodyPlan.AddEdge(NewDirectEdge(inputID, bodyEntry))

	for _, edge := range p.Edges {
		if bodyNodes[edge.FromNode] && bodyNodes[edge.ToNode] {
			if edge.Type.Kind == EdgeKindIteration || edge.Type.Kind == EdgeKindCollection {
				continue
			}
			bodyPlan.AddEdge(edge)
		}
	}

	outputID := foreachNodeID + "_body_output"
	bodyPlan.AddNode(NewOutputNode(outputID, "item_result", bodyExit))
	bodyPlan.AddEdge(NewDirectEdge(bodyExit, outputID))

	if err := bodyPlan.Validate(); err != nil {
		return nil, err
	}
	return bodyPlan, nil
}

// ExtractSuffixFrom extracts all descendants of sourceNodeID as a standalone plan.
func (p *MachinePlan) ExtractSuffixFrom(sourceNodeID, sourceMediaUrn string) (*MachinePlan, error) {
	if _, ok := p.Nodes[sourceNodeID]; !ok {
		return nil, NewInternalError(fmt.Sprintf("Source node '%s' not found in plan", sourceNodeID))
	}

	// Build forward adjacency
	forwardAdj := make(map[string][]string)
	for id := range p.Nodes {
		forwardAdj[id] = nil
	}
	for _, edge := range p.Edges {
		forwardAdj[edge.FromNode] = append(forwardAdj[edge.FromNode], edge.ToNode)
	}

	descendants := make(map[string]bool)
	queue := []string{sourceNodeID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if descendants[id] {
			continue
		}
		descendants[id] = true
		for _, succ := range forwardAdj[id] {
			if !descendants[succ] {
				queue = append(queue, succ)
			}
		}
	}

	subPlan := NewMachinePlan(p.Name + "_suffix")

	inputID := sourceNodeID + "_suffix_input"
	subPlan.AddNode(NewInputSlotNode(inputID, "collected_input", sourceMediaUrn, CardinalitySingle))

	for id := range descendants {
		if id == sourceNodeID {
			continue
		}
		descNode := p.Nodes[id]
		if descNode != nil && descNode.NodeType.Kind != NodeKindInputSlot {
			subPlan.AddNode(descNode)
		}
	}

	for _, edge := range p.Edges {
		if edge.FromNode == sourceNodeID && descendants[edge.ToNode] {
			subPlan.AddEdge(NewDirectEdge(inputID, edge.ToNode))
		} else if descendants[edge.FromNode] && descendants[edge.ToNode] && edge.FromNode != sourceNodeID {
			subPlan.AddEdge(edge)
		}
	}

	if err := subPlan.Validate(); err != nil {
		return nil, err
	}
	return subPlan, nil
}

// NodeExecutionResult holds the result of executing a single node.
type NodeExecutionResult struct {
	NodeID       string `json:"node_id"`
	Success      bool   `json:"success"`
	BinaryOutput []byte `json:"-"`
	TextOutput   string `json:"text_output,omitempty"`
	Error        string `json:"error,omitempty"`
	DurationMs   uint64 `json:"duration_ms"`
}

// MachineResult holds the result of executing a complete plan.
type MachineResult struct {
	Success         bool                          `json:"success"`
	NodeResults     map[string]*NodeExecutionResult `json:"node_results"`
	Outputs         map[string]any                `json:"outputs"`
	Error           string                        `json:"error,omitempty"`
	TotalDurationMs uint64                        `json:"total_duration_ms"`
}

// PrimaryOutput returns the first output value (non-deterministic).
func (r *MachineResult) PrimaryOutput() any {
	for _, v := range r.Outputs {
		return v
	}
	return nil
}
