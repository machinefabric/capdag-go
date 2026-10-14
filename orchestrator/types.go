// Package orchestrator turns the steps of a machine plan into the graph an
// executor runs, and holds the CBOR utilities cap execution shares.
//
// Mirrors Rust's orchestrator module.
package orchestrator

import (
	"fmt"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/planner"
)

// --- Error Types ---

// ParseOrchestrationErrorKind identifies the category of orchestration error.
type ParseOrchestrationErrorKind int

const (
	// ErrCapNotFound — a cap URN is not in the registry.
	ErrCapNotFound ParseOrchestrationErrorKind = iota
	// ErrInvalidGraph — the plan or graph is not one that can be run as asked.
	ErrInvalidGraph
)

// ParseOrchestrationError represents any error during orchestration.
type ParseOrchestrationError struct {
	Kind    ParseOrchestrationErrorKind
	Message string
	CapUrn  string
}

func (e *ParseOrchestrationError) Error() string {
	switch e.Kind {
	case ErrCapNotFound:
		return fmt.Sprintf("Cap URN '%s' not found in registry", e.CapUrn)
	case ErrInvalidGraph:
		return fmt.Sprintf("Invalid graph: %s", e.Message)
	default:
		return e.Message
	}
}

// Error constructors

func capNotFoundError(capUrn string) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrCapNotFound, CapUrn: capUrn}
}

func invalidGraphError(message string) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrInvalidGraph, Message: message}
}

// --- Data Types ---

// StepSource is what one input of a step is sent: one node's value as it is,
// or several nodes' values gathered, in order, into one sequence — a single
// value contributes one item, a sequence its items. A gather with no members
// is an empty sequence.
type StepSource struct {
	// IsGather says which of the two this is.
	IsGather bool
	// Node is the node whose value is sent, when this is not a gather.
	Node string
	// Members are the gathered nodes, in order, when this is one.
	Members []string
}

// NodeStepSource is the source that sends one node's value as it is.
func NodeStepSource(node string) StepSource {
	return StepSource{Node: node}
}

// GatherStepSource is the source that gathers `members`, in order.
func GatherStepSource(members []string) StepSource {
	return StepSource{IsGather: true, Members: members}
}

// Nodes returns every node this source reads, in order.
func (s StepSource) Nodes() []string {
	if s.IsGather {
		return s.Members
	}
	return []string{s.Node}
}

// StepInput is one input stream of a step.
type StepInput struct {
	// StreamUrn is the URN the stream is delivered under: the cap's `in=`
	// for its main input, the arg's stream URN for any other. A cartridge
	// tells its inputs apart by this.
	StreamUrn string
	Source    StepSource
}

// StepOutput is an output of a step other than its main one that something
// takes.
type StepOutput struct {
	// OutputMediaUrn is the output's media URN in the cap's definition: what
	// its response stream names.
	OutputMediaUrn string
	// Node is the node its value is stored at.
	Node string
}

// ResolvedStep is one step of a resolved graph: one invocation of a cap.
type ResolvedStep struct {
	// TokenId is the stable per-step identity carried from the originating
	// strand step (via the plan's Cap node). The key by which a run's live
	// progress maps back to the graph element a client rendered.
	TokenId planner.StepToken
	// Node is the step's node: where its MAIN output is stored.
	Node string
	// CapUrn is the cap URN string.
	CapUrn string
	// Cap is the resolved cap definition.
	Cap *cap.Cap
	// Inputs are the streams the step is sent, besides the values its args
	// are given by the run. None for a step nothing is wired into.
	Inputs []StepInput
	// Outputs are the outputs other than the main one that something takes.
	Outputs []StepOutput
	// OutMedia is the media URN of the main output, from the cap definition.
	OutMedia string
	// Policy is what the step does when it fails.
	Policy planner.StepPolicy
}

// SourceNodes returns every node the step reads, in input order.
func (s *ResolvedStep) SourceNodes() []string {
	var nodes []string
	for _, input := range s.Inputs {
		nodes = append(nodes, input.Source.Nodes()...)
	}
	return nodes
}

// ProducedNodes returns every node the step produces: its own, then its other
// taken outputs'.
func (s *ResolvedStep) ProducedNodes() []string {
	nodes := []string{s.Node}
	for _, output := range s.Outputs {
		nodes = append(nodes, output.Node)
	}
	return nodes
}

// ResolvedGraph is steps with the cap definitions they run, wired by the
// nodes they read and produce. A node no step of the graph produces is an
// input of the graph: its value is supplied when the graph is run.
type ResolvedGraph struct {
	// Nodes maps a node name to the media URN it holds.
	Nodes map[string]string
	// Steps are the steps, in the order they were asked for.
	Steps []*ResolvedStep
	// GraphName is the name of the plan the graph came from.
	GraphName *string
}
