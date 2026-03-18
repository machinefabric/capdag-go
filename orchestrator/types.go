// Package orchestrator provides machine notation parsing, DAG validation,
// plan conversion, and CBOR utilities for cap execution orchestration.
//
// Mirrors Rust's orchestrator module.
package orchestrator

import (
	"fmt"
	"strings"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/planner"
)

// --- Error Types ---

// ParseOrchestrationErrorKind identifies the category of orchestration error.
type ParseOrchestrationErrorKind int

const (
	ErrMachineSyntaxParseFailed ParseOrchestrationErrorKind = iota
	ErrCapNotFound
	ErrNodeMediaConflict
	ErrNotADag
	ErrInvalidGraph
	ErrCapUrnParseError
	ErrMediaUrnParseError
	ErrOrchestratorRegistryError
	ErrStructureMismatch
)

// ParseOrchestrationError represents any error during orchestration parsing.
type ParseOrchestrationError struct {
	Kind               ParseOrchestrationErrorKind
	Message            string
	CapUrn             string
	Node               string
	Existing           string
	RequiredByCap      string
	CycleNodes         []string
	SourceStructure    planner.InputStructure
	ExpectedStructure  planner.InputStructure
}

func (e *ParseOrchestrationError) Error() string {
	switch e.Kind {
	case ErrMachineSyntaxParseFailed:
		return fmt.Sprintf("Route notation parse failed: %s", e.Message)
	case ErrCapNotFound:
		return fmt.Sprintf("Cap URN '%s' not found in registry", e.CapUrn)
	case ErrNodeMediaConflict:
		return fmt.Sprintf("Node '%s' has conflicting media URNs: existing='%s', required_by_cap='%s'",
			e.Node, e.Existing, e.RequiredByCap)
	case ErrNotADag:
		return fmt.Sprintf("Graph is not a DAG, contains cycle involving nodes: %v", e.CycleNodes)
	case ErrInvalidGraph:
		return fmt.Sprintf("Invalid graph: %s", e.Message)
	case ErrCapUrnParseError:
		return fmt.Sprintf("Failed to parse Cap URN: %s", e.Message)
	case ErrMediaUrnParseError:
		return fmt.Sprintf("Failed to parse Media URN: %s", e.Message)
	case ErrOrchestratorRegistryError:
		return fmt.Sprintf("Registry error: %s", e.Message)
	case ErrStructureMismatch:
		return fmt.Sprintf("Structure mismatch at node '%s': source is %v but cap expects %v",
			e.Node, e.SourceStructure, e.ExpectedStructure)
	default:
		return e.Message
	}
}

// Error constructors

func machineNotationParseFailedError(message string) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrMachineSyntaxParseFailed, Message: message}
}

func capNotFoundError(capUrn string) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrCapNotFound, CapUrn: capUrn}
}

func nodeMediaConflictError(node, existing, requiredByCap string) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrNodeMediaConflict, Node: node, Existing: existing, RequiredByCap: requiredByCap}
}

func notADagError(cycleNodes []string) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrNotADag, CycleNodes: cycleNodes}
}

func invalidGraphError(message string) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrInvalidGraph, Message: message}
}

func mediaUrnParseError(message string) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrMediaUrnParseError, Message: message}
}

func structureMismatchError(node string, source, expected planner.InputStructure) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrStructureMismatch, Node: node, SourceStructure: source, ExpectedStructure: expected}
}

// --- Data Types ---

// ResolvedEdge is a resolved edge in the orchestration graph.
// Each edge represents a cap transformation from one node to another.
type ResolvedEdge struct {
	From     string
	To       string
	CapUrn   string
	Cap      *cap.Cap
	InMedia  string
	OutMedia string
}

// ResolvedGraph is a fully resolved orchestration graph.
// Contains nodes (name → media URN) and edges (resolved cap transformations).
type ResolvedGraph struct {
	Nodes     map[string]string
	Edges     []*ResolvedEdge
	GraphName *string
}

// ToMermaid generates Mermaid graph LR flowchart code from this resolved graph.
func (g *ResolvedGraph) ToMermaid() string {
	var out strings.Builder
	out.WriteString("graph LR\n")

	targets := make(map[string]bool)
	sources := make(map[string]bool)
	for _, edge := range g.Edges {
		sources[edge.From] = true
		targets[edge.To] = true
	}

	for name, mediaUrn := range g.Nodes {
		isInput := sources[name] && !targets[name]
		isOutput := targets[name] && !sources[name]
		escName := mermaidEscape(name)
		escUrn := mermaidEscape(mediaUrn)

		if isInput {
			fmt.Fprintf(&out, "    %s([\"%s<br/><small>%s</small>\"])\n", name, escName, escUrn)
		} else if isOutput {
			fmt.Fprintf(&out, "    %s((\"%s<br/><small>%s</small>\"))\n", name, escName, escUrn)
		} else {
			fmt.Fprintf(&out, "    %s[\"%s<br/><small>%s</small>\"]\n", name, escName, escUrn)
		}
	}

	out.WriteString("\n")

	type edgeKey struct{ from, to, capUrn string }
	seen := make(map[edgeKey]bool)
	for _, edge := range g.Edges {
		key := edgeKey{edge.From, edge.To, edge.CapUrn}
		if seen[key] {
			continue
		}
		seen[key] = true
		title := mermaidEscape(edge.Cap.Title)
		urn := mermaidEscape(edge.CapUrn)
		fmt.Fprintf(&out, "    %s -->|\"%s<br/><small>%s</small>\"| %s\n", edge.From, title, urn, edge.To)
	}

	return out.String()
}

func mermaidEscape(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "#quot;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

// --- Cap Registry Interface ---

// CapRegistryTrait is the interface for cap registry lookup.
// Implementations provide lookup of caps by URN string.
type CapRegistryTrait interface {
	Lookup(urn string) (*cap.Cap, error)
}
