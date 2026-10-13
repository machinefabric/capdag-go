// Package orchestrator provides machine notation parsing, DAG validation,
// plan conversion, and CBOR utilities for cap execution orchestration.
//
// Mirrors Rust's orchestrator module.
package orchestrator

import (
	"fmt"
	"strings"

	"github.com/machinefabric/capdag-go/cap"
)

// --- Error Types ---

// ParseOrchestrationErrorKind identifies the category of orchestration error.
type ParseOrchestrationErrorKind int

const (
	// ErrCapNotFound — a cap URN is not in the registry.
	ErrCapNotFound ParseOrchestrationErrorKind = iota
	// ErrNotADag — the graph contains a cycle.
	ErrNotADag
	// ErrInvalidGraph — the plan or graph is not one that can be run as asked.
	ErrInvalidGraph
)

// ParseOrchestrationError represents any error during orchestration parsing.
type ParseOrchestrationError struct {
	Kind       ParseOrchestrationErrorKind
	Message    string
	CapUrn     string
	CycleNodes []string
}

func (e *ParseOrchestrationError) Error() string {
	switch e.Kind {
	case ErrCapNotFound:
		return fmt.Sprintf("Cap URN '%s' not found in registry", e.CapUrn)
	case ErrNotADag:
		return fmt.Sprintf("Graph is not a DAG, contains cycle involving nodes: %v", e.CycleNodes)
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

func notADagError(cycleNodes []string) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrNotADag, CycleNodes: cycleNodes}
}

func invalidGraphError(message string) *ParseOrchestrationError {
	return &ParseOrchestrationError{Kind: ErrInvalidGraph, Message: message}
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

// FabricRegistryTrait is gone. The orchestrator parser and plan converter take
// *fabric.FabricRegistry directly. Tests use FabricRegistry.NewForTest() +
// AddCapsToCache(...) instead of a hand-built mock.
