// Package route implements machine notation — compact, round-trippable DAG path identifiers.
//
// Route notation replaces the DOT file format for describing capability
// transformation paths. It provides a typed graph model (Machine, MachineEdge)
// with semantic equivalence, a compact textual format, and conversion from
// resolved paths.
package route

import "fmt"

// MachineSyntaxError represents errors during machine notation parsing.
type MachineSyntaxError struct {
	Kind    ErrorKind
	Message string
}

func (e *MachineSyntaxError) Error() string {
	return e.Message
}

// ErrorKind identifies the category of machine notation error.
type ErrorKind int

const (
	// ErrEmpty — input string is empty or contains only whitespace.
	ErrEmpty ErrorKind = iota
	// ErrUnterminatedStatement — a bracket '[' was opened but never closed.
	ErrUnterminatedStatement
	// ErrInvalidCapUrn — a cap URN in a header statement failed to parse.
	ErrInvalidCapUrn
	// ErrUndefinedAlias — a wiring references an alias never defined in a header.
	ErrUndefinedAlias
	// ErrDuplicateAlias — two header statements define the same alias.
	ErrDuplicateAlias
	// ErrInvalidWiring — a wiring has invalid structure or conflicting media types.
	ErrInvalidWiring
	// ErrInvalidMediaUrn — a media URN referenced in a header failed to parse.
	ErrInvalidMediaUrn
	// ErrInvalidHeader — a header statement has invalid structure.
	ErrInvalidHeader
	// ErrNoEdges — headers defined but no wirings.
	ErrNoEdges
	// ErrNodeAliasCollision — a node name collides with a cap alias.
	ErrNodeAliasCollision
	// ErrParse — PEG parse error from the grammar.
	ErrParse
)

func emptyError() *MachineSyntaxError {
	return &MachineSyntaxError{Kind: ErrEmpty, Message: "machine notation is empty"}
}

func unterminatedStatementError(position int) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrUnterminatedStatement,
		Message: fmt.Sprintf("unterminated statement starting at byte %d", position),
	}
}

func invalidCapUrnError(alias, details string) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrInvalidCapUrn,
		Message: fmt.Sprintf("invalid cap URN in header '%s': %s", alias, details),
	}
}

func undefinedAliasError(alias string) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrUndefinedAlias,
		Message: fmt.Sprintf("wiring references undefined alias '%s'", alias),
	}
}

func duplicateAliasError(alias string, firstPosition int) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrDuplicateAlias,
		Message: fmt.Sprintf("duplicate alias '%s' (first defined at statement %d)", alias, firstPosition),
	}
}

func invalidWiringError(position int, details string) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrInvalidWiring,
		Message: fmt.Sprintf("invalid wiring at statement %d: %s", position, details),
	}
}

func invalidMediaUrnError(alias, details string) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrInvalidMediaUrn,
		Message: fmt.Sprintf("invalid media URN in cap '%s': %s", alias, details),
	}
}

func noEdgesError() *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrNoEdges,
		Message: "route has headers but no wirings — define at least one edge",
	}
}

func nodeAliasCollisionError(name, alias string) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrNodeAliasCollision,
		Message: fmt.Sprintf("node name '%s' collides with cap alias '%s'", name, alias),
	}
}

func parseError(details string) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrParse,
		Message: fmt.Sprintf("parse error: %s", details),
	}
}
