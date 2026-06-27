// Package machine implements machine notation — compact, round-trippable DAG path identifiers.
//
// Machine notation replaces the DOT file format for describing capability
// transformation paths. It provides a typed graph model (Machine, MachineStrand, MachineEdge)
// with semantic equivalence, a compact textual format, and conversion from
// resolved paths.
package machine

import "fmt"

// MachineAbstractionError covers anchor-realization failures during resolution.
//
// These are distinct from MachineSyntaxError, which covers lexical/grammatical
// failures of the notation parser. Resolution-level failures (cap not in
// registry, ambiguous matching, cyclic strand) are reported here.
type MachineAbstractionError struct {
	Kind    AbstractionErrorKind
	Message string
	// StrandIndex carries the strand index for ErrAbstractionCyclicMachineStrand,
	// mirroring Rust's CyclicMachineStrand { strand_index }. nil for other kinds.
	StrandIndex *int
}

func (e *MachineAbstractionError) Error() string {
	return e.Message
}

// AbstractionErrorKind identifies the category of abstraction error.
type AbstractionErrorKind int

const (
	// ErrAbstractionNoCapabilitySteps — strand or wiring set contains no Cap step.
	ErrAbstractionNoCapabilitySteps AbstractionErrorKind = iota
	// ErrAbstractionUnknownCap — cap URN not in registry cache.
	ErrAbstractionUnknownCap
	// ErrAbstractionUnmatchedSourceInCapArgs — source URN doesn't conform to any cap input arg.
	ErrAbstractionUnmatchedSourceInCapArgs
	// ErrAbstractionAmbiguousMachineNotation — multiple minimum-cost matchings exist.
	ErrAbstractionAmbiguousMachineNotation
	// ErrAbstractionCyclicMachineStrand — resolved data-flow graph contains a cycle.
	ErrAbstractionCyclicMachineStrand
)

func noCapabilityStepsError() *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind:    ErrAbstractionNoCapabilitySteps,
		Message: "strand or wiring set contains no capability steps",
	}
}

func unknownCapError(capUrn string) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind:    ErrAbstractionUnknownCap,
		Message: fmt.Sprintf("cap URN '%s' is not in the cap registry cache", capUrn),
	}
}

func unmatchedSourceError(strandIndex int, capUrn, sourceUrn string) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind: ErrAbstractionUnmatchedSourceInCapArgs,
		Message: fmt.Sprintf(
			"in strand %d, cap '%s': source URN '%s' does not conform to any of the cap's input arguments",
			strandIndex, capUrn, sourceUrn,
		),
	}
}

func ambiguousNotationError(strandIndex int, capUrn string) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind: ErrAbstractionAmbiguousMachineNotation,
		Message: fmt.Sprintf(
			"in strand %d, cap '%s': source-to-cap-arg assignment is ambiguous (multiple minimum-cost matchings exist)",
			strandIndex, capUrn,
		),
	}
}

func cyclicStrandError(strandIndex int) *MachineAbstractionError {
	idx := strandIndex
	return &MachineAbstractionError{
		Kind:        ErrAbstractionCyclicMachineStrand,
		Message:     fmt.Sprintf("strand %d: resolved data-flow graph contains a cycle", strandIndex),
		StrandIndex: &idx,
	}
}

// MachineParseError is the combined error type returned from ParseMachine and
// Machine.FromString. Notation parsing has two phases: lexical/grammatical
// (MachineSyntaxError) and resolution (MachineAbstractionError).
type MachineParseError struct {
	Syntax      *MachineSyntaxError
	Abstraction *MachineAbstractionError
}

func (e *MachineParseError) Error() string {
	if e.Syntax != nil {
		return e.Syntax.Error()
	}
	return e.Abstraction.Error()
}

func syntaxParseError(err *MachineSyntaxError) *MachineParseError {
	return &MachineParseError{Syntax: err}
}

func abstractionParseError(err *MachineAbstractionError) *MachineParseError {
	return &MachineParseError{Abstraction: err}
}

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
	// ErrAliasNotACap — a cap-position name resolved to a fabric alias whose
	// target is a media URN, not a cap.
	ErrAliasNotACap
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
		Kind: ErrUndefinedAlias,
		Message: fmt.Sprintf(
			"wiring references undefined alias '%s' (not a local header and not a registered cap alias)",
			alias),
	}
}

func aliasNotACapError(alias, target string) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind: ErrAliasNotACap,
		Message: fmt.Sprintf(
			"alias '%s' in cap position resolves to a media URN ('%s'), but a cap is required there",
			alias, target),
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
		Message: "machine has headers but no wirings — define at least one edge",
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
