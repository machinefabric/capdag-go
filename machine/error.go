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
	// ErrAbstractionRuntimeMediaInference — a cap could not be applied to the
	// runtime input media flowing into it while realizing a strand.
	ErrAbstractionRuntimeMediaInference
	// ErrAbstractionCapDoesNotDeclareInput — no argument declares a stdin source
	// whose URN is the cap's in=.
	ErrAbstractionCapDoesNotDeclareInput
	// ErrAbstractionNoStdinBinding — the resolver's source-to-arg assignment has
	// no binding feeding the cap's stdin argument.
	ErrAbstractionNoStdinBinding
	// ErrAbstractionNonProducerSecondaryArg — a non-primary wiring source is not
	// another cap's output.
	ErrAbstractionNonProducerSecondaryArg
	// ErrAbstractionDisconnectedStrand — a strand's edges do not form a
	// connected data-flow graph.
	ErrAbstractionDisconnectedStrand
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

// runtimeMediaInferenceError reports that a cap could not be applied to the
// runtime input media flowing into it while realizing a strand — the declared
// input/output specs are incompatible with the concrete upstream media.
// Realization cannot invent a valid data type, so it fails hard rather than
// guessing.
func runtimeMediaInferenceError(strandIndex int, capUrn, runtimeInput, reason string) *MachineAbstractionError {
	idx := strandIndex
	return &MachineAbstractionError{
		Kind: ErrAbstractionRuntimeMediaInference,
		Message: fmt.Sprintf(
			"strand %d: cap '%s' cannot be applied to runtime input '%s': %s",
			strandIndex, capUrn, runtimeInput, reason,
		),
		StrandIndex: &idx,
	}
}

// capDoesNotDeclareInputError reports that a cap does not declare its input: no
// argument declares a Stdin source whose URN is the cap's in=. The main input
// is the value piped in on stdin, so the main arg always declares a stdin
// source carrying in= (its declared slot URN may differ — e.g. a file-path slot
// whose piped content is in=). A cap without such an arg cannot receive its
// input to thread the strand's runtime media.
func capDoesNotDeclareInputError(strandIndex int, capUrn string) *MachineAbstractionError {
	idx := strandIndex
	return &MachineAbstractionError{
		Kind: ErrAbstractionCapDoesNotDeclareInput,
		Message: fmt.Sprintf(
			"strand %d: cap '%s' does not declare its input (no argument declares a stdin source whose URN is its in=)",
			strandIndex, capUrn,
		),
		StrandIndex: &idx,
	}
}

// noStdinBindingError reports that the resolver's source-to-arg assignment for
// a cap edge has no binding feeding the cap's stdin argument. The primary
// (main-input) source is missing — the wiring cannot be realized into a
// data-flow step.
func noStdinBindingError(strandIndex int, capUrn, stdinArg string) *MachineAbstractionError {
	idx := strandIndex
	return &MachineAbstractionError{
		Kind: ErrAbstractionNoStdinBinding,
		Message: fmt.Sprintf(
			"strand %d: cap '%s' has no wiring source bound to its stdin argument '%s'",
			strandIndex, capUrn, stdinArg,
		),
		StrandIndex: &idx,
	}
}

// nonProducerSecondaryArgError reports that a non-primary (convergence) wiring
// source is NOT another cap's output. Only a cap output may be wired into a
// non-main argument; a raw input feeding a non-main argument is an argument
// VALUE (default / setting / config / user input), delivered through the
// argument value channel, never wired. Exposed hard rather than silently
// mis-routed.
func nonProducerSecondaryArgError(strandIndex int, capUrn, argUrn string) *MachineAbstractionError {
	idx := strandIndex
	return &MachineAbstractionError{
		Kind: ErrAbstractionNonProducerSecondaryArg,
		Message: fmt.Sprintf(
			"strand %d: cap '%s' arg '%s' is wired from a source that is not a cap output; "+
				"wire only cap outputs into non-main args, deliver everything else as an argument value",
			strandIndex, capUrn, argUrn,
		),
		StrandIndex: &idx,
	}
}

// disconnectedStrandError reports that a strand's edges do not form a
// data-flow graph whose every source is reachable (an unreachable edge, or a
// source whose producer never becomes available).
func disconnectedStrandError(strandIndex int) *MachineAbstractionError {
	idx := strandIndex
	return &MachineAbstractionError{
		Kind:        ErrAbstractionDisconnectedStrand,
		Message:     fmt.Sprintf("strand %d: edges do not form a connected data-flow graph", strandIndex),
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
