// Package machine implements machine notation — compact, round-trippable DAG path identifiers.
//
// Machine notation replaces the DOT file format for describing capability
// transformation paths. It provides a typed graph model (Machine, MachineStrand, MachineEdge)
// with semantic equivalence, a compact textual format, and conversion from
// resolved paths.
package machine

import (
	"fmt"
	"strings"
)

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
	// Members are the members to name, each with what it could be for
	// (ErrAbstractionAmbiguousMachineNotation,
	// ErrAbstractionAmbiguousMachineMembers).
	Members []UndecidedMember
	// Chain is every machine on the way round an inclusion that closes on
	// itself, from the first to the one that closes the circle
	// (ErrAbstractionCyclicMachineReference).
	Chain []string
	// Cause is why an included machine is not a valid machine
	// (ErrAbstractionInvalidIncludedMachine).
	Cause *MachineParseError
}

func (e *MachineAbstractionError) Error() string {
	return e.Message
}

// UndecidedMember is a member of a wiring whose arg the types alone do not
// decide.
type UndecidedMember struct {
	// Node is the node, as the notation named it. Empty where no notation is
	// at hand (a strand resolved from a planner path): the member is then
	// known by its type alone.
	Node string
	// SourceUrn is the media URN the member holds.
	SourceUrn string
	// CouldBe is the args — or, for a machine in step position, the inputs —
	// it could go to, by name, in the order the definition declares them.
	CouldBe []string
}

func (m UndecidedMember) String() string {
	who := fmt.Sprintf("a member of type '%s'", m.SourceUrn)
	if m.Node != "" {
		who = fmt.Sprintf("'%s' (%s)", m.Node, m.SourceUrn)
	}
	return who + " could be " + strings.Join(m.CouldBe, " or ")
}

func undecidedList(members []UndecidedMember) string {
	said := make([]string, len(members))
	for i, member := range members {
		said[i] = member.String()
	}
	return strings.Join(said, "; ")
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
	// ErrAbstractionAmbiguousMachineNotation — the types do not say which arg
	// some unnamed member is for.
	ErrAbstractionAmbiguousMachineNotation
	// ErrAbstractionCyclicMachineStrand — resolved data-flow graph contains a cycle.
	ErrAbstractionCyclicMachineStrand
	// ErrAbstractionRuntimeMediaInference — a cap could not be applied to the
	// runtime input media flowing into it while realizing a strand.
	ErrAbstractionRuntimeMediaInference
	// ErrAbstractionCapDoesNotDeclareInput — no argument declares a stdin source
	// whose URN is the cap's in=.
	ErrAbstractionCapDoesNotDeclareInput
	// ErrAbstractionDisconnectedStrand — a strand's edges do not form a
	// connected data-flow graph.
	ErrAbstractionDisconnectedStrand
	// ErrAbstractionUnknownArgName — a member names an arg the step's cap does
	// not declare.
	ErrAbstractionUnknownArgName
	// ErrAbstractionUnknownOutputName — a target names an output the step's cap
	// does not declare.
	ErrAbstractionUnknownOutputName
	// ErrAbstractionUnknownStrandOutput — a strand step takes, by media URN, an
	// output its cap does not declare as another output.
	ErrAbstractionUnknownStrandOutput
	// ErrAbstractionSourceDoesNotConformToArg — a named member's URN does not
	// conform to the type that arg accepts.
	ErrAbstractionSourceDoesNotConformToArg
	// ErrAbstractionConstantFailsValidation — a constant does not satisfy the
	// validation of the arg it is bound to.
	ErrAbstractionConstantFailsValidation
	// ErrAbstractionArgBoundTwice — an arg that takes one value is named by
	// two members.
	ErrAbstractionArgBoundTwice
	// ErrAbstractionMainInputUnbound — a step whose cap takes an input is
	// given no member for it.
	ErrAbstractionMainInputUnbound
	// ErrAbstractionOutputTakenTwice — two targets of one wiring take the same
	// output.
	ErrAbstractionOutputTakenTwice
	// ErrAbstractionNoMainOutput — an unnamed target takes the main output of
	// a step whose cap has none.
	ErrAbstractionNoMainOutput
	// ErrAbstractionUntypedMachineInput — a wiring holds an unnamed member
	// that nothing produces and whose arg nothing decides.
	ErrAbstractionUntypedMachineInput
	// ErrAbstractionUnknownMachineReference — a machine header's reference
	// does not resolve.
	ErrAbstractionUnknownMachineReference
	// ErrAbstractionCyclicMachineReference — a machine includes itself.
	ErrAbstractionCyclicMachineReference
	// ErrAbstractionInvalidIncludedMachine — a machine named in a header is
	// not itself a valid machine.
	ErrAbstractionInvalidIncludedMachine
	// ErrAbstractionPolicyOnMachine — a policy is written on a step that is
	// another machine.
	ErrAbstractionPolicyOnMachine
	// ErrAbstractionUnknownMachineInput — a member names an input the machine
	// in step position does not have.
	ErrAbstractionUnknownMachineInput
	// ErrAbstractionUnknownMachineOutput — a target names an output the
	// machine in step position does not have.
	ErrAbstractionUnknownMachineOutput
	// ErrAbstractionMachineInputBinding — an input of a machine in step
	// position is not bound by exactly one member.
	ErrAbstractionMachineInputBinding
	// ErrAbstractionMachineOutputTakenTwice — two targets of one wiring take
	// the same output of a machine.
	ErrAbstractionMachineOutputTakenTwice
	// ErrAbstractionUnnamedMachineTarget — an unnamed target of a machine
	// step, where the other targets leave more than one output, or none.
	ErrAbstractionUnnamedMachineTarget
	// ErrAbstractionUntypedMachineMember — an unnamed member of a machine step
	// that nothing gives a type.
	ErrAbstractionUntypedMachineMember
	// ErrAbstractionUnmatchedMachineMember — an unnamed member of a machine
	// step conforms to no input left for it.
	ErrAbstractionUnmatchedMachineMember
	// ErrAbstractionAmbiguousMachineMembers — the unnamed members of a machine
	// step fit its inputs in more than one way.
	ErrAbstractionAmbiguousMachineMembers
	// ErrAbstractionSourceDoesNotConformToMachineInput — a named member's URN
	// does not conform to the type of the machine input it names.
	ErrAbstractionSourceDoesNotConformToMachineInput
	// ErrAbstractionNodeMediaConflict — the wirings that touch a node give it
	// URNs that are not on one specialization chain.
	ErrAbstractionNodeMediaConflict
	// ErrAbstractionUncachedMediaDef — a node's media has no cached
	// definition, so the render payload has no title for it.
	ErrAbstractionUncachedMediaDef
	// ErrAbstractionUncachedCap — a step's cap has no cached definition, so
	// the render payload has no title for it.
	ErrAbstractionUncachedCap
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

func ambiguousNotationError(strandIndex int, capUrn string, members []UndecidedMember) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind: ErrAbstractionAmbiguousMachineNotation,
		Message: fmt.Sprintf(
			"in strand %d, cap '%s': the types do not say which arg these members are for — name them: %s",
			strandIndex, capUrn, undecidedList(members),
		),
		Members: members,
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

func inStrand(kind AbstractionErrorKind, strandIndex int, format string, args ...any) *MachineAbstractionError {
	idx := strandIndex
	return &MachineAbstractionError{
		Kind:        kind,
		Message:     fmt.Sprintf("strand %d: ", strandIndex) + fmt.Sprintf(format, args...),
		StrandIndex: &idx,
	}
}

func atStatement(kind AbstractionErrorKind, position int, format string, args ...any) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind:    kind,
		Message: fmt.Sprintf("statement %d: ", position) + fmt.Sprintf(format, args...),
	}
}

func unknownArgNameError(strandIndex int, capUrn, name string, declared []string) *MachineAbstractionError {
	return inStrand(ErrAbstractionUnknownArgName, strandIndex,
		"cap '%s' declares no arg named '%s' (it declares: %s)", capUrn, name, strings.Join(declared, ", "))
}

func unknownOutputNameError(strandIndex int, capUrn, name string, declared []string) *MachineAbstractionError {
	return inStrand(ErrAbstractionUnknownOutputName, strandIndex,
		"cap '%s' declares no output named '%s' (it declares: %s)", capUrn, name, strings.Join(declared, ", "))
}

func unknownStrandOutputError(strandIndex int, capUrn, outputUrn string, declared []string) *MachineAbstractionError {
	return inStrand(ErrAbstractionUnknownStrandOutput, strandIndex,
		"cap '%s' declares no other output '%s' (it declares: %s)", capUrn, outputUrn, strings.Join(declared, ", "))
}

func sourceDoesNotConformToArgError(strandIndex int, capUrn, name, accepts, sourceUrn string) *MachineAbstractionError {
	return inStrand(ErrAbstractionSourceDoesNotConformToArg, strandIndex,
		"cap '%s' arg '%s' accepts '%s', and its source is '%s'", capUrn, name, accepts, sourceUrn)
}

func constantFailsValidationError(strandIndex int, capUrn, name, constant, reason string) *MachineAbstractionError {
	return inStrand(ErrAbstractionConstantFailsValidation, strandIndex,
		"cap '%s' arg '%s' refuses the constant %s: %s", capUrn, name, constant, reason)
}

func argBoundTwiceError(strandIndex int, capUrn, name string, count int) *MachineAbstractionError {
	return inStrand(ErrAbstractionArgBoundTwice, strandIndex,
		"cap '%s' arg '%s' takes one value and is named by %d members", capUrn, name, count)
}

func mainInputUnboundError(strandIndex int, capUrn, mainInput string) *MachineAbstractionError {
	return inStrand(ErrAbstractionMainInputUnbound, strandIndex,
		"cap '%s' takes an input ('%s') and no member of the wiring is bound to it", capUrn, mainInput)
}

func outputTakenTwiceError(strandIndex int, capUrn, output string) *MachineAbstractionError {
	return inStrand(ErrAbstractionOutputTakenTwice, strandIndex,
		"cap '%s' output '%s' is taken twice in one wiring", capUrn, output)
}

func noMainOutputError(strandIndex int, capUrn string) *MachineAbstractionError {
	return inStrand(ErrAbstractionNoMainOutput, strandIndex,
		"cap '%s' has no main output to take (its out= is media:void)", capUrn)
}

func untypedMachineInputError(position int, capUrn string, members []string) *MachineAbstractionError {
	return atStatement(ErrAbstractionUntypedMachineInput, position,
		"cap '%s' is given unnamed machine inputs whose arg nothing decides (%s) — name them", capUrn, strings.Join(members, ", "))
}

func unknownMachineReferenceError(alias, reference, details string) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind:    ErrAbstractionUnknownMachineReference,
		Message: fmt.Sprintf("machine header '%s' refers to \"%s\", which does not resolve: %s", alias, reference, details),
	}
}

func cyclicMachineReferenceError(chain []string) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind:    ErrAbstractionCyclicMachineReference,
		Message: "a machine includes itself: " + strings.Join(chain, " -> "),
		Chain:   chain,
	}
}

func invalidIncludedMachineError(alias, reference string, cause *MachineParseError) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind:    ErrAbstractionInvalidIncludedMachine,
		Message: fmt.Sprintf("machine '%s' (\"%s\") is not a valid machine: %s", alias, reference, cause.Error()),
		Cause:   cause,
	}
}

func policyOnMachineError(position int, alias string) *MachineAbstractionError {
	return atStatement(ErrAbstractionPolicyOnMachine, position,
		"'%s' is a machine, and a policy is written on it — write it on the steps inside", alias)
}

func unknownMachineInputError(position int, alias, name string, declared []string) *MachineAbstractionError {
	return atStatement(ErrAbstractionUnknownMachineInput, position,
		"machine '%s' has no input named '%s' (its inputs: %s)", alias, name, strings.Join(declared, ", "))
}

func unknownMachineOutputError(position int, alias, name string, declared []string) *MachineAbstractionError {
	return atStatement(ErrAbstractionUnknownMachineOutput, position,
		"machine '%s' has no output named '%s' (its outputs: %s)", alias, name, strings.Join(declared, ", "))
}

func machineInputBindingError(position int, alias, input string, count int) *MachineAbstractionError {
	return atStatement(ErrAbstractionMachineInputBinding, position,
		"input '%s' of machine '%s' is given %d members — every input of a machine takes exactly one", input, alias, count)
}

func machineOutputTakenTwiceError(position int, alias, output string) *MachineAbstractionError {
	return atStatement(ErrAbstractionMachineOutputTakenTwice, position,
		"output '%s' of machine '%s' is taken twice in one wiring", output, alias)
}

func unnamedMachineTargetError(position int, alias string, left []string) *MachineAbstractionError {
	return atStatement(ErrAbstractionUnnamedMachineTarget, position,
		"an unnamed target of machine '%s' has %d output(s) left to take (%s) — name it", alias, len(left), strings.Join(left, ", "))
}

func untypedMachineMemberError(position int, alias string, members []string) *MachineAbstractionError {
	return atStatement(ErrAbstractionUntypedMachineMember, position,
		"machine '%s' is given unnamed members that have no type to be matched by (%s) — name them", alias, strings.Join(members, ", "))
}

func unmatchedMachineMemberError(position int, alias, sourceUrn string) *MachineAbstractionError {
	return atStatement(ErrAbstractionUnmatchedMachineMember, position,
		"a member of type '%s' fits no input left of machine '%s'", sourceUrn, alias)
}

func ambiguousMachineMembersError(position int, alias string, members []UndecidedMember) *MachineAbstractionError {
	err := atStatement(ErrAbstractionAmbiguousMachineMembers, position,
		"the types do not say which input of machine '%s' these members are for — name them: %s", alias, undecidedList(members))
	err.Members = members
	return err
}

func sourceDoesNotConformToMachineInputError(position int, alias, name, accepts, sourceUrn string) *MachineAbstractionError {
	return atStatement(ErrAbstractionSourceDoesNotConformToMachineInput, position,
		"input '%s' of machine '%s' holds '%s', and its source is '%s'", name, alias, accepts, sourceUrn)
}

func uncachedMediaDefError(mediaUrn string) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind:    ErrAbstractionUncachedMediaDef,
		Message: fmt.Sprintf("media URN '%s' has no cached spec — cannot emit a display title for render payload", mediaUrn),
	}
}

func uncachedCapError(capUrn string) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind:    ErrAbstractionUncachedCap,
		Message: fmt.Sprintf("cap URN '%s' has no cached definition — cannot emit a display title for render payload", capUrn),
	}
}

func nodeMediaConflictError(node, first, second string) *MachineAbstractionError {
	return &MachineAbstractionError{
		Kind: ErrAbstractionNodeMediaConflict,
		Message: fmt.Sprintf(
			"node '%s' is given media URNs that are not on one specialization chain: '%s' and '%s'",
			node, first, second),
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
	// ErrInvalidCapUrn — a cap URN in a header statement failed to parse.
	ErrInvalidCapUrn
	// ErrUndefinedAlias — a wiring references an alias never defined in a header.
	ErrUndefinedAlias
	// ErrDuplicateAlias — two header statements define the same alias.
	ErrDuplicateAlias
	// ErrInvalidMediaUrn — a media URN referenced in a header failed to parse.
	ErrInvalidMediaUrn
	// ErrNoEdges — headers defined but no wirings.
	ErrNoEdges
	// ErrNodeAliasCollision — a node name collides with a cap alias.
	ErrNodeAliasCollision
	// ErrParse — PEG parse error from the grammar.
	ErrParse
	// ErrAliasNotACap — a cap-position name resolved to a fabric alias whose
	// target is a media URN, not a cap.
	ErrAliasNotACap
	// ErrMachineReferenceWithoutSource — a machine header names another
	// machine, and the notation is read with nothing to resolve it against.
	ErrMachineReferenceWithoutSource
	// ErrUnnamedConstant — a constant appears as a member without a name.
	ErrUnnamedConstant
	// ErrReservedNodeName — a node is named `true`, `false` or `null`.
	ErrReservedNodeName
	// ErrSeveralUnnamedTargets — more than one target of a wiring is unnamed.
	ErrSeveralUnnamedTargets
	// ErrInvalidPolicy — a step policy has an unknown key, a repeated key, or
	// a value outside the table.
	ErrInvalidPolicy
)

func emptyError() *MachineSyntaxError {
	return &MachineSyntaxError{Kind: ErrEmpty, Message: "machine notation is empty"}
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

func machineReferenceWithoutSourceError(alias, reference string) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind: ErrMachineReferenceWithoutSource,
		Message: fmt.Sprintf(
			"machine header '%s' refers to \"%s\", and no machine source was given to resolve it",
			alias, reference),
	}
}

func unnamedConstantError(constant string, position int) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind: ErrUnnamedConstant,
		Message: fmt.Sprintf(
			"constant %s in statement %d names no arg — write it as name=%s", constant, position, constant),
	}
}

func reservedNodeNameError(name string, position int) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrReservedNodeName,
		Message: fmt.Sprintf("'%s' in statement %d is a constant, not a node name", name, position),
	}
}

func severalUnnamedTargetsError(position int, targets []string) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind: ErrSeveralUnnamedTargets,
		Message: fmt.Sprintf(
			"statement %d has several unnamed targets (%s) — only the main output is taken without a name",
			position, strings.Join(targets, ", ")),
	}
}

func invalidPolicyError(step string, position int, details string) *MachineSyntaxError {
	return &MachineSyntaxError{
		Kind:    ErrInvalidPolicy,
		Message: fmt.Sprintf("invalid policy on step '%s' in statement %d: %s", step, position, details),
	}
}
