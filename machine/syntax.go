package machine

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	peg "github.com/yhirose/go-peg"
)

// The syntax of machine notation: one walk of the parse tree into a list of
// statements. This is the only reader of the grammar's rules. The resolver's
// parser (parser.go) consumes the statements for what they say. Nothing here
// knows a cap definition: a name is a name, a node is a node, and whether
// either exists is the resolver's question.
//
// What IS decided here is everything the text alone decides
// (capdag/docs/09-machine-notation.md §Statement kinds):
//
//   - a constant is always named (ErrUnnamedConstant);
//   - `true`, `false` and `null` are constants, never node names
//     (ErrReservedNodeName);
//   - at most one target of a wiring is unnamed (ErrSeveralUnnamedTargets);
//   - a step policy holds known keys, once each, with values from the table
//     (ErrInvalidPolicy).

// goPegGrammar is the go-peg equivalent of machine.pest, which is shipped
// beside this file as the grammar of record.
//
// go-peg uses ← for rule definition, < > for a token, and %whitespace for the
// implicit whitespace pest inserts between the operands of a non-atomic rule.
//
// Two statement forms, freely mixed:
//
//	bracketed   [alias cap:...] [src -> step -> dst]
//	line-based  alias cap:...
//	            src -> step -> dst
//
// A wiring is one invocation: what it is given, the step, what is taken.
//
//	(path, benchmark=bench) -> compare -> result            a named member
//	(prices, window=20, statistic="mean") -> rolling -> s   constants: any JSON value
//	() -> now -> stamp                                      a source given nothing
//	report -> publish -> ()                                 a sink
//	obs -> regress -> (fit, residuals=res)                  several outputs
//	ref -> fetch{retry=3} -> series                         a step policy
//
// A string is read here as "a quote, then anything up to the closing quote";
// whether what stands between is a JSON string is decided by reading it as
// one, and text that is not is a parse error like any other.
const goPegGrammar = `
  program        <- stmt* !.
  stmt           <- '[' inner ']' / inner
  inner          <- wiring / machine_header / header
  header         <- alias cap_urn
  machine_header <- alias machine_kw string
  machine_kw     <- < 'machine' ![-a-zA-Z0-9_] >
  wiring         <- sources arrow step arrow targets
  sources        <- source_group / alias
  source_group   <- '(' (member (',' member)*)? ')'
  member         <- named_member / constant / alias
  named_member   <- alias '=' (constant / alias)
  step           <- alias policy?
  policy         <- '{' (policy_entry (',' policy_entry)*)? '}'
  policy_entry   <- alias '=' policy_value
  policy_value   <- number / alias
  targets        <- target_group / alias
  target_group   <- '(' (target (',' target)*)? ')'
  target         <- named_target / alias
  named_target   <- alias '=' alias
  arrow          <- < '-'+ '>' >
  alias          <- < [a-zA-Z_] [-a-zA-Z0-9_]* >
  constant       <- object / array / string / number / boolean / null
  boolean        <- < ('true' / 'false') ![-a-zA-Z0-9_] >
  null           <- < 'null' ![-a-zA-Z0-9_] >
  object         <- '{' (pair (',' pair)*)? '}'
  pair           <- string ':' constant
  array          <- '[' (constant (',' constant)*)? ']'
  number         <- < '-'? ('0' / [1-9] [0-9]*) ('.' [0-9]+)? ([eE] [-+]? [0-9]+)? ![-a-zA-Z0-9_] >
  string         <- < '"' ('\\' . / !["\\] .)* '"' >
  cap_urn        <- < 'cap:' cap_urn_body* >
  cap_urn_body   <- quoted_value / !(']' / '\n' / '\r') .
  quoted_value   <- '"' ('\\"' / '\\\\' / !'"' .)* '"'
  %whitespace    <- [ \t\r\n]*
`

var (
	grammarOnce   sync.Once
	grammarParser *peg.Parser
	grammarErr    error
)

// notationParser compiles the grammar once. The grammar is a constant of this
// package: one that does not compile is a defect of the package.
func notationParser() *peg.Parser {
	grammarOnce.Do(func() {
		grammarParser, grammarErr = peg.NewParser(goPegGrammar)
		if grammarErr == nil {
			grammarErr = grammarParser.EnableAst()
		}
	})
	if grammarErr != nil {
		panic(fmt.Sprintf("the machine notation grammar does not compile: %v", grammarErr))
	}
	return grammarParser
}

// OnError is what a step's lasting failure does to the run.
type OnError string

const (
	// OnErrorFail — the run fails. The default.
	OnErrorFail OnError = "fail"
	// OnErrorAbsent — the step's outputs are absent and the run goes on.
	OnErrorAbsent OnError = "absent"
)

// StepPolicy is what a step does when it fails
// (capdag/docs/09-machine-notation.md §Step policy). The zero value — no
// policy written — is to fail the run at once.
type StepPolicy struct {
	// Retry is the number of further attempts after a failure whose class is
	// retryable.
	Retry uint32
	// OnError is what a failure that stands (after its retries) does to the
	// run. The empty value is OnErrorFail.
	OnError OnError
}

// IsDefault reports whether this is the policy of a step that wrote none.
func (p StepPolicy) IsDefault() bool {
	return p.Retry == 0 && p.onError() == OnErrorFail
}

func (p StepPolicy) onError() OnError {
	if p.OnError == "" {
		return OnErrorFail
	}
	return p.OnError
}

// Equal reports whether two policies make a step behave the same.
func (p StepPolicy) Equal(other StepPolicy) bool {
	return p.Retry == other.Retry && p.onError() == other.onError()
}

// ToNotation writes the policy as notation writes it after a step alias:
// `{retry=3}`, `{on_error=absent}`, both in that order, or nothing for the
// default.
func (p StepPolicy) ToNotation() string {
	var entries []string
	if p.Retry > 0 {
		entries = append(entries, "retry="+strconv.FormatUint(uint64(p.Retry), 10))
	}
	if p.onError() == OnErrorAbsent {
		entries = append(entries, "on_error=absent")
	}
	if len(entries) == 0 {
		return ""
	}
	return "{" + strings.Join(entries, ", ") + "}"
}

// syntaxMember is one member of a wiring's sources: a node or a constant,
// under the arg it names when it names one.
type syntaxMember struct {
	// name is the arg the member names; hasName says whether it names one.
	name       string
	hasName    bool
	isConstant bool
	node       string
	constant   any
}

// syntaxTarget is one target of a wiring.
type syntaxTarget struct {
	// name is the output the target names. Without one the target takes the
	// step's main output.
	name    string
	hasName bool
	node    string
}

// syntaxWiring is a wiring: one invocation.
type syntaxWiring struct {
	sources []syntaxMember
	step    string
	policy  StepPolicy
	targets []syntaxTarget
}

type syntaxStatementKind int

const (
	syntaxCapHeader syntaxStatementKind = iota
	syntaxMachineHeader
	syntaxWiringStatement
)

// syntaxStatement is one statement of a program.
type syntaxStatement struct {
	kind syntaxStatementKind
	// position is the statement's index among the program's statements.
	position int
	// A header's alias, and the cap URN (cap header) or the unescaped
	// reference (machine header) it gives it.
	alias     string
	capUrn    string
	reference string
	wiring    syntaxWiring
}

// reservedNodeNames are the words the notation reads as constants. A node may
// not bear one.
var reservedNodeNames = map[string]bool{"true": true, "false": true, "null": true}

func syntaxNode(ast *peg.Ast, position int) (string, *MachineSyntaxError) {
	if reservedNodeNames[ast.Token] {
		return "", reservedNodeNameError(ast.Token, position)
	}
	return ast.Token, nil
}

func syntaxConstant(ast *peg.Ast) (any, *MachineSyntaxError) {
	// The grammar's `constant` is JSON's value grammar, but for a string,
	// which it reads loosely: this is where text that is not JSON is refused.
	value, err := decodeConstant(ast.S)
	if err != nil {
		return nil, parseError(fmt.Sprintf("%s is not a JSON value: %v", strings.TrimSpace(ast.S), err))
	}
	return value, nil
}

func syntaxMemberOf(ast *peg.Ast, position int) (syntaxMember, *MachineSyntaxError) {
	inner := ast.Nodes[0]
	switch inner.Name {
	case "named_member":
		member := syntaxMember{name: inner.Nodes[0].Token, hasName: true}
		value := inner.Nodes[1]
		if value.Name == "constant" {
			constant, err := syntaxConstant(value)
			if err != nil {
				return syntaxMember{}, err
			}
			member.isConstant = true
			member.constant = constant
			return member, nil
		}
		node, err := syntaxNode(value, position)
		if err != nil {
			return syntaxMember{}, err
		}
		member.node = node
		return member, nil
	case "constant":
		// A constant has no type of its own to be matched by: it says which
		// arg it is for, or it is not a member.
		return syntaxMember{}, unnamedConstantError(strings.TrimSpace(inner.S), position)
	case "alias":
		node, err := syntaxNode(inner, position)
		if err != nil {
			return syntaxMember{}, err
		}
		return syntaxMember{node: node}, nil
	}
	panic("a member is named, a constant or an alias, not " + inner.Name)
}

func syntaxPolicy(ast *peg.Ast, step string, position int) (StepPolicy, *MachineSyntaxError) {
	var policy StepPolicy
	seen := map[string]bool{}
	for _, entry := range ast.Nodes {
		key := entry.Nodes[0].Token
		value := entry.Nodes[1].Nodes[0]
		if seen[key] {
			return StepPolicy{}, invalidPolicyError(step, position, fmt.Sprintf("'%s' is given twice", key))
		}
		switch key {
		case "retry":
			// A count of further attempts: a whole number, not negative.
			var count uint64
			var err error = strconv.ErrSyntax
			if value.Name == "number" {
				count, err = strconv.ParseUint(value.Token, 10, 32)
			}
			if err != nil {
				return StepPolicy{}, invalidPolicyError(step, position,
					fmt.Sprintf("retry takes a whole number of further attempts, not '%s'", value.Token))
			}
			policy.Retry = uint32(count)
		case "on_error":
			if value.Token != "absent" {
				return StepPolicy{}, invalidPolicyError(step, position,
					fmt.Sprintf("on_error takes 'absent', not '%s'", value.Token))
			}
			policy.OnError = OnErrorAbsent
		default:
			return StepPolicy{}, invalidPolicyError(step, position,
				fmt.Sprintf("unknown key '%s' (a policy takes retry and on_error)", key))
		}
		seen[key] = true
	}
	return policy, nil
}

func syntaxWiringOf(ast *peg.Ast, position int) (syntaxWiring, *MachineSyntaxError) {
	var wiring syntaxWiring

	sources := ast.Nodes[0].Nodes[0]
	switch sources.Name {
	case "alias":
		node, err := syntaxNode(sources, position)
		if err != nil {
			return syntaxWiring{}, err
		}
		wiring.sources = []syntaxMember{{node: node}}
	case "source_group":
		for _, each := range sources.Nodes {
			member, err := syntaxMemberOf(each, position)
			if err != nil {
				return syntaxWiring{}, err
			}
			wiring.sources = append(wiring.sources, member)
		}
	default:
		panic("sources is a group or an alias, not " + sources.Name)
	}

	// Nodes[1] and Nodes[3] are the arrows.
	step := ast.Nodes[2]
	wiring.step = step.Nodes[0].Token
	if len(step.Nodes) > 1 {
		policy, err := syntaxPolicy(step.Nodes[1], wiring.step, position)
		if err != nil {
			return syntaxWiring{}, err
		}
		wiring.policy = policy
	}

	targets := ast.Nodes[4].Nodes[0]
	switch targets.Name {
	case "alias":
		node, err := syntaxNode(targets, position)
		if err != nil {
			return syntaxWiring{}, err
		}
		wiring.targets = []syntaxTarget{{node: node}}
	case "target_group":
		for _, each := range targets.Nodes {
			inner := each.Nodes[0]
			var target syntaxTarget
			nodeAst := inner
			if inner.Name == "named_target" {
				target.name = inner.Nodes[0].Token
				target.hasName = true
				nodeAst = inner.Nodes[1]
			}
			node, err := syntaxNode(nodeAst, position)
			if err != nil {
				return syntaxWiring{}, err
			}
			target.node = node
			wiring.targets = append(wiring.targets, target)
		}
	default:
		panic("targets is a group or an alias, not " + targets.Name)
	}
	// The unnamed target is the main output, and a step has one.
	var unnamed []string
	for _, target := range wiring.targets {
		if !target.hasName {
			unnamed = append(unnamed, target.node)
		}
	}
	if len(unnamed) > 1 {
		return syntaxWiring{}, severalUnnamedTargetsError(position, unnamed)
	}
	return wiring, nil
}

// parseSyntax reads input into its statements, in the order written.
//
// It fails on text the grammar does not accept (ErrParse) and on the rules the
// text alone decides. An input with no statement at all is not an error here:
// what an empty program means is the caller's question.
func parseSyntax(input string) ([]syntaxStatement, *MachineSyntaxError) {
	ast, err := notationParser().ParseAndGetAst(input, nil)
	if err != nil {
		return nil, parseError(err.Error())
	}
	var statements []syntaxStatement
	for _, stmt := range ast.Nodes {
		if stmt.Name != "stmt" {
			continue
		}
		position := len(statements)
		content := stmt.Nodes[0].Nodes[0]
		statement := syntaxStatement{position: position}
		switch content.Name {
		case "header":
			statement.kind = syntaxCapHeader
			statement.alias = content.Nodes[0].Token
			statement.capUrn = content.Nodes[1].Token
		case "machine_header":
			statement.kind = syntaxMachineHeader
			statement.alias = content.Nodes[0].Token
			// Nodes[1] is the `machine` keyword.
			reference, err := decodeConstant(content.Nodes[2].Token)
			text, isString := reference.(string)
			if err != nil || !isString {
				return nil, parseError(fmt.Sprintf("%s is not a JSON string", content.Nodes[2].Token))
			}
			statement.reference = text
		case "wiring":
			wiring, err := syntaxWiringOf(content, position)
			if err != nil {
				return nil, err
			}
			statement.kind = syntaxWiringStatement
			statement.wiring = wiring
		default:
			panic("a statement is a header or a wiring, not " + content.Name)
		}
		statements = append(statements, statement)
	}
	return statements, nil
}
