package route

import (
	"fmt"
	"sort"
	"strings"

	peg "github.com/yhirose/go-peg"

	"github.com/machinefabric/capdag-go/urn"
)

// goPegGrammar is the go-peg equivalent of machine.pest.
// go-peg uses ← for rule definition, < > for token capture,
// and %whitespace for implicit whitespace handling.
// pest uses @{} for atomic (= go-peg tokens), _{ } for silent.
//
// The pest grammar is shipped alongside this file as machine.pest
// for reference — this go-peg grammar is a faithful translation.
const goPegGrammar = `
  program     <- stmt* !.
  stmt        <- '[' inner ']'
  inner       <- wiring / header
  header      <- alias cap_urn
  wiring      <- source arrow loop_cap arrow alias
  source      <- group / alias_ref
  group       <- '(' alias_ref (',' alias_ref)+ ')'
  loop_cap    <- loop_keyword alias_ref / alias_ref
  loop_keyword <- 'LOOP'
  arrow       <- < '-'+ '>' >
  alias       <- < [a-zA-Z_] [a-zA-Z0-9_-]* >
  alias_ref   <- < [a-zA-Z_] [a-zA-Z0-9_-]* >
  cap_urn     <- < 'cap:' cap_urn_body* >
  cap_urn_body <- quoted_value / !']' .
  quoted_value <- '"' ('\\"' / '\\\\' / !'"' .)* '"'
  %whitespace <- [ \t\r\n]*
`

// parsedStmt represents a parsed statement (header or wiring).
type parsedHeader struct {
	alias    string
	capUrn   *urn.CapUrn
	position int
}

type parsedWiring struct {
	sources  []string
	capAlias string
	target   string
	isLoop   bool
	position int
}

// ParseMachine parses machine notation into a Machine.
//
// Uses a PEG parser with a grammar equivalent to machine.pest.
// Fails hard — no fallbacks, no guessing, no recovery.
func ParseMachine(input string) (*Machine, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, emptyError()
	}

	// Phase 1: Parse with PEG grammar
	parser, err := peg.NewParser(goPegGrammar)
	if err != nil {
		return nil, parseError(fmt.Sprintf("grammar compilation failed: %s", err))
	}

	// Phase 2: Walk the AST and collect headers + wirings
	var headers []parsedHeader
	var wirings []parsedWiring
	stmtIdx := 0

	// Set up semantic actions to extract data from the parse tree
	g := parser.Grammar
	g["program"].Action = func(v *peg.Values, d peg.Any) (peg.Any, error) {
		return nil, nil
	}

	// Instead of actions, parse to AST and walk it
	parser.EnableAst()

	ast, err := parser.ParseAndGetAst(input, nil)
	if err != nil {
		return nil, parseError(err.Error())
	}

	// Walk the AST
	for _, stmtNode := range ast.Nodes {
		if stmtNode.Name != "stmt" {
			continue
		}
		if len(stmtNode.Nodes) == 0 {
			continue
		}

		innerNode := stmtNode.Nodes[0]
		if len(innerNode.Nodes) == 0 {
			continue
		}

		contentNode := innerNode.Nodes[0]

		switch contentNode.Name {
		case "header":
			if len(contentNode.Nodes) < 2 {
				return nil, parseError(fmt.Sprintf("header at statement %d missing components", stmtIdx))
			}
			alias := contentNode.Nodes[0].Token
			capUrnStr := contentNode.Nodes[1].Token

			capUrnParsed, err := urn.NewCapUrnFromString(capUrnStr)
			if err != nil {
				return nil, invalidCapUrnError(alias, err.Error())
			}

			headers = append(headers, parsedHeader{
				alias:    alias,
				capUrn:   capUrnParsed,
				position: stmtIdx,
			})

		case "wiring":
			// wiring = source arrow loop_cap arrow alias
			if len(contentNode.Nodes) < 5 {
				return nil, parseError(fmt.Sprintf("wiring at statement %d missing components", stmtIdx))
			}

			sourceNode := contentNode.Nodes[0]
			sources := parseSourceNode(sourceNode)

			// contentNode.Nodes[1] = arrow (skip)
			loopMachineNode := contentNode.Nodes[2]
			isLoop, capAlias := parseLoopMachineNode(loopMachineNode)

			// contentNode.Nodes[3] = arrow (skip)
			target := contentNode.Nodes[4].Token

			wirings = append(wirings, parsedWiring{
				sources:  sources,
				capAlias: capAlias,
				target:   target,
				isLoop:   isLoop,
				position: stmtIdx,
			})
		}

		stmtIdx++
	}

	// Phase 3: Build alias -> CapUrn map, checking for duplicates
	type aliasEntry struct {
		capUrn   *urn.CapUrn
		position int
	}
	aliasMap := make(map[string]aliasEntry)
	aliasOrder := make([]string, 0)

	for _, h := range headers {
		if existing, ok := aliasMap[h.alias]; ok {
			return nil, duplicateAliasError(h.alias, existing.position)
		}
		aliasMap[h.alias] = aliasEntry{capUrn: h.capUrn, position: h.position}
		aliasOrder = append(aliasOrder, h.alias)
	}

	// Phase 4: Resolve wirings into MachineEdges
	if len(wirings) == 0 && len(headers) > 0 {
		return nil, noEdgesError()
	}

	nodeMedia := make(map[string]*urn.MediaUrn)
	var edges []*MachineEdge

	for _, w := range wirings {
		// Look up the cap alias
		entry, ok := aliasMap[w.capAlias]
		if !ok {
			return nil, undefinedAliasError(w.capAlias)
		}
		capUrnVal := entry.capUrn

		// Check node-alias collisions
		for _, src := range w.sources {
			if _, ok := aliasMap[src]; ok {
				return nil, nodeAliasCollisionError(src, src)
			}
		}
		if _, ok := aliasMap[w.target]; ok {
			return nil, nodeAliasCollisionError(w.target, w.target)
		}

		// Derive media URNs from cap's in=/out= specs
		capInMedia, err := capUrnVal.InMediaUrn()
		if err != nil {
			return nil, invalidMediaUrnError(w.capAlias, fmt.Sprintf("in= spec: %s", err))
		}

		capOutMedia, err := capUrnVal.OutMediaUrn()
		if err != nil {
			return nil, invalidMediaUrnError(w.capAlias, fmt.Sprintf("out= spec: %s", err))
		}

		// Resolve source media URNs
		sourceUrns := make([]*urn.MediaUrn, 0, len(w.sources))
		for i, src := range w.sources {
			if i == 0 {
				// Primary source: use cap's in= spec
				if err := assignOrCheckNode(src, capInMedia, nodeMedia, w.position); err != nil {
					return nil, err
				}
				sourceUrns = append(sourceUrns, capInMedia)
			} else {
				// Secondary source (fan-in): use existing type if assigned,
				// otherwise use wildcard media:
				if existing, ok := nodeMedia[src]; ok {
					sourceUrns = append(sourceUrns, existing)
				} else {
					wildcard, _ := urn.NewMediaUrnFromString("media:")
					nodeMedia[src] = wildcard
					sourceUrns = append(sourceUrns, wildcard)
				}
			}
		}

		// Assign target media URN
		if err := assignOrCheckNode(w.target, capOutMedia, nodeMedia, w.position); err != nil {
			return nil, err
		}

		edges = append(edges, &MachineEdge{
			Sources: sourceUrns,
			CapUrn:  capUrnVal,
			Target:  capOutMedia,
			IsLoop:  w.isLoop,
		})
	}

	return NewMachine(edges), nil
}

// parseSourceNode extracts source aliases from a source AST node.
func parseSourceNode(node *peg.Ast) []string {
	switch node.Name {
	case "group":
		var aliases []string
		for _, child := range node.Nodes {
			if child.Name == "alias_ref" || child.Name == "alias" {
				aliases = append(aliases, child.Token)
			}
		}
		return aliases
	case "alias_ref", "alias":
		return []string{node.Token}
	case "source":
		if len(node.Nodes) > 0 {
			return parseSourceNode(node.Nodes[0])
		}
		if node.Token != "" {
			return []string{node.Token}
		}
	}
	return nil
}

// parseLoopMachineNode extracts is_loop flag and cap alias from a loop_cap AST node.
func parseLoopMachineNode(node *peg.Ast) (bool, string) {
	isLoop := false
	capAlias := ""

	for _, child := range node.Nodes {
		switch child.Name {
		case "loop_keyword":
			isLoop = true
		case "alias_ref", "alias":
			capAlias = child.Token
		}
	}

	// If no children, the token itself might be the alias
	if capAlias == "" && node.Token != "" {
		capAlias = node.Token
	}

	return isLoop, capAlias
}

// assignOrCheckNode assigns a media URN to a node, or checks consistency.
func assignOrCheckNode(
	node string,
	mediaUrn *urn.MediaUrn,
	nodeMedia map[string]*urn.MediaUrn,
	position int,
) error {
	if existing, ok := nodeMedia[node]; ok {
		if !existing.IsComparable(mediaUrn) {
			return invalidWiringError(position, fmt.Sprintf(
				"node '%s' has conflicting media types: existing '%s', new '%s'",
				node, existing, mediaUrn,
			))
		}
	} else {
		nodeMedia[node] = mediaUrn
	}
	return nil
}

// --- Serializer methods ---

// ToMachineNotation serializes this route graph to canonical one-line machine notation.
func (g *Machine) ToMachineNotation() string {
	if g.IsEmpty() {
		return ""
	}

	aliases, nodeNames, edgeOrder := g.buildSerializationMaps()
	var parts []string

	// Emit headers in alias-sorted order
	sortedAliases := make([]string, 0, len(aliases))
	for alias := range aliases {
		sortedAliases = append(sortedAliases, alias)
	}
	sort.Strings(sortedAliases)

	for _, alias := range sortedAliases {
		info := aliases[alias]
		edge := g.edges[info.edgeIdx]
		parts = append(parts, fmt.Sprintf("[%s %s]", alias, edge.CapUrn))
	}

	// Emit wirings in edge order
	for _, edgeIdx := range edgeOrder {
		edge := g.edges[edgeIdx]
		// Find alias for this edge
		var alias string
		for a, info := range aliases {
			if info.edgeIdx == edgeIdx {
				alias = a
				break
			}
		}

		// Source node name(s)
		sources := make([]string, len(edge.Sources))
		for i, s := range edge.Sources {
			sources[i] = nodeNames[s.String()]
		}

		targetName := nodeNames[edge.Target.String()]
		loopPrefix := ""
		if edge.IsLoop {
			loopPrefix = "LOOP "
		}

		if len(sources) == 1 {
			parts = append(parts, fmt.Sprintf("[%s -> %s%s -> %s]", sources[0], loopPrefix, alias, targetName))
		} else {
			group := strings.Join(sources, ", ")
			parts = append(parts, fmt.Sprintf("[(%s) -> %s%s -> %s]", group, loopPrefix, alias, targetName))
		}
	}

	return strings.Join(parts, "")
}

// ToMachineNotationMultiline serializes to multi-line machine notation.
func (g *Machine) ToMachineNotationMultiline() string {
	if g.IsEmpty() {
		return ""
	}

	aliases, nodeNames, edgeOrder := g.buildSerializationMaps()
	var lines []string

	// Emit headers
	sortedAliases := make([]string, 0, len(aliases))
	for alias := range aliases {
		sortedAliases = append(sortedAliases, alias)
	}
	sort.Strings(sortedAliases)

	for _, alias := range sortedAliases {
		info := aliases[alias]
		edge := g.edges[info.edgeIdx]
		lines = append(lines, fmt.Sprintf("[%s %s]", alias, edge.CapUrn))
	}

	// Emit wirings
	for _, edgeIdx := range edgeOrder {
		edge := g.edges[edgeIdx]
		var alias string
		for a, info := range aliases {
			if info.edgeIdx == edgeIdx {
				alias = a
				break
			}
		}

		sources := make([]string, len(edge.Sources))
		for i, s := range edge.Sources {
			sources[i] = nodeNames[s.String()]
		}

		targetName := nodeNames[edge.Target.String()]
		loopPrefix := ""
		if edge.IsLoop {
			loopPrefix = "LOOP "
		}

		if len(sources) == 1 {
			lines = append(lines, fmt.Sprintf("[%s -> %s%s -> %s]", sources[0], loopPrefix, alias, targetName))
		} else {
			group := strings.Join(sources, ", ")
			lines = append(lines, fmt.Sprintf("[(%s) -> %s%s -> %s]", group, loopPrefix, alias, targetName))
		}
	}

	return strings.Join(lines, "\n")
}

type aliasInfo struct {
	edgeIdx int
	capStr  string
}

// buildSerializationMaps builds alias map, node name map, and edge ordering.
func (g *Machine) buildSerializationMaps() (map[string]aliasInfo, map[string]string, []int) {
	// Step 1: Canonical edge ordering
	edgeOrder := make([]int, len(g.edges))
	for i := range edgeOrder {
		edgeOrder[i] = i
	}
	sort.Slice(edgeOrder, func(a, b int) bool {
		ea := g.edges[edgeOrder[a]]
		eb := g.edges[edgeOrder[b]]

		capCmp := strings.Compare(ea.CapUrn.String(), eb.CapUrn.String())
		if capCmp != 0 {
			return capCmp < 0
		}

		srcA := make([]string, len(ea.Sources))
		for i, s := range ea.Sources {
			srcA[i] = s.String()
		}
		srcB := make([]string, len(eb.Sources))
		for i, s := range eb.Sources {
			srcB[i] = s.String()
		}
		for i := 0; i < len(srcA) && i < len(srcB); i++ {
			if srcA[i] != srcB[i] {
				return srcA[i] < srcB[i]
			}
		}
		if len(srcA) != len(srcB) {
			return len(srcA) < len(srcB)
		}

		return ea.Target.String() < eb.Target.String()
	})

	// Step 2: Generate aliases from op= tag
	aliases := make(map[string]aliasInfo)
	aliasCounts := make(map[string]int)

	for _, idx := range edgeOrder {
		edge := g.edges[idx]
		baseAlias, ok := edge.CapUrn.GetTag("op")
		if !ok {
			baseAlias = fmt.Sprintf("edge_%d", idx)
		}

		count := aliasCounts[baseAlias]
		alias := baseAlias
		if count > 0 {
			alias = fmt.Sprintf("%s_%d", baseAlias, count)
		}
		aliasCounts[baseAlias] = count + 1

		aliases[alias] = aliasInfo{edgeIdx: idx, capStr: edge.CapUrn.String()}
	}

	// Step 3: Generate node names
	nodeNames := make(map[string]string)
	nodeCounter := 0

	for _, idx := range edgeOrder {
		edge := g.edges[idx]
		for _, src := range edge.Sources {
			key := src.String()
			if _, ok := nodeNames[key]; !ok {
				nodeNames[key] = fmt.Sprintf("n%d", nodeCounter)
				nodeCounter++
			}
		}
		targetKey := edge.Target.String()
		if _, ok := nodeNames[targetKey]; !ok {
			nodeNames[targetKey] = fmt.Sprintf("n%d", nodeCounter)
			nodeCounter++
		}
	}

	return aliases, nodeNames, edgeOrder
}
