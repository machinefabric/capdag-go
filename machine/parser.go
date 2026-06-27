package machine

import (
	"fmt"
	"strings"

	peg "github.com/yhirose/go-peg"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/urn"
)

// goPegGrammar is the go-peg equivalent of machine.pest.
// go-peg uses ← for rule definition, < > for token capture,
// and %whitespace for implicit whitespace handling.
// pest uses @{} for atomic (= go-peg tokens), _{ } for silent.
//
// The pest grammar is shipped alongside this file as machine.pest
// for reference — this go-peg grammar is a faithful translation.
//
// Two equally valid statement forms:
// - Bracketed: [alias cap:...] / [src -> alias -> dst]
// - Line-based: alias cap:... / src -> alias -> dst
const goPegGrammar = `
  program     <- stmt* !.
  stmt        <- '[' inner ']' / inner
  inner       <- wiring / header
  header      <- alias cap_urn
  wiring      <- source arrow loop_cap arrow alias
  source      <- group / alias_ref
  group       <- '(' alias_ref (',' alias_ref)+ ')'
  loop_cap    <- loop_keyword alias_ref / alias_ref
  loop_keyword <- 'LOOP'
  arrow       <- < '-'+ '>' >
  alias       <- < [a-zA-Z_] [-a-zA-Z0-9_]* >
  alias_ref   <- < [a-zA-Z_] [-a-zA-Z0-9_]* >
  cap_urn     <- < 'cap:' cap_urn_body* >
  cap_urn_body <- quoted_value / !(']' / '\n' / '\r') .
  quoted_value <- '"' ('\\"' / '\\\\' / !'"' .)* '"'
  %whitespace <- [ \t\r\n]*
`

// parsedHeader represents a parsed header statement.
type parsedHeader struct {
	alias    string
	capUrn   *urn.CapUrn
	position int
}

// rawWiring is one wiring as it comes off the AST walk, with raw alias names.
type rawWiring struct {
	sources  []string
	capAlias string
	target   string
	isLoop   bool
	position int
}

// ParseMachine parses machine notation into a Machine.
//
// Thin wrapper over ParseMachineWithNodeNames for callers that do not need the
// per-strand node-name maps.
func ParseMachine(input string, registry *cap.FabricRegistry) (*Machine, *MachineParseError) {
	m, _, err := ParseMachineWithNodeNames(input, registry)
	return m, err
}

// ParseMachineWithNodeNames parses machine notation into a Machine and also
// returns, per strand (in strand order), the user node-name → NodeId map the
// parser built while interning nodes.
//
// The resolver discards user node names — it works purely on NodeIds — but the
// orchestrator needs to key its ResolvedGraph on the user's original node
// names. This entry point preserves that mapping so callers can translate a
// strand's NodeIds back to the names written in the notation. Mirrors Rust's
// parse_machine_with_node_names.
//
// Two-phase: PEG grammar parsing → resolver. Either phase may fail; the
// combined error type is MachineParseError. The cap registry is required by
// the resolver to look up each cap's args list and run source-to-arg matching.
//
// Uses a PEG parser with a grammar equivalent to machine.pest.
// Fails hard — no fallbacks, no guessing, no recovery.
func ParseMachineWithNodeNames(input string, registry *cap.FabricRegistry) (*Machine, []map[string]NodeId, *MachineParseError) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, nil, syntaxParseError(emptyError())
	}

	// Phase 1: Parse with PEG grammar.
	parser, err := peg.NewParser(goPegGrammar)
	if err != nil {
		return nil, nil, syntaxParseError(parseError(fmt.Sprintf("grammar compilation failed: %s", err)))
	}

	parser.EnableAst()
	ast, err := parser.ParseAndGetAst(input, nil)
	if err != nil {
		return nil, nil, syntaxParseError(parseError(err.Error()))
	}

	// Phase 2: Walk the AST collecting headers and wirings.
	var headers []parsedHeader
	var wirings []rawWiring
	stmtIdx := 0

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
				return nil, nil, syntaxParseError(parseError(fmt.Sprintf("header at statement %d missing components", stmtIdx)))
			}
			alias := contentNode.Nodes[0].Token
			capUrnStr := contentNode.Nodes[1].Token

			capUrnParsed, err := urn.NewCapUrnFromString(capUrnStr)
			if err != nil {
				return nil, nil, syntaxParseError(invalidCapUrnError(alias, err.Error()))
			}

			headers = append(headers, parsedHeader{
				alias:    alias,
				capUrn:   capUrnParsed,
				position: stmtIdx,
			})

		case "wiring":
			if len(contentNode.Nodes) < 5 {
				return nil, nil, syntaxParseError(parseError(fmt.Sprintf("wiring at statement %d missing components", stmtIdx)))
			}

			sourceNode := contentNode.Nodes[0]
			sources := parseSourceNode(sourceNode)

			// contentNode.Nodes[1] = arrow (skip)
			loopCapNode := contentNode.Nodes[2]
			isLoop, capAlias := parseLoopCapNode(loopCapNode)

			// contentNode.Nodes[3] = arrow (skip)
			target := contentNode.Nodes[4].Token

			wirings = append(wirings, rawWiring{
				sources:  sources,
				capAlias: capAlias,
				target:   target,
				isLoop:   isLoop,
				position: stmtIdx,
			})
		}

		stmtIdx++
	}

	// Phase 3: Build alias -> CapUrn map, checking for duplicates.
	type aliasEntry struct {
		capUrn   *urn.CapUrn
		position int
	}
	aliasMap := make(map[string]aliasEntry)

	for _, h := range headers {
		if existing, ok := aliasMap[h.alias]; ok {
			return nil, nil, syntaxParseError(duplicateAliasError(h.alias, existing.position))
		}
		aliasMap[h.alias] = aliasEntry{capUrn: h.capUrn, position: h.position}
	}

	if len(wirings) == 0 && len(headers) > 0 {
		return nil, nil, syntaxParseError(noEdgesError())
	}
	if len(wirings) == 0 {
		return nil, nil, syntaxParseError(emptyError())
	}

	// Phase 3b: cap-alias resolution against the fabric registry.
	//
	// A wiring's cap-position name not defined by a local header is taken to
	// be a fabric cap alias and resolved through the registry — an identifier
	// with no local definition is resolved as an alias before it is declared
	// undefined. The resolved cap URN is injected into aliasMap as if a header
	// had defined it. Media URNs never appear in a wiring (they are implicit),
	// so only cap aliases are resolved here; an alias that points at a media
	// URN in cap position is a hard error. The lookup is the synchronous
	// in-memory cache (ResolveAliasCached); a name that resolves to nothing is
	// left for Phase 4's undefined-alias error.
	seenUnresolved := make(map[string]bool)
	for _, w := range wirings {
		if _, ok := aliasMap[w.capAlias]; ok {
			continue
		}
		if seenUnresolved[w.capAlias] {
			continue
		}
		seenUnresolved[w.capAlias] = true
		target, ok := registry.ResolveAliasCached(w.capAlias)
		if !ok {
			continue // not a known alias → Phase 4 yields undefinedAliasError
		}
		resolvedCapUrn, perr := urn.NewCapUrnFromString(target)
		if perr != nil {
			return nil, nil, syntaxParseError(aliasNotACapError(w.capAlias, target))
		}
		syntheticPos := len(headers) + len(wirings) + len(aliasMap)
		aliasMap[w.capAlias] = aliasEntry{capUrn: resolvedCapUrn, position: syntheticPos}
	}

	// Phase 4: Derive node-name → MediaUrn bindings.
	//
	// Walk wirings in textual order. For each wiring:
	//   - Primary source: bind to cap.in= URN.
	//   - Secondary sources: bind to wildcard media: if unbound.
	//   - Target: bind to cap.out= URN.
	// Re-binding is allowed iff the new URN is_comparable to the existing one.
	nodeMedia := make(map[string]*urn.MediaUrn)
	wildcard, _ := urn.NewMediaUrnFromString("media:")

	for _, w := range wirings {
		entry, ok := aliasMap[w.capAlias]
		if !ok {
			return nil, nil, syntaxParseError(undefinedAliasError(w.capAlias))
		}
		capUrnVal := entry.capUrn

		// Check node-alias collisions.
		for _, src := range w.sources {
			if _, ok := aliasMap[src]; ok {
				return nil, nil, syntaxParseError(nodeAliasCollisionError(src, src))
			}
		}
		if _, ok := aliasMap[w.target]; ok {
			return nil, nil, syntaxParseError(nodeAliasCollisionError(w.target, w.target))
		}

		// Derive media URNs from cap's in=/out= specs.
		capInMedia, err := capUrnVal.InMediaUrn()
		if err != nil {
			return nil, nil, syntaxParseError(invalidMediaUrnError(w.capAlias, fmt.Sprintf("in= spec: %s", err)))
		}

		capOutMedia, err := capUrnVal.OutMediaUrn()
		if err != nil {
			return nil, nil, syntaxParseError(invalidMediaUrnError(w.capAlias, fmt.Sprintf("out= spec: %s", err)))
		}

		// Primary source: bind to cap.in=
		if len(w.sources) > 0 {
			if syntaxErr := assignOrCheckNode(w.sources[0], capInMedia, nodeMedia, w.position); syntaxErr != nil {
				return nil, nil, syntaxParseError(syntaxErr)
			}
			// Secondaries: bind to wildcard if unbound.
			for _, src := range w.sources[1:] {
				if _, bound := nodeMedia[src]; !bound {
					nodeMedia[src] = wildcard
				}
			}
		}

		// Target: bind to cap.out=
		if syntaxErr := assignOrCheckNode(w.target, capOutMedia, nodeMedia, w.position); syntaxErr != nil {
			return nil, nil, syntaxParseError(syntaxErr)
		}
	}

	// Phase 5: Connected-components partition by shared node name.
	// Union-find over wiring indices, where two wirings are unioned iff they
	// share at least one node name.
	n := len(wirings)
	uf := newUnionFind(n)

	// Map: node name → index of the first wiring that touched it.
	nodeFirstWiring := make(map[string]int)
	for wIdx, w := range wirings {
		nodeNames := make([]string, 0, len(w.sources)+1)
		nodeNames = append(nodeNames, w.sources...)
		nodeNames = append(nodeNames, w.target)
		for _, nodeName := range nodeNames {
			if earlier, seen := nodeFirstWiring[nodeName]; seen {
				uf.union(earlier, wIdx)
			} else {
				nodeFirstWiring[nodeName] = wIdx
			}
		}
	}

	// Group wirings by their union-find root. Order roots by smallest wiring
	// index in each group (= first-appearance order).
	groups := make(map[int][]int)
	for wIdx := 0; wIdx < n; wIdx++ {
		root := uf.find(wIdx)
		groups[root] = append(groups[root], wIdx)
	}

	type groupInfo struct {
		root   int
		minIdx int
	}
	groupOrder := make([]groupInfo, 0, len(groups))
	for root, members := range groups {
		minIdx := members[0]
		for _, m := range members[1:] {
			if m < minIdx {
				minIdx = m
			}
		}
		groupOrder = append(groupOrder, groupInfo{root: root, minIdx: minIdx})
	}
	// Sort by minIdx for first-appearance order.
	for i := 1; i < len(groupOrder); i++ {
		for j := i; j > 0 && groupOrder[j].minIdx < groupOrder[j-1].minIdx; j-- {
			groupOrder[j], groupOrder[j-1] = groupOrder[j-1], groupOrder[j]
		}
	}

	// Phase 6: Per-component pre-interning + resolution.
	//
	// For each connected component (= strand), allocate NodeIds in the order
	// user node names are encountered (walking the wirings in textual order).
	// Two distinct user node names that happen to share a media URN stay
	// distinct NodeIds — that's the parser's identity contract.
	strands := make([]*MachineStrand, 0, len(groupOrder))
	strandNodeNames := make([]map[string]NodeId, 0, len(groupOrder))
	for strandIndex, gi := range groupOrder {
		memberIndices := groups[gi.root]
		// Sort member indices to walk wirings in textual order.
		for i := 1; i < len(memberIndices); i++ {
			for j := i; j > 0 && memberIndices[j] < memberIndices[j-1]; j-- {
				memberIndices[j], memberIndices[j-1] = memberIndices[j-1], memberIndices[j]
			}
		}

		var nodes []*urn.MediaUrn
		nameToId := make(map[string]NodeId)

		internNamed := func(name string) NodeId {
			if id, ok := nameToId[name]; ok {
				return id
			}
			u, ok := nodeMedia[name]
			if !ok {
				panic("every node name was bound during phase 4: " + name)
			}
			id := NodeId(len(nodes))
			nodes = append(nodes, u)
			nameToId[name] = id
			return id
		}

		wiringSet := make([]preInternedWiring, 0, len(memberIndices))
		for _, wIdx := range memberIndices {
			w := wirings[wIdx]
			entry := aliasMap[w.capAlias]

			sourceNodeIds := make([]NodeId, len(w.sources))
			for i, name := range w.sources {
				sourceNodeIds[i] = internNamed(name)
			}
			targetNodeId := internNamed(w.target)

			wiringSet = append(wiringSet, preInternedWiring{
				capUrn:        entry.capUrn,
				sourceNodeIds: sourceNodeIds,
				targetNodeId:  targetNodeId,
				isLoop:        w.isLoop,
			})
		}

		strand, absErr := resolvePreInterned(nodes, wiringSet, registry, strandIndex)
		if absErr != nil {
			return nil, nil, abstractionParseError(absErr)
		}
		strands = append(strands, strand)
		strandNodeNames = append(strandNodeNames, nameToId)
	}

	return fromResolvedStrands(strands), strandNodeNames, nil
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

// parseLoopCapNode extracts is_loop flag and cap alias from a loop_cap AST node.
func parseLoopCapNode(node *peg.Ast) (bool, string) {
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

	// If no children, the token itself might be the alias.
	if capAlias == "" && node.Token != "" {
		capAlias = node.Token
	}

	return isLoop, capAlias
}

// assignOrCheckNode assigns a media URN to a node, or checks that an existing
// binding is comparable. Two URNs bound to the same node name must be on the
// same specialization chain (IsComparable); the more-specific URN wins.
func assignOrCheckNode(
	node string,
	mediaUrn *urn.MediaUrn,
	nodeMedia map[string]*urn.MediaUrn,
	position int,
) *MachineSyntaxError {
	if existing, ok := nodeMedia[node]; ok {
		if !existing.IsComparable(mediaUrn) {
			return invalidWiringError(position, fmt.Sprintf(
				"node '%s' has conflicting media types: existing '%s', new '%s'",
				node, existing, mediaUrn,
			))
		}
		// The more-specific URN wins.
		if mediaUrn.Specificity() > existing.Specificity() {
			nodeMedia[node] = mediaUrn
		}
	} else {
		nodeMedia[node] = mediaUrn
	}
	return nil
}

// unionFind is a tiny union-find structure for connected-components partition.
type unionFind struct {
	parent []int
	rank   []int
}

func newUnionFind(n int) *unionFind {
	parent := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	return &unionFind{parent: parent, rank: make([]int, n)}
}

func (uf *unionFind) find(x int) int {
	if uf.parent[x] != x {
		uf.parent[x] = uf.find(uf.parent[x]) // path compression
	}
	return uf.parent[x]
}

func (uf *unionFind) union(a, b int) {
	ra, rb := uf.find(a), uf.find(b)
	if ra == rb {
		return
	}
	if uf.rank[ra] < uf.rank[rb] {
		uf.parent[ra] = rb
	} else if uf.rank[ra] > uf.rank[rb] {
		uf.parent[rb] = ra
	} else {
		uf.parent[rb] = ra
		uf.rank[ra]++
	}
}
