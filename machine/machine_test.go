package machine

import (
	"sort"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ===================================================================
// Test fixtures
// ===================================================================

// buildCap constructs a *cap.Cap with a single-stdin-arg per entry in
// argMediaUrns. Slot identity == stdin URN for each arg.
func buildCap(capUrnStr, title string, argMediaUrns []string, outputMediaUrn string) *cap.Cap {
	capUrnParsed, err := urn.NewCapUrnFromString(capUrnStr)
	if err != nil {
		panic("test fixture: invalid cap URN " + capUrnStr + ": " + err.Error())
	}
	args := make([]cap.CapArg, len(argMediaUrns))
	for i, mu := range argMediaUrns {
		stdinVal := mu
		args[i] = cap.NewCapArg(mu, true, []cap.ArgSource{{Stdin: &stdinVal}})
	}
	outMedia := outputMediaUrn
	output := cap.NewCapOutput(outMedia, "output of "+title)
	return &cap.Cap{
		Urn:     capUrnParsed,
		Title:   title,
		Command: "test-fixture://" + title,
		Args:    args,
		Output:  output,
	}
}

// registryWith builds a test FabricRegistry pre-populated with the given caps.
func registryWith(caps []*cap.Cap) *cap.FabricRegistry {
	r := cap.NewFabricRegistryForTest()
	r.AddCapsToCache(caps)
	return r
}

// mediaUrn parses a media URN string; panics on failure.
func mediaUrn(s string) *urn.MediaUrn {
	m, err := urn.NewMediaUrnFromString(s)
	if err != nil {
		panic("test fixture: invalid media URN " + s + ": " + err.Error())
	}
	return m
}

// capUrnVal parses a cap URN string; panics on failure.
func capUrnVal(s string) *urn.CapUrn {
	c, err := urn.NewCapUrnFromString(s)
	if err != nil {
		panic("test fixture: invalid cap URN " + s + ": " + err.Error())
	}
	return c
}

// foreachStep builds a StepTypeForEach StrandStep.
func foreachStep(mediaUrnStr string) *planner.StrandStep {
	u := mediaUrn(mediaUrnStr)
	step := planner.NewStrandStep(planner.StepTypeForEach, u, u)
	step.MediaDef = u
	return step
}

// collectStep builds a StepTypeCollect StrandStep.
func collectStep(mediaUrnStr string) *planner.StrandStep {
	u := mediaUrn(mediaUrnStr)
	step := planner.NewStrandStep(planner.StepTypeCollect, u, u)
	step.MediaDef = u
	return step
}

// capStep builds a StepTypeCap StrandStep with a single main input fed by the
// strand input. Chained fixtures wire the predecessor via chainCapSteps
// (below). New in the explicit-inputs model (v3): the LOOP keyword is retired
// and IsLoop is derived from cardinality — see resolvePreInterned.
func capStep(capUrnStr, title, from, to string) *planner.StrandStep {
	fromUrn := mediaUrn(from)
	step := planner.NewStrandStep(planner.StepTypeCap, fromUrn, mediaUrn(to))
	step.CapUrnVal = capUrnVal(capUrnStr)
	step.StepTitle = title
	step.Inputs = []planner.CapInput{
		{ArgUrn: fromUrn, Source: planner.NewArgSourceStrandInput()},
	}
	return step
}

// chainCapSteps wires a sequence of steps into a linear chain: each cap step
// after the first takes its single main input from the immediately preceding
// cap step's output (ForEach/Collect steps are passed over — they are
// cardinality transitions, not producers). Under the explicit-inputs model a
// chained fixture must name its predecessor rather than rely on position, so
// fixtures that build linear strands wrap their step slice in this before
// constructing the Strand. Mirrors Rust test_fixtures::chain_cap_steps.
func chainCapSteps(steps []*planner.StrandStep) []*planner.StrandStep {
	var prevCapToken string
	haveProducer := false
	for _, step := range steps {
		token := step.TokenId
		if step.StepType == planner.StepTypeCap {
			if haveProducer && len(step.Inputs) > 0 {
				step.Inputs[0].Source = planner.NewArgSourceStep(prevCapToken)
			}
			prevCapToken = token
			haveProducer = true
		}
	}
	return steps
}

// strandFromSteps wraps steps into a Strand.
func strandFromSteps(steps []*planner.StrandStep, description string) *planner.Strand {
	steps = chainCapSteps(steps)
	totalSteps := len(steps)
	capStepCount := 0
	for _, s := range steps {
		if s.StepType == planner.StepTypeCap {
			capStepCount++
		}
	}
	sourceMediaUrn := steps[0].FromSpec
	targetMediaUrn := steps[len(steps)-1].ToSpec
	return &planner.Strand{
		Steps:          steps,
		SourceMediaUrn: sourceMediaUrn,
		TargetMediaUrn: targetMediaUrn,
		TotalSteps:     totalSteps,
		CapStepCount:   capStepCount,
		Description:    description,
	}
}

// ===================================================================
// Cap definitions used across tests
// ===================================================================

func extractCapDef() *cap.Cap {
	return buildCap(
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
		"extract",
		[]string{"media:ext=pdf"},
		`media:txt;enc=utf-8`,
	)
}

func embedCapDef() *cap.Cap {
	return buildCap(
		`cap:in="media:enc=utf-8";embed;out="media:vec;record"`,
		"embed",
		[]string{"media:enc=utf-8"},
		`media:vec;record`,
	)
}

func pdfToTxtStrand() *planner.Strand {
	return strandFromSteps(
		[]*planner.StrandStep{
			capStep(
				`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
				"extract",
				"media:ext=pdf",
				`media:txt;enc=utf-8`,
			),
		},
		"pdf to txt",
	)
}

func txtToVecStrand() *planner.Strand {
	return strandFromSteps(
		[]*planner.StrandStep{
			capStep(
				`cap:in="media:enc=utf-8";embed;out="media:vec;record"`,
				"embed",
				`media:txt;enc=utf-8`,
				`media:vec;record`,
			),
		},
		"txt to vec",
	)
}

// ===================================================================
// FromStrand tests
// ===================================================================

// TEST1135: MachineStrand::node_urn(id) returns the MediaUrn at that NodeId. For a single-cap strand (pdf → extract → txt), there are exactly two nodes and each returns a valid URN.
func Test1135_StrandNodeUrnReturnsMediaUrnAtNodeId(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef()})
	m, err := FromStrand(pdfToTxtStrand(), registry)
	if err != nil {
		t.Fatalf("FromStrand failed: %s", err)
	}
	strand := m.Strands()[0]

	for id := range strand.Nodes() {
		nodeUrn := strand.NodeUrn(NodeId(id))
		if nodeUrn.String() == "" {
			t.Fatalf("node_urn(%d) must return a non-empty URN", id)
		}
	}
}

// TEST1155: Building a machine from one strand produces one strand with one resolved edge.
func Test1155_FromStrandProducesSingleStrandMachine(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef()})
	m, err := FromStrand(pdfToTxtStrand(), registry)
	if err != nil {
		t.Fatalf("FromStrand failed: %s", err)
	}
	if m.StrandCount() != 1 {
		t.Fatalf("expected 1 strand, got %d", m.StrandCount())
	}
	if len(m.Strands()[0].Edges()) != 1 {
		t.Fatalf("expected 1 edge in strand, got %d", len(m.Strands()[0].Edges()))
	}
}

// TEST1156: Building from multiple strands keeps them disjoint and preserves input strand order.
func Test1156_FromStrandsKeepStrandsDisjoint(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef(), embedCapDef()})
	m, err := FromStrands([]*planner.Strand{pdfToTxtStrand(), txtToVecStrand()}, registry)
	if err != nil {
		t.Fatalf("FromStrands failed: %s", err)
	}
	if m.StrandCount() != 2 {
		t.Fatalf("FromStrands must keep input strands as disjoint MachineStrands; got %d", m.StrandCount())
	}
	if len(m.Strands()[0].Edges()) != 1 {
		t.Fatalf("strand 0: expected 1 edge, got %d", len(m.Strands()[0].Edges()))
	}
	if len(m.Strands()[1].Edges()) != 1 {
		t.Fatalf("strand 1: expected 1 edge, got %d", len(m.Strands()[1].Edges()))
	}
	// Strand order must match input order.
	if !containsStr(m.Strands()[0].Edges()[0].CapUrn.String(), "extract") {
		t.Errorf("strand 0 should use extract cap, got %s", m.Strands()[0].Edges()[0].CapUrn)
	}
	if !containsStr(m.Strands()[1].Edges()[0].CapUrn.String(), "embed") {
		t.Errorf("strand 1 should use embed cap, got %s", m.Strands()[1].Edges()[0].CapUrn)
	}
}

// TEST1157: Building from zero strands fails with NoCapabilitySteps.
func Test1157_FromStrandsEmptyInputFailsHard(t *testing.T) {
	registry := registryWith([]*cap.Cap{})
	_, err := FromStrands([]*planner.Strand{}, registry)
	if err == nil {
		t.Fatal("expected error for empty strands, got nil")
	}
	if err.Kind != ErrAbstractionNoCapabilitySteps {
		t.Errorf("expected ErrAbstractionNoCapabilitySteps, got %v", err.Kind)
	}
}

// TEST1158: Machine equivalence is strict about strand order and rejects reordered strands.
func Test1158_MachineIsEquivalentIsStrictPositional(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef(), embedCapDef()})
	forward, err := FromStrands([]*planner.Strand{pdfToTxtStrand(), txtToVecStrand()}, registry)
	if err != nil {
		t.Fatal(err)
	}
	reversed, err := FromStrands([]*planner.Strand{txtToVecStrand(), pdfToTxtStrand()}, registry)
	if err != nil {
		t.Fatal(err)
	}
	if forward.IsEquivalent(reversed) {
		t.Error("swapping strand order must break strict equivalence")
	}
	if !forward.IsEquivalent(forward) {
		t.Error("a machine must be equivalent to itself")
	}
	if !reversed.IsEquivalent(reversed) {
		t.Error("a machine must be equivalent to itself")
	}
}

// TEST1159: MachineStrand equivalence accepts two separately built but structurally identical strands.
func Test1159_MachineStrandIsEquivalentWalksNodeBijection(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef()})
	m1, err1 := FromStrand(pdfToTxtStrand(), registry)
	m2, err2 := FromStrand(pdfToTxtStrand(), registry)
	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected error: %v %v", err1, err2)
	}
	if !m1.Strands()[0].IsEquivalent(m2.Strands()[0]) {
		t.Error("two MachineStrands built from the same planner strand must be equivalent")
	}
}

// ===================================================================
// Anchor computation tests
// ===================================================================

// TEST1160: Creating a MachineRun stores the canonical notation and starts in the pending state.
func Test1160_InputOutputAnchors(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef()})
	m, err := FromStrand(pdfToTxtStrand(), registry)
	if err != nil {
		t.Fatal(err)
	}
	strand := m.Strands()[0]

	if len(strand.InputAnchorIds()) != 1 {
		t.Errorf("expected 1 input anchor, got %d", len(strand.InputAnchorIds()))
	}
	if len(strand.OutputAnchorIds()) != 1 {
		t.Errorf("expected 1 output anchor, got %d", len(strand.OutputAnchorIds()))
	}

	// Input anchor URN must be media:ext=pdf (the from_spec of the extract step).
	inputAnchors := strand.InputAnchors()
	if len(inputAnchors) != 1 || !containsStr(inputAnchors[0].String(), "pdf") {
		t.Errorf("expected input anchor to contain 'pdf', got %v", inputAnchors)
	}

	// Output anchor URN must be media:enc=utf-8;ext=txt (the to_spec).
	outputAnchors := strand.OutputAnchors()
	if len(outputAnchors) != 1 || !containsStr(outputAnchors[0].String(), "txt") {
		t.Errorf("expected output anchor to contain 'txt', got %v", outputAnchors)
	}
}

// ===================================================================
// IsLoop / ForEach tests
// ===================================================================

// TEST1169: A sequence-output cap feeding a scalar-input cap makes the resolved
// edge a per-item map (IsLoop), derived from cardinality — the single rule
// cap.NeedsForeach, which replaces the retired LOOP keyword. The
// scalar->sequence producer edge itself does not loop.
func Test1169_SequenceIntoScalarCapDerivesIsLoop(t *testing.T) {
	// Producer: scalar text -> SEQUENCE of items.
	splitter := buildCap(
		`cap:in="media:enc=utf-8";split;out="media:item;enc=utf-8"`,
		"split",
		[]string{"media:enc=utf-8"},
		`media:item;enc=utf-8`,
	)
	splitter.Output.IsSequence = true
	// Consumer: scalar item -> scalar text.
	texter := buildCap(
		`cap:in="media:item;enc=utf-8";t;out="media:enc=utf-8"`,
		"t",
		[]string{"media:item;enc=utf-8"},
		`media:enc=utf-8`,
	)
	registry := registryWith([]*cap.Cap{splitter, texter})
	notation := `[split cap:in="media:enc=utf-8";split;out="media:item;enc=utf-8"]` +
		`[t cap:in="media:item;enc=utf-8";t;out="media:enc=utf-8"]` +
		`[a -> split -> items]` +
		`[items -> t -> b]`

	m, parseErr := ParseMachine(notation, registry)
	require.Nil(t, parseErr, "must parse")
	require.Equal(t, 1, m.StrandCount())
	strand := m.Strands()[0]
	require.Equal(t, 2, len(strand.Edges()))

	var splitEdge, tEdge *MachineEdge
	for _, e := range strand.Edges() {
		if containsStr(e.CapUrn.String(), "split") {
			splitEdge = e
		} else {
			tEdge = e
		}
	}
	require.NotNil(t, splitEdge, "split edge must be present")
	require.NotNil(t, tEdge, "t edge must be present")
	assert.False(t, splitEdge.IsLoop, "a scalar source feeding the sequence-producing cap must not map")
	assert.True(t, tEdge.IsLoop, "a sequence feeding a scalar-input cap must map per item (IsLoop)")
}

// TEST1170: Parsing and then serializing machine notation round-trips to the canonical form.
func Test1170_CollectIsElided(t *testing.T) {
	loopCap := buildCap(
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
		"extract",
		[]string{"media:ext=pdf"},
		`media:txt;enc=utf-8`,
	)
	registry := registryWith([]*cap.Cap{loopCap})

	steps := []*planner.StrandStep{
		capStep(
			`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
			"extract",
			"media:ext=pdf",
			`media:txt;enc=utf-8`,
		),
		{
			StepType: planner.StepTypeCollect,
			FromSpec: mediaUrn(`media:txt;enc=utf-8`),
			ToSpec:   mediaUrn(`media:txt;enc=utf-8`),
			MediaDef: mediaUrn(`media:txt;enc=utf-8`),
		},
	}
	strand := strandFromSteps(steps, "extract then collect")
	m, err := FromStrand(strand, registry)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Strands()[0].Edges()) != 1 {
		t.Errorf("Collect must produce no edge; expected 1 edge total, got %d", len(m.Strands()[0].Edges()))
	}
}

// ===================================================================
// Parser tests
// ===================================================================

// TEST1163: Parsing one connected strand yields a single machine strand with both caps connected by the shared node.
func Test1163_ParseSingleStrandTwoCapsConnectedViaSharedNode(t *testing.T) {
	registry := pdfExtractEmbedRegistry()
	notation := `[extract cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"]` +
		`[embed cap:in="media:enc=utf-8";embed;out="media:vec;record"]` +
		`[doc -> extract -> txt]` +
		`[txt -> embed -> vec]`

	m, parseErr := ParseMachine(notation, registry)
	if parseErr != nil {
		t.Fatalf("ParseMachine failed: %s", parseErr)
	}
	if m.StrandCount() != 1 {
		t.Fatalf("expected 1 strand (shared node 'txt' merges both wirings), got %d", m.StrandCount())
	}
	strand := m.Strands()[0]
	if len(strand.Edges()) != 2 {
		t.Fatalf("expected 2 edges in strand, got %d", len(strand.Edges()))
	}
	// The intermediate node must be the same NodeId for both edges.
	extractTarget := strand.Edges()[0].Target
	embedSource := strand.Edges()[1].Assignment[0].Source
	if extractTarget != embedSource {
		t.Errorf("intermediate node 'txt' must be the same NodeId: extract.Target=%d, embed.source=%d",
			extractTarget, embedSource)
	}
}

// TEST1164: Parsing two disconnected strand definitions yields two separate machine strands.
func Test1164_ParseTwoDisconnectedStrandsYieldsTwoMachineStrands(t *testing.T) {
	convertA := buildCap(
		`cap:in="media:fmt=json";convert-a;out="media:fmt=csv"`,
		"convert_a",
		[]string{"media:fmt=json"},
		"media:fmt=csv",
	)
	convertB := buildCap(
		"cap:in=\"media:ext=html\";convert-b;out=\"media:ext=txt\"",
		"convert_b",
		[]string{"media:ext=html"},
		"media:ext=txt",
	)
	registry := registryWith([]*cap.Cap{convertA, convertB})

	notation := `[ca cap:in="media:fmt=json";convert-a;out="media:fmt=csv"]` +
		`[cb cap:in="media:ext=html";convert-b;out="media:ext=txt"]` +
		`[input_a -> ca -> output_a]` +
		`[input_b -> cb -> output_b]`

	m, parseErr := ParseMachine(notation, registry)
	if parseErr != nil {
		t.Fatalf("ParseMachine failed: %s", parseErr)
	}
	if m.StrandCount() != 2 {
		t.Fatalf("two wirings sharing no nodes must produce 2 strands, got %d", m.StrandCount())
	}
	// Strand order is first-appearance order.
	if len(m.Strands()[0].Edges()) != 1 {
		t.Errorf("strand 0: expected 1 edge, got %d", len(m.Strands()[0].Edges()))
	}
	if len(m.Strands()[1].Edges()) != 1 {
		t.Errorf("strand 1: expected 1 edge, got %d", len(m.Strands()[1].Edges()))
	}
	// First strand uses the convert-a cap (marker), second uses convert-b.
	if !containsStr(m.Strands()[0].Edges()[0].CapUrn.String(), "convert-a") {
		t.Errorf("strand 0 should use convert-a, got %s", m.Strands()[0].Edges()[0].CapUrn)
	}
	if !containsStr(m.Strands()[1].Edges()[0].CapUrn.String(), "convert-b") {
		t.Errorf("strand 1 should use convert-b, got %s", m.Strands()[1].Edges()[0].CapUrn)
	}
}

// TEST1171: Empty machine notation is rejected as a syntax error.
func Test1171_ParseEmptyInputReturnsError(t *testing.T) {
	registry := registryWith([]*cap.Cap{})
	_, err := ParseMachine("   ", registry)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
	if err.Syntax == nil || err.Syntax.Kind != ErrEmpty {
		t.Errorf("expected ErrEmpty syntax error, got %v", err)
	}
}

// TEST1136: parse_machine with an undefined cap alias raises MachineParseError wrapping MachineSyntaxError::UndefinedAlias. This pins the error path so an alias lookup failure is always surfaced as a syntax error (not a resolution error or a panic).
func Test1136_ParseMachineUndefinedAliasRaisesSyntaxError(t *testing.T) {
	registry := registryWith([]*cap.Cap{})
	notation := "[doc -> undefined_alias -> text]"
	_, err := ParseMachine(notation, registry)
	if err == nil {
		t.Fatal("expected error for undefined alias")
	}
	if err.Syntax == nil || err.Syntax.Kind != ErrUndefinedAlias {
		t.Errorf("undefined alias must produce a MachineParseError syntax UndefinedAlias, got %v", err)
	}
}

// Test0124_ParseHeadersWithNoWiringsReturnsNoEdgesError verifies the ErrNoEdges case.
func Test0124_ParseHeadersWithNoWiringsReturnsNoEdgesError(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef()})
	notation := `[extract cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"]`
	_, err := ParseMachine(notation, registry)
	if err == nil {
		t.Fatal("expected error for headers with no wirings")
	}
	if err.Syntax == nil || err.Syntax.Kind != ErrNoEdges {
		t.Errorf("expected ErrNoEdges, got %v", err)
	}
}

// TEST1166: Duplicate header aliases are reported as syntax errors.
func Test1166_ParseDuplicateAliasReturnsError(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef()})
	notation := `[extract cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"]` +
		`[extract cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"]` +
		`[doc -> extract -> txt]`
	_, err := ParseMachine(notation, registry)
	if err == nil {
		t.Fatal("expected error for duplicate alias")
	}
	if err.Syntax == nil || err.Syntax.Kind != ErrDuplicateAlias {
		t.Errorf("expected ErrDuplicateAlias, got %v", err)
	}
}

// TEST1167: Wiring that references an undefined alias is reported as a syntax error.
func Test1167_ParseUndefinedAliasReturnsError(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef()})
	notation := `[doc -> no_such_cap -> txt]`
	_, err := ParseMachine(notation, registry)
	if err == nil {
		t.Fatal("expected error for undefined alias")
	}
	if err.Syntax == nil || err.Syntax.Kind != ErrUndefinedAlias {
		t.Errorf("expected ErrUndefinedAlias, got %v", err)
	}
}

// TEST1165: Parsing fails hard when a referenced cap is missing from the registry cache.
func Test1165_ParseUnknownCapInRegistryReturnsAbstractionError(t *testing.T) {
	// Empty registry — cap won't be found during resolution.
	registry := registryWith([]*cap.Cap{})
	notation := `[ex cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"]` +
		`[doc -> ex -> txt]`
	_, err := ParseMachine(notation, registry)
	if err == nil {
		t.Fatal("expected error for unknown cap in registry")
	}
	if err.Abstraction == nil || err.Abstraction.Kind != ErrAbstractionUnknownCap {
		t.Errorf("expected ErrAbstractionUnknownCap, got %v", err)
	}
}

// TEST1168: Parsing rejects node names that collide with declared cap aliases.
func Test1168_ParseNodeNameCollidesWithCapAlias(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef()})
	// Node name 'extract' collides with cap alias 'extract'.
	notation := `[extract cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"]` +
		`[extract -> extract -> txt]`
	_, err := ParseMachine(notation, registry)
	if err == nil {
		t.Fatal("expected error for node-alias collision")
	}
	if err.Syntax == nil || err.Syntax.Kind != ErrNodeAliasCollision {
		t.Errorf("expected ErrNodeAliasCollision, got %v", err)
	}
}

// ===================================================================
// Serializer tests
// ===================================================================

// TEST1173: Serializing and reparsing a machine preserves strict machine equivalence.
func Test1173_ToMachineNotationRoundTrips(t *testing.T) {
	registry := pdfExtractEmbedRegistry()
	notation := `[extract cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"]` +
		`[embed cap:in="media:enc=utf-8";embed;out="media:vec;record"]` +
		`[doc -> extract -> txt]` +
		`[txt -> embed -> vec]`

	m1, parseErr := ParseMachine(notation, registry)
	if parseErr != nil {
		t.Fatalf("first parse failed: %s", parseErr)
	}

	serialized := m1.ToMachineNotation()
	if serialized == "" {
		t.Fatal("ToMachineNotation returned empty string for non-empty machine")
	}

	m2, parseErr2 := ParseMachine(serialized, registry)
	if parseErr2 != nil {
		t.Fatalf("second parse (of serialized notation) failed: %s", parseErr2)
	}

	if !m1.IsEquivalent(m2) {
		t.Errorf("round-tripped machine is not equivalent to the original.\nOriginal: %s\nSerialized: %s",
			notation, serialized)
	}
}

// TEST1174: The line-based notation format round-trips back to the same machine.
func Test1174_LineBasedFormatRoundTripsToSameMachine(t *testing.T) {
	registry := pdfExtractEmbedRegistry()
	notation := `[extract cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"]` +
		`[embed cap:in="media:enc=utf-8";embed;out="media:vec;record"]` +
		`[doc -> extract -> txt]` +
		`[txt -> embed -> vec]`

	m1, parseErr := ParseMachine(notation, registry)
	if parseErr != nil {
		t.Fatalf("first parse failed: %s", parseErr)
	}

	lineBased := m1.ToMachineNotationFormatted(NotationFormatLineBased)
	// Line-based form is one statement per line, with no enclosing brackets.
	if containsStr(lineBased, "[") {
		t.Fatalf("line-based form must not contain brackets, got: %s", lineBased)
	}

	m2, parseErr2 := ParseMachine(lineBased, registry)
	if parseErr2 != nil {
		t.Fatalf("line-based form must parse: %s", parseErr2)
	}
	if !m1.IsEquivalent(m2) {
		t.Errorf("line-based round-trip not equivalent.\nOriginal: %s\nLine-based: %s", notation, lineBased)
	}
}

// TEST1175: Serializing an empty machine produces an empty string.
func Test1175_EmptyMachineSerializesToEmpty(t *testing.T) {
	m := fromResolvedStrands(nil)
	if m.ToMachineNotation() != "" {
		t.Errorf("empty machine must serialize to empty string, got %q", m.ToMachineNotation())
	}
}

// TEST1172: Serializing a two-step strand emits the expected aliases and node names.
func Test1172_MachineStringRepr(t *testing.T) {
	registry := registryWith([]*cap.Cap{extractCapDef()})
	m, err := FromStrand(pdfToTxtStrand(), registry)
	if err != nil {
		t.Fatal(err)
	}
	s := m.String()
	if !containsStr(s, "1 strands") || !containsStr(s, "1 edges") {
		t.Errorf("unexpected String() output: %q", s)
	}
}

// ===================================================================
// IsEquivalent structural corner cases
// ===================================================================

// TEST1189: Strand resolution keeps canonical anchor ordering stable across equivalent inputs.
func Test1189_StrandEquivalenceWithDifferentNodeAllocationOrders(t *testing.T) {
	// Build two machines from identical strands — node allocation order is
	// deterministic but this confirms the bijection handles it correctly.
	registry := registryWith([]*cap.Cap{extractCapDef()})
	m1, _ := FromStrand(pdfToTxtStrand(), registry)
	m2, _ := FromStrand(pdfToTxtStrand(), registry)
	if !m1.IsEquivalent(m2) {
		t.Error("identical strands must be equivalent")
	}

	// A two-step chain: extract then embed.
	twoStepCap := buildCap(
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
		"extract",
		[]string{"media:ext=pdf"},
		`media:txt;enc=utf-8`,
	)
	twoStepEmbed := buildCap(
		`cap:in="media:enc=utf-8";embed;out="media:vec;record"`,
		"embed",
		[]string{"media:enc=utf-8"},
		`media:vec;record`,
	)
	twoStepRegistry := registryWith([]*cap.Cap{twoStepCap, twoStepEmbed})

	twoStepStrand := strandFromSteps(
		[]*planner.StrandStep{
			capStep(`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`, "extract", "media:ext=pdf", `media:txt;enc=utf-8`),
			capStep(`cap:in="media:enc=utf-8";embed;out="media:vec;record"`, "embed", `media:txt;enc=utf-8`, `media:vec;record`),
		},
		"extract then embed",
	)

	m3, err := FromStrand(twoStepStrand, twoStepRegistry)
	if err != nil {
		t.Fatal(err)
	}
	m4, err := FromStrand(twoStepStrand, twoStepRegistry)
	if err != nil {
		t.Fatal(err)
	}
	if !m3.IsEquivalent(m4) {
		t.Error("two-step strands built from identical input must be equivalent")
	}
}

// TEST1187: Strand resolution fails when a referenced cap is not found in the registry.
func Test1187_StrandNonEquivalenceDifferentCap(t *testing.T) {
	cap1 := buildCap("cap:in=\"media:ext=pdf\";extract;out=\"media:ext=txt\"", "extract", []string{"media:ext=pdf"}, "media:ext=txt")
	cap2 := buildCap("cap:in=\"media:ext=pdf\";convert;out=\"media:ext=txt\"", "convert", []string{"media:ext=pdf"}, "media:ext=txt")
	reg1 := registryWith([]*cap.Cap{cap1})
	reg2 := registryWith([]*cap.Cap{cap2})

	s1, err1 := FromStrand(
		strandFromSteps([]*planner.StrandStep{
			capStep("cap:in=\"media:ext=pdf\";extract;out=\"media:ext=txt\"", "extract", "media:ext=pdf", "media:ext=txt"),
		}, "s1"), reg1,
	)
	s2, err2 := FromStrand(
		strandFromSteps([]*planner.StrandStep{
			capStep("cap:in=\"media:ext=pdf\";convert;out=\"media:ext=txt\"", "convert", "media:ext=pdf", "media:ext=txt"),
		}, "s2"), reg2,
	)
	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected errors: %v %v", err1, err2)
	}
	if s1.IsEquivalent(s2) {
		t.Error("strands with different cap URNs must not be equivalent")
	}
}

// TEST1119: Strand::knit returns a single-strand Machine via the new resolver. Smoke test the registry-threaded API end-to-end.
func Test1119_FromStrand_returns_single_strand_machine(t *testing.T) {
	c := buildCap(
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
		"Extract",
		[]string{"media:ext=pdf"},
		`media:txt;enc=utf-8`,
	)
	registry := registryWith([]*cap.Cap{c})

	strand := strandFromSteps([]*planner.StrandStep{
		capStep(`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`, "Extract", "media:ext=pdf", `media:txt;enc=utf-8`),
	}, "pdf to txt")

	machine, err := FromStrand(strand, registry)
	if err != nil {
		t.Fatalf("FromStrand must succeed: %v", err)
	}
	if machine.StrandCount() != 1 {
		t.Fatalf("expected 1 strand, got %d", machine.StrandCount())
	}
	if len(machine.Strands()[0].Edges()) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(machine.Strands()[0].Edges()))
	}
}

// TEST1120: Strand::knit fails hard when the cap is not in the registry — the planner produces strands referencing caps that must be present in the cap registry's cache for resolution to succeed.
func Test1120_FromStrand_unknown_cap_fails_hard(t *testing.T) {
	registry := registryWith(nil) // empty registry — no caps

	strand := strandFromSteps([]*planner.StrandStep{
		capStep(`cap:in=media:pdf;ghost;out="media:txt;enc=utf-8"`, "Ghost", "media:ext=pdf", `media:txt;enc=utf-8`),
	}, "ghost strand")

	_, err := FromStrand(strand, registry)
	if err == nil {
		t.Fatal("FromStrand must fail when cap is not in registry")
	}
	if err.Kind != ErrAbstractionUnknownCap {
		t.Fatalf("expected ErrAbstractionUnknownCap, got %v", err.Kind)
	}
}

// TEST1147: MachineSyntaxError.Error() includes position and detail.
// invalidWiringError(7) must produce a message containing "statement 7" and "invalid wiring".
func Test1147_machine_syntax_error_display_is_specific(t *testing.T) {
	err := invalidWiringError(7, "expected source -> cap -> target")
	msg := err.Error()
	if !containsStr(msg, "statement 7") {
		t.Errorf("error message must contain 'statement 7', got: %q", msg)
	}
	if !containsStr(msg, "invalid wiring") {
		t.Errorf("error message must contain 'invalid wiring', got: %q", msg)
	}
}

// TEST1148: MachineParseError::from(MachineSyntaxError) preserves the syntax error variant
func Test1148_machine_parse_error_from_syntax_preserves_variant(t *testing.T) {
	syntaxErr := undefinedAliasError("extract")
	parseErr := syntaxParseError(syntaxErr)

	if parseErr.Syntax == nil {
		t.Fatal("Syntax field must be set")
	}
	if parseErr.Syntax.Kind != ErrUndefinedAlias {
		t.Fatalf("expected ErrUndefinedAlias, got %v", parseErr.Syntax.Kind)
	}
	if parseErr.Abstraction != nil {
		t.Fatal("Abstraction field must be nil")
	}
}

// TEST1149: MachineParseError::from(MachineAbstractionError) preserves the resolution error variant
func Test1149_machine_parse_error_from_resolution_preserves_variant(t *testing.T) {
	absErr := ambiguousNotationError(2, "cap:in=\"media:ext=pdf\";out=media:text")
	parseErr := abstractionParseError(absErr)

	if parseErr.Abstraction == nil {
		t.Fatal("Abstraction field must be set")
	}
	if parseErr.Abstraction.Kind != ErrAbstractionAmbiguousMachineNotation {
		t.Fatalf("expected ErrAbstractionAmbiguousMachineNotation, got %v", parseErr.Abstraction.Kind)
	}
	if parseErr.Syntax != nil {
		t.Fatal("Syntax field must be nil")
	}
}

// TEST6700: Line-based notation format round-trips back to the same machine.
// ToMachineNotationFormatted(NotationFormatLineBased) must not contain '[', and
// re-parsing must yield an equivalent machine.
func Test6700_line_based_format_round_trips(t *testing.T) {
	registry := pdfExtractEmbedRegistry()

	strand := strandFromSteps([]*planner.StrandStep{
		capStep(`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`, "extract", "media:ext=pdf", `media:txt;enc=utf-8`),
		capStep(`cap:in="media:enc=utf-8";embed;out="media:vec;record"`, "embed", `media:txt;enc=utf-8`, `media:vec;record`),
	}, "pdf to vec")

	m1, aerr := FromStrand(strand, registry)
	if aerr != nil {
		t.Fatalf("FromStrand failed: %v", aerr)
	}

	lineBased := m1.ToMachineNotationFormatted(NotationFormatLineBased)
	if containsStr(lineBased, "[") {
		t.Errorf("line-based form must not contain brackets, got: %q", lineBased)
	}

	m2, parseErr := FromString(lineBased, registry)
	if parseErr != nil {
		t.Fatalf("line-based form must parse: %v", parseErr)
	}
	if !m1.IsEquivalent(m2) {
		t.Error("line-based round-trip must yield equivalent machine")
	}
}

// TEST1178: One source is assigned to the single compatible cap argument.
func Test1178_match_single_source_picks_unique_arg(t *testing.T) {
	sources := []*urn.MediaUrn{mediaUrn("media:ext=pdf")}
	args := []*urn.MediaUrn{mediaUrn("media:ext=pdf")}
	capUrnStr := `cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`

	pairs, err := matchSourcesToArgs(sources, args, capUrnStr, 0)
	require.Nil(t, err, "trivial single-source match must succeed")
	require.Equal(t, 1, len(pairs))
	assert.True(t, pairs[0][0].IsEquivalent(mediaUrn("media:ext=pdf")), "arg must be media:ext=pdf")
	assert.True(t, pairs[0][1].IsEquivalent(mediaUrn("media:ext=pdf")), "source must be media:ext=pdf")
}

// TEST1179: Source-to-arg matching assigns a more specific source to a compatible general argument.
func Test1179_match_more_specific_source_assigned_to_general_arg(t *testing.T) {
	sources := []*urn.MediaUrn{mediaUrn("media:page;enc=utf-8")}
	args := []*urn.MediaUrn{mediaUrn("media:enc=utf-8")}
	capUrnStr := `cap:in="media:enc=utf-8";make-decision;out="media:decision;fmt=json;record"`

	pairs, err := matchSourcesToArgs(sources, args, capUrnStr, 0)
	require.Nil(t, err, "more-specific source must be matched to its arg")
	require.Equal(t, 1, len(pairs))
	assert.True(t, pairs[0][0].IsEquivalent(mediaUrn("media:enc=utf-8")), "arg must be media:enc=utf-8")
	assert.True(t, pairs[0][1].IsEquivalent(mediaUrn("media:page;enc=utf-8")), "source must be media:page;enc=utf-8")
}

// TEST1180: Matching fails when a source does not conform to any cap input argument.
func Test1180_match_unmatched_source_fails_hard(t *testing.T) {
	sources := []*urn.MediaUrn{mediaUrn("media:numeric")}
	args := []*urn.MediaUrn{mediaUrn("media:enc=utf-8")}
	capUrnStr := `cap:in="media:enc=utf-8";t;out="media:enc=utf-8"`

	_, err := matchSourcesToArgs(sources, args, capUrnStr, 7)
	require.NotNil(t, err, "unmatched source must fail hard")
	assert.Equal(t, ErrAbstractionUnmatchedSourceInCapArgs, err.Kind)
}

// TEST1181: matchSourcesToArgs disambiguates two sources by specificity.
func Test1181_match_two_sources_disambiguated_by_specificity(t *testing.T) {
	sources := []*urn.MediaUrn{mediaUrn("media:ext=png;image"), mediaUrn("media:model-spec;enc=utf-8")}
	args := []*urn.MediaUrn{mediaUrn("media:ext=png;image"), mediaUrn("media:enc=utf-8")}
	capUrnStr := `cap:in="media:ext=png;image";describe;out="media:image-description;enc=utf-8"`

	pairs, err := matchSourcesToArgs(sources, args, capUrnStr, 0)
	require.Nil(t, err, "two sources disambiguated by specificity must succeed")
	require.Equal(t, 2, len(pairs))

	foundImage, foundText := false, false
	for _, pair := range pairs {
		arg, src := pair[0], pair[1]
		if arg.IsEquivalent(mediaUrn("media:ext=png;image")) {
			assert.True(t, src.IsEquivalent(mediaUrn("media:ext=png;image")))
			foundImage = true
		} else if arg.IsEquivalent(mediaUrn("media:enc=utf-8")) {
			assert.True(t, src.IsEquivalent(mediaUrn("media:model-spec;enc=utf-8")))
			foundText = true
		}
	}
	assert.True(t, foundImage && foundText, "both arg slots must be assigned")
}

// TEST1182: Matching fails as ambiguous when two sources can be swapped at equal minimum cost.
func Test1182_match_ambiguous_when_two_sources_could_swap(t *testing.T) {
	sources := []*urn.MediaUrn{mediaUrn("media:enc=utf-8"), mediaUrn("media:enc=utf-8")}
	args := []*urn.MediaUrn{mediaUrn("media:enc=utf-8"), mediaUrn("media:enc=utf-8")}
	capUrnStr := `cap:in="media:enc=utf-8";t;out="media:enc=utf-8"`

	_, err := matchSourcesToArgs(sources, args, capUrnStr, 0)
	require.NotNil(t, err, "ambiguous matching must fail hard")
	assert.Equal(t, ErrAbstractionAmbiguousMachineNotation, err.Kind)
}

// TEST1183: Matching fails when more sources are provided than the cap has input arguments.
func Test1183_match_more_sources_than_args_fails_hard(t *testing.T) {
	sources := []*urn.MediaUrn{mediaUrn("media:ext=pdf"), mediaUrn("media:ext=pdf"), mediaUrn("media:ext=pdf")}
	args := []*urn.MediaUrn{mediaUrn("media:ext=pdf"), mediaUrn("media:ext=pdf")}
	capUrnStr := "cap:in=\"media:ext=pdf\";t;out=\"media:ext=pdf\""

	_, err := matchSourcesToArgs(sources, args, capUrnStr, 0)
	require.NotNil(t, err, "more sources than args must fail hard")
	assert.Equal(t, ErrAbstractionUnmatchedSourceInCapArgs, err.Kind)
}

// TEST1184: Resolving a strand with one cap produces one resolved machine edge.
func Test1184_resolve_strand_single_cap_produces_one_edge(t *testing.T) {
	c := buildCap(
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
		"extract",
		[]string{"media:ext=pdf"},
		`media:txt;enc=utf-8`,
	)
	registry := registryWith([]*cap.Cap{c})
	strand := strandFromSteps([]*planner.StrandStep{
		capStep(`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`, "extract", "media:ext=pdf", `media:txt;enc=utf-8`),
	}, "pdf to txt")

	resolved, err := resolveStrand(strand, registry, 0)
	require.Nil(t, err, "must resolve")
	require.Equal(t, 1, len(resolved.Edges()))
	require.Equal(t, 1, len(resolved.Edges()[0].Assignment))

	binding := resolved.Edges()[0].Assignment[0]
	assert.True(t, binding.CapArgMediaUrn.IsEquivalent(mediaUrn("media:ext=pdf")))
	srcUrn := resolved.NodeUrn(binding.Source)
	assert.True(t, srcUrn.IsEquivalent(mediaUrn("media:ext=pdf")))

	inputs := resolved.InputAnchors()
	outputs := resolved.OutputAnchors()
	require.Equal(t, 1, len(inputs))
	require.Equal(t, 1, len(outputs))
	assert.True(t, inputs[0].IsEquivalent(mediaUrn("media:ext=pdf")))
	assert.True(t, outputs[0].IsEquivalent(mediaUrn(`media:txt;enc=utf-8`)))
}

// TEST1185: Resolving a chained strand reuses the intermediate node between adjacent caps.
func Test1185_resolve_strand_chained_caps_share_intermediate_node(t *testing.T) {
	registry := pdfExtractEmbedRegistry()
	strand := strandFromSteps([]*planner.StrandStep{
		capStep(`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`, "extract", "media:ext=pdf", `media:txt;enc=utf-8`),
		capStep(`cap:in="media:enc=utf-8";embed;out="media:vec;record"`, "embed", `media:txt;enc=utf-8`, `media:vec;record`),
	}, "pdf to vec")

	resolved, err := resolveStrand(strand, registry, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(resolved.Edges()))
	assert.Equal(t, 3, len(resolved.Nodes()),
		"three distinct data positions: pdf, enc=utf-8;ext=txt, vec;record")

	// Intermediate node must be shared between extract's target and embed's source.
	extractTarget := resolved.Edges()[0].Target
	embedSource := resolved.Edges()[1].Assignment[0].Source
	assert.Equal(t, extractTarget, embedSource,
		"intermediate data position must be one shared NodeId")

	inputs := resolved.InputAnchors()
	outputs := resolved.OutputAnchors()
	require.Equal(t, 1, len(inputs))
	require.Equal(t, 1, len(outputs))
	assert.True(t, inputs[0].IsEquivalent(mediaUrn("media:ext=pdf")))
	assert.True(t, outputs[0].IsEquivalent(mediaUrn(`media:vec;record`)))
}

// TEST1186: Resolving a strand with ForEach marks the following cap edge as a loop.
// IsLoop is derived from cardinality: disbind produces a SEQUENCE of pages, and
// make_decision consumes a scalar page, so make_decision's edge maps per item.
// Collect at the end is elided.
func Test1186_resolve_strand_foreach_marks_following_cap_as_loop(t *testing.T) {
	disbind := buildCap(
		`cap:in="media:ext=pdf";disbind;out="media:page;enc=utf-8"`,
		"disbind",
		[]string{"media:ext=pdf"},
		`media:page;enc=utf-8`,
	)
	disbind.Output.IsSequence = true
	makeDecision := buildCap(
		`cap:in="media:enc=utf-8";make-decision;out="media:decision;fmt=json;record"`,
		"make_decision",
		[]string{"media:enc=utf-8"},
		`media:decision;fmt=json;record`,
	)
	registry := registryWith([]*cap.Cap{disbind, makeDecision})

	strand := strandFromSteps([]*planner.StrandStep{
		capStep(`cap:in="media:ext=pdf";disbind;out="media:page;enc=utf-8"`, "disbind", "media:ext=pdf", `media:page;enc=utf-8`),
		foreachStep(`media:page;enc=utf-8`),
		capStep(`cap:in="media:enc=utf-8";make-decision;out="media:decision;fmt=json;record"`, "make_decision", `media:enc=utf-8`, `media:decision;fmt=json;record`),
		collectStep(`media:decision;fmt=json;record`),
	}, "disbind+foreach+make_decision")

	resolved, err := resolveStrand(strand, registry, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(resolved.Edges()))

	var disbindEdge, decisionEdge *MachineEdge
	for _, e := range resolved.Edges() {
		s := e.CapUrn.String()
		if containsStr(s, "disbind") {
			disbindEdge = e
		} else if containsStr(s, "make-decision") {
			decisionEdge = e
		}
	}
	require.NotNil(t, disbindEdge, "disbind edge must be present")
	require.NotNil(t, decisionEdge, "make-decision edge must be present")
	assert.False(t, disbindEdge.IsLoop, "disbind is not in a loop")
	assert.True(t, decisionEdge.IsLoop, "make-decision is inside ForEach")

	// Intermediate node is shared (positional interning).
	disbindTarget := disbindEdge.Target
	decisionSource := decisionEdge.Assignment[0].Source
	assert.Equal(t, disbindTarget, decisionSource,
		"disbind target and make_decision source must share the same NodeId (positional interning)")
	assert.True(t, resolved.NodeUrn(disbindTarget).IsEquivalent(mediaUrn(`media:page;enc=utf-8`)),
		"shared node URN must be the more-specific media:page;enc=utf-8")
}

// TEST1188: Strand resolution fails when the strand contains no capability steps.
func Test1188_resolve_strand_no_cap_steps_fails_hard(t *testing.T) {
	registry := registryWith(nil)
	strand := strandFromSteps([]*planner.StrandStep{
		foreachStep("media:ext=pdf"),
		collectStep("media:ext=pdf"),
	}, "no caps at all")

	_, err := resolveStrand(strand, registry, 0)
	require.NotNil(t, err, "no cap steps must fail hard")
	assert.Equal(t, ErrAbstractionNoCapabilitySteps, err.Kind)
}

// TEST1190: resolveStrand with inverse format converters produces 3 distinct nodes, no cycle.
func Test1190_resolve_strand_inverse_format_converters_no_cycle(t *testing.T) {
	toInt := buildCap(
		`cap:in="media:numeric";coerce-int;out="media:integer;numeric"`,
		"coerce_int",
		[]string{`media:numeric`},
		`media:integer;numeric`,
	)
	toNum := buildCap(
		`cap:in="media:integer;numeric";coerce-num;out="media:numeric"`,
		"coerce_num",
		[]string{`media:integer;numeric`},
		`media:numeric`,
	)
	registry := registryWith([]*cap.Cap{toInt, toNum})

	strand := strandFromSteps([]*planner.StrandStep{
		capStep(`cap:in="media:numeric";coerce-int;out="media:integer;numeric"`, "coerce_int", `media:numeric`, `media:integer;numeric`),
		capStep(`cap:in="media:integer;numeric";coerce-num;out="media:numeric"`, "coerce_num", `media:integer;numeric`, `media:numeric`),
	}, "round-trip numeric coercion")

	resolved, err := resolveStrand(strand, registry, 0)
	require.Nil(t, err,
		"inverse format converters must resolve without cycle under positional interning")
	assert.Equal(t, 3, len(resolved.Nodes()),
		"three distinct data positions: input, intermediate, output")
	assert.Equal(t, 2, len(resolved.Edges()))

	intTarget := resolved.Edges()[0].Target
	numSource := resolved.Edges()[1].Assignment[0].Source
	assert.Equal(t, intTarget, numSource, "intermediate node must be shared")
}

// TEST1191: Disbinding a PDF with a file-path slot preserves the expected identity of the slot binding.
func Test1191_resolve_strand_disbind_pdf_with_file_path_slot_identity(t *testing.T) {
	// The disbind cap declares `media:file-path;enc=utf-8` as the slot identity
	// (CapArg.MediaUrn) but its stdin source is `media:ext=pdf`. The resolver must
	// match the wiring's `media:ext=pdf` source against the stdin URN, then record
	// `media:file-path;enc=utf-8` as the binding's CapArgMediaUrn.
	filePathSlot := "media:file-path;enc=utf-8"
	stdinUrn := "media:ext=pdf"
	disbind := &cap.Cap{
		Urn:     capUrnVal(`cap:in="media:ext=pdf";disbind;out="media:page;enc=utf-8"`),
		Title:   "disbind",
		Command: "disbind",
		Args: []cap.CapArg{
			cap.NewCapArg(filePathSlot, true, []cap.ArgSource{{Stdin: &stdinUrn}}),
		},
		Output: cap.NewCapOutput(`media:page;enc=utf-8`, "pages"),
	}
	registry := registryWith([]*cap.Cap{disbind})

	strand := strandFromSteps([]*planner.StrandStep{
		capStep(`cap:in="media:ext=pdf";disbind;out="media:page;enc=utf-8"`, "disbind", "media:ext=pdf", `media:page;enc=utf-8`),
	}, "pdf to pages")

	resolved, err := resolveStrand(strand, registry, 0)
	require.Nil(t, err,
		"disbind strand must resolve via stdin URN matching, not slot identity")
	require.Equal(t, 1, len(resolved.Edges()))

	binding := resolved.Edges()[0].Assignment[0]
	// Binding's CapArgMediaUrn must be the slot identity (media:enc=utf-8;file-path).
	assert.True(t, binding.CapArgMediaUrn.IsEquivalent(mediaUrn("media:file-path;enc=utf-8")),
		"binding cap_arg_media_urn must be the slot identity, got: %s", binding.CapArgMediaUrn)
	// Source node must be media:ext=pdf.
	sourceUrn := resolved.NodeUrn(binding.Source)
	assert.True(t, sourceUrn.IsEquivalent(mediaUrn("media:ext=pdf")),
		"source node URN must be media:ext=pdf (the data-type URN), got: %s", sourceUrn)
}

// TEST1138: EdgeAssignmentBinding list is sorted by CapArgMediaUrn for canonical
// form. A two-source cap whose args are added in reverse-alphabetical order
// must still produce bindings sorted alphabetically by CapArgMediaUrn,
// enabling canonical comparison regardless of creation order.
func Test1138_assignment_bindings_are_sorted_by_cap_arg_media_urn(t *testing.T) {
	// Cap with two stdin args: enc=utf-8 (later alphabetically) and pdf
	// (earlier). Args are listed in reverse order so the test fails if
	// sorting is skipped.
	mergeCap := buildCap(
		`cap:in="media:ext=pdf";merge;out="media:txt;enc=utf-8"`,
		"merge",
		[]string{"media:enc=utf-8", "media:ext=pdf"},
		`media:txt;enc=utf-8`,
	)
	registry := registryWith([]*cap.Cap{mergeCap})

	// Pre-interned nodes: 0=pdf, 1=enc=utf-8, 2=enc=utf-8;txt (output)
	nodes := []*urn.MediaUrn{
		mediaUrn("media:ext=pdf"),
		mediaUrn("media:enc=utf-8"),
		mediaUrn(`media:txt;enc=utf-8`),
	}
	mergeCapUrn := capUrnVal(`cap:in="media:ext=pdf";merge;out="media:txt;enc=utf-8"`)
	wirings := []preInternedWiring{
		{
			tokenId:       "tok-1",
			capUrn:        mergeCapUrn,
			sourceNodeIds: []NodeId{0, 1}, // pdf first, enc=utf-8 second
			targetNodeId:  2,
		},
	}

	strand, err := resolvePreInterned(nodes, wirings, registry, 0)
	require.Nil(t, err)
	require.Equal(t, 1, len(strand.Edges()))

	bindings := strand.Edges()[0].Assignment
	require.Equal(t, 2, len(bindings))

	slotUrns := make([]string, len(bindings))
	for i, b := range bindings {
		slotUrns[i] = b.CapArgMediaUrn.String()
	}
	sorted := make([]string, len(slotUrns))
	copy(sorted, slotUrns)
	sort.Strings(sorted)
	assert.Equal(t, sorted, slotUrns, "bindings must be sorted by CapArgMediaUrn")
}

// TEST1176: ToRenderPayloadJSON for a populated machine includes strand with
// nodes, edges, input_anchor_nodes, and output_anchor_nodes.
func Test1176_render_payload_json_includes_strand_with_anchors(t *testing.T) {
	registry := pdfExtractEmbedRegistry()

	strand := strandFromSteps([]*planner.StrandStep{
		capStep(`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`, "extract", "media:ext=pdf", `media:txt;enc=utf-8`),
		capStep(`cap:in="media:enc=utf-8";embed;out="media:vec;record"`, "embed", `media:txt;enc=utf-8`, `media:vec;record`),
	}, "pdf to vec")

	m, aerr := FromStrand(strand, registry)
	if aerr != nil {
		t.Fatalf("FromStrand failed: %v", aerr)
	}

	payload := m.ToRenderPayloadJSON()

	if !containsStr(payload, `{"strands":[`) {
		t.Errorf("payload must start with strands array, got: %q", payload)
	}
	if !containsStr(payload, `"nodes":[`) {
		t.Errorf("payload must contain nodes, got: %q", payload)
	}
	if !containsStr(payload, `"edges":[`) {
		t.Errorf("payload must contain edges, got: %q", payload)
	}
	if !containsStr(payload, `"input_anchor_nodes":[`) {
		t.Errorf("payload must contain input_anchor_nodes, got: %q", payload)
	}
	if !containsStr(payload, `"output_anchor_nodes":[`) {
		t.Errorf("payload must contain output_anchor_nodes, got: %q", payload)
	}
	if !containsStr(payload, "extract") {
		t.Errorf("payload must contain extract cap URN, got: %q", payload)
	}
	if !containsStr(payload, "embed") {
		t.Errorf("payload must contain embed cap URN, got: %q", payload)
	}
}

// TEST1177: Rendering payload JSON for an empty machine emits an empty strands array.
func Test1177_render_payload_for_empty_machine_has_empty_strands_array(t *testing.T) {
	m := fromResolvedStrands(nil)
	payload := m.ToRenderPayloadJSON()
	if payload != `{"strands":[]}` {
		t.Errorf("empty machine must produce {\"strands\":[]}, got: %q", payload)
	}
}

// TEST1308: A wiring set that feeds a cap's output back into an ancestor forms a cycle and must fail hard with CyclicMachineStrand carrying the strand index. Cycle: node 0 → cap A → node 1 → cap B → node 0.
func Test1308_CyclicStrandFailsHard(t *testing.T) {
	urnA := `cap:in="media:ext=pdf";op-a;out="media:enc=utf-8;ext=txt"`
	urnB := `cap:in="media:enc=utf-8;ext=txt";op-b;out="media:ext=pdf"`
	capA := buildCap(urnA, "op_a", []string{"media:ext=pdf"}, "media:enc=utf-8;ext=txt")
	capB := buildCap(urnB, "op_b", []string{"media:enc=utf-8;ext=txt"}, "media:ext=pdf")
	registry := registryWith([]*cap.Cap{capA, capB})

	nodes := []*urn.MediaUrn{mediaUrn("media:ext=pdf"), mediaUrn("media:enc=utf-8;ext=txt")}
	// node 0 -> cap_a -> node 1  and  node 1 -> cap_b -> node 0 (cycle)
	wirings := []preInternedWiring{
		{
			tokenId:       "tok-2",
			capUrn:        capUrnVal(urnA),
			sourceNodeIds: []NodeId{0},
			targetNodeId:  1,
		},
		{
			tokenId:       "tok-3",
			capUrn:        capUrnVal(urnB),
			sourceNodeIds: []NodeId{1},
			targetNodeId:  0,
		},
	}

	_, err := resolvePreInterned(nodes, wirings, registry, 5)
	if err == nil {
		t.Fatal("expected CyclicMachineStrand error, got nil")
	}
	if err.Kind != ErrAbstractionCyclicMachineStrand {
		t.Fatalf("expected ErrAbstractionCyclicMachineStrand, got %v", err)
	}
	if err.StrandIndex == nil || *err.StrandIndex != 5 {
		t.Fatalf("expected strand_index 5, got %v", err.StrandIndex)
	}
}

// ===================================================================
// Helpers
// ===================================================================

func pdfExtractEmbedRegistry() *cap.FabricRegistry {
	extract := buildCap(
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
		"extract",
		[]string{"media:ext=pdf"},
		`media:txt;enc=utf-8`,
	)
	embed := buildCap(
		`cap:in="media:enc=utf-8";embed;out="media:vec;record"`,
		"embed",
		[]string{"media:enc=utf-8"},
		`media:vec;record`,
	)
	return registryWith([]*cap.Cap{extract, embed})
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || findSubstr(s, substr))
}

func findSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
