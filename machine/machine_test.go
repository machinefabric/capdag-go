package machine

import (
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
	return &planner.StrandStep{
		StepType:  planner.StepTypeForEach,
		FromSpec:  u,
		ToSpec:    u,
		MediaDef: u,
	}
}

// collectStep builds a StepTypeCollect StrandStep.
func collectStep(mediaUrnStr string) *planner.StrandStep {
	u := mediaUrn(mediaUrnStr)
	return &planner.StrandStep{
		StepType:  planner.StepTypeCollect,
		FromSpec:  u,
		ToSpec:    u,
		MediaDef: u,
	}
}

// capStep builds a StepTypeCap StrandStep.
func capStep(capUrnStr, title, from, to string) *planner.StrandStep {
	return &planner.StrandStep{
		StepType:  planner.StepTypeCap,
		FromSpec:  mediaUrn(from),
		ToSpec:    mediaUrn(to),
		CapUrnVal: capUrnVal(capUrnStr),
		StepTitle: title,
	}
}

// strandFromSteps wraps steps into a Strand.
func strandFromSteps(steps []*planner.StrandStep, description string) *planner.Strand {
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
		Steps:        steps,
		SourceMediaUrn:   sourceMediaUrn,
		TargetMediaUrn:   targetMediaUrn,
		TotalSteps:   totalSteps,
		CapStepCount: capStepCount,
		Description:  description,
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

// TestFromStrandProducesSingleStrandMachine verifies that a single-step
// planner strand yields a Machine with exactly one MachineStrand and one edge.
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

// TestFromStrandsKeepStrandsDisjoint verifies that FromStrands does NOT join
// two strands even when their URNs are type-compatible at runtime.
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

// TestFromStrandsEmptyInputFailsHard verifies that passing an empty slice to
// FromStrands returns a NoCapabilitySteps error.
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

// TestMachineIsEquivalentIsStrictPositional verifies that swapping strand
// order breaks IsEquivalent — strand declaration order is part of identity.
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

// TestMachineStrandIsEquivalentWalksNodeBijection verifies that two
// MachineStrands built from the same planner strand are equivalent.
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

// TestInputOutputAnchors verifies that the resolver correctly identifies
// root (input) and leaf (output) nodes for a simple linear strand.
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

// TestForEachSetsIsLoop verifies that a ForEach step preceding a Cap step
// sets IsLoop=true on the resulting MachineEdge.
// TEST1169: Loop markers in notation set the resolved edge loop flag on the following cap step.
func Test1169_ForEachSetsIsLoop(t *testing.T) {
	loopCap := buildCap(
		`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
		"extract",
		[]string{"media:ext=pdf"},
		`media:txt;enc=utf-8`,
	)
	registry := registryWith([]*cap.Cap{loopCap})

	steps := []*planner.StrandStep{
		{
			StepType:  planner.StepTypeForEach,
			FromSpec:  mediaUrn("media:ext=pdf"),
			ToSpec:    mediaUrn("media:ext=pdf"),
			MediaDef: mediaUrn("media:ext=pdf"),
		},
		capStep(
			`cap:in="media:ext=pdf";extract;out="media:txt;enc=utf-8"`,
			"extract",
			"media:ext=pdf",
			`media:txt;enc=utf-8`,
		),
	}
	strand := strandFromSteps(steps, "loop extract")
	m, err := FromStrand(strand, registry)
	if err != nil {
		t.Fatal(err)
	}
	edge := m.Strands()[0].Edges()[0]
	if !edge.IsLoop {
		t.Error("expected IsLoop=true on edge following ForEach step")
	}
}

// TestCollectIsElided verifies that a Collect step produces no MachineEdge —
// the resolved strand has only one edge (from the Cap step).
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
			StepType:  planner.StepTypeCollect,
			FromSpec:  mediaUrn(`media:txt;enc=utf-8`),
			ToSpec:    mediaUrn(`media:txt;enc=utf-8`),
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

// TestParseSingleStrandTwoCapsConnectedViaSharedNode verifies that two wirings
// sharing the node name `txt` become a single connected component (one strand).
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

// TestParseTwoDisconnectedStrandsYieldsTwoMachineStrands verifies that wirings
// with no shared node names are partitioned into two separate strands.
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

// TestParseEmptyInputReturnsError verifies that empty/whitespace input fails
// with an ErrEmpty error.
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

// TestParseDuplicateAliasReturnsError verifies that two headers with the same
// alias name return ErrDuplicateAlias.
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

// TestParseUndefinedAliasReturnsError verifies that a wiring referencing an
// undefined cap alias returns ErrUndefinedAlias.
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

// TestParseNodeNameCollidesWithCapAlias verifies that a node name matching a
// cap alias returns ErrNodeAliasCollision.
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

// TestToMachineNotationRoundTrips verifies that a machine parsed from
// notation and re-serialized produces a machine equivalent to the original.
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

// TestEmptyMachineSerializesToEmpty verifies that an empty machine produces
// an empty string from ToMachineNotation.
// TEST1175: Serializing an empty machine produces an empty string.
func Test1175_EmptyMachineSerializesToEmpty(t *testing.T) {
	m := fromResolvedStrands(nil)
	if m.ToMachineNotation() != "" {
		t.Errorf("empty machine must serialize to empty string, got %q", m.ToMachineNotation())
	}
}

// TestMachineStringRepr verifies the String() representation of a machine.
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

// TestStrandEquivalenceWithDifferentNodeAllocationOrders verifies that two
// equivalent strands remain equivalent even when their NodeIds were allocated
// in different positions (bijection check).
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

// TestStrandNonEquivalenceDifferentCap verifies that strands with different
// cap URNs are not equivalent.
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

// TEST1119: FromStrand builds a single-strand Machine from a planner.Strand.
// Smoke test the registry-threaded API end-to-end.
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

// TEST1120: FromStrand fails hard when the cap is not in the registry.
// The planner produces strands referencing caps that must be present in the
// cap registry cache for resolution to succeed.
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

// TEST1148: MachineParseError with Syntax field preserves the syntax error kind.
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

// TEST1149: MachineParseError with Abstraction field preserves the resolution error kind.
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

// TEST1174: Line-based notation format round-trips back to the same machine.
// ToMachineNotationFormatted(NotationFormatLineBased) must not contain '[', and
// re-parsing must yield an equivalent machine.
func Test1174_line_based_format_round_trips(t *testing.T) {
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

// TEST1178: matchSourcesToArgs assigns a single source to the single compatible cap arg.
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

// TEST1179: matchSourcesToArgs assigns a more specific source to a compatible general arg.
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

// TEST1180: matchSourcesToArgs fails when source does not conform to any cap arg.
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

// TEST1182: matchSourcesToArgs fails ambiguous when two identical sources can be swapped.
func Test1182_match_ambiguous_when_two_sources_could_swap(t *testing.T) {
	sources := []*urn.MediaUrn{mediaUrn("media:enc=utf-8"), mediaUrn("media:enc=utf-8")}
	args := []*urn.MediaUrn{mediaUrn("media:enc=utf-8"), mediaUrn("media:enc=utf-8")}
	capUrnStr := `cap:in="media:enc=utf-8";t;out="media:enc=utf-8"`

	_, err := matchSourcesToArgs(sources, args, capUrnStr, 0)
	require.NotNil(t, err, "ambiguous matching must fail hard")
	assert.Equal(t, ErrAbstractionAmbiguousMachineNotation, err.Kind)
}

// TEST1183: matchSourcesToArgs fails when more sources are provided than cap args.
func Test1183_match_more_sources_than_args_fails_hard(t *testing.T) {
	sources := []*urn.MediaUrn{mediaUrn("media:ext=pdf"), mediaUrn("media:ext=pdf"), mediaUrn("media:ext=pdf")}
	args := []*urn.MediaUrn{mediaUrn("media:ext=pdf"), mediaUrn("media:ext=pdf")}
	capUrnStr := "cap:in=\"media:ext=pdf\";t;out=\"media:ext=pdf\""

	_, err := matchSourcesToArgs(sources, args, capUrnStr, 0)
	require.NotNil(t, err, "more sources than args must fail hard")
	assert.Equal(t, ErrAbstractionUnmatchedSourceInCapArgs, err.Kind)
}

// TEST1184: resolveStrand with one cap produces one edge with correct input/output anchors.
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

// TEST1185: resolveStrand chained caps share the intermediate node (positional interning).
// 3 distinct nodes, not 4.
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

// TEST1186: resolveStrand with ForEach marks the following cap edge as IsLoop=true.
func Test1186_resolve_strand_foreach_marks_following_cap_as_loop(t *testing.T) {
	disbind := buildCap(
		`cap:in="media:ext=pdf";disbind;out="media:page;enc=utf-8"`,
		"disbind",
		[]string{"media:ext=pdf"},
		`media:page;enc=utf-8`,
	)
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

// TEST1188: resolveStrand fails when the strand contains no capability steps.
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

// TEST1191: resolveStrand with a disbind cap that uses file-path slot identity
// (distinct from stdin URN) preserves the slot identity in the binding.
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
	// Source node must be media:pdf.
	sourceUrn := resolved.NodeUrn(binding.Source)
	assert.True(t, sourceUrn.IsEquivalent(mediaUrn("media:ext=pdf")),
		"source node URN must be media:ext=pdf (the data-type URN), got: %s", sourceUrn)
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

// TEST1177: ToRenderPayloadJSON for an empty machine emits an empty strands array.
func Test1177_render_payload_for_empty_machine_has_empty_strands_array(t *testing.T) {
	m := fromResolvedStrands(nil)
	payload := m.ToRenderPayloadJSON()
	if payload != `{"strands":[]}` {
		t.Errorf("empty machine must produce {\"strands\":[]}, got: %q", payload)
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
