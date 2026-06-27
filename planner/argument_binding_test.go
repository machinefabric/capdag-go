package planner

import (
	"encoding/json"
	"testing"
)

func emptyContext(opts ...func(*ArgumentResolutionContext)) *ArgumentResolutionContext {
	ctx := &ArgumentResolutionContext{
		InputFiles:      []*CapInputFile{},
		PreviousOutputs: make(map[string]json.RawMessage),
	}
	for _, opt := range opts {
		opt(ctx)
	}
	return ctx
}

// TEST668: resolve_binding returns byte values when slot is populated with data
func Test668_ResolveSlotWithPopulatedByteSlotValues(t *testing.T) {
	ctx := emptyContext(func(c *ArgumentResolutionContext) {
		c.SlotValues = map[string][]byte{
			"step_0:media:width;numeric": []byte("800"),
		}
	})
	binding := NewSlotBinding("media:width;numeric", nil)
	result, err := ResolveBinding(
		binding, ctx,
		`cap:in="media:ext=pdf";resize;out="media:ext=pdf"`,
		"step_0",
		nil, true,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if string(result.Value) != "800" {
		t.Fatalf("expected value '800', got %q", string(result.Value))
	}
	if result.Source != SourceArgSlot {
		t.Fatalf("expected source Slot, got %d", result.Source)
	}
}

// TEST669: resolve_binding falls back to cap default value when slot has no data
func Test669_ResolveSlotFallsBackToDefault(t *testing.T) {
	ctx := emptyContext()
	binding := NewSlotBinding("media:quality;numeric", nil)
	defaultVal := json.RawMessage(`85`)
	result, err := ResolveBinding(binding, ctx, "cap:compress", "step_0", defaultVal, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	// jsonValueToBytes for number 85 returns "85" (raw JSON)
	if string(result.Value) != "85" {
		t.Fatalf("expected value '85', got %q", string(result.Value))
	}
	if result.Source != SourceArgCapDefault {
		t.Fatalf("expected source CapDefault, got %d", result.Source)
	}
}

// TEST670: resolve_binding returns error when required slot has no value and no default
func Test670_ResolveRequiredSlotNoValueReturnsErr(t *testing.T) {
	ctx := emptyContext()
	binding := NewSlotBinding("media:question;enc=utf-8", nil)
	_, err := ResolveBinding(binding, ctx, "cap:generate", "step_0", nil, true)
	if err == nil {
		t.Fatal("expected error for required slot with no value")
	}
	errStr := err.Error()
	if !contains(errStr, "media:question;enc=utf-8") {
		t.Fatalf("expected error to mention slot name, got: %s", errStr)
	}
}

// TEST671: resolve_binding returns None when optional slot has no value and no default
func Test671_ResolveOptionalSlotNoValueReturnsNone(t *testing.T) {
	ctx := emptyContext()
	binding := NewSlotBinding("media:suffix;enc=utf-8", nil)
	result, err := ResolveBinding(binding, ctx, "cap:rename", "step_0", nil, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatal("expected nil result for optional slot with no value")
	}
}

// TEST1105: Two steps with the same cap_urn get distinct slot values via different node_ids. This is the core disambiguation scenario that step-index keying was designed to solve.
func Test1105_TwoStepsSameCapUrnDifferentSlotValues(t *testing.T) {
	capUrn := `cap:in="media:ext=pdf";make-decision;out="media:bool;enc=utf-8"`
	slotName := "media:list;question;enc=utf-8"
	ctx := emptyContext(func(c *ArgumentResolutionContext) {
		c.SlotValues = map[string][]byte{
			"step_0:" + slotName: []byte("Is this a contract?"),
			"step_2:" + slotName: []byte("Is this confidential?"),
		}
	})
	binding := NewSlotBinding(slotName, nil)

	// step_0 resolves to "Is this a contract?"
	r0, err := ResolveBinding(binding, ctx, capUrn, "step_0", nil, true)
	if err != nil {
		t.Fatalf("step_0: unexpected error: %v", err)
	}
	if string(r0.Value) != "Is this a contract?" {
		t.Fatalf("step_0: expected 'Is this a contract?', got %q", string(r0.Value))
	}
	if r0.Source != SourceArgSlot {
		t.Fatalf("step_0: expected source Slot, got %d", r0.Source)
	}

	// step_2 resolves to "Is this confidential?"
	r2, err := ResolveBinding(binding, ctx, capUrn, "step_2", nil, true)
	if err != nil {
		t.Fatalf("step_2: unexpected error: %v", err)
	}
	if string(r2.Value) != "Is this confidential?" {
		t.Fatalf("step_2: expected 'Is this confidential?', got %q", string(r2.Value))
	}
	if r2.Source != SourceArgSlot {
		t.Fatalf("step_2: expected source Slot, got %d", r2.Source)
	}

	// Confirm they differ
	if string(r0.Value) == string(r2.Value) {
		t.Fatal("step_0 and step_2 must resolve to different values")
	}
}

// TEST1106: Slot resolution falls through to cap_settings when no slot_value exists. cap_settings are keyed by cap_urn (shared across steps), so both steps get the same value.
func Test1106_SlotFallsThroughToCapSettingsShared(t *testing.T) {
	capUrn := `cap:in="media:ext=pdf";make-decision;out="media:bool;enc=utf-8"`
	slotName := "media:language;enc=utf-8"
	ctx := emptyContext(func(c *ArgumentResolutionContext) {
		c.CapSettings = map[string]map[string]json.RawMessage{
			capUrn: {
				slotName: json.RawMessage(`"en"`),
			},
		}
	})
	binding := NewSlotBinding(slotName, nil)

	// Both steps fall through to cap_settings — same value
	r0, err := ResolveBinding(binding, ctx, capUrn, "step_0", nil, false)
	if err != nil {
		t.Fatalf("step_0: unexpected error: %v", err)
	}
	r1, err := ResolveBinding(binding, ctx, capUrn, "step_1", nil, false)
	if err != nil {
		t.Fatalf("step_1: unexpected error: %v", err)
	}
	if string(r0.Value) != "en" {
		t.Fatalf("step_0: expected 'en', got %q", string(r0.Value))
	}
	if string(r1.Value) != "en" {
		t.Fatalf("step_1: expected 'en', got %q", string(r1.Value))
	}
	if r0.Source != SourceArgCapSetting {
		t.Fatalf("step_0: expected source CapSetting, got %d", r0.Source)
	}
	if r1.Source != SourceArgCapSetting {
		t.Fatalf("step_1: expected source CapSetting, got %d", r1.Source)
	}
}

// TEST1107: step_0 has a slot_value override, step_1 falls through to cap_settings. Proves per-step override works while shared settings remain as fallback.
func Test1107_SlotValueOverridesCapSettingsPerStep(t *testing.T) {
	capUrn := `cap:in="media:ext=pdf";make-decision;out="media:bool;enc=utf-8"`
	slotName := "media:language;enc=utf-8"
	ctx := emptyContext(func(c *ArgumentResolutionContext) {
		c.SlotValues = map[string][]byte{
			"step_0:" + slotName: []byte("fr"),
			// step_1 has no slot_value entry
		}
		c.CapSettings = map[string]map[string]json.RawMessage{
			capUrn: {
				slotName: json.RawMessage(`"en"`),
			},
		}
	})
	binding := NewSlotBinding(slotName, nil)

	// step_0: slot_value "fr" (priority 1)
	r0, err := ResolveBinding(binding, ctx, capUrn, "step_0", nil, false)
	if err != nil {
		t.Fatalf("step_0: unexpected error: %v", err)
	}
	if string(r0.Value) != "fr" {
		t.Fatalf("step_0: expected 'fr', got %q", string(r0.Value))
	}
	if r0.Source != SourceArgSlot {
		t.Fatalf("step_0: expected source Slot, got %d", r0.Source)
	}

	// step_1: no slot_value → falls to cap_settings "en" (priority 2)
	r1, err := ResolveBinding(binding, ctx, capUrn, "step_1", nil, false)
	if err != nil {
		t.Fatalf("step_1: unexpected error: %v", err)
	}
	if string(r1.Value) != "en" {
		t.Fatalf("step_1: expected 'en', got %q", string(r1.Value))
	}
	if r1.Source != SourceArgCapSetting {
		t.Fatalf("step_1: expected source CapSetting, got %d", r1.Source)
	}
}

// TEST1108: ResolveAll with node_id threads correctly through to each binding.
func Test1108_ResolveAllPassesNodeID(t *testing.T) {
	ctx := emptyContext(func(c *ArgumentResolutionContext) {
		c.SlotValues = map[string][]byte{
			"step_3:media:width;numeric":   []byte("1024"),
			"step_3:media:quality;numeric": []byte("95"),
		}
	})

	bindings := NewArgumentBindings()
	bindings.Add("media:width;numeric",
		NewSlotBinding("media:width;numeric", nil))
	bindings.Add("media:quality;numeric",
		NewSlotBinding("media:quality;numeric", nil))

	results, err := bindings.ResolveAll(ctx, "cap:resize", "step_3", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	byName := make(map[string]*ResolvedArgument)
	for _, r := range results {
		byName[r.Name] = r
	}

	width := byName["media:width;numeric"]
	if width == nil {
		t.Fatal("missing width result")
	}
	if string(width.Value) != "1024" {
		t.Fatalf("width: expected '1024', got %q", string(width.Value))
	}
	if width.Source != SourceArgSlot {
		t.Fatalf("width: expected source Slot, got %d", width.Source)
	}

	quality := byName["media:quality;numeric"]
	if quality == nil {
		t.Fatal("missing quality result")
	}
	if string(quality.Value) != "95" {
		t.Fatalf("quality: expected '95', got %q", string(quality.Value))
	}
	if quality.Source != SourceArgSlot {
		t.Fatalf("quality: expected source Slot, got %d", quality.Source)
	}
}

// TEST1109: Slot key uses node_id, NOT cap_urn — a slot_value keyed by cap_urn must not match.
func Test1109_SlotKeyUsesNodeIDNotCapUrn(t *testing.T) {
	capUrn := `cap:in="media:ext=pdf";resize;out="media:ext=pdf"`
	slotName := "media:width;numeric"
	// Deliberately key by cap_urn (the OLD format) — should NOT match
	ctx := emptyContext(func(c *ArgumentResolutionContext) {
		c.SlotValues = map[string][]byte{
			capUrn + ":" + slotName: []byte("800"),
		}
	})
	binding := NewSlotBinding(slotName, nil)

	// Should NOT find the value because the key format is wrong (cap_urn instead of node_id)
	result, err := ResolveBinding(binding, ctx, capUrn, "step_0", nil, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatal("Old cap_urn-based key must not match node_id-based lookup")
	}
}

// TEST6659: ArgumentBinding RequiresInput distinguishes Slots from Literals
func Test6659_ArgumentBindingRequiresInput(t *testing.T) {
	slot := NewSlotBinding("width", nil)
	if !slot.RequiresInput() {
		t.Fatal("Slot binding must require input")
	}
	lit := NewLiteralBinding(json.RawMessage(`100`))
	if lit.RequiresInput() {
		t.Fatal("Literal binding must NOT require input")
	}
}

// TEST6660: ArgumentBinding PreviousOutput serializes/deserializes correctly
func Test6660_ArgumentBindingSerializationPreviousOutput(t *testing.T) {
	field := "result_path"
	binding := NewPreviousOutputBinding("node_0", &field)
	data, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	s := string(data)
	if !stringContains(s, "previous_output") {
		t.Fatalf("expected 'previous_output' in JSON: %s", s)
	}
	if !stringContains(s, "node_0") {
		t.Fatalf("expected 'node_0' in JSON: %s", s)
	}
	var recovered ArgumentBinding
	if err := json.Unmarshal(data, &recovered); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if recovered.Kind != BindingPreviousOutput {
		t.Fatal("expected PreviousOutput kind")
	}
	if recovered.NodeID != "node_0" {
		t.Fatalf("expected node_id 'node_0', got %q", recovered.NodeID)
	}
	if recovered.OutputField == nil || *recovered.OutputField != "result_path" {
		t.Fatalf("expected output_field 'result_path', got %v", recovered.OutputField)
	}
}

// TEST6661: ArgumentBindings AddFilePath adds InputFilePath binding
func Test6661_ArgumentBindingsAddFilePath(t *testing.T) {
	bindings := NewArgumentBindings()
	bindings.AddFilePath("input")
	b, ok := bindings.Bindings["input"]
	if !ok {
		t.Fatal("expected binding for 'input'")
	}
	if b.Kind != BindingInputFilePath {
		t.Fatalf("expected InputFilePath binding kind, got %v", b.Kind)
	}
}

// TEST6662: ArgumentBindings identifies unresolved Slot bindings
func Test6662_ArgumentBindingsUnresolvedSlots(t *testing.T) {
	bindings := NewArgumentBindings()
	bindings.Add("width", NewSlotBinding("width", nil))
	bindings.Add("height", NewLiteralBinding(json.RawMessage(`100`)))

	if !bindings.HasUnresolvedSlots() {
		t.Fatal("expected HasUnresolvedSlots to be true")
	}
	unresolved := bindings.GetUnresolvedSlots()
	if len(unresolved) != 1 || unresolved[0] != "width" {
		t.Fatalf("expected ['width'], got %v", unresolved)
	}
}

// TEST6663: resolve_binding resolves InputFilePath to current file path
func Test6663_ResolveInputFilePath(t *testing.T) {
	ctx := emptyContext(func(c *ArgumentResolutionContext) {
		c.InputFiles = []*CapInputFile{
			{FilePath: "/path/to/file.pdf", MediaUrn: "media:ext=pdf"},
		}
	})
	binding := NewInputFilePathBinding()
	result, err := ResolveBinding(binding, ctx, "cap:test", "step_0", nil, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if string(result.Value) != "/path/to/file.pdf" {
		t.Fatalf("expected path '/path/to/file.pdf', got %q", string(result.Value))
	}
	if result.Source != SourceArgInputFile {
		t.Fatalf("expected SourceArgInputFile, got %v", result.Source)
	}
}

// TEST6664: resolve_binding resolves Literal to JSON-encoded bytes
func Test6664_ResolveLiteral(t *testing.T) {
	ctx := emptyContext()
	binding := NewLiteralBinding(json.RawMessage(`42`))
	result, err := ResolveBinding(binding, ctx, "cap:test", "step_0", nil, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if string(result.Value) != "42" {
		t.Fatalf("expected '42', got %q", string(result.Value))
	}
	if result.Source != SourceArgLiteral {
		t.Fatalf("expected SourceArgLiteral, got %v", result.Source)
	}
}

// TEST6665: resolve_binding extracts value from previous node output
func Test6665_ResolvePreviousOutput(t *testing.T) {
	field := "result_path"
	ctx := emptyContext(func(c *ArgumentResolutionContext) {
		c.PreviousOutputs = map[string]json.RawMessage{
			"node_0": json.RawMessage(`{"result_path": "/output/result.png"}`),
		}
	})
	binding := NewPreviousOutputBinding("node_0", &field)
	result, err := ResolveBinding(binding, ctx, "cap:test", "step_0", nil, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if string(result.Value) != "/output/result.png" {
		t.Fatalf("expected '/output/result.png', got %q", string(result.Value))
	}
	if result.Source != SourceArgPreviousOutput {
		t.Fatalf("expected SourceArgPreviousOutput, got %v", result.Source)
	}
}

// TEST6666: StrandInput single constructor creates valid Single cardinality input
func Test6666_StrandInputSingle(t *testing.T) {
	file := &CapInputFile{FilePath: "/path/to/file.pdf", MediaUrn: "media:ext=pdf"}
	input := NewSingleStrandInput(file)
	if len(input.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(input.Files))
	}
	if input.Cardinality != CardinalitySingle {
		t.Fatal("expected Single cardinality")
	}
	if !input.IsValid() {
		t.Fatal("expected IsValid() to be true")
	}
}

// TEST6667: StrandInput sequence constructor creates valid Sequence cardinality input
func Test6667_StrandInputSequence(t *testing.T) {
	files := []*CapInputFile{
		{FilePath: "/path/1.pdf", MediaUrn: "media:ext=pdf"},
		{FilePath: "/path/2.pdf", MediaUrn: "media:ext=pdf"},
	}
	input := NewSequenceStrandInput(files, "media:ext=pdf")
	if len(input.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(input.Files))
	}
	if input.Cardinality != CardinalitySequence {
		t.Fatal("expected Sequence cardinality")
	}
	if !input.IsValid() {
		t.Fatal("expected IsValid() to be true")
	}
}

// TEST6668: CapInputFile deserializes from JSON with source metadata fields
func Test6668_CapInputFileDeserializationWithSourceMetadata(t *testing.T) {
	jsonStr := `[{"file_path":"/Users/bahram/ws/prj/machinefabric/pdfcartridge/test_files/aws_in_action.pdf","media_urn":"media:ext=pdf","source_id":"1b964d3b-f409-4f51-8684-884348ec2501","source_type":"listing"}]`
	var files []CapInputFile
	if err := json.Unmarshal([]byte(jsonStr), &files); err != nil {
		t.Fatalf("deserialization failed: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].SourceType == nil || *files[0].SourceType != SourceListing {
		t.Fatalf("expected SourceListing, got %v", files[0].SourceType)
	}
}

// TEST6669: CapInputFile deserializes from compact JSON
func Test6669_CapInputFileDeserializationCompact(t *testing.T) {
	jsonStr := `[{"file_path":"/path/to/file.pdf","media_urn":"media:ext=pdf","source_id":"abc123","source_type":"listing"}]`
	var files []CapInputFile
	if err := json.Unmarshal([]byte(jsonStr), &files); err != nil {
		t.Fatalf("deserialization failed: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
}

// TEST6670: StrandInput validation detects mismatched Single cardinality with multiple files
func Test6670_StrandInputInvalidSingle(t *testing.T) {
	files := []*CapInputFile{
		{FilePath: "/path/1.pdf", MediaUrn: "media:ext=pdf"},
		{FilePath: "/path/2.pdf", MediaUrn: "media:ext=pdf"},
	}
	input := &StrandInput{
		Files:           files,
		ExpectedMediaUrn: "media:ext=pdf",
		Cardinality:     CardinalitySingle,
	}
	if input.IsValid() {
		t.Fatal("expected IsValid() to be false for Single with multiple files")
	}
}

// TEST6679: NewCapInputFile creates a CapInputFile with correct path and media URN.
// Metadata and source fields must be nil.
func Test6679_cap_input_file_new(t *testing.T) {
	file := NewCapInputFile("/path/to/file.pdf", "media:ext=pdf")
	if file.FilePath != "/path/to/file.pdf" {
		t.Errorf("FilePath = %q, want /path/to/file.pdf", file.FilePath)
	}
	if file.MediaUrn != "media:ext=pdf" {
		t.Errorf("MediaUrn = %q, want media:ext=pdf", file.MediaUrn)
	}
	if file.Metadata != nil {
		t.Error("Metadata must be nil")
	}
	if file.SourceID != nil {
		t.Error("SourceID must be nil")
	}
}

// TEST6680: CapInputFileFromListing sets source_id and source_type to Listing.
func Test6680_cap_input_file_from_listing(t *testing.T) {
	file := CapInputFileFromListing("listing-123", "/path/to/file.pdf", "media:ext=pdf")
	if file.SourceID == nil || *file.SourceID != "listing-123" {
		t.Errorf("SourceID = %v, want listing-123", file.SourceID)
	}
	if file.SourceType == nil || *file.SourceType != SourceListing {
		t.Errorf("SourceType = %v, want SourceListing", file.SourceType)
	}
}

// TEST6681: CapInputFile.Filename() extracts the basename from a full path.
func Test6681_cap_input_file_filename(t *testing.T) {
	file := NewCapInputFile("/path/to/document.pdf", "media:ext=pdf")
	name := file.Filename()
	if name == nil {
		t.Fatal("Filename() must not return nil for a valid path")
	}
	if *name != "document.pdf" {
		t.Errorf("Filename() = %q, want document.pdf", *name)
	}
}

// TEST6682: NewLiteralStringBinding creates a Literal binding wrapping a JSON string.
func Test6682_argument_binding_literal_string(t *testing.T) {
	binding := NewLiteralStringBinding("test")
	if binding.Kind != BindingLiteral {
		t.Fatalf("expected BindingLiteral, got %v", binding.Kind)
	}
	// Value must be JSON encoding of "test" → `"test"`
	var s string
	if err := json.Unmarshal(binding.Value, &s); err != nil {
		t.Fatalf("failed to unmarshal value: %v", err)
	}
	if s != "test" {
		t.Errorf("literal value = %q, want test", s)
	}
}

// contains is a simple string-contains helper for tests.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && stringContains(s, substr))
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
