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

// TEST668: Resolve slot with populated byte slot_values using step-index key
func TestTest668ResolveSlotWithPopulatedByteSlotValues(t *testing.T) {
	ctx := emptyContext(func(c *ArgumentResolutionContext) {
		c.SlotValues = map[string][]byte{
			"step_0:media:width;textable;numeric": []byte("800"),
		}
	})
	binding := NewSlotBinding("media:width;textable;numeric", nil)
	result, err := ResolveBinding(
		binding, ctx,
		`cap:in="media:pdf";op=resize;out="media:pdf"`,
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

// TEST669: Resolve slot falls back to default when no slot_value or cap_setting
func TestTest669ResolveSlotFallsBackToDefault(t *testing.T) {
	ctx := emptyContext()
	binding := NewSlotBinding("media:quality;textable;numeric", nil)
	defaultVal := json.RawMessage(`85`)
	result, err := ResolveBinding(binding, ctx, "cap:op=compress", "step_0", defaultVal, false)
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

// TEST670: Required slot with no value returns error
func TestTest670ResolveRequiredSlotNoValueReturnsErr(t *testing.T) {
	ctx := emptyContext()
	binding := NewSlotBinding("media:question;textable", nil)
	_, err := ResolveBinding(binding, ctx, "cap:op=generate", "step_0", nil, true)
	if err == nil {
		t.Fatal("expected error for required slot with no value")
	}
	errStr := err.Error()
	if !contains(errStr, "media:question;textable") {
		t.Fatalf("expected error to mention slot name, got: %s", errStr)
	}
}

// TEST671: Optional slot with no value returns nil
func TestTest671ResolveOptionalSlotNoValueReturnsNone(t *testing.T) {
	ctx := emptyContext()
	binding := NewSlotBinding("media:suffix;textable", nil)
	result, err := ResolveBinding(binding, ctx, "cap:op=rename", "step_0", nil, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatal("expected nil result for optional slot with no value")
	}
}

// TEST1105: Two steps with the same cap_urn get distinct slot values via different node_ids.
// This is the core disambiguation scenario that step-index keying was designed to solve.
func TestTest1105TwoStepsSameCapUrnDifferentSlotValues(t *testing.T) {
	capUrn := `cap:in="media:pdf";op=make_decision;out="media:bool;textable"`
	slotName := "media:list;question;textable"
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

// TEST1106: Slot resolution falls through to cap_settings when no slot_value exists.
// cap_settings are keyed by cap_urn (shared across steps), so both steps get the same value.
func TestTest1106SlotFallsThroughToCapSettingsShared(t *testing.T) {
	capUrn := `cap:in="media:pdf";op=make_decision;out="media:bool;textable"`
	slotName := "media:language;textable"
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

// TEST1107: step_0 has a slot_value override, step_1 falls through to cap_settings.
// Proves per-step override works while shared settings remain as fallback.
func TestTest1107SlotValueOverridesCapSettingsPerStep(t *testing.T) {
	capUrn := `cap:in="media:pdf";op=make_decision;out="media:bool;textable"`
	slotName := "media:language;textable"
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
func TestTest1108ResolveAllPassesNodeID(t *testing.T) {
	ctx := emptyContext(func(c *ArgumentResolutionContext) {
		c.SlotValues = map[string][]byte{
			"step_3:media:width;textable;numeric":   []byte("1024"),
			"step_3:media:quality;textable;numeric":  []byte("95"),
		}
	})

	bindings := NewArgumentBindings()
	bindings.Add("media:width;textable;numeric",
		NewSlotBinding("media:width;textable;numeric", nil))
	bindings.Add("media:quality;textable;numeric",
		NewSlotBinding("media:quality;textable;numeric", nil))

	results, err := bindings.ResolveAll(ctx, "cap:op=resize", "step_3", nil, nil)
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

	width := byName["media:width;textable;numeric"]
	if width == nil {
		t.Fatal("missing width result")
	}
	if string(width.Value) != "1024" {
		t.Fatalf("width: expected '1024', got %q", string(width.Value))
	}
	if width.Source != SourceArgSlot {
		t.Fatalf("width: expected source Slot, got %d", width.Source)
	}

	quality := byName["media:quality;textable;numeric"]
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
func TestTest1109SlotKeyUsesNodeIDNotCapUrn(t *testing.T) {
	capUrn := `cap:in="media:pdf";op=resize;out="media:pdf"`
	slotName := "media:width;textable;numeric"
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
