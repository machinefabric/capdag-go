package input_resolver

import (
	"strings"
	"testing"
)

// specialAdapter detects "special" values and adds a marker.
type specialAdapter struct{}

func (specialAdapter) Name() string { return "SpecialAdapter" }

func (specialAdapter) Refine(baseMediaUrn, value string) *ValueAdapterResult {
	if strings.Contains(value, "special") {
		return &ValueAdapterResult{MediaUrn: baseMediaUrn + ";refined"}
	}
	return nil
}

// specificAdapter is a more specific adapter for a longer prefix.
type specificAdapter struct{}

func (specificAdapter) Name() string { return "SpecificAdapter" }

func (specificAdapter) Refine(_ string, _ string) *ValueAdapterResult {
	return &ValueAdapterResult{MediaUrn: "media:specific;result"}
}

// testValueAdapter adds a "special" marker to any URN containing "test" when the
// value contains "special".
type testValueAdapter struct{}

func (testValueAdapter) Name() string { return "TestValueAdapter" }

func (testValueAdapter) Refine(baseMediaUrn, value string) *ValueAdapterResult {
	if !strings.Contains(baseMediaUrn, "test") {
		return nil
	}
	if strings.Contains(value, "special") {
		return &ValueAdapterResult{MediaUrn: baseMediaUrn + ";special"}
	}
	return nil
}

// TEST1221: Matching value adapters refine the base media URN when the value fits.
func Test1221_RefineWithMatchingAdapter(t *testing.T) {
	registry := NewValueAdapterRegistry()
	registry.Register("media:test", specialAdapter{})

	// The adapter is keyed by the `media:test` prefix; the base URN starts
	// with that prefix, so the adapter fires and appends `;refined`.
	result := registry.RefineMediaUrn("media:test;enc=utf-8", "a-special-value")
	if result != "media:test;enc=utf-8;refined" {
		t.Fatalf("got %q", result)
	}
}

// TEST1222: Base URNs without a registered adapter are returned unchanged.
func Test1222_RefineNoMatchingAdapter(t *testing.T) {
	registry := NewValueAdapterRegistry()
	registry.Register("media:test", specialAdapter{})

	result := registry.RefineMediaUrn("media:other;enc=utf-8", "a-special-value")
	if result != "media:other;enc=utf-8" {
		t.Fatalf("got %q", result)
	}
}

// TEST1223: Adapters that decline to refine leave the original media URN intact.
func Test1223_RefineAdapterReturnsNone(t *testing.T) {
	registry := NewValueAdapterRegistry()
	registry.Register("media:test", specialAdapter{})

	result := registry.RefineMediaUrn("media:test;enc=utf-8", "ordinary-value")
	if result != "media:test;enc=utf-8" {
		t.Fatalf("got %q", result)
	}
}

// TEST1224: When multiple adapter prefixes match, the longest prefix wins.
func Test1224_RefineLongestPrefixMatch(t *testing.T) {
	registry := NewValueAdapterRegistry()
	registry.Register("media:test", specialAdapter{})
	registry.Register("media:test;specific", specificAdapter{})

	// "media:test;specific;foo" matches both prefixes, but "media:test;specific" is longer.
	result := registry.RefineMediaUrn("media:test;specific;foo", "any-value")
	if result != "media:specific;result" {
		t.Fatalf("got %q", result)
	}
}

// TEST1225: An empty value adapter registry returns the input media URN unchanged.
func Test1225_EmptyRegistry(t *testing.T) {
	registry := NewValueAdapterRegistry()
	result := registry.RefineMediaUrn("media:anything", "any-value")
	if result != "media:anything" {
		t.Fatalf("got %q", result)
	}
}

// TEST1226: Adapter presence checks report only the prefixes that were registered.
func Test1226_HasAdapter(t *testing.T) {
	registry := NewValueAdapterRegistry()
	registry.Register("media:test", specialAdapter{})

	if !registry.HasAdapter("media:test") {
		t.Error("expected has_adapter(media:test) to be true")
	}
	if registry.HasAdapter("media:other") {
		t.Error("expected has_adapter(media:other) to be false")
	}
}

// TEST1228: Value adapters can append a more specific marker when both base URN and value match.
func Test1228_ValueAdapterRefineMatch(t *testing.T) {
	adapter := testValueAdapter{}
	result := adapter.Refine("media:enc=utf-8;test", "something-special")
	if result == nil {
		t.Fatal("expected non-nil refinement")
	}
	if result.MediaUrn != "media:enc=utf-8;test;special" {
		t.Fatalf("got %q", result.MediaUrn)
	}
}

// TEST1229: Value adapters return no refinement when the base media URN is outside their domain.
func Test1229_ValueAdapterRefineNoMatchBase(t *testing.T) {
	adapter := testValueAdapter{}
	result := adapter.Refine("media:enc=utf-8;other", "something-special")
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

// TEST1230: Value adapters return no refinement when the inspected value does not match.
func Test1230_ValueAdapterRefineNoMatchValue(t *testing.T) {
	adapter := testValueAdapter{}
	result := adapter.Refine("media:enc=utf-8;test", "ordinary-value")
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}
