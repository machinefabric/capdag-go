package input_resolver

import (
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/media"
)

// createTestRegistry builds a FabricRegistry pre-seeded with a JSON media def
// (the only extension these tests reference). The registry hydrates extensions
// from spec arrival; tests must seed explicitly.
func createTestRegistry(t *testing.T) *media.FabricRegistry {
	t.Helper()
	registry, err := media.NewFabricRegistryForTest()
	if err != nil {
		t.Fatalf("NewFabricRegistryForTest: %v", err)
	}
	registry.AddSpec(media.StoredMediaDef{
		Version:    0,
		Urn:        "media:fmt=json;record",
		MediaType:  "application/json",
		Title:      "JSON",
		Extensions: []string{"json"},
	})
	return registry
}

// TEST1276: Registration of a cap group with non-conflicting adapters succeeds
func Test1276_RegisterNonConflicting(t *testing.T) {
	registry := NewMediaAdapterRegistry(createTestRegistry(t))

	err := registry.RegisterCapGroup(
		"text-formats",
		[]string{"media:fmt=json", "media:fmt=yaml"},
		"txtcartridge",
	)
	if err != nil {
		t.Fatalf("Non-conflicting adapters must register: %v", err)
	}
	if registry.RegisteredCount() != 2 {
		t.Fatalf("expected 2 registered adapters, got %d", registry.RegisteredCount())
	}
}

// TEST1277: Registration of a cap group with an adapter that conforms_to an existing adapter is rejected
func Test1277_RejectConformingOverlap(t *testing.T) {
	registry := NewMediaAdapterRegistry(createTestRegistry(t))

	// Register group A with media:fmt=json.
	if err := registry.RegisterCapGroup("group-a", []string{"media:fmt=json"}, "cartridge-a"); err != nil {
		t.Fatalf("group-a registration failed: %v", err)
	}

	// Try to register group B with media:fmt=json;record (conforms to media:fmt=json).
	err := registry.RegisterCapGroup("group-b", []string{"media:fmt=json;record"}, "cartridge-b")
	if err == nil {
		t.Fatal("Conforming overlap must be rejected")
	}
	if !strings.Contains(err.Error(), "group-b") {
		t.Errorf("Error must name the rejected group: %v", err)
	}
	if !strings.Contains(err.Error(), "group-a") {
		t.Errorf("Error must name the conflicting group: %v", err)
	}
}

// TEST1278: Registration rejects the entire group — no partial registration
func Test1278_RejectEntireGroup(t *testing.T) {
	registry := NewMediaAdapterRegistry(createTestRegistry(t))

	// Register an adapter for media:fmt=json.
	if err := registry.RegisterCapGroup("group-a", []string{"media:fmt=json"}, "cartridge-a"); err != nil {
		t.Fatalf("group-a registration failed: %v", err)
	}

	// Try to register group with 3 adapters, one of which conflicts.
	err := registry.RegisterCapGroup(
		"group-b",
		[]string{
			"media:fmt=yaml", // ok
			"media:fmt=json", // conflicts with media:fmt=json
			"media:fmt=csv",  // ok
		},
		"cartridge-b",
	)
	if err == nil {
		t.Fatal("expected rejection")
	}

	// Only the original adapter should remain.
	if registry.RegisteredCount() != 1 {
		t.Fatalf("Rejected group must not leave partial registrations, got %d", registry.RegisteredCount())
	}
}

// TEST1279: Intra-group conflict (two adapters within same group overlap) is rejected
func Test1279_IntraGroupConflict(t *testing.T) {
	registry := NewMediaAdapterRegistry(createTestRegistry(t))

	err := registry.RegisterCapGroup(
		"bad-group",
		[]string{
			"media:fmt=json",
			"media:fmt=json", // conforms to media:fmt=json
		},
		"cartridge-x",
	)
	if err == nil {
		t.Fatal("Intra-group conflict must be rejected")
	}
	if registry.RegisteredCount() != 0 {
		t.Fatalf("expected 0 registered adapters, got %d", registry.RegisteredCount())
	}
}

// TEST1280: find_adapters_for_extension returns correct cartridge IDs
func Test1280_FindAdaptersForExtension(t *testing.T) {
	registry := NewMediaAdapterRegistry(createTestRegistry(t))

	// Register adapter for media:fmt=json (which should match .json extension candidates).
	if err := registry.RegisterCapGroup("text-group", []string{"media:fmt=json"}, "txtcartridge"); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	results := registry.FindAdaptersForExtension("json")
	if len(results) == 0 {
		t.Fatalf("Must find adapter for json extension (found: %v)", results)
	}
	if results[0].CartridgeID != "txtcartridge" {
		t.Fatalf("expected txtcartridge, got %q", results[0].CartridgeID)
	}
}

// TEST1281: has_adapter_for_extension returns false for unregistered extension
func Test1281_NoAdapterForUnknown(t *testing.T) {
	registry := NewMediaAdapterRegistry(createTestRegistry(t))

	if registry.HasAdapterForExtension("xyz_unknown") {
		t.Fatal("Unknown extension must return false")
	}
}
