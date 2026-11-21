package input_resolver

import (
	"context"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

// createTestRegistry builds a FabricRegistry whose manifest names one file
// media type, a JSON file (the only extension these tests reference), cached.
func createTestRegistry(t *testing.T) *fabric.FabricRegistry {
	t.Helper()
	registry := fabric.NewForTest()
	registry.AddSpec(media.StoredMediaDef{
		Urn:        "media:enc=utf8;ext=json;fmt=json;record",
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
		[]string{"media:enc=utf8;fmt=json", "media:enc=utf8;fmt=yaml"},
		"txtcartridge",
	)
	if err != nil {
		t.Fatalf("Non-conflicting adapters must register: %v", err)
	}
	if registry.RegisteredCount() != 2 {
		t.Fatalf("expected 2 registered adapters, got %d", registry.RegisteredCount())
	}
}

// TEST1478: exact re-registration is idempotent — the SAME cartridge
// re-registering the SAME group with the SAME adapter URNs (a cartridge
// attached through two hosting routes, e.g. app-bundled AND
// system-installed) is a no-op, not a self-conflict and not duplicate rows.
// A DIFFERENT cartridge claiming the same URN stays rejected.
func Test1478_ExactReregistrationIsIdempotent(t *testing.T) {
	registry := NewMediaAdapterRegistry(createTestRegistry(t))

	urns := []string{"media:enc=utf8;fmt=json", "media:enc=utf8;fmt=yaml"}
	if err := registry.RegisterCapGroup("text-formats", urns, "txtcartridge"); err != nil {
		t.Fatalf("first registration failed: %v", err)
	}
	if err := registry.RegisterCapGroup("text-formats", urns, "txtcartridge"); err != nil {
		t.Fatalf("re-registering the identical group must be a no-op: %v", err)
	}
	if registry.RegisteredCount() != 2 {
		t.Fatalf("re-registration must not duplicate adapter rows: got %d", registry.RegisteredCount())
	}

	// A partially-new group from the same owner registers only the new URN.
	extended := []string{"media:enc=utf8;fmt=json", "media:fmt=toml"}
	if err := registry.RegisterCapGroup("text-formats", extended, "txtcartridge"); err != nil {
		t.Fatalf("known URNs skip, new URNs register: %v", err)
	}
	if registry.RegisteredCount() != 3 {
		t.Fatalf("expected 3 registered adapters, got %d", registry.RegisteredCount())
	}

	// Another cartridge claiming an identical URN is still ambiguity.
	if err := registry.RegisterCapGroup("text-formats", []string{"media:enc=utf8;fmt=json"}, "other"); err == nil {
		t.Fatal("an identical URN from a DIFFERENT cartridge must stay rejected")
	}
}

// TEST1277: Registration of a cap group with an adapter that conforms_to an existing adapter is rejected
func Test1277_RejectConformingOverlap(t *testing.T) {
	registry := NewMediaAdapterRegistry(createTestRegistry(t))

	// Register group A with media:enc=utf8;fmt=json.
	if err := registry.RegisterCapGroup("group-a", []string{"media:enc=utf8;fmt=json"}, "cartridge-a"); err != nil {
		t.Fatalf("group-a registration failed: %v", err)
	}

	// Try to register group B with media:enc=utf8;fmt=json;record (conforms to media:enc=utf8;fmt=json).
	err := registry.RegisterCapGroup("group-b", []string{"media:enc=utf8;fmt=json;record"}, "cartridge-b")
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

	// Register an adapter for media:enc=utf8;fmt=json.
	if err := registry.RegisterCapGroup("group-a", []string{"media:enc=utf8;fmt=json"}, "cartridge-a"); err != nil {
		t.Fatalf("group-a registration failed: %v", err)
	}

	// Try to register group with 3 adapters, one of which conflicts.
	err := registry.RegisterCapGroup(
		"group-b",
		[]string{
			"media:enc=utf8;fmt=yaml", // ok
			"media:enc=utf8;fmt=json", // conflicts with media:enc=utf8;fmt=json
			"media:enc=utf8;fmt=csv",  // ok
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
			"media:enc=utf8;fmt=json",
			"media:enc=utf8;fmt=json", // conforms to media:enc=utf8;fmt=json
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

	// Register adapter for media:enc=utf8;fmt=json (which should match .json extension candidates).
	if err := registry.RegisterCapGroup("text-group", []string{"media:enc=utf8;fmt=json"}, "txtcartridge"); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	results, err := registry.FindAdaptersForExtension("json")
	if err != nil {
		t.Fatalf("the fabric's file media types load: %v", err)
	}
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

	has, err := registry.HasAdapterForExtension("xyz_unknown")
	if err != nil {
		t.Fatalf("the fabric's file media types load: %v", err)
	}
	if has {
		t.Fatal("Unknown extension must return false")
	}
}

// claimsOne is a cartridge that claims every file as one media type, counting
// how often it is asked.
type claimsOne struct {
	claim string
	calls int
}

func (c *claimsOne) InvokeAdapterSelection(_ context.Context, _, _ string) ([]string, error) {
	c.calls++
	return []string{c.claim}, nil
}

// TEST1318: A cartridge with two incomparable adapters covering candidates of
// one extension is returned once per adapter, so detection hands the model
// every adapter that owns a candidate: asked once, the cartridge claims only
// the record type, and the list candidate under its second adapter is owned,
// unclaimed, and does not stand.
func Test1318_AdaptersForCandidatesReturnsEveryOwningAdapter(t *testing.T) {
	registry := NewMediaAdapterRegistry(createTestRegistry(t))
	record := "media:enc=utf8;ext=json;fmt=json;record"
	list := "media:enc=utf8;ext=json;fmt=json;list"
	if err := registry.RegisterCapGroup("g", []string{record, list}, "datacartridge"); err != nil {
		t.Fatalf("two incomparable adapters register: %v", err)
	}
	recordUrn, err := urn.NewMediaUrnFromString(record)
	if err != nil {
		t.Fatal(err)
	}
	listUrn, err := urn.NewMediaUrnFromString(list)
	if err != nil {
		t.Fatal(err)
	}

	matches := registry.AdaptersForCandidates([]*urn.MediaUrn{recordUrn, listUrn})
	if len(matches) != 2 {
		t.Fatalf("both adapters of the cartridge must be returned, got %v", matches)
	}
	for i, want := range []*urn.MediaUrn{recordUrn, listUrn} {
		if matches[i].CartridgeID != "datacartridge" || !matches[i].MediaUrn.IsEquivalent(want) {
			t.Fatalf("match %d: expected (datacartridge, %s), got (%s, %s)",
				i, want, matches[i].CartridgeID, matches[i].MediaUrn)
		}
	}

	candidates := []fileMediaCandidate{{urn: recordUrn}, {urn: listUrn}}
	invoker := &claimsOne{claim: record}
	verdict := consult(context.Background(), "doc.json", candidates, registry, invoker)
	if invoker.calls != 1 {
		t.Fatalf("the cartridge is asked once, was asked %d times", invoker.calls)
	}
	survivors, err := survivingCandidates([]byte(`{"a": 1}`), candidates, verdict)
	if err != nil {
		t.Fatal(err)
	}
	if len(survivors) != 1 || !survivors[0].IsEquivalent(recordUrn) {
		t.Fatalf("the list candidate is owned and unclaimed, so only the claimed record stands; got %v", survivors)
	}
}
