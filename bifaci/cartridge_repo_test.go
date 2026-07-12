package bifaci

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/urn"
)

// makeTestVersions returns a basic v4.0 versions map for testing
func makeTestVersions(platform string) map[string]CartridgeVersionData {
	return map[string]CartridgeVersionData{
		"1.0.0": {
			ReleaseDate:   "2026-02-07",
			Changelog:     []string{"Initial release"},
			MinAppVersion: "1.0.0",
			Builds: []CartridgeBuild{
				{
					Platform: platform,
					Package: CartridgeDistributionInfo{
						Name:   "test-1.0.0.pkg",
						Sha256: "abc123",
						Size:   1000,
						Url:    "https://cartridges.machinefabric.com/test-1.0.0.pkg",
					},
				},
			},
		},
	}
}

// makeTestRegistry builds a v5.0 registry with one cartridge under
// the release channel — most legacy tests don't care about channel
// semantics, only that an entry exists. Tests that need both channels
// populated use makeTestRegistryChannels.
func makeTestRegistry(id string, entry CartridgeRegistryEntry) CartridgeRegistry {
	return makeTestRegistryChannels(map[string]CartridgeRegistryEntry{id: entry}, nil)
}

// makeTestRegistryChannels builds a v5.0 registry with explicit
// per-channel maps. Either map can be nil to leave that channel empty.
func makeTestRegistryChannels(
	release map[string]CartridgeRegistryEntry,
	nightly map[string]CartridgeRegistryEntry,
) CartridgeRegistry {
	if release == nil {
		release = map[string]CartridgeRegistryEntry{}
	}
	if nightly == nil {
		nightly = map[string]CartridgeRegistryEntry{}
	}
	return CartridgeRegistry{
		SchemaVersion:   "5.0",
		RegistryVersion: CartridgeRegistryVersion,
		LastUpdated:     "2026-02-07",
		Channels: CartridgeRegistryChannels{
			Release: CartridgeChannelEntries{Cartridges: release},
			Nightly: CartridgeChannelEntries{Cartridges: nightly},
		},
	}
}

// TEST320-335: CartridgeRepoServer and CartridgeRepoClient tests
func Test320_cartridge_info_construction(t *testing.T) {
	cartridge := CartridgeInfo{
		Id:                "testcartridge",
		Name:              "Test Cartridge",
		Version:           "1.0.0",
		Description:       "A test cartridge",
		Author:            "Test Author",
		TeamId:            "TEAM123",
		SignedAt:          "2026-02-07T00:00:00Z",
		MinAppVersion:     "1.0.0",
		PageUrl:           "https://example.com/cartridge",
		Categories:        []string{"test"},
		Tags:              []string{"testing"},
		CapGroups:         []RegistryCapGroup{},
		Versions:          makeTestVersions("darwin-arm64"),
		AvailableVersions: []string{"1.0.0"},
	}

	if cartridge.Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", cartridge.Id)
	}
	if cartridge.Name != "Test Cartridge" {
		t.Errorf("Expected name 'Test Cartridge', got '%s'", cartridge.Name)
	}
	if cartridge.Version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", cartridge.Version)
	}
}

// TEST321: CartridgeInfo.is_signed() returns true when signature (team_id + signed_at) is present, false when either is empty.
func Test321_cartridge_info_is_signed(t *testing.T) {
	cartridge := CartridgeInfo{
		Id:        "testcartridge",
		Name:      "Test",
		Version:   "1.0.0",
		TeamId:    "TEAM123",
		SignedAt:  "2026-02-07T00:00:00Z",
		CapGroups: []RegistryCapGroup{},
	}

	if !cartridge.IsSigned() {
		t.Error("Expected cartridge to be signed")
	}

	cartridge.TeamId = ""
	if cartridge.IsSigned() {
		t.Error("Expected cartridge not to be signed when team_id is empty")
	}

	cartridge.TeamId = "TEAM123"
	cartridge.SignedAt = ""
	if cartridge.IsSigned() {
		t.Error("Expected cartridge not to be signed when signed_at is empty")
	}
}

// TEST322: CartridgeInfo.build_for_platform() returns the build that matches the requested platform string and None otherwise.
func Test322_cartridge_info_build_for_platform(t *testing.T) {
	cartridge := CartridgeInfo{
		Id:        "testcartridge",
		Name:      "Test",
		Version:   "1.0.0",
		CapGroups: []RegistryCapGroup{},
		Versions: map[string]CartridgeVersionData{
			"1.0.0": {
				ReleaseDate: "2026-02-07",
				Builds: []CartridgeBuild{
					{
						Platform: "darwin-arm64",
						Package: CartridgeDistributionInfo{
							Name:   "test-1.0.0.pkg",
							Sha256: "abc123",
							Size:   1000,
							Url:    "https://cartridges.machinefabric.com/test-1.0.0.pkg",
						},
					},
					{
						Platform: "linux-amd64",
						Package: CartridgeDistributionInfo{
							Name:   "test-1.0.0-linux.pkg",
							Sha256: "def456",
							Size:   2000,
							Url:    "https://cartridges.machinefabric.com/test-1.0.0-linux.pkg",
						},
					},
				},
			},
		},
	}

	build := cartridge.BuildForPlatform("darwin-arm64")
	if build == nil {
		t.Fatal("Expected build for darwin-arm64")
	}
	if build.Package.Name != "test-1.0.0.pkg" {
		t.Errorf("Expected package name 'test-1.0.0.pkg', got '%s'", build.Package.Name)
	}

	build2 := cartridge.BuildForPlatform("linux-amd64")
	if build2 == nil {
		t.Fatal("Expected build for linux-amd64")
	}
	if build2.Package.Name != "test-1.0.0-linux.pkg" {
		t.Errorf("Expected package name 'test-1.0.0-linux.pkg', got '%s'", build2.Package.Name)
	}

	notFound := cartridge.BuildForPlatform("windows-amd64")
	if notFound != nil {
		t.Error("Expected nil for non-existent platform")
	}
}

// TEST323: CartridgeRepoServer requires schema 5.0 and rejects older.
func Test323_cartridge_repo_server_validate_registry(t *testing.T) {
	registry := makeTestRegistryChannels(nil, nil)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Errorf("Expected no error for v5.0, got %v", err)
	}
	if server == nil {
		t.Error("Expected server to be created")
	}

	oldRegistry := CartridgeRegistry{
		SchemaVersion: "4.0",
		LastUpdated:   "2026-02-07",
		Channels: CartridgeRegistryChannels{
			Release: CartridgeChannelEntries{Cartridges: map[string]CartridgeRegistryEntry{}},
			Nightly: CartridgeChannelEntries{Cartridges: map[string]CartridgeRegistryEntry{}},
		},
	}
	server, err = NewCartridgeRepoServer(oldRegistry)
	if err == nil {
		t.Error("Expected error for v4.0 schema")
	}
	if server != nil {
		t.Error("Expected no server to be created for v4.0")
	}

	// A manifest from a different registry regime version is rejected too.
	wrongVersion := makeTestRegistry("cart", CartridgeRegistryEntry{})
	wrongVersion.RegistryVersion = CartridgeRegistryVersion + 1
	server, err = NewCartridgeRepoServer(wrongVersion)
	if err == nil {
		t.Error("Expected error for a mismatched cartridge registry version")
	}
	if server != nil {
		t.Error("Expected no server for a mismatched cartridge registry version")
	}
}

// TEST324: CartridgeRepoServer transforms a v4.0 entry into a flat CartridgeInfo, preserving cap_groups verbatim.
func Test324_cartridge_repo_server_transform_to_array(t *testing.T) {
	versions := makeTestVersions("darwin-arm64")
	entry := CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		PageUrl:       "https://example.com",
		TeamId:        "TEAM123",
		MinAppVersion: "1.0.0",
		CapGroups:     []RegistryCapGroup{},
		Categories:    []string{"test"},
		Tags:          []string{"testing"},
		LatestVersion: "1.0.0",
		Versions:      versions,
	}

	registry := makeTestRegistry("testcartridge", entry)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	arr, err := server.TransformToCartridgeArray()
	if err != nil {
		t.Fatalf("Failed to transform: %v", err)
	}
	if len(arr) != 1 {
		t.Fatalf("Expected 1 cartridge, got %d", len(arr))
	}
	if arr[0].Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", arr[0].Id)
	}
	if arr[0].Name != "Test Cartridge" {
		t.Errorf("Expected name 'Test Cartridge', got '%s'", arr[0].Name)
	}
	if arr[0].Version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", arr[0].Version)
	}
	// Verify build is accessible via BuildForPlatform
	build := arr[0].BuildForPlatform("darwin-arm64")
	if build == nil {
		t.Fatal("Expected build for darwin-arm64")
	}
	if build.Package.Name != "test-1.0.0.pkg" {
		t.Errorf("Expected package name 'test-1.0.0.pkg', got '%s'", build.Package.Name)
	}
}

// TEST325: CartridgeRepoServer.get_cartridges() returns all parsed cartridges
func Test325_cartridge_repo_server_get_cartridges(t *testing.T) {
	entry := CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		CapGroups:     []RegistryCapGroup{},
	}

	registry := makeTestRegistry("testcartridge", entry)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	response, err := server.GetCartridges()
	if err != nil {
		t.Fatalf("Failed to get cartridges: %v", err)
	}
	if len(response.Cartridges) != 1 {
		t.Fatalf("Expected 1 cartridge, got %d", len(response.Cartridges))
	}
	if response.Cartridges[0].Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", response.Cartridges[0].Id)
	}
}

// TEST326: CartridgeRepoServer.get_cartridge() returns cartridge matching the given ID
func Test326_cartridge_repo_server_get_cartridge_by_id(t *testing.T) {
	entry := CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		CapGroups:     []RegistryCapGroup{},
	}

	registry := makeTestRegistry("testcartridge", entry)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	result, err := server.GetCartridgeById(CartridgeChannelRelease, "testcartridge")
	if err != nil {
		t.Fatalf("Failed to get cartridge: %v", err)
	}
	if result == nil {
		t.Fatal("Expected cartridge to be found in release channel")
	}
	if result.Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", result.Id)
	}
	if result.Channel != CartridgeChannelRelease {
		t.Errorf("Expected channel 'release', got '%s'", result.Channel)
	}

	// Same id in the wrong channel must not be found — channels are
	// independent namespaces.
	wrongChannel, err := server.GetCartridgeById(CartridgeChannelNightly, "testcartridge")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if wrongChannel != nil {
		t.Error("Expected cartridge not to be found in nightly channel")
	}

	notFound, err := server.GetCartridgeById(CartridgeChannelRelease, "nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if notFound != nil {
		t.Error("Expected cartridge not to be found")
	}
}

// TEST327: CartridgeRepoServer.search_cartridges() filters by text query against name and description
func Test327_cartridge_repo_server_search_cartridges(t *testing.T) {
	entry := CartridgeRegistryEntry{
		Name:          "PDF Cartridge",
		Description:   "Process PDF documents",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		CapGroups:     []RegistryCapGroup{},
		Tags:          []string{"document"},
	}

	registry := makeTestRegistry("pdfcartridge", entry)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	results, err := server.SearchCartridges("pdf")
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].Id != "pdfcartridge" {
		t.Errorf("Expected id 'pdfcartridge', got '%s'", results[0].Id)
	}

	noMatch, err := server.SearchCartridges("nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(noMatch) != 0 {
		t.Errorf("Expected 0 results, got %d", len(noMatch))
	}
}

// TEST328: CartridgeRepoServer.get_by_category() filters cartridges by category tag
func Test328_cartridge_repo_server_get_by_category(t *testing.T) {
	entry := CartridgeRegistryEntry{
		Name:          "Doc Cartridge",
		Description:   "Process documents",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		CapGroups:     []RegistryCapGroup{},
		Categories:    []string{"document"},
	}

	registry := makeTestRegistry("doccartridge", entry)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	results, err := server.GetCartridgesByCategory("document")
	if err != nil {
		t.Fatalf("Failed to get by category: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].Id != "doccartridge" {
		t.Errorf("Expected id 'doccartridge', got '%s'", results[0].Id)
	}

	noMatch, err := server.GetCartridgesByCategory("nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(noMatch) != 0 {
		t.Errorf("Expected 0 results, got %d", len(noMatch))
	}
}

// TEST329: CartridgeRepoServer.get_suggestions_for_cap() finds cartridges providing a given cap URN
func Test329_cartridge_repo_server_get_by_cap(t *testing.T) {
	capUrn := `cap:in="media:ext=pdf";disbind;out="media:disbound-page;list;enc=utf-8"`
	entry := CartridgeRegistryEntry{
		Name:          "PDF Cartridge",
		Description:   "Process PDFs",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		CapGroups: []RegistryCapGroup{
			{
				Name: "pdf",
				Caps: []RegistryCap{
					{Urn: capUrn, Title: "Disbind PDF", Command: "disbind"},
				},
			},
		},
	}

	registry := makeTestRegistry("pdfcartridge", entry)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	results, err := server.GetCartridgesByCap(capUrn)
	if err != nil {
		t.Fatalf("Failed to get by cap: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].Id != "pdfcartridge" {
		t.Errorf("Expected id 'pdfcartridge', got '%s'", results[0].Id)
	}

	// Same cap URN, same in/out, same op, but the out-spec's tags appear
	// in a different declared order. Tagged-URN equivalence treats them
	// as identical, so the lookup must still resolve.
	reorderedUrn := `cap:in="media:ext=pdf";disbind;out="media:list;disbound-page;enc=utf-8"`
	reordered, err := server.GetCartridgesByCap(reorderedUrn)
	if err != nil {
		t.Fatalf("Failed to get by reordered cap: %v", err)
	}
	if len(reordered) != 1 {
		t.Fatalf("Expected 1 result for tag-reordered request, got %d", len(reordered))
	}

	// Well-formed but no provider in the registry matches it.
	noMatch, err := server.GetCartridgesByCap(`cap:in="media:bogus";nope;out="media:nonexistent"`)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(noMatch) != 0 {
		t.Errorf("Expected 0 results, got %d", len(noMatch))
	}
}

// TEST330: CartridgeRepoClient updates its local cache, keyed by
// (channel, id) so the same id can independently coexist in both
// channels.
func Test330_cartridge_repo_client_update_cache(t *testing.T) {
	repo := NewCartridgeRepo(3600)

	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:        "testcartridge",
				Name:      "Test Cartridge",
				Version:   "1.0.0",
				TeamId:    "TEAM123",
				SignedAt:  "2026-02-07",
				CapGroups: []RegistryCapGroup{},
				Versions:  makeTestVersions("darwin-arm64"),
				Channel:   CartridgeChannelRelease,
			},
		},
	}

	if err := repo.updateCache("https://example.com/cartridges", registry); err != nil {
		t.Fatalf("updateCache must succeed for a well-formed registry: %v", err)
	}

	cartridge := repo.GetCartridge(CartridgeChannelRelease, "testcartridge")
	if cartridge == nil {
		t.Fatal("Expected cartridge to be found in release channel")
	}
	if cartridge.Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", cartridge.Id)
	}
	// Same id in nightly is absent — channels are independent.
	if repo.GetCartridge(CartridgeChannelNightly, "testcartridge") != nil {
		t.Error("Expected cartridge not to be found in nightly channel")
	}
}

// TEST331: CartridgeRepoClient.GetSuggestionsForCap() returns cartridge
// suggestions and propagates the source channel onto each suggestion.
func Test331_cartridge_repo_client_get_suggestions(t *testing.T) {
	repo := NewCartridgeRepo(3600)

	capUrn := `cap:in="media:ext=pdf";disbind;out="media:disbound-page;list;enc=utf-8"`
	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:      "pdfcartridge",
				Name:    "PDF Cartridge",
				Version: "1.0.0",
				TeamId:  "TEAM123",
				PageUrl: "https://example.com/pdf",
				CapGroups: []RegistryCapGroup{
					{
						Name: "pdf",
						Caps: []RegistryCap{
							{Urn: capUrn, Title: "Disbind PDF", Command: "disbind"},
						},
					},
				},
				Versions: makeTestVersions("darwin-arm64"),
				Channel:  CartridgeChannelNightly,
			},
		},
	}

	if err := repo.updateCache("https://example.com/cartridges", registry); err != nil {
		t.Fatalf("updateCache must succeed for a well-formed registry: %v", err)
	}

	suggestions := repo.GetSuggestionsForCap(capUrn)
	if len(suggestions) != 1 {
		t.Fatalf("Expected 1 suggestion, got %d", len(suggestions))
	}
	if suggestions[0].CartridgeId != "pdfcartridge" {
		t.Errorf("Expected cartridge_id 'pdfcartridge', got '%s'", suggestions[0].CartridgeId)
	}
	if suggestions[0].Channel != CartridgeChannelNightly {
		t.Errorf("Expected channel 'nightly', got '%s'", suggestions[0].Channel)
	}
	// suggestions[0].CapUrn is the canonical (normalized) form. Compare
	// via tagged-URN equivalence rather than string equality so a
	// tag-order difference between request and canonical form is OK.
	requested, perr := urn.NewCapUrnFromString(capUrn)
	if perr != nil {
		t.Fatalf("test fixture cap URN must parse: %v", perr)
	}
	returned, perr := urn.NewCapUrnFromString(suggestions[0].CapUrn)
	if perr != nil {
		t.Fatalf("returned cap URN must parse: %v", perr)
	}
	if !returned.IsEquivalent(requested) {
		t.Errorf("Expected equivalent cap URN; got '%s' vs '%s'", suggestions[0].CapUrn, capUrn)
	}
}

// TEST332: CartridgeRepoClient.GetCartridge() retrieves by (channel, id).
func Test332_cartridge_repo_client_get_cartridge(t *testing.T) {
	repo := NewCartridgeRepo(3600)

	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:        "testcartridge",
				Name:      "Test Cartridge",
				Version:   "1.0.0",
				CapGroups: []RegistryCapGroup{},
				Versions:  makeTestVersions("darwin-arm64"),
				Channel:   CartridgeChannelRelease,
			},
		},
	}

	if err := repo.updateCache("https://example.com/cartridges", registry); err != nil {
		t.Fatalf("updateCache must succeed for a well-formed registry: %v", err)
	}

	cartridge := repo.GetCartridge(CartridgeChannelRelease, "testcartridge")
	if cartridge == nil {
		t.Fatal("Expected cartridge to be found")
	}
	if cartridge.Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", cartridge.Id)
	}

	notFound := repo.GetCartridge(CartridgeChannelRelease, "nonexistent")
	if notFound != nil {
		t.Error("Expected cartridge not to be found")
	}
}

// TEST333: CartridgeRepoClient.get_all_caps() returns aggregate cap URNs from all cached cartridges
func Test333_cartridge_repo_client_get_all_caps(t *testing.T) {
	repo := NewCartridgeRepo(3600)

	cap1 := `cap:in="media:ext=pdf";disbind;out="media:disbound-page;list;enc=utf-8"`
	cap2 := `cap:in="media:txt;enc=utf-8";disbind;out="media:disbound-page;list;enc=utf-8"`

	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:      "cartridge1",
				Name:    "Cartridge 1",
				Version: "1.0.0",
				CapGroups: []RegistryCapGroup{
					{Name: "g", Caps: []RegistryCap{{Urn: cap1, Title: "Cap 1", Command: "x"}}},
				},
				Versions: makeTestVersions("darwin-arm64"),
				Channel:  CartridgeChannelRelease,
			},
			{
				Id:      "cartridge2",
				Name:    "Cartridge 2",
				Version: "1.0.0",
				CapGroups: []RegistryCapGroup{
					{Name: "g", Caps: []RegistryCap{{Urn: cap2, Title: "Cap 2", Command: "x"}}},
				},
				Versions: makeTestVersions("darwin-arm64"),
				Channel:  CartridgeChannelRelease,
			},
		},
	}

	if err := repo.updateCache("https://example.com/cartridges", registry); err != nil {
		t.Fatalf("updateCache must succeed for a well-formed registry: %v", err)
	}

	// URNs are opaque: caps are stored in normalized form, so we compare
	// using parsed-URN equivalence rather than string equality.
	caps := repo.GetAllAvailableCaps()
	if len(caps) != 2 {
		t.Fatalf("Expected 2 distinct caps, got %d: %v", len(caps), caps)
	}
	cap1Parsed, _ := urn.NewCapUrnFromString(cap1)
	cap2Parsed, _ := urn.NewCapUrnFromString(cap2)
	capFound1, capFound2 := false, false
	for _, c := range caps {
		parsed, err := urn.NewCapUrnFromString(c)
		if err != nil {
			t.Fatalf("returned cap is not a valid URN: %s: %v", c, err)
		}
		if parsed.IsEquivalent(cap1Parsed) {
			capFound1 = true
		}
		if parsed.IsEquivalent(cap2Parsed) {
			capFound2 = true
		}
	}
	if !capFound1 {
		t.Error("Expected cap1 to be found")
	}
	if !capFound2 {
		t.Error("Expected cap2 to be found")
	}
}

// TEST334: CartridgeRepoClient.needs_sync() returns true when cache TTL has expired
func Test334_cartridge_repo_client_needs_sync(t *testing.T) {
	repo := NewCartridgeRepo(3600)
	urls := []string{"https://example.com/cartridges"}

	if !repo.NeedsSync(urls) {
		t.Error("Expected to need sync with empty cache")
	}

	registry := &CartridgeRegistryResponse{Cartridges: []CartridgeInfo{}}
	if err := repo.updateCache("https://example.com/cartridges", registry); err != nil {
		t.Fatalf("updateCache must succeed for a well-formed registry: %v", err)
	}

	if repo.NeedsSync(urls) {
		t.Error("Expected not to need sync after update")
	}
}

// TEST335: Server creates registry response and client consumes it end-to-end
func Test335_cartridge_repo_server_client_integration(t *testing.T) {
	capUrn := `cap:in="media:test";test;out="media:result"`
	entry := CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		PageUrl:       "https://example.com",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		CapGroups: []RegistryCapGroup{
			{
				Name: "test-group",
				Caps: []RegistryCap{
					{Urn: capUrn, Title: "Test Cap", Command: "test"},
				},
				AdapterUrns: []string{"media:test"},
			},
		},
		Categories: []string{"test"},
	}

	registry := makeTestRegistry("testcartridge", entry)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	response, err := server.GetCartridges()
	if err != nil {
		t.Fatalf("Failed to get cartridges: %v", err)
	}
	if len(response.Cartridges) != 1 {
		t.Fatalf("Expected 1 cartridge, got %d", len(response.Cartridges))
	}

	c := &response.Cartridges[0]
	if c.Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", c.Id)
	}
	if !c.IsSigned() {
		t.Error("Expected cartridge to be signed")
	}
	caps := c.IterCaps()
	if len(caps) != 1 {
		t.Fatalf("Expected 1 cap, got %d", len(caps))
	}
	if caps[0].Urn != capUrn {
		t.Errorf("Expected cap URN '%s', got '%s'", capUrn, caps[0].Urn)
	}

	// Verify build is accessible
	build := c.BuildForPlatform("darwin-arm64")
	if build == nil {
		t.Fatal("Expected build for darwin-arm64")
	}
	if build.Package.Name != "test-1.0.0.pkg" {
		t.Errorf("Expected package name 'test-1.0.0.pkg', got '%s'", build.Package.Name)
	}
	if build.Package.Sha256 != "abc123" {
		t.Errorf("Expected package sha256 'abc123', got '%s'", build.Package.Sha256)
	}
}

// TEST630: CartridgeRepo creation starts with empty cartridge list.
func Test630_cartridge_repo_creation(t *testing.T) {
	repo := NewCartridgeRepo(3600)
	if len(repo.GetAllCartridges()) != 0 {
		t.Error("Expected empty cartridge list on creation")
	}
}

// TEST631: needs_sync returns true with empty cache and non-empty URLs.
func Test631_needs_sync_empty_cache(t *testing.T) {
	repo := NewCartridgeRepo(3600)
	urls := []string{"https://example.com/cartridges"}
	if !repo.NeedsSync(urls) {
		t.Error("Expected needs_sync to be true with empty cache")
	}
}

// TEST319: A registry response with a malformed cap URN inside cap_groups must propagate as ParseError when indexed into the cache, not silently disappear.
func Test319_update_cache_rejects_malformed_cap_urn(t *testing.T) {
	repo := NewCartridgeRepo(3600)
	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:      "broken",
				Name:    "Broken",
				Version: "1.0.0",
				CapGroups: []RegistryCapGroup{
					{
						Name: "g",
						Caps: []RegistryCap{
							{Urn: "not a valid urn at all", Title: "Bad", Command: "x"},
						},
					},
				},
				Versions: makeTestVersions("darwin-arm64"),
				Channel:  CartridgeChannelRelease,
			},
		},
	}
	err := repo.updateCache("https://x", registry)
	if err == nil {
		t.Fatal("Expected ParseError for malformed cap URN, got nil")
	}
	repoErr, ok := err.(*CartridgeRepoError)
	if !ok || repoErr.Kind != "ParseError" {
		t.Errorf("Expected ParseError, got %T %v", err, err)
	}
}

// buildIdentityCapGroup builds a single-group cap-group slice with just
// the identity cap — the common fixture shape across channel-isolation
// and walk-order tests.
func buildIdentityCapGroup() []RegistryCapGroup {
	return []RegistryCapGroup{
		{
			Name: "g",
			Caps: []RegistryCap{
				{Urn: "cap:effect=none", Title: "Identity", Command: "identity"},
			},
		},
	}
}

// buildRegistryEntryNamed builds a CartridgeRegistryEntry with the given
// display name, a single version with a darwin-arm64 build, and the
// supplied cap groups.
func buildRegistryEntryNamed(name, version, pkgName string, groups []RegistryCapGroup) CartridgeRegistryEntry {
	return CartridgeRegistryEntry{
		Name:          name,
		Description:   "Test Cartridge",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: version,
		CapGroups:     groups,
		Versions: map[string]CartridgeVersionData{
			version: {
				ReleaseDate: "2026-02-07",
				Builds: []CartridgeBuild{
					{
						Platform: "darwin-arm64",
						Package: CartridgeDistributionInfo{
							Name:   pkgName,
							Sha256: "abc123",
							Size:   1000,
							Url:    "https://cartridges.machinefabric.com/" + pkgName,
						},
					},
				},
			},
		},
	}
}

// TEST300: A cartridge with the same id can independently exist in both channels. Each lookup must return the channel-specific entry.
func Test300_get_cartridge_by_id_channel_isolation(t *testing.T) {
	releaseEntry := buildRegistryEntryNamed("Foo (release)", "1.0.0", "foo-1.0.0.pkg", buildIdentityCapGroup())
	nightlyEntry := buildRegistryEntryNamed("Foo (nightly)", "2.0.0", "foo-2.0.0.pkg", buildIdentityCapGroup())

	registry := makeTestRegistryChannels(
		map[string]CartridgeRegistryEntry{"foocartridge": releaseEntry},
		map[string]CartridgeRegistryEntry{"foocartridge": nightlyEntry},
	)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	r, err := server.GetCartridgeById(CartridgeChannelRelease, "foocartridge")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if r == nil {
		t.Fatal("Expected release entry to be found")
	}
	if r.Name != "Foo (release)" {
		t.Errorf("Expected name 'Foo (release)', got '%s'", r.Name)
	}
	if r.Version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", r.Version)
	}
	if r.Channel != CartridgeChannelRelease {
		t.Errorf("Expected channel 'release', got '%s'", r.Channel)
	}

	n, err := server.GetCartridgeById(CartridgeChannelNightly, "foocartridge")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n == nil {
		t.Fatal("Expected nightly entry to be found")
	}
	if n.Name != "Foo (nightly)" {
		t.Errorf("Expected name 'Foo (nightly)', got '%s'", n.Name)
	}
	if n.Version != "2.0.0" {
		t.Errorf("Expected version '2.0.0', got '%s'", n.Version)
	}
	if n.Channel != CartridgeChannelNightly {
		t.Errorf("Expected channel 'nightly', got '%s'", n.Channel)
	}
}

// TEST301: Walking both channels produces release entries first.
func Test301_transform_walks_both_channels_release_first(t *testing.T) {
	releaseEntry := buildRegistryEntryNamed("R", "1.0.0", "r-1.0.0.pkg", buildIdentityCapGroup())
	nightlyEntry := buildRegistryEntryNamed("N", "1.0.0", "n-1.0.0.pkg", buildIdentityCapGroup())

	registry := makeTestRegistryChannels(
		map[string]CartridgeRegistryEntry{"foo": releaseEntry},
		map[string]CartridgeRegistryEntry{"bar": nightlyEntry},
	)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	cartridges, err := server.TransformToCartridgeArray()
	if err != nil {
		t.Fatalf("Failed to transform: %v", err)
	}
	if len(cartridges) != 2 {
		t.Fatalf("Expected 2 cartridges, got %d", len(cartridges))
	}
	channels := []CartridgeChannel{cartridges[0].Channel, cartridges[1].Channel}
	if channels[0] != CartridgeChannelRelease || channels[1] != CartridgeChannelNightly {
		t.Errorf("release entries must come before nightly entries; got %v", channels)
	}
}

// TEST632: A registry cap with only the three required fields parses.
func Test632_deserialize_minimal_registry_cap(t *testing.T) {
	jsonStr := `{"urn": "cap:effect=none", "title": "Identity", "aliases": ["identity"]}`
	var cap RegistryCap
	if err := json.Unmarshal([]byte(jsonStr), &cap); err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if cap.Urn != "cap:effect=none" {
		t.Errorf("Expected urn 'cap:effect=none', got '%s'", cap.Urn)
	}
	if cap.Title != "Identity" {
		t.Errorf("Expected title 'Identity', got '%s'", cap.Title)
	}
	if cap.PrimaryAlias() != "identity" {
		t.Errorf("Expected alias 'identity', got '%s'", cap.PrimaryAlias())
	}
	if cap.CapDescription != nil {
		t.Error("Expected cap_description to be nil")
	}
	if cap.Args != nil {
		t.Error("Expected args to be nil")
	}
	if cap.Output != nil {
		t.Error("Expected output to be nil")
	}
}

// TEST633: A registry cap with cap_description, args, output all parses.
func Test633_deserialize_rich_registry_cap(t *testing.T) {
	jsonStr := `{
		"urn": "cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"",
		"title": "Disbind PDF",
		"aliases": ["disbind"],
		"cap_description": "Extract each PDF page as plain page text.",
		"args": [
			{
				"media_urn": "media:enc=utf-8;file-path",
				"required": true,
				"is_sequence": false,
				"sources": [{"stdin": "media:ext=pdf"}, {"position": 0}],
				"arg_description": "Path to the PDF file to process"
			}
		],
		"output": {
			"media_urn": "media:enc=utf-8;page",
			"is_sequence": true,
			"output_description": "One page text per PDF page"
		}
	}`
	var cap RegistryCap
	if err := json.Unmarshal([]byte(jsonStr), &cap); err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if cap.PrimaryAlias() != "disbind" {
		t.Errorf("Expected alias 'disbind', got '%s'", cap.PrimaryAlias())
	}
	if cap.CapDescription == nil || *cap.CapDescription != "Extract each PDF page as plain page text." {
		t.Errorf("Expected cap_description, got %v", cap.CapDescription)
	}
	if len(cap.Args) != 1 {
		t.Fatalf("Expected 1 arg, got %d", len(cap.Args))
	}
	if cap.Args[0].MediaUrn != "media:enc=utf-8;file-path" {
		t.Errorf("Expected media_urn 'media:enc=utf-8;file-path', got '%s'", cap.Args[0].MediaUrn)
	}
	if len(cap.Args[0].Sources) != 2 {
		t.Fatalf("Expected 2 sources, got %d", len(cap.Args[0].Sources))
	}
	if cap.Args[0].Sources[0].Stdin == nil || *cap.Args[0].Sources[0].Stdin != "media:ext=pdf" {
		t.Errorf("Expected source[0].stdin 'media:ext=pdf', got %v", cap.Args[0].Sources[0].Stdin)
	}
	if cap.Args[0].Sources[1].Position == nil || *cap.Args[0].Sources[1].Position != 0 {
		t.Errorf("Expected source[1].position 0, got %v", cap.Args[0].Sources[1].Position)
	}
	if cap.Output == nil {
		t.Fatal("Expected output to be present")
	}
	if cap.Output.MediaUrn != "media:enc=utf-8;page" {
		t.Errorf("Expected output media_urn 'media:enc=utf-8;page', got '%s'", cap.Output.MediaUrn)
	}
	if !cap.Output.IsSequence {
		t.Error("Expected output.is_sequence to be true")
	}
}

// TEST634: A registry cap_group parses with caps + adapter_urns.
func Test634_deserialize_cap_group(t *testing.T) {
	jsonStr := `{
		"name": "pdf-formats",
		"caps": [
			{"urn": "cap:effect=none", "title": "Identity", "aliases": ["identity"]}
		],
		"adapter_urns": ["media:ext=pdf"]
	}`
	var group RegistryCapGroup
	if err := json.Unmarshal([]byte(jsonStr), &group); err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if group.Name != "pdf-formats" {
		t.Errorf("Expected name 'pdf-formats', got '%s'", group.Name)
	}
	if len(group.Caps) != 1 {
		t.Fatalf("Expected 1 cap, got %d", len(group.Caps))
	}
	if len(group.AdapterUrns) != 1 || group.AdapterUrns[0] != "media:ext=pdf" {
		t.Errorf("Expected adapter_urns ['media:ext=pdf'], got %v", group.AdapterUrns)
	}
}

// TEST635: CartridgeInfo deserializes the wire shape exactly as returned by /api/cartridges (camelCase top-level + snake_case cap_groups). Null camelCase string fields fall back to empty.
func Test635_deserialize_cartridge_info_wire_shape(t *testing.T) {
	jsonStr := `{
		"id": "pdfcartridge",
		"name": "pdfcartridge",
		"version": "0.179.441",
		"description": "PDF page renderer",
		"author": "https://github.com/machinefabric",
		"pageUrl": "https://github.com/machinefabric/pdfcartridge",
		"teamId": "P336JK947M",
		"signedAt": "2026-04-25T14:53:55Z",
		"minAppVersion": "1.0.0",
		"cap_groups": [
			{
				"name": "pdf-formats",
				"caps": [
					{"urn": "cap:effect=none", "title": "Identity", "aliases": ["identity"]},
					{"urn": "cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"", "title": "Disbind PDF Into Page Text", "aliases": ["disbind"]}
				],
				"adapter_urns": ["media:ext=pdf"]
			}
		],
		"categories": [],
		"tags": [],
		"versions": {},
		"availableVersions": [],
		"channel": "release",
		"registryUrl": "https://test.example/manifest"
	}`
	var cartridge CartridgeInfo
	if err := json.Unmarshal([]byte(jsonStr), &cartridge); err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if cartridge.Id != "pdfcartridge" {
		t.Errorf("Expected id 'pdfcartridge', got '%s'", cartridge.Id)
	}
	if cartridge.TeamId != "P336JK947M" {
		t.Errorf("Expected team_id 'P336JK947M', got '%s'", cartridge.TeamId)
	}
	if len(cartridge.CapGroups) != 1 {
		t.Fatalf("Expected 1 cap group, got %d", len(cartridge.CapGroups))
	}
	if len(cartridge.CapGroups[0].Caps) != 2 {
		t.Fatalf("Expected 2 caps, got %d", len(cartridge.CapGroups[0].Caps))
	}
	if len(cartridge.IterCaps()) != 2 {
		t.Errorf("Expected IterCaps to count 2, got %d", len(cartridge.IterCaps()))
	}
	if cartridge.Channel != CartridgeChannelRelease {
		t.Errorf("Expected channel 'release', got '%s'", cartridge.Channel)
	}
	if cartridge.RegistryUrl != "https://test.example/manifest" {
		t.Errorf("Expected registry_url 'https://test.example/manifest', got '%s'", cartridge.RegistryUrl)
	}
}

// TEST636: CartridgeInfo with null version/description/author still deserializes (the null_as_empty_string deserializer is the only tolerated coercion — every other malformed input is a hard error).
func Test636_deserialize_cartridge_info_with_null_strings(t *testing.T) {
	jsonStr := `{
		"id": "mlxcartridge",
		"name": "MLX Cartridge",
		"version": null,
		"description": null,
		"author": null,
		"cap_groups": [],
		"versions": {},
		"channel": "nightly",
		"registryUrl": "https://test.example/manifest"
	}`
	var cartridge CartridgeInfo
	if err := json.Unmarshal([]byte(jsonStr), &cartridge); err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if cartridge.Version != "" {
		t.Errorf("Expected version '', got '%s'", cartridge.Version)
	}
	if cartridge.Description != "" {
		t.Errorf("Expected description '', got '%s'", cartridge.Description)
	}
	if cartridge.Author != "" {
		t.Errorf("Expected author '', got '%s'", cartridge.Author)
	}
	if len(cartridge.CapGroups) != 0 {
		t.Errorf("Expected empty cap_groups, got %d", len(cartridge.CapGroups))
	}
}

// TEST637: A full /api/cartridges-shaped response with two cartridges and nested cap_groups round-trips through the response wrapper.
func Test637_deserialize_full_registry_response(t *testing.T) {
	jsonStr := `{
		"cartridges": [
			{
				"id": "pdfcartridge",
				"name": "pdfcartridge",
				"version": "0.179.441",
				"description": "PDF",
				"author": "https://github.com/machinefabric",
				"pageUrl": "",
				"teamId": "P336JK947M",
				"signedAt": "2026-04-25T14:53:55Z",
				"minAppVersion": "1.0.0",
				"cap_groups": [
					{
						"name": "pdf-formats",
						"caps": [
							{"urn": "cap:effect=none", "title": "Identity", "aliases": ["identity"]}
						],
						"adapter_urns": ["media:ext=pdf"]
					}
				],
				"categories": [],
				"tags": [],
				"versions": {},
				"availableVersions": [],
				"channel": "release",
				"registryUrl": "https://test.example/manifest"
			},
			{
				"id": "imagecartridge",
				"name": "imagecartridge",
				"version": "0.1.6",
				"description": "image",
				"author": "",
				"teamId": "P336JK947M",
				"signedAt": "2026-04-25T21:53:45Z",
				"minAppVersion": "1.0.0",
				"cap_groups": [
					{
						"name": "image-formats",
						"caps": [
							{"urn": "cap:in=\"media:convert-image;image;jpeg;png\";out=\"media:image\"", "title": "Convert JPEG to PNG", "aliases": ["convert-image"]}
						],
						"adapter_urns": ["media:ext=bmp;image", "media:ext=jpeg;image", "media:ext=png;image", "media:ext=tiff;image", "media:ext=webp;image", "media:ext=gif;image"]
					}
				],
				"categories": [],
				"tags": [],
				"versions": {},
				"availableVersions": [],
				"channel": "nightly",
				"registryUrl": "https://test.example/manifest"
			}
		],
		"total": 2,
		"page": 1,
		"limit": 20,
		"totalPages": 1
	}`
	var response CartridgeRegistryResponse
	if err := json.Unmarshal([]byte(jsonStr), &response); err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if len(response.Cartridges) != 2 {
		t.Fatalf("Expected 2 cartridges, got %d", len(response.Cartridges))
	}
	var img *CartridgeInfo
	for i := range response.Cartridges {
		if response.Cartridges[i].Id == "imagecartridge" {
			img = &response.Cartridges[i]
			break
		}
	}
	if img == nil {
		t.Fatal("Expected to find imagecartridge")
	}
	if len(img.CapGroups) != 1 {
		t.Fatalf("Expected 1 cap group, got %d", len(img.CapGroups))
	}
	if len(img.CapGroups[0].AdapterUrns) != 6 {
		t.Errorf("Expected 6 adapter_urns, got %d", len(img.CapGroups[0].AdapterUrns))
	}
}

// TEST1847: A build from a registry manifest published BEFORE `packages[]` existed carries only the legacy singular `package` (no `format`). It must still deserialize (a missing `packages` must not fail the whole parse) and `primary_package()` must fall back to that legacy package, so a registry not yet republished with the dual-write keeps installing. When `packages[]` is present it is preferred over the legacy field.
func Test1847_cartridge_build_legacy_package_fallback(t *testing.T) {
	legacyJSON := `{
		"platform": "linux-x86_64",
		"package": {
			"name": "imagecartridge-1.0.0.pkg",
			"url": "https://cartridges.machinefabric.com/imagecartridge-1.0.0.pkg",
			"sha256": "abc123",
			"size": 1000
		}
	}`
	var legacy CartridgeBuild
	if err := json.Unmarshal([]byte(legacyJSON), &legacy); err != nil {
		t.Fatalf("Failed to parse legacy: %v", err)
	}
	if len(legacy.Packages) != 0 {
		t.Errorf("Expected empty packages, got %d", len(legacy.Packages))
	}
	primary := legacy.PrimaryPackage()
	if primary == nil {
		t.Fatal("legacy package must be read as a fallback")
	}
	if primary.Name != "imagecartridge-1.0.0.pkg" {
		t.Errorf("Expected name 'imagecartridge-1.0.0.pkg', got '%s'", primary.Name)
	}
	if primary.Format != "" {
		t.Errorf("Expected empty format for legacy object, got '%s'", primary.Format)
	}
	if !strings.HasSuffix(primary.Url, "imagecartridge-1.0.0.pkg") {
		t.Errorf("Expected url to end with 'imagecartridge-1.0.0.pkg', got '%s'", primary.Url)
	}

	modernJSON := `{
		"platform": "linux-x86_64",
		"package": {
			"name": "legacy.pkg", "url": "https://x/legacy.pkg",
			"sha256": "dead", "size": 1
		},
		"packages": [
			{"name": "c.rpm", "url": "https://x/c.rpm", "sha256": "a", "size": 2, "format": "rpm"},
			{"name": "c.deb", "url": "https://x/c.deb", "sha256": "b", "size": 3, "format": "deb"}
		]
	}`
	var modern CartridgeBuild
	if err := json.Unmarshal([]byte(modernJSON), &modern); err != nil {
		t.Fatalf("Failed to parse modern: %v", err)
	}
	// linux prefers deb over rpm; the legacy `package` is ignored.
	mp := modern.PrimaryPackage()
	if mp == nil {
		t.Fatal("Expected a primary package from packages[]")
	}
	if mp.Name != "c.deb" {
		t.Errorf("Expected 'c.deb' (linux prefers deb), got '%s'", mp.Name)
	}
}

// buildForPlatformWithFormat builds a CartridgeBuild carrying a single
// native-format package, so a resolution test can assert exactly which
// package URL the host gets. Mirrors Rust build_for_platform_with_format.
func buildForPlatformWithFormat(platform, format, pkgName string) CartridgeBuild {
	return CartridgeBuild{
		Platform: platform,
		Packages: []CartridgeDistributionInfo{
			{
				Name:   pkgName,
				Sha256: "deadbeef",
				Size:   4242,
				Url:    "https://cartridges.machinefabric.com/" + pkgName,
				Format: format,
			},
		},
	}
}

// platformBuild is a (platform, format, pkgName) triple for cartridgeWithVersions.
type platformBuild struct {
	platform string
	format   string
	pkgName  string
}

// versionSpec is a (version, builds) pair for cartridgeWithVersions, newest-first.
type versionSpec struct {
	version string
	builds  []platformBuild
}

// cartridgeWithVersions constructs a cartridge whose versions/platform-builds
// are fully specified by the caller. `versions` is given newest-first;
// `Version` (the "latest" field) is set to the first entry. Mirrors Rust
// cartridge_with_versions.
func cartridgeWithVersions(id string, versions []versionSpec) CartridgeInfo {
	versionMap := make(map[string]CartridgeVersionData)
	available := make([]string, 0, len(versions))
	for _, vs := range versions {
		available = append(available, vs.version)
		builds := make([]CartridgeBuild, 0, len(vs.builds))
		for _, b := range vs.builds {
			builds = append(builds, buildForPlatformWithFormat(b.platform, b.format, b.pkgName))
		}
		versionMap[vs.version] = CartridgeVersionData{
			ReleaseDate: "2026-02-07",
			Builds:      builds,
		}
	}
	latest := ""
	if len(versions) > 0 {
		latest = versions[0].version
	}
	return CartridgeInfo{
		Id:                id,
		Name:              id,
		Version:           latest,
		TeamId:            "TEAM123",
		SignedAt:          "2026-02-07T00:00:00Z",
		CapGroups:         []RegistryCapGroup{},
		Versions:          versionMap,
		AvailableVersions: available,
		Channel:           CartridgeChannelRelease,
		RegistryUrl:       "https://example.com/cartridges",
	}
}

// TEST1849: latest version has a host build → Compatible, resolving to the latest version and that platform's native-format package.
func Test1849_resolve_for_host_compatible_latest(t *testing.T) {
	cartridge := cartridgeWithVersions("c", []versionSpec{
		{version: "1.2.0", builds: []platformBuild{
			{"darwin-arm64", "pkg", "c-1.2.0.pkg"},
			{"linux-x86_64", "deb", "c-1.2.0.deb"},
		}},
		{version: "1.1.0", builds: []platformBuild{
			{"darwin-arm64", "pkg", "c-1.1.0.pkg"},
		}},
	})

	r := cartridge.ResolveForHost("linux-x86_64")
	if r.Status != CompatStatusCompatible {
		t.Errorf("Expected Compatible, got %v", r.Status)
	}
	if r.ResolvedVersion != "1.2.0" {
		t.Errorf("Expected resolved version '1.2.0', got '%s'", r.ResolvedVersion)
	}
	if r.ResolvedPackage == nil || r.ResolvedPackage.Name != "c-1.2.0.deb" {
		t.Errorf("Expected package 'c-1.2.0.deb', got %v", r.ResolvedPackage)
	}
	if r.ResolvedPackage.Format != "deb" {
		t.Errorf("Expected format 'deb', got '%s'", r.ResolvedPackage.Format)
	}
	if r.Reason != "" {
		t.Errorf("Compatible carries no reason, got '%s'", r.Reason)
	}
	if r.HostPlatform != "linux-x86_64" {
		t.Errorf("Expected host platform 'linux-x86_64', got '%s'", r.HostPlatform)
	}
}

// TEST1850: the latest version lacks a host build but an older version has one → CompatibleOutdated, resolving to the older version with a reason naming both the latest and the resolved version.
func Test1850_resolve_for_host_compatible_outdated(t *testing.T) {
	cartridge := cartridgeWithVersions("c", []versionSpec{
		// Latest 1.3.0 ships only macOS.
		{version: "1.3.0", builds: []platformBuild{{"darwin-arm64", "pkg", "c-1.3.0.pkg"}}},
		// 1.2.0 still shipped Linux.
		{version: "1.2.0", builds: []platformBuild{
			{"darwin-arm64", "pkg", "c-1.2.0.pkg"},
			{"linux-x86_64", "deb", "c-1.2.0.deb"},
		}},
		{version: "1.1.0", builds: []platformBuild{{"linux-x86_64", "deb", "c-1.1.0.deb"}}},
	})

	r := cartridge.ResolveForHost("linux-x86_64")
	if r.Status != CompatStatusCompatibleOutdated {
		t.Errorf("Expected CompatibleOutdated, got %v", r.Status)
	}
	// Newest-with-host-build is 1.2.0, NOT the oldest 1.1.0 that also has it.
	if r.ResolvedVersion != "1.2.0" {
		t.Errorf("Expected resolved version '1.2.0', got '%s'", r.ResolvedVersion)
	}
	if r.ResolvedPackage == nil || r.ResolvedPackage.Name != "c-1.2.0.deb" {
		t.Errorf("Expected package 'c-1.2.0.deb', got %v", r.ResolvedPackage)
	}
	if r.Reason == "" {
		t.Fatal("outdated carries a reason")
	}
	if !strings.Contains(r.Reason, "1.3.0") {
		t.Errorf("reason must name the latest: %s", r.Reason)
	}
	if !strings.Contains(r.Reason, "1.2.0") {
		t.Errorf("reason must name the resolved: %s", r.Reason)
	}
}

// TEST1851: no version ships a host build → Incompatible, no resolved version/package, reason states the host platform.
func Test1851_resolve_for_host_incompatible(t *testing.T) {
	cartridge := cartridgeWithVersions("c", []versionSpec{
		{version: "1.2.0", builds: []platformBuild{{"darwin-arm64", "pkg", "c-1.2.0.pkg"}}},
		{version: "1.1.0", builds: []platformBuild{{"darwin-arm64", "pkg", "c-1.1.0.pkg"}}},
	})

	r := cartridge.ResolveForHost("windows-x86_64")
	if r.Status != CompatStatusIncompatible {
		t.Errorf("Expected Incompatible, got %v", r.Status)
	}
	if r.ResolvedVersion != "" {
		t.Errorf("Expected no resolved version, got '%s'", r.ResolvedVersion)
	}
	if r.ResolvedPackage != nil {
		t.Errorf("Expected no resolved package, got %v", r.ResolvedPackage)
	}
	if !strings.Contains(r.Reason, "windows-x86_64") {
		t.Errorf("reason must name the host platform: %s", r.Reason)
	}
}

// TEST1852: a host build whose packages[] is empty AND has no legacy `package` ships no installer; resolution must SKIP it (not resolve to an un-downloadable version) and fall through to an older usable version.
func Test1852_resolve_for_host_skips_build_with_no_installer(t *testing.T) {
	cartridge := cartridgeWithVersions("c", []versionSpec{
		// Latest has a linux build entry but we strip its installer below.
		{version: "2.0.0", builds: []platformBuild{{"linux-x86_64", "deb", "c-2.0.0.deb"}}},
		{version: "1.0.0", builds: []platformBuild{{"linux-x86_64", "deb", "c-1.0.0.deb"}}},
	})
	// Make 2.0.0's linux build ship nothing installable.
	v2 := cartridge.Versions["2.0.0"]
	v2.Builds[0].Packages = nil
	v2.Builds[0].Package = CartridgeDistributionInfo{}
	cartridge.Versions["2.0.0"] = v2

	r := cartridge.ResolveForHost("linux-x86_64")
	// 2.0.0 is skipped (no installer); newest USABLE host build is 1.0.0.
	if r.Status != CompatStatusCompatibleOutdated {
		t.Errorf("Expected CompatibleOutdated, got %v", r.Status)
	}
	if r.ResolvedVersion != "1.0.0" {
		t.Errorf("Expected resolved version '1.0.0', got '%s'", r.ResolvedVersion)
	}
	if r.ResolvedPackage == nil || r.ResolvedPackage.Name != "c-1.0.0.deb" {
		t.Errorf("Expected package 'c-1.0.0.deb', got %v", r.ResolvedPackage)
	}
}

// TEST1853: host_platform() returns a normalized {os}-{arch} string with arch aarch64 mapped to arm64 — the exact form the registry uses.
func Test1853_host_platform_normalized_form(t *testing.T) {
	p := HostPlatform()
	idx := strings.Index(p, "-")
	if idx < 0 {
		t.Fatalf("host_platform must be os-arch, got %s", p)
	}
	os := p[:idx]
	arch := p[idx+1:]
	if os == "" {
		t.Errorf("os segment must be present: %s", os)
	}
	// The registry never uses the raw "aarch64"; it must be normalized.
	if arch == "aarch64" {
		t.Errorf("arch must be normalized to arm64, got '%s'", arch)
	}
}
