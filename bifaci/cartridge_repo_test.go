package bifaci

import (
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
		SchemaVersion: "5.0",
		LastUpdated:   "2026-02-07",
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
		Id:       "testcartridge",
		Name:     "Test",
		Version:  "1.0.0",
		TeamId:   "TEAM123",
		SignedAt: "2026-02-07T00:00:00Z",
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
		Id:      "testcartridge",
		Name:    "Test",
		Version: "1.0.0",
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
