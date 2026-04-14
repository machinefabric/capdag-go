package bifaci

import (
	"testing"
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
					},
				},
			},
		},
	}
}

// makeTestRegistry builds a v4.0 registry with one cartridge for testing
func makeTestRegistry(id string, entry CartridgeRegistryEntry) CartridgeRegistry {
	return CartridgeRegistry{
		SchemaVersion: "4.0",
		LastUpdated:   "2026-02-07",
		Cartridges: map[string]CartridgeRegistryEntry{
			id: entry,
		},
	}
}

// TEST320: Construct CartridgeInfo and verify fields
func Test320_cartridge_info_construction(t *testing.T) {
	cartridge := CartridgeInfo{
		Id:                "testcartridge",
		Name:              "Test Cartridge",
		Version:           "1.0.0",
		Description:       "A test cartridge",
		Author:            "Test Author",
		Homepage:          "https://example.com",
		TeamId:            "TEAM123",
		SignedAt:          "2026-02-07T00:00:00Z",
		MinAppVersion:     "1.0.0",
		PageUrl:           "https://example.com/cartridge",
		Categories:        []string{"test"},
		Tags:              []string{"testing"},
		Caps:              []CartridgeCapSummary{},
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

// TEST321: Verify IsSigned() method
func Test321_cartridge_info_is_signed(t *testing.T) {
	cartridge := CartridgeInfo{
		Id:       "testcartridge",
		Name:     "Test",
		Version:  "1.0.0",
		TeamId:   "TEAM123",
		SignedAt: "2026-02-07T00:00:00Z",
		Caps:     []CartridgeCapSummary{},
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

// TEST322: Verify BuildForPlatform() method
func Test322_cartridge_info_build_for_platform(t *testing.T) {
	cartridge := CartridgeInfo{
		Id:      "testcartridge",
		Name:    "Test",
		Version: "1.0.0",
		Caps:    []CartridgeCapSummary{},
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
						},
					},
					{
						Platform: "linux-amd64",
						Package: CartridgeDistributionInfo{
							Name:   "test-1.0.0-linux.pkg",
							Sha256: "def456",
							Size:   2000,
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

// TEST323: Validate registry schema version
func Test323_cartridge_repo_server_validate_registry(t *testing.T) {
	registry := CartridgeRegistry{
		SchemaVersion: "4.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    make(map[string]CartridgeRegistryEntry),
	}

	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Errorf("Expected no error for v4.0, got %v", err)
	}
	if server == nil {
		t.Error("Expected server to be created")
	}

	// Test wrong schema version rejection
	oldRegistry := CartridgeRegistry{
		SchemaVersion: "3.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    make(map[string]CartridgeRegistryEntry),
	}
	server, err = NewCartridgeRepoServer(oldRegistry)
	if err == nil {
		t.Error("Expected error for v3.0 schema")
	}
	if server != nil {
		t.Error("Expected no server to be created for v3.0")
	}
}

// TEST324: Transform v4 registry to flat cartridge array
func Test324_cartridge_repo_server_transform_to_array(t *testing.T) {
	versions := makeTestVersions("darwin-arm64")
	entry := CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		PageUrl:       "https://example.com",
		TeamId:        "TEAM123",
		MinAppVersion: "1.0.0",
		Caps:          []CartridgeCapSummary{},
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

// TEST325: Get all cartridges via GetCartridges()
func Test325_cartridge_repo_server_get_cartridges(t *testing.T) {
	entry := CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		Caps:          []CartridgeCapSummary{},
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

// TEST326: Get cartridge by ID
func Test326_cartridge_repo_server_get_cartridge_by_id(t *testing.T) {
	entry := CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		Caps:          []CartridgeCapSummary{},
	}

	registry := makeTestRegistry("testcartridge", entry)
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	result, err := server.GetCartridgeById("testcartridge")
	if err != nil {
		t.Fatalf("Failed to get cartridge: %v", err)
	}
	if result == nil {
		t.Fatal("Expected cartridge to be found")
	}
	if result.Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", result.Id)
	}

	notFound, err := server.GetCartridgeById("nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if notFound != nil {
		t.Error("Expected cartridge not to be found")
	}
}

// TEST327: Search cartridges by text query
func Test327_cartridge_repo_server_search_cartridges(t *testing.T) {
	entry := CartridgeRegistryEntry{
		Name:          "PDF Cartridge",
		Description:   "Process PDF documents",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		Caps:          []CartridgeCapSummary{},
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

// TEST328: Filter cartridges by category
func Test328_cartridge_repo_server_get_by_category(t *testing.T) {
	entry := CartridgeRegistryEntry{
		Name:          "Doc Cartridge",
		Description:   "Process documents",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		Caps:          []CartridgeCapSummary{},
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

// TEST329: Find cartridges by cap URN
func Test329_cartridge_repo_server_get_by_cap(t *testing.T) {
	capUrn := `cap:in="media:pdf";op=disbind;out="media:disbound-page;textable;list"`
	entry := CartridgeRegistryEntry{
		Name:          "PDF Cartridge",
		Description:   "Process PDFs",
		Author:        "Test Author",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		Caps: []CartridgeCapSummary{
			{Urn: capUrn, Title: "Disbind PDF", Description: "Extract pages"},
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

	noMatch, err := server.GetCartridgesByCap("cap:nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(noMatch) != 0 {
		t.Errorf("Expected 0 results, got %d", len(noMatch))
	}
}

// TEST330: CartridgeRepo cache update
func Test330_cartridge_repo_client_update_cache(t *testing.T) {
	repo := NewCartridgeRepo(3600)

	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:       "testcartridge",
				Name:     "Test Cartridge",
				Version:  "1.0.0",
				TeamId:   "TEAM123",
				SignedAt: "2026-02-07",
				Caps:     []CartridgeCapSummary{},
				Versions: makeTestVersions("darwin-arm64"),
			},
		},
	}

	repo.updateCache("https://example.com/cartridges", registry)

	cartridge := repo.GetCartridge("testcartridge")
	if cartridge == nil {
		t.Fatal("Expected cartridge to be found")
	}
	if cartridge.Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", cartridge.Id)
	}
}

// TEST331: Get suggestions for missing cap
func Test331_cartridge_repo_client_get_suggestions(t *testing.T) {
	repo := NewCartridgeRepo(3600)

	capUrn := `cap:in="media:pdf";op=disbind;out="media:disbound-page;textable;list"`
	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:      "pdfcartridge",
				Name:    "PDF Cartridge",
				Version: "1.0.0",
				TeamId:  "TEAM123",
				PageUrl: "https://example.com/pdf",
				Caps: []CartridgeCapSummary{
					{Urn: capUrn, Title: "Disbind PDF", Description: "Extract pages"},
				},
				Versions: makeTestVersions("darwin-arm64"),
			},
		},
	}

	repo.updateCache("https://example.com/cartridges", registry)

	suggestions := repo.GetSuggestionsForCap(capUrn)
	if len(suggestions) != 1 {
		t.Fatalf("Expected 1 suggestion, got %d", len(suggestions))
	}
	if suggestions[0].CartridgeId != "pdfcartridge" {
		t.Errorf("Expected cartridge_id 'pdfcartridge', got '%s'", suggestions[0].CartridgeId)
	}
	if suggestions[0].CapUrn != capUrn {
		t.Errorf("Expected cap_urn '%s', got '%s'", capUrn, suggestions[0].CapUrn)
	}
}

// TEST332: Get cartridge by ID from client
func Test332_cartridge_repo_client_get_cartridge(t *testing.T) {
	repo := NewCartridgeRepo(3600)

	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:      "testcartridge",
				Name:    "Test Cartridge",
				Version: "1.0.0",
				Caps:    []CartridgeCapSummary{},
				Versions: makeTestVersions("darwin-arm64"),
			},
		},
	}

	repo.updateCache("https://example.com/cartridges", registry)

	cartridge := repo.GetCartridge("testcartridge")
	if cartridge == nil {
		t.Fatal("Expected cartridge to be found")
	}
	if cartridge.Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", cartridge.Id)
	}

	notFound := repo.GetCartridge("nonexistent")
	if notFound != nil {
		t.Error("Expected cartridge not to be found")
	}
}

// TEST333: Get all available caps
func Test333_cartridge_repo_client_get_all_caps(t *testing.T) {
	repo := NewCartridgeRepo(3600)

	cap1 := `cap:in="media:pdf";op=disbind;out="media:disbound-page;textable;list"`
	cap2 := `cap:in="media:txt;textable";op=disbind;out="media:disbound-page;textable;list"`

	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:      "cartridge1",
				Name:    "Cartridge 1",
				Version: "1.0.0",
				Caps:    []CartridgeCapSummary{{Urn: cap1, Title: "Cap 1"}},
				Versions: makeTestVersions("darwin-arm64"),
			},
			{
				Id:      "cartridge2",
				Name:    "Cartridge 2",
				Version: "1.0.0",
				Caps:    []CartridgeCapSummary{{Urn: cap2, Title: "Cap 2"}},
				Versions: makeTestVersions("darwin-arm64"),
			},
		},
	}

	repo.updateCache("https://example.com/cartridges", registry)

	caps := repo.GetAllAvailableCaps()
	if len(caps) != 2 {
		t.Fatalf("Expected 2 caps, got %d", len(caps))
	}

	capFound1, capFound2 := false, false
	for _, cap := range caps {
		if cap == cap1 {
			capFound1 = true
		}
		if cap == cap2 {
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

// TEST334: Check if client needs sync
func Test334_cartridge_repo_client_needs_sync(t *testing.T) {
	repo := NewCartridgeRepo(3600)
	urls := []string{"https://example.com/cartridges"}

	if !repo.NeedsSync(urls) {
		t.Error("Expected to need sync with empty cache")
	}

	registry := &CartridgeRegistryResponse{Cartridges: []CartridgeInfo{}}
	repo.updateCache("https://example.com/cartridges", registry)

	if repo.NeedsSync(urls) {
		t.Error("Expected not to need sync after update")
	}
}

// TEST335: Server creates response, client consumes it
func Test335_cartridge_repo_server_client_integration(t *testing.T) {
	capUrn := `cap:in="media:test";op=test;out="media:result"`
	entry := CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		PageUrl:       "https://example.com",
		TeamId:        "TEAM123",
		LatestVersion: "1.0.0",
		Versions:      makeTestVersions("darwin-arm64"),
		Caps: []CartridgeCapSummary{
			{Urn: capUrn, Title: "Test Cap", Description: "Test capability"},
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
	if len(c.Caps) != 1 {
		t.Fatalf("Expected 1 cap, got %d", len(c.Caps))
	}
	if c.Caps[0].Urn != capUrn {
		t.Errorf("Expected cap URN '%s', got '%s'", capUrn, c.Caps[0].Urn)
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

// TEST630: CartridgeRepo creation starts with empty cartridge list
func Test630_cartridge_repo_creation(t *testing.T) {
	repo := NewCartridgeRepo(3600)
	if len(repo.GetAllCartridges()) != 0 {
		t.Error("Expected empty cartridge list on creation")
	}
}

// TEST631: needs_sync returns true with empty cache
func Test631_needs_sync_empty_cache(t *testing.T) {
	repo := NewCartridgeRepo(3600)
	urls := []string{"https://example.com/cartridges"}
	if !repo.NeedsSync(urls) {
		t.Error("Expected needs_sync to be true with empty cache")
	}
}
