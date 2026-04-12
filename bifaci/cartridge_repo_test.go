package bifaci

import (
	"testing"
)

// TEST320: Construct CartridgeInfo and verify fields
func Test320_cartridge_info_construction(t *testing.T) {
	// TEST320: Construct CartridgeInfo and verify fields
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
		Platform:          "darwin-arm64",
		PackageName:       "test-1.0.0.pkg",
		PackageSha256:     "abc123",
		PackageSize:       1000,
		BinaryName:        "test-1.0.0-darwin-arm64",
		BinarySha256:      "def456",
		BinarySize:        2000,
		Changelog:         make(map[string][]string),
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
	// TEST321: Verify IsSigned() method
	cartridge := CartridgeInfo{
		Id:                "testcartridge",
		Name:              "Test",
		Version:           "1.0.0",
		Description:       "",
		Author:            "",
		Homepage:          "",
		TeamId:            "TEAM123",
		SignedAt:          "2026-02-07T00:00:00Z",
		MinAppVersion:     "",
		PageUrl:           "",
		Categories:        []string{},
		Tags:              []string{},
		Caps:              []CartridgeCapSummary{},
		Platform:          "",
		PackageName:       "",
		PackageSha256:     "",
		PackageSize:       0,
		BinaryName:        "",
		BinarySha256:      "",
		BinarySize:        0,
		Changelog:         make(map[string][]string),
		AvailableVersions: []string{},
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

// TEST322: Verify HasBinary() method
func Test322_cartridge_info_has_binary(t *testing.T) {
	// TEST322: Verify HasBinary() method
	cartridge := CartridgeInfo{
		Id:                "testcartridge",
		Name:              "Test",
		Version:           "1.0.0",
		Description:       "",
		Author:            "",
		Homepage:          "",
		TeamId:            "",
		SignedAt:          "",
		MinAppVersion:     "",
		PageUrl:           "",
		Categories:        []string{},
		Tags:              []string{},
		Caps:              []CartridgeCapSummary{},
		Platform:          "",
		PackageName:       "",
		PackageSha256:     "",
		PackageSize:       0,
		BinaryName:        "test-1.0.0",
		BinarySha256:      "abc123",
		BinarySize:        0,
		Changelog:         make(map[string][]string),
		AvailableVersions: []string{},
	}

	if !cartridge.HasBinary() {
		t.Error("Expected cartridge to have binary")
	}

	cartridge.BinaryName = ""
	if cartridge.HasBinary() {
		t.Error("Expected cartridge not to have binary when binary_name is empty")
	}

	cartridge.BinaryName = "test-1.0.0"
	cartridge.BinarySha256 = ""
	if cartridge.HasBinary() {
		t.Error("Expected cartridge not to have binary when binary_sha256 is empty")
	}
}

// TEST323: Validate registry schema version
func Test323_cartridge_repo_server_validate_registry(t *testing.T) {
	// TEST323: Validate registry schema version
	registry := CartridgeRegistryV3{
		SchemaVersion: "3.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    make(map[string]CartridgeRegistryEntry),
	}

	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Errorf("Expected no error for v3.0, got %v", err)
	}
	if server == nil {
		t.Error("Expected server to be created")
	}

	// Test v2.0 schema rejection
	oldRegistry := CartridgeRegistryV3{
		SchemaVersion: "2.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    make(map[string]CartridgeRegistryEntry),
	}

	server, err = NewCartridgeRepoServer(oldRegistry)
	if err == nil {
		t.Error("Expected error for v2.0 schema")
	}
	if server != nil {
		t.Error("Expected no server to be created for v2.0")
	}
}

// TEST324: Transform v3 registry to flat cartridge array
func Test324_cartridge_repo_server_transform_to_array(t *testing.T) {
	// TEST324: Transform v3 registry to flat cartridge array
	cartridges := make(map[string]CartridgeRegistryEntry)
	versions := make(map[string]CartridgeVersionData)

	versions["1.0.0"] = CartridgeVersionData{
		ReleaseDate:   "2026-02-07",
		Changelog:     []string{"Initial release"},
		MinAppVersion: "1.0.0",
		Platform:      "darwin-arm64",
		Package: CartridgeDistributionInfo{
			Name:   "test-1.0.0.pkg",
			Sha256: "abc123",
			Size:   1000,
		},
		Binary: CartridgeDistributionInfo{
			Name:   "test-1.0.0-darwin-arm64",
			Sha256: "def456",
			Size:   2000,
		},
	}

	cartridges["testcartridge"] = CartridgeRegistryEntry{
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

	registry := CartridgeRegistryV3{
		SchemaVersion: "3.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    cartridges,
	}

	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	cartridgesArray, err := server.TransformToCartridgeArray()
	if err != nil {
		t.Fatalf("Failed to transform: %v", err)
	}

	if len(cartridgesArray) != 1 {
		t.Fatalf("Expected 1 cartridge, got %d", len(cartridgesArray))
	}
	if cartridgesArray[0].Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", cartridgesArray[0].Id)
	}
	if cartridgesArray[0].Name != "Test Cartridge" {
		t.Errorf("Expected name 'Test Cartridge', got '%s'", cartridgesArray[0].Name)
	}
	if cartridgesArray[0].Version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", cartridgesArray[0].Version)
	}
	if cartridgesArray[0].BinaryName != "test-1.0.0-darwin-arm64" {
		t.Errorf("Expected binary_name 'test-1.0.0-darwin-arm64', got '%s'", cartridgesArray[0].BinaryName)
	}
}

// TEST325: Get all cartridges via GetCartridges()
func Test325_cartridge_repo_server_get_cartridges(t *testing.T) {
	// TEST325: Get all cartridges via GetCartridges()
	cartridges := make(map[string]CartridgeRegistryEntry)
	versions := make(map[string]CartridgeVersionData)

	versions["1.0.0"] = CartridgeVersionData{
		ReleaseDate:   "2026-02-07",
		Changelog:     []string{},
		MinAppVersion: "",
		Platform:      "darwin-arm64",
		Package: CartridgeDistributionInfo{
			Name:   "test-1.0.0.pkg",
			Sha256: "abc123",
			Size:   1000,
		},
		Binary: CartridgeDistributionInfo{
			Name:   "test-1.0.0-darwin-arm64",
			Sha256: "def456",
			Size:   2000,
		},
	}

	cartridges["testcartridge"] = CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		PageUrl:       "",
		TeamId:        "TEAM123",
		MinAppVersion: "",
		Caps:          []CartridgeCapSummary{},
		Categories:    []string{},
		Tags:          []string{},
		LatestVersion: "1.0.0",
		Versions:      versions,
	}

	registry := CartridgeRegistryV3{
		SchemaVersion: "3.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    cartridges,
	}

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
	// TEST326: Get cartridge by ID
	cartridges := make(map[string]CartridgeRegistryEntry)
	versions := make(map[string]CartridgeVersionData)

	versions["1.0.0"] = CartridgeVersionData{
		ReleaseDate:   "2026-02-07",
		Changelog:     []string{},
		MinAppVersion: "",
		Platform:      "darwin-arm64",
		Package: CartridgeDistributionInfo{
			Name:   "test-1.0.0.pkg",
			Sha256: "abc123",
			Size:   1000,
		},
		Binary: CartridgeDistributionInfo{
			Name:   "test-1.0.0-darwin-arm64",
			Sha256: "def456",
			Size:   2000,
		},
	}

	cartridges["testcartridge"] = CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		PageUrl:       "",
		TeamId:        "TEAM123",
		MinAppVersion: "",
		Caps:          []CartridgeCapSummary{},
		Categories:    []string{},
		Tags:          []string{},
		LatestVersion: "1.0.0",
		Versions:      versions,
	}

	registry := CartridgeRegistryV3{
		SchemaVersion: "3.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    cartridges,
	}

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
		t.Fatalf("Failed to get cartridge: %v", err)
	}
	if notFound != nil {
		t.Error("Expected cartridge not to be found")
	}
}

// TEST327: Search cartridges by text query
func Test327_cartridge_repo_server_search_cartridges(t *testing.T) {
	// TEST327: Search cartridges by text query
	cartridges := make(map[string]CartridgeRegistryEntry)
	versions := make(map[string]CartridgeVersionData)

	versions["1.0.0"] = CartridgeVersionData{
		ReleaseDate:   "2026-02-07",
		Changelog:     []string{},
		MinAppVersion: "",
		Platform:      "darwin-arm64",
		Package: CartridgeDistributionInfo{
			Name:   "pdf-1.0.0.pkg",
			Sha256: "abc123",
			Size:   1000,
		},
		Binary: CartridgeDistributionInfo{
			Name:   "pdf-1.0.0-darwin-arm64",
			Sha256: "def456",
			Size:   2000,
		},
	}

	cartridges["pdfcartridge"] = CartridgeRegistryEntry{
		Name:          "PDF Cartridge",
		Description:   "Process PDF documents",
		Author:        "Test Author",
		PageUrl:       "",
		TeamId:        "TEAM123",
		MinAppVersion: "",
		Caps:          []CartridgeCapSummary{},
		Categories:    []string{},
		Tags:          []string{"document"},
		LatestVersion: "1.0.0",
		Versions:      versions,
	}

	registry := CartridgeRegistryV3{
		SchemaVersion: "3.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    cartridges,
	}

	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	results, err := server.SearchCartridges("pdf")
	if err != nil {
		t.Fatalf("Failed to search cartridges: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].Id != "pdfcartridge" {
		t.Errorf("Expected id 'pdfcartridge', got '%s'", results[0].Id)
	}

	noMatch, err := server.SearchCartridges("nonexistent")
	if err != nil {
		t.Fatalf("Failed to search cartridges: %v", err)
	}
	if len(noMatch) != 0 {
		t.Errorf("Expected 0 results, got %d", len(noMatch))
	}
}

// TEST328: Filter cartridges by category
func Test328_cartridge_repo_server_get_by_category(t *testing.T) {
	// TEST328: Filter cartridges by category
	cartridges := make(map[string]CartridgeRegistryEntry)
	versions := make(map[string]CartridgeVersionData)

	versions["1.0.0"] = CartridgeVersionData{
		ReleaseDate:   "2026-02-07",
		Changelog:     []string{},
		MinAppVersion: "",
		Platform:      "darwin-arm64",
		Package: CartridgeDistributionInfo{
			Name:   "test-1.0.0.pkg",
			Sha256: "abc123",
			Size:   1000,
		},
		Binary: CartridgeDistributionInfo{
			Name:   "test-1.0.0-darwin-arm64",
			Sha256: "def456",
			Size:   2000,
		},
	}

	cartridges["doccartridge"] = CartridgeRegistryEntry{
		Name:          "Doc Cartridge",
		Description:   "Process documents",
		Author:        "Test Author",
		PageUrl:       "",
		TeamId:        "TEAM123",
		MinAppVersion: "",
		Caps:          []CartridgeCapSummary{},
		Categories:    []string{"document"},
		Tags:          []string{},
		LatestVersion: "1.0.0",
		Versions:      versions,
	}

	registry := CartridgeRegistryV3{
		SchemaVersion: "3.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    cartridges,
	}

	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	results, err := server.GetCartridgesByCategory("document")
	if err != nil {
		t.Fatalf("Failed to get cartridges by category: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].Id != "doccartridge" {
		t.Errorf("Expected id 'doccartridge', got '%s'", results[0].Id)
	}

	noMatch, err := server.GetCartridgesByCategory("nonexistent")
	if err != nil {
		t.Fatalf("Failed to get cartridges by category: %v", err)
	}
	if len(noMatch) != 0 {
		t.Errorf("Expected 0 results, got %d", len(noMatch))
	}
}

// TEST329: Find cartridges by cap URN
func Test329_cartridge_repo_server_get_by_cap(t *testing.T) {
	// TEST329: Find cartridges by cap URN
	cartridges := make(map[string]CartridgeRegistryEntry)
	versions := make(map[string]CartridgeVersionData)

	versions["1.0.0"] = CartridgeVersionData{
		ReleaseDate:   "2026-02-07",
		Changelog:     []string{},
		MinAppVersion: "",
		Platform:      "darwin-arm64",
		Package: CartridgeDistributionInfo{
			Name:   "test-1.0.0.pkg",
			Sha256: "abc123",
			Size:   1000,
		},
		Binary: CartridgeDistributionInfo{
			Name:   "test-1.0.0-darwin-arm64",
			Sha256: "def456",
			Size:   2000,
		},
	}

	capUrn := `cap:in="media:pdf";op=disbind;out="media:disbound-page;textable;list"`
	cartridges["pdfcartridge"] = CartridgeRegistryEntry{
		Name:          "PDF Cartridge",
		Description:   "Process PDFs",
		Author:        "Test Author",
		PageUrl:       "",
		TeamId:        "TEAM123",
		MinAppVersion: "",
		Caps: []CartridgeCapSummary{
			{
				Urn:         capUrn,
				Title:       "Disbind PDF",
				Description: "Extract pages",
			},
		},
		Categories:    []string{},
		Tags:          []string{},
		LatestVersion: "1.0.0",
		Versions:      versions,
	}

	registry := CartridgeRegistryV3{
		SchemaVersion: "3.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    cartridges,
	}

	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	results, err := server.GetCartridgesByCap(capUrn)
	if err != nil {
		t.Fatalf("Failed to get cartridges by cap: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].Id != "pdfcartridge" {
		t.Errorf("Expected id 'pdfcartridge', got '%s'", results[0].Id)
	}

	noMatch, err := server.GetCartridgesByCap("cap:nonexistent")
	if err != nil {
		t.Fatalf("Failed to get cartridges by cap: %v", err)
	}
	if len(noMatch) != 0 {
		t.Errorf("Expected 0 results, got %d", len(noMatch))
	}
}

// TEST330: CartridgeRepoClient cache update
func Test330_cartridge_repo_client_update_cache(t *testing.T) {
	// TEST330: CartridgeRepoClient cache update
	repo := NewCartridgeRepo(3600)

	// Create a mock registry response
	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:                "testcartridge",
				Name:              "Test Cartridge",
				Version:           "1.0.0",
				Description:       "",
				Author:            "",
				Homepage:          "",
				TeamId:            "TEAM123",
				SignedAt:          "2026-02-07",
				MinAppVersion:     "",
				PageUrl:           "",
				Categories:        []string{},
				Tags:              []string{},
				Caps:              []CartridgeCapSummary{},
				Platform:          "",
				PackageName:       "",
				PackageSha256:     "",
				PackageSize:       0,
				BinaryName:        "test-binary",
				BinarySha256:      "abc123",
				BinarySize:        0,
				Changelog:         make(map[string][]string),
				AvailableVersions: []string{},
			},
		},
	}

	// Update cache directly (simulating a fetch)
	repo.updateCache("https://example.com/cartridges", registry)

	// Verify cache was updated
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
	// TEST331: Get suggestions for missing cap
	repo := NewCartridgeRepo(3600)

	capUrn := `cap:in="media:pdf";op=disbind;out="media:disbound-page;textable;list"`
	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:            "pdfcartridge",
				Name:          "PDF Cartridge",
				Version:       "1.0.0",
				Description:   "Process PDFs",
				Author:        "",
				Homepage:      "",
				TeamId:        "TEAM123",
				SignedAt:      "2026-02-07",
				MinAppVersion: "",
				PageUrl:       "https://example.com/pdf",
				Categories:    []string{},
				Tags:          []string{},
				Caps: []CartridgeCapSummary{
					{
						Urn:         capUrn,
						Title:       "Disbind PDF",
						Description: "Extract pages",
					},
				},
				Platform:          "",
				PackageName:       "",
				PackageSha256:     "",
				PackageSize:       0,
				BinaryName:        "",
				BinarySha256:      "",
				BinarySize:        0,
				Changelog:         make(map[string][]string),
				AvailableVersions: []string{},
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
	// TEST332: Get cartridge by ID from client
	repo := NewCartridgeRepo(3600)

	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:                "testcartridge",
				Name:              "Test Cartridge",
				Version:           "1.0.0",
				Description:       "",
				Author:            "",
				Homepage:          "",
				TeamId:            "",
				SignedAt:          "",
				MinAppVersion:     "",
				PageUrl:           "",
				Categories:        []string{},
				Tags:              []string{},
				Caps:              []CartridgeCapSummary{},
				Platform:          "",
				PackageName:       "",
				PackageSha256:     "",
				PackageSize:       0,
				BinaryName:        "",
				BinarySha256:      "",
				BinarySize:        0,
				Changelog:         make(map[string][]string),
				AvailableVersions: []string{},
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
	// TEST333: Get all available caps
	repo := NewCartridgeRepo(3600)

	cap1 := `cap:in="media:pdf";op=disbind;out="media:disbound-page;textable;list"`
	cap2 := `cap:in="media:txt;textable";op=disbind;out="media:disbound-page;textable;list"`

	registry := &CartridgeRegistryResponse{
		Cartridges: []CartridgeInfo{
			{
				Id:            "cartridge1",
				Name:          "Cartridge 1",
				Version:       "1.0.0",
				Description:   "",
				Author:        "",
				Homepage:      "",
				TeamId:        "",
				SignedAt:      "",
				MinAppVersion: "",
				PageUrl:       "",
				Categories:    []string{},
				Tags:          []string{},
				Caps: []CartridgeCapSummary{
					{
						Urn:         cap1,
						Title:       "cap.Cap 1",
						Description: "",
					},
				},
				Platform:          "",
				PackageName:       "",
				PackageSha256:     "",
				PackageSize:       0,
				BinaryName:        "",
				BinarySha256:      "",
				BinarySize:        0,
				Changelog:         make(map[string][]string),
				AvailableVersions: []string{},
			},
			{
				Id:            "cartridge2",
				Name:          "Cartridge 2",
				Version:       "1.0.0",
				Description:   "",
				Author:        "",
				Homepage:      "",
				TeamId:        "",
				SignedAt:      "",
				MinAppVersion: "",
				PageUrl:       "",
				Categories:    []string{},
				Tags:          []string{},
				Caps: []CartridgeCapSummary{
					{
						Urn:         cap2,
						Title:       "cap.Cap 2",
						Description: "",
					},
				},
				Platform:          "",
				PackageName:       "",
				PackageSha256:     "",
				PackageSize:       0,
				BinaryName:        "",
				BinarySha256:      "",
				BinarySize:        0,
				Changelog:         make(map[string][]string),
				AvailableVersions: []string{},
			},
		},
	}

	repo.updateCache("https://example.com/cartridges", registry)

	caps := repo.GetAllAvailableCaps()
	if len(caps) != 2 {
		t.Fatalf("Expected 2 caps, got %d", len(caps))
	}

	// Check both caps are present
	capFound1 := false
	capFound2 := false
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
	// TEST334: Check if client needs sync
	repo := NewCartridgeRepo(3600)

	urls := []string{"https://example.com/cartridges"}

	// Empty cache should need sync
	if !repo.NeedsSync(urls) {
		t.Error("Expected to need sync with empty cache")
	}

	// After update, should not need sync
	registry := &CartridgeRegistryResponse{Cartridges: []CartridgeInfo{}}
	repo.updateCache("https://example.com/cartridges", registry)

	if repo.NeedsSync(urls) {
		t.Error("Expected not to need sync after update")
	}
}

// TEST335: Server creates response, client consumes it
func Test335_cartridge_repo_server_client_integration(t *testing.T) {
	// TEST335: Server creates response, client consumes it
	cartridges := make(map[string]CartridgeRegistryEntry)
	versions := make(map[string]CartridgeVersionData)

	versions["1.0.0"] = CartridgeVersionData{
		ReleaseDate:   "2026-02-07",
		Changelog:     []string{},
		MinAppVersion: "",
		Platform:      "darwin-arm64",
		Package: CartridgeDistributionInfo{
			Name:   "test-1.0.0.pkg",
			Sha256: "abc123",
			Size:   1000,
		},
		Binary: CartridgeDistributionInfo{
			Name:   "test-1.0.0-darwin-arm64",
			Sha256: "def456",
			Size:   2000,
		},
	}

	capUrn := `cap:in="media:test";op=test;out="media:result"`
	cartridges["testcartridge"] = CartridgeRegistryEntry{
		Name:          "Test Cartridge",
		Description:   "A test cartridge",
		Author:        "Test Author",
		PageUrl:       "https://example.com",
		TeamId:        "TEAM123",
		MinAppVersion: "",
		Caps: []CartridgeCapSummary{
			{
				Urn:         capUrn,
				Title:       "Test cap.Cap",
				Description: "Test capability",
			},
		},
		Categories:    []string{"test"},
		Tags:          []string{},
		LatestVersion: "1.0.0",
		Versions:      versions,
	}

	registry := CartridgeRegistryV3{
		SchemaVersion: "3.0",
		LastUpdated:   "2026-02-07",
		Cartridges:    cartridges,
	}

	// Server transforms registry
	server, err := NewCartridgeRepoServer(registry)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	response, err := server.GetCartridges()
	if err != nil {
		t.Fatalf("Failed to get cartridges: %v", err)
	}

	// Verify response structure
	if len(response.Cartridges) != 1 {
		t.Fatalf("Expected 1 cartridge, got %d", len(response.Cartridges))
	}

	cartridge := &response.Cartridges[0]
	if cartridge.Id != "testcartridge" {
		t.Errorf("Expected id 'testcartridge', got '%s'", cartridge.Id)
	}
	if cartridge.Name != "Test Cartridge" {
		t.Errorf("Expected name 'Test Cartridge', got '%s'", cartridge.Name)
	}
	if !cartridge.IsSigned() {
		t.Error("Expected cartridge to be signed")
	}
	if !cartridge.HasBinary() {
		t.Error("Expected cartridge to have binary")
	}
	if len(cartridge.Caps) != 1 {
		t.Fatalf("Expected 1 cap, got %d", len(cartridge.Caps))
	}
	if cartridge.Caps[0].Urn != capUrn {
		t.Errorf("Expected cap URN '%s', got '%s'", capUrn, cartridge.Caps[0].Urn)
	}

	// Simulate client consuming this response
	if cartridge.BinaryName != "test-1.0.0-darwin-arm64" {
		t.Errorf("Expected binary_name 'test-1.0.0-darwin-arm64', got '%s'", cartridge.BinaryName)
	}
	if cartridge.BinarySha256 != "def456" {
		t.Errorf("Expected binary_sha256 'def456', got '%s'", cartridge.BinarySha256)
	}
}
