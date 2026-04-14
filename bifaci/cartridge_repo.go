package bifaci

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// CartridgeRepoError represents errors from cartridge repository operations
type CartridgeRepoError struct {
	Kind    string
	Message string
}

func (e *CartridgeRepoError) Error() string {
	return fmt.Sprintf("%s: %s", e.Kind, e.Message)
}

// NewHttpError creates an HTTP error
func NewHttpError(msg string) *CartridgeRepoError {
	return &CartridgeRepoError{Kind: "HttpError", Message: msg}
}

// NewParseError creates a parse error
func NewParseError(msg string) *CartridgeRepoError {
	return &CartridgeRepoError{Kind: "ParseError", Message: msg}
}

// NewStatusError creates a status error
func NewStatusError(status int) *CartridgeRepoError {
	return &CartridgeRepoError{Kind: "StatusError", Message: fmt.Sprintf("Registry request failed with status %d", status)}
}

// NewNetworkBlockedError creates a network blocked error
func NewNetworkBlockedError(msg string) *CartridgeRepoError {
	return &CartridgeRepoError{Kind: "NetworkBlocked", Message: msg}
}

// CartridgeCapSummary represents a cartridge's capability summary
type CartridgeCapSummary struct {
	Urn         string `json:"urn"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
}

// CartridgeDistributionInfo represents package distribution data
type CartridgeDistributionInfo struct {
	Name   string `json:"name"`
	Sha256 string `json:"sha256"`
	Size   uint64 `json:"size"`
}

// CartridgeBuild represents a platform-specific build within a version.
type CartridgeBuild struct {
	Platform string                    `json:"platform"`
	Package  CartridgeDistributionInfo `json:"package"`
}

// CartridgeVersionData represents a cartridge version's data (v4.0 schema).
// Each version has one or more platform-specific builds.
type CartridgeVersionData struct {
	ReleaseDate   string           `json:"releaseDate"`
	Changelog     []string         `json:"changelog,omitempty"`
	MinAppVersion string           `json:"minAppVersion,omitempty"`
	Builds        []CartridgeBuild `json:"builds"`
}

// CartridgeRegistryEntry represents a cartridge entry in the v4.0 registry (nested format)
type CartridgeRegistryEntry struct {
	Name          string                          `json:"name"`
	Description   string                          `json:"description"`
	Author        string                          `json:"author"`
	PageUrl       string                          `json:"pageUrl,omitempty"`
	TeamId        string                          `json:"teamId"`
	MinAppVersion string                          `json:"minAppVersion,omitempty"`
	Caps          []CartridgeCapSummary           `json:"caps,omitempty"`
	Categories    []string                        `json:"categories,omitempty"`
	Tags          []string                        `json:"tags,omitempty"`
	LatestVersion string                          `json:"latestVersion"`
	Versions      map[string]CartridgeVersionData `json:"versions"`
}

// CartridgeRegistry represents the v4.0 cartridge registry (nested schema)
type CartridgeRegistry struct {
	SchemaVersion string                            `json:"schemaVersion"`
	LastUpdated   string                            `json:"lastUpdated"`
	Cartridges    map[string]CartridgeRegistryEntry `json:"cartridges"`
}

// CartridgeInfo represents a cartridge in the flat API response format
type CartridgeInfo struct {
	Id                string                          `json:"id"`
	Name              string                          `json:"name"`
	Version           string                          `json:"version"`
	Description       string                          `json:"description"`
	Author            string                          `json:"author"`
	Homepage          string                          `json:"homepage,omitempty"`
	TeamId            string                          `json:"teamId"`
	SignedAt          string                          `json:"signedAt"`
	MinAppVersion     string                          `json:"minAppVersion,omitempty"`
	PageUrl           string                          `json:"pageUrl,omitempty"`
	Categories        []string                        `json:"categories,omitempty"`
	Tags              []string                        `json:"tags,omitempty"`
	Caps              []CartridgeCapSummary           `json:"caps"`
	Versions          map[string]CartridgeVersionData `json:"versions"`
	AvailableVersions []string                        `json:"availableVersions,omitempty"`
}

// IsSigned checks if cartridge is signed (has team_id and signed_at)
func (p *CartridgeInfo) IsSigned() bool {
	return p.TeamId != "" && p.SignedAt != ""
}

// BuildForPlatform gets the build for a specific platform from the latest version.
func (p *CartridgeInfo) BuildForPlatform(platform string) *CartridgeBuild {
	vd, ok := p.Versions[p.Version]
	if !ok {
		return nil
	}
	for i := range vd.Builds {
		if vd.Builds[i].Platform == platform {
			return &vd.Builds[i]
		}
	}
	return nil
}

// AvailablePlatforms returns all platforms available across all versions.
func (p *CartridgeInfo) AvailablePlatforms() []string {
	seen := make(map[string]struct{})
	for _, vd := range p.Versions {
		for _, b := range vd.Builds {
			seen[b.Platform] = struct{}{}
		}
	}
	platforms := make([]string, 0, len(seen))
	for pl := range seen {
		platforms = append(platforms, pl)
	}
	sort.Strings(platforms)
	return platforms
}

// CartridgeRegistryResponse represents the cartridge registry response (flat format)
type CartridgeRegistryResponse struct {
	Cartridges []CartridgeInfo `json:"cartridges"`
}

// CartridgeSuggestion represents a cartridge suggestion for a missing cap
type CartridgeSuggestion struct {
	CartridgeId          string `json:"cartridgeId"`
	CartridgeName        string `json:"cartridgeName"`
	CartridgeDescription string `json:"cartridgeDescription"`
	CapUrn               string `json:"capUrn"`
	CapTitle             string `json:"capTitle"`
	LatestVersion        string `json:"latestVersion"`
	RepoUrl              string `json:"repoUrl"`
	PageUrl              string `json:"pageUrl"`
}

// CartridgeRepoServer serves registry data with queries.
// Transforms v4.0 nested registry schema to flat API response format.
type CartridgeRepoServer struct {
	registry CartridgeRegistry
}

// NewCartridgeRepoServer creates a new server instance from v4.0 registry
func NewCartridgeRepoServer(registry CartridgeRegistry) (*CartridgeRepoServer, error) {
	// Validate schema version - fail hard
	if registry.SchemaVersion != "4.0" {
		return nil, NewParseError(fmt.Sprintf(
			"Unsupported registry schema version: %s. Required: 4.0",
			registry.SchemaVersion,
		))
	}

	return &CartridgeRepoServer{registry: registry}, nil
}

// validateVersionData validates that version data has all required fields
func validateVersionData(id, version string, versionData *CartridgeVersionData) error {
	if len(versionData.Builds) == 0 {
		return NewParseError(fmt.Sprintf("Cartridge %s v%s: no builds", id, version))
	}
	for i, build := range versionData.Builds {
		if build.Platform == "" {
			return NewParseError(fmt.Sprintf(
				"Cartridge %s v%s: build[%d] missing platform", id, version, i,
			))
		}
		if build.Package.Name == "" {
			return NewParseError(fmt.Sprintf(
				"Cartridge %s v%s: build[%d] (%s) missing package.name", id, version, i, build.Platform,
			))
		}
	}
	return nil
}

// compareVersions compares semantic version strings; returns -1, 0, or 1
func compareVersions(a, b string) int {
	partsA := parseVersion(a)
	partsB := parseVersion(b)

	maxLen := len(partsA)
	if len(partsB) > maxLen {
		maxLen = len(partsB)
	}

	for i := 0; i < maxLen; i++ {
		var numA, numB uint32
		if i < len(partsA) {
			numA = partsA[i]
		}
		if i < len(partsB) {
			numB = partsB[i]
		}
		if numA < numB {
			return -1
		} else if numA > numB {
			return 1
		}
	}

	return 0
}

// parseVersion parses a version string into numeric parts
func parseVersion(v string) []uint32 {
	parts := strings.Split(v, ".")
	nums := make([]uint32, 0, len(parts))
	for _, p := range parts {
		if num, err := strconv.ParseUint(p, 10, 32); err == nil {
			nums = append(nums, uint32(num))
		}
	}
	return nums
}

// TransformToCartridgeArray transforms registry to flat cartridge array
func (s *CartridgeRepoServer) TransformToCartridgeArray() ([]CartridgeInfo, error) {
	result := make([]CartridgeInfo, 0, len(s.registry.Cartridges))

	for id, entry := range s.registry.Cartridges {
		latestVersion := entry.LatestVersion
		versionData, ok := entry.Versions[latestVersion]
		if !ok {
			return nil, NewParseError(fmt.Sprintf(
				"Cartridge %s: latest version %s not found in versions",
				id, latestVersion,
			))
		}

		// Validate required fields - fail hard
		if err := validateVersionData(id, latestVersion, &versionData); err != nil {
			return nil, err
		}

		// Get all versions sorted descending
		availableVersions := make([]string, 0, len(entry.Versions))
		for version := range entry.Versions {
			availableVersions = append(availableVersions, version)
		}
		sort.Slice(availableVersions, func(i, j int) bool {
			return compareVersions(availableVersions[i], availableVersions[j]) > 0
		})

		minAppVersion := versionData.MinAppVersion
		if minAppVersion == "" {
			minAppVersion = entry.MinAppVersion
		}

		caps := entry.Caps
		if caps == nil {
			caps = []CartridgeCapSummary{}
		}

		categories := entry.Categories
		if categories == nil {
			categories = []string{}
		}

		tags := entry.Tags
		if tags == nil {
			tags = []string{}
		}

		result = append(result, CartridgeInfo{
			Id:                id,
			Name:              entry.Name,
			Version:           latestVersion,
			Description:       entry.Description,
			Author:            entry.Author,
			Homepage:          "",
			TeamId:            entry.TeamId,
			SignedAt:          versionData.ReleaseDate,
			MinAppVersion:     minAppVersion,
			PageUrl:           entry.PageUrl,
			Categories:        categories,
			Tags:              tags,
			Caps:              caps,
			Versions:          entry.Versions,
			AvailableVersions: availableVersions,
		})
	}

	return result, nil
}

// GetCartridges returns all cartridges (API response format)
func (s *CartridgeRepoServer) GetCartridges() (*CartridgeRegistryResponse, error) {
	cartridges, err := s.TransformToCartridgeArray()
	if err != nil {
		return nil, err
	}
	return &CartridgeRegistryResponse{Cartridges: cartridges}, nil
}

// GetCartridgeById returns a cartridge by ID
func (s *CartridgeRepoServer) GetCartridgeById(id string) (*CartridgeInfo, error) {
	all, err := s.TransformToCartridgeArray()
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].Id == id {
			return &all[i], nil
		}
	}
	return nil, nil
}

// SearchCartridges searches cartridges by query
func (s *CartridgeRepoServer) SearchCartridges(query string) ([]CartridgeInfo, error) {
	all, err := s.TransformToCartridgeArray()
	if err != nil {
		return nil, err
	}

	lowerQuery := strings.ToLower(query)
	results := make([]CartridgeInfo, 0)

	for _, c := range all {
		if strings.Contains(strings.ToLower(c.Name), lowerQuery) ||
			strings.Contains(strings.ToLower(c.Description), lowerQuery) {
			results = append(results, c)
			continue
		}
		found := false
		for _, tag := range c.Tags {
			if strings.Contains(strings.ToLower(tag), lowerQuery) {
				found = true
				break
			}
		}
		if found {
			results = append(results, c)
			continue
		}
		for _, cap := range c.Caps {
			if strings.Contains(strings.ToLower(cap.Urn), lowerQuery) ||
				strings.Contains(strings.ToLower(cap.Title), lowerQuery) {
				found = true
				break
			}
		}
		if found {
			results = append(results, c)
		}
	}

	return results, nil
}

// GetCartridgesByCategory returns cartridges by category
func (s *CartridgeRepoServer) GetCartridgesByCategory(category string) ([]CartridgeInfo, error) {
	all, err := s.TransformToCartridgeArray()
	if err != nil {
		return nil, err
	}

	results := make([]CartridgeInfo, 0)
	for _, c := range all {
		for _, cat := range c.Categories {
			if cat == category {
				results = append(results, c)
				break
			}
		}
	}
	return results, nil
}

// GetCartridgesByCap returns cartridges that provide a specific cap
func (s *CartridgeRepoServer) GetCartridgesByCap(capUrn string) ([]CartridgeInfo, error) {
	all, err := s.TransformToCartridgeArray()
	if err != nil {
		return nil, err
	}

	results := make([]CartridgeInfo, 0)
	for _, c := range all {
		for _, cap := range c.Caps {
			if cap.Urn == capUrn {
				results = append(results, c)
				break
			}
		}
	}
	return results, nil
}

// CartridgeRepoCache holds cached cartridge repository data
type CartridgeRepoCache struct {
	cartridges      map[string]CartridgeInfo
	capToCartridges map[string][]string
	lastUpdated     time.Time
	repoUrl         string
}

// CartridgeRepo is a service for fetching and caching cartridge repository data
type CartridgeRepo struct {
	httpClient  *http.Client
	caches      map[string]*CartridgeRepoCache
	cacheTTL    time.Duration
	offlineFlag atomic.Bool
	mu          sync.RWMutex
}

// NewCartridgeRepo creates a new cartridge repo service
func NewCartridgeRepo(cacheTTLSeconds uint64) *CartridgeRepo {
	return &CartridgeRepo{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		caches:   make(map[string]*CartridgeRepoCache),
		cacheTTL: time.Duration(cacheTTLSeconds) * time.Second,
	}
}

// SetOffline sets the offline flag. When true, all registry fetches are blocked.
func (r *CartridgeRepo) SetOffline(offline bool) {
	r.offlineFlag.Store(offline)
}

// fetchRegistry fetches cartridge registry from a URL
func (r *CartridgeRepo) fetchRegistry(repoUrl string) (*CartridgeRegistryResponse, error) {
	if r.offlineFlag.Load() {
		return nil, NewNetworkBlockedError(fmt.Sprintf(
			"Network access blocked by policy — cannot fetch cartridge registry '%s'", repoUrl,
		))
	}

	resp, err := r.httpClient.Get(repoUrl)
	if err != nil {
		return nil, NewHttpError(fmt.Sprintf("Failed to fetch from %s: %v", repoUrl, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, NewStatusError(resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, NewHttpError(fmt.Sprintf("Failed to read response from %s: %v", repoUrl, err))
	}

	var registry CartridgeRegistryResponse
	if err := json.Unmarshal(body, &registry); err != nil {
		return nil, NewParseError(fmt.Sprintf("Failed to parse from %s: %v", repoUrl, err))
	}

	return &registry, nil
}

// updateCache updates cache from a registry response
func (r *CartridgeRepo) updateCache(repoUrl string, registry *CartridgeRegistryResponse) {
	cartridges := make(map[string]CartridgeInfo)
	capToCartridges := make(map[string][]string)

	for _, cartridgeInfo := range registry.Cartridges {
		cartridgeId := cartridgeInfo.Id
		for _, cap := range cartridgeInfo.Caps {
			capToCartridges[cap.Urn] = append(capToCartridges[cap.Urn], cartridgeId)
		}
		cartridges[cartridgeId] = cartridgeInfo
	}

	r.mu.Lock()
	r.caches[repoUrl] = &CartridgeRepoCache{
		cartridges:      cartridges,
		capToCartridges: capToCartridges,
		lastUpdated:     time.Now(),
		repoUrl:         repoUrl,
	}
	r.mu.Unlock()
}

// SyncRepos syncs cartridge data from the given repository URLs
func (r *CartridgeRepo) SyncRepos(repoUrls []string) {
	for _, repoUrl := range repoUrls {
		registry, err := r.fetchRegistry(repoUrl)
		if err != nil {
			// Log and continue with other repos on error
			continue
		}
		r.updateCache(repoUrl, registry)
	}
}

// isCacheStale checks if a cache is stale
func (r *CartridgeRepo) isCacheStale(cache *CartridgeRepoCache) bool {
	return time.Since(cache.lastUpdated) > r.cacheTTL
}

// GetSuggestionsForCap gets cartridge suggestions for a cap URN
func (r *CartridgeRepo) GetSuggestionsForCap(capUrn string) []CartridgeSuggestion {
	r.mu.RLock()
	defer r.mu.RUnlock()

	suggestions := make([]CartridgeSuggestion, 0)

	for _, cache := range r.caches {
		cartridgeIds, ok := cache.capToCartridges[capUrn]
		if !ok {
			continue
		}

		for _, cartridgeId := range cartridgeIds {
			cartridge, ok := cache.cartridges[cartridgeId]
			if !ok {
				continue
			}

			for _, capInfo := range cartridge.Caps {
				if capInfo.Urn == capUrn {
					pageUrl := cartridge.PageUrl
					if pageUrl == "" {
						pageUrl = cache.repoUrl
					}
					suggestions = append(suggestions, CartridgeSuggestion{
						CartridgeId:          cartridgeId,
						CartridgeName:        cartridge.Name,
						CartridgeDescription: cartridge.Description,
						CapUrn:               capUrn,
						CapTitle:             capInfo.Title,
						LatestVersion:        cartridge.Version,
						RepoUrl:              cache.repoUrl,
						PageUrl:              pageUrl,
					})
					break
				}
			}
		}
	}

	return suggestions
}

// GetAllCartridges gets all available cartridges from all repos
func (r *CartridgeRepo) GetAllCartridges() []CartridgeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cartridges := make([]CartridgeInfo, 0)
	for _, cache := range r.caches {
		for _, cartridge := range cache.cartridges {
			cartridges = append(cartridges, cartridge)
		}
	}
	return cartridges
}

// GetAllAvailableCaps gets all caps available from cartridges
func (r *CartridgeRepo) GetAllAvailableCaps() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	capsSet := make(map[string]bool)
	for _, cache := range r.caches {
		for cap := range cache.capToCartridges {
			capsSet[cap] = true
		}
	}

	caps := make([]string, 0, len(capsSet))
	for cap := range capsSet {
		caps = append(caps, cap)
	}
	sort.Strings(caps)
	return caps
}

// NeedsSync checks if any repo needs syncing (cache missing or stale)
func (r *CartridgeRepo) NeedsSync(repoUrls []string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, repoUrl := range repoUrls {
		cache, ok := r.caches[repoUrl]
		if !ok {
			return true
		}
		if r.isCacheStale(cache) {
			return true
		}
	}
	return false
}

// GetCartridge gets cartridge info by ID
func (r *CartridgeRepo) GetCartridge(cartridgeId string) *CartridgeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, cache := range r.caches {
		if cartridge, ok := cache.cartridges[cartridgeId]; ok {
			return &cartridge
		}
	}
	return nil
}

// GetSuggestionsForMissingCaps returns suggestions for caps that aren't currently available.
func (r *CartridgeRepo) GetSuggestionsForMissingCaps(availableCaps, requestedCaps []string) []CartridgeSuggestion {
	availableSet := make(map[string]bool, len(availableCaps))
	for _, cap := range availableCaps {
		availableSet[cap] = true
	}

	var suggestions []CartridgeSuggestion
	for _, capUrn := range requestedCaps {
		if !availableSet[capUrn] {
			suggestions = append(suggestions, r.GetSuggestionsForCap(capUrn)...)
		}
	}
	return suggestions
}
