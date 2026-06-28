package cap

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

const (
	DefaultRegistryBaseURL = "https://fabric.capdag.com"
	CacheDurationHours     = 24
	HTTPTimeoutSeconds     = 10
)

// RegistryConfig holds configuration for the registry client
type RegistryConfig struct {
	RegistryBaseURL string
	SchemaBaseURL   string
}

// DefaultRegistryConfig returns config from environment variables or defaults
//
// Environment variables:
//   - CDG_FABRIC_REGISTRY_URL: Base URL for the registry (default: https://fabric.capdag.com)
//   - CDG_SCHEMA_BASE_URL: Base URL for schemas (default: {registry_url}/schema)
func DefaultRegistryConfig() RegistryConfig {
	registryBase := os.Getenv("CDG_FABRIC_REGISTRY_URL")
	if registryBase == "" {
		registryBase = DefaultRegistryBaseURL
	}

	schemaBase := os.Getenv("CDG_SCHEMA_BASE_URL")
	if schemaBase == "" {
		schemaBase = registryBase + "/schema"
	}

	return RegistryConfig{
		RegistryBaseURL: registryBase,
		SchemaBaseURL:   schemaBase,
	}
}

// RegistryOption is a functional option for configuring the registry
type RegistryOption func(*RegistryConfig)

// WithRegistryURL sets a custom registry URL
func WithRegistryURL(url string) RegistryOption {
	return func(c *RegistryConfig) {
		// If schema URL was derived from the old registry URL, update it
		if c.SchemaBaseURL == c.RegistryBaseURL+"/schema" {
			c.SchemaBaseURL = url + "/schema"
		}
		c.RegistryBaseURL = url
	}
}

// WithSchemaURL sets a custom schema base URL
func WithSchemaURL(url string) RegistryOption {
	return func(c *RegistryConfig) {
		c.SchemaBaseURL = url
	}
}

// CacheEntry represents a cached cap definition
type CacheEntry struct {
	Definition Cap   `json:"definition"`
	CachedAt   int64 `json:"cached_at"`
	TTLHours   int64 `json:"ttl_hours"`
}

func (e *CacheEntry) isExpired() bool {
	return time.Now().Unix() > e.CachedAt+(e.TTLHours*3600)
}

// RegistryCapResponse represents the per-cap JSON body served at
// /caps/<sha256>. The wire shape is the flattened cap entry; fields
// beyond what we explicitly model below (urn_tags, in_spec, out_spec,
// in_media_title, out_media_title, media_defs, registered_by,
// documentation) are silently ignored by Go's JSON unmarshaller — the
// Go Cap type only carries the subset below.
type RegistryCapResponse struct {
	Urn            string            `json:"urn"` // URN in canonical string format
	Title          string            `json:"title"`
	Version        string            `json:"version"`
	CapDescription *string           `json:"cap_description,omitempty"`
	Metadata       map[string]string `json:"metadata"`
	Command        string            `json:"command"`
	Args           []CapArg          `json:"args,omitempty"`
	Output         *CapOutput        `json:"output,omitempty"`
}

// ToCap converts a registry response to a standard Cap
func (r *RegistryCapResponse) ToCap() (*Cap, error) {
	// URN must be a string in canonical format
	capUrn, err := urn.NewCapUrnFromString(r.Urn)
	if err != nil {
		return nil, fmt.Errorf("invalid URN string: %w", err)
	}

	// Use title from the response
	title := r.Title
	if title == "" {
		title = "Registry Capability"
	}

	cap := NewCap(capUrn, title, r.Command)
	cap.CapDescription = r.CapDescription
	if r.Metadata != nil {
		cap.Metadata = r.Metadata
	}
	cap.Args = r.Args
	cap.Output = r.Output

	return cap, nil
}

// FabricRegistry handles communication with the capdag registry
type FabricRegistry struct {
	client        *http.Client
	cacheDir      string
	cachedCaps    map[string]*Cap
	cachedAliases map[string]media.StoredAlias
	mutex         sync.RWMutex
	config        RegistryConfig
	// manifestVersion is the pinned fabric manifest version. 0 ⇒ legacy v0 /
	// flat-path mode. >= 1 ⇒ manifest-driven (alias resolution requires >= 1).
	manifestVersion uint32
	manifest        *media.Manifest
	// offline, when true, blocks all network fetches. Cached caps remain
	// accessible; uncached caps fail with a NetworkBlocked-style error.
	// Mirrors Rust's offline_flag.
	offline bool
}

// SetOffline toggles offline mode. When offline, fetchFromRegistry refuses to
// make network requests; cached caps stay accessible via the in-memory cache.
// Mirrors Rust's FabricRegistry::set_offline.
func (r *FabricRegistry) SetOffline(offline bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.offline = offline
}

// NewFabricRegistry creates a new registry client
//
// Accepts optional RegistryOption functions to configure the registry.
// Without options, uses environment variables or defaults.
//
// Example:
//
//	registry, err := NewFabricRegistry()  // Uses env vars or defaults
//	registry, err := NewFabricRegistry(WithRegistryURL("https://my-registry.com"))
func NewFabricRegistry(opts ...RegistryOption) (*FabricRegistry, error) {
	config := DefaultRegistryConfig()
	for _, opt := range opts {
		opt(&config)
	}

	cacheDir, err := getCacheDir(config.RegistryBaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to determine cache directory: %w", err)
	}

	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	client := &http.Client{
		Timeout: HTTPTimeoutSeconds * time.Second,
	}

	// Load all cached caps into memory
	cachedCaps, err := loadAllCachedCaps(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load cached caps: %w", err)
	}

	return &FabricRegistry{
		client:          client,
		cacheDir:        cacheDir,
		cachedCaps:      cachedCaps,
		cachedAliases:   make(map[string]media.StoredAlias),
		config:          config,
		manifestVersion: 0,
		manifest:        media.EmptyManifest(0),
	}, nil
}

// Config returns the current registry configuration
func (r *FabricRegistry) Config() RegistryConfig {
	return r.config
}

// GetCap gets a cap from in-memory cache or fetch from registry.
//
// The argument may be a cap URN (cap:...) or an alias (a colon-free token).
// An alias is resolved first; because this is the typed cap boundary, an
// alias whose target is not a cap URN is a hard error.
func (r *FabricRegistry) GetCap(urnStr string) (*Cap, error) {
	if media.IsAliasToken(urnStr) {
		target, err := r.ResolveAliasTyped(urnStr, media.AliasTargetCap)
		if err != nil {
			return nil, err
		}
		return r.GetCap(target)
	}

	// Normalize the lookup URN the same way AddCapsToCache normalizes the
	// storage key, so a cap cached under its canonical (tag-sorted) form is
	// found regardless of the tag order the caller passes. Without this a
	// cached cap is unreachable via a non-canonical lookup string, falling
	// through to a network fetch (and failing hard when offline). Mirrors
	// Rust get_cap's normalize_cap_urn.
	//
	// A malformed URN is a hard error here: this path returns an error, so the
	// parse failure PROPAGATES rather than silently keeping the raw string —
	// otherwise a bad URN surfaces later as a misleading "not in manifest" /
	// cache-miss instead of the truth.
	normalized, err := normalizeCapUrn(urnStr)
	if err != nil {
		return nil, err
	}

	// Check in-memory cache first
	r.mutex.RLock()
	if cap, exists := r.cachedCaps[normalized]; exists {
		r.mutex.RUnlock()
		return cap, nil
	}
	r.mutex.RUnlock()

	// Not in cache, fetch from registry and update in-memory cache
	cap, err := r.fetchFromRegistry(normalized)
	if err != nil {
		return nil, err
	}

	// Update in-memory cache
	r.mutex.Lock()
	r.cachedCaps[normalized] = cap
	r.mutex.Unlock()

	return cap, nil
}

// GetCaps gets multiple caps at once - fails if any cap is not available
func (r *FabricRegistry) GetCaps(urns []string) ([]*Cap, error) {
	var caps []*Cap
	for _, urn := range urns {
		cap, err := r.GetCap(urn)
		if err != nil {
			return nil, err
		}
		caps = append(caps, cap)
	}
	return caps, nil
}

// ValidateCap validates a local cap against its canonical definition
func (r *FabricRegistry) ValidateCap(cap *Cap) error {
	canonicalCap, err := r.GetCap(cap.UrnString())
	if err != nil {
		return err
	}

	if cap.Command != canonicalCap.Command {
		return fmt.Errorf("command mismatch. Local: %s, Canonical: %s", cap.Command, canonicalCap.Command)
	}

	// Compare stdin (from args with stdin sources)
	localStdinUrn := cap.GetStdinMediaUrn()
	canonicalStdinUrn := canonicalCap.GetStdinMediaUrn()
	if (localStdinUrn == nil) != (canonicalStdinUrn == nil) {
		localStdin := "<none>"
		canonicalStdin := "<none>"
		if localStdinUrn != nil {
			localStdin = *localStdinUrn
		}
		if canonicalStdinUrn != nil {
			canonicalStdin = *canonicalStdinUrn
		}
		return fmt.Errorf("stdin mismatch. Local: %s, Canonical: %s", localStdin, canonicalStdin)
	}
	if localStdinUrn != nil && *localStdinUrn != *canonicalStdinUrn {
		return fmt.Errorf("stdin mismatch. Local: %s, Canonical: %s", *localStdinUrn, *canonicalStdinUrn)
	}

	return nil
}

// CapExists checks if a cap URN exists in registry (either cached or available online)
func (r *FabricRegistry) CapExists(urn string) bool {
	_, err := r.GetCap(urn)
	return err == nil
}

// GetCachedCaps gets all currently cached caps from in-memory cache
func (r *FabricRegistry) GetCachedCaps() []*Cap {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	caps := make([]*Cap, 0, len(r.cachedCaps))
	for _, cap := range r.cachedCaps {
		caps = append(caps, cap)
	}
	return caps
}

// GetCachedCap returns a cap from the in-memory cache synchronously.
// Returns (*Cap, true) if found, (nil, false) otherwise.
func (r *FabricRegistry) GetCachedCap(capUrn string) (*Cap, bool) {
	// Lookup contract is (value, found). A malformed URN can never match a
	// canonically-keyed cache entry, so treat it as a miss — but log it rather
	// than silently keying on the raw string, which would hide the malformation.
	normalized, err := normalizeCapUrn(capUrn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] GetCachedCap: %v\n", err)
		return nil, false
	}
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	cap, ok := r.cachedCaps[normalized]
	if !ok {
		return nil, false
	}
	return cap, true
}

// ClearCache removes all cached registry definitions
func (r *FabricRegistry) ClearCache() error {
	// Clear in-memory cache
	r.mutex.Lock()
	r.cachedCaps = make(map[string]*Cap)
	r.cachedAliases = make(map[string]media.StoredAlias)
	r.mutex.Unlock()

	// Clear filesystem cache
	if err := os.RemoveAll(r.cacheDir); err != nil {
		return fmt.Errorf("failed to clear cache: %w", err)
	}
	return os.MkdirAll(r.cacheDir, 0755)
}

// Private helper methods

// normalizeCapUrn parses a cap URN and returns its canonical (tag-sorted)
// string form. A parse failure is a HARD error — it is NEVER silently swallowed
// into the raw string, which would let a malformed URN masquerade as a
// cache-miss / "not in manifest" downstream. Callers on a path that returns an
// error propagate this; lookup/void paths log and skip. This never panics.
//
// Mirrors Rust fabric::registry::normalize_cap_urn.
func normalizeCapUrn(urnStr string) (string, error) {
	parsed, err := urn.NewCapUrnFromString(urnStr)
	if err != nil {
		return "", fmt.Errorf("malformed cap URN %q: %w", urnStr, err)
	}
	return parsed.String(), nil
}

// normalizeMediaUrn parses a media URN and returns its canonical (tag-sorted)
// string form. A parse failure is a HARD error — never silently swallowed into
// the raw string. This never panics.
//
// Mirrors Rust fabric::registry::normalize_media_urn.
func normalizeMediaUrn(urnStr string) (string, error) {
	parsed, err := urn.NewMediaUrnFromString(urnStr)
	if err != nil {
		return "", fmt.Errorf("malformed media URN %q: %w", urnStr, err)
	}
	return parsed.String(), nil
}

// getCacheDir returns the on-disk cache root for a given registry origin.
//
// The root is namespaced by a stable slug of the registry base URL, using the
// SAME slug scheme as the cartridge registry layout
// (`<os_cache>/capdag/<registry_slug>/…`). Without this, a cache populated from
// one origin (e.g. prod https://fabric.capdag.com) would be reused to satisfy
// lookups against another origin (e.g. staging https://fabric-staging.capdag.com)
// — prod and staging serve different bytes for the same URN/version, so an
// origin-blind cache silently resolves against the wrong snapshot. Two origins
// therefore never share a cache slot; switching origins switches cache trees,
// and the same origin maps to a stable root so caching actually hits.
//
// Mirrors Rust's FabricRegistry::default_cache_root.
func getCacheDir(registryBaseURL string) (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	// Use standard cache location based on OS
	var cacheBase string
	if xdgCache := os.Getenv("XDG_CACHE_HOME"); xdgCache != "" {
		cacheBase = xdgCache
	} else {
		cacheBase = filepath.Join(homeDir, ".cache")
	}

	return filepath.Join(cacheBase, "capdag", registrySlug(registryBaseURL)), nil
}

// registrySlug computes the on-disk slug for a registry base URL: the first 16
// lowercase hex characters of sha256(url). This is byte-for-byte identical to
// the cartridge-registry slug scheme (bifaci.SlugFor for a non-nil URL) so caps
// and cartridges from the same origin live under the same per-origin folder
// name. It is inlined here rather than imported because bifaci depends on this
// package, and the slug is a stable on-disk format with no internal dependencies.
//
// Mirrors Rust's crate::bifaci::cartridge_slug::slug_for(Some(url)).
func registrySlug(registryBaseURL string) string {
	digest := sha256.Sum256([]byte(registryBaseURL))
	return hex.EncodeToString(digest[:])[:16]
}

func (r *FabricRegistry) cacheKey(urn string) string {
	hasher := sha256.New()
	hasher.Write([]byte(urn))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (r *FabricRegistry) cacheFilePath(urn string) string {
	key := r.cacheKey(urn)
	return filepath.Join(r.cacheDir, key+".json")
}

func loadAllCachedCaps(cacheDir string) (map[string]*Cap, error) {
	caps := make(map[string]*Cap)

	if _, err := os.Stat(cacheDir); os.IsNotExist(err) {
		return caps, nil
	}

	files, err := os.ReadDir(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read cache directory: %w", err)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		filePath := filepath.Join(cacheDir, file.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] Failed to read cache file %s: %v\n", filePath, err)
			continue
		}

		var entry CacheEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] Failed to parse cache file %s: %v\n", filePath, err)
			// Try to remove the invalid cache file
			os.Remove(filePath)
			continue
		}

		if entry.isExpired() {
			// Remove expired cache file
			if err := os.Remove(filePath); err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] Failed to remove expired cache file %s: %v\n", filePath, err)
			}
			continue
		}

		// Key the cache under the canonical (tag-sorted) URN so disk-hydrated
		// caps are reachable by the same normalized key GetCap/AddCapsToCache
		// use. A cache file whose stored URN is malformed is SKIPPED (logged) —
		// never inserted under a raw key, never a crash. Mirrors Rust's
		// loadAllCachedCaps using normalize_cap_urn.
		normalizedUrn, nerr := normalizeCapUrn(entry.Definition.UrnString())
		if nerr != nil {
			fmt.Fprintf(os.Stderr, "[WARN] loadAllCachedCaps: skipping cache file %s: %v\n", filePath, nerr)
			continue
		}
		caps[normalizedUrn] = &entry.Definition
	}

	return caps, nil
}

func (r *FabricRegistry) saveToCache(cap *Cap) error {
	urn := cap.UrnString()
	entry := CacheEntry{
		Definition: *cap,
		CachedAt:   time.Now().Unix(),
		TTLHours:   CacheDurationHours,
	}

	data, err := json.MarshalIndent(&entry, "", "  ")
	if err != nil {
		return err
	}

	cacheFile := r.cacheFilePath(urn)
	return os.WriteFile(cacheFile, data, 0644)
}

func (r *FabricRegistry) fetchFromRegistry(capUrn string) (*Cap, error) {
	// Offline policy: refuse network fetches. Cached caps are served from the
	// in-memory cache before this point; reaching here while offline means the
	// cap is uncached. Mirrors Rust's offline_flag check in fetch_cap_from_registry.
	r.mutex.RLock()
	offline := r.offline
	r.mutex.RUnlock()
	if offline {
		return nil, fmt.Errorf("Network access blocked by policy — cannot fetch cap '%s'", capUrn)
	}

	// Normalize the cap URN using the proper parser. This path returns an
	// error, so a malformed URN propagates rather than being hashed/fetched
	// under a raw key (which would 404 with a misleading "not found").
	normalizedUrn, err := normalizeCapUrn(capUrn)
	if err != nil {
		return nil, err
	}

	// The registry serves each cap at /caps/<sha256>, where
	// <sha256> is the hex digest of the canonical URN's UTF-8 bytes.
	// Hashing avoids URL-encoding hazards for the colons, quotes,
	// semicolons, and equals signs that appear in raw cap URNs.
	hash := sha256.Sum256([]byte(normalizedUrn))
	registryURL := fmt.Sprintf("%s/caps/%x", r.config.RegistryBaseURL, hash)
	resp, err := r.client.Get(registryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from registry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("cap '%s' not found in registry (HTTP %d)", capUrn, resp.StatusCode)
		}
		return nil, fmt.Errorf("registry returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Parse the registry response format
	var registryResp RegistryCapResponse
	if err := json.Unmarshal(body, &registryResp); err != nil {
		return nil, fmt.Errorf("failed to parse registry response for '%s': %w", capUrn, err)
	}

	// Convert to Cap format
	cap, err := registryResp.ToCap()
	if err != nil {
		return nil, fmt.Errorf("failed to convert registry response to cap for '%s': %w", capUrn, err)
	}

	// Cache the result
	if err := r.saveToCache(cap); err != nil {
		return nil, fmt.Errorf("failed to cache cap: %w", err)
	}

	return cap, nil
}

// Validation functions

// ValidateCapCanonical validates a cap against its canonical definition
func ValidateCapCanonical(registry *FabricRegistry, cap *Cap) error {
	return registry.ValidateCap(cap)
}

// identityCap constructs the canonical identity Cap definition.
// The identity cap accepts any media type as input and echoes it as output unchanged.
// It is mandatory in every capability set so the resolver's source-to-cap-arg
// matching can route through identity in any notation.
func identityCap() (*Cap, error) {
	identityUrn := standard.CapIdentity
	u, err := urn.NewCapUrnFromString(identityUrn)
	if err != nil {
		// CAP_IDENTITY is a build-time constant and must always parse. A bad
		// one is a serious defect, but it must NOT crash registry construction
		// — return the error so the caller can log and skip identity seeding.
		return nil, fmt.Errorf("identityCap: failed to parse identity URN %q: %w", identityUrn, err)
	}
	desc := "The categorical identity morphism. Echoes input as output unchanged. Mandatory in every capability set."
	c := &Cap{
		Urn:            u,
		Title:          "Identity",
		Command:        "identity",
		CapDescription: &desc,
		Metadata:       make(map[string]string),
		Args: []CapArg{
			NewCapArg("media:", true, []ArgSource{{Stdin: strPtr("media:")}}),
		},
	}
	c.SetOutput(NewCapOutput("media:", "The input data, unchanged"))
	return c, nil
}

// strPtr returns a pointer to the given string (helper for ArgSource.Stdin).
func strPtr(s string) *string { return &s }

// EnsureIdentityCap installs the mandatory identity cap into the in-memory cache
// if it is not already present. This is idempotent — calling it multiple times
// is safe.
func (r *FabricRegistry) EnsureIdentityCap() {
	identity, err := identityCap()
	if err != nil {
		// The identity URN is a build-time constant; a parse failure here is a
		// defect, but it must not crash registry construction. Log and skip
		// seeding the identity cap rather than panicking.
		fmt.Fprintf(os.Stderr, "[WARN] EnsureIdentityCap: %v\n", err)
		return
	}
	urnStr := identity.UrnString()
	// Normalize via parsing, same as how GetCachedCap and GetCap key the cache.
	// The identity URN just parsed successfully above, so this normalization
	// cannot fail; on the off chance it does, log and skip rather than crash.
	normalized, nerr := normalizeCapUrn(urnStr)
	if nerr != nil {
		fmt.Fprintf(os.Stderr, "[WARN] EnsureIdentityCap: %v\n", nerr)
		return
	}

	// STANDARD_CAPS travel with the manifest: their per-def version is the
	// registry's pinned manifest version (mirrors Rust).
	if r.manifestVersion >= 1 {
		identity.Version = r.manifestVersion
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, exists := r.cachedCaps[normalized]; !exists {
		r.cachedCaps[normalized] = identity
	}
	if r.manifestVersion >= 1 {
		r.manifest.Caps[normalized] = r.manifestVersion
	}
}

// NewFabricRegistryForTest creates an empty registry for testing purposes.
// The mandatory identity cap is auto-installed so the resolver's
// source-to-cap-arg matching can route through identity in any notation,
// matching the production FabricRegistry invariant.
func NewFabricRegistryForTest() *FabricRegistry {
	client := &http.Client{
		Timeout: HTTPTimeoutSeconds * time.Second,
	}
	registry := &FabricRegistry{
		client:          client,
		cacheDir:        "/tmp/capdag-test-cache",
		cachedCaps:      make(map[string]*Cap),
		cachedAliases:   make(map[string]media.StoredAlias),
		config:          RegistryConfig{},
		manifestVersion: 1,
		manifest:        media.EmptyManifest(1),
	}
	registry.EnsureIdentityCap()
	return registry
}

// NewFabricRegistryForTestWithConfig creates a registry for testing with a custom configuration.
// This is a synchronous constructor that doesn't perform any initialization.
// Intended for use in tests only - creates a registry with no network configuration.
// The mandatory identity cap is auto-installed (see NewFabricRegistryForTest).
func NewFabricRegistryForTestWithConfig(config RegistryConfig) *FabricRegistry {
	client := &http.Client{
		Timeout: HTTPTimeoutSeconds * time.Second,
	}

	registry := &FabricRegistry{
		client:          client,
		cacheDir:        "/tmp/capdag-test-cache",
		cachedCaps:      make(map[string]*Cap),
		cachedAliases:   make(map[string]media.StoredAlias),
		config:          config,
		manifestVersion: 1,
		manifest:        media.EmptyManifest(1),
	}
	registry.EnsureIdentityCap()
	return registry
}

// =============================================================================
// Alias resolution (cap registry surface)
// =============================================================================

// aliasDefver resolves a normalized alias name to its defver under the pinned
// manifest. Aliases exist only in the versioned regime; at v0 any lookup is a
// hard not-found.
func (r *FabricRegistry) aliasDefver(normalizedName string) (uint32, error) {
	if r.manifestVersion == 0 {
		return 0, fmt.Errorf(
			"alias '%s' cannot resolve: registry is pinned at v0 (aliases are a versioned-regime concept)",
			normalizedName)
	}
	defver, ok := r.manifest.Aliases[normalizedName]
	if !ok {
		return 0, fmt.Errorf("alias '%s' is not part of manifest v%d", normalizedName, r.manifestVersion)
	}
	return defver, nil
}

// AliasDefverFor looks up an alias name's pinned defver without fetching.
func (r *FabricRegistry) AliasDefverFor(name string) (uint32, error) {
	normalized, err := media.NormalizeAliasName(name)
	if err != nil {
		return 0, fmt.Errorf("invalid alias name: %w", err)
	}
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.aliasDefver(normalized)
}

// GetAlias fetches the full StoredAlias for a name from the in-memory cache.
// The Go cap registry seeds aliases via InsertCachedAliasForTest or loads them
// from disk; a name absent from the cache (after manifest membership is
// confirmed) is a hard error.
func (r *FabricRegistry) GetAlias(name string) (*media.StoredAlias, error) {
	normalized, err := media.NormalizeAliasName(name)
	if err != nil {
		return nil, fmt.Errorf("invalid alias name: %w", err)
	}
	r.mutex.RLock()
	alias, ok := r.cachedAliases[normalized]
	r.mutex.RUnlock()
	if ok {
		return &alias, nil
	}
	if _, derr := r.AliasDefverFor(normalized); derr != nil {
		return nil, derr
	}
	return nil, fmt.Errorf("alias '%s' is in manifest v%d but not present in cache", normalized, r.manifestVersion)
}

// ResolveAlias resolves an alias to the cap or media URN it points at
// (untyped): returns whatever the alias targets.
func (r *FabricRegistry) ResolveAlias(name string) (string, error) {
	alias, err := r.GetAlias(name)
	if err != nil {
		return "", err
	}
	return alias.Target, nil
}

// ResolveAliasTyped resolves an alias and asserts its target kind. If expected
// is non-empty and the resolved target is the other kind, fail hard.
func (r *FabricRegistry) ResolveAliasTyped(name string, expected media.AliasTargetKind) (string, error) {
	alias, err := r.GetAlias(name)
	if err != nil {
		return "", err
	}
	actual, ok := media.ClassifyAliasTarget(alias.Target)
	if !ok {
		return "", fmt.Errorf("alias '%s' target '%s' is neither a cap nor a media URN", alias.Name, alias.Target)
	}
	if expected != "" && actual != expected {
		return "", fmt.Errorf("alias '%s' resolves to a %s URN ('%s') but a %s was required here",
			alias.Name, actual, alias.Target, expected)
	}
	return alias.Target, nil
}

// ResolveAliasCached is a synchronous, in-memory-only alias resolution.
// Returns (target, true) if cached, ("", false) otherwise (including for a
// malformed name).
func (r *FabricRegistry) ResolveAliasCached(name string) (string, bool) {
	normalized, err := media.NormalizeAliasName(name)
	if err != nil {
		return "", false
	}
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	alias, ok := r.cachedAliases[normalized]
	if !ok {
		return "", false
	}
	return alias.Target, true
}

// selectDisplayAlias picks the display alias from a set of alias names that all
// target the same URN: the SHORTEST name, ties broken alphabetically. Returns
// ("", false) for an empty set.
//
// The ordering is total and deterministic: (len, name) lexicographic. So "png"
// beats "png-image" (shorter), and between equal-length "a16" / "a09" the
// alphabetical-smaller "a09" wins. Stable across processes for a given alias
// set, which is what makes aliased UI/notation reproducible.
//
// Mirrors Rust fabric::registry::select_display_alias.
func selectDisplayAlias(names []string) (string, bool) {
	best := ""
	found := false
	for _, name := range names {
		if !found {
			best = name
			found = true
			continue
		}
		if len(name) < len(best) || (len(name) == len(best) && name < best) {
			best = name
		}
	}
	return best, found
}

// DisplayAliasForURN is the reverse lookup: the display alias for a cap:/media:
// URN, or ("", false) if no cached alias points at it. This is the canonical
// primitive every UI surface and notation generator uses to render an aliased
// name in place of a raw URN.
//
// The query URN is canonicalised through its own parser (cap vs media by
// prefix) before matching, because alias targets are stored canonically — a
// non-canonical query (different tag order, redundant whitespace) would
// otherwise miss. A URN that is neither a cap nor a media URN, or that fails to
// parse, returns ("", false) (it cannot have an alias).
//
// When multiple aliases target the same URN, the winner is the SHORTEST name,
// ties broken alphabetically (see selectDisplayAlias). This is deterministic
// and stable across processes for a given alias set.
//
// Mirrors Rust fabric::registry::FabricRegistry::display_alias_for_urn.
func (r *FabricRegistry) DisplayAliasForURN(urnStr string) (string, bool) {
	// Canonicalise by kind. ClassifyAliasTarget keys off the prefix and is the
	// same classifier the alias publisher uses for targets, so a query and a
	// stored target canonicalise identically.
	kind, ok := media.ClassifyAliasTarget(urnStr)
	if !ok {
		return "", false
	}
	var canonical string
	var err error
	switch kind {
	case media.AliasTargetCap:
		canonical, err = normalizeCapUrn(urnStr)
	case media.AliasTargetMedia:
		canonical, err = normalizeMediaUrn(urnStr)
	default:
		return "", false
	}
	if err != nil {
		return "", false
	}

	r.mutex.RLock()
	defer r.mutex.RUnlock()
	var names []string
	for _, alias := range r.cachedAliases {
		if alias.Target == canonical {
			names = append(names, alias.Name)
		}
	}
	return selectDisplayAlias(names)
}

// CachedCapAliases returns all cached aliases whose target is a CAP URN, as
// (name, cap_urn) pairs. Used by the notation editor to offer registered cap
// aliases as wiring completions. Order is unspecified (the caller sorts/filters).
// Synchronous, cache-only — relies on the startup alias prefetch having warmed
// the cache.
//
// Mirrors Rust fabric::registry::FabricRegistry::cached_cap_aliases.
func (r *FabricRegistry) CachedCapAliases() [][2]string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	var out [][2]string
	for _, alias := range r.cachedAliases {
		if kind, ok := media.ClassifyAliasTarget(alias.Target); ok && kind == media.AliasTargetCap {
			out = append(out, [2]string{alias.Name, alias.Target})
		}
	}
	return out
}

// InsertCachedAliasForTest inserts an alias directly into the in-memory cache
// and registers its defver in the manifest (test helper). Mirrors Rust
// insert_cached_alias_for_test.
func (r *FabricRegistry) InsertCachedAliasForTest(alias media.StoredAlias) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.cachedAliases[alias.Name] = alias
	if r.manifestVersion >= 1 {
		r.manifest.Aliases[alias.Name] = alias.Version
	}
}

// AddCapsToCache inserts caps directly into the in-memory cache.
// Intended for use in tests only — production code should use the registry's
// fetch/cache pipeline. Each cap is keyed by its normalized URN string.
func (r *FabricRegistry) AddCapsToCache(caps []*Cap) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, cap := range caps {
		if cap.Version == 0 && r.manifestVersion >= 1 {
			cap.Version = r.manifestVersion
		}
		urnStr := cap.UrnString()
		// Void mutator: a malformed URN is SKIPPED (logged), never stored under
		// a raw key and never a crash, so the rest of the batch still caches.
		normalized, err := normalizeCapUrn(urnStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] AddCapsToCache: skipping cap: %v\n", err)
			continue
		}
		r.cachedCaps[normalized] = cap
		if r.manifestVersion >= 1 {
			r.manifest.Caps[normalized] = cap.Version
		}
	}
}
