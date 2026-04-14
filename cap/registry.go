package cap

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

const (
	DefaultRegistryBaseURL = "https://capdag.com"
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
//   - CAPDAG_REGISTRY_URL: Base URL for the registry (default: https://capdag.com)
//   - CAPDAG_SCHEMA_BASE_URL: Base URL for schemas (default: {registry_url}/schema)
func DefaultRegistryConfig() RegistryConfig {
	registryBase := os.Getenv("CAPDAG_REGISTRY_URL")
	if registryBase == "" {
		registryBase = DefaultRegistryBaseURL
	}

	schemaBase := os.Getenv("CAPDAG_SCHEMA_BASE_URL")
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

// RegistryCapResponse represents the response format from capdag.com registry
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

// CapRegistry handles communication with the capdag registry
type CapRegistry struct {
	client     *http.Client
	cacheDir   string
	cachedCaps map[string]*Cap
	mutex      sync.RWMutex
	config     RegistryConfig
}

// NewCapRegistry creates a new registry client
//
// Accepts optional RegistryOption functions to configure the registry.
// Without options, uses environment variables or defaults.
//
// Example:
//
//	registry, err := NewCapRegistry()  // Uses env vars or defaults
//	registry, err := NewCapRegistry(WithRegistryURL("https://my-registry.com"))
func NewCapRegistry(opts ...RegistryOption) (*CapRegistry, error) {
	config := DefaultRegistryConfig()
	for _, opt := range opts {
		opt(&config)
	}

	cacheDir, err := getCacheDir()
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

	return &CapRegistry{
		client:     client,
		cacheDir:   cacheDir,
		cachedCaps: cachedCaps,
		config:     config,
	}, nil
}

// Config returns the current registry configuration
func (r *CapRegistry) Config() RegistryConfig {
	return r.config
}

// GetCap gets a cap from in-memory cache or fetch from registry
func (r *CapRegistry) GetCap(urn string) (*Cap, error) {
	// Check in-memory cache first
	r.mutex.RLock()
	if cap, exists := r.cachedCaps[urn]; exists {
		r.mutex.RUnlock()
		return cap, nil
	}
	r.mutex.RUnlock()

	// Not in cache, fetch from registry and update in-memory cache
	cap, err := r.fetchFromRegistry(urn)
	if err != nil {
		return nil, err
	}

	// Update in-memory cache
	r.mutex.Lock()
	r.cachedCaps[urn] = cap
	r.mutex.Unlock()

	return cap, nil
}

// GetCaps gets multiple caps at once - fails if any cap is not available
func (r *CapRegistry) GetCaps(urns []string) ([]*Cap, error) {
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
func (r *CapRegistry) ValidateCap(cap *Cap) error {
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
func (r *CapRegistry) CapExists(urn string) bool {
	_, err := r.GetCap(urn)
	return err == nil
}

// GetCachedCaps gets all currently cached caps from in-memory cache
func (r *CapRegistry) GetCachedCaps() []*Cap {
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
func (r *CapRegistry) GetCachedCap(capUrn string) (*Cap, bool) {
	normalized := capUrn
	if parsed, err := urn.NewCapUrnFromString(capUrn); err == nil {
		normalized = parsed.String()
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
func (r *CapRegistry) ClearCache() error {
	// Clear in-memory cache
	r.mutex.Lock()
	r.cachedCaps = make(map[string]*Cap)
	r.mutex.Unlock()

	// Clear filesystem cache
	if err := os.RemoveAll(r.cacheDir); err != nil {
		return fmt.Errorf("failed to clear cache: %w", err)
	}
	return os.MkdirAll(r.cacheDir, 0755)
}

// Private helper methods

func getCacheDir() (string, error) {
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

	return filepath.Join(cacheBase, "capdag"), nil
}

func (r *CapRegistry) cacheKey(urn string) string {
	hasher := sha256.New()
	hasher.Write([]byte(urn))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (r *CapRegistry) cacheFilePath(urn string) string {
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

		urn := entry.Definition.UrnString()
		caps[urn] = &entry.Definition
	}

	return caps, nil
}

func (r *CapRegistry) saveToCache(cap *Cap) error {
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

func (r *CapRegistry) fetchFromRegistry(capUrn string) (*Cap, error) {
	// Normalize the cap URN using the proper parser
	normalizedUrn := capUrn
	if parsed, err := urn.NewCapUrnFromString(capUrn); err == nil {
		normalizedUrn = parsed.String()
	}

	// URL-encode only the tags part (after "cap:") while keeping "cap:" literal
	tagsPart := strings.TrimPrefix(normalizedUrn, "cap:")
	encodedTags := url.PathEscape(tagsPart)
	registryURL := fmt.Sprintf("%s/cap:%s", r.config.RegistryBaseURL, encodedTags)
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
func ValidateCapCanonical(registry *CapRegistry, cap *Cap) error {
	return registry.ValidateCap(cap)
}

// identityCap constructs the canonical identity Cap definition.
// The identity cap accepts any media type as input and echoes it as output unchanged.
// It is mandatory in every capability set so the resolver's source-to-cap-arg
// matching can route through identity in any notation.
func identityCap() *Cap {
	identityUrn := "cap:"
	u, err := urn.NewCapUrnFromString(identityUrn)
	if err != nil {
		// "cap:" is always valid — this is a programming error
		panic("identityCap: failed to parse identity URN: " + err.Error())
	}
	desc := "The categorical identity morphism. Echoes input as output unchanged. Mandatory in every capability set."
	c := &Cap{
		Urn:            u,
		Title:          "Identity",
		Command:        "identity",
		CapDescription: &desc,
		Metadata:       make(map[string]string),
		MediaSpecs:     []media.MediaSpecDef{},
		Args: []CapArg{
			NewCapArg("media:", true, []ArgSource{{Stdin: strPtr("media:")}}),
		},
	}
	c.SetOutput(NewCapOutput("media:", "The input data, unchanged"))
	return c
}

// strPtr returns a pointer to the given string (helper for ArgSource.Stdin).
func strPtr(s string) *string { return &s }

// EnsureIdentityCap installs the mandatory identity cap into the in-memory cache
// if it is not already present. This is idempotent — calling it multiple times
// is safe.
func (r *CapRegistry) EnsureIdentityCap() {
	identity := identityCap()
	urnStr := identity.UrnString()
	// Normalize via parsing, same as how GetCachedCap and GetCap key the cache
	normalized := urnStr
	if parsed, err := urn.NewCapUrnFromString(urnStr); err == nil {
		normalized = parsed.String()
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, exists := r.cachedCaps[normalized]; !exists {
		r.cachedCaps[normalized] = identity
	}
}

// NewCapRegistryForTest creates an empty registry for testing purposes.
// The mandatory identity cap is auto-installed so the resolver's
// source-to-cap-arg matching can route through identity in any notation,
// matching the production CapRegistry invariant.
func NewCapRegistryForTest() *CapRegistry {
	client := &http.Client{
		Timeout: HTTPTimeoutSeconds * time.Second,
	}
	registry := &CapRegistry{
		client:     client,
		cacheDir:   "/tmp/capdag-test-cache",
		cachedCaps: make(map[string]*Cap),
		config:     RegistryConfig{},
	}
	registry.EnsureIdentityCap()
	return registry
}

// NewCapRegistryForTestWithConfig creates a registry for testing with a custom configuration.
// This is a synchronous constructor that doesn't perform any initialization.
// Intended for use in tests only - creates a registry with no network configuration.
// The mandatory identity cap is auto-installed (see NewCapRegistryForTest).
func NewCapRegistryForTestWithConfig(config RegistryConfig) *CapRegistry {
	client := &http.Client{
		Timeout: HTTPTimeoutSeconds * time.Second,
	}

	registry := &CapRegistry{
		client:     client,
		cacheDir:   "/tmp/capdag-test-cache",
		cachedCaps: make(map[string]*Cap),
		config:     config,
	}
	registry.EnsureIdentityCap()
	return registry
}
