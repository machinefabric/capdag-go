// Package media profile schema registry.
//
// Registry for JSON Schema profiles. Downloads and caches schemas from profile
// URLs for validating data against media def type definitions. Uses a two-level
// cache: disk-based cached schemas and in-memory compiled schemas.
//
// This is a faithful port of the Rust ProfileSchemaRegistry
// (capdag/src/media/profile.rs). Production callers rely on the on-demand HTTP
// fetch path; tests seed schemas via InsertSchema with an isolated temp cache
// directory and offline mode so they never touch the network.
package media

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xeipuuv/gojsonschema"
)

// cacheDurationHours mirrors Rust's CACHE_DURATION_HOURS (1 week).
const cacheDurationHours uint64 = 24 * 7

// cacheEntry is the on-disk representation of a cached schema.
type cacheEntry struct {
	SchemaJSON json.RawMessage `json:"schema_json"`
	ProfileURL string          `json:"profile_url"`
	CachedAt   uint64          `json:"cached_at"`
	TTLHours   uint64          `json:"ttl_hours"`
}

func (c *cacheEntry) isExpired() bool {
	now := uint64(time.Now().Unix())
	return now > c.CachedAt+(c.TTLHours*3600)
}

// compiledSchema pairs a compiled validator with its source JSON.
type compiledSchema struct {
	compiled *gojsonschema.Schema
	source   json.RawMessage
}

// ProfileSchemaError represents errors from profile schema operations.
// Mirrors Rust's ProfileSchemaError enum via a Kind discriminant.
type ProfileSchemaError struct {
	Kind    string
	Message string
}

func (e *ProfileSchemaError) Error() string { return e.Message }

func httpError(format string, a ...interface{}) *ProfileSchemaError {
	return &ProfileSchemaError{Kind: "HttpError", Message: "HTTP error: " + fmt.Sprintf(format, a...)}
}
func notFoundError(format string, a ...interface{}) *ProfileSchemaError {
	return &ProfileSchemaError{Kind: "NotFound", Message: "Schema not found: " + fmt.Sprintf(format, a...)}
}
func parseError(format string, a ...interface{}) *ProfileSchemaError {
	return &ProfileSchemaError{Kind: "ParseError", Message: "Failed to parse schema: " + fmt.Sprintf(format, a...)}
}
func invalidSchemaError(format string, a ...interface{}) *ProfileSchemaError {
	return &ProfileSchemaError{Kind: "InvalidSchema", Message: "Invalid JSON Schema: " + fmt.Sprintf(format, a...)}
}
func cacheError(format string, a ...interface{}) *ProfileSchemaError {
	return &ProfileSchemaError{Kind: "CacheError", Message: "Cache error: " + fmt.Sprintf(format, a...)}
}
func networkBlockedError(format string, a ...interface{}) *ProfileSchemaError {
	return &ProfileSchemaError{Kind: "NetworkBlocked", Message: "Network access blocked: " + fmt.Sprintf(format, a...)}
}

// ProfileSchemaRegistry validates data against JSON Schema profiles, with a
// disk-backed cache and an in-memory compiled-schema cache.
type ProfileSchemaRegistry struct {
	client      *http.Client
	cacheDir    string
	mu          sync.RWMutex
	compiled    map[string]*compiledSchema
	offlineFlag atomic.Bool
}

// NewProfileSchemaRegistry creates a registry using the standard cache directory.
// Mirrors Rust's ProfileSchemaRegistry::new().
func NewProfileSchemaRegistry() (*ProfileSchemaRegistry, error) {
	cacheDir, err := getProfileCacheDir()
	if err != nil {
		return nil, err
	}
	return NewProfileSchemaRegistryWithCacheDir(cacheDir)
}

// NewProfileSchemaRegistryWithCacheDir creates a registry rooted at cacheDir.
// Existing, unexpired cached schemas are loaded and compiled into memory.
// Mirrors Rust's ProfileSchemaRegistry::new_with_cache_dir().
func NewProfileSchemaRegistryWithCacheDir(cacheDir string) (*ProfileSchemaRegistry, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, cacheError("Failed to create cache directory: %v", err)
	}

	compiled, err := loadAllCachedSchemas(cacheDir)
	if err != nil {
		return nil, err
	}

	r := &ProfileSchemaRegistry{
		client:   &http.Client{Timeout: 30 * time.Second},
		cacheDir: cacheDir,
		compiled: compiled,
	}
	return r, nil
}

// SetOffline sets the offline flag. When true, all schema fetches are blocked.
func (r *ProfileSchemaRegistry) SetOffline(offline bool) {
	r.offlineFlag.Store(offline)
}

func getProfileCacheDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", cacheError("Could not determine cache directory: %v", err)
	}
	var cacheBase string
	if xdgCache := os.Getenv("XDG_CACHE_HOME"); xdgCache != "" {
		cacheBase = xdgCache
	} else {
		cacheBase = filepath.Join(homeDir, ".cache")
	}
	return filepath.Join(cacheBase, "capdag", "profile_schemas"), nil
}

func cacheKey(profileURL string) string {
	hasher := sha256.New()
	hasher.Write([]byte(profileURL))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func (r *ProfileSchemaRegistry) cacheFilePath(profileURL string) string {
	key := cacheKey(profileURL)
	return filepath.Join(r.cacheDir, key[:16]+".json")
}

func loadAllCachedSchemas(cacheDir string) (map[string]*compiledSchema, error) {
	schemas := make(map[string]*compiledSchema)

	if _, err := os.Stat(cacheDir); os.IsNotExist(err) {
		return schemas, nil
	}

	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return nil, cacheError("Failed to read cache directory: %v", err)
	}

	for _, dirEntry := range entries {
		path := filepath.Join(cacheDir, dirEntry.Name())
		if !strings.HasSuffix(dirEntry.Name(), ".json") {
			continue
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return nil, cacheError("Failed to read cache file %q: %v", path, err)
		}

		var entry cacheEntry
		if err := json.Unmarshal(content, &entry); err != nil {
			continue // Skip invalid cache files
		}

		if entry.isExpired() {
			_ = os.Remove(path)
			continue
		}

		compiled, err := compileSchema(entry.SchemaJSON)
		if err != nil {
			continue
		}
		schemas[entry.ProfileURL] = &compiledSchema{compiled: compiled, source: entry.SchemaJSON}
	}

	return schemas, nil
}

func compileSchema(schemaJSON []byte) (*gojsonschema.Schema, error) {
	loader := gojsonschema.NewBytesLoader(schemaJSON)
	return gojsonschema.NewSchema(loader)
}

func (r *ProfileSchemaRegistry) saveToCache(profileURL string, schemaJSON []byte) error {
	entry := cacheEntry{
		SchemaJSON: append(json.RawMessage(nil), schemaJSON...),
		ProfileURL: profileURL,
		CachedAt:   uint64(time.Now().Unix()),
		TTLHours:   cacheDurationHours,
	}
	content, err := json.MarshalIndent(&entry, "", "  ")
	if err != nil {
		return cacheError("Failed to serialize cache entry: %v", err)
	}
	if err := os.WriteFile(r.cacheFilePath(profileURL), content, 0o644); err != nil {
		return cacheError("Failed to write cache file: %v", err)
	}
	return nil
}

// getSchema returns a compiled schema for a profile URL, fetching and caching
// it on demand. Returns nil if the profile can't be fetched or isn't a valid
// schema (validation is then skipped by callers).
func (r *ProfileSchemaRegistry) getSchema(profileURL string) *compiledSchema {
	r.mu.RLock()
	if schema, ok := r.compiled[profileURL]; ok {
		r.mu.RUnlock()
		return schema
	}
	r.mu.RUnlock()

	schemaJSON, compiled, err := r.fetchSchema(profileURL)
	if err != nil {
		return nil // Fetch failed - skip validation for this profile
	}

	cs := &compiledSchema{compiled: compiled, source: schemaJSON}
	_ = r.saveToCache(profileURL, schemaJSON)

	r.mu.Lock()
	r.compiled[profileURL] = cs
	r.mu.Unlock()

	return cs
}

func (r *ProfileSchemaRegistry) fetchSchema(profileURL string) ([]byte, *gojsonschema.Schema, error) {
	if r.offlineFlag.Load() {
		return nil, nil, networkBlockedError(
			"Network access blocked by policy — cannot fetch schema '%s'", profileURL)
	}

	resp, err := r.client.Get(profileURL)
	if err != nil {
		return nil, nil, httpError("Failed to fetch schema from %s: %v", profileURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, notFoundError("Schema not found at %s (HTTP %d)", profileURL, resp.StatusCode)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, httpError("Failed to read response from %s: %v", profileURL, err)
	}

	if !json.Valid(content) {
		return nil, nil, parseError("Invalid JSON from %s", profileURL)
	}

	compiled, err := compileSchema(content)
	if err != nil {
		return nil, nil, invalidSchemaError("Invalid JSON Schema from %s: %v", profileURL, err)
	}

	return content, compiled, nil
}

// validateValue marshals a value and validates it against a compiled schema,
// returning the list of error strings (empty slice when valid).
func validateValue(schema *gojsonschema.Schema, value interface{}) ([]string, error) {
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value: %w", err)
	}
	result, err := schema.Validate(gojsonschema.NewBytesLoader(valueJSON))
	if err != nil {
		return nil, err
	}
	if result.Valid() {
		return nil, nil
	}
	errs := make([]string, 0, len(result.Errors()))
	for _, e := range result.Errors() {
		errs = append(errs, e.String())
	}
	return errs, nil
}

// Validate validates a value against a profile's schema, fetching the schema
// on demand if it is not already cached. Returns nil if valid or if the schema
// is not available (validation is skipped). Returns a non-nil list of error
// strings if the value is invalid. Mirrors Rust's validate() returning
// Result<(), Vec<String>>.
func (r *ProfileSchemaRegistry) Validate(profileURL string, value interface{}) []string {
	schema := r.getSchema(profileURL)
	if schema == nil {
		// Schema not available - skip validation.
		return nil
	}
	errs, err := validateValue(schema.compiled, value)
	if err != nil {
		return []string{err.Error()}
	}
	return errs
}

// ValidateCached validates synchronously using only cached schemas. Returns nil
// if valid or if the schema is not cached (skip validation). Returns a non-nil
// list of error strings if the value is invalid.
func (r *ProfileSchemaRegistry) ValidateCached(profileURL string, value interface{}) []string {
	r.mu.RLock()
	schema, ok := r.compiled[profileURL]
	r.mu.RUnlock()
	if !ok {
		return nil
	}
	errs, err := validateValue(schema.compiled, value)
	if err != nil {
		return []string{err.Error()}
	}
	return errs
}

// SchemaExists checks if a profile URL exists in the in-memory cache.
func (r *ProfileSchemaRegistry) SchemaExists(profileURL string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.compiled[profileURL]
	return ok
}

// GetCachedProfiles returns all cached profile URLs.
func (r *ProfileSchemaRegistry) GetCachedProfiles() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	urls := make([]string, 0, len(r.compiled))
	for url := range r.compiled {
		urls = append(urls, url)
	}
	return urls
}

// InsertSchema compiles a JSON Schema body and stores it under profileURL in
// both the in-memory and disk caches without fetching over HTTP. Returns an
// error (and leaves the caches unchanged) when the body fails to compile.
// Mirrors Rust's insert_schema() — intended for tests and local seeding only.
func (r *ProfileSchemaRegistry) InsertSchema(profileURL string, schemaJSON []byte) error {
	compiled, err := compileSchema(schemaJSON)
	if err != nil {
		return invalidSchemaError("Failed to compile schema for %s: %v", profileURL, err)
	}

	// Persist to disk cache so subsequent process starts pick it up.
	if err := r.saveToCache(profileURL, schemaJSON); err != nil {
		// Rewrap to mirror Rust's distinct message wording.
		return cacheError("Failed to write schema cache for %s: %v", profileURL, err)
	}

	src := append(json.RawMessage(nil), schemaJSON...)
	r.mu.Lock()
	r.compiled[profileURL] = &compiledSchema{compiled: compiled, source: src}
	r.mu.Unlock()
	return nil
}

// ClearCache clears all caches (memory and disk).
func (r *ProfileSchemaRegistry) ClearCache() error {
	r.mu.Lock()
	r.compiled = make(map[string]*compiledSchema)
	r.mu.Unlock()

	if _, err := os.Stat(r.cacheDir); err == nil {
		if err := os.RemoveAll(r.cacheDir); err != nil {
			return cacheError("Failed to clear cache: %v", err)
		}
		if err := os.MkdirAll(r.cacheDir, 0o755); err != nil {
			return cacheError("Failed to recreate cache: %v", err)
		}
	}
	return nil
}
