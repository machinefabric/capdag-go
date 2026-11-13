// Package media profile schema registry.
//
// Registry for JSON Schema profiles. Downloads and caches schemas from profile
// URLs for validating data against media def type definitions. Uses a two-level
// cache: disk-based cached schemas and in-memory compiled schemas.
//
// The disk cache is shared: every capdag process on a machine reads and writes
// it, and they run at once. It follows the fabric definition cache's rules:
//
//	<os cache>/capdag/profile_schemas/              the profile-schema cache
//	  capdag-go-<version>/<sha256 of URL>.json      one schema, as this library at <version> keeps it
//
//   - One folder per library and version. The schemas are capdag's own,
//     versioned with it, and each capdag library compiles them with its own
//     JSON Schema validator. So this library at one version reads and writes
//     only its own folder, and never an entry another library, or another
//     version of it, wrote under other rules.
//   - An entry is replaced whole (package cachefile): written to a temporary
//     file in its folder and renamed over the entry, so a reader sees the old
//     entry or the new one, never part of one.
//   - A corrupt entry is an error. An entry that cannot be read, does not
//     parse, holds another profile URL than its name is the hash of, or holds
//     a schema that does not compile stops the registry from opening with an
//     error that names the file. An entry past its time to live is removed
//     and fetched again when it is next needed.
//
// Before this layout, every library and version kept its entries directly in
// profile_schemas/, named by the first 16 hex digits of the URL's hash. This
// library never reads those files; ClearCache removes them.
//
// This is a faithful port of the Rust ProfileSchemaRegistry
// (capdag/src/media/profile.rs). Production callers rely on the on-demand HTTP
// fetch path; tests seed schemas via InsertSchema with an isolated temp cache
// directory and offline mode so they never touch the network.
package media

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/machinefabric/capdag-go/internal/cachefile"
	"github.com/machinefabric/capdag-go/internal/libversion"
	"github.com/xeipuuv/gojsonschema"
)

// cacheDurationHours mirrors Rust's CACHE_DURATION_HOURS (1 week).
const cacheDurationHours uint64 = 24 * 7

// ProfileCacheLibraryFolder is the folder this library keeps its entries in,
// in the profile-schema cache folder: the library and its version.
// Matches Rust: PROFILE_CACHE_LIBRARY_FOLDER
const ProfileCacheLibraryFolder = "capdag-go-" + libversion.Version

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
	client *http.Client

	// cacheRoot is the profile-schema cache folder,
	// `<os cache>/capdag/profile_schemas`.
	cacheRoot string

	// cacheDir is this library's folder in it, ProfileCacheLibraryFolder.
	cacheDir string

	mu          sync.RWMutex
	compiled    map[string]*compiledSchema
	offlineFlag atomic.Bool
}

// NewProfileSchemaRegistry creates a registry over the profile-schema cache
// folder in the operating system's cache folder, loading the entries this
// library keeps there. Mirrors Rust's ProfileSchemaRegistry::new().
func NewProfileSchemaRegistry() (*ProfileSchemaRegistry, error) {
	cacheRoot, err := getProfileCacheRoot()
	if err != nil {
		return nil, err
	}
	return NewProfileSchemaRegistryWithCacheDir(cacheRoot)
}

// NewProfileSchemaRegistryWithCacheDir creates a registry over the
// profile-schema cache folder cacheRoot: its entries are kept in
// cacheRoot/ProfileCacheLibraryFolder, and loaded from there now. A corrupt
// entry is an error naming its file.
// Mirrors Rust's ProfileSchemaRegistry::new_with_cache_dir().
func NewProfileSchemaRegistryWithCacheDir(cacheRoot string) (*ProfileSchemaRegistry, error) {
	cacheDir := filepath.Join(cacheRoot, ProfileCacheLibraryFolder)
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, cacheError("failed to create the profile-schema cache folder %s: %v", cacheDir, err)
	}

	compiled, err := loadAllCachedSchemas(cacheDir)
	if err != nil {
		return nil, err
	}

	r := &ProfileSchemaRegistry{
		client:    &http.Client{Timeout: 30 * time.Second},
		cacheRoot: cacheRoot,
		cacheDir:  cacheDir,
		compiled:  compiled,
	}
	return r, nil
}

// SetOffline sets the offline flag. When true, all schema fetches are blocked.
func (r *ProfileSchemaRegistry) SetOffline(offline bool) {
	r.offlineFlag.Store(offline)
}

// getProfileCacheRoot is the profile-schema cache folder,
// `<os cache>/capdag/profile_schemas`, beside the fabric caches.
func getProfileCacheRoot() (string, error) {
	root, err := cachefile.CapdagRoot()
	if err != nil {
		return "", cacheError("%v", err)
	}
	return filepath.Join(root, "profile_schemas"), nil
}

// entryFileName is the name an entry for profileURL is kept under: the
// SHA-256 of the URL, in hex, with `.json`.
func entryFileName(profileURL string) string {
	return fmt.Sprintf("%x.json", sha256.Sum256([]byte(profileURL)))
}

func (r *ProfileSchemaRegistry) cacheFilePath(profileURL string) string {
	return filepath.Join(r.cacheDir, entryFileName(profileURL))
}

// corruptEntry is the error for an entry that is there and is not what its
// name says.
func corruptEntry(path, detail string) *ProfileSchemaError {
	return cacheError("the cached profile schema %s is corrupt: %s; remove the file", path, detail)
}

// loadAllCachedSchemas is every entry in this library's folder, compiled, by
// profile URL. A temporary file is not an entry and is passed over; an expired
// entry is removed; anything else that is not a whole, valid entry is an error
// naming its file.
func loadAllCachedSchemas(cacheDir string) (map[string]*compiledSchema, error) {
	schemas := make(map[string]*compiledSchema)

	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return nil, cacheError("failed to read the profile-schema cache folder %s: %v", cacheDir, err)
	}

	for _, dirEntry := range entries {
		name := dirEntry.Name()
		path := filepath.Join(cacheDir, name)
		if cachefile.IsTemporary(name) {
			continue
		}
		if !strings.HasSuffix(name, ".json") || dirEntry.IsDir() {
			return nil, corruptEntry(path,
				"it is not an entry: the folder holds only `<sha256 of URL>.json` files")
		}

		content, err := os.ReadFile(path)
		if err != nil {
			// A peer removed it (expired) between the listing and the read.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, cacheError("the cached profile schema %s cannot be read: %v", path, err)
		}

		var entry cacheEntry
		if err := json.Unmarshal(content, &entry); err != nil {
			return nil, corruptEntry(path, err.Error())
		}
		if entryFileName(entry.ProfileURL) != name {
			return nil, corruptEntry(path, fmt.Sprintf("it holds the schema of %s, whose entry is %s",
				entry.ProfileURL, entryFileName(entry.ProfileURL)))
		}

		if entry.isExpired() {
			// A peer removed or replaced it first: not an error.
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, cacheError("failed to remove the expired cached profile schema %s: %v", path, err)
			}
			continue
		}

		compiled, err := compileSchema(entry.SchemaJSON)
		if err != nil {
			return nil, corruptEntry(path, fmt.Sprintf("its schema does not compile: %v", err))
		}
		schemas[entry.ProfileURL] = &compiledSchema{compiled: compiled, source: entry.SchemaJSON}
	}

	return schemas, nil
}

func compileSchema(schemaJSON []byte) (*gojsonschema.Schema, error) {
	loader := gojsonschema.NewBytesLoader(schemaJSON)
	return gojsonschema.NewSchema(loader)
}

// saveToCache puts the entry for profileURL in this library's folder, whole.
func (r *ProfileSchemaRegistry) saveToCache(profileURL string, schemaJSON []byte) error {
	entry := cacheEntry{
		SchemaJSON: append(json.RawMessage(nil), schemaJSON...),
		ProfileURL: profileURL,
		CachedAt:   uint64(time.Now().Unix()),
		TTLHours:   cacheDurationHours,
	}
	content, err := json.MarshalIndent(&entry, "", "  ")
	if err != nil {
		return cacheError("failed to serialize the cached profile schema of %s: %v", profileURL, err)
	}
	if err := cachefile.WriteAtomically(r.cacheFilePath(profileURL), content); err != nil {
		return cacheError("%v", err)
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
	// The schema is at hand either way; a cache that cannot be written is
	// reported, not hidden.
	if err := r.saveToCache(profileURL, schemaJSON); err != nil {
		log.Printf("profile schema %s is not cached: %v", profileURL, err)
	}

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
		return err
	}

	src := append(json.RawMessage(nil), schemaJSON...)
	r.mu.Lock()
	r.compiled[profileURL] = &compiledSchema{compiled: compiled, source: src}
	r.mu.Unlock()
	return nil
}

// ClearCache clears all caches: the in-memory one, this library's folder on
// disk (emptied, not removed), and the files the older layout kept directly in
// the profile-schema cache folder. Other libraries' and versions' folders are
// left to them. Nothing is read first, so this clears a cache holding a
// corrupt entry; a file a peer removed first is not an error.
// Mirrors Rust's clear_cache().
func (r *ProfileSchemaRegistry) ClearCache() error {
	r.mu.Lock()
	r.compiled = make(map[string]*compiledSchema)
	r.mu.Unlock()

	if err := removeFilesIn(r.cacheDir); err != nil {
		return err
	}
	return removeFilesIn(r.cacheRoot)
}

// removeFilesIn removes every file directly in folder, leaving its folders.
func removeFilesIn(folder string) error {
	entries, err := os.ReadDir(folder)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return cacheError("failed to read the profile-schema cache folder %s: %v", folder, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(folder, entry.Name())
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return cacheError("failed to remove %s: %v", path, err)
		}
	}
	return nil
}
