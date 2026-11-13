package media

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/machinefabric/capdag-go/internal/libversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schemaBody returns a JSON Schema body suitable for one of the well-known
// scalar/array profile URLs. Tests use this to seed the registry without
// HTTP because the library no longer ships embedded schema bodies.
func schemaBody(profileURL string) []byte {
	switch profileURL {
	case ProfileStr:
		return []byte(fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"%s","type":"string"}`, profileURL))
	case ProfileInt:
		return []byte(fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"%s","type":"integer"}`, profileURL))
	case ProfileNum:
		return []byte(fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"%s","type":"number"}`, profileURL))
	case ProfileBool:
		return []byte(fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"%s","type":"boolean"}`, profileURL))
	case ProfileObj:
		return []byte(fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"%s","type":"object"}`, profileURL))
	case ProfileStrArray:
		return []byte(fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"%s","type":"array","items":{"type":"string"}}`, profileURL))
	case ProfileNumArray:
		return []byte(fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"%s","type":"array","items":{"type":"number"}}`, profileURL))
	case ProfileBoolArray:
		return []byte(fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"%s","type":"array","items":{"type":"boolean"}}`, profileURL))
	case ProfileObjArray:
		return []byte(fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"%s","type":"array","items":{"type":"object"}}`, profileURL))
	}
	panic(fmt.Sprintf("schemaBody: unknown profile URL %q", profileURL))
}

func standardProfileURLs() []string {
	return []string{
		ProfileStr, ProfileInt, ProfileNum, ProfileBool, ProfileObj,
		ProfileStrArray, ProfileNumArray, ProfileBoolArray, ProfileObjArray,
	}
}

// createEmptyTestRegistry returns a registry rooted at an isolated temp cache
// directory with offline mode enabled, so it never touches the network. Mirrors
// the Rust test harness (new_with_cache_dir over a TempDir).
func createEmptyTestRegistry(t *testing.T) *ProfileSchemaRegistry {
	t.Helper()
	registry, err := NewProfileSchemaRegistryWithCacheDir(t.TempDir())
	require.NoError(t, err, "Failed to create profile registry")
	registry.SetOffline(true)
	return registry
}

// createTestRegistry returns a registry with the well-known scalar/array
// profile schemas seeded into the cache (isolated temp dir, offline).
func createTestRegistry(t *testing.T) *ProfileSchemaRegistry {
	t.Helper()
	registry := createEmptyTestRegistry(t)
	for _, url := range standardProfileURLs() {
		require.NoError(t, registry.InsertSchema(url, schemaBody(url)))
	}
	return registry
}

// TEST6605: InsertSchema seeds the cache so subsequent validation hits a real
// compiled schema rather than the skip-on-unknown path. A registry that
// silently dropped inserts would let validation calls return nil even for
// inputs that violate the schema.
func Test6605_insert_schema_populates_cache(t *testing.T) {
	registry := createEmptyTestRegistry(t)
	assert.False(t, registry.SchemaExists(ProfileStr))

	require.NoError(t, registry.InsertSchema(ProfileStr, schemaBody(ProfileStr)))

	assert.True(t, registry.SchemaExists(ProfileStr))
	assert.Nil(t, registry.ValidateCached(ProfileStr, "ok"))
	assert.NotNil(t, registry.ValidateCached(ProfileStr, 7),
		"Number must not validate against the string schema")
}

// TEST612: clear_cache empties the in-memory cache for seeded schemas.
func Test612_clear_cache(t *testing.T) {
	registry := createTestRegistry(t)
	assert.True(t, len(registry.GetCachedProfiles()) > 0)
	registry.ClearCache()
	assert.Equal(t, 0, len(registry.GetCachedProfiles()))
}

// TEST613: validate_cached validates against cached standard schemas
func Test613_validate_cached(t *testing.T) {
	registry := createTestRegistry(t)

	assert.Nil(t, registry.ValidateCached(ProfileStr, "hello"))
	assert.NotNil(t, registry.ValidateCached(ProfileStr, 42))

	assert.Nil(t, registry.ValidateCached(ProfileInt, 42))

	assert.Nil(t, registry.ValidateCached(ProfileObjArray, []map[string]interface{}{{"key": "value"}}))
	assert.NotNil(t, registry.ValidateCached(ProfileObjArray, []string{"not", "objects"}))

	// Unknown profile returns nil (skip validation)
	assert.Nil(t, registry.ValidateCached("https://example.com/unknown", "anything"))
}

// TEST618: Verify profile schema registry creation succeeds with temp cache.
// A freshly constructed registry over a temp cache dir is operational: the cache
// directory exists on disk and the registry is usable. Inserting then reopening a
// registry on the same directory must load the persisted schema — this genuinely
// exercises the disk-cache round-trip (Rust new_with_cache_dir +
// load_all_cached_schemas), not just the in-memory map.
func Test618_registry_creation(t *testing.T) {
	dir := t.TempDir()
	registry, err := NewProfileSchemaRegistryWithCacheDir(dir)
	require.NoError(t, err)
	registry.SetOffline(true)

	info, statErr := os.Stat(dir)
	require.NoError(t, statErr)
	assert.True(t, info.IsDir(), "cache directory must exist")

	// Persist a schema, then reopen on the same directory.
	require.NoError(t, registry.InsertSchema(ProfileStr, schemaBody(ProfileStr)))

	reopened, err := NewProfileSchemaRegistryWithCacheDir(dir)
	require.NoError(t, err)
	reopened.SetOffline(true)
	assert.True(t, reopened.SchemaExists(ProfileStr),
		"reopened registry must load the persisted schema from disk")
	assert.Nil(t, reopened.ValidateCached(ProfileStr, "ok"))
	assert.NotNil(t, reopened.ValidateCached(ProfileStr, 7))
}

// TEST619: A freshly constructed registry has an empty cache. The well-known profile schemas are no longer bundled in the binary; callers must either fetch them on demand or seed via insert_schema.
func Test619_fresh_registry_cache_is_empty(t *testing.T) {
	registry := createEmptyTestRegistry(t)
	assert.Equal(t, 0, len(registry.GetCachedProfiles()),
		"Fresh registry must have no cached schemas; nothing is bundled into the library")
	for _, url := range standardProfileURLs() {
		assert.False(t, registry.SchemaExists(url), "%s must not be cached on a fresh registry", url)
	}
}

// TEST620: Verify string schema validates strings and rejects non-strings
func Test620_string_validation(t *testing.T) {
	registry := createTestRegistry(t)
	assert.Nil(t, registry.Validate(ProfileStr, "hello"))
	assert.NotNil(t, registry.Validate(ProfileStr, 42))
}

// TEST621: Verify integer schema validates integers and rejects floats and strings
func Test621_integer_validation(t *testing.T) {
	registry := createTestRegistry(t)
	assert.Nil(t, registry.Validate(ProfileInt, 42))
	assert.NotNil(t, registry.Validate(ProfileInt, 3.14))
	assert.NotNil(t, registry.Validate(ProfileInt, "hello"))
}

// TEST622: Verify number schema validates integers and floats, rejects strings
func Test622_number_validation(t *testing.T) {
	registry := createTestRegistry(t)
	assert.Nil(t, registry.Validate(ProfileNum, 42))
	assert.Nil(t, registry.Validate(ProfileNum, 3.14))
	assert.NotNil(t, registry.Validate(ProfileNum, "hello"))
}

// TEST623: Verify boolean schema validates true/false and rejects string "true"
func Test623_boolean_validation(t *testing.T) {
	registry := createTestRegistry(t)
	assert.Nil(t, registry.Validate(ProfileBool, true))
	assert.Nil(t, registry.Validate(ProfileBool, false))
	assert.NotNil(t, registry.Validate(ProfileBool, "true"))
}

// TEST624: Verify object schema validates objects and rejects arrays
func Test624_object_validation(t *testing.T) {
	registry := createTestRegistry(t)
	assert.Nil(t, registry.Validate(ProfileObj, map[string]interface{}{"key": "value"}))
	assert.NotNil(t, registry.Validate(ProfileObj, []int{1, 2, 3}))
}

// TEST625: Verify string array schema validates string arrays and rejects mixed arrays
func Test625_string_array_validation(t *testing.T) {
	registry := createTestRegistry(t)
	assert.Nil(t, registry.Validate(ProfileStrArray, []string{"a", "b", "c"}))
	assert.NotNil(t, registry.Validate(ProfileStrArray, []interface{}{"a", 1, "c"}))
	assert.NotNil(t, registry.Validate(ProfileStrArray, "hello"))
}

// TEST626: Verify unknown profile URL skips validation and returns Ok
func Test626_unknown_profile_skips_validation(t *testing.T) {
	registry := createEmptyTestRegistry(t)
	assert.Nil(t, registry.Validate("https://example.com/unknown", "anything"))
}

// TEST627: insert_schema rejects malformed JSON Schemas instead of caching them. A registry that silently accepted invalid schemas would hide compilation problems until the first validation call.
func Test627_insert_schema_rejects_invalid_schema(t *testing.T) {
	registry := createEmptyTestRegistry(t)
	bad := []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":99}`)
	err := registry.InsertSchema("https://capdag.com/schema/bad", bad)
	assert.Error(t, err, "Invalid schema must not be cached")
	assert.False(t, registry.SchemaExists("https://capdag.com/schema/bad"),
		"Failed insert must not leave the URL in the cache")
}

// cachedEntryFiles is every cache entry under root, at whatever depth the
// layout keeps them: the `.json` files, leaving out temporary files (named
// from a `.`). The tests find entries this way rather than by the registry's
// own path helpers, so that they say what is on disk.
func cachedEntryFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if !entry.IsDir() && strings.HasSuffix(name, ".json") && !strings.HasPrefix(name, ".") {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)
	sort.Strings(files)
	return files
}

// largeStringSchema is a string schema whose description is size copies of
// marker: large enough that writing it in place takes long enough for a
// reader to open it half-written.
func largeStringSchema(marker byte, size int) []byte {
	return []byte(fmt.Sprintf(
		`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string","description":"%s"}`,
		strings.Repeat(string(marker), size)))
}

// TEST604: Processes loading the profile-schema cache while others replace the
// same entry never read part of an entry: every load succeeds and holds the
// schema whole, the old one or the new one.
func Test604_concurrent_readers_never_read_a_torn_entry(t *testing.T) {
	const url = "https://capdag.com/schema/test604-large"
	const size = 512 * 1024
	const writes = 60
	root := t.TempDir()
	seed, err := NewProfileSchemaRegistryWithCacheDir(root)
	require.NoError(t, err)
	require.NoError(t, seed.InsertSchema(url, largeStringSchema('a', size)))

	var writing atomic.Bool
	writing.Store(true)
	var writers, readers sync.WaitGroup
	failures := make(chan string, 64)
	for _, marker := range []byte{'b', 'c'} {
		writers.Add(1)
		go func(marker byte) {
			defer writers.Done()
			registry, err := NewProfileSchemaRegistryWithCacheDir(root)
			if err != nil {
				failures <- fmt.Sprintf("the writer's registry did not open: %v", err)
				return
			}
			for i := 0; i < writes; i++ {
				if err := registry.InsertSchema(url, largeStringSchema(marker, size)); err != nil {
					failures <- fmt.Sprintf("write %d failed: %v", i, err)
					return
				}
			}
		}(marker)
	}
	var loads atomic.Int64
	for i := 0; i < 3; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for first := true; first || writing.Load(); first = false {
				registry, err := NewProfileSchemaRegistryWithCacheDir(root)
				if err != nil {
					failures <- fmt.Sprintf("a load failed: %v", err)
					return
				}
				if !registry.SchemaExists(url) {
					failures <- "a load lost the entry: it read part of it"
					return
				}
				if registry.ValidateCached(url, "text") != nil || registry.ValidateCached(url, 7) == nil {
					failures <- "a load holds another schema than the one written"
					return
				}
				loads.Add(1)
			}
		}()
	}
	writers.Wait()
	writing.Store(false)
	readers.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	assert.True(t, loads.Load() > 0)
	entries := cachedEntryFiles(t, root)
	assert.Len(t, entries, 1, "one entry, and no temporary file left: %v", entries)
}

// TEST611: A cache entry that is there and does not parse is an error that
// names its file; the registry does not open over it as if it were absent.
func Test611_corrupt_entry_is_an_error_naming_its_file(t *testing.T) {
	root := t.TempDir()
	registry, err := NewProfileSchemaRegistryWithCacheDir(root)
	require.NoError(t, err)
	require.NoError(t, registry.InsertSchema(ProfileStr, schemaBody(ProfileStr)))
	entries := cachedEntryFiles(t, root)
	require.Len(t, entries, 1)
	require.NoError(t, os.WriteFile(entries[0], []byte(`{"schema_json": {"type": "str`), 0o644))

	_, err = NewProfileSchemaRegistryWithCacheDir(root)
	require.Error(t, err, "a corrupt entry must stop the registry from opening")
	assert.Contains(t, err.Error(), entries[0], "the error names the file")
	assert.Contains(t, err.Error(), "corrupt")
}

// TEST615: A cache entry that parses but is not the schema its file names (it
// holds another profile URL, or a body that does not compile) is an error
// naming its file.
func Test615_entry_not_matching_its_file_is_an_error(t *testing.T) {
	// Another profile URL than its file name says.
	root := t.TempDir()
	registry, err := NewProfileSchemaRegistryWithCacheDir(root)
	require.NoError(t, err)
	require.NoError(t, registry.InsertSchema(ProfileStr, schemaBody(ProfileStr)))
	require.NoError(t, registry.InsertSchema(ProfileInt, schemaBody(ProfileInt)))
	entries := cachedEntryFiles(t, root)
	require.Len(t, entries, 2)
	var intEntry, strEntry string
	for _, path := range entries {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		if strings.Contains(string(content), ProfileInt) {
			intEntry = path
		} else {
			strEntry = path
		}
	}
	intContent, err := os.ReadFile(intEntry)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(strEntry, intContent, 0o644))
	_, err = NewProfileSchemaRegistryWithCacheDir(root)
	require.Error(t, err, "an entry holding another URL must stop the registry from opening")
	assert.Contains(t, err.Error(), strEntry, "the error names the file")

	// A body that does not compile.
	root = t.TempDir()
	registry, err = NewProfileSchemaRegistryWithCacheDir(root)
	require.NoError(t, err)
	require.NoError(t, registry.InsertSchema(ProfileStr, schemaBody(ProfileStr)))
	entries = cachedEntryFiles(t, root)
	require.Len(t, entries, 1)
	content, err := os.ReadFile(entries[0])
	require.NoError(t, err)
	var entry map[string]any
	require.NoError(t, json.Unmarshal(content, &entry))
	entry["schema_json"] = map[string]any{"type": 99}
	content, err = json.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(entries[0], content, 0o644))
	_, err = NewProfileSchemaRegistryWithCacheDir(root)
	require.Error(t, err, "a schema that does not compile must stop the registry from opening")
	assert.Contains(t, err.Error(), entries[0], "the error names the file")
}

// TEST654: A library keeps its entries in its own folder,
// `<cache>/<library>-<version>/`, and reads no other: neither another
// library's or version's folder nor the older layout's files directly in the
// cache folder. Clearing the cache empties its own folder and removes the
// older layout's files, and leaves other folders alone.
func Test654_entries_live_in_the_library_version_folder(t *testing.T) {
	root := t.TempDir()
	own := filepath.Join(root, "capdag-go-"+libversion.Version)
	other := filepath.Join(root, "capdag-rs-0.0.1")
	require.NoError(t, os.MkdirAll(other, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(other, "not-read.json"), []byte("not json"), 0o644))
	legacy := filepath.Join(root, "0123456789abcdef.json")
	require.NoError(t, os.WriteFile(legacy, []byte("not json either"), 0o644))
	legacyTemporary := filepath.Join(root, ".0123456789abcdef.json.1-0.tmp")
	require.NoError(t, os.WriteFile(legacyTemporary, nil, 0o644))

	registry, err := NewProfileSchemaRegistryWithCacheDir(root)
	require.NoError(t, err, "other folders and the older layout are not read")
	require.NoError(t, registry.InsertSchema(ProfileStr, schemaBody(ProfileStr)))
	entry := filepath.Join(own, fmt.Sprintf("%x.json", sha256.Sum256([]byte(ProfileStr))))
	info, err := os.Stat(entry)
	require.NoError(t, err, "the entry is %s", entry)
	assert.False(t, info.IsDir())

	require.NoError(t, registry.ClearCache())
	_, err = os.Stat(entry)
	assert.ErrorIs(t, err, fs.ErrNotExist)
	info, err = os.Stat(own)
	require.NoError(t, err, "the library's folder stays, emptied")
	assert.True(t, info.IsDir())
	_, err = os.Stat(legacy)
	assert.ErrorIs(t, err, fs.ErrNotExist, "the older layout's files are removed")
	_, err = os.Stat(legacyTemporary)
	assert.ErrorIs(t, err, fs.ErrNotExist)
	_, err = os.Stat(filepath.Join(other, "not-read.json"))
	assert.NoError(t, err, "another library's folder is left")
}
