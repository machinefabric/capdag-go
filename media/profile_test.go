package media

import (
	"fmt"
	"os"
	"testing"

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

// TEST6606: A freshly constructed registry over a temp cache dir is operational:
// the cache directory exists on disk and the registry is usable. Inserting then
// reopening a registry on the same directory must load the persisted schema —
// this genuinely exercises the disk-cache round-trip (Rust new_with_cache_dir +
// load_all_cached_schemas), not just the in-memory map.
func Test6606_registry_creation(t *testing.T) {
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
