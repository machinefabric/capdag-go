package cap

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper for registry tests
func regTestUrn(tags string) string {
	if tags == "" {
		return `cap:in="media:void";out="media:object"`
	}
	return `cap:in="media:void";out="media:object";` + tags
}

// TEST614: Verify registry creation succeeds and cache directory exists
func Test614_registry_creation(t *testing.T) {
	registry, err := NewFabricRegistry()
	require.NoError(t, err)
	assert.NotNil(t, registry)
}

// TEST136 (deleted): exercised the private `cacheKey` method on
// the unified FabricRegistry. The on-disk cache filename scheme is
// an implementation detail of the persistence layer; equivalent
// observable behavior — that two equivalent URNs land in the same
// cache slot — is covered by Test140 (`same_cap_different_spellings_same_url`).
// Rust and Python dropped this; this deletion keeps the Go mirror
// in parity.

func Test6186_RegistryGetCap(t *testing.T) {
	registry, err := NewFabricRegistry()
	require.NoError(t, err)

	// Test with a fake URN that won't exist (still needs in/out)
	testUrn := regTestUrn("test;target=fake")

	_, err = registry.GetCap(testUrn)
	// Should get an error since the cap doesn't exist
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found in registry")
}

// TEST6325: Registry validation
func Test6325_RegistryValidation(t *testing.T) {
	registry, err := NewFabricRegistry()
	require.NoError(t, err)

	// Create a test cap
	capUrn, err := urn.NewCapUrnFromString(regTestUrn("test;target=fake"))
	require.NoError(t, err)
	cap := NewCap(capUrn, "Test Command", "test-cmd")

	// Validation should fail since this cap doesn't exist in registry
	err = ValidateCapCanonical(registry, cap)
	assert.Error(t, err)
}

// TEST6329: Cache operations
func Test6329_CacheOperations(t *testing.T) {
	registry, err := NewFabricRegistry()
	require.NoError(t, err)

	// Test clearing empty cache (should not error)
	err = registry.ClearCache()
	assert.NoError(t, err)
}

// TEST6382: Test parsing registry JSON without stdin args verifies cap structure
func Test6382_parse_registry_json(t *testing.T) {
	// JSON without stdin args - means cap doesn't accept stdin
	jsonData := `{"urn":"cap:in=\"media:listing-id\";use-grinder;out=\"media:task;id\"","command":"grinder_task","title":"Create Grinder Tool Task","cap_description":"Create a task for initial document analysis - first glance phase","metadata":{},"media_defs":[{"urn":"media:listing-id","media_type":"text/plain","title":"Listing ID","profile_uri":"https://machinefabric.com/schema/listing-id","schema":{"type":"string","pattern":"[0-9a-f-]{36}","description":"MachineFabric listing UUID"}},{"urn":"media:task;id","media_type":"application/json","title":"Task ID","profile_uri":"https://capdag.com/schema/grinder_task-output","schema":{"type":"object","additionalProperties":false,"properties":{"task_id":{"type":"string","description":"ID of the created task"},"task_type":{"type":"string","description":"Type of task created"}},"required":["task_id","task_type"]}}],"args":[{"media_urn":"media:listing-id","required":true,"sources":[{"cli_flag":"--listing-id"}],"arg_description":"ID of the listing to analyze"}],"output":{"media_urn":"media:task;id","output_description":"Created task information"},"registered_by":{"username":"joeharshamshiri","registered_at":"2026-01-15T00:44:29.851Z"}}`

	var registryResp RegistryCapResponse
	err := json.Unmarshal([]byte(jsonData), &registryResp)
	require.NoError(t, err, "Failed to parse JSON")

	cap, err := registryResp.ToCap()
	require.NoError(t, err)
	assert.Equal(t, "Create Grinder Tool Task", cap.Title)
	assert.Equal(t, "grinder_task", cap.Command)
	assert.Nil(t, cap.GetStdinMediaUrn(), "No stdin source in args means no stdin support")
}

// TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction
func Test138_parse_registry_json_with_stdin(t *testing.T) {
	// JSON with stdin args - means cap accepts stdin of specified media type
	jsonData := `{"urn":"cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"","command":"disbind","title":"Disbind PDF","args":[{"media_urn":"media:ext=pdf","required":true,"sources":[{"stdin":"media:ext=pdf"}]}]}`

	var registryResp RegistryCapResponse
	err := json.Unmarshal([]byte(jsonData), &registryResp)
	require.NoError(t, err, "Failed to parse JSON")

	cap, err := registryResp.ToCap()
	require.NoError(t, err)
	assert.Equal(t, "Disbind PDF", cap.Title)
	assert.True(t, cap.AcceptsStdin())
	stdinUrn := cap.GetStdinMediaUrn()
	require.NotNil(t, stdinUrn)
	assert.Equal(t, "media:ext=pdf", *stdinUrn)
}

// TEST0123: Cap exists
func Test0123_CapExists(t *testing.T) {
	registry, err := NewFabricRegistry()
	require.NoError(t, err)

	// Test with a URN that doesn't exist
	exists := registry.CapExists(regTestUrn("nonexistent;target=fake"))
	assert.False(t, exists)
}

// Per-cap URL construction. The new scheme uses /caps/<sha256>,
// where the hash is computed over the canonical URN's UTF-8 bytes.
// buildRegistryURL replicates the construction logic from fetchFromRegistry.
func buildRegistryURL(capUrn string) string {
	normalizedUrn := capUrn
	if parsed, err := urn.NewCapUrnFromString(capUrn); err == nil {
		normalizedUrn = parsed.String()
	}
	digest := sha256.Sum256([]byte(normalizedUrn))
	return fmt.Sprintf("%s/caps/%x", DefaultRegistryBaseURL, digest)
}

// TEST6388: Per-cap URL is /caps/<sha256-hex> — no URN-grammar characters in the path, no percent-encoding gymnastics.
func Test6388_per_cap_url_uses_sha256(t *testing.T) {
	registryURL := buildRegistryURL(`cap:in="media:string";test;out="media:object"`)

	assert.Contains(t, registryURL, "/caps/", "URL must use the /caps/ path prefix")
	assert.NotContains(t, registryURL, "cap:", "URL must not contain raw cap: URN syntax")
	assert.NotContains(t, registryURL, "%3A", "URL must not contain percent-encoded URN characters")
	assert.NotContains(t, registryURL, "%3D", "URL must not contain percent-encoded URN characters")
	assert.NotContains(t, registryURL, "%3B", "URL must not contain percent-encoded URN characters")
}

// TEST6391: Equivalent URNs (different tag order, etc.) hash to the
// same key. This is the property that makes cross-language lookups
// land at the same registry object regardless of which capdag
// implementation issued the request.
func Test6391_same_cap_different_spellings_same_url(t *testing.T) {
	urlA := buildRegistryURL(`cap:in="media:listing-id";use-grinder;out="media:task;id"`)
	urlB := buildRegistryURL(`cap:out="media:task;id";in="media:listing-id";use-grinder`)
	assert.Equal(t, urlA, urlB, "Equivalent URNs must hash to the same registry key")
}

// TEST141: URL has the right shape — protocol, host, /caps/ prefix, 64 hex chars, no extension.
func Test141_per_cap_url_shape(t *testing.T) {
	registryURL := buildRegistryURL(`cap:in=media:listing-id;use-grinder;out=media:task;id`)

	parsed, err := url.Parse(registryURL)
	require.NoError(t, err, "Generated URL must be valid")
	assert.Equal(t, "fabric.capdag.com", parsed.Host, "Default host is fabric.capdag.com")
	assert.True(t, strings.HasPrefix(parsed.Path, "/caps/"))
	hashPart := strings.TrimPrefix(parsed.Path, "/caps/")
	assert.Len(t, hashPart, 64, "SHA-256 hex digest is 64 characters")
}

// TEST142: Different tag orders normalise to the same URL — the canonicaliser strips the variation before hashing.
func Test142_normalize_handles_different_tag_orders(t *testing.T) {
	urn1 := `cap:test;in="media:string";out="media:object"`
	urn2 := `cap:in="media:string";out="media:object";test`

	url1 := buildRegistryURL(urn1)
	url2 := buildRegistryURL(urn2)

	assert.Equal(t, url1, url2, "Different tag orders should produce the same URL")
}

// TEST143: Default config points at https://fabric.capdag.com/ unless overridden by CDG_FABRIC_REGISTRY_URL.
func Test143_default_config(t *testing.T) {
	config := DefaultRegistryConfig()
	registryURL := os.Getenv("CDG_FABRIC_REGISTRY_URL")
	if registryURL == "" {
		assert.Equal(t, "https://fabric.capdag.com", config.RegistryBaseURL,
			"Default registry URL is fabric.capdag.com")
	} else {
		assert.Equal(t, registryURL, config.RegistryBaseURL, "Registry URL should be from env var")
	}
	assert.Contains(t, config.SchemaBaseURL, "/schema", "Schema URL should contain /schema")
}

// TEST144: Test custom registry URL updates both registry and schema base URLs
func Test144_custom_registry_url(t *testing.T) {
	config := DefaultRegistryConfig()
	WithRegistryURL("https://localhost:8888")(&config)
	assert.Equal(t, "https://localhost:8888", config.RegistryBaseURL)
	assert.Equal(t, "https://localhost:8888/schema", config.SchemaBaseURL)
}

// TEST145: Test custom registry and schema URLs set independently
func Test145_custom_registry_and_schema_url(t *testing.T) {
	config := DefaultRegistryConfig()
	WithRegistryURL("https://localhost:8888")(&config)
	WithSchemaURL("https://schemas.example.com")(&config)
	assert.Equal(t, "https://localhost:8888", config.RegistryBaseURL)
	assert.Equal(t, "https://schemas.example.com", config.SchemaBaseURL)
}

// TEST146: Test schema URL not overwritten when set explicitly before registry URL
func Test146_schema_url_not_overwritten_when_explicit(t *testing.T) {
	// If schema URL is set explicitly first, changing registry URL shouldn't change it
	config := DefaultRegistryConfig()
	WithSchemaURL("https://schemas.example.com")(&config)
	WithRegistryURL("https://localhost:8888")(&config)
	assert.Equal(t, "https://localhost:8888", config.RegistryBaseURL)
	assert.Equal(t, "https://schemas.example.com", config.SchemaBaseURL)
}

// TEST147: Test registry for test with custom config creates registry with specified URLs
func Test147_registry_for_test_with_config(t *testing.T) {
	config := DefaultRegistryConfig()
	WithRegistryURL("https://test-registry.local")(&config)
	registry := NewFabricRegistryForTestWithConfig(config)
	assert.Equal(t, "https://test-registry.local", registry.Config().RegistryBaseURL)
}

// TEST908: cached caps remain accessible while offline.
func Test908_cached_caps_accessible_when_offline(t *testing.T) {
	registry := NewFabricRegistryForTest()
	capUrn, err := urn.NewCapUrnFromString("cap:in=media:void;test-offline;out=media:void")
	require.NoError(t, err)
	c := NewCap(capUrn, "Test Cap", "test")
	c.SetOutput(NewCapOutput("media:void", "void"))
	registry.AddCapsToCache([]*Cap{c})
	registry.SetOffline(true)
	got, err := registry.GetCap("cap:in=media:void;test-offline;out=media:void")
	require.NoError(t, err, "cached cap accessible offline")
	assert.Equal(t, "Test Cap", got.Title)
}
