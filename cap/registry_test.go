package cap

import (
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

// TEST135: Test registry creation with temporary cache directory succeeds
func Test135_registry_creation(t *testing.T) {
	registry, err := NewCapRegistry()
	require.NoError(t, err)
	assert.NotNil(t, registry)
}

// TEST136: Test cache key generation produces consistent hashes for same URN
func Test136_cache_key_generation(t *testing.T) {
	registry, err := NewCapRegistry()
	require.NoError(t, err)

	// Use URNs with required in/out
	urn1 := `cap:in="media:void";op=extract;out="media:json;record;textable;target=metadata"`
	urn2 := `cap:in="media:void";op=extract;out="media:json;record;textable;target=metadata"`
	urn3 := `cap:in="media:void";op=different;out="media:json;record;textable"`

	key1 := registry.cacheKey(urn1)
	key2 := registry.cacheKey(urn2)
	key3 := registry.cacheKey(urn3)

	assert.Equal(t, key1, key2, "Same URN should produce same cache key")
	assert.NotEqual(t, key1, key3, "Different URNs should produce different cache keys")
}

func TestRegistryGetCap(t *testing.T) {
	registry, err := NewCapRegistry()
	require.NoError(t, err)

	// Test with a fake URN that won't exist (still needs in/out)
	testUrn := regTestUrn("op=test;target=fake")

	_, err = registry.GetCap(testUrn)
	// Should get an error since the cap doesn't exist
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found in registry")
}

func TestRegistryValidation(t *testing.T) {
	registry, err := NewCapRegistry()
	require.NoError(t, err)

	// Create a test cap
	capUrn, err := urn.NewCapUrnFromString(regTestUrn("op=test;target=fake"))
	require.NoError(t, err)
	cap := NewCap(capUrn, "Test Command", "test-cmd")

	// Validation should fail since this cap doesn't exist in registry
	err = ValidateCapCanonical(registry, cap)
	assert.Error(t, err)
}

func TestCacheOperations(t *testing.T) {
	registry, err := NewCapRegistry()
	require.NoError(t, err)

	// Test clearing empty cache (should not error)
	err = registry.ClearCache()
	assert.NoError(t, err)
}

// TEST137: Test parsing registry JSON without stdin args verifies cap structure
func Test137_parse_registry_json(t *testing.T) {
	// JSON without stdin args - means cap doesn't accept stdin
	jsonData := `{"urn":"cap:in=\"media:listing-id\";op=use_grinder;out=\"media:task;id\"","command":"grinder_task","title":"Create Grinder Tool Task","cap_description":"Create a task for initial document analysis - first glance phase","metadata":{},"media_specs":[{"urn":"media:listing-id","media_type":"text/plain","title":"Listing ID","profile_uri":"https://machinefabric.com/schema/listing-id","schema":{"type":"string","pattern":"[0-9a-f-]{36}","description":"MachineFabric listing UUID"}},{"urn":"media:task;id","media_type":"application/json","title":"Task ID","profile_uri":"https://capdag.com/schema/grinder_task-output","schema":{"type":"object","additionalProperties":false,"properties":{"task_id":{"type":"string","description":"ID of the created task"},"task_type":{"type":"string","description":"Type of task created"}},"required":["task_id","task_type"]}}],"args":[{"media_urn":"media:listing-id","required":true,"sources":[{"cli_flag":"--listing-id"}],"arg_description":"ID of the listing to analyze"}],"output":{"media_urn":"media:task;id","output_description":"Created task information"},"registered_by":{"username":"joeharshamshiri","registered_at":"2026-01-15T00:44:29.851Z"}}`

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
	jsonData := `{"urn":"cap:in=\"media:pdf\";op=disbind;out=\"media:textable;page\"","command":"disbind","title":"Disbind PDF","args":[{"media_urn":"media:pdf","required":true,"sources":[{"stdin":"media:pdf"}]}]}`

	var registryResp RegistryCapResponse
	err := json.Unmarshal([]byte(jsonData), &registryResp)
	require.NoError(t, err, "Failed to parse JSON")

	cap, err := registryResp.ToCap()
	require.NoError(t, err)
	assert.Equal(t, "Disbind PDF", cap.Title)
	assert.True(t, cap.AcceptsStdin())
	stdinUrn := cap.GetStdinMediaUrn()
	require.NotNil(t, stdinUrn)
	assert.Equal(t, "media:pdf", *stdinUrn)
}

func TestCapExists(t *testing.T) {
	registry, err := NewCapRegistry()
	require.NoError(t, err)

	// Test with a URN that doesn't exist
	exists := registry.CapExists(regTestUrn("op=nonexistent;target=fake"))
	assert.False(t, exists)
}

// URL Encoding Tests - Guard against the bug where encoding "cap:" causes 404s

// buildRegistryURL replicates the URL construction logic from fetchFromRegistry
func buildRegistryURL(capUrn string) string {
	normalizedUrn := capUrn
	if parsed, err := urn.NewCapUrnFromString(capUrn); err == nil {
		normalizedUrn = parsed.String()
	}
	tagsPart := strings.TrimPrefix(normalizedUrn, "cap:")
	encodedTags := url.PathEscape(tagsPart)
	return fmt.Sprintf("%s/cap:%s", DefaultRegistryBaseURL, encodedTags)
}

// TEST139: Test URL construction keeps cap prefix literal and only encodes tags part
func Test139_url_keeps_cap_prefix_literal(t *testing.T) {
	urn := `cap:in="media:string";op=test;out="media:object"`
	registryURL := buildRegistryURL(urn)

	// URL must contain literal "/cap:" not encoded
	assert.Contains(t, registryURL, "/cap:", "URL must contain literal '/cap:' not encoded")
	// URL must NOT contain "cap%3A" (encoded version)
	assert.NotContains(t, registryURL, "cap%3A", "URL must not encode 'cap:' as 'cap%3A'")
}

// TEST140: Test URL encodes media URNs with proper percent encoding for special characters
func Test140_url_encodes_media_urns(t *testing.T) {
	// Colons don't need quoting, so the canonical form won't have quotes
	urn := `cap:in=media:listing-id;op=use_grinder;out=media:task;id`
	registryURL := buildRegistryURL(urn)

	// URL should contain the media URN values
	assert.Contains(t, registryURL, "media:listing-id", "URL should contain media URN")
	// Note: url.PathEscape doesn't encode =, :, or ; as they're valid in paths
	// The key requirement is that the URL is valid and the Netlify function can decode it
}

// TEST141: Test exact URL format contains properly encoded media URN components
func Test141_url_format_is_valid(t *testing.T) {
	// Colons don't need quoting, so the canonical form won't have quotes
	urn := `cap:in=media:listing-id;op=use_grinder;out=media:task;id`
	registryURL := buildRegistryURL(urn)

	// URL should be parseable
	parsed, err := url.Parse(registryURL)
	require.NoError(t, err, "Generated URL must be valid")

	// Host should be capdag.com
	assert.Equal(t, "capdag.com", parsed.Host, "Host must be capdag.com")

	// Raw URL string should start with the correct base
	assert.True(t, strings.HasPrefix(registryURL, DefaultRegistryBaseURL+"/cap:"), "URL must start with base URL and /cap:")
}

// TEST142: Test normalize handles different tag orders producing same canonical form
func Test142_normalize_handles_different_tag_orders(t *testing.T) {
	urn1 := `cap:op=test;in="media:string";out="media:object"`
	urn2 := `cap:in="media:string";out="media:object";op=test`

	url1 := buildRegistryURL(urn1)
	url2 := buildRegistryURL(urn2)

	assert.Equal(t, url1, url2, "Different tag orders should produce the same URL")
}

// TEST143: Test default config uses capdag.com or environment variable values
func Test143_default_config(t *testing.T) {
	config := DefaultRegistryConfig()
	// Default should use capdag.com (unless env var is set)
	registryURL := os.Getenv("CAPDAG_REGISTRY_URL")
	if registryURL == "" {
		assert.Contains(t, config.RegistryBaseURL, "capdag.com", "Default registry URL should be capdag.com")
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
	registry := NewCapRegistryForTestWithConfig(config)
	assert.Equal(t, "https://test-registry.local", registry.Config().RegistryBaseURL)
}
