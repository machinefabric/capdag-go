package fabric

// Tests for the unified fabric registry.
//
// These moved here from `cap` and `media` when the two split registries became
// one: the numbers are unchanged, because the behavior they assert is. A test
// that exercises NOTATION-level alias handling stayed in `machine`, where the
// notation lives.

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

// TEST0123: Cap exists
func Test0123_CapExists(t *testing.T) {
	registry := NewForTest()

	// Absent from the manifest ⇒ does not exist, and that is not an error.
	exists, err := registry.CapExists(regTestUrn("nonexistent;target=fake"))
	require.NoError(t, err)
	assert.False(t, exists)

	// Present in the cache ⇒ exists, with no network involved.
	seeded := regTestUrn("seeded;target=real")
	registry.AddCapsToCache([]*cap.Cap{
		buildCap(seeded, "Seeded", []string{"media:enc=utf-8"}, "media:enc=utf-8")})
	exists, err = registry.CapExists(seeded)
	require.NoError(t, err)
	assert.True(t, exists)
}

// Per-cap URL construction. The new scheme uses /caps/<sha256>,
// where the hash is computed over the canonical URN's UTF-8 bytes.
// buildRegistryURL replicates the construction logic from fetchFromRegistry.

// TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction

// TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction

// TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction
func Test138_parse_registry_json_with_stdin(t *testing.T) {
	// JSON with stdin args - means cap accepts stdin of specified media type
	jsonData := `{"urn":"cap:in=\"media:document;ext=pdf\";disbind;out=\"media:enc=utf-8;page\"","aliases":["disbind"],"title":"Disbind PDF","args":[{"media_urn":"media:document;ext=pdf","required":true,"sources":[{"stdin":"media:document;ext=pdf"}],"name":"input"}],"outputs":[{"media_urn":"media:enc=utf-8;page","name":"output","output_description":"output"}]}`

	var registryResp cap.RegistryCapResponse
	err := json.Unmarshal([]byte(jsonData), &registryResp)
	require.NoError(t, err, "Failed to parse JSON")

	cap, err := registryResp.ToCap()
	require.NoError(t, err)
	assert.Equal(t, "Disbind PDF", cap.Title)
	assert.True(t, cap.AcceptsStdin())
	stdinUrn := cap.GetStdinMediaUrn()
	require.NotNil(t, stdinUrn)
	assert.Equal(t, "media:document;ext=pdf", *stdinUrn)
}

// TEST141: URL has the right shape — protocol, host, /caps/ prefix, 64 hex chars, no extension.

// TEST141: URL has the right shape — protocol, host, /caps/ prefix, 64 hex chars, no extension.

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

// TEST142: Different tag orders normalise to the same URL — the canonicaliser strips the variation before hashing.

// TEST142: Different tag orders normalise to the same URL — the canonicaliser strips the variation before hashing.
func Test142_normalize_handles_different_tag_orders(t *testing.T) {
	urn1 := `cap:test;in="media:string";out="media:object"`
	urn2 := `cap:in="media:string";out="media:object";test`

	url1 := buildRegistryURL(urn1)
	url2 := buildRegistryURL(urn2)

	assert.Equal(t, url1, url2, "Different tag orders should produce the same URL")
}

// TEST143: Default config points at https://fabric.capdag.com/ unless overridden by CDG_FABRIC_REGISTRY_URL.

// TEST143: Default config points at https://fabric.capdag.com/ unless overridden by CDG_FABRIC_REGISTRY_URL.

// TEST143: Default config points at https://fabric.capdag.com/ unless overridden by CDG_FABRIC_REGISTRY_URL.
func Test143_default_config(t *testing.T) {
	config := DefaultConfig()
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

// TEST144: Test custom registry URL updates both registry and schema base URLs

// TEST144: Test custom registry URL updates both registry and schema base URLs
func Test144_custom_registry_url(t *testing.T) {
	config := DefaultConfig()
	config = config.WithRegistryURL("https://localhost:8888")
	assert.Equal(t, "https://localhost:8888", config.RegistryBaseURL)
	assert.Equal(t, "https://localhost:8888/schema", config.SchemaBaseURL)
}

// TEST145: Test custom registry and schema URLs set independently

// TEST145: Test custom registry and schema URLs set independently

// TEST145: Test custom registry and schema URLs set independently
func Test145_custom_registry_and_schema_url(t *testing.T) {
	config := DefaultConfig()
	config = config.WithRegistryURL("https://localhost:8888")
	config = config.WithSchemaURL("https://schemas.example.com")
	assert.Equal(t, "https://localhost:8888", config.RegistryBaseURL)
	assert.Equal(t, "https://schemas.example.com", config.SchemaBaseURL)
}

// TEST146: Test schema URL not overwritten when set explicitly before registry URL

// TEST146: Test schema URL not overwritten when set explicitly before registry URL

// TEST146: Test schema URL not overwritten when set explicitly before registry URL
func Test146_schema_url_not_overwritten_when_explicit(t *testing.T) {
	// If schema URL is set explicitly first, changing registry URL shouldn't change it
	config := DefaultConfig()
	config = config.WithSchemaURL("https://schemas.example.com")
	config = config.WithRegistryURL("https://localhost:8888")
	assert.Equal(t, "https://localhost:8888", config.RegistryBaseURL)
	assert.Equal(t, "https://schemas.example.com", config.SchemaBaseURL)
}

// TEST147: Test registry for test with custom config creates registry with specified URLs

// TEST147: Test registry for test with custom config creates registry with specified URLs

// TEST147: Test registry for test with custom config creates registry with specified URLs
func Test147_registry_for_test_with_config(t *testing.T) {
	config := DefaultConfig()
	config = config.WithRegistryURL("https://test-registry.local")
	registry := NewForTestWithConfig(config)
	assert.Equal(t, "https://test-registry.local", registry.Config().RegistryBaseURL)
}

// TEST908: cached caps remain accessible while offline; an UNCACHED one is a
// hard network-blocked error rather than a silent miss, so a caller can tell
// "not in the fabric" from "we refused to look".

// TEST614: Verify registry creation succeeds and cache directory exists

// TEST614: Verify registry creation succeeds and cache directory exists
func Test614_registry_creation(t *testing.T) {
	registry := NewForTest()
	assert.NotNil(t, registry)
	assert.DirExists(t, registry.CacheDir())
	// The identity cap is mandatory in every capability set, so a freshly
	// constructed registry already answers for it.
	_, ok := registry.GetCachedCap("cap:effect=none")
	assert.True(t, ok, "a new registry must carry the mandatory identity cap")
}

// TEST136 (deleted): exercised the private `cacheKey` method on
// the unified FabricRegistry. The on-disk cache filename scheme is
// an implementation detail of the persistence layer; equivalent
// observable behavior — that two equivalent URNs land in the same
// cache slot — is covered by Test140 (`same_cap_different_spellings_same_url`).
// Rust and Python dropped this; this deletion keeps the Go mirror
// in parity.

// TEST908: cached caps remain accessible while offline; an UNCACHED one is a
// hard network-blocked error rather than a silent miss, so a caller can tell
// "not in the fabric" from "we refused to look".

// TEST908: cached caps remain accessible while offline; an UNCACHED one is a
// hard network-blocked error rather than a silent miss, so a caller can tell
// "not in the fabric" from "we refused to look".
func Test908_cached_caps_accessible_when_offline(t *testing.T) {
	registry := NewForTest()
	seeded := regTestUrn("offline-seeded;target=real")
	registry.AddCapsToCache([]*cap.Cap{
		buildCap(seeded, "Seeded", []string{"media:enc=utf-8"}, "media:enc=utf-8")})

	registry.SetOffline(true)

	got, err := registry.GetCap(seeded)
	require.NoError(t, err, "a cached cap must stay readable while offline")
	// Compare canonical forms: the fixture writes the URN in source order and
	// the registry keys on the canonicalized spelling.
	canonical, cerr := NormalizeCapURN(seeded)
	require.NoError(t, cerr)
	assert.Equal(t, canonical, got.Urn.String())

	_, err = registry.GetCap(regTestUrn("offline-absent;target=fake"))
	require.Error(t, err)
	assert.True(t, IsNotFound(err) || kindOf(err) == ErrNetworkBlocked,
		"an uncached cap while offline must fail explicitly, got %v", err)
}

// TEST1893: The fabric registry's on-disk cache root is namespaced per
// registry origin. A cache populated from one origin must never be reused to
// satisfy a lookup against another — prod and staging serve different bytes for
// the same URN/version, so an origin-blind cache silently resolves against the
// wrong snapshot. This pins three properties: distinct origins → distinct
// roots; same origin → identical root (deterministic, so caching actually
// hits); and the slug is the same slug_for scheme the cartridge registry layout
// uses, living under the shared "capdag" cache directory.

// Test1887: the Manifest type round-trips an `aliases` map.

// Test1887: the Manifest type round-trips an `aliases` map.
func Test1887_ManifestSerdeRoundTripsAliases(t *testing.T) {
	body := `{"version":1,"previous":0,"caps":{},"media":{},"aliases":{"pdf2text":3,"jsondoc":1},"dims":{}}`
	var m media.Manifest
	require.NoError(t, json.Unmarshal([]byte(body), &m))
	assert.Equal(t, uint32(3), m.Aliases["pdf2text"])
	assert.Equal(t, uint32(1), m.Aliases["jsondoc"])

	out, err := json.Marshal(&m)
	require.NoError(t, err)
	var back map[string]any
	require.NoError(t, json.Unmarshal(out, &back))
	aliases := back["aliases"].(map[string]any)
	assert.Equal(t, float64(3), aliases["pdf2text"])
	assert.Equal(t, float64(1), aliases["jsondoc"])
}

// TEST0123: Cap exists

// TEST1888: resolve_alias returns the alias target untyped. Seeding a media alias and resolving it yields the media URN; a malformed alias name is rejected before any lookup.
func Test1888_ResolveAliasReturnsTarget(t *testing.T) {
	reg := NewForTest()
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "jsondoc", Target: "media:fmt=json;record", Version: 1})

	target, err := reg.ResolveAlias("jsondoc")
	require.NoError(t, err)
	assert.Equal(t, "media:fmt=json;record", target)

	target, err = reg.ResolveAlias("JSONDoc")
	require.NoError(t, err)
	assert.Equal(t, "media:fmt=json;record", target)

	_, err = reg.ResolveAlias("bad:name")
	assert.Error(t, err)
}

// Test1889: resolve alias typed enforces the expected kind.

// Test1889: resolve alias typed enforces the expected kind.

// Test1889: resolve alias typed enforces the expected kind.
func Test1889_ResolveAliasTypedEnforcesKind(t *testing.T) {
	reg := NewForTest()
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "jsondoc", Target: "media:fmt=json;record", Version: 1})

	_, err := reg.ResolveAliasTyped("jsondoc", media.AliasTargetMedia)
	assert.NoError(t, err)
	_, err = reg.ResolveAliasTyped("jsondoc", "")
	assert.NoError(t, err)
	_, err = reg.ResolveAliasTyped("jsondoc", media.AliasTargetCap)
	assert.Error(t, err, "a media alias demanded as a cap must fail hard")
}

// TEST1890: get_cap accepts a cap alias and returns the aliased cap; a media alias passed to get_cap fails hard (typed boundary). This proves alias substitution AND type enforcement at the registry's cap surface.

// TEST1890: get_cap accepts a cap alias and returns the aliased cap; a media alias passed to get_cap fails hard (typed boundary). This proves alias substitution AND type enforcement at the registry's cap surface.

// TEST1890: get_cap accepts a cap alias and returns the aliased cap; a media alias passed to get_cap fails hard (typed boundary). This proves alias substitution AND type enforcement at the registry's cap surface.
func Test1890_GetCapViaAliasAndTypeMismatch(t *testing.T) {
	reg := NewForTest()
	c := buildCap(`cap:extract;in="media:document;ext=pdf";out="media:enc=utf-8"`, "extract", []string{"media:document;ext=pdf"}, "media:enc=utf-8")
	canonical := c.UrnString()
	reg.AddCapsToCache([]*cap.Cap{c})
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "pdf2text", Target: canonical, Version: 1})

	got, err := reg.GetCap("pdf2text")
	require.NoError(t, err)
	assert.Equal(t, canonical, got.UrnString())

	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "jsondoc", Target: "media:fmt=json;record", Version: 1})
	_, err = reg.GetCap("jsondoc")
	assert.Error(t, err, "a media alias at GetCap must fail hard")
}

// TEST1891: get_media_def accepts a media alias and returns the aliased spec; a cap alias passed to get_media_def fails hard.

// TEST1891: get_media_def accepts a media alias and returns the aliased spec; a cap alias passed to get_media_def fails hard.

// TEST1891: get_media_def accepts a media alias and returns the aliased spec; a cap alias passed to get_media_def fails hard.
func Test1891_GetMediaDefViaAliasAndTypeMismatch(t *testing.T) {
	reg := NewForTest()
	reg.AddSpec(media.StoredMediaDef{Urn: "media:fmt=json;record", MediaType: "application/json", Title: "JSON"})
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "jsondoc", Target: "media:fmt=json;record", Version: 1})

	spec, err := reg.GetMediaDef("jsondoc")
	require.NoError(t, err)
	assert.Equal(t, "media:fmt=json;record", spec.Urn)

	reg.InsertCachedAliasForTest(media.StoredAlias{
		Name:    "pdf2text",
		Target:  `cap:extract;in="media:document;ext=pdf";out="media:enc=utf-8"`,
		Version: 1,
	})
	_, err = reg.GetMediaDef("pdf2text")
	assert.Error(t, err, "a cap alias at GetMediaDef must fail hard")
}

// TEST1892: an unknown alias name is a hard not-found, never a silent empty; unknown and malformed names are treated the same. This is the "expose issues, no fallback" contract.

// TEST1892: an unknown alias name is a hard not-found, never a silent empty; unknown and malformed names are treated the same. This is the "expose issues, no fallback" contract.

// TEST1892: an unknown alias name is a hard not-found, never a silent empty; unknown and malformed names are treated the same. This is the "expose issues, no fallback" contract.
func Test1892_UnknownAliasIsNotFound(t *testing.T) {
	reg := NewForTest()
	_, err := reg.GetAlias("nosuchalias")
	assert.Error(t, err)
	_, err = reg.AliasDefverFor("nosuchalias")
	assert.Error(t, err)
	_, ok := reg.ResolveAliasCached("nosuchalias")
	assert.False(t, ok)
	_, ok = reg.ResolveAliasCached("bad:name")
	assert.False(t, ok)
}

// TEST0123: Cap exists

// TEST1893: The fabric registry's on-disk cache root is namespaced per
// registry origin. A cache populated from one origin must never be reused to
// satisfy a lookup against another — prod and staging serve different bytes for
// the same URN/version, so an origin-blind cache silently resolves against the
// wrong snapshot. This pins three properties: distinct origins → distinct
// roots; same origin → identical root (deterministic, so caching actually
// hits); and the slug is the same slug_for scheme the cartridge registry layout
// uses, living under the shared "capdag" cache directory.

// TEST1893: The fabric registry's on-disk cache root is namespaced per
// registry origin. A cache populated from one origin must never be reused to
// satisfy a lookup against another — prod and staging serve different bytes for
// the same URN/version, so an origin-blind cache silently resolves against the
// wrong snapshot. This pins three properties: distinct origins → distinct
// roots; same origin → identical root (deterministic, so caching actually
// hits); and the slug is the same slug_for scheme the cartridge registry layout
// uses, living under the shared "capdag" cache directory.
func Test1893_CacheRootIsNamespacedPerRegistryOrigin(t *testing.T) {
	prod, err := CacheDirFor("https://fabric.capdag.com")
	require.NoError(t, err, "prod cache root")
	staging, err := CacheDirFor("https://fabric-staging.capdag.com")
	require.NoError(t, err, "staging cache root")
	stagingAgain, err := CacheDirFor("https://fabric-staging.capdag.com")
	require.NoError(t, err, "staging cache root again")

	assert.NotEqual(t, prod, staging,
		"prod and staging must not share a cache root — they serve different bytes for the same URN/version")
	assert.Equal(t, staging, stagingAgain,
		"the same registry origin must map to a stable cache root, or caching never hits")

	// The final path component is exactly the cartridge-registry slug of the
	// origin URL — one slug scheme across the codebase: a path-safe transform of
	// the URL's authority (host[:port]), byte-for-byte identical to
	// bifaci.SlugFor. For a bare host URL that is just the lowercased host.
	assert.Equal(t, "fabric-staging.capdag.com", filepath.Base(staging),
		"cache root must end in the registry authority slug")
	// And the parent of that slug is the shared "capdag" cache directory.
	assert.Equal(t, "capdag", filepath.Base(filepath.Dir(staging)),
		"the per-origin slug must live under the capdag cache directory")
}

// TEST6396: A malformed cap URN must FAIL HARD, not be passed through raw (the
// old silent fallback) and surface later as a misleading "not in manifest" /
// cache-miss. The `out` value below contains an unquoted `=`, which the cap
// grammar rejects. Against the old `if err == nil { normalized = ... }`
// fallback, normalizeCapUrn returned the raw string and GetCap then reported a
// not-found/manifest error; this test asserts the truthful parse error and that
// no path panics on a bad URN.

// TEST1894: selectDisplayAlias picks the SHORTEST name, ties broken
// alphabetically. This is the deterministic ordering every aliased-display
// surface relies on; a regression here silently changes which alias the whole
// UI renders. Mirrors Rust registry::test1894.

// TEST1894: selectDisplayAlias picks the SHORTEST name, ties broken
// alphabetically. This is the deterministic ordering every aliased-display
// surface relies on; a regression here silently changes which alias the whole
// UI renders. Mirrors Rust registry::test1894.

// TEST1894: selectDisplayAlias picks the SHORTEST name, ties broken
// alphabetically. This is the deterministic ordering every aliased-display
// surface relies on; a regression here silently changes which alias the whole
// UI renders. Mirrors Rust registry::test1894.
func Test1894_SelectDisplayAliasOrdering(t *testing.T) {
	// Shorter wins over longer regardless of alphabetical order.
	got, ok := SelectDisplayAlias([]string{"png-image", "png", "image-png"})
	require.True(t, ok)
	assert.Equal(t, "png", got)

	// Equal length → alphabetical (a09 < a16).
	got, ok = SelectDisplayAlias([]string{"a16", "a09", "a12"})
	require.True(t, ok)
	assert.Equal(t, "a09", got)

	// Single candidate returns itself.
	got, ok = SelectDisplayAlias([]string{"solo"})
	require.True(t, ok)
	assert.Equal(t, "solo", got)

	// Empty set → not found.
	_, ok = SelectDisplayAlias(nil)
	assert.False(t, ok)
}

// TEST1895: DisplayAliasForURN reverse-resolves a URN to its display alias.
// Proves: (1) the shortest-then-alphabetical winner among multiple aliases on
// the same target, (2) a NON-canonical query URN (different tag order) still
// resolves because the query is canonicalised before matching, (3) a URN with
// no alias returns not-found, (4) a non-URN string returns not-found. Mirrors
// Rust registry::test1895.

// TEST1895: DisplayAliasForURN reverse-resolves a URN to its display alias.
// Proves: (1) the shortest-then-alphabetical winner among multiple aliases on
// the same target, (2) a NON-canonical query URN (different tag order) still
// resolves because the query is canonicalised before matching, (3) a URN with
// no alias returns not-found, (4) a non-URN string returns not-found. Mirrors
// Rust registry::test1895.

// TEST1895: DisplayAliasForURN reverse-resolves a URN to its display alias.
// Proves: (1) the shortest-then-alphabetical winner among multiple aliases on
// the same target, (2) a NON-canonical query URN (different tag order) still
// resolves because the query is canonicalised before matching, (3) a URN with
// no alias returns not-found, (4) a non-URN string returns not-found. Mirrors
// Rust registry::test1895.
func Test1895_DisplayAliasForURN(t *testing.T) {
	registry := NewForTest()
	// Two aliases on the same cap target; "i2s" is shorter than "int2str".
	registry.InsertCachedAliasForTest(media.StoredAlias{
		Name:    "int2str",
		Target:  `cap:coerce;in="media:integer;numeric";out="media:enc=utf-8"`,
		Version: 1,
	})
	registry.InsertCachedAliasForTest(media.StoredAlias{
		Name:    "i2s",
		Target:  `cap:coerce;in="media:integer;numeric";out="media:enc=utf-8"`,
		Version: 1,
	})
	// A media alias too.
	registry.InsertCachedAliasForTest(media.StoredAlias{
		Name:    "json",
		Target:  "media:fmt=json;record",
		Version: 1,
	})

	// Canonical query → shortest alias wins.
	got, ok := registry.DisplayAliasForURN(`cap:coerce;in="media:integer;numeric";out="media:enc=utf-8"`)
	require.True(t, ok)
	assert.Equal(t, "i2s", got)

	// NON-canonical query (media tags reordered): must still resolve via
	// canonicalisation. media:record;fmt=json canonicalises to
	// media:fmt=json;record.
	got, ok = registry.DisplayAliasForURN("media:record;fmt=json")
	require.True(t, ok)
	assert.Equal(t, "json", got)

	// A real URN with no alias → not found.
	_, ok = registry.DisplayAliasForURN("media:document;enc=utf-8;ext=pdf")
	assert.False(t, ok)

	// A non-URN (no cap:/media: prefix) → not found, never a panic.
	_, ok = registry.DisplayAliasForURN("int2str")
	assert.False(t, ok)
}

// TEST1896: CachedCapAliases returns only CAP-targeted aliases as (name, target)
// pairs — media aliases are excluded. Drives the notation editor's registered-
// alias completions. Mirrors Rust registry::test1896.

// TEST1896: CachedCapAliases returns only CAP-targeted aliases as (name, target)
// pairs — media aliases are excluded. Drives the notation editor's registered-
// alias completions. Mirrors Rust registry::test1896.

// TEST1896: CachedCapAliases returns only CAP-targeted aliases as (name, target)
// pairs — media aliases are excluded. Drives the notation editor's registered-
// alias completions. Mirrors Rust registry::test1896.
func Test1896_CachedCapAliasesFiltersToCapTargets(t *testing.T) {
	registry := NewForTest()
	capTarget := `cap:coerce;in="media:integer;numeric";out="media:enc=utf-8"`
	registry.InsertCachedAliasForTest(media.StoredAlias{
		Name:    "int2str",
		Target:  capTarget,
		Version: 1,
	})
	registry.InsertCachedAliasForTest(media.StoredAlias{
		Name:    "json",
		Target:  "media:fmt=json;record",
		Version: 1,
	})
	capAliases := registry.CachedCapAliases()
	// Only the cap alias is returned; the media alias is filtered out.
	require.Len(t, capAliases, 1, "got: %v", capAliases)
	assert.Equal(t, "int2str", capAliases[0].Name)
	assert.Equal(t, capTarget, capAliases[0].CapURN)
}

// TEST614: Verify registry creation succeeds and cache directory exists

// TEST1899: a media def published under a manifest (v >= 1) resolves to the
// VERSIONED object path `/media/<sha>/<defver>.json`, never the legacy flat
// path `/media/<sha>`. The flat path is the pre-manifest (v0) layout; a
// registry that silently runs in v0 mode fetches it and 404s every lookup
// against a versioned registry — the exact regression where a fabric-registry
// mirror defaulted its manifest version to 0. This pins both the URL rule and
// the manifest-driven defver resolution. Mirrors the Rust reference's
// test0144_media_def_resolves_to_versioned_object_path_under_manifest.

// TEST1899: a media def published under a manifest (v >= 1) resolves to the
// VERSIONED object path `/media/<sha>/<defver>.json`, never the legacy flat
// path `/media/<sha>`. The flat path is the pre-manifest (v0) layout; a
// registry that silently runs in v0 mode fetches it and 404s every lookup
// against a versioned registry — the exact regression where a fabric-registry
// mirror defaulted its manifest version to 0. This pins both the URL rule and
// the manifest-driven defver resolution. Mirrors the Rust reference's
// test0144_media_def_resolves_to_versioned_object_path_under_manifest.
func Test1899_MediaDefResolvesToVersionedObjectPathUnderManifest(t *testing.T) {
	// 1. Object-path rule: defver >= 1 -> versioned; defver 0 -> flat.
	config := DefaultConfig()
	config = config.WithRegistryURL("https://fabric.example.test")
	cacheDir := "/tmp/capdag-test-cache-0144"
	mediaUrn := "media:enc=utf-8;ext=md"
	hash := sha256.Sum256([]byte(mediaUrn))
	hexHash := fmt.Sprintf("%x", hash)

	versioned, _ := mediaURLAndCachePath(cacheDir, config, mediaUrn, 1)
	assert.Equal(t, fmt.Sprintf("https://fabric.example.test/media/%s/1.json", hexHash), versioned,
		"a def at manifest defver 1 must resolve to the versioned object path")

	flat, _ := mediaURLAndCachePath(cacheDir, config, mediaUrn, 0)
	assert.Equal(t, fmt.Sprintf("https://fabric.example.test/media/%s", hexHash), flat,
		"defver 0 is the legacy flat path — the wrong target for a versioned registry")

	// 2. Manifest-driven defver: a registry pinned at v >= 1 resolves a
	// published media def to its pinned defver (versioned), never 0.
	registry := NewForTest() // pinned at manifest v1
	assert.GreaterOrEqual(t, registry.ManifestVersion(), uint32(1),
		"the production registry must be pinned at manifest v >= 1, never the legacy v0 flat-path mode")

	registry.AddSpec(media.StoredMediaDef{
		Urn:        mediaUrn,
		MediaType:  "text/markdown",
		Title:      "Markdown",
		Extensions: []string{"md"},
	})
	defver, err := registry.MediaDefverFor(mediaUrn)
	require.NoError(t, err, "a published media def under a v >= 1 manifest must resolve a defver")
	assert.Equal(t, registry.ManifestVersion(), defver,
		"a published media def under a v >= 1 manifest must resolve to the pinned defver, not 0")

	// 3. A URN that is NOT part of the snapshot is a hard NotFound — the
	// registry does NOT silently fall back to defver 0 (the flat path that
	// 404s). This is the fail-hard contract that replaced the silent v0
	// fallback.
	_, err = registry.MediaDefverFor("media:enc=utf-8;ext=zzz-not-in-snapshot")
	require.Error(t, err, "a URN outside the manifest must NOT resolve to a defver")
	assert.Contains(t, err.Error(), "not part of manifest",
		"the error must name the missing-from-manifest cause, not a misleading 404")
}

// TEST136 (deleted): exercised the private `cacheKey` method on
// the unified FabricRegistry. The on-disk cache filename scheme is
// an implementation detail of the persistence layer; equivalent
// observable behavior — that two equivalent URNs land in the same
// cache slot — is covered by Test140 (`same_cap_different_spellings_same_url`).
// Rust and Python dropped this; this deletion keeps the Go mirror
// in parity.

// TEST136 (deleted): exercised the private `cacheKey` method on
// the unified FabricRegistry. The on-disk cache filename scheme is
// an implementation detail of the persistence layer; equivalent
// observable behavior — that two equivalent URNs land in the same
// cache slot — is covered by Test140 (`same_cap_different_spellings_same_url`).
// Rust and Python dropped this; this deletion keeps the Go mirror
// in parity.
func Test6186_RegistryGetCap(t *testing.T) {
	registry := NewForTest()

	// A URN absent from the pinned manifest is a hard not-found, and the
	// message names the manifest — not the network — because the lookup never
	// gets that far.
	_, err := registry.GetCap(regTestUrn("test;target=fake"))
	require.Error(t, err)
	assert.True(t, IsNotFound(err), "expected a not-found, got %v", err)
	assert.Contains(t, err.Error(), "is not part of manifest v1")
}

// TEST6325: Registry validation

// TEST6325: Registry validation

// TEST6325: Registry validation
func Test6325_RegistryValidation(t *testing.T) {
	registry := NewForTest()

	capUrn, err := urn.NewCapUrnFromString(regTestUrn("test;target=fake"))
	require.NoError(t, err)
	local := cap.NewCap(capUrn, "Test Command", []string{"test-cmd"})

	// Validation resolves the canonical definition first, so a cap the fabric
	// does not carry fails there rather than reporting a false match.
	err = registry.ValidateCap(local)
	assert.Error(t, err)
}

// TEST6329: Cache operations

// TEST6329: Cache operations

// TEST6329: Cache operations
func Test6329_CacheOperations(t *testing.T) {
	registry := NewForTest()
	registry.AddSpec(media.StoredMediaDef{
		Urn: "media:fmt=json;record", MediaType: "application/json", Title: "JSON"})
	require.NotNil(t, registry.GetCachedMediaDef("media:fmt=json;record"))

	require.NoError(t, registry.ClearCache())

	// Clearing drops what was cached and leaves the cache root usable — and the
	// mandatory identity cap survives, because it is not cached content.
	assert.Nil(t, registry.GetCachedMediaDef("media:fmt=json;record"))
	assert.DirExists(t, registry.CacheDir())
	_, ok := registry.GetCachedCap("cap:effect=none")
	assert.True(t, ok, "identity must survive a cache clear")
}

// TEST6382: Test parsing registry JSON without stdin args verifies cap structure

// TEST6382: Test parsing registry JSON without stdin args verifies cap structure

// TEST6382: Test parsing registry JSON without stdin args verifies cap structure
func Test6382_parse_registry_json(t *testing.T) {
	// JSON without stdin args - means cap doesn't accept stdin
	jsonData := `{"urn":"cap:in=\"media:listing-id\";use-grinder;out=\"media:id;task\"","aliases": ["grinder_task"],"title":"Create Grinder Tool Task","args":[{"media_urn":"media:listing-id","name":"opt1","required":true,"sources":[{"cli_flag":"--listing-id"}]}]}`

	var registryResp cap.RegistryCapResponse
	err := json.Unmarshal([]byte(jsonData), &registryResp)
	require.NoError(t, err, "Failed to parse JSON")

	cap, err := registryResp.ToCap()
	require.NoError(t, err)
	assert.Equal(t, "Create Grinder Tool Task", cap.Title)
	assert.Equal(t, "grinder_task", cap.PrimaryAlias())
	assert.Nil(t, cap.GetStdinMediaUrn(), "No stdin source in args means no stdin support")
}

// TEST138: Test parsing registry JSON with stdin args verifies stdin media URN extraction

// TEST6388: Per-cap URL is /caps/<sha256-hex> — no URN-grammar characters in the path, no percent-encoding gymnastics.

// TEST6388: Per-cap URL is /caps/<sha256-hex> — no URN-grammar characters in the path, no percent-encoding gymnastics.

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

// TEST6391: Equivalent URNs (different tag order, etc.) hash to the
// same key. This is the property that makes cross-language lookups
// land at the same registry object regardless of which capdag
// implementation issued the request.

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

// TEST6396: A malformed cap URN must FAIL HARD, not be passed through raw (the
// old silent fallback) and surface later as a misleading "not in manifest" /
// cache-miss. The `out` value below contains an unquoted `=`, which the cap
// grammar rejects. Against the old `if err == nil { normalized = ... }`
// fallback, normalizeCapUrn returned the raw string and GetCap then reported a
// not-found/manifest error; this test asserts the truthful parse error and that
// no path panics on a bad URN.

// TEST6396: A malformed cap URN must FAIL HARD, not be passed through raw (the
// old silent fallback) and surface later as a misleading "not in manifest" /
// cache-miss. The `out` value below contains an unquoted `=`, which the cap
// grammar rejects. Against the old `if err == nil { normalized = ... }`
// fallback, normalizeCapUrn returned the raw string and GetCap then reported a
// not-found/manifest error; this test asserts the truthful parse error and that
// no path panics on a bad URN.

// TEST6396: A malformed cap URN must FAIL HARD, not be passed through raw (the
// old silent fallback) and surface later as a misleading "not in manifest" /
// cache-miss. The `out` value below contains an unquoted `=`, which the cap
// grammar rejects. Against the old `if err == nil { normalized = ... }`
// fallback, normalizeCapUrn returned the raw string and GetCap then reported a
// not-found/manifest error; this test asserts the truthful parse error and that
// no path panics on a bad URN.
func Test6396_MalformedCapUrnFailsHard(t *testing.T) {
	malformed := `cap:coerce;in="media:integer;numeric";out=media:enc=utf-8`

	// Direct normalization helper must return an error, NOT the raw string.
	normalized, err := NormalizeCapURN(malformed)
	require.Error(t, err, "normalizeCapUrn on a malformed URN must error, not fall back to the raw string")
	assert.Empty(t, normalized, "no normalized value on parse failure")
	assert.Contains(t, err.Error(), "malformed cap URN",
		"the error must name the malformation, not masquerade as not-found")

	// Public path (GetCap) must surface the parse error, NOT a not-found /
	// manifest error. It must not panic.
	registry := NewForTest()
	cap, gerr := registry.GetCap(malformed)
	require.Error(t, gerr, "malformed cap URN must not resolve")
	assert.Nil(t, cap, "no cap returned for a malformed URN")
	assert.Contains(t, gerr.Error(), "malformed cap URN",
		"GetCap on a malformed URN must be a parse/malformed error, not a not-found error")
	assert.NotContains(t, gerr.Error(), "not found",
		"the malformed-URN error must not be reported as a not-found")
	assert.NotContains(t, gerr.Error(), "manifest",
		"the malformed-URN error must not be reported as a manifest miss")

	// Lookup contract (value, found): a malformed URN is a graceful miss, no panic.
	got, ok := registry.GetCachedCap(malformed)
	assert.False(t, ok, "GetCachedCap on a malformed URN must report not-found")
	assert.Nil(t, got, "GetCachedCap on a malformed URN must return nil")
}

// TEST1894: selectDisplayAlias picks the SHORTEST name, ties broken
// alphabetically. This is the deterministic ordering every aliased-display
// surface relies on; a regression here silently changes which alias the whole
// UI renders. Mirrors Rust registry::test1894.

// TEST908: cached caps remain accessible while offline.

// Per-cap URL construction. The new scheme uses /caps/<sha256>,
// where the hash is computed over the canonical URN's UTF-8 bytes.
// buildRegistryURL replicates the construction logic from fetchFromRegistry.
// buildCap is the same fixture `machine` uses, duplicated here because a test
// helper is not exported and these tests moved packages. Keeping it identical
// keeps the fixtures the moved tests were written against unchanged.

// Per-cap URL construction. The new scheme uses /caps/<sha256>,
// where the hash is computed over the canonical URN's UTF-8 bytes.
// buildRegistryURL replicates the construction logic from fetchFromRegistry.
// buildCap is the same fixture `machine` uses, duplicated here because a test
// helper is not exported and these tests moved packages. Keeping it identical
// keeps the fixtures the moved tests were written against unchanged.
func buildCap(capUrnStr, title string, argMediaUrns []string, outputMediaUrn string) *cap.Cap {
	capUrnParsed, err := urn.NewCapUrnFromString(capUrnStr)
	if err != nil {
		panic("test fixture: invalid cap URN " + capUrnStr + ": " + err.Error())
	}
	args := make([]cap.CapArg, len(argMediaUrns))
	for i, mu := range argMediaUrns {
		stdinVal := mu
		args[i] = cap.NewCapArg(mu, "input", true, []cap.ArgSource{{Stdin: &stdinVal}})
	}
	output := cap.NewCapOutput(outputMediaUrn, "output", "output of "+title)
	return &cap.Cap{
		Urn:     capUrnParsed,
		Title:   title,
		Aliases: []string{"test-fixture://" + title},
		Args:    args,
		Outputs: []cap.CapOutput{*output},
	}
}

func buildRegistryURL(capUrn string) string {
	normalizedUrn := capUrn
	if parsed, err := urn.NewCapUrnFromString(capUrn); err == nil {
		normalizedUrn = parsed.String()
	}
	digest := sha256.Sum256([]byte(normalizedUrn))
	return fmt.Sprintf("%s/caps/%x", DefaultRegistryBaseURL, digest)
}

// TEST6388: Per-cap URL is /caps/<sha256-hex> — no URN-grammar characters in the path, no percent-encoding gymnastics.

// Test helper for registry tests

// Test helper for registry tests

// Test helper for registry tests
func regTestUrn(tags string) string {
	if tags == "" {
		return `cap:in="media:void";out="media:object"`
	}
	return `cap:in="media:void";out="media:object";` + tags
}

// TEST614: Verify registry creation succeeds and cache directory exists

// TEST1899: a media def published under a manifest (v >= 1) resolves to the
// VERSIONED object path `/media/<sha>/<defver>.json`, never the legacy flat
// path `/media/<sha>`. The flat path is the pre-manifest (v0) layout; a
// registry that silently runs in v0 mode fetches it and 404s every lookup
// against a versioned registry — the exact regression where a fabric-registry
// mirror defaulted its manifest version to 0. This pins both the URL rule and
// the manifest-driven defver resolution. Mirrors the Rust reference's
// test0144_media_def_resolves_to_versioned_object_path_under_manifest.

// TEST607: media_urns_for_extension returns error for unknown extension
func Test607_media_urns_for_extension_unknown(t *testing.T) {
	registry := NewForTest()

	_, err := registry.MediaUrnsForExtension("zzzzunknown")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zzzzunknown")
}

// TEST609: get_extension_mappings returns all registered extension→URN pairs.
func Test609_get_extension_mappings(t *testing.T) {
	registry := NewForTest()

	registry.AddSpec(media.StoredMediaDef{
		Urn:        "media:document;ext=pdf",
		MediaType:  "application/octet-stream",
		Title:      "Test",
		Extensions: []string{"pdf"},
	})
	registry.AddSpec(media.StoredMediaDef{
		Urn:        "media:document;ext=epub",
		MediaType:  "application/octet-stream",
		Title:      "Test",
		Extensions: []string{"epub"},
	})

	mappings := registry.GetExtensionMappings()
	extNames := make(map[string]bool)
	for _, m := range mappings {
		extNames[m.Extension] = true
	}
	assert.True(t, extNames["pdf"], "Should contain pdf")
	assert.True(t, extNames["epub"], "Should contain epub")
}

// TEST610: get_cached_spec returns None for unknown and Some for known
// TEST610: get_cached_spec returns None for unknown and Some for known
func Test610_get_cached_spec(t *testing.T) {
	registry := NewForTest()

	// Unknown spec
	assert.Nil(t, registry.GetCachedMediaDef("media:nonexistent;xyzzy"))

	// Add a spec and verify retrieval
	registry.AddSpec(media.StoredMediaDef{
		Urn:       "media:enc=utf-8;test;spec",
		MediaType: "text/plain",
		Title:     "Test Spec",
	})

	retrieved := registry.GetCachedMediaDef("media:enc=utf-8;test;spec")
	require.NotNil(t, retrieved, "Should find spec by URN")
	assert.Equal(t, "Test Spec", retrieved.Title)
}

// TEST618 lives in profile_test.go (profile schema registry creation with a
// temp disk cache) — it is the genuine Rust TEST618 (media/profile.rs). The
// former spec_test.go TEST618 was a tautological NotNil-only duplicate over the
// in-memory FabricRegistry and was removed.

// TEST615 (deleted): exercised the on-disk cache-key hashing
// scheme — an internal persistence detail with no user-observable
// behavior. Rust and Python dropped this for the same reason; this
// deletion keeps the Go mirror in parity.

func Test608_media_urns_for_extension_populated(t *testing.T) {
	registry := NewForTest()

	registry.AddSpec(media.StoredMediaDef{
		Urn:        "media:document;ext=pdf",
		MediaType:  "application/pdf",
		Title:      "PDF Document",
		Extensions: []string{"pdf"},
	})

	urns, err := registry.MediaUrnsForExtension("pdf")
	require.NoError(t, err)
	assert.NotEmpty(t, urns, "Should have at least one URN for pdf")

	found := false
	for _, u := range urns {
		if strings.Contains(u, "pdf") {
			found = true
			break
		}
	}
	assert.True(t, found, "URNs should contain pdf: %v", urns)

	// Case-insensitive
	urnsUpper, err := registry.MediaUrnsForExtension("PDF")
	require.NoError(t, err)
	assert.Equal(t, urns, urnsUpper)
}

// TEST616: StoredMediaDef converts to MediaDef preserving every field.

// TEST617: Verify normalize_media_urn produces consistent non-empty results
func Test617_normalize_media_urn(t *testing.T) {
	urn1, err1 := NormalizeMediaURN("media:string")
	urn2, err2 := NormalizeMediaURN("media:string")
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NotEmpty(t, urn1)
	assert.NotEmpty(t, urn2)
	assert.Equal(t, urn1, urn2)
}

// TEST12714: a definition a registry serves is held to the definition rules
// before it is cached or returned. One that does not parse is a parse error;
// one that parses and breaks a rule is a validation error naming the cap and
// the rule.
func Test12714_a_served_definition_is_held_to_the_definition_rules(t *testing.T) {
	const capURN = `cap:in="media:document;ext=pdf";disbind;out="media:enc=utf-8;page"`
	definition := func(args []any, outputs []any) []byte {
		body, err := json.Marshal(map[string]any{
			"urn": capURN, "aliases": []string{"disbind"}, "title": "Disbind PDF",
			"args": args, "outputs": outputs,
		})
		require.NoError(t, err)
		return body
	}
	main := func(stdin string) map[string]any {
		return map[string]any{
			"media_urn": "media:enc=utf-8;file-path", "name": "document", "required": true,
			"sources": []any{map[string]any{"stdin": stdin}},
		}
	}
	page := map[string]any{"media_urn": "media:enc=utf-8;page", "name": "pages", "output_description": "Pages"}

	c, err := checkedCapDefinition(definition([]any{main("media:document;ext=pdf")}, []any{page}), capURN)
	require.NoError(t, err, "a valid definition")
	require.NotNil(t, c.MainOutput())
	assert.Equal(t, "pages", c.MainOutput().Name)

	_, err = checkedCapDefinition([]byte("{not json"), capURN)
	require.Error(t, err)
	assert.Equal(t, ErrParse, kindOf(err))

	// The cap URN says it produces pages, and the definition declares no such output.
	_, err = checkedCapDefinition(definition([]any{main("media:document;ext=pdf")}, []any{}), capURN)
	require.Error(t, err)
	assert.Equal(t, ErrValidation, kindOf(err))
	assert.Contains(t, err.Error(), "RULE21")
	assert.Contains(t, err.Error(), "disbind")

	// The main input is declared under a type that is not the cap's `in=`.
	_, err = checkedCapDefinition(definition([]any{main("media:document")}, []any{page}), capURN)
	require.Error(t, err)
	assert.Equal(t, ErrValidation, kindOf(err))
	assert.Contains(t, err.Error(), "RULE15")
}

// TEST12715: the cache holds only checked definitions, so one on disk that
// breaks a rule was written under other rules: loading drops its file and
// keeps the rest, and the definition is fetched anew when next asked for.
func Test12715_a_cached_definition_that_breaks_a_rule_is_dropped(t *testing.T) {
	scratch := t.TempDir()
	r := NewForTestInDir(DefaultConfig(), scratch)
	entry := func(c *cap.Cap) []byte {
		definition, err := json.Marshal(c)
		require.NoError(t, err)
		body, err := json.Marshal(capCacheEntry{Definition: definition, CachedAt: 0, TTLHours: 0})
		require.NoError(t, err)
		return body
	}
	capOf := func(op string) *cap.Cap {
		u, err := urn.NewCapUrnFromString(`cap:in="media:void";` + op + `;out="media:record"`)
		require.NoError(t, err)
		c := cap.NewCap(u, op, []string{op})
		c.Version = 3
		return c
	}
	kept := capOf("kept")
	kept.AddOutput(*cap.NewCapOutput("media:record", "record", "The record"))
	// The same shape without the output its URN states (RULE21).
	stale := capOf("stale")

	keptFile := filepath.Join(scratch, "caps", "aa", "3.json")
	staleFile := filepath.Join(scratch, "caps", "bb", "3.json")
	for file, c := range map[string]*cap.Cap{keptFile: kept, staleFile: stale} {
		require.NoError(t, os.MkdirAll(filepath.Dir(file), 0o755))
		require.NoError(t, os.WriteFile(file, entry(c), 0o644))
	}

	r.loadAllCachedCaps()
	_, hasKept := r.GetCachedCap(kept.Urn.String())
	assert.True(t, hasKept, "the valid definition is loaded")
	_, hasStale := r.GetCachedCap(stale.Urn.String())
	assert.False(t, hasStale, "the definition that breaks a rule is not")
	_, err := os.Stat(keptFile)
	assert.NoError(t, err)
	_, err = os.Stat(staleFile)
	assert.True(t, os.IsNotExist(err), "the stale entry's file is removed")
}

// TEST12816: fetching a cap requires the definition of every media URN a
// machine can meet through it: its in= and out=, every arg's own URN and
// the type it accepts, and every output's URN — not the main output's
// alone. Each once; the wildcard names no definition; a URN that does not
// parse fails the footprint rather than being left out of it.
func Test12816_a_caps_media_footprint_covers_every_output_and_accepted_type(t *testing.T) {
	u, err := urn.NewCapUrnFromString(`cap:in="media:document;ext=pdf";footprint;out="media:enc=utf-8"`)
	require.NoError(t, err)
	c := cap.NewCap(u, "Footprint", []string{"footprint"})
	stdin := "media:document;ext=pdf"
	second, anything := "--second", "--anything"
	c.Args = append(c.Args,
		cap.NewCapArg("media:enc=utf-8;file-path", "document", true, []cap.ArgSource{{Stdin: &stdin}}),
		cap.NewCapArg("media:enc=utf-8;second", "second", false, []cap.ArgSource{{CliFlag: &second}}).
			Accepting("media:enc=utf-8;accepted"),
		cap.NewCapArg("media:", "anything", false, []cap.ArgSource{{CliFlag: &anything}}),
	)
	c.AddOutput(*cap.NewCapOutput("media:enc=utf-8", "text", "text"))
	c.AddOutput(*cap.NewCapOutput("media:enc=utf-8;rest", "rest", "rest"))

	var expected []string
	for _, each := range []string{
		"media:document;ext=pdf",
		"media:enc=utf-8",
		"media:enc=utf-8;file-path",
		"media:enc=utf-8;second",
		"media:enc=utf-8;accepted",
		"media:enc=utf-8;rest",
	} {
		normalized, err := NormalizeMediaURN(each)
		require.NoError(t, err)
		expected = append(expected, normalized)
	}
	got, err := collectReferencedMediaURNs(c)
	require.NoError(t, err)
	assert.ElementsMatch(t, expected, got, "each once, the wildcard left out")

	c.AddOutput(*cap.NewCapOutput("not a urn", "broken", "broken"))
	_, err = collectReferencedMediaURNs(c)
	require.Error(t, err, "a URN that does not parse fails the footprint")
	assert.Equal(t, ErrParse, kindOf(err))
}

// =============================================================================
// Dims
// =============================================================================

func testDim(name string, version uint32, values ...string) media.StoredDim {
	dim := media.StoredDim{
		Name:          name,
		Version:       version,
		Title:         "Title of " + name,
		Documentation: "## " + name + "\nWhat " + name + " is.",
		Values:        []media.StoredDimValue{},
	}
	for _, v := range values {
		doc := "The " + v + " value."
		dim.Values = append(dim.Values, media.StoredDimValue{
			Name:          v,
			Title:         strings.ToUpper(v),
			Tags:          []media.DimTag{{Key: name, Value: v}},
			Documentation: &doc,
		})
	}
	return dim
}

// TEST1966: the Manifest type round-trips a `dims` map (name -> defver)
// through JSON, and a manifest without one is refused: every manifest this
// library reads names the dims of its snapshot.
func Test1966_ManifestCarriesADimsMapAndRequiresIt(t *testing.T) {
	body := `{"version":11,"previous":10,"caps":{},"media":{},"aliases":{},"dims":{"language":1,"file-type":2}}`
	var m media.Manifest
	require.NoError(t, json.Unmarshal([]byte(body), &m))
	assert.Equal(t, uint32(1), m.Dims["language"])
	assert.Equal(t, uint32(2), m.Dims["file-type"])
	out, err := json.Marshal(&m)
	require.NoError(t, err)
	var back map[string]any
	require.NoError(t, json.Unmarshal(out, &back))
	assert.Equal(t, float64(2), back["dims"].(map[string]any)["file-type"])

	without := `{"version":11,"previous":10,"caps":{},"media":{},"aliases":{}}`
	var refused media.Manifest
	err = json.Unmarshal([]byte(without), &refused)
	require.Error(t, err, "a manifest without dims is refused")
	assert.Contains(t, err.Error(), "dims")
}

// TEST1967: GetDim returns a dim the registry holds; a name the pinned
// manifest does not list is NotFound naming the manifest version, decided
// before any fetch (the registry is offline, and the error is not the offline
// one); a string that is no dim name is refused as such.
func Test1967_GetDimResolvesThroughTheManifest(t *testing.T) {
	reg := NewForTest()
	reg.InsertCachedDimForTest(testDim("language", 1, "en", "es"))
	got, err := reg.GetDim("language")
	require.NoError(t, err, "a held dim resolves")
	require.NotNil(t, got.Value("es"))
	assert.Equal(t, "ES", got.Value("es").Title)
	defver, err := reg.DimDefverFor("language")
	require.NoError(t, err)
	assert.Equal(t, uint32(1), defver)

	reg.SetOffline(true)
	_, err = reg.GetDim("tone")
	require.Error(t, err, "an unlisted dim is not found")
	assert.Equal(t, ErrNotFound, kindOf(err), "%v", err)
	assert.Contains(t, err.Error(), "dim 'tone' is not part of manifest v1")

	_, err = reg.GetDim("Tone")
	require.Error(t, err)
	assert.Equal(t, ErrValidation, kindOf(err), "%v", err)
}

// TEST1968: a dim is addressed at dims/<sha256-of-name>/<defver>.json, on the
// registry and in the cache; a body served for it must be that dim at that
// version, with no field this library does not know, and must hold (values
// present, names unique).
func Test1968_ADimObjectIsTheDimAndVersionAskedFor(t *testing.T) {
	config := DefaultConfig().WithRegistryURL("https://fabric.example.test")
	cache := filepath.Join(os.TempDir(), "capdag-test-cache-1968")
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte("language")))
	url, path := dimURLAndCachePath(cache, config, "language", 3)
	assert.Equal(t, "https://fabric.example.test/dims/"+hash+"/3.json", url)
	assert.Equal(t, filepath.Join(cache, "dims", hash, "3.json"), path)

	good, err := json.Marshal(testDim("language", 3, "en"))
	require.NoError(t, err)
	dim, err := checkedDim(good, "language", 3)
	require.NoError(t, err)
	assert.Equal(t, "language", dim.Name)

	_, err = checkedDim(good, "file-type", 3)
	require.Error(t, err)
	assert.Equal(t, ErrParse, kindOf(err))
	assert.Contains(t, err.Error(), "does not match requested name")

	_, err = checkedDim(good, "language", 2)
	require.Error(t, err)
	assert.Equal(t, ErrParse, kindOf(err))
	assert.Contains(t, err.Error(), "manifest pins defver 2")

	var extra map[string]any
	require.NoError(t, json.Unmarshal(good, &extra))
	extra["predicates"] = []any{}
	extraBody, err := json.Marshal(extra)
	require.NoError(t, err)
	_, err = checkedDim(extraBody, "language", 3)
	require.Error(t, err)
	assert.Equal(t, ErrParse, kindOf(err))
	assert.Contains(t, err.Error(), "predicates")

	twice, err := json.Marshal(testDim("language", 3, "en", "en"))
	require.NoError(t, err)
	_, err = checkedDim(twice, "language", 3)
	require.Error(t, err)
	assert.Equal(t, ErrValidation, kindOf(err))
	assert.Contains(t, err.Error(), "declares value 'en' more than once")

	none, err := json.Marshal(testDim("language", 3))
	require.NoError(t, err)
	_, err = checkedDim(none, "language", 3)
	require.Error(t, err)
	assert.Equal(t, ErrValidation, kindOf(err))
}

func dimRefs(pairs ...[2]string) []media.DimRef {
	refs := make([]media.DimRef, 0, len(pairs))
	for _, p := range pairs {
		refs = append(refs, media.NewDimRef(p[0], p[1]))
	}
	return refs
}

func capWithDims(t *testing.T, urnStr string, refs []media.DimRef) *cap.Cap {
	t.Helper()
	parsed, err := urn.NewCapUrnFromString(urnStr)
	require.NoError(t, err)
	c := cap.NewCap(parsed, "t", []string{"t"})
	c.Version = 1
	c.Dims = refs
	return c
}

// TEST1971: what is loaded from the disk cache is kept only as the pinned
// manifest names it — a dim at another version is dropped — and a definition
// is kept only when every dim it names survives with the value it names. At v0
// there are no dims.
func Test1971_TheDiskCacheKeepsDefinitionsOnlyWithTheirPinnedDims(t *testing.T) {
	manifest := media.EmptyManifest(11)
	manifest.Dims["language"] = 2
	manifest.Dims["medium"] = 1
	capURNs := []string{"cap:a;in=media:;out=media:", "cap:b;in=media:;out=media:", "cap:c;in=media:;out=media:"}
	normalized := make([]string, len(capURNs))
	for i, u := range capURNs {
		n, err := NormalizeCapURN(u)
		require.NoError(t, err)
		normalized[i] = n
		manifest.Caps[n] = 1
	}
	manifest.Media["media:x"] = 1
	manifest.Media["media:y"] = 1

	caps := map[string]*cap.Cap{
		normalized[0]: capWithDims(t, capURNs[0], dimRefs([2]string{"medium", "text"})),
		normalized[1]: capWithDims(t, capURNs[1], dimRefs([2]string{"language", "en"})),
		normalized[2]: capWithDims(t, capURNs[2], nil),
	}
	specs := map[string]media.StoredMediaDef{
		"media:x": {Urn: "media:x", Version: 1, MediaType: "text/plain", Title: "t", Dims: dimRefs([2]string{"medium", "text"})},
		"media:y": {Urn: "media:y", Version: 1, MediaType: "text/plain", Title: "t", Dims: dimRefs([2]string{"medium", "audio"})},
	}
	aliases := map[string]media.StoredAlias{}
	dims := map[string]media.StoredDim{
		"language": testDim("language", 1, "en"), // the manifest pins 2
		"medium":   testDim("medium", 1, "text"),
	}

	retainPinned(manifest, 11, caps, specs, aliases, dims)
	assert.Equal(t, []string{"medium"}, sortedKeys(dims), "a dim at another version is dropped")
	assert.Equal(t, sortedStrings([]string{normalized[0], normalized[2]}), sortedKeys(caps),
		"the cap naming the dropped dim is dropped with it")
	assert.Equal(t, []string{"media:x"}, sortedKeys(specs), "a value the dim lacks drops the definition")

	v0Dims := map[string]media.StoredDim{"medium": testDim("medium", 1, "text")}
	retainPinned(media.EmptyManifest(0), 0, map[string]*cap.Cap{}, map[string]media.StoredMediaDef{},
		map[string]media.StoredAlias{}, v0Dims)
	assert.Empty(t, v0Dims, "v0 has no dims")
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return sortedStrings(keys)
}

func sortedStrings(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

// TEST1972: a definition is cached only once every dim it names is held, with
// the value it names. A dim the manifest does not list is refused naming the
// definition, the dim and the manifest version, before any fetch; a value the
// dim does not declare is refused naming all three; one dim named twice is
// refused.
func Test1972_ADefinitionNeedsEveryDimItNames(t *testing.T) {
	reg := NewForTest()
	reg.InsertCachedDimForTest(testDim("language", 1, "en"))
	reg.SetOffline(true)

	require.NoError(t, reg.ensureDimsCached("cap 'cap:probe'", dimRefs([2]string{"language", "en"})),
		"a held dim and value pass")

	err := reg.ensureDimsCached("cap 'cap:probe'", dimRefs([2]string{"language", "fr"}))
	require.Error(t, err)
	assert.Equal(t, ErrValidation, kindOf(err), "%v", err)
	assert.Equal(t, "cap 'cap:probe' names value 'fr' of dim 'language', which the dim does not declare", err.Error())

	err = reg.ensureDimsCached("cap 'cap:probe'", dimRefs([2]string{"tone", "formal"}))
	require.Error(t, err)
	assert.Equal(t, ErrNotFound, kindOf(err), "%v", err)
	assert.Equal(t, "cap 'cap:probe' names dim 'tone', which is not in manifest v1", err.Error())

	err = reg.ensureDimsCached("cap 'cap:probe'", dimRefs([2]string{"language", "en"}, [2]string{"language", "en"}))
	require.Error(t, err)
	assert.Equal(t, ErrValidation, kindOf(err), "%v", err)
	assert.Contains(t, err.Error(), "named more than once")
}
