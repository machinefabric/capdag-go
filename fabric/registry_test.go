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
	jsonData := `{"urn":"cap:in=\"media:ext=pdf\";disbind;out=\"media:enc=utf-8;page\"","aliases":["disbind"],"title":"Disbind PDF","args":[{"media_urn":"media:ext=pdf","required":true,"sources":[{"stdin":"media:ext=pdf"}]}]}`

	var registryResp cap.RegistryCapResponse
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
	body := `{"version":1,"previous":0,"caps":{},"media":{},"aliases":{"pdf2text":3,"jsondoc":1}}`
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
	c := buildCap(`cap:extract;in="media:ext=pdf";out="media:enc=utf-8"`, "extract", []string{"media:ext=pdf"}, "media:enc=utf-8")
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
		Target:  `cap:extract;in="media:ext=pdf";out="media:enc=utf-8"`,
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
	_, ok = registry.DisplayAliasForURN("media:enc=utf-8;ext=pdf")
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
	jsonData := `{"urn":"cap:in=\"media:listing-id\";use-grinder;out=\"media:task;id\"","aliases":["grinder_task"],"title":"Create Grinder Tool Task","cap_description":"Create a task for initial document analysis - first glance phase","metadata":{},"media_defs":[{"urn":"media:listing-id","media_type":"text/plain","title":"Listing ID","profile_uri":"https://machinefabric.com/schema/listing-id","schema":{"type":"string","pattern":"[0-9a-f-]{36}","description":"MachineFabric listing UUID"}},{"urn":"media:task;id","media_type":"application/json","title":"Task ID","profile_uri":"https://capdag.com/schema/grinder_task-output","schema":{"type":"object","additionalProperties":false,"properties":{"task_id":{"type":"string","description":"ID of the created task"},"task_type":{"type":"string","description":"Type of task created"}},"required":["task_id","task_type"]}}],"args":[{"media_urn":"media:listing-id","required":true,"sources":[{"cli_flag":"--listing-id"}],"arg_description":"ID of the listing to analyze"}],"output":{"media_urn":"media:task;id","output_description":"Created task information"},"registered_by":{"username":"joeharshamshiri","registered_at":"2026-01-15T00:44:29.851Z"}}`

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
		args[i] = cap.NewCapArg(mu, true, []cap.ArgSource{{Stdin: &stdinVal}})
	}
	output := cap.NewCapOutput(outputMediaUrn, "output of "+title)
	return &cap.Cap{
		Urn:     capUrnParsed,
		Title:   title,
		Aliases: []string{"test-fixture://" + title},
		Args:    args,
		Output:  output,
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

// TEST608: media_urns_for_extension returns URNs after adding a spec with extensions
// TEST609: get_extension_mappings returns all registered extension→URN pairs.
func Test609_get_extension_mappings(t *testing.T) {
	registry := NewForTest()

	registry.AddSpec(media.StoredMediaDef{
		Urn:        "media:ext=pdf",
		MediaType:  "application/octet-stream",
		Title:      "Test",
		Extensions: []string{"pdf"},
	})
	registry.AddSpec(media.StoredMediaDef{
		Urn:        "media:ext=epub",
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
		Urn:        "media:ext=pdf",
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
