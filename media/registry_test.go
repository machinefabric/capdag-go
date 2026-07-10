package media

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST0144: a media def published under a manifest (v >= 1) resolves to the
// VERSIONED object path `/media/<sha>/<defver>.json`, never the legacy flat
// path `/media/<sha>`. The flat path is the pre-manifest (v0) layout; a
// registry that silently runs in v0 mode fetches it and 404s every lookup
// against a versioned registry — the exact regression where a fabric-registry
// mirror defaulted its manifest version to 0. This pins both the URL rule and
// the manifest-driven defver resolution. Mirrors the Rust reference's
// test0144_media_def_resolves_to_versioned_object_path_under_manifest.
func Test0144_MediaDefResolvesToVersionedObjectPathUnderManifest(t *testing.T) {
	// 1. Object-path rule: defver >= 1 -> versioned; defver 0 -> flat.
	config := DefaultRegistryConfig()
	WithRegistryURL("https://fabric.example.test")(&config)
	cacheDir := "/tmp/capdag-test-cache-0144"
	mediaUrn := "media:enc=utf-8;ext=md"
	hash := sha256.Sum256([]byte(mediaUrn))
	hexHash := fmt.Sprintf("%x", hash)

	versioned, _ := MediaURLAndCachePath(cacheDir, config, mediaUrn, 1)
	assert.Equal(t, fmt.Sprintf("https://fabric.example.test/media/%s/1.json", hexHash), versioned,
		"a def at manifest defver 1 must resolve to the versioned object path")

	flat, _ := MediaURLAndCachePath(cacheDir, config, mediaUrn, 0)
	assert.Equal(t, fmt.Sprintf("https://fabric.example.test/media/%s", hexHash), flat,
		"defver 0 is the legacy flat path — the wrong target for a versioned registry")

	// 2. Manifest-driven defver: a registry pinned at v >= 1 resolves a
	// published media def to its pinned defver (versioned), never 0.
	registry, err := NewFabricRegistryForTest() // pinned at manifest v1
	require.NoError(t, err)
	assert.GreaterOrEqual(t, registry.ManifestVersion(), uint32(1),
		"the production registry must be pinned at manifest v >= 1, never the legacy v0 flat-path mode")

	registry.AddSpec(StoredMediaDef{
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
