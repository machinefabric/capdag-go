package bifaci

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nightlyDevIdentity() *DiscoveryIdentity {
	return &DiscoveryIdentity{
		Channel:                  CartridgeChannelNightly,
		RegistryURL:              nil,
		FabricManifestVersion:    1,
		CartridgeRegistryVersion: CartridgeRegistryVersion,
	}
}

// installFixture lays down
// {root}/{slug}/v{CartridgeRegistryVersion}/{channelFolder}/{name}/{version}/ —
// the version level pins to the host build's registry version, exactly where
// discovery scans. When cartridgeJSON is non-nil, also writes it plus an
// executable `entry` binary so ReadCartridgeJsonFromDir accepts the directory
// and discovery reaches its own identity checks.
func installFixture(t *testing.T, root, slug, channelFolder, name, version string, cartridgeJSON *string, entry string) {
	t.Helper()
	dir := filepath.Join(root, slug, fmt.Sprintf("v%d", CartridgeRegistryVersion), channelFolder, name, version)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	if cartridgeJSON != nil {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cartridge.json"), []byte(*cartridgeJSON), 0o644))
		entryPath := filepath.Join(dir, entry)
		require.NoError(t, os.WriteFile(entryPath, []byte("#!/bin/sh\nexit 0\n"), 0o755))
	}
}

func devCartridgeJSON(channel string, fabricManifestVersion uint32) string {
	return `{"name":"cart","version":"1.0.0","channel":"` + channel +
		`","registry_url":null,"entry":"cart","installed_at":"2024-01-01T00:00:00Z","fabric_manifest_version":` +
		strconv.FormatUint(uint64(fabricManifestVersion), 10) + `}`
}

func expectIncompatible(t *testing.T, out []DiscoveredCartridge, kind CartridgeAttachmentErrorKind) {
	t.Helper()
	require.Len(t, out, 1, "expected exactly one discovered entry")
	require.Equal(t, DiscoveredCartridgeIncompatible, out[0].Kind, "expected an Incompatible classification")
	require.NotNil(t, out[0].Error)
	assert.Equal(t, kind, out[0].Error.Kind, "wrong attachment-error kind: %s", out[0].Error.Message)
}

// registrySlugFor is the registry slug for a fixed URL, so tests can place a
// registry cartridge under the folder that matches its declared registry_url
// (three-place rule).
func registrySlugFor(url string) string {
	return SlugFor(&url)
}

func registryCartridgeJSON(url, channel string, fmv uint32) string {
	return `{"name":"cart","version":"1.0.0","channel":"` + channel +
		`","registry_url":"` + url + `","entry":"cart","installed_at":"2024-01-01T00:00:00Z","fabric_manifest_version":` +
		strconv.FormatUint(uint64(fmv), 10) + `}`
}

// TEST90: Absent scan root yields empty roster
func Test0090_absent_scan_root_yields_empty_roster(t *testing.T) {
	root := t.TempDir()
	out, err := DiscoverCartridges(filepath.Join(root, "does-not-exist"), nightlyDevIdentity())
	require.NoError(t, err)
	assert.Empty(t, out, "no install tree must be an empty roster, not an error")
}

// TEST91: Missing cartridge json is manifest invalid
func Test0091_missing_cartridge_json_is_manifest_invalid(t *testing.T) {
	root := t.TempDir()
	installFixture(t, root, "dev", "nightly", "cart", "1.0.0", nil, "cart")
	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindManifestInvalid)
}

// TEST92: Channel mismatch is bad installation
func Test0092_channel_mismatch_is_bad_installation(t *testing.T) {
	root := t.TempDir()
	// Declares release but lives under nightly/ — host is nightly.
	json := devCartridgeJSON("release", 1)
	installFixture(t, root, "dev", "nightly", "cart", "1.0.0", &json, "cart")
	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindBadInstallation)
}

// TEST94: Fabric manifest mismatch is flagged
func Test0094_fabric_manifest_mismatch_is_flagged(t *testing.T) {
	root := t.TempDir()
	json := devCartridgeJSON("nightly", 999)
	installFixture(t, root, "dev", "nightly", "cart", "1.0.0", &json, "cart")
	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindFabricManifestVersionMismatch)
}

// TEST120: Registry url under dev slug is rejected
func Test0120_registry_url_under_dev_slug_is_rejected(t *testing.T) {
	root := t.TempDir()
	// A non-null registry_url placed under the reserved dev slug violates the
	// three-place rule — ReadCartridgeJsonFromDir rejects it as a bad install
	// context (BadInstallation), surfaced + logged, never hosted.
	json := `{"name":"cart","version":"1.0.0","channel":"nightly","registry_url":"https://cartridges.example.com/manifest","entry":"cart","installed_at":"2024-01-01T00:00:00Z","fabric_manifest_version":1}`
	installFixture(t, root, "dev", "nightly", "cart", "1.0.0", &json, "cart")
	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindBadInstallation)
}

// TEST1875: scan-all — a registry slug folder AND the dev slot present on disk are BOTH scanned, regardless of the host's own baked registry. The dev cartridge (null registry under dev/) and the registry cartridge (its url hashing to its slug folder) each reach their probe. Both fixtures lack a real bifaci binary, so both end at HandshakeFailed — proving discovery REACHED them (was not filtered out by a registry pin), which is the behavior under test. A registry-pin rejection would instead surface BadInstallation and never probe.
func Test1875_scan_all_reaches_both_dev_and_registry_slugs(t *testing.T) {
	root := t.TempDir()
	url := "https://cartridges.example.com/manifest"
	rslug := registrySlugFor(url)
	other := "https://other.example.com/manifest"
	// Host baked for a DIFFERENT registry than the on-disk registry cartridge.
	host := &DiscoveryIdentity{
		Channel:                  CartridgeChannelNightly,
		RegistryURL:              &other,
		FabricManifestVersion:    1,
		CartridgeRegistryVersion: CartridgeRegistryVersion,
	}
	devJSON := devCartridgeJSON("nightly", 1)
	installFixture(t, root, "dev", "nightly", "devcart", "1.0.0", &devJSON, "cart")
	regJSON := registryCartridgeJSON(url, "nightly", 1)
	installFixture(t, root, rslug, "nightly", "regcart", "1.0.0", &regJSON, "cart")

	out, err := DiscoverCartridges(root, host)
	require.NoError(t, err)
	require.Len(t, out, 2, "both slugs must be scanned, got: %+v", out)
	for _, c := range out {
		require.Equal(t, DiscoveredCartridgeIncompatible, c.Kind, "expected probe-stage Incompatible, got %+v", c)
		require.NotNil(t, c.Error)
		assert.Equal(t, CartridgeAttachmentErrorKindHandshakeFailed, c.Error.Kind,
			"both reached the probe (not registry-pin-rejected): %s", c.Error.Message)
	}
}

// TEST1876: only the host's channel subtree is scanned. A cartridge under a slug's `release/` folder is invisible to a nightly host even though the slug folder is present (its `nightly/` subtree is absent).
func Test1876_other_channel_subtree_is_skipped(t *testing.T) {
	root := t.TempDir()
	url := "https://cartridges.example.com/manifest"
	rslug := registrySlugFor(url)
	regJSON := registryCartridgeJSON(url, "release", 1)
	installFixture(t, root, rslug, "release", "regcart", "1.0.0", &regJSON, "cart")

	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	assert.Empty(t, out, "a release-only slug must be invisible to a nightly host, got: %+v", out)
}

// TEST1877: a registry cartridge hand-copied under the WRONG registry slug folder fails the three-place rule (BadInstallation) — scan-all does not mean "accept anywhere", placement must still be self-consistent.
func Test1877_registry_cartridge_under_wrong_slug_is_bad_install(t *testing.T) {
	root := t.TempDir()
	url := "https://cartridges.example.com/manifest"
	wrongSlug := registrySlugFor("https://somewhere-else.example.com/manifest")
	json := registryCartridgeJSON(url, "nightly", 1)
	installFixture(t, root, wrongSlug, "nightly", "cart", "1.0.0", &json, "cart")

	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindBadInstallation)
}

// TEST1878: a cartridge marked `installed_from: bundle` with no baked hash in BUNDLED_CARTRIDGE_HASHES (the const is empty under plain `cargo test`) is rejected as BadInstallation — the bundled-integrity gate fires before the probe. Proves the verify is wired into discovery; a real bundle build bakes the hash so the matching directory passes. Non-macOS only: on macOS the baked-hash path is intentionally absent (OS code-signature is the guard), so a bundled cartridge is accepted there and would instead end at the probe.
func Test1878_bundled_cartridge_without_baked_hash_is_rejected(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("macOS bundled-cartridge integrity is the OS code-signature; baked-hash gate is intentionally absent")
	}
	root := t.TempDir()
	// Dev slug (null registry) but installed_from=bundle — placement is
	// self-consistent (null→dev), so it passes read_from_dir and reaches the
	// bundled-hash gate, which has no baked entry → BadInstallation.
	json := `{"name":"cart","version":"1.0.0","channel":"nightly","registry_url":null,"entry":"cart","installed_at":"2024-01-01T00:00:00Z","installed_from":"bundle","fabric_manifest_version":1}`
	installFixture(t, root, "dev", "nightly", "cart", "1.0.0", &json, "cart")

	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindBadInstallation)
	require.NotNil(t, out[0].Error)
	assert.Contains(t, out[0].Error.Message, "bundled cartridge integrity",
		"message should name the bundled-integrity failure: %s", out[0].Error.Message)
}
