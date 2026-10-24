package bifaci

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
		// A root that ships no bundle. Every test that is not ABOUT the bundle
		// scans a tree nothing built, so a cartridge claiming to be bundled
		// there is in the wrong place — which is what this says.
		Bundle: NoBundle("this directory is not a build's bundle"),
	}
}

// bundledIdentity is an identity whose bundled cartridges are proven by
// manifest.
//
// The manifest is built here rather than verified, because what is under test
// is what discovery DOES with a proof. This mirror carries no chain
// verification at all — the Rust library is the only implementation of it —
// which is exactly why the proof is a parameter.
func bundledIdentity(manifest BundleManifest) *DiscoveryIdentity {
	identity := nightlyDevIdentity()
	identity.Bundle = ProvenBundle(manifest)
	return identity
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

// TEST92: Channel mismatch is a misplaced install
func Test0092_channel_mismatch_is_misplaced(t *testing.T) {
	root := t.TempDir()
	// Declares release but lives under nightly/ — host is nightly.
	json := devCartridgeJSON("release", 1)
	installFixture(t, root, "dev", "nightly", "cart", "1.0.0", &json, "cart")
	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindMisplaced)
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
	// A non-null registry_url placed under the reserved dev folder violates the
	// charter rule — ReadCartridgeJsonFromDir rejects it as a bad install
	// context (Misplaced), surfaced + logged, never hosted.
	json := `{"name":"cart","version":"1.0.0","channel":"nightly","registry_url":"https://registry.example/cartridges","entry":"cart","installed_at":"2024-01-01T00:00:00Z","fabric_manifest_version":1}`
	installFixture(t, root, "dev", "nightly", "cart", "1.0.0", &json, "cart")
	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindMisplaced)
}

// TEST1875: scan-all — a chartered registry folder AND the dev slot present
// on disk are BOTH scanned, regardless of the host's own baked registry. The
// dev cartridge (null registry under dev/) and the registry cartridge (its url
// the anchor of its folder's charter) each reach their probe. Both fixtures
// lack a real bifaci binary, so both end at HandshakeFailed — proving
// discovery REACHED them (was not filtered out by a registry pin), which is
// the behavior under test. A registry-pin rejection would instead surface
// BadInstallation and never probe.
func Test1875_scan_all_reaches_both_dev_and_registry_slugs(t *testing.T) {
	root := t.TempDir()
	storeTestCharter(t, root, testCartridgesSlug, cartridgesCharter(testCartridgesAnchor, testCartridgesSlug))
	url := testCartridgesAnchor
	rslug := testCartridgesSlug
	other := "https://other.example.com/cartridges"
	// Host baked for a DIFFERENT registry than the on-disk registry cartridge.
	host := nightlyDevIdentity()
	host.RegistryURL = &other
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
	storeTestCharter(t, root, testCartridgesSlug, cartridgesCharter(testCartridgesAnchor, testCartridgesSlug))
	url := testCartridgesAnchor
	rslug := testCartridgesSlug
	regJSON := registryCartridgeJSON(url, "release", 1)
	installFixture(t, root, rslug, "release", "regcart", "1.0.0", &regJSON, "cart")

	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	assert.Empty(t, out, "a release-only slug must be invisible to a nightly host, got: %+v", out)
}

// TEST1877: a registry cartridge hand-copied into the folder of ANOTHER
// registry fails the charter rule (Misplaced) — scan-all does not mean
// "accept anywhere", placement must still be self-consistent.
func Test1877_registry_cartridge_under_wrong_slug_is_bad_install(t *testing.T) {
	root := t.TempDir()
	storeTestCharter(t, root, testCartridgesSlug, cartridgesCharter(testCartridgesAnchor, testCartridgesSlug))
	json := registryCartridgeJSON("https://somewhere-else.example/cartridges", "nightly", 1)
	installFixture(t, root, testCartridgesSlug, "nightly", "cart", "1.0.0", &json, "cart")

	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindMisplaced)
}

// TEST12923: a registry folder is what its stored charter proves, never what
// its name says. The same cartridge, recording the same anchor, is misplaced
// in a folder with no charter and in a folder whose charter is stored under
// another name; it reaches its probe only in the folder its charter names.
func Test12923_a_registry_folder_is_proven_by_its_charter_not_its_name(t *testing.T) {
	json := registryCartridgeJSON(testCartridgesAnchor, "nightly", 1)

	// A folder named for the registry, holding no charter.
	unchartered := t.TempDir()
	installFixture(t, unchartered, testCartridgesSlug, "nightly", "cart", "1.0.0", &json, "cart")
	out, err := DiscoverCartridges(unchartered, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindMisplaced)
	assert.Contains(t, out[0].Error.Message, "no charter that reads")

	// The registry's charter, stored in a folder of another name.
	renamed := t.TempDir()
	storeTestCharter(t, renamed, "cartridges.example.com", cartridgesCharter(testCartridgesAnchor, testCartridgesSlug))
	installFixture(t, renamed, "cartridges.example.com", "nightly", "cart", "1.0.0", &json, "cart")
	out, err = DiscoverCartridges(renamed, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindMisplaced)

	// Its charter, in the folder the charter names: placement holds, and
	// discovery goes on to probe the (fake) binary.
	placed := t.TempDir()
	storeTestCharter(t, placed, testCartridgesSlug, cartridgesCharter(testCartridgesAnchor, testCartridgesSlug))
	installFixture(t, placed, testCartridgesSlug, "nightly", "cart", "1.0.0", &json, "cart")
	out, err = DiscoverCartridges(placed, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindHandshakeFailed)
}

// bundledCartridgeJSON is the cartridge.json of a bundled cartridge in the dev
// slot: placement is self-consistent (null registry → dev slug), so it passes
// every earlier check and reaches the bundled-integrity gate.
func bundledCartridgeJSON() string {
	return `{"name":"cart","version":"1.0.0","channel":"nightly","registry_url":null,"entry":"cart","installed_at":"2024-01-01T00:00:00Z","installed_from":"bundle","fabric_manifest_version":1}`
}

// TEST1878: a bundled cartridge in a root that proves nothing is refused — on
// every platform.
//
// This is the check macOS did not have. The old rule was platform-split: Linux
// and Windows verified a baked content hash and macOS verified nothing of ours,
// trusting Gatekeeper instead — so this test skipped itself on darwin. It runs
// everywhere now because the guard does.
func Test1878_a_bundled_cartridge_is_refused_where_nothing_proves_it(t *testing.T) {
	root := t.TempDir()
	json := bundledCartridgeJSON()
	installFixture(t, root, "dev", "nightly", "cart", "1.0.0", &json, "cart")

	out, err := DiscoverCartridges(root, nightlyDevIdentity())
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindMisplaced)
	require.NotNil(t, out[0].Error)
	assert.Contains(t, out[0].Error.Message, "bundled cartridge integrity",
		"message should name the bundled-integrity failure: %s", out[0].Error.Message)
}

// TEST1928: a bundled cartridge the manifest records passes, and one it records
// differently does not.
//
// The other half of TEST1878, and the one that proves the gate is a real check
// rather than a refusal of everything: the same tree, the same cartridge, and
// the only difference is what the build recorded about it. A gate that always
// said no would pass TEST1878 alone.
func Test1928_a_bundled_cartridge_passes_exactly_when_the_manifest_records_it(t *testing.T) {
	root := t.TempDir()
	json := bundledCartridgeJSON()
	installFixture(t, root, "dev", "nightly", "cart", "1.0.0", &json, "cart")

	versionDir := filepath.Join(
		root, "dev", fmt.Sprintf("v%d", CartridgeRegistryVersion), "nightly", "cart", "1.0.0",
	)
	recorded, err := HashCartridgeDirectory(versionDir)
	require.NoError(t, err)

	listed := func(sha256 string) *DiscoveryIdentity {
		return bundledIdentity(NewBundleManifest("dev", []BundledCartridge{{
			Name:    "cart",
			Version: "1.0.0",
			Channel: "nightly",
			SHA256:  sha256,
		}}))
	}

	// Recorded as it is on disk: past the gate. It still ends at the HELLO
	// probe, because the fixture's entry point is not a cartridge — what
	// matters is that the failure is no longer the integrity one.
	out, err := DiscoverCartridges(root, listed(recorded))
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.NotNil(t, out[0].Error)
	assert.NotContains(t, out[0].Error.Message, "bundled cartridge integrity",
		"a cartridge the manifest records must get past the integrity gate: %s", out[0].Error.Message)

	// Recorded as something else — the cartridge was changed after the build
	// recorded it.
	out, err = DiscoverCartridges(root, listed(strings.Repeat("f", 64)))
	require.NoError(t, err)
	expectIncompatible(t, out, CartridgeAttachmentErrorKindMisplaced)
	require.NotNil(t, out[0].Error)
	assert.Contains(t, out[0].Error.Message, "bundled cartridge integrity",
		"a cartridge that differs from the manifest must be refused: %s", out[0].Error.Message)
}
