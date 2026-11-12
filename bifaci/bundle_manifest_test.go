package bifaci

// What a bundle manifest admits, and what holds a bundled cartridge to it.
//
// This mirror carries no chain verification, so the loader's signature tests
// (TEST1922, TEST1923) live with the Rust library alone. What is tested here
// needs no signature: admitting a cartridge from a manifest already in hand,
// holding a directory to a recorded hash, and the shape a verified document is
// held to. Each is measured against a manifest and a directory the test builds
// itself.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bundleCartridgeDir lays down a cartridge directory holding contents, and
// answers its path.
func bundleCartridgeDir(t *testing.T, root string, contents map[string]string) string {
	t.Helper()
	dir := filepath.Join(root, "cart", "1.0.0")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	// Excluded from the hash by HashCartridgeDirectory, and present because a
	// real cartridge has one.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cartridge.json"), []byte(`{"name":"cart"}`), 0o644))
	for name, body := range contents {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644))
	}
	return dir
}

// bundleEntry is one entry as a build records it.
func bundleEntry(name, version, channel, sha256 string) BundledCartridge {
	return BundledCartridge{
		Name:                  name,
		Version:               version,
		Channel:               channel,
		SHA256:                sha256,
		FabricManifestVersion: 1,
		CapGroups:             []CapGroup{},
	}
}

// bundleErrorKind answers the kind of a BundleError, failing the test when err
// is not one.
func bundleErrorKind(t *testing.T, err error) BundleErrorKind {
	t.Helper()
	require.Error(t, err)
	var bundleErr *BundleError
	require.True(t, errors.As(err, &bundleErr), "expected a BundleError, got %T: %v", err, err)
	return bundleErr.Kind
}

// TEST1924: a bundled cartridge is held to the bytes the manifest recorded,
// and the refusal says which cartridge was changed and where.
//
// The manifest is built here rather than loaded, because what is under test is
// the CHECK. The message is what a user reads when a host refuses to start the
// cartridge, so it must name the cartridge and say that its bundle changed.
func Test1924_a_bundled_cartridge_must_match_what_the_manifest_recorded(t *testing.T) {
	dir := bundleCartridgeDir(t, t.TempDir(), map[string]string{"cart": "the entry point"})
	recorded, err := HashCartridgeDirectory(dir)
	require.NoError(t, err)

	require.NoError(t, VerifyBundledContent("cart", "1.0.0", dir, recorded), "an unchanged cartridge passes")

	// Changed after the build recorded it.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cart"), []byte("something else"), 0o644))
	why := VerifyBundledContent("cart", "1.0.0", dir, recorded)
	require.Equal(t, BundleErrorContentMismatch, bundleErrorKind(t, why), "a changed cartridge must be refused: %v", why)
	var mismatch *BundleError
	require.True(t, errors.As(why, &mismatch))
	assert.Equal(t, recorded, mismatch.Expected, "the refusal must name what was recorded, said: %v", why)
	said := why.Error()
	assert.Contains(t, said, "the bundled cartridge cart 1.0.0 was changed")
	assert.Contains(t, said, dir, "the refusal must say where")
	assert.Contains(t, said, "is not started")

	// A file added beside the entry point is a change too.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cart"), []byte("the entry point"), 0o644))
	require.NoError(t, VerifyBundledContent("cart", "1.0.0", dir, recorded), "restored, it passes again")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "extra.dylib"), []byte("planted"), 0o644))
	assert.Equal(t, BundleErrorContentMismatch, bundleErrorKind(t, VerifyBundledContent("cart", "1.0.0", dir, recorded)))
}

// TEST1929: the manifest admits exactly the cartridges it records, on the
// channel it records them for, without reading a byte of them.
//
// Admission is what discovery does instead of hashing and starting each
// cartridge, so it must refuse everything the old check refused that does not
// need the bytes: a cartridge never recorded, a stale copy of another version,
// and one recorded for the other channel.
func Test1929_the_manifest_admits_only_what_it_records(t *testing.T) {
	manifest := NewBundleManifest("prod", []BundledCartridge{bundleEntry("cart", "1.0.0", "release", strings.Repeat("a", 64))})

	admitted, err := manifest.Admit("cart", "1.0.0", "release")
	require.NoError(t, err, "a recorded cartridge is admitted")
	assert.Equal(t, strings.Repeat("a", 64), admitted.SHA256)

	_, err = manifest.Admit("other", "1.0.0", "release")
	assert.Equal(t, BundleErrorNotListed, bundleErrorKind(t, err))
	// The right cartridge at the wrong version — a stale copy from an earlier
	// build, which a bare per-binary signature would have accepted because it
	// was validly signed once.
	_, err = manifest.Admit("cart", "0.9.0", "release")
	assert.Equal(t, BundleErrorNotListed, bundleErrorKind(t, err))
	// A manifest vouches for one channel's artefact, never the other's.
	_, err = manifest.Admit("cart", "1.0.0", "nightly")
	assert.Equal(t, BundleErrorWrongChannel, bundleErrorKind(t, err))

	// A proven root admits through its manifest.
	_, err = ProvenBundle(manifest).Admit("cart", "1.0.0", "release")
	require.NoError(t, err)

	// A root that proves nothing admits nothing, and says why.
	_, err = NoBundle("not a build's bundle").Admit("cart", "1.0.0", "release")
	require.Error(t, err)
	assert.Equal(t, "not a build's bundle", err.Error())
}

// TEST1930: a verified document is believed only in this format, with every
// field present, and with caps that pass the definition rules.
//
// No earlier format is read: a v1 manifest records no caps and no fabric
// manifest version, so a host reading one would have to start every cartridge
// to learn them, which is what v2 exists to stop. (Pinning the manifest's
// environment to the build's belongs with signature verification, which this
// mirror does not carry.)
func Test1930_a_verified_document_is_held_to_the_format_and_the_rules(t *testing.T) {
	const path = "bundle.json"
	good := NewBundleManifest("prod", []BundledCartridge{bundleEntry("cart", "1.0.0", "release", strings.Repeat("a", 64))})
	data, err := json.Marshal(good)
	require.NoError(t, err)
	parsed, err := parseBundleManifest(data, path)
	require.NoError(t, err, "a v2 manifest parses")
	assert.Equal(t, good, parsed)

	v1 := []byte(`{"format":"capdag.bundle/v1","environment":"prod","cartridges":[{"name":"cart","version":"1.0.0","channel":"release","sha256":"aa"}]}`)
	_, err = parseBundleManifest(v1, path)
	require.Equal(t, BundleErrorUnsupportedFormat, bundleErrorKind(t, err), "a v1 manifest must be refused for its format: %v", err)
	var unsupported *BundleError
	require.True(t, errors.As(err, &unsupported))
	assert.Equal(t, "capdag.bundle/v1", unsupported.Found)

	// A v2 document whose entry leaves out what discovery decides from is
	// malformed, never read as zero values.
	partial := []byte(`{"format":"capdag.bundle/v2","environment":"prod","cartridges":[{"name":"cart","version":"1.0.0","channel":"release","sha256":"aa"}]}`)
	_, err = parseBundleManifest(partial, path)
	assert.Equal(t, BundleErrorMalformed, bundleErrorKind(t, err), "an entry with no fabric manifest version or caps must be refused")

	// A recorded cap that breaks the definition rules (RULE21: an out= that is
	// not void needs its main output) is refused as an advertised one would be.
	brokenUrn, err := urn.NewCapUrnFromString(`cap:in="media:enc=utf8";test;out="media:enc=utf8;fmt=json;record"`)
	require.NoError(t, err)
	brokenCap := cap.NewCap(brokenUrn, "Test", []string{"cmd"})
	broken := NewBundleManifest("prod", []BundledCartridge{bundleEntry("cart", "1.0.0", "release", strings.Repeat("a", 64))})
	broken.Cartridges[0].CapGroups = []CapGroup{{Name: "g", Caps: []cap.Cap{*brokenCap}}}
	brokenData, err := json.Marshal(broken)
	require.NoError(t, err)
	_, err = parseBundleManifest(brokenData, path)
	assert.Equal(t, BundleErrorMalformed, bundleErrorKind(t, err), "a recorded cap that fails the definition rules must be refused")
}
