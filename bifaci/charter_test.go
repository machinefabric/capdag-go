package bifaci

// Charters, read as a client of this mirror reads them. Mirrors the structural
// half of Rust `bifaci/charter_tests.rs`: the shape, the locations, the folder
// a stored charter proves and the anchor a fetched one must be for. The
// signature half (TEST12910, 12911, 12914–12918) has no counterpart here,
// because this mirror verifies no chain (see charter.go). Every refusal below
// starts from a charter that reads, changes one thing, and checks that the one
// thing is what is refused.

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testCartridgesAnchor = "https://registry.example/cartridges"
	testCartridgesSlug   = "example-cartridges"
	testLabAnchor        = "https://lab.example/exp/t1/cartridges"
)

// cartridgesCharter is a prod cartridges charter at anchor, named slug.
func cartridgesCharter(anchor, slug string) *Charter {
	fabric := "https://registry.example/fabric"
	return &Charter{
		Format:      CharterFormat,
		Anchor:      anchor,
		Kind:        RegistryKindCartridges,
		Slug:        slug,
		Environment: "prod",
		Locations:   map[string]string{"manifest": anchor + "/v{registry_version}/manifest"},
		Fabric:      &fabric,
		IssuedAt:    1_792_800_000,
	}
}

// fabricCharter is a prod fabric charter at anchor, named slug.
func fabricCharter(anchor, slug string) *Charter {
	return &Charter{
		Format:      CharterFormat,
		Anchor:      anchor,
		Kind:        RegistryKindFabric,
		Slug:        slug,
		Environment: "prod",
		Locations: map[string]string{
			"manifest":   anchor + "/manifest/{manifest_version}.json",
			"latest":     anchor + "/manifest/latest.json",
			"definition": anchor + "/{kind}/{urn_hash}/{version}.json",
		},
		IssuedAt: 1_792_800_000,
	}
}

// labCharter is a lab cartridges charter, with an expiry of its own.
func labCharter() *Charter {
	charter := cartridgesCharter(testLabAnchor, "lab-t1-cartridges")
	experiment := "t1"
	notAfter := uint64(1_800_576_000)
	charter.Environment = "lab"
	charter.Experiment = &experiment
	charter.NotAfter = &notAfter
	return charter
}

// fetched is charter as its anchor would serve it. The signature is opaque
// here: this mirror carries it and never reads it.
func fetched(t *testing.T, charter *Charter) *FetchedCharter {
	t.Helper()
	body, err := json.Marshal(charter)
	require.NoError(t, err)
	return &FetchedCharter{Charter: charter, Body: body, Signature: []byte(`{"format":"machinefabric-charter-sig/1"}`)}
}

// storeTestCharter stores charter in {root}/{folder}, as an installer does.
func storeTestCharter(t *testing.T, root, folder string, charter *Charter) {
	t.Helper()
	f := fetched(t, charter)
	// StoreCharter refuses a folder not named for the slug; a test that
	// stores under another name writes the files itself.
	if folder == charter.Slug {
		require.NoError(t, StoreCharter(f, filepath.Join(root, folder)))
		return
	}
	dir := filepath.Join(root, folder)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, CharterFile), f.Body, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, CharterSigFile), f.Signature, 0o644))
}

// serveCharter serves charter under the anchor of an https test server, and
// returns the server and that anchor.
func serveCharter(t *testing.T, charterFor func(anchor string) *Charter) (*httptest.Server, string) {
	t.Helper()
	var anchor string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f := fetched(t, charterFor(anchor))
		switch r.URL.Path {
		case "/fabric/" + CharterFile:
			_, _ = w.Write(f.Body)
		case "/fabric/" + CharterSigFile:
			_, _ = w.Write(f.Signature)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	anchor = server.URL + "/fabric"
	return server, anchor
}

// TEST12912: a valid charter served under another anchor is refused — a
// registry cannot borrow another's charter. Nothing at an anchor's
// charter.json makes it no registry at all.
func Test12912_a_charter_for_another_anchor_is_refused(t *testing.T) {
	server, anchor := serveCharter(t, func(anchor string) *Charter { return fabricCharter(anchor, "test-fabric") })
	got, err := FetchCharter(server.Client(), anchor, RegistryKindFabric)
	require.NoError(t, err, "the charter served for its own anchor reads")
	assert.Equal(t, anchor, got.Charter.Anchor)

	borrowed, borrowedAnchor := serveCharter(t, func(string) *Charter {
		return fabricCharter("https://registry.example/fabric", "test-fabric")
	})
	_, err = FetchCharter(borrowed.Client(), borrowedAnchor, RegistryKindFabric)
	var fetchErr *CharterFetchError
	require.True(t, errors.As(err, &fetchErr), "%v", err)
	assert.Equal(t, CharterFetchRefused, fetchErr.Kind, "%v", err)

	_, err = FetchCharter(server.Client(), server.URL+"/nowhere", RegistryKindFabric)
	require.True(t, errors.As(err, &fetchErr), "%v", err)
	assert.Equal(t, CharterFetchUnchartered, fetchErr.Kind, "%v", err)
}

// TEST12913: a charter of one kind does not stand for another kind — fetched,
// or stored in a registry's folder.
func Test12913_a_charter_of_another_kind_is_refused(t *testing.T) {
	server, anchor := serveCharter(t, func(anchor string) *Charter { return fabricCharter(anchor, "test-fabric") })
	_, err := FetchCharter(server.Client(), anchor, RegistryKindCartridges)
	var fetchErr *CharterFetchError
	require.True(t, errors.As(err, &fetchErr), "%v", err)
	assert.Equal(t, CharterFetchRefused, fetchErr.Kind, "%v", err)

	root := t.TempDir()
	storeTestCharter(t, root, "test-fabric", fabricCharter("https://registry.example/fabric", "test-fabric"))
	_, err = ReadStoredCharter(filepath.Join(root, "test-fabric"), RegistryKindCartridges)
	require.Error(t, err, "a fabric's charter does not open a cartridges folder")
	_, err = ReadStoredCharter(filepath.Join(root, "test-fabric"), RegistryKindFabric)
	require.NoError(t, err)
}

// TEST12919: a location is filled with exactly the placeholders it takes —
// one missing, or one it does not take, is refused rather than guessed.
func Test12919_a_location_takes_exactly_its_placeholders(t *testing.T) {
	fabric := fabricCharter("https://registry.example/fabric", "test-fabric")
	url, err := fabric.Location("definition", map[string]string{"kind": "caps", "urn_hash": "ab12", "version": "3"})
	require.NoError(t, err)
	assert.Equal(t, "https://registry.example/fabric/caps/ab12/3.json", url)
	_, err = fabric.Location("definition", map[string]string{"kind": "caps", "urn_hash": "ab12"})
	assert.Error(t, err, "a placeholder left unfilled")
	_, err = fabric.Location("latest", map[string]string{"version": "3"})
	assert.Error(t, err, "a placeholder the location does not take")
	_, err = fabric.Location("schema", nil)
	assert.Error(t, err, "a fabric charter has no schema location")
}

// TEST12920: the shape every charter must have. Each case is a charter that
// reads with one thing wrong; the unchanged charter passes, so each refusal is
// about the one change.
func Test12920_a_charter_out_of_shape_is_malformed(t *testing.T) {
	base := func() *Charter { return cartridgesCharter(testCartridgesAnchor, testCartridgesSlug) }
	require.NoError(t, base().Validate(), "the base charter is in shape")
	require.NoError(t, labCharter().Validate(), "a lab charter is in shape")
	body, err := json.Marshal(base())
	require.NoError(t, err)
	_, err = ParseCharter(body)
	require.NoError(t, err, "the base charter parses")

	refused := func(why string, change func(c *Charter)) {
		c := base()
		change(c)
		assert.Error(t, c.Validate(), "%s was accepted", why)
	}
	refused("an http anchor", func(c *Charter) { c.Anchor = "http://registry.example/cartridges" })
	refused("an anchor ending in /", func(c *Charter) { c.Anchor += "/" })
	refused("the reserved dev slug", func(c *Charter) { c.Slug = DevSlug })
	refused("a slug outside [a-z0-9.-]", func(c *Charter) { c.Slug = "Has_Upper" })
	refused("no manifest location", func(c *Charter) { c.Locations = map[string]string{} })
	refused("a manifest location without {registry_version}", func(c *Charter) {
		c.Locations["manifest"] = "https://r.example/manifest"
	})
	refused("a placeholder the location does not take", func(c *Charter) {
		c.Locations["manifest"] = "https://r.example/v{registry_version}/{app_id}"
	})
	refused("a location of another kind", func(c *Charter) { c.Locations["latest"] = "https://r.example/latest" })
	refused("a cartridges registry with no fabric", func(c *Charter) { c.Fabric = nil })
	refused("an experiment that is not an id", func(c *Charter) {
		id := "Not An Id"
		c.Experiment = &id
	})
	refused("a not_after that is not after issue", func(c *Charter) {
		at := c.IssuedAt
		c.NotAfter = &at
	})

	unknown := strings.Replace(string(body), `"anchor"`, `"surprise":1,"anchor"`, 1)
	_, err = ParseCharter([]byte(unknown))
	assert.Error(t, err, "a field no charter has")
	manifest := strings.Replace(string(body), CharterFormat, "machinefabric-manifest-sig/1", 1)
	_, err = ParseCharter([]byte(manifest))
	assert.ErrorContains(t, err, "format", "a document of another format")
}

// TEST12921: a stored charter proves its folder only under its own slug: a
// folder renamed after install is refused, though the charter reads.
func Test12921_a_stored_charter_holds_its_folder_to_its_slug(t *testing.T) {
	root := t.TempDir()
	storeTestCharter(t, root, testCartridgesSlug, cartridgesCharter(testCartridgesAnchor, testCartridgesSlug))
	opened, err := ReadStoredCharter(filepath.Join(root, testCartridgesSlug), RegistryKindCartridges)
	require.NoError(t, err, "a stored charter reads offline")
	assert.Equal(t, testCartridgesAnchor, opened.Anchor)

	renamed := filepath.Join(root, "cartridges.machinefabric.com")
	require.NoError(t, os.Rename(filepath.Join(root, testCartridgesSlug), renamed))
	_, err = ReadStoredCharter(renamed, RegistryKindCartridges)
	assert.ErrorContains(t, err, "stored in a folder named 'cartridges.machinefabric.com'")

	err = StoreCharter(fetched(t, cartridgesCharter(testCartridgesAnchor, testCartridgesSlug)), renamed)
	assert.Error(t, err, "a charter is never stored under another name")
}

// TEST12922: a registry folder is opened through its charter, and found by
// its anchor — never by its name. `dev` is the one folder with no charter.
func Test12922_registry_folders_are_found_by_their_charters(t *testing.T) {
	root := t.TempDir()
	storeTestCharter(t, root, testCartridgesSlug, cartridgesCharter(testCartridgesAnchor, testCartridgesSlug))
	lab := labCharter()
	storeTestCharter(t, root, lab.Slug, lab)
	require.NoError(t, os.MkdirAll(filepath.Join(root, DevSlug), 0o755))
	// A folder named like the anchor's host holds no charter, and is not it.
	require.NoError(t, os.MkdirAll(filepath.Join(root, "registry.example"), 0o755))

	folder, found, ok := StoredCharterFor(root, testCartridgesAnchor, RegistryKindCartridges)
	require.True(t, ok, "the registry's folder is found by its anchor")
	assert.Equal(t, filepath.Join(root, testCartridgesSlug), folder)
	assert.Equal(t, testCartridgesSlug, found.Slug)
	_, _, ok = StoredCharterFor(root, "https://registry.example/elsewhere", RegistryKindCartridges)
	assert.False(t, ok, "an anchor with no stored charter finds nothing")

	dev, err := OpenRegistryFolder(filepath.Join(root, DevSlug))
	require.NoError(t, err)
	assert.Equal(t, DevSlug, dev.Slug())
	assert.Nil(t, dev.Anchor())
	chartered, err := OpenRegistryFolder(filepath.Join(root, testCartridgesSlug))
	require.NoError(t, err)
	require.NotNil(t, chartered.Anchor())
	assert.Equal(t, testCartridgesAnchor, *chartered.Anchor())
	_, err = OpenRegistryFolder(filepath.Join(root, "registry.example"))
	assert.Error(t, err, "a folder with no charter is no registry's")
}
