package bifaci

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chartedRegistryServer serves a cartridges registry at <server>/cartridges:
// its charter, and a manifest at the location the charter names — a path no
// URL convention would put it at. edit adjusts the manifest before it is
// served.
func chartedRegistryServer(t *testing.T, edit func(anchor string, m *CartridgeRegistry)) (*httptest.Server, string) {
	t.Helper()
	var anchor string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		charter := cartridgesCharter(anchor, testCartridgesSlug)
		charter.Locations["manifest"] = anchor + "/catalogue/r{registry_version}.json"
		*charter.Fabric = "https://fabric.test"
		f := fetched(t, charter)
		switch r.URL.Path {
		case "/cartridges/" + CharterFile:
			_, _ = w.Write(f.Body)
		case "/cartridges/" + CharterSigFile:
			_, _ = w.Write(f.Signature)
		case fmt.Sprintf("/cartridges/catalogue/r%d.json", CartridgeRegistryVersion):
			manifest := makeTestRegistry("foo", buildRegistryEntryNamed("Foo", "1.0.0", "foo-1.0.0.pkg", buildIdentityCapGroup()))
			manifest.RegistryURL = anchor
			edit(anchor, &manifest)
			body, err := json.Marshal(manifest)
			require.NoError(t, err)
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	anchor = server.URL + "/cartridges"
	return server, anchor
}

// TEST13005: a cartridge registry is read through its charter. The manifest
// is fetched from the location the charter names, and the charter is kept by
// anchor. An anchor with no charter is no registry; a manifest that names
// another registry, or another fabric than its charter, is refused — nothing
// of it is ingested.
func Test13005_a_cartridge_registry_is_read_through_its_charter(t *testing.T) {
	server, anchor := chartedRegistryServer(t, func(string, *CartridgeRegistry) {})
	repo := NewCartridgeRepo(3600)
	repo.httpClient = server.Client()
	response, err := repo.fetchRegistry(anchor)
	require.NoError(t, err)
	require.Len(t, response.Cartridges, 1, "the manifest at the charter's location was read")
	assert.Equal(t, "foo", response.Cartridges[0].Id)
	charter, ok := repo.Charter(anchor)
	require.True(t, ok, "the charter is kept by anchor")
	assert.Equal(t, testCartridgesSlug, charter.Slug)

	_, err = repo.fetchRegistry(server.URL + "/elsewhere")
	var repoErr *CartridgeRepoError
	require.True(t, errors.As(err, &repoErr), "%v", err)
	assert.Equal(t, "Unchartered", repoErr.Kind, "%v", err)

	for name, edit := range map[string]func(anchor string, m *CartridgeRegistry){
		"another registry's anchor": func(anchor string, m *CartridgeRegistry) { m.RegistryURL = anchor + "-other" },
		"another fabric":            func(_ string, m *CartridgeRegistry) { m.FabricRegistryURL = "https://fabric.other" },
	} {
		server, anchor := chartedRegistryServer(t, edit)
		repo := NewCartridgeRepo(3600)
		repo.httpClient = server.Client()
		_, err := repo.fetchRegistry(anchor)
		require.True(t, errors.As(err, &repoErr), "%s: %v", name, err)
		assert.Equal(t, "ParseError", repoErr.Kind, "%s: %v", name, err)
	}
}

// TEST13010: a slug is one registry's. A second anchor whose charter names a
// slug another registry already holds is refused at its charter and is never
// given that registry's folder; the registry that holds the slug syncs again
// under its own charter.
func Test13010_a_slug_is_one_registrys(t *testing.T) {
	const first = "https://first.example/cartridges"
	const second = "https://second.example/cartridges"
	repo := NewCartridgeRepo(3600)
	require.NoError(t, repo.admitCharter(first, cartridgesCharter(first, "shared")))

	err := repo.admitCharter(second, cartridgesCharter(second, "shared"))
	var repoErr *CartridgeRepoError
	require.True(t, errors.As(err, &repoErr), "a second registry with a held slug is refused: %v", err)
	assert.Equal(t, "CharterRefused", repoErr.Kind)
	assert.Contains(t, repoErr.Message, first, "the refusal names the holder")
	_, held := repo.Charter(second)
	assert.False(t, held, "the refused charter is not held")

	require.NoError(t, repo.admitCharter(first, cartridgesCharter(first, "shared")), "the holder syncs again")
	require.NoError(t, repo.admitCharter(second, cartridgesCharter(second, "second")), "a slug of its own is admitted")
	charter, _ := repo.Charter(first)
	assert.Equal(t, "shared", charter.Slug)
}
