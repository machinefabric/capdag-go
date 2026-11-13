package input_resolver

// The file media types a file's extension names, from the whole manifest, and
// what a file is.
//
// A registry fixture serves, at its `definition` location, every media
// definition of fabric manifest 13 that carries the `ext=` file-type tag, and
// the JSON records that list an extension without one, as published
// (testdata/fabric13_file_media.json). The registry's manifest names them all;
// what its cache holds is up to each test.

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

//go:embed testdata/fabric13_file_media.json
var fileMediaFixture []byte

const fileMediaManifestVersion = 13

// fixtureDefinition is one definition of the fixture: as parsed, and its body
// as published.
type fixtureDefinition struct {
	def  media.StoredMediaDef
	body json.RawMessage
}

func fixtureDefinitions(t *testing.T) map[string]fixtureDefinition {
	t.Helper()
	var fixture struct {
		Definitions map[string]json.RawMessage `json:"definitions"`
	}
	require.NoError(t, json.Unmarshal(fileMediaFixture, &fixture), "the fixture parses")
	defs := make(map[string]fixtureDefinition, len(fixture.Definitions))
	for key, body := range fixture.Definitions {
		var def media.StoredMediaDef
		require.NoError(t, json.Unmarshal(body, &def), "the fixture's %s parses", key)
		defs[key] = fixtureDefinition{def: def, body: body}
	}
	return defs
}

func isFileType(t *testing.T, mediaURN string) bool {
	t.Helper()
	u, err := urn.NewMediaUrnFromString(mediaURN)
	require.NoError(t, err)
	return u.HasTag("ext")
}

// serveFixture is the fixture registry: it serves
// `/media/<sha256 of URN>/<defver>.json` for every definition, 404 for
// anything else, and counts what it served.
func serveFixture(t *testing.T, defs map[string]fixtureDefinition) (string, *atomic.Int64) {
	t.Helper()
	bodies := make(map[string][]byte, len(defs))
	for key, d := range defs {
		bodies[fmt.Sprintf("/media/%x/%d.json", sha256.Sum256([]byte(key)), d.def.Version)] = d.body
	}
	served := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, ok := bodies[req.URL.Path]
		if !ok {
			http.NotFound(w, req)
			return
		}
		served.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server.URL, served
}

// fixtureRegistry is a registry pinned at manifest 13 whose manifest names
// every fixture definition, fetching from anchor; the definitions in cached
// are in its cache already.
func fixtureRegistry(t *testing.T, anchor string, cached ...string) *fabric.FabricRegistry {
	t.Helper()
	registry := fabric.NewForTestWithConfigAndVersion(fabric.DefaultConfig().WithAnchor(anchor), fileMediaManifestVersion)
	for key, d := range fixtureDefinitions(t) {
		if slices.Contains(cached, key) {
			registry.AddSpec(d.def)
		} else {
			registry.PinMediaDefForTest(key, d.def.Version)
		}
	}
	return registry
}

func fileMediaURNs(t *testing.T, registry *fabric.FabricRegistry, extension string) []string {
	t.Helper()
	defs, err := registry.FileMediaForExtension(extension)
	require.NoError(t, err, ".%s", extension)
	urns := make([]string, 0, len(defs))
	for _, def := range defs {
		urns = append(urns, def.Urn)
	}
	return urns
}

// TEST7177: on a cold cache, a file's extension names every file media type
// the manifest declares for it, fetched from the registry: .csv and .yaml are
// known although nothing has cached them, and the content-only JSON records a
// machine has cached (a model download result) are no file types
func Test7177_ExtensionsNameTheManifestsFileTypesOnAColdCache(t *testing.T) {
	defs := fixtureDefinitions(t)
	anchor, served := serveFixture(t, defs)
	registry := fixtureRegistry(t, anchor, "media:download-result;enc=utf8;fmt=json;record")

	assert.Equal(t, []string{"media:enc=utf8;ext=csv;fmt=csv;list;record"}, fileMediaURNs(t, registry, "csv"))
	yaml := []string{
		"media:enc=utf8;ext=yaml;fmt=yaml",
		"media:enc=utf8;ext=yaml;fmt=yaml;list",
		"media:enc=utf8;ext=yaml;fmt=yaml;list;record",
		"media:enc=utf8;ext=yaml;fmt=yaml;record",
	}
	assert.Equal(t, yaml, fileMediaURNs(t, registry, "yaml"))
	assert.Equal(t, yaml, fileMediaURNs(t, registry, "YML"))
	assert.Equal(t, []string{
		"media:enc=utf8;ext=json;fmt=json",
		"media:enc=utf8;ext=json;fmt=json;list",
		"media:enc=utf8;ext=json;fmt=json;list;record",
		"media:enc=utf8;ext=json;fmt=json;record",
	}, fileMediaURNs(t, registry, "json"))
	assert.Equal(t, []string{
		"media:digitized;enc=utf8;ext=txt;plain-text",
		"media:enc=utf8;ext=requirements",
		"media:enc=utf8;ext=txt;image-description;plain-text",
		"media:enc=utf8;ext=txt;page;plain-text",
		"media:enc=utf8;ext=txt;plain-text",
	}, fileMediaURNs(t, registry, "txt"))

	fileTypes := 0
	for key := range defs {
		if isFileType(t, key) {
			fileTypes++
		}
	}
	assert.Equal(t, 156, fileTypes, "manifest 13 has 156 file media types")
	assert.Equal(t, int64(fileTypes), served.Load(), "every file media type is fetched once, and nothing else")
}

// TEST7178: the file media types load only when a file's type is first asked,
// once: making the registry and reading other definitions fetches none of
// them, and a second question fetches nothing more
func Test7178_FileMediaTypesLoadLazilyAndOnce(t *testing.T) {
	anchor, served := serveFixture(t, fixtureDefinitions(t))
	registry := fixtureRegistry(t, anchor)

	_, err := registry.GetMediaDef("media:enc=utf8;fmt=json;model-list;record")
	require.NoError(t, err)
	assert.Equal(t, int64(1), served.Load(), "only the definition asked for is fetched")

	fileMediaURNs(t, registry, "pdf")
	afterFirst := served.Load()
	assert.Equal(t, int64(1+156), afterFirst)
	fileMediaURNs(t, registry, "csv")
	fileMediaURNs(t, registry, "md")
	assert.Equal(t, afterFirst, served.Load(), "the index is loaded once")
}

// TEST7179: file media types already cached are not fetched again
func Test7179_CachedFileMediaTypesAreNotFetched(t *testing.T) {
	defs := fixtureDefinitions(t)
	anchor, served := serveFixture(t, defs)
	all := make([]string, 0, len(defs))
	for key := range defs {
		all = append(all, key)
	}
	registry := fixtureRegistry(t, anchor, all...)

	assert.Equal(t, []string{"media:enc=utf8;ext=csv;fmt=csv;list;record"}, fileMediaURNs(t, registry, "csv"))
	assert.Equal(t, int64(0), served.Load())
}

// TEST7180: offline with a cold cache, a file's type cannot be told: the
// question fails, naming every file media type it could not load and why, and
// never answers from the part of the fabric that is cached
func Test7180_OfflineOnAColdCacheFailsNamingWhatItCouldNotLoad(t *testing.T) {
	anchor, served := serveFixture(t, fixtureDefinitions(t))
	registry := fixtureRegistry(t, anchor, "media:enc=utf8;ext=txt;plain-text", "media:document;ext=pdf")
	registry.SetOffline(true)

	_, err := registry.FileMediaForExtension("txt")
	require.Error(t, err)
	var registryErr *fabric.Error
	require.True(t, errors.As(err, &registryErr), "%v", err)
	assert.Equal(t, fabric.ErrFileTypes, registryErr.Kind, "%v", err)
	message := err.Error()
	assert.True(t, strings.HasPrefix(message, "154 of the 156 file media types of manifest v13 could not be loaded"), message)
	assert.Contains(t, message, "media:enc=utf8;ext=csv;fmt=csv;list;record: network access blocked while offline")
	assert.NotContains(t, message, "media:enc=utf8;ext=txt;plain-text:", "a cached type is not reported")
	assert.Equal(t, int64(0), served.Load())

	registry.SetOffline(false)
	assert.Equal(t, []string{"media:enc=utf8;ext=csv;fmt=csv;list;record"}, fileMediaURNs(t, registry, "csv"))
}

// ---------------------------------------------------------------------------
// What a file is
// ---------------------------------------------------------------------------

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

// warmMachineRegistry is a registry over the fixture whose cache holds what a
// machine that has run a model download and an OCR cap holds.
func warmMachineRegistry(t *testing.T) *fabric.FabricRegistry {
	t.Helper()
	anchor, _ := serveFixture(t, fixtureDefinitions(t))
	return fixtureRegistry(t, anchor,
		"media:download-result;enc=utf8;fmt=json;record",
		"media:digitized;enc=utf8;ext=txt;plain-text",
	)
}

func detectWithoutAdapters(path string, registry *fabric.FabricRegistry) (*FileMedia, error) {
	return DetectFileMedia(context.Background(), path, registry, NoAdapters{}, NoInvoker{}, NewValueAdapterRegistry())
}

// noAdapterTable is one file of each common kind, its content, and the media a
// host with no cartridge to ask takes it as.
var noAdapterTable = []struct{ name, content, expected string }{
	{"notes.txt", "A line of text.\n", "media:enc=utf8;ext=txt;plain-text"},
	{"people.csv", "name,age\nann,30\n", "media:enc=utf8;ext=csv;fmt=csv;list;record"},
	{"rows.tsv", "name\tage\nann\t30\n", "media:enc=utf8;ext=tsv;fmt=tsv;list;record"},
	{"config.yaml", "name: ann\nage: 30\n", "media:enc=utf8;ext=yaml;fmt=yaml"},
	{"config.yml", "name: ann\n", "media:enc=utf8;ext=yaml;fmt=yaml"},
	{"a.json", "{\"name\": \"ann\"}", "media:enc=utf8;ext=json;fmt=json"},
	{"events.ndjson", "{\"a\":1}\n{\"a\":2}\n", "media:enc=utf8;ext=ndjson;fmt=ndjson"},
	{"events.jsonl", "{\"a\":1}\n", "media:enc=utf8;ext=ndjson;fmt=ndjson"},
	{"data.xml", "<a><b/></a>", "media:enc=utf8;ext=xml;fmt=xml"},
	{"config.toml", "a = 1\n", "media:enc=utf8;ext=toml"},
	{"README.md", "# Title\n", "media:enc=utf8;ext=md;plain-text"},
	{"run.log", "started\n", "media:enc=utf8;ext=log;plain-text"},
	{"page.html", "<html></html>", "media:enc=utf8;ext=html"},
	{"notebook.ipynb", "{\"cells\": []}", "media:enc=utf8;ext=jupyter;record"},
	{"report.pdf", "%PDF-1.4\n", "media:document;ext=pdf"},
	{"letter.docx", "PK", "media:document;ext=docx"},
	{"sheet.xlsx", "PK", "media:document;ext=xlsx"},
	{"photo.png", "\u0089PNG", "media:ext=png;image"},
	{"photo.jpg", "ÿØ", "media:ext=jpeg;image"},
	{"song.mp3", "ID3", "media:audio;ext=mp3"},
	{"clip.mp4", "ftyp", "media:ext=mp4;video"},
	{"bundle.zip", "PK", "media:archive;ext=zip"},
}

// TEST7181: with no cartridge to ask (the capdag CLI, a host before its
// cartridges attach), each common extension is the file type the fabric
// declares for it: a .txt is plain text, not digitized text; a .json is JSON,
// not a model download result; .csv, .tsv, .yaml and .yml are known; a
// refinement nothing proves (a JSON record, digitized text, a filing document)
// is never claimed
func Test7181_CommonExtensionsAreTheirFileTypesWithoutAdapters(t *testing.T) {
	registry := warmMachineRegistry(t)
	dir := t.TempDir()
	var wrong []string
	for _, row := range noAdapterTable {
		path := writeFile(t, dir, row.name, row.content)
		var got string
		found, err := detectWithoutAdapters(path, registry)
		if err != nil {
			got = "error: " + err.Error()
		} else {
			got = found.Primary()
		}
		if got != row.expected {
			wrong = append(wrong, fmt.Sprintf("%s: %s, expected %s", row.name, got, row.expected))
		}
	}
	assert.Empty(t, wrong, "wrongly identified:\n  %s", strings.Join(wrong, "\n  "))

	unknown := writeFile(t, dir, "thing.zzzz", "x")
	_, err := detectWithoutAdapters(unknown, registry)
	var resolverErr *InputResolverError
	require.True(t, errors.As(err, &resolverErr), "%v", err)
	assert.Equal(t, InputErrUnknownExtension, resolverErr.Kind, "%v", err)
	assert.Equal(t, "zzzz", resolverErr.Extension)

	bare := writeFile(t, dir, "Makefile", "all:\n")
	_, err = detectWithoutAdapters(bare, registry)
	require.True(t, errors.As(err, &resolverErr), "%v", err)
	assert.Equal(t, InputErrNoExtension, resolverErr.Kind, "%v", err)
}

// bundledAdapters is what datacartridge and txtcartridge claim the files of
// TEST7182 are (their adapters' answers for that content).
type bundledAdapters struct {
	t *testing.T
}

func (b bundledAdapters) InvokeAdapterSelection(_ context.Context, cartridgeID, filePath string) ([]string, error) {
	name := filepath.Base(filePath)
	switch {
	case cartridgeID == "datacartridge" && name == "record.json":
		return []string{"media:enc=utf8;ext=json;fmt=json;record", "media:enc=utf8;ext=json;fmt=json"}, nil
	case cartridgeID == "datacartridge" && name == "rows.json":
		return []string{
			"media:enc=utf8;ext=json;fmt=json;list;record",
			"media:enc=utf8;ext=json;fmt=json;list",
			"media:enc=utf8;ext=json;fmt=json",
		}, nil
	case cartridgeID == "datacartridge" && name == "people.csv":
		return []string{"media:enc=utf8;ext=csv;fmt=csv;list;record"}, nil
	case cartridgeID == "txtcartridge" && name == "notes.txt":
		return []string{"media:enc=utf8;ext=txt;plain-text"}, nil
	}
	b.t.Errorf("%s was not expected to be asked about %s", cartridgeID, name)
	return nil, fmt.Errorf("%s was not expected to be asked about %s", cartridgeID, name)
}

// bundledAdapterRegistry is the adapter URNs datacartridge and txtcartridge
// declare.
func bundledAdapterRegistry(t *testing.T, registry *fabric.FabricRegistry) *MediaAdapterRegistry {
	t.Helper()
	adapters := NewMediaAdapterRegistry(registry)
	require.NoError(t, adapters.RegisterCapGroup("data-formats", []string{
		"media:enc=utf8;fmt=json",
		"media:enc=utf8;fmt=ndjson",
		"media:enc=utf8;fmt=csv",
		"media:ext=tsv",
		"media:ext=psv",
		"media:enc=utf8;fmt=yaml",
		"media:ext=xml",
		"media:ext=toml",
	}, "datacartridge"))
	require.NoError(t, adapters.RegisterCapGroup("text-formats", []string{"media:plain-text", "media:ext=html"}, "txtcartridge"))
	return adapters
}

// TEST7182: with the bundled cartridges' adapters (a desktop client), what a
// cartridge claims proves a refinement: a JSON object is a JSON record, an
// array of objects a list of records, and a .txt the cartridge reads as plain
// text is plain text, never digitized text
func Test7182_CartridgeClaimsProveRefinements(t *testing.T) {
	registry := warmMachineRegistry(t)
	adapters := bundledAdapterRegistry(t, registry)
	dir := t.TempDir()
	detect := func(path string) *FileMedia {
		found, err := DetectFileMedia(context.Background(), path, registry, adapters, bundledAdapters{t: t}, NewValueAdapterRegistry())
		require.NoError(t, err, path)
		return found
	}

	found := detect(writeFile(t, dir, "record.json", "{\"name\": \"ann\"}"))
	assert.Equal(t, []string{"media:enc=utf8;ext=json;fmt=json;record"}, found.Media)

	found = detect(writeFile(t, dir, "rows.json", "[{\"name\": \"ann\"}]"))
	assert.Equal(t, []string{"media:enc=utf8;ext=json;fmt=json;list;record"}, found.Media)

	found = detect(writeFile(t, dir, "people.csv", "name\nann\n"))
	assert.Equal(t, []string{"media:enc=utf8;ext=csv;fmt=csv;list;record"}, found.Media)

	// pip's requirements files claim .txt too, and no cartridge speaks for
	// them, so a .txt is offered as either; a host that cannot ask takes the
	// plain text, the more specific.
	found = detect(writeFile(t, dir, "notes.txt", "A line of text.\n"))
	assert.Equal(t, []string{"media:enc=utf8;ext=requirements", "media:enc=utf8;ext=txt;plain-text"}, found.Media)
	assert.Equal(t, "media:enc=utf8;ext=txt;plain-text", found.Primary())
}

// TEST7183: what a file is from its name alone is its extension's base types,
// never a refinement
func Test7183_AFilesMediaByNameAloneIsItsExtensionsBaseTypes(t *testing.T) {
	registry := warmMachineRegistry(t)

	jsonMedia, err := ExtensionMedia("json", registry)
	require.NoError(t, err)
	assert.Equal(t, []string{"media:enc=utf8;ext=json;fmt=json"}, jsonMedia)

	txt, err := ExtensionMedia("TXT", registry)
	require.NoError(t, err)
	assert.Equal(t, []string{"media:enc=utf8;ext=requirements", "media:enc=utf8;ext=txt;plain-text"}, txt)
	primary, ok, err := PrimaryMedia(txt)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "media:enc=utf8;ext=txt;plain-text", primary)

	_, err = ExtensionMedia("zzzz", registry)
	var resolverErr *InputResolverError
	require.True(t, errors.As(err, &resolverErr), "%v", err)
	assert.Equal(t, InputErrUnknownExtension, resolverErr.Kind, "%v", err)
}

// TEST7184: a refinement whose declared validation rules the content meets
// stands without a cartridge; one whose rules it fails falls; a definition
// with an invalid pattern is an error naming it
func Test7184_ValidationRulesProveOrDisproveARefinement(t *testing.T) {
	definition := func(body string) media.StoredMediaDef {
		var def media.StoredMediaDef
		require.NoError(t, json.Unmarshal([]byte(body), &def))
		return def
	}
	script := func(mediaURN, pattern string) media.StoredMediaDef {
		body, err := json.Marshal(map[string]any{
			"urn": mediaURN, "version": 1, "media_type": "text/plain", "title": "t", "extensions": []string{"txt"},
			"validation": map[string]any{"pattern": pattern},
		})
		require.NoError(t, err)
		return definition(string(body))
	}
	base := definition(`{"urn": "media:enc=utf8;ext=txt;plain-text", "version": 1, "media_type": "text/plain",
		"title": "Plain Text", "extensions": ["txt"]}`)

	registry := fabric.NewForTest()
	registry.AddSpec(base)
	registry.AddSpec(script("media:enc=utf8;ext=txt;plain-text;shebang", "^#!"))
	dir := t.TempDir()
	mediaOf := func(name, content string) []string {
		found, err := detectWithoutAdapters(writeFile(t, dir, name, content), registry)
		require.NoError(t, err, name)
		return found.Media
	}
	assert.Equal(t, []string{"media:enc=utf8;ext=txt;plain-text;shebang"}, mediaOf("run.txt", "#!/bin/sh\n"))
	assert.Equal(t, []string{"media:enc=utf8;ext=txt;plain-text"}, mediaOf("prose.txt", "hello\n"))

	broken := fabric.NewForTest()
	broken.AddSpec(script("media:broken;enc=utf8;ext=txt;plain-text", "(("))
	_, err := detectWithoutAdapters(writeFile(t, dir, "x.txt", "x"), broken)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "media:broken;enc=utf8;ext=txt;plain-text")
}
