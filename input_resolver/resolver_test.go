package input_resolver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

func createFile(t *testing.T, dir, name string, content []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write file %q: %v", path, err)
	}
	return path
}

// createTestFabricRegistry builds a FabricRegistry whose manifest names the
// file media types the resolver tests reference (pdf, json, txt), each cached.
func createTestFabricRegistry(t *testing.T) *fabric.FabricRegistry {
	t.Helper()
	registry := fabric.NewForTest()

	// PDF.
	registry.AddSpec(media.StoredMediaDef{
		Urn:        "media:document;ext=pdf",
		MediaType:  "application/pdf",
		Title:      "PDF",
		Extensions: []string{"pdf"},
	})

	// JSON family.
	registry.AddSpec(media.StoredMediaDef{
		Urn:        "media:enc=utf8;ext=json;fmt=json;record",
		MediaType:  "application/json",
		Title:      "JSON",
		Extensions: []string{"json"},
	})

	// Plain text.
	registry.AddSpec(media.StoredMediaDef{
		Urn:        "media:enc=utf8;ext=txt;list;plain-text",
		MediaType:  "text/plain",
		Title:      "Text",
		Extensions: []string{"txt"},
	})

	return registry
}

// resolve resolves paths as a host with no cartridges does.
func resolve(t *testing.T, paths []string, registry *fabric.FabricRegistry) *ResolvedInputSet {
	t.Helper()
	detection := &Detection{
		Registry:      registry,
		Adapters:      NoAdapters{},
		Invoker:       NoInvoker{},
		ValueAdapters: NewValueAdapterRegistry(),
	}
	result, err := ResolvePaths(context.Background(), paths, detection)
	if err != nil {
		t.Fatalf("ResolvePaths failed: %v", err)
	}
	return result
}

// TEST1090: 1 file → is_sequence=false
func Test1090_SingleFileScalar(t *testing.T) {
	registry := createTestFabricRegistry(t)
	dir := t.TempDir()
	path := createFile(t, dir, "doc.pdf", []byte("%PDF-1.4"))

	result := resolve(t, []string{path}, registry)
	if len(result.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result.Files))
	}
	if result.IsSequence {
		t.Error("single file must be is_sequence=false")
	}
}

// TEST1092: 2 files → is_sequence=true
func Test1092_TwoFiles(t *testing.T) {
	registry := createTestFabricRegistry(t)
	dir := t.TempDir()
	path1 := createFile(t, dir, "a.pdf", []byte("%PDF-1.4"))
	path2 := createFile(t, dir, "b.pdf", []byte("%PDF-1.5"))

	result := resolve(t, []string{path1, path2}, registry)
	if len(result.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(result.Files))
	}
	if !result.IsSequence {
		t.Error("multiple files must be is_sequence=true")
	}
}

// TEST1093: 1 dir with 1 file → is_sequence=false
func Test1093_DirSingleFile(t *testing.T) {
	registry := createTestFabricRegistry(t)
	dir := t.TempDir()
	createFile(t, dir, "only.pdf", []byte("%PDF-1.4"))

	result := resolve(t, []string{dir}, registry)
	if len(result.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result.Files))
	}
	if result.IsSequence {
		t.Error("directory with single file must be is_sequence=false")
	}
}

// TEST1094: 1 dir with 3 files → is_sequence=true
func Test1094_DirMultipleFiles(t *testing.T) {
	registry := createTestFabricRegistry(t)
	dir := t.TempDir()
	createFile(t, dir, "a.txt", []byte("hello"))
	createFile(t, dir, "b.txt", []byte("world"))
	createFile(t, dir, "c.txt", []byte("test"))

	result := resolve(t, []string{dir}, registry)
	if len(result.Files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(result.Files))
	}
	if !result.IsSequence {
		t.Error("directory with multiple files must be is_sequence=true")
	}
}

// TEST977: OS files excluded in resolve_paths
func Test977_OsFilesExcludedIntegration(t *testing.T) {
	registry := createTestFabricRegistry(t)
	dir := t.TempDir()
	createFile(t, dir, ".DS_Store", []byte(""))
	createFile(t, dir, "real.txt", []byte("content"))

	result := resolve(t, []string{dir}, registry)
	if len(result.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result.Files))
	}
	if !strings.Contains(result.Files[0].Path, "real.txt") {
		t.Fatalf("expected real.txt, got %q", result.Files[0].Path)
	}
}

// TEST1288: structure_from_marker_tags correctly maps tag combinations to ContentStructure
func Test1288_StructureFromMarkerTags(t *testing.T) {
	mustUrn := func(s string) *urn.MediaUrn {
		u, err := urn.NewMediaUrnFromString(s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		return u
	}

	if got := structureFromMarkerTags(mustUrn("media:document;ext=pdf")); got != ScalarOpaque {
		t.Errorf("ext=pdf → %v, want ScalarOpaque", got)
	}
	if got := structureFromMarkerTags(mustUrn("media:enc=utf8;fmt=json;record")); got != ScalarRecord {
		t.Errorf("fmt=json;record → %v, want ScalarRecord", got)
	}
	if got := structureFromMarkerTags(mustUrn("media:enc=utf8;list")); got != ListOpaque {
		t.Errorf("enc=utf8;list → %v, want ListOpaque", got)
	}
	if got := structureFromMarkerTags(mustUrn("media:enc=utf8;fmt=json;list;record")); got != ListRecord {
		t.Errorf("fmt=json;list;record → %v, want ListRecord", got)
	}
}
