package input_resolver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

func strPtr(s string) *string { return &s }

// createTestFabricRegistry builds a FabricRegistry pre-seeded with the media
// defs the resolver tests reference (pdf, txt, json, model-spec). The registry
// hydrates extension lookups from spec arrival, so tests must seed the specs
// they exercise explicitly.
func createTestFabricRegistry(t *testing.T) *media.FabricRegistry {
	t.Helper()
	registry, err := media.NewFabricRegistryForTest()
	if err != nil {
		t.Fatalf("NewFabricRegistryForTest: %v", err)
	}

	// PDF.
	registry.AddSpec(media.StoredMediaDef{
		Version:    0,
		Urn:        "media:ext=pdf",
		MediaType:  "application/pdf",
		Title:      "PDF",
		Extensions: []string{"pdf"},
	})

	// JSON family.
	registry.AddSpec(media.StoredMediaDef{
		Version:    0,
		Urn:        "media:fmt=json;record",
		MediaType:  "application/json",
		Title:      "JSON",
		Extensions: []string{"json"},
	})

	// Plain text.
	registry.AddSpec(media.StoredMediaDef{
		Version:    0,
		Urn:        "media:enc=utf-8;ext=txt;list",
		MediaType:  "text/plain",
		Title:      "Text",
		Extensions: []string{"txt"},
	})

	// Model-spec is a value-type URN with no file extension. The validation
	// pattern matches the canonical scheme:rest shape so plain prose is
	// filtered out.
	registry.AddSpec(media.StoredMediaDef{
		Version:   0,
		Urn:       "media:enc=utf-8;model-spec",
		MediaType: "text/plain",
		Title:     "Model spec",
		Validation: &media.MediaValidation{
			Pattern: strPtr(`^[A-Za-z0-9._-]+:\S+$`),
		},
	})

	return registry
}

// mockInvoker returns predefined media URNs for any cartridge.
type mockInvoker struct {
	response []string // nil ⇒ empty END (no match)
}

func (m *mockInvoker) InvokeAdapterSelection(_ context.Context, _ string, _ string) ([]string, error) {
	return m.response, nil
}

// TEST1090: 1 file → is_sequence=false
func Test1090_SingleFileScalar(t *testing.T) {
	registry := createTestFabricRegistry(t)
	dir := t.TempDir()
	path := createFile(t, dir, "doc.pdf", []byte("%PDF-1.4"))

	result, err := ResolvePaths([]string{path}, registry)
	if err != nil {
		t.Fatalf("ResolvePaths failed: %v", err)
	}
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

	result, err := ResolvePaths([]string{path1, path2}, registry)
	if err != nil {
		t.Fatalf("ResolvePaths failed: %v", err)
	}
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

	result, err := ResolvePaths([]string{dir}, registry)
	if err != nil {
		t.Fatalf("ResolvePaths failed: %v", err)
	}
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

	result, err := ResolvePaths([]string{dir}, registry)
	if err != nil {
		t.Fatalf("ResolvePaths failed: %v", err)
	}
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

	result, err := ResolvePaths([]string{dir}, registry)
	if err != nil {
		t.Fatalf("ResolvePaths failed: %v", err)
	}
	if len(result.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result.Files))
	}
	if !strings.Contains(result.Files[0].Path, "real.txt") {
		t.Fatalf("expected real.txt, got %q", result.Files[0].Path)
	}
}

// TEST1098: Extension-based detection picks up pdf tag for .pdf files
func Test1098_ExtensionBasedPdf(t *testing.T) {
	registry := createTestFabricRegistry(t)
	dir := t.TempDir()
	path := createFile(t, dir, "doc.pdf", []byte("%PDF-1.4"))

	resolved, err := detectFileByExtensionWithRegistry(path, registry)
	if err != nil {
		t.Fatalf("detect failed: %v", err)
	}
	u, perr := urn.NewMediaUrnFromString(resolved.MediaUrn)
	if perr != nil {
		t.Fatalf("parse URN %q: %v", resolved.MediaUrn, perr)
	}
	if v, ok := u.GetTag("ext"); !ok || v != "pdf" {
		t.Fatalf("PDF extension must produce URN with ext=pdf tag, got: %s", resolved.MediaUrn)
	}
}

// TEST1236: Colon-delimited model spec text survives TXT candidate discrimination.
func Test1236_Disc2ModelSpecValidationPatternFiltersContent(t *testing.T) {
	registry := createTestFabricRegistry(t)
	candidates := []string{"media:enc=utf-8;model-spec"}

	// Spec-shaped content survives the regex filter.
	survivors := DiscriminateCandidatesByValidation(
		[]byte("hf:MaziyarPanahi/Mistral-7B-Instruct-v0.3-GGUF"),
		candidates,
		registry,
		"media:enc=utf-8",
	)
	if !containsStr(survivors, "media:enc=utf-8;model-spec") {
		t.Fatalf("spec-shaped content must survive, got: %v", survivors)
	}

	// Plain prose with internal whitespace is rejected by the same regex.
	survivorsProse := DiscriminateCandidatesByValidation(
		[]byte("this is not a model spec"),
		candidates,
		registry,
		"media:enc=utf-8",
	)
	if containsStr(survivorsProse, "media:enc=utf-8;model-spec") {
		t.Fatalf("prose must NOT survive, got: %v", survivorsProse)
	}
}

// TEST1237: Empty candidates → empty result
func Test1237_Disc5EmptyCandidates(t *testing.T) {
	registry := createTestFabricRegistry(t)
	survivors := DiscriminateCandidatesByValidation([]byte("anything"), nil, registry, "media:")
	if len(survivors) != 0 {
		t.Fatalf("expected empty, got %v", survivors)
	}
}

// TEST1238: Unknown URN survives discrimination
func Test1238_Disc6UnknownUrnSurvives(t *testing.T) {
	registry := createTestFabricRegistry(t)
	candidates := []string{"media:nonexistent;fake"}
	survivors := DiscriminateCandidatesByValidation([]byte("anything"), candidates, registry, "media:")
	if len(survivors) != 1 || survivors[0] != "media:nonexistent;fake" {
		t.Fatalf("Unknown URN should survive — no spec to eliminate it, got: %v", survivors)
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

	if got := structureFromMarkerTags(mustUrn("media:ext=pdf")); got != ScalarOpaque {
		t.Errorf("ext=pdf → %v, want ScalarOpaque", got)
	}
	if got := structureFromMarkerTags(mustUrn("media:fmt=json;record")); got != ScalarRecord {
		t.Errorf("fmt=json;record → %v, want ScalarRecord", got)
	}
	if got := structureFromMarkerTags(mustUrn("media:enc=utf-8;list")); got != ListOpaque {
		t.Errorf("enc=utf-8;list → %v, want ListOpaque", got)
	}
	if got := structureFromMarkerTags(mustUrn("media:fmt=json;list;record")); got != ListRecord {
		t.Errorf("fmt=json;list;record → %v, want ListRecord", got)
	}
}

// TEST1285: detect_file_confirmed fails when no adapters are registered for the extension
func Test1285_ConfirmedNoAdaptersFails(t *testing.T) {
	dir := t.TempDir()
	path := createFile(t, dir, "data.json", []byte(`{"key": "value"}`))

	registry := createTestFabricRegistry(t)
	adapterRegistry := NewMediaAdapterRegistry(registry)
	invoker := &mockInvoker{response: nil}

	_, err := DetectFileConfirmed(context.Background(), path, adapterRegistry, invoker)
	if err == nil {
		t.Fatal("Must fail when no adapters are registered for the extension")
	}
	if !strings.Contains(err.Error(), "No content-inspection adapter") {
		t.Fatalf("Error must mention missing adapter, got: %v", err)
	}
}

// TEST1286: detect_file_confirmed succeeds when adapter returns URNs
func Test1286_ConfirmedAdapterReturnsUrns(t *testing.T) {
	dir := t.TempDir()
	path := createFile(t, dir, "data.json", []byte(`{"key": "value"}`))

	registry := createTestFabricRegistry(t)
	adapterRegistry := NewMediaAdapterRegistry(registry)

	// Register an adapter for media:fmt=json.
	if err := adapterRegistry.RegisterCapGroup("test-group", []string{"media:fmt=json"}, "test-cartridge"); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	invoker := &mockInvoker{response: []string{"media:fmt=json;record"}}

	resolved, err := DetectFileConfirmed(context.Background(), path, adapterRegistry, invoker)
	if err != nil {
		t.Fatalf("Must succeed when adapter returns URNs: %v", err)
	}
	if !strings.Contains(resolved.MediaUrn, "json") {
		t.Fatalf("Resolved URN must contain json, got: %s", resolved.MediaUrn)
	}
	if resolved.ContentStructure != ScalarRecord {
		t.Fatalf("expected ScalarRecord, got %v", resolved.ContentStructure)
	}
}

// TEST1287: detect_file_confirmed fails when all adapters return empty END (no match)
func Test1287_ConfirmedAllAdaptersNoMatch(t *testing.T) {
	dir := t.TempDir()
	path := createFile(t, dir, "data.json", []byte("not json"))

	registry := createTestFabricRegistry(t)
	adapterRegistry := NewMediaAdapterRegistry(registry)

	if err := adapterRegistry.RegisterCapGroup("test-group", []string{"media:fmt=json"}, "test-cartridge"); err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	// Invoker returns nil (empty END — no match).
	invoker := &mockInvoker{response: nil}

	_, err := DetectFileConfirmed(context.Background(), path, adapterRegistry, invoker)
	if err == nil {
		t.Fatal("Must fail when all adapters return no match")
	}
	if !strings.Contains(err.Error(), "returned no match") {
		t.Fatalf("Error must mention no match, got: %v", err)
	}
}

func containsStr(items []string, target string) bool {
	for _, s := range items {
		if s == target {
			return true
		}
	}
	return false
}
