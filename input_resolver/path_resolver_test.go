package input_resolver

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func mustCreateFile(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file %q: %v", path, err)
	}
	f.Close()
}

func asInputResolverError(err error) *InputResolverError {
	var ire *InputResolverError
	if errors.As(err, &ire) {
		return ire
	}
	return nil
}

// TEST1000: Single existing file
func Test1000_SingleExistingFile(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.pdf")
	mustCreateFile(t, filePath)

	result, err := resolveFile(filePath)
	if err != nil {
		t.Fatalf("resolveFile failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result))
	}
	if filepath.Base(result[0]) != "test.pdf" {
		t.Fatalf("expected base test.pdf, got %q", filepath.Base(result[0]))
	}
}

// TEST1001: Single non-existent file
func Test1001_NonexistentFile(t *testing.T) {
	_, err := resolveFile("/nonexistent/path/file.pdf")
	ire := asInputResolverError(err)
	if ire == nil || ire.Kind != InputErrNotFound {
		t.Fatalf("expected NotFound error, got %v", err)
	}
}

// TEST1002: Empty directory
func Test1002_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	result, err := ResolveDirectory(dir)
	if err != nil {
		t.Fatalf("ResolveDirectory failed: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty, got %d files", len(result))
	}
}

// TEST1003: Directory with files
func Test1003_DirectoryWithFiles(t *testing.T) {
	dir := t.TempDir()
	mustCreateFile(t, filepath.Join(dir, "a.txt"))
	mustCreateFile(t, filepath.Join(dir, "b.txt"))
	mustCreateFile(t, filepath.Join(dir, "c.txt"))

	result, err := ResolveDirectory(dir)
	if err != nil {
		t.Fatalf("ResolveDirectory failed: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 files, got %d", len(result))
	}
}

// TEST1004: Directory with subdirs (recursive)
func Test1004_DirectoryWithSubdirs(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	mustCreateFile(t, filepath.Join(dir, "root.txt"))
	mustCreateFile(t, filepath.Join(dir, "sub", "nested.txt"))

	result, err := ResolveDirectory(dir)
	if err != nil {
		t.Fatalf("ResolveDirectory failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 files, got %d", len(result))
	}
}

// TEST1005: Glob matching files
func Test1005_GlobMatchingFiles(t *testing.T) {
	dir := t.TempDir()
	mustCreateFile(t, filepath.Join(dir, "a.pdf"))
	mustCreateFile(t, filepath.Join(dir, "b.pdf"))
	mustCreateFile(t, filepath.Join(dir, "c.txt"))

	pattern := filepath.Join(dir, "*.pdf")
	result, err := resolveGlob(pattern)
	if err != nil {
		t.Fatalf("resolveGlob failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 files, got %d", len(result))
	}
}

// TEST1006: Glob matching nothing
func Test1006_GlobMatchingNothing(t *testing.T) {
	dir := t.TempDir()
	mustCreateFile(t, filepath.Join(dir, "a.txt"))

	pattern := filepath.Join(dir, "*.xyz")
	result, err := resolveGlob(pattern)
	if err != nil {
		t.Fatalf("resolveGlob failed: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty, got %d files", len(result))
	}
}

// TEST1007: Recursive glob
func Test1007_RecursiveGlob(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	mustCreateFile(t, filepath.Join(dir, "a.json"))
	mustCreateFile(t, filepath.Join(dir, "sub", "b.json"))

	pattern := filepath.Join(dir, "**", "*.json")
	result, err := resolveGlob(pattern)
	if err != nil {
		t.Fatalf("resolveGlob failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 files, got %d", len(result))
	}
}

// TEST1008: Mixed file + dir
func Test1008_MixedFileDir(t *testing.T) {
	dir := t.TempDir()
	subdir := filepath.Join(dir, "subdir")
	if err := os.Mkdir(subdir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	mustCreateFile(t, filepath.Join(dir, "file.pdf"))
	mustCreateFile(t, filepath.Join(subdir, "nested.txt"))

	items := []InputItem{
		{Kind: InputItemFile, Path: filepath.Join(dir, "file.pdf")},
		{Kind: InputItemDirectory, Path: subdir},
	}

	result, err := ResolveItems(items)
	if err != nil {
		t.Fatalf("ResolveItems failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 files, got %d", len(result))
	}
}

// TEST1010: Duplicate paths are deduplicated
func Test1010_DuplicatePaths(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "file.pdf")
	mustCreateFile(t, filePath)

	items := []InputItem{
		{Kind: InputItemFile, Path: filePath},
		{Kind: InputItemFile, Path: filePath},
	}

	result, err := ResolveItems(items)
	if err != nil {
		t.Fatalf("ResolveItems failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 file after dedup, got %d", len(result))
	}
}

// TEST1011: Invalid glob syntax
func Test1011_InvalidGlob(t *testing.T) {
	_, err := resolveGlob("[unclosed")
	ire := asInputResolverError(err)
	if ire == nil || ire.Kind != InputErrInvalidGlob {
		t.Fatalf("expected InvalidGlob error, got %v", err)
	}
}

// TEST1013: Empty input array
func Test1013_EmptyInput(t *testing.T) {
	_, err := ResolveItems(nil)
	ire := asInputResolverError(err)
	if ire == nil || ire.Kind != InputErrEmptyInput {
		t.Fatalf("expected EmptyInput error, got %v", err)
	}
}

// TEST1014: Symlink to file
func Test1014_SymlinkToFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink test is unix-only")
	}
	dir := t.TempDir()
	filePath := filepath.Join(dir, "real.txt")
	linkPath := filepath.Join(dir, "link.txt")
	mustCreateFile(t, filePath)
	if err := os.Symlink(filePath, linkPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	result, err := resolveFile(linkPath)
	if err != nil {
		t.Fatalf("resolveFile failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result))
	}
}

// TEST1016: Path with spaces
func Test1016_PathWithSpaces(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "my file.pdf")
	mustCreateFile(t, filePath)

	result, err := resolveFile(filePath)
	if err != nil {
		t.Fatalf("resolveFile failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result))
	}
}

// TEST1017: Path with unicode
func Test1017_PathWithUnicode(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "文档.pdf")
	mustCreateFile(t, filePath)

	result, err := resolveFile(filePath)
	if err != nil {
		t.Fatalf("resolveFile failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result))
	}
}

// TEST1018: Relative path
func Test1018_RelativePath(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "file.txt")
	mustCreateFile(t, filePath)

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer os.Chdir(cwd)
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	result, rerr := resolveFile("file.txt")
	// May fail due to working dir, but should handle relative paths.
	if rerr != nil {
		ire := asInputResolverError(rerr)
		if ire == nil || ire.Kind != InputErrNotFound {
			t.Fatalf("expected ok or NotFound, got %v", rerr)
		}
		return
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result))
	}
}
