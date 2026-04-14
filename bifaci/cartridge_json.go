// Package bifaci provides install-context metadata for installed cartridges.
//
// Every installed cartridge version directory contains a cartridge.json file
// that records how the cartridge was installed and where its entry point is.
//
// Layout:
//
//	cartridges/{name}/{version}/
//	  cartridge.json       ← this file
//	  <entry_point_binary>
//	  <supporting_files>
package bifaci

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// CartridgeInstallSource describes how a cartridge was installed.
type CartridgeInstallSource string

const (
	CartridgeInstallSourceRegistry CartridgeInstallSource = "registry"
	CartridgeInstallSourceDev      CartridgeInstallSource = "dev"
	CartridgeInstallSourceBundle   CartridgeInstallSource = "bundle"
)

// CartridgeJson holds install-context metadata stored in cartridge.json inside
// each cartridge version directory.
type CartridgeJson struct {
	// Name is the cartridge name (e.g., "pdfcartridge").
	Name string `json:"name"`
	// Version is the version string (e.g., "0.168.411").
	Version string `json:"version"`
	// Entry is the relative path from the version directory to the executable entry point.
	// For single-binary cartridges this is just the binary filename.
	// For directory cartridges it may be a nested path.
	Entry string `json:"entry"`
	// InstalledAt is the RFC3339 timestamp of when the cartridge was installed.
	InstalledAt string `json:"installed_at"`
	// InstalledFrom describes how the cartridge was installed.
	InstalledFrom CartridgeInstallSource `json:"installed_from"`
	// SourceURL is the URL the package was downloaded from (empty for dev/bundle installs).
	SourceURL string `json:"source_url,omitempty"`
	// PackageSha256 is the SHA256 hash of the original package (tarball or binary).
	PackageSha256 string `json:"package_sha256,omitempty"`
	// PackageSize is the size in bytes of the original package.
	PackageSize uint64 `json:"package_size,omitempty"`
}

// CartridgeJsonError is returned when reading or validating a cartridge.json fails.
type CartridgeJsonError struct {
	Kind    CartridgeJsonErrorKind
	Path    string
	Entry   string
	Message string
	Err     error
}

// CartridgeJsonErrorKind categorises cartridge.json errors.
type CartridgeJsonErrorKind int

const (
	CartridgeJsonErrorNotFound CartridgeJsonErrorKind = iota
	CartridgeJsonErrorReadFailed
	CartridgeJsonErrorInvalidJson
	CartridgeJsonErrorEntryPointMissing
	CartridgeJsonErrorEntryPointNotExecutable
	CartridgeJsonErrorEntryPathEscape
	CartridgeJsonErrorWriteFailed
)

func (e *CartridgeJsonError) Error() string {
	switch e.Kind {
	case CartridgeJsonErrorNotFound:
		return fmt.Sprintf("cartridge.json not found at %s", e.Path)
	case CartridgeJsonErrorReadFailed:
		return fmt.Sprintf("failed to read cartridge.json at %s: %v", e.Path, e.Err)
	case CartridgeJsonErrorInvalidJson:
		return fmt.Sprintf("invalid cartridge.json at %s: %v", e.Path, e.Err)
	case CartridgeJsonErrorEntryPointMissing:
		return fmt.Sprintf("cartridge.json at %s: entry point '%s' does not exist", e.Path, e.Entry)
	case CartridgeJsonErrorEntryPointNotExecutable:
		return fmt.Sprintf("cartridge.json at %s: entry point '%s' is not executable", e.Path, e.Entry)
	case CartridgeJsonErrorEntryPathEscape:
		return fmt.Sprintf("cartridge.json at %s: entry path '%s' escapes version directory", e.Path, e.Entry)
	case CartridgeJsonErrorWriteFailed:
		return fmt.Sprintf("failed to write cartridge.json at %s: %v", e.Path, e.Err)
	default:
		return fmt.Sprintf("cartridge.json error at %s: %s", e.Path, e.Message)
	}
}

func (e *CartridgeJsonError) Unwrap() error {
	return e.Err
}

// ReadCartridgeJsonFromDir reads and validates a cartridge.json from a version directory.
//
// Validates:
//   - File exists and is valid JSON
//   - Entry point path does not escape the version directory
//   - Entry point binary exists and is executable
func ReadCartridgeJsonFromDir(versionDir string) (*CartridgeJson, error) {
	jsonPath := filepath.Join(versionDir, "cartridge.json")

	if _, err := os.Stat(jsonPath); errors.Is(err, os.ErrNotExist) {
		return nil, &CartridgeJsonError{Kind: CartridgeJsonErrorNotFound, Path: jsonPath}
	}

	contents, err := os.ReadFile(jsonPath)
	if err != nil {
		return nil, &CartridgeJsonError{
			Kind: CartridgeJsonErrorReadFailed,
			Path: jsonPath,
			Err:  err,
		}
	}

	var cj CartridgeJson
	if err := json.Unmarshal(contents, &cj); err != nil {
		return nil, &CartridgeJsonError{
			Kind: CartridgeJsonErrorInvalidJson,
			Path: jsonPath,
			Err:  err,
		}
	}

	// Validate entry path does not escape version directory
	entryPath := filepath.Join(versionDir, cj.Entry)
	canonicalDir, err := filepath.EvalSymlinks(versionDir)
	if err != nil {
		canonicalDir = versionDir
	}
	canonicalEntry, err := filepath.EvalSymlinks(entryPath)
	if err != nil {
		canonicalEntry = entryPath
	}

	if !strings.HasPrefix(canonicalEntry, canonicalDir+string(filepath.Separator)) &&
		canonicalEntry != canonicalDir {
		return nil, &CartridgeJsonError{
			Kind:  CartridgeJsonErrorEntryPathEscape,
			Path:  jsonPath,
			Entry: cj.Entry,
		}
	}

	// Validate entry point exists
	info, err := os.Stat(entryPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &CartridgeJsonError{
				Kind:  CartridgeJsonErrorEntryPointMissing,
				Path:  jsonPath,
				Entry: cj.Entry,
			}
		}
		return nil, &CartridgeJsonError{
			Kind: CartridgeJsonErrorReadFailed,
			Path: jsonPath,
			Err:  err,
		}
	}

	// Validate entry point is executable (Unix)
	if info.Mode()&0o111 == 0 {
		return nil, &CartridgeJsonError{
			Kind:  CartridgeJsonErrorEntryPointNotExecutable,
			Path:  jsonPath,
			Entry: cj.Entry,
		}
	}

	return &cj, nil
}

// ResolveEntryPoint returns the absolute path to the entry point binary.
func (c *CartridgeJson) ResolveEntryPoint(versionDir string) string {
	return filepath.Join(versionDir, c.Entry)
}

// WriteToDir writes this cartridge.json to a version directory.
func (c *CartridgeJson) WriteToDir(versionDir string) error {
	jsonPath := filepath.Join(versionDir, "cartridge.json")
	contents, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		panic(fmt.Sprintf("CartridgeJson serialization cannot fail: %v", err))
	}
	if err := os.WriteFile(jsonPath, contents, 0o644); err != nil {
		return &CartridgeJsonError{
			Kind: CartridgeJsonErrorWriteFailed,
			Path: jsonPath,
			Err:  err,
		}
	}
	return nil
}

// HashCartridgeDirectory computes a deterministic SHA256 hash of a directory tree.
//
// Walks all files in the directory recursively, sorts them by relative path,
// then hashes each file's relative path (UTF-8 bytes) followed by its contents.
// This produces a stable identity hash regardless of filesystem ordering.
//
// Symbolic links are followed (their targets are hashed, not the links).
// cartridge.json itself is excluded from the hash — it contains install-time
// metadata (like installed_at) that changes between installs of the same content.
func HashCartridgeDirectory(dir string) (string, error) {
	type fileEntry struct {
		relPath string
		absPath string
	}

	var files []fileEntry

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}

		// Exclude cartridge.json from identity hash — it contains
		// install-time metadata that varies between installs of identical content.
		if rel == "cartridge.json" {
			return nil
		}

		files = append(files, fileEntry{relPath: rel, absPath: path})
		return nil
	})
	if err != nil {
		return "", err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].relPath < files[j].relPath
	})

	h := sha256.New()
	for _, f := range files {
		h.Write([]byte(f.relPath))
		contents, err := os.ReadFile(f.absPath)
		if err != nil {
			return "", err
		}
		h.Write(contents)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
