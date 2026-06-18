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

// CartridgeInstallSource is opaque optional metadata describing how
// a cartridge was installed. **Not consulted for any host or engine
// routing decision** — kept around so downstream telemetry / audit
// tooling can record install provenance. Within the codebase nothing
// should branch on the variant; the dev-vs-not-dev signal the host
// actually uses is RegistryURL (nil ⇔ dev).
type CartridgeInstallSource string

const (
	CartridgeInstallSourceRegistry     CartridgeInstallSource = "registry"
	CartridgeInstallSourceDev          CartridgeInstallSource = "dev"
	CartridgeInstallSourceBundle       CartridgeInstallSource = "bundle"
	CartridgeInstallSourceAppInstaller CartridgeInstallSource = "app_installer"
)

// CartridgeJson holds install-context metadata stored in cartridge.json inside
// each cartridge version directory.
//
// `(RegistryURL, Channel, Name, Version)` is the install's full
// identity. The installer (.pkg or `dx cartridge --install`) writes
// these fields based on which (registry × channel) the cartridge
// was published / built for. Each (registry, channel) is an
// independent namespace: install of the same id+version from
// different registries or channels are different artifacts that
// coexist on disk under different top-level slug folders.
//
// RegistryURL is `*string` (Go's nullable string) — present-but-nil
// means dev install (cartridge built locally without
// MFR_CARTRIDGE_REGISTRY_URL); present-and-non-nil means a registry URL the
// cartridge was published from. The on-disk top-level folder is
// the SHA-256-prefix slug of the URL (or the literal "dev" when
// nil); the host validates folder ⇔ provenance at scan time.
type CartridgeJson struct {
	// Name is the cartridge name (e.g., "pdfcartridge").
	Name string `json:"name"`
	// Version is the version string (e.g., "0.168.411").
	Version string `json:"version"`
	// Channel is "release" or "nightly". Required.
	Channel string `json:"channel"`
	// RegistryURL is the verbatim URL of the registry the cartridge
	// was published from. Required-but-nullable: present-but-null
	// means dev install. The JSON field is always emitted (never
	// elided for null) so a missing field is a parse error — that's
	// how we surface old-schema cartridge.json files.
	RegistryURL *string `json:"registry_url"`
	// Entry is the relative path from the version directory to the executable entry point.
	// For single-binary cartridges this is just the binary filename.
	// For directory cartridges it may be a nested path.
	Entry string `json:"entry"`
	// InstalledAt is the RFC3339 timestamp of when the cartridge was installed.
	InstalledAt string `json:"installed_at"`
	// InstalledFrom is optional install-provenance metadata. Not
	// consulted for any routing or attachment decision — the
	// dev-vs-not-dev signal the host uses is RegistryURL (nil ⇔
	// dev). Stored as `*CartridgeInstallSource` so an absent field
	// in the JSON parses to nil and a nil value is omitted on
	// serialize. Kept around as opaque metadata for future
	// telemetry / audit tooling.
	InstalledFrom *CartridgeInstallSource `json:"installed_from,omitempty"`
	// SourceURL is the URL the package was downloaded from (empty for dev/bundle installs).
	SourceURL string `json:"source_url,omitempty"`
	// PackageSha256 is the SHA256 hash of the original package (tarball or binary).
	PackageSha256 string `json:"package_sha256,omitempty"`
	// PackageSize is the size in bytes of the original package.
	PackageSize uint64 `json:"package_size,omitempty"`
	// FabricManifestVersion is the fabric registry manifest version this cartridge was built against.
	// 0 (absent on wire) means the cartridge predates the fabric registry's versioning protocol.
	// >= 1 means the cartridge was built against manifest version N.
	FabricManifestVersion uint32 `json:"fabric_manifest_version,omitempty"`
}

// UnmarshalJSON enforces "required-but-nullable" for RegistryURL:
// the key MUST be present, the value MAY be null. Encoding/json's
// default Unmarshal treats absence and explicit null identically,
// so we re-check the raw object map. This forces older
// cartridge.json files (without the field) to surface as parse
// errors rather than silently being treated as dev installs.
func (c *CartridgeJson) UnmarshalJSON(data []byte) error {
	type rawCartridgeJson CartridgeJson
	var raw rawCartridgeJson
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	// Check for the literal presence of `registry_url` — `nil` here
	// could mean either "absent" or "explicit null"; the raw map
	// disambiguates.
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &asMap); err != nil {
		return err
	}
	if _, present := asMap["registry_url"]; !present {
		return errors.New(
			"cartridge.json is missing required `registry_url` field. " +
				"It must be present, with value null for dev installs or " +
				"a URL string for registry installs.")
	}
	*c = CartridgeJson(raw)
	return nil
}

// MarshalJSON ensures `registry_url` is always present in the
// output even when nil. The default Go encoder with `,omitempty`
// would elide it, but the consumer requires presence.
func (c CartridgeJson) MarshalJSON() ([]byte, error) {
	type withReg struct {
		Name                  string                  `json:"name"`
		Version               string                  `json:"version"`
		Channel               string                  `json:"channel"`
		RegistryURL           *string                 `json:"registry_url"`
		Entry                 string                  `json:"entry"`
		InstalledAt           string                  `json:"installed_at"`
		InstalledFrom         *CartridgeInstallSource `json:"installed_from,omitempty"`
		SourceURL             string                  `json:"source_url,omitempty"`
		PackageSha256         string                  `json:"package_sha256,omitempty"`
		PackageSize           uint64                  `json:"package_size,omitempty"`
		FabricManifestVersion uint32                  `json:"fabric_manifest_version,omitempty"`
	}
	return json.Marshal(withReg{
		Name:                  c.Name,
		Version:               c.Version,
		Channel:               c.Channel,
		RegistryURL:           c.RegistryURL,
		Entry:                 c.Entry,
		InstalledAt:           c.InstalledAt,
		InstalledFrom:         c.InstalledFrom,
		SourceURL:             c.SourceURL,
		PackageSha256:         c.PackageSha256,
		PackageSize:           c.PackageSize,
		FabricManifestVersion: c.FabricManifestVersion,
	})
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
	// CartridgeJsonErrorRegistrySlugMismatch surfaces a violation
	// of the three-place rule: the on-disk slug folder doesn't
	// match the slug derived from cartridge.json:registry_url.
	CartridgeJsonErrorRegistrySlugMismatch
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
	case CartridgeJsonErrorRegistrySlugMismatch:
		return fmt.Sprintf("cartridge.json at %s: registry slug mismatch — %s", e.Path, e.Message)
	default:
		return fmt.Sprintf("cartridge.json error at %s: %s", e.Path, e.Message)
	}
}

func (e *CartridgeJsonError) Unwrap() error {
	return e.Err
}

// ReadCartridgeJsonFromDir reads and validates a cartridge.json
// from a version directory. `expectedSlug` is the on-disk registry
// slug folder the host reached the version directory through;
// passing it in lets the parser enforce the three-place rule
// (folder slug ⇔ provenance registry_url) without leaving it to
// every caller to remember.
//
// Validates:
//   - File exists and is valid JSON
//   - cartridge.json includes required `registry_url` field
//   - SlugFor(RegistryURL) == expectedSlug
//   - Entry point path does not escape the version directory
//   - Entry point binary exists and is executable
func ReadCartridgeJsonFromDir(versionDir, expectedSlug string) (*CartridgeJson, error) {
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

	// Three-place rule (places 1+2): folder slug must match the
	// slug derived from cartridge.json's registry_url. None+`dev`
	// and Some(url)+SlugFor(url) are the only valid pairings; any
	// other combination — including a null registry_url under a
	// non-dev folder, or a non-null registry_url under the dev
	// folder — is an installer bug or a tampered tree.
	derivedSlug := SlugFor(cj.RegistryURL)
	if derivedSlug != expectedSlug {
		regStr := "null"
		if cj.RegistryURL != nil {
			regStr = *cj.RegistryURL
		}
		return nil, &CartridgeJsonError{
			Kind: CartridgeJsonErrorRegistrySlugMismatch,
			Path: jsonPath,
			Message: fmt.Sprintf(
				"registry_url=%s hashes to slug='%s' but the directory tree placed it under '%s'",
				regStr, derivedSlug, expectedSlug,
			),
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
