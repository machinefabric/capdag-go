package media

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/machinefabric/capdag-go/urn"
)

// defaultMediaRegistryBaseURL mirrors cap.DefaultRegistryBaseURL — the same
// origin the unified Rust fabric::registry defaults to.
const defaultMediaRegistryBaseURL = "https://fabric.capdag.com"

// MediaValidation represents validation rules for media data
type MediaValidation struct {
	Min           *float64 `json:"min,omitempty"`
	Max           *float64 `json:"max,omitempty"`
	MinLength     *int     `json:"min_length,omitempty"`
	MaxLength     *int     `json:"max_length,omitempty"`
	Pattern       *string  `json:"pattern,omitempty"`
	AllowedValues []string `json:"allowed_values,omitempty"`
}

// RegistryConfig holds configuration for the media registry — the base URL
// object paths (media_url_and_cache_path) are built against. Mirrors Rust's
// fabric::registry::RegistryConfig (registry_base_url / schema_base_url) and
// cap.RegistryConfig, which carries the same two fields for the cap side.
type RegistryConfig struct {
	RegistryBaseURL string
	SchemaBaseURL   string
}

// DefaultRegistryConfig returns registry configuration from environment
// variables or defaults (CDG_FABRIC_REGISTRY_URL / CDG_SCHEMA_BASE_URL),
// mirroring Rust's RegistryConfig::default and cap.DefaultRegistryConfig.
func DefaultRegistryConfig() RegistryConfig {
	registryBase := os.Getenv("CDG_FABRIC_REGISTRY_URL")
	if registryBase == "" {
		registryBase = defaultMediaRegistryBaseURL
	}
	schemaBase := os.Getenv("CDG_SCHEMA_BASE_URL")
	if schemaBase == "" {
		schemaBase = registryBase + "/schema"
	}
	return RegistryConfig{RegistryBaseURL: registryBase, SchemaBaseURL: schemaBase}
}

// RegistryOption is a functional option for configuring the media registry.
// Mirrors cap.RegistryOption.
type RegistryOption func(*RegistryConfig)

// WithRegistryURL sets a custom registry URL. If the schema URL was derived
// from the old registry URL it is re-derived from the new one. Mirrors Rust's
// RegistryConfig::with_registry_url and cap.WithRegistryURL.
func WithRegistryURL(url string) RegistryOption {
	return func(c *RegistryConfig) {
		if c.SchemaBaseURL == c.RegistryBaseURL+"/schema" {
			c.SchemaBaseURL = url + "/schema"
		}
		c.RegistryBaseURL = url
	}
}

// WithSchemaURL sets a custom schema base URL. Mirrors Rust's
// RegistryConfig::with_schema_url and cap.WithSchemaURL.
func WithSchemaURL(url string) RegistryOption {
	return func(c *RegistryConfig) {
		c.SchemaBaseURL = url
	}
}

// StoredMediaDef represents a media def from the registry (matches Rust StoredMediaDef)
type StoredMediaDef struct {
	Urn string `json:"urn"`
	// Version is the per-definition version. 0 ⇒ v0 (frozen flat-path);
	// >= 1 ⇒ pinned at media/<sha256-of-urn>/<version>.json and referenced
	// by a manifest at that defver.
	Version       uint32           `json:"version,omitempty"`
	MediaType     string           `json:"media_type"`
	Title         string           `json:"title"`
	ProfileURI    string           `json:"profile_uri,omitempty"`
	Schema        any              `json:"schema,omitempty"`
	Description   string           `json:"description,omitempty"`
	Documentation *string          `json:"documentation,omitempty"`
	Validation    *MediaValidation `json:"validation,omitempty"`
	Metadata      map[string]any   `json:"metadata,omitempty"`
	Extensions    []string         `json:"extensions,omitempty"`
}

// ToMediaDef converts StoredMediaDef to MediaDef
func (s *StoredMediaDef) ToMediaDef() MediaDef {
	return MediaDef{
		Urn:           s.Urn,
		MediaType:     s.MediaType,
		Title:         s.Title,
		ProfileURI:    s.ProfileURI,
		Schema:        s.Schema,
		Description:   s.Description,
		Documentation: s.Documentation,
		Validation:    s.Validation,
		Metadata:      s.Metadata,
		Extensions:    s.Extensions,
	}
}

// MediaDefSource is the media-def lookup that `cap` and `media` need from a
// registry.
//
// The unified registry lives in package `fabric`, which imports `cap` and
// `media` — so neither of them can name its concrete type without a cycle. They
// take this interface instead, and `*fabric.FabricRegistry` satisfies it. The
// reference has one crate and therefore one concrete type at every call site;
// this is the same object reached through the only door Go's package graph
// leaves open.
type MediaDefSource interface {
	// GetMediaDef resolves a media URN or alias to its stored definition.
	GetMediaDef(urnOrAlias string) (*StoredMediaDef, error)
	// GetCachedMediaDef is the synchronous, in-memory-only probe. It never
	// touches the network, and a malformed URN reads as a miss rather than an
	// error — input resolution runs on every keystroke.
	GetCachedMediaDef(urnOrAlias string) *StoredMediaDef
	// MediaUrnsForExtension names the media URNs registered for a file
	// extension. The index hydrates lazily, so an extension is only known once
	// its owning spec has landed in cache.
	MediaUrnsForExtension(extension string) ([]string, error)
}

// =============================================================================
// Aliases (the DNS-analogue translation layer over URNs)
// =============================================================================

// AliasTargetKind is the kind of thing an alias resolves to. An alias target
// is always a URN; the kind is determined by the URN prefix.
type AliasTargetKind string

const (
	AliasTargetCap   AliasTargetKind = "cap"
	AliasTargetMedia AliasTargetKind = "media"
)

var aliasNameRe = regexp.MustCompile(`^[a-z0-9._-]+$`)

// TokenIsURN reports whether a contiguous token "looks like a URN": every
// tagged URN has the shape prefix:..., so the presence of ':' is the
// unambiguous discriminator between a URN and an alias name.
func TokenIsURN(token string) bool {
	return strings.Contains(token, ":")
}

// IsAliasToken is the complement of TokenIsURN: a colon-free token is an
// alias candidate (still subject to NormalizeAliasName validation).
func IsAliasToken(token string) bool {
	return !TokenIsURN(token)
}

// NormalizeAliasName normalizes and validates an alias name. Lowercases the
// input, then requires it non-empty, free of ':' (so it can never look like a
// tagged URN), free of whitespace, and matching [a-z0-9._-]+. Returns the
// canonical lowercased name or an error — no lenient path.
func NormalizeAliasName(name string) (string, error) {
	if name == "" {
		return "", &MediaDefError{Message: "alias name is empty"}
	}
	if strings.Contains(name, ":") {
		return "", &MediaDefError{Message: fmt.Sprintf(
			"alias name '%s' contains ':' — aliases must never look like a tagged URN", name)}
	}
	if strings.ContainsAny(name, " \t\n\r\f\v") {
		return "", &MediaDefError{Message: fmt.Sprintf(
			"alias name '%s' contains whitespace", name)}
	}
	lowered := strings.ToLower(name)
	if !aliasNameRe.MatchString(lowered) {
		return "", &MediaDefError{Message: fmt.Sprintf(
			"alias name '%s' contains invalid characters; allowed: lowercase letters, digits, '.', '_', '-'", name)}
	}
	return lowered, nil
}

// ClassifyAliasTarget classifies an alias target URN by prefix. Returns the
// kind plus true, or ("", false) if the target is neither a cap nor media URN.
func ClassifyAliasTarget(target string) (AliasTargetKind, bool) {
	if _, err := urn.NewCapUrnFromString(target); err == nil {
		return AliasTargetCap, true
	}
	if _, err := urn.NewMediaUrnFromString(target); err == nil {
		return AliasTargetMedia, true
	}
	return "", false
}

// StoredAlias is the stored alias definition. Mirrors fabric/alias.schema.json
// on the wire and is the body cached at aliases/<sha256-of-name>/<defver>.json.
type StoredAlias struct {
	Name    string `json:"name"`
	Target  string `json:"target"`
	Version uint32 `json:"version"`
}

// =============================================================================
// Manifest (registry snapshot)
// =============================================================================

// Manifest is a versioned registry snapshot. Mirrors fabric/manifest.schema.json
// on the wire. v0 has no manifest object; manifests at version >= 1 name every
// cap URN, media URN, and alias name in the snapshot paired with its defver.
type Manifest struct {
	Version  uint32            `json:"version"`
	Previous uint32            `json:"previous"`
	Caps     map[string]uint32 `json:"caps"`
	Media    map[string]uint32 `json:"media"`
	Aliases  map[string]uint32 `json:"aliases"`
}

// EmptyManifest builds an empty manifest pinned at version. previous is set to
// version-1 (or 0) so re-publishing the same content stays byte-stable.
func EmptyManifest(version uint32) *Manifest {
	prev := uint32(0)
	if version > 0 {
		prev = version - 1
	}
	return &Manifest{
		Version:  version,
		Previous: prev,
		Caps:     make(map[string]uint32),
		Media:    make(map[string]uint32),
		Aliases:  make(map[string]uint32),
	}
}

// =============================================================================
// Media defver resolution (manifest pin)
// =============================================================================

// MediaURLAndCachePath builds the registry URL and on-disk cache path for a
// per-media object at the given defver. defver == 0 addresses the frozen v0
// flat path (`<base>/media/<sha>`, cached at `<cacheDir>/media/<sha>.json`);
// defver >= 1 addresses the versioned subpath
// (`<base>/media/<sha>/<defver>.json`, cached at
// `<cacheDir>/media/<sha>/<defver>.json`). Mirrors Rust's
// media_url_and_cache_path.
func MediaURLAndCachePath(cacheDir string, config RegistryConfig, normalizedUrn string, defver uint32) (string, string) {
	hash := sha256.Sum256([]byte(normalizedUrn))
	hexHash := fmt.Sprintf("%x", hash)
	if defver == 0 {
		url := fmt.Sprintf("%s/media/%s", config.RegistryBaseURL, hexHash)
		cachePath := filepath.Join(cacheDir, "media", hexHash+".json")
		return url, cachePath
	}
	url := fmt.Sprintf("%s/media/%s/%d.json", config.RegistryBaseURL, hexHash, defver)
	cachePath := filepath.Join(cacheDir, "media", hexHash, fmt.Sprintf("%d.json", defver))
	return url, cachePath
}

// =============================================================================
// Alias resolution (media registry surface)
// =============================================================================
