package media

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"

	"github.com/machinefabric/capdag-go/urn"
)

// MediaValidation represents validation rules for media data
type MediaValidation struct {
	Min           *float64 `json:"min,omitempty"`
	Max           *float64 `json:"max,omitempty"`
	MinLength     *int     `json:"min_length,omitempty"`
	MaxLength     *int     `json:"max_length,omitempty"`
	Pattern       *string  `json:"pattern,omitempty"`
	AllowedValues []string `json:"allowed_values,omitempty"`
}

// RegistryConfig holds configuration for media registry
type RegistryConfig struct {
	// Add config fields as needed
}

// DefaultRegistryConfig returns default registry configuration
func DefaultRegistryConfig() RegistryConfig {
	return RegistryConfig{}
}

// StoredMediaSpec represents a media spec from the registry (matches Rust StoredMediaSpec)
type StoredMediaSpec struct {
	Urn           string           `json:"urn"`
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

// ToMediaSpecDef converts StoredMediaSpec to MediaSpecDef
func (s *StoredMediaSpec) ToMediaSpecDef() MediaSpecDef {
	return MediaSpecDef{
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

// MediaUrnRegistry provides media spec lookups with bundled standard specs
// This matches the Rust MediaUrnRegistry architecture
type MediaUrnRegistry struct {
	mu          sync.RWMutex
	cachedSpecs map[string]StoredMediaSpec
	extIndex    map[string][]string // lowercase extension -> list of URNs
	config      RegistryConfig
}

// MediaRegistryError represents errors from the media registry
type MediaRegistryError struct {
	Message string
}

func (e *MediaRegistryError) Error() string {
	return e.Message
}

// NewMediaUrnRegistry creates a new registry with bundled standard media specs
// This is the production constructor that loads all standard specs
func NewMediaUrnRegistry() (*MediaUrnRegistry, error) {
	config := DefaultRegistryConfig()
	registry := &MediaUrnRegistry{
		cachedSpecs: make(map[string]StoredMediaSpec),
		extIndex:    make(map[string][]string),
		config:      config,
	}

	// Install bundled standard media specs
	if err := registry.installStandardSpecs(); err != nil {
		return nil, err
	}

	return registry, nil
}

// NewMediaUrnRegistryForTest creates a lightweight registry for testing
// This matches Rust's new_for_test method
func NewMediaUrnRegistryForTest() (*MediaUrnRegistry, error) {
	return &MediaUrnRegistry{
		cachedSpecs: make(map[string]StoredMediaSpec),
		extIndex:    make(map[string][]string),
		config:      DefaultRegistryConfig(),
	}, nil
}

// installStandardSpecs loads bundled standard media specs into the registry
// This matches Rust's install_standard_specs method
func (r *MediaUrnRegistry) installStandardSpecs() error {
	standardSpecs := getBundledStandardMediaSpecs()

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, spec := range standardSpecs {
		normalizedUrn := normalizeMediaUrn(spec.Urn)
		r.cachedSpecs[normalizedUrn] = spec

		// Update extension index
		for _, ext := range spec.Extensions {
			extLower := toLower(ext)
			r.extIndex[extLower] = append(r.extIndex[extLower], spec.Urn)
		}
	}

	return nil
}

// GetMediaSpec retrieves a media spec by URN from the registry
// This matches Rust's get_media_spec method
//
// Resolution order:
//  1. In-memory cache (bundled standard specs)
//  2. (Future: disk cache, remote fetch)
func (r *MediaUrnRegistry) GetMediaSpec(urn string) (*StoredMediaSpec, error) {
	normalizedUrn := normalizeMediaUrn(urn)

	r.mu.RLock()
	defer r.mu.RUnlock()

	spec, ok := r.cachedSpecs[normalizedUrn]
	if !ok {
		return nil, &MediaRegistryError{
			Message: fmt.Sprintf("media URN '%s' not found in registry", urn),
		}
	}

	return &spec, nil
}

// normalizeMediaUrn normalizes a media URN for consistent lookups
// This matches Rust's normalize_media_urn function
func normalizeMediaUrn(urnStr string) string {
	// Parse and re-serialize to get canonical form
	parsed, err := urn.NewMediaUrnFromString(urnStr)
	if err != nil {
		// If parsing fails, return as-is
		return urnStr
	}
	return parsed.String()
}

// toLower is a helper to convert string to lowercase
func toLower(s string) string {
	return strings.ToLower(s)
}

// getBundledStandardMediaSpecs returns the bundled standard media specs
// This replaces the Rust STANDARD_MEDIA_SPECS static Dir with explicit data
func getBundledStandardMediaSpecs() []StoredMediaSpec {
	// These match the JSON files in capdag/standard/media/
	return []StoredMediaSpec{
		{
			Urn:         "media:",
			MediaType:   "application/octet-stream",
			Title:       "Bytes",
			ProfileURI:  "https://capdag.com/schema/bytes",
			Description: "Raw byte sequence.",
		},
		{
			Urn:         "media:textable",
			MediaType:   "text/plain",
			Title:       "String",
			ProfileURI:  "https://capdag.com/schema/string",
			Description: "UTF-8 string value.",
		},
		{
			Urn:         "media:record;textable",
			MediaType:   "application/json",
			Title:       "Map",
			ProfileURI:  "https://capdag.com/schema/map",
			Description: "String-map map value.",
		},
		{
			Urn:         "media:list;textable",
			MediaType:   "application/json",
			Title:       "List",
			ProfileURI:  "https://capdag.com/schema/list",
			Description: "Array/list value.",
		},
		{
			Urn:         "media:textable;numeric",
			MediaType:   "text/plain",
			Title:       "Number",
			ProfileURI:  "https://capdag.com/schema/number",
			Description: "Numeric scalar value.",
		},
		{
			Urn:         "media:bool;textable",
			MediaType:   "text/plain",
			Title:       "Boolean",
			ProfileURI:  "https://capdag.com/schema/boolean",
			Description: "Boolean value.",
		},
		{
			Urn:         "media:integer;textable;numeric",
			MediaType:   "text/plain",
			Title:       "Integer",
			ProfileURI:  "https://capdag.com/schema/integer",
			Description: "Integer value.",
		},
		{
			Urn:         "media:void",
			MediaType:   "application/octet-stream",
			Title:       "Void",
			ProfileURI:  "https://capdag.com/schema/void",
			Description: "No input/output.",
		},
		{
			Urn:         "media:pdf",
			MediaType:   "application/pdf",
			Title:       "PDF",
			ProfileURI:  "https://capdag.com/schema/pdf",
			Description: "PDF document.",
			Extensions:  []string{"pdf"},
		},
		{
			Urn:         "media:epub",
			MediaType:   "application/epub+zip",
			Title:       "EPUB",
			ProfileURI:  "https://capdag.com/schema/epub",
			Description: "EPUB document.",
			Extensions:  []string{"epub"},
		},
		{
			Urn:         "media:md;textable",
			MediaType:   "text/markdown",
			Title:       "Markdown",
			ProfileURI:  "https://capdag.com/schema/md",
			Description: "Markdown text.",
			Extensions:  []string{"md", "markdown"},
		},
		{
			Urn:         "media:txt;textable",
			MediaType:   "text/plain",
			Title:       "Plain Text",
			ProfileURI:  "https://capdag.com/schema/txt",
			Description: "Plain text.",
			Extensions:  []string{"txt"},
		},
		{
			Urn:         "media:html;textable",
			MediaType:   "text/html",
			Title:       "HTML",
			ProfileURI:  "https://capdag.com/schema/html",
			Description: "HTML document.",
			Extensions:  []string{"html", "htm"},
		},
		{
			Urn:         "media:xml;textable",
			MediaType:   "text/xml",
			Title:       "XML",
			ProfileURI:  "https://capdag.com/schema/xml",
			Description: "XML document.",
			Extensions:  []string{"xml"},
		},
		{
			Urn:         "media:json;textable;record",
			MediaType:   "application/json",
			Title:       "JSON",
			ProfileURI:  "https://capdag.com/schema/json",
			Description: "JSON data.",
			Extensions:  []string{"json"},
		},
		{
			Urn:         "media:yaml;textable;record",
			MediaType:   "text/yaml",
			Title:       "YAML",
			ProfileURI:  "https://capdag.com/schema/yaml",
			Description: "YAML data.",
			Extensions:  []string{"yaml", "yml"},
		},
		{
			Urn:         "media:image;png",
			MediaType:   "image/png",
			Title:       "PNG Image",
			ProfileURI:  "https://capdag.com/schema/image",
			Description: "PNG image data.",
			Extensions:  []string{"png"},
		},
		{
			Urn:         "media:image;jpeg",
			MediaType:   "image/jpeg",
			Title:       "JPEG Image",
			ProfileURI:  "https://capdag.com/schema/image",
			Description: "JPEG image data.",
			Extensions:  []string{"jpg", "jpeg"},
		},
		{
			Urn:         "media:audio;wav",
			MediaType:   "audio/wav",
			Title:       "WAV Audio",
			ProfileURI:  "https://capdag.com/schema/audio",
			Description: "WAV audio data.",
			Extensions:  []string{"wav"},
		},
		{
			Urn:         "media:video",
			MediaType:   "video/mp4",
			Title:       "Video",
			ProfileURI:  "https://capdag.com/schema/video",
			Description: "Video data.",
			Extensions:  []string{"mp4"},
		},
	}
}

// AddSpec adds a media spec to the registry (for testing)
func (r *MediaUrnRegistry) AddSpec(spec StoredMediaSpec) {
	r.mu.Lock()
	defer r.mu.Unlock()

	normalizedUrn := normalizeMediaUrn(spec.Urn)
	r.cachedSpecs[normalizedUrn] = spec

	// Update extension index
	for _, ext := range spec.Extensions {
		extLower := toLower(ext)
		r.extIndex[extLower] = append(r.extIndex[extLower], spec.Urn)
	}
}

// GetCachedSpec retrieves a cached spec by URN without network access.
// Returns nil if not found (no error — absence is expected).
func (r *MediaUrnRegistry) GetCachedSpec(urnStr string) *StoredMediaSpec {
	normalizedUrn := normalizeMediaUrn(urnStr)

	r.mu.RLock()
	defer r.mu.RUnlock()

	spec, ok := r.cachedSpecs[normalizedUrn]
	if !ok {
		return nil
	}
	return &spec
}

// MediaUrnsForExtension returns all media URNs registered for a given file extension.
// Case-insensitive. Returns error if extension not found.
func (r *MediaUrnRegistry) MediaUrnsForExtension(extension string) ([]string, error) {
	extLower := strings.ToLower(extension)

	r.mu.RLock()
	defer r.mu.RUnlock()

	urns, ok := r.extIndex[extLower]
	if !ok || len(urns) == 0 {
		return nil, &MediaRegistryError{
			Message: fmt.Sprintf("no media URNs found for extension '%s'", extension),
		}
	}

	// Return a copy to prevent mutation
	result := make([]string, len(urns))
	copy(result, urns)
	return result, nil
}

// GetExtensionMappings returns all registered extension-to-URN mappings.
func (r *MediaUrnRegistry) GetExtensionMappings() []struct {
	Extension string
	Urns      []string
} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []struct {
		Extension string
		Urns      []string
	}

	for ext, urns := range r.extIndex {
		urnsCopy := make([]string, len(urns))
		copy(urnsCopy, urns)
		result = append(result, struct {
			Extension string
			Urns      []string
		}{Extension: ext, Urns: urnsCopy})
	}

	return result
}

// CacheKey returns a deterministic cache key for a media URN.
// Uses SHA256 hash of the normalized URN.
func (r *MediaUrnRegistry) CacheKey(urnStr string) string {
	normalized := normalizeMediaUrn(urnStr)
	hash := sha256.Sum256([]byte(normalized))
	return fmt.Sprintf("%x", hash)
}
