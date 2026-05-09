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

// FabricRegistry provides media spec lookups against an in-memory cache
// hydrated from disk (and, in callers that wire it up, the remote registry
// catalogue at fetch time). The Go implementation does not currently
// fetch from the remote catalogue — callers populate the cache via
// AddSpec or load it from a previously persisted state.
type FabricRegistry struct {
	mu          sync.RWMutex
	cachedSpecs map[string]StoredMediaSpec
	extIndex    map[string][]string // lowercase extension -> list of URNs
	config      RegistryConfig
}

// FabricRegistryError represents errors from the media registry
type FabricRegistryError struct {
	Message string
}

func (e *FabricRegistryError) Error() string {
	return e.Message
}

// NewFabricRegistry creates an empty media URN registry. Callers add
// known specs via AddSpec; lookups against an unpopulated cache return
// a not-found error.
func NewFabricRegistry() (*FabricRegistry, error) {
	return &FabricRegistry{
		cachedSpecs: make(map[string]StoredMediaSpec),
		extIndex:    make(map[string][]string),
		config:      DefaultRegistryConfig(),
	}, nil
}

// NewFabricRegistryForTest is an alias for NewFabricRegistry kept for
// historic test compatibility; both produce an empty registry.
func NewFabricRegistryForTest() (*FabricRegistry, error) {
	return NewFabricRegistry()
}

// GetMediaSpec retrieves a media spec by URN from the registry's cache.
func (r *FabricRegistry) GetMediaSpec(urn string) (*StoredMediaSpec, error) {
	normalizedUrn := normalizeMediaUrn(urn)

	r.mu.RLock()
	defer r.mu.RUnlock()

	spec, ok := r.cachedSpecs[normalizedUrn]
	if !ok {
		return nil, &FabricRegistryError{
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

// AddSpec adds a media spec to the registry (for testing)
func (r *FabricRegistry) AddSpec(spec StoredMediaSpec) {
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

// GetCachedMediaSpec retrieves a cached spec by URN without network access.
// Returns nil if not found (no error — absence is expected).
func (r *FabricRegistry) GetCachedMediaSpec(urnStr string) *StoredMediaSpec {
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
func (r *FabricRegistry) MediaUrnsForExtension(extension string) ([]string, error) {
	extLower := strings.ToLower(extension)

	r.mu.RLock()
	defer r.mu.RUnlock()

	urns, ok := r.extIndex[extLower]
	if !ok || len(urns) == 0 {
		return nil, &FabricRegistryError{
			Message: fmt.Sprintf("no media URNs found for extension '%s'", extension),
		}
	}

	// Return a copy to prevent mutation
	result := make([]string, len(urns))
	copy(result, urns)
	return result, nil
}

// GetExtensionMappings returns all registered extension-to-URN mappings.
func (r *FabricRegistry) GetExtensionMappings() []struct {
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
func (r *FabricRegistry) CacheKey(urnStr string) string {
	normalized := normalizeMediaUrn(urnStr)
	hash := sha256.Sum256([]byte(normalized))
	return fmt.Sprintf("%x", hash)
}
