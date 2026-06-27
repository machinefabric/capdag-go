package media

import (
	"crypto/sha256"
	"fmt"
	"os"
	"regexp"
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

// FabricRegistry provides media def lookups against an in-memory cache
// hydrated from disk (and, in callers that wire it up, the remote registry
// catalogue at fetch time). The Go implementation does not currently
// fetch from the remote catalogue — callers populate the cache via
// AddSpec or load it from a previously persisted state.
type FabricRegistry struct {
	mu            sync.RWMutex
	cachedSpecs   map[string]StoredMediaDef
	cachedAliases map[string]StoredAlias
	extIndex      map[string][]string // lowercase extension -> list of URNs
	config        RegistryConfig
	// manifestVersion is the pinned fabric manifest version. 0 ⇒ legacy v0 /
	// flat-path mode (no manifest consulted). >= 1 ⇒ manifest-driven.
	manifestVersion uint32
	manifest        *Manifest
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
		cachedSpecs:     make(map[string]StoredMediaDef),
		cachedAliases:   make(map[string]StoredAlias),
		extIndex:        make(map[string][]string),
		config:          DefaultRegistryConfig(),
		manifestVersion: 0,
		manifest:        EmptyManifest(0),
	}, nil
}

// NewFabricRegistryForTest produces an empty registry pinned at manifest v1
// with an empty manifest, so test helpers (AddSpec, InsertCachedAliasForTest)
// flow specs/aliases into the manifest at their declared version — matching
// the Rust new_for_test invariant.
func NewFabricRegistryForTest() (*FabricRegistry, error) {
	return &FabricRegistry{
		cachedSpecs:     make(map[string]StoredMediaDef),
		cachedAliases:   make(map[string]StoredAlias),
		extIndex:        make(map[string][]string),
		config:          DefaultRegistryConfig(),
		manifestVersion: 1,
		manifest:        EmptyManifest(1),
	}, nil
}

// GetMediaDef retrieves a media def by URN or alias from the registry's cache.
//
// The argument may be a media URN (media:...) or an alias (a colon-free
// token). An alias is resolved first; because this is the typed media
// boundary, an alias whose target is not a media URN is a hard error.
func (r *FabricRegistry) GetMediaDef(urnStr string) (*StoredMediaDef, error) {
	if IsAliasToken(urnStr) {
		target, err := r.ResolveAliasTyped(urnStr, AliasTargetMedia)
		if err != nil {
			return nil, err
		}
		return r.GetMediaDef(target)
	}

	// This path returns an error, so a malformed URN propagates rather than
	// silently keeping the raw string (which would surface as a misleading
	// "not found in registry" instead of the truth).
	normalizedUrn, err := normalizeMediaUrn(urnStr)
	if err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	spec, ok := r.cachedSpecs[normalizedUrn]
	if !ok {
		return nil, &FabricRegistryError{
			Message: fmt.Sprintf("media URN '%s' not found in registry", urnStr),
		}
	}

	return &spec, nil
}

// normalizeMediaUrn parses a media URN and returns its canonical form. A parse
// failure is a HARD error — it is NEVER silently swallowed into the raw string,
// which would let a malformed URN masquerade as a cache-miss downstream.
// Callers on a path that returns an error propagate this; lookup/void paths log
// and skip. This never panics. Matches Rust's normalize_media_urn.
func normalizeMediaUrn(urnStr string) (string, error) {
	parsed, err := urn.NewMediaUrnFromString(urnStr)
	if err != nil {
		return "", &FabricRegistryError{
			Message: fmt.Sprintf("malformed media URN '%s': %v", urnStr, err),
		}
	}
	return parsed.String(), nil
}

// toLower is a helper to convert string to lowercase
func toLower(s string) string {
	return strings.ToLower(s)
}

// AddSpec adds a media def to the registry (for testing). Records the spec in
// the manifest at its version; a spec whose version is 0 is stamped to the
// pinned manifest version (matching Rust's 'test forgot to set it' handling).
func (r *FabricRegistry) AddSpec(spec StoredMediaDef) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if spec.Version == 0 && r.manifestVersion >= 1 {
		spec.Version = r.manifestVersion
	}
	// Void mutator: a malformed URN is SKIPPED (logged), never stored under a
	// raw key and never a crash.
	normalizedUrn, err := normalizeMediaUrn(spec.Urn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] AddSpec: skipping spec: %v\n", err)
		return
	}
	r.cachedSpecs[normalizedUrn] = spec
	if r.manifestVersion >= 1 {
		r.manifest.Media[normalizedUrn] = spec.Version
	}

	// Update extension index
	for _, ext := range spec.Extensions {
		extLower := toLower(ext)
		r.extIndex[extLower] = append(r.extIndex[extLower], spec.Urn)
	}
}

// GetCachedMediaDef retrieves a cached spec by URN without network access.
// Returns nil if not found (no error — absence is expected).
func (r *FabricRegistry) GetCachedMediaDef(urnStr string) *StoredMediaDef {
	// Lookup contract is a nil-on-absence pointer. A malformed URN can never
	// match a canonically-keyed entry, so treat it as a miss — but log it
	// rather than silently keying on the raw string.
	normalizedUrn, err := normalizeMediaUrn(urnStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] GetCachedMediaDef: %v\n", err)
		return nil
	}

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
	// This helper has no error channel. A malformed URN cannot be canonicalized,
	// so it is keyed by its raw bytes — but the malformation is logged rather
	// than silently passing as a valid canonical key.
	normalized, err := normalizeMediaUrn(urnStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] CacheKey: %v\n", err)
		normalized = urnStr
	}
	hash := sha256.Sum256([]byte(normalized))
	return fmt.Sprintf("%x", hash)
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
		return "", &FabricRegistryError{Message: "alias name is empty"}
	}
	if strings.Contains(name, ":") {
		return "", &FabricRegistryError{Message: fmt.Sprintf(
			"alias name '%s' contains ':' — aliases must never look like a tagged URN", name)}
	}
	if strings.ContainsAny(name, " \t\n\r\f\v") {
		return "", &FabricRegistryError{Message: fmt.Sprintf(
			"alias name '%s' contains whitespace", name)}
	}
	lowered := strings.ToLower(name)
	if !aliasNameRe.MatchString(lowered) {
		return "", &FabricRegistryError{Message: fmt.Sprintf(
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
// Alias resolution (media registry surface)
// =============================================================================

// aliasDefver resolves a normalized alias name to its defver under the pinned
// manifest. Aliases exist only in the versioned regime: at v0 any lookup is a
// hard not-found.
func (r *FabricRegistry) aliasDefver(normalizedName string) (uint32, error) {
	if r.manifestVersion == 0 {
		return 0, &FabricRegistryError{Message: fmt.Sprintf(
			"alias '%s' cannot resolve: registry is pinned at v0 (aliases are a versioned-regime concept)", normalizedName)}
	}
	defver, ok := r.manifest.Aliases[normalizedName]
	if !ok {
		return 0, &FabricRegistryError{Message: fmt.Sprintf(
			"alias '%s' is not part of manifest v%d", normalizedName, r.manifestVersion)}
	}
	return defver, nil
}

// AliasDefverFor looks up an alias name's pinned defver without fetching.
func (r *FabricRegistry) AliasDefverFor(name string) (uint32, error) {
	normalized, err := NormalizeAliasName(name)
	if err != nil {
		return 0, &FabricRegistryError{Message: fmt.Sprintf("invalid alias name: %s", err)}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.aliasDefver(normalized)
}

// GetAlias fetches the full StoredAlias for a name from the in-memory cache.
// The Go media registry does not fetch from the network; aliases are seeded
// via InsertCachedAliasForTest or loaded from disk by callers. A name absent
// from the cache (after manifest membership is confirmed) is a hard error.
func (r *FabricRegistry) GetAlias(name string) (*StoredAlias, error) {
	normalized, err := NormalizeAliasName(name)
	if err != nil {
		return nil, &FabricRegistryError{Message: fmt.Sprintf("invalid alias name: %s", err)}
	}
	r.mu.RLock()
	alias, ok := r.cachedAliases[normalized]
	r.mu.RUnlock()
	if ok {
		return &alias, nil
	}
	// Not cached: confirm manifest membership so an unknown alias is a hard
	// not-found rather than a silent miss.
	if _, derr := r.AliasDefverFor(normalized); derr != nil {
		return nil, derr
	}
	return nil, &FabricRegistryError{Message: fmt.Sprintf(
		"alias '%s' is in manifest v%d but not present in cache", normalized, r.manifestVersion)}
}

// ResolveAlias resolves an alias to the cap or media URN it points at
// (untyped): returns whatever the alias targets.
func (r *FabricRegistry) ResolveAlias(name string) (string, error) {
	alias, err := r.GetAlias(name)
	if err != nil {
		return "", err
	}
	return alias.Target, nil
}

// ResolveAliasTyped resolves an alias and asserts its target kind. If expected
// is non-empty and the resolved target is the other kind, fail hard. An empty
// expected accepts either kind.
func (r *FabricRegistry) ResolveAliasTyped(name string, expected AliasTargetKind) (string, error) {
	alias, err := r.GetAlias(name)
	if err != nil {
		return "", err
	}
	actual, ok := ClassifyAliasTarget(alias.Target)
	if !ok {
		return "", &FabricRegistryError{Message: fmt.Sprintf(
			"alias '%s' target '%s' is neither a cap nor a media URN", alias.Name, alias.Target)}
	}
	if expected != "" && actual != expected {
		return "", &FabricRegistryError{Message: fmt.Sprintf(
			"alias '%s' resolves to a %s URN ('%s') but a %s was required here",
			alias.Name, actual, alias.Target, expected)}
	}
	return alias.Target, nil
}

// ResolveAliasCached is a synchronous, in-memory-only alias resolution.
// Returns (target, true) if the alias is cached, ("", false) otherwise.
// Returns ("", false) for a malformed name so callers treat 'not a valid
// alias' and 'not cached' uniformly as 'no resolution'.
func (r *FabricRegistry) ResolveAliasCached(name string) (string, bool) {
	normalized, err := NormalizeAliasName(name)
	if err != nil {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	alias, ok := r.cachedAliases[normalized]
	if !ok {
		return "", false
	}
	return alias.Target, true
}

// InsertCachedAliasForTest inserts an alias directly into the in-memory cache
// and registers its defver in the manifest, bypassing the network (test
// helper). Mirrors Rust insert_cached_alias_for_test.
func (r *FabricRegistry) InsertCachedAliasForTest(alias StoredAlias) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cachedAliases[alias.Name] = alias
	if r.manifestVersion >= 1 {
		r.manifest.Aliases[alias.Name] = alias.Version
	}
}
