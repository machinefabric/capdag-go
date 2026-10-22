package media

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

// Violation returns the first of these rules `value` breaks, described, or ""
// when it breaks none. The error is for a rule that cannot itself be applied
// (a pattern that is not a regular expression): a defect in the media
// definition, not in the value.
//
// A rule applies to the values it is about and passes the rest: Min and Max to
// numbers, the length rules to strings and arrays, Pattern and AllowedValues
// to strings. This is the one decision of whether a value satisfies a media
// definition's validation — what an argument value is held to at run time and
// what a constant written in a machine is held to when the machine is
// resolved.
// Matches Rust: MediaValidation::violation
func (v *MediaValidation) Violation(value any) (string, error) {
	if number, isNumber := numericValue(value); isNumber {
		if v.Min != nil && number < *v.Min {
			return fmt.Sprintf("minimum value %v", *v.Min), nil
		}
		if v.Max != nil && number > *v.Max {
			return fmt.Sprintf("maximum value %v", *v.Max), nil
		}
	}
	// Length, for strings and arrays.
	length, kind, hasLength := 0, "", false
	switch held := value.(type) {
	case string:
		length, hasLength = len(held), true
	case []any:
		length, kind, hasLength = len(held), "array ", true
	}
	if hasLength {
		if v.MinLength != nil && length < *v.MinLength {
			return fmt.Sprintf("minimum %slength %d", kind, *v.MinLength), nil
		}
		if v.MaxLength != nil && length > *v.MaxLength {
			return fmt.Sprintf("maximum %slength %d", kind, *v.MaxLength), nil
		}
	}
	text, isText := value.(string)
	if v.Pattern != nil && isText {
		pattern, err := regexp.Compile(*v.Pattern)
		if err != nil {
			return "", fmt.Errorf("Invalid regex pattern '%s': %v", *v.Pattern, err)
		}
		if !pattern.MatchString(text) {
			return fmt.Sprintf("pattern '%s'", *v.Pattern), nil
		}
	}
	// A list that allows nothing is still a rule: it refuses every string.
	if v.AllowedValues != nil && isText {
		allowed := false
		quoted := make([]string, len(v.AllowedValues))
		for i, each := range v.AllowedValues {
			quoted[i] = strconv.Quote(each)
			if each == text {
				allowed = true
			}
		}
		if !allowed {
			return "allowed values: [" + strings.Join(quoted, ", ") + "]", nil
		}
	}
	return "", nil
}

// numericValue reads a number out of a decoded value, whatever Go type the
// decoder gave it.
func numericValue(value any) (float64, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), true
	case int8:
		return float64(number), true
	case int16:
		return float64(number), true
	case int32:
		return float64(number), true
	case int64:
		return float64(number), true
	case uint:
		return float64(number), true
	case uint8:
		return float64(number), true
	case uint16:
		return float64(number), true
	case uint32:
		return float64(number), true
	case uint64:
		return float64(number), true
	case float32:
		return float64(number), true
	case float64:
		return number, true
	case json.Number:
		if float, err := number.Float64(); err == nil {
			return float, true
		}
	}
	return 0, false
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
	Version       uint32  `json:"version,omitempty"`
	MediaType     string  `json:"media_type"`
	Title         string  `json:"title"`
	ProfileURI    string  `json:"profile_uri,omitempty"`
	Schema        any     `json:"schema,omitempty"`
	Description   string  `json:"description,omitempty"`
	Documentation *string `json:"documentation,omitempty"`
	// Dims are the dims this media def was built from (see MediaDef.Dims).
	Dims       []DimRef         `json:"dims,omitempty"`
	Validation *MediaValidation `json:"validation,omitempty"`
	Metadata   map[string]any   `json:"metadata,omitempty"`
	Extensions []string         `json:"extensions,omitempty"`
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
		Dims:          s.Dims,
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
// Dims (the axes definitions are built along)
// =============================================================================
//
// A dim (a language, a file type, a quantisation, …) is a fabric definition
// like a cap, a media def or an alias: published on its own at
// dims/<sha256-of-name>/<defver>.json and pinned by a manifest's `dims` map.
// Its body says what the dim is and which values it may take, each with its
// own documentation.
//
// A cap or media definition names the dims it was built from, and the value
// chosen from each, in its `dims` (DimRef). Its own `documentation` is only
// what is specific to it; what a dim and a value mean is read from the dim. A
// reader shows both.
//
// Every dim type refuses fields it does not know, and fields it requires that
// are missing (matches Rust `deny_unknown_fields` and serde's required
// fields): a key this library does not know would be dropped when the body is
// written back out, and the copy would no longer say what the registry said.

// decodeStrict decodes data into v, refusing any field v does not declare.
func decodeStrict(data []byte, v any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(v)
}

// missingField is the error for a required field a body lacks.
func missingField(name string) error {
	return fmt.Errorf("missing field `%s`", name)
}

// DimRef is one dim a definition was built from, and the value chosen from
// it. The entries of a definition's `dims`, in the order its source references
// them.
// Matches Rust: fabric::dim::DimRef
type DimRef struct {
	// Dim is the dim's name.
	Dim string `json:"dim"`
	// Value is the name of the value chosen from that dim.
	Value string `json:"value"`
}

// NewDimRef builds a DimRef. Matches Rust: DimRef::new(dim, value)
func NewDimRef(dim, value string) DimRef {
	return DimRef{Dim: dim, Value: value}
}

// UnmarshalJSON parses a DimRef, refusing unknown and missing fields.
func (d *DimRef) UnmarshalJSON(data []byte) error {
	var wire struct {
		Dim   *string `json:"dim"`
		Value *string `json:"value"`
	}
	if err := decodeStrict(data, &wire); err != nil {
		return err
	}
	if wire.Dim == nil {
		return missingField("dim")
	}
	if wire.Value == nil {
		return missingField("value")
	}
	d.Dim, d.Value = *wire.Dim, *wire.Value
	return nil
}

// DimTag is a tag a dim value contributes to the URN of every definition built
// with it. Value is "*" for a marker tag.
// Matches Rust: fabric::dim::DimTag
type DimTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// UnmarshalJSON parses a DimTag, refusing unknown and missing fields.
func (t *DimTag) UnmarshalJSON(data []byte) error {
	var wire struct {
		Key   *string `json:"key"`
		Value *string `json:"value"`
	}
	if err := decodeStrict(data, &wire); err != nil {
		return err
	}
	if wire.Key == nil {
		return missingField("key")
	}
	if wire.Value == nil {
		return missingField("value")
	}
	t.Key, t.Value = *wire.Key, *wire.Value
	return nil
}

// StoredDimValue is one value a dim may take.
// Matches Rust: fabric::dim::StoredDimValue
type StoredDimValue struct {
	// Name is the value's name, unique within the dim.
	Name string `json:"name"`
	// Title is the display-friendly title of the value.
	Title string `json:"title"`
	// Tags are the tags the value contributes. At least one.
	Tags []DimTag `json:"tags"`
	// Documentation is markdown documentation of what choosing this value
	// means.
	Documentation *string `json:"documentation,omitempty"`
	// MediaType is the MIME media type the value gives the media defs built
	// with it.
	MediaType *string `json:"media_type,omitempty"`
	// Extensions are the file extensions the value gives the media defs built
	// with it.
	Extensions []string `json:"extensions,omitempty"`
}

// UnmarshalJSON parses a dim value, refusing unknown and missing fields.
func (v *StoredDimValue) UnmarshalJSON(data []byte) error {
	var wire struct {
		Name          *string   `json:"name"`
		Title         *string   `json:"title"`
		Tags          *[]DimTag `json:"tags"`
		Documentation *string   `json:"documentation"`
		MediaType     *string   `json:"media_type"`
		Extensions    []string  `json:"extensions"`
	}
	if err := decodeStrict(data, &wire); err != nil {
		return err
	}
	if wire.Name == nil {
		return missingField("name")
	}
	if wire.Title == nil {
		return missingField("title")
	}
	if wire.Tags == nil {
		return missingField("tags")
	}
	*v = StoredDimValue{
		Name:          *wire.Name,
		Title:         *wire.Title,
		Tags:          *wire.Tags,
		Documentation: wire.Documentation,
		MediaType:     wire.MediaType,
		Extensions:    wire.Extensions,
	}
	return nil
}

// StoredDim is the stored dim definition. Mirrors fabric/dim.schema.json on
// the wire and is the body cached at dims/<sha256-of-name>/<defver>.json.
// Matches Rust: fabric::dim::StoredDim
type StoredDim struct {
	// Name is the dim's name, unique across the snapshot.
	Name string `json:"name"`
	// Version is the per-definition version. Always >= 1 — dims exist only in
	// the versioned regime.
	Version uint32 `json:"version"`
	// Title is the display-friendly title of the dim.
	Title string `json:"title"`
	// Documentation is markdown documentation of what the dim is.
	Documentation string `json:"documentation"`
	// Values are the values the dim may take, in the order the dim declares
	// them.
	Values []StoredDimValue `json:"values"`
}

// UnmarshalJSON parses a dim body, refusing unknown and missing fields.
func (d *StoredDim) UnmarshalJSON(data []byte) error {
	var wire struct {
		Name          *string           `json:"name"`
		Version       *uint32           `json:"version"`
		Title         *string           `json:"title"`
		Documentation *string           `json:"documentation"`
		Values        *[]StoredDimValue `json:"values"`
	}
	if err := decodeStrict(data, &wire); err != nil {
		return err
	}
	switch {
	case wire.Name == nil:
		return missingField("name")
	case wire.Version == nil:
		return missingField("version")
	case wire.Title == nil:
		return missingField("title")
	case wire.Documentation == nil:
		return missingField("documentation")
	case wire.Values == nil:
		return missingField("values")
	}
	*d = StoredDim{
		Name:          *wire.Name,
		Version:       *wire.Version,
		Title:         *wire.Title,
		Documentation: *wire.Documentation,
		Values:        *wire.Values,
	}
	return nil
}

// DimErrorKind says why a dim body is not one this library can hold.
type DimErrorKind string

const (
	DimErrInvalidName      DimErrorKind = "invalid_name"
	DimErrNoValues         DimErrorKind = "no_values"
	DimErrDuplicateValue   DimErrorKind = "duplicate_value"
	DimErrValueWithoutTags DimErrorKind = "value_without_tags"
)

// DimError is why a dim body is not one this library can hold.
// Matches Rust: fabric::dim::DimError
type DimError struct {
	Kind DimErrorKind
	// Dim is the dim's name.
	Dim string
	// Value is the offending value's name, for DuplicateValue and
	// ValueWithoutTags.
	Value string
}

func (e *DimError) Error() string {
	switch e.Kind {
	case DimErrInvalidName:
		return fmt.Sprintf("dim name '%s' is not a dim name (lowercase letters, digits and '-', starting with a letter)", e.Dim)
	case DimErrNoValues:
		return fmt.Sprintf("dim '%s' declares no values", e.Dim)
	case DimErrDuplicateValue:
		return fmt.Sprintf("dim '%s' declares value '%s' more than once", e.Dim, e.Value)
	case DimErrValueWithoutTags:
		return fmt.Sprintf("dim '%s' value '%s' contributes no tag", e.Dim, e.Value)
	}
	return fmt.Sprintf("dim '%s': %s", e.Dim, e.Kind)
}

// IsDimName reports whether name is a dim name: ^[a-z][a-z0-9-]*$.
// Matches Rust: fabric::dim::is_dim_name
func IsDimName(name string) bool {
	if name == "" || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return true
}

// Value returns the value named name, or nil if the dim does not declare it.
// Matches Rust: StoredDim::value
func (d *StoredDim) Value(name string) *StoredDimValue {
	for i := range d.Values {
		if d.Values[i].Name == name {
			return &d.Values[i]
		}
	}
	return nil
}

// Validate holds the body to what dim.schema.json says beyond its field
// types: a dim name, at least one value, value names unique, every value with
// a tag. Matches Rust: StoredDim::validate
func (d *StoredDim) Validate() error {
	if !IsDimName(d.Name) {
		return &DimError{Kind: DimErrInvalidName, Dim: d.Name}
	}
	if len(d.Values) == 0 {
		return &DimError{Kind: DimErrNoValues, Dim: d.Name}
	}
	seen := make(map[string]struct{}, len(d.Values))
	for _, v := range d.Values {
		if _, dup := seen[v.Name]; dup {
			return &DimError{Kind: DimErrDuplicateValue, Dim: d.Name, Value: v.Name}
		}
		seen[v.Name] = struct{}{}
		if len(v.Tags) == 0 {
			return &DimError{Kind: DimErrValueWithoutTags, Dim: d.Name, Value: v.Name}
		}
	}
	return nil
}

// CheckDimRefs reports why a definition's `dims` do not name each dim at most
// once, by dim and value names, naming the offending entry; nil when they do.
// Matches Rust: fabric::dim::check_dim_refs
func CheckDimRefs(dims []DimRef) error {
	seen := make(map[string]struct{}, len(dims))
	for _, r := range dims {
		if !IsDimName(r.Dim) {
			return fmt.Errorf("'%s' is not a dim name", r.Dim)
		}
		if r.Value == "" {
			return fmt.Errorf("dim '%s' is named without a value", r.Dim)
		}
		if _, dup := seen[r.Dim]; dup {
			return fmt.Errorf("dim '%s' is named more than once", r.Dim)
		}
		seen[r.Dim] = struct{}{}
	}
	return nil
}

// =============================================================================
// Manifest (registry snapshot)
// =============================================================================

// Manifest is a versioned registry snapshot. Mirrors fabric/manifest.schema.json
// on the wire. v0 has no manifest object; manifests at version >= 1 name every
// cap URN, media URN, alias name and dim name in the snapshot paired with its
// defver.
type Manifest struct {
	Version  uint32            `json:"version"`
	Previous uint32            `json:"previous"`
	Caps     map[string]uint32 `json:"caps"`
	Media    map[string]uint32 `json:"media"`
	Aliases  map[string]uint32 `json:"aliases"`
	// Dims maps a dim name to its per-definition version. The body (the dim's
	// title, documentation and values) lives at
	// dims/<sha256-of-name>/<defver>.json. Every dim a cap or media def of the
	// snapshot names is here. Required: a manifest without it is not one this
	// library reads.
	Dims map[string]uint32 `json:"dims"`
}

// UnmarshalJSON parses a manifest, refusing one without a `dims` map (matches
// Rust: `dims` has no serde default).
func (m *Manifest) UnmarshalJSON(data []byte) error {
	var wire struct {
		Version  uint32             `json:"version"`
		Previous uint32             `json:"previous"`
		Caps     map[string]uint32  `json:"caps"`
		Media    map[string]uint32  `json:"media"`
		Aliases  map[string]uint32  `json:"aliases"`
		Dims     *map[string]uint32 `json:"dims"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.Dims == nil {
		return missingField("dims")
	}
	*m = Manifest{
		Version:  wire.Version,
		Previous: wire.Previous,
		Caps:     wire.Caps,
		Media:    wire.Media,
		Aliases:  wire.Aliases,
		Dims:     *wire.Dims,
	}
	return nil
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
		Dims:     make(map[string]uint32),
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
