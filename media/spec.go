// Package capdag provides MediaDef parsing and media URN resolution
//
// Media URNs reference media type definitions in the media_defs array.
// Format: `media:<type>` with optional tags.
//
// Examples:
// - `media:textable`
// - `media:pdf`
//
// MediaDef is always a structured object - NO string form parsing.
package media

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/machinefabric/capdag-go/urn"
	taggedurn "github.com/machinefabric/tagged-urn-go"
)

// Built-in media URN constants with coercion tags
const (
	MediaVoid     = "media:void"
	MediaString   = "media:textable"
	MediaInteger  = "media:integer;textable;numeric"
	MediaNumber   = "media:textable;numeric"
	MediaBoolean  = "media:bool;textable"
	MediaObject   = "media:record;textable"
	MediaIdentity = "media:"
	// List types
	MediaList         = "media:list"
	MediaTextableList = "media:list;textable"
	MediaStringList   = "media:list;textable"
	MediaIntegerList  = "media:integer;list;textable;numeric"
	MediaNumberList   = "media:list;numeric;textable"
	MediaBooleanList  = "media:bool;list;textable"
	MediaObjectList   = "media:list;record"
	// Semantic content types
	MediaImage = "media:image;png"
	MediaAudio = "media:wav;audio"
	MediaVideo = "media:video"
	// Semantic AI input types
	MediaAudioSpeech = "media:audio;wav;speech"
	// Document types (PRIMARY naming - type IS the format)
	MediaPdf  = "media:pdf"
	MediaEpub = "media:epub"
	// Text format types (PRIMARY naming - type IS the format)
	MediaMd         = "media:md;textable"
	MediaTxt        = "media:txt;textable"
	MediaRst        = "media:rst;textable"
	MediaLog        = "media:log;textable"
	MediaHtml       = "media:html;textable"
	MediaXml        = "media:xml;textable"
	MediaJson       = "media:json;textable;record"
	MediaJsonSchema = "media:json;json-schema;textable;record"
	MediaYaml       = "media:yaml;textable;record"
	// Semantic input types
	MediaModelSpec = "media:model-spec;textable"
	MediaModelRepo = "media:model-repo;record;textable"
	// File path type — single URN; cardinality lives on is_sequence.
	MediaFilePath = "media:file-path;textable"
	// Semantic output types
	MediaModelDim  = "media:model-dim;integer;textable;numeric"
	MediaDecision  = "media:decision;json;record;textable"
	MediaTextablePage = "media:page;plain-text;textable;txt"
	// MediaPlainText is the canonical input/output of cap:save-as-txt.
	MediaPlainText = "media:plain-text;textable;txt"
	// Semantic output types for model operations
	MediaAvailabilityOutput = "media:model-availability;record;textable"
	MediaPathOutput         = "media:model-path;record;textable"
)

// Profile URL constants (defaults, use GetSchemaBase() for configurable version)
const (
	SchemaBase       = "https://capdag.com/schema"
	ProfileStr       = "https://capdag.com/schema/str"
	ProfileInt       = "https://capdag.com/schema/int"
	ProfileNum       = "https://capdag.com/schema/num"
	ProfileBool      = "https://capdag.com/schema/bool"
	ProfileObj       = "https://capdag.com/schema/obj"
	ProfileStrArray  = "https://capdag.com/schema/str-array"
	ProfileIntArray  = "https://capdag.com/schema/int-array"
	ProfileNumArray  = "https://capdag.com/schema/num-array"
	ProfileBoolArray = "https://capdag.com/schema/bool-array"
	ProfileObjArray  = "https://capdag.com/schema/obj-array"
	ProfileVoid      = "https://capdag.com/schema/void"
	// Semantic content type profiles
	ProfileImage = "https://capdag.com/schema/image"
	ProfileAudio = "https://capdag.com/schema/audio"
	ProfileVideo = "https://capdag.com/schema/video"
	ProfileText  = "https://capdag.com/schema/text"
	// Document type profiles (PRIMARY naming)
	ProfilePdf  = "https://capdag.com/schema/pdf"
	ProfileEpub = "https://capdag.com/schema/epub"
	// Text format type profiles (PRIMARY naming)
	ProfileMd   = "https://capdag.com/schema/md"
	ProfileTxt  = "https://capdag.com/schema/txt"
	ProfileRst  = "https://capdag.com/schema/rst"
	ProfileLog  = "https://capdag.com/schema/log"
	ProfileHtml = "https://capdag.com/schema/html"
	ProfileXml  = "https://capdag.com/schema/xml"
	ProfileJson = "https://capdag.com/schema/json"
	ProfileYaml = "https://capdag.com/schema/yaml"
)

// GetSchemaBase returns the schema base URL from environment variables or default
//
// Checks in order:
//  1. CDG_SCHEMA_BASE_URL environment variable
//  2. CDG_FABRIC_REGISTRY_URL environment variable + "/schema"
//  3. Default: "https://capdag.com/schema"
func GetSchemaBase() string {
	if schemaURL := os.Getenv("CDG_SCHEMA_BASE_URL"); schemaURL != "" {
		return schemaURL
	}
	if registryURL := os.Getenv("CDG_FABRIC_REGISTRY_URL"); registryURL != "" {
		return registryURL + "/schema"
	}
	return SchemaBase
}

// GetProfileURL returns a profile URL for the given profile name
//
// Example:
//
//	url := GetProfileURL("str") // Returns "{schema_base}/str"
func GetProfileURL(profileName string) string {
	return GetSchemaBase() + "/" + profileName
}

// MediaDef represents a media definition - always a structured object
// The Urn field identifies the media def within a cap's media_defs array
type MediaDef struct {
	Urn           string                 `json:"urn"`
	MediaType     string                 `json:"media_type"`
	ProfileURI    string                 `json:"profile_uri,omitempty"`
	Schema        interface{}            `json:"schema,omitempty"`
	Title         string                 `json:"title,omitempty"`
	Description   string                 `json:"description,omitempty"`
	Documentation *string                `json:"documentation,omitempty"`
	Validation    *MediaValidation       `json:"validation,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
	Extensions    []string               `json:"extensions,omitempty"`
}

// ToStored converts a MediaDef to a StoredMediaDef for use in the
// FabricRegistry. They have the same shape; this is a typed conversion.
func (m MediaDef) ToStored() StoredMediaDef {
	return StoredMediaDef{
		Urn:           m.Urn,
		MediaType:     m.MediaType,
		Title:         m.Title,
		ProfileURI:    m.ProfileURI,
		Schema:        m.Schema,
		Description:   m.Description,
		Documentation: m.Documentation,
		Validation:    m.Validation,
		Metadata:      m.Metadata,
		Extensions:    m.Extensions,
	}
}

// NewMediaDef creates a media def def with required fields
func NewMediaDef(urn, mediaType, profileURI string) MediaDef {
	return MediaDef{
		Urn:        urn,
		MediaType:  mediaType,
		ProfileURI: profileURI,
	}
}

// GetDocumentation returns the long-form markdown documentation, if any.
func (m *MediaDef) GetDocumentation() *string { return m.Documentation }

// SetDocumentation sets the long-form markdown documentation.
func (m *MediaDef) SetDocumentation(doc string) { m.Documentation = &doc }

// ClearDocumentation clears the long-form markdown documentation.
func (m *MediaDef) ClearDocumentation() { m.Documentation = nil }

// NewMediaDefWithTitle creates a media def def with title
func NewMediaDefWithTitle(urn, mediaType, profileURI, title string) MediaDef {
	return MediaDef{
		Urn:        urn,
		MediaType:  mediaType,
		ProfileURI: profileURI,
		Title:      title,
	}
}

// NewMediaDefWithSchema creates a media def def with schema
func NewMediaDefWithSchema(urn, mediaType, profileURI string, schema interface{}) MediaDef {
	return MediaDef{
		Urn:        urn,
		MediaType:  mediaType,
		ProfileURI: profileURI,
		Schema:     schema,
	}
}

// ResolvedMediaDef represents a fully resolved media def with all fields populated
type ResolvedMediaDef struct {
	SpecID        string
	MediaType     string
	ProfileURI    string
	Schema        interface{}
	Title         string
	Description   string
	Documentation *string
	Validation    *MediaValidation
	// Metadata contains arbitrary key-value pairs for display/categorization
	Metadata map[string]interface{}
	// Extensions are the file extensions for storing this media type (e.g., ["pdf"], ["jpg", "jpeg"])
	Extensions []string
}

// IsBinary returns true if the "textable" marker tag is NOT present in the source media URN.
func (r *ResolvedMediaDef) IsBinary() bool {
	return !HasMediaUrnTag(r.SpecID, "textable")
}

// IsRecord returns true if record marker tag is present (has internal key-value structure).
func (r *ResolvedMediaDef) IsRecord() bool {
	return HasMediaUrnMarkerTag(r.SpecID, "record")
}

// IsOpaque returns true if no record marker is present (opaque = default structure).
func (r *ResolvedMediaDef) IsOpaque() bool {
	return !r.IsRecord()
}

// IsScalar returns true if no list marker is present (scalar = default cardinality).
func (r *ResolvedMediaDef) IsScalar() bool {
	return !r.IsList()
}

// IsList returns true if list marker tag is present (array/list cardinality).
func (r *ResolvedMediaDef) IsList() bool {
	return HasMediaUrnMarkerTag(r.SpecID, "list")
}

// IsJSON returns true if the "json" marker tag is present in the source media URN.
// Note: This checks for JSON representation specifically, not record structure (use IsRecord for that).
func (r *ResolvedMediaDef) IsJSON() bool {
	return HasMediaUrnTag(r.SpecID, "json")
}

// IsStructured returns true if this represents structured data (has record marker).
// Structured data has internal key-value fields that can be accessed.
// Note: This does NOT check for the explicit `json` tag - use IsJSON() for that.
func (r *ResolvedMediaDef) IsStructured() bool {
	return r.IsRecord()
}

// IsText returns true if the "textable" marker tag is present in the source media URN.
func (r *ResolvedMediaDef) IsText() bool {
	return HasMediaUrnTag(r.SpecID, "textable")
}

// IsImage returns true if the "image" marker tag is present in the source media URN.
func (r *ResolvedMediaDef) IsImage() bool {
	return HasMediaUrnTag(r.SpecID, "image")
}

// IsAudio returns true if the "audio" marker tag is present in the source media URN.
func (r *ResolvedMediaDef) IsAudio() bool {
	return HasMediaUrnTag(r.SpecID, "audio")
}

// IsVideo returns true if the "video" marker tag is present in the source media URN.
func (r *ResolvedMediaDef) IsVideo() bool {
	return HasMediaUrnTag(r.SpecID, "video")
}

// IsNumeric returns true if the "numeric" marker tag is present in the source media URN.
func (r *ResolvedMediaDef) IsNumeric() bool {
	return HasMediaUrnTag(r.SpecID, "numeric")
}

// IsBool returns true if the "bool" marker tag is present in the source media URN.
func (r *ResolvedMediaDef) IsBool() bool {
	return HasMediaUrnTag(r.SpecID, "bool")
}

// HasMediaUrnTag checks if a media URN has a marker tag (e.g., json, textable).
// Uses tagged-urn parsing for proper tag detection.
// Requires a valid, non-empty media URN - panics otherwise.
func HasMediaUrnTag(mediaUrn, tagName string) bool {
	if mediaUrn == "" {
		panic("HasMediaUrnTag called with empty mediaUrn - this indicates the MediaDef was not resolved via ResolveMediaUrn")
	}
	parsed, err := taggedurn.NewTaggedUrnFromString(mediaUrn)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse media URN '%s': %v - this indicates invalid data", mediaUrn, err))
	}
	_, exists := parsed.GetTag(tagName)
	return exists
}

// HasMediaUrnTagValue checks if a media URN has a tag with a specific value (e.g., record).
// Uses tagged-urn parsing for proper tag detection.
// Requires a valid, non-empty media URN - panics otherwise.
func HasMediaUrnTagValue(mediaUrn, tagKey, tagValue string) bool {
	if mediaUrn == "" {
		panic("HasMediaUrnTagValue called with empty mediaUrn - this indicates the MediaDef was not resolved via ResolveMediaUrn")
	}
	parsed, err := taggedurn.NewTaggedUrnFromString(mediaUrn)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse media URN '%s': %v - this indicates invalid data", mediaUrn, err))
	}
	value, exists := parsed.GetTag(tagKey)
	return exists && value == tagValue
}

// HasMediaUrnMarkerTag checks if a media URN has a marker tag (tag with wildcard value "*").
// Marker tags are used for boolean flags like `list` and `record`.
// Uses tagged-urn parsing for proper tag detection.
// Requires a valid, non-empty media URN - panics otherwise.
func HasMediaUrnMarkerTag(mediaUrn, tagName string) bool {
	if mediaUrn == "" {
		panic("HasMediaUrnMarkerTag called with empty mediaUrn - this indicates the MediaDef was not resolved via ResolveMediaUrn")
	}
	parsed, err := taggedurn.NewTaggedUrnFromString(mediaUrn)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse media URN '%s': %v - this indicates invalid data", mediaUrn, err))
	}
	value, exists := parsed.GetTag(tagName)
	return exists && value == "*"
}

// PrimaryType returns the primary type (e.g., "image" from "image/png")
func (r *ResolvedMediaDef) PrimaryType() string {
	parts := strings.SplitN(r.MediaType, "/", 2)
	return parts[0]
}

// Subtype returns the subtype (e.g., "png" from "image/png")
func (r *ResolvedMediaDef) Subtype() string {
	parts := strings.SplitN(r.MediaType, "/", 2)
	if len(parts) > 1 {
		return parts[1]
	}
	return ""
}

// String returns the canonical string representation
func (r *ResolvedMediaDef) String() string {
	if r.ProfileURI != "" {
		return fmt.Sprintf("%s; profile=%s", r.MediaType, r.ProfileURI)
	}
	return r.MediaType
}

// MediaDefError represents an error in media def operations
type MediaDefError struct {
	Message string
}

func (e *MediaDefError) Error() string {
	return e.Message
}

var (
	ErrUnresolvableMediaUrn = &MediaDefError{"media URN cannot be resolved"}
	ErrInvalidMediaUrn      = &MediaDefError{"invalid media URN - must start with 'media:'"}
	ErrDuplicateMediaUrn    = &MediaDefError{"duplicate media URN in media_defs array"}
)

// NewUnresolvableMediaUrnError creates an error for unresolvable media URNs
func NewUnresolvableMediaUrnError(mediaUrn string) error {
	return &MediaDefError{
		Message: fmt.Sprintf("media URN '%s' cannot be resolved - not found in registry", mediaUrn),
	}
}

// NewDuplicateMediaUrnError creates an error for duplicate URNs in media_defs
func NewDuplicateMediaUrnError(mediaUrn string) error {
	return &MediaDefError{
		Message: fmt.Sprintf("duplicate media URN '%s' in media_defs array", mediaUrn),
	}
}

// ValidateNoMediaDefDuplicates checks for duplicate URNs in the media_defs array
func ValidateNoMediaDefDuplicates(mediaDefs []MediaDef) error {
	seen := make(map[string]bool)
	for _, spec := range mediaDefs {
		if seen[spec.Urn] {
			return NewDuplicateMediaUrnError(spec.Urn)
		}
		seen[spec.Urn] = true
	}
	return nil
}

// ResolveMediaUrn resolves a media URN to a ResolvedMediaDef via the registry.
//
// This is the SINGLE resolution path for all media URN lookups.
//
// Arguments:
//   - mediaUrn: The media URN to resolve (e.g., "media:textable")
//   - registry: The FabricRegistry for spec lookups
//
// Returns:
//   - ResolvedMediaDef if found
//   - Error if media URN cannot be resolved
func ResolveMediaUrn(mediaUrn string, registry *FabricRegistry) (*ResolvedMediaDef, error) {
	if !strings.HasPrefix(mediaUrn, "media:") {
		return nil, ErrInvalidMediaUrn
	}

	if registry == nil {
		return nil, &MediaDefError{
			Message: fmt.Sprintf("cannot resolve media URN '%s' - no registry provided", mediaUrn),
		}
	}

	storedSpec, err := registry.GetMediaDef(mediaUrn)
	if err != nil {
		return nil, &MediaDefError{
			Message: fmt.Sprintf("cannot resolve media URN '%s' - not found in registry: %v", mediaUrn, err),
		}
	}
	return &ResolvedMediaDef{
		SpecID:        mediaUrn,
		MediaType:     storedSpec.MediaType,
		ProfileURI:    storedSpec.ProfileURI,
		Schema:        storedSpec.Schema,
		Title:         storedSpec.Title,
		Description:   storedSpec.Description,
		Documentation: storedSpec.Documentation,
		Validation:    storedSpec.Validation,
		Metadata:      storedSpec.Metadata,
		Extensions:    storedSpec.Extensions,
	}, nil
}

// GetTypeFromMediaUrn returns the base type (string, integer, number, boolean, object, binary, etc.) from a media URN
// This is useful for validation to determine what Go type to expect
// Determines type based on media URN marker tags: no textable->binary, record marker->object, list marker->array, etc.
func GetTypeFromMediaUrn(mediaUrn string) string {
	// Parse the media URN to check tags
	parsed, err := taggedurn.NewTaggedUrnFromString(mediaUrn)
	if err != nil {
		return "unknown"
	}

	// Check for void
	if _, ok := parsed.GetTag("void"); ok {
		return "void"
	}

	// Check for binary (no "textable" tag)
	if _, ok := parsed.GetTag("textable"); !ok {
		return "binary"
	}

	// Check for record marker (has internal key-value structure)
	if val, ok := parsed.GetTag("record"); ok && val == "*" {
		return "object"
	}

	// Check for explicit json tag (also represents object)
	if _, ok := parsed.GetTag("json"); ok {
		return "object"
	}

	// Check for list marker (array/list cardinality)
	if val, ok := parsed.GetTag("list"); ok && val == "*" {
		return "array"
	}

	// Check specific type tags (for scalar types)
	if _, ok := parsed.GetTag("integer"); ok {
		return "integer"
	}
	if _, ok := parsed.GetTag("numeric"); ok {
		return "number"
	}
	if _, ok := parsed.GetTag("number"); ok {
		return "number"
	}
	if _, ok := parsed.GetTag("bool"); ok {
		return "boolean"
	}
	if _, ok := parsed.GetTag("textable"); ok {
		return "string"
	}

	return "unknown"
}

// GetTypeFromResolvedMediaDef determines the type from a resolved media def
func GetTypeFromResolvedMediaDef(resolved *ResolvedMediaDef) string {
	if resolved.IsBinary() {
		return "binary"
	}
	// Check for record structure (has internal fields) OR explicit json tag
	if resolved.IsRecord() || resolved.IsJSON() {
		return "object"
	}
	// Check for list structure (list)
	if resolved.IsList() {
		return "array"
	}
	// Scalar or text types
	if resolved.IsText() || resolved.IsScalar() {
		return "string"
	}
	return "unknown"
}

// GetMediaDefFromCapUrn extracts media def from a CapUrn using the 'out' tag
// The 'out' tag contains a media URN
func GetMediaDefFromCapUrn(urn *urn.CapUrn, registry *FabricRegistry) (*ResolvedMediaDef, error) {
	outUrn := urn.OutSpec()
	if outUrn == "" {
		return nil, errors.New("no 'out' tag found in cap URN")
	}
	return ResolveMediaUrn(outUrn, registry)
}
