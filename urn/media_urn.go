package urn

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/machinefabric/capdag-go/standard"
	taggedurn "github.com/machinefabric/tagged-urn-go"
)

// MediaUrn represents a media type URN with semantic tags
// Wraps TaggedUrn with media-specific functionality
type MediaUrn struct {
	inner *taggedurn.TaggedUrn
}

// NewMediaUrnFromString parses a media URN string
func NewMediaUrnFromString(s string) (*MediaUrn, error) {
	urn, err := taggedurn.NewTaggedUrnFromString(s)
	if err != nil {
		return nil, err
	}

	// Verify it has the "media:" prefix by checking the string representation
	urnStr := urn.String()
	if !strings.HasPrefix(strings.ToLower(urnStr), "media:") {
		return nil, &taggedurn.TaggedUrnError{
			Code:    taggedurn.ErrorPrefixMismatch,
			Message: "invalid prefix for media URN: expected 'media:'",
		}
	}

	return &MediaUrn{inner: urn}, nil
}

// String returns the canonical string representation
func (m *MediaUrn) String() string {
	if m.inner == nil {
		return ""
	}
	return m.inner.String()
}

// HasTag checks if the URN has a specific tag (presence check)
func (m *MediaUrn) HasTag(tag string) bool {
	if m.inner == nil {
		return false
	}
	_, ok := m.inner.GetTag(tag)
	return ok
}

// GetTag retrieves a tag value
func (m *MediaUrn) GetTag(tag string) (string, bool) {
	if m.inner == nil {
		return "", false
	}
	return m.inner.GetTag(tag)
}

// IsBinary returns true if this represents binary (non-text) data.
// Returns true if the "textable" marker tag is NOT present.
func (m *MediaUrn) IsBinary() bool {
	return !m.HasTag("textable")
}

// IsTextable returns true if this has the "textable" tag
func (m *MediaUrn) IsTextable() bool {
	return m.HasTag("textable")
}

// IsVoid returns true if this represents void/no data
func (m *MediaUrn) IsVoid() bool {
	return m.HasTag("void")
}

// IsJson returns true if this has the "json" tag
func (m *MediaUrn) IsJson() bool {
	return m.HasTag("json")
}

// IsYaml returns true if this media URN describes YAML representation.
func (m *MediaUrn) IsYaml() bool {
	return m.HasMarkerTag("yaml")
}

// IsCsv returns true if this media URN describes CSV representation.
func (m *MediaUrn) IsCsv() bool {
	return m.HasMarkerTag("csv")
}

// Accepts checks if this MediaUrn (pattern/handler) accepts the given instance (request).
// Uses TaggedUrn.Accepts semantics: pattern accepts instance if instance satisfies pattern's constraints.
func (m *MediaUrn) Accepts(instance *MediaUrn) bool {
	if m.inner == nil || instance == nil || instance.inner == nil {
		return false
	}
	match, err := m.inner.Accepts(instance.inner)
	if err != nil {
		return false
	}
	return match
}

// ConformsTo checks if this MediaUrn (instance) conforms to the given pattern's constraints.
// Equivalent to pattern.Accepts(self).
func (m *MediaUrn) ConformsTo(pattern *MediaUrn) bool {
	if m.inner == nil || pattern == nil || pattern.inner == nil {
		return false
	}
	match, err := m.inner.ConformsTo(pattern.inner)
	if err != nil {
		return false
	}
	return match
}

// IsComparable checks if two media URNs are comparable in the order-theoretic sense.
// Two URNs are comparable if either one accepts (subsumes) the other.
// Use for discovery/validation: are they on the same specialization chain?
func (m *MediaUrn) IsComparable(other *MediaUrn) bool {
	return m.Accepts(other) || other.Accepts(m)
}

// IsEquivalent checks if two media URNs are equivalent in the order-theoretic sense.
// Two URNs are equivalent if each accepts (subsumes) the other.
// This means they have the same tag set (order-independent equality).
// Use for exact stream matching.
func (m *MediaUrn) IsEquivalent(other *MediaUrn) bool {
	return m.Accepts(other) && other.Accepts(m)
}

// Equals checks if two MediaUrns are semantically equal
func (m *MediaUrn) Equals(other *MediaUrn) bool {
	if m == nil || other == nil {
		return m == other
	}
	if m.inner == nil || other.inner == nil {
		return m.inner == other.inner
	}
	return m.inner.Equals(other.inner)
}

// Compare returns -1, 0, or 1 for ordering two MediaUrns lexicographically by string.
func (m *MediaUrn) Compare(other *MediaUrn) int {
	a := m.String()
	b := other.String()
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// Specificity returns the specificity score (number of tags)
func (m *MediaUrn) Specificity() int {
	if m.inner == nil {
		return 0
	}
	return m.inner.Specificity()
}

// TagCount returns the raw number of tags (not weighted by type).
// This matches Rust's in_media.inner().tags.len() used in CapUrn specificity scoring.
func (m *MediaUrn) TagCount() int {
	if m.inner == nil {
		return 0
	}
	return len(m.inner.AllTags())
}

// MarshalJSON implements json.Marshaler
func (m *MediaUrn) MarshalJSON() ([]byte, error) {
	if m.inner == nil {
		return json.Marshal("")
	}
	return json.Marshal(m.inner.String())
}

// UnmarshalJSON implements json.Unmarshaler
func (m *MediaUrn) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	if s == "" {
		m.inner = nil
		return nil
	}

	urn, err := NewMediaUrnFromString(s)
	if err != nil {
		return err
	}

	m.inner = urn.inner
	return nil
}

// Helper functions for common media URN operations

// =========================================================================
// CARDINALITY (list marker)
// =========================================================================

// IsList returns true if this media URN describes a list data format (has `list` marker tag).
// This is a semantic check about the data format, NOT about input cardinality or shape.
// A cap with a list output declares the data it produces is a list; it does not imply
// the cap takes multiple inputs or processes items individually.
func (m *MediaUrn) IsList() bool {
	return m.hasMarkerTag("list")
}

// IsScalar returns true if this media URN describes a scalar data format (no `list` marker).
// This is a semantic check about the data format, NOT about input cardinality or shape.
// Scalar is the default cardinality — absence of the `list` marker means the data is a single value.
func (m *MediaUrn) IsScalar() bool {
	return !m.hasMarkerTag("list")
}

// =========================================================================
// STRUCTURE (record marker)
// =========================================================================

// IsRecord returns true if this media is a record (has `record` marker tag).
// A record has internal key-value structure (e.g., JSON object).
func (m *MediaUrn) IsRecord() bool {
	return m.hasMarkerTag("record")
}

// IsOpaque returns true if this media is opaque (no `record` marker).
// Opaque is the default structure - no internal fields recognized.
func (m *MediaUrn) IsOpaque() bool {
	return !m.hasMarkerTag("record")
}

// IsStructured returns true for record data (has internal structure).
// For list detection, use IsList separately.
func (m *MediaUrn) IsStructured() bool {
	return m.IsRecord()
}

// WithTag creates a new MediaUrn with an additional or updated tag.
func (m *MediaUrn) WithTag(key, value string) *MediaUrn {
	if m.inner == nil {
		return m
	}
	return &MediaUrn{inner: m.inner.WithTag(key, value)}
}

// WithoutTag creates a new MediaUrn without a specific tag.
func (m *MediaUrn) WithoutTag(key string) *MediaUrn {
	if m.inner == nil {
		return m
	}
	return &MediaUrn{inner: m.inner.WithoutTag(key)}
}

// LeastUpperBound computes the least upper bound (most specific common type) of a set of MediaUrns.
// Returns the MediaUrn whose tag set is the intersection of all input tag sets:
// only tags present in ALL inputs with matching values are kept.
//
// - Empty input -> media: (universal type)
// - Single input -> returned as-is
func LeastUpperBound(urns []*MediaUrn) *MediaUrn {
	if len(urns) == 0 {
		u, _ := NewMediaUrnFromString("media:")
		return u
	}

	if len(urns) == 1 {
		return urns[0]
	}

	// Start with the first URN's tags, intersect with each subsequent URN
	firstTags := urns[0].inner.AllTags()
	commonTags := make(map[string]string, len(firstTags))
	for k, v := range firstTags {
		commonTags[k] = v
	}

	for _, u := range urns[1:] {
		if u.inner == nil {
			// No tags = empty intersection
			commonTags = make(map[string]string)
			break
		}
		otherTags := u.inner.AllTags()
		for key, value := range commonTags {
			otherValue, exists := otherTags[key]
			if !exists || otherValue != value {
				delete(commonTags, key)
			}
		}
	}

	result := taggedurn.NewTaggedUrnFromTags("media", commonTags)
	return &MediaUrn{inner: result}
}

// =========================================================================
// HELPER: Check for marker tag presence
// =========================================================================

// hasMarkerTag checks if a marker tag (tag with wildcard/no value) is present.
// A marker tag is stored as key="*" in the tagged URN.
func (m *MediaUrn) hasMarkerTag(tagName string) bool {
	if m.inner == nil {
		return false
	}
	val, ok := m.inner.GetTag(tagName)
	return ok && val == "*"
}

// HasMarkerTag checks if a marker tag (tag with wildcard/no value) is present.
// A marker tag is stored as key="*" in the tagged URN.
// This is the exported version of hasMarkerTag.
func (m *MediaUrn) HasMarkerTag(tagName string) bool {
	return m.hasMarkerTag(tagName)
}

// IsImage returns true if this has the "image" marker tag
func (m *MediaUrn) IsImage() bool {
	return m.HasTag("image")
}

// IsAudio returns true if this has the "audio" marker tag
func (m *MediaUrn) IsAudio() bool {
	return m.HasTag("audio")
}

// IsVideo returns true if this has the "video" marker tag
func (m *MediaUrn) IsVideo() bool {
	return m.HasTag("video")
}

// IsNumeric returns true if this has the "numeric" marker tag
func (m *MediaUrn) IsNumeric() bool {
	return m.HasTag("numeric")
}

// IsBool returns true if this has the "bool" marker tag
func (m *MediaUrn) IsBool() bool {
	return m.HasTag("bool")
}

// IsFilePath returns true if this has the "file-path" marker tag AND NOT IsList()
func (m *MediaUrn) IsFilePath() bool {
	return m.HasTag("file-path") && !m.IsList()
}

// IsFilePathArray returns true if this has the "file-path" marker tag AND IsList()
func (m *MediaUrn) IsFilePathArray() bool {
	return m.HasTag("file-path") && m.IsList()
}

// IsAnyFilePath returns true if this has the "file-path" marker tag (single or array)
func (m *MediaUrn) IsAnyFilePath() bool {
	return m.HasTag("file-path")
}

// GetExtension returns the ext tag value if present
func (m *MediaUrn) GetExtension() (string, bool) {
	return m.GetTag("ext")
}

// Built-in media URN constructors matching Rust

// MediaUrnVoid creates a void media URN
func MediaUrnVoid() *MediaUrn {
	m, _ := NewMediaUrnFromString(standard.MediaVoid)
	return m
}

// MediaUrnString creates a string media URN
func MediaUrnString() *MediaUrn {
	m, _ := NewMediaUrnFromString(standard.MediaString)
	return m
}

// MediaUrnBytes creates a binary media URN
func MediaUrnBytes() *MediaUrn {
	m, _ := NewMediaUrnFromString(standard.MediaIdentity)
	return m
}

// MediaUrnObject creates an object media URN
func MediaUrnObject() *MediaUrn {
	m, _ := NewMediaUrnFromString(standard.MediaObject)
	return m
}

// MediaUrnInteger creates an integer media URN
func MediaUrnInteger() *MediaUrn {
	m, _ := NewMediaUrnFromString(standard.MediaInteger)
	return m
}

// MediaUrnNumber creates a number media URN
func MediaUrnNumber() *MediaUrn {
	m, _ := NewMediaUrnFromString(standard.MediaNumber)
	return m
}

// MediaUrnBoolean creates a boolean media URN
func MediaUrnBoolean() *MediaUrn {
	m, _ := NewMediaUrnFromString(standard.MediaBoolean)
	return m
}

// BinaryMediaUrnForExt builds a binary media URN with the given file extension
func BinaryMediaUrnForExt(ext string) string {
	return fmt.Sprintf("media:binary;ext=%s", ext)
}

// TextMediaUrnForExt builds a text media URN with the given file extension
func TextMediaUrnForExt(ext string) string {
	return fmt.Sprintf("media:ext=%s;textable", ext)
}

// ImageMediaUrnForExt builds an image media URN with the given file extension
func ImageMediaUrnForExt(ext string) string {
	return fmt.Sprintf("media:image;ext=%s", ext)
}

// AudioMediaUrnForExt builds an audio media URN with the given file extension
func AudioMediaUrnForExt(ext string) string {
	return fmt.Sprintf("media:audio;ext=%s", ext)
}
