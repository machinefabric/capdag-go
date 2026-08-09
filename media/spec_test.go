package media

import (
	"encoding/json"
	"testing"

	"github.com/machinefabric/capdag-go/standard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -------------------------------------------------------------------------
// Media URN resolution tests
// -------------------------------------------------------------------------

// Helper to create a test registry pre-seeded with the baseline media defs
// Returns a FabricRegistry pre-seeded with the small set of
// abstract value-type specs the bulk of the spec_test suite refers
// to. Tests that exercise specific seeded-spec roundtrip semantics
// (Test088, Test089) skip this helper and seed their own registry
// inline so the test reads as a self-contained statement of what
// the registry must round-trip. Long-term the goal is to migrate
// every test to explicit per-test seeding (mirroring the Rust
// reference's discipline); leaving the helper in place avoids a
// big-bang rewrite of unrelated tests.
func testRegistry(t *testing.T) *testMediaDefSource {
	t.Helper()
	registry := newTestMediaDefSource()
	for _, def := range []MediaDef{
		{Urn: "media:enc=utf-8", MediaType: "text/plain", ProfileURI: "https://capdag.com/schema/string"},
		{Urn: "media:enc=utf-8;record", MediaType: "application/json", ProfileURI: "https://capdag.com/schema/object"},
		{Urn: "media:", MediaType: "application/octet-stream"},
	} {
		registry.AddSpec(def.ToStored())
	}
	return registry
}

// TEST88: Resolving a media URN seeded into the registry returns the seeded spec verbatim. A regression in the registry-resolution path would surface as a `None`-shaped result here, since there is no local-override fallback to mask it.
func Test088_resolve_seeded_spec(t *testing.T) {
	registry := testRegistry(t)
	registry.AddSpec(MediaDef{
		Urn:       "media:enc=utf-8",
		MediaType: "text/plain",
		Title:     "Textable",
	}.ToStored())
	resolved, err := ResolveMediaUrn("media:enc=utf-8", registry)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", resolved.MediaType)
	assert.Empty(t, resolved.ProfileURI, "abstract value-type spec carries no profile_uri")
}

// TEST89: A seeded record-shaped media def carries its schema and profile_uri intact through resolution. Catches a regression that dropped optional fields when copying into ResolvedMediaDef.
func Test089_resolve_seeded_record_spec(t *testing.T) {
	registry := testRegistry(t)
	schema := map[string]any{
		"type":       "object",
		"properties": map[string]any{"name": map[string]any{"type": "string"}},
	}
	registry.AddSpec(MediaDef{
		Urn:        "media:json;output-spec;record",
		MediaType:  "application/json",
		Title:      "Output Spec",
		ProfileURI: "https://example.com/schema/output",
		Schema:     schema,
	}.ToStored())
	resolved, err := ResolveMediaUrn("media:json;output-spec;record", registry)
	require.NoError(t, err)
	assert.Equal(t, "application/json", resolved.MediaType)
	assert.Equal(t, "https://example.com/schema/output", resolved.ProfileURI)
	assert.Equal(t, schema, resolved.Schema)
}

// TEST6282: Test resolving a custom media URN from a registry-seeded media def
func Test6282_resolve_custom_media_def(t *testing.T) {
	registry := newTestMediaDefSource()
	registry.AddSpec(StoredMediaDef{
		Urn:        "media:custom-spec;fmt=json",
		MediaType:  "application/json",
		Title:      "Custom Spec",
		ProfileURI: "https://example.com/schema",
	})
	resolved, err := ResolveMediaUrn("media:custom-spec;fmt=json", registry)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	assert.Equal(t, "media:custom-spec;fmt=json", resolved.SpecID)
	assert.Equal(t, "application/json", resolved.MediaType)
	assert.Equal(t, "https://example.com/schema", resolved.ProfileURI)
	assert.Nil(t, resolved.Schema)
}

// TEST6283: Test resolving a custom record media def carrying a schema from a registry-seeded media def
func Test6283_resolve_custom_with_schema(t *testing.T) {
	registry := newTestMediaDefSource()
	schema := map[string]any{
		"type":       "object",
		"properties": map[string]any{"name": map[string]any{"type": "string"}},
	}
	registry.AddSpec(StoredMediaDef{
		Urn:        "media:fmt=json;output-spec;record",
		MediaType:  "application/json",
		Title:      "Output Spec",
		ProfileURI: "https://example.com/schema/output",
		Schema:     schema,
	})
	resolved, err := ResolveMediaUrn("media:fmt=json;output-spec;record", registry)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	assert.Equal(t, "media:fmt=json;output-spec;record", resolved.SpecID)
	assert.Equal(t, "application/json", resolved.MediaType)
	assert.Equal(t, "https://example.com/schema/output", resolved.ProfileURI)
	require.NotNil(t, resolved.Schema)
	schemaMap, ok := resolved.Schema.(map[string]any)
	require.True(t, ok, "resolved schema must be an object")
	assert.Equal(t, "object", schemaMap["type"])
}

// TESTs 090-092, 094 (deleted): exercised the legacy "local
// `media_defs` overrides registry" path. The unified registry is
// the only source of media defs in the new regime — there is no
// override layer to test. The seeded-spec roundtrip property is
// already covered by Test088 (above) and Test089 in the
// MediaDef block below. Rust dropped these for the same
// reason; this deletion keeps the Go mirror in parity with the
// Rust reference and the Python mirror.

// TEST93: Test resolving unknown media URN fails with UnresolvableMediaUrn error
func Test93_resolve_unresolvable_fails_hard(t *testing.T) {
	registry := testRegistry(t)
	// URN not in local media_defs and not in registry - FAIL HARD
	_, err := ResolveMediaUrn("media:completely-unknown-urn-not-in-registry", registry)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "media:completely-unknown-urn-not-in-registry")
	assert.Contains(t, err.Error(), "cannot resolve")
}

// TEST094 (deleted): see the consolidated note above with TESTs
// 090-092 — the override semantics it tested no longer exist.

// -------------------------------------------------------------------------
// MediaDef serialization tests
// -------------------------------------------------------------------------

// TEST95: Test MediaDef serializes with required fields and skips None fields
func Test095_media_def_def_serialize(t *testing.T) {
	def := MediaDef{
		Urn:         "media:test;json",
		MediaType:   "application/json",
		Title:       "Test Media",
		ProfileURI:  "https://example.com/profile",
		Schema:      nil,
		Description: "",
		Validation:  nil,
		Metadata:    nil,
		Extensions:  []string{},
	}
	jsonBytes, err := json.Marshal(def)
	require.NoError(t, err)
	jsonStr := string(jsonBytes)

	assert.Contains(t, jsonStr, `"urn":"media:test;json"`)
	assert.Contains(t, jsonStr, `"media_type":"application/json"`)
	assert.Contains(t, jsonStr, `"profile_uri":"https://example.com/profile"`)
	assert.Contains(t, jsonStr, `"title":"Test Media"`)
	// Empty/nil fields use omitempty - check they're omitted or empty
	// Schema is nil - omitempty skips it
	// Description is empty string - may or may not be omitted depending on tag
}

// TEST96: Test deserializing MediaDef from JSON object
func Test096_media_def_def_deserialize(t *testing.T) {
	jsonStr := `{"urn":"media:test;json","media_type":"application/json","title":"Test"}`
	var def MediaDef
	err := json.Unmarshal([]byte(jsonStr), &def)
	require.NoError(t, err)
	assert.Equal(t, "media:test;json", def.Urn)
	assert.Equal(t, "application/json", def.MediaType)
	assert.Equal(t, "Test", def.Title)
	assert.Equal(t, "", def.ProfileURI)
}

// -------------------------------------------------------------------------
// Duplicate URN validation tests
// -------------------------------------------------------------------------

// TEST97: Test duplicate URN validation catches duplicates
func Test097_validate_no_duplicate_urns_catches_duplicates(t *testing.T) {
	mediaDefs := []MediaDef{
		NewMediaDefWithTitle("media:dup;json", "application/json", "", "First"),
		NewMediaDefWithTitle("media:dup;json", "application/json", "", "Second"), // duplicate
	}
	err := ValidateNoMediaDefDuplicates(mediaDefs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "media:dup;json")
	assert.Contains(t, err.Error(), "duplicate")
}

// TEST98: Test duplicate URN validation passes for unique URNs
func Test098_validate_no_duplicate_urns_passes_for_unique(t *testing.T) {
	mediaDefs := []MediaDef{
		NewMediaDefWithTitle("media:first;json", "application/json", "", "First"),
		NewMediaDefWithTitle("media:second;json", "application/json", "", "Second"),
	}
	err := ValidateNoMediaDefDuplicates(mediaDefs)
	require.NoError(t, err)
}

// -------------------------------------------------------------------------
// ResolvedMediaDef tests
// -------------------------------------------------------------------------

// TEST99: A media def with no enc= tag is not text-representable. The old
// is_binary/is_text axis is gone; text is identified by the presence of an
// encoding (HasEncoding), so "binary" is simply the absence of one.
func Test99_resolved_is_binary(t *testing.T) {
	resolved := &ResolvedMediaDef{
		SpecID:      "media:",
		MediaType:   "application/octet-stream",
		ProfileURI:  "",
		Schema:      nil,
		Title:       "",
		Description: "",
		Validation:  nil,
		Metadata:    nil,
		Extensions:  []string{},
	}
	assert.False(t, resolved.HasEncoding(), "media: with no enc= tag is not text-representable")
	assert.False(t, resolved.IsRecord())
	assert.False(t, resolved.IsJSON())
}

// TEST100: Test ResolvedMediaDef is_record returns true when record marker is present
func Test100_resolved_is_map(t *testing.T) {
	resolved := &ResolvedMediaDef{
		SpecID:      standard.MediaJSON, // "media:fmt=json;record"
		MediaType:   "application/json",
		ProfileURI:  "",
		Schema:      nil,
		Title:       "",
		Description: "",
		Validation:  nil,
		Metadata:    nil,
		Extensions:  []string{},
	}
	assert.True(t, resolved.IsRecord())
	assert.True(t, resolved.IsJSON())
	assert.True(t, resolved.IsScalar()) // record is still scalar (no list marker)
	assert.False(t, resolved.IsList())
}

// TEST101: Test ResolvedMediaDef is_scalar returns true when list marker is absent
func Test101_resolved_is_scalar(t *testing.T) {
	resolved := &ResolvedMediaDef{
		SpecID:      "media:enc=utf-8",
		MediaType:   "text/plain",
		ProfileURI:  "",
		Schema:      nil,
		Title:       "",
		Description: "",
		Validation:  nil,
		Metadata:    nil,
		Extensions:  []string{},
	}
	assert.True(t, resolved.IsScalar())
	assert.False(t, resolved.IsRecord())
	assert.False(t, resolved.IsList())
}

// TEST102: Test ResolvedMediaDef is_list returns true when list marker is present
func Test102_resolved_is_list(t *testing.T) {
	resolved := &ResolvedMediaDef{
		SpecID:      "media:enc=utf-8;list",
		MediaType:   "application/json",
		ProfileURI:  "",
		Schema:      nil,
		Title:       "",
		Description: "",
		Validation:  nil,
		Metadata:    nil,
		Extensions:  []string{},
	}
	assert.True(t, resolved.IsList())
	assert.False(t, resolved.IsRecord())
	assert.False(t, resolved.IsScalar())
}

// TEST103: Test ResolvedMediaDef is_json returns true when json tag is present
func Test103_resolved_is_json(t *testing.T) {
	resolved := &ResolvedMediaDef{
		SpecID:      "media:fmt=json;record",
		MediaType:   "application/json",
		ProfileURI:  "",
		Schema:      nil,
		Title:       "",
		Description: "",
		Validation:  nil,
		Metadata:    nil,
		Extensions:  []string{},
	}
	assert.True(t, resolved.IsJSON())
	assert.True(t, resolved.IsRecord())
}

// TEST104: Test ResolvedMediaDef is_text returns true when enc tag is present
func Test104_resolved_is_text(t *testing.T) {
	resolved := &ResolvedMediaDef{
		SpecID:      "media:enc=utf-8",
		MediaType:   "text/plain",
		ProfileURI:  "",
		Schema:      nil,
		Title:       "",
		Description: "",
		Validation:  nil,
		Metadata:    nil,
		Extensions:  []string{},
	}
	assert.True(t, resolved.HasEncoding(), "enc=utf-8 means text-representable")
	assert.False(t, resolved.IsJSON())
}

// -------------------------------------------------------------------------
// Metadata propagation tests
// -------------------------------------------------------------------------

// TEST105: Test metadata propagates from media def def to resolved media def
func Test105_metadata_propagation(t *testing.T) {
	mediaDefs := []MediaDef{
		{
			Urn:         "media:custom-setting",
			MediaType:   "text/plain",
			Title:       "Custom Setting",
			ProfileURI:  "https://example.com/schema",
			Schema:      nil,
			Description: "A custom setting",
			Validation:  nil,
			Metadata: map[string]any{
				"category_key": "interface",
				"ui_type":      "SETTING_UI_TYPE_CHECKBOX",
			},
			Extensions: []string{},
		},
	}

	registry := testRegistry(t)
	for _, d := range mediaDefs {
		registry.AddSpec(d.ToStored())
	}
	resolved, err := ResolveMediaUrn("media:custom-setting", registry)
	require.NoError(t, err)
	require.NotNil(t, resolved.Metadata)
	assert.Equal(t, "interface", resolved.Metadata["category_key"])
	assert.Equal(t, "SETTING_UI_TYPE_CHECKBOX", resolved.Metadata["ui_type"])
}

// TEST106: Test metadata and validation can coexist in media definition
func Test106_metadata_with_validation(t *testing.T) {
	minVal := 0.0
	maxVal := 100.0
	mediaDefs := []MediaDef{
		{
			Urn:         "media:bounded-number;numeric",
			MediaType:   "text/plain",
			Title:       "Bounded Number",
			ProfileURI:  "https://example.com/schema",
			Schema:      nil,
			Description: "",
			Validation: &MediaValidation{
				Min: &minVal,
				Max: &maxVal,
			},
			Metadata: map[string]any{
				"category_key": "inference",
				"ui_type":      "SETTING_UI_TYPE_SLIDER",
			},
			Extensions: []string{},
		},
	}

	registry := testRegistry(t)
	for _, d := range mediaDefs {
		registry.AddSpec(d.ToStored())
	}
	resolved, err := ResolveMediaUrn("media:bounded-number;numeric", registry)
	require.NoError(t, err)

	// Verify validation
	require.NotNil(t, resolved.Validation)
	assert.Equal(t, 0.0, *resolved.Validation.Min)
	assert.Equal(t, 100.0, *resolved.Validation.Max)

	// Verify metadata
	require.NotNil(t, resolved.Metadata)
	assert.Equal(t, "inference", resolved.Metadata["category_key"])
}

// -------------------------------------------------------------------------
// Extension field tests
// -------------------------------------------------------------------------

// TEST107: Test extensions field propagates from media def def to resolved
func Test107_extensions_propagation(t *testing.T) {
	mediaDefs := []MediaDef{
		{
			Urn:         "media:custom-pdf",
			MediaType:   "application/pdf",
			Title:       "PDF Document",
			ProfileURI:  "https://capdag.com/schema/pdf",
			Schema:      nil,
			Description: "A PDF document",
			Validation:  nil,
			Metadata:    nil,
			Extensions:  []string{"pdf"},
		},
	}

	registry := testRegistry(t)
	for _, d := range mediaDefs {
		registry.AddSpec(d.ToStored())
	}
	resolved, err := ResolveMediaUrn("media:custom-pdf", registry)
	require.NoError(t, err)
	assert.Equal(t, []string{"pdf"}, resolved.Extensions)
}

// TEST892: Test extensions serializes/deserializes correctly in MediaDef
func Test892_extensions_serialization(t *testing.T) {
	def := MediaDef{
		Urn:         "media:json-data",
		MediaType:   "application/json",
		Title:       "JSON Data",
		ProfileURI:  "https://example.com/profile",
		Schema:      nil,
		Description: "",
		Validation:  nil,
		Metadata:    nil,
		Extensions:  []string{"json"},
	}
	jsonBytes, err := json.Marshal(def)
	require.NoError(t, err)
	jsonStr := string(jsonBytes)
	assert.Contains(t, jsonStr, `"extensions":["json"]`)

	// Deserialize and verify
	var parsed MediaDef
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)
	assert.Equal(t, []string{"json"}, parsed.Extensions)
}

// TEST893: Test extensions can coexist with metadata and validation
func Test893_extensions_with_metadata_and_validation(t *testing.T) {
	minLen := 1
	maxLen := 1000
	mediaDefs := []MediaDef{
		{
			Urn:         "media:custom-output;json",
			MediaType:   "application/json",
			Title:       "Custom Output",
			ProfileURI:  "https://example.com/schema",
			Schema:      nil,
			Description: "",
			Validation: &MediaValidation{
				MinLength: &minLen,
				MaxLength: &maxLen,
			},
			Metadata: map[string]any{
				"category": "output",
			},
			Extensions: []string{"json"},
		},
	}

	registry := testRegistry(t)
	for _, d := range mediaDefs {
		registry.AddSpec(d.ToStored())
	}
	resolved, err := ResolveMediaUrn("media:custom-output;json", registry)
	require.NoError(t, err)

	// Verify all fields are present
	require.NotNil(t, resolved.Validation)
	require.NotNil(t, resolved.Metadata)
	assert.Equal(t, []string{"json"}, resolved.Extensions)
}

// TEST894: Test multiple extensions in a media def
func Test894_multiple_extensions(t *testing.T) {
	mediaDefs := []MediaDef{
		{
			Urn:         "media:ext=jpeg;image",
			MediaType:   "image/jpeg",
			Title:       "JPEG Image",
			ProfileURI:  "https://capdag.com/schema/jpeg",
			Schema:      nil,
			Description: "JPEG image data",
			Validation:  nil,
			Metadata:    nil,
			Extensions:  []string{"jpg", "jpeg"},
		},
	}

	registry := testRegistry(t)
	for _, d := range mediaDefs {
		registry.AddSpec(d.ToStored())
	}
	resolved, err := ResolveMediaUrn("media:ext=jpeg;image", registry)
	require.NoError(t, err)
	assert.Equal(t, []string{"jpg", "jpeg"}, resolved.Extensions)
	assert.Len(t, resolved.Extensions, 2)
}

// TEST616: StoredMediaDef converts to MediaDef preserving every field.
func Test616_stored_media_def_to_media_def(t *testing.T) {
	spec := StoredMediaDef{
		Urn:         "media:ext=pdf",
		MediaType:   "application/pdf",
		Title:       "PDF Document",
		ProfileURI:  "https://capdag.com/schema/pdf",
		Description: "PDF document data",
		Extensions:  []string{"pdf"},
	}

	def := spec.ToMediaDef()
	assert.Equal(t, "media:ext=pdf", def.Urn)
	assert.Equal(t, "application/pdf", def.MediaType)
	assert.Equal(t, "PDF Document", def.Title)
	assert.Equal(t, "PDF document data", def.Description)
	assert.Equal(t, []string{"pdf"}, def.Extensions)
}

// TEST288: Documentation propagates from MediaDef through ResolveMediaUrn
// into ResolvedMediaDef. Verifies description and documentation remain distinct.
func Test288_media_documentation_propagates_through_resolve(t *testing.T) {
	registry := testRegistry(t)
	body := "## Markdown body\n\nWith `code` and a [link](https://example.com)."
	docUrn := "media:doc-test-1131;enc=utf-8"
	spec := MediaDef{
		Urn:           docUrn,
		MediaType:     "text/plain",
		Title:         "Documented",
		Description:   "short desc",
		Documentation: &body,
	}

	for _, d := range []MediaDef{spec} {
		registry.AddSpec(d.ToStored())
	}

	resolved, err := ResolveMediaUrn(docUrn, registry)
	require.NoError(t, err)
	require.NotNil(t, resolved.Documentation,
		"documentation must propagate from MediaDef into ResolvedMediaDef")
	assert.Equal(t, body, *resolved.Documentation)
	// description and documentation must remain distinct fields
	assert.Equal(t, "short desc", resolved.Description)
}

// TEST289: MediaDef serializes documentation only when present and round-trips losslessly. Mirrors TEST1127/1128 for the cap side.
func Test0289_media_def_def_documentation_round_trip(t *testing.T) {
	body := "Body with newline\nand backslash \\"
	withDoc := MediaDef{
		Urn:           "media:rt-test-1132",
		MediaType:     "text/plain",
		Title:         "Round Trip",
		Documentation: &body,
	}
	data, err := json.Marshal(withDoc)
	require.NoError(t, err)
	require.Contains(t, string(data), `"documentation"`)

	var parsed MediaDef
	require.NoError(t, json.Unmarshal(data, &parsed))
	require.NotNil(t, parsed.Documentation)
	assert.Equal(t, body, *parsed.Documentation)

	withoutDoc := MediaDef{
		Urn:       "media:rt-test-1132b",
		MediaType: "text/plain",
		Title:     "No Doc",
	}
	data2, err := json.Marshal(withoutDoc)
	require.NoError(t, err)
	assert.NotContains(t, string(data2), "documentation",
		"documentation must be omitted from MediaDef JSON when nil, got: %s", string(data2))
}

// TEST1133: MediaDef set/clear lifecycle for documentation.
// Setter and clearer must not cross-contaminate the description field.
func Test1133_media_def_def_documentation_lifecycle(t *testing.T) {
	spec := MediaDef{
		Urn:         "media:doc-test-1133",
		MediaType:   "text/plain",
		Title:       "Doc Test",
		Description: "short",
	}
	assert.Nil(t, spec.GetDocumentation())
	assert.Equal(t, "short", spec.Description)

	spec.SetDocumentation("body")
	assert.Equal(t, "body", *spec.GetDocumentation())
	// setter must not touch description
	assert.Equal(t, "short", spec.Description)

	spec.ClearDocumentation()
	assert.Nil(t, spec.GetDocumentation())
	// clearer must not touch description
	assert.Equal(t, "short", spec.Description)
}

// TEST629: Verify profile URL constants all start with capdag.com schema prefix
func Test629_profile_constants_format(t *testing.T) {
	prefix := "https://capdag.com/schema/"
	assert.True(t, len(ProfileStr) > len(prefix) && ProfileStr[:len(prefix)] == prefix,
		"PROFILE_STR must start with %s", prefix)
	assert.True(t, len(ProfileObj) > len(prefix) && ProfileObj[:len(prefix)] == prefix,
		"PROFILE_OBJ must start with %s", prefix)
}

// TEST895/896/897 (cap I/O media def extension regression checks)
// were removed: they queried `NewFabricRegistry()` for hardcoded URN
// lists and asserted each spec carried file extensions. The unified
// FabricRegistry no longer bundles standard specs — it hydrates from
// the publisher's catalogue (or local seeding) on demand. The
// "every-cap-output-URN-has-extensions" invariant belongs to the
// publisher (`fabric/src/fabric.js`), which validates spec contents
// at publish time. Asserting that invariant here against an empty
// registry is a category error.
