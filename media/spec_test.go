package media

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/standard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -------------------------------------------------------------------------
// Media URN resolution tests
// -------------------------------------------------------------------------

// Helper to create a test registry pre-seeded with the baseline media specs
// the Go test suite expects. Mirrors the Rust test_registry() helper which
// loads the bundled standard specs.
func testRegistry(t *testing.T) *FabricRegistry {
	t.Helper()
	registry, err := NewFabricRegistry()
	require.NoError(t, err, "Failed to create test registry")
	for _, def := range []MediaSpecDef{
		{Urn: "media:textable", MediaType: "text/plain", ProfileURI: "https://capdag.com/schema/string"},
		{Urn: "media:record;textable", MediaType: "application/json", ProfileURI: "https://capdag.com/schema/object"},
		{Urn: "media:", MediaType: "application/octet-stream"},
	} {
		registry.AddSpec(def.ToStored())
	}
	return registry
}

// TEST088: Test resolving string media URN from registry returns correct media type and profile
func Test088_resolve_from_registry_str(t *testing.T) {
	registry := testRegistry(t)
	resolved, err := ResolveMediaUrn("media:textable", registry)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", resolved.MediaType)
	assert.Equal(t, "https://capdag.com/schema/string", resolved.ProfileURI)
}

// TEST089: Test resolving JSON media URN from registry returns JSON media type
func Test089_resolve_from_registry_obj(t *testing.T) {
	registry := testRegistry(t)
	resolved, err := ResolveMediaUrn("media:record;textable", registry)
	require.NoError(t, err)
	assert.Equal(t, "application/json", resolved.MediaType)
}

// TEST090: Test resolving binary media URN returns octet-stream and is_binary true
func Test090_resolve_from_registry_binary(t *testing.T) {
	registry := testRegistry(t)
	resolved, err := ResolveMediaUrn("media:", registry)
	require.NoError(t, err)
	assert.Equal(t, "application/octet-stream", resolved.MediaType)
	assert.True(t, resolved.IsBinary())
}

// TEST091: Test resolving custom media URN from local media_specs takes precedence over registry
func Test091_resolve_custom_media_spec(t *testing.T) {
	registry := testRegistry(t)
	customSpecs := []MediaSpecDef{
		{
			Urn:         "media:custom-spec;json",
			MediaType:   "application/json",
			Title:       "Custom Spec",
			ProfileURI:  "https://example.com/schema",
			Schema:      nil,
			Description: "",
			Validation:  nil,
			Metadata:    nil,
			Extensions:  []string{},
		},
	}

	// Local media_specs takes precedence over registry
	for _, d := range customSpecs { registry.AddSpec(d.ToStored()) }
	resolved, err := ResolveMediaUrn("media:custom-spec;json", registry)
	require.NoError(t, err)
	assert.Equal(t, "media:custom-spec;json", resolved.SpecID)
	assert.Equal(t, "application/json", resolved.MediaType)
	assert.Equal(t, "https://example.com/schema", resolved.ProfileURI)
	assert.Nil(t, resolved.Schema)
}

// TEST092: Test resolving custom record media spec with schema from local media_specs
func Test092_resolve_custom_with_schema(t *testing.T) {
	registry := testRegistry(t)
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
		},
	}
	customSpecs := []MediaSpecDef{
		{
			Urn:         "media:output-spec;json;record",
			MediaType:   "application/json",
			Title:       "Output Spec",
			ProfileURI:  "https://example.com/schema/output",
			Schema:      schema,
			Description: "",
			Validation:  nil,
			Metadata:    nil,
			Extensions:  []string{},
		},
	}

	for _, d := range customSpecs { registry.AddSpec(d.ToStored()) }

	resolved, err := ResolveMediaUrn("media:output-spec;json;record", registry)
	require.NoError(t, err)
	assert.Equal(t, "media:output-spec;json;record", resolved.SpecID)
	assert.Equal(t, "application/json", resolved.MediaType)
	assert.Equal(t, "https://example.com/schema/output", resolved.ProfileURI)
	assert.Equal(t, schema, resolved.Schema)
}

// TEST093: Test resolving unknown media URN fails with UnresolvableMediaUrn error
func Test093_resolve_unresolvable_fails_hard(t *testing.T) {
	registry := testRegistry(t)
	// URN not in local media_specs and not in registry - FAIL HARD
	_, err := ResolveMediaUrn("media:completely-unknown-urn-not-in-registry", registry)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "media:completely-unknown-urn-not-in-registry")
	assert.Contains(t, err.Error(), "cannot resolve")
}

// TEST094: Test local media_specs definition overrides registry definition for same URN
func Test094_local_overrides_registry(t *testing.T) {
	registry := testRegistry(t)

	// Custom definition in media_specs takes precedence over registry
	customOverride := []MediaSpecDef{
		{
			Urn:         "media:textable",
			MediaType:   "application/json", // Override: normally text/plain
			Title:       "Custom String",
			ProfileURI:  "https://custom.example.com/str",
			Schema:      nil,
			Description: "",
			Validation:  nil,
			Metadata:    nil,
			Extensions:  []string{},
		},
	}

	for _, d := range customOverride { registry.AddSpec(d.ToStored()) }

	resolved, err := ResolveMediaUrn("media:textable", registry)
	require.NoError(t, err)
	// Custom definition used, not registry
	assert.Equal(t, "application/json", resolved.MediaType)
	assert.Equal(t, "https://custom.example.com/str", resolved.ProfileURI)
}

// -------------------------------------------------------------------------
// MediaSpecDef serialization tests
// -------------------------------------------------------------------------

// TEST095: Test MediaSpecDef serializes with required fields and skips None fields
func Test095_media_spec_def_serialize(t *testing.T) {
	def := MediaSpecDef{
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

// TEST096: Test deserializing MediaSpecDef from JSON object
func Test096_media_spec_def_deserialize(t *testing.T) {
	jsonStr := `{"urn":"media:test;json","media_type":"application/json","title":"Test"}`
	var def MediaSpecDef
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

// TEST097: Test duplicate URN validation catches duplicates
func Test097_validate_no_duplicate_urns_catches_duplicates(t *testing.T) {
	mediaSpecs := []MediaSpecDef{
		NewMediaSpecDefWithTitle("media:dup;json", "application/json", "", "First"),
		NewMediaSpecDefWithTitle("media:dup;json", "application/json", "", "Second"), // duplicate
	}
	err := ValidateNoMediaSpecDuplicates(mediaSpecs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "media:dup;json")
	assert.Contains(t, err.Error(), "duplicate")
}

// TEST098: Test duplicate URN validation passes for unique URNs
func Test098_validate_no_duplicate_urns_passes_for_unique(t *testing.T) {
	mediaSpecs := []MediaSpecDef{
		NewMediaSpecDefWithTitle("media:first;json", "application/json", "", "First"),
		NewMediaSpecDefWithTitle("media:second;json", "application/json", "", "Second"),
	}
	err := ValidateNoMediaSpecDuplicates(mediaSpecs)
	require.NoError(t, err)
}

// -------------------------------------------------------------------------
// ResolvedMediaSpec tests
// -------------------------------------------------------------------------

// TEST099: Test ResolvedMediaSpec is_binary returns true when textable tag is absent
func Test099_resolved_is_binary(t *testing.T) {
	resolved := &ResolvedMediaSpec{
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
	assert.True(t, resolved.IsBinary())
	assert.False(t, resolved.IsRecord())
	assert.False(t, resolved.IsJSON())
}

// TEST100: Test ResolvedMediaSpec is_record returns true when record marker is present
func Test100_resolved_is_map(t *testing.T) {
	resolved := &ResolvedMediaSpec{
		SpecID:      standard.MediaJSON, // "media:json;record;textable"
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
	assert.True(t, resolved.IsRecord())
	assert.False(t, resolved.IsBinary())
	assert.True(t, resolved.IsScalar()) // record is still scalar (no list marker)
	assert.False(t, resolved.IsList())
}

// TEST101: Test ResolvedMediaSpec is_scalar returns true when list marker is absent
func Test101_resolved_is_scalar(t *testing.T) {
	resolved := &ResolvedMediaSpec{
		SpecID:      "media:textable",
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

// TEST102: Test ResolvedMediaSpec is_list returns true when list marker is present
func Test102_resolved_is_list(t *testing.T) {
	resolved := &ResolvedMediaSpec{
		SpecID:      "media:textable;list",
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

// TEST103: Test ResolvedMediaSpec is_json returns true when json tag is present
func Test103_resolved_is_json(t *testing.T) {
	resolved := &ResolvedMediaSpec{
		SpecID:      "media:json;textable;record",
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
	assert.False(t, resolved.IsBinary())
}

// TEST104: Test ResolvedMediaSpec is_text returns true when textable tag is present
func Test104_resolved_is_text(t *testing.T) {
	resolved := &ResolvedMediaSpec{
		SpecID:      "media:textable",
		MediaType:   "text/plain",
		ProfileURI:  "",
		Schema:      nil,
		Title:       "",
		Description: "",
		Validation:  nil,
		Metadata:    nil,
		Extensions:  []string{},
	}
	assert.True(t, resolved.IsText())
	assert.False(t, resolved.IsBinary())
	assert.False(t, resolved.IsJSON())
}

// -------------------------------------------------------------------------
// Metadata propagation tests
// -------------------------------------------------------------------------

// TEST105: Test metadata propagates from media spec def to resolved media spec
func Test105_metadata_propagation(t *testing.T) {
	mediaSpecs := []MediaSpecDef{
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
	for _, d := range mediaSpecs { registry.AddSpec(d.ToStored()) }
	resolved, err := ResolveMediaUrn("media:custom-setting", registry)
	require.NoError(t, err)
	require.NotNil(t, resolved.Metadata)
	assert.Equal(t, "interface", resolved.Metadata["category_key"])
	assert.Equal(t, "SETTING_UI_TYPE_CHECKBOX", resolved.Metadata["ui_type"])
}

// TEST106: Test metadata and validation can coexist in media spec definition
func Test106_metadata_with_validation(t *testing.T) {
	minVal := 0.0
	maxVal := 100.0
	mediaSpecs := []MediaSpecDef{
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
	for _, d := range mediaSpecs { registry.AddSpec(d.ToStored()) }
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

// TEST107: Test extensions field propagates from media spec def to resolved
func Test107_extensions_propagation(t *testing.T) {
	mediaSpecs := []MediaSpecDef{
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
	for _, d := range mediaSpecs { registry.AddSpec(d.ToStored()) }
	resolved, err := ResolveMediaUrn("media:custom-pdf", registry)
	require.NoError(t, err)
	assert.Equal(t, []string{"pdf"}, resolved.Extensions)
}

// TEST892: Test extensions serializes/deserializes correctly in MediaSpecDef
func Test892_extensions_serialization(t *testing.T) {
	def := MediaSpecDef{
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
	var parsed MediaSpecDef
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)
	assert.Equal(t, []string{"json"}, parsed.Extensions)
}

// TEST893: Test extensions can coexist with metadata and validation
func Test893_extensions_with_metadata_and_validation(t *testing.T) {
	minLen := 1
	maxLen := 1000
	mediaSpecs := []MediaSpecDef{
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
	for _, d := range mediaSpecs { registry.AddSpec(d.ToStored()) }
	resolved, err := ResolveMediaUrn("media:custom-output;json", registry)
	require.NoError(t, err)

	// Verify all fields are present
	require.NotNil(t, resolved.Validation)
	require.NotNil(t, resolved.Metadata)
	assert.Equal(t, []string{"json"}, resolved.Extensions)
}

// TEST894: Test multiple extensions in a media spec
func Test894_multiple_extensions(t *testing.T) {
	mediaSpecs := []MediaSpecDef{
		{
			Urn:         "media:image;jpeg",
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
	for _, d := range mediaSpecs { registry.AddSpec(d.ToStored()) }
	resolved, err := ResolveMediaUrn("media:image;jpeg", registry)
	require.NoError(t, err)
	assert.Equal(t, []string{"jpg", "jpeg"}, resolved.Extensions)
	assert.Len(t, resolved.Extensions, 2)
}

// -------------------------------------------------------------------------
// Media registry tests
// -------------------------------------------------------------------------

// TEST607: media_urns_for_extension returns error for unknown extension
func Test607_media_urns_for_extension_unknown(t *testing.T) {
	registry, err := NewFabricRegistryForTest()
	require.NoError(t, err)

	_, err = registry.MediaUrnsForExtension("zzzzunknown")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zzzzunknown")
}

// TEST608: media_urns_for_extension returns URNs after adding a spec with extensions
func Test608_media_urns_for_extension_populated(t *testing.T) {
	registry, err := NewFabricRegistryForTest()
	require.NoError(t, err)

	registry.AddSpec(StoredMediaSpec{
		Urn:        "media:pdf",
		MediaType:  "application/pdf",
		Title:      "PDF Document",
		Extensions: []string{"pdf"},
	})

	urns, err := registry.MediaUrnsForExtension("pdf")
	require.NoError(t, err)
	assert.NotEmpty(t, urns, "Should have at least one URN for pdf")

	found := false
	for _, u := range urns {
		if strings.Contains(u, "pdf") {
			found = true
			break
		}
	}
	assert.True(t, found, "URNs should contain pdf: %v", urns)

	// Case-insensitive
	urnsUpper, err := registry.MediaUrnsForExtension("PDF")
	require.NoError(t, err)
	assert.Equal(t, urns, urnsUpper)
}

// TEST609: get_extension_mappings returns all registered extension->URN pairs
func Test609_get_extension_mappings(t *testing.T) {
	registry, err := NewFabricRegistryForTest()
	require.NoError(t, err)

	registry.AddSpec(StoredMediaSpec{
		Urn:        "media:pdf",
		MediaType:  "application/octet-stream",
		Title:      "Test",
		Extensions: []string{"pdf"},
	})
	registry.AddSpec(StoredMediaSpec{
		Urn:        "media:epub",
		MediaType:  "application/octet-stream",
		Title:      "Test",
		Extensions: []string{"epub"},
	})

	mappings := registry.GetExtensionMappings()
	extNames := make(map[string]bool)
	for _, m := range mappings {
		extNames[m.Extension] = true
	}
	assert.True(t, extNames["pdf"], "Should contain pdf")
	assert.True(t, extNames["epub"], "Should contain epub")
}

// TEST610: get_cached_spec returns None for unknown and Some for known
func Test610_get_cached_spec(t *testing.T) {
	registry, err := NewFabricRegistryForTest()
	require.NoError(t, err)

	// Unknown spec
	assert.Nil(t, registry.GetCachedSpec("media:nonexistent;xyzzy"))

	// Add a spec and verify retrieval
	registry.AddSpec(StoredMediaSpec{
		Urn:       "media:test;spec;textable",
		MediaType: "text/plain",
		Title:     "Test Spec",
	})

	retrieved := registry.GetCachedSpec("media:test;spec;textable")
	require.NotNil(t, retrieved, "Should find spec by URN")
	assert.Equal(t, "Test Spec", retrieved.Title)
}

// TEST614: Verify registry creation succeeds and cache directory exists
func Test614_registry_creation(t *testing.T) {
	registry, err := NewFabricRegistryForTest()
	require.NoError(t, err)
	require.NotNil(t, registry)
}

// TEST615: Verify cache key generation is deterministic and distinct for different URNs
func Test615_cache_key_generation(t *testing.T) {
	registry, err := NewFabricRegistryForTest()
	require.NoError(t, err)

	key1 := registry.CacheKey("media:textable")
	key2 := registry.CacheKey("media:textable")
	key3 := registry.CacheKey("media:integer")

	assert.Equal(t, key1, key2)
	assert.NotEqual(t, key1, key3)
}

// TEST616: Verify StoredMediaSpec converts to MediaSpecDef preserving all fields
func Test616_stored_media_spec_to_def(t *testing.T) {
	spec := StoredMediaSpec{
		Urn:         "media:pdf",
		MediaType:   "application/pdf",
		Title:       "PDF Document",
		ProfileURI:  "https://capdag.com/schema/pdf",
		Description: "PDF document data",
		Extensions:  []string{"pdf"},
	}

	def := spec.ToMediaSpecDef()
	assert.Equal(t, "media:pdf", def.Urn)
	assert.Equal(t, "application/pdf", def.MediaType)
	assert.Equal(t, "PDF Document", def.Title)
	assert.Equal(t, "PDF document data", def.Description)
	assert.Equal(t, []string{"pdf"}, def.Extensions)
}

// TEST617: Verify normalize_media_urn produces consistent non-empty results
func Test617_normalize_media_urn(t *testing.T) {
	urn1 := normalizeMediaUrn("media:string")
	urn2 := normalizeMediaUrn("media:string")
	assert.NotEmpty(t, urn1)
	assert.NotEmpty(t, urn2)
	assert.Equal(t, urn1, urn2)
}

// TEST1131: Documentation propagates from MediaSpecDef through ResolveMediaUrn
// into ResolvedMediaSpec. Verifies description and documentation remain distinct.
func Test1131_media_documentation_propagates_through_resolve(t *testing.T) {
	registry := testRegistry(t)
	body := "## Markdown body\n\nWith `code` and a [link](https://example.com)."
	docUrn := "media:doc-test-1131;textable"
	spec := MediaSpecDef{
		Urn:           docUrn,
		MediaType:     "text/plain",
		Title:         "Documented",
		Description:   "short desc",
		Documentation: &body,
	}

	for _, d := range []MediaSpecDef{spec} { registry.AddSpec(d.ToStored()) }

	resolved, err := ResolveMediaUrn(docUrn, registry)
	require.NoError(t, err)
	require.NotNil(t, resolved.Documentation,
		"documentation must propagate from MediaSpecDef into ResolvedMediaSpec")
	assert.Equal(t, body, *resolved.Documentation)
	// description and documentation must remain distinct fields
	assert.Equal(t, "short desc", resolved.Description)
}

// TEST1132: MediaSpecDef serializes documentation only when present and
// round-trips losslessly. When nil, the field must be omitted entirely.
func Test1132_media_spec_def_documentation_round_trip(t *testing.T) {
	body := "Body with newline\nand backslash \\"
	withDoc := MediaSpecDef{
		Urn:           "media:rt-test-1132",
		MediaType:     "text/plain",
		Title:         "Round Trip",
		Documentation: &body,
	}
	data, err := json.Marshal(withDoc)
	require.NoError(t, err)
	require.Contains(t, string(data), `"documentation"`)

	var parsed MediaSpecDef
	require.NoError(t, json.Unmarshal(data, &parsed))
	require.NotNil(t, parsed.Documentation)
	assert.Equal(t, body, *parsed.Documentation)

	withoutDoc := MediaSpecDef{
		Urn:       "media:rt-test-1132b",
		MediaType: "text/plain",
		Title:     "No Doc",
	}
	data2, err := json.Marshal(withoutDoc)
	require.NoError(t, err)
	assert.NotContains(t, string(data2), "documentation",
		"documentation must be omitted from MediaSpecDef JSON when nil, got: %s", string(data2))
}

// TEST1133: MediaSpecDef set/clear lifecycle for documentation.
// Setter and clearer must not cross-contaminate the description field.
func Test1133_media_spec_def_documentation_lifecycle(t *testing.T) {
	spec := MediaSpecDef{
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

// -------------------------------------------------------------------------
// Cap I/O media spec regression tests
// -------------------------------------------------------------------------

// TEST895: All cap output media specs must have file extensions defined.
// This is a regression guard: every cap output URN must produce user-facing files
// with a known extension. If a spec lacks extensions, save_cap_output will fail.
func Test895_cap_output_media_specs_have_extensions(t *testing.T) {
	registry, err := NewFabricRegistry()
	require.NoError(t, err)

	capOutputUrns := []string{
		"media:textable",
		"media:embedding-vector;textable;record",
		"media:image-description;textable",
		"media:transcription;textable;record",
		"media:decision;json;record;textable",
		"media:llm-text-stream;ndjson",
		"media:generated-text;textable;record",
		"media:llm-vocab-response;json;record",
		"media:llm-model-info;json;record",
		"media:model-dim;integer;textable;numeric",
		"media:model-availability;textable;record",
		"media:model-contents;textable;record",
		"media:model-list;textable;record",
		"media:model-path;textable;record",
		"media:model-status;textable;record",
		"media:download-result;textable;record",
		"media:json;textable;record",
	}

	var missing []string
	for _, u := range capOutputUrns {
		spec := registry.GetCachedSpec(u)
		if spec == nil {
			missing = append(missing, u+" (spec not found in registry)")
		} else if len(spec.Extensions) == 0 {
			missing = append(missing, u+" (found spec but extensions is empty)")
		}
	}
	assert.Empty(t, missing,
		"Cap output media specs missing file extensions:\n  %s",
		strings.Join(missing, "\n  "))
}

// TEST896: All cap input media specs that represent user files must have extensions.
// These are the entry points — the file types users can right-click on.
func Test896_cap_input_media_specs_have_extensions(t *testing.T) {
	registry, err := NewFabricRegistry()
	require.NoError(t, err)

	capInputUrns := []string{
		"media:textable",
		"media:txt;textable",
		"media:md;textable",
		"media:rst;textable",
		"media:pdf",
		"media:image;png",
		"media:audio;wav;speech",
		"media:log;textable",
		"media:json;json-schema;textable;record",
		"media:llm-generation-request;json;record",
		"media:model-repo;textable;record",
		"media:model-spec;textable",
	}

	var missing []string
	for _, u := range capInputUrns {
		spec := registry.GetCachedSpec(u)
		if spec == nil {
			missing = append(missing, u+" (spec not found in registry)")
		} else if len(spec.Extensions) == 0 {
			missing = append(missing, u+" (found spec but extensions is empty)")
		}
	}
	assert.Empty(t, missing,
		"Cap input media specs missing file extensions:\n  %s",
		strings.Join(missing, "\n  "))
}

// TEST897: Verify that specific cap output URNs resolve to the correct extension.
// This catches misconfigurations where a spec exists but has the wrong extension.
func Test897_cap_output_extension_values_correct(t *testing.T) {
	registry, err := NewFabricRegistry()
	require.NoError(t, err)

	expected := []struct {
		urn string
		ext string
	}{
		{"media:textable", "txt"},
		{"media:embedding-vector;textable;record", "json"},
		{"media:image-description;textable", "txt"},
		{"media:transcription;textable;record", "json"},
		{"media:decision;json;record;textable", "json"},
		{"media:llm-text-stream;ndjson", "ndjson"},
		{"media:generated-text;textable;record", "json"},
		{"media:llm-model-info;json;record", "json"},
		{"media:download-result;textable;record", "json"},
		{"media:model-dim;integer;textable;numeric", "txt"},
	}

	for _, e := range expected {
		spec := registry.GetCachedSpec(e.urn)
		require.NotNil(t, spec, "Spec not found for %s", e.urn)
		require.NotEmpty(t, spec.Extensions, "No extensions for %s", e.urn)
		assert.Equal(t, e.ext, spec.Extensions[0],
			"Wrong extension for %s: got %s, want %s", e.urn, spec.Extensions[0], e.ext)
	}
}
