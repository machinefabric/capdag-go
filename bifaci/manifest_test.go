package bifaci

import (
	"encoding/json"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper for manifest tests - use proper media URNs with tags
func manifestTestUrn(tags string) string {
	if tags == "" {
		return `cap:in="media:void";out="media:json;record;textable"`
	}
	return `cap:in="media:void";out="media:json;record;textable";` + tags
}

// TEST148: Test creating cap manifest with name, version, description, and caps
func Test148_cap_manifest_creation(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", "extract-metadata")

	manifest := NewCapManifest(
		"TestComponent",
		"0.1.0",
		"A test component for validation",
		[]cap.Cap{*capDef},
	)

	assert.Equal(t, "TestComponent", manifest.Name)
	assert.Equal(t, "0.1.0", manifest.Version)
	assert.Equal(t, "A test component for validation", manifest.Description)
	assert.Len(t, manifest.Caps, 1)
	assert.Nil(t, manifest.Author)
}

// TEST149: Test cap manifest with author field sets author correctly
func Test149_cap_manifest_with_author(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", "extract-metadata")

	manifest := NewCapManifest(
		"TestComponent",
		"0.1.0",
		"A test component for validation",
		[]cap.Cap{*capDef},
	).WithAuthor("Test Author")

	require.NotNil(t, manifest.Author)
	assert.Equal(t, "Test Author", *manifest.Author)
}

func TestCapManifestWithPageURL(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", "extract-metadata")

	manifest := NewCapManifest(
		"TestComponent",
		"0.1.0",
		"A test component for validation",
		[]cap.Cap{*capDef},
	).WithAuthor("Test Author").WithPageUrl("https://github.com/example/test")

	require.NotNil(t, manifest.PageUrl)
	assert.Equal(t, "https://github.com/example/test", *manifest.PageUrl)

	// Verify it serializes correctly
	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)
	jsonStr := string(jsonData)
	assert.Contains(t, jsonStr, `"page_url":"https://github.com/example/test"`)
}

// TEST150: Test cap manifest JSON serialization and deserialization roundtrip
func Test150_cap_manifest_json_serialization(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", "extract-metadata")
	// Add an argument with stdin source using new architecture
	stdinUrn := "media:pdf"
	capDef.AddArg(cap.CapArg{
		MediaUrn: standard.MediaIdentity,
		Required: true,
		Sources:  []cap.ArgSource{{Stdin: &stdinUrn}},
	})

	manifest := NewCapManifest(
		"TestComponent",
		"0.1.0",
		"A test component for validation",
		[]cap.Cap{*capDef},
	).WithAuthor("Test Author")

	// Test serialization
	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)

	jsonStr := string(jsonData)
	assert.Contains(t, jsonStr, `"name":"TestComponent"`)
	assert.Contains(t, jsonStr, `"version":"0.1.0"`)
	assert.Contains(t, jsonStr, `"author":"Test Author"`)
	assert.Contains(t, jsonStr, `"stdin":"media:pdf"`)

	// Test deserialization
	var deserialized CapManifest
	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	assert.Equal(t, manifest.Name, deserialized.Name)
	assert.Equal(t, manifest.Version, deserialized.Version)
	assert.Equal(t, manifest.Description, deserialized.Description)
	assert.Equal(t, manifest.Author, deserialized.Author)
	assert.Len(t, deserialized.Caps, len(manifest.Caps))
	assert.Equal(t, *manifest.Caps[0].GetStdinMediaUrn(), *deserialized.Caps[0].GetStdinMediaUrn())
}

// TEST151: Test cap manifest deserialization fails when required fields are missing
func Test151_cap_manifest_required_fields(t *testing.T) {
	// Test that deserialization succeeds even with missing optional fields
	// (Go JSON unmarshaling uses zero values for missing fields)
	partialJSON := `{"name": "TestComponent", "version": "1.0.0", "description": "Test", "caps": []}`
	var result CapManifest
	err := json.Unmarshal([]byte(partialJSON), &result)
	assert.NoError(t, err)
	assert.Equal(t, "TestComponent", result.Name)
	assert.Equal(t, "1.0.0", result.Version)
	assert.Equal(t, "Test", result.Description)
	assert.Len(t, result.Caps, 0)
	assert.Nil(t, result.Author)

	// Test that invalid JSON fails
	invalidJSON := `{"name": "TestComponent", invalid`
	var result2 CapManifest
	err2 := json.Unmarshal([]byte(invalidJSON), &result2)
	assert.Error(t, err2)
}

// TEST152: Test cap manifest with multiple caps stores and retrieves all capabilities
func Test152_cap_manifest_with_multiple_caps(t *testing.T) {
	id1, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=metadata"))
	require.NoError(t, err)
	cap1 := cap.NewCap(id1, "Metadata Extractor", "extract-metadata")

	id2, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=outline"))
	require.NoError(t, err)
	metadata := map[string]string{"supports_outline": "true"}
	cap2 := cap.NewCapWithMetadata(id2, "Outline Extractor", "extract-outline", metadata)

	manifest := NewCapManifest(
		"MultiCapComponent",
		"1.0.0",
		"Component with multiple caps",
		[]cap.Cap{*cap1, *cap2},
	)

	assert.Len(t, manifest.Caps, 2)
	// Canonical form includes in/out
	assert.Contains(t, manifest.Caps[0].UrnString(), "op=extract")
	assert.Contains(t, manifest.Caps[0].UrnString(), "target=metadata")
	assert.Contains(t, manifest.Caps[1].UrnString(), "op=extract")
	assert.Contains(t, manifest.Caps[1].UrnString(), "target=outline")
	assert.True(t, manifest.Caps[1].HasMetadata("supports_outline"))
}

// TEST153: Test cap manifest with empty caps list serializes and deserializes correctly
func Test153_cap_manifest_empty_caps(t *testing.T) {
	manifest := NewCapManifest(
		"EmptyComponent",
		"1.0.0",
		"Component with no caps",
		[]cap.Cap{},
	)

	assert.Len(t, manifest.Caps, 0)

	// Should still serialize/deserialize correctly
	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)

	var deserialized CapManifest
	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)
	assert.Len(t, deserialized.Caps, 0)
}

// TEST154: Test cap manifest optional author field skipped in serialization when None
func Test154_cap_manifest_optional_fields(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=validate;file"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "File Validator", "validate")

	manifestWithoutOptionals := NewCapManifest(
		"ValidatorComponent",
		"1.0.0",
		"File validation component",
		[]cap.Cap{*capDef},
	)

	// Serialize manifest without optional fields
	jsonData, err := json.Marshal(manifestWithoutOptionals)
	require.NoError(t, err)

	jsonStr := string(jsonData)
	assert.NotContains(t, jsonStr, `"author"`)
	assert.NotContains(t, jsonStr, `"page_url"`)

	// Should deserialize correctly
	var deserialized CapManifest
	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)
	assert.Nil(t, deserialized.Author)
	assert.Nil(t, deserialized.PageUrl)
}

// Test component that implements ComponentMetadata interface
type testComponent struct {
	name string
	caps []cap.Cap
}

// Implement the ComponentMetadata interface
func (tc *testComponent) ComponentManifest() *CapManifest {
	return NewCapManifest(
		tc.name,
		"1.0.0",
		"Test component implementation",
		tc.caps,
	)
}

func (tc *testComponent) Caps() []cap.Cap {
	return tc.ComponentManifest().Caps
}

// TEST155: Test ComponentMetadata trait provides manifest and caps accessor methods
func Test155_component_metadata_interface(t *testing.T) {
	// Use type=component key=value instead of component flag
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=test;type=component"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "Test Component", "test")

	component := &testComponent{
		name: "TestImpl",
		caps: []cap.Cap{*capDef},
	}

	manifest := component.ComponentManifest()
	assert.Equal(t, "TestImpl", manifest.Name)

	caps := component.Caps()
	assert.Len(t, caps, 1)
	assert.Contains(t, caps[0].UrnString(), "op=test")
	assert.Contains(t, caps[0].UrnString(), "type=component")
}

func TestCapManifestValidation(t *testing.T) {
	// Test that manifest with valid caps works
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", "extract-metadata")
	// Add an argument with stdin source using new architecture
	stdinUrn := "media:pdf"
	capDef.AddArg(cap.CapArg{
		MediaUrn: standard.MediaIdentity,
		Required: true,
		Sources:  []cap.ArgSource{{Stdin: &stdinUrn}},
	})

	manifest := NewCapManifest(
		"ValidComponent",
		"1.0.0",
		"Valid component for testing",
		[]cap.Cap{*capDef},
	)

	// Validate that all required fields are present
	assert.NotEmpty(t, manifest.Name)
	assert.NotEmpty(t, manifest.Version)
	assert.NotEmpty(t, manifest.Description)
	assert.NotNil(t, manifest.Caps)

	// Validate cap integrity
	assert.Len(t, manifest.Caps, 1)
	capInManifest := manifest.Caps[0]
	// Version field removed from cap.Cap struct
	assert.Equal(t, "extract-metadata", capInManifest.Command)
	assert.True(t, capInManifest.AcceptsStdin())
}

func TestCapManifestCompatibility(t *testing.T) {
	// Test that manifest format is compatible between different types
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=process"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "Data Processor", "process")

	// Create manifest similar to what a cartridge would have
	cartridgeStyleManifest := NewCapManifest(
		"CartridgeComponent",
		"0.1.0",
		"Cartridge-style component",
		[]cap.Cap{*capDef},
	)

	// Create manifest similar to what a provider would have
	providerStyleManifest := NewCapManifest(
		"ProviderComponent",
		"0.1.0",
		"Provider-style component",
		[]cap.Cap{*capDef},
	)

	// Both should serialize to the same structure
	cartridgeJSON, err := json.Marshal(cartridgeStyleManifest)
	require.NoError(t, err)

	providerJSON, err := json.Marshal(providerStyleManifest)
	require.NoError(t, err)

	// Structure should be identical (except for name/description)
	var cartridgeMap map[string]interface{}
	var providerMap map[string]interface{}

	err = json.Unmarshal(cartridgeJSON, &cartridgeMap)
	require.NoError(t, err)

	err = json.Unmarshal(providerJSON, &providerMap)
	require.NoError(t, err)

	// Same structure
	assert.Equal(t, len(cartridgeMap), len(providerMap))
	assert.Contains(t, cartridgeMap, "name")
	assert.Contains(t, cartridgeMap, "version")
	assert.Contains(t, cartridgeMap, "description")
	assert.Contains(t, cartridgeMap, "caps")

	// Same field types
	assert.IsType(t, providerMap["name"], cartridgeMap["name"])
	assert.IsType(t, providerMap["version"], cartridgeMap["version"])
	assert.IsType(t, providerMap["description"], cartridgeMap["description"])
	assert.IsType(t, providerMap["caps"], cartridgeMap["caps"])
}

// TEST475: CapManifest.Validate() passes when CAP_IDENTITY is present
func Test475_validate_passes_with_identity(t *testing.T) {
	identityUrn, err := urn.NewCapUrnFromString(standard.CapIdentity)
	require.NoError(t, err)
	identityCap := cap.NewCap(identityUrn, "Identity", "identity")

	manifest := NewCapManifest("TestCartridge", "1.0.0", "Test", []cap.Cap{*identityCap})
	err = manifest.Validate()
	assert.NoError(t, err, "Manifest with CAP_IDENTITY must validate")
}

// TEST476: CapManifest.Validate() fails when CAP_IDENTITY is missing
func Test476_validate_fails_without_identity(t *testing.T) {
	specificUrn, err := urn.NewCapUrnFromString(manifestTestUrn("op=convert"))
	require.NoError(t, err)
	specificCap := cap.NewCap(specificUrn, "Convert", "convert")

	manifest := NewCapManifest("TestCartridge", "1.0.0", "Test", []cap.Cap{*specificCap})
	err = manifest.Validate()
	require.Error(t, err, "Manifest without CAP_IDENTITY must fail validation")
	assert.Contains(t, err.Error(), "CAP_IDENTITY")
}
