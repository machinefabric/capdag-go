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

// TEST148: Manifest creation with cap groups
func Test148_cap_manifest_creation(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", "extract-metadata")

	manifest := NewCapManifest(
		"TestComponent",
		"0.1.0",
		"A test component for validation",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	assert.Equal(t, "TestComponent", manifest.Name)
	assert.Equal(t, "0.1.0", manifest.Version)
	assert.Equal(t, "A test component for validation", manifest.Description)
	assert.Len(t, manifest.CapGroups, 1)
	assert.Len(t, manifest.AllCaps(), 1)
	assert.Nil(t, manifest.Author)
}

// TEST149: Author field
func Test149_cap_manifest_with_author(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", "extract-metadata")

	manifest := NewCapManifest(
		"TestComponent",
		"0.1.0",
		"A test component",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
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
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	).WithAuthor("Test Author").WithPageUrl("https://github.com/example/test")

	require.NotNil(t, manifest.PageUrl)
	assert.Equal(t, "https://github.com/example/test", *manifest.PageUrl)

	// Verify it serializes correctly
	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)
	jsonStr := string(jsonData)
	assert.Contains(t, jsonStr, `"page_url":"https://github.com/example/test"`)
}

// TEST150: JSON roundtrip
func Test150_cap_manifest_json_serialization(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", "extract-metadata")
	stdinUrn := "media:pdf"
	capDef.AddArg(cap.CapArg{
		MediaUrn: standard.MediaIdentity,
		Required: true,
		Sources:  []cap.ArgSource{{Stdin: &stdinUrn}},
	})

	manifest := NewCapManifest(
		"TestComponent",
		"0.1.0",
		"A test component",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	).WithAuthor("Test Author")

	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)

	jsonStr := string(jsonData)
	assert.Contains(t, jsonStr, `"name":"TestComponent"`)
	assert.Contains(t, jsonStr, `"author":"Test Author"`)
	assert.Contains(t, jsonStr, `"cap_groups"`)

	var deserialized CapManifest
	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	assert.Equal(t, manifest.Name, deserialized.Name)
	assert.Len(t, deserialized.AllCaps(), len(manifest.AllCaps()))
}

// TEST151: Missing required fields fail
func Test151_cap_manifest_required_fields(t *testing.T) {
	// Test that invalid JSON fails
	invalidJSON := `{"name": "TestComponent", invalid`
	var result CapManifest
	err := json.Unmarshal([]byte(invalidJSON), &result)
	assert.Error(t, err)
}

// TEST152: Multiple caps across groups
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
		[]CapGroup{DefaultGroup([]cap.Cap{*cap1, *cap2})},
	)

	all := manifest.AllCaps()
	assert.Len(t, all, 2)
	assert.Contains(t, all[0].UrnString(), "target=metadata")
	assert.Contains(t, all[1].UrnString(), "target=outline")
	assert.True(t, all[1].HasMetadata("supports_outline"))
}

// TEST153: Empty cap groups
func Test153_cap_manifest_empty_cap_groups(t *testing.T) {
	manifest := NewCapManifest(
		"EmptyComponent",
		"1.0.0",
		"Component with no caps",
		[]CapGroup{},
	)

	assert.Len(t, manifest.AllCaps(), 0)

	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)

	var deserialized CapManifest
	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)
	assert.Len(t, deserialized.AllCaps(), 0)
}

// TEST154: Optional author field omitted in serialization
func Test154_cap_manifest_optional_fields(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=validate;file"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "File Validator", "validate")

	manifest := NewCapManifest(
		"ValidatorComponent",
		"1.0.0",
		"File validation component",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)

	jsonStr := string(jsonData)
	assert.NotContains(t, jsonStr, `"author"`)
	assert.NotContains(t, jsonStr, `"page_url"`)
}

// Test component that implements ComponentMetadata interface
type testComponent struct {
	name      string
	capGroups []CapGroup
}

// Implement the ComponentMetadata interface
func (tc *testComponent) ComponentManifest() *CapManifest {
	return NewCapManifest(
		tc.name,
		"1.0.0",
		"Test component",
		tc.capGroups,
	)
}

func (tc *testComponent) Caps() []cap.Cap {
	return tc.ComponentManifest().AllCaps()
}

// TEST155: ComponentMetadata interface
func Test155_component_metadata_interface(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=test;type=component"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "Test Component", "test")

	component := &testComponent{
		name:      "TestImpl",
		capGroups: []CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	}

	caps := component.Caps()
	assert.Len(t, caps, 1)
	assert.Contains(t, caps[0].UrnString(), "op=test")
}

func TestCapManifestValidation(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", "extract-metadata")
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
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	assert.NotEmpty(t, manifest.Name)
	assert.NotEmpty(t, manifest.Version)
	assert.NotEmpty(t, manifest.Description)
	assert.NotNil(t, manifest.CapGroups)

	all := manifest.AllCaps()
	assert.Len(t, all, 1)
	assert.Equal(t, "extract-metadata", all[0].Command)
	assert.True(t, all[0].AcceptsStdin())
}

func TestCapManifestCompatibility(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=process"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "Data Processor", "process")

	cartridgeStyleManifest := NewCapManifest(
		"CartridgeComponent",
		"0.1.0",
		"Cartridge-style component",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	providerStyleManifest := NewCapManifest(
		"ProviderComponent",
		"0.1.0",
		"Provider-style component",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	cartridgeJSON, err := json.Marshal(cartridgeStyleManifest)
	require.NoError(t, err)

	providerJSON, err := json.Marshal(providerStyleManifest)
	require.NoError(t, err)

	var cartridgeMap map[string]interface{}
	var providerMap map[string]interface{}

	err = json.Unmarshal(cartridgeJSON, &cartridgeMap)
	require.NoError(t, err)

	err = json.Unmarshal(providerJSON, &providerMap)
	require.NoError(t, err)

	assert.Equal(t, len(cartridgeMap), len(providerMap))
	assert.Contains(t, cartridgeMap, "name")
	assert.Contains(t, cartridgeMap, "version")
	assert.Contains(t, cartridgeMap, "description")
	assert.Contains(t, cartridgeMap, "cap_groups")
}

// TEST475: validate() passes with CAP_IDENTITY in a cap group
func Test475_validate_passes_with_identity(t *testing.T) {
	identityUrn, err := urn.NewCapUrnFromString(standard.CapIdentity)
	require.NoError(t, err)
	identityCap := cap.NewCap(identityUrn, "Identity", "identity")

	manifest := NewCapManifest("TestCartridge", "1.0.0", "Test", []CapGroup{DefaultGroup([]cap.Cap{*identityCap})})
	err = manifest.Validate()
	assert.NoError(t, err, "Manifest with CAP_IDENTITY must validate")
}

// TEST476: validate() fails without CAP_IDENTITY
func Test476_validate_fails_without_identity(t *testing.T) {
	specificUrn, err := urn.NewCapUrnFromString(manifestTestUrn("op=convert"))
	require.NoError(t, err)
	specificCap := cap.NewCap(specificUrn, "Convert", "convert")

	manifest := NewCapManifest("TestCartridge", "1.0.0", "Test", []CapGroup{DefaultGroup([]cap.Cap{*specificCap})})
	err = manifest.Validate()
	require.Error(t, err, "Manifest without CAP_IDENTITY must fail validation")
	assert.Contains(t, err.Error(), "CAP_IDENTITY")
}

// TEST1284: Cap group with adapter URNs serializes and deserializes correctly
func Test1284_cap_group_with_adapter_urns(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("op=convert"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "Convert", "convert")

	group := CapGroup{
		Name:        "data-formats",
		Caps:        []cap.Cap{*capDef},
		AdapterUrns: []string{"media:json", "media:csv"},
	}

	manifest := NewCapManifest("TestCartridge", "1.0.0", "Test", []CapGroup{group})

	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)

	jsonStr := string(jsonData)
	assert.Contains(t, jsonStr, `"adapter_urns"`)
	assert.Contains(t, jsonStr, "media:json")
	assert.Contains(t, jsonStr, "media:csv")

	var deserialized CapManifest
	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)
	assert.Len(t, deserialized.CapGroups[0].AdapterUrns, 2)
}
