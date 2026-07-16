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
		return `cap:in="media:void";out="media:fmt=json;record"`
	}
	return `cap:in="media:void";out="media:fmt=json;record";` + tags
}

// TEST148: Manifest creation with cap groups
func Test148_cap_manifest_creation(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", []string{"extract-metadata"})

	manifest := NewCapManifest("TestComponent", "0.1.0", "release",
		nil,
		"A test component for validation",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	assert.Equal(t, "TestComponent", manifest.Name)
	assert.Equal(t, "0.1.0", manifest.Version)
	assert.Equal(t, "release", manifest.Channel)
	assert.Equal(t, "A test component for validation", manifest.Description)
	assert.Len(t, manifest.CapGroups, 1)
	assert.Len(t, manifest.AllCaps(), 1)
	assert.Nil(t, manifest.Author)
}

// TEST149: Author field
func Test149_cap_manifest_with_author(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", []string{"extract-metadata"})

	manifest := NewCapManifest("TestComponent", "0.1.0", "release",
		nil,
		"A test component",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	).WithAuthor("Test Author")

	require.NotNil(t, manifest.Author)
	assert.Equal(t, "Test Author", *manifest.Author)
}

// TEST6363: Cap manifest with page u r l
func Test6363_CapManifestWithPageURL(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", []string{"extract-metadata"})

	manifest := NewCapManifest("TestComponent", "0.1.0", "release",
		nil,
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
	id, err := urn.NewCapUrnFromString(manifestTestUrn("extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", []string{"extract-metadata"})
	stdinUrn := "media:ext=pdf"
	capDef.AddArg(cap.CapArg{
		MediaUrn: standard.MediaIdentity,
		Required: true,
		Sources:  []cap.ArgSource{{Stdin: &stdinUrn}},
	})
	chunkDesc := "Chunk size"
	timestampDesc := "Include timestamps"
	chunkFlag := "--chunk-size"
	timestampFlag := "--timestamps"
	capDef.AddArg(cap.CapArg{
		MediaUrn:       "media:chunk-size;numeric",
		Required:       false,
		Sources:        []cap.ArgSource{{CliFlag: &chunkFlag}},
		ArgDescription: &chunkDesc,
		DefaultValue:   400,
		Metadata:       map[string]any{"unit": "words"},
	})
	capDef.AddArg(cap.CapArg{
		MediaUrn:       "media:timestamps;bool;enc=utf-8",
		Required:       false,
		Sources:        []cap.ArgSource{{CliFlag: &timestampFlag}},
		ArgDescription: &timestampDesc,
		DefaultValue:   false,
	})

	manifest := NewCapManifest("TestComponent", "0.1.0", "release",
		nil,
		"A test component",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	).WithAuthor("Test Author")

	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)

	jsonStr := string(jsonData)
	assert.Contains(t, jsonStr, `"name":"TestComponent"`)
	assert.Contains(t, jsonStr, `"author":"Test Author"`)
	assert.Contains(t, jsonStr, `"cap_groups"`)
	assert.Contains(t, jsonStr, `"default_value":400`)
	assert.Contains(t, jsonStr, `"default_value":false`)

	var deserialized CapManifest
	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	assert.Equal(t, manifest.Name, deserialized.Name)
	assert.Len(t, deserialized.AllCaps(), len(manifest.AllCaps()))
	decodedCap := deserialized.AllCaps()[0]
	assert.Equal(t, json.Number("400"), decodedCap.Args[1].DefaultValue)
	assert.Equal(t, map[string]any{"unit": "words"}, decodedCap.Args[1].Metadata)
	assert.Equal(t, false, decodedCap.Args[2].DefaultValue)
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
	id1, err := urn.NewCapUrnFromString(manifestTestUrn("extract;target=metadata"))
	require.NoError(t, err)
	cap1 := cap.NewCap(id1, "Metadata Extractor", []string{"extract-metadata"})

	id2, err := urn.NewCapUrnFromString(manifestTestUrn("extract;target=outline"))
	require.NoError(t, err)
	metadata := map[string]string{"supports_outline": "true"}
	cap2 := cap.NewCapWithMetadata(id2, "Outline Extractor", []string{"extract-outline"}, metadata)

	manifest := NewCapManifest("MultiCapComponent", "1.0.0", "release",
		nil,
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
	manifest := NewCapManifest("EmptyComponent", "1.0.0", "release",
		nil,
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
	id, err := urn.NewCapUrnFromString(manifestTestUrn("validate;file"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "File Validator", []string{"validate"})

	manifest := NewCapManifest("ValidatorComponent", "1.0.0", "release",
		nil,
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
		"release",
		nil,
		"Test component",
		tc.capGroups,
	)
}

func (tc *testComponent) Caps() []cap.Cap {
	return tc.ComponentManifest().AllCaps()
}

// TEST155: ComponentMetadata trait
func Test155_component_metadata_interface(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("test;type=component"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "Test Component", []string{"test"})

	component := &testComponent{
		name:      "TestImpl",
		capGroups: []CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	}

	caps := component.Caps()
	assert.Len(t, caps, 1)
	assert.Contains(t, caps[0].UrnString(), "test")
}

// TEST6367: Cap manifest validation
func Test6367_CapManifestValidation(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("extract;target=metadata"))
	require.NoError(t, err)

	capDef := cap.NewCap(id, "Metadata Extractor", []string{"extract-metadata"})
	stdinUrn := "media:ext=pdf"
	capDef.AddArg(cap.CapArg{
		MediaUrn: standard.MediaIdentity,
		Required: true,
		Sources:  []cap.ArgSource{{Stdin: &stdinUrn}},
	})

	manifest := NewCapManifest("ValidComponent", "1.0.0", "release",
		nil,
		"Valid component for testing",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	assert.NotEmpty(t, manifest.Name)
	assert.NotEmpty(t, manifest.Version)
	assert.NotEmpty(t, manifest.Description)
	assert.NotNil(t, manifest.CapGroups)

	all := manifest.AllCaps()
	assert.Len(t, all, 1)
	assert.Equal(t, "extract-metadata", all[0].PrimaryAlias())
	assert.True(t, all[0].AcceptsStdin())
}

// TEST6371: Cap manifest compatibility
func Test6371_CapManifestCompatibility(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("process"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "Data Processor", []string{"process"})

	cartridgeStyleManifest := NewCapManifest("CartridgeComponent", "0.1.0", "release",
		nil,
		"Cartridge-style component",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	cartridgeStyleManifest2 := NewCapManifest("CartridgeComponent2", "0.1.0", "release",
		nil,
		"Cartridge-style component 2",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	cartridgeJSON, err := json.Marshal(cartridgeStyleManifest)
	require.NoError(t, err)

	cartridgeJSON2, err := json.Marshal(cartridgeStyleManifest2)
	require.NoError(t, err)

	var cartridgeMap map[string]interface{}
	var cartridgeMap2 map[string]interface{}

	err = json.Unmarshal(cartridgeJSON, &cartridgeMap)
	require.NoError(t, err)

	err = json.Unmarshal(cartridgeJSON2, &cartridgeMap2)
	require.NoError(t, err)

	assert.Equal(t, len(cartridgeMap), len(cartridgeMap2))
	assert.Contains(t, cartridgeMap, "name")
	assert.Contains(t, cartridgeMap, "version")
	assert.Contains(t, cartridgeMap, "description")
	assert.Contains(t, cartridgeMap, "cap_groups")
}

// TEST475: validate() passes with CAP_IDENTITY in a cap group
func Test475_validate_passes_with_identity(t *testing.T) {
	identityUrn, err := urn.NewCapUrnFromString(standard.CapIdentity)
	require.NoError(t, err)
	identityCap := cap.NewCap(identityUrn, "Identity", []string{"identity"})

	manifest := NewCapManifest("TestCartridge", "1.0.0", "release", nil, "Test", []CapGroup{DefaultGroup([]cap.Cap{*identityCap})})
	err = manifest.Validate()
	assert.NoError(t, err, "Manifest with CAP_IDENTITY must validate")
}

// TEST476: validate() fails without CAP_IDENTITY
func Test476_validate_fails_without_identity(t *testing.T) {
	specificUrn, err := urn.NewCapUrnFromString(manifestTestUrn("convert"))
	require.NoError(t, err)
	specificCap := cap.NewCap(specificUrn, "Convert", []string{"convert"})

	manifest := NewCapManifest("TestCartridge", "1.0.0", "release", nil, "Test", []CapGroup{DefaultGroup([]cap.Cap{*specificCap})})
	err = manifest.Validate()
	require.Error(t, err, "Manifest without CAP_IDENTITY must fail validation")
	assert.Contains(t, err.Error(), "CAP_IDENTITY")
}

// TEST1284: Cap group with adapter URNs serializes and deserializes correctly
func Test1284_cap_group_with_adapter_urns(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("convert"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "Convert", []string{"convert"})

	group := CapGroup{
		Name:        "data-formats",
		Caps:        []cap.Cap{*capDef},
		AdapterUrns: []string{"media:fmt=json", "media:fmt=csv"},
	}

	manifest := NewCapManifest("TestCartridge", "1.0.0", "release", nil, "Test", []CapGroup{group})

	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)

	jsonStr := string(jsonData)
	assert.Contains(t, jsonStr, `"adapter_urns"`)
	assert.Contains(t, jsonStr, "media:fmt=json")
	assert.Contains(t, jsonStr, "media:fmt=csv")

	var deserialized CapManifest
	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)
	assert.Len(t, deserialized.CapGroups[0].AdapterUrns, 2)
}

// TEST1872: `registry_url_from_build_env` passes a non-empty registry URL through unchanged. This is the function that decides the engine's baked PRIMARY registry (surfaced over SystemService.HealthStatus); a published build must report exactly the URL it was compiled with.
func Test1872_registry_url_from_build_env_passes_through_nonempty(t *testing.T) {
	url := "https://cartridges.machinefabric.com/manifest"
	got := RegistryURLFromBuildEnv(&url)
	require.NotNil(t, got)
	assert.Equal(t, url, *got)
	// Passthrough, not a re-allocated copy: the returned pointer must carry the
	// exact value handed in.
	assert.Same(t, &url, got)
}

// TEST1873: an unset build-env value (nil) yields nil — a dev build has no baked
// registry, so the engine reports an empty primary-registry URL and loads only
// `dev/` cartridges (mirror of Rust test1873).
func Test1873_registry_url_from_build_env_none_for_dev(t *testing.T) {
	assert.Nil(t, RegistryURLFromBuildEnv(nil))
}

// TEST1874: an exported-but-empty value (a pointer to "") is neither a dev build
// nor a valid identity and MUST fail hard, so the build can never silently hash
// the empty string into a fake registry slug. We assert the panic AND its exact
// message, so a regression that dropped the check (or replaced it with a silent
// fallback) is caught rather than passing on a bogus empty primary registry
// (mirror of Rust test1874).
func Test1874_registry_url_from_build_env_rejects_empty_string(t *testing.T) {
	empty := ""
	assert.PanicsWithValue(t,
		"MFR_CARTRIDGE_REGISTRY_URL must be unset for dev builds or set to a non-empty registry URL for published builds; empty string is invalid",
		func() { RegistryURLFromBuildEnv(&empty) },
	)
}

// TEST117: A manifest's channel round-trips through serde and the serialized form uses the canonical lowercase wire word ("release" / "nightly"). A missing or unrecognized channel is a hard parse error — no defaults.
func Test117_cap_manifest_channel_roundtrip(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("extract;target=metadata"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "Extract Metadata", []string{"extract-metadata"})

	registryURL := "https://cartridges.machinefabric.com/manifest"
	manifest := NewCapManifest("TestComponent", "0.1.0", string(CartridgeChannelNightly),
		&registryURL,
		"Channel round-trip",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)
	jsonStr := string(jsonData)
	assert.Contains(t, jsonStr, `"channel":"nightly"`,
		"expected lowercase wire form, got: %s", jsonStr)
	// registry_url round-trips as the exact string the operator typed —
	// used to validate against the on-disk slug at scan time, so a single
	// byte of drift here would silently break discovery.
	assert.Contains(t, jsonStr, `"registry_url":"https://cartridges.machinefabric.com/manifest"`,
		"expected verbatim registry_url in serialized form, got: %s", jsonStr)

	var parsed CapManifest
	err = json.Unmarshal(jsonData, &parsed)
	require.NoError(t, err)
	assert.Equal(t, string(CartridgeChannelNightly), parsed.Channel)
	require.NotNil(t, parsed.RegistryURL)
	assert.Equal(t, "https://cartridges.machinefabric.com/manifest", *parsed.RegistryURL)

	// No-channel JSON must fail to parse.
	noChannel := `{"name":"X","version":"1.0.0","registry_url":null,"description":"x","cap_groups":[]}`
	err = json.Unmarshal([]byte(noChannel), &CapManifest{})
	assert.Error(t, err, "manifest without `channel` must fail to parse")

	// No-registry_url JSON must fail to parse — the field is required-but-
	// nullable, so a missing key means an old SDK, which can't be trusted
	// to know the new schema.
	noRegistry := `{"name":"X","version":"1.0.0","channel":"nightly","description":"x","cap_groups":[]}`
	err = json.Unmarshal([]byte(noRegistry), &CapManifest{})
	assert.Error(t, err, "manifest without `registry_url` must fail to parse")

	// Bogus channel string must fail.
	bogus := `{"name":"X","version":"1.0.0","channel":"staging","registry_url":null,"description":"x","cap_groups":[]}`
	err = json.Unmarshal([]byte(bogus), &CapManifest{})
	assert.Error(t, err, "manifest with channel='staging' must fail to parse")
}

// TEST118: A dev manifest (built without `MFR_CARTRIDGE_REGISTRY_URL`) carries `registry_url: null` and serializes the field explicitly. The null-vs-absent distinction matters because the parser refuses to accept absent (test117) — so an old SDK can't accidentally pass for a dev build.
func Test118_dev_manifest_registry_url_is_explicit_null(t *testing.T) {
	id, err := urn.NewCapUrnFromString(manifestTestUrn("dev"))
	require.NoError(t, err)
	capDef := cap.NewCap(id, "Dev", []string{"dev"})

	manifest := NewCapManifest("DevComponent", "0.1.0", string(CartridgeChannelNightly),
		nil,
		"Dev build",
		[]CapGroup{DefaultGroup([]cap.Cap{*capDef})},
	)

	jsonData, err := json.Marshal(manifest)
	require.NoError(t, err)
	assert.Contains(t, string(jsonData), `"registry_url":null`,
		"dev manifest must serialize registry_url=null explicitly, got: %s", string(jsonData))

	var parsed CapManifest
	err = json.Unmarshal(jsonData, &parsed)
	require.NoError(t, err)
	assert.Nil(t, parsed.RegistryURL)
}
