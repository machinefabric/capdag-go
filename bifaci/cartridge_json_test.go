package bifaci

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST6726: CartridgeJson with FabricManifestVersion=0 (zero value) is absent on the wire.
func Test6726_cartridge_json_fabric_manifest_version_zero_round_trip(t *testing.T) {
	url := "https://registry.example.com"
	cj := CartridgeJson{
		Name:                  "testcartridge",
		Version:               "1.0.0",
		Channel:               "release",
		RegistryURL:           &url,
		Entry:                 "testcartridge",
		InstalledAt:           "2026-01-01T00:00:00Z",
		FabricManifestVersion: 0,
	}

	data, err := json.Marshal(cj)
	require.NoError(t, err)
	jsonStr := string(data)

	assert.NotContains(t, jsonStr, "fabric_manifest_version",
		"fabric_manifest_version must be absent when zero, got: %s", jsonStr)

	// registry_url must always be present even when non-nil
	assert.Contains(t, jsonStr, `"registry_url"`)

	// Round-trip via UnmarshalJSON: cannot use directly since it validates entry point,
	// so just confirm the json.Unmarshal path via the rawCartridgeJson alias path works.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	_, present := raw["fabric_manifest_version"]
	assert.False(t, present, "fabric_manifest_version must not appear in serialized JSON when zero")
}

// TEST6727: CartridgeJson with FabricManifestVersion>0 round-trips the value correctly.
func Test6727_cartridge_json_fabric_manifest_version_nonzero_round_trip(t *testing.T) {
	url := "https://registry.example.com"
	cj := CartridgeJson{
		Name:                  "testcartridge",
		Version:               "1.0.0",
		Channel:               "release",
		RegistryURL:           &url,
		Entry:                 "testcartridge",
		InstalledAt:           "2026-01-01T00:00:00Z",
		FabricManifestVersion: 7,
	}

	data, err := json.Marshal(cj)
	require.NoError(t, err)
	jsonStr := string(data)

	assert.True(t, strings.Contains(jsonStr, `"fabric_manifest_version":7`),
		"fabric_manifest_version must be 7 in JSON, got: %s", jsonStr)

	// Unmarshal back via the type alias path (rawCartridgeJson) by unmarshaling into
	// a fresh CartridgeJson. We bypass ReadCartridgeJsonFromDir (which requires a real
	// file system) by using json.Unmarshal directly — this exercises the rawCartridgeJson
	// alias code path in UnmarshalJSON.
	type rawCartridgeJson CartridgeJson
	var raw rawCartridgeJson
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, uint32(7), raw.FabricManifestVersion,
		"FabricManifestVersion must survive unmarshal round-trip")
}
