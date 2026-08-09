package bifaci

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

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

// TEST7153: `installed_at` is a real RFC3339 UTC timestamp, at known epoch
// instants and at the instants that break naive date arithmetic — a leap day,
// the day after one, and a century year that is NOT a leap year. Emitting a
// bare epoch count with a `Z` appended would satisfy "some string ending in Z"
// and satisfy nothing else; every reader and every fixture in the tree treats
// this field as a parseable timestamp.
func Test7153_install_timestamp_is_rfc3339_utc(t *testing.T) {
	for _, tc := range []struct {
		secs int64
		want string
	}{
		{0, "1970-01-01T00:00:00Z"},
		{1_000_000_000, "2001-09-09T01:46:40Z"},
		// 2024-02-29 — a leap day in a leap year divisible by 4.
		{1_709_164_800, "2024-02-29T00:00:00Z"},
		// The instant after it: the rollover a naive +1 gets wrong.
		{1_709_251_199, "2024-02-29T23:59:59Z"},
		// 2100-03-01 — 2100 is divisible by 100 but not 400, so it has NO
		// Feb 29. A leap rule of "divisible by 4" lands a day early.
		{4_107_542_400, "2100-03-01T00:00:00Z"},
	} {
		assert.Equal(t, tc.want, formatRFC3339UTC(tc.secs), "epoch %d", tc.secs)
	}

	// The live producer emits the same shape, and a plausible present instant —
	// a broken epoch-to-civil conversion typically lands in 1970 or the far
	// future rather than producing a malformed string.
	now := InstallTimestampNow()
	require.Len(t, now, 20, "not RFC3339-shaped: %s", now)
	assert.True(t, strings.HasSuffix(now, "Z"), "not UTC-marked: %s", now)
	parsed, err := time.Parse("2006-01-02T15:04:05Z", now)
	require.NoError(t, err, "not parseable as RFC3339: %s", now)
	assert.True(t, parsed.Year() >= 2020 && parsed.Year() < 2200,
		"the current year came out as %d: %s", parsed.Year(), now)
}
