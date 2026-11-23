package standard_test

import (
	"testing"

	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST7243: every format conversion's URN is its published cap's: the JSON↔YAML
// patches carry the json-to-yaml and yaml-to-json markers their caps carry, so
// a request for either names a cap that the path's URN serves.
func Test7243_format_conversion_urns_are_the_published_caps(t *testing.T) {
	published := []string{
		`cap:convert-format;effect=patch;in="media:enc=utf8;fmt=json";json-to-yaml;out="media:enc=utf8;!ext;fmt=yaml"`,
		`cap:convert-format;effect=patch;in="media:enc=utf8;fmt=yaml";out="media:enc=utf8;!ext;fmt=json";yaml-to-json`,
		`cap:convert-format;in="media:enc=utf8;fmt=json;list;record";out="media:enc=utf8;fmt=csv;list;record"`,
		`cap:convert-format;in="media:enc=utf8;fmt=csv;list;record";out="media:enc=utf8;fmt=json;list;record"`,
		`cap:convert-format;in="media:enc=utf8;fmt=yaml;list;record";out="media:enc=utf8;fmt=csv;list;record"`,
		`cap:convert-format;in="media:enc=utf8;fmt=csv;list;record";out="media:enc=utf8;fmt=yaml;list;record"`,
		`cap:convert-format;in="media:enc=utf8;list";out="media:enc=utf8;fmt=json;list"`,
		`cap:convert-format;in="media:enc=utf8;fmt=json;list";out="media:enc=utf8;list"`,
		`cap:convert-format;in="media:enc=utf8;list";out="media:enc=utf8;fmt=yaml;list"`,
		`cap:convert-format;in="media:enc=utf8;fmt=yaml;list";out="media:enc=utf8;list"`,
		`cap:convert-format;in="media:enc=utf8;list";out="media:enc=utf8;fmt=csv;list;record"`,
		`cap:convert-format;in="media:enc=utf8;fmt=csv;list;record";out="media:enc=utf8;list"`,
	}
	paths := standard.AllFormatConversionPaths()
	require.Len(t, paths, len(published))
	for i, p := range paths {
		want, err := urn.NewCapUrnFromString(published[i])
		require.NoError(t, err)
		got, err := urn.NewCapUrnFromString(standard.FormatConversionUrn(p))
		require.NoError(t, err)
		assert.True(t, got.IsEquivalent(want), "%s is not the published %s", got, want)
	}
}
