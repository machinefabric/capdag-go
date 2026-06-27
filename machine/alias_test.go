package machine

import (
	"encoding/json"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/media"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shared test numbers (1880-1892) test the same behavior, with the same
// method, across every capdag implementation. See capdag/src/fabric/alias.rs,
// capdag/src/fabric/registry.rs, and capdag/src/machine/parser.rs.

// Test1880: alias name normalization lowercases and accepts the allowed char
// class; rejects colon, whitespace, and out-of-class chars.
func Test1880_AliasNameNormalizationRules(t *testing.T) {
	got, err := media.NormalizeAliasName("JSONDoc")
	require.NoError(t, err)
	assert.Equal(t, "jsondoc", got)

	got, err = media.NormalizeAliasName("my.alias-1_x")
	require.NoError(t, err)
	assert.Equal(t, "my.alias-1_x", got)

	for _, bad := range []string{"", "pdf:text", "my alias", "a/b"} {
		_, err := media.NormalizeAliasName(bad)
		assert.Error(t, err, "expected error for %q", bad)
	}
}

// Test1881: URN-vs-alias detection keys purely on the presence of ':'.
func Test1881_TokenURNvsAliasDetection(t *testing.T) {
	assert.True(t, media.TokenIsURN(`cap:in="media:ext=pdf";extract;out="media:enc=utf-8"`))
	assert.True(t, media.TokenIsURN("media:fmt=json;record"))
	assert.False(t, media.TokenIsURN("pdf2text"))
	assert.True(t, media.IsAliasToken("pdf2text"))
	assert.False(t, media.IsAliasToken("media:enc=utf-8"))
}

// Test1882: alias target classification distinguishes cap from media by
// prefix and rejects a non-URN target.
func Test1882_ClassifyAliasTargetByPrefix(t *testing.T) {
	k, ok := media.ClassifyAliasTarget("media:fmt=json;record")
	require.True(t, ok)
	assert.Equal(t, media.AliasTargetMedia, k)

	k, ok = media.ClassifyAliasTarget(`cap:effect=patch;in="media:image";name;out="media:ext=png;image"`)
	require.True(t, ok)
	assert.Equal(t, media.AliasTargetCap, k)

	_, ok = media.ClassifyAliasTarget("not-a-urn")
	assert.False(t, ok)
}

// Test1887: the Manifest type round-trips an `aliases` map.
func Test1887_ManifestSerdeRoundTripsAliases(t *testing.T) {
	body := `{"version":1,"previous":0,"caps":{},"media":{},"aliases":{"pdf2text":3,"jsondoc":1}}`
	var m media.Manifest
	require.NoError(t, json.Unmarshal([]byte(body), &m))
	assert.Equal(t, uint32(3), m.Aliases["pdf2text"])
	assert.Equal(t, uint32(1), m.Aliases["jsondoc"])

	out, err := json.Marshal(&m)
	require.NoError(t, err)
	var back map[string]any
	require.NoError(t, json.Unmarshal(out, &back))
	aliases := back["aliases"].(map[string]any)
	assert.Equal(t, float64(3), aliases["pdf2text"])
	assert.Equal(t, float64(1), aliases["jsondoc"])
}

// Test1888: resolve alias returns the alias target untyped; case-insensitive; malformed name rejected.
func Test1888_ResolveAliasReturnsTarget(t *testing.T) {
	reg := cap.NewFabricRegistryForTest()
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "jsondoc", Target: "media:fmt=json;record", Version: 1})

	target, err := reg.ResolveAlias("jsondoc")
	require.NoError(t, err)
	assert.Equal(t, "media:fmt=json;record", target)

	target, err = reg.ResolveAlias("JSONDoc")
	require.NoError(t, err)
	assert.Equal(t, "media:fmt=json;record", target)

	_, err = reg.ResolveAlias("bad:name")
	assert.Error(t, err)
}

// Test1889: resolve alias typed enforces the expected kind.
func Test1889_ResolveAliasTypedEnforcesKind(t *testing.T) {
	reg := cap.NewFabricRegistryForTest()
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "jsondoc", Target: "media:fmt=json;record", Version: 1})

	_, err := reg.ResolveAliasTyped("jsondoc", media.AliasTargetMedia)
	assert.NoError(t, err)
	_, err = reg.ResolveAliasTyped("jsondoc", "")
	assert.NoError(t, err)
	_, err = reg.ResolveAliasTyped("jsondoc", media.AliasTargetCap)
	assert.Error(t, err, "a media alias demanded as a cap must fail hard")
}

// Test1890: GetCap accepts a cap alias and returns the aliased cap; a media
// alias passed to GetCap fails hard (typed boundary).
func Test1890_GetCapViaAliasAndTypeMismatch(t *testing.T) {
	reg := cap.NewFabricRegistryForTest()
	c := buildCap(`cap:extract;in="media:ext=pdf";out="media:enc=utf-8"`, "extract", []string{"media:ext=pdf"}, "media:enc=utf-8")
	canonical := c.UrnString()
	reg.AddCapsToCache([]*cap.Cap{c})
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "pdf2text", Target: canonical, Version: 1})

	got, err := reg.GetCap("pdf2text")
	require.NoError(t, err)
	assert.Equal(t, canonical, got.UrnString())

	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "jsondoc", Target: "media:fmt=json;record", Version: 1})
	_, err = reg.GetCap("jsondoc")
	assert.Error(t, err, "a media alias at GetCap must fail hard")
}

// Test1891: GetMediaDef accepts a media alias and returns the aliased spec; a
// cap alias passed to GetMediaDef fails hard.
func Test1891_GetMediaDefViaAliasAndTypeMismatch(t *testing.T) {
	reg, err := media.NewFabricRegistryForTest()
	require.NoError(t, err)
	reg.AddSpec(media.StoredMediaDef{Urn: "media:fmt=json;record", MediaType: "application/json", Title: "JSON"})
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "jsondoc", Target: "media:fmt=json;record", Version: 1})

	spec, err := reg.GetMediaDef("jsondoc")
	require.NoError(t, err)
	assert.Equal(t, "media:fmt=json;record", spec.Urn)

	reg.InsertCachedAliasForTest(media.StoredAlias{
		Name:    "pdf2text",
		Target:  `cap:extract;in="media:ext=pdf";out="media:enc=utf-8"`,
		Version: 1,
	})
	_, err = reg.GetMediaDef("pdf2text")
	assert.Error(t, err, "a cap alias at GetMediaDef must fail hard")
}

// Test1892: an unknown alias name is a hard not-found, never a silent empty.
func Test1892_UnknownAliasIsNotFound(t *testing.T) {
	reg := cap.NewFabricRegistryForTest()
	_, err := reg.GetAlias("nosuchalias")
	assert.Error(t, err)
	_, err = reg.AliasDefverFor("nosuchalias")
	assert.Error(t, err)
	_, ok := reg.ResolveAliasCached("nosuchalias")
	assert.False(t, ok)
	_, ok = reg.ResolveAliasCached("bad:name")
	assert.False(t, ok)
}

// ----- machine notation cap aliases (1883-1886) -----

func extractWithAliasRegistry() (*cap.FabricRegistry, string) {
	extractUrn := `cap:extract;in="media:ext=pdf";out="media:enc=utf-8;ext=txt"`
	c := buildCap(extractUrn, "extract", []string{"media:ext=pdf"}, "media:enc=utf-8;ext=txt")
	canonical := c.UrnString()
	reg := cap.NewFabricRegistryForTest()
	reg.AddCapsToCache([]*cap.Cap{c})
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "pdf2text", Target: canonical, Version: 1})
	return reg, canonical
}

// Test1883: a cap-position name with no local header resolves as a cap alias.
func Test1883_CapPositionAliasResolvesToCap(t *testing.T) {
	reg, canonical := extractWithAliasRegistry()
	m, perr := ParseMachine("[doc -> pdf2text -> txt]", reg)
	require.Nil(t, perr)
	require.Equal(t, 1, m.StrandCount())
	strand := m.Strands()[0]
	require.Len(t, strand.Edges(), 1)
	assert.Equal(t, canonical, strand.Edges()[0].CapUrn.String())
}

// Test1884: a local header alias shadows a fabric alias of the same name.
func Test1884_LocalHeaderShadowsCapAlias(t *testing.T) {
	reg, _ := extractWithAliasRegistry()
	otherUrn := `cap:other;in="media:ext=pdf";out="media:enc=utf-8;ext=txt"`
	other := buildCap(otherUrn, "other", []string{"media:ext=pdf"}, "media:enc=utf-8;ext=txt")
	otherCanonical := other.UrnString()
	reg.AddCapsToCache([]*cap.Cap{other})
	notation := "[pdf2text " + otherUrn + "]\n[doc -> pdf2text -> txt]"
	m, perr := ParseMachine(notation, reg)
	require.Nil(t, perr)
	assert.Equal(t, otherCanonical, m.Strands()[0].Edges()[0].CapUrn.String())
}

// Test1885: a cap-position alias that resolves to a MEDIA URN is a hard error.
func Test1885_CapPositionAliasToMediaIsError(t *testing.T) {
	c := buildCap(`cap:extract;in="media:ext=pdf";out="media:enc=utf-8;ext=txt"`, "extract", []string{"media:ext=pdf"}, "media:enc=utf-8;ext=txt")
	reg := cap.NewFabricRegistryForTest()
	reg.AddCapsToCache([]*cap.Cap{c})
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "jsondoc", Target: "media:fmt=json;record", Version: 1})
	_, perr := ParseMachine("[doc -> jsondoc -> out]", reg)
	require.NotNil(t, perr)
	require.NotNil(t, perr.Syntax)
	assert.Equal(t, ErrAliasNotACap, perr.Syntax.Kind)
}

// Test1886: a cap-position name that is neither a local header nor a
// registered alias raises the undefined-alias error.
func Test1886_UnregisteredCapNameIsUndefinedAlias(t *testing.T) {
	reg, _ := extractWithAliasRegistry()
	_, perr := ParseMachine("[doc -> nosuchalias -> out]", reg)
	require.NotNil(t, perr)
	require.NotNil(t, perr.Syntax)
	assert.Equal(t, ErrUndefinedAlias, perr.Syntax.Kind)
}
