package machine

import (
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/planner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shared test numbers (1880-1892) test the same behavior, with the same
// method, across every capdag implementation. See capdag/src/fabric/alias.rs,
// capdag/src/fabric/registry.rs, and capdag/src/machine/parser.rs.

// TEST1880: alias name normalization lowercases and accepts the allowed character class; rejects colon, whitespace, and out-of-class chars with the right error. A broken validator would let a URN-shaped or whitespace name through, or mangle a valid name.
// TEST1880: alias name normalization lowercases and accepts the allowed character class; rejects colon, whitespace, and out-of-class chars with the right error. A broken validator would let a URN-shaped or whitespace name through, or mangle a valid name.
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

// TEST1881: URN-vs-alias detection keys purely on the presence of ':'. The whole design rests on this discriminator being exact.
func Test1881_TokenURNvsAliasDetection(t *testing.T) {
	assert.True(t, media.TokenIsURN(`cap:in="media:ext=pdf";extract;out="media:enc=utf-8"`))
	assert.True(t, media.TokenIsURN("media:fmt=json;record"))
	assert.False(t, media.TokenIsURN("pdf2text"))
	assert.True(t, media.IsAliasToken("pdf2text"))
	assert.False(t, media.IsAliasToken("media:enc=utf-8"))
}

// TEST1882: alias target classification distinguishes cap from media by prefix and rejects a non-URN target. The typed-boundary enforcement in the registry depends on this.
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
	reg := fabric.NewForTest()
	reg.AddCapsToCache([]*cap.Cap{c})
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "jsondoc", Target: "media:fmt=json;record", Version: 1})
	_, perr := ParseMachine("[doc -> jsondoc -> out]", reg)
	require.NotNil(t, perr)
	require.NotNil(t, perr.Syntax)
	assert.Equal(t, ErrAliasNotACap, perr.Syntax.Kind)
}

// TEST1886: a cap-position name that is neither a local header nor a registered alias still raises UndefinedAlias. The alias mechanism must not mask a genuinely undefined name.
func Test1886_UnregisteredCapNameIsUndefinedAlias(t *testing.T) {
	reg, _ := extractWithAliasRegistry()
	_, perr := ParseMachine("[doc -> nosuchalias -> out]", reg)
	require.NotNil(t, perr)
	require.NotNil(t, perr.Syntax)
	assert.Equal(t, ErrUndefinedAlias, perr.Syntax.Kind)
}

// TEST1196: ToMachineNotationAliased renders a cap by its registered display
// alias (shortest, then alphabetical), referencing it DIRECTLY in the wiring
// with NO header, while a cap with no alias keeps its synthetic edge_N header +
// raw URN; the result round-trips back to the same machine (the parser resolves
// the alias from the warm cache). Mirrors Rust serializer::test1196.
func Test1196_AliasedSerializationUsesAliasAndRoundTrips(t *testing.T) {
	extract := extractCapDef()
	embed := embedCapDef()
	reg := registryWith([]*cap.Cap{extract, embed})

	// Two aliases on the extract cap; "ex" is shorter than "extract-pdf". The
	// target is the cap's canonical URN string so the reverse lookup matches.
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "extract-pdf", Target: extract.UrnString(), Version: 1})
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "ex", Target: extract.UrnString(), Version: 1})
	// No alias for the embed cap → it must stay a raw URN.

	// Build a pdf -> txt -> vec machine spanning both caps.
	strand := strandFromSteps(
		[]*planner.StrandStep{
			capStep(extract.UrnString(), "extract", "media:ext=pdf", `media:txt;enc=utf-8`),
			capStep(embed.UrnString(), "embed", `media:txt;enc=utf-8`, `media:vec;record`),
		},
		"pdf to vec",
	)
	m1, err := FromStrand(strand, reg)
	require.Nil(t, err)

	aliased := m1.ToMachineNotationAliased(reg, NotationFormatBracketed)

	// The extract cap is aliased: referenced directly in the wiring by its
	// SHORTER alias `ex`, with NO header, and the URN must not appear.
	assert.Contains(t, aliased, "-> ex ->",
		"extract cap must be referenced in the wiring by the shortest alias `ex`, got: %s", aliased)
	assert.NotContains(t, aliased, "extract",
		"the aliased extract cap URN must not appear, got: %s", aliased)
	// The embed cap has no alias → it keeps its synthetic header + URN.
	assert.Contains(t, aliased, "embed",
		"the un-aliased embed cap must keep its header URN, got: %s", aliased)

	// Round-trip: parse the aliased notation back. The alias is already in the
	// warm cache (seeded above), so the sync parser resolves it.
	m2, perr := ParseMachine(aliased, reg)
	require.Nil(t, perr, "aliased notation must re-parse")
	assert.True(t, m1.IsEquivalent(m2),
		"aliased serialize → parse must preserve strict equivalence")
}

// ----- machine notation cap aliases (1883-1886) -----

func extractWithAliasRegistry() (*fabric.FabricRegistry, string) {
	extractUrn := `cap:extract;in="media:ext=pdf";out="media:enc=utf-8;ext=txt"`
	c := buildCap(extractUrn, "extract", []string{"media:ext=pdf"}, "media:enc=utf-8;ext=txt")
	canonical := c.UrnString()
	reg := fabric.NewForTest()
	reg.AddCapsToCache([]*cap.Cap{c})
	reg.InsertCachedAliasForTest(media.StoredAlias{Name: "pdf2text", Target: canonical, Version: 1})
	return reg, canonical
}

// Test1883: a cap-position name with no local header resolves as a cap alias.
