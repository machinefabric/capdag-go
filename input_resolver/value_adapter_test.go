package input_resolver

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/machinefabric/capdag-go/urn"
	taggedurn "github.com/machinefabric/tagged-urn-go"
)

func media(t *testing.T, s string) *urn.MediaUrn {
	t.Helper()
	m, err := urn.NewMediaUrnFromString(s)
	require.NoError(t, err, s)
	return m
}

// withMarker is base with a marker added, through the tagged-URN API.
func withMarker(base, marker string) string {
	m, err := urn.NewMediaUrnFromString(base)
	if err != nil {
		panic(err)
	}
	refined, err := m.WithTag(marker, taggedurn.ConstraintPresent{})
	if err != nil {
		panic(err)
	}
	return refined.String()
}

// specialAdapter detects "special" values and adds the refined marker.
type specialAdapter struct{}

func (specialAdapter) Name() string { return "SpecialAdapter" }

func (specialAdapter) Refine(baseMediaUrn, value string) *ValueAdapterResult {
	if strings.Contains(value, "special") {
		return &ValueAdapterResult{MediaUrn: withMarker(baseMediaUrn, "refined")}
	}
	return nil
}

// specificAdapter is the adapter for a more specific key, which marks what it chose.
type specificAdapter struct{}

func (specificAdapter) Name() string { return "SpecificAdapter" }

func (specificAdapter) Refine(baseMediaUrn string, _ string) *ValueAdapterResult {
	return &ValueAdapterResult{MediaUrn: withMarker(baseMediaUrn, "chosen-specific")}
}

// fixedAdapter answers with a fixed media URN, whatever the base.
type fixedAdapter string

func (fixedAdapter) Name() string { return "FixedAdapter" }

func (f fixedAdapter) Refine(_ string, _ string) *ValueAdapterResult {
	return &ValueAdapterResult{MediaUrn: string(f)}
}

// askedAdapter records that it was asked.
type askedAdapter struct{ asked *bool }

func (askedAdapter) Name() string { return "Asked" }

func (a askedAdapter) Refine(_ string, _ string) *ValueAdapterResult {
	*a.asked = true
	return nil
}

// testValueAdapter adds a "special" marker to a base with the test marker when
// the value contains "special".
type testValueAdapter struct{}

func (testValueAdapter) Name() string { return "TestValueAdapter" }

func (testValueAdapter) Refine(baseMediaUrn, value string) *ValueAdapterResult {
	base, err := urn.NewMediaUrnFromString(baseMediaUrn)
	if err != nil || !base.HasMarkerTag("test") {
		return nil
	}
	if strings.Contains(value, "special") {
		return &ValueAdapterResult{MediaUrn: withMarker(baseMediaUrn, "special")}
	}
	return nil
}

// TEST1221: Matching value adapters refine the base media URN when the value fits.
func Test1221_RefineWithMatchingAdapter(t *testing.T) {
	registry := NewValueAdapterRegistry()
	require.NoError(t, registry.Register("media:test", specialAdapter{}))

	result, err := registry.RefineMediaUrn(media(t, "media:test;enc=utf8"), "a-special-value")
	require.NoError(t, err)
	assert.True(t, result.IsEquivalent(media(t, "media:enc=utf8;refined;test")), result.String())
}

// TEST1222: Base URNs without a registered adapter are returned unchanged.
func Test1222_RefineNoMatchingAdapter(t *testing.T) {
	registry := NewValueAdapterRegistry()
	require.NoError(t, registry.Register("media:test", specialAdapter{}))

	base := media(t, "media:other;enc=utf8")
	result, err := registry.RefineMediaUrn(base, "a-special-value")
	require.NoError(t, err)
	assert.True(t, result.IsEquivalent(base))
}

// TEST1223: Adapters that decline to refine leave the original media URN intact.
func Test1223_RefineAdapterReturnsNone(t *testing.T) {
	registry := NewValueAdapterRegistry()
	require.NoError(t, registry.Register("media:test", specialAdapter{}))

	base := media(t, "media:test;enc=utf8")
	result, err := registry.RefineMediaUrn(base, "ordinary-value")
	require.NoError(t, err)
	assert.True(t, result.IsEquivalent(base))
}

// TEST1224: When several adapter keys cover the base, the adapter for the most specific key answers.
func Test1224_RefineMostSpecificKey(t *testing.T) {
	registry := NewValueAdapterRegistry()
	require.NoError(t, registry.Register("media:test", specialAdapter{}))
	require.NoError(t, registry.Register("media:specific;test", specificAdapter{}))

	result, err := registry.RefineMediaUrn(media(t, "media:foo;specific;test"), "a-special-value")
	require.NoError(t, err)
	assert.True(t, result.HasMarkerTag("chosen-specific"), result.String())
	assert.False(t, result.HasMarkerTag("refined"), result.String())
}

// TEST1225: An empty value adapter registry returns the input media URN unchanged.
func Test1225_EmptyRegistry(t *testing.T) {
	registry := NewValueAdapterRegistry()
	base := media(t, "media:anything")
	result, err := registry.RefineMediaUrn(base, "any-value")
	require.NoError(t, err)
	assert.True(t, result.IsEquivalent(base))
}

// TEST1226: Adapter presence checks report only the keys that were registered.
func Test1226_HasAdapter(t *testing.T) {
	registry := NewValueAdapterRegistry()
	require.NoError(t, registry.Register("media:test", specialAdapter{}))

	has, err := registry.HasAdapter("media:test")
	require.NoError(t, err)
	assert.True(t, has)
	has, err = registry.HasAdapter("media:other")
	require.NoError(t, err)
	assert.False(t, has)
}

// TEST1228: Value adapters can add a more specific marker when both base URN and value match.
func Test1228_ValueAdapterRefineMatch(t *testing.T) {
	result := testValueAdapter{}.Refine("media:enc=utf8;test", "something-special")
	require.NotNil(t, result)
	assert.True(t, media(t, result.MediaUrn).IsEquivalent(media(t, "media:enc=utf8;special;test")), result.MediaUrn)
}

// TEST1229: Value adapters return no refinement when the base media URN is outside their domain.
func Test1229_ValueAdapterRefineNoMatchBase(t *testing.T) {
	assert.Nil(t, testValueAdapter{}.Refine("media:enc=utf8;other", "something-special"))
}

// TEST1230: Value adapters return no refinement when the inspected value does not match.
func Test1230_ValueAdapterRefineNoMatchValue(t *testing.T) {
	assert.Nil(t, testValueAdapter{}.Refine("media:enc=utf8;test", "ordinary-value"))
}

func adapterErrorKind(t *testing.T, err error) string {
	t.Helper()
	adapterErr, ok := err.(*ValueAdapterError)
	require.True(t, ok, "expected a ValueAdapterError, got %v", err)
	return adapterErr.Kind
}

// TEST6758: Value adapters are chosen by inclusion: keys are media URNs, the most specific key covering the base wins, two unordered ones are refused, and a refinement must lie inside the base
func Test6758_ValueAdaptersChosenByInclusion(t *testing.T) {
	registry := NewValueAdapterRegistry()
	assert.Equal(t, ValueAdapterBadKey, adapterErrorKind(t, registry.Register("media:a=", specialAdapter{})))
	require.NoError(t, registry.Register("media:a", specialAdapter{}))
	assert.Equal(t, ValueAdapterDuplicateKey, adapterErrorKind(t, registry.Register("MEDIA:A", specialAdapter{})))

	require.NoError(t, registry.Register("media:b", specialAdapter{}))
	_, err := registry.RefineMediaUrn(media(t, "media:a;b"), "special")
	assert.Equal(t, ValueAdapterAmbiguous, adapterErrorKind(t, err))
	assert.Contains(t, err.Error(), "'media:a' and 'media:b'")

	outside := NewValueAdapterRegistry()
	require.NoError(t, outside.Register("media:a", fixedAdapter("media:other")))
	_, err = outside.RefineMediaUrn(media(t, "media:a"), "v")
	assert.Equal(t, ValueAdapterNotInside, adapterErrorKind(t, err))

	// A key fires for a base that falls under it whatever the base's text
	// begins with: prefix matching never asked this adapter.
	asked := false
	modelSpec := NewValueAdapterRegistry()
	require.NoError(t, modelSpec.Register("media:model-spec", askedAdapter{asked: &asked}))
	_, err = modelSpec.RefineMediaUrn(media(t, "media:enc=utf8;llm;model-spec"), "hf:x")
	require.NoError(t, err)
	assert.True(t, asked)
}
