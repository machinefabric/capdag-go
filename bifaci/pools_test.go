package bifaci

import (
	"testing"

	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func poolTestCaps(t *testing.T, urns ...string) []*urn.CapUrn {
	t.Helper()
	caps := make([]*urn.CapUrn, len(urns))
	for i, u := range urns {
		parsed, err := urn.NewCapUrnFromString(u)
		require.NoError(t, err, "test cap urn")
		caps[i] = parsed
	}
	return caps
}

func availableOf(n uint64) *uint64 { return &n }

// TEST1520: effective capacity is min(configured, available) under the
// 0-as-unlimited convention, with absent available treated as unlimited —
// the one formula every admission decision reduces to.
func Test1520_effective_capacity_min_semantics(t *testing.T) {
	assert.Equal(t, uint64(0), EffectiveCapacity(0, nil), "unlimited stays unlimited")
	assert.Equal(t, uint64(4), EffectiveCapacity(4, nil), "absent available is a free pass")
	assert.Equal(t, uint64(4), EffectiveCapacity(4, availableOf(0)), "available 0 is unlimited, not zero slots")
	assert.Equal(t, uint64(2), EffectiveCapacity(0, availableOf(2)), "self-limit binds an unlimited configured")
	assert.Equal(t, uint64(1), EffectiveCapacity(4, availableOf(1)), "the smaller bound wins")
	assert.Equal(t, uint64(1), EffectiveCapacity(1, availableOf(4)), "in either direction")
}

// TEST1521: a cap is a pool of one and `all` always exists — the declared
// state map materializes every singleton, every declared pool, and `all`,
// with capacities resolved by pool name uniformly.
func Test1521_declared_states_materialize_every_pool(t *testing.T) {
	declaredCaps := poolTestCaps(t,
		`cap:generate;in="media:enc=utf-8";out="media:enc=utf-8"`,
		`cap:embed;in="media:enc=utf-8";out="media:embeddings"`,
	)
	generate := declaredCaps[0].String()
	embed := declaredCaps[1].String()
	declarations := &PoolDeclarations{
		Pools: map[string][]string{"gpu": {generate, embed}},
		Capacities: map[string]uint64{
			generate: 1,
			"gpu":    1,
			PoolAll:  8,
		},
	}
	validated, err := declarations.Validated(declaredCaps)
	require.NoError(t, err, "valid declarations")
	states := validated.DeclaredStates(declaredCaps)

	assert.Len(t, states, 4, "two singletons + gpu + all")
	assert.Equal(t, uint64(1), states[generate].Declared)
	assert.Equal(t, uint64(1), states[generate].Configured, "configured starts at declared")
	assert.Equal(t, uint64(0), states[embed].Declared, "undeclared singleton is unlimited")
	assert.Equal(t, uint64(1), states["gpu"].Declared)
	assert.Equal(t, []string{generate, embed}, states["gpu"].Caps)
	assert.Equal(t, uint64(8), states[PoolAll].Declared)
	assert.Len(t, states[PoolAll].Caps, 2, "all contains every cap")

	// The chain: singleton, declared pools containing the cap, all.
	assert.Equal(t, []string{generate, "gpu", PoolAll}, validated.ChainFor(generate))
	// And the same chain derived from the materialized states.
	assert.Equal(t, []string{generate, "gpu", PoolAll}, ChainFromStates(states, generate))
}

// TEST1522: pool declarations are validated hard — reserved name, a pool
// named like a cap URN, an unknown member, a duplicate member, and an
// unknown capacity key are each refused with the offender named.
func Test1522_pool_declaration_validation_refuses_illegal_shapes(t *testing.T) {
	declaredCaps := poolTestCaps(t, `cap:generate;in="media:enc=utf-8";out="media:enc=utf-8"`)
	generate := declaredCaps[0].String()

	reserved := &PoolDeclarations{Pools: map[string][]string{PoolAll: {generate}}}
	_, err := reserved.Validated(declaredCaps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved")

	capNamed := &PoolDeclarations{Pools: map[string][]string{generate: {generate}}}
	_, err = capNamed.Validated(declaredCaps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parses as a cap URN")

	unknownMember := &PoolDeclarations{
		Pools: map[string][]string{"gpu": {`cap:absent;in="media:";out="media:"`}},
	}
	_, err = unknownMember.Validated(declaredCaps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names no cap")

	duplicate := &PoolDeclarations{Pools: map[string][]string{"gpu": {generate, generate}}}
	_, err = duplicate.Validated(declaredCaps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "more than once")

	unknownCapacity := &PoolDeclarations{Capacities: map[string]uint64{"warp": 3}}
	_, err = unknownCapacity.Validated(declaredCaps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "neither")
}

// TEST1523: the wire codec round-trips the full map — including the
// absent-vs-present distinction on `available`, which is the
// static-vs-self-limited distinction and must never collapse.
func Test1523_pool_state_wire_round_trip(t *testing.T) {
	singleton := `cap:x;in="media:";out="media:"`
	states := PoolStates{
		singleton: {
			Declared:   2,
			Configured: 4,
			Available:  availableOf(1),
			Active:     1,
			Queued:     3,
		},
		PoolAll: DeclaredPoolState(0, []string{singleton}),
	}

	bytes := EncodePoolStates(states)
	decoded, err := DecodePoolStates(bytes)
	require.NoError(t, err, "round-trip")
	assert.Equal(t, states, decoded)
	state := decoded[singleton]
	assert.Equal(t, uint64(1), state.Effective())
	assert.Nil(t, decoded[PoolAll].Available, "static stays static")

	_, err = DecodePoolStates([]byte("not json"))
	assert.Error(t, err)

	desired := DesiredCapacities{PoolAll: 6}
	decodedDesired, err := DecodeDesired(EncodeDesired(desired))
	require.NoError(t, err, "desired round-trip")
	assert.Equal(t, desired, decodedDesired)
}
