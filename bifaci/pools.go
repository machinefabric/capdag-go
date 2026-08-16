// Concurrency pools — the ONE capacity concept of the protocol.
//
// A pool is a named concurrency domain on a cartridge process. Every
// registered cap IS a pool of one, named by its canonical cap URN; the
// reserved pool PoolAll contains every cap (it is what the deleted scalar
// handler_capacity used to be); the manifest may declare further named
// pools over subsets of caps. A cap's POOL CHAIN — its own singleton pool,
// every declared pool containing it, then `all` — is the set of domains a
// dispatch must be admitted through. Queues lead to pools: each request
// waits in its cap's singleton-pool queue; shared pools own no queue of
// their own.
//
// Three numbers per pool, one effective value:
//   - declared   — the manifest's shipped default (the cartridge's).
//   - configured — the operator's number (starts = declared; persisted by
//     the engine's cartridge configuration store).
//   - available  — OPTIONAL cartridge self-report: what the process can
//     serve right now from its OWN state. Absent means static: the normal,
//     fully-supported case.
//
// effective = min(configured, available) with 0-as-unlimited treated as
// infinity inside the min and absent available treated as infinity.
//
// On the wire the pool-state map rides as JSON bytes in frame meta —
// exactly the transport the manifest itself uses — under the MetaPools key
// (HELLO and every heartbeat reply) and, host→cartridge, the
// MetaDesiredCapacities key on a heartbeat probe. The roster's
// runtime_stats carries the same map. (matches Rust bifaci::pools)
package bifaci

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/machinefabric/capdag-go/urn"
)

// PoolAll is the reserved pool containing every cap. Always exists; default
// capacity 0 (unlimited). The exact replacement of the deleted scalar
// handler_capacity. (matches Rust POOL_ALL)
const PoolAll = "all"

// MetaPools is the frame-meta key carrying a JSON-encoded PoolStates map
// (HELLO, every heartbeat reply, and the roster's runtime stats).
const MetaPools = "pools"

// MetaDesiredCapacities is the frame-meta key on a heartbeat PROBE carrying
// a JSON-encoded map of pool name → desired configured value.
const MetaDesiredCapacities = "desired_capacities"

// CapacityUnlimited is unlimited, as a capacity value. Everywhere a
// capacity is read, 0 means "no limit" — never "zero slots".
const CapacityUnlimited uint64 = 0

// PoolState is one pool's full state. The same shape everywhere:
// manifest-derived declarations, heartbeat replies, roster stats, and the
// clients' cartridge views. (matches Rust PoolState)
type PoolState struct {
	// Declared is the manifest's shipped default. 0 = unlimited.
	Declared uint64 `json:"declared"`
	// Configured is the operator's number. Starts equal to Declared.
	Configured uint64 `json:"configured"`
	// Available is the cartridge's self-reported current limit. Absent
	// (nil) = static and is treated as unlimited inside Effective.
	Available *uint64 `json:"available,omitempty"`
	// Active counts requests currently being served in this pool.
	Active uint64 `json:"active"`
	// Queued counts requests currently queued against this pool. For
	// shared pools this counts waiters whose OWN pool has room but this
	// pool does not.
	Queued uint64 `json:"queued"`
	// Caps lists member caps (canonical URNs). Singleton pools omit the
	// list — the pool's name IS its one member.
	Caps []string `json:"caps,omitempty"`
}

// DeclaredPoolState is a declared pool at rest: configured = declared,
// nothing active, no self-report. (matches Rust PoolState::declared)
func DeclaredPoolState(declared uint64, caps []string) PoolState {
	return PoolState{Declared: declared, Configured: declared, Caps: caps}
}

// Effective is the admission bound: min(configured, available) with 0
// meaning unlimited on either input and on the output, and an absent
// available treated as unlimited. (matches Rust PoolState::effective)
func (p *PoolState) Effective() uint64 {
	return EffectiveCapacity(p.Configured, p.Available)
}

// EffectiveCapacity is min(configured, available) under the 0-as-unlimited
// convention. (matches Rust effective_capacity)
func EffectiveCapacity(configured uint64, available *uint64) uint64 {
	c := configured
	if c == CapacityUnlimited {
		c = math.MaxUint64
	}
	a := uint64(math.MaxUint64)
	if available != nil && *available != CapacityUnlimited {
		a = *available
	}
	effective := c
	if a < effective {
		effective = a
	}
	if effective == math.MaxUint64 {
		return CapacityUnlimited
	}
	return effective
}

// PoolStates is the full pool-state map of one cartridge process, keyed by
// pool name (a canonical cap URN for singletons, a declared pool name, or
// `all`). (matches Rust PoolStates)
type PoolStates = map[string]PoolState

// DesiredCapacities is the host→cartridge desired-configured map delivered
// on a heartbeat probe. (matches Rust DesiredCapacities)
type DesiredCapacities = map[string]uint64

// PoolDeclarations is the manifest's pool DECLARATIONS: shared-pool
// memberships plus a capacities map whose keys are pool names uniformly (a
// canonical cap URN, a declared pool name, or `all`).
// (matches Rust PoolDeclarations)
type PoolDeclarations struct {
	// Pools maps declared shared pools: name → member caps (canonical URNs).
	Pools map[string][]string `json:"pools,omitempty"`
	// Capacities maps declared capacities by pool name. Absent = 0 = unlimited.
	Capacities map[string]uint64 `json:"capacities,omitempty"`
}

// IsEmpty reports whether nothing is declared.
func (d *PoolDeclarations) IsEmpty() bool {
	return len(d.Pools) == 0 && len(d.Capacities) == 0
}

// sortedKeys returns map keys in deterministic (sorted) order — the Go
// stand-in for Rust's BTreeMap iteration order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Validated validates the declarations against the set of declared caps and
// canonicalizes every cap reference. Hard errors, never coercion (matches
// Rust PoolDeclarations::validated):
//   - a shared pool named `all` or parsing as a cap URN;
//   - a pool member or capacity key that names no declared cap (capacity
//     keys may also name a declared pool or `all`);
//   - a cap listed twice in one pool.
func (d *PoolDeclarations) Validated(declaredCaps []*urn.CapUrn) (*PoolDeclarations, error) {
	canonical := make([]string, len(declaredCaps))
	for i, c := range declaredCaps {
		canonical[i] = c.String()
	}
	canonicalize := func(raw string) (string, error) {
		parsed, err := urn.NewCapUrnFromString(raw)
		if err != nil {
			return "", fmt.Errorf("pool cap reference '%s' is not a valid cap URN: %w", raw, err)
		}
		canon := parsed.String()
		if slices.Contains(canonical, canon) {
			return canon, nil
		}
		return "", fmt.Errorf("pool cap reference '%s' names no cap declared by this manifest", raw)
	}

	pools := make(map[string][]string)
	for _, name := range sortedKeys(d.Pools) {
		members := d.Pools[name]
		if name == PoolAll {
			return nil, fmt.Errorf("pool name '%s' is reserved for the implicit all-caps pool", PoolAll)
		}
		if _, err := urn.NewCapUrnFromString(name); err == nil {
			return nil, fmt.Errorf(
				"pool name '%s' parses as a cap URN — cap URNs name the implicit singleton pools and cannot be redeclared",
				name)
		}
		if len(members) == 0 {
			return nil, fmt.Errorf("pool '%s' declares no member caps", name)
		}
		var canonMembers []string
		for _, member := range members {
			canon, err := canonicalize(member)
			if err != nil {
				return nil, err
			}
			if slices.Contains(canonMembers, canon) {
				return nil, fmt.Errorf("pool '%s' lists cap '%s' more than once", name, canon)
			}
			canonMembers = append(canonMembers, canon)
		}
		pools[name] = canonMembers
	}

	capacities := make(map[string]uint64)
	for _, key := range sortedKeys(d.Capacities) {
		value := d.Capacities[key]
		canonKey := key
		if key != PoolAll {
			if _, isPool := pools[key]; !isPool {
				canon, err := canonicalize(key)
				if err != nil {
					return nil, fmt.Errorf(
						"capacity key '%s' is neither '%s', a declared pool, nor a declared cap: %w",
						key, PoolAll, err)
				}
				canonKey = canon
			}
		}
		if _, dup := capacities[canonKey]; dup {
			return nil, fmt.Errorf(
				"capacity for pool '%s' is declared more than once (two spellings of one cap URN?)",
				canonKey)
		}
		capacities[canonKey] = value
	}

	return &PoolDeclarations{Pools: pools, Capacities: capacities}, nil
}

// DeclaredStates materializes the full declared pool-state map for a cap
// set: one singleton pool per cap, every declared shared pool, and `all`.
// The receiver must already be validated against the same cap set.
// (matches Rust PoolDeclarations::declared_states)
func (d *PoolDeclarations) DeclaredStates(declaredCaps []*urn.CapUrn) PoolStates {
	states := make(PoolStates)
	allMembers := make([]string, len(declaredCaps))
	for i, c := range declaredCaps {
		allMembers[i] = c.String()
	}
	for _, cap := range allMembers {
		states[cap] = DeclaredPoolState(d.Capacities[cap], nil)
	}
	for name, members := range d.Pools {
		states[name] = DeclaredPoolState(d.Capacities[name], append([]string(nil), members...))
	}
	states[PoolAll] = DeclaredPoolState(d.Capacities[PoolAll], allMembers)
	return states
}

// ChainFor is the pool CHAIN of one cap, in admission order: its singleton
// pool, every declared pool containing it, then `all`. cap must be the
// canonical URN string. (matches Rust PoolDeclarations::chain_for)
func (d *PoolDeclarations) ChainFor(cap string) []string {
	chain := []string{cap}
	for _, name := range sortedKeys(d.Pools) {
		if slices.Contains(d.Pools[name], cap) {
			chain = append(chain, name)
		}
	}
	return append(chain, PoolAll)
}

// ChainFromStates is the chain of one cap over a MATERIALIZED state map
// (roster / heartbeat truth): the singleton pool, every pool listing the
// cap as a member, then `all`. Order: singleton, declared pools in sorted
// order, `all`. (matches Rust chain_from_states)
func ChainFromStates(states PoolStates, cap string) []string {
	var chain []string
	if _, ok := states[cap]; ok {
		chain = append(chain, cap)
	}
	for _, name := range sortedKeys(states) {
		if name == PoolAll || name == cap {
			continue
		}
		if slices.Contains(states[name].Caps, cap) {
			chain = append(chain, name)
		}
	}
	if _, ok := states[PoolAll]; ok {
		chain = append(chain, PoolAll)
	}
	return chain
}

// EncodePoolStates encodes a pool-state map for frame meta (JSON bytes —
// the manifest's own transport). (matches Rust encode_pool_states)
func EncodePoolStates(states PoolStates) []byte {
	bytes, err := json.Marshal(states)
	if err != nil {
		panic(fmt.Sprintf("pool states are always JSON-encodable: %v", err))
	}
	return bytes
}

// DecodePoolStates decodes a pool-state map from frame meta. A malformed
// map is a protocol error at the caller's boundary — never partially read.
// (matches Rust decode_pool_states)
func DecodePoolStates(bytes []byte) (PoolStates, error) {
	var states PoolStates
	if err := json.Unmarshal(bytes, &states); err != nil {
		return nil, fmt.Errorf("malformed pool-state map: %w", err)
	}
	return states, nil
}

// EncodeDesired encodes the desired-configured map for a heartbeat probe.
// (matches Rust encode_desired)
func EncodeDesired(desired DesiredCapacities) []byte {
	bytes, err := json.Marshal(desired)
	if err != nil {
		panic(fmt.Sprintf("desired capacities are always JSON-encodable: %v", err))
	}
	return bytes
}

// DecodeDesired decodes the desired-configured map from a heartbeat probe.
// (matches Rust decode_desired)
func DecodeDesired(bytes []byte) (DesiredCapacities, error) {
	var desired DesiredCapacities
	if err := json.Unmarshal(bytes, &desired); err != nil {
		return nil, fmt.Errorf("malformed desired-capacities map: %w", err)
	}
	return desired, nil
}
