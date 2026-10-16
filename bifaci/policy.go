package bifaci

// What the host allows the cartridges it serves to do (12.2 §Policy).
//
// The engine holds the live policy and gives it to the RelaySwitch; a Policy
// frame carries it through every relay to each host, which keeps the latest
// and sends it to each of its cartridges after the handshake and on every
// change. A cartridge runtime keeps the latest in a LivePolicy, and a handler
// reads it through NetworkAccess before each call it makes to the network —
// never a copy taken when the request was dispatched, so a change to
// `offline` stops a running download or stream at its next call.
// (matches Rust bifaci::policy)

import (
	"fmt"
	"sync/atomic"
)

// PolicyMetaNetwork is the wire key a Policy frame's meta names the network
// policy by.
const PolicyMetaNetwork = "network"

// NetworkPolicyCode is the code a call the policy does not allow fails with.
const NetworkPolicyCode = "NETWORK_POLICY"

// NetworkPolicyUnsetCode is the code of a call refused because no policy has
// been given at all — a defect, not the operator's decision.
const NetworkPolicyUnsetCode = "NETWORK_POLICY_UNSET"

// NetworkPolicy is how far a cartridge may reach on the network.
type NetworkPolicy uint8

const (
	// NetworkPolicyOffline: nothing beyond this machine.
	NetworkPolicyOffline NetworkPolicy = iota + 1
	// NetworkPolicyNoTelemetry: third-party services and MachineFabric's own,
	// never analytics.
	NetworkPolicyNoTelemetry
	// NetworkPolicyFull: the same, and analytics.
	NetworkPolicyFull
)

// AsStr is the policy's wire word.
func (p NetworkPolicy) AsStr() string {
	switch p {
	case NetworkPolicyOffline:
		return "offline"
	case NetworkPolicyNoTelemetry:
		return "no_telemetry"
	case NetworkPolicyFull:
		return "full"
	default:
		panic(fmt.Sprintf("BUG: NetworkPolicy %d is not a policy", uint8(p)))
	}
}

func (p NetworkPolicy) String() string { return p.AsStr() }

// NetworkPolicyFromWire is the policy a wire word names; false for any other
// word. A word nobody defined is never read as one of these.
func NetworkPolicyFromWire(word string) (NetworkPolicy, bool) {
	switch word {
	case "offline":
		return NetworkPolicyOffline, true
	case "no_telemetry":
		return NetworkPolicyNoTelemetry, true
	case "full":
		return NetworkPolicyFull, true
	default:
		return 0, false
	}
}

// Reach is what a call to the network reaches, which is what the policy
// decides on.
type Reach uint8

const (
	// ReachThirdParty: a service MachineFabric does not run.
	ReachThirdParty Reach = iota
	// ReachOurServices: a service MachineFabric runs for the product to work.
	ReachOurServices
	// ReachTelemetry: analytics and crash reports.
	ReachTelemetry
)

func (r Reach) describe() string {
	switch r {
	case ReachThirdParty:
		return "an outside service"
	case ReachOurServices:
		return "MachineFabric's services"
	case ReachTelemetry:
		return "analytics"
	default:
		panic(fmt.Sprintf("BUG: Reach %d is not a reach", uint8(r)))
	}
}

// Allows says whether this policy lets a call of this reach be made.
func (p NetworkPolicy) Allows(r Reach) bool {
	switch p {
	case NetworkPolicyOffline:
		return false
	case NetworkPolicyNoTelemetry:
		return r != ReachTelemetry
	case NetworkPolicyFull:
		return true
	default:
		panic(fmt.Sprintf("BUG: NetworkPolicy %d is not a policy", uint8(p)))
	}
}

// HostPolicy is everything a Policy frame says.
type HostPolicy struct {
	Network NetworkPolicy
}

// NetworkRefused is a call that was not made. Policy is zero when no policy
// had been given at all.
type NetworkRefused struct {
	Policy NetworkPolicy
	Reach  Reach
}

func (e *NetworkRefused) Error() string {
	if e.Policy == 0 {
		return fmt.Sprintf("no network policy has been given to this cartridge, so nothing may reach %s", e.Reach.describe())
	}
	return fmt.Sprintf("the network policy is '%s', which does not allow reaching %s", e.Policy.AsStr(), e.Reach.describe())
}

// Classified is the failure a handler ends with: the operator's own setting
// refusing the work is `user` (never retried, never tolerated, 17.2); a
// missing policy is `internal`.
func (e *NetworkRefused) Classified() *ClassifiedError {
	if e.Policy == 0 {
		return &ClassifiedError{Code: NetworkPolicyUnsetCode, Class: AttributionClassInternal, Message: e.Error()}
	}
	return &ClassifiedError{Code: NetworkPolicyCode, Class: AttributionClassUser, Message: e.Error()}
}

// LivePolicy is the latest policy a runtime was given, shared with every
// handler it runs. Empty until the first Policy frame arrives.
type LivePolicy struct {
	network atomic.Uint32
}

// NewLivePolicy is a store that has not been given a policy yet.
func NewLivePolicy() *LivePolicy { return &LivePolicy{} }

// NewLivePolicyWith is a store holding policy from the start.
func NewLivePolicyWith(policy HostPolicy) *LivePolicy {
	live := &LivePolicy{}
	live.Set(policy)
	return live
}

// Set replaces the policy; every reader sees it at its next read.
func (l *LivePolicy) Set(policy HostPolicy) {
	l.network.Store(uint32(policy.Network))
}

// Current is the policy in force, false before the first one was given.
func (l *LivePolicy) Current() (HostPolicy, bool) {
	v := l.network.Load()
	if v == 0 {
		return HostPolicy{}, false
	}
	return HostPolicy{Network: NetworkPolicy(v)}, true
}

// NetworkAccess is a handler's view of the live network policy: cheap to
// copy, safe to hand to the code that opens connections, which asks it
// before every call.
type NetworkAccess struct {
	live *LivePolicy
}

// NewNetworkAccess reads live.
func NewNetworkAccess(live *LivePolicy) NetworkAccess { return NetworkAccess{live: live} }

// Allow says whether a call of this reach may be made now, under the policy
// in force at this moment.
func (a NetworkAccess) Allow(r Reach) error {
	current, ok := a.live.Current()
	if !ok {
		return &NetworkRefused{Reach: r}
	}
	if current.Network.Allows(r) {
		return nil
	}
	return &NetworkRefused{Policy: current.Network, Reach: r}
}

// Policy is the policy in force now, false before the first one was given.
func (a NetworkAccess) Policy() (NetworkPolicy, bool) {
	current, ok := a.live.Current()
	return current.Network, ok
}

// NewPolicyFrame creates a POLICY frame: what the host allows its cartridges
// to do now (12.2 §Policy).
func NewPolicyFrame(policy HostPolicy) *Frame {
	frame := newFrame(FrameTypePolicy, NewMessageIdFromUint(0))
	frame.Meta = map[string]interface{}{PolicyMetaNetwork: policy.Network.AsStr()}
	return frame
}

// HostPolicy reads the policy a POLICY frame carries: false for any other
// frame type, an error for a Policy frame whose meta.network is missing or
// names no policy. Decode refuses such a frame, so a decoded Policy always
// reads.
func (f *Frame) HostPolicy() (HostPolicy, bool, error) {
	if f.FrameType != FrameTypePolicy {
		return HostPolicy{}, false, nil
	}
	raw, ok := f.Meta[PolicyMetaNetwork]
	if !ok {
		return HostPolicy{}, true, fmt.Errorf("POLICY frame missing required meta.%s", PolicyMetaNetwork)
	}
	word, ok := raw.(string)
	if !ok {
		return HostPolicy{}, true, fmt.Errorf("POLICY frame's meta.%s is not text: %v", PolicyMetaNetwork, raw)
	}
	network, ok := NetworkPolicyFromWire(word)
	if !ok {
		return HostPolicy{}, true, fmt.Errorf("POLICY frame's meta.%s names no network policy: '%s'", PolicyMetaNetwork, word)
	}
	return HostPolicy{Network: network}, true, nil
}
