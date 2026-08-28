package bifaci

// What a consumer concluded about a cartridge registry, as a closed vocabulary
// shared by every implementation. Mirrors Rust `bifaci/registry_verdict.rs`.
//
// A REGISTRY IS NOT A CARTRIDGE. A registry verdict is one fact per registry
// URL, shared by every cartridge that claims provenance from it; a cartridge
// attachment error is one fact per cartridge. Squeezing the first through the
// second is how a signature that failed verification came to be reported as a
// network outage, with "check your connection" as the remedy.
//
// The vocabulary separates the two things a consumer can conclude:
//
//   - It could not get an answer — Offline, Unreachable, HTTPError, Malformed.
//     We do not know what the registry says. Retrying, or changing a setting,
//     may change the answer.
//   - It got an answer and refused it — Unsigned, Untrusted, Unverifiable. We
//     know what the registry says and we will not act on it. Retrying changes
//     nothing.
//
// Those two groups have opposite remedies, which is the whole reason the
// distinction exists.

import (
	"encoding/json"
	"fmt"
	"slices"
)

// ReleaseKeyCertFormat is the format discriminator for release-key
// certificates. Mirrors Rust RELEASE_KEY_CERT_FORMAT.
//
// THE WIRE FORMAT IS NOT A PRODUCT NAME. Every verifier — this library, a
// client's in-process verifier, the publisher — must compare against THIS
// constant. A client holding its own copy can be renamed away from the protocol
// by a search-and-replace, verify nothing, and report the registry as
// unreachable; that is exactly what happened, and it is why these live here.
const ReleaseKeyCertFormat = "machinefabric-release-key-cert/1"

// ManifestSigFormat is the format discriminator for manifest signature
// envelopes. Mirrors Rust MANIFEST_SIG_FORMAT.
const ManifestSigFormat = "machinefabric-manifest-sig/1"

// RegistryVerdictState is what a consumer concluded about a registry.
type RegistryVerdictState string

const (
	// RegistryVerdictStateVerified: fetched, chain-verified and parsed. The
	// only state in which a cartridge from this registry may attach.
	RegistryVerdictStateVerified RegistryVerdictState = "verified"
	// RegistryVerdictStatePending: no verdict yet — the first check has not
	// run. NOT a failure: a consumer that renders this as an error tells
	// every operator their registry is broken for the first seconds of
	// every launch.
	RegistryVerdictStatePending RegistryVerdictState = "pending"
	// RegistryVerdictStateOffline: the consumer's own network policy forbade
	// the request. The remedy is a setting, not the network, which is why
	// this is not Unreachable.
	RegistryVerdictStateOffline RegistryVerdictState = "offline"
	// RegistryVerdictStateUnreachable: DNS, refused, timeout, TLS. The only
	// state for which "check your connection" is sound advice.
	RegistryVerdictStateUnreachable RegistryVerdictState = "unreachable"
	// RegistryVerdictStateHTTPError: the registry answered with an HTTP
	// error; the status travels with the verdict, because 404 and 5xx are
	// different situations for the operator.
	RegistryVerdictStateHTTPError RegistryVerdictState = "http_error"
	// RegistryVerdictStateMalformed: the registry answered with a body this
	// build cannot read as a manifest.
	RegistryVerdictStateMalformed RegistryVerdictState = "malformed"
	// RegistryVerdictStateUnsigned: no signature sidecar where one is
	// required. An unsigned registry is refused rather than trusted.
	RegistryVerdictStateUnsigned RegistryVerdictState = "unsigned"
	// RegistryVerdictStateUntrusted: the chain was evaluated and REJECTED.
	// The registry's problem.
	RegistryVerdictStateUntrusted RegistryVerdictState = "untrusted"
	// RegistryVerdictStateUnverifiable: the chain could NOT be evaluated —
	// a format this build does not implement. Our problem, remedied by
	// updating the client, never by distrusting the registry and never by
	// checking the network.
	RegistryVerdictStateUnverifiable RegistryVerdictState = "unverifiable"
	// RegistryVerdictStateUnenforced: this build bakes no trust anchors, so
	// there is no regime to verify against and the manifest was accepted
	// without proof. A development build, and only ever that. It permits
	// attachment — a dev build has to work — and is a SEPARATE state rather
	// than being reported as Verified, because "we checked and it passed" and
	// "we did not check" are different facts, and a consumer that cannot tell
	// them apart will one day ship the second believing the first.
	RegistryVerdictStateUnenforced RegistryVerdictState = "unenforced"
)

// RegistryVerdictStates is every state, in declaration order.
var RegistryVerdictStates = []RegistryVerdictState{
	RegistryVerdictStateVerified,
	RegistryVerdictStatePending,
	RegistryVerdictStateOffline,
	RegistryVerdictStateUnreachable,
	RegistryVerdictStateHTTPError,
	RegistryVerdictStateMalformed,
	RegistryVerdictStateUnsigned,
	RegistryVerdictStateUntrusted,
	RegistryVerdictStateUnverifiable,
	RegistryVerdictStateUnenforced,
}

// Valid reports whether s is a state this build implements. An unknown state is
// never guessed at.
func (s RegistryVerdictState) Valid() bool {
	return slices.Contains(RegistryVerdictStates, s)
}

// PermitsAttachment reports whether a cartridge claiming provenance from a
// registry in this state may attach. True for Verified alone: every other
// state, the hopeful ones included, means the claim is unconfirmed.
func (s RegistryVerdictState) PermitsAttachment() bool {
	return s == RegistryVerdictStateVerified || s == RegistryVerdictStateUnenforced
}

// IsTrustFailure reports whether this state is a refusal of an answer we DID
// get, as opposed to not having got one. A refusal will not change on retry.
func (s RegistryVerdictState) IsTrustFailure() bool {
	switch s {
	case RegistryVerdictStateUnsigned, RegistryVerdictStateUntrusted, RegistryVerdictStateUnverifiable:
		return true
	default:
		return false
	}
}

// IsTransient reports whether an unattended retry could plausibly reach a
// different verdict. A trust failure never can; neither does a policy that
// forbids the request, until the policy changes.
func (s RegistryVerdictState) IsTransient() bool {
	switch s {
	case RegistryVerdictStatePending, RegistryVerdictStateUnreachable,
		RegistryVerdictStateHTTPError, RegistryVerdictStateMalformed:
		return true
	default:
		return false
	}
}

// RegistryRemedy is WHAT TO DO ABOUT A REGISTRY IN A GIVEN STATE. Mirrors Rust
// RegistryRemedy.
//
// The remedy follows from the state and nothing else. It used to be a sentence
// glued onto the failure message at the point the record was built — "Check the
// network connection and try again." — appended whatever the cause, so a
// signature a build could not read sent operators to their router. A remedy
// asserted as fact regardless of what failed is worse than none.
//
// This is the ACTION, not its wording: a CLI prints a line, a desktop client
// offers a control. Both derive them from here.
type RegistryRemedy string

const (
	// RegistryRemedyNone: nothing to do — the registry verified.
	RegistryRemedyNone RegistryRemedy = "none"
	// RegistryRemedyWait: a check is in flight and will answer on its own.
	RegistryRemedyWait RegistryRemedy = "wait"
	// RegistryRemedyCheckNetwork: the machine cannot reach the registry.
	RegistryRemedyCheckNetwork RegistryRemedy = "check_network"
	// RegistryRemedyChangeNetworkPolicy: this build was told not to go out.
	RegistryRemedyChangeNetworkPolicy RegistryRemedy = "change_network_policy"
	// RegistryRemedyRetryLater: the registry answered badly; its side to fix.
	RegistryRemedyRetryLater RegistryRemedy = "retry_later"
	// RegistryRemedyUpdateClient: this build cannot read the registry's
	// signature format. The registry is not at fault and the network is not
	// involved.
	RegistryRemedyUpdateClient RegistryRemedy = "update_client"
	// RegistryRemedyDoNotProceed: the registry's answer was rejected.
	RegistryRemedyDoNotProceed RegistryRemedy = "do_not_proceed"
)

// Remedy is the one thing to do about a registry in this state.
func (s RegistryVerdictState) Remedy() (RegistryRemedy, error) {
	switch s {
	case RegistryVerdictStateVerified, RegistryVerdictStateUnenforced:
		return RegistryRemedyNone, nil
	case RegistryVerdictStatePending:
		return RegistryRemedyWait, nil
	case RegistryVerdictStateOffline:
		return RegistryRemedyChangeNetworkPolicy, nil
	case RegistryVerdictStateUnreachable:
		return RegistryRemedyCheckNetwork, nil
	case RegistryVerdictStateHTTPError, RegistryVerdictStateMalformed:
		return RegistryRemedyRetryLater, nil
	case RegistryVerdictStateUnverifiable:
		return RegistryRemedyUpdateClient, nil
	case RegistryVerdictStateUnsigned, RegistryVerdictStateUntrusted:
		return RegistryRemedyDoNotProceed, nil
	default:
		return "", fmt.Errorf("unknown registry verdict state %q", string(s))
	}
}

// ChainFailureReason is why a signature chain failed, as a closed vocabulary.
// Mirrors Rust ChainFailureReason.
type ChainFailureReason string

const (
	ChainFailureReasonMalformedEnvelope            ChainFailureReason = "malformed_envelope"
	ChainFailureReasonUnsupportedEnvelopeFormat    ChainFailureReason = "unsupported_envelope_format"
	ChainFailureReasonMalformedCertificate         ChainFailureReason = "malformed_certificate"
	ChainFailureReasonUnsupportedCertificateFormat ChainFailureReason = "unsupported_certificate_format"
	ChainFailureReasonEmptyCertificateList         ChainFailureReason = "empty_certificate_list"
	ChainFailureReasonInsufficientRootSignatures   ChainFailureReason = "insufficient_root_signatures"
	ChainFailureReasonExpiredCertificate           ChainFailureReason = "expired_certificate"
	ChainFailureReasonNotYetValidCertificate       ChainFailureReason = "not_yet_valid_certificate"
	ChainFailureReasonEnvironmentMismatch          ChainFailureReason = "environment_mismatch"
	ChainFailureReasonKeyIDMismatch                ChainFailureReason = "key_id_mismatch"
	ChainFailureReasonNoAuthorizingCertificate     ChainFailureReason = "no_authorizing_certificate"
	ChainFailureReasonManifestSignatureInvalid     ChainFailureReason = "manifest_signature_invalid"
)

// ChainFailureReasons is every reason, in declaration order.
var ChainFailureReasons = []ChainFailureReason{
	ChainFailureReasonMalformedEnvelope,
	ChainFailureReasonUnsupportedEnvelopeFormat,
	ChainFailureReasonMalformedCertificate,
	ChainFailureReasonUnsupportedCertificateFormat,
	ChainFailureReasonEmptyCertificateList,
	ChainFailureReasonInsufficientRootSignatures,
	ChainFailureReasonExpiredCertificate,
	ChainFailureReasonNotYetValidCertificate,
	ChainFailureReasonEnvironmentMismatch,
	ChainFailureReasonKeyIDMismatch,
	ChainFailureReasonNoAuthorizingCertificate,
	ChainFailureReasonManifestSignatureInvalid,
}

// Valid reports whether r is a reason this build implements.
func (r ChainFailureReason) Valid() bool {
	return slices.Contains(ChainFailureReasons, r)
}

// RegistryVerdictStateForChainFailure is the verdict a chain failure produces.
//
// COULD THE CHAIN BE EVALUATED AT ALL? A format this build does not implement,
// or bytes it cannot parse, means no judgement was reached (Unverifiable —
// update the client). Everything else means the chain WAS judged and found
// wanting (Untrusted — do not proceed). Leaving this decision to each consumer
// is how one client reported an unreadable signature format as a network
// outage.
func RegistryVerdictStateForChainFailure(reason ChainFailureReason) (RegistryVerdictState, error) {
	switch reason {
	case ChainFailureReasonMalformedEnvelope, ChainFailureReasonUnsupportedEnvelopeFormat,
		ChainFailureReasonMalformedCertificate, ChainFailureReasonUnsupportedCertificateFormat,
		ChainFailureReasonEmptyCertificateList:
		return RegistryVerdictStateUnverifiable, nil
	case ChainFailureReasonInsufficientRootSignatures, ChainFailureReasonExpiredCertificate,
		ChainFailureReasonNotYetValidCertificate, ChainFailureReasonEnvironmentMismatch,
		ChainFailureReasonKeyIDMismatch, ChainFailureReasonNoAuthorizingCertificate,
		ChainFailureReasonManifestSignatureInvalid:
		return RegistryVerdictStateUntrusted, nil
	default:
		return "", fmt.Errorf("unknown chain failure reason %q", string(reason))
	}
}

// RegistryVerdict is what a consumer concluded about one registry, and why.
// Mirrors Rust RegistryVerdict.
//
// Illegal combinations are unrepresentable: the constructors take exactly what
// their state requires, and Validate re-checks every invariant on the way in
// from the wire. A verdict that says "http_error" without a status, or
// "verified" with a failure detail, is a bug in the producer and is refused at
// the boundary rather than rendered as a contradiction.
type RegistryVerdict struct {
	// RegistryURL is the registry this verdict is about — the verbatim URL a
	// cartridge declares, which is what consumers join on.
	RegistryURL string               `json:"registry_url"`
	State       RegistryVerdictState `json:"state"`
	// Detail is one operator-visible line saying what happened. Empty
	// exactly when the state states no failure (Verified, Pending).
	Detail string `json:"detail"`
	// HTTPStatus is the status the registry answered with. Present exactly
	// on HTTPError.
	HTTPStatus *int `json:"http_status"`
	// ChainFailure is which chain check failed. Present exactly on Untrusted
	// and Unverifiable — never on Unsigned, where there was no chain.
	ChainFailure *ChainFailureReason `json:"chain_failure"`
	// CheckedAtUnixSeconds is when this verdict was reached.
	CheckedAtUnixSeconds int64 `json:"checked_at_unix_seconds"`
}

// NewVerifiedRegistryVerdict: the registry answered, verified and parsed.
func NewVerifiedRegistryVerdict(registryURL string, checkedAtUnixSeconds int64) (RegistryVerdict, error) {
	v := RegistryVerdict{RegistryURL: registryURL, State: RegistryVerdictStateVerified, CheckedAtUnixSeconds: checkedAtUnixSeconds}
	return v, v.Validate()
}

// NewUnenforcedRegistryVerdict: this build bakes no trust anchors, so the
// manifest was accepted without proof — and says so rather than claiming it
// verified.
func NewUnenforcedRegistryVerdict(registryURL string, checkedAtUnixSeconds int64) (RegistryVerdict, error) {
	v := RegistryVerdict{RegistryURL: registryURL, State: RegistryVerdictStateUnenforced, CheckedAtUnixSeconds: checkedAtUnixSeconds}
	return v, v.Validate()
}

// NewPendingRegistryVerdict: no verdict yet. Carries no time, because nothing
// has been checked.
func NewPendingRegistryVerdict(registryURL string) (RegistryVerdict, error) {
	v := RegistryVerdict{RegistryURL: registryURL, State: RegistryVerdictStatePending}
	return v, v.Validate()
}

// NewStatedRegistryVerdict builds a state that carries only a detail line:
// Offline, Unreachable, Malformed, Unsigned. The other states have their own
// constructors because they require more, and this refuses them rather than
// letting a caller build a verdict missing what it needs.
func NewStatedRegistryVerdict(registryURL string, state RegistryVerdictState, detail string, checkedAtUnixSeconds int64) (RegistryVerdict, error) {
	switch state {
	case RegistryVerdictStateOffline, RegistryVerdictStateUnreachable,
		RegistryVerdictStateMalformed, RegistryVerdictStateUnsigned:
	case RegistryVerdictStateVerified, RegistryVerdictStatePending, RegistryVerdictStateUnenforced:
		return RegistryVerdict{}, fmt.Errorf("a %q verdict states no failure, so it carries no detail (got %q)", string(state), detail)
	case RegistryVerdictStateHTTPError:
		return RegistryVerdict{}, fmt.Errorf("an 'http_error' verdict must carry the status the registry answered with")
	case RegistryVerdictStateUntrusted, RegistryVerdictStateUnverifiable:
		return RegistryVerdict{}, fmt.Errorf("a %q verdict must carry the chain failure reason that produced it", string(state))
	default:
		return RegistryVerdict{}, fmt.Errorf("unknown registry verdict state %q", string(state))
	}
	v := RegistryVerdict{RegistryURL: registryURL, State: state, Detail: detail, CheckedAtUnixSeconds: checkedAtUnixSeconds}
	return v, v.Validate()
}

// NewHTTPErrorRegistryVerdict: the registry answered with an HTTP error.
func NewHTTPErrorRegistryVerdict(registryURL string, status int, detail string, checkedAtUnixSeconds int64) (RegistryVerdict, error) {
	v := RegistryVerdict{
		RegistryURL: registryURL, State: RegistryVerdictStateHTTPError, Detail: detail,
		HTTPStatus: &status, CheckedAtUnixSeconds: checkedAtUnixSeconds,
	}
	return v, v.Validate()
}

// NewChainFailedRegistryVerdict: a signature chain that failed. The state
// FOLLOWS from the reason, so a caller cannot file an unreadable format as a
// rejected key or the other way round.
func NewChainFailedRegistryVerdict(registryURL string, reason ChainFailureReason, detail string, checkedAtUnixSeconds int64) (RegistryVerdict, error) {
	state, err := RegistryVerdictStateForChainFailure(reason)
	if err != nil {
		return RegistryVerdict{}, err
	}
	r := reason
	v := RegistryVerdict{
		RegistryURL: registryURL, State: state, Detail: detail,
		ChainFailure: &r, CheckedAtUnixSeconds: checkedAtUnixSeconds,
	}
	return v, v.Validate()
}

// Validate checks every invariant this type promises. A verdict that fails this
// has no meaning and must not travel.
func (v RegistryVerdict) Validate() error {
	if v.RegistryURL == "" {
		return fmt.Errorf("a registry verdict must name the registry it is about")
	}
	if !v.State.Valid() {
		return fmt.Errorf("unknown registry verdict state %q", string(v.State))
	}
	statesNoFailure := v.State == RegistryVerdictStateVerified ||
		v.State == RegistryVerdictStatePending ||
		v.State == RegistryVerdictStateUnenforced
	if statesNoFailure && v.Detail != "" {
		return fmt.Errorf("a %q verdict states no failure, so it carries no detail (got %q)", string(v.State), v.Detail)
	}
	if !statesNoFailure && v.Detail == "" {
		return fmt.Errorf("a %q verdict must carry the detail that explains it", string(v.State))
	}
	if v.State == RegistryVerdictStateHTTPError {
		if v.HTTPStatus == nil {
			return fmt.Errorf("an 'http_error' verdict must carry the status the registry answered with")
		}
	} else if v.HTTPStatus != nil {
		return fmt.Errorf("only an 'http_error' verdict carries an HTTP status (got one on %q)", string(v.State))
	}
	chainStates := v.State == RegistryVerdictStateUntrusted || v.State == RegistryVerdictStateUnverifiable
	if chainStates {
		if v.ChainFailure == nil {
			return fmt.Errorf("a %q verdict must carry the chain failure reason that produced it", string(v.State))
		}
		produced, err := RegistryVerdictStateForChainFailure(*v.ChainFailure)
		if err != nil {
			return err
		}
		if produced != v.State {
			return fmt.Errorf("only a trust failure carries a chain failure reason (got one on %q)", string(v.State))
		}
	} else if v.ChainFailure != nil {
		return fmt.Errorf("only a trust failure carries a chain failure reason (got one on %q)", string(v.State))
	}
	return nil
}

// PermitsAttachment reports whether a cartridge from this registry may attach.
func (v RegistryVerdict) PermitsAttachment() bool { return v.State.PermitsAttachment() }

// StatesTheSameAs reports whether two verdicts say the same thing about the
// registry.
//
// Not equality. A verdict carries CheckedAtUnixSeconds, which is provenance
// about the CHECK and not about the registry — so a consumer asking "did this
// change?" by comparing whole verdicts is told yes on every re-check, forever.
// Both desktop clients asked exactly that to decide whether to re-run cartridge
// discovery, and the answer drove a loop that left the engine discovering
// cartridges and never reaching ready.
func (v RegistryVerdict) StatesTheSameAs(other RegistryVerdict) bool {
	if v.HTTPStatus == nil || other.HTTPStatus == nil {
		if (v.HTTPStatus == nil) != (other.HTTPStatus == nil) {
			return false
		}
	} else if *v.HTTPStatus != *other.HTTPStatus {
		return false
	}
	if v.ChainFailure == nil || other.ChainFailure == nil {
		if (v.ChainFailure == nil) != (other.ChainFailure == nil) {
			return false
		}
	} else if *v.ChainFailure != *other.ChainFailure {
		return false
	}
	return v.RegistryURL == other.RegistryURL &&
		v.State == other.State &&
		v.Detail == other.Detail
}

// UnmarshalJSON decodes and validates in one step: a contradictory verdict is
// refused ON THE WAY IN, where the producer can still be named, rather than
// surfacing later as an interface that says two things at once.
func (v *RegistryVerdict) UnmarshalJSON(data []byte) error {
	type wire RegistryVerdict
	var raw wire
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	decoded := RegistryVerdict(raw)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*v = decoded
	return nil
}
