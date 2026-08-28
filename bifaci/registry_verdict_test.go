package bifaci

import (
	"encoding/json"
	"testing"
)

// The registry-trust vocabulary, mirrored from Rust
// `bifaci/registry_verdict.rs`. These tests pin the same facts its Rust, Swift
// and JavaScript twins do — a mirror that drifts stops understanding its own
// producers, which is the failure this vocabulary exists to make impossible.

const verdictTestURL = "https://cartridges.example/v1/manifest"
const verdictTestNow int64 = 1_700_000_000

// TEST8150: the wire vocabulary is closed and matches the other mirrors.
func TestTest8150RegistryVerdictWireVocabularyIsClosed(t *testing.T) {
	expected := []string{
		"verified", "pending", "offline", "unreachable",
		"http_error", "malformed", "unsigned", "untrusted", "unverifiable", "unenforced",
	}
	if len(RegistryVerdictStates) != len(expected) {
		t.Fatalf("state count %d, want %d — the mirrors must carry the same vocabulary",
			len(RegistryVerdictStates), len(expected))
	}
	for i, want := range expected {
		if string(RegistryVerdictStates[i]) != want {
			t.Errorf("state %d is %q, want %q", i, RegistryVerdictStates[i], want)
		}
	}
	reasons := []string{
		"malformed_envelope", "unsupported_envelope_format", "malformed_certificate",
		"unsupported_certificate_format", "empty_certificate_list",
		"insufficient_root_signatures", "expired_certificate", "not_yet_valid_certificate",
		"environment_mismatch", "key_id_mismatch", "no_authorizing_certificate",
		"manifest_signature_invalid",
	}
	if len(ChainFailureReasons) != len(reasons) {
		t.Fatalf("reason count %d, want %d", len(ChainFailureReasons), len(reasons))
	}
	for i, want := range reasons {
		if string(ChainFailureReasons[i]) != want {
			t.Errorf("reason %d is %q, want %q", i, ChainFailureReasons[i], want)
		}
	}
	if RegistryVerdictState("network_error").Valid() {
		t.Error("an unknown state must not validate")
	}
}

// TEST8151: a format this build cannot read is OUR limitation — never the
// registry being untrustworthy, and never a network problem.
func TestTest8151UnreadableFormatIsUnverifiable(t *testing.T) {
	unevaluable := []ChainFailureReason{
		ChainFailureReasonMalformedEnvelope,
		ChainFailureReasonUnsupportedEnvelopeFormat,
		ChainFailureReasonMalformedCertificate,
		ChainFailureReasonUnsupportedCertificateFormat,
		ChainFailureReasonEmptyCertificateList,
	}
	for _, reason := range unevaluable {
		state, err := RegistryVerdictStateForChainFailure(reason)
		if err != nil {
			t.Fatalf("%q: %v", reason, err)
		}
		if state != RegistryVerdictStateUnverifiable {
			t.Errorf("%q is %q, want unverifiable — it could not be judged at all", reason, state)
		}
	}
	judged := []ChainFailureReason{
		ChainFailureReasonInsufficientRootSignatures,
		ChainFailureReasonExpiredCertificate,
		ChainFailureReasonNotYetValidCertificate,
		ChainFailureReasonEnvironmentMismatch,
		ChainFailureReasonKeyIDMismatch,
		ChainFailureReasonNoAuthorizingCertificate,
		ChainFailureReasonManifestSignatureInvalid,
	}
	for _, reason := range judged {
		state, err := RegistryVerdictStateForChainFailure(reason)
		if err != nil {
			t.Fatalf("%q: %v", reason, err)
		}
		if state != RegistryVerdictStateUntrusted {
			t.Errorf("%q is %q, want untrusted — the chain WAS judged", reason, state)
		}
	}
	if _, err := RegistryVerdictStateForChainFailure("bad_signature"); err == nil {
		t.Error("an unknown reason must be refused, not classified")
	}
}

// TEST8152: only a verified registry lets a cartridge attach — pending
// included, which must never read as permission.
func TestTest8152OnlyVerifiedPermitsAttachment(t *testing.T) {
	for _, state := range RegistryVerdictStates {
		want := state == RegistryVerdictStateVerified || state == RegistryVerdictStateUnenforced
		if state.PermitsAttachment() != want {
			t.Errorf("%q.PermitsAttachment() = %v, want %v", state, state.PermitsAttachment(), want)
		}
	}
	// A DEV BUILD HAS TO WORK, and it says which of the two it is: "we checked
	// and it passed" and "we did not check" are different facts.
	if !RegistryVerdictStateUnenforced.PermitsAttachment() {
		t.Error("a build with no trust anchors must still attach its cartridges")
	}
	if RegistryVerdictStateUnenforced.IsTrustFailure() || RegistryVerdictStateUnenforced.IsTransient() {
		t.Error("unenforced is neither a refusal nor something a retry changes")
	}
}

// TEST8153: a refusal never resolves itself, so nothing may present it as worth
// retrying.
func TestTest8153TrustFailuresAreNeverTransient(t *testing.T) {
	for _, state := range RegistryVerdictStates {
		if state.IsTrustFailure() && state.IsTransient() {
			t.Errorf("%q cannot be both a refusal and something a retry could fix", state)
		}
	}
	if !RegistryVerdictStateUnverifiable.IsTrustFailure() {
		t.Error("unverifiable is a refusal of an answer we got")
	}
	if !RegistryVerdictStateUnreachable.IsTransient() {
		t.Error("unreachable is worth retrying")
	}
	// Policy is not transient: it holds until an operator changes it.
	if RegistryVerdictStateOffline.IsTransient() || RegistryVerdictStateOffline.IsTrustFailure() {
		t.Error("offline is neither transient nor a trust failure")
	}
}

// TEST8159: the remedy follows from the state, and "check the network" is
// reachable from exactly one state. That sentence used to be appended to every
// held-cartridge message whatever the cause, which is how a signature format a
// build could not read sent operators to their router.
func TestTest8159RemedyFollowsFromTheState(t *testing.T) {
	var network []RegistryVerdictState
	for _, state := range RegistryVerdictStates {
		remedy, err := state.Remedy()
		if err != nil {
			t.Fatalf("%q: %v", state, err)
		}
		if remedy == RegistryRemedyCheckNetwork {
			network = append(network, state)
		}
		if state.IsTrustFailure() && remedy != RegistryRemedyDoNotProceed && remedy != RegistryRemedyUpdateClient {
			t.Errorf("%q is a refusal; its remedy must not be a retry (got %q)", state, remedy)
		}
	}
	if len(network) != 1 || network[0] != RegistryVerdictStateUnreachable {
		t.Errorf("only a registry we could not reach is a network problem, got %v", network)
	}
	// The one that was misclassified: our limitation, so update the client —
	// never distrust the registry, never touch the network.
	if remedy, _ := RegistryVerdictStateUnverifiable.Remedy(); remedy != RegistryRemedyUpdateClient {
		t.Errorf("unverifiable remedy is %q", remedy)
	}
	if remedy, _ := RegistryVerdictStateUntrusted.Remedy(); remedy != RegistryRemedyDoNotProceed {
		t.Errorf("untrusted remedy is %q", remedy)
	}
	// Policy is the operator's setting, not their router.
	if remedy, _ := RegistryVerdictStateOffline.Remedy(); remedy != RegistryRemedyChangeNetworkPolicy {
		t.Errorf("offline remedy is %q", remedy)
	}
	if _, err := RegistryVerdictState("flaky").Remedy(); err == nil {
		t.Error("an unknown state has no remedy and must be refused")
	}
}

// TEST8154: illegal states are unrepresentable — every contradiction is refused
// at construction and again at the wire boundary.
func TestTest8154ContradictoryVerdictsAreRefused(t *testing.T) {
	if _, err := NewStatedRegistryVerdict(verdictTestURL, RegistryVerdictStateUnreachable, "", verdictTestNow); err == nil {
		t.Error("a failure with nothing said about it must be refused")
	}
	if _, err := NewStatedRegistryVerdict(verdictTestURL, RegistryVerdictStateVerified, "all good", verdictTestNow); err == nil {
		t.Error("success carrying a failure detail must be refused")
	}
	if _, err := NewStatedRegistryVerdict(verdictTestURL, RegistryVerdictStateHTTPError, "500", verdictTestNow); err == nil {
		t.Error("an http_error with no status must be refused")
	}
	if _, err := NewStatedRegistryVerdict(verdictTestURL, RegistryVerdictStateUntrusted, "nope", verdictTestNow); err == nil {
		t.Error("a trust failure with no reason must be refused")
	}
	if _, err := NewStatedRegistryVerdict("", RegistryVerdictStateUnreachable, "timeout", verdictTestNow); err == nil {
		t.Error("a verdict about no registry at all must be refused")
	}

	// A status on a state that never answered, smuggled in over the wire.
	smuggled := `{"registry_url":"` + verdictTestURL + `","state":"unreachable","detail":"timeout",` +
		`"http_status":404,"chain_failure":null,"checked_at_unix_seconds":1700000000}`
	var decoded RegistryVerdict
	if err := json.Unmarshal([]byte(smuggled), &decoded); err == nil {
		t.Error("a verdict that contradicts itself must be refused where the producer can still be named")
	}
	// A reason that contradicts the state it is filed under.
	contradiction := `{"registry_url":"` + verdictTestURL + `","state":"untrusted","detail":"x",` +
		`"http_status":null,"chain_failure":"unsupported_envelope_format","checked_at_unix_seconds":1700000000}`
	if err := json.Unmarshal([]byte(contradiction), &decoded); err == nil {
		t.Error("a reason that produces another state must be refused")
	}
}

// TEST8155: the wire form round-trips with its invariants intact.
func TestTest8155RegistryVerdictWireRoundTrip(t *testing.T) {
	verdict, err := NewChainFailedRegistryVerdict(
		verdictTestURL,
		ChainFailureReasonUnsupportedEnvelopeFormat,
		"envelope format 'other/1' is not implemented by this build",
		verdictTestNow,
	)
	if err != nil {
		t.Fatalf("building a chain failure: %v", err)
	}
	if verdict.State != RegistryVerdictStateUnverifiable {
		t.Fatalf("state is %q, want unverifiable", verdict.State)
	}
	data, err := json.Marshal(verdict)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded RegistryVerdict
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.State != verdict.State || decoded.Detail != verdict.Detail ||
		decoded.RegistryURL != verdict.RegistryURL {
		t.Errorf("round trip changed the verdict: %+v vs %+v", decoded, verdict)
	}
	if decoded.ChainFailure == nil || *decoded.ChainFailure != ChainFailureReasonUnsupportedEnvelopeFormat {
		t.Error("the failing check must travel with the verdict, not only in prose")
	}
	if decoded.PermitsAttachment() {
		t.Error("an unverifiable registry does not permit attachment")
	}

	http, err := NewHTTPErrorRegistryVerdict(verdictTestURL, 404, "registry answered HTTP 404", verdictTestNow)
	if err != nil {
		t.Fatalf("building an http error: %v", err)
	}
	if http.HTTPStatus == nil || *http.HTTPStatus != 404 {
		t.Error("the status must travel: 404 and 5xx are different situations")
	}

	statusless := `{"registry_url":"` + verdictTestURL + `","state":"http_error","detail":"answered badly",` +
		`"http_status":null,"chain_failure":null,"checked_at_unix_seconds":1700000000}`
	if err := json.Unmarshal([]byte(statusless), &decoded); err == nil {
		t.Error("an http_error with no status must be refused on the way in")
	}
	unknown := `{"registry_url":"` + verdictTestURL + `","state":"flaky","detail":"hm",` +
		`"http_status":null,"chain_failure":null,"checked_at_unix_seconds":1700000000}`
	if err := json.Unmarshal([]byte(unknown), &decoded); err == nil {
		t.Error("an unknown state must be refused, not guessed at")
	}
}

// TEST8157: the format discriminators are the library's, so no consumer can
// hold a divergent copy. A product rename that edits a client's private
// constant makes that client verify nothing while every other implementation
// keeps working — which is precisely what happened.
func TestTest8157SignatureFormatDiscriminatorsComeFromTheLibrary(t *testing.T) {
	if ManifestSigFormat != "machinefabric-manifest-sig/1" {
		t.Errorf("manifest envelope format is %q", ManifestSigFormat)
	}
	if ReleaseKeyCertFormat != "machinefabric-release-key-cert/1" {
		t.Errorf("release-key certificate format is %q", ReleaseKeyCertFormat)
	}
}

// TestTest8162AVerdictSaysTheSameThingAtADifferentTime — WHEN IS A VERDICT
// NEWS? Both desktop clients re-verify their registries after every discovery
// round and re-run discovery when the verdicts "changed". Change had to mean
// "the registry said something different", and comparing whole verdicts cannot
// mean that: they carry the moment of the check, so the same answer taken a
// second later is a different value. That is the loop that left an engine
// discovering cartridges forever.
func TestTest8162AVerdictSaysTheSameThingAtADifferentTime(t *testing.T) {
	earlier, err := NewVerifiedRegistryVerdict("https://r.example", 1756000000)
	if err != nil {
		t.Fatalf("verified verdict: %v", err)
	}
	later, err := NewVerifiedRegistryVerdict("https://r.example", 1756000931)
	if err != nil {
		t.Fatalf("verified verdict: %v", err)
	}
	if earlier == later {
		t.Fatal("they are not the same value — one is a later check")
	}
	if !earlier.StatesTheSameAs(later) {
		t.Fatal("but they say the same thing about the registry")
	}

	unreachable, err := NewStatedRegistryVerdict("https://r.example", RegistryVerdictStateUnreachable, "connection timed out", 1756000000)
	if err != nil {
		t.Fatalf("stated verdict: %v", err)
	}
	if earlier.StatesTheSameAs(unreachable) {
		t.Fatal("unreachable is a different statement about the registry")
	}

	notFound, err := NewHTTPErrorRegistryVerdict("https://r.example", 404, "the registry answered HTTP 404", 1756000000)
	if err != nil {
		t.Fatalf("http error verdict: %v", err)
	}
	unavailable, err := NewHTTPErrorRegistryVerdict("https://r.example", 503, "the registry answered HTTP 503", 1756000000)
	if err != nil {
		t.Fatalf("http error verdict: %v", err)
	}
	// 404 and 503 are different situations with different remedies.
	if notFound.StatesTheSameAs(unavailable) {
		t.Fatal("two http errors with different statuses are different statements")
	}
	if earlier.StatesTheSameAs(notFound) {
		t.Fatal("verified and an http error are different statements")
	}

	elsewhere, err := NewVerifiedRegistryVerdict("https://other.example/manifest", 1756000000)
	if err != nil {
		t.Fatalf("verified verdict: %v", err)
	}
	if earlier.StatesTheSameAs(elsewhere) {
		t.Fatal("a verdict about another registry is not the same statement")
	}
}
