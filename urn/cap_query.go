package urn

// Asking the fabric for a cap.
//
// A request is not a cap. A cap takes this and gives that; a request is a
// QUESTION about caps. A call always says what it sends, but a search may leave
// a side unasked: "what gives me this file, whatever it takes?", "what can this
// input become?". Those are not cap URNs with media: on a side — media: is the
// type "anything", a claim about every input — they are queries with that side
// unknown.
//
// A query asks of what a cap GIVES for what is sent: a declared output, read
// complete — what it does not mention, the cap does not give — or, for an
// effect=none or effect=patch cap, what it was sent with its effect applied.
//
// CapQuery is such a question, and MatchGrade is how a registered cap answers
// it. A cap URN read as a typed call or as a pattern is one (CapQueryFromCall,
// CapQueryFromPattern) — which is what CapUrn.IsDispatchable and CapUrn.Accepts
// ask — and so are the questions no cap URN can spell (CapQueryProducing,
// CapQueryConsuming, CapQueryBetween).
//
// Every answer is decided by the proved model (formal/CapDAG/Query.lean).

import (
	"fmt"

	capformal "github.com/machinefabric/capdag-go/formal"
	taggedurn "github.com/machinefabric/tagged-urn-go"
)

// MatchGrade is how a registered cap answers a CapQuery.
type MatchGrade string

const (
	// MatchExact: guaranteed, and exactly what was asked on every side that was
	// asked.
	MatchExact MatchGrade = "exact"
	// MatchGuaranteed: whatever the cap takes and gives, it is what was asked.
	MatchGuaranteed MatchGrade = "guaranteed"
	// MatchPossible: not guaranteed and not excluded — only running it tells.
	// For exploring; a call is never routed on it.
	MatchPossible MatchGrade = "possible"
	// MatchNone: excluded.
	MatchNone MatchGrade = "none"
)

// IsGuaranteed reports whether routing may act on the grade.
func (g MatchGrade) IsGuaranteed() bool {
	return g == MatchExact || g == MatchGuaranteed
}

// IsPossible reports whether a search should show the cap.
func (g MatchGrade) IsPossible() bool {
	return g != MatchNone
}

// CapQuery is a question about caps.
type CapQuery struct {
	formal capformal.WfQuery
	asked  string
}

// String is what was asked, in words.
func (q *CapQuery) String() string {
	return q.asked
}

// tagPattern is the cap-tag pattern a query asks for: `cap:` with these tags.
// A nil or empty map asks for none; a key that is no identifier, or a
// constraint no URN writes, is refused.
func tagPattern(tags map[string]taggedurn.Constraint) (*taggedurn.TaggedUrn, error) {
	return taggedurn.NewTaggedUrnFromTags("cap", tags)
}

// CapQueryFromCall is the cap URN call, read as a typed call: it sends what its
// input says — media:, if it says nothing, is anything — and asks for its
// output, tags and effect. What CapUrn.IsDispatchable asks.
func CapQueryFromCall(call *CapUrn) *CapQuery {
	return &CapQuery{
		formal: decided(capformal.QueryOfCall(call.formal)),
		asked:  fmt.Sprintf("call %s", call),
	}
}

// CapQueryFromPattern is the cap URN pattern, read as a pattern over caps: caps
// taking within its input, everything they give of the type its output names;
// an output it leaves open asks nothing. What CapUrn.Accepts asks.
func CapQueryFromPattern(pattern *CapUrn) *CapQuery {
	return &CapQuery{
		formal: decided(capformal.QueryOfPattern(pattern.formal)),
		asked:  fmt.Sprintf("pattern %s", pattern),
	}
}

// CapQueryProducing asks for caps that GIVE output, whatever they take, with
// the cap-tags tags asks for.
func CapQueryProducing(output *MediaUrn, tags map[string]taggedurn.Constraint) (*CapQuery, error) {
	pattern, err := tagPattern(tags)
	if err != nil {
		return nil, err
	}
	return &CapQuery{
		formal: decided(capformal.QueryProducing(output.inner.Formal(), pattern.Formal())),
		asked:  fmt.Sprintf("anything giving %s", output),
	}, nil
}

// CapQueryConsuming asks for caps that TAKE input, whatever they give: what
// this input can become in one step.
func CapQueryConsuming(input *MediaUrn, tags map[string]taggedurn.Constraint) (*CapQuery, error) {
	pattern, err := tagPattern(tags)
	if err != nil {
		return nil, err
	}
	return &CapQuery{
		formal: decided(capformal.QueryConsuming(input.inner.Formal(), pattern.Formal())),
		asked:  fmt.Sprintf("anything taking %s", input),
	}, nil
}

// CapQueryBetween asks for caps that take input and give output: nothing
// unknown.
func CapQueryBetween(input, output *MediaUrn, tags map[string]taggedurn.Constraint) (*CapQuery, error) {
	pattern, err := tagPattern(tags)
	if err != nil {
		return nil, err
	}
	return &CapQuery{
		formal: decided(capformal.QueryBetween(input.inner.Formal(), output.inner.Formal(), pattern.Formal())),
		asked:  fmt.Sprintf("anything taking %s and giving %s", input, output),
	}, nil
}

// Admits reports whether cap is guaranteed to be what is asked.
func (q *CapQuery) Admits(cap *CapUrn) bool {
	return decided(capformal.QueryAdmits(q.formal, cap.formal))
}

// MayAdmit reports whether cap could be what is asked.
func (q *CapQuery) MayAdmit(cap *CapUrn) bool {
	return decided(capformal.QueryMayAdmit(q.formal, cap.formal))
}

// Grade is how cap answers.
func (q *CapQuery) Grade(cap *CapUrn) MatchGrade {
	switch answer := decided(capformal.QueryGrade(q.formal, cap.formal)).(type) {
	case capformal.GradeExact:
		return MatchExact
	case capformal.GradeGuaranteed:
		return MatchGuaranteed
	case capformal.GradePossible:
		return MatchPossible
	case capformal.GradeNone:
		return MatchNone
	default:
		panic(fmt.Sprintf("capdag: the model answered with a grade this mirror does not know: %T", answer))
	}
}
