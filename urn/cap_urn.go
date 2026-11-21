// Package capdag provides the fundamental cap URN system used across
// all CapDAG cartridges and candidates. It defines the formal structure for cap
// identifiers with flat tag-based naming, pattern matching, and graded specificity.
//
// Cap URN matching semantics:
//   - Pattern (handler) specifies constraints via its tags
//   - Instance (request) must satisfy all pattern constraints
//   - K=v: Instance must have key K with exact value v
//   - K=*: Wildcard - matches any value for that key
//   - (missing): Pattern doesn't constrain this key (accepts any)
//   - Instance missing a required tag → reject
//
// Uses TaggedUrn for parsing to ensure consistency across implementations.
package urn

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"

	capformal "github.com/machinefabric/capdag-go/formal"
	lungo "github.com/machinefabric/lungo-go"
	taggedurn "github.com/machinefabric/tagged-urn-go"
)

// CapKind is the functional category of a cap, derived from all
// three axes (in, out, and remaining tags). The classification is
// logical — the dispatch protocol does not branch on CapKind. Exposed
// for tools, UIs, planners, and tests so callers can reason about a
// cap's role without re-deriving the rules.
//
// media:void is the unit type (no meaningful value). media: is the
// top type (universal wildcard). With those anchors the five kinds
// fall out:
//
//	Kind        in            out           other tags  reads as
//	Identity    media:        media:        none        A → A
//	Source      media:void    not void      any         () → B
//	Sink        not void      media:void    any         A → ()
//	Effect      media:void    media:void    any         () → ()
//	Transform   anything else
//
// Identity is the fully generic cap on every axis: input wide open,
// output wide open, no operation/metadata tags. Adding any tag
// specifies something on the third axis and demotes the morphism to
// a Transform whose in/out happen to be the wildcards.
type CapKind string

const (
	CapKindIdentity  CapKind = "identity"
	CapKindSource    CapKind = "source"
	CapKindSink      CapKind = "sink"
	CapKindEffect    CapKind = "effect"
	CapKindTransform CapKind = "transform"
)

type CapEffect string

const (
	CapEffectDeclared CapEffect = "declared"
	CapEffectNone     CapEffect = "none"
	CapEffectPatch    CapEffect = "patch"
	CapEffectAny      CapEffect = "?"
)

// CapUrn represents a cap URN using flat, ordered tags with required direction specifiers
//
// Direction (in→out) is integral to a cap's identity. The `inSpec` and `outSpec`
// fields specify the input and output media URNs respectively.
//
// Examples:
// - cap:in="media:binary";generate;out="media:binary";target=thumbnail
// - cap:in="media:void";dimensions;out="media:integer"
// - cap:in="media:string";key="Value With Spaces";out="media:object"
type CapUrn struct {
	// inSpec is the input media URN - required (use media:void for caps with no input)
	inSpec string
	// outSpec is the output media URN - required
	outSpec string
	// effect is the runtime media/type effect coordinate
	effect string
	// tags are the cap-tags (the y axis), each the constraint it writes; in, out
	// and effect are never here
	tags map[string]taggedurn.Constraint
	// formal is the same cap on the proved model's side: its three URNs and its
	// effect. Dispatch, acceptance, equivalence and specificity are asked of it,
	// through code generated from ../formal (package formal). Built once, by
	// assemble, so it can never describe a different cap from the fields beside it.
	formal capformal.WfCap
}

// CapUrnError represents errors that can occur during cap URN operations
type CapUrnError struct {
	Code    int
	Message string
}

func (e *CapUrnError) Error() string {
	return e.Message
}

// Error codes for cap URN operations
const (
	ErrorInvalidFormat         = 1
	ErrorEmptyTag              = 2
	ErrorInvalidCharacter      = 3
	ErrorInvalidTagFormat      = 4
	ErrorMissingCapPrefix      = 5
	ErrorDuplicateKey          = 6
	ErrorUnterminatedQuote     = 8
	ErrorInvalidEscapeSequence = 9
	ErrorMissingInSpec         = 10
	ErrorMissingOutSpec        = 11
	ErrorInvalidMediaUrn       = 12
	ErrorInvalidEffect         = 13
	ErrorInvalidEffectApply    = 14
	ErrorIllegalDeclaration    = 15
)

// writtenTag is one tag as a tagged URN writes it, for an error that names it.
func writtenTag(key string, constraint taggedurn.Constraint) string {
	urn, err := taggedurn.NewTaggedUrnFromTags("cap", map[string]taggedurn.Constraint{key: constraint})
	if err != nil {
		panic(fmt.Sprintf("capdag: a tag a tagged URN held is one it writes: %v", err))
	}
	text := urn.ToString()
	return text[len("cap:"):]
}

// normalizeEffectValue is the effect a cap's tag map writes: none is declared;
// the marker or ?effect is the unconstrained ?; an exact declared, none or patch
// is that effect; anything else is refused, named as written.
func normalizeEffectValue(raw taggedurn.Constraint, present bool) (string, error) {
	if !present {
		return string(CapEffectDeclared), nil
	}
	switch v := raw.(type) {
	case taggedurn.ConstraintPresent, taggedurn.ConstraintUnconstrained:
		return string(CapEffectAny), nil
	case taggedurn.ConstraintExact:
		switch v.Value {
		case "declared", "none", "patch":
			return v.Value, nil
		}
	}
	return "", &CapUrnError{
		Code: ErrorInvalidEffect,
		Message: fmt.Sprintf(
			"Unsupported effect '%s'. Supported values are declared, none, patch, or explicit unconstrained ?effect/effect=*",
			writtenTag("effect", raw),
		),
	}
}

// parseEffect is an effect given as text to a constructor: declared, none,
// patch or ?.
func parseEffect(effect string) (string, error) {
	switch CapEffect(effect) {
	case CapEffectDeclared, CapEffectNone, CapEffectPatch, CapEffectAny:
		return effect, nil
	}
	return "", &CapUrnError{
		Code:    ErrorInvalidEffect,
		Message: fmt.Sprintf("Unsupported effect '%s'. Supported values are declared, none, patch, or ?", effect),
	}
}

// directionError is what a direction side says when its value is no media URN:
// a value that does not even name the media prefix, in the validation rules'
// words, and one that names it but does not parse, with the parser's reason.
func directionError(side, value string, err error) *CapUrnError {
	code := ErrorInvalidMediaUrn
	var taggedErr *taggedurn.TaggedUrnError
	notMedia := false
	if errors.As(err, &taggedErr) {
		switch taggedErr.Code {
		case taggedurn.ErrorMissingPrefix, taggedurn.ErrorEmptyPrefix, taggedurn.ErrorPrefixMismatch:
			notMedia = true
		}
	}
	if notMedia {
		return &CapUrnError{Code: code, Message: fmt.Sprintf("Invalid '%s' media URN: %s. Must start with 'media:' or be '*'", side, value)}
	}
	return &CapUrnError{Code: code, Message: fmt.Sprintf("Invalid '%s' media URN: %s: %v", side, value, err)}
}

// directionMedia is a direction side's media URN, in canonical form. An empty
// value is refused.
func directionMedia(side, value string) (string, error) {
	if value == "" {
		return "", &CapUrnError{Code: ErrorInvalidMediaUrn, Message: fmt.Sprintf("Empty value for '%s' tag is not allowed", side)}
	}
	media, err := NewMediaUrnFromString(value)
	if err != nil {
		return "", directionError(side, value, err)
	}
	return media.String(), nil
}

// directionSpec is a direction spec given as text to a constructor or the
// builder: * is the top media media:, as the marker is in parsing; anything else
// is a media URN, in canonical form.
func directionSpec(side, raw string) (string, error) {
	if raw == "*" {
		return "media:", nil
	}
	return directionMedia(side, raw)
}

// processDirectionTag reads a direction tag as a cap's tag map holds it: missing,
// or the marker (in, in=*), is media:; an exact value is that media URN in
// canonical form; any other form is refused.
func processDirectionTag(tag taggedurn.Constraint, present bool, side string) (string, error) {
	if !present {
		return "media:", nil
	}
	switch v := tag.(type) {
	case taggedurn.ConstraintPresent:
		return "media:", nil
	case taggedurn.ConstraintExact:
		return directionMedia(side, v.Value)
	}
	return "", &CapUrnError{
		Code:    ErrorInvalidMediaUrn,
		Message: fmt.Sprintf("Invalid '%s' media URN: %s. Must start with 'media:' or be '*'", side, writtenTag(side, tag)),
	}
}

// structuralKeys are the keys a cap carries apart from its cap-tags.
var structuralKeys = map[string]bool{"in": true, "out": true, "effect": true}

// assemble is the one way a CapUrn is made: the fields, the model's value built from
// exactly them, and the admissibility check — so no constructor or edit can produce a
// cap whose model value describes something else. inSpec and outSpec are canonical
// media URNs; tags are the cap-tags, which may not name a structural key.
func assemble(inSpec, outSpec, effect string, tags map[string]taggedurn.Constraint) (*CapUrn, error) {
	for k := range tags {
		if structuralKeys[strings.ToLower(k)] {
			return nil, &CapUrnError{
				Code:    ErrorInvalidTagFormat,
				Message: fmt.Sprintf("reserved structural key '%s' must be passed as in_urn, out_urn or effect, not as a tag", k),
			}
		}
	}
	inMedia, err := NewMediaUrnFromString(inSpec)
	if err != nil {
		return nil, directionError("in", inSpec, err)
	}
	outMedia, err := NewMediaUrnFromString(outSpec)
	if err != nil {
		return nil, directionError("out", outSpec, err)
	}
	other, err := taggedurn.NewTaggedUrnFromTags("cap", tags)
	if err != nil {
		return nil, capUrnErrorFromTaggedUrn(err)
	}
	var formalEffect capformal.Effect
	switch CapEffect(effect) {
	case CapEffectDeclared:
		formalEffect = capformal.EffectDeclared{}
	case CapEffectNone:
		formalEffect = capformal.EffectNone{}
	case CapEffectPatch:
		formalEffect = capformal.EffectPatch{}
	case CapEffectAny:
		formalEffect = capformal.EffectUnspecified{}
	default:
		return nil, &CapUrnError{Code: ErrorInvalidEffect, Message: fmt.Sprintf("effect '%s' is not declared, none, patch or ?", effect)}
	}
	cap := &CapUrn{
		inSpec:  inSpec,
		outSpec: outSpec,
		effect:  effect,
		tags:    other.AllTags(),
		formal: capformal.WfCap{
			Input:  inMedia.inner.Formal(),
			Output: outMedia.inner.Formal(),
			Other:  other.Formal(),
			Effect: formalEffect,
		},
	}
	if err := cap.validateAdmissible(); err != nil {
		return nil, err
	}
	return cap, nil
}

// decided is the model's answer, or a panic: a call into the generated program fails
// only when the runtime does, never on a cap this package built.
func decided[T any](answer T, err error) T {
	if err != nil {
		panic(fmt.Sprintf("capdag: the model could not decide: %v", err))
	}
	return answer
}

// capUrnErrorFromTaggedUrn carries a tagged URN error's code into a cap URN error.
func capUrnErrorFromTaggedUrn(err error) *CapUrnError {
	var taggedErr *taggedurn.TaggedUrnError
	if !errors.As(err, &taggedErr) {
		panic(fmt.Sprintf("capdag: a tagged URN failed with no TaggedUrnError: %v", err))
	}
	code := map[int]int{
		taggedurn.ErrorInvalidFormat:         ErrorInvalidFormat,
		taggedurn.ErrorEmptyTag:              ErrorEmptyTag,
		taggedurn.ErrorInvalidCharacter:      ErrorInvalidCharacter,
		taggedurn.ErrorInvalidTagFormat:      ErrorInvalidTagFormat,
		taggedurn.ErrorMissingPrefix:         ErrorMissingCapPrefix,
		taggedurn.ErrorDuplicateKey:          ErrorDuplicateKey,
		taggedurn.ErrorUnterminatedQuote:     ErrorUnterminatedQuote,
		taggedurn.ErrorInvalidEscapeSequence: ErrorInvalidEscapeSequence,
		taggedurn.ErrorEmptyPrefix:           ErrorMissingCapPrefix,
		taggedurn.ErrorPrefixMismatch:        ErrorMissingCapPrefix,
		taggedurn.ErrorWhitespaceInInput:     ErrorInvalidCharacter,
	}[taggedErr.Code]
	if code == 0 {
		panic(fmt.Sprintf("capdag: tagged URN error code %d has no cap URN code", taggedErr.Code))
	}
	return &CapUrnError{Code: code, Message: taggedErr.Message}
}

// NewCapUrnFromString creates a cap URN from a string
// Format: cap:in="media:...";out="media:...";key1=value1;...
// The "cap:" prefix is mandatory. A missing in or out, or the marker (in, in=*),
// is media:; an exact value is a media URN; a quoted in="*" is the literal value
// *, which is no media URN.
// Trailing semicolons are optional and ignored
// Tags are automatically sorted alphabetically for canonical form
//
// Case handling:
// - Keys: Always normalized to lowercase
// - Unquoted values: Normalized to lowercase
// - Quoted values: Case preserved exactly as specified
func NewCapUrnFromString(s string) (*CapUrn, error) {
	taggedUrn, err := taggedurn.NewTaggedUrnFromString(s)
	if err != nil {
		return nil, capUrnErrorFromTaggedUrn(err)
	}
	if taggedUrn.GetPrefix() != "cap" {
		return nil, &CapUrnError{
			Code:    ErrorMissingCapPrefix,
			Message: "cap URN must start with 'cap:'",
		}
	}
	return NewCapUrnFromTags(taggedUrn.AllTags())
}

// NewCapUrnFromTags creates a cap URN from a tag map, as string parsing reads one:
// in, out and effect are taken out of it and read exactly as NewCapUrnFromString
// reads them, and the rest are the cap-tags.
func NewCapUrnFromTags(tags map[string]taggedurn.Constraint) (*CapUrn, error) {
	rest := make(map[string]taggedurn.Constraint, len(tags))
	for k, v := range tags {
		rest[k] = v
	}
	take := func(key string) (taggedurn.Constraint, bool) {
		v, ok := rest[key]
		delete(rest, key)
		return v, ok
	}
	inTag, hasIn := take("in")
	outTag, hasOut := take("out")
	effectTag, hasEffect := take("effect")
	inSpec, err := processDirectionTag(inTag, hasIn, "in")
	if err != nil {
		return nil, err
	}
	outSpec, err := processDirectionTag(outTag, hasOut, "out")
	if err != nil {
		return nil, err
	}
	effect, err := normalizeEffectValue(effectTag, hasEffect)
	if err != nil {
		return nil, err
	}
	return assemble(inSpec, outSpec, effect, rest)
}

// NewCapUrn creates a cap URN from direction specs and cap-tags, with the default
// effect declared; a cap that cannot be made is a panic. See NewCapUrnWithEffect.
func NewCapUrn(inSpec, outSpec string, tags map[string]taggedurn.Constraint) *CapUrn {
	cap, err := NewCapUrnWithEffect(inSpec, outSpec, string(CapEffectDeclared), tags)
	if err != nil {
		panic(fmt.Sprintf("invalid CapUrn construction: %v", err))
	}
	return cap
}

// NewCapUrnWithEffect creates a cap URN from direction specs, an effect and cap-tags.
// inSpec and outSpec are media URNs, normalized to canonical form; * and media: are
// both the top media media:, as in parsing, and an empty spec is refused. effect is
// declared, none, patch or ?. The cap-tags may not name in, out or effect.
func NewCapUrnWithEffect(inSpec, outSpec, effect string, tags map[string]taggedurn.Constraint) (*CapUrn, error) {
	in, err := directionSpec("in", inSpec)
	if err != nil {
		return nil, err
	}
	out, err := directionSpec("out", outSpec)
	if err != nil {
		return nil, err
	}
	effectValue, err := parseEffect(effect)
	if err != nil {
		return nil, err
	}
	return assemble(in, out, effectValue, tags)
}

// InSpec returns the input spec ID
func (c *CapUrn) InSpec() string {
	return c.inSpec
}

// OutSpec returns the output spec ID
func (c *CapUrn) OutSpec() string {
	return c.outSpec
}

// InMediaUrn parses the input spec as a MediaUrn
func (c *CapUrn) InMediaUrn() (*MediaUrn, error) {
	return NewMediaUrnFromString(c.inSpec)
}

// OutMediaUrn parses the output spec as a MediaUrn
func (c *CapUrn) OutMediaUrn() (*MediaUrn, error) {
	return NewMediaUrnFromString(c.outSpec)
}

func (c *CapUrn) EffectSpec() string {
	return c.effect
}

func (c *CapUrn) Effect() CapEffect {
	switch c.effect {
	case string(CapEffectDeclared):
		return CapEffectDeclared
	case string(CapEffectNone):
		return CapEffectNone
	case string(CapEffectPatch):
		return CapEffectPatch
	case string(CapEffectAny):
		return CapEffectAny
	default:
		panic(fmt.Sprintf("CapUrn invariant: invalid stored effect '%s'", c.effect))
	}
}

func (c *CapUrn) validateAdmissible() error {
	inMedia, err := c.InMediaUrn()
	if err != nil {
		return &CapUrnError{Code: ErrorInvalidMediaUrn, Message: fmt.Sprintf("Stored inSpec failed CapUrn admissibility validation: %v", err)}
	}
	outMedia, err := c.OutMediaUrn()
	if err != nil {
		return &CapUrnError{Code: ErrorInvalidMediaUrn, Message: fmt.Sprintf("Stored outSpec failed CapUrn admissibility validation: %v", err)}
	}
	if inMedia.IsTop() && outMedia.IsTop() && len(c.tags) == 0 && c.Effect() == CapEffectDeclared {
		return &CapUrnError{
			Code:    ErrorIllegalDeclaration,
			Message: "illegal bare top cap; use cap:effect=none for identity, or declare a non-vacuous input/output/effect/tag",
		}
	}

	switch c.Effect() {
	case CapEffectDeclared, CapEffectAny:
		return nil
	case CapEffectNone:
		if !inMedia.ConformsTo(outMedia) {
			return &CapUrnError{
				Code:    ErrorIllegalDeclaration,
				Message: fmt.Sprintf("effect=none requires declared input '%s' to conform to declared output '%s'", inMedia, outMedia),
			}
		}
		return nil
	case CapEffectPatch:
		delta, err := outMedia.DeltaFrom(inMedia)
		if err != nil {
			return &CapUrnError{
				Code:    ErrorIllegalDeclaration,
				Message: fmt.Sprintf("effect=patch requires a computable declared media delta from '%s' to '%s': %v", inMedia, outMedia, err),
			}
		}
		witness, err := inMedia.ApplyDelta(delta)
		if err != nil {
			return &CapUrnError{
				Code:    ErrorIllegalDeclaration,
				Message: fmt.Sprintf("effect=patch failed to apply declared media delta to input '%s': %v", inMedia, err),
			}
		}
		if !witness.ConformsTo(outMedia) {
			return &CapUrnError{
				Code:    ErrorIllegalDeclaration,
				Message: fmt.Sprintf("effect=patch witness '%s' does not conform to declared output '%s'", witness, outMedia),
			}
		}
		return nil
	default:
		return &CapUrnError{Code: ErrorInvalidEffect, Message: fmt.Sprintf("invalid stored effect '%s'", c.effect)}
	}
}

// Kind classifies this cap into one of CapKind's five categories,
// looking at all four structural axes:
//   - in (parsed MediaUrn)
//   - out (parsed MediaUrn)
//   - effect
//   - the rest of the tags (the operation/metadata axis — c.tags
//     does not include in/out, those live in their own fields)
//
// Identity requires every axis to be in its explicit identity form:
// in is the top media URN (media:), out is the top media URN,
// effect is none, and there are no other tags.
//
// Returns an error if either in/out side is not a valid MediaUrn —
// this only happens on internally inconsistent state since
// construction validates both sides.
func (c *CapUrn) Kind() (CapKind, error) {
	inMedia, err := c.InMediaUrn()
	if err != nil {
		return "", fmt.Errorf("invalid in media URN: %w", err)
	}
	outMedia, err := c.OutMediaUrn()
	if err != nil {
		return "", fmt.Errorf("invalid out media URN: %w", err)
	}

	inVoid := inMedia.IsVoid()
	outVoid := outMedia.IsVoid()
	inTop := inMedia.IsTop()
	outTop := outMedia.IsTop()
	noExtraTags := len(c.tags) == 0
	effect := c.Effect()

	if inTop && outTop && noExtraTags && effect == CapEffectNone {
		return CapKindIdentity, nil
	}
	if inVoid && outVoid {
		return CapKindEffect, nil
	}
	if inVoid {
		return CapKindSource, nil
	}
	if outVoid {
		return CapKindSink, nil
	}
	return CapKindTransform, nil
}

// CanonicalOption takes an optional cap URN string, parses and re-serializes it
// to canonical form. Returns (nil, nil) for nil input, (canonical, nil) for valid
// input, or (nil, error) for invalid input.
func CanonicalOption(capUrn *string) (*string, error) {
	if capUrn == nil {
		return nil, nil
	}
	parsed, err := NewCapUrnFromString(*capUrn)
	if err != nil {
		return nil, err
	}
	canonical := parsed.String()
	return &canonical, nil
}

// GetTag returns the constraint this cap writes for the cap-tag key, and
// whether it mentions it. Key is normalized to lowercase. The direction and the
// effect are not cap-tags: they are read with InSpec, OutSpec and Effect.
func (c *CapUrn) GetTag(key string) (taggedurn.Constraint, bool) {
	value, exists := c.tags[strings.ToLower(key)]
	return value, exists
}

// ExactValue is the exact value this cap writes for the cap-tag key (key=value),
// and whether it writes one; false when it does not mention the key or
// constrains it in another form.
func (c *CapUrn) ExactValue(key string) (string, bool) {
	exact, ok := c.tags[strings.ToLower(key)].(taggedurn.ConstraintExact)
	return exact.Value, ok
}

// Tags returns a copy of the cap-tags.
func (c *CapUrn) Tags() map[string]taggedurn.Constraint {
	out := make(map[string]taggedurn.Constraint, len(c.tags))
	for k, v := range c.tags {
		out[k] = v
	}
	return out
}

// HasTag reports whether this cap writes the exact value value for the cap-tag
// key (key=value). Key is normalized to lowercase; value comparison is
// case-sensitive.
func (c *CapUrn) HasTag(key, value string) bool {
	v, ok := c.ExactValue(key)
	return ok && v == value
}

// HasMarkerTag reports whether the cap-tag tagName is present as a marker
// (written as just the key). Example: `cap:constrained;...` has marker tag
// "constrained".
func (c *CapUrn) HasMarkerTag(tagName string) bool {
	_, ok := c.tags[strings.ToLower(tagName)].(taggedurn.ConstraintPresent)
	return ok
}

// WithTag returns a new cap URN with a cap-tag added or replaced. The key must be
// an identifier; the constraint must be one a URN can write; a structural key
// (in, out, effect) is refused — those change with WithInSpec, WithOutSpec and
// WithEffect.
func (c *CapUrn) WithTag(key string, constraint taggedurn.Constraint) (*CapUrn, error) {
	newTags := c.Tags()
	newTags[strings.ToLower(key)] = constraint
	return assemble(c.inSpec, c.outSpec, c.effect, newTags)
}

// WithInSpec returns a new cap URN with a different input spec
func (c *CapUrn) WithInSpec(inSpec string) *CapUrn {
	result, err := NewCapUrnWithEffect(inSpec, c.outSpec, c.effect, c.tags)
	if err != nil {
		panic(fmt.Sprintf("CapUrn::WithInSpec produced an illegal cap declaration: %v", err))
	}
	return result
}

// WithOutSpec returns a new cap URN with a different output spec
func (c *CapUrn) WithOutSpec(outSpec string) *CapUrn {
	result, err := NewCapUrnWithEffect(c.inSpec, outSpec, c.effect, c.tags)
	if err != nil {
		panic(fmt.Sprintf("CapUrn::WithOutSpec produced an illegal cap declaration: %v", err))
	}
	return result
}

func (c *CapUrn) WithEffect(effect CapEffect) *CapUrn {
	result, err := NewCapUrnWithEffect(c.inSpec, c.outSpec, string(effect), c.tags)
	if err != nil {
		panic(fmt.Sprintf("CapUrn::WithEffect produced an illegal cap declaration: %v", err))
	}
	return result
}

// WithoutTag returns a new cap URN with a tag removed
// Key is normalized to lowercase for case-insensitive removal
// Note: Cannot remove structural coordinates.
func (c *CapUrn) WithoutTag(key string) *CapUrn {
	keyLower := strings.ToLower(key)
	if keyLower == "in" || keyLower == "out" || keyLower == "effect" {
		panic(fmt.Sprintf("CapUrn::WithoutTag cannot remove reserved structural key '%s'", keyLower))
	}
	newTags := c.Tags()
	delete(newTags, keyLower)
	result, err := assemble(c.inSpec, c.outSpec, c.effect, newTags)
	if err != nil {
		panic(fmt.Sprintf("CapUrn::WithoutTag produced an illegal cap declaration: %v", err))
	}
	return result
}

// Accepts reports whether cap fits this cap read as a PATTERN over caps — what a
// search asks: cap's input is within this pattern's, its output covers the
// pattern's, its effect is the pattern's (a pattern's ?effect fits any), and its
// cap-tags — complete: a cap has the tags it has — satisfy the pattern's.
//
// A side the pattern leaves open is not asked about. `cap:candle` fits every cap
// tagged candle, whatever it takes and gives: its open output is "not
// established", not the type "anything".
//
// Decided by the proved model (CapDAG.Exec.accepts, which is CapDAG.fits). For a
// question no cap URN can spell — "what gives this, whatever it takes" — ask a
// CapQuery.
func (c *CapUrn) Accepts(cap *CapUrn) bool {
	if cap == nil {
		return true
	}
	return decided(capformal.Accepts(c.formal, cap.formal))
}

// ConformsTo reports whether this cap fits pattern: pattern.Accepts(self).
func (c *CapUrn) ConformsTo(pattern *CapUrn) bool {
	return pattern.Accepts(c)
}

// IsDispatchable reports whether this candidate SERVES request — the predicate
// routing and dispatch act on.
//
// The candidate takes at least what the request sends, gives at least what the
// request needs, has the effect asked for unless the request says ?effect, and
// has the cap-tags asked for — its own tags being complete, so a request for
// `!x` is served by a candidate that does not mention x, and a candidate may
// carry tags the request does not ask about.
//
// An input the request leaves open is not established: the caller has not said
// what it will send, and every candidate passes that side. That is "some input",
// not "any input" — media: on a CANDIDATE's input does mean it takes anything.
//
// This is a guarantee. What only could serve does not; see MayDispatch and
// CapQuery.Grade. Decided by the proved model (CapDAG.Exec.dispatch, which is
// CapDAG.serves). Not symmetric.
func (c *CapUrn) IsDispatchable(request *CapUrn) bool {
	if request == nil {
		return true
	}
	return decided(capformal.Dispatch(c.formal, request.formal))
}

// Select returns the position in candidates of the cap the proved ranking
// chooses to serve request (CapDAG.Exec.select): among the candidates that
// serve it, the one whose ranking key is least, and of equal keys the first.
// The second result is false when none serves it.
func Select(candidates []*CapUrn, request *CapUrn) (int, bool) {
	formals := make([]capformal.WfCap, len(candidates))
	for i, candidate := range candidates {
		formals[i] = candidate.formal
	}
	chosen := decided(capformal.Select(formals, request.formal))
	if !chosen.Valid {
		return 0, false
	}
	// The model returns the cap it chose; it is the first candidate that
	// serves the request with the chosen cap's key, since the model keeps the
	// first of equal keys.
	key := decided(capformal.RankingKey(chosen.Value, request.formal))
	for i, candidate := range candidates {
		if candidate.IsDispatchable(request) && compareRankingKeys(candidate.rankingKey(request), key) == 0 {
			return i, true
		}
	}
	panic(fmt.Sprintf("capdag: the model chose a cap to serve %s that is none of the candidates", request))
}

// rankingKey is this candidate's key in the proved ranking for request
// (CapDAG.Exec.rankingKey): the smaller key ranks first.
func (c *CapUrn) rankingKey(request *CapUrn) lungo.Pair[*big.Int, *big.Int] {
	return decided(capformal.RankingKey(c.formal, request.formal))
}

// compareRankingKeys orders two ranking keys as the model does: by their
// first part, then by their second.
func compareRankingKeys(a, b lungo.Pair[*big.Int, *big.Int]) int {
	if order := a.First.Cmp(b.First); order != 0 {
		return order
	}
	return a.Second.Cmp(b.Second)
}

// MayDispatch reports whether this candidate COULD serve request: not
// guaranteed, not excluded. For exploring what the fabric might do — never for
// routing a call, which must be served.
func (c *CapUrn) MayDispatch(request *CapUrn) bool {
	return decided(capformal.MayDispatch(c.formal, request.formal))
}

// FlowsInto reports whether what this cap gives, next takes: the edge of a route.
func (c *CapUrn) FlowsInto(next *CapUrn) bool {
	return decided(capformal.Flows(c.formal, next.formal))
}

// MayFlowInto reports whether what this cap gives COULD be something next takes:
// an edge a search may explore and a run has to check.
func (c *CapUrn) MayFlowInto(next *CapUrn) bool {
	return decided(capformal.MayFlow(c.formal, next.formal))
}

// IsComparable reports whether the two caps are on one chain: one stands in for
// the other on every side. Both are read as descriptions — nothing is unknown.
func (c *CapUrn) IsComparable(other *CapUrn) bool {
	return decided(capformal.Comparable(c.formal, other.formal))
}

// IsEquivalent reports whether the two are the SAME cap: equivalent on every
// side, the effects agreeing. What resolving a name to its cap asks. Nothing is
// read as unknown: a cap that promises no particular output is not the same cap
// as one that promises pages, though as a pattern it fits it.
func (c *CapUrn) IsEquivalent(other *CapUrn) bool {
	return decided(capformal.Equivalent(c.formal, other.formal))
}

// AcceptsStr checks if this cap (handler) accepts a request given as a string.
func (c *CapUrn) AcceptsStr(requestStr string) bool {
	request, err := NewCapUrnFromString(requestStr)
	if err != nil {
		return false
	}
	return c.Accepts(request)
}

func (c *CapUrn) InferRuntimeOutputMedia(runtimeInput *MediaUrn) (*MediaUrn, error) {
	declaredIn, err := c.InMediaUrn()
	if err != nil {
		return nil, &CapUrnError{Code: ErrorInvalidMediaUrn, Message: fmt.Sprintf("Stored inSpec failed to parse during inference: %v", err)}
	}
	declaredOut, err := c.OutMediaUrn()
	if err != nil {
		return nil, &CapUrnError{Code: ErrorInvalidMediaUrn, Message: fmt.Sprintf("Stored outSpec failed to parse during inference: %v", err)}
	}
	if runtimeInput == nil {
		return nil, &CapUrnError{Code: ErrorInvalidEffectApply, Message: "cannot infer runtime output for nil runtime input"}
	}
	// A runtime media URN is the media of a value that exists, so it is read
	// complete: it SATISFIES the declared type, or does not.
	if !runtimeInput.Satisfies(declaredIn) {
		return nil, &CapUrnError{
			Code:    ErrorInvalidEffectApply,
			Message: fmt.Sprintf("Runtime input '%s' does not conform to declared input '%s'", runtimeInput, declaredIn),
		}
	}

	var runtimeOut *MediaUrn
	switch c.Effect() {
	case CapEffectDeclared:
		runtimeOut = declaredOut
	case CapEffectNone:
		runtimeOut = runtimeInput
	case CapEffectPatch:
		delta, err := declaredOut.DeltaFrom(declaredIn)
		if err != nil {
			return nil, &CapUrnError{Code: ErrorInvalidEffectApply, Message: fmt.Sprintf("Failed to derive media delta from '%s' to '%s': %v", declaredIn, declaredOut, err)}
		}
		runtimeOut, err = runtimeInput.ApplyDelta(delta)
		if err != nil {
			return nil, &CapUrnError{Code: ErrorInvalidEffectApply, Message: fmt.Sprintf("Failed to apply media delta to runtime input '%s': %v", runtimeInput, err)}
		}
	case CapEffectAny:
		return nil, &CapUrnError{Code: ErrorInvalidEffectApply, Message: "Cannot infer runtime output for an unconstrained effect request"}
	}

	if !runtimeOut.Satisfies(declaredOut) {
		return nil, &CapUrnError{
			Code:    ErrorInvalidEffectApply,
			Message: fmt.Sprintf("Inferred runtime output '%s' does not conform to declared output '%s'", runtimeOut, declaredOut),
		}
	}
	return runtimeOut, nil
}

// IsConformantRuntimeOutput reports whether an actually-emitted runtime
// output satisfies this cap's declared effect contract for the given
// runtime input.
//
// This is THE effect-audit predicate: every check of "did the cap emit
// what its effect promised" must go through it — never a hand-rolled
// combination of InferRuntimeOutputMedia with equality or conformance
// checks, so the contract has exactly one definition.
//
// The condition is deliberately asymmetric per effect:
//
//   - effect=none and effect=patch compute an EXACT runtime output type,
//     so the emission must be tag-equivalent to the inference. A more
//     specific emission would still be a lie: the effect promises the
//     type is fully determined by the input.
//   - effect=declared promises only the declared out=, so an emission
//     that is MORE specific than the declaration is legal and desirable;
//     the emission must conform to the declared output. A more generic
//     emission breaks downstream plan refinement and fails.
//
// Errors (rather than returning false) when the inference itself is
// impossible: the runtime input does not conform to the declared input,
// the effect is the unconstrained request form (?effect), or the URN
// state is internally inconsistent — those are upstream contract breaks,
// not emission mismatches, and must surface as such.
func (c *CapUrn) IsConformantRuntimeOutput(runtimeInput, runtimeOutput *MediaUrn) (bool, error) {
	inferred, err := c.InferRuntimeOutputMedia(runtimeInput)
	if err != nil {
		return false, err
	}
	if runtimeOutput == nil {
		return false, &CapUrnError{Code: ErrorInvalidEffectApply, Message: "cannot audit a nil runtime output"}
	}
	switch c.Effect() {
	case CapEffectNone, CapEffectPatch:
		return runtimeOutput.IsEquivalent(inferred), nil
	case CapEffectDeclared:
		return runtimeOutput.Satisfies(inferred), nil
	case CapEffectAny:
		return false, &CapUrnError{Code: ErrorInvalidEffectApply, Message: "Cannot audit an emission against an unconstrained effect request"}
	}
	return false, &CapUrnError{Code: ErrorInvalidEffectApply, Message: fmt.Sprintf("unknown effect '%s'", c.Effect())}
}

// Per-axis weights for cap-URN specificity. Two orders of magnitude
// separate each axis to keep them in distinct digit slots while
// folding into a single comparable integer.
const (
	WeightOut = 10_000
	WeightIn  = 100
)

// Specificity returns the specificity score for cap matching.
// More specific caps have higher scores and are preferred.
//
// The score is a weighted sum of the per-tag truth-table score
// across the three axes (out, in, y), each axis scored as a Tagged
// URN per TaggedUrn.Specificity:
//
//	stored value       score   form
//	------------------ -----   ----------------------
//	"?"                0       no constraint
//	starts with "?="   1       absent or not v
//	"*"                2       must-have-any
//	starts with "!="   3       present and not v
//	exact value        4       exact match
//	"!"                5       must-not-have
//
// Axis weighting:
//
//	spec_C(c) = WeightOut*spec_U(c.out) + WeightIn*spec_U(c.in) + spec_U(c.y)
//
// The lexicographic priority (out, in, y) reflects the routing
// intent: producing different things is the largest semantic
// difference between two caps; consuming different things is next;
// descriptive y-axis metadata is last.
//
// Computed by the proved model (CapDAG.Exec.specificity).
func (c *CapUrn) Specificity() int {
	score := decided(capformal.Specificity(c.formal))
	if !score.IsInt64() {
		panic(fmt.Sprintf("capdag: the specificity of %s does not fit an int: %s", c, score))
	}
	return int(score.Int64())
}

// IsMoreSpecificThan checks if this cap is more specific than another
func (c *CapUrn) IsMoreSpecificThan(other *CapUrn) bool {
	if other == nil {
		return true
	}

	return c.Specificity() > other.Specificity()
}

// effectOrder is where each effect stands in the order of caps: as the
// reference declares them, the unspecified effect last.
func effectOrder(effect CapEffect) int {
	switch effect {
	case CapEffectDeclared:
		return 0
	case CapEffectNone:
		return 1
	case CapEffectPatch:
		return 2
	case CapEffectAny:
		return 3
	default:
		panic(fmt.Sprintf("CapUrn invariant: no effect '%s'", effect))
	}
}

// Compare orders two cap URNs: negative when c stands before other, positive
// when after, zero when they are the same cap. The order is structural — `in=`
// and `out=` as media URNs (MediaUrn.Compare), then the effect, then the
// remaining tags as (key, stored value) pairs in key order — and not the order
// of the canonical text, which would fold the four axes into one string.
func (c *CapUrn) Compare(other *CapUrn) int {
	if c == nil && other == nil {
		return 0
	}
	if c == nil {
		return -1
	}
	if other == nil {
		return 1
	}
	mediaOf := func(spec string) *MediaUrn {
		media, err := NewMediaUrnFromString(spec)
		if err != nil {
			panic(fmt.Sprintf("CapUrn invariant: '%s' parses as a media URN: %v", spec, err))
		}
		return media
	}
	if cmp := mediaOf(c.inSpec).Compare(mediaOf(other.inSpec)); cmp != 0 {
		return cmp
	}
	if cmp := mediaOf(c.outSpec).Compare(mediaOf(other.outSpec)); cmp != 0 {
		return cmp
	}
	if cmp := effectOrder(c.Effect()) - effectOrder(other.Effect()); cmp != 0 {
		return cmp
	}
	capTags := func(cap *CapUrn) *taggedurn.TaggedUrn {
		urn, err := taggedurn.NewTaggedUrnFromTags("cap", cap.tags)
		if err != nil {
			panic(fmt.Sprintf("CapUrn invariant: cap-tags make a tagged URN: %v", err))
		}
		return urn
	}
	return capTags(c).Compare(capTags(other))
}

// Less returns true if this CapUrn is ordered before other (see Compare).
func (c *CapUrn) Less(other *CapUrn) bool {
	if other == nil {
		return false
	}
	return c.Compare(other) < 0
}

// WithWildcardTag returns a new cap with a specific tag set to wildcard
// For structural coordinates, sets the corresponding coordinate to its explicit
// unconstrained value.
func (c *CapUrn) WithWildcardTag(key string) *CapUrn {
	keyLower := strings.ToLower(key)
	switch keyLower {
	case "in":
		return c.WithInSpec("media:")
	case "out":
		return c.WithOutSpec("media:")
	case "effect":
		return c.WithEffect(CapEffectAny)
	default:
		if _, exists := c.tags[keyLower]; exists {
			newTags := c.Tags()
			newTags[keyLower] = taggedurn.ConstraintPresent{}
			result, err := assemble(c.inSpec, c.outSpec, c.effect, newTags)
			if err != nil {
				panic(fmt.Sprintf("CapUrn::WithWildcardTag produced an illegal cap declaration: %v", err))
			}
			return result
		}
		return c
	}
}

// Subset returns a new cap with only specified tags
// Structural coordinates remain intact; y-axis tags are filtered.
func (c *CapUrn) Subset(keys []string) *CapUrn {
	newTags := make(map[string]taggedurn.Constraint)
	for _, key := range keys {
		keyLower := strings.ToLower(key)
		// Skip in/out as they're handled separately
		if keyLower == "in" || keyLower == "out" || keyLower == "effect" {
			continue
		}
		if value, exists := c.tags[keyLower]; exists {
			newTags[keyLower] = value
		}
	}
	result, err := assemble(c.inSpec, c.outSpec, c.effect, newTags)
	if err != nil {
		panic(fmt.Sprintf("CapUrn::Subset produced an illegal cap declaration: %v", err))
	}
	return result
}

// Merge returns a new cap merged with another (other takes precedence for conflicts)
// Direction specs from other override this one's
func (c *CapUrn) Merge(other *CapUrn) *CapUrn {
	newTags := c.Tags()
	for k, v := range other.tags {
		newTags[k] = v
	}
	result, err := assemble(other.inSpec, other.outSpec, other.effect, newTags)
	if err != nil {
		panic(fmt.Sprintf("CapUrn::Merge produced an illegal cap declaration: %v", err))
	}
	return result
}

// ToString returns the canonical string representation of this cap URN.
// Uses TaggedUrn for serialization to ensure consistency across
// implementations.
//
// `in` and `out` segments are emitted only when they refine beyond the
// trivial wildcard `media:`. `effect=declared` is omitted because it is the
// default on admissible caps. `effect=none` is never omitted; identity is the
// explicit `cap:effect=none`, never bare `cap:`.
func (c *CapUrn) ToString() string {
	allTags := make(map[string]taggedurn.Constraint, len(c.tags)+3)
	if c.inSpec != "media:" {
		allTags["in"] = taggedurn.ConstraintExact{Value: c.inSpec}
	}
	if c.outSpec != "media:" {
		allTags["out"] = taggedurn.ConstraintExact{Value: c.outSpec}
	}
	switch CapEffect(c.effect) {
	case CapEffectDeclared:
	case CapEffectAny:
		allTags["effect"] = taggedurn.ConstraintUnconstrained{}
	default:
		allTags["effect"] = taggedurn.ConstraintExact{Value: c.effect}
	}
	for k, v := range c.tags {
		allTags[k] = v
	}

	taggedUrn, err := taggedurn.NewTaggedUrnFromTags("cap", allTags)
	if err != nil {
		panic(fmt.Sprintf("CapUrn invariant: its tags make a tagged URN: %v", err))
	}
	return taggedUrn.ToString()
}

// String implements the Stringer interface
func (c *CapUrn) String() string {
	return c.ToString()
}

// Equals checks if this cap URN is equal to another
func (c *CapUrn) Equals(other *CapUrn) bool {
	if other == nil {
		return false
	}

	// Check direction specs
	if c.inSpec != other.inSpec || c.outSpec != other.outSpec || c.effect != other.effect {
		return false
	}

	if len(c.tags) != len(other.tags) {
		return false
	}

	for key, value := range c.tags {
		otherValue, exists := other.tags[key]
		if !exists || value != otherValue {
			return false
		}
	}

	return true
}

// Hash returns a hash of this cap URN
// Two equivalent cap URNs will have the same hash
func (c *CapUrn) Hash() string {
	// Use canonical string representation for consistent hashing
	canonical := c.ToString()
	h := sha256.Sum256([]byte(canonical))
	return fmt.Sprintf("%x", h)
}

// MarshalJSON implements the json.Marshaler interface
func (c *CapUrn) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.ToString())
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (c *CapUrn) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("failed to unmarshal CapUrn: expected string, got: %s", string(data))
	}

	capUrn, err := NewCapUrnFromString(s)
	if err != nil {
		return err
	}

	c.inSpec = capUrn.inSpec
	c.outSpec = capUrn.outSpec
	c.effect = capUrn.effect
	c.tags = capUrn.tags
	c.formal = capUrn.formal
	return nil
}

// CapMatcher chooses among registered caps for a request, as the proved
// ranking does (CapDAG.Exec.select): only the caps that serve the request
// (IsDispatchable) are considered, the one whose specificity is closest to the
// request's ranks first (an exact match, then the nearest refinement, then the
// nearest fallback), and of caps that rank alike the first registered comes
// first. caps is in registration order.
type CapMatcher struct{}

// FindBestMatch returns the cap the ranking chooses to serve request (Select),
// or nil when no cap serves it.
func (m *CapMatcher) FindBestMatch(caps []*CapUrn, request *CapUrn) *CapUrn {
	index, found := Select(caps, request)
	if !found {
		return nil
	}
	return caps[index]
}

// FindAllMatches returns every cap that serves request, in ranking order: the
// one FindBestMatch chooses first, and caps that rank alike in registration
// order.
func (m *CapMatcher) FindAllMatches(caps []*CapUrn, request *CapUrn) []*CapUrn {
	type ranked struct {
		cap *CapUrn
		key lungo.Pair[*big.Int, *big.Int]
	}
	var serving []ranked
	for _, cap := range caps {
		if cap.IsDispatchable(request) {
			serving = append(serving, ranked{cap: cap, key: cap.rankingKey(request)})
		}
	}
	// A stable sort, so caps of equal key keep their registration order.
	sort.SliceStable(serving, func(i, j int) bool {
		return compareRankingKeys(serving[i].key, serving[j].key) < 0
	})
	matches := make([]*CapUrn, len(serving))
	for i, entry := range serving {
		matches[i] = entry.cap
	}
	return matches
}

// AreCompatible reports whether two cap sets overlap: some cap of one is
// comparable with some cap of the other (IsComparable).
func (m *CapMatcher) AreCompatible(caps1, caps2 []*CapUrn) bool {
	for _, c1 := range caps1 {
		for _, c2 := range caps2 {
			if c1.IsComparable(c2) {
				return true
			}
		}
	}
	return false
}

// CapUrnBuilder provides a fluent builder interface for creating cap URNs
// Direction specs (in/out) are required and must be set before building
type CapUrnBuilder struct {
	inSpec  *string
	outSpec *string
	effect  *string
	tags    map[string]taggedurn.Constraint
}

// NewCapUrnBuilder creates a new builder
func NewCapUrnBuilder() *CapUrnBuilder {
	return &CapUrnBuilder{
		tags: make(map[string]taggedurn.Constraint),
	}
}

// InSpec sets the input spec ID (required)
func (b *CapUrnBuilder) InSpec(spec string) *CapUrnBuilder {
	b.inSpec = &spec
	return b
}

// OutSpec sets the output spec ID (required)
func (b *CapUrnBuilder) OutSpec(spec string) *CapUrnBuilder {
	b.outSpec = &spec
	return b
}

func (b *CapUrnBuilder) Effect(effect CapEffect) *CapUrnBuilder {
	value := string(effect)
	b.effect = &value
	return b
}

// Constraint adds a cap-tag of any form, its key normalized to lowercase. The key
// and constraint are checked when the cap is built; a structural key (in, out,
// effect) is set with InSpec/OutSpec/Effect instead.
func (b *CapUrnBuilder) Constraint(key string, constraint taggedurn.Constraint) *CapUrnBuilder {
	keyLower := strings.ToLower(key)
	if structuralKeys[keyLower] {
		panic(fmt.Sprintf("CapUrnBuilder cannot set reserved structural key '%s'; use InSpec/OutSpec/Effect", keyLower))
	}
	b.tags[keyLower] = constraint
	return b
}

// Tag adds a cap-tag with the exact value value, taken literally.
func (b *CapUrnBuilder) Tag(key, value string) *CapUrnBuilder {
	return b.Constraint(key, taggedurn.ConstraintExact{Value: value})
}

// Marker adds a cap-tag as a marker (written as just the key).
func (b *CapUrnBuilder) Marker(key string) *CapUrnBuilder {
	return b.Constraint(key, taggedurn.ConstraintPresent{})
}

// Build creates the final CapUrn
func (b *CapUrnBuilder) Build() (*CapUrn, error) {
	if b.inSpec == nil {
		return nil, &CapUrnError{
			Code:    ErrorMissingInSpec,
			Message: "Cap URN requires 'in' tag",
		}
	}

	if b.outSpec == nil {
		return nil, &CapUrnError{
			Code:    ErrorMissingOutSpec,
			Message: "Cap URN requires 'out' tag",
		}
	}

	effect := string(CapEffectDeclared)
	if b.effect != nil {
		effect = *b.effect
	}
	return NewCapUrnWithEffect(*b.inSpec, *b.outSpec, effect, b.tags)
}
