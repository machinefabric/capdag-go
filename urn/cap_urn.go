// Package capdag provides the fundamental cap URN system used across
// all MACHFAB cartridges and candidates. It defines the formal structure for cap
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
	"sort"
	"strings"

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
	// tags are additional tags that define this cap (not including in/out/effect)
	tags map[string]string
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
	ErrorNumericKey            = 7
	ErrorUnterminatedQuote     = 8
	ErrorInvalidEscapeSequence = 9
	ErrorMissingInSpec         = 10
	ErrorMissingOutSpec        = 11
	ErrorInvalidMediaUrn       = 12
	ErrorInvalidEffect         = 13
	ErrorInvalidEffectApply    = 14
	ErrorIllegalDeclaration    = 15
)

func normalizeEffectValue(raw *string) (string, error) {
	if raw == nil {
		return string(CapEffectDeclared), nil
	}
	switch *raw {
	case "*", "?":
		return string(CapEffectAny), nil
	case "declared":
		return string(CapEffectDeclared), nil
	case "none":
		return string(CapEffectNone), nil
	case "patch":
		return string(CapEffectPatch), nil
	case "":
		return "", &CapUrnError{Code: ErrorInvalidEffect, Message: "Empty value for 'effect' tag is not allowed"}
	default:
		return "", &CapUrnError{
			Code: ErrorInvalidEffect,
			Message: fmt.Sprintf(
				"Unsupported effect '%s'. Supported values are declared, none, patch, or explicit unconstrained ?effect/effect=*",
				*raw,
			),
		}
	}
}

func validateNonStructuralTags(tags map[string]string) error {
	urn := taggedurn.NewTaggedUrnFromTags("cap", tags)
	if _, err := taggedurn.NewTaggedUrnFromString(urn.ToString()); err != nil {
		return capUrnErrorFromTaggedUrn(err)
	}
	return nil
}

// processDirectionTag processes a direction tag (in or out) with wildcard expansion
//
// - Missing tag → "media:" (wildcard)
// - tag=* → "media:" (wildcard)
// - tag= (empty) → error
// - tag=value → value (validated later)
func processDirectionTag(taggedUrn *taggedurn.TaggedUrn, tagName string) (string, error) {
	value, hasTag := taggedUrn.GetTag(tagName)
	if !hasTag {
		// Tag is missing - default to media: wildcard
		return "media:", nil
	}

	if value == "*" {
		// Replace * with media: wildcard
		return "media:", nil
	}

	if value == "" {
		// Empty value is not allowed (in= or out= with nothing after =)
		if tagName == "in" {
			return "", &CapUrnError{
				Code:    ErrorInvalidMediaUrn,
				Message: "Empty value for 'in' tag is not allowed",
			}
		}
		return "", &CapUrnError{
			Code:    ErrorInvalidMediaUrn,
			Message: "Empty value for 'out' tag is not allowed",
		}
	}

	// Regular value - will be validated as MediaUrn later
	return value, nil
}

// Note: needsQuoting and quoteValue are delegated to TaggedUrn

// capUrnErrorFromTaggedUrn converts TaggedUrn errors to CapUrn errors
func capUrnErrorFromTaggedUrn(err error) *CapUrnError {
	if err == nil {
		return nil
	}
	msg := err.Error()
	msgLower := strings.ToLower(msg)

	var code int
	switch {
	case strings.Contains(msgLower, "invalid character"):
		code = ErrorInvalidCharacter
	case strings.Contains(msgLower, "duplicate"):
		code = ErrorDuplicateKey
	case strings.Contains(msgLower, "unterminated") || strings.Contains(msgLower, "unclosed"):
		code = ErrorUnterminatedQuote
	case strings.Contains(msgLower, "expected") && strings.Contains(msgLower, "after quoted"):
		code = ErrorUnterminatedQuote
	case strings.Contains(msgLower, "numeric"):
		code = ErrorNumericKey
	case strings.Contains(msgLower, "escape"):
		code = ErrorInvalidEscapeSequence
	case strings.Contains(msgLower, "incomplete") || strings.Contains(msgLower, "missing value"):
		code = ErrorInvalidTagFormat
	default:
		code = ErrorInvalidFormat
	}

	return &CapUrnError{Code: code, Message: msg}
}

// NewCapUrnFromString creates a cap URN from a string
// Format: cap:in="media:...";out="media:...";key1=value1;...
// The "cap:" prefix is mandatory
// The 'in' and 'out' tags are REQUIRED (direction is part of cap identity)
// The in/out values must be valid media URNs (starting with "media:") or wildcards (*)
// Trailing semicolons are optional and ignored
// Tags are automatically sorted alphabetically for canonical form
//
// Case handling:
// - Keys: Always normalized to lowercase
// - Unquoted values: Normalized to lowercase
// - Quoted values: Case preserved exactly as specified
func NewCapUrnFromString(s string) (*CapUrn, error) {
	if s == "" {
		return nil, &CapUrnError{
			Code:    ErrorInvalidFormat,
			Message: "cap URN cannot be empty",
		}
	}

	// Check for "cap:" prefix early (case-insensitive)
	if len(s) < 4 || !strings.EqualFold(s[:4], "cap:") {
		return nil, &CapUrnError{
			Code:    ErrorMissingCapPrefix,
			Message: "cap URN must start with 'cap:'",
		}
	}

	// Use TaggedUrn for parsing
	taggedUrn, err := taggedurn.NewTaggedUrnFromString(s)
	if err != nil {
		return nil, capUrnErrorFromTaggedUrn(err)
	}

	// Verify prefix is "cap"
	if taggedUrn.GetPrefix() != "cap" {
		return nil, &CapUrnError{
			Code:    ErrorMissingCapPrefix,
			Message: "cap URN must start with 'cap:'",
		}
	}

	// Process in and out tags with wildcard expansion
	// Missing tag or tag=* → "media:" (the wildcard)
	inSpec, err := processDirectionTag(taggedUrn, "in")
	if err != nil {
		return nil, err
	}

	outSpec, err := processDirectionTag(taggedUrn, "out")
	if err != nil {
		return nil, err
	}
	effectRaw, hasEffect := taggedUrn.GetTag("effect")
	var effectPtr *string
	if hasEffect {
		effectPtr = &effectRaw
	}
	effect, err := normalizeEffectValue(effectPtr)
	if err != nil {
		return nil, err
	}

	// Validate and canonicalize in/out specs as media URNs.
	// Parse through MediaUrn and re-serialize to get canonical tag ordering.
	// After processing, "media:" is the wildcard (not "*").
	if inSpec != "media:" {
		inMediaUrn, err := NewMediaUrnFromString(inSpec)
		if err != nil {
			return nil, &CapUrnError{
				Code:    ErrorInvalidMediaUrn,
				Message: fmt.Sprintf("Invalid media URN for in spec '%s': %v", inSpec, err),
			}
		}
		inSpec = inMediaUrn.String()
	}
	if outSpec != "media:" {
		outMediaUrn, err := NewMediaUrnFromString(outSpec)
		if err != nil {
			return nil, &CapUrnError{
				Code:    ErrorInvalidMediaUrn,
				Message: fmt.Sprintf("Invalid media URN for out spec '%s': %v", outSpec, err),
			}
		}
		outSpec = outMediaUrn.String()
	}

	// Build tags map without in/out
	tags := make(map[string]string)
	for key, value := range taggedUrn.AllTags() {
		if key != "in" && key != "out" && key != "effect" {
			tags[key] = value
		}
	}
	if err := validateNonStructuralTags(tags); err != nil {
		return nil, err
	}
	cap := &CapUrn{inSpec: inSpec, outSpec: outSpec, effect: effect, tags: tags}
	if err := cap.validateAdmissible(); err != nil {
		return nil, err
	}
	return cap, nil
}

// NewCapUrnFromTags creates a cap URN from tags that must contain 'in' and 'out'
// Keys are normalized to lowercase; values are preserved as-is
// Returns error if 'in' or 'out' tags are missing or invalid
func NewCapUrnFromTags(tags map[string]string) (*CapUrn, error) {
	// Normalize keys to lowercase
	result := make(map[string]string)
	for k, v := range tags {
		result[strings.ToLower(k)] = v
	}

	// Extract required in and out specs with wildcard expansion
	inSpec, hasIn := result["in"]
	if !hasIn {
		// Missing tag defaults to wildcard
		inSpec = "media:"
	} else if inSpec == "*" {
		// Wildcard expansion
		inSpec = "media:"
	} else if inSpec == "" {
		return nil, &CapUrnError{
			Code:    ErrorInvalidMediaUrn,
			Message: "Empty value for 'in' tag is not allowed",
		}
	}
	delete(result, "in")

	// Validate and canonicalize in spec
	if inSpec != "media:" {
		inMediaUrn, err := NewMediaUrnFromString(inSpec)
		if err != nil {
			return nil, &CapUrnError{
				Code:    ErrorInvalidMediaUrn,
				Message: fmt.Sprintf("Invalid media URN for in spec '%s': %v", inSpec, err),
			}
		}
		inSpec = inMediaUrn.String()
	}

	outSpec, hasOut := result["out"]
	if !hasOut {
		// Missing tag defaults to wildcard
		outSpec = "media:"
	} else if outSpec == "*" {
		// Wildcard expansion
		outSpec = "media:"
	} else if outSpec == "" {
		return nil, &CapUrnError{
			Code:    ErrorInvalidMediaUrn,
			Message: "Empty value for 'out' tag is not allowed",
		}
	}
	delete(result, "out")
	effectRaw, hasEffect := result["effect"]
	var effectPtr *string
	if hasEffect {
		effectPtr = &effectRaw
	}
	effect, err := normalizeEffectValue(effectPtr)
	if err != nil {
		return nil, err
	}
	delete(result, "effect")

	// Validate and canonicalize out spec
	if outSpec != "media:" {
		outMediaUrn, err := NewMediaUrnFromString(outSpec)
		if err != nil {
			return nil, &CapUrnError{
				Code:    ErrorInvalidMediaUrn,
				Message: fmt.Sprintf("Invalid media URN for out spec '%s': %v", outSpec, err),
			}
		}
		outSpec = outMediaUrn.String()
	}

	if err := validateNonStructuralTags(result); err != nil {
		return nil, err
	}
	cap := &CapUrn{inSpec: inSpec, outSpec: outSpec, effect: effect, tags: result}
	if err := cap.validateAdmissible(); err != nil {
		return nil, err
	}
	return cap, nil
}

// NewCapUrn creates a cap URN from direction specs and additional tags
// Keys are normalized to lowercase; values are preserved as-is
// inSpec and outSpec are required direction specifiers
// Specs are canonicalized through MediaUrn parsing for consistent tag ordering.
func NewCapUrn(inSpec, outSpec string, tags map[string]string) *CapUrn {
	// Canonicalize specs through MediaUrn parsing
	if inSpec != "" && inSpec != "media:" {
		if mu, err := NewMediaUrnFromString(inSpec); err == nil {
			inSpec = mu.String()
		}
	}
	if outSpec != "" && outSpec != "media:" {
		if mu, err := NewMediaUrnFromString(outSpec); err == nil {
			outSpec = mu.String()
		}
	}
	normalizedTags := make(map[string]string)
	for k, v := range tags {
		keyLower := strings.ToLower(k)
		// Ensure in, out, and effect are not in tags
		if keyLower != "in" && keyLower != "out" && keyLower != "effect" {
			normalizedTags[keyLower] = v
		}
	}
	cap, err := NewCapUrnWithEffect(inSpec, outSpec, string(CapEffectDeclared), normalizedTags)
	if err != nil {
		panic(fmt.Sprintf("invalid CapUrn construction: %v", err))
	}
	return cap
}

func NewCapUrnWithEffect(inSpec, outSpec, effect string, tags map[string]string) (*CapUrn, error) {
	if inSpec == "*" || inSpec == "" {
		inSpec = "media:"
	}
	if outSpec == "*" || outSpec == "" {
		outSpec = "media:"
	}
	inMedia, err := NewMediaUrnFromString(inSpec)
	if err != nil {
		return nil, &CapUrnError{Code: ErrorInvalidMediaUrn, Message: fmt.Sprintf("Invalid media URN for in spec '%s': %v", inSpec, err)}
	}
	outMedia, err := NewMediaUrnFromString(outSpec)
	if err != nil {
		return nil, &CapUrnError{Code: ErrorInvalidMediaUrn, Message: fmt.Sprintf("Invalid media URN for out spec '%s': %v", outSpec, err)}
	}
	effectValue, err := normalizeEffectValue(&effect)
	if err != nil {
		return nil, err
	}
	normalizedTags := make(map[string]string)
	for k, v := range tags {
		keyLower := strings.ToLower(k)
		if keyLower == "in" || keyLower == "out" || keyLower == "effect" {
			continue
		}
		normalizedTags[keyLower] = v
	}
	if err := validateNonStructuralTags(normalizedTags); err != nil {
		return nil, err
	}
	cap := &CapUrn{
		inSpec:  inMedia.String(),
		outSpec: outMedia.String(),
		effect:  effectValue,
		tags:    normalizedTags,
	}
	if err := cap.validateAdmissible(); err != nil {
		return nil, err
	}
	return cap, nil
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
			Code: ErrorIllegalDeclaration,
			Message: "illegal bare top cap; use cap:effect=none for identity, or declare a non-vacuous input/output/effect/tag",
		}
	}

	switch c.Effect() {
	case CapEffectDeclared, CapEffectAny:
		return nil
	case CapEffectNone:
		if !inMedia.ConformsTo(outMedia) {
			return &CapUrnError{
				Code: ErrorIllegalDeclaration,
				Message: fmt.Sprintf("effect=none requires declared input '%s' to conform to declared output '%s'", inMedia, outMedia),
			}
		}
		return nil
	case CapEffectPatch:
		delta, err := outMedia.DeltaFrom(inMedia)
		if err != nil {
			return &CapUrnError{
				Code: ErrorIllegalDeclaration,
				Message: fmt.Sprintf("effect=patch requires a computable declared media delta from '%s' to '%s': %v", inMedia, outMedia, err),
			}
		}
		witness, err := inMedia.ApplyDelta(delta)
		if err != nil {
			return &CapUrnError{
				Code: ErrorIllegalDeclaration,
				Message: fmt.Sprintf("effect=patch failed to apply declared media delta to input '%s': %v", inMedia, err),
			}
		}
		if !witness.ConformsTo(outMedia) {
			return &CapUrnError{
				Code: ErrorIllegalDeclaration,
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

// GetTag returns the value of a specific tag
// Key is normalized to lowercase for lookup
// For 'in', 'out', and 'effect', returns the structural coordinate fields
func (c *CapUrn) GetTag(key string) (string, bool) {
	keyLower := strings.ToLower(key)
	switch keyLower {
	case "in":
		return c.inSpec, true
	case "out":
		return c.outSpec, true
	case "effect":
		return c.effect, true
	default:
		value, exists := c.tags[keyLower]
		return value, exists
	}
}

// HasTag checks if this cap has a specific tag with a specific value
// Key is normalized to lowercase; value comparison is case-sensitive
// For structural coordinates, checks the dedicated fields
func (c *CapUrn) HasTag(key, value string) bool {
	keyLower := strings.ToLower(key)
	switch keyLower {
	case "in":
		return c.inSpec == value
	case "out":
		return c.outSpec == value
	case "effect":
		return c.effect == value
	default:
		tagValue, exists := c.tags[keyLower]
		return exists && tagValue == value
	}
}

// HasMarkerTag checks if a marker tag (solo tag with no value) is present.
// A marker tag is stored as key="*" in the cap URN.
// Example: `cap:constrained;...` has marker tag "constrained"
func (c *CapUrn) HasMarkerTag(tagName string) bool {
	val, ok := c.tags[strings.ToLower(tagName)]
	return ok && val == "*"
}

// WithTag returns a new cap URN with an added or updated tag
// Key is normalized to lowercase; value is preserved as-is
// Note: Cannot modify structural coordinates here.
func (c *CapUrn) WithTag(key, value string) *CapUrn {
	keyLower := strings.ToLower(key)
	if keyLower == "in" || keyLower == "out" || keyLower == "effect" {
		panic(fmt.Sprintf("CapUrn::WithTag cannot set reserved structural key '%s'; use WithInSpec/WithOutSpec/WithEffect", keyLower))
	}
	newTags := make(map[string]string)
	for k, v := range c.tags {
		newTags[k] = v
	}
	newTags[keyLower] = value
	result, err := NewCapUrnWithEffect(c.inSpec, c.outSpec, c.effect, newTags)
	if err != nil {
		panic(fmt.Sprintf("CapUrn::WithTag produced an illegal cap declaration: %v", err))
	}
	return result
}

// WithTagValidated adds or updates a tag, rejecting empty values (matches Rust with_tag)
func (c *CapUrn) WithTagValidated(key, value string) (*CapUrn, error) {
	if value == "" {
		return nil, errors.New("tag value cannot be empty")
	}
	keyLower := strings.ToLower(key)
	if keyLower == "in" || keyLower == "out" || keyLower == "effect" {
		return nil, &CapUrnError{
			Code:    ErrorInvalidTagFormat,
			Message: fmt.Sprintf("reserved structural key '%s' must be changed via dedicated CapUrn accessors", keyLower),
		}
	}
	return c.WithTag(key, value), nil
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
	newTags := make(map[string]string)
	for k, v := range c.tags {
		if k != keyLower {
			newTags[k] = v
		}
	}
	result, err := NewCapUrnWithEffect(c.inSpec, c.outSpec, c.effect, newTags)
	if err != nil {
		panic(fmt.Sprintf("CapUrn::WithoutTag produced an illegal cap declaration: %v", err))
	}
	return result
}

// Accepts checks if this cap (pattern/handler) accepts the given request (instance).
//
// Direction specs use semantic TaggedUrn matching via MediaUrn:
// - Input: `cap_in.accepts(request_in)` — does request's data satisfy cap's input requirement?
// - Output: `request_out.accepts(cap_out)` — does cap's output satisfy what request expects?
//
// For other tags: cap satisfies request's tag constraints.
// Missing cap tags are wildcards (cap accepts any value for that tag).
func (c *CapUrn) Accepts(request *CapUrn) bool {
	if request == nil {
		return true
	}

	// Input direction: self.in_spec is pattern, request.in_spec is instance
	// "media:" on the PATTERN side means "I accept any input" — skip check.
	// "media:" on the INSTANCE side is just the least specific — still check.
	if c.inSpec != "media:" {
		capIn, err := NewMediaUrnFromString(c.inSpec)
		if err != nil {
			panic(fmt.Sprintf("CU2: cap in_spec '%s' is not a valid MediaUrn: %v", c.inSpec, err))
		}
		requestIn, err := NewMediaUrnFromString(request.inSpec)
		if err != nil {
			panic(fmt.Sprintf("CU2: request in_spec '%s' is not a valid MediaUrn: %v", request.inSpec, err))
		}
		if !capIn.Accepts(requestIn) {
			return false
		}
	}

	// Output direction: self.out_spec is pattern, request.out_spec is instance
	// "media:" on the PATTERN side means "I accept any output" — skip check.
	// "media:" on the INSTANCE side is just the least specific — still check.
	if c.outSpec != "media:" {
		capOut, err := NewMediaUrnFromString(c.outSpec)
		if err != nil {
			panic(fmt.Sprintf("CU2: cap out_spec '%s' is not a valid MediaUrn: %v", c.outSpec, err))
		}
		requestOut, err := NewMediaUrnFromString(request.outSpec)
		if err != nil {
			panic(fmt.Sprintf("CU2: request out_spec '%s' is not a valid MediaUrn: %v", request.outSpec, err))
		}
		if !capOut.ConformsTo(requestOut) {
			return false
		}
	}

	if c.effect != string(CapEffectAny) && c.effect != request.effect {
		return false
	}

	// Y-axis: every tag's per-key match runs through the six-form
	// truth table (taggedurn.ValuesMatch). Walk the union of all
	// keys appearing on either side so missing-on-pattern and
	// missing-on-instance cells both get evaluated.
	allKeys := make(map[string]struct{}, len(c.tags)+len(request.tags))
	for k := range c.tags {
		allKeys[k] = struct{}{}
	}
	for k := range request.tags {
		allKeys[k] = struct{}{}
	}
	for key := range allKeys {
		var pattPtr, instPtr *string
		if v, ok := c.tags[key]; ok {
			vCopy := v
			pattPtr = &vCopy
		}
		if v, ok := request.tags[key]; ok {
			vCopy := v
			instPtr = &vCopy
		}
		if !taggedurn.ValuesMatch(instPtr, pattPtr) {
			return false
		}
	}
	return true
}

// ConformsTo checks if this cap conforms to another cap's constraints.
// Equivalent to cap.Accepts(self).
func (c *CapUrn) ConformsTo(cap *CapUrn) bool {
	return cap.Accepts(c)
}

// inputDispatchable checks if candidate's input is dispatchable for request's input.
//
// Input is CONTRAVARIANT: candidate with looser input constraint can handle
// request with stricter input. media: is the identity (top) and means
// "unconstrained" — vacuously true on either side.
func (c *CapUrn) inputDispatchable(request *CapUrn) bool {
	// Request wildcard: any candidate input is fine
	if request.inSpec == "media:" {
		return true
	}

	// Candidate wildcard: candidate accepts any input
	if c.inSpec == "media:" {
		return true
	}

	// Both specific: request input must conform to candidate input requirement
	reqIn, err := NewMediaUrnFromString(request.inSpec)
	if err != nil {
		return false
	}
	candIn, err := NewMediaUrnFromString(c.inSpec)
	if err != nil {
		return false
	}

	return reqIn.ConformsTo(candIn)
}

// outputDispatchable checks if candidate's output is dispatchable for request's output.
//
// Output is COVARIANT: candidate must produce at least what request needs.
// Candidate out=media: + request specific: FAIL (cannot guarantee).
// This is asymmetric with input.
func (c *CapUrn) outputDispatchable(request *CapUrn) bool {
	// Request wildcard: any candidate output is fine
	if request.outSpec == "media:" {
		return true
	}

	// Candidate wildcard: cannot guarantee specific output request needs
	if c.outSpec == "media:" {
		return false
	}

	// Both specific: candidate output must conform to request output
	reqOut, err := NewMediaUrnFromString(request.outSpec)
	if err != nil {
		return false
	}
	candOut, err := NewMediaUrnFromString(c.outSpec)
	if err != nil {
		return false
	}

	return candOut.ConformsTo(reqOut)
}

// capTagsDispatchable checks if candidate's cap-tags are dispatchable for request's cap-tags.
//
// Every explicit request tag must be satisfied by candidate.
// Candidate may have extra tags (refinement is OK).
// Wildcard (*) in request means any value acceptable.
// Wildcard (*) in candidate means candidate can handle any value.
func (c *CapUrn) capTagsDispatchable(request *CapUrn) bool {
	allKeys := make(map[string]struct{}, len(c.tags)+len(request.tags))
	for key := range c.tags {
		allKeys[key] = struct{}{}
	}
	for key := range request.tags {
		allKeys[key] = struct{}{}
	}
	for key := range allKeys {
		var pattPtr, instPtr *string
		if v, ok := request.tags[key]; ok {
			vCopy := v
			pattPtr = &vCopy
		}
		if v, ok := c.tags[key]; ok {
			vCopy := v
			instPtr = &vCopy
		}
		if !taggedurn.ValuesMatch(instPtr, pattPtr) {
			return false
		}
	}
	return true
}

func (c *CapUrn) effectDispatchable(request *CapUrn) bool {
	return request.effect == string(CapEffectAny) || c.effect == request.effect
}

// IsDispatchable checks if this candidate can dispatch (handle) the given request.
//
// This is the PRIMARY predicate for routing/dispatch decisions.
//
// A candidate is dispatchable for a request iff:
// 1. Input axis: candidate can handle request's input (contravariant)
// 2. Output axis: candidate meets request's output needs (covariant)
// 3. Cap-tags: candidate satisfies all explicit request tags, may add more
//
// Key insight: This is NOT symmetric.
func (c *CapUrn) IsDispatchable(request *CapUrn) bool {
	if request == nil {
		return true
	}
	if !c.inputDispatchable(request) {
		return false
	}
	if !c.outputDispatchable(request) {
		return false
	}
	if !c.effectDispatchable(request) {
		return false
	}
	if !c.capTagsDispatchable(request) {
		return false
	}
	return true
}

// IsComparable checks if two cap URNs are comparable in the order-theoretic sense.
//
// Two URNs are comparable if either one accepts (subsumes) the other.
// This is the symmetric closure of the Accepts relation.
// Matches Rust's is_comparable which uses accepts, not is_dispatchable.
func (c *CapUrn) IsComparable(other *CapUrn) bool {
	return c.Accepts(other) || other.Accepts(c)
}

// IsEquivalent checks if two cap URNs are equivalent in the order-theoretic sense.
//
// Two URNs are equivalent if each accepts (subsumes) the other.
// This means they have the same position in the specificity lattice.
// Matches Rust's is_equivalent which uses accepts, not is_dispatchable.
func (c *CapUrn) IsEquivalent(other *CapUrn) bool {
	return c.Accepts(other) && other.Accepts(c)
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
	if !runtimeInput.ConformsTo(declaredIn) {
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

	if !runtimeOut.ConformsTo(declaredOut) {
		return nil, &CapUrnError{
			Code:    ErrorInvalidEffectApply,
			Message: fmt.Sprintf("Inferred runtime output '%s' does not conform to declared output '%s'", runtimeOut, declaredOut),
		}
	}
	return runtimeOut, nil
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
func (c *CapUrn) Specificity() int {
	inMedia, err := NewMediaUrnFromString(c.inSpec)
	if err != nil {
		panic(fmt.Sprintf("CU2: in_spec '%s' is not a valid MediaUrn: %v", c.inSpec, err))
	}
	outMedia, err := NewMediaUrnFromString(c.outSpec)
	if err != nil {
		panic(fmt.Sprintf("CU2: out_spec '%s' is not a valid MediaUrn: %v", c.outSpec, err))
	}

	yScore := 0
	for _, value := range c.tags {
		yScore += taggedurn.ScoreTagValue(value)
	}
	return WeightOut*outMedia.Specificity() + WeightIn*inMedia.Specificity() + yScore
}

// scoreTagValue: kept for callers that need the raw scorer; delegates
// to the canonical implementation in tagged_urn.
func scoreTagValue(value string) int {
	return taggedurn.ScoreTagValue(value)
}

// IsMoreSpecificThan checks if this cap is more specific than another
func (c *CapUrn) IsMoreSpecificThan(other *CapUrn) bool {
	if other == nil {
		return true
	}

	return c.Specificity() > other.Specificity()
}

// Less returns true if this CapUrn is ordered before other.
// Comparison is structural over in/out/effect/tags; it does not route through
// flat full-string comparison.
func (c *CapUrn) Less(other *CapUrn) bool {
	if other == nil {
		return false
	}
	selfIn, errA := NewMediaUrnFromString(c.inSpec)
	otherIn, errB := NewMediaUrnFromString(other.inSpec)
	if errA == nil && errB == nil {
		if cmp := selfIn.Compare(otherIn); cmp != 0 {
			return cmp < 0
		}
	}
	selfOut, errC := NewMediaUrnFromString(c.outSpec)
	otherOut, errD := NewMediaUrnFromString(other.outSpec)
	if errC == nil && errD == nil {
		if cmp := selfOut.Compare(otherOut); cmp != 0 {
			return cmp < 0
		}
	}
	if c.effect != other.effect {
		return c.effect < other.effect
	}
	selfTagged := taggedurn.NewTaggedUrnFromTags("cap", c.tags)
	otherTagged := taggedurn.NewTaggedUrnFromTags("cap", other.tags)
	return selfTagged.Compare(otherTagged) < 0
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
			newTags := make(map[string]string)
			for k, v := range c.tags {
				newTags[k] = v
			}
			newTags[keyLower] = "*"
			result, err := NewCapUrnWithEffect(c.inSpec, c.outSpec, c.effect, newTags)
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
	newTags := make(map[string]string)
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
	result, err := NewCapUrnWithEffect(c.inSpec, c.outSpec, c.effect, newTags)
	if err != nil {
		panic(fmt.Sprintf("CapUrn::Subset produced an illegal cap declaration: %v", err))
	}
	return result
}

// Merge returns a new cap merged with another (other takes precedence for conflicts)
// Direction specs from other override this one's
func (c *CapUrn) Merge(other *CapUrn) *CapUrn {
	newTags := make(map[string]string)
	for k, v := range c.tags {
		newTags[k] = v
	}
	for k, v := range other.tags {
		newTags[k] = v
	}
	result, err := NewCapUrnWithEffect(other.inSpec, other.outSpec, other.effect, newTags)
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
	allTags := make(map[string]string, len(c.tags)+3)
	if c.inSpec != "media:" {
		allTags["in"] = c.inSpec
	}
	if c.outSpec != "media:" {
		allTags["out"] = c.outSpec
	}
	if c.effect != string(CapEffectDeclared) {
		allTags["effect"] = c.effect
	}
	for k, v := range c.tags {
		allTags[k] = v
	}

	taggedUrn := taggedurn.NewTaggedUrnFromTags("cap", allTags)
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
	return nil
}

// CapMatcher provides utility methods for matching caps
type CapMatcher struct{}

// FindBestMatch finds the most specific cap that accepts a request
func (m *CapMatcher) FindBestMatch(caps []*CapUrn, request *CapUrn) *CapUrn {
	var best *CapUrn
	bestSpecificity := -1

	for _, cap := range caps {
		// Routing direction: request.accepts(cap) — request is pattern, cap is instance
		if request.Accepts(cap) {
			specificity := cap.Specificity()
			if specificity > bestSpecificity {
				best = cap
				bestSpecificity = specificity
			}
		}
	}

	return best
}

// FindAllMatches finds all caps that match a request, sorted by specificity
func (m *CapMatcher) FindAllMatches(caps []*CapUrn, request *CapUrn) []*CapUrn {
	var matches []*CapUrn

	for _, cap := range caps {
		// Routing direction: request.accepts(cap) — request is pattern, cap is instance
		if request.Accepts(cap) {
			matches = append(matches, cap)
		}
	}

	// Sort by specificity (most specific first)
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Specificity() > matches[j].Specificity()
	})

	return matches
}

// AreCompatible checks if two cap sets are compatible
// Two caps are compatible if either accepts the other (bidirectional accepts)
func (m *CapMatcher) AreCompatible(caps1, caps2 []*CapUrn) bool {
	for _, c1 := range caps1 {
		for _, c2 := range caps2 {
			if c1.Accepts(c2) || c2.Accepts(c1) {
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
	tags    map[string]string
}

// NewCapUrnBuilder creates a new builder
func NewCapUrnBuilder() *CapUrnBuilder {
	return &CapUrnBuilder{
		tags: make(map[string]string),
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

// Tag adds or updates a tag
// Key is normalized to lowercase; value is preserved as-is
// Note: structural coordinates are not set here.
func (b *CapUrnBuilder) Tag(key, value string) *CapUrnBuilder {
	keyLower := strings.ToLower(key)
	if keyLower == "in" || keyLower == "out" || keyLower == "effect" {
		panic(fmt.Sprintf("CapUrnBuilder::Tag cannot set reserved structural key '%s'; use InSpec/OutSpec/Effect", keyLower))
	}
	b.tags[keyLower] = value
	return b
}

// Marker adds a marker tag (a wildcard-valued tag that serializes as just the key).
// Equivalent to Tag(key, "*") but expresses authorial intent: this tag is
// present as a marker, not a key=value pair.
// Structural coordinates cannot be set as marker keys.
func (b *CapUrnBuilder) Marker(key string) *CapUrnBuilder {
	keyLower := strings.ToLower(key)
	if keyLower == "in" || keyLower == "out" || keyLower == "effect" {
		panic(fmt.Sprintf("CapUrnBuilder::Marker cannot set reserved structural key '%s'; use InSpec/OutSpec/Effect", keyLower))
	}
	b.tags[keyLower] = "*"
	return b
}

// Build creates the final CapUrn
func (b *CapUrnBuilder) Build() (*CapUrn, error) {
	if b.inSpec == nil {
		return nil, &CapUrnError{
			Code:    ErrorMissingInSpec,
			Message: "cap URN is missing required 'in' spec - caps must declare their input type (use media:void for no input)",
		}
	}

	if b.outSpec == nil {
		return nil, &CapUrnError{
			Code:    ErrorMissingOutSpec,
			Message: "cap URN is missing required 'out' spec - caps must declare their output type",
		}
	}

	effect := string(CapEffectDeclared)
	if b.effect != nil {
		effect = *b.effect
	}
	return NewCapUrnWithEffect(*b.inSpec, *b.outSpec, effect, b.tags)
}
