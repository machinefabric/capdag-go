// Package capdag provides the fundamental cap URN system used across
// all MACINA plugins and providers. It defines the formal structure for cap
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

// CapUrn represents a cap URN using flat, ordered tags with required direction specifiers
//
// Direction (in→out) is integral to a cap's identity. The `inSpec` and `outSpec`
// fields specify the input and output media URNs respectively.
//
// Examples:
// - cap:in="media:binary";op=generate;out="media:binary";target=thumbnail
// - cap:in="media:void";op=dimensions;out="media:integer"
// - cap:in="media:string";key="Value With Spaces";out="media:object"
type CapUrn struct {
	// inSpec is the input media URN - required (use media:void for caps with no input)
	inSpec string
	// outSpec is the output media URN - required
	outSpec string
	// tags are additional tags that define this cap (not including in/out)
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
)

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
		if key != "in" && key != "out" {
			tags[key] = value
		}
	}

	return &CapUrn{inSpec: inSpec, outSpec: outSpec, tags: tags}, nil
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

	return &CapUrn{inSpec: inSpec, outSpec: outSpec, tags: result}, nil
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
		// Ensure in and out are not in tags
		if keyLower != "in" && keyLower != "out" {
			normalizedTags[keyLower] = v
		}
	}
	return &CapUrn{inSpec: inSpec, outSpec: outSpec, tags: normalizedTags}
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
// For 'in' and 'out', returns the direction spec fields
func (c *CapUrn) GetTag(key string) (string, bool) {
	keyLower := strings.ToLower(key)
	switch keyLower {
	case "in":
		return c.inSpec, true
	case "out":
		return c.outSpec, true
	default:
		value, exists := c.tags[keyLower]
		return value, exists
	}
}

// HasTag checks if this cap has a specific tag with a specific value
// Key is normalized to lowercase; value comparison is case-sensitive
// For 'in' and 'out', checks the direction spec fields
func (c *CapUrn) HasTag(key, value string) bool {
	keyLower := strings.ToLower(key)
	switch keyLower {
	case "in":
		return c.inSpec == value
	case "out":
		return c.outSpec == value
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
// Note: Cannot modify 'in' or 'out' tags - use WithInSpec/WithOutSpec
func (c *CapUrn) WithTag(key, value string) *CapUrn {
	keyLower := strings.ToLower(key)
	// Silently ignore attempts to set in/out via WithTag
	// Use WithInSpec/WithOutSpec instead
	if keyLower == "in" || keyLower == "out" {
		return c
	}
	newTags := make(map[string]string)
	for k, v := range c.tags {
		newTags[k] = v
	}
	newTags[keyLower] = value
	return &CapUrn{inSpec: c.inSpec, outSpec: c.outSpec, tags: newTags}
}

// WithTagValidated adds or updates a tag, rejecting empty values (matches Rust with_tag)
func (c *CapUrn) WithTagValidated(key, value string) (*CapUrn, error) {
	if value == "" {
		return nil, errors.New("tag value cannot be empty")
	}
	return c.WithTag(key, value), nil
}

// WithInSpec returns a new cap URN with a different input spec
func (c *CapUrn) WithInSpec(inSpec string) *CapUrn {
	newTags := make(map[string]string)
	for k, v := range c.tags {
		newTags[k] = v
	}
	return &CapUrn{inSpec: inSpec, outSpec: c.outSpec, tags: newTags}
}

// WithOutSpec returns a new cap URN with a different output spec
func (c *CapUrn) WithOutSpec(outSpec string) *CapUrn {
	newTags := make(map[string]string)
	for k, v := range c.tags {
		newTags[k] = v
	}
	return &CapUrn{inSpec: c.inSpec, outSpec: outSpec, tags: newTags}
}

// WithoutTag returns a new cap URN with a tag removed
// Key is normalized to lowercase for case-insensitive removal
// Note: Cannot remove 'in' or 'out' tags - they are required
func (c *CapUrn) WithoutTag(key string) *CapUrn {
	keyLower := strings.ToLower(key)
	// Silently ignore attempts to remove in/out
	if keyLower == "in" || keyLower == "out" {
		return c
	}
	newTags := make(map[string]string)
	for k, v := range c.tags {
		if k != keyLower {
			newTags[k] = v
		}
	}
	return &CapUrn{inSpec: c.inSpec, outSpec: c.outSpec, tags: newTags}
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

	// Check all tags that the pattern (self) requires.
	// The instance (request param) must satisfy every pattern constraint.
	// Missing tag in instance → instance doesn't satisfy constraint → reject.
	for selfKey, selfValue := range c.tags {
		reqValue, reqExists := request.tags[selfKey]
		if !reqExists {
			// Instance missing a tag the pattern requires
			return false
		}
		// Wildcard matching
		if selfValue == "*" {
			continue
		}
		if reqValue == "*" {
			continue
		}
		// Exact match required
		if selfValue != reqValue {
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

// inputDispatchable checks if provider's input is dispatchable for request's input.
//
// Input is CONTRAVARIANT: provider with looser input constraint can handle
// request with stricter input. media: is the identity (top) and means
// "unconstrained" — vacuously true on either side.
func (c *CapUrn) inputDispatchable(request *CapUrn) bool {
	// Request wildcard: any provider input is fine
	if request.inSpec == "media:" {
		return true
	}

	// Provider wildcard: provider accepts any input
	if c.inSpec == "media:" {
		return true
	}

	// Both specific: request input must conform to provider input requirement
	reqIn, err := NewMediaUrnFromString(request.inSpec)
	if err != nil {
		return false
	}
	provIn, err := NewMediaUrnFromString(c.inSpec)
	if err != nil {
		return false
	}

	return reqIn.ConformsTo(provIn)
}

// outputDispatchable checks if provider's output is dispatchable for request's output.
//
// Output is COVARIANT: provider must produce at least what request needs.
// Provider out=media: + request specific: FAIL (cannot guarantee).
// This is asymmetric with input.
func (c *CapUrn) outputDispatchable(request *CapUrn) bool {
	// Request wildcard: any provider output is fine
	if request.outSpec == "media:" {
		return true
	}

	// Provider wildcard: cannot guarantee specific output request needs
	if c.outSpec == "media:" {
		return false
	}

	// Both specific: provider output must conform to request output
	reqOut, err := NewMediaUrnFromString(request.outSpec)
	if err != nil {
		return false
	}
	provOut, err := NewMediaUrnFromString(c.outSpec)
	if err != nil {
		return false
	}

	return provOut.ConformsTo(reqOut)
}

// capTagsDispatchable checks if provider's cap-tags are dispatchable for request's cap-tags.
//
// Every explicit request tag must be satisfied by provider.
// Provider may have extra tags (refinement is OK).
// Wildcard (*) in request means any value acceptable.
// Wildcard (*) in provider means provider can handle any value.
func (c *CapUrn) capTagsDispatchable(request *CapUrn) bool {
	for key, requestValue := range request.tags {
		providerValue, exists := c.tags[key]
		if !exists {
			// Provider missing a tag that request specifies.
			// Even wildcard (*) means "any value is fine" — the tag
			// must still be present.
			return false
		}
		if requestValue == "*" {
			continue
		}
		if providerValue == "*" {
			continue
		}
		if requestValue != providerValue {
			return false
		}
	}
	return true
}

// IsDispatchable checks if this provider can dispatch (handle) the given request.
//
// This is the PRIMARY predicate for routing/dispatch decisions.
//
// A provider is dispatchable for a request iff:
// 1. Input axis: provider can handle request's input (contravariant)
// 2. Output axis: provider meets request's output needs (covariant)
// 3. Cap-tags: provider satisfies all explicit request tags, may add more
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
	if !c.capTagsDispatchable(request) {
		return false
	}
	return true
}

// IsComparable checks if two cap URNs are comparable in the order-theoretic sense.
//
// Two URNs are comparable if either one can dispatch the other.
// This is the symmetric closure of the IsDispatchable relation.
func (c *CapUrn) IsComparable(other *CapUrn) bool {
	return c.IsDispatchable(other) || other.IsDispatchable(c)
}

// IsEquivalent checks if two cap URNs are equivalent in the order-theoretic sense.
//
// Two URNs are equivalent if each can dispatch the other.
// This means they have the same position in the specificity lattice.
func (c *CapUrn) IsEquivalent(other *CapUrn) bool {
	return c.IsDispatchable(other) && other.IsDispatchable(c)
}

// AcceptsStr checks if this cap (handler) accepts a request given as a string.
func (c *CapUrn) AcceptsStr(requestStr string) bool {
	request, err := NewCapUrnFromString(requestStr)
	if err != nil {
		return false
	}
	return c.Accepts(request)
}

// Specificity returns the specificity score for cap matching.
// More specific caps have higher scores and are preferred.
//
// Direction specs contribute their raw media URN tag count (more tags = more specific).
// This matches Rust's in_media.inner().tags.len() — NOT the TaggedUrn weighted score.
// Other tags contribute 1 per non-wildcard value.
func (c *CapUrn) Specificity() int {
	count := 0
	// "media:" is the wildcard (contributes 0 to specificity)
	if c.inSpec != "media:" {
		inMedia, err := NewMediaUrnFromString(c.inSpec)
		if err != nil {
			panic(fmt.Sprintf("CU2: in_spec '%s' is not a valid MediaUrn: %v", c.inSpec, err))
		}
		count += inMedia.TagCount()
	}
	if c.outSpec != "media:" {
		outMedia, err := NewMediaUrnFromString(c.outSpec)
		if err != nil {
			panic(fmt.Sprintf("CU2: out_spec '%s' is not a valid MediaUrn: %v", c.outSpec, err))
		}
		count += outMedia.TagCount()
	}
	// Count non-wildcard tags
	for _, value := range c.tags {
		if value != "*" {
			count++
		}
	}
	return count
}

// IsMoreSpecificThan checks if this cap is more specific than another
func (c *CapUrn) IsMoreSpecificThan(other *CapUrn) bool {
	if other == nil {
		return true
	}

	return c.Specificity() > other.Specificity()
}

// WithWildcardTag returns a new cap with a specific tag set to wildcard
// For 'in' or 'out', sets the corresponding direction spec to wildcard
func (c *CapUrn) WithWildcardTag(key string) *CapUrn {
	keyLower := strings.ToLower(key)
	switch keyLower {
	case "in":
		return c.WithInSpec("*")
	case "out":
		return c.WithOutSpec("*")
	default:
		if _, exists := c.tags[keyLower]; exists {
			newTags := make(map[string]string)
			for k, v := range c.tags {
				newTags[k] = v
			}
			newTags[keyLower] = "*"
			return &CapUrn{inSpec: c.inSpec, outSpec: c.outSpec, tags: newTags}
		}
		return c
	}
}

// Subset returns a new cap with only specified tags
// Note: 'in' and 'out' are always included as they are required
func (c *CapUrn) Subset(keys []string) *CapUrn {
	newTags := make(map[string]string)
	for _, key := range keys {
		keyLower := strings.ToLower(key)
		// Skip in/out as they're handled separately
		if keyLower == "in" || keyLower == "out" {
			continue
		}
		if value, exists := c.tags[keyLower]; exists {
			newTags[keyLower] = value
		}
	}
	return &CapUrn{inSpec: c.inSpec, outSpec: c.outSpec, tags: newTags}
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
	return &CapUrn{inSpec: other.inSpec, outSpec: other.outSpec, tags: newTags}
}

// ToString returns the canonical string representation of this cap URN
// Uses TaggedUrn for serialization to ensure consistency across implementations
func (c *CapUrn) ToString() string {
	// Build complete tags map including in and out
	allTags := make(map[string]string, len(c.tags)+2)
	allTags["in"] = c.inSpec
	allTags["out"] = c.outSpec
	for k, v := range c.tags {
		allTags[k] = v
	}

	// Use TaggedUrn for serialization
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
	if c.inSpec != other.inSpec || c.outSpec != other.outSpec {
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

// Tag adds or updates a tag
// Key is normalized to lowercase; value is preserved as-is
// Note: 'in' and 'out' are ignored here - use InSpec() and OutSpec()
func (b *CapUrnBuilder) Tag(key, value string) *CapUrnBuilder {
	keyLower := strings.ToLower(key)
	if keyLower == "in" || keyLower == "out" {
		return b
	}
	b.tags[keyLower] = value
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

	return &CapUrn{inSpec: *b.inSpec, outSpec: *b.outSpec, tags: b.tags}, nil
}
