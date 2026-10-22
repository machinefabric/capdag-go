package cap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// ArgSource specifies how an argument can be provided
type ArgSource struct {
	Stdin    *string `json:"stdin,omitempty"`
	Position *int    `json:"position,omitempty"`
	CliFlag  *string `json:"cli_flag,omitempty"`
}

// GetType returns the type of this source
func (s *ArgSource) GetType() string {
	if s.Stdin != nil {
		return "stdin"
	}
	if s.Position != nil {
		return "position"
	}
	if s.CliFlag != nil {
		return "cli_flag"
	}
	return ""
}

// IsStdin returns true if this is a stdin source
func (s *ArgSource) IsStdin() bool {
	return s.Stdin != nil
}

// IsPosition returns true if this is a position source
func (s *ArgSource) IsPosition() bool {
	return s.Position != nil
}

// IsCliFlag returns true if this is a cli_flag source
func (s *ArgSource) IsCliFlag() bool {
	return s.CliFlag != nil
}

// StdinMediaUrn returns the stdin media URN if this is a stdin source
// Matches Rust: pub fn stdin_media_urn(&self) -> Option<&str>
func (s *ArgSource) StdinMediaUrn() *string {
	return s.Stdin
}

// GetPosition returns the position if this is a position source
// Matches Rust: pub fn position(&self) -> Option<usize>
// Named GetPosition to avoid conflict with Position field
func (s *ArgSource) GetPosition() *int {
	return s.Position
}

// GetCliFlag returns the CLI flag if this is a cli_flag source
// Matches Rust: pub fn cli_flag(&self) -> Option<&str>
// Named GetCliFlag to avoid conflict with CliFlag field
func (s *ArgSource) GetCliFlag() *string {
	return s.CliFlag
}

// CapArg represents an argument definition with sources
type CapArg struct {
	MediaUrn string `json:"media_urn"`
	// Name is the label a machine selects this argument by where types cannot
	// decide. Required, and unique among the cap's arguments (RULE17). It is a
	// label local to the definition and identifies nothing: the argument is
	// identified by MediaUrn, and nothing may be derived about a URN from a
	// name or about a name from a URN (RULE12).
	Name string `json:"name"`
	// Accepts is the media URN this argument's stream must conform to, when
	// that is not its own MediaUrn. Never beside a stdin source, which says
	// the same for the main input (RULE18). It is what lets two arguments
	// take values of one type while keeping distinct URNs.
	Accepts  *string `json:"accepts,omitempty"`
	Required bool    `json:"required"`
	// Emitted even when false, as the reference does: the manifest is compared
	// across implementations and `omitempty` made Go's bytes differ from Rust's
	// for the identical cap.
	IsSequence bool `json:"is_sequence"`
	// Streaming declares that this argument is consumed WITHOUT a length
	// promise: the cap processes items as they arrive and may be wired live
	// to an unbounded producer. Orthogonal to IsSequence (cardinality), not
	// part of the URN. Only the main input may stream (RULE14). Emitted even
	// when false (manifest byte parity with the reference).
	Streaming      bool        `json:"streaming"`
	Sources        []ArgSource `json:"sources"`
	ArgDescription *string     `json:"arg_description,omitempty"`
	DefaultValue   any         `json:"default_value,omitempty"`
	Metadata       any         `json:"metadata,omitempty"`
}

// StringPtr preserves the distinction between an omitted optional string and an
// explicitly present empty string in wire models.
func StringPtr(value string) *string {
	return &value
}

func decodeArbitraryJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

type capArgJSON struct {
	MediaUrn       string           `json:"media_urn"`
	Name           *string          `json:"name"`
	Accepts        *string          `json:"accepts,omitempty"`
	Required       bool             `json:"required"`
	IsSequence     bool             `json:"is_sequence"`
	Streaming      bool             `json:"streaming"`
	Sources        []ArgSource      `json:"sources"`
	ArgDescription *string          `json:"arg_description,omitempty"`
	DefaultValue   *json.RawMessage `json:"default_value,omitempty"`
	Metadata       *json.RawMessage `json:"metadata,omitempty"`
}

// UnmarshalJSON parses an argument definition. Unknown fields are REFUSED:
// a key this type does not know is a fabric NEWER than this capdag — a
// contract this build cannot honour. Dropping it would let a cartridge
// advertise a contract the fabric never made (this is how a `streaming`
// argument once went out as bounded). (matches Rust `deny_unknown_fields`)
func (a *CapArg) UnmarshalJSON(data []byte) error {
	var raw capArgJSON
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return err
	}

	// A definition without a name is not a definition this capdag reads
	// (matches Rust: `name` has no default).
	if raw.Name == nil {
		return fmt.Errorf("missing field `name`")
	}

	a.MediaUrn = raw.MediaUrn
	a.Name = *raw.Name
	a.Accepts = raw.Accepts
	a.Required = raw.Required
	a.IsSequence = raw.IsSequence
	a.Streaming = raw.Streaming
	a.Sources = raw.Sources
	a.ArgDescription = raw.ArgDescription
	a.DefaultValue = nil
	a.Metadata = nil

	if raw.DefaultValue != nil {
		value, err := decodeArbitraryJSON(*raw.DefaultValue)
		if err != nil {
			return err
		}
		a.DefaultValue = value
	}

	if raw.Metadata != nil {
		value, err := decodeArbitraryJSON(*raw.Metadata)
		if err != nil {
			return err
		}
		a.Metadata = value
	}

	return nil
}

// NewCapArg creates a new cap argument. `name` is the label a machine selects
// it by; it must be unique among the cap's arguments.
// Matches Rust: CapArg::new(media_urn, name, required, sources)
func NewCapArg(mediaUrn string, name string, required bool, sources []ArgSource) CapArg {
	return CapArg{
		MediaUrn: mediaUrn,
		Name:     name,
		Required: required,
		Sources:  sources,
	}
}

// NewCapArgWithDescription creates a new cap argument with description
func NewCapArgWithDescription(mediaUrn string, name string, required bool, sources []ArgSource, description string) CapArg {
	desc := description
	return CapArg{
		MediaUrn:       mediaUrn,
		Name:           name,
		Required:       required,
		Sources:        sources,
		ArgDescription: &desc,
	}
}

// NewCapArgWithFullDefinition creates a new cap argument with all fields set
func NewCapArgWithFullDefinition(
	mediaUrn string,
	name string,
	required bool,
	sources []ArgSource,
	argDescription string,
	defaultValue any,
	metadata any,
) CapArg {
	desc := argDescription
	return CapArg{
		MediaUrn:       mediaUrn,
		Name:           name,
		Required:       required,
		Sources:        sources,
		ArgDescription: &desc,
		DefaultValue:   defaultValue,
		Metadata:       metadata,
	}
}

// Accepting returns this argument declaring `accepts` as the type its stream
// must conform to.
// Matches Rust: pub fn accepting(self, accepts) -> Self
func (a CapArg) Accepting(accepts string) CapArg {
	a.Accepts = &accepts
	return a
}

// GetName returns the argument's name.
func (a *CapArg) GetName() string {
	return a.Name
}

// AcceptedUrn returns the media URN a value bound to this argument must
// conform to: its stdin source's URN when it declares one (the main input),
// else what it says it accepts, else its own media URN. This is what matching
// reads. It is not what the stream is delivered under — that is StreamUrn,
// which for an argument with `accepts` is its own URN, so two arguments
// accepting one type stay distinct on the wire.
// Matches Rust: pub fn accepted_urn(&self) -> &str
func (a *CapArg) AcceptedUrn() string {
	for _, s := range a.Sources {
		if s.Stdin != nil {
			return *s.Stdin
		}
	}
	if a.Accepts != nil {
		return *a.Accepts
	}
	return a.MediaUrn
}

// GetMetadata gets the metadata for CapArg
func (a *CapArg) GetMetadata() any {
	return a.Metadata
}

// SetMetadata sets the metadata for CapArg
func (a *CapArg) SetMetadata(metadata any) {
	a.Metadata = metadata
}

// ClearMetadata clears the metadata for CapArg
func (a *CapArg) ClearMetadata() {
	a.Metadata = nil
}

// StreamUrn returns the media URN the runtime demuxes this arg's input stream by:
// its Stdin source URN if it declares one, otherwise its declared slot media URN.
// A cap need not declare any Stdin source at all — a producer-fed arg may be
// delivered by its declared URN — so this never assumes a stdin source exists.
// Matches Rust: pub fn stream_urn(&self) -> &str
func (a *CapArg) StreamUrn() string {
	for _, s := range a.Sources {
		if s.Stdin != nil {
			return *s.Stdin
		}
	}
	return a.MediaUrn
}

// IsMainInput reports whether this arg is the cap's MAIN input relative to inSpec
// (the cap URN's in= value): it declares a Stdin source whose URN is in=. The main
// input is always the value piped in on stdin (like a Unix command's stdin), so the
// main arg always declares a Stdin source carrying in=. Its DECLARED slot URN may
// differ from that stdin URN (e.g. a file-path slot whose piped content is a
// pdf-stream) — the stdin URN, not the slot URN, is in=. The main input may ALSO be
// delivered by position/cli-flag, but stdin is the defining route. Compared by
// tagged-URN equivalence, never as strings.
// Matches Rust: pub fn is_main_input(&self, in_spec: &MediaUrn) -> bool
func (a *CapArg) IsMainInput(inSpec *urn.MediaUrn) bool {
	if inSpec == nil {
		return false
	}
	for _, s := range a.Sources {
		if s.Stdin == nil {
			continue
		}
		stdinUrn, err := urn.NewMediaUrnFromString(*s.Stdin)
		if err != nil {
			continue
		}
		if stdinUrn.IsEquivalent(inSpec) {
			return true
		}
	}
	return false
}

// HasStdinSource returns true if this argument has a stdin source
func (a *CapArg) HasStdinSource() bool {
	for _, s := range a.Sources {
		if s.IsStdin() {
			return true
		}
	}
	return false
}

// GetStdinMediaUrn returns the stdin media URN if present
func (a *CapArg) GetStdinMediaUrn() *string {
	for _, s := range a.Sources {
		if s.Stdin != nil {
			return s.Stdin
		}
	}
	return nil
}

// HasPositionSource returns true if this argument has a position source
func (a *CapArg) HasPositionSource() bool {
	for _, s := range a.Sources {
		if s.IsPosition() {
			return true
		}
	}
	return false
}

// GetPosition returns the position if present
func (a *CapArg) GetPosition() *int {
	for _, s := range a.Sources {
		if s.Position != nil {
			return s.Position
		}
	}
	return nil
}

// HasCliFlagSource returns true if this argument has a cli_flag source
func (a *CapArg) HasCliFlagSource() bool {
	for _, s := range a.Sources {
		if s.IsCliFlag() {
			return true
		}
	}
	return false
}

// GetCliFlag returns the cli_flag if present
func (a *CapArg) GetCliFlag() *string {
	for _, s := range a.Sources {
		if s.CliFlag != nil {
			return s.CliFlag
		}
	}
	return nil
}

// Resolve resolves the argument's media URN through the FabricRegistry.
func (a *CapArg) Resolve(registry media.MediaDefSource) (*media.ResolvedMediaDef, error) {
	return media.ResolveMediaUrn(a.MediaUrn, registry)
}

// IsStructured checks if this argument expects structured data (map or list).
func (a *CapArg) IsStructured(registry media.MediaDefSource) (bool, error) {
	resolved, err := a.Resolve(registry)
	if err != nil {
		return false, fmt.Errorf("failed to resolve argument media_urn '%s': %w", a.MediaUrn, err)
	}
	return resolved.IsStructured(), nil
}

// GetMediaType returns the resolved media type for this argument.
func (a *CapArg) GetMediaType(registry media.MediaDefSource) (string, error) {
	resolved, err := a.Resolve(registry)
	if err != nil {
		return "", fmt.Errorf("failed to resolve argument media_urn '%s': %w", a.MediaUrn, err)
	}
	return resolved.MediaType, nil
}

// CapOutput represents the output definition for a cap
// OutputCarries says what an output carries when that is not its own media URN.
type OutputCarries string

// OutputCarriesInput: the output carries the cap's main input, type unchanged.
const OutputCarriesInput OutputCarries = "input"

type CapOutput struct {
	MediaUrn string `json:"media_urn"`
	// Name is the label a machine takes this output by. Required, and unique
	// among the cap's outputs (RULE19). A label only: the output is
	// identified by MediaUrn.
	Name              string `json:"name"`
	OutputDescription string `json:"output_description"`
	// Optional: the cap may end without producing this output. What needs
	// it is then skipped. Emitted even when false — see CapArg.IsSequence.
	Optional bool `json:"optional"`
	// Carries is what the output carries when that is not its own MediaUrn.
	// Only an output other than the main one, of a cap that takes an input,
	// may declare it (RULE22).
	Carries *OutputCarries `json:"carries,omitempty"`
	// Emitted even when false — see CapArg.IsSequence.
	IsSequence bool `json:"is_sequence"`
	// Streaming declares that this output MAY be emitted without a length
	// promise (unbounded). The executor feeds a non-streaming consumer the
	// bounded whole after this cap ends; the runtime refuses an unbounded
	// emission from an output that did not declare it.
	Streaming bool `json:"streaming"`
	Metadata  any  `json:"metadata,omitempty"`
}

type capOutputJSON struct {
	MediaUrn          string           `json:"media_urn"`
	Name              *string          `json:"name"`
	OutputDescription string           `json:"output_description"`
	Optional          bool             `json:"optional"`
	Carries           *string          `json:"carries,omitempty"`
	IsSequence        bool             `json:"is_sequence"`
	Streaming         bool             `json:"streaming"`
	Metadata          *json.RawMessage `json:"metadata,omitempty"`
}

// UnmarshalJSON parses an output definition, refusing unknown fields for the
// same reason as CapArg.UnmarshalJSON: an unknown key is a newer fabric.
func (co *CapOutput) UnmarshalJSON(data []byte) error {
	var raw capOutputJSON
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	if raw.Name == nil {
		return fmt.Errorf("missing field `name`")
	}
	co.MediaUrn = raw.MediaUrn
	co.Name = *raw.Name
	co.OutputDescription = raw.OutputDescription
	co.Optional = raw.Optional
	co.Carries = nil
	if raw.Carries != nil {
		carries := OutputCarries(*raw.Carries)
		if carries != OutputCarriesInput {
			return fmt.Errorf("unknown variant `%s`, expected `input`", *raw.Carries)
		}
		co.Carries = &carries
	}
	co.IsSequence = raw.IsSequence
	co.Streaming = raw.Streaming
	co.Metadata = nil
	if raw.Metadata != nil {
		value, err := decodeArbitraryJSON(*raw.Metadata)
		if err != nil {
			return err
		}
		co.Metadata = value
	}
	return nil
}

// Resolve resolves the output's media URN through the FabricRegistry.
func (co *CapOutput) Resolve(registry media.MediaDefSource) (*media.ResolvedMediaDef, error) {
	return media.ResolveMediaUrn(co.MediaUrn, registry)
}

// IsStructured checks if this output produces structured data (map or list).
func (co *CapOutput) IsStructured(registry media.MediaDefSource) (bool, error) {
	resolved, err := co.Resolve(registry)
	if err != nil {
		return false, fmt.Errorf("failed to resolve output media_urn '%s': %w", co.MediaUrn, err)
	}
	return resolved.IsStructured(), nil
}

// GetMediaType returns the resolved media type for this output.
func (co *CapOutput) GetMediaType(registry media.MediaDefSource) (string, error) {
	resolved, err := co.Resolve(registry)
	if err != nil {
		return "", fmt.Errorf("failed to resolve output media_urn '%s': %w", co.MediaUrn, err)
	}
	return resolved.MediaType, nil
}

// GetMetadata gets the metadata JSON for CapOutput
func (co *CapOutput) GetMetadata() any {
	return co.Metadata
}

// SetMetadata sets the metadata JSON for CapOutput
func (co *CapOutput) SetMetadata(metadata any) {
	co.Metadata = metadata
}

// NewCapOutput creates a new output definition. `name` is the label a machine
// takes it by; it must be unique among the cap's outputs.
// Matches Rust: CapOutput::new(media_urn, name, description)
func NewCapOutput(mediaUrn string, name string, description string) *CapOutput {
	return &CapOutput{
		MediaUrn:          mediaUrn,
		Name:              name,
		OutputDescription: description,
	}
}

// NewCapOutputWithFullDefinition creates a new output definition with all fields set
func NewCapOutputWithFullDefinition(mediaUrn string, name string, description string, metadata any) *CapOutput {
	return &CapOutput{
		MediaUrn:          mediaUrn,
		Name:              name,
		OutputDescription: description,
		Metadata:          metadata,
	}
}

// GetName returns the output's name.
func (co *CapOutput) GetName() string {
	return co.Name
}

// AsOptional marks whether the cap may end without producing this output.
// Matches Rust: pub fn optional(self, optional: bool) -> Self
func (co *CapOutput) AsOptional(optional bool) *CapOutput {
	co.Optional = optional
	return co
}

// CarryingInput marks the output as carrying the cap's main input, type
// unchanged, rather than its own media URN.
// Matches Rust: pub fn carrying_input(self) -> Self
func (co *CapOutput) CarryingInput() *CapOutput {
	carries := OutputCarriesInput
	co.Carries = &carries
	return co
}

// ClearMetadata clears the metadata for CapOutput
func (co *CapOutput) ClearMetadata() {
	co.Metadata = nil
}

// RegisteredBy represents registration attribution - who registered a capability and when
type RegisteredBy struct {
	Username     string `json:"username"`
	RegisteredAt string `json:"registered_at"`
}

// NewRegisteredBy creates a new registration attribution
func NewRegisteredBy(username string, registeredAt string) RegisteredBy {
	return RegisteredBy{
		Username:     username,
		RegisteredAt: registeredAt,
	}
}

// NewMediaValidationNumericRange creates validation with numeric constraints
func NewMediaValidationNumericRange(min, max *float64) *media.MediaValidation {
	return &media.MediaValidation{
		Min: min,
		Max: max,
	}
}

// NewMediaValidationStringLength creates validation with string length constraints
func NewMediaValidationStringLength(minLength, maxLength *int) *media.MediaValidation {
	return &media.MediaValidation{
		MinLength: minLength,
		MaxLength: maxLength,
	}
}

// NewMediaValidationPattern creates validation with pattern
func NewMediaValidationPattern(pattern string) *media.MediaValidation {
	return &media.MediaValidation{
		Pattern: &pattern,
	}
}

// NewMediaValidationAllowedValues creates validation with allowed values
func NewMediaValidationAllowedValues(values []string) *media.MediaValidation {
	return &media.MediaValidation{
		AllowedValues: values,
	}
}

// Cap represents a formal cap definition.
//
// Caps do not carry inline media defs; every media URN is resolved
// through the unified FabricRegistry.
type Cap struct {
	Urn            *urn.CapUrn `json:"urn"`
	Version        uint32      `json:"version,omitempty"`
	Title          string      `json:"title"`
	CapDescription *string     `json:"cap_description,omitempty"`
	// Documentation is only what is specific to this cap: what the dims it
	// was built from mean is documented with each dim (see Dims).
	Documentation *string `json:"documentation,omitempty"`
	// Dims are the dims this cap was built from, and the value chosen from
	// each, in the order its source references them. Each dim is published on
	// its own (FabricRegistry.GetDim) with its documentation and each value's;
	// a reader shows that beside Documentation. Empty when the cap was built
	// from no dim. Descriptive only: matching and dispatch never read it.
	Dims     []media.DimRef    `json:"dims,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	// Aliases are the globally-unique human-facing names that select this cap
	// in both the capdag CLI and the direct cartridge CLI. Replaces the former
	// non-unique `command`. At least one; uniqueness is enforced at publish.
	Aliases []string `json:"aliases"`
	// IsAbstract marks a generic-input dispatch umbrella cap: a valid alias
	// target never backed by a cartridge and never a runnable graph edge.
	IsAbstract bool     `json:"abstract,omitempty"`
	Args       []CapArg `json:"args,omitempty"`
	// Outputs are the outputs the cap gives. Exactly one is the main output,
	// whose media URN is the cap URN's out=; a cap whose out= is media:void
	// declares none (RULE21). Each has a response stream of its own.
	Outputs             []CapOutput   `json:"outputs,omitempty"`
	MetadataJSON        any           `json:"metadata_json,omitempty"`
	RegisteredBy        *RegisteredBy `json:"registered_by,omitempty"`
	SupportedModelTypes []string      `json:"supported_model_types,omitempty"`
	DefaultModelSpec    *string       `json:"default_model_spec,omitempty"`
}

// NewCap creates a new cap
func NewCap(urn *urn.CapUrn, title string, aliases []string) *Cap {
	return &Cap{
		Urn:      urn,
		Title:    title,
		Aliases:  aliases,
		Metadata: make(map[string]string),
		Args:     []CapArg{},
	}
}

// NewCapWithDescription creates a new cap with description
func NewCapWithDescription(urn *urn.CapUrn, title string, aliases []string, description string) *Cap {
	return &Cap{
		Urn:            urn,
		Title:          title,
		Aliases:        aliases,
		CapDescription: &description,
		Metadata:       make(map[string]string),
		Args:           []CapArg{},
	}
}

// NewCapWithArgs creates a new cap with arguments
func NewCapWithArgs(u *urn.CapUrn, title string, aliases []string, args []CapArg) *Cap {
	return &Cap{
		Urn:      u,
		Title:    title,
		Aliases:  aliases,
		Metadata: make(map[string]string),
		Args:     args,
	}
}

// NewCapWithFullDefinition creates a new cap with all fields set
func NewCapWithFullDefinition(
	u *urn.CapUrn,
	title string,
	capDescription *string,
	metadata map[string]string,
	aliases []string,
	args []CapArg,
	outputs []CapOutput,
	metadataJSON any,
) *Cap {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	if args == nil {
		args = []CapArg{}
	}
	return &Cap{
		Urn:            u,
		Title:          title,
		CapDescription: capDescription,
		Metadata:       metadata,
		Aliases:        aliases,
		Args:           args,
		Outputs:        outputs,
		MetadataJSON:   metadataJSON,
	}
}

// NewCapWithMetadata creates a new cap with metadata
func NewCapWithMetadata(urn *urn.CapUrn, title string, aliases []string, metadata map[string]string) *Cap {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	return &Cap{
		Urn:      urn,
		Title:    title,
		Aliases:  aliases,
		Metadata: metadata,
		Args:     []CapArg{},
	}
}

// MatchesRequest reports whether this cap, as a candidate, can serve the request
// given as a string (CapUrn.IsDispatchable).
func (c *Cap) MatchesRequest(request string) bool {
	requestId, err := urn.NewCapUrnFromString(request)
	if err != nil {
		return false
	}
	return c.Urn.IsDispatchable(requestId)
}

// AcceptsRequest reports whether this cap, as a candidate, can serve the request
// (CapUrn.IsDispatchable).
func (c *Cap) AcceptsRequest(request *urn.CapUrn) bool {
	return c.Urn.IsDispatchable(request)
}

// IsMoreSpecificThan checks if this cap is more specific than another for a given request.
// Both caps must accept the request; then compares specificity.
func (c *Cap) IsMoreSpecificThan(other *Cap, request string) bool {
	if other == nil {
		return true
	}
	if !c.MatchesRequest(request) || !other.MatchesRequest(request) {
		return false
	}
	return c.Urn.IsMoreSpecificThan(other.Urn)
}

// GetMetadata gets a metadata value by key
func (c *Cap) GetMetadata(key string) (string, bool) {
	if c.Metadata == nil {
		return "", false
	}
	value, exists := c.Metadata[key]
	return value, exists
}

// SetMetadata sets a metadata value
func (c *Cap) SetMetadata(key, value string) {
	if c.Metadata == nil {
		c.Metadata = make(map[string]string)
	}
	c.Metadata[key] = value
}

// RemoveMetadata removes a metadata value and returns it (or empty string + false if absent)
func (c *Cap) RemoveMetadata(key string) (string, bool) {
	if c.Metadata == nil {
		return "", false
	}
	value, exists := c.Metadata[key]
	if exists {
		delete(c.Metadata, key)
	}
	return value, exists
}

// HasMetadata checks if this cap has specific metadata
func (c *Cap) HasMetadata(key string) bool {
	if c.Metadata == nil {
		return false
	}
	_, exists := c.Metadata[key]
	return exists
}

// GetTitle gets the title
func (c *Cap) GetTitle() string {
	return c.Title
}

// SetTitle sets the title
func (c *Cap) SetTitle(title string) {
	c.Title = title
}

// GetAliases returns the cap's globally-unique selection names.
func (c *Cap) GetAliases() []string {
	return c.Aliases
}

// SetAliases sets the cap's aliases.
func (c *Cap) SetAliases(aliases []string) {
	c.Aliases = aliases
}

// PrimaryAlias returns the first alias (single-name display). A cap always
// has at least one alias.
func (c *Cap) PrimaryAlias() string {
	if len(c.Aliases) == 0 {
		return ""
	}
	return c.Aliases[0]
}

// HasAlias reports whether name is one of this cap's aliases (exact match).
func (c *Cap) HasAlias(name string) bool {
	for _, a := range c.Aliases {
		if a == name {
			return true
		}
	}
	return false
}

// GetOutputs returns every output the cap declares.
// Matches Rust: pub fn get_outputs(&self) -> &[CapOutput]
func (c *Cap) GetOutputs() []CapOutput {
	return c.Outputs
}

// MainOutput returns the cap's main output — the one whose media URN is
// equivalent to the URN's out= — or nil when it declares none.
// Matches Rust: pub fn main_output(&self) -> Option<&CapOutput>
func (c *Cap) MainOutput() *CapOutput {
	outSpec, err := urn.NewMediaUrnFromString(c.Urn.OutSpec())
	if err != nil {
		return nil
	}
	for i := range c.Outputs {
		outputUrn, err := urn.NewMediaUrnFromString(c.Outputs[i].MediaUrn)
		if err != nil {
			continue
		}
		if outputUrn.IsEquivalent(outSpec) {
			return &c.Outputs[i]
		}
	}
	return nil
}

// OutputByUrn returns the output whose media URN is equivalent to mediaUrn.
// Matches Rust: pub fn output_by_urn(&self, media_urn: &str) -> Option<&CapOutput>
func (c *Cap) OutputByUrn(mediaUrn string) *CapOutput {
	wanted, err := urn.NewMediaUrnFromString(mediaUrn)
	if err != nil {
		return nil
	}
	for i := range c.Outputs {
		outputUrn, err := urn.NewMediaUrnFromString(c.Outputs[i].MediaUrn)
		if err != nil {
			continue
		}
		if outputUrn.IsEquivalent(wanted) {
			return &c.Outputs[i]
		}
	}
	return nil
}

// OutputNamed returns the output a machine takes by `name`.
// Matches Rust: pub fn output_named(&self, name: &str) -> Option<&CapOutput>
func (c *Cap) OutputNamed(name string) *CapOutput {
	for i := range c.Outputs {
		if c.Outputs[i].Name == name {
			return &c.Outputs[i]
		}
	}
	return nil
}

// ArgNamed returns the argument a machine selects by `name`.
// Matches Rust: pub fn arg_named(&self, name: &str) -> Option<&CapArg>
func (c *Cap) ArgNamed(name string) *CapArg {
	for i := range c.Args {
		if c.Args[i].Name == name {
			return &c.Args[i]
		}
	}
	return nil
}

// SetOutputs replaces the cap's outputs.
func (c *Cap) SetOutputs(outputs []CapOutput) {
	c.Outputs = outputs
}

// AddOutput adds an output to the cap.
func (c *Cap) AddOutput(output CapOutput) {
	c.Outputs = append(c.Outputs, output)
}

// GetMetadataJSON gets the metadata JSON
func (c *Cap) GetMetadataJSON() any {
	return c.MetadataJSON
}

// SetMetadataJSON sets the metadata JSON
func (c *Cap) SetMetadataJSON(metadata any) {
	c.MetadataJSON = metadata
}

// ClearMetadataJSON clears the metadata JSON
func (c *Cap) ClearMetadataJSON() {
	c.MetadataJSON = nil
}

// GetRegisteredBy gets the registration attribution
func (c *Cap) GetRegisteredBy() *RegisteredBy {
	return c.RegisteredBy
}

// SetRegisteredBy sets the registration attribution
func (c *Cap) SetRegisteredBy(registeredBy *RegisteredBy) {
	c.RegisteredBy = registeredBy
}

// ClearRegisteredBy clears the registration attribution
func (c *Cap) ClearRegisteredBy() {
	c.RegisteredBy = nil
}

// GetDocumentation returns the long-form markdown documentation, if any.
func (c *Cap) GetDocumentation() *string {
	return c.Documentation
}

// SetDocumentation sets the long-form markdown documentation.
func (c *Cap) SetDocumentation(doc string) {
	c.Documentation = &doc
}

// ClearDocumentation clears the long-form markdown documentation.
func (c *Cap) ClearDocumentation() {
	c.Documentation = nil
}

// GetStdinMediaUrn returns the stdin media URN from args (first stdin source found)
func (c *Cap) GetStdinMediaUrn() *string {
	for _, arg := range c.Args {
		if urn := arg.GetStdinMediaUrn(); urn != nil {
			return urn
		}
	}
	return nil
}

// AcceptsStdin returns true if any arg has a stdin source
func (c *Cap) AcceptsStdin() bool {
	return c.GetStdinMediaUrn() != nil
}

// SequenceShape returns the cardinality shape of this cap's primary data path:
// (inputIsSequence, outputIsSequence).
//
// inputIsSequence is the IsSequence flag of the arg whose Stdin source matches
// the cap URN's in= spec. Argument declaration order has no semantics.
// outputIsSequence is the output's IsSequence flag.
//
// This is THE single definition of cap cardinality. Path search
// (planner.LiveCapFab path search), editor realization (machine.realizeStrand),
// and notation resolution (machine.resolvePreInterned) all read it here so they
// can never diverge — the distinction that decides whether a ForEach is
// synthesized.
// Matches Rust: pub fn sequence_shape(&self) -> (bool, bool)
func (c *Cap) SequenceShape() (bool, bool) {
	inputIsSequence := false
	inSpec, err := urn.NewMediaUrnFromString(c.Urn.InSpec())
	if err != nil {
		panic("cap registry invariant: cap in= is a valid MediaUrn: " + err.Error())
	}
	voidMedia, err := urn.NewMediaUrnFromString(standard.MediaVoid)
	if err != nil {
		panic("standard.MediaVoid is a valid MediaUrn: " + err.Error())
	}
	if !inSpec.IsEquivalent(voidMedia) {
		foundMain := false
		for i := range c.Args {
			if c.Args[i].IsMainInput(inSpec) {
				inputIsSequence = c.Args[i].IsSequence
				foundMain = true
				break
			}
		}
		if !foundMain {
			panic("cap registry invariant: every non-void cap declares its main input")
		}
	}
	// The primary data path ends at the main output.
	outputIsSequence := false
	if main := c.MainOutput(); main != nil {
		outputIsSequence = main.IsSequence
	}
	return inputIsSequence, outputIsSequence
}

// MainInputArg returns the cap's main input argument — the arg whose stdin
// source is equivalent to the URN's in= — or nil for a void-input cap.
// Matches Rust: pub fn main_input_arg(&self) -> Option<&CapArg>
func (c *Cap) MainInputArg() *CapArg {
	inSpec, err := urn.NewMediaUrnFromString(c.Urn.InSpec())
	if err != nil {
		return nil
	}
	for i := range c.Args {
		if c.Args[i].IsMainInput(inSpec) {
			return &c.Args[i]
		}
	}
	return nil
}

// StreamingShape returns the streaming contract of this cap's primary data
// path: (inputStreams, outputStreams) — whether the main input is consumed
// without a length promise and whether the output may be emitted without
// one. Orthogonal to SequenceShape. The executor's hop rule reads it: a
// streaming producer into a non-streaming consumer is a chain boundary.
// Matches Rust: pub fn streaming_shape(&self) -> (bool, bool)
func (c *Cap) StreamingShape() (bool, bool) {
	inputStreams := false
	if arg := c.MainInputArg(); arg != nil {
		inputStreams = arg.Streaming
	}
	outputStreams := false
	if main := c.MainOutput(); main != nil {
		outputStreams = main.Streaming
	}
	return inputStreams, outputStreams
}

// NeedsForeach reports whether a data position of cardinality sourceIsSequence
// feeding this cap's primary input requires a ForEach (per-item map) to be
// inserted before it.
//
// The one rule, shared by every planner/resolver path: a sequence feeding a
// scalar-input cap must be mapped. The media URN does not change — ForEach is a
// shape transition, not a type transition.
// Matches Rust: pub fn needs_foreach(&self, source_is_sequence: bool) -> bool
func (c *Cap) NeedsForeach(sourceIsSequence bool) bool {
	inputIsSequence, _ := c.SequenceShape()
	return sourceIsSequence && !inputIsSequence
}

// GetArgs returns the args
func (c *Cap) GetArgs() []CapArg {
	return c.Args
}

// AddArg adds an argument
func (c *Cap) AddArg(arg CapArg) {
	c.Args = append(c.Args, arg)
}

// GetRequiredArgs returns all required arguments
func (c *Cap) GetRequiredArgs() []CapArg {
	var required []CapArg
	for _, arg := range c.Args {
		if arg.Required {
			required = append(required, arg)
		}
	}
	return required
}

// GetOptionalArgs returns all optional arguments
func (c *Cap) GetOptionalArgs() []CapArg {
	var optional []CapArg
	for _, arg := range c.Args {
		if !arg.Required {
			optional = append(optional, arg)
		}
	}
	return optional
}

// FindArgByMediaUrn finds an argument by media_urn
func (c *Cap) FindArgByMediaUrn(mediaUrn string) *CapArg {
	for i := range c.Args {
		if c.Args[i].MediaUrn == mediaUrn {
			return &c.Args[i]
		}
	}
	return nil
}

// GetPositionalArgs returns arguments that have position sources, sorted by position
func (c *Cap) GetPositionalArgs() []CapArg {
	var positional []CapArg
	for _, arg := range c.Args {
		if arg.HasPositionSource() {
			positional = append(positional, arg)
		}
	}
	// Sort by position
	for i := 0; i < len(positional)-1; i++ {
		for j := i + 1; j < len(positional); j++ {
			posI := positional[i].GetPosition()
			posJ := positional[j].GetPosition()
			if posI != nil && posJ != nil && *posI > *posJ {
				positional[i], positional[j] = positional[j], positional[i]
			}
		}
	}
	return positional
}

// GetFlagArgs returns arguments that have cli_flag sources
func (c *Cap) GetFlagArgs() []CapArg {
	var flagArgs []CapArg
	for _, arg := range c.Args {
		if arg.HasCliFlagSource() {
			flagArgs = append(flagArgs, arg)
		}
	}
	return flagArgs
}

// UrnString gets the cap URN as a string
func (c *Cap) UrnString() string {
	return c.Urn.ToString()
}

// equalStringSetsCap reports whether two string slices contain the same
// elements regardless of order (used for order-insensitive alias comparison).
func equalStringSetsCap(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, s := range a {
		counts[s]++
	}
	for _, s := range b {
		counts[s]--
		if counts[s] < 0 {
			return false
		}
	}
	return true
}

// Equals checks if this cap is equal to another
func (c *Cap) Equals(other *Cap) bool {
	if other == nil {
		return false
	}

	if !c.Urn.Equals(other.Urn) {
		return false
	}

	if c.Title != other.Title {
		return false
	}

	if !equalStringSetsCap(c.Aliases, other.Aliases) {
		return false
	}

	if c.IsAbstract != other.IsAbstract {
		return false
	}

	if (c.CapDescription == nil) != (other.CapDescription == nil) {
		return false
	}

	if c.CapDescription != nil && *c.CapDescription != *other.CapDescription {
		return false
	}

	if (c.Documentation == nil) != (other.Documentation == nil) {
		return false
	}

	if c.Documentation != nil && *c.Documentation != *other.Documentation {
		return false
	}

	if len(c.Dims) != len(other.Dims) {
		return false
	}
	for i := range c.Dims {
		if c.Dims[i] != other.Dims[i] {
			return false
		}
	}

	if len(c.Metadata) != len(other.Metadata) {
		return false
	}

	for key, value := range c.Metadata {
		if otherValue, exists := other.Metadata[key]; !exists || value != otherValue {
			return false
		}
	}

	if !reflect.DeepEqual(c.Args, other.Args) {
		return false
	}

	if len(c.Outputs) != len(other.Outputs) || (len(c.Outputs) > 0 && !reflect.DeepEqual(c.Outputs, other.Outputs)) {
		return false
	}

	if !reflect.DeepEqual(c.MetadataJSON, other.MetadataJSON) {
		return false
	}

	if !reflect.DeepEqual(c.RegisteredBy, other.RegisteredBy) {
		return false
	}

	return true
}

// MarshalJSON implements custom JSON marshaling
func (c *Cap) MarshalJSON() ([]byte, error) {
	capData := map[string]any{
		"urn":     c.Urn.String(),
		"title":   c.Title,
		"aliases": c.Aliases,
	}

	if c.IsAbstract {
		capData["abstract"] = true
	}

	if c.Version != 0 {
		capData["version"] = c.Version
	}

	if c.CapDescription != nil {
		capData["cap_description"] = *c.CapDescription
	}

	if c.Documentation != nil {
		capData["documentation"] = *c.Documentation
	}

	// Emit `dims` only when the cap was built from some; absent ⇒ none,
	// exactly as the registry publishes it.
	if len(c.Dims) > 0 {
		capData["dims"] = c.Dims
	}

	if len(c.Metadata) > 0 {
		capData["metadata"] = c.Metadata
	}

	if len(c.Args) > 0 {
		capData["args"] = c.Args
	}

	if len(c.Outputs) > 0 {
		capData["outputs"] = c.Outputs
	}

	if c.MetadataJSON != nil {
		capData["metadata_json"] = c.MetadataJSON
	}

	if c.RegisteredBy != nil {
		capData["registered_by"] = c.RegisteredBy
	}

	if len(c.SupportedModelTypes) > 0 {
		capData["supported_model_types"] = c.SupportedModelTypes
	}

	if c.DefaultModelSpec != nil {
		capData["default_model_spec"] = *c.DefaultModelSpec
	}

	return json.Marshal(capData)
}

// UnmarshalJSON implements custom JSON unmarshaling
func (c *Cap) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// URN must be a string in canonical format
	urnField, ok := raw["urn"]
	if !ok {
		return fmt.Errorf("missing required field 'urn'")
	}

	urnStr, ok := urnField.(string)
	if !ok {
		return fmt.Errorf("URN must be a string in canonical format (e.g., 'cap:in=\"media:...\";op=...;out=\"media:...\"')")
	}

	urn, err := urn.NewCapUrnFromString(urnStr)
	if err != nil {
		return fmt.Errorf("failed to parse URN string: %v", err)
	}

	c.Urn = urn

	// Handle version (optional, defaults to 0)
	if versionRaw, ok := raw["version"]; ok && versionRaw != nil {
		switch v := versionRaw.(type) {
		case float64:
			c.Version = uint32(v)
		case json.Number:
			n, err := v.Int64()
			if err != nil {
				return fmt.Errorf("invalid 'version' field: %w", err)
			}
			c.Version = uint32(n)
		}
	}

	// Handle required fields
	if title, ok := raw["title"].(string); ok {
		c.Title = title
	} else {
		return fmt.Errorf("missing required field 'title'")
	}

	// A cap must declare at least one alias — it is how the cap is selected in
	// both CLIs. Absent or empty is a hard error, never silently defaulted.
	if aliasesRaw, ok := raw["aliases"]; ok {
		aliasesBytes, _ := json.Marshal(aliasesRaw)
		var aliases []string
		if err := json.Unmarshal(aliasesBytes, &aliases); err != nil {
			return fmt.Errorf("failed to unmarshal aliases: %w", err)
		}
		c.Aliases = aliases
	}
	if len(c.Aliases) == 0 {
		return fmt.Errorf("cap %q must declare at least one alias (the 'aliases' field is required and non-empty)", c.Urn.ToString())
	}

	// Abstract flag (optional; absent => false).
	if abstractRaw, ok := raw["abstract"].(bool); ok {
		c.IsAbstract = abstractRaw
	}

	if desc, ok := raw["cap_description"].(string); ok {
		c.CapDescription = &desc
	}

	if doc, ok := raw["documentation"].(string); ok {
		c.Documentation = &doc
	}

	// Dims: each entry refuses keys it does not know, like an argument's; a
	// dim is named at most once, since a definition cannot have been built
	// with two values of one dim.
	c.Dims = nil
	if dimsRaw, ok := raw["dims"]; ok {
		dimsBytes, _ := json.Marshal(dimsRaw)
		var dims []media.DimRef
		if err := json.Unmarshal(dimsBytes, &dims); err != nil {
			return fmt.Errorf("failed to unmarshal dims: %w", err)
		}
		if err := media.CheckDimRefs(dims); err != nil {
			return fmt.Errorf("cap '%s' dims: %v", c.Urn.String(), err)
		}
		c.Dims = dims
	}

	if metadata, ok := raw["metadata"].(map[string]any); ok {
		c.Metadata = make(map[string]string)
		for k, v := range metadata {
			if s, ok := v.(string); ok {
				c.Metadata[k] = s
			}
		}
	}

	// Handle args
	if argsRaw, ok := raw["args"]; ok {
		argsBytes, _ := json.Marshal(argsRaw)
		var args []CapArg
		if err := json.Unmarshal(argsBytes, &args); err != nil {
			return fmt.Errorf("failed to unmarshal args: %w", err)
		}
		c.Args = args
	}

	// Handle outputs
	c.Outputs = nil
	if outputsRaw, ok := raw["outputs"]; ok {
		outputsBytes, _ := json.Marshal(outputsRaw)
		var outputs []CapOutput
		if err := json.Unmarshal(outputsBytes, &outputs); err != nil {
			return fmt.Errorf("failed to unmarshal outputs: %w", err)
		}
		c.Outputs = outputs
	}

	if metadataJSON, ok := raw["metadata_json"]; ok {
		c.MetadataJSON = metadataJSON
	}

	if registeredByRaw, ok := raw["registered_by"]; ok {
		registeredByBytes, _ := json.Marshal(registeredByRaw)
		var registeredBy RegisteredBy
		if err := json.Unmarshal(registeredByBytes, &registeredBy); err != nil {
			return fmt.Errorf("failed to unmarshal registered_by: %w", err)
		}
		c.RegisteredBy = &registeredBy
	}

	if supportedModelTypesRaw, ok := raw["supported_model_types"]; ok {
		supportedModelTypesBytes, _ := json.Marshal(supportedModelTypesRaw)
		var supportedModelTypes []string
		if err := json.Unmarshal(supportedModelTypesBytes, &supportedModelTypes); err != nil {
			return fmt.Errorf("failed to unmarshal supported_model_types: %w", err)
		}
		c.SupportedModelTypes = supportedModelTypes
	}

	if defaultModelSpec, ok := raw["default_model_spec"].(string); ok {
		c.DefaultModelSpec = &defaultModelSpec
	}

	return nil
}
