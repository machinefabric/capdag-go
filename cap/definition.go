package cap

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/machinefabric/capdag-go/media"
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
	MediaUrn       string      `json:"media_urn"`
	Required       bool        `json:"required"`
	IsSequence     bool        `json:"is_sequence,omitempty"`
	Sources        []ArgSource `json:"sources"`
	ArgDescription string      `json:"arg_description,omitempty"`
	DefaultValue   any         `json:"default_value,omitempty"`
	Metadata       any         `json:"metadata,omitempty"`
}

// NewCapArg creates a new cap argument
func NewCapArg(mediaUrn string, required bool, sources []ArgSource) CapArg {
	return CapArg{
		MediaUrn: mediaUrn,
		Required: required,
		Sources:  sources,
	}
}

// NewCapArgWithDescription creates a new cap argument with description
func NewCapArgWithDescription(mediaUrn string, required bool, sources []ArgSource, description string) CapArg {
	return CapArg{
		MediaUrn:       mediaUrn,
		Required:       required,
		Sources:        sources,
		ArgDescription: description,
	}
}

// NewCapArgWithFullDefinition creates a new cap argument with all fields set
func NewCapArgWithFullDefinition(
	mediaUrn string,
	required bool,
	sources []ArgSource,
	argDescription string,
	defaultValue any,
	metadata any,
) CapArg {
	return CapArg{
		MediaUrn:       mediaUrn,
		Required:       required,
		Sources:        sources,
		ArgDescription: argDescription,
		DefaultValue:   defaultValue,
		Metadata:       metadata,
	}
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

// Resolve resolves the argument's media URN to a media.ResolvedMediaSpec
func (a *CapArg) Resolve(mediaSpecs []media.MediaSpecDef, registry *media.MediaUrnRegistry) (*media.ResolvedMediaSpec, error) {
	return media.ResolveMediaUrn(a.MediaUrn, mediaSpecs, registry)
}

// IsBinary checks if this argument expects binary data.
func (a *CapArg) IsBinary(mediaSpecs []media.MediaSpecDef, registry *media.MediaUrnRegistry) (bool, error) {
	resolved, err := a.Resolve(mediaSpecs, registry)
	if err != nil {
		return false, fmt.Errorf("failed to resolve argument media_urn '%s': %w", a.MediaUrn, err)
	}
	return resolved.IsBinary(), nil
}

// IsStructured checks if this argument expects structured data (map or list).
// Structured data can be serialized as JSON when transmitted as text.
func (a *CapArg) IsStructured(mediaSpecs []media.MediaSpecDef, registry *media.MediaUrnRegistry) (bool, error) {
	resolved, err := a.Resolve(mediaSpecs, registry)
	if err != nil {
		return false, fmt.Errorf("failed to resolve argument media_urn '%s': %w", a.MediaUrn, err)
	}
	return resolved.IsStructured(), nil
}

// GetMediaType returns the resolved media type for this argument.
func (a *CapArg) GetMediaType(mediaSpecs []media.MediaSpecDef, registry *media.MediaUrnRegistry) (string, error) {
	resolved, err := a.Resolve(mediaSpecs, registry)
	if err != nil {
		return "", fmt.Errorf("failed to resolve argument media_urn '%s': %w", a.MediaUrn, err)
	}
	return resolved.MediaType, nil
}

// CapOutput represents the output definition for a cap
type CapOutput struct {
	MediaUrn          string `json:"media_urn"`
	OutputDescription string `json:"output_description"`
	IsSequence        bool   `json:"is_sequence,omitempty"`
	Metadata          any    `json:"metadata,omitempty"`
}

// Resolve resolves the output's media URN to a media.ResolvedMediaSpec
func (co *CapOutput) Resolve(mediaSpecs []media.MediaSpecDef, registry *media.MediaUrnRegistry) (*media.ResolvedMediaSpec, error) {
	return media.ResolveMediaUrn(co.MediaUrn, mediaSpecs, registry)
}

// IsBinary checks if this output produces binary data.
func (co *CapOutput) IsBinary(mediaSpecs []media.MediaSpecDef, registry *media.MediaUrnRegistry) (bool, error) {
	resolved, err := co.Resolve(mediaSpecs, registry)
	if err != nil {
		return false, fmt.Errorf("failed to resolve output media_urn '%s': %w", co.MediaUrn, err)
	}
	return resolved.IsBinary(), nil
}

// IsStructured checks if this output produces structured data (map or list).
// Structured data can be serialized as JSON when transmitted as text.
func (co *CapOutput) IsStructured(mediaSpecs []media.MediaSpecDef, registry *media.MediaUrnRegistry) (bool, error) {
	resolved, err := co.Resolve(mediaSpecs, registry)
	if err != nil {
		return false, fmt.Errorf("failed to resolve output media_urn '%s': %w", co.MediaUrn, err)
	}
	return resolved.IsStructured(), nil
}

// GetMediaType returns the resolved media type for this output.
func (co *CapOutput) GetMediaType(mediaSpecs []media.MediaSpecDef, registry *media.MediaUrnRegistry) (string, error) {
	resolved, err := co.Resolve(mediaSpecs, registry)
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

// NewCapOutput creates a new output definition with a media URN
func NewCapOutput(mediaUrn string, description string) *CapOutput {
	return &CapOutput{
		MediaUrn:          mediaUrn,
		OutputDescription: description,
	}
}

// NewCapOutputWithFullDefinition creates a new output definition with all fields set
func NewCapOutputWithFullDefinition(mediaUrn string, description string, metadata any) *CapOutput {
	return &CapOutput{
		MediaUrn:          mediaUrn,
		OutputDescription: description,
		Metadata:          metadata,
	}
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

// Cap represents a formal cap definition
type Cap struct {
	Urn            *urn.CapUrn          `json:"urn"`
	Title          string               `json:"title"`
	CapDescription *string              `json:"cap_description,omitempty"`
	Documentation  *string              `json:"documentation,omitempty"`
	Metadata       map[string]string    `json:"metadata,omitempty"`
	Command        string               `json:"command"`
	MediaSpecs     []media.MediaSpecDef `json:"media_specs,omitempty"`
	Args           []CapArg             `json:"args,omitempty"`
	Output         *CapOutput           `json:"output,omitempty"`
	MetadataJSON   any                  `json:"metadata_json,omitempty"`
	RegisteredBy   *RegisteredBy        `json:"registered_by,omitempty"`
}

// NewCap creates a new cap
func NewCap(urn *urn.CapUrn, title string, command string) *Cap {
	return &Cap{
		Urn:        urn,
		Title:      title,
		Command:    command,
		Metadata:   make(map[string]string),
		MediaSpecs: []media.MediaSpecDef{},
		Args:       []CapArg{},
	}
}

// NewCapWithDescription creates a new cap with description
func NewCapWithDescription(urn *urn.CapUrn, title string, command string, description string) *Cap {
	return &Cap{
		Urn:            urn,
		Title:          title,
		Command:        command,
		CapDescription: &description,
		Metadata:       make(map[string]string),
		MediaSpecs:     []media.MediaSpecDef{},
		Args:           []CapArg{},
	}
}

// NewCapWithArgs creates a new cap with arguments
func NewCapWithArgs(u *urn.CapUrn, title string, command string, args []CapArg) *Cap {
	return &Cap{
		Urn:        u,
		Title:      title,
		Command:    command,
		Metadata:   make(map[string]string),
		MediaSpecs: []media.MediaSpecDef{},
		Args:       args,
	}
}

// NewCapWithFullDefinition creates a new cap with all fields set
func NewCapWithFullDefinition(
	u *urn.CapUrn,
	title string,
	capDescription *string,
	metadata map[string]string,
	command string,
	mediaSpecs []media.MediaSpecDef,
	args []CapArg,
	output *CapOutput,
	metadataJSON any,
) *Cap {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	if mediaSpecs == nil {
		mediaSpecs = []media.MediaSpecDef{}
	}
	if args == nil {
		args = []CapArg{}
	}
	return &Cap{
		Urn:            u,
		Title:          title,
		CapDescription: capDescription,
		Metadata:       metadata,
		Command:        command,
		MediaSpecs:     mediaSpecs,
		Args:           args,
		Output:         output,
		MetadataJSON:   metadataJSON,
	}
}

// NewCapWithMetadata creates a new cap with metadata
func NewCapWithMetadata(urn *urn.CapUrn, title string, command string, metadata map[string]string) *Cap {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	return &Cap{
		Urn:        urn,
		Title:      title,
		Command:    command,
		Metadata:   metadata,
		MediaSpecs: []media.MediaSpecDef{},
		Args:       []CapArg{},
	}
}

// GetMediaSpecs returns the media specs array
func (c *Cap) GetMediaSpecs() []media.MediaSpecDef {
	if c.MediaSpecs == nil {
		c.MediaSpecs = []media.MediaSpecDef{}
	}
	return c.MediaSpecs
}

// SetMediaSpecs sets the media specs array
func (c *Cap) SetMediaSpecs(mediaSpecs []media.MediaSpecDef) {
	c.MediaSpecs = mediaSpecs
}

// AddMediaSpec adds a media spec to the array
// The URN is taken from the def.Urn field
func (c *Cap) AddMediaSpec(def media.MediaSpecDef) {
	if c.MediaSpecs == nil {
		c.MediaSpecs = []media.MediaSpecDef{}
	}
	c.MediaSpecs = append(c.MediaSpecs, def)
}

// ResolveMediaUrn resolves a media URN using this cap's media_specs table and registry
func (c *Cap) ResolveMediaUrn(mediaUrn string, registry *media.MediaUrnRegistry) (*media.ResolvedMediaSpec, error) {
	return media.ResolveMediaUrn(mediaUrn, c.GetMediaSpecs(), registry)
}

// MatchesRequest checks if this cap matches a request string.
// Uses routing direction: request is the pattern, cap is the instance.
// request.Accepts(cap) — request only specifies constraints; cap must satisfy them.
func (c *Cap) MatchesRequest(request string) bool {
	requestId, err := urn.NewCapUrnFromString(request)
	if err != nil {
		return false
	}
	return requestId.Accepts(c.Urn)
}

// AcceptsRequest checks if this cap matches a request.
// Uses routing direction: request is the pattern, cap is the instance.
// request.Accepts(cap) — request specifies constraints; cap must satisfy them.
func (c *Cap) AcceptsRequest(request *urn.CapUrn) bool {
	return request.Accepts(c.Urn)
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

// GetCommand gets the command
func (c *Cap) GetCommand() string {
	return c.Command
}

// SetCommand sets the command
func (c *Cap) SetCommand(command string) {
	c.Command = command
}

// GetOutput gets the output definition if defined
func (c *Cap) GetOutput() *CapOutput {
	return c.Output
}

// SetOutput sets the output definition
func (c *Cap) SetOutput(output *CapOutput) {
	c.Output = output
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

	if c.Command != other.Command {
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

	if len(c.Metadata) != len(other.Metadata) {
		return false
	}

	for key, value := range c.Metadata {
		if otherValue, exists := other.Metadata[key]; !exists || value != otherValue {
			return false
		}
	}

	if !reflect.DeepEqual(c.MediaSpecs, other.MediaSpecs) {
		return false
	}

	if !reflect.DeepEqual(c.Args, other.Args) {
		return false
	}

	if !reflect.DeepEqual(c.Output, other.Output) {
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
		"command": c.Command,
	}

	if c.CapDescription != nil {
		capData["cap_description"] = *c.CapDescription
	}

	if c.Documentation != nil {
		capData["documentation"] = *c.Documentation
	}

	if len(c.Metadata) > 0 {
		capData["metadata"] = c.Metadata
	}

	if len(c.MediaSpecs) > 0 {
		capData["media_specs"] = c.MediaSpecs
	}

	if len(c.Args) > 0 {
		capData["args"] = c.Args
	}

	if c.Output != nil {
		capData["output"] = c.Output
	}

	if c.MetadataJSON != nil {
		capData["metadata_json"] = c.MetadataJSON
	}

	if c.RegisteredBy != nil {
		capData["registered_by"] = c.RegisteredBy
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

	// Handle required fields
	if title, ok := raw["title"].(string); ok {
		c.Title = title
	} else {
		return fmt.Errorf("missing required field 'title'")
	}

	if command, ok := raw["command"].(string); ok {
		c.Command = command
	} else {
		return fmt.Errorf("missing required field 'command'")
	}

	if desc, ok := raw["cap_description"].(string); ok {
		c.CapDescription = &desc
	}

	if doc, ok := raw["documentation"].(string); ok {
		c.Documentation = &doc
	}

	if metadata, ok := raw["metadata"].(map[string]any); ok {
		c.Metadata = make(map[string]string)
		for k, v := range metadata {
			if s, ok := v.(string); ok {
				c.Metadata[k] = s
			}
		}
	}

	// Handle media_specs (array format)
	if mediaSpecsRaw, ok := raw["media_specs"]; ok {
		mediaSpecsBytes, _ := json.Marshal(mediaSpecsRaw)
		var mediaSpecs []media.MediaSpecDef
		if err := json.Unmarshal(mediaSpecsBytes, &mediaSpecs); err != nil {
			return fmt.Errorf("failed to unmarshal media_specs: %w", err)
		}
		c.MediaSpecs = mediaSpecs
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

	// Handle output
	if output, ok := raw["output"]; ok {
		outputBytes, _ := json.Marshal(output)
		var capOutput CapOutput
		if err := json.Unmarshal(outputBytes, &capOutput); err != nil {
			return fmt.Errorf("failed to unmarshal output: %w", err)
		}
		c.Output = &capOutput
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

	return nil
}
