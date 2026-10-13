package cap

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/xeipuuv/gojsonschema"
)

// ValidationError represents validation errors with descriptive failure information
type ValidationError struct {
	Type         string
	CapUrn       string
	ArgumentName string
	ExpectedType string
	ActualType   string
	ActualValue  interface{}
	Rule         string
	Message      string
}

func (e *ValidationError) Error() string {
	return e.Message
}

// NewUnknownCapError creates an error for unknown caps
func NewUnknownCapError(capUrn string) *ValidationError {
	return &ValidationError{
		Type:    "UnknownCap",
		CapUrn:  capUrn,
		Message: fmt.Sprintf("Unknown cap '%s' - cap not registered or advertised", capUrn),
	}
}

// NewMissingRequiredArgumentError creates an error for missing required arguments
func NewMissingRequiredArgumentError(capUrn, argumentName string) *ValidationError {
	return &ValidationError{
		Type:         "MissingRequiredArgument",
		CapUrn:       capUrn,
		ArgumentName: argumentName,
		Message:      fmt.Sprintf("Cap '%s' requires argument '%s' but it was not provided", capUrn, argumentName),
	}
}

// NewUnknownArgumentError creates an error for unknown arguments
func NewUnknownArgumentError(capUrn, argumentName string) *ValidationError {
	return &ValidationError{
		Type:         "UnknownArgument",
		CapUrn:       capUrn,
		ArgumentName: argumentName,
		Message:      fmt.Sprintf("Cap '%s' does not accept argument '%s' - check capability definition for valid arguments", capUrn, argumentName),
	}
}

// NewInvalidArgumentTypeErrorFromMediaUrn creates an error for invalid argument types using media URNs
func NewInvalidArgumentTypeErrorFromMediaUrn(capUrn, argumentName, mediaUrn, expectedType, actualType string, actualValue interface{}) *ValidationError {
	return &ValidationError{
		Type:         "InvalidArgumentType",
		CapUrn:       capUrn,
		ArgumentName: argumentName,
		ExpectedType: expectedType,
		ActualType:   actualType,
		ActualValue:  actualValue,
		Message:      fmt.Sprintf("Cap '%s' argument '%s' (media URN: %s) expects type '%s' but received '%s' with value: %v", capUrn, argumentName, mediaUrn, expectedType, actualType, actualValue),
	}
}

// NewInvalidArgumentTypeSchemaError creates an InvalidArgumentType error from
// schema validation failures (local schema or profile). Mirrors Rust's
// ValidationError::InvalidArgumentType { schema_errors }.
func NewInvalidArgumentTypeSchemaError(capUrn, mediaUrn string, actualValue interface{}, schemaErrors []string) *ValidationError {
	return &ValidationError{
		Type:         "InvalidArgumentType",
		CapUrn:       capUrn,
		ArgumentName: mediaUrn,
		ExpectedType: mediaUrn,
		ActualValue:  actualValue,
		Message: fmt.Sprintf("Cap '%s' argument '%s' failed schema validation: %s with value: %v",
			capUrn, mediaUrn, strings.Join(schemaErrors, "; "), actualValue),
	}
}

// NewInvalidOutputTypeSchemaError creates an InvalidOutputType error from schema
// validation failures. Mirrors Rust's ValidationError::InvalidOutputType { schema_errors }.
func NewInvalidOutputTypeSchemaError(capUrn, mediaUrn string, actualValue interface{}, schemaErrors []string) *ValidationError {
	return &ValidationError{
		Type:         "InvalidOutputType",
		CapUrn:       capUrn,
		ExpectedType: mediaUrn,
		ActualValue:  actualValue,
		Message: fmt.Sprintf("Cap '%s' output '%s' failed schema validation: %s with value: %v",
			capUrn, mediaUrn, strings.Join(schemaErrors, "; "), actualValue),
	}
}

// SchemaViolations compiles an inline JSON Schema and validates a value
// against it, returning the list of error strings (empty when valid). This is
// the one reading of a media definition's schema: what an argument value and
// an output are held to at run time, and what a constant written in a machine
// is held to when the machine is resolved.
func SchemaViolations(schema interface{}, value interface{}) []string {
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return []string{fmt.Sprintf("Failed to serialize schema: %v", err)}
	}
	compiled, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(schemaJSON))
	if err != nil {
		return []string{fmt.Sprintf("Failed to compile schema: %v", err)}
	}
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return []string{fmt.Sprintf("Failed to serialize value: %v", err)}
	}
	result, err := compiled.Validate(gojsonschema.NewBytesLoader(valueJSON))
	if err != nil {
		return []string{fmt.Sprintf("Validation error: %v", err)}
	}
	if result.Valid() {
		return nil
	}
	errs := make([]string, 0, len(result.Errors()))
	for _, e := range result.Errors() {
		errs = append(errs, fmt.Sprintf("%s: %s", e.Field(), e.String()))
	}
	return errs
}

// NewUnresolvableMediaUrnErrorForValidation creates an error for unresolvable media URNs in validation
func NewUnresolvableMediaUrnErrorForValidation(capUrn, argumentName, mediaUrn string) *ValidationError {
	return &ValidationError{
		Type:         "UnresolvableMediaUrn",
		CapUrn:       capUrn,
		ArgumentName: argumentName,
		Message:      fmt.Sprintf("Cap '%s' argument '%s' has unresolvable media URN '%s' - not found in registry", capUrn, argumentName, mediaUrn),
	}
}

// NewMediaValidationFailedError creates an error for media validation failures
func NewMediaValidationFailedError(capUrn, argumentName, rule string, actualValue interface{}) *ValidationError {
	return &ValidationError{
		Type:         "MediaValidationFailed",
		CapUrn:       capUrn,
		ArgumentName: argumentName,
		Rule:         rule,
		ActualValue:  actualValue,
		Message:      fmt.Sprintf("Cap '%s' argument '%s' failed validation rule '%s' with value: %v", capUrn, argumentName, rule, actualValue),
	}
}

// NewMediaDefValidationFailedError creates an error for media def validation failures (inherent to semantic type)
func NewMediaDefValidationFailedError(capUrn, argumentName, mediaUrn, rule string, actualValue interface{}) *ValidationError {
	return &ValidationError{
		Type:         "MediaDefValidationFailed",
		CapUrn:       capUrn,
		ArgumentName: argumentName,
		Rule:         rule,
		ActualValue:  actualValue,
		Message:      fmt.Sprintf("Cap '%s' argument '%s' failed media def '%s' validation rule '%s' with value: %v", capUrn, argumentName, mediaUrn, rule, actualValue),
	}
}

// NewInvalidOutputTypeErrorFromMediaUrn creates an error for invalid output types using media URNs
func NewInvalidOutputTypeErrorFromMediaUrn(capUrn, mediaUrn, expectedType, actualType string, actualValue interface{}) *ValidationError {
	return &ValidationError{
		Type:         "InvalidOutputType",
		CapUrn:       capUrn,
		ExpectedType: expectedType,
		ActualType:   actualType,
		ActualValue:  actualValue,
		Message:      fmt.Sprintf("Cap '%s' output (media URN: %s) expects type '%s' but received '%s' with value: %v", capUrn, mediaUrn, expectedType, actualType, actualValue),
	}
}

// NewOutputValidationFailedError creates an error for output validation failures
func NewOutputValidationFailedError(capUrn, rule string, actualValue interface{}) *ValidationError {
	return &ValidationError{
		Type:        "OutputValidationFailed",
		CapUrn:      capUrn,
		Rule:        rule,
		ActualValue: actualValue,
		Message:     fmt.Sprintf("Cap '%s' output failed validation rule '%s' with value: %v", capUrn, rule, actualValue),
	}
}

// NewOutputMediaDefValidationFailedError creates an error for output media def validation failures
func NewOutputMediaDefValidationFailedError(capUrn, mediaUrn, rule string, actualValue interface{}) *ValidationError {
	return &ValidationError{
		Type:        "OutputMediaDefValidationFailed",
		CapUrn:      capUrn,
		Rule:        rule,
		ActualValue: actualValue,
		Message:     fmt.Sprintf("Cap '%s' output failed media def '%s' validation rule '%s' with value: %v", capUrn, mediaUrn, rule, actualValue),
	}
}

// NewSchemaValidationFailedError creates an error for schema validation failures
func NewSchemaValidationFailedError(capUrn, argumentName, details string, actualValue interface{}) *ValidationError {
	return &ValidationError{
		Type:         "SchemaValidationFailed",
		CapUrn:       capUrn,
		ArgumentName: argumentName,
		ActualValue:  actualValue,
		Message:      fmt.Sprintf("Cap '%s' argument '%s' failed schema validation: %s", capUrn, argumentName, details),
	}
}

// InputValidator validates arguments against cap input schemas.
// It mirrors Rust's InputValidator, holding a ProfileSchemaRegistry for
// profile-URI-based JSON Schema validation.
type InputValidator struct {
	schemaValidator *SchemaValidator
	schemaRegistry  *media.ProfileSchemaRegistry
}

// newProfileRegistryOrPanic constructs a ProfileSchemaRegistry, mirroring how
// Rust's call sites obtain one via ProfileSchemaRegistry::new(). Construction
// only fails if the cache directory cannot be created, which is an environment
// fault the reference also surfaces by failing hard.
func newProfileRegistryOrPanic() *media.ProfileSchemaRegistry {
	registry, err := media.NewProfileSchemaRegistry()
	if err != nil {
		panic(fmt.Sprintf("failed to create profile schema registry: %v", err))
	}
	return registry
}

// NewInputValidator creates a new input validator with a default profile registry.
func NewInputValidator() *InputValidator {
	return NewInputValidatorWithRegistry(newProfileRegistryOrPanic())
}

// NewInputValidatorWithRegistry creates a new input validator with the given
// profile schema registry (mirrors Rust's InputValidator::new schema_registry param).
func NewInputValidatorWithRegistry(schemaRegistry *media.ProfileSchemaRegistry) *InputValidator {
	return &InputValidator{
		schemaValidator: NewSchemaValidator(),
		schemaRegistry:  schemaRegistry,
	}
}

// NewInputValidatorWithSchemaResolver creates a new input validator with schema resolver
func NewInputValidatorWithSchemaResolver(resolver SchemaResolver) *InputValidator {
	return &InputValidator{
		schemaValidator: NewSchemaValidatorWithResolver(resolver),
		schemaRegistry:  newProfileRegistryOrPanic(),
	}
}

// ValidateArguments validates arguments against a cap's input schema
func (iv *InputValidator) ValidateArguments(cap *Cap, arguments []interface{}, registry media.MediaDefSource) error {
	capUrn := cap.UrnString()
	args := cap.GetArgs()

	// Check if too many arguments provided
	if len(arguments) > len(args) {
		return &ValidationError{
			Type:    "TooManyArguments",
			CapUrn:  capUrn,
			Message: fmt.Sprintf("Cap '%s' expects at most %d arguments but received %d", capUrn, len(args), len(arguments)),
		}
	}

	// Get required and optional args
	requiredArgs := cap.GetRequiredArgs()
	optionalArgs := cap.GetOptionalArgs()

	// Validate required arguments
	for index, reqArg := range requiredArgs {
		if index >= len(arguments) {
			return NewMissingRequiredArgumentError(capUrn, reqArg.MediaUrn)
		}

		if err := iv.validateSingleArgument(cap, &reqArg, arguments[index], registry); err != nil {
			return err
		}
	}

	// Validate optional arguments if provided
	requiredCount := len(requiredArgs)
	for index, optArg := range optionalArgs {
		argIndex := requiredCount + index
		if argIndex < len(arguments) {
			if err := iv.validateSingleArgument(cap, &optArg, arguments[argIndex], registry); err != nil {
				return err
			}
		}
	}

	return nil
}

// ValidateNamedArguments validates named arguments against a cap's input schema
func (iv *InputValidator) ValidateNamedArguments(cap *Cap, namedArgs []map[string]interface{}, registry media.MediaDefSource) error {
	capUrn := cap.UrnString()
	args := cap.GetArgs()

	// Extract named argument values into a map (using media_urn as key)
	providedArgs := make(map[string]interface{})
	for _, arg := range namedArgs {
		if name, hasName := arg["media_urn"].(string); hasName {
			if value, hasValue := arg["value"]; hasValue {
				providedArgs[name] = value
			}
		}
	}

	// Check that all required arguments are provided as named arguments
	requiredArgs := cap.GetRequiredArgs()
	for _, reqArg := range requiredArgs {
		if _, provided := providedArgs[reqArg.MediaUrn]; !provided {
			return NewMissingRequiredArgumentError(capUrn, fmt.Sprintf("%s (expected as named argument)", reqArg.MediaUrn))
		}

		// Validate the provided argument value
		providedValue := providedArgs[reqArg.MediaUrn]
		if err := iv.validateSingleArgument(cap, &reqArg, providedValue, registry); err != nil {
			return err
		}
	}

	// Validate optional arguments if provided
	optionalArgs := cap.GetOptionalArgs()
	for _, optArg := range optionalArgs {
		if providedValue, provided := providedArgs[optArg.MediaUrn]; provided {
			if err := iv.validateSingleArgument(cap, &optArg, providedValue, registry); err != nil {
				return err
			}
		}
	}

	// Check for unknown arguments
	knownArgUrns := make(map[string]bool)
	for _, arg := range args {
		knownArgUrns[arg.MediaUrn] = true
	}

	for providedUrn := range providedArgs {
		if !knownArgUrns[providedUrn] {
			return NewUnknownArgumentError(capUrn, providedUrn)
		}
	}

	return nil
}

func (iv *InputValidator) validateSingleArgument(cap *Cap, argDef *CapArg, value interface{}, registry media.MediaDefSource) error {
	// Resolve the media URN. Mirrors Rust's validate_argument_type:
	// resolve -> local-schema-if-present -> else profile-uri-if-present -> rules.
	resolved, err := argDef.Resolve(registry)
	if err != nil {
		return NewUnresolvableMediaUrnErrorForValidation(cap.UrnString(), argDef.MediaUrn, argDef.MediaUrn)
	}

	// Schema/profile type validation.
	if err := iv.validateArgumentType(cap, argDef, resolved, value); err != nil {
		return err
	}

	// Media def validation rules (inherent to the semantic type).
	if resolved.Validation != nil {
		if err := iv.validateMediaDefRules(cap, argDef, resolved, value); err != nil {
			return err
		}
	}

	return nil
}

// validateMediaDefRules validates value against media def's inherent validation rules (first pass)
func (iv *InputValidator) validateMediaDefRules(cap *Cap, argDef *CapArg, resolved *media.ResolvedMediaDef, value interface{}) error {
	capUrn := cap.UrnString()
	mediaUrn := resolved.SpecID
	rule, err := resolved.Validation.Violation(value)
	if err != nil {
		return &ValidationError{
			Type:    "InvalidCapSchema",
			CapUrn:  capUrn,
			Message: fmt.Sprintf("%v in media def '%s'", err, mediaUrn),
		}
	}
	if rule != "" {
		return NewMediaDefValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, rule, value)
	}
	return nil
}

// validateArgumentType mirrors Rust's validate_argument_type: it validates the
// value against the resolved media def's local schema if present, otherwise
// against its profile URI via the ProfileSchemaRegistry. A media def with
// neither a local schema nor a profile URI accepts any JSON value. There is no
// type-string inference.
func (iv *InputValidator) validateArgumentType(cap *Cap, argDef *CapArg, resolved *media.ResolvedMediaDef, value interface{}) error {
	capUrn := cap.UrnString()

	// First, try the local schema from the resolved spec.
	if resolved.Schema != nil {
		if errs := SchemaViolations(resolved.Schema, value); len(errs) > 0 {
			return NewInvalidArgumentTypeSchemaError(capUrn, argDef.MediaUrn, value, errs)
		}
		return nil
	}

	// Otherwise validate against the profile schema via the registry.
	if resolved.ProfileURI != "" {
		if errs := iv.schemaRegistry.Validate(resolved.ProfileURI, value); len(errs) > 0 {
			return NewInvalidArgumentTypeSchemaError(capUrn, argDef.MediaUrn, value, errs)
		}
	}

	// No profile or schema means any JSON value is valid for that media type.
	return nil
}

// OutputValidator validates output against cap output schemas.
// Mirrors Rust's OutputValidator, holding a ProfileSchemaRegistry.
type OutputValidator struct {
	schemaValidator *SchemaValidator
	schemaRegistry  *media.ProfileSchemaRegistry
}

// NewOutputValidator creates a new output validator with a default profile registry.
func NewOutputValidator() *OutputValidator {
	return NewOutputValidatorWithRegistry(newProfileRegistryOrPanic())
}

// NewOutputValidatorWithRegistry creates a new output validator with the given
// profile schema registry (mirrors Rust's OutputValidator::new schema_registry param).
func NewOutputValidatorWithRegistry(schemaRegistry *media.ProfileSchemaRegistry) *OutputValidator {
	return &OutputValidator{
		schemaValidator: NewSchemaValidator(),
		schemaRegistry:  schemaRegistry,
	}
}

// NewOutputValidatorWithSchemaResolver creates a new output validator with schema resolver
func NewOutputValidatorWithSchemaResolver(resolver SchemaResolver) *OutputValidator {
	return &OutputValidator{
		schemaValidator: NewSchemaValidatorWithResolver(resolver),
		schemaRegistry:  newProfileRegistryOrPanic(),
	}
}

// ValidateOutput validates output against a cap's output schema
// Two-pass validation:
// 1. Type validation + media def validation rules (inherent to semantic type)
// 2. Output-level validation rules (context-specific)
func (ov *OutputValidator) ValidateOutput(cap *Cap, output interface{}, registry media.MediaDefSource) error {
	capUrn := cap.UrnString()

	// What a cap's response is validated as is its main output.
	outputDef := cap.MainOutput()
	if outputDef == nil {
		return &ValidationError{
			Type:    "InvalidCapSchema",
			CapUrn:  capUrn,
			Message: fmt.Sprintf("Cap '%s' has no output definition specified", capUrn),
		}
	}

	// Resolve the media URN
	resolved, err := outputDef.Resolve(registry)
	if err != nil {
		return &ValidationError{
			Type:    "UnresolvableMediaUrn",
			CapUrn:  capUrn,
			Message: fmt.Sprintf("Cap '%s' output has unresolvable media URN '%s'", capUrn, outputDef.MediaUrn),
		}
	}

	// Type validation (local schema or profile via registry)
	if err := ov.validateOutputType(cap, outputDef, resolved, output); err != nil {
		return err
	}

	// Media def validation rules (inherent to the semantic type)
	if resolved.Validation != nil {
		if err := ov.validateOutputMediaDefRules(cap, resolved, output); err != nil {
			return err
		}
	}

	return nil
}

// validateOutputMediaDefRules validates output against media def's inherent validation rules (first pass)
func (ov *OutputValidator) validateOutputMediaDefRules(cap *Cap, resolved *media.ResolvedMediaDef, value interface{}) error {
	capUrn := cap.UrnString()
	mediaUrn := resolved.SpecID
	rule, err := resolved.Validation.Violation(value)
	if err != nil {
		return &ValidationError{
			Type:    "InvalidCapSchema",
			CapUrn:  capUrn,
			Message: fmt.Sprintf("%v in media def '%s'", err, mediaUrn),
		}
	}
	if rule != "" {
		return NewOutputMediaDefValidationFailedError(capUrn, mediaUrn, rule, value)
	}
	return nil
}

// validateOutputType mirrors Rust's validate_output_type: local schema if
// present, otherwise profile URI via the ProfileSchemaRegistry; a media def with
// neither accepts any JSON value.
func (ov *OutputValidator) validateOutputType(cap *Cap, outputDef *CapOutput, resolved *media.ResolvedMediaDef, value interface{}) error {
	capUrn := cap.UrnString()

	if resolved.Schema != nil {
		if errs := SchemaViolations(resolved.Schema, value); len(errs) > 0 {
			return NewInvalidOutputTypeSchemaError(capUrn, outputDef.MediaUrn, value, errs)
		}
		return nil
	}

	if resolved.ProfileURI != "" {
		if errs := ov.schemaRegistry.Validate(resolved.ProfileURI, value); len(errs) > 0 {
			return NewInvalidOutputTypeSchemaError(capUrn, outputDef.MediaUrn, value, errs)
		}
	}

	return nil
}

// CapValidationCoordinator provides centralized validation coordination
type CapValidationCoordinator struct {
	caps            map[string]*Cap
	inputValidator  *InputValidator
	outputValidator *OutputValidator
}

// NewCapValidationCoordinator creates a new validation coordinator. Both the
// input and output validators share a single profile schema registry, mirroring
// Rust where one Arc<ProfileSchemaRegistry> is threaded into both.
func NewCapValidationCoordinator() *CapValidationCoordinator {
	registry := newProfileRegistryOrPanic()
	return &CapValidationCoordinator{
		caps:            make(map[string]*Cap),
		inputValidator:  NewInputValidatorWithRegistry(registry),
		outputValidator: NewOutputValidatorWithRegistry(registry),
	}
}

// NewCapValidationCoordinatorWithSchemaResolver creates a coordinator with schema resolver
func NewCapValidationCoordinatorWithSchemaResolver(resolver SchemaResolver) *CapValidationCoordinator {
	return &CapValidationCoordinator{
		caps:            make(map[string]*Cap),
		inputValidator:  NewInputValidatorWithSchemaResolver(resolver),
		outputValidator: NewOutputValidatorWithSchemaResolver(resolver),
	}
}

// RegisterCap registers a cap schema for validation
func (cvc *CapValidationCoordinator) RegisterCap(cap *Cap) {
	cvc.caps[cap.UrnString()] = cap
}

// GetCap gets a cap by ID
func (cvc *CapValidationCoordinator) GetCap(capUrn string) *Cap {
	return cvc.caps[capUrn]
}

// ValidateInputs validates arguments against a cap's input schema
func (cvc *CapValidationCoordinator) ValidateInputs(capUrn string, arguments []interface{}, registry media.MediaDefSource) error {
	cap := cvc.GetCap(capUrn)
	if cap == nil {
		return NewUnknownCapError(capUrn)
	}

	return cvc.inputValidator.ValidateArguments(cap, arguments, registry)
}

// ValidateOutput validates output against a cap's output schema
func (cvc *CapValidationCoordinator) ValidateOutput(capUrn string, output interface{}, registry media.MediaDefSource) error {
	cap := cvc.GetCap(capUrn)
	if cap == nil {
		return NewUnknownCapError(capUrn)
	}

	return cvc.outputValidator.ValidateOutput(cap, output, registry)
}

// ValidateCapSchema validates a cap definition itself
func (cvc *CapValidationCoordinator) ValidateCapSchema(cap *Cap, registry media.MediaDefSource) error {
	capUrn := cap.UrnString()
	args := cap.GetArgs()

	if len(args) == 0 {
		// Validate output media URN if present
		for i := range cap.Outputs {
			if _, err := cap.Outputs[i].Resolve(registry); err != nil {
				return &ValidationError{
					Type:    "InvalidCapSchema",
					CapUrn:  capUrn,
					Message: fmt.Sprintf("Cap '%s' output has unresolvable media URN '%s'", capUrn, cap.Outputs[i].MediaUrn),
				}
			}
		}
		return nil
	}

	// Validate that required arguments don't have default values
	for _, arg := range args {
		if arg.Required && arg.DefaultValue != nil {
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("Cap '%s' required argument '%s' cannot have a default value", capUrn, arg.MediaUrn),
			}
		}
	}

	// Validate that all argument media URNs can be resolved
	for _, arg := range args {
		if _, err := arg.Resolve(registry); err != nil {
			argType := "optional"
			if arg.Required {
				argType = "required"
			}
			return &ValidationError{
				Type:         "InvalidCapSchema",
				CapUrn:       capUrn,
				ArgumentName: arg.MediaUrn,
				Message:      fmt.Sprintf("Cap '%s' %s argument '%s' has unresolvable media URN", capUrn, argType, arg.MediaUrn),
			}
		}
	}

	// Validate every output's media URN
	for i := range cap.Outputs {
		if _, err := cap.Outputs[i].Resolve(registry); err != nil {
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("Cap '%s' output has unresolvable media URN '%s'", capUrn, cap.Outputs[i].MediaUrn),
			}
		}
	}

	// Validate argument position uniqueness
	positions := make(map[int]string)
	for _, arg := range args {
		pos := arg.GetPosition()
		if pos != nil {
			if existing, exists := positions[*pos]; exists {
				return &ValidationError{
					Type:    "InvalidCapSchema",
					CapUrn:  capUrn,
					Message: fmt.Sprintf("Cap '%s' duplicate argument position %d for arguments '%s' and '%s'", capUrn, *pos, existing, arg.MediaUrn),
				}
			}
			positions[*pos] = arg.MediaUrn
		}
	}

	// Validate CLI flag uniqueness
	cliFlags := make(map[string]string)
	for _, arg := range args {
		cliFlag := arg.GetCliFlag()
		if cliFlag != nil && *cliFlag != "" {
			if existing, exists := cliFlags[*cliFlag]; exists {
				return &ValidationError{
					Type:    "InvalidCapSchema",
					CapUrn:  capUrn,
					Message: fmt.Sprintf("Cap '%s' duplicate CLI flag '%s' for arguments '%s' and '%s'", capUrn, *cliFlag, existing, arg.MediaUrn),
				}
			}
			cliFlags[*cliFlag] = arg.MediaUrn
		}
	}

	return nil
}

// ReservedCliFlags are CLI flags that cannot be used as cap argument flags
var ReservedCliFlags = []string{"manifest", "--help", "--version", "-v", "-h"}

// IsValidDefinitionName reports whether `name` can name an argument or an
// output (RULE17, RULE19): the lexical form of a notation alias, and none of
// the words notation reads as constants.
// Matches Rust: pub fn is_valid_definition_name(name: &str) -> bool
func IsValidDefinitionName(name string) bool {
	if name == "" || name == "true" || name == "false" || name == "null" {
		return false
	}
	for i, c := range name {
		letter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		digit := c >= '0' && c <= '9'
		if i == 0 {
			if !letter && c != '_' {
				return false
			}
			continue
		}
		if !letter && !digit && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// ValidateCapDefinition holds a whole definition to the definition rules
// (RULE1-RULE22): its arguments, then its outputs. A definition is held to
// them wherever it crosses from one program to another.
// Matches Rust: pub fn validate_cap_definition(cap: &Cap)
func ValidateCapDefinition(cap *Cap) error {
	if err := ValidateCapArgs(cap); err != nil {
		return err
	}
	return ValidateCapOutputs(cap)
}

// ValidateCapOutputs enforces the rules on a cap's outputs.
//
//	RULE19: Output names: required, well-formed, unique
//	RULE20: No duplicate output media_urns
//	RULE21: Exactly one main output, matching out= (none when out= is media:void)
//	RULE22: What an output carries
//
// Matches Rust: pub fn validate_cap_outputs(cap: &Cap)
func ValidateCapOutputs(cap *Cap) error {
	return namingTheCap(capOutputRuleBroken(cap))
}

// namingTheCap renders a broken definition rule as the reference does: the
// cap it was found in, then the rule. A refusal that does not say which cap
// broke the rule sends its reader to every definition in the manifest.
// Matches Rust: "Cap '{cap_urn}' has invalid schema: {issue}"
func namingTheCap(broken *ValidationError) error {
	if broken == nil {
		return nil
	}
	broken.Message = fmt.Sprintf("Cap '%s' has invalid schema: %s", broken.CapUrn, broken.Message)
	return broken
}

// capOutputRuleBroken returns the first output rule `cap` breaks, as the bare
// rule text, or nil.
func capOutputRuleBroken(cap *Cap) *ValidationError {
	capUrn := cap.UrnString()
	schema := func(message string) *ValidationError {
		return &ValidationError{Type: "InvalidCapSchema", CapUrn: capUrn, Message: message}
	}
	voidUrn, err := urn.NewMediaUrnFromString(media.MediaVoid)
	if err != nil {
		panic("media.MediaVoid is a valid MediaUrn: " + err.Error())
	}
	if cap.Urn == nil {
		return schema("RULE21: Failed to parse out= spec: the cap has no URN")
	}
	outMedia, err := cap.Urn.OutMediaUrn()
	if err != nil {
		return schema(fmt.Sprintf("RULE21: Failed to parse out= spec: %v", err))
	}
	inMedia, err := cap.Urn.InMediaUrn()
	if err != nil {
		return schema(fmt.Sprintf("RULE22: Failed to parse in= spec: %v", err))
	}
	outIsVoid := outMedia.IsEquivalent(voidUrn)
	inIsVoid := inMedia.IsEquivalent(voidUrn)

	// RULE19: names
	type namedUrn struct{ name, mediaUrn string }
	var names []namedUrn
	for i := range cap.Outputs {
		output := &cap.Outputs[i]
		if output.Name == "" {
			return schema(fmt.Sprintf("RULE19: Output '%s' has no name", output.MediaUrn))
		}
		if !IsValidDefinitionName(output.Name) {
			return schema(fmt.Sprintf("RULE19: Output name '%s' is not a valid name", output.Name))
		}
		for _, seen := range names {
			if seen.name == output.Name {
				return schema(fmt.Sprintf("RULE19: Outputs '%s' and '%s' share the name '%s'", seen.mediaUrn, output.MediaUrn, output.Name))
			}
		}
		names = append(names, namedUrn{output.Name, output.MediaUrn})
	}

	// RULE20: identities, compared as media URNs (tag order is not identity)
	identities := make([]*urn.MediaUrn, 0, len(cap.Outputs))
	for i := range cap.Outputs {
		output := &cap.Outputs[i]
		outputUrn, err := urn.NewMediaUrnFromString(output.MediaUrn)
		if err != nil {
			return schema(fmt.Sprintf("RULE20: Output media_urn '%s' is not a media URN: %v", output.MediaUrn, err))
		}
		for _, seen := range identities {
			if seen.IsEquivalent(outputUrn) {
				return schema(fmt.Sprintf("RULE20: Duplicate output media_urn '%s'", output.MediaUrn))
			}
		}
		identities = append(identities, outputUrn)
	}

	// RULE21: the main output
	if outIsVoid {
		if len(cap.Outputs) > 0 {
			return schema(fmt.Sprintf("RULE21: out= is 'media:void' but %d output(s) are declared", len(cap.Outputs)))
		}
		return nil
	}
	var main []int
	for i, identity := range identities {
		if identity.IsEquivalent(outMedia) {
			main = append(main, i)
		}
	}
	if len(main) != 1 {
		return schema(fmt.Sprintf("RULE21: out= is '%s' but %d outputs are equivalent to it; exactly one must be", cap.Urn.OutSpec(), len(main)))
	}

	// RULE22: what an output carries
	for i := range cap.Outputs {
		output := &cap.Outputs[i]
		if output.Carries == nil {
			continue
		}
		if i == main[0] {
			return schema(fmt.Sprintf("RULE22: The main output '%s' declares carries", output.MediaUrn))
		}
		if inIsVoid {
			return schema(fmt.Sprintf("RULE22: Output '%s' carries the input but the cap takes none", output.MediaUrn))
		}
	}
	return nil
}

// ValidateCapArgs enforces structural rules on a cap's argument definitions.
// This is a standalone function matching Rust's validate_cap_args().
// Rules:
//
//	RULE1: No duplicate media_urns across args
//	RULE2: Sources must not be empty
//	RULE3: At most one arg declares a stdin source
//	RULE4: No arg may specify same source type more than once
//	RULE5: No two args may have same position
//	RULE6: Positions must be sequential (0-based, no gaps)
//	RULE7: No arg may have both position and cli_flag
//	RULE9: No two args may have same cli_flag
//	RULE10: Reserved cli_flags rejected
//	RULE14: Only the main input argument may declare streaming=true
//	RULE15: The main input matches in= (void input declares no stdin; any other declares one, equivalent to in=)
//	RULE17: Argument names: required, well-formed, unique
//	RULE18: `accepts` only without a stdin source
//
// Every rule is raised under the number the reference raises it under
// (capdag docs/10-validation-rules.md): a number names one rule.
func ValidateCapArgs(cap *Cap) error {
	return namingTheCap(capArgRuleBroken(cap))
}

// capArgRuleBroken returns the first argument rule `cap` breaks, as the bare
// rule text, or nil.
func capArgRuleBroken(cap *Cap) *ValidationError {
	capUrn := cap.UrnString()
	args := cap.GetArgs()

	// RULE1: No duplicate media_urns
	mediaUrns := make(map[string]bool)
	for _, arg := range args {
		if mediaUrns[arg.MediaUrn] {
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("RULE1: Duplicate media_urn '%s'", arg.MediaUrn),
			}
		}
		mediaUrns[arg.MediaUrn] = true
	}

	// RULE2: sources must not be empty
	for _, arg := range args {
		if len(arg.Sources) == 0 {
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("RULE2: Argument '%s' has empty sources", arg.MediaUrn),
			}
		}
	}

	// Collect cross-arg data
	var stdinUrns []string
	type posEntry struct {
		pos      int
		mediaUrn string
	}
	var positions []posEntry
	type flagEntry struct {
		flag     string
		mediaUrn string
	}
	var cliFlags []flagEntry

	for _, arg := range args {
		sourceTypes := make(map[string]bool)
		hasPosition := false
		hasCliFlag := false

		for _, source := range arg.Sources {
			sourceType := source.GetType()

			// RULE4: No arg may specify same source type more than once
			if sourceTypes[sourceType] {
				return &ValidationError{
					Type:    "InvalidCapSchema",
					CapUrn:  capUrn,
					Message: fmt.Sprintf("RULE4: Argument '%s' has duplicate source type '%s'", arg.MediaUrn, sourceType),
				}
			}
			sourceTypes[sourceType] = true

			if source.Stdin != nil {
				stdinUrns = append(stdinUrns, *source.Stdin)
			}
			if source.Position != nil {
				hasPosition = true
				positions = append(positions, posEntry{pos: *source.Position, mediaUrn: arg.MediaUrn})
			}
			if source.CliFlag != nil {
				hasCliFlag = true
				flag := *source.CliFlag
				cliFlags = append(cliFlags, flagEntry{flag: flag, mediaUrn: arg.MediaUrn})

				// RULE10: Reserved cli_flags
				for _, reserved := range ReservedCliFlags {
					if flag == reserved {
						return &ValidationError{
							Type:    "InvalidCapSchema",
							CapUrn:  capUrn,
							Message: fmt.Sprintf("RULE10: Argument '%s' uses reserved cli_flag '%s'", arg.MediaUrn, flag),
						}
					}
				}
			}
		}

		// RULE7: No arg may have both position and cli_flag
		if hasPosition && hasCliFlag {
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("RULE7: Argument '%s' has both position and cli_flag sources", arg.MediaUrn),
			}
		}
	}

	// RULE3: At most one arg declares a stdin source. `stdin` is a command
	// line's one piped input, and the arg that declares it is the main input.
	var stdinArgs []string
	for i := range args {
		if args[i].HasStdinSource() {
			stdinArgs = append(stdinArgs, args[i].MediaUrn)
		}
	}
	if len(stdinArgs) > 1 {
		return &ValidationError{
			Type:    "InvalidCapSchema",
			CapUrn:  capUrn,
			Message: fmt.Sprintf("RULE3: Arguments '%s' and '%s' both declare a stdin source; a cap has one main input", stdinArgs[0], stdinArgs[1]),
		}
	}

	// RULE17: names
	type namedUrn struct{ name, mediaUrn string }
	var names []namedUrn
	for i := range args {
		arg := &args[i]
		if arg.Name == "" {
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("RULE17: Argument '%s' has no name", arg.MediaUrn),
			}
		}
		if !IsValidDefinitionName(arg.Name) {
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("RULE17: Argument name '%s' is not a valid name", arg.Name),
			}
		}
		for _, seen := range names {
			if seen.name == arg.Name {
				return &ValidationError{
					Type:    "InvalidCapSchema",
					CapUrn:  capUrn,
					Message: fmt.Sprintf("RULE17: Arguments '%s' and '%s' share the name '%s'", seen.mediaUrn, arg.MediaUrn, arg.Name),
				}
			}
		}
		names = append(names, namedUrn{arg.Name, arg.MediaUrn})
	}

	// RULE18: an arg says what it accepts in one place.
	for i := range args {
		if args[i].Accepts != nil && args[i].HasStdinSource() {
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("RULE18: Argument '%s' declares both a stdin source and accepts", args[i].MediaUrn),
			}
		}
	}

	// RULE15: The main input matches in=. If in= is media:void, no arg may
	// have a stdin source (the cap takes no data-flow input). Otherwise
	// exactly one does (RULE3 caps it at one), and its URN is in=.
	if cap.Urn == nil {
		return &ValidationError{
			Type:    "InvalidCapSchema",
			CapUrn:  capUrn,
			Message: "RULE15: Failed to parse in= spec: the cap has no URN",
		}
	}
	inMediaUrn, err := cap.Urn.InMediaUrn()
	if err != nil {
		return &ValidationError{
			Type:    "InvalidCapSchema",
			CapUrn:  capUrn,
			Message: fmt.Sprintf("RULE15: Failed to parse in= spec: %v", err),
		}
	}
	voidUrn, err := urn.NewMediaUrnFromString(media.MediaVoid)
	if err != nil {
		panic("media.MediaVoid is a valid MediaUrn: " + err.Error())
	}
	inIsVoid := inMediaUrn.IsEquivalent(voidUrn)
	if inIsVoid && len(stdinUrns) > 0 {
		return &ValidationError{
			Type:    "InvalidCapSchema",
			CapUrn:  capUrn,
			Message: fmt.Sprintf("RULE15: in= is 'media:void' but %d arg(s) declare a stdin source", len(stdinUrns)),
		}
	}
	if !inIsVoid {
		// The stdin arg IS the main input, so its URN is `in=` — compared as
		// tagged URNs, never as strings.
		matchesIn := false
		for _, stdin := range stdinUrns {
			stdinUrn, err := urn.NewMediaUrnFromString(stdin)
			if err == nil && stdinUrn.IsEquivalent(inMediaUrn) {
				matchesIn = true
				break
			}
		}
		if !matchesIn {
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("RULE15: in= is '%s' but no arg declares a stdin source equivalent to it", cap.Urn.InSpec()),
			}
		}
	}

	// RULE14: Only the main input may stream. `streaming: true` declares that
	// an argument is consumed without a length promise — a feed. Side
	// arguments (options, model specs, prompts alongside the main input) are
	// values the runtime demuxes whole; a feed there has no meaning and would
	// let an unbounded stream reach a collector that must refuse it (L16).
	for i := range cap.Args {
		if cap.Args[i].Streaming && !cap.Args[i].IsMainInput(inMediaUrn) {
			return &ValidationError{
				Type:   "InvalidCapSchema",
				CapUrn: capUrn,
				Message: fmt.Sprintf("RULE14: Argument '%s' declares streaming=true but is not the main input (no stdin source equivalent to in='%s') — only the main input may be consumed without a length promise",
					cap.Args[i].MediaUrn, cap.Urn.InSpec()),
			}
		}
	}

	// RULE5: No two args may have same position
	positionSet := make(map[int]string)
	for _, pe := range positions {
		if existing, exists := positionSet[pe.pos]; exists {
			_ = existing
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("RULE5: Duplicate position %d in argument '%s'", pe.pos, pe.mediaUrn),
			}
		}
		positionSet[pe.pos] = pe.mediaUrn
	}

	// RULE6: Positions must be sequential (0-based, no gaps)
	if len(positions) > 0 {
		sorted := make([]posEntry, len(positions))
		copy(sorted, positions)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].pos < sorted[j].pos })
		for i, pe := range sorted {
			if pe.pos != i {
				return &ValidationError{
					Type:    "InvalidCapSchema",
					CapUrn:  capUrn,
					Message: fmt.Sprintf("RULE6: Position gap - expected %d but found %d", i, pe.pos),
				}
			}
		}
	}

	// RULE9: No two args may have same cli_flag
	flagSet := make(map[string]string)
	for _, fe := range cliFlags {
		if existing, exists := flagSet[fe.flag]; exists {
			_ = existing
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("RULE9: Duplicate cli_flag '%s' in argument '%s'", fe.flag, fe.mediaUrn),
			}
		}
		flagSet[fe.flag] = fe.mediaUrn
	}

	return nil
}

// Utility functions

// ============================================================================
// XV5 VALIDATION - No Redefinition of Registry Media Defs
// ============================================================================
