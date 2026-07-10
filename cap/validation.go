package cap

import (
	"encoding/json"
	"fmt"
	"regexp"
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

// validateWithLocalSchema compiles an inline JSON Schema and validates a value
// against it, returning the list of error strings (empty when valid). Mirrors
// Rust's validate_with_local_schema.
func validateWithLocalSchema(schema interface{}, value interface{}) []string {
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
func (iv *InputValidator) ValidateArguments(cap *Cap, arguments []interface{}, registry *media.FabricRegistry) error {
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
func (iv *InputValidator) ValidateNamedArguments(cap *Cap, namedArgs []map[string]interface{}, registry *media.FabricRegistry) error {
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

func (iv *InputValidator) validateSingleArgument(cap *Cap, argDef *CapArg, value interface{}, registry *media.FabricRegistry) error {
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
	validation := resolved.Validation
	mediaUrn := resolved.SpecID

	// Numeric validation
	if validation.Min != nil {
		if num, ok := getNumericValue(value); ok {
			if num < *validation.Min {
				return NewMediaDefValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("minimum value %v", *validation.Min), value)
			}
		}
	}

	if validation.Max != nil {
		if num, ok := getNumericValue(value); ok {
			if num > *validation.Max {
				return NewMediaDefValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("maximum value %v", *validation.Max), value)
			}
		}
	}

	// String length validation
	if validation.MinLength != nil {
		if s, ok := value.(string); ok {
			if len(s) < *validation.MinLength {
				return NewMediaDefValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("minimum length %d", *validation.MinLength), value)
			}
		}
	}

	if validation.MaxLength != nil {
		if s, ok := value.(string); ok {
			if len(s) > *validation.MaxLength {
				return NewMediaDefValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("maximum length %d", *validation.MaxLength), value)
			}
		}
	}

	// Pattern validation
	if validation.Pattern != nil {
		if s, ok := value.(string); ok {
			regex, err := regexp.Compile(*validation.Pattern)
			if err != nil {
				return &ValidationError{
					Type:    "InvalidCapSchema",
					CapUrn:  capUrn,
					Message: fmt.Sprintf("Invalid regex pattern '%s' in media def '%s': %v", *validation.Pattern, mediaUrn, err),
				}
			}
			if !regex.MatchString(s) {
				return NewMediaDefValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("pattern '%s'", *validation.Pattern), value)
			}
		}
	}

	// Allowed values validation
	if len(validation.AllowedValues) > 0 {
		if s, ok := value.(string); ok {
			allowed := false
			for _, allowedValue := range validation.AllowedValues {
				if s == allowedValue {
					allowed = true
					break
				}
			}
			if !allowed {
				return NewMediaDefValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("allowed values: %v", validation.AllowedValues), value)
			}
		}
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
		if errs := validateWithLocalSchema(resolved.Schema, value); len(errs) > 0 {
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
func (ov *OutputValidator) ValidateOutput(cap *Cap, output interface{}, registry *media.FabricRegistry) error {
	capUrn := cap.UrnString()

	outputDef := cap.GetOutput()
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
	validation := resolved.Validation
	mediaUrn := resolved.SpecID

	// Numeric validation
	if validation.Min != nil {
		if num, ok := getNumericValue(value); ok {
			if num < *validation.Min {
				return NewOutputMediaDefValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("minimum value %v", *validation.Min), value)
			}
		}
	}

	if validation.Max != nil {
		if num, ok := getNumericValue(value); ok {
			if num > *validation.Max {
				return NewOutputMediaDefValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("maximum value %v", *validation.Max), value)
			}
		}
	}

	// String length validation
	if validation.MinLength != nil {
		if s, ok := value.(string); ok {
			if len(s) < *validation.MinLength {
				return NewOutputMediaDefValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("minimum length %d", *validation.MinLength), value)
			}
		}
	}

	if validation.MaxLength != nil {
		if s, ok := value.(string); ok {
			if len(s) > *validation.MaxLength {
				return NewOutputMediaDefValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("maximum length %d", *validation.MaxLength), value)
			}
		}
	}

	// Pattern validation
	if validation.Pattern != nil {
		if s, ok := value.(string); ok {
			regex, err := regexp.Compile(*validation.Pattern)
			if err != nil {
				return &ValidationError{
					Type:    "InvalidCapSchema",
					CapUrn:  capUrn,
					Message: fmt.Sprintf("Invalid regex pattern '%s' in media def '%s': %v", *validation.Pattern, mediaUrn, err),
				}
			}
			if !regex.MatchString(s) {
				return NewOutputMediaDefValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("pattern '%s'", *validation.Pattern), value)
			}
		}
	}

	// Allowed values validation
	if len(validation.AllowedValues) > 0 {
		if s, ok := value.(string); ok {
			allowed := false
			for _, allowedValue := range validation.AllowedValues {
				if s == allowedValue {
					allowed = true
					break
				}
			}
			if !allowed {
				return NewOutputMediaDefValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("allowed values: %v", validation.AllowedValues), value)
			}
		}
	}

	return nil
}

// validateOutputType mirrors Rust's validate_output_type: local schema if
// present, otherwise profile URI via the ProfileSchemaRegistry; a media def with
// neither accepts any JSON value.
func (ov *OutputValidator) validateOutputType(cap *Cap, outputDef *CapOutput, resolved *media.ResolvedMediaDef, value interface{}) error {
	capUrn := cap.UrnString()

	if resolved.Schema != nil {
		if errs := validateWithLocalSchema(resolved.Schema, value); len(errs) > 0 {
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
func (cvc *CapValidationCoordinator) ValidateInputs(capUrn string, arguments []interface{}, registry *media.FabricRegistry) error {
	cap := cvc.GetCap(capUrn)
	if cap == nil {
		return NewUnknownCapError(capUrn)
	}

	return cvc.inputValidator.ValidateArguments(cap, arguments, registry)
}

// ValidateOutput validates output against a cap's output schema
func (cvc *CapValidationCoordinator) ValidateOutput(capUrn string, output interface{}, registry *media.FabricRegistry) error {
	cap := cvc.GetCap(capUrn)
	if cap == nil {
		return NewUnknownCapError(capUrn)
	}

	return cvc.outputValidator.ValidateOutput(cap, output, registry)
}

// ValidateCapSchema validates a cap definition itself
func (cvc *CapValidationCoordinator) ValidateCapSchema(cap *Cap, registry *media.FabricRegistry) error {
	capUrn := cap.UrnString()
	args := cap.GetArgs()

	if len(args) == 0 {
		// Validate output media URN if present
		if cap.Output != nil {
			if _, err := cap.Output.Resolve(registry); err != nil {
				return &ValidationError{
					Type:    "InvalidCapSchema",
					CapUrn:  capUrn,
					Message: fmt.Sprintf("Cap '%s' output has unresolvable media URN '%s'", capUrn, cap.Output.MediaUrn),
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

	// Validate output media URN if present
	if cap.Output != nil {
		if _, err := cap.Output.Resolve(registry); err != nil {
			return &ValidationError{
				Type:    "InvalidCapSchema",
				CapUrn:  capUrn,
				Message: fmt.Sprintf("Cap '%s' output has unresolvable media URN '%s'", capUrn, cap.Output.MediaUrn),
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

// ValidateCapArgs enforces structural rules on a cap's argument definitions.
// This is a standalone function matching Rust's validate_cap_args().
// Rules:
//
//	RULE1: No duplicate media_urns across args
//	RULE2: Sources must not be empty
//	RULE3: If multiple args have stdin source, stdin media_urns must be identical
//	RULE4: No arg may specify same source type more than once
//	RULE5: No two args may have same position
//	RULE6: Positions must be sequential (0-based, no gaps)
//	RULE7: No arg may have both position and cli_flag
//	RULE9: No two args may have same cli_flag
//	RULE10: Reserved cli_flags rejected
//	RULE11: Stdin source consistency with in= spec (void input must have no stdin; non-void must have at least one)
func ValidateCapArgs(cap *Cap) error {
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

	// RULE3: If multiple args have stdin source, stdin media_urns must be identical
	if len(stdinUrns) > 1 {
		first := stdinUrns[0]
		for _, su := range stdinUrns[1:] {
			if su != first {
				return &ValidationError{
					Type:    "InvalidCapSchema",
					CapUrn:  capUrn,
					Message: fmt.Sprintf("RULE3: Multiple args have different stdin media_urns: '%s' vs '%s'", first, su),
				}
			}
		}
	}

	// RULE11: Stdin source consistency with in= spec
	if cap.Urn != nil {
		inMediaUrn, err := cap.Urn.InMediaUrn()
		if err == nil {
			voidUrn, voidErr := urn.NewMediaUrnFromString(media.MediaVoid)
			if voidErr == nil {
				isVoid := inMediaUrn.IsEquivalent(voidUrn)
				hasStdin := len(stdinUrns) > 0
				if isVoid && hasStdin {
					return &ValidationError{
						Type:    "InvalidCapSchema",
						CapUrn:  capUrn,
						Message: "RULE11: Cap has in=\"media:void\" but args have stdin sources",
					}
				}
				if !isVoid && !hasStdin {
					return &ValidationError{
						Type:    "InvalidCapSchema",
						CapUrn:  capUrn,
						Message: fmt.Sprintf("RULE11: Cap has in='%s' but no args declare a stdin source — the main input is the value piped in on stdin, so at least one arg must accept stdin", cap.Urn.InSpec()),
					}
				}
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

func getNumericValue(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

// ============================================================================
// XV5 VALIDATION - No Redefinition of Registry Media Defs
// ============================================================================

