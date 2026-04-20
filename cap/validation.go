package cap

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
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

// NewUnresolvableMediaUrnErrorForValidation creates an error for unresolvable media URNs in validation
func NewUnresolvableMediaUrnErrorForValidation(capUrn, argumentName, mediaUrn string) *ValidationError {
	return &ValidationError{
		Type:         "UnresolvableMediaUrn",
		CapUrn:       capUrn,
		ArgumentName: argumentName,
		Message:      fmt.Sprintf("Cap '%s' argument '%s' has unresolvable media URN '%s' - not found in media_specs and not a built-in", capUrn, argumentName, mediaUrn),
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

// NewMediaSpecValidationFailedError creates an error for media spec validation failures (inherent to semantic type)
func NewMediaSpecValidationFailedError(capUrn, argumentName, mediaUrn, rule string, actualValue interface{}) *ValidationError {
	return &ValidationError{
		Type:         "MediaSpecValidationFailed",
		CapUrn:       capUrn,
		ArgumentName: argumentName,
		Rule:         rule,
		ActualValue:  actualValue,
		Message:      fmt.Sprintf("Cap '%s' argument '%s' failed media spec '%s' validation rule '%s' with value: %v", capUrn, argumentName, mediaUrn, rule, actualValue),
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

// NewOutputMediaSpecValidationFailedError creates an error for output media spec validation failures
func NewOutputMediaSpecValidationFailedError(capUrn, mediaUrn, rule string, actualValue interface{}) *ValidationError {
	return &ValidationError{
		Type:        "OutputMediaSpecValidationFailed",
		CapUrn:      capUrn,
		Rule:        rule,
		ActualValue: actualValue,
		Message:     fmt.Sprintf("Cap '%s' output failed media spec '%s' validation rule '%s' with value: %v", capUrn, mediaUrn, rule, actualValue),
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

// InputValidator validates arguments against cap input schemas
type InputValidator struct {
	schemaValidator *SchemaValidator
}

// NewInputValidator creates a new input validator
func NewInputValidator() *InputValidator {
	return &InputValidator{
		schemaValidator: NewSchemaValidator(),
	}
}

// NewInputValidatorWithSchemaResolver creates a new input validator with schema resolver
func NewInputValidatorWithSchemaResolver(resolver SchemaResolver) *InputValidator {
	return &InputValidator{
		schemaValidator: NewSchemaValidatorWithResolver(resolver),
	}
}

// ValidateArguments validates arguments against a cap's input schema
func (iv *InputValidator) ValidateArguments(cap *Cap, arguments []interface{}, registry *media.MediaUrnRegistry) error {
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
func (iv *InputValidator) ValidateNamedArguments(cap *Cap, namedArgs []map[string]interface{}, registry *media.MediaUrnRegistry) error {
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

func (iv *InputValidator) validateSingleArgument(cap *Cap, argDef *CapArg, value interface{}, registry *media.MediaUrnRegistry) error {
	// Resolve the media URN to determine the expected type
	resolved, err := argDef.Resolve(cap.GetMediaSpecs(), registry)
	if err != nil {
		return NewUnresolvableMediaUrnErrorForValidation(cap.UrnString(), argDef.MediaUrn, argDef.MediaUrn)
	}

	// Type validation based on resolved media spec
	if err := iv.validateArgumentType(cap, argDef, resolved, value); err != nil {
		return err
	}

	// Media spec validation rules (inherent to the semantic type)
	if resolved.Validation != nil {
		if err := iv.validateMediaSpecRules(cap, argDef, resolved, value); err != nil {
			return err
		}
	}

	// Schema validation for object/array types
	if err := iv.validateArgumentSchema(cap, argDef, resolved, value); err != nil {
		return err
	}

	return nil
}

// validateMediaSpecRules validates value against media spec's inherent validation rules (first pass)
func (iv *InputValidator) validateMediaSpecRules(cap *Cap, argDef *CapArg, resolved *media.ResolvedMediaSpec, value interface{}) error {
	capUrn := cap.UrnString()
	validation := resolved.Validation
	mediaUrn := resolved.SpecID

	// Numeric validation
	if validation.Min != nil {
		if num, ok := getNumericValue(value); ok {
			if num < *validation.Min {
				return NewMediaSpecValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("minimum value %v", *validation.Min), value)
			}
		}
	}

	if validation.Max != nil {
		if num, ok := getNumericValue(value); ok {
			if num > *validation.Max {
				return NewMediaSpecValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("maximum value %v", *validation.Max), value)
			}
		}
	}

	// String length validation
	if validation.MinLength != nil {
		if s, ok := value.(string); ok {
			if len(s) < *validation.MinLength {
				return NewMediaSpecValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("minimum length %d", *validation.MinLength), value)
			}
		}
	}

	if validation.MaxLength != nil {
		if s, ok := value.(string); ok {
			if len(s) > *validation.MaxLength {
				return NewMediaSpecValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("maximum length %d", *validation.MaxLength), value)
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
					Message: fmt.Sprintf("Invalid regex pattern '%s' in media spec '%s': %v", *validation.Pattern, mediaUrn, err),
				}
			}
			if !regex.MatchString(s) {
				return NewMediaSpecValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("pattern '%s'", *validation.Pattern), value)
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
				return NewMediaSpecValidationFailedError(capUrn, argDef.MediaUrn, mediaUrn, fmt.Sprintf("allowed values: %v", validation.AllowedValues), value)
			}
		}
	}

	return nil
}

// validateArgumentSchema validates argument against JSON schema
func (iv *InputValidator) validateArgumentSchema(cap *Cap, argDef *CapArg, resolved *media.ResolvedMediaSpec, value interface{}) error {
	// Only validate structured types (map, list, or json) that have schemas
	if !resolved.IsStructured() {
		return nil
	}

	// Get schema from resolved media spec
	schema := resolved.Schema
	if schema == nil {
		return nil // No schema to validate against
	}

	if err := iv.schemaValidator.ValidateArgumentWithSchema(argDef, schema, value); err != nil {
		if schemaErr, ok := err.(*SchemaValidationError); ok {
			return NewSchemaValidationFailedError(cap.UrnString(), argDef.MediaUrn, schemaErr.Details, value)
		}
		return err
	}

	return nil
}

func (iv *InputValidator) validateArgumentType(cap *Cap, argDef *CapArg, resolved *media.ResolvedMediaSpec, value interface{}) error {
	capUrn := cap.UrnString()
	actualType := getValueTypeName(value)

	// Determine expected type from media URN
	expectedType := getExpectedTypeFromMediaUrn(argDef.MediaUrn, resolved)

	typeMatches := false
	switch expectedType {
	case "string":
		_, typeMatches = value.(string)
	case "integer":
		if num, ok := value.(float64); ok {
			typeMatches = num == float64(int64(num))
		} else if _, ok := value.(int); ok {
			typeMatches = true
		} else if _, ok := value.(int64); ok {
			typeMatches = true
		}
	case "number":
		_, ok1 := value.(float64)
		_, ok2 := value.(int)
		_, ok3 := value.(int64)
		typeMatches = ok1 || ok2 || ok3
	case "boolean":
		_, typeMatches = value.(bool)
	case "array":
		_, typeMatches = value.([]interface{})
	case "object":
		_, typeMatches = value.(map[string]interface{})
	case "binary":
		_, typeMatches = value.(string) // Binary as base64 string
	default:
		// For unknown types from custom specs, accept any value
		typeMatches = true
	}

	if !typeMatches {
		return NewInvalidArgumentTypeErrorFromMediaUrn(capUrn, argDef.MediaUrn, argDef.MediaUrn, expectedType, actualType, value)
	}

	return nil
}

// getExpectedTypeFromMediaUrn determines the expected Go type from a media URN
// Uses media.GetTypeFromMediaUrn for consistent type detection based on media URN tags
func getExpectedTypeFromMediaUrn(mediaUrn string, resolved *media.ResolvedMediaSpec) string {
	// Use the centralized type detection based on media URN tags
	typeFromUrn := media.GetTypeFromMediaUrn(mediaUrn)
	if typeFromUrn != "unknown" {
		return typeFromUrn
	}

	// Fallback: infer from resolved media spec if available
	if resolved != nil {
		if resolved.IsBinary() {
			return "binary"
		}
		// Check for record structure (has internal fields) OR explicit json tag
		if resolved.IsRecord() || resolved.IsJSON() {
			return "object"
		}
		// Check for list structure (list)
		if resolved.IsList() {
			return "array"
		}
		// Scalar or text types
		if resolved.IsText() || resolved.IsScalar() {
			return "string"
		}
	}

	return "unknown"
}

// OutputValidator validates output against cap output schemas
type OutputValidator struct {
	schemaValidator *SchemaValidator
}

// NewOutputValidator creates a new output validator
func NewOutputValidator() *OutputValidator {
	return &OutputValidator{
		schemaValidator: NewSchemaValidator(),
	}
}

// NewOutputValidatorWithSchemaResolver creates a new output validator with schema resolver
func NewOutputValidatorWithSchemaResolver(resolver SchemaResolver) *OutputValidator {
	return &OutputValidator{
		schemaValidator: NewSchemaValidatorWithResolver(resolver),
	}
}

// ValidateOutput validates output against a cap's output schema
// Two-pass validation:
// 1. Type validation + media spec validation rules (inherent to semantic type)
// 2. Output-level validation rules (context-specific)
func (ov *OutputValidator) ValidateOutput(cap *Cap, output interface{}, registry *media.MediaUrnRegistry) error {
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
	resolved, err := outputDef.Resolve(cap.GetMediaSpecs(), registry)
	if err != nil {
		return &ValidationError{
			Type:    "UnresolvableMediaUrn",
			CapUrn:  capUrn,
			Message: fmt.Sprintf("Cap '%s' output has unresolvable media URN '%s'", capUrn, outputDef.MediaUrn),
		}
	}

	// Type validation
	if err := ov.validateOutputType(cap, outputDef, resolved, output); err != nil {
		return err
	}

	// Media spec validation rules (inherent to the semantic type)
	if resolved.Validation != nil {
		if err := ov.validateOutputMediaSpecRules(cap, resolved, output); err != nil {
			return err
		}
	}

	// Schema validation for structured outputs
	if err := ov.validateOutputSchema(cap, outputDef, resolved, output); err != nil {
		return err
	}

	return nil
}

// validateOutputMediaSpecRules validates output against media spec's inherent validation rules (first pass)
func (ov *OutputValidator) validateOutputMediaSpecRules(cap *Cap, resolved *media.ResolvedMediaSpec, value interface{}) error {
	capUrn := cap.UrnString()
	validation := resolved.Validation
	mediaUrn := resolved.SpecID

	// Numeric validation
	if validation.Min != nil {
		if num, ok := getNumericValue(value); ok {
			if num < *validation.Min {
				return NewOutputMediaSpecValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("minimum value %v", *validation.Min), value)
			}
		}
	}

	if validation.Max != nil {
		if num, ok := getNumericValue(value); ok {
			if num > *validation.Max {
				return NewOutputMediaSpecValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("maximum value %v", *validation.Max), value)
			}
		}
	}

	// String length validation
	if validation.MinLength != nil {
		if s, ok := value.(string); ok {
			if len(s) < *validation.MinLength {
				return NewOutputMediaSpecValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("minimum length %d", *validation.MinLength), value)
			}
		}
	}

	if validation.MaxLength != nil {
		if s, ok := value.(string); ok {
			if len(s) > *validation.MaxLength {
				return NewOutputMediaSpecValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("maximum length %d", *validation.MaxLength), value)
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
					Message: fmt.Sprintf("Invalid regex pattern '%s' in media spec '%s': %v", *validation.Pattern, mediaUrn, err),
				}
			}
			if !regex.MatchString(s) {
				return NewOutputMediaSpecValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("pattern '%s'", *validation.Pattern), value)
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
				return NewOutputMediaSpecValidationFailedError(capUrn, mediaUrn, fmt.Sprintf("allowed values: %v", validation.AllowedValues), value)
			}
		}
	}

	return nil
}

// validateOutputSchema validates output against JSON schema
func (ov *OutputValidator) validateOutputSchema(cap *Cap, outputDef *CapOutput, resolved *media.ResolvedMediaSpec, value interface{}) error {
	// Only validate structured types (map, list, or json) that have schemas
	if !resolved.IsStructured() {
		return nil
	}

	// Get schema from resolved media spec
	schema := resolved.Schema
	if schema == nil {
		return nil // No schema to validate against
	}

	if err := ov.schemaValidator.ValidateOutputWithSchema(outputDef, schema, value); err != nil {
		if schemaErr, ok := err.(*SchemaValidationError); ok {
			return NewOutputValidationFailedError(cap.UrnString(), "schema validation: "+schemaErr.Details, value)
		}
		return err
	}

	return nil
}

func (ov *OutputValidator) validateOutputType(cap *Cap, outputDef *CapOutput, resolved *media.ResolvedMediaSpec, value interface{}) error {
	capUrn := cap.UrnString()
	actualType := getValueTypeName(value)

	// Determine expected type from media URN
	expectedType := getExpectedTypeFromMediaUrn(outputDef.MediaUrn, resolved)

	typeMatches := false
	switch expectedType {
	case "string":
		_, typeMatches = value.(string)
	case "integer":
		if num, ok := value.(float64); ok {
			typeMatches = num == float64(int64(num))
		} else if _, ok := value.(int); ok {
			typeMatches = true
		} else if _, ok := value.(int64); ok {
			typeMatches = true
		}
	case "number":
		_, ok1 := value.(float64)
		_, ok2 := value.(int)
		_, ok3 := value.(int64)
		typeMatches = ok1 || ok2 || ok3
	case "boolean":
		_, typeMatches = value.(bool)
	case "array":
		_, typeMatches = value.([]interface{})
	case "object":
		_, typeMatches = value.(map[string]interface{})
	case "binary":
		_, typeMatches = value.(string) // Binary as base64 string
	default:
		// For unknown types from custom specs, accept any value
		typeMatches = true
	}

	if !typeMatches {
		return NewInvalidOutputTypeErrorFromMediaUrn(capUrn, outputDef.MediaUrn, expectedType, actualType, value)
	}

	return nil
}

// CapValidationCoordinator provides centralized validation coordination
type CapValidationCoordinator struct {
	caps            map[string]*Cap
	inputValidator  *InputValidator
	outputValidator *OutputValidator
}

// NewCapValidationCoordinator creates a new validation coordinator
func NewCapValidationCoordinator() *CapValidationCoordinator {
	return &CapValidationCoordinator{
		caps:            make(map[string]*Cap),
		inputValidator:  NewInputValidator(),
		outputValidator: NewOutputValidator(),
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
func (cvc *CapValidationCoordinator) ValidateInputs(capUrn string, arguments []interface{}, registry *media.MediaUrnRegistry) error {
	cap := cvc.GetCap(capUrn)
	if cap == nil {
		return NewUnknownCapError(capUrn)
	}

	return cvc.inputValidator.ValidateArguments(cap, arguments, registry)
}

// ValidateOutput validates output against a cap's output schema
func (cvc *CapValidationCoordinator) ValidateOutput(capUrn string, output interface{}, registry *media.MediaUrnRegistry) error {
	cap := cvc.GetCap(capUrn)
	if cap == nil {
		return NewUnknownCapError(capUrn)
	}

	return cvc.outputValidator.ValidateOutput(cap, output, registry)
}

// ValidateCapSchema validates a cap definition itself
func (cvc *CapValidationCoordinator) ValidateCapSchema(cap *Cap, registry *media.MediaUrnRegistry) error {
	capUrn := cap.UrnString()
	args := cap.GetArgs()

	if len(args) == 0 {
		// Validate output media URN if present
		if cap.Output != nil {
			if _, err := cap.Output.Resolve(cap.GetMediaSpecs(), registry); err != nil {
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
		if _, err := arg.Resolve(cap.GetMediaSpecs(), registry); err != nil {
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
		if _, err := cap.Output.Resolve(cap.GetMediaSpecs(), registry); err != nil {
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
						Message: fmt.Sprintf("RULE11: Cap has in=\"%s\" but no args have stdin sources", cap.Urn.InSpec()),
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

func getValueTypeName(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case int, int8, int16, int32, int64:
		return "integer"
	case float32, float64:
		return "number"
	case string:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	case json.Number:
		if _, err := v.Int64(); err == nil {
			return "integer"
		}
		return "number"
	default:
		return fmt.Sprintf("%T", value)
	}
}

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
// XV5 VALIDATION - No Redefinition of Registry Media Specs
// ============================================================================

// XV5ValidationResult contains the result of XV5 validation
type XV5ValidationResult struct {
	Valid     bool
	Error     string
	Redefines []string
}

// NewInlineMediaSpecRedefinesRegistryError creates an error for XV5 violations
func NewInlineMediaSpecRedefinesRegistryError(mediaUrn string) *ValidationError {
	return &ValidationError{
		Type:    "InlineMediaSpecRedefinesRegistry",
		Message: fmt.Sprintf("XV5: Inline media spec '%s' redefines existing registry spec", mediaUrn),
	}
}

// MediaUrnExistsInRegistryFunc is a function that checks if a media URN exists in the registry
// Returns true if the media URN exists in registry (cache or online), false otherwise
// If the check fails (network error, etc.), should return false to allow graceful degradation
type MediaUrnExistsInRegistryFunc func(mediaUrn string) bool

// ValidateNoInlineMediaSpecRedefinition checks that inline media_specs don't redefine existing registry specs (XV5)
// If existsInRegistry is nil, validation passes (graceful degradation - can't check)
func ValidateNoInlineMediaSpecRedefinition(mediaSpecs map[string]any, existsInRegistry MediaUrnExistsInRegistryFunc) XV5ValidationResult {
	if len(mediaSpecs) == 0 {
		return XV5ValidationResult{Valid: true}
	}

	// If no registry check provided, degrade gracefully and allow
	if existsInRegistry == nil {
		return XV5ValidationResult{Valid: true}
	}

	var redefines []string
	for mediaUrn := range mediaSpecs {
		// Check if this media URN already exists in the registry
		if existsInRegistry(mediaUrn) {
			redefines = append(redefines, mediaUrn)
		}
	}

	if len(redefines) > 0 {
		return XV5ValidationResult{
			Valid:     false,
			Error:     fmt.Sprintf("XV5: Inline media specs redefine existing registry specs: %v", redefines),
			Redefines: redefines,
		}
	}

	return XV5ValidationResult{Valid: true}
}
