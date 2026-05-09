package cap

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/machinefabric/capdag-go/media"
	"github.com/xeipuuv/gojsonschema"
)

// SchemaValidationError represents errors that occur during JSON schema validation
type SchemaValidationError struct {
	Type     string      `json:"type"`
	CapUrn   string      `json:"cap_urn,omitempty"`
	Argument string      `json:"argument,omitempty"`
	Details  string      `json:"details"`
	Context  string      `json:"context,omitempty"`
	Value    interface{} `json:"value,omitempty"`
}

func (e *SchemaValidationError) Error() string {
	if e.Argument != "" {
		return fmt.Sprintf("Schema validation failed for argument '%s': %s", e.Argument, e.Details)
	}
	return fmt.Sprintf("Schema validation failed: %s", e.Details)
}

// SchemaResolver interface for resolving external schema references
type SchemaResolver interface {
	ResolveSchema(schemaRef string) (interface{}, error)
}

// FileSchemaResolver implements SchemaResolver for file-based schemas
type FileSchemaResolver struct {
	basePath string
}

// NewFileSchemaResolver creates a new file-based schema resolver
func NewFileSchemaResolver(basePath string) *FileSchemaResolver {
	return &FileSchemaResolver{
		basePath: basePath,
	}
}

// ResolveSchema resolves a schema reference to a JSON schema
func (f *FileSchemaResolver) ResolveSchema(schemaRef string) (interface{}, error) {
	// This is a simple implementation - in production you might want
	// to support HTTP URLs, caching, etc.
	schemaPath := f.basePath + "/" + schemaRef

	// For now, return an error indicating that file resolution is not implemented
	// In a full implementation, you would read the file and parse the JSON
	return nil, &SchemaValidationError{
		Type:    "SchemaRefNotResolved",
		Details: fmt.Sprintf("Schema reference '%s' could not be resolved from path '%s'", schemaRef, schemaPath),
	}
}

// SchemaValidator provides JSON Schema Draft-7 validation capabilities
type SchemaValidator struct {
	resolver SchemaResolver
}

// NewSchemaValidator creates a new schema validator
func NewSchemaValidator() *SchemaValidator {
	return &SchemaValidator{}
}

// NewSchemaValidatorWithResolver creates a new schema validator with a schema resolver
func NewSchemaValidatorWithResolver(resolver SchemaResolver) *SchemaValidator {
	return &SchemaValidator{
		resolver: resolver,
	}
}

// ValidateArgumentWithSchema validates an argument value against a provided schema
// The schema comes from the resolved media spec
func (sv *SchemaValidator) ValidateArgumentWithSchema(arg *CapArg, schema interface{}, value interface{}) error {
	if schema == nil {
		return nil // No schema to validate against
	}
	return sv.validateValueAgainstSchema(arg.MediaUrn, value, schema, "argument")
}

// ValidateOutputWithSchema validates output value against a provided schema
// The schema comes from the resolved media spec
func (sv *SchemaValidator) ValidateOutputWithSchema(output *CapOutput, schema interface{}, value interface{}) error {
	if schema == nil {
		return nil // No schema to validate against
	}
	return sv.validateValueAgainstSchema("output", value, schema, "output")
}

// ValidateArguments validates all arguments for a capability using media specs
func (sv *SchemaValidator) ValidateArguments(cap *Cap, arguments []interface{}, namedArgs map[string]interface{}, registry *media.FabricRegistry) error {
	args := cap.GetArgs()
	if len(args) == 0 {
		return nil
	}

	requiredArgs := cap.GetRequiredArgs()
	optionalArgs := cap.GetOptionalArgs()

	// Validate positional required arguments
	for i, argDef := range requiredArgs {
		var value interface{}
		var found bool

		// Check if this argument has a position
		pos := argDef.GetPosition()
		if pos != nil {
			if *pos < len(arguments) {
				value = arguments[*pos]
				found = true
			}
		} else if i < len(arguments) {
			// Use index-based position
			value = arguments[i]
			found = true
		}

		// Also check named arguments (by media_urn)
		if namedArgs != nil {
			if namedValue, exists := namedArgs[argDef.MediaUrn]; exists {
				value = namedValue
				found = true
			}
		}

		if found {
			// Resolve the media URN to get the schema
			resolved, err := argDef.Resolve(registry)
			if err != nil {
				return &SchemaValidationError{
					Type:     "UnresolvableMediaUrn",
					Argument: argDef.MediaUrn,
					Details:  fmt.Sprintf("Could not resolve media URN '%s'", argDef.MediaUrn),
				}
			}

			// Only validate if there's a schema
			if resolved.Schema != nil {
				if err := sv.ValidateArgumentWithSchema(&argDef, resolved.Schema, value); err != nil {
					return err
				}
			}
		}
	}

	// Validate optional arguments if provided
	for _, argDef := range optionalArgs {
		var value interface{}
		var found bool

		// Check named arguments first for optional args (by media_urn)
		if namedArgs != nil {
			if namedValue, exists := namedArgs[argDef.MediaUrn]; exists {
				value = namedValue
				found = true
			}
		}

		// Check positional if not found in named args
		pos := argDef.GetPosition()
		if !found && pos != nil {
			if *pos < len(arguments) {
				value = arguments[*pos]
				found = true
			}
		}

		if found {
			// Resolve the media URN to get the schema
			resolved, err := argDef.Resolve(registry)
			if err != nil {
				return &SchemaValidationError{
					Type:     "UnresolvableMediaUrn",
					Argument: argDef.MediaUrn,
					Details:  fmt.Sprintf("Could not resolve media URN '%s'", argDef.MediaUrn),
				}
			}

			// Only validate if there's a schema
			if resolved.Schema != nil {
				if err := sv.ValidateArgumentWithSchema(&argDef, resolved.Schema, value); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// validateValueAgainstSchema performs the actual JSON schema validation
func (sv *SchemaValidator) validateValueAgainstSchema(name string, value interface{}, schema interface{}, context string) error {
	// Convert schema to JSON string for gojsonschema
	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return &SchemaValidationError{
			Type:    "SchemaCompilation",
			Details: fmt.Sprintf("Failed to marshal schema: %v", err),
			Context: context,
		}
	}

	// Convert value to JSON string for validation
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return &SchemaValidationError{
			Type:    "InvalidJson",
			Details: fmt.Sprintf("Failed to marshal value for validation: %v", err),
			Context: context,
			Value:   value,
		}
	}

	// Create schema and document loaders
	schemaLoader := gojsonschema.NewBytesLoader(schemaBytes)
	documentLoader := gojsonschema.NewBytesLoader(valueBytes)

	// Validate
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return &SchemaValidationError{
			Type:    "SchemaCompilation",
			Details: fmt.Sprintf("Failed to validate schema: %v", err),
			Context: context,
		}
	}

	// Check validation results
	if !result.Valid() {
		var errorDetails []string
		for _, desc := range result.Errors() {
			errorDetails = append(errorDetails, fmt.Sprintf("  - %s", desc))
		}

		if context == "argument" {
			return &SchemaValidationError{
				Type:     "MediaValidation",
				Argument: name,
				Details:  strings.Join(errorDetails, "\n"),
				Value:    value,
			}
		} else {
			return &SchemaValidationError{
				Type:    "OutputValidation",
				Details: strings.Join(errorDetails, "\n"),
				Value:   value,
			}
		}
	}

	return nil
}
