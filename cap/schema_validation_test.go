package cap

import (
	"testing"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST163: Test argument schema validation succeeds with valid JSON matching schema
func Test163_schema_validator_validate_argument_with_schema_success(t *testing.T) {
	validator := NewSchemaValidator()

	// Define a JSON schema for user data
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"name": map[string]interface{}{
				"type": "string",
			},
			"age": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
			},
		},
		"required": []interface{}{"name"},
	}

	// Create an argument using new architecture
	cliFlag := "--user"
	pos := 0
	arg := CapArg{
		MediaUrn:       "media:json;record;test-obj;textable",
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: StringPtr("User data"),
	}

	// Test valid data
	validData := map[string]interface{}{
		"name": "John Doe",
		"age":  30,
	}

	err := validator.ValidateArgumentWithSchema(&arg, schema, validData)
	assert.NoError(t, err)
}

// TEST164: Test argument schema validation fails with JSON missing required fields
func Test164_schema_validator_validate_argument_with_schema_failure(t *testing.T) {
	validator := NewSchemaValidator()

	// Define a JSON schema requiring name field
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"name": map[string]interface{}{
				"type": "string",
			},
			"age": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
			},
		},
		"required": []interface{}{"name"},
	}

	// Create an argument using new architecture
	cliFlag := "--user"
	pos := 0
	arg := CapArg{
		MediaUrn:       "media:json;record;test-obj;textable",
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: StringPtr("User data"),
	}

	// Test invalid data (missing required field)
	invalidData := map[string]interface{}{
		"age": 30,
	}

	err := validator.ValidateArgumentWithSchema(&arg, schema, invalidData)
	assert.Error(t, err)

	schemaErr, ok := err.(*SchemaValidationError)
	require.True(t, ok)
	assert.Equal(t, "MediaValidation", schemaErr.Type)
	assert.Equal(t, "media:json;record;test-obj;textable", schemaErr.Argument)
	assert.Contains(t, schemaErr.Details, "name")
}

// Additional Go-specific coverage: nil schema skips direct schema validation
func TestSchemaValidator_ValidateArgumentWithSchema_NilSchema(t *testing.T) {
	validator := NewSchemaValidator()

	// Create argument using new architecture
	cliFlag := "--string"
	pos := 0
	arg := CapArg{
		MediaUrn:       standard.MediaString,
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: StringPtr("Simple string"),
	}

	// Nil schema should not validate
	err := validator.ValidateArgumentWithSchema(&arg, nil, "any string value")
	assert.NoError(t, err)
}

// TEST165: Test output schema validation succeeds with valid JSON matching schema
func Test165_schema_validator_validate_output_with_schema_success(t *testing.T) {
	validator := NewSchemaValidator()

	// Define a JSON schema for result data
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"result": map[string]interface{}{
				"type": "string",
			},
			"timestamp": map[string]interface{}{
				"type":   "string",
				"format": "date-time",
			},
		},
		"required": []interface{}{"result"},
	}

	// Create output
	output := NewCapOutput("media:test;result;textable;record", "Query result")

	// Test valid output data
	validData := map[string]interface{}{
		"result":    "success",
		"timestamp": "2023-01-01T00:00:00Z",
	}

	err := validator.ValidateOutputWithSchema(output, schema, validData)
	assert.NoError(t, err)
}

func TestSchemaValidator_ValidateOutputWithSchema_Failure(t *testing.T) {
	validator := NewSchemaValidator()

	// Define a JSON schema requiring result field
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"result": map[string]interface{}{
				"type": "string",
			},
		},
		"required": []interface{}{"result"},
	}

	// Create output
	output := NewCapOutput("media:test;result;textable;record", "Query result")

	// Test invalid output data (missing required field)
	invalidData := map[string]interface{}{
		"status": "ok",
	}

	err := validator.ValidateOutputWithSchema(output, schema, invalidData)
	assert.Error(t, err)

	schemaErr, ok := err.(*SchemaValidationError)
	require.True(t, ok)
	assert.Equal(t, "OutputValidation", schemaErr.Type)
	assert.Contains(t, schemaErr.Details, "result")
}

func TestSchemaValidator_ValidateArguments_Integration(t *testing.T) {
	registry := testRegistry(t)
	validator := NewSchemaValidator()

	// Create a capability with schema-enabled arguments
	urn, err := urn.NewCapUrnFromString(`cap:in="media:void";query;out="media:json;record;textable";target=structured`)
	require.NoError(t, err)

	cap := NewCap(urn, "Query Processor", "test-command")

	// Add a custom media def with schema
	userSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"name": map[string]interface{}{"type": "string"},
			"age":  map[string]interface{}{"type": "integer", "minimum": 0},
		},
		"required": []interface{}{"name"},
	}

	registry.AddSpec((media.NewMediaDefWithSchema(
		"media:user;textable;record",
		"application/json",
		"https://example.com/schema/user",
		userSchema,
	)).ToStored())

	// Add argument referencing the custom spec using new architecture
	cliFlag := "--user"
	pos := 0
	cap.AddArg(CapArg{
		MediaUrn:       "media:user;textable;record",
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: StringPtr("User data"),
	})

	// Test valid arguments
	validUser := map[string]interface{}{
		"name": "Alice",
		"age":  25,
	}

	namedArgs := map[string]interface{}{
		"media:user;textable;record": validUser,
	}

	err = validator.ValidateArguments(cap, []interface{}{}, namedArgs, registry)
	assert.NoError(t, err)

	// Test invalid arguments
	invalidUser := map[string]interface{}{
		"age": 25, // Missing required "name"
	}

	namedArgs = map[string]interface{}{
		"media:user;textable;record": invalidUser,
	}

	err = validator.ValidateArguments(cap, []interface{}{}, namedArgs, registry)
	assert.Error(t, err)
}

func TestSchemaValidator_ArraySchemaValidation(t *testing.T) {
	validator := NewSchemaValidator()

	// Define a JSON schema for an array of items
	schema := map[string]interface{}{
		"type": "array",
		"items": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"id":   map[string]interface{}{"type": "integer"},
				"name": map[string]interface{}{"type": "string"},
			},
			"required": []interface{}{"id", "name"},
		},
		"minItems": 1,
	}

	// Create an argument using new architecture
	cliFlag := "--items"
	pos := 0
	arg := CapArg{
		MediaUrn:       "media:items;textable;record",
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: StringPtr("List of items"),
	}

	// Test valid array data
	validData := []interface{}{
		map[string]interface{}{"id": 1, "name": "Item 1"},
		map[string]interface{}{"id": 2, "name": "Item 2"},
	}

	err := validator.ValidateArgumentWithSchema(&arg, schema, validData)
	assert.NoError(t, err)

	// Test invalid array data (missing required field)
	invalidData := []interface{}{
		map[string]interface{}{"id": 1}, // Missing "name"
	}

	err = validator.ValidateArgumentWithSchema(&arg, schema, invalidData)
	assert.Error(t, err)

	// Test empty array (violates minItems)
	emptyData := []interface{}{}

	err = validator.ValidateArgumentWithSchema(&arg, schema, emptyData)
	assert.Error(t, err)
}

func TestInputValidator_WithSchemaValidation(t *testing.T) {
	registry := testRegistry(t)
	validator := NewInputValidator()

	// Create a capability with schema-enabled arguments
	urn, err := urn.NewCapUrnFromString(`cap:in="media:void";test;out="media:json;record;textable"`)
	require.NoError(t, err)

	cap := NewCap(urn, "Config Validator", "test-command")

	// Add a custom media def with schema
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"value": map[string]interface{}{"type": "string", "minLength": 3},
		},
		"required": []interface{}{"value"},
	}

	registry.AddSpec((media.NewMediaDefWithSchema(
		"media:config;textable;record",
		"application/json",
		"https://example.com/schema/config",
		schema,
	)).ToStored())

	cliFlag := "--config"
	pos := 0
	cap.AddArg(CapArg{
		MediaUrn:       "media:config;textable;record",
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: StringPtr("Configuration"),
	})

	// Test valid input
	validConfig := map[string]interface{}{
		"value": "valid string",
	}

	err = validator.ValidateArguments(cap, []interface{}{validConfig}, registry)
	assert.NoError(t, err)

	// Test invalid input (violates minLength)
	invalidConfig := map[string]interface{}{
		"value": "ab", // Too short
	}

	err = validator.ValidateArguments(cap, []interface{}{invalidConfig}, registry)
	assert.Error(t, err)

	// Should get a ValidationError with schema validation type
	validationErr, ok := err.(*ValidationError)
	require.True(t, ok)
	assert.Equal(t, "SchemaValidationFailed", validationErr.Type)
}

func TestOutputValidator_WithSchemaValidation(t *testing.T) {
	registry := testRegistry(t)
	validator := NewOutputValidator()

	// Create a capability with schema-enabled output
	urn, err := urn.NewCapUrnFromString(`cap:in="media:void";test;out="media:json;record;textable"`)
	require.NoError(t, err)

	cap := NewCap(urn, "Output Validator", "test-command")

	// Add a custom media def with schema for output
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"status": map[string]interface{}{
				"type": "string",
				"enum": []interface{}{"success", "error"},
			},
			"data": map[string]interface{}{"type": "object"},
		},
		"required": []interface{}{"status"},
	}

	registry.AddSpec((media.NewMediaDefWithSchema(
		"media:result;textable;record",
		"application/json",
		"https://example.com/schema/result",
		schema,
	)).ToStored())

	output := NewCapOutput("media:result;textable;record", "Command result")
	cap.SetOutput(output)

	// Test valid output
	validOutput := map[string]interface{}{
		"status": "success",
		"data":   map[string]interface{}{"result": "ok"},
	}

	err = validator.ValidateOutput(cap, validOutput, registry)
	assert.NoError(t, err)

	// Test invalid output (invalid enum value)
	invalidOutput := map[string]interface{}{
		"status": "unknown", // Not in enum
		"data":   map[string]interface{}{"result": "ok"},
	}

	err = validator.ValidateOutput(cap, invalidOutput, registry)
	assert.Error(t, err)

	// Should get a ValidationError
	validationErr, ok := err.(*ValidationError)
	require.True(t, ok)
	assert.Equal(t, "OutputValidationFailed", validationErr.Type)
}

func TestCapValidationCoordinator_EndToEnd(t *testing.T) {
	registry := testRegistry(t)
	coordinator := NewCapValidationCoordinator()

	// Create a capability with full schema validation
	urn, err := urn.NewCapUrnFromString(`cap:in="media:void";query;out="media:json;record;textable";target=structured`)
	require.NoError(t, err)

	cap := NewCap(urn, "Structured Query", "query-command")

	// Add input argument with schema
	inputSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"query": map[string]interface{}{"type": "string", "minLength": 1},
			"limit": map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 100},
		},
		"required": []interface{}{"query"},
	}

	registry.AddSpec((media.NewMediaDefWithSchema(
		"media:query-params;textable;record",
		"application/json",
		"https://example.com/schema/query-params",
		inputSchema,
	)).ToStored())

	cliFlag := "--query"
	pos := 0
	cap.AddArg(CapArg{
		MediaUrn:       "media:query-params;textable;record",
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: StringPtr("Query parameters"),
	})

	// Add output with schema
	outputSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"results": map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"id":    map[string]interface{}{"type": "integer"},
						"title": map[string]interface{}{"type": "string"},
					},
				},
			},
			"total": map[string]interface{}{"type": "integer"},
		},
		"required": []interface{}{"results", "total"},
	}

	registry.AddSpec((media.NewMediaDefWithSchema(
		"media:query-results;textable;record",
		"application/json",
		"https://example.com/schema/query-results",
		outputSchema,
	)).ToStored())

	output := NewCapOutput("media:query-results;textable;record", "Query results")
	cap.SetOutput(output)

	// Register the capability
	coordinator.RegisterCap(cap)

	// Test valid input validation
	validInput := []interface{}{
		map[string]interface{}{
			"query": "search term",
			"limit": 10,
		},
	}

	err = coordinator.ValidateInputs(cap.UrnString(), validInput, registry)
	assert.NoError(t, err)

	// Test invalid input validation
	invalidInput := []interface{}{
		map[string]interface{}{
			"query": "", // Empty string violates minLength
			"limit": 0,  // Zero violates minimum
		},
	}

	err = coordinator.ValidateInputs(cap.UrnString(), invalidInput, registry)
	assert.Error(t, err)

	// Test valid output validation
	validOutput := map[string]interface{}{
		"results": []interface{}{
			map[string]interface{}{"id": 1, "title": "Result 1"},
			map[string]interface{}{"id": 2, "title": "Result 2"},
		},
		"total": 2,
	}

	err = coordinator.ValidateOutput(cap.UrnString(), validOutput, registry)
	assert.NoError(t, err)

	// Test invalid output validation
	invalidOutput := map[string]interface{}{
		"results": []interface{}{
			map[string]interface{}{"id": "not_integer", "title": "Result 1"}, // Invalid type
		},
		// Missing required "total" field
	}

	err = coordinator.ValidateOutput(cap.UrnString(), invalidOutput, registry)
	assert.Error(t, err)
}

func TestFileSchemaResolver_ErrorHandling(t *testing.T) {
	resolver := NewFileSchemaResolver("/nonexistent/path")

	_, err := resolver.ResolveSchema("test.schema.json")
	assert.Error(t, err)

	schemaErr, ok := err.(*SchemaValidationError)
	require.True(t, ok)
	assert.Equal(t, "SchemaRefNotResolved", schemaErr.Type)
}

func TestComplexNestedSchemaValidation(t *testing.T) {
	validator := NewSchemaValidator()

	// Define a complex nested schema
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"user": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"profile": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"name": map[string]interface{}{"type": "string"},
							"settings": map[string]interface{}{
								"type": "object",
								"properties": map[string]interface{}{
									"theme":         map[string]interface{}{"type": "string"},
									"notifications": map[string]interface{}{"type": "boolean"},
								},
							},
						},
						"required": []interface{}{"name"},
					},
					"permissions": map[string]interface{}{
						"type": "array",
						"items": map[string]interface{}{
							"type": "string",
							"enum": []interface{}{"read", "write", "admin"},
						},
					},
				},
				"required": []interface{}{"profile", "permissions"},
			},
		},
		"required": []interface{}{"user"},
	}

	cliFlag := "--user-data"
	pos := 0
	arg := CapArg{
		MediaUrn:       "media:user-data;textable;record",
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: StringPtr("Complex user data"),
	}

	// Test valid complex data
	validData := map[string]interface{}{
		"user": map[string]interface{}{
			"profile": map[string]interface{}{
				"name": "John Doe",
				"settings": map[string]interface{}{
					"theme":         "dark",
					"notifications": true,
				},
			},
			"permissions": []interface{}{"read", "write"},
		},
	}

	err := validator.ValidateArgumentWithSchema(&arg, schema, validData)
	assert.NoError(t, err)

	// Test invalid complex data (invalid permission)
	invalidData := map[string]interface{}{
		"user": map[string]interface{}{
			"profile": map[string]interface{}{
				"name": "John Doe",
			},
			"permissions": []interface{}{"read", "invalid_permission"}, // Invalid enum value
		},
	}

	err = validator.ValidateArgumentWithSchema(&arg, schema, invalidData)
	assert.Error(t, err)
}

func TestMediaUrnResolutionWithRegistry(t *testing.T) {
	registry := testRegistry(t)

	// Seed the registry with the specs the test resolves.
	for _, def := range []media.MediaDef{
		{Urn: media.MediaString, MediaType: "text/plain", ProfileURI: media.ProfileStr},
		{Urn: media.MediaInteger, MediaType: "text/plain", ProfileURI: media.ProfileInt},
		{Urn: standard.MediaJSON, MediaType: "application/json", ProfileURI: media.ProfileObj},
		{Urn: media.MediaIdentity, MediaType: "application/octet-stream"},
	} {
		registry.AddSpec(def.ToStored())
	}

	resolved, err := media.ResolveMediaUrn(media.MediaString, registry)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", resolved.MediaType)
	assert.Equal(t, media.ProfileStr, resolved.ProfileURI)

	resolved, err = media.ResolveMediaUrn(media.MediaInteger, registry)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", resolved.MediaType)
	assert.Equal(t, media.ProfileInt, resolved.ProfileURI)

	resolved, err = media.ResolveMediaUrn(standard.MediaJSON, registry)
	require.NoError(t, err)
	assert.Equal(t, "application/json", resolved.MediaType)
	assert.Equal(t, media.ProfileObj, resolved.ProfileURI)

	resolved, err = media.ResolveMediaUrn(media.MediaIdentity, registry)
	require.NoError(t, err)
	assert.Equal(t, "application/octet-stream", resolved.MediaType)
	assert.True(t, resolved.IsBinary())
}

func TestCustomMediaUrnResolution(t *testing.T) {
	registry := testRegistry(t)

	for _, def := range []media.MediaDef{
		{Urn: "media:custom;textable", MediaType: "text/html", ProfileURI: "https://example.com/schema/html"},
		media.NewMediaDefWithSchema(
			"media:complex;textable;record",
			"application/json",
			"https://example.com/schema/complex",
			map[string]interface{}{"type": "object"},
		),
	} {
		registry.AddSpec(def.ToStored())
	}

	// Resolution
	resolved, err := media.ResolveMediaUrn("media:custom;textable", registry)
	require.NoError(t, err)
	assert.Equal(t, "text/html", resolved.MediaType)
	assert.Equal(t, "https://example.com/schema/html", resolved.ProfileURI)

	// Object form resolution with schema
	resolved, err = media.ResolveMediaUrn("media:complex;textable;record", registry)
	require.NoError(t, err)
	assert.Equal(t, "application/json", resolved.MediaType)
	assert.NotNil(t, resolved.Schema)

	// Unknown media URN should fail
	_, err = media.ResolveMediaUrn("media:unknown", registry)
	assert.Error(t, err)
}

// TEST166: Test validation skipped when resolved media def has no schema
func Test166_schema_validator_skip_validation_without_schema(t *testing.T) {
	registry := testRegistry(t)
	validator := NewSchemaValidator()

	// Create cap with no custom media defs
	urn, err := urn.NewCapUrnFromString(`cap:in="media:void";test;out="media:json;record;textable"`)
	require.NoError(t, err)
	cap := NewCap(urn, "Test Cap", "test-command")

	// Add argument using media.MediaString (expanded form, resolves from registry, has no schema)
	cliFlag := "--input"
	pos := 0
	cap.AddArg(CapArg{
		MediaUrn:       media.MediaString,
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: StringPtr("String input"),
	})

	// Validate with any string value - should succeed because media.MediaString has no schema
	err = validator.ValidateArguments(cap, []interface{}{"any string value"}, nil, registry)
	assert.NoError(t, err, "Validation should succeed when resolved spec has no schema")
}

// TEST167: Test validation fails hard when media URN cannot be resolved from any source
func Test167_schema_validator_unresolvable_media_urn_fails_hard(t *testing.T) {
	registry := testRegistry(t)
	validator := NewSchemaValidator()

	// Create cap with no custom media defs
	urn, err := urn.NewCapUrnFromString(`cap:in="media:void";test;out="media:json;record;textable"`)
	require.NoError(t, err)
	cap := NewCap(urn, "Test Cap", "test-command")

	// Add argument with completely unknown media URN (not in media_defs, not in registry)
	cliFlag := "--input"
	pos := 0
	unknownUrn := "media:completely-unknown-urn-that-does-not-exist"
	cap.AddArg(CapArg{
		MediaUrn:       unknownUrn,
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: StringPtr("Unknown type"),
	})

	// Validate with any value - should fail hard because URN cannot be resolved
	err = validator.ValidateArguments(cap, []interface{}{"test"}, nil, registry)
	require.Error(t, err, "Validation should fail when media URN cannot be resolved")

	// Check it's the right kind of error
	schemaErr, ok := err.(*SchemaValidationError)
	require.True(t, ok, "Error should be SchemaValidationError")
	assert.Equal(t, "UnresolvableMediaUrn", schemaErr.Type)
	assert.Equal(t, unknownUrn, schemaErr.Argument)
}
