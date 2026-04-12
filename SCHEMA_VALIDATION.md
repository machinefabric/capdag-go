# JSON Schema Validation for capdag-go and machfab-cartridge-sdk-go

This document describes the comprehensive JSON Schema validation system implemented for the Go SDKs to match the capabilities of the Rust implementation.

## Overview

The implementation adds full JSON Schema Draft-7 validation for capability arguments and outputs, including:

- **Embedded schemas** - JSON schemas defined directly in capability definitions
- **Schema references** - External schema files referenced by path
- **Comprehensive error reporting** - Detailed validation error messages with schema context
- **Production-quality error handling** - Fail-hard on validation errors instead of warnings
- **Integration with existing validation** - Works seamlessly with existing CapCaller and ResponseWrapper systems

## New Features Added

### 1. Enhanced Core Types

**File: `cap.go`**

Added schema fields to `CapArgument` and `CapOutput` structs:
- `SchemaRef *string` - Reference to external JSON schema file
- `Schema interface{}` - Embedded JSON schema definition

New constructor functions:
- `NewCapArgumentWithSchema()` - Create argument with embedded schema
- `NewCapArgumentWithSchemaRef()` - Create argument with schema reference
- `NewCapOutputWithEmbeddedSchema()` - Create output with embedded schema
- `NewCapOutputWithSchemaRef()` - Create output with schema reference

### 2. JSON Schema Validation Engine

**File: `schema_validation.go`**

Core validation components:
- `SchemaValidator` - Main validation engine using `github.com/xeipuuv/gojsonschema`
- `SchemaValidationError` - Structured error type for validation failures
- `SchemaResolver` interface - For resolving external schema references
- `FileSchemaResolver` - File-based schema resolver implementation

Key methods:
- `ValidateArgument()` - Validate single argument against schema
- `ValidateOutput()` - Validate output against schema
- `ValidateArguments()` - Validate all capability arguments with named/positional support

### 3. Enhanced Validation System

**File: `validation.go`**

Updated validation infrastructure:
- `InputValidator` - Enhanced with schema validation support
- `OutputValidator` - Enhanced with schema validation support  
- `CapValidationCoordinator` - Centralized validation coordination
- Integration with existing type and rule validation

New validation error type:
- `SchemaValidationFailed` - Added to existing `ValidationError` types

### 4. Cartridge SDK Integration

**File: `machfab-cartridge-sdk-go/sdk.go`**

Re-exported all new types and constructors:
- Schema validation types (`SchemaValidator`, `SchemaValidationError`, etc.)
- Schema-enabled constructors
- Argument and output type constants
- Full compatibility with existing cartridge development workflows

## Usage Examples

### Basic Schema Validation

```go
import sdk "github.com/machinefabric/machfab-cartridge-sdk-go"

// Create capability with embedded schema
schema := map[string]interface{}{
    "type": "object",
    "properties": map[string]interface{}{
        "name": map[string]interface{}{"type": "string", "minLength": 2},
        "age":  map[string]interface{}{"type": "integer", "minimum": 0},
    },
    "required": []interface{}{"name", "age"},
}

arg := sdk.NewCapArgumentWithSchema("user_data", sdk.ArgumentTypeObject, "User data", "--user", schema)
```

### Validation Coordinator

```go
// Create validation coordinator
coordinator := sdk.NewCapValidationCoordinator()
coordinator.RegisterCap(cap)

// Validate inputs
err := coordinator.ValidateInputs(cap.UrnString(), arguments)
if err != nil {
    // Handle schema validation error
}

// Validate outputs  
err = coordinator.ValidateOutput(cap.UrnString(), output)
```

### Schema References

```go
// Create resolver for external schemas
resolver := sdk.NewFileSchemaResolver("/path/to/schemas")
validator := sdk.NewSchemaValidatorWithResolver(resolver)

// Create argument with schema reference
arg := sdk.NewCapArgumentWithSchemaRef("config", sdk.ArgumentTypeObject, "Configuration", "--config", "config.schema.json")
```

## Integration with Existing Systems

### CapCaller Integration

The `CapCaller` has been updated to automatically use schema validation:

```go
caller := sdk.NewCapCaller(capUrn, host, capDefinition)
response, err := caller.Call(ctx, args, namedArgs, nil)
// Automatically validates inputs and outputs against schemas
```

### Error Handling

Schema validation errors provide detailed information:

```go
if err != nil {
    if schemaErr, ok := err.(*sdk.SchemaValidationError); ok {
        fmt.Printf("Validation failed: %s\n", schemaErr.Details)
        fmt.Printf("For argument: %s\n", schemaErr.Argument) 
        fmt.Printf("Value: %v\n", schemaErr.Value)
    }
}
```

## Production Quality Features

### 1. Comprehensive Error Reporting
- Detailed JSON Schema validation errors with path information
- Structured error types for programmatic handling
- Clear distinction between different types of validation failures

### 2. Performance Optimizations
- Schema compilation caching for repeated validations
- Only validates structured types (object/array) that have schemas
- Minimal overhead for capabilities without schemas

### 3. Robust Schema Resolution
- Support for both embedded schemas and external references
- Pluggable resolver architecture for different schema sources
- Graceful error handling for missing or invalid schemas

### 4. Full JSON Schema Draft-7 Support
- All standard JSON Schema features: types, formats, constraints
- Complex nested object and array validation
- Pattern matching, enumerations, and conditional schemas

## Testing

Comprehensive test coverage includes:

**File: `schema_validation_test.go`**
- Basic argument and output validation (success/failure cases)
- Array schema validation with complex nested structures
- Schema reference resolution testing
- Integration testing with validation coordinator
- Complex nested schema validation
- Error handling and edge cases

**Example files:**
- `examples/example_schema_usage.go` - Basic usage examples
- `examples/cartridge_sdk_example.go` - Complete cartridge SDK integration example

## Dependencies

Added dependency:
- `github.com/xeipuuv/gojsonschema v1.2.0` - JSON Schema Draft-7 validation

## Backward Compatibility

The implementation is fully backward compatible:
- Existing capabilities without schemas continue to work unchanged
- New schema features are opt-in
- All existing validation continues to work as before
- Cartridge SDK maintains all existing APIs

## Expected Functionality

After implementation, developers can:

```go
// Create capability with embedded schema
arg := sdk.NewCapArgumentWithSchema("user_data", sdk.ArgumentTypeObject, "User data", "--user", schema)

// Validation automatically checks JSON objects against schemas
validator := sdk.NewSchemaValidator()
err := validator.ValidateArgument(arg, jsonValue) // Returns detailed schema errors

// Integration with caller system
caller := registry.Can("cap:op=query;target=structured;")
response, err := caller.Call(ctx, args, namedArgs, nil) // Validates inputs and outputs
```

This implementation provides feature parity with the Rust JSON schema validation system while maintaining the Go SDK's ease of use and performance characteristics.