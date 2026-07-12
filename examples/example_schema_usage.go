// Example demonstrating schema validation usage
// This file shows how to use the comprehensive JSON schema validation system
package main

import (
	"fmt"

	capdag "github.com/machfab/cap-sdk-go"
)

func main() {
	// Example 1: Create capability with embedded schema
	fmt.Println("=== Example 1: Basic Schema Validation ===")

	urn, _ := capdag.NewCapUrnFromString(`cap:in="media:void";query;out="media:enc=utf-8;record";target=structured`)
	cap := capdag.NewCap(urn, "Query Command", []string{"query-command"})

	// Define JSON schema for user data
	userSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"name": map[string]interface{}{
				"type":      "string",
				"minLength": 2,
			},
			"age": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
				"maximum": 150,
			},
			"email": map[string]interface{}{
				"type":   "string",
				"format": "email",
			},
		},
		"required": []interface{}{"name", "age"},
	}

	// Add custom media def with schema
	// Add argument with schema using new CapArg architecture
	cliFlag := "--user"
	pos := 0
	userArg := capdag.CapArg{
		MediaUrn:       "media:enc=utf-8;user;record",
		Required:       true,
		Sources:        []capdag.ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: capdag.StringPtr("User data"),
	}
	cap.AddArg(userArg)

	// Create validator and test
	validator := capdag.NewSchemaValidator()

	// Valid data
	validUser := map[string]interface{}{
		"name":  "John Doe",
		"age":   30,
		"email": "john@example.com",
	}

	// Get registry for resolving media URNs
	registry, err := capdag.NewFabricRegistry()
	if err != nil {
		fmt.Printf("ERR Failed to create media URN registry: %v\n", err)
		return
	}

	// Resolve the arg and validate
	args := cap.GetArgs()
	if len(args) > 0 {
		resolved, _ := args[0].Resolve(registry)
		if resolved != nil && resolved.Schema != nil {
			err := validator.ValidateArgumentWithSchema(&args[0], resolved.Schema, validUser)
			if err != nil {
				fmt.Printf("ERR Validation failed: %v\n", err)
			} else {
				fmt.Printf("OK Valid data passed validation\n")
			}
		}
	}

	// Invalid data
	invalidUser := map[string]interface{}{
		"name": "A", // Too short
		"age":  -5,  // Negative age
	}

	if len(args) > 0 {
		resolved, _ := args[0].Resolve(registry)
		if resolved != nil && resolved.Schema != nil {
			err := validator.ValidateArgumentWithSchema(&args[0], resolved.Schema, invalidUser)
			if err != nil {
				fmt.Printf("OK Invalid data correctly rejected: %v\n", err)
			} else {
				fmt.Printf("ERR Invalid data incorrectly accepted\n")
			}
		}
	}

	// Example 2: Output validation
	fmt.Println("\n=== Example 2: Output Schema Validation ===")

	outputSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"status": map[string]interface{}{
				"type": "string",
				"enum": []interface{}{"success", "error", "pending"},
			},
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
			"total": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
			},
		},
		"required": []interface{}{"status", "total"},
	}

	// Add custom media def for output with schema
	output := capdag.NewCapOutput("media:enc=utf-8;query-result;record", "Query results")
	cap.SetOutput(output)

	// Valid output
	validOutput := map[string]interface{}{
		"status": "success",
		"results": []interface{}{
			map[string]interface{}{"id": 1, "title": "Result 1"},
			map[string]interface{}{"id": 2, "title": "Result 2"},
		},
		"total": 2,
	}

	// Resolve output and validate
	if cap.Output != nil {
		resolved, _ := cap.Output.Resolve(registry)
		if resolved != nil && resolved.Schema != nil {
			err := validator.ValidateOutputWithSchema(cap.Output, resolved.Schema, validOutput)
			if err != nil {
				fmt.Printf("ERR Output validation failed: %v\n", err)
			} else {
				fmt.Printf("OK Valid output passed validation\n")
			}
		}
	}

	// Example 3: Integration with CapValidationCoordinator
	fmt.Println("\n=== Example 3: Full Integration ===")

	coordinator := capdag.NewCapValidationCoordinator()
	coordinator.RegisterCap(cap)

	// Test input validation through coordinator
	positionalArgs := []interface{}{validUser}
	err := coordinator.ValidateInputs(cap.UrnString(), positionalArgs)
	if err != nil {
		fmt.Printf("ERR Coordinator input validation failed: %v\n", err)
	} else {
		fmt.Printf("OK Coordinator input validation passed\n")
	}

	// Test output validation through coordinator
	err = coordinator.ValidateOutput(cap.UrnString(), validOutput)
	if err != nil {
		fmt.Printf("ERR Coordinator output validation failed: %v\n", err)
	} else {
		fmt.Printf("OK Coordinator output validation passed\n")
	}

	// Example 4: Array schema validation
	fmt.Println("\n=== Example 4: Array Schema Validation ===")

	arraySchema := map[string]interface{}{
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
		"maxItems": 10,
	}

	// Add custom media def for array with schema
	cliFlag2 := "--items"
	pos2 := 1
	itemsArg := capdag.CapArg{
		MediaUrn:       "media:enc=utf-8;items;record",
		Required:       false,
		Sources:        []capdag.ArgSource{{CliFlag: &cliFlag2}, {Position: &pos2}},
		ArgDescription: capdag.StringPtr("List of items"),
	}

	validArray := []interface{}{
		map[string]interface{}{"id": 1, "name": "Item 1"},
		map[string]interface{}{"id": 2, "name": "Item 2"},
	}

	resolved, _ := itemsArg.Resolve(registry)
	if resolved != nil && resolved.Schema != nil {
		err = validator.ValidateArgumentWithSchema(&itemsArg, resolved.Schema, validArray)
		if err != nil {
			fmt.Printf("ERR Array validation failed: %v\n", err)
		} else {
			fmt.Printf("OK Valid array passed validation\n")
		}
	}

	fmt.Println("\n=== Schema validation examples completed successfully! ===")
}
