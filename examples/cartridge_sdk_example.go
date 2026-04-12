// Example demonstrating schema validation with caps
package main

import (
	"fmt"

	capdag "github.com/machfab/cap-sdk-go"
)

func main() {
	fmt.Println("=== Schema Integration Example ===")

	// Create a capability
	urn, _ := capdag.NewCapUrnFromString(`cap:in="media:void";op=query;out="media:record;textable";target=structured`)
	cap := capdag.NewCap(urn, "Query Command", "query-command")

	// Define a comprehensive schema for document query parameters
	querySchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"search_terms": map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type":      "string",
					"minLength": 1,
				},
				"minItems": 1,
				"maxItems": 10,
			},
			"filters": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"file_types": map[string]interface{}{
						"type": "array",
						"items": map[string]interface{}{
							"type": "string",
							"enum": []interface{}{"pdf", "docx", "txt", "md"},
						},
					},
					"date_range": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"start": map[string]interface{}{"type": "string", "format": "date"},
							"end":   map[string]interface{}{"type": "string", "format": "date"},
						},
					},
					"max_results": map[string]interface{}{
						"type":    "integer",
						"minimum": 1,
						"maximum": 1000,
						"default": 50,
					},
				},
			},
		},
		"required": []interface{}{"search_terms"},
	}

	// Add a custom media spec with the schema
	cap.AddMediaSpec("media:query-params;textable;record", capdag.NewMediaSpecDefObjectWithSchema(
		"application/json",
		"https://example.com/schema/query-params",
		querySchema,
	))

	// Add schema-enabled argument using new CapArg architecture
	cliFlag := "--query"
	pos := 0
	cap.AddArg(capdag.CapArg{
		MediaUrn:       "media:query-params;textable;record",
		Required:       true,
		Sources:        []capdag.ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: "Document query parameters",
	})

	// Define output schema
	resultSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"documents": map[string]interface{}{
				"type": "array",
				"items": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"id":        map[string]interface{}{"type": "string"},
						"title":     map[string]interface{}{"type": "string"},
						"path":      map[string]interface{}{"type": "string"},
						"relevance": map[string]interface{}{"type": "number", "minimum": 0, "maximum": 1},
						"metadata": map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"file_type":   map[string]interface{}{"type": "string"},
								"size":        map[string]interface{}{"type": "integer"},
								"created_at":  map[string]interface{}{"type": "string", "format": "date-time"},
								"modified_at": map[string]interface{}{"type": "string", "format": "date-time"},
							},
						},
					},
					"required": []interface{}{"id", "title", "path", "relevance"},
				},
			},
			"total_found": map[string]interface{}{"type": "integer", "minimum": 0},
			"query_time":  map[string]interface{}{"type": "number", "minimum": 0},
			"pagination": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"page":        map[string]interface{}{"type": "integer", "minimum": 1},
					"page_size":   map[string]interface{}{"type": "integer", "minimum": 1},
					"total_pages": map[string]interface{}{"type": "integer", "minimum": 0},
				},
			},
		},
		"required": []interface{}{"documents", "total_found", "query_time"},
	}

	// Add custom media spec for output with schema
	cap.AddMediaSpec("media:query-results;textable;record", capdag.NewMediaSpecDefObjectWithSchema(
		"application/json",
		"https://example.com/schema/query-results",
		resultSchema,
	))

	// Set output
	cap.SetOutput(capdag.NewCapOutput("media:query-results;textable;record", "Document search results"))

	// Create validation coordinator
	coordinator := capdag.NewCapValidationCoordinator()
	coordinator.RegisterCap(cap)

	// Test with valid input
	fmt.Println("\n--- Testing Valid Input ---")
	validQuery := map[string]interface{}{
		"search_terms": []interface{}{"machine learning", "neural networks"},
		"filters": map[string]interface{}{
			"file_types":  []interface{}{"pdf", "docx"},
			"max_results": 25,
		},
	}

	err := coordinator.ValidateInputs(cap.UrnString(), []interface{}{validQuery})
	if err != nil {
		fmt.Printf("ERR Valid input validation failed: %v\n", err)
	} else {
		fmt.Printf("OK Valid input passed validation\n")
	}

	// Test with invalid input
	fmt.Println("\n--- Testing Invalid Input ---")
	invalidQuery := map[string]interface{}{
		"search_terms": []interface{}{}, // Empty array violates minItems
		"filters": map[string]interface{}{
			"file_types":  []interface{}{"invalid_type"}, // Invalid enum value
			"max_results": 2000,                          // Exceeds maximum
		},
	}

	err = coordinator.ValidateInputs(cap.UrnString(), []interface{}{invalidQuery})
	if err != nil {
		fmt.Printf("OK Invalid input correctly rejected:\n%v\n", err)
	} else {
		fmt.Printf("ERR Invalid input incorrectly accepted\n")
	}

	// Test with valid output
	fmt.Println("\n--- Testing Valid Output ---")
	validResult := map[string]interface{}{
		"documents": []interface{}{
			map[string]interface{}{
				"id":        "doc_123",
				"title":     "Introduction to Machine Learning",
				"path":      "/documents/ml_intro.pdf",
				"relevance": 0.95,
				"metadata": map[string]interface{}{
					"file_type":   "pdf",
					"size":        1024000,
					"created_at":  "2023-01-01T10:00:00Z",
					"modified_at": "2023-01-02T15:30:00Z",
				},
			},
			map[string]interface{}{
				"id":        "doc_456",
				"title":     "Neural Network Fundamentals",
				"path":      "/documents/nn_fundamentals.docx",
				"relevance": 0.87,
				"metadata": map[string]interface{}{
					"file_type":   "docx",
					"size":        512000,
					"created_at":  "2023-02-15T09:00:00Z",
					"modified_at": "2023-02-16T14:20:00Z",
				},
			},
		},
		"total_found": 42,
		"query_time":  0.125,
		"pagination": map[string]interface{}{
			"page":        1,
			"page_size":   25,
			"total_pages": 2,
		},
	}

	err = coordinator.ValidateOutput(cap.UrnString(), validResult)
	if err != nil {
		fmt.Printf("ERR Valid output validation failed: %v\n", err)
	} else {
		fmt.Printf("OK Valid output passed validation\n")
	}

	// Test with invalid output
	fmt.Println("\n--- Testing Invalid Output ---")
	invalidResult := map[string]interface{}{
		"documents": []interface{}{
			map[string]interface{}{
				"id":    "doc_123",
				"title": "Introduction to Machine Learning",
				// Missing required "path" and "relevance" fields
			},
		},
		"total_found": -5,   // Negative value violates minimum constraint
		"query_time":  -0.1, // Negative value violates minimum constraint
		// Missing required fields
	}

	err = coordinator.ValidateOutput(cap.UrnString(), invalidResult)
	if err != nil {
		fmt.Printf("OK Invalid output correctly rejected:\n%v\n", err)
	} else {
		fmt.Printf("ERR Invalid output incorrectly accepted\n")
	}

	// Demonstrate schema resolver functionality
	fmt.Println("\n--- Testing Schema Resolver ---")
	resolver := capdag.NewFileSchemaResolver("/schema/base/path")
	coordinatorWithResolver := capdag.NewCapValidationCoordinatorWithSchemaResolver(resolver)

	// Register the cap with resolver
	coordinatorWithResolver.RegisterCap(cap)

	// Since we don't have an actual file, this demonstrates the resolver pattern
	fmt.Println("Schema resolver configured with base path: /schema/base/path")

	fmt.Println("\n=== Schema integration examples completed! ===")
}
