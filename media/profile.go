// Package media profile schema registry.
//
// Schemas are not bundled into the library — callers seed the in-memory
// cache explicitly via InsertSchema (e.g. after fetching a schema body
// from the public registry). A registry constructed via
// NewProfileSchemaRegistry is empty until populated.
package media

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/xeipuuv/gojsonschema"
)

// ProfileSchemaError represents errors from profile schema operations
type ProfileSchemaError struct {
	Message string
}

func (e *ProfileSchemaError) Error() string {
	return e.Message
}

// ProfileSchemaRegistry validates data against JSON Schema profiles.
type ProfileSchemaRegistry struct {
	mu      sync.RWMutex
	schemas map[string]*gojsonschema.Schema
}

// NewProfileSchemaRegistry creates an empty registry. Schemas must be
// added via InsertSchema before validation against them succeeds.
func NewProfileSchemaRegistry() (*ProfileSchemaRegistry, error) {
	return &ProfileSchemaRegistry{
		schemas: make(map[string]*gojsonschema.Schema),
	}, nil
}

// InsertSchema compiles a JSON Schema body and stores it under profileURL.
// Returns an error (and leaves the cache unchanged) when the body fails to
// compile — callers must not rely on a silent skip path for malformed schemas.
func (r *ProfileSchemaRegistry) InsertSchema(profileURL string, schemaJSON []byte) error {
	loader := gojsonschema.NewBytesLoader(schemaJSON)
	compiled, err := gojsonschema.NewSchema(loader)
	if err != nil {
		return &ProfileSchemaError{
			Message: fmt.Sprintf("Failed to compile schema for %s: %v", profileURL, err),
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[profileURL] = compiled
	return nil
}

// Validate validates a value against a profile schema.
// Returns nil if valid or if the schema is not in the cache; a list of
// error strings if the value is invalid. Callers that want to fail loudly
// when a schema is missing should pre-check with SchemaExists.
func (r *ProfileSchemaRegistry) Validate(profileURL string, value interface{}) []string {
	r.mu.RLock()
	schema, exists := r.schemas[profileURL]
	r.mu.RUnlock()

	if !exists {
		return nil
	}

	valueJSON, err := json.Marshal(value)
	if err != nil {
		return []string{fmt.Sprintf("Failed to marshal value: %v", err)}
	}

	loader := gojsonschema.NewBytesLoader(valueJSON)
	result, err := schema.Validate(loader)
	if err != nil {
		return []string{fmt.Sprintf("Validation error: %v", err)}
	}

	if result.Valid() {
		return nil
	}

	errors := make([]string, 0, len(result.Errors()))
	for _, e := range result.Errors() {
		errors = append(errors, e.String())
	}
	return errors
}

// ValidateCached is the same as Validate (synchronous, no HTTP fetching).
func (r *ProfileSchemaRegistry) ValidateCached(profileURL string, value interface{}) []string {
	return r.Validate(profileURL, value)
}

// SchemaExists checks if a schema is available in the registry.
func (r *ProfileSchemaRegistry) SchemaExists(profileURL string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.schemas[profileURL]
	return exists
}

// GetCachedProfiles returns all profile URLs in the registry.
func (r *ProfileSchemaRegistry) GetCachedProfiles() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	urls := make([]string, 0, len(r.schemas))
	for url := range r.schemas {
		urls = append(urls, url)
	}
	return urls
}

// ClearCache clears all schemas from the registry.
func (r *ProfileSchemaRegistry) ClearCache() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas = make(map[string]*gojsonschema.Schema)
}
