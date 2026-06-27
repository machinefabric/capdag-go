// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

import "strings"

// ValueAdapterResult is the result of value-based content inspection.
type ValueAdapterResult struct {
	// MediaUrn is the refined media URN with additional marker tags.
	MediaUrn string
}

// ValueAdapter is the interface for value-based content inspection adapters.
//
// Implementations inspect string argument values to refine a base media URN.
// This follows the same content-inspection pattern as MediaAdapter, but
// operates on string values rather than file paths and byte content.
type ValueAdapter interface {
	// Name returns a unique name for this adapter (for debugging/logging).
	Name() string

	// Refine refines a base media URN based on the value filling the argument slot.
	//
	// Returns a non-nil *ValueAdapterResult if this adapter can refine the URN,
	// or nil if this adapter does not handle this base URN / value.
	Refine(baseMediaUrn, value string) *ValueAdapterResult
}

// ValueAdapterRegistry is a collection of value-based content inspection adapters.
//
// Adapters are registered by a base URN key. When RefineMediaUrn is called,
// the registry finds the adapter whose key is a prefix of the base media URN
// and delegates refinement to it.
type ValueAdapterRegistry struct {
	// adapters indexed by base URN prefix they handle.
	adapters map[string]ValueAdapter
}

// NewValueAdapterRegistry creates an empty registry.
func NewValueAdapterRegistry() *ValueAdapterRegistry {
	return &ValueAdapterRegistry{
		adapters: make(map[string]ValueAdapter),
	}
}

// Register registers a value adapter for a base URN prefix.
//
// The key should be the shortest URN prefix that uniquely identifies the
// domain this adapter handles (e.g., "media:model-spec" for all model-spec URNs).
func (r *ValueAdapterRegistry) Register(baseUrnPrefix string, adapter ValueAdapter) {
	r.adapters[baseUrnPrefix] = adapter
}

// RefineMediaUrn refines a media URN based on the value filling an argument slot.
//
// Finds the adapter whose registered prefix matches the baseMediaUrn (longest
// prefix wins), calls its Refine method, and returns the refined URN.
//
// If no adapter matches or the adapter declines to refine (returns nil),
// returns the baseMediaUrn unchanged.
func (r *ValueAdapterRegistry) RefineMediaUrn(baseMediaUrn, value string) string {
	// Find the adapter with the longest matching prefix.
	var bestPrefix string
	var bestAdapter ValueAdapter
	found := false

	for prefix, adapter := range r.adapters {
		if strings.HasPrefix(baseMediaUrn, prefix) {
			if !found || len(prefix) > len(bestPrefix) {
				bestPrefix = prefix
				bestAdapter = adapter
				found = true
			}
		}
	}

	if !found {
		return baseMediaUrn
	}

	if result := bestAdapter.Refine(baseMediaUrn, value); result != nil {
		return result.MediaUrn
	}
	return baseMediaUrn
}

// HasAdapter reports whether an adapter exists for the given base URN prefix.
func (r *ValueAdapterRegistry) HasAdapter(baseUrnPrefix string) bool {
	_, ok := r.adapters[baseUrnPrefix]
	return ok
}

// RegisteredPrefixes returns all registered adapter prefixes.
func (r *ValueAdapterRegistry) RegisteredPrefixes() []string {
	prefixes := make([]string, 0, len(r.adapters))
	for prefix := range r.adapters {
		prefixes = append(prefixes, prefix)
	}
	return prefixes
}
