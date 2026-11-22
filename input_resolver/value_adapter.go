// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

import (
	"fmt"

	model "github.com/machinefabric/capdag-go/formal"
	"github.com/machinefabric/capdag-go/urn"
	taggedformal "github.com/machinefabric/tagged-urn-go/formal"
)

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

// ValueAdapterError says why the registry refused a key, or could not refine a
// base. Kind names which of the five it is.
type ValueAdapterError struct {
	Kind    string
	Message string
}

func (e *ValueAdapterError) Error() string { return e.Message }

// The kinds of ValueAdapterError.
const (
	// ValueAdapterBadKey: the key an adapter was registered under is no media URN.
	ValueAdapterBadKey = "bad-key"
	// ValueAdapterDuplicateKey: an adapter is registered under an equivalent key.
	ValueAdapterDuplicateKey = "duplicate-key"
	// ValueAdapterAmbiguous: the base falls under two most specific keys,
	// neither of which refines the other.
	ValueAdapterAmbiguous = "ambiguous"
	// ValueAdapterBadRefinement: the adapter's refinement is no media URN.
	ValueAdapterBadRefinement = "bad-refinement"
	// ValueAdapterNotInside: the adapter's refinement does not lie inside the base.
	ValueAdapterNotInside = "not-inside"
)

type registeredValueAdapter struct {
	key     *urn.MediaUrn
	adapter ValueAdapter
}

// ValueAdapterRegistry is a collection of value-based content inspection adapters.
//
// Each adapter is registered under a media URN, its key: the type whose values it
// can refine. A base media URN is refined by the adapter chosen by inclusion, as
// the proved model chooses (CapDAG.Detect.adapterFor): of the adapters whose key
// the base falls under, the one registered for the most specific key. Two most
// specific keys neither of which refines the other are ambiguous, and refused.
type ValueAdapterRegistry struct {
	// adapters by the key they are registered under, in registration order.
	adapters []registeredValueAdapter
}

// NewValueAdapterRegistry creates an empty registry.
func NewValueAdapterRegistry() *ValueAdapterRegistry {
	return &ValueAdapterRegistry{}
}

func parseKey(key string) (*urn.MediaUrn, error) {
	k, err := urn.NewMediaUrnFromString(key)
	if err != nil {
		return nil, &ValueAdapterError{Kind: ValueAdapterBadKey, Message: fmt.Sprintf("value adapter key '%s' is no media URN: %v", key, err)}
	}
	return k, nil
}

// Register registers a value adapter under key, the media URN of the type whose
// values it refines. A key that is no media URN, or one equivalent to a
// registered key, is refused.
func (r *ValueAdapterRegistry) Register(key string, adapter ValueAdapter) error {
	k, err := parseKey(key)
	if err != nil {
		return err
	}
	for _, existing := range r.adapters {
		if existing.key.IsEquivalent(k) {
			return &ValueAdapterError{
				Kind:    ValueAdapterDuplicateKey,
				Message: fmt.Sprintf("value adapter key '%s' is equivalent to the registered key '%s'", k, existing.key),
			}
		}
	}
	r.adapters = append(r.adapters, registeredValueAdapter{key: k, adapter: adapter})
	return nil
}

// RefineMediaUrn refines base, the media URN an argument slot declares, by the
// value filling the slot. With no adapter covering the base, or when the chosen
// adapter declines (nil), the base is kept. A refinement must be a media URN
// that conforms to the base.
func (r *ValueAdapterRegistry) RefineMediaUrn(base *urn.MediaUrn, value string) (*urn.MediaUrn, error) {
	keys := make([]taggedformal.Wf, 0, len(r.adapters))
	for _, a := range r.adapters {
		keys = append(keys, a.key.Formal())
	}
	choice, err := model.AdapterFor(keys, base.Formal())
	if err != nil {
		panic(fmt.Sprintf("capdag: the model could not choose a value adapter: %v", err))
	}
	switch c := choice.(type) {
	case model.AdapterChoiceUncovered:
		return base, nil
	case model.AdapterChoiceAmbiguous:
		return nil, &ValueAdapterError{
			Kind: ValueAdapterAmbiguous,
			Message: fmt.Sprintf("'%s' falls under the value adapter keys '%s' and '%s', neither of which refines the other",
				base, r.adapters[int(c.I.Int64())].key, r.adapters[int(c.J.Int64())].key),
		}
	case model.AdapterChoiceOne:
		adapter := r.adapters[int(c.I.Int64())].adapter
		result := adapter.Refine(base.String(), value)
		if result == nil {
			return base, nil
		}
		refined, err := urn.NewMediaUrnFromString(result.MediaUrn)
		if err != nil {
			return nil, &ValueAdapterError{
				Kind: ValueAdapterBadRefinement,
				Message: fmt.Sprintf("value adapter %s refined '%s' to '%s', which is no media URN: %v",
					adapter.Name(), base, result.MediaUrn, err),
			}
		}
		if !refined.ConformsTo(base) {
			return nil, &ValueAdapterError{
				Kind: ValueAdapterNotInside,
				Message: fmt.Sprintf("value adapter %s refined '%s' to '%s', which does not conform to it",
					adapter.Name(), base, refined),
			}
		}
		return refined, nil
	}
	panic(fmt.Sprintf("capdag: the model chose no known adapter choice: %#v", choice))
}

// HasAdapter reports whether an adapter is registered under a key equivalent to
// key. A key that is no media URN is refused.
func (r *ValueAdapterRegistry) HasAdapter(key string) (bool, error) {
	k, err := parseKey(key)
	if err != nil {
		return false, err
	}
	for _, a := range r.adapters {
		if a.key.IsEquivalent(k) {
			return true, nil
		}
	}
	return false, nil
}

// RegisteredKeys returns every registered key, in canonical form and
// registration order.
func (r *ValueAdapterRegistry) RegisteredKeys() []string {
	keys := make([]string, 0, len(r.adapters))
	for _, a := range r.adapters {
		keys = append(keys, a.key.String())
	}
	return keys
}
