// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

import (
	"fmt"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

// AdapterRegistrationError is returned when cap group registration fails due to
// adapter ambiguity.
type AdapterRegistrationError struct {
	// GroupName is the cap group that was rejected.
	GroupName string
	// NewAdapterUrn is the adapter URN from the new group that caused the conflict.
	NewAdapterUrn string
	// ExistingAdapterUrn is the existing adapter URN it conflicts with.
	ExistingAdapterUrn string
	// ExistingGroupName is the cap group that owns the existing adapter.
	ExistingGroupName string
	// ExistingCartridgeID is the cartridge that owns the existing adapter.
	ExistingCartridgeID string
}

// Error implements the error interface.
func (e *AdapterRegistrationError) Error() string {
	return fmt.Sprintf(
		"Cap group '%s' rejected: adapter URN '%s' conflicts with '%s' "+
			"(registered by group '%s' in cartridge '%s'). "+
			"One conforms to the other, creating ambiguity.",
		e.GroupName,
		e.NewAdapterUrn,
		e.ExistingAdapterUrn,
		e.ExistingGroupName,
		e.ExistingCartridgeID,
	)
}

// registeredAdapter is a registered adapter URN with its owning group and cartridge.
type registeredAdapter struct {
	mediaUrn    *urn.MediaUrn
	urnString   string
	groupName   string
	cartridgeID string
}

// MediaAdapterRegistry tracks cartridge-provided content inspection adapters.
//
// This registry:
//  1. Tracks which cartridges have registered adapter URNs.
//  2. Detects ambiguity at registration time (rejects entire cap groups).
//  3. Maps file extensions to cartridges that can inspect them.
type MediaAdapterRegistry struct {
	registeredAdapters []registeredAdapter
	fabricRegistry     *media.FabricRegistry
}

// NewMediaAdapterRegistry creates a new empty registry with the given FabricRegistry.
// No adapters are registered by default — cartridges register them via RegisterCapGroup.
func NewMediaAdapterRegistry(fabricRegistry *media.FabricRegistry) *MediaAdapterRegistry {
	return &MediaAdapterRegistry{
		registeredAdapters: nil,
		fabricRegistry:     fabricRegistry,
	}
}

// FabricRegistry returns the media URN registry.
func (r *MediaAdapterRegistry) FabricRegistry() *media.FabricRegistry {
	return r.fabricRegistry
}

// RegisteredCount returns the number of registered adapters (for tests/diagnostics).
func (r *MediaAdapterRegistry) RegisteredCount() int {
	return len(r.registeredAdapters)
}

// RegisterCapGroup registers a cap group's adapter URNs.
//
// Checks each new adapter URN against ALL existing registered URNs. If any pair
// has a ConformsTo relationship in either direction, the entire group is
// rejected — none of its adapters get registered. On success, all adapter URNs
// from the group are added atomically.
//
// Invalid URN strings are a hard error (panic), mirroring the Rust reference.
func (r *MediaAdapterRegistry) RegisterCapGroup(groupName string, adapterUrnStrs []string, cartridgeID string) error {
	// Parse all new adapter URNs first — fail hard on invalid URNs.
	type parsed struct {
		u   *urn.MediaUrn
		str string
	}
	newAdapters := make([]parsed, 0, len(adapterUrnStrs))
	for _, s := range adapterUrnStrs {
		u, err := urn.NewMediaUrnFromString(s)
		if err != nil {
			panic(fmt.Sprintf("Cap group '%s' has invalid adapter URN '%s': %v", groupName, s, err))
		}
		newAdapters = append(newAdapters, parsed{u: u, str: s})
	}

	// Check each new adapter against all existing registered adapters.
	for _, na := range newAdapters {
		for i := range r.registeredAdapters {
			existing := &r.registeredAdapters[i]
			newConformsToExisting := na.u.ConformsTo(existing.mediaUrn)
			existingConformsToNew := existing.mediaUrn.ConformsTo(na.u)

			if newConformsToExisting || existingConformsToNew {
				return &AdapterRegistrationError{
					GroupName:           groupName,
					NewAdapterUrn:       na.str,
					ExistingAdapterUrn:  existing.urnString,
					ExistingGroupName:   existing.groupName,
					ExistingCartridgeID: existing.cartridgeID,
				}
			}
		}
	}

	// Also check new adapters against each other within the same group.
	for i := 0; i < len(newAdapters); i++ {
		for j := i + 1; j < len(newAdapters); j++ {
			a := newAdapters[i]
			b := newAdapters[j]
			aConformsToB := a.u.ConformsTo(b.u)
			bConformsToA := b.u.ConformsTo(a.u)

			if aConformsToB || bConformsToA {
				return &AdapterRegistrationError{
					GroupName:           groupName,
					NewAdapterUrn:       a.str,
					ExistingAdapterUrn:  b.str,
					ExistingGroupName:   groupName,
					ExistingCartridgeID: cartridgeID,
				}
			}
		}
	}

	// No conflicts — register atomically.
	for _, na := range newAdapters {
		r.registeredAdapters = append(r.registeredAdapters, registeredAdapter{
			mediaUrn:    na.u,
			urnString:   na.str,
			groupName:   groupName,
			cartridgeID: cartridgeID,
		})
	}

	return nil
}

// adapterMatch is a (cartridgeID, adapterMediaUrn) pair.
type adapterMatch struct {
	CartridgeID string
	MediaUrn    *urn.MediaUrn
}

// FindAdaptersForExtension finds adapters that can handle candidate URNs for a
// given file extension.
//
//  1. Queries FabricRegistry for candidate URNs via extension.
//  2. For each candidate, finds registered adapters where the candidate
//     ConformsTo the registered adapter URN.
//  3. Returns (cartridgeID, adapterMediaUrn) pairs.
func (r *MediaAdapterRegistry) FindAdaptersForExtension(ext string) []adapterMatch {
	candidateStrings, err := r.fabricRegistry.MediaUrnsForExtension(ext)
	if err != nil || len(candidateStrings) == 0 {
		return nil
	}

	var candidates []*urn.MediaUrn
	for _, s := range candidateStrings {
		if u, err := urn.NewMediaUrnFromString(s); err == nil {
			candidates = append(candidates, u)
		}
	}

	var results []adapterMatch
	seenCartridges := make(map[string]bool)

	for i := range r.registeredAdapters {
		registered := &r.registeredAdapters[i]
		matches := false
		for _, c := range candidates {
			if c.ConformsTo(registered.mediaUrn) {
				matches = true
				break
			}
		}
		if matches && !seenCartridges[registered.cartridgeID] {
			seenCartridges[registered.cartridgeID] = true
			results = append(results, adapterMatch{
				CartridgeID: registered.cartridgeID,
				MediaUrn:    registered.mediaUrn,
			})
		}
	}

	return results
}

// HasAdapterForExtension is a quick check for UI queries — returns true if any
// registered adapter handles candidate URNs for this extension.
func (r *MediaAdapterRegistry) HasAdapterForExtension(ext string) bool {
	candidateStrings, err := r.fabricRegistry.MediaUrnsForExtension(ext)
	if err != nil || len(candidateStrings) == 0 {
		return false
	}

	var candidates []*urn.MediaUrn
	for _, s := range candidateStrings {
		if u, err := urn.NewMediaUrnFromString(s); err == nil {
			candidates = append(candidates, u)
		}
	}

	for i := range r.registeredAdapters {
		registered := &r.registeredAdapters[i]
		for _, c := range candidates {
			if c.ConformsTo(registered.mediaUrn) {
				return true
			}
		}
	}
	return false
}
