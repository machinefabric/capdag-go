// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

import (
	"fmt"
	"os"

	"github.com/machinefabric/capdag-go/fabric"
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
	fabricRegistry     *fabric.FabricRegistry
}

// NewMediaAdapterRegistry creates a new empty registry with the given FabricRegistry.
// No adapters are registered by default — cartridges register them via RegisterCapGroup.
func NewMediaAdapterRegistry(fabricRegistry *fabric.FabricRegistry) *MediaAdapterRegistry {
	return &MediaAdapterRegistry{
		registeredAdapters: nil,
		fabricRegistry:     fabricRegistry,
	}
}

// FabricRegistry returns the fabric registry whose file media types the
// adapters are matched against.
func (r *MediaAdapterRegistry) FabricRegistry() *fabric.FabricRegistry {
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
// Exact re-registration is IDEMPOTENT: an adapter URN equivalent to one this
// same (cartridgeID, groupName) already registered is neither a conflict nor
// a second row — a cartridge attached through more than one hosting route
// (e.g. app-bundled and system-installed) is still one adapter provider, not
// an ambiguity with itself. The skip is logged descriptively.
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

	// Which new adapters are exact re-registrations by the same owner —
	// skipped in the conflict scan AND in the final insert, so repeated
	// attachment of the same cartridge stays a no-op instead of either
	// refusing (self-conflict) or duplicating rows.
	alreadyRegistered := make([]bool, len(newAdapters))
	for i, na := range newAdapters {
		for j := range r.registeredAdapters {
			existing := &r.registeredAdapters[j]
			if existing.cartridgeID == cartridgeID &&
				existing.groupName == groupName &&
				existing.mediaUrn.IsEquivalent(na.u) {
				alreadyRegistered[i] = true
				break
			}
		}
		if alreadyRegistered[i] {
			fmt.Fprintf(os.Stderr,
				"[WARN] Adapter URN '%s' of cap group '%s' is already registered by "+
					"cartridge '%s' — the cartridge is attached through more than one "+
					"hosting route; keeping the first registration and skipping this one\n",
				na.str, groupName, cartridgeID)
		}
	}

	// Check each new adapter against all existing registered adapters.
	for idx, na := range newAdapters {
		if alreadyRegistered[idx] {
			continue
		}
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

	// No conflicts — register atomically (re-registered URNs already have
	// their row).
	for idx, na := range newAdapters {
		if alreadyRegistered[idx] {
			continue
		}
		r.registeredAdapters = append(r.registeredAdapters, registeredAdapter{
			mediaUrn:    na.u,
			urnString:   na.str,
			groupName:   groupName,
			cartridgeID: cartridgeID,
		})
	}

	return nil
}

// AdapterMatch is one registered content-inspection adapter: the cartridge
// that registered it and its adapter media URN.
type AdapterMatch struct {
	CartridgeID string
	MediaUrn    *urn.MediaUrn
}

// extensionCandidates is the file media types the fabric declares for an
// extension, parsed.
func (r *MediaAdapterRegistry) extensionCandidates(ext string) ([]*urn.MediaUrn, error) {
	defs, err := r.fabricRegistry.FileMediaForExtension(ext)
	if err != nil {
		return nil, err
	}
	candidates := make([]*urn.MediaUrn, 0, len(defs))
	for _, def := range defs {
		u, err := urn.NewMediaUrnFromString(def.Urn)
		if err != nil {
			return nil, &fabric.Error{
				Kind:    fabric.ErrParse,
				Message: fmt.Sprintf("the fabric's media definition '%s' has no valid URN", def.Urn),
				Err:     err,
			}
		}
		candidates = append(candidates, u)
	}
	return candidates, nil
}

// FindAdaptersForExtension finds adapters that can handle candidate URNs for a
// given file extension.
//
//  1. Queries the FabricRegistry for the extension's file media types.
//  2. For each candidate, finds registered adapters where the candidate
//     ConformsTo the registered adapter URN.
//  3. Returns one match per cartridge.
//
// The fabric's file media types that cannot be loaded are an error.
func (r *MediaAdapterRegistry) FindAdaptersForExtension(ext string) ([]AdapterMatch, error) {
	candidates, err := r.extensionCandidates(ext)
	if err != nil {
		return nil, err
	}
	return r.AdaptersForCandidates(candidates), nil
}

// AdaptersForCandidates is the registered adapters some of these candidates
// conform to, one per cartridge.
func (r *MediaAdapterRegistry) AdaptersForCandidates(candidates []*urn.MediaUrn) []AdapterMatch {
	var results []AdapterMatch
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
			results = append(results, AdapterMatch{
				CartridgeID: registered.cartridgeID,
				MediaUrn:    registered.mediaUrn,
			})
		}
	}

	return results
}

// AdaptersFor is the cartridges to consult for a file whose candidates these
// are (AdapterSource): AdaptersForCandidates.
func (r *MediaAdapterRegistry) AdaptersFor(candidates []*urn.MediaUrn) []AdapterMatch {
	return r.AdaptersForCandidates(candidates)
}

// HasAdapterForExtension is a quick check for UI queries — returns true if any
// registered adapter handles candidate URNs for this extension. The fabric's
// file media types that cannot be loaded are an error.
func (r *MediaAdapterRegistry) HasAdapterForExtension(ext string) (bool, error) {
	candidates, err := r.extensionCandidates(ext)
	if err != nil {
		return false, err
	}
	for i := range r.registeredAdapters {
		registered := &r.registeredAdapters[i]
		for _, c := range candidates {
			if c.ConformsTo(registered.mediaUrn) {
				return true, nil
			}
		}
	}
	return false, nil
}
