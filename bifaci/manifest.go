// Package bifaci provides the unified cap-based manifest interface
package bifaci

import (
	"fmt"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// CapGroup bundles caps and adapter URNs as an atomic registration unit.
//
// If any adapter in the group creates ambiguity with an already-registered adapter,
// the entire group is rejected — none of its caps or adapters get registered.
type CapGroup struct {
	// Group name (for diagnostics and error messages)
	Name string `json:"name"`

	// Caps in this group
	Caps []cap.Cap `json:"caps"`

	// Media URNs this group's adapter handles.
	// These are matched via conforms_to during registration — they are not patterns,
	// they are declared URNs checked for overlap with existing registrations.
	AdapterUrns []string `json:"adapter_urns,omitempty"`
}

// CapManifest represents unified cap manifest for --manifest output
type CapManifest struct {
	// Component name
	Name string `json:"name"`

	// Component version
	Version string `json:"version"`

	// Component description
	Description string `json:"description"`

	// Cap groups — bundles of caps + adapter URNs registered atomically.
	// All caps must be in a cap group. Groups without adapter URNs are valid
	// (they just don't contribute content inspection adapters).
	CapGroups []CapGroup `json:"cap_groups"`

	// Component author/maintainer
	Author *string `json:"author,omitempty"`

	// Human-readable page URL for the cartridge (e.g., repository page, documentation)
	PageUrl *string `json:"page_url,omitempty"`
}

// NewCapManifest creates a new cap manifest with cap groups
func NewCapManifest(name, version, description string, capGroups []CapGroup) *CapManifest {
	return &CapManifest{
		Name:        name,
		Version:     version,
		Description: description,
		CapGroups:   capGroups,
	}
}

// AllCaps returns all caps from all cap groups.
func (cm *CapManifest) AllCaps() []cap.Cap {
	var all []cap.Cap
	for _, group := range cm.CapGroups {
		all = append(all, group.Caps...)
	}
	return all
}

// DefaultGroup wraps caps in a cap group with no adapter URNs.
func DefaultGroup(caps []cap.Cap) CapGroup {
	return CapGroup{
		Name: "default",
		Caps: caps,
	}
}

// WithAuthor sets the author of the component
func (cm *CapManifest) WithAuthor(author string) *CapManifest {
	cm.Author = &author
	return cm
}

// WithPageUrl sets the page URL for the cartridge (human-readable page, e.g., repository)
func (cm *CapManifest) WithPageUrl(pageUrl string) *CapManifest {
	cm.PageUrl = &pageUrl
	return cm
}

// Validate checks that CAP_IDENTITY is declared in this manifest.
// Checks caps within cap_groups.
// Returns error if missing — identity is mandatory in every capset.
func (cm *CapManifest) Validate() error {
	identityUrn, err := urn.NewCapUrnFromString(standard.CapIdentity)
	if err != nil {
		return fmt.Errorf("BUG: CAP_IDENTITY constant is invalid: %v", err)
	}

	for _, c := range cm.AllCaps() {
		if c.Urn != nil && identityUrn.ConformsTo(c.Urn) {
			return nil
		}
	}

	return fmt.Errorf("Manifest missing required CAP_IDENTITY (%s)", standard.CapIdentity)
}

// EnsureIdentity ensures the manifest includes CAP_IDENTITY
// Returns a new manifest with identity added if not present, or the same manifest if already present
func (cm *CapManifest) EnsureIdentity() *CapManifest {
	// Check if identity already present
	identityUrn, err := urn.NewCapUrnFromString(standard.CapIdentity)
	if err != nil {
		panic("CAP_IDENTITY constant is invalid")
	}

	for _, c := range cm.AllCaps() {
		if c.Urn != nil && c.Urn.Equals(identityUrn) {
			return cm // Already has identity
		}
	}

	// Add identity cap in a default group
	identityCap := cap.NewCap(identityUrn, "Identity", "identity")
	newGroups := make([]CapGroup, 0, len(cm.CapGroups)+1)
	newGroups = append(newGroups, DefaultGroup([]cap.Cap{*identityCap}))
	newGroups = append(newGroups, cm.CapGroups...)

	return &CapManifest{
		Name:        cm.Name,
		Version:     cm.Version,
		Description: cm.Description,
		CapGroups:   newGroups,
		Author:      cm.Author,
		PageUrl:     cm.PageUrl,
	}
}

// ComponentMetadata interface for components to provide metadata about themselves
type ComponentMetadata interface {
	// ComponentManifest returns the component manifest
	ComponentManifest() *CapManifest

	// Caps returns all component caps from all cap groups
	Caps() []cap.Cap
}
