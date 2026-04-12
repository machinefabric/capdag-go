// Package bifaci provides the unified cap-based manifest interface
package bifaci

import (
	"fmt"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// CapManifest represents unified cap manifest for --manifest output
type CapManifest struct {
	// Component name
	Name string `json:"name"`

	// Component version
	Version string `json:"version"`

	// Component description
	Description string `json:"description"`

	// Component caps with formal definitions
	Caps []cap.Cap `json:"caps"`

	// Component author/maintainer
	Author *string `json:"author,omitempty"`

	// Human-readable page URL for the cartridge (e.g., repository page, documentation)
	PageUrl *string `json:"page_url,omitempty"`
}

// NewCapManifest creates a new cap manifest
func NewCapManifest(name, version, description string, caps []cap.Cap) *CapManifest {
	return &CapManifest{
		Name:        name,
		Version:     version,
		Description: description,
		Caps:        caps,
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
// Returns error if missing — identity is mandatory in every capset.
func (cm *CapManifest) Validate() error {
	identityUrn, err := urn.NewCapUrnFromString(standard.CapIdentity)
	if err != nil {
		return fmt.Errorf("BUG: CAP_IDENTITY constant is invalid: %v", err)
	}

	for _, c := range cm.Caps {
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

	for _, cap := range cm.Caps {
		if cap.Urn != nil && cap.Urn.Equals(identityUrn) {
			return cm // Already has identity
		}
	}

	// Add identity cap
	identityCap := cap.NewCap(identityUrn, "Identity", "identity")
	newCaps := make([]cap.Cap, 0, len(cm.Caps)+1)
	newCaps = append(newCaps, *identityCap)
	newCaps = append(newCaps, cm.Caps...)

	return &CapManifest{
		Name:        cm.Name,
		Version:     cm.Version,
		Description: cm.Description,
		Caps:        newCaps,
		Author:      cm.Author,
		PageUrl:     cm.PageUrl,
	}
}

// ComponentMetadata interface for components to provide metadata about themselves
type ComponentMetadata interface {
	// ComponentManifest returns the component manifest
	ComponentManifest() *CapManifest

	// Caps returns the component caps
	Caps() []cap.Cap
}
