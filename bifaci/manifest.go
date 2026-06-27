// Package bifaci provides the unified cap-based manifest interface
package bifaci

import (
	"encoding/json"
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

// CapManifest represents unified cap manifest for --manifest output.
//
// `(Name, Version, Channel, RegistryURL)` is the cartridge's full
// identity. Channel and RegistryURL are baked in at compile time
// — channel via -ldflags '-X main.cartridgeChannel=…' and
// registry URL via -ldflags '-X main.cartridgeRegistryURL=…'.
// RegistryURL is `*string` (nullable): nil ⇔ dev build (the
// cartridge can only be installed under the `dev/` slot).
type CapManifest struct {
	// Component name
	Name string `json:"name"`

	// Component version
	Version string `json:"version"`

	// Distribution channel ("release" or "nightly"). Required.
	// Channels are independent namespaces — release v1.0.0 and
	// nightly v1.0.0 are distinct artifacts that share id+version
	// strings.
	Channel string `json:"channel"`

	// RegistryURL — verbatim URL of the registry the cartridge
	// was built for. nil ⇔ dev build. The JSON field is required-
	// but-nullable: the encoder always emits it (never elides for
	// nil) and the decoder rejects missing keys, surfacing
	// old-schema cartridges as parse errors.
	RegistryURL *string `json:"registry_url"`

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

// UnmarshalJSON deserializes a CapManifest, enforcing the stricter
// wire contract Rust's manual Deserialize impl enforces (capdag/src/
// bifaci/manifest.rs):
//
//   - `channel` is REQUIRED. A missing key is a hard parse error — Go's
//     stock encoding/json would silently leave it the zero value, so the
//     presence check is explicit. The value must be the canonical
//     lowercase wire word ("release" / "nightly"); anything else (e.g.
//     "staging") is rejected via CartridgeChannel's UnmarshalJSON. No
//     defaults.
//   - `registry_url` is REQUIRED-BUT-NULLABLE: the key MUST be present,
//     the value MAY be null. present-and-null means dev build; an absent
//     key means the cartridge SDK is too old to know the field exists,
//     which is a parse error. This is the null-vs-absent distinction
//     stock decoders collapse for pointer fields.
//   - `author` and `page_url` remain optional (absent -> nil).
func (cm *CapManifest) UnmarshalJSON(data []byte) error {
	// First pass: a presence map so we can distinguish absent keys from
	// explicit nulls — encoding/json does not surface this otherwise.
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(data, &keys); err != nil {
		return err
	}
	if _, ok := keys["channel"]; !ok {
		return fmt.Errorf("missing field `channel`")
	}
	if _, ok := keys["registry_url"]; !ok {
		return fmt.Errorf("missing field `registry_url`")
	}

	type rawManifest struct {
		Name        string           `json:"name"`
		Version     string           `json:"version"`
		Channel     CartridgeChannel `json:"channel"`
		RegistryURL *string          `json:"registry_url"`
		Description string           `json:"description"`
		CapGroups   []CapGroup       `json:"cap_groups"`
		Author      *string          `json:"author"`
		PageUrl     *string          `json:"page_url"`
	}
	var raw rawManifest
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	cm.Name = raw.Name
	cm.Version = raw.Version
	cm.Channel = string(raw.Channel)
	cm.RegistryURL = raw.RegistryURL
	cm.Description = raw.Description
	cm.CapGroups = raw.CapGroups
	cm.Author = raw.Author
	cm.PageUrl = raw.PageUrl
	return nil
}

// NewCapManifest creates a new cap manifest with cap groups.
// `channel` is required — every cartridge declares which channel it
// was built for so the host can verify the install context
// (cartridge.json) matches the cartridge's self-report.
// `registryURL` is `*string` — pass nil for dev builds; pass a
// pointer to the URL string for cartridges built for a specific
// registry (mirror of Rust's `option_env!("MFR_CARTRIDGE_REGISTRY_URL")`).
func NewCapManifest(name, version, channel string, registryURL *string, description string, capGroups []CapGroup) *CapManifest {
	return &CapManifest{
		Name:        name,
		Version:     version,
		Channel:     channel,
		RegistryURL: registryURL,
		Description: description,
		CapGroups:   capGroups,
	}
}

// RegistryURLFromBuildEnv validates a build-time cartridge registry URL,
// mirroring Rust's `registry_url_from_build_env` (capdag/src/bifaci/manifest.rs).
//
// It encodes the registry-identity contract a cartridge or engine bakes in at
// build time. The argument is the optional build-env value (mirror of Rust's
// `option_env!("MFR_CARTRIDGE_REGISTRY_URL")` — `*string` here stands in for
// `Option<&str>`):
//
//   - raw == nil               => dev build. Registry identity is absent; the
//     build uses the on-disk `dev/` slot. Returns nil.
//   - raw != nil, *raw != ""   => published-registry build. Returns raw unchanged.
//   - raw != nil, *raw == ""   => INVALID. The variable was exported with an
//     empty value — neither a dev build nor a valid
//     registry identity. This MUST fail hard so the
//     build cannot silently hash the empty string
//     into a fake registry slug. Panics.
//
// Failing hard on the empty-but-set case is deliberate: a fallback that treated
// "" as dev would hide a real build-script bug. Callers that genuinely mean a
// dev build pass nil, never a pointer to "".
func RegistryURLFromBuildEnv(raw *string) *string {
	if raw == nil {
		return nil
	}
	if *raw == "" {
		panic("MFR_CARTRIDGE_REGISTRY_URL must be unset for dev builds or set to a non-empty registry URL for published builds; empty string is invalid")
	}
	return raw
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
