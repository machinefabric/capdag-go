package cap

import (
	"fmt"
	"os"

	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

const (
	DefaultRegistryBaseURL = "https://fabric.capdag.com"
	CacheDurationHours     = 24
	HTTPTimeoutSeconds     = 10
)

// RegistryConfig holds configuration for the registry client
type RegistryConfig struct {
	RegistryBaseURL string
	SchemaBaseURL   string
}

// DefaultRegistryConfig returns config from environment variables or defaults
//
// Environment variables:
//   - CDG_FABRIC_REGISTRY_URL: Base URL for the registry (default: https://fabric.capdag.com)
//   - CDG_SCHEMA_BASE_URL: Base URL for schemas (default: {registry_url}/schema)
func DefaultRegistryConfig() RegistryConfig {
	registryBase := os.Getenv("CDG_FABRIC_REGISTRY_URL")
	if registryBase == "" {
		registryBase = DefaultRegistryBaseURL
	}

	schemaBase := os.Getenv("CDG_SCHEMA_BASE_URL")
	if schemaBase == "" {
		schemaBase = registryBase + "/schema"
	}

	return RegistryConfig{
		RegistryBaseURL: registryBase,
		SchemaBaseURL:   schemaBase,
	}
}

// RegistryOption is a functional option for configuring the registry
type RegistryOption func(*RegistryConfig)

// WithRegistryURL sets a custom registry URL
func WithRegistryURL(url string) RegistryOption {
	return func(c *RegistryConfig) {
		// If schema URL was derived from the old registry URL, update it
		if c.SchemaBaseURL == c.RegistryBaseURL+"/schema" {
			c.SchemaBaseURL = url + "/schema"
		}
		c.RegistryBaseURL = url
	}
}

// WithSchemaURL sets a custom schema base URL
func WithSchemaURL(url string) RegistryOption {
	return func(c *RegistryConfig) {
		c.SchemaBaseURL = url
	}
}

// RegistryCapResponse represents the per-cap JSON body served at
// /caps/<sha256>. The wire shape is the flattened cap entry; fields
// beyond what we explicitly model below (urn_tags, in_spec, out_spec,
// in_media_title, out_media_title, media_defs, registered_by,
// documentation) are silently ignored by Go's JSON unmarshaller — the
// Go Cap type only carries the subset below.
type RegistryCapResponse struct {
	Urn            string            `json:"urn"` // URN in canonical string format
	Title          string            `json:"title"`
	Version        string            `json:"version"`
	CapDescription *string           `json:"cap_description,omitempty"`
	Metadata       map[string]string `json:"metadata"`
	Aliases        []string          `json:"aliases"`
	IsAbstract     bool              `json:"abstract,omitempty"`
	Args           []CapArg          `json:"args,omitempty"`
	Output         *CapOutput        `json:"output,omitempty"`
}

// ToCap converts a registry response to a standard Cap
func (r *RegistryCapResponse) ToCap() (*Cap, error) {
	// URN must be a string in canonical format
	capUrn, err := urn.NewCapUrnFromString(r.Urn)
	if err != nil {
		return nil, fmt.Errorf("invalid URN string: %w", err)
	}

	// Use title from the response
	title := r.Title
	if title == "" {
		title = "Registry Capability"
	}

	cap := NewCap(capUrn, title, r.Aliases)
	cap.IsAbstract = r.IsAbstract
	cap.CapDescription = r.CapDescription
	if r.Metadata != nil {
		cap.Metadata = r.Metadata
	}
	cap.Args = r.Args
	cap.Output = r.Output

	return cap, nil
}

// Private helper methods

// normalizeCapUrn parses a cap URN and returns its canonical (tag-sorted)
// string form. A parse failure is a HARD error — it is NEVER silently swallowed
// into the raw string, which would let a malformed URN masquerade as a
// cache-miss / "not in manifest" downstream. Callers on a path that returns an
// error propagate this; lookup/void paths log and skip. This never panics.
//
// Mirrors Rust fabric::registry::normalize_cap_urn.
func normalizeCapUrn(urnStr string) (string, error) {
	parsed, err := urn.NewCapUrnFromString(urnStr)
	if err != nil {
		return "", fmt.Errorf("malformed cap URN %q: %w", urnStr, err)
	}
	return parsed.String(), nil
}

// normalizeMediaUrn parses a media URN and returns its canonical (tag-sorted)
// string form. A parse failure is a HARD error — never silently swallowed into
// the raw string. This never panics.
//
// Mirrors Rust fabric::registry::normalize_media_urn.
func normalizeMediaUrn(urnStr string) (string, error) {
	parsed, err := urn.NewMediaUrnFromString(urnStr)
	if err != nil {
		return "", fmt.Errorf("malformed media URN %q: %w", urnStr, err)
	}
	return parsed.String(), nil
}

// Validation functions

// IdentityCap constructs the canonical identity Cap definition.
//
// The identity cap accepts any media type as input and echoes it as output
// unchanged. It is mandatory in every capability set so the resolver's
// source-to-cap-arg matching can route through identity in any notation.
//
// The reference declares this in `standard::caps`; Go's `standard` package sits
// BELOW `cap` in the import graph and so cannot name the `Cap` type. Declaring
// it here is the same object at a different site — an object-level divergence,
// not a behavioral one.
func IdentityCap() (*Cap, error) {
	identityUrn := standard.CapIdentity
	u, err := urn.NewCapUrnFromString(identityUrn)
	if err != nil {
		// CAP_IDENTITY is a build-time constant and must always parse. A bad
		// one is a serious defect, but it must NOT crash registry construction
		// — return the error so the caller can log and skip identity seeding.
		return nil, fmt.Errorf("IdentityCap: failed to parse identity URN %q: %w", identityUrn, err)
	}
	desc := "The categorical identity morphism. Echoes input as output unchanged. Mandatory in every capability set."
	c := &Cap{
		Urn:            u,
		Title:          "Identity",
		Aliases:        []string{"identity"},
		CapDescription: &desc,
		Metadata:       make(map[string]string),
		Args: []CapArg{
			NewCapArg("media:", true, []ArgSource{{Stdin: strPtr("media:")}}),
		},
	}
	c.SetOutput(NewCapOutput("media:", "The input data, unchanged"))
	return c, nil
}

// strPtr returns a pointer to the given string (helper for ArgSource.Stdin).
func strPtr(s string) *string { return &s }

// =============================================================================
// Alias resolution (cap registry surface)
// =============================================================================
