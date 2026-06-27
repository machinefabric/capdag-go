package bifaci

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/machinefabric/capdag-go/urn"
)

// CartridgeRepoError represents errors from cartridge repository operations
type CartridgeRepoError struct {
	Kind    string
	Message string
}

func (e *CartridgeRepoError) Error() string {
	return fmt.Sprintf("%s: %s", e.Kind, e.Message)
}

// NewHttpError creates an HTTP error
func NewHttpError(msg string) *CartridgeRepoError {
	return &CartridgeRepoError{Kind: "HttpError", Message: msg}
}

// NewParseError creates a parse error
func NewParseError(msg string) *CartridgeRepoError {
	return &CartridgeRepoError{Kind: "ParseError", Message: msg}
}

// NewStatusError creates a status error
func NewStatusError(status int) *CartridgeRepoError {
	return &CartridgeRepoError{Kind: "StatusError", Message: fmt.Sprintf("Registry request failed with status %d", status)}
}

// NewNetworkBlockedError creates a network blocked error
func NewNetworkBlockedError(msg string) *CartridgeRepoError {
	return &CartridgeRepoError{Kind: "NetworkBlocked", Message: msg}
}

// RegistryArgSource is one source for a registry cap argument. Exactly
// one of Stdin / Position / CliFlag is populated by the producer.
type RegistryArgSource struct {
	Stdin    *string `json:"stdin,omitempty"`
	Position *int64  `json:"position,omitempty"`
	CliFlag  *string `json:"cli_flag,omitempty"`
}

// RegistryCapArg is one argument descriptor on a registry cap.
type RegistryCapArg struct {
	MediaUrn       string              `json:"media_urn"`
	Required       bool                `json:"required"`
	IsSequence     bool                `json:"is_sequence,omitempty"`
	Sources        []RegistryArgSource `json:"sources,omitempty"`
	ArgDescription *string             `json:"arg_description,omitempty"`
	// DefaultValue is whatever JSON the registry emits for the default
	// (string, number, bool, object). json.RawMessage preserves it
	// verbatim so producers and consumers can round-trip without losing
	// type information.
	DefaultValue json.RawMessage `json:"default_value,omitempty"`
}

// RegistryCapOutput is the output descriptor on a registry cap.
type RegistryCapOutput struct {
	MediaUrn          string  `json:"media_urn"`
	IsSequence        bool    `json:"is_sequence,omitempty"`
	OutputDescription *string `json:"output_description,omitempty"`
}

// RegistryCap is a single capability advertised by a cartridge in the
// registry. Urn / Title / Command are always present; the other three
// fields appear only when the cartridge documents them.
type RegistryCap struct {
	Urn            string             `json:"urn"`
	Title          string             `json:"title"`
	Command        string             `json:"command"`
	CapDescription *string            `json:"cap_description,omitempty"`
	Args           []RegistryCapArg   `json:"args,omitempty"`
	Output         *RegistryCapOutput `json:"output,omitempty"`
}

// RegistryCapGroup bundles caps + adapter URNs as one atomic
// registration unit.
type RegistryCapGroup struct {
	Name        string        `json:"name"`
	Caps        []RegistryCap `json:"caps,omitempty"`
	AdapterUrns []string      `json:"adapter_urns,omitempty"`
}

// CartridgeDistributionInfo represents package distribution data.
// `Url` is the absolute URL of the package — every consumer downloads
// from that URL directly. There is no derived URL pattern any more.
type CartridgeDistributionInfo struct {
	Name   string `json:"name"`
	Sha256 string `json:"sha256"`
	Size   uint64 `json:"size"`
	Url    string `json:"url"`
	// Format is the installer format: "pkg" (macOS), "deb"/"rpm" (Linux),
	// "msi"/"exe" (Windows). Empty-when-omitted so the legacy singular
	// `package` (which has no `format`) round-trips through this same
	// struct.
	Format string `json:"format,omitempty"`
}

// CartridgeBuild represents a platform-specific build within a version.
//
// A platform may ship more than one installer format (e.g. linux-x86_64
// → `.deb` + `.rpm`), so `Packages` is a list; consumers pick the format
// the host can run via PrimaryPackage. `Package` is the legacy singular
// installer (no `format`), read only as a fallback when `Packages` is
// empty so a registry not yet republished with the dual-write keeps
// installing.
type CartridgeBuild struct {
	Platform string `json:"platform"`
	// Per-format installer list (`.pkg`/`.deb`/`.rpm`/`.msi`/`.exe`).
	// `omitempty` so a manifest published before `packages[]` existed
	// (which carries only the legacy singular `package`) still
	// deserializes instead of failing the whole parse.
	Packages []CartridgeDistributionInfo `json:"packages,omitempty"`
	// Legacy singular installer (`{name,url,sha256,size}`, no `format`).
	// Read here only as a fallback when `Packages` is empty. An empty
	// Name marks it absent.
	Package CartridgeDistributionInfo `json:"package,omitempty"`
}

// PrimaryPackage returns the installer package the host should use,
// preferring the platform's native format. Falls back to the legacy
// singular `Package` when `Packages` is empty (pre-dual-write
// manifests). Returns nil only when the build ships no installer at all
// (empty `Packages` and a legacy `Package` with no name).
func (b *CartridgeBuild) PrimaryPackage() *CartridgeDistributionInfo {
	os := b.Platform
	if i := strings.Index(os, "-"); i >= 0 {
		os = os[:i]
	}
	var preference []string
	switch os {
	case "darwin":
		preference = []string{"pkg"}
	case "linux":
		preference = []string{"deb", "rpm"}
	case "windows":
		preference = []string{"msi", "exe"}
	}
	for _, format := range preference {
		for i := range b.Packages {
			if b.Packages[i].Format == format {
				return &b.Packages[i]
			}
		}
	}
	if len(b.Packages) > 0 {
		return &b.Packages[0]
	}
	if b.Package.Name != "" {
		return &b.Package
	}
	return nil
}

// CartridgeVersionData represents a cartridge version's data (v5.0 schema).
// Each version has one or more platform-specific builds.
//
// `NotesUrl` is the absolute URL of the version's release-notes
// Markdown file, when one was uploaded at publish time. Optional —
// cartridges historically did not ship per-version notes.
type CartridgeVersionData struct {
	ReleaseDate   string           `json:"releaseDate"`
	Changelog     []string         `json:"changelog,omitempty"`
	MinAppVersion string           `json:"minAppVersion,omitempty"`
	Builds        []CartridgeBuild `json:"builds"`
	NotesUrl      string           `json:"notesUrl,omitempty"`
}

// CartridgeRegistryEntry represents a cartridge entry in the v4.0
// registry (nested format). Each entry's capability surface lives in
// CapGroups; there is no flat caps list.
type CartridgeRegistryEntry struct {
	Name          string                          `json:"name"`
	Description   string                          `json:"description"`
	Author        string                          `json:"author"`
	PageUrl       string                          `json:"pageUrl,omitempty"`
	TeamId        string                          `json:"teamId"`
	MinAppVersion string                          `json:"minAppVersion,omitempty"`
	CapGroups     []RegistryCapGroup              `json:"cap_groups,omitempty"`
	Categories    []string                        `json:"categories,omitempty"`
	Tags          []string                        `json:"tags,omitempty"`
	LatestVersion string                          `json:"latestVersion"`
	Versions      map[string]CartridgeVersionData `json:"versions"`
}

// CartridgeChannel is the distribution channel for a cartridge entry.
// Mirrors capdag's CartridgeChannel and the registry's
// channels.<channel> keys.
type CartridgeChannel string

const (
	// User-facing builds. Promoted via the publish script's --release.
	CartridgeChannelRelease CartridgeChannel = "release"
	// In-flight builds. Default for the publish scripts.
	CartridgeChannelNightly CartridgeChannel = "nightly"
)

// ParseCartridgeChannel parses the wire-form string ("release" /
// "nightly") at runtime. Anything else is a hard error so a typo never
// silently masquerades as a known channel. Mirrors Rust's
// CartridgeChannel::parse (capdag/src/bifaci/cartridge_repo.rs).
func ParseCartridgeChannel(s string) (CartridgeChannel, error) {
	switch s {
	case "release":
		return CartridgeChannelRelease, nil
	case "nightly":
		return CartridgeChannelNightly, nil
	default:
		return "", fmt.Errorf("invalid CartridgeChannel '%s'; expected 'release' or 'nightly'", s)
	}
}

// UnmarshalJSON deserializes a CartridgeChannel from its lowercase wire
// form, rejecting any value that is not "release" or "nightly". Mirrors
// the Rust enum's `#[serde(rename_all = "lowercase")]` Deserialize, which
// fails on unknown variants — there are no defaults and no tolerated
// aliases.
func (c *CartridgeChannel) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := ParseCartridgeChannel(s)
	if err != nil {
		return err
	}
	*c = parsed
	return nil
}

// CartridgeChannelEntries is one channel's cartridges map. Always
// present in the parent registry, possibly empty.
type CartridgeChannelEntries struct {
	Cartridges map[string]CartridgeRegistryEntry `json:"cartridges"`
}

// CartridgeRegistryChannels is the per-channel partitioning of the
// registry. Each channel is a distinct namespace — a cartridge id can
// exist independently in release and nightly with different versions
// and metadata.
type CartridgeRegistryChannels struct {
	Release CartridgeChannelEntries `json:"release"`
	Nightly CartridgeChannelEntries `json:"nightly"`
}

// CartridgeRegistry represents the v5.0 channel-partitioned cartridge
// registry. Both `release` and `nightly` are always present (possibly
// empty) so consumers never need conditional fallbacks.
type CartridgeRegistry struct {
	SchemaVersion string                    `json:"schemaVersion"`
	LastUpdated   string                    `json:"lastUpdated"`
	Channels      CartridgeRegistryChannels `json:"channels"`
}

// CartridgeInfo represents a cartridge in the flat API response format.
//
// The cartridge's capability surface lives in CapGroups; there is no
// flat caps list. The Homepage field has been removed (it was never on
// the wire). IterCaps walks every cap across every group in declaration
// order.
type CartridgeInfo struct {
	Id                string                          `json:"id"`
	Name              string                          `json:"name"`
	Version           string                          `json:"version"`
	Description       string                          `json:"description"`
	Author            string                          `json:"author"`
	TeamId            string                          `json:"teamId"`
	SignedAt          string                          `json:"signedAt"`
	MinAppVersion     string                          `json:"minAppVersion,omitempty"`
	PageUrl           string                          `json:"pageUrl,omitempty"`
	Categories        []string                        `json:"categories,omitempty"`
	Tags              []string                        `json:"tags,omitempty"`
	CapGroups         []RegistryCapGroup              `json:"cap_groups"`
	Versions          map[string]CartridgeVersionData `json:"versions"`
	AvailableVersions []string                        `json:"availableVersions,omitempty"`
	// Channel this entry belongs to. Set by the transformer; consumers
	// must not synthesize this field — it comes from the registry's
	// `channels` partitioning.
	Channel CartridgeChannel `json:"channel"`
	// RegistryUrl is the registry URL this entry was fetched from.
	// Stamped onto each entry by the fetch path based on the URL the
	// manifest was served from. Verbatim string — never trimmed,
	// normalized, or re-derived from the manifest body. Identity
	// comparison is byte equality.
	RegistryUrl string `json:"registryUrl"`
}

// UnmarshalJSON deserializes a CartridgeInfo from the wire shape. The
// camelCase string fields version/description/author/team_id/signed_at/
// min_app_version/page_url tolerate an explicit JSON null by coercing it
// to the empty string — this is the only tolerated coercion (mirrors
// Rust's null_as_empty_string deserializer); every other malformed input
// is a hard error.
func (c *CartridgeInfo) UnmarshalJSON(data []byte) error {
	type rawInfo struct {
		Id                string                          `json:"id"`
		Name              string                          `json:"name"`
		Version           *string                         `json:"version"`
		Description       *string                         `json:"description"`
		Author            *string                         `json:"author"`
		TeamId            *string                         `json:"teamId"`
		SignedAt          *string                         `json:"signedAt"`
		MinAppVersion     *string                         `json:"minAppVersion"`
		PageUrl           *string                         `json:"pageUrl"`
		Categories        []string                        `json:"categories"`
		Tags              []string                        `json:"tags"`
		CapGroups         []RegistryCapGroup              `json:"cap_groups"`
		Versions          map[string]CartridgeVersionData `json:"versions"`
		AvailableVersions []string                        `json:"availableVersions"`
		Channel           CartridgeChannel                `json:"channel"`
		RegistryUrl       string                          `json:"registryUrl"`
	}
	var raw rawInfo
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	deref := func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	}
	c.Id = raw.Id
	c.Name = raw.Name
	c.Version = deref(raw.Version)
	c.Description = deref(raw.Description)
	c.Author = deref(raw.Author)
	c.TeamId = deref(raw.TeamId)
	c.SignedAt = deref(raw.SignedAt)
	c.MinAppVersion = deref(raw.MinAppVersion)
	c.PageUrl = deref(raw.PageUrl)
	c.Categories = raw.Categories
	c.Tags = raw.Tags
	c.CapGroups = raw.CapGroups
	c.Versions = raw.Versions
	c.AvailableVersions = raw.AvailableVersions
	c.Channel = raw.Channel
	c.RegistryUrl = raw.RegistryUrl
	return nil
}

// IterCaps yields every cap across every cap group in declaration order.
// Use this whenever you need a flat view of the cartridge's caps now
// that the on-wire shape groups them.
func (p *CartridgeInfo) IterCaps() []RegistryCap {
	var out []RegistryCap
	for _, g := range p.CapGroups {
		out = append(out, g.Caps...)
	}
	return out
}

// IsSigned checks if cartridge is signed (has team_id and signed_at)
func (p *CartridgeInfo) IsSigned() bool {
	return p.TeamId != "" && p.SignedAt != ""
}

// BuildForPlatform gets the build for a specific platform from the latest version.
func (p *CartridgeInfo) BuildForPlatform(platform string) *CartridgeBuild {
	vd, ok := p.Versions[p.Version]
	if !ok {
		return nil
	}
	for i := range vd.Builds {
		if vd.Builds[i].Platform == platform {
			return &vd.Builds[i]
		}
	}
	return nil
}

// AvailablePlatforms returns all platforms available across all versions.
func (p *CartridgeInfo) AvailablePlatforms() []string {
	seen := make(map[string]struct{})
	for _, vd := range p.Versions {
		for _, b := range vd.Builds {
			seen[b.Platform] = struct{}{}
		}
	}
	platforms := make([]string, 0, len(seen))
	for pl := range seen {
		platforms = append(platforms, pl)
	}
	sort.Strings(platforms)
	return platforms
}

// HostPlatform returns the platform string ({os}-{arch}) of the binary
// that calls this, in the exact form the registry uses
// (`darwin-arm64`, `darwin-x86_64`, `linux-x86_64`, `windows-x86_64`).
// Derived from runtime.GOOS/runtime.GOARCH — the engine binary literally
// runs on this platform, so this is the authoritative host string for
// compatibility resolution. Single source of truth: every consumer that
// needs "what am I running on?" calls this rather than re-deriving the
// os/arch mapping.
func HostPlatform() string {
	var os string
	switch runtime.GOOS {
	case "darwin":
		os = "darwin"
	case "linux":
		os = "linux"
	case "windows":
		os = "windows"
	default:
		os = runtime.GOOS
	}
	arch := runtime.GOARCH
	switch arch {
	case "arm64":
		arch = "arm64"
	case "amd64":
		// Go reports x86-64 as "amd64"; the registry uses "x86_64".
		arch = "x86_64"
	case "aarch64":
		arch = "arm64"
	}
	return fmt.Sprintf("%s-%s", os, arch)
}

// CompatStatus is the host-compatibility status of a registry cartridge,
// resolved against a specific host platform string. Mirrors the proto
// CartridgeCompatibilityStatus.
type CompatStatus int

const (
	// CompatStatusCompatible: the latest version has a build for this host
	// platform — install as-is.
	CompatStatusCompatible CompatStatus = iota
	// CompatStatusCompatibleOutdated: the latest version has no host build,
	// but an older version does; ResolvedVersion names that older version.
	// Install it, mark outdated.
	CompatStatusCompatibleOutdated
	// CompatStatusIncompatible: no version has a build for this host
	// platform. Nothing to install.
	CompatStatusIncompatible
)

// CartridgeCompatibilityResolution is the resolved verdict the engine
// attaches to an available cartridge: which version/package the host
// should install (if any) and a human reason when it is not the
// latest-and-greatest.
type CartridgeCompatibilityResolution struct {
	Status       CompatStatus
	HostPlatform string
	// ResolvedVersion is the newest version that has a build for this host
	// (empty when Incompatible).
	ResolvedVersion string
	// ResolvedPackage is the host-preferred installer package within
	// ResolvedVersion (nil when Incompatible). Cloned from the registry
	// data.
	ResolvedPackage *CartridgeDistributionInfo
	// Reason is the explanation, set whenever Status is not Compatible.
	Reason string
}

// buildForHost finds this cartridge's build for hostPlatform within a
// given version, if any. The host package within it is then chosen by
// PrimaryPackage.
func (p *CartridgeInfo) buildForHost(version, hostPlatform string) *CartridgeBuild {
	vd, ok := p.Versions[version]
	if !ok {
		return nil
	}
	for i := range vd.Builds {
		if vd.Builds[i].Platform == hostPlatform {
			return &vd.Builds[i]
		}
	}
	return nil
}

// ResolveForHost resolves which version/package this host should
// install, scanning versions newest-first (AvailableVersions is the
// authoritative newest-first ordering). The newest version with a host
// build wins:
//   - it IS the latest version → Compatible
//   - it is older than the latest → CompatibleOutdated
//   - no version has a host build → Incompatible
//
// "Latest" is p.Version — the same field BuildForPlatform trusts — not
// AvailableVersions[0]. A host build found at p.Version classifies as
// Compatible while any other found version classifies as
// CompatibleOutdated. We do not paper over a p.Version with no host
// build by silently calling it latest. A build whose PrimaryPackage is
// nil ships no installer at all — skip it rather than resolve to a
// version the host cannot download, and keep scanning older versions.
func (p *CartridgeInfo) ResolveForHost(hostPlatform string) CartridgeCompatibilityResolution {
	latest := p.Version

	for _, ver := range p.AvailableVersions {
		build := p.buildForHost(ver, hostPlatform)
		if build == nil {
			continue
		}
		pkg := build.PrimaryPackage()
		if pkg == nil {
			continue
		}
		pkgCopy := *pkg
		if ver == latest {
			return CartridgeCompatibilityResolution{
				Status:          CompatStatusCompatible,
				HostPlatform:    hostPlatform,
				ResolvedVersion: ver,
				ResolvedPackage: &pkgCopy,
			}
		}
		return CartridgeCompatibilityResolution{
			Status:          CompatStatusCompatibleOutdated,
			HostPlatform:    hostPlatform,
			ResolvedVersion: ver,
			ResolvedPackage: &pkgCopy,
			Reason: fmt.Sprintf(
				"Latest %s has no %s build; newest compatible is %s",
				latest, hostPlatform, ver,
			),
		}
	}

	return CartridgeCompatibilityResolution{
		Status:       CompatStatusIncompatible,
		HostPlatform: hostPlatform,
		Reason: fmt.Sprintf(
			"No installable %s build available in any version", hostPlatform,
		),
	}
}

// CartridgeRegistryResponse represents the cartridge registry response (flat format)
type CartridgeRegistryResponse struct {
	Cartridges []CartridgeInfo `json:"cartridges"`
}

// CartridgeSuggestion represents a cartridge suggestion for a missing cap.
// Channel propagates from the source registry — the UI uses it to render
// the release/nightly distinction without re-deriving.
type CartridgeSuggestion struct {
	CartridgeId          string           `json:"cartridgeId"`
	CartridgeName        string           `json:"cartridgeName"`
	CartridgeDescription string           `json:"cartridgeDescription"`
	CapUrn               string           `json:"capUrn"`
	CapTitle             string           `json:"capTitle"`
	LatestVersion        string           `json:"latestVersion"`
	RepoUrl              string           `json:"repoUrl"`
	PageUrl              string           `json:"pageUrl"`
	Channel              CartridgeChannel `json:"channel"`
}

// CartridgeRepoServer serves registry data with queries.
// Transforms v4.0 nested registry schema to flat API response format.
type CartridgeRepoServer struct {
	registry CartridgeRegistry
}

// NewCartridgeRepoServer creates a new server instance from a v5.0
// channel-partitioned registry.
func NewCartridgeRepoServer(registry CartridgeRegistry) (*CartridgeRepoServer, error) {
	if registry.SchemaVersion != "5.0" {
		return nil, NewParseError(fmt.Sprintf(
			"Unsupported registry schema version: %s. Required: 5.0",
			registry.SchemaVersion,
		))
	}
	if registry.Channels.Release.Cartridges == nil {
		registry.Channels.Release.Cartridges = map[string]CartridgeRegistryEntry{}
	}
	if registry.Channels.Nightly.Cartridges == nil {
		registry.Channels.Nightly.Cartridges = map[string]CartridgeRegistryEntry{}
	}
	return &CartridgeRepoServer{registry: registry}, nil
}

// validateVersionData validates that version data has all required fields
func validateVersionData(id, version string, versionData *CartridgeVersionData) error {
	if len(versionData.Builds) == 0 {
		return NewParseError(fmt.Sprintf("Cartridge %s v%s: no builds", id, version))
	}
	for i, build := range versionData.Builds {
		if build.Platform == "" {
			return NewParseError(fmt.Sprintf(
				"Cartridge %s v%s: build[%d] missing platform", id, version, i,
			))
		}
		if len(build.Packages) == 0 {
			// A manifest published before `packages[]` existed carries
			// only the legacy singular `package`. Accept it (read as a
			// fallback by PrimaryPackage); reject only a build with no
			// installer at all. The legacy object has no `format`.
			if build.Package.Name == "" {
				return NewParseError(fmt.Sprintf(
					"Cartridge %s v%s: build[%d] (%s) ships no packages", id, version, i, build.Platform,
				))
			}
		}
		for j, pkg := range build.Packages {
			if pkg.Name == "" {
				return NewParseError(fmt.Sprintf(
					"Cartridge %s v%s: build[%d] (%s) package[%d] missing name",
					id, version, i, build.Platform, j,
				))
			}
			if pkg.Format == "" {
				return NewParseError(fmt.Sprintf(
					"Cartridge %s v%s: build[%d] (%s) package[%d] (%s) missing format",
					id, version, i, build.Platform, j, pkg.Name,
				))
			}
		}
	}
	return nil
}

// compareVersions compares semantic version strings; returns -1, 0, or 1
func compareVersions(a, b string) int {
	partsA := parseVersion(a)
	partsB := parseVersion(b)

	maxLen := len(partsA)
	if len(partsB) > maxLen {
		maxLen = len(partsB)
	}

	for i := 0; i < maxLen; i++ {
		var numA, numB uint32
		if i < len(partsA) {
			numA = partsA[i]
		}
		if i < len(partsB) {
			numB = partsB[i]
		}
		if numA < numB {
			return -1
		} else if numA > numB {
			return 1
		}
	}

	return 0
}

// parseVersion parses a version string into numeric parts
func parseVersion(v string) []uint32 {
	parts := strings.Split(v, ".")
	nums := make([]uint32, 0, len(parts))
	for _, p := range parts {
		if num, err := strconv.ParseUint(p, 10, 32); err == nil {
			nums = append(nums, uint32(num))
		}
	}
	return nums
}

// entryToCartridgeInfo flattens one channel-entry into a CartridgeInfo.
// Fails hard if the entry's LatestVersion is missing from Versions or
// if the latest version's builds are malformed.
func (s *CartridgeRepoServer) entryToCartridgeInfo(
	channel CartridgeChannel,
	id string,
	entry CartridgeRegistryEntry,
) (CartridgeInfo, error) {
	latestVersion := entry.LatestVersion
	versionData, ok := entry.Versions[latestVersion]
	if !ok {
		return CartridgeInfo{}, NewParseError(fmt.Sprintf(
			"Cartridge %s (%s): latestVersion %s not found in versions",
			id, channel, latestVersion,
		))
	}
	if err := validateVersionData(id, latestVersion, &versionData); err != nil {
		return CartridgeInfo{}, err
	}

	availableVersions := make([]string, 0, len(entry.Versions))
	for version := range entry.Versions {
		availableVersions = append(availableVersions, version)
	}
	sort.Slice(availableVersions, func(i, j int) bool {
		return compareVersions(availableVersions[i], availableVersions[j]) > 0
	})

	minAppVersion := versionData.MinAppVersion
	if minAppVersion == "" {
		minAppVersion = entry.MinAppVersion
	}
	capGroups := entry.CapGroups
	if capGroups == nil {
		capGroups = []RegistryCapGroup{}
	}
	categories := entry.Categories
	if categories == nil {
		categories = []string{}
	}
	tags := entry.Tags
	if tags == nil {
		tags = []string{}
	}

	return CartridgeInfo{
		Id:                id,
		Name:              entry.Name,
		Version:           latestVersion,
		Description:       entry.Description,
		Author:            entry.Author,
		TeamId:            entry.TeamId,
		SignedAt:          versionData.ReleaseDate,
		MinAppVersion:     minAppVersion,
		PageUrl:           entry.PageUrl,
		Categories:        categories,
		Tags:              tags,
		CapGroups:         capGroups,
		Versions:          entry.Versions,
		AvailableVersions: availableVersions,
		Channel:           channel,
	}, nil
}

// TransformToCartridgeArray walks both channels and emits CartridgeInfo
// for every entry, preserving channel provenance. Release entries
// appear before nightly entries in the result.
func (s *CartridgeRepoServer) TransformToCartridgeArray() ([]CartridgeInfo, error) {
	result := make([]CartridgeInfo, 0,
		len(s.registry.Channels.Release.Cartridges)+len(s.registry.Channels.Nightly.Cartridges))

	for _, ch := range []CartridgeChannel{CartridgeChannelRelease, CartridgeChannelNightly} {
		entries := s.channelEntries(ch)
		for id, entry := range entries {
			info, err := s.entryToCartridgeInfo(ch, id, entry)
			if err != nil {
				return nil, err
			}
			result = append(result, info)
		}
	}
	return result, nil
}

// channelEntries returns the cartridges map for the requested channel.
func (s *CartridgeRepoServer) channelEntries(ch CartridgeChannel) map[string]CartridgeRegistryEntry {
	switch ch {
	case CartridgeChannelRelease:
		return s.registry.Channels.Release.Cartridges
	case CartridgeChannelNightly:
		return s.registry.Channels.Nightly.Cartridges
	default:
		return nil
	}
}

// GetCartridges returns all cartridges (API response format)
func (s *CartridgeRepoServer) GetCartridges() (*CartridgeRegistryResponse, error) {
	cartridges, err := s.TransformToCartridgeArray()
	if err != nil {
		return nil, err
	}
	return &CartridgeRegistryResponse{Cartridges: cartridges}, nil
}

// GetCartridgeById returns a cartridge by (channel, id). Channel is
// required because the same id can independently exist in both
// channels with different versions/metadata.
func (s *CartridgeRepoServer) GetCartridgeById(channel CartridgeChannel, id string) (*CartridgeInfo, error) {
	if channel != CartridgeChannelRelease && channel != CartridgeChannelNightly {
		return nil, NewParseError(fmt.Sprintf("Invalid channel %q", channel))
	}
	entries := s.channelEntries(channel)
	entry, ok := entries[id]
	if !ok {
		return nil, nil
	}
	info, err := s.entryToCartridgeInfo(channel, id, entry)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// SearchCartridges searches cartridges by free-text query.
//
// Matches against cartridge name, description, tags, and cap titles. Cap
// URN strings are NOT substring-matched: a cap URN is a tagged
// identifier and substring matching against it is a category error. Use
// GetCartridgesByCap to look up cartridges that provide a specific cap.
func (s *CartridgeRepoServer) SearchCartridges(query string) ([]CartridgeInfo, error) {
	all, err := s.TransformToCartridgeArray()
	if err != nil {
		return nil, err
	}

	lowerQuery := strings.ToLower(query)
	results := make([]CartridgeInfo, 0)

	for _, c := range all {
		if strings.Contains(strings.ToLower(c.Name), lowerQuery) ||
			strings.Contains(strings.ToLower(c.Description), lowerQuery) {
			results = append(results, c)
			continue
		}
		found := false
		for _, tag := range c.Tags {
			if strings.Contains(strings.ToLower(tag), lowerQuery) {
				found = true
				break
			}
		}
		if found {
			results = append(results, c)
			continue
		}
		for _, cap := range c.IterCaps() {
			if strings.Contains(strings.ToLower(cap.Title), lowerQuery) {
				found = true
				break
			}
		}
		if found {
			results = append(results, c)
		}
	}

	return results, nil
}

// GetCartridgesByCategory returns cartridges by category
func (s *CartridgeRepoServer) GetCartridgesByCategory(category string) ([]CartridgeInfo, error) {
	all, err := s.TransformToCartridgeArray()
	if err != nil {
		return nil, err
	}

	results := make([]CartridgeInfo, 0)
	for _, c := range all {
		for _, cat := range c.Categories {
			if cat == category {
				results = append(results, c)
				break
			}
		}
	}
	return results, nil
}

// GetCartridgesByCap returns cartridges that provide a specific cap.
//
// The request URN is parsed via NewCapUrnFromString; each declared
// cartridge cap is parsed and matched with ConformsTo: cap dispatch is
// the partial-order question "does the declared cap conform to (i.e.
// refine, equal, or be more specific than) the requested pattern?".
// Only `in` and `out` tags are functionally meaningful — the `op` tag
// has no role in the predicate. Malformed input or declared URNs are
// returned as ParseError, never silently ignored.
func (s *CartridgeRepoServer) GetCartridgesByCap(capUrn string) ([]CartridgeInfo, error) {
	requested, err := urn.NewCapUrnFromString(capUrn)
	if err != nil {
		return nil, NewParseError(fmt.Sprintf(
			"GetCartridgesByCap: invalid cap URN %q: %v", capUrn, err,
		))
	}

	all, err := s.TransformToCartridgeArray()
	if err != nil {
		return nil, err
	}

	results := make([]CartridgeInfo, 0)
	for _, c := range all {
		for _, cap := range c.IterCaps() {
			declared, perr := urn.NewCapUrnFromString(cap.Urn)
			if perr != nil {
				return nil, NewParseError(fmt.Sprintf(
					"cartridge %s (%s): invalid declared cap URN %q: %v",
					c.Id, c.Channel, cap.Urn, perr,
				))
			}
			if declared.ConformsTo(requested) {
				results = append(results, c)
				break
			}
		}
	}
	return results, nil
}

// CartridgeKey is the composite (channel, id) cache key. A cartridge id
// is unique within a channel but can appear in both channels at the
// same time.
type CartridgeKey struct {
	Channel CartridgeChannel
	Id      string
}

// CartridgeRepoCache holds cached cartridge repository data, keyed by
// (channel, id) so the same id can independently coexist in release
// and nightly with separate metadata/versions.
type CartridgeRepoCache struct {
	cartridges      map[CartridgeKey]CartridgeInfo
	capToCartridges map[string][]CartridgeKey
	lastUpdated     time.Time
	repoUrl         string
}

// CartridgeRepo is a service for fetching and caching cartridge repository data
type CartridgeRepo struct {
	httpClient  *http.Client
	caches      map[string]*CartridgeRepoCache
	cacheTTL    time.Duration
	offlineFlag atomic.Bool
	mu          sync.RWMutex
}

// NewCartridgeRepo creates a new cartridge repo service
func NewCartridgeRepo(cacheTTLSeconds uint64) *CartridgeRepo {
	return &CartridgeRepo{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		caches:   make(map[string]*CartridgeRepoCache),
		cacheTTL: time.Duration(cacheTTLSeconds) * time.Second,
	}
}

// SetOffline sets the offline flag. When true, all registry fetches are blocked.
func (r *CartridgeRepo) SetOffline(offline bool) {
	r.offlineFlag.Store(offline)
}

// fetchRegistry fetches the v5.0 channel-partitioned cartridge
// manifest from a URL and flattens it through CartridgeRepoServer into
// the CartridgeRegistryResponse shape the cache expects (one
// CartridgeInfo per (channel, id) pair). 404 is "no cartridges
// published yet" → empty response. Any other non-200, network failure,
// or schema validation error surfaces as an error. There is no
// fallback to a stale cache shape — the manifest is the source of
// truth.
func (r *CartridgeRepo) fetchRegistry(repoUrl string) (*CartridgeRegistryResponse, error) {
	if r.offlineFlag.Load() {
		return nil, NewNetworkBlockedError(fmt.Sprintf(
			"Network access blocked by policy — cannot fetch cartridge registry '%s'", repoUrl,
		))
	}

	resp, err := r.httpClient.Get(repoUrl)
	if err != nil {
		return nil, NewHttpError(fmt.Sprintf("Failed to fetch from %s: %v", repoUrl, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return &CartridgeRegistryResponse{Cartridges: []CartridgeInfo{}}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, NewStatusError(resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, NewHttpError(fmt.Sprintf("Failed to read response from %s: %v", repoUrl, err))
	}

	var manifest CartridgeRegistry
	if err := json.Unmarshal(body, &manifest); err != nil {
		return nil, NewParseError(fmt.Sprintf("Failed to parse from %s: %v", repoUrl, err))
	}

	server, err := NewCartridgeRepoServer(manifest)
	if err != nil {
		return nil, err
	}
	return server.GetCartridges()
}

// updateCache updates cache from a registry response.
//
// Each entry's Channel must be set on arrival — the flat response is
// produced by the server transformer that walks the v5.0 channel-
// partitioned source. Cache key is (channel, id). The cap-to-cartridges
// index keys on the *normalized* tagged-URN form (parse via
// NewCapUrnFromString, then String()) and stores CartridgeKey
// references so suggestions preserve channel provenance.
func (r *CartridgeRepo) updateCache(repoUrl string, registry *CartridgeRegistryResponse) error {
	cartridges := make(map[CartridgeKey]CartridgeInfo)
	capToCartridges := make(map[string][]CartridgeKey)

	for _, cartridgeInfo := range registry.Cartridges {
		if cartridgeInfo.Channel != CartridgeChannelRelease &&
			cartridgeInfo.Channel != CartridgeChannelNightly {
			return NewParseError(fmt.Sprintf(
				"cartridge %s: invalid or missing channel %q",
				cartridgeInfo.Id, cartridgeInfo.Channel,
			))
		}
		key := CartridgeKey{Channel: cartridgeInfo.Channel, Id: cartridgeInfo.Id}
		for _, cap := range cartridgeInfo.IterCaps() {
			parsed, err := urn.NewCapUrnFromString(cap.Urn)
			if err != nil {
				return NewParseError(fmt.Sprintf(
					"cartridge %s (%s): invalid cap URN %q: %v",
					key.Id, key.Channel, cap.Urn, err,
				))
			}
			normalized := parsed.String()
			capToCartridges[normalized] = append(capToCartridges[normalized], key)
		}
		cartridges[key] = cartridgeInfo
	}

	r.mu.Lock()
	r.caches[repoUrl] = &CartridgeRepoCache{
		cartridges:      cartridges,
		capToCartridges: capToCartridges,
		lastUpdated:     time.Now(),
		repoUrl:         repoUrl,
	}
	r.mu.Unlock()
	return nil
}

// SyncRepos syncs cartridge data from the given repository URLs.
//
// A fetch error or a malformed registry response moves on to the next
// repo: a single bad repo must not stall the others. updateCache
// returns an error rather than swallowing malformed cap URNs, so
// indexing failures are surfaced to stderr where they are visible.
func (r *CartridgeRepo) SyncRepos(repoUrls []string) {
	for _, repoUrl := range repoUrls {
		registry, err := r.fetchRegistry(repoUrl)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cartridge repo sync %s: %v\n", repoUrl, err)
			continue
		}
		if err := r.updateCache(repoUrl, registry); err != nil {
			fmt.Fprintf(os.Stderr, "cartridge repo index %s: %v\n", repoUrl, err)
		}
	}
}

// isCacheStale checks if a cache is stale
func (r *CartridgeRepo) isCacheStale(cache *CartridgeRepoCache) bool {
	return time.Since(cache.lastUpdated) > r.cacheTTL
}

// GetSuggestionsForCap gets cartridge suggestions for a cap URN.
//
// The request URN is parsed via NewCapUrnFromString; the parsed-and-
// re-serialized form is the canonical key used to look up the
// cap-to-cartridges index. Each declared cap is parsed and matched on
// IsEquivalent (suggestion lookup uses exact-match URNs). A malformed
// input URN logs and returns an empty result rather than masking the
// error.
func (r *CartridgeRepo) GetSuggestionsForCap(capUrn string) []CartridgeSuggestion {
	requested, err := urn.NewCapUrnFromString(capUrn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetSuggestionsForCap: invalid cap URN %q: %v\n", capUrn, err)
		return []CartridgeSuggestion{}
	}
	normalized := requested.String()

	r.mu.RLock()
	defer r.mu.RUnlock()

	suggestions := make([]CartridgeSuggestion, 0)

	for _, cache := range r.caches {
		keys, ok := cache.capToCartridges[normalized]
		if !ok {
			continue
		}

		for _, key := range keys {
			cartridge, ok := cache.cartridges[key]
			if !ok {
				continue
			}

			for _, capInfo := range cartridge.IterCaps() {
				parsed, perr := urn.NewCapUrnFromString(capInfo.Urn)
				if perr != nil {
					continue
				}
				if !parsed.IsEquivalent(requested) {
					continue
				}
				pageUrl := cartridge.PageUrl
				if pageUrl == "" {
					pageUrl = cache.repoUrl
				}
				suggestions = append(suggestions, CartridgeSuggestion{
					CartridgeId:          key.Id,
					CartridgeName:        cartridge.Name,
					CartridgeDescription: cartridge.Description,
					CapUrn:               normalized,
					CapTitle:             capInfo.Title,
					LatestVersion:        cartridge.Version,
					RepoUrl:              cache.repoUrl,
					PageUrl:              pageUrl,
					Channel:              key.Channel,
				})
				break
			}
		}
	}

	return suggestions
}

// GetAllCartridges gets all available cartridges from all repos.
// Channel provenance is on each CartridgeInfo so consumers can render
// the release/nightly distinction without a separate lookup.
func (r *CartridgeRepo) GetAllCartridges() []CartridgeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cartridges := make([]CartridgeInfo, 0)
	for _, cache := range r.caches {
		for _, cartridge := range cache.cartridges {
			cartridges = append(cartridges, cartridge)
		}
	}
	return cartridges
}

// GetAllAvailableCaps gets all caps available from cartridges
func (r *CartridgeRepo) GetAllAvailableCaps() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	capsSet := make(map[string]bool)
	for _, cache := range r.caches {
		for cap := range cache.capToCartridges {
			capsSet[cap] = true
		}
	}

	caps := make([]string, 0, len(capsSet))
	for cap := range capsSet {
		caps = append(caps, cap)
	}
	sort.Strings(caps)
	return caps
}

// NeedsSync checks if any repo needs syncing (cache missing or stale)
func (r *CartridgeRepo) NeedsSync(repoUrls []string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, repoUrl := range repoUrls {
		cache, ok := r.caches[repoUrl]
		if !ok {
			return true
		}
		if r.isCacheStale(cache) {
			return true
		}
	}
	return false
}

// GetCartridge gets cartridge info by (channel, id). Channel is
// required because the same id can independently exist in both
// channels with different versions/metadata.
func (r *CartridgeRepo) GetCartridge(channel CartridgeChannel, cartridgeId string) *CartridgeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := CartridgeKey{Channel: channel, Id: cartridgeId}
	for _, cache := range r.caches {
		if cartridge, ok := cache.cartridges[key]; ok {
			return &cartridge
		}
	}
	return nil
}

// GetSuggestionsForMissingCaps returns suggestions for caps that aren't currently available.
func (r *CartridgeRepo) GetSuggestionsForMissingCaps(availableCaps, requestedCaps []string) []CartridgeSuggestion {
	availableSet := make(map[string]bool, len(availableCaps))
	for _, cap := range availableCaps {
		availableSet[cap] = true
	}

	var suggestions []CartridgeSuggestion
	for _, capUrn := range requestedCaps {
		if !availableSet[capUrn] {
			suggestions = append(suggestions, r.GetSuggestionsForCap(capUrn)...)
		}
	}
	return suggestions
}
