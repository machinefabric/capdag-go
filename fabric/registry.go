// Package fabric provides the unified fabric registry — cap definitions, media
// definitions, aliases and dims behind one type.
//
// All live in the same online registry — named by its anchor, and found where
// its charter's `definition` location says — share the same caching policy,
// the same manifest pin, and the same offline / clear-cache surface. They differ only in wire payload shape (`cap.Cap`,
// `media.StoredMediaDef`, `media.StoredAlias`, `media.StoredDim`) and in
// disk-cache subdirectory (`caps/`, `media/`, `aliases/`, `dims/`). Where the
// cache is on disk, and how an entry is written and read there, is cache.go.
//
// On miss, `GetCap` atomically fetches the cap AND every media URN it
// references; if any of those media defs cannot be fetched the cap is NOT
// cached. This keeps the cap cache consistent with its media-def footprint.
//
// A cap or media def is likewise cached only once every dim it names (`dims`)
// is cached, at the version the manifest pins, with the value it names. So a
// reader holding a cached definition can always read the documentation of the
// dims it was built from (`GetCachedDim`).
//
// Mirrors the reference implementation in capdag/src/fabric/registry.rs.
//
// # Where this diverges from the reference, and why
//
// Rust is one crate, so `fabric::registry` owns `StoredMediaDef`, `StoredAlias`
// and `Manifest` outright. Go's package graph does not allow that: `cap` and
// `media` both need those types, and a `fabric` package that owns them while
// also importing `cap` would be a cycle. So the DATA types stay in `media`
// (which imports nothing above it) and this package owns the REGISTRY. The
// behavior is identical; only the declaration site moves, which is an
// object-level divergence the parity contract permits.
package fabric

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/machinefabric/capdag-go/bifaci"
	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/internal/cachefile"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

// DefaultFabricAnchor is the anchor of MachineFabric's production fabric, used
// when nothing names another. Only the anchor: where its objects are is read
// from its charter.
const DefaultFabricAnchor = "https://fabric.capdag.com"

// CacheDurationHours is the TTL applied to cache entries of version 0.
// Versioned entries (defver >= 1) are immutable and never expire.
const CacheDurationHours = 24

const httpTimeout = 10 * time.Second

// ---------------------------------------------------------------------------
// Errors
//
// Each kind is distinguishable because callers branch on them: `CapExists`
// swallows NotFound but not a transport failure, and the CLI reports a blocked
// network differently from a missing cap.
// ---------------------------------------------------------------------------

// ErrorKind classifies a registry failure.
type ErrorKind string

const (
	ErrNetworkBlocked ErrorKind = "network_blocked"
	// ErrNotFound: the registry's answer is that it does not hold the
	// definition — the pinned manifest does not name it, or the registry
	// answered 404 (or 410) for it. Nothing else is reported as missing.
	ErrNotFound ErrorKind = "not_found"
	// ErrFetchFailed: a fetch brought back no body — the registry refused it,
	// or did not answer. Its Err is the *FetchFailure saying what, where, for
	// which manifest, and why.
	ErrFetchFailed ErrorKind = "fetch_failed"
	// ErrPending: the pinned manifest names the definition, it is not in
	// memory, and it has not been fetched. Only a synchronous lookup, which
	// never touches the network, says this.
	ErrPending    ErrorKind = "pending"
	ErrParse      ErrorKind = "parse"
	ErrCache      ErrorKind = "cache"
	ErrValidation ErrorKind = "validation"
	ErrExtension  ErrorKind = "extension_not_found"
	// ErrCharter: the registry's charter could not be had, or was refused.
	// Nothing the registry serves is used without it.
	ErrCharter ErrorKind = "charter"
)

// Error is a registry failure carrying the kind callers branch on.
type Error struct {
	Kind    ErrorKind
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil && e.Message == "" {
		return e.Err.Error()
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// FetchCauseKind says why a fetch from the registry brought back no body.
type FetchCauseKind string

const (
	// FetchStatus: the registry answered with a status that is neither a
	// success nor an answer that the object is absent (404, 410).
	FetchStatus FetchCauseKind = "status"
	// FetchTimedOut: the registry did not answer before the client's timeout.
	FetchTimedOut FetchCauseKind = "timed_out"
	// FetchTransport: the request could not be made or its answer could not
	// be read — the connection was refused or broke off.
	FetchTransport FetchCauseKind = "transport"
)

// FetchFailure is one fetch from the registry that brought back no body: what
// was being fetched, from where, for which fabric manifest, and why.
type FetchFailure struct {
	// What was fetched, e.g. "media def 'media:pdf' (defver 3)" or
	// "manifest v10".
	What            string
	URL             string
	ManifestVersion uint32
	Cause           FetchCauseKind
	// Status is the HTTP status, for FetchStatus.
	Status int
	// Detail is what happened on the connection, for FetchTransport.
	Detail string
}

func (f *FetchFailure) Error() string {
	switch f.Cause {
	case FetchStatus:
		reason := http.StatusText(f.Status)
		if reason != "" {
			reason = " " + reason
		}
		return fmt.Sprintf("the registry refused the fetch of %s for fabric manifest v%d: HTTP %d%s from %s",
			f.What, f.ManifestVersion, f.Status, reason, f.URL)
	case FetchTimedOut:
		return fmt.Sprintf("the registry did not answer the fetch of %s for fabric manifest v%d: the request to %s timed out",
			f.What, f.ManifestVersion, f.URL)
	default:
		return fmt.Sprintf("the fetch of %s for fabric manifest v%d from %s failed: %s",
			f.What, f.ManifestVersion, f.URL, f.Detail)
	}
}

// AsFetchFailure returns the failed fetch err carries, if it is one.
func AsFetchFailure(err error) (*FetchFailure, bool) {
	var failure *FetchFailure
	if errors.As(err, &failure) {
		return failure, true
	}
	return nil, false
}

// fetchObject returns the body of one registry object — what at url, fetched
// for fabric manifest manifestVersion.
//
// A success is the body. 404 or 410 is the registry's answer that it does not
// hold the object, and only that is ErrNotFound. Any other status, and a fetch
// that got no answer, is ErrFetchFailed carrying a *FetchFailure: a refused
// fetch is never reported as a missing definition.
func fetchObject(client *http.Client, url, what string, manifestVersion uint32) ([]byte, error) {
	failed := func(cause FetchCauseKind, status int, detail string) error {
		return &Error{Kind: ErrFetchFailed, Err: &FetchFailure{
			What: what, URL: url, ManifestVersion: manifestVersion,
			Cause: cause, Status: status, Detail: detail,
		}}
	}
	transport := func(err error) error {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return failed(FetchTimedOut, 0, "")
		}
		return failed(FetchTransport, 0, err.Error())
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, transport(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, transport(err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, nil
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		return nil, errf(ErrNotFound, "the registry has no %s for fabric manifest v%d (HTTP %d from %s)",
			what, manifestVersion, resp.StatusCode, url)
	}
	return nil, failed(FetchStatus, resp.StatusCode, "")
}

func (e *Error) Unwrap() error { return e.Err }

func errf(kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

// IsNotFound reports whether err is a registry not-found.
func IsNotFound(err error) bool { return kindOf(err) == ErrNotFound }

func kindOf(err error) ErrorKind {
	var e *Error
	for err != nil {
		if casted, ok := err.(*Error); ok {
			e = casted
			break
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = unwrapper.Unwrap()
	}
	if e == nil {
		return ""
	}
	return e.Kind
}

// ---------------------------------------------------------------------------
// URN normalization
// ---------------------------------------------------------------------------

// NormalizeCapURN returns a cap URN's canonical string form.
//
// A URN that fails to parse is malformed, and this reports that. Passing the
// raw string through would let the lookup proceed and resurface downstream as a
// misleading "not part of manifest" instead of the truth.
func NormalizeCapURN(s string) (string, error) {
	parsed, err := urn.NewCapUrnFromString(s)
	if err != nil {
		return "", &Error{Kind: ErrParse, Message: fmt.Sprintf("malformed cap URN '%s'", s), Err: err}
	}
	return parsed.String(), nil
}

// NormalizeMediaURN returns a media URN's canonical string form. See
// NormalizeCapURN for the rationale on malformed input.
func NormalizeMediaURN(s string) (string, error) {
	parsed, err := urn.NewMediaUrnFromString(s)
	if err != nil {
		return "", &Error{Kind: ErrParse, Message: fmt.Sprintf("malformed media URN '%s'", s), Err: err}
	}
	return parsed.String(), nil
}

// ---------------------------------------------------------------------------
// Cache entries
// ---------------------------------------------------------------------------

type capCacheEntry struct {
	Definition json.RawMessage `json:"definition"`
	CachedAt   int64           `json:"cached_at"`
	TTLHours   int             `json:"ttl_hours"`
}

func (e *capCacheEntry) isExpired() bool {
	return time.Now().Unix() > e.CachedAt+int64(e.TTLHours)*3600
}

type mediaCacheEntry struct {
	Spec     media.StoredMediaDef `json:"spec"`
	CachedAt int64                `json:"cached_at"`
	TTLHours int                  `json:"ttl_hours"`
}

func (e *mediaCacheEntry) isExpired() bool {
	return time.Now().Unix() > e.CachedAt+int64(e.TTLHours)*3600
}

type aliasCacheEntry struct {
	Alias    media.StoredAlias `json:"alias"`
	CachedAt int64             `json:"cached_at"`
	TTLHours int               `json:"ttl_hours"`
}

type dimCacheEntry struct {
	Dim      media.StoredDim `json:"dim"`
	CachedAt int64           `json:"cached_at"`
	TTLHours int             `json:"ttl_hours"`
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Config is what a fabric registry client is asked to use.
//
// The ANCHOR names the registry and is all a client is told; where its
// manifest and definitions are, and the folder its cache lives in, come from
// the registry's charter (Locations). Sources, in priority order: WithAnchor,
// then CDG_FABRIC_REGISTRY_URL, then MachineFabric's production fabric.
type Config struct {
	Anchor string
}

// DefaultConfig reads the environment, falling back to the production fabric.
func DefaultConfig() Config {
	anchor := os.Getenv("CDG_FABRIC_REGISTRY_URL")
	if anchor == "" {
		anchor = DefaultFabricAnchor
	}
	return Config{Anchor: anchor}
}

// WithAnchor points the config at another fabric registry.
func (c Config) WithAnchor(anchor string) Config {
	c.Anchor = anchor
	return c
}

// Locations is where a fabric registry's objects are, as its charter states
// them. Built once from a charter whose shape check guarantees each template
// uses exactly the placeholders filled here — so filling one cannot fail, and
// nothing in the client composes a fabric URL any other way.
type Locations struct {
	manifest   string
	definition string
}

// LocationsFromCharter reads a fabric charter's locations.
func LocationsFromCharter(charter *bifaci.Charter) (Locations, error) {
	if charter.Kind != bifaci.RegistryKindFabric {
		return Locations{}, errf(ErrCharter, "%s is chartered as a %s registry, not a fabric",
			charter.Anchor, charter.Kind)
	}
	take := func(name string) (string, error) {
		template, ok := charter.Locations[name]
		if !ok {
			return "", errf(ErrCharter, "the fabric charter of %s declares no '%s' location", charter.Anchor, name)
		}
		return template, nil
	}
	manifest, err := take("manifest")
	if err != nil {
		return Locations{}, err
	}
	definition, err := take("definition")
	if err != nil {
		return Locations{}, err
	}
	return Locations{manifest: manifest, definition: definition}, nil
}

// Manifest is the URL of manifest version.
func (l Locations) Manifest(version uint32) string {
	return strings.ReplaceAll(l.manifest, "{manifest_version}", strconv.FormatUint(uint64(version), 10))
}

// Definition is the URL of one definition: kind is caps, media, aliases or dims.
func (l Locations) Definition(kind, urnHash string, version uint32) string {
	out := strings.ReplaceAll(l.definition, "{kind}", kind)
	out = strings.ReplaceAll(out, "{urn_hash}", urnHash)
	return strings.ReplaceAll(out, "{version}", strconv.FormatUint(uint64(version), 10))
}

// PublisherLayoutCharter is the charter of a fabric published at anchor the
// way the publisher lays a fabric out (fabric/src/fabric.js). For a registry
// that is never fetched: the test constructors, and tests of where things are.
func PublisherLayoutCharter(anchor string) *bifaci.Charter {
	return &bifaci.Charter{
		Format:      bifaci.CharterFormat,
		Anchor:      anchor,
		Kind:        bifaci.RegistryKindFabric,
		Slug:        "test-fabric",
		Environment: "prod",
		Locations: map[string]string{
			"manifest":   anchor + "/manifest/{manifest_version}.json",
			"latest":     anchor + "/manifest/latest.json",
			"definition": anchor + "/{kind}/{urn_hash}/{version}.json",
		},
	}
}

// definitionURLAndCachePath is the URL of one definition at defver, from the
// charter's `definition` location, and the cache file it is kept in. The fetch
// path and the cache-path resolver both go through here so the two can never
// drift apart. `key` is what the kind hashes: the normalized URN for caps and
// media, the name for aliases and dims.
func definitionURLAndCachePath(cacheDir string, locations Locations, kind, key string, defver uint32) (string, string) {
	return locations.Definition(kind, keyHash(key), defver), entryPath(cacheDir, kind, key, defver)
}

// BakedManifestVersion is the workspace-pinned fabric manifest version.
//
// Rust bakes this into the binary at compile time; Go has no build-time
// codegen, so a shipped binary carries it through the linker
// (-X github.com/machinefabric/capdag-go/fabric.BakedManifestVersion=N) and a
// workspace build reads MFR_FABRIC_MANIFEST_VERSION, the same variable the workspace build tool
// exports for every other language. Neither is a fallback for the other: both
// are explicit declarations, and their absence is a hard failure rather than a
// silent drop to v0, which names no manifest and would report "not found" for
// the whole fabric.
var BakedManifestVersion string

// ManifestVersion resolves the pinned fabric manifest version, or reports why
// it cannot.
func ManifestVersion() (uint32, error) {
	raw := BakedManifestVersion
	if raw == "" {
		raw = os.Getenv("MFR_FABRIC_MANIFEST_VERSION")
	}
	if raw == "" {
		return 0, errf(ErrValidation,
			"the fabric manifest version is not set. A packaged build bakes it via "+
				"-ldflags '-X github.com/machinefabric/capdag-go/fabric.BakedManifestVersion=N'; "+
				"a workspace build reads MFR_FABRIC_MANIFEST_VERSION, which the workspace build tool exports "+
				"from fabric/manifest-version.txt")
	}
	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, errf(ErrValidation, "the fabric manifest version must be an integer, got %q", raw)
	}
	return uint32(parsed), nil
}

// ---------------------------------------------------------------------------
// FabricRegistry
// ---------------------------------------------------------------------------

// FabricRegistry is the unified cap + media-def registry.
type FabricRegistry struct {
	mu     sync.RWMutex
	client *http.Client

	// cacheDir is the cache folder of the manifest version this registry is
	// pinned at, `<os cache>/capdag/<charter slug>/v<N>`. Definitions live at
	// `<kind>/<sha>/<defver>.json` and the manifest at `manifest.json`
	// (cache.go).
	cacheDir string

	config Config
	// charter is the registry's charter: its slug names the folder cacheDir is
	// in, and its locations are where every object is fetched from.
	charter   *bifaci.Charter
	locations Locations

	cachedCaps    map[string]*cap.Cap
	cachedSpecs   map[string]media.StoredMediaDef
	cachedAliases map[string]media.StoredAlias
	// cachedDims maps a dim name to its body, filtered to the pinned
	// manifest's defvers.
	cachedDims map[string]media.StoredDim
	extIndex   map[string][]string

	// manifestVersion is >= 1: every URN lookup resolves to a (urn, defver)
	// pair via the manifest before fetching.
	manifestVersion uint32
	manifest        *media.Manifest

	// fetchFailures keeps what the last failed fetch of each definition came
	// to, until a fetch of it succeeds. A synchronous lookup says this rather
	// than "not fetched yet" when the registry already answered.
	fetchFailures map[fetchKey]error

	offline bool
}

// fetchKey names one definition at one version: its kind ("cap", "media",
// "alias"), its normalized name, and its defver.
type fetchKey struct {
	kind   string
	name   string
	defver uint32
}

// note keeps what a fetch of key came to: a failure is remembered, and a
// success forgets the failure before it.
func (r *FabricRegistry) note(key fetchKey, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		delete(r.fetchFailures, key)
	} else {
		r.fetchFailures[key] = err
	}
}

// notAtHand is why a definition the manifest names is not in memory: what its
// last fetch came to, or ErrPending when it has not been fetched.
func (r *FabricRegistry) notAtHand(key fetchKey, what, url string) error {
	r.mu.RLock()
	last, failed := r.fetchFailures[key]
	r.mu.RUnlock()
	if failed {
		return last
	}
	return errf(ErrPending, "%s is named by manifest v%d and has not been fetched from %s",
		what, r.manifestVersion, url)
}

// New builds a registry with the default config, pinned at the workspace-baked
// manifest version.
func New() (*FabricRegistry, error) {
	version, err := ManifestVersion()
	if err != nil {
		return nil, err
	}
	return WithConfigAndManifestVersion(DefaultConfig(), version)
}

// WithConfig builds a registry with a custom config, pinned at the
// workspace-baked manifest version.
func WithConfig(config Config) (*FabricRegistry, error) {
	version, err := ManifestVersion()
	if err != nil {
		return nil, err
	}
	return WithConfigAndManifestVersion(config, version)
}

// WithConfigAndManifestVersion is the full constructor.
//
// The registry's charter is resolved first — the one stored in the cache when
// one is stored for this anchor, else fetched from the anchor — and everything
// after it is read from the charter: the cache folder is the manifest
// version's folder in the folder its slug names, and the manifest is fetched
// from its location. The constructor blocks on the network when either is not
// cached. A version below 1 names no manifest and is refused. A cached entry
// that is corrupt (cache.go) fails construction with an error naming its file.
func WithConfigAndManifestVersion(config Config, manifestVersion uint32) (*FabricRegistry, error) {
	if manifestVersion == 0 {
		return nil, errf(ErrValidation,
			"fabric manifest version 0 names no manifest; a registry is pinned at 1 or later")
	}
	client := &http.Client{Timeout: httpTimeout}
	capdagCache, err := CapdagCacheRoot()
	if err != nil {
		return nil, err
	}
	charter, err := resolveCharter(client, capdagCache, config.Anchor)
	if err != nil {
		return nil, err
	}
	locations, err := LocationsFromCharter(charter)
	if err != nil {
		return nil, err
	}
	cacheDir := versionDir(filepath.Join(capdagCache, charter.Slug), manifestVersion)
	for _, kind := range kindFolders {
		if err := os.MkdirAll(filepath.Join(cacheDir, kind), 0o755); err != nil {
			return nil, errf(ErrCache, "creating cache dir %s: %v", filepath.Join(cacheDir, kind), err)
		}
	}

	manifest, err := loadOrFetchManifest(cacheDir, client, locations, manifestVersion)
	if err != nil {
		return nil, err
	}

	// Hydrate from the cache only what the manifest pins, each from the one
	// file its key and defver name, so nothing at another defver, and nothing
	// another manifest version cached, is ever read.
	pinned, err := loadPinnedEntries(cacheDir, manifest)
	if err != nil {
		return nil, err
	}
	// Keep a definition only when every dim it names survives with it.
	retainPinned(manifest, manifestVersion, pinned.caps, pinned.media, pinned.aliases, pinned.dims)

	r := &FabricRegistry{
		client:          client,
		cacheDir:        cacheDir,
		config:          config,
		charter:         charter,
		locations:       locations,
		cachedCaps:      pinned.caps,
		cachedSpecs:     pinned.media,
		cachedAliases:   pinned.aliases,
		cachedDims:      pinned.dims,
		extIndex:        make(map[string][]string),
		manifestVersion: manifestVersion,
		manifest:        manifest,
		fetchFailures:   make(map[fetchKey]error),
	}
	r.mu.Lock()
	for _, spec := range r.cachedSpecs {
		r.updateExtensionIndexLocked(spec)
	}
	r.mu.Unlock()

	r.EnsureIdentityCap()
	return r, nil
}

// resolveCharter is the charter of the fabric registry at anchor.
//
// A fabric's contents carry no signature, so no content environment is
// expected of its charter: any chartered fabric can be resolved against, and
// what keeps one client's registries on one fabric is each cartridge
// registry's charter naming it.
//
// The stored one, when a folder under capdagCache holds a charter for this
// anchor — so an offline client keeps its fabric. Else the charter is fetched
// from the anchor and stored in the folder its slug names. Matches Rust: fn
// resolve_charter (which also verifies the signature; see bifaci/charter.go).
func resolveCharter(client *http.Client, capdagCache, anchor string) (*bifaci.Charter, error) {
	if _, charter, ok := bifaci.StoredCharterFor(capdagCache, anchor, bifaci.RegistryKindFabric); ok {
		return charter, nil
	}
	fetched, err := bifaci.FetchCharter(client, anchor, bifaci.RegistryKindFabric)
	if err != nil {
		return nil, &Error{Kind: ErrCharter, Message: fmt.Sprintf("the fabric registry %s has no charter this client can use", anchor), Err: err}
	}
	folder := filepath.Join(capdagCache, fetched.Charter.Slug)
	if err := bifaci.StoreCharter(fetched, folder); err != nil {
		return nil, errf(ErrCache, "storing the charter of %s in %s: %v", anchor, folder, err)
	}
	return fetched.Charter, nil
}

// loadOrFetchManifest is the manifest bootstrap, in the cache folder cacheDir
// of manifest version. It tries the cached manifest first, then a blocking
// network GET; if neither produces a manifest it is an error — there is no v0
// fallback. A cached manifest that does not parse, or is another version's, is
// corrupt: an error naming its file, never a silent refetch. The fetched bytes
// are written whole (writeAtomically). Matches Rust: fn load_or_fetch_manifest
func loadOrFetchManifest(cacheDir string, client *http.Client, locations Locations, version uint32) (*media.Manifest, error) {
	cacheFile := filepath.Join(cacheDir, manifestFile)
	cached, err := readEntry[media.Manifest](cacheFile, "manifest")
	if err != nil {
		return nil, err
	}
	if cached != nil {
		if cached.Version != version {
			return nil, corrupt(cacheFile, "manifest",
				fmt.Sprintf("it is manifest %d, in the folder of manifest %d", cached.Version, version))
		}
		return cached, nil
	}

	url := locations.Manifest(version)
	body, err := fetchObject(client, url, fmt.Sprintf("manifest v%d", version), version)
	if err != nil {
		return nil, err
	}
	var m media.Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, &Error{Kind: ErrParse, Message: fmt.Sprintf("parsing manifest v%d", version), Err: err}
	}
	if m.Version != version {
		return nil, errf(ErrParse, "manifest fetched as v%d reports version %d", version, m.Version)
	}
	if err := writeAtomically(cacheFile, body); err != nil {
		return nil, err
	}
	return &m, nil
}

// CapdagCacheRoot is the directory every registry's cache folder lives under:
// `<os cache>/capdag`. A registry's own folder is its charter's slug, so two
// registries never share one, whatever their URLs look like — a cache
// populated from one fabric can never satisfy lookups against another, which
// serves DIFFERENT bytes for the same URN and version.
func CapdagCacheRoot() (string, error) {
	root, err := cachefile.CapdagRoot()
	if err != nil {
		return "", errf(ErrCache, "%v", err)
	}
	return root, nil
}

// ---------------------------------------------------------------------------
// Disk-cache hydration
// ---------------------------------------------------------------------------

// pinnedEntries are the definitions a manifest version's cache folder holds at
// the versions its manifest pins, keyed as the registry keys them in memory.
type pinnedEntries struct {
	caps    map[string]*cap.Cap
	media   map[string]media.StoredMediaDef
	aliases map[string]media.StoredAlias
	dims    map[string]media.StoredDim
}

// loadPinnedEntries reads, from the cache folder cacheDir of manifest, every
// definition the manifest pins that is cached: each from the one file its key
// and pinned defver name, so nothing at another defver, and nothing another
// manifest version cached, is ever read.
//
// An entry that is not there is not cached. One that is there and cannot be
// read, does not parse, or is not the definition and defver its path names is
// corrupt, and an error naming the file. Two rules drop an entry and leave it
// to be fetched anew, naming its file in the log:
//   - a defver-0 cap or media def older than its time to live (defver 0 is the
//     flat, mutable regime);
//   - a cap or dim that breaks the definition rules was written by a capdag
//     with other rules (capdag/docs/10-validation-rules.md); its file is
//     removed.
//
// Matches Rust: fn load_pinned_entries
func loadPinnedEntries(cacheDir string, manifest *media.Manifest) (pinnedEntries, error) {
	entries := pinnedEntries{
		caps:    make(map[string]*cap.Cap),
		media:   make(map[string]media.StoredMediaDef),
		aliases: make(map[string]media.StoredAlias),
		dims:    make(map[string]media.StoredDim),
	}
	for key, defver := range manifest.Caps {
		path := entryPath(cacheDir, "caps", key, defver)
		entry, err := readEntry[capCacheEntry](path, "cap")
		if err != nil {
			return pinnedEntries{}, err
		}
		if entry == nil {
			continue
		}
		var c cap.Cap
		if err := json.Unmarshal(entry.Definition, &c); err != nil {
			return pinnedEntries{}, corrupt(path, "cap", err.Error())
		}
		cachedURN, err := NormalizeCapURN(c.Urn.String())
		if err != nil {
			return pinnedEntries{}, corrupt(path, "cap", err.Error())
		}
		if cachedURN != key || c.Version != defver {
			return pinnedEntries{}, corrupt(path, "cap", fmt.Sprintf(
				"it holds '%s' at defver %d, where '%s' at defver %d belongs",
				cachedURN, c.Version, key, defver))
		}
		if defver == 0 && entry.isExpired() {
			fmt.Fprintf(os.Stderr, "[INFO] the cached cap %s is past its time to live\n", path)
			continue
		}
		if err := cap.ValidateCapDefinition(&c); err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] dropping the cached cap %s: %v\n", path, err)
			_ = os.Remove(path)
			continue
		}
		entries.caps[cachedURN] = &c
	}
	for key, defver := range manifest.Media {
		path := entryPath(cacheDir, "media", key, defver)
		entry, err := readEntry[mediaCacheEntry](path, "media def")
		if err != nil {
			return pinnedEntries{}, err
		}
		if entry == nil {
			continue
		}
		cachedURN, err := NormalizeMediaURN(entry.Spec.Urn)
		if err != nil {
			return pinnedEntries{}, corrupt(path, "media def", err.Error())
		}
		if cachedURN != key || entry.Spec.Version != defver {
			return pinnedEntries{}, corrupt(path, "media def", fmt.Sprintf(
				"it holds '%s' at defver %d, where '%s' at defver %d belongs",
				cachedURN, entry.Spec.Version, key, defver))
		}
		if defver == 0 && entry.isExpired() {
			fmt.Fprintf(os.Stderr, "[INFO] the cached media def %s is past its time to live\n", path)
			continue
		}
		entries.media[cachedURN] = entry.Spec
	}
	for name, defver := range manifest.Aliases {
		path := entryPath(cacheDir, "aliases", name, defver)
		entry, err := readEntry[aliasCacheEntry](path, "alias")
		if err != nil {
			return pinnedEntries{}, err
		}
		if entry == nil {
			continue
		}
		if err := validateFetchedAlias(&entry.Alias, name, defver); err != nil {
			return pinnedEntries{}, corrupt(path, "alias", err.Error())
		}
		entries.aliases[entry.Alias.Name] = entry.Alias
	}
	for name, defver := range manifest.Dims {
		path := entryPath(cacheDir, "dims", name, defver)
		entry, err := readEntry[dimCacheEntry](path, "dim")
		if err != nil {
			return pinnedEntries{}, err
		}
		if entry == nil {
			continue
		}
		if entry.Dim.Name != name || entry.Dim.Version != defver {
			return pinnedEntries{}, corrupt(path, "dim", fmt.Sprintf(
				"it holds '%s' at defver %d, where '%s' at defver %d belongs",
				entry.Dim.Name, entry.Dim.Version, name, defver))
		}
		if err := entry.Dim.Validate(); err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] dropping the cached dim %s: %v\n", path, err)
			_ = os.Remove(path)
			continue
		}
		entries.dims[entry.Dim.Name] = entry.Dim
	}
	return entries, nil
}

// retainPinned keeps, of what was loaded from the disk cache, only what the
// pinned manifest names at the version it names, and only definitions whose
// dims all survive with them. At v0 the manifest is empty and caps and media
// are all retained; aliases and dims are versioned-regime concepts and are all
// dropped.
//
// A definition whose dims did not survive is dropped here and fetched again,
// with its dims, when it is next asked for — so a cached definition always has
// its dims cached. Matches Rust: fn retain_pinned
func retainPinned(
	manifest *media.Manifest,
	manifestVersion uint32,
	caps map[string]*cap.Cap,
	specs map[string]media.StoredMediaDef,
	aliases map[string]media.StoredAlias,
	dims map[string]media.StoredDim,
) {
	if manifestVersion >= 1 {
		for key, c := range caps {
			if manifest.Caps[key] != c.Version {
				delete(caps, key)
			}
		}
		for key, spec := range specs {
			if manifest.Media[key] != spec.Version {
				delete(specs, key)
			}
		}
		for name, alias := range aliases {
			if manifest.Aliases[name] != alias.Version {
				delete(aliases, name)
			}
		}
		for name, dim := range dims {
			if manifest.Dims[name] != dim.Version {
				delete(dims, name)
			}
		}
	} else {
		clear(aliases)
		clear(dims)
	}
	for key, c := range caps {
		if !dimsResolveIn(c.Dims, dims) {
			delete(caps, key)
		}
	}
	for key, spec := range specs {
		if !dimsResolveIn(spec.Dims, dims) {
			delete(specs, key)
		}
	}
}

// dimsResolveIn reports whether every dim refs names is in cached with the
// value it names. Matches Rust: fn dims_resolve_in
func dimsResolveIn(refs []media.DimRef, cached map[string]media.StoredDim) bool {
	for _, ref := range refs {
		dim, ok := cached[ref.Dim]
		if !ok || dim.Value(ref.Value) == nil {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Disk-cache writers
// ---------------------------------------------------------------------------

func (r *FabricRegistry) capCacheFilePath(capURN string, defver uint32) (string, error) {
	normalized, err := NormalizeCapURN(capURN)
	if err != nil {
		return "", err
	}
	_, path := definitionURLAndCachePath(r.cacheDir, r.locations, "caps", normalized, defver)
	return path, nil
}

func (r *FabricRegistry) mediaCacheFilePath(mediaURN string, defver uint32) (string, error) {
	normalized, err := NormalizeMediaURN(mediaURN)
	if err != nil {
		return "", err
	}
	_, path := definitionURLAndCachePath(r.cacheDir, r.locations, "media", normalized, defver)
	return path, nil
}

func (r *FabricRegistry) aliasCacheFilePath(normalizedName string, defver uint32) string {
	_, path := definitionURLAndCachePath(r.cacheDir, r.locations, "aliases", normalizedName, defver)
	return path
}

func (r *FabricRegistry) saveCapToCache(c *cap.Cap) error {
	path, err := r.capCacheFilePath(c.Urn.String(), c.Version)
	if err != nil {
		return err
	}
	definition, err := json.Marshal(c)
	if err != nil {
		return errf(ErrCache, "serializing cap %s: %v", c.Urn.String(), err)
	}
	return storeEntry(path, capCacheEntry{
		Definition: definition,
		CachedAt:   time.Now().Unix(),
		TTLHours:   CacheDurationHours,
	}, "cap")
}

func (r *FabricRegistry) saveMediaDefToCache(spec media.StoredMediaDef) error {
	path, err := r.mediaCacheFilePath(spec.Urn, spec.Version)
	if err != nil {
		return err
	}
	return storeEntry(path, mediaCacheEntry{
		Spec:     spec,
		CachedAt: time.Now().Unix(),
		TTLHours: CacheDurationHours,
	}, "media def")
}

func (r *FabricRegistry) saveAliasToCache(alias media.StoredAlias) error {
	return storeEntry(r.aliasCacheFilePath(alias.Name, alias.Version), aliasCacheEntry{
		Alias:    alias,
		CachedAt: time.Now().Unix(),
		TTLHours: CacheDurationHours,
	}, "alias")
}

func (r *FabricRegistry) saveDimToCache(dim media.StoredDim) error {
	_, path := definitionURLAndCachePath(r.cacheDir, r.locations, "dims", dim.Name, dim.Version)
	return storeEntry(path, dimCacheEntry{
		Dim:      dim,
		CachedAt: time.Now().Unix(),
		TTLHours: CacheDurationHours,
	}, "dim")
}

// ---------------------------------------------------------------------------
// Extension index
// ---------------------------------------------------------------------------

func (r *FabricRegistry) updateExtensionIndexLocked(spec media.StoredMediaDef) {
	for _, ext := range spec.Extensions {
		lower := strings.ToLower(ext)
		urns := r.extIndex[lower]
		found := false
		for _, existing := range urns {
			if existing == spec.Urn {
				found = true
				break
			}
		}
		if !found {
			r.extIndex[lower] = append(urns, spec.Urn)
		}
	}
}

// ---------------------------------------------------------------------------
// HTTP fetch
// ---------------------------------------------------------------------------

func (r *FabricRegistry) get(url string, what string) ([]byte, error) {
	r.mu.RLock()
	offline := r.offline
	r.mu.RUnlock()
	if offline {
		return nil, errf(ErrNetworkBlocked, "network access blocked while offline: cannot fetch %s", what)
	}
	return fetchObject(r.client, url, what, r.manifestVersion)
}

func (r *FabricRegistry) fetchCapFromRegistry(capURN string, defver uint32) (*cap.Cap, error) {
	normalized, err := NormalizeCapURN(capURN)
	if err != nil {
		return nil, err
	}
	url, _ := definitionURLAndCachePath(r.cacheDir, r.locations, "caps", normalized, defver)
	body, err := r.get(url, fmt.Sprintf("cap '%s' (defver %d)", normalized, defver))
	if err != nil {
		return nil, err
	}
	return checkedCapDefinition(body, capURN)
}

// checkedCapDefinition reads a definition a registry served and holds it to
// the definition rules (RULE1-RULE22) before it is cached or returned: a
// definition is held to them by the side that receives it as well as the side
// that sends it. One that does not parse is a parse error; one that parses
// and breaks a rule is a validation error naming the cap and the rule.
// Matches Rust: fn checked_cap_definition(body, normalized_urn)
func checkedCapDefinition(body []byte, capURN string) (*cap.Cap, error) {
	var wire cap.RegistryCapResponse
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, &Error{Kind: ErrParse, Message: fmt.Sprintf("parsing the registry response for cap '%s'", capURN), Err: err}
	}
	c, err := wire.ToCap()
	if err != nil {
		return nil, &Error{Kind: ErrParse, Message: fmt.Sprintf("converting the registry response for cap '%s'", capURN), Err: err}
	}
	if err := cap.ValidateCapDefinition(c); err != nil {
		return nil, &Error{Kind: ErrValidation, Message: fmt.Sprintf("the registry's definition of cap '%s' breaks a definition rule", capURN), Err: err}
	}
	return c, nil
}

func (r *FabricRegistry) fetchMediaDefFromRegistry(mediaURN string, defver uint32) (*media.StoredMediaDef, error) {
	normalized, err := NormalizeMediaURN(mediaURN)
	if err != nil {
		return nil, err
	}
	url, _ := definitionURLAndCachePath(r.cacheDir, r.locations, "media", normalized, defver)
	body, err := r.get(url, fmt.Sprintf("media def '%s' (defver %d)", normalized, defver))
	if err != nil {
		return nil, err
	}
	var spec media.StoredMediaDef
	if err := json.Unmarshal(body, &spec); err != nil {
		return nil, &Error{Kind: ErrParse, Message: fmt.Sprintf("parsing the registry response for media '%s'", mediaURN), Err: err}
	}
	return &spec, nil
}

func (r *FabricRegistry) fetchAliasFromRegistry(normalizedName string, defver uint32) (*media.StoredAlias, error) {
	if defver < 1 {
		return nil, errf(ErrNotFound,
			"alias '%s' has non-positive defver %d; aliases are versioned-only", normalizedName, defver)
	}
	url, _ := definitionURLAndCachePath(r.cacheDir, r.locations, "aliases", normalizedName, defver)
	body, err := r.get(url, fmt.Sprintf("alias '%s' (defver %d)", normalizedName, defver))
	if err != nil {
		return nil, err
	}
	var alias media.StoredAlias
	if err := json.Unmarshal(body, &alias); err != nil {
		return nil, &Error{Kind: ErrParse, Message: fmt.Sprintf("parsing alias '%s'", normalizedName), Err: err}
	}
	if err := validateFetchedAlias(&alias, normalizedName, defver); err != nil {
		return nil, err
	}
	return &alias, nil
}

// validateFetchedAlias holds an alias body to what was asked for: the name and
// defver the manifest pins, and a target that is a cap or a media URN. A
// mismatched object means the registry (or the cache) holds a different
// definition than the manifest pinned. Matches Rust: fn validate_fetched_alias
func validateFetchedAlias(alias *media.StoredAlias, expectedName string, expectedDefver uint32) error {
	if alias.Name != expectedName {
		return errf(ErrParse, "alias object name '%s' does not match the requested name '%s'",
			alias.Name, expectedName)
	}
	if alias.Version != expectedDefver {
		return errf(ErrParse, "alias '%s' object reports version %d but the manifest pins defver %d",
			alias.Name, alias.Version, expectedDefver)
	}
	if _, ok := media.ClassifyAliasTarget(alias.Target); !ok {
		return errf(ErrValidation, "alias '%s' target '%s' is neither a cap nor a media URN",
			alias.Name, alias.Target)
	}
	return nil
}

// fetchDim fetches a single dim body at its pinned defver, checks it, and
// caches it in memory and on disk. Matches Rust: fn fetch_one_dim
func (r *FabricRegistry) fetchDim(name string, defver uint32) (*media.StoredDim, error) {
	if defver < 1 {
		return nil, errf(ErrNotFound,
			"dim '%s' has non-positive defver %d; dims are versioned-only", name, defver)
	}
	url, _ := definitionURLAndCachePath(r.cacheDir, r.locations, "dims", name, defver)
	body, err := r.get(url, fmt.Sprintf("dim '%s' (defver %d)", name, defver))
	if err != nil {
		return nil, err
	}
	dim, err := checkedDim(body, name, defver)
	if err != nil {
		return nil, err
	}
	if err := r.saveDimToCache(*dim); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.cachedDims[dim.Name] = *dim
	r.mu.Unlock()
	return dim, nil
}

// checkedDim reads a dim body as a registry served it, for the dim name at
// defver: parsed (unknown fields refused), named and versioned as requested —
// a registry that serves another object is a hard error, never silently
// accepted — and holding (StoredDim.Validate).
// Matches Rust: fn checked_dim(body, name, defver)
func checkedDim(body []byte, name string, defver uint32) (*media.StoredDim, error) {
	var dim media.StoredDim
	if err := json.Unmarshal(body, &dim); err != nil {
		return nil, &Error{Kind: ErrParse, Message: fmt.Sprintf("failed to parse dim '%s'", name), Err: err}
	}
	if dim.Name != name {
		return nil, errf(ErrParse, "dim object name '%s' does not match requested name '%s'", dim.Name, name)
	}
	if dim.Version != defver {
		return nil, errf(ErrParse, "dim '%s' object reports version %d but manifest pins defver %d",
			dim.Name, dim.Version, defver)
	}
	if err := dim.Validate(); err != nil {
		return nil, &Error{Kind: ErrValidation, Message: err.Error()}
	}
	return &dim, nil
}

// ensureDimsCached makes sure every dim a definition names is cached,
// fetching the ones that are not at the defver the manifest pins, and that
// each names a value the dim declares. owner names the definition in errors.
// Nothing about the definition itself is cached here: its fetcher caches it
// only once this succeeds. Matches Rust: fn ensure_dims_cached
func (r *FabricRegistry) ensureDimsCached(owner string, refs []media.DimRef) error {
	if err := media.CheckDimRefs(refs); err != nil {
		return errf(ErrValidation, "%s dims: %v", owner, err)
	}
	for _, ref := range refs {
		r.mu.RLock()
		cached, ok := r.cachedDims[ref.Dim]
		manifestVersion := r.manifestVersion
		defver, listed := r.manifest.Dims[ref.Dim]
		r.mu.RUnlock()
		dim := &cached
		if !ok {
			if manifestVersion == 0 {
				return errf(ErrNotFound,
					"%s names dim '%s', but the registry is pinned at v0 (dims are a versioned-regime concept)",
					owner, ref.Dim)
			}
			if !listed {
				return errf(ErrNotFound, "%s names dim '%s', which is not in manifest v%d",
					owner, ref.Dim, manifestVersion)
			}
			// The dim's own failure, as it is: a refused fetch stays a
			// refused fetch, and only the registry's 404 is ErrNotFound.
			fetched, err := r.fetchDim(ref.Dim, defver)
			if err != nil {
				return err
			}
			dim = fetched
		}
		if dim.Value(ref.Value) == nil {
			return errf(ErrValidation, "%s names value '%s' of dim '%s', which the dim does not declare",
				owner, ref.Value, ref.Dim)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Defver resolution (the manifest pin)
// ---------------------------------------------------------------------------

func (r *FabricRegistry) capDefver(normalizedURN string) (uint32, error) {
	if r.manifestVersion == 0 {
		return 0, nil
	}
	defver, ok := r.manifest.Caps[normalizedURN]
	if !ok {
		return 0, errf(ErrNotFound, "cap '%s' is not part of manifest v%d", normalizedURN, r.manifestVersion)
	}
	return defver, nil
}

func (r *FabricRegistry) mediaDefver(normalizedURN string) (uint32, error) {
	if r.manifestVersion == 0 {
		return 0, nil
	}
	// The bare `media:` wildcard is a sentinel with no published spec.
	if normalizedURN == "media:" {
		return 0, nil
	}
	defver, ok := r.manifest.Media[normalizedURN]
	if !ok {
		return 0, errf(ErrNotFound, "media def '%s' is not part of manifest v%d", normalizedURN, r.manifestVersion)
	}
	return defver, nil
}

func (r *FabricRegistry) aliasDefver(normalizedName string) (uint32, error) {
	if r.manifestVersion == 0 {
		return 0, errf(ErrNotFound,
			"alias '%s' cannot resolve: the registry is pinned at v0 (aliases are a versioned-regime concept)",
			normalizedName)
	}
	defver, ok := r.manifest.Aliases[normalizedName]
	if !ok {
		return 0, errf(ErrNotFound, "alias '%s' is not part of manifest v%d", normalizedName, r.manifestVersion)
	}
	return defver, nil
}

// CapDefverFor resolves a cap URN's pinned defver without fetching.
func (r *FabricRegistry) CapDefverFor(capURN string) (uint32, error) {
	normalized, err := NormalizeCapURN(capURN)
	if err != nil {
		return 0, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.capDefver(normalized)
}

// MediaDefverFor resolves a media URN's pinned defver without fetching.
func (r *FabricRegistry) MediaDefverFor(mediaURN string) (uint32, error) {
	normalized, err := NormalizeMediaURN(mediaURN)
	if err != nil {
		return 0, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mediaDefver(normalized)
}

// AliasDefverFor resolves an alias name's pinned defver without fetching.
func (r *FabricRegistry) AliasDefverFor(name string) (uint32, error) {
	normalized, err := media.NormalizeAliasName(name)
	if err != nil {
		return 0, errf(ErrValidation, "invalid alias name: %v", err)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.aliasDefver(normalized)
}

// dimDefver resolves a dim name to its defver under the pinned manifest. Dims
// exist only in the versioned regime: at v0 any dim lookup is a hard NotFound.
// At v >= 1 the name must be in the manifest's `dims` map. The caller holds
// r.mu.
func (r *FabricRegistry) dimDefver(name string) (uint32, error) {
	if r.manifestVersion == 0 {
		return 0, errf(ErrNotFound,
			"dim '%s' cannot resolve: registry is pinned at v0 (dims are a versioned-regime concept)", name)
	}
	defver, ok := r.manifest.Dims[name]
	if !ok {
		return 0, errf(ErrNotFound, "dim '%s' is not part of manifest v%d", name, r.manifestVersion)
	}
	return defver, nil
}

// DimDefverFor looks up a dim name's pinned defver under this registry's
// manifest without fetching the body.
func (r *FabricRegistry) DimDefverFor(name string) (uint32, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.dimDefver(name)
}

// ManifestVersion is the fabric manifest version this registry is pinned to.
func (r *FabricRegistry) ManifestVersion() uint32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.manifestVersion
}

// Config returns what this registry was asked to use.
func (r *FabricRegistry) Config() Config {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
}

// Charter is the charter of the registry this client reads.
func (r *FabricRegistry) Charter() *bifaci.Charter {
	return r.charter
}

// ---------------------------------------------------------------------------
// Atomic cap fetch — the core of the merged registry
// ---------------------------------------------------------------------------

// isWildcardMediaURN reports whether urnStr is the bare `media:` identity URN.
// The wildcard is not a fetchable spec — it is the universal input/output
// marker — so it is skipped from a cap's recursive fetch list.
func isWildcardMediaURN(urnStr string) bool {
	parsed, err := urn.NewMediaUrnFromString(urnStr)
	if err != nil {
		return false
	}
	return parsed.IsTop()
}

// collectReferencedMediaURNs returns every media URN a cap references, skipping
// the bare `media:` wildcard. Order is deterministic (in-spec, out-spec, args,
// stdin sources, output) and de-duplicated.
func collectReferencedMediaURNs(c *cap.Cap) ([]string, error) {
	var seen []string
	push := func(u string) error {
		if u == "" || isWildcardMediaURN(u) {
			return nil
		}
		normalized, err := NormalizeMediaURN(u)
		if err != nil {
			return err
		}
		for _, existing := range seen {
			if existing == normalized {
				return nil
			}
		}
		seen = append(seen, normalized)
		return nil
	}

	if err := push(c.Urn.InSpec()); err != nil {
		return nil, err
	}
	if err := push(c.Urn.OutSpec()); err != nil {
		return nil, err
	}
	// Every arg's own URN and the type it accepts (its stdin source's, or
	// its `accepts`), and every output's URN — the main one and the others
	// alike, since each is the type of a node a machine can hold.
	args := c.GetArgs()
	for i := range args {
		if err := push(args[i].MediaUrn); err != nil {
			return nil, err
		}
		if err := push(args[i].AcceptedUrn()); err != nil {
			return nil, err
		}
	}
	for _, output := range c.GetOutputs() {
		if err := push(output.MediaUrn); err != nil {
			return nil, err
		}
	}
	return seen, nil
}

// fetchCapAtomic fetches a cap and every media URN it references.
//
// On any failure — the cap itself or any referenced media spec — the cap is NOT
// cached and the error propagates. A cap cached without its media footprint
// would resolve and then fail at the first argument check, far from the cause.
func (r *FabricRegistry) fetchCapAtomic(capURN string) (*cap.Cap, error) {
	normalized, err := NormalizeCapURN(capURN)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	defver, err := r.capDefver(normalized)
	r.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	c, err := r.fetchCapAndFootprint(capURN, normalized, defver)
	r.note(fetchKey{"cap", normalized, defver}, err)
	return c, err
}

// fetchCapAndFootprint is fetchCapAtomic's work for the cap at defver.
func (r *FabricRegistry) fetchCapAndFootprint(capURN, normalized string, defver uint32) (*cap.Cap, error) {
	c, err := r.fetchCapFromRegistry(capURN, defver)
	if err != nil {
		return nil, err
	}

	referenced, err := collectReferencedMediaURNs(c)
	if err != nil {
		return nil, err
	}
	for _, mediaURN := range referenced {
		r.mu.RLock()
		_, alreadyCached := r.cachedSpecs[mediaURN]
		mediaDefver, defverErr := r.mediaDefver(mediaURN)
		r.mu.RUnlock()
		if alreadyCached {
			continue
		}
		if defverErr != nil {
			return nil, defverErr
		}
		if _, err := r.fetchAndCacheMediaDef(mediaURN, mediaDefver); err != nil {
			return nil, err
		}
	}

	// Every dim the cap names, at its pinned version, with the value named.
	if err := r.ensureDimsCached(fmt.Sprintf("cap '%s'", normalized), c.Dims); err != nil {
		return nil, err
	}

	// Every referenced media def and dim landed — now the cap may be cached.
	if err := r.saveCapToCache(c); err != nil {
		return nil, err
	}
	capNormalized, err := NormalizeCapURN(c.Urn.String())
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.cachedCaps[capNormalized] = c
	r.mu.Unlock()
	return c, nil
}

// ---------------------------------------------------------------------------
// Public cap API
// ---------------------------------------------------------------------------

// GetCap returns a cap by URN or alias, hitting the in-memory cache first.
//
// The argument may be a cap URN (cap:...) or an alias (a colon-free token). An
// alias resolves first; because this is the typed cap boundary, an alias whose
// target is not a cap URN is a hard error.
func (r *FabricRegistry) GetCap(urnOrAlias string) (*cap.Cap, error) {
	if media.IsAliasToken(urnOrAlias) {
		target, err := r.ResolveAliasTyped(urnOrAlias, media.AliasTargetCap)
		if err != nil {
			return nil, err
		}
		return r.GetCap(target)
	}

	normalized, err := NormalizeCapURN(urnOrAlias)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	cached, ok := r.cachedCaps[normalized]
	offline := r.offline
	_, defverErr := r.capDefver(normalized)
	r.mu.RUnlock()
	if ok {
		return cached, nil
	}
	// The manifest answers first: a cap it does not name is absent, offline
	// or not.
	if defverErr != nil {
		return nil, defverErr
	}
	if offline {
		return nil, errf(ErrNetworkBlocked, "network access blocked while offline: cannot fetch cap '%s'", urnOrAlias)
	}
	return r.fetchCapAtomic(urnOrAlias)
}

// GetCaps resolves several caps in order, failing on the first that cannot.
func (r *FabricRegistry) GetCaps(urns []string) ([]*cap.Cap, error) {
	out := make([]*cap.Cap, 0, len(urns))
	for _, u := range urns {
		c, err := r.GetCap(u)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// GetCachedCaps returns every cap currently in the in-memory cache.
func (r *FabricRegistry) GetCachedCaps() []*cap.Cap {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*cap.Cap, 0, len(r.cachedCaps))
	for _, c := range r.cachedCaps {
		out = append(out, c)
	}
	return out
}

// GetCachedCap is a synchronous in-memory lookup that never touches the
// network: the cap, or why it is not at hand — ErrNotFound when the
// registry's answer is that it has no such cap (the pinned manifest does not
// name it, or its fetch was answered 404); ErrFetchFailed when its last fetch
// was refused or got no answer, with the URL, the status and the manifest
// version; ErrPending when it has not been fetched; and ErrParse when the URN
// does not parse.
func (r *FabricRegistry) GetCachedCap(urnStr string) (*cap.Cap, error) {
	normalized, err := NormalizeCapURN(urnStr)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	c, ok := r.cachedCaps[normalized]
	defver, defverErr := r.capDefver(normalized)
	r.mu.RUnlock()
	if ok {
		return c, nil
	}
	if defverErr != nil {
		return nil, defverErr
	}
	url, _ := definitionURLAndCachePath(r.cacheDir, r.locations, "caps", normalized, defver)
	return nil, r.notAtHand(fetchKey{"cap", normalized, defver},
		fmt.Sprintf("cap '%s' (defver %d)", normalized, defver), url)
}

// CapExists reports whether a cap resolves. A transport failure is not
// "absent", so it propagates rather than reading as false.
func (r *FabricRegistry) CapExists(urnStr string) (bool, error) {
	_, err := r.GetCap(urnStr)
	if err == nil {
		return true, nil
	}
	switch kindOf(err) {
	case ErrNotFound, ErrParse:
		return false, nil
	default:
		return false, err
	}
}

// ValidateCap checks a local cap against its canonical fabric definition.
func (r *FabricRegistry) ValidateCap(local *cap.Cap) error {
	canonical, err := r.GetCap(local.Urn.String())
	if err != nil {
		return err
	}
	canonicalAliases := make(map[string]struct{}, len(canonical.Aliases))
	for _, a := range canonical.Aliases {
		canonicalAliases[a] = struct{}{}
	}
	var unknown []string
	for _, a := range local.Aliases {
		if _, ok := canonicalAliases[a]; !ok {
			unknown = append(unknown, a)
		}
	}
	if len(unknown) > 0 {
		return errf(ErrValidation, "alias mismatch: %v not among the fabric cap's aliases %v",
			unknown, canonical.Aliases)
	}
	if local.IsAbstract != canonical.IsAbstract {
		return errf(ErrValidation, "abstract-flag mismatch. Local: %v, canonical: %v",
			local.IsAbstract, canonical.IsAbstract)
	}
	localStdin := local.GetStdinMediaUrn()
	canonicalStdin := canonical.GetStdinMediaUrn()
	if !stringPtrEqual(localStdin, canonicalStdin) {
		return errf(ErrValidation, "stdin mismatch. Local: %s, canonical: %s",
			derefOr(localStdin, "<none>"), derefOr(canonicalStdin, "<none>"))
	}
	return nil
}

func stringPtrEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func derefOr(s *string, fallback string) string {
	if s == nil {
		return fallback
	}
	return *s
}

// ---------------------------------------------------------------------------
// Public media-def API
// ---------------------------------------------------------------------------

// GetMediaDef returns a media def by URN or alias, cache first.
func (r *FabricRegistry) GetMediaDef(urnOrAlias string) (*media.StoredMediaDef, error) {
	if media.IsAliasToken(urnOrAlias) {
		target, err := r.ResolveAliasTyped(urnOrAlias, media.AliasTargetMedia)
		if err != nil {
			return nil, err
		}
		return r.GetMediaDef(target)
	}

	normalized, err := NormalizeMediaURN(urnOrAlias)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	cached, ok := r.cachedSpecs[normalized]
	offline := r.offline
	defver, defverErr := r.mediaDefver(normalized)
	r.mu.RUnlock()
	if ok {
		return &cached, nil
	}
	// The manifest answers first: a media def it does not name is absent,
	// offline or not.
	if defverErr != nil {
		return nil, defverErr
	}
	if offline {
		return nil, errf(ErrNetworkBlocked, "network access blocked while offline: cannot fetch media def '%s'", urnOrAlias)
	}
	return r.fetchAndCacheMediaDef(urnOrAlias, defver)
}

// fetchAndCacheMediaDef fetches a media def at defver and caches it — only once
// every dim it names is cached (see ensureDimsCached).
// Matches Rust: fn fetch_one_media_def
func (r *FabricRegistry) fetchAndCacheMediaDef(mediaURN string, defver uint32) (*media.StoredMediaDef, error) {
	normalized, err := NormalizeMediaURN(mediaURN)
	if err != nil {
		return nil, err
	}
	spec, err := r.fetchMediaDefAndDims(mediaURN, normalized, defver)
	r.note(fetchKey{"media", normalized, defver}, err)
	return spec, err
}

// fetchMediaDefAndDims is fetchAndCacheMediaDef's work for the media def at
// defver.
func (r *FabricRegistry) fetchMediaDefAndDims(mediaURN, normalized string, defver uint32) (*media.StoredMediaDef, error) {
	spec, err := r.fetchMediaDefFromRegistry(mediaURN, defver)
	if err != nil {
		return nil, err
	}
	specNormalized, err := NormalizeMediaURN(spec.Urn)
	if err != nil {
		return nil, err
	}
	if err := r.ensureDimsCached(fmt.Sprintf("media def '%s'", normalized), spec.Dims); err != nil {
		return nil, err
	}
	if err := r.saveMediaDefToCache(*spec); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.cachedSpecs[specNormalized] = *spec
	r.updateExtensionIndexLocked(*spec)
	r.mu.Unlock()
	return spec, nil
}

// GetMediaDefs resolves several media defs in order.
func (r *FabricRegistry) GetMediaDefs(urns []string) ([]*media.StoredMediaDef, error) {
	out := make([]*media.StoredMediaDef, 0, len(urns))
	for _, u := range urns {
		spec, err := r.GetMediaDef(u)
		if err != nil {
			return nil, err
		}
		out = append(out, spec)
	}
	return out, nil
}

// GetCachedMediaDefs returns every media def currently in the in-memory cache.
func (r *FabricRegistry) GetCachedMediaDefs() []media.StoredMediaDef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]media.StoredMediaDef, 0, len(r.cachedSpecs))
	for _, spec := range r.cachedSpecs {
		out = append(out, spec)
	}
	return out
}

// GetCachedMediaDef is a synchronous in-memory lookup that never touches the
// network: the media def, or why it is not at hand, as GetCachedCap says.
func (r *FabricRegistry) GetCachedMediaDef(urnStr string) (*media.StoredMediaDef, error) {
	normalized, err := NormalizeMediaURN(urnStr)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	spec, ok := r.cachedSpecs[normalized]
	defver, defverErr := r.mediaDefver(normalized)
	r.mu.RUnlock()
	if ok {
		return &spec, nil
	}
	if defverErr != nil {
		return nil, defverErr
	}
	url, _ := definitionURLAndCachePath(r.cacheDir, r.locations, "media", normalized, defver)
	return nil, r.notAtHand(fetchKey{"media", normalized, defver},
		fmt.Sprintf("media def '%s' (defver %d)", normalized, defver), url)
}

// MediaDefExists reports whether a media def resolves.
func (r *FabricRegistry) MediaDefExists(urnStr string) (bool, error) {
	_, err := r.GetMediaDef(urnStr)
	if err == nil {
		return true, nil
	}
	switch kindOf(err) {
	case ErrNotFound, ErrParse:
		return false, nil
	default:
		return false, err
	}
}

// MediaUrnsForExtension looks up the media URNs registered for a file
// extension. The registry hydrates lazily, so an extension only becomes known
// once its owning spec has landed in cache.
func (r *FabricRegistry) MediaUrnsForExtension(extension string) ([]string, error) {
	lower := strings.ToLower(extension)
	r.mu.RLock()
	defer r.mu.RUnlock()
	urns, ok := r.extIndex[lower]
	if !ok {
		return nil, errf(ErrExtension,
			"no media def registered for extension '%s'. Ensure the media def declares an 'extension' field.",
			extension)
	}
	return append([]string(nil), urns...), nil
}

// ExtensionMapping pairs a lowercase extension with the media URNs claiming it.
type ExtensionMapping struct {
	Extension string
	Urns      []string
}

// GetExtensionMappings returns the whole extension index.
func (r *FabricRegistry) GetExtensionMappings() []ExtensionMapping {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ExtensionMapping, 0, len(r.extIndex))
	for ext, urns := range r.extIndex {
		out = append(out, ExtensionMapping{Extension: ext, Urns: append([]string(nil), urns...)})
	}
	return out
}

// ---------------------------------------------------------------------------
// Public alias API
// ---------------------------------------------------------------------------

// GetAlias returns the full StoredAlias for a name, cache first then network.
func (r *FabricRegistry) GetAlias(name string) (*media.StoredAlias, error) {
	normalized, err := media.NormalizeAliasName(name)
	if err != nil {
		return nil, errf(ErrValidation, "invalid alias name: %v", err)
	}
	r.mu.RLock()
	cached, ok := r.cachedAliases[normalized]
	offline := r.offline
	defver, defverErr := r.aliasDefver(normalized)
	r.mu.RUnlock()
	if ok {
		return &cached, nil
	}
	// The manifest answers first: an alias it does not name is absent,
	// offline or not.
	if defverErr != nil {
		return nil, defverErr
	}
	if offline {
		return nil, errf(ErrNetworkBlocked, "network access blocked while offline: cannot fetch alias '%s'", name)
	}
	alias, err := r.fetchAliasFromRegistry(normalized, defver)
	r.note(fetchKey{"alias", normalized, defver}, err)
	if err != nil {
		return nil, err
	}
	if err := r.saveAliasToCache(*alias); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.cachedAliases[alias.Name] = *alias
	r.mu.Unlock()
	return alias, nil
}

// ResolveAlias resolves an alias to whatever URN it targets (untyped).
func (r *FabricRegistry) ResolveAlias(name string) (string, error) {
	alias, err := r.GetAlias(name)
	if err != nil {
		return "", err
	}
	return alias.Target, nil
}

// ResolveAliasTyped resolves an alias and asserts its target kind. An empty
// expected accepts either kind.
func (r *FabricRegistry) ResolveAliasTyped(name string, expected media.AliasTargetKind) (string, error) {
	alias, err := r.GetAlias(name)
	if err != nil {
		return "", err
	}
	actual, ok := media.ClassifyAliasTarget(alias.Target)
	if !ok {
		return "", errf(ErrValidation, "alias '%s' target '%s' is neither a cap nor a media URN",
			alias.Name, alias.Target)
	}
	if expected != "" && actual != expected {
		return "", errf(ErrValidation,
			"alias '%s' resolves to a %s URN ('%s') but a %s was required here",
			alias.Name, actual, alias.Target, expected)
	}
	return alias.Target, nil
}

// ResolveAliasCached is synchronous, in-memory-only alias resolution.
//
// The target and true when the alias is in memory. False, with no error, when
// the registry has no alias by this name: the pinned manifest does not name
// it, or the name is not one an alias can have. An alias the manifest names
// that is not in memory is an error: what its last fetch came to (a refusal
// is reported as itself), or ErrPending when it has not been fetched — it
// exists, so it is never reported as undefined.
func (r *FabricRegistry) ResolveAliasCached(name string) (string, bool, error) {
	normalized, err := media.NormalizeAliasName(name)
	if err != nil {
		return "", false, nil
	}
	r.mu.RLock()
	alias, ok := r.cachedAliases[normalized]
	defver, defverErr := r.aliasDefver(normalized)
	r.mu.RUnlock()
	if ok {
		return alias.Target, true, nil
	}
	if defverErr != nil {
		if IsNotFound(defverErr) {
			return "", false, nil
		}
		return "", false, defverErr
	}
	url, _ := definitionURLAndCachePath(r.cacheDir, r.locations, "aliases", normalized, defver)
	return "", false, r.notAtHand(fetchKey{"alias", normalized, defver},
		fmt.Sprintf("alias '%s' (defver %d)", normalized, defver), url)
}

// DisplayAliasForURN reverse-resolves a cap or media URN to its display alias.
//
// The query is canonicalized by kind first — via the same classifier the alias
// publisher uses for targets — so a non-canonical query still resolves. When
// several aliases target one URN the winner is the shortest name, ties broken
// alphabetically, which is deterministic across processes for a given set.
func (r *FabricRegistry) DisplayAliasForURN(urnStr string) (string, bool) {
	kind, ok := media.ClassifyAliasTarget(urnStr)
	if !ok {
		return "", false
	}
	var canonical string
	var err error
	if kind == media.AliasTargetCap {
		canonical, err = NormalizeCapURN(urnStr)
	} else {
		canonical, err = NormalizeMediaURN(urnStr)
	}
	if err != nil {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var names []string
	for _, alias := range r.cachedAliases {
		if alias.Target == canonical {
			names = append(names, alias.Name)
		}
	}
	return SelectDisplayAlias(names)
}

// SelectDisplayAlias picks the display alias from a set of names that all target
// the same URN: the SHORTEST name, ties broken alphabetically.
//
// The ordering is total and deterministic — (len, name) lexicographic — so
// `png` beats `png-image`, and between equal-length `a16` and `a09` the
// alphabetically smaller wins. Stable across processes for a given alias set,
// which is what makes aliased UI and notation reproducible.
func SelectDisplayAlias(names []string) (string, bool) {
	best := ""
	for _, name := range names {
		if best == "" || len(name) < len(best) || (len(name) == len(best) && name < best) {
			best = name
		}
	}
	return best, best != ""
}

// CachedCapAlias pairs an alias name with the cap URN it targets.
type CachedCapAlias struct {
	Name   string
	CapURN string
}

// CachedCapAliases returns every cached alias whose target is a cap URN.
// Synchronous and cache-only — it relies on the startup alias prefetch.
func (r *FabricRegistry) CachedCapAliases() []CachedCapAlias {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []CachedCapAlias
	for _, alias := range r.cachedAliases {
		if kind, ok := media.ClassifyAliasTarget(alias.Target); ok && kind == media.AliasTargetCap {
			out = append(out, CachedCapAlias{Name: alias.Name, CapURN: alias.Target})
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Public dim API
// ---------------------------------------------------------------------------

// GetDim returns a dim's definition — its title, documentation and values,
// each value with its own — by name (cache first, then network), at the
// version the pinned manifest names. A name the manifest does not list is
// NotFound, decided before any fetch.
func (r *FabricRegistry) GetDim(name string) (*media.StoredDim, error) {
	if !media.IsDimName(name) {
		return nil, errf(ErrValidation, "'%s' is not a dim name", name)
	}
	if dim, ok := r.GetCachedDim(name); ok {
		return dim, nil
	}
	r.mu.RLock()
	defver, err := r.dimDefver(name)
	r.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return r.fetchDim(name, defver)
}

// GetCachedDim is the synchronous, in-memory-only dim lookup. Every dim a
// cached cap or media def names is cached with it, so a reader holding a
// definition from this registry finds its dims here.
func (r *FabricRegistry) GetCachedDim(name string) (*media.StoredDim, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	dim, ok := r.cachedDims[name]
	if !ok {
		return nil, false
	}
	return &dim, true
}

// ---------------------------------------------------------------------------
// Offline / cache control
// ---------------------------------------------------------------------------

// SetOffline blocks (or unblocks) every network fetch. Cached definitions stay
// accessible; an uncached one fails rather than reaching the network.
func (r *FabricRegistry) SetOffline(offline bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.offline = offline
}

// ClearCache invalidates every cache of this registry's manifest version: the
// in-memory maps and its whole folder on disk, INCLUDING the manifest. After
// this the next lookup (or a freshly constructed registry) fetches the
// manifest and every body from the network again. The folders of the layout
// before per-version folders are removed with it (clearCacheFolder).
func (r *FabricRegistry) ClearCache() error {
	r.mu.Lock()
	r.cachedCaps = make(map[string]*cap.Cap)
	r.cachedSpecs = make(map[string]media.StoredMediaDef)
	r.cachedAliases = make(map[string]media.StoredAlias)
	r.cachedDims = make(map[string]media.StoredDim)
	r.extIndex = make(map[string][]string)
	r.fetchFailures = make(map[fetchKey]error)
	r.mu.Unlock()
	// The identity cap is mandatory in every capability set; a cleared cache
	// still has it.
	r.EnsureIdentityCap()

	return clearCacheFolder(r.cacheDir)
}

// ClearCacheOf clears the disk cache of manifest manifestVersion of the
// registry config names, without opening a registry on it — so a cache holding
// a corrupt entry, which a registry refuses to open, can still be cleared. The
// registry's charter is the stored one, else fetched, as a registry finds it.
// It answers the folder cleared. Matches Rust: FabricRegistry::clear_cache_of
func ClearCacheOf(config Config, manifestVersion uint32) (string, error) {
	client := &http.Client{Timeout: httpTimeout}
	capdagCache, err := CapdagCacheRoot()
	if err != nil {
		return "", err
	}
	charter, err := resolveCharter(client, capdagCache, config.Anchor)
	if err != nil {
		return "", err
	}
	cacheDir := versionDir(filepath.Join(capdagCache, charter.Slug), manifestVersion)
	if err := clearCacheFolder(cacheDir); err != nil {
		return "", err
	}
	return cacheDir, nil
}

// CacheDir is the cache folder of the manifest version this registry is
// pinned at, `<os cache>/capdag/<charter slug>/v<N>`: the folder it reads,
// writes and clears.
func (r *FabricRegistry) CacheDir() string { return r.cacheDir }

// ---------------------------------------------------------------------------
// Identity cap (mandatory)
// ---------------------------------------------------------------------------

// EnsureIdentityCap installs the mandatory identity cap into the in-memory
// cache. Idempotent. The identity cap is mandatory in every capability set so
// the resolver's source-to-cap-arg matching can route through identity in any
// notation.
func (r *FabricRegistry) EnsureIdentityCap() {
	identity, err := cap.IdentityCap()
	if err != nil {
		// The identity URN is a built-in constant; if it ever stops parsing,
		// say so loudly rather than failing every later lookup with a
		// mysterious miss.
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Standard caps travel with the manifest: their per-def version IS the
	// registry's pinned manifest version.
	if r.manifestVersion >= 1 {
		identity.Version = r.manifestVersion
	}
	normalized, err := NormalizeCapURN(identity.Urn.String())
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] identity cap URN does not parse: %v\n", err)
		return
	}
	if _, ok := r.cachedCaps[normalized]; !ok {
		r.cachedCaps[normalized] = identity
	}
	if r.manifestVersion >= 1 {
		r.manifest.Caps[normalized] = r.manifestVersion
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// NewForTest builds an empty registry over a throwaway cache dir, pinned at v1
// with an empty manifest so the helpers below flow definitions into the
// manifest at their declared version.
//
// It returns the registry rather than a (registry, error) pair, matching the
// reference: the only thing that can fail here is creating a temp directory,
// and a test host that cannot do that is broken in a way no test should try to
// report gracefully.
func NewForTest() *FabricRegistry {
	return NewForTestWithConfig(DefaultConfig())
}

// NewForTestWithConfig is NewForTest against a chosen anchor, laid out the way
// the publisher lays a fabric out.
func NewForTestWithConfig(config Config) *FabricRegistry {
	registryDir, err := os.MkdirTemp("", "capdag-fabric-test-")
	if err != nil {
		panic(fmt.Sprintf("cannot create a test cache dir: %v", err))
	}
	return NewForTestInDir(config, registryDir)
}

// NewForTestInDir is NewForTestWithConfig over a caller-chosen registry
// folder, for tests that assert on the on-disk layout. The registry is pinned
// at manifest 1, so its cache folder is registryDir's v1 folder (CacheDir).
func NewForTestInDir(config Config, registryDir string) *FabricRegistry {
	return newForTestInDirAt(config, registryDir, 1)
}

// newForTestInDirAt is NewForTestInDir pinned at manifestVersion, with an
// empty manifest; its cache folder is that version's folder in registryDir.
func newForTestInDirAt(config Config, registryDir string, manifestVersion uint32) *FabricRegistry {
	charter := PublisherLayoutCharter(config.Anchor)
	locations, err := LocationsFromCharter(charter)
	if err != nil {
		panic(fmt.Sprintf("the publisher-layout charter is a fabric charter: %v", err))
	}
	cacheDir := versionDir(registryDir, manifestVersion)
	for _, kind := range kindFolders {
		if err := os.MkdirAll(filepath.Join(cacheDir, kind), 0o755); err != nil {
			panic(fmt.Sprintf("cannot create the test cache dir %s: %v", filepath.Join(cacheDir, kind), err))
		}
	}
	r := &FabricRegistry{
		client:          &http.Client{Timeout: httpTimeout},
		cacheDir:        cacheDir,
		config:          config,
		charter:         charter,
		locations:       locations,
		cachedCaps:      make(map[string]*cap.Cap),
		cachedSpecs:     make(map[string]media.StoredMediaDef),
		cachedAliases:   make(map[string]media.StoredAlias),
		cachedDims:      make(map[string]media.StoredDim),
		extIndex:        make(map[string][]string),
		manifestVersion: manifestVersion,
		manifest:        media.EmptyManifest(manifestVersion),
		fetchFailures:   make(map[fetchKey]error),
	}
	r.EnsureIdentityCap()
	return r
}

// AddCapsToCache inserts caps directly into the in-memory cache and records
// each in the manifest at its version. A cap whose version is 0 is stamped to
// the pinned manifest version.
func (r *FabricRegistry) AddCapsToCache(caps []*cap.Cap) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range caps {
		if c.Version == 0 && r.manifestVersion >= 1 {
			c.Version = r.manifestVersion
		}
		normalized, err := NormalizeCapURN(c.Urn.String())
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] AddCapsToCache: skipping cap: %v\n", err)
			continue
		}
		r.cachedCaps[normalized] = c
		if r.manifestVersion >= 1 {
			r.manifest.Caps[normalized] = c.Version
		}
	}
}

// AddSpec inserts a media def directly into the in-memory cache, updating the
// extension index and recording it in the manifest.
func (r *FabricRegistry) AddSpec(spec media.StoredMediaDef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if spec.Version == 0 && r.manifestVersion >= 1 {
		spec.Version = r.manifestVersion
	}
	normalized, err := NormalizeMediaURN(spec.Urn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] AddSpec: skipping media def: %v\n", err)
		return
	}
	r.cachedSpecs[normalized] = spec
	r.updateExtensionIndexLocked(spec)
	if r.manifestVersion >= 1 {
		r.manifest.Media[normalized] = spec.Version
	}
}

// PinMediaDefForTest names a media def in the pinned manifest at defver
// without caching it, so a lookup of it goes to the registry (test helper).
func (r *FabricRegistry) PinMediaDefForTest(mediaURN string, defver uint32) {
	normalized, err := NormalizeMediaURN(mediaURN)
	if err != nil {
		panic(fmt.Sprintf("a test media URN parses: %v", err))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.manifest.Media[normalized] = defver
}

// PinAliasForTest names an alias in the pinned manifest at defver without
// caching it, so a lookup of it goes to the registry (test helper).
func (r *FabricRegistry) PinAliasForTest(name string, defver uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.manifest.Aliases[name] = defver
}

// InsertCachedAliasForTest inserts one alias directly into the in-memory cache
// and registers its defver in the manifest, bypassing the network.
func (r *FabricRegistry) InsertCachedAliasForTest(alias media.StoredAlias) {
	r.AddAliasesToCache([]media.StoredAlias{alias})
}

// AddAliasesToCache inserts aliases directly into the in-memory cache and
// registers each defver in the manifest, bypassing the network.
func (r *FabricRegistry) AddAliasesToCache(aliases []media.StoredAlias) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, alias := range aliases {
		r.cachedAliases[alias.Name] = alias
		if r.manifestVersion >= 1 {
			r.manifest.Aliases[alias.Name] = alias.Version
		}
	}
}

// InsertCachedDimForTest inserts a dim directly into the in-memory cache and
// registers its defver in the manifest, bypassing the network. Mirrors
// InsertCachedAliasForTest.
func (r *FabricRegistry) InsertCachedDimForTest(dim media.StoredDim) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cachedDims[dim.Name] = dim
	if r.manifestVersion >= 1 {
		r.manifest.Dims[dim.Name] = dim.Version
	}
}
