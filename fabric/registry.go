// Package fabric provides the unified fabric registry — cap definitions and
// media definitions behind one type.
//
// Both live in the same online registry
// (`https://fabric.capdag.com/{caps,media,aliases}/<sha256>`), share the same
// caching policy, the same manifest pin, and the same offline / clear-cache
// surface. They differ only in wire payload shape (`cap.Cap` vs
// `media.StoredMediaDef`) and in disk-cache subdirectory (`caps/` vs `media/`).
//
// On miss, `GetCap` atomically fetches the cap AND every media URN it
// references; if any of those media defs cannot be fetched the cap is NOT
// cached. This keeps the cap cache consistent with its media-def footprint.
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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/machinefabric/capdag-go/bifaci"
	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

// DefaultRegistryBaseURL is the fabric origin used when nothing else says
// otherwise.
const DefaultRegistryBaseURL = "https://fabric.capdag.com"

// CacheDurationHours is the TTL applied to v0 (flat-path) cache entries.
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
	ErrHTTP           ErrorKind = "http"
	ErrNetworkBlocked ErrorKind = "network_blocked"
	ErrNotFound       ErrorKind = "not_found"
	ErrParse          ErrorKind = "parse"
	ErrCache          ErrorKind = "cache"
	ErrValidation     ErrorKind = "validation"
	ErrExtension      ErrorKind = "extension_not_found"
)

// Error is a registry failure carrying the kind callers branch on.
type Error struct {
	Kind    ErrorKind
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
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

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Config is the registry's origin pair. Resolution order is explicit argument,
// then environment (CDG_FABRIC_REGISTRY_URL / CDG_SCHEMA_BASE_URL), then
// default.
type Config struct {
	RegistryBaseURL string
	SchemaBaseURL   string
}

// DefaultConfig reads the environment, falling back to the built-in origin.
func DefaultConfig() Config {
	base := os.Getenv("CDG_FABRIC_REGISTRY_URL")
	if base == "" {
		base = DefaultRegistryBaseURL
	}
	schema := os.Getenv("CDG_SCHEMA_BASE_URL")
	if schema == "" {
		schema = base + "/schema"
	}
	return Config{RegistryBaseURL: base, SchemaBaseURL: schema}
}

// WithRegistryURL points the config at a different fabric origin, re-deriving
// the schema base when it was derived from the old one — pairing a caller-chosen
// fabric with a stale schema origin would validate one origin's definitions
// against another's schemas.
func (c Config) WithRegistryURL(url string) Config {
	if c.SchemaBaseURL == c.RegistryBaseURL+"/schema" {
		c.SchemaBaseURL = url + "/schema"
	}
	c.RegistryBaseURL = url
	return c
}

// WithSchemaURL overrides only the schema origin.
func (c Config) WithSchemaURL(url string) Config {
	c.SchemaBaseURL = url
	return c
}

// mediaURLAndCachePath builds the registry URL and on-disk cache path for a
// media object at a given defver. defver 0 addresses the frozen v0 flat path;
// >= 1 addresses the versioned subpath. Both the fetch path and the cache-path
// resolver go through here so the two can never drift apart.
func mediaURLAndCachePath(cacheDir string, config Config, normalizedURN string, defver uint32) (string, string) {
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(normalizedURN)))
	if defver == 0 {
		return config.RegistryBaseURL + "/media/" + digest,
			filepath.Join(cacheDir, "media", digest+".json")
	}
	return fmt.Sprintf("%s/media/%s/%d.json", config.RegistryBaseURL, digest, defver),
		filepath.Join(cacheDir, "media", digest, fmt.Sprintf("%d.json", defver))
}

// BakedManifestVersion is the workspace-pinned fabric manifest version.
//
// Rust bakes this into the binary at compile time; Go has no build-time
// codegen, so a shipped binary carries it through the linker
// (-X github.com/machinefabric/capdag-go/fabric.BakedManifestVersion=N) and a
// workspace build reads MFR_FABRIC_MANIFEST_VERSION, the same variable dx
// exports for every other language. Neither is a fallback for the other: both
// are explicit declarations, and their absence is a hard failure rather than a
// silent drop to v0, which would resolve every URN against the frozen flat-path
// regime and report "not found" for the whole fabric.
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
				"a workspace build reads MFR_FABRIC_MANIFEST_VERSION, which dx exports "+
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

	cacheDir        string
	capsCacheDir    string
	mediaCacheDir   string
	aliasesCacheDir string

	config Config

	cachedCaps    map[string]*cap.Cap
	cachedSpecs   map[string]media.StoredMediaDef
	cachedAliases map[string]media.StoredAlias
	extIndex      map[string][]string

	// manifestVersion 0 is the legacy v0 / flat-path regime (no manifest
	// consulted). >= 1 is manifest-driven: every URN lookup resolves to a
	// (urn, defver) pair via the manifest before fetching.
	manifestVersion uint32
	manifest        *media.Manifest

	offline bool
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
// manifestVersion 0 is the legacy v0 / flat-path mode (no manifest fetch).
// >= 1 loads manifest/<N>.json from the local cache, else blocks on a network
// fetch. If neither can provide it the construction fails — there is no
// fallback to v0, which would silently resolve against a different regime.
func WithConfigAndManifestVersion(config Config, manifestVersion uint32) (*FabricRegistry, error) {
	cacheDir, err := CacheDirFor(config.RegistryBaseURL)
	if err != nil {
		return nil, err
	}
	for _, sub := range []string{"", "caps", "media", "aliases", "manifests"} {
		if err := os.MkdirAll(filepath.Join(cacheDir, sub), 0o755); err != nil {
			return nil, errf(ErrCache, "creating cache dir %s: %v", filepath.Join(cacheDir, sub), err)
		}
	}

	client := &http.Client{Timeout: httpTimeout}

	manifest := media.EmptyManifest(0)
	if manifestVersion >= 1 {
		manifest, err = loadOrFetchManifest(filepath.Join(cacheDir, "manifests"), client, config, manifestVersion)
		if err != nil {
			return nil, err
		}
	}

	r := &FabricRegistry{
		client:          client,
		cacheDir:        cacheDir,
		capsCacheDir:    filepath.Join(cacheDir, "caps"),
		mediaCacheDir:   filepath.Join(cacheDir, "media"),
		aliasesCacheDir: filepath.Join(cacheDir, "aliases"),
		config:          config,
		cachedCaps:      make(map[string]*cap.Cap),
		cachedSpecs:     make(map[string]media.StoredMediaDef),
		cachedAliases:   make(map[string]media.StoredAlias),
		extIndex:        make(map[string][]string),
		manifestVersion: manifestVersion,
		manifest:        manifest,
	}

	r.loadAllCachedCaps()
	r.loadAllCachedSpecs()
	r.loadAllCachedAliases()

	// Filter the hydrated caches to the pinned manifest's defvers. A cache
	// populated under an older manifest holds definitions this build must not
	// serve — same URN, different bytes.
	r.mu.Lock()
	if manifestVersion >= 1 {
		for urnKey, c := range r.cachedCaps {
			if manifest.Caps[urnKey] != c.Version {
				delete(r.cachedCaps, urnKey)
			}
		}
		for urnKey, spec := range r.cachedSpecs {
			if manifest.Media[urnKey] != spec.Version {
				delete(r.cachedSpecs, urnKey)
			}
		}
		for name, alias := range r.cachedAliases {
			if manifest.Aliases[name] != alias.Version {
				delete(r.cachedAliases, name)
			}
		}
		r.extIndex = make(map[string][]string)
		for _, spec := range r.cachedSpecs {
			r.updateExtensionIndexLocked(spec)
		}
	} else {
		// Aliases exist only in the versioned regime.
		r.cachedAliases = make(map[string]media.StoredAlias)
	}
	r.mu.Unlock()

	r.EnsureIdentityCap()
	return r, nil
}

func loadOrFetchManifest(manifestsDir string, client *http.Client, config Config, version uint32) (*media.Manifest, error) {
	cacheFile := filepath.Join(manifestsDir, fmt.Sprintf("%d.json", version))
	if raw, err := os.ReadFile(cacheFile); err == nil {
		var m media.Manifest
		if jsonErr := json.Unmarshal(raw, &m); jsonErr == nil {
			if m.Version != version {
				return nil, errf(ErrParse,
					"cached manifest at %s reports version %d but the file is %d.json",
					cacheFile, m.Version, version)
			}
			return &m, nil
		}
		// A corrupt cache file is removed rather than read around: leaving it
		// would fail every construction until someone cleared it by hand.
		_ = os.Remove(cacheFile)
	}

	url := fmt.Sprintf("%s/manifest/%d.json", config.RegistryBaseURL, version)
	resp, err := client.Get(url)
	if err != nil {
		return nil, &Error{Kind: ErrHTTP, Message: fmt.Sprintf("fetching manifest v%d at %s", version, url), Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errf(ErrNotFound, "manifest v%d not found in registry (HTTP %d) at %s",
			version, resp.StatusCode, url)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &Error{Kind: ErrHTTP, Message: fmt.Sprintf("reading manifest v%d", version), Err: err}
	}
	var m media.Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, &Error{Kind: ErrParse, Message: fmt.Sprintf("parsing manifest v%d", version), Err: err}
	}
	if m.Version != version {
		return nil, errf(ErrParse, "manifest fetched as v%d reports version %d", version, m.Version)
	}
	if err := os.WriteFile(cacheFile, body, 0o644); err != nil {
		return nil, errf(ErrCache, "writing manifest cache to %s: %v", cacheFile, err)
	}
	return &m, nil
}

// CacheDirFor is the on-disk cache root, namespaced per registry origin.
//
// The namespace matters: a cache populated from one fabric would otherwise
// satisfy lookups against another, and the two serve DIFFERENT bytes for the
// same URN and version. Same origin means a stable root (so caching hits);
// distinct origins mean distinct roots.
func CacheDirFor(registryBaseURL string) (string, error) {
	var base string
	if runtime.GOOS == "windows" {
		base = os.Getenv("LOCALAPPDATA")
	} else {
		base = os.Getenv("XDG_CACHE_HOME")
	}
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", errf(ErrCache, "cannot determine the home directory for the cache root: %v", err)
		}
		base = filepath.Join(home, ".cache")
	}
	return filepath.Join(base, "capdag", bifaci.SlugFor(&registryBaseURL)), nil
}

// ---------------------------------------------------------------------------
// Disk-cache hydration
// ---------------------------------------------------------------------------

func walkJSONFiles(root string, visit func(path string, raw []byte)) {
	entries, err := os.ReadDir(root)
	if err != nil {
		// A missing cache dir is not an error — nothing has been cached yet.
		return
	}
	for _, entry := range entries {
		path := filepath.Join(root, entry.Name())
		if entry.IsDir() {
			walkJSONFiles(path, visit)
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] failed to read cache file %s: %v\n", path, err)
			continue
		}
		visit(path, raw)
	}
}

func (r *FabricRegistry) loadAllCachedCaps() {
	walkJSONFiles(r.capsCacheDir, func(path string, raw []byte) {
		var entry capCacheEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] failed to load cap cache file %s: %v\n", path, err)
			_ = os.Remove(path)
			return
		}
		var c cap.Cap
		if err := json.Unmarshal(entry.Definition, &c); err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] failed to load cap cache file %s: %v\n", path, err)
			_ = os.Remove(path)
			return
		}
		// TTL applies only to v0 entries; versioned definitions are immutable.
		if c.Version == 0 && entry.isExpired() {
			_ = os.Remove(path)
			return
		}
		normalized, err := NormalizeCapURN(c.Urn.String())
		if err != nil {
			// Hydration with a malformed URN is a graceful no-op: never cache
			// under a raw key, never crash the whole load.
			fmt.Fprintf(os.Stderr, "[WARN] skipping cap cache file %s: %v\n", path, err)
			return
		}
		r.mu.Lock()
		r.cachedCaps[normalized] = &c
		r.mu.Unlock()
	})
}

func (r *FabricRegistry) loadAllCachedSpecs() {
	walkJSONFiles(r.mediaCacheDir, func(path string, raw []byte) {
		var entry mediaCacheEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] failed to load media cache file %s: %v\n", path, err)
			_ = os.Remove(path)
			return
		}
		if entry.Spec.Version == 0 && entry.isExpired() {
			_ = os.Remove(path)
			return
		}
		normalized, err := NormalizeMediaURN(entry.Spec.Urn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] skipping media cache file %s: %v\n", path, err)
			return
		}
		r.mu.Lock()
		r.cachedSpecs[normalized] = entry.Spec
		r.updateExtensionIndexLocked(entry.Spec)
		r.mu.Unlock()
	})
}

func (r *FabricRegistry) loadAllCachedAliases() {
	walkJSONFiles(r.aliasesCacheDir, func(path string, raw []byte) {
		var entry aliasCacheEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] failed to load alias cache file %s: %v\n", path, err)
			_ = os.Remove(path)
			return
		}
		r.mu.Lock()
		r.cachedAliases[entry.Alias.Name] = entry.Alias
		r.mu.Unlock()
	})
}

// ---------------------------------------------------------------------------
// Disk-cache writers
// ---------------------------------------------------------------------------

func (r *FabricRegistry) capCacheFilePath(capURN string, defver uint32) (string, error) {
	normalized, err := NormalizeCapURN(capURN)
	if err != nil {
		return "", err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(normalized)))
	if defver == 0 {
		return filepath.Join(r.capsCacheDir, digest+".json"), nil
	}
	return filepath.Join(r.capsCacheDir, digest, fmt.Sprintf("%d.json", defver)), nil
}

func (r *FabricRegistry) mediaCacheFilePath(mediaURN string, defver uint32) (string, error) {
	normalized, err := NormalizeMediaURN(mediaURN)
	if err != nil {
		return "", err
	}
	_, path := mediaURLAndCachePath(r.cacheDir, r.config, normalized, defver)
	return path, nil
}

func (r *FabricRegistry) aliasCacheFilePath(normalizedName string, defver uint32) string {
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(normalizedName)))
	return filepath.Join(r.aliasesCacheDir, digest, fmt.Sprintf("%d.json", defver))
}

func writeCacheFile(path string, payload any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errf(ErrCache, "creating %s: %v", filepath.Dir(path), err)
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return errf(ErrCache, "serializing the cache entry for %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return errf(ErrCache, "writing %s: %v", path, err)
	}
	return nil
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
	return writeCacheFile(path, capCacheEntry{
		Definition: definition,
		CachedAt:   time.Now().Unix(),
		TTLHours:   CacheDurationHours,
	})
}

func (r *FabricRegistry) saveMediaDefToCache(spec media.StoredMediaDef) error {
	path, err := r.mediaCacheFilePath(spec.Urn, spec.Version)
	if err != nil {
		return err
	}
	return writeCacheFile(path, mediaCacheEntry{
		Spec:     spec,
		CachedAt: time.Now().Unix(),
		TTLHours: CacheDurationHours,
	})
}

func (r *FabricRegistry) saveAliasToCache(alias media.StoredAlias) error {
	return writeCacheFile(r.aliasCacheFilePath(alias.Name, alias.Version), aliasCacheEntry{
		Alias:    alias,
		CachedAt: time.Now().Unix(),
		TTLHours: CacheDurationHours,
	})
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
	resp, err := r.client.Get(url)
	if err != nil {
		return nil, &Error{Kind: ErrHTTP, Message: fmt.Sprintf("failed to fetch %s", what), Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errf(ErrNotFound, "%s not found in registry (HTTP %d)", what, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &Error{Kind: ErrHTTP, Message: fmt.Sprintf("reading the response for %s", what), Err: err}
	}
	return body, nil
}

func (r *FabricRegistry) fetchCapFromRegistry(capURN string, defver uint32) (*cap.Cap, error) {
	normalized, err := NormalizeCapURN(capURN)
	if err != nil {
		return nil, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(normalized)))
	url := r.config.RegistryBaseURL + "/caps/" + digest
	if defver != 0 {
		url = fmt.Sprintf("%s/caps/%s/%d.json", r.config.RegistryBaseURL, digest, defver)
	}
	body, err := r.get(url, fmt.Sprintf("cap '%s'", capURN))
	if err != nil {
		return nil, err
	}
	var wire cap.RegistryCapResponse
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, &Error{Kind: ErrParse, Message: fmt.Sprintf("parsing the registry response for cap '%s'", capURN), Err: err}
	}
	c, err := wire.ToCap()
	if err != nil {
		return nil, &Error{Kind: ErrParse, Message: fmt.Sprintf("converting the registry response for cap '%s'", capURN), Err: err}
	}
	return c, nil
}

func (r *FabricRegistry) fetchMediaDefFromRegistry(mediaURN string, defver uint32) (*media.StoredMediaDef, error) {
	normalized, err := NormalizeMediaURN(mediaURN)
	if err != nil {
		return nil, err
	}
	url, _ := mediaURLAndCachePath(r.cacheDir, r.config, normalized, defver)
	body, err := r.get(url, fmt.Sprintf("media def '%s'", mediaURN))
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
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(normalizedName)))
	url := fmt.Sprintf("%s/aliases/%s/%d.json", r.config.RegistryBaseURL, digest, defver)
	body, err := r.get(url, fmt.Sprintf("alias '%s'", normalizedName))
	if err != nil {
		return nil, err
	}
	var alias media.StoredAlias
	if err := json.Unmarshal(body, &alias); err != nil {
		return nil, &Error{Kind: ErrParse, Message: fmt.Sprintf("parsing alias '%s'", normalizedName), Err: err}
	}
	// The fetched body must match what was requested; a mismatched object means
	// the registry served a different definition than the manifest pinned.
	if alias.Name != normalizedName {
		return nil, errf(ErrParse, "alias object name '%s' does not match the requested name '%s'",
			alias.Name, normalizedName)
	}
	if alias.Version != defver {
		return nil, errf(ErrParse, "alias '%s' object reports version %d but the manifest pins defver %d",
			alias.Name, alias.Version, defver)
	}
	if _, ok := media.ClassifyAliasTarget(alias.Target); !ok {
		return nil, errf(ErrValidation, "alias '%s' target '%s' is neither a cap nor a media URN",
			alias.Name, alias.Target)
	}
	return &alias, nil
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

// ManifestVersion is the fabric manifest version this registry is pinned to.
func (r *FabricRegistry) ManifestVersion() uint32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.manifestVersion
}

// Config returns the origin pair this registry resolves against.
func (r *FabricRegistry) Config() Config {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
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
	for _, arg := range c.GetArgs() {
		if err := push(arg.MediaUrn); err != nil {
			return nil, err
		}
		for _, source := range arg.Sources {
			if source.Stdin != nil {
				if err := push(*source.Stdin); err != nil {
					return nil, err
				}
			}
		}
	}
	if output := c.GetOutput(); output != nil {
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
		spec, err := r.fetchMediaDefFromRegistry(mediaURN, mediaDefver)
		if err != nil {
			return nil, err
		}
		if err := r.saveMediaDefToCache(*spec); err != nil {
			return nil, err
		}
		specNormalized, err := NormalizeMediaURN(spec.Urn)
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		r.cachedSpecs[specNormalized] = *spec
		r.updateExtensionIndexLocked(*spec)
		r.mu.Unlock()
	}

	// Every referenced media def landed — now the cap may be cached.
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
	r.mu.RUnlock()
	if ok {
		return cached, nil
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

// GetCachedCap is a synchronous in-memory probe that never touches the network.
//
// A malformed URN can never match a cache entry, so it degrades to the same
// graceful miss rather than erroring on this latency-critical path.
func (r *FabricRegistry) GetCachedCap(urnStr string) (*cap.Cap, bool) {
	normalized, err := NormalizeCapURN(urnStr)
	if err != nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.cachedCaps[normalized]
	return c, ok
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
	if offline {
		return nil, errf(ErrNetworkBlocked, "network access blocked while offline: cannot fetch media def '%s'", urnOrAlias)
	}
	if defverErr != nil {
		return nil, defverErr
	}
	spec, err := r.fetchMediaDefFromRegistry(urnOrAlias, defver)
	if err != nil {
		return nil, err
	}
	if err := r.saveMediaDefToCache(*spec); err != nil {
		return nil, err
	}
	specNormalized, err := NormalizeMediaURN(spec.Urn)
	if err != nil {
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

// GetCachedMediaDef is a synchronous in-memory probe; see GetCachedCap.
func (r *FabricRegistry) GetCachedMediaDef(urnStr string) *media.StoredMediaDef {
	normalized, err := NormalizeMediaURN(urnStr)
	if err != nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	spec, ok := r.cachedSpecs[normalized]
	if !ok {
		return nil
	}
	return &spec
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
	if offline {
		return nil, errf(ErrNetworkBlocked, "network access blocked while offline: cannot fetch alias '%s'", name)
	}
	if defverErr != nil {
		return nil, defverErr
	}
	alias, err := r.fetchAliasFromRegistry(normalized, defver)
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

// ResolveAliasCached is synchronous, in-memory-only alias resolution. A
// malformed name reads as "no resolution" rather than an error, so callers
// treat "not a valid alias" and "not cached" uniformly.
func (r *FabricRegistry) ResolveAliasCached(name string) (string, bool) {
	normalized, err := media.NormalizeAliasName(name)
	if err != nil {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	alias, ok := r.cachedAliases[normalized]
	if !ok {
		return "", false
	}
	return alias.Target, true
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
// Offline / cache control
// ---------------------------------------------------------------------------

// SetOffline blocks (or unblocks) every network fetch. Cached definitions stay
// accessible; an uncached one fails rather than reaching the network.
func (r *FabricRegistry) SetOffline(offline bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.offline = offline
}

// ClearCache drops both the in-memory and the on-disk caches.
func (r *FabricRegistry) ClearCache() error {
	r.mu.Lock()
	r.cachedCaps = make(map[string]*cap.Cap)
	r.cachedSpecs = make(map[string]media.StoredMediaDef)
	r.cachedAliases = make(map[string]media.StoredAlias)
	r.extIndex = make(map[string][]string)
	r.mu.Unlock()

	if err := os.RemoveAll(r.cacheDir); err != nil {
		return errf(ErrCache, "clearing the cache directory %s: %v", r.cacheDir, err)
	}
	for _, sub := range []string{"", "caps", "media", "aliases", "manifests"} {
		if err := os.MkdirAll(filepath.Join(r.cacheDir, sub), 0o755); err != nil {
			return errf(ErrCache, "recreating %s: %v", filepath.Join(r.cacheDir, sub), err)
		}
	}
	// The identity cap is mandatory in every capability set; a cleared cache
	// still has it.
	r.EnsureIdentityCap()
	return nil
}

// CacheDir is the on-disk root this registry reads and writes.
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

// NewForTestWithConfig is NewForTest against a chosen origin.
func NewForTestWithConfig(config Config) *FabricRegistry {
	cacheDir, err := os.MkdirTemp("", "capdag-fabric-test-")
	if err != nil {
		panic(fmt.Sprintf("cannot create a test cache dir: %v", err))
	}
	return NewForTestInDir(config, cacheDir)
}

// NewForTestInDir is NewForTestWithConfig over a caller-chosen cache root, for
// tests that assert on the on-disk layout.
func NewForTestInDir(config Config, cacheDir string) *FabricRegistry {
	for _, sub := range []string{"", "caps", "media", "aliases", "manifests"} {
		if err := os.MkdirAll(filepath.Join(cacheDir, sub), 0o755); err != nil {
			panic(fmt.Sprintf("cannot create the test cache dir %s: %v", filepath.Join(cacheDir, sub), err))
		}
	}
	r := &FabricRegistry{
		client:          &http.Client{Timeout: httpTimeout},
		cacheDir:        cacheDir,
		capsCacheDir:    filepath.Join(cacheDir, "caps"),
		mediaCacheDir:   filepath.Join(cacheDir, "media"),
		aliasesCacheDir: filepath.Join(cacheDir, "aliases"),
		config:          config,
		cachedCaps:      make(map[string]*cap.Cap),
		cachedSpecs:     make(map[string]media.StoredMediaDef),
		cachedAliases:   make(map[string]media.StoredAlias),
		extIndex:        make(map[string][]string),
		manifestVersion: 1,
		manifest:        media.EmptyManifest(1),
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
