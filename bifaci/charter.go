package bifaci

// Registry charters: what makes a URL a MachineFabric registry. Mirrors Rust
// `bifaci/charter.rs`.
//
// A registry — of cartridges, of fabric definitions, or of app releases — is
// named by its ANCHOR, which may be any URL. The one convention anywhere is
// that `<anchor>/charter.json` and `<anchor>/charter.json.sig` exist.
// Everything else is read from the charter, never derived from the shape of a
// URL: the registry's slug (its folder on disk), its kind, the environment its
// contents are signed under, the locations of its manifests, and for a
// cartridge registry the fabric its caps resolve against.
//
// # What this package does and does not do
//
// It reads a charter and holds it to its shape, to the folder it is stored in
// and to the anchor its cartridges record. It does NOT verify the signature:
// this mirror carries no chain verification (`release_cert.rs` and
// `charter.rs` in the Rust library are the only implementation of it, checked
// against the MachineFabric roots every Rust build bakes), and a mirror that
// stubbed one would be worse than a mirror that has none. What this reads is
// what the Rust host, or the installer that wrote it, already verified.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
)

// CharterFormat is the format discriminator inside every charter body.
const CharterFormat = "machinefabric-registry-charter/1"

// CharterSigFormat is the format discriminator of a charter's signature
// envelope — distinct from the manifest envelope's, so a prod-signed manifest
// can never be presented as a charter.
const CharterSigFormat = "machinefabric-charter-sig/1"

// CharterFile is the charter's file name under its anchor and in a
// registry's folder.
const CharterFile = "charter.json"

// CharterSigFile is the charter signature's file name.
const CharterSigFile = "charter.json.sig"

// CharterSigningEnvironment is the label a charter's certificate carries:
// charters are signed by prod for every environment.
const CharterSigningEnvironment = "prod"

// DevSlug is the reserved folder name of cartridges built with no registry. No
// charter may claim it.
const DevSlug = "dev"

// RegistryKind is one of the three kinds of registry.
type RegistryKind string

const (
	RegistryKindCartridges RegistryKind = "cartridges"
	RegistryKindFabric     RegistryKind = "fabric"
	RegistryKindReleases   RegistryKind = "releases"
)

// registryLocations is, per kind, the locations a charter must declare, each
// with the placeholders its template must use — all of them and no others.
var registryLocations = map[RegistryKind]map[string][]string{
	RegistryKindCartridges: {"manifest": {"registry_version"}},
	RegistryKindFabric: {
		"manifest":   {"manifest_version"},
		"latest":     {},
		"definition": {"kind", "urn_hash", "version"},
	},
	RegistryKindReleases: {"manifest": {"app_id"}},
}

// Charter is a charter body, as signed.
type Charter struct {
	Format string `json:"format"`
	// Anchor is the URL this registry is named by — the registry_url every
	// cartridge from it records.
	Anchor string       `json:"anchor"`
	Kind   RegistryKind `json:"kind"`
	// Slug is the registry's name on disk. Never "dev".
	Slug string `json:"slug"`
	// Environment is the label this registry's CONTENTS are signed under.
	Environment string `json:"environment"`
	// Experiment is the lab experiment this registry belongs to, if any.
	Experiment *string `json:"experiment,omitempty"`
	// Locations are templates, by name.
	Locations map[string]string `json:"locations"`
	// Fabric is, for a cartridge registry, the anchor of the fabric registry
	// its caps resolve against.
	Fabric   *string `json:"fabric,omitempty"`
	IssuedAt uint64  `json:"issued_at"`
	// NotAfter is when the charter is void; nil: valid while the key that
	// signed it is trusted.
	NotAfter *uint64 `json:"not_after"`
}

// ParseCharter reads a charter's bytes: the format first, then the shape.
func ParseCharter(body []byte) (*Charter, error) {
	var probe struct {
		Format *string `json:"format"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil, fmt.Errorf("the charter is not a charter document: %w", err)
	}
	if probe.Format == nil {
		return nil, fmt.Errorf("the charter is not a charter document: no format")
	}
	if *probe.Format != CharterFormat {
		return nil, fmt.Errorf("the charter declares format '%s' (this build reads '%s')", *probe.Format, CharterFormat)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var charter Charter
	if err := decoder.Decode(&charter); err != nil {
		return nil, fmt.Errorf("the charter is not a charter document: %w", err)
	}
	if err := charter.Validate(); err != nil {
		return nil, fmt.Errorf("the charter is not a charter document: %w", err)
	}
	return &charter, nil
}

// Validate holds a charter to the shape every charter must have, whoever
// signed it.
func (c *Charter) Validate() error {
	if c.Format != CharterFormat {
		return fmt.Errorf("format '%s' is not '%s'", c.Format, CharterFormat)
	}
	if err := checkHTTPS("anchor", c.Anchor); err != nil {
		return err
	}
	if strings.HasSuffix(c.Anchor, "/") {
		return fmt.Errorf("anchor '%s' ends with '/'", c.Anchor)
	}
	if !IsSlug(c.Slug) {
		return fmt.Errorf("slug '%s' is not a registry slug ([a-z0-9][a-z0-9.-]{0,62}, not '%s')", c.Slug, DevSlug)
	}
	if !isLabel(c.Environment) {
		return fmt.Errorf("environment '%s' is not a label ([a-z0-9-]+)", c.Environment)
	}
	if c.Experiment != nil && !IsExperimentID(*c.Experiment) {
		return fmt.Errorf("experiment '%s' is not an experiment id ([a-z0-9][a-z0-9-]{0,39})", *c.Experiment)
	}
	required, known := registryLocations[c.Kind]
	if !known {
		return fmt.Errorf("kind '%s' is not a kind of registry", c.Kind)
	}
	for name := range c.Locations {
		if _, declared := required[name]; !declared {
			return fmt.Errorf("location '%s' is not one a %s registry has", name, c.Kind)
		}
	}
	names := make([]string, 0, len(required))
	for name := range required {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		template, present := c.Locations[name]
		if !present {
			return fmt.Errorf("a %s registry must declare location '%s'", c.Kind, name)
		}
		if err := checkHTTPS(fmt.Sprintf("location '%s'", name), template); err != nil {
			return err
		}
		used, err := placeholdersIn(template)
		if err != nil {
			return fmt.Errorf("location '%s': %w", name, err)
		}
		for _, placeholder := range used {
			if !slices.Contains(required[name], placeholder) {
				return fmt.Errorf("location '%s' uses '{%s}', which it does not take", name, placeholder)
			}
		}
		for _, placeholder := range required[name] {
			if !slices.Contains(used, placeholder) {
				return fmt.Errorf("location '%s' must use '{%s}'", name, placeholder)
			}
		}
	}
	switch {
	case c.Kind == RegistryKindCartridges && c.Fabric == nil:
		return fmt.Errorf("a cartridges registry must name its fabric")
	case c.Kind == RegistryKindCartridges:
		if err := checkHTTPS("fabric", *c.Fabric); err != nil {
			return err
		}
		if strings.HasSuffix(*c.Fabric, "/") {
			return fmt.Errorf("fabric '%s' ends with '/'", *c.Fabric)
		}
	case c.Fabric != nil:
		return fmt.Errorf("only a cartridges registry names a fabric, and this is a %s registry", c.Kind)
	}
	if c.NotAfter != nil && *c.NotAfter <= c.IssuedAt {
		return fmt.Errorf("not_after %d is not after issued_at %d", *c.NotAfter, c.IssuedAt)
	}
	return nil
}

// Location is one location with its placeholders filled. Every placeholder the
// template uses must be given, and nothing else may be.
func (c *Charter) Location(name string, values map[string]string) (string, error) {
	template, present := c.Locations[name]
	if !present {
		return "", fmt.Errorf("the %s registry at %s has no location '%s'", c.Kind, c.Anchor, name)
	}
	used, err := placeholdersIn(template)
	if err != nil {
		return "", err
	}
	for key := range values {
		if !slices.Contains(used, key) {
			return "", fmt.Errorf("location '%s' does not take '%s'", name, key)
		}
	}
	out := template
	for _, placeholder := range used {
		value, given := values[placeholder]
		if !given {
			return "", fmt.Errorf("location '%s' needs '%s'", name, placeholder)
		}
		out = strings.ReplaceAll(out, "{"+placeholder+"}", value)
	}
	return out, nil
}

// IsSlug reports whether s can be a registry slug: [a-z0-9][a-z0-9.-]{0,62},
// and not the reserved "dev".
func IsSlug(s string) bool {
	if s == "" || len(s) > 63 || s == DevSlug || !isLowerAlnum(s[0]) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isLowerAlnum(s[i]) && s[i] != '.' && s[i] != '-' {
			return false
		}
	}
	return true
}

// IsExperimentID reports whether s can be a lab experiment id:
// [a-z0-9][a-z0-9-]{0,39}.
func IsExperimentID(s string) bool {
	if s == "" || len(s) > 40 || !isLowerAlnum(s[0]) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isLowerAlnum(s[i]) && s[i] != '-' {
			return false
		}
	}
	return true
}

func isLowerAlnum(b byte) bool { return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') }

func isLabel(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isLowerAlnum(s[i]) && s[i] != '-' {
			return false
		}
	}
	return true
}

func checkHTTPS(what, url string) error {
	scheme, rest, found := strings.Cut(url, "://")
	if !found || scheme != "https" || rest == "" || strings.HasPrefix(rest, "/") {
		return fmt.Errorf("%s '%s' is not an https URL", what, url)
	}
	if strings.ContainsAny(url, "?#") {
		return fmt.Errorf("%s '%s' carries a query or fragment", what, url)
	}
	return nil
}

// placeholdersIn is the {name} placeholders a template uses, in order of first
// use. A stray brace is an error rather than literal text.
func placeholdersIn(template string) ([]string, error) {
	var out []string
	rest := template
	for {
		open := strings.IndexAny(rest, "{}")
		if open < 0 {
			return out, nil
		}
		if rest[open] == '}' {
			return nil, fmt.Errorf("'%s' has an unmatched '}'", template)
		}
		after := rest[open+1:]
		end := strings.IndexByte(after, '}')
		if end < 0 {
			return nil, fmt.Errorf("'%s' has an unclosed '{'", template)
		}
		name := after[:end]
		valid := name != ""
		for i := 0; i < len(name); i++ {
			if !(name[i] >= 'a' && name[i] <= 'z') && name[i] != '_' {
				valid = false
			}
		}
		if !valid {
			return nil, fmt.Errorf("'%s' has a placeholder '{%s}' that is not a name", template, name)
		}
		if !slices.Contains(out, name) {
			out = append(out, name)
		}
		rest = after[end+1:]
	}
}

// ReadStoredCharter reads the charter stored in a registry's folder, holds it
// to its shape and its kind, and requires the folder to be named for it. The
// signature beside it is the Rust host's to verify (see the package comment).
func ReadStoredCharter(folder string, kind RegistryKind) (*Charter, error) {
	path := filepath.Join(folder, CharterFile)
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", path, err)
	}
	if _, err := os.Stat(filepath.Join(folder, CharterSigFile)); err != nil {
		return nil, fmt.Errorf("the charter in %s has no signature beside it: %w", folder, err)
	}
	charter, err := ParseCharter(body)
	if err != nil {
		return nil, err
	}
	if charter.Kind != kind {
		return nil, fmt.Errorf("the charter is for a %s registry, not a %s one", charter.Kind, kind)
	}
	if name := filepath.Base(folder); name != charter.Slug {
		return nil, fmt.Errorf("the charter for '%s' names slug '%s', but it is stored in a folder named '%s'",
			charter.Anchor, charter.Slug, name)
	}
	return charter, nil
}

// RegistryFolder is what one top-level folder of a cartridge tree holds: the
// cartridges built with no registry, or a chartered registry's — named by the
// charter stored in it, never by its own name.
type RegistryFolder struct {
	// Charter is nil for the reserved dev folder.
	Charter *Charter
}

// OpenRegistryFolder opens a top-level folder: "dev" by its reserved name, any
// other by the cartridges charter stored in it.
func OpenRegistryFolder(folder string) (RegistryFolder, error) {
	if filepath.Base(folder) == DevSlug {
		return RegistryFolder{}, nil
	}
	charter, err := ReadStoredCharter(folder, RegistryKindCartridges)
	if err != nil {
		return RegistryFolder{}, err
	}
	return RegistryFolder{Charter: charter}, nil
}

// Slug is the folder's name.
func (f RegistryFolder) Slug() string {
	if f.Charter == nil {
		return DevSlug
	}
	return f.Charter.Slug
}

// Anchor is the anchor every cartridge in the folder must record: nil for dev.
func (f RegistryFolder) Anchor() *string {
	if f.Charter == nil {
		return nil
	}
	return &f.Charter.Anchor
}

// StoredCharterFor finds, among the registry folders under root, the one whose
// stored charter is of kind and for anchor: its folder and charter. A folder
// whose charter does not read is passed over; the anchor's current charter is
// then fetched in its place.
func StoredCharterFor(root, anchor string, kind RegistryKind) (string, *Charter, bool) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", nil, false
	}
	for _, entry := range entries {
		folder := filepath.Join(root, entry.Name())
		if _, err := os.Stat(filepath.Join(folder, CharterFile)); err != nil {
			continue
		}
		charter, err := ReadStoredCharter(folder, kind)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] the charter stored in %s is not used: %v\n", folder, err)
			continue
		}
		if charter.Anchor == anchor {
			return folder, charter, true
		}
	}
	return "", nil, false
}

// FetchedCharter is a charter as its anchor serves it: the exact bytes of the
// body and of its signature, and the body read.
type FetchedCharter struct {
	Charter   *Charter
	Body      []byte
	Signature []byte
}

// CharterFetchErrorKind says why a charter could not be had from its anchor.
type CharterFetchErrorKind string

const (
	CharterFetchUnreachable CharterFetchErrorKind = "unreachable"
	// CharterFetchUnchartered: nothing is at <anchor>/charter.json — the URL is
	// not a MachineFabric registry.
	CharterFetchUnchartered CharterFetchErrorKind = "unchartered"
	CharterFetchHTTP        CharterFetchErrorKind = "http"
	// CharterFetchRefused: a charter is there, and it is not this registry's.
	CharterFetchRefused CharterFetchErrorKind = "refused"
)

// CharterFetchError is why fetching a charter from its anchor failed.
type CharterFetchError struct {
	Kind   CharterFetchErrorKind
	Anchor string
	URL    string
	Status int
	Err    error
}

func (e *CharterFetchError) Error() string {
	switch e.Kind {
	case CharterFetchUnchartered:
		return fmt.Sprintf("%s has no charter (%s answered 404) — it is not a MachineFabric registry", e.Anchor, e.URL)
	case CharterFetchHTTP:
		return fmt.Sprintf("%s answered HTTP %d", e.URL, e.Status)
	case CharterFetchRefused:
		return fmt.Sprintf("the charter of %s was refused: %v", e.Anchor, e.Err)
	default:
		return fmt.Sprintf("cannot reach %s: %v", e.URL, e.Err)
	}
}

func (e *CharterFetchError) Unwrap() error { return e.Err }

// FetchCharter fetches <anchor>/charter.json and its signature, reads the body
// and holds it to the anchor and kind asked. The signature is carried for the
// Rust host to verify (see the package comment).
//
// The two are fetched with one cache-bust token, because the signature holds
// only over a coherent pair and a CDN may cache them from different issues.
func FetchCharter(client *http.Client, anchor string, kind RegistryKind) (*FetchedCharter, error) {
	cacheBust := time.Now().UnixMilli()
	body, err := getCharterFile(client, anchor, anchor+"/"+CharterFile, cacheBust)
	if err != nil {
		return nil, err
	}
	signature, err := getCharterFile(client, anchor, anchor+"/"+CharterSigFile, cacheBust)
	if err != nil {
		return nil, err
	}
	refused := func(err error) error {
		return &CharterFetchError{Kind: CharterFetchRefused, Anchor: anchor, Err: err}
	}
	charter, err := ParseCharter(body)
	if err != nil {
		return nil, refused(err)
	}
	if charter.Anchor != anchor {
		return nil, refused(fmt.Errorf("it is the charter of %s", charter.Anchor))
	}
	if charter.Kind != kind {
		return nil, refused(fmt.Errorf("it charters a %s registry, not a %s one", charter.Kind, kind))
	}
	return &FetchedCharter{Charter: charter, Body: body, Signature: signature}, nil
}

func getCharterFile(client *http.Client, anchor, url string, cacheBust int64) ([]byte, error) {
	resp, err := client.Get(fmt.Sprintf("%s?cb=%d", url, cacheBust))
	if err != nil {
		return nil, &CharterFetchError{Kind: CharterFetchUnreachable, Anchor: anchor, URL: url, Err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, &CharterFetchError{Kind: CharterFetchUnchartered, Anchor: anchor, URL: url}
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, &CharterFetchError{Kind: CharterFetchHTTP, Anchor: anchor, URL: url, Status: resp.StatusCode}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &CharterFetchError{Kind: CharterFetchUnreachable, Anchor: anchor, URL: url, Err: err}
	}
	return body, nil
}

// StoreCharter writes a fetched charter and its signature, byte for byte, into
// folder — which must be named for the charter's slug, as ReadStoredCharter
// requires of every folder it reads.
func StoreCharter(fetched *FetchedCharter, folder string) error {
	if name := filepath.Base(folder); name != fetched.Charter.Slug {
		return fmt.Errorf("the charter of %s names slug '%s' and cannot be stored in a folder named '%s'",
			fetched.Charter.Anchor, fetched.Charter.Slug, name)
	}
	if err := os.MkdirAll(folder, 0o755); err != nil {
		return err
	}
	if err := writeAtomic(filepath.Join(folder, CharterFile), fetched.Body); err != nil {
		return err
	}
	return writeAtomic(filepath.Join(folder, CharterSigFile), fetched.Signature)
}

func writeAtomic(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp-%d", path, os.Getpid())
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
