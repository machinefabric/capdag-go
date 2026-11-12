package bifaci

// The signed manifest a build ships beside its bundled cartridges.
//
// # What this replaces, and why
//
// Bundled cartridges — the ones shipped inside a build's own
// `bundled-cartridges/` tree — have no upstream registry to verify against, so
// they need their own integrity proof. That proof used to be a content hash
// baked into the binary at build time, and it was DISABLED on macOS: the
// distribution step re-signs every cartridge when it seals the `.app`, which
// rewrites their bytes long after the binary was compiled, so a baked hash
// could not survive. macOS was left trusting Gatekeeper instead.
//
// That made Apple's signature the load-bearing check on one platform and ours
// the load-bearing check on the others. It is the wrong way round: Apple's
// signature is what stops the operating system warning a user; OUR chain is
// what decides whether code runs, and it has to say the same thing everywhere.
//
// So the proof is a signed manifest — `bundle.json` with a `bundle.json.sig`
// envelope beside it — produced at the END of a build, after every platform
// signing step. There is no ordering problem left to have.
//
// # What a host learns from it, and when it checks the bytes
//
// Each entry records, under the signature, everything discovery decides from:
// the cartridge's directory hash, the fabric manifest version it was built
// against, and the cap groups it advertises in its HELLO (recorded by the
// build, which starts each cartridge once to ask). So discovering a bundle
// reads one signed file: it neither hashes a cartridge nor starts one, and a
// command that lists or plans pays for no cartridge it does not run.
//
// The bytes are checked when they matter, which is when a host starts a
// cartridge: its directory is hashed and held to the recorded value each time
// it is spawned, and a cartridge that was changed is refused there, by name
// (VerifyBundledContent). Checking at the spawn rather than at discovery also
// closes the gap between the two, during which a checked cartridge could be
// replaced before it ran.
//
// # What this package does and does not do
//
// It reads and applies a manifest. It does NOT verify the signature: this
// mirror carries no chain verification (see `release_cert.rs` in the Rust
// library, which is the only implementation of it), and a mirror that stubbed
// one would be worse than a mirror that has none. The caller supplies a
// BundleProof, and the only way to get a Verified one is to have verified.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/machinefabric/capdag-go/cap"
)

// BundleManifestFormat is the `format` every bundle manifest carries. A
// manifest without exactly this is refused rather than interpreted: the shape
// is the contract between the build that writes it and the runtime that reads
// it.
const BundleManifestFormat = "capdag.bundle/v2"

// BundleManifestFile is the manifest's name inside the bundled-cartridges root.
const BundleManifestFile = "bundle.json"

// BundleManifestSigFile is the signature envelope's name, beside the manifest.
const BundleManifestSigFile = "bundle.json.sig"

// BundledCartridge is one cartridge a build ships.
type BundledCartridge struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	// Channel is `release` or `nightly`. Stated so a manifest cannot vouch for
	// a cartridge from the other channel.
	Channel string `json:"channel"`
	// SHA256 is the directory hash, as HashCartridgeDirectory computes it —
	// sorted relative paths and file contents, `cartridge.json` excluded. That
	// exclusion is what lets a build write the manifest without changing what
	// the manifest attests.
	SHA256 string `json:"sha256"`
	// FabricManifestVersion is the fabric manifest version the cartridge was
	// built against.
	//
	// Its `cartridge.json` states it too, but that file is outside the hash;
	// recorded here it is signed, and a host pinned to another version refuses
	// the cartridge without starting it.
	FabricManifestVersion uint32 `json:"fabric_manifest_version"`
	// CapGroups are the cap groups the cartridge advertises in its HELLO, as
	// the build read them by starting it. Discovery takes them from here
	// instead of starting every bundled cartridge to ask again; they are a
	// function of the bytes SHA256 covers, which are checked before the
	// cartridge runs.
	CapGroups []CapGroup `json:"cap_groups"`
}

// UnmarshalJSON reads one entry and refuses an entry that leaves a field out.
//
// Every field is part of what the build signed and what discovery decides
// from, so an absent one is a malformed manifest, never a zero value: a
// missing fabric_manifest_version read as 0 would refuse the cartridge for the
// wrong reason, and missing cap_groups read as none would host a cartridge
// that can be asked for nothing.
func (c *BundledCartridge) UnmarshalJSON(data []byte) error {
	var raw struct {
		Name                  *string     `json:"name"`
		Version               *string     `json:"version"`
		Channel               *string     `json:"channel"`
		SHA256                *string     `json:"sha256"`
		FabricManifestVersion *uint32     `json:"fabric_manifest_version"`
		CapGroups             *[]CapGroup `json:"cap_groups"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var missing []string
	if raw.Name == nil {
		missing = append(missing, "name")
	}
	if raw.Version == nil {
		missing = append(missing, "version")
	}
	if raw.Channel == nil {
		missing = append(missing, "channel")
	}
	if raw.SHA256 == nil {
		missing = append(missing, "sha256")
	}
	if raw.FabricManifestVersion == nil {
		missing = append(missing, "fabric_manifest_version")
	}
	if raw.CapGroups == nil {
		missing = append(missing, "cap_groups")
	}
	if len(missing) > 0 {
		return fmt.Errorf("a bundled cartridge entry is missing %s", strings.Join(missing, ", "))
	}
	*c = BundledCartridge{
		Name:                  *raw.Name,
		Version:               *raw.Version,
		Channel:               *raw.Channel,
		SHA256:                *raw.SHA256,
		FabricManifestVersion: *raw.FabricManifestVersion,
		CapGroups:             *raw.CapGroups,
	}
	return nil
}

// BundleManifest is what a build ships beside its executable.
type BundleManifest struct {
	Format string `json:"format"`
	// Environment is the signing environment this bundle was built for.
	Environment string `json:"environment"`
	Cartridges  []BundledCartridge `json:"cartridges"`
}

// NewBundleManifest builds a manifest in a stable order, so the same tree
// produces the same bytes and therefore the same signature.
func NewBundleManifest(environment string, cartridges []BundledCartridge) BundleManifest {
	sorted := make([]BundledCartridge, len(cartridges))
	copy(sorted, cartridges)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Name != sorted[j].Name {
			return sorted[i].Name < sorted[j].Name
		}
		return sorted[i].Version < sorted[j].Version
	})
	return BundleManifest{
		Format:      BundleManifestFormat,
		Environment: environment,
		Cartridges:  sorted,
	}
}

// Entry returns what the manifest says about one cartridge, and whether it says
// anything at all.
func (m BundleManifest) Entry(name, version string) (BundledCartridge, bool) {
	for _, one := range m.Cartridges {
		if one.Name == name && one.Version == version {
			return one, true
		}
	}
	return BundledCartridge{}, false
}

// BundleProof is what a discovery run knows about the bundle it is scanning.
//
// Carried rather than looked up. Verification is one act per discovery — a
// chain check per cartridge would do the same work repeatedly and give as many
// chances to disagree — and making it a value means the thing that LOADS a
// manifest and the thing that USES one are separable.
//
// The zero value refuses everything with an empty reason, which is why it is
// never the zero value in practice: NoBundle states why, and a caller that
// forgot gets a refusal rather than a pass.
type BundleProof struct {
	// Manifest is the verified manifest, when there is one.
	Manifest *BundleManifest
	// Reason is why nothing can be proven, when Manifest is nil.
	Reason string
}

// NoBundle is a root that ships no bundle at all.
//
// The operator's installed-cartridges directory is one: nothing there was put
// there by a build, so a cartridge claiming to be bundled is in the wrong place
// and is refused saying so.
func NoBundle(reason string) BundleProof {
	return BundleProof{Reason: reason}
}

// ProvenBundle is a root whose bundled cartridges are held to `manifest`.
//
// Only a caller that has verified the manifest's signature may construct this.
// This package cannot: it carries no chain verification, and stubbing one would
// turn a refusal into a pass.
func ProvenBundle(manifest BundleManifest) BundleProof {
	return BundleProof{Manifest: &manifest}
}

// Admit answers what this proof records about one bundled cartridge, which
// discovery takes its caps and its fabric manifest version from, or why the
// cartridge is not proven.
//
// Reads nothing on disk. The cartridge's bytes are held to the recorded hash
// when a host starts it (VerifyBundledContent), not here.
func (p BundleProof) Admit(name, version, channel string) (BundledCartridge, error) {
	if p.Manifest == nil {
		if p.Reason == "" {
			return BundledCartridge{}, errors.New("nothing proves the bundled cartridges under this root")
		}
		return BundledCartridge{}, errors.New(p.Reason)
	}
	return p.Manifest.Admit(name, version, channel)
}

// Admit answers the entry for one bundled cartridge, held to the channel it is
// on.
//
// Unlisted, or listed at another version, is a cartridge the build did not
// record — a stale copy left from an earlier build among them
// (BundleErrorNotListed). Listed for the other channel is a manifest vouching
// for an artefact it did not describe (BundleErrorWrongChannel).
func (m BundleManifest) Admit(name, version, channel string) (BundledCartridge, error) {
	entry, listed := m.Entry(name, version)
	if !listed {
		return BundledCartridge{}, &BundleError{Kind: BundleErrorNotListed, Name: name, Version: version}
	}
	if entry.Channel != channel {
		return BundledCartridge{}, &BundleError{
			Kind:     BundleErrorWrongChannel,
			Name:     name,
			Version:  version,
			Recorded: entry.Channel,
			Found:    channel,
		}
	}
	return entry, nil
}

// VerifyBundledContent holds a bundled cartridge's directory to the hash its
// build recorded.
//
// A host calls this each time it starts a bundled cartridge, which is the
// moment the bytes are about to run. A directory that cannot be hashed is
// BundleErrorUnreadable; one that hashes to anything else is
// BundleErrorContentMismatch, whose message names the cartridge, its
// directory, and both hashes.
func VerifyBundledContent(name, version, versionDir, recorded string) error {
	actual, err := HashCartridgeDirectory(versionDir)
	if err != nil {
		return &BundleError{Kind: BundleErrorUnreadable, Path: versionDir, Err: err}
	}
	if actual != recorded {
		return &BundleError{
			Kind:     BundleErrorContentMismatch,
			Path:     versionDir,
			Name:     name,
			Version:  version,
			Expected: recorded,
			Actual:   actual,
		}
	}
	return nil
}

// BundleErrorKind says which way a bundle manifest, or a bundled cartridge,
// failed to be proven.
type BundleErrorKind int

const (
	// BundleErrorUnreadable: a file or directory could not be read.
	BundleErrorUnreadable BundleErrorKind = iota
	// BundleErrorMalformed: the document is not a bundle manifest, or a cap
	// it records breaks the definition rules.
	BundleErrorMalformed
	// BundleErrorUnsupportedFormat: the document is a bundle manifest in
	// another format than BundleManifestFormat.
	BundleErrorUnsupportedFormat
	// BundleErrorNotListed: the manifest does not record this cartridge at
	// this version.
	BundleErrorNotListed
	// BundleErrorWrongChannel: the manifest records this cartridge for the
	// other channel.
	BundleErrorWrongChannel
	// BundleErrorContentMismatch: the cartridge's directory is not what the
	// build recorded.
	BundleErrorContentMismatch
)

// BundleError is why a bundle manifest, or a bundled cartridge, could not be
// proven.
//
// Every kind is a refusal. There is no advisory state and no "verified
// except": a bundled cartridge either is what its build recorded, or it does
// not run.
type BundleError struct {
	Kind BundleErrorKind
	// Path is the file that could not be read or parsed, or the cartridge's
	// version directory (ContentMismatch).
	Path    string
	Name    string
	Version string
	// Found is the format the document carries (UnsupportedFormat), or the
	// channel the cartridge is on (WrongChannel).
	Found string
	// Recorded is the channel the manifest records (WrongChannel).
	Recorded string
	// Expected is the hash the manifest records and Actual the one the
	// directory hashes to (ContentMismatch).
	Expected string
	Actual   string
	// Reason is why the document is malformed (Malformed).
	Reason string
	// Err is the read failure (Unreadable).
	Err error
}

func (e *BundleError) Error() string {
	switch e.Kind {
	case BundleErrorUnreadable:
		return fmt.Sprintf("cannot read %s: %v", e.Path, e.Err)
	case BundleErrorMalformed:
		return fmt.Sprintf("%s is not a bundle manifest: %s", e.Path, e.Reason)
	case BundleErrorUnsupportedFormat:
		return fmt.Sprintf("bundle manifest has format '%s' (expected '%s')", e.Found, BundleManifestFormat)
	case BundleErrorNotListed:
		return fmt.Sprintf(
			"the bundle manifest does not list %s %s; this build ships a cartridge it did not record",
			e.Name, e.Version,
		)
	case BundleErrorWrongChannel:
		return fmt.Sprintf(
			"the bundle manifest records %s %s for the '%s' channel, and this cartridge is on the '%s' channel",
			e.Name, e.Version, e.Recorded, e.Found,
		)
	case BundleErrorContentMismatch:
		return fmt.Sprintf(
			"the bundled cartridge %s %s was changed after its build recorded it, so it is not started: "+
				"its directory %s hashes to %s, and the build's signed bundle manifest records %s. "+
				"Reinstall the application it is bundled with",
			e.Name, e.Version, e.Path, e.Actual, e.Expected,
		)
	default:
		return fmt.Sprintf("bundle manifest error (kind %d)", int(e.Kind))
	}
}

func (e *BundleError) Unwrap() error {
	return e.Err
}

// BundleManifestPaths returns where the manifest and its signature live under a
// bundled-cartridges root.
func BundleManifestPaths(bundledRoot string) (manifest string, signature string) {
	return filepath.Join(bundledRoot, BundleManifestFile),
		filepath.Join(bundledRoot, BundleManifestSigFile)
}

// IsBundleManifestFile reports whether a name in a bundled-cartridges root
// belongs to this mechanism.
//
// Discovery reports unmanaged files in that directory; these two are managed,
// and a warning about them on every startup would train an operator to ignore
// the one that matters.
func IsBundleManifestFile(fileName string) bool {
	return fileName == BundleManifestFile || fileName == BundleManifestSigFile
}

// ReadBundleManifest reads and shape-checks the manifest under a
// bundled-cartridges root (parseBundleManifest). It does NOT verify the
// signature — the caller does that with a chain verifier this mirror does not
// have, and only then builds a ProvenBundle. Pinning the manifest's
// environment to the build's belongs with that verification, and so with the
// caller, too.
//
// The signature file's presence IS checked: an unsigned manifest proves
// nothing, and reporting that here means a caller cannot forget to look.
func ReadBundleManifest(bundledRoot string) (BundleManifest, []byte, string, error) {
	manifestPath, sigPath := BundleManifestPaths(bundledRoot)
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return BundleManifest{}, nil, "", fmt.Errorf(
			"no bundle manifest at %s — this build shipped cartridges it cannot vouch for: %w",
			manifestPath, err,
		)
	}
	envelope, err := os.ReadFile(sigPath)
	if err != nil {
		return BundleManifest{}, nil, "", fmt.Errorf(
			"no signature at %s — an unsigned bundle manifest proves nothing: %w",
			sigPath, err,
		)
	}
	manifest, err := parseBundleManifest(manifestBytes, manifestPath)
	if err != nil {
		return BundleManifest{}, nil, "", err
	}
	return manifest, manifestBytes, string(envelope), nil
}

// parseBundleManifest reads a manifest document, held to this format and to
// the cap definition rules.
//
// The format is read first and on its own, so a manifest in another format is
// refused for being one (BundleErrorUnsupportedFormat), rather than as a
// document that fails to parse. No earlier format is read: a v1 manifest
// records no caps and no fabric manifest version, so a host reading one would
// have to start every cartridge to learn them, which is what v2 exists to
// stop.
func parseBundleManifest(data []byte, path string) (BundleManifest, error) {
	var format struct {
		Format *string `json:"format"`
	}
	if err := json.Unmarshal(data, &format); err != nil {
		return BundleManifest{}, &BundleError{Kind: BundleErrorMalformed, Path: path, Reason: err.Error()}
	}
	if format.Format == nil {
		return BundleManifest{}, &BundleError{Kind: BundleErrorMalformed, Path: path, Reason: "it has no format"}
	}
	if *format.Format != BundleManifestFormat {
		return BundleManifest{}, &BundleError{Kind: BundleErrorUnsupportedFormat, Found: *format.Format}
	}
	var manifest BundleManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return BundleManifest{}, &BundleError{Kind: BundleErrorMalformed, Path: path, Reason: err.Error()}
	}
	if manifest.Cartridges == nil {
		return BundleManifest{}, &BundleError{Kind: BundleErrorMalformed, Path: path, Reason: "it has no cartridges list"}
	}
	// The recorded caps cross from the cartridge that advertised them to every
	// host that reads them here, so they are held to the definition rules as an
	// advertised manifest is (CapManifestFromAdvertised).
	for _, cartridge := range manifest.Cartridges {
		for _, group := range cartridge.CapGroups {
			for i := range group.Caps {
				if err := cap.ValidateCapDefinition(&group.Caps[i]); err != nil {
					return BundleManifest{}, &BundleError{
						Kind:   BundleErrorMalformed,
						Path:   path,
						Reason: fmt.Sprintf("%s %s: %v", cartridge.Name, cartridge.Version, err),
					}
				}
			}
		}
	}
	return manifest, nil
}
