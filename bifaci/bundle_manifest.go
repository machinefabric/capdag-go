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
// # What this package does and does not do
//
// It reads and applies a manifest. It does NOT verify the signature: this
// mirror carries no chain verification (see `release_cert.rs` in the Rust
// library, which is the only implementation of it), and a mirror that stubbed
// one would be worse than a mirror that has none. The caller supplies a
// BundleProof, and the only way to get a Verified one is to have verified.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// BundleManifestFormat is the `format` every bundle manifest carries. A
// manifest without exactly this is refused rather than interpreted.
const BundleManifestFormat = "capdag.bundle/v1"

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

// Check holds one bundled cartridge to what this proof allows. Returns "" when
// it passes and the reason when it does not.
func (p BundleProof) Check(name, version, versionDir string) string {
	if p.Manifest == nil {
		if p.Reason == "" {
			return "nothing proves the bundled cartridges under this root"
		}
		return p.Reason
	}
	entry, listed := p.Manifest.Entry(name, version)
	if !listed {
		return fmt.Sprintf(
			"the bundle manifest does not list %s %s; this build ships a cartridge it did not record",
			name, version,
		)
	}
	actual, err := HashCartridgeDirectory(versionDir)
	if err != nil {
		return fmt.Sprintf("failed to hash bundled cartridge directory: %v", err)
	}
	if actual != entry.SHA256 {
		return fmt.Sprintf(
			"%s %s does not match the bundle manifest: recorded %s, on disk %s",
			name, version, entry.SHA256, actual,
		)
	}
	return ""
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
// bundled-cartridges root. It does NOT verify the signature — the caller does
// that with a chain verifier this mirror does not have, and only then builds a
// ProvenBundle.
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
	var manifest BundleManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return BundleManifest{}, nil, "", fmt.Errorf("%s is not a bundle manifest: %w", manifestPath, err)
	}
	if manifest.Format != BundleManifestFormat {
		return BundleManifest{}, nil, "", fmt.Errorf(
			"bundle manifest has format %q (expected %q)", manifest.Format, BundleManifestFormat,
		)
	}
	return manifest, manifestBytes, string(envelope), nil
}
