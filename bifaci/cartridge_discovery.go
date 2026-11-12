package bifaci

// Shared cartridge discovery.
//
// The on-disk scan + identity validation + HELLO probe that classifies each
// installed cartridge version directory as attachable (Directory) or
// Incompatible. A bundled cartridge is not probed: its build's signed bundle
// manifest records what the probe would learn, and its bytes are held to that
// manifest when a host starts it. This is the single source of truth used by
// BOTH the engine
// (for its bundled cartridges/ tree) and the daemon (for the user-installed
// cartridge tree). Keeping one implementation guarantees the two hosts accept
// exactly the same cartridges and reject the rest with byte-identical verdicts.
//
// Managed layout (relative to the root passed to DiscoverCartridges):
// {root}/{slug}/{channel}/{name}/{version}/cartridge.json.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// DiscoveryIdentity is the identity a host accepts cartridges for. A cartridge
// whose cartridge.json diverges from this on channel, registry URL, registry
// scheme, or fabric manifest version is surfaced as Incompatible — never hosted.
type DiscoveryIdentity struct {
	Channel CartridgeChannel
	// RegistryURL is non-nil for release/nightly hosts, nil for dev hosts
	// (cartridges then live under the reserved dev slug and any registry
	// scheme is allowed).
	RegistryURL           *string
	FabricManifestVersion uint32
	// CartridgeRegistryVersion is the registry regime version this host speaks —
	// an on-disk PATH level: cartridges live under
	// {slug}/v{CartridgeRegistryVersion}/{channel}/…, pinned like the channel so
	// a v1 host never scans a v2 cartridge tree.
	CartridgeRegistryVersion uint32
	// Bundle is what proves the bundled cartridges under the root being
	// scanned. Carried rather than looked up, because verification is one act
	// per discovery and because the caller is the only thing that knows what
	// this root IS: a build's own bundled-cartridges tree carries a verified
	// manifest, and the operator's installed-cartridges directory carries
	// NoBundle — so a cartridge claiming to be bundled while sitting there is
	// refused for exactly that reason instead of being hosted because nobody
	// asked.
	Bundle BundleProof
}

// DiscoveredCartridgeKind discriminates the two classifications of a discovered
// cartridge version directory.
type DiscoveredCartridgeKind int

const (
	// DiscoveredCartridgeDirectory: passed every identity check, and its caps
	// are known: from its HELLO probe, or, for a bundled cartridge, from its
	// build's signed bundle manifest. Its caps will be registered for dispatch.
	DiscoveredCartridgeDirectory DiscoveredCartridgeKind = iota
	// DiscoveredCartridgeIncompatible: found on disk but failed a check. NOT
	// spawned, caps never enter the dispatch graph; surfaced with a structured
	// AttachmentError so the UI can render the reason. This is the uniform
	// surface for every discovery-time rejection — no silent log-and-skip.
	DiscoveredCartridgeIncompatible
)

// DiscoveredCartridge is a discovered cartridge version directory, classified.
//
// When Kind == DiscoveredCartridgeDirectory: EntryPoint and CapGroups are set,
// Error is nil. When Kind == DiscoveredCartridgeIncompatible: Error is set,
// EntryPoint/CapGroups are absent.
type DiscoveredCartridge struct {
	Kind        DiscoveredCartridgeKind
	EntryPoint  string
	VersionDir  string
	Id          string
	Channel     CartridgeChannel
	RegistryURL *string
	Version     string
	CapGroups   []CapGroup
	// BundleSHA256 is, for a bundled Directory cartridge, the directory hash
	// its build's signed bundle manifest records; nil for an installed or dev
	// one, and for an Incompatible one.
	//
	// Discovery does not hash a bundled cartridge. A host holds its directory
	// to this value each time it starts the cartridge (VerifyBundledContent),
	// and refuses to start one that was changed.
	BundleSHA256 *string
	Error        *CartridgeAttachmentError
}

// unixSecondsNow is the current wall-clock time as Unix seconds, for stamping
// CartridgeAttachmentError.DetectedAtUnixSeconds.
func unixSecondsNow() int64 {
	return time.Now().Unix()
}

// ProbeCartridgeCapGroups probes a cartridge binary for its capability surface.
//
// Spawns the binary, performs the bifaci HELLO handshake, parses the manifest,
// returns its full cap_groups, then kills the process. A binary that fails to
// spawn, fails HELLO, or returns an unparseable manifest is an error — the
// caller surfaces it as HandshakeFailed.
func ProbeCartridgeCapGroups(path string) ([]CapGroup, error) {
	cmd := Command(path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("cartridge '%s' stdin pipe: %w", path, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("cartridge '%s' stdout pipe: %w", path, err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to spawn cartridge '%s': %w", path, err)
	}
	// SIGKILL immediately once we are done — we have (or failed to get) the
	// manifest and don't wait for a clean exit.
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	reader := NewFrameReader(stdout)
	writer := NewFrameWriter(stdin)

	manifest, limits, _, err := HandshakeInitiate(reader, writer)
	if err != nil {
		return nil, fmt.Errorf("cartridge '%s' HELLO failed: %w", path, err)
	}
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	capGroups, err := parseCapGroupsFromManifest(manifest)
	if err != nil {
		return nil, fmt.Errorf("cartridge '%s' invalid manifest: %w", path, err)
	}
	return capGroups, nil
}

// DiscoverCartridges discovers every cartridge under {cartridgesRoot}/{slug}/
// {channel}/, scanning EVERY registry folder present on disk (full parity). The
// host's baked RegistryURL does NOT restrict which folders are scanned; each
// cartridge is validated in place against the registry folder it sits under,
// as the charter stored in that folder names it (the charter rule in
// ReadCartridgeJsonFromDir). The channel folder IS pinned to the host's
// channel — release and nightly artefacts never mix.
//
// An empty/absent scan root is not an error — it yields an empty roster. A real
// IO failure reading an existing scan root IS an error (it would otherwise
// masquerade as "no cartridges installed").
func DiscoverCartridges(cartridgesRoot string, identity *DiscoveryIdentity) ([]DiscoveredCartridge, error) {
	discovered := make([]DiscoveredCartridge, 0)

	info, err := os.Stat(cartridgesRoot)
	if err != nil || !info.IsDir() {
		return discovered, nil
	}

	slugEntries, err := os.ReadDir(cartridgesRoot)
	if err != nil {
		return nil, fmt.Errorf("read_dir(%s): %w", cartridgesRoot, err)
	}

	for _, slugEntry := range slugEntries {
		slugDir := filepath.Join(cartridgesRoot, slugEntry.Name())
		if !slugEntry.IsDir() {
			if slugEntry.Name() != ".DS_Store" {
				fmt.Fprintf(os.Stderr, "Unmanaged file in cartridges root — only registry-slug / dev directories belong here: %s\n", slugDir)
			}
			continue
		}
		// The folder is what its stored charter names, never what its own
		// name says. A folder whose charter is missing or refused still has
		// its cartridges surfaced — each as misplaced, with the reason — so
		// the operator sees what is there and why none of it runs.
		folder, folderErr := OpenRegistryFolder(slugDir)
		// {slug}/v{CartridgeRegistryVersion}/{channel}/… — the registry regime
		// version is a path level pinned to the host's version (like channel).
		scanRoot := filepath.Join(slugDir, fmt.Sprintf("v%d", identity.CartridgeRegistryVersion), string(identity.Channel))
		if fi, err := os.Stat(scanRoot); err != nil || !fi.IsDir() {
			// This slug has no subtree for the host's (version, channel) — skip.
			continue
		}
		if err := scanChannelRoot(scanRoot, slugDir, folder, folderErr, identity, &discovered); err != nil {
			return nil, err
		}
	}

	return discovered, nil
}

// scanChannelRoot scans one {slug}/{channel}/ root: classify each cartridge name
// directory's newest version against the host identity and the registry folder
// it sits under. folder is that folder, opened through the charter stored in
// it, or folderErr why it could not be — passed through so the charter rule is
// enforced per cartridge. Appends results to discovered.
func scanChannelRoot(scanRoot, slugDir string, folder RegistryFolder, folderErr error, identity *DiscoveryIdentity, discovered *[]DiscoveredCartridge) error {
	nameEntries, err := os.ReadDir(scanRoot)
	if err != nil {
		return fmt.Errorf("read_dir(%s): %w", scanRoot, err)
	}

	for _, entry := range nameEntries {
		nameDir := filepath.Join(scanRoot, entry.Name())
		if !entry.IsDir() {
			if entry.Name() != ".DS_Store" {
				fmt.Fprintf(os.Stderr, "Unmanaged file in {slug}/{channel}/ — only cartridge name directories belong here: %s\n", nameDir)
			}
			continue
		}

		subEntries, err := os.ReadDir(nameDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot read cartridge name directory %s: %v\n", nameDir, err)
			continue
		}

		var versionDirs []string
		for _, sub := range subEntries {
			subPath := filepath.Join(nameDir, sub.Name())
			if sub.IsDir() {
				versionDirs = append(versionDirs, subPath)
			} else if sub.Name() != ".DS_Store" {
				fmt.Fprintf(os.Stderr, "Unmanaged file inside cartridge name directory — only version directories belong here: %s\n", subPath)
			}
		}

		if len(versionDirs) == 0 {
			fmt.Fprintf(os.Stderr, "Cartridge name directory contains no version subdirectories: %s\n", nameDir)
			continue
		}

		// Prefer the newest version (lexical-descending on the version folder name).
		sort.Slice(versionDirs, func(i, j int) bool {
			return filepath.Base(versionDirs[i]) > filepath.Base(versionDirs[j])
		})
		versionDir := versionDirs[0]

		pathDerivedName := filepath.Base(nameDir)
		pathDerivedVersion := filepath.Base(versionDir)
		detectedAt := unixSecondsNow()

		// A folder with no charter that reads makes every cartridge in it
		// misplaced, for the charter's reason.
		if folderErr != nil {
			fmt.Fprintf(os.Stderr, "registry folder %s has no charter that reads (%s) — surfacing as incompatible: %v\n", slugDir, versionDir, folderErr)
			*discovered = append(*discovered, DiscoveredCartridge{
				Kind:       DiscoveredCartridgeIncompatible,
				VersionDir: versionDir,
				Id:         pathDerivedName,
				Channel:    identity.Channel,
				Version:    pathDerivedVersion,
				Error: &CartridgeAttachmentError{
					Kind:                  CartridgeAttachmentErrorKindMisplaced,
					Message:               fmt.Sprintf("the registry folder %s has no charter that reads: %v", slugDir, folderErr),
					DetectedAtUnixSeconds: detectedAt,
				},
			})
			continue
		}
		// ReadCartridgeJsonFromDir enforces the charter rule against the
		// folder the cartridge ACTUALLY sits in: its declared registry_url
		// must be the folder's anchor. A non-null registry_url under dev/, or
		// another registry's anchor, fails here as misplaced.
		cj, err := ReadCartridgeJsonFromDir(versionDir, folder)
		if err != nil {
			// A folder mismatch is a bad INSTALL CONTEXT, distinct from an
			// unreadable/garbage cartridge.json (ManifestInvalid). Both are
			// surfaced + logged, never hosted.
			kind := CartridgeAttachmentErrorKindManifestInvalid
			var cje *CartridgeJsonError
			if errors.As(err, &cje) && cje.Kind == CartridgeJsonErrorRegistryFolderMismatch {
				kind = CartridgeAttachmentErrorKindMisplaced
			}
			fmt.Fprintf(os.Stderr, "cartridge.json invalid or mis-placed in the registry folder %q (%s) — surfacing as incompatible: %v\n", folder.Slug(), versionDir, err)
			*discovered = append(*discovered, DiscoveredCartridge{
				Kind:        DiscoveredCartridgeIncompatible,
				VersionDir:  versionDir,
				Id:          pathDerivedName,
				Channel:     identity.Channel,
				RegistryURL: identity.RegistryURL,
				Version:     pathDerivedVersion,
				Error: &CartridgeAttachmentError{
					Kind:                  kind,
					Message:               fmt.Sprintf("cartridge.json failed to load in the registry folder '%s': %v", folder.Slug(), err),
					DetectedAtUnixSeconds: detectedAt,
				},
			})
			continue
		}

		if cj.Channel != string(identity.Channel) {
			*discovered = append(*discovered, DiscoveredCartridge{
				Kind:        DiscoveredCartridgeIncompatible,
				VersionDir:  versionDir,
				Id:          cj.Name,
				Channel:     CartridgeChannel(cj.Channel),
				RegistryURL: cj.RegistryURL,
				Version:     cj.Version,
				Error: &CartridgeAttachmentError{
					Kind: CartridgeAttachmentErrorKindMisplaced,
					Message: fmt.Sprintf(
						"Channel mismatch: cartridge declares '%s' but host is pinned to '%s'. Release and nightly artefacts must not mix.",
						cj.Channel, identity.Channel,
					),
					DetectedAtUnixSeconds: detectedAt,
				},
			})
			continue
		}

		// NO registry pin: the host's baked registry does NOT restrict which
		// registries' cartridges are discovered. Whether a version is actually
		// LISTED upstream is the verdict layer's call, applied after discovery.

		if cj.FabricManifestVersion != identity.FabricManifestVersion {
			*discovered = append(*discovered, DiscoveredCartridge{
				Kind:        DiscoveredCartridgeIncompatible,
				VersionDir:  versionDir,
				Id:          cj.Name,
				Channel:     CartridgeChannel(cj.Channel),
				RegistryURL: cj.RegistryURL,
				Version:     cj.Version,
				Error: &CartridgeAttachmentError{
					Kind: CartridgeAttachmentErrorKindFabricManifestVersionMismatch,
					Message: fmt.Sprintf(
						"Cartridge built against fabric manifest version %d, but host is pinned to %d. Rebuild the cartridge with MFR_FABRIC_MANIFEST_VERSION=%d.",
						cj.FabricManifestVersion, identity.FabricManifestVersion, identity.FabricManifestVersion,
					),
					DetectedAtUnixSeconds: detectedAt,
				},
			})
			continue
		}

		// A bundled cartridge. A cartridge marked `installed_from: bundle` is
		// shipped INSIDE this build, not user-installed, and has no upstream
		// registry to verify against — so the build's signed bundle manifest is
		// what proves it, on every platform. The proof was established when this
		// discovery started (see BundleProof); here it is applied to the
		// cartridge in hand.
		//
		// The manifest records what a probe would learn — the caps the cartridge
		// advertises, and the fabric manifest version it was built against — so
		// a bundled cartridge is neither started nor hashed here. Its bytes are
		// held to the recorded hash when a host starts it, which is when they
		// are about to run.
		if cj.InstalledFrom != nil && *cj.InstalledFrom == CartridgeInstallSourceBundle {
			recorded, err := identity.Bundle.Admit(cj.Name, cj.Version, cj.Channel)
			if err != nil {
				fmt.Fprintf(os.Stderr, "bundled cartridge is not proven by this build's signed bundle manifest — surfacing as incompatible: %s %s: %v\n", cj.Name, cj.Version, err)
				*discovered = append(*discovered, DiscoveredCartridge{
					Kind:        DiscoveredCartridgeIncompatible,
					VersionDir:  versionDir,
					Id:          cj.Name,
					Channel:     CartridgeChannel(cj.Channel),
					RegistryURL: cj.RegistryURL,
					Version:     cj.Version,
					Error: &CartridgeAttachmentError{
						Kind:                  CartridgeAttachmentErrorKindMisplaced,
						Message:               fmt.Sprintf("bundled cartridge integrity check failed: %v", err),
						DetectedAtUnixSeconds: detectedAt,
					},
				})
				continue
			}
			// cartridge.json is outside the hash; the signed record is what is
			// believed about the fabric the cartridge was built against.
			if recorded.FabricManifestVersion != identity.FabricManifestVersion {
				*discovered = append(*discovered, DiscoveredCartridge{
					Kind:        DiscoveredCartridgeIncompatible,
					VersionDir:  versionDir,
					Id:          cj.Name,
					Channel:     CartridgeChannel(cj.Channel),
					RegistryURL: cj.RegistryURL,
					Version:     cj.Version,
					Error: &CartridgeAttachmentError{
						Kind: CartridgeAttachmentErrorKindFabricManifestVersionMismatch,
						Message: fmt.Sprintf(
							"Bundled cartridge built against fabric manifest version %d, as its build's signed bundle manifest records, but host is pinned to %d. Rebuild the bundle with MFR_FABRIC_MANIFEST_VERSION=%d.",
							recorded.FabricManifestVersion, identity.FabricManifestVersion, identity.FabricManifestVersion,
						),
						DetectedAtUnixSeconds: detectedAt,
					},
				})
				continue
			}
			bundleSHA256 := recorded.SHA256
			*discovered = append(*discovered, DiscoveredCartridge{
				Kind:         DiscoveredCartridgeDirectory,
				EntryPoint:   cj.ResolveEntryPoint(versionDir),
				VersionDir:   versionDir,
				Id:           cj.Name,
				Channel:      CartridgeChannel(cj.Channel),
				RegistryURL:  cj.RegistryURL,
				Version:      cj.Version,
				CapGroups:    recorded.CapGroups,
				BundleSHA256: &bundleSHA256,
			})
			continue
		}

		entryPoint := cj.ResolveEntryPoint(versionDir)
		capGroups, err := ProbeCartridgeCapGroups(entryPoint)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to probe cartridge entry point — surfacing as incompatible: %s: %v\n", versionDir, err)
			*discovered = append(*discovered, DiscoveredCartridge{
				Kind:        DiscoveredCartridgeIncompatible,
				VersionDir:  versionDir,
				Id:          cj.Name,
				Channel:     CartridgeChannel(cj.Channel),
				RegistryURL: cj.RegistryURL,
				Version:     cj.Version,
				Error: &CartridgeAttachmentError{
					Kind:                  CartridgeAttachmentErrorKindHandshakeFailed,
					Message:               fmt.Sprintf("HELLO handshake / cap discovery probe failed: %v", err),
					DetectedAtUnixSeconds: detectedAt,
				},
			})
			continue
		}

		*discovered = append(*discovered, DiscoveredCartridge{
			Kind:        DiscoveredCartridgeDirectory,
			EntryPoint:  entryPoint,
			VersionDir:  versionDir,
			Id:          cj.Name,
			Channel:     CartridgeChannel(cj.Channel),
			RegistryURL: cj.RegistryURL,
			Version:     cj.Version,
			CapGroups:   capGroups,
		})
	}

	return nil
}
