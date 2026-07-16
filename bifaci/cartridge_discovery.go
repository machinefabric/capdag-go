package bifaci

// Shared cartridge discovery.
//
// The on-disk scan + identity validation + HELLO probe that classifies each
// installed cartridge version directory as attachable (Directory) or
// Incompatible. This is the single source of truth used by BOTH the engine
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
	"os/exec"
	"path/filepath"
	"runtime"
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
	RegistryURL *string
	FabricManifestVersion uint32
	// CartridgeRegistryVersion is the registry regime version this host speaks —
	// an on-disk PATH level: cartridges live under
	// {slug}/v{CartridgeRegistryVersion}/{channel}/…, pinned like the channel so
	// a v1 host never scans a v2 cartridge tree.
	CartridgeRegistryVersion uint32
}

// Slug returns the on-disk top-level slug for THIS host's own baked registry
// (DevSlug when RegistryURL is nil). Discovery does not restrict scanning to
// this slug — it enumerates every slug folder on disk and validates each
// cartridge against the folder it sits under. Retained as a helper for callers
// that need the host's own slug.
func (d *DiscoveryIdentity) Slug() string {
	return SlugFor(d.RegistryURL)
}

// DiscoveredCartridgeKind discriminates the two classifications of a discovered
// cartridge version directory.
type DiscoveredCartridgeKind int

const (
	// DiscoveredCartridgeDirectory: passed every identity check and its HELLO
	// probe succeeded. Its caps will be registered for dispatch.
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
	Error       *CartridgeAttachmentError
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
	cmd := exec.Command(path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("cartridge %q stdin pipe: %w", path, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("cartridge %q stdout pipe: %w", path, err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to spawn cartridge %q: %w", path, err)
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

	manifest, limits, err := HandshakeInitiate(reader, writer)
	if err != nil {
		return nil, fmt.Errorf("cartridge %q HELLO failed: %w", path, err)
	}
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	capGroups, err := parseCapGroupsFromManifest(manifest)
	if err != nil {
		return nil, fmt.Errorf("cartridge %q invalid manifest: %w", path, err)
	}
	return capGroups, nil
}

// DiscoverCartridges discovers every cartridge under {cartridgesRoot}/{slug}/
// {channel}/, scanning EVERY slug folder present on disk (full parity). The
// host's baked RegistryURL does NOT restrict which slugs are scanned; each
// cartridge is validated in place against the slug folder it sits under (the
// three-place rule in ReadCartridgeJsonFromDir). The channel folder IS pinned
// to the host's channel — release and nightly artefacts never mix.
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
		expectedSlug := slugEntry.Name()
		// {slug}/v{CartridgeRegistryVersion}/{channel}/… — the registry regime
		// version is a path level pinned to the host's version (like channel).
		scanRoot := filepath.Join(slugDir, fmt.Sprintf("v%d", identity.CartridgeRegistryVersion), string(identity.Channel))
		if fi, err := os.Stat(scanRoot); err != nil || !fi.IsDir() {
			// This slug has no subtree for the host's (version, channel) — skip.
			continue
		}
		if err := scanChannelRoot(scanRoot, expectedSlug, identity, &discovered); err != nil {
			return nil, err
		}
	}

	return discovered, nil
}

// scanChannelRoot scans one {slug}/{channel}/ root: classify each cartridge name
// directory's newest version against the host identity and the slug folder it
// sits under. expectedSlug is the on-disk slug folder name — passed through so
// the three-place rule is enforced per cartridge. Appends results to discovered.
func scanChannelRoot(scanRoot, expectedSlug string, identity *DiscoveryIdentity, discovered *[]DiscoveredCartridge) error {
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

		// ReadCartridgeJsonFromDir enforces the three-place rule against the
		// ACTUAL slug folder (expectedSlug): the cartridge's declared
		// registry_url must hash to it. A non-null registry_url under dev/ (or
		// any slug != SlugFor(registry_url)) fails here as a slug mismatch.
		cj, err := ReadCartridgeJsonFromDir(versionDir, expectedSlug)
		if err != nil {
			// A slug mismatch (declared registry_url doesn't hash to this folder)
			// is a bad INSTALL CONTEXT (BadInstallation), distinct from an
			// unreadable/garbage cartridge.json (ManifestInvalid). Both are
			// surfaced + logged, never hosted.
			kind := CartridgeAttachmentErrorKindManifestInvalid
			var cje *CartridgeJsonError
			if errors.As(err, &cje) && cje.Kind == CartridgeJsonErrorRegistrySlugMismatch {
				kind = CartridgeAttachmentErrorKindBadInstallation
			}
			fmt.Fprintf(os.Stderr, "cartridge.json invalid or mis-placed under slug %q (%s) — surfacing as incompatible: %v\n", expectedSlug, versionDir, err)
			*discovered = append(*discovered, DiscoveredCartridge{
				Kind:        DiscoveredCartridgeIncompatible,
				VersionDir:  versionDir,
				Id:          pathDerivedName,
				Channel:     identity.Channel,
				RegistryURL: identity.RegistryURL,
				Version:     pathDerivedVersion,
				Error: &CartridgeAttachmentError{
					Kind:                  kind,
					Message:               fmt.Sprintf("cartridge.json failed to load under slug '%s': %v", expectedSlug, err),
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
					Kind: CartridgeAttachmentErrorKindBadInstallation,
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

		// Scheme check is per-cartridge: a dev cartridge (null registry_url)
		// never reaches here; a registry cartridge must use https (devMode=false
		// for the scheme relaxation, which only ever applied to null-registry
		// dev cartridges).
		if cj.RegistryURL != nil {
			res := ValidateRegistryURLScheme(*cj.RegistryURL, false)
			switch res.Kind {
			case RegistryURLSchemeOk:
				// allowed
			case RegistryURLSchemeNonHTTPS:
				*discovered = append(*discovered, DiscoveredCartridge{
					Kind:        DiscoveredCartridgeIncompatible,
					VersionDir:  versionDir,
					Id:          cj.Name,
					Channel:     CartridgeChannel(cj.Channel),
					RegistryURL: cj.RegistryURL,
					Version:     cj.Version,
					Error: &CartridgeAttachmentError{
						Kind: CartridgeAttachmentErrorKindIncompatible,
						Message: fmt.Sprintf(
							"registry_url uses '%s' scheme, must be https in non-dev builds. Rebuild the cartridge with an https registry URL.",
							res.Scheme,
						),
						DetectedAtUnixSeconds: detectedAt,
					},
				})
				continue
			case RegistryURLSchemeNotAURL:
				*discovered = append(*discovered, DiscoveredCartridge{
					Kind:        DiscoveredCartridgeIncompatible,
					VersionDir:  versionDir,
					Id:          cj.Name,
					Channel:     CartridgeChannel(cj.Channel),
					RegistryURL: cj.RegistryURL,
					Version:     cj.Version,
					Error: &CartridgeAttachmentError{
						Kind:                  CartridgeAttachmentErrorKindIncompatible,
						Message:               fmt.Sprintf("registry_url '%s' is not a well-formed URL.", res.Bad),
						DetectedAtUnixSeconds: detectedAt,
					},
				})
				continue
			}
		}

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

		// Bundled-cartridge integrity. A cartridge marked `installed_from: bundle`
		// is shipped INSIDE this build, not user-installed, and has no upstream
		// registry to verify against — so it needs its own integrity proof. The
		// mechanism is platform-split:
		//
		//   - macOS: the OS code-signature IS the guard (notarized .app); a
		//     content hash would be re-broken by Apple's (re)signing, so macOS
		//     does NOT bake or verify hashes — it trusts the signature, logged
		//     as an explicit, visible rule.
		//   - Linux/Windows: binaries are unsigned, so the integrity proof is a
		//     content hash baked into the binary at build time
		//     (BundledCartridgeHashes). The on-disk directory must hash to the
		//     baked value; a mismatch or an entry absent from the baked set means
		//     the shipped cartridge was tampered with or the build failed to
		//     record it — surfaced incompatible + logged, never hosted.
		if cj.InstalledFrom != nil && *cj.InstalledFrom == CartridgeInstallSourceBundle {
			if runtime.GOOS == "darwin" {
				fmt.Fprintf(os.Stderr, "bundled cartridge integrity on macOS is the OS code-signature (notarized .app); baked-hash verification is intentionally skipped: %s %s\n", cj.Name, cj.Version)
			} else {
				if reason := verifyBundledCartridgeHash(cj.Name, cj.Version, versionDir); reason != "" {
					fmt.Fprintf(os.Stderr, "bundled cartridge hash verification failed — surfacing as incompatible: %s %s: %s\n", cj.Name, cj.Version, reason)
					*discovered = append(*discovered, DiscoveredCartridge{
						Kind:        DiscoveredCartridgeIncompatible,
						VersionDir:  versionDir,
						Id:          cj.Name,
						Channel:     CartridgeChannel(cj.Channel),
						RegistryURL: cj.RegistryURL,
						Version:     cj.Version,
						Error: &CartridgeAttachmentError{
							Kind:                  CartridgeAttachmentErrorKindBadInstallation,
							Message:               fmt.Sprintf("bundled cartridge integrity check failed: %s", reason),
							DetectedAtUnixSeconds: detectedAt,
						},
					})
					continue
				}
			}
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

// verifyBundledCartridgeHash verifies a bundled cartridge's on-disk content
// against the hash baked into this binary at build time. Returns "" when the
// directory hashes to the expected value for (name, version); a non-empty
// reason string when the pair is absent from the baked set or the hash differs.
//
// Non-macOS only: macOS bundled-cartridge integrity is the OS code-signature
// (see the discovery call site), so the binary there neither bakes nor checks
// these hashes.
func verifyBundledCartridgeHash(name, version, versionDir string) string {
	expected, ok := bundledCartridgeExpectedHash(name, version)
	if !ok {
		return fmt.Sprintf(
			"no baked hash for bundled cartridge %s %s — this build did not record it (MFR_BUNDLED_CARTRIDGE_HASHES)",
			name, version,
		)
	}
	actual, err := HashCartridgeDirectory(versionDir)
	if err != nil {
		return fmt.Sprintf("failed to hash bundled cartridge directory: %v", err)
	}
	if actual == expected {
		return ""
	}
	return fmt.Sprintf(
		"content hash mismatch — baked %s, on-disk %s; the shipped cartridge differs from what this build was compiled to ship",
		expected, actual,
	)
}
