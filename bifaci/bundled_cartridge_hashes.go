package bifaci

// bundledCartridgeHash is one (name, version, dir-hash) entry baked into the
// binary at build time. Mirrors the Rust `BUNDLED_CARTRIDGE_HASHES` const,
// which build.rs codegens from MFR_BUNDLED_CARTRIDGE_HASHES. A bundled
// provider (`installed_from: bundle`) on a non-macOS host must hash to the
// recorded value or it is rejected as BadInstallation.
type bundledCartridgeHash struct {
	Name    string
	Version string
	Hash    string
}

// BundledCartridgeHashes is the baked set of bundled-cartridge directory hashes.
//
// Empty by default — the equivalent of the Rust const under a plain build with
// no providers bundled (MFR_BUNDLED_CARTRIDGE_HASHES unset). A real bundle build
// would generate this slice so the matching on-disk provider directory passes
// the integrity gate; an empty set means every `installed_from: bundle`
// cartridge is rejected (no baked hash), which is the intended behaviour when
// nothing was bundled.
var BundledCartridgeHashes = []bundledCartridgeHash{}

// bundledCartridgeExpectedHash looks up the baked expected directory hash for a
// bundled cartridge, or ("", false) if (name, version) was not recorded at build
// time.
func bundledCartridgeExpectedHash(name, version string) (string, bool) {
	for _, e := range BundledCartridgeHashes {
		if e.Name == name && e.Version == version {
			return e.Hash, true
		}
	}
	return "", false
}
