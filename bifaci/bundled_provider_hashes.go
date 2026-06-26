package bifaci

// bundledProviderHash is one (name, version, dir-hash) entry baked into the
// binary at build time. Mirrors the Rust `BUNDLED_PROVIDER_HASHES` const,
// which build.rs codegens from MFR_BUNDLED_PROVIDER_HASHES. A bundled
// provider (`installed_from: bundle`) on a non-macOS host must hash to the
// recorded value or it is rejected as BadInstallation.
type bundledProviderHash struct {
	Name    string
	Version string
	Hash    string
}

// BundledProviderHashes is the baked set of bundled-provider directory hashes.
//
// Empty by default — the equivalent of the Rust const under a plain build with
// no providers bundled (MFR_BUNDLED_PROVIDER_HASHES unset). A real bundle build
// would generate this slice so the matching on-disk provider directory passes
// the integrity gate; an empty set means every `installed_from: bundle`
// cartridge is rejected (no baked hash), which is the intended behaviour when
// nothing was bundled.
var BundledProviderHashes = []bundledProviderHash{}

// bundledProviderExpectedHash looks up the baked expected directory hash for a
// bundled provider, or ("", false) if (name, version) was not recorded at build
// time.
func bundledProviderExpectedHash(name, version string) (string, bool) {
	for _, e := range BundledProviderHashes {
		if e.Name == name && e.Version == version {
			return e.Hash, true
		}
	}
	return "", false
}
