// A cartridge whose build identity arrives through the linker.
//
// This is the fixture for TEST7161. It exists to be BUILT — with and without
// the ldflags `dx cartridge` injects — so the test can observe what each build
// produces. It is under `testdata/` so the module's own `go build ./...` never
// compiles it: the point is what happens when it is built a particular way, and
// the test is the only thing that knows which way.
//
// It was previously `capdag-go/examples/testcartridge`, a separate module built
// as its own test row. It moved here when the canonical cartridge stubs took
// over the "example a module consumer reads" role — the build contract is what
// was left, and a contract deserves a test rather than a compile.
package main

import (
	"fmt"
	"os"

	capdag "github.com/machinefabric/capdag-go"
)

// cartridgeChannel is set at link time via
//
//	go build -ldflags='-X main.cartridgeChannel=release'
//
// (or "nightly"). `dx cartridge` injects it from $MFR_CARTRIDGE_CHANNEL. An
// empty value means the build path did not set the flag, which is a build-system
// bug — fail loudly at startup rather than ship a binary with no channel.
var cartridgeChannel string

// cartridgeRegistryURL is set at link time via
//
//	go build -ldflags='-X main.cartridgeRegistryURL=https://...'
//
// when the cartridge is built for a specific registry. Empty (the default) ⇔ dev
// build; the cartridge can only be installed under the on-disk `dev/` slot.
// Mirror of Rust's `option_env!("MFR_CARTRIDGE_REGISTRY_URL")`.
var cartridgeRegistryURL string

func main() {
	if cartridgeChannel != "release" && cartridgeChannel != "nightly" {
		fmt.Fprintf(os.Stderr,
			"FATAL: cartridgeChannel link-time var is %q; expected \"release\" or \"nightly\". "+
				"Build with `dx cartridge --release` or `--nightly` to inject the channel via "+
				"-ldflags '-X main.cartridgeChannel=…'.\n",
			cartridgeChannel,
		)
		os.Exit(1)
	}

	// Derive the baked registry identity through the validated build-env path
	// (mirror of Rust's `registry_url_from_build_env(option_env!(...))`). The
	// build injects this var only for a published build; for a dev build the
	// ldflag is omitted, so the link-time string is empty — that empty IS the
	// "unset / dev" signal, passed as nil. RegistryURLFromBuildEnv panics on a
	// pointer-to-empty (a build-script bug), so an empty registry identity can
	// never be silently hashed into a fake slug.
	var rawRegistry *string
	if cartridgeRegistryURL != "" {
		rawRegistry = &cartridgeRegistryURL
	}
	registryURL := capdag.RegistryURLFromBuildEnv(rawRegistry)

	manifest := capdag.NewCapManifest(
		"buildcontract",
		"1.0.0",
		cartridgeChannel,
		registryURL,
		"Build-identity contract fixture",
		[]capdag.CapGroup{capdag.DefaultGroup([]capdag.Cap{
			{
				Urn:     mustParseCapUrn(capdag.CapIdentity),
				Title:   "Identity",
				Aliases: []string{"identity"},
			},
		})},
	)

	runtime, err := capdag.NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create runtime: %v\n", err)
		os.Exit(1)
	}
	// Auto-detects CLI vs CBOR mode; `manifest` is answered in CLI mode, which
	// is how the host reads a cartridge's identity.
	if err := runtime.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Runtime error: %v\n", err)
		os.Exit(1)
	}
}

func mustParseCapUrn(urnStr string) *capdag.CapUrn {
	urn, err := capdag.NewCapUrnFromString(urnStr)
	if err != nil {
		panic(fmt.Sprintf("invalid URN: %s - %v", urnStr, err))
	}
	return urn
}
