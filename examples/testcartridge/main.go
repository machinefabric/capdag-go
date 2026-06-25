package main

import (
	"encoding/json"
	"fmt"
	"os"

	capdag "github.com/machinefabric/capdag-go"
)

// cartridgeChannel is set at link time via
//   go build -ldflags='-X main.cartridgeChannel=release'
// (or "nightly"). The build wrapper (`dx cartridge`) injects this
// from $MFR_CARTRIDGE_CHANNEL. An empty value here means the build
// path didn't set the flag, which is a build-system bug — fail
// loudly at startup rather than ship a binary with no channel.
var cartridgeChannel string

// cartridgeRegistryURL is set at link time via
//   go build -ldflags='-X main.cartridgeRegistryURL=https://...'
// when the cartridge is being built for a specific registry. Empty
// (the default) ⇔ dev build; the cartridge can only be installed
// under the on-disk `dev/` slot. Mirror of Rust's
// `option_env!("MFR_CARTRIDGE_REGISTRY_URL")`.
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
	// (mirror of Rust's
	// `registry_url_from_build_env(option_env!("MFR_CARTRIDGE_REGISTRY_URL"))`).
	// The build injects this var only for a published build; for a dev build the
	// `-X main.cartridgeRegistryURL` ldflag is omitted, so the link-time string
	// is empty — that empty is the "unset / dev" signal, passed as nil. A
	// non-empty value is validated and passed through. RegistryURLFromBuildEnv
	// panics on a pointer-to-empty (a build-script bug), so an empty registry
	// identity can never be silently hashed into a fake slug.
	var rawRegistry *string
	if cartridgeRegistryURL != "" {
		rawRegistry = &cartridgeRegistryURL
	}
	registryURL := capdag.RegistryURLFromBuildEnv(rawRegistry)

	// Create manifest
	manifest := capdag.NewCapManifest(
		"testcartridge",
		"1.0.0",
		cartridgeChannel,
		registryURL,
		"Test cartridge for Go",
		[]capdag.CapGroup{capdag.DefaultGroup([]capdag.Cap{
			{
				Urn:     mustParseCapUrn(capdag.CapIdentity),
				Title:   "Echo",
				Command: "echo",
			},
			{
				Urn:     mustParseCapUrn(`cap:in="media:void";void-test;out="media:void"`),
				Title:   "Void Test",
				Command: "void",
			},
		})},
	)

	// Create runtime
	runtime, err := capdag.NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create runtime: %v\n", err)
		os.Exit(1)
	}

	// Register echo handler
	runtime.Register(capdag.CapIdentity,
		func(payload []byte, emitter capdag.StreamEmitter, peer capdag.PeerInvoker) error {
			// Parse input JSON
			var input map[string]interface{}
			if err := json.Unmarshal(payload, &input); err != nil {
				return fmt.Errorf("failed to parse input: %w", err)
			}

			// Extract the text field
			text, ok := input["text"].(string)
			if !ok {
				return fmt.Errorf("missing or invalid 'text' field")
			}

			// Echo it back
			response := map[string]string{
				"result": text,
			}

			responseData, err := json.Marshal(response)
			if err != nil {
				return fmt.Errorf("failed to marshal response: %w", err)
			}

			emitter.Emit(responseData)
			return nil
		})

	// Register void test handler
	runtime.Register(`cap:in="media:void";void-test;out="media:void"`,
		func(payload []byte, emitter capdag.StreamEmitter, peer capdag.PeerInvoker) error {
			// Void capability - no input, no output
			emitter.Emit([]byte{})
			return nil
		})

	// Run runtime (auto-detects CLI vs CBOR mode)
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
