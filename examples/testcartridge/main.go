package main

import (
	"encoding/json"
	"fmt"
	"os"

	capdag "github.com/machinefabric/capdag-go"
	"github.com/machinefabric/capdag-go/bifaci"
)

// cartridgeChannel is set at link time via
//
//	go build -ldflags='-X main.cartridgeChannel=release'
//
// (or "nightly"). The build wrapper (`dx cartridge`) injects this
// from $MFR_CARTRIDGE_CHANNEL. An empty value here means the build
// path didn't set the flag, which is a build-system bug — fail
// loudly at startup rather than ship a binary with no channel.
var cartridgeChannel string

// cartridgeRegistryURL is set at link time via
//
//	go build -ldflags='-X main.cartridgeRegistryURL=https://...'
//
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
				Aliases: []string{"echo"},
			},
			{
				Urn:     mustParseCapUrn(`cap:in="media:void";void-test;out="media:void"`),
				Title:   "Void Test",
				Aliases: []string{"void"},
			},
		})},
	)

	// Create runtime
	runtime, err := capdag.NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create runtime: %v\n", err)
		os.Exit(1)
	}

	runtime.RegisterOp(capdag.CapIdentity, &EchoOp{})
	runtime.RegisterOp(`cap:in="media:void";void-test;out="media:void"`, &VoidOp{})

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

// EchoOp reads a `{"text": ...}` request and answers `{"result": ...}`.
type EchoOp struct{}

func (op *EchoOp) Perform(req *bifaci.Request) error {
	payload, err := collectPayload(req.Frames())
	if err != nil {
		return err
	}

	var input map[string]interface{}
	if err := json.Unmarshal(payload, &input); err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	text, ok := input["text"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid 'text' field")
	}

	responseData, err := json.Marshal(map[string]string{"result": text})
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	// Write, not EmitCbor: these are already-marshalled JSON bytes, and
	// EmitCbor would CBOR-encode them a second time.
	return req.Output().Write(responseData)
}

// VoidOp is a capability with no input and no output.
type VoidOp struct{}

func (op *VoidOp) Perform(req *bifaci.Request) error {
	// Drain the request stream even though there is nothing in it: leaving the
	// channel unread would strand the frame pump.
	for range req.Frames() {
	}
	// `out="media:void"` emits NOTHING. The runtime finalizes the empty
	// response stream when this returns; a zero-length Write would open one.
	return nil
}

// collectPayload reassembles a request's CHUNK payloads into the raw bytes the
// handler was sent, stopping at END.
func collectPayload(frames <-chan bifaci.Frame) ([]byte, error) {
	var payload []byte
	for frame := range frames {
		switch frame.FrameType {
		case bifaci.FrameTypeChunk:
			if err := bifaci.VerifyChunkChecksum(&frame); err != nil {
				return nil, fmt.Errorf("corrupted data: %w", err)
			}
			if frame.Payload == nil {
				continue
			}
			chunk, err := bifaci.DecodeChunkPayload(frame.Payload)
			if err != nil {
				return nil, err
			}
			payload = append(payload, chunk...)
		case bifaci.FrameTypeEnd:
			return payload, nil
		}
	}
	return payload, nil
}
