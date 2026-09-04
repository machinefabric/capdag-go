package bifaci_test

// The cartridge build-identity contract, exercised through a real build.
//
// `(name, version, channel, registry_url)` is a cartridge's identity, and the
// last two are baked in by the BUILD — they are not runtime configuration. Each
// mirror bakes them its own way (Rust `env!()` at compile time, Swift a
// generated source file, Go the linker), which is an object-level divergence;
// what every mirror shares is the behavior: the values reach the manifest the
// host reads, and a build that omits them refuses to start rather than shipping
// a cartridge with no identity.
//
// TEST1872-1874 already cover `RegistryURLFromBuildEnv` as a function. This is
// the level above: that the build path actually delivers a value to it.

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/bifaci"
)

// The ldflag variable names are read out of the build script rather than
// written here, so renaming one there breaks this test instead of silently
// breaking every Go cartridge build.
const buildScriptRelPath = "../../scripts/lib/cartridge_project.sh"

var ldflagVarPattern = regexp.MustCompile(`-X (main\.[A-Za-z][A-Za-z0-9_]*)=`)

// ldflagVarNames returns the `main.*` variables the build script injects, or nil
// when the script is not present.
//
// capdag-go is published standalone, where the machinefabric build script does
// not exist. There the test falls back to the names this repo's fixture
// declares — the contract is still exercised end to end, it just cannot also
// detect a rename on the other side of a boundary that is not there.
func ldflagVarNames(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(buildScriptRelPath)
	if err != nil {
		t.Logf("build script not present at %s (standalone checkout): "+
			"testing against the fixture's own declared names", buildScriptRelPath)
		return []string{"main.cartridgeChannel", "main.cartridgeRegistryURL"}
	}
	var names []string
	seen := map[string]bool{}
	for _, m := range ldflagVarPattern.FindAllStringSubmatch(string(raw), -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			names = append(names, m[1])
		}
	}
	if len(names) == 0 {
		t.Fatalf("found no `-X main.<var>=` injections in %s — either the build "+
			"stopped baking the cartridge identity, or this parse broke",
			buildScriptRelPath)
	}
	return names
}

func pick(t *testing.T, names []string, suffix string) string {
	t.Helper()
	for _, n := range names {
		if strings.HasSuffix(strings.ToLower(n), strings.ToLower(suffix)) {
			return n
		}
	}
	t.Fatalf("the build script injects %v, none matching %q — the fixture and the "+
		"build script no longer agree on what carries the cartridge identity",
		names, suffix)
	return ""
}

// buildFixture compiles the build-contract fixture with the given ldflags and
// returns the binary path.
func buildFixture(t *testing.T, ldflags string) string {
	t.Helper()
	// The platform's executable suffix, because the fixture is BUILT here and
	// then RUN. Go writes whatever `-o` names, but Windows resolves an
	// executable by extension: an extensionless binary built fine and then
	// could not be started at all.
	out := filepath.Join(t.TempDir(), "buildcontract"+bifaci.ExecutableSuffix())
	args := []string{"build", "-o", out}
	if ldflags != "" {
		args = append(args, "-ldflags="+ldflags)
	}
	args = append(args, ".")
	cmd := exec.Command("go", args...)
	cmd.Dir = "testdata/buildcontract"
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the fixture with ldflags %q failed: %v\n%s", ldflags, err, combined)
	}
	return out
}

// TEST7161: a cartridge's channel and registry identity are baked by the BUILD
// and reach the manifest the host reads; a build that omits the channel refuses
// to start rather than shipping a cartridge with no identity.
//
// Checked by building the fixture three ways and running each, because that is
// the only level at which the claim is meaningful — the source compiles
// identically in all three cases, so compiling it proves nothing about the
// contract. Renaming the injected variable in the build script, or dropping the
// injection, fails here.
func Test7161_cartridge_build_identity_comes_from_the_build(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not available to build the fixture")
	}
	names := ldflagVarNames(t)
	channelVar := pick(t, names, "channel")
	registryVar := pick(t, names, "registryurl")

	type manifest struct {
		Name        string  `json:"name"`
		Channel     string  `json:"channel"`
		RegistryURL *string `json:"registry_url"`
	}
	readManifest := func(bin string) manifest {
		t.Helper()
		out, err := exec.Command(bin, "manifest").Output()
		if err != nil {
			t.Fatalf("the built cartridge could not answer `manifest`: %v", err)
		}
		var m manifest
		if err := json.Unmarshal(out, &m); err != nil {
			t.Fatalf("the cartridge printed a manifest that does not parse: %v\n%s", err, out)
		}
		return m
	}

	// 1. A nightly DEV build: channel injected, no registry. `registry_url` null
	//    is the dev signal the host keys on to allow only the `dev/` slot.
	dev := readManifest(buildFixture(t, "-X "+channelVar+"=nightly"))
	if dev.Channel != "nightly" {
		t.Errorf("channel %q did not reach the manifest — the linker injection is not landing", dev.Channel)
	}
	if dev.RegistryURL != nil {
		t.Errorf("a build with no registry ldflag must report registry_url null, got %q", *dev.RegistryURL)
	}

	// 2. A release build bound to a registry: both values reach the manifest
	//    verbatim. A published build must report exactly the URL it was
	//    compiled with — the on-disk slug is derived from it.
	const registry = "https://cartridges.example.test/v1/manifest"
	published := readManifest(buildFixture(t,
		"-X "+channelVar+"=release -X "+registryVar+"="+registry))
	if published.Channel != "release" {
		t.Errorf("channel %q did not reach the manifest", published.Channel)
	}
	if published.RegistryURL == nil || *published.RegistryURL != registry {
		t.Errorf("registry URL did not reach the manifest verbatim: %v", published.RegistryURL)
	}

	// 3. NO channel injected: the build is a build-system bug, and the cartridge
	//    refuses to start. Shipping it would put a cartridge with no channel
	//    into a tree whose layout is keyed by channel.
	unbaked := buildFixture(t, "")
	out, err := exec.Command(unbaked, "manifest").CombinedOutput()
	if err == nil {
		t.Fatalf("a cartridge built with no channel started anyway and printed:\n%s", out)
	}
	if !strings.Contains(string(out), "cartridgeChannel") {
		t.Errorf("the refusal must name the missing link-time variable so the build can be fixed; got:\n%s", out)
	}
}
