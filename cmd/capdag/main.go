// capdag — the Go capdag CLI.
//
// A capdag mirror is not just a library: `capdag new` is how a cartridge
// project comes into existence, and every mirror must be able to create the
// same project. This binary is the Go mirror's CLI.
//
// # What this binary does and does not do
//
// The commands here are exactly those the Go library can back today:
//
//	new                  scaffold a cartridge project in any vendored language
//	dev-install          install/update a dev cartridge under the dev slug
//	find                 show which fabric cap an alias or URN resolves to
//	resolve              print cap definition JSON
//	cache                clear/refresh the local fabric cache
//	hash-cartridge-dir   the deterministic content hash of a version directory
//
// `run`, single-cap dispatch, `plan` and `dag-viz` are NOT here, because this
// mirror has no plan executor and no path-planner engine. They are absent
// rather than stubbed: a command that accepted the arguments and then reported
// "unsupported" would be a worse lie than not existing, and `capdag help` says
// plainly what is missing and where it lives.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/machinefabric/capdag-go/bifaci"
	"github.com/machinefabric/capdag-go/dev"
	"github.com/machinefabric/capdag-go/fabric"
)

func main() {
	args := os.Args

	// `--fabric <url>` — point this invocation at a different fabric origin,
	// the CLI's equivalent of the registry base URL the desktop apps hand their
	// engine. It is stripped from argv before dispatch so it works in front of
	// ANY subcommand, and it sets the environment so the registry constructed
	// later reads one value from one place.
	args, err := takeFabricFlag(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if len(args) < 2 {
		printUsage(args[0])
		os.Exit(1)
	}

	switch args[1] {
	case "new":
		cmdNew(args)
	case "dev-install":
		cmdDevInstall(args)
	case "find":
		cmdFind(args)
	case "resolve":
		cmdResolve(args)
	case "cache":
		cmdCache(args)
	case "hash-cartridge-dir":
		cmdHashCartridgeDir(args)
	case "help", "--help", "-h":
		printUsage(args[0])
		os.Exit(0)
	default:
		// A `.machine` file or a bare cap alias means the caller wants to
		// EXECUTE something, which this mirror cannot do. Saying so — and
		// naming what does — beats "unknown command".
		if strings.HasSuffix(args[1], ".machine") || !strings.HasPrefix(args[1], "-") {
			fmt.Fprintf(os.Stderr,
				"%s: this mirror does not execute machines or caps — it has no plan executor.\n"+
					"Run it with the reference capdag CLI (the Rust build) instead.\n"+
					"This binary covers: %s\n",
				args[1], strings.Join(commandNames(), ", "))
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "Unknown option '%s'.\n", args[1])
		printUsage(args[0])
		os.Exit(2)
	}
}

func commandNames() []string {
	return []string{"new", "dev-install", "find", "resolve", "cache", "hash-cartridge-dir"}
}

func printUsage(program string) {
	p := filepath.Base(program)
	fmt.Fprintf(os.Stderr, `Usage:
  %[1]s new <name> <%[2]s> [-o <dir>]   Scaffold a new cartridge project
  %[1]s dev-install [<project-dir>]     Install/update a dev cartridge under the dev slug
  %[1]s find <cap-alias-or-urn>         Show what an alias or URN resolves to
  %[1]s resolve <cap-alias-or-urn>...   Print cap definition JSON (array for >1)
  %[1]s cache [clear|refresh]           Invalidate/renew the local fabric cache
  %[1]s hash-cartridge-dir <dir>        Deterministic content hash of a version directory

Options:
  --fabric <url>   Resolve caps/media/aliases against this fabric registry
                   instead of the built-in one (env: CDG_FABRIC_REGISTRY_URL).
                   Works before any subcommand.
  --help           Show this help

Not in this mirror: run, single-cap dispatch, plan, dag-viz. They need the plan
executor and the path-planner engine, which the Go library does not implement.
Use the reference capdag CLI for those.
`, p, dev.LanguageFlagList())
}

// takeFabricFlag strips `--fabric <url>` from argv and applies it to the
// environment.
//
// A caller-chosen origin invalidates any baked schema base: pairing a runtime
// fabric with a build-time schema URL would validate one origin's definitions
// against another's schemas.
func takeFabricFlag(args []string) ([]string, error) {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if args[i] != "--fabric" {
			out = append(out, args[i])
			continue
		}
		if i+1 >= len(args) {
			return nil, fmt.Errorf("--fabric requires a registry URL")
		}
		url := args[i+1]
		if url == "" || strings.HasPrefix(url, "-") {
			return nil, fmt.Errorf("--fabric requires a registry URL, got %q", url)
		}
		os.Unsetenv("CDG_SCHEMA_BASE_URL")
		os.Setenv("CDG_FABRIC_REGISTRY_URL", url)
		i++
	}
	return out, nil
}

// userCartridgeDir is the per-user cartridge install root, in the same
// {registry_slug}/{channel}/{name}/{version}/ tree every host uses.
func userCartridgeDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("cannot determine the home directory: %w", err)
	}
	return filepath.Join(home, ".capdag", "cartridges"), nil
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func registryOrExit() *fabric.FabricRegistry {
	registry, err := fabric.New()
	if err != nil {
		die("%v", err)
	}
	return registry
}

// ---------------------------------------------------------------------------
// new
// ---------------------------------------------------------------------------

func cmdNew(args []string) {
	var name string
	var language *dev.StubLanguage
	parent := "."

	for i := 2; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-o" || arg == "--output":
			i++
			if i >= len(args) {
				fmt.Fprintln(os.Stderr, "--output requires a directory path")
				os.Exit(2)
			}
			parent = args[i]
		case dev.Language(arg) != nil:
			// Two language flags is not a preference to resolve, it is a
			// command that cannot mean one thing.
			if language != nil {
				fmt.Fprintf(os.Stderr, "`new` takes one language: '%s' was already given, then '%s'.\n",
					language.Flag, arg)
				os.Exit(2)
			}
			language = dev.Language(arg)
		case strings.HasPrefix(arg, "--"):
			fmt.Fprintf(os.Stderr, "Unknown option '%s' for `new`. Languages: %s.\n",
				arg, dev.LanguageFlagList())
			os.Exit(2)
		case name == "":
			name = arg
		default:
			fmt.Fprintf(os.Stderr, "Unexpected argument '%s' for `new`.\n", arg)
			os.Exit(2)
		}
	}

	if name == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s new <name> <%s> [-o <dir>]\n",
			filepath.Base(args[0]), dev.LanguageFlagList())
		os.Exit(2)
	}
	// No default language. Defaulting would make `capdag new mycart` produce a
	// different project as the stub set grows, and silently pick for someone
	// who simply forgot to say.
	if language == nil {
		fmt.Fprintf(os.Stderr,
			"`new` requires a language: %s. Each scaffolds the same cartridge, in that language.\n",
			dev.LanguageFlagList())
		os.Exit(2)
	}

	projectDir, err := dev.ScaffoldCartridge(name, language, parent)
	if err != nil {
		die("%v", err)
	}
	fmt.Fprintf(os.Stderr, "Scaffolded %s cartridge '%s' at %s\n", language.Display, name, projectDir)
	fmt.Fprintln(os.Stderr, "Next:")
	fmt.Fprintf(os.Stderr, "  cd %s\n", projectDir)
	for _, step := range language.Build {
		fmt.Fprintf(os.Stderr, "  %s\n", strings.ReplaceAll(step, dev.StubPlaceholder, name))
	}
	fmt.Fprintln(os.Stderr, "  capdag dev-install .          # install under the local `dev` slug")
	fmt.Fprintf(os.Stderr, "  echo \"I love this\" | capdag %s\n", name)
	fmt.Println(projectDir)
}

// ---------------------------------------------------------------------------
// dev-install
// ---------------------------------------------------------------------------

func cmdDevInstall(args []string) {
	projectDir := "."
	if len(args) > 2 {
		projectDir = args[2]
	}

	entry, err := dev.ProjectEntry(projectDir)
	if err != nil {
		die("%v", err)
	}
	manifest, err := dev.ReadEntryManifest(entry)
	if err != nil {
		die("%v", err)
	}

	// A dev cartridge may declare caps the fabric does not know, but its
	// aliases must not collide with the fabric. Check every declared cap up
	// front so a conflict is reported before anything is written to disk.
	registry := registryOrExit()
	resolve := func(alias string) (string, error) { return registry.ResolveAlias(alias) }
	for _, group := range manifest.CapGroups {
		for i := range group.Caps {
			if err := dev.CheckNoFabricConflict(resolve, &group.Caps[i]); err != nil {
				die("%v", err)
			}
		}
	}

	root, err := userCartridgeDir()
	if err != nil {
		die("%v", err)
	}
	versionDir, err := dev.StageDevCartridge(
		projectDir, manifest, root, bifaci.CartridgeRegistryVersion, registry.ManifestVersion())
	if err != nil {
		die("%v", err)
	}

	fmt.Fprintf(os.Stderr, "Installed dev cartridge '%s' v%s (%s) at %s\n",
		manifest.Name, manifest.Version, manifest.Channel, versionDir)
	// Hint the run command using the first non-identity cap alias.
	for _, group := range manifest.CapGroups {
		for i := range group.Caps {
			aliases := group.Caps[i].GetAliases()
			if len(aliases) == 0 || aliases[0] == "identity" {
				continue
			}
			fmt.Fprintf(os.Stderr, "Run it:  echo \"...\" | capdag %s\n", aliases[0])
			fmt.Println(versionDir)
			return
		}
	}
	fmt.Println(versionDir)
}

// ---------------------------------------------------------------------------
// find
// ---------------------------------------------------------------------------

func cmdFind(args []string) {
	if len(args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s find <cap-alias-or-urn>\n", filepath.Base(args[0]))
		os.Exit(2)
	}
	token := args[2]
	registry := registryOrExit()

	capURN := token
	if !strings.Contains(token, ":") {
		resolved, err := registry.ResolveAlias(token)
		if err != nil {
			die("%v", err)
		}
		fmt.Printf("alias  %s -> %s\n", token, resolved)
		capURN = resolved
	}

	definition, err := registry.GetCap(capURN)
	if err != nil {
		die("%v", err)
	}
	fmt.Printf("cap    %s\n", definition.Urn.String())
	fmt.Printf("title  %s\n", definition.Title)
	fmt.Printf("aliases %s\n", strings.Join(definition.Aliases, ", "))
	// A dev-installed cartridge answers caps the fabric does not, and it is the
	// thing a developer is most often looking for here.
	root, err := userCartridgeDir()
	if err == nil {
		for _, alias := range definition.Aliases {
			found, versionDir, ferr := dev.FindDevCapByAlias(root, bifaci.CartridgeRegistryVersion, alias)
			if ferr == nil && found != nil {
				fmt.Printf("dev    %s (alias %s)\n", versionDir, alias)
				break
			}
		}
	}
}

// ---------------------------------------------------------------------------
// resolve
// ---------------------------------------------------------------------------

func cmdResolve(args []string) {
	tokens := args[2:]
	if len(tokens) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: %s resolve <cap-alias-or-urn>...\n", filepath.Base(args[0]))
		os.Exit(2)
	}
	registry := registryOrExit()

	definitions := make([]any, 0, len(tokens))
	for _, token := range tokens {
		definition, err := registry.GetCap(token)
		if err != nil {
			die("%v", err)
		}
		definitions = append(definitions, definition)
	}

	// One argument prints the object; several print the array. Callers pipe
	// this into jq, and wrapping a single result would make every one-cap
	// invocation index into an array for no reason.
	var payload any = definitions[0]
	if len(definitions) > 1 {
		payload = definitions
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		die("serializing the cap definitions: %v", err)
	}
	fmt.Println(string(encoded))
}

// ---------------------------------------------------------------------------
// cache
// ---------------------------------------------------------------------------

func cmdCache(args []string) {
	sub := "status"
	if len(args) > 2 {
		sub = args[2]
	}
	registry := registryOrExit()

	switch sub {
	case "status":
		fmt.Printf("origin   %s\n", registry.Config().RegistryBaseURL)
		fmt.Printf("manifest v%d\n", registry.ManifestVersion())
		fmt.Printf("root     %s\n", registry.CacheDir())
		fmt.Printf("caps     %d\n", len(registry.GetCachedCaps()))
		fmt.Printf("media    %d\n", len(registry.GetCachedMediaDefs()))
		fmt.Printf("aliases  %d\n", len(registry.CachedCapAliases()))
	case "clear":
		if err := registry.ClearCache(); err != nil {
			die("%v", err)
		}
		fmt.Fprintf(os.Stderr, "Cleared %s\n", registry.CacheDir())
	case "refresh":
		// Refresh is clear followed by a re-read: the manifest is re-fetched by
		// the next construction, which is what makes the following lookups warm
		// against the current snapshot rather than the one on disk.
		if err := registry.ClearCache(); err != nil {
			die("%v", err)
		}
		refreshed, err := fabric.New()
		if err != nil {
			die("%v", err)
		}
		fmt.Fprintf(os.Stderr, "Refreshed %s at manifest v%d\n",
			refreshed.CacheDir(), refreshed.ManifestVersion())
	default:
		fmt.Fprintf(os.Stderr, "Usage: %s cache [status|clear|refresh]\n", filepath.Base(args[0]))
		os.Exit(2)
	}
}

// ---------------------------------------------------------------------------
// hash-cartridge-dir
// ---------------------------------------------------------------------------

// cmdHashCartridgeDir prints the deterministic content hash of a cartridge
// version directory.
//
// This is the same walk every host computes at discovery time, so a hash
// printed here is byte-identical to the one a running engine derives. Never
// reimplement the walk elsewhere — it would silently drift.
func cmdHashCartridgeDir(args []string) {
	if len(args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s hash-cartridge-dir <version-dir>\n", filepath.Base(args[0]))
		os.Exit(2)
	}
	hash, err := bifaci.HashCartridgeDirectory(args[2])
	if err != nil {
		die("hash-cartridge-dir: failed to hash '%s': %v", args[2], err)
	}
	fmt.Println(hash)
}
