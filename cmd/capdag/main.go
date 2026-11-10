// capdag — the Go capdag CLI.
//
// A capdag mirror is not just a library: `capdag dev new` is how a cartridge
// project comes into existence, and every mirror must be able to create the
// same project. This binary is the Go mirror's CLI.
//
// # What this binary does and does not do
//
// The commands here are exactly those the Go library can back today, named and
// shaped as the reference capdag CLI names them:
//
//	info <cap>...                     a capability: title, aliases, URN; --json, its definition
//	cache refresh|clear               renew or remove the local copy of the fabric
//	dev new <path> --lang <language>  start a cartridge project in any vendored language
//	dev install [<dir>]               install a cartridge project for local runs
//	dev hash-dir <dir>                the deterministic content hash of a version directory
//
// `run`, `plan`, `caps`, `check` and `graph` are NOT here, because this mirror
// has no plan executor and no path-planner engine. They are absent rather than
// stubbed: a command that accepted the arguments and then reported
// "unsupported" would be a worse lie than not existing.
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

// cliError is what went wrong, said as the reference CLI says it: the
// problem, what was expected and found, and what to type next.
type cliError struct {
	problem string
	context []string
	hint    string
	exit    int
}

func (e *cliError) Error() string { return e.problem }

func (e *cliError) render() string {
	out := "error: " + e.problem + "\n"
	for _, line := range e.context {
		out += "  " + line + "\n"
	}
	if e.hint != "" {
		out += "hint: " + e.hint + "\n"
	}
	return out
}

func failed(problem string, context ...string) *cliError {
	return &cliError{problem: problem, context: context, exit: 1}
}

func usage(problem, hint string, context ...string) *cliError {
	return &cliError{problem: problem, context: context, hint: hint, exit: 2}
}

// globals are the flags every command takes.
type globals struct {
	json bool
}

func main() {
	args, g, err := takeGlobals(os.Args[1:])
	if err == nil {
		err = dispatch(args, g)
	}
	if err == nil {
		return
	}
	e, ok := err.(*cliError)
	if !ok {
		e = failed(err.Error())
	}
	if g.json {
		record, _ := json.Marshal(map[string]any{
			"type": "error", "problem": e.problem, "context": e.context, "hint": e.hint, "exit": e.exit,
		})
		fmt.Println(string(record))
	}
	fmt.Fprint(os.Stderr, e.render())
	os.Exit(e.exit)
}

// takeGlobals takes `--fabric <url>` and `--json` from anywhere in argv, as
// the reference CLI's global flags are.
func takeGlobals(args []string) ([]string, globals, error) {
	var g globals
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--json":
			g.json = true
		case args[i] == "--fabric":
			if i+1 >= len(args) {
				return nil, g, usage("--fabric needs a URL", "for example: --fabric https://fabric-staging.capdag.com")
			}
			url := args[i+1]
			if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
				return nil, g, usage(fmt.Sprintf("--fabric '%s' is not an http(s) URL", url), "for example: --fabric https://fabric-staging.capdag.com")
			}
			os.Setenv("CDG_FABRIC_REGISTRY_URL", url)
			i++
		default:
			out = append(out, args[i])
		}
	}
	return out, g, nil
}

const help = `The Go capdag CLI: capability lookup, the fabric cache and cartridge development.

Usage: capdag [--fabric <url>] [--json] <command>

Commands:
  info <cap>... [--no-cache]          a capability: title, aliases, URN
  cache refresh|clear                 renew or remove the local copy of the fabric
  dev new <path> --lang <language>    start a cartridge project
  dev install [<dir>]                 install a cartridge project for local runs

Examples:
  capdag info disbind-pdf
  capdag dev new sentiment --lang go

This mirror has no plan executor: run, plan, caps, check and graph are the
reference capdag's.
`

func dispatch(args []string, g globals) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, help)
		return usage("no command was given", "capdag --help lists them")
	}
	switch args[0] {
	case "help", "--help", "-h":
		fmt.Print(help)
		return nil
	case "info":
		return cmdInfo(args[1:], g)
	case "cache":
		if len(args) != 2 || (args[1] != "refresh" && args[1] != "clear") {
			return usage("cache takes refresh or clear", "capdag cache refresh")
		}
		return cmdCache(args[1], g)
	case "dev":
		if len(args) < 2 {
			return usage("dev takes new, install or hash-dir", "capdag dev new <path> --lang <language>")
		}
		switch args[1] {
		case "new":
			return cmdDevNew(args[2:], g)
		case "install":
			return cmdDevInstall(args[2:], g)
		case "hash-dir":
			return cmdHashDir(args[2:])
		}
		return usage(fmt.Sprintf("there is no command `capdag dev %s`", args[1]), "dev takes new, install or hash-dir")
	}
	return usage(fmt.Sprintf("there is no command `%s` in this mirror", args[0]),
		"run, plan, caps, check and graph are the reference capdag's (the Rust build)",
		"this binary covers: info, cache, dev")
}

// userCartridgeDir is the per-user cartridge install root, in the same
// {registry_slug}/{channel}/{name}/{version}/ tree every host uses.
func userCartridgeDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", failed("there is no home directory to keep cartridges in", err.Error())
	}
	return filepath.Join(home, ".capdag", "cartridges"), nil
}

func registry() (*fabric.FabricRegistry, error) {
	r, err := fabric.New()
	if err != nil {
		return nil, failed("the fabric could not be opened", err.Error())
	}
	return r, nil
}

func emit(record map[string]any) {
	encoded, _ := json.Marshal(record)
	fmt.Println(string(encoded))
}

func cmdDevNew(args []string, g globals) error {
	var path, lang string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--lang":
			if i+1 >= len(args) {
				return usage("--lang needs a language", "languages: "+languageIDs())
			}
			lang = args[i+1]
			i++
		case strings.HasPrefix(args[i], "-"):
			return usage(fmt.Sprintf("dev new has no option '%s'", args[i]), "capdag dev new <path> --lang <language>")
		case path == "":
			path = args[i]
		default:
			return usage(fmt.Sprintf("dev new takes one path, and was also given '%s'", args[i]), "capdag dev new <path> --lang <language>")
		}
	}
	if path == "" {
		return usage("dev new needs the path of the project to make", "capdag dev new <path> --lang <language>")
	}
	// No default language: a default would make one command mean different
	// projects as the stub set grows.
	language := dev.Language(lang)
	if lang == "" || language == nil {
		return usage(fmt.Sprintf("'%s' is not a language capdag scaffolds", lang), "languages: "+languageIDs())
	}
	name := filepath.Base(path)
	parent := filepath.Dir(path)
	projectDir, err := dev.ScaffoldCartridge(name, language, parent)
	if err != nil {
		return failed(fmt.Sprintf("the project %s could not be made", path), err.Error())
	}
	if g.json {
		emit(map[string]any{"type": "project", "dir": projectDir, "language": language.ID})
		return nil
	}
	fmt.Fprintf(os.Stderr, "Made the %s cartridge '%s' at %s\n", language.Display, name, projectDir)
	fmt.Fprintln(os.Stderr, "Next:")
	fmt.Fprintf(os.Stderr, "  cd %s\n", projectDir)
	for _, step := range language.Build {
		fmt.Fprintf(os.Stderr, "  %s\n", strings.ReplaceAll(step, dev.StubPlaceholder, name))
	}
	fmt.Fprintln(os.Stderr, "  capdag dev install")
	fmt.Fprintf(os.Stderr, "  echo \"I love this\" | capdag run %s\n", name)
	fmt.Println(projectDir)
	return nil
}

func languageIDs() string {
	ids := make([]string, 0)
	for _, l := range dev.Languages() {
		ids = append(ids, l.ID)
	}
	return strings.Join(ids, ", ")
}

func cmdDevInstall(args []string, g globals) error {
	projectDir := "."
	if len(args) > 1 {
		return usage("dev install takes one directory", "capdag dev install [<dir>]")
	}
	if len(args) == 1 {
		projectDir = args[0]
	}
	entry, err := dev.ProjectEntry(projectDir)
	if err != nil {
		return &cliError{problem: fmt.Sprintf("%s is not a cartridge project that is built", projectDir),
			context: []string{err.Error()}, hint: "build it first, as `capdag dev new` said; then run this again", exit: 1}
	}
	manifest, err := dev.ReadEntryManifest(entry)
	if err != nil {
		return failed("the cartridge's manifest could not be read", err.Error())
	}
	// A dev cartridge may declare caps the fabric does not know, but none of
	// its aliases may mean another cap upstream.
	r, err := registry()
	if err != nil {
		return err
	}
	resolve := func(alias string) (string, error) { return r.ResolveAlias(alias) }
	for _, group := range manifest.CapGroups {
		for i := range group.Caps {
			if err := dev.CheckNoFabricConflict(resolve, &group.Caps[i]); err != nil {
				return failed("the cartridge's caps conflict with the fabric", err.Error())
			}
		}
	}
	root, err := userCartridgeDir()
	if err != nil {
		return err
	}
	versionDir, err := dev.StageDevCartridge(projectDir, manifest, root, bifaci.CartridgeRegistryVersion, r.ManifestVersion())
	if err != nil {
		return failed("the cartridge could not be installed", err.Error())
	}
	if g.json {
		emit(map[string]any{"type": "cartridge", "id": manifest.Name, "version": manifest.Version, "state": "dev", "dir": versionDir})
		return nil
	}
	fmt.Fprintf(os.Stderr, "Installed the dev cartridge '%s' %s at %s\n", manifest.Name, manifest.Version, versionDir)
	for _, group := range manifest.CapGroups {
		for i := range group.Caps {
			aliases := group.Caps[i].GetAliases()
			if len(aliases) > 0 && aliases[0] != "identity" {
				fmt.Fprintf(os.Stderr, "Run it: capdag run %s <input>\n", aliases[0])
				fmt.Println(versionDir)
				return nil
			}
		}
	}
	fmt.Println(versionDir)
	return nil
}

func cmdInfo(args []string, g globals) error {
	noCache := false
	names := make([]string, 0, len(args))
	for _, a := range args {
		switch {
		case a == "--no-cache":
			noCache = true
		case strings.HasPrefix(a, "-"):
			return usage(fmt.Sprintf("info has no option '%s'", a), "capdag info <cap>... [--no-cache]")
		default:
			names = append(names, a)
		}
	}
	if len(names) == 0 {
		return usage("info needs the capabilities to show", "capdag info disbind-pdf")
	}
	r, err := registry()
	if err != nil {
		return err
	}
	if noCache {
		if err := r.ClearCache(); err != nil {
			return failed("the fabric cache could not be cleared", err.Error())
		}
		if r, err = registry(); err != nil {
			return err
		}
	}
	for index, name := range names {
		definition, err := r.GetCap(name)
		if err != nil {
			return usage(fmt.Sprintf("there is no capability named '%s'", name),
				"find one with the reference CLI: capdag caps "+name, err.Error())
		}
		alias := name
		if len(definition.Aliases) > 0 {
			alias = definition.Aliases[0]
		}
		if g.json {
			// The definition as the fabric publishes it, as the reference CLI
			// writes it.
			raw, err := json.Marshal(definition)
			if err != nil {
				return failed(fmt.Sprintf("%s could not be written as JSON", alias), err.Error())
			}
			head, _ := json.Marshal(map[string]any{"type": "cap", "alias": alias, "urn": definition.Urn.String()})
			fmt.Printf("%s,\"bundled\":[],\"installed\":[],\"definition\":%s}\n", strings.TrimSuffix(string(head), "}"), raw)
			continue
		}
		if index > 0 {
			fmt.Println()
		}
		fmt.Printf("%s — %s\n", alias, definition.Title)
		fmt.Println(definition.Urn.String())
		fmt.Printf("Aliases: %s\n", strings.Join(definition.Aliases, ", "))
		if root, err := userCartridgeDir(); err == nil {
			for _, a := range definition.Aliases {
				found, versionDir, ferr := dev.FindDevCapByAlias(root, bifaci.CartridgeRegistryVersion, a)
				if ferr == nil && found != nil {
					fmt.Printf("Provided by the dev cartridge at %s\n", versionDir)
					break
				}
			}
		}
	}
	return nil
}

func cmdCache(action string, g globals) error {
	r, err := registry()
	if err != nil {
		return err
	}
	dir := r.CacheDir()
	if err := r.ClearCache(); err != nil {
		return failed(fmt.Sprintf("the fabric cache at %s could not be cleared", dir), err.Error())
	}
	var manifest any
	if action == "refresh" {
		// The manifest and everything it pins, fetched afresh into the now
		// empty cache, so the renewal is complete rather than lazy.
		refreshed, err := registry()
		if err != nil {
			return err
		}
		manifest = refreshed.ManifestVersion()
	}
	if g.json {
		emit(map[string]any{"type": "cache", "dir": dir, "refreshed": manifest != nil, "manifest": manifest})
	} else if manifest != nil {
		fmt.Fprintf(os.Stderr, "Fabric cache refreshed at %s: manifest %v\n", dir, manifest)
	} else {
		fmt.Fprintf(os.Stderr, "Fabric cache cleared at %s; the next command fetches what it needs.\n", dir)
	}
	return nil
}

// cmdHashDir prints the deterministic content hash of a cartridge version
// directory: the walk every host computes at discovery time.
func cmdHashDir(args []string) error {
	if len(args) != 1 {
		return usage("dev hash-dir takes one directory", "capdag dev hash-dir <version-dir>")
	}
	hash, err := bifaci.HashCartridgeDirectory(args[0])
	if err != nil {
		return failed(fmt.Sprintf("%s could not be hashed", args[0]), err.Error())
	}
	fmt.Println(hash)
	return nil
}
