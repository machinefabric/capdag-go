// Package dev provides cartridge-development support for the capdag CLI.
//
// It backs three developer commands and the local-manifest run path:
//
//   - ScaffoldCartridge — `capdag new <name> --<language>`: write a fresh,
//     runnable cartridge project (one custom cap, one Op that peer-calls a
//     model, one manifest) into a new directory, in any language the vendored
//     canonical stubs cover. The stubs are the SAME bytes in every capdag
//     implementation (see stubs_generated.go), so the project you get does not
//     depend on which capdag binary you ran.
//   - StageDevCartridge — `capdag dev-install <project-dir>`: read the project's
//     manifest, then copy it under the per-user cartridge root's reserved `dev`
//     slug so the capdag host discovers it. Re-running overwrites the same
//     version directory — the update step of the edit/reinstall loop.
//   - FindDevCapByAlias + CheckNoFabricConflict — the local-manifest run path:
//     when `capdag <alias>` names a cap the fabric does NOT define, a locally
//     dev-installed cartridge's OWN manifest answers it, as long as the cap does
//     not conflict with the fabric. A dev cap never needs to be published to be
//     developed and run locally.
//
// The on-disk layout mirrors every other host exactly:
// {user_cartridge_dir}/dev/v{CartridgeRegistryVersion}/{channel}/{name}/{version}/
//
// Mirrors the reference implementation in capdag/src/dev.rs.
package dev

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/machinefabric/capdag-go/bifaci"
	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/urn"
)

// ---------------------------------------------------------------------------
// Errors — each names the file, entry or conflicting alias so a developer can
// act on it without reproducing the failure.
// ---------------------------------------------------------------------------

// InvalidNameError is a project name that is not path-safe.
type InvalidNameError struct{ Name string }

func (e *InvalidNameError) Error() string {
	return fmt.Sprintf(
		"invalid cartridge name %q: use a lowercase, path-safe name matching [a-z0-9] "+
			"with '-' or '_' separators (e.g. sentiment-tagger)", e.Name)
}

// AlreadyExistsError is a scaffold target that already exists. Scaffolding never
// overwrites existing work.
type AlreadyExistsError struct{ Path string }

func (e *AlreadyExistsError) Error() string {
	return fmt.Sprintf("%q already exists — pick a new name or remove it first", e.Path)
}

// NoEntryError is a project with no cartridge entry for any known language.
type NoEntryError struct{ Project string }

func (e *NoEntryError) Error() string {
	return fmt.Sprintf(
		"no cartridge entry found in %q. Looked for %s. A compiled cartridge must be "+
			"BUILT before it is installed — the host launches the binary, not the sources. "+
			"Create the project with `capdag new`.",
		e.Project, entryCandidatesDescription(e.Project))
}

// AmbiguousEntryError is a project carrying more than one language's entry.
type AmbiguousEntryError struct {
	Project string
	Found   []string
}

func (e *AmbiguousEntryError) Error() string {
	return fmt.Sprintf(
		"%q contains more than one cartridge entry (%s) — capdag cannot tell which one to "+
			"install. A project is ONE cartridge; remove the build outputs of the language "+
			"you are not developing.",
		e.Project, strings.Join(e.Found, ", "))
}

// NotDevError is a manifest that declares a registry URL, i.e. a published
// cartridge being dev-installed.
type NotDevError struct{ RegistryURL string }

func (e *NotDevError) Error() string {
	return fmt.Sprintf(
		"this project's manifest declares registry_url %q, so it is a PUBLISHED cartridge, "+
			"not a dev one. `dev-install` stages only dev cartridges (registry_url null).",
		e.RegistryURL)
}

// FabricConflictError is a dev cap whose alias already means a different cap
// upstream. Installing it would shadow the published one.
type FabricConflictError struct {
	Alias     string
	DevURN    string
	FabricURN string
}

func (e *FabricConflictError) Error() string {
	return fmt.Sprintf(
		"the dev cap %q claims alias %q, which the fabric already resolves to %q. "+
			"Rename the dev cap's alias — a dev cartridge may not shadow a published cap.",
		e.DevURN, e.Alias, e.FabricURN)
}

// ---------------------------------------------------------------------------
// The vendored stub contract.
// ---------------------------------------------------------------------------

// Languages returns every language `capdag new` can scaffold, in contract order.
//
// A mirror that offered a subset would silently make `capdag new --rust` mean
// different things depending on which capdag binary you happened to run.
func Languages() []StubLanguage { return StubLanguages }

// Language looks a language up by its id ("python") or its flag ("--python"),
// returning nil for anything else. The caller turns nil into an error that lists
// what IS available, which is the only useful thing to say.
func Language(selector string) *StubLanguage {
	for i := range StubLanguages {
		if StubLanguages[i].ID == selector || StubLanguages[i].Flag == selector {
			return &StubLanguages[i]
		}
	}
	return nil
}

// LanguageFlagList renders the scaffoldable flags for usage and error messages,
// built from the contract so a newly vendored language appears everywhere at
// once rather than in whichever message someone remembered to update.
func LanguageFlagList() string {
	flags := make([]string, 0, len(StubLanguages))
	for i := range StubLanguages {
		flags = append(flags, StubLanguages[i].Flag)
	}
	return strings.Join(flags, " | ")
}

// render substitutes the project name into a stub's text.
//
// The placeholder appears in file CONTENTS, in destination PATHS, and in the
// entry — a compiled cartridge's binary is named after the project — so one
// function serves all three rather than three call sites each remembering.
func render(template, name string) string {
	return strings.ReplaceAll(template, StubPlaceholder, name)
}

// Entry is the executable the host launches for a scaffolded project, relative
// to the project directory.
func Entry(language *StubLanguage, name string) string {
	return render(language.Entry, name)
}

// ValidCartridgeName reports whether a name is safe as a directory, a cap alias
// and a media-URN fragment all at once.
func ValidCartridgeName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		isLower := r >= 'a' && r <= 'z'
		isDigit := r >= '0' && r <= '9'
		if i == 0 {
			if !isLower && !isDigit {
				return false
			}
			continue
		}
		if !isLower && !isDigit && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// new — scaffold a project.
// ---------------------------------------------------------------------------

// ScaffoldCartridge writes a new cartridge project named `name` under
// `parentDir`, in `language`, and returns the created project directory.
//
// Fails hard if the name is not path-safe or the target already exists — never
// overwrites existing work, and never half-writes: a failure part-way names the
// exact file it could not write.
func ScaffoldCartridge(name string, language *StubLanguage, parentDir string) (string, error) {
	if !ValidCartridgeName(name) {
		return "", &InvalidNameError{Name: name}
	}
	projectDir := filepath.Join(parentDir, name)
	if _, err := os.Stat(projectDir); err == nil {
		return "", &AlreadyExistsError{Path: projectDir}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("checking %q: %w", projectDir, err)
	}
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		return "", fmt.Errorf("creating project dir %q: %w", projectDir, err)
	}

	for _, file := range language.Files {
		dest := filepath.Join(projectDir, render(file.Dest, name))
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return "", fmt.Errorf("creating %q: %w", filepath.Dir(dest), err)
		}
		mode := os.FileMode(0o644)
		if file.Executable {
			mode = 0o755
		}
		if err := os.WriteFile(dest, []byte(render(file.Contents, name)), mode); err != nil {
			return "", fmt.Errorf("writing %q: %w", dest, err)
		}
	}
	return projectDir, nil
}

// ---------------------------------------------------------------------------
// Entry discovery.
// ---------------------------------------------------------------------------

// projectName is the name a scaffolded directory carries: its own directory
// name. `capdag new <name>` creates <parent>/<name> and every rendered path is
// seeded from that name, so the directory IS the name. Reading it back is how
// dev-install knows what a compiled entry is called without being told.
func projectName(projectDir string) string {
	return filepath.Base(filepath.Clean(projectDir))
}

// entryCandidatesDescription names every entry path that WOULD have been
// accepted, turning "no entry found" into an instruction.
func entryCandidatesDescription(projectDir string) string {
	name := projectName(projectDir)
	parts := make([]string, 0, len(StubLanguages))
	for i := range StubLanguages {
		parts = append(parts, fmt.Sprintf("%s (%s)", Entry(&StubLanguages[i], name), StubLanguages[i].Display))
	}
	return strings.Join(parts, ", ")
}

// ProjectEntry finds the project's entry across every scaffoldable language and
// verifies it exists.
//
// A project is ONE cartridge, so finding two entries is an error rather than a
// silent pick: installing whichever language happened to sort first would be a
// coin flip the developer never sees.
func ProjectEntry(projectDir string) (string, error) {
	name := projectName(projectDir)
	var found []string
	for i := range StubLanguages {
		candidate := filepath.Join(projectDir, Entry(&StubLanguages[i], name))
		if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
			found = append(found, candidate)
		}
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return "", &NoEntryError{Project: projectDir}
	default:
		return "", &AmbiguousEntryError{Project: projectDir, Found: found}
	}
}

// ---------------------------------------------------------------------------
// Reading a project's manifest.
// ---------------------------------------------------------------------------

// ReadEntryManifest runs a cartridge entry's `manifest` subcommand and parses
// the printed CapManifest JSON.
//
// Every cartridge in every language prints the same wire shape, which is what
// lets capdag read a Python project's manifest from Go without knowing or
// caring which language wrote it.
func ReadEntryManifest(entry string) (*bifaci.CapManifest, error) {
	out, err := exec.Command(entry, "manifest").Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf(
				"the cartridge entry %q exited %d when asked for its manifest: %s",
				entry, exitErr.ExitCode(), strings.TrimSpace(string(exitErr.Stderr)))
		}
		return nil, fmt.Errorf(
			"could not run the cartridge entry %q to read its manifest: %w. Make sure it is "+
				"executable and its dependencies are importable.", entry, err)
	}
	var manifest bifaci.CapManifest
	if err := json.Unmarshal(out, &manifest); err != nil {
		return nil, fmt.Errorf("the cartridge entry %q printed a manifest capdag cannot parse: %w", entry, err)
	}
	return &manifest, nil
}

// ---------------------------------------------------------------------------
// dev-install — stage a project under the `dev` slug.
// ---------------------------------------------------------------------------

// DevVersionDir is the install directory for a dev cartridge under
// userCartridgeDir: dev/v{registryVersion}/{channel}/{name}/{version}/
func DevVersionDir(userCartridgeDir string, registryVersion uint32, channel, name, version string) string {
	return filepath.Join(
		userCartridgeDir,
		bifaci.DevSlug,
		fmt.Sprintf("v%d", registryVersion),
		channel,
		name,
		version,
	)
}

// ignoredProjectEntry reports whether a project tree entry is skipped by the
// install copy.
func ignoredProjectEntry(name string) bool {
	switch name {
	// Developer scratch.
	case ".venv", "__pycache__", ".git", ".pytest_cache", "cartridge.json":
		return true
	// Build trees. A compiled cartridge's intermediates are gigabytes of object
	// files and dependency sources the host never reads — only the linked entry
	// matters, and StageDevCartridge copies that explicitly after this walk.
	case "target", ".build", ".swiftpm", "node_modules":
		return true
	}
	return strings.HasSuffix(name, ".pyc")
}

// copyProjectTree recursively copies a project into dst, skipping scratch and
// build trees.
func copyProjectTree(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("reading project dir %q: %w", src, err)
	}
	for _, entry := range entries {
		if ignoredProjectEntry(entry.Name()) {
			continue
		}
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			if err := os.MkdirAll(dstPath, 0o755); err != nil {
				return fmt.Errorf("creating %q: %w", dstPath, err)
			}
			if err := copyProjectTree(srcPath, dstPath); err != nil {
				return err
			}
			continue
		}
		if err := copyFilePreservingMode(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func copyFilePreservingMode(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat %q: %w", src, err)
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("reading %q: %w", src, err)
	}
	if err := os.WriteFile(dst, data, info.Mode().Perm()); err != nil {
		return fmt.Errorf("writing %q: %w", dst, err)
	}
	return nil
}

// StageDevCartridge copies a project under the per-user cartridge root's `dev`
// slug and writes its cartridge.json, returning the version directory.
//
// `manifest` must already have been read from the project (via
// ReadEntryManifest) and is verified here to be a dev cartridge (registry_url
// null); this staging step does not itself re-run the entry.
func StageDevCartridge(
	projectDir string,
	manifest *bifaci.CapManifest,
	userCartridgeDir string,
	registryVersion uint32,
	fabricManifestVersion uint32,
) (string, error) {
	if manifest.RegistryURL != nil {
		return "", &NotDevError{RegistryURL: *manifest.RegistryURL}
	}

	versionDir := DevVersionDir(
		userCartridgeDir, registryVersion, manifest.Channel, manifest.Name, manifest.Version)

	// The entry is discovered in the PROJECT, then recorded relative to the
	// install — a compiled cartridge's entry lives under its build directory
	// (target/release/<name>), and the two are the same relative path.
	entryPath, err := ProjectEntry(projectDir)
	if err != nil {
		return "", err
	}
	relativeEntry, err := filepath.Rel(projectDir, entryPath)
	if err != nil {
		return "", fmt.Errorf("locating the entry inside the project: %w", err)
	}

	// Update semantics: replace the version directory wholesale so a removed
	// file in the project does not linger in a stale install.
	if err := os.RemoveAll(versionDir); err != nil {
		return "", fmt.Errorf("clearing old install %q: %w", versionDir, err)
	}
	if err := os.MkdirAll(versionDir, 0o755); err != nil {
		return "", fmt.Errorf("creating %q: %w", versionDir, err)
	}

	if err := copyProjectTree(projectDir, versionDir); err != nil {
		return "", err
	}

	// The entry is copied explicitly because a compiled one lives INSIDE a build
	// tree the walk above deliberately skips. Doing it here rather than
	// exempting the whole tree keeps the install to the sources plus the one
	// binary the host actually launches.
	installedEntry := filepath.Join(versionDir, relativeEntry)
	if info, err := os.Stat(installedEntry); err != nil || !info.Mode().IsRegular() {
		if err := os.MkdirAll(filepath.Dir(installedEntry), 0o755); err != nil {
			return "", fmt.Errorf("creating %q: %w", filepath.Dir(installedEntry), err)
		}
		if err := copyFilePreservingMode(entryPath, installedEntry); err != nil {
			return "", fmt.Errorf("copying the cartridge entry into the install: %w", err)
		}
	}
	if err := os.Chmod(installedEntry, 0o755); err != nil {
		return "", fmt.Errorf("making %q executable: %w", installedEntry, err)
	}

	source := bifaci.CartridgeInstallSourceDev
	cj := bifaci.CartridgeJson{
		Name:                  manifest.Name,
		Version:               manifest.Version,
		Channel:               manifest.Channel,
		RegistryURL:           nil,
		Entry:                 relativeEntry,
		InstalledAt:           bifaci.InstallTimestampNow(),
		InstalledFrom:         &source,
		FabricManifestVersion: fabricManifestVersion,
	}
	if err := cj.WriteToDir(versionDir); err != nil {
		return "", fmt.Errorf("writing cartridge.json: %w", err)
	}
	return versionDir, nil
}

// ---------------------------------------------------------------------------
// The local-manifest run path.
// ---------------------------------------------------------------------------

// FindDevCapByAlias searches every dev-installed cartridge's own manifest for a
// cap carrying `alias`, returning the cap and its version directory.
//
// Returns (nil, "", nil) when no dev cartridge claims the alias — an ordinary
// outcome, not an error: the caller then reports the alias as unknown to both
// the fabric and the dev slug.
func FindDevCapByAlias(userCartridgeDir string, registryVersion uint32, alias string) (*cap.Cap, string, error) {
	dirs, err := walkVersionDirs(filepath.Join(userCartridgeDir, bifaci.DevSlug, fmt.Sprintf("v%d", registryVersion)))
	if err != nil {
		return nil, "", err
	}
	for _, dir := range dirs {
		// A version directory with no cartridge.json is not an install — it is
		// a leftover directory, and skipping it is not a fallback: the reader
		// distinguishes "absent" from "unreadable", and only the latter is an
		// error worth stopping the whole lookup for.
		if _, err := os.Stat(filepath.Join(dir, "cartridge.json")); os.IsNotExist(err) {
			continue
		}
		cj, err := bifaci.ReadCartridgeJsonFromDir(dir, bifaci.DevSlug)
		if err != nil {
			return nil, "", fmt.Errorf("the dev install at %q has an unreadable cartridge.json: %w", dir, err)
		}
		manifest, err := ReadEntryManifest(cj.ResolveEntryPoint(dir))
		if err != nil {
			return nil, "", err
		}
		for _, group := range manifest.CapGroups {
			for i := range group.Caps {
				for _, a := range group.Caps[i].GetAliases() {
					if a == alias {
						found := group.Caps[i]
						return &found, dir, nil
					}
				}
			}
		}
	}
	return nil, "", nil
}

// walkVersionDirs lists every {channel}/{name}/{version}/ directory under a dev
// root. A missing root is not an error — nothing has been dev-installed yet.
func walkVersionDirs(devRoot string) ([]string, error) {
	channels, err := readSubdirs(devRoot)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, channel := range channels {
		names, err := readSubdirs(channel)
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			versions, err := readSubdirs(name)
			if err != nil {
				return nil, err
			}
			out = append(out, versions...)
		}
	}
	sort.Strings(out)
	return out, nil
}

func readSubdirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading %q: %w", dir, err)
	}
	var out []string
	for _, entry := range entries {
		if entry.IsDir() {
			out = append(out, filepath.Join(dir, entry.Name()))
		}
	}
	return out, nil
}

// CheckNoFabricConflict refuses a dev cap whose alias already means a DIFFERENT
// cap in the fabric.
//
// A dev cap providing the same fabric cap (e.g. identity) is not a conflict —
// the comparison is on canonical URNs, not on the alias alone.
func CheckNoFabricConflict(resolveAlias func(string) (string, error), c *cap.Cap) error {
	devURN := c.Urn.String()
	for _, alias := range c.GetAliases() {
		target, err := resolveAlias(alias)
		if err != nil {
			// The fabric does not define this alias — nothing to conflict with.
			continue
		}
		fabricURN := target
		if parsed, err := urn.NewCapUrnFromString(target); err == nil {
			fabricURN = parsed.String()
		}
		if fabricURN != devURN {
			return &FabricConflictError{Alias: alias, DevURN: devURN, FabricURN: fabricURN}
		}
	}
	return nil
}
