package dev

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/machinefabric/capdag-go/bifaci"
	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/urn"
)

func tempRoot(t *testing.T, tag string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "capdag-dev-"+tag+"-")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// TEST7154: EVERY vendored language scaffolds a runnable-shaped project — every
// declared file exists, no placeholder survives anywhere (contents or paths),
// the manifest/alias/media URNs are seeded from the project name, and the
// interpreted languages' entries are executable.
//
// Iterating the contract rather than testing one language is the point: a newly
// vendored language is covered the moment it appears, instead of whenever
// someone remembers to add a test for it.
func Test7154_scaffold_writes_a_runnable_project_in_every_language(t *testing.T) {
	root := tempRoot(t, "scaffold")
	if len(Languages()) == 0 {
		t.Fatal("the vendored contract must declare at least one language")
	}

	for i := range StubLanguages {
		language := &StubLanguages[i]
		name := "mood-tagger-" + language.ID
		proj, err := ScaffoldCartridge(name, language, root)
		if err != nil {
			t.Fatalf("%s: scaffold failed: %v", language.ID, err)
		}
		if proj != filepath.Join(root, name) {
			t.Fatalf("%s: scaffolded at %q, expected %q", language.ID, proj, filepath.Join(root, name))
		}

		var sources strings.Builder
		for _, file := range language.Files {
			dest := filepath.Join(proj, render(file.Dest, name))
			info, err := os.Stat(dest)
			if err != nil {
				t.Fatalf("%s: declared file %q was not written: %v", language.ID, dest, err)
			}
			body, err := os.ReadFile(dest)
			if err != nil {
				t.Fatalf("%s: reading %q: %v", language.ID, dest, err)
			}
			if strings.Contains(string(body), StubPlaceholder) {
				t.Fatalf("%s: %q still contains the placeholder", language.ID, dest)
			}
			sources.Write(body)

			if file.Executable && info.Mode().Perm()&0o111 == 0 {
				t.Fatalf("%s: %q is declared executable but is not", language.ID, dest)
			}
		}

		// The rendered entry path must itself be free of the placeholder — a
		// compiled cartridge's binary is named after the project.
		if strings.Contains(Entry(language, name), StubPlaceholder) {
			t.Fatalf("%s: the entry path was not rendered", language.ID)
		}

		// The project name reaches the cap it declares, in every language.
		want := fmt.Sprintf("media:enc=utf-8;%s-input", name)
		if !strings.Contains(sources.String(), want) {
			t.Fatalf("%s: input media URN is not seeded from the project name", language.ID)
		}
		if strings.Contains(sources.String(), "command=") {
			t.Fatalf("%s: carries the removed `command=` field", language.ID)
		}
	}
}

// TEST7155: scaffolding rejects a bad name and refuses to overwrite.
func Test7155_scaffold_guards(t *testing.T) {
	root := tempRoot(t, "guards")
	language := &StubLanguages[0]

	if _, err := ScaffoldCartridge("Bad Name", language, root); err == nil {
		t.Fatal("a non-path-safe name must be rejected")
	} else if _, ok := err.(*InvalidNameError); !ok {
		t.Fatalf("expected InvalidNameError, got %T: %v", err, err)
	}

	if _, err := ScaffoldCartridge("greeter", language, root); err != nil {
		t.Fatalf("first scaffold failed: %v", err)
	}
	if _, err := ScaffoldCartridge("greeter", language, root); err == nil {
		t.Fatal("scaffolding over an existing project must be refused")
	} else if _, ok := err.(*AlreadyExistsError); !ok {
		t.Fatalf("expected AlreadyExistsError, got %T: %v", err, err)
	}
}

// writeStubEntry writes a cartridge entry (a bash script) that prints a canned
// CapManifest on `manifest`, exercising the capdag-side staging/parsing/
// resolution without any language runtime.
//
// It is written at the PYTHON entry because that is the one language whose entry
// is a source file with no build step, so a bash script standing in for it is
// discovered by exactly the same path a real project would be.
func writeStubEntry(t *testing.T, dir, name, alias, capURN string) string {
	t.Helper()
	python := Language("python")
	if python == nil {
		t.Fatal("the contract must cover python")
	}
	urnJSON := strings.ReplaceAll(capURN, `"`, `\"`)
	manifest := fmt.Sprintf(
		`{"name":"%s","version":"0.1.0","channel":"nightly","registry_url":null,`+
			`"description":"stub","cap_groups":[{"name":"default","caps":[`+
			`{"urn":"cap:effect=none","title":"Identity","aliases":["identity"]},`+
			`{"urn":"%s","title":"%s","aliases":["%s"]}]}]}`,
		name, urnJSON, name, alias)
	script := "#!/usr/bin/env bash\nif [ \"$1\" = manifest ]; then\n  cat <<'EOF'\n" + manifest + "\nEOF\nfi\n"

	path := filepath.Join(dir, Entry(python, filepath.Base(dir)))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("creating entry dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("writing stub entry: %v", err)
	}
	return path
}

// TEST7156: read_entry_manifest + stage_dev_cartridge + find_dev_cap_by_alias
// round-trip: a stub project installs under dev/v{N}/nightly/<name>/<ver>/,
// writes a cartridge.json, and its custom cap is resolvable by alias.
func Test7156_dev_install_and_find_by_alias(t *testing.T) {
	root := tempRoot(t, "install")
	project := filepath.Join(root, "proj")
	if err := os.MkdirAll(project, 0o755); err != nil {
		t.Fatalf("creating project: %v", err)
	}
	capURN := `cap:greet;in="media:enc=utf-8";out="media:enc=utf-8;greeting"`
	writeStubEntry(t, project, "greeter", "greet", capURN)

	userDir := filepath.Join(root, "cartridges")
	entry, err := ProjectEntry(project)
	if err != nil {
		t.Fatalf("entry discovery: %v", err)
	}
	manifest, err := ReadEntryManifest(entry)
	if err != nil {
		t.Fatalf("reading manifest: %v", err)
	}
	if manifest.Name != "greeter" {
		t.Fatalf("manifest name %q, expected greeter", manifest.Name)
	}
	if manifest.RegistryURL != nil {
		t.Fatal("a scaffolded manifest must be a dev cartridge (registry_url null)")
	}

	versionDir, err := StageDevCartridge(project, manifest, userDir, 1, 7)
	if err != nil {
		t.Fatalf("staging: %v", err)
	}
	wantSuffix := filepath.Join("dev", "v1", "nightly", "greeter", "0.1.0")
	if !strings.HasSuffix(versionDir, wantSuffix) {
		t.Fatalf("installed at %q, expected it to end with %q", versionDir, wantSuffix)
	}
	if _, err := os.Stat(filepath.Join(versionDir, "cartridge.json")); err != nil {
		t.Fatalf("cartridge.json not written: %v", err)
	}
	python := Language("python")
	if _, err := os.Stat(filepath.Join(versionDir, Entry(python, "proj"))); err != nil {
		t.Fatalf("entry not installed: %v", err)
	}

	found, dir, err := FindDevCapByAlias(userDir, 1, "greet")
	if err != nil {
		t.Fatalf("alias lookup: %v", err)
	}
	if found == nil {
		t.Fatal("the dev cap must be resolvable by its alias")
	}
	if dir != versionDir {
		t.Fatalf("resolved to %q, expected %q", dir, versionDir)
	}
	hasAlias := false
	for _, a := range found.GetAliases() {
		if a == "greet" {
			hasAlias = true
		}
	}
	if !hasAlias {
		t.Fatalf("resolved cap does not carry the alias: %v", found.GetAliases())
	}
}

// TEST7157: dev-install refuses a PUBLISHED manifest. `registry_url` non-null
// means the cartridge belongs to a registry, and staging it under the dev slug
// would put a published identity in a slot reserved for local work.
func Test7157_dev_install_rejects_published_manifest(t *testing.T) {
	root := tempRoot(t, "published")
	project := filepath.Join(root, "proj")
	if err := os.MkdirAll(project, 0o755); err != nil {
		t.Fatalf("creating project: %v", err)
	}

	url := "https://cartridges.machinefabric.com/v1/manifest"
	manifest := &bifaci.CapManifest{
		Name:        "greeter",
		Version:     "0.1.0",
		Channel:     "nightly",
		RegistryURL: &url,
		Description: "published",
	}
	_, err := StageDevCartridge(project, manifest, filepath.Join(root, "cartridges"), 1, 7)
	if err == nil {
		t.Fatal("a published manifest must be refused")
	}
	if _, ok := err.(*NotDevError); !ok {
		t.Fatalf("expected NotDevError, got %T: %v", err, err)
	}
}

// TEST7159: a project with two languages' entries is REFUSED, not silently
// resolved. A project is one cartridge; installing whichever entry sorted first
// would be a coin flip the developer never sees.
func Test7159_two_entries_is_ambiguous_not_a_coin_flip(t *testing.T) {
	root := tempRoot(t, "ambiguous")
	proj := filepath.Join(root, "twoheaded")
	if err := os.MkdirAll(proj, 0o755); err != nil {
		t.Fatalf("creating project: %v", err)
	}

	written := 0
	for i := range StubLanguages {
		entry := filepath.Join(proj, Entry(&StubLanguages[i], "twoheaded"))
		if err := os.MkdirAll(filepath.Dir(entry), 0o755); err != nil {
			t.Fatalf("creating entry dir: %v", err)
		}
		if err := os.WriteFile(entry, []byte("#!/usr/bin/env bash\n"), 0o755); err != nil {
			t.Fatalf("writing entry: %v", err)
		}
		written++
		if written == 2 {
			break
		}
	}
	if written != 2 {
		t.Fatal("the contract must cover at least two languages")
	}

	if _, err := ProjectEntry(proj); err == nil {
		t.Fatal("two entries must be an error, not a pick")
	} else if _, ok := err.(*AmbiguousEntryError); !ok {
		t.Fatalf("expected AmbiguousEntryError, got %T: %v", err, err)
	}
}

// TEST7160: the vendored stub contract is IDENTICAL to the canonical source.
//
// This is the whole promise of `capdag new`: the same command from any capdag
// binary writes the same project. Every mirror's copy is generated from this
// one source, so a difference here means the reference itself was vendored
// from a different commit than the stub repo currently holds — which would
// ship capdags that disagree about what a cartridge looks like, silently.
func Test7160_vendored_stub_contract_matches_the_canonical_source(t *testing.T) {
	// Locate the canonical stubs relative to this mirror inside the workspace.
	// Absent (a standalone checkout of capdag-go), there is nothing to compare
	// against and the vendored copy IS the contract — that is not a skip to
	// hide behind, it is the only meaningful statement available.
	canonical := filepath.Join("..", "..", "capdag-stub-cartridges", "stubs.json")
	raw, err := os.ReadFile(canonical)
	if err != nil {
		t.Skipf("canonical stubs not present at %s (standalone checkout): %v", canonical, err)
	}

	var contract struct {
		ContractVersion int    `json:"contract_version"`
		Placeholder     string `json:"placeholder"`
		Languages       map[string]struct {
			Flag  string `json:"flag"`
			Entry string `json:"entry"`
			Files []struct {
				Source     string `json:"source"`
				Dest       string `json:"dest"`
				Executable bool   `json:"executable"`
			} `json:"files"`
		} `json:"languages"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatalf("canonical stubs.json does not parse: %v", err)
	}

	if contract.ContractVersion != StubContractVersion {
		t.Fatalf("vendored contract version %d, canonical %d — re-vendor the stubs",
			StubContractVersion, contract.ContractVersion)
	}
	if contract.Placeholder != StubPlaceholder {
		t.Fatalf("vendored placeholder %q, canonical %q", StubPlaceholder, contract.Placeholder)
	}
	if len(contract.Languages) != len(StubLanguages) {
		t.Fatalf("vendored %d languages, canonical %d — re-vendor the stubs",
			len(StubLanguages), len(contract.Languages))
	}

	stubRoot := filepath.Join("..", "..", "capdag-stub-cartridges")
	for i := range StubLanguages {
		vendored := &StubLanguages[i]
		spec, ok := contract.Languages[vendored.ID]
		if !ok {
			t.Fatalf("vendored language %q is not in the canonical contract", vendored.ID)
		}
		if spec.Flag != vendored.Flag || spec.Entry != vendored.Entry {
			t.Fatalf("%s: vendored flag/entry (%s, %s) differ from canonical (%s, %s)",
				vendored.ID, vendored.Flag, vendored.Entry, spec.Flag, spec.Entry)
		}
		if len(spec.Files) != len(vendored.Files) {
			t.Fatalf("%s: vendored %d files, canonical %d", vendored.ID, len(vendored.Files), len(spec.Files))
		}
		for j, file := range spec.Files {
			want, err := os.ReadFile(filepath.Join(stubRoot, file.Source))
			if err != nil {
				t.Fatalf("%s: canonical %s unreadable: %v", vendored.ID, file.Source, err)
			}
			got := vendored.Files[j]
			if got.Dest != file.Dest || got.Executable != file.Executable {
				t.Fatalf("%s: vendored file %d (%s) differs from canonical (%s)",
					vendored.ID, j, got.Dest, file.Dest)
			}
			assertStubMatches(t, vendored.ID, got.Dest, got.Contents, string(want))
		}
	}
}

// TEST7158: the fabric-conflict guard — a dev cap whose alias the fabric maps
// to a DIFFERENT cap is rejected; a brand-new alias, and a dev cap that matches
// an existing fabric cap exactly, are both accepted.
func Test7158_fabric_conflict_guard(t *testing.T) {
	alphaURN := `cap:alpha;in="media:enc=utf-8";out="media:enc=utf-8;alpha"`
	alphaParsed, err := urn.NewCapUrnFromString(alphaURN)
	if err != nil {
		t.Fatalf("parsing the seed URN: %v", err)
	}
	alpha := cap.NewCap(alphaParsed, "Alpha", []string{"alpha"})

	// The fabric knows exactly one alias: `alpha`.
	resolve := func(alias string) (string, error) {
		if alias == "alpha" {
			return alphaURN, nil
		}
		return "", fmt.Errorf("unknown alias %q", alias)
	}

	// A dev cap claiming `alpha` but with a DIFFERENT URN => conflict.
	betaParsed, err := urn.NewCapUrnFromString(
		`cap:beta;in="media:enc=utf-8";out="media:enc=utf-8;beta"`)
	if err != nil {
		t.Fatalf("parsing the clashing URN: %v", err)
	}
	clashing := cap.NewCap(betaParsed, "Clash", []string{"alpha"})
	var conflict *FabricConflictError
	if err := CheckNoFabricConflict(resolve, clashing); !errors.As(err, &conflict) {
		t.Fatalf("expected FabricConflictError for a shadowed alias, got %v", err)
	}
	if conflict.Alias != "alpha" {
		t.Errorf("the error must name the conflicting alias, got %q", conflict.Alias)
	}

	// A brand-new alias the fabric never heard of => fine.
	gammaParsed, err := urn.NewCapUrnFromString(
		`cap:gamma;in="media:enc=utf-8";out="media:enc=utf-8;gamma"`)
	if err != nil {
		t.Fatalf("parsing the fresh URN: %v", err)
	}
	fresh := cap.NewCap(gammaParsed, "Fresh", []string{"gamma"})
	if err := CheckNoFabricConflict(resolve, fresh); err != nil {
		t.Errorf("a brand-new alias must be accepted, got %v", err)
	}

	// The very same fabric cap (same alias => same URN) => not a conflict.
	if err := CheckNoFabricConflict(resolve, alpha); err != nil {
		t.Errorf("a dev cap matching the fabric cap exactly must be accepted, got %v", err)
	}
}

// firstTriple returns the first `N.N.N` in a line and the span it occupies.
func firstTriple(line string) ([]uint64, int, int, bool) {
	for index := 0; index < len(line); index++ {
		if line[index] < '0' || line[index] > '9' {
			continue
		}
		start := index
		for index < len(line) && ((line[index] >= '0' && line[index] <= '9') || line[index] == '.') {
			index++
		}
		parts := strings.Split(line[start:index], ".")
		if len(parts) != 3 {
			index = start
			continue
		}
		numbers := make([]uint64, 0, 3)
		ok := true
		for _, part := range parts {
			value, err := strconv.ParseUint(part, 10, 64)
			if err != nil {
				ok = false
				break
			}
			numbers = append(numbers, value)
		}
		if ok {
			return numbers, start, index, true
		}
		index = start
	}
	return nil, 0, 0, false
}

// isPinLine reports whether a line's dotted triple is a STAMPED version, not
// contract: one that names capdag (the dependency pin, in any language's
// syntax), or the stub's own version — a manifest's `version = "…"` line, a
// CapManifest `version: "…"` / `version="…"` argument, or a bare positional
// `"N.N.N",` (the stub repo's release, stamped by the templates so a
// scaffolded cartridge carries an accurate version). All move on every release
// and none says anything about the stub.
func isPinLine(line string) bool {
	stripped := strings.TrimSpace(line)
	if strings.Contains(line, "capdag") || strings.HasPrefix(stripped, "version") {
		return true
	}
	bare := strings.TrimSpace(strings.TrimSuffix(stripped, ","))
	if len(bare) < 2 || bare[0] != '"' || bare[len(bare)-1] != '"' {
		return false
	}
	_, start, end, ok := firstTriple(bare)
	return ok && start == 1 && end == len(bare)-1
}

// splitPins separates a stub file into its version pins (in order) and
// everything else. Rather than teach this several grammars, the first
// dotted-triple on every pin line (see isPinLine) IS a pin.
func splitPins(text string) ([][]uint64, string) {
	var pins [][]uint64
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if isPinLine(line) {
			if numbers, start, end, ok := firstTriple(line); ok {
				pins = append(pins, numbers)
				lines[i] = line[:start] + "<pin>" + line[end:]
			}
		}
	}
	return pins, strings.Join(lines, "\n")
}

// isCapdagDependencySource reports whether a manifest line is the capdag
// dependency SOURCE: a path, git tag, module version or SwiftPM from: naming
// capdag.
func isCapdagDependencySource(line string) bool {
	t := strings.TrimSpace(line)
	return strings.Contains(t, "capdag") && (strings.Contains(t, "path") ||
		strings.Contains(t, "git =") || strings.Contains(t, "tag =") ||
		strings.Contains(t, "url:") || strings.Contains(t, "from:") ||
		strings.HasPrefix(t, "require ") || strings.HasPrefix(t, "replace "))
}

// stripCapdagDependencySource strips the capdag dependency source from a
// manifest: the dependency line(s), the comment lines that explain them, and
// blank lines (the templates' conditional blocks differ in spacing). Other
// files are returned untouched.
func stripCapdagDependencySource(dest, text string) string {
	if !(strings.HasSuffix(dest, "Cargo.toml") || strings.HasSuffix(dest, "go.mod") || strings.HasSuffix(dest, "Package.swift")) {
		return text
	}
	var kept []string
	for _, line := range strings.Split(text, "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "#") || strings.HasPrefix(t, "//") || isCapdagDependencySource(t) {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n") + "\n"
}

func joinVersion(version []uint64) string {
	parts := make([]string, 0, len(version))
	for _, value := range version {
		parts = append(parts, strconv.FormatUint(value, 10))
	}
	return strings.Join(parts, ".")
}

// assertStubMatches compares a vendored stub file with the canonical bytes.
//
// Byte equality, with ONE allowance: the capdag version the stub pins may be
// OLDER in the vendored copy than in the canonical one. The canonical stub is
// rendered from a template that stamps capdag's current version, and the
// vendored copies are snapshots taken when someone last vendored them — so the
// two disagree from the moment capdag's version moves, which is every time it is
// bumped, and the disagreement says nothing about the stub CONTRACT.
//
// An older pin is harmless: it names a release that exists, so a cartridge
// scaffolded from it resolves. A NEWER pin is not, because it would name a
// version this capdag has not reached, so the comparison is an ordering and not
// "ignore the version".
//
// And ONE more: HOW the stub reaches capdag is environment, not contract. The
// dependency line of a language manifest (tag / module version / SwiftPM from:
// — or a path, were one ever rendered) and the comment lines explaining it are
// removed by stripCapdagDependencySource before comparing. The VERSION on that
// line is read first, and the ordering rule still applies to it.
func assertStubMatches(t *testing.T, language, dest, vendored, canonical string) {
	t.Helper()
	if vendored == canonical {
		return
	}
	// The pin is read from the dependency line BEFORE that line is stripped
	// — the ordering rule below must keep seeing it.
	vendoredPins, vendoredRest := splitPins(vendored)
	canonicalPins, canonicalRest := splitPins(canonical)
	vendoredRest = stripCapdagDependencySource(dest, vendoredRest)
	canonicalRest = stripCapdagDependencySource(dest, canonicalRest)
	if vendoredRest != canonicalRest {
		t.Fatalf("%s: vendored %s differs from the canonical bytes in more than the capdag dependency source and the stamped version pins — re-vendor the stubs", language, dest)
	}
	if len(vendoredPins) == 0 || len(vendoredPins) != len(canonicalPins) {
		t.Fatalf("%s: vendored %s differs from the canonical bytes and the two sides do not carry the same version pins to explain it — re-vendor the stubs", language, dest)
	}
	for p := range vendoredPins {
		vendoredPin, canonicalPin := vendoredPins[p], canonicalPins[p]
		for i := range vendoredPin {
			if vendoredPin[i] == canonicalPin[i] {
				continue
			}
			if vendoredPin[i] > canonicalPin[i] {
				t.Fatalf("%s: vendored %s pins %s but the canonical stub is at %s — a stub may lag a release, never precede one",
					language, dest, joinVersion(vendoredPin), joinVersion(canonicalPin))
			}
			break
		}
	}
}
