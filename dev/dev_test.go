package dev

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// TEST7156: ReadEntryManifest + StageDevCartridge + FindDevCapByAlias
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

// TEST7160: the vendored stub contract is IDENTICAL to the reference's.
//
// This is the whole promise of `capdag new`: the same command from any capdag
// binary writes the same project. The vendored copies are generated from one
// source, so a difference here means a mirror was vendored from a different
// commit — which would ship two capdags that disagree about what a cartridge
// looks like, silently.
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
		t.Fatalf("vendored contract version %d, canonical %d — re-run dx stubs vendor",
			StubContractVersion, contract.ContractVersion)
	}
	if contract.Placeholder != StubPlaceholder {
		t.Fatalf("vendored placeholder %q, canonical %q", StubPlaceholder, contract.Placeholder)
	}
	if len(contract.Languages) != len(StubLanguages) {
		t.Fatalf("vendored %d languages, canonical %d — re-run dx stubs vendor",
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
			if got.Contents != string(want) {
				t.Fatalf("%s: vendored %s differs from the canonical bytes — re-run dx stubs vendor",
					vendored.ID, file.Dest)
			}
		}
	}
}

// TEST7158: the fabric-conflict guard — a dev cap whose alias the fabric maps
// to a DIFFERENT cap is rejected; a brand-new alias, and a dev cap that matches
// an existing fabric cap exactly, are both accepted.
//
// The resolver stands in for the fabric's alias table. The reference passes a
// live FabricRegistry; this mirror takes the lookup as a function, which is a
// documented object-level divergence — the guard's behavior is identical, and
// that is what the shared number asserts.
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
