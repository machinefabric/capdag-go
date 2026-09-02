package dev

import (
	"os/exec"
	"path/filepath"

	"github.com/machinefabric/capdag-go/bifaci"
)

// EntryFile is the entry as it is spelled ON DISK for this platform.
//
// The declaration is one string for every platform — it is vendored into four
// mirrors and must not carry one platform's spelling — so the platform's part
// of the answer is added here. An entry with an extension already has one and
// is left alone; an entry with none is a compiled binary and gains the
// platform's suffix.
//
// A scaffolded Rust cartridge declares `target/release/<name>` and Cargo writes
// `target/release/<name>.exe`. Looking for the declared spelling found nothing
// on Windows, so a project that had built perfectly reported that it had no
// entry at all.
func EntryFile(language *StubLanguage, name string) string {
	entry := Entry(language, name)
	if filepath.Ext(entry) != "" {
		return entry
	}
	return entry + bifaci.ExecutableSuffix()
}

// Command runs a cartridge entry with `arguments`.
//
// The rule is bifaci's, because hosting a cartridge and reading one's manifest
// are the same act of starting it and must not disagree about how.
func Command(entry string, arguments ...string) *exec.Cmd {
	return bifaci.Command(entry, arguments...)
}
