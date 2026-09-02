package bifaci

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// Interpreters is how a cartridge entry that is a SCRIPT is run.
//
// A scaffolded Python cartridge is `cartridge.py`, and on Unix the shebang
// makes that directly executable. Windows has no shebang: `CreateProcess` — and
// therefore every language's `exec` — refuses a `.py` outright with
//
//	%1 is not a valid Win32 application
//
// so `capdag dev-install` could not read a Python project's manifest, and no
// scaffolded Python cartridge could be launched at all on the platform. Naming
// the interpreter is what the shebang was doing; doing it here does it on both.
//
// Keyed by the entry's extension rather than by the language, because the
// callers that need it have a PATH and not a language: `ProjectEntry` finds an
// entry by looking, and what it finds is a filename.
var Interpreters = map[string]string{
	".py": "python3",
	".js": "node",
}

// ExecutableSuffix is what a COMPILED entry is called on this platform.
//
// A scaffolded Rust cartridge declares `target/release/<name>` and Cargo
// writes `target/release/<name>.exe`. Looking for the declared spelling found
// nothing on Windows, so a project that had built perfectly reported that it
// had no entry.
func ExecutableSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

// Launcher is the command that runs a cartridge entry: the program, and the
// arguments that come before the entry's own.
//
// A compiled entry runs itself. A script entry runs under the interpreter its
// extension names, which is what makes a Python cartridge launchable on a
// platform with no shebang.
func Launcher(entry string) (string, []string) {
	interpreter, isScript := Interpreters[strings.ToLower(filepath.Ext(entry))]
	if !isScript {
		return entry, nil
	}
	// `python3` is the name everywhere except a Windows install, which ships
	// `python.exe` and no `python3.exe`. Asked in order; the interpreter is
	// still the answer when neither resolves, so the refusal names the
	// interpreter rather than the file.
	if runtime.GOOS == "windows" && interpreter == "python3" {
		for _, candidate := range []string{"python3", "python"} {
			if _, err := exec.LookPath(candidate); err == nil {
				return candidate, []string{entry}
			}
		}
	}
	return interpreter, []string{entry}
}

// Command builds an exec.Cmd that runs a cartridge entry with `arguments`.
//
// One place, so every caller that starts a cartridge — reading a manifest,
// probing its caps, hosting it — starts it the same way. They did not: each
// wrote `exec.Command(entry, …)`, and all three were wrong on Windows in the
// same way at once.
func Command(entry string, arguments ...string) *exec.Cmd {
	program, leading := Launcher(entry)
	return exec.Command(program, append(leading, arguments...)...)
}
