package bifaci

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TEST7162: a script cartridge is started through its interpreter.
//
// A scaffolded Python cartridge is `cartridge.py`, and on Unix its shebang
// makes it directly executable. Windows has no shebang, so `CreateProcess`
// refuses the file outright:
//
//	%1 is not a valid Win32 application
//
// Every caller wrote `exec.Command(entry, …)` and all three — reading a
// manifest, probing caps, hosting — were wrong on Windows at once. No
// scaffolded Python cartridge could be launched on the platform at all.
func Test7162_a_script_entry_is_launched_through_an_interpreter(t *testing.T) {
	program, leading := Launcher(filepath.Join("proj", "cartridge.py"))
	if program == filepath.Join("proj", "cartridge.py") {
		t.Fatalf("a .py must not be launched as a program: got %q", program)
	}
	if len(leading) != 1 || leading[0] != filepath.Join("proj", "cartridge.py") {
		t.Fatalf("the entry must be the interpreter's argument: got %v", leading)
	}

	// Case does not decide it. A `CARTRIDGE.PY` is the same file on the
	// platform where the question arises at all.
	if program, _ := Launcher("CARTRIDGE.PY"); program == "CARTRIDGE.PY" {
		t.Fatalf("extension matching must not be case-sensitive: got %q", program)
	}
}

// TEST7163: a compiled cartridge is started as itself.
//
// The rule keys on the extension, so it has to leave alone the entries that
// already are programs — a Rust or Go cartridge's binary. Running one through
// an interpreter would be a new failure invented by the fix.
func Test7163_a_compiled_entry_runs_itself(t *testing.T) {
	entry := filepath.Join("target", "release", "mood-tagger"+ExecutableSuffix())
	program, leading := Launcher(entry)
	if program != entry {
		t.Fatalf("a compiled entry is its own program: got %q", program)
	}
	if len(leading) != 0 {
		t.Fatalf("a compiled entry takes no leading arguments: got %v", leading)
	}
}

// TEST7164: a compiled entry is looked for by the name the platform gives it.
//
// The stub declares `target/release/<name>` — one string, vendored into four
// mirrors, so it cannot carry one platform's spelling. Cargo writes
// `<name>.exe` on Windows, and looking for the declared spelling found
// nothing: a project that had built perfectly reported that it had no entry.
func Test7164_a_compiled_entry_carries_the_platforms_suffix(t *testing.T) {
	suffix := ExecutableSuffix()
	if runtime.GOOS == "windows" && suffix != ".exe" {
		t.Fatalf("Windows programs end in .exe: got %q", suffix)
	}
	if runtime.GOOS != "windows" && suffix != "" {
		t.Fatalf("no other platform suffixes its programs: got %q", suffix)
	}
}

// TEST7165: the entry's own arguments come after the interpreter's.
//
// `Command(entry, "manifest")` has to produce `python3 cartridge.py manifest`
// and never `python3 manifest cartridge.py`, which would ask the interpreter
// to run a file called `manifest`.
func Test7165_the_entrys_arguments_follow_it(t *testing.T) {
	cmd := Command(filepath.Join("proj", "cartridge.py"), "manifest")
	argv := cmd.Args
	if argv[len(argv)-1] != "manifest" {
		t.Fatalf("the entry's argument must come last: %v", argv)
	}
	if !strings.HasSuffix(argv[len(argv)-2], "cartridge.py") {
		t.Fatalf("the entry must precede its own arguments: %v", argv)
	}
}
