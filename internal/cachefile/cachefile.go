// Package cachefile writes a file of a cache that several processes share.
//
// capdag keeps two caches on disk that every capdag process on a machine reads
// and writes at once: the fabric definition cache (package fabric) and the
// profile-schema cache (package media). Both replace an entry whole, the same
// way: it is written to a temporary file in the folder it belongs in and
// renamed over the entry, so a reader sees the old entry or the new one, never
// part of one. A temporary file is named `.<entry>.<pid>-<n>.tmp`; one a
// killed writer left behind is never read as an entry.
//
// Matches Rust: capdag::cache_file
package cachefile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

// CapdagRoot is the folder every capdag cache lives under: `capdag` in the
// operating system's cache folder (`%LOCALAPPDATA%` on Windows, else
// `$XDG_CACHE_HOME`, else `~/.cache`).
func CapdagRoot() (string, error) {
	var base string
	if runtime.GOOS == "windows" {
		base = os.Getenv("LOCALAPPDATA")
	} else {
		base = os.Getenv("XDG_CACHE_HOME")
	}
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("cannot determine the home directory for the cache root: %v", err)
		}
		base = filepath.Join(home, ".cache")
	}
	return filepath.Join(base, "capdag"), nil
}

// temporaryFiles tells apart the temporary files one process writes at once.
var temporaryFiles atomic.Uint64

// IsTemporary reports whether fileName is a temporary file WriteAtomically
// writes, or one left behind when its writer was killed: never an entry.
func IsTemporary(fileName string) bool {
	return strings.HasPrefix(fileName, ".") && strings.HasSuffix(fileName, ".tmp")
}

// WriteAtomically puts data at path whole: written to a temporary file beside
// it, then renamed over it. A reader opening path meanwhile reads what was
// there before, or data, never part of them. The folder is made when missing.
// The error names the file.
func WriteAtomically(path string, data []byte) error {
	folder := filepath.Dir(path)
	if err := os.MkdirAll(folder, 0o755); err != nil {
		return fmt.Errorf("failed to create the cache folder %s: %v", folder, err)
	}
	temporary := filepath.Join(folder, fmt.Sprintf(".%s.%d-%d.tmp",
		filepath.Base(path), os.Getpid(), temporaryFiles.Add(1)-1))
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("failed to write the cache file %s: %v", temporary, err)
	}
	if err := renameOver(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("failed to put the cache file %s in place: %v", path, err)
	}
	return nil
}

// renameOver renames from over to, replacing it.
//
// Unix replaces the name in one step whatever else has it open. Windows
// refuses the replacement with "access denied" while another process is
// replacing the same entry at that moment; that is a peer writing the same
// entry, so the rename is asked again for a short bound, after which the
// refusal is reported as it stands.
func renameOver(from, to string) error {
	for attempts := 0; ; attempts++ {
		err := os.Rename(from, to)
		if err != nil && runtime.GOOS == "windows" && errors.Is(err, fs.ErrPermission) && attempts < 50 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return err
	}
}
