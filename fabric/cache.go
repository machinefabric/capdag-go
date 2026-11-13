package fabric

// Where a fabric registry's definitions are kept on disk, and how an entry is
// written and read there.
//
// The cache is shared: every capdag process on a machine that reads the same
// fabric reads and writes the same folder, and they run at once (a test run
// starts many builds within milliseconds of each other). Two rules follow.
//
//   - One folder per fabric manifest version. A registry pinned at manifest N
//     keeps everything under `<registry folder>/v<N>/`:
//
//     <os cache>/capdag/<charter slug>/        the registry's folder; its charter
//     v<N>/manifest.json                     manifest N, as the registry served it
//     v<N>/caps/<sha256 of URN>/<defver>.json
//     v<N>/media/<sha256 of URN>/<defver>.json
//     v<N>/aliases/<sha256 of name>/<defver>.json
//     v<N>/dims/<sha256 of name>/<defver>.json
//
//     capdag releases built against different manifests can be installed side
//     by side; each reads and writes only its own `v<N>`, so none ever reads a
//     definition another wrote under other rules.
//
//   - An entry is replaced whole. It is written to a temporary file in the
//     folder it belongs in and renamed over the entry, so a reader sees the
//     old entry or the new one, never part of one. A temporary file is named
//     `.<entry>.<pid>-<n>.tmp`; one a killed writer left behind is never read,
//     and `capdag cache clear` removes it.
//
// An entry that is there and cannot be read, or does not parse, or is not the
// definition its path names, is corrupt: it is an error naming the file, never
// treated as missing.
//
// Before this layout, every manifest version shared the registry's folder
// (`<charter slug>/{caps,media,aliases,dims,manifests}`). This library neither
// reads nor writes those folders: capdag releases from before it still do.
// `capdag cache clear` removes them.
//
// Matches Rust: capdag::fabric::cache

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
)

// kindFolders are the folders a manifest version's definitions live in, one
// per kind.
var kindFolders = []string{"caps", "media", "aliases", "dims"}

// manifestFile is the file a manifest version's manifest is kept in, in its
// folder.
const manifestFile = "manifest.json"

// legacyFolders are the folders the shared layout before this one kept in a
// registry's folder. Nothing reads them; clearing the cache removes them.
var legacyFolders = []string{"caps", "media", "aliases", "dims", "manifests"}

// versionDir is the folder manifest manifestVersion of a registry keeps its
// cache in.
func versionDir(registryDir string, manifestVersion uint32) string {
	return filepath.Join(registryDir, fmt.Sprintf("v%d", manifestVersion))
}

// entryPath is the file the definition key (a normalized URN, or an alias or
// dim name) of kind at defver is kept in, under a manifest version's folder.
func entryPath(versionDir, kind, key string, defver uint32) string {
	return filepath.Join(versionDir, kind, keyHash(key), fmt.Sprintf("%d.json", defver))
}

// keyHash is the hash a definition is filed under, in the cache as in the
// registry.
func keyHash(key string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(key)))
}

// temporaryFiles tells apart the temporary files one process writes at once.
var temporaryFiles atomic.Uint64

// writeAtomically puts data at path whole: written to a temporary file beside
// it, then renamed over it. A reader opening path meanwhile reads what was
// there before, or data, never part of them. The folder is made when missing.
func writeAtomically(path string, data []byte) error {
	folder := filepath.Dir(path)
	if err := os.MkdirAll(folder, 0o755); err != nil {
		return errf(ErrCache, "failed to create the cache folder %s: %v", folder, err)
	}
	temporary := filepath.Join(folder, fmt.Sprintf(".%s.%d-%d.tmp",
		filepath.Base(path), os.Getpid(), temporaryFiles.Add(1)-1))
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		_ = os.Remove(temporary)
		return errf(ErrCache, "failed to write the cache file %s: %v", temporary, err)
	}
	if err := renameOver(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return errf(ErrCache, "failed to put the cache file %s in place: %v", path, err)
	}
	return nil
}

// renameOver renames from over to, replacing it.
//
// Unix replaces the name in one step whatever else has it open. Windows
// refuses the replacement with "access denied" while another process is
// replacing the same entry at that moment; that is a peer writing the same
// definition, so the rename is asked again for a short bound, after which the
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

// storeEntry serializes entry and puts it at path whole (writeAtomically).
// what names the kind of entry in the error.
func storeEntry(path string, entry any, what string) error {
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return errf(ErrCache, "failed to serialize the %s cache entry: %v", what, err)
	}
	return writeAtomically(path, data)
}

// readEntry is the entry at path: nil when there is none, the entry when it
// parses, and an error naming the file when it is there and cannot be read or
// does not parse. what names the kind of entry in the error.
func readEntry[T any](path, what string) (*T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, errf(ErrCache, "the cached %s %s cannot be read: %v", what, path, err)
	}
	var entry T
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, corrupt(path, what, err.Error())
	}
	return &entry, nil
}

// corrupt is the error for a cache entry that is there and is not what its
// path says.
func corrupt(path, what, detail string) *Error {
	return errf(ErrCache,
		"the cached %s %s is corrupt: %s; remove the file, or run `capdag cache clear`",
		what, path, detail)
}

// clearCacheFolder empties the cache folder of one manifest version,
// versionDir, and removes the folders the layout before per-version folders
// kept beside it.
//
// The cache is SHARED, and the processes that clear it run at the same time:
// a test run starts eight cartridge builds within milliseconds of each other
// and every one of them refreshes this folder. Removing the tree and putting
// it back is not safe between peers — one process deleting an ancestor while
// another is creating into it is how a build fails with `no such file or
// directory` on a path it had just made.
//
// So a kind's folder is never removed, only its contents, and every step
// tolerates a peer having done the same thing first. The end state is the
// same — an empty cache — and no peer is ever left with the tree missing
// underneath it. The manifest, and any temporary file a killed writer left,
// are removed from the version's folder; other versions' folders and the
// stored charter are left as they are. Matches Rust: fn clear_cache_folder
func clearCacheFolder(versionDir string) error {
	for _, kind := range kindFolders {
		directory := filepath.Join(versionDir, kind)
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return errf(ErrCache, "failed to create cache directory %s: %v", directory, err)
		}
		if err := removeContents(directory); err != nil {
			return err
		}
	}
	// What is left at the top of the version's folder: the manifest, and
	// temporary files. The kinds' folders themselves stay.
	top, err := os.ReadDir(versionDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return errf(ErrCache, "failed to read cache directory %s: %v", versionDir, err)
	}
	for _, entry := range top {
		path := filepath.Join(versionDir, entry.Name())
		if !isDir(path) {
			if err := removeShared(path); err != nil {
				return err
			}
		}
	}
	registryDir := filepath.Dir(versionDir)
	for _, legacy := range legacyFolders {
		path := filepath.Join(registryDir, legacy)
		if isDir(path) {
			if err := removeShared(path); err != nil {
				return err
			}
		}
	}
	return nil
}

// removeContents removes everything in directory, which peers are clearing
// and filling at the same time (removeShared).
func removeContents(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		// A peer removed it between the create and the read. It is empty by
		// any reading, which is what was wanted.
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return errf(ErrCache, "failed to read cache directory %s: %v", directory, err)
	}
	for _, entry := range entries {
		if err := removeShared(filepath.Join(directory, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// removeShared removes one file or folder of the shared cache, which a peer
// may be removing or refilling at the same moment.
func removeShared(path string) error {
	var err error
	if isDir(path) {
		err = os.RemoveAll(path)
	} else {
		err = os.Remove(path)
	}
	switch {
	case err == nil:
		return nil
	// Already gone: a peer clearing the same cache got there first, which is
	// the outcome either way.
	case errors.Is(err, fs.ErrNotExist):
		return nil
	// Refilled underneath us. macOS reports this as `directory not empty`
	// where Linux reports the missing path — two faces of the same race, and
	// neither is a failure of this clear: the entries now there were put there
	// by a peer that has just refreshed them, so they are exactly what a
	// refresh was for.
	case isBusy(err):
		return nil
	case deletedByAPeer(path, err):
		return nil
	}
	return errf(ErrCache, "failed to remove %s: %v", path, err)
}

// isDir reports whether path is a folder, following a link as a removal does.
func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// isBusy reports whether an error means "somebody else is using this right
// now".
//
// A shared cache is cleared by several processes at once. One of them removing
// a directory another is writing into is answered differently by each platform
// — ENOTEMPTY or EBUSY — and neither says the clear failed. Windows answers
// with its own numbers: ERROR_DIR_NOT_EMPTY (145) and ERROR_SHARING_VIOLATION
// (32, a peer holding an entry open to refresh it) are the same two faces.
// Matches Rust: fn is_busy
func isBusy(err error) bool {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return false
	}
	if runtime.GOOS == "windows" {
		return errno == 145 || errno == 32
	}
	return errno == syscall.ENOTEMPTY || errno == syscall.EBUSY
}

// deletedByAPeer reports whether a removal refused with "access denied" was a
// PEER deleting the same entry.
//
// Windows answers every open of a file that is being deleted — a second
// delete included — with ERROR_ACCESS_DENIED until the deleting process's last
// handle closes; Unix reports the same race as "not found". Access denied is
// also what a genuine permission problem says, so the entry is asked for
// afterwards: gone within a short bound means a peer took it, still there
// means it really cannot be removed and that is reported.
// Matches Rust: fn deleted_by_a_peer
func deletedByAPeer(path string, err error) bool {
	if runtime.GOOS != "windows" || !errors.Is(err, fs.ErrPermission) {
		return false
	}
	for attempt := 0; attempt < 50; attempt++ {
		if _, statErr := os.Lstat(path); statErr != nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
