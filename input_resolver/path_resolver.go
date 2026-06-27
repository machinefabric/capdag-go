// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// maxRecursionDepth bounds directory recursion (prevents infinite loops).
const maxRecursionDepth = 100

// ResolveItem resolves a single input item to a list of file paths.
func ResolveItem(item InputItem) ([]string, error) {
	switch item.Kind {
	case InputItemFile:
		return resolveFile(item.Path)
	case InputItemDirectory:
		return ResolveDirectory(item.Path)
	case InputItemGlob:
		return resolveGlob(item.Pattern)
	default:
		return nil, nil
	}
}

// ResolveItems resolves multiple input items, deduplicating by canonical path.
func ResolveItems(items []InputItem) ([]string, error) {
	if len(items) == 0 {
		return nil, EmptyInputError()
	}

	seen := make(map[string]bool)
	var result []string

	for i := range items {
		paths, err := ResolveItem(items[i])
		if err != nil {
			return nil, err
		}
		for _, path := range paths {
			// Canonicalize for deduplication; fall back to the raw path.
			canonical, err := filepath.Abs(path)
			if err != nil {
				canonical = path
			}
			if resolved, err := filepath.EvalSymlinks(canonical); err == nil {
				canonical = resolved
			}
			if !seen[canonical] {
				seen[canonical] = true
				result = append(result, path)
			}
		}
	}

	if len(result) == 0 {
		return nil, NoFilesResolvedError()
	}

	// Sort for consistent ordering.
	sort.Strings(result)

	return result, nil
}

// resolveFile resolves a single file path.
func resolveFile(path string) ([]string, error) {
	expanded := expandTilde(path)

	if _, err := os.Lstat(expanded); err != nil {
		if os.IsNotExist(err) {
			return nil, NotFoundError(expanded)
		}
		if os.IsPermission(err) {
			return nil, PermissionDeniedError(expanded)
		}
		return nil, IoError(expanded, err)
	}

	// If it points to a directory (following symlinks), resolve as a directory.
	statInfo, statErr := os.Stat(expanded)
	if statErr == nil && statInfo.IsDir() {
		return ResolveDirectory(expanded)
	}

	// Check if excluded.
	if ShouldExclude(expanded) {
		return []string{}, nil // Silently skip excluded files.
	}

	// Resolve symlinks (with cycle detection).
	resolved, err := resolveSymlink(expanded)
	if err != nil {
		return nil, err
	}

	return []string{resolved}, nil
}

// ResolveDirectory resolves a directory recursively.
func ResolveDirectory(path string) ([]string, error) {
	expanded := expandTilde(path)

	info, err := os.Stat(expanded)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, NotFoundError(expanded)
		}
		if os.IsPermission(err) {
			return nil, PermissionDeniedError(expanded)
		}
		return nil, IoError(expanded, err)
	}

	if !info.IsDir() {
		// If user passed a file as a directory, resolve it as a file.
		return resolveFile(expanded)
	}

	var files []string
	visited := make(map[string]bool)

	if err := resolveDirectoryRecursive(expanded, &files, visited, 0); err != nil {
		return nil, err
	}

	return files, nil
}

// resolveDirectoryRecursive walks a directory tree, collecting files.
func resolveDirectoryRecursive(dir string, files *[]string, visited map[string]bool, depth int) error {
	if depth > maxRecursionDepth {
		return SymlinkCycleError(dir)
	}

	// Canonicalize for cycle detection.
	canonical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		if os.IsPermission(err) {
			return PermissionDeniedError(dir)
		}
		return IoError(dir, err)
	}

	if visited[canonical] {
		// Already visited, skip (handles symlink cycles).
		return nil
	}
	visited[canonical] = true

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsPermission(err) {
			return PermissionDeniedError(dir)
		}
		return IoError(dir, err)
	}

	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())

		info, statErr := os.Stat(path)
		if statErr != nil {
			// Skip entries we cannot stat (broken symlinks, special files).
			continue
		}

		if info.IsDir() {
			if ShouldExcludeDir(path) {
				continue
			}
			if err := resolveDirectoryRecursive(path, files, visited, depth+1); err != nil {
				return err
			}
		} else if info.Mode().IsRegular() {
			if !ShouldExclude(path) {
				*files = append(*files, path)
			}
		}
		// Skip other types (special files).
	}

	return nil
}

// resolveGlob resolves a glob pattern (supports ** recursive matching).
func resolveGlob(pattern string) ([]string, error) {
	expanded := expandTildeString(pattern)

	if reason, ok := globSyntaxError(expanded); ok {
		return nil, InvalidGlobError(pattern, reason)
	}

	matches, err := globMatch(expanded)
	if err != nil {
		return nil, InvalidGlobError(pattern, err.Error())
	}

	var files []string
	for _, path := range matches {
		info, statErr := os.Stat(path)
		if statErr != nil {
			continue
		}
		if info.Mode().IsRegular() && !ShouldExclude(path) {
			files = append(files, path)
		}
		// Skip directories and excluded files.
	}

	return files, nil
}

// resolveSymlink canonicalizes a path, surfacing permission/IO errors.
func resolveSymlink(path string) (string, error) {
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		if os.IsPermission(err) {
			return "", PermissionDeniedError(path)
		}
		return "", IoError(path, err)
	}
	abs, absErr := filepath.Abs(canonical)
	if absErr == nil {
		return abs, nil
	}
	return canonical, nil
}

// expandTilde expands a leading ~ to the user's home directory.
func expandTilde(path string) string {
	return expandTildeString(path)
}

// expandTildeString expands a leading ~ in a string path.
func expandTildeString(s string) string {
	if strings.HasPrefix(s, "~") {
		home, err := os.UserHomeDir()
		if err == nil && home != "" {
			rest := strings.TrimPrefix(s, "~")
			rest = strings.TrimPrefix(rest, string(os.PathSeparator))
			rest = strings.TrimPrefix(rest, "/")
			return filepath.Join(home, rest)
		}
	}
	return s
}
