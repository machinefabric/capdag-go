// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// globSyntaxError detects invalid glob syntax, mirroring the Rust `glob`
// crate's PatternError surface that the resolver relied on. The case the
// resolver tests exercise is an unclosed character class (e.g. "[unclosed").
// Returns (reason, true) when the pattern is invalid.
func globSyntaxError(pattern string) (string, bool) {
	inClass := false
	escaped := false
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if escaped {
			escaped = false
			continue
		}
		switch c {
		case '\\':
			escaped = true
		case '[':
			if !inClass {
				inClass = true
			}
		case ']':
			if inClass {
				inClass = false
			}
		}
	}
	if inClass {
		return "wildcards are either regular `[...]` character classes, [!...] negated character classes, or [^...] negated character classes; unclosed character class", true
	}
	return "", false
}

// globMatch expands a glob pattern to matching paths. It supports the `**`
// recursive wildcard (matching across directory separators) in addition to
// the single-segment wildcards `*`, `?`, and `[...]` honored by filepath.Match.
// The semantics mirror the Rust `glob` crate as used by the resolver.
func globMatch(pattern string) ([]string, error) {
	pattern = filepath.ToSlash(pattern)

	// Split into a non-glob prefix (the deepest directory with no
	// metacharacters) and the remaining glob segments.
	segments := strings.Split(pattern, "/")

	var baseParts []string
	rest := segments
	for len(rest) > 0 && !hasMeta(rest[0]) {
		baseParts = append(baseParts, rest[0])
		rest = rest[1:]
	}

	base := strings.Join(baseParts, "/")
	if base == "" {
		if strings.HasPrefix(pattern, "/") {
			base = "/"
		} else {
			base = "."
		}
	}

	var results []string
	if err := globWalk(base, rest, &results); err != nil {
		return nil, err
	}
	sort.Strings(results)
	return results, nil
}

// hasMeta reports whether a path segment contains glob metacharacters.
func hasMeta(segment string) bool {
	return strings.ContainsAny(segment, "*?[")
}

// globWalk recursively matches the remaining glob segments against the
// filesystem starting at dir.
func globWalk(current string, segments []string, results *[]string) error {
	if len(segments) == 0 {
		if _, err := os.Lstat(current); err == nil {
			*results = append(*results, current)
		}
		return nil
	}

	seg := segments[0]
	remaining := segments[1:]

	if seg == "**" {
		// `**` matches zero or more path segments. First, try matching the
		// rest at the current level (zero segments consumed).
		if err := globWalk(current, remaining, results); err != nil {
			return err
		}
		// Then descend into every subdirectory, keeping `**` in play.
		entries, err := os.ReadDir(current)
		if err != nil {
			return nil // Unreadable dir: no matches, not a hard error.
		}
		for _, entry := range entries {
			if entry.IsDir() {
				child := filepath.ToSlash(filepath.Join(current, entry.Name()))
				if err := globWalk(child, segments, results); err != nil {
					return err
				}
			}
		}
		return nil
	}

	entries, err := os.ReadDir(current)
	if err != nil {
		return nil // Unreadable dir: no matches.
	}
	for _, entry := range entries {
		matched, matchErr := filepath.Match(seg, entry.Name())
		if matchErr != nil {
			return matchErr
		}
		if matched {
			child := filepath.ToSlash(filepath.Join(current, entry.Name()))
			if err := globWalk(child, remaining, results); err != nil {
				return err
			}
		}
	}
	return nil
}
