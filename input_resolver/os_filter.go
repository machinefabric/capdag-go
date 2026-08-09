// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

import (
	"path/filepath"
	"strings"
)

// excludedFiles lists filenames that are always excluded (exact match).
var excludedFiles = map[string]bool{
	// macOS
	".DS_Store":                           true,
	".localized":                          true,
	".AppleDouble":                        true,
	".LSOverride":                         true,
	".DocumentRevisions-V100":             true,
	".fseventsd":                          true,
	".Spotlight-V100":                     true,
	".TemporaryItems":                     true,
	".Trashes":                            true,
	".VolumeIcon.icns":                    true,
	".com.apple.timemachine.donotpresent": true,
	".AppleDB":                            true,
	".AppleDesktop":                       true,
	"Network Trash Folder":                true,
	"Temporary Items":                     true,
	".apdisk":                             true,
	// Windows
	"Thumbs.db":             true,
	"Thumbs.db:encryptable": true,
	"ehthumbs.db":           true,
	"ehthumbs_vista.db":     true,
	"desktop.ini":           true,
	// Linux
	".directory": true,
	// Editor/IDE
	".project":   true,
	".settings":  true,
	".classpath": true,
}

// excludedDirs lists directory names that are always excluded (entire subtree).
var excludedDirs = map[string]bool{
	// Version control
	".git":    true,
	".svn":    true,
	".hg":     true,
	".bzr":    true,
	"_darcs":  true,
	".fossil": true,
	// macOS
	".Spotlight-V100":         true,
	".Trashes":                true,
	".fseventsd":              true,
	".TemporaryItems":         true,
	"__MACOSX":                true,
	".DocumentRevisions-V100": true,
	// IDE/Editor
	".idea":         true,
	".vscode":       true,
	".vs":           true,
	"__pycache__":   true,
	"node_modules":  true,
	".tox":          true,
	".nox":          true,
	".eggs":         true,
	".mypy_cache":   true,
	".pytest_cache": true,
	".hypothesis":   true,
}

// excludedExtensions lists extensions that indicate temp/backup files.
var excludedExtensions = map[string]bool{
	"tmp":    true,
	"temp":   true,
	"swp":    true,
	"swo":    true,
	"swn":    true,
	"bak":    true,
	"backup": true,
	"orig":   true,
}

// ShouldExclude returns true if the path is an OS artifact and should be skipped.
func ShouldExclude(path string) bool {
	filename := filepath.Base(path)
	if filename == "" || filename == "." {
		return false
	}

	// Exact filename match
	if excludedFiles[filename] {
		return true
	}

	// macOS resource fork files (._*)
	if strings.HasPrefix(filename, "._") {
		return true
	}

	// Office lock files (~$*)
	if strings.HasPrefix(filename, "~$") {
		return true
	}

	// macOS Icon file (Icon\r)
	if filename == "Icon\r" || filename == "Icon\x0d" {
		return true
	}

	// Temp/backup extensions
	ext := filepath.Ext(filename)
	if ext != "" {
		extLower := strings.ToLower(ext[1:]) // strip the leading dot
		if excludedExtensions[extLower] {
			return true
		}
	}

	return false
}

// ShouldExcludeDir returns true if the directory is an OS artifact or should be skipped.
func ShouldExcludeDir(path string) bool {
	dirname := filepath.Base(path)
	if dirname == "" || dirname == "." {
		return false
	}
	return excludedDirs[dirname]
}
