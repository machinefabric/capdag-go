// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/machinefabric/capdag-go/urn"
)

// InputItemKind identifies the type of an InputItem.
type InputItemKind int

const (
	InputItemFile      InputItemKind = iota
	InputItemDirectory InputItemKind = iota
	InputItemGlob      InputItemKind = iota
)

// InputItem is a single input specification from the user.
type InputItem struct {
	Kind    InputItemKind
	Path    string // for File and Directory
	Pattern string // for Glob
}

// FromString creates an InputItem from a string, auto-detecting the type.
// Glob metacharacters (* ? [) make it a Glob. Existing directories are Directory.
// Everything else is File (existence is checked during resolution, not here).
func FromString(s string) InputItem {
	if strings.ContainsAny(s, "*?[") {
		return InputItem{Kind: InputItemGlob, Pattern: s}
	}
	info, err := os.Stat(s)
	if err == nil && info.IsDir() {
		return InputItem{Kind: InputItemDirectory, Path: s}
	}
	return InputItem{Kind: InputItemFile, Path: s}
}

// ContentStructure is the detected internal structure of file content.
type ContentStructure int

const (
	// ScalarOpaque is a single opaque value (no list, no record markers). E.g. PDF, PNG.
	ScalarOpaque ContentStructure = iota
	// ScalarRecord is a single structured record (no list, has record marker). E.g. JSON object.
	ScalarRecord ContentStructure = iota
	// ListOpaque is a list of opaque values (has list, no record markers). E.g. array of strings.
	ListOpaque ContentStructure = iota
	// ListRecord is a list of records (has list and record markers). E.g. NDJSON.
	ListRecord ContentStructure = iota
)

// IsList returns true if this structure has the list marker.
func (c ContentStructure) IsList() bool {
	return c == ListOpaque || c == ListRecord
}

// IsRecord returns true if this structure has the record marker.
func (c ContentStructure) IsRecord() bool {
	return c == ScalarRecord || c == ListRecord
}

// String implements fmt.Stringer.
func (c ContentStructure) String() string {
	switch c {
	case ScalarOpaque:
		return "scalar/opaque"
	case ScalarRecord:
		return "scalar/record"
	case ListOpaque:
		return "list/opaque"
	case ListRecord:
		return "list/record"
	default:
		return fmt.Sprintf("ContentStructure(%d)", int(c))
	}
}

// ResolvedFile is a single resolved file with detected media information.
type ResolvedFile struct {
	Path             string
	MediaUrn         string
	SizeBytes        uint64
	ContentStructure ContentStructure
}

// ResolvedInputSet is the complete result of input resolution.
type ResolvedInputSet struct {
	Files       []ResolvedFile
	IsSequence  bool
	CommonMedia *string
}

// NewResolvedInputSet creates a ResolvedInputSet from files, computing IsSequence and CommonMedia.
func NewResolvedInputSet(files []ResolvedFile) *ResolvedInputSet {
	isSequence := len(files) > 1
	commonMedia := computeCommonMedia(files)
	return &ResolvedInputSet{
		Files:       files,
		IsSequence:  isSequence,
		CommonMedia: commonMedia,
	}
}

func computeCommonMedia(files []ResolvedFile) *string {
	if len(files) == 0 {
		return nil
	}
	first, err := urn.NewMediaUrnFromString(files[0].MediaUrn)
	if err != nil {
		panic(fmt.Sprintf("ResolvedInputSet: invalid media URN %q: %v", files[0].MediaUrn, err))
	}
	for _, file := range files[1:] {
		other, err := urn.NewMediaUrnFromString(file.MediaUrn)
		if err != nil {
			panic(fmt.Sprintf("ResolvedInputSet: invalid media URN %q: %v", file.MediaUrn, err))
		}
		if !first.IsEquivalent(other) {
			return nil
		}
	}
	s := files[0].MediaUrn
	return &s
}

// IsHomogeneous returns true if all files share the same base media type.
func (r *ResolvedInputSet) IsHomogeneous() bool {
	return r.CommonMedia != nil
}

// Len returns the number of files.
func (r *ResolvedInputSet) Len() int {
	return len(r.Files)
}

// IsEmpty returns true if there are no files.
func (r *ResolvedInputSet) IsEmpty() bool {
	return len(r.Files) == 0
}

// InputResolverErrorKind identifies the category of InputResolverError.
type InputResolverErrorKind int

const (
	InputErrNotFound         InputResolverErrorKind = iota
	InputErrPermissionDenied InputResolverErrorKind = iota
	InputErrInvalidGlob      InputResolverErrorKind = iota
	InputErrIo               InputResolverErrorKind = iota
	InputErrInspectionFailed InputResolverErrorKind = iota
	InputErrEmptyInput       InputResolverErrorKind = iota
	InputErrNoFilesResolved  InputResolverErrorKind = iota
	InputErrSymlinkCycle     InputResolverErrorKind = iota
)

// InputResolverError is an error that can occur during input resolution.
type InputResolverError struct {
	Kind    InputResolverErrorKind
	Path    string // for NotFound, PermissionDenied, IoError, InspectionFailed, SymlinkCycle
	Pattern string // for InvalidGlob
	Reason  string // for InvalidGlob and InspectionFailed
	Cause   error  // for IoError — the underlying OS error
}

// Error implements the error interface.
func (e *InputResolverError) Error() string {
	switch e.Kind {
	case InputErrNotFound:
		return fmt.Sprintf("Path not found: %s", e.Path)
	case InputErrPermissionDenied:
		return fmt.Sprintf("Permission denied: %s", e.Path)
	case InputErrInvalidGlob:
		return fmt.Sprintf("Invalid glob pattern %q: %s", e.Pattern, e.Reason)
	case InputErrIo:
		return fmt.Sprintf("IO error at %s: %v", e.Path, e.Cause)
	case InputErrInspectionFailed:
		return fmt.Sprintf("Content inspection failed for %s: %s", e.Path, e.Reason)
	case InputErrEmptyInput:
		return "No input paths provided"
	case InputErrNoFilesResolved:
		return "No files found after resolving all inputs"
	case InputErrSymlinkCycle:
		return fmt.Sprintf("Symlink cycle detected at: %s", e.Path)
	default:
		return fmt.Sprintf("InputResolverError(%d)", int(e.Kind))
	}
}

// Unwrap returns the underlying cause for IoError, nil otherwise.
func (e *InputResolverError) Unwrap() error {
	if e.Kind == InputErrIo {
		return e.Cause
	}
	return nil
}

// NotFoundError creates a NotFound InputResolverError.
func NotFoundError(path string) *InputResolverError {
	return &InputResolverError{Kind: InputErrNotFound, Path: path}
}

// PermissionDeniedError creates a PermissionDenied InputResolverError.
func PermissionDeniedError(path string) *InputResolverError {
	return &InputResolverError{Kind: InputErrPermissionDenied, Path: path}
}

// InvalidGlobError creates an InvalidGlob InputResolverError.
func InvalidGlobError(pattern, reason string) *InputResolverError {
	return &InputResolverError{Kind: InputErrInvalidGlob, Pattern: pattern, Reason: reason}
}

// IoError creates an IoError InputResolverError.
func IoError(path string, cause error) *InputResolverError {
	return &InputResolverError{Kind: InputErrIo, Path: path, Cause: cause}
}

// InspectionFailedError creates an InspectionFailed InputResolverError.
func InspectionFailedError(path, reason string) *InputResolverError {
	return &InputResolverError{Kind: InputErrInspectionFailed, Path: path, Reason: reason}
}

// EmptyInputError creates an EmptyInput InputResolverError.
func EmptyInputError() *InputResolverError {
	return &InputResolverError{Kind: InputErrEmptyInput}
}

// NoFilesResolvedError creates a NoFilesResolved InputResolverError.
func NoFilesResolvedError() *InputResolverError {
	return &InputResolverError{Kind: InputErrNoFilesResolved}
}

// SymlinkCycleError creates a SymlinkCycle InputResolverError.
func SymlinkCycleError(path string) *InputResolverError {
	return &InputResolverError{Kind: InputErrSymlinkCycle, Path: path}
}

// Source returns the underlying cause error (nil unless IoError).
// Matches Rust's std::error::Error::source().
func (e *InputResolverError) Source() error {
	return errors.Unwrap(e)
}
