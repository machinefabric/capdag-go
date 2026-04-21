// Package cap defines argument/result/stdin-source data types used by
// in-process cap handlers.
//
// Previously this file also housed a CapCaller + CapSet interface for an
// in-process direct-dispatch execution model. That stack is gone: cap
// invocation now goes through the bifaci relay (RelaySwitch.ExecuteCap) for
// out-of-process cartridges and through in-process frame handlers for
// engine-built providers. The remaining types below are the argument/return
// shape both paths share.
package cap

import (
	"fmt"
	"unicode/utf8"
)

// StdinSourceKind identifies the type of stdin source
type StdinSourceKind int

const (
	// StdinSourceKindData represents raw byte data for stdin
	StdinSourceKindData StdinSourceKind = iota
	// StdinSourceKindFileReference represents a file reference for stdin
	// Used for cartridges to read files locally instead of sending bytes over the wire
	StdinSourceKindFileReference
)

// StdinSource represents the source for stdin data.
// For cartridges (via gRPC/XPC), using FileReference avoids size limits
// by letting the receiving side read the file locally.
type StdinSource struct {
	Kind StdinSourceKind

	// Data is the raw byte data (used when Kind == StdinSourceKindData)
	Data []byte

	// FileReference fields (used when Kind == StdinSourceKindFileReference)
	TrackedFileID    string
	OriginalPath     string
	SecurityBookmark []byte
	MediaUrn         string
}

// NewStdinSourceFromData creates a StdinSource from raw bytes
func NewStdinSourceFromData(data []byte) *StdinSource {
	return &StdinSource{
		Kind: StdinSourceKindData,
		Data: data,
	}
}

// NewStdinSourceFromFileReference creates a StdinSource from a file reference
func NewStdinSourceFromFileReference(trackedFileID, originalPath string, securityBookmark []byte, mediaUrn string) *StdinSource {
	return &StdinSource{
		Kind:             StdinSourceKindFileReference,
		TrackedFileID:    trackedFileID,
		OriginalPath:     originalPath,
		SecurityBookmark: securityBookmark,
		MediaUrn:         mediaUrn,
	}
}

// IsData returns true if this is a data source
func (s *StdinSource) IsData() bool {
	return s != nil && s.Kind == StdinSourceKindData
}

// IsFileReference returns true if this is a file reference source
func (s *StdinSource) IsFileReference() bool {
	return s != nil && s.Kind == StdinSourceKindFileReference
}

// CapArgumentValue is a unified argument type — arguments are identified by media_urn.
// The cap definition's sources specify how to extract values (stdin, position, cli_flag).
type CapArgumentValue struct {
	// MediaUrn is the semantic identifier, e.g., "media:model-spec;textable"
	MediaUrn string
	// Value is the argument bytes (UTF-8 for text, raw for binary)
	Value []byte
}

// NewCapArgumentValue creates a new CapArgumentValue
func NewCapArgumentValue(mediaUrn string, value []byte) CapArgumentValue {
	return CapArgumentValue{
		MediaUrn: mediaUrn,
		Value:    value,
	}
}

// NewCapArgumentValueFromStr creates a new CapArgumentValue from a string value
func NewCapArgumentValueFromStr(mediaUrn string, value string) CapArgumentValue {
	return CapArgumentValue{
		MediaUrn: mediaUrn,
		Value:    []byte(value),
	}
}

// ValueAsStr returns the value as a UTF-8 string. Returns error for non-UTF-8 data.
func (a *CapArgumentValue) ValueAsStr() (string, error) {
	if !utf8.Valid(a.Value) {
		return "", fmt.Errorf("value contains invalid UTF-8 data")
	}
	return string(a.Value), nil
}

// String returns a string representation of the CapArgumentValue for debugging
func (a *CapArgumentValue) String() string {
	if utf8.Valid(a.Value) {
		return fmt.Sprintf("CapArgumentValue{MediaUrn: %q, Value: %q}", a.MediaUrn, string(a.Value))
	}
	return fmt.Sprintf("CapArgumentValue{MediaUrn: %q, Value: %d bytes}", a.MediaUrn, len(a.Value))
}

// CapResultKind identifies the variant of a CapResult.
type CapResultKind int

const (
	// CapResultKindScalar represents raw materialized bytes (scalar output).
	CapResultKindScalar CapResultKind = iota
	// CapResultKindList represents individual CBOR values (list output).
	CapResultKindList
	// CapResultKindEmpty represents no output (void cap).
	CapResultKindEmpty
)

// CapResult is the result from a cap execution.
//
// Scalar outputs carry raw materialized bytes (e.g. UTF-8 text, raw binary).
// List outputs carry a CBOR sequence of values, one per list item.
// Empty represents a void cap with no output.
type CapResult struct {
	Kind   CapResultKind
	Scalar []byte
	List   []byte // CBOR sequence of list items
}

// NewCapResultScalar creates a CapResult carrying raw bytes (scalar output).
func NewCapResultScalar(data []byte) CapResult {
	return CapResult{Kind: CapResultKindScalar, Scalar: data}
}

// NewCapResultList creates a CapResult carrying a CBOR sequence (list output).
func NewCapResultList(cborSequence []byte) CapResult {
	return CapResult{Kind: CapResultKindList, List: cborSequence}
}

// NewCapResultEmpty creates a CapResult for void caps.
func NewCapResultEmpty() CapResult {
	return CapResult{Kind: CapResultKindEmpty}
}
