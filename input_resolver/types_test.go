package input_resolver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TEST1143: InputItem::from_string distinguishes glob patterns, directories, and files
func Test1143_InputItemFromStringDistinguishesGlobDirectoryAndFile(t *testing.T) {
	// Existing directory
	dir := t.TempDir()
	dirItem := FromString(dir)
	if dirItem.Kind != InputItemDirectory {
		t.Fatalf("expected Directory for existing dir %q, got kind %d", dir, dirItem.Kind)
	}
	if dirItem.Path != dir {
		t.Fatalf("expected path %q, got %q", dir, dirItem.Path)
	}

	// Non-existent file path
	filePath := filepath.Join(dir, "missing.txt")
	fileItem := FromString(filePath)
	if fileItem.Kind != InputItemFile {
		t.Fatalf("expected File for non-existent path %q, got kind %d", filePath, fileItem.Kind)
	}
	if fileItem.Path != filePath {
		t.Fatalf("expected path %q, got %q", filePath, fileItem.Path)
	}

	// Glob pattern
	globItem := FromString("fixtures/**/*.pdf")
	if globItem.Kind != InputItemGlob {
		t.Fatalf("expected Glob for pattern, got kind %d", globItem.Kind)
	}
	if globItem.Pattern != "fixtures/**/*.pdf" {
		t.Fatalf("expected pattern %q, got %q", "fixtures/**/*.pdf", globItem.Pattern)
	}
}

// TEST1144: ContentStructure is_list/is_record helpers and Display implementation are correct
func Test1144_ContentStructureHelpersAndDisplay(t *testing.T) {
	if ScalarOpaque.IsList() {
		t.Error("ScalarOpaque.IsList() must be false")
	}
	if ScalarOpaque.IsRecord() {
		t.Error("ScalarOpaque.IsRecord() must be false")
	}
	if ScalarOpaque.String() != "scalar/opaque" {
		t.Errorf("ScalarOpaque.String() = %q, want %q", ScalarOpaque.String(), "scalar/opaque")
	}

	if !ListRecord.IsList() {
		t.Error("ListRecord.IsList() must be true")
	}
	if !ListRecord.IsRecord() {
		t.Error("ListRecord.IsRecord() must be true")
	}
	if ListRecord.String() != "list/record" {
		t.Errorf("ListRecord.String() = %q, want %q", ListRecord.String(), "list/record")
	}

	if !ListOpaque.IsList() {
		t.Error("ListOpaque.IsList() must be true")
	}
	if ListOpaque.IsRecord() {
		t.Error("ListOpaque.IsRecord() must be false")
	}

	if ScalarRecord.IsList() {
		t.Error("ScalarRecord.IsList() must be false")
	}
	if !ScalarRecord.IsRecord() {
		t.Error("ScalarRecord.IsRecord() must be true")
	}
}

// TEST1145: ResolvedInputSet uses URN equivalence for common_media and file count for is_sequence
func Test1145_ResolvedInputSetUsesEquivalentMediaAndFileCountCardinality(t *testing.T) {
	// Single list file — is_sequence=false (only 1 file), but has list marker
	singleListFile := NewResolvedInputSet([]ResolvedFile{
		{
			Path:             "/tmp/items.json",
			MediaUrn:         "media:application;fmt=json;list;record",
			SizeBytes:        42,
			ContentStructure: ListRecord,
		},
	})
	if singleListFile.IsSequence {
		t.Error("single list file must NOT be a sequence (is_sequence is count-based, not structure-based)")
	}
	if !singleListFile.IsHomogeneous() {
		t.Error("single file must be homogeneous")
	}
	if singleListFile.CommonMedia == nil || *singleListFile.CommonMedia != "media:application;fmt=json;list;record" {
		t.Errorf("expected CommonMedia %q, got %v", "media:application;fmt=json;list;record", singleListFile.CommonMedia)
	}

	// Two files with equivalent URNs (different tag order) — is_sequence=true, homogeneous
	equivalentOrdering := NewResolvedInputSet([]ResolvedFile{
		{
			Path:             "/tmp/a.json",
			MediaUrn:         "media:application;fmt=json;record",
			SizeBytes:        10,
			ContentStructure: ScalarRecord,
		},
		{
			Path:             "/tmp/b.json",
			MediaUrn:         "media:application;record;fmt=json",
			SizeBytes:        11,
			ContentStructure: ScalarRecord,
		},
	})
	if !equivalentOrdering.IsSequence {
		t.Error("two files must be a sequence")
	}
	if !equivalentOrdering.IsHomogeneous() {
		t.Error("equivalent URNs must be homogeneous")
	}
	if equivalentOrdering.CommonMedia == nil || *equivalentOrdering.CommonMedia != "media:application;fmt=json;record" {
		t.Errorf("expected CommonMedia to be first file's URN, got %v", equivalentOrdering.CommonMedia)
	}
}

// TEST1146: InputResolverError Display and source() implementations produce correct messages
func Test1146_InputResolverErrorDisplayAndSource(t *testing.T) {
	ioErr := IoError("/tmp/data.bin", os.ErrPermission)
	if !strings.Contains(ioErr.Error(), "IO error at /tmp/data.bin") {
		t.Errorf("IoError message wrong: %q", ioErr.Error())
	}
	if ioErr.Source() == nil {
		t.Error("IoError.Source() must be non-nil")
	}

	invalidGlob := InvalidGlobError("[", "unclosed character class")
	if invalidGlob.Error() != "Invalid glob pattern \"[\": unclosed character class" {
		t.Errorf("InvalidGlob message wrong: %q", invalidGlob.Error())
	}
	if invalidGlob.Source() != nil {
		t.Error("InvalidGlob.Source() must be nil")
	}

	notFound := NotFoundError("/no/such/file.txt")
	if !strings.Contains(notFound.Error(), "Path not found: /no/such/file.txt") {
		t.Errorf("NotFound message wrong: %q", notFound.Error())
	}
	if notFound.Source() != nil {
		t.Error("NotFound.Source() must be nil")
	}
}
