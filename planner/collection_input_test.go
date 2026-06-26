package planner

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST716: Tests CapInputCollection empty collection has zero files and folders
// Verifies is_empty() returns true and counts are zero for new collection
func Test716_empty_collection(t *testing.T) {
	collection := NewCapInputCollection("folder-123", "Test Folder")
	assert.True(t, collection.IsEmpty())
	assert.Equal(t, 0, collection.TotalFileCount())
	assert.Equal(t, 0, collection.TotalFolderCount())
}

// TEST717: Tests CapInputCollection correctly counts files in flat collection
// Verifies total_file_count() returns 2 for collection with 2 files, no folders
func Test717_collection_with_files(t *testing.T) {
	collection := NewCapInputCollection("folder-123", "Test Folder")
	collection.Files = append(collection.Files, NewCollectionFile("listing-1", "/path/to/file1.pdf", "media:ext=pdf"))
	collection.Files = append(collection.Files, NewCollectionFile("listing-2", "/path/to/file2.md", "media:enc=utf-8;ext=md"))

	assert.False(t, collection.IsEmpty())
	assert.Equal(t, 2, collection.TotalFileCount())
	assert.Equal(t, 0, collection.TotalFolderCount())
}

// TEST718: Tests CapInputCollection correctly counts files and folders in nested structure
// Verifies total_file_count() includes subfolder files and total_folder_count() counts subfolders
func Test718_nested_collection(t *testing.T) {
	root := NewCapInputCollection("folder-root", "Root")
	root.Files = append(root.Files, NewCollectionFile("listing-1", "/path/file1.pdf", "media:ext=pdf"))

	subfolder := NewCapInputCollection("folder-sub", "Subfolder")
	subfolder.Files = append(subfolder.Files, NewCollectionFile("listing-2", "/path/sub/file2.pdf", "media:ext=pdf"))
	subfolder.Files = append(subfolder.Files, NewCollectionFile("listing-3", "/path/sub/file3.pdf", "media:ext=pdf"))

	root.Folders["Subfolder"] = subfolder

	assert.Equal(t, 3, root.TotalFileCount())
	assert.Equal(t, 1, root.TotalFolderCount())
}

// TEST719: Tests CapInputCollection flatten_to_files recursively collects all files
// Verifies flatten() extracts files from root and all subfolders into flat list
func Test719_flatten_to_files(t *testing.T) {
	root := NewCapInputCollection("folder-root", "Root")
	root.Files = append(root.Files, NewCollectionFile("listing-1", "/path/file1.pdf", "media:ext=pdf"))

	subfolder := NewCapInputCollection("folder-sub", "Subfolder")
	subfolder.Files = append(subfolder.Files, NewCollectionFile("listing-2", "/path/sub/file2.pdf", "media:ext=pdf"))

	root.Folders["Subfolder"] = subfolder

	flattened := root.FlattenToFiles()
	assert.Equal(t, 2, len(flattened))
	// Root file comes first
	assert.Equal(t, "/path/file1.pdf", flattened[0].FilePath)
	// Subfolder file comes second
	assert.Equal(t, "/path/sub/file2.pdf", flattened[1].FilePath)
}

// TEST933: CapInputCollection serializes to JSON and deserializes back preserving all fields
// Verifies JSON round-trip preserves folder_id, folder_name, files and file metadata.
func Test933_serialization_roundtrip(t *testing.T) {
	collection := NewCapInputCollection("folder-123", "Test Folder")
	file := NewCollectionFile("listing-1", "/path/to/file.pdf", "media:ext=pdf")
	file.Title = "My Document"
	collection.Files = append(collection.Files, file)

	jsonBytes := collection.ToJSON()
	require.NotNil(t, jsonBytes)

	var roundtrip CapInputCollection
	err := json.Unmarshal(jsonBytes, &roundtrip)
	require.NoError(t, err)

	assert.Equal(t, collection.FolderID, roundtrip.FolderID)
	assert.Equal(t, collection.FolderName, roundtrip.FolderName)
	require.Len(t, roundtrip.Files, 1)
	assert.Equal(t, "My Document", roundtrip.Files[0].Title)
}
