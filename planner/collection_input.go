package planner

import "encoding/json"

const collectionMediaUrn = "media:collection;enc=utf-8;record"

// CollectionFile is a single file within a collection folder.
type CollectionFile struct {
	ListingID        string `json:"listing_id"`
	FilePath         string `json:"file_path"`
	MediaUrn         string `json:"media_urn"`
	Title            string `json:"title,omitempty"`
	SecurityBookmark []byte `json:"-"` // runtime-only, never serialized
}

// NewCollectionFile creates a CollectionFile with required fields.
func NewCollectionFile(listingID, filePath, mediaUrn string) *CollectionFile {
	return &CollectionFile{
		ListingID: listingID,
		FilePath:  filePath,
		MediaUrn:  mediaUrn,
	}
}

// WithTitle sets the title (builder pattern).
func (f *CollectionFile) WithTitle(title string) *CollectionFile {
	f.Title = title
	return f
}

// WithSecurityBookmark sets the security bookmark (builder pattern).
func (f *CollectionFile) WithSecurityBookmark(bookmark []byte) *CollectionFile {
	f.SecurityBookmark = bookmark
	return f
}

// CapInputCollection is a recursive folder hierarchy for collection-type cap inputs.
type CapInputCollection struct {
	FolderID   string                        `json:"folder_id"`
	FolderName string                        `json:"folder_name"`
	Files      []*CollectionFile             `json:"files"`
	Folders    map[string]*CapInputCollection `json:"folders"`
	MediaUrn   string                        `json:"media_urn"`
}

// NewCapInputCollection creates an empty collection with required fields.
func NewCapInputCollection(folderID, folderName string) *CapInputCollection {
	return &CapInputCollection{
		FolderID:   folderID,
		FolderName: folderName,
		Files:      make([]*CollectionFile, 0),
		Folders:    make(map[string]*CapInputCollection),
		MediaUrn:   collectionMediaUrn,
	}
}

// ToJSON serializes the entire tree to a json.RawMessage.
func (c *CapInputCollection) ToJSON() json.RawMessage {
	data, err := json.Marshal(c)
	if err != nil {
		panic("CapInputCollection serialization failed: " + err.Error())
	}
	return data
}

// FlattenToFiles recursively collects all files into a flat slice of CapInputFile.
func (c *CapInputCollection) FlattenToFiles() []*CapInputFile {
	var result []*CapInputFile
	c.collectFilesRecursive(&result)
	return result
}

// TotalFileCount recursively counts all files in this node and all descendants.
func (c *CapInputCollection) TotalFileCount() int {
	count := len(c.Files)
	for _, subfolder := range c.Folders {
		count += subfolder.TotalFileCount()
	}
	return count
}

// TotalFolderCount recursively counts all subfolder entries (not including self).
func (c *CapInputCollection) TotalFolderCount() int {
	count := len(c.Folders)
	for _, subfolder := range c.Folders {
		count += subfolder.TotalFolderCount()
	}
	return count
}

// IsEmpty returns true if this node has no direct files and no subfolders (shallow check).
func (c *CapInputCollection) IsEmpty() bool {
	return len(c.Files) == 0 && len(c.Folders) == 0
}

func (c *CapInputCollection) collectFilesRecursive(result *[]*CapInputFile) {
	for _, cf := range c.Files {
		st := SourceListing
		inputFile := &CapInputFile{
			FilePath:   cf.FilePath,
			MediaUrn:   cf.MediaUrn,
			SourceID:   &cf.ListingID,
			SourceType: &st,
		}
		if cf.SecurityBookmark != nil {
			inputFile.SecurityBookmark = cf.SecurityBookmark
		}
		*result = append(*result, inputFile)
	}

	for _, subfolder := range c.Folders {
		subfolder.collectFilesRecursive(result)
	}
}
