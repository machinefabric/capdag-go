// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

// Resolving a run's inputs — files, directories and globs — to the files they
// name and what each of them is.
//
// What a file is comes from the one detection every host runs
// (DetectFileMedia); this file adds only the path resolution around it and
// the shape of the result.
//
// Matches Rust: capdag::input_resolver::resolver

import (
	"context"

	"github.com/machinefabric/capdag-go/fabric"
	"github.com/machinefabric/capdag-go/urn"
)

// Detection is how a host detects its inputs: its fabric, the cartridges it
// can ask, and its value adapters. A host with no cartridges sets Adapters to
// NoAdapters{} and Invoker to NoInvoker{}.
type Detection struct {
	Registry      *fabric.FabricRegistry
	Adapters      AdapterSource
	Invoker       CartridgeAdapterInvoker
	ValueAdapters *ValueAdapterRegistry
}

// ResolveFile is what one file is, as a run's input: the media a host that
// cannot ask takes (PrimaryMedia).
func ResolveFile(ctx context.Context, path string, detection *Detection) (*ResolvedFile, error) {
	found, err := DetectFileMedia(ctx, path, detection.Registry, detection.Adapters, detection.Invoker, detection.ValueAdapters)
	if err != nil {
		return nil, err
	}
	mediaUrn := found.Primary()
	u, err := urn.NewMediaUrnFromString(mediaUrn)
	if err != nil {
		return nil, InspectionFailedError(path, "'"+mediaUrn+"' is no media URN: "+err.Error())
	}
	return &ResolvedFile{
		Path:             found.Path,
		MediaUrn:         mediaUrn,
		SizeBytes:        found.SizeBytes,
		ContentStructure: structureFromMarkerTags(u),
	}, nil
}

// ResolveInput resolves one input item.
func ResolveInput(ctx context.Context, item InputItem, detection *Detection) (*ResolvedInputSet, error) {
	return ResolveInputs(ctx, []InputItem{item}, detection)
}

// ResolveInputs resolves input items to the files they name and what each is.
func ResolveInputs(ctx context.Context, items []InputItem, detection *Detection) (*ResolvedInputSet, error) {
	paths, err := ResolveItems(items)
	if err != nil {
		return nil, err
	}

	files := make([]ResolvedFile, 0, len(paths))
	for _, path := range paths {
		resolved, err := ResolveFile(ctx, path, detection)
		if err != nil {
			return nil, err
		}
		files = append(files, *resolved)
	}

	if len(files) == 0 {
		return nil, NoFilesResolvedError()
	}

	return NewResolvedInputSet(files), nil
}

// ResolvePaths resolves string paths (a file, a directory or a glob each).
func ResolvePaths(ctx context.Context, paths []string, detection *Detection) (*ResolvedInputSet, error) {
	items := make([]InputItem, 0, len(paths))
	for _, s := range paths {
		items = append(items, FromString(s))
	}
	return ResolveInputs(ctx, items, detection)
}

// structureFromMarkerTags determines content structure from a MediaUrn's marker tags.
func structureFromMarkerTags(u *urn.MediaUrn) ContentStructure {
	hasList := u.HasMarkerTag("list")
	hasRecord := u.HasMarkerTag("record")

	switch {
	case hasList && hasRecord:
		return ListRecord
	case hasList && !hasRecord:
		return ListOpaque
	case !hasList && hasRecord:
		return ScalarRecord
	default:
		return ScalarOpaque
	}
}
