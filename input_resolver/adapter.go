// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

import "context"

// MaxContentInspectionBytes is the maximum number of bytes of file content sent
// to a cartridge for adapter (content-inspection) selection.
//
// This is the single source of truth for the inspection prefix size. All paths
// that hand bytes to a content-inspection adapter must read at most this many
// bytes so cartridge handlers and the engine's pattern validators see exactly
// the same prefix.
const MaxContentInspectionBytes = 100 * 1024

// AdapterResult is the result of adapter detection — a selected media URN and
// its structure.
type AdapterResult struct {
	// MediaUrn is the selected media URN.
	MediaUrn string
	// ContentStructure is the detected content structure.
	ContentStructure ContentStructure
}

// CartridgeAdapterInvoker invokes the adapter-selection cap on a specific cartridge.
//
// The implementation lives on the host side where it has access to the cartridge
// process/relay infrastructure. capdag defines the interface; the host implements it.
type CartridgeAdapterInvoker interface {
	// InvokeAdapterSelection invokes the adapter-selection cap on a specific
	// cartridge by ID.
	//
	// Returns:
	//   - (nil, nil) for empty END frame (no match — cartridge doesn't handle this file)
	//   - (mediaUrns, nil) for a successful detection with one or more media URNs
	//   - (_, err) for protocol errors, invalid responses, or infrastructure failures
	InvokeAdapterSelection(ctx context.Context, cartridgeID, filePath string) ([]string, error)
}
