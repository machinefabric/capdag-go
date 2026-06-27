// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

// DiscriminateCandidatesByValidation discriminates candidate media URNs by the
// validation rules in their specs.
//
// Given file content and a set of candidate URN strings (e.g. all URNs for a
// file extension), eliminates candidates whose media def validation rules reject
// the content. Candidates with no validation rules survive (no rules = no basis
// for elimination). Candidates more specific than the baseline must have
// validation rules that positively match the content — otherwise they're
// eliminated (they overclaim without proof). Candidates equivalent to or less
// specific than the baseline survive without validation.
//
// Returns the surviving candidate URN strings in their original order.
func DiscriminateCandidatesByValidation(content []byte, candidateUrns []string, fabricRegistry *media.FabricRegistry, baselineUrn string) []string {
	var contentStr string
	hasContentStr := utf8.Valid(content)
	if hasContentStr {
		contentStr = string(content)
	}
	contentLen := len(content)

	baseline, err := urn.NewMediaUrnFromString(baselineUrn)
	if err != nil {
		panic(fmt.Sprintf("DiscriminateCandidatesByValidation: invalid baseline URN '%s': %v", baselineUrn, err))
	}

	var survivors []string
	for _, u := range candidateUrns {
		spec := fabricRegistry.GetCachedMediaDef(u)
		if spec == nil {
			// No spec in cache → cannot eliminate.
			survivors = append(survivors, u)
			continue
		}

		validation := spec.Validation
		if validation == nil || isEmptyValidation(validation) {
			// No validation rules. Only keep if the candidate is not more
			// specific than the baseline (more specific without validation =
			// overclaiming). Keep if baseline conforms to candidate.
			candidateUrn, perr := urn.NewMediaUrnFromString(u)
			if perr != nil {
				survivors = append(survivors, u) // Can't parse → keep.
				continue
			}
			if baseline.ConformsTo(candidateUrn) {
				survivors = append(survivors, u)
			}
			continue
		}

		if !validationPasses(validation, contentStr, hasContentStr, contentLen) {
			continue
		}

		survivors = append(survivors, u)
	}

	return survivors
}

// isEmptyValidation reports whether a validation block carries no actual rules.
func isEmptyValidation(v *media.MediaValidation) bool {
	return v.Min == nil && v.Max == nil && v.MinLength == nil && v.MaxLength == nil &&
		v.Pattern == nil && len(v.AllowedValues) == 0
}

// validationPasses applies the validation rules against the content.
func validationPasses(validation *media.MediaValidation, contentStr string, hasContentStr bool, contentLen int) bool {
	// Check pattern (regex against content as UTF-8).
	if validation.Pattern != nil {
		if hasContentStr {
			re, reErr := regexp.Compile(*validation.Pattern)
			if reErr == nil {
				if !re.MatchString(contentStr) {
					return false // Pattern didn't match → eliminate.
				}
			}
			// Invalid regex in spec is a spec authoring bug — keep the
			// candidate rather than eliminate on a broken rule.
		} else {
			// Binary content cannot match a text pattern → eliminate.
			return false
		}
	}

	// Check min_length (byte length).
	if validation.MinLength != nil {
		if contentLen < *validation.MinLength {
			return false
		}
	}

	// Check max_length (byte length).
	if validation.MaxLength != nil {
		if contentLen > *validation.MaxLength {
			return false
		}
	}

	// Check allowed_values.
	if len(validation.AllowedValues) > 0 {
		if !hasContentStr {
			return false // Binary content can't match allowed text values.
		}
		trimmed := strings.TrimSpace(contentStr)
		found := false
		for _, v := range validation.AllowedValues {
			if v == trimmed {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// =============================================================================
// SYNCHRONOUS EXTENSION-BASED DETECTION (preliminary, for UI queries)
// =============================================================================

// ResolveInput resolves a single input item using the supplied FabricRegistry
// for extension lookups.
func ResolveInput(item InputItem, fabricRegistry *media.FabricRegistry) (*ResolvedInputSet, error) {
	return ResolveInputs([]InputItem{item}, fabricRegistry)
}

// ResolveInputs resolves multiple input items using the supplied FabricRegistry
// for extension lookups.
func ResolveInputs(items []InputItem, fabricRegistry *media.FabricRegistry) (*ResolvedInputSet, error) {
	paths, err := ResolveItems(items)
	if err != nil {
		return nil, err
	}

	files := make([]ResolvedFile, 0, len(paths))
	for _, path := range paths {
		resolved, derr := detectFileByExtensionWithRegistry(path, fabricRegistry)
		if derr != nil {
			return nil, derr
		}
		files = append(files, *resolved)
	}

	if len(files) == 0 {
		return nil, NoFilesResolvedError()
	}

	return NewResolvedInputSet(files), nil
}

// ResolvePaths is a convenience: resolve from string paths (auto-detect
// file/dir/glob) using the supplied FabricRegistry for extension lookups.
func ResolvePaths(paths []string, fabricRegistry *media.FabricRegistry) (*ResolvedInputSet, error) {
	items := make([]InputItem, 0, len(paths))
	for _, s := range paths {
		items = append(items, FromString(s))
	}
	return ResolveInputs(items, fabricRegistry)
}

// detectFileByExtensionWithRegistry performs extension-based detection using a
// specific FabricRegistry.
func detectFileByExtensionWithRegistry(path string, fabricRegistry *media.FabricRegistry) (*ResolvedFile, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, NotFoundError(path)
		}
		if os.IsPermission(err) {
			return nil, PermissionDeniedError(path)
		}
		return nil, IoError(path, err)
	}

	sizeBytes := uint64(info.Size())

	ext := extensionLower(path)

	mediaUrn := "media:"
	contentStructure := ScalarOpaque

	if ext != "" {
		urns, lerr := fabricRegistry.MediaUrnsForExtension(ext)
		if lerr == nil && len(urns) > 0 {
			// Parse and pick the most specific candidate.
			var bestUrn *urn.MediaUrn
			var bestStr string
			for _, urnStr := range urns {
				u, perr := urn.NewMediaUrnFromString(urnStr)
				if perr != nil {
					continue
				}
				if bestUrn == nil || u.Specificity() > bestUrn.Specificity() {
					bestUrn = u
					bestStr = urnStr
				}
			}
			if bestUrn != nil {
				mediaUrn = bestStr
				contentStructure = structureFromMarkerTags(bestUrn)
			}
		}
	}

	return &ResolvedFile{
		Path:             path,
		MediaUrn:         mediaUrn,
		SizeBytes:        sizeBytes,
		ContentStructure: contentStructure,
	}, nil
}

// extensionLower returns the lowercased file extension (without the dot), or "".
func extensionLower(path string) string {
	ext := filepath.Ext(path)
	if ext == "" {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(ext, "."))
}

// =============================================================================
// ASYNC CARTRIDGE-CONFIRMED DETECTION
// =============================================================================

// DetectFileConfirmed detects the media type for a file with cartridge adapter
// confirmation.
//
// This is the full detection flow:
//  1. Extension lookup → candidate URNs
//  2. Find registered adapters for those candidates
//  3. Invoke adapter-selection cap on each matched cartridge
//  4. Select most specific confirmed URN
//
// Fails hard if no adapters are registered, if all cartridges return no match,
// or if the response is invalid.
func DetectFileConfirmed(ctx context.Context, path string, adapterRegistry *MediaAdapterRegistry, invoker CartridgeAdapterInvoker) (*ResolvedFile, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, NotFoundError(path)
		}
		if os.IsPermission(err) {
			return nil, PermissionDeniedError(path)
		}
		return nil, IoError(path, err)
	}

	sizeBytes := uint64(info.Size())

	// Step 1: Extension lookup.
	extStr := extensionLower(path)

	// Step 2: Find adapters.
	adapters := adapterRegistry.FindAdaptersForExtension(extStr)

	if len(adapters) == 0 {
		return nil, InspectionFailedError(path, fmt.Sprintf(
			"No content-inspection adapter registered for extension '.%s'. "+
				"A cartridge must register an adapter for this file type.",
			extStr,
		))
	}

	// Step 3: Invoke each cartridge's adapter-selection cap.
	type returnedUrn struct {
		urnStr      string
		cartridgeID string
	}
	var allReturnedUrns []returnedUrn

	for _, a := range adapters {
		mediaUrns, ierr := invoker.InvokeAdapterSelection(ctx, a.CartridgeID, path)
		if ierr != nil {
			return nil, ierr
		}
		for _, urnStr := range mediaUrns {
			allReturnedUrns = append(allReturnedUrns, returnedUrn{urnStr: urnStr, cartridgeID: a.CartridgeID})
		}
	}

	// Step 4: All cartridges returned empty END — none matched.
	if len(allReturnedUrns) == 0 {
		adapterNames := make([]string, 0, len(adapters))
		for _, a := range adapters {
			adapterNames = append(adapterNames, a.CartridgeID)
		}
		return nil, InspectionFailedError(path, fmt.Sprintf(
			"All registered adapters returned no match (extension '.%s'). "+
				"Adapters consulted: %s. The file content does not match any registered media type.",
			extStr,
			formatStringSlice(adapterNames),
		))
	}

	// Step 5: Validate and parse returned URNs.
	type parsedUrn struct {
		u           *urn.MediaUrn
		urnStr      string
		cartridgeID string
	}
	parsedUrns := make([]parsedUrn, 0, len(allReturnedUrns))
	for _, ru := range allReturnedUrns {
		u, perr := urn.NewMediaUrnFromString(ru.urnStr)
		if perr != nil {
			return nil, InspectionFailedError(path, fmt.Sprintf(
				"Cartridge '%s' returned invalid media URN '%s': %v",
				ru.cartridgeID, ru.urnStr, perr,
			))
		}
		parsedUrns = append(parsedUrns, parsedUrn{u: u, urnStr: ru.urnStr, cartridgeID: ru.cartridgeID})
	}

	// Step 6: Select by specificity.
	bestIdx := 0
	for i := 1; i < len(parsedUrns); i++ {
		if parsedUrns[i].u.Specificity() > parsedUrns[bestIdx].u.Specificity() {
			bestIdx = i
		}
	}

	// Check for ties at the same specificity.
	bestSpecificity := parsedUrns[bestIdx].u.Specificity()
	var ties []*parsedUrn
	for i := range parsedUrns {
		if parsedUrns[i].u.Specificity() == bestSpecificity {
			ties = append(ties, &parsedUrns[i])
		}
	}

	if len(ties) > 1 {
		// Check if one conforms to the other (which would make it not a real tie).
		var realTies []*parsedUrn
		for _, tie := range ties {
			dominated := false
			for _, other := range ties {
				if tie != other && tie.u.ConformsTo(other.u) {
					dominated = true
					break
				}
			}
			if !dominated {
				realTies = append(realTies, tie)
			}
		}

		if len(realTies) > 1 {
			tieDescs := make([]string, 0, len(realTies))
			for _, t := range realTies {
				tieDescs = append(tieDescs, fmt.Sprintf("'%s' (from cartridge '%s')", t.urnStr, t.cartridgeID))
			}
			return nil, InspectionFailedError(path, fmt.Sprintf(
				"Ambiguous adapter selection: multiple adapters returned URNs "+
					"at the same specificity level with no conformance relationship: %s. "+
					"This indicates a registration conflict that should have been caught "+
					"at cap group registration time.",
				strings.Join(tieDescs, ", "),
			))
		}
	}

	selected := parsedUrns[bestIdx]
	contentStructure := structureFromMarkerTags(selected.u)

	return &ResolvedFile{
		Path:             path,
		MediaUrn:         selected.urnStr,
		SizeBytes:        sizeBytes,
		ContentStructure: contentStructure,
	}, nil
}

// ResolveInputsConfirmed resolves multiple input items with cartridge-confirmed
// detection.
func ResolveInputsConfirmed(ctx context.Context, items []InputItem, adapterRegistry *MediaAdapterRegistry, invoker CartridgeAdapterInvoker) (*ResolvedInputSet, error) {
	paths, err := ResolveItems(items)
	if err != nil {
		return nil, err
	}

	files := make([]ResolvedFile, 0, len(paths))
	for _, path := range paths {
		resolved, derr := DetectFileConfirmed(ctx, path, adapterRegistry, invoker)
		if derr != nil {
			return nil, derr
		}
		files = append(files, *resolved)
	}

	if len(files) == 0 {
		return nil, NoFilesResolvedError()
	}

	return NewResolvedInputSet(files), nil
}

// =============================================================================
// HELPERS
// =============================================================================

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

// formatStringSlice renders a string slice the way Rust's {:?} debug formats a
// Vec<&str>, e.g. ["a", "b"]. Used for error-message parity.
func formatStringSlice(items []string) string {
	quoted := make([]string, 0, len(items))
	for _, s := range items {
		quoted = append(quoted, fmt.Sprintf("%q", s))
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}
