// Package input_resolver provides types for resolving user-specified input paths.
package input_resolver

// What a file is: the one detection every host runs.
//
// A file's extension names the file media types the fabric declares for it
// (its candidates: every definition in the pinned manifest that carries the
// `ext=` file-type tag and lists the extension,
// fabric.FabricRegistry.FileMediaForExtension). Its content decides between
// them, by the proved model (capdag/formal/CapDAG/Detect.lean, generated into
// package formal):
//
//   - a candidate whose definition declares validation rules the content fails
//     never stands;
//   - a candidate under a cartridge's content-inspection adapter stands only if
//     a consulted cartridge claims the file as it or as something more specific
//     (the adapter speaks for its whole domain);
//   - any other candidate stands if the content met rules it declares, or if it
//     is a base type of the extension: no other candidate is more general. A
//     refinement (digitized text of a `.txt`, a JSON record of a `.json`) is a
//     claim about the content, and an unproven claim is never made.
//
// What stands is refined by the value adapters; each line of refinement then
// keeps only its most specific member. A `list` tag stays: it says what the
// content is (a CSV file is a list of records), never how many values are in
// transit, which is the planner's IsSequence. What is left is what the file
// is; more than one is a genuine alternative a host may offer. PrimaryMedia
// picks the one a host that cannot ask uses.
//
// What this file does itself is the I/O the model is handed: reading the
// content, evaluating each definition's rules against it, and asking the
// cartridges.
//
// Matches Rust: capdag::input_resolver::detection

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/machinefabric/capdag-go/fabric"
	model "github.com/machinefabric/capdag-go/formal"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
	taggedformal "github.com/machinefabric/tagged-urn-go/formal"
)

// fileMediaCandidate is one file media type a file's extension names, with
// the validation rules its definition declares.
type fileMediaCandidate struct {
	urn        *urn.MediaUrn
	validation *media.MediaValidation
}

// adapterVerdict is the cartridges whose content-inspection adapters cover
// some of a file's candidates, and what each of them claims the file is.
type adapterVerdict struct {
	// owners are the adapter URNs of every cartridge consulted.
	owners []*urn.MediaUrn
	// claimed is every media URN a consulted cartridge claimed the file as.
	claimed []*urn.MediaUrn
}

// AdapterSource is where a host's content-inspection adapters come from: which
// cartridges to consult for a file whose candidates these are.
type AdapterSource interface {
	AdaptersFor(candidates []*urn.MediaUrn) []AdapterMatch
}

// NoAdapters is a host with no content-inspection adapters.
type NoAdapters struct{}

// AdaptersFor is no adapter, whatever the candidates.
func (NoAdapters) AdaptersFor(_ []*urn.MediaUrn) []AdapterMatch {
	return nil
}

// NoInvoker is a host that asks no cartridge: it has none to ask. It is only
// ever paired with NoAdapters, so it is never asked; being asked is a defect
// of the host and panics naming the cartridge.
type NoInvoker struct{}

// InvokeAdapterSelection panics: a host with no adapters asks no cartridge.
func (NoInvoker) InvokeAdapterSelection(_ context.Context, cartridgeID, _ string) ([]string, error) {
	panic(fmt.Sprintf("a host with no adapters was asked to consult cartridge '%s'", cartridgeID))
}

// FileMedia is what a file is.
type FileMedia struct {
	Path      string
	SizeBytes uint64
	// Extension is the file's extension, lowercased.
	Extension string
	// Candidates is every file media type the fabric declares for the
	// extension.
	Candidates []string
	// Media is what the file is: one entry, or several genuine alternatives, in
	// the candidates' order.
	Media []string
}

// Primary is the one media a host that cannot ask uses (PrimaryMedia).
func (f *FileMedia) Primary() string {
	primary, ok, err := PrimaryMedia(f.Media)
	if err != nil {
		panic(fmt.Sprintf("every media of a FileMedia is a media URN it parsed: %v", err))
	}
	if !ok {
		panic("a FileMedia always holds at least one media")
	}
	return primary
}

// decided is the model's answer, or a panic: a call into the generated program
// fails only when the runtime does, never on a URN this package parsed.
func decided[T any](answer T, err error) T {
	if err != nil {
		panic(fmt.Sprintf("input_resolver: the model could not decide: %v", err))
	}
	return answer
}

// formalOf is the URNs as the model takes them.
func formalOf(urns []*urn.MediaUrn) []taggedformal.Wf {
	out := make([]taggedformal.Wf, 0, len(urns))
	for _, u := range urns {
		out = append(out, u.Formal())
	}
	return out
}

// meets reports whether the content meets the rules. An invalid pattern is a
// defect of the definition and an error naming it.
func meets(content []byte, u *urn.MediaUrn, rules *media.MediaValidation) (bool, error) {
	isText := utf8.Valid(content)
	text := ""
	if isText {
		text = string(content)
	}
	if rules.Pattern != nil {
		re, err := regexp.Compile(*rules.Pattern)
		if err != nil {
			return false, fmt.Errorf("the media definition '%s' declares an invalid validation pattern '%s': %v",
				u.String(), *rules.Pattern, err)
		}
		if !isText || !re.MatchString(text) {
			return false, nil
		}
	}
	if rules.MinLength != nil && len(content) < *rules.MinLength {
		return false, nil
	}
	if rules.MaxLength != nil && len(content) > *rules.MaxLength {
		return false, nil
	}
	if rules.AllowedValues != nil {
		if !isText {
			return false, nil
		}
		trimmed := strings.TrimSpace(text)
		allowed := false
		for _, v := range rules.AllowedValues {
			if v == trimmed {
				allowed = true
				break
			}
		}
		if !allowed {
			return false, nil
		}
	}
	if rules.Min != nil || rules.Max != nil {
		if !isText {
			return false, nil
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
		if err != nil {
			return false, nil
		}
		if (rules.Min != nil && value < *rules.Min) || (rules.Max != nil && value > *rules.Max) {
			return false, nil
		}
	}
	return true, nil
}

// declaresRules reports whether a definition's validation declares any rule.
func declaresRules(rules *media.MediaValidation) bool {
	return rules != nil && (rules.Pattern != nil ||
		rules.MinLength != nil ||
		rules.MaxLength != nil ||
		rules.AllowedValues != nil ||
		rules.Min != nil ||
		rules.Max != nil)
}

// rulesVerdict is what a candidate's rules say of the content, as the model
// takes it.
func rulesVerdict(content []byte, c fileMediaCandidate) (model.Rules, error) {
	if !declaresRules(c.validation) {
		return model.RulesUnruled{}, nil
	}
	met, err := meets(content, c.urn, c.validation)
	if err != nil {
		return nil, err
	}
	if met {
		return model.RulesMet{}, nil
	}
	return model.RulesUnmet{}, nil
}

// survivingCandidates is the candidates that stand (the rule above, decided by
// the model's Stands), in the candidates' order. An invalid validation pattern
// is an error naming its definition.
func survivingCandidates(content []byte, candidates []fileMediaCandidate, verdict adapterVerdict) ([]*urn.MediaUrn, error) {
	modelled := make([]model.Candidate, 0, len(candidates))
	for _, c := range candidates {
		rules, err := rulesVerdict(content, c)
		if err != nil {
			return nil, err
		}
		modelled = append(modelled, model.Candidate{Urn: c.urn.Formal(), Rules: rules})
	}
	owners := formalOf(verdict.owners)
	claims := formalOf(verdict.claimed)
	survivors := make([]*urn.MediaUrn, 0, len(candidates))
	for i, c := range candidates {
		if decided(model.Stands(modelled, owners, claims, modelled[i])) {
			survivors = append(survivors, c.urn)
		}
	}
	return survivors, nil
}

// settle refines what stands through the value adapters and keeps only the
// most specific member of each line of refinement (the model's MostSpecific),
// in order.
func settle(survivors []*urn.MediaUrn, valueAdapters *ValueAdapterRegistry, content []byte) ([]string, error) {
	text := ""
	if utf8.Valid(content) {
		text = string(content)
	}
	refined := make([]*urn.MediaUrn, 0, len(survivors))
	for _, survivor := range survivors {
		refinedStr := valueAdapters.RefineMediaUrn(survivor.String(), text)
		u, err := urn.NewMediaUrnFromString(refinedStr)
		if err != nil {
			return nil, fmt.Errorf("a value adapter refined a file's media to '%s', which is no media URN: %v",
				refinedStr, err)
		}
		seen := false
		for _, prior := range refined {
			if prior.IsEquivalent(u) {
				seen = true
				break
			}
		}
		if !seen {
			refined = append(refined, u)
		}
	}
	all := formalOf(refined)
	kept := make([]string, 0, len(refined))
	for _, u := range refined {
		if decided(model.MostSpecific(all, u.Formal())) {
			kept = append(kept, u.String())
		}
	}
	return kept, nil
}

// PrimaryMedia is the one media a host that cannot ask uses: the most
// specific, the first of them on a tie (the model's Primary). ok is false only
// for an empty list; a member that is no media URN is an error naming it.
func PrimaryMedia(mediaUrns []string) (primary string, ok bool, err error) {
	all := make([]taggedformal.Wf, 0, len(mediaUrns))
	for _, m := range mediaUrns {
		u, perr := urn.NewMediaUrnFromString(m)
		if perr != nil {
			return "", false, fmt.Errorf("'%s' is no media URN: %v", m, perr)
		}
		all = append(all, u.Formal())
	}
	position := decided(model.Primary(all))
	if !position.Valid {
		return "", false, nil
	}
	if !position.Value.IsUint64() || position.Value.Uint64() >= uint64(len(mediaUrns)) {
		panic(fmt.Sprintf("the model named position %s of a list of %d media", position.Value.String(), len(mediaUrns)))
	}
	return mediaUrns[position.Value.Uint64()], true, nil
}

// lowercaseExtension is a path's extension, lowercased: what follows the last
// '.' of its file name, and empty when the name has none, or has only a
// leading one (a dotfile).
func lowercaseExtension(path string) string {
	name := filepath.Base(path)
	dot := strings.LastIndex(name, ".")
	if dot <= 0 {
		return ""
	}
	return strings.ToLower(name[dot+1:])
}

// candidatesFor is the file media types a file with this extension can be,
// with their validation rules: the fabric's answer, loaded when first asked.
func candidatesFor(path, extension string, registry *fabric.FabricRegistry) ([]fileMediaCandidate, error) {
	if extension == "" {
		return nil, NoExtensionError(path)
	}
	defs, err := registry.FileMediaForExtension(extension)
	if err != nil {
		return nil, FabricUnavailableError(path, err.Error())
	}
	if len(defs) == 0 {
		return nil, UnknownExtensionError(path, extension)
	}
	candidates := make([]fileMediaCandidate, 0, len(defs))
	for _, def := range defs {
		u, err := urn.NewMediaUrnFromString(def.Urn)
		if err != nil {
			return nil, InspectionFailedError(path,
				fmt.Sprintf("the fabric's media definition '%s' has no valid URN: %v", def.Urn, err))
		}
		candidates = append(candidates, fileMediaCandidate{urn: u, validation: def.Validation})
	}
	return candidates, nil
}

// readPrefix is a file's size and the first MaxContentInspectionBytes of its
// content.
func readPrefix(path string) (uint64, []byte, error) {
	ioError := func(err error) error {
		switch {
		case os.IsNotExist(err):
			return NotFoundError(path)
		case os.IsPermission(err):
			return PermissionDeniedError(path)
		default:
			return IoError(path, err)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, nil, ioError(err)
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, nil, ioError(err)
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, MaxContentInspectionBytes))
	if err != nil {
		return 0, nil, ioError(err)
	}
	return uint64(info.Size()), content, nil
}

// consult asks every cartridge whose adapter covers a candidate what the file
// is. A cartridge that declines, or fails, claims nothing; its domain stays
// covered, so its candidates fall.
func consult(ctx context.Context, path string, candidates []fileMediaCandidate, adapters AdapterSource, invoker CartridgeAdapterInvoker) adapterVerdict {
	urns := make([]*urn.MediaUrn, 0, len(candidates))
	for _, c := range candidates {
		urns = append(urns, c.urn)
	}
	var verdict adapterVerdict
	asked := make(map[string]bool)
	for _, match := range adapters.AdaptersFor(urns) {
		verdict.owners = append(verdict.owners, match.MediaUrn)
		if asked[match.CartridgeID] {
			continue
		}
		asked[match.CartridgeID] = true
		claims, err := invoker.InvokeAdapterSelection(ctx, match.CartridgeID, path)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"[ERROR] the content inspection of cartridge '%s' (adapter '%s') failed for %s: %v\n",
				match.CartridgeID, match.MediaUrn.String(), path, err)
			continue
		}
		for _, claim := range claims {
			u, perr := urn.NewMediaUrnFromString(claim)
			if perr != nil {
				fmt.Fprintf(os.Stderr,
					"[ERROR] cartridge '%s' claimed %s as '%s', which is no media URN: %v\n",
					match.CartridgeID, path, claim, perr)
				continue
			}
			verdict.claimed = append(verdict.claimed, u)
		}
	}
	return verdict
}

// DetectFileMedia is what a file is, by its extension and its content: the one
// detection every host runs (see the top of this file). A host with no
// cartridges passes NoAdapters{} and NoInvoker{}.
func DetectFileMedia(
	ctx context.Context,
	path string,
	registry *fabric.FabricRegistry,
	adapters AdapterSource,
	invoker CartridgeAdapterInvoker,
	valueAdapters *ValueAdapterRegistry,
) (*FileMedia, error) {
	extension := lowercaseExtension(path)
	candidates, err := candidatesFor(path, extension, registry)
	if err != nil {
		return nil, err
	}
	sizeBytes, content, err := readPrefix(path)
	if err != nil {
		return nil, err
	}
	verdict := consult(ctx, path, candidates, adapters, invoker)
	survivors, err := survivingCandidates(content, candidates, verdict)
	if err != nil {
		return nil, InspectionFailedError(path, err.Error())
	}
	if len(survivors) == 0 {
		return nil, InspectionFailedError(path, fmt.Sprintf(
			"its content is none of the %d media types the fabric declares for .%s",
			len(candidates), extension))
	}
	found, err := settle(survivors, valueAdapters, content)
	if err != nil {
		return nil, InspectionFailedError(path, err.Error())
	}
	names := make([]string, 0, len(candidates))
	for _, c := range candidates {
		names = append(names, c.urn.String())
	}
	return &FileMedia{
		Path:       path,
		SizeBytes:  sizeBytes,
		Extension:  extension,
		Candidates: names,
		Media:      found,
	}, nil
}

// ExtensionMedia is what a file with this extension is before its content is
// read: the extension's base types (no candidate is more general), in the
// candidates' order — the model's rule with no rules read and no cartridge
// asked. Hosts that name a file's media from its name alone use this, and
// PrimaryMedia when they need one.
func ExtensionMedia(extension string, registry *fabric.FabricRegistry) ([]string, error) {
	path := "*." + extension
	candidates, err := candidatesFor(path, strings.ToLower(extension), registry)
	if err != nil {
		return nil, err
	}
	unread := make([]fileMediaCandidate, 0, len(candidates))
	for _, c := range candidates {
		unread = append(unread, fileMediaCandidate{urn: c.urn})
	}
	bases, err := survivingCandidates(nil, unread, adapterVerdict{})
	if err != nil {
		panic(fmt.Sprintf("candidates with no rules are decided without reading a pattern: %v", err))
	}
	out := make([]string, 0, len(bases))
	for _, u := range bases {
		out = append(out, u.String())
	}
	return out, nil
}
