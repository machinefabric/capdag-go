package input_resolver

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/require"
)

// TEST7124: what a file is, decided through the detection module as a host
// runs it: which candidates stand, given what their rules say of the content,
// the consulted adapters and what their cartridges claimed; which of those
// each line of refinement keeps; and which of the kept a host that cannot ask
// takes. The content is one byte, so a rule of at least one byte is met and a
// rule of at least two is not. The rows are the model's
// (../../formal/conformance.json, `lake exe conformance`), and the same table
// runs in every mirror.
func Test7124_DetectionIsTheModels(t *testing.T) {
	raw, err := os.ReadFile("../../formal/conformance.json")
	require.NoError(t, err, "the model's table")
	var table struct {
		Detect []struct {
			Candidates []struct {
				Urn   string `json:"urn"`
				Rules string `json:"rules"`
			} `json:"candidates"`
			Owners       []string `json:"owners"`
			Claims       []string `json:"claims"`
			Stands       []bool   `json:"stands"`
			MostSpecific []bool   `json:"most_specific"`
			Primary      *int     `json:"primary"`
		} `json:"detect"`
	}
	require.NoError(t, json.Unmarshal(raw, &table))
	require.Len(t, table.Detect, (9+81)*2*4, "every rule, owner and claim set over both lists")

	content := []byte("x")
	one, two := 1, 2
	mediaOf := func(text string) *urn.MediaUrn {
		u, err := urn.NewMediaUrnFromString(text)
		require.NoError(t, err)
		return u
	}
	mediaList := func(texts []string) []*urn.MediaUrn {
		out := make([]*urn.MediaUrn, 0, len(texts))
		for _, text := range texts {
			out = append(out, mediaOf(text))
		}
		return out
	}

	var wrong []string
	for _, r := range table.Detect {
		candidates := make([]fileMediaCandidate, 0, len(r.Candidates))
		for _, c := range r.Candidates {
			var validation *media.MediaValidation
			switch c.Rules {
			case "unruled":
			case "met":
				validation = &media.MediaValidation{MinLength: &one}
			case "unmet":
				validation = &media.MediaValidation{MinLength: &two}
			default:
				t.Fatalf("the model's table names no rules '%s'", c.Rules)
			}
			candidates = append(candidates, fileMediaCandidate{urn: mediaOf(c.Urn), validation: validation})
		}
		verdict := adapterVerdict{owners: mediaList(r.Owners), claimed: mediaList(r.Claims)}
		survivors, err := survivingCandidates(content, candidates, verdict)
		require.NoError(t, err)
		stands := make([]bool, len(candidates))
		for i, c := range candidates {
			for _, s := range survivors {
				if s == c.urn {
					stands[i] = true
				}
			}
		}
		if fmt.Sprint(stands) != fmt.Sprint(r.Stands) {
			wrong = append(wrong, fmt.Sprintf("%+v: stands %v", r, stands))
			continue
		}
		kept, err := settle(survivors, NewValueAdapterRegistry(), content)
		require.NoError(t, err)
		mostSpecific := make([]bool, len(survivors))
		keptCount := 0
		for i, s := range survivors {
			for _, k := range kept {
				if k == s.String() {
					mostSpecific[i] = true
				}
			}
		}
		for _, k := range r.MostSpecific {
			if k {
				keptCount++
			}
		}
		if fmt.Sprint(mostSpecific) != fmt.Sprint(r.MostSpecific) || len(kept) != keptCount {
			wrong = append(wrong, fmt.Sprintf("%+v: kept %v", r, kept))
			continue
		}
		primary, ok, err := PrimaryMedia(kept)
		require.NoError(t, err)
		switch {
		case r.Primary == nil && ok:
			wrong = append(wrong, fmt.Sprintf("%+v: primary %s, model none", r, primary))
		case r.Primary != nil && (!ok || primary != kept[*r.Primary]):
			wrong = append(wrong, fmt.Sprintf("%+v: primary %q (%v)", r, primary, ok))
		}
	}
	require.Empty(t, wrong, "%d of %d detection row(s) differ from the model, e.g. %v",
		len(wrong), len(table.Detect), wrong[:min(len(wrong), 8)])
}
