package urn

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TEST12166: matching, specificity, dispatch and acceptance are the proved model's.
//
// The rules are proved in capdag/formal (Lean); this is what ties them to this
// mirror: every row of ../../formal/conformance.json (written by the model,
// `lake exe conformance`) is parsed by the media and cap URN parsers here and
// must get the model's verdict. The same table runs in every mirror.
func Test12166_TheImplementationIsTheProvedModel(t *testing.T) {
	raw, err := os.ReadFile("../../formal/conformance.json")
	require.NoError(t, err, "the model's table")
	var table struct {
		Refines []struct {
			Instance string `json:"instance"`
			Pattern  string `json:"pattern"`
			Refines  bool   `json:"refines"`
		} `json:"refines"`
		Scores []struct {
			Urn   string `json:"urn"`
			Score int    `json:"score"`
		} `json:"scores"`
		Dispatch []struct {
			Candidate string `json:"candidate"`
			Request   string `json:"request"`
			Dispatch  bool   `json:"dispatch"`
			Accepts   bool   `json:"accepts"`
		} `json:"dispatch"`
	}
	require.NoError(t, json.Unmarshal(raw, &table))

	var wrong []string
	for _, r := range table.Refines {
		a, err := NewMediaUrnFromString(r.Instance)
		require.NoError(t, err)
		b, err := NewMediaUrnFromString(r.Pattern)
		require.NoError(t, err)
		if got := a.ConformsTo(b); got != r.Refines {
			wrong = append(wrong, fmt.Sprintf("%s ⪯ %s: model %v, got %v", a, b, r.Refines, got))
		}
	}
	for _, r := range table.Scores {
		u, err := NewMediaUrnFromString(r.Urn)
		require.NoError(t, err)
		if got := u.Specificity(); got != r.Score {
			wrong = append(wrong, fmt.Sprintf("score %s: model %d, got %d", u, r.Score, got))
		}
	}
	for _, r := range table.Dispatch {
		c, err := NewCapUrnFromString(r.Candidate)
		require.NoError(t, err)
		q, err := NewCapUrnFromString(r.Request)
		require.NoError(t, err)
		if got := c.IsDispatchable(q); got != r.Dispatch {
			wrong = append(wrong, fmt.Sprintf("%s serves %s: model %v, got %v", c, q, r.Dispatch, got))
		}
		if got := c.Accepts(q); got != r.Accepts {
			wrong = append(wrong, fmt.Sprintf("%s accepts %s: model %v, got %v", c, q, r.Accepts, got))
		}
	}
	require.True(t, len(table.Refines) > 4000 && len(table.Dispatch) > 20000, "the table is the full one")
	if len(wrong) > 0 {
		t.Fatalf("%d row(s) differ from the model, e.g.\n  %v", len(wrong), wrong[:min(8, len(wrong))])
	}
}
