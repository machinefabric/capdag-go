package urn

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TEST12166: every answer about media and caps is the proved model's.
//
// The rules are proved in capdag/formal (Lean); this is what ties them to this
// mirror: every row of ../../formal/conformance.json (written by the model,
// `lake exe conformance`) is parsed by the media and cap URN parsers here and
// must get the model's verdict: between media, the guarantee, the possibility
// and the complete reading; between caps, serving, could-serve and the grade
// of a request, fitting a pattern, being the same cap, and flowing into one
// another. The same table runs in every mirror.
func Test12166_TheImplementationIsTheProvedModel(t *testing.T) {
	raw, err := os.ReadFile("../../formal/conformance.json")
	require.NoError(t, err, "the model's table")
	var table struct {
		Refines []struct {
			Instance   string `json:"instance"`
			Pattern    string `json:"pattern"`
			Refines    bool   `json:"refines"`
			Meets      bool   `json:"meets"`
			Satisfies  bool   `json:"satisfies"`
			MaySatisfy bool   `json:"may_satisfy"`
		} `json:"refines"`
		Scores []struct {
			Urn   string `json:"urn"`
			Score int    `json:"score"`
		} `json:"scores"`
		Dispatch []struct {
			Candidate   string `json:"candidate"`
			Request     string `json:"request"`
			Dispatch    bool   `json:"dispatch"`
			MayDispatch bool   `json:"may_dispatch"`
			Grade       string `json:"grade"`
			Accepts     bool   `json:"accepts"`
			Equivalent  bool   `json:"equivalent"`
			Flows       bool   `json:"flows"`
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
		for _, check := range []struct {
			name       string
			got, model bool
		}{
			{"meets", a.Meets(b), r.Meets},
			{"satisfies", a.Satisfies(b), r.Satisfies},
			{"may satisfy", a.MaySatisfy(b), r.MaySatisfy},
		} {
			if check.got != check.model {
				wrong = append(wrong, fmt.Sprintf("%s %s %s: model %v, got %v", a, check.name, b, check.model, check.got))
			}
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
		request := CapQueryFromRequest(q)
		if got := string(request.Grade(c)); got != r.Grade {
			wrong = append(wrong, fmt.Sprintf("grade of %s for %s: model %s, got %s", c, q, r.Grade, got))
		}
		for _, check := range []struct {
			name       string
			got, model bool
		}{
			{"dispatch", c.IsDispatchable(q), r.Dispatch},
			{"dispatch (query)", request.Admits(c), r.Dispatch},
			{"may dispatch", c.MayDispatch(q), r.MayDispatch},
			{"may dispatch (query)", request.MayAdmit(c), r.MayDispatch},
			{"accepts", c.Accepts(q), r.Accepts},
			{"accepts (query)", CapQueryFromPattern(c).Admits(q), r.Accepts},
			{"accepts (conforms)", q.ConformsTo(c), r.Accepts},
			{"equivalent", c.IsEquivalent(q), r.Equivalent},
			{"flows", c.FlowsInto(q), r.Flows},
		} {
			if check.got != check.model {
				wrong = append(wrong, fmt.Sprintf("%s: %s / %s: model %v, got %v", check.name, c, q, check.model, check.got))
			}
		}
	}
	require.True(t, len(table.Refines) > 4000 && len(table.Dispatch) > 30000, "the table is the full one")
	if len(wrong) > 0 {
		t.Fatalf("%d row(s) differ from the model, e.g.\n  %v", len(wrong), wrong[:min(8, len(wrong))])
	}
}

// TEST12596: a call into the model whose argument is a temporary survives a
// collection during the call.
//
// An argument is lent to the runtime as a handle identifier, and the value
// holding the handle can be unreachable by then — `CapQueryFromPattern(c)` is
// used once and dropped. Its finalizer released the handle before the runtime
// read it, and a release's test run panicked with "217920 is not a live
// handle". Collected continuously here, so a handle that is not kept alive
// for the call is released during one.
func Test12596_AnArgumentOutlivesACollectionDuringTheCall(t *testing.T) {
	c, err := NewCapUrnFromString(`cap:in="media:ext=pdf";render-page-image;out="media:ext=png;image"`)
	require.NoError(t, err)
	q, err := NewCapUrnFromString(`cap:in="media:ext=pdf";render-page-image;out="media:image"`)
	require.NoError(t, err)

	stop := make(chan struct{})
	collecting := make(chan struct{})
	go func() {
		defer close(collecting)
		for {
			select {
			case <-stop:
				return
			default:
				runtime.GC()
			}
		}
	}()
	defer func() {
		close(stop)
		<-collecting
	}()

	want := CapQueryFromPattern(c).Admits(q)
	for i := 0; i < 20000; i++ {
		require.Equal(t, want, CapQueryFromPattern(c).Admits(q), "call %d", i)
	}
}
