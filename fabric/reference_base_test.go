package fabric

// A cap may name a published media type refined by an absence (`!k`) or
// leaving a key open (`?k`): the registry holds the published definition the
// reference resolves to, and the cap keeps its own URN.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

const (
	referenceJSON     = "media:enc=utf8;fmt=json"
	referenceContent  = "media:enc=utf8;fmt=json;!ext"
	referenceAnyShape = "media:enc=utf8;fmt=json;?list;?record"
)

// TEST7211: a cap whose sides name the published media:enc=utf8;fmt=json
// refined by an absence (!ext) and by open keys (?list;?record) is held: its
// media footprint is that one definition, fetched at the version the manifest
// pins for it, and the cap keeps its URN as written.
func Test7211_ACapNamingAPublishedTypeRefinedByAbsenceIsHeld(t *testing.T) {
	capUrn, err := urn.NewCapUrnFromString(`cap:in="` + referenceContent + `";out="` + referenceAnyShape + `";reference-probe`)
	require.NoError(t, err)
	probe := cap.NewCap(capUrn, "Reference probe", nil)
	probe.Version = 1
	stdin := referenceContent
	probe.Args = append(probe.Args, cap.NewCapArg(referenceContent, "input", true, []cap.ArgSource{{Stdin: &stdin}}))
	probe.AddOutput(*cap.NewCapOutput(referenceAnyShape, "output", "out"))

	jsonKey, err := NormalizeMediaURN(referenceJSON)
	require.NoError(t, err)
	referenced, err := collectReferencedMediaURNs(probe)
	require.NoError(t, err)
	assert.Equal(t, []string{jsonKey}, referenced)

	capKey, err := NormalizeCapURN(capUrn.String())
	require.NoError(t, err)
	capBody, err := json.Marshal(probe)
	require.NoError(t, err)
	mediaBody := []byte(`{"urn":"media:enc=utf8;fmt=json","version":1,"title":"JSON","media_type":"application/json"}`)

	var mu sync.Mutex
	requests := map[string]int{}
	var capPath, mediaPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		requests[req.URL.Path]++
		mu.Unlock()
		switch req.URL.Path {
		case capPath:
			_, _ = w.Write(capBody)
		case mediaPath:
			_, _ = w.Write(mediaBody)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	locations := layoutLocations(server.URL)
	capURL, _ := definitionURLAndCachePath("", locations, "caps", capKey, 1)
	mediaURL, _ := definitionURLAndCachePath("", locations, "media", jsonKey, 1)
	capPath, mediaPath = capURL[len(server.URL):], mediaURL[len(server.URL):]

	r := NewForTestWithConfigAndVersion(DefaultConfig().WithAnchor(server.URL), 12)
	manifest := media.EmptyManifest(12)
	manifest.Caps[capKey] = 1
	manifest.Media[jsonKey] = 1
	r.manifest = manifest

	held, err := r.GetCap(capKey)
	require.NoError(t, err)
	content, err := urn.NewMediaUrnFromString(referenceContent)
	require.NoError(t, err)
	anyShape, err := urn.NewMediaUrnFromString(referenceAnyShape)
	require.NoError(t, err)
	assert.Equal(t, content.String(), held.Urn.InSpec())
	assert.Equal(t, anyShape.String(), held.Urn.OutSpec())

	// Each side's type is drawn with the title of the definition it resolves to.
	for _, side := range []string{referenceContent, referenceAnyShape} {
		def, err := r.GetCachedMediaDef(side)
		require.NoError(t, err, side)
		assert.Equal(t, "JSON", def.Title, side)
	}
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, requests[mediaPath], "the base definition is fetched once")
	assert.Equal(t, 2, len(requests), "the cap and its one media definition")
}
