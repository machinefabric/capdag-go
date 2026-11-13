package fabric

// A fetch the registry refused, or did not answer, is reported as itself —
// the URL, the status and the manifest version — and never as a definition
// the registry does not have. Run against a stub registry on a local socket.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/machinefabric/capdag-go/media"
)

const (
	failureManifestVersion = 10
	failureMedia           = "media:refusal-probe;enc=utf8"
	failureCap             = `cap:in="media:refusal-probe;enc=utf8";refusal-probe;out="media:refusal-probe;enc=utf8"`
	failureAlias           = "measure-returns"
	failureDim             = "refusal-probe"
)

// stubRegistry answers every request with answer(path), counting requests by
// path. A nil-handler answer of status 0 means silence: the request is held
// until the client gives up.
type stubRegistry struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests map[string]int
}

func startStub(t *testing.T, status func(path string) int) *stubRegistry {
	stub := &stubRegistry{requests: map[string]int{}}
	stub.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		stub.mu.Lock()
		stub.requests[req.URL.Path]++
		stub.mu.Unlock()
		code := status(req.URL.Path)
		if code == 0 {
			<-req.Context().Done()
			return
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte("stub"))
	}))
	t.Cleanup(stub.server.Close)
	return stub
}

func (s *stubRegistry) requestsFor(t *testing.T, url string) int {
	require.True(t, strings.HasPrefix(url, s.server.URL), "%s is not on the stub", url)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[strings.TrimPrefix(url, s.server.URL)]
}

// failureRegistry is a registry pinned at manifest v10 against stub: its
// manifest names one cap, media def, alias and dim, none of them in memory.
func failureRegistry(t *testing.T, stub *stubRegistry, timeout time.Duration) *FabricRegistry {
	r := NewForTestWithConfig(DefaultConfig().WithAnchor(stub.server.URL))
	r.client = &http.Client{Timeout: timeout}
	capURN, err := NormalizeCapURN(failureCap)
	require.NoError(t, err)
	mediaURN, err := NormalizeMediaURN(failureMedia)
	require.NoError(t, err)
	manifest := media.EmptyManifest(failureManifestVersion)
	manifest.Caps[capURN] = 1
	manifest.Media[mediaURN] = 1
	manifest.Aliases[failureAlias] = 2
	manifest.Dims[failureDim] = 1
	r.manifestVersion = failureManifestVersion
	r.manifest = manifest
	return r
}

func capWhat(t *testing.T) string {
	normalized, err := NormalizeCapURN(failureCap)
	require.NoError(t, err)
	return "cap '" + normalized + "' (defver 1)"
}

func mediaWhat(t *testing.T) string {
	normalized, err := NormalizeMediaURN(failureMedia)
	require.NoError(t, err)
	return "media def '" + normalized + "' (defver 1)"
}

func requireFailure(t *testing.T, err error) *FetchFailure {
	require.Error(t, err)
	require.Equal(t, ErrFetchFailed, kindOf(err), "expected a failed fetch, got %v", err)
	failure, ok := AsFetchFailure(err)
	require.True(t, ok, "a failed fetch carries its FetchFailure: %v", err)
	return failure
}

// TEST7167: a manifest fetch the registry refuses (403) is reported as the
// refusal — the URL, the status and the manifest version — and not as a
// manifest that is not found. A 403 is not repeated.
func Test7167_ARefusedManifestFetchIsReportedAsTheRefusal(t *testing.T) {
	stub := startStub(t, func(string) int { return http.StatusForbidden })
	locations, err := LocationsFromCharter(PublisherLayoutCharter(stub.server.URL))
	require.NoError(t, err)
	_, err = loadOrFetchManifest(t.TempDir(), &http.Client{Timeout: httpTimeout}, locations, 10)
	refusal := requireFailure(t, err)
	assert.Equal(t, FetchFailure{What: "manifest v10", URL: locations.Manifest(10), ManifestVersion: 10,
		Cause: FetchStatus, Status: http.StatusForbidden}, *refusal)
	text := err.Error()
	for _, part := range []string{"refused", "HTTP 403 Forbidden", refusal.URL, "fabric manifest v10"} {
		assert.Contains(t, text, part)
	}
	assert.NotContains(t, strings.ToLower(text), "not found")
	assert.Equal(t, 1, stub.requestsFor(t, refusal.URL), "a 403 is not repeated")
}

// TEST7168: a manifest the registry answers 404 for is the registry's word
// that it has no such manifest: NotFound, naming the URL and the status.
func Test7168_AManifestTheRegistryDoesNotHaveIsNotFound(t *testing.T) {
	stub := startStub(t, func(string) int { return http.StatusNotFound })
	locations, err := LocationsFromCharter(PublisherLayoutCharter(stub.server.URL))
	require.NoError(t, err)
	_, err = loadOrFetchManifest(t.TempDir(), &http.Client{Timeout: httpTimeout}, locations, 10)
	require.True(t, IsNotFound(err), "expected NotFound, got %v", err)
	assert.Contains(t, err.Error(), "HTTP 404")
	assert.Contains(t, err.Error(), locations.Manifest(10))
}

// TEST7169: a refused fetch of a cap, a media def, an alias or a dim is
// reported as the refusal, each with its own URL, the status and the
// manifest version it was fetched for.
func Test7169_ARefusedDefinitionFetchIsReportedAsTheRefusal(t *testing.T) {
	stub := startStub(t, func(string) int { return http.StatusForbidden })
	r := failureRegistry(t, stub, httpTimeout)
	_, capErr := r.GetCap(failureCap)
	_, mediaErr := r.GetMediaDef(failureMedia)
	_, aliasErr := r.GetAlias(failureAlias)
	_, dimErr := r.GetDim(failureDim)
	for _, c := range []struct {
		err  error
		what string
	}{
		{capErr, capWhat(t)},
		{mediaErr, mediaWhat(t)},
		{aliasErr, "alias '" + failureAlias + "' (defver 2)"},
		{dimErr, "dim '" + failureDim + "' (defver 1)"},
	} {
		refusal := requireFailure(t, c.err)
		assert.Equal(t, c.what, refusal.What)
		assert.Equal(t, FetchStatus, refusal.Cause)
		assert.Equal(t, http.StatusForbidden, refusal.Status)
		assert.Equal(t, uint32(failureManifestVersion), refusal.ManifestVersion)
		assert.Equal(t, 1, stub.requestsFor(t, refusal.URL), "%s was fetched from %s", c.what, refusal.URL)
		assert.Contains(t, c.err.Error(), refusal.URL)
	}
}

// TEST7170: a registry that answers 503 is reported with the 503 — never as
// a missing definition. (This mirror has no retry layer: the reference asks a
// 503 again before it reports it.)
func Test7170_ARegistryThatKeepsFailingIsReportedWithItsStatus(t *testing.T) {
	stub := startStub(t, func(string) int { return http.StatusServiceUnavailable })
	r := failureRegistry(t, stub, httpTimeout)
	_, err := r.GetMediaDef(failureMedia)
	refusal := requireFailure(t, err)
	assert.Equal(t, mediaWhat(t), refusal.What)
	assert.Equal(t, FetchStatus, refusal.Cause)
	assert.Equal(t, http.StatusServiceUnavailable, refusal.Status)
}

// TEST7171: a registry that does not answer is reported as a fetch that timed
// out, with the URL and the manifest version.
func Test7171_ARegistryThatDoesNotAnswerIsReportedAsATimeout(t *testing.T) {
	stub := startStub(t, func(string) int { return 0 })
	r := failureRegistry(t, stub, 200*time.Millisecond)
	_, err := r.GetMediaDef(failureMedia)
	timeout := requireFailure(t, err)
	assert.Equal(t, mediaWhat(t), timeout.What)
	assert.Equal(t, FetchTimedOut, timeout.Cause)
	assert.Equal(t, uint32(failureManifestVersion), timeout.ManifestVersion)
	assert.Contains(t, err.Error(), "timed out")
	assert.Contains(t, err.Error(), timeout.URL)
}

// TEST7172: a definition the registry answers 404 for is the registry's word
// that it does not have it, and only that is NotFound.
func Test7172_ADefinitionTheRegistryDoesNotHaveIsNotFound(t *testing.T) {
	stub := startStub(t, func(string) int { return http.StatusNotFound })
	r := failureRegistry(t, stub, httpTimeout)
	_, capErr := r.GetCap(failureCap)
	_, mediaErr := r.GetMediaDef(failureMedia)
	_, aliasErr := r.GetAlias(failureAlias)
	for _, err := range []error{capErr, mediaErr, aliasErr} {
		require.True(t, IsNotFound(err), "expected NotFound, got %v", err)
		assert.Contains(t, err.Error(), "HTTP 404")
		assert.Contains(t, err.Error(), stub.server.URL)
	}
}

// TEST7173: the synchronous lookups report a refused fetch as the refusal,
// where they used to come back empty — and report a definition the manifest
// does not name as not found.
func Test7173_ASynchronousLookupReportsARefusedFetch(t *testing.T) {
	stub := startStub(t, func(string) int { return http.StatusForbidden })
	r := failureRegistry(t, stub, httpTimeout)
	_, _ = r.GetMediaDef(failureMedia)
	_, _ = r.GetCap(failureCap)
	_, err := r.GetCachedMediaDef(failureMedia)
	mediaRefusal := requireFailure(t, err)
	assert.Equal(t, mediaWhat(t), mediaRefusal.What)
	assert.Equal(t, http.StatusForbidden, mediaRefusal.Status)
	_, err = r.GetCachedCap(failureCap)
	capRefusal := requireFailure(t, err)
	assert.Equal(t, capWhat(t), capRefusal.What)
	assert.Equal(t, http.StatusForbidden, capRefusal.Status)
	_, err = r.GetCachedMediaDef("media:named-by-no-manifest")
	assert.True(t, IsNotFound(err), "expected NotFound, got %v", err)
}

// TEST7175: what a fetch came to is kept: a lookup that never touches the
// network reports the refusal the fetch got, says a definition that has not
// been fetched is pending, and finds an alias the manifest does not name to
// be no alias.
func Test7175_ARefusedFetchIsRememberedByLookupsThatCannotWait(t *testing.T) {
	stub := startStub(t, func(string) int { return http.StatusForbidden })
	r := failureRegistry(t, stub, httpTimeout)
	_, _, err := r.ResolveAliasCached(failureAlias)
	assert.Equal(t, ErrPending, kindOf(err), "not fetched yet: %v", err)
	_, err = r.GetCachedMediaDef(failureMedia)
	assert.Equal(t, ErrPending, kindOf(err), "not fetched yet: %v", err)

	_, _ = r.GetAlias(failureAlias)
	_, _, err = r.ResolveAliasCached(failureAlias)
	refusal := requireFailure(t, err)
	assert.Equal(t, "alias '"+failureAlias+"' (defver 2)", refusal.What)
	assert.Equal(t, http.StatusForbidden, refusal.Status)

	_, known, err := r.ResolveAliasCached("named-by-no-manifest")
	assert.False(t, known)
	assert.NoError(t, err)
}
