package machine

// A machine that needs a definition the registry refused to serve fails with
// the refusal — never as an undefined alias, nor as a definition that is not
// available. Run against a stub registry on a local socket.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/fabric"
)

// refusingRegistry is a registry whose fabric answers every fetch 403, with
// the notation tests' caps in memory.
func refusingRegistry(t *testing.T) (*fabric.FabricRegistry, string) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(server.Close)
	registry := fabric.NewForTestWithConfig(fabric.DefaultConfig().WithAnchor(server.URL))
	registry.AddCapsToCache([]*cap.Cap{compareCap(), fetchCap(), publishCap(), meanCap(), scoreCap()})
	return registry, server.URL
}

// TEST7174: a machine whose cap alias the registry refused to serve fails
// with the refusal, never as an undefined alias; an alias the registry does
// not have is still undefined.
func Test7174_ARefusedAliasIsReportedAsTheRefusalNotAsUndefined(t *testing.T) {
	registry, anchor := refusingRegistry(t)
	registry.PinAliasForTest("measure-returns", 2)
	_, err := registry.GetAlias("measure-returns")
	require.Error(t, err)

	_, perr := ParseMachine("[doc -> measure-returns -> text]", registry)
	require.NotNil(t, perr)
	require.NotNil(t, perr.Abstraction, "expected a resolution error, got %v", perr)
	assert.Equal(t, ErrAbstractionDefinitionUnavailable, perr.Abstraction.Kind)
	refusal, ok := fabric.AsFetchFailure(perr.Abstraction.Err)
	require.True(t, ok, "the refused fetch is carried: %v", perr)
	assert.Equal(t, "alias 'measure-returns' (defver 2)", refusal.What)
	assert.Equal(t, http.StatusForbidden, refusal.Status)
	for _, part := range []string{"refused", "403", "measure-returns", "fabric manifest v1", anchor} {
		assert.Contains(t, perr.Error(), part)
	}

	_, undefined := ParseMachine("[doc -> named-by-no-manifest -> text]", registry)
	require.NotNil(t, undefined)
	require.NotNil(t, undefined.Syntax, "expected an undefined alias, got %v", undefined)
	assert.Equal(t, ErrUndefinedAlias, undefined.Syntax.Kind)
}

// TEST7176: a constant whose arg's media definition is in the registry, and
// whose fetch the registry refused, is reported as the refusal — the URL, the
// status and the manifest version — not as a definition that is not available.
func Test7176_AConstantWhoseDefinitionWasRefusedReportsTheRefusal(t *testing.T) {
	registry, _ := refusingRegistry(t)
	registry.PinMediaDefForTest(benchmarkMedia, 1)
	_, err := registry.GetMediaDef(benchmarkMedia)
	require.Error(t, err)

	_, perr := ParseMachine(notationHeaders+"[(path, benchmark=[1, 2]) -> compare -> result]", registry)
	require.NotNil(t, perr)
	require.NotNil(t, perr.Abstraction, "expected a resolution error, got %v", perr)
	assert.Equal(t, ErrAbstractionDefinitionUnavailable, perr.Abstraction.Kind)
	refusal, ok := fabric.AsFetchFailure(perr.Abstraction.Err)
	require.True(t, ok, "the refused fetch is carried: %v", perr)
	assert.Equal(t, http.StatusForbidden, refusal.Status)
	assert.Equal(t, uint32(1), refusal.ManifestVersion)
	assert.NotContains(t, perr.Error(), "not available")
	assert.Contains(t, perr.Error(), "refused")
}
