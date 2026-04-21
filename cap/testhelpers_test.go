package cap

import (
	"testing"

	"github.com/machinefabric/capdag-go/media"
	"github.com/stretchr/testify/require"
)

// testRegistry builds a fresh MediaUrnRegistry for unit tests. Historically
// this helper lived alongside the (now-deleted) CapCaller tests in
// caller_test.go. Its absence failed to compile a dozen other tests in
// this package, so it is kept as a shared helper.
func testRegistry(t *testing.T) *media.MediaUrnRegistry {
	t.Helper()
	registry, err := media.NewMediaUrnRegistry()
	require.NoError(t, err)
	return registry
}
