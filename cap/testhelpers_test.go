package cap

import (
	"testing"

	"github.com/machinefabric/capdag-go/media"
)

// testRegistry builds a fresh FabricRegistry for unit tests. Historically
// this helper lived alongside the (now-deleted) CapCaller tests in
// caller_test.go. Its absence failed to compile a dozen other tests in
// this package, so it is kept as a shared helper.
func testRegistry(t *testing.T) *testMediaDefSource {
	t.Helper()
	registry := newTestMediaDefSource()
	// Seed the baseline standard specs the cap-package tests resolve against.
	for _, def := range []media.MediaDef{
		{Urn: "media:enc=utf-8", MediaType: "text/plain", ProfileURI: media.ProfileStr},
		{Urn: "media:record", MediaType: "application/json", ProfileURI: media.ProfileObj},
		{Urn: "media:fmt=json;record", MediaType: "application/json", ProfileURI: media.ProfileObj},
		{Urn: "media:", MediaType: "application/octet-stream"},
		{Urn: "media:void", MediaType: "application/x-void", ProfileURI: media.ProfileVoid},
	} {
		registry.AddSpec(def.ToStored())
	}
	return registry
}
