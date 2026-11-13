package cap

import (
	"fmt"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/urn"
)

// testMediaDefSource is a media-def lookup backed by a plain map.
//
// The registry itself is tested in package `fabric`; these tests exercise the
// code that CONSULTS a registry, and an in-package test file here cannot import
// `fabric` (it imports this package, so the test binary would cycle). A map is
// the whole contract this package needs.
type testMediaDefSource struct {
	specs map[string]media.StoredMediaDef
}

func newTestMediaDefSource() *testMediaDefSource {
	return &testMediaDefSource{
		specs: make(map[string]media.StoredMediaDef),
	}
}

func (s *testMediaDefSource) AddSpec(spec media.StoredMediaDef) {
	normalized := spec.Urn
	if parsed, err := urn.NewMediaUrnFromString(spec.Urn); err == nil {
		normalized = parsed.String()
	}
	s.specs[normalized] = spec
}

func (s *testMediaDefSource) GetMediaDef(urnOrAlias string) (*media.StoredMediaDef, error) {
	return s.GetCachedMediaDef(urnOrAlias)
}

func (s *testMediaDefSource) GetCachedMediaDef(urnOrAlias string) (*media.StoredMediaDef, error) {
	normalized := urnOrAlias
	if parsed, err := urn.NewMediaUrnFromString(urnOrAlias); err == nil {
		normalized = parsed.String()
	}
	spec, ok := s.specs[normalized]
	if !ok {
		return nil, fmt.Errorf("media URN '%s' not found in the test source", urnOrAlias)
	}
	return &spec, nil
}
