package input_resolver

import (
	"fmt"
	"strings"

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
	byExt map[string][]string
}

func newTestMediaDefSource() *testMediaDefSource {
	return &testMediaDefSource{
		specs: make(map[string]media.StoredMediaDef),
		byExt: make(map[string][]string),
	}
}

func (s *testMediaDefSource) AddSpec(spec media.StoredMediaDef) {
	normalized := spec.Urn
	if parsed, err := urn.NewMediaUrnFromString(spec.Urn); err == nil {
		normalized = parsed.String()
	}
	s.specs[normalized] = spec
	for _, ext := range spec.Extensions {
		lower := strings.ToLower(ext)
		s.byExt[lower] = append(s.byExt[lower], spec.Urn)
	}
}

func (s *testMediaDefSource) GetMediaDef(urnOrAlias string) (*media.StoredMediaDef, error) {
	spec := s.GetCachedMediaDef(urnOrAlias)
	if spec == nil {
		return nil, fmt.Errorf("media URN '%s' not found in the test source", urnOrAlias)
	}
	return spec, nil
}

func (s *testMediaDefSource) GetCachedMediaDef(urnOrAlias string) *media.StoredMediaDef {
	normalized := urnOrAlias
	if parsed, err := urn.NewMediaUrnFromString(urnOrAlias); err == nil {
		normalized = parsed.String()
	}
	spec, ok := s.specs[normalized]
	if !ok {
		return nil
	}
	return &spec
}

func (s *testMediaDefSource) MediaUrnsForExtension(extension string) ([]string, error) {
	urns, ok := s.byExt[strings.ToLower(extension)]
	if !ok {
		return nil, fmt.Errorf("no media def registered for extension '%s'", extension)
	}
	return append([]string(nil), urns...), nil
}
