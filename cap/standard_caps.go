package cap

import (
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// LookupCapFabricUrn parses and returns the canonical cap:lookup-cap;fabric CapUrn.
func LookupCapFabricUrn() *urn.CapUrn {
	u, err := urn.NewCapUrnFromString(standard.CapLookupCapFabric)
	if err != nil {
		panic("BUG: standard.CapLookupCapFabric constant is invalid: " + err.Error())
	}
	return u
}

// LookupMediaDefFabricUrn parses and returns the canonical cap:lookup-media-def;fabric CapUrn.
func LookupMediaDefFabricUrn() *urn.CapUrn {
	u, err := urn.NewCapUrnFromString(standard.CapLookupMediaDefFabric)
	if err != nil {
		panic("BUG: standard.CapLookupMediaDefFabric constant is invalid: " + err.Error())
	}
	return u
}

// LookupCapFabricCap constructs the canonical cap:lookup-cap;fabric Cap definition.
//
// Mirrors the fabric/caps/lookup-cap-fabric.toml source-of-truth so that
// the cartridge can declare it in its manifest without duplicating the wire
// shape across two places.
func LookupCapFabricCap() *Cap {
	u := LookupCapFabricUrn()
	cap := NewCapWithDescription(
		u,
		"Look Up Cap Definition (Fabric)",
		[]string{"lookup_cap"},
		"Resolve a canonical cap URN to its full registry-published cap definition by fetching from the public fabric registry.",
	)
	cap.Args = append(cap.Args, NewCapArgWithDescription(
		standard.MediaCapURN,
		true,
		[]ArgSource{
			{Stdin: StringPtr(standard.MediaCapURN)},
			{Position: func() *int { p := 0; return &p }()},
		},
		"Canonical cap URN to look up.",
	))
	cap.Args = append(cap.Args, NewCapArgWithDescription(
		standard.MediaFabricDefver,
		false,
		[]ArgSource{{CliFlag: StringPtr("--defver")}},
		"Per-definition version under the caller's manifest snapshot. Absent ⇒ defver 0 (legacy v0 flat-path lookup).",
	))
	cap.Output = NewCapOutput(
		standard.MediaCapDefinition,
		"Full flattened cap definition as published in the registry.",
	)
	return cap
}

// LookupMediaDefFabricCap constructs the canonical cap:lookup-media-def;fabric Cap definition.
//
// Mirrors fabric/caps/lookup-media-def-fabric.toml.
func LookupMediaDefFabricCap() *Cap {
	u := LookupMediaDefFabricUrn()
	cap := NewCapWithDescription(
		u,
		"Look Up Media Definition (Fabric)",
		[]string{"lookup_media_def"},
		"Resolve a canonical media URN to its full registry-published media def definition by fetching from the public fabric registry.",
	)
	cap.Args = append(cap.Args, NewCapArgWithDescription(
		standard.MediaMediaURN,
		true,
		[]ArgSource{
			{Stdin: StringPtr(standard.MediaMediaURN)},
			{Position: func() *int { p := 0; return &p }()},
		},
		"Canonical media URN to look up.",
	))
	cap.Args = append(cap.Args, NewCapArgWithDescription(
		standard.MediaFabricDefver,
		false,
		[]ArgSource{{CliFlag: StringPtr("--defver")}},
		"Per-definition version under the caller's manifest snapshot. Absent ⇒ defver 0 (legacy v0 flat-path lookup).",
	))
	cap.Output = NewCapOutput(
		standard.MediaMediaDefinition,
		"Full media definition as published in the registry.",
	)
	return cap
}
