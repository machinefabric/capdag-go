// Package planner provides execution plan construction and shape analysis for machines.
package planner

import (
	"fmt"

	"github.com/machinefabric/capdag-go/urn"
)

// InputCardinality represents the cardinality of cap inputs/outputs.
type InputCardinality int

const (
	// CardinalitySingle — exactly 1 item (no list marker = scalar by default).
	CardinalitySingle InputCardinality = iota
	// CardinalitySequence — array of items (has list marker).
	CardinalitySequence
	// CardinalityAtLeastOne — 1 or more items (cap can handle either).
	CardinalityAtLeastOne
)

// String returns the string representation for JSON serialization.
func (c InputCardinality) String() string {
	switch c {
	case CardinalitySingle:
		return "single"
	case CardinalitySequence:
		return "sequence"
	case CardinalityAtLeastOne:
		return "at_least_one"
	default:
		return "single"
	}
}

// CardinalityFromMediaUrn parses cardinality from a media URN string.
func CardinalityFromMediaUrn(urnStr string) InputCardinality {
	mediaUrn, err := urn.NewMediaUrnFromString(urnStr)
	if err != nil {
		panic(fmt.Sprintf("Invalid media URN in cardinality detection: %s", urnStr))
	}
	if mediaUrn.IsList() {
		return CardinalitySequence
	}
	return CardinalitySingle
}

// IsMultiple checks if this cardinality accepts multiple items.
func (c InputCardinality) IsMultiple() bool {
	return c == CardinalitySequence || c == CardinalityAtLeastOne
}

// AcceptsSingle checks if this cardinality can accept a single item.
func (c InputCardinality) AcceptsSingle() bool {
	return c == CardinalitySingle || c == CardinalityAtLeastOne
}

// CardinalityCompatibility represents the result of checking cardinality compatibility.
type CardinalityCompatibility int

const (
	// CardinalityDirect — direct flow, no transformation needed.
	CardinalityDirect CardinalityCompatibility = iota
	// CardinalityWrapInArray — need to wrap single item in array.
	CardinalityWrapInArray
	// CardinalityRequiresFanOut — need to fan-out: iterate over sequence.
	CardinalityRequiresFanOut
)

// IsCompatibleWith checks if cardinalities are compatible for data flow.
func (c InputCardinality) IsCompatibleWith(source InputCardinality) CardinalityCompatibility {
	if source == CardinalityAtLeastOne || c == CardinalityAtLeastOne {
		return CardinalityDirect
	}

	switch {
	case source == CardinalitySingle && c == CardinalitySingle:
		return CardinalityDirect
	case source == CardinalitySingle && c == CardinalitySequence:
		return CardinalityWrapInArray
	case source == CardinalitySequence && c == CardinalitySingle:
		return CardinalityRequiresFanOut
	case source == CardinalitySequence && c == CardinalitySequence:
		return CardinalityDirect
	default:
		return CardinalityDirect
	}
}

// ApplyToUrn creates a media URN with this cardinality from a base URN.
func (c InputCardinality) ApplyToUrn(baseUrn string) string {
	mediaUrn, err := urn.NewMediaUrnFromString(baseUrn)
	if err != nil {
		panic(fmt.Sprintf("Invalid media URN in ApplyToUrn: %s - %s", baseUrn, err))
	}
	hasList := mediaUrn.IsList()

	switch c {
	case CardinalitySingle, CardinalityAtLeastOne:
		if hasList {
			return mediaUrn.WithoutList().String()
		}
		return baseUrn
	case CardinalitySequence:
		if hasList {
			return baseUrn
		}
		return mediaUrn.WithList().String()
	default:
		return baseUrn
	}
}

// InputStructure represents the structure of media data.
type InputStructure int

const (
	// StructureOpaque — indivisible, no internal fields (no record marker).
	StructureOpaque InputStructure = iota
	// StructureRecord — has internal key-value fields (record marker present).
	StructureRecord
)

// String returns the string representation.
func (s InputStructure) String() string {
	switch s {
	case StructureOpaque:
		return "opaque"
	case StructureRecord:
		return "record"
	default:
		return "opaque"
	}
}

// StructureFromMediaUrn parses structure from a media URN string.
func StructureFromMediaUrn(urnStr string) InputStructure {
	mediaUrn, err := urn.NewMediaUrnFromString(urnStr)
	if err != nil {
		panic(fmt.Sprintf("Invalid media URN in structure detection: %s", urnStr))
	}
	if mediaUrn.IsRecord() {
		return StructureRecord
	}
	return StructureOpaque
}

// StructureCompatibility represents the result of checking structure compatibility.
type StructureCompatibility struct {
	IsDirect bool
	Message  string
}

// StructureCompatibilityDirect is the direct compatibility result.
var StructureCompatibilityDirect = StructureCompatibility{IsDirect: true}

// StructureIncompatible creates an incompatible structure result.
func StructureIncompatible(message string) StructureCompatibility {
	return StructureCompatibility{IsDirect: false, Message: message}
}

// IsError checks if this is an error result.
func (sc StructureCompatibility) IsError() bool {
	return !sc.IsDirect
}

// IsCompatibleWith checks if structures are compatible for data flow.
func (s InputStructure) IsCompatibleWith(source InputStructure) StructureCompatibility {
	switch {
	case source == StructureOpaque && s == StructureOpaque:
		return StructureCompatibilityDirect
	case source == StructureRecord && s == StructureRecord:
		return StructureCompatibilityDirect
	case source == StructureOpaque && s == StructureRecord:
		return StructureIncompatible("cannot add structure to opaque data")
	default: // Record → Opaque
		return StructureIncompatible("cannot discard structure from record")
	}
}

// ApplyToUrn creates a media URN with this structure from a base URN.
func (s InputStructure) ApplyToUrn(baseUrn string) string {
	mediaUrn, err := urn.NewMediaUrnFromString(baseUrn)
	if err != nil {
		panic(fmt.Sprintf("Invalid media URN in ApplyToUrn: %s - %s", baseUrn, err))
	}
	hasRecord := mediaUrn.IsRecord()

	switch s {
	case StructureOpaque:
		if hasRecord {
			return mediaUrn.WithoutTag("record").String()
		}
		return baseUrn
	case StructureRecord:
		if hasRecord {
			return baseUrn
		}
		return mediaUrn.WithTag("record", "*").String()
	default:
		return baseUrn
	}
}

// MediaShape represents the complete shape of media data.
type MediaShape struct {
	Cardinality InputCardinality
	Structure   InputStructure
}

// MediaShapeFromUrn parses complete shape from a media URN string.
func MediaShapeFromUrn(urnStr string) MediaShape {
	return MediaShape{
		Cardinality: CardinalityFromMediaUrn(urnStr),
		Structure:   StructureFromMediaUrn(urnStr),
	}
}

// ScalarOpaque creates a scalar opaque shape.
func ScalarOpaque() MediaShape {
	return MediaShape{Cardinality: CardinalitySingle, Structure: StructureOpaque}
}

// ScalarRecord creates a scalar record shape.
func ScalarRecord() MediaShape {
	return MediaShape{Cardinality: CardinalitySingle, Structure: StructureRecord}
}

// ListOpaque creates a list opaque shape.
func ListOpaque() MediaShape {
	return MediaShape{Cardinality: CardinalitySequence, Structure: StructureOpaque}
}

// ListRecord creates a list record shape.
func ListRecord() MediaShape {
	return MediaShape{Cardinality: CardinalitySequence, Structure: StructureRecord}
}

// ShapeCompatibility represents the result of checking complete shape compatibility.
type ShapeCompatibility struct {
	Kind    ShapeCompatibilityKind
	Message string
}

// ShapeCompatibilityKind identifies the shape compatibility result type.
type ShapeCompatibilityKind int

const (
	ShapeDirect ShapeCompatibilityKind = iota
	ShapeWrapInArray
	ShapeRequiresFanOut
	ShapeIncompatible
)

var (
	ShapeCompatibilityDirect        = ShapeCompatibility{Kind: ShapeDirect}
	ShapeCompatibilityWrapInArray   = ShapeCompatibility{Kind: ShapeWrapInArray}
	ShapeCompatibilityRequiresFanOut = ShapeCompatibility{Kind: ShapeRequiresFanOut}
)

// ShapeIncompat creates an incompatible shape result.
func ShapeIncompat(message string) ShapeCompatibility {
	return ShapeCompatibility{Kind: ShapeIncompatible, Message: message}
}

// IsError checks if this is an error result.
func (sc ShapeCompatibility) IsError() bool {
	return sc.Kind == ShapeIncompatible
}

// RequiresFanOut checks if fan-out is required.
func (sc ShapeCompatibility) RequiresFanOut() bool {
	return sc.Kind == ShapeRequiresFanOut
}

// RequiresWrap checks if wrap-in-array is needed.
func (sc ShapeCompatibility) RequiresWrap() bool {
	return sc.Kind == ShapeWrapInArray
}

// IsCompatibleWith checks if shapes are compatible for data flow.
func (ms MediaShape) IsCompatibleWith(source MediaShape) ShapeCompatibility {
	structCompat := ms.Structure.IsCompatibleWith(source.Structure)
	if structCompat.IsError() {
		return ShapeIncompat(structCompat.Message)
	}

	cardCompat := ms.Cardinality.IsCompatibleWith(source.Cardinality)
	switch cardCompat {
	case CardinalityDirect:
		return ShapeCompatibilityDirect
	case CardinalityWrapInArray:
		return ShapeCompatibilityWrapInArray
	case CardinalityRequiresFanOut:
		return ShapeCompatibilityRequiresFanOut
	default:
		return ShapeCompatibilityDirect
	}
}

// ApplyToUrn applies this shape to a base URN.
func (ms MediaShape) ApplyToUrn(baseUrn string) string {
	withCardinality := ms.Cardinality.ApplyToUrn(baseUrn)
	return ms.Structure.ApplyToUrn(withCardinality)
}

// CapShapeInfo provides complete shape analysis for a cap transformation.
type CapShapeInfo struct {
	Input  MediaShape
	Output MediaShape
	CapUrn string
}

// CapShapeInfoFromSpecs creates shape info by parsing a cap's input and output specs.
func CapShapeInfoFromSpecs(capUrn, inSpec, outSpec string) CapShapeInfo {
	return CapShapeInfo{
		Input:  MediaShapeFromUrn(inSpec),
		Output: MediaShapeFromUrn(outSpec),
		CapUrn: capUrn,
	}
}

// CardinalityPattern describes the input/output cardinality relationship.
type CardinalityPattern int

const (
	PatternOneToOne CardinalityPattern = iota
	PatternOneToMany
	PatternManyToOne
	PatternManyToMany
)

// String returns the string representation.
func (p CardinalityPattern) String() string {
	switch p {
	case PatternOneToOne:
		return "one_to_one"
	case PatternOneToMany:
		return "one_to_many"
	case PatternManyToOne:
		return "many_to_one"
	case PatternManyToMany:
		return "many_to_many"
	default:
		return "one_to_one"
	}
}

// CardinalityPatternOf describes the cardinality transformation pattern.
func (info CapShapeInfo) CardinalityPatternOf() CardinalityPattern {
	inp := info.Input.Cardinality
	out := info.Output.Cardinality

	switch {
	case inp == CardinalitySingle && out == CardinalitySingle:
		return PatternOneToOne
	case inp == CardinalitySingle && out == CardinalitySequence:
		return PatternOneToMany
	case inp == CardinalitySequence && out == CardinalitySingle:
		return PatternManyToOne
	case inp == CardinalitySequence && out == CardinalitySequence:
		return PatternManyToMany
	case inp == CardinalityAtLeastOne && out == CardinalitySingle:
		return PatternOneToOne
	case inp == CardinalityAtLeastOne && out == CardinalitySequence:
		return PatternOneToMany
	case inp == CardinalitySingle && out == CardinalityAtLeastOne:
		return PatternOneToOne
	case inp == CardinalitySequence && out == CardinalityAtLeastOne:
		return PatternManyToMany
	default:
		return PatternOneToOne
	}
}

// StructuresMatch checks if input/output structures match.
func (info CapShapeInfo) StructuresMatch() bool {
	return info.Input.Structure == info.Output.Structure
}

// ProducesVector checks if this pattern may produce multiple outputs.
func (p CardinalityPattern) ProducesVector() bool {
	return p == PatternOneToMany || p == PatternManyToMany
}

// RequiresVector checks if this pattern requires multiple inputs.
func (p CardinalityPattern) RequiresVector() bool {
	return p == PatternManyToOne || p == PatternManyToMany
}

// StrandShapeAnalysis analyzes shape chain for a sequence of caps.
type StrandShapeAnalysis struct {
	CapInfos     []CapShapeInfo
	FanOutPoints []int
	FanInPoints  []int
	IsValid      bool
	Error        string
}

// AnalyzeShapeChain analyzes a chain of caps for shape transitions.
func AnalyzeShapeChain(capInfos []CapShapeInfo) StrandShapeAnalysis {
	if len(capInfos) == 0 {
		return StrandShapeAnalysis{
			CapInfos: nil,
			IsValid:  true,
		}
	}

	var fanOutPoints []int
	var fanInPoints []int
	currentShape := capInfos[0].Input
	var errorMsg string

	for i, info := range capInfos {
		compat := info.Input.IsCompatibleWith(currentShape)

		switch {
		case compat.Kind == ShapeDirect:
			// ok
		case compat.Kind == ShapeWrapInArray:
			// ok
		case compat.RequiresFanOut():
			fanOutPoints = append(fanOutPoints, i)
		case compat.IsError():
			errorMsg = fmt.Sprintf(
				"Shape mismatch at cap %d (%s): %s - source has %s/%s, cap expects %s/%s",
				i, info.CapUrn, compat.Message,
				currentShape.Cardinality, currentShape.Structure,
				info.Input.Cardinality, info.Input.Structure,
			)
		}

		if errorMsg != "" {
			break
		}

		currentShape = info.Output
	}

	if errorMsg != "" {
		return StrandShapeAnalysis{
			CapInfos:     capInfos,
			FanOutPoints: fanOutPoints,
			FanInPoints:  fanInPoints,
			IsValid:      false,
			Error:        errorMsg,
		}
	}

	if len(fanOutPoints) > 0 {
		fanInPoints = append(fanInPoints, len(capInfos))
	}

	return StrandShapeAnalysis{
		CapInfos:     capInfos,
		FanOutPoints: fanOutPoints,
		FanInPoints:  fanInPoints,
		IsValid:      true,
	}
}

// RequiresTransformation checks if this chain requires any cardinality transformations.
func (a StrandShapeAnalysis) RequiresTransformation() bool {
	return len(a.FanOutPoints) > 0 || len(a.FanInPoints) > 0
}

// FinalOutputShape gets the final output shape of the chain.
func (a StrandShapeAnalysis) FinalOutputShape() *MediaShape {
	if len(a.CapInfos) == 0 {
		return nil
	}
	shape := a.CapInfos[len(a.CapInfos)-1].Output
	return &shape
}
