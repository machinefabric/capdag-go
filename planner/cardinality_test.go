package planner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TEST688: Tests IsMultiple method correctly identifies multi-value cardinalities
// Verifies Single returns false while Sequence and AtLeastOne return true
func Test688_is_multiple(t *testing.T) {
	assert.False(t, CardinalitySingle.IsMultiple())
	assert.True(t, CardinalitySequence.IsMultiple())
	assert.True(t, CardinalityAtLeastOne.IsMultiple())
}

// TEST689: Tests AcceptsSingle method identifies cardinalities that accept single values
// Verifies Single and AtLeastOne accept singles while Sequence does not
func Test689_accepts_single(t *testing.T) {
	assert.True(t, CardinalitySingle.AcceptsSingle())
	assert.False(t, CardinalitySequence.AcceptsSingle())
	assert.True(t, CardinalityAtLeastOne.AcceptsSingle())
}

// TEST690: Tests cardinality compatibility for single-to-single data flow
// Verifies Direct compatibility when both input and output are Single
func Test690_compatibility_single_to_single(t *testing.T) {
	assert.Equal(t, CardinalityDirect, CardinalitySingle.IsCompatibleWith(CardinalitySingle))
}

// TEST691: Tests cardinality compatibility when wrapping single value into array
// Verifies WrapInArray compatibility when Sequence expects Single input
func Test691_compatibility_single_to_vector(t *testing.T) {
	assert.Equal(t, CardinalityWrapInArray, CardinalitySequence.IsCompatibleWith(CardinalitySingle))
}

// TEST692: Tests cardinality compatibility when unwrapping array to singles
// Verifies RequiresFanOut compatibility when Single expects Sequence input
func Test692_compatibility_vector_to_single(t *testing.T) {
	assert.Equal(t, CardinalityRequiresFanOut, CardinalitySingle.IsCompatibleWith(CardinalitySequence))
}

// TEST693: Tests cardinality compatibility for sequence-to-sequence data flow
// Verifies Direct compatibility when both input and output are Sequence
func Test693_compatibility_vector_to_vector(t *testing.T) {
	assert.Equal(t, CardinalityDirect, CardinalitySequence.IsCompatibleWith(CardinalitySequence))
}

// TEST697: Tests CapShapeInfo correctly identifies one-to-one pattern
// Verifies Single input and Single output result in OneToOne pattern
func Test697_cap_shape_info_one_to_one(t *testing.T) {
	info := CapShapeInfoFromSpecs("cap:test", "media:ext=pdf", "media:ext=png;image")
	assert.Equal(t, CardinalitySingle, info.Input.Cardinality)
	assert.Equal(t, CardinalitySingle, info.Output.Cardinality)
	assert.Equal(t, PatternOneToOne, info.CardinalityPatternOf())
}

// TEST698: CapShapeInfo cardinality is always Single when derived from URN
// Cardinality comes from context (IsSequence), not from URN tags.
// The list tag is a semantic type property, not a cardinality indicator.
func Test698_cap_shape_info_cardinality_always_single_from_urn(t *testing.T) {
	info := CapShapeInfoFromSpecs("cap:pdf-to-pages", "media:ext=pdf", "media:ext=png;list")
	assert.Equal(t, CardinalitySingle, info.Input.Cardinality)
	assert.Equal(t, CardinalitySingle, info.Output.Cardinality)
	assert.Equal(t, PatternOneToOne, info.CardinalityPatternOf())
}

// TEST699: CapShapeInfo cardinality from URN is always Single; ManyToOne requires IsSequence context
func Test699_cap_shape_info_list_urn_still_single_cardinality(t *testing.T) {
	// URN parsing always yields Single — the "list" tag is a structure marker, not cardinality
	fromUrn := CapShapeInfoFromSpecs("cap:merge-pdfs", "media:list;pdf", "media:ext=pdf")
	assert.Equal(t, CardinalitySingle, fromUrn.Input.Cardinality)
	assert.Equal(t, CardinalitySingle, fromUrn.Output.Cardinality)
	assert.Equal(t, PatternOneToOne, fromUrn.CardinalityPatternOf())

	// With Sequence cardinality on input (set from IsSequence wire context), pattern becomes ManyToOne
	withSeq := CapShapeInfo{
		Input:  MediaShape{Cardinality: CardinalitySequence, Structure: fromUrn.Input.Structure},
		Output: fromUrn.Output,
		CapUrn: "cap:merge-pdfs",
	}
	assert.Equal(t, CardinalitySequence, withSeq.Input.Cardinality)
	assert.Equal(t, CardinalitySingle, withSeq.Output.Cardinality)
	assert.Equal(t, PatternManyToOne, withSeq.CardinalityPatternOf())
}

// TEST709: Tests CardinalityPattern correctly identifies patterns that produce vectors
// Verifies OneToMany and ManyToMany return true, others return false
func Test709_pattern_produces_vector(t *testing.T) {
	assert.False(t, PatternOneToOne.ProducesVector())
	assert.True(t, PatternOneToMany.ProducesVector())
	assert.False(t, PatternManyToOne.ProducesVector())
	assert.True(t, PatternManyToMany.ProducesVector())
}

// TEST710: Tests CardinalityPattern correctly identifies patterns that require vectors
// Verifies ManyToOne and ManyToMany return true, others return false
func Test710_pattern_requires_vector(t *testing.T) {
	assert.False(t, PatternOneToOne.RequiresVector())
	assert.False(t, PatternOneToMany.RequiresVector())
	assert.True(t, PatternManyToOne.RequiresVector())
	assert.True(t, PatternManyToMany.RequiresVector())
}

// TEST711: Tests shape chain analysis for simple linear one-to-one capability chains
func Test711_strand_shape_analysis_simple_linear(t *testing.T) {
	infos := []CapShapeInfo{
		CapShapeInfoFromSpecs("cap:pdf-to-png", "media:ext=pdf", "media:ext=png;image"),
		CapShapeInfoFromSpecs("cap:resize", "media:ext=png;image", "media:ext=png;image"),
	}
	analysis := AnalyzeShapeChain(infos)
	assert.True(t, analysis.IsValid)
	assert.Empty(t, analysis.FanOutPoints)
	assert.False(t, analysis.RequiresTransformation())
}

// TEST712: Tests shape chain analysis detects fan-out points in capability chains
// Fan-out requires Sequence cardinality on the cap's output (from is_sequence=true wire context)
func Test712_strand_shape_analysis_with_fan_out(t *testing.T) {
	// Simulate pdf-to-pages with Sequence output (is_sequence=true)
	pdfToPages := CapShapeInfo{
		Input:  MediaShapeFromUrn("media:ext=pdf"),
		Output: MediaShape{Cardinality: CardinalitySequence, Structure: StructureFromMediaUrn("media:ext=png;image")},
		CapUrn: "cap:pdf-to-pages",
	}
	infos := []CapShapeInfo{
		pdfToPages,
		CapShapeInfoFromSpecs("cap:thumbnail", "media:ext=png;image", "media:ext=png;image"),
	}
	analysis := AnalyzeShapeChain(infos)
	assert.True(t, analysis.IsValid)
	assert.Equal(t, []int{1}, analysis.FanOutPoints)
	assert.True(t, analysis.RequiresTransformation())
}

// TEST713: Tests shape chain analysis handles empty capability chains correctly
func Test713_strand_shape_analysis_empty(t *testing.T) {
	analysis := AnalyzeShapeChain(nil)
	assert.True(t, analysis.IsValid)
	assert.False(t, analysis.RequiresTransformation())
}

// TEST714: Tests InputCardinality String() representation
func Test714_cardinality_string(t *testing.T) {
	assert.Equal(t, "single", CardinalitySingle.String())
	assert.Equal(t, "sequence", CardinalitySequence.String())
	assert.Equal(t, "at_least_one", CardinalityAtLeastOne.String())
}

// TEST715: Tests CardinalityPattern String() representation
func Test715_pattern_string(t *testing.T) {
	assert.Equal(t, "one_to_many", PatternOneToMany.String())
	assert.Equal(t, "one_to_one", PatternOneToOne.String())
	assert.Equal(t, "many_to_one", PatternManyToOne.String())
	assert.Equal(t, "many_to_many", PatternManyToMany.String())
}

// TEST720: Tests InputStructure correctly identifies opaque media URNs
// Verifies that URNs without record marker are parsed as Opaque
func Test720_from_media_urn_opaque(t *testing.T) {
	assert.Equal(t, StructureOpaque, StructureFromMediaUrn("media:ext=pdf"))
	assert.Equal(t, StructureOpaque, StructureFromMediaUrn("media:enc=utf-8"))
	assert.Equal(t, StructureOpaque, StructureFromMediaUrn("media:integer"))
	// List marker doesn't affect structure
	assert.Equal(t, StructureOpaque, StructureFromMediaUrn("media:file-path;list"))
}

// TEST721: Tests InputStructure correctly identifies record media URNs
// Verifies that URNs with record marker tag are parsed as Record
func Test721_from_media_urn_record(t *testing.T) {
	assert.Equal(t, StructureRecord, StructureFromMediaUrn("media:fmt=json;record"))
	assert.Equal(t, StructureRecord, StructureFromMediaUrn("media:record;enc=utf-8"))
	assert.Equal(t, StructureRecord, StructureFromMediaUrn("media:file-metadata;record;enc=utf-8"))
	// List of records
	assert.Equal(t, StructureRecord, StructureFromMediaUrn("media:fmt=json;list;record"))
}

// TEST722: Tests structure compatibility for opaque-to-opaque data flow
func Test722_structure_compatibility_opaque_to_opaque(t *testing.T) {
	result := StructureOpaque.IsCompatibleWith(StructureOpaque)
	assert.Equal(t, StructureCompatibilityDirect, result)
}

// TEST723: Tests structure compatibility for record-to-record data flow
func Test723_structure_compatibility_record_to_record(t *testing.T) {
	result := StructureRecord.IsCompatibleWith(StructureRecord)
	assert.Equal(t, StructureCompatibilityDirect, result)
}

// TEST724: Tests structure incompatibility for opaque-to-record flow
func Test724_structure_incompatibility_opaque_to_record(t *testing.T) {
	result := StructureRecord.IsCompatibleWith(StructureOpaque)
	assert.True(t, result.IsError())
}

// TEST725: Tests structure incompatibility for record-to-opaque flow
func Test725_structure_incompatibility_record_to_opaque(t *testing.T) {
	result := StructureOpaque.IsCompatibleWith(StructureRecord)
	assert.True(t, result.IsError())
}

// TEST726: Tests applying Record structure adds record marker to URN
func Test726_apply_structure_add_record(t *testing.T) {
	result := StructureRecord.ApplyToUrn("media:fmt=json")
	assert.Contains(t, result, "record")
}

// TEST727: Tests applying Opaque structure removes record marker from URN
func Test727_apply_structure_remove_record(t *testing.T) {
	result := StructureOpaque.ApplyToUrn("media:fmt=json;record")
	assert.False(t, strings.Contains(result, "record"), "record tag must be removed")
}

// TEST730: Tests MediaShape correctly parses all four combinations
func Test730_media_shape_from_urn_all_combinations(t *testing.T) {
	// Scalar opaque (default)
	shape := MediaShapeFromUrn("media:enc=utf-8")
	assert.Equal(t, CardinalitySingle, shape.Cardinality)
	assert.Equal(t, StructureOpaque, shape.Structure)

	// Scalar record
	shape = MediaShapeFromUrn("media:fmt=json;record")
	assert.Equal(t, CardinalitySingle, shape.Cardinality)
	assert.Equal(t, StructureRecord, shape.Structure)

	// List opaque — cardinality is always Single from URN (shape comes from wire context)
	shape = MediaShapeFromUrn("media:file-path;list")
	assert.Equal(t, CardinalitySingle, shape.Cardinality)
	assert.Equal(t, StructureOpaque, shape.Structure)

	// List record — cardinality is always Single from URN
	shape = MediaShapeFromUrn("media:fmt=json;list;record")
	assert.Equal(t, CardinalitySingle, shape.Cardinality)
	assert.Equal(t, StructureRecord, shape.Structure)
}

// TEST731: Tests MediaShape compatibility for matching shapes (Direct)
func Test731_media_shape_compatible_direct(t *testing.T) {
	scalarOpaque := ScalarOpaque()
	scalarRecord := ScalarRecord()
	listOpaque := ListOpaque()
	listRecord := ListRecord()

	assert.Equal(t, ShapeCompatibilityDirect, scalarOpaque.IsCompatibleWith(scalarOpaque))
	assert.Equal(t, ShapeCompatibilityDirect, scalarRecord.IsCompatibleWith(scalarRecord))
	assert.Equal(t, ShapeCompatibilityDirect, listOpaque.IsCompatibleWith(listOpaque))
	assert.Equal(t, ShapeCompatibilityDirect, listRecord.IsCompatibleWith(listRecord))
}

// TEST732: Tests MediaShape compatibility for cardinality changes with matching structure
func Test732_media_shape_cardinality_changes(t *testing.T) {
	scalarOpaque := ScalarOpaque()
	listOpaque := ListOpaque()
	scalarRecord := ScalarRecord()
	listRecord := ListRecord()

	// Scalar to list (same structure) = WrapInArray
	assert.Equal(t, ShapeCompatibilityWrapInArray, listOpaque.IsCompatibleWith(scalarOpaque))
	assert.Equal(t, ShapeCompatibilityWrapInArray, listRecord.IsCompatibleWith(scalarRecord))

	// List to scalar (same structure) = RequiresFanOut
	assert.Equal(t, ShapeCompatibilityRequiresFanOut, scalarOpaque.IsCompatibleWith(listOpaque))
	assert.Equal(t, ShapeCompatibilityRequiresFanOut, scalarRecord.IsCompatibleWith(listRecord))
}

// TEST733: Tests MediaShape incompatibility when structures don't match
func Test733_media_shape_structure_mismatch(t *testing.T) {
	scalarOpaque := ScalarOpaque()
	scalarRecord := ScalarRecord()
	listOpaque := ListOpaque()
	listRecord := ListRecord()

	// Structure mismatch = Incompatible regardless of cardinality
	assert.True(t, scalarRecord.IsCompatibleWith(scalarOpaque).IsError())
	assert.True(t, scalarOpaque.IsCompatibleWith(scalarRecord).IsError())
	assert.True(t, listRecord.IsCompatibleWith(listOpaque).IsError())
	assert.True(t, listOpaque.IsCompatibleWith(listRecord).IsError())

	// Cross cardinality + structure mismatch
	assert.True(t, listRecord.IsCompatibleWith(scalarOpaque).IsError())
	assert.True(t, scalarOpaque.IsCompatibleWith(listRecord).IsError())
}

// TEST740: Tests CapShapeInfo correctly parses cap specs
func Test740_cap_shape_info_from_specs(t *testing.T) {
	info := CapShapeInfoFromSpecs("cap:test", "media:enc=utf-8", "media:fmt=json;record")
	assert.Equal(t, CardinalitySingle, info.Input.Cardinality)
	assert.Equal(t, StructureOpaque, info.Input.Structure)
	assert.Equal(t, CardinalitySingle, info.Output.Cardinality)
	assert.Equal(t, StructureRecord, info.Output.Structure)
}

// TEST741: Tests CapShapeInfo pattern detection — OneToMany requires Sequence output cardinality
func Test741_cap_shape_info_pattern(t *testing.T) {
	// Simulate one-to-many (output is_sequence=true on wire)
	oneToMany := CapShapeInfo{
		Input:  MediaShapeFromUrn("media:ext=pdf"),
		Output: MediaShape{Cardinality: CardinalitySequence, Structure: StructureFromMediaUrn("media:disbound-page;enc=utf-8")},
		CapUrn: "cap:disbind",
	}
	assert.Equal(t, PatternOneToMany, oneToMany.CardinalityPatternOf())
}

// TEST750: Tests shape chain analysis for valid chain with matching structures
func Test750_strand_shape_valid(t *testing.T) {
	infos := []CapShapeInfo{
		CapShapeInfoFromSpecs("cap:resize", "media:ext=png;image", "media:ext=png;image"),
		CapShapeInfoFromSpecs("cap:compress", "media:ext=png;image", "media:ext=png;image"),
	}
	analysis := AnalyzeShapeChain(infos)
	assert.True(t, analysis.IsValid)
	assert.Empty(t, analysis.Error)
}

// TEST751: Tests shape chain analysis detects structure mismatch
func Test751_strand_shape_structure_mismatch(t *testing.T) {
	infos := []CapShapeInfo{
		CapShapeInfoFromSpecs("cap:extract", "media:ext=pdf", "media:enc=utf-8"),
		// This cap expects record but gets opaque — should fail
		CapShapeInfoFromSpecs("cap:parse", "media:fmt=json;record", "media:data;record"),
	}
	analysis := AnalyzeShapeChain(infos)
	assert.False(t, analysis.IsValid)
	assert.NotEmpty(t, analysis.Error)
	assert.Contains(t, analysis.Error, "Shape mismatch")
}

// TEST752: Tests shape chain analysis with fan-out (matching structures)
// Fan-out requires Sequence output cardinality (from is_sequence=true wire context)
func Test752_strand_shape_with_fanout(t *testing.T) {
	disbind := CapShapeInfo{
		Input:  MediaShapeFromUrn("media:ext=pdf"),
		Output: MediaShape{Cardinality: CardinalitySequence, Structure: StructureFromMediaUrn("media:page;enc=utf-8")},
		CapUrn: "cap:disbind",
	}
	infos := []CapShapeInfo{
		disbind,
		CapShapeInfoFromSpecs("cap:process", "media:enc=utf-8", "media:result;enc=utf-8"),
	}
	analysis := AnalyzeShapeChain(infos)
	assert.True(t, analysis.IsValid)
	assert.True(t, analysis.RequiresTransformation())
	assert.Equal(t, []int{1}, analysis.FanOutPoints)
}

// TEST753: Tests shape chain analysis correctly handles list-to-list record flow
func Test753_strand_shape_list_record_to_list_record(t *testing.T) {
	infos := []CapShapeInfo{
		CapShapeInfoFromSpecs("cap:parse_csv", "media:fmt=csv", "media:fmt=json;list;record"),
		CapShapeInfoFromSpecs("cap:transform", "media:fmt=json;list;record", "media:result;list;record"),
	}
	analysis := AnalyzeShapeChain(infos)
	assert.True(t, analysis.IsValid)
	assert.False(t, analysis.RequiresTransformation())
}
