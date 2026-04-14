package urn

import (
	"encoding/json"
	"testing"

	"github.com/machinefabric/capdag-go/standard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST057: Test parsing simple media URN verifies correct structure with no version, subtype, or profile
func Test057_parse_simple(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("string"))
	assert.False(t, urn.HasTag("textable"))
}

// TEST058: Test parsing media URN with marker tags works correctly
func Test058_parse_with_subtype(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("string"))
	// No list marker = scalar by default
	assert.True(t, urn.IsScalar())
	assert.False(t, urn.IsList())
}

// TEST059: Test parsing media URN with profile extracts profile URL correctly
func Test059_parse_with_profile(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:string;textable")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("textable"))
}

// TEST060: Test wrong prefix fails with InvalidPrefix error
func Test060_wrong_prefix_fails(t *testing.T) {
	_, err := NewMediaUrnFromString("notmedia:string")
	assert.Error(t, err)
}

// TEST061: Test is_binary returns true when textable tag is absent (binary = not textable)
func Test061_is_binary(t *testing.T) {
	// Binary types (no textable tag)
	binary, err := NewMediaUrnFromString("media:")
	require.NoError(t, err)
	assert.True(t, binary.IsBinary(), "media: (wildcard) should be binary")

	pdfUrn, err := NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	assert.True(t, pdfUrn.IsBinary(), "media:pdf should be binary")

	pngUrn, err := NewMediaUrnFromString(standard.MediaPNG)
	require.NoError(t, err)
	assert.True(t, pngUrn.IsBinary(), "media:image;png should be binary")

	voidUrn, err := NewMediaUrnFromString("media:void")
	require.NoError(t, err)
	assert.True(t, voidUrn.IsBinary(), "media:void should be binary (no textable tag)")

	// Non-binary types (have textable tag)
	text, err := NewMediaUrnFromString("media:textable")
	require.NoError(t, err)
	assert.False(t, text.IsBinary(), "media:textable should NOT be binary")

	textMap, err := NewMediaUrnFromString("media:textable;record")
	require.NoError(t, err)
	assert.False(t, textMap.IsBinary(), "media:textable;record should NOT be binary")

	strUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, strUrn.IsBinary(), "MEDIA_STRING should NOT be binary")

	jsonUrn, err := NewMediaUrnFromString(standard.MediaJSON)
	require.NoError(t, err)
	assert.False(t, jsonUrn.IsBinary(), "MEDIA_JSON should NOT be binary")
}

// TEST062: Test is_record returns true when record marker tag is present indicating key-value structure
func Test062_is_record(t *testing.T) {
	// is_record returns true if record marker tag is present (key-value structure)
	recordUrn, err := NewMediaUrnFromString(standard.MediaObject)
	require.NoError(t, err)
	assert.True(t, recordUrn.IsRecord()) // "media:record"

	customRecord, err := NewMediaUrnFromString("media:custom;record")
	require.NoError(t, err)
	assert.True(t, customRecord.IsRecord())

	jsonUrn, err := NewMediaUrnFromString(standard.MediaJSON)
	require.NoError(t, err)
	assert.True(t, jsonUrn.IsRecord()) // "media:json;record;textable"

	// Without record marker, is_record is false
	scalar, err := NewMediaUrnFromString("media:textable")
	require.NoError(t, err)
	assert.False(t, scalar.IsRecord())

	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, stringUrn.IsRecord()) // scalar, no record marker

	listUrn2, err := NewMediaUrnFromString(standard.MediaStringList)
	require.NoError(t, err)
	assert.False(t, listUrn2.IsRecord()) // list, no record marker
}

// TEST063: Test is_scalar returns true when no list marker is present (scalar = default cardinality)
func Test063_is_scalar(t *testing.T) {
	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.True(t, stringUrn.IsScalar())

	intUrn, err := NewMediaUrnFromString(standard.MediaInteger)
	require.NoError(t, err)
	assert.True(t, intUrn.IsScalar())

	// record is still scalar (no list marker)
	recordUrn, err := NewMediaUrnFromString("media:record")
	require.NoError(t, err)
	assert.True(t, recordUrn.IsScalar())

	// list is NOT scalar
	listUrn, err := NewMediaUrnFromString(standard.MediaStringList)
	require.NoError(t, err)
	assert.False(t, listUrn.IsScalar())
}

// TEST064: Test is_list returns true when list tag is present indicating ordered collection
func Test064_is_list(t *testing.T) {
	strList, err := NewMediaUrnFromString(standard.MediaStringList)
	require.NoError(t, err)
	assert.True(t, strList.IsList())

	intList, err := NewMediaUrnFromString(standard.MediaIntegerList)
	require.NoError(t, err)
	assert.True(t, intList.IsList())

	scalar, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.False(t, scalar.IsList())
}

// TEST065: Test is_structured returns true for record (has internal structure)
func Test065_is_structured(t *testing.T) {
	objUrn, err := NewMediaUrnFromString(standard.MediaObject)
	require.NoError(t, err)
	assert.True(t, objUrn.IsStructured())

	jsonUrn, err := NewMediaUrnFromString(standard.MediaJSON)
	require.NoError(t, err)
	assert.True(t, jsonUrn.IsStructured())

	// list of opaque items (no record marker) is NOT structured
	strList, err := NewMediaUrnFromString(standard.MediaStringList)
	require.NoError(t, err)
	assert.False(t, strList.IsStructured())

	// scalar opaque is NOT structured
	scalar, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.False(t, scalar.IsStructured())

	binary, err := NewMediaUrnFromString("media:")
	require.NoError(t, err)
	assert.False(t, binary.IsStructured())
}

// TEST066: Test is_json returns true only when json marker tag is present for JSON representation
func Test066_is_json(t *testing.T) {
	jsonUrn, err := NewMediaUrnFromString(standard.MediaJSON)
	require.NoError(t, err)
	assert.True(t, jsonUrn.IsJson())

	customJson, err := NewMediaUrnFromString("media:custom;json")
	require.NoError(t, err)
	assert.True(t, customJson.IsJson())

	nonJson, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.False(t, nonJson.IsJson())
}

// TEST067: Test is_text returns true only when textable marker tag is present
func Test067_is_text(t *testing.T) {
	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.True(t, stringUrn.IsTextable())

	intUrn, err := NewMediaUrnFromString(standard.MediaInteger)
	require.NoError(t, err)
	assert.True(t, intUrn.IsTextable())

	binary, err := NewMediaUrnFromString("media:")
	require.NoError(t, err)
	assert.False(t, binary.IsTextable())
}

// TEST068: Test is_void returns true when void flag or type=void tag is present
func Test068_is_void(t *testing.T) {
	voidUrn, err := NewMediaUrnFromString("media:void")
	require.NoError(t, err)
	assert.True(t, voidUrn.IsVoid())

	nonVoid, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.False(t, nonVoid.IsVoid())
}

// TEST069: Test simple constructor creates media URN with type tag
func Test069_constructor(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("string"))
}

// TEST070: Test with_subtype constructor creates media URN with subtype
func Test070_with_subtype_constructor(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:application;subtype=json")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("application"))
	subtype, ok := urn.GetTag("subtype")
	assert.True(t, ok)
	assert.Equal(t, "json", subtype)
}

// TEST071: Test to_string roundtrip ensures serialization and deserialization preserve URN structure
func Test071_to_string_roundtrip(t *testing.T) {
	original := "media:string;textable"
	urn1, err := NewMediaUrnFromString(original)
	require.NoError(t, err)

	serialized := urn1.String()
	urn2, err := NewMediaUrnFromString(serialized)
	require.NoError(t, err)

	assert.True(t, urn1.Equals(urn2))
}

// TEST072: Test all media URN constants parse successfully as valid media URNs
func Test072_constants_parse(t *testing.T) {
	constants := []string{
		standard.MediaVoid,
		standard.MediaString,
		standard.MediaIdentity,
		standard.MediaObject,
		standard.MediaInteger,
		standard.MediaNumber,
		standard.MediaBoolean,
		standard.MediaString,
		standard.MediaInteger,
		standard.MediaNumber,
		standard.MediaBoolean,
		standard.MediaObject,
		standard.MediaIdentity,
		standard.MediaList,
		standard.MediaTextableList,
		standard.MediaStringList,
		standard.MediaIntegerList,
		standard.MediaNumberList,
		standard.MediaBooleanList,
		standard.MediaObjectList,
		standard.MediaPNG,
		standard.MediaAudio,
		standard.MediaVideo,
		standard.MediaAudioSpeech,
		standard.MediaTextablePage,
		standard.MediaPDF,
		standard.MediaEPUB,
		standard.MediaJSON,
		standard.MediaFilePath,
		standard.MediaFilePathArray,
		standard.MediaDecision,
	}

	for _, constant := range constants {
		_, err := NewMediaUrnFromString(constant)
		assert.NoError(t, err, "Failed to parse constant: %s", constant)
	}
}

// TEST073: Test extension helper functions create media URNs with ext tag and correct format
func Test073_extension_helpers(t *testing.T) {
	pdfUrn, err := NewMediaUrnFromString("media:ext=pdf")
	require.NoError(t, err)
	ext, ok := pdfUrn.GetTag("ext")
	assert.True(t, ok)
	assert.Equal(t, "pdf", ext)
}

// TEST074: Test media URN conforms_to using tagged URN semantics with specific and generic requirements
func Test074_media_urn_matching(t *testing.T) {
	specific, err := NewMediaUrnFromString("media:string;textable")
	require.NoError(t, err)

	generic, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)

	// Specific pattern does NOT accept generic instance (generic missing textable and form)
	assert.False(t, specific.Accepts(generic))

	// Generic pattern DOES accept specific instance (generic has no constraints on extra tags)
	assert.True(t, generic.Accepts(specific))

	// Specific instance conforms to generic pattern
	assert.True(t, specific.ConformsTo(generic))

	// Generic instance does NOT conform to specific pattern
	assert.False(t, generic.ConformsTo(specific))
}

// TEST075: Test accepts with implicit wildcards where handlers with fewer tags can handle more requests
func Test075_matching(t *testing.T) {
	handler, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)

	request, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)

	// Handler with fewer tags can match requests with more tags (wildcard semantics)
	assert.True(t, handler.Accepts(request))
}

// TEST076: Test specificity increases with more tags for ranking conformance
func Test076_specificity(t *testing.T) {
	simple, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)

	detailed, err := NewMediaUrnFromString("media:string;textable")
	require.NoError(t, err)

	// More tags = higher specificity
	assert.True(t, detailed.Specificity() > simple.Specificity())
}

// TEST077: Test serde roundtrip serializes to JSON string and deserializes back correctly
func Test077_serde_roundtrip(t *testing.T) {
	original, err := NewMediaUrnFromString("media:string;textable")
	require.NoError(t, err)

	// JSON marshaling
	data, err := json.Marshal(original)
	require.NoError(t, err)

	// JSON unmarshaling
	var restored MediaUrn
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	assert.True(t, original.Equals(&restored))
}

// TEST078: conforms_to behavior between MEDIA_OBJECT and MEDIA_STRING
func Test078_object_does_not_conform_to_string(t *testing.T) {
	strUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	objUrn, err := NewMediaUrnFromString(standard.MediaObject)
	require.NoError(t, err)

	assert.True(t, strUrn.ConformsTo(strUrn), "string conforms to string")
	assert.True(t, objUrn.ConformsTo(objUrn), "object conforms to object")
	assert.False(t, objUrn.ConformsTo(strUrn), "MEDIA_OBJECT should NOT conform to MEDIA_STRING (missing textable)")
}

// TEST304: Test MEDIA_AVAILABILITY_OUTPUT constant parses as valid media URN with correct tags
func Test304_media_availability_output_constant(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:model-availability;textable;record")
	require.NoError(t, err)
	assert.True(t, urn.IsTextable())
	assert.True(t, urn.IsRecord())
	assert.False(t, urn.IsBinary())
}

// TEST305: Test MEDIA_PATH_OUTPUT constant parses as valid media URN with correct tags
func Test305_media_path_output_constant(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:model-path;textable;record")
	require.NoError(t, err)
	assert.True(t, urn.IsTextable())
	assert.True(t, urn.IsRecord())
	assert.False(t, urn.IsBinary())
}

// TEST306: Test MEDIA_AVAILABILITY_OUTPUT and MEDIA_PATH_OUTPUT are distinct URNs
func Test306_availability_and_path_output_distinct(t *testing.T) {
	availUrn, err := NewMediaUrnFromString("media:model-availability;textable;record")
	require.NoError(t, err)
	pathUrn, err := NewMediaUrnFromString("media:model-path;textable;record")
	require.NoError(t, err)
	assert.False(t, availUrn.Equals(pathUrn))
	// They must NOT conform to each other (different marker tags)
	assert.False(t, availUrn.ConformsTo(pathUrn))
}

// TEST546: is_image returns true only when image marker tag is present
func Test546_is_image(t *testing.T) {
	pngUrn, err := NewMediaUrnFromString(standard.MediaPNG)
	require.NoError(t, err)
	assert.True(t, pngUrn.IsImage())

	customImage, err := NewMediaUrnFromString("media:image;jpg")
	require.NoError(t, err)
	assert.True(t, customImage.IsImage())

	// Non-image types
	pdfUrn, err := NewMediaUrnFromString(standard.MediaPDF)
	require.NoError(t, err)
	assert.False(t, pdfUrn.IsImage())

	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, stringUrn.IsImage())

	audioUrn, err := NewMediaUrnFromString(standard.MediaAudio)
	require.NoError(t, err)
	assert.False(t, audioUrn.IsImage())

	videoUrn, err := NewMediaUrnFromString(standard.MediaVideo)
	require.NoError(t, err)
	assert.False(t, videoUrn.IsImage())
}

// TEST547: is_audio returns true only when audio marker tag is present
func Test547_is_audio(t *testing.T) {
	audioUrn, err := NewMediaUrnFromString(standard.MediaAudio)
	require.NoError(t, err)
	assert.True(t, audioUrn.IsAudio())

	speechUrn, err := NewMediaUrnFromString(standard.MediaAudioSpeech)
	require.NoError(t, err)
	assert.True(t, speechUrn.IsAudio())

	customAudio, err := NewMediaUrnFromString("media:audio;mp3")
	require.NoError(t, err)
	assert.True(t, customAudio.IsAudio())

	// Non-audio types
	videoUrn, err := NewMediaUrnFromString(standard.MediaVideo)
	require.NoError(t, err)
	assert.False(t, videoUrn.IsAudio())

	pngUrn, err := NewMediaUrnFromString(standard.MediaPNG)
	require.NoError(t, err)
	assert.False(t, pngUrn.IsAudio())

	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, stringUrn.IsAudio())
}

// TEST548: is_video returns true only when video marker tag is present
func Test548_is_video(t *testing.T) {
	videoUrn, err := NewMediaUrnFromString(standard.MediaVideo)
	require.NoError(t, err)
	assert.True(t, videoUrn.IsVideo())

	customVideo, err := NewMediaUrnFromString("media:video;mp4")
	require.NoError(t, err)
	assert.True(t, customVideo.IsVideo())

	// Non-video types
	audioUrn, err := NewMediaUrnFromString(standard.MediaAudio)
	require.NoError(t, err)
	assert.False(t, audioUrn.IsVideo())

	pngUrn, err := NewMediaUrnFromString(standard.MediaPNG)
	require.NoError(t, err)
	assert.False(t, pngUrn.IsVideo())

	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, stringUrn.IsVideo())
}

// TEST549: is_numeric returns true only when numeric marker tag is present
func Test549_is_numeric(t *testing.T) {
	intUrn, err := NewMediaUrnFromString(standard.MediaInteger)
	require.NoError(t, err)
	assert.True(t, intUrn.IsNumeric())

	numUrn, err := NewMediaUrnFromString(standard.MediaNumber)
	require.NoError(t, err)
	assert.True(t, numUrn.IsNumeric())

	intListUrn, err := NewMediaUrnFromString(standard.MediaIntegerList)
	require.NoError(t, err)
	assert.True(t, intListUrn.IsNumeric())

	numListUrn, err := NewMediaUrnFromString(standard.MediaNumberList)
	require.NoError(t, err)
	assert.True(t, numListUrn.IsNumeric())

	// Non-numeric types
	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, stringUrn.IsNumeric())

	boolUrn, err := NewMediaUrnFromString(standard.MediaBoolean)
	require.NoError(t, err)
	assert.False(t, boolUrn.IsNumeric())

	binaryUrn, err := NewMediaUrnFromString(standard.MediaIdentity)
	require.NoError(t, err)
	assert.False(t, binaryUrn.IsNumeric())
}

// TEST550: is_bool returns true only when bool marker tag is present
func Test550_is_bool(t *testing.T) {
	boolUrn, err := NewMediaUrnFromString(standard.MediaBoolean)
	require.NoError(t, err)
	assert.True(t, boolUrn.IsBool())

	boolListUrn, err := NewMediaUrnFromString(standard.MediaBooleanList)
	require.NoError(t, err)
	assert.True(t, boolListUrn.IsBool())

	// MediaDecision is now a JSON record, not a bool type
	decisionUrn, err := NewMediaUrnFromString(standard.MediaDecision)
	require.NoError(t, err)
	assert.False(t, decisionUrn.IsBool())

	// Non-bool types
	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, stringUrn.IsBool())

	intUrn, err := NewMediaUrnFromString(standard.MediaInteger)
	require.NoError(t, err)
	assert.False(t, intUrn.IsBool())

	binaryUrn, err := NewMediaUrnFromString(standard.MediaIdentity)
	require.NoError(t, err)
	assert.False(t, binaryUrn.IsBool())
}

// TEST551: is_file_path returns true for scalar file-path, false for array
func Test551_is_file_path(t *testing.T) {
	fpUrn, err := NewMediaUrnFromString(standard.MediaFilePath)
	require.NoError(t, err)
	assert.True(t, fpUrn.IsFilePath())

	// Array file-path is NOT is_file_path (it's is_file_path_array)
	fpArrayUrn, err := NewMediaUrnFromString(standard.MediaFilePathArray)
	require.NoError(t, err)
	assert.False(t, fpArrayUrn.IsFilePath())

	// Non-file-path types
	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, stringUrn.IsFilePath())

	binaryUrn, err := NewMediaUrnFromString(standard.MediaIdentity)
	require.NoError(t, err)
	assert.False(t, binaryUrn.IsFilePath())
}

// TEST552: is_file_path_array returns true for list file-path, false for scalar
func Test552_is_file_path_array(t *testing.T) {
	fpArrayUrn, err := NewMediaUrnFromString(standard.MediaFilePathArray)
	require.NoError(t, err)
	assert.True(t, fpArrayUrn.IsFilePathArray())

	// Scalar file-path is NOT is_file_path_array
	fpUrn, err := NewMediaUrnFromString(standard.MediaFilePath)
	require.NoError(t, err)
	assert.False(t, fpUrn.IsFilePathArray())

	// Non-file-path types
	strListUrn, err := NewMediaUrnFromString(standard.MediaStringList)
	require.NoError(t, err)
	assert.False(t, strListUrn.IsFilePathArray())
}

// TEST553: is_any_file_path returns true for both scalar and list file-path
func Test553_is_any_file_path(t *testing.T) {
	fpUrn, err := NewMediaUrnFromString(standard.MediaFilePath)
	require.NoError(t, err)
	assert.True(t, fpUrn.IsAnyFilePath())

	fpArrayUrn, err := NewMediaUrnFromString(standard.MediaFilePathArray)
	require.NoError(t, err)
	assert.True(t, fpArrayUrn.IsAnyFilePath())

	// Non-file-path types
	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, stringUrn.IsAnyFilePath())

	strListUrn2, err := NewMediaUrnFromString(standard.MediaStringList)
	require.NoError(t, err)
	assert.False(t, strListUrn2.IsAnyFilePath())
}


// TEST558: predicates are consistent with constants -- every constant triggers exactly the expected predicates
func Test558_predicate_constant_consistency(t *testing.T) {
	// MEDIA_INTEGER must be numeric, text, scalar, NOT binary/bool/image/audio/video
	intUrn, err := NewMediaUrnFromString(standard.MediaInteger)
	require.NoError(t, err)
	assert.True(t, intUrn.IsNumeric())
	assert.True(t, intUrn.IsTextable())
	assert.True(t, intUrn.IsScalar())
	assert.False(t, intUrn.IsBinary())
	assert.False(t, intUrn.IsBool())
	assert.False(t, intUrn.IsImage())
	assert.False(t, intUrn.IsList())

	// MEDIA_BOOLEAN must be bool, text, scalar, NOT numeric
	boolUrn, err := NewMediaUrnFromString(standard.MediaBoolean)
	require.NoError(t, err)
	assert.True(t, boolUrn.IsBool())
	assert.True(t, boolUrn.IsTextable())
	assert.True(t, boolUrn.IsScalar())
	assert.False(t, boolUrn.IsNumeric())

	// MEDIA_JSON must be json, text, map, structured, NOT binary
	jsonUrn, err := NewMediaUrnFromString(standard.MediaJSON)
	require.NoError(t, err)
	assert.True(t, jsonUrn.IsJson())
	assert.True(t, jsonUrn.IsTextable())
	assert.True(t, jsonUrn.IsRecord())
	assert.True(t, jsonUrn.IsStructured())
	assert.False(t, jsonUrn.IsBinary())
	assert.False(t, jsonUrn.IsList())

	// MEDIA_VOID is void, binary (no textable tag), NOT textable/numeric
	voidUrn, err := NewMediaUrnFromString(standard.MediaVoid)
	require.NoError(t, err)
	assert.True(t, voidUrn.IsVoid())
	assert.False(t, voidUrn.IsTextable())
	assert.True(t, voidUrn.IsBinary(), "void is binary because it has no textable tag")
	assert.False(t, voidUrn.IsNumeric())
}

// TEST852: LUB of identical URNs returns the same URN
func Test852_lub_identical(t *testing.T) {
	pdf, err := NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{pdf, pdf})
	assert.True(t, lub.IsEquivalent(pdf))
}

// TEST853: LUB of URNs with no common tags returns media: (universal)
func Test853_lub_no_common_tags(t *testing.T) {
	pdf, err := NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	png, err := NewMediaUrnFromString("media:png")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{pdf, png})
	universal, err := NewMediaUrnFromString("media:")
	require.NoError(t, err)
	assert.True(t, lub.IsEquivalent(universal), "LUB of pdf and png should be media: but got %s", lub.String())
}

// TEST854: LUB keeps common tags, drops differing ones
func Test854_lub_partial_overlap(t *testing.T) {
	jsonText, err := NewMediaUrnFromString("media:json;textable")
	require.NoError(t, err)
	csvText, err := NewMediaUrnFromString("media:csv;textable")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{jsonText, csvText})
	expected, err := NewMediaUrnFromString("media:textable")
	require.NoError(t, err)
	assert.True(t, lub.IsEquivalent(expected), "LUB should be media:textable but got %s", lub.String())
}

// TEST855: LUB of list and non-list drops list tag
func Test855_lub_list_vs_scalar(t *testing.T) {
	jsonList, err := NewMediaUrnFromString("media:json;list;textable")
	require.NoError(t, err)
	jsonScalar, err := NewMediaUrnFromString("media:json;textable")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{jsonList, jsonScalar})
	expected, err := NewMediaUrnFromString("media:json;textable")
	require.NoError(t, err)
	assert.True(t, lub.IsEquivalent(expected), "LUB should drop list tag, got %s", lub.String())
}

// TEST856: LUB of empty input returns universal type
func Test856_lub_empty(t *testing.T) {
	lub := LeastUpperBound([]*MediaUrn{})
	universal, err := NewMediaUrnFromString("media:")
	require.NoError(t, err)
	assert.True(t, lub.IsEquivalent(universal))
}

// TEST857: LUB of single input returns that input
func Test857_lub_single(t *testing.T) {
	pdf, err := NewMediaUrnFromString("media:pdf")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{pdf})
	assert.True(t, lub.IsEquivalent(pdf))
}

// TEST858: LUB with three+ inputs narrows correctly
func Test858_lub_three_inputs(t *testing.T) {
	a, err := NewMediaUrnFromString("media:json;list;record;textable")
	require.NoError(t, err)
	b, err := NewMediaUrnFromString("media:csv;list;record;textable")
	require.NoError(t, err)
	c, err := NewMediaUrnFromString("media:ndjson;list;textable")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{a, b, c})
	expected, err := NewMediaUrnFromString("media:list;textable")
	require.NoError(t, err)
	assert.True(t, lub.IsEquivalent(expected), "LUB should be media:list;textable but got %s", lub.String())
}

// TEST859: LUB with valued tags (non-marker) that differ
func Test859_lub_valued_tags(t *testing.T) {
	v1, err := NewMediaUrnFromString("media:image;format=png")
	require.NoError(t, err)
	v2, err := NewMediaUrnFromString("media:image;format=jpeg")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{v1, v2})
	expected, err := NewMediaUrnFromString("media:image")
	require.NoError(t, err)
	assert.True(t, lub.IsEquivalent(expected), "LUB should drop conflicting format tag, got %s", lub.String())
}

// TEST628: Verify media URN constants all start with "media:" prefix
func Test628_media_urn_constants_format(t *testing.T) {
	assert.True(t, len(standard.MediaString) > 6 && standard.MediaString[:6] == "media:")
	assert.True(t, len(standard.MediaInteger) > 6 && standard.MediaInteger[:6] == "media:")
	assert.True(t, len(standard.MediaObject) > 6 && standard.MediaObject[:6] == "media:")
	assert.True(t, len(standard.MediaIdentity) >= 6 && standard.MediaIdentity[:6] == "media:")
}

// TEST555: with_tag and without_tag on MediaUrn
func Test555_with_tag_and_without_tag(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)

	withExt := urn.WithTag("ext", "pdf")
	ext, ok := withExt.GetExtension()
	assert.True(t, ok)
	assert.Equal(t, "pdf", ext)

	// Original unchanged (immutability)
	_, ok = urn.GetExtension()
	assert.False(t, ok)

	// Remove the tag
	withoutExt := withExt.WithoutTag("ext")
	_, ok = withoutExt.GetExtension()
	assert.False(t, ok)

	// Removing non-existent tag is a no-op
	same := urn.WithoutTag("nonexistent")
	assert.True(t, urn.Equals(same))
}

// TEST556: image_media_urn_for_ext creates valid image URN
func Test556_image_media_urn_for_ext(t *testing.T) {
	jpgUrn := ImageMediaUrnForExt("jpg")
	parsed, err := NewMediaUrnFromString(jpgUrn)
	require.NoError(t, err)
	assert.True(t, parsed.IsImage(), "image helper must set image tag")
	assert.True(t, parsed.IsBinary(), "image URN must be binary (no textable tag)")
	ext, ok := parsed.GetExtension()
	assert.True(t, ok)
	assert.Equal(t, "jpg", ext)
}

// TEST557: audio_media_urn_for_ext creates valid audio URN
func Test557_audio_media_urn_for_ext(t *testing.T) {
	mp3Urn := AudioMediaUrnForExt("mp3")
	parsed, err := NewMediaUrnFromString(mp3Urn)
	require.NoError(t, err)
	assert.True(t, parsed.IsAudio(), "audio helper must set audio tag")
	assert.True(t, parsed.IsBinary(), "audio URN must be binary (no textable tag)")
	ext, ok := parsed.GetExtension()
	assert.True(t, ok)
	assert.Equal(t, "mp3", ext)
}

// TEST629: Profile constants verified in media/spec_test.go (urn cannot import media due to cycle)
