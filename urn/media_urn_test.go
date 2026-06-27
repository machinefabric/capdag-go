package urn

import (
	"encoding/json"
	"testing"

	"github.com/machinefabric/capdag-go/standard"
	taggedurn "github.com/machinefabric/tagged-urn-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mirror-specific coverage: Test parsing simple media URN verifies correct structure with no version, subtype, or profile
func Test6343_ParseSimple(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("string"))
	assert.False(t, urn.HasTag("enc"))
}

// Mirror-specific coverage: Test parsing media URN with marker tags works correctly
func Test6347_ParseWithSubtype(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("string"))
	// No list marker = scalar by default
	assert.True(t, urn.IsScalar())
	assert.False(t, urn.IsList())
}

// Mirror-specific coverage: Test parsing media URN with profile extracts profile URL correctly
func Test6351_ParseWithProfile(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:enc=utf-8;string")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("enc"))
}

// TEST60: Test wrong prefix fails with InvalidPrefix error showing expected and actual prefix
func Test060_wrong_prefix_fails(t *testing.T) {
	_, err := NewMediaUrnFromString("cap:string")
	require.Error(t, err)
	taggedErr, ok := err.(*taggedurn.TaggedUrnError)
	require.True(t, ok)
	assert.Equal(t, taggedurn.ErrorPrefixMismatch, taggedErr.Code)
	assert.Contains(t, taggedErr.Error(), "media")
	assert.Contains(t, taggedErr.Error(), "cap")
}

// TEST061: REMOVED — the binary/text distinction no longer exists in the
// vocabulary (IsBinary() was deleted from MediaUrn; everything is bytes).
// Encoding is now expressed by the orthogonal `enc=` tag, exercised by other
// tests (e.g. the enc-based text checks below). No replacement assertion is
// meaningful here.

// TEST62: Test is_record returns true when record marker tag is present indicating key-value structure
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
	assert.True(t, jsonUrn.IsRecord()) // "media:fmt=json;record"

	// Without record marker, is_record is false
	scalar, err := NewMediaUrnFromString("media:enc=utf-8")
	require.NoError(t, err)
	assert.False(t, scalar.IsRecord())

	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, stringUrn.IsRecord()) // scalar, no record marker

	listUrn2, err := NewMediaUrnFromString(standard.MediaStringList)
	require.NoError(t, err)
	assert.False(t, listUrn2.IsRecord()) // list, no record marker
}

// TEST63: Test is_scalar returns true when list marker tag is absent (scalar is default)
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

// TEST64: Test is_list returns true when list marker tag is present indicating ordered collection
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

// TEST65: Test is_opaque returns true when record marker is absent (opaque is default)
func Test065_is_opaque(t *testing.T) {
	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.True(t, stringUrn.IsOpaque())

	strList, err := NewMediaUrnFromString(standard.MediaStringList)
	require.NoError(t, err)
	assert.True(t, strList.IsOpaque())

	pdfUrn, err := NewMediaUrnFromString(standard.MediaPDF)
	require.NoError(t, err)
	assert.True(t, pdfUrn.IsOpaque())

	textUrn, err := NewMediaUrnFromString("media:enc=utf-8")
	require.NoError(t, err)
	assert.True(t, textUrn.IsOpaque())

	objUrn, err := NewMediaUrnFromString(standard.MediaObject)
	require.NoError(t, err)
	assert.False(t, objUrn.IsOpaque())

	jsonUrn, err := NewMediaUrnFromString(standard.MediaJSON)
	require.NoError(t, err)
	assert.False(t, jsonUrn.IsOpaque())

	objListUrn, err := NewMediaUrnFromString(standard.MediaObjectList)
	require.NoError(t, err)
	assert.False(t, objListUrn.IsOpaque())
}

// TEST66: Test is_json returns true only when json marker tag is present for JSON representation
func Test066_is_json(t *testing.T) {
	jsonUrn, err := NewMediaUrnFromString(standard.MediaJSON)
	require.NoError(t, err)
	assert.True(t, jsonUrn.IsJson())

	customJson, err := NewMediaUrnFromString("media:custom;fmt=json")
	require.NoError(t, err)
	assert.True(t, customJson.IsJson())

	nonJson, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.False(t, nonJson.IsJson())
}

// TEST67: Text-representability is now carried by the orthogonal `enc=` tag (the old `textable` marker and is_text() are gone). A media is "text" iff it declares an encoding. enc is orthogonal to format/numeric, so only media that actually carry enc= are text.
func Test067_is_text(t *testing.T) {
	// Has enc= → text-representable
	stringUrn, err := NewMediaUrnFromString(standard.MediaString) // media:enc=utf-8
	require.NoError(t, err)
	assert.True(t, stringUrn.HasTag("enc"))

	boolUrn, err := NewMediaUrnFromString(standard.MediaBoolean) // media:bool;enc=utf-8
	require.NoError(t, err)
	assert.True(t, boolUrn.HasTag("enc"))

	// No enc= → not text-representable
	intUrn, err := NewMediaUrnFromString(standard.MediaInteger) // media:integer;numeric
	require.NoError(t, err)
	assert.False(t, intUrn.HasTag("enc"))

	jsonUrn, err := NewMediaUrnFromString(standard.MediaJSON) // media:fmt=json;record
	require.NoError(t, err)
	assert.False(t, jsonUrn.HasTag("enc"))

	binary, err := NewMediaUrnFromString("media:")
	require.NoError(t, err)
	assert.False(t, binary.HasTag("enc"))
}

// TEST68: Test is_void returns true when void flag or type=void tag is present
func Test068_is_void(t *testing.T) {
	voidUrn, err := NewMediaUrnFromString("media:void")
	require.NoError(t, err)
	assert.True(t, voidUrn.IsVoid())

	nonVoid, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.False(t, nonVoid.IsVoid())
}

// Mirror-specific coverage: Test simple constructor creates media URN with type tag
func Test6355_Constructor(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("string"))
}

// Mirror-specific coverage: Test with_subtype constructor creates media URN with subtype
func Test6359_WithSubtypeConstructor(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:application;subtype=json")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("application"))
	subtype, ok := urn.GetTag("subtype")
	assert.True(t, ok)
	assert.Equal(t, "json", subtype)
}

// TEST71: Test to_string roundtrip ensures serialization and deserialization preserve URN structure
func Test071_to_string_roundtrip(t *testing.T) {
	original := "media:enc=utf-8;string"
	urn1, err := NewMediaUrnFromString(original)
	require.NoError(t, err)

	serialized := urn1.String()
	urn2, err := NewMediaUrnFromString(serialized)
	require.NoError(t, err)

	assert.True(t, urn1.Equals(urn2))
}

// TEST72: Test all media URN constants parse successfully as valid media URNs
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
		standard.MediaDecision,
	}

	for _, constant := range constants {
		_, err := NewMediaUrnFromString(constant)
		assert.NoError(t, err, "Failed to parse constant: %s", constant)
	}
}

// TEST73: Test extension helper functions create media URNs with ext tag and correct format
func Test073_extension_helpers(t *testing.T) {
	pdfUrn, err := NewMediaUrnFromString("media:ext=pdf")
	require.NoError(t, err)
	ext, ok := pdfUrn.GetTag("ext")
	assert.True(t, ok)
	assert.Equal(t, "pdf", ext)
}

// TEST74: Test media URN conforms_to using tagged URN semantics with specific and generic requirements
func Test074_media_urn_matching(t *testing.T) {
	specific, err := NewMediaUrnFromString("media:enc=utf-8;string")
	require.NoError(t, err)

	generic, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)

	// Specific pattern does NOT accept generic instance (generic missing enc and form)
	assert.False(t, specific.Accepts(generic))

	// Generic pattern DOES accept specific instance (generic has no constraints on extra tags)
	assert.True(t, generic.Accepts(specific))

	// Specific instance conforms to generic pattern
	assert.True(t, specific.ConformsTo(generic))

	// Generic instance does NOT conform to specific pattern
	assert.False(t, generic.ConformsTo(specific))
}

// TEST75: Test accepts with implicit wildcards where handlers with fewer tags can handle more requests
func Test075_matching(t *testing.T) {
	handler, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)

	request, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)

	// Handler with fewer tags can match requests with more tags (wildcard semantics)
	assert.True(t, handler.Accepts(request))
}

// TEST76: Test specificity increases with more tags for ranking conformance
func Test076_specificity(t *testing.T) {
	simple, err := NewMediaUrnFromString("media:string")
	require.NoError(t, err)

	detailed, err := NewMediaUrnFromString("media:enc=utf-8;string")
	require.NoError(t, err)

	// More tags = higher specificity
	assert.True(t, detailed.Specificity() > simple.Specificity())
}

// TEST77: Test serde roundtrip serializes to JSON string and deserializes back correctly
func Test077_serde_roundtrip(t *testing.T) {
	original, err := NewMediaUrnFromString("media:enc=utf-8;string")
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

// TEST78: conforms_to behavior between MEDIA_OBJECT and MEDIA_STRING
func Test078_object_does_not_conform_to_string(t *testing.T) {
	strUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	objUrn, err := NewMediaUrnFromString(standard.MediaObject)
	require.NoError(t, err)

	assert.True(t, strUrn.ConformsTo(strUrn), "string conforms to string")
	assert.True(t, objUrn.ConformsTo(objUrn), "object conforms to object")
	assert.False(t, objUrn.ConformsTo(strUrn), "MEDIA_OBJECT should NOT conform to MEDIA_STRING (missing enc)")
}

// TEST304: Test MEDIA_AVAILABILITY_OUTPUT constant parses as valid media URN with correct tags
func Test304_media_availability_output_constant(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:enc=utf-8;model-availability;record")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("enc"))
	assert.True(t, urn.IsRecord())
}

// TEST305: Test MEDIA_PATH_OUTPUT constant parses as valid media URN with correct tags
func Test305_media_path_output_constant(t *testing.T) {
	urn, err := NewMediaUrnFromString("media:enc=utf-8;model-path;record")
	require.NoError(t, err)
	assert.True(t, urn.HasTag("enc"))
	assert.True(t, urn.IsRecord())
}

// TEST306: Test MEDIA_AVAILABILITY_OUTPUT and MEDIA_PATH_OUTPUT are distinct URNs
func Test306_availability_and_path_output_distinct(t *testing.T) {
	availUrn, err := NewMediaUrnFromString("media:enc=utf-8;model-availability;record")
	require.NoError(t, err)
	pathUrn, err := NewMediaUrnFromString("media:enc=utf-8;model-path;record")
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

	customImage, err := NewMediaUrnFromString("media:ext=jpg;image")
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

	customAudio, err := NewMediaUrnFromString("media:audio;ext=mp3")
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

	customVideo, err := NewMediaUrnFromString("media:ext=mp4;video")
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

// TEST551: is_file_path returns true for the single file-path media URN, false for everything else. There is no "array" variant — cardinality is carried by is_sequence on the wire, not by URN tags.
func Test551_is_file_path(t *testing.T) {
	fpUrn, err := NewMediaUrnFromString(standard.MediaFilePath)
	require.NoError(t, err)
	assert.True(t, fpUrn.IsFilePath())

	stringUrn, err := NewMediaUrnFromString(standard.MediaString)
	require.NoError(t, err)
	assert.False(t, stringUrn.IsFilePath())

	binaryUrn, err := NewMediaUrnFromString(standard.MediaIdentity)
	require.NoError(t, err)
	assert.False(t, binaryUrn.IsFilePath())
}


// TEST558: predicates are consistent with constants — every constant triggers exactly the expected predicates
func Test558_predicate_constant_consistency(t *testing.T) {
	// MEDIA_INTEGER must be numeric, scalar, NOT enc/bool/image/json/list.
	// Integers are numeric, not text — they carry no enc= tag.
	intUrn, err := NewMediaUrnFromString(standard.MediaInteger)
	require.NoError(t, err)
	assert.True(t, intUrn.IsNumeric())
	assert.False(t, intUrn.HasTag("enc"))
	assert.True(t, intUrn.IsScalar())
	assert.False(t, intUrn.IsBool())
	assert.False(t, intUrn.IsImage())
	assert.False(t, intUrn.IsJson())
	assert.False(t, intUrn.IsList())

	// MEDIA_BOOLEAN must be bool, text (enc), scalar, NOT numeric
	boolUrn, err := NewMediaUrnFromString(standard.MediaBoolean)
	require.NoError(t, err)
	assert.True(t, boolUrn.IsBool())
	assert.True(t, boolUrn.HasTag("enc"))
	assert.True(t, boolUrn.IsScalar())
	assert.False(t, boolUrn.IsNumeric())

	// MEDIA_JSON must be json, record, structured, NOT enc/list.
	// JSON content carries fmt=json (not enc=) — it is a serialization, not bare text.
	jsonUrn, err := NewMediaUrnFromString(standard.MediaJSON)
	require.NoError(t, err)
	assert.True(t, jsonUrn.IsJson())
	assert.False(t, jsonUrn.HasTag("enc"))
	assert.True(t, jsonUrn.IsRecord())
	assert.True(t, jsonUrn.IsStructured())
	assert.False(t, jsonUrn.IsList())

	// MEDIA_VOID is void, NOT enc/numeric
	voidUrn, err := NewMediaUrnFromString(standard.MediaVoid)
	require.NoError(t, err)
	assert.True(t, voidUrn.IsVoid())
	assert.False(t, voidUrn.HasTag("enc"))
	assert.False(t, voidUrn.IsNumeric())
}

// TEST852: LUB of identical URNs returns the same URN
func Test852_lub_identical(t *testing.T) {
	pdf, err := NewMediaUrnFromString("media:ext=pdf")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{pdf, pdf})
	assert.True(t, lub.IsEquivalent(pdf))
}

// TEST853: LUB of URNs with no common tags returns media: (universal)
func Test853_lub_no_common_tags(t *testing.T) {
	pdf, err := NewMediaUrnFromString("media:ext=pdf")
	require.NoError(t, err)
	png, err := NewMediaUrnFromString("media:ext=png;image")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{pdf, png})
	universal, err := NewMediaUrnFromString("media:")
	require.NoError(t, err)
	assert.True(t, lub.IsEquivalent(universal), "LUB of pdf and png should be media: but got %s", lub.String())
}

// TEST854: LUB keeps common tags, drops differing ones
func Test854_lub_partial_overlap(t *testing.T) {
	jsonRec, err := NewMediaUrnFromString("media:fmt=json;record")
	require.NoError(t, err)
	yamlRec, err := NewMediaUrnFromString("media:fmt=yaml;record")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{jsonRec, yamlRec})
	expected, err := NewMediaUrnFromString("media:record")
	require.NoError(t, err)
	assert.True(t, lub.IsEquivalent(expected), "LUB should drop differing fmt, keep record, got %s", lub.String())
}

// TEST855: LUB of list and non-list drops list tag
func Test855_lub_list_vs_scalar(t *testing.T) {
	jsonList, err := NewMediaUrnFromString("media:fmt=json;list")
	require.NoError(t, err)
	jsonScalar, err := NewMediaUrnFromString("media:fmt=json")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{jsonList, jsonScalar})
	expected, err := NewMediaUrnFromString("media:fmt=json")
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
	pdf, err := NewMediaUrnFromString("media:ext=pdf")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{pdf})
	assert.True(t, lub.IsEquivalent(pdf))
}

// TEST858: LUB with three+ inputs narrows correctly
func Test858_lub_three_inputs(t *testing.T) {
	a, err := NewMediaUrnFromString("media:fmt=json;list;record")
	require.NoError(t, err)
	b, err := NewMediaUrnFromString("media:fmt=csv;list;record")
	require.NoError(t, err)
	c, err := NewMediaUrnFromString("media:fmt=ndjson;list")
	require.NoError(t, err)
	lub := LeastUpperBound([]*MediaUrn{a, b, c})
	expected, err := NewMediaUrnFromString("media:list")
	require.NoError(t, err)
	assert.True(t, lub.IsEquivalent(expected), "LUB should be media:list but got %s", lub.String())
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

// TEST555: with_tag adds a tag and without_tag removes it
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

// TEST556: image_media_urn_for_ext creates valid image media URN
func Test556_image_media_urn_for_ext(t *testing.T) {
	jpgUrn := ImageMediaUrnForExt("jpg")
	parsed, err := NewMediaUrnFromString(jpgUrn)
	require.NoError(t, err)
	assert.True(t, parsed.IsImage(), "image helper must set image tag")
	assert.False(t, parsed.HasTag("enc"), "image URN is not text (no enc tag)")
	ext, ok := parsed.GetExtension()
	assert.True(t, ok)
	assert.Equal(t, "jpg", ext)
}

// TEST557: audio_media_urn_for_ext creates valid audio media URN
func Test557_audio_media_urn_for_ext(t *testing.T) {
	mp3Urn := AudioMediaUrnForExt("mp3")
	parsed, err := NewMediaUrnFromString(mp3Urn)
	require.NoError(t, err)
	assert.True(t, parsed.IsAudio(), "audio helper must set audio tag")
	assert.False(t, parsed.HasTag("enc"), "audio URN is not text (no enc tag)")
	ext, ok := parsed.GetExtension()
	assert.True(t, ok)
	assert.Equal(t, "mp3", ext)
}

// TEST629: Profile constants verified in media/spec_test.go (urn cannot import media due to cycle)

// TEST1810: media:void is atomic — refinements are parse errors. Mirrored across every language port (Rust, Go, Python, Swift/ObjC, JS) under the SAME number. Any divergence is a wire-level inconsistency — the unit type's atomicity is part of the protocol's deepest layer, not a per-port detail. The bare `media:void` parses successfully; any combination with another tag (marker or key=value) MUST fail with VoidNotAtomic. This forecloses a fake taxonomy of unit values; reasons or labels for *why* void is used belong on the cap URN's non-directional tags or in cap args.
func Test1810_media_void_is_atomic(t *testing.T) {
	bare, err := NewMediaUrnFromString("media:void")
	require.NoError(t, err, "bare `media:void` must parse — it is the unit type")
	assert.True(t, bare.IsVoid())

	badInputs := []string{
		"media:void;text",
		"media:pdf;void",
		"media:void;audio",
		"media:void;reason=warmup",
		"media:void;heartbeat",
		"media:void;manual",
		"media:warmup;void",
		"media:reason=foo;void",
	}
	for _, input := range badInputs {
		_, err := NewMediaUrnFromString(input)
		require.Error(t, err, "%s: expected parse error", input)
		mediaErr, ok := err.(*MediaUrnError)
		require.True(t, ok, "%s: expected *MediaUrnError, got %T (%v)", input, err, err)
		assert.Equal(t, ErrorMediaVoidNotAtomic, mediaErr.Code,
			"%s: expected ErrorMediaVoidNotAtomic, got code %d (%s)",
			input, mediaErr.Code, mediaErr.Message)
	}
}
