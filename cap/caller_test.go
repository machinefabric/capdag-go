package cap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TEST156: Test creating StdinSource Data variant with byte vector
func Test156_StdinSourceDataCreation(t *testing.T) {
	data := []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f} // "Hello"
	source := NewStdinSourceFromData(data)

	require.True(t, source.IsData(), "Expected Data variant")
	assert.Equal(t, data, source.Data)
}

// TEST157: Test creating StdinSource FileReference variant with all required fields
func Test157_StdinSourceFileReferenceCreation(t *testing.T) {
	trackedFileID := "tracked-file-123"
	originalPath := "/path/to/original.pdf"
	securityBookmark := []byte{0x62, 0x6f, 0x6f, 0x6b} // "book"
	mediaUrn := "media:ext=pdf"

	source := NewStdinSourceFromFileReference(trackedFileID, originalPath, securityBookmark, mediaUrn)

	require.True(t, source.IsFileReference(), "Expected FileReference variant")
	assert.Equal(t, trackedFileID, source.TrackedFileID)
	assert.Equal(t, originalPath, source.OriginalPath)
	assert.Equal(t, securityBookmark, source.SecurityBookmark)
	assert.Equal(t, mediaUrn, source.MediaUrn)
}

// TEST158: Test StdinSource Data with empty vector stores and retrieves correctly
func Test158_StdinSourceEmptyData(t *testing.T) {
	source := NewStdinSourceFromData([]byte{})

	require.True(t, source.IsData(), "Expected Data variant")
	assert.Empty(t, source.Data)
}

// TEST159: Test StdinSource Data with binary content like PNG header bytes
func Test159_StdinSourceBinaryContent(t *testing.T) {
	pngHeader := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}
	source := NewStdinSourceFromData(pngHeader)

	require.True(t, source.IsData(), "Expected Data variant")
	assert.Equal(t, 8, len(source.Data))
	assert.Equal(t, byte(0x89), source.Data[0])
	assert.Equal(t, byte(0x50), source.Data[1]) // 'P'
	assert.Equal(t, pngHeader, source.Data)
}

// TEST160: Test StdinSource Data clone creates independent copy with same data
func Test160_StdinSourceClone(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}
	source := NewStdinSourceFromData(data)
	cloned := *source

	require.True(t, source.IsData())
	require.True(t, cloned.IsData())
	assert.Equal(t, source.Data, cloned.Data)
}

// TEST161: Test StdinSource FileReference clone creates independent copy with same fields
func Test161_StdinSourceFileReferenceClone(t *testing.T) {
	source := NewStdinSourceFromFileReference(
		"test-id",
		"/test/path.pdf",
		[]byte{1, 2, 3},
		"media:ext=pdf",
	)
	cloned := *source

	require.True(t, source.IsFileReference())
	require.True(t, cloned.IsFileReference())
	assert.Equal(t, source.TrackedFileID, cloned.TrackedFileID)
	assert.Equal(t, source.OriginalPath, cloned.OriginalPath)
	assert.Equal(t, source.SecurityBookmark, cloned.SecurityBookmark)
	assert.Equal(t, source.MediaUrn, cloned.MediaUrn)
}

// TEST162: Test StdinSource Debug format displays variant type and relevant fields
func Test162_StdinSourceDebug(t *testing.T) {
	dataSource := NewStdinSourceFromData([]byte{1, 2, 3})
	debugStr := dataSource.String()
	assert.Contains(t, debugStr, "Data")

	fileSource := NewStdinSourceFromFileReference(
		"test-id",
		"/test/path.pdf",
		[]byte{},
		"media:ext=pdf",
	)
	debugStr = fileSource.String()
	assert.Contains(t, debugStr, "FileReference")
	assert.Contains(t, debugStr, "test-id")
	assert.Contains(t, debugStr, "/test/path.pdf")
}

// TEST274: Test CapArgumentValue::new stores media_urn and raw byte value
func Test274_CapArgumentValueNew(t *testing.T) {
	arg := NewCapArgumentValue("media:enc=utf-8;model-spec", []byte("gpt-4"))
	assert.Equal(t, "media:enc=utf-8;model-spec", arg.MediaUrn)
	assert.Equal(t, []byte("gpt-4"), arg.Value)
}

// TEST275: Test CapArgumentValue::from_str converts string to UTF-8 bytes
func Test275_CapArgumentValueFromStr(t *testing.T) {
	arg := NewCapArgumentValueFromStr("media:enc=utf-8;string", "hello world")
	assert.Equal(t, "media:enc=utf-8;string", arg.MediaUrn)
	assert.Equal(t, []byte("hello world"), arg.Value)
}

// TEST276: Test CapArgumentValue::value_as_str succeeds for UTF-8 data
func Test276_CapArgumentValueAsStrValid(t *testing.T) {
	arg := NewCapArgumentValueFromStr("media:string", "test")
	s, err := arg.ValueAsStr()
	require.NoError(t, err)
	assert.Equal(t, "test", s)
}

// TEST277: Test CapArgumentValue::value_as_str fails for non-UTF-8 binary data
func Test277_CapArgumentValueAsStrInvalidUtf8(t *testing.T) {
	arg := NewCapArgumentValue("media:ext=pdf", []byte{0xFF, 0xFE, 0x80})
	_, err := arg.ValueAsStr()
	assert.Error(t, err, "non-UTF-8 data must fail")
}

// TEST278: Test CapArgumentValue::new with empty value stores empty vec
func Test278_CapArgumentValueEmpty(t *testing.T) {
	arg := NewCapArgumentValue("media:void", []byte{})
	assert.Empty(t, arg.Value)
	s, err := arg.ValueAsStr()
	require.NoError(t, err)
	assert.Equal(t, "", s)
}

// TEST279: Test CapArgumentValue Clone produces independent copy with same data
func Test279_CapArgumentValueClone(t *testing.T) {
	arg := NewCapArgumentValue("media:test", []byte("data"))
	cloned := arg
	assert.Equal(t, arg.MediaUrn, cloned.MediaUrn)
	assert.Equal(t, arg.Value, cloned.Value)
}

// TEST280: Test CapArgumentValue Debug format includes media_urn and value
func Test280_CapArgumentValueDebug(t *testing.T) {
	arg := NewCapArgumentValueFromStr("media:test", "val")
	debug := arg.String()
	assert.True(t, strings.Contains(debug, "media:test"), "debug must include media_urn")
}

// TEST281: Test CapArgumentValue::new accepts Into<String> for media_urn (String and &str)
func Test281_CapArgumentValueIntoString(t *testing.T) {
	s := string("media:owned")
	arg1 := NewCapArgumentValue(s, []byte{})
	assert.Equal(t, "media:owned", arg1.MediaUrn)

	arg2 := NewCapArgumentValue("media:borrowed", []byte{})
	assert.Equal(t, "media:borrowed", arg2.MediaUrn)
}

// TEST282: Test CapArgumentValue::from_str with Unicode string preserves all characters
func Test282_CapArgumentValueUnicode(t *testing.T) {
	arg := NewCapArgumentValueFromStr("media:string", "hello 世界 🌍")
	s, err := arg.ValueAsStr()
	require.NoError(t, err)
	assert.Equal(t, "hello 世界 🌍", s)
}

// TEST283: Test CapArgumentValue with large binary payload preserves all bytes
func Test283_CapArgumentValueLargeBinary(t *testing.T) {
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	arg := NewCapArgumentValue("media:ext=pdf", data)
	assert.Equal(t, 10000, len(arg.Value))
	assert.Equal(t, data, arg.Value)
}
