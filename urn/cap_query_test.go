package urn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func queryCap(t *testing.T, s string) *CapUrn {
	t.Helper()
	c, err := NewCapUrnFromString(s)
	require.NoError(t, err, s)
	return c
}

func queryMedia(t *testing.T, s string) *MediaUrn {
	t.Helper()
	m, err := NewMediaUrnFromString(s)
	require.NoError(t, err, s)
	return m
}

// TEST12368: "what gives me this, whatever it takes?" and "what can this
// become?" are questions with a side unknown, and each cap answers with a grade.
//
// No cap URN can ask them: `media:` on a side is the type "anything", so a
// request spelled that way asked for a cap that takes everything, and found
// none. The unknown side asks nothing; the stated side is held to.
func Test12368_AQuestionMayLeaveASideUnknown(t *testing.T) {
	pages := queryCap(t, `cap:disbind;in="media:ext=pdf";out="media:enc=utf-8;ext=txt;page"`)
	toJpeg := queryCap(t, `cap:convert-image;in="media:ext=png;image";out="media:ext=jpeg;image"`)
	someImage := queryCap(t, `cap:render;in="media:ext=pdf";out="media:image"`)
	passesThrough := queryCap(t, "cap:decimate-sequence;effect=none")

	wantsJpeg := CapQueryProducing(queryMedia(t, "media:ext=jpeg;image"), nil)
	assert.Equal(t, MatchExact, wantsJpeg.Grade(toJpeg))
	assert.True(t, wantsJpeg.Admits(toJpeg), "whatever it takes: a png here")
	// "Some image" is not a jpeg, and not excluded: possible, never routed on.
	assert.Equal(t, MatchPossible, wantsJpeg.Grade(someImage))
	assert.False(t, wantsJpeg.Admits(someImage))
	assert.True(t, wantsJpeg.MayAdmit(someImage))
	assert.Equal(t, MatchNone, wantsJpeg.Grade(pages))

	// Asked for any image, a jpeg is guaranteed to be one, not exactly it.
	wantsImage := CapQueryProducing(queryMedia(t, "media:image"), nil)
	assert.Equal(t, MatchGuaranteed, wantsImage.Grade(toJpeg))
	assert.Equal(t, MatchExact, wantsImage.Grade(someImage))
	assert.False(t, wantsImage.Admits(passesThrough), "media: out promises no image")

	// What can a pdf become? Whatever takes a pdf — or takes anything.
	hasPdf := CapQueryConsuming(queryMedia(t, "media:ext=pdf"), nil)
	assert.True(t, hasPdf.Admits(pages))
	assert.True(t, hasPdf.Admits(someImage))
	assert.True(t, hasPdf.Admits(passesThrough), "it takes anything, a pdf included")
	assert.False(t, hasPdf.Admits(toJpeg), "a png converter does not take a pdf")
	assert.Equal(t, MatchNone, hasPdf.Grade(toJpeg))

	// Both sides stated is the typed call.
	pdfToImage := CapQueryBetween(queryMedia(t, "media:ext=pdf"), queryMedia(t, "media:image"), nil)
	assert.True(t, pdfToImage.Admits(someImage))
	assert.False(t, pdfToImage.Admits(pages))
	assert.False(t, pdfToImage.Admits(toJpeg))
}

// TEST12369: the cap-tags a query asks for are matched against the tags the
// cap HAS — a cap's own list is complete.
//
// So asking that a tag be absent selects the caps that do not carry it, which
// no cap needs to declare; and a cap may carry tags nobody asked about.
func Test12369_AQuerysTagsAreAskedOfTheTagsACapHas(t *testing.T) {
	toJpeg := queryCap(t, `cap:convert-image;in="media:ext=png;image";out="media:ext=jpeg;image"`)
	someImage := queryCap(t, `cap:render;in="media:ext=pdf";out="media:image"`)
	anyImage := queryMedia(t, "media:image")

	converters := CapQueryProducing(anyImage, map[string]string{"convert-image": "*"})
	assert.True(t, converters.Admits(toJpeg))
	assert.False(t, converters.Admits(someImage), "it renders; it is not tagged convert-image")

	notConverters := CapQueryProducing(anyImage, map[string]string{"convert-image": "!"})
	assert.True(t, notConverters.Admits(someImage), "it does not have the tag")
	assert.False(t, notConverters.Admits(toJpeg), "it has it")
	assert.Equal(t, MatchNone, notConverters.Grade(toJpeg))

	// The same holds of a request: `!x` is served by a cap silent on x.
	request := queryCap(t, `cap:!convert-image;out="media:image"`)
	assert.True(t, someImage.IsDispatchable(request))
	assert.False(t, toJpeg.IsDispatchable(request))
	assert.Equal(t, MatchExact, CapQueryFromRequest(request).Grade(someImage))
}
