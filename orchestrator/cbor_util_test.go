package orchestrator

import (
	"testing"

	cborlib "github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cborEncodeVal encodes a Go value to CBOR bytes.
func cborEncodeVal(v interface{}) []byte {
	data, err := cborlib.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// buildCborSequence concatenates individually-encoded CBOR values (RFC 8742 sequence).
func buildCborSequence(items []interface{}) []byte {
	var result []byte
	for _, item := range items {
		result = append(result, cborEncodeVal(item)...)
	}
	return result
}

// TEST780: split_cbor_array splits a simple array of integers
func Test780_split_integer_array(t *testing.T) {
	data := cborEncodeVal([]int{1, 2, 3})

	items, err := SplitCborArray(data)
	require.NoError(t, err)
	assert.Equal(t, 3, len(items))

	for i, item := range items {
		var val int
		err := cborlib.Unmarshal(item, &val)
		require.NoError(t, err)
		assert.Equal(t, i+1, val)
	}
}

// TEST782: split_cbor_array rejects non-array input
func Test782_split_non_array(t *testing.T) {
	data := cborEncodeVal("not an array")
	_, err := SplitCborArray(data)
	require.Error(t, err)
	cborErr, ok := err.(*CborUtilError)
	require.True(t, ok, "expected CborUtilError")
	assert.Equal(t, CborErrNotAnArray, cborErr.Kind)
}

// TEST783: split_cbor_array rejects empty array
func Test783_split_empty_array(t *testing.T) {
	data := cborEncodeVal([]interface{}{})
	_, err := SplitCborArray(data)
	require.Error(t, err)
	cborErr, ok := err.(*CborUtilError)
	require.True(t, ok, "expected CborUtilError")
	assert.Equal(t, CborErrEmptyArray, cborErr.Kind)
}

// TEST784: split_cbor_array rejects invalid CBOR bytes
func Test784_split_invalid_cbor(t *testing.T) {
	_, err := SplitCborArray([]byte{0xFF, 0xFE, 0xFD})
	require.Error(t, err)
	cborErr, ok := err.(*CborUtilError)
	require.True(t, ok, "expected CborUtilError")
	assert.Equal(t, CborErrDeserialize, cborErr.Kind)
}

// TEST785: assemble_cbor_array creates array from individual items
func Test785_assemble_integer_array(t *testing.T) {
	items := [][]byte{
		cborEncodeVal(10),
		cborEncodeVal(20),
		cborEncodeVal(30),
	}

	assembled, err := AssembleCborArray(items)
	require.NoError(t, err)

	var vals []int
	require.NoError(t, cborlib.Unmarshal(assembled, &vals))
	assert.Equal(t, []int{10, 20, 30}, vals)
}

// TEST786: split then assemble roundtrip preserves data
func Test786_roundtrip_split_assemble(t *testing.T) {
	original := []interface{}{"hello", true, 42, []byte{1, 2, 3}}
	originalBytes := cborEncodeVal(original)

	items, err := SplitCborArray(originalBytes)
	require.NoError(t, err)
	assert.Equal(t, 4, len(items))

	reassembled, err := AssembleCborArray(items)
	require.NoError(t, err)

	// Decode both and compare via interface
	var origDecoded, reDecoded interface{}
	require.NoError(t, cborlib.Unmarshal(originalBytes, &origDecoded))
	require.NoError(t, cborlib.Unmarshal(reassembled, &reDecoded))
	assert.Equal(t, origDecoded, reDecoded)
}

// TEST955: split_cbor_array with nested maps
func Test955_split_map_array(t *testing.T) {
	map1 := map[string]string{"name": "Alice"}
	map2 := map[string]string{"name": "Bob"}
	data := cborEncodeVal([]interface{}{map1, map2})

	items, err := SplitCborArray(data)
	require.NoError(t, err)
	assert.Equal(t, 2, len(items))

	var decoded1, decoded2 map[string]string
	require.NoError(t, cborlib.Unmarshal(items[0], &decoded1))
	require.NoError(t, cborlib.Unmarshal(items[1], &decoded2))
	assert.Equal(t, "Alice", decoded1["name"])
	assert.Equal(t, "Bob", decoded2["name"])
}

// TEST956: assemble then split roundtrip preserves data
func Test956_roundtrip_assemble_split(t *testing.T) {
	items := [][]byte{
		cborEncodeVal("a"),
		cborEncodeVal("b"),
	}

	assembled, err := AssembleCborArray(items)
	require.NoError(t, err)

	splitBack, err := SplitCborArray(assembled)
	require.NoError(t, err)
	assert.Equal(t, 2, len(splitBack))
	assert.Equal(t, items[0], splitBack[0])
	assert.Equal(t, items[1], splitBack[1])
}

// TEST961: assemble empty list produces empty CBOR array
func Test961_assemble_empty(t *testing.T) {
	assembled, err := AssembleCborArray([][]byte{})
	require.NoError(t, err)

	var vals []interface{}
	require.NoError(t, cborlib.Unmarshal(assembled, &vals))
	assert.Equal(t, 0, len(vals))
}

// TEST962: assemble rejects invalid CBOR item
func Test962_assemble_invalid_item(t *testing.T) {
	items := [][]byte{
		cborEncodeVal(1),
		{0xFF, 0xFE}, // invalid CBOR
	}
	_, err := AssembleCborArray(items)
	require.Error(t, err)
	cborErr, ok := err.(*CborUtilError)
	require.True(t, ok, "expected CborUtilError")
	assert.Equal(t, CborErrDeserialize, cborErr.Kind)
}

// TEST963: split preserves CBOR byte strings (binary data — the common case in bifaci)
func Test963_split_binary_items(t *testing.T) {
	pdfBytes := []byte{0x25, 0x50, 0x44, 0x46} // %PDF
	pngBytes := []byte{0x89, 0x50, 0x4E, 0x47} // .PNG

	data := cborEncodeVal([]interface{}{pdfBytes, pngBytes})

	items, err := SplitCborArray(data)
	require.NoError(t, err)
	assert.Equal(t, 2, len(items))

	var decoded0, decoded1 []byte
	require.NoError(t, cborlib.Unmarshal(items[0], &decoded0))
	require.NoError(t, cborlib.Unmarshal(items[1], &decoded1))
	assert.Equal(t, pdfBytes, decoded0)
	assert.Equal(t, pngBytes, decoded1)
}

// TEST964: split_cbor_sequence splits concatenated CBOR Bytes values
func Test964_split_sequence_bytes(t *testing.T) {
	page1 := []byte("page1 json data")
	page2 := []byte("page2 json data")
	page3 := []byte("page3 json data")

	seq := buildCborSequence([]interface{}{page1, page2, page3})

	items, err := SplitCborSequence(seq)
	require.NoError(t, err)
	assert.Equal(t, 3, len(items))

	var d0, d1, d2 []byte
	require.NoError(t, cborlib.Unmarshal(items[0], &d0))
	require.NoError(t, cborlib.Unmarshal(items[1], &d1))
	require.NoError(t, cborlib.Unmarshal(items[2], &d2))
	assert.Equal(t, page1, d0)
	assert.Equal(t, page2, d1)
	assert.Equal(t, page3, d2)
}

// TEST965: split_cbor_sequence splits concatenated CBOR Text values
func Test965_split_sequence_text(t *testing.T) {
	seq := buildCborSequence([]interface{}{"hello", "world"})

	items, err := SplitCborSequence(seq)
	require.NoError(t, err)
	assert.Equal(t, 2, len(items))

	var d0, d1 string
	require.NoError(t, cborlib.Unmarshal(items[0], &d0))
	require.NoError(t, cborlib.Unmarshal(items[1], &d1))
	assert.Equal(t, "hello", d0)
	assert.Equal(t, "world", d1)
}

// TEST966: split_cbor_sequence handles mixed types
func Test966_split_sequence_mixed(t *testing.T) {
	seq := buildCborSequence([]interface{}{
		[]byte{1, 2, 3},
		"mixed",
		map[string]int{"key": 42},
		99,
	})

	items, err := SplitCborSequence(seq)
	require.NoError(t, err)
	assert.Equal(t, 4, len(items))

	var d0 []byte
	require.NoError(t, cborlib.Unmarshal(items[0], &d0))
	assert.Equal(t, []byte{1, 2, 3}, d0)

	var d3 int
	require.NoError(t, cborlib.Unmarshal(items[3], &d3))
	assert.Equal(t, 99, d3)
}

// TEST967: split_cbor_sequence single-item sequence
func Test967_split_sequence_single(t *testing.T) {
	seq := buildCborSequence([]interface{}{[]byte{0xDE, 0xAD}})

	items, err := SplitCborSequence(seq)
	require.NoError(t, err)
	assert.Equal(t, 1, len(items))

	var d0 []byte
	require.NoError(t, cborlib.Unmarshal(items[0], &d0))
	assert.Equal(t, []byte{0xDE, 0xAD}, d0)
}

// TEST968: roundtrip — assemble then split preserves items
func Test968_roundtrip_assemble_split_sequence(t *testing.T) {
	items := [][]byte{
		cborEncodeVal([]byte("first")),
		cborEncodeVal([]byte("second")),
		cborEncodeVal("third"),
	}

	assembled, err := AssembleCborSequence(items)
	require.NoError(t, err)

	splitBack, err := SplitCborSequence(assembled)
	require.NoError(t, err)
	assert.Equal(t, 3, len(splitBack))
	assert.Equal(t, items[0], splitBack[0])
	assert.Equal(t, items[1], splitBack[1])
	assert.Equal(t, items[2], splitBack[2])
}

// TEST969: roundtrip — split then assemble preserves byte-for-byte
func Test969_roundtrip_split_assemble_sequence(t *testing.T) {
	seq := buildCborSequence([]interface{}{
		[]byte("alpha"),
		[]byte("beta"),
	})

	items, err := SplitCborSequence(seq)
	require.NoError(t, err)

	reassembled, err := AssembleCborSequence(items)
	require.NoError(t, err)
	assert.Equal(t, seq, reassembled, "split then assemble must preserve bytes exactly")
}

// TEST970: split_cbor_sequence rejects empty data
func Test970_split_sequence_empty(t *testing.T) {
	_, err := SplitCborSequence([]byte{})
	require.Error(t, err)
	cborErr, ok := err.(*CborUtilError)
	require.True(t, ok, "expected CborUtilError")
	assert.Equal(t, CborErrEmptyArray, cborErr.Kind)
}

// TEST971: split_cbor_sequence rejects truncated CBOR
func Test971_split_sequence_truncated(t *testing.T) {
	// Valid first item
	first := cborEncodeVal([]byte("complete"))
	// Append truncated second item: major type 2 (bytes), length=10, but only 3 bytes of content
	truncated := []byte{0x4A, 0x01, 0x02, 0x03}
	seq := append(first, truncated...)

	_, err := SplitCborSequence(seq)
	require.Error(t, err, "truncated CBOR at end must produce DeserializeError")
	cborErr, ok := err.(*CborUtilError)
	require.True(t, ok, "expected CborUtilError")
	assert.Equal(t, CborErrDeserialize, cborErr.Kind)
}

// TEST972: assemble_cbor_sequence rejects invalid CBOR item
func Test972_assemble_sequence_invalid_item(t *testing.T) {
	items := [][]byte{
		cborEncodeVal(1),
		{0xFF, 0xFE}, // invalid CBOR
	}
	_, err := AssembleCborSequence(items)
	require.Error(t, err)
	cborErr, ok := err.(*CborUtilError)
	require.True(t, ok, "expected CborUtilError")
	assert.Equal(t, CborErrDeserialize, cborErr.Kind)
}

// TEST973: assemble_cbor_sequence with empty items list produces empty bytes
func Test973_assemble_sequence_empty(t *testing.T) {
	assembled, err := AssembleCborSequence([][]byte{})
	require.NoError(t, err)
	assert.Equal(t, 0, len(assembled), "empty sequence must produce zero-length bytes")
}

// TEST974: CBOR sequence is NOT a CBOR array — split_cbor_array rejects a sequence
func Test974_sequence_is_not_array(t *testing.T) {
	seq := buildCborSequence([]interface{}{
		[]byte("item1"),
		[]byte("item2"),
	})
	// A CBOR sequence (concatenation of raw CBOR values) is not a valid CBOR array.
	// Go's fxamacker/cbor rejects concatenated items as "extraneous data", returning
	// CborErrDeserialize (rather than CborErrNotAnArray as Rust's ciborium does).
	// Both correctly reject: a sequence cannot be split as an array.
	_, err := SplitCborArray(seq)
	require.Error(t, err, "CBOR sequence must be rejected by split_cbor_array")
	cborErr, ok := err.(*CborUtilError)
	require.True(t, ok, "expected CborUtilError")
	// Either DeserializeError (extraneous data) or NotAnArray is correct.
	assert.True(t, cborErr.Kind == CborErrDeserialize || cborErr.Kind == CborErrNotAnArray,
		"expected DeserializeError or NotAnArray, got kind=%d", cborErr.Kind)
}

// TEST975: split_cbor_sequence works on data that is also a valid CBOR array (single top-level value)
func Test975_single_value_sequence(t *testing.T) {
	single := cborEncodeVal([]byte("solo"))
	items, err := SplitCborSequence(single)
	require.NoError(t, err)
	assert.Equal(t, 1, len(items))

	var decoded []byte
	require.NoError(t, cborlib.Unmarshal(items[0], &decoded))
	assert.Equal(t, []byte("solo"), decoded)
}

// TEST976: WrapRawItemsAsCborSequence wraps RAW (non-CBOR) item bytes — e.g. PNG
// frames — into a valid, self-delimiting CBOR sequence that SplitCborSequence
// round-trips back to the exact raw items.
//
// Regression guard for commit 99df51c4 ("full dag"), which materialised sequence
// node_data with AssembleCborSequence(items) on the *raw* (unwrapped) items from
// decode_terminal_output. Raw binary is not itself CBOR, so that path failed with
// a deserialize error (observed as the video extract-frames chain failure). This
// test asserts the wrap path succeeds AND that the assemble path genuinely rejects
// the same raw bytes — so the two functions are not interchangeable and the fix is
// load-bearing.
func Test976_wrap_raw_items_roundtrips_and_assemble_rejects_them(t *testing.T) {
	// Raw items that are NOT valid CBOR on their own: a PNG magic header
	// (0x89 'P' 'N' 'G' … decodes as CBOR array(9) then chokes) and a raw JSON
	// record.
	pngMagic := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x01}
	jsonRecord := []byte(`{"name":"alpha"}`)
	rawItems := [][]byte{pngMagic, jsonRecord}

	// AssembleCborSequence must REJECT raw items (this is the bug the regression
	// introduced — proving the two helpers are distinct).
	_, err := AssembleCborSequence(rawItems)
	require.Error(t, err, "AssembleCborSequence must reject raw non-CBOR items")
	cborErr, ok := err.(*CborUtilError)
	require.True(t, ok, "expected CborUtilError")
	assert.Equal(t, CborErrDeserialize, cborErr.Kind)

	// WrapRawItemsAsCborSequence must SUCCEED and produce a valid RFC 8742
	// sequence of self-delimiting CBOR Bytes values.
	seq, err := WrapRawItemsAsCborSequence(rawItems)
	require.NoError(t, err)

	// Split it back the way a downstream cap head (send_one_stream) would, then
	// unwrap each CBOR Bytes value → must equal the original raw items.
	split, err := SplitCborSequence(seq)
	require.NoError(t, err)
	require.Equal(t, 2, len(split), "must round-trip to 2 items")

	recovered := make([][]byte, 0, len(split))
	for _, item := range split {
		var b []byte
		require.NoError(t, cborlib.Unmarshal(item, &b))
		recovered = append(recovered, b)
	}
	assert.Equal(t, [][]byte{pngMagic, jsonRecord}, recovered,
		"wrap→split→unwrap must recover the exact raw item bytes")
}

// TEST1317: empty item list wraps to empty bytes (mirrors the scalar/empty sink
// case where a chain produced no items).
func Test1317_wrap_raw_items_empty(t *testing.T) {
	seq, err := WrapRawItemsAsCborSequence([][]byte{})
	require.NoError(t, err)
	assert.Empty(t, seq, "empty item list must wrap to empty bytes")
}
