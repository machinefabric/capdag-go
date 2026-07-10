package orchestrator

import (
	"bytes"
	"fmt"
	"io"

	cborlib "github.com/fxamacker/cbor/v2"
)

// --- Error Types ---

// CborUtilError is the base error type for CBOR utility operations.
type CborUtilError struct {
	Kind    CborUtilErrorKind
	Message string
}

// CborUtilErrorKind identifies the category of CBOR utility error.
type CborUtilErrorKind int

const (
	CborErrDeserialize CborUtilErrorKind = iota
	CborErrNotAnArray
	CborErrSerialize
	CborErrEmptyArray
)

func (e *CborUtilError) Error() string {
	switch e.Kind {
	case CborErrDeserialize:
		return fmt.Sprintf("Failed to deserialize CBOR data: %s", e.Message)
	case CborErrNotAnArray:
		return "CBOR data is not an array (expected array for splitting)"
	case CborErrSerialize:
		return fmt.Sprintf("Failed to serialize CBOR value: %s", e.Message)
	case CborErrEmptyArray:
		return "Empty CBOR array — nothing to split"
	default:
		return e.Message
	}
}

func cborDeserializeError(message string) *CborUtilError {
	return &CborUtilError{Kind: CborErrDeserialize, Message: message}
}

func cborNotAnArrayError() *CborUtilError {
	return &CborUtilError{Kind: CborErrNotAnArray}
}

func cborSerializeError(message string) *CborUtilError {
	return &CborUtilError{Kind: CborErrSerialize, Message: message}
}

func cborEmptyArrayError() *CborUtilError {
	return &CborUtilError{Kind: CborErrEmptyArray}
}

// --- CBOR Array Operations ---

// SplitCborArray splits a CBOR-encoded array into individually-serialized CBOR items.
// Each returned []byte is a complete, independently-parseable CBOR value.
func SplitCborArray(data []byte) ([][]byte, error) {
	var raw cborlib.RawMessage
	if err := cborlib.Unmarshal(data, &raw); err != nil {
		return nil, cborDeserializeError(err.Error())
	}

	// Decode as interface{} to check type
	var value interface{}
	if err := cborlib.Unmarshal(data, &value); err != nil {
		return nil, cborDeserializeError(err.Error())
	}

	// Check if it's an array
	arr, ok := value.([]interface{})
	if !ok {
		return nil, cborNotAnArrayError()
	}

	if len(arr) == 0 {
		return nil, cborEmptyArrayError()
	}

	// Re-serialize each element individually
	result := make([][]byte, 0, len(arr))
	for _, item := range arr {
		encoded, err := cborlib.Marshal(item)
		if err != nil {
			return nil, cborSerializeError(err.Error())
		}
		result = append(result, encoded)
	}

	return result, nil
}

// AssembleCborArray assembles individually-serialized CBOR items into a single CBOR array.
// Each input []byte must be a complete CBOR value.
func AssembleCborArray(items [][]byte) ([]byte, error) {
	values := make([]interface{}, 0, len(items))
	for i, item := range items {
		var value interface{}
		if err := cborlib.Unmarshal(item, &value); err != nil {
			return nil, cborDeserializeError(fmt.Sprintf("Item %d: %s", i, err))
		}
		values = append(values, value)
	}

	encoded, err := cborlib.Marshal(values)
	if err != nil {
		return nil, cborSerializeError(err.Error())
	}

	return encoded, nil
}

// --- CBOR Sequence (RFC 8742) Operations ---

// SplitCborSequence splits an RFC 8742 CBOR sequence into individually-serialized CBOR items.
// A CBOR sequence is a concatenation of independently-encoded CBOR data items
// with no array wrapper. Returns each item re-serialized as independent []byte.
func SplitCborSequence(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, cborEmptyArrayError()
	}

	var items [][]byte
	reader := bytes.NewReader(data)
	decoder := cborlib.NewDecoder(reader)

	// Loop until EOF — fxamacker buffers the full reader internally so reader.Len()
	// drops to 0 after the first Decode call. The decoder itself tracks remaining items.
	for {
		var value interface{}
		if err := decoder.Decode(&value); err != nil {
			if err == io.EOF {
				break
			}
			return nil, cborDeserializeError(err.Error())
		}

		encoded, err := cborlib.Marshal(value)
		if err != nil {
			return nil, cborSerializeError(err.Error())
		}
		items = append(items, encoded)
	}

	if len(items) == 0 {
		return nil, cborEmptyArrayError()
	}

	return items, nil
}

// AssembleCborSequence assembles individually-serialized CBOR items into an RFC 8742 CBOR sequence.
// Each input item must be a complete CBOR value. The result is their raw concatenation
// (no array wrapper). This is the inverse of SplitCborSequence.
func AssembleCborSequence(items [][]byte) ([]byte, error) {
	var result []byte
	for i, item := range items {
		// Validate each item is valid CBOR
		var value interface{}
		if err := cborlib.Unmarshal(item, &value); err != nil {
			return nil, cborDeserializeError(fmt.Sprintf("Item %d: %s", i, err))
		}
		result = append(result, item...)
	}
	return result, nil
}

// WrapRawItemsAsCborSequence wraps raw (unwrapped) item bytes into an RFC 8742 CBOR sequence.
//
// Each raw item — the bytes yielded by DecodeTerminalOutput / UnwrapCborValue (PNG
// frames, JSON records, …) — is re-encoded as a single self-delimiting CBOR byte
// string value, and the values are concatenated. This is the storage form a
// *sequence* node's node data must take so that a downstream cap's input
// (send_one_stream, which splits the sequence and forwards each item's CBOR bytes
// *without* re-wrapping) and SplitCborSequence both see well-formed self-delimiting
// values.
//
// Contrast AssembleCborSequence, which requires each item to ALREADY be a complete
// CBOR value (it validates rather than wraps) — the form used when the caller has
// itself CBOR-encoded each item (e.g. machfab's file-item interpreter).
//
// Returns a CborUtilError with kind CborErrSerialize if an item cannot be
// CBOR-serialized (practically never for a byte string).
func WrapRawItemsAsCborSequence(items [][]byte) ([]byte, error) {
	var result []byte
	for _, item := range items {
		encoded, err := cborlib.Marshal(item)
		if err != nil {
			return nil, cborSerializeError(err.Error())
		}
		result = append(result, encoded...)
	}
	return result, nil
}
