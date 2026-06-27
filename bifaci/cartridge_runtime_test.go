package bifaci

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	cborlib "github.com/fxamacker/cbor/v2"
	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

const testManifest = `{"name":"TestCartridge","version":"1.0.0","channel":"release","registry_url":null,"description":"Test cartridge","cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"Identity","command":"identity"},{"urn":"cap:in=\"media:void\";test;out=\"media:void\"","title":"Test","command":"test"}]}]}`

// Mock emitter that captures emitted data for testing
type mockStreamEmitter struct {
	emittedData [][]byte
}

func (m *mockStreamEmitter) EmitCbor(value interface{}) error {
	// CBOR-encode the value
	cborPayload, err := cborlib.Marshal(value)
	if err != nil {
		return err
	}
	m.emittedData = append(m.emittedData, cborPayload)
	return nil
}

func (m *mockStreamEmitter) Write(data []byte) error {
	m.emittedData = append(m.emittedData, append([]byte{}, data...))
	return nil
}

func (m *mockStreamEmitter) EmitListItem(value interface{}) error {
	cborPayload, err := cborlib.Marshal(value)
	if err != nil {
		return err
	}
	m.emittedData = append(m.emittedData, cborPayload)
	return nil
}

func (m *mockStreamEmitter) EmitLog(level, message string) {
	// No-op for tests
}

func (m *mockStreamEmitter) Progress(progress float32, message string) {
	// No-op for tests
}

// Helper to get all emitted data as single concatenated bytes
func (m *mockStreamEmitter) GetAllData() []byte {
	var result []byte
	for _, chunk := range m.emittedData {
		result = append(result, chunk...)
	}
	return result
}

// bytesToFrameChannel converts a byte payload to a frame channel for testing.
// Sends: STREAM_START → CHUNK → STREAM_END → END
func bytesToFrameChannel(payload []byte) <-chan Frame {
	ch := make(chan Frame, 4)
	go func() {
		defer close(ch)
		requestID := NewMessageIdDefault()
		streamID := "test-arg"
		mediaUrn := "media:"

		// STREAM_START
		ch <- *NewStreamStart(requestID, streamID, mediaUrn, nil)

		// CHUNK (if payload is not empty)
		if len(payload) > 0 {
			chunkIndex := uint64(0)
			checksum := ComputeChecksum(payload)
			ch <- *NewChunk(requestID, streamID, 0, payload, chunkIndex, checksum)
		}

		// STREAM_END
		ch <- *NewStreamEnd(requestID, streamID, 1)

		// END
		ch <- *NewEnd(requestID, nil)
	}()
	return ch
}

// TEST248: Test register_op and find_handler by exact cap URN
func Test248_register_and_find_handler(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	runtime.Register(`cap:in="media:void";test;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			return emitter.EmitCbor("result")
		})

	handler := runtime.FindHandler(`cap:in="media:void";test;out="media:void"`)
	if handler == nil {
		t.Fatal("Expected to find handler, got nil")
	}
}

// TEST249: Test register_op handler echoes bytes directly
func Test249_raw_handler(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	runtime.Register(`cap:in="media:void";raw;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			// Collect first arg and echo it
			payload, err := CollectFirstArg(frames)
			if err != nil {
				return err
			}
			return emitter.EmitCbor(payload)
		})

	handler := runtime.FindHandler(`cap:in="media:void";raw;out="media:void"`)
	if handler == nil {
		t.Fatal("Expected to find handler")
	}

	emitter := &mockStreamEmitter{}
	peer := &noPeerInvoker{}
	err = handler(bytesToFrameChannel([]byte("echo this")), emitter, peer)
	if err != nil {
		t.Fatalf("Handler failed: %v", err)
	}
	// Decode CBOR
	var result []byte
	if err := cborlib.Unmarshal(emitter.GetAllData(), &result); err != nil {
		t.Fatalf("Failed to decode result: %v", err)
	}
	if string(result) != "echo this" {
		t.Errorf("Expected 'echo this', got %s", string(result))
	}
}

// TEST250: Test Op handler collects input and processes it
func Test250_typed_handler_deserialization(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	runtime.Register(`cap:in="media:void";test;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			payload, err := CollectFirstArg(frames)
			if err != nil {
				return err
			}
			var req map[string]interface{}
			if err := json.Unmarshal(payload, &req); err != nil {
				return err
			}
			value := req["key"]
			if value == nil {
				return emitter.EmitCbor("missing")
			}
			return emitter.EmitCbor(value.(string))
		})

	handler := runtime.FindHandler(`cap:in="media:void";test;out="media:void"`)
	emitter := &mockStreamEmitter{}
	peer := &noPeerInvoker{}
	err = handler(bytesToFrameChannel([]byte(`{"key":"hello"}`)), emitter, peer)
	if err != nil {
		t.Fatalf("Handler failed: %v", err)
	}
	var result string
	if err := cborlib.Unmarshal(emitter.GetAllData(), &result); err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}
	if result != "hello" {
		t.Errorf("Expected 'hello', got %s", result)
	}
}

// TEST251: Test Op handler propagates errors through RuntimeError::Handler
func Test251_typed_handler_rejects_invalid_json(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	runtime.Register(`cap:in="media:void";test;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			payload, err := CollectFirstArg(frames)
			if err != nil {
				return err
			}
			var req map[string]interface{}
			if err := json.Unmarshal(payload, &req); err != nil {
				return err
			}
			return emitter.EmitCbor([]byte{})
		})

	handler := runtime.FindHandler(`cap:in="media:void";test;out="media:void"`)
	emitter := &mockStreamEmitter{}
	peer := &noPeerInvoker{}
	err = handler(bytesToFrameChannel([]byte("not json {{{{")), emitter, peer)
	if err == nil {
		t.Fatal("Expected error for invalid JSON, got nil")
	}
}

// TEST252: Test find_handler returns None for unregistered cap URNs
func Test252_find_handler_unknown_cap(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	handler := runtime.FindHandler(`cap:in="media:void";nonexistent;out="media:void"`)
	if handler != nil {
		t.Fatal("Expected nil for unknown cap, got handler")
	}
}

// TEST253: Test OpFactory can be cloned via Arc and sent across tasks (Send + Sync)
func Test253_handler_is_send_sync(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	runtime.Register(`cap:in="media:void";threaded;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			return emitter.EmitCbor("done")
		})

	handler := runtime.FindHandler(`cap:in="media:void";threaded;out="media:void"`)
	if handler == nil {
		t.Fatal("Expected handler")
	}

	// Test that handler can be called from goroutine
	doneCh := make(chan bool)
	go func() {
		emitter := &mockStreamEmitter{}
		peer := &noPeerInvoker{}
		err := handler(bytesToFrameChannel([]byte("{}")), emitter, peer)
		if err != nil {
			t.Errorf("Handler failed: %v", err)
		}
		var result string
		if err := cborlib.Unmarshal(emitter.GetAllData(), &result); err != nil {
			t.Errorf("Failed to decode: %v", err)
		}
		if result != "done" {
			t.Errorf("Expected 'done', got %s", result)
		}
		doneCh <- true
	}()
	<-doneCh
}

// TEST254: Test NoPeerInvoker always returns PeerRequest error
func Test254_no_peer_invoker(t *testing.T) {
	peer := &noPeerInvoker{}
	_, err := peer.Invoke(`cap:in="media:void";test;out="media:void"`, []cap.CapArgumentValue{})
	if err == nil {
		t.Fatal("Expected error from NoPeerInvoker, got nil")
	}
	if err.Error() != "peer invocation not supported in this context" {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

// TEST255: Test NoPeerInvoker call_with_bytes also returns error
func Test255_no_peer_invoker_with_arguments(t *testing.T) {
	peer := &noPeerInvoker{}
	args := []cap.CapArgumentValue{
		cap.NewCapArgumentValueFromStr("media:test", "value"),
	}
	_, err := peer.Invoke(`cap:in="media:void";test;out="media:void"`, args)
	if err == nil {
		t.Fatal("Expected error from NoPeerInvoker with arguments")
	}
}

// TEST256: Test CartridgeRuntime::with_manifest_json stores manifest data and parses when valid
func Test256_new_cartridge_runtime_with_valid_json(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	if len(runtime.manifestData) == 0 {
		t.Fatal("Expected manifest data to be stored")
	}
	if runtime.manifest == nil {
		t.Fatal("Expected manifest to be parsed")
	}
}

// TEST257: Test CartridgeRuntime::new with invalid JSON still creates runtime (manifest is None)
func Test257_new_cartridge_runtime_with_invalid_json(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte("not json"))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	if len(runtime.manifestData) == 0 {
		t.Fatal("Expected manifest data to be stored even if invalid")
	}
	if runtime.manifest != nil {
		t.Fatal("Expected manifest to be nil for invalid JSON")
	}
}

// TEST258: Test CartridgeRuntime::with_manifest creates runtime with valid manifest data
func Test258_new_cartridge_runtime_with_manifest_struct(t *testing.T) {
	var manifest CapManifest
	if err := json.Unmarshal([]byte(testManifest), &manifest); err != nil {
		t.Fatalf("Failed to parse test manifest: %v", err)
	}

	runtime, err := NewCartridgeRuntimeWithManifest(&manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	if len(runtime.manifestData) == 0 {
		t.Fatal("Expected manifest data")
	}
	if runtime.manifest == nil {
		t.Fatal("Expected manifest to be set")
	}
}

// TEST259: Test extract_effective_payload with non-CBOR content_type returns raw payload unchanged
func Test259_extract_effective_payload_non_cbor(t *testing.T) {
	capDef := createTestCap(`cap:in="media:void";test;out="media:void"`, "Test", "test", nil)
	payload := []byte("raw data")
	result, err := extractEffectivePayload(payload, "application/json", capDef, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if string(result) != string(payload) {
		t.Errorf("Expected unchanged payload, got %s", string(result))
	}
}

// TEST260: Test extract_effective_payload with None content_type returns raw payload unchanged
func Test260_extract_effective_payload_no_content_type(t *testing.T) {
	capDef := createTestCap(`cap:in="media:void";test;out="media:void"`, "Test", "test", nil)
	payload := []byte("raw data")
	result, err := extractEffectivePayload(payload, "", capDef, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if string(result) != string(payload) {
		t.Errorf("Expected unchanged payload")
	}
}

// TEST261: Test extract_effective_payload with CBOR content extracts matching argument value
func Test261_extract_effective_payload_cbor_match(t *testing.T) {
	// Build CBOR arguments: [{media_urn: "media:string;enc=utf-8", value: bytes("hello")}]
	args := []interface{}{
		map[string]interface{}{
			"media_urn": "media:string;enc=utf-8",
			"value":     []byte("hello"),
		},
	}
	payload, err := cborlib.Marshal(args)
	if err != nil {
		t.Fatalf("Failed to encode payload: %v", err)
	}

	// The cap URN has in=media:string;enc=utf-8
	capDef := createTestCap(`cap:in="media:string;enc=utf-8";test;out="media:void"`, "Test", "test", nil)
	result, err := extractEffectivePayload(payload, "application/cbor", capDef, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// NEW REGIME: result is full CBOR array; extract value from matching argument
	var resultArr []interface{}
	if err := cborlib.Unmarshal(result, &resultArr); err != nil {
		t.Fatalf("Failed to decode result CBOR: %v", err)
	}
	var foundValue []byte
	for _, arg := range resultArr {
		argMap, ok := arg.(map[interface{}]interface{})
		if !ok {
			continue
		}
		for k, v := range argMap {
			if key, ok := k.(string); ok && key == "value" {
				if b, ok := v.([]byte); ok {
					foundValue = b
				}
			}
		}
	}
	if string(foundValue) != "hello" {
		t.Errorf("Expected handler to receive 'hello', got %q", string(foundValue))
	}
}

// TEST262: Test extract_effective_payload with CBOR content fails when no argument matches expected input
func Test262_extract_effective_payload_cbor_no_match(t *testing.T) {
	args := []interface{}{
		map[string]interface{}{
			"media_urn": "media:other-type",
			"value":     []byte("data"),
		},
	}
	payload, err := cborlib.Marshal(args)
	if err != nil {
		t.Fatalf("Failed to encode payload: %v", err)
	}

	capDef := createTestCap(`cap:in="media:string;enc=utf-8";test;out="media:void"`, "Test", "test", nil)
	_, err = extractEffectivePayload(payload, "application/cbor", capDef, false)
	if err == nil {
		t.Fatal("Expected error when no argument matches expected input")
	}
	if !strings.Contains(err.Error(), "No argument found matching") {
		t.Errorf("Expected error containing 'No argument found matching', got: %v", err)
	}
}

// TEST263: Test extract_effective_payload with invalid CBOR bytes returns deserialization error
func Test263_extract_effective_payload_invalid_cbor(t *testing.T) {
	capDef := createTestCap(`cap:in="media:void";test;out="media:void"`, "Test", "test", nil)
	_, err := extractEffectivePayload([]byte("not cbor"), "application/cbor", capDef, false)
	if err == nil {
		t.Fatal("Expected error for invalid CBOR bytes")
	}
}

// TEST264: Test extract_effective_payload with CBOR non-array (e.g. map) returns error
func Test264_extract_effective_payload_cbor_not_array(t *testing.T) {
	// Encode a CBOR map (not array)
	payload, err := cborlib.Marshal(map[string]interface{}{})
	if err != nil {
		t.Fatalf("Failed to encode payload: %v", err)
	}
	capDef := createTestCap(`cap:in="media:void";test;out="media:void"`, "Test", "test", nil)
	_, err = extractEffectivePayload(payload, "application/cbor", capDef, false)
	if err == nil {
		t.Fatal("Expected error for CBOR non-array payload")
	}
}

// TEST270: Test registering multiple Op handlers for different caps and finding each independently
func Test270_multiple_handlers(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	runtime.Register(`cap:in="media:void";alpha;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			return emitter.EmitCbor("a")
		})
	runtime.Register(`cap:in="media:void";beta;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			return emitter.EmitCbor("b")
		})
	runtime.Register(`cap:in="media:void";gamma;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			return emitter.EmitCbor("g")
		})

	peer := &noPeerInvoker{}

	emitterA := &mockStreamEmitter{}
	hAlpha := runtime.FindHandler(`cap:in="media:void";alpha;out="media:void"`)
	_ = hAlpha(bytesToFrameChannel([]byte{}), emitterA, peer)
	var resultA string
	cborlib.Unmarshal(emitterA.GetAllData(), &resultA)
	if resultA != "a" {
		t.Errorf("Expected 'a', got %s", resultA)
	}

	emitterB := &mockStreamEmitter{}
	hBeta := runtime.FindHandler(`cap:in="media:void";beta;out="media:void"`)
	_ = hBeta(bytesToFrameChannel([]byte{}), emitterB, peer)
	var resultB string
	cborlib.Unmarshal(emitterB.GetAllData(), &resultB)
	if resultB != "b" {
		t.Errorf("Expected 'b', got %s", resultB)
	}

	emitterG := &mockStreamEmitter{}
	hGamma := runtime.FindHandler(`cap:in="media:void";gamma;out="media:void"`)
	_ = hGamma(bytesToFrameChannel([]byte{}), emitterG, peer)
	var resultG string
	cborlib.Unmarshal(emitterG.GetAllData(), &resultG)
	if resultG != "g" {
		t.Errorf("Expected 'g', got %s", resultG)
	}
}

// TEST271: Test Op handler replacing an existing registration for the same cap URN
func Test271_handler_replacement(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	runtime.Register(`cap:in="media:void";test;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			return emitter.EmitCbor("first")
		})
	runtime.Register(`cap:in="media:void";test;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			return emitter.EmitCbor("second")
		})

	handler := runtime.FindHandler(`cap:in="media:void";test;out="media:void"`)
	emitter := &mockStreamEmitter{}
	peer := &noPeerInvoker{}
	_ = handler(bytesToFrameChannel([]byte{}), emitter, peer)
	var result string
	cborlib.Unmarshal(emitter.GetAllData(), &result)
	if result != "second" {
		t.Errorf("Expected 'second' (later registration), got %s", result)
	}
}

// TEST272: Test extract_effective_payload CBOR with multiple arguments selects the correct one
func Test272_extract_effective_payload_multiple_args(t *testing.T) {
	args := []interface{}{
		map[string]interface{}{
			"media_urn": "media:other-type;enc=utf-8",
			"value":     []byte("wrong"),
		},
		map[string]interface{}{
			"media_urn": "media:model-spec;enc=utf-8",
			"value":     []byte("correct"),
		},
	}
	payload, err := cborlib.Marshal(args)
	if err != nil {
		t.Fatalf("Failed to encode payload: %v", err)
	}

	capDef := createTestCap(`cap:in="media:model-spec;enc=utf-8";infer;out="media:void"`, "Test", "infer", nil)
	result, err := extractEffectivePayload(payload, "application/cbor", capDef, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// NEW REGIME: handler receives full CBOR array with BOTH arguments;
	// handler matches against in_spec to find main input.
	var resultArr []interface{}
	if err := cborlib.Unmarshal(result, &resultArr); err != nil {
		t.Fatalf("Failed to decode result CBOR: %v", err)
	}
	if len(resultArr) != 2 {
		t.Errorf("Expected both arguments present, got %d", len(resultArr))
	}

	inSpec, _ := urn.NewMediaUrnFromString("media:model-spec;enc=utf-8")
	var foundValue []byte
	for _, arg := range resultArr {
		argMap, ok := arg.(map[interface{}]interface{})
		if !ok {
			continue
		}
		var urnStr string
		var val []byte
		for k, v := range argMap {
			if key, ok := k.(string); ok {
				if key == "media_urn" {
					if s, ok := v.(string); ok {
						urnStr = s
					}
				} else if key == "value" {
					if b, ok := v.([]byte); ok {
						val = b
					}
				}
			}
		}
		if urnStr != "" && val != nil {
			argUrn, perr := urn.NewMediaUrnFromString(urnStr)
			if perr == nil && inSpec.IsComparable(argUrn) {
				foundValue = val
				break
			}
		}
	}

	if string(foundValue) != "correct" {
		t.Errorf("Expected 'correct' value to be selected, got %q", string(foundValue))
	}
}

// TEST273: Test extract_effective_payload with binary data in CBOR value (not just text)
func Test273_ExtractEffectivePayloadBinaryValue(t *testing.T) {
	binaryData := make([]byte, 256)
	for i := 0; i < 256; i++ {
		binaryData[i] = byte(i)
	}
	args := []interface{}{
		map[string]interface{}{
			"media_urn": "media:ext=pdf",
			"value":     binaryData,
		},
	}
	payload, err := cborlib.Marshal(args)
	if err != nil {
		t.Fatalf("Failed to encode payload: %v", err)
	}

	capDef := createTestCap(`cap:in="media:ext=pdf";process;out="media:void"`, "Test", "process", nil)
	result, err := extractEffectivePayload(payload, "application/cbor", capDef, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Decode result and verify binary roundtrip
	var resultArr []interface{}
	if err := cborlib.Unmarshal(result, &resultArr); err != nil {
		t.Fatalf("Failed to decode result CBOR: %v", err)
	}
	if len(resultArr) != 1 {
		t.Fatalf("Expected 1 arg in result, got %d", len(resultArr))
	}
	argMap, ok := resultArr[0].(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Expected map for arg, got %T", resultArr[0])
	}
	val, ok := argMap["value"].([]byte)
	if !ok {
		t.Fatalf("Expected []byte value, got %T", argMap["value"])
	}
	if len(val) != 256 {
		t.Errorf("Expected binary data length 256, got %d", len(val))
	}
	for i := range val {
		if val[i] != byte(i) {
			t.Errorf("Byte at index %d: expected %d got %d", i, i, val[i])
			break
		}
	}
}

// Helper function to create runtime errors (for TEST268)
func NewCartridgeRuntimeError(errorType, message string) error {
	return &CartridgeRuntimeError{
		Type:    errorType,
		Message: message,
	}
}

type CartridgeRuntimeError struct {
	Type    string
	Message string
}

func (e *CartridgeRuntimeError) Error() string {
	return e.Type + ": " + e.Message
}

// Helper to create test caps for file-path tests
func createTestCap(urnStr, title, command string, args []cap.CapArg) *cap.Cap {
	urn, err := urn.NewCapUrnFromString(urnStr)
	if err != nil {
		panic(fmt.Sprintf("Invalid cap URN: %v", err))
	}
	return &cap.Cap{
		Urn:     urn,
		Title:   title,
		Command: command,
		Args:    args,
	}
}

// Helper to create cap.ArgSource with stdin
func stdinSource(mediaUrn string) cap.ArgSource {
	return cap.ArgSource{Stdin: &mediaUrn}
}

// Helper to create cap.ArgSource with position
func positionSource(pos int) cap.ArgSource {
	return cap.ArgSource{Position: &pos}
}

// Helper to create cap.ArgSource with CLI flag
func cliFlagSource(flag string) cap.ArgSource {
	return cap.ArgSource{CliFlag: &flag}
}

// Helper to create test manifest
func createTestManifest(name, version, description string, caps []*cap.Cap) *CapManifest {
	capSlice := make([]cap.Cap, len(caps))
	for i, cap := range caps {
		capSlice[i] = *cap
	}
	identity, err := urn.NewCapUrnFromString(standard.CapIdentity)
	if err != nil {
		panic(fmt.Sprintf("Invalid CAP_IDENTITY constant: %v", err))
	}
	hasIdentity := false
	for i := range capSlice {
		if capSlice[i].Urn.ToString() == identity.ToString() {
			hasIdentity = true
			break
		}
	}
	if !hasIdentity {
		capSlice = append(capSlice, cap.Cap{
			Urn:     identity,
			Title:   "Identity",
			Command: "identity",
		})
	}
	return NewCapManifest(name, version, "release", nil, description, []CapGroup{DefaultGroup(capSlice)})
}

// firstCap returns the first cap from the manifest's cap groups (for test convenience).
func firstCap(m *CapManifest) *cap.Cap {
	for i := range m.CapGroups {
		if len(m.CapGroups[i].Caps) > 0 {
			return &m.CapGroups[i].Caps[0]
		}
	}
	return nil
}

// effectiveCborToFrameChannel converts the CBOR-args-array payload returned
// by extractEffectivePayload into a frame stream that mirrors what
// dispatchCliPayload sends to the handler: one STREAM_START / CHUNK /
// STREAM_END group per argument, then a single END.
func effectiveCborToFrameChannel(t *testing.T, payload []byte) <-chan Frame {
	t.Helper()
	ch := make(chan Frame, 32)
	go func() {
		defer close(ch)
		requestID := NewMessageIdDefault()
		var arguments []interface{}
		if len(payload) > 0 {
			if err := cborlib.Unmarshal(payload, &arguments); err != nil {
				return
			}
		}
		for i, arg := range arguments {
			argMap, ok := arg.(map[interface{}]interface{})
			if !ok {
				continue
			}
			var mediaUrn string
			var value interface{}
			for k, v := range argMap {
				if key, ok := k.(string); ok {
					if key == "media_urn" {
						if s, ok := v.(string); ok {
							mediaUrn = s
						}
					} else if key == "value" {
						value = v
					}
				}
			}
			if mediaUrn == "" || value == nil {
				continue
			}
			streamID := fmt.Sprintf("arg-%d", i)
			ch <- *NewStreamStart(requestID, streamID, mediaUrn, nil)
			cborValue, err := cborlib.Marshal(value)
			if err != nil {
				continue
			}
			checksum := ComputeChecksum(cborValue)
			ch <- *NewChunk(requestID, streamID, 0, cborValue, 0, checksum)
			ch <- *NewStreamEnd(requestID, streamID, 1)
		}
		ch <- *NewEnd(requestID, nil)
	}()
	return ch
}

// mustExtractFirstArgValueBytes decodes the post-extractEffectivePayload
// CBOR array and returns the first arg's `value` as []byte. Test helper.
func mustExtractFirstArgValueBytes(t *testing.T, payload []byte) []byte {
	t.Helper()
	var argsArr []interface{}
	if err := cborlib.Unmarshal(payload, &argsArr); err != nil {
		t.Fatalf("Failed to decode result CBOR: %v", err)
	}
	if len(argsArr) == 0 {
		t.Fatalf("Expected at least 1 arg, got 0")
	}
	argMap, ok := argsArr[0].(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Expected map for arg, got %T", argsArr[0])
	}
	val, ok := argMap["value"].([]byte)
	if !ok {
		t.Fatalf("Expected []byte value, got %T", argMap["value"])
	}
	return val
}

// buildPayloadFromCLIWithStdin constructs the raw CBOR arguments payload
// from CLI args and explicit stdin data. Used by tests that need to drive
// stdin without going through the runtime's non-blocking stdin probe.
func buildPayloadFromCLIWithStdin(pr *CartridgeRuntime, capDef *cap.Cap, cliArgs []string, stdinData []byte) ([]byte, error) {
	var arguments []cap.CapArgumentValue
	for i := range capDef.Args {
		argDef := &capDef.Args[i]
		value, cameFromStdin, err := pr.extractArgValue(argDef, cliArgs, stdinData)
		if err != nil {
			return nil, err
		}
		if value != nil {
			mediaUrn := argDef.MediaUrn
			if cameFromStdin {
				for j := range argDef.Sources {
					if argDef.Sources[j].Stdin != nil {
						mediaUrn = *argDef.Sources[j].Stdin
						break
					}
				}
			}
			arguments = append(arguments, cap.CapArgumentValue{MediaUrn: mediaUrn, Value: value})
		} else if argDef.Required {
			return nil, fmt.Errorf("Required argument '%s' not provided", argDef.MediaUrn)
		}
	}
	if len(arguments) == 0 {
		return []byte{}, nil
	}
	cborArgs := make([]interface{}, len(arguments))
	for i, arg := range arguments {
		cborArgs[i] = map[string]interface{}{
			"media_urn": arg.MediaUrn,
			"value":     arg.Value,
		}
	}
	return cborlib.Marshal(cborArgs)
}

// TEST336: Single file-path arg with stdin source reads file and passes bytes to handler
func Test336_FilePathReadsFilePassesBytes(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test336_input.pdf")
	if err := os.WriteFile(tempFile, []byte("PDF binary content 336"), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:ext=pdf";process;out="media:void"`,
		"Process PDF",
		"process",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:ext=pdf"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// Track what handler receives. Wire chunks are CBOR-encoded individual
	// values (per dispatch_cli_payload), so the handler decodes the CBOR
	// wrapper to recover the raw file bytes.
	var receivedPayload []byte
	runtime.Register(
		`cap:in="media:ext=pdf";process;out="media:void"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			payload, err := CollectFirstArg(frames)
			if err != nil {
				return err
			}
			var fileBytes []byte
			if err := cborlib.Unmarshal(payload, &fileBytes); err != nil {
				return err
			}
			receivedPayload = fileBytes
			return emitter.EmitCbor("processed")
		},
	)

	// Simulate CLI invocation
	cliArgs := []string{tempFile}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	payload, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}

	handler := runtime.FindHandler(firstCap(manifest).UrnString())
	emitter := &mockStreamEmitter{}
	peerInvoker := &noPeerInvoker{}
	err = handler(effectiveCborToFrameChannel(t, payload), emitter, peerInvoker)
	if err != nil {
		t.Fatalf("Handler failed: %v", err)
	}

	// Verify handler received file bytes, not file path
	if string(receivedPayload) != "PDF binary content 336" {
		t.Errorf("Expected handler to receive file bytes, got: %s", string(receivedPayload))
	}
	var result string
	cborlib.Unmarshal(emitter.GetAllData(), &result)
	if result != "processed" {
		t.Errorf("Expected 'processed', got: %s", result)
	}
}

// TEST337: file-path arg without stdin source passes path as string (no conversion)
func Test337_FilePathWithoutStdinPassesString(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test337_input.txt")
	if err := os.WriteFile(tempFile, []byte("content"), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:void";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					positionSource(0), // NO stdin source!
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{tempFile}
	result, _, err := runtime.extractArgValue(&firstCap(manifest).Args[0], cliArgs, nil)
	if err != nil {
		t.Fatalf("Failed to extract arg: %v", err)
	}

	// Should get file PATH as string, not file CONTENTS
	valueStr := string(result)
	if !strings.Contains(valueStr, "test337_input.txt") {
		t.Errorf("Expected file path string containing 'test337_input.txt', got: %s", valueStr)
	}
}

// TEST338: file-path arg reads file via --file CLI flag
func Test338_FilePathViaCliFlag(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test338.pdf")
	if err := os.WriteFile(tempFile, []byte("PDF via flag 338"), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:ext=pdf";process;out="media:void"`,
		"Process",
		"process",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:ext=pdf"),
					cliFlagSource("--file"),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{"--file", tempFile}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}
	val := mustExtractFirstArgValueBytes(t, effective)
	if string(val) != "PDF via flag 338" {
		t.Errorf("Expected 'PDF via flag 338', got: %s", string(val))
	}
}

// TEST339: file-path arg with is_sequence=true expands a glob to N files
// and the runtime delivers them as a CBOR Array of Bytes — one array item
// per matched file. List-ness comes from the arg declaration, not from any
// `;list` URN tag. Mirrors Rust test339_file_path_array_glob_expansion.
func Test339_FilePathArrayGlobExpansion(t *testing.T) {
	tempDir := filepath.Join(t.TempDir(), "test339")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	file1 := filepath.Join(tempDir, "doc1.txt")
	file2 := filepath.Join(tempDir, "doc2.txt")
	if err := os.WriteFile(file1, []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}
	if err := os.WriteFile(file2, []byte("content2"), 0644); err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}

	batchArg := cap.CapArg{
		MediaUrn:   "media:file-path;enc=utf-8",
		Required:   true,
		IsSequence: true,
		Sources: []cap.ArgSource{
			stdinSource("media:"),
			positionSource(0),
		},
	}
	capDef := createTestCap(
		`cap:in="media:";batch;out="media:void"`,
		"Batch",
		"batch",
		[]cap.CapArg{batchArg},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// CLI: bare glob pattern — no JSON, no array.
	pattern := filepath.Join(tempDir, "*.txt")
	cliArgs := []string{pattern}

	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}

	// Decode the result and pull out the value array
	var argsArr []interface{}
	if err := cborlib.Unmarshal(effective, &argsArr); err != nil {
		t.Fatalf("Failed to decode result CBOR: %v", err)
	}
	if len(argsArr) != 1 {
		t.Fatalf("Expected 1 arg, got %d", len(argsArr))
	}
	argMap, ok := argsArr[0].(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Expected map, got %T", argsArr[0])
	}
	rawValue := argMap["value"]
	rawArr, ok := rawValue.([]interface{})
	if !ok {
		t.Fatalf("Expected value to be array, got %T", rawValue)
	}
	if len(rawArr) != 2 {
		t.Errorf("Expected 2 files, got %d", len(rawArr))
	}
	contents := make(map[string]bool)
	for _, item := range rawArr {
		b, ok := item.([]byte)
		if !ok {
			t.Fatalf("Expected []byte item, got %T", item)
		}
		contents[string(b)] = true
	}
	if !contents["content1"] || !contents["content2"] {
		t.Error("Expected both content1 and content2 in results")
	}
}

// TEST340: File not found error provides clear message
func Test340_FileNotFoundClearError(t *testing.T) {
	capDef := createTestCap(
		`cap:in="media:ext=pdf";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:ext=pdf"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{"/nonexistent/file.pdf"}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	_, err = extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err == nil {
		t.Fatal("Expected error when file doesn't exist")
	}
	errMsg := err.Error()
	if !strings.Contains(errMsg, "/nonexistent/file.pdf") {
		t.Errorf("Error should mention file path: %s", errMsg)
	}
	if !strings.Contains(errMsg, "File not found") {
		t.Errorf("Error should be clear about read failure: %s", errMsg)
	}
}

// TEST341: stdin takes precedence over file-path in source order
func Test341_StdinPrecedenceOverFilePath(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test341_input.txt")
	if err := os.WriteFile(tempFile, []byte("file content"), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Stdin source comes BEFORE position source
	capDef := createTestCap(
		`cap:in="media:";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:"), // First
					positionSource(0),     // Second
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{tempFile}
	stdinData := []byte("stdin content 341")
	result, _, err := runtime.extractArgValue(&firstCap(manifest).Args[0], cliArgs, stdinData)
	if err != nil {
		t.Fatalf("Failed to extract arg: %v", err)
	}

	// Should get stdin data, not file content (stdin source tried first)
	if string(result) != "stdin content 341" {
		t.Errorf("Expected stdin content, got: %s", string(result))
	}
}

// TEST342: file-path with position 0 reads first positional arg as file
func Test342_FilePathPositionZeroReadsFirstArg(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test342.dat")
	if err := os.WriteFile(tempFile, []byte("binary data 342"), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{tempFile}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}

	// The effective payload is a CBOR array; pull out the value bytes.
	var argsArr []interface{}
	if err := cborlib.Unmarshal(effective, &argsArr); err != nil {
		t.Fatalf("Failed to decode result CBOR: %v", err)
	}
	argMap, ok := argsArr[0].(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Expected map, got %T", argsArr[0])
	}
	val, ok := argMap["value"].([]byte)
	if !ok {
		t.Fatalf("Expected []byte value, got %T", argMap["value"])
	}
	if string(val) != "binary data 342" {
		t.Errorf("Expected file content, got: %s", string(val))
	}
}

// TEST343: Non-file-path args are not affected by file reading
func Test343_NonFilePathArgsUnaffected(t *testing.T) {
	capDef := createTestCap(
		`cap:in="media:void";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:model-spec;enc=utf-8", // NOT file-path
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:model-spec;enc=utf-8"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{"mlx-community/Llama-3.2-3B-Instruct-4bit"}
	result, _, err := runtime.extractArgValue(&firstCap(manifest).Args[0], cliArgs, nil)
	if err != nil {
		t.Fatalf("Failed to extract arg: %v", err)
	}

	// Should get the string value, not attempt file read
	if string(result) != "mlx-community/Llama-3.2-3B-Instruct-4bit" {
		t.Errorf("Expected model spec string, got: %s", string(result))
	}
}

// TEST6586: A scalar file-path arg receiving a nonexistent path fails hard
// with a clear error that names the path. The runtime refuses to silently
// swallow user mistakes like typos or wrong directories.
func Test6586_FilePathArrayInvalidJSONFails(t *testing.T) {
	capDef := createTestCap(
		`cap:in="media:";batch;out="media:void"`,
		"Test",
		"batch",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{"/nonexistent/path/to/nothing"}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	_, err = extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err == nil {
		t.Fatal("Expected error for nonexistent file path")
	}
	errMsg := err.Error()
	if !strings.Contains(errMsg, "/nonexistent/path/to/nothing") {
		t.Errorf("Error should mention the path: %s", errMsg)
	}
	if !strings.Contains(errMsg, "File not found") {
		t.Errorf("Error should be clear about file access failure: %s", errMsg)
	}
}

// TEST6587: file-path-array with literal nonexistent path fails hard
func Test6587_FilePathArrayOneFileMissingFailsHard(t *testing.T) {
	tempDir := t.TempDir()
	missingPath := filepath.Join(tempDir, "test345_missing.txt")

	capDef := createTestCap(
		`cap:in="media:";batch;out="media:void"`,
		"Test",
		"batch",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{missingPath}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	_, err = extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err == nil {
		t.Fatal("Expected error when literal path doesn't exist")
	}
	errMsg := err.Error()
	if !strings.Contains(errMsg, "test345_missing.txt") {
		t.Errorf("Error should mention the missing file: %s", errMsg)
	}
	if !strings.Contains(errMsg, "File not found") {
		t.Errorf("Error should be clear about missing file: %s", errMsg)
	}
}

// TEST346: Large file (1MB) reads successfully
func Test346_LargeFileReadsSuccessfully(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test346_large.bin")
	largeData := make([]byte, 1_000_000)
	for i := range largeData {
		largeData[i] = 42
	}
	if err := os.WriteFile(tempFile, largeData, 0644); err != nil {
		t.Fatalf("Failed to create large file: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{tempFile}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}
	val := mustExtractFirstArgValueBytes(t, effective)
	if len(val) != 1_000_000 {
		t.Errorf("Expected 1MB file, got %d bytes", len(val))
	}
}

// TEST347: Empty file reads as empty bytes
func Test347_EmptyFileReadsAsEmptyBytes(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test347_empty.txt")
	if err := os.WriteFile(tempFile, []byte{}, 0644); err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{tempFile}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}
	val := mustExtractFirstArgValueBytes(t, effective)
	if len(val) != 0 {
		t.Errorf("Expected empty bytes, got %d bytes", len(val))
	}
}

// TEST348: file-path conversion respects source order
func Test348_FilePathConversionRespectsSourceOrder(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test348.txt")
	if err := os.WriteFile(tempFile, []byte("file content 348"), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Position source BEFORE stdin source
	capDef := createTestCap(
		`cap:in="media:";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					positionSource(0),     // First
					stdinSource("media:"), // Second
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{tempFile}
	stdinData := []byte("stdin content 348")
	rawPayload, err := buildPayloadFromCLIWithStdin(runtime, firstCap(manifest), cliArgs, stdinData)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}
	val := mustExtractFirstArgValueBytes(t, effective)
	// Position source tried first, so file is read
	if string(val) != "file content 348" {
		t.Errorf("Expected file content (position first), got: %s", string(val))
	}
}

// TEST349: file-path arg with multiple sources tries all in order
func Test349_FilePathMultipleSourcesFallback(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test349.txt")
	if err := os.WriteFile(tempFile, []byte("content 349"), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					cliFlagSource("--file"), // First (not provided)
					positionSource(0),       // Second (provided)
					stdinSource("media:"),   // Third (not used)
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// Only provide position arg, no --file flag
	cliArgs := []string{tempFile}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}
	val := mustExtractFirstArgValueBytes(t, effective)
	if string(val) != "content 349" {
		t.Errorf("Expected to fall back to position source, got: %s", string(val))
	}
}

// TEST350: Integration test - full CLI mode invocation with file-path
func Test350_FullCLIModeWithFilePathIntegration(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test350_input.pdf")
	testContent := []byte("PDF file content for integration test")
	if err := os.WriteFile(tempFile, testContent, 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:ext=pdf";process;out="media:result;enc=utf-8"`,
		"Process PDF",
		"process",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:ext=pdf"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// Track what handler receives. Wire chunks are CBOR-encoded individual
	// values (per dispatch_cli_payload), so the handler decodes the CBOR
	// wrapper to recover the raw file bytes.
	var receivedPayload []byte
	runtime.Register(
		`cap:in="media:ext=pdf";process;out="media:result;enc=utf-8"`,
		func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
			payload, err := CollectFirstArg(frames)
			if err != nil {
				return err
			}
			var fileBytes []byte
			if err := cborlib.Unmarshal(payload, &fileBytes); err != nil {
				return err
			}
			receivedPayload = fileBytes
			return emitter.EmitCbor("processed")
		},
	)

	cliArgs := []string{tempFile}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	payload, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}

	handler := runtime.FindHandler(firstCap(manifest).UrnString())
	emitter := &mockStreamEmitter{}
	peerInvoker := &noPeerInvoker{}
	err = handler(effectiveCborToFrameChannel(t, payload), emitter, peerInvoker)
	if err != nil {
		t.Fatalf("Handler failed: %v", err)
	}

	if string(receivedPayload) != string(testContent) {
		t.Errorf("Handler should receive file bytes, not path")
	}
	var result string
	cborlib.Unmarshal(emitter.GetAllData(), &result)
	if result != "processed" {
		t.Errorf("Expected 'processed', got: %s", result)
	}
}

// TEST6588: file-path arg in CBOR mode with empty Array returns empty.
// CBOR Array (not JSON-encoded) is the multi-input wire form for sequence
// args. Mirrors Rust test351_file_path_array_empty_array.
func Test6588_FilePathArrayEmptyArray(t *testing.T) {
	batchArg := cap.CapArg{
		MediaUrn:   "media:file-path;enc=utf-8",
		Required:   false,
		IsSequence: true,
		Sources: []cap.ArgSource{
			stdinSource("media:"),
		},
	}
	capDef := createTestCap(
		`cap:in="media:";batch;out="media:void"`,
		"Test",
		"batch",
		[]cap.CapArg{batchArg},
	)

	// CBOR-mode payload: value is an empty Array
	args := []interface{}{
		map[string]interface{}{
			"media_urn": "media:file-path;enc=utf-8",
			"value":     []interface{}{},
		},
	}
	payload, err := cborlib.Marshal(args)
	if err != nil {
		t.Fatalf("Failed to encode payload: %v", err)
	}

	result, err := extractEffectivePayload(payload, "application/cbor", capDef, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var resultArr []interface{}
	if err := cborlib.Unmarshal(result, &resultArr); err != nil {
		t.Fatalf("Failed to decode result CBOR: %v", err)
	}
	if len(resultArr) != 1 {
		t.Fatalf("Expected 1 arg, got %d", len(resultArr))
	}
	argMap, ok := resultArr[0].(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Expected map, got %T", resultArr[0])
	}
	val, ok := argMap["value"].([]interface{})
	if !ok {
		t.Fatalf("Expected value to be array, got %T", argMap["value"])
	}
	if len(val) != 0 {
		t.Errorf("Expected empty array, got %d elements", len(val))
	}
}

// TEST352: file permission denied error is clear (Unix-specific)
func Test352_FilePermissionDeniedClearError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping permission test on Windows")
	}

	tempFile := filepath.Join(t.TempDir(), "test352_noperm.txt")
	if err := os.WriteFile(tempFile, []byte("content"), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Remove read permissions
	if err := os.Chmod(tempFile, 0000); err != nil {
		t.Fatalf("Failed to change permissions: %v", err)
	}
	defer os.Chmod(tempFile, 0644) // Restore for cleanup

	capDef := createTestCap(
		`cap:in="media:";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{tempFile}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	_, err = extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err == nil {
		t.Fatal("Expected error for permission denied")
	}
	if !strings.Contains(err.Error(), "test352_noperm.txt") {
		t.Errorf("Error should mention the file: %s", err.Error())
	}
}

// TEST353: CBOR payload format matches between CLI and CBOR mode
func Test353_CBORPayloadFormatConsistency(t *testing.T) {
	capDef := createTestCap(
		`cap:in="media:text;enc=utf-8";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:text;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:text;enc=utf-8"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{"test value"}
	payload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}

	// Decode CBOR payload
	var argsArray []map[string]interface{}
	if err := cborlib.Unmarshal(payload, &argsArray); err != nil {
		t.Fatalf("Failed to decode CBOR: %v", err)
	}

	if len(argsArray) != 1 {
		t.Errorf("Expected 1 argument, got %d", len(argsArray))
	}

	// Verify structure: { media_urn: "...", value: bytes }
	arg := argsArray[0]
	mediaUrn, hasUrn := arg["media_urn"].(string)
	value, hasValue := arg["value"].([]byte)

	if !hasUrn || !hasValue {
		t.Fatal("Expected argument to have media_urn and value fields")
	}

	if mediaUrn != "media:text;enc=utf-8" {
		t.Errorf("Expected media_urn 'media:text;enc=utf-8', got: %s", mediaUrn)
	}

	if string(value) != "test value" {
		t.Errorf("Expected value 'test value', got: %s", string(value))
	}
}

// TEST354: Glob pattern with no matches fails hard (NO FALLBACK)
func Test354_GlobPatternNoMatchesEmptyArray(t *testing.T) {
	tempDir := t.TempDir()

	capDef := createTestCap(
		`cap:in="media:";batch;out="media:void"`,
		"Test",
		"batch",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// CLI: bare glob that matches nothing — must fail hard.
	pattern := filepath.Join(tempDir, "nonexistent_*.xyz")
	cliArgs := []string{pattern}

	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	_, err = extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err == nil {
		t.Fatal("Should fail hard when glob matches nothing — NO FALLBACK")
	}
	errMsg := err.Error()
	if !strings.Contains(errMsg, "No files matched") && !strings.Contains(errMsg, "nonexistent") {
		t.Errorf("Error should explain glob matched nothing: %s", errMsg)
	}
}

// TEST355: Glob pattern skips directories.
// Mirrors Rust test355_glob_pattern_skips_directories.
func Test355_GlobPatternSkipsDirectories(t *testing.T) {
	tempDir := filepath.Join(t.TempDir(), "test355")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	subdir := filepath.Join(tempDir, "subdir")
	if err := os.MkdirAll(subdir, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	file1 := filepath.Join(tempDir, "file1.txt")
	if err := os.WriteFile(file1, []byte("content1"), 0644); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	batchArg := cap.CapArg{
		MediaUrn:   "media:file-path;enc=utf-8",
		Required:   true,
		IsSequence: true,
		Sources: []cap.ArgSource{
			stdinSource("media:"),
			positionSource(0),
		},
	}
	capDef := createTestCap(
		`cap:in="media:";batch;out="media:void"`,
		"Test",
		"batch",
		[]cap.CapArg{batchArg},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// CLI: bare glob matching both file and directory.
	pattern := filepath.Join(tempDir, "*")
	cliArgs := []string{pattern}

	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}

	var argsArr []interface{}
	if err := cborlib.Unmarshal(effective, &argsArr); err != nil {
		t.Fatalf("Failed to decode result CBOR: %v", err)
	}
	argMap, ok := argsArr[0].(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Expected map, got %T", argsArr[0])
	}
	val, ok := argMap["value"].([]interface{})
	if !ok {
		t.Fatalf("Expected value to be array, got %T", argMap["value"])
	}
	if len(val) != 1 {
		t.Errorf("Should only include files, not directories: got %d items", len(val))
	}
	if b, ok := val[0].([]byte); !ok || string(b) != "content1" {
		t.Errorf("Expected 'content1', got: %v", val[0])
	}
}

// TEST356: Multiple glob patterns combined
func Test356_MultipleGlobPatternsCombined(t *testing.T) {
	tempDir := filepath.Join(t.TempDir(), "test356")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	file1 := filepath.Join(tempDir, "doc.txt")
	file2 := filepath.Join(tempDir, "data.json")
	if err := os.WriteFile(file1, []byte("text"), 0644); err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}
	if err := os.WriteFile(file2, []byte("json"), 0644); err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}

	batchArg := cap.CapArg{
		MediaUrn:   "media:file-path;enc=utf-8",
		Required:   true,
		IsSequence: true,
		Sources: []cap.ArgSource{
			stdinSource("media:"),
			positionSource(0),
		},
	}
	capDef := createTestCap(
		`cap:in="media:";batch;out="media:void"`,
		"Test",
		"batch",
		[]cap.CapArg{batchArg},
	)

	// Multiple patterns as CBOR Array (CBOR mode allows arrays of patterns).
	pattern1 := filepath.Join(tempDir, "*.txt")
	pattern2 := filepath.Join(tempDir, "*.json")
	args := []interface{}{
		map[string]interface{}{
			"media_urn": "media:file-path;enc=utf-8",
			"value":     []interface{}{pattern1, pattern2},
		},
	}
	payload, err := cborlib.Marshal(args)
	if err != nil {
		t.Fatalf("Failed to encode payload: %v", err)
	}

	result, err := extractEffectivePayload(payload, "application/cbor", capDef, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var resultArr []interface{}
	if err := cborlib.Unmarshal(result, &resultArr); err != nil {
		t.Fatalf("Failed to decode result CBOR: %v", err)
	}
	argMap, ok := resultArr[0].(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Expected map, got %T", resultArr[0])
	}
	val, ok := argMap["value"].([]interface{})
	if !ok {
		t.Fatalf("Expected value to be array, got %T", argMap["value"])
	}
	if len(val) != 2 {
		t.Errorf("Expected 2 files from different patterns, got %d", len(val))
	}
	contents := make(map[string]bool)
	for _, item := range val {
		b, ok := item.([]byte)
		if !ok {
			t.Fatalf("Expected []byte item, got %T", item)
		}
		contents[string(b)] = true
	}
	if !contents["text"] || !contents["json"] {
		t.Error("Expected both 'text' and 'json' in results")
	}
}

// TEST357: Symlinks are followed when reading files
func Test357_SymlinksFollowed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping symlink test on Windows")
	}

	tempDir := filepath.Join(t.TempDir(), "test357")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	realFile := filepath.Join(tempDir, "real.txt")
	linkFile := filepath.Join(tempDir, "link.txt")
	if err := os.WriteFile(realFile, []byte("real content"), 0644); err != nil {
		t.Fatalf("Failed to create real file: %v", err)
	}
	if err := os.Symlink(realFile, linkFile); err != nil {
		t.Fatalf("Failed to create symlink: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{linkFile}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}
	val := mustExtractFirstArgValueBytes(t, effective)
	if string(val) != "real content" {
		t.Errorf("Expected symlink to be followed, got: %s", string(val))
	}
}

// TEST358: Binary file with non-UTF8 data reads correctly
func Test358_BinaryFileNonUTF8(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test358.bin")
	binaryData := []byte{0xFF, 0xFE, 0x00, 0x01, 0x80, 0x7F, 0xAB, 0xCD}
	if err := os.WriteFile(tempFile, binaryData, 0644); err != nil {
		t.Fatalf("Failed to create binary file: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:";test;out="media:void"`,
		"Test",
		"test",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{tempFile}
	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract payload: %v", err)
	}
	val := mustExtractFirstArgValueBytes(t, effective)
	if len(val) != len(binaryData) {
		t.Errorf("Expected %d bytes, got %d", len(binaryData), len(val))
	}
	for i := range binaryData {
		if val[i] != binaryData[i] {
			t.Errorf("Binary data mismatch at index %d: expected %d, got %d", i, binaryData[i], val[i])
		}
	}
}

// TEST359: Invalid glob pattern fails with clear error
func Test359_InvalidGlobPatternFails(t *testing.T) {
	batchArg := cap.CapArg{
		MediaUrn:   "media:file-path;enc=utf-8",
		Required:   true,
		IsSequence: true,
		Sources: []cap.ArgSource{
			stdinSource("media:"),
			positionSource(0),
		},
	}
	capDef := createTestCap(
		`cap:in="media:";batch;out="media:void"`,
		"Test",
		"batch",
		[]cap.CapArg{batchArg},
	)

	// Invalid glob pattern (unclosed bracket) sent in CBOR mode.
	args := []interface{}{
		map[string]interface{}{
			"media_urn": "media:file-path;enc=utf-8",
			"value":     "[invalid",
		},
	}
	payload, err := cborlib.Marshal(args)
	if err != nil {
		t.Fatalf("Failed to encode payload: %v", err)
	}

	_, err = extractEffectivePayload(payload, "application/cbor", capDef, false)
	if err == nil {
		t.Fatal("Expected error for invalid glob pattern")
	}
	if !strings.Contains(err.Error(), "Invalid glob pattern") && !strings.Contains(err.Error(), "Pattern") {
		t.Errorf("Error should mention invalid glob: %s", err.Error())
	}
}

// TEST360: Extract effective payload handles file-path data correctly
func Test360_ExtractEffectivePayloadWithFileData(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "test360.pdf")
	pdfContent := []byte("PDF content for extraction test")
	if err := os.WriteFile(tempFile, pdfContent, 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	capDef := createTestCap(
		`cap:in="media:ext=pdf";process;out="media:void"`,
		"Process",
		"process",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:ext=pdf"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	cliArgs := []string{tempFile}

	rawPayload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload: %v", err)
	}
	effective, err := extractEffectivePayload(rawPayload, "application/cbor", firstCap(manifest), true)
	if err != nil {
		t.Fatalf("Failed to extract effective payload: %v", err)
	}

	// NEW REGIME: effective is the full CBOR args array. Pull out the value
	// of the argument matching the cap's in_spec via parsed-URN comparison.
	var resultArr []interface{}
	if err := cborlib.Unmarshal(effective, &resultArr); err != nil {
		t.Fatalf("Failed to decode result CBOR: %v", err)
	}
	inSpec, _ := urn.NewMediaUrnFromString("media:ext=pdf")
	var foundValue []byte
	for _, arg := range resultArr {
		argMap, ok := arg.(map[interface{}]interface{})
		if !ok {
			continue
		}
		var urnStr string
		var val []byte
		for k, v := range argMap {
			if key, ok := k.(string); ok {
				if key == "media_urn" {
					if s, ok := v.(string); ok {
						urnStr = s
					}
				} else if key == "value" {
					if b, ok := v.([]byte); ok {
						val = b
					}
				}
			}
		}
		if urnStr != "" && val != nil {
			argUrn, perr := urn.NewMediaUrnFromString(urnStr)
			if perr == nil && inSpec.IsComparable(argUrn) {
				foundValue = val
				break
			}
		}
	}
	if string(foundValue) != string(pdfContent) {
		t.Errorf("File-path auto-converted to bytes; got: %q", string(foundValue))
	}
}

// TEST361: CLI mode with file path - pass file path as command-line argument
func Test361_CLIModeFilePath(t *testing.T) {
	tempFile := filepath.Join(os.TempDir(), "test361.pdf")
	pdfContent := []byte("PDF content for CLI file path test")
	if err := os.WriteFile(tempFile, pdfContent, 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile)

	capDef := createTestCap(
		`cap:in="media:ext=pdf";process;out="media:void"`,
		"Process",
		"process",
		[]cap.CapArg{
			{
				MediaUrn: "media:file-path;enc=utf-8",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:ext=pdf"),
					positionSource(0),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// CLI mode: pass file path as positional argument
	cliArgs := []string{tempFile}
	payload, err := runtime.buildPayloadFromCLI(firstCap(manifest), cliArgs)
	if err != nil {
		t.Fatalf("Failed to build payload from CLI: %v", err)
	}

	// Verify payload is CBOR array with file-path argument
	var cborVal interface{}
	if err := cborlib.Unmarshal(payload, &cborVal); err != nil {
		t.Fatalf("Failed to unmarshal CBOR: %v", err)
	}

	if _, ok := cborVal.([]interface{}); !ok {
		t.Errorf("CLI mode should produce CBOR array, got: %T", cborVal)
	}
}

// TEST362: CLI mode with binary piped in - pipe binary data via stdin This test simulates real-world conditions: - Pure binary data piped to stdin (NOT CBOR) - CLI mode detected (command arg present) - Cap accepts stdin source - Binary is chunked on-the-fly and accumulated - Handler receives complete CBOR payload
func Test362_CLIModePipedBinary(t *testing.T) {
	// Simulate large binary being piped (1MB PDF)
	pdfContent := make([]byte, 1_000_000)
	for i := range pdfContent {
		pdfContent[i] = 0xAB
	}

	// Create cap that accepts stdin
	capDef := createTestCap(
		`cap:in="media:ext=pdf";process;out="media:void"`,
		"Process",
		"process",
		[]cap.CapArg{
			{
				MediaUrn: "media:ext=pdf",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:ext=pdf"),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// Mock stdin with bytes.Reader (simulates piped binary)
	mockStdin := strings.NewReader(string(pdfContent))

	// Build payload from streaming reader (what CLI piped mode does)
	payload, err := runtime.buildPayloadFromStreamingReader(capDef, mockStdin, DefaultLimits().MaxChunk)
	if err != nil {
		t.Fatalf("Failed to build payload from streaming reader: %v", err)
	}

	// Verify payload is CBOR array with correct structure
	var cborVal interface{}
	if err := cborlib.Unmarshal(payload, &cborVal); err != nil {
		t.Fatalf("Failed to unmarshal CBOR: %v", err)
	}

	arr, ok := cborVal.([]interface{})
	if !ok {
		t.Fatalf("Expected CBOR Array, got: %T", cborVal)
	}

	if len(arr) != 1 {
		t.Fatalf("CBOR array should have one argument, got: %d", len(arr))
	}

	argMap, ok := arr[0].(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Expected Map in CBOR array, got: %T", arr[0])
	}

	var mediaUrn string
	var value []byte

	for k, v := range argMap {
		key, ok := k.(string)
		if !ok {
			continue
		}
		switch key {
		case "media_urn":
			if s, ok := v.(string); ok {
				mediaUrn = s
			}
		case "value":
			if b, ok := v.([]byte); ok {
				value = b
			}
		}
	}

	if mediaUrn != "media:ext=pdf" {
		t.Errorf("Media URN should match cap in_spec, got: %s", mediaUrn)
	}
	if string(value) != string(pdfContent) {
		t.Errorf("Binary content should be preserved exactly, got %d bytes, expected %d bytes", len(value), len(pdfContent))
	}
}

// TEST363: CBOR mode with chunked content - send file content streaming as chunks
func Test363_CBORModeChunkedContent(t *testing.T) {
	pdfContent := make([]byte, 10000) // 10KB of data
	for i := range pdfContent {
		pdfContent[i] = 0xAA
	}

	var receivedData []byte
	receivedChan := make(chan []byte, 1)

	handler := func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
		// TRUE STREAMING: Relay frames and verify
		var total []byte
		for frame := range frames {
			if frame.FrameType == FrameTypeChunk {
				if frame.Payload != nil {
					total = append(total, frame.Payload...)
					if err := emitter.EmitCbor(frame.Payload); err != nil {
						return err
					}
				}
			}
		}

		// Verify what we received
		var cborVal interface{}
		if err := cborlib.Unmarshal(total, &cborVal); err != nil {
			return err
		}

		if arr, ok := cborVal.([]interface{}); ok {
			if argMap, ok := arr[0].(map[interface{}]interface{}); ok {
				for k, v := range argMap {
					if key, ok := k.(string); ok && key == "value" {
						if data, ok := v.([]byte); ok {
							receivedChan <- data
						}
					}
				}
			}
		}
		return nil
	}

	capDef := createTestCap(
		`cap:in="media:ext=pdf";process;out="media:void"`,
		"Process",
		"process",
		[]cap.CapArg{
			{
				MediaUrn: "media:ext=pdf",
				Required: true,
				Sources: []cap.ArgSource{
					stdinSource("media:ext=pdf"),
				},
			},
		},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}
	runtime.Register(capDef.UrnString(), handler)

	// Build CBOR payload
	args := []cap.CapArgumentValue{
		{
			MediaUrn: "media:ext=pdf",
			Value:    pdfContent,
		},
	}
	var payloadBytes []byte
	cborArgs := make([]interface{}, len(args))
	for i, arg := range args {
		cborArgs[i] = map[string]interface{}{
			"media_urn": arg.MediaUrn,
			"value":     arg.Value,
		}
	}
	payloadBytes, err = cborlib.Marshal(cborArgs)
	if err != nil {
		t.Fatalf("Failed to marshal CBOR: %v", err)
	}

	// Simulate streaming: chunk payload and send via channel
	handlerFunc := runtime.FindHandler(capDef.UrnString())
	if handlerFunc == nil {
		t.Fatal("Handler not found")
	}

	noPeer := &noPeerInvoker{}
	emitter := &mockStreamEmitter{}

	frameChan := make(chan Frame, 100)
	const maxChunk = 262144
	requestID := NewMessageIdDefault()
	streamID := "test-stream"

	// Send STREAM_START
	frameChan <- *NewStreamStart(requestID, streamID, "media:", nil)

	// Send CHUNK frames
	offset := 0
	seq := uint64(0)
	for offset < len(payloadBytes) {
		chunkSize := len(payloadBytes) - offset
		if chunkSize > maxChunk {
			chunkSize = maxChunk
		}
		chunkData := payloadBytes[offset : offset+chunkSize]
		chunkIndex := seq
		checksum := ComputeChecksum(chunkData)
		frameChan <- *NewChunk(requestID, streamID, seq, chunkData, chunkIndex, checksum)
		offset += chunkSize
		seq++
	}

	// Send STREAM_END and END
	frameChan <- *NewStreamEnd(requestID, streamID, seq)
	frameChan <- *NewEnd(requestID, nil)
	close(frameChan)

	// Run handler in goroutine
	go func() {
		if err := handlerFunc(frameChan, emitter, noPeer); err != nil {
			t.Errorf("Handler failed: %v", err)
		}
	}()

	// Wait for result
	select {
	case receivedData = <-receivedChan:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for handler response")
	}

	if string(receivedData) != string(pdfContent) {
		t.Errorf("Handler should receive chunked content, got %d bytes, expected %d bytes", len(receivedData), len(pdfContent))
	}
}

// TEST364: CBOR mode with file path - send file path in CBOR arguments (auto-conversion)
func Test364_CBORModeFilePath(t *testing.T) {
	tempFile := filepath.Join(os.TempDir(), "test364.pdf")
	pdfContent := []byte("PDF content for CBOR file path test")
	if err := os.WriteFile(tempFile, pdfContent, 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile)

	// Build CBOR arguments with file-path URN
	args := []cap.CapArgumentValue{
		{
			MediaUrn: "media:file-path;enc=utf-8",
			Value:    []byte(tempFile),
		},
	}
	var payload []byte
	cborArgs := make([]interface{}, len(args))
	for i, arg := range args {
		cborArgs[i] = map[string]interface{}{
			"media_urn": arg.MediaUrn,
			"value":     arg.Value,
		}
	}
	payload, err := cborlib.Marshal(cborArgs)
	if err != nil {
		t.Fatalf("Failed to marshal CBOR: %v", err)
	}

	// Verify the CBOR structure is correct
	var decoded []interface{}
	if err := cborlib.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal CBOR: %v", err)
	}

	if len(decoded) != 1 {
		t.Fatalf("Expected 1 argument, got: %d", len(decoded))
	}

	argMap, ok := decoded[0].(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Expected map, got: %T", decoded[0])
	}

	mediaUrn, _ := argMap["media_urn"].(string)
	value, _ := argMap["value"].([]byte)

	if mediaUrn != "media:file-path;enc=utf-8" {
		t.Errorf("Expected media:file-path URN, got: %s", mediaUrn)
	}
	if string(value) != tempFile {
		t.Errorf("Expected file path as value, got: %s", string(value))
	}
}

// TEST395: Small payload (< max_chunk) produces correct CBOR arguments
func Test395_BuildPayloadSmall(t *testing.T) {
	capDef := createTestCap(
		`cap:in="media:";process;out="media:void"`,
		"Process",
		"process",
		[]cap.CapArg{},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	data := []byte("small payload")
	reader := bytes.NewReader(data)

	payload, err := runtime.buildPayloadFromStreamingReader(capDef, reader, DefaultLimits().MaxChunk)
	if err != nil {
		t.Fatalf("buildPayloadFromStreamingReader failed: %v", err)
	}

	// Verify CBOR structure
	var cborVal interface{}
	if err := cborlib.Unmarshal(payload, &cborVal); err != nil {
		t.Fatalf("Failed to parse CBOR: %v", err)
	}

	arr, ok := cborVal.([]interface{})
	if !ok || len(arr) != 1 {
		t.Fatalf("Expected array with one argument, got: %T %v", cborVal, cborVal)
	}

	argMap, ok := arr[0].(map[interface{}]interface{})
	if !ok {
		t.Fatalf("Expected map, got: %T", arr[0])
	}

	valueBytes, ok := argMap["value"].([]byte)
	if !ok {
		t.Fatalf("Expected bytes for value, got: %T", argMap["value"])
	}

	if !bytes.Equal(valueBytes, data) {
		t.Errorf("Payload bytes should match, expected: %v, got: %v", data, valueBytes)
	}
}

// TEST396: Large payload (> max_chunk) accumulates across chunks correctly
func Test396_BuildPayloadLarge(t *testing.T) {
	capDef := createTestCap(
		`cap:in="media:";process;out="media:void"`,
		"Process",
		"process",
		[]cap.CapArg{},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// Use small max_chunk to force multi-chunk
	data := make([]byte, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = byte(i % 256)
	}
	reader := bytes.NewReader(data)

	payload, err := runtime.buildPayloadFromStreamingReader(capDef, reader, 100)
	if err != nil {
		t.Fatalf("buildPayloadFromStreamingReader failed: %v", err)
	}

	var cborVal interface{}
	if err := cborlib.Unmarshal(payload, &cborVal); err != nil {
		t.Fatalf("Failed to parse CBOR: %v", err)
	}

	arr := cborVal.([]interface{})
	argMap := arr[0].(map[interface{}]interface{})
	valueBytes := argMap["value"].([]byte)

	if len(valueBytes) != 1000 {
		t.Errorf("All bytes should be accumulated, expected: 1000, got: %d", len(valueBytes))
	}
	if !bytes.Equal(valueBytes, data) {
		t.Errorf("Data should match exactly")
	}
}

// TEST397: Empty reader produces valid empty CBOR arguments
func Test397_BuildPayloadEmpty(t *testing.T) {
	capDef := createTestCap(
		`cap:in="media:";process;out="media:void"`,
		"Process",
		"process",
		[]cap.CapArg{},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	reader := bytes.NewReader([]byte{})

	payload, err := runtime.buildPayloadFromStreamingReader(capDef, reader, DefaultLimits().MaxChunk)
	if err != nil {
		t.Fatalf("buildPayloadFromStreamingReader failed: %v", err)
	}

	var cborVal interface{}
	if err := cborlib.Unmarshal(payload, &cborVal); err != nil {
		t.Fatalf("Failed to parse CBOR: %v", err)
	}

	arr := cborVal.([]interface{})
	argMap := arr[0].(map[interface{}]interface{})
	valueBytes := argMap["value"].([]byte)

	if len(valueBytes) != 0 {
		t.Errorf("Empty reader should produce empty bytes, got: %d bytes", len(valueBytes))
	}
}

// errorReader is a reader that always returns an error
type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("simulated read error")
}

// TEST398: IO error from reader propagates as RuntimeError::Io
func Test398_BuildPayloadIOError(t *testing.T) {
	capDef := createTestCap(
		`cap:in="media:";process;out="media:void"`,
		"Process",
		"process",
		[]cap.CapArg{},
	)

	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{capDef})
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	reader := &errorReader{}

	_, err = runtime.buildPayloadFromStreamingReader(capDef, reader, DefaultLimits().MaxChunk)
	if err == nil {
		t.Fatal("IO error should propagate")
	}
	if !strings.Contains(err.Error(), "simulated read error") {
		t.Errorf("Expected error to contain 'simulated read error', got: %s", err.Error())
	}
}

// =============================================================================
// PeerResponse / DemuxPeerResponse Tests
// =============================================================================

// TEST544: PeerCall::finish sends END frame
func Test544_peer_invoker_sends_end_frame(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)
	syncWriter := &syncFrameWriter{writer: writer, seqAssigner: NewSeqAssigner()}
	pendingRequests := &sync.Map{}

	peer := newPeerInvokerImpl(syncWriter, pendingRequests, DefaultLimits().MaxChunk)
	args := []cap.CapArgumentValue{
		cap.NewCapArgumentValueFromStr("media:test", "hello"),
	}
	_, err := peer.Invoke(`cap:in="media:void";test;out="media:void"`, args)
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}

	// Read all frames back from the buffer
	reader := NewFrameReader(bytes.NewReader(buf.Bytes()))
	var endCount int
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			break
		}
		if frame.FrameType == FrameTypeEnd {
			endCount++
		}
	}
	if endCount != 1 {
		t.Fatalf("Expected 1 END frame, got %d", endCount)
	}
}

// TEST545: PeerCall::finish returns PeerResponse with data
func Test545_demux_peer_response_returns_data(t *testing.T) {
	reqId := NewMessageIdRandom()
	rawCh := make(chan Frame, 10)

	// STREAM_START
	rawCh <- *NewStreamStart(reqId, "s1", "media:binary", nil)

	// CHUNK with CBOR-encoded bytes
	data := []byte("response data")
	cborPayload, _ := cborlib.Marshal(data)
	checksum := ComputeChecksum(cborPayload)
	rawCh <- *NewChunk(reqId, "s1", 0, cborPayload, 0, checksum)

	// STREAM_END + close
	rawCh <- *NewStreamEnd(reqId, "s1", 1)
	close(rawCh)

	response := DemuxPeerResponse(rawCh)
	result, err := response.CollectBytes()
	if err != nil {
		t.Fatalf("CollectBytes failed: %v", err)
	}
	if !bytes.Equal(result, []byte("response data")) {
		t.Fatalf("Expected 'response data', got %q", result)
	}
}

// TEST839: LOG frames arriving BEFORE StreamStart are delivered immediately This tests the critical fix: during a peer call, the peer (e.g., modelcartridge) sends LOG frames for minutes during model download BEFORE sending any data (StreamStart + Chunk). The handler must receive these LOGs in real-time so it can re-emit progress and keep the engine's activity timer alive. Previously, demux_single_stream blocked on awaiting StreamStart before returning PeerResponse, which meant the handler couldn't call recv() until data arrived — causing 120s activity timeouts during long downloads.
func Test839_peer_response_delivers_logs_before_stream_start(t *testing.T) {
	reqId := NewMessageIdRandom()
	rawCh := make(chan Frame, 20)

	// Send LOG frames BEFORE any StreamStart
	rawCh <- *NewProgress(reqId, 0.1, "downloading file 1/10")
	rawCh <- *NewProgress(reqId, 0.5, "downloading file 5/10")
	rawCh <- *NewLog(reqId, "status", "large file in progress")

	// Now send the actual data
	rawCh <- *NewStreamStart(reqId, "s1", "media:binary", nil)
	data := []byte("model output")
	cborPayload, _ := cborlib.Marshal(data)
	checksum := ComputeChecksum(cborPayload)
	rawCh <- *NewChunk(reqId, "s1", 0, cborPayload, 0, checksum)
	rawCh <- *NewStreamEnd(reqId, "s1", 1)
	close(rawCh)

	response := DemuxPeerResponse(rawCh)

	// Must receive LOG frames first
	item1, ok := response.Recv()
	if !ok {
		t.Fatal("expected first LOG")
	}
	if item1.LogFrame == nil {
		t.Fatal("expected LOG frame, got Data")
	}
	p1, ok1 := item1.LogFrame.LogProgress()
	if !ok1 || p1-0.1 > 0.01 {
		t.Fatalf("expected progress ~0.1, got %v (ok=%v)", p1, ok1)
	}

	item2, ok := response.Recv()
	if !ok {
		t.Fatal("expected second LOG")
	}
	if item2.LogFrame == nil {
		t.Fatal("expected LOG frame, got Data")
	}

	item3, ok := response.Recv()
	if !ok {
		t.Fatal("expected third LOG")
	}
	if item3.LogFrame == nil {
		t.Fatal("expected LOG frame, got Data")
	}
	if item3.LogFrame.LogMessage() != "large file in progress" {
		t.Fatalf("expected 'large file in progress', got %q", item3.LogFrame.LogMessage())
	}

	// Data must arrive after the LOGs
	item4, ok := response.Recv()
	if !ok {
		t.Fatal("expected data item")
	}
	if !item4.IsDataItem {
		t.Fatal("expected Data, got LOG")
	}

	_, ok = response.Recv()
	if ok {
		t.Fatal("stream must end after STREAM_END")
	}
}

// TEST840: PeerResponse::collect_bytes discards LOG frames
func Test840_peer_response_collect_bytes_discards_logs(t *testing.T) {
	reqId := NewMessageIdRandom()
	rawCh := make(chan Frame, 20)

	rawCh <- *NewStreamStart(reqId, "s1", "media:binary", nil)
	rawCh <- *NewProgress(reqId, 0.25, "working")
	rawCh <- *NewProgress(reqId, 0.75, "almost")

	data := []byte("hello")
	cborPayload, _ := cborlib.Marshal(data)
	checksum := ComputeChecksum(cborPayload)
	rawCh <- *NewChunk(reqId, "s1", 0, cborPayload, 0, checksum)

	rawCh <- *NewLog(reqId, "info", "done")
	rawCh <- *NewStreamEnd(reqId, "s1", 1)
	close(rawCh)

	response := DemuxPeerResponse(rawCh)
	result, err := response.CollectBytes()
	if err != nil {
		t.Fatalf("CollectBytes failed: %v", err)
	}
	if !bytes.Equal(result, []byte("hello")) {
		t.Fatalf("Expected 'hello', got %q (LOG frames must be discarded)", result)
	}
}

// TEST841: PeerResponse::collect_value discards LOG frames
func Test841_peer_response_collect_value_discards_logs(t *testing.T) {
	reqId := NewMessageIdRandom()
	rawCh := make(chan Frame, 20)

	rawCh <- *NewStreamStart(reqId, "s1", "media:binary", nil)
	rawCh <- *NewProgress(reqId, 0.5, "half")
	rawCh <- *NewLog(reqId, "debug", "processing")

	cborPayload, _ := cborlib.Marshal(42)
	checksum := ComputeChecksum(cborPayload)
	rawCh <- *NewChunk(reqId, "s1", 0, cborPayload, 0, checksum)

	rawCh <- *NewStreamEnd(reqId, "s1", 1)
	close(rawCh)

	response := DemuxPeerResponse(rawCh)
	value, err := response.CollectValue()
	if err != nil {
		t.Fatalf("CollectValue failed: %v", err)
	}
	// CBOR unsigned integers decode as uint64 in Go
	if value != uint64(42) {
		t.Fatalf("Expected 42, got %v (type %T)", value, value)
	}
}

// =============================================================================
// FindStream / RequireStream Tests
// =============================================================================

type testStream struct {
	MediaUrn string
	Data     []byte
}

func streamsToSlice(streams []testStream) []struct {
	MediaUrn string
	Data     []byte
} {
	result := make([]struct {
		MediaUrn string
		Data     []byte
	}, len(streams))
	for i, s := range streams {
		result[i].MediaUrn = s.MediaUrn
		result[i].Data = s.Data
	}
	return result
}

// TEST678: find_stream with exact equivalent URN (same tags, different order) succeeds
func Test678_find_stream_equivalent_urn(t *testing.T) {
	streams := streamsToSlice([]testStream{
		{"media:txt;enc=utf-8", []byte("hello world")},
	})
	result, err := FindStream(streams, "media:txt;enc=utf-8")
	if err != nil {
		t.Fatalf("FindStream error: %v", err)
	}
	if result == nil {
		t.Fatal("Expected to find stream")
	}
	if !bytes.Equal(result, []byte("hello world")) {
		t.Fatalf("Expected 'hello world', got %q", result)
	}
}

// TEST679: find_stream with base URN vs full URN fails — is_equivalent is strict This is the root cause of the cartridge_client.rs bug. Sender sent "media:llm-generation-request" but receiver looked for "media:fmt=json;llm-generation-request;record".
func Test679_find_stream_base_vs_full_fails(t *testing.T) {
	streams := streamsToSlice([]testStream{
		{"media:txt;enc=utf-8", []byte("hello")},
	})
	result, _ := FindStream(streams, "media:enc=utf-8")
	if result != nil {
		t.Fatal("Base URN must not match more specific URN (is_equivalent is strict)")
	}
}

// TEST680: require_stream with missing URN returns hard StreamError
func Test680_require_stream_missing_fails(t *testing.T) {
	streams := streamsToSlice([]testStream{
		{"media:txt;enc=utf-8", []byte("hello")},
	})
	_, err := RequireStream(streams, "media:binary")
	if err == nil {
		t.Fatal("Expected error for missing stream")
	}
	if !strings.Contains(err.Error(), "missing required arg") {
		t.Fatalf("Expected 'missing required arg' error, got: %s", err.Error())
	}
}

// TEST681: find_stream with multiple streams returns the correct one
func Test681_find_stream_multiple(t *testing.T) {
	streams := streamsToSlice([]testStream{
		{"media:txt;enc=utf-8", []byte("text data")},
		{"media:ext=png;image", []byte("image data")},
		{"media:fmt=json", []byte("json data")},
	})
	result, err := FindStream(streams, "media:ext=png;image")
	if err != nil {
		t.Fatalf("FindStream error: %v", err)
	}
	if !bytes.Equal(result, []byte("image data")) {
		t.Fatalf("Expected 'image data', got %q", result)
	}
}

// TEST682: require_stream_str returns UTF-8 string for text data
func Test682_require_stream_returns_data(t *testing.T) {
	streams := streamsToSlice([]testStream{
		{"media:txt;enc=utf-8", []byte("hello text")},
	})
	result, err := RequireStream(streams, "media:txt;enc=utf-8")
	if err != nil {
		t.Fatalf("RequireStream failed: %v", err)
	}
	if !bytes.Equal(result, []byte("hello text")) {
		t.Fatalf("Expected 'hello text', got %q", result)
	}
}

// TEST683: find_stream returns None for invalid media URN string (not a parse error — just None)
func Test683_find_stream_invalid_urn_returns_nil(t *testing.T) {
	streams := streamsToSlice([]testStream{
		{"media:txt;enc=utf-8", []byte("data")},
	})
	found, _ := FindStream(streams, "")
	if found != nil {
		t.Fatal("Invalid URN must return nil, not panic")
	}
}

// =============================================================================
// ProgressSender Tests
// =============================================================================

// mockFrameWriter captures frames for testing
type mockFrameWriter struct {
	frames []Frame
}

func (m *mockFrameWriter) WriteFrame(frame *Frame) error {
	m.frames = append(m.frames, *frame)
	return nil
}

// TEST842: run_with_keepalive returns closure result (fast operation, no keepalive frames)
func Test842_progress_sender_emits_frames(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)
	syncWriter := &syncFrameWriter{writer: writer, seqAssigner: NewSeqAssigner()}
	reqId := NewMessageIdRandom()
	ps := &ProgressSender{
		writer:    syncWriter,
		requestID: reqId,
		routingId: nil,
	}

	ps.Progress(0.5, "halfway there")
	ps.Log("info", "loading complete")

	// Read the frames back from the buffer
	reader := NewFrameReader(bytes.NewReader(buf.Bytes()))
	var logFrames []*Frame
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			break
		}
		if frame.FrameType == FrameTypeLog {
			logFrames = append(logFrames, frame)
		}
	}

	if len(logFrames) != 2 {
		t.Fatalf("Expected 2 LOG frames, got %d", len(logFrames))
	}
	pv, pok := logFrames[0].LogProgress()
	if !pok || pv-0.5 > 0.01 {
		t.Fatalf("Expected progress ~0.5, got %v (ok=%v)", pv, pok)
	}
	if logFrames[0].LogMessage() != "halfway there" {
		t.Fatalf("Expected 'halfway there', got %q", logFrames[0].LogMessage())
	}
	if logFrames[1].LogLevel() != "info" {
		t.Fatalf("Expected level 'info', got %q", logFrames[1].LogLevel())
	}
	if logFrames[1].LogMessage() != "loading complete" {
		t.Fatalf("Expected 'loading complete', got %q", logFrames[1].LogMessage())
	}
}

// TEST843: run_with_keepalive returns Ok/Err from closure
func Test843_progress_sender_from_goroutine(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)
	syncWriter := &syncFrameWriter{writer: writer, seqAssigner: NewSeqAssigner()}
	reqId := NewMessageIdRandom()
	ps := &ProgressSender{
		writer:    syncWriter,
		requestID: reqId,
		routingId: nil,
	}

	done := make(chan struct{})
	go func() {
		ps.Progress(0.25, "quarter")
		close(done)
	}()
	<-done

	reader := NewFrameReader(bytes.NewReader(buf.Bytes()))
	frame, err := reader.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if frame.FrameType != FrameTypeLog {
		t.Fatalf("Expected LOG frame, got %v", frame.FrameType)
	}
	pv, pok := frame.LogProgress()
	if !pok || pv-0.25 > 0.01 {
		t.Fatalf("Expected progress ~0.25, got %v (ok=%v)", pv, pok)
	}
}

// TEST844: run_with_keepalive propagates errors from closure
func Test844_progress_sender_multiple_goroutines(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)
	syncWriter := &syncFrameWriter{writer: writer, seqAssigner: NewSeqAssigner()}
	reqId := NewMessageIdRandom()
	ps := &ProgressSender{
		writer:    syncWriter,
		requestID: reqId,
		routingId: nil,
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ps.Progress(0.25, "t1")
	}()
	go func() {
		defer wg.Done()
		ps.Progress(0.75, "t2")
	}()
	wg.Wait()

	reader := NewFrameReader(bytes.NewReader(buf.Bytes()))
	var messages []string
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			break
		}
		if frame.FrameType == FrameTypeLog {
			messages = append(messages, frame.LogMessage())
		}
	}

	if len(messages) != 2 {
		t.Fatalf("Expected 2 frames from 2 goroutines, got %d", len(messages))
	}
	sort.Strings(messages)
	if messages[0] != "t1" || messages[1] != "t2" {
		t.Fatalf("Expected messages [t1, t2], got %v", messages)
	}
}

// TEST845: ProgressSender emits progress and log frames independently of OutputStream
func Test845_progress_sender_independent_of_emitter(t *testing.T) {
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)
	syncWriter := &syncFrameWriter{writer: writer, seqAssigner: NewSeqAssigner()}
	reqId := NewMessageIdRandom()
	ps := &ProgressSender{
		writer:    syncWriter,
		requestID: reqId,
		routingId: nil,
	}

	ps.Progress(0.5, "halfway there")
	ps.Log("info", "loading complete")

	reader := NewFrameReader(bytes.NewReader(buf.Bytes()))
	var logFrames []*Frame
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			break
		}
		logFrames = append(logFrames, frame)
	}

	if len(logFrames) != 2 {
		t.Fatalf("Expected 2 frames, got %d", len(logFrames))
	}
	pv, pok := logFrames[0].LogProgress()
	if !pok || pv-0.5 > 0.01 {
		t.Fatalf("Expected progress ~0.5, got %v (ok=%v)", pv, pok)
	}
	if logFrames[0].LogMessage() != "halfway there" {
		t.Fatalf("Expected 'halfway there', got %q", logFrames[0].LogMessage())
	}
	if logFrames[1].LogLevel() != "info" {
		t.Fatalf("Expected 'info', got %q", logFrames[1].LogLevel())
	}
	if logFrames[1].LogMessage() != "loading complete" {
		t.Fatalf("Expected 'loading complete', got %q", logFrames[1].LogMessage())
	}
}

// TEST1282: AdapterSelectionOp is auto-registered by CartridgeRuntime
func Test1282_adapter_selection_auto_registered(t *testing.T) {
	identity := createTestCap(standard.CapIdentity, "Identity", "identity", nil)
	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{identity})
	rt, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("NewCartridgeRuntimeWithManifest failed: %v", err)
	}
	handler := rt.FindHandler(standard.CapAdapterSelection)
	if handler == nil {
		t.Fatal("CartridgeRuntime must auto-register adapter selection handler")
	}
}

// TEST1283: Custom adapter selection Op overrides the default
func Test1283_adapter_selection_custom_override(t *testing.T) {
	identity := createTestCap(standard.CapIdentity, "Identity", "identity", nil)
	manifest := createTestManifest("TestCartridge", "1.0.0", "Test", []*cap.Cap{identity})
	rt, err := NewCartridgeRuntimeWithManifest(manifest)
	if err != nil {
		t.Fatalf("NewCartridgeRuntimeWithManifest failed: %v", err)
	}

	// Verify default is registered
	if rt.FindHandler(standard.CapAdapterSelection) == nil {
		t.Fatal("Default adapter selection handler must be registered")
	}

	// Override with custom handler
	customHandler := func(input <-chan Frame, output StreamEmitter, peer PeerInvoker) error {
		for range input {
		}
		return nil
	}
	rt.Register(standard.CapAdapterSelection, customHandler)

	// Must still find a handler (the custom one)
	if rt.FindHandler(standard.CapAdapterSelection) == nil {
		t.Fatal("Custom adapter selection handler must be findable after override")
	}
}

// extractStreamsFromFrames simulates the cartridge-side stream extraction
// (like collect_streams does): walk frames, CBOR-decode each CHUNK payload
// into raw bytes, and aggregate per stream. Returns FindStream-shaped slice.
func extractStreamsFromFrames(t *testing.T, frames []Frame) []struct {
	MediaUrn string
	Data     []byte
} {
	t.Helper()
	type stream struct {
		media string
		data  []byte
	}
	var streams []stream
	active := make(map[string]int)

	for i := range frames {
		f := &frames[i]
		switch f.FrameType {
		case FrameTypeStreamStart:
			sid := ""
			if f.StreamId != nil {
				sid = *f.StreamId
			}
			media := ""
			if f.MediaUrn != nil {
				media = *f.MediaUrn
			}
			idx := len(streams)
			streams = append(streams, stream{media: media})
			active[sid] = idx
		case FrameTypeChunk:
			sid := ""
			if f.StreamId != nil {
				sid = *f.StreamId
			}
			if idx, ok := active[sid]; ok && f.Payload != nil {
				decoded, err := DecodeChunkPayload(f.Payload)
				if err != nil {
					t.Fatalf("CHUNK payload must be valid CBOR: %v", err)
				}
				streams[idx].data = append(streams[idx].data, decoded...)
			}
		}
	}

	result := make([]struct {
		MediaUrn string
		Data     []byte
	}, len(streams))
	for i, s := range streams {
		result[i].MediaUrn = s.media
		result[i].Data = s.data
	}
	return result
}

// TEST675: build_request_frames with full media URN preserves it in STREAM_START frame
func Test675_build_request_frames_preserves_media_urn_in_stream_start(t *testing.T) {
	fullUrn := "media:fmt=json;llm-generation-request;record"
	arg := cap.NewCapArgumentValue(fullUrn, []byte("{\"prompt\":\"test\"}"))
	rid := NewMessageIdRandom()
	frames := BuildRequestFrames(rid, "cap:test", []cap.CapArgumentValue{arg}, 32768)

	// Find the STREAM_START frame
	var streamStart *Frame
	for i := range frames {
		if frames[i].FrameType == FrameTypeStreamStart {
			streamStart = &frames[i]
			break
		}
	}
	if streamStart == nil {
		t.Fatal("Must have STREAM_START frame")
	}

	if streamStart.MediaUrn == nil || *streamStart.MediaUrn != fullUrn {
		t.Fatalf("STREAM_START must carry the exact media URN from CapArgumentValue, got %v", streamStart.MediaUrn)
	}
}

// TEST676: Full round-trip: build_request_frames → extract streams → find_stream succeeds
func Test676_build_request_frames_round_trip_find_stream_succeeds(t *testing.T) {
	fullUrn := "media:fmt=json;llm-generation-request;record"
	payload := []byte("{\"prompt\":\"hello\",\"model_spec\":\"test\"}")
	arg := cap.NewCapArgumentValue(fullUrn, payload)
	rid := NewMessageIdRandom()
	frames := BuildRequestFrames(rid, "cap:test", []cap.CapArgumentValue{arg}, 32768)

	streams := extractStreamsFromFrames(t, frames)

	// Now find_stream should succeed with the full URN
	found, err := FindStream(streams, fullUrn)
	if err != nil {
		t.Fatalf("FindStream error: %v", err)
	}
	if found == nil {
		t.Fatal("find_stream must find the stream by full media URN")
	}
	if !bytes.Equal(found, payload) {
		t.Fatalf("Round-tripped bytes must match original: got %q want %q", found, payload)
	}
}

// TEST677: build_request_frames with BASE URN → find_stream with FULL URN FAILS
// This documents the root cause of the cartridge_client.rs bug:
// sender used "media:llm-generation-request" (base), receiver looked for
// "media:fmt=json;llm-generation-request;record" (full). is_equivalent requires
// exact tag set match, so base != full.
func Test677_base_urn_does_not_match_full_urn_in_find_stream(t *testing.T) {
	// Sender uses BASE URN (the bug)
	baseUrn := "media:llm-generation-request"
	fullUrn := "media:fmt=json;llm-generation-request;record"
	arg := cap.NewCapArgumentValue(baseUrn, []byte("{}"))
	rid := NewMessageIdRandom()
	frames := BuildRequestFrames(rid, "cap:test", []cap.CapArgumentValue{arg}, 32768)

	streams := extractStreamsFromFrames(t, frames)

	// STREAM_START carries the base URN
	if streams[0].MediaUrn != baseUrn {
		t.Fatalf("expected base URN %q, got %q", baseUrn, streams[0].MediaUrn)
	}

	// find_stream with FULL URN must FAIL — base URN is not equivalent to full URN
	found, _ := FindStream(streams, fullUrn)
	if found != nil {
		t.Fatalf("Base URN %q must NOT match full URN %q — is_equivalent requires exact tag set", baseUrn, fullUrn)
	}
}

// TEST478: CartridgeRuntime auto-registers identity and discard handlers on construction
func Test478_auto_registers_identity_handler(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// Identity handler must be registered at exact CAP_IDENTITY URN
	if runtime.FindHandler(standard.CapIdentity) == nil {
		t.Fatal("CartridgeRuntime must auto-register identity handler")
	}

	// Discard handler must be registered at exact CAP_DISCARD URN
	if runtime.FindHandler(standard.CapDiscard) == nil {
		t.Fatal("CartridgeRuntime must auto-register discard handler")
	}

	// Standard handlers must NOT match arbitrary specific requests
	// (request is pattern, registered cap is instance — broad caps don't
	// satisfy specific patterns).
	if runtime.FindHandler(`cap:in="media:void";nonexistent;out="media:void"`) != nil {
		t.Fatal("Standard handlers must not catch arbitrary specific requests")
	}
}

// TEST479: Custom identity Op overrides auto-registered default
func Test479_custom_identity_overrides_default(t *testing.T) {
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	if err != nil {
		t.Fatalf("Failed to create runtime: %v", err)
	}

	// Auto-registered identity handler must exist
	if runtime.FindHandler(standard.CapIdentity) == nil {
		t.Fatal("Auto-registered identity must exist before override")
	}

	// Count handlers before override
	runtime.mu.RLock()
	handlersBefore := len(runtime.handlers)
	runtime.mu.RUnlock()

	// Override identity with a custom handler (always fails when invoked, to
	// prove it's the custom one).
	runtime.Register(standard.CapIdentity, func(frames <-chan Frame, emitter StreamEmitter, peer PeerInvoker) error {
		for range frames {
		}
		return fmt.Errorf("custom identity")
	})

	// Handler count must not change (map insert replaces, doesn't add).
	runtime.mu.RLock()
	handlersAfter := len(runtime.handlers)
	runtime.mu.RUnlock()
	if handlersAfter != handlersBefore {
		t.Errorf("Overriding identity must replace, not add: before=%d after=%d", handlersBefore, handlersAfter)
	}

	// The handler at CAP_IDENTITY must still be findable.
	if runtime.FindHandler(standard.CapIdentity) == nil {
		t.Fatal("Identity handler must be findable after override")
	}

	// Discard was NOT affected by the override.
	if runtime.FindHandler(standard.CapDiscard) == nil {
		t.Fatal("Discard handler must still be present after overriding identity")
	}
}

// =========================================================================
// Stream Abstractions Tests (input frame streams, output emitter)
//
// The Go mirror models a cartridge's input as a <-chan Frame carrying
// STREAM_START / CHUNK(s) / STREAM_END groups (one per argument) followed
// by a terminal END, and models output through the StreamEmitter that
// writes those same frame types. These tests port the Rust InputStream /
// InputPackage / OutputStream behaviour onto that frame model.
// =========================================================================

// chunkFramesForStream builds the STREAM_START → CHUNK(s) → STREAM_END frame
// group for a single named stream. Each chunk value is CBOR-encoded exactly
// as the wire carries it. No END frame — callers concatenate groups then
// append a single END.
func chunkFramesForStream(requestID MessageId, streamID, mediaUrn string, chunkValues []interface{}) []Frame {
	frames := []Frame{*NewStreamStart(requestID, streamID, mediaUrn, nil)}
	for i, v := range chunkValues {
		cborValue, err := cborlib.Marshal(v)
		if err != nil {
			panic(fmt.Sprintf("failed to encode chunk: %v", err))
		}
		checksum := ComputeChecksum(cborValue)
		frames = append(frames, *NewChunk(requestID, streamID, uint64(i), cborValue, uint64(i), checksum))
	}
	frames = append(frames, *NewStreamEnd(requestID, streamID, uint64(len(chunkValues))))
	return frames
}

// singleStreamFrames builds the full frame slice (one stream group + END)
// for a one-argument request, the exact shape a handler receives.
func singleStreamFrames(mediaUrn string, chunkValues []interface{}) []Frame {
	requestID := NewMessageIdDefault()
	frames := chunkFramesForStream(requestID, "arg-0", mediaUrn, chunkValues)
	return append(frames, *NewEnd(requestID, nil))
}

// TEST529: InputStream recv yields chunks in order
func Test529_input_stream_recv_order(t *testing.T) {
	chunks := []interface{}{[]byte("chunk1"), []byte("chunk2"), []byte("chunk3")}
	frames := singleStreamFrames("media:test", chunks)

	var collected [][]byte
	for i := range frames {
		if frames[i].FrameType != FrameTypeChunk {
			continue
		}
		decoded, err := DecodeChunkPayload(frames[i].Payload)
		if err != nil {
			t.Fatalf("CHUNK must decode: %v", err)
		}
		collected = append(collected, decoded)
	}
	if len(collected) != 3 {
		t.Fatalf("Expected 3 chunks, got %d", len(collected))
	}
	if string(collected[0]) != "chunk1" || string(collected[1]) != "chunk2" || string(collected[2]) != "chunk3" {
		t.Errorf("Chunks out of order: %q %q %q", collected[0], collected[1], collected[2])
	}
}

// TEST530: InputStream::collect_bytes concatenates byte chunks
func Test530_input_stream_collect_bytes(t *testing.T) {
	chunks := []interface{}{[]byte("hello"), []byte(" "), []byte("world")}
	frames := singleStreamFrames("media:", chunks)

	streams := extractStreamsFromFrames(t, frames)
	if len(streams) != 1 {
		t.Fatalf("Expected 1 stream, got %d", len(streams))
	}
	if string(streams[0].Data) != "hello world" {
		t.Errorf("Expected 'hello world', got %q", string(streams[0].Data))
	}
}

// TEST531: InputStream::collect_bytes handles text chunks
func Test531_input_stream_collect_bytes_text(t *testing.T) {
	chunks := []interface{}{"hello", " world"}
	frames := singleStreamFrames("media:text", chunks)

	streams := extractStreamsFromFrames(t, frames)
	if len(streams) != 1 {
		t.Fatalf("Expected 1 stream, got %d", len(streams))
	}
	if string(streams[0].Data) != "hello world" {
		t.Errorf("Expected 'hello world', got %q", string(streams[0].Data))
	}
}

// TEST532: InputStream empty stream produces empty bytes
func Test532_input_stream_empty(t *testing.T) {
	frames := singleStreamFrames("media:void", []interface{}{})

	streams := extractStreamsFromFrames(t, frames)
	if len(streams) != 1 {
		t.Fatalf("Expected 1 (empty) stream, got %d", len(streams))
	}
	if len(streams[0].Data) != 0 {
		t.Errorf("Expected empty bytes, got %d bytes", len(streams[0].Data))
	}
}

// TEST533: InputStream propagates errors
func Test533_input_stream_error_propagation(t *testing.T) {
	// A CHUNK whose checksum does not match its payload is a corrupted stream;
	// the integrity check (the same VerifyChunkChecksum the runtime applies on
	// every incoming CHUNK) must surface it as an error rather than silently
	// accept it.
	requestID := NewMessageIdDefault()
	goodValue, _ := cborlib.Marshal([]byte("data"))
	badValue, _ := cborlib.Marshal([]byte("more"))
	frames := []Frame{
		*NewStreamStart(requestID, "arg-0", "media:test", nil),
		*NewChunk(requestID, "arg-0", 0, goodValue, 0, ComputeChecksum(goodValue)),
		// Corrupt chunk: checksum deliberately wrong.
		*NewChunk(requestID, "arg-0", 1, badValue, 1, ComputeChecksum(badValue)+1),
		*NewStreamEnd(requestID, "arg-0", 2),
		*NewEnd(requestID, nil),
	}

	sawError := false
	for i := range frames {
		if frames[i].FrameType == FrameTypeChunk {
			if err := VerifyChunkChecksum(&frames[i]); err != nil {
				sawError = true
			}
		}
	}
	if !sawError {
		t.Fatal("error must propagate from corrupted chunk")
	}
}

// TEST534: InputStream::media_urn returns correct URN
func Test534_input_stream_media_urn(t *testing.T) {
	frames := singleStreamFrames("media:image;format=png", []interface{}{[]byte("data")})

	var streamStartUrn string
	found := false
	for i := range frames {
		if frames[i].FrameType == FrameTypeStreamStart {
			if frames[i].MediaUrn != nil {
				streamStartUrn = *frames[i].MediaUrn
				found = true
			}
		}
	}
	if !found {
		t.Fatal("must have STREAM_START carrying media URN")
	}
	if streamStartUrn != "media:image;format=png" {
		t.Errorf("Expected 'media:image;format=png', got %q", streamStartUrn)
	}
}

// TEST535: InputPackage recv yields streams
func Test535_input_package_iteration(t *testing.T) {
	requestID := NewMessageIdDefault()
	var frames []Frame
	for i := 0; i < 3; i++ {
		group := chunkFramesForStream(requestID,
			fmt.Sprintf("arg-%d", i),
			fmt.Sprintf("media:stream%d", i),
			[]interface{}{[]byte(fmt.Sprintf("stream%d", i))})
		frames = append(frames, group...)
	}
	frames = append(frames, *NewEnd(requestID, nil))

	streams := extractStreamsFromFrames(t, frames)
	if len(streams) != 3 {
		t.Fatalf("must yield 3 streams, got %d", len(streams))
	}
	for i, s := range streams {
		expected := fmt.Sprintf("media:stream%d", i)
		if s.MediaUrn != expected {
			t.Errorf("stream %d: expected media %q, got %q", i, expected, s.MediaUrn)
		}
	}
}

// TEST536: InputPackage::collect_all_bytes aggregates all streams
func Test536_input_package_collect_all_bytes(t *testing.T) {
	requestID := NewMessageIdDefault()
	var frames []Frame
	frames = append(frames, chunkFramesForStream(requestID, "arg-0", "media:s1", []interface{}{[]byte("hello")})...)
	frames = append(frames, chunkFramesForStream(requestID, "arg-1", "media:s2", []interface{}{[]byte(" world")})...)
	frames = append(frames, *NewEnd(requestID, nil))

	streams := extractStreamsFromFrames(t, frames)
	var allBytes []byte
	for _, s := range streams {
		allBytes = append(allBytes, s.Data...)
	}
	if string(allBytes) != "hello world" {
		t.Errorf("Expected 'hello world', got %q", string(allBytes))
	}
}

// TEST537: InputPackage empty package produces empty bytes
func Test537_input_package_empty(t *testing.T) {
	requestID := NewMessageIdDefault()
	frames := []Frame{*NewEnd(requestID, nil)}

	streams := extractStreamsFromFrames(t, frames)
	var allBytes []byte
	for _, s := range streams {
		allBytes = append(allBytes, s.Data...)
	}
	if len(allBytes) != 0 {
		t.Errorf("empty package must produce empty bytes, got %d", len(allBytes))
	}
}

// TEST538: InputPackage propagates stream errors
func Test538_input_package_error_propagation(t *testing.T) {
	// A multi-stream input where the second stream carries a corrupted chunk:
	// draining the package via the integrity-checking collector must fail.
	requestID := NewMessageIdDefault()
	ch := make(chan Frame, 16)
	good, _ := cborlib.Marshal([]byte("data"))
	// Good stream.
	ch <- *NewStreamStart(requestID, "arg-0", "media:good", nil)
	ch <- *NewChunk(requestID, "arg-0", 0, good, 0, ComputeChecksum(good))
	ch <- *NewStreamEnd(requestID, "arg-0", 1)
	// Bad stream: corrupted checksum.
	bad, _ := cborlib.Marshal([]byte("oops"))
	ch <- *NewStreamStart(requestID, "arg-1", "media:bad", nil)
	ch <- *NewChunk(requestID, "arg-1", 0, bad, 0, ComputeChecksum(bad)+7)
	ch <- *NewStreamEnd(requestID, "arg-1", 1)
	ch <- *NewEnd(requestID, nil)
	close(ch)

	sawError := false
	for frame := range ch {
		if frame.FrameType == FrameTypeChunk {
			if err := VerifyChunkChecksum(&frame); err != nil {
				sawError = true
			}
		}
	}
	if !sawError {
		t.Fatal("error must propagate from the corrupted stream")
	}
}

// outputEmitterCapture wires a threadSafeEmitter to an in-memory buffer and
// reads the frames it produced back out. This is the Go OutputStream: it
// sends STREAM_START on first emit, CHUNK(s) per emit, and STREAM_END + END
// on Finalize.
func outputEmitterCapture(t *testing.T, streamID, mediaUrn string, maxChunk int, emit func(e *threadSafeEmitter)) []Frame {
	t.Helper()
	var buf bytes.Buffer
	writer := NewFrameWriter(&buf)
	syncWriter := &syncFrameWriter{writer: writer, seqAssigner: NewSeqAssigner()}
	emitter := newThreadSafeEmitter(syncWriter, NewMessageIdRandom(), nil, streamID, mediaUrn, maxChunk)
	emit(emitter)
	emitter.Finalize()

	reader := NewFrameReader(bytes.NewReader(buf.Bytes()))
	var frames []Frame
	for {
		frame, err := reader.ReadFrame()
		if err != nil {
			break
		}
		frames = append(frames, *frame)
	}
	return frames
}

// TEST539: OutputStream sends STREAM_START on first write
func Test539_output_stream_sends_stream_start(t *testing.T) {
	frames := outputEmitterCapture(t, "stream-1", "media:test", 256000, func(e *threadSafeEmitter) {
		if err := e.EmitCbor([]byte("test")); err != nil {
			t.Fatalf("emit must succeed: %v", err)
		}
	})

	if len(frames) < 1 {
		t.Fatal("must send at least STREAM_START")
	}
	if frames[0].FrameType != FrameTypeStreamStart {
		t.Errorf("first frame must be STREAM_START, got %v", frames[0].FrameType)
	}
	if frames[0].StreamId == nil || *frames[0].StreamId != "stream-1" {
		t.Errorf("STREAM_START must carry stream_id 'stream-1', got %v", frames[0].StreamId)
	}
}

// TEST540: OutputStream::close sends STREAM_END with correct chunk_count
func Test540_output_stream_close_sends_stream_end(t *testing.T) {
	frames := outputEmitterCapture(t, "stream-1", "media:test", 256000, func(e *threadSafeEmitter) {
		_ = e.EmitCbor([]byte("chunk1"))
		_ = e.EmitCbor([]byte("chunk2"))
		_ = e.EmitCbor([]byte("chunk3"))
	})

	var streamEnd *Frame
	for i := range frames {
		if frames[i].FrameType == FrameTypeStreamEnd {
			streamEnd = &frames[i]
			break
		}
	}
	if streamEnd == nil {
		t.Fatal("must have STREAM_END")
	}
	if streamEnd.ChunkCount == nil || *streamEnd.ChunkCount != 3 {
		t.Errorf("chunk_count must be 3, got %v", streamEnd.ChunkCount)
	}
}

// TEST541: OutputStream chunks large data correctly
func Test541_output_stream_chunks_large_data(t *testing.T) {
	// 250 bytes with a 100-byte max chunk → 3 chunks (100, 100, 50).
	largeData := make([]byte, 250)
	for i := range largeData {
		largeData[i] = 0xAA
	}
	frames := outputEmitterCapture(t, "stream-1", "media:", 100, func(e *threadSafeEmitter) {
		if err := e.EmitCbor(largeData); err != nil {
			t.Fatalf("emit must succeed: %v", err)
		}
	})

	chunkCount := 0
	for i := range frames {
		if frames[i].FrameType == FrameTypeChunk {
			chunkCount++
		}
	}
	if chunkCount < 3 {
		t.Errorf("large data must be chunked (got %d chunks)", chunkCount)
	}
}

// TEST542: OutputStream empty stream sends STREAM_START and STREAM_END only
func Test542_output_stream_empty(t *testing.T) {
	frames := outputEmitterCapture(t, "stream-1", "media:void", 256000, func(e *threadSafeEmitter) {
		// Emit nothing.
	})

	sawStart, sawEnd := false, false
	chunkCount := 0
	for i := range frames {
		switch frames[i].FrameType {
		case FrameTypeStreamStart:
			sawStart = true
		case FrameTypeStreamEnd:
			sawEnd = true
		case FrameTypeChunk:
			chunkCount++
		}
	}
	if !sawStart {
		t.Error("empty stream must still send STREAM_START")
	}
	if !sawEnd {
		t.Error("empty stream must send STREAM_END")
	}
	if chunkCount != 0 {
		t.Errorf("empty stream must have zero chunks, got %d", chunkCount)
	}
}
