package bifaci

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
)

// testManifestWithCaps builds a RelayNotify-shaped manifest JSON map
// from a flat list of cap-URN strings. The wire schema embeds caps
// inside `installed_cartridges[*].cap_groups`, so this helper wraps
// the list in a single synthetic installed-cartridge entry. Test code
// stays compact while exercising the production payload shape.
//
// An empty cap-urn list produces an empty `installed_cartridges`
// array, matching the "host has no cartridges that passed the
// attachment checklist" wire state.
// testManifestCounter assigns each testManifestWithCaps call a unique
// `id`. Aggregate-capability tests register multiple slaves and expect
// each to appear as a distinct installed cartridge; under the new
// dedup-by-(id, version) rule, identical ids would silently collapse
// into a single entry, so each manifest must carry its own.
var testManifestCounter int64

const (
	testCapIdentity = standard.CapIdentity
	testCapEcho     = "cap:echo"
)

func testManifestWithCaps(capURNs []string) map[string]interface{} {
	id := atomic.AddInt64(&testManifestCounter, 1)
	if len(capURNs) == 0 {
		return map[string]interface{}{
			"installed_cartridges": []interface{}{
				map[string]interface{}{
					"registry_url": nil,
					"channel":      "release",
					"id":           fmt.Sprintf("test-cartridge-%d", id),
					"version":      "0.0.0",
					"sha256":       "0000000000000000000000000000000000000000000000000000000000000000",
					"cap_groups":   []interface{}{},
				},
			},
		}
	}
	groupCaps := make([]map[string]interface{}, 0, len(capURNs))
	for _, urn := range capURNs {
		groupCaps = append(groupCaps, map[string]interface{}{
			"urn":     urn,
			"title":   "test",
			"command": "test",
			"args":    []interface{}{},
		})
	}
	return map[string]interface{}{
		"installed_cartridges": []interface{}{
			map[string]interface{}{
				"registry_url": nil,
				"channel":      "release",
				"id":           fmt.Sprintf("test-cartridge-%d", id),
				"version":      "0.0.0",
				"sha256":       "0000000000000000000000000000000000000000000000000000000000000000",
				"cap_groups": []interface{}{
					map[string]interface{}{
						"name":         "test",
						"caps":         groupCaps,
						"adapter_urns": []interface{}{},
					},
				},
			},
		},
	}
}

// serveRelayHandshake sends the initial RelayNotify carrying capURNs
// and, when that cap list is non-empty, answers the relay switch's
// end-to-end identity probe by echoing the nonce back verbatim. It
// mirrors Rust's `slave_notify_with_identity` test helper: the
// constructor (and Rust's add_master) run an identity verification
// round-trip whenever the master advertises at least one cap, so a
// slave that advertises caps MUST satisfy the probe before any further
// REQ/response traffic. An empty cap list skips the probe (no handler
// chain to test), matching the production gate.
//
// On any I/O error it reports via t.Errorf and returns false so the
// caller's goroutine can bail out cleanly.
func serveRelayHandshake(t *testing.T, reader *FrameReader, writer *FrameWriter, capURNs []string) bool {
	t.Helper()
	manifest := testManifestWithCaps(capURNs)
	manifestJSON, _ := json.Marshal(manifest)
	if err := SendNotify(writer, manifestJSON, DefaultLimits()); err != nil {
		t.Errorf("serveRelayHandshake: SendNotify: %v", err)
		return false
	}
	if len(capURNs) == 0 {
		// No caps advertised — production skips the identity probe.
		return true
	}

	// Read identity REQ.
	req, err := reader.ReadFrame()
	if err != nil || req == nil {
		t.Errorf("serveRelayHandshake: expected identity REQ: %v", err)
		return false
	}
	if req.FrameType != FrameTypeReq {
		t.Errorf("serveRelayHandshake: first frame after RelayNotify must be identity REQ, got %d", req.FrameType)
		return false
	}

	// Read request body: STREAM_START → CHUNK(s) → STREAM_END → END.
	var payload []byte
	for {
		f, err := reader.ReadFrame()
		if err != nil || f == nil {
			t.Errorf("serveRelayHandshake: expected frame during identity request: %v", err)
			return false
		}
		if f.FrameType == FrameTypeStreamStart || f.FrameType == FrameTypeStreamEnd {
			continue
		}
		if f.FrameType == FrameTypeChunk {
			payload = append(payload, f.Payload...)
			continue
		}
		if f.FrameType == FrameTypeEnd {
			break
		}
		t.Errorf("serveRelayHandshake: unexpected frame type during identity request: %d", f.FrameType)
		return false
	}

	// Echo response: STREAM_START → CHUNK → STREAM_END → END. The
	// payload is echoed verbatim — VerifyIdentity CBOR-decodes each
	// chunk back to the nonce, so the slave must return exactly the
	// bytes it received.
	streamId := "identity-echo"
	if err := writer.WriteFrame(NewStreamStart(req.Id, streamId, "media:", nil)); err != nil {
		t.Errorf("serveRelayHandshake: write STREAM_START: %v", err)
		return false
	}
	checksum := ComputeChecksum(payload)
	if err := writer.WriteFrame(NewChunk(req.Id, streamId, 0, payload, 0, checksum)); err != nil {
		t.Errorf("serveRelayHandshake: write CHUNK: %v", err)
		return false
	}
	if err := writer.WriteFrame(NewStreamEnd(req.Id, streamId, 1)); err != nil {
		t.Errorf("serveRelayHandshake: write STREAM_END: %v", err)
		return false
	}
	if err := writer.WriteFrame(NewEnd(req.Id, nil)); err != nil {
		t.Errorf("serveRelayHandshake: write END: %v", err)
		return false
	}
	return true
}

// TEST426: Single master REQ/response routing
func Test426_relay_switch_single_master_req_response(t *testing.T) {
	// Create socket pairs
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	// Spawn mock slave - no sync needed, NewRelaySwitch reads the notify
	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)

		if !serveRelayHandshake(t, reader, writer, []string{testCapEcho}) {
			return
		}

		// Read REQ and send response
		frame, err := reader.ReadFrame()
		if err != nil || frame == nil {
			return
		}
		if frame.FrameType == FrameTypeReq {
			response := NewEnd(frame.Id, []byte{42})
			writer.WriteFrame(response)
		}
	}()

	// Create RelaySwitch - this reads the RelayNotify from the goroutine
	sw, err := NewRelaySwitch([]SocketPair{{ID: "test-master-0", Read: engineRead, Write: engineWrite}})
	if err != nil {
		t.Fatalf("Failed to create RelaySwitch: %v", err)
	}

	// Send REQ
	req := NewReq(
		NewMessageIdFromUint(1),
		testCapEcho,
		[]byte{1, 2, 3},
		"text/plain",
	)
	if err := sw.SendToMaster(req, nil); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	// Read response
	response, err := sw.ReadFromMasters()
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	if response.FrameType != FrameTypeEnd {
		t.Errorf("Expected END frame, got %d", response.FrameType)
	}
	if response.Id.ToString() != NewMessageIdFromUint(1).ToString() {
		t.Errorf("ID mismatch")
	}
	if len(response.Payload) != 1 || response.Payload[0] != 42 {
		t.Errorf("Payload mismatch: %v", response.Payload)
	}
}

// TEST427: Multi-master cap routing
func Test427_relay_switch_multi_master_cap_routing(t *testing.T) {
	engineRead1, slaveWrite1 := net.Pipe()
	slaveRead1, engineWrite1 := net.Pipe()
	engineRead2, slaveWrite2 := net.Pipe()
	slaveRead2, engineWrite2 := net.Pipe()

	// Spawn slave 1 (echo)
	go func() {
		reader := NewFrameReader(slaveRead1)
		writer := NewFrameWriter(slaveWrite1)

		if !serveRelayHandshake(t, reader, writer, []string{testCapEcho}) {
			return
		}

		for {
			frame, err := reader.ReadFrame()
			if err != nil || frame == nil {
				return
			}
			if frame.FrameType == FrameTypeReq {
				response := NewEnd(frame.Id, []byte{1})
				writer.WriteFrame(response)
			}
		}
	}()

	// Spawn slave 2 (double)
	go func() {
		reader := NewFrameReader(slaveRead2)
		writer := NewFrameWriter(slaveWrite2)

		if !serveRelayHandshake(t, reader, writer, []string{`cap:in="media:void";double;out="media:void"`}) {
			return
		}

		for {
			frame, err := reader.ReadFrame()
			if err != nil || frame == nil {
				return
			}
			if frame.FrameType == FrameTypeReq {
				response := NewEnd(frame.Id, []byte{2})
				writer.WriteFrame(response)
			}
		}
	}()

	sw, err := NewRelaySwitch([]SocketPair{
		{ID: "test-master-1", Read: engineRead1, Write: engineWrite1},
		{ID: "test-master-2", Read: engineRead2, Write: engineWrite2},
	})
	if err != nil {
		t.Fatalf("Failed to create RelaySwitch: %v", err)
	}

	// Send REQ for echo
	req1 := NewReq(
		NewMessageIdFromUint(1),
		testCapEcho,
		[]byte{},
		"text/plain",
	)
	sw.SendToMaster(req1, nil)
	resp1, _ := sw.ReadFromMasters()
	if len(resp1.Payload) != 1 || resp1.Payload[0] != 1 {
		t.Errorf("Expected payload [1], got %v", resp1.Payload)
	}

	// Send REQ for double
	req2 := NewReq(
		NewMessageIdFromUint(2),
		`cap:in="media:void";double;out="media:void"`,
		[]byte{},
		"text/plain",
	)
	sw.SendToMaster(req2, nil)
	resp2, _ := sw.ReadFromMasters()
	if len(resp2.Payload) != 1 || resp2.Payload[0] != 2 {
		t.Errorf("Expected payload [2], got %v", resp2.Payload)
	}
}

// TEST428: Unknown cap returns error
func Test428_relay_switch_unknown_cap_returns_error(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)

		if !serveRelayHandshake(t, reader, writer, []string{testCapEcho}) {
			return
		}

		// Keep reading to prevent blocking
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	sw, err := NewRelaySwitch([]SocketPair{{ID: "test-master-3", Read: engineRead, Write: engineWrite}})
	if err != nil {
		t.Fatalf("Failed to create RelaySwitch: %v", err)
	}

	// Send REQ for unknown cap
	req := NewReq(
		NewMessageIdFromUint(1),
		`cap:in="media:void";unknown;out="media:void"`,
		[]byte{},
		"text/plain",
	)

	err = sw.SendToMaster(req, nil)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if _, ok := err.(*RelaySwitchError); !ok {
		t.Errorf("Expected RelaySwitchError, got %T", err)
	}
}

// TEST429: Cap routing logic (find_master_for_cap)
func Test429_relay_switch_find_master_for_cap(t *testing.T) {
	engineRead1, slaveWrite1 := net.Pipe()
	slaveRead1, engineWrite1 := net.Pipe()
	engineRead2, slaveWrite2 := net.Pipe()
	slaveRead2, engineWrite2 := net.Pipe()

	go func() {
		reader := NewFrameReader(slaveRead1)
		writer := NewFrameWriter(slaveWrite1)
		if !serveRelayHandshake(t, reader, writer, []string{testCapEcho}) {
			return
		}
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	go func() {
		reader := NewFrameReader(slaveRead2)
		writer := NewFrameWriter(slaveWrite2)
		if !serveRelayHandshake(t, reader, writer, []string{`cap:in="media:void";double;out="media:void"`}) {
			return
		}
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	sw, err := NewRelaySwitch([]SocketPair{
		{ID: "test-master-4", Read: engineRead1, Write: engineWrite1},
		{ID: "test-master-5", Read: engineRead2, Write: engineWrite2},
	})
	if err != nil {
		t.Fatalf("Failed to create RelaySwitch: %v", err)
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Verify routing
	idx1, err := sw.findMasterForCap(testCapEcho, nil)
	if err != nil || idx1 != 0 {
		t.Errorf("Expected master 0 for echo, got %d (err=%v)", idx1, err)
	}

	idx2, err := sw.findMasterForCap(`cap:in="media:void";double;out="media:void"`, nil)
	if err != nil || idx2 != 1 {
		t.Errorf("Expected master 1 for double, got %d (err=%v)", idx2, err)
	}

	_, err = sw.findMasterForCap(`cap:in="media:void";unknown;out="media:void"`, nil)
	if err == nil {
		t.Error("Expected error for unknown cap")
	}

	// Verify aggregate capabilities. The wire payload carries caps inside
	// `installed_cartridges[*].cap_groups[*].caps` rather than a flat
	// top-level `caps` array — count the union across both masters.
	var payload map[string]interface{}
	if err := json.Unmarshal(sw.capabilities, &payload); err != nil {
		t.Fatalf("failed to parse aggregate capabilities JSON: %v", err)
	}
	installedCartridges, ok := payload["installed_cartridges"].([]interface{})
	if !ok {
		t.Fatalf("expected installed_cartridges array in aggregate capabilities, got %T", payload["installed_cartridges"])
	}
	totalCaps := 0
	for _, c := range installedCartridges {
		cart, _ := c.(map[string]interface{})
		groups, _ := cart["cap_groups"].([]interface{})
		for _, g := range groups {
			grp, _ := g.(map[string]interface{})
			capsInGroup, _ := grp["caps"].([]interface{})
			totalCaps += len(capsInGroup)
		}
	}
	if totalCaps != 2 {
		t.Errorf("Expected 2 capabilities total across installed cartridges, got %d", totalCaps)
	}
}

// TEST430: Tie-breaking (same cap on multiple masters - first match wins, routing is consistent)
func Test430_relay_switch_tie_breaking(t *testing.T) {
	engineRead1, slaveWrite1 := net.Pipe()
	slaveRead1, engineWrite1 := net.Pipe()
	engineRead2, slaveWrite2 := net.Pipe()
	slaveRead2, engineWrite2 := net.Pipe()

	sameCap := testCapEcho

	// Slave 1 responds with [1]
	go func() {
		reader := NewFrameReader(slaveRead1)
		writer := NewFrameWriter(slaveWrite1)
		if !serveRelayHandshake(t, reader, writer, []string{sameCap}) {
			return
		}

		for {
			frame, err := reader.ReadFrame()
			if err != nil || frame == nil {
				return
			}
			if frame.FrameType == FrameTypeReq {
				response := NewEnd(frame.Id, []byte{1})
				writer.WriteFrame(response)
			}
		}
	}()

	// Slave 2 responds with [2]
	go func() {
		reader := NewFrameReader(slaveRead2)
		writer := NewFrameWriter(slaveWrite2)
		if !serveRelayHandshake(t, reader, writer, []string{sameCap}) {
			return
		}

		for {
			frame, err := reader.ReadFrame()
			if err != nil || frame == nil {
				return
			}
			if frame.FrameType == FrameTypeReq {
				response := NewEnd(frame.Id, []byte{2})
				writer.WriteFrame(response)
			}
		}
	}()

	sw, _ := NewRelaySwitch([]SocketPair{
		{ID: "test-master-6", Read: engineRead1, Write: engineWrite1},
		{ID: "test-master-7", Read: engineRead2, Write: engineWrite2},
	})

	// First request
	req1 := NewReq(NewMessageIdFromUint(1), sameCap, []byte{}, "text/plain")
	sw.SendToMaster(req1, nil)
	resp1, _ := sw.ReadFromMasters()
	if len(resp1.Payload) != 1 || resp1.Payload[0] != 1 {
		t.Errorf("First request should route to master 0, got payload %v", resp1.Payload)
	}

	// Second request - should also go to master 0
	req2 := NewReq(NewMessageIdFromUint(2), sameCap, []byte{}, "text/plain")
	sw.SendToMaster(req2, nil)
	resp2, _ := sw.ReadFromMasters()
	if len(resp2.Payload) != 1 || resp2.Payload[0] != 1 {
		t.Errorf("Second request should also route to master 0, got payload %v", resp2.Payload)
	}
}

// TEST431: Continuation frame routing (CHUNK, END follow REQ)
func Test431_relay_switch_continuation_frame_routing(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)

		if !serveRelayHandshake(t, reader, writer, []string{`cap:in="media:void";test;out="media:void"`}) {
			return
		}

		// Read REQ
		req, _ := reader.ReadFrame()
		if req.FrameType != FrameTypeReq {
			t.Errorf("Expected REQ, got %d", req.FrameType)
			return
		}

		// Read CHUNK
		chunk, _ := reader.ReadFrame()
		if chunk.FrameType != FrameTypeChunk {
			t.Errorf("Expected CHUNK, got %d", chunk.FrameType)
			return
		}
		if chunk.Id.ToString() != req.Id.ToString() {
			t.Error("CHUNK ID mismatch")
			return
		}

		// Read END
		end, _ := reader.ReadFrame()
		if end.FrameType != FrameTypeEnd {
			t.Errorf("Expected END, got %d", end.FrameType)
			return
		}
		if end.Id.ToString() != req.Id.ToString() {
			t.Error("END ID mismatch")
			return
		}

		// Send response
		response := NewEnd(req.Id, []byte{42})
		writer.WriteFrame(response)
	}()

	sw, _ := NewRelaySwitch([]SocketPair{{ID: "test-master-8", Read: engineRead, Write: engineWrite}})

	reqID := NewMessageIdFromUint(1)

	// Send REQ
	req := NewReq(reqID, `cap:in="media:void";test;out="media:void"`, []byte{}, "text/plain")
	sw.SendToMaster(req, nil)

	// Send CHUNK
	payload := []byte{1, 2, 3}
	checksum := ComputeChecksum(payload)
	chunk := NewChunk(reqID, "stream1", 0, payload, 0, checksum)
	sw.SendToMaster(chunk, nil)

	// Send END
	end := NewEnd(reqID, nil)
	sw.SendToMaster(end, nil)

	// Read response
	response, _ := sw.ReadFromMasters()
	if response.FrameType != FrameTypeEnd {
		t.Errorf("Expected END, got %d", response.FrameType)
	}
	if len(response.Payload) != 1 || response.Payload[0] != 42 {
		t.Errorf("Payload mismatch: %v", response.Payload)
	}
}

// TEST432: Empty masters list creates empty switch, add_master works
func Test432_relay_switch_empty_masters_list_error(t *testing.T) {
	_, err := NewRelaySwitch([]SocketPair{})
	if err == nil {
		t.Fatal("Expected error for empty masters list")
	}
	rsErr, ok := err.(*RelaySwitchError)
	if !ok {
		t.Fatalf("Expected RelaySwitchError, got %T", err)
	}
	if rsErr.Type != RelaySwitchErrorTypeProtocol {
		t.Errorf("Expected Protocol error, got %d", rsErr.Type)
	}
}

// TEST433: Capability aggregation deduplicates caps
func Test433_relay_switch_capability_aggregation_deduplicates(t *testing.T) {
	engineRead1, slaveWrite1 := net.Pipe()
	slaveRead1, engineWrite1 := net.Pipe()
	engineRead2, slaveWrite2 := net.Pipe()
	slaveRead2, engineWrite2 := net.Pipe()

	go func() {
		reader := NewFrameReader(slaveRead1)
		writer := NewFrameWriter(slaveWrite1)
		if !serveRelayHandshake(t, reader, writer, []string{
			testCapEcho,
			`cap:in="media:void";double;out="media:void"`,
		}) {
			return
		}
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	go func() {
		reader := NewFrameReader(slaveRead2)
		writer := NewFrameWriter(slaveWrite2)
		if !serveRelayHandshake(t, reader, writer, []string{
			testCapEcho, // Duplicate
			`cap:in="media:void";triple;out="media:void"`,
		}) {
			return
		}
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	sw, _ := NewRelaySwitch([]SocketPair{
		{ID: "test-master-9", Read: engineRead1, Write: engineWrite1},
		{ID: "test-master-10", Read: engineRead2, Write: engineWrite2},
	})

	// The wire payload carries caps inside
	// installed_cartridges[*].cap_groups[*].caps; deduplication is a
	// computed view via RelayNotifyCapabilitiesPayload.CapURNs(), not
	// a property of the payload itself. Both source manifests survive
	// in the aggregate; the dedup is observable through the computed
	// flat view.
	var payload RelayNotifyCapabilitiesPayload
	if err := json.Unmarshal(sw.Capabilities(), &payload); err != nil {
		t.Fatalf("failed to parse aggregate capabilities JSON: %v", err)
	}
	if got := len(payload.InstalledCartridges); got != 2 {
		t.Errorf("Expected 2 installed cartridges in the aggregate (one per master), got %d", got)
	}
	uniqueCaps := payload.CapURNs()
	if len(uniqueCaps) != 3 {
		t.Errorf("Expected 3 unique caps after dedup, got %d: %v", len(uniqueCaps), uniqueCaps)
	}
}

// TEST434: Limits negotiation takes minimum
func Test434_relay_switch_limits_negotiation_minimum(t *testing.T) {
	engineRead1, slaveWrite1 := net.Pipe()
	slaveRead1, engineWrite1 := net.Pipe()
	engineRead2, slaveWrite2 := net.Pipe()
	slaveRead2, engineWrite2 := net.Pipe()

	go func() {
		reader := NewFrameReader(slaveRead1)
		writer := NewFrameWriter(slaveWrite1)
		manifest := testManifestWithCaps([]string{})
		manifestJSON, _ := json.Marshal(manifest)
		limits1 := Limits{MaxFrame: 1_000_000, MaxChunk: 100_000}
		SendNotify(writer, manifestJSON, limits1)
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	go func() {
		reader := NewFrameReader(slaveRead2)
		writer := NewFrameWriter(slaveWrite2)
		manifest := testManifestWithCaps([]string{})
		manifestJSON, _ := json.Marshal(manifest)
		limits2 := Limits{MaxFrame: 2_000_000, MaxChunk: 50_000}
		SendNotify(writer, manifestJSON, limits2)
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	sw, _ := NewRelaySwitch([]SocketPair{
		{ID: "test-master-11", Read: engineRead1, Write: engineWrite1},
		{ID: "test-master-12", Read: engineRead2, Write: engineWrite2},
	})

	limits := sw.Limits()
	if limits.MaxFrame != 1_000_000 {
		t.Errorf("Expected max_frame 1000000, got %d", limits.MaxFrame)
	}
	if limits.MaxChunk != 50_000 {
		t.Errorf("Expected max_chunk 50000, got %d", limits.MaxChunk)
	}
}

// TEST435: URN matching (exact vs accepts())
func Test435_relay_switch_urn_matching(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	registeredCap := `cap:in="media:text;utf8";process;out="media:text;utf8"`

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		if !serveRelayHandshake(t, reader, writer, []string{registeredCap}) {
			return
		}

		for {
			frame, err := reader.ReadFrame()
			if err != nil || frame == nil {
				return
			}
			if frame.FrameType == FrameTypeReq {
				response := NewEnd(frame.Id, []byte{42})
				writer.WriteFrame(response)
			}
		}
	}()

	sw, _ := NewRelaySwitch([]SocketPair{{ID: "test-master-13", Read: engineRead, Write: engineWrite}})

	// Exact match should work
	req1 := NewReq(NewMessageIdFromUint(1), registeredCap, []byte{}, "text/plain")
	if err := sw.SendToMaster(req1, nil); err != nil {
		t.Errorf("Exact match should work: %v", err)
	}
	resp1, _ := sw.ReadFromMasters()
	if len(resp1.Payload) != 1 || resp1.Payload[0] != 42 {
		t.Errorf("Payload mismatch: %v", resp1.Payload)
	}

	// More specific request SHOULD match under is_dispatchable semantics:
	// Input (contravariant): request's media:text;utf8;normalized conforms_to provider's media:text;utf8
	// Output (covariant): provider's media:text;utf8 conforms_to request's media:text
	req2 := NewReq(
		NewMessageIdFromUint(2),
		`cap:in="media:text;utf8;normalized";process;out="media:text"`,
		[]byte{},
		"text/plain",
	)
	if err := sw.SendToMaster(req2, nil); err != nil {
		t.Errorf("More specific request should match under is_dispatchable: %v", err)
	}
	resp2, err := sw.ReadFromMasters()
	if err != nil {
		t.Fatalf("Failed to read response for req2: %v", err)
	}
	if len(resp2.Payload) != 1 || resp2.Payload[0] != 42 {
		t.Errorf("Payload mismatch for req2: %v", resp2.Payload)
	}
}

// TEST437: find_master_for_cap with preferred_cap routes to generic handler With is_dispatchable semantics: - Generic provider (in=media:) CAN dispatch specific request (in="media:ext=pdf") because media: (wildcard) accepts any input type - Preference routes to preferred among dispatchable candidates
func Test437_preferred_cap_routes_to_generic(t *testing.T) {
	// Master 0: generic thumbnail handler
	engineRead0, slaveWrite0 := net.Pipe()
	slaveRead0, engineWrite0 := net.Pipe()

	// Master 1: specific PDF thumbnail handler
	engineRead1, slaveWrite1 := net.Pipe()
	slaveRead1, engineWrite1 := net.Pipe()

	genericCap := `cap:in=media:;generate-thumbnail;out="media:ext=png;image;thumbnail"`
	specificCap := `cap:in="media:ext=pdf";generate-thumbnail;out="media:ext=png;image;thumbnail"`

	spawnSlave := func(r, w net.Conn, caps []string) {
		go func() {
			reader := NewFrameReader(r)
			writer := NewFrameWriter(w)
			if !serveRelayHandshake(t, reader, writer, caps) {
				return
			}
			for {
				if _, err := reader.ReadFrame(); err != nil {
					return
				}
			}
		}()
	}
	// Master 0 has identity + generic cap
	spawnSlave(slaveRead0, slaveWrite0, []string{testCapIdentity, genericCap})
	// Master 1 has identity + specific cap
	spawnSlave(slaveRead1, slaveWrite1, []string{testCapIdentity, specificCap})

	sw, err := NewRelaySwitch([]SocketPair{
		{ID: "test-master-14", Read: engineRead0, Write: engineWrite0},
		{ID: "test-master-15", Read: engineRead1, Write: engineWrite1},
	})
	if err != nil {
		t.Fatalf("Failed to create RelaySwitch: %v", err)
	}

	request := `cap:in="media:ext=pdf";generate-thumbnail;out="media:ext=png;image;thumbnail"`

	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Without preference: routes to master 1 (specific, closest-specificity wins)
	idx, err := sw.findMasterForCap(request, nil)
	if err != nil || idx != 1 {
		t.Errorf("Without preference: expected master 1 (specific), got %d (err=%v)", idx, err)
	}

	// With preference for generic cap: routes to master 0 (generic is IsEquivalent to preference)
	idx, err = sw.findMasterForCap(request, &genericCap)
	if err != nil || idx != 0 {
		t.Errorf("With generic preference: expected master 0, got %d (err=%v)", idx, err)
	}

	// With preference for specific cap: routes to master 1 (specificCap on master 1 is IsEquivalent)
	idx, err = sw.findMasterForCap(request, &specificCap)
	if err != nil || idx != 1 {
		t.Errorf("With specific preference: expected master 1, got %d (err=%v)", idx, err)
	}
}

// TEST438: find_master_for_cap with preference falls back to closest-specificity when preferred cap is not in the comparable set
func Test438_preferred_cap_falls_back_when_not_comparable(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	registered := `cap:in="media:ext=pdf";generate-thumbnail;out="media:ext=png;image;thumbnail"`

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		if !serveRelayHandshake(t, reader, writer, []string{testCapIdentity, registered}) {
			return
		}
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	sw, err := NewRelaySwitch([]SocketPair{{ID: "test-master-16", Read: engineRead, Write: engineWrite}})
	if err != nil {
		t.Fatalf("Failed to create RelaySwitch: %v", err)
	}

	request := `cap:in="media:ext=pdf";generate-thumbnail;out="media:ext=png;image;thumbnail"`
	// Preference for an unrelated cap — no equivalent match, falls back to closest-specificity
	unrelated := `cap:in="media:txt;enc=utf-8";generate-thumbnail;out="media:ext=png;image;thumbnail"`

	sw.mu.Lock()
	defer sw.mu.Unlock()

	idx, err := sw.findMasterForCap(request, &unrelated)
	if err != nil || idx != 0 {
		t.Errorf("Expected fallback to master 0 for unrelated preference, got %d (err=%v)", idx, err)
	}
}

// TEST439: Generic provider CAN dispatch specific request (but only matches if no more specific provider exists) With is_dispatchable: generic provider (in=media:) CAN handle specific request (in="media:ext=pdf") because media: accepts any input type. With preference, can route to generic even when more specific exists.
func Test439_generic_provider_can_dispatch_specific_request(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	genericCap := `cap:in=media:;generate-thumbnail;out="media:ext=png;image;thumbnail"`

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		if !serveRelayHandshake(t, reader, writer, []string{testCapIdentity, genericCap}) {
			return
		}
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	sw, err := NewRelaySwitch([]SocketPair{{ID: "test-master-17", Read: engineRead, Write: engineWrite}})
	if err != nil {
		t.Fatalf("Failed to create RelaySwitch: %v", err)
	}

	// Specific PDF request — generic handler CAN dispatch it
	request := `cap:in="media:ext=pdf";generate-thumbnail;out="media:ext=png;image;thumbnail"`

	sw.mu.Lock()
	defer sw.mu.Unlock()

	idx, err := sw.findMasterForCap(request, nil)
	if err != nil || idx != 0 {
		t.Errorf("Generic provider should dispatch specific request: got %d (err=%v)", idx, err)
	}
}

// =============================================================
// Wire-format tests for CartridgeAttachmentErrorKind
// =============================================================
//
// The kind enum crosses three boundaries (relay socket JSON, gRPC
// proto enum, and on the Mac side NSXPC dictionaries). Every
// variant's string value MUST match its proto snake_case name
// byte-for-byte; otherwise a Swift-side cartridge marked
// `disabled` arrives at the engine as an unknown variant and the
// whole RelayNotify aggregate fails to deserialize for that
// master.
//
// Test1720/1721/1722 mirror the Rust counterparts in
// capdag/src/bifaci/relay_switch.rs. Cross-language parity is the
// whole point — these are not "test the language's enum" tests,
// they're "test that the Go port hasn't drifted from the wire
// contract" tests.

// Test6374_CartridgeAttachmentErrorKindMatchesProtoSnakeCase pins
// every variant's string value against its proto snake_case
// name. New variants must be added here AND in the Rust /
// Swift / proto sides.
func Test6374_CartridgeAttachmentErrorKindMatchesProtoSnakeCase(t *testing.T) {
	cases := []struct {
		kind     CartridgeAttachmentErrorKind
		expected string
	}{
		{CartridgeAttachmentErrorKindIncompatible, "incompatible"},
		{CartridgeAttachmentErrorKindManifestInvalid, "manifest_invalid"},
		{CartridgeAttachmentErrorKindHandshakeFailed, "handshake_failed"},
		{CartridgeAttachmentErrorKindIdentityRejected, "identity_rejected"},
		{CartridgeAttachmentErrorKindEntryPointMissing, "entry_point_missing"},
		{CartridgeAttachmentErrorKindQuarantined, "quarantined"},
		{CartridgeAttachmentErrorKindBadInstallation, "bad_installation"},
		{CartridgeAttachmentErrorKindDisabled, "disabled"},
		{CartridgeAttachmentErrorKindRegistryUnreachable, "registry_unreachable"},
	}
	for _, c := range cases {
		if string(c.kind) != c.expected {
			t.Errorf("variant %q must have string value %q to match cartridge.proto's CartridgeAttachmentErrorKind",
				c.kind, c.expected)
		}
	}
}

// Test6379_CartridgeAttachmentErrorJSONRoundTrips verifies a
// CartridgeAttachmentError marshals to JSON and unmarshals back
// without changing the kind for every variant. RelayNotify wire
// payload is JSON; a single-variant regression breaks the entire
// per-master parse.
func Test6379_CartridgeAttachmentErrorJSONRoundTrips(t *testing.T) {
	cases := []CartridgeAttachmentErrorKind{
		CartridgeAttachmentErrorKindIncompatible,
		CartridgeAttachmentErrorKindManifestInvalid,
		CartridgeAttachmentErrorKindHandshakeFailed,
		CartridgeAttachmentErrorKindIdentityRejected,
		CartridgeAttachmentErrorKindEntryPointMissing,
		CartridgeAttachmentErrorKindQuarantined,
		CartridgeAttachmentErrorKindBadInstallation,
		CartridgeAttachmentErrorKindDisabled,
		CartridgeAttachmentErrorKindRegistryUnreachable,
	}
	for _, kind := range cases {
		original := CartridgeAttachmentError{
			Kind:                  kind,
			Message:               "round-trip test for " + string(kind),
			DetectedAtUnixSeconds: 1700000000,
		}
		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("marshal failed for kind %q: %v", kind, err)
		}
		var decoded CartridgeAttachmentError
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("unmarshal failed for kind %q (json=%s): %v", kind, data, err)
		}
		if decoded.Kind != original.Kind {
			t.Errorf("kind round-trip drift: original=%q decoded=%q (json=%s)",
				original.Kind, decoded.Kind, data)
		}
		if decoded.Message != original.Message {
			t.Errorf("message round-trip drift for kind %q: original=%q decoded=%q",
				kind, original.Message, decoded.Message)
		}
		if decoded.DetectedAtUnixSeconds != original.DetectedAtUnixSeconds {
			t.Errorf("detected_at_unix_seconds round-trip drift for kind %q: original=%d decoded=%d",
				kind, original.DetectedAtUnixSeconds, decoded.DetectedAtUnixSeconds)
		}
	}
}

// Test6423_CartridgeAttachmentErrorDecodesProtoSnakeCaseStrings is the
// engine→Go-host (or Swift→Go-host) decode path: incoming JSON
// uses the snake_case wire format, and the Go side must resolve
// each string into the matching variant. CartridgeAttachmentErrorKind
// is just `type ... string`, so this test is also a check that the
// JSON unmarshaller doesn't normalise/lowercase/etc the bytes
// behind our backs.
func Test6423_CartridgeAttachmentErrorDecodesProtoSnakeCaseStrings(t *testing.T) {
	cases := []struct {
		raw          string
		expectedKind CartridgeAttachmentErrorKind
	}{
		{"incompatible", CartridgeAttachmentErrorKindIncompatible},
		{"manifest_invalid", CartridgeAttachmentErrorKindManifestInvalid},
		{"handshake_failed", CartridgeAttachmentErrorKindHandshakeFailed},
		{"identity_rejected", CartridgeAttachmentErrorKindIdentityRejected},
		{"entry_point_missing", CartridgeAttachmentErrorKindEntryPointMissing},
		{"quarantined", CartridgeAttachmentErrorKindQuarantined},
		{"bad_installation", CartridgeAttachmentErrorKindBadInstallation},
		{"disabled", CartridgeAttachmentErrorKindDisabled},
		{"registry_unreachable", CartridgeAttachmentErrorKindRegistryUnreachable},
	}
	for _, c := range cases {
		jsonStr := `{"kind":"` + c.raw + `","message":"x","detected_at_unix_seconds":1}`
		var decoded CartridgeAttachmentError
		if err := json.Unmarshal([]byte(jsonStr), &decoded); err != nil {
			t.Fatalf("unmarshal of %s failed: %v", jsonStr, err)
		}
		if decoded.Kind != c.expectedKind {
			t.Errorf("wire kind %q must decode to %q, got %q",
				c.raw, c.expectedKind, decoded.Kind)
		}
	}
}

// ============================================================
// Reattach-by-id tests for the cardinality-stable slot model.
//
// When a master dies and the host reconnects, the new socket MUST
// attach to the same slot index — preserving routing entries
// keyed by index. Accumulating zombie slots on each reconnect was
// the bug class these tests guard against.

func Test133_ReattachByIDPreservesSlotIndex(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	// Slave 1: send RelayNotify, answer the identity probe, and stay
	// alive until the pipe closes.
	go func() {
		writer := NewFrameWriter(slaveWrite)
		reader := NewFrameReader(slaveRead)
		if !serveRelayHandshake(t, reader, writer, []string{testCapIdentity}) {
			return
		}
		// Block on a read so the pipe stays open until the test
		// closes its end.
		_, _ = reader.ReadFrame()
	}()

	sw, err := NewRelaySwitch([]SocketPair{
		{ID: "xpc-service", Read: engineRead, Write: engineWrite},
	})
	if err != nil {
		t.Fatalf("NewRelaySwitch: %v", err)
	}
	if got := len(sw.masters); got != 1 {
		t.Fatalf("masters len: got %d, want 1", got)
	}
	if sw.masters[0].id != "xpc-service" {
		t.Fatalf("master id: got %q, want %q", sw.masters[0].id, "xpc-service")
	}
	if !sw.masters[0].healthy {
		t.Fatalf("master should be healthy after construction")
	}

	// Simulate master death via the same code path the frame loop
	// uses on EOF. Bypassing the frame loop keeps the test focused
	// on the reattach contract itself.
	sw.handleMasterDeath(0)
	if got := len(sw.masters); got != 1 {
		t.Fatalf("handleMasterDeath must NOT remove the slot — reattach depends on it staying in place; got len %d", got)
	}
	if sw.masters[0].healthy {
		t.Fatalf("master should be unhealthy after handleMasterDeath")
	}

	// Reconnect: build a fresh slave + pipe pair under the SAME id.
	engineRead2, slaveWrite2 := net.Pipe()
	slaveRead2, engineWrite2 := net.Pipe()

	go func() {
		writer := NewFrameWriter(slaveWrite2)
		manifest := testManifestWithCaps([]string{testCapIdentity})
		manifestJSON, _ := json.Marshal(manifest)
		limits := DefaultLimits()
		if err := SendNotify(writer, manifestJSON, limits); err != nil {
			t.Errorf("slave2 SendNotify: %v", err)
			return
		}
		reader := NewFrameReader(slaveRead2)
		_, _ = reader.ReadFrame()
	}()

	newIdx, err := sw.AddMaster(SocketPair{
		ID:    "xpc-service",
		Read:  engineRead2,
		Write: engineWrite2,
	})
	if err != nil {
		t.Fatalf("AddMaster reattach: %v", err)
	}
	if newIdx != 0 {
		t.Fatalf("reattach MUST return the same slot index (0), not append a new slot; got %d", newIdx)
	}
	if got := len(sw.masters); got != 1 {
		t.Fatalf("reattach MUST NOT grow the slot count — that was the zombie-slot bug; got len %d", got)
	}
	if !sw.masters[0].healthy {
		t.Fatalf("slot should be healthy after reattach")
	}
	if sw.masters[0].id != "xpc-service" {
		t.Fatalf("slot id MUST be preserved across reattach; got %q", sw.masters[0].id)
	}
}

// TEST134: Add master with duplicate healthy i d errors
func Test134_AddMasterWithDuplicateHealthyIDErrors(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	go func() {
		writer := NewFrameWriter(slaveWrite)
		reader := NewFrameReader(slaveRead)
		if !serveRelayHandshake(t, reader, writer, []string{testCapIdentity}) {
			return
		}
		_, _ = reader.ReadFrame()
	}()

	sw, err := NewRelaySwitch([]SocketPair{
		{ID: "xpc-service", Read: engineRead, Write: engineWrite},
	})
	if err != nil {
		t.Fatalf("NewRelaySwitch: %v", err)
	}
	if !sw.masters[0].healthy {
		t.Fatalf("slot should be healthy after construction")
	}

	// Try to add a second master with the same id while healthy.
	// The duplicate-id check fires BEFORE any I/O on the dummy
	// pipes, so the dummies never have to go through a handshake.
	dummyRead, _ := net.Pipe()
	dummyOther, _ := net.Pipe()
	_, err = sw.AddMaster(SocketPair{
		ID:    "xpc-service",
		Read:  dummyRead,
		Write: dummyOther,
	})
	if err == nil {
		t.Fatalf("re-adding a healthy id must error")
	}
	if !strings.Contains(err.Error(), "already attached to a healthy slot") {
		t.Fatalf("error message should name the cardinality violation; got %q", err.Error())
	}
	if got := len(sw.masters); got != 1 {
		t.Fatalf("no slot should be created when the duplicate-id check fires; got len %d", got)
	}
}

// TEST6745: RelaySwitch::new rejects duplicate ids in its cardinality list.
func Test6745_RelaySwitchNewRejectsDuplicateIDs(t *testing.T) {
	a, _ := net.Pipe()
	aOther, _ := net.Pipe()
	b, _ := net.Pipe()
	bOther, _ := net.Pipe()

	_, err := NewRelaySwitch([]SocketPair{
		{ID: "dup-id", Read: a, Write: aOther},
		{ID: "dup-id", Read: b, Write: bOther},
	})
	if err == nil {
		t.Fatalf("duplicate ids must be rejected")
	}
	if !strings.Contains(err.Error(), `duplicate master id "dup-id"`) {
		t.Fatalf("error should name the duplicate id; got %q", err.Error())
	}
}

// TEST487: RelaySwitch construction succeeds when master's identity verification passes
func Test487_relay_switch_identity_verification_succeeds(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		// Advertise identity + one test cap so construction runs the
		// end-to-end identity probe before the caps become routable.
		if !serveRelayHandshake(t, reader, writer, []string{
			testCapIdentity,
			`cap:in="media:void";test;out="media:void"`,
		}) {
			return
		}
		// Keep the pipe open after the probe.
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	sw, err := NewRelaySwitch([]SocketPair{
		{ID: "test-master-0", Read: engineRead, Write: engineWrite},
	})
	if err != nil {
		t.Fatalf("construction must succeed when identity verification passes: %v", err)
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Construction succeeded — caps are populated and routable.
	idx, err := sw.findMasterForCap(testCapIdentity, nil)
	if err != nil || idx != 0 {
		t.Errorf("expected master 0 for identity cap, got %d (err=%v)", idx, err)
	}
	idx, err = sw.findMasterForCap(`cap:in="media:void";test;out="media:void"`, nil)
	if err != nil || idx != 0 {
		t.Errorf("expected master 0 for test cap, got %d (err=%v)", idx, err)
	}
}

// TEST488: RelaySwitch construction fails when master's identity verification fails
func Test488_relay_switch_identity_verification_fails(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)

		// Send RelayNotify — an installed cartridge whose single
		// cap-group declares CAP_IDENTITY so the host clears the
		// payload-level identity check before the engine probes.
		manifest := testManifestWithCaps([]string{testCapIdentity})
		manifestJSON, _ := json.Marshal(manifest)
		if err := SendNotify(writer, manifestJSON, DefaultLimits()); err != nil {
			t.Errorf("slave SendNotify: %v", err)
			return
		}

		// Read identity REQ, respond with ERR.
		req, err := reader.ReadFrame()
		if err != nil || req == nil {
			t.Errorf("expected identity REQ: %v", err)
			return
		}
		if req.FrameType != FrameTypeReq {
			t.Errorf("expected identity REQ, got %d", req.FrameType)
			return
		}
		errFrame := NewErr(req.Id, "BROKEN", "identity verification broken")
		_ = writer.WriteFrame(errFrame)
	}()

	_, err := NewRelaySwitch([]SocketPair{
		{ID: "test-master-0", Read: engineRead, Write: engineWrite},
	})
	if err == nil {
		t.Fatal("construction must fail when identity verification fails")
	}
	if !strings.Contains(err.Error(), "identity verification failed") {
		t.Errorf("error must mention identity verification: %v", err)
	}
}

// =========================================================================
// all_masters_ready / SetExpectedMasterCount
// =========================================================================
//
// The host's `.configuring → .ready` advance is gated on AllMastersReady,
// so its corner cases matter:
//
//   - Returns false when expected count is unset (default 0).
//   - Returns false when only some of the expected masters connected.
//   - Returns true once the expected count is met AND every connected
//     master is healthy (caps may be empty — they register
//     incrementally as cartridges progress to Operational).

// buildSwitchWithNMasters builds a switch whose constructor reads
// RelayNotify from n slaves, each registering identity + one cap, and
// answering the construction-time identity probe. Mirrors Rust's
// build_switch_with_n_masters.
func buildSwitchWithNMasters(t *testing.T, n int) *RelaySwitch {
	t.Helper()
	sockets := make([]SocketPair, 0, n)
	for i := 0; i < n; i++ {
		engineRead, slaveWrite := net.Pipe()
		slaveRead, engineWrite := net.Pipe()
		capURN := fmt.Sprintf(`cap:in="media:t%d";noop;out="media:t%d"`, i, i)
		go func() {
			reader := NewFrameReader(slaveRead)
			writer := NewFrameWriter(slaveWrite)
			if !serveRelayHandshake(t, reader, writer, []string{testCapIdentity, capURN}) {
				return
			}
			for {
				if _, err := reader.ReadFrame(); err != nil {
					return
				}
			}
		}()
		sockets = append(sockets, SocketPair{
			ID:    fmt.Sprintf("test-master-%d", i),
			Read:  engineRead,
			Write: engineWrite,
		})
	}
	sw, err := NewRelaySwitch(sockets)
	if err != nil {
		t.Fatalf("buildSwitchWithNMasters(%d): %v", n, err)
	}
	return sw
}

// buildSwitchWithNCaplessMasters builds a switch whose masters connect
// but RelayNotify an EMPTY cap set — mirrors the real-world "cartridges
// still inspecting / verifying" state where the master has connected
// but no cartridge has reached Operational yet. An empty cap set skips
// the identity probe (no handler chain to test). Mirrors Rust's
// build_switch_with_n_capless_masters.
func buildSwitchWithNCaplessMasters(t *testing.T, n int) *RelaySwitch {
	t.Helper()
	sockets := make([]SocketPair, 0, n)
	for i := 0; i < n; i++ {
		engineRead, slaveWrite := net.Pipe()
		slaveRead, engineWrite := net.Pipe()
		go func() {
			reader := NewFrameReader(slaveRead)
			writer := NewFrameWriter(slaveWrite)
			if !serveRelayHandshake(t, reader, writer, []string{}) {
				return
			}
			for {
				if _, err := reader.ReadFrame(); err != nil {
					return
				}
			}
		}()
		sockets = append(sockets, SocketPair{
			ID:    fmt.Sprintf("test-master-%d", i),
			Read:  engineRead,
			Write: engineWrite,
		})
	}
	sw, err := NewRelaySwitch(sockets)
	if err != nil {
		t.Fatalf("buildSwitchWithNCaplessMasters(%d): %v", n, err)
	}
	return sw
}

// TEST0136: All masters ready false when expected count unset
func Test0136_all_masters_ready_false_when_expected_count_unset(t *testing.T) {
	// Even with a connected, fully-RelayNotify'd master, the
	// predicate must return false until the engine explicitly
	// declares its expected master count via SetExpectedMasterCount.
	// The default-zero policy is the safety net that makes "engine
	// boot forgot to declare its expected count" surface as a hung
	// readiness gate rather than a false-positive ready signal.
	sw := buildSwitchWithNMasters(t, 1)
	if sw.AllMastersReady() {
		t.Errorf("all_masters_ready must return false when expected_master_count is 0")
	}
}

// TEST0137: All masters ready false when partially connected
func Test0137_all_masters_ready_false_when_partially_connected(t *testing.T) {
	// 1 master connected, 2 expected. This is the live regression we
	// shipped: the internal master had caps from t=0 but the
	// external-providers master was still spawning cartridges. The
	// host saw ready immediately and the bidi never started.
	sw := buildSwitchWithNMasters(t, 1)
	sw.SetExpectedMasterCount(2)
	if sw.AllMastersReady() {
		t.Errorf("all_masters_ready must return false until masters.len() reaches expected_master_count")
	}
}

// TEST0139: All masters ready true when masters connected but capless
func Test0139_all_masters_ready_true_when_masters_connected_but_capless(t *testing.T) {
	// Cartridges in `.discovered` / `.inspecting` / `.verifying`
	// contribute zero caps to their master's RelayNotify. The engine
	// readiness gate must still fire so the splash screen can unblock
	// — caps register incrementally as cartridges progress to
	// `.operational`. A regression that re-coupled readiness to
	// cap-set non-emptiness would make this test fail (and would hang
	// the splash screen on every cold start with slow cartridges).
	sw := buildSwitchWithNCaplessMasters(t, 2)
	sw.SetExpectedMasterCount(2)
	if !sw.AllMastersReady() {
		t.Errorf("all_masters_ready must NOT require master.caps to be non-empty — caps register asynchronously as cartridges progress to Operational")
	}
}

// TEST0140: All masters ready does not overshoot
func Test0140_all_masters_ready_does_not_overshoot(t *testing.T) {
	// 2 masters connected, 1 expected. The predicate should still
	// report ready — the engine got more masters than it declared,
	// which is fine; "at least expected" is the semantic. (A
	// regression that used `==` instead of `>=` would make this case
	// false and break edition setups where an extra master arrives
	// later.)
	sw := buildSwitchWithNMasters(t, 2)
	sw.SetExpectedMasterCount(1)
	if !sw.AllMastersReady() {
		t.Errorf("all_masters_ready uses >= not == against expected_master_count")
	}
}

// constHandler returns a constant byte string (ignores input).
// Mirrors the Rust ConstHandler used by the add_master / preferred-cap
// routing tests.
type constHandler struct {
	value string
}

func (h constHandler) HandleRequest(_ string, input <-chan Frame, output *ResponseWriter, _ PeerInvoker) {
	// Drain input until END.
	for frame := range input {
		if frame.FrameType == FrameTypeEnd {
			break
		}
	}
	output.EmitResponse("media:", []byte(h.value))
}

// wireInProcessHost connects an InProcessCartridgeHost directly to one
// end of a net.Pipe and returns the switch-side SocketPair. The Go
// InProcessCartridgeHost.Run already sends the RelayNotify, answers the
// identity probe, and routes REQs — so it stands in for Rust's
// host→RelaySlave→switch chain (the RelaySlave is a transparent
// forwarder; omitting it does not change observable switch behavior).
func wireInProcessHost(t *testing.T, id string, host *InProcessCartridgeHost) SocketPair {
	t.Helper()
	// A real socketpair (buffered, full-duplex) rather than net.Pipe:
	// the in-process host batches its handshake writes, which would
	// deadlock against an unbuffered pipe.
	hostConn, switchConn := createSocketPair(t)
	go func() {
		_ = host.Run(hostConn, hostConn)
	}()
	return SocketPair{ID: id, Read: switchConn, Write: switchConn}
}

// readSwitchResponse drains the switch response stream for rid and
// returns the concatenated CBOR-decoded chunk payloads up to END.
func readSwitchResponse(t *testing.T, sw *RelaySwitch, rid MessageId) []byte {
	t.Helper()
	var data []byte
	for i := 0; i < 20; i++ {
		frame, err := sw.ReadFromMasters()
		if err != nil {
			t.Fatalf("ReadFromMasters: %v", err)
		}
		if !frame.Id.Equals(rid) {
			continue
		}
		switch frame.FrameType {
		case FrameTypeChunk:
			decoded, err := DecodeChunkPayload(frame.Payload)
			if err != nil {
				t.Fatalf("DecodeChunkPayload: %v", err)
			}
			data = append(data, decoded...)
		case FrameTypeEnd:
			return data
		case FrameTypeErr:
			t.Fatalf("unexpected ERR: %s", frame.ErrorMessage())
		}
	}
	t.Fatalf("did not receive END for rid %s", rid.ToString())
	return nil
}

// TEST0132: add_master dynamically connects new host to running switch
func Test0132_add_master_dynamic(t *testing.T) {
	// Create initial switch with handler A (alpha).
	capA := `cap:in="media:void";alpha;out="media:void"`
	hostA := NewInProcessCartridgeHost(
		InProcessHostIdentityForTest("alpha-host"),
		[]HandlerRegistration{{
			Name:    "alpha",
			Caps:    []cap.Cap{makeTestCap(t, capA)},
			Handler: constHandler{value: "alpha"},
		}},
	)
	spA := wireInProcessHost(t, "test-master-0", hostA)

	sw, err := NewRelaySwitch([]SocketPair{spA})
	if err != nil {
		t.Fatalf("NewRelaySwitch: %v", err)
	}
	if got := len(sw.masters); got != 1 {
		t.Fatalf("masters len: got %d, want 1", got)
	}

	// Add handler B (beta) dynamically. A distinct id exercises the
	// append branch of AddMaster (slot index 1).
	capB := `cap:in="media:void";beta;out="media:void"`
	hostB := NewInProcessCartridgeHost(
		InProcessHostIdentityForTest("beta-host"),
		[]HandlerRegistration{{
			Name:    "beta",
			Caps:    []cap.Cap{makeTestCap(t, capB)},
			Handler: constHandler{value: "beta"},
		}},
	)
	spB := wireInProcessHost(t, "test-master-1", hostB)

	idx, err := sw.AddMaster(spB)
	if err != nil {
		t.Fatalf("AddMaster: %v", err)
	}
	if idx != 1 {
		t.Fatalf("AddMaster must append at index 1, got %d", idx)
	}
	if got := len(sw.masters); got != 2 {
		t.Fatalf("masters len: got %d, want 2", got)
	}

	// Verify both caps are in the aggregate capabilities.
	var payload RelayNotifyCapabilitiesPayload
	if err := json.Unmarshal(sw.Capabilities(), &payload); err != nil {
		t.Fatalf("parse aggregate capabilities: %v", err)
	}
	caps := payload.CapURNs()
	foundAlpha, foundBeta := false, false
	for _, c := range caps {
		if strings.Contains(c, "alpha") {
			foundAlpha = true
		}
		if strings.Contains(c, "beta") {
			foundBeta = true
		}
	}
	if !foundAlpha {
		t.Errorf("aggregate caps missing alpha: %v", caps)
	}
	if !foundBeta {
		t.Errorf("aggregate caps missing beta: %v", caps)
	}

	// Execute against beta (dynamically added master).
	rid := NewMessageIdRandom()
	maxChunk := sw.Limits().MaxChunk
	frames := BuildRequestFrames(rid, capB, nil, maxChunk)
	for i := range frames {
		if err := sw.SendToMaster(&frames[i], nil); err != nil {
			t.Fatalf("SendToMaster: %v", err)
		}
	}

	got := readSwitchResponse(t, sw, rid)
	if string(got) != "beta" {
		t.Errorf("expected response %q, got %q", "beta", string(got))
	}
}

// TEST666: Preferred cap routing - routes to exact equivalent when multiple masters match
func Test666_preferred_cap_routing(t *testing.T) {
	// Master 0: exact-match handler (matches request exactly — closest specificity).
	capExact := `cap:in="media:void";test;out="media:void"`
	hostExact := NewInProcessCartridgeHost(
		InProcessHostIdentityForTest("exact-host"),
		[]HandlerRegistration{{
			Name:    "exact",
			Caps:    []cap.Cap{makeTestCap(t, capExact)},
			Handler: constHandler{value: "EXACT"},
		}},
	)

	// Master 1: more-specific handler (extra tag — also matches, but
	// further from the request).
	capExtra := `cap:in="media:void";test;ext=pdf;out="media:void"`
	hostExtra := NewInProcessCartridgeHost(
		InProcessHostIdentityForTest("extra-host"),
		[]HandlerRegistration{{
			Name:    "extra",
			Caps:    []cap.Cap{makeTestCap(t, capExtra)},
			Handler: constHandler{value: "EXTRA"},
		}},
	)

	spExact := wireInProcessHost(t, "test-master-0", hostExact)
	spExtra := wireInProcessHost(t, "test-master-1", hostExtra)

	sw, err := NewRelaySwitch([]SocketPair{spExact, spExtra})
	if err != nil {
		t.Fatalf("NewRelaySwitch: %v", err)
	}
	if got := len(sw.masters); got != 2 {
		t.Fatalf("masters len: got %d, want 2", got)
	}

	reqCap := `cap:in="media:void";test;out="media:void"`

	// Test 1: without preferred_cap, routes to exact match (closest specificity).
	rid1 := NewMessageIdFromUint(1)
	maxChunk := sw.Limits().MaxChunk
	frames1 := BuildRequestFrames(rid1, reqCap, nil, maxChunk)
	for i := range frames1 {
		if err := sw.SendToMaster(&frames1[i], nil); err != nil {
			t.Fatalf("SendToMaster (no preference): %v", err)
		}
	}
	resp1 := readSwitchResponse(t, sw, rid1)
	if string(resp1) != "EXACT" {
		t.Errorf("Without preferred_cap, should route to exact-match handler (closest specificity); got %q", string(resp1))
	}

	// Test 2: with preferred_cap = capExtra, routes to extra handler (preferred override).
	rid2 := NewMessageIdFromUint(2)
	frames2 := BuildRequestFrames(rid2, reqCap, nil, maxChunk)
	pref := capExtra
	for i := range frames2 {
		var p *string
		if frames2[i].FrameType == FrameTypeReq {
			p = &pref
		}
		if err := sw.SendToMaster(&frames2[i], p); err != nil {
			t.Fatalf("SendToMaster (with preference): %v", err)
		}
	}
	resp2 := readSwitchResponse(t, sw, rid2)
	if string(resp2) != "EXTRA" {
		t.Errorf("With preferred_cap, should route to extra handler (preferred override); got %q", string(resp2))
	}
}
