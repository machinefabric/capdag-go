package bifaci

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
			"aliases": ["test"],
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
			response.RoutingId = frame.RoutingId
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
				response.RoutingId = frame.RoutingId
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
				response.RoutingId = frame.RoutingId
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
				response.RoutingId = frame.RoutingId
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
				response.RoutingId = frame.RoutingId
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
		response.RoutingId = req.RoutingId
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
				response.RoutingId = frame.RoutingId
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
		reader := NewFrameReader(slaveRead2)
		// AddMaster now runs an end-to-end identity probe whenever the
		// reattaching host advertises caps (mirrors Rust add_master), so
		// the slave must answer it before the slot can come back healthy.
		if !serveRelayHandshake(t, reader, writer, []string{testCapIdentity}) {
			return
		}
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

// TEST487: RelaySwitch construction verifies identity through relay chain
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

		// Drain the COMPLETE identity request (REQ + STREAM_START + CHUNK(s) +
		// STREAM_END + END) before replying, exactly as a real master's reader
		// loop does — the verifier writes the whole request before reading the
		// response, so replying mid-request would deadlock the synchronous
		// transport. Then respond with ERR to model a broken identity handler.
		req, _ := drainIdentityRequest(t, reader)
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

// TEST136: All masters ready false when expected count unset
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

// TEST137: All masters ready false when partially connected
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

// TEST139: All masters ready true when masters connected but capless
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

// TEST140: All masters ready does not overshoot
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

// TEST132: add_master dynamically connects new host to running switch
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

// =========================================================================
// Deferred runtime identity probe / health-filtered routing / cap watch
// =========================================================================

// deferredIdentityNotify builds a populated RelayNotify manifest carrying
// capURNs under a single installed cartridge whose id is a FIXED
// "test-cartridge" (so inventory assertions can find it by id). Mirrors
// the populated notify in Rust's slave_deferred_identity helper.
func deferredIdentityNotify(capURNs []string) []byte {
	groupCaps := make([]map[string]interface{}, 0, len(capURNs))
	for _, u := range capURNs {
		groupCaps = append(groupCaps, map[string]interface{}{
			"urn":     u,
			"title":   "test",
			"aliases": ["test"],
			"args":    []interface{}{},
		})
	}
	manifest := map[string]interface{}{
		"installed_cartridges": []interface{}{
			map[string]interface{}{
				"registry_url": nil,
				"channel":      "release",
				"id":           "test-cartridge",
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
	data, _ := json.Marshal(manifest)
	return data
}

// serveDeferredIdentity is the Go port of Rust's slave_deferred_identity.
// It sends an EMPTY initial RelayNotify (so construction skips the
// synchronous identity probe and the master joins capless+healthy), then
// a populated RelayNotify carrying capURNs (the empty→non-empty transition
// the relay must re-verify), then answers the runtime identity probe: if
// succeed it echoes the probe nonce back verbatim on the probe's flow
// (probe passes → master flips healthy → caps routable); if !succeed it
// replies ERR (probe fails → master stays unhealthy, caps held back).
func serveDeferredIdentity(t *testing.T, reader *FrameReader, writer *FrameWriter, capURNs []string, succeed bool) {
	t.Helper()

	// 1. Empty initial RelayNotify — construction skips the probe.
	empty, _ := json.Marshal(map[string]interface{}{"installed_cartridges": []interface{}{}})
	if err := SendNotify(writer, empty, DefaultLimits()); err != nil {
		t.Errorf("serveDeferredIdentity: empty SendNotify: %v", err)
		return
	}

	// 2. Populated RelayNotify — the empty→non-empty transition.
	if err := SendNotify(writer, deferredIdentityNotify(capURNs), DefaultLimits()); err != nil {
		t.Errorf("serveDeferredIdentity: populated SendNotify: %v", err)
		return
	}

	// 3. Answer the runtime identity probe.
	var probeRid *MessageId
	var probeXid *MessageId
	var nonce []byte
	for {
		f, err := reader.ReadFrame()
		if err != nil || f == nil {
			return
		}
		switch f.FrameType {
		case FrameTypeReq:
			rid := f.Id
			probeRid = &rid
			probeXid = f.RoutingId
			if !succeed {
				errFrame := NewErr(f.Id, "BROKEN", "test cartridge")
				errFrame.RoutingId = f.RoutingId
				_ = writer.WriteFrame(errFrame)
				return
			}
		case FrameTypeChunk:
			nonce = append(nonce, f.Payload...)
		case FrameTypeEnd:
			// Echo the accumulated nonce back verbatim on the probe flow.
			if probeRid == nil {
				return
			}
			rid := *probeRid
			streamID := "identity-echo"
			ss := NewStreamStart(rid, streamID, "media:", nil)
			ss.RoutingId = probeXid
			checksum := ComputeChecksum(nonce)
			chunk := NewChunk(rid, streamID, 0, nonce, 0, checksum)
			chunk.RoutingId = probeXid
			se := NewStreamEnd(rid, streamID, 1)
			se.RoutingId = probeXid
			end := NewEnd(rid, nil)
			end.RoutingId = probeXid
			for _, fr := range []*Frame{ss, chunk, se, end} {
				if err := writer.WriteFrame(fr); err != nil {
					return
				}
			}
			return
		}
	}
}

// masterHealthAndError reads a slot's (healthy, lastError-present) under
// the switch lock so tests don't race the probe-driver goroutine.
func masterHealthAndError(sw *RelaySwitch, idx int) (bool, bool) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if idx < 0 || idx >= len(sw.masters) {
		return false, false
	}
	return sw.masters[idx].healthy, sw.masters[idx].lastError != nil
}

// TEST0131: When a master initially advertises empty caps (so the
// constructor skips the identity probe) and later sends a RelayNotify
// update with non-empty caps, the relay must run an end-to-end identity
// probe before the new caps become routable. A master that fails the
// runtime probe must end up unhealthy with lastError populated, and its
// caps must NOT appear in the cap_table.
func Test0131_runtime_identity_probe_required_on_empty_to_nonempty_transition(t *testing.T) {
	engineRead, slaveWrite := createSocketPair(t)
	slaveRead, engineWrite := createSocketPair(t)

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		serveDeferredIdentity(t, reader, writer, []string{
			testCapIdentity,
			`cap:in="media:void";test;out="media:void"`,
		}, false) // probe fails (ERR)
	}()

	sw, err := NewRelaySwitch([]SocketPair{
		{ID: "test-master-0", Read: engineRead, Write: engineWrite},
	})
	if err != nil {
		t.Fatalf("construction must succeed for empty-cap initial notify: %v", err)
	}
	sw.StartBackgroundPump()

	// Poll until the runtime probe path marks the master unhealthy with a
	// recorded lastError.
	deadline := time.Now().Add(15 * time.Second)
	unhealthy := false
	for time.Now().Before(deadline) {
		if healthy, hasErr := masterHealthAndError(sw, 0); !healthy && hasErr {
			unhealthy = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !unhealthy {
		t.Fatalf("master must be marked unhealthy after the runtime identity probe fails")
	}

	// The master's caps must NOT be routable: routing is health-filtered,
	// so dispatch to the unverified master's cap must fail. Proven via the
	// dispatch path, not by string-comparing URNs.
	sw.mu.Lock()
	idx, ferr := sw.findMasterForCap(`cap:in="media:void";test;out="media:void"`, nil)
	sw.mu.Unlock()
	if ferr == nil {
		t.Fatalf("unverified master's cap must NOT be routable, but findMasterForCap returned master %d", idx)
	}
}

// TEST0135: the runtime identity probe SUCCESS path — a master that
// advertises caps AFTER connecting (empty→non-empty) and then passes the
// probe must flip healthy and its caps must become routable.
func Test0135_runtime_identity_probe_success_makes_caps_routable(t *testing.T) {
	engineRead, slaveWrite := createSocketPair(t)
	slaveRead, engineWrite := createSocketPair(t)

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		serveDeferredIdentity(t, reader, writer, []string{
			testCapIdentity,
			`cap:in="media:void";test;out="media:void"`,
		}, true) // probe succeeds
	}()

	sw, err := NewRelaySwitch([]SocketPair{
		{ID: "test-master-0", Read: engineRead, Write: engineWrite},
	})
	if err != nil {
		t.Fatalf("construction must succeed for empty-cap initial notify: %v", err)
	}
	sw.StartBackgroundPump()

	// Poll until the probe driver completes the round-trip: the post-init
	// advertised cap becomes routable only AFTER the probe passes.
	deadline := time.Now().Add(15 * time.Second)
	routable := false
	for time.Now().Before(deadline) {
		sw.mu.Lock()
		idx, ferr := sw.findMasterForCap(`cap:in="media:void";test;out="media:void"`, nil)
		sw.mu.Unlock()
		if ferr == nil && idx == 0 {
			routable = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !routable {
		t.Fatalf("after a successful runtime identity probe the master's post-init advertised cap must become routable")
	}
	if healthy, _ := masterHealthAndError(sw, 0); !healthy {
		t.Fatalf("master must be marked healthy after a successful runtime identity probe")
	}
}

// TEST0138: the installed-cartridge INVENTORY is NOT health-filtered. A
// master held unhealthy by a failed runtime identity probe still has its
// cartridges visible in the aggregate inventory (so a transient master
// flap does not make cartridges "disappear" from the engine's view), even
// though its caps are excluded from ROUTING.
func Test0138_unhealthy_master_inventory_retained_but_not_routable(t *testing.T) {
	engineRead, slaveWrite := createSocketPair(t)
	slaveRead, engineWrite := createSocketPair(t)

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		serveDeferredIdentity(t, reader, writer, []string{
			testCapIdentity,
			`cap:in="media:void";test;out="media:void"`,
		}, false) // probe fails → master stays unhealthy
	}()

	sw, err := NewRelaySwitch([]SocketPair{
		{ID: "test-master-0", Read: engineRead, Write: engineWrite},
	})
	if err != nil {
		t.Fatalf("construction must succeed for empty-cap initial notify: %v", err)
	}
	sw.StartBackgroundPump()

	deadline := time.Now().Add(15 * time.Second)
	unhealthy := false
	for time.Now().Before(deadline) {
		if healthy, hasErr := masterHealthAndError(sw, 0); !healthy && hasErr {
			unhealthy = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !unhealthy {
		t.Fatalf("master must be unhealthy after the probe fails")
	}

	// ROUTING: the unhealthy master's caps are excluded.
	sw.mu.Lock()
	_, ferr := sw.findMasterForCap(`cap:in="media:void";test;out="media:void"`, nil)
	sw.mu.Unlock()
	if ferr == nil {
		t.Fatalf("an unhealthy master's caps must NOT be routable")
	}

	// INVENTORY: the cartridge is STILL visible — inventory is NOT
	// health-filtered.
	inventory := sw.InstalledCartridges()
	found := false
	for _, c := range inventory {
		if c.Id == "test-cartridge" {
			found = true
			break
		}
	}
	if !found {
		ids := make([]string, 0, len(inventory))
		for _, c := range inventory {
			ids = append(ids, c.Id)
		}
		t.Fatalf("an unhealthy master's installed cartridges must remain visible in the inventory aggregate, got: %v", ids)
	}
}

// TEST0141: the routable-capability watch (SubscribeCapabilities). A
// subscriber must receive the CURRENT routable cap set on subscribe even
// though it was rebuilt during construction — BEFORE any receiver existed
// (the watch must persist the value, i.e. send_replace semantics). The
// delivered snapshot must be the health-filtered routable set.
func Test0141_subscribe_capabilities_delivers_routable_set(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		if !serveRelayHandshake(t, reader, writer, []string{
			testCapIdentity,
			`cap:in="media:void";test;out="media:void"`,
		}) {
			return
		}
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	// Capabilities are rebuilt inside NewRelaySwitch — before we subscribe.
	sw, err := NewRelaySwitch([]SocketPair{
		{ID: "test-master-0", Read: engineRead, Write: engineWrite},
	})
	if err != nil {
		t.Fatalf("NewRelaySwitch: %v", err)
	}

	rx := sw.SubscribeCapabilities()
	watched := rx.Borrow()

	// Snapshot-identity check: the watch must mirror the synchronous
	// getter (same bytes from the same source). This catches the
	// send-vs-send_replace bug: the snapshot is rebuilt before any
	// subscriber exists, so the watch must persist it.
	if !bytes.Equal(watched, sw.Capabilities()) {
		t.Fatalf("the capability watch must deliver the same routable-set snapshot as Capabilities()\n watch=%s\n  cap=%s", watched, sw.Capabilities())
	}

	// Prove the snapshot is the live ROUTABLE set via dispatch conformance
	// (NOT a URN string comparison): a healthy, verified master makes its
	// advertised cap dispatchable.
	sw.mu.Lock()
	idx, ferr := sw.findMasterForCap(`cap:in="media:void";test;out="media:void"`, nil)
	sw.mu.Unlock()
	if ferr != nil || idx != 0 {
		t.Fatalf("the routable set the watch delivers must make the master's advertised cap dispatchable; got master %d (err=%v)", idx, ferr)
	}
}

// TEST0142: AddMaster runs an end-to-end identity probe on reattach
// whenever the host advertises caps (mirrors Rust add_master). When the
// reattaching host FAILS the probe, the master rejoins as UNHEALTHY — its
// installed cartridges stay visible in the inventory aggregate while its
// caps are held out of the routing table — rather than the reattach
// erroring out.
func Test0142_add_master_reattach_verifies_identity(t *testing.T) {
	// Initial healthy master.
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()
	testCap := `cap:in="media:void";test;out="media:void"`
	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		if !serveRelayHandshake(t, reader, writer, []string{testCapIdentity, testCap}) {
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

	// Kill the slot so reattach takes the in-place branch.
	sw.mu.Lock()
	sw.handleMasterDeath(0)
	sw.mu.Unlock()

	// Reconnect under the SAME id with a slave that advertises caps but
	// FAILS the identity probe (replies ERR).
	engineRead2, slaveWrite2 := net.Pipe()
	slaveRead2, engineWrite2 := net.Pipe()
	go func() {
		reader := NewFrameReader(slaveRead2)
		writer := NewFrameWriter(slaveWrite2)
		manifest := testManifestWithCaps([]string{testCapIdentity, testCap})
		manifestJSON, _ := json.Marshal(manifest)
		if err := SendNotify(writer, manifestJSON, DefaultLimits()); err != nil {
			t.Errorf("slave2 SendNotify: %v", err)
			return
		}
		// Answer the probe with ERR after draining the full request.
		req, _ := drainIdentityRequest(t, reader)
		errFrame := NewErr(req.Id, "BROKEN", "identity verification broken")
		errFrame.RoutingId = req.RoutingId
		_ = writer.WriteFrame(errFrame)
		_, _ = reader.ReadFrame()
	}()

	idx, err := sw.AddMaster(SocketPair{ID: "xpc-service", Read: engineRead2, Write: engineWrite2})
	if err != nil {
		t.Fatalf("AddMaster reattach must NOT error on identity failure — the master rejoins unhealthy: %v", err)
	}
	if idx != 0 {
		t.Fatalf("reattach must return the same slot index 0, got %d", idx)
	}

	// The reattached master is UNHEALTHY with a recorded lastError.
	if healthy, hasErr := masterHealthAndError(sw, 0); healthy || !hasErr {
		t.Fatalf("reattached master must be unhealthy with lastError after a failed identity probe (healthy=%v, hasErr=%v)", healthy, hasErr)
	}

	// ROUTING: its caps are NOT routable.
	sw.mu.Lock()
	_, ferr := sw.findMasterForCap(testCap, nil)
	sw.mu.Unlock()
	if ferr == nil {
		t.Fatalf("an unhealthy reattached master's caps must NOT be routable")
	}

	// INVENTORY: its cartridges remain visible.
	if len(sw.InstalledCartridges()) == 0 {
		t.Fatalf("an unhealthy reattached master's installed cartridges must remain visible in the inventory aggregate")
	}
}

// =============================================================================
// Protocol v3: unified RequestTable, credit forwarding, counted no_route
// drops, protocol_stats(), and host protocol stats retention.
// =============================================================================

// TEST7085: The RelayNotify capabilities payload carries the host's
// protocol stats snapshot, surviving the wire round-trip.
func Test7085_relay_notify_carries_host_protocol_stats(t *testing.T) {
	counters := NewDropCounters()
	counters.Record(DropReasonNoRoute)
	counters.Record(DropReasonNoRoute)
	stats := HostProtocolStats{
		Drops:                 counters.Snapshot(),
		OutgoingRids:          3,
		IncomingRxids:         5,
		RoutingGcRunsTotal:    2,
		RoutingGcEvictedTotal: 7,
	}
	payload := RelayNotifyCapabilitiesPayload{InstalledCartridges: []InstalledCartridgeRecord{}}.
		WithHostProtocolStats(stats)
	wireBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	parsed, err := parseRelayNotifyPayload(wireBytes)
	if err != nil {
		t.Fatalf("payload must parse: %v", err)
	}
	got := parsed.HostProtocolStats
	if got == nil {
		t.Fatalf("host stats must survive the round trip")
	}
	if got.Drops.Total != 2 {
		t.Errorf("expected drops.total=2, got %d", got.Drops.Total)
	}
	if got.Drops.ByReason["no_route"] != 2 {
		t.Errorf("expected drops.by_reason[no_route]=2, got %v", got.Drops.ByReason)
	}
	if got.IncomingRxids != 5 {
		t.Errorf("expected incoming_rxids=5, got %d", got.IncomingRxids)
	}
	if got.RoutingGcEvictedTotal != 7 {
		t.Errorf("expected routing_gc_evicted_total=7, got %d", got.RoutingGcEvictedTotal)
	}

	// A payload WITHOUT stats (initial capability advertisement) still
	// parses — the field is a per-republish refresh, not a requirement.
	bare := RelayNotifyCapabilitiesPayload{InstalledCartridges: []InstalledCartridgeRecord{}}
	bareBytes, err := json.Marshal(bare)
	if err != nil {
		t.Fatalf("marshal bare payload: %v", err)
	}
	parsedBare, err := parseRelayNotifyPayload(bareBytes)
	if err != nil {
		t.Fatalf("bare payload parses: %v", err)
	}
	if parsedBare.HostProtocolStats != nil {
		t.Errorf("expected no host stats on bare payload, got %+v", parsedBare.HostProtocolStats)
	}
}

// TEST7093: A response frame for a LIVE request whose external consumer is
// gone (dropped/timed-out caller future) is a counted channel_closed drop
// AND cancels the request upstream — the destination receives Cancel, the
// entry terminates as cancelled, and the cartridge stops producing for a
// dead channel instead of running to completion against it.
//
// This mirror has no execute_cap-style external-caller API (see
// RelaySwitch.requests' doc comment), so the registration execute_cap would
// perform — response channel + routing registered atomically BEFORE
// sending, then the REQ written to the destination master — is done by
// hand here, mirroring what runIdentityProbeViaRelay already does for the
// deferred-probe's own external registration.
func Test7093_dead_consumer_cancels_upstream(t *testing.T) {
	const capURN = "cap:effect=none"
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	type slaveOutcome struct {
		cancelID  MessageId
		sawCancel bool
		err       error
	}
	resultCh := make(chan slaveOutcome, 1)

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		if !serveRelayHandshake(t, reader, writer, []string{capURN}) {
			resultCh <- slaveOutcome{err: fmt.Errorf("handshake failed")}
			return
		}

		// Serve one REQ: read it, then stream a non-terminal response
		// frame. The engine-side consumer will already be gone — the
		// switch must answer with Cancel on this connection.
		var req *Frame
		for {
			f, err := reader.ReadFrame()
			if err != nil || f == nil {
				resultCh <- slaveOutcome{err: fmt.Errorf("expected REQ: %v", err)}
				return
			}
			if f.FrameType == FrameTypeReq {
				req = f
				break
			}
		}
		logFrame := NewLog(req.Id, "info", "first result row")
		logFrame.RoutingId = req.RoutingId
		if err := writer.WriteFrame(logFrame); err != nil {
			resultCh <- slaveOutcome{err: fmt.Errorf("write log: %v", err)}
			return
		}

		// The switch must now cancel this request (dead consumer).
		for {
			f, err := reader.ReadFrame()
			if err != nil || f == nil {
				resultCh <- slaveOutcome{err: fmt.Errorf("expected Cancel: %v", err)}
				return
			}
			if f.FrameType == FrameTypeCancel {
				resultCh <- slaveOutcome{sawCancel: true, cancelID: f.Id}
				return
			}
		}
	}()

	sw, err := NewRelaySwitch([]SocketPair{{ID: "test-master-0", Read: engineRead, Write: engineWrite}})
	if err != nil {
		t.Fatalf("NewRelaySwitch: %v", err)
	}
	sw.StartBackgroundPump()

	rid := NewMessageIdRandom()
	xid := NewMessageIdFromUint(atomic.AddUint64(&sw.xidCounter, 1))
	key := RequestKey{Xid: xid, Rid: rid}
	// Unbuffered and deliberately never read: simulates the caller
	// abandoning the request (a dropped/timed-out future) — the switch's
	// non-blocking deliverExternal send fails immediately, exactly the
	// "dead consumer" condition under test.
	ch := make(chan Frame)
	registeredCap := capURN

	sw.mu.Lock()
	state := NewRequestState(
		RequestRoutingEntry{SourceMasterIdx: nil, DestinationMasterIdx: 0},
		nil,
		ch,
		false,
	).WithCapUrn(&registeredCap)
	if regErr := sw.requests.Register(key, state); regErr != nil {
		sw.mu.Unlock()
		t.Fatalf("register: %v", regErr)
	}
	req := NewReq(rid, capURN, []byte{}, "application/cbor")
	req.RoutingId = &xid
	if werr := sw.masters[0].socketWriter.WriteFrame(req); werr != nil {
		sw.mu.Unlock()
		t.Fatalf("write REQ: %v", werr)
	}
	sw.mu.Unlock()

	// The caller abandons the request: nobody will ever read `ch`.

	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("slave error: %v", res.err)
		}
		if !res.sawCancel {
			t.Fatalf("expected Cancel frame")
		}
		if res.cancelID.ToString() != rid.ToString() {
			t.Fatalf("cancel targets the abandoned request: got %s want %s", res.cancelID.ToString(), rid.ToString())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slave must observe Cancel before timeout")
	}

	stats := sw.ProtocolStats()
	if got := stats.Drops.ByReason["channel_closed"]; got != 1 {
		t.Errorf("the abandoned frame is a counted channel_closed drop: got %d (%v)", got, stats.Drops.ByReason)
	}
	if got := stats.Requests.TerminatedByKind["cancelled"]; got != 1 {
		t.Errorf("the abandoned request terminates as cancelled — it never lingers: got %d", got)
	}
	if len(stats.Requests.Active) != 0 {
		t.Errorf("no state remains for the abandoned request (L7): %+v", stats.Requests.Active)
	}
}

// TEST7091: Host protocol stats carried by a master's RelayNotify are
// RETAINED by the switch (not parsed-and-discarded) and surface in
// ProtocolStats().Hosts keyed by master id; a master that has not yet
// advertised stats is absent from the map — never a zeroed placeholder.
func Test7091_switch_retains_host_protocol_stats_from_relay_notify(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		if !serveRelayHandshake(t, reader, writer, []string{testCapIdentity}) {
			return
		}

		// Republish the SAME inventory (no cap change → no re-verify),
		// now carrying host protocol stats — the periodic refresh path.
		manifest := testManifestWithCaps([]string{testCapIdentity})
		manifest["host_protocol_stats"] = map[string]interface{}{
			"drops": map[string]interface{}{
				"total": 3,
				"by_reason": map[string]interface{}{
					"post_terminal": 2,
					"no_route":      1,
				},
			},
			"outgoing_rids":            4,
			"incoming_rxids":           6,
			"incoming_to_peer_rids":    0,
			"outgoing_max_seq":         2,
			"routing_gc_runs_total":    1,
			"routing_gc_evicted_total": 9,
		}
		manifestJSON, err := json.Marshal(manifest)
		if err != nil {
			t.Errorf("marshal republish manifest: %v", err)
			return
		}
		if err := SendNotify(writer, manifestJSON, DefaultLimits()); err != nil {
			t.Errorf("republish SendNotify: %v", err)
			return
		}
		// Keep the connection open until the assertion side finishes.
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	sw, err := NewRelaySwitch([]SocketPair{{ID: "test-master-0", Read: engineRead, Write: engineWrite}})
	if err != nil {
		t.Fatalf("NewRelaySwitch: %v", err)
	}

	// The initial advertisement carried no host stats: absent, not zeroed.
	if hosts := sw.ProtocolStats().Hosts; len(hosts) != 0 {
		t.Fatalf("no host stats before a RelayNotify carries them, got %+v", hosts)
	}

	sw.StartBackgroundPump()

	deadline := time.Now().Add(2 * time.Second)
	var got HostProtocolStats
	found := false
	for time.Now().Before(deadline) {
		if h, ok := sw.ProtocolStats().Hosts["test-master-0"]; ok {
			got = h
			found = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !found {
		t.Fatalf("host stats must surface in ProtocolStats().Hosts after RelayNotify")
	}

	if got.Drops.Total != 3 {
		t.Errorf("expected drops.total=3, got %d", got.Drops.Total)
	}
	if got.Drops.ByReason["post_terminal"] != 2 {
		t.Errorf("expected drops.by_reason[post_terminal]=2, got %v", got.Drops.ByReason)
	}
	if got.IncomingRxids != 6 {
		t.Errorf("expected incoming_rxids=6, got %d", got.IncomingRxids)
	}
	if got.RoutingGcEvictedTotal != 9 {
		t.Errorf("expected routing_gc_evicted_total=9, got %d", got.RoutingGcEvictedTotal)
	}
}

// TEST7025: A flow frame for a request with no routing state is a counted
// no_route drop — not a protocol error and not a silent loss — observable
// in the protocol stats snapshot.
//
// The Rust reference constructs an empty (zero-master) RelaySwitch for
// this test; this Go mirror's NewRelaySwitch hard-rejects an empty
// cardinality list (a pre-existing, unrelated divergence — see TEST432),
// so a single capless connected master stands in. Neither dropped frame
// below references the master (both fail the routing-table lookup before
// any master index is touched), so the substitution changes nothing about
// the behavior under test.
func Test7025_unroutable_flow_frame_is_counted_drop(t *testing.T) {
	sw := buildSwitchWithNCaplessMasters(t, 1)

	// Response continuation (has XID) for a key that was never registered
	// (or already terminated): must be dropped + counted, never an error.
	orphan := NewProgress(NewMessageIdRandom(), 0.5, "orphan")
	orphanXid := NewMessageIdFromUint(999)
	orphan.RoutingId = &orphanXid
	sw.mu.Lock()
	result, err := sw.handleMasterFrame(0, orphan)
	sw.mu.Unlock()
	if err != nil {
		t.Fatalf("unroutable frame must not surface as an error (L6): %v", err)
	}
	if result != nil {
		t.Fatalf("nothing to deliver, got %+v", result)
	}

	// Request continuation (no XID) for an unknown RID: same law.
	chunk := newFrame(FrameTypeChunk, NewMessageIdRandom())
	streamID := "s"
	chunk.StreamId = &streamID
	zero := uint64(0)
	chunk.ChunkIndex = &zero
	chunk.Checksum = &zero
	sw.mu.Lock()
	result, err = sw.handleMasterFrame(0, chunk)
	sw.mu.Unlock()
	if err != nil {
		t.Fatalf("unknown request continuation must not error: %v", err)
	}
	if result != nil {
		t.Fatalf("nothing to deliver, got %+v", result)
	}

	stats := sw.ProtocolStats()
	if got := stats.Drops.ByReason["no_route"]; got != 2 {
		t.Fatalf("both drops counted, exactly once each (L8): got %d, %+v", got, stats.Drops)
	}
	if len(stats.Requests.Active) != 0 {
		t.Fatalf("expected no active requests, got %+v", stats.Requests.Active)
	}
}

// TEST7035: After END, the switch holds zero state for the request —
// entry, rid index, and response channel all released atomically, with
// the terminal delivered and a terminated summary recorded.
//
// Uses a single capless connected master in place of the Rust reference's
// zero-master switch — see TEST7025's doc comment.
func Test7035_end_terminates_and_releases_all_state(t *testing.T) {
	sw := buildSwitchWithNCaplessMasters(t, 1)

	xid := NewMessageIdFromUint(11)
	rid := NewMessageIdRandom()
	key := RequestKey{Xid: xid, Rid: rid}
	ch := make(chan Frame, 1)
	sw.mu.Lock()
	if err := sw.requests.Register(key, NewRequestState(
		RequestRoutingEntry{SourceMasterIdx: nil, DestinationMasterIdx: 0},
		nil,
		ch,
		false,
	)); err != nil {
		sw.mu.Unlock()
		t.Fatalf("registration must succeed: %v", err)
	}
	sw.mu.Unlock()

	if got := len(sw.ProtocolStats().Requests.Active); got != 1 {
		t.Fatalf("expected 1 active request, got %d", got)
	}

	// Terminal END arrives from the master side.
	progress := 1.0
	end := EndOkWith(rid, nil, &progress, nil)
	end.RoutingId = &xid
	sw.mu.Lock()
	_, err := sw.handleMasterFrame(0, end)
	sw.mu.Unlock()
	if err != nil {
		t.Fatalf("terminal must route: %v", err)
	}

	// The terminal was DELIVERED to the waiting channel...
	select {
	case delivered := <-ch:
		if delivered.FrameType != FrameTypeEnd {
			t.Fatalf("expected END frame, got %d", delivered.FrameType)
		}
		if got := delivered.FinalProgress(); got == nil || *got != 1.0 {
			t.Fatalf("expected final progress 1.0, got %v", got)
		}
	default:
		t.Fatalf("END must reach the response channel")
	}

	// ...and zero state remains (L7), with the lifecycle recorded.
	stats := sw.ProtocolStats()
	if len(stats.Requests.Active) != 0 {
		t.Fatalf("no live entry after END, got %+v", stats.Requests.Active)
	}
	if got := stats.Requests.TerminatedByKind["end"]; got != 1 {
		t.Fatalf("expected 1 end termination, got %d", got)
	}
	if len(stats.Requests.RecentTerminated) == 0 {
		t.Fatalf("terminated summary must be recorded")
	}
	summary := stats.Requests.RecentTerminated[len(stats.Requests.RecentTerminated)-1]
	if summary.Rid != rid.ToString() {
		t.Fatalf("expected summary.rid=%s, got %s", rid.ToString(), summary.Rid)
	}
	if summary.FramesIn != 1 {
		t.Fatalf("ingress recording captured the terminal frame: expected frames_in=1, got %d", summary.FramesIn)
	}

	// A follow-up frame for the released key is a counted no_route drop.
	late := NewProgress(rid, 1.0, "late")
	late.RoutingId = &xid
	sw.mu.Lock()
	_, err = sw.handleMasterFrame(0, late)
	sw.mu.Unlock()
	if err != nil {
		t.Fatalf("post-terminal frame must not error: %v", err)
	}
	if got := sw.ProtocolStats().Drops.ByReason["no_route"]; got != 1 {
		t.Fatalf("expected 1 no_route drop, got %d", got)
	}
}

// TEST7036: After ERR, the same total-cleanup invariant holds as after
// END, with kind err.
func Test7036_err_terminates_and_releases_all_state(t *testing.T) {
	sw := buildSwitchWithNCaplessMasters(t, 1)

	xid := NewMessageIdFromUint(21)
	rid := NewMessageIdRandom()
	key := RequestKey{Xid: xid, Rid: rid}
	ch := make(chan Frame, 1)
	sw.mu.Lock()
	if err := sw.requests.Register(key, NewRequestState(
		RequestRoutingEntry{SourceMasterIdx: nil, DestinationMasterIdx: 0},
		nil,
		ch,
		false,
	)); err != nil {
		sw.mu.Unlock()
		t.Fatalf("registration must succeed: %v", err)
	}
	sw.mu.Unlock()

	errFrame := NewErr(rid, "HANDLER_ERROR", "boom")
	errFrame.RoutingId = &xid
	sw.mu.Lock()
	_, err := sw.handleMasterFrame(0, errFrame)
	sw.mu.Unlock()
	if err != nil {
		t.Fatalf("handleMasterFrame: %v", err)
	}

	select {
	case delivered := <-ch:
		if delivered.FrameType != FrameTypeErr {
			t.Fatalf("expected ERR frame, got %d", delivered.FrameType)
		}
		if delivered.ErrorCode() != "HANDLER_ERROR" {
			t.Fatalf("expected error code HANDLER_ERROR, got %q", delivered.ErrorCode())
		}
	default:
		t.Fatalf("ERR must reach the channel")
	}

	stats := sw.ProtocolStats()
	if len(stats.Requests.Active) != 0 {
		t.Fatalf("expected no active requests, got %+v", stats.Requests.Active)
	}
	if got := stats.Requests.TerminatedByKind["err"]; got != 1 {
		t.Fatalf("expected 1 err termination, got %d", got)
	}
}

// TEST7037: Cancelling a request terminates it AND its recursively-linked
// peer children — Cancel frames reach the destination, waiting channels
// get ERR CANCELLED, and zero state remains for parent or child.
func Test7037_cancel_cascades_to_children_and_cleans_all_state(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	cancelsCh := make(chan []MessageId, 1)
	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		if !serveRelayHandshake(t, reader, writer, []string{testCapIdentity}) {
			cancelsCh <- nil
			return
		}
		var cancels []MessageId
		for len(cancels) < 2 {
			f, err := reader.ReadFrame()
			if err != nil || f == nil {
				break
			}
			if f.FrameType == FrameTypeCancel {
				cancels = append(cancels, f.Id)
			}
		}
		cancelsCh <- cancels
	}()

	sw, err := NewRelaySwitch([]SocketPair{{ID: "test-master-0", Read: engineRead, Write: engineWrite}})
	if err != nil {
		t.Fatalf("switch with one master must construct: %v", err)
	}

	// Parent (engine-origin, has a waiting channel) + child peer call.
	parentXid := NewMessageIdFromUint(1)
	parentRid := NewMessageIdRandom()
	parentKey := RequestKey{Xid: parentXid, Rid: parentRid}
	childXid := NewMessageIdFromUint(2)
	childRid := NewMessageIdRandom()
	childKey := RequestKey{Xid: childXid, Rid: childRid}

	pch := make(chan Frame, 1)
	srcIdx := 0
	sw.mu.Lock()
	if err := sw.requests.Register(parentKey, NewRequestState(
		RequestRoutingEntry{SourceMasterIdx: nil, DestinationMasterIdx: 0},
		nil,
		pch,
		false,
	)); err != nil {
		sw.mu.Unlock()
		t.Fatalf("register parent: %v", err)
	}
	if err := sw.requests.Register(childKey, NewRequestState(
		RequestRoutingEntry{SourceMasterIdx: &srcIdx, DestinationMasterIdx: 0},
		&srcIdx,
		nil,
		true,
	)); err != nil {
		sw.mu.Unlock()
		t.Fatalf("register child: %v", err)
	}
	sw.requests.LinkChild(parentKey, childKey)
	sw.mu.Unlock()

	sw.CancelRequest(parentRid, false)

	// Parent's waiter observes ERR CANCELLED.
	select {
	case delivered := <-pch:
		if delivered.ErrorCode() != "CANCELLED" {
			t.Fatalf("expected error code CANCELLED, got %q", delivered.ErrorCode())
		}
	default:
		t.Fatalf("parent channel gets ERR")
	}

	// Both parent and child are fully released (L7), recorded cancelled.
	stats := sw.ProtocolStats()
	if len(stats.Requests.Active) != 0 {
		t.Fatalf("no state for parent or child remains: %+v", stats.Requests.Active)
	}
	if got := stats.Requests.TerminatedByKind["cancelled"]; got != 2 {
		t.Fatalf("expected 2 cancelled terminations, got %d", got)
	}

	// The destination master received Cancel for BOTH rids.
	var cancels []MessageId
	select {
	case cancels = <-cancelsCh:
	case <-time.After(5 * time.Second):
		t.Fatal("slave task timed out")
	}
	if len(cancels) != 2 {
		t.Fatalf("expected parent + cascaded child Cancel frames, got %d", len(cancels))
	}
	foundParent, foundChild := false, false
	for _, id := range cancels {
		if id.ToString() == parentRid.ToString() {
			foundParent = true
		}
		if id.ToString() == childRid.ToString() {
			foundChild = true
		}
	}
	if !foundParent || !foundChild {
		t.Fatalf("expected cancels for both parent and child rids, got %v", cancels)
	}
}

// TEST7038: Master death terminates every request routed to it with kind
// master_died, delivering synthetic MASTER_DIED ERRs to waiting channels
// and leaving zero state.
func Test7038_master_death_terminates_pending_requests(t *testing.T) {
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()

	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		if !serveRelayHandshake(t, reader, writer, []string{testCapIdentity}) {
			return
		}
		// Keep the connection alive until the test drops it.
		for {
			if _, err := reader.ReadFrame(); err != nil {
				return
			}
		}
	}()

	sw, err := NewRelaySwitch([]SocketPair{{ID: "test-master-0", Read: engineRead, Write: engineWrite}})
	if err != nil {
		t.Fatalf("switch with one master must construct: %v", err)
	}

	xid := NewMessageIdFromUint(5)
	rid := NewMessageIdRandom()
	key := RequestKey{Xid: xid, Rid: rid}
	ch := make(chan Frame, 1)
	sw.mu.Lock()
	if err := sw.requests.Register(key, NewRequestState(
		RequestRoutingEntry{SourceMasterIdx: nil, DestinationMasterIdx: 0},
		nil,
		ch,
		false,
	)); err != nil {
		sw.mu.Unlock()
		t.Fatalf("register: %v", err)
	}
	sw.handleMasterDeath(0)
	sw.mu.Unlock()

	select {
	case delivered := <-ch:
		if delivered.ErrorCode() != "MASTER_DIED" {
			t.Fatalf("expected error code MASTER_DIED, got %q", delivered.ErrorCode())
		}
	default:
		t.Fatalf("synthetic ERR must be delivered")
	}

	stats := sw.ProtocolStats()
	if len(stats.Requests.Active) != 0 {
		t.Fatalf("zero state remains (L7), got %+v", stats.Requests.Active)
	}
	if got := stats.Requests.TerminatedByKind["master_died"]; got != 1 {
		t.Fatalf("expected 1 master_died termination, got %d", got)
	}
	if len(stats.Requests.RecentTerminated) == 0 {
		t.Fatalf("expected a recorded terminated summary")
	}
	summary := stats.Requests.RecentTerminated[len(stats.Requests.RecentTerminated)-1]
	if summary.Rid != rid.ToString() {
		t.Fatalf("expected summary.rid=%s, got %s", rid.ToString(), summary.Rid)
	}
}
