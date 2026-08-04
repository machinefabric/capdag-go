package bifaci

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// capGroupsFromUrns builds a single-group cap_groups slice from cap URN
// strings, mirroring the reference test helper. A URN that fails to parse is a
// test-fixture bug — fail hard.
func capGroupsFromUrns(t *testing.T, urns []string) []CapGroup {
	t.Helper()
	caps := make([]cap.Cap, 0, len(urns))
	for _, u := range urns {
		parsed, err := urn.NewCapUrnFromString(u)
		require.NoErrorf(t, err, "invalid cap URN in test fixture %q", u)
		caps = append(caps, *cap.NewCap(parsed, "test", []string{"test"}))
	}
	return []CapGroup{{Name: "test", Caps: caps}}
}

// readNotifyIDs reads frames from the engine side of the relay until a
// RelayNotify arrives, then returns the ids of the cartridges it advertises.
func readNotifyIDs(t *testing.T, r *FrameReader) []string {
	t.Helper()
	for {
		frame, err := r.ReadFrame()
		if err != nil {
			return nil
		}
		if frame.FrameType != FrameTypeRelayNotify {
			continue
		}
		manifest := frame.RelayNotifyManifest()
		var payload RelayNotifyCapabilitiesPayload
		require.NoError(t, json.Unmarshal(manifest, &payload))
		ids := make([]string, 0, len(payload.InstalledCartridges))
		for _, c := range payload.InstalledCartridges {
			ids = append(ids, c.Id)
		}
		return ids
	}
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// TEST1871: SyncRoster updates the LIVE host inventory in place — the engine sees an added registered-dir cartridge via a fresh RelayNotify without reconnecting, and a subsequent empty sync removes it. This is the macOS-XPC `syncDiscoveryOutcomes` parity path the daemon uses after a registry verdict flips a held cartridge to Listed.
func Test1871_sync_roster_adds_and_removes_registered_dir_live(t *testing.T) {
	// A valid registered-dir cartridge (hashable dir + cartridge.json).
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "cartridge.json"),
		[]byte(`{"name":"latejoiner","version":"1.0.0","channel":"release","registry_url":null,"entry":"bin","installed_at":"2026-01-01T00:00:00Z","installed_from":"dev"}`),
		0o644,
	))
	entry := filepath.Join(dir, "bin")
	require.NoError(t, os.WriteFile(entry, []byte("#!/bin/sh\n"), 0o755))

	host := NewCartridgeHost()
	handle := host.ProcessHandle()

	// Relay pipe pair: the engine side reads RelayNotify frames the host emits.
	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()

	type snapshots struct {
		initial     []string
		afterAdd    []string
		afterRemove []string
	}
	resultCh := make(chan snapshots, 1)

	// Engine side: collect the cartridge ids advertised across RelayNotify
	// frames, sending the two SyncRoster commands between reads.
	go func() {
		r := NewFrameReader(engineRead)

		// Initial RelayNotify (empty roster).
		initial := readNotifyIDs(t, r)

		// Add the cartridge live.
		_ = handle.SyncRoster([]RegisteredDirSpec{{
			EntryPoint:  entry,
			VersionDir:  dir,
			Id:          "latejoiner",
			Channel:     CartridgeChannelRelease,
			RegistryURL: nil,
			Version:     "1.0.0",
			CapGroups:   capGroupsFromUrns(t, []string{`cap:in="media:void";late;out="media:void"`}),
		}})
		afterAdd := readNotifyIDs(t, r)

		// Remove it again (empty roster).
		_ = handle.SyncRoster([]RegisteredDirSpec{})
		afterRemove := readNotifyIDs(t, r)

		// Drop the relay so the host's Run loop exits.
		engineWrite.Close()
		engineRead.Close()

		resultCh <- snapshots{initial: initial, afterAdd: afterAdd, afterRemove: afterRemove}
	}()

	// Drive the host until the engine side drops the relay.
	_ = host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()

	res := <-resultCh

	assert.False(t, containsString(res.initial, "latejoiner"),
		"cartridge must be absent before the sync; got %v", res.initial)
	assert.True(t, containsString(res.afterAdd, "latejoiner"),
		"SyncRoster must add the cartridge to the live inventory; got %v", res.afterAdd)
	assert.False(t, containsString(res.afterRemove, "latejoiner"),
		"an empty SyncRoster must retire the cartridge; got %v", res.afterRemove)
}

// retireFixture builds a registered-dir cartridge, marked running, for
// roster-retire tests. Mirrors the reference test helper.
func retireFixture(t *testing.T) (*CartridgeHost, string, string) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "cartridge.json"),
		[]byte(`{"name":"retiring","version":"1.0.0","channel":"release","registry_url":null,"entry":"bin","installed_at":"2026-01-01T00:00:00Z","installed_from":"dev"}`),
		0o644,
	))
	entry := filepath.Join(dir, "bin")
	require.NoError(t, os.WriteFile(entry, []byte("#!/bin/sh\n"), 0o755))

	host := NewCartridgeHost()
	host.RegisterCartridgeDir(RegisteredDirSpec{
		EntryPoint:  entry,
		VersionDir:  dir,
		Id:          "retiring",
		Channel:     CartridgeChannelRelease,
		RegistryURL: nil,
		Version:     "1.0.0",
		CapGroups:   capGroupsFromUrns(t, []string{`cap:in="media:void";out="media:void";retiring`}),
	})
	// Pretend it started: retirement only has to make a decision about a LIVE
	// process.
	host.cartridges[0].running = true
	return host, dir, entry
}

// TEST1945: a roster retire DRAINS a busy cartridge instead of killing it.
//
// The incident this pins: a transient registry outage shrank the roster and the
// host killed three cartridges outright, ERRing every request they were
// serving. Retirement means "no NEW work" — the process must survive until the
// requests it is already handling terminate.
func Test1945_roster_retire_drains_a_busy_cartridge_before_killing_it(t *testing.T) {
	host, _, _ := retireFixture(t)
	out := &relayOutbound{ch: make(chan *Frame, 64)}

	// One request in flight on this cartridge.
	host.incomingRxids[rxidKey{xid: "x1", rid: "r1"}] = incomingRoute{
		cartridgeIdx: 0,
		xid:          NewMessageIdFromUint(1),
		rid:          NewMessageIdFromUint(2),
	}

	host.syncRegisteredRoster([]RegisteredDirSpec{}, out)

	assert.True(t, host.cartridges[0].removed && host.cartridges[0].helloFailed,
		"a retired cartridge must leave the cap table and inventory immediately")
	assert.NotNil(t, host.cartridges[0].retiringSince,
		"a busy retired cartridge must be marked draining")
	assert.True(t, host.cartridges[0].running,
		"a cartridge mid-request must not be killed by a roster change")

	// Still busy → still alive.
	host.mu.Lock()
	host.reapDrainedCartridgesLocked()
	host.mu.Unlock()
	assert.True(t, host.cartridges[0].running)

	// The request terminates; the next reap collects it.
	delete(host.incomingRxids, rxidKey{xid: "x1", rid: "r1"})
	host.mu.Lock()
	host.reapDrainedCartridgesLocked()
	host.mu.Unlock()
	assert.False(t, host.cartridges[0].running,
		"a drained cartridge must be shut down once its last request ends")
	assert.Nil(t, host.cartridges[0].retiringSince)
}

// TEST1946: an IDLE cartridge is retired immediately (no reason to keep a
// process nothing routes to).
func Test1946_roster_retire_kills_an_idle_cartridge_as_retired(t *testing.T) {
	host, _, _ := retireFixture(t)
	out := &relayOutbound{ch: make(chan *Frame, 64)}

	host.syncRegisteredRoster([]RegisteredDirSpec{}, out)

	assert.Nil(t, host.cartridges[0].retiringSince)
	assert.False(t, host.cartridges[0].running)
	assert.True(t, host.cartridges[0].removed)
}

// TEST1947: a roster that flaps — retire then restore the same identity — keeps
// the SAME live process. This is the incident's shape end to end: the registry
// became unreachable, the roster shrank, and 26 seconds later it came back.
// Nothing about that sequence should cost a running cartridge, its warm model,
// or the work queued on it.
func Test1947_roster_flap_cancels_retirement_instead_of_respawning(t *testing.T) {
	host, dir, entry := retireFixture(t)
	out := &relayOutbound{ch: make(chan *Frame, 64)}
	spec := RegisteredDirSpec{
		EntryPoint:  entry,
		VersionDir:  dir,
		Id:          "retiring",
		Channel:     CartridgeChannelRelease,
		RegistryURL: nil,
		Version:     "1.0.0",
		CapGroups:   capGroupsFromUrns(t, []string{`cap:in="media:void";out="media:void";retiring`}),
	}

	// Busy, so the outage puts it into a drain rather than killing it.
	host.incomingRxids[rxidKey{xid: "x1", rid: "r1"}] = incomingRoute{
		cartridgeIdx: 0,
		xid:          NewMessageIdFromUint(1),
		rid:          NewMessageIdFromUint(2),
	}
	host.syncRegisteredRoster([]RegisteredDirSpec{}, out)
	require.NotNil(t, host.cartridges[0].retiringSince)

	// The registry answers again and the roster is restored.
	host.syncRegisteredRoster([]RegisteredDirSpec{spec}, out)

	assert.Len(t, host.cartridges, 1,
		"the restored identity must reuse the draining process, not spawn a second one")
	assert.Nil(t, host.cartridges[0].retiringSince)
	assert.False(t, host.cartridges[0].removed)
	assert.False(t, host.cartridges[0].helloFailed)
	assert.True(t, host.cartridges[0].running, "the process must never have been killed")

	// And it is not reaped afterwards.
	delete(host.incomingRxids, rxidKey{xid: "x1", rid: "r1"})
	host.mu.Lock()
	host.reapDrainedCartridgesLocked()
	host.mu.Unlock()
	assert.True(t, host.cartridges[0].running)
}
