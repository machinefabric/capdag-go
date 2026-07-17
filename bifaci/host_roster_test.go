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
