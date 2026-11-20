//go:build windows

package bifaci

import "os"

// describeExit says how a process ended: its exit code, or the exception it
// ended with.
func describeExit(state *os.ProcessState) string {
	return describeWindowsExitCode(uint32(state.ExitCode()))
}
