//go:build !windows

package bifaci

import (
	"fmt"
	"os"
	"syscall"
)

var signalNames = map[syscall.Signal]string{
	syscall.SIGHUP:  "SIGHUP",
	syscall.SIGINT:  "SIGINT",
	syscall.SIGQUIT: "SIGQUIT",
	syscall.SIGILL:  "SIGILL",
	syscall.SIGTRAP: "SIGTRAP",
	syscall.SIGABRT: "SIGABRT",
	syscall.SIGBUS:  "SIGBUS",
	syscall.SIGFPE:  "SIGFPE",
	syscall.SIGKILL: "SIGKILL",
	syscall.SIGUSR1: "SIGUSR1",
	syscall.SIGSEGV: "SIGSEGV",
	syscall.SIGUSR2: "SIGUSR2",
	syscall.SIGPIPE: "SIGPIPE",
	syscall.SIGALRM: "SIGALRM",
	syscall.SIGTERM: "SIGTERM",
	syscall.SIGXCPU: "SIGXCPU",
	syscall.SIGXFSZ: "SIGXFSZ",
	syscall.SIGSYS:  "SIGSYS",
}

// describeExit says how a process ended: its exit code, or the signal that
// killed it.
func describeExit(state *os.ProcessState) string {
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		signal := status.Signal()
		named := fmt.Sprintf("killed by signal %d", int(signal))
		if name, known := signalNames[signal]; known {
			named = fmt.Sprintf("killed by signal %d (%s)", int(signal), name)
		}
		if status.CoreDump() {
			named += ", and dumped core"
		}
		return named
	}
	return fmt.Sprintf("exit code %d", state.ExitCode())
}
