package bifaci

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// exitWaitLimit is how long a cartridge whose output ended on its own is
// given to exit before the host takes it to be still running. A crashing
// process closes its output before it can be collected, so the end of the
// stream usually arrives first; asked at once, a crash read as a process
// still running. Mirrors the reference host's EXIT_WAIT_LIMIT.
const exitWaitLimit = 5 * time.Second

// stderrDrainLimit is how long a dead cartridge's stderr is read for after
// its process ended. The stream ends with the process, unless a process it
// started still holds it open.
const stderrDrainLimit = 500 * time.Millisecond

type streamEndKind int

const (
	// streamClosed: the stream ended between two frames.
	streamClosed streamEndKind = iota
	// streamBroken: a read failed, or the stream ended part way through a frame.
	streamBroken
	// streamMalformed: the cartridge wrote something that is not a frame.
	streamMalformed
)

// streamEnd is how a cartridge's output stream ended, as its reader saw it.
type streamEnd struct {
	kind streamEndKind
	err  string
}

func streamEndFromReadError(err error) streamEnd {
	var malformed *MalformedFrameError
	switch {
	case err == io.EOF:
		return streamEnd{kind: streamClosed}
	case errors.As(err, &malformed):
		return streamEnd{kind: streamMalformed, err: err.Error()}
	default:
		return streamEnd{kind: streamBroken, err: err.Error()}
	}
}

type processEndKind int

const (
	// processNone: the cartridge has no process (an attached one).
	processNone processEndKind = iota
	// processExited: it exited, with state.
	processExited
	// processKilled: it was still running when the host gave up on it, and
	// the host killed it.
	processKilled
	// processUnknown: whether it had exited could not be asked.
	processUnknown
)

// processEnd is how a dead cartridge's process ended.
type processEnd struct {
	kind  processEndKind
	state *os.ProcessState
	err   string
}

// awaitExit waits up to limit for process to exit and collects it. A process
// still running then is killed, and collected.
func awaitExit(process *os.Process, limit time.Duration) processEnd {
	type waited struct {
		state *os.ProcessState
		err   error
	}
	done := make(chan waited, 1)
	go func() {
		state, err := process.Wait()
		done <- waited{state, err}
	}()
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case w := <-done:
		if w.err != nil {
			return processEnd{kind: processUnknown, err: w.err.Error()}
		}
		return processEnd{kind: processExited, state: w.state}
	case <-timer.C:
	}
	killErr := process.Kill()
	w := <-done
	if w.err != nil {
		return processEnd{kind: processUnknown, err: w.err.Error()}
	}
	if errors.Is(killErr, os.ErrProcessDone) {
		// It exited between the wait ending and the kill.
		return processEnd{kind: processExited, state: w.state}
	}
	return processEnd{kind: processKilled, state: w.state}
}

// deathReport says what happened to a cartridge that died without the host
// ending it: how its output ended and how its process did, in one sentence
// naming it. Mirrors the reference host's death_report.
func deathReport(cartridge string, end streamEnd, process processEnd) string {
	waited := int(exitWaitLimit / time.Second)
	if process.kind == processKilled {
		switch end.kind {
		case streamClosed:
			return fmt.Sprintf("Cartridge %s closed its output and was still running %ds later, so it was killed.", cartridge, waited)
		case streamBroken:
			return fmt.Sprintf("Cartridge %s stopped answering: reading its output failed: %s, and it was still running %ds later, so it was killed.", cartridge, end.err, waited)
		default:
			return fmt.Sprintf("Cartridge %s wrote output that is not a frame (%s), so it was killed.", cartridge, end.err)
		}
	}
	var ended string
	switch process.kind {
	case processExited:
		ended = fmt.Sprintf("Cartridge %s exited unexpectedly (%s)", cartridge, describeExit(process.state))
	case processNone:
		ended = fmt.Sprintf("Cartridge %s disconnected unexpectedly", cartridge)
	default:
		ended = fmt.Sprintf("Cartridge %s stopped unexpectedly, and was killed because whether it had exited could not be read (%s)", cartridge, process.err)
	}
	switch end.kind {
	case streamClosed:
		return ended + "."
	case streamBroken:
		return fmt.Sprintf("%s: reading its output failed: %s.", ended, end.err)
	default:
		return fmt.Sprintf("%s: its output is not a frame: %s.", ended, end.err)
	}
}

var windowsExceptions = map[uint32]string{
	0x80000003: "STATUS_BREAKPOINT",
	0xC0000005: "STATUS_ACCESS_VIOLATION",
	0xC000001D: "STATUS_ILLEGAL_INSTRUCTION",
	0xC0000094: "STATUS_INTEGER_DIVIDE_BY_ZERO",
	0xC00000FD: "STATUS_STACK_OVERFLOW",
	0xC0000374: "STATUS_HEAP_CORRUPTION",
	0xC0000409: "STATUS_STACK_BUFFER_OVERRUN",
	0xC0000417: "STATUS_INVALID_CRUNTIME_PARAMETER",
	0xC0000420: "STATUS_ASSERTION_FAILURE",
}

var windowsStatuses = map[uint32]string{
	0xC0000135: "STATUS_DLL_NOT_FOUND",
	0xC0000139: "STATUS_ENTRYPOINT_NOT_FOUND",
	0xC000013A: "STATUS_CONTROL_C_EXIT",
	0xC0000142: "STATUS_DLL_INIT_FAILED",
}

// describeWindowsExitCode says a Windows process's exit code in words: a
// crash names the exception it ended with, a process the loader could not
// start the status that stopped it, and anything else is the program's own
// exit code.
func describeWindowsExitCode(code uint32) string {
	if name, ok := windowsExceptions[code]; ok {
		return fmt.Sprintf("exception 0x%08X (%s)", code, name)
	}
	if name, ok := windowsStatuses[code]; ok {
		return fmt.Sprintf("status 0x%08X (%s)", code, name)
	}
	if code >= 0x80000000 {
		return fmt.Sprintf("exit code %d (0x%08X)", int32(code), code)
	}
	return fmt.Sprintf("exit code %d", code)
}
