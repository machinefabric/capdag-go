package bifaci

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dieHowVariable, when set, makes this test binary a cartridge that dies part
// way through a frame instead of running the tests: the host under test starts
// the binary itself as its cartridge, and the child inherits the variable.
const dieHowVariable = "CAPDAG_TEST_DIE_HOW"

const dyingManifest = `{"name":"Dying","version":"1.0","channel":"release","registry_url":null,"cap_groups":[{"name":"default","caps":[{"urn":"cap:effect=none","title":"identity","aliases":["identity"],"args":[{"media_urn":"media:","name":"input","required":true,"sources":[{"stdin":"media:"}]}],"outputs":[{"media_urn":"media:","name":"output","output_description":"output"}]},{"urn":"cap:die","title":"die","aliases":["die"],"args":[{"media_urn":"media:","name":"input","required":true,"sources":[{"stdin":"media:"}]}],"outputs":[{"media_urn":"media:","name":"output","output_description":"output"}]}]}]}`

func TestMain(m *testing.M) {
	if how := os.Getenv(dieHowVariable); how != "" {
		os.Unsetenv(dieHowVariable)
		dieMidFrame(how)
	}
	os.Exit(m.Run())
}

// dieMidFrame answers the host's handshake, any identity check and its policy, takes
// one request, says on stderr that it is dying, writes a length prefix that
// promises more than follows it, and dies the way how says: exit:<code> exits
// with that code, kill kills the process with SIGKILL.
func dieMidFrame(how string) {
	fail := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "dying cartridge: "+format+"\n", args...)
		os.Exit(70)
	}
	reader := NewFrameReader(os.Stdin)
	writer := NewFrameWriter(os.Stdout)
	limits, err := HandshakeAccept(reader, writer, []byte(dyingManifest), make(PoolStates))
	if err != nil {
		fail("handshake: %v", err)
	}
	reader.SetLimits(limits)
	writer.SetLimits(limits)

	// Echo an identity check, take the policy, and stop at the first other
	// request: the one to die on.
	for {
		f, err := reader.ReadFrame()
		if err != nil {
			fail("reading the host: %v", err)
		}
		if f.FrameType == FrameTypePolicy {
			continue
		}
		if f.FrameType != FrameTypeReq {
			fail("unexpected %v before a request", f.FrameType)
		}
		if f.Cap == nil || *f.Cap != "cap:effect=none" {
			break
		}
		var payload []byte
		for {
			body, err := reader.ReadFrame()
			if err != nil {
				fail("identity body: %v", err)
			}
			if body.FrameType == FrameTypeChunk {
				payload = append(payload, body.Payload...)
			}
			if body.FrameType == FrameTypeEnd {
				break
			}
		}
		for _, echo := range []*Frame{
			NewStreamStart(f.Id, "identity-echo", "media:", nil),
			NewChunk(f.Id, "identity-echo", 0, payload, 0, ComputeChecksum(payload)),
			NewStreamEnd(f.Id, "identity-echo", 1),
			NewEnd(f.Id, nil),
		} {
			if err := writer.WriteFrame(echo); err != nil {
				fail("identity echo: %v", err)
			}
		}
	}

	fmt.Fprintf(os.Stderr, "dying cartridge is dying: %s\n", how)
	os.Stdout.Write([]byte{0, 0, 0, 64, 'p', 'a', 'r', 't'})
	if how == "kill" {
		killSelf()
	}
	code, err := strconv.Atoi(strings.TrimPrefix(how, "exit:"))
	if err != nil {
		fail("how is exit:<code> or kill, not %q", how)
	}
	os.Exit(code)
}

// dieMidFrameReport starts this test binary as a cartridge that dies part way
// through a frame the way how says, with a request pending on it, and returns
// the ERR the host answered that request with.
func dieMidFrameReport(t *testing.T, how string) string {
	t.Helper()
	binary, err := os.Executable()
	require.NoError(t, err)
	t.Setenv(dieHowVariable, how)

	host := newTestHost()
	host.RegisterCartridge(binary, "dying", "0.0.0", CartridgeChannelRelease, nil, capGroupsFromURNs(t, "cap:effect=none", "cap:die"))

	relayRead, engineWrite := net.Pipe()
	engineRead, relayWrite := net.Pipe()
	errFrame := make(chan *Frame, 1)
	go func() {
		writer := NewFrameWriter(engineWrite)
		reader := NewFrameReader(engineRead)
		reqId := NewMessageIdRandom()
		xid := NewMessageIdFromUint(1)
		req := NewReq(reqId, "cap:die", nil, "text/plain")
		req.RoutingId = &xid
		end := NewEnd(reqId, nil)
		end.RoutingId = &xid
		if writer.WriteFrame(req) != nil || writer.WriteFrame(end) != nil {
			close(errFrame)
			return
		}
		var answered *Frame
		for {
			f, err := reader.ReadFrame()
			if err != nil {
				break
			}
			if f.FrameType == FrameTypeErr && answered == nil {
				answered = f
				// The answer is in: end the relay, which ends Run.
				engineWrite.Close()
			}
		}
		errFrame <- answered
	}()

	host.Run(relayRead, relayWrite, nil)
	relayRead.Close()
	relayWrite.Close()
	engineRead.Close()
	answered := <-errFrame
	require.NotNil(t, answered, "the request the dead cartridge held must be answered")
	assert.Equal(t, "CARTRIDGE_DIED", answered.ErrorCode())
	message := answered.ErrorMessage()
	assert.Contains(t, message, binary, "the death names the cartridge")
	assert.Contains(t, message, "reading its output failed: unexpected EOF", "the death names the read error the host saw")
	assert.NotContains(t, message, "still running", "the process had exited")
	assert.Contains(t, message, "stderr:\ndying cartridge is dying: "+how, "the death carries the cartridge's stderr")
	return message
}

// TEST13210: a cartridge that writes part of a frame and exits is reported
// with the read error the host saw and the code it exited with — not only as
// having died, and never as still running.
func Test13210_a_cartridge_that_exits_mid_frame_is_reported_with_its_read_error_and_exit_code(t *testing.T) {
	message := dieMidFrameReport(t, "exit:3")
	assert.Contains(t, message, "exited unexpectedly (exit code 3)")
}

// TEST13212: a Windows process's exit code is told apart: a crash names the
// exception it ended with, a process the loader could not start names the
// status that stopped it, and any other code is the program's own, shown
// signed and in hex when its high bit is set.
func Test13212_a_windows_exit_code_names_its_exception(t *testing.T) {
	assert.Equal(t, "exception 0xC0000409 (STATUS_STACK_BUFFER_OVERRUN)", describeWindowsExitCode(0xC0000409))
	assert.Equal(t, "exception 0xC0000005 (STATUS_ACCESS_VIOLATION)", describeWindowsExitCode(0xC0000005))
	assert.Equal(t, "status 0xC0000135 (STATUS_DLL_NOT_FOUND)", describeWindowsExitCode(0xC0000135))
	assert.Equal(t, "exit code -1 (0xFFFFFFFF)", describeWindowsExitCode(0xFFFFFFFF))
	assert.Equal(t, "exit code 3", describeWindowsExitCode(3))
	assert.Equal(t, "exit code 0", describeWindowsExitCode(0))
}

// TEST13213: a stream that ends right after a whole length prefix, before any
// of the frame it announces, ended part way through that frame.
func Test13213_a_stream_ending_after_a_length_prefix_is_an_unexpected_end(t *testing.T) {
	reader := NewFrameReader(strings.NewReader("\x00\x00\x00\x10"))
	_, err := reader.ReadFrame()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
