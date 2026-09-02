//go:build windows

package bifaci

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// createSocketPair returns two connected, full-duplex byte streams.
//
// Windows has no `socketpair`, and the whole of this package's tests failed to
// COMPILE without one — so every protocol test the platform could have run
// was reported as a build failure instead of as a result.
//
// A loopback TCP connection is the pair that behaves the same way where these
// tests care: kernel-buffered, so a writer may run ahead of its reader by a
// socket buffer, which several tests below rely on by writing a whole frame
// before anything reads. `net.Pipe` is the other obvious answer and is the
// wrong one: it is synchronous and unbuffered, so those tests would deadlock
// rather than pass.
//
// The listener is closed as soon as it has accepted. It exists to introduce
// two sockets to each other and holding it open would leave a port bound for
// the life of the test binary.
func createSocketPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	type accepted struct {
		conn net.Conn
		err  error
	}
	incoming := make(chan accepted, 1)
	go func() {
		conn, err := listener.Accept()
		incoming <- accepted{conn: conn, err: err}
	}()

	dialed, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	served := <-incoming
	require.NoError(t, served.err)

	// Nagle would hold a small frame back waiting for more to send, which
	// turns "the reader sees the frame" into "the reader sees the frame in
	// forty milliseconds" — long enough for a test with a deadline to call it
	// a protocol failure.
	if tcp, ok := dialed.(*net.TCPConn); ok {
		require.NoError(t, tcp.SetNoDelay(true))
	}
	if tcp, ok := served.conn.(*net.TCPConn); ok {
		require.NoError(t, tcp.SetNoDelay(true))
	}

	return dialed, served.conn
}
