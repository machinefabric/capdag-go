//go:build !windows

package bifaci

import (
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// createSocketPair returns two connected, full-duplex byte streams.
//
// AF_UNIX SOCK_STREAM, which is what the host and a cartridge actually get:
// kernel-buffered, so a writer may run ahead of its reader by a socket
// buffer's worth. The protocol tests below depend on that — several write a
// whole frame before anything reads — and an unbuffered pair would deadlock
// on the first of them rather than exercise the framing.
func createSocketPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	require.NoError(t, err)

	file1 := os.NewFile(uintptr(fds[0]), "socket1")
	file2 := os.NewFile(uintptr(fds[1]), "socket2")

	conn1, err := net.FileConn(file1)
	require.NoError(t, err)
	conn2, err := net.FileConn(file2)
	require.NoError(t, err)

	file1.Close()
	file2.Close()

	return conn1, conn2
}
