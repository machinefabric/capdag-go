//go:build !windows

package bifaci

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func killSelf() {
	syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

// TEST13211: a cartridge killed by a signal mid-frame is reported with the
// read error and the signal that killed it.
func Test13211_a_cartridge_killed_mid_frame_is_reported_with_its_signal(t *testing.T) {
	message := dieMidFrameReport(t, "kill")
	assert.Contains(t, message, "exited unexpectedly (killed by signal 9 (SIGKILL))")
}
