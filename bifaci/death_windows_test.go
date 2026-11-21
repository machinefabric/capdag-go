//go:build windows

package bifaci

import "os"

func killSelf() {
	self, err := os.FindProcess(os.Getpid())
	if err == nil {
		self.Kill()
	}
	select {}
}
