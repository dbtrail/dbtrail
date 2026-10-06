//go:build linux

package consoleapp

import (
	"os"
	"syscall"
)

func bootID() string {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return string(b)
}

func networkFS(dir string) (string, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return "", false
	}
	return networkFSMagic(int64(st.Type))
}
