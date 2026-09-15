//go:build darwin || linux

package consoleapp

import (
	"os"
	"syscall"
)

// sharedLink reports whether fi names a file with more than one hard link.
func sharedLink(fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && st.Nlink > 1
}
