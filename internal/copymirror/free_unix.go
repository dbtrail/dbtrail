//go:build darwin || linux

package copymirror

import "syscall"

// diskFree is the space available to this user on the disk holding dir.
func diskFree(dir string) (uint64, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, false
	}
	return st.Bavail * uint64(st.Bsize), true
}
