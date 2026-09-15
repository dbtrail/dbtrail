//go:build darwin || linux

package doctor

import "syscall"

// diskFree returns the bytes available to non-root users on the filesystem
// containing path. Stdlib syscall, not golang.org/x/sys (a transitive dep we
// must not import directly).
func diskFree(path string) (uint64, error) {
	free, _, err := diskSpace(path)
	return free, err
}

// diskSpace is diskFree plus the filesystem's total size, which tells a full
// disk (free zero, a real total) from a mount that cannot answer (total zero).
func diskSpace(path string) (free, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	return st.Bavail * uint64(st.Bsize), st.Blocks * uint64(st.Bsize), nil
}
