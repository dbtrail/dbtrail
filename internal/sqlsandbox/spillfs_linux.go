//go:build linux

package sqlsandbox

import "syscall"

// tmpfsMagic is the f_type statfs reports for a tmpfs mount.
const tmpfsMagic = 0x01021994

// spillFS reports the space free to this user under dir and whether dir is
// on a tmpfs, which keeps its files in memory.
func spillFS(dir string) (free int64, inMemory bool, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, false, err
	}
	return int64(st.Bavail) * st.Bsize, st.Type == tmpfsMagic, nil
}
