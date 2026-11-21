//go:build darwin || linux

package pgdelta

import (
	"os"
	"syscall"
)

// fileID is a file as the file system tells it from another: device,
// inode, size and time of last write (UNIX nanoseconds).
type fileID struct {
	dev, ino    uint64
	size, mtime int64
}

// fileIDOf is the identity of a stat'ed file, false where the platform does
// not give a device and an inode.
func fileIDOf(fi os.FileInfo) (fileID, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fileID{}, false
	}
	return fileID{dev: uint64(st.Dev), ino: uint64(st.Ino), size: fi.Size(), mtime: fi.ModTime().UnixNano()}, true
}
