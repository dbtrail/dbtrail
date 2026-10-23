//go:build darwin || linux

package baselineintegrity

import (
	"os"
	"syscall"
)

// fileIdentity is what names the bytes of a stat'ed file for the verified
// memo: device, inode, size and modification time. A hard link keeps all
// four; a rewrite by rename changes the inode; a truncate or an append
// changes the size; any in-place write moves the mtime. ok=false when the
// platform exposes no device/inode, in which case nothing is memoized.
func fileIdentity(info os.FileInfo) (id verifiedKey, ok bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return verifiedKey{}, false
	}
	return verifiedKey{
		dev:   uint64(st.Dev),
		ino:   uint64(st.Ino),
		size:  info.Size(),
		mtime: info.ModTime().UnixNano(),
	}, true
}
