//go:build !darwin && !linux

package pgdelta

import "os"

// fileID is a file as the file system tells it from another; on this
// platform there is no device and inode to tell it by.
type fileID struct {
	dev, ino    uint64
	size, mtime int64
}

// fileIDOf cannot tell on this platform: nothing is remembered, and every
// call reads the file as before.
func fileIDOf(fi os.FileInfo) (fileID, bool) { return fileID{}, false }
