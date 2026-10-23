//go:build !darwin && !linux

package baselineintegrity

import "os"

// fileIdentity cannot name the bytes on this platform (no device/inode to
// tell a hard link from a copy), so the verified memo is never consulted and
// every call validates, as ValidateLocalFile does.
func fileIdentity(info os.FileInfo) (id verifiedKey, ok bool) {
	return verifiedKey{}, false
}
