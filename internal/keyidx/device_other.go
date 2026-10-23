//go:build !darwin && !linux

package keyidx

import "os"

// sameDevice cannot tell on this platform: each directory is checked
// against its own free space.
func sameDevice(a, b string) bool { return false }

// fileIno cannot tell on this platform: 0, so only the modification time
// distinguishes a rewritten base of the same shape.
func fileIno(info os.FileInfo) uint64 { return 0 }
