//go:build !darwin && !linux

package keyidx

// sameDevice cannot tell on this platform: each directory is checked
// against its own free space.
func sameDevice(a, b string) bool { return false }
