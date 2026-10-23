//go:build darwin || linux

package keyidx

import (
	"os"
	"syscall"
)

// sameDevice reports whether a and b are on one filesystem (same device
// id). Either path unreadable, or no device id to compare: false, so each
// is checked against its own free space.
func sameDevice(a, b string) bool {
	sa, err := os.Stat(a)
	if err != nil {
		return false
	}
	sb, err := os.Stat(b)
	if err != nil {
		return false
	}
	da, ok := sa.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	db, ok := sb.Sys().(*syscall.Stat_t)
	return ok && da.Dev == db.Dev
}
