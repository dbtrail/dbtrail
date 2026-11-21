//go:build darwin

package ttyflush

import (
	"syscall"
	"unsafe"
)

// fread is FREAD of <sys/fcntl.h>: TIOCFLUSH with it flushes the input
// queue only.
const fread = 1

// Input discards the input fd's terminal holds and nobody has read.
func Input(fd uintptr) error {
	which := int32(fread)
	if _, _, e := syscall.Syscall(syscall.SYS_IOCTL, fd, syscall.TIOCFLUSH, uintptr(unsafe.Pointer(&which))); e != 0 {
		return e
	}
	return nil
}
