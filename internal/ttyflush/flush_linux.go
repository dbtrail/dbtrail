//go:build linux

package ttyflush

import "syscall"

// Input discards the input fd's terminal holds and nobody has read.
func Input(fd uintptr) error {
	if _, _, e := syscall.Syscall(syscall.SYS_IOCTL, fd, syscall.TCFLSH, syscall.TCIFLUSH); e != 0 {
		return e
	}
	return nil
}
