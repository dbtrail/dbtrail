//go:build linux && (amd64 || arm64 || 386 || arm || riscv64 || loong64 || s390x)

package ttyflush

import "syscall"

// tcflsh is TCFLSH of <asm-generic/ioctls.h>, the number these architectures
// use (syscall names it on some of them only).
const tcflsh = 0x540B

// Input discards the input fd's terminal holds and nobody has read.
func Input(fd uintptr) error {
	if _, _, e := syscall.Syscall(syscall.SYS_IOCTL, fd, tcflsh, syscall.TCIFLUSH); e != 0 {
		return e
	}
	return nil
}
