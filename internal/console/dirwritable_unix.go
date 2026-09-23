//go:build unix

package console

import "syscall"

// dirWritable asks the kernel whether this process may create entries in dir,
// without creating one: the read-only serve checks a snapshot folder this way.
func dirWritable(dir string) error {
	return syscall.Access(dir, 0x2|0x1) // W_OK|X_OK
}
