//go:build !darwin && !(linux && (amd64 || arm64 || 386 || arm || riscv64 || loong64 || s390x))

package ttyflush

// Input does nothing on this system: what was typed ahead stays.
func Input(fd uintptr) error { return nil }
