//go:build !linux && !darwin

package ttyflush

// Input does nothing on this system: what was typed ahead stays.
func Input(fd uintptr) error { return nil }
