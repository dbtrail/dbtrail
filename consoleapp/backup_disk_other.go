//go:build !darwin && !linux

package consoleapp

import "errors"

// sameFilesystem cannot tell on this platform; the caller reads the error as
// "shared", which asks for the larger margin.
func sameFilesystem(a, b string) (bool, error) {
	return false, errors.New("filesystem identity not supported on this platform")
}
