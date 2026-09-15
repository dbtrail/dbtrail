//go:build !darwin && !linux

package doctor

import "errors"

// diskFree is unsupported on this platform; the capacity check degrades to
// projection-only (free space reported as not measurable).
func diskFree(path string) (uint64, error) {
	return 0, errors.New("free-space probe not supported on this platform")
}

func diskSpace(path string) (free, total uint64, err error) {
	return 0, 0, errors.New("free-space probe not supported on this platform")
}
