//go:build !darwin && !linux

package copymirror

func diskFree(string) (uint64, bool) { return 0, false }
