//go:build darwin || linux

package consoleapp

import (
	"errors"
	"os"
	"syscall"
)

// sameFilesystem reports whether two existing paths sit on one filesystem, by
// device id. Used to tell whether a full read's dump and its Parquet copy
// share a disk (#1938).
func sameFilesystem(a, b string) (bool, error) {
	da, err := deviceOf(a)
	if err != nil {
		return false, err
	}
	db, err := deviceOf(b)
	if err != nil {
		return false, err
	}
	return da == db, nil
}

func deviceOf(p string) (uint64, error) {
	fi, err := os.Stat(p)
	if err != nil {
		return 0, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("no device id for " + p)
	}
	return uint64(st.Dev), nil //nolint:unconvert // int32 on darwin, uint64 on linux
}
