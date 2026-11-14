//go:build unix

package console

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockTailDir takes an flock on the directory dir itself, without waiting,
// held for as long as the returned file stays open (#2819). The kernel drops
// it when the process dies however it dies, so a held lock proves a live
// process still uses the directory, with no rule of age. flock, not fcntl:
// an fcntl lock is the process's, so a second one taken in the same process
// would not conflict, and a keeper and a sweep of one process must. Same
// primitive as the snapshot job locks (consoleapp/baseline_jobs_lock_unix.go).
// errTailDirHeld when another open file holds it.
func lockTailDir(dir string) (*os.File, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errTailDirHeld
		}
		return nil, fmt.Errorf("lock %s: %w", dir, err)
	}
	return f, nil
}
