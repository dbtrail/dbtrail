//go:build unix

package consoleapp

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

// createJobLock creates a fresh lock file in dir and takes its flock. The
// flock is held for as long as the returned file stays open, and the kernel
// drops it when the process dies however it dies (SIGKILL, the OOM killer):
// that is the whole proof of life a reclaim relies on. Same primitive as the
// baseline prune lock (internal/baseline/prunelock_unix.go).
func createJobLock(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(dir, "run-*.lock")
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, fmt.Errorf("lock %s: %w", f.Name(), err)
	}
	return f, nil
}

// afterJobLockTaken runs between taking a lock and checking that the path
// still names the locked file; tests use it to stage the owner ending in
// between. Production never reassigns it.
var afterJobLockTaken = func(string) {}

// jobLockState is what an attempt on another job's lock found.
type jobLockState int

const (
	jobLockHeld    jobLockState = iota // a live process holds it: the job is running
	jobLockTaken                       // nobody held it: the owner is gone, and the caller now holds it
	jobLockMissing                     // no lock file at that path
)

// tryJobLock takes the flock of an existing lock file without waiting. A
// second open in THIS process conflicts like any other (flock locks belong
// to the open file, not the process), so a job running here reads as held.
//
// On jobLockTaken it also checks that the path still names the file it
// locked: an owner that ends removes the file while holding the lock, and a
// lock taken on that removed file proves nothing about the path.
func tryJobLock(path string) (*os.File, jobLockState, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, jobLockMissing, nil
	}
	if err != nil {
		return nil, jobLockHeld, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, jobLockHeld, nil
		}
		return nil, jobLockHeld, err
	}
	afterJobLockTaken(path)
	held, err1 := f.Stat()
	now, err2 := os.Stat(path)
	if err1 != nil || err2 != nil || !os.SameFile(held, now) {
		f.Close()
		if errors.Is(err2, fs.ErrNotExist) {
			return nil, jobLockMissing, nil
		}
		// Replaced under us: whoever replaced it is not known to be dead.
		return nil, jobLockHeld, nil
	}
	return f, jobLockTaken, nil
}
