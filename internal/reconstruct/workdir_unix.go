//go:build unix

package reconstruct

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

// lockWorkDir takes an flock on the directory dir itself, held for as long as
// the returned file stays open (#2513). The kernel drops it when the process
// dies however it dies, so a held lock proves a live process still uses the
// directory. flock, not fcntl: an fcntl lock is the process's, so a second
// one taken in the same process would not conflict, and a fold and a sweep of
// one process must. Same primitive as the tails' directories
// (internal/console/tail_lock_unix.go).
//
// wait true is the maker's call: it waits for a sweep that got there first.
// wait false is the sweep's: errWorkDirHeld when another open file holds it.
// Either way the path must still name the directory that was locked once the
// lock is held, or the answer is errWorkDirGone: a sweep that locked first
// has removed it, or the maker removed it and another directory took its
// name. A link is not followed: it is not a directory this code made.
func lockWorkDir(dir string, wait bool) (*os.File, error) {
	f, err := os.OpenFile(dir, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errWorkDirGone
		}
		return nil, err
	}
	how := syscall.LOCK_EX
	if !wait {
		how |= syscall.LOCK_NB
	}
	for {
		err = syscall.Flock(int(f.Fd()), how)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errWorkDirHeld
		}
		return nil, fmt.Errorf("lock %s: %w", dir, err)
	}
	afterWorkDirLocked(dir)
	held, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	named, err := os.Lstat(dir)
	if err != nil || !os.SameFile(held, named) {
		f.Close()
		if err == nil || errors.Is(err, fs.ErrNotExist) {
			return nil, errWorkDirGone
		}
		return nil, err
	}
	return f, nil
}

// ownedByThisUser says the directory is this process's user's. Another
// user's is theirs to sweep.
func ownedByThisUser(fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == os.Getuid()
}
