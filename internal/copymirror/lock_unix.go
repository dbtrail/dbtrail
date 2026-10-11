//go:build unix

package copymirror

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockMirror takes the mirror's lock without waiting. Nothing stops two
// processes from being given one mirror; the one that finds it held steps
// aside. flock, so a round that dies releases it.
func lockMirror(root string) (unlock func(), busy bool, err error) {
	p := filepath.Join(root, lockName)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, fmt.Errorf("open the mirror lock %s: %w", p, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("lock %s: %w", p, err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, false, nil
}
