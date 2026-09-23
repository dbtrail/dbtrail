//go:build unix

package baseline

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// pruneLockName serializes prunes of one snapshot root (#1681). Same shape as
// the pointer lock: dot-prefixed, not a timestamp (discovery skips it), a
// regular file (the upload skips it by name, isPruneArtifact).
const pruneLockName = ".prune.lock"

// lockPrune takes the root's prune lock without waiting. busy means another
// prune holds it; that prune is doing this one's job, so the caller steps
// aside instead of queueing behind it. flock, so a prune that dies releases it.
func lockPrune(root string) (unlock func(), busy bool, err error) {
	path := filepath.Join(root, pruneLockName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, fmt.Errorf("open prune lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("lock %s: %w", path, err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, false, nil
}

// reclaimableSize sums the regular files under dir that deleting dir would
// actually free: those with no other hard link. A carried-forward table file
// is shared with a newer snapshot, and deleting this name of it frees nothing.
func reclaimableSize(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}
