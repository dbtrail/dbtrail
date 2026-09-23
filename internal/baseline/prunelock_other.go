//go:build !unix

package baseline

import (
	"io/fs"
	"path/filepath"
)

const pruneLockName = ".prune.lock"

// lockPrune is a no-op where flock is unavailable. bintrail ships on Linux and
// macOS; this keeps the package compiling elsewhere, at the cost of
// serializing concurrent prunes.
func lockPrune(string) (func(), bool, error) { return func() {}, false, nil }

// reclaimableSize cannot see link counts here, so it counts every file: an
// over-estimate where carried-forward files are shared.
func reclaimableSize(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, ierr := d.Info(); ierr == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}
