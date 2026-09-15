package consoleapp

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// errFoldDiskFull marks a table refused before writing because the disk could
// not hold it (#1614). applyFoldStatus turns it into DiskRefused, which keeps
// the schedule from answering it with a full backup into the same disk.
var errFoldDiskFull = errors.New("not enough free disk space for this backup")

// foldDiskMargin is the room kept free on top of each file's size. It covers
// what the size cannot see on a host that also captures: DuckDB spill, the
// index or logs sharing the disk, a file that grows past the one it is
// rebuilt from. A constant, not a setting: nobody should tune a backup into
// the last gigabyte of a capture host.
const foldDiskMargin = 1 << 30

// diskSpaceFn is a seam for tests.
var diskSpaceFn = doctor.DiskSpace

// newDiskSpaceCheck returns the FullTableConfig.SpaceCheck for one backup
// build (#1614). The fold calls it right before it creates a file: a table's
// Parquet file, sized on the backup file it is rebuilt from, or the next SQL
// file of a .sql backup. A table carried forward unchanged never reaches it,
// so only what is really written counts.
//
// It refuses when the directory's filesystem has less free space than the file
// plus foldDiskMargin. When the space cannot be measured (an error, or a mount
// that reports no size) it lets the file through and logs that once for the
// build: a false refusal would stop backups on a host that has the room. A
// full disk reports zero free with a real size, and refuses.
func newDiskSpaceCheck() func(dir string, need int64) error {
	var once sync.Once
	return func(dir string, need int64) error {
		dir = existingParent(dir)
		free, total, err := diskSpaceFn(dir)
		if err != nil || total == 0 {
			once.Do(func() {
				slog.Warn("backup disk check skipped: free space on the backup directory cannot be measured",
					"dir", dir, "error", err)
			})
			return nil
		}
		inAll := uint64(max(need, 0)) + foldDiskMargin
		if free >= inAll {
			return nil
		}
		freeS, inAllS := humanSize(int64(free)), humanSize(int64(inAll))
		if freeS == inAllS {
			freeS, inAllS = fmt.Sprintf("%d bytes", free), fmt.Sprintf("%d bytes", inAll)
		}
		return fmt.Errorf("%w: %s has %s free, and the next file needs about %s plus %s kept free, %s in all",
			errFoldDiskFull, dir, freeS, humanSize(need), humanSize(foldDiskMargin), inAllS)
	}
}

// foldDiskRefused reports whether a fold stopped for disk space: refused by
// the check, or a write that found the disk full anyway (another server's
// build, the index, anything else filling the same disk meanwhile).
func foldDiskRefused(err error) bool {
	return errors.Is(err, errFoldDiskFull) || errors.Is(err, syscall.ENOSPC)
}

// existingParent walks up from dir to the nearest directory that exists, so a
// directory the build has not created yet is measured on the filesystem it
// will be created on.
func existingParent(dir string) string {
	d := filepath.Clean(dir)
	for {
		if fi, err := os.Stat(d); err == nil && fi.IsDir() {
			return d
		}
		parent := filepath.Dir(d)
		if parent == d {
			return d
		}
		d = parent
	}
}

// humanSize renders bytes in binary units with one decimal. A value that would
// round up to 1024.0 moves to the next unit.
func humanSize(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	v, exp := float64(b)/1024, 0
	for v >= 1023.95 && exp < 5 {
		v /= 1024
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", v, "KMGTPE"[exp])
}
