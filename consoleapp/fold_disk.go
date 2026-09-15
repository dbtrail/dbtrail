package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// errFoldDiskFull marks a fold refused before writing because the disk could
// not hold it (#1614). applyFoldStatus turns it into DiskRefused, which keeps
// the schedule from answering it with a full backup into the same disk.
var errFoldDiskFull = errors.New("not enough free disk space for this backup")

// foldDiskMargin is the room kept free on top of the estimate. A fold writes
// its tables as Parquet about the size of the snapshot it starts from; the
// margin covers what the estimate cannot see on a host that also captures
// (DuckDB spill, the index or logs sharing the disk, a row group or two of
// growth). A constant, not a setting: nobody should tune a backup into the
// last gigabyte of a capture host.
const foldDiskMargin = 1 << 30

// diskSpaceFn and snapshotFileSizesFn are seams for tests; the listing goes
// through listBaselines, the fold source's own seam.
var (
	diskSpaceFn         = doctor.DiskSpace
	snapshotFileSizesFn = console.BaselineSnapshotFileSizes
)

// checkFoldDisk refuses a fold whose output directory's filesystem cannot
// hold the snapshot it starts from plus foldDiskMargin. When either number is
// not reliable it proceeds and says so in the log: a false refusal would stop
// backups on a host that has the room.
//
// reuse is the run's carry-forward setting. With it on, a local table file
// that already has another hard link is left out of the estimate: it was
// carried forward before, so its table did not change between two backups,
// and the fold will most likely link it again for free. Counting those files
// would refuse, at every slot, a host whose backups are mostly links. A table
// that does change is then written without having been counted; the margin is
// what covers that.
func checkFoldDisk(ctx context.Context, source, outDir string, at time.Time, tables []string, reuse bool) error {
	need, from, err := snapshotBytes(ctx, source, at, tables, reuse)
	if err != nil {
		slog.Warn("backup disk check skipped: the backup this one starts from could not be sized",
			"source", source, "at", at.UTC().Format(time.RFC3339), "error", err)
		return nil
	}
	if from.IsZero() {
		// Nothing to start from: the fold refuses on its own, with its reason.
		return nil
	}
	dir := existingParent(outDir)
	free, total, err := diskSpaceFn(dir)
	if err != nil || total == 0 {
		// A total of zero is what a read-only or network mount reports when it
		// cannot answer, and an error is no answer at all: proceed rather than
		// refuse. A full disk reports zero free with a real total, and refuses.
		slog.Warn("backup disk check skipped: free space on the backup directory cannot be measured",
			"dir", dir, "error", err)
		return nil
	}
	total64 := need + foldDiskMargin
	if int64(free) >= total64 {
		return nil
	}
	what := "is"
	if reuse && !strings.HasPrefix(source, "s3://") {
		what = "is, without the tables already shared with another backup,"
	}
	return fmt.Errorf("%w: %s has %s free, and this backup needs about %s (the %s backup it starts from %s %s, plus %s kept free)",
		errFoldDiskFull, dir, humanSize(int64(free)), humanSize(total64), from.UTC().Format("2006-01-02 15:04:05"),
		what, humanSize(need), humanSize(foldDiskMargin))
}

// snapshotBytes sums the stored files of the snapshot a fold toward at starts
// from, for the tables it will write: the newest snapshot at or before at,
// whatever order the listing returns. A zero time with no error means no such
// snapshot. Local files are stat'ed (reuse leaves out shared links, see
// checkFoldDisk); an S3 snapshot is sized from its object listing, where
// nothing can be reused.
func snapshotBytes(ctx context.Context, source string, at time.Time, tables []string, reuse bool) (int64, time.Time, error) {
	files, err := listBaselines(ctx, source)
	if err != nil {
		return 0, time.Time{}, err
	}
	var anchor time.Time
	for _, f := range files {
		if !f.SnapshotTime.After(at) && f.SnapshotTime.After(anchor) {
			anchor = f.SnapshotTime
		}
	}
	if anchor.IsZero() {
		return 0, time.Time{}, nil
	}
	want := make(map[string]bool, len(tables))
	for _, t := range tables {
		want[t] = true
	}
	dirName := reconstruct.SnapshotDirName(anchor)
	var sizes map[string]int64
	if strings.HasPrefix(source, "s3://") {
		if sizes, err = snapshotFileSizesFn(ctx, source, dirName); err != nil {
			return 0, time.Time{}, err
		}
	}
	var total int64
	for _, f := range files {
		if !f.SnapshotTime.Equal(anchor) || !want[f.Schema+"."+f.Table] {
			continue
		}
		if sizes != nil {
			n, ok := sizes[dirName+"/"+f.Schema+"/"+f.Table+".parquet"]
			if !ok {
				return 0, time.Time{}, fmt.Errorf("%s.%s is listed in the %s backup but has no stored size", f.Schema, f.Table, dirName)
			}
			total += n
			continue
		}
		fi, err := os.Stat(f.Path)
		if err != nil {
			return 0, time.Time{}, err
		}
		if reuse && sharedLink(fi) {
			continue
		}
		total += fi.Size()
	}
	return total, anchor, nil
}

// checkChunkDisk is the .sql backup's disk check (#1614), run before each SQL
// file it writes. The export writes plain SQL text, usually larger than the
// compressed backup it reads, so an estimate from that backup would wave
// through a build that fills the disk; checking one file at a time needs no
// estimate. A probe that cannot answer lets the file through.
func checkChunkDisk(dir string, next int64) error {
	free, total, err := diskSpaceFn(dir)
	if err != nil || total == 0 {
		slog.Debug(".sql backup disk check skipped: free space cannot be measured", "dir", dir, "error", err)
		return nil
	}
	if int64(free) >= next+foldDiskMargin {
		return nil
	}
	return fmt.Errorf("%w: %s has %s free, and the next file of this .sql backup takes up to %s, plus %s kept free",
		errFoldDiskFull, dir, humanSize(int64(free)), humanSize(next), humanSize(foldDiskMargin))
}

// existingParent walks up from dir to the nearest directory that exists, so a
// backup directory the first run has not created yet is measured on the
// filesystem it will be created on.
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

// humanSize renders bytes in binary units with one decimal.
func humanSize(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
