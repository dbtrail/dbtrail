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

// diskFreeFn and snapshotBytesFn are seams for tests.
var (
	diskFreeFn      = doctor.DiskFree
	snapshotBytesFn = snapshotBytes
)

// checkFoldDisk refuses a fold whose output directory's filesystem cannot
// hold the snapshot it starts from plus foldDiskMargin. When either number is
// not reliable it proceeds and says so in the log: a false refusal would stop
// backups on a host that has the room.
//
// The estimate is an upper bound: tables carried forward by hard link take no
// space, and a fold rarely grows a table past its previous size by much.
func checkFoldDisk(ctx context.Context, source, outDir string, at time.Time, tables []string) error {
	need, from, ok := snapshotBytesFn(ctx, source, at, tables)
	if !ok {
		// Info, not Warn: on an S3-backed server this is every run, and a
		// warning that fires on every healthy run is one nobody reads.
		slog.Info("backup disk check skipped: the size of the backup this one starts from is not known here",
			"source", source, "at", at.UTC().Format(time.RFC3339))
		return nil
	}
	dir := existingParent(outDir)
	free, err := diskFreeFn(dir)
	if err != nil || free == 0 {
		// Zero is what a read-only or network mount reports when it cannot
		// answer, and an error is no answer at all: proceed rather than refuse.
		slog.Warn("backup disk check skipped: free space on the backup directory cannot be measured",
			"dir", dir, "free", free, "error", err)
		return nil
	}
	total := need + foldDiskMargin
	if int64(free) >= total {
		return nil
	}
	return fmt.Errorf("%w: %s has %s free, and this backup needs about %s (the %s backup it starts from is %s, plus %s kept free)",
		errFoldDiskFull, dir, humanSize(int64(free)), humanSize(total), from.UTC().Format("2006-01-02 15:04:05"),
		humanSize(need), humanSize(foldDiskMargin))
}

// snapshotBytes sums the files of the snapshot a fold toward at starts from,
// for the tables it will write. Only a local source can be sized here; an S3
// source reports not ok.
func snapshotBytes(ctx context.Context, source string, at time.Time, tables []string) (int64, time.Time, bool) {
	if source == "" || strings.HasPrefix(source, "s3://") {
		return 0, time.Time{}, false
	}
	files, err := reconstruct.ListBaselines(ctx, source)
	if err != nil {
		return 0, time.Time{}, false
	}
	want := make(map[string]bool, len(tables))
	for _, t := range tables {
		want[t] = true
	}
	var anchor time.Time
	var total int64
	for _, f := range files { // newest first
		if f.SnapshotTime.After(at) {
			continue
		}
		if anchor.IsZero() {
			anchor = f.SnapshotTime
		}
		if !f.SnapshotTime.Equal(anchor) {
			break
		}
		if !want[f.Schema+"."+f.Table] {
			continue
		}
		fi, err := os.Stat(f.Path)
		if err != nil {
			return 0, time.Time{}, false
		}
		total += fi.Size()
	}
	if anchor.IsZero() {
		return 0, time.Time{}, false
	}
	return total, anchor, true
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
