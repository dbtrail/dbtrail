package reconstruct

import (
	"context"
	"log/slog"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// Views skipped, carried forward (#1879).
//
// A full read records the views it left out beside the snapshot it wrote
// (baseline/viewsskipped.go). A snapshot built here reads no dump, so it has
// nothing to say about views on its own: it carries the record of the
// snapshot it started from, marked as carried, with the time of the full
// read unchanged. The record then says what the source held at that read. A
// view created since is not in it, and a view dropped since still is.
//
// When the tables of a run came from more than one snapshot, the NEWEST one
// decides, and only it. If that one has no record, nothing is carried: an
// older snapshot's list next to a newer full read that found no view would
// report views the newer read did not see.
//
// Never a condition for a snapshot: every failure here is a warning, and the
// snapshot is published without the record.

// viewsCarryTimeout bounds the one read of the record from S3.
const viewsCarryTimeout = 30 * time.Second

// snapshotOfTable is the snapshot a table file lives in
// (<snapshot>/<schema>/<table>.parquet), as a local directory or an s3://
// URL. "" for no path.
func snapshotOfTable(tablePath string) string {
	if tablePath == "" {
		return ""
	}
	if strings.HasPrefix(tablePath, "s3://") {
		rest := strings.TrimPrefix(tablePath, "s3://")
		snap := path.Dir(path.Dir(rest))
		if snap == "." || snap == "/" || !strings.Contains(snap, "/") {
			return ""
		}
		return "s3://" + snap
	}
	return filepath.Dir(filepath.Dir(tablePath))
}

// newestSourceSnapshot is the newest snapshot the run's tables were read
// from, by the time in its name. "" when no table had one.
func newestSourceSnapshot(reports []*TableReport) (snapshot string, at time.Time) {
	for _, r := range reports {
		if r == nil || r.SourceSnapshot == "" {
			continue
		}
		t, ok := snapshotdir.ParseTime(path.Base(filepath.ToSlash(r.SourceSnapshot)))
		if !ok {
			continue
		}
		if snapshot == "" || t.After(at) {
			snapshot, at = r.SourceSnapshot, t
		}
	}
	return snapshot, at
}

// readViewsSkippedS3 reads the record of a snapshot in S3. A package
// variable so a test can answer for the bucket.
var readViewsSkippedS3 = func(ctx context.Context, snapshotURL string) (baseline.ViewsSkipped, bool, error) {
	bucket, key, err := storage.ParseS3URL(snapshotURL)
	if err != nil {
		return baseline.ViewsSkipped{}, false, err
	}
	rc, err := baselineintegrity.OpenS3Object(ctx, bucket, path.Join(key, baseline.ViewsSkippedName))
	if err != nil {
		if baselineintegrity.S3ObjectAbsent(err) {
			return baseline.ViewsSkipped{}, false, nil
		}
		return baseline.ViewsSkipped{}, false, err
	}
	defer rc.Close()
	return baseline.ReadViewsSkippedFrom(rc)
}

// readViewsSkippedOf reads the record of a snapshot, local or in S3.
func readViewsSkippedOf(ctx context.Context, snapshot string) (baseline.ViewsSkipped, bool, error) {
	if strings.HasPrefix(snapshot, "s3://") {
		ctx, cancel := context.WithTimeout(ctx, viewsCarryTimeout)
		defer cancel()
		return readViewsSkippedS3(ctx, snapshot)
	}
	return baseline.ReadViewsSkipped(snapshot)
}

// carryViewsSkipped writes into snapshotDir the record of the newest
// snapshot the run's tables were read from, marked as carried. With no such
// record it writes none, and removes one an earlier attempt at the same
// directory left.
func carryViewsSkipped(ctx context.Context, snapshotDir string, reports []*TableReport) {
	rec, from := carriedViewsSkipped(ctx, reports)
	if rec.Count > 0 {
		slog.Info("the skipped views are carried from the snapshot this one was built from; this run read no dump, so the list is as of that full read",
			"snapshot", snapshotDir, "from", from, "views", rec.Count, "read_at", rec.ReadAt)
	}
	baseline.RecordViewsSkipped(snapshotDir, rec)
}

// carriedViewsSkipped is the record to carry and the snapshot it came from.
// The zero record when there is nothing to carry.
func carriedViewsSkipped(ctx context.Context, reports []*TableReport) (baseline.ViewsSkipped, string) {
	from, at := newestSourceSnapshot(reports)
	if from == "" {
		return baseline.ViewsSkipped{}, ""
	}
	rec, ok, err := readViewsSkippedOf(ctx, from)
	if err != nil {
		slog.Warn("could not read the skipped views of the snapshot this one was built from; the new snapshot is published without the record",
			"from", from, "error", err)
		return baseline.ViewsSkipped{}, from
	}
	if !ok {
		return baseline.ViewsSkipped{}, from
	}
	// A full read whose record lost its date was taken at its snapshot's
	// time. A carried record with no date stays without one: the snapshot it
	// was read from is not the one that read the source.
	if rec.ReadAt == "" && !rec.Carried {
		rec.ReadAt = at.UTC().Format(time.RFC3339)
	}
	rec.Carried = true
	return rec, from
}
