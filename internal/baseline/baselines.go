package baseline

import (
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BaselineInfo holds metadata about a discovered baseline Parquet file.
type BaselineInfo struct {
	SnapshotTime time.Time
	Database     string
	Table        string
	BinlogFile   string
	BinlogPos    int64
	GTIDSet      string
	LSN          uint64 // PostgreSQL WAL LSN anchor; 0 = absent (MySQL baseline, or pre-#593 PG baseline)
	Path         string
}

// DiscoverBaselines walks a baseline directory and returns metadata for each
// Parquet file found. The expected layout is:
//
//	<dir>/<timestamp>/<database>/<table>.parquet
//
// where <timestamp> is an RFC3339 string with colons replaced by hyphens
// (e.g. "2025-02-28T00-00-00Z"). Files that cannot be parsed are skipped.
// internal/reconstruct.ListBaselines walks the same layout (local + S3,
// path-only) for the console's listing — keep the two in sync if the layout
// ever changes.
func DiscoverBaselines(dir string) ([]BaselineInfo, error) {
	infos, _, err := DiscoverBaselinesReport(dir)
	return infos, err
}

// DiscoverBaselinesReport is DiscoverBaselines plus the snapshot times of the
// folders it skipped because they could not be read (a snapshot folder, or a
// schema folder inside one). A caller that grades the newest snapshot must
// not grade over a skipped folder at or after it (#1639).
func DiscoverBaselinesReport(dir string) ([]BaselineInfo, []time.Time, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	var unreadable []time.Time

	var results []BaselineInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		ts, ok := parseBaselineDirTimestamp(entry.Name())
		if !ok {
			continue
		}

		snapshotDir := filepath.Join(dir, entry.Name())
		// Skip a partially-converted snapshot (#467): an _INCOMPLETE marker
		// without _SUCCESS means a run failed mid-way. Pre-marker (legacy)
		// snapshots have neither and stay complete-by-default.
		if !SnapshotComplete(snapshotDir) {
			slog.Warn("skipping incomplete baseline snapshot", "path", snapshotDir)
			continue
		}
		dbEntries, err := os.ReadDir(snapshotDir)
		if errors.Is(err, fs.ErrNotExist) {
			continue // removed while walking
		}
		if err != nil {
			slog.Warn("could not read baseline snapshot directory", "path", snapshotDir, "error", err)
			unreadable = append(unreadable, ts)
			continue
		}
		for _, dbEntry := range dbEntries {
			if !dbEntry.IsDir() {
				continue
			}
			dbName := dbEntry.Name()
			tableDir := filepath.Join(snapshotDir, dbName)
			tableFiles, err := os.ReadDir(tableDir)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				slog.Warn("could not read baseline table directory", "path", tableDir, "error", err)
				unreadable = append(unreadable, ts)
				continue
			}
			for _, tf := range tableFiles {
				if tf.IsDir() || !strings.HasSuffix(tf.Name(), ".parquet") {
					continue
				}
				tableName := strings.TrimSuffix(tf.Name(), ".parquet")
				filePath := filepath.Join(tableDir, tf.Name())

				info := BaselineInfo{
					SnapshotTime: ts,
					Database:     dbName,
					Table:        tableName,
					Path:         filePath,
				}

				// Best-effort: read binlog position from Parquet metadata.
				if meta, err := ReadParquetMetadata(filePath); err == nil {
					info.BinlogFile = meta.BinlogFile
					info.BinlogPos = meta.BinlogPos
					info.GTIDSet = meta.GTIDSet
					info.LSN = meta.LSN
				} else {
					slog.Warn("could not read Parquet metadata for baseline", "path", filePath, "error", err)
				}

				results = append(results, info)
			}
		}
	}
	return results, unreadable, nil
}

// parseBaselineDirTimestamp converts a baseline directory name like
// "2025-02-28T00-00-00Z" to a time.Time. The format is RFC3339 with colons
// in the time portion replaced by hyphens for filesystem compatibility.
func parseBaselineDirTimestamp(name string) (time.Time, bool) {
	idx := strings.IndexByte(name, 'T')
	if idx < 0 {
		return time.Time{}, false
	}
	rfc := name[:idx+1] + strings.ReplaceAll(name[idx+1:], "-", ":")
	t, err := time.Parse(time.RFC3339, rfc)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}
