package parquetquery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// BelowMark is the binlog-renumbering question asked of one archive file
// (#2186): is there a change of Schema.Table indexed after the event mark
// (event_id > AfterID), recorded in [From, Until] (a zero bound: none), whose
// END sorts before the mark's end in the binary log (shorter file name first,
// then name, then position, as the read orders them), or that sits under
// another base name (Base, when the mark's file has one)? The read takes such
// a change by position from the snapshot and drops it. The DuckDB twin of the
// live check's firstBelowMark (internal/reconstruct).
type BelowMark struct {
	Schema, Table string
	AfterID       uint64
	MarkFile      string
	MarkEnd       uint64
	// Base is the mark file's base name ("binlog" for "binlog.000007"); ""
	// when the name has no numeric extension, which leaves the base-name
	// clause out, as the live check does.
	Base        string
	From, Until time.Time
}

// BelowMarkRow is the change FirstBelowMark found.
type BelowMarkRow struct {
	EventID uint64
	File    string
	End     uint64
}

// FirstBelowMark asks q of one archive file: a local path, or an s3:// URL
// (downloaded to a temporary file, as Fetch reads S3, and removed after). It
// reads only the columns the question names, so it costs a fraction of the
// read that scans the same file for the same table. found false: no such
// change in the file.
func FirstBelowMark(ctx context.Context, file string, q BelowMark) (row BelowMarkRow, found bool, err error) {
	path := file
	if strings.HasPrefix(file, "s3://") {
		bucket, _, _ := strings.Cut(strings.TrimPrefix(file, "s3://"), "/")
		client, _, err := s3ClientForBucket(ctx, bucket)
		if err != nil {
			return row, false, fmt.Errorf("open the S3 bucket of %s: %w", file, err)
		}
		if path, err = newS3Downloader(client).download(ctx, file); err != nil {
			return row, false, err
		}
		defer removeTempFile(path)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return row, false, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	duckdbutil.SetTempDirectory(ctx, db)
	duckdbutil.DefaultTuning().Apply(ctx, db)
	stmt, args := belowMarkQuery(path, q)
	err = db.QueryRowContext(ctx, stmt, args...).Scan(&row.EventID, &row.File, &row.End)
	switch {
	case err == nil:
		return row, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return row, false, nil
	}
	return row, false, fmt.Errorf("read %s: %w", file, err)
}

// belowMarkQuery is FirstBelowMark's statement over one local file.
func belowMarkQuery(path string, q BelowMark) (string, []any) {
	where := []string{"schema_name = ?", "table_name = ?", "event_id > ?"}
	args := []any{q.Schema, q.Table, q.AfterID}
	if !q.From.IsZero() {
		where = append(where, "event_timestamp >= ?")
		args = append(args, q.From.UTC())
	}
	if !q.Until.IsZero() {
		where = append(where, "event_timestamp <= ?")
		args = append(args, q.Until.UTC())
	}
	below := "length(binlog_file) < length(?)" +
		" OR (length(binlog_file) = length(?) AND binlog_file < ?)" +
		" OR (binlog_file = ? AND end_pos < ?)"
	args = append(args, q.MarkFile, q.MarkFile, q.MarkFile, q.MarkFile, posArg(q.MarkEnd))
	if q.Base != "" {
		below += " OR left(binlog_file, length(?) + 1) <> (? || '.')"
		args = append(args, q.Base, q.Base)
	}
	where = append(where, "("+below+")")
	return "SELECT event_id, binlog_file, end_pos FROM parquet_scan('" + strings.ReplaceAll(path, "'", "''") +
		"', union_by_name=true) WHERE " + strings.Join(where, " AND ") + " LIMIT 1", args
}
