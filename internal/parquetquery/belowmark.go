package parquetquery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// ErrArchiveObjectMissing: an archive file's S3 object is not there (404),
// as when its upload never completed and the local copy is gone. The read,
// which lists archives by prefix, does not see it either; the
// binlog-renumbering check reports it as an archive it cannot open (#2186).
var ErrArchiveObjectMissing = errors.New("archive object not found")

// BelowMark is the binlog-renumbering question asked of one archive file
// (#2186): the changes of Schema.Table indexed after the event mark
// (event_id > AfterID), recorded in [From, Until] (a zero bound: none), whose
// END sorts before the mark's end in the binary log (shorter file name first,
// then name, then position, as the read orders them), or that sit under
// another base name (Base, when the mark's file has one). The read takes such
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

// BelowMarkRow is one change the question found.
type BelowMarkRow struct {
	EventID uint64
	File    string
	End     uint64
	At      time.Time
}

// BelowMarkSpan is what FirstBelowMark found: the earliest and the latest
// such change by recorded time. Found false: none.
type BelowMarkSpan struct {
	Found            bool
	Earliest, Latest BelowMarkRow
}

// FirstBelowMark asks q of one archive file: a local path, or an s3:// URL
// (downloaded to a temporary file, as Fetch reads S3, and removed after; a
// missing object is ErrArchiveObjectMissing). It reads only the columns the
// question names, in one pass, so it costs a fraction of the read that scans
// the same file for the same table.
func FirstBelowMark(ctx context.Context, file string, q BelowMark) (BelowMarkSpan, error) {
	var span BelowMarkSpan
	err := withLocalArchive(ctx, file, func(db *sql.DB, path string) error {
		stmt, args := belowMarkQuery(path, q)
		var n int64
		var e, l struct {
			id, end sql.Null[uint64]
			file    sql.NullString
			at      sql.NullTime
		}
		if err := db.QueryRowContext(ctx, stmt, args...).Scan(&n,
			&e.id, &e.file, &e.end, &e.at, &l.id, &l.file, &l.end, &l.at); err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		span.Found = true
		span.Earliest = BelowMarkRow{EventID: e.id.V, File: e.file.String, End: e.end.V, At: e.at.Time.UTC()}
		span.Latest = BelowMarkRow{EventID: l.id.V, File: l.file.String, End: l.end.V, At: l.at.Time.UTC()}
		return nil
	})
	return span, err
}

// EventByID reads the change with event_id id from one archive file (local
// or s3://), as FirstBelowMark opens it: its binlog file and end. found false:
// the file does not hold it.
func EventByID(ctx context.Context, file string, id uint64) (row BelowMarkRow, found bool, err error) {
	err = withLocalArchive(ctx, file, func(db *sql.DB, path string) error {
		e := db.QueryRowContext(ctx, "SELECT binlog_file, end_pos FROM parquet_scan('"+strings.ReplaceAll(path, "'", "''")+
			"', union_by_name=true) WHERE event_id = ? LIMIT 1", id).Scan(&row.File, &row.End)
		switch {
		case e == nil:
			row.EventID, found = id, true
			return nil
		case errors.Is(e, sql.ErrNoRows):
			return nil
		}
		return e
	})
	return row, found, err
}

// withLocalArchive runs fn over file on a fresh DuckDB session, downloading an
// s3:// file first.
func withLocalArchive(ctx context.Context, file string, fn func(db *sql.DB, path string) error) error {
	path := file
	if strings.HasPrefix(file, "s3://") {
		bucket, _, _ := strings.Cut(strings.TrimPrefix(file, "s3://"), "/")
		client, _, err := s3ClientForBucket(ctx, bucket)
		if err != nil {
			return fmt.Errorf("open the S3 bucket of %s: %w", file, err)
		}
		if path, err = newS3Downloader(client).download(ctx, file); err != nil {
			if objectMissing(err) {
				return fmt.Errorf("%s: %w", file, ErrArchiveObjectMissing)
			}
			return err
		}
		defer removeTempFile(path)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	duckdbutil.SetTempDirectory(ctx, db)
	duckdbutil.DefaultTuning().Apply(ctx, db)
	if err := fn(db, path); err != nil {
		return fmt.Errorf("read %s: %w", file, err)
	}
	return nil
}

// objectMissing reports an S3 "no such object" in any of the shapes S3 and
// S3-compatible stores return it (storage.S3Backend.Exists reads the same).
// AWS answers 403, not 404, for a missing key when the caller may not list
// the bucket (no s3:ListBucket): that stays an error, since a 403 cannot be
// told from a real permission problem.
func objectMissing(err error) bool {
	var nsk *types.NoSuchKey
	var nf *types.NotFound
	var re *smithyhttp.ResponseError
	return errors.As(err, &nsk) || errors.As(err, &nf) || (errors.As(err, &re) && re.Response != nil && re.Response.StatusCode == 404)
}

// belowMarkQuery is FirstBelowMark's statement over one local file: the
// count of matching changes, and the earliest and the latest of them by
// recorded time (event_id breaks a tie).
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
	const first = "arg_min(%s, (event_timestamp, event_id))"
	const last = "arg_max(%s, (event_timestamp, event_id))"
	cols := []string{"count(*)"}
	for _, f := range []string{first, last} {
		for _, c := range []string{"event_id", "binlog_file", "end_pos", "event_timestamp"} {
			cols = append(cols, fmt.Sprintf(f, c))
		}
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM parquet_scan('" + strings.ReplaceAll(path, "'", "''") +
		"', union_by_name=true) WHERE " + strings.Join(where, " AND "), args
}
