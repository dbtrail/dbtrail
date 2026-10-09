// Package query — snapshot.go: merges mydumper baseline Parquet rows as a
// third source for `bintrail query --include-snapshot`.
//
// Baseline rows are emitted as synthetic events with EventType=EventSnapshot
// and EventTimestamp=baseline_creation_ts (read from the Parquet file's
// `bintrail.snapshot_timestamp` key-value metadata). They slot into the
// existing MergeAndTrim pipeline alongside live-MySQL and archive events.
package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// snapshotEventIDBase is OR'd with the row index to synthesise a ResultRow
// EventID high enough that collision with MySQL's AUTO_INCREMENT `event_id`
// (BIGINT UNSIGNED — see internal/indexer) is effectively impossible in
// practice: at 2^62 a real index would need to accumulate ~4.6e18 rows to
// reach this range. MergeResults dedups by event_id, so uniqueness across
// snapshot rows is enough.
const snapshotEventIDBase uint64 = 1 << 62

// FetchSnapshot reads a single mydumper baseline Parquet file and returns
// matching rows as synthetic SNAPSHOT events. path must point at a
// <schema>/<table>.parquet file produced by `bintrail baseline` (local or
// s3:// URL).
//
// Filters that don't apply to baseline rows (gtid, changed-column, flag)
// exclude the whole snapshot source. --event-type SNAPSHOT keeps only
// snapshot rows; any other event-type filter excludes them. --since/--until
// apply to the baseline's recorded creation timestamp.
//
// Returns nil, nil when filters exclude all snapshot rows — this is not an
// error. An slog.Info line is emitted at each exclusion so operators who
// pass --include-snapshot but see "No results" can tell why.
func FetchSnapshot(ctx context.Context, path string, opts Options) ([]ResultRow, error) {
	if reason, skip := shouldSkipSnapshot(opts); skip {
		slog.Info("snapshot source excluded by filter", "reason", reason)
		return nil, nil
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if strings.HasPrefix(path, "s3://") {
		// At-rest integrity (#636/#698): pre-pass-stream the S3 object through
		// CRC-32C against its snapshot's _MANIFEST before parquet_scan reads its
		// rows as snapshot events. Runs before LoadHTTPFS so a corrupt object
		// fails loud without DuckDB ever touching S3.
		if err := baselineintegrity.ValidateS3File(ctx, path); err != nil {
			return nil, err
		}
		if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
			return nil, fmt.Errorf("load httpfs: %w", err)
		}
		if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
			return nil, err
		}
	} else if err := baselineintegrity.ValidateLocalFile(path); err != nil {
		// At-rest integrity (#636): fail loud on a corrupt local baseline before
		// `query --include-snapshot` reads its rows as snapshot events — the third
		// local baseline read path, validated like reconstruct and recover.
		return nil, err
	}

	ts, err := readSnapshotTimestamp(ctx, db, path)
	if err != nil {
		return nil, err
	}
	if opts.Since != nil && ts.Before(*opts.Since) {
		slog.Info("snapshot source excluded by filter", "reason", "baseline timestamp before --since", "snapshot_ts", ts, "since", *opts.Since)
		return nil, nil
	}
	if opts.Until != nil && ts.After(*opts.Until) {
		slog.Info("snapshot source excluded by filter", "reason", "baseline timestamp after --until", "snapshot_ts", ts, "until", *opts.Until)
		return nil, nil
	}

	where, args, err := snapshotFilters(opts)
	if err != nil {
		return nil, err
	}
	safePath := strings.ReplaceAll(duckdbutil.FileGlob(path), "'", "''")
	q := "SELECT * FROM parquet_scan('" + safePath + "')"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	if opts.Limit > 0 {
		// ORDER BY ALL pins WHICH rows the LIMIT keeps: without it, DuckDB's
		// top-N over parquet_scan depends on row-group layout and scan
		// parallelism, so repeated runs return different baseline subsets
		// (#839). Ties under ORDER BY ALL are byte-identical rows, so both the
		// selected set and the synthetic EventID assignment below (which
		// MergeAndTrim tie-breaks on) are deterministic.
		q += fmt.Sprintf(" ORDER BY ALL LIMIT %d", opts.Limit)
	}

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("snapshot query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("snapshot columns: %w", err)
	}

	var results []ResultRow
	idx := uint64(0)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan snapshot row: %w", err)
		}
		rowAfter := make(map[string]any, len(cols))
		for i, c := range cols {
			rowAfter[c] = normalizeSnapshotValue(vals[i])
		}
		results = append(results, ResultRow{
			EventID:        snapshotEventIDBase | idx,
			EventTimestamp: ts,
			SchemaName:     opts.Schema,
			TableName:      opts.Table,
			EventType:      event.EventSnapshot,
			RowAfter:       rowAfter,
		})
		idx++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot rows: %w", err)
	}
	return results, nil
}

// shouldSkipSnapshot reports whether any filter in opts rules out the entire
// snapshot source before the DuckDB query runs. It is the unit-testable
// truth table for the five always-excluder filters: wrong event-type, gtid,
// changed-column, flag, query-hash. Extracted so the branches are testable without
// standing up DuckDB.
//
// Returns (reason, true) when the source must be skipped, with a
// human-readable reason for the slog message. Returns ("", false) otherwise.
func shouldSkipSnapshot(opts Options) (string, bool) {
	if opts.EventType != nil && *opts.EventType != event.EventSnapshot {
		return "--event-type ≠ SNAPSHOT", true
	}
	if opts.GTID != "" {
		return "--gtid set (baseline rows carry no GTID)", true
	}
	if opts.ChangedColumn != "" {
		return "--changed-column set (baseline rows have no changed-columns metadata)", true
	}
	if opts.QueryHash != "" {
		// A baseline row is a materialised row image, not a statement's effect:
		// no statement produced it, so it can never carry the digest asked for.
		// Skipping the whole source is what the other always-excluders do, and
		// it keeps the filter honest — a snapshot row surviving a
		// statement-scoped query would claim provenance it does not have.
		return "--query-hash set (baseline rows carry no statement digest)", true
	}
	if opts.Flag != "" {
		return "--flag set (baseline rows do not carry table_flags)", true
	}
	return "", false
}

// readSnapshotTimestamp resolves the instant a baseline file's rows describe,
// for the --since/--until filter. Returns a non-nil error when neither source
// below answers: the caller cannot apply the filter without it.
//
// The DIRECTORY name is preferred, and the footer is the fallback. That order
// matters and it is not the order this used to use.
//
// A table whose delta window held no events is published by carrying its
// previous Parquet file forward unchanged, so its footer keeps the OLDER
// bintrail.snapshot_timestamp while it sits in the newer snapshot's directory.
// Reading the footer first dated two files in the SAME snapshot differently,
// purely by whether each table happened to be cold, and a `--since` past the
// stale value dropped the cold table's baseline rows with a log line and no
// error. Every other discovery path in the tree (FindBaseline, ListBaselines,
// status's staleness grading) already treats the directory as authoritative;
// this was the one that did not.
//
// The footer fallback still earns its place: a file read from outside a
// snapshot layout has no directory to ask.
func readSnapshotTimestamp(ctx context.Context, db *sql.DB, path string) (time.Time, error) {
	// <root>/<timestamp>/<schema>/<table>.parquet — two levels up. Lexical on
	// purpose so it works for an s3:// URL as well as a local path.
	if dir := path[:strings.LastIndex(path, "/")+1]; dir != "" {
		trimmed := strings.TrimSuffix(dir, "/")
		if i := strings.LastIndex(trimmed, "/"); i >= 0 {
			snapDir := trimmed[:i]
			if j := strings.LastIndex(snapDir, "/"); j >= 0 {
				snapDir = snapDir[j+1:]
			}
			if ts, ok := snapshotdir.ParseTime(snapDir); ok {
				return ts, nil
			}
		}
	}
	return readSnapshotTimestampFromFooter(ctx, db, path)
}

func readSnapshotTimestampFromFooter(ctx context.Context, db *sql.DB, path string) (time.Time, error) {
	safePath := strings.ReplaceAll(duckdbutil.FileGlob(path), "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT key, value FROM parquet_kv_metadata('"+safePath+"')")
	if err != nil {
		return time.Time{}, fmt.Errorf("read baseline metadata: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var keyBytes, valBytes []byte
		if err := rows.Scan(&keyBytes, &valBytes); err != nil {
			return time.Time{}, fmt.Errorf("scan metadata row: %w", err)
		}
		if string(keyBytes) != "bintrail.snapshot_timestamp" {
			continue
		}
		t, err := time.Parse(time.RFC3339, string(valBytes))
		if err != nil {
			return time.Time{}, fmt.Errorf("parse snapshot_timestamp %q: %w", string(valBytes), err)
		}
		return t.UTC(), nil
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, fmt.Errorf("iterate metadata: %w", err)
	}
	return time.Time{}, fmt.Errorf("baseline %s missing bintrail.snapshot_timestamp metadata — re-run `bintrail baseline`", path)
}

// snapshotFilters returns WHERE fragments and bind args matching opts.ColumnEq.
// PKValues/PKValuesIn are NOT applied here — this PR does not build PK_values
// for snapshot rows, and combining --pk/--pks with --include-snapshot is
// rejected at the CLI validation layer. Schema/table/since/until are handled
// against the baseline's own metadata before the query runs.
//
// A ColumnEq entry whose column fails IsSafeColumnName returns an error —
// emitting a silent "1=0" clause would leave the operator seeing "No results"
// with a reason that's only in structured logs.
func snapshotFilters(opts Options) ([]string, []any, error) {
	var where []string
	var args []any
	if opts.PKRange != nil {
		// Same reason as --pk/--pks: snapshot rows carry no pk_values, so
		// a range over them cannot be evaluated. The CLI refuses the
		// combination first; this keeps a hand-built Options honest.
		return nil, nil, errors.New("snapshot: a primary key range cannot be applied to snapshot rows (they carry no pk_values)")
	}
	for _, ce := range opts.ColumnEq {
		if !IsSafeColumnName(ce.Column) {
			return nil, nil, fmt.Errorf("snapshot: unsafe column name %q in --column-eq; must match [A-Za-z_][A-Za-z0-9_]*", ce.Column)
		}
		ident := `"` + strings.ReplaceAll(ce.Column, `"`, `""`) + `"`
		if ce.IsNull {
			where = append(where, ident+" IS NULL")
			continue
		}
		// Cast to VARCHAR so string-typed --column-eq values match typed
		// Parquet columns (int, date, etc.) the same way the binlog index's
		// JSON_UNQUOTE(JSON_EXTRACT(...)) path coerces stored values to
		// strings before comparison.
		where = append(where, "CAST("+ident+" AS VARCHAR) = ?")
		args = append(args, ce.Value)
	}
	return where, args, nil
}

// normalizeSnapshotValue converts DuckDB scan outputs into types that
// encoding/json can marshal cleanly. Byte slices are parsed as JSON only when
// the payload's first non-whitespace byte is '{' or '[' — this avoids silently
// promoting a TEXT column containing the literal "null" to Go nil, or "123"
// to a number, which would diverge from the binlog index's string-preserving
// storage. time.Time is formatted MySQL-style; everything else passes through.
func normalizeSnapshotValue(v any) any {
	switch x := v.(type) {
	case []byte:
		if looksLikeJSONContainer(x) && json.Valid(x) {
			var decoded any
			if err := json.Unmarshal(x, &decoded); err == nil {
				return decoded
			}
		}
		return string(x)
	case time.Time:
		return x.UTC().Format("2006-01-02 15:04:05")
	default:
		return v
	}
}

// looksLikeJSONContainer reports whether b's first non-whitespace byte is
// '{' or '[' — a cheap prefix test that distinguishes JSON object/array
// payloads (which MySQL stores in JSON columns) from bare string/numeric
// literals that json.Valid would also accept.
func looksLikeJSONContainer(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}
