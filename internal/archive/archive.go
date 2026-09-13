// Package archive writes a binlog_events partition to a Parquet file before
// it is dropped. The output uses the same column schema as baseline Parquet
// files so the two datasets can be joined for full audit reconstruction.
package archive

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// BinlogEventColumns defines the 15 non-generated binlog_events columns in
// MySQL table order (pk_hash is a stored generated column and is omitted).
// Exported for reuse by the buffer package when writing Parquet files.
//
// event_id / start_pos / end_pos are BIGINT UNSIGNED in MySQL. event_id is
// AUTO_INCREMENT (counts from 1, never near 2^63), so its signed Int64 column
// is lossless. start_pos/end_pos are NOT bounded the same way: pre-#1180
// builds stored the MariaDB underflow shape (StartPos = 2^64 - EventSize,
// #986/#1117) in real indexes, so a legitimate stored position exceeds 2^63.
// They are therefore scanned through sql.Null[uint64] (the #1202/#1217
// pattern from internal/query) and mapped to UNSIGNED Uint(64) parquet columns
// (#1218) — widening only the scan against the old signed Int(64) columns
// would have written a silently wrapped negative to disk. Archives written
// before #1218 keep signed Int(64) positions forever; parquetquery reads the
// two generations together in one union_by_name scan (DuckDB promotes the
// mixed column to HUGEINT) and widens its scan targets to match — see its
// scanRows. connection_id needs the same treatment one size down: it is INT
// UNSIGNED (a CONNECTION_ID() can exceed int32's 2147483647) and is written
// via FormatUint by the buffer path, so a signed Int(32) column would reject
// it.
var BinlogEventColumns = []baseline.Column{
	{Name: "event_id", MySQLType: "bigint", ParquetType: baseline.MysqlToParquetNode("bigint")},
	{Name: "binlog_file", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "start_pos", MySQLType: "bigint", Unsigned: true, ParquetType: baseline.MysqlToParquetNode2("bigint", true)},
	{Name: "end_pos", MySQLType: "bigint", Unsigned: true, ParquetType: baseline.MysqlToParquetNode2("bigint", true)},
	{Name: "event_timestamp", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	{Name: "gtid", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "connection_id", MySQLType: "int", Unsigned: true, ParquetType: baseline.MysqlToParquetNode2("int", true)},
	{Name: "schema_name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "table_name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "event_type", MySQLType: "tinyint", ParquetType: baseline.MysqlToParquetNode("tinyint")},
	{Name: "pk_values", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "changed_columns", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "row_before", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "row_after", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "schema_version", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	{Name: "query_text", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
	{Name: "query_hash", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	// commit_ts_us is BIGINT UNSIGNED microseconds since epoch (#18); the Int64
	// Parquet node it maps to holds it losslessly (epoch µs stays far below
	// 2^63 — year 294247).
	{Name: "commit_ts_us", MySQLType: "bigint", Unsigned: true, ParquetType: baseline.MysqlToParquetNode2("bigint", true)},
}

// Stats summarizes what ArchivePartition wrote: the row count and the
// content-derived event_timestamp range of the archived rows. MinEventTS and
// MaxEventTS are the zero time when the partition had no rows (or none with a
// non-NULL event_timestamp). They exist because a partition's NAME is not a
// reliable statement of what it holds (#1037): backfilled events older than
// the oldest live RANGE partition boundary land in that oldest partition, so
// the file archived under its hour label can contain rows from much earlier
// hours. Callers record this true range in archive_state so time-scoped
// pruning consults content, not just the label.
type Stats struct {
	Rows       int64
	MinEventTS time.Time
	MaxEventTS time.Time
	// Columns is the canonical column set of the file that was just written
	// (#1535), for the caller to record in archive_state.column_set. Reported by
	// the writer rather than read back off BinlogEventColumns at the call site
	// so the record cannot drift from the bytes: a build that ever writes a
	// narrower file records the narrower set.
	Columns string
}

// ColumnSet renders a Parquet column list as the canonical archive_state.column_set
// value: lowercase names, SORTED, comma-joined.
//
// Sorted, because it is a grouping KEY (#1535). Two files holding the same
// columns in different physical order are one schema group and must produce one
// string; grouping on write order would split them for nothing, and each extra
// group costs a footer read at bind time — the exact cost this exists to remove.
func ColumnSet(cols []baseline.Column) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = strings.ToLower(c.Name)
	}
	return ColumnSetOf(names)
}

// ColumnSetOf is ColumnSet over bare names — what a footer read produces.
// Deduplicates: a repeated name would make the set a multiset and split a group
// that is really the same schema.
func ColumnSetOf(names []string) string {
	lower := make([]string, 0, len(names))
	for _, n := range names {
		n = strings.ToLower(strings.TrimSpace(n))
		if n != "" && !slices.Contains(lower, n) {
			lower = append(lower, n)
		}
	}
	slices.Sort(lower)
	return strings.Join(lower, ",")
}


// ArchivePartition writes all rows from the named partition of binlog_events
// to a Parquet file at outputPath. The db must have been opened with
// parseTime=true (i.e. via config.Connect) so DATETIME columns scan as
// time.Time. Returns Stats for the rows written (count + content min/max
// event_timestamp).
//
// The file is written to outputPath+".tmp" and atomically renamed into place
// only after the write completes and the writer is closed (issue #802): a
// process kill, OOM, or host reboot mid-write can never leave a truncated,
// no-footer file at outputPath — either the rename happened (a complete,
// valid file) or it didn't (outputPath is untouched). A stale .tmp file left
// by an earlier crash is silently overwritten by the next run (os.Create
// truncates it) and is otherwise ignored — nothing ever reads from it.
//
// On error the partial .tmp output file is removed before returning.
func ArchivePartition(ctx context.Context, db *sql.DB, dbName, partition, outputPath, compression string) (Stats, error) {
	tmpPath := outputPath + ".tmp"
	cfg := baseline.WriterConfig{
		Compression:  compression,
		RowGroupSize: 500_000,
		Metadata: map[string]string{
			"bintrail.archive.partition": partition,
			"bintrail.archive.timestamp": time.Now().UTC().Format(time.RFC3339),
			"bintrail.archive.version":   "1.0.0",
		},
	}

	w, err := baseline.NewWriter(tmpPath, BinlogEventColumns, cfg)
	if err != nil {
		return Stats{}, fmt.Errorf("create parquet writer: %w", err)
	}

	var closed bool
	defer func() {
		if !closed {
			w.Close()          //nolint
			os.Remove(tmpPath) //nolint
		}
	}()

	q := fmt.Sprintf(
		"SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp,"+
			" gtid, connection_id, schema_name, table_name, event_type, pk_values,"+
			" changed_columns, row_before, row_after, schema_version, query_text, query_hash, commit_ts_us"+
			" FROM `%s`.`binlog_events` PARTITION (`%s`) ORDER BY event_id",
		dbName, partition,
	)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return Stats{}, fmt.Errorf("query partition %s: %w", partition, err)
	}
	defer rows.Close()

	// Reported from the SAME list the writer above was built with, so the
	// recorded set is the file's set by construction (#1535).
	stats := Stats{Columns: ColumnSet(BinlogEventColumns)}
	for rows.Next() {
		// Every NOT NULL column is scanned defensively and its
		// Valid bit is propagated into the nulls[] slice so the Parquet
		// writer preserves true NULL. See dbtrail/bintrail#318 for the
		// production observation that "NOT NULL" cannot be trusted in
		// drifted/external-pipeline-fed indexes. event_id stays a bare
		// uint64 since AUTO_INCREMENT cannot return NULL.
		// start_pos/end_pos scan through sql.Null[uint64], not sql.NullInt64:
		// a stored position above 2^63 (the #986/#1117 MariaDB underflow
		// shape written by pre-#1180 builds) failed the int64 Scan and the
		// partition could never archive — so rotation could never drop it
		// (#1218). The mysql driver hands back uint64 (TEXT protocol) or
		// []byte above 2^63 (BINARY protocol); convertAssign takes both
		// losslessly into uint64, same as internal/query's scanRows.
		var (
			eventID        uint64
			binlogFile     sql.NullString
			startPos       sql.Null[uint64]
			endPos         sql.Null[uint64]
			eventTimestamp sql.NullTime
			gtid           sql.NullString
			connID         sql.NullInt64
			schemaName     sql.NullString
			tableName      sql.NullString
			eventType      sql.NullInt32
			pkValues       sql.NullString
			changedColumns []byte // nil = NULL
			rowBefore      []byte // nil = NULL
			rowAfter       []byte // nil = NULL
			schemaVersion  sql.NullInt32
			queryText      sql.NullString
			queryHash      sql.NullString
			commitTsUS     sql.NullInt64
		)
		if err := rows.Scan(
			&eventID, &binlogFile, &startPos, &endPos, &eventTimestamp,
			&gtid, &connID, &schemaName, &tableName, &eventType, &pkValues,
			&changedColumns, &rowBefore, &rowAfter, &schemaVersion, &queryText, &queryHash,
			&commitTsUS,
		); err != nil {
			return stats, fmt.Errorf("scan row: %w", err)
		}

		connIDStr := ""
		if connID.Valid {
			connIDStr = strconv.FormatInt(connID.Int64, 10)
		}
		commitTsStr := ""
		if commitTsUS.Valid {
			commitTsStr = strconv.FormatInt(commitTsUS.Int64, 10)
		}
		eventTimestampStr := ""
		if eventTimestamp.Valid {
			ts := eventTimestamp.Time.UTC()
			eventTimestampStr = ts.Format("2006-01-02 15:04:05")
			// Content-derived time range of the file (#1037). Tracked from the
			// exact rows written — not a separate MIN()/MAX() query — so the
			// recorded range can never disagree with the file's contents.
			if stats.MinEventTS.IsZero() || ts.Before(stats.MinEventTS) {
				stats.MinEventTS = ts
			}
			if stats.MaxEventTS.IsZero() || ts.After(stats.MaxEventTS) {
				stats.MaxEventTS = ts
			}
		}

		values := []string{
			strconv.FormatUint(eventID, 10),
			binlogFile.String,
			strconv.FormatUint(startPos.V, 10),
			strconv.FormatUint(endPos.V, 10),
			eventTimestampStr,
			gtid.String,
			connIDStr,
			schemaName.String,
			tableName.String,
			strconv.FormatInt(int64(eventType.Int32), 10),
			pkValues.String,
			string(changedColumns),
			string(rowBefore),
			string(rowAfter),
			strconv.FormatInt(int64(schemaVersion.Int32), 10),
			queryText.String,
			queryHash.String,
			commitTsStr,
		}
		nulls := []bool{
			false, // event_id (AUTO_INCREMENT, cannot be NULL)
			!binlogFile.Valid,
			!startPos.Valid,
			!endPos.Valid,
			!eventTimestamp.Valid,
			!gtid.Valid,
			!connID.Valid,
			!schemaName.Valid,
			!tableName.Valid,
			!eventType.Valid,
			!pkValues.Valid,
			changedColumns == nil,
			rowBefore == nil,
			rowAfter == nil,
			!schemaVersion.Valid,
			!queryText.Valid,
			!queryHash.Valid,
			!commitTsUS.Valid,
		}

		if err := w.WriteRow(values, nulls); err != nil {
			return stats, fmt.Errorf("write row: %w", err)
		}
		stats.Rows++
	}
	if err := rows.Err(); err != nil {
		return stats, fmt.Errorf("iterate rows: %w", err)
	}

	closed = true
	if err := w.Close(); err != nil {
		os.Remove(tmpPath) //nolint
		return stats, fmt.Errorf("close writer: %w", err)
	}
	if err := os.Rename(tmpPath, outputPath); err != nil {
		os.Remove(tmpPath) //nolint
		return stats, fmt.Errorf("rename %s into place: %w", tmpPath, err)
	}
	return stats, nil
}

// ValidateArchiveFile opens the Parquet file at path just far enough to read
// its trailing footer metadata (row-group index, schema, row count) without
// decoding any row data. A file left partially written by a crash mid-write
// (issue #802: kill -9, OOM, host reboot — none of which run Go's
// defer-based cleanup) has no valid footer and parquet.OpenFile fails on it,
// even though the file's size is greater than zero. Callers deciding whether
// an on-disk archive can be trusted (e.g. rotation's --retry skip, or
// deciding it is safe to drop the source partition) must not rely on
// size > 0 alone — they should use this instead. Returns the row count
// recorded in the footer on success.
func ValidateArchiveFile(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return 0, fmt.Errorf("invalid or truncated parquet footer: %w", err)
	}
	return pf.NumRows(), nil
}
