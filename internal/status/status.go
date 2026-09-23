// Package status provides shared types and display helpers for the binlog index status.
// It is used by both cmd/bintrail/status.go and cmd/bintrail-mcp/main.go.
package status

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/go-sql-driver/mysql"
)

// IndexStateRow holds one row from the index_state table.
type IndexStateRow struct {
	BinlogFile    string
	Status        string
	EventsIndexed int64
	FileSize      int64
	LastPosition  int64
	StartedAt     time.Time
	CompletedAt   sql.NullTime
	ErrorMessage  sql.NullString
	BintrailID    sql.NullString
}

// PartitionStat holds one partition row from information_schema.PARTITIONS.
type PartitionStat struct {
	Name        string
	Description string // LESS THAN value (integer TO_SECONDS value) or "MAXVALUE"
	TableRows   int64  // estimate from information_schema
	Ordinal     int
}

// ServerInfo holds one active row from the bintrail_servers table.
type ServerInfo struct {
	BintrailID       string
	ServerUUID       string
	Host             string
	Port             uint16
	Username         string
	CreatedAt        time.Time
	DecommissionedAt sql.NullTime
}

// ArchiveStats holds aggregate statistics from the archive_state table.
// A single archive file may be both local and in S3, so
// LocalFiles + S3Files may exceed TotalFiles.
type ArchiveStats struct {
	TotalFiles     int
	TotalRows      int64
	TotalSizeBytes int64
	LocalFiles     int
	S3Files        int
	S3Buckets      []string // distinct non-empty buckets
}

// SchemaChange holds one row from the schema_changes table.
type SchemaChange struct {
	ID         int
	DetectedAt time.Time
	BinlogFile string
	SchemaName string
	TableName  string
	DDLType    string
	SnapshotID sql.NullInt32
}

// StreamStateInfo holds the single row from the stream_state table.
type StreamStateInfo struct {
	Mode           string // "position" or "gtid"
	BinlogFile     string
	BinlogPosition uint64
	GTIDSet        sql.NullString
	EventsIndexed  int64
	LastEventTime  sql.NullTime
	LastCheckpoint time.Time
	ServerID       uint32
	BintrailID     sql.NullString
	// GapLostAt / GapLostDetail record that the stream permanently lost data it could
	// not recover — an unfillable binlog gap (MySQL) or an invalidated/lost replication
	// slot (PostgreSQL, #532). When set, the index is valid only up to the gap and
	// capture must be re-baselined to resume; status surfaces them loudly. GapLostAt is
	// authoritative (the badge is gated on it alone); GapLostDetail is supplementary
	// human-readable context and may be absent. Both writers set them atomically.
	GapLostAt     sql.NullTime
	GapLostDetail sql.NullString
	// GapColumnsPresent is true when the gap_lost_* columns existed and were read
	// (the normal, migrated index). It is false only on a legacy index whose
	// schema predates those columns, read before any migration — there the gap
	// state was never evaluable, so the continuity verdict is "unknown" (not a
	// false "ok" asserted from absent data), and --fail-on-gap fails closed.
	GapColumnsPresent bool
	// SourceHealth is the latest source-side health snapshot a streaming daemon polled
	// (#599) — for PostgreSQL, a JSON document with the replication slot's wal_status/lag
	// and REPLICA IDENTITY coverage plus an embedded checked_at. Opaque here (raw JSON);
	// the console renders it and computes staleness from checked_at. Invalid on a legacy
	// index without the column or an index no daemon has polled.
	SourceHealth sql.NullString
	// CaptureSkips is the raw capture_skips JSON (#1034): per-reason monotonic
	// counters of events the streaming daemon READ and chose to DROP (e.g. the
	// column-count guard rejecting rows against a stale snapshot), shaped
	// {"<reason>":{"count":N,"last_at":"RFC3339"}}. "{}" is the affirmative
	// evaluated-and-clean marker written by a skip-aware daemon. Invalid on a
	// legacy index without the column or one no such daemon has written — the
	// Capture health verdict is then unknown and the line is omitted rather
	// than asserting OK from absent data (the GapColumnsPresent philosophy).
	CaptureSkips sql.NullString
	// CaptureSkipsAck is the raw capture_skips_ack JSON (#1314): the operator's
	// acknowledgement of the tally above, shaped
	// {"<reason>":{"count":N,"at":"RFC3339"}}. Invalid on a legacy index without
	// the column, or one nobody has acknowledged. See acknowledge.go for why an
	// acknowledgement records a COUNT rather than a fact.
	CaptureSkipsAck sql.NullString
	// scopedSkips is the capture-skip ledger already filtered to what the
	// reader may see (#1452), set only on a COPY made for one rendering — see
	// withScopedSkips. Never loaded from the database, never serialized.
	scopedSkips map[string]CaptureSkipStat
	// SchemaSnapshotAt is when the newest schema snapshot in this index was
	// taken — the layout capture decodes against today. It is the anchor that
	// makes a monotonic skip tally answerable (#1312): a skip older than this
	// cannot have been caused by the snapshot now in force. Invalid when the
	// index holds no snapshot at all, which keeps the verdict at "cannot tell"
	// rather than inventing a clean window.
	//
	// stream_state is single-row per index database, so the newest snapshot in
	// the same database belongs to the same source — the comparison cannot
	// cross sources.
	SchemaSnapshotAt sql.NullTime
}

// CaptureSkipStat is one reason's tally decoded from CaptureSkips. The JSON
// field names mirror the persistence format written by the capture daemon
// (parser.SkipStat) — kept as an independent decl so this display package does
// not import the binlog parser.
type CaptureSkipStat struct {
	Count  int64     `json:"count"`
	LastAt time.Time `json:"last_at"`
	// Last-seen attribution (#999), stamped by the capture daemon for the most
	// recent skip of a reason — present only for reasons whose detection site
	// stamps it (see parser.SkipStat, the single source of truth for which do).
	LastFile          string `json:"last_file,omitempty"`
	LastPos           uint64 `json:"last_pos,omitempty"`
	LastStatementType string `json:"last_statement_type,omitempty"`
	LastConnectionID  uint32 `json:"last_connection_id,omitempty"`
	// Tables / TablesTruncated / LastDetail mirror parser.SkipStat (#1296):
	// which tables stopped being captured, whether the capped list is complete,
	// and the newest per-skip explanation. Empty on a ledger written before
	// per-table attribution — the explanation must then name no table at all
	// rather than present the empty list as "none".
	Tables          []string `json:"tables,omitempty"`
	TablesTruncated bool     `json:"tables_truncated,omitempty"`
	LastDetail      string   `json:"last_detail,omitempty"`
	// TablesWithheld counts the names ScopeCaptureSkips removed from Tables
	// because the reader's data scope may not see them (#1452). It is a
	// rendering fact, never a persisted one: json:"-" so no ledger, however
	// written, can ever set it — the count is only ever what THIS rendering
	// withheld, and the sentence built from it says "outside your access".
	TablesWithheld int `json:"-"`
}

// CaptureSkipReasonStatementFormatDML mirrors parser.SkipStatementFormatDML —
// the persisted reason key for STATEMENT/MIXED-format DML drops (#999). Kept as
// an independent decl for the same reason CaptureSkipStat is: this display
// package deliberately does not import the binlog parser.
const CaptureSkipReasonStatementFormatDML = "statement_format_dml"

// CaptureSkipReasonUnreadablePreviousLedger mirrors
// parser.SkipUnreadablePreviousLedger — the meta-reason a restarting daemon
// stamps when the previously persisted ledger could not be parsed (#1206).
const CaptureSkipReasonUnreadablePreviousLedger = "unreadable_previous_ledger"

// ParseCaptureSkips decodes the persisted capture_skips document. ok is false
// when the verdict is not evaluable (no column / no skip-aware daemon /
// unparseable payload) — callers must then omit the Capture health verdict
// entirely, never render OK. An empty map with ok=true is the affirmative
// "evaluated, nothing skipped".
func (s *StreamStateInfo) ParseCaptureSkips() (skips map[string]CaptureSkipStat, ok bool) {
	// A rendering that already scoped the ledger to its reader hands it back
	// here (withScopedSkips) rather than re-serializing it: TablesWithheld is
	// json:"-" on purpose, so a round trip through the persisted shape would
	// lose the very count that keeps the prose honest.
	if s.scopedSkips != nil {
		return s.scopedSkips, true
	}
	if !s.CaptureSkips.Valid || strings.TrimSpace(s.CaptureSkips.String) == "" {
		return nil, false
	}
	m := map[string]CaptureSkipStat{}
	if err := json.Unmarshal([]byte(s.CaptureSkips.String), &m); err != nil {
		slog.Warn("could not parse capture_skips; capture health shown as unknown", "error", err)
		return nil, false
	}
	return m, true
}

// CoverageInfo summarizes the restore coverage of the index.
type CoverageInfo struct {
	EarliestEvent sql.NullTime
	LatestEvent   sql.NullTime
	TotalEvents   int64
	SchemaChanges int
	// UncoveredDDLs counts schema_changes rows (one per table a statement
	// changed) with snapshot_id IS NULL whose
	// DDL type NEEDS a snapshot — i.e. file-mode indexing without --source-dsn,
	// or a failed auto-snapshot (any mode). TRUNCATE TABLE rows are excluded:
	// they record snapshot_id = NULL by design (no structure change, so both
	// the stream DDL hook and the file-mode handler deliberately skip the
	// snapshot), and counting them would permanently inflate the warning.
	UncoveredDDLs int

	// Archive-derived fields (from archive_state partition names and row counts).
	ArchiveEarliestHour sql.NullTime // earliest hour derived from MIN(partition_name)
	ArchiveTotalRows    int64
	// ArchiveUnavailable marks archive coverage as UNREAD or UNPLACEABLE rather
	// than absent
	// (#816). Both states leave the two fields above zero, and reporting them
	// the same way understates the restore window: an operator reads a
	// too-recent "Earliest event" and concludes an old incident is beyond
	// recovery while the Parquet that covers it is sitting in the bucket.
	//
	// Only a genuinely missing archive_state (ER_NO_SUCH_TABLE on an index
	// that never archived) counts as "no archives"; every other outcome —
	// a table-level permission denial, a corrupt table, a legacy shape
	// missing a column, or a partition_name that will not parse — is
	// unknown, and unknown is never rendered as a fact.
	//
	// Note the scope: a dead connection or a query timeout fails the
	// binlog_events read at the top of LoadCoverage and never reaches here,
	// so this flag is specific to archive_state. That outer failure drops
	// the whole coverage section instead, which is its own gap (see the
	// CoverageErr follow-up).
	ArchiveUnavailable bool
	ArchiveError       string

	// IndexSizeBytes is the on-disk size of the binlog_events table
	// (DATA_LENGTH + INDEX_LENGTH, an InnoDB estimate). Surfaced so an operator
	// sees how much disk the live index occupies alongside its time coverage.
	IndexSizeBytes int64
}

// TSFmt is the timestamp format used in status output.
const TSFmt = "2006-01-02 15:04:05"

// LoadIndexState loads all rows from index_state ordered by bintrail_id, then started_at.
func LoadIndexState(ctx context.Context, db *sql.DB) ([]IndexStateRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT binlog_file, status, events_indexed, file_size, last_position,
		       started_at, completed_at, error_message, bintrail_id
		FROM index_state
		ORDER BY bintrail_id, started_at, binlog_file`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []IndexStateRow
	for rows.Next() {
		var r IndexStateRow
		if err := rows.Scan(
			&r.BinlogFile, &r.Status, &r.EventsIndexed, &r.FileSize, &r.LastPosition,
			&r.StartedAt, &r.CompletedAt, &r.ErrorMessage, &r.BintrailID,
		); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// LoadPartitionStats loads partition metadata for binlog_events from information_schema.
func LoadPartitionStats(ctx context.Context, db *sql.DB, dbName string) ([]PartitionStat, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, IFNULL(PARTITION_DESCRIPTION, ''),
		       PARTITION_ORDINAL_POSITION, COALESCE(TABLE_ROWS, 0)
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'
		ORDER BY PARTITION_ORDINAL_POSITION`,
		dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []PartitionStat
	for rows.Next() {
		var p PartitionStat
		if err := rows.Scan(&p.Name, &p.Description, &p.Ordinal, &p.TableRows); err != nil {
			return nil, err
		}
		stats = append(stats, p)
	}
	return stats, rows.Err()
}

// LoadArchiveStats loads aggregate archive statistics from the archive_state table.
func LoadArchiveStats(ctx context.Context, db *sql.DB) (*ArchiveStats, error) {
	var a ArchiveStats
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COALESCE(SUM(row_count), 0),
		       COALESCE(SUM(file_size_bytes), 0),
		       COALESCE(SUM(CASE WHEN local_path IS NOT NULL THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN s3_key IS NOT NULL THEN 1 ELSE 0 END), 0)
		FROM archive_state`).Scan(
		&a.TotalFiles, &a.TotalRows, &a.TotalSizeBytes,
		&a.LocalFiles, &a.S3Files,
	)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT s3_bucket FROM archive_state WHERE s3_bucket IS NOT NULL AND s3_bucket != ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, err
		}
		a.S3Buckets = append(a.S3Buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &a, nil
}

// UncoveredDDLWhere is the ONE definition of "a DDL with no covering
// snapshot", shared by this package's UncoveredDDLs count and the
// list_schema_changes tool's uncovered_only filter (#1436) — the two were
// hand-maintained WHERE clauses and diverged once: the tool used a bare
// `snapshot_id IS NULL` and listed 9 rows where the status warning counted 1.
//
// TRUNCATE TABLE is excluded: it does not change table structure, so every
// capture path records it with snapshot_id = NULL on purpose ("DDL detected
// (no snapshot needed)") — a NULL there is not a coverage gap, and counting
// it would permanently inflate the warning.
//
// Parenthesized so both consumers compose it safely: status appends it to
// WHERE, the tool to an existing clause with AND — an OR added here later
// would otherwise bind correctly in one site and silently wrong in the other.
const UncoveredDDLWhere = `(snapshot_id IS NULL AND ddl_type <> 'TRUNCATE TABLE')`

// LoadCoverage loads restore coverage info from binlog_events, schema_changes,
// and archive_state. Archive coverage is derived from partition names stored in
// archive_state (e.g. "p_2026021914" → 2026-02-19 14:00 UTC) without reading
// Parquet files.
func LoadCoverage(ctx context.Context, db *sql.DB) (*CoverageInfo, error) {
	var c CoverageInfo
	err := db.QueryRowContext(ctx, `
		SELECT MIN(event_timestamp),
		       MAX(event_timestamp),
		       COUNT(*)
		FROM binlog_events`).Scan(&c.EarliestEvent, &c.LatestEvent, &c.TotalEvents)
	if err != nil {
		return nil, fmt.Errorf("query binlog_events coverage: %w", err)
	}

	// Rows, one per table a statement changed (a DROP or RENAME of several
	// tables has one each): the same unit list_schema_changes returns.
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_changes`).Scan(&c.SchemaChanges)
	if err != nil {
		return nil, fmt.Errorf("query schema_changes count: %w", err)
	}

	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schema_changes WHERE `+UncoveredDDLWhere).Scan(&c.UncoveredDDLs)
	if err != nil {
		return nil, fmt.Errorf("query uncovered DDLs: %w", err)
	}

	// Extend coverage with archived partition data.
	var minPartition sql.NullString
	err = db.QueryRowContext(ctx, `
		SELECT MIN(partition_name), COALESCE(SUM(row_count), 0)
		FROM archive_state`).Scan(&minPartition, &c.ArchiveTotalRows)
	if err != nil {
		// Non-fatal either way — coverage is a report, not a gate — but the
		// two causes are NOT the same fact (#816). A missing table is an
		// index with no archive tier, which the zeroed fields describe
		// correctly. Anything else means the archives may exist and we could
		// not see them, so the report must say the window it prints is a
		// LOWER BOUND rather than quietly printing the live-only one.
		if isMissingTableErr(err) {
			slog.Debug("archive_state not present; reporting live-only coverage", "error", err)
			return &c, nil
		}
		slog.Warn("could not load archive coverage; the restore window will be reported as a lower bound", "error", err)
		c.ArchiveUnavailable = true
		c.ArchiveError = err.Error()
		return &c, nil
	}
	if minPartition.Valid {
		t, ok := parsePartitionName(minPartition.String)
		if !ok {
			// Same class as an unreadable table, reached three lines later:
			// the archives exist, we read the row, and we still cannot place
			// them in time — so the floor below is live-only and the window
			// is a lower bound. Dropping this silently printed a too-recent
			// "Earliest event" as fact, which is the whole of #816.
			//
			// staleness.go's OldestDeltaFromDB hard-errors on the identical
			// parse of the identical value ("our own naming scheme failing to
			// parse is drift"). Two readers of one value must not take
			// opposite stances, and the silent one was the one an operator
			// reads mid-incident.
			c.ArchiveUnavailable = true
			c.ArchiveError = fmt.Sprintf("archive_state MIN(partition_name) %q is not a partition name", minPartition.String)
			return &c, nil
		}
		c.ArchiveEarliestHour = sql.NullTime{Time: t, Valid: true}
	}

	return &c, nil
}

// parsePartitionName converts a partition name like "p_2026021914" to the
// corresponding UTC hour. Returns false for "p_future" or malformed names.
func parsePartitionName(name string) (time.Time, bool) {
	if len(name) != 12 || !strings.HasPrefix(name, "p_") {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("p_2006010215", name, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// LoadSchemaChanges loads all schema changes ordered by detection time.
func LoadSchemaChanges(ctx context.Context, db *sql.DB) ([]SchemaChange, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, detected_at, binlog_file, schema_name, table_name, ddl_type, snapshot_id
		FROM schema_changes
		ORDER BY detected_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var changes []SchemaChange
	for rows.Next() {
		var sc SchemaChange
		if err := rows.Scan(&sc.ID, &sc.DetectedAt, &sc.BinlogFile,
			&sc.SchemaName, &sc.TableName, &sc.DDLType, &sc.SnapshotID); err != nil {
			return nil, err
		}
		changes = append(changes, sc)
	}
	return changes, rows.Err()
}

// LoadServers loads all rows from bintrail_servers ordered by created_at.
func LoadServers(ctx context.Context, db *sql.DB) ([]ServerInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT bintrail_id, server_uuid, host, port, username,
		       created_at, decommissioned_at
		FROM bintrail_servers
		ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var servers []ServerInfo
	for rows.Next() {
		var s ServerInfo
		if err := rows.Scan(
			&s.BintrailID, &s.ServerUUID, &s.Host, &s.Port, &s.Username,
			&s.CreatedAt, &s.DecommissionedAt,
		); err != nil {
			return nil, err
		}
		servers = append(servers, s)
	}
	return servers, rows.Err()
}

// LoadStreamState loads the single row from stream_state (if any).
// Returns nil with no error when the table is empty (no active stream).
func LoadStreamState(ctx context.Context, db *sql.DB) (*StreamStateInfo, error) {
	s, err := loadStreamStateCore(ctx, db)
	if err != nil || s == nil {
		return s, err
	}
	// source_health (#599) is loaded by a SEPARATE best-effort query, not folded into the
	// SELECTs above: it is newer than gap_lost_*, so adding it there would drag any index
	// that has gap_lost but not yet source_health down to the base fallback (losing the
	// loss record). The separate query degrades to "no health" on the unknown-column error
	// (a legacy/un-migrated index) or an empty row; any OTHER error surfaces, since the core
	// load already proved the DB reachable; any hard error here is logged and ignored,
	// keeping the already-loaded stream state (including gap_lost) visible rather than
	// discarding it — that would re-hide the very GAP LOST banner #815 exists to surface.
	if err := loadSourceHealth(ctx, db, s); err != nil {
		slog.Warn("could not load source_health; keeping stream state without it", "error", err)
	}
	// capture_skips (#1034) follows the same separate best-effort pattern (and
	// the same rationale) as source_health above: newer than both, so folding
	// it into either SELECT would drag older indexes down a fallback tier.
	if err := loadCaptureSkips(ctx, db, s); err != nil {
		slog.Warn("could not load capture_skips; keeping stream state without it", "error", err)
	}
	// The acknowledgement (#1314) is best-effort too, but note the direction it
	// fails in: an unreadable ack leaves the verdict UNACKNOWLEDGED, so the
	// alarm stays up. Losing this column can only ever over-report.
	if err := loadCaptureSkipsAck(ctx, db, s); err != nil {
		slog.Warn("could not load capture_skips_ack; capture skips will read as unacknowledged", "error", err)
	}
	// The snapshot anchor (#1312) is best-effort for the same reason: it only
	// SHARPENS the capture verdict, so failing to read it must never cost the
	// caller the stream state it already has.
	if err := loadSchemaSnapshotTime(ctx, db, s); err != nil {
		slog.Warn("could not load the newest schema snapshot time; capture skips will not be dated against it", "error", err)
	}
	return s, nil
}

// loadSchemaSnapshotTime augments an already-loaded StreamStateInfo with the
// newest schema_snapshots.snapshot_time (#1312). MAX over the whole table, not
// the newest snapshot_id: the id is an auto-increment and the time is what the
// comparison is about.
//
// An index with no snapshot yields a NULL row, not zero rows, so the scan
// target is nullable and an empty result leaves the field invalid — the
// verdict then stays "cannot tell". A missing TABLE (an index predating
// snapshots entirely) is tolerated for the same reason the sibling loaders
// tolerate a missing column.
func loadSchemaSnapshotTime(ctx context.Context, db *sql.DB, s *StreamStateInfo) error {
	err := db.QueryRowContext(ctx, `SELECT MAX(snapshot_time) FROM schema_snapshots`).Scan(&s.SchemaSnapshotAt)
	if isMissingTableErr(err) || errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

func loadStreamStateCore(ctx context.Context, db *sql.DB) (*StreamStateInfo, error) {
	var s StreamStateInfo
	err := db.QueryRowContext(ctx, `
		SELECT mode, binlog_file, binlog_position, gtid_set,
		       events_indexed, last_event_time, last_checkpoint,
		       server_id, bintrail_id, gap_lost_at, gap_lost_detail
		FROM stream_state
		WHERE id = 1`).Scan(
		&s.Mode, &s.BinlogFile, &s.BinlogPosition, &s.GTIDSet,
		&s.EventsIndexed, &s.LastEventTime, &s.LastCheckpoint,
		&s.ServerID, &s.BintrailID, &s.GapLostAt, &s.GapLostDetail,
	)
	// A legacy index predating the gap_lost_* columns (added by the cascade-recovery
	// work), read before any migrating command (EnsureSchema) ran — the console never
	// migrates registry DSNs — lacks those columns. Degrade gracefully to the base
	// columns rather than erroring `status`; such an index simply has no loss record.
	if isUnknownColumnErr(err) {
		return loadStreamStateBase(ctx, db)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	s.GapColumnsPresent = true // the gap_lost_* columns were read — gap state is evaluable
	return &s, nil
}

// loadSourceHealth augments an already-loaded StreamStateInfo with the source_health
// column. It tolerates ONLY the unknown-column error (a legacy index missing the column)
// and an empty row — there it leaves SourceHealth invalid (no health to show). Any other
// error is returned, so a genuine fault is not silently hidden behind a blank panel.
func loadSourceHealth(ctx context.Context, db *sql.DB, s *StreamStateInfo) error {
	err := db.QueryRowContext(ctx, `SELECT source_health FROM stream_state WHERE id = 1`).Scan(&s.SourceHealth)
	if isUnknownColumnErr(err) || errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

// loadCaptureSkips augments an already-loaded StreamStateInfo with the
// capture_skips column (#1034) — same tolerance contract as loadSourceHealth:
// only the unknown-column error (legacy index) and an empty row leave
// CaptureSkips invalid (verdict unknown); any other error is returned.
func loadCaptureSkips(ctx context.Context, db *sql.DB, s *StreamStateInfo) error {
	err := db.QueryRowContext(ctx, `SELECT capture_skips FROM stream_state WHERE id = 1`).Scan(&s.CaptureSkips)
	if isUnknownColumnErr(err) || errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

// loadCaptureSkipsAck augments an already-loaded StreamStateInfo with the
// capture_skips_ack column (#1314) — same tolerance contract as
// loadCaptureSkips above.
func loadCaptureSkipsAck(ctx context.Context, db *sql.DB, s *StreamStateInfo) error {
	err := db.QueryRowContext(ctx, `SELECT capture_skips_ack FROM stream_state WHERE id = 1`).Scan(&s.CaptureSkipsAck)
	if isUnknownColumnErr(err) || errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

// loadStreamStateBase loads the stream_state columns guaranteed by the original CREATE
// TABLE (no gap_lost_*), for a legacy index that has not been migrated. The gap-loss
// fields stay zero (no badge).
func loadStreamStateBase(ctx context.Context, db *sql.DB) (*StreamStateInfo, error) {
	var s StreamStateInfo
	err := db.QueryRowContext(ctx, `
		SELECT mode, binlog_file, binlog_position, gtid_set,
		       events_indexed, last_event_time, last_checkpoint,
		       server_id, bintrail_id
		FROM stream_state
		WHERE id = 1`).Scan(
		&s.Mode, &s.BinlogFile, &s.BinlogPosition, &s.GTIDSet,
		&s.EventsIndexed, &s.LastEventTime, &s.LastCheckpoint,
		&s.ServerID, &s.BintrailID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// isUnknownColumnErr reports whether err is MySQL error 1054 (ER_BAD_FIELD_ERROR,
// "Unknown column"), i.e. the index schema predates a column this build SELECTs.
func isUnknownColumnErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1054
}

// isMissingTableErr is 1054's sibling for a whole table that is not there
// (1146) — the shape an index predating a table shows, as opposed to a
// connection that died. Kept narrow to 1146 on purpose: swallowing any error
// would report "no snapshot exists" for an unreachable database, and the
// caller reads that absence as "cannot tell", not as an alarm.
func isMissingTableErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1146
}

// StatusData holds all data sections loaded by CollectStatus.
type StatusData struct {
	Files     []IndexStateRow
	Parts     []PartitionStat
	Archives  *ArchiveStats
	Coverage  *CoverageInfo
	Servers   []ServerInfo
	Stream    *StreamStateInfo
	Baselines []BaselineInfo
	// Retention is the built-in rotation window this index runs on while its
	// operator sets none, and where it came from (#1709). nil when it could
	// not be read at all. It says nothing about whether a rotation loop is
	// RUNNING — status reads an index, not a daemon — which is why the line
	// it feeds names the window an unattended daemon would use, not a promise
	// that one is.
	Retention *RetentionInfo
	// BaselinesUnavailable: a baseline location was configured but could not
	// be read, so Baselines is empty for a BAD reason — JSON must report
	// baseline_staleness "unknown", not omit it as if nothing were configured.
	BaselinesUnavailable bool
	// BaselinesErr is why, for the text report (#1639): the JSON carries the
	// unknown verdict, and the text report had nothing at all.
	BaselinesErr error
	// StreamErr records a failure to READ stream_state (transient timeout, revoked
	// permission, an unexpected loadSourceHealth error) — as distinct from an empty
	// table (Stream==nil, StreamErr==nil = no active stream). When set, the continuity
	// verdict could not be evaluated: the output must show it as "unavailable" rather
	// than silently omit the Stream section and its permanent-loss banner (fail visible,
	// not silent). json:"-" — StatusData is rendered via the manual jsonSummary mapping,
	// never marshalled directly; the tag is insurance against a future direct marshal.
	StreamErr error `json:"-"`
	// ArchivesErr records a failure to READ archive_state for the Archives
	// section (StreamErr's sibling, #1323) — as distinct from an index with no
	// archive tier at all (Archives==nil, ArchivesErr==nil: ER_NO_SUCH_TABLE,
	// which the absent section describes correctly). When set, the output must
	// render the section as unreadable rather than omit it: a consumer reading
	// "no archives section" concludes "no archives exist" while the Parquet may
	// be sitting in the bucket.
	ArchivesErr error `json:"-"`
	// CoverageErr records a LoadCoverage failure (#1323) — the likeliest
	// at-scale one being the full binlog_events scan hitting max_execution_time
	// or a lost connection. #816 taught LoadCoverage to report an unreadable
	// archive tier INSIDE its result; this is the frame above, where the whole
	// result is lost: without it the report prints no restore window, no error,
	// and exits 0 — a read failure rendering as an affirmative fact about the
	// data. When set, Coverage is nil and the output must carry a tombstone in
	// both renderings.
	CoverageErr error `json:"-"`
	// TableVisible, when set, scopes the capture-health table NAMES WriteJSON
	// emits (#1452): a name whose (schema, table) the predicate refuses is
	// dropped from `skipped[*].tables` and from the explanation prose, and
	// counted in `tables_withheld` instead, so the counts stay whole while a
	// reader with restricted data access learns no name it may not read
	// elsewhere. Nil renders the ledger verbatim. Set by the web console (a
	// session is the thing that has a scope) and by the MCP status tool from
	// its surface's deny rules; `bintrail status` has none. The text report
	// consults it only for the TableCapture section: its capture-health lines
	// still print the ledger's names unscoped.
	TableVisible func(schema, table string) bool `json:"-"`
	// TableCapture is which tables the current schema snapshot captures and
	// which it left out (#1802), or nil when the caller did not load it —
	// CollectStatus does not, because the index-metrics scraper calls it on a
	// timer and has no use for a per-table list. `bintrail status` and the MCP
	// status tool load it with LoadTableCapture; the console serves it from
	// its own endpoint. Both renderings withhold the names TableVisible
	// refuses, the text report included: this section is the one place the
	// text report consults TableVisible.
	TableCapture *TableCapture `json:"-"`
}

// BaselineInfo holds metadata about a discovered baseline Parquet file.
// Populated externally (from baseline.DiscoverBaselines) and attached to StatusData.
type BaselineInfo struct {
	SnapshotTime time.Time
	Database     string
	Table        string
	BinlogFile   string
	BinlogPos    int64
	GTIDSet      string
	// Staleness is this snapshot's #1193 verdict against the oldest available
	// delta coverage ("" until AnnotateBaselineStaleness runs).
	Staleness BaselineStalenessVerdict
	Path      string // filesystem path; ignored by display/JSON output
	// Size is the Parquet file size in bytes (0 = unknown). Surfaced so an
	// operator can see per-table baseline size — the signal that tells whether
	// a single-table baseline has grown into the large regime.
	Size int64
}

// CollectStatus loads all status data from the index database.
// IndexState and PartitionStats are required (errors are returned).
// Servers, StreamState, ArchiveStats, and Coverage are best-effort
// (failures are logged as warnings and the field is left nil).
func CollectStatus(ctx context.Context, db *sql.DB, dbName string) (*StatusData, error) {
	files, err := LoadIndexState(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to load index state: %w", err)
	}

	parts, err := LoadPartitionStats(ctx, db, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to load partition info: %w", err)
	}

	d := &StatusData{Files: files, Parts: parts}

	if servers, err := LoadServers(ctx, db); err != nil {
		slog.Warn("could not load servers", "error", err)
	} else {
		d.Servers = servers
	}

	if stream, err := LoadStreamState(ctx, db); err != nil {
		slog.Warn("could not load stream state", "error", err)
		// Record the read failure so the continuity verdict reports "unavailable"
		// instead of the output silently omitting the Stream section (and its
		// permanent-loss banner) — a swallowed error must not read as "no stream".
		d.StreamErr = err
	} else {
		d.Stream = stream
	}

	if archives, err := LoadArchiveStats(ctx, db); err != nil {
		// Same discrimination LoadCoverage applies to the same table (#816,
		// #1323): a missing archive_state is an index with no archive tier —
		// a fact the absent section describes correctly, and flagging it would
		// put a scary banner on every pre-archive index. Anything else means
		// archives may exist that we could not see, and the report must say so
		// instead of rendering the failure as "no archives".
		if isMissingTableErr(err) {
			slog.Debug("archive_state not present; no archive tier to report", "error", err)
		} else {
			slog.Warn("could not load archive stats; the Archives section will report itself unreadable", "error", err)
			d.ArchivesErr = err
		}
	} else {
		d.Archives = archives
	}

	if coverage, err := LoadCoverage(ctx, db); err != nil {
		slog.Warn("could not load coverage info; the coverage section will report itself unreadable", "error", err)
		// Record the failure so the output renders a coverage tombstone instead
		// of silently omitting the section — a monitor keyed on
		// coverage.archives_unavailable must see coverage_error, not nothing.
		d.CoverageErr = err
	} else {
		d.Coverage = coverage
	}

	// Best-effort: the binlog_events on-disk size, attached to coverage so it
	// surfaces alongside the time-coverage figures (and is reused by the
	// bintrail_index_storage_bytes gauge).
	if size, err := LoadIndexSizeBytes(ctx, db, dbName); err != nil {
		slog.Warn("could not load index size", "error", err)
	} else if d.Coverage != nil {
		d.Coverage.IndexSizeBytes = size
	}

	return d, nil
}

// LoadIndexSizeBytes returns the on-disk size of the binlog_events table
// (DATA_LENGTH + INDEX_LENGTH summed across partitions) — an InnoDB estimate
// from information_schema, the same figure the doctor capacity check uses.
func LoadIndexSizeBytes(ctx context.Context, db *sql.DB, dbName string) (int64, error) {
	var b sql.NullInt64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(DATA_LENGTH + INDEX_LENGTH), 0)
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'`, dbName).Scan(&b)
	if err != nil {
		return 0, err
	}
	return b.Int64, nil
}

// withScopedSkips returns the stream state this rendering should use: with a
// visibility predicate, a COPY whose capture-skip ledger is already scoped,
// so the capture-health prose cannot name a table the same output withholds
// from the uncaptured list beside it (#1802 round 4). Without one, the
// original — `bintrail status` has no scope and renders the ledger verbatim.
func withScopedSkips(stream *StreamStateInfo, visible func(schema, table string) bool) *StreamStateInfo {
	if stream == nil || visible == nil {
		return stream
	}
	skips, ok := stream.ParseCaptureSkips()
	if !ok {
		return stream
	}
	scoped := *stream
	scoped.scopedSkips = ScopeCaptureSkips(skips, visible)
	return &scoped
}

// Write writes the status data as a human-readable report to w.
func (d *StatusData) Write(w io.Writer) {
	WriteStatus(w, d.Files, d.Parts, d.Archives, d.Coverage, d.Servers, withScopedSkips(d.Stream, d.TableVisible), d.Retention)
	writeTableCapture(w, d.TableCapture, d.TableVisible)
	if d.Stream == nil && d.StreamErr != nil {
		writeStreamUnavailable(w, d.StreamErr)
	}
	if d.Archives == nil && d.ArchivesErr != nil {
		writeArchivesUnavailable(w, d.ArchivesErr)
	}
	if d.Coverage == nil && d.CoverageErr != nil {
		writeCoverageUnavailable(w, d.CoverageErr)
	}
	if d.BaselinesUnavailable && len(d.Baselines) == 0 && d.BaselinesErr != nil {
		writeBaselinesUnavailable(w, d.BaselinesErr)
	}
	writeBaselines(w, d.Baselines)
}

// writeBaselinesUnavailable renders a visible Baselines block when the
// baseline directory could not be read in full, so the staleness verdict the
// JSON reports as unknown is not an absence in the text report.
func writeBaselinesUnavailable(w io.Writer, err error) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "=== Baselines ===")
	fmt.Fprintf(w, "Unavailable: %v\n", err)
	fmt.Fprintln(w, "⚠ BASELINE STALENESS NOT EVALUABLE: fix the cause above and run status again.")
}

// writeStreamUnavailable renders a visible Stream block when stream_state could not
// be READ (StreamErr set, Stream nil). Distinct from an empty table (no block): here
// the continuity verdict — and any permanent-loss banner — could not be evaluated, so
// the operator sees "unavailable" rather than an absence they'd misread as "no loss".
func writeStreamUnavailable(w io.Writer, err error) {
	fmt.Fprintln(w, "=== Stream ===")
	fmt.Fprintf(w, "  Continuity:      ⚠ unavailable (could not read stream state: %v)\n", err)
	fmt.Fprintln(w, "  The gap/continuity state could not be read from the index; a permanent-loss")
	fmt.Fprintln(w, "  banner can neither be shown nor ruled out. Re-run status to retry.")
	fmt.Fprintln(w)
}

// writeArchivesUnavailable renders a visible Archives block when archive_state
// could not be READ (ArchivesErr set, Archives nil) — writeStreamUnavailable's
// sibling (#1323). Distinct from a missing table (no block): "no archive tier"
// is a fact the absent section describes correctly, while a read failure
// rendered the same way tells the operator archives do not exist when they may
// simply be unreadable right now.
func writeArchivesUnavailable(w io.Writer, err error) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "=== Archives ===")
	fmt.Fprintf(w, "  ⚠ NOT READ — archive_state could not be queried: %v\n", err)
	fmt.Fprintln(w, "  This is a read failure, not \"no archives\": archived data may exist that")
	fmt.Fprintln(w, "  is not shown here. Re-run status to retry.")
	fmt.Fprintln(w)
}

// writeCoverageUnavailable renders a visible Restore Coverage block when
// LoadCoverage itself failed (CoverageErr set, Coverage nil) — the frame above
// the #816 in-section flag (#1323). Without it the report prints no restore
// window at all and an operator mid-incident reads the silence as "nothing to
// restore from".
func writeCoverageUnavailable(w io.Writer, err error) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "=== Restore Coverage ===")
	fmt.Fprintf(w, "  ⚠ NOT READ — coverage could not be computed: %v\n", err)
	fmt.Fprintln(w, "  The restore window is UNKNOWN, not empty: indexed events and archived")
	fmt.Fprintln(w, "  hours may exist that are not shown here. Re-run status to retry.")
	fmt.Fprintln(w)
}

// WriteJSON writes the status data as JSON to w.
func (d *StatusData) WriteJSON(w io.Writer) error {
	var tables *TableCaptureView
	if d.TableCapture != nil {
		v := d.TableCapture.View(d.TableVisible)
		tables = &v
	}
	return writeStatusJSONFull(w, d.Files, d.Parts, d.Archives, d.Coverage, d.Servers, d.Stream, d.Baselines, d.BaselinesUnavailable, d.StreamErr, d.ArchivesErr, d.CoverageErr, tables, d.TableVisible, d.Retention)
}

// WriteStatus writes a multi-section status report (Servers, Stream, Indexed Files, Partitions, Archives, Coverage, Summary) to w.
func WriteStatus(w io.Writer, files []IndexStateRow, parts []PartitionStat, archives *ArchiveStats, coverage *CoverageInfo, servers []ServerInfo, stream *StreamStateInfo, retentions ...*RetentionInfo) {
	var retention *RetentionInfo
	if len(retentions) > 0 {
		retention = retentions[0]
	}
	// ── Section 0: Servers ───────────────────────────────────────────────────
	if len(servers) > 0 {
		fmt.Fprintln(w, "=== Servers ===")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "BINTRAIL_ID\tHOST\tPORT\tSERVER_UUID\tCREATED_AT\tSTATUS")
		fmt.Fprintln(tw, "───────────\t────\t────\t───────────\t──────────\t──────")
		for _, s := range servers {
			st := "active"
			if s.DecommissionedAt.Valid {
				st = "decommissioned"
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\n",
				s.BintrailID, s.Host, s.Port, s.ServerUUID,
				s.CreatedAt.Format(TSFmt), st,
			)
		}
		tw.Flush()
		fmt.Fprintln(w)
	}

	// ── Section 0b: Stream ──────────────────────────────────────────────────
	if stream != nil {
		fmt.Fprintln(w, "=== Stream ===")
		bintrailID := "(none)"
		if stream.BintrailID.Valid && stream.BintrailID.String != "" {
			bintrailID = stream.BintrailID.String
		}
		fmt.Fprintf(w, "  Bintrail ID:     %s\n", bintrailID)
		fmt.Fprintf(w, "  Mode:            %s\n", stream.Mode)
		if stream.BinlogFile != "" {
			fmt.Fprintf(w, "  Position:        %s:%d\n", stream.BinlogFile, stream.BinlogPosition)
		}
		if stream.GTIDSet.Valid && stream.GTIDSet.String != "" {
			fmt.Fprintf(w, "  GTID set:        %s\n", stream.GTIDSet.String)
		}
		fmt.Fprintf(w, "  Events indexed:  %d\n", stream.EventsIndexed)
		if stream.LastEventTime.Valid {
			fmt.Fprintf(w, "  Last event:      %s\n", stream.LastEventTime.Time.Format(TSFmt))
		}
		fmt.Fprintf(w, "  Last checkpoint: %s\n", stream.LastCheckpoint.Format(TSFmt))
		fmt.Fprintf(w, "  Server ID:       %d\n", stream.ServerID)
		// Always-present continuity verdict — the cheap "did I lose any events?"
		// answer the gap detector already computes, surfaced so an operator reads
		// it at a glance instead of inferring it from the absence of the loud
		// banner below. It is strictly about gap-CONTIGUITY of the captured range
		// (the cursor is the Position / GTID set above, when one is printed); it is
		// deliberately NOT a liveness/lag check — a contiguous stream may still be
		// stopped or behind. "not evaluated" guards a legacy index that never had
		// the gap_lost_* columns, so a clean verdict is never asserted from
		// un-evaluated data.
		switch {
		case stream.GapLostAt.Valid:
			fmt.Fprintf(w, "  Continuity:      ⚠ GAP LOST at %s\n", stream.GapLostAt.Time.Format(TSFmt))
		case !stream.GapColumnsPresent:
			fmt.Fprintln(w, "  Continuity:      not evaluated (legacy index — migrate the schema to enable gap detection)")
		default:
			fmt.Fprintln(w, "  Continuity:      no gaps in the captured range (not a liveness check)")
		}
		// Freshness (#1226) — the liveness half Continuity explicitly is not.
		// Continuity answers "did I lose events inside what I captured?"; this
		// answers "is capture still keeping up?". A contiguous index can be three
		// days stale and a fresh one can have a hole, so neither substitutes for
		// the other.
		writeFreshness(w, stream, nil, time.Now())
		// Capture health (#1034) — the continuity verdict's sibling for
		// IN-STREAM discards: events the daemon read and chose to drop (e.g.
		// the column-count guard rejecting every row against a stale snapshot)
		// while the checkpoint stayed fresh and continuity honestly said "no
		// gaps". Omitted (not asserted OK) when no skip-aware daemon has
		// written the counters — same never-a-false-ok stance as Continuity's
		// "not evaluated".
		if skips, ok := stream.ParseCaptureSkips(); ok {
			ack := stream.ParseCaptureSkipsAck()
			switch total := totalCaptureSkips(skips); {
			case total > 0 && CaptureSkipsAcknowledged(skips, ack):
				// An ACKNOWLEDGED record (#1314) still prints its tally — the
				// events are still missing and a restore window over them is
				// still incomplete — but not the cause/remedy essay. An
				// operator who read that advice and acted on it gets it
				// re-printed on every status run forever otherwise, which is
				// how advice stops being read at all.
				fmt.Fprintf(w, "  Capture health:  ⚠ ON RECORD — %s events skipped (%s), last %s\n",
					commaGroup(total), captureSkipReasons(skips), lastCaptureSkip(skips).Format(TSFmt))
				fmt.Fprintf(w, "  Acknowledged:    %s — that count is retired; a later skip alarms again\n",
					CaptureSkipsAcknowledgedAt(skips, ack).Format(TSFmt))
				fmt.Fprintln(w, "  Those events were read from the stream but NOT indexed — a restore window")
				fmt.Fprintln(w, "  over them is incomplete.")
			case total > 0:
				fmt.Fprintf(w, "  Capture health:  ⚠ DEGRADED — %s events skipped (%s), last %s\n",
					commaGroup(total), captureSkipReasons(skips), lastCaptureSkip(skips).Format(TSFmt))
				if attr := lastCaptureSkipAttribution(skips); attr != "" {
					fmt.Fprintf(w, "  Last drop:       %s\n", attr)
				}
				fmt.Fprintln(w, "  Skipped events were read from the stream but NOT indexed — a restore window")
				fmt.Fprintln(w, "  over them is incomplete.")
				// Cause, remedy and scope come from the shared explanation
				// builder (#1296) so this report and the console cannot tell
				// two different stories about the same ledger.
				writeCaptureSkipExplanation(w, skips, stream.SchemaSnapshotAt.Time)
			default:
				fmt.Fprintln(w, "  Capture health:  OK — no events skipped")
			}
		}
		fmt.Fprintln(w)

		// Loud, unmissable banner when the stream permanently lost data (an unfillable
		// binlog gap, or an invalidated/lost PostgreSQL replication slot — #532). This
		// is the only way index-only `status` can show a lost stream after the capture
		// process has exited; the index up to the gap is still valid for recovery.
		if stream.GapLostAt.Valid {
			fmt.Fprintln(w, "=== ⚠ EVENTS PERMANENTLY LOST ===")
			fmt.Fprintf(w, "  Detected:  %s\n", stream.GapLostAt.Time.Format(TSFmt))
			if stream.GapLostDetail.Valid && stream.GapLostDetail.String != "" {
				fmt.Fprintf(w, "  Detail:    %s\n", stream.GapLostDetail.String)
			}
			fmt.Fprintln(w, "  The capture stream lost data it could not recover. The index up to the")
			fmt.Fprintln(w, "  gap is still valid for recovery, but to resume capture you must re-baseline.")
			fmt.Fprintln(w)
		}
	}

	// ── Section 1: Indexed Files ──────────────────────────────────────────────
	fmt.Fprintln(w, "=== Indexed Files ===")
	if len(files) == 0 {
		fmt.Fprintln(w, "  (no files indexed yet)")
	} else {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "FILE\tSTATUS\tEVENTS\tSTARTED_AT\tCOMPLETED_AT\tBINTRAIL_ID\tERROR")
		fmt.Fprintln(tw, "────\t──────\t──────\t──────────\t────────────\t───────────\t─────")
		for _, f := range files {
			completedAt := "-"
			if f.CompletedAt.Valid {
				completedAt = f.CompletedAt.Time.Format(TSFmt)
			}
			bintrailID := "-"
			if f.BintrailID.Valid && f.BintrailID.String != "" {
				bintrailID = f.BintrailID.String
			}
			errMsg := "-"
			if f.ErrorMessage.Valid && f.ErrorMessage.String != "" {
				errMsg = Truncate(f.ErrorMessage.String, 60)
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
				f.BinlogFile, f.Status, f.EventsIndexed,
				f.StartedAt.Format(TSFmt),
				completedAt, bintrailID, errMsg,
			)
		}
		tw.Flush()
	}

	// ── Section 2: Partitions ─────────────────────────────────────────────────
	fmt.Fprintln(w)
	fmt.Fprintln(w, "=== Partitions ===")
	if len(parts) == 0 {
		fmt.Fprintln(w, "  (no partitions found — run 'bintrail init' first)")
	} else {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "PARTITION\tLESS_THAN\tROWS (est.)")
		fmt.Fprintln(tw, "─────────\t─────────\t───────────")
		var totalRows int64
		for _, p := range parts {
			fmt.Fprintf(tw, "%s\t%s\t%d\n", p.Name, DescriptionToHuman(p.Description), p.TableRows)
			totalRows += p.TableRows
		}
		tw.Flush()
		fmt.Fprintf(w, "Total events (est.): %d\n", totalRows)
	}
	if retention != nil {
		fmt.Fprintf(w, "Rotation window: %s — %s\n", retention.Raw, retention.Source)
		fmt.Fprintln(w, "  (the window an unattended daemon drops on while nobody sets --rotate-retain;")
		fmt.Fprintln(w, "   an explicit --rotate-retain, BINTRAIL_ROTATE_RETAIN, or a setting saved in")
		fmt.Fprintln(w, "   the web interface, overrides it)")
	}

	// ── Section 3: Archives ──────────────────────────────────────────────────
	if archives != nil && archives.TotalFiles > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== Archives ===")
		fmt.Fprintf(w, "  Total:  %d files (%s, %d rows)\n",
			archives.TotalFiles, formatBytes(archives.TotalSizeBytes), archives.TotalRows)
		fmt.Fprintf(w, "  Local:  %d\n", archives.LocalFiles)
		if archives.S3Files > 0 {
			fmt.Fprintf(w, "  S3:     %d (bucket: %s)\n",
				archives.S3Files, strings.Join(archives.S3Buckets, ", "))
		} else {
			fmt.Fprintf(w, "  S3:     0\n")
		}
	}

	// ── Section 4: Restore Coverage ─────────────────────────────────────────
	if coverage != nil {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== Restore Coverage ===")

		// Determine the effective earliest event: archive may extend further back.
		earliest := coverage.EarliestEvent
		hasArchive := coverage.ArchiveEarliestHour.Valid
		if hasArchive && (!earliest.Valid || coverage.ArchiveEarliestHour.Time.Before(earliest.Time)) {
			earliest = coverage.ArchiveEarliestHour
		}
		if earliest.Valid {
			label := earliest.Time.Format(TSFmt)
			switch {
			case coverage.ArchiveUnavailable:
				// Never "(includes archives)" here: we could not read them.
				label += " (LIVE INDEX ONLY — archives not read, see below)"
			case hasArchive:
				label += " (includes archives)"
			}
			fmt.Fprintf(w, "  Earliest event: %s\n", label)
		} else if coverage.ArchiveUnavailable {
			fmt.Fprintln(w, "  Earliest event: (none live; archives not read, see below)")
		} else {
			fmt.Fprintln(w, "  Earliest event: (none)")
		}
		if coverage.LatestEvent.Valid {
			fmt.Fprintf(w, "  Latest event:   %s\n", coverage.LatestEvent.Time.Format(TSFmt))
		} else {
			fmt.Fprintln(w, "  Latest event:   (none)")
		}

		totalEvents := coverage.TotalEvents + coverage.ArchiveTotalRows
		if coverage.ArchiveTotalRows > 0 {
			fmt.Fprintf(w, "  Total events:   %d (%d live + %d archived)\n",
				totalEvents, coverage.TotalEvents, coverage.ArchiveTotalRows)
		} else {
			fmt.Fprintf(w, "  Total events:   %d\n", totalEvents)
		}
		if coverage.IndexSizeBytes > 0 {
			fmt.Fprintf(w, "  Index size:     %s (MySQL binlog_events)\n", formatBytes(coverage.IndexSizeBytes))
		}
		if coverage.ArchiveUnavailable {
			fmt.Fprintln(w, "  Archives:       ⚠ NOT READ — archive_state could not be queried, so any")
			fmt.Fprintln(w, "                  archived hours are missing from the figures above. The")
			fmt.Fprintln(w, "                  restore window is a LOWER BOUND, not the real reach:")
			fmt.Fprintln(w, "                  data older than the earliest event shown may still be")
			fmt.Fprintln(w, "                  recoverable from Parquet.")
			for _, line := range wrapAt("Error: "+coverage.ArchiveError, 60) {
				fmt.Fprintf(w, "                  %s\n", line)
			}
		}
		fmt.Fprintf(w, "  Schema changes: %d\n", coverage.SchemaChanges)
		if coverage.UncoveredDDLs > 0 {
			fmt.Fprintf(w, "  Warning: %d schema change(s) detected without auto-snapshot (file-mode indexing without --source-dsn, or a failed auto-snapshot) — recovery across these changes may require manual snapshot\n",
				coverage.UncoveredDDLs)
		}
	}

	// ── Section 5: Summary (grouped by server) ────────────────────────────────
	if len(files) > 0 {
		// Group files by bintrail_id; preserve insertion order for display.
		type serverStats struct {
			counts map[string]int
			events int64
		}
		serverOrder := []string{}
		byServer := map[string]*serverStats{}
		for _, f := range files {
			key := "(unknown)"
			if f.BintrailID.Valid && f.BintrailID.String != "" {
				key = f.BintrailID.String
			}
			if _, seen := byServer[key]; !seen {
				serverOrder = append(serverOrder, key)
				byServer[key] = &serverStats{counts: map[string]int{}}
			}
			byServer[key].counts[f.Status]++
			byServer[key].events += f.EventsIndexed
		}

		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== Summary ===")
		for _, id := range serverOrder {
			s := byServer[id]
			fmt.Fprintf(w, "Server %s\n", id)
			fmt.Fprintf(w, "  Files:  %d completed, %d in_progress, %d failed\n",
				s.counts["completed"], s.counts["in_progress"], s.counts["failed"])
			fmt.Fprintf(w, "  Events: %d indexed\n", s.events)
		}
	}
}

// DescriptionToHuman converts a PARTITION_DESCRIPTION value to a readable string.
// RANGE partitions using TO_SECONDS() store the evaluated integer second count; MAXVALUE is literal.
// TO_SECONDS('1970-01-01 00:00:00') = 62167219200, so we convert back via: time.Unix(secs-62167219200, 0).
func DescriptionToHuman(desc string) string {
	if desc == "" || strings.EqualFold(desc, "MAXVALUE") {
		return "MAXVALUE"
	}
	secs, err := strconv.ParseInt(desc, 10, 64)
	if err != nil {
		return desc // not an integer — return raw value
	}
	return time.Unix(secs-62167219200, 0).UTC().Format("2006-01-02 15:00 UTC")
}

// formatBytes converts a byte count to a human-readable string (e.g. "1.2 GB").
func formatBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
	)
	switch {
	case b >= tb:
		return fmt.Sprintf("%.1f TB", float64(b)/float64(tb))
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// totalCaptureSkips sums the per-reason capture-skip counts.
func totalCaptureSkips(skips map[string]CaptureSkipStat) int64 {
	var n int64
	for _, st := range skips {
		n += st.Count
	}
	return n
}

// lastCaptureSkip returns the most recent per-reason last_at.
func lastCaptureSkip(skips map[string]CaptureSkipStat) time.Time {
	var last time.Time
	for _, st := range skips {
		if st.LastAt.After(last) {
			last = st.LastAt
		}
	}
	return last
}

// lastCaptureSkipAttribution formats the newest attributed skip (#999) for the
// DEGRADED block: "file:pos" plus, when a statement keyword was stamped, a
// " (STATEMENT_TYPE, connection id N)" segment. An empty file (a drop before
// the first rotate event) renders as "?" rather than a malformed ":pos". ""
// when no stat carries attribution (pre-#999 ledger, or only unattributed
// reasons).
func lastCaptureSkipAttribution(skips map[string]CaptureSkipStat) string {
	var best CaptureSkipStat
	found := false
	for _, st := range skips {
		if st.LastFile == "" && st.LastStatementType == "" {
			continue
		}
		if !found || st.LastAt.After(best.LastAt) {
			best, found = st, true
		}
	}
	if !found {
		return ""
	}
	file := best.LastFile
	if file == "" {
		file = "?"
	}
	s := fmt.Sprintf("%s:%d", file, best.LastPos)
	if best.LastStatementType != "" {
		s += fmt.Sprintf(" (%s, connection id %d)", best.LastStatementType, best.LastConnectionID)
	}
	return s
}

// captureSkipReasons renders the non-zero reasons for the DEGRADED line: the
// bare reason when there is one ("column_count_mismatch", the #1034 wording),
// else "reason: count" pairs sorted by count descending (ties alphabetical).
func captureSkipReasons(skips map[string]CaptureSkipStat) string {
	var reasons []string
	for r, st := range skips {
		if st.Count > 0 {
			reasons = append(reasons, r)
		}
	}
	if len(reasons) == 1 {
		return reasons[0]
	}
	sort.Slice(reasons, func(i, j int) bool {
		if skips[reasons[i]].Count != skips[reasons[j]].Count {
			return skips[reasons[i]].Count > skips[reasons[j]].Count
		}
		return reasons[i] < reasons[j]
	})
	parts := make([]string, len(reasons))
	for i, r := range reasons {
		parts[i] = fmt.Sprintf("%s: %s", r, commaGroup(skips[r].Count))
	}
	return strings.Join(parts, ", ")
}

// commaGroup formats n with thousands separators ("41203" → "41,203").
func commaGroup(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	if neg {
		s = "-" + s
	}
	return s
}

// Truncate shortens s to at most n bytes, appending "…" if truncated.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// WriteStatusJSON writes the status data as a JSON object to w.
func WriteStatusJSON(w io.Writer, files []IndexStateRow, parts []PartitionStat, archives *ArchiveStats, coverage *CoverageInfo, servers []ServerInfo, stream *StreamStateInfo) error {
	return writeStatusJSONFull(w, files, parts, archives, coverage, servers, stream, nil, false, nil, nil, nil, nil, nil)
}

// tableVisible scopes the capture-health table names (#1452); nil renders the
// ledger verbatim. See StatusData.TableVisible.
func writeStatusJSONFull(w io.Writer, files []IndexStateRow, parts []PartitionStat, archives *ArchiveStats, coverage *CoverageInfo, servers []ServerInfo, stream *StreamStateInfo, baselines []BaselineInfo, baselinesUnavailable bool, streamErr, archivesErr, coverageErr error, tables *TableCaptureView, tableVisible func(schema, table string) bool, retentions ...*RetentionInfo) error {
	var retention *RetentionInfo
	if len(retentions) > 0 {
		retention = retentions[0]
	}
	type jsonFile struct {
		BinlogFile    string  `json:"binlog_file"`
		Status        string  `json:"status"`
		EventsIndexed int64   `json:"events_indexed"`
		FileSize      int64   `json:"file_size"`
		LastPosition  int64   `json:"last_position"`
		StartedAt     string  `json:"started_at"`
		CompletedAt   *string `json:"completed_at"`
		BintrailID    *string `json:"bintrail_id"`
		ErrorMessage  *string `json:"error_message"`
	}
	type jsonPartition struct {
		Name      string `json:"name"`
		LessThan  string `json:"less_than"`
		TableRows int64  `json:"table_rows"`
	}
	type jsonArchives struct {
		TotalFiles     int      `json:"total_files"`
		TotalRows      int64    `json:"total_rows"`
		TotalSizeBytes int64    `json:"total_size_bytes"`
		TotalSizeHuman string   `json:"total_size_human"`
		LocalFiles     int      `json:"local_files"`
		S3Files        int      `json:"s3_files"`
		S3Buckets      []string `json:"s3_buckets"`
	}
	type jsonCoverage struct {
		EarliestEvent        *string `json:"earliest_event"`
		LatestEvent          *string `json:"latest_event"`
		TotalEvents          int64   `json:"total_events"`
		LiveEvents           int64   `json:"live_events"`
		ArchivedEvents       int64   `json:"archived_events"`
		ArchiveEarliestEvent *string `json:"archive_earliest_event,omitempty"`
		IndexSizeBytes       int64   `json:"index_size_bytes,omitempty"`
		IndexSizeHuman       string  `json:"index_size_human,omitempty"`
		SchemaChanges        int     `json:"schema_changes"`
		UncoveredDDLs        int     `json:"uncovered_ddls"`
		// ArchivesUnavailable says the archive tier could NOT be read (#816),
		// so every figure above is live-index-only and earliest_event is a
		// LOWER BOUND on the restore reach rather than the reach itself.
		// Absent (omitted) means the figures are complete: an index with no
		// archives at all reports zeros without this flag, because "no
		// archive tier" is a fact and "could not look" is not.
		ArchivesUnavailable bool   `json:"archives_unavailable,omitempty"`
		ArchivesError       string `json:"archives_error,omitempty"`
	}
	type jsonServer struct {
		BintrailID       string  `json:"bintrail_id"`
		ServerUUID       string  `json:"server_uuid"`
		Host             string  `json:"host"`
		Port             uint16  `json:"port"`
		Username         string  `json:"username"`
		CreatedAt        string  `json:"created_at"`
		DecommissionedAt *string `json:"decommissioned_at"`
	}
	type jsonGapLost struct {
		At     string `json:"at"`
		Detail string `json:"detail,omitempty"`
	}
	// jsonContinuity is the always-present, machine-readable continuity verdict —
	// the affirmative counterpart to gap_lost. status is one of:
	//   "ok"       — no gap in the captured range (NOT a liveness/lag assertion;
	//                a contiguous stream may still be stopped or behind)
	//   "gap_lost" — an unfillable gap was stamped (see the gap_lost object)
	//   "unknown"  — a legacy index without the gap_lost_* columns; the gap state
	//                was never evaluable, so "ok" is not asserted from absent data
	// The console green badge keys on "ok"; gap_lost stays for the loud red detail.
	type jsonContinuity struct {
		Status string `json:"status"`
	}
	// jsonFreshness is the always-present LIVENESS verdict (#1226) — continuity's
	// sibling, answering "is capture keeping up?" where continuity answers "did I
	// lose anything inside what I captured?". status is one of current / idle /
	// stalled / unknown / unavailable / none; see status.FreshnessStatus for what
	// each asserts and, importantly, what "idle" does NOT assert. The two age
	// fields are omitted rather than zeroed when unknowable, so a consumer can
	// never read an absent checkpoint as "0 seconds ago".
	type jsonFreshness struct {
		Status            string `json:"status"`
		CheckpointAgeSecs *int64 `json:"checkpoint_age_seconds,omitempty"`
		NewestEventSecs   *int64 `json:"newest_event_age_seconds,omitempty"`
	}
	// jsonCaptureHealth is the machine-readable Capture health verdict (#1034)
	// — the continuity verdict's sibling for in-stream discards. Present only
	// when a skip-aware daemon has persisted the counters ("unknown" is an
	// omitted key, never a false "ok"). status is "ok" or "degraded"; skipped
	// carries the per-reason monotonic tallies.
	type jsonSkipStat struct {
		Count  int64  `json:"count"`
		LastAt string `json:"last_at"`
		// Last-seen attribution (#999); present only for reasons the capture
		// daemon stamps (see parser.SkipStat for which do).
		LastFile          string `json:"last_file,omitempty"`
		LastPos           uint64 `json:"last_pos,omitempty"`
		LastStatementType string `json:"last_statement_type,omitempty"`
		LastConnectionID  uint32 `json:"last_connection_id,omitempty"`
		// Which tables stopped being captured (#1296), and whether the capped
		// list is the complete set.
		Tables          []string `json:"tables,omitempty"`
		TablesTruncated bool     `json:"tables_truncated,omitempty"`
		LastDetail      string   `json:"last_detail,omitempty"`
		// TablesWithheld counts names left out of Tables because the reader's
		// data scope may not see them (#1452, console sessions only). The
		// count above is still the whole tally: a count is not a name.
		TablesWithheld int `json:"tables_withheld,omitempty"`
	}
	type jsonCaptureHealth struct {
		Status       string                  `json:"status"`
		TotalSkipped int64                   `json:"total_skipped"`
		LastSkipAt   string                  `json:"last_skip_at,omitempty"`
		Skipped      map[string]jsonSkipStat `json:"skipped,omitempty"`
		// Explanation is the rendered cause/remedy/scope prose (#1296), built by
		// the same ExplainCaptureSkips the text report uses. It ships over the
		// wire rather than being re-authored in the console's JavaScript,
		// because two hand-written copies of this advice drifted once already —
		// and the half that drifts is the half telling an operator what a
		// remedy does NOT recover.
		Explanation []string `json:"explanation,omitempty"`
		// SnapshotAt / SkipsPredateSnapshot are the anchor that makes a
		// monotonic tally answerable (#1312): when every skip predates the
		// schema snapshot capture decodes against today, the console renders
		// the box quietly instead of as a live alarm. Omitted together when
		// the index holds no snapshot — no anchor, no claim.
		//
		// Status stays "degraded" in both cases on purpose. --fail-on-gap keys
		// on it, and turning a permanent-loss record into an "ok" would be a
		// change to exit semantics, not a rendering change.
		SnapshotAt           string `json:"snapshot_at,omitempty"`
		SkipsPredateSnapshot bool   `json:"skips_predate_snapshot,omitempty"`
		// Acknowledged / AcknowledgedAt (#1314): an operator has seen this
		// exact count and retired it. Status stays "degraded" here too, for
		// the reason above and one more — the events really are still missing,
		// and a consumer keying on the verdict must not read a human's "seen
		// it" as the loss being undone. What acknowledgement changes is
		// LOUDNESS: the console collapses its alarm and --fail-on-gap stops
		// exiting non-zero, both of which read these fields, not Status.
		//
		// The count is what was acknowledged, so a later skip lifts the tally
		// above it and both surfaces go loud again with no further action.
		Acknowledged   bool   `json:"acknowledged,omitempty"`
		AcknowledgedAt string `json:"acknowledged_at,omitempty"`
	}
	type jsonStream struct {
		BintrailID     *string        `json:"bintrail_id"`
		Mode           string         `json:"mode"`
		BinlogFile     string         `json:"binlog_file,omitempty"`
		BinlogPosition uint64         `json:"binlog_position,omitempty"`
		GTIDSet        *string        `json:"gtid_set,omitempty"`
		EventsIndexed  int64          `json:"events_indexed"`
		LastEventTime  *string        `json:"last_event_time"`
		LastCheckpoint string         `json:"last_checkpoint"`
		ServerID       uint32         `json:"server_id"`
		Continuity     jsonContinuity `json:"continuity"`
		Freshness      jsonFreshness  `json:"freshness"`
		GapLost        *jsonGapLost   `json:"gap_lost,omitempty"`
		// SourceHealth is the raw source_health JSON passed through verbatim (#599):
		// the console knows its shape (slot wal_status/lag, replica_identity_not_full,
		// checked_at), this layer does not. Omitted when no daemon has polled.
		SourceHealth  json.RawMessage    `json:"source_health,omitempty"`
		CaptureHealth *jsonCaptureHealth `json:"capture_health,omitempty"`
	}
	type jsonBaseline struct {
		SnapshotTime string  `json:"snapshot_time"`
		Database     string  `json:"database"`
		Table        string  `json:"table"`
		BinlogFile   *string `json:"binlog_file,omitempty"`
		BinlogPos    *int64  `json:"binlog_position,omitempty"`
		GTIDSet      *string `json:"gtid_set,omitempty"`
		Size         int64   `json:"size_bytes,omitempty"`
		SizeHuman    string  `json:"size_human,omitempty"`
		Staleness    string  `json:"staleness,omitempty"`
	}
	// jsonStreamError is emitted (under a distinct key, never a fake `stream` object —
	// jsonStream's non-omitempty events_indexed:0/mode:"" would read as a real empty
	// stream) when stream_state could not be READ. continuity.status is "unavailable":
	// a consumer switching on continuity keeps the gap state OUT of "ok", and one that
	// only checks stream presence still finds this loud sibling instead of silence.
	type jsonStreamError struct {
		Continuity jsonContinuity `json:"continuity"`
		Freshness  jsonFreshness  `json:"freshness"`
		Error      string         `json:"error"`
	}
	// jsonSectionError marks a section whose data could not be READ (#1323) —
	// stream_error's shape for sections with no verdict sub-structure. Emitted
	// under a distinct key, never as a fake archives/coverage object whose
	// non-omitempty zero fields would read as real empty data. Absence of the
	// data key alone is NOT the signal: absent-and-no-error means the fact
	// "there is nothing here", absent-with-this means "could not look".
	type jsonSectionError struct {
		Error string `json:"error"`
	}
	type jsonSummary struct {
		Servers     []jsonServer     `json:"servers,omitempty"`
		Stream      *jsonStream      `json:"stream,omitempty"`
		StreamError *jsonStreamError `json:"stream_error,omitempty"`
		Files       []jsonFile       `json:"files"`
		Parts       []jsonPartition  `json:"partitions"`
		Total       int64            `json:"total_events_estimate"`
		Archives    *jsonArchives    `json:"archives,omitempty"`
		// ArchivesError / CoverageError (#1323): the read failed, so the
		// corresponding data key is absent for a BAD reason. Note the scope
		// difference from coverage.archives_error (#816): that one says
		// coverage was computed but its archive extension was not; these say
		// the whole section is missing because its read failed.
		ArchivesError *jsonSectionError `json:"archives_error,omitempty"`
		Coverage      *jsonCoverage     `json:"coverage,omitempty"`
		CoverageError *jsonSectionError `json:"coverage_error,omitempty"`
		// Baselines names a table per entry. CollectStatus never populates it
		// on the console path (the console lists baselines through
		// /api/baselines, which refuses a restricted session outright), so
		// tableVisible is not applied here today; anything that starts
		// filling it for a scoped reader must pass it through the predicate.
		Baselines []jsonBaseline `json:"baselines,omitempty"`
		// BaselineStaleness is the worst per-table-newest verdict — the same
		// headline the text banner keys on (#1193).
		BaselineStaleness string `json:"baseline_staleness,omitempty"`
		// Retention is the built-in rotation window this index runs on while
		// nobody sets one, with basis recorded|legacy|unreadable (#1709).
		// Absent when it could not be read.
		Retention *RetentionInfo `json:"retention,omitempty"`
		// TableCapture: which tables the current schema snapshot captures and
		// leaves out (#1802), already scoped to the reader. Absent when the
		// caller did not load it.
		TableCapture *TableCaptureView `json:"table_capture,omitempty"`
	}

	jf := make([]jsonFile, len(files))
	for i, f := range files {
		jf[i] = jsonFile{
			BinlogFile:    f.BinlogFile,
			Status:        f.Status,
			EventsIndexed: f.EventsIndexed,
			FileSize:      f.FileSize,
			LastPosition:  f.LastPosition,
			StartedAt:     f.StartedAt.Format(TSFmt),
		}
		if f.CompletedAt.Valid {
			s := f.CompletedAt.Time.Format(TSFmt)
			jf[i].CompletedAt = &s
		}
		if f.BintrailID.Valid && f.BintrailID.String != "" {
			jf[i].BintrailID = &f.BintrailID.String
		}
		if f.ErrorMessage.Valid && f.ErrorMessage.String != "" {
			jf[i].ErrorMessage = &f.ErrorMessage.String
		}
	}

	jp := make([]jsonPartition, len(parts))
	var total int64
	for i, p := range parts {
		jp[i] = jsonPartition{
			Name:      p.Name,
			LessThan:  DescriptionToHuman(p.Description),
			TableRows: p.TableRows,
		}
		total += p.TableRows
	}

	var js []jsonServer
	for _, s := range servers {
		srv := jsonServer{
			BintrailID: s.BintrailID,
			ServerUUID: s.ServerUUID,
			Host:       s.Host,
			Port:       s.Port,
			Username:   s.Username,
			CreatedAt:  s.CreatedAt.Format(TSFmt),
		}
		if s.DecommissionedAt.Valid {
			ts := s.DecommissionedAt.Time.Format(TSFmt)
			srv.DecommissionedAt = &ts
		}
		js = append(js, srv)
	}

	out := jsonSummary{Servers: js, Files: jf, Parts: jp, Total: total, Retention: retention, TableCapture: tables}
	if stream != nil {
		jstr := &jsonStream{
			Mode:           stream.Mode,
			BinlogFile:     stream.BinlogFile,
			BinlogPosition: stream.BinlogPosition,
			EventsIndexed:  stream.EventsIndexed,
			LastCheckpoint: stream.LastCheckpoint.Format(TSFmt),
			ServerID:       stream.ServerID,
		}
		if stream.BintrailID.Valid && stream.BintrailID.String != "" {
			jstr.BintrailID = &stream.BintrailID.String
		}
		if stream.GTIDSet.Valid && stream.GTIDSet.String != "" {
			jstr.GTIDSet = &stream.GTIDSet.String
		}
		if stream.LastEventTime.Valid {
			s := stream.LastEventTime.Time.Format(TSFmt)
			jstr.LastEventTime = &s
		}
		jstr.Continuity.Status = ContinuityStatus(stream, nil)
		jstr.Freshness = jsonFreshness{Status: FreshnessStatus(stream, nil, time.Now(), 0, 0)}
		if age, ok := CheckpointAge(stream, time.Now()); ok {
			secs := int64(age.Seconds())
			jstr.Freshness.CheckpointAgeSecs = &secs
		}
		if age, ok := NewestEventAge(stream, time.Now()); ok {
			secs := int64(age.Seconds())
			jstr.Freshness.NewestEventSecs = &secs
		}
		if stream.GapLostAt.Valid {
			jstr.GapLost = &jsonGapLost{At: stream.GapLostAt.Time.Format(TSFmt), Detail: stream.GapLostDetail.String}
		}
		if stream.SourceHealth.Valid && stream.SourceHealth.String != "" {
			jstr.SourceHealth = json.RawMessage(stream.SourceHealth.String)
		}
		if skips, ok := stream.ParseCaptureSkips(); ok {
			// Scope the names BEFORE anything renders them, so the per-reason
			// list and the explanation prose are built from the same filtered
			// ledger and cannot disagree about what the reader may see.
			skips = ScopeCaptureSkips(skips, tableVisible)
			ch := &jsonCaptureHealth{Status: "ok"}
			if total := totalCaptureSkips(skips); total > 0 {
				ch.Status = "degraded"
				ch.TotalSkipped = total
				ch.LastSkipAt = lastCaptureSkip(skips).Format(TSFmt)
				ch.Skipped = make(map[string]jsonSkipStat, len(skips))
				for r, st := range skips {
					if st.Count > 0 {
						ch.Skipped[r] = jsonSkipStat{
							Count:             st.Count,
							LastAt:            st.LastAt.Format(TSFmt),
							LastFile:          st.LastFile,
							LastPos:           st.LastPos,
							LastStatementType: st.LastStatementType,
							LastConnectionID:  st.LastConnectionID,
							Tables:            st.Tables,
							TablesTruncated:   st.TablesTruncated,
							LastDetail:        st.LastDetail,
							TablesWithheld:    st.TablesWithheld,
						}
					}
				}
				if stream.SchemaSnapshotAt.Valid {
					ch.SnapshotAt = stream.SchemaSnapshotAt.Time.Format(TSFmt)
					ch.SkipsPredateSnapshot = SkipsPredateSnapshot(skips, stream.SchemaSnapshotAt.Time)
				}
				ch.Explanation = ExplainCaptureSkips(skips, stream.SchemaSnapshotAt.Time)
				// Explanation ships even when acknowledged: the console keeps
				// it behind a disclosure so an operator who wants the cause
				// back does not have to leave the page for it.
				if ack := stream.ParseCaptureSkipsAck(); CaptureSkipsAcknowledged(skips, ack) {
					ch.Acknowledged = true
					ch.AcknowledgedAt = CaptureSkipsAcknowledgedAt(skips, ack).Format(TSFmt)
				}
			}
			jstr.CaptureHealth = ch
		}
		out.Stream = jstr
	} else if streamErr != nil {
		// stream_state could not be READ (nil stream + error) — surface it as a distinct
		// "unavailable" verdict so the omitted Stream section is not misread as "no loss".
		out.StreamError = &jsonStreamError{
			Continuity: jsonContinuity{Status: ContinuityStatus(nil, streamErr)},
			Freshness:  jsonFreshness{Status: FreshnessStatus(nil, streamErr, time.Now(), 0, 0)},
			Error:      streamErr.Error(),
		}
	}
	if archives != nil && archives.TotalFiles > 0 {
		out.Archives = &jsonArchives{
			TotalFiles:     archives.TotalFiles,
			TotalRows:      archives.TotalRows,
			TotalSizeBytes: archives.TotalSizeBytes,
			TotalSizeHuman: formatBytes(archives.TotalSizeBytes),
			LocalFiles:     archives.LocalFiles,
			S3Files:        archives.S3Files,
			S3Buckets:      archives.S3Buckets,
		}
	} else if archives == nil && archivesErr != nil {
		// archive_state could not be READ — surface it so an absent archives
		// key is not misread as "no archives exist" (#1323).
		out.ArchivesError = &jsonSectionError{Error: archivesErr.Error()}
	}
	if coverage == nil && coverageErr != nil {
		// LoadCoverage failed — a monitor keyed on coverage.archives_unavailable
		// must find this loud sibling instead of no coverage key at all (#1323).
		out.CoverageError = &jsonSectionError{Error: coverageErr.Error()}
	}
	if coverage != nil {
		jc := &jsonCoverage{
			TotalEvents:    coverage.TotalEvents + coverage.ArchiveTotalRows,
			LiveEvents:     coverage.TotalEvents,
			ArchivedEvents: coverage.ArchiveTotalRows,
			IndexSizeBytes: coverage.IndexSizeBytes,
			SchemaChanges:  coverage.SchemaChanges,
			UncoveredDDLs:  coverage.UncoveredDDLs,
			// #816: the figures above are live-only and a LOWER BOUND when
			// this is set. A monitor that treats earliest_event as the
			// recovery horizon must be able to tell the difference.
			ArchivesUnavailable: coverage.ArchiveUnavailable,
			ArchivesError:       coverage.ArchiveError,
		}
		if coverage.IndexSizeBytes > 0 {
			jc.IndexSizeHuman = formatBytes(coverage.IndexSizeBytes)
		}

		// Effective earliest: archive may extend further back than live data.
		earliest := coverage.EarliestEvent
		if coverage.ArchiveEarliestHour.Valid &&
			(!earliest.Valid || coverage.ArchiveEarliestHour.Time.Before(earliest.Time)) {
			earliest = coverage.ArchiveEarliestHour
		}
		if earliest.Valid {
			s := earliest.Time.Format(TSFmt)
			jc.EarliestEvent = &s
		}
		if coverage.LatestEvent.Valid {
			s := coverage.LatestEvent.Time.Format(TSFmt)
			jc.LatestEvent = &s
		}
		if coverage.ArchiveEarliestHour.Valid {
			s := coverage.ArchiveEarliestHour.Time.Format(TSFmt)
			jc.ArchiveEarliestEvent = &s
		}
		out.Coverage = jc
	}

	for _, b := range baselines {
		jb := jsonBaseline{
			SnapshotTime: b.SnapshotTime.Format(TSFmt),
			Database:     b.Database,
			Table:        b.Table,
			Size:         b.Size,
		}
		if b.Size > 0 {
			jb.SizeHuman = formatBytes(b.Size)
		}
		if b.BinlogFile != "" {
			jb.BinlogFile = &b.BinlogFile
		}
		if b.BinlogPos != 0 {
			jb.BinlogPos = &b.BinlogPos
		}
		if b.GTIDSet != "" {
			jb.GTIDSet = &b.GTIDSet
		}
		jb.Staleness = string(b.Staleness)
		out.Baselines = append(out.Baselines, jb)
	}
	out.BaselineStaleness = string(OverallBaselineStaleness(baselines))
	if out.BaselineStaleness == "" && baselinesUnavailable {
		out.BaselineStaleness = string(BaselineUnknown)
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// writeBaselines writes the baselines section to a text status report.
func writeBaselines(w io.Writer, baselines []BaselineInfo) {
	if len(baselines) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "=== Baselines ===")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SNAPSHOT\tDATABASE\tTABLE\tSIZE\tBINLOG_FILE\tBINLOG_POS\tGTID\tSTALENESS")
	fmt.Fprintln(tw, "────────\t────────\t─────\t────\t───────────\t──────────\t────\t─────────")
	// The ⚠ glyph is reserved for rows the banner keys on — each table's
	// NEWEST snapshot. A superseded snapshot past coverage is routine on a
	// healthy retention cadence (the console's rule too); it still reads
	// "broken" honestly, just not as an alarm.
	newestOf := make(map[string]time.Time, len(baselines))
	for _, b := range baselines {
		k := b.Database + "." + b.Table
		if b.SnapshotTime.After(newestOf[k]) {
			newestOf[k] = b.SnapshotTime
		}
	}
	for _, b := range baselines {
		binlogFile := "-"
		if b.BinlogFile != "" {
			binlogFile = b.BinlogFile
		}
		binlogPos := "-"
		if b.BinlogPos != 0 {
			binlogPos = strconv.FormatInt(b.BinlogPos, 10)
		}
		gtid := "-"
		if b.GTIDSet != "" {
			gtid = Truncate(b.GTIDSet, 40)
		}
		size := "-"
		if b.Size > 0 {
			size = formatBytes(b.Size)
		}
		staleness := "-"
		if b.Staleness != "" {
			staleness = string(b.Staleness)
			if b.Staleness == BaselineBroken && newestOf[b.Database+"."+b.Table].Equal(b.SnapshotTime) {
				staleness = "⚠ broken"
			}
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			b.SnapshotTime.Format(TSFmt),
			b.Database, b.Table, size,
			binlogFile, binlogPos, gtid, staleness)
	}
	tw.Flush()

	// Continuity-banner-style loud line: a table whose NEWEST baseline
	// predates delta coverage cannot be fully restored through that hole, and
	// waiting for restore time to find out is the failure mode #1193 exists
	// to remove.
	// The check being DISARMED is itself a finding: a bare "unknown" in the
	// column reads as a cosmetic gap, while it actually means a broken
	// restore window would not be detected here (#1219 makes this the routine
	// verdict for below-floor snapshots on multi-source indexes; an
	// unreadable floor lands here too).
	if OverallBaselineStaleness(baselines) == BaselineUnknown {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== ⚠ BASELINE STALENESS NOT EVALUABLE ===")
		fmt.Fprintln(w, "The delta-coverage floor could not be established for at least one table:")
		fmt.Fprintln(w, "an index serving more than one source cannot attribute archived coverage to")
		fmt.Fprintln(w, "the source that owns a baseline, and an unreadable index yields the same.")
		fmt.Fprintln(w, "A broken restore window would NOT be detected here — see")
		fmt.Fprintln(w, "docs/rotation-and-status.md (Baseline staleness).")
	}
	if OverallBaselineStaleness(baselines) == BaselineBroken {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== ⚠ BASELINE STALE — FULL-TABLE RESTORE BROKEN ===")
		fmt.Fprintln(w, "The newest baseline for at least one table predates the oldest available")
		fmt.Fprintln(w, "delta coverage: reconstructing those tables through the missing window is")
		fmt.Fprintln(w, "impossible. Take a fresh baseline (bintrail dump + bintrail baseline).")
	}
}

// RetentionInfo is the built-in rotation window one index runs on while its
// operator sets none, with the sentence that says where it came from.
type RetentionInfo struct {
	Raw    string `json:"retain"`
	Source string `json:"source"`
	// Basis is the machine-readable form of Source: recorded | legacy |
	// unreadable. JSON consumers grade on this, never on the sentence.
	Basis string `json:"basis"`
}
