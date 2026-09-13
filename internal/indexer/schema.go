package indexer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/serverid"
)

// CreateIndexTables creates every index table (idempotent — all DDL is
// CREATE TABLE IF NOT EXISTS). Shared by `bintrail init` and the control-plane
// supervisor, which provisions a per-source index database with the same set.
// logTable is invoked per table for progress output; nil is allowed (no output).
func CreateIndexTables(ctx context.Context, db *sql.DB, partitions int, encrypt bool, logTable func(string)) error {
	if logTable == nil {
		logTable = func(string) {}
	}

	// Create binlog_events with dynamic hourly partitions.
	if err := createBinlogEventsTable(db, partitions, encrypt); err != nil {
		return fmt.Errorf("failed to create binlog_events: %w", err)
	}
	logTable("binlog_events")

	// If encryption was requested, verify that the table actually has it
	// enabled. CREATE TABLE IF NOT EXISTS is a no-op when the table already
	// exists, so a pre-existing unencrypted table will silently remain
	// unencrypted. Warn the operator so they can encrypt it manually.
	if encrypt {
		var createOpts string
		row := db.QueryRowContext(ctx,
			`SELECT COALESCE(CREATE_OPTIONS, '') FROM information_schema.TABLES
			 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'`)
		if err := row.Scan(&createOpts); err == nil &&
			!strings.Contains(strings.ToUpper(createOpts), "ENCRYPTION=Y") {
			fmt.Fprintf(os.Stderr, "Warning: binlog_events already exists without encryption.\n"+
				"To encrypt it, run: ALTER TABLE binlog_events ENCRYPTION='Y'\n")
		}
	}

	ddls := []struct {
		name string
		ddl  string
	}{
		{"schema_snapshots", ddlSchemaSnapshots},
		{"index_state", ddlIndexState},
		{"stream_state", ddlStreamState},
		{"bintrail_servers", serverid.DDLBintrailServers},
		{"bintrail_server_changes", serverid.DDLBintrailServerChanges},
		{"table_flags", ddlTableFlags},
		{"profiles", ddlProfiles},
		{"access_rules", ddlAccessRules},
		{"archive_state", ddlArchiveState},
		{"schema_changes", ddlSchemaChanges},
		{"fk_constraints", ddlFKConstraints},
		{"snapshot_id_seq", metadata.DDLSnapshotIDSeq},
	}
	for _, t := range ddls {
		if _, err := db.Exec(t.ddl); err != nil {
			return fmt.Errorf("failed to create %s: %w", t.name, err)
		}
		logTable(t.name)
	}
	return nil
}

// buildPartitionDefs returns numPartitions hourly partition clauses starting at
// the current hour (truncated from now), followed by a p_future catch-all.
//
// Partitions span forward from the current hour so that incoming events
// land in named partitions rather than accumulating in p_future.
// With numPartitions=48 (the
// default), the range covers the current hour through the next 47 hours.
// New events arriving beyond that range fall into p_future until rotate adds
// more named partitions.
func buildPartitionDefs(now time.Time, numPartitions int) []string {
	now = now.UTC().Truncate(time.Hour)
	start := now

	parts := make([]string, 0, numPartitions+1)
	for i := range numPartitions {
		hour := start.Add(time.Duration(i) * time.Hour)
		nextHour := hour.Add(time.Hour)
		parts = append(parts, fmt.Sprintf(
			"    PARTITION p_%s VALUES LESS THAN (TO_SECONDS('%s'))",
			hour.Format("2006010215"),
			nextHour.UTC().Format("2006-01-02 15:04:05"),
		))
	}
	parts = append(parts, "    PARTITION p_future VALUES LESS THAN MAXVALUE")
	return parts
}

// buildBinlogEventsDDL assembles the full CREATE TABLE statement for
// binlog_events. When encrypt is true, ENCRYPTION='Y' is added to the table
// options so MySQL uses InnoDB tablespace encryption (requires a keyring
// plugin on the server).
func buildBinlogEventsDDL(parts []string, encrypt bool) string {
	encryptClause := ""
	if encrypt {
		encryptClause = " ENCRYPTION='Y'"
	}
	return `CREATE TABLE IF NOT EXISTS binlog_events (
    event_id        BIGINT UNSIGNED AUTO_INCREMENT,
    binlog_file     VARCHAR(255)     NOT NULL,
    start_pos       BIGINT UNSIGNED  NOT NULL,
    end_pos         BIGINT UNSIGNED  NOT NULL,
    event_timestamp DATETIME         NOT NULL,
    gtid            VARCHAR(255)     DEFAULT NULL,
    connection_id   INT UNSIGNED     DEFAULT NULL COMMENT 'MySQL connection ID (pseudo_thread_id) that produced this event',
    schema_name     VARCHAR(64)      NOT NULL,
    table_name      VARCHAR(64)      NOT NULL,
    event_type      TINYINT UNSIGNED NOT NULL COMMENT '1=INSERT, 2=UPDATE, 3=DELETE',
    pk_values       VARCHAR(512)     NOT NULL COMMENT 'PK values in ordinal order, pipe-delimited. e.g. 12345 or 12345|2',
    pk_hash         VARCHAR(64)      AS (SHA2(pk_values, 256)) STORED,
    changed_columns JSON             DEFAULT NULL COMMENT 'list of columns that changed (UPDATEs only)',
    row_before      JSON             DEFAULT NULL COMMENT 'full row before image (UPDATE, DELETE)',
    row_after       JSON             DEFAULT NULL COMMENT 'full row after image (INSERT, UPDATE)',
    schema_version  INT UNSIGNED     NOT NULL DEFAULT 0 COMMENT 'snapshot_id from schema_snapshots at index time; enables per-row resolver lookup for recovery',
    query_text      MEDIUMTEXT       DEFAULT NULL COMMENT 'original SQL statement from ROWS_QUERY/ANNOTATE_ROWS; NULL unless binlog_rows_query_log_events (MySQL) / binlog_annotate_row_events (MariaDB) is ON at the source (#699)',
    query_hash      CHAR(64)         DEFAULT NULL COMMENT 'STATEMENT_DIGEST(query_text) computed on the index connection at index time; groups statements by normalized shape (#699)',
    commit_ts_us    BIGINT UNSIGNED  DEFAULT NULL COMMENT 'transaction commit time in microseconds since epoch, from the GTID event immediate_commit_timestamp (MySQL 8.0.1+, anonymous GTID events included); NULL on MariaDB and pre-8.0.1 MySQL',
    PRIMARY KEY (event_id, event_timestamp),
    INDEX idx_row_lookup (schema_name, table_name, event_timestamp),
    INDEX idx_pk_hash    (schema_name, table_name, pk_hash, event_timestamp),
    INDEX idx_gtid       (gtid)
) ENGINE=InnoDB` + encryptClause + `
  PARTITION BY RANGE (TO_SECONDS(event_timestamp)) (
` + strings.Join(parts, ",\n") + `
)`
}

// createBinlogEventsTable generates the CREATE TABLE with N hourly partitions
// spanning from the current hour (UTC) forward through the next N-1 hours, plus
// a p_future catch-all partition for any events arriving beyond that range.
//
// Each partition p_YYYYMMDDHH covers events where TO_SECONDS(event_timestamp)
// is less than TO_SECONDS of the following hour (timezone-independent).
// When encrypt is true, ENCRYPTION='Y' is added to enable InnoDB tablespace
// encryption (requires a keyring plugin on the MySQL server).
func createBinlogEventsTable(db *sql.DB, numPartitions int, encrypt bool) error {
	parts := buildPartitionDefs(time.Now(), numPartitions)
	ddl := buildBinlogEventsDDL(parts, encrypt)
	_, err := db.Exec(ddl)
	return err
}

const ddlSchemaSnapshots = `CREATE TABLE IF NOT EXISTS schema_snapshots (
    id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    snapshot_id      INT UNSIGNED NOT NULL,
    snapshot_time    DATETIME     NOT NULL,
    schema_name      VARCHAR(64)  NOT NULL,
    table_name       VARCHAR(64)  NOT NULL,
    column_name      VARCHAR(64)  NOT NULL,
    ordinal_position INT UNSIGNED NOT NULL,
    column_key       VARCHAR(3)   NOT NULL COMMENT 'PRI, UNI, MUL, or empty',
    data_type        VARCHAR(64)  NOT NULL,
    column_type      TEXT         DEFAULT NULL COMMENT 'full type from information_schema.COLUMNS.COLUMN_TYPE, e.g. "datetime(6)" or "enum(...)"; needed by full-table reconstruct (DATETIME precision) and shim ENUM/SET label mapping. TEXT not VARCHAR: a realistic ENUM declaration easily exceeds 128 chars and the 1406 aborts the whole snapshot transaction',
    character_set_name VARCHAR(32) DEFAULT NULL COMMENT 'information_schema.COLUMNS.CHARACTER_SET_NAME; NULL for BINARY/VARBINARY/numeric columns; enables safe latin1-to-UTF8 transcoding of CHAR/VARCHAR values at index time instead of silent U+FFFD corruption (#756)',
    is_nullable      VARCHAR(3)   NOT NULL,
    column_default   TEXT         DEFAULT NULL,
    is_generated     TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '1 if STORED or VIRTUAL generated column',
    pg_type_oid      INT UNSIGNED DEFAULT NULL COMMENT 'PostgreSQL pg_type OID (pgoutput RelationMessage); NULL for MySQL snapshots (#533)',
    pg_type_mod      INT          DEFAULT NULL COMMENT 'PostgreSQL atttypmod (pgoutput RelationMessage); NULL for MySQL snapshots (#533)',
    is_identity_always TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1 if PostgreSQL GENERATED ALWAYS AS IDENTITY; 0 for MySQL (#557)',
    INDEX idx_snapshot_id    (snapshot_id),
    INDEX idx_snapshot_table (snapshot_id, schema_name, table_name)
) ENGINE=InnoDB`

const ddlStreamState = `CREATE TABLE IF NOT EXISTS stream_state (
    id               INT UNSIGNED    PRIMARY KEY DEFAULT 1,
    mode             ENUM('position','gtid') NOT NULL,
    binlog_file      VARCHAR(255)    NOT NULL DEFAULT '',
    binlog_position  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    gtid_set         TEXT            DEFAULT NULL,
    flavor           VARCHAR(16)     NOT NULL DEFAULT 'mysql' COMMENT 'source flavor: mysql or mariadb; selects the GTID parser on resume',
    events_indexed   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_event_time  DATETIME        DEFAULT NULL,
    last_checkpoint  DATETIME        NOT NULL,
    server_id        INT UNSIGNED    NOT NULL,
    bintrail_id      CHAR(36)        NULL DEFAULT NULL,
    gap_lost_at      DATETIME        DEFAULT NULL COMMENT 'when an unfillable binlog gap forced an auto-advance (events permanently lost); cleared only by an explicit monitor Stop; --reset re-stamps or preserves it',
    gap_lost_detail  TEXT            DEFAULT NULL COMMENT 'human-readable description of the lost gap',
    source_health    JSON            DEFAULT NULL COMMENT 'latest source-side health snapshot (PostgreSQL: replication-slot wal_status/lag + REPLICA IDENTITY coverage) with an embedded checked_at; serialized payload, source-agnostic column',
    capture_skips    JSON            DEFAULT NULL COMMENT 'per-reason monotonic counters of events the daemon read and chose to drop (#1034): {"<reason>":{"count":N,"last_at":"RFC3339"}}; {} = evaluated and clean; NULL = no skip-aware daemon has written yet',
    capture_skips_ack JSON           DEFAULT NULL COMMENT 'operator acknowledgement of capture_skips (#1314): {"<reason>":{"count":N,"at":"RFC3339"}}. Written ONLY by an explicit acknowledgement, never by the capture daemon — which is why it cannot live inside capture_skips, a column every checkpoint overwrites. A reason is acknowledged while ack.count >= skips.count, so a new skip raises the alarm again on its own',
    CONSTRAINT single_row CHECK (id = 1)
) ENGINE=InnoDB`

const ddlIndexState = `CREATE TABLE IF NOT EXISTS index_state (
    binlog_file    VARCHAR(255) PRIMARY KEY,
    file_size      BIGINT UNSIGNED NOT NULL,
    last_position  BIGINT UNSIGNED NOT NULL COMMENT 'last parsed position',
    events_indexed BIGINT UNSIGNED NOT NULL DEFAULT 0,
    status         ENUM('in_progress','completed','failed') NOT NULL,
    started_at     DATETIME NOT NULL,
    completed_at   DATETIME DEFAULT NULL,
    error_message  TEXT     DEFAULT NULL,
    bintrail_id    CHAR(36) NULL DEFAULT NULL,
    INDEX idx_bintrail_id (bintrail_id)
) ENGINE=InnoDB`

// ─── Archive tracking ─────────────────────────────────────────────────────────

const ddlArchiveState = `CREATE TABLE IF NOT EXISTS archive_state (
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    partition_name  VARCHAR(20) NOT NULL,
    bintrail_id     VARCHAR(36),
    local_path      VARCHAR(1024),
    file_size_bytes BIGINT UNSIGNED,
    row_count       BIGINT UNSIGNED,
    s3_bucket       VARCHAR(255),
    s3_key          VARCHAR(1024),
    s3_uploaded_at  DATETIME,
    min_event_ts    DATETIME DEFAULT NULL COMMENT 'MIN(event_timestamp) of the archived rows; NULL for archives written before #1037 or registered by upload/reconcile. Content-derived pruning: may precede the partition hour label when backfilled events landed in the partition',
    max_event_ts    DATETIME DEFAULT NULL COMMENT 'MAX(event_timestamp) of the archived rows; see min_event_ts (#1037)',
    column_set      VARCHAR(4096) DEFAULT NULL COMMENT 'the archived Parquet file own column set: lowercase, sorted, comma-joined (#1535). NULL = unknown (written before this column, or registered without a footer read); archive reconcile --repair records it, needing --deep on an S3-only archive',
    archived_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_partition (partition_name, bintrail_id)
) ENGINE=InnoDB`

// ─── RBAC tables ─────────────────────────────────────────────────────────────

// ddlTableFlags stores named flags on tables or individual columns.
// column_name = ” means the flag applies to the whole table; a non-empty
// value names the specific column that carries the flag.
// This two-level design lets access_rules express both "deny the billing
// table" (table-level flag) and "redact the amount column" (column-level flag)
// using the same flag name with different column_name values.
const ddlTableFlags = `CREATE TABLE IF NOT EXISTS table_flags (
    id          INT UNSIGNED  AUTO_INCREMENT PRIMARY KEY,
    schema_name VARCHAR(64)   NOT NULL,
    table_name  VARCHAR(64)   NOT NULL,
    column_name VARCHAR(64)   NOT NULL DEFAULT '' COMMENT 'empty = table-level flag; non-empty = column-level flag',
    flag        VARCHAR(255)  NOT NULL,
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_unique (schema_name, table_name, column_name, flag),
    INDEX idx_flag (flag)
) ENGINE=InnoDB`

// ddlProfiles stores named access profiles (e.g. "dev", "marketing").
// Profiles are referenced by access_rules to define what flags each profile
// may or may not access. Management is typically done from the web panel;
// the CLI provides the 'bintrail flag' command for DBA use.
const ddlProfiles = `CREATE TABLE IF NOT EXISTS profiles (
    id          INT UNSIGNED  AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(255)  NOT NULL,
    description TEXT          DEFAULT NULL,
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_name (name)
) ENGINE=InnoDB`

// ddlAccessRules maps a profile to a flag with an allow/deny permission.
// Combined with table_flags this enables RBAC:
//   - profile "dev" DENY flag "billing"  → dev users cannot see billing table events
//   - profile "marketing" DENY flag "pii" → marketing users see events but pii columns are redacted
const ddlAccessRules = `CREATE TABLE IF NOT EXISTS access_rules (
    id          INT UNSIGNED  AUTO_INCREMENT PRIMARY KEY,
    profile_id  INT UNSIGNED  NOT NULL,
    flag        VARCHAR(255)  NOT NULL,
    permission  ENUM('allow','deny') NOT NULL,
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_profile_flag (profile_id, flag),
    CONSTRAINT fk_access_rules_profile FOREIGN KEY (profile_id) REFERENCES profiles (id) ON DELETE CASCADE
) ENGINE=InnoDB`

// ─── FK constraints ───────────────────────────────────────────────────────────

const ddlFKConstraints = `CREATE TABLE IF NOT EXISTS fk_constraints (
    snapshot_id              INT UNSIGNED NOT NULL,
    constraint_name          VARCHAR(64)  NOT NULL,
    schema_name              VARCHAR(64)  NOT NULL,
    table_name               VARCHAR(64)  NOT NULL,
    column_name              VARCHAR(64)  NOT NULL,
    ordinal_position         INT          NOT NULL,
    referenced_schema_name   VARCHAR(64)  NOT NULL,
    referenced_table_name    VARCHAR(64)  NOT NULL,
    referenced_column_name   VARCHAR(64)  NOT NULL,
    delete_rule              VARCHAR(16)  NOT NULL DEFAULT '' COMMENT 'ON DELETE rule (CASCADE/RESTRICT/SET NULL/NO ACTION); empty for pre-cascade-recovery snapshots',
    update_rule              VARCHAR(16)  NOT NULL DEFAULT '' COMMENT 'ON UPDATE rule; empty for pre-cascade-recovery snapshots',
    PRIMARY KEY (snapshot_id, schema_name, constraint_name, ordinal_position)
) ENGINE=InnoDB`

// ─── DDL tracking ─────────────────────────────────────────────────────────────

const ddlSchemaChanges = `CREATE TABLE IF NOT EXISTS schema_changes (
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    detected_at     DATETIME NOT NULL,
    binlog_file     VARCHAR(255) NOT NULL,
    binlog_pos      BIGINT UNSIGNED NOT NULL,
    gtid            VARCHAR(255) DEFAULT NULL,
    schema_name     VARCHAR(64) NOT NULL,
    table_name      VARCHAR(64) NOT NULL,
    ddl_type        VARCHAR(50) NOT NULL,
    ddl_query       TEXT NOT NULL,
    snapshot_id     INT UNSIGNED DEFAULT NULL COMMENT 'auto-snapshot after DDL; NULL when not taken (file mode or snapshot failure)',
    INDEX idx_detected_at (detected_at),
    INDEX idx_schema_table (schema_name, table_name)
) ENGINE=InnoDB`
