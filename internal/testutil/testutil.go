// Package testutil provides shared helpers for integration tests that require
// a live MySQL connection. All helpers are designed for use in test functions
// and call t.Fatal/t.Skip on errors as appropriate.
package testutil

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/serverid"
)

// dbCounter ensures unique database names across calls within the same test.
var dbCounter atomic.Int64

// DefaultDSN is the base DSN used when BINTRAIL_TEST_DSN is not set.
// It assumes a local Docker container on port 13306 with root:testroot.
const DefaultDSN = "root:testroot@tcp(127.0.0.1:13306)"

// BaseDSN returns the base DSN (without database name) from the environment
// or the default. It always includes parseTime=true.
func BaseDSN() string {
	if env := os.Getenv("BINTRAIL_TEST_DSN"); env != "" {
		return env
	}
	return DefaultDSN
}

// IntegrationDSN returns a full DSN with the given database name appended.
func IntegrationDSN(dbName string) string {
	return BaseDSN() + "/" + dbName + "?parseTime=true"
}

// MySQLRequired reports whether MySQL integration tests must RUN (and fail
// rather than skip) when the server is unreachable. The dedicated 8.0/8.4
// integration matrix jobs set BINTRAIL_REQUIRE_MYSQL=1, where a MySQL server
// is guaranteed present — so a silent skip there would be a false green that
// hides a real regression in the flagship capture->index->query->recover
// pipeline. Unset on developer machines, where graceful skipping is right.
// Mirrors MariaDBRequired / PostgresRequired.
func MySQLRequired() bool {
	return os.Getenv("BINTRAIL_REQUIRE_MYSQL") == "1"
}

// SkipIfNoMySQL pings the MySQL server and calls t.Skip if unreachable, or
// t.Fatal when MySQLRequired() — so a dial/ping failure never silently
// passes as green in the CI job that guarantees a live MySQL server. This
// provides graceful degradation when no Docker container is running.
func SkipIfNoMySQL(t *testing.T) {
	t.Helper()
	db, err := sql.Open("mysql", BaseDSN()+"/?parseTime=true")
	if err != nil {
		skipOrFailMySQL(t, "cannot open MySQL connection: %v", err)
		return
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		skipOrFailMySQL(t, "MySQL not reachable: %v", err)
	}
}

// skipOrFailMySQL fails the test when MySQLRequired(), otherwise skips it.
// Scoped to connectivity failures only (dial/ping) — callers must not use
// this for other skip reasons (e.g. a version-gated feature), which must
// stay t.Skip regardless of MySQLRequired().
func skipOrFailMySQL(t *testing.T, format string, args ...any) {
	t.Helper()
	if MySQLRequired() {
		t.Fatalf(format+" (BINTRAIL_REQUIRE_MYSQL=1 — the integration matrix job must provide a live MySQL server)", args...)
	}
	t.Skipf("skipping: "+format, args...)
}

// CreateTestDB creates a unique database for the calling test, returning
// a connected *sql.DB, the database name, and a cleanup function that
// drops the database. The cleanup function is also registered via t.Cleanup.
func CreateTestDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	SkipIfNoMySQL(t)

	// Build a unique name: test name (sanitised) + atomic counter for uniqueness.
	name := fmt.Sprintf("%s_%d", sanitiseDBName(t.Name()), dbCounter.Add(1))

	// Connect without a specific database to create one.
	rootDB, err := sql.Open("mysql", BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatalf("failed to connect for DB creation: %v", err)
	}
	defer rootDB.Close()

	// Drop first in case a previous test run left it behind.
	rootDB.Exec("DROP DATABASE IF EXISTS `" + name + "`")
	if _, err := rootDB.Exec("CREATE DATABASE `" + name + "`"); err != nil {
		t.Fatalf("failed to create test database %q: %v", name, err)
	}

	dsn := IntegrationDSN(name)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to connect to test database %q: %v", name, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping test database %q: %v", name, err)
	}

	t.Cleanup(func() {
		db.Close()
		cleanup, _ := sql.Open("mysql", BaseDSN()+"/?parseTime=true")
		if cleanup != nil {
			cleanup.Exec("DROP DATABASE IF EXISTS `" + name + "`")
			cleanup.Close()
		}
	})

	return db, name
}

// ─── MariaDB source helpers (MariaDB-as-source alpha) ─────────────────────────
//
// These mirror the MySQL helpers above for a MariaDB SOURCE container, used only
// by MariaDB-tagged integration/e2e tests. The MySQL helpers (index DB) are left
// completely unchanged. Default container: port 13307, root:testroot.

// MariaDBDefaultDSN is the base DSN used when BINTRAIL_TEST_MARIADB_DSN is not
// set. It assumes a local Docker MariaDB container on port 13307 with
// root:testroot (distinct from the MySQL index container on 13306).
const MariaDBDefaultDSN = "root:testroot@tcp(127.0.0.1:13307)"

// MariaDBBaseDSN returns the base MariaDB source DSN (without database name)
// from the environment or the default. It always includes parseTime=true.
func MariaDBBaseDSN() string {
	if env := os.Getenv("BINTRAIL_TEST_MARIADB_DSN"); env != "" {
		return env
	}
	return MariaDBDefaultDSN
}

// MariaDBRequired reports whether MariaDB integration tests must RUN (and fail
// rather than skip when MariaDB is unavailable or a setup step no-ops). The
// dedicated CI job sets BINTRAIL_REQUIRE_MARIADB=1, where a MariaDB source is
// guaranteed present — so a silent skip there would be a false green that hides
// a real regression in the headline capture path. Unset on developer machines
// and the MySQL-only matrix job, where graceful skipping is the right behavior.
func MariaDBRequired() bool {
	return os.Getenv("BINTRAIL_REQUIRE_MARIADB") == "1"
}

// SkipOrFailMariaDB skips the test, or fails it when MariaDBRequired() — so a
// MariaDB step that can't run never silently passes as green in CI.
func SkipOrFailMariaDB(t *testing.T, format string, args ...any) {
	t.Helper()
	if MariaDBRequired() {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}

// SkipIfNoMariaDB pings the MariaDB source server and skips (or fails, when
// MariaDBRequired()) if unreachable, so MariaDB tests degrade gracefully when
// only the MySQL container is running but cannot false-green in the CI job that
// guarantees a MariaDB source.
func SkipIfNoMariaDB(t *testing.T) {
	t.Helper()
	db, err := sql.Open("mysql", MariaDBBaseDSN()+"/?parseTime=true")
	if err == nil {
		defer db.Close()
		err = db.Ping()
	}
	if err != nil {
		SkipOrFailMariaDB(t, "MariaDB not reachable: %v", err)
	}
}

// CreateTestMariaDB creates a unique database on the MariaDB SOURCE server,
// returning a connected *sql.DB, the database name, and registering cleanup.
// It mirrors CreateTestDB but targets the MariaDB container (13307).
func CreateTestMariaDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	SkipIfNoMariaDB(t)

	name := fmt.Sprintf("%s_%d", sanitiseDBName(t.Name()), dbCounter.Add(1))

	rootDB, err := sql.Open("mysql", MariaDBBaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatalf("failed to connect to MariaDB for DB creation: %v", err)
	}
	defer rootDB.Close()

	rootDB.Exec("DROP DATABASE IF EXISTS `" + name + "`")
	if _, err := rootDB.Exec("CREATE DATABASE `" + name + "`"); err != nil {
		t.Fatalf("failed to create MariaDB test database %q: %v", name, err)
	}

	dsn := MariaDBBaseDSN() + "/" + name + "?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to connect to MariaDB test database %q: %v", name, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping MariaDB test database %q: %v", name, err)
	}

	t.Cleanup(func() {
		db.Close()
		cleanup, _ := sql.Open("mysql", MariaDBBaseDSN()+"/?parseTime=true")
		if cleanup != nil {
			cleanup.Exec("DROP DATABASE IF EXISTS `" + name + "`")
			cleanup.Close()
		}
	})

	return db, name
}

// MustExec executes a query or calls t.Fatal on error.
func MustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("MustExec failed: %s\n  error: %v", query, err)
	}
}

// sanitiseDBName converts a test name like "TestFoo/sub_test" into a valid
// MySQL database name (max 64 chars, alphanumeric + underscore).
func sanitiseDBName(testName string) string {
	var b strings.Builder
	b.WriteString("bt_")
	for _, r := range testName {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	name := b.String()
	if len(name) > 50 {
		name = name[:50]
	}
	return name
}

// InitIndexTables creates all index tables in the given database.
// This mirrors `bintrail init` without requiring the CLI binary.
func InitIndexTables(t *testing.T, db *sql.DB) {
	t.Helper()

	MustExec(t, db, `CREATE TABLE IF NOT EXISTS binlog_events (
		event_id        BIGINT UNSIGNED  AUTO_INCREMENT,
		binlog_file     VARCHAR(255)     NOT NULL,
		start_pos       BIGINT UNSIGNED  NOT NULL,
		end_pos         BIGINT UNSIGNED  NOT NULL,
		event_timestamp DATETIME         NOT NULL,
		gtid            VARCHAR(255)     DEFAULT NULL,
		connection_id   INT UNSIGNED     DEFAULT NULL,
		schema_name     VARCHAR(64)      NOT NULL,
		table_name      VARCHAR(64)      NOT NULL,
		event_type      TINYINT UNSIGNED NOT NULL,
		pk_values       VARCHAR(512)     NOT NULL,
		pk_hash         VARCHAR(64)      AS (SHA2(pk_values, 256)) STORED,
		changed_columns JSON             DEFAULT NULL,
		row_before      JSON             DEFAULT NULL,
		row_after       JSON             DEFAULT NULL,
		schema_version  INT UNSIGNED     NOT NULL DEFAULT 0,
		query_text      MEDIUMTEXT       DEFAULT NULL,
		query_hash      CHAR(64)         DEFAULT NULL,
		commit_ts_us    BIGINT UNSIGNED  DEFAULT NULL,
		PRIMARY KEY (event_id, event_timestamp),
		INDEX idx_row_lookup (schema_name, table_name, event_timestamp),
		INDEX idx_pk_hash    (schema_name, table_name, pk_hash, event_timestamp),
		INDEX idx_gtid       (gtid)
	) ENGINE=InnoDB
	  PARTITION BY RANGE (TO_SECONDS(event_timestamp)) (
		PARTITION p_future VALUES LESS THAN MAXVALUE
	  )`)

	MustExec(t, db, `CREATE TABLE IF NOT EXISTS schema_snapshots (
		id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		snapshot_id      INT UNSIGNED NOT NULL,
		snapshot_time    DATETIME     NOT NULL,
		schema_name      VARCHAR(64)  NOT NULL,
		table_name       VARCHAR(64)  NOT NULL,
		column_name      VARCHAR(64)  NOT NULL,
		ordinal_position INT UNSIGNED NOT NULL,
		column_key       VARCHAR(3)   NOT NULL,
		data_type        VARCHAR(64)  NOT NULL,
		column_type      TEXT         DEFAULT NULL,
		character_set_name VARCHAR(32) DEFAULT NULL,
		is_nullable      VARCHAR(3)   NOT NULL,
		column_default   TEXT         DEFAULT NULL,
		is_generated     TINYINT(1)   NOT NULL DEFAULT 0,
		pg_type_oid      INT UNSIGNED DEFAULT NULL,
		pg_type_mod      INT          DEFAULT NULL,
		is_identity_always TINYINT(1) NOT NULL DEFAULT 0,
		INDEX idx_snapshot_id    (snapshot_id),
		INDEX idx_snapshot_table (snapshot_id, schema_name, table_name)
	) ENGINE=InnoDB`)

	MustExec(t, db, `CREATE TABLE IF NOT EXISTS index_state (
		binlog_file    VARCHAR(255) PRIMARY KEY,
		file_size      BIGINT UNSIGNED NOT NULL,
		last_position  BIGINT UNSIGNED NOT NULL,
		events_indexed BIGINT UNSIGNED NOT NULL DEFAULT 0,
		status         ENUM('in_progress','completed','failed') NOT NULL,
		started_at     DATETIME NOT NULL,
		completed_at   DATETIME DEFAULT NULL,
		error_message  TEXT     DEFAULT NULL,
		bintrail_id    CHAR(36) NULL DEFAULT NULL,
		INDEX idx_bintrail_id (bintrail_id)
	) ENGINE=InnoDB`)

	MustExec(t, db, `CREATE TABLE IF NOT EXISTS stream_state (
		id               INT UNSIGNED    PRIMARY KEY DEFAULT 1,
		mode             ENUM('position','gtid') NOT NULL,
		binlog_file      VARCHAR(255)    NOT NULL DEFAULT '',
		binlog_position  BIGINT UNSIGNED NOT NULL DEFAULT 0,
		gtid_set         TEXT            DEFAULT NULL,
		flavor           VARCHAR(16)     NOT NULL DEFAULT 'mysql',
		events_indexed   BIGINT UNSIGNED NOT NULL DEFAULT 0,
		last_event_time  DATETIME        DEFAULT NULL,
		last_checkpoint  DATETIME        NOT NULL,
		server_id        INT UNSIGNED    NOT NULL,
		bintrail_id      CHAR(36)        NULL DEFAULT NULL,
		gap_lost_at      DATETIME        DEFAULT NULL,
		gap_lost_detail  TEXT            DEFAULT NULL,
		source_health    JSON            DEFAULT NULL,
		capture_skips    JSON            DEFAULT NULL,
		capture_skips_ack JSON           DEFAULT NULL,
		CONSTRAINT single_row CHECK (id = 1)
	) ENGINE=InnoDB`)

	MustExec(t, db, serverid.DDLBintrailServers)
	MustExec(t, db, serverid.DDLBintrailServerChanges)

	// snapshot_id_seq (#844): dedicated AUTO_INCREMENT counter for
	// schema_snapshots.snapshot_id allocation. Hand-inlined rather than
	// imported from internal/metadata.DDLSnapshotIDSeq — that package's own
	// _test.go files import this package, so importing metadata here would
	// be an import cycle in the test build.
	MustExec(t, db, `CREATE TABLE IF NOT EXISTS snapshot_id_seq (
		id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
	) ENGINE=InnoDB`)

	MustExec(t, db, `CREATE TABLE IF NOT EXISTS table_flags (
		id          INT UNSIGNED  AUTO_INCREMENT PRIMARY KEY,
		schema_name VARCHAR(64)   NOT NULL,
		table_name  VARCHAR(64)   NOT NULL,
		column_name VARCHAR(64)   NOT NULL DEFAULT '',
		flag        VARCHAR(255)  NOT NULL,
		created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UNIQUE KEY idx_unique (schema_name, table_name, column_name, flag),
		INDEX idx_flag (flag)
	) ENGINE=InnoDB`)

	MustExec(t, db, `CREATE TABLE IF NOT EXISTS profiles (
		id          INT UNSIGNED  AUTO_INCREMENT PRIMARY KEY,
		name        VARCHAR(255)  NOT NULL,
		description TEXT          DEFAULT NULL,
		created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UNIQUE KEY idx_name (name)
	) ENGINE=InnoDB`)

	MustExec(t, db, `CREATE TABLE IF NOT EXISTS access_rules (
		id          INT UNSIGNED  AUTO_INCREMENT PRIMARY KEY,
		profile_id  INT UNSIGNED  NOT NULL,
		flag        VARCHAR(255)  NOT NULL,
		permission  ENUM('allow','deny') NOT NULL,
		created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UNIQUE KEY idx_profile_flag (profile_id, flag),
		CONSTRAINT fk_access_rules_profile FOREIGN KEY (profile_id) REFERENCES profiles (id) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	MustExec(t, db, `CREATE TABLE IF NOT EXISTS archive_state (
		id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		partition_name  VARCHAR(20) NOT NULL,
		bintrail_id     VARCHAR(36),
		local_path      VARCHAR(1024),
		file_size_bytes BIGINT UNSIGNED,
		row_count       BIGINT UNSIGNED,
		s3_bucket       VARCHAR(255),
		s3_key          VARCHAR(1024),
		s3_uploaded_at  DATETIME,
		min_event_ts    DATETIME DEFAULT NULL,
		max_event_ts    DATETIME DEFAULT NULL,
		column_set      VARCHAR(4096) DEFAULT NULL,
		archived_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UNIQUE KEY uq_partition (partition_name, bintrail_id)
	) ENGINE=InnoDB`)

	MustExec(t, db, `CREATE TABLE IF NOT EXISTS fk_constraints (
		snapshot_id              INT UNSIGNED NOT NULL,
		constraint_name          VARCHAR(64)  NOT NULL,
		schema_name              VARCHAR(64)  NOT NULL,
		table_name               VARCHAR(64)  NOT NULL,
		column_name              VARCHAR(64)  NOT NULL,
		ordinal_position         INT          NOT NULL,
		referenced_schema_name   VARCHAR(64)  NOT NULL,
		referenced_table_name    VARCHAR(64)  NOT NULL,
		referenced_column_name   VARCHAR(64)  NOT NULL,
		delete_rule              VARCHAR(16)  NOT NULL DEFAULT '',
		update_rule              VARCHAR(16)  NOT NULL DEFAULT '',
		PRIMARY KEY (snapshot_id, schema_name, constraint_name, ordinal_position)
	) ENGINE=InnoDB`)

	MustExec(t, db, `CREATE TABLE IF NOT EXISTS schema_changes (
		id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		detected_at     DATETIME NOT NULL,
		binlog_file     VARCHAR(255) NOT NULL,
		binlog_pos      BIGINT UNSIGNED NOT NULL,
		gtid            VARCHAR(255) DEFAULT NULL,
		schema_name     VARCHAR(64) NOT NULL,
		table_name      VARCHAR(64) NOT NULL,
		ddl_type        VARCHAR(50) NOT NULL,
		ddl_query       TEXT NOT NULL,
		snapshot_id     INT UNSIGNED DEFAULT NULL,
		INDEX idx_detected_at (detected_at)
	) ENGINE=InnoDB`)
}

// InsertEvent inserts a single event into binlog_events using raw SQL.
// Useful for setting up test data without going through the indexer.
func InsertEvent(t *testing.T, db *sql.DB,
	binlogFile string, startPos, endPos uint64,
	ts string, gtid *string,
	schema, table string, eventType uint8,
	pkValues string,
	changedCols, rowBefore, rowAfter []byte,
) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, gtid,
		 schema_name, table_name, event_type, pk_values,
		 changed_columns, row_before, row_after)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		binlogFile, startPos, endPos, ts, gtid,
		schema, table, eventType, pkValues,
		changedCols, rowBefore, rowAfter,
	)
	if err != nil {
		t.Fatalf("InsertEvent failed: %v", err)
	}
}

// InsertSnapshot inserts a single snapshot row into schema_snapshots.
func InsertSnapshot(t *testing.T, db *sql.DB,
	snapshotID int, snapshotTime, schema, table, column string,
	ordinal int, columnKey, dataType, isNullable string,
) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, is_nullable)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshotID, snapshotTime, schema, table, column,
		ordinal, columnKey, dataType, isNullable,
	)
	if err != nil {
		t.Fatalf("InsertSnapshot failed: %v", err)
	}
}

// SnapshotDSN returns a DSN string for the given database name, suitable for
// passing to commands that accept --index-dsn.
func SnapshotDSN(dbName string) string {
	return fmt.Sprintf("%s/%s?parseTime=true", BaseDSN(), dbName)
}
