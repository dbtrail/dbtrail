package metadata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"strings"
)

// DDLSnapshotExclusions records which base tables a DEGRADED snapshot
// (TakeSnapshotExcludingInvalid, #1051) left out and why. It is the EXPLICIT
// source of truth the cascade FK loaders read to flag
// CascadeFK.ChildExcludedFromSnapshot: inferring exclusion from "the child has
// fk_constraints rows but no schema_snapshots rows" is unsound — a table
// created between TakeSnapshot's columns query and its FK query produces that
// exact shape with nothing excluded — and a false "provably partial" caveat is
// the cry-wolf failure the cascade Result contract forbids. An absent table
// (legacy index, or no degraded snapshot ever taken) simply means "no
// exclusions"; readers must tolerate it.
//
// The name columns are BINARY (SnapshotExclusionsNameCollation, #1815). On
// the index database's default collation (utf8mb4_0900_ai_ci on 8.x) two
// tables whose names differ only in case or accent — `Audit_Log` and
// `audit_log`, `cafe` and `café`, both ordinary on a case-sensitive source —
// were one key, and the second exclusion's ERROR 1062 rolled back the WHOLE
// snapshot transaction: the DDL hook's auto-snapshot failed and capture
// restarted into the same DDL. The source keeps them apart, and so does the
// capture-skip ledger; schema_snapshots stores both only because it has no
// unique key on names. This table must keep them apart too.
const DDLSnapshotExclusions = `CREATE TABLE IF NOT EXISTS snapshot_exclusions (
    snapshot_id INT UNSIGNED NOT NULL,
    schema_name VARCHAR(64)  COLLATE utf8mb4_bin NOT NULL,
    table_name  VARCHAR(64)  COLLATE utf8mb4_bin NOT NULL,
    reason      VARCHAR(64)  NOT NULL,
    pk_column   VARCHAR(64)  DEFAULT NULL COMMENT 'a column name this table does not already use, for the PRIMARY KEY it would take to capture it (#1802); NULL when none was needed or the row predates this column',
    PRIMARY KEY (snapshot_id, schema_name, table_name)
) ENGINE=InnoDB COMMENT='tables a degraded snapshot excluded (no PK / non-InnoDB, #1051); see metadata.DDLSnapshotExclusions'`

// The reasons a degraded snapshot records in snapshot_exclusions.reason. They
// are PERSISTED vocabulary: every index that ever excluded a table holds these
// exact strings, and the status report classifies them to name the fix (#1802).
// Renaming one would turn every recorded row into an unknown reason, which the
// report still shows but can no longer offer a fix for. A table that fails both
// checks gets one row whose reason joins the two with ExclusionReasonSeparator,
// not-InnoDB first.
const (
	ExclusionReasonNotInnoDB    = "not InnoDB"
	ExclusionReasonNoPrimaryKey = "no primary key"
	ExclusionReasonSeparator    = "; "
)

// snapshotExclusion is one table takeSnapshot's degrade branch left out of the
// snapshot, carried separately from the "schema.table" display strings so the
// insert never has to re-split a concatenated key.
type snapshotExclusion struct {
	schema, table, reason string
	// pkColumn is the free column name a PRIMARY KEY would take on this
	// table (#1802); empty when the table already has one and only its
	// engine is wrong.
	pkColumn string
}

// ensureSnapshotExclusionsTable lazily creates snapshot_exclusions on an index
// that predates it — the ensureSnapshotIDSeqTable pattern: indexer.EnsureSchema
// provisions it eagerly at daemon startup, but the writer must self-heal for
// paths that never run EnsureSchema.
//
// It reports whether pk_column (#1802) is there to write, and adding it is
// BEST EFFORT: the column feeds a report, and a snapshot must never fail over
// one. A false return means the insert leaves it out, and the reader then
// prints no statement instead of a guessed one.
func ensureSnapshotExclusionsTable(ctx context.Context, db *sql.DB) (hasPKColumn bool, err error) {
	var exists bool
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions'",
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("metadata: check snapshot_exclusions table: %w", err)
	}
	if !exists {
		if _, err := db.ExecContext(ctx, DDLSnapshotExclusions); err != nil {
			return false, fmt.Errorf("metadata: create snapshot_exclusions: %w", err)
		}
		return true, nil
	}
	MigrateSnapshotExclusionsNamesBestEffort(ctx, db)
	return ensureSnapshotExclusionsPKColumn(ctx, db), nil
}

// SnapshotExclusionsNameCollation is the collation of snapshot_exclusions'
// schema_name and table_name (#1815): binary, so two names the source keeps
// apart are two keys here.
//
// utf8mb4_bin is PAD SPACE, so it would compare 'x' and 'x ' as equal. That
// cannot bite: MySQL refuses a schema or table name ending in a space (ERROR
// 1102/1103, pinned by TestIntegrationServerRefusesTrailingSpaceTableNames_1815).
// It is chosen over the NO PAD utf8mb4_0900_bin because it exists on every
// MySQL 8.0 the index supports, not only 8.0.17 and later.
const SnapshotExclusionsNameCollation = "utf8mb4_bin"

// migrationLockWait bounds how long the writer's collation migration waits
// for the table's metadata lock. The ALTER rebuilds the table and needs an
// exclusive lock, which queues behind any open transaction that read the
// table — and every later reader queues behind the ALTER. The server default
// wait is a year; on the stream's DDL hook that would be capture hanging.
const migrationLockWait = 5

// execQuerier is what the migration needs from a connection: *sql.DB and
// *sql.Conn both satisfy it.
type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// EnsureSnapshotExclusionsNameCollation converts an existing
// snapshot_exclusions table, created by a build before #1815, to binary name
// columns. Idempotent: it reads information_schema first and issues no ALTER
// when both columns already carry SnapshotExclusionsNameCollation. A missing
// table is not an error (the creator uses the new DDL).
//
// The rows already there always survive: the old key was insensitive, so no
// two rows in it can be equal under a binary one.
//
// The check runs on the pool. Only when an ALTER is due does it take a
// dedicated connection, bound lock_wait_timeout to migrationLockWait seconds
// there, and then DISCARD that connection instead of returning it: the
// session setting would otherwise ride back into the pool and make some
// later, unrelated statement (rotation DDL, the snapshot transaction, a batch
// INSERT) give up on a lock after a few seconds.
func EnsureSnapshotExclusionsNameCollation(ctx context.Context, db *sql.DB) error {
	due, err := snapshotExclusionsNamesNeedMigration(ctx, db)
	if err != nil || !due {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("metadata: snapshot_exclusions collation: %w", err)
	}
	defer func() {
		// A Raw callback that returns ErrBadConn makes database/sql close the
		// connection rather than pool it. Close afterwards is then a no-op.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
	}()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", migrationLockWait)); err != nil {
		return fmt.Errorf("metadata: snapshot_exclusions collation: bound lock wait: %w", err)
	}
	return ensureSnapshotExclusionsNameCollationOn(ctx, conn)
}

// snapshotExclusionsNamesNeedMigration reports whether snapshot_exclusions
// exists with at least one name column not yet binary. A NULL collation
// counts as not binary, which errs toward converting.
func snapshotExclusionsNamesNeedMigration(ctx context.Context, q execQuerier) (bool, error) {
	var found, binary int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(COLLATION_NAME = ?), 0)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions'
		  AND COLUMN_NAME IN ('schema_name', 'table_name')`, SnapshotExclusionsNameCollation,
	).Scan(&found, &binary); err != nil {
		return false, fmt.Errorf("metadata: check snapshot_exclusions name collation: %w", err)
	}
	return found > 0 && binary != found, nil
}

// ensureSnapshotExclusionsNameCollationOn re-checks and ALTERs on the given
// connection; the caller owns its session settings.
func ensureSnapshotExclusionsNameCollationOn(ctx context.Context, c execQuerier) error {
	due, err := snapshotExclusionsNamesNeedMigration(ctx, c)
	if err != nil || !due {
		return err
	}
	if _, err := c.ExecContext(ctx, `ALTER TABLE snapshot_exclusions
		MODIFY COLUMN schema_name VARCHAR(64) COLLATE `+SnapshotExclusionsNameCollation+` NOT NULL,
		MODIFY COLUMN table_name  VARCHAR(64) COLLATE `+SnapshotExclusionsNameCollation+` NOT NULL`); err != nil {
		return fmt.Errorf("metadata: convert snapshot_exclusions names to %s: %w", SnapshotExclusionsNameCollation, err)
	}
	return nil
}

// MigrateSnapshotExclusionsNamesBestEffort runs the conversion and logs a
// warning instead of failing. Both callers want that:
//   - the snapshot writer, for paths that never run EnsureSchema (`bintrail
//     snapshot`): failing the snapshot over it would fail every degraded
//     snapshot, while leaving it unconverted only fails the rare one that
//     excludes two case/accent twins, the bug as it was before;
//   - EnsureSchema, which the READ plane also runs with SELECT-only DSNs
//     that cannot ALTER, and which must not refuse a read over a table the
//     read plane never writes, nor refuse to start over a transient lock.
//
// The writer retries it before every degraded snapshot, so a missed run heals
// on the next one.
func MigrateSnapshotExclusionsNamesBestEffort(ctx context.Context, db *sql.DB) {
	if err := EnsureSnapshotExclusionsNameCollation(ctx, db); err != nil {
		slog.Warn("could not convert snapshot_exclusions names to a binary collation; a snapshot that excludes two tables whose names differ only in case or accent will fail until it is converted (the next degraded snapshot retries it)",
			"error", err)
	}
}

// ensureSnapshotExclusionsPKColumn adds pk_column to a snapshot_exclusions
// table written before it existed. Best effort, and loud in the log: without
// it the card names the table and cannot hand over a statement for it.
func ensureSnapshotExclusionsPKColumn(ctx context.Context, db *sql.DB) bool {
	var present bool
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) > 0 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions' AND COLUMN_NAME = 'pk_column'",
	).Scan(&present); err != nil {
		slog.Warn("could not check snapshot_exclusions.pk_column; the report will name each uncaptured table without the statement that fixes it", "error", err)
		return false
	}
	if present {
		return true
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE snapshot_exclusions ADD COLUMN pk_column VARCHAR(64) DEFAULT NULL"); err != nil {
		slog.Warn("could not add snapshot_exclusions.pk_column; the report will name each uncaptured table without the statement that fixes it", "error", err)
		return false
	}
	return true
}

// SuggestPKColumn picks a column name a table does not already use, for the
// PRIMARY KEY it would take to capture it. Only the snapshot can do this: an
// excluded table has no columns in the snapshot, so no reader of the index
// can see what is already there.
//
// `id` unless the table has one — MySQL refuses a duplicate column name with
// ERROR 1060, and a plain non-key `id` column is the commonest shape of a
// key-less table, so the obvious guess is exactly the one that fails. Names
// compare case-insensitively, as MySQL compares them.
//
// strings.ToLower here is deliberate. The invariant this function needs is
// one-directional: the "taken" key must fold AT LEAST as much as the server
// does. Folding more than the server costs a less pretty name and nothing
// else; folding LESS offers a name the server already has, and the operator
// gets a statement that dies with ERROR 1060 — the exact failure this card
// exists to remove. strings.ToLower satisfies it, including for the Turkish
// dotted İ, which it maps to i exactly as the server does.
//
// So do not reach for IdentEqualFold above, or for strings.EqualFold, without
// re-checking that invariant: EqualFold folds NEITHER Turkish letter, so on a
// table whose only key-ish column is `İd` it would offer `id` and reproduce
// the 1060. Both answers, and the server's own verdict on each, are pinned by
// TestIntegrationSuggestPKColumnNameIsAccepted: `İd` gets `dbtrail_id`
// because the server really does refuse `id` beside it, and `ıd` gets `id`
// because the server really does accept it.
func SuggestPKColumn(existing []string) string {
	taken := make(map[string]bool, len(existing))
	for _, c := range existing {
		taken[strings.ToLower(strings.TrimSpace(c))] = true
	}
	if !taken["id"] {
		return "id"
	}
	if !taken["dbtrail_id"] {
		return "dbtrail_id"
	}
	for n := 2; ; n++ {
		if candidate := fmt.Sprintf("dbtrail_id_%d", n); !taken[candidate] {
			return candidate
		}
	}
}

// loadSnapshotExclusions returns the "schema.table" → reason map a degraded
// snapshot (#1051) recorded for snapshotID. A missing snapshot_exclusions
// table is NOT an error (see the DDLSnapshotExclusions doc: an index that only
// ever took strict snapshots simply has no exclusions) — nil map, nil error.
func loadSnapshotExclusions(db *sql.DB, snapshotID int) (map[string]string, error) {
	var exists bool
	if err := db.QueryRow(
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions'",
	).Scan(&exists); err != nil {
		return nil, fmt.Errorf("metadata: check snapshot_exclusions table: %w", err)
	}
	if !exists {
		return nil, nil
	}
	rows, err := db.Query(
		"SELECT schema_name, table_name, reason FROM snapshot_exclusions WHERE snapshot_id = ?",
		snapshotID)
	if err != nil {
		return nil, fmt.Errorf("metadata: query snapshot_exclusions for snapshot %d: %w", snapshotID, err)
	}
	defer rows.Close()

	var excluded map[string]string
	for rows.Next() {
		var schemaName, tableName, reason string
		if err := rows.Scan(&schemaName, &tableName, &reason); err != nil {
			return nil, fmt.Errorf("metadata: scan snapshot_exclusions row: %w", err)
		}
		if excluded == nil {
			excluded = make(map[string]string)
		}
		excluded[schemaName+"."+tableName] = reason
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate snapshot_exclusions: %w", err)
	}
	return excluded, nil
}

// IdentEqualFold reports whether two column identifiers name the same column
// for the purposes of this repo's comparisons. It is strings.EqualFold plus a
// fold of the Turkish dotted İ (U+0130) and dotless ı (U+0131) onto 'i'.
//
// Measured on MySQL 8.4.9, default utf8mb4_0900_ai_ci,
// lower_case_table_names=0, each case a CREATE TABLE carrying both names as
// columns, through the same driver the product uses — pinned by
// TestIntegrationMySQLFoldsOnlyASCIICaseInColumnIdentifiers:
//
//	i vs I    ERROR 1060: one identifier
//	i vs İ    ERROR 1060: one identifier  (so the dotted I DOES fold)
//	I vs İ    ERROR 1060: one identifier
//	i vs ı    accepted: two distinct columns
//	I vs ı    accepted: two distinct columns
//	e vs é    accepted: two distinct columns
//
// So the dotted İ folds and Unicode simple folding does not know it, which is
// why strings.EqualFold alone is not enough: it would read the legal
// case-style rename İstanbul -> istanbul as schema drift (#700).
//
// The dotless ı is the other half, and it is NOT verified as folding — the
// server above keeps it apart. This helper folds it anyway, wider than the
// server, on purpose: both callers are safe in that direction and only that
// direction. The drift check (#700) folding more means warning less, never
// reporting drift that is not there; the uncaptured-tables list (#1802)
// folding more shows an extra row, while folding less HIDES a table from a
// list whose whole job is to hide nothing. Narrowing it to match the server
// exactly is therefore a behaviour change to the drift check, not a cleanup.
//
// Accents are not folded by identifiers either. Note this is NOT the data
// collation, which is a different rule with different answers: under
// utf8mb4_0900_ai_ci the string comparison 'e' = 'é' is TRUE (it is
// accent-INsensitive) while the two column names above stay distinct.
//
// SuggestPKColumn below deliberately does NOT use this; see its own note.
func IdentEqualFold(a, b string) bool {
	if strings.EqualFold(a, b) {
		return true
	}
	dotless := func(r rune) rune {
		if r == 'İ' || r == 'ı' {
			return 'i'
		}
		return r
	}
	return strings.EqualFold(strings.Map(dotless, a), strings.Map(dotless, b))
}
