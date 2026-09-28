package metadata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
)

// FKConstraintsNameCollation is the collation of fk_constraints.schema_name
// (#1839): binary, so two schema names the source keeps apart are two keys
// here.
//
// On the index database's default collation (utf8mb4_0900_ai_ci on 8.x) two
// schemas whose names differ only in case or accent (`shop` and `Shop`,
// `cafe` and `café`, both ordinary on a case-sensitive source) were one key.
// With a foreign key of the same name in each, the second row's ERROR 1062
// rolled back the WHOLE snapshot transaction.
//
// schema_name is the only column that needs it. Inside one schema the server
// itself refuses two constraint names that differ only in case or accent
// (ERROR 1826, pinned by TestIntegrationServerRefusesAccentTwinFKNames_1839),
// so constraint_name cannot collide once schema_name is exact. table_name and
// the referenced_* columns are not in the key and keep the default collation.
//
// utf8mb4_bin is PAD SPACE; SnapshotExclusionsNameCollation explains why that
// cannot bite for identifiers and why it is preferred over utf8mb4_0900_bin.
const FKConstraintsNameCollation = "utf8mb4_bin"

// fkSchemaNameFolded is the fk_constraints.schema_name expression the readers
// filter on when they match a schema name the user typed. The column stores
// the exact name; the lookup keeps the insensitive match every build before
// #1839 gave it, so `--schema Shop` still finds a schema stored as `shop`.
//
// With twins stored (`shop` and `Shop`) a typed name matches both. That is on
// purpose: the delete events the cascade path reads come from binlog_events,
// which matches the typed name the same insensitive way, so both twins' deletes
// arrive and both twins' edges are needed. The synthesis keys edges by the
// exact schema of each event, so an extra edge is never used for the wrong
// schema.
//
// CONVERT(... USING utf8mb4) first, because the conversion is best effort: on
// an index a read-only DSN never converted, and whose default charset is not
// utf8mb4, a bare COLLATE utf8mb4_0900_ai_ci on the column is ERROR 1253.
const fkSchemaNameFolded = "CONVERT(schema_name USING utf8mb4) COLLATE utf8mb4_0900_ai_ci"

// EnsureFKConstraintsNameCollation converts an existing fk_constraints table,
// created by a build before #1839, to a binary schema_name. Idempotent: it
// reads information_schema first and issues no ALTER when the column already
// carries FKConstraintsNameCollation. A missing table is not an error
// (an index that predates fk_constraints skips FK capture).
//
// The rows already there always survive: the old key was insensitive, so no
// two rows in it can be equal under a binary one.
//
// Only an ALTER that is due takes a dedicated connection, with lock_wait_timeout
// bound to migrationLockWait there, and that connection is DISCARDED rather
// than pooled (EnsureSnapshotExclusionsNameCollation has the reasoning).
func EnsureFKConstraintsNameCollation(ctx context.Context, db *sql.DB) error {
	due, err := fkConstraintsNamesNeedMigration(ctx, db)
	if err != nil || !due {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("metadata: fk_constraints collation: %w", err)
	}
	defer func() {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
	}()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", migrationLockWait)); err != nil {
		return fmt.Errorf("metadata: fk_constraints collation: bound lock wait: %w", err)
	}
	return ensureFKConstraintsNameCollationOn(ctx, conn)
}

// fkConstraintsNamesNeedMigration reports whether fk_constraints exists with
// schema_name not yet binary. A NULL collation counts as not binary, which
// errs toward converting.
func fkConstraintsNamesNeedMigration(ctx context.Context, q execQuerier) (bool, error) {
	var found, binary int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(COLLATION_NAME = ?), 0)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'fk_constraints'
		  AND COLUMN_NAME = 'schema_name'`, FKConstraintsNameCollation,
	).Scan(&found, &binary); err != nil {
		return false, fmt.Errorf("metadata: check fk_constraints name collation: %w", err)
	}
	return found > 0 && binary != found, nil
}

// ensureFKConstraintsNameCollationOn re-checks and ALTERs on the given
// connection; the caller owns its session settings.
func ensureFKConstraintsNameCollationOn(ctx context.Context, c execQuerier) error {
	due, err := fkConstraintsNamesNeedMigration(ctx, c)
	if err != nil || !due {
		return err
	}
	if _, err := c.ExecContext(ctx, `ALTER TABLE fk_constraints
		MODIFY COLUMN schema_name VARCHAR(64) COLLATE `+FKConstraintsNameCollation+` NOT NULL`); err != nil {
		return fmt.Errorf("metadata: convert fk_constraints.schema_name to %s: %w", FKConstraintsNameCollation, err)
	}
	return nil
}

// MigrateFKConstraintsNamesBestEffort runs the conversion and logs a warning
// instead of failing, for the same two callers and reasons as
// MigrateSnapshotExclusionsNamesBestEffort: the snapshot writer (failing it
// here would fail every snapshot with an FK, while leaving the table
// unconverted only fails the rare one over twin schemas) and EnsureSchema
// (which the read plane runs with SELECT-only DSNs). The writer retries it
// before every snapshot that captures FKs, so a missed run heals on the next.
func MigrateFKConstraintsNamesBestEffort(ctx context.Context, db *sql.DB) {
	if err := EnsureFKConstraintsNameCollation(ctx, db); err != nil {
		slog.Warn("could not convert fk_constraints.schema_name to a binary collation; a snapshot of two schemas whose names differ only in case or accent, with a foreign key of the same name in each, will fail until it is converted (the next snapshot retries it)",
			"error", err)
	}
}
