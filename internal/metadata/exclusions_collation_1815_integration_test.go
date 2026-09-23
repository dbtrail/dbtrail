//go:build integration

package metadata

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// legacyDDLSnapshotExclusions is snapshot_exclusions as every build before
// #1815 created it: the name columns carry no collation of their own, so they
// take the index database's default, which on MySQL 8.x is case- and
// accent-INsensitive.
const legacyDDLSnapshotExclusions = `CREATE TABLE snapshot_exclusions (
    snapshot_id INT UNSIGNED NOT NULL,
    schema_name VARCHAR(64)  NOT NULL,
    table_name  VARCHAR(64)  NOT NULL,
    reason      VARCHAR(64)  NOT NULL,
    pk_column   VARCHAR(64)  DEFAULT NULL,
    PRIMARY KEY (snapshot_id, schema_name, table_name)
) ENGINE=InnoDB`

// sourceFoldsTableNameCase reports whether the source server would refuse two
// tables whose names differ only in case (lower_case_table_names 1 or 2). On
// such a server the case pair cannot exist, so the tests only build it on 0,
// the Linux default; the accent pair exists under every setting.
func sourceFoldsTableNameCase(t *testing.T, db *sql.DB) bool {
	t.Helper()
	var lctn int
	if err := db.QueryRow("SELECT @@lower_case_table_names").Scan(&lctn); err != nil {
		t.Fatalf("read lower_case_table_names: %v", err)
	}
	return lctn != 0
}

// createCollidingExcludedTables creates, in the source, key-less tables whose
// names one case- and accent-insensitive key would merge, and returns their
// names sorted.
func createCollidingExcludedTables(t *testing.T, sourceDB *sql.DB) []string {
	t.Helper()
	names := []string{"cafe", "café"}
	if !sourceFoldsTableNameCase(t, sourceDB) {
		names = append(names, "Audit_Log", "audit_log")
	}
	for _, n := range names {
		testutil.MustExec(t, sourceDB, "CREATE TABLE `"+n+"` (v INT) ENGINE=InnoDB")
	}
	testutil.MustExec(t, sourceDB, `CREATE TABLE ok_tbl (id INT PRIMARY KEY) ENGINE=InnoDB`)
	sort.Strings(names)
	return names
}

func recordedExclusions(t *testing.T, indexDB *sql.DB, snapshotID int) []string {
	t.Helper()
	rows, err := indexDB.Query(
		"SELECT table_name FROM snapshot_exclusions WHERE snapshot_id = ? ORDER BY table_name", snapshotID)
	if err != nil {
		t.Fatalf("read snapshot_exclusions: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	sort.Strings(got)
	return got
}

func assertNameColumnsAreBinary(t *testing.T, indexDB *sql.DB) {
	t.Helper()
	for _, col := range []string{"schema_name", "table_name"} {
		var collation, nullable string
		if err := indexDB.QueryRow(`SELECT COLLATION_NAME, IS_NULLABLE FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions' AND COLUMN_NAME = ?`, col,
		).Scan(&collation, &nullable); err != nil {
			t.Fatalf("read %s collation: %v", col, err)
		}
		if collation != SnapshotExclusionsNameCollation {
			t.Errorf("snapshot_exclusions.%s collation = %s, want %s", col, collation, SnapshotExclusionsNameCollation)
		}
		if nullable != "NO" {
			t.Errorf("snapshot_exclusions.%s must stay NOT NULL, IS_NULLABLE = %s", col, nullable)
		}
	}
}

// #1815: two tables whose names differ only in case or in an accent are two
// tables on the source, and the degraded snapshot must record both — not
// fail the whole schema read with ERROR 1062 inside its transaction.
func TestIntegrationExclusionsKeepCaseAndAccentPairsApart_1815(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	want := createCollidingExcludedTables(t, sourceDB)

	stats, err := TakeSnapshotExcludingInvalid(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshotExcludingInvalid: %v", err)
	}
	if len(stats.ExcludedTables) != len(want) {
		t.Errorf("ExcludedTables = %v, want %d entries", stats.ExcludedTables, len(want))
	}
	got := recordedExclusions(t, indexDB, stats.SnapshotID)
	if len(got) != len(want) {
		t.Fatalf("recorded exclusions = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("recorded exclusions = %q, want %q (names must come back byte for byte)", got, want)
		}
	}

	// The resolver's map must name each one, too.
	r, err := NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	for _, n := range want {
		if _, ok := r.ExclusionReason(sourceName, n); !ok {
			t.Errorf("ExclusionReason(%s.%s) = false, want excluded", sourceName, n)
		}
	}
	assertNameColumnsAreBinary(t, indexDB)
}

// An index created by an older build keeps the insensitive table until
// something migrates it. The snapshot writer self-heals it (the standalone
// `bintrail snapshot` never runs EnsureSchema), and the rows already there
// survive the change: making a key stricter can never merge two of them.
func TestIntegrationExclusionsWriterMigratesALegacyTable_1815(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	testutil.MustExec(t, indexDB, legacyDDLSnapshotExclusions)
	testutil.MustExec(t, indexDB,
		`INSERT INTO snapshot_exclusions VALUES (9999, 'old_schema', 'Old_Tbl', 'no primary key', 'id')`)

	// The premise: the legacy table really does merge the pair.
	testutil.MustExec(t, indexDB,
		`INSERT INTO snapshot_exclusions VALUES (9998, 's', 'Audit_Log', 'no primary key', NULL)`)
	_, err := indexDB.Exec(`INSERT INTO snapshot_exclusions VALUES (9998, 's', 'àudit_log', 'no primary key', NULL)`)
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1062 {
		t.Fatalf("legacy table must refuse the accent twin with 1062 (the bug's premise), got %v", err)
	}

	want := createCollidingExcludedTables(t, sourceDB)
	stats, err := TakeSnapshotExcludingInvalid(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshotExcludingInvalid on a legacy index: %v", err)
	}
	if got := recordedExclusions(t, indexDB, stats.SnapshotID); len(got) != len(want) {
		t.Fatalf("recorded exclusions = %q, want %q", got, want)
	}
	assertNameColumnsAreBinary(t, indexDB)

	var reason, pk string
	if err := indexDB.QueryRow(
		`SELECT reason, pk_column FROM snapshot_exclusions WHERE snapshot_id = 9999 AND table_name = 'Old_Tbl'`,
	).Scan(&reason, &pk); err != nil || reason != "no primary key" || pk != "id" {
		t.Fatalf("pre-existing row after migration = (%q, %q, %v), want it untouched", reason, pk, err)
	}
}

// The migration is idempotent and quiet: a second run on a migrated table
// issues no ALTER. Proved by holding a metadata lock that any ALTER would
// wait on: the call must return well inside the lock wait.
func TestIntegrationExclusionsCollationMigrationIsIdempotent_1815(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	ctx := context.Background()
	testutil.MustExec(t, indexDB, legacyDDLSnapshotExclusions)

	if err := EnsureSnapshotExclusionsNameCollation(ctx, indexDB); err != nil {
		t.Fatalf("first migration: %v", err)
	}
	assertNameColumnsAreBinary(t, indexDB)

	// Hold a shared metadata lock from another session; an ALTER would block
	// on it until lock_wait_timeout, which we set to 1s on the migrating
	// session so a regression fails fast instead of hanging.
	holder, err := indexDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if _, err := holder.ExecContext(ctx, "START TRANSACTION"); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(ctx, "SELECT COUNT(*) FROM snapshot_exclusions"); err != nil {
		t.Fatal(err)
	}
	defer holder.ExecContext(ctx, "ROLLBACK") //nolint:errcheck

	migrator, err := indexDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer migrator.Close()
	if _, err := migrator.ExecContext(ctx, "SET SESSION lock_wait_timeout = 1"); err != nil {
		t.Fatal(err)
	}
	if err := ensureSnapshotExclusionsNameCollationOn(ctx, migrator); err != nil {
		t.Fatalf("second migration on an already-binary table must be a no-op, got %v", err)
	}
}

// No table yet is not an error: the writer and EnsureSchema create it with
// the binary collation already in place.
func TestIntegrationExclusionsCollationMigrationToleratesMissingTable_1815(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	if err := EnsureSnapshotExclusionsNameCollation(context.Background(), indexDB); err != nil {
		t.Fatalf("missing table must be a no-op, got %v", err)
	}
}

// PAD SPACE: utf8mb4_bin compares 'x' and 'x ' as equal, so a trailing-space
// twin WOULD collide. It cannot reach this table because the server refuses
// such a name outright; this pins that premise so the choice of utf8mb4_bin
// over the NO PAD utf8mb4_0900_bin (absent before 8.0.17) is re-examined if a
// server ever accepts one.
func TestIntegrationServerRefusesTrailingSpaceTableNames_1815(t *testing.T) {
	sourceDB, _ := testutil.CreateTestDB(t)
	_, err := sourceDB.Exec("CREATE TABLE `trailing ` (v INT)")
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1103 {
		t.Fatalf("CREATE TABLE with a trailing space = %v, want ERROR 1103", err)
	}
	_, err = sourceDB.Exec("CREATE DATABASE `trailing_db `")
	if !errors.As(err, &me) || me.Number != 1102 {
		t.Fatalf("CREATE DATABASE with a trailing space = %v, want ERROR 1102", err)
	}
}
