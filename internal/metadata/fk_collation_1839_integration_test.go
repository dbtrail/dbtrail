//go:build integration

package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// legacyDDLFKConstraints is fk_constraints as every build before #1839
// created it: the name columns carry no collation of their own, so they take
// the index database's default, which on MySQL 8.x is case- and
// accent-INsensitive.
const legacyDDLFKConstraints = `CREATE TABLE fk_constraints (
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
) ENGINE=InnoDB`

var twinSchemaCounter atomic.Int64

// createTwinFKSchemas creates, on the source server, schemas whose names one
// case- and accent-insensitive key would merge, each holding a parent and a
// child joined by an ON DELETE CASCADE foreign key named fk_x. The case pair
// only exists where the server keeps schema names apart by case
// (lower_case_table_names = 0, the Linux default); the accent pair exists
// under every setting. Returns the names sorted.
func createTwinFKSchemas(t *testing.T, sourceDB *sql.DB) []string {
	t.Helper()
	sfx := fmt.Sprintf("%d_%d", time.Now().UnixNano()%1_000_000, twinSchemaCounter.Add(1))
	names := []string{"cafe1839_" + sfx, "café1839_" + sfx}
	if !sourceFoldsTableNameCase(t, sourceDB) {
		names = append(names, "shop1839_"+sfx, "Shop1839_"+sfx)
	}
	root, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, n := range names {
			root.Exec("DROP DATABASE IF EXISTS `" + n + "`") //nolint:errcheck
		}
		root.Close()
	})
	for _, n := range names {
		testutil.MustExec(t, root, "CREATE DATABASE `"+n+"`")
		testutil.MustExec(t, root, "CREATE TABLE `"+n+"`.parent (id INT PRIMARY KEY) ENGINE=InnoDB")
		testutil.MustExec(t, root, "CREATE TABLE `"+n+"`.child (id INT PRIMARY KEY, pid INT, "+
			"CONSTRAINT fk_x FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE) ENGINE=InnoDB")
	}
	sort.Strings(names)
	return names
}

// recordedFKSchemas returns the schema_name of every fk_constraints row of
// the snapshot, sorted, byte for byte as stored.
func recordedFKSchemas(t *testing.T, indexDB *sql.DB, snapshotID int) []string {
	t.Helper()
	rows, err := indexDB.Query(
		"SELECT schema_name, constraint_name FROM fk_constraints WHERE snapshot_id = ?", snapshotID)
	if err != nil {
		t.Fatalf("read fk_constraints: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var s, c string
		if err := rows.Scan(&s, &c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if c != "fk_x" {
			t.Errorf("constraint_name = %q, want fk_x", c)
		}
		got = append(got, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	sort.Strings(got)
	return got
}

func assertFKKeyColumnsAreBinary(t *testing.T, indexDB *sql.DB) {
	t.Helper()
	for _, col := range []string{"schema_name"} {
		var collation, nullable string
		if err := indexDB.QueryRow(`SELECT COLLATION_NAME, IS_NULLABLE FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'fk_constraints' AND COLUMN_NAME = ?`, col,
		).Scan(&collation, &nullable); err != nil {
			t.Fatalf("read %s collation: %v", col, err)
		}
		if collation != FKConstraintsNameCollation {
			t.Errorf("fk_constraints.%s collation = %s, want %s", col, collation, FKConstraintsNameCollation)
		}
		if nullable != "NO" {
			t.Errorf("fk_constraints.%s must stay NOT NULL, IS_NULLABLE = %s", col, nullable)
		}
	}
}

// requireFKConversionDue pins the premise of a migration test: the legacy
// table really is not binary yet, so the migration has an ALTER to run. On a
// server whose default collation were already binary, the tests after it
// would pass without converting anything.
func requireFKConversionDue(t *testing.T, indexDB *sql.DB) {
	t.Helper()
	due, err := fkConstraintsNamesNeedMigration(context.Background(), indexDB)
	if err != nil || !due {
		t.Fatalf("legacy fk_constraints must need the conversion (the test's premise), got due=%v err=%v", due, err)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// #1839: two schemas whose names differ only in case or accent, each with a
// foreign key of the same name, are two schemas on the source, and the
// snapshot must record both FKs instead of failing with ERROR 1062.
func TestIntegrationFKTwinSchemasBothCaptured_1839(t *testing.T) {
	sourceDB, _ := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	want := createTwinFKSchemas(t, sourceDB)

	stats, err := TakeSnapshot(sourceDB, indexDB, want)
	if err != nil {
		t.Fatalf("TakeSnapshot over twin schemas: %v", err)
	}
	if stats.FKCount != len(want) {
		t.Errorf("FKCount = %d, want %d", stats.FKCount, len(want))
	}
	if got := recordedFKSchemas(t, indexDB, stats.SnapshotID); !equalStrings(got, want) {
		t.Fatalf("recorded FK schemas = %q, want %q (names must come back byte for byte)", got, want)
	}
	assertFKKeyColumnsAreBinary(t, indexDB)
}

// A schema listed twice is still one schema: the source query's IN collapses
// it, so no row is written twice (which would be a 1062 under any collation).
func TestIntegrationFKSameSchemaListedTwice_1839(t *testing.T) {
	sourceDB, _ := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	names := createTwinFKSchemas(t, sourceDB)

	stats, err := TakeSnapshot(sourceDB, indexDB, []string{names[0], names[0]})
	if err != nil {
		t.Fatalf("TakeSnapshot with a repeated schema: %v", err)
	}
	if got := recordedFKSchemas(t, indexDB, stats.SnapshotID); !equalStrings(got, []string{names[0]}) {
		t.Fatalf("recorded FK schemas = %q, want exactly %q once", got, names[0])
	}
}

// Within ONE schema the server refuses two constraint names that differ only
// in case or accent. That is why schema_name alone is made binary: once the
// schema is exact, constraint_name cannot collide. This pins the premise, so
// the choice is re-examined if a server ever accepts the pair.
func TestIntegrationServerRefusesAccentTwinFKNames_1839(t *testing.T) {
	sourceDB, _ := testutil.CreateTestDB(t)
	testutil.MustExec(t, sourceDB, "CREATE TABLE parent (id INT PRIMARY KEY) ENGINE=InnoDB")
	testutil.MustExec(t, sourceDB, "CREATE TABLE child_a (id INT PRIMARY KEY, pid INT, "+
		"CONSTRAINT fk_cafe FOREIGN KEY (pid) REFERENCES parent (id)) ENGINE=InnoDB")
	for _, twin := range []string{"fk_café", "FK_CAFE"} {
		_, err := sourceDB.Exec("CREATE TABLE child_b (id INT PRIMARY KEY, pid INT, " +
			"CONSTRAINT `" + twin + "` FOREIGN KEY (pid) REFERENCES parent (id)) ENGINE=InnoDB")
		var me *mysql.MySQLError
		if !errors.As(err, &me) || me.Number != 1826 {
			t.Fatalf("a second FK named %q beside fk_cafe = %v, want ERROR 1826", twin, err)
		}
	}
}

// An index created by an older build keeps the insensitive table until
// something migrates it. The snapshot writer self-heals it (the standalone
// `bintrail snapshot` never runs EnsureSchema), and the rows already there
// survive: making a key stricter can never merge two of them.
func TestIntegrationFKWriterMigratesALegacyTable_1839(t *testing.T) {
	sourceDB, _ := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	testutil.MustExec(t, indexDB, "DROP TABLE fk_constraints")
	testutil.MustExec(t, indexDB, legacyDDLFKConstraints)
	testutil.MustExec(t, indexDB, `INSERT INTO fk_constraints VALUES
		(9999, 'fk_old', 'old_schema', 'child', 'pid', 1, 'old_schema', 'parent', 'id', 'CASCADE', 'RESTRICT')`)

	// The premise: the legacy table really does merge the pair.
	testutil.MustExec(t, indexDB, `INSERT INTO fk_constraints VALUES
		(9998, 'fk_x', 'cafe', 'child', 'pid', 1, 'cafe', 'parent', 'id', '', '')`)
	_, err := indexDB.Exec(`INSERT INTO fk_constraints VALUES
		(9998, 'fk_x', 'CAFÉ', 'child', 'pid', 1, 'CAFÉ', 'parent', 'id', '', '')`)
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1062 {
		t.Fatalf("legacy table must refuse the twin with 1062 (the bug's premise), got %v", err)
	}

	want := createTwinFKSchemas(t, sourceDB)
	stats, err := TakeSnapshot(sourceDB, indexDB, want)
	if err != nil {
		t.Fatalf("TakeSnapshot on a legacy index: %v", err)
	}
	if got := recordedFKSchemas(t, indexDB, stats.SnapshotID); !equalStrings(got, want) {
		t.Fatalf("recorded FK schemas = %q, want %q", got, want)
	}
	assertFKKeyColumnsAreBinary(t, indexDB)

	var rule string
	if err := indexDB.QueryRow(
		`SELECT delete_rule FROM fk_constraints WHERE snapshot_id = 9999 AND constraint_name = 'fk_old'`,
	).Scan(&rule); err != nil || rule != "CASCADE" {
		t.Fatalf("pre-existing row after migration = (%q, %v), want it untouched", rule, err)
	}
}

// The name the user types still finds the schema as stored, whatever its case
// or accents: storage is exact, the lookup is not. With twins stored, a typed
// name finds every twin, because the delete events the cascade path reads come
// from binlog_events, which matches the typed name the same insensitive way.
// Loading an edge nobody deleted from is harmless (the synthesis keys edges by
// the exact schema of each event); missing one is silent under-recovery.
func TestIntegrationFKLookupResolvesTypedSchemaName_1839(t *testing.T) {
	sourceDB, _ := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	names := createTwinFKSchemas(t, sourceDB)
	if _, err := TakeSnapshot(sourceDB, indexDB, names); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// "CAFE1839_<sfx>" differs from both stored accent twins in case, and from
	// one of them in an accent too.
	var accentPair []string
	for _, n := range names {
		if strings.HasPrefix(strings.ToLower(n), "caf") {
			accentPair = append(accentPair, n)
		}
	}
	typed := strings.ToUpper(strings.Replace(accentPair[0], "é", "e", 1))
	edges, err := CascadeConstraintsInIndex(indexDB, []string{typed})
	if err != nil {
		t.Fatalf("CascadeConstraintsInIndex(%q): %v", typed, err)
	}
	var got []string
	for _, e := range edges {
		got = append(got, e.Schema)
	}
	sort.Strings(got)
	if !equalStrings(got, accentPair) {
		t.Fatalf("CascadeConstraintsInIndex(%q) schemas = %q, want the stored twins %q", typed, got, accentPair)
	}
}

// The folded lookup must also run against a table no migration has touched:
// the conversion is best effort (a read-only DSN cannot ALTER), and a read
// that worked before this change must not start failing because of it.
func TestIntegrationFKLookupWorksOnAnUnconvertedTable_1839(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	testutil.MustExec(t, indexDB, "DROP TABLE fk_constraints")
	// latin1, as on an index whose default charset is not utf8mb4: a bare
	// COLLATE utf8mb4_0900_ai_ci on this column is ERROR 1253.
	testutil.MustExec(t, indexDB, legacyDDLFKConstraints+" DEFAULT CHARSET=latin1")
	testutil.MustExec(t, indexDB, `INSERT INTO fk_constraints VALUES
		(1, 'fk_x', 'Shop', 'child', 'pid', 1, 'Shop', 'parent', 'id', 'CASCADE', 'RESTRICT')`)

	edges, err := CascadeConstraintsInIndex(indexDB, []string{"shop"})
	if err != nil {
		t.Fatalf("CascadeConstraintsInIndex on a legacy table: %v", err)
	}
	if len(edges) != 1 || edges[0].Schema != "Shop" {
		t.Fatalf("edges = %+v, want the one row stored as Shop", edges)
	}
}

// The migration is idempotent and quiet: a second run on a migrated table
// issues no ALTER. Proved by holding a metadata lock that any ALTER would
// wait on: the call must return well inside the lock wait.
func TestIntegrationFKCollationMigrationIsIdempotent_1839(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	ctx := context.Background()
	testutil.MustExec(t, indexDB, legacyDDLFKConstraints)
	requireFKConversionDue(t, indexDB)

	if err := EnsureFKConstraintsNameCollation(ctx, indexDB); err != nil {
		t.Fatalf("first migration: %v", err)
	}
	assertFKKeyColumnsAreBinary(t, indexDB)

	holder, err := indexDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if _, err := holder.ExecContext(ctx, "START TRANSACTION"); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(ctx, "SELECT COUNT(*) FROM fk_constraints"); err != nil {
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
	if err := ensureFKConstraintsNameCollationOn(ctx, migrator); err != nil {
		t.Fatalf("second migration on an already-binary table must be a no-op, got %v", err)
	}
}

// No table is not an error: an index that predates fk_constraints skips FK
// capture, and `bintrail init` creates the table with binary names.
func TestIntegrationFKCollationMigrationToleratesMissingTable_1839(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	if err := EnsureFKConstraintsNameCollation(context.Background(), indexDB); err != nil {
		t.Fatalf("missing table must be a no-op, got %v", err)
	}
}

// The migration's bounded lock wait must never go back into the pool. One
// connection in the pool makes the test get the same one back.
func TestIntegrationFKMigrationLeavesThePoolUntouched_1839(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	indexDB.SetMaxOpenConns(1)
	indexDB.SetMaxIdleConns(1)
	ctx := context.Background()
	testutil.MustExec(t, indexDB, legacyDDLFKConstraints)
	requireFKConversionDue(t, indexDB)

	if err := EnsureFKConstraintsNameCollation(ctx, indexDB); err != nil {
		t.Fatalf("migration: %v", err)
	}
	assertFKKeyColumnsAreBinary(t, indexDB)
	var same bool
	if err := indexDB.QueryRowContext(ctx,
		"SELECT @@SESSION.lock_wait_timeout = @@GLOBAL.lock_wait_timeout").Scan(&same); err != nil {
		t.Fatal(err)
	}
	if !same {
		t.Fatal("a pooled connection kept the migration's lock_wait_timeout")
	}
}

// When the conversion cannot run (another session holds the table), the
// writer waits only the bounded lock wait, carries on, and a snapshot that
// then hits the old collision says why instead of a bare 1062.
func TestIntegrationFKBlockedMigrationIsBoundedAndNamed_1839(t *testing.T) {
	sourceDB, _ := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	testutil.MustExec(t, indexDB, "DROP TABLE fk_constraints")
	testutil.MustExec(t, indexDB, legacyDDLFKConstraints)
	names := createTwinFKSchemas(t, sourceDB)
	ctx := context.Background()

	holder, err := indexDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if _, err := holder.ExecContext(ctx, "START TRANSACTION"); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(ctx, "SELECT COUNT(*) FROM fk_constraints"); err != nil {
		t.Fatal(err)
	}
	defer holder.ExecContext(ctx, "ROLLBACK") //nolint:errcheck

	start := time.Now()
	_, err = TakeSnapshot(sourceDB, indexDB, names)
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("snapshot took %v; the migration's lock wait is not bounded", elapsed)
	}
	if err == nil {
		t.Fatal("with the table unconverted, the twins must still collide (this test's premise)")
	}
	if !strings.Contains(err.Error(), "#1839") || !strings.Contains(err.Error(), "could not be converted") {
		t.Fatalf("the collision must name its cause, got: %v", err)
	}
}
