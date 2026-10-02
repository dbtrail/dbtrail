//go:build integration

package metadata

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestTakeSnapshotCapturesFKRules pins cascade-recovery Slice A: a snapshot of
// a cascade schema records each FK's delete_rule/update_rule in fk_constraints,
// and CascadeConstraintsInIndex surfaces the CASCADE edge for the source-less
// recover-time warning.
func TestTakeSnapshotCapturesFKRules(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child (
		id  INT PRIMARY KEY,
		pid INT,
		CONSTRAINT fk_child FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE CASCADE
	) ENGINE=InnoDB`)
	// A non-cascade (RESTRICT) child must be EXCLUDED by CascadeConstraintsInIndex —
	// pins the delete_rule/update_rule = 'CASCADE' filter against being too permissive.
	testutil.MustExec(t, sourceDB, `CREATE TABLE child_restrict (
		id  INT PRIMARY KEY,
		pid INT,
		CONSTRAINT fk_restrict FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE RESTRICT ON UPDATE RESTRICT
	) ENGINE=InnoDB`)

	if _, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	var del, upd string
	if err := indexDB.QueryRow(`SELECT delete_rule, update_rule FROM fk_constraints
		WHERE schema_name = ? AND table_name = 'child' AND column_name = 'pid'`,
		sourceName).Scan(&del, &upd); err != nil {
		t.Fatalf("read fk_constraints rule: %v", err)
	}
	if del != "CASCADE" {
		t.Errorf("delete_rule = %q, want CASCADE", del)
	}
	if upd != "CASCADE" {
		t.Errorf("update_rule = %q, want CASCADE", upd)
	}

	edges, err := CascadeConstraintsInIndex(indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("CascadeConstraintsInIndex: %v", err)
	}
	if len(edges) != 1 {
		t.Fatalf("want exactly 1 cascade edge (RESTRICT child excluded), got %d: %+v", len(edges), edges)
	}
	if edges[0].Table != "child" || edges[0].DeleteRule != "CASCADE" || edges[0].ReferencedTable != "parent" {
		t.Errorf("unexpected cascade edge: %+v", edges[0])
	}
	for _, e := range edges {
		if e.Table == "child_restrict" {
			t.Errorf("RESTRICT-only child must be excluded, but appeared: %+v", e)
		}
	}

	// An unrelated schema yields no cascade edges.
	none, err := CascadeConstraintsInIndex(indexDB, []string{"nonexistent_schema"})
	if err != nil {
		t.Fatalf("CascadeConstraintsInIndex(none): %v", err)
	}
	if len(none) != 0 {
		t.Errorf("want 0 edges for an unrelated schema, got %d", len(none))
	}
}

func TestTakeSnapshot_nonInnoDB(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		status VARCHAR(20)
	) ENGINE=MyISAM`)

	_, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("expected validation error for non-InnoDB table, got nil")
	}
	if !strings.Contains(err.Error(), "not using InnoDB") {
		t.Errorf("expected 'not using InnoDB' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), sourceName+".orders") {
		t.Errorf("expected table name in error, got: %v", err)
	}
}

func TestTakeSnapshot_noPK(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE events (
		name VARCHAR(100),
		value INT
	) ENGINE=InnoDB`)

	_, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("expected validation error for table without primary key, got nil")
	}
	if !strings.Contains(err.Error(), "without a primary key") {
		t.Errorf("expected 'without a primary key' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), sourceName+".events") {
		t.Errorf("expected table name in error, got: %v", err)
	}
}

func TestTakeSnapshot_bothViolations(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE myisam_tbl (id INT PRIMARY KEY) ENGINE=MyISAM`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE nopk_tbl (name VARCHAR(100)) ENGINE=InnoDB`)

	_, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "not using InnoDB") {
		t.Errorf("expected 'not using InnoDB' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "without a primary key") {
		t.Errorf("expected 'without a primary key' in error, got: %v", err)
	}
}

// TestTakeSnapshotExcludingInvalid_degrades pins the #1051 fix: against a scope
// holding a no-PK table and a MyISAM table, the strict TakeSnapshot must still
// fail (crash-loop prevention lives ONLY in the hook variant), while
// TakeSnapshotExcludingInvalid must succeed, report exactly the invalid tables
// as excluded, keep the valid tables capturable, and KEEP every fk_constraints
// row — including the excluded child's CASCADE edge, which recover-cascade
// needs to see to report an honest Incomplete instead of a silent Complete
// (the cascade-layer half is TestSynthesizeVictims_excludedChildFlagged in
// internal/cascade).
func TestTakeSnapshotExcludingInvalid_degrades(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE ok_parent (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE ok_child (
		id  INT PRIMARY KEY,
		pid INT,
		CONSTRAINT fk_ok_child FOREIGN KEY (pid) REFERENCES ok_parent(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	// Invalid on two different axes — and nopk_child carries a real CASCADE
	// FK, whose fk_constraints rows must SURVIVE the exclusion (dropping them
	// would erase the edge and let recover-cascade report Complete over a
	// genuine cascade).
	testutil.MustExec(t, sourceDB, `CREATE TABLE nopk_child (
		pid INT,
		CONSTRAINT fk_nopk_child FOREIGN KEY (pid) REFERENCES ok_parent(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE myisam_tbl (id INT PRIMARY KEY) ENGINE=MyISAM`)

	// Strict mode: unchanged — the same scope still fails wholesale.
	if _, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err == nil {
		t.Fatal("strict TakeSnapshot must still fail on a scope with invalid tables")
	}

	stats, err := TakeSnapshotExcludingInvalid(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshotExcludingInvalid: %v", err)
	}

	wantExcluded := []string{sourceName + ".myisam_tbl", sourceName + ".nopk_child"}
	if len(stats.ExcludedTables) != 2 ||
		stats.ExcludedTables[0] != wantExcluded[0] || stats.ExcludedTables[1] != wantExcluded[1] {
		t.Errorf("ExcludedTables = %v, want %v", stats.ExcludedTables, wantExcluded)
	}
	if stats.TableCount != 2 {
		t.Errorf("TableCount = %d, want 2 (ok_parent + ok_child)", stats.TableCount)
	}

	// The snapshot rows must cover exactly the valid tables.
	var snapTables []string
	rows, err := indexDB.Query(
		"SELECT DISTINCT table_name FROM schema_snapshots WHERE snapshot_id = ? ORDER BY table_name",
		stats.SnapshotID)
	if err != nil {
		t.Fatalf("query schema_snapshots: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		snapTables = append(snapTables, name)
	}
	if len(snapTables) != 2 || snapTables[0] != "ok_child" || snapTables[1] != "ok_parent" {
		t.Errorf("snapshot tables = %v, want [ok_child ok_parent]", snapTables)
	}

	// fk_constraints: BOTH children's FK rows survive — the excluded child's
	// edge is the one recover-cascade must still see (#1051 review).
	var fkTables []string
	fkRows, err := indexDB.Query(
		"SELECT DISTINCT table_name FROM fk_constraints WHERE snapshot_id = ? ORDER BY table_name",
		stats.SnapshotID)
	if err != nil {
		t.Fatalf("query fk_constraints: %v", err)
	}
	defer fkRows.Close()
	for fkRows.Next() {
		var name string
		if err := fkRows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		fkTables = append(fkTables, name)
	}
	if len(fkTables) != 2 || fkTables[0] != "nopk_child" || fkTables[1] != "ok_child" {
		t.Errorf("fk_constraints tables = %v, want [nopk_child ok_child]", fkTables)
	}
	if stats.FKCount != 2 {
		t.Errorf("FKCount = %d, want 2", stats.FKCount)
	}

	// The exclusions must be recorded EXPLICITLY in snapshot_exclusions under
	// the same snapshot_id (the record the cascade FK loaders flag from —
	// inferring exclusion from schema_snapshots absence was retired as
	// unsound), with per-table reasons.
	exclReasons := map[string]string{}
	exclRows, err := indexDB.Query(
		"SELECT table_name, reason FROM snapshot_exclusions WHERE snapshot_id = ? ORDER BY table_name",
		stats.SnapshotID)
	if err != nil {
		t.Fatalf("query snapshot_exclusions: %v", err)
	}
	defer exclRows.Close()
	for exclRows.Next() {
		var name, reason string
		if err := exclRows.Scan(&name, &reason); err != nil {
			t.Fatalf("scan: %v", err)
		}
		exclReasons[name] = reason
	}
	if len(exclReasons) != 2 {
		t.Errorf("snapshot_exclusions rows = %v, want 2", exclReasons)
	}
	if exclReasons["myisam_tbl"] != "not InnoDB" {
		t.Errorf("myisam_tbl reason = %q, want %q", exclReasons["myisam_tbl"], "not InnoDB")
	}
	if exclReasons["nopk_child"] != "no primary key" {
		t.Errorf("nopk_child reason = %q, want %q", exclReasons["nopk_child"], "no primary key")
	}

	// The resolver loaded from this snapshot sees the valid tables and not the
	// excluded ones — the property the DDL hook actually depends on.
	resolver, err := NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	if _, err := resolver.Resolve(sourceName, "ok_parent"); err != nil {
		t.Errorf("resolver must resolve ok_parent: %v", err)
	}
	if _, err := resolver.Resolve(sourceName, "nopk_child"); err == nil {
		t.Error("resolver must NOT resolve the excluded nopk_child")
	}
}

// TestTakeSnapshotExcludingInvalid_allInvalidStillFails: degrading must never
// write an EMPTY snapshot — if every table in scope is invalid there is nothing
// to capture, and that stays a hard (#760 fail-loud) error even in hook mode.
func TestTakeSnapshotExcludingInvalid_allInvalidStillFails(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE nopk_tbl (name VARCHAR(100)) ENGINE=InnoDB`)

	_, err := TakeSnapshotExcludingInvalid(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("expected error when every table in scope fails validation, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to write an empty snapshot") {
		t.Errorf("expected empty-snapshot refusal, got: %v", err)
	}
	if !strings.Contains(err.Error(), sourceName+".nopk_tbl") {
		t.Errorf("expected offending table name in error, got: %v", err)
	}
}

// TestTakeSnapshotExcludingInvalid_cleanScope: with nothing to exclude the
// variant must behave exactly like TakeSnapshot — no exclusions reported.
func TestTakeSnapshotExcludingInvalid_cleanScope(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(20)) ENGINE=InnoDB`)

	stats, err := TakeSnapshotExcludingInvalid(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshotExcludingInvalid: %v", err)
	}
	if len(stats.ExcludedTables) != 0 {
		t.Errorf("ExcludedTables = %v, want none", stats.ExcludedTables)
	}
	if stats.TableCount != 1 || stats.ColumnCount != 2 {
		t.Errorf("stats = %+v, want 1 table / 2 columns", stats)
	}
}

func TestTakeSnapshot_basic(t *testing.T) {
	// Create two databases: source (with a real table) and index (for snapshot storage).
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)

	// Create index tables.
	testutil.InitIndexTables(t, indexDB)

	// Create a source table.
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		customer VARCHAR(100) NOT NULL,
		status VARCHAR(20) NOT NULL,
		amount DECIMAL(10,2) NOT NULL
	)`)

	stats, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	if stats.SnapshotID < 1 {
		t.Errorf("expected SnapshotID >= 1, got %d", stats.SnapshotID)
	}
	if stats.TableCount < 1 {
		t.Errorf("expected at least 1 table, got %d", stats.TableCount)
	}
	if stats.ColumnCount < 4 {
		t.Errorf("expected at least 4 columns (orders has 4), got %d", stats.ColumnCount)
	}

	// Verify rows exist in schema_snapshots.
	var count int
	indexDB.QueryRow("SELECT COUNT(*) FROM schema_snapshots WHERE snapshot_id = ?", stats.SnapshotID).Scan(&count)
	if count < 4 {
		t.Errorf("expected at least 4 snapshot rows, got %d", count)
	}
}

// TestTakeSnapshot_defaultGeneratedNotFlaggedGenerated is the #758 regression
// test: information_schema.COLUMNS.EXTRA reports "DEFAULT_GENERATED" for an
// ordinary column with an expression default (e.g. created_at TIMESTAMP
// DEFAULT CURRENT_TIMESTAMP), not just for true VIRTUAL/STORED generated
// columns. TakeSnapshot must tell these apart via GENERATION_EXPRESSION —
// a substring match on EXTRA wrongly flags created_at as generated, which
// makes recover silently omit it from reversal SQL (data corruption on
// restore).
func TestTakeSnapshot_defaultGeneratedNotFlaggedGenerated(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id         INT PRIMARY KEY,
		total      DECIMAL(10,2) NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		full_total DECIMAL(10,2) GENERATED ALWAYS AS (total * 1) STORED
	) ENGINE=InnoDB`)

	stats, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	isGenerated := func(column string) bool {
		var got bool
		if err := indexDB.QueryRow(
			`SELECT is_generated FROM schema_snapshots
			 WHERE snapshot_id = ? AND schema_name = ? AND table_name = 'orders' AND column_name = ?`,
			stats.SnapshotID, sourceName, column,
		).Scan(&got); err != nil {
			t.Fatalf("read is_generated for %s: %v", column, err)
		}
		return got
	}

	if isGenerated("created_at") {
		t.Error("created_at (DEFAULT_GENERATED expression default) must NOT be flagged is_generated — it is a real, captured data column (#758)")
	}
	if !isGenerated("full_total") {
		t.Error("full_total (STORED generated) must be flagged is_generated")
	}
}

func TestTakeSnapshot_filteredSchemas(t *testing.T) {
	// Two source DBs but only snapshot one.
	sourceDB1, name1 := testutil.CreateTestDB(t)
	sourceDB2, name2 := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB1, "CREATE TABLE tbl1 (id INT PRIMARY KEY)")
	testutil.MustExec(t, sourceDB2, "CREATE TABLE tbl2 (id INT PRIMARY KEY)")

	// Only snapshot name1.
	stats, err := TakeSnapshot(sourceDB1, indexDB, []string{name1})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Verify name2's table is not in the snapshot.
	var count int
	indexDB.QueryRow("SELECT COUNT(*) FROM schema_snapshots WHERE snapshot_id = ? AND schema_name = ?",
		stats.SnapshotID, name2).Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 rows for filtered schema %q, got %d", name2, count)
	}
}

func TestNewResolver_latestSnapshot(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Insert two snapshots manually.
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "status", 3, "", "varchar", "NO")

	// NewResolver(db, 0) should load the latest (snapshot 2 with 3 columns).
	resolver, err := NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver(db, 0) failed: %v", err)
	}
	if resolver.SnapshotID() != 2 {
		t.Errorf("expected snapshot_id=2, got %d", resolver.SnapshotID())
	}

	tm, err := resolver.Resolve("mydb", "orders")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(tm.Columns) != 3 {
		t.Errorf("expected 3 columns in snapshot 2, got %d", len(tm.Columns))
	}
}

func TestNewResolver_specificSnapshot(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "status", 3, "", "varchar", "NO")

	// Load specific snapshot 1 (2 columns only).
	resolver, err := NewResolver(indexDB, 1)
	if err != nil {
		t.Fatalf("NewResolver(db, 1) failed: %v", err)
	}
	if resolver.SnapshotID() != 1 {
		t.Errorf("expected snapshot_id=1, got %d", resolver.SnapshotID())
	}

	tm, err := resolver.Resolve("mydb", "orders")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(tm.Columns) != 2 {
		t.Errorf("expected 2 columns in snapshot 1, got %d", len(tm.Columns))
	}
}

func TestNewResolver_emptyTable(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// No snapshots inserted — should error.
	_, err := NewResolver(indexDB, 0)
	if err == nil {
		t.Error("expected error for empty schema_snapshots, got nil")
	}
}

// ─── NewLatestPerTableResolver (#603) ─────────────────────────────────────────

// TestNewLatestPerTableResolver_pgPerTableSnapshots pins the PG layout that
// motivated #603: WritePGSnapshot writes ONE table per snapshot_id, so the
// single-latest-snapshot resolver sees only the last table that saw DML. The
// per-table-newest union must surface every table, each under its own newest
// shape.
func TestNewLatestPerTableResolver_pgPerTableSnapshots(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	rel := func(table string, extraCols ...string) *PGRelationSchema {
		r := &PGRelationSchema{Schema: "public", Table: table,
			Columns: []PGRelationColumn{{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 23, TypeMod: -1}}}
		for i, c := range extraCols {
			r.Columns = append(r.Columns, PGRelationColumn{Name: c, Ordinal: i + 2, TypeOID: 25, TypeMod: -1})
		}
		return r
	}

	for _, tbl := range []string{"users", "orders"} {
		if _, err := WritePGSnapshot(context.Background(), indexDB, rel(tbl, "v1")); err != nil {
			t.Fatalf("WritePGSnapshot(%s): %v", tbl, err)
		}
	}
	// users evolves (a later RelationMessage after an ALTER): its NEWEST
	// per-table snapshot must win over its older one.
	if _, err := WritePGSnapshot(context.Background(), indexDB, rel("users", "v1", "v2")); err != nil {
		t.Fatalf("WritePGSnapshot(users v2): %v", err)
	}

	r, err := NewLatestPerTableResolver(indexDB)
	if err != nil {
		t.Fatalf("NewLatestPerTableResolver: %v", err)
	}

	tables := r.Tables("public")
	if len(tables) != 2 {
		names := make([]string, 0, len(tables))
		for _, tm := range tables {
			names = append(names, tm.Table)
		}
		t.Fatalf("Tables(public) = %v, want [orders users]", names)
	}
	users, err := r.Resolve("public", "users")
	if err != nil {
		t.Fatalf("Resolve(public.users): %v", err)
	}
	if len(users.Columns) != 3 {
		t.Errorf("users columns = %d, want 3 (newest per-table snapshot must win)", len(users.Columns))
	}
	if got := users.PKColumns; len(got) != 1 || got[0] != "id" {
		t.Errorf("users PKColumns = %v, want [id]", got)
	}
	orders, err := r.Resolve("public", "orders")
	if err != nil {
		t.Fatalf("Resolve(public.orders): %v", err)
	}
	if len(orders.Columns) != 2 {
		t.Errorf("orders columns = %d, want 2", len(orders.Columns))
	}
}

// TestNewLatestPerTableResolver_mysqlEquivalenceAndDroppedTable pins the two
// MySQL-layout properties the union must hold:
//
//  1. Strict generalization: for a table present in the latest whole-schema
//     snapshot, the union resolves the SAME shape NewResolver(db, 0) does.
//  2. Documented retention semantic: a table present only in an OLDER
//     snapshot (dropped at the source, then re-snapshotted) stays resolvable
//     under its last-known shape — its indexed history remains addressable
//     by the shim's time-travel surfaces (the table-level analog of #600's
//     dropped-column surfacing).
func TestNewLatestPerTableResolver_mysqlEquivalenceAndDroppedTable(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Snapshot 1: orders (2 cols) + legacy (1 col). Snapshot 2: orders only
	// (3 cols) — legacy was dropped before the re-snapshot.
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "legacy", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "status", 3, "", "varchar", "NO")

	r, err := NewLatestPerTableResolver(indexDB)
	if err != nil {
		t.Fatalf("NewLatestPerTableResolver: %v", err)
	}

	// (1) orders resolves identically to the single-latest-snapshot view.
	latest, err := NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver(db, 0): %v", err)
	}
	fromLatest, err := latest.Resolve("mydb", "orders")
	if err != nil {
		t.Fatalf("latest Resolve(mydb.orders): %v", err)
	}
	fromUnion, err := r.Resolve("mydb", "orders")
	if err != nil {
		t.Fatalf("union Resolve(mydb.orders): %v", err)
	}
	if len(fromUnion.Columns) != len(fromLatest.Columns) || len(fromUnion.Columns) != 3 {
		t.Errorf("union orders columns = %d, latest = %d, want both 3",
			len(fromUnion.Columns), len(fromLatest.Columns))
	}

	// (2) legacy (only in snapshot 1) is retained under its last-known shape.
	legacy, err := r.Resolve("mydb", "legacy")
	if err != nil {
		t.Fatalf("union Resolve(mydb.legacy): %v (dropped table must stay resolvable)", err)
	}
	if len(legacy.Columns) != 1 {
		t.Errorf("legacy columns = %d, want 1", len(legacy.Columns))
	}
	// And it lists in the schema's table view (SHOW TABLES backing).
	names := make([]string, 0)
	for _, tm := range r.Tables("mydb") {
		names = append(names, tm.Table)
	}
	if len(names) != 2 || names[0] != "legacy" || names[1] != "orders" {
		t.Errorf("Tables(mydb) = %v, want [legacy orders]", names)
	}
}

// TestNewLatestPerTableResolver_pre212WarnsPerTable pins the per-table
// pre-#212 warning: in a union, one post-#212 table (column_type captured)
// must NOT silence the warning for a retained table whose newest shape
// predates column_type — and the warning must name the affected table.
func TestNewLatestPerTableResolver_pre212WarnsPerTable(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// "stale" (snapshot 1): data_type only — the pre-#212 signature.
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "stale", "id", 1, "PRI", "int", "NO")
	// "fresh" (snapshot 2): column_type present — post-#212.
	if _, err := indexDB.Exec(`INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, column_type, is_nullable)
		VALUES (2, '2026-02-19 10:00:00', 'mydb', 'fresh', 'id', 1, 'PRI', 'int', 'int unsigned', 'NO')`); err != nil {
		t.Fatalf("insert post-#212 row: %v", err)
	}

	var logbuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	if _, err := NewLatestPerTableResolver(indexDB); err != nil {
		t.Fatalf("NewLatestPerTableResolver: %v", err)
	}

	logs := logbuf.String()
	if !strings.Contains(logs, "predates column_type capture") {
		t.Errorf("expected pre-#212 warning (a post-#212 table must not silence it); logs:\n%s", logs)
	}
	if !strings.Contains(logs, "mydb.stale") {
		t.Errorf("warning must name the affected table mydb.stale; logs:\n%s", logs)
	}
	if strings.Contains(logs, "mydb.fresh") {
		t.Errorf("warning must not name the post-#212 table mydb.fresh; logs:\n%s", logs)
	}
}

// TestNewLatestPerTableResolver_empty pins the ErrNoSnapshots sentinel: an
// empty schema_snapshots must return the same benign first-install signal
// NewResolver does, so the shim's empty-set SHOW TABLES path keeps working.
func TestNewLatestPerTableResolver_empty(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	_, err := NewLatestPerTableResolver(indexDB)
	if !errors.Is(err, ErrNoSnapshots) {
		t.Errorf("expected ErrNoSnapshots, got %v", err)
	}
}

func TestResolver_pkColumns(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Composite PK: (order_id, item_id).
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-19 10:00:00", "mydb", "order_items", "order_id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-19 10:00:00", "mydb", "order_items", "item_id", 2, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-19 10:00:00", "mydb", "order_items", "quantity", 3, "", "int", "NO")

	resolver, err := NewResolver(indexDB, 1)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	tm, err := resolver.Resolve("mydb", "order_items")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	pkCols := tm.PKColumnMetas()
	if len(pkCols) != 2 {
		t.Fatalf("expected 2 PK columns, got %d", len(pkCols))
	}
	if pkCols[0].Name != "order_id" || pkCols[1].Name != "item_id" {
		t.Errorf("expected PK columns [order_id, item_id], got [%s, %s]", pkCols[0].Name, pkCols[1].Name)
	}
}

// TestTakeSnapshot_columnType is the #212 regression test for the schema-
// snapshot side of the precision-aware PK canonicalizer. TakeSnapshot must
// read `information_schema.COLUMNS.COLUMN_TYPE` and store the full type
// (e.g. "datetime(6)") in `schema_snapshots.column_type`, so the reconstruct
// canonicalizer can parse the declared fractional precision.
//
// Before this fix, schema_snapshots only had `data_type` (base type like
// "datetime"), and the canonicalizer had no way to tell DATETIME(0) from
// DATETIME(6). This test proves TakeSnapshot actually captures the precision.
func TestTakeSnapshot_columnType(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE events (
		id INT PRIMARY KEY,
		created_at DATETIME(6) NOT NULL,
		amount DECIMAL(12,4) NOT NULL,
		slug VARCHAR(64) NOT NULL
	)`)

	stats, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Pull column_type back out for each column and check it matches.
	cases := []struct {
		column   string
		wantType string
	}{
		{"id", "int"},
		{"created_at", "datetime(6)"},
		{"amount", "decimal(12,4)"},
		{"slug", "varchar(64)"},
	}
	for _, c := range cases {
		var got string
		err := indexDB.QueryRow(
			`SELECT column_type FROM schema_snapshots
			 WHERE snapshot_id = ? AND table_name = 'events' AND column_name = ?`,
			stats.SnapshotID, c.column,
		).Scan(&got)
		if err != nil {
			t.Errorf("query column_type for %s: %v", c.column, err)
			continue
		}
		if got != c.wantType {
			t.Errorf("column %s: got column_type=%q, want %q", c.column, got, c.wantType)
		}
	}
}

// ─── ValidateBinlogFormat ────────────────────────────────────────────────────────────

func TestValidateBinlogFormat_row(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	// Docker test container should have binlog_format=ROW.
	dsn := testutil.IntegrationDSN("")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	if err := ValidateBinlogFormat(db); err != nil {
		t.Fatalf("expected nil error for ROW binlog_format, got: %v", err)
	}
}

// ─── ValidateBinlogRowImage ────────────────────────────────────────────────────────────

func TestValidateBinlogRowImage_full(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	// Docker test container should have binlog_row_image=FULL (default).
	dsn := testutil.IntegrationDSN("")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	if err := ValidateBinlogRowImage(db); err != nil {
		t.Fatalf("expected nil error for FULL binlog_row_image, got: %v", err)
	}
}

// ─── ValidateNoFKCascades ────────────────────────────────────────────────────────────

func TestValidateNoFKCascades_none(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT,
		total DECIMAL(10,2) NOT NULL
	)`)

	if err := ValidateNoFKCascades(db, []string{dbName}); err != nil {
		t.Fatalf("expected nil error for schema with no cascades, got: %v", err)
	}
}

func TestValidateNoFKCascades_cascade(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT,
		total DECIMAL(10,2) NOT NULL
	)`)
	testutil.MustExec(t, db, `CREATE TABLE order_items (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		order_id INT NOT NULL,
		CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
	)`)

	err := ValidateNoFKCascades(db, []string{dbName})
	if err == nil {
		t.Fatal("expected error for schema with FK cascade, got nil")
	}
	// The cascade finding must be wrapped in ErrFKCascadesFound so call sites can
	// errors.Is it apart from an operational query failure (which must still abort).
	if !errors.Is(err, ErrFKCascadesFound) {
		t.Errorf("cascade error must wrap ErrFKCascadesFound, got: %v", err)
	}
}

// ON DELETE SET NULL and ON UPDATE SET NULL are cascading actions bintrail's
// recover-cascade synthesizes, so the doctor/pre-flight must report them too
// (#1125) — matching CASCADE alone silently under-reported these schemas.
func TestValidateNoFKCascades_setNull(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE users (
		id INT PRIMARY KEY AUTO_INCREMENT
	)`)
	testutil.MustExec(t, db, `CREATE TABLE posts (
		id      INT PRIMARY KEY AUTO_INCREMENT,
		user_id INT,
		CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
	)`)

	err := ValidateNoFKCascades(db, []string{dbName})
	if err == nil {
		t.Fatal("expected error for schema with ON DELETE SET NULL, got nil")
	}
	if !errors.Is(err, ErrFKCascadesFound) {
		t.Errorf("SET NULL finding must wrap ErrFKCascadesFound, got: %v", err)
	}
}

func TestValidateNoFKCascades_updateSetNull(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE tags (
		id INT PRIMARY KEY AUTO_INCREMENT
	)`)
	testutil.MustExec(t, db, `CREATE TABLE tagged (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		tag_id INT,
		CONSTRAINT fk_tag FOREIGN KEY (tag_id) REFERENCES tags(id) ON UPDATE SET NULL
	)`)

	if err := ValidateNoFKCascades(db, []string{dbName}); err == nil {
		t.Fatal("expected error for schema with ON UPDATE SET NULL, got nil")
	}
}

func TestValidateNoFKCascades_updateCascade(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE categories (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)
	testutil.MustExec(t, db, `CREATE TABLE products (
		id          INT PRIMARY KEY AUTO_INCREMENT,
		category_id INT NOT NULL,
		CONSTRAINT fk_cat FOREIGN KEY (category_id) REFERENCES categories(id) ON UPDATE CASCADE
	)`)

	if err := ValidateNoFKCascades(db, []string{dbName}); err == nil {
		t.Fatal("expected error for schema with UPDATE CASCADE, got nil")
	}
}

func TestValidateNoFKCascades_otherSchemaIgnored(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	otherDB, otherName := testutil.CreateTestDB(t)

	// Create a cascade in otherDB.
	testutil.MustExec(t, otherDB, `CREATE TABLE parents (id INT PRIMARY KEY)`)
	testutil.MustExec(t, otherDB, `CREATE TABLE children (
		id INT PRIMARY KEY,
		parent_id INT NOT NULL,
		CONSTRAINT fk_p FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE CASCADE
	)`)

	// dbName has no cascades — checking only dbName should pass.
	_ = dbName
	if err := ValidateNoFKCascades(db, []string{dbName}); err != nil {
		t.Fatalf("expected nil when cascade is only in %q (not targeted), got: %v", otherName, err)
	}
}

// The unscoped pre-flight skips a bintrail index schema regardless of its name:
// it is recognised by its signature tables (binlog_events, schema_snapshots,
// stream_state), not by a name pattern. Here the index DB has a non-bintrail
// name (`audit_index`) yet carries the access_rules→profiles cascade — the scan
// must skip it (the #347 fix, now name-independent, closing the custom-name
// under-exclusion hole, #365). When the operator names it explicitly, it is
// still policed.
func TestValidateNoFKCascades_customNamedIndexSkippedWhenUnscoped(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)

	const idxSchema = "audit_index" // not bt_-prefixed, not the default index name
	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `"+idxSchema+"`")
	testutil.MustExec(t, db, "CREATE DATABASE `"+idxSchema+"`")
	t.Cleanup(func() { _, _ = db.Exec("DROP DATABASE IF EXISTS `" + idxSchema + "`") })

	// The signature tables that mark the schema as a bintrail index. binlog_events
	// is RANGE-partitioned in production, so partition it here too — this confirms
	// the subquery's TABLE_TYPE = 'BASE TABLE' filter matches a partitioned table.
	testutil.MustExec(t, db, "CREATE TABLE `"+idxSchema+"`.binlog_events ("+
		"id INT, event_timestamp DATETIME NOT NULL, PRIMARY KEY (id, event_timestamp)) "+
		"PARTITION BY RANGE (TO_SECONDS(event_timestamp)) (PARTITION p_future VALUES LESS THAN MAXVALUE)")
	testutil.MustExec(t, db, "CREATE TABLE `"+idxSchema+"`.schema_snapshots (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "CREATE TABLE `"+idxSchema+"`.stream_state (id INT PRIMARY KEY)")
	// ...plus the access_rules→profiles ON DELETE CASCADE the pre-flight trips on.
	testutil.MustExec(t, db, "CREATE TABLE `"+idxSchema+"`.profiles (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "CREATE TABLE `"+idxSchema+"`.access_rules ("+
		"id INT PRIMARY KEY, profile_id INT NOT NULL, "+
		"CONSTRAINT fk_access_rules_profile FOREIGN KEY (profile_id) REFERENCES `"+idxSchema+"`.profiles(id) ON DELETE CASCADE)")

	// Unscoped: skipped because it is structurally a bintrail index, despite the
	// non-bintrail name. Assert audit_index specifically is absent from the scan
	// rather than that the whole server-wide scan is clean — that keeps the test
	// robust to unrelated cascades that may exist on a shared/dev MySQL server.
	if schemas := unscopedFKCascadeSchemas(t, db); schemas[idxSchema] {
		t.Fatalf("expected structurally-internal schema %q to be excluded from the unscoped scan, but it was flagged", idxSchema)
	}

	// Explicitly named: still policed.
	if err := ValidateNoFKCascades(db, []string{idxSchema}); err == nil {
		t.Fatalf("expected error when %q is explicitly targeted", idxSchema)
	}
}

// unscopedFKCascadeSchemas runs the unscoped FK-cascade query and returns the
// set of schemas it flags. Lets a test assert that a specific schema is (or is
// not) excluded without depending on the rest of the server being cascade-free.
func unscopedFKCascadeSchemas(t *testing.T, db *sql.DB) map[string]bool {
	t.Helper()
	found, err := FindFKCascades(db, nil)
	if err != nil {
		t.Fatalf("unscoped FK-cascade query: %v", err)
	}
	got := map[string]bool{}
	for _, c := range found {
		got[c.Schema] = true
	}
	return got
}

// The inverse of the exclusion, and the load-bearing direction: an unscoped
// scan must still CATCH a real CASCADE FK in an ordinary user schema (no bt_
// prefix, not an index-DB name). This guards against a regression that
// broadens the exclusion until the unscoped branch matches nothing. testutil
// only makes bt_-prefixed DBs, so the non-internal schema is created by hand.
func TestValidateNoFKCascades_userSchemaCaughtWhenUnscoped(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)

	const userSchema = "fkcascade_user"
	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `"+userSchema+"`")
	testutil.MustExec(t, db, "CREATE DATABASE `"+userSchema+"`")
	t.Cleanup(func() { _, _ = db.Exec("DROP DATABASE IF EXISTS `" + userSchema + "`") })

	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.parents (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.children ("+
		"id INT PRIMARY KEY, parent_id INT NOT NULL, "+
		"CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES `"+userSchema+"`.parents(id) ON DELETE CASCADE)")

	if err := ValidateNoFKCascades(db, nil); err == nil {
		t.Fatalf("expected unscoped scan to catch the CASCADE FK in non-internal schema %q, got nil", userSchema)
	}
}

// A schema with only SOME of the signature tables (here 2 of 3) is NOT a
// bintrail index — its real CASCADE must still be CAUGHT when unscoped. This
// pins the exactness of HAVING COUNT(DISTINCT TABLE_NAME) = 3: a regression to
// >= 1 (or a shorter IN-list) would silently skip this real user cascade,
// reopening the #347-class silent-skip bug. (The existing "caught" test above
// uses zero signature tables, so it would not detect such a loosening.)
func TestValidateNoFKCascades_partialSignatureCaughtWhenUnscoped(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)

	const userSchema = "fkcascade_partial"
	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `"+userSchema+"`")
	testutil.MustExec(t, db, "CREATE DATABASE `"+userSchema+"`")
	t.Cleanup(func() { _, _ = db.Exec("DROP DATABASE IF EXISTS `" + userSchema + "`") })

	// Two of the three signature names, but not all three.
	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.binlog_events (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.stream_state (id INT PRIMARY KEY)")
	// ...plus a genuine cascade that must be caught.
	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.parents (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "CREATE TABLE `"+userSchema+"`.children ("+
		"id INT PRIMARY KEY, parent_id INT NOT NULL, "+
		"CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES `"+userSchema+"`.parents(id) ON DELETE CASCADE)")

	if !unscopedFKCascadeSchemas(t, db)[userSchema] {
		t.Fatalf("expected partial-signature schema %q (2 of 3 signature tables) to be flagged by the unscoped scan, but it was not", userSchema)
	}
}

// ─── EnsureResolver ──────────────────────────────────────────────────────────────────

func TestEnsureResolver_autoSnapshot(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Create a table on the source.
	testutil.MustExec(t, sourceDB, `CREATE TABLE products (
		id   INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)

	resolver, err := EnsureResolver(indexDB, sourceDB, []string{sourceName})
	if err != nil {
		t.Fatalf("EnsureResolver failed: %v", err)
	}

	if resolver.SnapshotID() == 0 {
		t.Error("expected non-zero snapshot ID")
	}
	if resolver.TableCount() != 1 {
		t.Errorf("expected 1 table, got %d", resolver.TableCount())
	}
}

func TestEnsureResolver_noSnapshotNoSource(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	_, err := EnsureResolver(indexDB, nil, nil)
	if err == nil {
		t.Fatal("expected error when no snapshot and no sourceDB")
	}
}

func TestEnsureResolver_existingSnapshot(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100)
	)`)

	// Take snapshot manually first.
	testutil.InsertSnapshot(t, indexDB, 1, "2026-01-01 00:00:00",
		sourceName, "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-01-01 00:00:00",
		sourceName, "orders", "name", 2, "", "varchar", "YES")

	// Should load existing snapshot without needing sourceDB.
	resolver, err := EnsureResolver(indexDB, nil, nil)
	if err != nil {
		t.Fatalf("EnsureResolver failed: %v", err)
	}
	if resolver.SnapshotID() != 1 {
		t.Errorf("expected snapshot ID 1, got %d", resolver.SnapshotID())
	}
}

// TestTakeSnapshot_longEnumColumnType pins the #472 capture-side fix: a
// realistic ENUM declaration renders a COLUMN_TYPE well past the 128
// chars #212's VARCHAR allowed, and under strict mode the resulting
// 1406 ("Data too long") aborted the ENTIRE snapshot transaction — not
// one column. column_type is TEXT now; capture must be byte-exact
// against information_schema's own rendering, including MySQL's
// backslash escaping inside members.
func TestTakeSnapshot_longEnumColumnType(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		status ENUM('pending_payment','payment_confirmed','awaiting_fulfillment','partially_shipped','shipped','out_for_delivery','delivered','return_requested','refund_processed','cancelled_by_customer') NOT NULL,
		path ENUM('a\\b','plain') NOT NULL
	) ENGINE=InnoDB`)

	stats, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot must survive a long ENUM declaration: %v", err)
	}

	for _, column := range []string{"status", "path"} {
		var want string
		if err := sourceDB.QueryRow(
			`SELECT COLUMN_TYPE FROM information_schema.COLUMNS
			 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'orders' AND COLUMN_NAME = ?`,
			sourceName, column,
		).Scan(&want); err != nil {
			t.Fatalf("read information_schema COLUMN_TYPE for %s: %v", column, err)
		}
		var got string
		if err := indexDB.QueryRow(
			`SELECT column_type FROM schema_snapshots
			 WHERE snapshot_id = ? AND table_name = 'orders' AND column_name = ?`,
			stats.SnapshotID, column,
		).Scan(&got); err != nil {
			t.Fatalf("read captured column_type for %s: %v", column, err)
		}
		if got != want {
			t.Errorf("column %s: captured %q, information_schema renders %q (must be byte-exact)", column, got, want)
		}
		if column == "status" && len(want) <= 128 {
			t.Fatalf("fixture regression: status COLUMN_TYPE is %d chars, must exceed the old VARCHAR(128) to pin the widening", len(want))
		}
	}
}

// TestWritePGSnapshot_OracleRoundTrip pins the #533 schema/type oracle: a PostgreSQL
// relation persisted via WritePGSnapshot reads back through the SAME metadata.Resolver
// the MySQL path uses — a composite PK in table-ordinal order, with the type OID/typmod
// round-tripped — and the pre-#212 UNSIGNED warning is suppressed for a PG snapshot
// (all data_type empty) while STILL firing for a genuine pre-#212 MySQL snapshot.
func TestWritePGSnapshot_OracleRoundTrip(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	rel := &PGRelationSchema{
		Schema: "public", Table: "orders",
		Columns: []PGRelationColumn{
			{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 23},                      // int4
			{Name: "region", Ordinal: 2, IsPK: true, TypeOID: 25},                  // text — composite PK part
			{Name: "amount", Ordinal: 3, IsPK: false, TypeOID: 1700, TypeMod: 100}, // numeric(p,s)
		},
	}
	id, err := WritePGSnapshot(context.Background(), indexDB, rel)
	if err != nil {
		t.Fatalf("WritePGSnapshot: %v", err)
	}
	if id <= 0 {
		t.Fatalf("snapshot_id = %d, want > 0", id)
	}

	// Load through the shared resolver; capture warnings — a PG snapshot must NOT trip
	// the MySQL-only pre-#212 UNSIGNED warning (all data_type empty is the PG signature).
	r := newResolverCapturingWarnings(t, indexDB, id, false /* wantWarning */)

	tm, err := r.Resolve("public", "orders")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	// Composite PK preserved in table-ordinal order [id, region].
	if got := tm.PKColumns; len(got) != 2 || got[0] != "id" || got[1] != "region" {
		t.Errorf("PKColumns = %v, want [id region] (table-ordinal order)", got)
	}
	if pks := tm.PKColumnMetas(); len(pks) != 2 || pks[0].Name != "id" || pks[1].Name != "region" {
		t.Errorf("PKColumnMetas = %+v, want id,region in order", pks)
	}

	// Type OID/typmod round-trip (read directly — slice-1 NewResolver does not surface them).
	var oid, mod sql.NullInt64
	if err := indexDB.QueryRow(
		`SELECT pg_type_oid, pg_type_mod FROM schema_snapshots
		 WHERE snapshot_id=? AND column_name='amount'`, id,
	).Scan(&oid, &mod); err != nil {
		t.Fatalf("select pg_type cols: %v", err)
	}
	if !oid.Valid || oid.Int64 != 1700 || !mod.Valid || mod.Int64 != 100 {
		t.Errorf("amount pg_type_oid/mod = (%v,%v), want (1700,100)", oid, mod)
	}

	// Control: a genuine pre-#212 MySQL snapshot (non-empty data_type, NULL column_type)
	// STILL trips the warning — the gate suppresses ONLY the all-empty-data_type PG case.
	mysqlID := id + 1
	if _, err := indexDB.Exec(
		`INSERT INTO schema_snapshots
		   (snapshot_id, snapshot_time, schema_name, table_name, column_name,
		    ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		 VALUES (?, UTC_TIMESTAMP(), 'app', 'widgets', 'qty', 1, 'PRI', 'int', NULL, 'NO', 0)`,
		mysqlID,
	); err != nil {
		t.Fatalf("insert pre-#212 MySQL snapshot: %v", err)
	}
	newResolverCapturingWarnings(t, indexDB, mysqlID, true /* wantWarning */)
}

// newResolverCapturingWarnings loads a snapshot with slog redirected to a buffer and
// asserts whether the pre-#212 UNSIGNED warning fired.
func newResolverCapturingWarnings(t *testing.T, db *sql.DB, snapshotID int, wantWarning bool) *Resolver {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	r, err := NewResolver(db, snapshotID)
	slog.SetDefault(old)
	if err != nil {
		t.Fatalf("NewResolver(%d): %v", snapshotID, err)
	}
	got := strings.Contains(buf.String(), "#212")
	if got != wantWarning {
		t.Errorf("pre-#212 UNSIGNED warning fired=%v, want %v (snapshot %d):\n%s", got, wantWarning, snapshotID, buf.String())
	}
	return r
}

// TestWritePGSnapshot_concurrentAllocatorSerialized pins the snapshot_id
// allocator's concurrency contract (#844): N truly concurrent snapshot
// writers must all succeed, each get a distinct snapshot_id, and never merge
// rows under one id (which would double every table's columns in the
// resolver and make it skip ALL events of those tables as "column count
// mismatch").
//
// This subsumes an earlier version of this test that asserted a second
// writer *blocks* behind an uncommitted one holding a `MAX(snapshot_id)+1
// FOR UPDATE` lock. That FOR UPDATE design was replaced (still under #844)
// because it reliably deadlocked (MySQL Error 1213) under 3+ concurrent
// writers — see DDLSnapshotIDSeq. The new snapshot_id_seq AUTO_INCREMENT
// allocator deliberately does NOT block one writer behind another (InnoDB's
// AUTO_INCREMENT lock is held only for the allocating statement, not the
// whole transaction), so "blocks behind an uncommitted writer" is no longer
// the right invariant to test; "never collides, never deadlocks" is.
func TestWritePGSnapshot_concurrentAllocatorSerialized(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	const writers = 8
	ids := make([]int, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], errs[i] = WritePGSnapshot(context.Background(), indexDB, &PGRelationSchema{
				Schema: "pgdb", Table: fmt.Sprintf("pgtable_%d", i),
				Columns: []PGRelationColumn{{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 23, TypeMod: -1}},
			})
		}(i)
	}
	wg.Wait()

	seen := make(map[int]bool, writers)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("WritePGSnapshot[%d]: %v (concurrent allocation must never error, including deadlock)", i, err)
		}
		if seen[ids[i]] {
			t.Fatalf("snapshot_id collision: %d allocated more than once across %d concurrent writers", ids[i], writers)
		}
		seen[ids[i]] = true
	}
}

// TestTakeSnapshot_concurrentAllocatorSerialized is the MySQL-path sibling of
// TestWritePGSnapshot_concurrentAllocatorSerialized: TakeSnapshot's allocator
// must hold the same concurrency contract (#844) — see that test's doc
// comment for why this no longer asserts blocking behavior.
func TestTakeSnapshot_concurrentAllocatorSerialized(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		status VARCHAR(20) NOT NULL
	) ENGINE=InnoDB`)

	const writers = 8
	stats := make([]SnapshotStats, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stats[i], errs[i] = TakeSnapshot(sourceDB, indexDB, []string{sourceName})
		}(i)
	}
	wg.Wait()

	seen := make(map[int]bool, writers)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("TakeSnapshot[%d]: %v (concurrent allocation must never error, including deadlock)", i, err)
		}
		id := stats[i].SnapshotID
		if seen[id] {
			t.Fatalf("snapshot_id collision: %d allocated more than once across %d concurrent writers", id, writers)
		}
		seen[id] = true

		// Each writer's snapshot must hold exactly its own row set — no merge
		// with any other writer's rows under the same snapshot_id.
		var rowCount int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM schema_snapshots WHERE snapshot_id = ? AND schema_name = ?", id, sourceName).Scan(&rowCount); err != nil {
			t.Fatalf("count rows for snapshot_id %d: %v", id, err)
		}
		if rowCount != 2 { // orders.id + orders.status
			t.Errorf("snapshot_id %d holds %d rows, want 2 (no merge with another concurrent writer)", id, rowCount)
		}
	}
}

// ─── #1033: corrupt snapshot with duplicated column rows ─────────────────────

func TestNewResolver_dedupesDuplicatedSnapshotRows(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Pre-#844 concurrent writers sharing one snapshot_id: every column
	// double-inserted. The resolver must report the real column count, or the
	// parser's column-count guard skips 100% of row events (#1033).
	for range 2 {
		testutil.InsertSnapshot(t, indexDB, 12, "2026-07-04 15:02:39", "mydb", "wp_options", "option_id", 1, "PRI", "bigint", "NO")
		testutil.InsertSnapshot(t, indexDB, 12, "2026-07-04 15:02:39", "mydb", "wp_options", "option_name", 2, "", "varchar", "NO")
		testutil.InsertSnapshot(t, indexDB, 12, "2026-07-04 15:02:39", "mydb", "wp_options", "option_value", 3, "", "longtext", "NO")
		testutil.InsertSnapshot(t, indexDB, 12, "2026-07-04 15:02:39", "mydb", "wp_options", "autoload", 4, "", "varchar", "NO")
	}

	resolver, err := NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}
	tm, err := resolver.Resolve("mydb", "wp_options")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(tm.Columns) != 4 {
		t.Errorf("expected 4 deduplicated columns, got %d", len(tm.Columns))
	}
	if len(tm.PKColumns) != 1 {
		t.Errorf("expected 1 PK column, got %v", tm.PKColumns)
	}
}

func TestNewLatestPerTableResolver_dedupesDuplicatedSnapshotRows(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	for range 2 {
		testutil.InsertSnapshot(t, indexDB, 12, "2026-07-04 15:02:39", "mydb", "wp_options", "option_id", 1, "PRI", "bigint", "NO")
		testutil.InsertSnapshot(t, indexDB, 12, "2026-07-04 15:02:39", "mydb", "wp_options", "option_name", 2, "", "varchar", "NO")
	}

	resolver, err := NewLatestPerTableResolver(indexDB)
	if err != nil {
		t.Fatalf("NewLatestPerTableResolver failed: %v", err)
	}
	tm, err := resolver.Resolve("mydb", "wp_options")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(tm.Columns) != 2 {
		t.Errorf("expected 2 deduplicated columns, got %d", len(tm.Columns))
	}
}

func TestNewResolver_conflictingDuplicateRowsFailLoudIntegration(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.InsertSnapshot(t, indexDB, 12, "2026-07-04 15:02:39", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 12, "2026-07-04 15:02:39", "mydb", "orders", "name", 2, "", "varchar", "NO")
	testutil.InsertSnapshot(t, indexDB, 12, "2026-07-04 15:02:39", "mydb", "orders", "renamed", 2, "", "varchar", "NO")

	_, err := NewResolver(indexDB, 0)
	if err == nil {
		t.Fatal("expected error for conflicting duplicate ordinal rows, got nil")
	}
	if !strings.Contains(err.Error(), "corrupt") {
		t.Errorf("error should say the snapshot is corrupt, got: %v", err)
	}
}
