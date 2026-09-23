//go:build integration

package status_test

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// These go through the REAL snapshot writer (#1802): hand-inserted exclusion
// rows could not prove that only the CURRENT snapshot's exclusions are read,
// because the point is precisely that the old rows stay on disk.

func uncapturedIndex(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	return db
}

func names(us []status.UncapturedTable) []string {
	out := make([]string, len(us))
	for i, u := range us {
		out[i] = u.Schema + "." + u.Table + " (" + u.Kind() + ")"
	}
	return out
}

// TestIntegrationLoadTableCapture_fixedTableDisappears: every kind of
// excluded table is listed with a fix; running each fix on the source and
// re-reading the schema makes the list empty while the OLD snapshot's rows are
// still in snapshot_exclusions. The fixes run as-is against MySQL, so the
// quoting of an uppercase name, a name with a space and one with a backtick is
// proven by the server, not by a string comparison.
func TestIntegrationLoadTableCapture_fixedTableDisappears(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	source, src := testutil.CreateTestDB(t)
	index := uncapturedIndex(t)

	for _, ddl := range []string{
		"CREATE TABLE orders (id INT PRIMARY KEY) ENGINE=InnoDB",
		// The commonest shape of a key-less table: a plain non-key `id`
		// column. An ALTER that adds one named `id` fails with ERROR 1060,
		// so the snapshot must pick a name the table does not use (#1802).
		"CREATE TABLE events (id VARCHAR(20), payload TEXT) ENGINE=InnoDB",
		"CREATE TABLE taken (id INT, dbtrail_id INT, DBTRAIL_ID_2 INT) ENGINE=InnoDB",
		// Two tables whose names differ only in case, both captured: the
		// index collation folds case, so a SQL DISTINCT would count one.
		"CREATE TABLE `Customers` (id INT PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE `customers` (id INT PRIMARY KEY) ENGINE=InnoDB",
		// A view has columns but no rows to capture: never a table here.
		"CREATE VIEW v_orders AS SELECT id FROM orders",
		"CREATE TABLE audit_log (happened_at DATETIME, action VARCHAR(20)) ENGINE=InnoDB",
		"CREATE TABLE legacy_pk (id INT PRIMARY KEY) ENGINE=MyISAM",
		"CREATE TABLE legacy_nopk (v INT) ENGINE=MyISAM",
		"CREATE TABLE `Audit Log` (v INT) ENGINE=InnoDB",
		"CREATE TABLE `odd``name` (v INT) ENGINE=InnoDB",
	} {
		testutil.MustExec(t, source, ddl)
	}

	first, err := metadata.TakeSnapshotExcludingInvalid(source, index, []string{src})
	if err != nil {
		t.Fatalf("TakeSnapshotExcludingInvalid: %v", err)
	}
	c := status.LoadTableCapture(ctx, index)
	if c.State != status.TableCaptureChecked || c.SnapshotID != first.SnapshotID {
		t.Fatalf("capture = %+v, want checked on snapshot %d", c, first.SnapshotID)
	}
	if c.TablesCaptured() != 3 {
		t.Errorf("captured = %d (%v), want 3: orders, Customers, customers (the view is not a table)", c.TablesCaptured(), c.Captured)
	}
	if c.CoverageKnown {
		t.Error("a capture loaded straight from the index must claim no coverage until a filter says it may")
	}
	c = c.WithFilter(status.CaptureFilter{Known: true})
	want := []string{
		src + ".Audit Log (no_primary_key)",
		src + ".audit_log (no_primary_key)",
		src + ".events (no_primary_key)",
		src + ".legacy_nopk (not_innodb_no_primary_key)",
		src + ".legacy_pk (not_innodb)",
		src + ".odd`name (no_primary_key)",
		src + ".taken (no_primary_key)",
	}
	if got := names(c.Uncaptured); !slices.Equal(got, want) {
		t.Fatalf("uncaptured = %q\nwant        %q", got, want)
	}
	if c.TablesTotal() != 10 {
		t.Errorf("total = %d, want 10", c.TablesTotal())
	}
	// The column each fix adds is one the table does not already have. A
	// table that only sits on the wrong engine keeps the key it has, so it
	// gets no column at all.
	pkColumns := map[string]string{}
	for _, u := range c.Uncaptured {
		pkColumns[u.Table] = u.PKColumn
	}
	for table, want := range map[string]string{
		"audit_log": "id", "events": "dbtrail_id", "taken": "dbtrail_id_3", "legacy_pk": "",
	} {
		if pkColumns[table] != want {
			t.Errorf("%s: pk column = %q, want %q", table, pkColumns[table], want)
		}
	}

	// Run every fix exactly as the report prints it.
	for _, u := range c.Uncaptured {
		fix := u.FixSQL()
		if fix == "" {
			t.Fatalf("%s.%s has no fix", u.Schema, u.Table)
		}
		if _, err := source.ExecContext(ctx, fix); err != nil {
			t.Fatalf("the printed fix for %s.%s does not run: %v\n%s", u.Schema, u.Table, err, fix)
		}
	}

	second, err := metadata.TakeSnapshotExcludingInvalid(source, index, []string{src})
	if err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	if len(second.ExcludedTables) != 0 {
		t.Fatalf("after every fix the snapshot still excludes %v", second.ExcludedTables)
	}
	c = status.LoadTableCapture(ctx, index)
	if c.State != status.TableCaptureChecked || c.SnapshotID != second.SnapshotID || len(c.Uncaptured) != 0 {
		t.Fatalf("after the re-read: %+v, want checked, snapshot %d, nothing uncaptured", c, second.SnapshotID)
	}
	if c.TablesCaptured() != 10 || c.TablesTotal() != 10 {
		t.Errorf("after the re-read: captured %d of %d, want 10 of 10", c.TablesCaptured(), c.TablesTotal())
	}
	// The first snapshot's exclusions are still on disk: the list emptied
	// because it reads the current snapshot, not because rows were removed.
	var old int
	if err := index.QueryRowContext(ctx, "SELECT COUNT(*) FROM snapshot_exclusions WHERE snapshot_id = ?", first.SnapshotID).Scan(&old); err != nil {
		t.Fatal(err)
	}
	if old != 7 {
		t.Errorf("the first snapshot's exclusion rows = %d, want 7 still recorded", old)
	}
}

// TestIntegrationLoadTableCapture_legacyIndexNotChecked: an index without
// snapshot_exclusions cannot say which tables were left out, so it says "not
// checked", never "all captured", while still knowing what it captures.
func TestIntegrationLoadTableCapture_legacyIndexNotChecked(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	source, src := testutil.CreateTestDB(t)
	index, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, index, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	testutil.MustExec(t, source, "CREATE TABLE orders (id INT PRIMARY KEY) ENGINE=InnoDB")
	// The strict snapshot with nothing to exclude never creates the table:
	// the shape of an index last written before #1051.
	if _, err := metadata.TakeSnapshot(source, index, []string{src}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	var present int
	if err := index.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions'").Scan(&present); err != nil {
		t.Fatal(err)
	}
	if present != 0 {
		t.Fatal("fixture: snapshot_exclusions exists, so this is not a legacy index")
	}
	c := status.LoadTableCapture(ctx, index)
	if c.State != status.TableCaptureNotChecked {
		t.Fatalf("legacy index: state = %q, want not_checked (%+v)", c.State, c)
	}
	if c.TablesCaptured() != 1 || len(c.Uncaptured) != 0 {
		t.Errorf("legacy index: %+v, want 1 captured and no list", c)
	}
}

// TestIntegrationLoadTableCapture_noSnapshot: nothing read yet, including an
// index whose schema_snapshots table is not there at all.
func TestIntegrationLoadTableCapture_noSnapshot(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	index := uncapturedIndex(t)
	if c := status.LoadTableCapture(ctx, index); c.State != status.TableCaptureNoSnapshot {
		t.Errorf("empty index: %+v, want no_snapshot", c)
	}
	bare, _ := testutil.CreateTestDB(t)
	if c := status.LoadTableCapture(ctx, bare); c.State != status.TableCaptureNoSnapshot {
		t.Errorf("index with no schema_snapshots table: %+v, want no_snapshot", c)
	}
}

// TestIntegrationLoadTableCapture_postgresNotApplicable: a PostgreSQL index
// writes one snapshot PER TABLE, so its newest snapshot names a single table
// and a count from it would claim "1 of 1" on a healthy install.
func TestIntegrationLoadTableCapture_postgresNotApplicable(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	index := uncapturedIndex(t)
	for _, tbl := range []string{"orders", "customers"} {
		if _, err := metadata.WritePGSnapshot(ctx, index, &metadata.PGRelationSchema{
			Schema: "public", Table: tbl,
			Columns: []metadata.PGRelationColumn{{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 23}},
		}); err != nil {
			t.Fatalf("WritePGSnapshot: %v", err)
		}
	}
	c := status.LoadTableCapture(ctx, index)
	if c.State != status.TableCaptureNotApplicable {
		t.Errorf("PostgreSQL index: %+v, want not_applicable", c)
	}
	if strings.Contains(c.State, "checked") {
		t.Errorf("a PostgreSQL index must never read as checked")
	}
}

// TestIntegrationSchemaFilterMustMatchTheSnapshot (#1802 round 4): the gate
// that withdraws a count runs against a REAL snapshot, written by the real
// snapshot taker, so the names it compares are the ones MySQL actually
// recorded — not a fixture's idea of them. Capture matches its --schemas
// entries byte for byte, so an entry that differs from the recorded schema
// only in case describes nothing that is being watched.
func TestIntegrationSchemaFilterMustMatchTheSnapshot(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	source, src := testutil.CreateTestDB(t)
	index := uncapturedIndex(t)
	testutil.MustExec(t, source, "CREATE TABLE orders (id INT PRIMARY KEY) ENGINE=InnoDB")
	testutil.MustExec(t, source, "CREATE TABLE audit_log (v INT) ENGINE=InnoDB")
	if _, err := metadata.TakeSnapshotExcludingInvalid(source, index, []string{src}); err != nil {
		t.Fatalf("TakeSnapshotExcludingInvalid: %v", err)
	}
	c := status.LoadTableCapture(ctx, index)

	exact := c.WithFilter(status.CaptureFilter{Known: true, Schemas: []string{src}})
	if !exact.CoverageKnown || exact.TablesCaptured() != 1 || exact.TablesTotal() != 2 {
		t.Errorf("the schema as the snapshot recorded it must count: known=%v %d of %d",
			exact.CoverageKnown, exact.TablesCaptured(), exact.TablesTotal())
	}

	miscased := c.WithFilter(status.CaptureFilter{Known: true, Schemas: []string{strings.ToUpper(src)}})
	if miscased.CoverageKnown {
		t.Error("a schema spelled differently from the snapshot's own record claimed a count")
	}
	if len(miscased.Uncaptured) != 1 || miscased.Uncaptured[0].Table != "audit_log" {
		t.Errorf("the table left out must stay listed whatever the filter's case: %+v", miscased.Uncaptured)
	}
	if miscased.View(nil).CoverageNote == "" {
		t.Error("the withdrawn count must say why")
	}
}
