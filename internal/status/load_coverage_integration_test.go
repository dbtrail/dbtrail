//go:build integration

package status_test

import (
	"context"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestLoadCoverage_uncoveredDDLs_integration proves the UncoveredDDLs counter
// against the real schema_changes rows the capture paths write (#1049):
//   - a TRUNCATE TABLE row records snapshot_id = NULL by design (no structure
//     change; every mode deliberately skips the snapshot) and must NOT count;
//   - an ALTER TABLE row with snapshot_id = NULL (file mode without
//     --source-dsn, or a failed auto-snapshot) MUST count;
//   - an ALTER TABLE row with a snapshot_id must not count.
func TestLoadCoverage_uncoveredDDLs_integration(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	insert := `INSERT INTO schema_changes
		(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
		VALUES (?, 'mysql-bin.000001', ?, 'shop', ?, ?, ?, ?)`

	// By-design NULL: TRUNCATE skips the auto-snapshot in every mode.
	testutil.MustExec(t, db, insert,
		"2026-02-18 10:00:00", 100, "orders", "TRUNCATE TABLE", "TRUNCATE TABLE orders", nil)
	// Genuinely uncovered: an ALTER whose auto-snapshot was skipped or failed.
	testutil.MustExec(t, db, insert,
		"2026-02-18 11:00:00", 200, "orders", "ALTER TABLE", "ALTER TABLE orders ADD COLUMN note TEXT", nil)
	// Covered: an ALTER with its auto-snapshot recorded.
	testutil.MustExec(t, db, insert,
		"2026-02-18 12:00:00", 300, "users", "ALTER TABLE", "ALTER TABLE users ADD COLUMN note TEXT", 5)
	// One uncovered statement naming two tables: a row per table, each counted,
	// as list_schema_changes lists each (#1436).
	for _, tbl := range []string{"tmp", "old_orders"} {
		testutil.MustExec(t, db, insert,
			"2026-02-18 13:00:00", 400, tbl, "DROP TABLE", "DROP TABLE tmp, old_orders", nil)
	}

	coverage, err := status.LoadCoverage(context.Background(), db)
	if err != nil {
		t.Fatalf("LoadCoverage failed: %v", err)
	}

	if coverage.SchemaChanges != 5 {
		t.Errorf("SchemaChanges = %d, want 5 (a row per table: TRUNCATE included, the two-table DROP twice)", coverage.SchemaChanges)
	}
	if coverage.UncoveredDDLs != 3 {
		t.Errorf("UncoveredDDLs = %d, want 3 (the NULL-snapshot ALTER and both tables of the DROP; TRUNCATE is by design, snapshot-carrying rows are covered)",
			coverage.UncoveredDDLs)
	}
}
