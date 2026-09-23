//go:build integration

package indexer

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1815: EnsureSchema is the daemon's migration point, so an index an older
// build created must come out of it with binary name columns on
// snapshot_exclusions, its rows intact, and a second run must be harmless.
func TestEnsureSchemaMigratesExclusionNameCollation_1815(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	if err := CreateIndexTables(context.Background(), db, 2, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	testutil.MustExec(t, db, "DROP TABLE IF EXISTS snapshot_exclusions")
	testutil.MustExec(t, db, `CREATE TABLE snapshot_exclusions (
		snapshot_id INT UNSIGNED NOT NULL,
		schema_name VARCHAR(64)  NOT NULL,
		table_name  VARCHAR(64)  NOT NULL,
		reason      VARCHAR(64)  NOT NULL,
		PRIMARY KEY (snapshot_id, schema_name, table_name)
	) ENGINE=InnoDB`)
	testutil.MustExec(t, db, "INSERT INTO snapshot_exclusions VALUES (1, 'shop', 'Audit_Log', 'no primary key')")

	for run := 1; run <= 2; run++ {
		if err := EnsureSchema(db); err != nil {
			t.Fatalf("EnsureSchema run %d: %v", run, err)
		}
		for _, col := range []string{"schema_name", "table_name"} {
			var collation string
			if err := db.QueryRow(`SELECT COLLATION_NAME FROM information_schema.COLUMNS
				WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions' AND COLUMN_NAME = ?`, col,
			).Scan(&collation); err != nil {
				t.Fatal(err)
			}
			if collation != metadata.SnapshotExclusionsNameCollation {
				t.Errorf("run %d: %s collation = %s, want %s", run, col, collation, metadata.SnapshotExclusionsNameCollation)
			}
		}
	}
	// The twins now fit, and the old row is still there.
	testutil.MustExec(t, db, "INSERT INTO snapshot_exclusions (snapshot_id, schema_name, table_name, reason) VALUES (1, 'shop', 'audit_log', 'no primary key')")
	testutil.MustExec(t, db, "INSERT INTO snapshot_exclusions (snapshot_id, schema_name, table_name, reason) VALUES (1, 'shop', 'àudit_log', 'no primary key')")
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM snapshot_exclusions WHERE snapshot_id = 1").Scan(&n); err != nil || n != 3 {
		t.Fatalf("rows = %d, %v; want 3", n, err)
	}
}

// A fresh index gets the binary columns from the DDL itself.
func TestEnsureSchemaCreatesExclusionsWithBinaryNames_1815(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	if err := CreateIndexTables(context.Background(), db, 2, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.MustExec(t, db, "INSERT INTO snapshot_exclusions (snapshot_id, schema_name, table_name, reason) VALUES (1, 'shop', 'Audit_Log', 'no primary key')")
	testutil.MustExec(t, db, "INSERT INTO snapshot_exclusions (snapshot_id, schema_name, table_name, reason) VALUES (1, 'shop', 'audit_log', 'no primary key')")
}
