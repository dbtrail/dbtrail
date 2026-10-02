//go:build integration

package indexer_test

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2007: every --pk lookup reads a table's newest snapshot by
// (schema_name, table_name); without an index leading with those columns
// that read scans schema_snapshots. EnsureSchema adds it, idempotently.
func TestEnsureSchema_snapshotLookupIndex_2007(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	for i := 0; i < 2; i++ {
		if err := indexer.EnsureSchema(db); err != nil {
			t.Fatalf("EnsureSchema run %d: %v", i+1, err)
		}
	}
	var cols string
	if err := db.QueryRow(`SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'schema_snapshots' AND INDEX_NAME = 'idx_table_snapshot'`).Scan(&cols); err != nil {
		t.Fatal(err)
	}
	if cols != "schema_name,table_name,snapshot_id" {
		t.Fatalf("idx_table_snapshot columns = %q, want schema_name,table_name,snapshot_id", cols)
	}
}
