//go:build integration

package indexer_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

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

// The index is performance-only: an account that may read and write the
// index but not ALTER it (a read-only console, an MCP server) must keep
// working. Before, EnsureSchema returned the 1142 and every command that
// runs it failed.
func TestEnsureSchema_snapshotLookupIndexWithoutAlter_2007(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, db, "ALTER TABLE schema_snapshots DROP INDEX idx_table_snapshot")

	user := fmt.Sprintf("noalter_%d", time.Now().UnixNano()%1_000_000)
	testutil.MustExec(t, db, fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY 'noalter-pass'", user))
	t.Cleanup(func() { db.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user)) })
	testutil.MustExec(t, db, fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON `%s`.* TO '%s'@'%%'", name, user))

	cfg, err := drivermysql.ParseDSN(testutil.IntegrationDSN(name))
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Passwd = user, "noalter-pass"
	limited, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	if err := indexer.EnsureSchema(limited); err != nil {
		t.Fatalf("EnsureSchema without ALTER must warn and continue, got: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE()
		AND TABLE_NAME = 'schema_snapshots' AND INDEX_NAME = 'idx_table_snapshot'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("test premise: the index must still be missing (n=%d, err=%v)", n, err)
	}
}
