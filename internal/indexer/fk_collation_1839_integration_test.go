//go:build integration

package indexer

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func fkSchemaNameCollation(t *testing.T, db *sql.DB) string {
	t.Helper()
	var collation string
	if err := db.QueryRow(`SELECT COLLATION_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'fk_constraints' AND COLUMN_NAME = 'schema_name'`,
	).Scan(&collation); err != nil {
		t.Fatal(err)
	}
	return collation
}

// #1839: EnsureSchema is the daemon's migration point, so an index an older
// build created must come out of it with a binary fk_constraints.schema_name,
// its rows intact, and a second run must be harmless.
func TestEnsureSchemaMigratesFKSchemaNameCollation_1839(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	if err := CreateIndexTables(context.Background(), db, 2, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	testutil.MustExec(t, db, "DROP TABLE IF EXISTS fk_constraints")
	testutil.MustExec(t, db, `CREATE TABLE fk_constraints (
		snapshot_id              INT UNSIGNED NOT NULL,
		constraint_name          VARCHAR(64)  NOT NULL,
		schema_name              VARCHAR(64)  NOT NULL,
		table_name               VARCHAR(64)  NOT NULL,
		column_name              VARCHAR(64)  NOT NULL,
		ordinal_position         INT          NOT NULL,
		referenced_schema_name   VARCHAR(64)  NOT NULL,
		referenced_table_name    VARCHAR(64)  NOT NULL,
		referenced_column_name   VARCHAR(64)  NOT NULL,
		PRIMARY KEY (snapshot_id, schema_name, constraint_name, ordinal_position)
	) ENGINE=InnoDB`)
	testutil.MustExec(t, db, "INSERT INTO fk_constraints VALUES (1, 'fk_x', 'shop', 'child', 'pid', 1, 'shop', 'parent', 'id')")

	for run := 1; run <= 2; run++ {
		if err := EnsureSchema(db); err != nil {
			t.Fatalf("EnsureSchema run %d: %v", run, err)
		}
		if got := fkSchemaNameCollation(t, db); got != metadata.FKConstraintsNameCollation {
			t.Errorf("run %d: schema_name collation = %s, want %s", run, got, metadata.FKConstraintsNameCollation)
		}
	}
	// The twins now fit, and the old row is still there.
	testutil.MustExec(t, db, "INSERT INTO fk_constraints (snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position, referenced_schema_name, referenced_table_name, referenced_column_name) VALUES (1, 'fk_x', 'Shop', 'child', 'pid', 1, 'Shop', 'parent', 'id')")
	testutil.MustExec(t, db, "INSERT INTO fk_constraints (snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position, referenced_schema_name, referenced_table_name, referenced_column_name) VALUES (1, 'fk_x', 'shöp', 'child', 'pid', 1, 'shöp', 'parent', 'id')")
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM fk_constraints WHERE snapshot_id = 1").Scan(&n); err != nil || n != 3 {
		t.Fatalf("rows = %d, %v; want 3", n, err)
	}
}

// A fresh index gets the binary column from the DDL itself.
func TestCreateIndexTablesFKSchemaNameIsBinary_1839(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	if err := CreateIndexTables(context.Background(), db, 2, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if got := fkSchemaNameCollation(t, db); got != metadata.FKConstraintsNameCollation {
		t.Fatalf("schema_name collation = %s, want %s", got, metadata.FKConstraintsNameCollation)
	}
}
