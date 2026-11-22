//go:build integration

package icebergexport

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const resolveRefusal3110 = "in the schema snapshot"

// wantPGRefusal3110 is the refusal a found table meets next: the empty
// baseline directory.
const wantPGRefusal3110 = "no usable baseline snapshot for public."

// run3110 runs the export of one table against an index and returns its
// outcome's detail.
func run3110(t *testing.T, dsn, table string) Outcome {
	t.Helper()
	return runOne(t, Config{
		IndexDSN:       dsn,
		BaselineSrc:    t.TempDir(),
		Warehouse:      t.TempDir(),
		Tables:         []string{table},
		At:             time.Now().UTC(),
		ArchiveFetcher: parquetquery.Fetch,
	})
}

// #3110: on a PostgreSQL index (one table per schema snapshot) the export
// finds a table that was not the last one saved, instead of refusing it as
// absent from the schema snapshot.
func TestIntegrationExport_PGFindsEveryTable_3110(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range []string{"users", "orders", "items"} {
		rel := &metadata.PGRelationSchema{Schema: "public", Table: tbl, Columns: []metadata.PGRelationColumn{
			{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 23, TypeMod: -1},
			{Name: "name", Ordinal: 2, TypeOID: 25, TypeMod: -1},
		}}
		if _, err := metadata.WritePGSnapshot(context.Background(), db, rel); err != nil {
			t.Fatal(err)
		}
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, source_health) VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1, '{}')`)

	for _, tbl := range []string{"users", "orders"} {
		o := run3110(t, testutil.IntegrationDSN(dbName), "public."+tbl)
		if strings.Contains(o.Detail, resolveRefusal3110) {
			t.Errorf("public.%s refused as absent from the schema snapshot: %s", tbl, o.Detail)
		}
		if o.Err == nil || !strings.Contains(o.Detail, wantPGRefusal3110) {
			t.Errorf("public.%s: %q; want the refusal past the schema lookup (%q)", tbl, o.Detail, wantPGRefusal3110)
		}
	}
}

// #3110: on a MySQL or MariaDB index the export resolves tables exactly as
// before: a table dropped before the newest snapshot is still refused as
// absent from it, which the per-table loader would not do.
func TestIntegrationExport_MySQLResolveUnchanged_3110(t *testing.T) {
	for _, stream := range []string{"mysql", "mariadb", ""} {
		t.Run("stream="+stream, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatal(err)
			}
			testutil.MustExec(t, db, `CREATE TABLE gone (id INT NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
			testutil.MustExec(t, db, `CREATE TABLE kept (id INT NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
			if _, err := metadata.TakeSnapshot(db, db, []string{dbName}); err != nil {
				t.Fatal(err)
			}
			testutil.MustExec(t, db, `DROP TABLE gone`)
			if _, err := metadata.TakeSnapshot(db, db, []string{dbName}); err != nil {
				t.Fatal(err)
			}
			if stream != "" {
				testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, source_health) VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1, '{}')`, stream)
			}
			o := run3110(t, testutil.IntegrationDSN(dbName), dbName+".gone")
			if !strings.Contains(o.Detail, "resolve "+dbName+".gone "+resolveRefusal3110) {
				t.Errorf("the dropped table: %q; want it refused as absent from the newest snapshot", o.Detail)
			}
		})
	}
}
