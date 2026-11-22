//go:build integration

package cli

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func pgRel3110(table string, cols ...string) *metadata.PGRelationSchema {
	rel := &metadata.PGRelationSchema{Schema: "public", Table: table,
		Columns: []metadata.PGRelationColumn{{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 23, TypeMod: -1}}}
	for i, c := range cols {
		rel.Columns = append(rel.Columns, metadata.PGRelationColumn{Name: c, Ordinal: i + 2, TypeOID: 25, TypeMod: -1})
	}
	return rel
}

// seedPGShapes3110 is a PostgreSQL index as the capture writes it: one table
// per snapshot, orders saved with legacy and again without it, then two other
// tables saved after. A DELETE on orders is stamped with the snapshot that
// still had legacy.
func seedPGShapes3110(t *testing.T) string {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ctx := context.Background()
	first, err := metadata.WritePGSnapshot(ctx, db, pgRel3110("orders", "status", "legacy"))
	if err != nil {
		t.Fatal(err)
	}
	for _, rel := range []*metadata.PGRelationSchema{pgRel3110("orders", "status"), pgRel3110("users", "name"), pgRel3110("items", "name")} {
		if _, err := metadata.WritePGSnapshot(ctx, db, rel); err != nil {
			t.Fatal(err)
		}
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, source_health) VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1, '{}')`)
	testutil.InsertEvent(t, db, "0/100", 0x100, 0x100, "2026-11-21 10:00:00", nil, "public", "orders", 3, "1",
		nil, []byte(`{"id":1,"status":"paid","legacy":"x"}`), nil)
	testutil.MustExec(t, db, `UPDATE binlog_events SET schema_version = ? WHERE table_name = 'orders'`, first)
	return testutil.IntegrationDSN(dbName)
}

// seedMySQLDroppedTable3110 is a MySQL index where the two loaders disagree:
// "gone" was dropped before the newest snapshot, and has an UPDATE indexed.
// The MySQL path does not know it (all-column WHERE in an undo, a refused key
// range); the per-table loader would.
func seedMySQLDroppedTable3110(t *testing.T, stream string) (*sql.DB, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.MustExec(t, db, `CREATE TABLE gone (id INT NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
	testutil.MustExec(t, db, `CREATE TABLE kept (id INT NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
	if _, err := metadata.TakeSnapshot(db, db, []string{dbName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	testutil.MustExec(t, db, `DROP TABLE gone`)
	if _, err := metadata.TakeSnapshot(db, db, []string{dbName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	if stream != "" {
		testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, source_health) VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1, '{}')`, stream)
	}
	testutil.InsertEvent(t, db, "bin.000001", 100, 150, "2026-06-01 12:00:00", nil, dbName, "gone", 2, "7",
		[]byte(`["v"]`), []byte(`{"id":7,"v":1}`), []byte(`{"id":7,"v":2}`))
	return db, dbName
}

// scriptBody3110 is the statements of a reversal script, BEGIN to COMMIT:
// the header carries the generation time.
func scriptBody3110(t *testing.T, s string) string {
	t.Helper()
	i := strings.Index(s, "BEGIN;")
	j := strings.LastIndex(s, "COMMIT;")
	if i < 0 || j < i {
		t.Fatalf("no BEGIN/COMMIT in the script:\n%s", s)
	}
	return s[i : j+len("COMMIT;")]
}

func runRecover3110(t *testing.T, dsn, schema, table string) (string, error) {
	t.Helper()
	saved := struct {
		dsn, s, tbl, format string
		dry                 bool
	}{rIndexDSN, rSchema, rTable, rFormat, rDryRun}
	t.Cleanup(func() {
		rIndexDSN, rSchema, rTable, rFormat, rDryRun = saved.dsn, saved.s, saved.tbl, saved.format, saved.dry
	})
	rIndexDSN, rSchema, rTable, rFormat, rDryRun = dsn, schema, table, "text", true
	recoverCmd.SetContext(context.Background())
	var runErr error
	out := captureStdout(t, func() { runErr = runRecover(recoverCmd, nil) })
	return out, runErr
}

func runQueryPKMin3110(t *testing.T, dsn, schema, table string) error {
	t.Helper()
	saved := struct{ dsn, format, s, tbl, min, max string }{qIndexDSN, qFormat, qSchema, qTable, qPKMin, qPKMax}
	t.Cleanup(func() {
		qIndexDSN, qFormat, qSchema, qTable, qPKMin, qPKMax = saved.dsn, saved.format, saved.s, saved.tbl, saved.min, saved.max
	})
	qIndexDSN, qFormat, qSchema, qTable, qPKMin, qPKMax = dsn, "json", schema, table, "1", ""
	queryCmd.SetContext(context.Background())
	var runErr error
	captureStdout(t, func() { runErr = runQuery(queryCmd, nil) })
	return runErr
}

// #3110: recover on a PostgreSQL index checks the undo against the table's
// own newest shape, so the dropped column is refused instead of written.
func TestIntegrationPGRecoverRefusesDroppedColumn_3110(t *testing.T) {
	dsn := seedPGShapes3110(t)
	out, err := runRecover3110(t, dsn, "public", "orders")
	if err == nil || !strings.Contains(err.Error(), "legacy") {
		t.Fatalf("recover wrote an undo with the dropped column instead of refusing: err=%v\n%s", err, out)
	}
	if strings.Contains(out, "legacy") {
		t.Errorf("the refused script reached stdout:\n%s", out)
	}
}

// #3110: --pk-min/--pk-max on a PostgreSQL index finds a table that was not
// the last one saved, and gives the PostgreSQL refusal instead of "not found
// in snapshot".
func TestIntegrationPGQueryPKRangeFindsEveryTable_3110(t *testing.T) {
	dsn := seedPGShapes3110(t)
	for _, tbl := range []string{"orders", "users", "items"} {
		err := runQueryPKMin3110(t, dsn, "public", tbl)
		if err == nil || !strings.Contains(err.Error(), "not available on a PostgreSQL-sourced index") {
			t.Errorf("public.%s: %v; want the PostgreSQL key range refusal", tbl, err)
		}
	}
}

// #3110: on a MySQL or MariaDB index recover writes, byte for byte, the
// statements the newest-snapshot resolver gives, and query refuses a key
// range on a table the newest snapshot does not hold, as before the fork.
func TestIntegrationMySQLRecoverAndQueryUnchanged_3110(t *testing.T) {
	for _, stream := range []string{"mysql", "mariadb", ""} {
		t.Run("stream="+stream, func(t *testing.T) {
			db, dbName := seedMySQLDroppedTable3110(t, stream)
			ctx := context.Background()
			rows, err := query.New(db).Fetch(ctx, query.Options{Schema: dbName, Table: "gone"})
			if err != nil {
				t.Fatal(err)
			}
			render := func(r *metadata.Resolver) string {
				var buf bytes.Buffer
				gen := recovery.NewForDialect(db, r, recovery.DialectForIndex(db))
				gen.SetBinlogOrder(query.BinlogOrderProof(ctx, db))
				if _, err := gen.GenerateSQLFromRows(rows, &buf); err != nil {
					t.Fatal(err)
				}
				return scriptBody3110(t, buf.String())
			}
			latest, err := metadata.NewResolver(db, 0)
			if err != nil {
				t.Fatal(err)
			}
			perTable, err := metadata.NewLatestPerTableResolver(db)
			if err != nil {
				t.Fatal(err)
			}
			want, other := render(latest), render(perTable)
			if want == other {
				t.Fatalf("fixture: both loaders give the same undo, so the pin proves nothing:\n%s", want)
			}

			out, err := runRecover3110(t, testutil.IntegrationDSN(dbName), dbName, "gone")
			if err != nil {
				t.Fatalf("recover: %v", err)
			}
			if got := scriptBody3110(t, out); got != want {
				t.Errorf("recover changed on a %q index:\ngot:\n%s\nwant:\n%s", stream, got, want)
			}

			err = runQueryPKMin3110(t, testutil.IntegrationDSN(dbName), dbName, "gone")
			if err == nil || !strings.Contains(err.Error(), "--pk-min/--pk-max: table "+dbName+".gone not found in snapshot") {
				t.Errorf("query --pk-min on the dropped table: %v; want the not-found refusal", err)
			}
		})
	}
}
