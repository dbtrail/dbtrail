//go:build integration

package mcptools

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

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

// seedPGShapes3110: a PostgreSQL index as the capture writes it (one table
// per snapshot): orders with legacy, orders without it, then users and items.
// A DELETE on orders carries the snapshot that still had legacy.
func seedPGShapes3110(t *testing.T) *sql.DB {
	t.Helper()
	db, _ := testutil.CreateTestDB(t)
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
	return db
}

// seedMySQLDroppedTable3110: a MySQL index where "gone" was dropped before the
// newest snapshot and has an UPDATE indexed. The MySQL path does not know the
// table; the per-table loader would.
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

// standalone3110 is the standalone MCP posture: no preloaded resolver, so each
// tool loads the schema itself.
func standalone3110(db *sql.DB) Config {
	return Config{
		Reconstruct:         true,
		AllowBaselineParams: true,
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, NoArchive: true}, nil
		},
	}
}

// #3110: the standalone MCP tools on a PostgreSQL index find every table under
// its newest shape, not only the table saved last.
func TestIntegrationPGToolsSeeEveryTable_3110(t *testing.T) {
	db := seedPGShapes3110(t)
	cfg := standalone3110(db)
	ctx := context.Background()

	// reconstruct: users was not the last table saved; its key resolves.
	res, _, err := MakeReconstructTool(cfg)(ctx, &mcp.CallToolRequest{}, ReconstructArgs{
		Schema: "public", Table: "users", PK: "1", At: "2026-11-21 11:00:00", BaselineDir: t.TempDir(), AllowGaps: true})
	if err != nil {
		t.Fatal(err)
	}
	// The key resolved, so the tool got as far as the baseline, which the
	// empty directory does not have.
	if txt := resultText(res); !res.IsError || !strings.Contains(txt, "no baseline snapshot covers public.users") {
		t.Errorf("reconstruct on public.users: %s; want the no-baseline refusal, past the key lookup", txt)
	}

	// query pk_min: the PostgreSQL refusal, not "not found in snapshot".
	for _, tbl := range []string{"orders", "users", "items"} {
		res, _, err := MakeQueryTool(cfg)(ctx, &mcp.CallToolRequest{}, QueryArgs{Schema: "public", Table: tbl, PKMin: "1"})
		if err != nil {
			t.Fatal(err)
		}
		if !res.IsError || !strings.Contains(resultText(res), "not available on a PostgreSQL-sourced index") {
			t.Errorf("query pk_min on public.%s: %s", tbl, resultText(res))
		}
	}

	// recover: the dropped column is refused, not written.
	res, _, err = MakeRecoverTool(cfg)(ctx, &mcp.CallToolRequest{}, RecoverArgs{Schema: "public", Table: "orders", NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(res), "legacy") {
		t.Errorf("recover did not refuse the undo with the dropped column: %s", resultText(res))
	}
	if strings.Contains(resultText(res), "INSERT INTO") {
		t.Errorf("recover wrote the undo with the dropped column: %s", resultText(res))
	}
}

// #3110: on a MySQL or MariaDB index (or one with no stream_state row) the
// standalone tools answer as before the fork: the table dropped before the
// newest snapshot is unknown to reconstruct and pk_min, and the recover
// script is byte for byte the newest-snapshot resolver's.
func TestIntegrationMySQLToolsUnchanged_3110(t *testing.T) {
	for _, stream := range []string{"mysql", "mariadb", ""} {
		t.Run("stream="+stream, func(t *testing.T) {
			db, dbName := seedMySQLDroppedTable3110(t, stream)
			cfg := standalone3110(db)
			ctx := context.Background()

			res, _, err := MakeReconstructTool(cfg)(ctx, &mcp.CallToolRequest{}, ReconstructArgs{
				Schema: dbName, Table: "gone", PK: "7", At: "2026-06-01 13:00:00", BaselineDir: t.TempDir(), AllowGaps: true})
			if err != nil {
				t.Fatal(err)
			}
			if !res.IsError || !strings.Contains(resultText(res), "no schema snapshot for "+dbName+".gone") {
				t.Errorf("reconstruct on the dropped table: %s; want the no-snapshot refusal", resultText(res))
			}

			res, _, err = MakeQueryTool(cfg)(ctx, &mcp.CallToolRequest{}, QueryArgs{Schema: dbName, Table: "gone", PKMin: "1"})
			if err != nil {
				t.Fatal(err)
			}
			if !res.IsError || !strings.Contains(resultText(res), "pk_min/pk_max: table "+dbName+".gone not found in snapshot") {
				t.Errorf("query pk_min on the dropped table: %s; want the not-found refusal", resultText(res))
			}

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
				s := buf.String()
				i := strings.Index(s, "BEGIN;")
				if i < 0 {
					t.Fatalf("no BEGIN in the script:\n%s", s)
				}
				return s[i:]
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
				t.Fatalf("fixture: both loaders give the same undo:\n%s", want)
			}
			res, _, err = MakeRecoverTool(cfg)(ctx, &mcp.CallToolRequest{}, RecoverArgs{Schema: dbName, Table: "gone", NoArchive: true})
			if err != nil {
				t.Fatal(err)
			}
			if res.IsError || !strings.Contains(resultText(res), want) {
				t.Errorf("recover changed on a %q index:\ngot:\n%s\nwant (from BEGIN):\n%s", stream, resultText(res), want)
			}
		})
	}
}
