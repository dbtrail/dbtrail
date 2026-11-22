//go:build integration

package console

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"slices"
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

// #3110: on a PostgreSQL index (one schema snapshot per table) the console's
// bundle resolver sees every table, under its newest shape, not only the
// table saved last.
func TestIntegrationPGConsoleSeesEveryTable_3110(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()
	for _, rel := range []*metadata.PGRelationSchema{
		pgRel3110("orders", "status", "legacy"), pgRel3110("orders", "status"),
		pgRel3110("users", "name"), pgRel3110("items", "name"),
	} {
		if _, err := metadata.WritePGSnapshot(ctx, db, rel); err != nil {
			t.Fatal(err)
		}
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, source_health) VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1, '{}')`)

	r, unavailable := loadResolver(db)
	if r == nil || unavailable {
		t.Fatalf("loadResolver: r=%v unavailable=%v", r, unavailable)
	}
	b := &bundle{db: db, resolver: r}
	got, err := b.tablesForSchema(ctx, "public")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"items", "orders", "users"}; !slices.Equal(got, want) {
		t.Errorf("tablesForSchema(public) = %v, want %v", got, want)
	}
	tm, err := r.Resolve("public", "orders")
	if err != nil {
		t.Fatal(err)
	}
	var cols []string
	for _, c := range tm.Columns {
		cols = append(cols, c.Name)
	}
	if !slices.Equal(cols, []string{"id", "status"}) {
		t.Errorf("public.orders columns = %v, want [id status]", cols)
	}
}

// #3110: a MySQL or MariaDB index (and one with no stream_state row) loads
// exactly what it loaded before the fork. "gone" was dropped before the
// newest snapshot: the per-table loader would list it, the MySQL path does
// not.
func TestIntegrationMySQLConsoleResolverUnchanged_3110(t *testing.T) {
	for _, stream := range []string{"mysql", "mariadb", ""} {
		t.Run("stream="+stream, func(t *testing.T) {
			db, dbName := seedMySQLDroppedTable3110(t, stream)
			want, err := metadata.NewResolver(db, 0)
			if err != nil {
				t.Fatal(err)
			}
			got, unavailable := loadResolver(db)
			if got == nil || unavailable {
				t.Fatalf("loadResolver: r=%v unavailable=%v", got, unavailable)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("loadResolver differs from NewResolver(db, 0) on a %q index", stream)
			}
			b := &bundle{db: db, resolver: got}
			tables, err := b.tablesForSchema(context.Background(), dbName)
			if err != nil {
				t.Fatal(err)
			}
			var wantTables []string
			for _, m := range want.Tables(dbName) {
				wantTables = append(wantTables, m.Table)
			}
			if !slices.Equal(tables, wantTables) || slices.Contains(tables, "gone") {
				t.Errorf("tablesForSchema(%s) = %v, want %v (without the dropped table)", dbName, tables, wantTables)
			}
		})
	}
}

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
	return db, dbName
}

func tables3110(t *testing.T, srv *Server, schema string) []string {
	t.Helper()
	rec, body := doReq(t, srv, "GET", "/api/schemas?schema="+schema, "")
	if rec.Code != 200 {
		t.Fatalf("GET /api/schemas?schema=%s: %d %s", schema, rec.Code, body)
	}
	var resp tablesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	return resp.Tables
}

// #3110: a PostgreSQL index receives table shapes one at a time for as long
// as capture runs. A console opened before a DROP COLUMN, and before a
// table's first change, must see both: the new table in the list, and the
// undo of a row that still had the dropped column refused instead of
// written. The server is the command-line one (server.go), whose flavor is
// read once when it opens.
func TestIntegrationPGConsoleReloadsShapesAfterOpen_3110(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()
	first, err := metadata.WritePGSnapshot(ctx, db, pgRel3110("orders", "status", "legacy"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.WritePGSnapshot(ctx, db, pgRel3110("users", "name")); err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, source_health) VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1, '{}')`)
	testutil.InsertEvent(t, db, "0/100", 0x100, 0x100, "2026-11-21 10:00:00", nil, "public", "orders", 3, "1",
		nil, []byte(`{"id":1,"status":"paid","legacy":"x"}`), nil)
	testutil.MustExec(t, db, `UPDATE binlog_events SET schema_version = ? WHERE table_name = 'orders'`, first)

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := tables3110(t, srv, "public"); !slices.Equal(got, []string{"orders", "users"}) {
		t.Fatalf("tables when the console opens = %v, want [orders users]", got)
	}

	// After the console opened: legacy dropped, and a table's first change.
	if _, err := metadata.WritePGSnapshot(ctx, db, pgRel3110("orders", "status")); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.WritePGSnapshot(ctx, db, pgRel3110("late", "name")); err != nil {
		t.Fatal(err)
	}

	if got := tables3110(t, srv, "public"); !slices.Equal(got, []string{"late", "orders", "users"}) {
		t.Errorf("tables after the new shapes = %v, want [late orders users]", got)
	}
	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"public","table":"orders","since":"2026-11-21 00:00:00","until":"2026-11-22 00:00:00"}`)
	if rec.Code == 200 || !strings.Contains(string(body), "legacy") {
		t.Errorf("recover after the DROP COLUMN: %d %s; want the drift refusal naming legacy", rec.Code, body)
	}
	if strings.Contains(string(body), "INSERT INTO") {
		t.Errorf("recover wrote the undo with the dropped column: %s", body)
	}

	// Nothing new: the same bundle, no reload.
	b1, err := srv.cm.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	b2, err := srv.cm.Resolve(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if b1 != b2 {
		t.Errorf("an unchanged index published a new bundle")
	}
}

// #3110: a MySQL console keeps the resolver it opened with, as before: a
// snapshot taken after it opened changes neither the bundle nor its tables.
func TestIntegrationMySQLConsoleKeepsItsResolver_3110(t *testing.T) {
	for _, stream := range []string{"mysql", "mariadb", ""} {
		t.Run("stream="+stream, func(t *testing.T) {
			db, dbName := seedMySQLDroppedTable3110(t, stream)
			srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			b1, err := srv.cm.Resolve(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			before := tables3110(t, srv, dbName)
			testutil.MustExec(t, db, `CREATE TABLE later (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB`)
			if _, err := metadata.TakeSnapshot(db, db, []string{dbName}); err != nil {
				t.Fatal(err)
			}
			b2, err := srv.cm.Resolve(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			if b1 != b2 || b1.resolver != b2.resolver {
				t.Errorf("a MySQL-family bundle was reloaded after a new snapshot")
			}
			if after := tables3110(t, srv, dbName); !slices.Equal(after, before) || slices.Contains(after, "later") {
				t.Errorf("tables changed on a %q index: before %v, after %v", stream, before, after)
			}
		})
	}
}

// #3110: a PostgreSQL server of the registry (the one `watch` serves) gets the
// same reload: after its bundle opened, a DROP COLUMN and a table's first
// shape are both seen, through the registry's own bundle map. Its flavor comes
// from the entry, and the bundle opens with every table and its newest
// snapshot recorded. Evicting it forgets that record.
func TestIntegrationPGRegistryServerReloadsShapes_3110(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()
	first, err := metadata.WritePGSnapshot(ctx, db, pgRel3110("orders", "status", "legacy"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.WritePGSnapshot(ctx, db, pgRel3110("users", "name")); err != nil {
		t.Fatal(err)
	}
	testutil.InsertEvent(t, db, "0/100", 0x100, 0x100, "2026-11-21 10:00:00", nil, "public", "orders", 3, "1",
		nil, []byte(`{"id":1,"status":"paid","legacy":"x"}`), nil)
	testutil.MustExec(t, db, `UPDATE binlog_events SET schema_version = ? WHERE table_name = 'orders'`, first)

	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	entry, err := reg.Add(ServerEntry{Name: "pg", DSN: testutil.IntegrationDSN(dbName), Flavor: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	cm := newConnManager(reg, false)
	t.Cleanup(cm.CloseAll)

	// The bundle as opened, before any request: every table, from the
	// entry's flavor, and its newest snapshot recorded.
	opened, err := cm.buildBundle(entry)
	if err != nil {
		t.Fatal(err)
	}
	for _, tbl := range []string{"orders", "users"} {
		if opened.resolver == nil {
			t.Fatal("the PostgreSQL server opened with no table shapes")
		}
		if _, err := opened.resolver.Resolve("public", tbl); err != nil {
			t.Errorf("opened bundle: public.%s: %v", tbl, err)
		}
	}
	if v, ok := pgShapes.Load(opened.db); !ok || !v.(*pgShapeState).known {
		t.Error("the opened bundle did not record the snapshot its shapes were loaded at")
	}
	forgetPGShapes(opened.db)
	opened.db.Close()

	b1, err := cm.Resolve(ctx, entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := cm.Resolve(ctx, entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if b1 != b2 {
		t.Errorf("a request with no new shape published a new bundle")
	}
	if got, _ := b1.tablesForSchema(ctx, "public"); !slices.Equal(got, []string{"orders", "users"}) {
		t.Fatalf("tables when the server opens = %v, want [orders users]", got)
	}

	if _, err := metadata.WritePGSnapshot(ctx, db, pgRel3110("orders", "status")); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.WritePGSnapshot(ctx, db, pgRel3110("late", "name")); err != nil {
		t.Fatal(err)
	}

	b3, err := cm.Resolve(ctx, entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := b3.tablesForSchema(ctx, "public"); !slices.Equal(got, []string{"late", "orders", "users"}) {
		t.Errorf("tables after the new shapes = %v, want [late orders users]", got)
	}
	cm.mu.Lock()
	published, boot := cm.bundles[entry.ID], cm.boot
	cm.mu.Unlock()
	if published != b3 || boot != nil {
		t.Errorf("the reload was not published in the server's place (published %p, returned %p, boot %v)", published, b3, boot)
	}
	rows, err := query.New(db).Fetch(ctx, query.Options{Schema: "public", Table: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := recovery.NewForDialect(b3.db, b3.resolver, recovery.PostgresDialect).GenerateSQLFromRows(rows, &buf); err == nil || !strings.Contains(err.Error(), "legacy") {
		t.Errorf("undo after the DROP COLUMN: %v; want the drift refusal naming legacy\n%s", err, buf.String())
	}

	cm.evict(entry.ID)
	if _, ok := pgShapes.Load(b3.db); ok {
		t.Error("an evicted server's shape record was kept")
	}
}

// #3110: the command-line PostgreSQL server opens knowing its newest snapshot,
// so its first request reloads nothing.
func TestIntegrationPGBootServerFirstRequestKeepsItsBundle_3110(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if _, err := metadata.WritePGSnapshot(context.Background(), db, pgRel3110("users", "name")); err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, source_health) VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1, '{}')`)
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { forgetPGShapes(db) })
	opened, _ := srv.cm.bootInfo()
	got, err := srv.cm.Resolve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got != opened {
		t.Errorf("the first request reloaded the shapes the server opened with")
	}
}
