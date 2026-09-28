package views

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// Two servers that share a schema.table, which is the whole premise of #1874:
// both have shop.orders, and the rows differ so a view that reads the other
// server's file cannot pass by reading whatever is there.
const (
	serverAID = "aaaaaaaa-0000-0000-0000-000000000001"
	serverBID = "bbbbbbbb-0000-0000-0000-000000000002"
	stampA    = "2026-04-30T03-00-00Z"
	stampB    = "2026-05-02T03-00-00Z"
)

var (
	rowsA = []string{"from-a"}
	rowsB = []string{"from-b-1", "from-b-2"}
)

// serverFile generates one server's file over its own baseline root and its
// own archive root, in the following mode asked for.
func serverFile(t *testing.T, schema, id, stamp string, follow FollowMode, rows []string) string {
	t.Helper()
	baselines, archives := t.TempDir(), t.TempDir()
	path := writeSnapshot(t, baselines, stamp, true, rows...)
	writeFixtureArchive(t, archives, id)
	snap, err := time.Parse("2006-01-02T15-04-05Z", stamp)
	if err != nil {
		t.Fatalf("parse stamp: %v", err)
	}
	in := Input{
		GeneratedAt:      time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		Schema:           schema,
		ArchiveSources:   []string{filepath.Join(archives, "bintrail_id="+id)},
		BaselineSource:   baselines,
		BaselineSnapshot: snap,
		Baselines:        []BaselineTable{{Schema: "shop", Table: "orders", Path: path}},
	}
	switch follow {
	case FollowPointer:
		if err := baseline.PublishCurrentPointer(filepath.Join(baselines, stamp)); err != nil {
			t.Fatalf("publish pointer: %v", err)
		}
		ApplyFollow(&in, baselines, false)
		if in.Follow != FollowPointer {
			t.Fatalf("ApplyFollow chose %v, want FollowPointer", in.Follow)
		}
	case FollowNewest:
		// Set by hand over a local root, for the reason newestFixture gives.
		in.Follow = FollowNewest
		in.Baselines[0].Rel = "shop/orders.parquet"
	}
	return Generate(in)
}

func statuses(t *testing.T, db *sql.DB, view string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT "status" FROM ` + view + ` ORDER BY 1`)
	if err != nil {
		t.Fatalf("query %s: %v", view, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan %s: %v", view, err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read %s: %v", view, err)
	}
	return out
}

func sameStrings(a, b []string) bool {
	return strings.Join(a, "\x00") == strings.Join(b, "\x00") && len(a) == len(b)
}

var schemaFollowModes = []struct {
	name   string
	follow FollowMode
}{
	{"pinned", FollowNone},
	{"following the pointer", FollowPointer},
	{"following the newest snapshot", FollowNewest},
}

// TestSchema_twoServersInOneDatabase is the acceptance test of #1874, and it
// RUNS the files: one per server, concatenated, loaded into one DuckDB
// database file.
//
// It reads the ROWS and not the catalog. Both views existing under their own
// names is the half a text check could also see; the half it cannot is a view
// that is listed under server A and returns server B's rows, which is what a
// name shared outside the schema produces.
func TestSchema_twoServersInOneDatabase(t *testing.T) {
	for _, mode := range schemaFollowModes {
		t.Run(mode.name, func(t *testing.T) {
			fileA := serverFile(t, "a", serverAID, stampA, mode.follow, rowsA)
			fileB := serverFile(t, "b", serverBID, stampB, mode.follow, rowsB)

			lake := filepath.Join(t.TempDir(), "lake.db")
			db, err := sql.Open("duckdb", lake)
			if err != nil {
				t.Fatalf("open duckdb: %v", err)
			}
			defer db.Close()
			if _, err := db.Exec(fileA + "\n" + fileB); err != nil {
				t.Fatalf("DuckDB rejected the two files loaded together:\n%v\n\n--- a ---\n%s\n--- b ---\n%s", err, fileA, fileB)
			}

			if got := statuses(t, db, "a.state_shop_orders"); !sameStrings(got, rowsA) {
				t.Errorf("a.state_shop_orders returned %v, want server a's rows %v", got, rowsA)
			}
			if got := statuses(t, db, "b.state_shop_orders"); !sameStrings(got, rowsB) {
				t.Errorf("b.state_shop_orders returned %v, want server b's rows %v", got, rowsB)
			}

			for schema, want := range map[string]string{"a": serverAID, "b": serverBID} {
				var id string
				var n int
				if err := db.QueryRow(`SELECT min("bintrail_id"), count(DISTINCT "bintrail_id") FROM ` + schema + `.events`).Scan(&id, &n); err != nil {
					t.Fatalf("query %s.events: %v", schema, err)
				}
				if n != 1 || id != want {
					t.Errorf("%s.events holds %d source(s), the first %q; want only %q", schema, n, id, want)
				}
			}

			// Nothing was left in the default schema for a third file to replace.
			var inMain int
			if err := db.QueryRow(`SELECT count(*) FROM duckdb_views()
			                       WHERE NOT internal AND schema_name = 'main'`).Scan(&inMain); err != nil {
				t.Fatalf("list views: %v", err)
			}
			if inMain != 0 {
				t.Errorf("%d view(s) were created in main", inMain)
			}

			// The cross-server read the feature exists for.
			var both int
			if err := db.QueryRow(`SELECT count(*) FROM a.state_shop_orders x, b.state_shop_orders y`).Scan(&both); err != nil {
				t.Fatalf("join across servers: %v", err)
			}
			if both != len(rowsA)*len(rowsB) {
				t.Errorf("the join across servers returned %d row(s), want %d", both, len(rowsA)*len(rowsB))
			}

			// Loading a file again replaces that server's views and leaves the
			// other's alone: regenerating one server is the ordinary case.
			if _, err := db.Exec(fileA); err != nil {
				t.Fatalf("DuckDB rejected server a's file loaded a second time: %v", err)
			}
			if got := statuses(t, db, "b.state_shop_orders"); !sameStrings(got, rowsB) {
				t.Errorf("after reloading a, b.state_shop_orders returned %v, want %v", got, rowsB)
			}
		})
	}
}

// TestSchema_withoutOneTheSecondServerReplacesTheFirst is the control: the
// same two servers, the same load, no schema. It states the defect the flag
// exists for, so the test above cannot be passing for a reason that has
// nothing to do with the schema.
func TestSchema_withoutOneTheSecondServerReplacesTheFirst(t *testing.T) {
	for _, mode := range schemaFollowModes {
		t.Run(mode.name, func(t *testing.T) {
			fileA := serverFile(t, "", serverAID, stampA, mode.follow, rowsA)
			fileB := serverFile(t, "", serverBID, stampB, mode.follow, rowsB)
			db := execViews(t, fileA+"\n"+fileB)

			if got := statuses(t, db, "state_shop_orders"); !sameStrings(got, rowsB) {
				t.Errorf("state_shop_orders returned %v; the premise is that server b's %v replaced it", got, rowsB)
			}
			var n int
			if err := db.QueryRow(`SELECT count(*) FROM duckdb_views() WHERE NOT internal`).Scan(&n); err != nil {
				t.Fatalf("list views: %v", err)
			}
			if n != 2 {
				t.Errorf("%d views after loading two servers, want the 2 that survive out of 4", n)
			}
		})
	}
}

// TestSchema_persistedViewsKeepTheirServer reopens the database file, which is
// where an operator with one file per session actually meets these views.
// Pinned only: a following file keeps its snapshot in a session variable, and
// what a new session does with that is its own documented behaviour.
func TestSchema_persistedViewsKeepTheirServer(t *testing.T) {
	fileA := serverFile(t, "a", serverAID, stampA, FollowNone, rowsA)
	fileB := serverFile(t, "b", serverBID, stampB, FollowNone, rowsB)
	lake := filepath.Join(t.TempDir(), "lake.db")

	db, err := sql.Open("duckdb", lake)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	if _, err := db.Exec(fileA + "\n" + fileB); err != nil {
		t.Fatalf("DuckDB rejected the two files: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = sql.Open("duckdb", lake)
	if err != nil {
		t.Fatalf("reopen duckdb: %v", err)
	}
	defer db.Close()
	if got := statuses(t, db, "a.state_shop_orders"); !sameStrings(got, rowsA) {
		t.Errorf("after reopening, a.state_shop_orders returned %v, want %v", got, rowsA)
	}
	if got := statuses(t, db, "b.state_shop_orders"); !sameStrings(got, rowsB) {
		t.Errorf("after reopening, b.state_shop_orders returned %v, want %v", got, rowsB)
	}
}

// TestSchema_twoServersWithTheLiveLeg loads two files that each carry an
// index leg into one session.
//
// The ATTACH itself cannot run here: it needs the mysql extension and a MySQL
// to dial. What stands in for it is a catalog under the alias each file names,
// so everything AFTER the ATTACH is the real generated text: the two-leg view,
// its anti-join, and which catalog each server's view reads.
func TestSchema_twoServersWithTheLiveLeg(t *testing.T) {
	file := func(schema, id string) string {
		in := twoLegInput(t, id)
		in.Schema = schema
		return Generate(in)
	}
	fileA, fileB := file("a", serverAID), file("b", serverBID)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	liveStandInAs(t, db, "a_live")
	liveStandInAs(t, db, "b_live")
	// The stand-ins hold the same rows. Mark b's, so a view that read the
	// other server's index is visible in what it returns.
	if _, err := db.Exec(`UPDATE "b_live"."binlog_events" SET "table_name" = 'orders_on_b'`); err != nil {
		t.Fatalf("mark server b's index: %v", err)
	}

	both := stripLivePreamble(fileA) + "\n" + stripLivePreamble(fileB)
	if strings.Contains(both, "ATTACH ''") {
		t.Fatal("the ATTACH survived the strip, so this would dial a MySQL")
	}
	if _, err := db.Exec(both); err != nil {
		t.Fatalf("DuckDB rejected the two files loaded together:\n%v\n\n--- generated ---\n%s", err, both)
	}

	// Event 2 is only in the index, so its table name says WHICH index.
	for schema, want := range map[string]string{"a": "orders", "b": "orders_on_b"} {
		var table string
		if err := db.QueryRow(`SELECT "table_name" FROM ` + schema + `.events WHERE "event_id" = 2`).Scan(&table); err != nil {
			t.Fatalf("query %s.events: %v", schema, err)
		}
		if table != want {
			t.Errorf("%s.events read its index rows from a catalog holding %q, want %q", schema, table, want)
		}
	}
	// Event 1 is archived on both servers and the archives win the overlap, so
	// its source is the one on the PATH, per server.
	for schema, want := range map[string]string{"a": serverAID, "b": serverBID} {
		var id sql.NullString
		if err := db.QueryRow(`SELECT "bintrail_id" FROM ` + schema + `.events WHERE "event_id" = 1`).Scan(&id); err != nil {
			t.Fatalf("query %s.events: %v", schema, err)
		}
		if id.String != want {
			t.Errorf("%s.events attributes the archived event to %q, want %q", schema, id.String, want)
		}
	}
}

// TestDuckDB_oneAliasCannotBeAttachedTwice is why the alias is derived: two
// files that both attach "bintrail_live" do not load into one session at all.
// Loud rather than silent, and still the end of the multi-server load.
func TestDuckDB_oneAliasCannotBeAttachedTwice(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	attach := `ATTACH ':memory:' AS ` + quoteIdent(liveAttachAlias)
	if _, err := db.Exec(attach); err != nil {
		t.Fatalf("first attach: %v", err)
	}
	if _, err := db.Exec(attach); err == nil {
		t.Error("DuckDB attached one alias twice; a shared alias would then be a silent overwrite instead")
	}
}

// TestDuckDB_schemaNamesFoldCase is the measurement behind refusing uppercase.
//
// DuckDB compares identifiers without case even when they are QUOTED, so "A"
// and "a" are one schema and `--schema A` for one server with `--schema a` for
// another is the overwrite this flag exists to prevent. Quoting the name in
// the output does not separate them. If this ever fails, DuckDB started
// telling them apart and the refusal can go.
func TestDuckDB_schemaNamesFoldCase(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS "A"; CREATE OR REPLACE VIEW "A"."v" AS SELECT 'upper' AS x;
	                      CREATE SCHEMA IF NOT EXISTS "a"; CREATE OR REPLACE VIEW "a"."v" AS SELECT 'lower' AS x;`); err != nil {
		t.Fatalf("create: %v", err)
	}
	var x string
	if err := db.QueryRow(`SELECT x FROM "A"."v"`).Scan(&x); err != nil {
		t.Fatalf("query: %v", err)
	}
	if x != "lower" {
		t.Errorf(`"A"."v" returned %q: DuckDB kept "A" and "a" apart, so uppercase names need not be refused`, x)
	}
	if err := ValidateSchemaName("A"); err == nil {
		t.Error(`"A" is accepted, and DuckDB reads it as the same schema as "a"`)
	}
}

// TestDuckDB_namesItAlreadyHas measures each name the validator refuses as
// taken, so the list is what this DuckDB does and not what someone remembered.
func TestDuckDB_namesItAlreadyHas(t *testing.T) {
	for _, name := range builtinSchemaNames {
		t.Run(name, func(t *testing.T) {
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatalf("open duckdb: %v", err)
			}
			defer db.Close()
			// A view in main first: the default schema is the one name that
			// loads WITHOUT an error, and what it does instead is replace
			// whatever a file generated with no schema put there.
			if _, err := db.Exec(`CREATE OR REPLACE VIEW "events" AS SELECT 'unqualified' AS x`); err != nil {
				t.Fatalf("create the unqualified view: %v", err)
			}
			q := quoteIdent(name)
			_, err = db.Exec(`CREATE SCHEMA IF NOT EXISTS ` + q + `; CREATE OR REPLACE VIEW ` + q + `."events" AS SELECT 'qualified' AS x;`)
			if err != nil {
				return // refused by DuckDB itself: taken
			}
			var x string
			if err := db.QueryRow(`SELECT x FROM "events"`).Scan(&x); err != nil {
				t.Fatalf("query: %v", err)
			}
			if x != "qualified" {
				t.Errorf("schema %q loaded and left the unqualified view alone, so it is not taken", name)
			}
		})
	}
}

// TestDuckDB_aDatabaseNamedLikeTheSchema pins the one collision the generator
// cannot see and the file therefore warns about: DuckDB names a database after
// its file, and a two-part name whose first part is both a database and a
// schema is refused as ambiguous.
func TestDuckDB_aDatabaseNamedLikeTheSchema(t *testing.T) {
	fileA := serverFile(t, "wp", serverAID, stampA, FollowNone, rowsA)
	if !strings.Contains(fileA, "wp.db") {
		t.Errorf("the file does not warn against a database file named like its schema:\n%s", fileA)
	}
	db, err := sql.Open("duckdb", filepath.Join(t.TempDir(), "wp.db"))
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(fileA)
	if err == nil || !strings.Contains(err.Error(), "Ambiguous") {
		t.Errorf("loading schema wp into wp.db gave %v; the file warns that DuckDB refuses it as ambiguous", err)
	}
}

// TestDuckDB_keywordsThatCannotBeTypedBare ties the keyword list to the
// engine, in both directions.
//
// The generated file quotes the schema, so ANY keyword loads. What a keyword
// breaks is the reader's own query: `SELECT * FROM order.events` is a syntax
// error, and the reader meets it after the file loaded cleanly. So a name is
// refused exactly when DuckDB cannot take it unquoted in that position, and
// this asks DuckDB which those are instead of trusting a list.
func TestDuckDB_keywordsThatCannotBeTypedBare(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT keyword_name FROM duckdb_keywords() ORDER BY 1`)
	if err != nil {
		t.Fatalf("list keywords: %v", err)
	}
	var keywords []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatalf("scan keyword: %v", err)
		}
		keywords = append(keywords, k)
	}
	rows.Close()
	if len(keywords) < 100 {
		t.Fatalf("DuckDB listed %d keywords, so this would pass on almost nothing", len(keywords))
	}

	var refusedForNothing, acceptedAndBroken []string
	for _, k := range keywords {
		q := quoteIdent(k)
		if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS ` + q + `; CREATE OR REPLACE VIEW ` + q + `."v" AS SELECT 1 AS x;`); err != nil {
			// DuckDB will not create it under any spelling (a built-in name).
			if ValidateSchemaName(k) == nil {
				acceptedAndBroken = append(acceptedAndBroken, k)
			}
			continue
		}
		var x int
		bare := db.QueryRow(`SELECT x FROM ` + k + `.v`).Scan(&x)
		verdict := ValidateSchemaName(k)
		switch {
		case bare != nil && verdict == nil:
			acceptedAndBroken = append(acceptedAndBroken, k)
		case bare == nil && verdict != nil && strings.Contains(verdict.Error(), "SQL keyword"):
			refusedForNothing = append(refusedForNothing, k)
		}
		if _, err := db.Exec(`DROP SCHEMA ` + q + ` CASCADE`); err != nil {
			t.Fatalf("drop schema %s: %v", k, err)
		}
	}
	if len(acceptedAndBroken) > 0 {
		t.Errorf("accepted as schema names, and DuckDB cannot read them unquoted: %v", acceptedAndBroken)
	}
	if len(refusedForNothing) > 0 {
		t.Errorf("refused as SQL keywords, and DuckDB reads them unquoted: %v", refusedForNothing)
	}
}
