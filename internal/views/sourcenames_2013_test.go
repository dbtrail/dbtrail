package views

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2013: every table is queried by its own name, inside a DuckDB schema named
// after its source schema — demo.prices, demo."order.items" — never through a
// manufactured state_<schema>_<table> name.

const stamp2013 = "2026-04-30T03-00-00Z"

// writeTable2013 writes one table file of the snapshot at root/stamp2013 whose
// status column holds the given values (ids 1..n), so every view in these tests
// can be checked against the rows of ITS file and not just "some rows".
func writeTable2013(t *testing.T, root, schema, table string, statuses ...string) BaselineTable {
	t.Helper()
	return writeTableFile2013(t, root, schema, table, table, statuses...)
}

// writeTableFile2013 is writeTable2013 with the file named apart from the
// table: the case-collision fixture needs Orders and orders in two files, and
// macOS's file system would make them one.
func writeTableFile2013(t *testing.T, root, schema, table, file string, statuses ...string) BaselineTable {
	t.Helper()
	rel := schema + "/" + file + ".parquet"
	path := filepath.Join(root, stamp2013, schema, file+".parquet")
	cols, err := baseline.ParseSchema(writeSchemaFile(t))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("baseline writer: %v", err)
	}
	for i, st := range statuses {
		if err := w.WriteRow([]string{string(rune('1' + i)), st}, []bool{false, false}); err != nil {
			t.Fatalf("write row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return BaselineTable{Schema: schema, Table: table, Path: path, Rel: rel}
}

func markSnapshot2013(t *testing.T, root string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, stamp2013, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

func input2013(root string, tables ...BaselineTable) Input {
	return Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   root,
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Baselines:        tables,
	}
}

// statusesOf runs `SELECT status FROM <ref> ORDER BY id` and fails the test,
// with the generated file attached, when the view does not answer.
func statusesOf(t *testing.T, db *sql.DB, ref, sqlText string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT status FROM ` + ref + ` ORDER BY id`)
	if err != nil {
		t.Fatalf("SELECT FROM %s: %v\n--- generated ---\n%s", ref, err, sqlText)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func TestSourceNames_tablesAreQueriedByTheirOwnName_2013(t *testing.T) {
	root := t.TempDir()
	tables := []BaselineTable{
		writeTable2013(t, root, "demo", "prices", "p1", "p2"),
		writeTable2013(t, root, "demo", "order.items", "oi"),
		writeTable2013(t, root, "Café.ü", "ñandú", "accented"),
		writeTable2013(t, root, "shop", "select", "keyword"),
	}
	sqlText := Generate(input2013(root, tables...))
	if strings.Contains(sqlText, "state_") {
		t.Fatalf("the file still names a state_ view:\n%s", sqlText)
	}
	if !strings.Contains(sqlText, "in demo.db, demo.prices is ambiguous") {
		t.Errorf("the file does not warn against a database file named after a schema:\n%s", sqlText)
	}
	db := execViews(t, sqlText)
	for ref, want := range map[string][]string{
		`demo.prices`:        {"p1", "p2"},
		`demo."order.items"`: {"oi"},
		`"Café.ü"."ñandú"`:   {"accented"},
		`shop."select"`:      {"keyword"},
	} {
		if got := statusesOf(t, db, ref, sqlText); !slices.Equal(got, want) {
			t.Errorf("%s returned %v, want %v", ref, got, want)
		}
	}
	// The names a person types: bare where DuckDB reads them bare, quoted
	// where it would not.
	got := input2013(root, tables...).DefinedViews()
	want := []string{`"Café.ü"."ñandú"`, `demo."order.items"`, `demo.prices`, `shop."select"`}
	if !slices.Equal(got, want) {
		t.Errorf("DefinedViews = %v, want %v", got, want)
	}
}

// DuckDB compares identifiers case-insensitively (ASCII only), so demo.Orders
// and demo.orders are ONE name there. Every real table keeps its exact name
// when it can; only the loser of a case collision is renamed, and never onto a
// name a real table holds (demo.orders_2 here is a real table).
func TestSourceNames_caseCollisionNeverStealsARealName_2013(t *testing.T) {
	root := t.TempDir()
	tables := []BaselineTable{
		writeTableFile2013(t, root, "demo", "Orders", "f1", "upper"),
		writeTableFile2013(t, root, "demo", "orders", "f2", "lower"),
		writeTableFile2013(t, root, "demo", "orders_2", "f3", "real-2"),
		writeTableFile2013(t, root, "demo", "Ñ", "f4", "enye-upper"),
		writeTableFile2013(t, root, "demo", "ñ", "f5", "enye-lower"),
	}
	in := input2013(root, tables...)
	sqlText := Generate(in)
	db := execViews(t, sqlText)
	for ref, want := range map[string][]string{
		`demo."Orders"`: {"upper"},
		`demo.orders_2`: {"real-2"},
		`demo.orders_3`: {"lower"},
		`demo."Ñ"`:      {"enye-upper"},
		`demo."ñ"`:      {"enye-lower"},
	} {
		if got := statusesOf(t, db, ref, sqlText); !slices.Equal(got, want) {
			t.Errorf("%s returned %v, want %v", ref, got, want)
		}
	}
	notes := strings.Join(in.NamingNotes(), "\n")
	if !strings.Contains(notes, "demo.orders_3") || !strings.Contains(notes, "demo.orders") {
		t.Errorf("the rename is not reported; notes:\n%s", notes)
	}
	if !strings.Contains(sqlText, "-- demo.orders_3: ") {
		t.Errorf("the file does not say why demo.orders_3 is named that way:\n%s", sqlText)
	}
}

// Schema names DuckDB already uses. main is the default schema, so tables of a
// source schema called main simply land there. information_schema and
// pg_catalog are DuckDB's own catalog and refuse views; temp, system and memory
// are DuckDB databases, so temp.x is ambiguous. Those tables are left out with
// a note, and the rest of the file still runs.
func TestSourceNames_builtInSchemaNames_2013(t *testing.T) {
	root := t.TempDir()
	var tables []BaselineTable
	tables = append(tables, writeTable2013(t, root, "demo", "prices", "ok"))
	tables = append(tables, writeTable2013(t, root, "main", "orders", "in-main"))
	for _, s := range []string{"information_schema", "pg_catalog", "temp", "SYSTEM", "memory"} {
		tables = append(tables, writeTable2013(t, root, s, "t", s))
	}
	in := input2013(root, tables...)
	sqlText := Generate(in)
	db := execViews(t, sqlText)
	if got := statusesOf(t, db, "demo.prices", sqlText); !slices.Equal(got, []string{"ok"}) {
		t.Errorf("demo.prices = %v", got)
	}
	if got := statusesOf(t, db, "main.orders", sqlText); !slices.Equal(got, []string{"in-main"}) {
		t.Errorf("main.orders = %v", got)
	}
	notes := strings.Join(in.NamingNotes(), "\n")
	for _, s := range []string{"information_schema.t", "pg_catalog.t", "temp.t", "SYSTEM.t", "memory.t"} {
		if !strings.Contains(notes, s) {
			t.Errorf("%s is left out without a note; notes:\n%s", s, notes)
		}
		if !strings.Contains(sqlText, "-- "+s+": ") {
			t.Errorf("the file does not say why %s is missing", s)
		}
	}
	if !strings.Contains(notes, "--database") {
		t.Errorf("the note for temp/system/memory should name --database, which makes them reachable:\n%s", notes)
	}
	defined := in.DefinedViews()
	if !slices.Equal(defined, []string{"demo.prices", "main.orders"}) {
		t.Errorf("DefinedViews = %v, want only the two definable tables", defined)
	}

	// Inside a per-server database, temp/system/memory are reachable as
	// wp.temp.t; DuckDB's own catalog names still are not.
	in.Database = "wp"
	sqlText = Generate(in)
	db = execViews(t, sqlText)
	for _, s := range []string{"temp", "SYSTEM", "memory"} {
		if got := statusesOf(t, db, "wp."+s+".t", sqlText); !slices.Equal(got, []string{s}) {
			t.Errorf("wp.%s.t = %v", s, got)
		}
	}
	if got := statusesOf(t, db, "wp.main.orders", sqlText); !slices.Equal(got, []string{"in-main"}) {
		t.Errorf("wp.main.orders = %v", got)
	}
	notes = strings.Join(in.NamingNotes(), "\n")
	if !strings.Contains(notes, "information_schema.t") || strings.Contains(notes, "temp.t") {
		t.Errorf("with --database only DuckDB's catalog names are left out; notes:\n%s", notes)
	}
}

// The events view is main.events. A source table main.events cannot have that
// name, so it is renamed (and said so); shop.events is a different name and
// keeps it.
func TestSourceNames_tablesNamedEvents_2013(t *testing.T) {
	root, archives := t.TempDir(), t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeFixtureArchive(t, archives, id)
	in := input2013(root,
		writeTable2013(t, root, "main", "events", "main-events"),
		writeTable2013(t, root, "shop", "events", "shop-events"),
		writeTable2013(t, root, "events", "orders", "events-schema"),
	)
	in.ArchiveSources = []string{filepath.Join(archives, "bintrail_id="+id)}
	sqlText := Generate(in)
	db := execViews(t, sqlText)
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM events WHERE schema_name = 'shop'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("the events view answered %d, %v; want the one archived event", n, err)
	}
	for ref, want := range map[string][]string{
		`main.events_2`: {"main-events"},
		`shop.events`:   {"shop-events"},
		`events.orders`: {"events-schema"},
	} {
		if got := statusesOf(t, db, ref, sqlText); !slices.Equal(got, want) {
			t.Errorf("%s returned %v, want %v", ref, got, want)
		}
	}
	// Reserved whether or not this render defines the events view, so the
	// name does not move when it is switched on.
	in.OmitEvents = true
	if got := in.DefinedViews(); !slices.Contains(got, "main.events_2") {
		t.Errorf("without the events view main.events moved: %v", got)
	}
}

// Every following mode, with a dotted name, inside a per-server database.
func TestSourceNames_everyFollowModeInADatabase_2013(t *testing.T) {
	for _, mode := range followModes {
		t.Run(mode.name, func(t *testing.T) {
			root := t.TempDir()
			tables := []BaselineTable{writeTable2013(t, root, "demo", "order.items", "a", "b")}
			markSnapshot2013(t, root)
			in := input2013(root, tables...)
			in.Database = "wp"
			switch mode.follow {
			case FollowPointer:
				if err := baseline.PublishCurrentPointer(filepath.Join(root, stamp2013)); err != nil {
					t.Fatal(err)
				}
				ApplyFollow(&in, root, false)
				if in.Follow != FollowPointer {
					t.Fatalf("ApplyFollow chose %v", in.Follow)
				}
			case FollowNewest:
				in.Follow = FollowNewest
			}
			sqlText := Generate(in)
			db := execViews(t, sqlText)
			if got := statusesOf(t, db, `wp.demo."order.items"`, sqlText); !slices.Equal(got, []string{"a", "b"}) {
				t.Errorf(`wp.demo."order.items" = %v`, got)
			}
		})
	}
}

// The chain-aware body (a table with deltas beside its file) under a dotted
// name: the delta files are found by the file's name, not the view's.
func TestSourceNames_deltaChainUnderADottedName_2013(t *testing.T) {
	root := t.TempDir()
	tb := writeTable2013(t, root, "demo", "order.items", "one", "two")
	writeDeltaPair(t, tb.Path, 0, []int64{0}, [][3]string{{"1", "changed", "u"}})
	tables := []BaselineTable{tb}
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatal(err)
	}
	if !tables[0].Delta {
		t.Fatal("fixture: the table delta was not detected")
	}
	sqlText := Generate(input2013(root, tables...))
	db := execViews(t, sqlText)
	if got := statusesOf(t, db, `demo."order.items"`, sqlText); !slices.Equal(got, []string{"changed", "two"}) {
		t.Errorf(`demo."order.items" = %v, want the chain applied`, got)
	}
}

// An S3 root renders the same names; the file is not executed (no bucket).
func TestSourceNames_s3RootUsesTheSameNames_2013(t *testing.T) {
	in := input2013("s3://bucket/baselines/", BaselineTable{
		Schema: "demo", Table: "order.items", Rel: "demo/order.items.parquet",
		Path: "s3://bucket/baselines/" + stamp2013 + "/demo/order.items.parquet",
	})
	ApplyFollow(&in, "s3://bucket/baselines/", false)
	sqlText := Generate(in)
	if !strings.Contains(sqlText, `CREATE OR REPLACE VIEW "demo"."order.items" AS`) {
		t.Errorf("the S3 file does not define demo.\"order.items\":\n%s", sqlText)
	}
	if !strings.Contains(sqlText, `CREATE SCHEMA IF NOT EXISTS "demo";`) {
		t.Errorf("the S3 file does not create schema demo:\n%s", sqlText)
	}
}

// Two servers that share demo.prices, each in its own database, loaded into
// one session: neither replaces the other.
func TestSourceNames_twoServersOneSession_2013(t *testing.T) {
	var files []string
	for _, srv := range []struct{ db, status string }{{"wp", "from-wp"}, {"rds", "from-rds"}} {
		root := t.TempDir()
		in := input2013(root, writeTable2013(t, root, "demo", "prices", srv.status))
		in.Database = srv.db
		files = append(files, Generate(in))
	}
	db := execViews(t, files[0])
	if _, err := db.Exec(files[1]); err != nil {
		t.Fatalf("second server's file: %v", err)
	}
	if got := statusesOf(t, db, "wp.demo.prices", files[0]); !slices.Equal(got, []string{"from-wp"}) {
		t.Errorf("wp.demo.prices = %v", got)
	}
	if got := statusesOf(t, db, "rds.demo.prices", files[1]); !slices.Equal(got, []string{"from-rds"}) {
		t.Errorf("rds.demo.prices = %v", got)
	}
}

// Loaded into a database file of the same name (duckdb wp.db), the ATTACH is a
// no-op and the views land in that file, so they are still there next time.
func TestSourceNames_databaseFileOfTheSameNamePersists_2013(t *testing.T) {
	root := t.TempDir()
	in := input2013(root, writeTable2013(t, root, "demo", "prices", "kept"))
	in.Database = "wp"
	sqlText := Generate(in)
	dbPath := filepath.Join(t.TempDir(), "wp.db")
	for i := 0; i < 2; i++ {
		db, err := sql.Open("duckdb", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if _, err := db.Exec(sqlText); err != nil {
				t.Fatalf("load into wp.db: %v\n%s", err, sqlText)
			}
		} else if got := statusesOf(t, db, "wp.demo.prices", sqlText); !slices.Equal(got, []string{"kept"}) {
			t.Errorf("after reopening wp.db, wp.demo.prices = %v", got)
		}
		db.Close()
	}
}

func TestOnlyViews_keyedBySourceName_2013(t *testing.T) {
	root := t.TempDir()
	in := input2013(root,
		writeTable2013(t, root, "demo", "prices", "p"),
		writeTable2013(t, root, "demo", "order.items", "oi"),
	)
	in.OnlyViews = ViewSet{`demo."order.items"`: true}
	got := createdViews(t, GenerateViews(in))
	if !slices.Equal(got, []string{"demo.order.items"}) {
		t.Errorf("OnlyViews{demo.\"order.items\"} defined %v", got)
	}
}

// A name with a line break is legal in MySQL. Every comment the file writes
// about it must stay one comment, or the rest of the name becomes SQL.
func TestSourceNames_lineBreakInANameStaysInsideTheComment_2013(t *testing.T) {
	root := t.TempDir()
	in := input2013(root,
		writeTableFile2013(t, root, "demo", "X\nSELECT 1", "f1", "upper"),
		writeTableFile2013(t, root, "demo", "x\nSELECT 1", "f2", "lower"),
		writeTableFile2013(t, root, "temp", "t\nDROP", "f3", "skipped"),
	)
	sqlText := Generate(in)
	db := execViews(t, sqlText)
	if got := statusesOf(t, db, "demo.\"x\nSELECT 1_2\"", sqlText); !slices.Equal(got, []string{"lower"}) {
		t.Errorf("the renamed view returned %v", got)
	}
	// With every quoted identifier and literal taken out, the name's second
	// line must not start a line of its own: that is SQL a comment leaked.
	unquoted := regexp.MustCompile(`"(?:[^"]|"")*"|'(?:[^']|'')*'`).ReplaceAllString(sqlText, "")
	for _, line := range strings.Split(unquoted, "\n") {
		if strings.HasPrefix(line, "SELECT 1") || strings.HasPrefix(line, "DROP") {
			t.Errorf("a name's second line escaped its comment: %q", line)
		}
	}
}
