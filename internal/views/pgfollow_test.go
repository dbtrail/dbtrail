package views

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

const pgFollowStamp = "2026-04-30T03-00-00Z"

// writePGFollowBase writes a PostgreSQL table file of (id, status) rows at
// <root>/<stamp>/<schema>/<table>.parquet.
func writePGFollowBase(t *testing.T, root, schema, table string, rows ...[2]string) string {
	t.Helper()
	path := filepath.Join(root, pgFollowStamp, schema, table+".parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, []baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow([]string{r[0], r[1]}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// writePGFollowLink writes link seq of base's chain, after link seq-1, as a
// snapshot update writes it: each change is a key and its new status, ""
// for a deleted key. The dead rows are the base rows of the keys changed.
func writePGFollowLink(t *testing.T, base string, seq int, changes map[string]string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	at, err := pgdelta.BasePositions(context.Background(), db, base, []string{"id"}, func(k string) bool { _, ok := changes[k]; return ok })
	if err != nil {
		t.Fatal(err)
	}
	prev := ""
	if seq > 0 {
		prev = fmt.Sprintf("run-%d", seq-1)
	}
	_, _, _, err = pgdelta.WritePair(pgdelta.PairInput{
		BasePath: base, Seq: seq, Base: "base-1", Run: fmt.Sprintf("run-%d", seq), PrevRun: prev,
		Columns: pgPairCols, KeyColumns: []string{"id"}, Positions: at,
		Changes: func(yield func(pgdelta.Change) error) error {
			for _, k := range keys {
				c := pgdelta.Change{Key: k, Delete: changes[k] == ""}
				if !c.Delete {
					c.Values, c.Nulls = []string{k, changes[k]}, []bool{false, false}
				}
				if err := yield(c); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("WritePair %d: %v", seq, err)
	}
}

// pgFollowTable is pgPairTable for a table in any schema.
func pgFollowTable(path, schema, table string) BaselineTable {
	tbl := pgPairTable(path, table)
	tbl.Schema, tbl.Rel = schema, schema+"/"+table+".parquet"
	return tbl
}

// pgFollowRows reads schema.table's state view as "id=status", ordered by
// id as a number. Not its type: a pinned view casts id to the number it is,
// a following one reads it as text (Generate drops the casts of a file that
// follows, #2755).
func pgFollowRows(t *testing.T, db *sql.DB, schema, table string) ([]string, error) {
	t.Helper()
	rows, err := db.Query(`SELECT id::VARCHAR || '=' || status FROM "` + schema + `"."` + table + `" ORDER BY id::BIGINT`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

type pgFollowCase struct {
	schema, table string
	want          []string
}

// pgFollowFixture is one snapshot folder of PostgreSQL tables, with the
// `current` pointer and the _SUCCESS marker, and what each reads as:
//
//   - public.items has no pair;
//   - public.single has one pair;
//   - public.orders has a chain of three links: key 1 changed in each (the
//     newest wins), key 2 deleted and put back, key 7 inserted and deleted
//     again, key 9 inserted, keys 3 and 10 never touched;
//   - public."or*ders" has a chain of its own, and a name that, taken as a
//     pattern, would match the files of orders;
//   - public."items.eu" (a dot in the name, and a name that starts with
//     items' and a dot: items is then kept to its file alone, behind the
//     guard, which reads fine with no pair) and sales.orders (the same name
//     in another schema) each have their own.
func pgFollowFixture(t *testing.T) (root string, cases []pgFollowCase) {
	t.Helper()
	root = t.TempDir()
	writePGFollowBase(t, root, "public", "items", [2]string{"5", "five"})

	single := writePGFollowBase(t, root, "public", "single", [2]string{"1", "one"}, [2]string{"2", "two"})
	writePGFollowLink(t, single, 0, map[string]string{"1": "ONE"})

	orders := writePGFollowBase(t, root, "public", "orders", [2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"3", "three"}, [2]string{"10", "ten"})
	writePGFollowLink(t, orders, 0, map[string]string{"1": "ONE", "2": "", "7": "seven"})
	writePGFollowLink(t, orders, 1, map[string]string{"1": "uno", "2": "back", "7": ""})
	writePGFollowLink(t, orders, 2, map[string]string{"1": "eins", "9": "nine"})

	star := writePGFollowBase(t, root, "public", "or*ders", [2]string{"1", "star"}, [2]string{"4", "four"})
	writePGFollowLink(t, star, 0, map[string]string{"1": "STAR", "4": ""})

	eu := writePGFollowBase(t, root, "public", "items.eu", [2]string{"1", "eu"})
	writePGFollowLink(t, eu, 0, map[string]string{"1": "EU"})

	sales := writePGFollowBase(t, root, "sales", "orders", [2]string{"1", "sale"}, [2]string{"2", "deal"})
	writePGFollowLink(t, sales, 0, map[string]string{"2": "DEAL"})

	if err := os.WriteFile(filepath.Join(root, pgFollowStamp, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(pgFollowStamp, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
		t.Fatal(err)
	}
	return root, []pgFollowCase{
		{"public", "items", []string{"5=five"}},
		{"public", "single", []string{"1=ONE", "2=two"}},
		{"public", "orders", []string{"1=eins", "2=back", "3=three", "9=nine", "10=ten"}},
		{"public", "or*ders", []string{"1=STAR"}},
		{"public", "items.eu", []string{"1=EU"}},
		{"sales", "orders", []string{"1=sale", "2=DEAL"}},
	}
}

// pgFollowTables are the fixture's tables as a views file is handed them,
// their paths under dir (the snapshot's folder, or the pointer).
func pgFollowTables(dir string, cases []pgFollowCase) []BaselineTable {
	out := make([]BaselineTable, len(cases))
	for i, c := range cases {
		out[i] = pgFollowTable(filepath.Join(dir, c.schema, c.table+".parquet"), c.schema, c.table)
	}
	return out
}

// The views file a user downloads without pinning it follows later
// snapshots. Over a PostgreSQL table it reads the table's file and every
// pair beside it, one link or a chain, as a MySQL table's following view
// reads its chain, with no refusal: the same rows, table by table, as the
// view pinned to that snapshot, which lists the pairs when it is written.
//
// Every way the file can follow: the `current` pointer, the newest snapshot
// read through the file's one listing of it, the same with no listing in the
// session (a database file reopened, the variable set by hand), and the
// newest snapshot read without a listing at all, as a file whose events view
// reads archives in S3 is written.
func TestPGFollowStateView(t *testing.T) {
	root, cases := pgFollowFixture(t)
	snap := filepath.Join(root, pgFollowStamp)
	check := func(t *testing.T, label string, db *sql.DB) {
		t.Helper()
		for _, c := range cases {
			got, err := pgFollowRows(t, db, c.schema, c.table)
			if err != nil || !reflect.DeepEqual(got, c.want) {
				t.Errorf("%s: %s.%s reads %v, %v; want %v", label, c.schema, c.table, got, err, c.want)
			}
		}
	}

	pinned := pgPairGenerate(root, FollowNone, pgFollowTables(snap, cases)...)
	check(t, "pinned", execViews(t, pinned))

	pointer := pgPairGenerate(root, FollowPointer, pgFollowTables(filepath.Join(root, baseline.CurrentLinkName), cases)...)
	if strings.Contains(pointer, "public.orders has a PostgreSQL table delta") {
		t.Fatalf("the file that follows the pointer still refuses a table with a pair:\n%s", pointer)
	}
	check(t, "following the pointer", execViews(t, pointer))

	newest := pgPairGenerate(root, FollowNewest, pgFollowTables(snap, cases)...)
	if strings.Contains(newest, "public.orders has a PostgreSQL table delta") {
		t.Fatalf("the file that follows the newest snapshot still refuses a table with a pair:\n%s", newest)
	}
	db := execViews(t, newest)
	check(t, "following the newest snapshot", db)
	if _, err := db.Exec("SET VARIABLE " + filesVar + " = NULL"); err != nil {
		t.Fatal(err)
	}
	check(t, "following the newest snapshot, with no listing in the session", db)

	// Not listed at all: the patterns go to the engine as they are.
	in := Input{Follow: FollowNewest, ArchiveSources: []string{"s3://bucket/archives"}}
	if in.listsOnce() {
		t.Fatal("the input meant to read without a listing lists once")
	}
	plain, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if _, err := plain.Exec("SET VARIABLE " + newestVar + " = " + sqlString(snap+"/")); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range pgFollowTables(snap, cases) {
		body := pgFollowStateBody(in, tbl)
		if strings.Contains(body, filesVar) {
			t.Fatalf("the body that does not list reads the listing: %s", body)
		}
		if _, err := plain.Exec(`CREATE SCHEMA IF NOT EXISTS "` + tbl.Schema + `"; CREATE OR REPLACE VIEW "` + tbl.Schema + `"."` + tbl.Table + `" AS ` + body); err != nil {
			t.Fatalf("%s.%s: %v\n%s", tbl.Schema, tbl.Table, err, body)
		}
	}
	check(t, "following the newest snapshot, not listed", plain)
}

// A following file meets snapshots it was not written against: the same file
// reads a table rewritten bare (no pair), then with one link, then with a
// chain, each time as that snapshot leaves it.
func TestPGFollowStateView_theChainGrows(t *testing.T) {
	root := t.TempDir()
	base := writePGFollowBase(t, root, "public", "orders", [2]string{"1", "one"}, [2]string{"2", "two"})
	if err := os.WriteFile(filepath.Join(root, pgFollowStamp, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(pgFollowStamp, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
		t.Fatal(err)
	}
	tbl := pgFollowTable(filepath.Join(root, baseline.CurrentLinkName, "public", "orders.parquet"), "public", "orders")
	text := pgPairGenerate(root, FollowPointer, tbl)
	read := func(want ...string) {
		t.Helper()
		got, err := pgFollowRows(t, execViews(t, text), "public", "orders")
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%v, %v; want %v\n%s", got, err, want, text)
		}
	}
	read("1=one", "2=two")
	writePGFollowLink(t, base, 0, map[string]string{"1": "ONE"})
	read("1=ONE", "2=two")
	writePGFollowLink(t, base, 1, map[string]string{"2": "", "3": "three"})
	read("1=ONE", "3=three")
	writePGFollowLink(t, base, 2, map[string]string{"1": "uno", "2": "dos"})
	read("1=uno", "2=dos", "3=three")
}

// Over S3 the following file is written the same way, against the session's
// variable, and names the patterns of the table's chain beside its file:
// no refusal, and no listing of the store at the view's own read when the
// file lists the snapshot once.
func TestPGFollowStateView_s3(t *testing.T) {
	tbl := pgFollowTable("s3://bucket/"+pgFollowStamp+"/public/or*ders.parquet", "public", "or*ders")
	text := pgPairGenerate("s3://bucket", FollowNewest, tbl)
	for _, want := range []string{
		"public/or[*]ders.[0-9p][0-9a][0-9r][0-9q][0-9u][0-9e]*[td]",
		"public/or[*]ders.[0-9p][0-9a][0-9r][0-9q][0-9u][0-9e]*[tw]",
		`or\*ders\.[0-9]{6}\.pgnew$`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the following file over S3 does not name %s:\n%s", want, text)
		}
	}
	if strings.Contains(text, "PostgreSQL table delta beside its file") {
		t.Fatalf("the following file over S3 refuses the table:\n%s", text)
	}
	body := pgFollowStateBody(Input{Follow: FollowNewest, BaselineSource: "s3://bucket"}, tbl)
	if strings.Contains(body, "glob(") || !strings.Contains(body, filesVar) {
		t.Fatalf("a following view over S3 that lists once asks the store itself: %s", body)
	}
}

// The two tables a following view keeps off the chain's patterns, as the
// MySQL views keep theirs (chainReady): one with a column a pair reserves,
// which never gets a pair and whose file the chain's reads would not bind
// over, and one with a sibling named "<table>.<six digits>", whose file the
// patterns would take for the table's first .pgdead. Each reads its file
// alone behind the guard: the first answers, the second refuses once a pair
// is beside it, naming why, instead of answering wrong.
func TestPGFollowStateView_keptToTheFile(t *testing.T) {
	for _, follow := range []FollowMode{FollowPointer, FollowNewest} {
		t.Run(fmt.Sprint(follow), func(t *testing.T) {
			root := t.TempDir()
			snap := filepath.Join(root, pgFollowStamp)
			dir := snap
			if follow == FollowPointer {
				dir = filepath.Join(root, baseline.CurrentLinkName)
			}
			// A column named filename: written with the writer, no pair.
			res := filepath.Join(snap, "public", "logs.parquet")
			if err := os.MkdirAll(filepath.Dir(res), 0o755); err != nil {
				t.Fatal(err)
			}
			w, err := baseline.NewWriter(res, []baseline.Column{{Name: "id", RawText: true}, {Name: "filename", RawText: true}},
				baseline.WriterConfig{Compression: "none"})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.WriteRow([]string{"1", "a.log"}, []bool{false, false}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			logs := BaselineTable{Schema: "public", Table: "logs", Path: filepath.Join(dir, "public", "logs.parquet"), Rel: "public/logs.parquet",
				PGRelation: &baseline.PGRelation{OID: 16600, Columns: []baseline.PGColumn{
					{Name: "id", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 1},
					{Name: "filename", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 2},
				}, PrimaryKey: []string{"id"}}}

			orders := writePGFollowBase(t, root, "public", "orders", [2]string{"1", "one"})
			writePGFollowLink(t, orders, 0, map[string]string{"1": "ONE"})
			writePGFollowBase(t, root, "public", "orders.000000", [2]string{"8", "sibling"})
			if err := os.WriteFile(filepath.Join(snap, baseline.SuccessMarker), nil, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(pgFollowStamp, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
				t.Fatal(err)
			}
			text := pgPairGenerate(root, follow, logs,
				pgFollowTable(filepath.Join(dir, "public", "orders.parquet"), "public", "orders"),
				pgFollowTable(filepath.Join(dir, "public", "orders.000000.parquet"), "public", "orders.000000"))
			db := execViews(t, text)
			var got string
			if err := db.QueryRow(`SELECT id || '=' || filename FROM public.logs`).Scan(&got); err != nil || got != "1=a.log" {
				t.Fatalf("a table with a column named filename: %q, %v\n%s", got, err, text)
			}
			if got, err := pgFollowRows(t, db, "public", "orders.000000"); err != nil || !reflect.DeepEqual(got, []string{"8=sibling"}) {
				t.Fatalf("the sibling: %v, %v\n%s", got, err, text)
			}
			_, err = pgFollowRows(t, db, "public", "orders")
			if err == nil || !strings.Contains(err.Error(), "PostgreSQL table delta beside its file") || !strings.Contains(err.Error(), "orders.000000") {
				t.Fatalf("a table with a sibling named like its chain: %v; want the refusal naming the sibling\n%s", err, text)
			}
		})
	}
}

// A table capture dropped changes of is published in the next snapshot as
// it was (#3189): its file and its old pairs linked into the new folder
// under the same names, beside tables brought up to date. A following file
// that reaches that snapshot reads the carried table as the pinned view of
// either snapshot does: the file as its pairs leave it.
func TestPGFollowStateView_carriedTable(t *testing.T) {
	root := t.TempDir()
	base := writePGFollowBase(t, root, "public", "orders", [2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"3", "three"})
	writePGFollowLink(t, base, 0, map[string]string{"1": "ONE", "2": ""})
	writePGFollowLink(t, base, 1, map[string]string{"1": "uno", "4": "four"})
	writePGFollowBase(t, root, "public", "items", [2]string{"5", "five"})
	older := filepath.Join(root, pgFollowStamp)
	if err := os.WriteFile(filepath.Join(older, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	const newerStamp = "2026-04-30T04-00-00Z"
	newer := filepath.Join(root, newerStamp)
	if err := os.MkdirAll(filepath.Join(newer, "public"), 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(older, "public"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "orders.") {
			continue
		}
		if err := os.Link(filepath.Join(older, "public", e.Name()), filepath.Join(newer, "public", e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	// items brought up to date in the newer snapshot: a new file, no pair.
	w, err := baseline.NewWriter(filepath.Join(newer, "public", "items.parquet"), []baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}},
		baseline.WriterConfig{Compression: "none"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"5", "FIVE"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(newer, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(newerStamp, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
		t.Fatal(err)
	}
	tables := func(dir string) []BaselineTable {
		return []BaselineTable{
			pgFollowTable(filepath.Join(dir, "public", "orders.parquet"), "public", "orders"),
			pgFollowTable(filepath.Join(dir, "public", "items.parquet"), "public", "items"),
		}
	}
	want := map[string][]string{"orders": {"1=uno", "3=three", "4=four"}, "items": {"5=FIVE"}}
	for label, text := range map[string]string{
		"pinned to the newer snapshot": pgPairGenerate(root, FollowNone, tables(newer)...),
		"following the pointer":        pgPairGenerate(root, FollowPointer, tables(filepath.Join(root, baseline.CurrentLinkName))...),
		// Generated against the older snapshot, read when the newer one is
		// the newest.
		"following the newest snapshot": pgPairGenerate(root, FollowNewest, tables(older)...),
	} {
		db := execViews(t, text)
		for table, w := range want {
			if got, err := pgFollowRows(t, db, "public", table); err != nil || !reflect.DeepEqual(got, w) {
				t.Errorf("%s: %s reads %v, %v; want %v", label, table, got, err, w)
			}
		}
	}
	pinnedOlder := execViews(t, pgPairGenerate(root, FollowNone, tables(older)...))
	if got, err := pgFollowRows(t, pinnedOlder, "public", "orders"); err != nil || !reflect.DeepEqual(got, want["orders"]) {
		t.Errorf("pinned to the older snapshot: orders reads %v, %v", got, err)
	}
}
