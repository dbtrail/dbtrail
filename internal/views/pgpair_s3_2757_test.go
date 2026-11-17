package views

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// linesWith is the lines of text that hold sub.
func linesWith(text, sub string) []string {
	var out []string
	for _, l := range strings.Split(text, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return out
}

// plainRows is a table's rows as text. The files of these tests carry no
// PostgreSQL types (pgPairRows asks for them), which is not what is tested
// here.
func plainRows(t *testing.T, db *sql.DB, table string) ([]string, error) {
	t.Helper()
	rs, err := db.Query(`SELECT id || '=' || status FROM public."` + table + `" ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var v string
		if err := rs.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rs.Err()
}

// The views file of a snapshot is written from the snapshot's own folder and
// names its files somewhere else: in S3, or by paths relative to a folder it
// is moved with. What is beside each table's file is looked at before the
// paths are respelled (#2757), so a PostgreSQL table with a delta is read
// with it, each file named exactly, and nothing is listed when it is read.
func TestGenerateSnapshotViews_pgPair(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	orders := writePGPairBase(t, root, "orders", [2]string{"1", "one"}, [2]string{"2", "two"})
	writePGPairBase(t, root, "items", [2]string{"5", "five"})
	writePGPair(t, orders, map[string]string{"1": "ONE", "2": ""})
	snap := filepath.Dir(filepath.Dir(orders))

	// Relative: the spelling that can be run here. From the folder itself.
	relText, ok, err := GenerateSnapshotViews(ctx, snap, ".")
	if err != nil || !ok {
		t.Fatalf("GenerateSnapshotViews, relative: %v, %v", ok, err)
	}
	if strings.Contains(relText, "[0-9]") {
		t.Fatalf("a table that was looked at carries a guard:\n%s", relText)
	}
	t.Chdir(snap)
	db := execViews(t, relText)
	if got, err := plainRows(t, db, "orders"); err != nil || !reflect.DeepEqual(got, []string{"1=ONE"}) {
		t.Fatalf("orders, with its delta: %v, %v\n%s", got, err, relText)
	}
	if got, err := plainRows(t, db, "items"); err != nil || !reflect.DeepEqual(got, []string{"5=five"}) {
		t.Fatalf("items, which has none: %v, %v\n%s", got, err, relText)
	}

	// In S3: the same statement, with the files named there.
	const s3root = "s3://bucket/copies/2026-04-30T03-00-00Z"
	s3Text, ok, err := GenerateSnapshotViews(ctx, snap, s3root)
	if err != nil || !ok {
		t.Fatalf("GenerateSnapshotViews, S3: %v, %v", ok, err)
	}
	for _, f := range []string{"orders.parquet", "orders.000000.pgdead", "orders.000000.pgnew", "items.parquet"} {
		if !strings.Contains(s3Text, s3root+"/public/"+f) {
			t.Fatalf("the S3 views do not name %s:\n%s", f, s3Text)
		}
	}
	if strings.Contains(s3Text, "[0-9]") || strings.Contains(s3Text, "glob(") {
		t.Fatalf("the S3 views list the store:\n%s", s3Text)
	}
	if strings.Contains(s3Text, snap) {
		t.Fatalf("the S3 views name a local path:\n%s", s3Text)
	}
	relLines := linesWith(relText, "orders.000000.pgnew")
	s3Lines := linesWith(s3Text, "orders.000000.pgnew")
	if len(relLines) == 0 || len(relLines) != len(s3Lines) {
		t.Fatalf("lines that name the delta: %d relative, %d in S3", len(relLines), len(s3Lines))
	}
	for i := range relLines {
		if want := strings.ReplaceAll(relLines[i], "./public/", s3root+"/public/"); s3Lines[i] != want {
			t.Fatalf("the S3 statement is not the one that was run here:\n got %s\nwant %s", s3Lines[i], want)
		}
	}
}

// A delta that is not whole is refused in the S3 views too, for that table
// alone.
func TestGenerateSnapshotViews_pgPairNotWhole(t *testing.T) {
	root := t.TempDir()
	orders := writePGPairBase(t, root, "orders", [2]string{"1", "one"})
	writePGPairBase(t, root, "items", [2]string{"5", "five"})
	_, rows, err := pgdelta.Paths(orders, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rows, []byte("half a pair, by its name"), 0o644); err != nil {
		t.Fatal(err)
	}
	snap := filepath.Dir(filepath.Dir(orders))
	text, ok, err := GenerateSnapshotViews(context.Background(), snap, "s3://bucket/copies/2026-04-30T03-00-00Z")
	if err != nil || !ok {
		t.Fatalf("GenerateSnapshotViews: %v, %v", ok, err)
	}
	if got := linesWith(text, "cannot be read whole"); len(got) != 1 || !strings.Contains(got[0], "public.orders") {
		t.Fatalf("refusals %v; want one, for orders:\n%s", got, text)
	}
	// The same refusal, where it can be run: it is raised when the table is
	// read and not when the views are made, and the other table answers.
	relText, ok, err := GenerateSnapshotViews(context.Background(), snap, ".")
	if err != nil || !ok {
		t.Fatalf("GenerateSnapshotViews, relative: %v, %v", ok, err)
	}
	t.Chdir(snap)
	db := execViews(t, relText)
	if got, err := plainRows(t, db, "items"); err != nil || !reflect.DeepEqual(got, []string{"5=five"}) {
		t.Fatalf("items: %v, %v", got, err)
	}
	if got, err := plainRows(t, db, "orders"); err == nil || !strings.Contains(err.Error(), "cannot be read whole") {
		t.Fatalf("orders: %v, %v; want the refusal", got, err)
	}
}

// What is marked and what is left: a MySQL table is never looked at, nor a
// file that is somewhere else or not there.
func TestMarkPGPairs(t *testing.T) {
	root := t.TempDir()
	orders := writePGPairBase(t, root, "orders", [2]string{"1", "one"})
	items := writePGPairBase(t, root, "items", [2]string{"5", "five"})
	mysql := writePGPairBase(t, root, "legacy", [2]string{"7", "seven"})
	mysqlDelta := writePGPairBase(t, root, "legacy2", [2]string{"8", "eight"})
	writePGPair(t, orders, map[string]string{"1": "ONE"})
	writePGPair(t, mysql, map[string]string{"7": "SEVEN"})
	writePGPair(t, mysqlDelta, map[string]string{"8": "EIGHT"})
	gone := filepath.Join(filepath.Dir(orders), "gone.parquet")

	in := Input{Baselines: []BaselineTable{
		pgPairTable(orders, "orders"),
		pgPairTable(items, "items"),
		{Schema: "public", Table: "legacy", Path: mysql, Rel: "public/legacy.parquet", SchemaKnown: true},
		{Schema: "public", Table: "legacy2", Path: mysqlDelta, Rel: "public/legacy2.parquet", Delta: true},
		pgPairTable("s3://bucket/2026-04-30T03-00-00Z/public/orders.parquet", "orders"),
		pgPairTable(gone, "gone"),
	}}
	in.MarkPGPairs()
	b := in.Baselines
	if b[0].PGPair == nil || b[0].PGPair.Err != nil || len(b[0].PGPair.Pairs) != 1 {
		t.Fatalf("the table with a delta: %+v", b[0].PGPair)
	}
	if b[1].PGPair == nil || b[1].PGPair.Err != nil || len(b[1].PGPair.Pairs) != 0 {
		t.Fatalf("the table with none: %+v; want marked, with no pair", b[1].PGPair)
	}
	for i, what := range map[int]string{2: "a table whose definition is known", 3: "a table with a MySQL delta", 4: "a file in S3", 5: "a file that is not there"} {
		if b[i].PGPair != nil {
			t.Errorf("%s was marked: %+v", what, b[i].PGPair)
		}
	}

	// Respelled to S3: the marked table with a delta has its body, the
	// marked one with none and the unmarked ones are written as before.
	in.RespellBaselines("s3://bucket/copies/2026-04-30T03-00-00Z")
	if body, ok := pgPairStateBody(in, in.Baselines[0]); !ok || !strings.Contains(body, "s3://bucket/copies/2026-04-30T03-00-00Z/public/orders.000000.pgnew") {
		t.Fatalf("the marked table, in S3: %q, %v", body, ok)
	}
	for _, i := range []int{1, 2, 3, 4, 5} {
		if body, ok := pgPairStateBody(in, in.Baselines[i]); ok {
			t.Errorf("%s was given a body: %s", in.Baselines[i].Table, body)
		}
	}
}

func TestPGPairBeside(t *testing.T) {
	for _, c := range [][3]string{
		{"s3://bucket/p/public/orders.parquet", "orders.000000.pgnew", "s3://bucket/p/public/orders.000000.pgnew"},
		{"./public/orders.parquet", "orders.000000.pgnew", "./public/orders.000000.pgnew"},
		{"/snap/public/orders.parquet", "orders.000000.pgdead", "/snap/public/orders.000000.pgdead"},
		{"orders.parquet", "orders.000000.pgdead", "orders.000000.pgdead"},
	} {
		if got := pgPairBeside(c[0], c[1]); got != c[2] {
			t.Errorf("pgPairBeside(%q, %q) = %q, want %q", c[0], c[1], got, c[2])
		}
	}
}
