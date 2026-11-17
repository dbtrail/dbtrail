package views

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

var pgPairCols = []string{"id", "status"}

// writePGPairBase writes a PostgreSQL table file of (id, status) rows,
// every column text, at <root>/<stamp>/public/<table>.parquet.
func writePGPairBase(t *testing.T, root, table string, rows ...[2]string) string {
	t.Helper()
	path := filepath.Join(root, "2026-04-30T03-00-00Z", "public", table+".parquet")
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

// writePGPair writes pair 0 beside base: each change is a key and its new
// status, "" for a deleted key.
func writePGPair(t *testing.T, base string, changes map[string]string) {
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
	_, _, _, err = pgdelta.WritePair(pgdelta.PairInput{
		BasePath: base, Seq: 0, Base: "base-1", Run: "run-0",
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
		t.Fatalf("WritePair: %v", err)
	}
}

// pgPairTable is the table as the views are handed it: its columns and
// casts from its footer, id read as a number.
func pgPairTable(path, table string) BaselineTable {
	t := BaselineTable{Schema: "public", Table: table, Path: path, Rel: "public/" + table + ".parquet"}
	in := Input{Baselines: []BaselineTable{t}}
	in.ApplyFooters(map[string]baseline.TableFooter{path: {
		Columns: pgPairCols,
		PG:      []baseline.PGCast{{Name: "id", As: "BIGINT"}, {Name: "status", As: baseline.PGAsText}},
	}})
	return in.Baselines[0]
}

func pgPairGenerate(root string, follow FollowMode, tables ...BaselineTable) string {
	return Generate(Input{
		GeneratedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), Version: "test",
		BaselineSource: root, BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow: follow, Baselines: tables,
	})
}

// pgPairRows reads a table's state view as "id=status", ordered by the
// number id is read as, and that id is a number.
func pgPairRows(t *testing.T, db *sql.DB, table string) ([]string, error) {
	t.Helper()
	rows, err := db.Query(`SELECT id::VARCHAR || '=' || status, typeof(id) FROM public."` + table + `" ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s, typ string
		if err := rows.Scan(&s, &typ); err != nil {
			return nil, err
		}
		if typ != "BIGINT" {
			t.Fatalf("id is read as %s, want BIGINT", typ)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// A PostgreSQL table with a table delta beside its file reads as the file
// and the pair leave it (#2757), with the casts its columns have, and the
// file alone when there is none.
func TestPGPairStateView(t *testing.T) {
	root := t.TempDir()
	base := writePGPairBase(t, root, "orders", [2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"3", "three"}, [2]string{"10", "ten"})
	tbl := pgPairTable(base, "orders")

	alone := pgPairGenerate(root, FollowNone, tbl)
	if strings.Contains(alone, ".pgnew") || strings.Contains(alone, ".pgdead") {
		t.Fatalf("a table with no delta names delta files:\n%s", alone)
	}
	if got, err := pgPairRows(t, execViews(t, alone), "orders"); err != nil || !reflect.DeepEqual(got, []string{"1=one", "2=two", "3=three", "10=ten"}) {
		t.Fatalf("with no delta: %v, %v", got, err)
	}

	writePGPair(t, base, map[string]string{"1": "ONE", "2": "", "7": "seven"})
	withPair := pgPairGenerate(root, FollowNone, tbl)
	got, err := pgPairRows(t, execViews(t, withPair), "orders")
	if want := []string{"1=ONE", "3=three", "7=seven", "10=ten"}; err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("with its delta: %v, %v; want %v\n%s", got, err, want, withPair)
	}
	// The table's columns, in the table's order, and none of the pair's.
	rs, err := execViews(t, withPair).Query(`SELECT * FROM public.orders LIMIT 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if cols, _ := rs.Columns(); !reflect.DeepEqual(cols, pgPairCols) {
		t.Fatalf("the view's columns are %v, want %v", cols, pgPairCols)
	}
}

// The table's tail lies over the file and its delta.
func TestPGPairStateView_withTail(t *testing.T) {
	root := t.TempDir()
	base := writePGPairBase(t, root, "orders", [2]string{"1", "one"}, [2]string{"2", "two"}, [2]string{"3", "three"})
	writePGPair(t, base, map[string]string{"1": "ONE", "2": "", "7": "seven"})

	cols, err := baseline.TableDeltaColumns([]baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}})
	if err != nil {
		t.Fatal(err)
	}
	tail := filepath.Join(t.TempDir(), "1-public.orders.parquet")
	w, err := baseline.NewWriter(tail, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][4]string{
		{"3", "THREE", "3", baseline.TableDeltaOpUpsert}, // over the file
		{"7", "", "7", baseline.TableDeltaOpDelete},      // over the pair
		{"9", "nine", "9", baseline.TableDeltaOpUpsert},  // new
	} {
		if err := w.WriteRow([]string{r[0], r[1], r[2], r[3]}, []bool{false, r[3] == baseline.TableDeltaOpDelete, false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	tbl := pgPairTable(base, "orders")
	tbl.Tail = &TableTail{Paths: []string{tail}, PK: []string{"id"}}
	sqlText := pgPairGenerate(root, FollowNone, tbl)
	got, err := pgPairRows(t, execViews(t, sqlText), "orders")
	if want := []string{"1=ONE", "3=THREE", "9=nine"}; err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("with its delta and its tail: %v, %v; want %v\n%s", got, err, want, sqlText)
	}
}

// Half a pair refuses the table when it is read, and no other table: the
// views are created, and the other table answers.
func TestPGPairStateView_brokenPairRefusesTheTableAlone(t *testing.T) {
	root := t.TempDir()
	orders := writePGPairBase(t, root, "orders", [2]string{"1", "one"})
	items := writePGPairBase(t, root, "items", [2]string{"5", "five"})
	writePGPair(t, orders, map[string]string{"1": "ONE"})
	dead, _, err := pgdelta.Paths(orders, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dead); err != nil {
		t.Fatal(err)
	}
	db := execViews(t, pgPairGenerate(root, FollowNone, pgPairTable(orders, "orders"), pgPairTable(items, "items")))
	if got, err := pgPairRows(t, db, "items"); err != nil || !reflect.DeepEqual(got, []string{"5=five"}) {
		t.Fatalf("the other table: %v, %v", got, err)
	}
	if got, err := pgPairRows(t, db, "orders"); err == nil || !strings.Contains(err.Error(), "cannot be read whole") {
		t.Fatalf("the table with half a pair: %v, %v; want a refusal", got, err)
	}
}

// A table whose name is a glob has its own delta read, and no other's.
func TestPGPairStateView_globInTheName(t *testing.T) {
	root := t.TempDir()
	star := writePGPairBase(t, root, "or*ders", [2]string{"1", "one"})
	plain := writePGPairBase(t, root, "orders", [2]string{"1", "plain"})
	writePGPair(t, star, map[string]string{"1": "STAR"})
	writePGPair(t, plain, map[string]string{"1": "PLAIN"})
	db := execViews(t, pgPairGenerate(root, FollowNone, pgPairTable(star, "or*ders"), pgPairTable(plain, "orders")))
	if got, err := pgPairRows(t, db, "or*ders"); err != nil || !reflect.DeepEqual(got, []string{"1=STAR"}) {
		t.Fatalf("the table named with a star: %v, %v", got, err)
	}
	if got, err := pgPairRows(t, db, "orders"); err != nil || !reflect.DeepEqual(got, []string{"1=PLAIN"}) {
		t.Fatalf("the other table: %v, %v", got, err)
	}
}

// What the pair's body is not written for is written as it was: a MySQL
// table (its definition is known) with such files beside it, a view that
// follows later snapshots, and a file named in S3 or by a relative path.
func TestPGPairStateBody_leavesTheOthersAlone(t *testing.T) {
	root := t.TempDir()
	base := writePGPairBase(t, root, "orders", [2]string{"1", "one"})
	writePGPair(t, base, map[string]string{"1": "ONE"})
	pg := pgPairTable(base, "orders")

	mysql := BaselineTable{Schema: "public", Table: "orders", Path: base, Rel: "public/orders.parquet", SchemaKnown: true, Columns: pgPairCols}
	mysqlDelta := BaselineTable{Schema: "public", Table: "orders", Path: base, Rel: "public/orders.parquet", Delta: true}
	relative := pg
	relative.Path = "./public/orders.parquet" // for a folder the file is moved with: nothing is there from here
	s3 := pg
	s3.Path = "s3://bucket/2026-04-30T03-00-00Z/public/orders.parquet"
	for name, c := range map[string]struct {
		in Input
		t  BaselineTable
	}{
		"a table whose definition is known": {Input{}, mysql},
		"a table with a MySQL delta":        {Input{}, mysqlDelta},
		"a view that follows the pointer":   {Input{Follow: FollowPointer}, pg},
		"a view that follows the newest":    {Input{Follow: FollowNewest}, pg},
		"a relative path with nothing there": {Input{}, relative},
		"a file in S3":                      {Input{}, s3},
	} {
		if body, ok := pgPairStateBody(c.in, c.t); ok {
			t.Errorf("%s: the pair's body was written: %s", name, body)
		}
	}
	if _, ok := pgPairStateBody(Input{}, pg); !ok {
		t.Fatal("the PostgreSQL table's own body was not written")
	}
	// And a MySQL table's whole file is the same with the files there as
	// without: nothing of its folder was asked for.
	bare := writePGPairBase(t, t.TempDir(), "orders", [2]string{"1", "one"})
	withFiles := pgPairGenerate(root, FollowNone, mysql)
	mysql.Path = bare
	without := pgPairGenerate(root, FollowNone, mysql)
	if strings.ReplaceAll(withFiles, base, "X") != strings.ReplaceAll(without, bare, "X") {
		t.Fatalf("a MySQL table's view changed with PostgreSQL delta files beside its file:\n%s\n--- without ---\n%s", withFiles, without)
	}
}

// A snapshot folder given as a relative path: the file is looked for from
// where the process runs, and the view names it as it was given.
func TestPGPairStateView_relativeFolder(t *testing.T) {
	root := t.TempDir()
	base := writePGPairBase(t, root, "orders", [2]string{"1", "one"}, [2]string{"2", "two"})
	writePGPair(t, base, map[string]string{"1": "ONE", "2": ""})
	t.Chdir(root)
	rel, err := filepath.Rel(root, base)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := pgPairGenerate(".", FollowNone, pgPairTable(rel, "orders"))
	if strings.Contains(sqlText, root) {
		t.Fatalf("the view names the file by another path than it was given:\n%s", sqlText)
	}
	if got, err := pgPairRows(t, execViews(t, sqlText), "orders"); err != nil || !reflect.DeepEqual(got, []string{"1=ONE"}) {
		t.Fatalf("with its delta, from a relative folder: %v, %v\n%s", got, err, sqlText)
	}
}

// The refusal of a pair that is not whole does not depend on the file
// holding rows, nor on what the statement asks of the view.
func TestPGPairStateView_refusalOverAnEmptyFile(t *testing.T) {
	root := t.TempDir()
	empty := writePGPairBase(t, root, "orders")
	_, rows, err := pgdelta.Paths(empty, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rows, []byte("not a pair"), 0o644); err != nil { // half a pair, by its name
		t.Fatal(err)
	}
	db := execViews(t, pgPairGenerate(root, FollowNone, pgPairTable(empty, "orders")))
	for _, q := range []string{
		`SELECT * FROM public.orders`,
		`SELECT count(*) FROM public.orders`,
		`SELECT id FROM public.orders WHERE id = 1`,
		`SELECT 1 FROM public.orders LIMIT 1`,
	} {
		rs, err := db.Query(q)
		if err == nil {
			for rs.Next() {
			}
			err = rs.Err()
			rs.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "cannot be read whole") {
			t.Errorf("%s: %v, want the refusal", q, err)
		}
	}
}

// Every character the engine reads as a pattern, in a table's name.
func TestPGPairStateView_patternCharactersInTheName(t *testing.T) {
	for _, name := range []string{"or[d]ers", "or?ers", "or{d,x}ers", "or**ers"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			base := writePGPairBase(t, root, name, [2]string{"1", "one"}, [2]string{"2", "two"})
			other := writePGPairBase(t, root, "orders", [2]string{"1", "plain"})
			writePGPair(t, base, map[string]string{"1": "ONE", "2": ""})
			db := execViews(t, pgPairGenerate(root, FollowNone, pgPairTable(base, name), pgPairTable(other, "orders")))
			if got, err := pgPairRows(t, db, name); err != nil || !reflect.DeepEqual(got, []string{"1=ONE"}) {
				t.Fatalf("%s: %v, %v", name, got, err)
			}
			if got, err := pgPairRows(t, db, "orders"); err != nil || !reflect.DeepEqual(got, []string{"1=plain"}) {
				t.Fatalf("the other table: %v, %v", got, err)
			}
		})
	}
}
