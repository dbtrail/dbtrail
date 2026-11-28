package views

import (
	"context"
	"database/sql"
	"errors"
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

// The table of the typed-copy tests (#3315): a key of two columns, one a
// number and one text (written in that order, the key's order), every type
// a file stores typed, a numeric that stays text (its scale is its
// precision), a column named as a keyword and plain text.
var typedRel = baseline.PGRelation{OID: 33150, PrimaryKey: []string{"id", "k"}, Collations: true, Columns: []baseline.PGColumn{
	{Name: "id", TypeOID: 20, TypeMod: -1, Type: "bigint", NotNull: true, Ordinal: 1},
	{Name: "k", TypeOID: 25, TypeMod: -1, Type: "text", NotNull: true, Ordinal: 2},
	{Name: "a", TypeOID: 21, TypeMod: -1, Type: "smallint", Ordinal: 3},
	{Name: "b", TypeOID: 23, TypeMod: -1, Type: "integer", Ordinal: 4},
	{Name: "c", TypeOID: 20, TypeMod: -1, Type: "bigint", Ordinal: 5},
	{Name: "o", TypeOID: 26, TypeMod: -1, Type: "oid", Ordinal: 6},
	{Name: "d", TypeOID: 1082, TypeMod: -1, Type: "date", Ordinal: 7},
	{Name: "u", TypeOID: 2950, TypeMod: -1, Type: "uuid", Ordinal: 8},
	{Name: "n", TypeOID: 1700, TypeMod: (12<<16 | 2) + 4, Type: "numeric(12,2)", Ordinal: 9},
	{Name: "n0", TypeOID: 1700, TypeMod: (15 << 16) + 4, Type: "numeric(15,0)", Ordinal: 10},
	{Name: "w", TypeOID: 1700, TypeMod: (38<<16 | 4) + 4, Type: "numeric(38,4)", Ordinal: 11},
	{Name: "p", TypeOID: 1700, TypeMod: (2<<16 | 2) + 4, Type: "numeric(2,2)", Ordinal: 12},
	{Name: "select", TypeOID: 23, TypeMod: -1, Type: "integer", Ordinal: 13},
	{Name: "t", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 14},
}}

func typedColNames() []string {
	out := make([]string, len(typedRel.Columns))
	for i, c := range typedRel.Columns {
		out[i] = c.Name
	}
	return out
}

// N is a NULL in a test row.
const N = "\x00NULL"

func typedRows() [][]string {
	return [][]string{
		{"1", "a|b", "0", "0", "0", "0", "1970-01-01", "00000000-0000-0000-0000-000000000000", "0.00", "0", "0.0000", "0.50", "0", "plain"},
		{"2", `back\slash`, "32767", "2147483647", "9223372036854775807", "4294967295", "9999-12-31", "ffffffff-ffff-ffff-ffff-ffffffffffff", "9999999999.99", "999999999999999", "1234567890123456789012345678901234.5678", "0.99", "7", "x"},
		{"3", "", "-32768", "-2147483648", "-9223372036854775808", "16384", "0001-01-01", "12345678-9abc-def0-1234-56789abcdef0", "-9999999999.99", "-15", "-9999999999999999999999999999999999.9999", "-0.01", "-7", ""},
		{"4", "n", N, N, N, N, N, N, N, N, N, N, N, N},
		{"5", "inf", "1", "1", "1", "1", "infinity", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "0.05", "1", "0.0001", "0.01", "1", "y"},
		{"6", "ninf", "-1", "-1", "-1", "2", "-infinity", "aaaaaaaa-bbbb-cccc-dddd-000000000000", "-0.05", "-1", "-0.0001", "-0.01", "-1", "z"},
		{"7", "big", "2", "2", "2", "3", "10000-01-01", "aaaaaaaa-bbbb-cccc-dddd-111111111111", "1.50", "2", "1.5000", "0.10", "2", "w"},
		{"8", "leap", "3", "3", "3", "4", "2024-02-29", "aaaaaaaa-bbbb-cccc-dddd-222222222222", "3.00", "3", "3.0000", "0.30", "3", "v"},
	}
}

func splitNulls(row []string) ([]string, []bool) {
	vals := make([]string, len(row))
	nulls := make([]bool, len(row))
	for i, v := range row {
		if v == N {
			nulls[i] = true
		} else {
			vals[i] = v
		}
	}
	return vals, nulls
}

// writeTypedBase writes the table's file under root, with the footer the
// snapshot writes (its shape), every column text or, typed, the columns
// baseline.PGTypedColumns stores typed with their footer key.
func writeTypedBase(t *testing.T, root string, typed bool, rows [][]string) string {
	t.Helper()
	path := filepath.Join(root, "2026-04-30T03-00-00Z", "public", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	shape, err := typedRel.Encode()
	if err != nil {
		t.Fatal(err)
	}
	md := map[string]string{baseline.MetaKeyPGRelation: shape}
	type writer interface {
		WriteRow([]string, []bool) error
		Close() error
	}
	var w writer
	if typed {
		cols := baseline.PGTypedColumns(typedRel)
		enc, err := baseline.EncodePGTypedColumns(cols)
		if err != nil {
			t.Fatal(err)
		}
		md[baseline.MetaKeyPGTypedColumns] = enc
		w, err = baseline.NewPGWriter(path, typedColNames(), cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 3, Metadata: md})
		if err != nil {
			t.Fatal(err)
		}
	} else {
		cols := make([]baseline.Column, len(typedRel.Columns))
		for i, c := range typedRel.Columns {
			cols[i] = baseline.Column{Name: c.Name, RawText: true}
		}
		w, err = baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 3, Metadata: md})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range rows {
		v, n := splitNulls(r)
		if err := w.WriteRow(v, n); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// typedChanges are the changes of a pair, by key: a row, or nil for a
// deleted key.
func writeTypedPair(t *testing.T, base string, changes map[string][]string) {
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
	keyCols := []string{"id", "k"}
	at, err := pgdelta.BasePositions(context.Background(), db, base, keyCols, func(k string) bool { _, ok := changes[k]; return ok })
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = pgdelta.WritePair(pgdelta.PairInput{
		BasePath: base, Seq: 0, Base: "base-1", Run: "run-0",
		Columns: typedColNames(), KeyColumns: keyCols, Positions: at,
		Changes: func(yield func(pgdelta.Change) error) error {
			for _, k := range keys {
				c := pgdelta.Change{Key: k, Delete: changes[k] == nil}
				if !c.Delete {
					c.Values, c.Nulls = splitNulls(changes[k])
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

// typedTable is the table as the views are handed it, from its file's own
// footer as the console and the command line read it.
func typedTable(t *testing.T, path string) BaselineTable {
	t.Helper()
	footers, err := baseline.TableFootersFor(context.Background(), []string{path})
	if err != nil {
		t.Fatal(err)
	}
	in := Input{Baselines: []BaselineTable{{Schema: "public", Table: "orders", Path: path, Rel: "public/orders.parquet"}}}
	in.ApplyFooters(footers)
	return in.Baselines[0]
}

// viewShape is DESCRIBE of the view: each column's name and type.
func viewShape(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT column_name, column_type FROM (DESCRIBE public.orders)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n, ty string
		if err := rows.Scan(&n, &ty); err != nil {
			t.Fatal(err)
		}
		out = append(out, n+" "+ty)
	}
	return out
}

// viewRows is every row of the view, each column as the engine writes its
// value, in a fixed order.
func viewRows(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT * FROM public.orders`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = fmt.Sprintf("%s=%T:%v", cols[i], v, v)
		}
		out = append(out, strings.Join(parts, " "))
	}
	sort.Strings(out)
	return out, rows.Err()
}

func typedTail(t *testing.T, rows ...[]string) string {
	t.Helper()
	cols := make([]baseline.Column, len(typedRel.Columns))
	for i, c := range typedRel.Columns {
		cols[i] = baseline.Column{Name: c.Name, RawText: true}
	}
	tcols, err := baseline.TableDeltaColumns(cols)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "1-public.orders.parquet")
	w, err := baseline.NewWriter(path, tcols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		op := baseline.TableDeltaOpUpsert
		vals, nulls := splitNulls(r)
		if r[2] == "DEL" {
			op = baseline.TableDeltaOpDelete
			for i := 2; i < len(nulls); i++ {
				vals[i], nulls[i] = "", true
			}
		}
		key := pgdelta.KeyOf([]string{r[0], r[1]})
		if err := w.WriteRow(append(vals, key, op), append(nulls, false, false)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// The heart of #3315: a view over a file that stores columns typed has the
// same columns, of the same types, and the same rows as a view over the text
// file of the same COPY, with the same pairs and the same tail beside it.
func TestPGTypedViewSameAsText(t *testing.T) {
	changes := map[string][]string{
		pgdelta.KeyOf([]string{"1", "a|b"}):        {"1", "a|b", "5", "6", "7", "8", "2000-01-01", "12345678-9abc-def0-1234-56789abcdef0", "-0.05", "8", "8.0000", "0.20", "9", "changed"},
		pgdelta.KeyOf([]string{"2", `back\slash`}): nil,
		pgdelta.KeyOf([]string{"99", "new"}):       {"99", "new", N, "1", N, "1", "infinity", N, "12.34", N, "0.0000", N, N, "inserted"},
	}
	tail := typedTail(t,
		[]string{"3", "", "1", "1", "1", "1", "2024-02-29", N, "0.01", "1", "1.0000", "0.01", "1", "tail"},
		[]string{"5", "inf", "DEL"},
		[]string{"100", "t", "2", "2", "2", "2", "10000-01-01", N, N, N, N, N, N, "tail new"},
	)
	for _, tc := range []struct {
		name     string
		pair     bool
		withTail bool
	}{{"alone", false, false}, {"pair", true, false}, {"tail", false, true}, {"pair and tail", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			var shapes [2][]string
			var rows [2][]string
			var sqls [2]string
			for i, typed := range []bool{false, true} {
				root := t.TempDir()
				base := writeTypedBase(t, root, typed, typedRows())
				if tc.pair {
					writeTypedPair(t, base, changes)
				}
				tbl := typedTable(t, base)
				if typed {
					if len(tbl.PGStored) != 10 {
						t.Fatalf("the footer of the typed file says %d stored columns: %v", len(tbl.PGStored), tbl.PGStored)
					}
				}
				if tc.withTail {
					tbl.Tail = &TableTail{Paths: []string{tail}, PK: []string{"id", "k"}}
				}
				sqls[i] = pgPairGenerate(root, FollowNone, tbl)
				db := execViews(t, sqls[i])
				shapes[i] = viewShape(t, db)
				var err error
				if rows[i], err = viewRows(db); err != nil {
					t.Fatalf("typed %v: %v\n%s", typed, err, sqls[i])
				}
			}
			if !reflect.DeepEqual(shapes[0], shapes[1]) {
				t.Fatalf("the view's columns differ:\ntext  %v\ntyped %v", shapes[0], shapes[1])
			}
			if !reflect.DeepEqual(rows[0], rows[1]) {
				t.Fatalf("the view's rows differ:\ntext\n%s\ntyped\n%s\n--- typed sql ---\n%s", strings.Join(rows[0], "\n"), strings.Join(rows[1], "\n"), sqls[1])
			}
			if len(rows[0]) == 0 {
				t.Fatal("no rows")
			}
		})
	}
}

// A value of a pair that the cast refuses fails the read of a typed file's
// view as it fails the text file's: an error, never a NULL.
func TestPGTypedViewRefusesAsText(t *testing.T) {
	bc := map[string][]string{
		pgdelta.KeyOf([]string{"1", "a|b"}): {"1", "a|b", "5", "6", "7", "8", "2000-01-01 BC", N, N, N, N, N, N, "bc"},
	}
	for _, typed := range []bool{false, true} {
		root := t.TempDir()
		base := writeTypedBase(t, root, typed, typedRows())
		writeTypedPair(t, base, bc)
		db := execViews(t, pgPairGenerate(root, FollowNone, typedTable(t, base)))
		if _, err := viewRows(db); err == nil || !strings.Contains(err.Error(), "before the common era") {
			t.Fatalf("typed %v: got %v", typed, err)
		}
	}
}

// The base's row numbers are the same in a typed file as in the text file
// of the same rows, so a .pgdead names the same rows in both.
func TestPGTypedFileRowNumbers(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got [2][]string
	for i, typed := range []bool{false, true} {
		base := writeTypedBase(t, t.TempDir(), typed, typedRows())
		rows, err := db.Query(`SELECT file_row_number, id, k FROM read_parquet('` + base + `', file_row_number=true) ORDER BY file_row_number`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var n int64
			var id, k string
			if err := rows.Scan(&n, &id, &k); err != nil {
				t.Fatal(err)
			}
			got[i] = append(got[i], fmt.Sprintf("%d %s %s", n, id, k))
		}
		rows.Close()
	}
	if !reflect.DeepEqual(got[0], got[1]) || len(got[0]) != len(typedRows()) {
		t.Fatalf("row numbers differ:\n%v\n%v", got[0], got[1])
	}
}

// Every reader that reads a file's columns as PostgreSQL's text gets the
// text file's bytes from the typed file: the following views'
// (pgdelta.FollowStateSQL), a fold's (pgdelta.TextStateSQL) and the readers
// of whole rows (baseline.PGScanSource).
func TestPGTypedReadsAsText(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	changes := map[string][]string{pgdelta.KeyOf([]string{"99", "new"}): {"99", "new", N, "1", N, "1", "infinity", N, "12.34", N, "0.0000", N, N, "inserted"}}
	read := func(q string) ([]string, []string) {
		t.Helper()
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		types, _ := rows.ColumnTypes()
		var shape []string
		for i, c := range cols {
			shape = append(shape, c+" "+types[i].DatabaseTypeName())
		}
		var out []string
		for rows.Next() {
			vals := make([]sql.NullString, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprint(vals))
		}
		sort.Strings(out)
		return shape, out
	}
	var follow, fold, scan [2][2][]string
	for i, typed := range []bool{false, true} {
		base := writeTypedBase(t, t.TempDir(), typed, typedRows())
		writeTypedPair(t, base, changes)
		dead, rows, err := pgdelta.Paths(base, 0)
		if err != nil {
			t.Fatal(err)
		}
		deadGlob, rowsGlob := pgdelta.FollowGlobs(base)
		follow[i][0], follow[i][1] = read(pgdelta.FollowStateSQL("'"+pgdelta.GlobLiteral(base)+"'", "'"+deadGlob+"'", "'"+rowsGlob+"'", base))
		fold[i][0], fold[i][1] = read(pgdelta.TextStateSQL(base, []string{dead}, []string{rows}))
		src, err := baseline.PGScanSource(ctx, db, "'"+base+"'")
		if err != nil {
			t.Fatal(err)
		}
		scan[i][0], scan[i][1] = read("SELECT * FROM " + src)
	}
	for name, got := range map[string][2][2][]string{"follow": follow, "fold": fold, "scan": scan} {
		if !reflect.DeepEqual(got[0], got[1]) {
			t.Errorf("%s: the typed file reads otherwise:\ntext  %v\n%v\ntyped %v\n%v", name, got[0][0], got[0][1], got[1][0], got[1][1])
		}
		for _, c := range got[1][0] {
			if !strings.HasSuffix(c, " VARCHAR") {
				t.Errorf("%s: column %s is not text", name, c)
			}
		}
	}
}

// pgTypedCorpus is every edge case the writer is held to, by the type the
// column is stored as (baseline's own test says which it stores).
var pgTypedCorpus = map[string][]string{
	"SMALLINT": {"0", "1", "-1", "32767", "-32768", "32768", "-32769", "-0", "+1", "01", " 1", "1 ", "", "1.0", "1e2", "0x10", "1_000", "abc"},
	"INTEGER":  {"2147483647", "-2147483648", "42", "2147483648", "-2147483649", "1.5", "+5", "007"},
	"BIGINT":   {"9223372036854775807", "-9223372036854775808", "9223372036854775808", "00"},
	"UINTEGER": {"0", "4294967295", "16384", "4294967296", "-1", "+1", "01"},
	"DATE": {"2024-02-29", "1970-01-01", "1969-12-31", "0001-01-01", "1582-10-10", "9999-12-31", "10000-01-01", "294276-12-31", "5874897-12-31",
		"infinity", "-infinity", "2023-02-29", "2024-13-01", "2024-00-10", "2024-01-00", "2024-04-31", "0000-01-01", "0001-01-01 BC", "2024-01-05 BC",
		"Infinity", "2024-1-5", " 2024-01-05", "2024-01-05 ", "", "20240105", "01000-01-01", "2024/01/05"},
	"UUID": {"12345678-9abc-def0-1234-56789abcdef0", "00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff",
		"12345678-9ABC-DEF0-1234-56789ABCDEF0", "123456789abcdef0123456789abcdef0", "{12345678-9abc-def0-1234-56789abcdef0}",
		"12345678-9abc-def0-1234-56789abcdef", "12345678-9abc-def0-1234-56789abcdefg", ""},
	"DECIMAL(12,2)": {"0.00", "1.50", "-1.50", "0.05", "-0.05", "9999999999.99", "-9999999999.99", "10000000000.00", "-0.00", "1.5", "1.505", "1",
		"NaN", "Infinity", "-Infinity", "1e2", ".50", "+1.50", "01.50", " 1.50", "", "1.-5", "-"},
	"DECIMAL(5,2)":   {"999.99", "-999.99", "1000.00"},
	"DECIMAL(9,0)":   {"999999999", "-999999999"},
	"DECIMAL(15,0)":  {"0", "-15", "999999999999999", "-0", "1000000000000000", "1.0"},
	"DECIMAL(18,4)":  {"99999999999999.9999", "-99999999999999.9999"},
	"DECIMAL(19,2)":  {"99999999999999999.99", "-99999999999999999.99", "0.01"},
	"DECIMAL(38,4)":  {"1234567890123456789012345678901234.5678", "-9999999999999999999999999999999999.9999", "0.0001", "99999999999999999999999999999999999.0000"},
	"DECIMAL(38,0)":  {"99999999999999999999999999999999999999", "-99999999999999999999999999999999999999", "100000000000000000000000000000000000000"},
	"DECIMAL(38,37)": {"9.9999999999999999999999999999999999999", "-0.0000000000000000000000000000000000001"},
}

// The writer and the views' casts agree on every edge case: a value the
// writer stores typed is the value the cast reads its text as, and renders
// back as that text; a text the cast refuses, the writer does not store
// typed. (The writer also leaves to a text file some text the cast would
// take, a "+1" or " 1": PostgreSQL never writes them, and a table written
// as text reads through the cast as before.)
func TestPGTypedAgreesWithTheViewsCasts(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := t.TempDir()
	n := 0
	for as, raws := range pgTypedCorpus {
		tbl := BaselineTable{Schema: "public", Table: "t", PGCasts: []baseline.PGCast{{Name: "v", As: as}}}
		expr := pgParts(tbl)[0].expr
		for _, raw := range raws {
			n++
			typedPath := filepath.Join(dir, fmt.Sprintf("typed%d.parquet", n))
			w, err := baseline.NewPGWriter(typedPath, []string{"v"}, map[string]string{"v": as}, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
			if err != nil {
				t.Fatal(err)
			}
			werr := w.WriteRow([]string{raw}, []bool{false})
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if werr != nil && !errors.Is(werr, baseline.ErrPGNotTyped) {
				t.Fatalf("%s %q: %v", as, raw, werr)
			}
			var cast sql.NullString
			cerr := db.QueryRow("SELECT CAST(" + expr + " AS VARCHAR) FROM (SELECT " + sqlString(raw) + "::VARCHAR AS v)").Scan(&cast)
			if werr != nil {
				continue
			}
			if cerr != nil {
				t.Errorf("%s %q: the writer stores it and the cast refuses it: %v", as, raw, cerr)
				continue
			}
			var stored, text sql.NullString
			var same bool
			q := "SELECT CAST(v AS VARCHAR), CAST(v AS VARCHAR), v IS NOT DISTINCT FROM (" + strings.ReplaceAll(expr, `"v"`, sqlString(raw)+"::VARCHAR") + ") FROM read_parquet('" + typedPath + "')"
			if err := db.QueryRow(q).Scan(&stored, &text, &same); err != nil {
				t.Fatalf("%s %q: %v", as, raw, err)
			}
			if !same || stored != cast {
				t.Errorf("%s %q: stored %v, the cast reads %v", as, raw, stored, cast)
			}
			if !text.Valid || text.String != raw {
				t.Errorf("%s %q: the stored value reads back as %v", as, raw, text)
			}
			var typ string
			if err := db.QueryRow("SELECT typeof(v) FROM read_parquet('" + typedPath + "')").Scan(&typ); err != nil || typ != as {
				t.Errorf("%s %q: the file holds a %s (%v)", as, raw, typ, err)
			}
		}
	}
}

// A view that follows the `current` pointer reads every column as text, as
// it always read a PostgreSQL table: the same whether the snapshot it reads
// stores columns typed or not, and whichever kind it was written over.
func TestPGTypedFollowingViewSameAsText(t *testing.T) {
	changes := map[string][]string{
		pgdelta.KeyOf([]string{"99", "new"}): {"99", "new", N, "1", N, "1", "infinity", "aaaaaaaa-bbbb-cccc-dddd-333333333333", "12.34", N, "0.0000", N, N, "inserted"},
		// Text no typed column could hold: a following view reads it as
		// the text it is, over either kind of file.
		pgdelta.KeyOf([]string{"1", "a|b"}): {"1", "a|b", "1", "1", "1", "1", "0044-03-15 BC", "aaaaaaaa-bbbb-cccc-dddd-444444444444", "NaN", "NaN", "NaN", N, "1", "bc"},
	}
	root := t.TempDir()
	mk := func(stamp string, typed bool) string {
		dir := t.TempDir()
		base := writeTypedBase(t, dir, typed, typedRows())
		writeTypedPair(t, base, changes)
		snap := filepath.Join(root, stamp)
		if err := os.Rename(filepath.Dir(filepath.Dir(base)), snap); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(snap, baseline.SuccessMarker), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		return snap
	}
	textSnap, typedSnap := mk("2026-04-30T03-00-00Z", false), mk("2026-04-30T04-00-00Z", true)
	point := func(snap string) {
		cur := filepath.Join(root, baseline.CurrentLinkName)
		os.Remove(cur)
		if err := os.Symlink(filepath.Base(snap), cur); err != nil {
			t.Fatal(err)
		}
	}
	var got [][]string
	var shapes [][]string
	for _, writtenOver := range []string{textSnap, typedSnap} {
		point(writtenOver)
		tbl := typedTable(t, filepath.Join(root, baseline.CurrentLinkName, "public", "orders.parquet"))
		db := execViews(t, pgPairGenerate(root, FollowPointer, tbl))
		for _, reads := range []string{textSnap, typedSnap} {
			point(reads)
			shapes = append(shapes, viewShape(t, db))
			rows, err := viewRows(db)
			if err != nil {
				t.Fatalf("written over %s, reading %s: %v", writtenOver, reads, err)
			}
			got = append(got, rows)
		}
	}
	for i := 1; i < len(got); i++ {
		if !reflect.DeepEqual(shapes[0], shapes[i]) || !reflect.DeepEqual(got[0], got[i]) {
			t.Fatalf("read %d differs:\n%v\n%v\n%s\n%s", i, shapes[0], shapes[i], strings.Join(got[0], "\n"), strings.Join(got[i], "\n"))
		}
	}
	for _, c := range shapes[0] {
		if !strings.HasSuffix(c, " VARCHAR") {
			t.Fatalf("a following view reads %s", c)
		}
	}
}

// A file whose list of stored columns cannot be used (a newer version wrote
// it) is refused by its pinned view, loudly, never read through casts
// written for text.
func TestPGTypedUnusableListRefuses(t *testing.T) {
	root := t.TempDir()
	base := writeTypedBase(t, root, true, typedRows())
	tbl := typedTable(t, base)
	if tbl.PGTypedUnusable != "" {
		t.Fatalf("a good list is unusable: %s", tbl.PGTypedUnusable)
	}
	tbl.PGStored, tbl.PGTypedUnusable = nil, "version 2"
	db := execViews(t, pgPairGenerate(root, FollowNone, tbl))
	if _, err := viewRows(db); err == nil || !strings.Contains(err.Error(), "version 2") {
		t.Fatalf("got %v", err)
	}
	// A footer kept for that alone (its shape could not be read): the
	// table is not taken for a MySQL one, and its view refuses.
	in := Input{Baselines: []BaselineTable{{Schema: "public", Table: "orders", Path: base, Rel: "public/orders.parquet"}}}
	in.ApplyFooters(map[string]baseline.TableFooter{base: {PGTypedUnusable: "unread"}})
	if b := in.Baselines[0]; b.SchemaKnown || b.PGTypedUnusable != "unread" {
		t.Fatalf("got %+v", b)
	}
	db = execViews(t, pgPairGenerate(root, FollowNone, in.Baselines[0]))
	if _, err := viewRows(db); err == nil || !strings.Contains(err.Error(), "unread") {
		t.Fatalf("got %v", err)
	}
}
