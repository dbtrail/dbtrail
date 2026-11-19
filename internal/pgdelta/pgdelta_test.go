package pgdelta

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
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// v is a value and null is NULL, for a row written by the tests.
type cell struct {
	s    string
	null bool
}

func v(s string) cell { return cell{s: s} }

var null = cell{null: true}

func duck(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// writeBase writes a table file the way a PostgreSQL snapshot does: every
// column text.
func writeBase(t *testing.T, path string, cols []string, rows ...[]cell) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	bc := make([]baseline.Column, len(cols))
	for i, c := range cols {
		bc[i] = baseline.Column{Name: c, RawText: true}
	}
	w, err := baseline.NewWriter(path, bc, baseline.WriterConfig{Compression: "none", RowGroupSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		vals, nulls := make([]string, len(r)), make([]bool, len(r))
		for i, c := range r {
			vals[i], nulls[i] = c.s, c.null
		}
		if err := w.WriteRow(vals, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// change is one key's change in a test window: a row, or del.
type change struct {
	key string
	row []cell
	del bool
}

// pair writes the pair of sequence seq beside base for changes, looking the
// changed keys up in the base as a fold will.
func pair(t *testing.T, db *sql.DB, base string, seq int, cols, keyCols []string, changes ...change) (dead, rows string) {
	t.Helper()
	sort.Slice(changes, func(i, j int) bool { return changes[i].key < changes[j].key })
	wanted := map[string]bool{}
	for _, c := range changes {
		wanted[c.key] = true
	}
	at, err := BasePositions(context.Background(), db, base, keyCols, func(k string) bool { return wanted[k] })
	if err != nil {
		t.Fatal(err)
	}
	prevRun := ""
	if seq > 0 {
		prevRun = fmt.Sprintf("run-%d", seq-1)
	}
	dead, rows, _, err = WritePair(PairInput{
		BasePath: base, Seq: seq, Base: "base-1", Run: fmt.Sprintf("run-%d", seq), PrevRun: prevRun,
		Columns: cols, KeyColumns: keyCols, Positions: at,
		Changes: func(yield func(Change) error) error {
			for _, c := range changes {
				ch := Change{Key: c.key, Delete: c.del}
				for _, x := range c.row {
					ch.Values, ch.Nulls = append(ch.Values, x.s), append(ch.Nulls, x.null)
				}
				if err := yield(ch); err != nil {
					return err
				}
			}
			return nil
		},
		Metadata: map[string]string{baseline.MetaKeyLSN: "4096"},
	})
	if err != nil {
		t.Fatalf("WritePair %d: %v", seq, err)
	}
	return dead, rows
}

// state runs StateSQL and returns the rows as text, ordered, NULL as ∅.
func state(t *testing.T, db *sql.DB, base string, dead, rows []string, cols ...string) []string {
	t.Helper()
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = `"` + c + `"`
	}
	rs, err := db.Query("SELECT " + strings.Join(quoted, ", ") + " FROM (" + StateSQL(base, dead, rows) + ") ORDER BY ALL")
	if err != nil {
		t.Fatalf("the state: %v\n%s", err, StateSQL(base, dead, rows))
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		vals := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range vals {
			dest[i] = &vals[i]
		}
		if err := rs.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(vals))
		for i, x := range vals {
			parts[i] = "∅"
			if x.Valid {
				parts[i] = "[" + x.String + "]"
			}
		}
		out = append(out, strings.Join(parts, " "))
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func want(t *testing.T, what string, got []string, rows ...string) {
	t.Helper()
	sort.Strings(rows)
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("%s:\n got  %q\n want %q", what, got, rows)
	}
}

// The state through two windows: an update, a delete, an insert, NULL
// against an empty string both ways, an update of a row an earlier pair
// already changed, a deleted row put back, an inserted row deleted, and the
// deletion of a key the base never held.
func TestStateThroughAChain(t *testing.T) {
	db := duck(t)
	base := filepath.Join(t.TempDir(), "public", "orders.parquet")
	cols, key := []string{"id", "status", "note"}, []string{"id"}
	writeBase(t, base, cols,
		[]cell{v("1"), v("paid"), v("a")},
		[]cell{v("2"), v("open"), v("b")},
		[]cell{v("3"), v("keep"), v("c")},
		[]cell{v("4"), null, v("d")},
		[]cell{v("5"), v(""), v("e")},
	)
	want(t, "no pair", state(t, db, base, nil, nil, cols...),
		"[1] [paid] [a]", "[2] [open] [b]", "[3] [keep] [c]", "[4] ∅ [d]", "[5] [] [e]")

	d0, r0 := pair(t, db, base, 0, cols, key,
		change{key: "1", row: []cell{v("1"), v("shipped"), v("a")}},
		change{key: "2", del: true},
		change{key: "6", row: []cell{v("6"), v("new"), null}},
		change{key: "4", row: []cell{v("4"), v(""), v("d")}},
		change{key: "5", row: []cell{v("5"), null, v("e")}},
	)
	want(t, "one pair", state(t, db, base, []string{d0}, []string{r0}, cols...),
		"[1] [shipped] [a]", "[3] [keep] [c]", "[4] [] [d]", "[5] ∅ [e]", "[6] [new] ∅")

	d1, r1 := pair(t, db, base, 1, cols, key,
		change{key: "1", row: []cell{v("1"), v("late"), v("a")}},
		change{key: "2", row: []cell{v("2"), v("back"), v("b2")}},
		change{key: "6", del: true},
		change{key: "9", del: true},
	)
	got := state(t, db, base, []string{d0, d1}, []string{r0, r1}, cols...)
	want(t, "two pairs", got, "[1] [late] [a]", "[2] [back] [b2]", "[3] [keep] [c]", "[4] [] [d]", "[5] ∅ [e]")
	// Whatever order the chain's files are listed in.
	want(t, "two pairs, listed backwards", state(t, db, base, []string{d1, d0}, []string{r1, r0}, cols...), got...)

	// The state has the base's columns and no other.
	rs, err := db.Query("SELECT * FROM (" + StateSQL(base, []string{d0, d1}, []string{r0, r1}) + ") LIMIT 0")
	if err != nil {
		t.Fatal(err)
	}
	names, _ := rs.Columns()
	rs.Close()
	sort.Strings(names)
	if !reflect.DeepEqual(names, []string{"id", "note", "status"}) {
		t.Fatalf("the state's columns = %v", names)
	}
}

// Keys are bytes: case, an accent, a trailing space, a pipe and a backslash
// each make another row, under a session that folds case and accents too. A
// folder with a quote in its name is read.
func TestKeysAreBytes(t *testing.T) {
	db := duck(t)
	if _, err := db.Exec("SET default_collation = 'nocase.noaccent'"); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(t.TempDir(), "it's a folder", "t.parquet")
	cols := []string{"k", "val"}
	keys := []string{"a", "A", "e", "é", "x", "x ", "a|b", `a\b`, "it's", `q"q`, "", "b", "B", "o", "ö"}
	var rows [][]cell
	for _, k := range keys {
		rows = append(rows, []cell{v(k), v("base")})
	}
	writeBase(t, base, cols, rows...)

	// One of each pair of look-alikes changes, the other stays.
	d0, r0 := pair(t, db, base, 0, cols, []string{"k"},
		change{key: KeyOf([]string{"A"}), row: []cell{v("A"), v("new")}},
		change{key: KeyOf([]string{"é"}), row: []cell{v("é"), v("new")}},
		change{key: KeyOf([]string{"x "}), del: true},
		change{key: KeyOf([]string{"a|b"}), row: []cell{v("a|b"), v("new")}},
		change{key: KeyOf([]string{`a\b`}), del: true},
		change{key: KeyOf([]string{""}), row: []cell{v(""), v("new")}},
		// Two look-alikes changed by the SAME pair, to different rows: a
		// comparison that folded them would give each row to both keys.
		change{key: KeyOf([]string{"b"}), row: []cell{v("b"), v("lower")}},
		change{key: KeyOf([]string{"B"}), row: []cell{v("B"), v("upper")}},
		change{key: KeyOf([]string{"o"}), row: []cell{v("o"), v("plain")}},
		change{key: KeyOf([]string{"ö"}), del: true},
	)
	// A later pair changes the lower-case one: it must not replace "A".
	d1, r1 := pair(t, db, base, 1, cols, []string{"k"},
		change{key: KeyOf([]string{"a"}), row: []cell{v("a"), v("newer")}},
		change{key: KeyOf([]string{"e"}), del: true},
	)
	want(t, "look-alike keys", state(t, db, base, []string{d0, d1}, []string{r0, r1}, cols...),
		"[] [new]", "[A] [new]", "[a] [newer]", "[a|b] [new]", "[it's] [base]", `[q"q] [base]`, "[x] [base]", "[é] [new]",
		"[B] [upper]", "[b] [lower]", "[o] [plain]")
}

// KeyOf is the key the capture writes: the same text event.BuildPKValues
// gives for a row of text values, one column or several.
func TestKeyOfIsTheIndexKey(t *testing.T) {
	cols := []metadata.ColumnMeta{{Name: "a"}, {Name: "b"}}
	for _, parts := range [][]string{{"1", "2"}, {"a|b", "c"}, {`a\`, `|b`}, {"", ""}, {"é", "x y "}, {`\|`, "|"}} {
		row := map[string]any{"a": parts[0], "b": parts[1]}
		if got, idx := KeyOf(parts), event.BuildPKValues(cols, row); got != idx {
			t.Errorf("KeyOf(%q) = %q, and the index writes %q", parts, got, idx)
		}
	}
	if got := KeyOf([]string{"only"}); got != "only" {
		t.Errorf("one column: %q", got)
	}
}

func TestBasePositions(t *testing.T) {
	db := duck(t)
	ctx := context.Background()
	dir := t.TempDir()
	all := func(string) bool { return true }

	two := filepath.Join(dir, "two.parquet")
	writeBase(t, two, []string{"a", "b", "val"}, []cell{v("1"), v("x"), v("r0")}, []cell{v("1"), v("y"), v("r1")}, []cell{v("2"), v("x"), v("r2")})
	got, err := BasePositions(ctx, db, two, []string{"a", "b"}, func(k string) bool { return k != "1|x" })
	if err != nil || !reflect.DeepEqual(got, map[string]int64{"1|y": 1, "2|x": 2}) {
		t.Fatalf("a two-column key, one key not wanted: %v, %v", got, err)
	}
	// The key's order is the caller's, not the file's.
	if got, err = BasePositions(ctx, db, two, []string{"b", "a"}, all); err != nil || got["x|1"] != 0 || got["y|1"] != 1 || len(got) != 3 {
		t.Fatalf("the key the other way round: %v, %v", got, err)
	}
	if got, err = BasePositions(ctx, db, two, []string{"a", "b"}, func(string) bool { return false }); err != nil || len(got) != 0 {
		t.Fatalf("nothing wanted: %v, %v", got, err)
	}

	twice := filepath.Join(dir, "twice.parquet")
	writeBase(t, twice, []string{"id"}, []cell{v("7")}, []cell{v("8")}, []cell{v("7")})
	if _, err := BasePositions(ctx, db, twice, []string{"id"}, all); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("a key the file holds twice: %v", err)
	}
	// Not an error while nobody asks for that key.
	if got, err := BasePositions(ctx, db, twice, []string{"id"}, func(k string) bool { return k == "8" }); err != nil || got["8"] != 1 {
		t.Fatalf("the other key of that file: %v, %v", got, err)
	}

	noKey := filepath.Join(dir, "nokey.parquet")
	writeBase(t, noKey, []string{"id"}, []cell{v("1")}, []cell{null})
	if _, err := BasePositions(ctx, db, noKey, []string{"id"}, all); err == nil || !strings.Contains(err.Error(), "no value in key column") {
		t.Fatalf("a NULL key: %v", err)
	}
	if _, err := BasePositions(ctx, db, two, nil, all); err == nil {
		t.Fatal("no key column gave no error")
	}
	if _, err := BasePositions(ctx, db, filepath.Join(dir, "missing.parquet"), []string{"id"}, all); err == nil {
		t.Fatal("a missing file gave no error")
	}
}

func TestWritePairRefuses(t *testing.T) {
	// In a folder this test may write in: a table file "in a bucket" that
	// was not refused would be made here, as a folder named "s3:".
	t.Chdir(t.TempDir())
	base := filepath.Join(t.TempDir(), "t.parquet")
	one := func(changes ...Change) func(func(Change) error) error {
		return func(yield func(Change) error) error {
			for _, c := range changes {
				if err := yield(c); err != nil {
					return err
				}
			}
			return nil
		}
	}
	row := func(k string) Change { return Change{Key: k, Values: []string{k, "x"}, Nulls: []bool{false, false}} }
	good := PairInput{BasePath: base, Seq: 0, Base: "b", Run: "r", Columns: []string{"id", "val"}, KeyColumns: []string{"id"},
		Positions: map[string]int64{"1": 0, "2": 5}, Changes: one(row("1"), row("2"))}
	with := func(f func(*PairInput)) PairInput { in := good; f(&in); return in }
	cases := []struct {
		name string
		in   PairInput
		is   error
	}{
		{"a column named as the pair's key", with(func(in *PairInput) { in.Columns = []string{"id", KeyColumn} }), ErrReservedColumn},
		{"a column named as the pair's sequence", with(func(in *PairInput) { in.Columns = []string{SeqColumn, "val"} }), ErrReservedColumn},
		{"a column named file_row_number", with(func(in *PairInput) { in.Columns = []string{"id", "file_row_number"} }), ErrReservedColumn},
		{"a column named twice", with(func(in *PairInput) { in.Columns = []string{"id", "id"} }), ErrReservedColumn},
		// The engine that reads the files sets case aside.
		{"two columns that differ by case", with(func(in *PairInput) { in.Columns = []string{"id", "ID"} }), ErrReservedColumn},
		{"the pair's key column in capitals", with(func(in *PairInput) { in.Columns = []string{"id", "BINTRAIL_PK"} }), ErrReservedColumn},
		{"File_Row_Number", with(func(in *PairInput) { in.Columns = []string{"id", "File_Row_Number"} }), ErrReservedColumn},
		{"no key column", with(func(in *PairInput) { in.KeyColumns = nil }), nil},
		{"a key column the table does not have", with(func(in *PairInput) { in.KeyColumns = []string{"nope"} }), nil},
		{"a key column in another case", with(func(in *PairInput) { in.KeyColumns = []string{"ID"} }), nil},
		// One dead row for the one key, so nothing else refuses these two.
		{"a row filed under another row's key", with(func(in *PairInput) {
			in.Positions, in.Changes = map[string]int64{"1": 0}, one(Change{Key: "1", Values: []string{"2", "x"}, Nulls: []bool{false, false}})
		}), nil},
		{"a row with no value in its key column", with(func(in *PairInput) {
			in.Positions, in.Changes = map[string]int64{"1": 0}, one(Change{Key: "1", Values: []string{"1", "x"}, Nulls: []bool{true, false}})
		}), nil},
		{"rows of the base and no changed key", with(func(in *PairInput) { in.Changes = one() }), nil},
		{"the first pair with a pair before it", with(func(in *PairInput) { in.PrevRun = "r0" }), nil},
		{"a later pair with none before it", with(func(in *PairInput) { in.Seq = 1 }), nil},
		{"a table file in a bucket", with(func(in *PairInput) { in.BasePath = "s3://bucket/snap/public/t.parquet" }), nil},
		{"no column", with(func(in *PairInput) { in.Columns = nil }), nil},
		// A row of the base dies because its key changed, and for nothing
		// else: a row whose key did not change would vanish.
		{"a key placed in the base that did not change", with(func(in *PairInput) { in.Positions = map[string]int64{"1": 0, "9": 7} }), nil},
		{"only a key that did not change", with(func(in *PairInput) { in.Positions = map[string]int64{"9": 7} }), nil},
		{"two keys at one row of the base", with(func(in *PairInput) { in.Positions = map[string]int64{"1": 3, "2": 3} }), nil},
		{"a row below zero", with(func(in *PairInput) { in.Positions = map[string]int64{"1": -1} }), nil},
		{"keys out of order", with(func(in *PairInput) { in.Changes = one(row("2"), row("1")) }), nil},
		{"a key twice", with(func(in *PairInput) { in.Changes = one(row("1"), row("1")) }), nil},
		{"a row with a value missing", with(func(in *PairInput) {
			in.Positions, in.Changes = map[string]int64{"1": 0}, one(Change{Key: "1", Values: []string{"1"}, Nulls: []bool{false}})
		}), nil},
		{"nothing changed", with(func(in *PairInput) { in.Positions, in.Changes = nil, one() }), ErrNothingToWrite},
		{"nothing changed, no source of changes", with(func(in *PairInput) { in.Positions, in.Changes = nil, nil }), ErrNothingToWrite},
		{"no base id", with(func(in *PairInput) { in.Base = "" }), nil},
		{"no run", with(func(in *PairInput) { in.Run = "" }), nil},
		{"a sequence past the last", with(func(in *PairInput) { in.Seq = MaxSeq + 1 }), nil},
		{"a base that is not a table file", with(func(in *PairInput) { in.BasePath = strings.TrimSuffix(base, ".parquet") }), nil},
		{"the source of changes fails", with(func(in *PairInput) {
			in.Changes = func(yield func(Change) error) error { _ = yield(row("1")); return errors.New("the fold broke") }
		}), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dead, rows, keys, err := WritePair(tc.in)
			if err == nil || (tc.is != nil && !errors.Is(err, tc.is)) {
				t.Fatalf("WritePair = %q, %q, %d, %v; want an error (%v)", dead, rows, keys, err, tc.is)
			}
			if dead != "" || rows != "" || keys != 0 {
				t.Errorf("a refused pair answered %q, %q, %d", dead, rows, keys)
			}
			left, _ := filepath.Glob(filepath.Join(filepath.Dir(base), "*"))
			if len(left) != 0 {
				t.Errorf("a refused pair left files: %v", left)
			}
			// And made no folder named after a URL.
			if _, serr := os.Stat("s3:"); serr == nil {
				os.RemoveAll("s3:")
				t.Error("a table file in a bucket was written as a local folder named s3:")
			}
		})
	}
	// A deletion alone is a pair: of a key the base never held, with no
	// dead row, and of one it holds, whose row dies.
	if _, _, keys, err := WritePair(with(func(in *PairInput) { in.Positions, in.Changes = nil, one(Change{Key: "9", Delete: true}) })); err != nil || keys != 1 {
		t.Fatalf("a deletion alone: %d, %v", keys, err)
	}
	if _, _, keys, err := WritePair(with(func(in *PairInput) {
		in.BasePath, in.Positions, in.Changes = filepath.Join(t.TempDir(), "t.parquet"), map[string]int64{"9": 4}, one(Change{Key: "9", Delete: true})
	})); err != nil || keys != 1 {
		t.Fatalf("the deletion of a row the base holds: %d, %v", keys, err)
	}

	// A pair is written once. A second write of the same sequence is
	// refused, whether it would have succeeded or failed, and the pair that
	// was there is still there, whole.
	dir := t.TempDir()
	in := with(func(in *PairInput) { in.BasePath = filepath.Join(dir, "t.parquet") })
	dead, rows, _, err := WritePair(in)
	if err != nil {
		t.Fatal(err)
	}
	before := func() [2]int64 {
		var out [2]int64
		for i, f := range []string{dead, rows} {
			st, err := os.Stat(f)
			if err != nil {
				t.Fatalf("the pair that was there is gone: %v", err)
			}
			out[i] = st.Size()
		}
		return out
	}
	sizes := before()
	for name, again := range map[string]PairInput{
		"the same write again": in,
		"a write that fails":   func() PairInput { x := in; x.Changes = one(row("2"), row("1")); return x }(),
		"a write with nothing": func() PairInput { x := in; x.Positions, x.Changes = nil, one(); return x }(),
	} {
		if _, _, _, err := WritePair(again); !errors.Is(err, ErrPairExists) {
			t.Errorf("%s over a pair that is there: %v, want ErrPairExists", name, err)
		}
		if got := before(); got != sizes {
			t.Errorf("%s changed the pair that was there: sizes %v, were %v", name, got, sizes)
		}
	}
	// Half a pair left by something else is not written over either.
	half := with(func(in *PairInput) { in.BasePath = filepath.Join(t.TempDir(), "t.parquet") })
	_, lone, _ := Paths(half.BasePath, 0)
	if err := os.WriteFile(lone, []byte("not ours"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := WritePair(half); !errors.Is(err, ErrPairExists) {
		t.Errorf("a write beside half a pair: %v, want ErrPairExists", err)
	}
	if b, _ := os.ReadFile(lone); string(b) != "not ours" {
		t.Errorf("the file that was there was replaced")
	}
	// Nothing is left under a name still being written, by the write that
	// worked or by the three that were refused; and such a name is told
	// apart from a pair's.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, ok := ParseName(e.Name()); !ok || IsTemp(e.Name()) {
			t.Errorf("left in the folder: %s", e.Name())
		}
	}
	if len(entries) != 2 {
		t.Errorf("the folder holds %d files, want the pair's two", len(entries))
	}
	for name, want := range map[string]bool{
		"t.000000.pgnew.tmp-123": true, "t.000000.pgdead.tmp-9x": true,
		"t.000000.pgnew": false, "t.000000.pgdead": false, "t.parquet": false, "t.000000.upserts": false, "x.tmp-1": false,
	} {
		if IsTemp(name) != want {
			t.Errorf("IsTemp(%q) = %v", name, !want)
		}
		if _, ok := ParseName(name); ok && want {
			t.Errorf("a file still being written parses as a pair's: %q", name)
		}
	}
}

// Two writes of the same pair at the same moment: exactly one is the pair,
// the other is told the pair exists, and what is left is one whole pair
// from one write.
func TestWritePairTwiceAtOnce(t *testing.T) {
	db := duck(t)
	for round := range 20 {
		dir := t.TempDir()
		base := filepath.Join(dir, "t.parquet")
		results := make(chan error, 2)
		for _, run := range []string{"run-a", "run-b"} {
			go func() {
				_, _, _, err := WritePair(PairInput{BasePath: base, Seq: 0, Base: "b", Run: run, Columns: []string{"id", "val"}, KeyColumns: []string{"id"},
					Positions: map[string]int64{"1": 0},
					Changes: func(yield func(Change) error) error {
						return yield(Change{Key: "1", Values: []string{"1", run}, Nulls: []bool{false, false}})
					}})
				results <- err
			}()
		}
		var won, refused int
		for range 2 {
			switch err := <-results; {
			case err == nil:
				won++
			case errors.Is(err, ErrPairExists):
				refused++
			default:
				t.Fatalf("round %d: %v", round, err)
			}
		}
		if won != 1 || refused != 1 {
			t.Fatalf("round %d: %d writes won and %d were refused, want one of each", round, won, refused)
		}
		entries, _ := os.ReadDir(dir)
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		chain, err := ChainOf("t", names)
		if err != nil || len(chain.Pairs) != 1 || len(names) != 2 {
			t.Fatalf("round %d: the folder holds %v (%v)", round, names, err)
		}
		if _, err := CheckChain(context.Background(), db, dir, chain, "b", []string{"id", "val"}); err != nil {
			t.Fatalf("round %d: the pair left is not from one write: %v", round, err)
		}
	}
}

// The key's columns are checked in the order the index writes a key in: on
// a table (a, b) the key is "a|b" whatever order the primary key declares.
func TestWritePairChecksTheKeyOrder(t *testing.T) {
	change := func(yield func(Change) error) error {
		return yield(Change{Key: "1|x", Values: []string{"1", "x", "v"}, Nulls: []bool{false, false, false}})
	}
	in := PairInput{Seq: 0, Base: "b", Run: "r", Columns: []string{"a", "b", "val"}, Changes: change}
	in.BasePath, in.KeyColumns = filepath.Join(t.TempDir(), "t.parquet"), []string{"a", "b"}
	if _, _, keys, err := WritePair(in); err != nil || keys != 1 {
		t.Fatalf("the table's order: %d, %v", keys, err)
	}
	in.BasePath, in.KeyColumns = filepath.Join(t.TempDir(), "t.parquet"), []string{"b", "a"}
	if _, _, _, err := WritePair(in); err == nil || !strings.Contains(err.Error(), `make "x|1"`) {
		t.Fatalf("the key's columns the other way round: %v", err)
	}
}

func TestCheckChain(t *testing.T) {
	db := duck(t)
	ctx := context.Background()
	dir := t.TempDir()
	base := filepath.Join(dir, "t.parquet")
	tcols := []string{"val", "id"} // any order
	writeBase(t, base, []string{"id", "val"}, []cell{v("1"), v("a")}, []cell{v("2"), v("b")})
	up := func(k, val string) change { return change{key: k, row: []cell{v(k), v(val)}} }
	pair(t, db, base, 0, []string{"id", "val"}, []string{"id"}, up("1", "x"))
	pair(t, db, base, 1, []string{"id", "val"}, []string{"id"}, up("2", "y"))

	names := func() []string {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range entries {
			out = append(out, e.Name())
		}
		return out
	}
	chain, err := ChainOf("t", names())
	if err != nil || len(chain.Pairs) != 2 {
		t.Fatalf("ChainOf: %+v, %v", chain, err)
	}
	last, err := CheckChain(ctx, db, dir, chain, "base-1", tcols)
	if err != nil || last[MetaKeySeq] != "1" || last[baseline.MetaKeyLSN] != "4096" || last[MetaKeyRun] != "run-1" {
		t.Fatalf("a whole chain: %v, %v", last, err)
	}
	if last, err := CheckChain(ctx, db, dir, Chain{Table: "t"}, "base-1", tcols); err != nil || last != nil {
		t.Fatalf("an empty chain: %v, %v", last, err)
	}
	if _, err := CheckChain(ctx, db, dir, chain, "another-base", tcols); !errors.Is(err, ErrBrokenChain) || !strings.Contains(err.Error(), "another table file") {
		t.Fatalf("a chain of another base: %v", err)
	}
	if _, err := CheckChain(ctx, db, dir, chain, "", tcols); err == nil {
		t.Fatal("no base id gave no error")
	}
	// The table file has a column the pairs were not written with (or lacks
	// one): their rows would read as NULL there.
	for _, other := range [][]string{{"id", "val", "added"}, {"id"}, {"id", "VAL"}, nil} {
		if _, err := CheckChain(ctx, db, dir, chain, "base-1", other); !errors.Is(err, ErrBrokenChain) || !strings.Contains(err.Error(), "written with the columns") {
			t.Fatalf("pairs of other columns than %v: %v", other, err)
		}
	}

	// The second pair's rows, written again by another run: the two files of
	// that pair no longer come from one write.
	d1, r1, _ := Paths(base, 1)
	if err := os.Remove(r1); err != nil {
		t.Fatal(err)
	}
	_, other, _, err := WritePair(PairInput{BasePath: filepath.Join(dir, "other", "t.parquet"), Seq: 1, Base: "base-1", Run: "another-run", PrevRun: "run-0",
		Columns: []string{"id", "val"}, KeyColumns: []string{"id"}, Changes: func(yield func(Change) error) error {
			return yield(Change{Key: "2", Values: []string{"2", "z"}, Nulls: []bool{false, false}})
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(other, r1); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckChain(ctx, db, dir, chain, "base-1", tcols); !errors.Is(err, ErrBrokenChain) || !strings.Contains(err.Error(), "different writes") {
		t.Fatalf("a pair from two writes: %v", err)
	}

	// Both files of the second pair from that other write: a whole pair, of
	// the right number, that another writer put there. It was written after
	// a pair this chain does not hold.
	otherDead, _, _ := Paths(filepath.Join(dir, "other", "t.parquet"), 1)
	if err := os.Rename(otherDead, d1); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckChain(ctx, db, dir, chain, "base-1", tcols); err != nil {
		t.Fatalf("a pair that follows the one before it, from another run: %v", err)
	}
	if err := os.Remove(d1); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(r1); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := WritePair(PairInput{BasePath: base, Seq: 1, Base: "base-1", Run: "run-1b", PrevRun: "a-pair-0-of-another-writer",
		Columns: []string{"id", "val"}, KeyColumns: []string{"id"}, Changes: func(yield func(Change) error) error {
			return yield(Change{Key: "2", Values: []string{"2", "w"}, Nulls: []bool{false, false}})
		}}); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckChain(ctx, db, dir, chain, "base-1", tcols); !errors.Is(err, ErrBrokenChain) || !strings.Contains(err.Error(), "does not follow the pair before it") {
		t.Fatalf("a pair written after another pair 0: %v", err)
	}

	// A file under the name of another sequence.
	d0, _, _ := Paths(base, 0)
	if err := os.Rename(d1, d0); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckChain(ctx, db, dir, Chain{Table: "t", Pairs: chain.Pairs[:1]}, "base-1", tcols); !errors.Is(err, ErrBrokenChain) || !strings.Contains(err.Error(), "says it is pair") {
		t.Fatalf("a file under another sequence's name: %v", err)
	}
	// A file that is not there.
	if _, err := CheckChain(ctx, db, dir, chain, "base-1", tcols); err == nil {
		t.Fatal("a missing file gave no error")
	}
}

func TestBaseID(t *testing.T) {
	taken := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	good := baseline.DumpMetadata{LSN: 0x500, PGDeltaSince: "2026-11-15T09:59:00Z", SnapshotTimestamp: taken, RenderGUCs: baseline.RenderGUCsPinned}
	id, err := BaseID(good, 1234, 10)
	if err != nil || id == "" {
		t.Fatalf("BaseID: %q, %v", id, err)
	}
	// Any part that differs is another base.
	for name, other := range map[string]func() (string, error){
		"another LSN":   func() (string, error) { m := good; m.LSN++; return BaseID(m, 1234, 10) },
		"another bound": func() (string, error) { m := good; m.PGDeltaSince = "2026-11-15T09:58:00Z"; return BaseID(m, 1234, 10) },
		"another time": func() (string, error) {
			m := good
			m.SnapshotTimestamp = taken.Add(time.Microsecond)
			return BaseID(m, 1234, 10)
		},
		"another size": func() (string, error) { return BaseID(good, 1235, 10) },
		// Two tables of one snapshot share everything above but these two.
		"another row count": func() (string, error) { return BaseID(good, 1234, 11) },
		"no row":            func() (string, error) { return BaseID(good, 1234, 0) },
	} {
		if got, err := other(); err != nil || got == id {
			t.Errorf("%s: %q, %v; want another id than %q", name, got, err, id)
		}
	}
	// The same instant in another zone is the same base.
	m := good
	m.SnapshotTimestamp = taken.In(time.FixedZone("x", 3*3600))
	if got, _ := BaseID(m, 1234, 10); got != id {
		t.Errorf("the same instant in another zone: %q, want %q", got, id)
	}
	for name, bad := range map[string]func() (string, error){
		"no LSN":   func() (string, error) { m := good; m.LSN = 0; return BaseID(m, 1234, 10) },
		"no bound": func() (string, error) { m := good; m.PGDeltaSince = ""; return BaseID(m, 1234, 10) },
		"no time":  func() (string, error) { m := good; m.SnapshotTimestamp = time.Time{}; return BaseID(m, 1234, 10) },
		"no size":  func() (string, error) { return BaseID(good, 0, 10) },
		"no record of the settings its values were rendered under": func() (string, error) {
			m := good
			m.RenderGUCs = ""
			return BaseID(m, 1234, 10)
		},
		"a row count below zero": func() (string, error) { return BaseID(good, 1234, -1) },
	} {
		if got, err := bad(); err == nil {
			t.Errorf("%s: %q, want an error", name, got)
		}
	}
}

// A chain past its tenth pair, a pair with no dead row (an insert alone, a
// deletion of a key the base never held), the files under a folder named
// like a Hive partition, and a file listed twice.
func TestStateOfALongChain(t *testing.T) {
	db := duck(t)
	base := filepath.Join(t.TempDir(), "env=prod", "public", "t.parquet")
	cols, key := []string{"id", "val"}, []string{"id"}
	writeBase(t, base, cols, []cell{v("1"), v("base")}, []cell{v("2"), v("base")})
	if n, err := BaseRows(context.Background(), db, base); err != nil || n != 2 {
		t.Fatalf("BaseRows = %d, %v", n, err)
	}
	var dead, rows []string
	for seq := range 12 {
		changes := []change{{key: "1", row: []cell{v("1"), v(fmt.Sprintf("v%d", seq))}}}
		switch seq {
		case 3: // an insert alone would have no dead row; with it, one.
			changes = append(changes, change{key: "3", row: []cell{v("3"), v("new")}})
		case 7:
			changes = []change{{key: "9", del: true}} // a key the base never held: no dead row at all
		case 10:
			changes = append(changes, change{key: "3", del: true})
		}
		d, r := pair(t, db, base, seq, cols, key, changes...)
		dead, rows = append(dead, d), append(rows, r)
	}
	got := state(t, db, base, dead, rows, cols...)
	want(t, "twelve pairs", got, "[1] [v11]", "[2] [base]")
	// The folder's name is not a column.
	rs, err := db.Query("SELECT * FROM (" + StateSQL(base, dead, rows) + ") LIMIT 0")
	if err != nil {
		t.Fatal(err)
	}
	names, _ := rs.Columns()
	rs.Close()
	sort.Strings(names)
	if !reflect.DeepEqual(names, []string{"id", "val"}) {
		t.Fatalf("columns = %v", names)
	}
	// A file named twice is read once, also under another spelling of its
	// path.
	want(t, "every file listed twice", state(t, db, base, append(dead, dead...), append(rows, rows...), cols...), got...)
	respelled := func(paths []string) []string {
		out := append([]string{}, paths...)
		for _, p := range paths {
			out = append(out, filepath.Dir(p)+"/./"+filepath.Base(p))
		}
		return out
	}
	want(t, "every file listed under two spellings", state(t, db, base, respelled(dead), respelled(rows), cols...), got...)
	// Up to the tenth pair only: pair 9's row, not pair 10's or 11's.
	want(t, "the first ten pairs", state(t, db, base, dead[:10], rows[:10], cols...), "[1] [v9]", "[2] [base]", "[3] [new]")
}

// A table whose name is a glob is read as its own file: the engine takes a
// path as a pattern, and "or*ders" would be read with "orders" beside it,
// one file's row numbers taken for the other's.
func TestGlobInAFileName(t *testing.T) {
	db := duck(t)
	dir := t.TempDir()
	cols := []string{"id", "status"}
	star := filepath.Join(dir, "or*ders.parquet")
	plain := filepath.Join(dir, "orders.parquet")
	// The other table holds the same keys, at other rows.
	writeBase(t, star, cols, []cell{v("1"), v("one")}, []cell{v("2"), v("two")})
	writeBase(t, plain, cols, []cell{v("9"), v("nine")}, []cell{v("2"), v("other two")}, []cell{v("1"), v("other one")})

	at, err := BasePositions(context.Background(), db, star, []string{"id"}, func(string) bool { return true })
	if err != nil || len(at) != 2 || at["1"] != 0 || at["2"] != 1 {
		t.Fatalf("BasePositions of the table named with a star: %v, %v", at, err)
	}
	if n, err := BaseRows(context.Background(), db, star); err != nil || n != 2 {
		t.Fatalf("BaseRows = %d, %v; want its own 2", n, err)
	}
	dead, rows := pair(t, db, star, 0, cols, []string{"id"}, change{key: "2", row: []cell{v("2"), v("TWO")}})
	if md, err := Footer(context.Background(), db, rows); err != nil || md[MetaKeyBase] == "" {
		t.Fatalf("Footer of its pair: %v, %v", md, err)
	}
	if got, want := state(t, db, star, []string{dead}, []string{rows}, cols...), []string{"[1] [one]", "[2] [TWO]"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
	// And the other table is as it was.
	if got, want := state(t, db, plain, nil, nil, cols...), []string{"[1] [other one]", "[2] [other two]", "[9] [nine]"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("the other table: %v, want %v", got, want)
	}
}

// A pair written from the pair before it and the window after it: the rows
// that pair took away stay dead with no key named for them, and its rows
// come back in key order, NULLs and deletions as they were written.
func TestAPairFromThePairBefore(t *testing.T) {
	db := duck(t)
	ctx := context.Background()
	cols, keyCols := []string{"id", "val"}, []string{"id"}
	up := func(k string, val cell) change { return change{key: k, row: []cell{v(k), val}} }
	dir := t.TempDir()
	base := filepath.Join(dir, "t.parquet")
	writeBase(t, base, cols, []cell{v("1"), v("a")}, []cell{v("2"), v("b")}, []cell{v("3"), v("c")}, []cell{v("4"), v("d")}, []cell{v("5"), v("e")})
	dead0, rows0 := pair(t, db, base, 0, cols, keyCols, up("2", null), change{key: "4", del: true}, up("9", v("new")), up("1|x", v("p")))

	gotDead, err := ReadDead(ctx, db, dead0)
	if err != nil || !reflect.DeepEqual(gotDead, []int64{1, 3}) {
		t.Fatalf("ReadDead = %v, %v; want rows 1 and 3", gotDead, err)
	}
	var seen []string
	err = EachNew(ctx, db, rows0, cols, func(c Change) error {
		line := c.Key
		if c.Delete {
			line += " deleted"
		} else {
			for i := range c.Values {
				if c.Nulls[i] {
					line += " ∅"
				} else {
					line += " [" + c.Values[i] + "]"
				}
			}
		}
		seen = append(seen, line)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want(t, "the pair's rows", seen, `1\|x [1|x] [p]`, "2 [2] ∅", "4 deleted", "9 [9] [new]")
	// The columns in another order are handed over in that order.
	if err := EachNew(ctx, db, rows0, []string{"val", "id"}, func(c Change) error {
		if c.Key == "9" && (c.Values[0] != "new" || c.Values[1] != "9") {
			return fmt.Errorf("key 9 came as %v", c.Values)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stop := errors.New("stop")
	if err := EachNew(ctx, db, rows0, cols, func(Change) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("yield's error came back as %v", err)
	}

	// The next pair, in another folder beside the same file's link: key 2
	// is kept from the pair before, 4 stays dead with nothing written of
	// it, 5 dies now, and 3 was never named.
	dir2 := t.TempDir()
	base2 := filepath.Join(dir2, "t.parquet")
	if err := os.Link(base, base2); err != nil {
		t.Fatal(err)
	}
	kept := []Change{
		{Key: `1\|x`, Values: []string{"1|x", "p"}, Nulls: []bool{false, false}},
		{Key: "2", Values: []string{"2", ""}, Nulls: []bool{false, true}},
		{Key: "5", Delete: true},
	}
	write := func(carried []int64, positions map[string]int64) (string, string, error) {
		d, r, _, err := WritePair(PairInput{
			BasePath: base2, Seq: 0, Base: "base-1", Run: "run-next", Columns: cols, KeyColumns: keyCols,
			Positions: positions, CarriedDead: carried,
			Changes: func(yield func(Change) error) error {
				for _, c := range kept {
					if err := yield(c); err != nil {
						return err
					}
				}
				return nil
			},
		})
		return d, r, err
	}
	for name, carried := range map[string][]int64{"descending": {3, 1}, "twice": {1, 1}, "negative": {-1, 3}} {
		if _, _, err := write(carried, map[string]int64{"5": 4}); err == nil || !strings.Contains(err.Error(), "not ascending") {
			t.Errorf("carried rows %s: %v", name, err)
		}
	}
	// Row 4 both carried and placed: dead once.
	dead1, rows1, err := write([]int64{1, 3, 4}, map[string]int64{"5": 4})
	if err != nil {
		t.Fatal(err)
	}
	if gotDead, err = ReadDead(ctx, db, dead1); err != nil || !reflect.DeepEqual(gotDead, []int64{1, 3, 4}) {
		t.Fatalf("the next pair's dead rows = %v, %v; want 1, 3 and 4", gotDead, err)
	}
	want(t, "the table through the next pair", state(t, db, base2, []string{dead1}, []string{rows1}, "id", "val"),
		"[1] [a]", "[1|x] [p]", "[2] ∅", "[3] [c]")

	// A file that is not a .pgdead, or not in order, is refused.
	if _, err := ReadDead(ctx, db, base); err == nil {
		t.Error("a table file was read as dead rows")
	}
	if err := EachNew(ctx, db, base, cols, func(Change) error { return nil }); err == nil {
		t.Error("a table file was read as a pair's rows")
	}
}

func TestMergeDead(t *testing.T) {
	for _, c := range []struct{ a, b, want []int64 }{
		{nil, nil, nil},
		{[]int64{1, 5}, nil, []int64{1, 5}},
		{nil, []int64{2}, []int64{2}},
		{[]int64{1, 5}, []int64{0, 5, 9}, []int64{0, 1, 5, 9}},
		{[]int64{7}, []int64{1, 2}, []int64{1, 2, 7}},
	} {
		if got := mergeDead(c.a, c.b); !reflect.DeepEqual(got, c.want) && (len(got) != 0 || len(c.want) != 0) {
			t.Errorf("mergeDead(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
