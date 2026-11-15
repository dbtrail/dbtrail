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
	var positions []int64
	for _, p := range at {
		positions = append(positions, p)
	}
	sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })
	dead, rows, _, err = WritePair(PairInput{
		BasePath: base, Seq: seq, Base: "base-1", Run: fmt.Sprintf("run-%d", seq), Columns: cols, Dead: positions,
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
	keys := []string{"a", "A", "e", "é", "x", "x ", "a|b", `a\b`, "it's", `q"q`, ""}
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
	)
	// A later pair changes the lower-case one: it must not replace "A".
	d1, r1 := pair(t, db, base, 1, cols, []string{"k"},
		change{key: KeyOf([]string{"a"}), row: []cell{v("a"), v("newer")}},
		change{key: KeyOf([]string{"e"}), del: true},
	)
	want(t, "look-alike keys", state(t, db, base, []string{d0, d1}, []string{r0, r1}, cols...),
		"[] [new]", "[A] [new]", "[a] [newer]", "[a|b] [new]", "[it's] [base]", `[q"q] [base]`, "[x] [base]", "[é] [new]")
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
	good := PairInput{BasePath: base, Seq: 0, Base: "b", Run: "r", Columns: []string{"id", "val"}, Dead: []int64{0, 5}, Changes: one(row("1"), row("2"))}
	with := func(f func(*PairInput)) PairInput { in := good; f(&in); return in }
	cases := []struct {
		name string
		in   PairInput
		is   error
	}{
		{"a column named as the pair's key", with(func(in *PairInput) { in.Columns = []string{"id", KeyColumn} }), ErrReservedColumn},
		{"a column named as the pair's sequence", with(func(in *PairInput) { in.Columns = []string{SeqColumn, "val"} }), ErrReservedColumn},
		{"a column named file_row_number", with(func(in *PairInput) { in.Columns = []string{"id", "file_row_number"} }), ErrReservedColumn},
		{"a column named twice", with(func(in *PairInput) { in.Columns = []string{"id", "id"} }), nil},
		{"no column", with(func(in *PairInput) { in.Columns = nil }), nil},
		{"dead rows out of order", with(func(in *PairInput) { in.Dead = []int64{5, 0} }), nil},
		{"a dead row twice", with(func(in *PairInput) { in.Dead = []int64{3, 3} }), nil},
		{"keys out of order", with(func(in *PairInput) { in.Changes = one(row("2"), row("1")) }), nil},
		{"a key twice", with(func(in *PairInput) { in.Changes = one(row("1"), row("1")) }), nil},
		{"a row with a value missing", with(func(in *PairInput) {
			in.Changes = one(Change{Key: "1", Values: []string{"1"}, Nulls: []bool{false}})
		}), nil},
		{"nothing changed", with(func(in *PairInput) { in.Dead, in.Changes = nil, one() }), ErrNothingToWrite},
		{"nothing changed, no source of changes", with(func(in *PairInput) { in.Dead, in.Changes = nil, nil }), ErrNothingToWrite},
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
		})
	}
	// A deletion alone, and dead rows alone, are pairs.
	if _, _, keys, err := WritePair(with(func(in *PairInput) { in.Dead, in.Changes = nil, one(Change{Key: "9", Delete: true}) })); err != nil || keys != 1 {
		t.Fatalf("a deletion alone: %d, %v", keys, err)
	}
	if _, _, keys, err := WritePair(with(func(in *PairInput) { in.Seq, in.Changes = 1, one() })); err != nil || keys != 0 {
		t.Fatalf("dead rows alone: %d, %v", keys, err)
	}
}

func TestCheckChain(t *testing.T) {
	db := duck(t)
	ctx := context.Background()
	dir := t.TempDir()
	base := filepath.Join(dir, "t.parquet")
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
	last, err := CheckChain(ctx, db, dir, chain, "base-1")
	if err != nil || last[MetaKeySeq] != "1" || last[baseline.MetaKeyLSN] != "4096" || last[MetaKeyRun] != "run-1" {
		t.Fatalf("a whole chain: %v, %v", last, err)
	}
	if last, err := CheckChain(ctx, db, dir, Chain{Table: "t"}, "base-1"); err != nil || last != nil {
		t.Fatalf("an empty chain: %v, %v", last, err)
	}
	if _, err := CheckChain(ctx, db, dir, chain, "another-base"); !errors.Is(err, ErrBrokenChain) || !strings.Contains(err.Error(), "another table file") {
		t.Fatalf("a chain of another base: %v", err)
	}
	if _, err := CheckChain(ctx, db, dir, chain, ""); err == nil {
		t.Fatal("no base id gave no error")
	}

	// The second pair's rows, written again by another run: the two files of
	// that pair no longer come from one write.
	d1, r1, _ := Paths(base, 1)
	if err := os.Remove(r1); err != nil {
		t.Fatal(err)
	}
	_, other, _, err := WritePair(PairInput{BasePath: filepath.Join(dir, "other", "t.parquet"), Seq: 1, Base: "base-1", Run: "another-run",
		Columns: []string{"id", "val"}, Changes: func(yield func(Change) error) error {
			return yield(Change{Key: "2", Values: []string{"2", "z"}, Nulls: []bool{false, false}})
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(other, r1); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckChain(ctx, db, dir, chain, "base-1"); !errors.Is(err, ErrBrokenChain) || !strings.Contains(err.Error(), "different writes") {
		t.Fatalf("a pair from two writes: %v", err)
	}

	// A file under the name of another sequence.
	d0, _, _ := Paths(base, 0)
	if err := os.Rename(d1, d0); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckChain(ctx, db, dir, Chain{Table: "t", Pairs: chain.Pairs[:1]}, "base-1"); !errors.Is(err, ErrBrokenChain) || !strings.Contains(err.Error(), "says it is pair") {
		t.Fatalf("a file under another sequence's name: %v", err)
	}
	// A file that is not there.
	if _, err := CheckChain(ctx, db, dir, chain, "base-1"); err == nil {
		t.Fatal("a missing file gave no error")
	}
}

func TestBaseID(t *testing.T) {
	taken := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	good := baseline.DumpMetadata{LSN: 0x500, PGDeltaSince: "2026-11-15T09:59:00Z", SnapshotTimestamp: taken}
	id, err := BaseID(good, 1234)
	if err != nil || id == "" {
		t.Fatalf("BaseID: %q, %v", id, err)
	}
	// Any part that differs is another base.
	for name, other := range map[string]func() (string, error){
		"another LSN":   func() (string, error) { m := good; m.LSN++; return BaseID(m, 1234) },
		"another bound": func() (string, error) { m := good; m.PGDeltaSince = "2026-11-15T09:58:00Z"; return BaseID(m, 1234) },
		"another time":  func() (string, error) { m := good; m.SnapshotTimestamp = taken.Add(time.Microsecond); return BaseID(m, 1234) },
		"another size":  func() (string, error) { return BaseID(good, 1235) },
	} {
		if got, err := other(); err != nil || got == id {
			t.Errorf("%s: %q, %v; want another id than %q", name, got, err, id)
		}
	}
	// The same instant in another zone is the same base.
	m := good
	m.SnapshotTimestamp = taken.In(time.FixedZone("x", 3*3600))
	if got, _ := BaseID(m, 1234); got != id {
		t.Errorf("the same instant in another zone: %q, want %q", got, id)
	}
	for name, bad := range map[string]func() (string, error){
		"no LSN":   func() (string, error) { m := good; m.LSN = 0; return BaseID(m, 1234) },
		"no bound": func() (string, error) { m := good; m.PGDeltaSince = ""; return BaseID(m, 1234) },
		"no time":  func() (string, error) { m := good; m.SnapshotTimestamp = time.Time{}; return BaseID(m, 1234) },
		"no size":  func() (string, error) { return BaseID(good, 0) },
	} {
		if got, err := bad(); err == nil {
			t.Errorf("%s: %q, want an error", name, got)
		}
	}
}
