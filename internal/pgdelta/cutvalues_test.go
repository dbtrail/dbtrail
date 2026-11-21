package pgdelta

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// cutRows prints an answer of CutValuesOf: "key: col=[text] col=∅".
func cutRows(got map[string]map[string]*string, cols ...string) map[string]string {
	out := map[string]string{}
	for k, row := range got {
		parts := make([]string, 0, len(cols))
		for _, c := range cols {
			v, ok := row[c]
			switch {
			case !ok:
				parts = append(parts, c+" not read")
			case v == nil:
				parts = append(parts, c+"=∅")
			default:
				parts = append(parts, c+"=["+*v+"]")
			}
		}
		out[k] = strings.Join(parts, " ")
	}
	return out
}

// The rows of a cut by key: the pair before the base, a row the pair deleted
// is no row, a NULL is a NULL and not a missing row, and keys are their
// bytes.
func TestCutValuesOf(t *testing.T) {
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	cols := []string{"id", "big", "other"}
	keyCols := cols[:1]
	writeBase(t, base, cols,
		[]cell{v("base only"), v("B1"), v("o1")},
		[]cell{v("in both"), v("B2"), v("o2")},
		[]cell{v("deleted by the pair"), v("B3"), v("o3")},
		[]cell{v("null in the base"), null, v("o4")},
		[]cell{v("made null by the pair"), v("B5"), v("o5")},
		[]cell{v("empty in the base"), v(""), v("o6")},
		[]cell{v("K"), v("upper"), v("o7")},
		[]cell{v("k"), v("lower"), v("o8")},
		[]cell{v(`a|b\c`), v("piped"), v("o9")},
	)
	dead, rows := pair(t, db, base, 0, cols, keyCols,
		change{key: "in both", row: []cell{v("in both"), v("P2"), v("p2")}},
		change{key: "deleted by the pair", del: true},
		change{key: "made null by the pair", row: []cell{v("made null by the pair"), null, v("p5")}},
		change{key: "pair only", row: []cell{v("pair only"), v("P9"), null}},
		change{key: "k", row: []cell{v("k"), v("LOWER"), v("p8")}},
	)
	keys := []string{"base only", "in both", "deleted by the pair", "null in the base", "made null by the pair", "empty in the base",
		"K", "k", `a\|b\\c`, "pair only", "nowhere", "in both"}
	got, err := CutValuesOf(ctx, db, base, []string{dead}, []string{rows}, keyCols, keys, []string{"big", "other"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"base only":             "big=[B1] other=[o1]",
		"in both":               "big=[P2] other=[p2]",
		"null in the base":      "big=∅ other=[o4]",
		"made null by the pair": "big=∅ other=[p5]",
		"empty in the base":     "big=[] other=[o6]",
		"K":                     "big=[upper] other=[o7]",
		"k":                     "big=[LOWER] other=[p8]",
		`a\|b\\c`:               "big=[piped] other=[o9]",
		"pair only":             "big=[P9] other=∅",
	}
	if g := cutRows(got, "big", "other"); !reflect.DeepEqual(g, want) {
		t.Fatalf("the cut's rows:\n got  %q\n want %q", g, want)
	}
	for _, absent := range []string{"deleted by the pair", "nowhere"} {
		if _, ok := got[absent]; ok {
			t.Fatalf("key %q has a row in the answer; the cut holds none", absent)
		}
	}

	// It answers as the state the views read does, for every key and column.
	for _, line := range state(t, db, base, []string{dead}, []string{rows}, cols...) {
		id := line[1:strings.Index(line, "]")]
		key := KeyOf([]string{id})
		row, ok := got[key]
		if !ok {
			t.Fatalf("the state holds %s and the answer has no row of key %q", line, key)
		}
		spell := func(p *string) string {
			if p == nil {
				return "∅"
			}
			return "[" + *p + "]"
		}
		if mine := "[" + id + "] " + spell(row["big"]) + " " + spell(row["other"]); mine != line {
			t.Fatalf("key %q: the answer is %s, the state reads %s", key, mine, line)
		}
	}

	// With no pair it is BaseValuesOf.
	alone, err := CutValuesOf(ctx, db, base, nil, nil, keyCols, []string{"in both", "deleted by the pair", "nowhere"}, []string{"big"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if g, w := cutRows(alone, "big"), map[string]string{"in both": "big=[B2]", "deleted by the pair": "big=[B3]"}; !reflect.DeepEqual(g, w) {
		t.Fatalf("with no pair: %q, want %q", g, w)
	}
	if got, err := CutValuesOf(ctx, db, base, []string{dead}, []string{rows}, keyCols, nil, []string{"big"}, 0); err != nil || len(got) != 0 {
		t.Fatalf("no key asked: %v, %v", got, err)
	}
	if _, err := CutValuesOf(ctx, db, base, []string{dead}, []string{rows}, keyCols, []string{"in both"}, []string{"gone"}, 0); err == nil || !strings.Contains(err.Error(), "has no column gone") {
		t.Fatalf("a column the file does not have: %v", err)
	}

	// No more text than allowed, the pair's and the base's counted together.
	two := []string{"in both", "base only"}
	if _, err := CutValuesOf(ctx, db, base, []string{dead}, []string{rows}, keyCols, two, []string{"big"}, 3); !errors.Is(err, ErrBaseValuesTooLarge) {
		t.Fatalf("four bytes of text with three allowed: %v", err)
	}
	if _, err := CutValuesOf(ctx, db, base, []string{dead}, []string{rows}, keyCols, two, []string{"big"}, 1); !errors.Is(err, ErrBaseValuesTooLarge) {
		t.Fatalf("the pair's text alone past what is allowed: %v", err)
	}
	if _, err := CutValuesOf(ctx, db, base, []string{dead}, []string{rows}, keyCols, []string{"in both", "empty in the base", "base only"}, []string{"big"}, 2); !errors.Is(err, ErrBaseValuesTooLarge) {
		t.Fatalf("the pair's text takes all that is allowed and the base has more: %v", err)
	}
	if got, err := CutValuesOf(ctx, db, base, []string{dead}, []string{rows}, keyCols, []string{"in both", "empty in the base"}, []string{"big"}, 2); err != nil || len(got) != 2 {
		t.Fatalf("the pair's text takes all that is allowed and the base has an empty text: %v, %v", got, err)
	}
	if got, err := CutValuesOf(ctx, db, base, []string{dead}, []string{rows}, keyCols, two, []string{"big"}, 4); err != nil || len(got) != 2 {
		t.Fatalf("as much text as allowed: %v, %v", got, err)
	}

	// A row the delta takes away whose key no pair names: not a whole chain,
	// and no value.
	if _, err := CutValuesOf(ctx, db, base, []string{dead}, nil, keyCols, []string{"in both"}, []string{"big"}, 0); !errors.Is(err, ErrBrokenChain) {
		t.Fatalf("a dead row whose key the delta does not hold: %v", err)
	}
	// A file that does not read is an error.
	bad := filepath.Join(t.TempDir(), "bad.pgnew")
	if err := os.WriteFile(bad, []byte("not parquet"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := CutValuesOf(ctx, db, base, []string{dead}, []string{bad}, keyCols, []string{"in both"}, []string{"big"}, 0); err == nil {
		t.Fatal("a table delta that does not read gave an answer")
	}
}

// Through a chain, a key is as its newest pair leaves it; and a composite
// key is found in the base by its columns in the table's order.
func TestCutValuesOf_chainAndCompositeKey(t *testing.T) {
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	cols := []string{"a", "b", "big"}
	keyCols := cols[:2]
	key := func(a, b string) string { return KeyOf([]string{a, b}) }
	writeBase(t, base, cols,
		[]cell{v("1"), v("x"), v("base 1x")},
		[]cell{v("1"), v("y"), v("base 1y")},
		[]cell{v("2"), v("x|y"), v("base 2")},
		[]cell{v("3"), v("z"), v("base 3")},
		[]cell{v("x"), v("1"), v("base x1")},
	)
	d0, r0 := pair(t, db, base, 0, cols, keyCols,
		change{key: key("1", "x"), row: []cell{v("1"), v("x"), v("first pair")}},
		change{key: key("1", "y"), del: true},
		change{key: key("3", "z"), del: true},
	)
	d1, r1 := pair(t, db, base, 1, cols, keyCols,
		change{key: key("1", "x"), row: []cell{v("1"), v("x"), v("second pair")}},
		change{key: key("1", "y"), row: []cell{v("1"), v("y"), v("back")}},
	)
	keys := []string{key("1", "x"), key("1", "y"), key("2", "x|y"), key("3", "z"), key("x", "1"), key("1", "z")}
	// The files in any order: the sequence in each row says which is newest.
	for _, order := range [][2][]string{{{d0, d1}, {r0, r1}}, {{d1, d0}, {r1, r0}}} {
		got, err := CutValuesOf(ctx, db, base, order[0], order[1], keyCols, keys, []string{"big"}, 0)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]string{
			key("1", "x"):   "big=[second pair]",
			key("1", "y"):   "big=[back]",
			key("2", "x|y"): "big=[base 2]",
			key("x", "1"):   "big=[base x1]",
		}
		if g := cutRows(got, "big"); !reflect.DeepEqual(g, want) {
			t.Fatalf("through two pairs:\n got  %q\n want %q", g, want)
		}
	}
}
