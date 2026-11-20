package pgdelta

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The text of a column in the row of each wanted key, as the file holds it:
// a NULL is nil, an empty text is an empty text, and a key the file does not
// hold is not in the answer.
func TestBaseValuesOf(t *testing.T) {
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	cols := []string{"a", "odd \"name", "big", "other"}
	keyCols := cols[:2]
	big := strings.Repeat("ñ|\\'\"", 3000)
	writeBase(t, base, cols,
		[]cell{v("1"), v(""), v(big), v("x")},
		[]cell{v("1"), v("k"), null, v("y")},
		[]cell{v("x|y"), v(`\`), v(""), null},
		[]cell{v("A"), v("k"), v("upper"), v("z")},
		[]cell{v("a"), v("k"), v("lower"), v("z")},
	)
	text := func(s string) *string { return &s }
	got, err := BaseValuesOf(ctx, db, base, keyCols,
		[]string{KeyOf([]string{"1", ""}), KeyOf([]string{"1", "k"}), KeyOf([]string{"x|y", `\`}), KeyOf([]string{"a", "k"}), KeyOf([]string{"1", "k"}), "not there"},
		[]string{"big", "other"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]*string{
		KeyOf([]string{"1", ""}):      {"big": text(big), "other": text("x")},
		KeyOf([]string{"1", "k"}):     {"big": nil, "other": text("y")},
		KeyOf([]string{"x|y", `\`}):  {"big": text(""), "other": nil},
		KeyOf([]string{"a", "k"}):     {"big": text("lower"), "other": text("z")},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %d keys, want %d", len(got), len(want))
	}
	for k, row := range got {
		if v, ok := row["big"]; !ok || (v == nil) != (want[k]["big"] == nil) {
			t.Fatalf("key %q: big is %v", k, v)
		}
	}

	if got, err := BaseValuesOf(ctx, db, base, keyCols, nil, []string{"big"}); err != nil || len(got) != 0 {
		t.Fatalf("no key asked: %v, %v", got, err)
	}
	if _, err := BaseValuesOf(ctx, db, base, keyCols, []string{KeyOf([]string{"1", ""})}, []string{"gone"}); err == nil || !strings.Contains(err.Error(), "has no column gone") {
		t.Fatalf("a column the file does not have: %v", err)
	}

	twice := filepath.Join(t.TempDir(), "twice.parquet")
	writeBase(t, twice, []string{"id", "big"}, []cell{v("1"), v("one")}, []cell{v("2"), v("x")}, []cell{v("1"), v("other")})
	if _, err := BaseValuesOf(ctx, db, twice, []string{"id"}, []string{"1"}, []string{"big"}); err == nil || !strings.Contains(err.Error(), "twice, at rows 0 and 2") {
		t.Fatalf("a key the file holds twice: %v", err)
	}
	noKey := filepath.Join(t.TempDir(), "nokey.parquet")
	writeBase(t, noKey, []string{"id", "big"}, []cell{v("1"), v("one")}, []cell{null, v("x")})
	if _, err := BaseValuesOf(ctx, db, noKey, []string{"id"}, []string{"1"}, []string{"big"}); err == nil || !strings.Contains(err.Error(), "no value in a key column") {
		t.Fatalf("a row with no key: %v", err)
	}
}
