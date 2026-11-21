package pgdelta

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// freshNoNullMemo empties the memo of files with no NULL key for one test,
// and again after it, with its bound as it was.
func freshNoNullMemo(t *testing.T) {
	t.Helper()
	clearMemo := func() {
		noNullMu.Lock()
		defer noNullMu.Unlock()
		noNullCurrent = map[noNullKey]struct{}{}
		noNullPrevious = map[noNullKey]struct{}{}
	}
	clearMemo()
	cap := noNullCap
	t.Cleanup(func() {
		clearMemo()
		noNullCap = cap
	})
}

// noNullLen is how many answers are remembered, both generations.
func noNullLen() int {
	noNullMu.Lock()
	defer noNullMu.Unlock()
	return len(noNullCurrent) + len(noNullPrevious)
}

// aged sets path's time of last write an hour back, past the margin a file
// must be older than to be remembered, and returns it as the file system
// keeps it.
func aged(t *testing.T, path string) time.Time {
	t.Helper()
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.ModTime()
}

// nullPassesOf is how many passes for a NULL key f made.
func nullPassesOf(f func()) uint64 {
	before := KeyNullPasses()
	f()
	return KeyNullPasses() - before
}

// A file found to hold no NULL key is not read for one again: not at the
// next call, not through another name of the same file (a snapshot update
// reads its base through a hard link in a newer folder), and not for the
// same columns only: other key columns are another question.
func TestNoNullMemoSkipsThePassForAFileAlreadyRead(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("a"), v("1")}, []cell{v("b"), null}, []cell{v("c"), v("3")})
	aged(t, base)
	want := map[string]int64{"a": 0, "c": 2}
	var got map[string]int64
	var err error
	if n := nullPassesOf(func() { got, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"a", "c", "z"}) }); n != 1 || err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("first call: %d passes, %v, %v; want 1 pass and %v", n, got, err, want)
	}
	if n := nullPassesOf(func() { got, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"a", "c", "z"}) }); n != 0 || err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("second call: %d passes, %v, %v; want none and %v", n, got, err, want)
	}
	link := filepath.Join(t.TempDir(), "t.parquet")
	if err := os.Link(base, link); err != nil {
		t.Fatal(err)
	}
	if n := nullPassesOf(func() { got, err = BasePositionsOf(ctx, db, link, []string{"id"}, []string{"a"}) }); n != 0 || err != nil || got["a"] != 0 {
		t.Fatalf("the same file under another name: %d passes, %v, %v; want none", n, got, err)
	}
	// val holds a NULL: "no NULL in id" says nothing of (id, val).
	if n := nullPassesOf(func() { _, err = BasePositionsOf(ctx, db, base, []string{"id", "val"}, []string{"a|1"}) }); n != 1 || err == nil || !strings.Contains(err.Error(), "row 1 of") {
		t.Fatalf("other key columns: %d passes, %v; want a pass and the NULL refused", n, err)
	}
}

// A file with a NULL in a key column is refused at every call, wanted key
// or not: a refusal is never remembered as a file with no NULL.
func TestNoNullMemoRefusesANullKeyAtEveryCall(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("1"), v("a")}, []cell{null, v("b")}, []cell{v("3"), v("c")})
	aged(t, base)
	for i := 1; i <= 3; i++ {
		var err error
		if n := nullPassesOf(func() { _, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"1"}) }); n != 1 || err == nil || !strings.Contains(err.Error(), "row 1 of") {
			t.Fatalf("call %d: %d passes, %v; want the pass and the NULL refused", i, n, err)
		}
	}
	if n := noNullLen(); n != 0 {
		t.Fatalf("%d answers remembered for a file with a NULL key", n)
	}
}

// A file put in the place of another, under the same name, is another file:
// with the same bytes, size and time of last write to the nanosecond (only
// the inode tells it), and with a NULL key and the same time.
func TestNoNullMemoReadsAFileReplacedUnderTheSameNameAgain(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	dir := t.TempDir()
	base := filepath.Join(dir, "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("1"), v("a")}, []cell{v("2"), v("b")})
	mtime := aged(t, base)
	call := func() (map[string]int64, uint64, error) {
		var got map[string]int64
		var err error
		n := nullPassesOf(func() { got, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"2"}) })
		return got, n, err
	}
	if _, n, err := call(); err != nil || n != 1 {
		t.Fatalf("first call: %v, %d passes", err, n)
	}

	// The same bytes, renamed over it, with the old file's time.
	bytes, err := os.ReadFile(base)
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, "copy.parquet")
	if err := os.WriteFile(tmp, bytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(tmp, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, base); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(base); err != nil || fi.Size() != int64(len(bytes)) || !fi.ModTime().Equal(mtime) {
		t.Fatalf("the copy is not of the same size and time (%v): this case tests nothing", err)
	}
	if got, n, err := call(); err != nil || n != 1 || got["2"] != 1 {
		t.Fatalf("the same bytes under the same name, another file: %v, %v, %d passes; want the pass again", got, err, n)
	}

	// A file with a NULL key, in the same second as the one before.
	writeBase(t, tmp, []string{"id", "val"}, []cell{null, v("a")}, []cell{v("2"), v("b")})
	if err := os.Chtimes(tmp, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, base); err != nil {
		t.Fatal(err)
	}
	if _, n, err := call(); n != 1 || err == nil || !strings.Contains(err.Error(), "row 0 of") {
		t.Fatalf("a file with a NULL key under the same name: %v, %d passes; want the NULL refused", err, n)
	}
}

// A file written just now is not remembered: a file written again within
// the same tick of the clock could keep every field it is known by.
func TestNoNullMemoDoesNotRememberAFileWrittenJustNow(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id"}, []cell{v("1")})
	for i := 1; i <= 2; i++ {
		var err error
		if n := nullPassesOf(func() { _, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"1"}) }); n != 1 || err != nil {
			t.Fatalf("call %d over a file written just now: %d passes, %v; want the pass", i, n, err)
		}
	}
}

// Two keys that differ only in case, or in how an accent is written, stay
// two: on the call that reads the file for a NULL, and on the one that does
// not, in a session that sets case and accents aside too.
func TestNoNullMemoKeepsKeysThatDifferOnlyInCaseApart(t *testing.T) {
	freshNoNullMemo(t)
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "t.parquet")
	writeBase(t, base, []string{"id"}, []cell{v("a")}, []cell{v("A")}, []cell{v("é")}, []cell{v("é")})
	aged(t, base)
	want := map[string]int64{"a": 0, "A": 1, "é": 2, "é": 3}
	keys := []string{"A", "a", "é", "é"}
	db := duck(t)
	loose := duck(t)
	if _, err := loose.Exec("SET default_collation='nocase.noaccent'"); err != nil {
		t.Fatal(err)
	}
	for i, c := range []struct {
		name   string
		passes uint64
	}{{"reading the file for a NULL", 1}, {"remembered", 0}, {"remembered, in a session that sets case aside", 0}} {
		s := db
		if i == 2 {
			s = loose
		}
		var got map[string]int64
		var err error
		if n := nullPassesOf(func() { got, err = BasePositionsOf(ctx, s, base, []string{"id"}, keys) }); n != c.passes || err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: %d passes, %v, %v; want %d and %v", c.name, n, got, err, c.passes, want)
		}
	}
}

// The memo holds at most two generations of its bound, and a file asked
// about at every turn stays in it while the others go.
func TestNoNullMemoIsBounded(t *testing.T) {
	freshNoNullMemo(t)
	noNullCap = 2
	db := duck(t)
	ctx := context.Background()
	dir := t.TempDir()
	file := func(name string) string {
		p := filepath.Join(dir, name+".parquet")
		writeBase(t, p, []string{"id"}, []cell{v("1")})
		aged(t, p)
		return p
	}
	ask := func(p string) uint64 {
		return nullPassesOf(func() {
			if _, err := BasePositionsOf(ctx, db, p, []string{"id"}, []string{"1"}); err != nil {
				t.Fatal(err)
			}
		})
	}
	hot := file("hot")
	ask(hot)
	var cold []string
	for i := 0; i < 6; i++ {
		p := file("cold" + string(rune('a'+i)))
		cold = append(cold, p)
		if n := ask(p); n != 1 {
			t.Fatalf("a new file: %d passes", n)
		}
		if n := ask(hot); n != 0 {
			t.Fatalf("after %d other files the file asked about every time was read again", i+1)
		}
		if n := noNullLen(); n > 2*noNullCap {
			t.Fatalf("%d answers remembered with a bound of %d", n, noNullCap)
		}
	}
	if n := ask(cold[0]); n != 1 {
		t.Fatal("the first of the files asked about once is still remembered past the bound")
	}
}

// A file put in the place of a remembered one between the join and the look
// after it: the answer the join gave is dropped, and the call is made again
// with the pass for a NULL, which refuses the new file.
func TestNoNullMemoAsksAgainWhenTheFileChangedDuringTheJoin(t *testing.T) {
	freshNoNullMemo(t)
	db := duck(t)
	ctx := context.Background()
	dir := t.TempDir()
	base := filepath.Join(dir, "t.parquet")
	writeBase(t, base, []string{"id", "val"}, []cell{v("1"), v("a")}, []cell{v("2"), v("b")})
	aged(t, base)
	if _, err := BasePositionsOf(ctx, db, base, []string{"id"}, []string{"1"}); err != nil {
		t.Fatal(err)
	}
	holed := filepath.Join(dir, "holed.parquet")
	writeBase(t, holed, []string{"id", "val"}, []cell{v("1"), v("a")}, []cell{null, v("b")})
	aged(t, holed)
	prev := afterKeyJoin
	t.Cleanup(func() { afterKeyJoin = prev })
	swapped := false
	afterKeyJoin = func(string) {
		if !swapped {
			swapped = true
			if err := os.Rename(holed, base); err != nil {
				t.Error(err)
			}
		}
	}
	var got map[string]int64
	var err error
	n := nullPassesOf(func() { got, err = BasePositionsOf(ctx, db, base, []string{"id"}, []string{"1"}) })
	if !swapped || n != 1 || err == nil || !strings.Contains(err.Error(), "row 1 of") {
		t.Fatalf("the file changed during the join: swapped %v, %d passes, %v, %v; want the call made again and the NULL refused", swapped, n, got, err)
	}
}
