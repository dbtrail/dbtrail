package reconstruct

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
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #3049: a fresh-mode tail reads from the table's copy the large values no
// change it holds gives. The table here is (id, status, note), status and
// note standing for large values.

var pgCopyCols = []string{"id", "status", "note"}

func pgCopyRel(pk ...string) baseline.PGRelation {
	return baseline.PGRelation{OID: 16600, Columns: []baseline.PGColumn{
		{Name: "id", TypeOID: 25, TypeMod: -1, Type: "text", NotNull: true, Ordinal: 1},
		{Name: "status", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 2},
		{Name: "note", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 3},
	}, PrimaryKey: pk}
}

// writePGCopyBase writes a table file of (id, status, note) rows in dir, cut
// at lsn; a nil value is a NULL.
func writePGCopyBase(t testing.TB, dir string, lsn uint64, rel baseline.PGRelation, rows ...[3]any) (string, baseline.DumpMetadata) {
	t.Helper()
	enc, err := rel.Encode()
	if err != nil {
		t.Fatal(err)
	}
	md := map[string]string{
		baseline.MetaKeySnapshotTimestamp: "2026-11-16T00:00:00Z",
		baseline.MetaKeyLSN:               fmt.Sprint(lsn),
		baseline.MetaKeyPGRelation:        enc,
		baseline.MetaKeyRenderGUCs:        baseline.RenderGUCsPinned,
		baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "orders.parquet")
	w, err := baseline.NewWriter(path, rawTextColumns(pgCopyCols), baseline.WriterConfig{Compression: "none", RowGroupSize: 1000, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		vals, nulls := make([]string, 3), make([]bool, 3)
		for i, v := range r {
			if v == nil {
				nulls[i] = true
			} else {
				vals[i] = v.(string)
			}
		}
		if err := w.WriteRow(vals, nulls); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	footer, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, footer
}

// writePGCopyPair writes pair 0 beside base: each change is a key and its
// row, nil for a deleted key.
func writePGCopyPair(t testing.TB, base string, keyCols []string, changes map[string]*[3]any) {
	t.Helper()
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	keys := make([]string, 0, len(changes))
	for k := range changes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	at, err := pgdelta.BasePositions(context.Background(), duck, base, keyCols, func(k string) bool { _, ok := changes[k]; return ok })
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = pgdelta.WritePair(pgdelta.PairInput{
		BasePath: base, Seq: 0, Base: "base-1", Run: "run-0", Columns: pgCopyCols, KeyColumns: keyCols, Positions: at,
		Metadata: map[string]string{baseline.MetaKeyLSN: "1792"},
		Changes: func(yield func(pgdelta.Change) error) error {
			for _, k := range keys {
				c := pgdelta.Change{Key: k, Delete: changes[k] == nil}
				if !c.Delete {
					c.Values, c.Nulls = make([]string, 3), make([]bool, 3)
					for i, v := range changes[k] {
						if v == nil {
							c.Nulls[i] = true
						} else {
							c.Values[i] = v.(string)
						}
					}
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

// pgCopyState is the table as a statement reads it: the tail's files over
// the cut (the file and the pair beside it), each row "id=status/note", a
// NULL as ∅.
func pgCopyState(t testing.TB, base string, tail *PGTail, files ...string) []string {
	t.Helper()
	dead, rows, err := pgCutChain(base)
	if err != nil {
		t.Fatal(err)
	}
	segs := make([]string, len(files))
	for i, f := range files {
		segs[i] = lit(f)
	}
	q := pgdelta.StateSQL(base, dead, rows)
	if len(segs) > 0 {
		q = baseline.TableTailChainStateSQL(q, segs, tail.PKColumns(), "")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rs, err := db.Query("SELECT id, coalesce(status, '∅'), coalesce(note, '∅') FROM (" + q + ") ORDER BY 1 COLLATE \"binary\"")
	if err != nil {
		t.Fatalf("%v\n%s", err, q)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var id, st, note string
		if err := rs.Scan(&id, &st, &note); err != nil {
			t.Fatal(err)
		}
		out = append(out, id+"="+st+"/"+note)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func newPGCopyTail(t testing.TB, md baseline.DumpMetadata) *PGTail {
	t.Helper()
	tail, err := NewPGTail("mydb", "orders", md, pgCopyCols, pkColsIntID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	tail.AwaitCopyValues()
	return tail
}

// copyKeyOnly is the earlier row of a change of key as a table below REPLICA
// IDENTITY FULL sends it: the key, every other column not captured.
func copyKeyOnly(key string) map[string]any {
	return map[string]any{"id": key, "status": notCaptured(), "note": notCaptured()}
}

func copyUpd(id, lsn uint64, key string, status, note any) query.ResultRow {
	return pgEv(id, lsn, event.EventUpdate, copyKeyOnly(key), map[string]any{"id": key, "status": status, "note": note})
}

func copyIns(id, lsn uint64, key string, status, note any) query.ResultRow {
	return pgEv(id, lsn, event.EventInsert, nil, map[string]any{"id": key, "status": status, "note": note})
}

func copyDel(id, lsn uint64, key string) query.ResultRow {
	return pgEv(id, lsn, event.EventDelete, copyKeyOnly(key), nil)
}

// readCopy does what a tail's keeper does: one read of the cut at base for
// everything the tail waits for, handed back.
func readCopy(t testing.TB, tail *PGTail, base string) (asked, given int, refusal string) {
	t.Helper()
	keys, cols := tail.CopyWants()
	rows, err := PGCutValues(context.Background(), base, tail.PKColumns(), keys, cols, duckdbutil.Tuning{}, 0)
	if err != nil {
		t.Fatalf("PGCutValues: %v", err)
	}
	given, refusal = tail.GiveCopyValues(keys, cols, rows)
	return len(keys), given, refusal
}

// noMarkerWritten fails when a file of a tail holds the marker's name
// anywhere: a value not sent is never written as the marker.
func noMarkerWritten(t testing.TB, files ...string) {
	t.Helper()
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), event.UnchangedToastKey) {
			t.Fatalf("%s holds the marker of a value that was not sent", f)
		}
	}
}

func TestPGTail_readsALargeValueFromTheCopy(t *testing.T) {
	u := unsent()
	baseRows := [][3]any{
		{"1", "one", "note one"}, {"2", "two", "note two"}, {"3", "three", "note three"},
		{"4", nil, "note four"}, {"5", "", "note five"}, {"6", "six", "note six"},
	}
	// rig is a cut (its file, and a pair beside it when changes are given)
	// and an empty tail over it.
	rig := func(t *testing.T, pair map[string]*[3]any) (dir, base string, tail *PGTail) {
		t.Helper()
		dir = t.TempDir()
		base, md := writePGCopyBase(t, filepath.Join(dir, "cut"), 0x500, pgCopyRel("id"), baseRows...)
		if pair != nil {
			writePGCopyPair(t, base, []string{"id"}, pair)
		}
		return dir, base, newPGCopyTail(t, md)
	}
	apply := func(t *testing.T, tail *PGTail, pages ...[]query.ResultRow) {
		t.Helper()
		for _, p := range pages {
			if err := tail.Apply(p); err != nil {
				t.Fatal(err)
			}
		}
	}
	wants := func(tail *PGTail) string {
		keys, cols := tail.CopyWants()
		return strings.Join(keys, ",") + " " + strings.Join(cols, ",")
	}
	written := func(t *testing.T, tail *PGTail, dir, base string) []string {
		t.Helper()
		file := filepath.Join(dir, "1-orders.parquet")
		if _, err := tail.WriteFile(file); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		noMarkerWritten(t, file)
		return pgCopyState(t, base, tail, file)
	}
	untouched := []string{"1=one/note one", "2=two/note two", "3=three/note three", "4=∅/note four", "5=/note five", "6=six/note six"}
	with := func(rows ...string) []string {
		out := append([]string(nil), untouched...)
		for _, r := range rows {
			id := r[:strings.Index(r, "=")]
			found := false
			for i := range out {
				if strings.HasPrefix(out[i], id+"=") {
					out[i], found = r, true
				}
			}
			if !found {
				out = append(out, r)
			}
		}
		sort.Strings(out)
		return out
	}

	t.Run("an earlier change in the tail gives it, and the copy is not asked", func(t *testing.T) {
		dir, base, tail := rig(t, nil)
		apply(t, tail, []query.ResultRow{copyUpd(10, 0x700, "1", "S", "N")}, []query.ResultRow{copyUpd(11, 0x710, "1", u, "n2")})
		if got := wants(tail); got != " " {
			t.Fatalf("the tail asks the copy for %q; its own earlier change holds the value", got)
		}
		if got, want := written(t, tail, dir, base), with("1=S/n2"); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	t.Run("with no earlier change it is the row's in the base file, and nothing is written until it is read", func(t *testing.T) {
		dir, base, tail := rig(t, nil)
		apply(t, tail, []query.ResultRow{copyUpd(10, 0x700, "1", u, "n"), copyUpd(11, 0x710, "2", "plain", "p")})
		if got := wants(tail); got != "1 status" {
			t.Fatalf("the tail asks the copy for %q, want key 1's status", got)
		}
		file := filepath.Join(dir, "0-orders.parquet")
		_, err := tail.WriteFile(file)
		var refusal *TailRefusal
		if !errors.Is(err, ErrPGTailCopyPending) || asTailRefusal(err, &refusal) {
			t.Fatalf("a write while a value waits to be read: %v; want ErrPGTailCopyPending and no refusal", err)
		}
		if _, serr := os.Lstat(file); !os.IsNotExist(serr) {
			t.Fatalf("a file was written while a value waited (%v)", serr)
		}
		if tail.Dirty() != 2 {
			t.Fatalf("%d keys are still to write after a write that could not be made, want 2", tail.Dirty())
		}
		if asked, given, why := readCopy(t, tail, base); asked != 1 || given != 1 || why != "" {
			t.Fatalf("read of the copy: %d keys asked, %d values given, refusal %q", asked, given, why)
		}
		if got := wants(tail); got != " " {
			t.Fatalf("after the read the tail still asks for %q", got)
		}
		if _, held := tail.held("1", tail.fold.Changes["1"]); !held {
			t.Fatal("the row given its value is not put away packed")
		}
		if got, want := written(t, tail, dir, base), with("1=one/n", "2=plain/p"); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	t.Run("the pair beside the file before the file", func(t *testing.T) {
		dir, base, tail := rig(t, map[string]*[3]any{"1": {"1", "uno", "nota"}, "2": nil, "9": {"9", "nueve", "nota 9"}})
		apply(t, tail, []query.ResultRow{copyUpd(10, 0x700, "1", u, u), copyUpd(11, 0x710, "3", u, "n3"), copyUpd(12, 0x720, "9", "s9", u)})
		if got := wants(tail); got != "1,3,9 note,status" {
			t.Fatalf("the tail asks the copy for %q", got)
		}
		if asked, given, why := readCopy(t, tail, base); asked != 3 || given != 4 || why != "" {
			t.Fatalf("read of the copy: %d keys asked, %d values given, refusal %q", asked, given, why)
		}
		want := []string{"1=uno/nota", "3=three/n3", "4=∅/note four", "5=/note five", "6=six/note six", "9=s9/nota 9"}
		if got := written(t, tail, dir, base); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	// What the copy cannot give: each refuses the tail, in its own words,
	// and leaves the marker where it was. Nothing is written.
	for name, c := range map[string]struct {
		pair map[string]*[3]any
		key  string
		says string
	}{
		"a key the cut deleted":           {map[string]*[3]any{"2": nil}, "2", "the copy has no row under that key"},
		"a key the cut never held":        {nil, "77", "the copy has no row under that key"},
		"a NULL in the copy":              {nil, "4", "a NULL or an empty text"},
		"an empty text in the copy":       {nil, "5", "a NULL or an empty text"},
		"a NULL the pair put in the copy": {map[string]*[3]any{"6": {"6", nil, "x"}}, "6", "a NULL or an empty text"},
	} {
		t.Run(name+" is refused, never written", func(t *testing.T) {
			dir, base, tail := rig(t, c.pair)
			apply(t, tail, []query.ResultRow{copyUpd(10, 0x700, c.key, u, "n"), copyUpd(11, 0x710, "1", "plain", "p")})
			_, given, why := readCopy(t, tail, base)
			if given != 0 || !strings.Contains(why, c.says) || !strings.Contains(why, fmt.Sprintf("key %q", c.key)) || !strings.Contains(why, "column status") {
				t.Fatalf("%d values given, refusal %q; want one that says %q of key %s", given, why, c.says, c.key)
			}
			if v := tail.fold.Changes[c.key].RowAfter["status"]; !event.IsUnchangedToastMarker(v) {
				t.Fatalf("the row was given %v in the place of a value the copy does not hold", v)
			}
			file := filepath.Join(dir, "1-orders.parquet")
			_, err := tail.WriteFile(file)
			var refusal *TailRefusal
			if !asTailRefusal(err, &refusal) || refusal.Reason != why {
				t.Fatalf("the write after the refusal: %v; want the refusal %q", err, why)
			}
			if _, serr := os.Lstat(file); !os.IsNotExist(serr) {
				t.Fatalf("a file was written for a refused tail (%v)", serr)
			}
			if keys, _ := tail.CopyWants(); len(keys) != 0 {
				t.Fatalf("a refused tail still asks the copy for %v", keys)
			}
		})
	}

	t.Run("two large values of one row, one from the tail and one from the copy", func(t *testing.T) {
		dir, base, tail := rig(t, nil)
		apply(t, tail, []query.ResultRow{copyUpd(10, 0x700, "1", "sent", u)}, []query.ResultRow{copyUpd(11, 0x710, "1", u, u)})
		if got := wants(tail); got != "1 note" {
			t.Fatalf("the tail asks the copy for %q, want key 1's note alone", got)
		}
		readCopy(t, tail, base)
		if got, want := written(t, tail, dir, base), with("1=sent/note one"); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	t.Run("an INSERT and then an UPDATE that leaves its values", func(t *testing.T) {
		dir, base, tail := rig(t, nil)
		apply(t, tail, []query.ResultRow{copyIns(10, 0x700, "8", "s8", "n8"), copyUpd(11, 0x710, "8", u, u)})
		if got := wants(tail); got != " " {
			t.Fatalf("the tail asks the copy for %q", got)
		}
		if got, want := written(t, tail, dir, base), with("8=s8/n8"); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	t.Run("a DELETE, an INSERT and then an UPDATE that leaves a value", func(t *testing.T) {
		dir, base, tail := rig(t, nil)
		apply(t, tail, []query.ResultRow{copyDel(10, 0x700, "1")}, []query.ResultRow{copyIns(11, 0x710, "1", "again", "new")}, []query.ResultRow{copyUpd(12, 0x720, "1", u, "newer")})
		if got := wants(tail); got != " " {
			t.Fatalf("the tail asks the copy for %q; the row in the copy is the deleted one", got)
		}
		if got, want := written(t, tail, dir, base), with("1=again/newer"); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	t.Run("a value behind a DELETE is not asked of the copy, whose row is the deleted one", func(t *testing.T) {
		dir, _, tail := rig(t, nil)
		apply(t, tail, []query.ResultRow{copyDel(10, 0x700, "1")}, []query.ResultRow{copyUpd(12, 0x720, "1", u, "newer")})
		if got := wants(tail); got != " " {
			t.Fatalf("the tail asks the copy for %q", got)
		}
		var refusal *TailRefusal
		if _, err := tail.WriteFile(filepath.Join(dir, "1-orders.parquet")); !asTailRefusal(err, &refusal) || !strings.Contains(refusal.Reason, "as it was") {
			t.Fatalf("the write: %v; want a refusal", err)
		}
	})

	t.Run("a nearer change that arrives late gives the value, before and after the copy was read", func(t *testing.T) {
		// Before: the copy is not asked at all.
		dir, base, tail := rig(t, nil)
		apply(t, tail, []query.ResultRow{copyUpd(11, 0x720, "1", u, "n")})
		if got := wants(tail); got != "1 status" {
			t.Fatalf("the tail asks the copy for %q", got)
		}
		apply(t, tail, []query.ResultRow{copyUpd(10, 0x710, "1", "late", "x")})
		if got := wants(tail); got != " " {
			t.Fatalf("after the nearer change arrived the tail still asks for %q", got)
		}
		if got, want := written(t, tail, dir, base), with("1=late/n"); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
		// After: the copy's value is replaced, in the row already put away.
		dir, base, tail = rig(t, nil)
		apply(t, tail, []query.ResultRow{copyUpd(11, 0x720, "1", u, "n")})
		readCopy(t, tail, base)
		apply(t, tail, []query.ResultRow{copyUpd(10, 0x710, "1", "late", "x")})
		if got, want := written(t, tail, dir, base), with("1=late/n"); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v: the copy's value stayed over a nearer change's", got, want)
		}
		// A late change that left the value too: the copy's stands.
		dir, base, tail = rig(t, nil)
		apply(t, tail, []query.ResultRow{copyUpd(11, 0x720, "1", u, "n")})
		apply(t, tail, []query.ResultRow{copyUpd(10, 0x710, "1", u, "x")})
		if got := wants(tail); got != "1 status" {
			t.Fatalf("the tail asks the copy for %q", got)
		}
		readCopy(t, tail, base)
		if got, want := written(t, tail, dir, base), with("1=one/n"); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	t.Run("after a prune the value is the newer cut's", func(t *testing.T) {
		dir, old, tail := rig(t, nil)
		apply(t, tail, []query.ResultRow{copyUpd(10, 0x700, "1", "S1", "N1"), copyUpd(11, 0x720, "2", u, "kept")})
		readCopy(t, tail, old)
		// A snapshot update publishes the table as of 708: key 1's change is
		// in it, key 2's is after it.
		newer := append([][3]any{{"1", "S1", "N1"}}, baseRows[1:]...)
		base, _ := writePGCopyBase(t, filepath.Join(dir, "cut2"), 0x708, pgCopyRel("id"), newer...)
		if n, err := tail.PruneThrough(PGTailPos(0x708), 0); err != nil || n != 1 {
			t.Fatalf("PruneThrough dropped %d keys, %v; want key 1", n, err)
		}
		apply(t, tail, []query.ResultRow{copyUpd(12, 0x730, "1", u, "n")})
		if got := wants(tail); got != "1 status" {
			t.Fatalf("after the prune the tail asks the copy for %q, want key 1's status", got)
		}
		if asked, given, why := readCopy(t, tail, base); asked != 1 || given != 1 || why != "" {
			t.Fatalf("read of the newer cut: %d keys asked, %d values given, refusal %q", asked, given, why)
		}
		file := filepath.Join(dir, "2-orders.parquet")
		if _, err := tail.WriteFile(file); err != nil {
			t.Fatal(err)
		}
		noMarkerWritten(t, file)
		want := []string{"1=S1/n", "2=two/kept", "3=three/note three", "4=∅/note four", "5=/note five", "6=six/note six"}
		if got := pgCopyState(t, base, tail, file); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	t.Run("an answer for a key that no longer waits changes nothing", func(t *testing.T) {
		dir, base, tail := rig(t, nil)
		apply(t, tail, []query.ResultRow{copyUpd(10, 0x700, "1", u, "n"), copyUpd(11, 0x700, "2", u, "n"), copyUpd(12, 0x700, "3", u, "n")})
		keys, cols := tail.CopyWants()
		rows, err := PGCutValues(context.Background(), base, tail.PKColumns(), keys, cols, duckdbutil.Tuning{}, 0)
		if err != nil {
			t.Fatal(err)
		}
		// While the copy was read: key 1 changed whole, key 2 was deleted.
		apply(t, tail, []query.ResultRow{copyUpd(13, 0x710, "1", "fresh", "f"), copyDel(14, 0x720, "2")})
		if given, why := tail.GiveCopyValues(keys, cols, rows); given != 1 || why != "" {
			t.Fatalf("%d values given, refusal %q; want key 3's alone", given, why)
		}
		want := []string{"1=fresh/f", "3=three/n", "4=∅/note four", "5=/note five", "6=six/note six"}
		if got := written(t, tail, dir, base); !reflect.DeepEqual(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})

	t.Run("a change of key with a value not sent is refused as before", func(t *testing.T) {
		_, _, tail := rig(t, nil)
		move := pgEv(10, 0x700, event.EventUpdate, copyKeyOnly("1"), map[string]any{"id": "9", "status": u, "note": "n"})
		if err := tail.Apply([]query.ResultRow{move}); err == nil {
			t.Fatal("a change of key whose new row holds a value that was not sent was folded")
		}
	})

	t.Run("a tail no one reads the copy for refuses as before", func(t *testing.T) {
		dir := t.TempDir()
		_, md := writePGCopyBase(t, filepath.Join(dir, "cut"), 0x500, pgCopyRel("id"), baseRows...)
		tail, err := NewPGTail("mydb", "orders", md, pgCopyCols, pkColsIntID(), nil)
		if err != nil {
			t.Fatal(err)
		}
		apply(t, tail, []query.ResultRow{copyUpd(10, 0x700, "1", u, "n")})
		if keys, _ := tail.CopyWants(); len(keys) != 0 {
			t.Fatalf("it asks the copy for %v", keys)
		}
		var refusal *TailRefusal
		if _, err := tail.WriteFile(filepath.Join(dir, "1-orders.parquet")); !asTailRefusal(err, &refusal) || !strings.Contains(refusal.Reason, "as it was") {
			t.Fatalf("the write: %v; want the refusal", err)
		}
	})
}

// A key of two columns, of text with a pipe and a backslash, is found in the
// copy by the key the index files it under.
func TestPGTail_readsTheCopyByACompositeTextKey(t *testing.T) {
	dir := t.TempDir()
	base, md := writePGCopyBase(t, filepath.Join(dir, "cut"), 0x500, pgCopyRel("id", "status"),
		[3]any{`a|b`, `x\y`, "large one"}, [3]any{`a`, `b|x\y`, "large two"}, [3]any{"A|b", `x\y`, "large three"})
	pk := []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true}, {Name: "status", OrdinalPosition: 2, IsPK: true}}
	tail, err := NewPGTail("mydb", "orders", md, pgCopyCols, pk, nil)
	if err != nil {
		t.Fatal(err)
	}
	tail.AwaitCopyValues()
	upd := func(id, lsn uint64, a, b string) query.ResultRow {
		ev := query.ResultRow{EventID: id, BinlogFile: lsnText(lsn), StartPos: lsn, EndPos: lsn, EventTimestamp: foldBase,
			SchemaName: "mydb", TableName: "orders", EventType: event.EventUpdate,
			RowBefore: map[string]any{"id": a, "status": b, "note": notCaptured()},
			RowAfter:  map[string]any{"id": a, "status": b, "note": unsent()}}
		ev.PKValues = event.BuildPKValues(pk, ev.RowBefore)
		return ev
	}
	if err := tail.Apply([]query.ResultRow{upd(10, 0x700, `a|b`, `x\y`), upd(11, 0x710, `a`, `b|x\y`)}); err != nil {
		t.Fatal(err)
	}
	if asked, given, why := readCopy(t, tail, base); asked != 2 || given != 2 || why != "" {
		t.Fatalf("read of the copy: %d keys asked, %d values given, refusal %q", asked, given, why)
	}
	file := filepath.Join(dir, "1-orders.parquet")
	if _, err := tail.WriteFile(file); err != nil {
		t.Fatal(err)
	}
	want := []string{`A|b=x\y/large three`, `a=b|x\y/large two`, `a|b=x\y/large one`}
	if got := pgCopyState(t, base, tail, file); !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
}

// Hundreds of thousands of keys that wait are read in one read of the copy,
// bounded by the text it may bring in: past it nothing is given.
func TestPGTail_readsTheCopyForManyKeysAtOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("writes and reads a table of 120,000 rows")
	}
	const n = 120_000
	dir := t.TempDir()
	rows := make([][3]any, n)
	for i := range rows {
		rows[i] = [3]any{fmt.Sprint(i + 1), fmt.Sprintf("large value %d", i+1), "note"}
	}
	base, md := writePGCopyBase(t, filepath.Join(dir, "cut"), 0x500, pgCopyRel("id"), rows...)
	tail := newPGCopyTail(t, md)
	u := unsent()
	for from := 0; from < n; from += 5000 {
		page := make([]query.ResultRow, 0, 5000)
		for i := from; i < from+5000; i++ {
			page = append(page, copyUpd(uint64(i+1), uint64(0x10000+i), fmt.Sprint(i+1), u, "changed"))
		}
		if err := tail.Apply(page); err != nil {
			t.Fatal(err)
		}
	}
	keys, cols := tail.CopyWants()
	if len(keys) != n || !reflect.DeepEqual(cols, []string{"status"}) {
		t.Fatalf("%d keys wait for %v, want %d for status", len(keys), cols, n)
	}
	// Less text than the values hold: nothing is read into the tail.
	if _, err := PGCutValues(context.Background(), base, tail.PKColumns(), keys, cols, duckdbutil.Tuning{}, 1<<20); !errors.Is(err, pgdelta.ErrBaseValuesTooLarge) {
		t.Fatalf("a megabyte allowed for more text than that: %v", err)
	}
	if got, _ := tail.CopyWants(); len(got) != n {
		t.Fatalf("%d keys wait after a read that was refused, want %d", len(got), n)
	}
	if asked, given, why := readCopy(t, tail, base); asked != n || given != n || why != "" {
		t.Fatalf("read of the copy: %d keys asked, %d values given, refusal %q", asked, given, why)
	}
	file := filepath.Join(dir, "1-orders.parquet")
	if written, err := tail.WriteFile(file); err != nil || written != n {
		t.Fatalf("WriteFile: %d rows, %v", written, err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var wrong int
	if err := db.QueryRow("SELECT count(*) FROM read_parquet(" + lit(file) + ") WHERE status IS DISTINCT FROM 'large value ' || id OR note <> 'changed'").Scan(&wrong); err != nil || wrong != 0 {
		t.Fatalf("%d rows of the tail do not hold their own value from the copy (%v)", wrong, err)
	}
}

// A cut that does not read is an error with its cause, never a panic and
// never a value.
func TestPGCutValues_aCutThatDoesNotRead(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	base, _ := writePGCopyBase(t, filepath.Join(dir, "cut"), 0x500, pgCopyRel("id"), [3]any{"1", "one", "n"})
	read := func(path string) error {
		_, err := PGCutValues(ctx, path, []string{"id"}, []string{"1"}, []string{"status"}, duckdbutil.Tuning{}, 0)
		return err
	}
	if err := read(base); err != nil {
		t.Fatal(err)
	}
	if err := read(filepath.Join(dir, "cut", "gone.parquet")); err == nil {
		t.Fatal("a file that is not there gave an answer")
	}
	garbage := filepath.Join(dir, "garbage.parquet")
	if err := os.WriteFile(garbage, []byte("this is not a parquet file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := read(garbage); err == nil {
		t.Fatal("a file that is not Parquet gave an answer")
	}
	if err := read("s3://bucket/snap/orders.parquet"); err == nil || !strings.Contains(err.Error(), "not a local table file") {
		t.Fatalf("a file in a bucket: %v", err)
	}
	// Half a pair beside the file: what the table holds cannot be said.
	_, rows, err := pgdelta.Paths(base, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rows, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := read(base); !errors.Is(err, pgdelta.ErrBrokenChain) {
		t.Fatalf("half a pair beside the file: %v; want a broken chain", err)
	}
	// Nothing to read reads nothing, whatever is there.
	if got, err := PGCutValues(ctx, garbage, []string{"id"}, nil, []string{"status"}, duckdbutil.Tuning{}, 0); err != nil || len(got) != 0 {
		t.Fatalf("no key to read: %v, %v", got, err)
	}
}
