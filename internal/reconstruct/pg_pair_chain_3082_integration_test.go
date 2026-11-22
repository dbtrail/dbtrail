//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// chainRows is a table of 64 rows: the keys the tests below change stay a
// small share of it, so no pair ends for its size.
func chainRows() [][2]string {
	rows := pairRows()
	for i := 100; i < 140; i++ {
		rows = append(rows, [2]string{fmt.Sprint(i), "more"})
	}
	return rows
}

// chainRig is two folders fed the same snapshot and the same updates, one
// with PostgreSQL table deltas on and one with them off: the second's
// rewritten file is what the first must read as at every step.
type chainRig struct {
	t       *testing.T
	f       *pgFixture
	on, off string
	taken   time.Time
}

func newChainRig(t *testing.T) *chainRig {
	t.Helper()
	f := newPGFixture(t)
	r := &chainRig{t: t, f: f, on: t.TempDir(), off: t.TempDir(), taken: f.at(10 * time.Minute)}
	for _, root := range []string{r.on, r.off} {
		f.baseline(root, r.taken, 0x500, r.taken.Add(-time.Minute).Format(time.RFC3339), chainRows()...)
	}
	f.other(0x600, 12*time.Minute)
	return r
}

// update runs the update at at in both folders and returns the report of
// the folder with deltas on and the table as it reads there.
func (r *chainRig) update(at time.Time) (*reconstruct.TableReport, map[string]string, map[string]string) {
	r.t.Helper()
	reconstruct.SetPGTableDeltas(r.t, false)
	want, _, _, err := r.f.run(r.off, pgRun{at: at, carry: true, chainFloor: r.f.base})
	if err != nil {
		r.t.Fatal(err)
	}
	reconstruct.SetPGTableDeltas(r.t, true)
	_, _, reps, err := r.f.run(r.on, pgRun{at: at, carry: true, chainFloor: r.f.base})
	if err != nil {
		r.t.Fatal(err)
	}
	got, footer := r.f.state(r.on, at)
	if !sameRows(got, want) {
		r.t.Fatalf("at %s the table reads %v, and a rewrite gives %v", at.Format("15:04"), got, want)
	}
	return reps[0], got, footer
}

// pairs is how many pairs sit beside the table file of the snapshot at at.
func (r *chainRig) pairs(at time.Time) int {
	r.t.Helper()
	return len(r.f.pairFiles(r.on, at)) / 2
}

// checkChain fails unless the snapshot at at is its table file (the same
// file as base's snapshot holds) and a chain of n pairs whose last footer
// records the rows the table holds and the pair before it.
func (r *chainRig) checkChain(at, base time.Time, n int, rep *reconstruct.TableReport, got, footer map[string]string) {
	r.t.Helper()
	when := at.Format("15:04")
	if p := r.pairs(at); p != n {
		r.t.Fatalf("at %s: %d pairs beside the file (%v), want %d", when, p, r.f.pairFiles(r.on, at), n)
	}
	if !r.f.sameFile(r.f.tableFile(r.on, base), r.f.tableFile(r.on, at)) {
		r.t.Fatalf("at %s the table file is not the one of %s", when, base.Format("15:04"))
	}
	if len(rep.Files) != 1+2*n {
		r.t.Fatalf("at %s the report names %v; want the file and %d pairs", when, rep.Files, n)
	}
	if footer[pgdelta.MetaKeySeq] != strconv.Itoa(n-1) {
		r.t.Fatalf("at %s the last pair says it is pair %q, want %d", when, footer[pgdelta.MetaKeySeq], n-1)
	}
	if rows := footer["bintrail.pgdelta_rows"]; rows != strconv.Itoa(len(got)) || rep.RowsWritten != int64(len(got)) {
		r.t.Fatalf("at %s the last pair records %q rows and the report %d; the table holds %d", when, rows, rep.RowsWritten, len(got))
	}
}

// #3082: every update writes the pair of its own window beside the table
// file, after the pairs the updates before it wrote, instead of reading the
// pair before, merging the window into it and writing it whole again. At
// every step the table reads as a rewrite leaves it: a key changed in
// several pairs reads as its newest, a key deleted in one pair and put back
// in a later one is back, a key moved by an UPDATE of its key is under its
// new key, and a window with no change carries the chain as it is. The
// chain ends when it is older than a chain may be, folded into a new file
// by the engine, and the next update starts a chain beside that file.
func TestReconstructTables_PGPairChain(t *testing.T) {
	// A chain older than 25 minutes ends: the sixth update (24 minutes after
	// the file) adds a pair, the seventh (28) writes the table in full.
	reconstruct.SetPGChainMaxAge(t, 25*time.Minute)
	folds := reconstruct.CountPGFoldsByEngine(t, true)
	r := newChainRig(t)
	f := r.f

	// Pair 0, from the file's floor: three changes.
	f.event(0x700, 12*time.Minute, 2, "1", s("one"))
	f.event(0x710, 13*time.Minute, 3, "2", nil)
	f.event(0x720, 13*time.Minute, 1, "40", s("forty"))
	f.checkpoint(0x720)
	at1 := f.at(14 * time.Minute)
	rep, got, footer := r.update(at1)
	r.checkChain(at1, r.taken, 1, rep, got, footer)
	if rep.EventsApplied != 3 || footer[pgdelta.MetaKeyPrev] != "" {
		t.Fatalf("first update: %d events, pair before %q; want 3 and none", rep.EventsApplied, footer[pgdelta.MetaKeyPrev])
	}

	// Pair 1: keys pair 0 holds, changed again. Only this window is read.
	f.event(0x800, 15*time.Minute, 2, "1", s("one again"))
	f.event(0x810, 16*time.Minute, 2, "40", s("forty again"))
	f.checkpoint(0x810)
	at2 := f.at(18 * time.Minute)
	rep, got, footer = r.update(at2)
	r.checkChain(at2, r.taken, 2, rep, got, footer)
	if rep.EventsApplied != 2 || footer[pgdelta.MetaKeyPrev] == "" || got["1"] != "one again" || got["40"] != "forty again" {
		t.Fatalf("second update: %d events, pair before %q, 1=%q 40=%q; want 2 events after pair 0, the newest values",
			rep.EventsApplied, footer[pgdelta.MetaKeyPrev], got["1"], got["40"])
	}
	// The pair of the second window holds its two keys and nothing of the
	// first window: one pair per window, not one that grows.
	if rows := pairRowCount(t, r.on, at2, 1); rows != 2 {
		t.Fatalf("pair 1 holds %d rows; want the 2 keys of its own window", rows)
	}

	// No change: the file and its chain, as they are.
	at3 := f.at(22 * time.Minute)
	rep, _, _ = r.update(at3)
	if !rep.CarriedForward || r.pairs(at3) != 2 || len(rep.Files) != 5 {
		t.Fatalf("third update: carried %v, %d pairs, files %v; want the file and its two pairs carried", rep.CarriedForward, r.pairs(at3), rep.Files)
	}

	// Pair 2: the key pair 0 deleted is put back, the key pair 0 inserted
	// is deleted, a new key.
	f.event(0x900, 23*time.Minute, 1, "2", s("two is back"))
	f.event(0x910, 24*time.Minute, 3, "40", nil)
	f.event(0x920, 25*time.Minute, 1, "41", s("forty-one"))
	f.checkpoint(0x920)
	at4 := f.at(26 * time.Minute)
	rep, got, footer = r.update(at4)
	r.checkChain(at4, r.taken, 3, rep, got, footer)
	if rep.EventsApplied != 3 || got["2"] != "two is back" || got["40"] != "" || got["41"] != "forty-one" {
		t.Fatalf("fourth update: %d events, 2=%q 40=%q 41=%q", rep.EventsApplied, got["2"], got["40"], got["41"])
	}

	// Pair 3: a key moved by an UPDATE of its key, and a key three pairs
	// changed, changed again.
	f.move(0xa00, 27*time.Minute, "5", "50", "moved")
	f.event(0xa10, 28*time.Minute, 2, "1", s("one, third time"))
	f.checkpoint(0xa10)
	at5 := f.at(30 * time.Minute)
	rep, got, footer = r.update(at5)
	r.checkChain(at5, r.taken, 4, rep, got, footer)
	if _, still := got["5"]; still || got["50"] != "moved" || got["1"] != "one, third time" {
		t.Fatalf("fifth update: 5 there %v, 50=%q, 1=%q", still, got["50"], got["1"])
	}

	// Pair 4: deleted and put back in one window, of a key a pair before
	// inserted.
	f.event(0xb00, 31*time.Minute, 3, "41", nil)
	f.event(0xb10, 32*time.Minute, 1, "41", s("forty-one again"))
	f.event(0xb20, 33*time.Minute, 2, "3", s("three"))
	f.checkpoint(0xb20)
	at6 := f.at(34 * time.Minute)
	rep, got, footer = r.update(at6)
	r.checkChain(at6, r.taken, 5, rep, got, footer)
	if got["41"] != "forty-one again" || *folds != 0 {
		t.Fatalf("sixth update: 41=%q, %d folds by the engine; want the key back and no fold yet", got["41"], *folds)
	}

	// The chain is older than a chain may be: the table is written in full
	// from the file and the five pairs, by the engine, with only this
	// window read from the index.
	f.event(0xc00, 35*time.Minute, 2, "7", s("seven"))
	f.checkpoint(0xc00)
	at7 := f.at(38 * time.Minute)
	rep, got, _ = r.update(at7)
	if r.pairs(at7) != 0 || len(rep.Files) != 1 || r.f.sameFile(r.f.tableFile(r.on, r.taken), r.f.tableFile(r.on, at7)) {
		t.Fatalf("seventh update: %d pairs, files %v; want the table written in full", r.pairs(at7), rep.Files)
	}
	if rep.EventsApplied != 1 || *folds != 1 || rep.DeltaCompacted == "" || rep.RowsWritten != int64(len(got)) {
		t.Fatalf("seventh update: %d events, %d folds by the engine, reason %q, %d rows of %d; want 1 event, 1 fold, a reason, every row",
			rep.EventsApplied, *folds, rep.DeltaCompacted, rep.RowsWritten, len(got))
	}

	// A chain starts beside the new file.
	f.event(0xd00, 39*time.Minute, 2, "8", s("eight"))
	f.checkpoint(0xd00)
	at8 := f.at(42 * time.Minute)
	rep, got, footer = r.update(at8)
	r.checkChain(at8, at7, 1, rep, got, footer)
	if rep.EventsApplied != 1 {
		t.Fatalf("eighth update: %d events; want the 1 since the new file", rep.EventsApplied)
	}
	f.event(0xe00, 43*time.Minute, 3, "8", nil)
	f.checkpoint(0xe00)
	at9 := f.at(46 * time.Minute)
	rep, got, footer = r.update(at9)
	r.checkChain(at9, at7, 2, rep, got, footer)

	// Every older snapshot still reads as it did.
	if old, _ := r.f.state(r.on, at2); old["1"] != "one again" || old["2"] != "" {
		t.Fatalf("the second snapshot changed under the later ones: %v", old)
	}
	if old, _ := r.f.state(r.on, at5); old["41"] != "forty-one" || old["50"] != "moved" {
		t.Fatalf("the fifth snapshot changed under the later ones: %v", old)
	}
}

// pairRowCount is how many rows the .pgnew file of pair seq holds in the
// snapshot at at.
func pairRowCount(t *testing.T, root string, at time.Time, seq int) int64 {
	t.Helper()
	_, rows, err := pgdelta.Paths(filepath.Join(root, reconstruct.SnapshotDirName(at), "public", "orders.parquet"), seq)
	if err != nil {
		t.Fatal(err)
	}
	n, err := baseline.TableFileRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// A chain that is not whole beside the previous file is never read as an
// older table: the update reads every change since the file again, from the
// index, and starts a chain of one pair. A file a write left behind is not
// part of the chain and changes nothing.
func TestReconstructTables_PGPairChainNotWhole(t *testing.T) {
	cases := []struct {
		name   string
		damage func(t *testing.T, base string)
		// whole: the chain is still whole and is continued.
		whole bool
	}{
		{"a pair in the middle is gone", func(t *testing.T, base string) { removePair(t, base, 1, true, true) }, false},
		{"pair 0 is gone, so no file of the chain's start is there", func(t *testing.T, base string) { removePair(t, base, 0, true, true) }, false},
		{"the last pair is gone, and the snapshot records it", func(t *testing.T, base string) { removePair(t, base, 2, true, true) }, false},
		{"the last pair is half there: the process stopped between its two names", func(t *testing.T, base string) {
			removePair(t, base, 2, true, false)
		}, false},
		{"a file a write was cut short in is beside the chain", func(t *testing.T, base string) {
			_, rows, err := pgdelta.Paths(base, 3)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(rows+".tmp-4821", []byte("half written"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baselineintegrity.ForgetVerified()
			t.Cleanup(baselineintegrity.ForgetVerified)
			r := newChainRig(t)
			f := r.f
			// Three pairs, one change each.
			var last time.Time
			for i, key := range []string{"1", "3", "7"} {
				lsn := uint64(0x700 + 0x100*i)
				f.event(lsn, time.Duration(12+4*i)*time.Minute, 2, key, s("changed"))
				f.checkpoint(lsn)
				last = f.at(time.Duration(14+4*i) * time.Minute)
				rep, got, footer := r.update(last)
				r.checkChain(last, r.taken, i+1, rep, got, footer)
			}
			c.damage(t, r.f.tableFile(r.on, last))
			baselineintegrity.ForgetVerified()

			f.event(0xc00, 25*time.Minute, 2, "9", s("nine"))
			f.checkpoint(0xc00)
			at := f.at(28 * time.Minute)
			rep, got, footer := r.update(at)
			if c.whole {
				r.checkChain(at, r.taken, 4, rep, got, footer)
				if rep.EventsApplied != 1 {
					t.Fatalf("%d events; want the 1 of the window after the chain", rep.EventsApplied)
				}
				return
			}
			// Read again from the file's floor: every change since the file,
			// in one pair that continues none.
			r.checkChain(at, r.taken, 1, rep, got, footer)
			if rep.EventsApplied != 4 || footer[pgdelta.MetaKeyPrev] != "" {
				t.Fatalf("%d events, pair before %q; want the 4 changes since the file, in a pair of its own", rep.EventsApplied, footer[pgdelta.MetaKeyPrev])
			}
			if got["1"] != "changed" || got["3"] != "changed" || got["7"] != "changed" || got["9"] != "nine" {
				t.Fatalf("the table reads %v", got)
			}
		})
	}
}

// A chain an update will not continue is not carried either: with no change
// since the file, the table would be published as the file and the same
// chain, under a new record that certifies it. The update refuses the table
// instead, and says why.
func TestReconstructTables_PGPairChainNotWholeIsNotCarried(t *testing.T) {
	baselineintegrity.ForgetVerified()
	t.Cleanup(baselineintegrity.ForgetVerified)
	r := newChainRig(t)
	f := r.f
	var last time.Time
	for i, key := range []string{"1", "3"} {
		lsn := uint64(0x700 + 0x100*i)
		f.event(lsn, time.Duration(12+4*i)*time.Minute, 2, key, s("changed"))
		f.checkpoint(lsn)
		last = f.at(time.Duration(14+4*i) * time.Minute)
		rep, got, footer := r.update(last)
		r.checkChain(last, r.taken, i+1, rep, got, footer)
	}
	removePair(t, r.f.tableFile(r.on, last), 1, true, true)
	baselineintegrity.ForgetVerified()
	// The index no longer holds the changes: nothing in the window.
	if _, err := f.db.Exec("DELETE FROM binlog_events"); err != nil {
		t.Fatal(err)
	}
	at := f.at(28 * time.Minute)
	reconstruct.SetPGTableDeltas(t, true)
	_, _, _, err := f.run(r.on, pgRun{at: at, carry: true, chainFloor: f.base})
	if !errors.Is(err, pgdelta.ErrBrokenChain) {
		t.Fatalf("the update over a chain short of its record: %v; want the broken chain refused", err)
	}
	if _, serr := os.Stat(f.tableFile(r.on, at)); serr == nil {
		t.Fatal("the table's file was published beside a chain that was refused")
	}
}

// A table with no key column is refused as it always was, chain or not: no
// pair can say which row of the file a change takes away.
func TestReconstructTables_PGPairChainTableWithNoKey(t *testing.T) {
	r := newChainRig(t)
	f := r.f
	f.event(0x700, 12*time.Minute, 2, "1", s("one"))
	f.checkpoint(0x700)
	at1 := f.at(14 * time.Minute)
	rep, got, footer := r.update(at1)
	r.checkChain(at1, r.taken, 1, rep, got, footer)

	// The source drops the table's primary key; the stream records the
	// shape it now has.
	rel := &metadata.PGRelationSchema{OID: pgOID, Schema: "public", Table: "orders", SeenLSN: 0x310, SeenAt: f.base}
	for i, c := range pgOrdersRel.Columns {
		rel.Columns = append(rel.Columns, metadata.PGRelationColumn{Name: c.Name, Ordinal: i + 1, TypeOID: c.TypeOID, TypeMod: c.TypeMod})
	}
	if _, err := metadata.WritePGSnapshot(context.Background(), f.db, rel); err != nil {
		t.Fatal(err)
	}
	f.event(0x800, 15*time.Minute, 2, "3", s("three"))
	f.checkpoint(0x800)
	at2 := f.at(18 * time.Minute)
	reconstruct.SetPGTableDeltas(t, true)
	if _, _, _, err := f.run(r.on, pgRun{at: at2, carry: true, chainFloor: f.base}); err == nil {
		t.Fatal("a table with no key column was published")
	}
	if _, err := os.Stat(f.tableFile(r.on, at2)); err == nil {
		t.Fatal("the table's file is in the snapshot folder of a refused update")
	}
	if dir := filepath.Join(r.on, reconstruct.SnapshotDirName(at2), "public"); dirHasPair(t, dir) {
		t.Fatal("a pair is in the snapshot folder of a refused update")
	}
}

// dirHasPair reports whether a file of a pair is in dir (missing: no).
func dirHasPair(t *testing.T, dir string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if _, ok := pgdelta.ParseName(e.Name()); ok || pgdelta.IsTemp(e.Name()) {
			return true
		}
	}
	return false
}

// removePair removes the .pgdead (dead) and the .pgnew (rows) file of pair
// seq beside the table file at base.
func removePair(t *testing.T, base string, seq int, dead, rows bool) {
	t.Helper()
	d, n, err := pgdelta.Paths(base, seq)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []struct {
		path string
		do   bool
	}{{d, dead}, {n, rows}} {
		if !x.do {
			continue
		}
		if err := os.Remove(x.path); err != nil {
			t.Fatal(err)
		}
	}
}

// A chain's age is counted from where it starts: the earliest its file's
// rows can stand at (the earlier of the file's time and its bound), as the
// MySQL chain's age is from its start and as FindBaseline reads it. A file
// written at 10 minutes whose rows stand at 2, with chains ended at 25
// minutes: at 30 the chain is 28 minutes old and is folded into a new file,
// though the file itself was written only 20 minutes before.
func TestReconstructTables_PGPairChainAgeFromTheFileBound(t *testing.T) {
	reconstruct.SetPGChainMaxAge(t, 25*time.Minute)
	f := newPGFixture(t)
	root := t.TempDir()
	taken := f.at(10 * time.Minute)
	f.baseline(root, taken, 0x500, f.at(2*time.Minute).Format(time.RFC3339), chainRows()...)
	f.other(0x600, 11*time.Minute)
	reconstruct.SetPGTableDeltas(t, true)
	f.event(0x700, 12*time.Minute, 2, "1", s("one"))
	f.checkpoint(0x700)
	at1 := f.at(14 * time.Minute)
	if _, _, _, err := f.run(root, pgRun{at: at1, carry: true, chainFloor: f.base}); err != nil {
		t.Fatal(err)
	}
	if len(f.pairFiles(root, at1)) != 2 {
		t.Fatalf("the first update wrote no pair: %v", f.pairFiles(root, at1))
	}
	f.event(0x800, 26*time.Minute, 2, "3", s("three"))
	f.checkpoint(0x800)
	at2 := f.at(30 * time.Minute)
	got, _, reps, err := f.run(root, pgRun{at: at2, carry: true, chainFloor: f.base})
	if err != nil {
		t.Fatal(err)
	}
	if left := f.pairFiles(root, at2); len(left) != 0 || reps[0].DeltaCompacted == "" || f.sameFile(f.tableFile(root, taken), f.tableFile(root, at2)) {
		t.Fatalf("at 30 minutes: pairs %v, reason %q; want the chain, 28 minutes old from its start, folded into a new file", left, reps[0].DeltaCompacted)
	}
	if got["1"] != "one" || got["3"] != "three" {
		t.Fatalf("the new file reads %v", got)
	}
}
