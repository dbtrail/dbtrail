//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// eventKey is event for a key whose text in the index (pk) is not its
// column's value (id): one with a pipe, which the index escapes.
func (f *pgFixture) eventKey(lsn uint64, d time.Duration, typ uint8, pk, id string, after *string) {
	f.t.Helper()
	var img, before []byte
	if after != nil {
		img, _ = json.Marshal(map[string]string{"id": id, "status": *after})
	}
	if typ != 1 {
		before, _ = json.Marshal(map[string]string{"id": id, "status": "x"})
	}
	testutil.InsertEvent(f.t, f.db, lsnString(lsn), lsn, lsn, f.at(d).Format("2006-01-02 15:04:05"), nil,
		"public", "orders", typ, pk, nil, before, img)
}

// A delta's files are in their snapshot's manifest, and a damaged one is not
// carried forward: the update refuses the table and says why, where it would
// have published the damage into a new snapshot with a clean manifest.
func TestReconstructTables_PGTablePairDamagedIsNotCarried(t *testing.T) {
	for _, suffix := range []string{pgdelta.DeadSuffix, pgdelta.NewSuffix} {
		t.Run(suffix, func(t *testing.T) {
			baselineintegrity.ForgetVerified()
			t.Cleanup(baselineintegrity.ForgetVerified)
			f := newPGFixture(t)
			root := t.TempDir()
			// As in the test below: a first update that carries the file,
			// then one that writes a pair beside it.
			f.baseline(root, f.at(10*time.Minute), 0x500, "", pairRows()...)
			f.other(0x600, 12*time.Minute)
			if _, _, _, err := f.run(root, pgRun{at: f.at(20 * time.Minute), carry: true}); err != nil {
				t.Fatal(err)
			}
			f.event(0x700, 22*time.Minute, 2, "1", s("late"))
			f.event(0x710, 23*time.Minute, 3, "2", nil)
			f.checkpoint(0x710)
			at2 := f.at(30 * time.Minute)
			reconstruct.SetPGTableDeltas(t, true)
			if _, _, _, err := f.run(root, pgRun{at: at2, carry: true, chainFloor: f.base}); err != nil {
				t.Fatal(err)
			}
			snap := filepath.Join(root, reconstruct.SnapshotDirName(at2))
			m, ok, err := baselineintegrity.LoadManifest(snap)
			if err != nil || !ok {
				t.Fatalf("the update's manifest: %v, %v", ok, err)
			}
			var damaged string
			for _, name := range f.pairFiles(root, at2) {
				if m.Files["public/"+name] == "" {
					t.Fatalf("%s is not in its snapshot's manifest: %v", name, m.Files)
				}
				if strings.HasSuffix(name, suffix) {
					damaged = filepath.Join(snap, "public", name)
				}
			}
			if damaged == "" {
				t.Fatalf("no %s file beside the table: %v", suffix, f.pairFiles(root, at2))
			}
			whole, err := os.ReadFile(damaged)
			if err != nil {
				t.Fatal(err)
			}
			whole[len(whole)/2] ^= 0xff
			if err := os.Remove(damaged); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(damaged, whole, 0o644); err != nil {
				t.Fatal(err)
			}
			baselineintegrity.ForgetVerified()

			// Nothing in the window: the table would be carried.
			testutil.MustExec(t, f.db, "DELETE FROM binlog_events")
			at3 := f.at(40 * time.Minute)
			_, _, _, err = f.run(root, pgRun{at: at3, carry: true, chainFloor: f.base})
			if !errors.Is(err, baselineintegrity.ErrIntegrity) {
				t.Fatalf("the update over a damaged %s: %v; want the integrity refusal", suffix, err)
			}
			if _, serr := os.Stat(f.tableFile(root, at3)); serr == nil {
				t.Fatal("the table's file was published beside a delta that was refused")
			}
		})
	}
}

// pairFiles is the names of the delta files beside the table file of the
// snapshot at at.
func (f *pgFixture) pairFiles(root string, at time.Time) []string {
	f.t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, reconstruct.SnapshotDirName(at), "public"))
	if err != nil {
		f.t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), pgdelta.DeadSuffix) || strings.HasSuffix(e.Name(), pgdelta.NewSuffix) || pgdelta.IsTemp(e.Name()) {
			names = append(names, e.Name())
		}
	}
	return names
}

// state is the table's rows as the snapshot at at leaves them: its table
// file and the chain beside it, checked against that file. It also returns
// the footer of the chain's last pair, nil with no pair.
func (f *pgFixture) state(root string, at time.Time) (map[string]string, map[string]string) {
	f.t.Helper()
	ctx := context.Background()
	dir := filepath.Join(root, reconstruct.SnapshotDirName(at), "public")
	base := filepath.Join(dir, "orders.parquet")
	entries, err := os.ReadDir(dir)
	if err != nil {
		f.t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	chain, err := pgdelta.ChainOf("orders", names)
	if err != nil {
		f.t.Fatal(err)
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		f.t.Fatal(err)
	}
	defer duck.Close()
	var footer map[string]string
	var dead, rows []string
	if len(chain.Pairs) > 0 {
		md, err := baseline.ReadParquetMetadata(base)
		if err != nil {
			f.t.Fatal(err)
		}
		fi, err := os.Stat(base)
		if err != nil {
			f.t.Fatal(err)
		}
		n, err := pgdelta.BaseRows(ctx, duck, base)
		if err != nil {
			f.t.Fatal(err)
		}
		id, err := pgdelta.BaseID(md, fi.Size(), n)
		if err != nil {
			f.t.Fatal(err)
		}
		if footer, err = pgdelta.CheckChain(ctx, duck, dir, chain, id, []string{"id", "status"}); err != nil {
			f.t.Fatalf("the chain beside %s: %v", base, err)
		}
		for _, p := range chain.Pairs {
			dead, rows = append(dead, filepath.Join(dir, p.Dead)), append(rows, filepath.Join(dir, p.New))
		}
	}
	rs, err := duck.Query("SELECT id, status FROM (" + pgdelta.StateSQL(base, dead, rows) + ")")
	if err != nil {
		f.t.Fatal(err)
	}
	defer rs.Close()
	got := map[string]string{}
	for rs.Next() {
		var id, st string
		if err := rs.Scan(&id, &st); err != nil {
			f.t.Fatal(err)
		}
		if _, twice := got[id]; twice {
			f.t.Fatalf("row %q is in the state twice", id)
		}
		got[id] = st
	}
	if err := rs.Err(); err != nil {
		f.t.Fatal(err)
	}
	return got, footer
}

// pairRows is a table of 24 rows: enough that a few changed keys are a
// small share of it. One key has a pipe in it.
func pairRows() [][2]string {
	rows := [][2]string{{"x|y", "piped"}}
	for i := 1; i <= 23; i++ {
		rows = append(rows, [2]string{fmt.Sprint(i), fmt.Sprintf("s%d", i)})
	}
	return rows
}

func rowsMap(rows [][2]string) map[string]string {
	m := map[string]string{}
	for _, r := range rows {
		m[r[0]] = r[1]
	}
	return m
}

// A PostgreSQL table that changed is published as its previous file and a
// pair beside it (#2757): the same rows a rewrite gives, update after
// update, with the file's footer left where it was, until the changes are
// too many beside the file and the table is written in full.
//
// Two folders are fed the same snapshot and the same runs: one with the
// switch on, one with it off, whose rewritten file is what the first must
// equal.
func TestReconstructTables_PGTablePair(t *testing.T) {
	ctx := context.Background()
	f := newPGFixture(t)
	onRoot, offRoot := t.TempDir(), t.TempDir()
	for _, root := range []string{onRoot, offRoot} {
		f.baseline(root, f.at(10*time.Minute), 0x500, "", pairRows()...)
	}
	f.other(0x600, 12*time.Minute)

	// both runs the update at at in the two folders, and returns the
	// reports of the folder with the switch on.
	both := func(at time.Time) []*reconstruct.TableReport {
		t.Helper()
		reconstruct.SetPGTableDeltas(t, false)
		want, wantMD, _, err := f.run(offRoot, pgRun{at: at, carry: true, chainFloor: f.base})
		if err != nil {
			t.Fatal(err)
		}
		if left := f.pairFiles(offRoot, at); len(left) != 0 {
			t.Fatalf("with the switch off the folder holds %v", left)
		}
		reconstruct.SetPGTableDeltas(t, true)
		_, _, reps, err := f.run(onRoot, pgRun{at: at, carry: true, chainFloor: f.base})
		if err != nil {
			t.Fatal(err)
		}
		got, footer := f.state(onRoot, at)
		if !sameRows(got, want) {
			t.Fatalf("at %s the table reads %v, and a rewrite gives %v", at.Format("15:04"), got, want)
		}
		// A pair's footer is the one the rewrite wrote on its file.
		if footer != nil {
			if footer[baseline.MetaKeyLSN] != fmt.Sprint(wantMD.LSN) || footer[baseline.MetaKeyPGDeltaSince] != wantMD.PGDeltaSince || wantMD.PGDeltaSince == "" {
				t.Fatalf("the pair's footer says LSN %s since %q, and the rewrite's %d since %q",
					footer[baseline.MetaKeyLSN], footer[baseline.MetaKeyPGDeltaSince], wantMD.LSN, wantMD.PGDeltaSince)
			}
		}
		return reps
	}

	// The full read's file records no bound: written in full, switch or not.
	at1 := f.at(20 * time.Minute)
	reps := both(at1)
	if left := f.pairFiles(onRoot, at1); len(left) != 0 || len(reps[0].Files) != 1 {
		t.Fatalf("first update: delta files %v, files %v; want the table alone", left, reps[0].Files)
	}
	md1, err := baseline.ReadParquetMetadata(f.tableFile(onRoot, at1))
	if err != nil {
		t.Fatal(err)
	}

	// An update, a delete, an insert, and both of a key with a pipe.
	f.event(0x700, 22*time.Minute, 2, "1", s("late"))
	f.event(0x710, 23*time.Minute, 3, "2", nil)
	f.event(0x720, 24*time.Minute, 1, "40", s("new"))
	f.eventKey(0x730, 25*time.Minute, 2, `x\|y`, "x|y", s("piped twice"))
	f.checkpoint(0x730)
	at2 := f.at(30 * time.Minute)
	reps = both(at2)
	want := rowsMap(pairRows())
	want["1"], want["40"], want["x|y"] = "late", "new", "piped twice"
	delete(want, "2")
	if got, _ := f.state(onRoot, at2); !sameRows(got, want) {
		t.Fatalf("second update reads %v, want %v", got, want)
	}
	if left := f.pairFiles(onRoot, at2); len(left) != 2 {
		t.Fatalf("second update: delta files %v, want one pair", left)
	}
	r := reps[0]
	if r.CarriedForward || !r.PG || len(r.Files) != 3 || r.BaselineRows != 21 || r.UpdatesApplied != 2 || r.InsertsEmitted != 1 || r.DeletesSkipped != 1 || r.RowsWritten != 24 {
		t.Fatalf("second update's report: carried %v pg %v files %v file rows %d updates %d inserts %d deletes %d rows %d",
			r.CarriedForward, r.PG, r.Files, r.BaselineRows, r.UpdatesApplied, r.InsertsEmitted, r.DeletesSkipped, r.RowsWritten)
	}
	// The table file is the first update's own, footer and all.
	if !f.sameFile(f.tableFile(onRoot, at1), f.tableFile(onRoot, at2)) {
		t.Fatal("the table file beside the pair is not the previous snapshot's file")
	}
	// Its readers start where the file's footer says.
	if from, err := reconstruct.SnapshotReadsFromPG(ctx, onRoot, at2); err != nil || from.UTC().Format(time.RFC3339) != md1.PGDeltaSince {
		t.Fatalf("SnapshotReadsFromPG = %v, %v; want the table file's bound %s", from, err, md1.PGDeltaSince)
	}

	// More changes, of keys the first pair held and of new ones: one pair
	// again, with everything since the file was written.
	f.event(0x800, 32*time.Minute, 2, "40", s("newer"))
	f.event(0x810, 33*time.Minute, 1, "2", s("back"))
	f.event(0x820, 34*time.Minute, 3, "3", nil)
	f.event(0x830, 35*time.Minute, 1, "41", s("brief"))
	f.event(0x840, 36*time.Minute, 3, "41", nil)
	f.checkpoint(0x840)
	at3 := f.at(40 * time.Minute)
	reps = both(at3)
	// Written from the second update's pair and the five changes since:
	// the four before them were not read again from the index.
	if _, footer := f.state(onRoot, at3); reps[0].EventsApplied != 5 || footer["bintrail.pgdelta_from"] == "" {
		t.Fatalf("third update applied %d events and its pair continues %q; want the 5 since the pair before, continued",
			reps[0].EventsApplied, footer["bintrail.pgdelta_from"])
	}
	want["40"], want["2"] = "newer", "back"
	delete(want, "3")
	if got, _ := f.state(onRoot, at3); !sameRows(got, want) {
		t.Fatalf("third update reads %v, want %v", got, want)
	}
	if left := f.pairFiles(onRoot, at3); len(left) != 2 || reps[0].CarriedForward || !f.sameFile(f.tableFile(onRoot, at1), f.tableFile(onRoot, at3)) {
		t.Fatalf("third update: delta files %v carried %v; want one pair beside the first update's file", left, reps[0].CarriedForward)
	}
	// The second snapshot is as it was.
	if got, _ := f.state(onRoot, at2); got["40"] != "new" || got["3"] != "s3" {
		t.Fatalf("the second snapshot changed under the third: %v", got)
	}

	// Too many changes beside 24 rows: the table is written in full, and
	// the next update starts from that file.
	for i := 10; i <= 14; i++ {
		f.event(uint64(0x900+i), 42*time.Minute, 2, fmt.Sprint(i), s("bulk"))
	}
	f.checkpoint(0x90e)
	at4 := f.at(50 * time.Minute)
	reps = both(at4)
	if left := f.pairFiles(onRoot, at4); len(left) != 0 || len(reps[0].Files) != 1 || f.sameFile(f.tableFile(onRoot, at1), f.tableFile(onRoot, at4)) {
		t.Fatalf("fourth update: delta files %v files %v; want the table written in full", left, reps[0].Files)
	}
	md4, err := baseline.ReadParquetMetadata(f.tableFile(onRoot, at4))
	if err != nil {
		t.Fatal(err)
	}
	if md4.LSN != 0x90f {
		t.Fatalf("the rewritten file's LSN is %X, want 90F", md4.LSN)
	}
	// Written from the previous file and the pair beside it, with the five
	// changes since: the nine before them, which the pair held, were not
	// read again from the index.
	if reps[0].EventsApplied != 5 {
		t.Fatalf("the rewrite applied %d events; want the 5 since the pair before", reps[0].EventsApplied)
	}

	// And the update after it starts from that file: a pair again.
	f.event(0xa00, 52*time.Minute, 2, "20", s("after the rewrite"))
	f.checkpoint(0xa00)
	at5 := f.at(60 * time.Minute)
	reps = both(at5)
	if left := f.pairFiles(onRoot, at5); len(left) != 2 || reps[0].EventsApplied != 1 || !f.sameFile(f.tableFile(onRoot, at4), f.tableFile(onRoot, at5)) {
		t.Fatalf("fifth update: delta files %v, %d events; want a pair of 1 change beside the rewritten file", left, reps[0].EventsApplied)
	}
	// The older snapshots still read as they did: the rewrite wrote a new
	// file, not through the linked one.
	if got, _ := f.state(onRoot, at3); !sameRows(got, want) {
		t.Fatalf("the third snapshot changed under the rewrite: %v", got)
	}
}

// The file's floor does not move while pairs are written beside it, so past
// the line the daemon draws the table is written in full, switch on or not;
// and the other outputs never get a pair.
func TestReconstructTables_PGTablePairDeclines(t *testing.T) {
	cases := []struct {
		name string
		run  func(f *pgFixture, at time.Time) pgRun
	}{
		{"the bound is before the line", func(f *pgFixture, at time.Time) pgRun {
			return pgRun{at: at, carry: true, chainFloor: f.at(15 * time.Minute)}
		}},
		{"no line", func(f *pgFixture, at time.Time) pgRun { return pgRun{at: at, carry: true} }},
		{"carrying is off", func(f *pgFixture, at time.Time) pgRun { return pgRun{at: at, chainFloor: f.base} }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reconstruct.SetPGTableDeltas(t, true)
			f := newPGFixture(t)
			root := t.TempDir()
			f.baseline(root, f.at(10*time.Minute), 0x500, "", pairRows()...)
			f.other(0x600, 12*time.Minute)
			at1 := f.at(20 * time.Minute)
			if _, _, _, err := f.run(root, pgRun{at: at1, carry: true}); err != nil {
				t.Fatal(err)
			}
			f.event(0x700, 22*time.Minute, 2, "1", s("late"))
			f.checkpoint(0x700)
			at2 := f.at(30 * time.Minute)
			got, md, reps, err := f.run(root, c.run(f, at2))
			if err != nil {
				t.Fatal(err)
			}
			if left := f.pairFiles(root, at2); len(left) != 0 || len(reps[0].Files) != 1 || md.LSN != 0x701 || got["1"] != "late" || f.sameFile(f.tableFile(root, at1), f.tableFile(root, at2)) {
				t.Fatalf("delta files %v files %v LSN %X row %q; want the table written in full to 701", left, reps[0].Files, md.LSN, got["1"])
			}
		})
	}
}

// A table whose file has a delta beside it, and no change in the window, is
// carried with its delta. The window can be empty because the changes the
// delta holds are gone from the index: carried alone, the file would be the
// table as it was before them.
func TestReconstructTables_PGTablePairIsCarriedWithItsFile(t *testing.T) {
	for _, on := range []bool{true, false} {
		t.Run(fmt.Sprintf("switch on at the carry: %v", on), func(t *testing.T) {
			f := newPGFixture(t)
			root := t.TempDir()
			f.baseline(root, f.at(10*time.Minute), 0x500, "", pairRows()...)
			f.other(0x600, 12*time.Minute)
			at1 := f.at(20 * time.Minute)
			if _, _, _, err := f.run(root, pgRun{at: at1, carry: true}); err != nil {
				t.Fatal(err)
			}
			f.event(0x700, 22*time.Minute, 2, "1", s("late"))
			f.event(0x710, 23*time.Minute, 3, "2", nil)
			f.checkpoint(0x710)
			at2 := f.at(30 * time.Minute)
			reconstruct.SetPGTableDeltas(t, true)
			if _, _, _, err := f.run(root, pgRun{at: at2, carry: true, chainFloor: f.base}); err != nil {
				t.Fatal(err)
			}
			want, _ := f.state(root, at2)
			if want["1"] != "late" || len(f.pairFiles(root, at2)) != 2 {
				t.Fatalf("the second update wrote no pair: %v, %v", want, f.pairFiles(root, at2))
			}

			// Another table's file beside it, with a backslash in its name.
			if err := os.WriteFile(filepath.Join(root, reconstruct.SnapshotDirName(at2), "public", `a\b.note`), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			// The index loses every event; the switch is whatever it is.
			testutil.MustExec(t, f.db, "DELETE FROM binlog_events")
			reconstruct.SetPGTableDeltas(t, on)
			at3 := f.at(40 * time.Minute)
			_, _, reps, err := f.run(root, pgRun{at: at3, carry: true, chainFloor: f.base})
			if err != nil {
				t.Fatal(err)
			}
			if !reps[0].CarriedForward || len(reps[0].Files) != 3 {
				t.Fatalf("third update: carried %v files %v; want the file and its pair carried", reps[0].CarriedForward, reps[0].Files)
			}
			if got, _ := f.state(root, at3); !sameRows(got, want) {
				t.Fatalf("the carried table reads %v, want %v", got, want)
			}
		})
	}
}

// A pair is continued: an update reads the changes since the pair beside the
// previous file ended, and writes the next pair from that one and them. From
// a full read's file on, which records its bound; across an update that
// found no change and carried the file and its pair as they were; and when
// the pair beside the file cannot be continued, by reading every change
// since the file again, as before. At every step the table reads as a
// rewrite leaves it.
func TestReconstructTables_PGTablePairContinuesThePairBefore(t *testing.T) {
	f := newPGFixture(t)
	onRoot, offRoot := t.TempDir(), t.TempDir()
	taken := f.at(10 * time.Minute)
	// 64 rows: the seven keys changed below stay a small share of the file.
	rows := pairRows()
	for i := 100; i < 140; i++ {
		rows = append(rows, [2]string{fmt.Sprint(i), "more"})
	}
	for _, root := range []string{onRoot, offRoot} {
		f.baseline(root, taken, 0x500, taken.Add(-time.Minute).Format(time.RFC3339), rows...)
	}
	f.other(0x600, 12*time.Minute)
	both := func(at time.Time) *reconstruct.TableReport {
		t.Helper()
		reconstruct.SetPGTableDeltas(t, false)
		want, _, _, err := f.run(offRoot, pgRun{at: at, carry: true, chainFloor: f.base})
		if err != nil {
			t.Fatal(err)
		}
		reconstruct.SetPGTableDeltas(t, true)
		_, _, reps, err := f.run(onRoot, pgRun{at: at, carry: true, chainFloor: f.base})
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := f.state(onRoot, at); !sameRows(got, want) {
			t.Fatalf("at %s the table reads %v, and a rewrite gives %v", at.Format("15:04"), got, want)
		}
		if !f.sameFile(f.tableFile(onRoot, taken), f.tableFile(onRoot, at)) {
			t.Fatalf("at %s the table file is not the full read's own", at.Format("15:04"))
		}
		return reps[0]
	}

	// The first update after the full read: a pair, not the table in full.
	f.event(0x700, 12*time.Minute, 2, "1", s("one"))
	f.event(0x710, 13*time.Minute, 3, "2", nil)
	f.event(0x720, 14*time.Minute, 1, "40", s("forty"))
	// Two keys no later window names: the pair after this one has the row
	// of 5 and the dead row of 6 from this pair alone.
	f.event(0x730, 15*time.Minute, 2, "5", s("five"))
	f.event(0x740, 16*time.Minute, 3, "6", nil)
	f.checkpoint(0x740)
	at1 := f.at(20 * time.Minute)
	if r := both(at1); len(f.pairFiles(onRoot, at1)) != 2 || r.EventsApplied != 5 {
		t.Fatalf("first update: delta files %v, %d events; want a pair from 5", f.pairFiles(onRoot, at1), r.EventsApplied)
	}

	// No change: the file and its pair, as they are.
	at2 := f.at(30 * time.Minute)
	if r := both(at2); !r.CarriedForward || len(r.Files) != 3 {
		t.Fatalf("second update: carried %v files %v; want the file and its pair carried", r.CarriedForward, r.Files)
	}

	// Changes again, continued from the carried pair: a key before every
	// key it holds and one after, a key it holds changed and one deleted, a
	// row it took away put back, and a row of the file it never named.
	f.event(0x800, 32*time.Minute, 1, "0", s("zero"))
	f.event(0x810, 33*time.Minute, 1, "9z", s("last"))
	f.event(0x820, 34*time.Minute, 2, "1", s("one again"))
	f.event(0x830, 35*time.Minute, 3, "40", nil)
	f.event(0x840, 36*time.Minute, 1, "2", s("two is back"))
	f.event(0x850, 37*time.Minute, 2, "7", s("seven"))
	f.checkpoint(0x850)
	at3 := f.at(40 * time.Minute)
	r := both(at3)
	_, footer := f.state(onRoot, at3)
	if len(f.pairFiles(onRoot, at3)) != 2 || r.EventsApplied != 6 || footer["bintrail.pgdelta_from"] == "" {
		t.Fatalf("third update: delta files %v, %d events, continues %q; want a pair from the 6 changes since the pair before",
			f.pairFiles(onRoot, at3), r.EventsApplied, footer["bintrail.pgdelta_from"])
	}
	// 64 rows in the file; 1, 2, 5, 6 and 7 are taken away; 0, 1, 2, 7 and
	// 9z are the window's rows, and 5 is kept from the pair before. Without
	// the rows that pair took away, 5 and 6 would be read from the file too.
	if r.BaselineRows != 60 || r.RowsWritten != 65 || r.InsertsEmitted+r.UpdatesApplied != 5 {
		t.Fatalf("third update's report: %d rows left as they were, %d rows in all, %d written by the window; want 60, 65 and 5",
			r.BaselineRows, r.RowsWritten, r.InsertsEmitted+r.UpdatesApplied)
	}
	if got, _ := f.state(onRoot, at3); got["5"] != "five" || got["6"] != "" || len(got) != 65 {
		t.Fatalf("third update: 5 reads %q, 6 reads %q, %d rows; want five, no row, 65", got["5"], got["6"], len(got))
	}

	// The pair beside the file is not whole: every change since the file is
	// read again, and the table is the same.
	dead, _, err := pgdelta.Paths(f.tableFile(onRoot, at3), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dead); err != nil {
		t.Fatal(err)
	}
	f.event(0x900, 42*time.Minute, 2, "8", s("eight"))
	f.checkpoint(0x900)
	at4 := f.at(50 * time.Minute)
	r = both(at4)
	_, footer = f.state(onRoot, at4)
	if len(f.pairFiles(onRoot, at4)) != 2 || r.EventsApplied != 12 || footer["bintrail.pgdelta_from"] != "" {
		t.Fatalf("fourth update: delta files %v, %d events, continues %q; want a pair from all 12 changes since the file, continuing none",
			f.pairFiles(onRoot, at4), r.EventsApplied, footer["bintrail.pgdelta_from"])
	}
}
