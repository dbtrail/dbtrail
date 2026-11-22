//go:build integration

package reconstruct_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// other writes a change of another table at lsn: it moves the run's end and
// the newest change the index holds, and is no change of orders.
func (f *pgFixture) other(lsn uint64, d time.Duration) {
	f.t.Helper()
	testutil.InsertEvent(f.t, f.db, lsnString(lsn), lsn, lsn, f.at(d).Format("2006-01-02 15:04:05"), nil,
		"public", "items", 1, "1", nil, nil, []byte(`{"id":"1"}`))
	f.checkpoint(lsn)
}

func (f *pgFixture) tableFile(root string, at time.Time) string {
	return filepath.Join(root, reconstruct.SnapshotDirName(at), "public", "orders.parquet")
}

func (f *pgFixture) sameFile(a, b string) bool {
	f.t.Helper()
	x, err := os.Stat(a)
	if err != nil {
		f.t.Fatal(err)
	}
	y, err := os.Stat(b)
	if err != nil {
		f.t.Fatal(err)
	}
	return os.SameFile(x, y)
}

// An unchanged PostgreSQL table is carried forward (#2757): the first update
// after a full read rewrites it (the full read's file records no bound), the
// next ones publish that same file, a later change is applied from the
// carried file's floor, and the snapshot's readers are said to start where
// the carried file's footer says.
func TestReconstructTables_PGCarryForwardUnchanged(t *testing.T) {
	// The carry of a table that did not change, and a table that did written
	// in full: with deltas on (the default) the change of the third update
	// would be a pair beside the file (pg_pair_chain_3082_integration_test).
	reconstruct.SetPGTableDeltas(t, false)
	ctx := context.Background()
	f := newPGFixture(t)
	root := t.TempDir()
	rows := [][2]string{{"1", "paid"}, {"5", "keep"}}
	f.baseline(root, f.at(10*time.Minute), 0x500, "", rows...)
	f.other(0x600, 12*time.Minute)

	// The full read's file has no bound of its own: rewritten.
	at1 := f.at(20 * time.Minute)
	got, md1, reps, err := f.run(root, pgRun{at: at1, carry: true})
	if err != nil {
		t.Fatal(err)
	}
	since1 := f.at(11 * time.Minute).Format(time.RFC3339)
	if reps[0].CarriedForward || !reps[0].PG || md1.LSN != 0x601 || md1.PGDeltaSince != since1 || !sameRows(got, map[string]string{"1": "paid", "5": "keep"}) {
		t.Fatalf("first update: carried %v pg %v LSN %X since %q rows %v", reps[0].CarriedForward, reps[0].PG, md1.LSN, md1.PGDeltaSince, got)
	}

	// Nothing of orders changed: the same file, by link, with its footer.
	f.other(0x700, 22*time.Minute)
	at2 := f.at(30 * time.Minute)
	got, md2, reps, err := f.run(root, pgRun{at: at2, carry: true, chainFloor: f.base})
	if err != nil {
		t.Fatal(err)
	}
	if !reps[0].CarriedForward || !reps[0].CarriedByLink || !reps[0].PG || md2.LSN != 0x601 || md2.PGDeltaSince != since1 ||
		!sameRows(got, map[string]string{"1": "paid", "5": "keep"}) {
		t.Fatalf("second update: carried %v linked %v LSN %X since %q rows %v", reps[0].CarriedForward, reps[0].CarriedByLink, md2.LSN, md2.PGDeltaSince, got)
	}
	if !f.sameFile(f.tableFile(root, at1), f.tableFile(root, at2)) {
		t.Fatal("the carried file is not the previous snapshot's file")
	}
	// Its readers start at the carried file's bound, not at the snapshot.
	if from, err := reconstruct.SnapshotReadsFromPG(ctx, root, at2); err != nil || !from.Equal(f.at(11*time.Minute)) {
		t.Fatalf("SnapshotReadsFromPG = %v, %v; want %v", from, err, f.at(11*time.Minute))
	}
	if from, err := reconstruct.SnapshotReadsFrom(ctx, root, at2); err != nil || !from.Equal(at2) {
		t.Fatalf("SnapshotReadsFrom = %v, %v; want the snapshot's time", from, err)
	}

	// A change after the carried snapshot is applied from the file's floor.
	f.event(0x800, 32*time.Minute, 2, "1", s("late"))
	f.checkpoint(0x800)
	at3 := f.at(40 * time.Minute)
	got, md3, reps, err := f.run(root, pgRun{at: at3, carry: true, chainFloor: f.base})
	if err != nil {
		t.Fatal(err)
	}
	if reps[0].CarriedForward || md3.LSN != 0x801 || !sameRows(got, map[string]string{"1": "late", "5": "keep"}) {
		t.Fatalf("third update: carried %v LSN %X rows %v", reps[0].CarriedForward, md3.LSN, got)
	}
}

// A carried file's bound is not left to age past the line the daemon draws:
// the table is rewritten and records the run's bound. On a source with no
// change at all the rewrite would record the same bound, so the file is
// carried still.
func TestReconstructTables_PGCarryForwardEndsAtTheLine(t *testing.T) {
	for _, quiet := range []bool{false, true} {
		name := "the source changed"
		if quiet {
			name = "the source is quiet"
		}
		t.Run(name, func(t *testing.T) {
			f := newPGFixture(t)
			root := t.TempDir()
			f.baseline(root, f.at(10*time.Minute), 0x500, "", [2]string{"1", "paid"})
			f.other(0x600, 12*time.Minute)
			at1 := f.at(20 * time.Minute)
			if _, _, _, err := f.run(root, pgRun{at: at1, carry: true}); err != nil {
				t.Fatal(err)
			}
			if !quiet {
				f.other(0x700, 22*time.Minute)
			}
			// The file's bound (11 minutes) is before the line.
			at2 := f.at(30 * time.Minute)
			got, md, reps, err := f.run(root, pgRun{at: at2, carry: true, chainFloor: f.at(15 * time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			if got["1"] != "paid" {
				t.Fatalf("rows %v", got)
			}
			if quiet {
				if !reps[0].CarriedForward || md.LSN != 0x601 {
					t.Fatalf("quiet source: carried %v LSN %X, want the same file", reps[0].CarriedForward, md.LSN)
				}
				return
			}
			if want := f.at(21 * time.Minute).Format(time.RFC3339); reps[0].CarriedForward || md.LSN != 0x701 || md.PGDeltaSince != want {
				t.Fatalf("carried %v LSN %X since %q, want a rewrite to 701 and %q", reps[0].CarriedForward, md.LSN, md.PGDeltaSince, want)
			}
		})
	}
}
