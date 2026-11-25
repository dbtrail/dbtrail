//go:build integration

package reconstruct_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// runCut3084 reads the run cut of root's snapshot at at; found false when the
// folder has none.
func (f *pgFixture) runCut3084(root string, at time.Time) (reconstruct.PGRunCut, bool) {
	f.t.Helper()
	cut, found, err := reconstruct.ReadPGRunCut(filepath.Join(root, reconstruct.SnapshotDirName(at)))
	if err != nil {
		f.t.Fatal(err)
	}
	return cut, found
}

func (f *pgFixture) newestID3084() uint64 {
	f.t.Helper()
	var id uint64
	if err := f.db.QueryRow("SELECT MAX(event_id) FROM binlog_events").Scan(&id); err != nil {
		f.t.Fatal(err)
	}
	return id
}

// #3084: each update records where it ended and the tables it brought there,
// a carried table among them; an update whose newest transaction is not
// complete records no event id.
func TestReconstructTables_PGRunCut_3084(t *testing.T) {
	reconstruct.SetPGTableDeltas(t, false)
	f := newPGFixture(t)
	root := t.TempDir()
	f.baseline(root, f.at(10*time.Minute), 0x500, "", [2]string{"1", "paid"})
	// The full read's folder: written by no update, it has none.
	if _, found := f.runCut3084(root, f.at(10*time.Minute)); found {
		t.Fatal("a full read's folder has a run cut")
	}
	f.other(0x600, 12*time.Minute)

	check := func(when string, at time.Time, carried bool, wantLSN, wantReach uint64) {
		t.Helper()
		_, md, reps, err := f.run(root, pgRun{at: at, carry: true, chainFloor: f.base})
		if err != nil {
			t.Fatal(err)
		}
		if reps[0].CarriedForward != carried {
			t.Fatalf("%s: carried %v, want %v", when, reps[0].CarriedForward, carried)
		}
		cut, found := f.runCut3084(root, at)
		if !found {
			t.Fatalf("%s: no run cut", when)
		}
		if cut.LSN != wantLSN || cut.ReachID != wantReach || !cut.At.Equal(at) || !cut.Holds("public", "orders") {
			t.Fatalf("%s: run cut LSN %X reach %d at %s holds orders %v; want LSN %X reach %d at %s, holding orders",
				when, cut.LSN, cut.ReachID, cut.At, cut.Holds("public", "orders"), wantLSN, wantReach, at)
		}
		if !carried && md.LSN != cut.LSN {
			t.Fatalf("%s: the written file's LSN %X is not the run cut's %X", when, md.LSN, cut.LSN)
		}
		if carried && md.LSN >= cut.LSN {
			t.Fatalf("%s: the carried file's LSN %X is not below the run cut's %X", when, md.LSN, cut.LSN)
		}
	}
	check("the first update (written)", f.at(20*time.Minute), false, 0x601, f.newestID3084())
	f.other(0x700, 22*time.Minute)
	check("the second update (carried)", f.at(30*time.Minute), true, 0x701, f.newestID3084())
	// A quiet source: the same end, the same id.
	check("the third update, nothing changed (carried)", f.at(35*time.Minute), true, 0x701, f.newestID3084())
	// A change of another table indexed but not yet saved as a whole
	// transaction: the run ends before it, and names no event id.
	testutil.InsertEvent(t, f.db, lsnString(0x800), 0x800, 0x800, f.at(36*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"public", "items", 1, "2", nil, nil, []byte(`{"id":"2"}`))
	check("the fourth update, the newest transaction split (carried)", f.at(40*time.Minute), true, 0x800, 0)
}

// A table the update published as an earlier read left it, not brought to
// the run's end, is not in the record.
func TestReconstructTables_PGRunCut_heldFullReadLeftOut_3084(t *testing.T) {
	reconstruct.SetPGTableDeltas(t, false)
	f := newPGFixture(t)
	f.anchor = 0x800
	root := t.TempDir()
	taken := f.at(10 * time.Minute)
	f.baseline(root, taken, 0x500, taken.Add(-time.Minute).Format(time.RFC3339), [2]string{"1", "second"})
	f.event(0x600, 9*time.Minute+30*time.Second, 2, "1", s("first"))
	f.checkpoint(0x600)
	at := f.at(20 * time.Minute)
	_, _, reps, err := f.run(root, pgRun{at: at, carry: true, chainFloor: f.base})
	if err != nil {
		t.Fatal(err)
	}
	if !reps[0].CarriedForward {
		t.Fatal("the full read's file was not published as it is")
	}
	if cut, found := f.runCut3084(root, at); found && cut.Holds("public", "orders") {
		t.Fatalf("a full read published as it is, short of the run's end, is in the run cut: %+v", cut)
	}
}

// A TRUNCATE in the window refuses the update: nothing is published, and no
// run cut is left.
func TestReconstructTables_PGRunCut_truncateRefusesAndLeavesNone_3084(t *testing.T) {
	f := newPGFixture(t)
	f.event(0x700, 15*time.Minute, 2, "1", s("x"))
	f.checkpoint(0x700)
	root := t.TempDir()
	f.baseline(root, f.at(10*time.Minute), 0x500, "", [2]string{"1", "paid"})
	testutil.MustExec(t, f.db, fmt.Sprintf(`INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES ('%s', '0/650', 1616, 'public', 'orders', 'TRUNCATE TABLE', 'TRUNCATE public.orders')`, f.at(9*time.Minute+30*time.Second).Format("2006-01-02 15:04:05")))
	at := f.at(30 * time.Minute)
	if _, _, _, err := f.run(root, pgRun{at: at}); !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("err = %v, want the destructive-DDL refusal", err)
	}
	if _, err := os.Stat(filepath.Join(root, reconstruct.SnapshotDirName(at), reconstruct.PGRunCutFile)); !os.IsNotExist(err) {
		t.Fatalf("a refused update left a run cut: %v", err)
	}
}

// An index with no change: nothing was folded, and no run cut is written.
func TestReconstructTables_PGRunCut_emptyIndexLeavesNone_3084(t *testing.T) {
	f := newPGFixture(t)
	// Capture started and saved a position, and holds no change.
	f.checkpoint(0x500)
	root := t.TempDir()
	f.baseline(root, f.at(10*time.Minute), 0x500, f.at(9*time.Minute).Format(time.RFC3339), [2]string{"1", "paid"})
	at := f.at(20 * time.Minute)
	if _, _, _, err := f.run(root, pgRun{at: at, carry: true, chainFloor: f.base}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, reconstruct.SnapshotDirName(at), baseline.SuccessMarker)); err != nil {
		t.Fatalf("the update was not published: %v", err)
	}
	if _, found := f.runCut3084(root, at); found {
		t.Fatal("an update over an index with no change wrote a run cut")
	}
}
