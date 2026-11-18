//go:build integration

package reconstruct_test

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A table nothing has written to since capture began has no shape in the
// index: the stream describes a table when it first carries a change of it.
// An update must still publish it, from the shape its own file records. It
// used to refuse the table ("not found in any schema snapshot"), and one
// refused table makes the whole update publish nothing and a full read of
// the source follow, at every update: any database with a lookup table.
func TestReconstructTables_PGTableWithNoShapeInTheIndex(t *testing.T) {
	f := newPGFixture(t)
	// The fixture records orders' shape; this table never had a change.
	testutil.MustExec(t, f.db, "DELETE FROM schema_snapshots WHERE table_name = 'orders'")
	root := t.TempDir()
	rows := [][2]string{{"1", "paid"}, {"5", "keep"}}
	// A full read's file: no bound of its own, so the first update rewrites it.
	f.baseline(root, f.at(10*time.Minute), 0x500, "", rows...)
	f.other(0x600, 12*time.Minute)

	at1 := f.at(20 * time.Minute)
	got, md1, reps, err := f.run(root, pgRun{at: at1, carry: true})
	if err != nil {
		t.Fatalf("the first update of a table with no shape in the index: %v", err)
	}
	if reps[0].CarriedForward || !reps[0].PG || md1.LSN != 0x601 || md1.PGDeltaSince == "" || !sameRows(got, map[string]string{"1": "paid", "5": "keep"}) {
		t.Fatalf("first update: carried %v pg %v LSN %X since %q rows %v; want the table written again with its rows and a bound",
			reps[0].CarriedForward, reps[0].PG, md1.LSN, md1.PGDeltaSince, got)
	}

	// And from then on carried, like any table with no change.
	f.other(0x700, 22*time.Minute)
	at2 := f.at(30 * time.Minute)
	got, _, reps, err = f.run(root, pgRun{at: at2, carry: true, chainFloor: f.base})
	if err != nil {
		t.Fatalf("the second update: %v", err)
	}
	if !reps[0].CarriedForward || !sameRows(got, map[string]string{"1": "paid", "5": "keep"}) {
		t.Fatalf("second update: carried %v rows %v; want the previous file", reps[0].CarriedForward, got)
	}
	if !f.sameFile(f.tableFile(root, at1), f.tableFile(root, at2)) {
		t.Fatal("the carried file is not the previous snapshot's file")
	}
}

// A change of a table the index has no shape of is refused as before: it was
// captured under a shape the file does not say.
func TestReconstructTables_PGTableWithNoShapeButAChange(t *testing.T) {
	f := newPGFixture(t)
	testutil.MustExec(t, f.db, "DELETE FROM schema_snapshots WHERE table_name = 'orders'")
	root := t.TempDir()
	f.baseline(root, f.at(10*time.Minute), 0x500, "", [2]string{"1", "paid"})
	f.event(0x600, 12*time.Minute, 2, "1", s("late"))
	f.checkpoint(0x600)
	_, _, _, err := f.run(root, pgRun{at: f.at(20 * time.Minute), carry: true})
	if err == nil || !strings.Contains(err.Error(), "not found in") {
		t.Fatalf("an update over a change with no shape: %v; want the refusal", err)
	}
}
