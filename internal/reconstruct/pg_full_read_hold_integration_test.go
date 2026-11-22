//go:build integration

package reconstruct_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A full read's rows are the source as of a position past the file's floor,
// and an update that ends short of it holds only the older of two changes a
// row had in between. The file holds the newer. Such an update publishes the
// file as it is; once capture is past the position, or the run holds every
// change below it, the changes are laid over the file as always.
func TestReconstructTables_PGUpdateWaitsForCaptureToPassAFullRead(t *testing.T) {
	// What the hold publishes, read from the table file: past the read the
	// changes are written into it in full, which with deltas on (the default)
	// would be a pair beside it instead.
	SetPGTableDeltas(t, false)
	// Row 1 changed at 0x600 ("first") and at 0x700 ("second"); the full
	// read opened at 0x800, so its file holds "second". Its floor is 0x500.
	setup := func(t *testing.T, since string) (*pgFixture, string, time.Time) {
		f := newPGFixture(t)
		f.anchor = 0x800
		root := t.TempDir()
		taken := f.at(10 * time.Minute)
		if since == "recorded" {
			since = taken.Add(-time.Minute).Format(time.RFC3339)
		}
		f.baseline(root, taken, 0x500, since, [2]string{"1", "second"}, [2]string{"2", "two"})
		f.event(0x600, 9*time.Minute+30*time.Second, 2, "1", s("first"))
		f.checkpoint(0x600)
		return f, root, taken
	}
	sentThrough := func(f *pgFixture, lsn uint64) {
		testutil.MustExec(f.t, f.db, fmt.Sprintf("UPDATE stream_state SET pg_sent_through_lsn = %d, pg_sent_through_at = last_checkpoint WHERE id = 1", lsn))
	}

	t.Run("capture short of the read: the file as it is, then the changes", func(t *testing.T) {
		f, root, taken := setup(t, "recorded")
		for name, sent := range map[string]uint64{"no position saved": 0, "a position short of the read": 0x650} {
			if sent != 0 {
				sentThrough(f, sent)
			}
			at := f.at(20 * time.Minute)
			if sent != 0 {
				at = f.at(21 * time.Minute)
			}
			got, _, reps, err := f.run(root, pgRun{at: at, carry: true, chainFloor: f.base})
			if err != nil {
				t.Fatal(err)
			}
			if got["1"] != "second" || !reps[0].CarriedForward || !f.sameFile(f.tableFile(root, taken), f.tableFile(root, at)) {
				t.Fatalf("%s: row 1 reads %q, carried %v; want the full read's file, with \"second\"", name, got["1"], reps[0].CarriedForward)
			}
		}
		// Capture goes on: the second change, and one past the read.
		f.event(0x700, 9*time.Minute+40*time.Second, 2, "1", s("second"))
		f.event(0x900, 25*time.Minute, 2, "1", s("third"))
		f.checkpoint(0x900)
		at := f.at(30 * time.Minute)
		got, md, reps, err := f.run(root, pgRun{at: at, carry: true, chainFloor: f.base})
		if err != nil {
			t.Fatal(err)
		}
		if got["1"] != "third" || reps[0].CarriedForward || reps[0].EventsApplied != 3 || md.LSN != 0x901 {
			t.Fatalf("past the read: row 1 reads %q, carried %v, %d events, next LSN %X; want third, written, 3 events from the file's floor, 901",
				got["1"], reps[0].CarriedForward, reps[0].EventsApplied, md.LSN)
		}
	})

	t.Run("a quiet source: capture says it is past the read, and the run holds every change", func(t *testing.T) {
		f, root, _ := setup(t, "recorded")
		f.event(0x700, 9*time.Minute+40*time.Second, 2, "1", s("second"))
		f.checkpoint(0x700)
		sentThrough(f, 0x800)
		at := f.at(20 * time.Minute)
		got, _, reps, err := f.run(root, pgRun{at: at, carry: true, chainFloor: f.base})
		if err != nil {
			t.Fatal(err)
		}
		if got["1"] != "second" || reps[0].EventsApplied != 2 {
			t.Fatalf("row 1 reads %q from %d events; want second, both changes applied", got["1"], reps[0].EventsApplied)
		}
	})

	// Where the file cannot be published again as it is, the run goes on
	// as it did before this check existed.
	for name, c := range map[string]struct {
		since string
		carry bool
	}{
		"carrying is off":           {"recorded", false},
		"the file records no bound": {"", true},
	} {
		t.Run(name, func(t *testing.T) {
			f, root, _ := setup(t, c.since)
			got, _, reps, err := f.run(root, pgRun{at: f.at(20 * time.Minute), carry: c.carry, chainFloor: f.base})
			if err != nil {
				t.Fatal(err)
			}
			if got["1"] != "first" || reps[0].CarriedForward {
				t.Fatalf("row 1 reads %q, carried %v; want the run to go on as before (first)", got["1"], reps[0].CarriedForward)
			}
		})
	}
}
