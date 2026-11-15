//go:build integration

package reconstruct_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2760 through the real fold, a real index and the Parquet writer: a
// key-changing UPDATE of a PostgreSQL table is applied as a delete of the old
// key and an insert of the new one, each ordered by commit LSN against every
// other change of its key.

// move writes a key-changing UPDATE of the transaction committed at lsn: the
// row keyed from becomes the row keyed to, with status. Stored as capture
// stores it, under the before-image's key.
func (f *pgFixture) move(lsn uint64, d time.Duration, from, to, status string) {
	f.t.Helper()
	before, _ := json.Marshal(map[string]string{"id": from, "status": "x"})
	after, _ := json.Marshal(map[string]string{"id": to, "status": status})
	testutil.InsertEvent(f.t, f.db, lsnString(lsn), lsn, lsn, f.at(d).Format("2006-01-02 15:04:05"), nil,
		"public", "orders", 2, from, nil, before, after)
}

func TestReconstructTables_PGKeyChange_2760(t *testing.T) {
	f := newPGFixture(t)
	snap := f.at(10 * time.Minute)
	// Raced the snapshot: at or after its floor, before its time, missing
	// from it. Row 1 moves to key 7.
	f.move(0x600, 9*time.Minute+30*time.Second, "1", "7", "moved")
	// One transaction: row 2 is deleted and row 5 takes its key. Both rows
	// carry the same LSN; the index numbers them in this order.
	f.event(0x700, 15*time.Minute, 3, "2", nil)
	f.move(0x700, 15*time.Minute, "5", "2", "took its place")
	// Row 7 moves back to key 1, and the row is then changed under key 1 by
	// a transaction that committed LATER (higher LSN) with an EARLIER commit
	// time, so it reaches the fold first. The LSN decides: "final".
	f.move(0x800, 20*time.Minute, "7", "1", "back")
	f.event(0x820, 19*time.Minute, 2, "1", s("final"))
	// Past the point: left to the next fold.
	f.move(0x900, 40*time.Minute, "6", "9", "nine")
	f.checkpoint(0x900)

	root := t.TempDir()
	f.baseline(root, snap, 0x500, "", [2]string{"1", "paid"}, [2]string{"2", "open"}, [2]string{"5", "keep"}, [2]string{"6", "six"})

	at1 := f.at(30 * time.Minute)
	got, md, reps, err := f.run(root, pgRun{at: at1})
	if err != nil {
		t.Fatalf("fold to %s: %v", at1, err)
	}
	if want := map[string]string{"1": "final", "2": "took its place", "6": "six"}; !sameRows(got, want) {
		t.Fatalf("rows at %s = %v, want %v", at1, got, want)
	}
	if md.LSN != 0x900 {
		t.Fatalf("footer LSN %X, want 900 (right before the first change past the point)", md.LSN)
	}
	if len(reps) != 1 || reps[0].EventsApplied != 5 {
		t.Errorf("the report counts %+v, want 5 events: a key change is one event", reps)
	}

	// The next fold, from the folded snapshot: the move it left.
	at2 := f.at(50 * time.Minute)
	got, _, _, err = f.run(root, pgRun{at: at2})
	if err != nil {
		t.Fatalf("fold to %s: %v", at2, err)
	}
	if want := map[string]string{"1": "final", "2": "took its place", "9": "nine"}; !sameRows(got, want) {
		t.Fatalf("rows at %s = %v, want %v", at2, got, want)
	}
}

// A key change whose old row was not captured whole keeps the refusal, with
// the reason, and publishes nothing.
func TestReconstructTables_PGKeyChange_refusedWhenNotWhole_2760(t *testing.T) {
	f := newPGFixture(t)
	snap := f.at(10 * time.Minute)
	testutil.InsertEvent(f.t, f.db, lsnString(0x700), 0x700, 0x700, f.at(15*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"public", "orders", 2, "1", nil, []byte(`{"id":"1"}`), []byte(`{"id":"7","status":"moved"}`))
	// A later change that overwrites both keys: the refusal is per event.
	f.event(0x800, 20*time.Minute, 3, "1", nil)
	f.event(0x810, 21*time.Minute, 3, "7", nil)
	f.checkpoint(0x810)

	root := t.TempDir()
	f.baseline(root, snap, 0x500, "", [2]string{"1", "paid"})
	_, _, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute)})
	if err == nil {
		t.Fatal("a key change with a key-only before-image was folded")
	}
	for _, want := range []string{"PK-changing UPDATE", "public.orders", `"1"`, `"7"`, "On PostgreSQL", "not captured whole", "bintrail-pg baseline"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
}
