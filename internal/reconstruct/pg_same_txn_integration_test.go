//go:build integration

package reconstruct_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The one-transaction rule through the real fold, a real index and the
// Parquet writer: the changes one transaction makes to one key must describe
// one row, each starting from the row the one before it left.

// change writes one row of the transaction committed at lsn with explicit
// images (nil = none), stored under key 1 as capture stores it.
func (f *pgFixture) change(lsn uint64, d time.Duration, typ uint8, before, after map[string]string) {
	f.t.Helper()
	var b, a []byte
	if before != nil {
		b, _ = json.Marshal(before)
	}
	if after != nil {
		a, _ = json.Marshal(after)
	}
	testutil.InsertEvent(f.t, f.db, lsnString(lsn), lsn, lsn, f.at(d).Format("2006-01-02 15:04:05"), nil,
		"public", "orders", typ, "1", nil, b, a)
}

func pgOrder(status string) map[string]string { return map[string]string{"id": "1", "status": status} }

// A deferrable key: a new row 1 is inserted while the old one is there, and
// the OLD one is deleted, in one transaction. The source keeps the new row;
// keeping the last change would lose it. Refused.
func TestReconstructTables_PGSameTransaction_newRowInsertedOldRowDeleted(t *testing.T) {
	f := newPGFixture(t)
	snap := f.at(10 * time.Minute)
	f.change(0x700, 15*time.Minute, 1, nil, pgOrder("new"))
	f.change(0x700, 15*time.Minute, 3, pgOrder("old"), nil)
	f.checkpoint(0x700)

	root := t.TempDir()
	f.baseline(root, snap, 0x500, "", [2]string{"1", "old"}, [2]string{"5", "keep"})
	got, _, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute)})
	if err == nil {
		t.Fatalf("folded to %v; the source holds row 1 = new", got)
	}
	for _, want := range []string{"public.orders", "inside one transaction (commit LSN 0/700)", `primary key "1"`, "do not describe one row",
		"not the row the change before it left", "DEFERRABLE", "bintrail-pg baseline"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
}

// The ordinary transactions fold: a row inserted and deleted again, and in
// the next transaction inserted and updated.
func TestReconstructTables_PGSameTransaction_ordinary(t *testing.T) {
	f := newPGFixture(t)
	snap := f.at(10 * time.Minute)
	f.change(0x700, 15*time.Minute, 3, pgOrder("old"), nil)
	f.change(0x700, 15*time.Minute, 1, nil, pgOrder("new"))
	f.change(0x700, 15*time.Minute, 3, pgOrder("new"), nil)
	f.checkpoint(0x700)

	root := t.TempDir()
	f.baseline(root, snap, 0x500, "", [2]string{"1", "old"}, [2]string{"5", "keep"})
	got, _, _, err := f.run(root, pgRun{at: f.at(20 * time.Minute)})
	if err != nil {
		t.Fatalf("an ordinary transaction was refused: %v", err)
	}
	if want := map[string]string{"5": "keep"}; !sameRows(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}

	f.change(0x800, 25*time.Minute, 1, nil, pgOrder("again"))
	f.change(0x800, 25*time.Minute, 2, pgOrder("again"), pgOrder("changed"))
	f.checkpoint(0x800)
	got, _, _, err = f.run(root, pgRun{at: f.at(40 * time.Minute)})
	if err != nil {
		t.Fatalf("an ordinary transaction was refused: %v", err)
	}
	if want := map[string]string{"1": "changed", "5": "keep"}; !sameRows(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
}
