package verify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A server that deletes its local snapshots by age keeps a few hours of them
// on disk and all of them in its S3 destination. Once the last full read is
// older than the local window, neither side of a table's pair is on disk any
// more. FindBaselinePairWith looks for them in the second location. The
// second location here is another local folder: the lookup is
// reconstruct.FindBaselineFile, which serves both.

func lrFindWith(t *testing.T, root, other string) []BaselinePair {
	t.Helper()
	pairs, _, err := FindBaselinePairWith(context.Background(), root, other)
	if err != nil {
		t.Fatalf("FindBaselinePairWith: %v", err)
	}
	return pairs
}

func wantUnder(t *testing.T, what, path, root string) {
	t.Helper()
	if !strings.HasPrefix(path, root+string(filepath.Separator)) {
		t.Fatalf("%s is %s, want a file under %s", what, path, root)
	}
}

// lrLocalFolds is the local folder of such a server: only snapshots built
// from the recorded changes, all inheriting the read at lr1.
func lrLocalFolds(t *testing.T, root, table string) {
	t.Helper()
	lrWrite(t, root, lr2, table, lrFold(lr2, lr1, lr1, 1, 300))
	lrWrite(t, root, lr3, table, lrFold(lr3, lr2, lr1, 2, 400))
}

// The read and the snapshot before it are both only in the second location.
func TestLastReadElsewhere_readAndPreviousAreBothThere(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrLocalFolds(t, root, "orders")
	lrWrite(t, other, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, other, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, other, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	p := lrOne(t, lrFindWith(t, root, other), "orders")
	wantCompared(t, p, lr0, lr1, 200)
	wantUnder(t, "the read", p.NewPath, other)
	wantUnder(t, "the snapshot before it", p.PrevPath, other)
}

// The read is on disk, the snapshot before it only in the second location.
func TestLastReadElsewhere_onlyThePreviousIsThere(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	lrWrite(t, other, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, other, lr1, "orders", lrDump(lr1, 200))
	p := lrOne(t, lrFindWith(t, root, other), "orders")
	wantCompared(t, p, lr0, lr1, 200)
	wantUnder(t, "the read", p.NewPath, root)
	wantUnder(t, "the snapshot before it", p.PrevPath, other)
}

// The newest snapshot is itself the read, and the only one of the table on
// disk: its previous comes from the second location, never the read's own
// copy there.
func TestLastReadElsewhere_previousIsNeverTheReadItself(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrWrite(t, root, lr0, "items", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, root, lr1, "items", lrDump(lr1, 200))
	lrWrite(t, other, lr1, "orders", lrDump(lr1, 200))
	wantSettled(t, lrOne(t, lrFindWith(t, root, other), "orders"), StatusInconclusive, "no earlier snapshot", "on this machine or in "+other)
}

// What is on disk is used before the second location is asked.
func TestLastReadElsewhere_diskComesFirst(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	lrWrite(t, other, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, other, lr1, "orders", lrDump(lr1, 200))
	p := lrOne(t, lrFindWith(t, root, other), "orders")
	wantCompared(t, p, lr0, lr1, 200)
	wantUnder(t, "the read", p.NewPath, root)
	wantUnder(t, "the snapshot before it", p.PrevPath, root)
}

// The read is in neither place. An OLDER snapshot in the second location is
// not the read and must not be taken for it.
func TestLastReadElsewhere_readIsInNeitherPlace(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrLocalFolds(t, root, "orders")
	lrWrite(t, other, lr0, "orders", lrDump(lr0, 100))
	wantSettled(t, lrOne(t, lrFindWith(t, root, other), "orders"), StatusInconclusive,
		lr1.Format(time.RFC3339), "no longer kept on this machine or in "+other, "is not a read of the database itself", "full read of the database")
}

// No second location: the answer names no place, and says what is kept.
func TestLastReadElsewhere_noSecondLocation(t *testing.T) {
	root := t.TempDir()
	lrLocalFolds(t, root, "orders")
	p := lrOne(t, lrFindWith(t, root, ""), "orders")
	wantSettled(t, p, StatusInconclusive, lr1.Format(time.RFC3339), "no longer kept. ", "is not a read of the database itself", "full read of the database")
	if strings.Contains(p.Settled.Detail, "on this machine") {
		t.Fatalf("detail names a second place when there is none: %q", p.Settled.Detail)
	}
}

// The same folder given twice is one location.
func TestLastReadElsewhere_sameLocationTwice(t *testing.T) {
	root := t.TempDir()
	lrLocalFolds(t, root, "orders")
	p := lrOne(t, lrFindWith(t, root, root), "orders")
	wantSettled(t, p, StatusInconclusive, "no longer kept. ")
}

// The read in the second location is half written (#467): not a snapshot.
func TestLastReadElsewhere_incompleteReadIsNotUsed(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrLocalFolds(t, root, "orders")
	lrWrite(t, other, lr0, "orders", lrDump(lr0, 100))
	path := lrWrite(t, other, lr1, "orders", lrDump(lr1, 200))
	dir := filepath.Dir(filepath.Dir(path))
	if err := os.Remove(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	wantSettled(t, lrOne(t, lrFindWith(t, root, other), "orders"), StatusInconclusive, "no longer kept on this machine or in "+other)
}

// Two tables read at different instants: each gets its own read.
func TestLastReadElsewhere_perTableReads(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	lrWrite(t, root, lr3, "orders", lrFold(lr3, lr2, lr1, 2, 400))
	lrWrite(t, root, lr2, "items", lrDump(lr2, 300))
	lrWrite(t, root, lr3, "items", lrFold(lr3, lr2, lr2, 1, 400))
	lrWrite(t, other, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, other, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, other, lr1, "items", lrDump(lr1, 200))
	pairs := lrFindWith(t, root, other)
	wantCompared(t, lrOne(t, pairs, "orders"), lr0, lr1, 200)
	items := lrOne(t, pairs, "items")
	wantCompared(t, items, lr1, lr2, 300)
	wantUnder(t, "the read of items", items.NewPath, root)
	wantUnder(t, "the snapshot before it", items.PrevPath, other)
}

// The snapshot before the read keeps its changes beside it (#1638): the
// fetch is bounded from the chain's start, as it is for one on disk.
func TestLastReadElsewhere_previousIsAChain(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrWrite(t, root, lr3, "orders", lrDump(lr3, 400))
	lrWrite(t, root, lr3, "items", lrDump(lr3, 400))
	lrWrite(t, root, lr2, "items", lrDump(lr2, 300))
	base := lrWrite(t, other, lr0, "orders", lrDump(lr0, 100))
	lrStartChain(t, base, lr0, 100)
	lrCarry(t, other, lr0, lr1, "orders", 0)
	p := lrOne(t, lrFindWith(t, root, other), "orders")
	wantCompared(t, p, lr0, lr3, 400)
	if !p.PrevDir.Equal(lr1) {
		t.Fatalf("previous snapshot folder is %s, want %s", p.PrevDir, lr1)
	}
}

// The second location cannot be searched: that table is an error, never
// "not kept", and the error names the place.
func TestLastReadElsewhere_aFailedLookupIsAnError(t *testing.T) {
	root := t.TempDir()
	lrLocalFolds(t, root, "orders")
	other := filepath.Join(t.TempDir(), "not-a-folder")
	if err := os.WriteFile(other, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	wantSettled(t, lrOne(t, lrFindWith(t, root, other), "orders"), StatusError, "looking for it in "+other+" failed")
}

// One snapshot on disk (a folder that keeps less than one schedule interval)
// and the rest in the second location: the tables are still paired.
func TestLastReadElsewhere_oneSnapshotOnDisk(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrWrite(t, root, lr3, "orders", lrFold(lr3, lr2, lr1, 2, 400))
	lrWrite(t, other, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, other, lr1, "orders", lrDump(lr1, 200))
	wantCompared(t, lrOne(t, lrFindWith(t, root, other), "orders"), lr0, lr1, 200)
}

// One snapshot on disk whose read is in neither place: said, not "nothing
// to verify yet".
func TestLastReadElsewhere_oneSnapshotOnDiskReadNowhere(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrWrite(t, root, lr3, "orders", lrFold(lr3, lr2, lr1, 2, 400))
	wantSettled(t, lrOne(t, lrFindWith(t, root, other), "orders"), StatusInconclusive, "no longer kept on this machine or in "+other)
}

// The first snapshot a server ever took, in both places: still nothing to
// verify yet, as without a second location.
func TestLastReadElsewhere_firstSnapshotEver(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, other, lr0, "orders", lrDump(lr0, 100))
	if pairs := lrFindWith(t, root, other); pairs != nil {
		t.Fatalf("pairs = %+v, want none: there is nothing to compare yet", pairs)
	}
}

// The snapshot before the read came from the second location; the local
// folder of that same instant cannot be read. The second location already
// answered for it, so the table is compared.
func TestLastReadElsewhere_unreadableLocalFolderOfThePrevious(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads every folder")
	}
	root, other := t.TempDir(), t.TempDir()
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	lrWrite(t, other, lr0, "orders", lrDump(lr0, 100))
	dir := filepath.Join(root, reconstruct.SnapshotDirName(lr0))
	if err := os.Mkdir(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	wantCompared(t, lrOne(t, lrFindWith(t, root, other), "orders"), lr0, lr1, 200)
}

// One snapshot on disk, built from the changes; its read is in the second
// location and is the oldest there. That is the table's answer, not "nothing
// to verify yet": the server has more than one snapshot.
func TestLastReadElsewhere_oneSnapshotOnDiskReadIsTheOldestElsewhere(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	lrWrite(t, root, lr3, "orders", lrFold(lr3, lr2, lr1, 2, 400))
	lrWrite(t, other, lr1, "orders", lrDump(lr1, 200))
	wantSettled(t, lrOne(t, lrFindWith(t, root, other), "orders"), StatusInconclusive, lr1.Format(time.RFC3339), "no earlier snapshot")
}
