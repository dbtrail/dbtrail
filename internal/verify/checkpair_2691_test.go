package verify

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The cleanup of a folder with a count and no S3 keeps the pair a check
// compares (#2691). baseline.CheckPair works the pair out on its own (the
// baseline package cannot import this one), so this pins it to the pair
// FindBaselinePair picks, and shows the check still finds it after the
// cleanup has applied a count of 3.
func TestCheckPair_isThePairTheCheckCompares(t *testing.T) {
	root := t.TempDir()
	at := func(h int) time.Time { return lr0.Add(time.Duration(h) * time.Hour) }
	lrWrite(t, root, at(0), "orders", lrDump(at(0), 100))
	lrWrite(t, root, at(1), "orders", lrFold(at(1), at(0), at(0), 1, 110))
	lrWrite(t, root, at(2), "orders", lrDump(at(2), 120)) // the last full read
	for h := 3; h <= 5; h++ {
		lrWrite(t, root, at(h), "orders", lrFold(at(h), at(h-1), at(2), h-2, int64(100+10*h)))
	}
	name := func(h int) string { return reconstruct.SnapshotDirName(at(h)) }
	read, before := baseline.CheckPair(root)
	if read != name(2) || before != name(1) {
		t.Fatalf("CheckPair = %s, %s; want the full read %s and the snapshot before it %s", read, before, name(2), name(1))
	}
	pairOf := func() (string, string) {
		t.Helper()
		pairs, _, err := FindBaselinePair(context.Background(), root)
		if err != nil || len(pairs) != 1 || pairs[0].Settled != nil {
			t.Fatalf("FindBaselinePair: %+v, %v; want one pair to compare", pairs, err)
		}
		return filepath.Base(filepath.Dir(filepath.Dir(pairs[0].NewPath))), filepath.Base(filepath.Dir(filepath.Dir(pairs[0].PrevPath)))
	}
	if r, b := pairOf(); r != read || b != before {
		t.Fatalf("the check compares %s with %s, CheckPair names %s and %s", r, b, read, before)
	}
	res, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: root, KeepNewest: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Pruned, []string{name(0)}) {
		t.Fatalf("a count of 3 removed %v, want only %s: the pair the check compares is kept on top of the count", res.Pruned, name(0))
	}
	if r, b := pairOf(); r != name(2) || b != name(1) {
		t.Fatalf("after the cleanup the check compares %s with %s", r, b)
	}
	// The next full read: the old pair is no longer needed and goes.
	lrWrite(t, root, at(6), "orders", lrDump(at(6), 160))
	if read, before := baseline.CheckPair(root); read != name(6) || before != name(5) {
		t.Fatalf("after a new full read CheckPair = %s, %s; want %s, %s", read, before, name(6), name(5))
	}
	res, err = baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: root, KeepNewest: 3})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, []string{name(1), name(2), name(3)}) {
		t.Fatalf("after a new full read the cleanup removed %v, want the old pair and the snapshot past the count", res.Pruned)
	}
}

// Nothing to name: no folder, an S3 location, a newest copy that records no
// read, a read whose snapshot is gone.
func TestCheckPair_nothingToKeep(t *testing.T) {
	for _, dir := range []string{"", "s3://bkt/snaps"} {
		if r, b := baseline.CheckPair(dir); r != "" || b != "" {
			t.Errorf("CheckPair(%q) = %q, %q", dir, r, b)
		}
	}
	root := t.TempDir()
	lrWrite(t, root, lr1, "orders", lrFold(lr1, lr0, time.Time{}, 0, 100))
	if r, b := baseline.CheckPair(root); r != "" || b != "" {
		t.Errorf("a newest copy that records no read: %q, %q", r, b)
	}
	root = t.TempDir()
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 100))
	if r, b := baseline.CheckPair(root); r != "" || b != "" {
		t.Errorf("a read whose snapshot is gone: %q, %q", r, b)
	}
	// Right after a full read with nothing before it: the read alone.
	root = t.TempDir()
	lrWrite(t, root, lr3, "orders", lrDump(lr3, 100))
	if r, b := baseline.CheckPair(root); r != reconstruct.SnapshotDirName(lr3) || b != "" {
		t.Errorf("a lone full read: %q, %q", r, b)
	}
}

// The pair kept is the read most tables of the newest snapshot come from,
// not the first table's (#2691 review). accounts, first by name, is read
// again on its own after the full read both tables shared: the full read's
// pair is kept, orders keeps its check, and accounts' own re-read pair is
// not kept.
func TestCheckPair_theReadMostTablesShare(t *testing.T) {
	root := t.TempDir()
	at := func(h int) time.Time { return lr0.Add(time.Duration(h) * time.Hour) }
	tables := []string{"accounts", "orders", "payments"}
	for _, tb := range tables {
		lrWrite(t, root, at(0), tb, lrDump(at(0), 100))
		lrWrite(t, root, at(1), tb, lrFold(at(1), at(0), at(0), 1, 110))
		lrWrite(t, root, at(2), tb, lrDump(at(2), 120)) // the full read
		lrWrite(t, root, at(3), tb, lrFold(at(3), at(2), at(2), 1, 130))
	}
	lrWrite(t, root, at(4), "accounts", lrDump(at(4), 140)) // read again on its own
	for _, tb := range tables[1:] {
		lrWrite(t, root, at(4), tb, lrFold(at(4), at(3), at(2), 2, 140))
	}
	for h := 5; h <= 6; h++ {
		lrWrite(t, root, at(h), "accounts", lrFold(at(h), at(h-1), at(4), h-4, int64(100+10*h)))
		for _, tb := range tables[1:] {
			lrWrite(t, root, at(h), tb, lrFold(at(h), at(h-1), at(2), h-2, int64(100+10*h)))
		}
	}
	name := func(h int) string { return reconstruct.SnapshotDirName(at(h)) }
	if r, b := baseline.CheckPair(root); r != name(2) || b != name(1) {
		t.Fatalf("CheckPair = %s, %s; want the full read most tables share, %s, and %s", r, b, name(2), name(1))
	}
	if _, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: root, KeepNewest: 3}); err != nil {
		t.Fatal(err)
	}
	pairs, _, err := FindBaselinePair(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	for _, tb := range tables[1:] {
		if p := lrOne(t, pairs, tb); p.Settled != nil {
			t.Fatalf("%s, read by the full read, lost its pair after the cleanup: %s", tb, p.Settled.Detail)
		}
	}
	if p := lrOne(t, pairs, "accounts"); p.Settled == nil {
		t.Logf("accounts compares %s with %s", p.PrevSnapshot, p.NewSnapshot)
	} else {
		t.Logf("accounts, read on its own since, as documented: %s", p.Settled.Detail)
	}
}

// A table with no recorded read (built from the changes alone, created after
// the full read) is skipped: the others still have their pair kept, even
// when it sorts first.
func TestCheckPair_aTableWithoutARecordedReadIsSkipped(t *testing.T) {
	root := t.TempDir()
	at := func(h int) time.Time { return lr0.Add(time.Duration(h) * time.Hour) }
	lrWrite(t, root, at(0), "orders", lrDump(at(0), 100))
	lrWrite(t, root, at(1), "orders", lrFold(at(1), at(0), at(0), 1, 110))
	lrWrite(t, root, at(2), "orders", lrDump(at(2), 120))
	for h := 3; h <= 6; h++ {
		lrWrite(t, root, at(h), "orders", lrFold(at(h), at(h-1), at(2), h-2, int64(100+10*h)))
		lrWrite(t, root, at(h), "accounts", lrFold(at(h), at(h-1), time.Time{}, 0, int64(100+10*h)))
	}
	if r, b := baseline.CheckPair(root); r != reconstruct.SnapshotDirName(at(2)) || b != reconstruct.SnapshotDirName(at(1)) {
		t.Fatalf("CheckPair = %q, %q; want orders' full read and the snapshot before it", r, b)
	}
}

// A tie goes to the oldest read: the full read a table read on its own
// since came after.
func TestCheckPair_aTieGoesToTheOldestRead(t *testing.T) {
	root := t.TempDir()
	at := func(h int) time.Time { return lr0.Add(time.Duration(h) * time.Hour) }
	for _, tb := range []string{"accounts", "orders"} {
		lrWrite(t, root, at(0), tb, lrDump(at(0), 100))
		lrWrite(t, root, at(1), tb, lrDump(at(1), 110))
	}
	lrWrite(t, root, at(2), "accounts", lrDump(at(2), 120))
	lrWrite(t, root, at(2), "orders", lrFold(at(2), at(1), at(1), 1, 120))
	if r, b := baseline.CheckPair(root); r != reconstruct.SnapshotDirName(at(1)) || b != reconstruct.SnapshotDirName(at(0)) {
		t.Fatalf("CheckPair = %s, %s; want the older of the two reads, %s", r, b, reconstruct.SnapshotDirName(at(1)))
	}
}

// When the snapshot of the read most tables share is gone (removed by
// hand), the next most shared read whose snapshot is still here is kept.
func TestCheckPair_theNextReadWhenTheMostSharedIsGone(t *testing.T) {
	root := t.TempDir()
	at := func(h int) time.Time { return lr0.Add(time.Duration(h) * time.Hour) }
	for _, tb := range []string{"accounts", "orders", "payments"} {
		lrWrite(t, root, at(1), tb, lrDump(at(1), 110))
		lrWrite(t, root, at(2), tb, lrDump(at(2), 120))
		lrWrite(t, root, at(3), tb, lrFold(at(3), at(2), at(2), 1, 130))
	}
	lrWrite(t, root, at(4), "payments", lrDump(at(4), 140))
	for _, tb := range []string{"accounts", "orders"} {
		lrWrite(t, root, at(4), tb, lrFold(at(4), at(3), at(2), 2, 140))
	}
	if err := os.RemoveAll(filepath.Join(root, reconstruct.SnapshotDirName(at(2)))); err != nil {
		t.Fatal(err)
	}
	if r, b := baseline.CheckPair(root); r != reconstruct.SnapshotDirName(at(4)) || b != reconstruct.SnapshotDirName(at(3)) {
		t.Fatalf("CheckPair = %q, %q; want payments' read %s, the one still here", r, b, reconstruct.SnapshotDirName(at(4)))
	}
}
