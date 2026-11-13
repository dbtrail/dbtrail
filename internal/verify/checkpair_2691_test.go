package verify

import (
	"context"
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
