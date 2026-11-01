package baseline

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// #2596: a snapshot the upload interval held back on this machine.

func TestUploadHeld_markReadClear(t *testing.T) {
	root := t.TempDir()
	name := snapName(days(1))
	makeSnapshot(t, root, name, true, "shop/orders")
	if held, err := UploadHeld(root, name); held || err != nil {
		t.Fatalf("unmarked snapshot: held=%v err=%v", held, err)
	}
	if err := MarkUploadHeld(root, name); err != nil {
		t.Fatal(err)
	}
	if held, err := UploadHeld(root, name); !held || err != nil {
		t.Fatalf("marked snapshot: held=%v err=%v", held, err)
	}
	// Marking twice is not an error: a retried run can hold the same one.
	if err := MarkUploadHeld(root, name); err != nil {
		t.Fatalf("second mark: %v", err)
	}
	if err := ClearUploadHeld(root, name); err != nil {
		t.Fatal(err)
	}
	if held, _ := UploadHeld(root, name); held {
		t.Fatal("cleared snapshot still reads as held")
	}
	// Clearing what is not there is not an error either.
	if err := ClearUploadHeld(root, name); err != nil {
		t.Fatalf("clearing an absent mark: %v", err)
	}
	// The mark lives beside the snapshot, never inside it: everything that
	// reads a snapshot's own files (upload, integrity, carry-forward) must not
	// see a new one.
	if err := MarkUploadHeld(root, name); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "shop" && e.Name() != SuccessMarker {
			t.Fatalf("snapshot directory gained %q", e.Name())
		}
	}
}

// A name that is not a snapshot directory name is refused: the mark is a file
// at the root, and a name with a separator would land somewhere else.
func TestUploadHeld_refusesNonSnapshotNames(t *testing.T) {
	root := t.TempDir()
	for _, bad := range []string{"", "..", "../x", "a/b", "not-a-timestamp", " " + snapName(days(1))} {
		if err := MarkUploadHeld(root, bad); err == nil {
			t.Errorf("MarkUploadHeld(%q) accepted", bad)
		}
	}
}

// The prune, against a destination, with snapshots the interval held back.
func TestPrune_heldSnapshots(t *testing.T) {
	old1, old2, old3 := snapName(days(30)), snapName(days(20)), snapName(days(10))
	newest := snapName(days(1))
	opts := func(root string) PruneOptions {
		return PruneOptions{LocalDir: root, S3URL: "s3://b/p", Retain: days(7), Now: kn}
	}
	// probeOf answers "present" for exactly the names given and records
	// every name it was asked about.
	probeOf := func(asked *[]string, present ...string) durableProbe {
		return func(_ context.Context, name string) (bool, error) {
			*asked = append(*asked, name)
			return slices.Contains(present, name), nil
		}
	}
	setup := func(t *testing.T, held ...string) string {
		root := t.TempDir()
		for _, n := range []string{old1, old2, old3, newest} {
			makeSnapshot(t, root, n, true, "shop/orders")
		}
		for _, n := range held {
			if err := MarkUploadHeld(root, n); err != nil {
				t.Fatal(err)
			}
		}
		return root
	}

	t.Run("held and superseded by a newer snapshot in S3: removed, with its mark", func(t *testing.T) {
		root := setup(t, old1, old2)
		var asked []string
		res, err := pruneWithProbe(context.Background(), opts(root), probeOf(&asked, old3))
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(res.Pruned)
		if want := []string{old1, old2, old3}; !slices.Equal(res.Pruned, want) {
			t.Fatalf("pruned %v, want %v", res.Pruned, want)
		}
		for _, n := range []string{old1, old2} {
			if held, _ := UploadHeld(root, n); held {
				t.Errorf("%s: the mark outlived its snapshot", n)
			}
		}
		if len(res.NotAtDestination) != 0 || res.KeptNotDurable != 0 {
			t.Fatalf("held snapshots reported as missing from the destination: %+v", res)
		}
	})

	t.Run("held with nothing newer in S3: kept, and not reported as missing", func(t *testing.T) {
		root := setup(t, old1, old2, old3)
		var asked []string
		res, err := pruneWithProbe(context.Background(), opts(root), probeOf(&asked))
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Pruned) != 0 {
			t.Fatalf("pruned %v with no newer copy in S3", res.Pruned)
		}
		if res.KeptHeld != 3 || res.KeptNotDurable != 0 || len(res.NotAtDestination) != 0 {
			t.Fatalf("want 3 kept as held, none as missing: %+v", res)
		}
		if _, ok, _ := ReadNotInDestination(root); ok {
			t.Fatal("the page's 'not in S3' record counts snapshots held on purpose")
		}
	})

	t.Run("held newer than the newest copy in S3: kept", func(t *testing.T) {
		root := setup(t, old2, old3)
		var asked []string
		res, err := pruneWithProbe(context.Background(), opts(root), probeOf(&asked, old1))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(res.Pruned, []string{old1}) || res.KeptHeld != 2 {
			t.Fatalf("pruned %v kept-held %d, want only %s pruned and 2 held", res.Pruned, res.KeptHeld, old1)
		}
	})

	t.Run("the newest snapshot is never removed, held or not", func(t *testing.T) {
		root := setup(t, old1, old2, old3, newest)
		var asked []string
		res, err := pruneWithProbe(context.Background(), opts(root), probeOf(&asked, old1, old2, old3, newest))
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(res.Pruned, newest) {
			t.Fatal("removed the newest snapshot")
		}
	})

	t.Run("a probe error on the newer copy keeps the held ones", func(t *testing.T) {
		root := setup(t, old1, old2)
		failing := func(_ context.Context, name string) (bool, error) {
			return false, os.ErrDeadlineExceeded
		}
		res, err := pruneWithProbe(context.Background(), opts(root), failing)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []string{old1, old2} {
			if slices.Contains(res.Pruned, n) {
				t.Fatalf("removed held %s on an unconfirmed newer copy", n)
			}
		}
	})

	t.Run("held snapshots are not probed one by one", func(t *testing.T) {
		root := setup(t, old1, old2)
		var asked []string
		if _, err := pruneWithProbe(context.Background(), opts(root), probeOf(&asked, old3)); err != nil {
			t.Fatal(err)
		}
		for _, n := range asked {
			if n == old1 || n == old2 {
				t.Fatalf("probed held %s: a held snapshot is not in S3 by design", n)
			}
		}
	})

	t.Run("an orphaned mark is swept", func(t *testing.T) {
		root := setup(t)
		gone := snapName(days(40))
		if err := MarkUploadHeld(root, gone); err != nil {
			t.Fatal(err)
		}
		var asked []string
		if _, err := pruneWithProbe(context.Background(), opts(root), probeOf(&asked)); err != nil {
			t.Fatal(err)
		}
		if held, _ := UploadHeld(root, gone); held {
			t.Fatal("a mark for a snapshot that no longer exists was kept")
		}
	})

	t.Run("dry run removes no mark", func(t *testing.T) {
		root := setup(t, old1)
		o := opts(root)
		o.DryRun = true
		var asked []string
		res, err := pruneWithProbe(context.Background(), o, probeOf(&asked, old3))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(res.Pruned, old1) {
			t.Fatalf("dry run would not remove held %s: %v", old1, res.Pruned)
		}
		if held, _ := UploadHeld(root, old1); !held {
			t.Fatal("a dry run removed a mark")
		}
	})
}

// Local-only roots never carry marks (the interval needs a destination), but
// one that does must not change what the keep-newest prune decides.
func TestPrune_heldMarksIgnoredWithoutDestination(t *testing.T) {
	root := t.TempDir()
	a, b := snapName(days(20)), snapName(days(10))
	makeSnapshot(t, root, a, true, "shop/orders")
	makeSnapshot(t, root, b, true, "shop/orders")
	if err := MarkUploadHeld(root, a); err != nil {
		t.Fatal(err)
	}
	res := keepNewest(t, root, 1)
	if !slices.Equal(res.Pruned, []string{a}) || res.KeptHeld != 0 {
		t.Fatalf("keep-newest with a mark: %+v", res)
	}
}
