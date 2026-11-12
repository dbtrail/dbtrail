package baseline

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Nothing the local folder keeps for itself goes to S3 (#2691). The upload of
// a whole root sent the upload interval's marks (.upload-held.<snapshot>) and
// the leftovers of an interrupted prune (.<snapshot>.pruning, a whole
// snapshot under a name no reader knows). The rule is one, shared by every
// upload path: an entry directly under a snapshots root whose name starts
// with a dot is local. It covers a mark added later without a list to keep
// up to date, which the invented names below stand for.

// rootDotEntries writes every dot-file and dot-directory the folder uses,
// through the real writers where there is one, plus invented ones, and
// returns their names.
func rootDotEntries(t *testing.T, root, snap string) []string {
	t.Helper()
	if err := MarkUploadHeld(root, snap); err != nil {
		t.Fatal(err)
	}
	if err := MarkRestored(root, snap); err != nil {
		t.Fatal(err)
	}
	noteRestoredLocalOnly(root, snap, true)
	files := []string{LastPruneFile, LastPruneFailureFile, NotInDestinationFile, CleanupWaitingFile, ResentFile,
		LastPruneFile + ".tmp-1", pruneLockName, pointerLockName, ".a-mark-added-later", ".DS_Store"}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(root, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	dirs := []string{"." + snap + pruningSuffix, "." + snap + ".discarding", ResolvedDirName + "/" + snap + "/shop",
		CompactDirName + "/shop", ".a-dir-added-later/shop"}
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, d, "orders.parquet"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			names = append(names, e.Name())
		}
	}
	return names
}

func uploadedKeys(b *fakeBucket) []string {
	var keys []string
	for _, c := range b.calls {
		if k, ok := strings.CutPrefix(c, "upload "); ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}

func TestUploadLocalOnly_noDotEntryOfTheRootIsSent(t *testing.T) {
	root := t.TempDir()
	snap := "2026-10-04T12-00-03Z"
	mkUploadSnapshot(t, root, snap)
	dots := rootDotEntries(t, root, snap)
	if !slices.Contains(dots, uploadHeldPrefix+snap) || !slices.Contains(dots, "."+snap+pruningSuffix) {
		t.Fatalf("the fixture lacks the entries of the defect: %v", dots)
	}
	b := newFakeBucket()
	if _, err := uploadWithOps(context.Background(), root, "p", false, lockedOps(b.ops())); err != nil {
		t.Fatal(err)
	}
	keys := uploadedKeys(b)
	if !slices.Equal(keys, []string{"p/" + snap + "/" + SuccessMarker, "p/" + snap + "/shop/orders.parquet"}) {
		t.Fatalf("uploaded %v, want the snapshot's table and its marker only (root dot entries: %v)", keys, dots)
	}
	for _, c := range b.calls {
		if strings.Contains(c, "p/.") {
			t.Fatalf("a dot entry of the root reached the bucket: %q", c)
		}
	}
}

// When the upload's root is ONE snapshot (the daemon sends one at a time),
// what is directly under it is a schema, and a schema may start with a dot
// (PostgreSQL allows it): that is data, and it is sent.
func TestUploadLocalOnly_aDotSchemaOfASnapshotIsSent(t *testing.T) {
	root := t.TempDir()
	snap := mkUploadSnapshot(t, root, "2026-10-04T12-00-03Z")
	if err := os.MkdirAll(filepath.Join(snap, ".hidden"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snap, ".hidden", "t.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	b := newFakeBucket()
	if _, err := uploadWithOps(context.Background(), snap, "p/2026-10-04T12-00-03Z", false, lockedOps(b.ops())); err != nil {
		t.Fatal(err)
	}
	if keys := uploadedKeys(b); !slices.Contains(keys, "p/2026-10-04T12-00-03Z/.hidden/t.parquet") {
		t.Fatalf("uploaded %v: a schema whose name starts with a dot was left out of its snapshot", keys)
	}
	// Told by a marker too, when the folder's name is not a snapshot's.
	other := filepath.Join(t.TempDir(), "renamed")
	if err := os.MkdirAll(filepath.Join(other, ".hidden"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if IsLocalOnly(other, filepath.Join(other, ".hidden")) {
		t.Fatal("a dot schema of a snapshot told by its marker was taken for a local entry")
	}
}

func TestUploadLocalOnly_theRule(t *testing.T) {
	root := t.TempDir()
	for _, tc := range []struct {
		root, path string
		want       bool
	}{
		{root, filepath.Join(root, ".upload-held.x"), true},
		{root + "/", filepath.Join(root, ".x"), true},
		{root, filepath.Join(root, "2026-10-04T12-00-03Z"), false},
		{root, filepath.Join(root, "views.sql"), false},
		{root, filepath.Join(root, "2026-10-04T12-00-03Z", ".x"), false}, // inside a snapshot
		{root, root, false},
		{".", ".x", true},
		{".", ".", false}, // the walk of "." starts at "." itself
	} {
		if got := IsLocalOnly(tc.root, tc.path); got != tc.want {
			t.Errorf("IsLocalOnly(%q, %q) = %v, want %v", tc.root, tc.path, got, tc.want)
		}
	}
}

func TestUploadLocalOnly_isSnapshotsRoot(t *testing.T) {
	root := t.TempDir()
	if IsSnapshotsRoot(root) {
		t.Fatal("an empty folder is not a folder of snapshots")
	}
	if err := os.MkdirAll(filepath.Join(root, ".archive"), 0o755); err != nil {
		t.Fatal(err)
	}
	if IsSnapshotsRoot(root) {
		t.Fatal("a folder with no snapshot directory was taken for a folder of snapshots")
	}
	snap := mkUploadSnapshot(t, root, "2026-10-04T12-00-03Z")
	if !IsSnapshotsRoot(root) {
		t.Fatal("a folder holding a snapshot is a folder of snapshots")
	}
	if IsSnapshotsRoot(snap) {
		t.Fatal("a snapshot is not a folder of snapshots")
	}
}
