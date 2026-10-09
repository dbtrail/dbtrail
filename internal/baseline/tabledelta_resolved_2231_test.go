package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// chainOf is a chain of plain pairs 0..n-1 beside base, as a listing returns
// it. The files are not written.
func chainOf(base string, n int) []TableDeltaFile {
	var files []TableDeltaFile
	for seq := range n {
		p, u := TableDeltaPaths(base, seq)
		files = append(files, TableDeltaFile{Seq: seq, SeqLo: seq, Posdel: p, Upserts: u})
	}
	return files
}

// TestResolvedTableDeltaPaths: where a chain's resolved pair is kept, and the
// chains and paths that have none.
func TestResolvedTableDeltaPaths(t *testing.T) {
	root := filepath.Join("/var", "lib", "snapshots")
	base := filepath.Join(root, "2026-10-09T00-00-00Z", "shop", "orders.parquet")
	dir := filepath.Join(root, ResolvedDirName, "2026-10-09T00-00-00Z", "shop")
	if got := resolvedTableDeltaDir(base); got != dir {
		t.Fatalf("dir = %q, want %q", got, dir)
	}
	f, ok := ResolvedTableDeltaPaths(base, chainOf(base, 3))
	if !ok || f.SeqLo != 0 || f.Seq != 2 || !f.Range() ||
		f.Posdel != filepath.Join(dir, "orders.000000-000002.posdel") || f.Upserts != filepath.Join(dir, "orders.000000-000002.upserts") {
		t.Fatalf("three pairs: %+v ok=%v", f, ok)
	}
	// A chain whose first pairs are already a range keeps that range's start.
	ranged := []TableDeltaFile{{SeqLo: 0, Seq: 14}, {SeqLo: 15, Seq: 15}}
	if f, ok := ResolvedTableDeltaPaths(base, ranged); !ok || f.SeqLo != 0 || f.Seq != 15 {
		t.Fatalf("a range and a pair: %+v ok=%v", f, ok)
	}
	for name, tc := range map[string]struct {
		base  string
		files []TableDeltaFile
	}{
		"no chain":                 {base, nil},
		"one pair":                 {base, chainOf(base, 1)},
		"one range pair":           {base, []TableDeltaFile{{SeqLo: 0, Seq: 5}}},
		"an s3 snapshot":           {"s3://bucket/snaps/2026-10-09T00-00-00Z/shop/orders.parquet", chainOf(base, 3)},
		"no path":                  {"", chainOf(base, 3)},
		"a file with no snapshot":  {"orders.parquet", chainOf(base, 3)},
		"a file one level under /": {string(filepath.Separator) + "orders.parquet", chainOf(base, 3)},
	} {
		if f, ok := ResolvedTableDeltaPaths(tc.base, tc.files); ok {
			t.Errorf("%s: a resolved pair is named: %+v", name, f)
		}
	}
}

// TestFindResolvedTableDelta: a pair is found only whole and only under the
// chain's own range. What a killed run leaves (the posdel alone, or the
// temporary names) and the pair of a shorter chain are not a pair.
func TestFindResolvedTableDelta(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "2026-10-09T00-00-00Z", "shop", "orders.parquet")
	files := chainOf(base, 3)
	want, _ := ResolvedTableDeltaPaths(base, files)
	if err := os.MkdirAll(filepath.Dir(want.Upserts), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(p string) {
		t.Helper()
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	found := func() bool { _, ok := FindResolvedTableDelta(base, files); return ok }
	if found() {
		t.Fatal("found with nothing written")
	}
	write(want.Posdel + ".tmp")
	write(want.Upserts + ".tmp")
	if found() {
		t.Fatal("found with only the temporary names written")
	}
	write(want.Posdel)
	if found() {
		t.Fatal("found with the posdel alone: a run killed between its two renames")
	}
	// The pair of the chain one pair ago.
	shorter, _ := ResolvedTableDeltaPaths(base, files[:2])
	write(shorter.Posdel)
	write(shorter.Upserts)
	if found() {
		t.Fatal("the pair of a shorter chain was found for this one")
	}
	write(want.Upserts)
	got, ok := FindResolvedTableDelta(base, files)
	if !ok || got != want {
		t.Fatalf("whole pair: %+v ok=%v, want %+v", got, ok, want)
	}
	// Marked as leaving: the daemon is about to remove this snapshot's
	// pairs, and a statement that named one now would open it too late.
	mark := filepath.Join(filepath.Dir(filepath.Dir(want.Upserts)), ResolvedLeavingMark)
	write(mark)
	if found() {
		t.Fatal("a pair under a leaving mark is handed out")
	}
	if err := os.Remove(mark); err != nil {
		t.Fatal(err)
	}
	if !found() {
		t.Fatal("the pair is not found again once the mark is gone")
	}
	// The upserts alone: no writer leaves that, and it is still not a pair.
	if err := os.Remove(want.Posdel); err != nil {
		t.Fatal(err)
	}
	if found() {
		t.Fatal("found with the upserts alone")
	}
	write(want.Posdel)
	// A directory under the pair's name is not a file to read.
	if err := os.Remove(want.Upserts); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(want.Upserts, 0o755); err != nil {
		t.Fatal(err)
	}
	if found() {
		t.Fatal("found with a directory in place of the upserts")
	}
}

// An upload of the whole snapshots root (`bintrail baseline --upload`) sends
// the snapshots and not what the daemon keeps beside them: resolved pairs,
// their work directories and staged compactions were sent as snapshot data,
// to a bucket where nothing reads them and no retention removes them. A
// directory of the same name INSIDE a snapshot is that snapshot's data.
func TestUpload_leavesTheDaemonsWorkDirectoriesOut(t *testing.T) {
	root := t.TempDir()
	snap := mkUploadSnapshot(t, root, "2026-10-04T12-00-03Z")
	for _, f := range []string{
		filepath.Join(root, ResolvedDirName, "2026-10-04T12-00-03Z", "shop", "orders.000000-000001.posdel"),
		filepath.Join(root, ResolvedDirName, "2026-10-04T12-00-03Z", "shop", "orders.000000-000001.upserts"),
		filepath.Join(root, ResolvedDirName, "2026-10-04T12-00-03Z", "shop", ".work-1", "orders.000000-000001.upserts.tmp"),
		filepath.Join(root, CompactDirName, "shop", "orders", "2026-10-04T00-00-00Z", "orders.000000-000014.upserts"),
		filepath.Join(snap, "shop", CompactDirName, "kept.parquet"),
	} {
		if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b := newFakeBucket()
	if _, err := uploadWithOps(context.Background(), root, "p", false, lockedOps(b.ops())); err != nil {
		t.Fatal(err)
	}
	sent := map[string]bool{}
	for key := range b.objects {
		sent[key] = true
		if strings.HasPrefix(key, "p/"+ResolvedDirName+"/") || strings.HasPrefix(key, "p/"+CompactDirName+"/") {
			t.Errorf("uploaded the daemon's own work: %s", key)
		}
	}
	for _, want := range []string{"p/2026-10-04T12-00-03Z/shop/orders.parquet", "p/2026-10-04T12-00-03Z/shop/" + CompactDirName + "/kept.parquet"} {
		if !sent[want] {
			t.Errorf("%s was not uploaded; sent: %v", want, sent)
		}
	}
}

// The daemon uploads ONE snapshot directory. What sits directly under it is
// a schema, and a schema named like one of the daemon's work directories is
// that snapshot's data: leaving it out would publish a snapshot with a
// schema missing.
func TestUpload_aSchemaNamedLikeAWorkDirectoryIsUploaded(t *testing.T) {
	root := t.TempDir()
	snap := mkUploadSnapshot(t, root, "2026-10-04T12-00-03Z")
	for _, schema := range []string{ResolvedDirName, CompactDirName} {
		f := filepath.Join(snap, schema, "orders.parquet")
		if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b := newFakeBucket()
	if _, err := uploadWithOps(context.Background(), snap, "p/2026-10-04T12-00-03Z", false, lockedOps(b.ops())); err != nil {
		t.Fatal(err)
	}
	for _, schema := range []string{"shop", ResolvedDirName, CompactDirName} {
		if key := "p/2026-10-04T12-00-03Z/" + schema + "/orders.parquet"; b.objects[key] == nil {
			t.Errorf("%s was not uploaded", key)
		}
	}
}

// The same for a snapshot whose directory is not named as a time (a copy an
// operator renamed): its completeness marker says it is a snapshot.
func TestUpload_aRenamedSnapshotKeepsItsSchemas(t *testing.T) {
	root := t.TempDir()
	snap := mkUploadSnapshot(t, root, "before-the-migration")
	f := filepath.Join(snap, ResolvedDirName, "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	b := newFakeBucket()
	if _, err := uploadWithOps(context.Background(), snap, "p/before-the-migration", false, lockedOps(b.ops())); err != nil {
		t.Fatal(err)
	}
	if key := "p/before-the-migration/" + ResolvedDirName + "/orders.parquet"; b.objects[key] == nil {
		t.Errorf("%s was not uploaded; sent: %v", key, b.calls)
	}
}
