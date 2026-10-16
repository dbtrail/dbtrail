package copymirror

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2431: a server that stops keeping its snapshots only in S3 takes its
// mirror's complete snapshots into its new folder, so SQL on its copy does not
// go dark until its next snapshot.

const (
	moveOld   = "2026-10-01T00-00-00Z"
	moveNew   = "2026-10-02T00-00-00Z"
	moveUndon = "2026-10-03T00-00-00Z"
)

// moveFixture is a mirror holding two complete snapshots, one without its
// marker, and an interrupted download.
func moveFixture(t *testing.T) (mirror, dest string) {
	t.Helper()
	mirror = filepath.Join(t.TempDir(), "mirror")
	write := func(rel string) {
		p := filepath.Join(mirror, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(rel), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(SourceFile)
	for _, s := range []string{moveOld, moveNew} {
		write(s + "/db/t.parquet")
		write(s + "/" + baseline.SuccessMarker)
	}
	write(moveUndon + "/db/t.parquet")
	write("." + moveUndon + downloadingSuffix + "/db/t.parquet")
	dest = filepath.Join(t.TempDir(), "snapshots", "id1")
	if err := os.MkdirAll(dest, 0o700); err != nil {
		t.Fatal(err)
	}
	return mirror, dest
}

func TestMoveSnapshots_movesTheCompleteOnesAndPointsCurrentAtTheNewest(t *testing.T) {
	mirror, dest := moveFixture(t)
	var marked []string
	baseline.SetSnapshotViewsWriter(func(dir string) { marked = append(marked, dir) })
	t.Cleanup(func() { baseline.SetSnapshotViewsWriter(nil) })

	moved, err := MoveSnapshots(mirror, dest)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(moved, []string{moveOld, moveNew}) {
		t.Fatalf("moved %v, want oldest first, complete only", moved)
	}
	for _, s := range moved {
		if _, err := os.Stat(filepath.Join(dest, s, "db", "t.parquet")); err != nil {
			t.Errorf("%s not in the folder: %v", s, err)
		}
		if _, err := os.Stat(filepath.Join(dest, s, baseline.SuccessMarker)); err != nil {
			t.Errorf("%s lost its marker: %v", s, err)
		}
		if _, err := os.Stat(filepath.Join(mirror, s)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s still in the mirror: %v", s, err)
		}
	}
	// Its own views file is written again where it now lives.
	if !slices.Equal(marked, []string{filepath.Join(dest, moveOld), filepath.Join(dest, moveNew)}) {
		t.Errorf("views rewritten for %v", marked)
	}
	if got, err := os.Readlink(filepath.Join(dest, baseline.CurrentLinkName)); err != nil || filepath.Base(got) != moveNew {
		t.Errorf("current = %q (%v), want %s", got, err, moveNew)
	}
	// What is not complete stays for the sweep.
	for _, left := range []string{moveUndon, "." + moveUndon + downloadingSuffix, SourceFile} {
		if _, err := os.Lstat(filepath.Join(mirror, left)); err != nil {
			t.Errorf("%s was taken: %v", left, err)
		}
	}
}

// A folder that already holds a snapshot keeps its own: nothing is moved,
// so no name collides and the pointer never goes back.
func TestMoveSnapshots_aFolderWithSnapshotsTakesNone(t *testing.T) {
	mirror, dest := moveFixture(t)
	if err := os.MkdirAll(filepath.Join(dest, "2026-09-01T00-00-00Z"), 0o700); err != nil {
		t.Fatal(err)
	}
	moved, err := MoveSnapshots(mirror, dest)
	if !errors.Is(err, ErrDestHoldsSnapshots) || len(moved) != 0 {
		t.Fatalf("moved %v err %v, want ErrDestHoldsSnapshots and nothing", moved, err)
	}
	if _, err := os.Stat(filepath.Join(mirror, moveNew)); err != nil {
		t.Errorf("the mirror lost a snapshot: %v", err)
	}
}

// Files that are not snapshots (the write check's leftovers, a dangling
// pointer) do not count as the folder holding snapshots.
func TestMoveSnapshots_otherFilesInTheFolderAreNotSnapshots(t *testing.T) {
	mirror, dest := moveFixture(t)
	if err := os.WriteFile(filepath.Join(dest, ".dbtrail-write-check-1"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest, moveOld), nil, 0o600); err != nil { // a FILE under a snapshot's name
		t.Fatal(err)
	}
	if _, err := MoveSnapshots(mirror, dest); err == nil {
		t.Fatal("a file in the way of a snapshot's name was overwritten or ignored")
	}
}

func TestMoveSnapshots_noMirror(t *testing.T) {
	dest := t.TempDir()
	moved, err := MoveSnapshots(filepath.Join(t.TempDir(), "absent"), dest)
	if err != nil || len(moved) != 0 {
		t.Fatalf("absent mirror: moved %v err %v, want nothing and no error", moved, err)
	}
	moved, err = MoveSnapshots("", dest)
	if err != nil || len(moved) != 0 {
		t.Fatalf("no mirror dir: moved %v err %v", moved, err)
	}
}

// A folder with snapshots and no SourceFile is not a mirror, so not ours to
// empty.
func TestMoveSnapshots_notAMirror(t *testing.T) {
	mirror, dest := moveFixture(t)
	if err := os.Remove(filepath.Join(mirror, SourceFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := MoveSnapshots(mirror, dest); !errors.Is(err, ErrNotAMirror) {
		t.Fatalf("err %v, want ErrNotAMirror", err)
	}
}

func TestMoveSnapshots_busyMirrorIsLeftAlone(t *testing.T) {
	mirror, dest := moveFixture(t)
	unlock, busy, err := lockMirror(mirror)
	if err != nil || busy {
		t.Skipf("no lock on this platform (busy=%v err=%v)", busy, err)
	}
	defer unlock()
	if moved, err := MoveSnapshots(mirror, dest); !errors.Is(err, ErrBusy) || len(moved) != 0 {
		t.Fatalf("moved %v err %v, want ErrBusy and nothing", moved, err)
	}
}
