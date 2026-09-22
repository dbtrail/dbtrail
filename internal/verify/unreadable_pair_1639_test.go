package verify

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1639: with the newest folder unreadable, verify used to decide from the
// folders before it and could report "match"; it now refuses and names the
// folder. A folder older than the pair changes nothing.

// snapshot1639 writes a read of the database (a real dump footer), so what
// a test sees is the unreadable-folder guard and not a footer that would not
// open.
func snapshot1639(t *testing.T, root string, ts time.Time) string {
	t.Helper()
	path := lrWrite(t, root, ts, "a", lrDump(ts, 100))
	return filepath.Dir(filepath.Dir(path))
}

func unreadable1639(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		if os.Getenv("CI") != "" {
			t.Fatal("running as root under CI: the mode-000 fixture is a no-op and this coverage would silently vanish")
		}
		t.Skip("root bypasses directory read permissions")
	}
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
}

var (
	v1639a = time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	v1639b = time.Date(2026, 9, 2, 6, 0, 0, 0, time.UTC)
	v1639c = time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
)

func TestFindBaselinePair_newestUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, v1639a)
	snapshot1639(t, root, v1639b)
	newest := snapshot1639(t, root, v1639c)
	unreadable1639(t, newest)
	pairs, prevOnly, err := FindBaselinePair(context.Background(), root)
	if !errors.Is(err, reconstruct.ErrUnreadableSnapshot) || pairs != nil || prevOnly != nil {
		t.Fatalf("pairs=%v prevOnly=%v err=%v; want a refusal", pairs, prevOnly, err)
	}
}

// The second newest unreadable: the pair would skip over it and its
// unpaired/prevOnly sets would describe the wrong predecessor.
func TestFindBaselinePair_middleUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, v1639a)
	unreadable1639(t, snapshot1639(t, root, v1639b))
	snapshot1639(t, root, v1639c)
	if _, _, err := FindBaselinePair(context.Background(), root); !errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want a refusal", err)
	}
}

// One readable snapshot and an older unreadable one: the unreadable one is
// the predecessor, so "only one baseline, nothing to verify yet" would be a
// false exit 0.
func TestFindBaselinePair_oneReadableWithOlderUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	unreadable1639(t, snapshot1639(t, root, v1639a))
	snapshot1639(t, root, v1639b)
	if _, _, err := FindBaselinePair(context.Background(), root); !errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want a refusal", err)
	}
}

// Two readable snapshots and an older unreadable one: the pair is the two
// newest, and the old folder changes nothing.
func TestFindBaselinePair_unreadableOlderThanThePairChangesNothing(t *testing.T) {
	root := t.TempDir()
	unreadable1639(t, snapshot1639(t, root, v1639a))
	snapshot1639(t, root, v1639b)
	snapshot1639(t, root, v1639c)
	pairs, _, err := FindBaselinePair(context.Background(), root)
	if err != nil || len(pairs) != 1 || pairs[0].Settled != nil || !pairs[0].PrevSnapshot.Equal(v1639b) {
		t.Fatalf("pairs=%+v err=%v: an unreadable folder older than the pair must change nothing", pairs, err)
	}
}

// One readable snapshot and a NEWER unreadable one: "only one baseline,
// nothing to verify yet" would be a false exit 0.
func TestFindBaselinePair_oneReadableWithNewerUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, v1639a)
	unreadable1639(t, snapshot1639(t, root, v1639b))
	if _, _, err := FindBaselinePair(context.Background(), root); !errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want a refusal", err)
	}
}

// Nothing readable: "no baselines found, check the job" (the AnyBaseline
// path) would name a cause that did not happen; FindBaselinePair refuses first.
func TestFindBaselinePair_onlyUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	unreadable1639(t, snapshot1639(t, root, v1639a))
	if _, _, err := FindBaselinePair(context.Background(), root); !errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
		t.Fatalf("FindBaselinePair err = %v, want a refusal", err)
	}
}

// A table absent from every readable folder is not "never baselined" while a
// folder could not be read.
func TestEverBaselinedTables_returnsTheFoldersItCouldNotRead(t *testing.T) {
	root := t.TempDir()
	locked := snapshot1639(t, root, v1639a)
	unreadable1639(t, locked)
	snapshot1639(t, root, v1639b)
	ever, unreadable, err := EverBaselinedTables(context.Background(), root)
	if err != nil || !ever["shop.a"] || len(unreadable) != 1 || unreadable[0].Path != locked {
		t.Fatalf("ever=%v unreadable=%+v err=%v", ever, unreadable, err)
	}
}
