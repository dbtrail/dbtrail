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

// #1639: verify compares the two newest backups. With the newest folder
// unreadable it used to compare the two before it and could report "match";
// it now refuses and names the folder. A folder older than the pair changes
// nothing.

func snapshot1639(t *testing.T, root string, ts time.Time) string {
	t.Helper()
	dir := filepath.Join(root, reconstruct.SnapshotDirName(ts), "shop")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.parquet"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(dir)
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
	pairs, unpaired, prevOnly, err := FindBaselinePair(context.Background(), root)
	if !errors.Is(err, reconstruct.ErrUnreadableSnapshot) || pairs != nil || unpaired != nil || prevOnly != nil {
		t.Fatalf("pairs=%v unpaired=%v prevOnly=%v err=%v; want a refusal", pairs, unpaired, prevOnly, err)
	}
}

// The second newest unreadable: the pair would skip over it and its
// unpaired/prevOnly sets would describe the wrong predecessor.
func TestFindBaselinePair_middleUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, v1639a)
	unreadable1639(t, snapshot1639(t, root, v1639b))
	snapshot1639(t, root, v1639c)
	if _, _, _, err := FindBaselinePair(context.Background(), root); !errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want a refusal", err)
	}
}

// One readable snapshot and an older unreadable one: still the benign "only
// one baseline" answer, not a refusal.
func TestFindBaselinePair_olderUnreadableChangesNothing(t *testing.T) {
	root := t.TempDir()
	unreadable1639(t, snapshot1639(t, root, v1639a))
	snapshot1639(t, root, v1639b)
	pairs, unpaired, prevOnly, err := FindBaselinePair(context.Background(), root)
	if err != nil || pairs != nil || unpaired != nil || prevOnly != nil {
		t.Fatalf("pairs=%v unpaired=%v prevOnly=%v err=%v; want nothing to verify, no error", pairs, unpaired, prevOnly, err)
	}
}

// One readable snapshot and a NEWER unreadable one: "only one baseline,
// nothing to verify yet" would be a false exit 0.
func TestFindBaselinePair_oneReadableWithNewerUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, v1639a)
	unreadable1639(t, snapshot1639(t, root, v1639b))
	if _, _, _, err := FindBaselinePair(context.Background(), root); !errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want a refusal", err)
	}
}

// Nothing readable: "no baselines found, check the job" (the AnyBaseline
// path) would name a cause that did not happen; FindBaselinePair refuses first.
func TestFindBaselinePair_onlyUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	unreadable1639(t, snapshot1639(t, root, v1639a))
	if _, _, _, err := FindBaselinePair(context.Background(), root); !errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
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
