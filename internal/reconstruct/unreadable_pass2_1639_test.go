package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// listableNotEnterable sets dir to 0644: its names can be listed, its files
// cannot be opened or stat'ed. The shape a `chmod -R 644` leaves behind.
func listableNotEnterable(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		if os.Getenv("CI") != "" {
			t.Fatal("running as root under CI: the permission fixture is a no-op and this coverage would silently vanish")
		}
		t.Skip("root bypasses directory permissions")
	}
	if err := os.Chmod(dir, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
}

// #1639: a schema folder that lists but cannot be entered is unreadable to
// the listing too, so the table list and the lookup agree. Before, the list
// named the newest backup's tables and the lookup then read an older one.
func TestNewestSnapshotTables_listableButNotEnterableSchemaRefuses(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, t1639a, "shop.a")
	newer := snapshot1639(t, root, t1639b, "shop.a")
	listableNotEnterable(t, filepath.Join(newer, "shop"))
	files, skipped, err := ListBaselinesUnreadable(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || !files[0].SnapshotTime.Equal(t1639a) || len(skipped) != 1 || !skipped[0].SnapshotTime.Equal(t1639b) {
		t.Fatalf("files = %+v, skipped = %+v; want only the older file listed and the newer folder skipped", files, skipped)
	}
	_, err = NewestSnapshotTables(context.Background(), root)
	mustRefuseUnreadable(t, err, newer)
}

// Decision on #1639: when no readable backup holds the table, only a folder
// at or after the newest backup counts. An old unreadable folder must not
// turn a table created after the last backup into a refusal; it stays "no
// baseline", which callers answer with their fallbacks.
func TestFindBaseline_olderUnreadableWithNoReadableCopyIsNoBaseline(t *testing.T) {
	root := t.TempDir()
	old := snapshot1639(t, root, t1639a, "shop.a")
	snapshot1639(t, root, t1639b, "shop.b")
	unreadable(t, old)
	_, _, _, err := FindBaseline(context.Background(), root, "shop", "a", t1639c)
	if !errors.Is(err, ErrNoBaseline) || errors.Is(err, ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want ErrNoBaseline and not the unreadable refusal", err)
	}
}

func TestFindBaseline_newestUnreadableWithNoReadableCopyStillRefuses(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, t1639a, "shop.b")
	newest := snapshot1639(t, root, t1639b, "shop.a")
	unreadable(t, newest)
	_, _, _, err := FindBaseline(context.Background(), root, "shop", "a", t1639c)
	mustRefuseUnreadable(t, err, newest)
}
