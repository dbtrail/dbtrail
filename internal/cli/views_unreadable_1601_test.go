package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunViews_unreadableSnapshotDirectoryIsNamed drives #1601 through the
// COMMAND: the count the lister reports has to reach the header, and what
// can silently regress is the command layer dropping it on the floor (the
// two-value ListBaselines wrapper still exists and compiles).
func TestRunViews_unreadableSnapshotDirectoryIsNamed(t *testing.T) {
	if os.Geteuid() == 0 {
		if os.Getenv("CI") != "" {
			t.Fatal("running as root under CI: the mode-000 fixture is a no-op and this coverage would silently vanish")
		}
		t.Skip("root bypasses directory read permissions; the mode-000 fixture is a no-op")
	}
	root, _, newer := baselineDirWithTwoSnapshots(t)
	sealed := filepath.Join(root, newer)
	if err := os.Chmod(sealed, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sealed, 0o755) })

	sql := runViewsOverBaselines(t, root, true)
	if !strings.Contains(sql, "2026-06-09T12:00:00Z") {
		t.Fatalf("the readable older snapshot is not pinned:\n%s", sql)
	}
	if !strings.Contains(sql, "NOTE: 1 snapshot director") || !strings.Contains(sql, "could not be read") {
		t.Errorf("the header does not say a snapshot directory could not be read:\n%s", sql)
	}
}
