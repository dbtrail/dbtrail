package baseline

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// An empty snapshot folder left by a process that died (#2691): every writer
// makes the folder and then writes _INCOMPLETE into it, so a folder that
// holds no file at all is one whose writer stopped in between. It used to be
// kept for ever as a snapshot being created. The cleanup now removes it once
// it has held nothing for emptySnapshotDirAge, by removing empty directories
// only, so a file written into it meanwhile is never removed.

// emptyFolder makes root/name with the given empty subdirectories and dates
// every directory of it age ago.
func emptyFolder(t *testing.T, root, name string, age time.Duration, subdirs ...string) string {
	t.Helper()
	p := filepath.Join(root, name)
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, s := range subdirs {
		if err := os.MkdirAll(filepath.Join(p, s), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Now().Add(-age)
	_ = filepath.WalkDir(p, func(q string, _ os.DirEntry, _ error) error { return os.Chtimes(q, at, at) })
	return p
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

func TestPruneEmpty_anOldEmptyFolderIsRemoved(t *testing.T) {
	for _, shape := range []string{"no subdirectory", "empty schema directories"} {
		t.Run(shape, func(t *testing.T) {
			root := t.TempDir()
			s := minutes(t, root, 10, 5)
			name := snapName(3 * time.Hour)
			var subs []string
			if shape != "no subdirectory" {
				subs = []string{"shop", "billing/nested"}
			}
			p := emptyFolder(t, root, name, emptySnapshotDirAge+time.Minute, subs...)
			prev := slog.Default()
			t.Cleanup(func() { slog.SetDefault(prev) })
			var logged bytes.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
			res := keepNewest(t, root, 3)
			if exists(p) {
				t.Fatal("an empty snapshot folder older than the age is still there")
			}
			if !slices.Equal(res.RemovedEmpty, []string{name}) || res.KeptIncomplete != 0 {
				t.Fatalf("RemovedEmpty = %v, KeptIncomplete = %d", res.RemovedEmpty, res.KeptIncomplete)
			}
			wantOnDisk(t, root, s...)
			line := "baseline prune: removed an empty snapshot folder; the run that created it stopped before writing anything in it"
			if !strings.Contains(logged.String(), line) {
				t.Fatalf("log = %q, want %q", logged.String(), line)
			}
			t.Logf("log: %s", strings.TrimSpace(logged.String()))
		})
	}
}

// What is never removed: a folder younger than the age (its writer may be
// between the mkdir and its first file), one dated in the future, one with a
// file anywhere in it, one whose newest subdirectory is young, a folder not
// named as a snapshot, the `current` target and a pinned one.
func TestPruneEmpty_whatStays(t *testing.T) {
	old := emptySnapshotDirAge + time.Minute
	cases := []struct {
		name  string
		setup func(t *testing.T, root string) string
	}{
		{"younger than the age", func(t *testing.T, root string) string {
			return emptyFolder(t, root, snapName(3*time.Hour), emptySnapshotDirAge-time.Minute)
		}},
		{"dated in the future", func(t *testing.T, root string) string {
			return emptyFolder(t, root, snapName(3*time.Hour), -time.Hour)
		}},
		{"only the incomplete marker", func(t *testing.T, root string) string {
			p := emptyFolder(t, root, snapName(3*time.Hour), old)
			if err := WriteIncompleteMarker(p); err != nil {
				t.Fatal(err)
			}
			return p
		}},
		{"a file deep inside", func(t *testing.T, root string) string {
			p := emptyFolder(t, root, snapName(3*time.Hour), old, "shop/x")
			if err := os.WriteFile(filepath.Join(p, "shop", "x", "orders.parquet.tmp"), nil, 0o644); err != nil {
				t.Fatal(err)
			}
			return p
		}},
		{"a young subdirectory", func(t *testing.T, root string) string {
			p := emptyFolder(t, root, snapName(3*time.Hour), old)
			if err := os.Mkdir(filepath.Join(p, "shop"), 0o755); err != nil {
				t.Fatal(err)
			}
			old := time.Now().Add(-old)
			if err := os.Chtimes(p, old, old); err != nil {
				t.Fatal(err)
			}
			return p
		}},
		{"not a snapshot name", func(t *testing.T, root string) string {
			return emptyFolder(t, root, "notes", old)
		}},
		{"the current target", func(t *testing.T, root string) string {
			p := emptyFolder(t, root, snapName(3*time.Hour), old)
			if err := os.Symlink(filepath.Base(p), filepath.Join(root, CurrentLinkName)); err != nil {
				t.Fatal(err)
			}
			return p
		}},
		{"pinned", func(t *testing.T, root string) string {
			p := emptyFolder(t, root, snapName(3*time.Hour), old)
			t.Cleanup(PinSnapshot(root, filepath.Base(p), "a test reader", time.Hour))
			return p
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			minutes(t, root, 5)
			p := tc.setup(t, root)
			res := keepNewest(t, root, 1)
			if !exists(p) || len(res.RemovedEmpty) != 0 {
				t.Fatalf("removed %v; the folder exists: %v", res.RemovedEmpty, exists(p))
			}
		})
	}
}

// A writer that puts a file in the folder between the check and the
// removal: the file stays, and so does the folder.
func TestPruneEmpty_aFileWrittenDuringTheRemovalStays(t *testing.T) {
	root := t.TempDir()
	minutes(t, root, 5)
	p := emptyFolder(t, root, snapName(3*time.Hour), emptySnapshotDirAge+time.Minute, "shop")
	prev := emptyDirRemoveHook
	t.Cleanup(func() { emptyDirRemoveHook = prev })
	emptyDirRemoveHook = func(string) {
		if err := WriteIncompleteMarker(p); err != nil {
			t.Error(err)
		}
	}
	res := keepNewest(t, root, 1)
	if !exists(filepath.Join(p, IncompleteMarker)) {
		t.Fatal("the cleanup removed a file a writer had just put in the folder")
	}
	if len(res.RemovedEmpty) != 0 {
		t.Fatalf("RemovedEmpty = %v for a folder that holds a file", res.RemovedEmpty)
	}
}

// The other modes: a destination and the age rule remove it too, and a dry
// run removes nothing.
func TestPruneEmpty_everyModeAndTheDryRun(t *testing.T) {
	old := emptySnapshotDirAge + time.Minute
	t.Run("destination", func(t *testing.T) {
		root := t.TempDir()
		minutes(t, root, 5)
		p := emptyFolder(t, root, snapName(3*time.Hour), old)
		res, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, S3URL: "s3://b/p", KeepNewest: 1, Now: kn}, answers(nil))
		if err != nil || exists(p) || len(res.RemovedEmpty) != 1 {
			t.Fatalf("err=%v exists=%v removed=%v", err, exists(p), res.RemovedEmpty)
		}
	})
	t.Run("dry run", func(t *testing.T) {
		root := t.TempDir()
		minutes(t, root, 5)
		p := emptyFolder(t, root, snapName(3*time.Hour), old)
		res, err := PruneLocal(context.Background(), PruneOptions{LocalDir: root, KeepNewest: 1, Now: kn, DryRun: true})
		if err != nil || !exists(p) || len(res.RemovedEmpty) != 0 {
			t.Fatalf("a dry run: err=%v exists=%v removed=%v", err, exists(p), res.RemovedEmpty)
		}
	})
}
