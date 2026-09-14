package console

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1639: the console reads a local backup folder first and the durable copy
// second. A local folder that cannot be read used to look like "no backup
// here" and sent the lookup to the durable copy; after #1639 it refuses, and
// the durable copy must still be asked, with the answer saying why.
func TestBundleFindBaseline_unreadableLocalAsksTheDurableCopy(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory read permissions")
	}
	t1 := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 9, 2, 6, 0, 0, 0, time.UTC)
	mk := func(t *testing.T, root string, ts time.Time) string {
		dir := filepath.Join(root, reconstruct.SnapshotDirName(ts))
		if err := os.MkdirAll(filepath.Join(dir, "shop"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "shop", "a.parquet"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	// Both take the subtest's t: a restore registered on the parent would run
	// after the subtest's TempDir cleanup, which then cannot remove the folder.
	lock := func(t *testing.T, dir string) {
		if err := os.Chmod(dir, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	}

	t.Run("only local folder unreadable", func(t *testing.T) {
		local, durable := t.TempDir(), t.TempDir()
		lock(t, mk(t, local, t2))
		mk(t, durable, t2)
		b := &bundle{baselineSrc: local, baselineFallbackSrc: durable}
		path, at, stale, err := b.findBaseline(context.Background(), "shop", "a", t2.Add(time.Hour))
		if err != nil || !strings.HasPrefix(path, durable) || !at.Equal(t2) || !strings.Contains(stale.Message, "could not be read") {
			t.Fatalf("path=%s at=%v stale=%+v err=%v; want the durable copy with the cause", path, at, stale, err)
		}
	})
	t.Run("newer local folder unreadable, durable copy has it", func(t *testing.T) {
		local, durable := t.TempDir(), t.TempDir()
		mk(t, local, t1)
		lock(t, mk(t, local, t2))
		mk(t, durable, t2)
		b := &bundle{baselineSrc: local, baselineFallbackSrc: durable}
		path, at, _, err := b.findBaseline(context.Background(), "shop", "a", t2.Add(time.Hour))
		if err != nil || !strings.HasPrefix(path, durable) || !at.Equal(t2) {
			t.Fatalf("path=%s at=%v err=%v; want the newer snapshot from the durable copy", path, at, err)
		}
	})
	t.Run("durable copy is no newer: keep the local answer and its warning", func(t *testing.T) {
		local, durable := t.TempDir(), t.TempDir()
		mk(t, local, t1)
		lock(t, mk(t, local, t2))
		mk(t, durable, t1)
		b := &bundle{baselineSrc: local, baselineFallbackSrc: durable}
		path, at, stale, err := b.findBaseline(context.Background(), "shop", "a", t2.Add(time.Hour))
		if err != nil || !strings.HasPrefix(path, local) || !at.Equal(t1) || !stale.Unreadable {
			t.Fatalf("path=%s at=%v stale=%+v err=%v", path, at, stale, err)
		}
	})
}
