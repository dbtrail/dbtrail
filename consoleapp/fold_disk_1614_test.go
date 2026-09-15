package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// stubDisk swaps the free-space probe for the test, on a filesystem with a
// real total size.
func stubDisk(t *testing.T, free uint64, err error) {
	t.Helper()
	stubDiskTotal(t, free, 1<<50, err)
}

func stubDiskTotal(t *testing.T, free, total uint64, err error) {
	t.Helper()
	prev := diskSpaceFn
	diskSpaceFn = func(string) (uint64, uint64, error) { return free, total, err }
	t.Cleanup(func() { diskSpaceFn = prev })
}

// localSnapshot lays out <root>/<ts>/shop/<table>.parquet files of the given
// sizes and returns root.
func localSnapshot(t *testing.T, ts time.Time, sizes map[string]int) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, reconstruct.SnapshotDirName(ts), "shop")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for table, n := range sizes {
		if err := os.WriteFile(filepath.Join(dir, table+".parquet"), make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestCheckFoldDisk(t *testing.T) {
	ts := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	at := ts.Add(time.Hour)
	root := localSnapshot(t, ts, map[string]int{"a": 3000, "b": 5000})
	tables := []string{"shop.a", "shop.b"}
	need := int64(8000 + foldDiskMargin)
	ctx := context.Background()

	t.Run("room for it", func(t *testing.T) {
		stubDisk(t, uint64(need), nil)
		if err := checkFoldDisk(ctx, root, root, at, tables, false); err != nil {
			t.Fatalf("refused with exactly enough room: %v", err)
		}
	})
	t.Run("one byte short refuses and names both numbers", func(t *testing.T) {
		stubDisk(t, uint64(need-1), nil)
		err := checkFoldDisk(ctx, root, root, at, tables, false)
		if !errors.Is(err, errFoldDiskFull) || !strings.Contains(err.Error(), root) ||
			!strings.Contains(err.Error(), "2026-09-01 06:00:00") || !strings.Contains(err.Error(), "7.8 KiB") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("only the tables the fold writes count", func(t *testing.T) {
		stubDisk(t, uint64(3000+foldDiskMargin), nil)
		if err := checkFoldDisk(ctx, root, root, at, []string{"shop.a"}, false); err != nil {
			t.Fatalf("counted a table the fold does not write: %v", err)
		}
	})
	t.Run("a full disk refuses", func(t *testing.T) {
		stubDisk(t, 0, nil)
		if err := checkFoldDisk(ctx, root, root, at, tables, false); !errors.Is(err, errFoldDiskFull) {
			t.Fatalf("a disk with zero free and a real size was waved through: %v", err)
		}
	})
	t.Run("a mount that reports no size proceeds", func(t *testing.T) {
		stubDiskTotal(t, 0, 0, nil)
		if err := checkFoldDisk(ctx, root, root, at, tables, false); err != nil {
			t.Fatalf("refused on a mount that cannot answer: %v", err)
		}
	})
	t.Run("a probe error proceeds", func(t *testing.T) {
		stubDisk(t, 1, errors.New("statfs: not supported"))
		if err := checkFoldDisk(ctx, root, root, at, tables, false); err != nil {
			t.Fatalf("refused on a probe error: %v", err)
		}
	})
	t.Run("a source that cannot be listed proceeds", func(t *testing.T) {
		stubDisk(t, 1, nil)
		if err := checkFoldDisk(ctx, filepath.Join(root, "missing"), root, at, tables, false); err != nil {
			t.Fatalf("refused without an estimate: %v", err)
		}
	})
	t.Run("the snapshot the fold starts from, not a newer one", func(t *testing.T) {
		stubDisk(t, uint64(8000+foldDiskMargin), nil)
		root := localSnapshot(t, ts, map[string]int{"a": 3000, "b": 5000})
		newer := filepath.Join(root, reconstruct.SnapshotDirName(at.Add(time.Hour)), "shop")
		if err := os.MkdirAll(newer, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(newer, "a.parquet"), make([]byte, 1<<20), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := checkFoldDisk(ctx, root, root, at, tables, false); err != nil {
			t.Fatalf("sized a snapshot newer than the target: %v", err)
		}
	})
	t.Run("the newest older snapshot, whatever order the listing returns", func(t *testing.T) {
		root := localSnapshot(t, ts, map[string]int{"a": 3000, "b": 5000})
		older := filepath.Join(root, reconstruct.SnapshotDirName(ts.Add(-time.Hour)), "shop")
		if err := os.MkdirAll(older, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, tb := range []string{"a", "b"} {
			if err := os.WriteFile(filepath.Join(older, tb+".parquet"), make([]byte, 1<<20), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		real, err := reconstruct.ListBaselines(ctx, root)
		if err != nil {
			t.Fatal(err)
		}
		for _, order := range []string{"as listed", "reversed"} {
			files := slices.Clone(real)
			if order == "reversed" {
				slices.Reverse(files)
			}
			prev := listBaselines
			listBaselines = func(context.Context, string) ([]reconstruct.BaselineFile, error) { return files, nil }
			stubDisk(t, uint64(need), nil)
			err := checkFoldDisk(ctx, root, root, at, tables, false)
			listBaselines = prev
			if err != nil {
				t.Fatalf("%s: sized an older snapshot than the one the fold starts from: %v", order, err)
			}
		}
	})
	t.Run("with reuse on, tables already shared with another backup are not counted", func(t *testing.T) {
		root := localSnapshot(t, ts, map[string]int{"a": 3000, "b": 5000})
		older := filepath.Join(root, reconstruct.SnapshotDirName(ts.Add(-time.Hour)), "shop")
		if err := os.MkdirAll(older, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(filepath.Join(root, reconstruct.SnapshotDirName(ts), "shop", "b.parquet"), filepath.Join(older, "b.parquet")); err != nil {
			t.Fatal(err)
		}
		stubDisk(t, uint64(3000+foldDiskMargin), nil)
		if err := checkFoldDisk(ctx, root, root, at, tables, true); err != nil {
			t.Fatalf("a mostly linked backup was refused with room for what it writes: %v", err)
		}
		err := checkFoldDisk(ctx, root, root, at, tables, false)
		if !errors.Is(err, errFoldDiskFull) {
			t.Fatalf("with reuse off the linked table must count: %v", err)
		}
	})
	t.Run("an S3 source is sized from its object listing", func(t *testing.T) {
		src := "s3://bucket/backups"
		dirName := reconstruct.SnapshotDirName(ts)
		prevL, prevS := listBaselines, snapshotFileSizesFn
		t.Cleanup(func() { listBaselines, snapshotFileSizesFn = prevL, prevS })
		listBaselines = func(context.Context, string) ([]reconstruct.BaselineFile, error) {
			return []reconstruct.BaselineFile{
				{SnapshotTime: ts, Schema: "shop", Table: "a", Path: src + "/" + dirName + "/shop/a.parquet"},
				{SnapshotTime: ts, Schema: "shop", Table: "b", Path: src + "/" + dirName + "/shop/b.parquet"},
			}, nil
		}
		sizes := map[string]int64{dirName + "/shop/a.parquet": 3000, dirName + "/shop/b.parquet": 5000, dirName + "/_SUCCESS": 0}
		snapshotFileSizesFn = func(_ context.Context, s, d string) (map[string]int64, error) {
			if s != src || d != dirName {
				t.Fatalf("sized %q %q", s, d)
			}
			return sizes, nil
		}
		stubDisk(t, uint64(need-1), nil)
		if err := checkFoldDisk(ctx, src, root, at, tables, true); !errors.Is(err, errFoldDiskFull) {
			t.Fatalf("an S3 backup one byte too big was waved through: %v", err)
		}
		stubDisk(t, uint64(need), nil)
		if err := checkFoldDisk(ctx, src, root, at, tables, true); err != nil {
			t.Fatalf("refused with exactly enough room: %v", err)
		}
		delete(sizes, dirName+"/shop/b.parquet")
		stubDisk(t, 1, nil)
		if err := checkFoldDisk(ctx, src, root, at, tables, true); err != nil {
			t.Fatalf("a table without a stored size must skip the check, not refuse: %v", err)
		}
	})
	t.Run("an output directory not created yet is measured on its parent", func(t *testing.T) {
		var probed string
		prev := diskSpaceFn
		diskSpaceFn = func(p string) (uint64, uint64, error) { probed = p; return uint64(need), 1 << 50, nil }
		t.Cleanup(func() { diskSpaceFn = prev })
		out := filepath.Join(root, "not", "yet")
		if err := checkFoldDisk(ctx, root, out, at, tables, false); err != nil || probed != root {
			t.Fatalf("err=%v probed=%q, want the existing parent %q", err, probed, root)
		}
	})
}

// A disk refusal is recorded so the scheduled update does not fall back to a
// full backup into the same disk; every other outcome clears it.
func TestApplyFoldStatus_diskRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"disk refusal, wrapped", errors.Join(errors.New("prefix"), errFoldDiskFull), true},
		{"another refusal", reconstruct.ErrCaptureGap, false},
		{"success", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &console.BaselineStatus{DiskRefused: !tc.want}
			applyFoldStatus(st, 1, 0, reuseTally{}, tc.err)
			if st.DiskRefused != tc.want {
				t.Fatalf("DiskRefused = %v, want %v", st.DiskRefused, tc.want)
			}
		})
	}
}

// A scheduled update refused for disk space is not answered with a full
// backup: that one would write into the same disk under capture. The fold
// itself never runs.
func TestBackupScheduler_diskRefusedUpdateDoesNotFallBack(t *testing.T) {
	var folded atomic.Bool
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folded.Store(true)
		return nil, nil, nil
	})
	stubDisk(t, 1, nil)
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	e.SourceDSN = "not a dsn" // a fallback full backup, if one started, would fail fast
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	b.watchers.Wait()
	if st.Last == nil || st.Last.State != "failed" || !st.Last.DiskRefused || !strings.Contains(st.Last.LastError, "not enough free disk") {
		t.Fatalf("the update was not refused for disk space: %+v", st.Last)
	}
	if now := b.ScheduleState(e.ID); now.LastFallbackAt != "" || now.LastMethod != console.BackupMethodRefresh {
		t.Fatalf("a full backup stood in for a disk refusal: %+v", now)
	}
	if got := sup.Status(e.ID).State; got != "idle" {
		t.Fatalf("a full backup was started after a disk refusal: state %q", got)
	}
	if folded.Load() {
		t.Fatal("the fold ran although the disk check refused")
	}
}

// The .sql backup checks the disk before each file it writes, not with an
// estimate from the compressed backup it reads (#1614).
func TestSQLExport_checksDiskPerFile(t *testing.T) {
	var space func(string, int64) error
	holdFold(t, func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		space = cfg.ChunkSpace
		return nil, nil, nil
	})
	_, _, sup := newScheduleFixture(t, true)
	root := t.TempDir()
	writeFakeSnapshot(t, root)
	dir := filepath.Join(t.TempDir(), "srv", "build")
	stubDisk(t, 1, nil)
	if _, _, _, err := sup.executeSQLExport(console.SQLExportRequest{ServerID: "srv", BaselineSrc: root, At: time.Now().UTC()}, dir); errors.Is(err, errFoldDiskFull) {
		t.Fatalf("the .sql backup refused up front from an estimate: %v", err)
	}
	if space == nil {
		t.Fatal("the .sql backup's fold has no per-file disk check")
	}
	if err := space(dir, 256<<20); !errors.Is(err, errFoldDiskFull) || !strings.Contains(err.Error(), "256.0 MiB") {
		t.Fatalf("a file that does not fit: err = %v", err)
	}
	stubDisk(t, uint64(256<<20), nil)
	if err := space(dir, 256<<20); !errors.Is(err, errFoldDiskFull) {
		t.Fatalf("room for the file but not the margin kept free: err = %v", err)
	}
	stubDisk(t, uint64(256<<20+foldDiskMargin), nil)
	if err := space(dir, 256<<20); err != nil {
		t.Fatalf("a file that fits: %v", err)
	}
	stubDiskTotal(t, 0, 0, nil)
	if err := space(dir, 256<<20); err != nil {
		t.Fatalf("a mount that cannot answer: %v", err)
	}
}
