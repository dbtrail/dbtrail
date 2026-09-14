package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// stubDisk swaps the free-space probe for the test.
func stubDisk(t *testing.T, free uint64, err error) {
	t.Helper()
	prev := diskFreeFn
	diskFreeFn = func(string) (uint64, error) { return free, err }
	t.Cleanup(func() { diskFreeFn = prev })
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

	t.Run("room for it", func(t *testing.T) {
		stubDisk(t, uint64(need), nil)
		if err := checkFoldDisk(context.Background(), root, root, at, tables); err != nil {
			t.Fatalf("refused with exactly enough room: %v", err)
		}
	})
	t.Run("one byte short refuses and names both numbers", func(t *testing.T) {
		stubDisk(t, uint64(need-1), nil)
		err := checkFoldDisk(context.Background(), root, root, at, tables)
		if !errors.Is(err, errFoldDiskFull) || !strings.Contains(err.Error(), root) ||
			!strings.Contains(err.Error(), "2026-09-01 06:00:00") || !strings.Contains(err.Error(), "7.8 KiB") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("only the tables the fold writes count", func(t *testing.T) {
		stubDisk(t, uint64(3000+foldDiskMargin), nil)
		if err := checkFoldDisk(context.Background(), root, root, at, []string{"shop.a"}); err != nil {
			t.Fatalf("counted a table the fold does not write: %v", err)
		}
	})
	t.Run("a zero reading proceeds", func(t *testing.T) {
		stubDisk(t, 0, nil)
		if err := checkFoldDisk(context.Background(), root, root, at, tables); err != nil {
			t.Fatalf("refused on an unreliable zero: %v", err)
		}
	})
	t.Run("a probe error proceeds", func(t *testing.T) {
		stubDisk(t, 1, errors.New("statfs: not supported"))
		if err := checkFoldDisk(context.Background(), root, root, at, tables); err != nil {
			t.Fatalf("refused on a probe error: %v", err)
		}
	})
	t.Run("an S3 source is not sized and proceeds", func(t *testing.T) {
		stubDisk(t, 1, nil)
		if err := checkFoldDisk(context.Background(), "s3://bucket/backups", root, at, tables); err != nil {
			t.Fatalf("refused without an estimate: %v", err)
		}
	})
	t.Run("the snapshot the fold starts from, not a newer one", func(t *testing.T) {
		stubDisk(t, uint64(8000+foldDiskMargin), nil)
		newer := filepath.Join(root, reconstruct.SnapshotDirName(at.Add(time.Hour)), "shop")
		if err := os.MkdirAll(newer, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(newer, "a.parquet"), make([]byte, 1<<20), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := checkFoldDisk(context.Background(), root, root, at, tables); err != nil {
			t.Fatalf("sized a snapshot newer than the target: %v", err)
		}
	})
	t.Run("an output directory not created yet is measured on its parent", func(t *testing.T) {
		var probed string
		prev := diskFreeFn
		diskFreeFn = func(p string) (uint64, error) { probed = p; return uint64(need), nil }
		t.Cleanup(func() { diskFreeFn = prev })
		out := filepath.Join(root, "not", "yet")
		if err := checkFoldDisk(context.Background(), root, out, at, tables); err != nil || probed != root {
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

// The SQL export checks the disk before it folds too.
func TestExecuteSQLExport_refusesOnDiskBeforeFolding(t *testing.T) {
	var folded atomic.Bool
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folded.Store(true)
		return nil, nil, nil
	})
	stubDisk(t, 1, nil)
	_, _, sup := newScheduleFixture(t, true)
	root := t.TempDir()
	writeFakeSnapshot(t, root)
	dir := filepath.Join(t.TempDir(), "srv", "build")
	_, _, _, err := sup.executeSQLExport(console.SQLExportRequest{ServerID: "srv", BaselineSrc: root, At: time.Now().UTC()}, dir)
	if !errors.Is(err, errFoldDiskFull) {
		t.Fatalf("err = %v, want the disk refusal", err)
	}
	if folded.Load() {
		t.Fatal("the fold ran although the disk check refused")
	}
}
