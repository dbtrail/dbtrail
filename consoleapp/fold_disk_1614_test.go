package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
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

func TestDiskSpaceCheck(t *testing.T) {
	dir := t.TempDir()
	const need = 8000
	full := uint64(need + foldDiskMargin)

	t.Run("room for it", func(t *testing.T) {
		stubDisk(t, full, nil)
		if err := newDiskSpaceCheck()(dir, need); err != nil {
			t.Fatalf("refused with exactly enough room: %v", err)
		}
	})
	t.Run("one byte short refuses and names the directory and both numbers", func(t *testing.T) {
		stubDisk(t, full-1, nil)
		err := newDiskSpaceCheck()(dir, need)
		if !errors.Is(err, errFoldDiskFull) || !strings.Contains(err.Error(), dir) ||
			!strings.Contains(err.Error(), "7.8 KiB") || !strings.Contains(err.Error(), "1.0 GiB kept free") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("numbers that round alike are given in bytes", func(t *testing.T) {
		stubDisk(t, full-1, nil)
		err := newDiskSpaceCheck()(dir, need)
		if !strings.Contains(err.Error(), fmt.Sprintf("%d bytes free", full-1)) ||
			!strings.Contains(err.Error(), fmt.Sprintf("%d bytes in all", full)) {
			t.Fatalf("free and needed render the same, so the refusal reads as a contradiction: %v", err)
		}
	})
	t.Run("a full disk refuses", func(t *testing.T) {
		stubDisk(t, 0, nil)
		if err := newDiskSpaceCheck()(dir, need); !errors.Is(err, errFoldDiskFull) {
			t.Fatalf("a disk with zero free and a real size was waved through: %v", err)
		}
	})
	t.Run("a mount that reports no size proceeds", func(t *testing.T) {
		stubDiskTotal(t, 0, 0, nil)
		if err := newDiskSpaceCheck()(dir, need); err != nil {
			t.Fatalf("refused on a mount that cannot answer: %v", err)
		}
	})
	t.Run("a probe error proceeds", func(t *testing.T) {
		stubDisk(t, 1, errors.New("statfs: not supported"))
		if err := newDiskSpaceCheck()(dir, need); err != nil {
			t.Fatalf("refused on a probe error: %v", err)
		}
	})
	t.Run("a directory not created yet is measured on its parent", func(t *testing.T) {
		var probed string
		prev := diskSpaceFn
		diskSpaceFn = func(p string) (uint64, uint64, error) { probed = p; return full, 1 << 50, nil }
		t.Cleanup(func() { diskSpaceFn = prev })
		if err := newDiskSpaceCheck()(filepath.Join(dir, "not", "yet"), need); err != nil || probed != dir {
			t.Fatalf("err=%v probed=%q, want the existing parent %q", err, probed, dir)
		}
	})
	t.Run("an unmeasurable disk is logged once per build, at a level an operator sees", func(t *testing.T) {
		prev := slog.Default()
		t.Cleanup(func() { slog.SetDefault(prev) })
		var buf bytes.Buffer
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		stubDiskTotal(t, 0, 0, nil)
		check := newDiskSpaceCheck()
		for range 3 {
			_ = check(dir, need)
		}
		if n := strings.Count(buf.String(), "cannot be measured"); n != 1 {
			t.Fatalf("one build logged the skip %d times, want 1:\n%s", n, buf.String())
		}
		_ = newDiskSpaceCheck()(dir, need)
		if n := strings.Count(buf.String(), "cannot be measured"); n != 2 {
			t.Fatalf("the next build did not log its own skip: %d lines", n)
		}
	})
}

func TestHumanSize(t *testing.T) {
	for _, tc := range []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1<<20 - 1, "1.0 MiB"},
		{256 << 20, "256.0 MiB"},
		{1<<30 - 1, "1.0 GiB"},
		{1 << 30, "1.0 GiB"},
	} {
		if got := humanSize(tc.in); got != tc.want {
			t.Errorf("humanSize(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// Both daemon builds that write a backup from recorded changes carry the check,
// each with its own log-once state.
func TestFoldConfigs_carryTheDiskCheck(t *testing.T) {
	stubDisk(t, 1, nil)
	refresh := refreshFoldConfig(refreshRequest{IndexDSN: "dsn", BaselineDir: "/b"}, time.Now(), []string{"shop.orders"})
	export := sqlExportFoldConfig(console.SQLExportRequest{IndexDSN: "dsn", BaselineSrc: "/b"}, "/out", []string{"shop.orders"})
	for name, cfg := range map[string]reconstruct.FullTableConfig{"refresh and restore": refresh, ".sql backup": export} {
		if cfg.SpaceCheck == nil {
			t.Errorf("%s: no disk check", name)
			continue
		}
		if err := cfg.SpaceCheck(t.TempDir(), 1); !errors.Is(err, errFoldDiskFull) {
			t.Errorf("%s: a full disk was waved through: %v", name, err)
		}
	}
}

// A disk refusal is recorded so the scheduled update does not fall back to a
// full backup into the same disk; every other outcome clears it. A disk that
// filled while the build was writing counts too.
func TestApplyFoldStatus_diskRefused(t *testing.T) {
	enospc := &fs.PathError{Op: "write", Path: "/b/x.parquet", Err: syscall.ENOSPC}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"disk refusal, wrapped", errors.Join(errors.New("prefix"), errFoldDiskFull), true},
		{"disk filled mid-build", errors.Join(errors.New("shop.a"), fmt.Errorf("write row: %w", enospc)), true},
		{"another write error", &fs.PathError{Op: "write", Path: "/b/x.parquet", Err: syscall.EIO}, false},
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
// backup: that one would write into the same disk under capture. The refusal
// comes from inside the fold, through the check the refresh configuration
// carries, or from a disk that filled while writing.
func TestBackupScheduler_diskRefusedUpdateDoesNotFallBack(t *testing.T) {
	for _, tc := range []struct {
		name string
		fold func(reconstruct.FullTableConfig) error
		want string
	}{
		{"the check refuses a table", func(cfg reconstruct.FullTableConfig) error {
			if cfg.SpaceCheck == nil {
				return errors.New("the refresh fold has no disk check")
			}
			return fmt.Errorf("shop.orders: %w", cfg.SpaceCheck(cfg.OutputDir, 1))
		}, "not enough free disk"},
		{"the disk fills while writing", func(reconstruct.FullTableConfig) error {
			return fmt.Errorf("shop.orders: write row: %w", &fs.PathError{Op: "write", Path: "/b/x", Err: syscall.ENOSPC})
		}, "no space left"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var folded atomic.Bool
			holdFold(t, func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
				folded.Store(true)
				return nil, []reconstruct.TableFailure{{Table: "shop.orders"}}, tc.fold(cfg)
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
			if !folded.Load() {
				t.Fatal("the fold never ran, so nothing here reached the check it carries")
			}
			if st.Last == nil || st.Last.State != "failed" || !st.Last.DiskRefused || !strings.Contains(st.Last.LastError, tc.want) {
				t.Fatalf("the update was not refused for disk space: %+v", st.Last)
			}
			if now := b.ScheduleState(e.ID); now.LastFallbackAt != "" || now.LastMethod != console.BackupMethodRefresh {
				t.Fatalf("a full backup stood in for a disk refusal: %+v", now)
			}
			if got := sup.Status(e.ID).State; got != "idle" {
				t.Fatalf("a full backup was started after a disk refusal: state %q", got)
			}
		})
	}
}

// The .sql backup checks the disk before each file it writes, not with an
// estimate from the compressed backup it reads (#1614).
func TestSQLExport_checksDiskPerFile(t *testing.T) {
	var space func(string, int64) error
	holdFold(t, func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		space = cfg.SpaceCheck
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
