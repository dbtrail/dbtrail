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

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #1938: a full read writes the whole dump to the staging folder before it
// becomes Parquet. The check before mydumper refuses a disk that cannot hold
// the dump, warns below the dump-plus-Parquet peak, and never refuses on a
// guess.

const gib = uint64(1) << 30

// diskByPath answers diskSpaceFn per directory, so a test can tell which
// folder was measured. An unknown path fails the test: measuring somewhere
// the dump does not go is the defect this guards.
func diskByPath(t *testing.T, free map[string]uint64) {
	t.Helper()
	prev := diskSpaceFn
	diskSpaceFn = func(p string) (uint64, uint64, error) {
		f, ok := free[p]
		if !ok {
			t.Errorf("free space measured at %q, which no part of the full read writes to", p)
			return 0, 0, errors.New("unexpected path")
		}
		return f, 1 << 50, nil
	}
	t.Cleanup(func() { diskSpaceFn = prev })
}

func stubSameFS(t *testing.T, same bool, err error) {
	t.Helper()
	prev := sameFilesystemFn
	sameFilesystemFn = func(string, string) (bool, error) { return same, err }
	t.Cleanup(func() { sameFilesystemFn = prev })
}

func stubEstimate(t *testing.T, est dumpEstimate, err error) {
	t.Helper()
	prev := dumpSizeEstimateFn
	dumpSizeEstimateFn = func(context.Context, string, []string) (dumpEstimate, error) { return est, err }
	t.Cleanup(func() { dumpSizeEstimateFn = prev })
}

func TestDumpDiskVerdict(t *testing.T) {
	stage := t.TempDir()
	est := dumpEstimate{bytes: int64(10 * gib), tables: 3}

	// The estimate is an upper bound (secondary indexes are not dumped,
	// deleted rows still count), and the smallest dump the #1938 measurement
	// saw was well under it, so only less than half of it refuses.
	t.Run("below half the estimate refuses and says how to move the folder", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 4 * gib})
		_, _, err := dumpDiskVerdict(stage, "", est, nil)
		if !errors.Is(err, errFoldDiskFull) {
			t.Fatalf("err = %v, want a disk refusal", err)
		}
		for _, want := range []string{stage, "10.0 GiB", "4.0 GiB", "5.0 GiB", "upper bound", "BINTRAIL_CONSOLE_BASELINE_STAGING", `".sql build folder"`, "restart"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal lacks %q: %v", want, err)
			}
		}
	})
	t.Run("exactly half the estimate is not a refusal", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 5 * gib})
		check, _, err := dumpDiskVerdict(stage, "", est, nil)
		if err != nil || check != dumpDiskLow {
			t.Fatalf("check = %q, err = %v, want low and no refusal", check, err)
		}
	})
	t.Run("between half and the estimate warns with everything the refusal says", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 9 * gib})
		check, note, err := dumpDiskVerdict(stage, "", est, nil)
		if err != nil || check != dumpDiskLow {
			t.Fatalf("check = %q, err = %v, want low and no refusal", check, err)
		}
		for _, want := range []string{"Low disk", stage, "10.0 GiB", "9.0 GiB free", "upper bound", "BINTRAIL_CONSOLE_BASELINE_STAGING", `".sql build folder"`, "restart"} {
			if !strings.Contains(note, want) {
				t.Errorf("warning lacks %q: %s", want, note)
			}
		}
	})
	t.Run("between 1x and 1.8x runs and warns", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 15 * gib})
		check, note, err := dumpDiskVerdict(stage, "", est, nil)
		if err != nil || check != dumpDiskLow {
			t.Fatalf("check = %q, err = %v", check, err)
		}
		for _, want := range []string{"Low disk", stage, "15.0 GiB free", "about 10.0 GiB", "about 18.0 GiB", "BINTRAIL_CONSOLE_BASELINE_STAGING"} {
			if !strings.Contains(note, want) {
				t.Errorf("note lacks %q: %s", want, note)
			}
		}
	})
	t.Run("room for the peak is ok and still says the numbers", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 18 * gib})
		check, note, err := dumpDiskVerdict(stage, "", est, nil)
		if err != nil || check != dumpDiskOK || !strings.Contains(note, "10.0 GiB") || !strings.Contains(note, "18.0 GiB free") {
			t.Fatalf("check = %q, note = %q, err = %v", check, note, err)
		}
	})
	t.Run("estimate query error goes ahead and says the check did not run", func(t *testing.T) {
		diskByPath(t, map[string]uint64{})
		check, note, err := dumpDiskVerdict(stage, "", dumpEstimate{}, errors.New("Error 1142: SELECT command denied\nsecond line"))
		if err != nil || check != dumpDiskUnchecked || !strings.Contains(note, "did not run") ||
			!strings.Contains(note, "SELECT command denied") || strings.Contains(note, "second line") {
			t.Fatalf("check = %q, note = %q, err = %v", check, note, err)
		}
	})
	t.Run("unmeasurable disk goes ahead", func(t *testing.T) {
		prev := diskSpaceFn
		t.Cleanup(func() { diskSpaceFn = prev })
		for _, probe := range []func(string) (uint64, uint64, error){
			func(string) (uint64, uint64, error) { return 0, 0, errors.New("statfs: no") },
			func(string) (uint64, uint64, error) { return 0, 0, nil },
		} {
			diskSpaceFn = probe
			check, note, err := dumpDiskVerdict(stage, "", est, nil)
			if err != nil || check != dumpDiskUnchecked || !strings.Contains(note, stage) {
				t.Fatalf("check = %q, note = %q, err = %v", check, note, err)
			}
		}
	})
	t.Run("zero tables says nothing matched instead of needing 0 B", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 0})
		check, note, err := dumpDiskVerdict(stage, "", dumpEstimate{}, nil)
		if err != nil || check != dumpDiskOK || !strings.Contains(note, "no tables match") {
			t.Fatalf("check = %q, note = %q, err = %v", check, note, err)
		}
	})
	t.Run("sizes the source would not refresh are named", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 100 * gib})
		_, note, _ := dumpDiskVerdict(stage, "", dumpEstimate{bytes: 1, tables: 1, stale: true}, nil)
		if !strings.Contains(note, "up to a day old") {
			t.Fatalf("note = %q", note)
		}
	})
	t.Run("tables without a size are named", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 100 * gib})
		_, note, _ := dumpDiskVerdict(stage, "", dumpEstimate{bytes: 1, tables: 4, unsized: 2}, nil)
		if !strings.Contains(note, "no size for 2 table(s)") {
			t.Fatalf("note = %q", note)
		}
	})
}

// The dump goes to the staging folder whatever the destination. With a local
// folder on another disk, the staging folder only has to hold the dump, and
// the other disk is warned about, never refused.
func TestDumpDiskVerdict_localFolderOnAnotherDisk(t *testing.T) {
	stage, local := t.TempDir(), t.TempDir()
	est := dumpEstimate{bytes: int64(10 * gib), tables: 3}
	stubSameFS(t, false, nil)

	t.Run("small staging with a huge local folder refuses", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 4 * gib, local: 1000 * gib})
		if _, _, err := dumpDiskVerdict(stage, local, est, nil); !errors.Is(err, errFoldDiskFull) || !strings.Contains(err.Error(), stage) {
			t.Fatalf("err = %v, want a refusal naming the staging folder", err)
		}
	})
	t.Run("staging below the estimate warns, naming the staging folder", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 7 * gib, local: 1000 * gib})
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		if err != nil || check != dumpDiskLow || !strings.Contains(note, stage) || !strings.Contains(note, "BINTRAIL_CONSOLE_BASELINE_STAGING") {
			t.Fatalf("check = %q, note = %q, err = %v", check, note, err)
		}
	})
	t.Run("the dump fits at staging and needs no Parquet margin there", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 11 * gib, local: 1000 * gib})
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		if err != nil || check != dumpDiskOK || !strings.Contains(note, stage) {
			t.Fatalf("check = %q, note = %q, err = %v", check, note, err)
		}
	})
	t.Run("a small local folder is a warning naming it", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 1000 * gib, local: 5 * gib})
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		if err != nil || check != dumpDiskLow || !strings.Contains(note, local) {
			t.Fatalf("check = %q, note = %q, err = %v", check, note, err)
		}
	})
	t.Run("an unmeasurable local folder makes the check unfinished, not ok", func(t *testing.T) {
		prev := diskSpaceFn
		t.Cleanup(func() { diskSpaceFn = prev })
		diskSpaceFn = func(p string) (uint64, uint64, error) {
			if p == local {
				return 0, 0, errors.New("statfs: no")
			}
			return 1000 * gib, 1 << 50, nil
		}
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		if err != nil || check != dumpDiskUnchecked || !strings.Contains(note, local) {
			t.Fatalf("check = %q, note = %q, err = %v", check, note, err)
		}
	})
	t.Run("a local folder not created yet is measured on its parent", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 1000 * gib, local: 1000 * gib})
		check, _, err := dumpDiskVerdict(stage, filepath.Join(local, "not", "yet"), est, nil)
		if err != nil || check != dumpDiskOK {
			t.Fatalf("check = %q, err = %v", check, err)
		}
	})
}

// Same disk, or a disk whose identity cannot be read: the dump and the
// Parquet share it, so the 1.8x margin applies.
func TestDumpDiskVerdict_localFolderOnTheSameDisk(t *testing.T) {
	stage, local := t.TempDir(), t.TempDir()
	est := dumpEstimate{bytes: int64(10 * gib), tables: 1}
	t.Run("same", func(t *testing.T) {
		stubSameFS(t, true, nil)
		diskByPath(t, map[string]uint64{stage: 12 * gib})
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		if err != nil || check != dumpDiskLow || !strings.Contains(note, "copy on the same disk") {
			t.Fatalf("check = %q, note = %q, err = %v, want low", check, note, err)
		}
	})
	// Unknown asks for the larger margin, says it is a guess, and still
	// measures the snapshot folder on its own.
	t.Run("unknown", func(t *testing.T) {
		stubSameFS(t, false, errors.New("no"))
		diskByPath(t, map[string]uint64{stage: 12 * gib, local: 1000 * gib})
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		if err != nil || check != dumpDiskLow || !strings.Contains(note, "could not tell") {
			t.Fatalf("check = %q, note = %q, err = %v, want low", check, note, err)
		}
	})
	t.Run("unknown with a small snapshot folder", func(t *testing.T) {
		stubSameFS(t, false, errors.New("no"))
		diskByPath(t, map[string]uint64{stage: 1000 * gib, local: 5 * gib})
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		if err != nil || check != dumpDiskLow || !strings.Contains(note, local) {
			t.Fatalf("check = %q, note = %q, err = %v, want low naming the snapshot folder", check, note, err)
		}
	})
}

func TestSameFilesystem_realPaths(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	same, err := sameFilesystem(a, b)
	if err != nil || !same {
		t.Fatalf("two temp dirs: same = %v, err = %v", same, err)
	}
	if _, err := sameFilesystem(filepath.Join(a, "missing"), b); err == nil {
		t.Fatal("a missing path reported a device")
	}
}

// The estimate selects exactly the tables the dump selects: every
// non-system schema for an empty list, the names verbatim otherwise.
func TestDumpSizeQuery(t *testing.T) {
	q, args := dumpSizeQuery(nil)
	if !strings.Contains(q, "TABLE_SCHEMA NOT IN ('mysql','sys','performance_schema','information_schema')") || len(args) != 0 {
		t.Fatalf("empty list: %s %v", q, args)
	}
	if !strings.Contains(q, "COALESCE(SUM(") || !strings.Contains(q, "INDEX_LENGTH") || !strings.Contains(q, "'BASE TABLE', 'SYSTEM VERSIONED'") {
		t.Fatalf("query = %s", q)
	}
	_, args = dumpSizeQuery([]string{"Shop", " b "})
	if len(args) != 2 || args[0] != "Shop" || args[1] != " b " {
		t.Fatalf("names were changed on the way: %v", args)
	}
	count, _ := dumpableTableCountQuery([]string{"a"})
	if !strings.HasSuffix(count, "TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED') AND TABLE_SCHEMA IN (?)") {
		t.Fatalf("count query = %s", count)
	}
}

// countMydumper replaces the dump with a counter that fails, so a test sees
// whether mydumper would have started.
func countMydumper(t *testing.T) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	prev := runMydumperFunc
	runMydumperFunc = func(context.Context, string, []string, string, baseline.LockMode, lockModeSource) error {
		n.Add(1)
		return errors.New("fake mydumper stopped")
	}
	t.Cleanup(func() { runMydumperFunc = prev })
	return &n
}

// The refusal comes before anything is created or dumped.
func TestExecute_diskRefusalStopsBeforeMydumper(t *testing.T) {
	stage := t.TempDir()
	stubEstimate(t, dumpEstimate{bytes: int64(10 * gib), tables: 1}, nil)
	diskByPath(t, map[string]uint64{stage: gib})
	calls := countMydumper(t)
	var marks atomic.Int32
	prevMark := dumpDDLMarkFunc
	dumpDDLMarkFunc = func(console.BaselineRequest) string { marks.Add(1); return "" }
	t.Cleanup(func() { dumpDDLMarkFunc = prevMark })
	// A staging folder nothing can be created in: a check placed after the
	// dump folder is made fails on that first, and the deferred removal of a
	// folder that was made cannot hide it.
	if err := os.Chmod(stage, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stage, 0o755) })
	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
	_, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: "src", S3: "s3://b/p"})
	if !errors.Is(err, errFoldDiskFull) {
		t.Fatalf("err = %v, want the disk refusal before any folder is created", err)
	}
	if calls.Load() != 0 || marks.Load() != 0 {
		t.Fatalf("after the refusal: mydumper ran %d time(s), the DDL mark was read %d time(s)", calls.Load(), marks.Load())
	}
}

// runFullRead drives Trigger through the real execute and waits for the end.
func runFullRead(t *testing.T, s *baselineSupervisor, req console.BaselineRequest) console.BaselineStatus {
	t.Helper()
	if err := s.Trigger(req); err != nil {
		t.Fatal(err)
	}
	return waitForTerminalState(t, func() console.BaselineStatus { return s.Status(req.ServerID) })
}

func supWithHistory(t *testing.T, stage string) *baselineSupervisor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := newBaselineSupervisor(ctx, stage, baseline.LockModeFTWRL)
	h, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	s.history = h
	return s
}

// S3 only: the Parquet is staged in the same folder as the dump, so the
// peak margin applies there. The warning reaches the status and survives a
// later failure into the history.
func TestFullRead_S3OnlyLowDiskWarnsInStatusAndHistory(t *testing.T) {
	stage := t.TempDir()
	stubEstimate(t, dumpEstimate{bytes: int64(10 * gib), tables: 2}, nil)
	diskByPath(t, map[string]uint64{stage: 12 * gib})
	calls := countMydumper(t)
	s := supWithHistory(t, stage)
	st := runFullRead(t, s, console.BaselineRequest{ServerID: "s1", ServerName: "wp", SourceDSN: "src", S3: "s3://b/p"})
	if calls.Load() != 1 {
		t.Fatalf("mydumper ran %d times, want 1: a low disk is a warning", calls.Load())
	}
	if st.State != "failed" || !strings.Contains(st.LastError, "fake mydumper stopped") {
		t.Fatalf("status = %+v", st)
	}
	if st.DiskCheck != dumpDiskLow || !strings.Contains(st.DiskNote, "Low disk") || st.DiskRefused {
		t.Fatalf("status lost the warning: %+v", st)
	}
	runs := s.history.List("s1")
	if len(runs) != 1 {
		t.Fatalf("history = %+v", runs)
	}
	if runs[0].DiskCheck != dumpDiskLow || runs[0].DiskNote != st.DiskNote {
		t.Fatalf("history record = %+v, want the warning kept", runs[0])
	}
}

// An estimate that could not be read never refuses: the read goes ahead,
// and the status says the check did not run.
func TestFullRead_estimateErrorGoesAheadAndSaysSo(t *testing.T) {
	stage := t.TempDir()
	stubEstimate(t, dumpEstimate{}, errors.New("dial tcp: connection refused"))
	diskByPath(t, map[string]uint64{stage: 0})
	calls := countMydumper(t)
	s := supWithHistory(t, stage)
	st := runFullRead(t, s, console.BaselineRequest{ServerID: "s1", SourceDSN: "src", LocalDir: t.TempDir()})
	if calls.Load() != 1 || st.DiskCheck != dumpDiskUnchecked || !strings.Contains(st.DiskNote, "connection refused") {
		t.Fatalf("calls = %d, status = %+v", calls.Load(), st)
	}
}

// A refused full read is a failed run with DiskRefused on the status, the
// schedule slot and the history.
func TestFullRead_refusalIsDiskRefused(t *testing.T) {
	stage := t.TempDir()
	stubEstimate(t, dumpEstimate{bytes: int64(10 * gib), tables: 2}, nil)
	diskByPath(t, map[string]uint64{stage: gib})
	calls := countMydumper(t)
	s := supWithHistory(t, stage)
	st := runFullRead(t, s, console.BaselineRequest{ServerID: "s1", SourceDSN: "src", S3: "s3://b/p"})
	if calls.Load() != 0 || st.State != "failed" || !st.DiskRefused || !strings.Contains(st.LastError, stage) {
		t.Fatalf("calls = %d, status = %+v", calls.Load(), st)
	}
	runs := s.history.List("s1")
	if len(runs) != 1 || !strings.Contains(runs[0].Error, "BINTRAIL_CONSOLE_BASELINE_STAGING") {
		t.Fatalf("history = %+v", runs)
	}
}

// A scheduled full read refused for disk shows the refusal on its schedule
// slot, and nothing stands in for it with another full read.
func TestBackupScheduler_diskRefusedFullReadIsOnItsSlot(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	var estimates atomic.Int32
	prevEst := dumpSizeEstimateFn
	dumpSizeEstimateFn = func(context.Context, string, []string) (dumpEstimate, error) {
		estimates.Add(1)
		return dumpEstimate{bytes: int64(10 * gib), tables: 2}, nil
	}
	t.Cleanup(func() { dumpSizeEstimateFn = prevEst })
	diskByPath(t, map[string]uint64{sup.stagingDir: gib})
	calls := countMydumper(t)
	e := addScheduled(t, reg, false)
	fireAt(b, time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	st := waitTerminal(t, b, e.ID)
	if st.LastMethod != console.BackupMethodFull || st.Last == nil || st.Last.State != "failed" || !st.Last.DiskRefused ||
		!strings.Contains(st.Last.LastError, "Nothing was dumped") {
		t.Fatalf("slot = %+v last = %+v", st, st.Last)
	}
	if calls.Load() != 0 {
		t.Fatal("mydumper ran")
	}
	// Every full read starts with the estimate, so a second one means a
	// second full read. Give the watcher several polls to start one.
	time.Sleep(20 * fallbackPoll)
	b.watchers.Wait()
	if n := estimates.Load(); n != 1 {
		t.Fatalf("%d full reads started, want only the refused one", n)
	}
	runs := sup.history.List(e.ID)
	if len(runs) != 1 {
		t.Fatalf("history has %d runs, want the one refused full read: %+v", len(runs), runs)
	}
}
