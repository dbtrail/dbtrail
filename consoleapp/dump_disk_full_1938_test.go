package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #1938: a working folder that fills DURING the dump. The disk check before
// the dump works from an estimate, so a read it let through can still fill
// the disk, and that failure read "mydumper failed: exit status 1; output:
// ..." with no mention of the disk.

// mydumperDiskFullOutput is what mydumper 1.0.3-1 printed, with the flags
// the console passes, when a 150 MB ext4 filesystem filled under a 318 MB
// dump (measured on a real filesystem; the same lines on tmpfs). It exits 1
// only after every table, so the three lines repeat per failed file.
const mydumperDiskFullOutput = `
** (mydumper:1): WARNING **: 13:09:32.940: Using --trx-tables options, binlog coordinates will not be accurate if you are writing to non transactional tables.

** (mydumper:1): CRITICAL **: 13:09:38.470: Couldn't write data to a file(13): No space left on device

** (mydumper:1): CRITICAL **: 13:09:38.470: Could not write out data for shop.big

** (mydumper:1): CRITICAL **: 13:09:38.470: Fail to write on /out/dump/shop.big.00000.sql
`

// failingMydumperOnPath puts a mydumper on PATH that answers --version and
// fails a dump with output on stderr and exit status 1.
func failingMydumperOnPath(t *testing.T, output string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "out"), []byte(output), 0o644); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo 'mydumper v1.0.3-1, built against MariaDB 10.11.18 with SSL support'; exit 0; fi\n" +
		"cat " + filepath.Join(dir, "out") + " >&2\n" +
		"exit 1\n"
	if err := os.WriteFile(filepath.Join(dir, "mydumper"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// Through runMydumper itself: the words are read once, where the output is in
// hand, and become a type. Nothing downstream matches text.
func TestRunMydumper_marksAnOutputThatSaysNoSpace_1938(t *testing.T) {
	stubUserSchemas(t, []string{"shop"})
	cases := []struct {
		name, output string
		want         bool
	}{
		{"the real output of a full disk", mydumperDiskFullOutput, true},
		{"another failure", "\n** (mydumper:1): CRITICAL **: 10:00:00.000: Error connecting to database: Access denied for user 'src'@'%'\n", false},
		{"the same words in another case are not the kernel's", "** (mydumper:1): CRITICAL **: no space left on DEVICE\n", false},
		{"no output at all", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			failingMydumperOnPath(t, c.output)
			err := runMydumper(context.Background(), wiringDSN, config.SSL{}, nil, t.TempDir(), baseline.LockModeNoLock, lockModeFromEnv)
			if err == nil {
				t.Fatal("a mydumper that exits 1 was reported as a success")
			}
			var ns *mydumperNoSpaceError
			if got := errors.As(err, &ns); got != c.want {
				t.Fatalf("marked as no-space = %v, want %v: %v", got, c.want, err)
			}
			// The message itself is what it was: the mark adds no words.
			if !strings.HasPrefix(err.Error(), "mydumper failed: exit status 1") {
				t.Fatalf("message changed: %v", err)
			}
		})
	}
}

var errNoSpaceDump = &mydumperNoSpaceError{err: errors.New("mydumper failed: exit status 1; output: ** (mydumper:1): CRITICAL **: Couldn't write data to a file(13): No space left on device")}

// stubFailedDump makes the dump write a file into its folder and then fail
// with err. sawDump reports, from inside the free-space measurement, whether
// a dump folder still existed under stage.
func stubFailedDump(t *testing.T, stage string, err error, free uint64, measureErr error, total uint64) (measuredWithDump *atomic.Int32) {
	t.Helper()
	prev := runMydumperFunc
	runMydumperFunc = func(_ context.Context, _ string, _ config.SSL, _ []string, dir string, _ baseline.LockMode, _ lockModeSource) error {
		if werr := os.WriteFile(filepath.Join(dir, "shop.big.00000.sql"), []byte("INSERT"), 0o644); werr != nil {
			t.Errorf("the fake dump could not write: %v", werr)
		}
		return err
	}
	t.Cleanup(func() { runMydumperFunc = prev })
	measuredWithDump = new(atomic.Int32)
	prevDisk := diskSpaceFn
	diskSpaceFn = func(p string) (uint64, uint64, error) {
		if p != stage {
			t.Errorf("free space measured at %q, want the working folder %q", p, stage)
		}
		if dumps, _ := filepath.Glob(filepath.Join(stage, "dump-*")); len(dumps) > 0 {
			measuredWithDump.Add(1)
		}
		return free, total, measureErr
	}
	t.Cleanup(func() { diskSpaceFn = prevDisk })
	prevDDL, prevEv := dumpDDLMarkFunc, dumpEventMarkFunc
	dumpDDLMarkFunc = func(console.BaselineRequest) string { return "" }
	dumpEventMarkFunc = func(console.BaselineRequest) string { return "" }
	t.Cleanup(func() { dumpDDLMarkFunc, dumpEventMarkFunc = prevDDL, prevEv })
	return measuredWithDump
}

func TestDumpAttempt_aWorkingFolderThatFilledIsSaidAsOne_1938(t *testing.T) {
	const total = 1 << 40
	plain := errors.New("mydumper failed: exit status 1; output: Access denied")
	cases := []struct {
		name       string
		dumpErr    error
		free       uint64
		measureErr error
		total      uint64
		wantFull   bool
	}{
		{"no space said, and the folder has none", errNoSpaceDump, 0, nil, total, true},
		{"no space said, a few bytes left (a filesystem that keeps a reserve)", errNoSpaceDump, dumpFullFreeBelow - 1, nil, total, true},
		// At the limit and above, the folder had room: the words came from
		// somewhere else (the source's own disk says the same).
		{"no space said, the folder has room at the limit", errNoSpaceDump, dumpFullFreeBelow, nil, total, false},
		{"no space said, the folder has plenty", errNoSpaceDump, 50 << 30, nil, total, false},
		{"no space said, free space cannot be measured", errNoSpaceDump, 0, errors.New("statfs: permission denied"), total, false},
		{"no space said, the filesystem reports no size", errNoSpaceDump, 0, nil, 0, false},
		{"another failure on a full folder", plain, 0, nil, total, false},
		{"mydumper's refusal of the read lock on a full folder", errDeniedFTWRL, 0, nil, total, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stage := t.TempDir()
			measured := stubFailedDump(t, stage, c.dumpErr, c.free, c.measureErr, c.total)
			s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
			_, err := s.dumpAttempt(console.BaselineRequest{ServerID: "s1", ServerName: "shop-db", SourceDSN: "src"}, baseline.LockModeFTWRL, lockModeFromEnv)
			if err == nil {
				t.Fatal("a failed dump was reported as a success")
			}
			t.Logf("error: %v", err)
			// Whatever the verdict, the failed dump's folder is gone.
			if left, _ := filepath.Glob(filepath.Join(stage, "dump-*")); len(left) != 0 {
				t.Fatalf("the failed dump's folder was kept: %v", left)
			}
			if got := foldDiskRefused(err); got != c.wantFull {
				t.Fatalf("disk failure = %v, want %v", got, c.wantFull)
			}
			// The original failure stays reachable either way.
			if !errors.Is(err, c.dumpErr) {
				t.Fatalf("the dump's own error is no longer in the chain: %v", err)
			}
			if !c.wantFull {
				if want := "dump: " + c.dumpErr.Error(); err.Error() != want {
					t.Fatalf("an unrelated failure changed its message:\n got %q\nwant %q", err, want)
				}
				return
			}
			// Free space is read while the dump is still on disk: after the
			// removal the folder always has room.
			if measured.Load() == 0 {
				t.Fatal("free space was measured after the dump folder was removed")
			}
			msg := err.Error()
			first, rest, _ := strings.Cut(msg, "\n")
			for _, want := range []string{
				"not enough free disk space for this snapshot",
				"the working folder " + stage + " filled up during the full read",
				humanSize(int64(c.free)) + " free",
				"No new snapshot was made",
				"earlier snapshots are unchanged",
				"The partial dump was deleted",
				`"Working folder" setting`, "BINTRAIL_CONSOLE_BASELINE_STAGING",
			} {
				if !strings.Contains(first, want) {
					t.Errorf("the first line lacks %q:\n%s", want, first)
				}
			}
			if strings.Contains(first, "mydumper failed") || strings.Contains(first, "exit status") {
				t.Errorf("mydumper's own words come before the cause:\n%s", first)
			}
			// mydumper's output is still there, after the cause.
			if !strings.Contains(rest, "No space left on device") {
				t.Errorf("mydumper's output was dropped: %q", rest)
			}
		})
	}
}

// A dump folder that cannot be removed is not called deleted.
func TestDumpAttempt_aPartialDumpThatStaysIsNotCalledDeleted_1938(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root removes anything")
	}
	stage := t.TempDir()
	stubFailedDump(t, stage, errNoSpaceDump, 0, nil, 1<<40)
	prev := runMydumperFunc
	inner := runMydumperFunc
	runMydumperFunc = func(ctx context.Context, dsn string, ssl config.SSL, schemas []string, dir string, lm baseline.LockMode, src lockModeSource) error {
		err := inner(ctx, dsn, ssl, schemas, dir, lm, src)
		// The folder's entries can no longer be unlinked.
		if cerr := os.Chmod(dir, 0o555); cerr != nil {
			t.Errorf("chmod: %v", cerr)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
		return err
	}
	t.Cleanup(func() { runMydumperFunc = prev })
	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
	_, err := s.dumpAttempt(console.BaselineRequest{ServerID: "s1", SourceDSN: "src"}, baseline.LockModeFTWRL, lockModeFromEnv)
	if !foldDiskRefused(err) {
		t.Fatalf("err = %v, want the disk failure", err)
	}
	left, _ := filepath.Glob(filepath.Join(stage, "dump-*"))
	if len(left) != 1 {
		t.Fatalf("the test did not keep the dump folder: %v", left)
	}
	if strings.Contains(err.Error(), "was deleted") || !strings.Contains(err.Error(), "The partial dump at "+left[0]+" could not be removed") {
		t.Fatalf("the message does not say the dump is still on disk: %v", err)
	}
}

// Through Trigger and the real execute: the check before the dump saw room,
// the disk filled anyway, and the page learns it as a disk failure in the
// status, the run history and the failure card.
func TestFullRead_aDiskThatFillsDuringTheDumpReachesThePage_1938(t *testing.T) {
	stage := t.TempDir()
	stubEstimate(t, dumpEstimate{bytes: int64(10 * gib), tables: 2}, nil)
	stubSameFS(t, true, nil)
	var full atomic.Bool
	prevDisk := diskSpaceFn
	diskSpaceFn = func(string) (uint64, uint64, error) {
		if full.Load() {
			return 0, 1 << 40, nil
		}
		return 100 * gib, 1 << 40, nil
	}
	t.Cleanup(func() { diskSpaceFn = prevDisk })
	prev := runMydumperFunc
	runMydumperFunc = func(context.Context, string, config.SSL, []string, string, baseline.LockMode, lockModeSource) error {
		full.Store(true)
		return errNoSpaceDump
	}
	t.Cleanup(func() { runMydumperFunc = prev })
	prevDDL, prevEv := dumpDDLMarkFunc, dumpEventMarkFunc
	dumpDDLMarkFunc = func(console.BaselineRequest) string { return "" }
	dumpEventMarkFunc = func(console.BaselineRequest) string { return "" }
	t.Cleanup(func() { dumpDDLMarkFunc, dumpEventMarkFunc = prevDDL, prevEv })

	s := supWithHistory(t, stage)
	st := runFullRead(t, s, console.BaselineRequest{ServerID: "s1", ServerName: "shop-db", SourceDSN: "src", S3: "s3://b/p"})
	if st.State != "failed" || !st.DiskRefused {
		t.Fatalf("status = %+v, want a failed run marked as a disk failure", st)
	}
	const opening = "not enough free disk space for this snapshot: the working folder "
	if !strings.HasPrefix(st.LastError, "dump: "+opening+stage+" filled up during the full read") {
		t.Fatalf("LastError = %q", st.LastError)
	}
	// The card shows one line, whole: the cause, then the fix by the name of
	// the setting. Nothing of mydumper's, and no cut.
	want := "The working folder " + stage + " filled up during the full read (0 B free when the dump stopped). " +
		`Free space there, or move it to a bigger disk with the "Working folder" setting.`
	if st.Failure == nil || st.Failure.Summary != want {
		t.Fatalf("failure card = %+v\nwant summary %q", st.Failure, want)
	}
	if left, _ := filepath.Glob(filepath.Join(stage, "*")); len(left) != 0 {
		t.Fatalf("the working folder still holds %v", left)
	}
}

// After the automatic retry with lock-all, the error opens with the ftwrl
// refusal (up to 300 runes of it), which is as much as the card's line holds.
// The card still names the disk, and a long folder path does not cut the fix.
func TestSnapshotFailure_aFilledWorkingFolderIsNeverCutFromTheCard_1938(t *testing.T) {
	folder := "/" + strings.Repeat("very-long-folder-name/", 12) + "work"
	full := &workingFolderFullError{err: errNoSpaceDump, folder: folder, free: 4096}
	retried := fmt.Errorf("lock mode ftwrl was refused by the source (%s), and the automatic retry with lock-all failed too: %w",
		strings.Repeat("x", 300), fmt.Errorf("dump: %w", full))
	for name, err := range map[string]error{"plain": fmt.Errorf("dump: %w", full), "after the lock-all retry": retried, "a chosen mode": &chosenModeError{err: retried}} {
		f := snapshotFailureOf(err, console.BaselineRequest{ServerName: "shop-db"})
		if f == nil || f.Kind != "" {
			t.Fatalf("%s: failure = %+v", name, f)
		}
		if !strings.HasPrefix(f.Summary, "The working folder "+folder+" filled up") || !strings.HasSuffix(f.Summary, `the "Working folder" setting.`) || !strings.Contains(f.Summary, "4.0 KiB free") {
			t.Errorf("%s: summary = %q", name, f.Summary)
		}
	}
	// Any other failure keeps the first line of its own error.
	plain := errors.New("mydumper failed: exit status 1; output: Access denied")
	if f := snapshotFailureOf(fmt.Errorf("dump: %w", plain), console.BaselineRequest{}); f.Summary != "dump: mydumper failed: exit status 1" {
		t.Errorf("an unrelated failure's summary changed: %q", f.Summary)
	}
}
