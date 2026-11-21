//go:build unix

package reconstruct

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2513: a process killed in the middle of a fold left its working
// directories in the temporary directory for good. The sweep that removes
// them must never remove one a live fold still reads, of this process or of
// another.

// sweepNow is the sweep as a starting process runs it, over parent.
func sweepNow(parent string) {
	sweepWorkDirs(parent, workDirHost(), time.Now().Add(-workDirLegacyIdle))
}

// aged sets the last write of every path to d ago.
func aged(t *testing.T, d time.Duration, paths ...string) {
	t.Helper()
	at := time.Now().Add(-d)
	for _, p := range paths {
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
}

// leftDir makes parent/name as a killed process leaves a working directory:
// a file in it, no lock held. It returns the directory and the file.
func leftDir(t *testing.T, parent, name string) (dir, file string) {
	t.Helper()
	dir = filepath.Join(parent, name)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	file = filepath.Join(dir, "00.gob")
	if err := os.WriteFile(file, []byte("changed rows"), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, file
}

func markedName(kind string, n int) string {
	return fmt.Sprintf("bintrail-%s-%s%s-%d", kind, workDirMark, workDirHost(), n)
}

func there(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

func oneChange() map[string]*query.ResultRow {
	return map[string]*query.ResultRow{"1": {EventType: event.EventUpdate, EventID: 7, PKValues: "1", RowAfter: map[string]any{"id": "1"}}}
}

// A fold of this process, its changes written long ago and not yet read
// back: the sweep keeps them, and the fold reads them after it.
func TestWorkDirSweep_keepsALiveFoldOfThisProcess_2513(t *testing.T) {
	parent := t.TempDir()
	t.Setenv("TMPDIR", parent)
	s, err := newChangeSpill(10)
	if err != nil {
		t.Fatal(err)
	}
	defer s.remove()
	if filepath.Dir(s.dir) != parent {
		t.Fatalf("the fold's directory %s is not under the temporary directory %s", s.dir, parent)
	}
	if host, legacy, ok := parseWorkDirName(filepath.Base(s.dir)); !ok || legacy || host != workDirHost() {
		t.Fatalf("the fold's directory %s does not read as a locked one of this host (host %q, legacy %v, ok %v)", s.dir, host, legacy, ok)
	}
	if err := s.drain(oneChange()); err != nil {
		t.Fatal(err)
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	b := spillBucket("1")
	aged(t, 48*time.Hour, s.path(b), s.dir)

	sweepNow(parent)

	got, err := s.load(b)
	if err != nil {
		t.Fatalf("a live fold's changes were removed by a sweep of its own process: %v", err)
	}
	if got["1"] == nil || got["1"].EventID != 7 {
		t.Fatalf("the fold read back %+v, want the change it wrote", got)
	}

	// The fold over: its directory goes with it, and the lock with the
	// directory.
	dir := s.dir
	if err := s.remove(); err != nil {
		t.Fatal(err)
	}
	if there(dir) {
		t.Fatalf("the fold's directory stayed after it ended")
	}
}

// The same for the directory a compaction merges from.
func TestWorkDirSweep_keepsALiveCompactionOfThisProcess_2513(t *testing.T) {
	parent := t.TempDir()
	w, err := newWorkDir(parent, workDirCompact)
	if err != nil {
		t.Fatal(err)
	}
	defer w.remove()
	file := filepath.Join(w.path, "baseline.parquet")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	aged(t, 48*time.Hour, file, w.path)
	sweepNow(parent)
	if !there(file) {
		t.Fatal("a live compaction's file was removed by a sweep of its own process")
	}
	if err := w.remove(); err != nil {
		t.Fatal(err)
	}
	if there(w.path) {
		t.Fatal("the directory stayed after remove")
	}
	if err := w.remove(); err != nil {
		t.Fatalf("a second remove failed: %v", err)
	}
}

// workDirHelperEnv makes TestWorkDirHelperProcess a fold in a process of its
// own, under the temporary directory it names.
const workDirHelperEnv = "BINTRAIL_WORKDIR_HELPER_TMP"

// TestWorkDirHelperProcess is not a test: run by
// TestWorkDirSweep_anotherProcess_2513 as a second process, it spills a
// fold's changes, prints where, and lives until its stdin closes or it is
// killed.
func TestWorkDirHelperProcess(t *testing.T) {
	if os.Getenv(workDirHelperEnv) == "" {
		t.Skip("run as a second process by TestWorkDirSweep_anotherProcess_2513")
	}
	s, err := newChangeSpill(10)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.drain(oneChange()); err != nil {
		t.Fatal(err)
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("DIR=%s\n", s.dir)
	fmt.Printf("FILE=%s\n", s.path(spillBucket("1")))
	fmt.Println("READY")
	_, _ = io.Copy(io.Discard, os.Stdin)
}

// Two processes under one temporary directory. While the second lives, its
// fold's changes survive the first's sweep however old they are; once it is
// killed, uncatchably, the next sweep removes what it left.
func TestWorkDirSweep_anotherProcess_2513(t *testing.T) {
	parent := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkDirHelperProcess$", "-test.count=1")
	cmd.Env = append(os.Environ(), workDirHelperEnv+"="+parent, "TMPDIR="+parent)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	var dir, file string
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		line := sc.Text()
		if d, ok := strings.CutPrefix(line, "DIR="); ok {
			dir = d
		}
		if f, ok := strings.CutPrefix(line, "FILE="); ok {
			file = f
		}
		if line == "READY" {
			break
		}
	}
	if dir == "" || file == "" {
		t.Fatalf("the second process spilled nothing (dir %q, file %q)", dir, file)
	}
	if filepath.Dir(dir) != parent {
		t.Fatalf("the second process's directory %s is not under %s", dir, parent)
	}
	go func() { _, _ = io.Copy(io.Discard, stdout) }()
	aged(t, 48*time.Hour, file, dir)

	sweepNow(parent)
	if !there(file) {
		t.Fatal("a live process's changed rows were removed by another process's sweep")
	}

	// SIGKILL: nothing in the process runs after it, as with the kernel's
	// out-of-memory killer.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if !there(file) {
		t.Fatal("the killed process's directory is already gone: the test proves nothing")
	}
	logs := captureLogs(t)
	sweepNow(parent)
	if there(dir) {
		t.Errorf("the directory a killed process left stayed after the sweep")
	}
	if !strings.Contains(logs.String(), "removed a working directory a stopped process left") || !strings.Contains(logs.String(), dir) {
		t.Errorf("the removal was not logged with its directory:\n%s", logs.String())
	}
}

// What a dead process left goes: a locked-by-design directory whose lock is
// free, however new (the free lock is the proof, not its age), and a
// directory of a version from before the lock once nothing was written in it
// for a day. A newer one of that version stays: its process may be folding.
func TestWorkDirSweep_removesWhatADeadProcessLeft_2513(t *testing.T) {
	parent := t.TempDir()
	newFold, _ := leftDir(t, parent, markedName(workDirFold, 1))
	newCompact, _ := leftDir(t, parent, markedName(workDirCompact, 2))
	oldMarked, oldMarkedFile := leftDir(t, parent, markedName(workDirFold, 3))
	aged(t, 30*24*time.Hour, oldMarkedFile, oldMarked)

	legacyOld, f := leftDir(t, parent, "bintrail-fold-1234567890")
	aged(t, 48*time.Hour, f, legacyOld)
	legacyOldCompact, f := leftDir(t, parent, "bintrail-compact-777")
	aged(t, 48*time.Hour, f, legacyOldCompact)
	legacyNew, _ := leftDir(t, parent, "bintrail-fold-42")
	legacyBusy, f := leftDir(t, parent, "bintrail-fold-43")
	// Made long ago, written a moment ago: the directory's own time does
	// not move when a file in it grows.
	aged(t, 48*time.Hour, legacyBusy)
	_ = f
	legacyAlmost, f := leftDir(t, parent, "bintrail-fold-44")
	aged(t, workDirLegacyIdle-time.Hour, f, legacyAlmost)

	sweepNow(parent)

	for _, gone := range []string{newFold, newCompact, oldMarked, legacyOld, legacyOldCompact} {
		if there(gone) {
			t.Errorf("%s was left by a dead process and stayed", filepath.Base(gone))
		}
	}
	for _, kept := range []string{legacyNew, legacyBusy, legacyAlmost} {
		if !there(filepath.Join(kept, "00.gob")) {
			t.Errorf("%s may belong to a live process of a version without the lock, and was removed", filepath.Base(kept))
		}
	}
}

// Names that only look like a working directory's, another host's, and a
// file by the name: none is touched, old as they are.
func TestWorkDirSweep_leavesWhatIsNotItsOwn_2513(t *testing.T) {
	parent := t.TempDir()
	const host = "db-host-1"
	names := []string{
		"bintrail-fold",
		"bintrail-fold-",
		"bintrail-folder-123",
		"bintrail-fold-12a",
		"bintrail-fold-v2-",
		"bintrail-fold-v2-" + host,
		"bintrail-fold-v2-" + host + "-",
		"bintrail-fold-v2-" + host + "-abc",
		"bintrail-fold-v2--123",
		"bintrail-fold-v3-" + host + "-123",
		"bintrail-fold-v2-db-host-123",        // host "db-host", not this one
		"bintrail-fold-v2-" + host + "-1-123", // host "db-host-1-1"
		"bintrail-fold-v2-other-123",
		"xbintrail-fold-123",
		"bintrail-fold-123.bak",
		"bintrail-tails-123",
		"bintrail-baseline-123",
		"bintrail-compactor-123",
		"BINTRAIL-FOLD-123",
	}
	var files []string
	for _, n := range names {
		dir, f := leftDir(t, parent, n)
		aged(t, 48*time.Hour, f, dir)
		files = append(files, f)
	}
	// A plain file under a working directory's name.
	plain := filepath.Join(parent, "bintrail-fold-555")
	if err := os.WriteFile(plain, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	marked := filepath.Join(parent, "bintrail-compact-v2-"+host+"-556")
	if err := os.WriteFile(marked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	aged(t, 48*time.Hour, plain, marked)
	files = append(files, plain, marked)
	// The control: this host's, with the dashes its name holds.
	own, _ := leftDir(t, parent, "bintrail-fold-v2-"+host+"-124")

	sweepWorkDirs(parent, host, time.Now().Add(-workDirLegacyIdle))

	for _, f := range files {
		if !there(f) {
			t.Errorf("%s is not a working directory of this host and was removed", f)
		}
	}
	if there(own) {
		t.Errorf("the control directory of host %q stayed: the sweep removed nothing at all", host)
	}
}

func TestParseWorkDirName_2513(t *testing.T) {
	for _, c := range []struct {
		name         string
		host         string
		legacy, isOK bool
	}{
		{"bintrail-fold-123", "", true, true},
		{"bintrail-compact-0", "", true, true},
		{"bintrail-fold-v2-h-1", "h", false, true},
		{"bintrail-compact-v2-a-b-c-99", "a-b-c", false, true},
		{"bintrail-fold-v2-v2-7-99", "v2-7", false, true},
		{"bintrail-fold-v2-1", "", false, false},
		{"bintrail-fold-v2-h-", "", false, false},
		{"bintrail-fold-v2--1", "", false, false},
		{"bintrail-fold-", "", false, false},
		{"bintrail-fold-1x", "", false, false},
		{"bintrail-fold-١٢٣", "", false, false},
		{"", "", false, false},
	} {
		host, legacy, ok := parseWorkDirName(c.name)
		if host != c.host || legacy != c.legacy || ok != c.isOK {
			t.Errorf("parseWorkDirName(%q) = (%q, %v, %v), want (%q, %v, %v)", c.name, host, legacy, ok, c.host, c.legacy, c.isOK)
		}
	}
}

// A fold follows TMPDIR, and a sweep of one temporary directory does not
// reach into another.
func TestWorkDirSweep_anotherTemporaryDirectory_2513(t *testing.T) {
	mine, theirs := t.TempDir(), t.TempDir()
	left, _ := leftDir(t, theirs, markedName(workDirFold, 1))
	t.Setenv("TMPDIR", mine)
	w, err := newWorkDir("", workDirFold)
	if err != nil {
		t.Fatal(err)
	}
	defer w.remove()
	if filepath.Dir(w.path) != mine {
		t.Fatalf("made %s, want it under TMPDIR %s", w.path, mine)
	}
	sweepNow(mine)
	if !there(left) {
		t.Fatal("a sweep of one temporary directory removed a directory under another")
	}
	if !there(w.path) {
		t.Fatal("the sweep removed the live directory of its own temporary directory")
	}
}

// A directory that cannot be removed does not stop the sweep (or the start
// it runs in): it is said, with the directory, and the next one is removed.
func TestWorkDirSweep_aDirectoryItMayNotRemove_2513(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root removes anything; run as an ordinary user")
	}
	parent := t.TempDir()
	stuck, _ := leftDir(t, parent, markedName(workDirFold, 1))
	inner := filepath.Join(stuck, "inner")
	if err := os.Mkdir(inner, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, "f"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(inner, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(inner, 0o700) })
	next, _ := leftDir(t, parent, markedName(workDirFold, 2))
	logs := captureLogs(t)

	sweepNow(parent)

	if !there(inner) {
		t.Fatal("the directory that may not be removed is gone: the test proves nothing")
	}
	out := logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "could not remove a working directory a stopped process left") || !strings.Contains(out, stuck) {
		t.Errorf("the failed removal was not warned about with its directory:\n%s", out)
	}
	if there(next) {
		t.Errorf("the sweep stopped at the directory it could not remove")
	}
	t.Logf("the warning as logged:\n%s", out)
}

// Another user's directory is theirs: root's sweep leaves it.
func TestWorkDirSweep_anotherUsersDirectory_2513(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("needs root to hand a directory to another user")
	}
	parent := t.TempDir()
	dir, _ := leftDir(t, parent, markedName(workDirFold, 1))
	if err := os.Chown(dir, 54321, 54321); err != nil {
		t.Fatal(err)
	}
	own, _ := leftDir(t, parent, markedName(workDirFold, 2))
	sweepNow(parent)
	if !there(dir) {
		t.Error("another user's directory was removed")
	}
	if there(own) {
		t.Error("the control directory stayed")
	}
}

// A directory removed between the listing and the lock (its fold ended, or
// another process's sweep got it) is nothing to report, and the sweep goes
// on.
func TestWorkDirSweep_aDirectoryGoneMidSweep_2513(t *testing.T) {
	parent := t.TempDir()
	first, _ := leftDir(t, parent, markedName(workDirFold, 1))
	second, _ := leftDir(t, parent, markedName(workDirFold, 2))
	beforeWorkDirSwept = func(dir string) {
		if dir == first {
			if err := os.RemoveAll(dir); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(func() { beforeWorkDirSwept = func(string) {} })
	logs := captureLogs(t)

	sweepNow(parent)

	if there(second) {
		t.Error("the sweep stopped at a directory that was gone")
	}
	if out := logs.String(); strings.Contains(out, first) {
		t.Errorf("a directory that was simply gone was reported:\n%s", out)
	}
}

// The path names another directory by the time the lock is held: the one
// that was locked is gone and a live fold's took its name. The sweep removes
// neither.
func TestWorkDirSweep_aDirectoryReplacedUnderTheLock_2513(t *testing.T) {
	parent, aside := t.TempDir(), t.TempDir()
	dir, _ := leftDir(t, parent, markedName(workDirFold, 1))
	live := filepath.Join(dir, "live.gob")
	var once sync.Once
	afterWorkDirLocked = func(locked string) {
		if locked != dir {
			return
		}
		once.Do(func() {
			if err := os.Rename(dir, filepath.Join(aside, "moved")); err != nil {
				t.Error(err)
			}
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Error(err)
			}
			if err := os.WriteFile(live, []byte("x"), 0o600); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(func() { afterWorkDirLocked = func(string) {} })

	if _, err := lockWorkDir(dir, false); !errors.Is(err, errWorkDirGone) {
		t.Fatalf("lockWorkDir on a path that names another directory = %v, want errWorkDirGone", err)
	}
	// And through the sweep, on a fresh pair.
	_ = os.RemoveAll(dir)
	dir2, _ := leftDir(t, parent, markedName(workDirFold, 2))
	live2 := filepath.Join(dir2, "live.gob")
	afterWorkDirLocked = func(locked string) {
		if locked != dir2 || there(live2) {
			return
		}
		if err := os.Rename(dir2, filepath.Join(aside, "moved2")); err != nil {
			t.Error(err)
		}
		if err := os.Mkdir(dir2, 0o700); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(live2, []byte("x"), 0o600); err != nil {
			t.Error(err)
		}
		// One sweep only: the directory now there is not held by anyone in
		// this test, so a second lock on it would be free.
		afterWorkDirLocked = func(string) {}
	}
	sweepNow(parent)
	if !there(live2) {
		t.Error("the sweep removed a directory other than the one it locked")
	}
}

// A link under a working directory's name is not one: neither it nor what it
// points at is touched, and it is never locked through.
func TestWorkDirSweep_aSymbolicLink_2513(t *testing.T) {
	parent, elsewhere := t.TempDir(), t.TempDir()
	target, file := leftDir(t, elsewhere, "precious")
	aged(t, 48*time.Hour, file, target)
	links := []string{
		filepath.Join(parent, markedName(workDirFold, 1)),
		filepath.Join(parent, "bintrail-fold-123"),
		filepath.Join(parent, markedName(workDirCompact, 2)),
	}
	for _, l := range links {
		if err := os.Symlink(target, l); err != nil {
			t.Fatal(err)
		}
	}
	// A real one whose contents include a link out: the link goes, what it
	// points at stays.
	holder, _ := leftDir(t, parent, markedName(workDirFold, 3))
	if err := os.Symlink(target, filepath.Join(holder, "out")); err != nil {
		t.Fatal(err)
	}

	sweepNow(parent)

	for _, l := range links {
		fi, err := os.Lstat(l)
		if err != nil || fi.Mode()&os.ModeSymlink == 0 {
			t.Errorf("the link %s was removed or replaced (%v)", filepath.Base(l), err)
		}
		if f, err := lockWorkDir(l, false); err == nil {
			f.Close()
			t.Errorf("lockWorkDir followed the link %s", filepath.Base(l))
		}
	}
	if !there(file) {
		t.Error("what a link pointed at was removed")
	}
	if there(holder) {
		t.Error("the directory holding a link was not removed")
	}
}

// A sweep that takes a new directory's lock before its maker does removes
// it, empty. The maker sees its path gone and makes another, which it holds.
func TestNewWorkDir_makesAnotherWhenASweepTookItFirst_2513(t *testing.T) {
	parent := t.TempDir()
	var taken string
	afterWorkDirLocked = func(dir string) {
		if taken == "" {
			taken = dir
			if err := os.Remove(dir); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(func() { afterWorkDirLocked = func(string) {} })
	w, err := newWorkDir(parent, workDirFold)
	if err != nil {
		t.Fatal(err)
	}
	defer w.remove()
	afterWorkDirLocked = func(string) {}
	if taken == "" || w.path == taken {
		t.Fatalf("made %s; the directory removed under it was %q", w.path, taken)
	}
	if !there(w.path) || w.lock == nil {
		t.Fatalf("the second directory %s is not there or not locked", w.path)
	}
	if _, err := lockWorkDir(w.path, false); !errors.Is(err, errWorkDirHeld) {
		t.Fatalf("a second lock on the directory = %v, want errWorkDirHeld", err)
	}
	if _, legacy, ok := parseWorkDirName(filepath.Base(w.path)); !ok || legacy {
		t.Errorf("%s does not read as a locked directory", w.path)
	}
}

// A directory that cannot be locked is not made under the name whose free
// lock means a dead owner: it would be removed by the next process to start.
// It gets the older name, kept while written in the last day.
func TestNewWorkDir_withoutALockTakesTheOlderName_2513(t *testing.T) {
	parent := t.TempDir()
	workDirLock = func(string, bool) (*os.File, error) { return nil, errors.New("no locks on this filesystem") }
	workDirLockWarn = sync.Once{}
	t.Cleanup(func() { workDirLock = lockWorkDir })
	logs := captureLogs(t)
	w, err := newWorkDir(parent, workDirFold)
	workDirLock = lockWorkDir
	if err != nil {
		t.Fatal(err)
	}
	defer w.remove()
	if _, legacy, ok := parseWorkDirName(filepath.Base(w.path)); !ok || !legacy || w.lock != nil {
		t.Fatalf("%s (lock %v): want the older name and no lock", w.path, w.lock)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("the directory that could not be locked was left beside the one in use: %v", entries)
	}
	out := logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "could not be locked") || !strings.Contains(out, "no locks on this filesystem") {
		t.Errorf("no warning that the directory is not locked:\n%s", out)
	}
	t.Logf("the warning as logged:\n%s", out)
	file := filepath.Join(w.path, "00.gob")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	sweepNow(parent)
	if !there(file) {
		t.Fatal("a directory in use, not lockable, was removed while new")
	}
}

// The lock is held by an open file of the directory itself, so it is the
// kernel that lets go of it: closing that file is all a dead process does.
func TestWorkDir_theLockIsWhatKeepsIt_2513(t *testing.T) {
	parent := t.TempDir()
	w, err := newWorkDir(parent, workDirFold)
	if err != nil {
		t.Fatal(err)
	}
	if w.lock == nil {
		t.Fatal("the directory was made without its lock")
	}
	sweepNow(parent)
	if !there(w.path) {
		t.Fatal("a held directory was removed")
	}
	w.lock.Close() // what the kernel does for a process that died
	sweepNow(parent)
	if there(w.path) {
		t.Fatal("the directory stayed once its lock was let go")
	}
}
