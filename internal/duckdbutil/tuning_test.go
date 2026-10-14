package duckdbutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestTuningConstructors pins the two named budgets so renaming or retuning
// them is a deliberate, test-visible change. DefaultTuning must reproduce the
// values parquetquery.Fetch hardcoded before this struct existed; Ultrafast
// leaves threads/memory unset (DuckDB self-tunes) and sets S3Direct (httpfs).
func TestTuningConstructors(t *testing.T) {
	if got := DefaultTuning(); got.Threads != 2 || got.MemoryLimit != "4GB" || got.S3Direct {
		t.Fatalf("DefaultTuning() = %+v, want {Threads:2 MemoryLimit:4GB S3Direct:false}", got)
	}
	if got := Ultrafast(); got != (Tuning{S3Direct: true}) {
		t.Fatalf("Ultrafast() = %+v, want {S3Direct:true} (threads/memory unset)", got)
	}
}

// TestTuningApplyDefault: the conservative budget reaches DuckDB. threads is
// the clean deterministic signal; memory_limit is only checked for presence
// here because DuckDB renders it in base-2 units ('4GB' = 4×10^9 B → "3.7 GiB"),
// which is awkward to pin — the override test below proves the value wires
// through with a clean GiB unit.
func TestTuningApplyDefault(t *testing.T) {
	db := openDuckDB(t)
	DefaultTuning().Apply(context.Background(), db)

	if got := currentSetting(t, db, "threads"); got != "2" {
		t.Fatalf("threads after DefaultTuning = %q, want 2", got)
	}
	if mem := currentSetting(t, db, "memory_limit"); mem == "" {
		t.Fatal("memory_limit empty after DefaultTuning; want it set")
	}
	assertUsable(t, db)
}

// TestTuningApplyUltrafastIsNoOp: an empty Tuning must emit no SET at all, so
// DuckDB keeps its own native defaults. Proven by capturing the threads
// setting before and after Apply and asserting it is unchanged — this does not
// assume how many cores the test host has.
func TestTuningApplyUltrafastIsNoOp(t *testing.T) {
	db := openDuckDB(t)

	threadsBefore := currentSetting(t, db, "threads")
	memBefore := currentSetting(t, db, "memory_limit")
	Ultrafast().Apply(context.Background(), db)

	if after := currentSetting(t, db, "threads"); after != threadsBefore {
		t.Fatalf("Ultrafast().Apply changed threads %q → %q; want it left at DuckDB's default", threadsBefore, after)
	}
	if after := currentSetting(t, db, "memory_limit"); after != memBefore {
		t.Fatalf("Ultrafast().Apply changed memory_limit %q → %q; want it left at DuckDB's default", memBefore, after)
	}
	assertUsable(t, db)
}

// TestTuningApplyExplicitOverride: an operator-supplied Tuning (the --duckdb-*
// flags) takes effect verbatim. Uses a base-2 GiB value so DuckDB's rendered
// form is exact and unit-stable ("2.0 GiB"), unlike a base-10 GB suffix.
func TestTuningApplyExplicitOverride(t *testing.T) {
	db := openDuckDB(t)
	Tuning{Threads: 8, MemoryLimit: "2GiB"}.Apply(context.Background(), db)

	if got := currentSetting(t, db, "threads"); got != "8" {
		t.Fatalf("threads after override = %q, want 8", got)
	}
	if mem := currentSetting(t, db, "memory_limit"); mem != "2.0 GiB" {
		t.Fatalf("memory_limit after override = %q, want \"2.0 GiB\"", mem)
	}
	assertUsable(t, db)
}

// TestSetTempDirectory pins the #842 regression this function exists to close:
// every Tuning.Apply call site must be paired with SetTempDirectory (Apply's
// memory_limit is a hard cap with nowhere to spill without it — see Apply's
// doc comment). DuckDB's own default is a ".tmp" dir relative to the process
// CWD, not the OS temp dir; this proves the session actually ends up pointed
// at os.TempDir() rather than that CWD-relative default.
func TestSetTempDirectory(t *testing.T) {
	db := openDuckDB(t)
	SetTempDirectory(context.Background(), db)

	got := currentSetting(t, db, "temp_directory")
	want := filepath.Join(os.TempDir(), spillDirPrefix)
	if !strings.HasPrefix(got, want) {
		t.Fatalf("temp_directory after SetTempDirectory = %q, want a directory of its own under os.TempDir() (%q*)", got, want)
	}
	if got == "" || got == ".tmp" {
		t.Fatalf("temp_directory after SetTempDirectory = %q, want it NOT DuckDB's CWD-relative default", got)
	}
	assertUsable(t, db)
}

func openDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func currentSetting(t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	var v string
	if err := db.QueryRow("SELECT current_setting('" + name + "')").Scan(&v); err != nil {
		t.Fatalf("read current_setting(%q): %v", name, err)
	}
	return v
}

func assertUsable(t *testing.T, db *sql.DB) {
	t.Helper()
	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after Apply: %v (got %d)", err, one)
	}
}

// TestSetTempDirectory_ownDirPerSession pins #2387: DuckDB names spill files
// by block size only, so two sessions sharing a directory overwrite each
// other's. Two sessions spilling at once must both get the right answer, and
// each session's directory must be gone once it closes.
func TestSetTempDirectory_ownDirPerSession(t *testing.T) {
	// Sessions colliding in one directory can also hang inside DuckDB, where
	// the deadline does not reach: a regression may show as the go test
	// timeout rather than as these assertions.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const n = 3_000_000
	dirs := make([]string, 2)
	errs := make([]error, 2)
	counts := make([]int64, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		db, err := sql.Open("duckdb", filepath.Join(t.TempDir(), "s.duckdb"))
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		Tuning{Threads: 2, MemoryLimit: "64MB"}.Apply(ctx, db)
		SetTempDirectory(ctx, db)
		dirs[i] = currentSetting(t, db, "temp_directory")
		for _, stmt := range []string{
			fmt.Sprintf("CREATE TABLE a AS SELECT i AS k1, hash(i) AS k2 FROM range(%d) t(i)", n),
			fmt.Sprintf("CREATE TABLE b AS SELECT i AS k1, hash(i) AS k2 FROM range(%d) t(i)", n),
		} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer db.Close()
			for range 3 {
				if errs[i] = db.QueryRowContext(ctx, "SELECT count(*) FROM a JOIN b ON a.k1 = b.k1 AND a.k2 = b.k2").Scan(&counts[i]); errs[i] != nil || counts[i] != n {
					return
				}
			}
		}()
	}
	wg.Wait()
	if dirs[0] == dirs[1] {
		t.Errorf("two sessions share the spill directory %s", dirs[0])
	}
	for i := range 2 {
		if errs[i] != nil || counts[i] != n {
			t.Errorf("session %d: count=%d err=%v, want %d", i, counts[i], errs[i], n)
		}
		if _, err := os.Stat(dirs[i]); !os.IsNotExist(err) {
			t.Errorf("session %d spill directory %s still there after close: %v", i, dirs[i], err)
		}
	}
}

// TestSetTempDirectory_privateProcessDir: the sessions' directories sit in
// one per-process directory only its owner can enter, so no other user can
// read spilled rows or create a session's directory first.
func TestSetTempDirectory_privateProcessDir(t *testing.T) {
	db := openDuckDB(t)
	SetTempDirectory(context.Background(), db)
	got := currentSetting(t, db, "temp_directory")
	parent := filepath.Dir(got)
	if parent != spillRoot || spillRoot == "" {
		t.Fatalf("session dir %s not under the process dir %q", got, spillRoot)
	}
	fi, err := os.Stat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Errorf("process spill dir mode = %v, want 0700", fi.Mode().Perm())
	}
	if h, pid, ok := parseSpillDirName(filepath.Base(parent)); !ok || h != spillHost() || pid != os.Getpid() {
		t.Errorf("process spill dir %s: host %q pid %d ok %v", parent, h, pid, ok)
	}
}

// TestRemoveDeadSpillDirs: on this host, a spill directory whose process is
// gone, or an earlier one with this process's pid, is removed; a live
// process's, another host's, and anything not named like one stay.
func TestRemoveDeadSpillDirs(t *testing.T) {
	dir := t.TempDir()
	me := os.Getpid()
	dead := deadPid(t)
	name := func(host string, pid int) string {
		return fmt.Sprintf("%s%s-%d-%s", spillDirPrefix, host, pid, "123456")
	}
	mk := func(n string) string {
		p := filepath.Join(dir, n)
		if err := os.MkdirAll(filepath.Join(p, "1"), 0o700); err != nil {
			t.Fatal(err)
		}
		return p
	}
	gone := []string{
		mk(name("box-a", dead)),
		mk(name("box-a", me)), // an earlier process that had this pid
	}
	stay := []string{
		mk(name("box-a", os.Getppid())),
		mk(name("box-b", dead)), // another container's; its pids mean nothing here
		mk(spillDirPrefix + "box-a-x-1"),
		mk("something-else"),
	}
	file := filepath.Join(dir, name("box-a", dead)+"0")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	stay = append(stay, file)

	removeDeadSpillDirs(dir, "box-a")
	for _, p := range gone {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s should be removed: %v", filepath.Base(p), err)
		}
	}
	for _, p := range stay {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s should stay: %v", filepath.Base(p), err)
		}
	}
}

func TestParseSpillDirName(t *testing.T) {
	for _, tc := range []struct {
		name string
		host string
		pid  int
		ok   bool
	}{
		{spillDirPrefix + "box-42-123", "box", 42, true},
		{spillDirPrefix + "ip-10-0-0-1-42-987", "ip-10-0-0-1", 42, true},
		{spillDirPrefix + "box-42-", "", 0, false},
		{spillDirPrefix + "box-42", "", 0, false},
		{spillDirPrefix + "-42-123", "", 0, false},
		{spillDirPrefix + "box-0-123", "", 0, false},
		{spillDirPrefix + "box-x-123", "", 0, false},
		{spillDirPrefix + "42-123", "", 0, false},
		{"other-box-42-123", "", 0, false},
	} {
		host, pid, ok := parseSpillDirName(tc.name)
		if host != tc.host || pid != tc.pid || ok != tc.ok {
			t.Errorf("parseSpillDirName(%q) = %q, %d, %v; want %q, %d, %v", tc.name, host, pid, ok, tc.host, tc.pid, tc.ok)
		}
	}
}

// deadPid returns the pid of a process that has exited.
func deadPid(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Skipf("cannot run a short-lived process: %v", err)
	}
	pid := cmd.Process.Pid
	if processAlive(pid) {
		t.Skipf("pid %d was reused", pid)
	}
	return pid
}

// TestDefaultTuningFor: a job's budget on ctx replaces DefaultTuning; no
// budget, or a zero one, leaves DefaultTuning.
func TestDefaultTuningFor(t *testing.T) {
	ctx := context.Background()
	if got := DefaultTuningFor(ctx); got != DefaultTuning() {
		t.Errorf("no budget: %+v", got)
	}
	if got := DefaultTuningFor(WithSessionTuning(ctx, Tuning{})); got != DefaultTuning() {
		t.Errorf("zero budget: %+v", got)
	}
	b := Tuning{Threads: 2, MemoryLimit: "512MiB"}
	if got := DefaultTuningFor(WithSessionTuning(ctx, b)); got != b {
		t.Errorf("budget: %+v, want %+v", got, b)
	}
}
