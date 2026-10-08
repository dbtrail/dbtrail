package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// spillQuery needs more than spillMemory: a DISTINCT over 20 M integers fails
// with an out-of-memory error at 128 MB without a temp directory and finishes
// with one (measured on the pinned engine, under the full lock-down).
const (
	spillQuery  = "SELECT count(*) FROM (SELECT DISTINCT i FROM range(20000000) t(i))"
	spillMemory = "128MB"
)

func spillRunner(t *testing.T, root string, l Limits) *Runner {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return New(Config{Exe: exe, Args: []string{}, Limits: l, MaxWait: -1, SpillDir: root})
}

func spillLeft(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var left []string
	for _, e := range entries {
		left = append(left, e.Name())
	}
	return left
}

// Past its memory a statement spills to its own directory and finishes,
// where the same statement with nowhere to spill fails (#2210). The second
// runner's root does not exist, so it cannot make the directory: that is
// also the path a host with no writable temp directory takes, and it must
// still run the statement, as before #2210, not refuse it.
func TestRun_spillsPastMemoryInsteadOfFailing(t *testing.T) {
	f := newCopyFixture(t)
	l := testLimits()
	l.MemoryLimit = spillMemory

	root := t.TempDir()
	res, err := spillRunner(t, root, l).Run(context.Background(), f.job(spillQuery))
	if err != nil {
		t.Fatalf("with a spill directory: %v", err)
	}
	if res.Rows[0][0] != json.Number("20000000") {
		t.Errorf("with a spill directory: got %#v, want 20000000", res.Rows[0][0])
	}
	if left := spillLeft(t, root); len(left) != 0 {
		t.Errorf("the statement's spill directory outlived it: %v", left)
	}

	_, err = spillRunner(t, filepath.Join(t.TempDir(), "missing"), l).Run(context.Background(), f.job(spillQuery))
	var qe *QueryError
	if !errors.As(err, &qe) || !strings.Contains(qe.Message, "Out of Memory") {
		t.Fatalf("with nowhere to spill: err = %v, want DuckDB's Out of Memory error", err)
	}
}

// The directory goes away on the paths that do not end in a result too: a
// statement killed at its timeout, and one DuckDB fails.
func TestRun_spillDirRemovedOnTimeoutAndError(t *testing.T) {
	f := newCopyFixture(t)
	root := t.TempDir()
	l := testLimits()
	l.Timeout = time.Second
	r := spillRunner(t, root, l)
	var te *TimeoutError
	if _, err := r.Run(context.Background(), f.job("SELECT count(*) FROM range(100000000) a, range(100000000) b")); !errors.As(err, &te) {
		t.Fatalf("err = %v, want TimeoutError", err)
	}
	if left := spillLeft(t, root); len(left) != 0 {
		t.Errorf("after a timeout, left behind: %v", left)
	}
	var qe *QueryError
	if _, err := spillRunner(t, root, testLimits()).Run(context.Background(), f.job("SELECT 1/'x'")); !errors.As(err, &qe) {
		t.Fatalf("err = %v, want QueryError", err)
	}
	if left := spillLeft(t, root); len(left) != 0 {
		t.Errorf("after a failed statement, left behind: %v", left)
	}
}

// New removes what a parent that died left behind, and only that: an old
// spill directory goes, a recent one (a statement another console on the
// host is running) and anything not named like a spill directory stay.
func TestNew_sweepsStaleSpillDirectories(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-2 * staleSpillAge)
	mk := func(name string, at time.Time) {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Join(p, "inner"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	mk(spillPrefix+"old", old)
	mk(spillPrefix+"live", time.Now())
	mk("someone-elses-old", old)
	New(Config{SpillDir: root})
	left := strings.Join(spillLeft(t, root), " ")
	if left != spillPrefix+"live someone-elses-old" {
		t.Errorf("after the sweep: %q, want only the live spill directory and the foreign one", left)
	}
}

// On the engine, under the full lock-down: what the statement can reach of the
// spill tree, and the error past the cap, from a real run.
func TestLockdown_spillDirIsTheEnginesOnly(t *testing.T) {
	open := func(spill spillSpec) *sql.DB {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		for _, s := range append([]string{"SET memory_limit = '" + spillMemory + "'", "SET threads = 2"}, lockdownStatements([]string{t.TempDir()}, spill)...) {
			if _, err := db.Exec(s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		return db
	}
	// DuckDB admits its temp directory to the statement (observed on the
	// pinned engine: glob and read_csv over it succeed). That is why the
	// directory is one statement's own and goes away with it: what the
	// statement can read there is its own spill. Its PARENT, and a sibling
	// statement's directory, stay out of reach.
	root := t.TempDir()
	dir := filepath.Join(root, spillPrefix+"mine")
	sibling := filepath.Join(root, spillPrefix+"theirs")
	for _, d := range []string{dir, sibling} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "x.csv"), []byte("a\n1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	db := open(spillSpec{Dir: dir, MaxBytes: 1 << 30})
	defer db.Close()
	for _, q := range []string{
		"SELECT * FROM glob('" + root + "/*')",
		"SELECT * FROM glob('" + sibling + "/*')",
		"SELECT * FROM read_csv('" + sibling + "/x.csv')",
	} {
		if _, err := db.Exec(q); err == nil || !strings.Contains(err.Error(), "Permission Error") {
			t.Errorf("%s: err = %v, want DuckDB's Permission Error", q, err)
		}
	}
	var n int64
	if err := db.QueryRow(spillQuery).Scan(&n); err != nil || n != 20000000 {
		t.Fatalf("spilling under the lock-down: n = %d, err = %v", n, err)
	}

	capped := open(spillSpec{Dir: t.TempDir(), MaxBytes: 1 << 20})
	defer capped.Close()
	err := capped.QueryRow(spillQuery).Scan(&n)
	if err == nil || !strings.Contains(err.Error(), "failed to offload data block") {
		t.Fatalf("past the spill cap: err = %v, want DuckDB's failed-to-offload error, which spillCapMessage recognizes", err)
	}
	spec := spillSpec{Dir: "/spill", MaxBytes: 8 << 30}
	if got := spillCapMessage(err.Error(), spec); !strings.HasSuffix(got, "may also use 8192 MB of disk, and it filled that too.") {
		t.Errorf("past the spill cap the user reads %q", got)
	}
	if got := spillCapMessage("Out of Memory Error: could not allocate block", spec); strings.Contains(got, "disk") {
		t.Errorf("a plain out-of-memory error reads as a full disk: %q", got)
	}
}

// The spill statements: the directory quoted, the cap in whole MiB, and both
// BEFORE external access goes off (after it, DuckDB refuses to change
// temp_directory). No spec is the old line, no spill.
func TestLockdownStatements_spill(t *testing.T) {
	stmts := lockdownStatements([]string{"/copy"}, spillSpec{Dir: "/tmp/it's", MaxBytes: 4 << 30})
	script := strings.Join(stmts, "\n")
	for _, want := range []string{"SET temp_directory = '/tmp/it''s'", "SET max_temp_directory_size = '4096MiB'"} {
		if !strings.Contains(script, want) {
			t.Errorf("lockdown lacks %q:\n%s", want, script)
		}
	}
	if strings.Index(script, "max_temp_directory_size") > strings.Index(script, "enable_external_access") {
		t.Errorf("the spill settings run after external access goes off:\n%s", script)
	}
	none := strings.Join(lockdownStatements([]string{"/copy"}, spillSpec{}), "\n")
	if !strings.Contains(none, "SET temp_directory = ''") || strings.Contains(none, "max_temp_directory_size") {
		t.Errorf("with no spill spec:\n%s", none)
	}
}
