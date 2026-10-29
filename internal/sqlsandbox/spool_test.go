package sqlsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// spoolLimits keeps a result in memory up to a few kilobytes and spools what
// is larger, up to spoolMax.
func spoolLimits(spoolMax int64) Limits {
	l := testLimits()
	l.MaxRows = 1_000_000
	l.MaxResultBytes = 4 << 10
	l.SpoolMaxBytes = spoolMax
	return l
}

func spoolDirs(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), spoolPrefix) {
			out = append(out, e.Name())
		}
	}
	return out
}

// spoolRunner asks for the file on every statement, as the port will: the
// Runner's own SpoolMaxBytes reaches no statement (TestRun_spoolIsNeverInherited).
type spoolRunner struct {
	*Runner
	spoolMax int64
}

func (s spoolRunner) Run(ctx context.Context, job Job) (Result, error) {
	if job.Limits.SpoolMaxBytes == 0 {
		job.Limits.SpoolMaxBytes = s.spoolMax
	}
	return s.Runner.Run(ctx, job)
}

func newSpoolRunner(t *testing.T, limits Limits, statements int) (spoolRunner, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	r := New(Config{Exe: exe, Args: []string{}, Limits: limits, MaxWait: -1, SpillDir: root, WorkerStatements: statements})
	t.Cleanup(r.Close)
	return spoolRunner{Runner: r, spoolMax: limits.SpoolMaxBytes}, root
}

func collect(t *testing.T, s *Spool) [][]any {
	t.Helper()
	var rows [][]any
	if err := s.Each(func(row []any) error { rows = append(rows, row); return nil }); err != nil {
		t.Fatalf("Each: %v", err)
	}
	return rows
}

// A result larger than what is held in memory is written to a file the
// caller reads and closes: every row, in order, each cell as the in-memory
// result would hold it. Off (no SpoolMaxBytes), the same result is refused
// as too large, as before. A result that fits stays in memory and leaves no
// file behind.
func TestRun_spoolsAResultPastTheMemoryCap(t *testing.T) {
	f := newCopyFixture(t)
	const q = "SELECT i, 'row ' || i AS s, i::DECIMAL(10,2) / 4 AS d FROM range(5000) t(i) ORDER BY i"

	t.Run("off", func(t *testing.T) {
		r, root := newSpoolRunner(t, spoolLimits(0), 1)
		_, err := r.Run(context.Background(), f.job(q))
		if !errors.Is(err, ErrResultTooLarge) {
			t.Fatalf("with no spool: %v, want ErrResultTooLarge", err)
		}
		if d := spoolDirs(t, root); len(d) != 0 {
			t.Errorf("left %v behind", d)
		}
	})

	for _, statements := range []int{1, 3} { // a worker per statement, and one that stays
		t.Run(fmt.Sprintf("on, %d statements per worker", statements), func(t *testing.T) {
			r, root := newSpoolRunner(t, spoolLimits(64<<20), statements)
			mem, err := r.Run(context.Background(), f.job("SELECT i, 'row ' || i AS s, i::DECIMAL(10,2) / 4 AS d FROM range(20) t(i) ORDER BY i"))
			if err != nil {
				t.Fatal(err)
			}
			if mem.Spool != nil || len(mem.Rows) != 20 {
				t.Fatalf("a small result: spool %v, %d rows; want 20 rows in memory", mem.Spool, len(mem.Rows))
			}
			if d := spoolDirs(t, root); len(d) != 0 {
				t.Errorf("a result held in memory left %v behind", d)
			}

			res, err := r.Run(context.Background(), f.job(q))
			if err != nil {
				t.Fatal(err)
			}
			if res.Spool == nil || res.Rows != nil {
				t.Fatalf("a large result: spool %v, %d rows in memory; want it spooled", res.Spool, len(res.Rows))
			}
			defer res.Spool.Close()
			if res.Spool.Rows() != 5000 || res.Truncated {
				t.Errorf("spool says %d rows, truncated %v; want 5000, not truncated", res.Spool.Rows(), res.Truncated)
			}
			if len(res.Columns) != 3 || res.Columns[2].Type == "" {
				t.Errorf("columns %+v", res.Columns)
			}
			rows := collect(t, res.Spool)
			if len(rows) != 5000 {
				t.Fatalf("read %d rows, want 5000", len(rows))
			}
			for i, row := range rows {
				if fmt.Sprint(row[0]) != fmt.Sprint(i) || row[1] != fmt.Sprintf("row %d", i) {
					t.Fatalf("row %d = %v: out of order or wrong", i, row)
				}
			}
			// The cells are what the in-memory result holds: numbers as
			// json.Number, strings as strings.
			if _, ok := rows[1][0].(json.Number); !ok {
				t.Errorf("an integer cell is %T, want json.Number as in memory", rows[1][0])
			}
			if _, ok := rows[1][2].(json.Number); !ok {
				t.Errorf("a decimal cell is %T (%v), want json.Number as in memory", rows[1][2], rows[1][2])
			}
			// Read again: the same rows.
			if again := collect(t, res.Spool); len(again) != 5000 {
				t.Errorf("a second read: %d rows", len(again))
			}
			if err := res.Spool.Close(); err != nil {
				t.Fatal(err)
			}
			if err := res.Spool.Close(); err != nil {
				t.Errorf("a second Close: %v", err)
			}
			if err := res.Spool.Each(func([]any) error { return nil }); err == nil {
				t.Error("Each after Close read nothing and said nothing")
			}
			if d := spoolDirs(t, root); len(d) != 0 {
				t.Errorf("Close left %v behind", d)
			}
			// The worker goes on: the next statement answers.
			if _, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil {
				t.Errorf("the next statement: %v", err)
			}
		})
	}
}

// A spool has a cap of its own; past it the result is refused as too large,
// as an in-memory one past its cap is, and the file is removed. The row cap
// still cuts a spooled result.
func TestRun_spoolCaps(t *testing.T) {
	f := newCopyFixture(t)

	r, root := newSpoolRunner(t, spoolLimits(64<<10), 1)
	_, err := r.Run(context.Background(), f.job("SELECT i, repeat('x', 100) AS s FROM range(100000) t(i)"))
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("past the spool's cap: %v, want ErrResultTooLarge", err)
	}
	if d := spoolDirs(t, root); len(d) != 0 {
		t.Errorf("a refused spool left %v behind", d)
	}

	l := spoolLimits(64 << 20)
	l.MaxRows = 3000
	r, root = newSpoolRunner(t, l, 1)
	res, err := r.Run(context.Background(), f.job("SELECT i, 'row ' || i FROM range(5000) t(i)"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Spool == nil {
		t.Fatal("not spooled")
	}
	defer res.Spool.Close()
	if res.Spool.Rows() != 3000 || !res.Truncated || len(collect(t, res.Spool)) != 3000 {
		t.Errorf("past the row cap: %d rows, truncated %v; want 3000, truncated", res.Spool.Rows(), res.Truncated)
	}
	_ = root
}

// A statement that fails after rows were written, or runs out of time while
// writing them, fails as it always did, and its file goes with it.
func TestRun_spoolFailuresLeaveNothing(t *testing.T) {
	f := newCopyFixture(t)

	r, root := newSpoolRunner(t, spoolLimits(64<<20), 1)
	_, err := r.Run(context.Background(), f.job("SELECT CASE WHEN i = 4000 THEN error('boom') ELSE i END AS v, 'row ' || i FROM range(5000) t(i)"))
	var qe *QueryError
	if !errors.As(err, &qe) || !strings.Contains(qe.Message, "boom") {
		t.Fatalf("a failure mid-spool: %v, want the QueryError", err)
	}
	if d := spoolDirs(t, root); len(d) != 0 {
		t.Errorf("a failed statement left %v behind", d)
	}

	l := spoolLimits(256 << 20) // the clock, not the cap, ends this one
	l.Timeout = 2 * time.Second
	r, root = newSpoolRunner(t, l, 1)
	_, err = r.Run(context.Background(), f.job("SELECT a.i, b.i FROM range(100000) a(i), range(100000) b(i)"))
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("out of time mid-spool: %v, want a TimeoutError", err)
	}
	if d := spoolDirs(t, root); len(d) != 0 {
		t.Errorf("a statement out of time left %v behind", d)
	}
}

// A spool is read through a file the parent opened before handing it over:
// a sweep (or anything else) that removes its directory meanwhile does not
// cut a read under way. The sweep removes only spools older than the stale
// age, never a fresh one.
func TestSpool_survivesRemovalAndTheSweepIsByAge(t *testing.T) {
	f := newCopyFixture(t)
	r, root := newSpoolRunner(t, spoolLimits(64<<20), 1)
	res, err := r.Run(context.Background(), f.job("SELECT i, 'row ' || i FROM range(5000) t(i) ORDER BY i"))
	if err != nil || res.Spool == nil {
		t.Fatalf("not spooled: %v", err)
	}
	defer res.Spool.Close()
	sweepSpill(root, time.Now().Add(-time.Hour))
	if d := spoolDirs(t, root); len(d) != 1 {
		t.Fatalf("the sweep took a fresh spool: %v left", d)
	}
	for _, d := range spoolDirs(t, root) {
		if err := os.RemoveAll(filepath.Join(root, d)); err != nil {
			t.Fatal(err)
		}
	}
	if rows := collect(t, res.Spool); len(rows) != 5000 {
		t.Errorf("after its directory was removed: %d rows, want 5000", len(rows))
	}

	stale := filepath.Join(root, spoolPrefix+"stale")
	if err := os.Mkdir(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	sweepSpill(root, time.Now().Add(-time.Hour))
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("a stale spool was not swept: %v", err)
	}
}

// Cells cut at MaxCellBytes are counted in a spooled result as in memory.
func TestRun_spoolCountsCutCells(t *testing.T) {
	f := newCopyFixture(t)
	r, _ := newSpoolRunner(t, spoolLimits(64<<20), 1)
	res, err := r.Run(context.Background(), f.job(fmt.Sprintf("SELECT i, CASE WHEN i = 7 THEN repeat('y', %d) ELSE 'x' END FROM range(5000) t(i)", MaxCellBytes+10)))
	if err != nil || res.Spool == nil {
		t.Fatalf("not spooled: %v", err)
	}
	defer res.Spool.Close()
	if res.TruncatedCells != 1 {
		t.Errorf("TruncatedCells = %d, want 1", res.TruncatedCells)
	}
}

// A file that holds fewer rows than the worker said it wrote (cut short on a
// full disk, damaged) is an error at its end, never a short answer taken as
// whole; and a line that does not read is an error naming its row.
func TestSpool_shortOrDamagedFileIsAnError(t *testing.T) {
	for name, content := range map[string]string{
		"short":   "[1,\"a\"]\n[2,\"b\"]\n",
		"damaged": "[1,\"a\"]\n[2,\"b\n",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, spoolFile), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			s, err := openSpool(dir, 3, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var got int
			err = s.Each(func([]any) error { got++; return nil })
			if err == nil {
				t.Fatalf("read %d rows of 3 and said nothing", got)
			}
		})
	}
}

// SpoolMaxBytes is the statement's to ask for: one in the Runner's limits
// does not reach a statement that did not, whose caller reads only Rows and
// would take a spooled result for an empty one.
func TestRun_spoolIsNeverInherited(t *testing.T) {
	f := newCopyFixture(t)
	r, root := newSpoolRunner(t, spoolLimits(64<<20), 1)
	job := f.job("SELECT i, 'row ' || i FROM range(5000) t(i)")
	job.Limits = Limits{MaxResultBytes: 4 << 10}
	_, err := r.Runner.Run(context.Background(), job)
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("a statement that asked for no file: %v, want ErrResultTooLarge", err)
	}
	if d := spoolDirs(t, root); len(d) != 0 {
		t.Errorf("left %v behind", d)
	}
}

// Result files keep to the spill directories' rules: none on a disk in
// memory, and all those alive at once within half the free disk. A file
// that cannot be had refuses the large result as before, and its share of
// the disk comes back when the file is closed.
func TestRun_spoolReservesTheDisk(t *testing.T) {
	f := newCopyFixture(t)
	q := "SELECT i, 'row ' || i FROM range(5000) t(i)"
	defer func(old func(string) (int64, bool, error)) { spillFSFor = old }(spillFSFor)

	t.Run("a disk in memory", func(t *testing.T) {
		spillFSFor = func(string) (int64, bool, error) { return 1 << 40, true, nil }
		r, root := newSpoolRunner(t, spoolLimits(64<<20), 1)
		if _, err := r.Run(context.Background(), f.job(q)); !errors.Is(err, ErrResultTooLarge) {
			t.Fatalf("on tmpfs: %v, want ErrResultTooLarge", err)
		}
		if d := spoolDirs(t, root); len(d) != 0 {
			t.Errorf("left %v behind", d)
		}
	})

	t.Run("half the free disk", func(t *testing.T) {
		// Room for one 64 MB file and not two.
		spillFSFor = func(string) (int64, bool, error) { return 200 << 20, false, nil }
		r, _ := newSpoolRunner(t, spoolLimits(64<<20), 1)
		first, err := r.Run(context.Background(), f.job(q))
		if err != nil || first.Spool == nil {
			t.Fatalf("the first: %v, spool %v", err, first.Spool)
		}
		if _, err := r.Run(context.Background(), f.job(q)); !errors.Is(err, ErrResultTooLarge) {
			t.Fatalf("a second while the first is open: %v, want ErrResultTooLarge", err)
		}
		if err := first.Spool.Close(); err != nil {
			t.Fatal(err)
		}
		again, err := r.Run(context.Background(), f.job(q))
		if err != nil || again.Spool == nil {
			t.Fatalf("after the first closed: %v, spool %v", err, again.Spool)
		}
		again.Spool.Close()
		// A refused statement gives its share back too: the next one has it.
		big, err := r.Run(context.Background(), f.job("SELECT i, repeat('x', 100) FROM range(1000000) t(i)"))
		if !errors.Is(err, ErrResultTooLarge) {
			t.Fatalf("past the cap: %v, spool %v", err, big.Spool)
		}
		last, err := r.Run(context.Background(), f.job(q))
		if err != nil || last.Spool == nil {
			t.Fatalf("after a refused one: %v", err)
		}
		last.Spool.Close()
		r.spillMu.Lock()
		left := r.spoolReserved
		r.spillMu.Unlock()
		if left != 0 {
			t.Errorf("%d bytes still reserved with every file closed", left)
		}
	})
}

// A result file that cannot be written (its folder gone, a full disk) is the
// server's trouble, not the statement's: answered as a result too large,
// never as a query error the client would take for its own.
func TestRun_spoolWriteFailureIsTooLarge(t *testing.T) {
	f := newCopyFixture(t)
	r, root := newSpoolRunner(t, spoolLimits(64<<20), 1)
	r.afterSpoolDir = func(dir string) {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}
	_, err := r.Run(context.Background(), f.job("SELECT i, 'row ' || i FROM range(5000) t(i)"))
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("an unwritable file: %v, want ErrResultTooLarge", err)
	}
	var qe *QueryError
	if errors.As(err, &qe) {
		t.Errorf("reads as the statement's error: %v", err)
	}
	if d := spoolDirs(t, root); len(d) != 0 {
		t.Errorf("left %v behind", d)
	}
}

// A reader may close the Spool from inside Each (a client that left): the
// read stops with an error and nothing hangs.
func TestSpool_closeInsideEach(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, spoolFile), []byte("[1]\n[2]\n[3]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	released := false
	s, err := openSpool(dir, 3, func() { released = true })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Each(func([]any) error { return s.Close() }) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close inside Each hung")
	}
	if !released {
		t.Error("Close did not give the disk back")
	}
	if err := s.Each(func([]any) error { return nil }); err == nil {
		t.Error("Each after Close said nothing")
	}
}

// SpoolRoom answers what reserveSpool would, without reserving: no file on
// a disk in memory, nor past half the free disk with the files alive.
func TestRunner_spoolRoom(t *testing.T) {
	defer func(old func(string) (int64, bool, error)) { spillFSFor = old }(spillFSFor)
	r, _ := newSpoolRunner(t, spoolLimits(64<<20), 1)
	spillFSFor = func(string) (int64, bool, error) { return 1 << 40, true, nil }
	if r.SpoolRoom(64 << 20) {
		t.Error("room on tmpfs")
	}
	spillFSFor = func(string) (int64, bool, error) { return 200 << 20, false, nil }
	if !r.SpoolRoom(64 << 20) {
		t.Error("no room for one 64 MB file in 200 MB free")
	}
	release, err := r.reserveSpool(64 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if r.SpoolRoom(64 << 20) {
		t.Error("room for a second 64 MB file in 200 MB free")
	}
	release()
	if !r.SpoolRoom(64 << 20) {
		t.Error("no room once the first was given back")
	}
}
