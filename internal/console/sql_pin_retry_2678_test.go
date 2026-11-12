package console

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// hookRunner runs each statement through fn, which sees the job the way the
// worker would.
type hookRunner struct {
	fn func(n int, job sqlsandbox.Job) (sqlsandbox.Result, error)
	n  int
}

func (h *hookRunner) Reserve(context.Context, string) (sqlSlot, error) { return hookSlot{h}, nil }

type hookSlot struct{ h *hookRunner }

func (s hookSlot) Run(_ context.Context, job sqlsandbox.Job) (sqlsandbox.Result, error) {
	s.h.n++
	return s.h.fn(s.h.n, job)
}
func (hookSlot) Release() {}

// A statement on the copy and the cleanup (#2678), through the real run:
// while it runs, the ONE snapshot it reads is pinned and the folder is not
// stopped; when it ends nothing is pinned; and if its snapshot is removed
// while it runs, it is run once more, on the newest snapshot.
func TestSQLStatement_pinsItsSnapshotAndRunsOnceMoreIfItIsRemoved(t *testing.T) {
	var pinned [][]string
	var folder []string
	var f *sqlFixture
	var newer string
	runner := &hookRunner{}
	f = newSQLFixture(t, runner, false)
	old := filepath.Base(filepath.Dir(f.schemaDir))
	at, ok := snapshotdir.ParseTime(old)
	if !ok {
		t.Fatalf("the fixture's snapshot folder %q is not a snapshot name", old)
	}
	newer = snapshotdir.Name(at.Add(time.Hour))
	runner.fn = func(n int, _ sqlsandbox.Job) (sqlsandbox.Result, error) {
		pinned = append(pinned, baseline.Pinned(f.root))
		folder = append(folder, baseline.Reading(f.root))
		if n == 1 {
			// The next snapshot is published and the cleanup removes the
			// one this statement is on (a count of 1, the pin lost to a
			// race): the worker's read fails.
			if err := os.CopyFS(filepath.Join(f.root, newer), os.DirFS(filepath.Join(f.root, old))); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(filepath.Join(f.root, old)); err != nil {
				t.Fatal(err)
			}
			return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "IO Error: No files found that match the pattern"}
		}
		return sqlsandbox.Result{}, nil
	}
	if _, err := f.s.runSQLVouched(context.Background(), f.s.cm.boot, "u", "SELECT count(*) FROM shop.orders", "", 0, sqlsandbox.Session{}, nil); err != nil {
		t.Fatalf("the statement whose snapshot was removed under it was not answered: %v", err)
	}
	if runner.n != 2 {
		t.Fatalf("the statement ran %d times, want 2 (once more on the newest snapshot)", runner.n)
	}
	if !slices.Equal(pinned[0], []string{old}) || !slices.Equal(pinned[1], []string{newer}) {
		t.Fatalf("pinned while running: %v, want [%s] then [%s]", pinned, old, newer)
	}
	if folder[0] != "" || folder[1] != "" {
		t.Fatalf("a statement stopped the cleanup of the whole folder: %q", folder)
	}
	if got := baseline.Pinned(f.root); len(got) != 0 {
		t.Fatalf("the statement is over and %v is still pinned", got)
	}

	// An error with the snapshot still there is the statement's own: once.
	runner.n = 0
	runner.fn = func(int, sqlsandbox.Job) (sqlsandbox.Result, error) {
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Binder Error: no such column"}
	}
	_, err := f.s.runSQLVouched(context.Background(), f.s.cm.boot, "u", "SELECT nope FROM shop.orders", "", 0, sqlsandbox.Session{}, nil)
	var qe *sqlsandbox.QueryError
	if !errors.As(err, &qe) || runner.n != 1 {
		t.Fatalf("an ordinary error: ran %d times, err %v; want once and the error", runner.n, err)
	}
}
