package console

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A statement run once more on the newest snapshot (#2678) runs under the
// session it was given, not one made again: the PostgreSQL port's join order
// for a statement shape that needed the order written (KeepJoinOrder) and
// its join check's counts (JoinNDV) reach the second run too.
func TestSQLStatement_runOnceMoreKeepsTheSession(t *testing.T) {
	runner := &hookRunner{}
	f := newSQLFixture(t, runner, false)
	old := filepath.Base(filepath.Dir(f.schemaDir))
	at, ok := snapshotdir.ParseTime(old)
	if !ok {
		t.Fatalf("the fixture's snapshot folder %q is not a snapshot name", old)
	}
	newer := snapshotdir.Name(at.Add(time.Hour))
	var seen []sqlsandbox.Session
	runner.fn = func(n int, job sqlsandbox.Job) (sqlsandbox.Result, error) {
		seen = append(seen, job.Session)
		if n == 1 {
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
	counts := &sqlsandbox.JoinCounts{NDV: map[string]float64{"id": 3}}
	sess := sqlsandbox.Session{KeepJoinOrder: true, JoinNDV: counts, RetryOutOfMemoryInWrittenOrder: true}
	if _, err := f.s.runSQLVouched(context.Background(), f.s.cm.boot, "u", "SELECT count(*) FROM shop.orders", "", 0, sess, nil); err != nil {
		t.Fatalf("not answered: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("ran %d times, want 2", len(seen))
	}
	for i, got := range seen {
		if !got.KeepJoinOrder || got.JoinNDV != counts || !got.RetryOutOfMemoryInWrittenOrder {
			t.Errorf("run %d: KeepJoinOrder %v, JoinNDV %v, retry %v", i+1, got.KeepJoinOrder, got.JoinNDV, got.RetryOutOfMemoryInWrittenOrder)
		}
	}
}
