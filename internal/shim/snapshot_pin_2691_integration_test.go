//go:build integration

package shim

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A `_snapshot` query, one row or the whole table, keeps the snapshot it
// reads from a cleanup that runs while it reads (#2691), through the PinFunc
// its Config carries: the cleanup runs to a count of 1 right after the pin,
// the older snapshot the query is on stays and answers, and it is let go
// when the query ends.
func TestSnapshotPin_aCleanupDuringTheQueryKeepsItsSnapshot(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	hourTop := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	addHourlyPartition(t, db, hourTop)
	seedUsersSnapshot(t, db, hourTop.Add(time.Minute))
	oldAt, asOf, newAt := hourTop.Add(5*time.Minute), hourTop.Add(10*time.Minute), hourTop.Add(20*time.Minute)

	for _, q := range []TimeTravelQuery{
		{Type: TypeSnapshot, Schema: "myapp", Table: "users", PKColumn: "id", PKValue: "1", AsOf: asOf},
		{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf},
	} {
		folder := writeBaselineSnapshot(t, oldAt, "myapp", "users", usersBaselineCols(), [][]string{{"1", "older"}})
		other := writeBaselineSnapshot(t, newAt, "myapp", "users", usersBaselineCols(), [][]string{{"1", "newer"}})
		newName := newAt.UTC().Format("2006-01-02T15-04-05") + "Z"
		if err := os.Rename(filepath.Join(other, newName), filepath.Join(folder, newName)); err != nil {
			t.Fatal(err)
		}
		var readers []string
		var results []baseline.PruneResult
		pin := func(_ context.Context, path, what string) func() {
			readers = append(readers, what)
			root, name, ok := baseline.TableFileSnapshot(path)
			if !ok {
				t.Errorf("the lookup answered %q, not a table file in a snapshot", path)
				return func() {}
			}
			release := baseline.PinSnapshot(root, name, what, time.Hour)
			res, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: folder, KeepNewest: 1})
			if err != nil {
				t.Errorf("cleanup during the query: %v", err)
			}
			results = append(results, res)
			return release
		}
		h := NewHandlerWithConfig(db, Config{AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: folder, PinBaseline: pin}, slog.Default())
		res, err := h.runSnapshot(q)
		if err != nil {
			t.Fatalf("pk %q: %v", q.PKColumn, err)
		}
		if cells := rowCells(t, res.Resultset); len(cells) != 1 || !slices.Equal(cells[0], []string{"1", "older"}) {
			t.Fatalf("pk %q answered %v, want the older snapshot's row [[1 older]]", q.PKColumn, cells)
		}
		if !slices.Equal(readers, []string{"a _snapshot query"}) {
			t.Fatalf("pk %q: readers that pinned %v, want one _snapshot query: it reads without a pin", q.PKColumn, readers)
		}
		if len(results) != 1 || results[0].KeptPinned != 1 || len(results[0].Pruned) != 0 {
			t.Fatalf("pk %q: the cleanup during the query: %+v, want its snapshot kept as pinned and nothing removed", q.PKColumn, results)
		}
		if got := baseline.Pinned(folder); len(got) != 0 {
			t.Fatalf("pk %q: the query is over and %v is still pinned", q.PKColumn, got)
		}
	}
}
