package sqlsandbox

import (
	"os"
	"testing"
)

// A result is cut by its cells in the worker, as it reads rows (#3286): the
// rows past MaxCells / columns are never decoded, so the caller's memory
// follows the cells. Never under CellsFloorRows; 0 cuts nothing.
func TestWorker_cutsByCells(t *testing.T) {
	f := newCopyFixture(t)
	run := func(sql string, maxRows, maxCells, floor int) wireResult {
		t.Helper()
		res := runJob(wireJob{
			CopyDirs: []string{f.archiveRoot}, SQL: sql,
			Threads: 1, MemoryLimit: "256MB", MaxRows: maxRows, MaxResultBytes: 64 << 20,
			MaxCells: maxCells, CellsFloorRows: floor,
		}, nil, os.Stderr)
		if res.Error != nil {
			t.Fatalf("%s: %+v", sql, res.Error)
		}
		return res
	}
	// 4 columns: 1,000 cells is 250 rows, over a floor of 100.
	narrow := "SELECT i, i, i, i FROM range(1000) t(i)"
	if res := run(narrow, 2000, 1000, 100); len(res.Rows) != 250 || !res.Truncated || !res.CutByCells {
		t.Errorf("narrow: %d rows, truncated %v, by cells %v; want 250 cut by cells", len(res.Rows), res.Truncated, res.CutByCells)
	}
	// 20 columns: 1,000 cells is 50 rows, under the floor of 100: 100 rows.
	wide := "SELECT " + cols20 + " FROM range(1000) t(i)"
	if res := run(wide, 2000, 1000, 100); len(res.Rows) != 100 || !res.Truncated || !res.CutByCells {
		t.Errorf("wide: %d rows, truncated %v, by cells %v; want the floor's 100 cut by cells", len(res.Rows), res.Truncated, res.CutByCells)
	}
	// The row cap lower than the cells': cut by rows, as before.
	if res := run(narrow, 100, 1000, 10); len(res.Rows) != 100 || !res.Truncated || res.CutByCells {
		t.Errorf("row cap: %d rows, truncated %v, by cells %v; want 100 cut by rows", len(res.Rows), res.Truncated, res.CutByCells)
	}
	// A result under both: whole.
	if res := run("SELECT i, i FROM range(100) t(i)", 2000, 1000, 10); len(res.Rows) != 100 || res.Truncated || res.CutByCells {
		t.Errorf("small: %d rows, truncated %v, by cells %v", len(res.Rows), res.Truncated, res.CutByCells)
	}
	// 0: no cut by cells (every caller but the PostgreSQL port).
	if res := run(wide, 2000, 0, 0); len(res.Rows) != 1000 || res.Truncated {
		t.Errorf("off: %d rows, truncated %v", len(res.Rows), res.Truncated)
	}
}

const cols20 = "i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i"
