package reconstruct

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// fileColumns is the columns a Parquet file holds, sorted.
func fileColumns(t *testing.T, path string) []string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(fmt.Sprintf("SELECT name FROM parquet_schema('%s') WHERE coalesce(num_children, 0) = 0", path))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// The state views list a table's columns from the CREATE TABLE in the BASE
// file's footer and read the chain's upserts through that list (#2111), so a
// column an upserts file held and the base did not would be hidden with no
// error. This pins why that cannot happen: a table delta takes its columns
// from the base's own CREATE TABLE, never from the events it folds, and a
// window whose events carry a column the base lacks (an ALTER ... ADD COLUMN
// between two refreshes) is refused whole, before anything is written.
func TestTableDelta_upsertsHoldTheBaseColumnsAndNoOther_2111(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	want := append(fileColumns(t, src), baseline.TableDeltaPKColumn, baseline.TableDeltaOpColumn)
	sort.Strings(want)

	// An ordinary window, then one whose row images LACK a column of the
	// base: each upserts file has exactly the base's columns and the two
	// technical ones.
	base, prevTime := src, t0
	short := upd(2, "short")
	delete(short.RowAfter, "ratio")
	for i, changes := range []map[string]*query.ResultRow{changeMap(upd(1, "v1"), ins(9, "new")), changeMap(short)} {
		at := t0.Add(time.Duration(i+1) * time.Hour)
		nb, _, err := deltaWindow(t, root, base, prevTime, changes, at, &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * (i + 1))}, nil)
		if err != nil {
			t.Fatalf("window %d: %v", i+1, err)
		}
		_, ups := baseline.TableDeltaPaths(nb, i)
		if got := fileColumns(t, ups); !slices.Equal(got, want) {
			t.Errorf("window %d: the upserts file holds %v, want the base's columns and the two technical ones %v", i+1, got, want)
		}
		base, prevTime = nb, at
	}

	// A window whose events carry a column the base does not have.
	added := upd(1, "after the alter")
	added.RowAfter["added_later"] = "x"
	at := t0.Add(3 * time.Hour)
	nb, _, err := deltaWindow(t, root, base, prevTime, changeMap(added), at, &query.BinlogPos{File: "binlog.000009", Pos: 3000}, nil)
	if !errors.Is(err, ErrSchemaChanged) {
		t.Fatalf("a window with a column the base lacks: err = %v, want ErrSchemaChanged", err)
	}
	if left, _ := filepath.Glob(filepath.Join(filepath.Dir(nb), "*")); len(left) != 0 {
		t.Errorf("the refused window left files behind: %v", left)
	}
	if _, err := os.Stat(nb); !os.IsNotExist(err) {
		t.Errorf("the refused window published the table: %v", err)
	}
}
