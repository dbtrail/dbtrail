package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2243: DuckDB takes a path given to read_parquet / parquet_scan as a glob.
// A table whose name holds a pattern character, beside a table the pattern
// matches, must have every read of the daemon reach its own files only: these
// reads decide what a refresh writes.

// globTables are table names beside a table named "orders". "order[st]" is
// the sharp one: as a pattern it matches "orders" and not itself.
// The two with a backslash are no patterns: the listing of a table's chain
// cut a name there, so `a\orders` was given the chain of "orders".
var globTables = []string{"or?ers", "or*s", "order[st]", "{orders,x}", `a\orders`, `\orders`}

// copyTableFile copies src to dst, creating dst's directory.
func copyTableFile(t *testing.T, src, dst string) {
	t.Helper()
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// globWindow is deltaWindow for a table of any name: one refresh with table
// deltas on, from prevBase, into the snapshot of `at` under root.
func globWindow(t *testing.T, root, table, prevBase string, prevDirTime time.Time, changes map[string]*query.ResultRow, at time.Time, pos uint64) (string, error) {
	t.Helper()
	bmeta, err := baseline.ReadParquetMetadata(prevBase)
	if err != nil {
		t.Fatalf("read base footer: %v", err)
	}
	prev, err := readTableDelta(context.Background(), prevBase, bmeta)
	if err != nil {
		return "", fmt.Errorf("readTableDelta: %w", err)
	}
	chainStart := prevDirTime
	if prev != nil {
		chainStart = prev.Meta.DeltaChainStart
	}
	_, anchorMeta := fetchFloor(prevDirTime, bmeta, prev)
	snapDir := filepath.Join(root, SnapshotDirName(at))
	p := tableDeltaPublish{
		cfg:    FullTableConfig{At: at, OutputFormat: OutputFormatParquet, TableDeltas: true, snapshotDir: snapDir, cut: &query.BinlogPos{File: "binlog.000009", Pos: pos}},
		schema: "mydb", table: table,
		basePath: prevBase, chainStart: chainStart, baseMeta: bmeta, anchorMeta: anchorMeta,
		prev: prev, fold: &foldResult{Changes: changes}, pkCols: pkColsIntID(),
		streamCaptured: true,
	}
	err = publishWithTableDelta(context.Background(), p, &TableReport{Schema: "mydb", Table: table})
	return filepath.Join(snapDir, "mydb", table+".parquet"), err
}

// TestRefresh_aTableNamedLikeAGlobKeepsItsOwnState runs two refreshes with
// table deltas on over a table named like a pattern, beside a table named
// "orders" whose file holds other rows in other positions and which has a
// chain of its own, and reads the table's state back after each.
func TestRefresh_aTableNamedLikeAGlobKeepsItsOwnState(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	own := writeZooBaseline(t, rows, nulls)
	// The neighbour: the same rows in reverse, so every key sits at another
	// row number, less the row of id 1, so its rows are not the table's.
	nrows, nnulls := [][]string{}, [][]bool{}
	for i := len(rows) - 1; i >= 1; i-- {
		nrows, nnulls = append(nrows, rows[i]), append(nnulls, nulls[i])
	}
	neighbour := writeZooBaseline(t, nrows, nnulls)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	t1, t2 := t0.Add(5*time.Minute), t0.Add(10*time.Minute)

	for _, table := range globTables {
		t.Run(table, func(t *testing.T) {
			root := t.TempDir()
			at := func(ts time.Time, name string) string {
				return filepath.Join(root, SnapshotDirName(ts), "mydb", name+".parquet")
			}
			copyTableFile(t, own, at(t0, table))
			copyTableFile(t, neighbour, at(t0, "orders"))
			// The reads of the table's file alone: a rewrite's scan, the
			// row lookup of a cascade, the column list.
			ownRows := byID(readSnapshotRows(t, own))
			if got := byID(readSnapshotRows(t, at(t0, table))); !reflect.DeepEqual(got, ownRows) {
				t.Fatalf("the scan of the table's file:\n got %v\nwant %v", got, ownRows)
			}
			if got, err := ReadBaselineRows(context.Background(), at(t0, table), nil, 0); err != nil || len(got) != len(rows) {
				t.Fatalf("ReadBaselineRows: %d rows (err=%v), want the table's %d", len(got), err, len(rows))
			}
			if got, err := ReadBaselineRows(context.Background(), at(t0, table), map[string]string{"id": "1"}, 0); err != nil || len(got) != 1 {
				t.Fatalf("ReadBaselineRows of id 1: %d rows (err=%v), want 1 (the neighbour has none)", len(got), err)
			}
			// The neighbour's refreshes: its own changes, each window.
			nb1, err := globWindow(t, root, "orders", at(t0, "orders"), t0, changeMap(upd(2, "neighbour"), ins(77, "neighbour")), t1, 1000)
			if err != nil {
				t.Fatalf("the neighbour's first refresh: %v", err)
			}
			base1, err := globWindow(t, root, table, at(t0, table), t0, changeMap(upd(1, "one"), del(3), ins(50, "fifty")), t1, 1000)
			if err != nil {
				t.Fatalf("first refresh: %v", err)
			}
			want1 := byID(wantZooState(t, own, changeMap(upd(1, "one"), del(3), ins(50, "fifty"))))
			if got := byID(deltaState(t, base1)); !reflect.DeepEqual(got, want1) {
				t.Fatalf("state after the first refresh:\n got %v\nwant %v", got, want1)
			}
			if _, err := globWindow(t, root, "orders", nb1, t1, changeMap(del(2), ins(78, "neighbour")), t2, 2000); err != nil {
				t.Fatalf("the neighbour's second refresh: %v", err)
			}
			base2, err := globWindow(t, root, table, base1, t1, changeMap(upd(4, "four"), del(50), ins(51, "fifty-one")), t2, 2000)
			if err != nil {
				t.Fatalf("second refresh: %v", err)
			}
			want2 := byID(wantZooState(t, own, changeMap(upd(1, "one"), del(3), upd(4, "four"), ins(51, "fifty-one"))))
			if got := byID(deltaState(t, base2)); !reflect.DeepEqual(got, want2) {
				t.Fatalf("state after the second refresh:\n got %v\nwant %v", got, want2)
			}
			// The chain merged into one pair, as a compaction and the
			// resolved pair do: the state through it is the chain's.
			chain := listedChain(t, base2)
			if len(chain.Files) != 2 {
				t.Fatalf("chain = %+v, want two pairs", chain.Files)
			}
			validateWith(t, func(string) error { return nil })
			if done, _, err := ResolveTableDelta(context.Background(), base2, chain, ""); err != nil || !done {
				t.Fatalf("ResolveTableDelta: done=%v err=%v", done, err)
			}
			r, ok := baseline.FindResolvedTableDelta(base2, chain.Files)
			if !ok {
				t.Fatal("no resolved pair")
			}
			file := func(p string) string { return sqlLit(duckdbutil.FileGlob(p)) }
			var posdels, upserts []string
			for _, f := range chain.Files {
				posdels, upserts = append(posdels, file(f.Posdel)), append(upserts, file(f.Upserts))
			}
			through := jsonRows(t, baseline.TableDeltaOnePairStateSQL(file(base2), file(r.Posdel), file(r.Upserts), ""))
			chainState := jsonRows(t, baseline.TableDeltaStateSQL(file(base2), "["+strings.Join(posdels, ", ")+"]", "["+strings.Join(upserts, ", ")+"]", base2, ""))
			if len(chainState) != len(want2) || !reflect.DeepEqual(through, chainState) {
				t.Fatalf("state through the resolved pair:\n %v\nthrough the chain (%d rows, want %d):\n %v", through, len(chainState), len(want2), chainState)
			}
		})
	}
}

// wantZooState is the state a table reaches from the file at base by the
// changes, worked out through a table with a plain name in a directory of
// its own, where no pattern can reach anything else.
func wantZooState(t *testing.T, base string, changes map[string]*query.ResultRow) []map[string]any {
	t.Helper()
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	start := filepath.Join(root, SnapshotDirName(t0), "mydb", "plain.parquet")
	copyTableFile(t, base, start)
	out, err := globWindow(t, root, "plain", start, t0, changes, t0.Add(5*time.Minute), 1000)
	if err != nil {
		t.Fatalf("the plain table's refresh: %v", err)
	}
	return deltaState(t, out)
}

// TestReadBaselineColumns_aTableNamedLikeAGlob: "order[st]" as a pattern
// matches "orders" and not itself, so the column list of the one came from
// the file of the other.
func TestReadBaselineColumns_aTableNamedLikeAGlob(t *testing.T) {
	rows, nulls := zooRows()
	dir := filepath.Join(t.TempDir(), "mydb")
	path := filepath.Join(dir, "order[st].parquet")
	copyTableFile(t, writeZooBaseline(t, rows, nulls), path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	if _, err := ddb.Exec("COPY (SELECT 1 AS other) TO " + sqlLit(filepath.Join(dir, "orders.parquet")) + " (FORMAT PARQUET)"); err != nil {
		t.Fatal(err)
	}
	cols, err := readBaselineColumns(context.Background(), path, duckdbutil.Tuning{})
	if err != nil || len(cols) != 8 || !slices.Contains(cols, "id") {
		t.Fatalf("readBaselineColumns = %v (err=%v), want the table's own 8 columns", cols, err)
	}
}
