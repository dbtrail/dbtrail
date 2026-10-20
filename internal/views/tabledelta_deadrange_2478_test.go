package views

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2478, second slice: a pinned view over one pair whose dead row numbers all
// fall in one stretch of at most half the base reads the rest of the base
// without the dead-row join. rangeMark is the predicate that confines the
// join to that stretch.
const rangeMark = "file_row_number BETWEEN"

// writeSnapshotRows writes a marked snapshot whose base has n rows, ids
// 1..n, status "s<id>" (writeSnapshot spells ids as one character, so it
// stops at nine).
func writeSnapshotRows(t *testing.T, root, stamp string, n int) string {
	t.Helper()
	path := filepath.Join(root, stamp, "shop", "orders.parquet")
	cols, err := baseline.ParseSchema(writeSchemaFile(t))
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if err := w.WriteRow([]string{strconv.Itoa(i + 1), fmt.Sprintf("s%d", i+1)}, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, stamp, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestStateView_deadRowsInOneStretchConfineTheJoin: where the dead rows sit
// decides the body, and every body returns the rows of the plain join form.
// The base has 20 rows (row numbers 0..19).
func TestStateView_deadRowsInOneStretchConfineTheJoin(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	cases := []struct {
		name      string
		dead      []int64
		wantRange bool
	}{
		{"at the end (a table whose recent rows change)", []int64{17, 18, 19}, true},
		{"at the start", []int64{0, 2}, true},
		{"in the middle, rows on both sides", []int64{8, 9, 11}, true},
		{"one dead row", []int64{5}, true},
		{"exactly half the base", []int64{0, 9}, true},
		{"just over half the base", []int64{0, 10}, false},
		{"spread over the whole base", []int64{0, 7, 19}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			base := writeSnapshotRows(t, root, stamp, 20)
			writeDeltaPair(t, base, 0, c.dead, [][3]string{{"42", "new", "u"}})
			tables := onePairTables(base)
			sqlText := generatePinnedMarked(t, root, tables, true)
			if got := tables[0].DeltaDeadRange != nil; got != c.wantRange {
				t.Fatalf("DeltaDeadRange set = %v, want %v (%+v)", got, c.wantRange, tables[0].DeltaDeadRange)
			}
			if c.wantRange {
				if r := tables[0].DeltaDeadRange; r.Lo != c.dead[0] || r.Hi != c.dead[len(c.dead)-1] {
					t.Fatalf("DeltaDeadRange = %+v, want %d..%d", *r, c.dead[0], c.dead[len(c.dead)-1])
				}
			}
			if confined := strings.Contains(sqlText, rangeMark); confined != c.wantRange {
				t.Fatalf("the join is confined = %v, want %v\n%s", confined, c.wantRange, sqlText)
			}
			got := stateRows(t, sqlText)
			before := stateRows(t, generatePinnedMarked(t, root, onePairTables(base), false))
			if !reflect.DeepEqual(got, before) {
				t.Fatalf("rows changed:\n with the range %v\n without it    %v", got, before)
			}
			if len(got) != 20-len(c.dead)+1 {
				t.Fatalf("%d rows, want %d: %v", len(got), 20-len(c.dead)+1, got)
			}
		})
	}
}

// TestStateView_deadRangeFromADuckDBPosdel: the resolved pair is written by
// DuckDB's COPY (reconstruct's compaction), not by the Go writer; its footer's
// statistics must give the same range, and the view the same rows.
func TestStateView_deadRangeFromADuckDBPosdel(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshotRows(t, root, stamp, 20)
	writeDeltaPair(t, base, 0, []int64{1}, [][3]string{{"2", "first", "u"}})
	writeDeltaPair(t, base, 1, nil, [][3]string{{"2", "second", "u"}})
	tables := onePairTables(base)
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatal(err)
	}
	want, ok := baseline.ResolvedTableDeltaPaths(base, tables[0].DeltaFiles)
	if !ok {
		t.Fatal("a chain of two pairs has no place for a resolved pair")
	}
	stage := filepath.Join(filepath.Dir(want.Upserts), "orders.parquet")
	writeDeltaPairAt(t, stage, 7, nil, [][3]string{{"99", "resolved", "u"}})
	stagePosdel, upserts := baseline.TableDeltaPaths(stage, 7)
	if err := os.Rename(upserts, want.Upserts); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(stagePosdel); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Rows 15..17 dead, written in no particular order, as COPY may.
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT pos::BIGINT AS pos FROM (VALUES (17), (15), (16)) v(pos)) TO '%s' (FORMAT parquet)`, want.Posdel)); err != nil {
		t.Fatal(err)
	}
	sqlText := generatePinnedMarked(t, root, tables, true)
	if r := tables[0].DeltaDeadRange; r == nil || r.Lo != 15 || r.Hi != 17 {
		t.Fatalf("DeltaDeadRange = %+v, want 15..17", r)
	}
	got := stateRows(t, sqlText)
	before := stateRows(t, generatePinnedMarked(t, root, onePairTables(base), false))
	if !reflect.DeepEqual(got, before) || len(got) != 18 {
		t.Fatalf("rows:\n with the range %v\n without it    %v", got, before)
	}
}

// TestStateView_deadRangeWithoutStatisticsKeepsTheJoin: a posdel whose only
// row is a NULL position has no min or max to read; the view keeps the plain
// join, which ignores the NULL.
func TestStateView_deadRangeWithoutStatisticsKeepsTheJoin(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshotRows(t, root, stamp, 20)
	writeDeltaPair(t, base, 0, nil, nil)
	posdel, _ := baseline.TableDeltaPaths(base, 0)
	posCols, err := baseline.PosdelColumns()
	if err != nil {
		t.Fatal(err)
	}
	pw, err := baseline.NewWriter(posdel, posCols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRow([]string{""}, []bool{true}); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	tables := onePairTables(base)
	sqlText := generatePinnedMarked(t, root, tables, true)
	if tables[0].DeltaDeadRange != nil || tables[0].DeltaNoDead {
		t.Fatalf("a NULL-only posdel was marked: range %+v, no-dead %v", tables[0].DeltaDeadRange, tables[0].DeltaNoDead)
	}
	if strings.Contains(sqlText, rangeMark) || !strings.Contains(sqlText, deadJoinMark) {
		t.Fatalf("the view is not the plain join\n%s", sqlText)
	}
	if got := stateRows(t, sqlText); len(got) != 20 {
		t.Fatalf("%d rows, want 20", len(got))
	}
}

// TestStateView_deadRowOutsideTheRangeFailsTheStatement: the pair is replaced
// under the same name after the views were generated, and the new posdel
// kills a row outside the stretch the view reads through the join. Read as
// generated, the view would show that row; it must fail instead.
func TestStateView_deadRowOutsideTheRangeFailsTheStatement(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshotRows(t, root, stamp, 20)
	writeDeltaPair(t, base, 0, []int64{18, 19}, nil)
	tables := onePairTables(base)
	sqlText := generatePinnedMarked(t, root, tables, true)
	if !strings.Contains(sqlText, rangeMark) {
		t.Fatalf("the case needs a confined join\n%s", sqlText)
	}
	writeDeltaPair(t, base, 0, []int64{3, 18, 19}, nil)
	db := execViews(t, sqlText)
	// The whole table, and the shapes that could finish without the joined
	// stretch: a filter that matches no row in it, a LIMIT the first plain
	// read alone can fill. Each must still run the check, or show id 4.
	for _, q := range []string{
		`SELECT id FROM shop.orders`,
		`SELECT id FROM shop.orders WHERE id = 4`,
		`SELECT id FROM shop.orders LIMIT 1`,
	} {
		rows, err := db.Query(q)
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err()
			rows.Close()
		}
		if err == nil || !strings.Contains(err.Error(), baseline.TableDeltaPosdelChangedError) {
			t.Fatalf("%s: read on after the posdel gained a dead row outside the range: err = %v", q, err)
		}
	}
}

// TestMarkTableDeltas_resetsTheDeadRange: the range belongs to one listing,
// like the no-dead mark.
func TestMarkTableDeltas_resetsTheDeadRange(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshotRows(t, root, stamp, 20)
	writeDeltaPair(t, base, 0, []int64{19}, nil)
	tables := onePairTables(base)
	generatePinnedMarked(t, root, tables, true)
	if tables[0].DeltaDeadRange == nil {
		t.Fatal("the case needs a marked range")
	}
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatal(err)
	}
	if tables[0].DeltaDeadRange != nil {
		t.Fatal("MarkTableDeltas kept the range of the listing before")
	}
	tables[0].DeltaDeadRange = &baseline.TableDeltaDeadRange{Lo: 19, Hi: 19}
	MarkResolvedTableDeltas(tables)
	if tables[0].DeltaDeadRange != nil {
		t.Fatal("MarkResolvedTableDeltas kept a range read for another pair")
	}
}

// TestStateView_followingViewIgnoresTheDeadRange: a following view keeps the
// plain join even for a table marked here.
func TestStateView_followingViewIgnoresTheDeadRange(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshotRows(t, root, stamp, 20)
	writeDeltaPair(t, base, 0, []int64{19}, nil)
	tables := onePairTables(base)
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatal(err)
	}
	MarkTableDeltaDeadRows(tables)
	if tables[0].DeltaDeadRange == nil {
		t.Fatal("the case needs a marked range")
	}
	if sqlText := generateMarked(root, FollowNewest, tables); strings.Contains(sqlText, rangeMark) {
		t.Fatalf("a following view confined the join\n%s", sqlText)
	}
}

// writePosdelRows writes the pair's posdel again, one row per row group, in
// the order given; "" is a NULL position. The writers keep positions
// ascending; a posdel in another order or with a NULL group is what a
// footer's bounds must still be right about.
func writePosdelRows(t *testing.T, base string, positions ...string) {
	t.Helper()
	posdel, _ := baseline.TableDeltaPaths(base, 0)
	posCols, err := baseline.PosdelColumns()
	if err != nil {
		t.Fatal(err)
	}
	pw, err := baseline.NewWriter(posdel, posCols, baseline.WriterConfig{Compression: "none", RowGroupSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range positions {
		if err := pw.WriteRow([]string{p}, []bool{p == ""}); err != nil {
			t.Fatal(err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestStateView_deadRangeSpansEveryRowGroup: the range is the smallest min and
// the largest max over ALL row groups, not the first group's. Positions
// written 16, 15, 17 in three groups: the smallest is in the second group
// and the largest in the third, so a range kept from any one group leaves a
// dead row outside it, read as alive.
func TestStateView_deadRangeSpansEveryRowGroup(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshotRows(t, root, stamp, 20)
	writeDeltaPair(t, base, 0, nil, nil)
	writePosdelRows(t, base, "16", "15", "17")
	tables := onePairTables(base)
	sqlText := generatePinnedMarked(t, root, tables, true)
	if r := tables[0].DeltaDeadRange; r == nil || r.Lo != 15 || r.Hi != 17 {
		t.Fatalf("DeltaDeadRange = %+v, want 15..17", r)
	}
	got := stateRows(t, sqlText)
	before := stateRows(t, generatePinnedMarked(t, root, onePairTables(base), false))
	if !reflect.DeepEqual(got, before) || len(got) != 17 {
		t.Fatalf("rows:\n with the range %v\n without it    %v", got, before)
	}
}

// TestStateView_rowGroupWithoutBoundsKeepsTheJoin: one group holds a real
// position, another only a NULL, which records no min or max. Nothing bounds
// that group, so no range is read from the others: the view keeps the plain
// join and answers (a range from the first group alone would also make the
// in-statement check refuse the statement over the unbounded group).
func TestStateView_rowGroupWithoutBoundsKeepsTheJoin(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshotRows(t, root, stamp, 20)
	writeDeltaPair(t, base, 0, nil, nil)
	writePosdelRows(t, base, "19", "")
	tables := onePairTables(base)
	sqlText := generatePinnedMarked(t, root, tables, true)
	if tables[0].DeltaDeadRange != nil || tables[0].DeltaNoDead {
		t.Fatalf("marked: range %+v, no-dead %v", tables[0].DeltaDeadRange, tables[0].DeltaNoDead)
	}
	if strings.Contains(sqlText, rangeMark) {
		t.Fatalf("the join is confined\n%s", sqlText)
	}
	if got := stateRows(t, sqlText); len(got) != 19 {
		t.Fatalf("%d rows, want 19: %v", len(got), got)
	}
}

// TestStateView_nonIntegerPositionsGiveNoRange: a posdel whose positions are
// not INT64 (written by hand as DOUBLE) has statistics of eight bytes too,
// which read as an integer would be a nonsense range. No range is read from
// it; the plain join is what refuses such a file, as before.
func TestStateView_nonIntegerPositionsGiveNoRange(t *testing.T) {
	const stamp = "2026-04-30T03-00-00Z"
	root := t.TempDir()
	base := writeSnapshotRows(t, root, stamp, 20)
	writeDeltaPair(t, base, 0, nil, nil)
	posdel, _ := baseline.TableDeltaPaths(base, 0)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT 19.0::DOUBLE AS pos) TO '%s' (FORMAT parquet)`, posdel)); err != nil {
		t.Fatal(err)
	}
	tables := onePairTables(base)
	if err := MarkTableDeltas(context.Background(), tables); err != nil {
		t.Fatal(err)
	}
	MarkTableDeltaDeadRows(tables)
	if tables[0].DeltaDeadRange != nil || tables[0].DeltaNoDead {
		t.Fatalf("marked: range %+v, no-dead %v", tables[0].DeltaDeadRange, tables[0].DeltaNoDead)
	}
}
