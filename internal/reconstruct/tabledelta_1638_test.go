package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The tests here are DIFFERENTIAL (#1638): every window is folded twice from
// the same previous state, once by the full rewrite that exists today and once
// as a table delta, and the two states must hold the same rows. The rewrite is
// the reference because it is what every restore has been built on; a delta
// that disagrees with it is wrong whatever else it gets right.

func zooRow(id int, name string) map[string]any {
	return map[string]any{
		"id": float64(id), "qty": float64(id * 10), "name": name, "price": "1.50",
		"created": "2026-03-03 03:03:03", "day": "2026-03-03", "payload": nil, "ratio": float64(0.5),
	}
}

func upd(id int, name string) *query.ResultRow {
	return &query.ResultRow{PKValues: pkStrForInt(id), EventType: event.EventUpdate, RowAfter: zooRow(id, name)}
}

func ins(id int, name string) *query.ResultRow {
	return &query.ResultRow{PKValues: pkStrForInt(id), EventType: event.EventInsert, RowAfter: zooRow(id, name)}
}

func del(id int) *query.ResultRow {
	return &query.ResultRow{PKValues: pkStrForInt(id), EventType: event.EventDelete}
}

func changeMap(evs ...*query.ResultRow) map[string]*query.ResultRow {
	m := map[string]*query.ResultRow{}
	for _, e := range evs {
		m[e.PKValues] = e
	}
	return m
}

// noCompaction lifts the size threshold. The fixture's base is three rows, so
// any upserts file is "more than a quarter of the table" and every second
// window would compact; the chain tests want the chain.
func noCompaction(t *testing.T) {
	t.Helper()
	prev := tableDeltaMaxFraction
	tableDeltaMaxFraction = 1e9
	t.Cleanup(func() { tableDeltaMaxFraction = prev })
}

// noSizeFloor drops the floor under which the size rule does not apply, so a
// fixture of a few rows can reach the rule.
func noSizeFloor(t *testing.T) {
	t.Helper()
	prev := tableDeltaMinCompactBytes
	tableDeltaMinCompactBytes = 0
	t.Cleanup(func() { tableDeltaMinCompactBytes = prev })
}

// deltaWindow publishes one window as a table delta, doing for the unit under
// test what ReconstructTable does around it: read the base's footer, read the
// delta beside it, resume from the delta's anchor.
func deltaWindow(t *testing.T, root, prevBase string, prevDirTime time.Time, changes map[string]*query.ResultRow,
	at time.Time, cut *query.BinlogPos, mutate func(*tableDeltaPublish)) (newBase string, rep *TableReport, err error) {
	t.Helper()
	bmeta, merr := baseline.ReadParquetMetadata(prevBase)
	if merr != nil {
		t.Fatalf("read base footer: %v", merr)
	}
	prev, merr := readTableDelta(context.Background(), prevBase, bmeta)
	if merr != nil {
		t.Fatalf("readTableDelta: %v", merr)
	}
	chainStart, anchorMeta := prevDirTime, bmeta
	if prev != nil {
		chainStart = prev.Meta.DeltaChainStart
		anchorMeta.BinlogFile, anchorMeta.BinlogPos = prev.Meta.BinlogFile, prev.Meta.BinlogPos
	}
	snapDir := filepath.Join(root, SnapshotDirName(at))
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	p := tableDeltaPublish{
		cfg:    FullTableConfig{At: at, OutputFormat: OutputFormatParquet, TableDeltas: true, snapshotDir: snapDir, cut: cut},
		schema: "mydb", table: "orders",
		basePath: prevBase, chainStart: chainStart, baseMeta: bmeta, anchorMeta: anchorMeta,
		prev: prev, fold: &foldResult{Changes: changes}, pkCols: pkColsIntID(),
	}
	if mutate != nil {
		mutate(&p)
	}
	rep = &TableReport{Schema: "mydb", Table: "orders"}
	err = publishWithTableDelta(context.Background(), p, rep)
	return filepath.Join(snapDir, "mydb", "orders.parquet"), rep, err
}

// deltaState reads a table's state the way `bintrail views` and a compaction
// do: through baseline.TableDeltaStateSQL.
func deltaState(t *testing.T, base string) []map[string]any {
	t.Helper()
	bmeta, err := baseline.ReadParquetMetadata(base)
	if err != nil {
		t.Fatal(err)
	}
	d, err := readTableDelta(context.Background(), base, bmeta)
	if err != nil || d == nil {
		t.Fatalf("no usable table delta beside %s (err=%v)", base, err)
	}
	tmp, cleanup, err := materializeBaseWithDelta(context.Background(), base, d, duckdbutil.Tuning{})
	if err != nil {
		t.Fatalf("materializeBaseWithDelta: %v", err)
	}
	defer cleanup()
	return readSnapshotRows(t, tmp)
}

func byID(rows []map[string]any) []map[string]any {
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]["id"]) < fmt.Sprint(rows[j]["id"]) })
	return rows
}

func readPositions(t *testing.T, path string) []int64 {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	rows, err := ddb.Query(fmt.Sprintf("SELECT pos FROM parquet_scan('%s')", path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var p int64
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func ids(rows []map[string]any) []string {
	out := []string{}
	for _, r := range byID(rows) {
		out = append(out, fmt.Sprint(r["id"]))
	}
	return out
}

// TestTableDelta_chainMatchesFullRewrite walks one chain through every way a
// row can move between the base, the dead positions and the upserts.
func TestTableDelta_chainMatchesFullRewrite(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows() // ids 1, 2, 3 at base rows 0, 1, 2
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	windows := []struct {
		name     string
		changes  map[string]*query.ResultRow
		wantDead []int64
		wantUps  []string
	}{
		{"start: update, delete, insert",
			changeMap(upd(1, "one-v2"), del(2), ins(9, "nine")), []int64{0, 1}, []string{"1", "9"}},
		{"extend: update again, delete an inserted row, re-insert a deleted one, touch the NULL row",
			changeMap(upd(1, "one-v3"), del(9), ins(2, "two-back"), upd(3, "three")), []int64{0, 1, 2}, []string{"1", "2", "3"}},
		{"extend: nothing changed", changeMap(), []int64{0, 1, 2}, []string{"1", "2", "3"}},
		{"extend: delete a row that lives in the upserts", changeMap(del(1)), []int64{0, 1, 2}, []string{"2", "3"}},
	}

	refPrev, deltaPrev, prevTime := src, src, t0
	for i, w := range windows {
		at := t0.Add(time.Duration(i+1) * 5 * time.Minute)
		cut := &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * (i + 1))}

		_, refOut, _ := emitSnapshot(t, refPrev, cloneChanges(w.changes), cut, at)
		newBase, rep, err := deltaWindow(t, root, deltaPrev, prevTime, cloneChanges(w.changes), at, cut, nil)
		if err != nil {
			t.Fatalf("%s: %v", w.name, err)
		}
		if !rep.TableDelta || rep.DeltaCompacted != "" {
			t.Fatalf("%s: want a delta, got TableDelta=%v compacted=%q", w.name, rep.TableDelta, rep.DeltaCompacted)
		}

		want, got := byID(readSnapshotRows(t, refOut)), byID(deltaState(t, newBase))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: state differs from the full rewrite\n got: %v\nwant: %v", w.name, got, want)
		}

		posdel, upserts := baseline.TableDeltaPaths(newBase)
		if dead := readPositions(t, posdel); !reflect.DeepEqual(dead, w.wantDead) {
			t.Errorf("%s: dead positions = %v, want %v", w.name, dead, w.wantDead)
		}
		if ups := ids(readSnapshotRows(t, upserts)); !reflect.DeepEqual(ups, w.wantUps) {
			t.Errorf("%s: upsert ids = %v, want %v", w.name, ups, w.wantUps)
		}
		if rep.DeltaDeadRows != int64(len(w.wantDead)) || rep.DeltaUpsertRows != int64(len(w.wantUps)) {
			t.Errorf("%s: report says %d dead / %d upserts", w.name, rep.DeltaDeadRows, rep.DeltaUpsertRows)
		}

		// The base is the SAME file, not a copy of it and not a rewrite.
		a, _ := os.Stat(src)
		b, _ := os.Stat(newBase)
		if !os.SameFile(a, b) {
			t.Errorf("%s: the base was not carried forward by link", w.name)
		}

		// Footer: the delta resumes from this run's cut, and the chain still
		// starts where the first window found the base.
		um, err := baseline.ReadParquetMetadata(upserts)
		if err != nil {
			t.Fatal(err)
		}
		if um.BinlogFile != cut.File || um.BinlogPos != int64(cut.Pos) {
			t.Errorf("%s: delta anchor = %s:%d, want the run's cut %s:%d", w.name, um.BinlogFile, um.BinlogPos, cut.File, cut.Pos)
		}
		if !um.DeltaChainStart.Equal(t0) {
			t.Errorf("%s: chain start = %s, want %s", w.name, um.DeltaChainStart, t0)
		}
		if !um.SnapshotTimestamp.Equal(at) {
			t.Errorf("%s: snapshot time = %s, want %s", w.name, um.SnapshotTimestamp, at)
		}
		refPrev, deltaPrev, prevTime = refOut, newBase, at
	}
}

// TestTableDelta_compactionMatchesFullRewrite: past the size threshold the
// table is rewritten from base+delta, holds the same rows a rewrite chain
// holds, and leaves with an EMPTY pair that starts the next chain here.
func TestTableDelta_compactionMatchesFullRewrite(t *testing.T) {
	noSizeFloor(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	w1 := changeMap(upd(1, "one-v2"), del(2), ins(9, "nine"))
	w2 := changeMap(upd(3, "three"), ins(10, "ten"), del(9))

	at1, cut1 := t0.Add(5*time.Minute), &query.BinlogPos{File: "binlog.000009", Pos: 1000}
	_, ref1, _ := emitSnapshot(t, src, cloneChanges(w1), cut1, at1)
	base1, rep1, err := deltaWindow(t, root, src, t0, cloneChanges(w1), at1, cut1, nil)
	if err != nil || !rep1.TableDelta {
		t.Fatalf("window 1: err=%v delta=%v", err, rep1.TableDelta)
	}

	// The three-row base is smaller than four times any upserts file, so the
	// default threshold compacts on the very next window.
	at2, cut2 := t0.Add(10*time.Minute), &query.BinlogPos{File: "binlog.000009", Pos: 2000}
	_, ref2, _ := emitSnapshot(t, ref1, cloneChanges(w2), cut2, at2)
	base2, rep2, err := deltaWindow(t, root, base1, at1, cloneChanges(w2), at2, cut2, nil)
	if err != nil {
		t.Fatalf("window 2: %v", err)
	}
	if rep2.TableDelta || !strings.Contains(rep2.DeltaCompacted, "passed 25% of the table") {
		t.Fatalf("window 2: want a compaction by size, got TableDelta=%v compacted=%q", rep2.TableDelta, rep2.DeltaCompacted)
	}
	want := byID(readSnapshotRows(t, ref2))
	// The rewritten base ALONE is the state...
	if got := byID(readSnapshotRows(t, base2)); !reflect.DeepEqual(got, want) {
		t.Fatalf("compacted base differs from the full rewrite\n got: %v\nwant: %v", got, want)
	}
	// ...and so is base+delta, because the pair it left with is empty.
	if got := byID(deltaState(t, base2)); !reflect.DeepEqual(got, want) {
		t.Fatalf("compacted base+delta differs from the full rewrite\n got: %v\nwant: %v", got, want)
	}
	posdel, upserts := baseline.TableDeltaPaths(base2)
	if n := len(readPositions(t, posdel)); n != 0 {
		t.Errorf("a compaction left %d dead positions", n)
	}
	um, _ := baseline.ReadParquetMetadata(upserts)
	if !um.DeltaChainStart.Equal(at2) {
		t.Errorf("the chain after a compaction starts at %s, want this snapshot %s", um.DeltaChainStart, at2)
	}
	a, _ := os.Stat(src)
	b, _ := os.Stat(base2)
	if os.SameFile(a, b) {
		t.Error("a compaction published the old base")
	}
}

func TestTableDeltaCompactReason(t *testing.T) {
	noSizeFloor(t)
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	chain := func(age time.Duration, upserts int64) *tableDelta {
		return &tableDelta{Meta: baseline.DumpMetadata{DeltaChainStart: at.Add(-age)}, PairSize: upserts}
	}
	cases := []struct {
		name    string
		prev    *tableDelta
		base    string
		spilled bool
		gap     *CaptureGap
		want    string
		// zero values are the ordinary table: it has an anchor, and no column
		// named file_row_number.
		noAnchor, reserved bool
	}{
		{"start a chain", nil, "/b/t.parquet", false, nil, "", false, false},
		{"extend", chain(time.Hour, 100), "/b/t.parquet", false, nil, "", false, false},
		{"exactly a quarter still extends", chain(time.Hour, 250), "/b/t.parquet", false, nil, "", false, false},
		{"past a quarter", chain(time.Hour, 251), "/b/t.parquet", false, nil, "passed 25%", false, false},
		{"exactly a day still extends", chain(24*time.Hour, 1), "/b/t.parquet", false, nil, "", false, false},
		{"past a day", chain(24*time.Hour+time.Minute, 1), "/b/t.parquet", false, nil, "old", false, false},
		{"s3 source", nil, "s3://b/t.parquet", false, nil, "S3", false, false},
		{"spilled fold", nil, "/b/t.parquet", true, nil, "did not fit in memory", false, false},
		{"capture gap", chain(time.Hour, 1), "/b/t.parquet", false, &CaptureGap{}, "capture gap", false, false},
		{"nothing to resume from", nil, "/b/t.parquet", false, nil, "no binlog position", true, false},
		{"a column named file_row_number", nil, "/b/t.parquet", false, nil, "file_row_number", false, true},
	}
	for _, c := range cases {
		got := tableDeltaCompactReason(c.prev, c.base, 1000, c.spilled, c.gap, at, !c.noAnchor, c.reserved)
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: reason = %q, want it to contain %q", c.name, got, c.want)
		}
	}
}

// TestTableDeltaCompactReason_leavesSmallTablesAlone: with the shipped floor, an
// upserts file far larger than a tiny base is still not a reason to rewrite.
func TestTableDeltaCompactReason_leavesSmallTablesAlone(t *testing.T) {
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	prev := &tableDelta{Meta: baseline.DumpMetadata{DeltaChainStart: at.Add(-time.Hour)}, PairSize: 1<<20 - 1}
	if got := tableDeltaCompactReason(prev, "/b/t.parquet", 700, false, nil, at, true, false); got != "" {
		t.Errorf("under the floor: reason = %q, want none", got)
	}
	prev.PairSize = 1 << 20
	if got := tableDeltaCompactReason(prev, "/b/t.parquet", 700, false, nil, at, true, false); !strings.Contains(got, "passed 25%") {
		t.Errorf("at the floor: reason = %q, want the size rule", got)
	}
}

// TestReadTableDelta_setsAsideADeltaOfAnotherBase: row numbers mean nothing
// against any other file, so a delta whose base was replaced is not applied.
func TestReadTableDelta_setsAsideADeltaOfAnotherBase(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	base, _, err := deltaWindow(t, t.TempDir(), src, t0, changeMap(del(2)), t0.Add(5*time.Minute),
		&query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	bmeta, _ := baseline.ReadParquetMetadata(base)
	if d, err := readTableDelta(context.Background(), base, bmeta); err != nil || d == nil {
		t.Fatalf("control: the delta must be usable before the base changes (d=%v err=%v)", d, err)
	}

	// Another base under the same name: one more row, so another size.
	other := writeZooBaseline(t, append(rows, []string{"4", "1", "x", "1.00", "2026-01-02 03:04:05", "2026-01-02", "0x00", "1"}),
		append(nulls, make([]bool, 8)))
	if err := os.Remove(base); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(other)
	if err := os.WriteFile(base, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if d, err := readTableDelta(context.Background(), base, bmeta); err != nil || d != nil {
		t.Fatalf("a delta computed against another base was accepted (d=%v err=%v)", d, err)
	}

	// Half a pair is a damaged snapshot and is refused, not set aside.
	posdel, _ := baseline.TableDeltaPaths(base)
	if err := os.Remove(posdel); err != nil {
		t.Fatal(err)
	}
	if d, err := readTableDelta(context.Background(), base, bmeta); err != nil || d != nil {
		t.Fatalf("half a pair must be set aside for the fold (d=%v err=%v)", d, err)
	}
	if _, err := baseline.HasTableDelta(context.Background(), base); !errors.Is(err, baseline.ErrHalfTableDelta) {
		t.Fatalf("HasTableDelta on half a pair: err = %v, want ErrHalfTableDelta", err)
	}
}

// TestFindBaseline_returnsTheChainStart pins the rule that keeps every reader
// that ignores deltas correct: the time FindBaseline hands back for a base with
// a delta beside it is where the chain started, not the directory it sits in.
func TestFindBaseline_returnsTheChainStart(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	base, prevTime := src, t0
	// Three hours of windows: far enough that a fetch floored an hour before
	// the DIRECTORY time would start after the first window's events.
	for i := 1; i <= 3; i++ {
		at := t0.Add(time.Duration(i) * time.Hour)
		nb, _, err := deltaWindow(t, root, base, prevTime, changeMap(upd(1, fmt.Sprintf("v%d", i))), at,
			&query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * i)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		base, prevTime = nb, at
	}
	path, got, _, err := FindBaseline(context.Background(), root, "mydb", "orders", t0.Add(4*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if path != base {
		t.Fatalf("FindBaseline picked %s, want the newest snapshot %s", path, base)
	}
	if !got.Equal(t0) {
		t.Fatalf("FindBaseline time = %s, want the chain's start %s (the directory is %s)", got, t0, prevTime)
	}
}

// writeStampedBaseline is writeZooBaseline inside a snapshot directory, with
// the footer a real dump writes: an anchor AND the time it was written.
func writeStampedBaseline(t *testing.T, root string, at time.Time) string {
	t.Helper()
	rows, nulls := zooRows()
	path := filepath.Join(root, SnapshotDirName(at), "mydb", "orders.parquet")
	w, err := baseline.NewWriter(path, zooColumns(t), baseline.WriterConfig{
		Compression: "none", RowGroupSize: 10,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL:    zooCreateTableSQL,
			baseline.MetaKeyBinlogFile:        "binlog.000007",
			baseline.MetaKeyBinlogPos:         "4",
			baseline.MetaKeySnapshotTimestamp: at.UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rows {
		if err := w.WriteRow(r, nulls[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestWriteEmptyTableDeltas: a full backup taken with table deltas on leaves
// with the pair, the pair is one a refresh will EXTEND (not set aside), and the
// state it describes is the table file's own rows.
func TestWriteEmptyTableDeltas(t *testing.T) {
	noCompaction(t)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	base := writeStampedBaseline(t, root, t0)
	if err := baseline.WriteEmptyTableDeltas(filepath.Dir(filepath.Dir(base))); err != nil {
		t.Fatalf("WriteEmptyTableDeltas: %v", err)
	}
	bmeta, _ := baseline.ReadParquetMetadata(base)
	d, err := readTableDelta(context.Background(), base, bmeta)
	if err != nil || d == nil {
		t.Fatalf("the pair a full backup wrote is not usable by a refresh (d=%v err=%v)", d, err)
	}
	if !d.Meta.DeltaChainStart.Equal(t0) {
		t.Errorf("chain start = %s, want the backup's own time %s", d.Meta.DeltaChainStart, t0)
	}
	if got, want := ids(deltaState(t, base)), []string{"1", "2", "3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state under the empty pair = %v, want the table's own rows %v", got, want)
	}
	// Idempotent, for a --retry run.
	if err := baseline.WriteEmptyTableDeltas(filepath.Dir(filepath.Dir(base))); err != nil {
		t.Fatalf("second WriteEmptyTableDeltas: %v", err)
	}
	// And a refresh extends it.
	at := t0.Add(5 * time.Minute)
	nb, rep, err := deltaWindow(t, root, base, t0, changeMap(del(2)), at, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
	if err != nil || !rep.TableDelta {
		t.Fatalf("refresh over a full backup's pair: err=%v delta=%v", err, rep.TableDelta)
	}
	if got, want := ids(deltaState(t, nb)), []string{"1", "3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %v, want %v", got, want)
	}
}

// TestFindBaseline_damagedDeltaFallsBackToTheFileOwnTime: the delta is
// optional, the bound is not. With half a pair FindBaseline still answers, from
// the time the table file itself was written, which is never after the chain's
// start. A file with no time of its own has no safe bound and is refused.
func TestFindBaseline_damagedDeltaFallsBackToTheFileOwnTime(t *testing.T) {
	noCompaction(t)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	base := writeStampedBaseline(t, root, t0)
	at := t0.Add(3 * time.Hour)
	nb, _, err := deltaWindow(t, root, base, t0, changeMap(del(2)), at, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	posdel, _ := baseline.TableDeltaPaths(nb)
	if err := os.Remove(posdel); err != nil {
		t.Fatal(err)
	}
	_, got, _, err := FindBaseline(context.Background(), root, "mydb", "orders", at.Add(time.Minute))
	if err != nil {
		t.Fatalf("half a pair blocked the read: %v", err)
	}
	if !got.Equal(t0) {
		t.Fatalf("time = %s, want the table file's own %s (the directory is %s)", got, t0, at)
	}

	// Same damage over a file that records no time: nothing safe to bound from.
	root2 := t.TempDir()
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	nb2, _, err := deltaWindow(t, root2, src, t0, changeMap(del(2)), at, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	posdel2, _ := baseline.TableDeltaPaths(nb2)
	if err := os.Remove(posdel2); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := FindBaseline(context.Background(), root2, "mydb", "orders", at.Add(time.Minute)); !errors.Is(err, baseline.ErrHalfTableDelta) {
		t.Fatalf("err = %v, want a refusal that names the half pair", err)
	}
}
