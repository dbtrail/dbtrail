package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	// The anchor comes from fetchFloor, as in ReconstructTable, so what the
	// writer carries forward (position and stamp) is what production gives it.
	chainStart := prevDirTime
	if prev != nil {
		chainStart = prev.Meta.DeltaChainStart
	}
	_, anchorMeta := fetchFloor(prevDirTime, bmeta, prev)
	snapDir := filepath.Join(root, SnapshotDirName(at))
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	p := tableDeltaPublish{
		cfg:    FullTableConfig{At: at, OutputFormat: OutputFormatParquet, TableDeltas: true, snapshotDir: snapDir, cut: cut},
		schema: "mydb", table: "orders",
		basePath: prevBase, chainStart: chainStart, baseMeta: bmeta, anchorMeta: anchorMeta,
		prev: prev, fold: &foldResult{Changes: changes}, pkCols: pkColsIntID(),
		streamCaptured: true,
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
// TestTableDeltaCompactReason_leavesSmallTablesAlone: with the shipped floor, an
// upserts file far larger than a tiny base is still not a reason to rewrite.
func TestTableDeltaCompactReason_leavesSmallTablesAlone(t *testing.T) {
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	prev := &tableDelta{Meta: baseline.DumpMetadata{DeltaChainStart: at.Add(-time.Hour)}, PairSize: 1<<20 - 1}
	if got := tableDeltaCompactReason(prev, "/b/t.parquet", 700, false, nil, at, true, ""); got != "" {
		t.Errorf("under the floor: reason = %q, want none", got)
	}
	prev.PairSize = 1 << 20
	if got := tableDeltaCompactReason(prev, "/b/t.parquet", 700, false, nil, at, true, ""); !strings.Contains(got, "passed 25%") {
		t.Errorf("at the floor: reason = %q, want the size rule", got)
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
	posdel, _ := baseline.TableDeltaPaths(nb, 0)
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
	posdel2, _ := baseline.TableDeltaPaths(nb2, 0)
	if err := os.Remove(posdel2); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := FindBaseline(context.Background(), root2, "mydb", "orders", at.Add(time.Minute)); !errors.Is(err, baseline.ErrHalfTableDelta) {
		t.Fatalf("err = %v, want a refusal that names the half pair", err)
	}
}
