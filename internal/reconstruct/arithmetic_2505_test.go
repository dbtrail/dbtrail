package reconstruct

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2505: the numbers that must close, worked by hand. Every case is one
// table's refresh; "problems" is what the error line would say.
func TestRefreshArithmetic_problems(t *testing.T) {
	cases := []struct {
		name string
		a    refreshArithmetic
		// wantAfter is rows after; wantProblems is how many things do not close.
		wantAfter    int64
		wantProblems int
	}{
		{"pair at seq 0: an update, a delete and an insert over 3 rows",
			refreshArithmetic{Pair: true, Seq: 0, RowsBefore: 3, Inserts: 1, Deletes: 1, Keys: 3, Upserts: 2, Dead: 2}, 3, 0},
		{"pair at seq 0: a key deleted and inserted again in one window",
			// Events: 1 delete, 1 insert; the pair holds the key once, alive;
			// it was alive before (1 - 1 + 1), and the base holds it (dead 1).
			refreshArithmetic{Pair: true, Seq: 0, RowsBefore: 3, Inserts: 1, Deletes: 1, Keys: 1, Upserts: 1, Dead: 1}, 3, 0},
		{"pair at seq 0: a key inserted and deleted in one window",
			// A tombstone for a key that never was: 0 alive after, net 0.
			refreshArithmetic{Pair: true, Seq: 0, RowsBefore: 3, Inserts: 1, Deletes: 1, Keys: 1, Upserts: 0, Dead: 0}, 3, 0},
		{"pair at seq 0: only inserts",
			refreshArithmetic{Pair: true, Seq: 0, RowsBefore: 3, Inserts: 4, Keys: 4, Upserts: 4, Dead: 0}, 7, 0},
		{"pair at seq 0: a key the window inserted sits in the base (the wrong base)",
			refreshArithmetic{Pair: true, Seq: 0, RowsBefore: 3, Inserts: 1, Keys: 1, Upserts: 1, Dead: 1}, 4, 1},
		{"pair at seq 0: a key the window deleted is not in the base (the wrong base)",
			refreshArithmetic{Pair: true, Seq: 0, RowsBefore: 3, Deletes: 1, Keys: 1, Upserts: 0, Dead: 0}, 2, 1},
		{"pair at seq 0: a dropped key (the events say two inserts, the pair holds one)",
			refreshArithmetic{Pair: true, Seq: 0, RowsBefore: 3, Inserts: 2, Keys: 1, Upserts: 1, Dead: 0}, 5, 2},
		{"pair later in the chain: a touched key dead in the base is found again, no equality",
			refreshArithmetic{Pair: true, Seq: 2, RowsBefore: 5, Inserts: 1, Deletes: 0, Keys: 1, Upserts: 1, Dead: 1}, 6, 0},
		{"pair later in the chain: more base rows than keys",
			refreshArithmetic{Pair: true, Seq: 2, RowsBefore: 5, Inserts: 1, Keys: 1, Upserts: 1, Dead: 2}, 6, 1},
		{"pair later in the chain: more deletes than the keys could have been alive",
			refreshArithmetic{Pair: true, Seq: 2, RowsBefore: 5, Deletes: 3, Keys: 1, Upserts: 0, Dead: 1}, 2, 1},
		{"pair over an unknown count: checked without a rows after",
			refreshArithmetic{Pair: true, Seq: 2, RowsBefore: -1, Inserts: 1, Keys: 1, Upserts: 1, Dead: 0}, -1, 0},
		{"pair: rows after fewer than the keys alive",
			refreshArithmetic{Pair: true, Seq: 1, RowsBefore: 0, Inserts: 1, Deletes: 2, Keys: 3, Upserts: 1, Dead: 2}, -1, 1},
		{"rewrite over a chain whose count agrees",
			refreshArithmetic{Rewrite: true, RowsBefore: 3, Inserts: 1, Deletes: 1, BaselineRows: 1, UpdatesApplied: 1, DeletesSkipped: 1, InsertsEmitted: 1, RowsWritten: 3}, 3, 0},
		{"rewrite: the key deleted and inserted again is one replaced row",
			refreshArithmetic{Rewrite: true, RowsBefore: 3, Inserts: 1, Deletes: 1, BaselineRows: 2, UpdatesApplied: 1, DeletesSkipped: 0, InsertsEmitted: 0, RowsWritten: 3}, 3, 0},
		{"rewrite without a count to meet",
			refreshArithmetic{Rewrite: true, RowsBefore: -1, Inserts: 2, BaselineRows: 3, InsertsEmitted: 2, RowsWritten: 5}, 5, 0},
		{"rewrite: the writer's tally disagrees with the merge",
			refreshArithmetic{Rewrite: true, RowsBefore: -1, Inserts: 2, BaselineRows: 3, InsertsEmitted: 2, RowsWritten: 4}, 4, 1},
		{"rewrite: the merge's net disagrees with the events",
			refreshArithmetic{Rewrite: true, RowsBefore: -1, Inserts: 2, BaselineRows: 3, InsertsEmitted: 1, RowsWritten: 4}, 4, 1},
		{"rewrite: the chain's count disagrees with the state read",
			refreshArithmetic{Rewrite: true, RowsBefore: 4, Inserts: 2, BaselineRows: 3, InsertsEmitted: 2, RowsWritten: 5}, 5, 1},
	}
	for _, c := range cases {
		got := c.a.problems()
		if c.wantProblems > 0 {
			// Across a known capture gap the numbers are logged, not checked.
			unchecked := c.a
			unchecked.Unchecked = "the window crosses a known capture gap"
			rep := &TableReport{}
			unchecked.report(rep)
			if len(rep.ArithmeticProblems) != 0 {
				t.Errorf("%s: an unchecked window reported %q", c.name, rep.ArithmeticProblems)
			}
		}
		if len(got) != c.wantProblems {
			t.Errorf("%s: %d problem(s) %q, want %d", c.name, len(got), got, c.wantProblems)
		}
		if after := c.a.rowsAfter(); after != c.wantAfter {
			t.Errorf("%s: rows after = %d, want %d", c.name, after, c.wantAfter)
		}
	}
}

func mismatchesNow(t *testing.T) float64 {
	t.Helper()
	n, err := observe.RefreshArithmeticMismatches()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// captureLogs routes slog to a buffer at debug level for the test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// #2505 through the real pair writer: a window of inserts, updates and
// deletes, then one with a key deleted and inserted again, close and carry
// the count from pair to pair; a planted miscount logs and counts, and the
// table is published all the same.
func TestRefreshArithmetic_deltaWindows(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	baseRows, err := baseline.TableFileRows(src)
	if err != nil || baseRows != 3 {
		t.Fatalf("base rows = %d, %v; want 3", baseRows, err)
	}
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	type window struct {
		name string
		fold *foldResult
		// wantBefore / wantAfter are the report's; wantFooter is the pair's.
		wantBefore, wantAfter int64
	}
	// Window 2: key 2 (deleted in window 1) is inserted again, and key 1 is
	// deleted and inserted again INSIDE the window: two inserts and two
	// deletes over three keys, and the row count moves by the one key that
	// was dead before (2).
	reinsert := foldOf(changeMap(ins(1, "one-back"), ins(2, "two-back"), upd(3, "three")))
	reinsert.Deletes, reinsert.Total = 1, reinsert.Total+1
	windows := []window{
		{"inserts, updates and deletes", foldOf(changeMap(upd(1, "one-v2"), del(2), ins(9, "nine"))), 3, 3},
		{"a key deleted and inserted again in one window", reinsert, 3, 4},
		{"only deletes", foldOf(changeMap(del(9), del(3))), 4, 2},
	}
	prevBase, prevTime := src, t0
	logs := captureLogs(t)
	before := mismatchesNow(t)
	for i, w := range windows {
		at := t0.Add(time.Duration(i+1) * 5 * time.Minute)
		cut := &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * (i + 1))}
		newBase, rep, err := deltaWindow(t, root, prevBase, prevTime, w.fold.Changes, at, cut, func(p *tableDeltaPublish) { p.fold = w.fold })
		if err != nil {
			t.Fatalf("%s: %v", w.name, err)
		}
		if rep.RowsBefore != w.wantBefore || rep.RowsAfter != w.wantAfter || len(rep.ArithmeticProblems) != 0 {
			t.Errorf("%s: rows before/after = %d/%d problems %q, want %d/%d and none", w.name, rep.RowsBefore, rep.RowsAfter, rep.ArithmeticProblems, w.wantBefore, w.wantAfter)
		}
		if rep.WindowInserts != w.fold.Inserts || rep.WindowDeletes != w.fold.Deletes {
			t.Errorf("%s: report events %d/%d, want the fold's %d/%d", w.name, rep.WindowInserts, rep.WindowDeletes, w.fold.Inserts, w.fold.Deletes)
		}
		_, upserts := baseline.TableDeltaPaths(newBase, rep.DeltaSeq)
		um, err := baseline.ReadParquetMetadata(upserts)
		if err != nil {
			t.Fatal(err)
		}
		if um.DeltaRows != w.wantAfter {
			t.Errorf("%s: the pair's footer carries %d rows, want %d", w.name, um.DeltaRows, w.wantAfter)
		}
		// The state the chain resolves to holds that many rows: the count is
		// not only consistent with itself.
		if got := int64(len(deltaState(t, newBase))); got != w.wantAfter {
			t.Errorf("%s: the resolved state holds %d rows, the count says %d", w.name, got, w.wantAfter)
		}
		prevBase, prevTime = newBase, at
	}
	if moved := mismatchesNow(t) - before; moved != 0 {
		t.Errorf("bintrail_refresh_arithmetic_mismatch_total moved by %v on windows that close", moved)
	}
	if n := strings.Count(logs.String(), "refresh arithmetic: rows after = rows before + inserts - deletes"); n != len(windows) {
		t.Errorf("%d debug lines for %d windows:\n%s", n, len(windows), logs.String())
	}
	if strings.Contains(logs.String(), "does not close") {
		t.Errorf("an error line on windows that close:\n%s", logs.String())
	}

	// The planted miscount, at the start of a chain, where the base is the
	// state before the window and the lookup's count must meet the events':
	// the fold says the window inserted nothing, the pair holds an insert
	// of a key the base does not have, so by the events that key was alive
	// before and the lookup should have found it.
	logs.Reset()
	planted := foldOf(changeMap(ins(7, "seven")))
	planted.Inserts = 0
	at := t0.Add(time.Hour)
	newBase, rep, err := deltaWindow(t, t.TempDir(), src, t0, planted.Changes, at, &query.BinlogPos{File: "binlog.000009", Pos: 9000}, func(p *tableDeltaPublish) { p.fold = planted })
	if err != nil {
		t.Fatalf("planted miscount refused the table: %v", err)
	}
	if len(rep.ArithmeticProblems) == 0 || !rep.TableDelta || !rep.DeltaPairWritten {
		t.Fatalf("planted miscount: problems %q, delta %v written %v; want a problem on a published pair", rep.ArithmeticProblems, rep.TableDelta, rep.DeltaPairWritten)
	}
	if _, err := os.Stat(newBase); err != nil {
		t.Errorf("the table was not published: %v", err)
	}
	if moved := mismatchesNow(t) - before; moved != 1 {
		t.Errorf("bintrail_refresh_arithmetic_mismatch_total moved by %v, want 1", moved)
	}
	if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "does not close") ||
		!strings.Contains(logs.String(), "table=orders") || !strings.Contains(logs.String(), "rows_before=3") ||
		!strings.Contains(logs.String(), "snapshot=") || !strings.Contains(logs.String(), "the lookup found 0 of the touched keys in the base, but 1 were alive before the window") {
		t.Errorf("the error line is missing or incomplete:\n%s", logs.String())
	}
}

// #2505: a chain written before the count existed carries none, so the
// window is checked for what it can be and reports no rows, with one
// exception: a chain that is only the empty sequence-0 pair applies nothing,
// so the base's count still stands. The previous pair's footer is read by
// deltaWindow; the count is withheld from what it read, as an older pair
// would have none.
func TestRefreshArithmetic_chainWithoutACount(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	noCount := func(p *tableDeltaPublish) { p.prev.Meta.DeltaRows = -1 }

	t.Run("a pair with changes and no count: unknown, not a mismatch", func(t *testing.T) {
		root := t.TempDir()
		first, rep, err := deltaWindow(t, root, src, t0, changeMap(ins(9, "nine")), t0.Add(5*time.Minute), &query.BinlogPos{File: "b.1", Pos: 10}, nil)
		if err != nil || len(rep.ArithmeticProblems) != 0 || rep.RowsAfter != 4 {
			t.Fatalf("first window: %v, problems %q, rows after %d", err, rep.ArithmeticProblems, rep.RowsAfter)
		}
		logs := captureLogs(t)
		before := mismatchesNow(t)
		_, rep, err = deltaWindow(t, root, first, t0.Add(5*time.Minute), changeMap(upd(9, "nine-v2"), del(1)), t0.Add(10*time.Minute), &query.BinlogPos{File: "b.1", Pos: 20}, noCount)
		if err != nil {
			t.Fatal(err)
		}
		if rep.RowsBefore != -1 || rep.RowsAfter != -1 || len(rep.ArithmeticProblems) != 0 {
			t.Errorf("rows before/after = %d/%d problems %q; want unknown and none", rep.RowsBefore, rep.RowsAfter, rep.ArithmeticProblems)
		}
		if moved := mismatchesNow(t) - before; moved != 0 || strings.Contains(logs.String(), "does not close") {
			t.Errorf("a chain without a count was counted as a mismatch (%v):\n%s", moved, logs.String())
		}
		if !strings.Contains(logs.String(), "rows_before=-1") {
			t.Errorf("the debug line does not say the count is unknown:\n%s", logs.String())
		}
	})

	t.Run("only the empty sequence-0 pair: the base's count stands", func(t *testing.T) {
		root := t.TempDir()
		first, rep, err := deltaWindow(t, root, src, t0, changeMap(), t0.Add(5*time.Minute), &query.BinlogPos{File: "b.1", Pos: 10}, nil)
		if err != nil || !rep.DeltaPairWritten || rep.DeltaSeq != 0 {
			t.Fatalf("empty first window: %v, written %v seq %d", err, rep.DeltaPairWritten, rep.DeltaSeq)
		}
		_, rep, err = deltaWindow(t, root, first, t0.Add(5*time.Minute), changeMap(ins(9, "nine"), del(1)), t0.Add(10*time.Minute), &query.BinlogPos{File: "b.1", Pos: 20}, noCount)
		if err != nil {
			t.Fatal(err)
		}
		if rep.RowsBefore != 3 || rep.RowsAfter != 3 || len(rep.ArithmeticProblems) != 0 {
			t.Errorf("rows before/after = %d/%d problems %q; want 3/3 and none", rep.RowsBefore, rep.RowsAfter, rep.ArithmeticProblems)
		}
	})
}

// #2505 (review): a table the window did not touch is carried forward with
// nothing to check, and its report must say so: the counts unknown (-1),
// not zero, and the check not run.
func TestRefreshArithmetic_carriedForwardTableIsNotCounted(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	first, rep, err := deltaWindow(t, root, src, t0, changeMap(ins(9, "nine")), t0.Add(5*time.Minute), &query.BinlogPos{File: "b.1", Pos: 10}, nil)
	if err != nil || !rep.ArithmeticChecked || rep.RowsAfter != 4 {
		t.Fatalf("first window: %v checked=%v rows after %d", err, rep.ArithmeticChecked, rep.RowsAfter)
	}
	_, rep, err = deltaWindow(t, root, first, t0.Add(5*time.Minute), changeMap(), t0.Add(10*time.Minute), &query.BinlogPos{File: "b.1", Pos: 20}, nil)
	if err != nil || rep.DeltaPairWritten {
		t.Fatalf("empty window: %v, pair written %v", err, rep.DeltaPairWritten)
	}
	if rep.ArithmeticChecked || rep.RowsBefore != -1 || rep.RowsAfter != -1 || rep.WindowInserts != 0 || len(rep.ArithmeticProblems) != 0 {
		t.Errorf("carried-forward report: checked=%v rows %d/%d inserts %d problems %q; want unchecked, -1/-1", rep.ArithmeticChecked, rep.RowsBefore, rep.RowsAfter, rep.WindowInserts, rep.ArithmeticProblems)
	}
	if r := newTableReport("s", "t"); r.RowsBefore != -1 || r.RowsAfter != -1 || r.ArithmeticChecked {
		t.Errorf("a fresh report starts at %+v, want unknown counts", r)
	}
}

// #2505 (review): the rewrite that compacts a chain reads "rows before" the
// way the pair that extends it does: a chain that is only the empty
// sequence-0 pair without a count still has the base's count, so the state
// the merge read is checked against it.
func TestRefreshArithmetic_rewriteReadsTheChainsCountLikeThePair(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	first, rep, err := deltaWindow(t, root, src, t0, changeMap(), t0.Add(5*time.Minute), &query.BinlogPos{File: "b.1", Pos: 10}, nil)
	if err != nil || !rep.DeltaPairWritten || rep.DeltaSeq != 0 {
		t.Fatalf("empty first window: %v, written %v seq %d", err, rep.DeltaPairWritten, rep.DeltaSeq)
	}
	// A day later the chain is rewritten; the previous pair carries no count.
	at := t0.Add(25 * time.Hour)
	logs := captureLogs(t)
	_, rep, err = deltaWindow(t, root, first, t0.Add(5*time.Minute), changeMap(ins(9, "nine"), del(1)), at, &query.BinlogPos{File: "b.1", Pos: 20},
		func(p *tableDeltaPublish) { p.prev.Meta.DeltaRows = -1 })
	if err != nil {
		t.Fatal(err)
	}
	if rep.TableDelta || rep.DeltaCompacted == "" {
		t.Fatalf("want a rewrite, got delta=%v compacted=%q", rep.TableDelta, rep.DeltaCompacted)
	}
	if !rep.ArithmeticChecked || rep.RowsBefore != 3 || rep.RowsAfter != 3 || len(rep.ArithmeticProblems) != 0 {
		t.Errorf("rewrite report: checked=%v rows %d/%d problems %q; want 3/3 from the base's count and none", rep.ArithmeticChecked, rep.RowsBefore, rep.RowsAfter, rep.ArithmeticProblems)
	}
	// rows_before is what the merge read; chain_rows is what the chain
	// carried in, the base's count through the exception, and the two met.
	if !strings.Contains(logs.String(), "published_as=rewrite") || !strings.Contains(logs.String(), "rows_before=3") ||
		!strings.Contains(logs.String(), "chain_rows=3") {
		t.Errorf("the rewrite's debug line:\n%s", logs.String())
	}
}
