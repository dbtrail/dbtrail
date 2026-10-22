package verify

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

var joinCols = []metadata.ColumnMeta{
	{Name: "id", DataType: "int"},
	{Name: "note", DataType: "varchar"},
	{Name: "kind", DataType: "enum"},
}

func cellsOf(vals ...[]byte) map[string][]byte {
	m := map[string][]byte{}
	for i, v := range vals {
		m[joinCols[i].Name] = v
	}
	return m
}

func runJoin(t *testing.T, truth, rec []struct {
	key string
	r   rowCells
}) *MismatchExplanation {
	t.Helper()
	ctx := context.Background()
	j, err := newRowJoin(ctx, duckdbutil.Tuning{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	for _, x := range truth {
		if err := j.addTruth(x.key, x.r, joinCols); err != nil {
			t.Fatal(err)
		}
	}
	for _, x := range rec {
		if err := j.addRecovery(x.key, x.r, joinCols); err != nil {
			t.Fatal(err)
		}
	}
	ex := &MismatchExplanation{}
	if err := j.diff(ctx, joinCols, ex); err != nil {
		t.Fatal(err)
	}
	return ex
}

type kr = struct {
	key string
	r   rowCells
}

func row(pk string, vals ...[]byte) rowCells { return rowCells{pk: pk, cells: cellsOf(vals...)} }

// TestRowJoin_kindsAndOrder pins what the old map join produced: changed and
// extra rows in recovery-stream order, then missing rows by primary-key label
// (byte order), with equal rows silent.
func TestRowJoin_kindsAndOrder(t *testing.T) {
	b := func(s string) []byte { return []byte(s) }
	ex := runJoin(t,
		[]kr{
			{"k3", row("id=3", b("3"), b("c"), b("x"))},
			{"k1", row("id=1", b("1"), b("a"), b("x"))},
			{"k9", row("id=9", b("9"), b("z"), b("x"))},
			{"k8", row("id=8", b("8"), b("y"), b("x"))},
		},
		[]kr{
			{"k5", row("id=5", b("5"), b("e"), b("x"))}, // extra
			{"k3", row("id=3", b("3"), b("C"), b("x"))}, // changed
			{"k1", row("id=1", b("1"), b("a"), b("x"))}, // equal
		})
	var got []string
	for _, d := range ex.Diffs {
		got = append(got, d.Kind+":"+d.PK)
	}
	want := "extra:id=5 changed:id=3 missing:id=8 missing:id=9"
	if strings.Join(got, " ") != want {
		t.Fatalf("diffs = %v, want %s", got, want)
	}
	if ex.Total != 4 {
		t.Errorf("Total = %d, want 4", ex.Total)
	}
	c := ex.Diffs[1].Cells
	if len(c) != 1 || c[0].Column != "note" || c[0].Recovery != "C" || c[0].Baseline != "c" {
		t.Errorf("changed cells = %+v, want note C vs c", c)
	}
}

// TestRowJoin_nullVsEmpty: a SQL NULL and an empty value are different cells,
// as in cellEqual and the digest; equal NULLs are not a difference.
func TestRowJoin_nullVsEmpty(t *testing.T) {
	ex := runJoin(t,
		[]kr{{"k1", row("id=1", []byte("1"), []byte(""), nil)}},
		[]kr{{"k1", row("id=1", []byte("1"), nil, nil)}})
	if ex.Total != 1 || ex.Diffs[0].Kind != diffChanged {
		t.Fatalf("want one changed row, got %+v", ex.Diffs)
	}
	c := ex.Diffs[0].Cells
	if len(c) != 1 || c[0].Column != "note" || c[0].Recovery != "NULL" || c[0].Baseline != "" {
		t.Errorf("cells = %+v, want note NULL vs empty", c)
	}
}

// TestRowJoin_binaryKey: a primary key with a NUL byte and bytes that are not
// UTF-8 still joins (keys are BLOBs, never text).
func TestRowJoin_binaryKey(t *testing.T) {
	k := "3:\x00\xff\xfe"
	ex := runJoin(t,
		[]kr{{k, row("id=\x00\xff\xfe", []byte("1"), []byte("a"), nil)}},
		[]kr{{k, row("id=\x00\xff\xfe", []byte("1"), []byte("a"), nil)}})
	if ex.Total != 0 {
		t.Fatalf("binary key did not join: %+v", ex.Diffs)
	}
}

// TestRowJoin_duplicateRecoveryKey: a key the recovery emits twice is
// compared each time, as the map join did.
func TestRowJoin_duplicateRecoveryKey(t *testing.T) {
	b := func(s string) []byte { return []byte(s) }
	ex := runJoin(t,
		[]kr{{"k1", row("id=1", b("1"), b("new"), nil)}},
		[]kr{{"k1", row("id=1", b("1"), b("new"), nil)}, {"k1", row("id=1", b("1"), b("other"), nil)}})
	if ex.Total != 1 || ex.Diffs[0].Cells[0].Recovery != "other" || ex.Diffs[0].Cells[0].Baseline != "new" {
		t.Fatalf("diffs = %+v, want one changed row other vs new", ex.Diffs)
	}
}

// TestRowJoin_deferredPastCap: a deferred-type difference beyond the printed cap
// still turns on the caveat, as when every row was compared in Go.
func TestRowJoin_deferredPastCap(t *testing.T) {
	var truth, rec []kr
	for i := range maxExplainRows + 5 {
		k := fmt.Sprintf("k%04d", i)
		kind := []byte("x")
		if i == maxExplainRows+3 {
			kind = []byte("y")
		}
		truth = append(truth, kr{k, row(k, []byte("1"), []byte("a"), []byte("x"))})
		note := []byte("b")
		if i == maxExplainRows+3 {
			note = []byte("a")
		}
		rec = append(rec, kr{k, row(k, []byte("1"), note, kind)})
	}
	ex := runJoin(t, truth, rec)
	if ex.Total != maxExplainRows+5 || len(ex.Diffs) != maxExplainRows {
		t.Fatalf("Total=%d shown=%d", ex.Total, len(ex.Diffs))
	}
	if !ex.deferredSeen {
		t.Error("a deferred-type difference past the cap must still show the caveat")
	}
}

// TestRowJoin_emptySides: nothing on either side is no difference, not an error.
func TestRowJoin_emptySides(t *testing.T) {
	if ex := runJoin(t, nil, nil); ex.Total != 0 {
		t.Fatalf("Total = %d", ex.Total)
	}
}

// TestEncodeCells_roundTrip: the encoding keeps NULL apart from empty and
// decodes back to the same cells; truncated input is an error, not a panic.
func TestEncodeCells_roundTrip(t *testing.T) {
	in := cellsOf([]byte("1"), []byte(""), nil)
	got, err := decodeCells(encodeCells(in, joinCols, nil), len(joinCols))
	if err != nil {
		t.Fatal(err)
	}
	if string(got[0]) != "1" || got[1] == nil || len(got[1]) != 0 || got[2] != nil {
		t.Fatalf("round trip = %q", got)
	}
	enc := encodeCells(cellsOf([]byte("abc"), nil, nil), joinCols, nil)
	if _, err := decodeCells(enc[:3], len(joinCols)); err == nil {
		t.Error("truncated row must fail")
	}
}

// TestRowJoin_spillsUnderCap is the point of #2385: far more rows than the
// session's memory cap, and the join still finishes (DuckDB spills to its temp
// directory) while the Go heap stays flat instead of holding the table.
func TestRowJoin_spillsUnderCap(t *testing.T) {
	if testing.Short() {
		t.Skip("appends a few hundred MB")
	}
	ctx := context.Background()
	j, err := newRowJoinCapped(ctx, duckdbutil.Tuning{}, "64MB")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	const n = 3_000_000
	pad := []byte(strings.Repeat("p", 100))
	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range n {
		k := fmt.Sprintf("%d", i)
		r := row(k, []byte(k), pad, nil)
		if err := j.addTruth(k, r, joinCols); err != nil {
			t.Fatal(err)
		}
		switch i % 3 {
		case 1: // a third of the rows differ
			r = row(k, []byte(k), []byte("changed"), nil)
		case 2: // and a few are only in the new baseline
			if i%300 == 2 {
				continue
			}
		}
		if err := j.addRecovery(k, r, joinCols); err != nil {
			t.Fatal(err)
		}
	}
	var after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&after)
	if grew := int64(after.HeapAlloc) - int64(before.HeapAlloc); grew > 64<<20 {
		t.Errorf("Go heap grew %d MB appending %d rows; the table is being held in Go", grew>>20, n)
	}
	ex := &MismatchExplanation{}
	if err := j.diff(ctx, joinCols, ex); err != nil {
		t.Fatalf("join under a 64MB cap failed instead of spilling: %v", err)
	}
	if ex.byKind[diffChanged] != n/3 || ex.byKind[diffMissing] != n/300 || ex.Total != n/3+n/300 {
		t.Fatalf("Total=%d byKind=%v", ex.Total, ex.byKind)
	}
	if len(ex.Diffs) != maxExplainRows || ex.Diffs[0].PK != "1" || ex.Diffs[1].PK != "4" {
		t.Fatalf("first diffs = %+v", ex.Diffs[:2])
	}
	j.close()
	if _, err := os.Stat(j.dir); !os.IsNotExist(err) {
		t.Errorf("working dir %s (database and spill files) not removed: %v", j.dir, err)
	}
}

// TestRemoveOldExplainDirs: a drill-down directory a killed process left
// behind is removed once it is past any run still going; a recent one, and
// anything not ours, stays.
func TestRemoveOldExplainDirs(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, explainDirPrefix+"old")
	fresh := filepath.Join(dir, explainDirPrefix+"fresh")
	other := filepath.Join(dir, "something-else")
	for _, d := range []string{old, fresh, other} {
		if err := os.MkdirAll(filepath.Join(d, "x"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-explainDirMaxAge - time.Hour)
	for _, d := range []string{old, other} {
		if err := os.Chtimes(d, past, past); err != nil {
			t.Fatal(err)
		}
	}
	removeOldExplainDirs(dir)
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Errorf("old drill-down dir not removed: %v", err)
	}
	for _, d := range []string{fresh, other} {
		if _, err := os.Stat(d); err != nil {
			t.Errorf("%s must stay: %v", d, err)
		}
	}
}

// TestRowJoin_capFollowsTheJobBudget: under a job's DuckDB budget (#2385) the
// join takes that memory cap instead of its own 1GB.
func TestRowJoin_capFollowsTheJobBudget(t *testing.T) {
	ctx := duckdbutil.WithSessionTuning(context.Background(), duckdbutil.Tuning{Threads: 2, MemoryLimit: "300MiB"})
	j, err := newRowJoin(ctx, duckdbutil.Tuning{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	var got string
	if err := j.conn.QueryRowContext(ctx, "SELECT current_setting('memory_limit')").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "300.0 MiB" {
		t.Errorf("join memory_limit = %q, want the job's 300MiB", got)
	}
}

// TestRowJoin_threadsFollowTheJobBudget: under a job's DuckDB budget the
// join takes the budget's threads (#2491) when its caller set none, and the
// caller's when it did; with no budget, DefaultTuning's 2.
func TestRowJoin_threadsFollowTheJobBudget(t *testing.T) {
	budget := duckdbutil.WithSessionTuning(context.Background(), duckdbutil.Tuning{Threads: 1, MemoryLimit: "300MiB"})
	for _, tc := range []struct {
		name   string
		ctx    context.Context
		tuning duckdbutil.Tuning
		want   string
	}{
		{"budget, caller set none", budget, duckdbutil.Tuning{}, "1"},
		{"budget, caller set 2", budget, duckdbutil.Tuning{Threads: 2}, "2"},
		{"no budget", context.Background(), duckdbutil.Tuning{}, "2"},
	} {
		j, err := newRowJoin(tc.ctx, tc.tuning)
		if err != nil {
			t.Fatal(err)
		}
		var got string
		if err := j.conn.QueryRowContext(tc.ctx, "SELECT current_setting('threads')").Scan(&got); err != nil {
			t.Fatal(err)
		}
		j.close()
		if got != tc.want {
			t.Errorf("%s: join threads = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestRowJoin_neverAboveItsOwnCap: a job budget larger than the join's own
// 1GB (a large host) leaves the join at 1GB.
func TestRowJoin_neverAboveItsOwnCap(t *testing.T) {
	ctx := duckdbutil.WithSessionTuning(context.Background(), duckdbutil.Tuning{Threads: 2, MemoryLimit: "4096MiB"})
	j, err := newRowJoin(ctx, duckdbutil.Tuning{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	var got string
	if err := j.conn.QueryRowContext(ctx, "SELECT current_setting('memory_limit')").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "953.6 MiB" && got != "1.0 GiB" && got != "1000.0 MiB" {
		t.Errorf("join memory_limit = %q, want its own 1GB", got)
	}
}

// TestRowJoin_WorkingFilesInTheProcessScratchDir: a drill-down runs in a child
// that can be killed (#2385). Its join must then be in that process's own
// scratch directory, which the next process removes once this one is dead,
// not in the shared temp dir, where only a day-old sweep would take it.
func TestRowJoin_WorkingFilesInTheProcessScratchDir(t *testing.T) {
	j, err := newRowJoin(context.Background(), duckdbutil.Tuning{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if got, want := filepath.Dir(j.dir), duckdbutil.ScratchDir(); got != want {
		t.Errorf("the join's files are under %s, want this process's scratch dir %s", got, want)
	}
}
