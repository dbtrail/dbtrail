package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #1718: a refresh with deltas on writes ONE numbered pair holding its own
// window and links every earlier pair forward. Nothing accumulated is ever
// rewritten. The tests stay DIFFERENTIAL against the full rewrite (#1638's
// rule: a delta that disagrees with the rewrite is wrong whatever else it gets
// right) and add what the layout promises: which file holds what, which file
// is the same inode as before, and what the report counts.

// upsertRows reads a .upserts file as "<pk>:<op>" in file order.
func upsertRows(t *testing.T, path string) []string {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	rows, err := ddb.Query(fmt.Sprintf(`SELECT "%s" || ':' || "%s" FROM parquet_scan('%s')`,
		baseline.TableDeltaPKColumn, baseline.TableDeltaOpColumn, path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func sameFile(a, b string) bool {
	fa, err := os.Stat(a)
	if err != nil {
		return false
	}
	fb, err := os.Stat(b)
	if err != nil {
		return false
	}
	return os.SameFile(fa, fb)
}

func TestTableDelta_oneFilePerWindowNeverRewritten(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows() // ids 1, 2, 3 at base rows 0, 1, 2
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	windows := []struct {
		name    string
		changes map[string]*query.ResultRow
		// what THIS window's pair holds: nothing accumulated
		wantDead []int64
		wantUps  []string
		wantSeq  int
	}{
		{"start: update, delete, insert", changeMap(upd(1, "one-v2"), del(2), ins(9, "nine")),
			[]int64{0, 1}, []string{"1:u", "2:d", "9:u"}, 0},
		{"update again, delete an inserted row, re-insert a deleted one, touch the NULL row",
			changeMap(upd(1, "one-v3"), del(9), ins(2, "two-back"), upd(3, "three")),
			[]int64{0, 1, 2}, []string{"1:u", "2:u", "3:u", "9:d"}, 1},
		{"nothing changed: no file at all", changeMap(), nil, nil, 1},
		{"delete a row that lives in the upserts", changeMap(del(1)), []int64{0}, []string{"1:d"}, 2},
	}

	refPrev, deltaPrev, prevTime := src, src, t0
	var written []string // every delta file ever written, to check it is linked, not copied
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

		chain, err := baseline.ListTableDelta(context.Background(), newBase)
		if err != nil || chain == nil || chain.Legacy {
			t.Fatalf("%s: chain = %+v, %v", w.name, chain, err)
		}
		if chain.Last().Seq != w.wantSeq {
			t.Fatalf("%s: last sequence = %d, want %d", w.name, chain.Last().Seq, w.wantSeq)
		}
		if rep.DeltaSeq != w.wantSeq {
			t.Errorf("%s: report seq = %d, want %d", w.name, rep.DeltaSeq, w.wantSeq)
		}
		posdel, upserts := baseline.TableDeltaPaths(newBase, w.wantSeq)
		if len(w.changes) == 0 {
			// An empty window writes nothing: the previous last pair is still
			// the last, and it is the SAME file.
			if !sameFile(upserts, written[len(written)-1]) {
				t.Fatalf("%s: an empty window wrote a new file", w.name)
			}
			if rep.DeltaDeadRows != 0 || rep.DeltaUpsertRows != 0 || rep.DeltaPairWritten {
				t.Errorf("%s: report says %d dead / %d upserts / written=%v for an empty window", w.name, rep.DeltaDeadRows, rep.DeltaUpsertRows, rep.DeltaPairWritten)
			}
		} else {
			if dead := readPositions(t, posdel); !reflect.DeepEqual(dead, w.wantDead) {
				t.Errorf("%s: dead positions = %v, want %v", w.name, dead, w.wantDead)
			}
			if ups := upsertRows(t, upserts); !reflect.DeepEqual(ups, w.wantUps) {
				t.Errorf("%s: upserts = %v, want %v", w.name, ups, w.wantUps)
			}
			if rep.DeltaDeadRows != int64(len(w.wantDead)) || rep.DeltaUpsertRows != int64(len(w.wantUps)) {
				t.Errorf("%s: report says %d dead / %d upserts, want this window's %d / %d",
					w.name, rep.DeltaDeadRows, rep.DeltaUpsertRows, len(w.wantDead), len(w.wantUps))
			}
			written = append(written, upserts)
			if !rep.DeltaPairWritten {
				t.Errorf("%s: a pair was written but the report says it was not", w.name)
			}
			// Footer: this pair carries its sequence, resumes from this run's
			// cut, and the chain still starts where the first window found the base.
			um, err := baseline.ReadParquetMetadata(upserts)
			if err != nil {
				t.Fatal(err)
			}
			if um.DeltaSeq != w.wantSeq {
				t.Errorf("%s: footer seq = %d, want %d", w.name, um.DeltaSeq, w.wantSeq)
			}
			if um.BinlogFile != cut.File || um.BinlogPos != int64(cut.Pos) {
				t.Errorf("%s: delta anchor = %s:%d, want the run's cut", w.name, um.BinlogFile, um.BinlogPos)
			}
			if !um.DeltaChainStart.Equal(t0) {
				t.Errorf("%s: chain start = %s, want %s", w.name, um.DeltaChainStart, t0)
			}
		}
		// Every EARLIER pair is the same inode in this snapshot: linked, not
		// copied and not rewritten. And the base too.
		for s, f := range chain.Files {
			if s == len(chain.Files)-1 && len(w.changes) > 0 {
				continue
			}
			prevPosdel, prevUps := baseline.TableDeltaPaths(deltaPrev, f.Seq)
			if !sameFile(f.Posdel, prevPosdel) || !sameFile(f.Upserts, prevUps) {
				t.Errorf("%s: pair %d was not carried forward by link", w.name, f.Seq)
			}
		}
		if !sameFile(src, newBase) {
			t.Errorf("%s: the base was not carried forward by link", w.name)
		}
		if rep.DeltaChainFiles != len(chain.Files) {
			t.Errorf("%s: report chain files = %d, want %d", w.name, rep.DeltaChainFiles, len(chain.Files))
		}
		refPrev, deltaPrev, prevTime = refOut, newBase, at
	}
}

// TestTableDelta_compactionStartsTheChainAtZero: past the size threshold the
// table is rewritten from base+chain, holds the same rows a rewrite chain
// holds, and leaves with ONLY the empty sequence-0 pair. The old chain's files
// are not carried into the new snapshot.
func TestTableDelta_compactionStartsTheChainAtZero(t *testing.T) {
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
	if got := byID(readSnapshotRows(t, base2)); !reflect.DeepEqual(got, want) {
		t.Fatalf("compacted base differs from the full rewrite\n got: %v\nwant: %v", got, want)
	}
	if got := byID(deltaState(t, base2)); !reflect.DeepEqual(got, want) {
		t.Fatalf("compacted base+chain differs from the full rewrite\n got: %v\nwant: %v", got, want)
	}
	chain, err := baseline.ListTableDelta(context.Background(), base2)
	if err != nil || chain == nil || len(chain.Files) != 1 || chain.Files[0].Seq != 0 {
		t.Fatalf("after a compaction the chain = %+v, %v; want only sequence 0", chain, err)
	}
	if n := len(readPositions(t, chain.Files[0].Posdel)); n != 0 {
		t.Errorf("a compaction left %d dead positions", n)
	}
	if n := len(upsertRows(t, chain.Files[0].Upserts)); n != 0 {
		t.Errorf("a compaction left %d upserts", n)
	}
	um, _ := baseline.ReadParquetMetadata(chain.Files[0].Upserts)
	if !um.DeltaChainStart.Equal(at2) || um.DeltaSeq != 0 {
		t.Errorf("the chain after a compaction starts at %s seq %d, want this snapshot %s seq 0", um.DeltaChainStart, um.DeltaSeq, at2)
	}
	if sameFile(src, base2) {
		t.Error("a compaction published the old base")
	}
}

// TestTableDelta_sizeRuleCountsTheWholeChain: the 25% rule is over every pair
// of the chain, not the last one, or a chain of small windows would never
// compact.
func TestTableDelta_sizeRuleCountsTheWholeChain(t *testing.T) {
	noSizeFloor(t)
	prev := tableDeltaMaxFraction
	tableDeltaMaxFraction = 0.75 // one window's pair is under it; three are over
	t.Cleanup(func() { tableDeltaMaxFraction = prev })

	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	base, prevTime := src, t0
	var compactedAt int
	for i := 1; i <= 6; i++ {
		at := t0.Add(time.Duration(i) * 5 * time.Minute)
		nb, rep, err := deltaWindow(t, root, base, prevTime, changeMap(upd(1, fmt.Sprintf("v%d", i))), at,
			&query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * i)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rep.DeltaCompacted != "" {
			compactedAt = i
			break
		}
		base, prevTime = nb, at
	}
	if compactedAt < 2 {
		t.Fatalf("compacted at window %d: the rule fired on one window's pair, or never", compactedAt)
	}
}

func TestTableDeltaCompactReason_1718(t *testing.T) {
	noSizeFloor(t)
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	chain := func(age time.Duration, pair int64) *tableDelta {
		return &tableDelta{Meta: baseline.DumpMetadata{DeltaChainStart: at.Add(-age)}, PairSize: pair}
	}
	legacy := &tableDelta{Meta: baseline.DumpMetadata{DeltaChainStart: at.Add(-time.Hour)}, PairSize: 1, Legacy: true}
	cases := []struct {
		name    string
		prev    *tableDelta
		base    string
		spilled bool
		gap     *CaptureGap
		want    string
		// zero values are the ordinary table: it has an anchor, and no
		// reserved column name.
		noAnchor bool
		reserved string
	}{
		{"start a chain", nil, "/b/t.parquet", false, nil, "", false, ""},
		{"extend", chain(time.Hour, 100), "/b/t.parquet", false, nil, "", false, ""},
		{"past a quarter", chain(time.Hour, 251), "/b/t.parquet", false, nil, "passed 25%", false, ""},
		{"past a day", chain(24*time.Hour+time.Minute, 1), "/b/t.parquet", false, nil, "old", false, ""},
		{"s3 source", nil, "s3://b/t.parquet", false, nil, "S3", false, ""},
		{"spilled fold", nil, "/b/t.parquet", true, nil, "did not fit in memory", false, ""},
		{"capture gap", chain(time.Hour, 1), "/b/t.parquet", false, &CaptureGap{}, "capture gap", false, ""},
		{"nothing to resume from", nil, "/b/t.parquet", false, nil, "no binlog position", true, ""},
		{"a column named file_row_number", nil, "/b/t.parquet", false, nil, "file_row_number", false, "file_row_number"},
		{"a column named bintrail_pk", nil, "/b/t.parquet", false, nil, "bintrail_pk", false, "bintrail_pk"},
		{"the v0.83.0 layout", legacy, "/b/t.parquet", false, nil, "older layout", false, ""},
		{"the sequence is exhausted", &tableDelta{Meta: baseline.DumpMetadata{DeltaChainStart: at.Add(-time.Hour), DeltaSeq: baseline.TableDeltaMaxSeq}},
			"/b/t.parquet", false, nil, "sequence", false, ""},
	}
	for _, c := range cases {
		got := tableDeltaCompactReason(c.prev, c.base, 1000, c.spilled, c.gap, at, !c.noAnchor, c.reserved)
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: reason = %q, want it to contain %q", c.name, got, c.want)
		}
	}
}

// TestReadTableDelta_setsAsideDamage: a chain is used only when every pair of
// it was computed against THIS base by runs of ONE chain; otherwise the fold
// starts from the base alone (correct, longer). Half a pair at any sequence
// is set aside the same way, and named by HasTableDelta.
func TestReadTableDelta_setsAsideDamage(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	build := func(t *testing.T) string {
		src := writeZooBaseline(t, rows, nulls)
		root := t.TempDir()
		base, prevTime := src, t0
		for i := 1; i <= 3; i++ {
			at := t0.Add(time.Duration(i) * 5 * time.Minute)
			nb, _, err := deltaWindow(t, root, base, prevTime, changeMap(upd(1, fmt.Sprintf("v%d", i))), at,
				&query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * i)}, nil)
			if err != nil {
				t.Fatal(err)
			}
			base, prevTime = nb, at
		}
		return base
	}
	usable := func(t *testing.T, base string) (*tableDelta, error) {
		bmeta, _ := baseline.ReadParquetMetadata(base)
		return readTableDelta(context.Background(), base, bmeta)
	}

	t.Run("control", func(t *testing.T) {
		base := build(t)
		d, err := usable(t, base)
		if err != nil || d == nil || d.Meta.DeltaSeq != 2 || len(d.Chain.Files) != 3 {
			t.Fatalf("d=%+v err=%v", d, err)
		}
	})
	t.Run("another base", func(t *testing.T) {
		base := build(t)
		other := writeZooBaseline(t, append(rows, []string{"4", "1", "x", "1.00", "2026-01-02 03:04:05", "2026-01-02", "0x00", "1"}),
			append(nulls, make([]bool, 8)))
		data, _ := os.ReadFile(other)
		if err := os.Remove(base); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(base, data, 0o644); err != nil {
			t.Fatal(err)
		}
		bmeta, _ := baseline.ReadParquetMetadata(base)
		if d, err := readTableDelta(context.Background(), base, bmeta); err != nil || d != nil {
			t.Fatalf("a chain computed against another base was accepted (d=%v err=%v)", d, err)
		}
	})
	t.Run("half a pair in the middle", func(t *testing.T) {
		base := build(t)
		posdel, _ := baseline.TableDeltaPaths(base, 1)
		if err := os.Remove(posdel); err != nil {
			t.Fatal(err)
		}
		if d, err := usable(t, base); err != nil || d != nil {
			t.Fatalf("half a pair must be set aside for the fold (d=%v err=%v)", d, err)
		}
		if _, err := baseline.HasTableDelta(context.Background(), base); !errors.Is(err, baseline.ErrHalfTableDelta) {
			t.Fatalf("HasTableDelta: err = %v, want ErrHalfTableDelta", err)
		}
	})
	t.Run("a pair of another chain in the middle", func(t *testing.T) {
		base := build(t)
		// Rewrite pair 1 with a footer that says another chain start.
		bmeta, _ := baseline.ReadParquetMetadata(base)
		cols, _ := baseline.ParseSchemaText(bmeta.CreateTableSQL)
		posdel, upserts := baseline.TableDeltaPaths(base, 1)
		um, _ := baseline.ReadParquetMetadata(upserts)
		os.Remove(posdel)
		os.Remove(upserts)
		md := map[string]string{
			baseline.MetaKeySnapshotTimestamp: um.SnapshotTimestamp.Format(time.RFC3339),
			baseline.MetaKeyBinlogFile:        um.BinlogFile,
			baseline.MetaKeyBinlogPos:         strconv.FormatInt(um.BinlogPos, 10),
			baseline.MetaKeyCreateTableSQL:    bmeta.CreateTableSQL,
			baseline.MetaKeyDeltaChainStart:   um.DeltaChainStart.Add(time.Hour).Format(time.RFC3339),
			baseline.MetaKeyDeltaBaseAnchor:   um.DeltaBaseAnchor,
			baseline.MetaKeyDeltaBaseSize:     strconv.FormatInt(um.DeltaBaseSize, 10),
		}
		if err := baseline.WriteTableDeltaPair(base, 1, cols, md, nil, nil); err != nil {
			t.Fatal(err)
		}
		if d, err := usable(t, base); err != nil || d != nil {
			t.Fatalf("a chain with a pair of another chain was accepted (d=%v err=%v)", d, err)
		}
	})
	t.Run("a pair whose footer disagrees with its name", func(t *testing.T) {
		base := build(t)
		_, ups2 := baseline.TableDeltaPaths(base, 2)
		_, ups1 := baseline.TableDeltaPaths(base, 1)
		data, _ := os.ReadFile(ups1)
		os.Remove(ups2)
		if err := os.WriteFile(ups2, data, 0o644); err != nil {
			t.Fatal(err)
		}
		if d, err := usable(t, base); err != nil || d != nil {
			t.Fatalf("a pair with sequence 1 in its footer under name 2 was accepted (d=%v err=%v)", d, err)
		}
	})
}

// writeLegacyPair writes the v0.83.0 layout beside base: one pair, no
// sequence, no technical columns, holding the given dead positions and
// upsert rows (full images).
func writeLegacyPair(t *testing.T, base string, chainStart time.Time, cut query.BinlogPos, dead []int64, upserts []map[string]any) {
	t.Helper()
	bmeta, err := baseline.ReadParquetMetadata(base)
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(base)
	md := map[string]string{
		baseline.MetaKeySnapshotTimestamp: chainStart.Add(5 * time.Minute).Format(time.RFC3339),
		baseline.MetaKeyBinlogFile:        cut.File,
		baseline.MetaKeyBinlogPos:         strconv.FormatUint(cut.Pos, 10),
		baseline.MetaKeyCreateTableSQL:    bmeta.CreateTableSQL,
		baseline.MetaKeyDeltaChainStart:   chainStart.Format(time.RFC3339),
		baseline.MetaKeyDeltaBaseAnchor:   baseAnchorString(bmeta),
		baseline.MetaKeyDeltaBaseSize:     strconv.FormatInt(fi.Size(), 10),
	}
	stem := strings.TrimSuffix(base, ".parquet")
	posCols, _ := baseline.PosdelColumns()
	pw, err := baseline.NewWriter(stem+baseline.TableDeltaPosdelSuffix, posCols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range dead {
		if err := pw.WriteRow([]string{strconv.FormatInt(p, 10)}, []bool{false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	cols, _ := baseline.ParseSchemaText(bmeta.CreateTableSQL)
	uw, err := newParquetTableWriter(stem+baseline.TableDeltaUpsertsSuffix, cols, md)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range upserts {
		if err := uw.WriteRow(r, "mydb", "orders"); err != nil {
			t.Fatal(err)
		}
	}
	if err := uw.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestTableDelta_legacyPairIsCompactedOnceWithItsChanges: a pair written by
// v0.83.0 is not extended; the refresh that finds it rewrites the table from
// base + that pair + its own window, and starts a numbered chain. The pair's
// changes must be IN the result: dropping them would publish the table as it
// was when the old chain started. And FindBaseline still bounds from the old
// chain's start while the pair is there.
func TestTableDelta_legacyPairIsCompactedOnceWithItsChanges(t *testing.T) {
	noCompaction(t)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	base := writeStampedBaseline(t, root, t0) // ids 1,2,3; the legacy refresh happened at t0+5m
	writeLegacyPair(t, base, t0, query.BinlogPos{File: "binlog.000009", Pos: 500}, []int64{1}, []map[string]any{zooRow(9, "nine")})

	if _, got, _, err := FindBaseline(context.Background(), root, "mydb", "orders", t0.Add(time.Hour)); err != nil || !got.Equal(t0) {
		t.Fatalf("FindBaseline over a legacy pair: time=%s err=%v, want the chain's start %s", got, err, t0)
	}

	at := t0.Add(10 * time.Minute)
	nb, rep, err := deltaWindow(t, root, base, t0, changeMap(upd(1, "one-v2")), at, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.TableDelta || !strings.Contains(rep.DeltaCompacted, "older layout") {
		t.Fatalf("want a compaction naming the older layout, got TableDelta=%v compacted=%q", rep.TableDelta, rep.DeltaCompacted)
	}
	// Base rows minus the legacy dead row (id 2), plus the legacy upsert (9),
	// plus this window's update.
	if got, want := ids(readSnapshotRows(t, nb)), []string{"1", "3", "9"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("compacted state = %v, want %v", got, want)
	}
	for _, r := range readSnapshotRows(t, nb) {
		if fmt.Sprint(r["id"]) == "1" && r["name"] != "one-v2" {
			t.Fatalf("this window's change was lost in the compaction: %v", r)
		}
	}
	chain, err := baseline.ListTableDelta(context.Background(), nb)
	if err != nil || chain == nil || chain.Legacy || len(chain.Files) != 1 || chain.Files[0].Seq != 0 {
		t.Fatalf("after compacting a legacy pair the chain = %+v, %v; want only sequence 0", chain, err)
	}
	// The legacy files did not travel.
	stem := strings.TrimSuffix(nb, ".parquet")
	if _, err := os.Stat(stem + baseline.TableDeltaPosdelSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the v0.83.0 pair was carried into the new snapshot (err=%v)", err)
	}
	// And the fetch resumed from the legacy pair's anchor, not the base's:
	// the window the test handed in is what it folded, and the footer of the
	// new file says so.
	um, _ := baseline.ReadParquetMetadata(chain.Files[0].Upserts)
	if um.BinlogPos != 1000 {
		t.Errorf("new chain anchor = %d, want this run's cut 1000", um.BinlogPos)
	}
}

// TestTableDelta_copyWhenTheLinkFails: a chain file that cannot be hard-linked
// (another filesystem) is COPIED, the state is still right, and the report
// says the base was not linked. The copy is the designed fallback, not a
// failure; each copy is a reason to compact sooner, which the report exposes
// through DeltaChainCopied.
func TestTableDelta_copyWhenTheLinkFails(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	at1 := t0.Add(5 * time.Minute)
	_, ref1, _ := emitSnapshot(t, src, changeMap(del(2)), &query.BinlogPos{File: "binlog.000009", Pos: 1000}, at1)
	base1, _, err := deltaWindow(t, root, src, t0, changeMap(del(2)), at1, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	prev := linkFile
	linkFile = func(_, _ string) error { return errors.New("EXDEV: cross-device link") }
	t.Cleanup(func() { linkFile = prev })

	at2 := t0.Add(10 * time.Minute)
	_, ref2, _ := emitSnapshot(t, ref1, changeMap(ins(9, "nine")), &query.BinlogPos{File: "binlog.000009", Pos: 2000}, at2)
	base2, rep, err := deltaWindow(t, root, base1, at1, changeMap(ins(9, "nine")), at2, &query.BinlogPos{File: "binlog.000009", Pos: 2000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.TableDelta || rep.DeltaChainCopied != 1 {
		t.Fatalf("report = TableDelta %v, copied %d; want a delta with one copied pair", rep.TableDelta, rep.DeltaChainCopied)
	}
	if sameFile(base1, base2) {
		t.Error("the base was linked although the link fails")
	}
	want, got := byID(readSnapshotRows(t, ref2)), byID(deltaState(t, base2))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state after a copied chain differs from the full rewrite\n got: %v\nwant: %v", got, want)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(base2), "orders.000000.posdel")); err != nil {
		t.Fatalf("pair 0 was not copied: %v", err)
	}
}

// TestWriteEmptyTableDeltas_1718: a full backup taken with table deltas on
// leaves with the sequence-0 pair, the pair is one a refresh will EXTEND with
// sequence 1, and the state it describes is the table file's own rows.
func TestWriteEmptyTableDeltas_1718(t *testing.T) {
	noCompaction(t)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	base := writeStampedBaseline(t, root, t0)
	if err := baseline.WriteEmptyTableDeltas(filepath.Dir(filepath.Dir(base))); err != nil {
		t.Fatalf("WriteEmptyTableDeltas: %v", err)
	}
	chain, err := baseline.ListTableDelta(context.Background(), base)
	if err != nil || chain == nil || len(chain.Files) != 1 || chain.Files[0].Seq != 0 {
		t.Fatalf("chain after a full backup = %+v, %v", chain, err)
	}
	if got, want := ids(deltaState(t, base)), []string{"1", "2", "3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state under the empty pair = %v, want the table's own rows %v", got, want)
	}
	if err := baseline.WriteEmptyTableDeltas(filepath.Dir(filepath.Dir(base))); err != nil {
		t.Fatalf("second WriteEmptyTableDeltas: %v", err)
	}
	at := t0.Add(5 * time.Minute)
	nb, rep, err := deltaWindow(t, root, base, t0, changeMap(del(2)), at, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
	if err != nil || !rep.TableDelta || rep.DeltaSeq != 1 {
		t.Fatalf("refresh over a full backup's pair: err=%v delta=%v seq=%d", err, rep.TableDelta, rep.DeltaSeq)
	}
	if got, want := ids(deltaState(t, nb)), []string{"1", "3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %v, want %v", got, want)
	}
}
