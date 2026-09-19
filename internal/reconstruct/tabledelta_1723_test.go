package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #1723: a minor compaction merges a prefix of a chain into one range pair.
// The state read through the range must be the state read through the
// pairs, the range must list, validate and carry forward like any pair, and
// a refresh after it extends the chain past it.

// installRange puts a compaction's range pair beside base in place of the
// pairs it merged: what the refresh does by linking forward.
func installRange(t *testing.T, base string, chain *baseline.TableDeltaChain, mc *MinorCompaction) {
	t.Helper()
	for _, f := range chain.Files {
		if f.SeqLo >= mc.Lo && f.Seq <= mc.Hi {
			for _, p := range []string{f.Posdel, f.Upserts} {
				if err := os.Remove(p); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	posdel, upserts := baseline.TableDeltaRangePaths(base, mc.Lo, mc.Hi)
	for _, p := range [][2]string{{mc.Posdel, posdel}, {mc.Upserts, upserts}} {
		if err := os.Rename(p[0], p[1]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompactTableDeltaMinor(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows() // ids 1, 2, 3 at base rows 0, 1, 2
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	windows := []map[string]*query.ResultRow{
		changeMap(upd(1, "one-v2"), del(2), ins(9, "nine")),
		changeMap(upd(1, "one-v3"), del(9), ins(2, "two-back"), upd(3, "three")),
		changeMap(del(1), ins(4, "four")),
	}
	refPrev, deltaPrev, prevTime := src, src, t0
	var refOut, base string
	at := t0
	for i, w := range windows {
		at = t0.Add(time.Duration(i+1) * 5 * time.Minute)
		cut := &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * (i + 1))}
		_, refOut, _ = emitSnapshot(t, refPrev, cloneChanges(w), cut, at)
		newBase, _, err := deltaWindow(t, root, deltaPrev, prevTime, cloneChanges(w), at, cut, nil)
		if err != nil {
			t.Fatalf("window %d: %v", i, err)
		}
		refPrev, deltaPrev, prevTime, base = refOut, newBase, at, newBase
	}
	chain, err := baseline.ListTableDelta(context.Background(), base)
	if err != nil || len(chain.Files) != 3 {
		t.Fatalf("chain = %+v err=%v, want three plain pairs", chain, err)
	}
	want := byID(readSnapshotRows(t, refOut))

	// Not covered exactly: refused, nothing written.
	out := t.TempDir()
	if _, err := CompactTableDeltaMinor(context.Background(), base, chain, 1, 5, out); err == nil || !strings.Contains(err.Error(), "covering exactly 1..5") {
		t.Fatalf("err = %v, want the uncovered range refused", err)
	}
	if left, _ := os.ReadDir(out); len(left) != 0 {
		t.Fatalf("a refused compaction left %v", left)
	}

	// Pairs 0..1 into one range; pair 2 stays plain after it.
	mc, err := CompactTableDeltaMinor(context.Background(), base, chain, 0, 1, out)
	if err != nil {
		t.Fatal(err)
	}
	if mc.Merged != 2 || filepath.Base(mc.Posdel) != "orders.000000-000001.posdel" {
		t.Fatalf("compaction = %+v", mc)
	}
	// The footer is pair 1's with the two ends; nothing else of it lost.
	um, err := baseline.ReadParquetMetadata(mc.Upserts)
	if err != nil {
		t.Fatal(err)
	}
	p1, _ := baseline.ReadParquetMetadata(chain.Files[1].Upserts)
	if um.DeltaSeq != 1 || um.DeltaSeqLo != 0 || um.BinlogPos != p1.BinlogPos || !um.SnapshotTimestamp.Equal(p1.SnapshotTimestamp) ||
		!um.DeltaChainStart.Equal(p1.DeltaChainStart) || um.DeltaBaseAnchor != p1.DeltaBaseAnchor || um.DeltaBaseSize != p1.DeltaBaseSize || um.LastEventID != p1.LastEventID {
		t.Fatalf("range footer = %+v, want pair 1's with ends 0..1 (pair 1: %+v)", um, p1)
	}
	if um.CreateTableSQL != p1.CreateTableSQL || um.CreateTableSQL == "" || !strings.Contains(um.CreateTableSQL, "\n") {
		t.Fatalf("the range footer's CREATE TABLE is not pair 1's bytes:\n%q\nwant\n%q", um.CreateTableSQL, p1.CreateTableSQL)
	}
	pm, _ := baseline.ReadParquetMetadata(mc.Posdel)
	if pm.DeltaSeq != 1 || pm.DeltaSeqLo != 0 || pm.BinlogPos != um.BinlogPos {
		t.Fatalf("posdel footer = %+v", pm)
	}
	installRange(t, base, chain, mc)
	chain, err = baseline.ListTableDelta(context.Background(), base)
	if err != nil || len(chain.Files) != 2 || !chain.Files[0].Range() || chain.Files[0].SeqLo != 0 || chain.Files[0].Seq != 1 || chain.Files[1].Seq != 2 {
		t.Fatalf("chain after the compaction = %+v err=%v, want [0-1, 2]", chain, err)
	}
	if got := byID(deltaState(t, base)); !reflect.DeepEqual(got, want) {
		t.Fatalf("state through the range differs from the reference:\n got %v\nwant %v", got, want)
	}
	bmeta, _ := baseline.ReadParquetMetadata(base)
	d, err := readTableDelta(context.Background(), base, bmeta)
	if err != nil || d == nil || d.Meta.DeltaSeq != 2 {
		t.Fatalf("readTableDelta over the range = %+v err=%v, want the chain accepted with pair 2 last", d, err)
	}

	// A refresh after the compaction carries the range forward under its
	// own name and writes pair 3; the state still matches the reference.
	w := changeMap(upd(4, "four-v2"), del(3))
	at = at.Add(5 * time.Minute)
	cut := &query.BinlogPos{File: "binlog.000009", Pos: 4000}
	_, refOut, _ = emitSnapshot(t, refOut, cloneChanges(w), cut, at)
	newBase, rep, err := deltaWindow(t, root, base, prevTime, cloneChanges(w), at, cut, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.DeltaSeq != 3 || rep.DeltaCompacted != "" {
		t.Fatalf("report = %+v, want pair 3 written, no rewrite", rep)
	}
	names, _ := filepath.Glob(strings.TrimSuffix(newBase, ".parquet") + ".*")
	for i := range names {
		names[i] = filepath.Base(names[i])
	}
	wantNames := []string{"orders.000000-000001.posdel", "orders.000000-000001.upserts", "orders.000002.posdel", "orders.000002.upserts",
		"orders.000003.posdel", "orders.000003.upserts", "orders.parquet"}
	if !reflect.DeepEqual(names, wantNames) {
		t.Fatalf("files beside the new base = %v\nwant %v", names, wantNames)
	}
	if got := byID(deltaState(t, newBase)); !reflect.DeepEqual(got, byID(readSnapshotRows(t, refOut))) {
		t.Fatalf("state after extending past the range differs from the reference")
	}
	// The range travelled by hard link, like a plain pair.
	old, _ := os.Stat(filepath.Join(filepath.Dir(base), "orders.000000-000001.upserts"))
	linked, _ := os.Stat(filepath.Join(filepath.Dir(newBase), "orders.000000-000001.upserts"))
	if !os.SameFile(old, linked) {
		t.Fatal("the range pair was copied, not linked, into the next snapshot")
	}
}

// TestPublishWithTableDelta_adoptsACompaction: a complete result under the
// compaction directory for the chain's start is linked into the next
// snapshot in place of the pairs it merged, and removed once adopted; a
// result for another chain is removed unadopted; an unfinished one is left
// alone and the chain travels plain.
func TestPublishWithTableDelta_adoptsACompaction(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	compactDir := filepath.Join(root, ".compact")
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	windows := []map[string]*query.ResultRow{
		changeMap(upd(1, "one-v2"), del(2), ins(9, "nine")),
		changeMap(upd(1, "one-v3"), del(9), ins(2, "two-back")),
		changeMap(del(1), ins(4, "four")),
	}
	refPrev, deltaPrev, prevTime := src, src, t0
	var refOut, base string
	at := t0
	for i, w := range windows {
		at = t0.Add(time.Duration(i+1) * 5 * time.Minute)
		cut := &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * (i + 1))}
		_, refOut, _ = emitSnapshot(t, refPrev, cloneChanges(w), cut, at)
		newBase, _, err := deltaWindow(t, root, deltaPrev, prevTime, cloneChanges(w), at, cut, nil)
		if err != nil {
			t.Fatal(err)
		}
		refPrev, deltaPrev, prevTime, base = refOut, newBase, at, newBase
	}
	chain, _ := baseline.ListTableDelta(context.Background(), base)
	bmeta, _ := baseline.ReadParquetMetadata(base)
	prev, _ := readTableDelta(context.Background(), base, bmeta)
	stage := CompactionDir(compactDir, "mydb", "orders", prev.Meta.DeltaChainStart)
	withCompact := func(p *tableDeltaPublish) { p.cfg.CompactDir = compactDir }
	next := func(i int, w map[string]*query.ResultRow) (string, *TableReport) {
		t.Helper()
		at = at.Add(5 * time.Minute)
		cut := &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * (i + 10))}
		_, refOut, _ = emitSnapshot(t, refOut, cloneChanges(w), cut, at)
		nb, rep, err := deltaWindow(t, root, base, prevTime, cloneChanges(w), at, cut, withCompact)
		if err != nil {
			t.Fatal(err)
		}
		prevTime, base = at, nb
		if got := byID(deltaState(t, nb)); !reflect.DeepEqual(got, byID(readSnapshotRows(t, refOut))) {
			t.Fatalf("state differs from the reference after window %d", i)
		}
		return nb, rep
	}
	names := func(nb string) []string {
		out, _ := filepath.Glob(strings.TrimSuffix(nb, ".parquet") + ".*")
		for i := range out {
			out[i] = filepath.Base(out[i])
		}
		return out
	}

	// Unfinished (no _SUCCESS): left alone, chain carried plain.
	if _, err := CompactTableDeltaMinor(context.Background(), base, chain, 0, 1, stage); err != nil {
		t.Fatal(err)
	}
	nb, rep := next(1, changeMap(upd(4, "four-v2")))
	if rep.DeltaCompactedRange != "" || names(nb)[0] != "orders.000000.posdel" {
		t.Fatalf("an unfinished compaction was adopted: %+v %v", rep, names(nb))
	}
	if _, err := os.Stat(filepath.Join(stage, "orders.000000-000001.upserts")); err != nil {
		t.Fatal("an unfinished compaction was removed")
	}

	// Complete: adopted, removed, the chain reads [0-1, 2, 3, 4].
	if err := os.WriteFile(filepath.Join(stage, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	nb, rep = next(2, changeMap(del(3)))
	if rep.DeltaCompactedRange != "0-1" {
		t.Fatalf("report = %+v, want the range adopted", rep)
	}
	wantNames := []string{"orders.000000-000001.posdel", "orders.000000-000001.upserts", "orders.000002.posdel", "orders.000002.upserts",
		"orders.000003.posdel", "orders.000003.upserts", "orders.000004.posdel", "orders.000004.upserts", "orders.parquet"}
	if got := names(nb); !reflect.DeepEqual(got, wantNames) {
		t.Fatalf("files = %v\nwant %v", got, wantNames)
	}
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Fatalf("the adopted compaction was not removed: %v", err)
	}
	c, err := baseline.ListTableDelta(context.Background(), nb)
	if err != nil || len(c.Files) != 4 || c.Files[0].Seq != 1 || c.Last().Seq != 4 {
		t.Fatalf("chain = %+v err=%v", c, err)
	}

	// A result staged for a chain that has since ended (another chain
	// start under the same table) is removed when the current chain is
	// looked at, so an ended chain's merged pairs never stay on disk.
	other := CompactionDir(compactDir, "mydb", "orders", prev.Meta.DeltaChainStart.Add(time.Hour))
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// And one under the CURRENT chain start whose footers name 2-3 while
	// its name claims 0-3: a result whose name and content disagree. (The
	// coverage check alone accepts 0-3 over [0-1, 2, 3]; it is the footer
	// check that refuses it, and without that check the next window's
	// state, missing windows 0 and 1, would fail the reference above.)
	c, _ = baseline.ListTableDelta(context.Background(), nb)
	stale := CompactionDir(compactDir, "mydb", "orders", prev.Meta.DeltaChainStart)
	if _, err := CompactTableDeltaMinor(context.Background(), nb, c, 2, 3, stale); err != nil {
		t.Fatal(err)
	}
	// Tamper: pretend it merged 0..3 by renaming, so the footers disagree.
	for _, sfx := range []string{baseline.TableDeltaPosdelSuffix, baseline.TableDeltaUpsertsSuffix} {
		if err := os.Rename(filepath.Join(stale, "orders.000002-000003"+sfx), filepath.Join(stale, "orders.000000-000003"+sfx)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(stale, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	nb, rep = next(3, changeMap(ins(5, "five")))
	if rep.DeltaCompactedRange != "" {
		t.Fatalf("a result whose footers disagree was adopted: %+v", rep)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("a rejected compaction result was left behind")
	}
	wantNames = append(wantNames[:8], "orders.000005.posdel", "orders.000005.upserts", "orders.parquet")
	if got := names(nb); !reflect.DeepEqual(got, wantNames) {
		t.Fatalf("files = %v\nwant the earlier range kept and pairs 2..5 plain: %v", got, wantNames)
	}
	if _, err := os.Stat(other); !os.IsNotExist(err) {
		t.Fatal("the ended chain's result was left on disk")
	}
}
