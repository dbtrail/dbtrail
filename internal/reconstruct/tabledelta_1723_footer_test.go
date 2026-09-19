package reconstruct

import (
	"context"
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

// #1723, second look: the three range checks a chain's reader makes on a
// pair's footer, each reached for ITS reason and not an earlier check; the
// compaction that runs over a chain already holding a range (every one
// after the first); and the splice that puts a range in place of the pairs.

func zooWindows() []map[string]*query.ResultRow {
	return []map[string]*query.ResultRow{
		changeMap(upd(1, "one-v2"), del(2), ins(9, "nine")),
		changeMap(upd(1, "one-v3"), del(9), ins(2, "two-back"), upd(3, "three")),
		changeMap(del(1), ins(4, "four")),
	}
}

// plainChain publishes the windows as a chain of plain pairs beside a base
// and returns the base, the reference snapshot and the last window's time.
func plainChain(t *testing.T, root string, windows []map[string]*query.ResultRow) (base, refOut string, at time.Time) {
	t.Helper()
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	refPrev, deltaPrev, prevTime := src, src, t0
	at = t0
	for i, w := range windows {
		at = t0.Add(time.Duration(i+1) * 5 * time.Minute)
		cut := &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * (i + 1))}
		_, refOut, _ = emitSnapshot(t, refPrev, cloneChanges(w), cut, at)
		nb, _, err := deltaWindow(t, root, deltaPrev, prevTime, cloneChanges(w), at, cut, nil)
		if err != nil {
			t.Fatalf("window %d: %v", i, err)
		}
		refPrev, deltaPrev, prevTime, base = refOut, nb, at, nb
	}
	return base, refOut, at
}

// rangeChain is plainChain with pairs 0..1 merged and installed as a range:
// the layout every compaction after the first sees.
func rangeChain(t *testing.T, root string, windows []map[string]*query.ResultRow) (base, refOut string, at time.Time) {
	t.Helper()
	base, refOut, at = plainChain(t, root, windows)
	chain, err := baseline.ListTableDelta(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	mc, err := CompactTableDeltaMinor(context.Background(), base, chain, 0, 1, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	installRange(t, base, chain, mc)
	return base, refOut, at
}

// footerOf copies the footer keys the chain reader compares, so a pair can
// be rewritten with exactly one of them changed.
func footerOf(um baseline.DumpMetadata, createSQL string) map[string]string {
	return map[string]string{
		baseline.MetaKeySnapshotTimestamp: um.SnapshotTimestamp.Format(time.RFC3339),
		baseline.MetaKeyBinlogFile:        um.BinlogFile,
		baseline.MetaKeyBinlogPos:         strconv.FormatInt(um.BinlogPos, 10),
		baseline.MetaKeyCreateTableSQL:    createSQL,
		baseline.MetaKeyDeltaChainStart:   um.DeltaChainStart.Format(time.RFC3339),
		baseline.MetaKeyDeltaBaseAnchor:   um.DeltaBaseAnchor,
		baseline.MetaKeyDeltaBaseSize:     strconv.FormatInt(um.DeltaBaseSize, 10),
	}
}

func TestReadTableDelta_rangeFooterReasons(t *testing.T) {
	noCompaction(t)
	setAsideFor := func(t *testing.T, base, want string) {
		t.Helper()
		bmeta, _ := baseline.ReadParquetMetadata(base)
		d, why, err := readTableDeltaReason(context.Background(), base, bmeta)
		if err != nil || d != nil || !strings.Contains(why, want) {
			t.Fatalf("d=%v why=%q err=%v, want set aside for %q", d, why, err, want)
		}
	}
	t.Run("control: a range with the right footer is accepted", func(t *testing.T) {
		base, _, _ := rangeChain(t, t.TempDir(), zooWindows())
		bmeta, _ := baseline.ReadParquetMetadata(base)
		d, why, err := readTableDeltaReason(context.Background(), base, bmeta)
		if err != nil || d == nil || why != "" || len(d.Chain.Files) != 2 || !d.Chain.Files[0].Range() {
			t.Fatalf("d=%+v why=%q err=%v", d, why, err)
		}
	})
	t.Run("range name, plain footer", func(t *testing.T) {
		// [0-1, 2, 3] with pair 2 gone and pair 3 (both halves, so they
		// agree with each other and the high end matches the name) under
		// the name 2-3: a plain pair renamed to claim windows it lacks.
		base, _, _ := rangeChain(t, t.TempDir(), append(zooWindows(), changeMap(upd(4, "four-v2"))))
		p2, u2 := baseline.TableDeltaPaths(base, 2)
		p3, u3 := baseline.TableDeltaPaths(base, 3)
		rp, ru := baseline.TableDeltaRangePaths(base, 2, 3)
		for _, p := range []string{p2, u2} {
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
		}
		for _, m := range [][2]string{{p3, rp}, {u3, ru}} {
			if err := os.Rename(m[0], m[1]); err != nil {
				t.Fatal(err)
			}
		}
		setAsideFor(t, base, "the file named sequences 2-3 records low end -1 in its footer")
	})
	t.Run("plain name, range footer", func(t *testing.T) {
		// A plain chain whose pair 1 records a range 0-1 in its footer: a
		// range pair renamed to pass as a plain one.
		base, _, _ := plainChain(t, t.TempDir(), zooWindows())
		bmeta, _ := baseline.ReadParquetMetadata(base)
		cols, err := baseline.ParseSchemaText(bmeta.CreateTableSQL)
		if err != nil {
			t.Fatal(err)
		}
		posdel, upserts := baseline.TableDeltaPaths(base, 1)
		um, _ := baseline.ReadParquetMetadata(upserts)
		for _, p := range []string{posdel, upserts} {
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
		}
		if err := baseline.WriteTableDeltaPair(base, 1, cols, baseline.WithDeltaSeqRange(footerOf(um, bmeta.CreateTableSQL), 0, 1), nil, nil); err != nil {
			t.Fatal(err)
		}
		setAsideFor(t, base, "the file named sequence 1 records a range starting at 0 in its footer")
	})
	t.Run("halves disagree on the low end", func(t *testing.T) {
		// The range's dead-positions file rewritten with the same sequence
		// and another low end: every earlier check passes (same run by
		// anchor, chain start, position and sequence; the upserts name the
		// right range), so only the last one can refuse.
		base, _, _ := rangeChain(t, t.TempDir(), zooWindows())
		bmeta, _ := baseline.ReadParquetMetadata(base)
		posdel, _ := baseline.TableDeltaRangePaths(base, 0, 1)
		pm, _ := baseline.ReadParquetMetadata(posdel)
		if pm.DeltaSeqLo != 0 || pm.DeltaSeq != 1 {
			t.Fatalf("range posdel footer = %+v", pm)
		}
		if err := os.Remove(posdel); err != nil {
			t.Fatal(err)
		}
		if _, err := baseline.WritePosdel(posdel, baseline.WithDeltaSeqRange(footerOf(pm, bmeta.CreateTableSQL), 1, 1), nil); err != nil {
			t.Fatal(err)
		}
		setAsideFor(t, base, "its two files record different low ends (1 and 0)")
	})
}

// TestCompactTableDeltaMinor_overAChainHoldingARange: from the second
// compaction on, the chain's first entry IS a range. The merge reads it with
// the pairs after it, the newest version still wins (the range's name sorts
// before every later pair's), the result names 0..hi, and the next refresh
// adopts it in place of the range AND the pairs.
func TestCompactTableDeltaMinor_overAChainHoldingARange(t *testing.T) {
	noCompaction(t)
	root := t.TempDir()
	compactDir := filepath.Join(root, ".compact")
	base, refOut, at := rangeChain(t, root, zooWindows()) // [0-1, 2]
	prevTime := at
	window := func(pos uint64, w map[string]*query.ResultRow, opt func(*tableDeltaPublish)) *TableReport {
		t.Helper()
		at = at.Add(5 * time.Minute)
		cut := &query.BinlogPos{File: "binlog.000009", Pos: pos}
		_, refOut, _ = emitSnapshot(t, refOut, cloneChanges(w), cut, at)
		nb, rep, err := deltaWindow(t, root, base, prevTime, cloneChanges(w), at, cut, opt)
		if err != nil {
			t.Fatal(err)
		}
		prevTime, base = at, nb
		if got := byID(deltaState(t, nb)); !reflect.DeepEqual(got, byID(readSnapshotRows(t, refOut))) {
			t.Fatalf("state differs from the reference after the window cut at %d", pos)
		}
		return rep
	}
	window(4000, changeMap(upd(4, "four-v2"), del(3)), nil) // [0-1, 2, 3]
	chain, err := baseline.ListTableDelta(context.Background(), base)
	if err != nil || len(chain.Files) != 3 || !chain.Files[0].Range() || chain.Files[2].Seq != 3 {
		t.Fatalf("chain = %+v err=%v, want [0-1, 2, 3]", chain, err)
	}

	out := t.TempDir()
	mc, err := CompactTableDeltaMinor(context.Background(), base, chain, 0, 2, out)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(mc.Posdel) != "orders.000000-000002.posdel" || mc.Merged != 2 {
		t.Fatalf("compaction = %+v, want the range and pair 2 merged into 0-2", mc)
	}
	um, _ := baseline.ReadParquetMetadata(mc.Upserts)
	p2, _ := baseline.ReadParquetMetadata(chain.Files[1].Upserts)
	if um.DeltaSeq != 2 || um.DeltaSeqLo != 0 || um.BinlogPos != p2.BinlogPos || !um.DeltaChainStart.Equal(p2.DeltaChainStart) || um.CreateTableSQL != p2.CreateTableSQL {
		t.Fatalf("range footer = %+v, want pair 2's with ends 0..2", um)
	}

	// Staged as the job stages it; the next refresh adopts it.
	bmeta, _ := baseline.ReadParquetMetadata(base)
	prev, err := readTableDelta(context.Background(), base, bmeta)
	if err != nil || prev == nil {
		t.Fatalf("prev = %+v err=%v", prev, err)
	}
	stage := CompactionDir(compactDir, "mydb", "orders", prev.Meta.DeltaChainStart)
	if err := os.MkdirAll(stage, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{mc.Posdel, mc.Upserts} {
		if err := os.Rename(p, filepath.Join(stage, filepath.Base(p))); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(stage, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	rep := window(5000, changeMap(ins(5, "five")), func(p *tableDeltaPublish) { p.cfg.CompactDir = compactDir })
	if rep.DeltaCompactedRange != "0-2" {
		t.Fatalf("report = %+v, want the range 0-2 adopted", rep)
	}
	names, _ := filepath.Glob(strings.TrimSuffix(base, ".parquet") + ".*")
	for i := range names {
		names[i] = filepath.Base(names[i])
	}
	wantNames := []string{"orders.000000-000002.posdel", "orders.000000-000002.upserts", "orders.000003.posdel", "orders.000003.upserts",
		"orders.000004.posdel", "orders.000004.upserts", "orders.parquet"}
	if !reflect.DeepEqual(names, wantNames) {
		t.Fatalf("files beside the new base = %v\nwant %v", names, wantNames)
	}
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Fatalf("the adopted compaction was not removed: %v", err)
	}
}

func TestSpliceRange(t *testing.T) {
	f := func(lo, hi int) baseline.TableDeltaFile { return baseline.TableDeltaFile{SeqLo: lo, Seq: hi} }
	cases := []struct {
		name  string
		files []baseline.TableDeltaFile
		r     baseline.TableDeltaFile
		want  []baseline.TableDeltaFile
	}{
		{"a range and pairs into a wider range", []baseline.TableDeltaFile{f(0, 1), f(2, 2), f(3, 3), f(4, 4)}, f(0, 2),
			[]baseline.TableDeltaFile{f(0, 2), f(3, 3), f(4, 4)}},
		{"not at the start", []baseline.TableDeltaFile{f(0, 0), f(1, 1), f(2, 2), f(3, 3)}, f(1, 2),
			[]baseline.TableDeltaFile{f(0, 0), f(1, 2), f(3, 3)}},
		{"covering nothing", []baseline.TableDeltaFile{f(0, 0), f(1, 1)}, f(5, 6),
			[]baseline.TableDeltaFile{f(0, 0), f(1, 1)}},
	}
	for _, c := range cases {
		if got := spliceRange(c.files, c.r); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: spliceRange = %v, want %v", c.name, got, c.want)
		}
	}
}
