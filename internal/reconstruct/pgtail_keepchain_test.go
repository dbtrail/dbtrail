package reconstruct

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A prune that keeps the chain (#3081): what is pending is written first, the
// keys the newer cut holds leave memory, and the files stay. Random histories
// are compared, after every step, with a tail rebuilt from the cut.

func TestPGChainFilesToRetire(t *testing.T) {
	for _, c := range []struct {
		name  string
		highs []uint64
		floor uint64
		want  int
	}{
		{"no files", nil, 10, 0},
		{"all below", []uint64{1, 2, 3}, 10, 3},
		{"none below", []uint64{20, 30}, 10, 0},
		{"a file ending at the floor holds a change the cut does not", []uint64{9, 10, 11}, 10, 1},
		// The third file is below the floor, but the second may hold an older
		// row of one of its keys: without the third, that row would win.
		{"only a prefix", []uint64{10, 30, 20}, 25, 1},
		{"a file of unknown height stays, and so does all after it", []uint64{1, 0, 2}, 10, 1},
	} {
		if got := PGChainFilesToRetire(c.highs, c.floor); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
}

func TestPGTailPruneKeepingChain_refusals(t *testing.T) {
	dir := t.TempDir()
	_, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"})
	tail := newTestPGTail(t, md)
	if err := tail.Apply([]query.ResultRow{pgIns(1, 0x600, "7", "seven")}); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.PruneKeepingChain(PGTailPos(0x700), 0); err == nil {
		t.Fatal("a key whose last change is in no file was dropped")
	}
	if tail.Keys() != 1 {
		t.Fatalf("a refused prune left %d keys", tail.Keys())
	}
	if _, err := tail.WriteDelta(filepath.Join(dir, "d1.parquet")); err != nil {
		t.Fatal(err)
	}
	if got := tail.WrittenHigh(); got != 0x600 {
		t.Fatalf("the file's highest position is %#x, want 0x600", got)
	}
	if _, err := tail.PruneKeepingChain(query.BinlogPos{File: "binlog.000001", Pos: 4}, 0); err == nil {
		t.Fatal("a position that is not a PostgreSQL tail's was taken")
	}
	if tail.Keys() != 1 {
		t.Fatalf("a refused prune left %d keys", tail.Keys())
	}
	if n, err := tail.PruneKeepingChain(PGTailPos(0x700), 0); err != nil || n != 1 {
		t.Fatalf("prune: %d, %v", n, err)
	}
	// The chain goes on: the next file is a delta.
	if err := tail.Apply([]query.ResultRow{pgIns(2, 0x800, "8", "eight")}); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.WriteDelta(filepath.Join(dir, "d2.parquet")); err != nil {
		t.Fatalf("a delta after a prune that kept the chain: %v", err)
	}
	// After a prune that did NOT keep it, the chain cannot be kept either.
	if _, err := tail.PruneThrough(PGTailPos(0x750), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.PruneKeepingChain(PGTailPos(0x760), 0); err == nil {
		t.Fatal("a chain an earlier prune ended was kept")
	}
}

func TestPGTailWriteSince(t *testing.T) {
	dir := t.TempDir()
	_, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"})
	tail := newTestPGTail(t, md)
	if err := tail.Apply([]query.ResultRow{pgIns(1, 0x600, "a", "1"), pgIns(2, 0x610, "b", "2")}); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.WriteFile(filepath.Join(dir, "w.parquet")); err != nil {
		t.Fatal(err)
	}
	after := tail.LastEventID()
	if err := tail.Apply([]query.ResultRow{pgIns(3, 0x620, "c", "3")}); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.WriteDelta(filepath.Join(dir, "d.parquet")); err != nil {
		t.Fatal(err)
	}
	if err := tail.Apply([]query.ResultRow{pgIns(4, 0x630, "d", "4")}); err != nil {
		t.Fatal(err)
	}
	n, err := tail.WriteSince(filepath.Join(dir, "m.parquet"), after)
	if err != nil || n != 2 {
		t.Fatalf("the merge wrote %d rows, %v: want c and d", n, err)
	}
	if tail.Dirty() != 0 {
		t.Fatalf("%d keys still count as changed", tail.Dirty())
	}
	if _, err := tail.PruneThrough(PGTailPos(0x700), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := tail.WriteSince(filepath.Join(dir, "x.parquet"), 0); err == nil {
		t.Fatal("a merge was written over a chain a prune ended")
	}
}

func TestPGTailKeepChain_randomHistories(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		r := rand.New(rand.NewSource(seed))
		root := t.TempDir()
		base, md := writePGTailBase(t, filepath.Join(root, "cut0"), nil,
			[2]string{"1", "base1"}, [2]string{"2", "base2"}, [2]string{"3", "base3"},
			[2]string{"4", "base4"}, [2]string{"5", "base5"}, [2]string{"6", "base6"})
		firstBase, firstMD := base, md
		last := map[string]string{"1": "base1", "2": "base2", "3": "base3", "4": "base4", "5": "base5", "6": "base6"}
		exists := map[string]bool{"1": true, "2": true, "3": true, "4": true, "5": true, "6": true}
		tail := newTestPGTail(t, md)
		var segs []pgSpillSeg
		var ids []uint64 // the last event id folded when each file was written
		pruned := 0      // the leading files from before the last prune
		var all []query.ResultRow
		cut := uint64(0)
		lsn, id, files := uint64(0x1000), uint64(0), 0
		write := func(whole bool, cycle int) {
			t.Helper()
			files++
			file := filepath.Join(root, fmt.Sprintf("seg-%d.parquet", files))
			var err error
			if whole {
				if tail.Keys() == 0 {
					segs, ids, pruned = nil, nil, 0
					return
				}
				_, err = tail.WriteFile(file)
				segs, ids, pruned = nil, nil, 0
			} else {
				_, err = tail.WriteDelta(file)
			}
			if err != nil {
				t.Fatalf("seed %d cycle %d: %v", seed, cycle, err)
			}
			segs = append(segs, pgSpillSeg{path: file, hi: tail.WrittenHigh()})
			ids = append(ids, tail.LastEventID())
		}
		// merge replaces the files from i on, none from before a prune, by
		// one holding every key changed since the file before them.
		merge := func(cycle int) {
			t.Helper()
			if len(segs)-pruned < 2 {
				return
			}
			i := pruned + r.Intn(len(segs)-pruned-1)
			var after uint64
			if i > 0 {
				after = ids[i-1]
			}
			files++
			file := filepath.Join(root, fmt.Sprintf("merged-%d.parquet", files))
			if _, err := tail.WriteSince(file, after); err != nil {
				t.Fatalf("seed %d cycle %d: %v", seed, cycle, err)
			}
			segs = append(segs[:i:i], pgSpillSeg{path: file, hi: tail.WrittenHigh()})
			ids = append(ids[:i:i], tail.LastEventID())
		}
		check := func(when string, cycle int) {
			t.Helper()
			var since []query.ResultRow
			for _, ev := range all {
				if ev.StartPos >= cut {
					since = append(since, ev)
				}
			}
			files++
			want := pgSpillWhole(t, root, fmt.Sprintf("whole-%d.parquet", files), base, md, since)
			got := readPGState(t, pgPublished(base))
			if len(segs) > 0 {
				got = readPGState(t, baseline.TableTailChainStateSQL(pgPublished(base), pgSpillPaths(segs), tail.PKColumns(), ""))
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("seed %d cycle %d, %s, %d files, cut %#x:\nchain   %v\nrebuilt %v", seed, cycle, when, len(segs), cut, got, want)
			}
		}
		for cycle := range 16 {
			var page []query.ResultRow
			for range r.Intn(6) {
				lsn += 0x10
				key := fmt.Sprint(1 + r.Intn(10))
				for range 1 + r.Intn(3) {
					id++
					val := fmt.Sprintf("s%d-c%d-e%d", seed, cycle, id)
					switch {
					case !exists[key]:
						page = append(page, pgIns(id, lsn, key, val))
						exists[key], last[key] = true, val
					case r.Intn(3) == 0:
						page = append(page, pgEv(id, lsn, event.EventDelete, pgImg(key, last[key]), nil))
						exists[key] = false
					default:
						page = append(page, pgEv(id, lsn, event.EventUpdate, pgImg(key, last[key]), pgImg(key, val)))
						last[key] = val
					}
				}
			}
			if len(page) > 0 {
				if err := tail.Apply(page); err != nil {
					t.Fatalf("seed %d cycle %d: %v", seed, cycle, err)
				}
				all = append(all, page...)
			}
			if tail.Dirty() > 0 && r.Intn(3) != 0 {
				write(r.Intn(5) == 0, cycle)
				if tail.Dirty() == 0 {
					check("after a write", cycle)
				}
			}
			if r.Intn(2) == 0 {
				merge(cycle)
				if tail.Dirty() == 0 {
					check("after a merge", cycle)
				}
			}
			if r.Intn(3) != 0 {
				continue
			}
			// A newer cut. Everything pending is written, then memory is pruned.
			newCut := lsn + 0x8
			if r.Intn(2) == 0 && lsn > 0x1000 {
				newCut = 0x1000 + uint64(r.Intn(int(lsn-0x1000)+1))
			}
			if newCut <= cut {
				continue
			}
			if tail.Dirty() > 0 {
				write(false, cycle)
			}
			var before []query.ResultRow
			for _, ev := range all {
				if ev.StartPos < newCut {
					before = append(before, ev)
				}
			}
			files++
			rows := pgSpillRows(t, pgSpillWhole(t, root, fmt.Sprintf("cutstate-%d.parquet", files), firstBase, firstMD, before))
			base, md = writePGTailBase(t, filepath.Join(root, fmt.Sprintf("cut%d", files)), func(m map[string]string) {
				m[baseline.MetaKeyLSN] = fmt.Sprint(newCut)
			}, rows...)
			cut = newCut
			if _, err := tail.PruneKeepingChain(PGTailPos(cut), 0); err != nil {
				t.Fatalf("seed %d cycle %d: %v", seed, cycle, err)
			}
			highs := make([]uint64, len(segs))
			for i, s := range segs {
				highs[i] = s.hi
			}
			gone := PGChainFilesToRetire(highs, cut)
			segs, ids = segs[gone:], ids[gone:]
			pruned = len(segs)
			check("after a prune that kept the chain", cycle)
		}
	}
}
