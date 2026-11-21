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

// A model of a tail that goes to disk, with no code of its own: the tail
// writes the keys that changed as a file at the end of its chain and FORGETS
// every key (a new tail takes its place), and the chain is read as views
// read it, the newest file that holds a key winning
// (baseline.TableTailChainStateSQL). Nothing in the product does this yet;
// the tests below say when it gives the table's state and when it does not,
// which is what a tail that goes to disk has to be built on.

// pgSpillSeg is one file of a chain, and the LSN of the last change the tail
// had folded when the file was written.
type pgSpillSeg struct {
	path string
	hi   uint64
}

// pgSpillPaths is the chain's files, oldest first, as SQL literals.
func pgSpillPaths(segs []pgSpillSeg) []string {
	out := make([]string, len(segs))
	for i, s := range segs {
		out[i] = lit(s.path)
	}
	return out
}

// pgSpillRows turns "id=status" lines back into rows.
func pgSpillRows(t *testing.T, lines []string) [][2]string {
	t.Helper()
	out := make([][2]string, len(lines))
	for i, l := range lines {
		id, status, ok := strings.Cut(l, "=")
		if !ok {
			t.Fatalf("a state line that is not id=status: %q", l)
		}
		out[i] = [2]string{id, status}
	}
	return out
}

// pgSpillWhole is the state of base with every one of evs laid over it by
// one tail that forgot nothing: what the table holds.
func pgSpillWhole(t *testing.T, dir, name, base string, md baseline.DumpMetadata, evs []query.ResultRow) []string {
	t.Helper()
	tail := newTestPGTail(t, md)
	if len(evs) > 0 {
		if err := tail.Apply(evs); err != nil {
			t.Fatal(err)
		}
	}
	if tail.Keys() == 0 {
		return readPGState(t, pgPublished(base))
	}
	file := filepath.Join(dir, name)
	if _, err := tail.WriteFile(file); err != nil {
		t.Fatal(err)
	}
	return readPGState(t, baseline.TableTailStateSQL(pgPublished(base), lit(file), tail.PKColumns(), ""))
}

// Changes that arrive in the log's order, as a tail's advance hands them
// (whole transactions, the commit LSN never going back): after any number of
// writes, of forgets, and of moves to a newer copy that drop only the files
// written wholly before its cut, the chain reads as the table.
//
// A file kept across a move to a newer copy may hold rows of changes from
// before that copy's cut. They are the rows the copy holds, or older ones a
// later file of the chain covers, so they change nothing: that is the claim
// the moves here test.
func TestPGTailSpill_inLogOrderTheChainIsTheTable(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		r := rand.New(rand.NewSource(seed))
		root := t.TempDir()
		base, md := writePGTailBase(t, filepath.Join(root, "cut0"), nil,
			[2]string{"1", "base1"}, [2]string{"2", "base2"}, [2]string{"3", "base3"},
			[2]string{"4", "base4"}, [2]string{"5", "base5"}, [2]string{"6", "base6"})
		firstBase, firstMD := base, md
		last := map[string]string{"1": "base1", "2": "base2", "3": "base3", "4": "base4", "5": "base5", "6": "base6"}
		exists := map[string]bool{"1": true, "2": true, "3": true, "4": true, "5": true, "6": true}

		cur := newTestPGTail(t, md)
		var segs []pgSpillSeg
		var all []query.ResultRow // every change, in order
		cut := uint64(0)          // the copy's cut: changes below it are in the copy
		lsn, id, files := uint64(0x1000), uint64(0), 0
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
				got = readPGState(t, baseline.TableTailChainStateSQL(pgPublished(base), pgSpillPaths(segs), cur.PKColumns(), ""))
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("seed %d cycle %d, %s, %d files, cut %#x:\nchain %v\ntable %v", seed, cycle, when, len(segs), cut, got, want)
			}
		}
		for cycle := range 14 {
			var page []query.ResultRow
			for range r.Intn(6) {
				lsn += 0x10
				key := fmt.Sprint(1 + r.Intn(10))
				// One, two or three changes of the key in one transaction.
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
				if err := cur.Apply(page); err != nil {
					t.Fatalf("seed %d cycle %d: %v", seed, cycle, err)
				}
				all = append(all, page...)
			}
			// What changed is written before anything is forgotten.
			if cur.Dirty() > 0 {
				files++
				file := filepath.Join(root, fmt.Sprintf("seg-%d.parquet", files))
				if _, err := cur.WriteDelta(file); err != nil {
					t.Fatalf("seed %d cycle %d: %v", seed, cycle, err)
				}
				segs = append(segs, pgSpillSeg{path: file, hi: lsn})
			}
			check("after the write", cycle)
			// The tail goes to disk: every key is forgotten.
			if r.Intn(3) == 0 {
				cur = newTestPGTail(t, md)
				check("after the forget", cycle)
			}
			// A newer copy, cut at a position at or before the last change:
			// it holds every change below its cut. Files written wholly
			// before the cut go; the others stay as they are.
			if r.Intn(4) == 0 {
				newCut := lsn + 0x8
				if r.Intn(2) == 0 && lsn > 0x1000 {
					newCut = 0x1000 + uint64(r.Intn(int(lsn-0x1000)+1))
				}
				if newCut <= cut {
					continue
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
				kept := segs[:0:0]
				for _, s := range segs {
					if s.hi >= cut {
						kept = append(kept, s)
					}
				}
				segs = kept
				cur = newTestPGTail(t, md)
				check("after the move to a newer copy", cycle)
			}
		}
	}
}

// Why a tail may go to disk only while it advances, never while it is built:
// a build reads its first changes by time, so an older change of a key can
// arrive after a newer one. In memory the newer one is kept (laterChange).
// After a forget nothing remembers it, the older change is written to a newer
// file, and the chain answers with it.
func TestPGTailSpill_outOfLogOrderAForgetGivesAnOlderRow(t *testing.T) {
	dir := t.TempDir()
	base, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"})
	newer := pgEv(2, 0x1020, event.EventUpdate, pgImg("1", "one"), pgImg("1", "newer"))
	older := pgEv(1, 0x1010, event.EventUpdate, pgImg("1", "one"), pgImg("1", "older"))
	write := func(tail *PGTail, name string) string {
		t.Helper()
		file := filepath.Join(dir, name)
		if _, err := tail.WriteDelta(file); err != nil {
			t.Fatal(err)
		}
		return lit(file)
	}

	// One tail that forgets nothing keeps the newer change.
	kept := newTestPGTail(t, md)
	if err := kept.Apply([]query.ResultRow{newer}); err != nil {
		t.Fatal(err)
	}
	first := write(kept, "kept-1.parquet")
	if err := kept.Apply([]query.ResultRow{older}); err != nil {
		t.Fatal(err)
	}
	chain := []string{first}
	if kept.Dirty() > 0 {
		chain = append(chain, write(kept, "kept-2.parquet"))
	}
	if got := strings.Join(readPGState(t, baseline.TableTailChainStateSQL(pgPublished(base), chain, kept.PKColumns(), "")), ","); got != "1=newer" {
		t.Fatalf("a tail that forgot nothing answers %q, want the newer change", got)
	}

	// The same two changes with a forget between them.
	a := newTestPGTail(t, md)
	if err := a.Apply([]query.ResultRow{newer}); err != nil {
		t.Fatal(err)
	}
	one := write(a, "forgot-1.parquet")
	b := newTestPGTail(t, md)
	if err := b.Apply([]query.ResultRow{older}); err != nil {
		t.Fatal(err)
	}
	two := write(b, "forgot-2.parquet")
	if got := strings.Join(readPGState(t, baseline.TableTailChainStateSQL(pgPublished(base), []string{one, two}, b.PKColumns(), "")), ","); got != "1=older" {
		t.Fatalf("after a forget the chain answers %q; this test records that it answers the OLDER change, "+
			"which is why a tail is not to forget keys while changes can arrive out of the log's order", got)
	}
}
