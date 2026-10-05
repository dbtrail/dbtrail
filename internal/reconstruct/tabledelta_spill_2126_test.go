package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2126: a window whose changes went to disk (#1107) used to end the chain,
// because the pair was written from the in-memory change map and the spill
// was only readable by the full rewrite. One skipped slot doubled a busy
// table's window past the limit, the table was written in full, the update
// outran its slot, and the schedule fell back to full reads of the source.

// bigWindow touches the three base rows and adds thirty more, so a spill with
// a limit of four reads back in several passes.
func bigWindow(tag string) map[string]*query.ResultRow {
	evs := []*query.ResultRow{upd(1, "one-"+tag), del(2), upd(3, "three-"+tag)}
	for id := 10; id < 40; id++ {
		evs = append(evs, ins(id, "new-"+tag))
	}
	return changeMap(evs...)
}

// spilledChain publishes one in-memory window over the zoo base and then
// window as a second one, from memory or from a spill, and returns the second
// snapshot's base, its report, and the same state written in full.
func spilledChain(t *testing.T, window map[string]*query.ResultRow, spill func(t *testing.T) *changeSpill) (base2, ref2 string, rep *TableReport) {
	t.Helper()
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	at1, at2 := t0.Add(5*time.Minute), t0.Add(10*time.Minute)
	cut1, cut2 := &query.BinlogPos{File: "binlog.000009", Pos: 1000}, &query.BinlogPos{File: "binlog.000009", Pos: 2000}
	w1 := func() map[string]*query.ResultRow { return changeMap(upd(1, "v1"), ins(5, "five")) }
	_, ref1, _ := emitSnapshot(t, src, w1(), cut1, at1)
	_, ref2, _ = emitSnapshot(t, ref1, cloneChanges(window), cut2, at2)
	base1, _, err := deltaWindow(t, root, src, t0, w1(), at1, cut1, nil)
	if err != nil {
		t.Fatal(err)
	}
	changes := cloneChanges(window)
	var mutate func(*tableDeltaPublish)
	if spill != nil {
		changes = map[string]*query.ResultRow{}
		mutate = func(p *tableDeltaPublish) { p.fold.Spill = spill(t) }
	}
	base2, rep, err = deltaWindow(t, root, base1, at1, changes, at2, cut2, mutate)
	if err != nil {
		t.Fatal(err)
	}
	return base2, ref2, rep
}

func sortedUpserts(t *testing.T, base string, seq int) []string {
	t.Helper()
	_, ups := baseline.TableDeltaPaths(base, seq)
	out := upsertRows(t, ups)
	slices.Sort(out)
	return out
}

func deadPositions(t *testing.T, base string, seq int) []int64 {
	t.Helper()
	posdel, _ := baseline.TableDeltaPaths(base, seq)
	return readPositions(t, posdel)
}

// The window that spilled is published as a pair, the table file is the
// previous one, and the pair is the one the same window writes from memory.
func TestTableDelta_spilledWindowWritesAPair(t *testing.T) {
	for _, tc := range []struct {
		name string
		join bool
	}{
		{"integer keys joined in DuckDB", true},
		{"key columns scanned through Go", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			noCompaction(t)
			prev := tableDeltaJoinKeys
			tableDeltaJoinKeys = tc.join
			t.Cleanup(func() { tableDeltaJoinKeys = prev })

			w := bigWindow("final")
			// The spill saw two of these rows change twice: what reads back is
			// the last write.
			spill := func(t *testing.T) *changeSpill {
				return spillOf(t, 4, changeMap(upd(3, "three-early"), ins(10, "new-early")), cloneChanges(w))
			}
			base2, ref2, rep := spilledChain(t, w, spill)
			if !rep.TableDelta || !rep.DeltaPairWritten || rep.DeltaCompacted != "" {
				t.Fatalf("report = TableDelta %v, pair written %v, compacted %q; want a pair and no rewrite",
					rep.TableDelta, rep.DeltaPairWritten, rep.DeltaCompacted)
			}
			if rep.DeltaSpillPasses < 3 {
				t.Fatalf("the spill was read in %d pass(es): the fixture has to exercise more than one", rep.DeltaSpillPasses)
			}
			if rep.DeltaSeq != 1 || rep.DeltaChainFiles != 2 {
				t.Errorf("chain = seq %d, %d file(s); want the second pair of the chain the first window started", rep.DeltaSeq, rep.DeltaChainFiles)
			}
			want, got := byID(readSnapshotRows(t, ref2)), byID(deltaState(t, base2))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("state through a pair written from the spill differs from the full rewrite\n got: %v\nwant: %v", got, want)
			}

			memBase, _, memRep := spilledChain(t, w, nil)
			if memRep.DeltaSpillPasses != 0 {
				t.Errorf("an in-memory window reports %d spill pass(es)", memRep.DeltaSpillPasses)
			}
			if got, want := sortedUpserts(t, base2, 1), sortedUpserts(t, memBase, 1); !reflect.DeepEqual(got, want) {
				t.Errorf("upserts from the spill differ from the same window in memory\n got: %v\nwant: %v", got, want)
			}
			got64, want64 := deadPositions(t, base2, 1), deadPositions(t, memBase, 1)
			if !reflect.DeepEqual(got64, want64) {
				t.Errorf("dead positions from the spill = %v, from memory = %v", got64, want64)
			}
			if !slices.IsSorted(got64) || len(slices.Compact(slices.Clone(got64))) != len(got64) {
				t.Errorf("dead positions %v are not ascending and distinct, which the posdel reader requires", got64)
			}
			if rep.DeltaDeadRows != memRep.DeltaDeadRows || rep.DeltaUpsertRows != memRep.DeltaUpsertRows {
				t.Errorf("report counts %d dead, %d upserts; from memory %d, %d", rep.DeltaDeadRows, rep.DeltaUpsertRows, memRep.DeltaDeadRows, memRep.DeltaUpsertRows)
			}
		})
	}
}

// A spilled window that STARTS a chain writes sequence 0 with its changes.
func TestTableDelta_spilledWindowStartsAChain(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	at, cut := t0.Add(5*time.Minute), &query.BinlogPos{File: "binlog.000009", Pos: 1000}
	w := bigWindow("first")
	_, ref, _ := emitSnapshot(t, src, cloneChanges(w), cut, at)
	base, rep, err := deltaWindow(t, t.TempDir(), src, t0, map[string]*query.ResultRow{}, at, cut,
		func(p *tableDeltaPublish) { p.fold.Spill = spillOf(t, 4, cloneChanges(w)) })
	if err != nil {
		t.Fatal(err)
	}
	if !rep.TableDelta || rep.DeltaSeq != 0 || rep.DeltaCompacted != "" || rep.DeltaSpillPasses < 3 {
		t.Fatalf("report = TableDelta %v seq %d compacted %q passes %d; want pair 0 from the spill",
			rep.TableDelta, rep.DeltaSeq, rep.DeltaCompacted, rep.DeltaSpillPasses)
	}
	want, got := byID(readSnapshotRows(t, ref)), byID(deltaState(t, base))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state differs from the full rewrite\n got: %v\nwant: %v", got, want)
	}
}

// snapshotFilesOf lists what a failed publish left of the table in the new
// snapshot directory.
func snapshotFilesOf(t *testing.T, base string) []string {
	t.Helper()
	ents, err := os.ReadDir(filepath.Dir(base))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}

// failedSpilledWindow publishes one in-memory window and then a spilled one
// that is expected to fail, and returns the error and the would-be base.
func failedSpilledWindow(t *testing.T, mutate func(*tableDeltaPublish)) (string, error) {
	t.Helper()
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	at1, at2 := t0.Add(5*time.Minute), t0.Add(10*time.Minute)
	base1, _, err := deltaWindow(t, root, src, t0, changeMap(upd(1, "v1")), at1, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	base2, _, err := deltaWindow(t, root, base1, at1, map[string]*query.ResultRow{}, at2, &query.BinlogPos{File: "binlog.000009", Pos: 2000}, mutate)
	return base2, err
}

// The #602 guard reads the window's row images. With the changes on disk the
// in-memory map is empty, and a guard run over it alone passes on everything.
func TestTableDelta_spilledWindowStillRefusesAnAddedColumn(t *testing.T) {
	w := bigWindow("x")
	added := ins(77, "wide")
	added.RowAfter["added_later"] = "v"
	w[added.PKValues] = added
	base2, err := failedSpilledWindow(t, func(p *tableDeltaPublish) { p.fold.Spill = spillOf(t, 4, w) })
	if !errors.Is(err, ErrSchemaChanged) {
		t.Fatalf("err = %v, want the added-column refusal (ErrSchemaChanged): the column exists only in rows that are on disk", err)
	}
	if left := snapshotFilesOf(t, base2); len(left) != 0 {
		t.Errorf("a refused table left %v in the new snapshot", left)
	}
}

// A change still in the map beside the ones on disk would never be written.
func TestTableDelta_spillBesideChangesInMemoryIsRefused(t *testing.T) {
	_, err := failedSpilledWindow(t, func(p *tableDeltaPublish) {
		p.fold.Changes = changeMap(upd(3, "in memory"))
		p.fold.Spill = spillOf(t, 4, bigWindow("x"))
	})
	if err == nil || !strings.Contains(err.Error(), "in memory beside the ones on disk") {
		t.Fatalf("err = %v, want the refusal of a fold that holds changes in both places", err)
	}
}

// A group that cannot be read back fails the table after earlier passes have
// written rows: nothing of it may stay published.
func TestTableDelta_spilledWindowThatFailsMidwayLeavesNothing(t *testing.T) {
	var s *changeSpill
	base2, err := failedSpilledWindow(t, func(p *tableDeltaPublish) {
		s = spillOf(t, 4, bigWindow("x"))
		// The last group written, so every pass before it has already run.
		last := -1
		for b := range spillBuckets {
			if s.written[b] {
				last = b
			}
		}
		if err := os.WriteFile(s.path(last), []byte("not gob"), 0o600); err != nil {
			t.Fatal(err)
		}
		p.fold.Spill = s
	})
	if err == nil {
		t.Fatal("a spill with an unreadable group published")
	}
	if left := snapshotFilesOf(t, base2); len(left) != 0 {
		t.Errorf("a failed table left %v in the new snapshot: half a chain publishes the table as it was at some earlier pair", left)
	}
}

// One group alone over the limit is the changed-rows refusal, as it is for
// the rewrite.
func TestTableDelta_spilledGroupOverTheLimitIsRefused(t *testing.T) {
	base2, err := failedSpilledWindow(t, func(p *tableDeltaPublish) {
		// A limit of 1 with 33 keys in 64 groups: some group holds two.
		s := spillOf(t, 1, bigWindow("x"))
		two := false
		for b := range spillBuckets {
			if g, err := s.load(b); err != nil {
				two = true
			} else if len(g) > 1 {
				two = true
			}
		}
		if !two {
			t.Skip("no group of this fixture holds two keys")
		}
		p.fold.Spill = s
	})
	if !errors.Is(err, ErrTouchedRowBudget) {
		t.Fatalf("err = %v, want the changed-rows refusal", err)
	}
	if left := snapshotFilesOf(t, base2); len(left) != 0 {
		t.Errorf("a refused table left %v in the new snapshot", left)
	}
}

// Every other reason to write the table in full still does, from the spill.
func TestTableDelta_spilledWindowOverAnOldChainIsStillRewritten(t *testing.T) {
	noCompaction(t)
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	at1 := t0.Add(5 * time.Minute)
	cut1, cut2 := &query.BinlogPos{File: "binlog.000009", Pos: 1000}, &query.BinlogPos{File: "binlog.000009", Pos: 2000}
	base1, _, err := deltaWindow(t, root, src, t0, changeMap(upd(1, "v1")), at1, cut1, nil)
	if err != nil {
		t.Fatal(err)
	}
	at2 := t0.Add(tableDeltaMaxAge + time.Hour)
	w := bigWindow("late")
	_, ref1, _ := emitSnapshot(t, src, changeMap(upd(1, "v1")), cut1, at1)
	_, ref2, _ := emitSnapshot(t, ref1, cloneChanges(w), cut2, at2)
	base2, rep, err := deltaWindow(t, root, base1, at1, map[string]*query.ResultRow{}, at2, cut2,
		func(p *tableDeltaPublish) { p.fold.Spill = spillOf(t, 4, cloneChanges(w)) })
	if err != nil {
		t.Fatal(err)
	}
	if rep.TableDelta || !strings.Contains(rep.DeltaCompacted, "old") {
		t.Fatalf("report = TableDelta %v, compacted %q; want the day-old chain rewritten", rep.TableDelta, rep.DeltaCompacted)
	}
	want, got := byID(readSnapshotRows(t, ref2)), byID(readSnapshotRows(t, base2))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state after a rewrite from the spill differs from the full rewrite\n got: %v\nwant: %v", got, want)
	}
	chain, err := baseline.ListTableDelta(context.Background(), base2)
	if err != nil || chain == nil || len(chain.Files) != 1 || chain.Files[0].Seq != 0 {
		t.Fatalf("chain after the rewrite = %+v, %v", chain, err)
	}
}
