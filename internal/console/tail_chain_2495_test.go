package console

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// #2495: the keeper writes a table's tail as a chain, the whole tail and
// then the keys changed in each cycle, so a cycle costs what changed, not the
// whole tail. A statement reads every file of the chain it was handed.
func TestTailKeeper_chain(t *testing.T) {
	ctx := context.Background()
	orders := keeperTable("orders")
	ko := tailKey("shop", "orders")
	clock := time.Date(2026, 10, 14, 13, 0, 0, 0, time.UTC)
	newKeeper := func(f *fakeIndex, keys int) *tailKeeper {
		k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: keys}, time.Minute, time.Hour)
		k.now = func() time.Time { return clock }
		k.retryMin, k.retryMax = 0, 0
		return k
	}
	refresh := func(t *testing.T, k *tailKeeper, tables ...views.BaselineTable) map[string]tailView {
		t.Helper()
		return settled(t, ctx, k, tables...)
	}
	fake := func(t *testing.T, k *tailKeeper) *fakeTail {
		t.Helper()
		e := k.tables[ko]
		if e == nil || e.tail == nil {
			t.Fatal("no tail")
		}
		return e.tail.(*fakeTail)
	}

	t.Run("a cycle writes only the keys that changed", func(t *testing.T) {
		f := &fakeIndex{}
		for i := range 50 {
			f.add("orders", fmt.Sprint(i))
		}
		k := newKeeper(f, 1000)
		first := refresh(t, k, orders)
		if fileKeys(t, first[ko].Paths) != 50 || len(first[ko].Paths) != 1 {
			t.Fatalf("first version %v", first[ko].Paths)
		}
		f.add("orders", "7")
		f.add("orders", "50")
		got := refresh(t, k, orders)
		w := fake(t, k).writes
		if w[len(w)-1] != 2 {
			t.Errorf("the cycle wrote %d rows, want 2 (writes %v)", w[len(w)-1], w)
		}
		if len(got[ko].Paths) != 2 || got[ko].Paths[0] != first[ko].Paths[0] || fileKeys(t, got[ko].Paths) != 51 {
			t.Errorf("chain %v holds %d keys, want the first file and one more, 51 keys", got[ko].Paths, fileKeys(t, got[ko].Paths))
		}
		// Nothing new: no file, the same version.
		if again := refresh(t, k, orders); chain(again[ko]) != chain(got[ko]) {
			t.Errorf("a cycle with nothing changed wrote %v", again[ko].Paths)
		}
	})

	t.Run("past tailMaxSegments files the next write is whole", func(t *testing.T) {
		f := &fakeIndex{}
		for i := range 100 {
			f.add("orders", fmt.Sprint(i))
		}
		k := newKeeper(f, 1000)
		refresh(t, k, orders)
		var got map[string]tailView
		for i := range tailMaxSegments {
			f.add("orders", fmt.Sprint(i))
			got = refresh(t, k, orders)
		}
		if n := len(got[ko].Paths); n != 1 {
			t.Fatalf("a chain of %d files after %d cycles", n, tailMaxSegments)
		}
		w := fake(t, k).writes
		if w[len(w)-1] != 100 || fileKeys(t, got[ko].Paths) != 100 {
			t.Errorf("the merge wrote %d rows (writes %v), want the whole tail of 100", w[len(w)-1], w)
		}
		for range 3 {
			f.add("orders", "1")
			got = refresh(t, k, orders)
		}
		if len(got[ko].Paths) != 4 {
			t.Errorf("after the merge, deltas again: chain of %d", len(got[ko].Paths))
		}
		// The merged chain's files go once no statement can read them.
		clock = clock.Add(2 * time.Minute)
		f.add("orders", "2")
		got = refresh(t, k, orders)
		if names, _ := filepath.Glob(filepath.Join(k.dir, "*")); len(names) != len(got[ko].Paths) {
			t.Errorf("%d files on disk for a chain of %d: %v", len(names), len(got[ko].Paths), names)
		}
	})

	t.Run("a chain holding more than tailMergeRatio times the keys is merged", func(t *testing.T) {
		f := &fakeIndex{}
		for i := range 3 {
			f.add("orders", fmt.Sprint(i))
		}
		k := newKeeper(f, 1000)
		refresh(t, k, orders) // 3 rows
		var got map[string]tailView
		for range 3 {
			f.add("orders", "0")
			got = refresh(t, k, orders) // 4, 5, 6 rows: not past 2 x 3
		}
		if len(got[ko].Paths) != 4 {
			t.Fatalf("a chain of %d before the ratio", len(got[ko].Paths))
		}
		f.add("orders", "0")
		refresh(t, k, orders) // 7 rows: past it, so the next write is whole
		f.add("orders", "1")
		got = refresh(t, k, orders)
		if len(got[ko].Paths) != 1 || fileKeys(t, got[ko].Paths) != 3 {
			t.Errorf("chain %v, want one whole file of 3 keys", got[ko].Paths)
		}
	})

	t.Run("a delta that fails leaves the chain before it, and the next one carries its keys", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f, 1000)
		before := refresh(t, k, orders)
		ft := fake(t, k)
		failing := errors.New("disk full")
		ft.writeErr = &failing
		f.add("orders", "2")
		got := refresh(t, k, orders)
		if chain(got[ko]) != chain(before[ko]) {
			t.Fatalf("a failed write served %v", got[ko].Paths)
		}
		ft.writeErr = nil
		f.add("orders", "3")
		got = refresh(t, k, orders)
		if len(got[ko].Paths) != 2 || fileKeys(t, got[ko].Paths) != 3 {
			t.Errorf("chain %v holds %d keys, want 2 files and keys 1, 2, 3", got[ko].Paths, fileKeys(t, got[ko].Paths))
		}
		if w := ft.writes; w[len(w)-1] != 2 {
			t.Errorf("the next delta wrote %d rows, want 2", w[len(w)-1])
		}
	})

	t.Run("a prune, dropping keys or none, starts a new chain", func(t *testing.T) {
		for _, cut := range []uint64{1, 2} { // 1 drops nothing, 2 drops key 2
			f := &fakeIndex{anchors: true, cuts: map[string]uint64{}}
			f.add("customers", "9")
			f.add("orders", "2")
			k := newKeeper(f, 1000)
			refresh(t, k, orders)
			f.add("orders", "3")
			before := refresh(t, k, orders)
			if len(before[ko].Paths) != 2 {
				t.Fatalf("cut %d: chain %v", cut, before[ko].Paths)
			}
			f.add("customers", "9")
			f.cuts[ko] = cut
			got := refresh(t, k, newerCut(orders))
			if fake(t, k).prunes != 1 {
				t.Fatalf("cut %d: not pruned (%d)", cut, fake(t, k).prunes)
			}
			if len(got[ko].Paths) != 1 || slices.Contains(before[ko].Paths, got[ko].Paths[0]) {
				t.Errorf("cut %d: after the prune %v, over %v", cut, got[ko].Paths, before[ko].Paths)
			}
			if want := 3 - int(cut); fileKeys(t, got[ko].Paths) != want {
				t.Errorf("cut %d: %d keys, want %d", cut, fileKeys(t, got[ko].Paths), want)
			}
			if k.tables[ko].fileCut != tailCutOf(newerCut(orders)) {
				t.Errorf("cut %d: the chain is not marked as written over the newer cut", cut)
			}
		}
	})

	t.Run("the version a statement was handed is not changed by the next one", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f, 1000)
		refresh(t, k, orders)
		var held []map[string]tailView
		var want []string
		for i := range 5 {
			f.add("orders", fmt.Sprint(i+2))
			v := refresh(t, k, orders)
			held = append(held, v)
			want = append(want, chain(v[ko]))
		}
		for i, v := range held {
			if chain(v[ko]) != want[i] {
				t.Errorf("version %d changed after it was handed out: %v", i, v[ko].Paths)
			}
			if err := statChain(t, v[ko], false); err != nil {
				t.Errorf("version %d lost a file: %v", i, err)
			}
		}
	})
}
