package console

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/views"
)

// A refresh writes its snapshot into a directory of its own and links the
// base of each table into it: the base keeps its file and changes its name at
// every refresh. The tails' prune to a newer cut (#2483) asked for the same
// name, so in production it never ran: every refresh rebuilt every tail from
// the index, from the new cut, and a statement waited for it. A bench saw a
// stall of 1 to 2.3 s of every copy answer after each refresh at 50 clients,
// and past fresh mode's 3 s wait (heavy reads sent to MySQL, `copy_too_old`)
// at 200 clients and on the first statement after a refresh no statement read.

// refreshedCut is t as the snapshot in directory dir lists it: its base under
// that directory's name, the chain one pair longer.
func refreshedCut(t views.BaselineTable, dir string) views.BaselineTable {
	t.Path = "/snap/" + dir + "/shop/" + t.Table + ".parquet"
	t.DeltaFiles = append(slices.Clone(t.DeltaFiles), baseline.TableDeltaFile{Upserts: "/snap/" + dir + "/shop/" + t.Table + ".000001.upserts"})
	return t
}

func TestTailKeeper_aRefreshLinksTheBaseUnderANewName(t *testing.T) {
	ctx := context.Background()
	orders := keeperTable("orders") // /snap/1/shop/orders.parquet
	ko := tailKey("shop", "orders")
	// started is a keeper whose tail of orders holds keys 1..4 over a cut at
	// 0, built against the file "inode 7", and an index whose next cut of
	// orders stops at event 2, the cursor at event 5.
	started := func(t *testing.T, bases map[string]string) (*fakeIndex, *tailKeeper) {
		t.Helper()
		f := &fakeIndex{anchors: true, cuts: map[string]uint64{}, bases: bases}
		for _, key := range []string{"1", "2", "3", "4"} {
			f.add("orders", key)
		}
		k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 10}, time.Minute, time.Hour)
		k.retryMin, k.retryMax = 0, 0
		if v := settled(t, ctx, k, orders)[ko]; fileKeys(t, v.Paths) != 4 || f.fills != 1 {
			t.Fatalf("setup: %+v, %d fills", v, f.fills)
		}
		f.add("customers", "9")
		f.mu.Lock()
		f.cuts[ko] = 2
		f.mu.Unlock()
		return f, k
	}
	keys := func(k *tailKeeper) string {
		var out []string
		for key := range k.tables[ko].tail.(*fakeTail).keys {
			out = append(out, key)
		}
		slices.Sort(out)
		return strings.Join(out, ",")
	}
	linked := func() map[string]string {
		return map[string]string{
			"/snap/1/shop/orders.parquet": "inode 7",
			"/snap/2/shop/orders.parquet": "inode 7",
			"/snap/3/shop/orders.parquet": "inode 7",
		}
	}

	t.Run("the same file under the new snapshot's name is pruned to, with no read of the index and no build", func(t *testing.T) {
		f, k := started(t, linked())
		f.fillGate = map[string]chan struct{}{ko: make(chan struct{})} // a build would hang
		second := refreshedCut(orders, "2")
		got, err := k.refresh(ctx, []views.BaselineTable{second})
		if err != nil {
			t.Fatal(err)
		}
		v, ok := got[ko]
		if !ok || fileKeys(t, v.Paths) != 2 || keys(k) != "3,4" || f.fills != 1 || len(k.builds) != 0 {
			t.Fatalf("view %+v %v, kept %s, %d fills, builds %v: the refresh rebuilt the tail", v, ok, keys(k), f.fills, k.builds)
		}
		if !v.Through.Equal(f.at(5)) {
			t.Errorf("through %v, want the cursor's %v", v.Through, f.at(5))
		}
		if e := k.tables[ko]; e.cut != tailCutOf(second) || e.table.Path != second.Path || e.base != "inode 7" {
			t.Errorf("entry not moved to the newer cut: cut %q path %q base %q", e.cut, e.table.Path, e.base)
		}
		// The next refresh, another name again, the same file.
		f.mu.Lock()
		f.cuts[ko] = 3
		f.mu.Unlock()
		third := refreshedCut(second, "3")
		got, err = k.refresh(ctx, []views.BaselineTable{third})
		if err != nil || keys(k) != "4" || fileKeys(t, got[ko].Paths) != 1 || f.fills != 1 || len(k.builds) != 0 {
			t.Fatalf("third snapshot: %+v %v, kept %s, %d fills", got[ko], err, keys(k), f.fills)
		}
		close(f.fillGate[ko])
		k.close()
		k.settle()
	})

	t.Run("the same keys as a rebuild from the new cut", func(t *testing.T) {
		_, pruned := started(t, linked())
		settled(t, ctx, pruned, refreshedCut(orders, "2"))
		f, rebuilt := started(t, map[string]string{"/snap/1/shop/orders.parquet": "inode 7", "/snap/2/shop/orders.parquet": "inode 8"})
		settled(t, ctx, rebuilt, refreshedCut(orders, "2"))
		if f.fills != 2 {
			t.Fatalf("the reference was not rebuilt: %d fills", f.fills)
		}
		if keys(pruned) != keys(rebuilt) {
			t.Errorf("pruned %s, rebuilt %s", keys(pruned), keys(rebuilt))
		}
	})

	rebuilds := func(t *testing.T, f *fakeIndex, k *tailKeeper, table views.BaselineTable) {
		t.Helper()
		settled(t, ctx, k, table)
		if f.fills != 2 {
			t.Errorf("filled %d times, want a rebuild", f.fills)
		}
		if e := k.tables[ko]; e != nil && e.tail != nil && e.tail.(*fakeTail).prunes != 0 {
			t.Errorf("pruned too")
		}
	}
	for name, c := range map[string]struct {
		bases map[string]string
		edit  func(f *fakeIndex, k *tailKeeper)
	}{
		"the base was written again (a compaction, a copy where the link failed)": {
			bases: map[string]string{"/snap/1/shop/orders.parquet": "inode 7", "/snap/2/shop/orders.parquet": "inode 8"},
		},
		"the new name cannot be read": {
			bases: map[string]string{"/snap/1/shop/orders.parquet": "inode 7"},
		},
		"the file the tail was built against was not known": {
			bases: map[string]string{},
		},
		"a frozen tail": {
			bases: linked(),
			edit:  func(_ *fakeIndex, k *tailKeeper) { k.tables[ko].frozen = true },
		},
		"the new cut is past the cursor": {
			bases: linked(),
			edit: func(f *fakeIndex, _ *tailKeeper) {
				f.mu.Lock()
				f.cutAt = map[string]query.BinlogPos{ko: {File: "binlog.000003", Pos: 501}}
				f.cutLast = map[string]uint64{ko: 0}
				f.mu.Unlock()
			},
		},
	} {
		t.Run("rebuilds: "+name, func(t *testing.T) {
			f, k := started(t, c.bases)
			if c.edit != nil {
				c.edit(f, k)
			}
			rebuilds(t, f, k, refreshedCut(orders, "2"))
		})
	}

	t.Run("rebuilds: the same name, another file (written again under it)", func(t *testing.T) {
		f, k := started(t, linked())
		f.mu.Lock()
		f.bases["/snap/1/shop/orders.parquet"] = "inode 9"
		f.mu.Unlock()
		rebuilds(t, f, k, newerCut(orders))
	})

	t.Run("a tail rebuilt against a new file prunes over the next link of that file", func(t *testing.T) {
		f, k := started(t, map[string]string{
			"/snap/1/shop/orders.parquet": "inode 7",
			"/snap/2/shop/orders.parquet": "inode 8",
			"/snap/3/shop/orders.parquet": "inode 8",
		})
		second := refreshedCut(orders, "2")
		settled(t, ctx, k, second)
		if f.fills != 2 || k.tables[ko].base != "inode 8" {
			t.Fatalf("setup: %d fills, base %q", f.fills, k.tables[ko].base)
		}
		f.mu.Lock()
		f.cuts[ko] = 3
		f.mu.Unlock()
		if got := settled(t, ctx, k, refreshedCut(second, "3"))[ko]; f.fills != 2 || keys(k) != "4" || fileKeys(t, got.Paths) != 1 {
			t.Fatalf("kept %s, %d fills: the new file's link was rebuilt", keys(k), f.fills)
		}
	})

	t.Run("a file replaced while the build read it leaves the identity unknown", func(t *testing.T) {
		f := &fakeIndex{anchors: true, cuts: map[string]uint64{}, bases: linked(),
			startBases: map[string]string{"/snap/1/shop/orders.parquet": "inode 9"}}
		f.add("orders", "1")
		k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 10}, time.Minute, time.Hour)
		settled(t, ctx, k, orders)
		if e := k.tables[ko]; e == nil || e.base != "" {
			t.Fatalf("base %q: the identity of a file other than the one the checks read", e.base)
		}
	})

	t.Run("the old tail does not stand for a base written again while it is rebuilt", func(t *testing.T) {
		f, k := started(t, map[string]string{"/snap/1/shop/orders.parquet": "inode 7", "/snap/2/shop/orders.parquet": "inode 8"})
		f.fillGate = map[string]chan struct{}{ko: make(chan struct{})}
		got, err := k.refresh(ctx, []views.BaselineTable{refreshedCut(orders, "2")})
		if _, ok := got[ko]; ok || err != nil {
			t.Fatalf("a tail of another file laid over the new cut: %+v %v", got[ko], err)
		}
		close(f.fillGate[ko])
		k.close()
		k.settle()
	})
}

// baseFileID names a file, not a name: a hard link is the same file, a copy
// of the same bytes or a file written again under the same name is not, and
// what cannot be read is not known.
func TestBaseFileID(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	a := write("a.parquet", "rows")
	id := baseFileID(a)
	if id == "" {
		t.Fatal("no identity for a file that exists")
	}
	if baseFileID(a) != id {
		t.Fatal("two reads of one file differ")
	}
	link := filepath.Join(dir, "link.parquet")
	if err := os.Link(a, link); err != nil {
		t.Fatal(err)
	}
	if baseFileID(link) != id {
		t.Errorf("a hard link is another file: %q vs %q", baseFileID(link), id)
	}
	if c := write("copy.parquet", "rows"); baseFileID(c) == id {
		t.Error("a copy of the same bytes is the same file")
	}
	// Written again under the same name, by rename as a snapshot's writer does.
	next := write("next.tmp", "rows")
	if err := os.Rename(next, a); err != nil {
		t.Fatal(err)
	}
	if got := baseFileID(a); got == id || got == "" {
		t.Errorf("a file written again under the same name: %q, was %q", got, id)
	}
	if baseFileID(link) != id {
		t.Error("the old file under its other name changed")
	}
	for name, p := range map[string]string{
		"missing":   filepath.Join(dir, "gone.parquet"),
		"directory": dir,
		"empty":     "",
		"s3":        "s3://bucket/snap/shop/orders.parquet",
	} {
		if got := baseFileID(p); got != "" {
			t.Errorf("%s: %q, want unknown", name, got)
		}
	}
}

// A statement waiting for a table's tail (statementTailsFresh) wakes the
// follower once; the tail is ready only when its build is installed, and
// the update that publishes it would then start after the follower's pause,
// up to a second of a 3 s wait. The build's install wakes it.
func TestABuiltTailWakesTheFollower(t *testing.T) {
	ctx := context.Background()
	ko := tailKey("shop", "orders")
	f := &fakeIndex{fillGate: map[string]chan struct{}{ko: make(chan struct{})}}
	f.add("orders", "1")
	k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	defer func() { k.close(); k.settle() }()
	s := &Server{}
	orders := []views.BaselineTable{keeperTable("orders")}
	type res struct {
		ok      bool
		missing string
	}
	done := make(chan res, 1)
	go func() {
		_, _, _, ok, missing := s.statementTailsFresh(ctx, k, orders, orders, 10*time.Second)
		done <- res{ok, missing}
	}()
	// The test is the follower. The statement's one wake:
	select {
	case <-k.pub.wake:
	case r := <-done:
		t.Fatalf("the statement ended before its tail was built: %+v", r)
	case <-time.After(10 * time.Second):
		t.Fatal("the statement did not wake the follower")
	}
	k.followOnce(ctx, time.Minute) // the build is in flight: it says so
	select {
	case <-k.pub.wake:
		t.Fatal("a wake before the build was installed")
	default:
	}
	close(f.fillGate[ko])
	k.settle()
	select {
	case <-k.pub.wake:
	default:
		t.Fatal("the build was installed and the follower was not woken: the statement waits out its pause")
	}
	k.followOnce(ctx, time.Minute)
	select {
	case r := <-done:
		if !r.ok || r.missing != "" {
			t.Fatalf("got %+v, want the tail", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the statement never got its tail")
	}
}
