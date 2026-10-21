package console

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// #2483 slice 2: a statement never waits for a tail to be built. A table
// that needs one has it built in the background, one build per table, and
// reads without it (or with the old tail, where that is allowed) until it is
// in. Every case below holds a build at the fake's gate, so what a statement
// sees while it runs is deterministic, and awaits every build it started
// (settle) before it ends.
func TestTailKeeper_buildsInTheBackground(t *testing.T) {
	ctx := context.Background()
	orders, customers := keeperTable("orders"), keeperTable("customers")
	ko, kc := tailKey("shop", "orders"), tailKey("shop", "customers")
	clock := time.Date(2026, 10, 14, 13, 0, 0, 0, time.UTC)
	newKeeper := func(f *fakeIndex) *tailKeeper {
		k := newTailKeeper(f, t.TempDir(), 3, time.Minute, time.Hour)
		k.now = func() time.Time { return clock }
		k.retryMin, k.retryMax = 0, 0
		return k
	}
	// statement is one statement, which must not wait for any build.
	statement := func(t *testing.T, k *tailKeeper, tables ...views.BaselineTable) map[string]tailView {
		t.Helper()
		got, err := k.refresh(ctx, tables)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	// gated holds the builds of key at the fake's gate; entered says when
	// one reached it.
	gated := func(f *fakeIndex, key string, deaf bool) (release func()) {
		gate := make(chan struct{})
		f.fillGate = map[string]chan struct{}{key: gate}
		f.fillEntered = make(chan string, 8)
		f.fillDeaf = deaf
		return func() { close(gate) }
	}
	keys := func(k *tailKeeper, key string) string {
		var out []string
		for k := range k.tables[key].tail.(*fakeTail).keys {
			out = append(out, k)
		}
		slices.Sort(out)
		return strings.Join(out, ",")
	}

	t.Run("a build in flight: the statement goes on without it, and the next starts no other", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		release := gated(f, ko, false)
		if got := statement(t, k, orders); len(got) != 0 {
			t.Fatalf("a tail before its build is in: %+v", got)
		}
		<-f.fillEntered
		if got := statement(t, k, orders); len(got) != 0 {
			t.Fatalf("a tail before its build is in: %+v", got)
		}
		if len(f.fillEntered) != 0 {
			t.Fatal("a second build of the same table while one runs")
		}
		release()
		k.settle()
		if v, ok := statement(t, k, orders)[ko]; !ok || fileKeys(t, v.Path) != 1 || !v.Through.Equal(f.at(1)) {
			t.Fatalf("the finished build is not the next statement's: %+v %v", v, ok)
		}
		if f.fills != 1 {
			t.Errorf("%d fills, want one", f.fills)
		}
	})

	t.Run("the cursor moved during the build: it catches up before it is in", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		release := gated(f, ko, false)
		statement(t, k, orders) // the build reads up to event 1
		<-f.fillEntered
		f.add("orders", "2")
		f.add("customers", "9")
		f.add("orders", "3")
		statement(t, k, orders) // the cursor moves to event 4; the build still runs
		if k.mark.ID != 4 {
			t.Fatalf("setup: cursor at %d", k.mark.ID)
		}
		release()
		k.settle()
		if got := keys(k, ko); got != "1,2,3" {
			t.Fatalf("installed with %s, want 1,2,3: the changes the cursor passed during the build are missing", got)
		}
		v := statement(t, k, orders)[ko]
		if fileKeys(t, v.Path) != 3 || !v.Through.Equal(f.at(4)) {
			t.Fatalf("%+v (%d keys)", v, fileKeys(t, v.Path))
		}
		// And it advances from there like any tail.
		f.add("orders", "2")
		if after := statement(t, k, orders)[ko]; after.Path == v.Path || fileKeys(t, after.Path) != 3 || !after.Through.Equal(f.at(5)) {
			t.Fatalf("after: %+v", after)
		}
	})

	t.Run("the catch-up keeps the numbering check of advance", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		release := gated(f, ko, false)
		statement(t, k, orders)
		<-f.fillEntered
		// Event 2 is under a numbering that started again, event 3 back
		// under the cursor's: the newest is after the cursor, and no tail
		// advances to read event 2. Only the build's own catch-up does.
		f.add("orders", "2")
		f.events[1].BinlogFile = "binlog.000001"
		f.add("customers", "9")
		statement(t, k, orders)
		if k.mark.ID != 3 || len(k.tables) != 0 {
			t.Fatalf("setup: cursor at %d, %d tables", k.mark.ID, len(k.tables))
		}
		release()
		k.settle()
		if e := k.tables[ko]; e != nil {
			t.Fatalf("installed over a numbering that started again: %+v", e)
		}
		if b := k.builds[ko]; b == nil || b.failures != 1 || b.running {
			t.Fatalf("not recorded as a failed build: %+v", b)
		}
	})

	t.Run("over the budget while it catches up: refused for that cut, as at a first read", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		release := gated(f, ko, false)
		statement(t, k, orders)
		<-f.fillEntered
		for _, key := range []string{"2", "3", "4"} {
			f.add("orders", key)
		}
		statement(t, k, orders)
		release()
		k.settle()
		if e := k.tables[ko]; e == nil || !strings.Contains(e.refused, "more keys changed") || e.frozen {
			t.Fatalf("%+v", e)
		}
		if got := statement(t, k, orders); len(got) != 0 {
			t.Fatalf("%+v", got)
		}
	})

	t.Run("the cut moved during the build: it is ended, never installed, and the newer one built after", func(t *testing.T) {
		for _, deaf := range []bool{false, true} {
			f := &fakeIndex{}
			f.add("orders", "1")
			k := newKeeper(f)
			release := gated(f, ko, deaf)
			statement(t, k, orders)
			<-f.fillEntered
			if got := statement(t, k, newerCut(orders)); len(got) != 0 {
				t.Fatalf("deaf=%v: %+v", deaf, got)
			}
			if len(f.fillEntered) != 0 {
				t.Fatalf("deaf=%v: the newer cut's build started while the older one still ran", deaf)
			}
			release()
			k.settle()
			if e := k.tables[ko]; e != nil {
				t.Fatalf("deaf=%v: a build for a cut no statement reads was installed: %+v", deaf, e)
			}
			statement(t, k, newerCut(orders))
			k.settle()
			if e := k.tables[ko]; e == nil || e.cut != tailCutOf(newerCut(orders)) {
				t.Fatalf("deaf=%v: not built over the newer cut: %+v", deaf, e)
			}
		}
	})

	t.Run("the keeper was replaced during the build: nothing is installed, nothing more starts", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		release := gated(f, ko, true)
		statement(t, k, orders)
		<-f.fillEntered
		k.close()
		release()
		k.settle()
		if len(k.tables) != 0 || len(k.builds) != 0 {
			t.Fatalf("a closed keeper got %d tables, %d builds", len(k.tables), len(k.builds))
		}
		statement(t, k, orders)
		k.settle()
		if len(k.builds) != 0 || len(f.fillEntered) != 0 {
			t.Fatal("a closed keeper started a build")
		}
	})

	t.Run("every tail forgotten during the build (the index was rebuilt): not installed", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("orders", "2")
		k := newKeeper(f)
		release := gated(f, ko, true)
		statement(t, k, orders)
		<-f.fillEntered
		f.events[1].StartPos = 900 // the mark's event is no longer where it was read
		statement(t, k, orders)
		release()
		k.settle()
		if e := k.tables[ko]; e != nil {
			t.Fatalf("a build from before the index was rebuilt was installed: %+v", e)
		}
	})

	t.Run("a definition that changed during the build: the refusal stands, the build is not installed", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		release := gated(f, ko, true)
		statement(t, k, orders)
		<-f.fillEntered
		f.changed = map[string]string{ko: "ALTER TABLE"}
		statement(t, k, orders)
		release()
		k.settle()
		if e := k.tables[ko]; e == nil || e.refused != "ALTER TABLE" {
			t.Fatalf("%+v", e)
		}
	})

	t.Run("a build that fails: never refused, tried again after a wait that doubles", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		k.retryMin, k.retryMax = time.Minute, 3*time.Minute
		armed := func() { f.fillErr = map[string]error{ko: errors.New("connection reset")} }
		armed()
		for i, want := range []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute, 3 * time.Minute} {
			statement(t, k, orders) // fails, and takes the armed error
			k.settle()
			b := k.builds[ko]
			if b == nil || b.running || b.retryAt.Sub(clock) != want {
				t.Fatalf("failure %d: %+v, want a retry in %v", i+1, b, want)
			}
			if e := k.tables[ko]; e != nil {
				t.Fatalf("failure %d left an entry (refused %q)", i+1, e.refused)
			}
			armed()
			statement(t, k, orders) // before the wait is over: no read
			k.settle()
			if f.fillErr[ko] == nil {
				t.Fatalf("failure %d: tried again before its wait was over", i+1)
			}
			clock = clock.Add(want)
		}
		delete(f.fillErr, ko)
		statement(t, k, orders)
		k.settle()
		if v, ok := statement(t, k, orders)[ko]; !ok || fileKeys(t, v.Path) != 1 {
			t.Fatalf("not built once the index answered: %+v", v)
		}
		if _, ok := k.builds[ko]; ok {
			t.Error("a build that went in is still recorded")
		}
	})

	t.Run("a build that runs out of its own time: tried again, never refused", func(t *testing.T) {
		f := &fakeIndex{fillWait: map[string]bool{ko: true}}
		f.add("orders", "1")
		k := newKeeper(f)
		k.buildTimeout = 10 * time.Millisecond
		statement(t, k, orders)
		k.settle()
		if b := k.builds[ko]; b == nil || b.failures != 1 || k.tables[ko] != nil {
			t.Fatalf("%+v %+v", b, k.tables[ko])
		}
		statement(t, k, orders)
		k.settle()
		if _, ok := statement(t, k, orders)[ko]; !ok {
			t.Fatal("not built again")
		}
	})

	t.Run("a build that panics fails like an error, and is tried again", func(t *testing.T) {
		f := &fakeIndex{fillPanic: map[string]bool{ko: true}}
		f.add("orders", "1")
		k := newKeeper(f)
		statement(t, k, orders)
		k.settle()
		if b := k.builds[ko]; b == nil || b.running || b.failures != 1 {
			t.Fatalf("the slot of a build that panicked: %+v", b)
		}
		statement(t, k, orders)
		k.settle()
		if _, ok := statement(t, k, orders)[ko]; !ok {
			t.Fatal("not built again after the panic")
		}
	})

	t.Run("the old tail stands while the newer cut's tail is built, where it may", func(t *testing.T) {
		f := &fakeIndex{anchors: true}
		f.add("orders", "1")
		k := newKeeper(f)
		statement(t, k, orders)
		k.settle()
		before := statement(t, k, orders)[ko]
		// The newer cut's anchor did not move: no prune, a build.
		release := gated(f, ko, false)
		if got := statement(t, k, newerCut(orders))[ko]; got.Path != before.Path {
			t.Fatalf("the old tail did not stand during the build: %+v", got)
		}
		<-f.fillEntered
		release()
		k.settle()
		if e := k.tables[ko]; e.cut != tailCutOf(newerCut(orders)) {
			t.Fatalf("the build was not installed over the old tail: %+v", e)
		}
	})

	t.Run("a frozen tail and a newer cut: no tail while it is rebuilt, the rebuilt one after", func(t *testing.T) {
		f := &fakeIndex{anchors: true}
		f.add("orders", "1")
		k := newKeeper(f)
		statement(t, k, orders)
		k.settle()
		statement(t, k, orders)
		for _, key := range []string{"2", "3", "4"} {
			f.add("orders", key)
		}
		statement(t, k, orders)
		if !k.tables[ko].frozen {
			t.Fatal("setup: not frozen")
		}
		f.cuts = map[string]uint64{ko: 4}
		release := gated(f, ko, false)
		if got := statement(t, k, newerCut(orders)); len(got) != 0 {
			t.Fatalf("a frozen tail over a newer cut: %+v", got)
		}
		<-f.fillEntered
		release()
		k.settle()
		if v, ok := statement(t, k, newerCut(orders))[ko]; !ok || v.Path != "" || !v.Through.Equal(f.at(4)) {
			t.Fatalf("%+v %v", v, ok)
		}
	})

	t.Run("two tables, one ready and one building: the ready one is served, the other is not", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("customers", "9")
		k := newKeeper(f)
		statement(t, k, orders)
		k.settle()
		release := gated(f, kc, false)
		got := statement(t, k, orders, customers)
		if _, ok := got[ko]; !ok {
			t.Error("the ready table is not served")
		}
		if _, ok := got[kc]; ok {
			t.Error("the building table is served")
		}
		<-f.fillEntered
		release()
		k.settle()
		if got := statement(t, k, orders, customers); len(got) != 2 || !got[ko].Through.Equal(got[kc].Through) {
			t.Fatalf("%+v", got)
		}
	})

	t.Run("a failed build no statement asks for again is forgotten", func(t *testing.T) {
		f := &fakeIndex{fillErr: map[string]error{ko: errors.New("gone")}}
		f.add("orders", "1")
		k := newKeeper(f)
		k.retryMin, k.retryMax = time.Hour, time.Hour
		statement(t, k, orders)
		k.settle()
		if k.builds[ko] == nil {
			t.Fatal("setup: no failed build")
		}
		clock = clock.Add(2 * time.Hour)
		k.tidy()
		if _, ok := k.builds[ko]; ok {
			t.Error("an idle failed build is kept")
		}
	})
}
