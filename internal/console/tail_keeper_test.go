package console

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// fakeIndex is a tailSource over events held in memory, ids in the order
// they were indexed, handed out two at a time.
type fakeIndex struct {
	rowBytes int64 // what each fakeTail counts per key (Bytes)
	// mu guards every field against the keeper's background builds; a test
	// changes a field only while no build reads it (before one starts, or
	// after settle).
	mu     sync.Mutex
	events []query.ResultRow
	// newestErr, sinceErr, changedErr fail those reads; fillErr fails Fill
	// for a table once (a rebuild that times out); startWhy, fillWhy,
	// changed refuse.
	newestErr, sinceErr, changedErr error
	fillErr                         map[string]error
	// startErr fails Start for a table once (its schema snapshot could not
	// be read).
	startErr          map[string]error
	startWhy, fillWhy map[string]string
	changed           map[string]string
	// cuts is the event id each table's published cut stops at.
	cuts map[string]uint64
	// fills counts Fill calls; pages the pages handed out; sinceTables the
	// tables the last Since was asked for.
	fills, pages int
	sinceTables  []string
	writeErr     error
	// sinceFails fails Since after handing out that many pages; sinceFrom
	// is the cursor the last Since started after. fillWait blocks Fill for
	// a table until its context ends (a read that runs out of time).
	sinceFails int
	sinceFrom  uint64
	fillWait   map[string]bool
	// anchors turns on Cut (#2483), off by default so the cases above keep
	// rebuilding: a table's cut then stands where the read of its window
	// starts (Fill's id floor, cuts), in binlog.000003 unless cutFile says
	// otherwise; cutAt moves a table's anchor, cutWhy refuses it.
	anchors bool
	cutFile string
	cutAt   map[string]query.BinlogPos
	cutWhy  map[string]string
	cutLast map[string]uint64
	// fillGate holds a table's Fill (a build in flight) until the channel is
	// closed or the build's context ends (fillDeaf: only the channel, a read
	// that finishes after it was no longer wanted); fillEntered is told the
	// table when a Fill reaches its gate. fillPanic makes a table's Fill
	// panic once.
	fillGate    map[string]chan struct{}
	fillDeaf    bool
	fillEntered chan string
	fillPanic   map[string]bool
}

const fakePage = 2

func (f *fakeIndex) Newest(ctx context.Context) (tailMark, error) {
	if err := ctx.Err(); err != nil {
		return tailMark{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.newestErr != nil || len(f.events) == 0 {
		return tailMark{}, f.newestErr
	}
	last := f.events[len(f.events)-1]
	return tailMark{ID: last.EventID, Pos: query.BinlogPos{File: last.BinlogFile, Pos: last.StartPos}, At: last.EventTimestamp}, nil
}

func (f *fakeIndex) Holds(_ context.Context, m tailMark) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ev := range f.events {
		if ev.EventID == m.ID {
			return ev.BinlogFile == m.Pos.File && ev.StartPos == m.Pos.Pos, nil
		}
	}
	return false, nil
}

func (f *fakeIndex) Start(_ context.Context, t views.BaselineTable) (tailTable, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.startErr[tailKey(t.Schema, t.Table)]; err != nil {
		delete(f.startErr, tailKey(t.Schema, t.Table))
		return nil, "", err
	}
	if why := f.startWhy[tailKey(t.Schema, t.Table)]; why != "" {
		return nil, why, nil
	}
	return &fakeTail{writeErr: &f.writeErr, rowBytes: f.rowBytes}, "", nil
}

func (f *fakeIndex) hand(evs []query.ResultRow, page func([]query.ResultRow) error) error {
	for len(evs) > 0 {
		n := min(fakePage, len(evs))
		f.pages++
		if err := page(evs[:n]); err != nil {
			return err
		}
		evs = evs[n:]
	}
	return nil
}

func (f *fakeIndex) Fill(ctx context.Context, t views.BaselineTable, upTo uint64, page func([]query.ResultRow) error) (string, error) {
	key := tailKey(t.Schema, t.Table)
	f.mu.Lock()
	gate, entered, deaf := f.fillGate[key], f.fillEntered, f.fillDeaf
	f.mu.Unlock()
	if gate != nil {
		if entered != nil {
			entered <- key
		}
		if deaf {
			<-gate
		} else {
			select {
			case <-gate:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fillPanic[key] {
		delete(f.fillPanic, key)
		panic("a fold that panics")
	}
	if f.fillWait[key] {
		delete(f.fillWait, key) // once: a retry must not hang the test
		f.mu.Unlock()
		<-ctx.Done()
		f.mu.Lock()
		return "", ctx.Err()
	}
	if err := f.fillErr[key]; err != nil {
		delete(f.fillErr, key)
		return "", err
	}
	if why := f.fillWhy[key]; why != "" {
		return why, nil
	}
	f.fills++
	var evs []query.ResultRow
	for _, ev := range f.events {
		if tailKey(ev.SchemaName, ev.TableName) == key && ev.EventID > f.cuts[key] && ev.EventID <= upTo {
			evs = append(evs, ev)
		}
	}
	return "", f.hand(evs, page)
}

func (f *fakeIndex) Since(_ context.Context, tables []views.BaselineTable, from, upTo uint64, page func([]query.ResultRow) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sinceErr != nil {
		return f.sinceErr
	}
	want := map[string]bool{}
	f.sinceTables, f.sinceFrom = nil, from
	for _, t := range tables {
		want[tailKey(t.Schema, t.Table)] = true
		f.sinceTables = append(f.sinceTables, t.Table)
	}
	var evs []query.ResultRow
	for _, ev := range f.events {
		if want[tailKey(ev.SchemaName, ev.TableName)] && ev.EventID > from && ev.EventID <= upTo {
			evs = append(evs, ev)
		}
	}
	if f.sinceFails > 0 {
		n := min(len(evs), f.sinceFails*fakePage)
		if err := f.hand(evs[:n], page); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}
	return f.hand(evs, page)
}

func (f *fakeIndex) Cut(t views.BaselineTable) (tailFloor, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := tailKey(t.Schema, t.Table)
	switch {
	case !f.anchors:
		return tailFloor{}, "no anchor in this fake"
	case f.cutWhy[key] != "":
		return tailFloor{}, f.cutWhy[key]
	}
	file := f.cutFile
	if file == "" {
		file = "binlog.000003"
	}
	// add puts event id at id*100: the anchor sits just after the cut's last
	// event, so the window starts at the event after it, as Fill's id floor.
	c := tailFloor{anchor: query.BinlogPos{File: file, Pos: f.cuts[key]*100 + 1}, lastEventID: f.cuts[key]}
	if at, ok := f.cutAt[key]; ok {
		c.anchor = at
	}
	if last, ok := f.cutLast[key]; ok {
		c.lastEventID = last
	}
	return c, ""
}

func (f *fakeIndex) Broken(context.Context, []views.BaselineTable) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed, f.changedErr
}

// add indexes one change of shop.<table> to key, a minute after the last,
// further on in binlog.000003.
func (f *fakeIndex) add(table, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := uint64(len(f.events) + 1)
	f.events = append(f.events, query.ResultRow{EventID: id, SchemaName: "shop", TableName: table, PKValues: key,
		BinlogFile: "binlog.000003", StartPos: id * 100,
		EventTimestamp: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC).Add(time.Duration(id) * time.Minute)})
}

func (f *fakeIndex) at(id uint64) time.Time { return f.events[id-1].EventTimestamp }

// fakeTail keeps the last event of each key it was given and writes the
// keys, one per line.
type fakeTail struct {
	keys     map[string]query.ResultRow
	rowBytes int64 // what Bytes counts per key; 0 counts 1
	applyErr error
	writeErr *error
	pruneErr error
	prunes   int
}

func (t *fakeTail) Apply(page []query.ResultRow) error {
	if t.applyErr != nil {
		return t.applyErr
	}
	if t.keys == nil {
		t.keys = map[string]query.ResultRow{}
	}
	for _, ev := range page {
		t.keys[ev.PKValues] = ev
	}
	return nil
}

// PruneThrough is reconstruct.Tail's rule over the fake's single numbering.
func (t *fakeTail) PruneThrough(anchor query.BinlogPos, afterID uint64) (int, error) {
	if t.pruneErr != nil {
		return 0, t.pruneErr
	}
	t.prunes++
	n := 0
	for k, ev := range t.keys {
		if !anchor.AtOrBefore(query.BinlogPos{File: ev.BinlogFile, Pos: ev.StartPos}) || afterID != 0 && ev.EventID <= afterID {
			delete(t.keys, k)
			n++
		}
	}
	return n, nil
}
func (t *fakeTail) Keys() int           { return len(t.keys) }
func (t *fakeTail) Bytes() int64        { return int64(len(t.keys)) * max(1, t.rowBytes) }
func (t *fakeTail) PKColumns() []string { return []string{"id"} }
func (t *fakeTail) WriteFile(path string) (int64, error) {
	if t.writeErr != nil && *t.writeErr != nil {
		return 0, *t.writeErr
	}
	if _, err := os.Lstat(path); err == nil {
		return 0, errors.New("exists")
	}
	var keys []string
	for k := range t.keys {
		keys = append(keys, k)
	}
	return int64(len(keys)), os.WriteFile(path, []byte(strings.Join(keys, "\n")), 0o600)
}

func keeperTable(name string) views.BaselineTable {
	return views.BaselineTable{Schema: "shop", Table: name, Path: "/snap/1/shop/" + name + ".parquet"}
}

func newerCut(t views.BaselineTable) views.BaselineTable {
	t.DeltaFiles = append(t.DeltaFiles, baseline.TableDeltaFile{Upserts: "/snap/2/shop/" + t.Table + ".000000.upserts"})
	return t
}

// settled is a statement over tables after the builds an earlier one started
// are in: a refresh, every build it started run to its end, and the refresh
// the next statement makes, whose builds also end before it returns, so the
// test reads a keeper no goroutine touches.
func settled(t *testing.T, ctx context.Context, k *tailKeeper, tables ...views.BaselineTable) map[string]tailView {
	t.Helper()
	if _, err := k.refresh(ctx, tables); err != nil {
		t.Fatal(err)
	}
	k.settle()
	got, err := k.refresh(ctx, tables)
	k.settle()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func fileKeys(t *testing.T, path string) int {
	t.Helper()
	if path == "" {
		return 0
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		return 0
	}
	return len(strings.Split(string(b), "\n"))
}

func TestTailKeeper(t *testing.T) {
	ctx := context.Background()
	orders, customers := keeperTable("orders"), keeperTable("customers")
	ko, kc := tailKey("shop", "orders"), tailKey("shop", "customers")
	clock := time.Date(2026, 10, 14, 13, 0, 0, 0, time.UTC)
	newKeeper := func(f *fakeIndex) *tailKeeper {
		k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
		k.now = func() time.Time { return clock }
		k.retryMin, k.retryMax = 0, 0
		return k
	}
	refresh := func(t *testing.T, k *tailKeeper, tables ...views.BaselineTable) map[string]tailView {
		t.Helper()
		return settled(t, ctx, k, tables...)
	}

	t.Run("nothing indexed: no tails", func(t *testing.T) {
		if got := refresh(t, newKeeper(&fakeIndex{}), orders); len(got) != 0 {
			t.Fatalf("%v", got)
		}
	})

	t.Run("a Start that cannot read the index is tried again at the next statement", func(t *testing.T) {
		f := &fakeIndex{startErr: map[string]error{ko: errors.New("connection reset")}}
		f.add("orders", "1")
		k := newKeeper(f)
		if got := refresh(t, k, orders); len(got) != 0 {
			t.Fatalf("a tail out of a Start that failed: %v", got)
		}
		if e := k.tables[ko]; e != nil && e.refused != "" {
			t.Fatalf("a failed read is remembered as a refusal (%q): it would stand until the copy's next snapshot", e.refused)
		}
		if v, ok := refresh(t, k, orders)[ko]; !ok || fileKeys(t, v.Path) != 1 {
			t.Fatalf("the next statement has no tail: %+v (present %v)", v, ok)
		}
	})

	t.Run("first read: the table's changes after its cut, through the newest event", func(t *testing.T) {
		f := &fakeIndex{cuts: map[string]uint64{ko: 1}}
		f.add("orders", "1") // at or before the cut
		f.add("orders", "2")
		f.add("customers", "9")
		v := refresh(t, newKeeper(f), orders)[ko]
		if fileKeys(t, v.Path) != 1 || !v.Through.Equal(f.at(3)) || v.PK[0] != "id" {
			t.Fatalf("%+v", v)
		}
	})

	t.Run("later reads fold only the tails' tables after the cursor, into a new file; the old one stays", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		first := refresh(t, k, orders)
		f.add("orders", "2")
		f.add("customers", "5")
		second := refresh(t, k, orders)
		if second[ko].Path == first[ko].Path || fileKeys(t, second[ko].Path) != 2 || !second[ko].Through.Equal(f.at(3)) {
			t.Fatalf("first %+v second %+v", first[ko], second[ko])
		}
		if strings.Join(f.sinceTables, ",") != "orders" {
			t.Errorf("read the events of %v, want only the tails' tables", f.sinceTables)
		}
		if _, err := os.Stat(first[ko].Path); err != nil {
			t.Errorf("the replaced version is gone at once: %v", err)
		}
		if f.fills != 1 {
			t.Errorf("filled %d times, want once: later reads go by the cursor", f.fills)
		}
		third := refresh(t, k, orders)
		if third[ko].Path != second[ko].Path || !third[ko].Through.Equal(second[ko].Through) {
			t.Errorf("nothing new changed the view: %+v -> %+v", second[ko], third[ko])
		}
	})

	t.Run("an event indexed after the newest was read is not folded and does not move the instant", func(t *testing.T) {
		for _, warm := range []bool{false, true} {
			f := &fakeIndex{}
			f.add("orders", "1")
			k := newKeeper(f)
			if warm {
				refresh(t, k, orders) // so the next read goes by Since
			}
			f.add("orders", "2")
			// One statement whose Newest is followed by event 3: its fold
			// (warm) or the build it starts (cold) stops at event 2.
			k.src = &lateIndex{fakeIndex: f}
			if _, err := k.refresh(ctx, []views.BaselineTable{orders}); err != nil {
				t.Fatal(err)
			}
			k.settle()
			e := k.tables[ko]
			if e == nil || e.tail.Keys() != 2 || k.mark.ID != 2 || !k.through.Equal(f.at(2)) {
				t.Fatalf("warm=%v: %+v, mark %d, through %v", warm, e, k.mark.ID, k.through)
			}
		}
	})

	t.Run("a late commit, the newest event with an early time, does not move the instant back", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		before := refresh(t, k, orders)
		f.add("orders", "2")
		f.events[1].EventTimestamp = f.events[0].EventTimestamp.Add(-time.Hour)
		got := refresh(t, k, orders)
		if fileKeys(t, got[ko].Path) != 2 || got[ko].Through.Before(before[ko].Through) {
			t.Fatalf("%+v -> %+v", before[ko], got[ko])
		}
	})

	t.Run("an event under another case of a tail's name refuses that tail", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		refresh(t, k, orders)
		f.add("Orders", "2") // the index's spelling, which the copy does not share
		k.src = &foldingIndex{f}
		if got := refresh(t, k, orders); len(got) != 0 {
			t.Fatalf("still a tail: %+v", got[ko])
		}
	})

	t.Run("an unchanged table has no file and the cursor's instant", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("customers", "1")
		if v, ok := refresh(t, newKeeper(f), orders)[ko]; !ok || v.Path != "" || !v.Through.Equal(f.at(1)) {
			t.Fatalf("%+v %v", v, ok)
		}
	})

	t.Run("a newer published cut rebuilds the tail from it", func(t *testing.T) {
		f := &fakeIndex{cuts: map[string]uint64{}}
		f.add("orders", "1")
		f.add("orders", "2")
		k := newKeeper(f)
		before := refresh(t, k, orders)
		f.cuts[ko] = 1
		after := refresh(t, k, newerCut(orders))
		if fileKeys(t, before[ko].Path) != 2 || fileKeys(t, after[ko].Path) != 1 {
			t.Fatalf("before %+v after %+v", before[ko], after[ko])
		}
	})

	t.Run("a rebuild that cannot read the index keeps the advancing tail it had, and is retried", func(t *testing.T) {
		f := &fakeIndex{fillErr: map[string]error{}, anchors: true, cuts: map[string]uint64{}}
		f.add("orders", "1")
		k := newKeeper(f)
		before := refresh(t, k, orders)
		f.cuts[ko] = 1                                                     // the cut moves forward, to the cursor
		k.tables[ko].tail.(*fakeTail).pruneErr = errors.New("no position") // but cannot be pruned to: a build
		f.fillErr[ko] = errors.New("timeout")
		if got := refresh(t, k, newerCut(orders)); got[ko].Path != before[ko].Path {
			t.Fatalf("the tail it had was dropped: %+v", got[ko])
		}
		if got := refresh(t, k, newerCut(orders)); got[ko].Path == before[ko].Path {
			t.Error("the rebuild was not retried")
		}
	})

	t.Run("a read that runs out of time keeps the pages it folded; the next goes on after them", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		refresh(t, k, orders)
		for _, key := range []string{"2", "2", "2", "2", "2"} {
			f.add("orders", key)
		}
		f.sinceFails = 1 // one page of two events, then out of time
		if _, err := k.refresh(ctx, []views.BaselineTable{orders}); err == nil {
			t.Fatal("a read that ran out of time gave tails")
		}
		f.sinceFails = 0
		got := refresh(t, k, orders)
		if f.sinceFrom != 3 {
			t.Errorf("the next read started after event %d, want 3: the folded page was read again", f.sinceFrom)
		}
		if fileKeys(t, got[ko].Path) != 2 || !got[ko].Through.Equal(f.at(6)) {
			t.Errorf("%+v", got[ko])
		}
	})

	t.Run("a first read that runs out of time: no tail for now, never refused, built again later", func(t *testing.T) {
		f := &fakeIndex{fillWait: map[string]bool{ko: true}}
		f.add("orders", "1")
		f.add("customers", "9")
		k := newKeeper(f)
		k.buildTimeout = 20 * time.Millisecond
		k.retryMin, k.retryMax = time.Minute, time.Hour
		if got, err := k.refresh(ctx, []views.BaselineTable{orders, customers}); err != nil || len(got) != 0 {
			t.Fatalf("a statement waited for its builds: %+v %v", got, err)
		}
		k.settle() // orders' build ran out of its own time; customers' is in
		got := refresh(t, k, orders, customers)
		if _, ok := got[ko]; ok {
			t.Errorf("a build that ran out of time served a tail: %+v", got[ko])
		}
		if _, ok := got[kc]; !ok {
			t.Error("the other table has no tail: it was judged on the one that ran out of time")
		}
		if e := k.tables[ko]; e != nil {
			t.Fatalf("a build that ran out of time left an entry (refused %q): it would stand until the next cut", e.refused)
		}
		fills := f.fills
		refresh(t, k, orders, customers)
		if f.fills != fills {
			t.Error("tried again before its wait was over")
		}
		clock = clock.Add(time.Minute)
		if _, ok := refresh(t, k, orders, customers)[ko]; !ok {
			t.Error("not built again once its wait was over")
		}
	})

	t.Run("a rebuild that runs out of time keeps the advancing tail it had, over the newer cut", func(t *testing.T) {
		f := &fakeIndex{anchors: true, cuts: map[string]uint64{}}
		f.add("orders", "1")
		k := newKeeper(f)
		k.buildTimeout = 20 * time.Millisecond
		k.retryMin, k.retryMax = time.Hour, time.Hour
		before := refresh(t, k, orders)
		f.cuts[ko] = 1
		k.tables[ko].tail.(*fakeTail).pruneErr = errors.New("no position")
		f.fillWait = map[string]bool{ko: true}
		if got, err := k.refresh(ctx, []views.BaselineTable{newerCut(orders)}); err != nil || got[ko].Path != before[ko].Path {
			t.Fatalf("the tail it had was dropped: %+v %v", got[ko], err)
		}
		k.settle()
		fills := f.fills
		if got := refresh(t, k, newerCut(orders)); got[ko].Path != before[ko].Path || f.fills != fills {
			t.Errorf("rebuilt again at every statement: %+v, %d fills", got[ko], f.fills-fills)
		}
	})

	t.Run("the time to refresh is counted from when the keeper is the statement's", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		k.mu.Lock()
		released := make(chan struct{})
		go func() { time.Sleep(100 * time.Millisecond); k.mu.Unlock(); close(released) }()
		if _, err := k.refreshWithin(ctx, 50*time.Millisecond, []views.BaselineTable{orders}); err != nil {
			t.Fatalf("the wait for another statement was spent from this one's time: %v", err)
		}
		<-released
		k.settle()
	})

	t.Run("a version written over an older cut is not laid over a newer one", func(t *testing.T) {
		f := &fakeIndex{fillErr: map[string]error{}}
		f.add("orders", "1")
		k := newKeeper(f)
		before := refresh(t, k, orders)
		if before[ko].Path == "" {
			t.Fatal("no version written")
		}
		f.add("orders", "2")
		f.fillErr[ko] = errors.New("timeout") // the rebuild fails: the advancing tail stays
		f.writeErr = errors.New("disk full")  // and its newer version cannot be written
		if got := refresh(t, k, newerCut(orders)); len(got) != 0 {
			t.Fatalf("a version of the older cut over the newer one: %+v", got[ko])
		}
		f.writeErr = nil
		if got := refresh(t, k, newerCut(orders)); got[ko].Path == "" {
			t.Error("no version once it could be written")
		}
	})

	t.Run("a frozen tail is never laid over a newer cut, even when its rebuild fails", func(t *testing.T) {
		f := &fakeIndex{fillErr: map[string]error{}}
		f.add("orders", "1")
		k := newKeeper(f)
		refresh(t, k, orders)
		for _, key := range []string{"2", "3", "4"} {
			f.add("orders", key)
		}
		refresh(t, k, orders) // frozen
		f.fillErr[ko] = errors.New("timeout")
		if got := refresh(t, k, newerCut(orders)); len(got) != 0 {
			t.Fatalf("a frozen tail over a newer cut: %+v", got[ko])
		}
	})

	t.Run("refusals: no tail, until the next published cut", func(t *testing.T) {
		f := &fakeIndex{startWhy: map[string]string{ko: "text key"}, fillWhy: map[string]string{kc: "rotated"}}
		f.add("orders", "1")
		f.add("customers", "1")
		k := newKeeper(f)
		if got := refresh(t, k, orders, customers); len(got) != 0 {
			t.Fatalf("%+v", got)
		}
		delete(f.fillWhy, kc)
		if got := refresh(t, k, orders, customers); len(got) != 0 {
			t.Fatalf("a refusal lifted before the cut moved: %+v", got)
		}
		if got := refresh(t, k, orders, newerCut(customers)); len(got) != 1 {
			t.Fatalf("a newer cut did not try again: %+v", got)
		}
	})

	t.Run("a change of definition after the cut drops the tail and, in time, its file", func(t *testing.T) {
		f := &fakeIndex{changed: map[string]string{}}
		f.add("orders", "1")
		k := newKeeper(f)
		before := refresh(t, k, orders)
		f.changed[ko] = "ALTER TABLE"
		if got := refresh(t, k, orders); len(got) != 0 {
			t.Fatalf("still a tail: %+v", got[ko])
		}
		clock = clock.Add(2 * time.Minute)
		refresh(t, k, orders)
		if _, err := os.Stat(before[ko].Path); !os.IsNotExist(err) {
			t.Errorf("its file stays: %v", err)
		}
	})

	t.Run("a fold that fails refuses the table", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		refresh(t, k, orders)
		k.tables[ko].tail.(*fakeTail).applyErr = errors.New("a key-changing UPDATE")
		f.add("orders", "2")
		if got := refresh(t, k, orders); len(got) != 0 {
			t.Fatalf("%+v", got)
		}
	})

	t.Run("over its budget a tail stops advancing and keeps serving what it holds", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		before := refresh(t, k, orders)
		for _, key := range []string{"2", "3", "4"} {
			f.add("orders", key)
		}
		got := refresh(t, k, orders)
		if got[ko].Path != before[ko].Path || !got[ko].Through.Equal(before[ko].Through) {
			t.Fatalf("a frozen tail moved: %+v -> %+v", before[ko], got[ko])
		}
		held := k.tables[ko].tail.Keys()
		f.add("orders", "5")
		f.sinceTables = nil
		if got := refresh(t, k, orders); got[ko].Path != before[ko].Path {
			t.Fatalf("a frozen tail advanced: %+v", got[ko])
		}
		if n := k.tables[ko].tail.Keys(); n != held {
			t.Errorf("a frozen tail went on growing in memory: %d keys, held %d", n, held)
		}
		if len(f.sinceTables) != 0 {
			t.Errorf("the events of a frozen tail's table are still read: %v", f.sinceTables)
		}
	})

	t.Run("a tail frozen by one page takes none of the pages after it", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		refresh(t, k, orders)
		for _, key := range []string{"2", "3", "4", "5", "6", "7"} {
			f.add("orders", key) // pages (2,3) (4,5) (6,7): frozen at the second
		}
		refresh(t, k, orders)
		if n := k.tables[ko].tail.Keys(); n != 5 {
			t.Errorf("%d keys, want the 5 it held when it froze", n)
		}
	})

	t.Run("a table over the budget at its first read has no tail, and its read stops early", func(t *testing.T) {
		f := &fakeIndex{}
		for _, key := range []string{"1", "2", "3", "4", "5", "6", "7", "8"} {
			f.add("orders", key)
		}
		if got := refresh(t, newKeeper(f), orders); len(got) != 0 {
			t.Fatalf("%+v", got)
		}
		if f.pages != 2 {
			t.Errorf("read %d pages, want 2: the read stops at the page that crossed the budget", f.pages)
		}
	})

	t.Run("an index that holds fewer events than were read starts over", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("orders", "2")
		k := newKeeper(f)
		refresh(t, k, orders)
		// Rebuilt: its ids start again, while the source's binary log went on.
		f.events = f.events[:1]
		f.events[0].StartPos = 900
		if got := refresh(t, k, orders); fileKeys(t, got[ko].Path) != 1 || !got[ko].Through.Equal(f.at(1)) || f.fills != 2 {
			t.Fatalf("%+v (filled %d times)", got[ko], f.fills)
		}
	})

	t.Run("a binary log that started again from another numbering starts over", func(t *testing.T) {
		for name, restart := range map[string]func(*query.ResultRow){
			"RESET MASTER":       func(r *query.ResultRow) { r.BinlogFile, r.StartPos = "binlog.000001", 4 },
			"another base name":  func(r *query.ResultRow) { r.BinlogFile = "mysql-bin.000009" },
			"no binlog position": func(r *query.ResultRow) { r.BinlogFile = "" },
		} {
			f := &fakeIndex{}
			f.add("orders", "1")
			f.add("orders", "2")
			k := newKeeper(f)
			refresh(t, k, orders)
			f.add("orders", "3")
			restart(&f.events[2])
			got := refresh(t, k, orders)
			if f.fills != 2 {
				t.Errorf("%s: filled %d times, want a rebuild", name, f.fills)
			}
			_ = got
		}
	})

	t.Run("an index rebuilt whose new ids passed the old mark starts over", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("orders", "2")
		k := newKeeper(f)
		refresh(t, k, orders)
		// Re-created: ids from 1 again, the source's log further on, and by
		// the next statement past the old mark.
		f.events = nil
		for _, key := range []string{"7", "8", "9"} {
			f.add("orders", key)
		}
		for i := range f.events {
			f.events[i].StartPos += 1000
		}
		got := refresh(t, k, orders)
		if f.fills != 2 || fileKeys(t, got[ko].Path) != 3 {
			t.Fatalf("filled %d times, %d keys: the new events were skipped", f.fills, fileKeys(t, got[ko].Path))
		}
	})

	t.Run("a numbering that started again inside the window starts over", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("orders", "2")
		k := newKeeper(f)
		refresh(t, k, orders) // mark at binlog.000003:200
		f.add("orders", "3")
		f.events[2].BinlogFile, f.events[2].StartPos = "binlog.000001", 4 // RESET MASTER
		f.add("orders", "4")
		f.events[3].BinlogFile, f.events[3].StartPos = "binlog.000004", 4 // the new numbering went on
		refresh(t, k, orders)
		if f.fills != 2 {
			t.Fatalf("filled %d times, want a rebuild", f.fills)
		}
	})

	t.Run("a numbering that started again, seen only in another table's event, starts over", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		refresh(t, k, orders) // mark at binlog.000003:100
		f.add("customers", "1")
		f.events[1].BinlogFile, f.events[1].StartPos = "binlog.000001", 4 // RESET MASTER, no tail reads it
		refresh(t, k, orders)
		if f.fills != 2 {
			t.Fatalf("filled %d times, want a rebuild", f.fills)
		}
	})

	t.Run("a numbering that started again between two events after the cursor starts over", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		refresh(t, k, orders) // mark at binlog.000003:100
		f.add("orders", "2")
		f.events[1].BinlogFile = "binlog.000009"
		f.add("orders", "3")
		f.events[2].BinlogFile = "binlog.000004" // after the mark, before the event before it
		f.add("orders", "4")
		f.events[3].BinlogFile = "binlog.000010"
		refresh(t, k, orders)
		if f.fills != 2 {
			t.Fatalf("filled %d times, want a rebuild", f.fills)
		}
	})

	t.Run("a tail frozen before any version was written has no view", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		f.writeErr = errors.New("disk full")
		if got := refresh(t, k, orders); len(got) != 0 {
			t.Fatalf("%+v", got)
		}
		f.writeErr = nil
		for _, key := range []string{"2", "3", "4"} {
			f.add("orders", key)
		}
		if got := refresh(t, k, orders); len(got) != 0 {
			t.Fatalf("a frozen, never written tail: %+v", got[ko])
		}
	})

	t.Run("an index that cannot be read: no tails for the statement, caught up after", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		refresh(t, k, orders)
		f.add("orders", "2")
		f.sinceErr = errors.New("gone")
		if got, err := k.refresh(ctx, []views.BaselineTable{orders}); err == nil || got != nil {
			t.Fatalf("%+v %v", got, err)
		}
		f.sinceErr = nil
		f.changedErr = errors.New("gone")
		if _, err := k.refresh(ctx, []views.BaselineTable{orders}); err == nil {
			t.Fatal("a schema_changes read that failed gave tails")
		}
		f.changedErr = nil
		if got := refresh(t, k, orders); fileKeys(t, got[ko].Path) != 2 {
			t.Errorf("not caught up after the index came back: %+v", got[ko])
		}
	})

	t.Run("a version that cannot be written leaves the last one; a refusal ends the tail", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		before := refresh(t, k, orders)
		f.add("orders", "2")
		f.writeErr = errors.New("disk full")
		got := refresh(t, k, orders)
		if got[ko].Path != before[ko].Path || !got[ko].Through.Equal(before[ko].Through) {
			t.Fatalf("a failed write moved the view: %+v -> %+v", before[ko], got[ko])
		}
		f.writeErr = nil
		if got := refresh(t, k, orders); fileKeys(t, got[ko].Path) != 2 || !got[ko].Through.Equal(f.at(2)) {
			t.Fatalf("not retried: %+v", got[ko])
		}
		f.add("orders", "3")
		f.writeErr = &reconstruct.TailRefusal{Reason: "a column dropped"}
		if got := refresh(t, k, orders); len(got) != 0 {
			t.Fatalf("a refused write still served: %+v", got[ko])
		}
	})

	t.Run("a tail no statement asked for in a while is forgotten, its files in time", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("customers", "1")
		k := newKeeper(f)
		first := refresh(t, k, orders)
		clock = clock.Add(2 * time.Hour)
		refresh(t, k, customers)
		if _, ok := k.tables[ko]; ok {
			t.Fatal("an idle tail is still kept")
		}
		clock = clock.Add(2 * time.Minute)
		refresh(t, k, customers)
		if _, err := os.Stat(first[ko].Path); !os.IsNotExist(err) {
			t.Errorf("its file stays: %v", err)
		}
	})

	t.Run("replaced versions are removed once no statement can read them", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		first := refresh(t, k, orders)
		f.add("orders", "2")
		second := refresh(t, k, orders)
		clock = clock.Add(2 * time.Minute)
		refresh(t, k, orders)
		if _, err := os.Stat(first[ko].Path); !os.IsNotExist(err) {
			t.Errorf("an old version outlived its time: %v", err)
		}
		if _, err := os.Stat(second[ko].Path); err != nil {
			t.Errorf("the current version was removed: %v", err)
		}
		if names, _ := filepath.Glob(filepath.Join(k.dir, "*")); len(names) != 1 {
			t.Errorf("files left: %v", names)
		}
	})

	t.Run("each keeper writes in a directory of its own", func(t *testing.T) {
		parent := t.TempDir()
		var dirs []string
		for range 2 {
			f := &fakeIndex{}
			f.add("orders", "1")
			k := newTailKeeper(f, parent, tailBudget{Keys: 3}, time.Minute, time.Hour)
			refresh(t, k, orders)
			dirs = append(dirs, k.dir)
		}
		if dirs[0] == dirs[1] || filepath.Dir(dirs[0]) != parent {
			t.Errorf("directories %v under %s", dirs, parent)
		}
	})
}

// lateIndex indexes one more event between the newest being read and the
// events being read, as a running capture does.
type lateIndex struct{ *fakeIndex }

func (l *lateIndex) Newest(ctx context.Context) (tailMark, error) {
	m, err := l.fakeIndex.Newest(ctx)
	l.fakeIndex.add("orders", "3")
	return m, err
}

// foldingIndex answers Since as the index does: names compared without case.
type foldingIndex struct{ *fakeIndex }

func (f *foldingIndex) Since(_ context.Context, tables []views.BaselineTable, from, upTo uint64, page func([]query.ResultRow) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	want := map[string]bool{}
	for _, t := range tables {
		want[strings.ToLower(tailKey(t.Schema, t.Table))] = true
	}
	var evs []query.ResultRow
	for _, ev := range f.events {
		if want[strings.ToLower(tailKey(ev.SchemaName, ev.TableName))] && ev.EventID > from && ev.EventID <= upTo {
			evs = append(evs, ev)
		}
	}
	return f.hand(evs, page)
}

// #2483: a newer published cut prunes a live tail to it, with no read of the
// index and no wait; every case where pruning could serve a wrong row builds
// the tail again from the index instead, as before.
func TestTailKeeper_pruneToANewerCut(t *testing.T) {
	ctx := context.Background()
	orders := keeperTable("orders")
	ko := tailKey("shop", "orders")
	clock := time.Date(2026, 10, 14, 13, 0, 0, 0, time.UTC)
	newKeeper := func(f *fakeIndex) *tailKeeper {
		k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 10}, time.Minute, time.Hour)
		k.now = func() time.Time { return clock }
		k.retryMin, k.retryMax = 0, 0
		return k
	}
	refresh := func(t *testing.T, k *tailKeeper, tables ...views.BaselineTable) map[string]tailView {
		t.Helper()
		return settled(t, ctx, k, tables...)
	}
	// started is a keeper whose tail of orders holds keys 1..4 (events 1..4)
	// over a cut at 0, and an index whose newer cut of orders stops at event
	// 2, the cursor at event 5 (another table's).
	started := func(t *testing.T) (*fakeIndex, *tailKeeper, tailView) {
		t.Helper()
		f := &fakeIndex{anchors: true, cuts: map[string]uint64{}}
		for _, key := range []string{"1", "2", "3", "4"} {
			f.add("orders", key)
		}
		k := newKeeper(f)
		before := refresh(t, k, orders)[ko]
		if fileKeys(t, before.Path) != 4 || f.fills != 1 {
			t.Fatalf("setup: %+v, %d fills", before, f.fills)
		}
		f.add("customers", "9")
		f.cuts[ko] = 2
		return f, k, before
	}
	keys := func(k *tailKeeper) string {
		var out []string
		for key := range k.tables[ko].tail.(*fakeTail).keys {
			out = append(out, key)
		}
		slices.Sort(out)
		return strings.Join(out, ",")
	}

	t.Run("prunes what the newer cut holds, into a new file, with no read of the index", func(t *testing.T) {
		f, k, before := started(t)
		got := refresh(t, k, newerCut(orders))[ko]
		if f.fills != 1 {
			t.Errorf("filled %d times: the newer cut was read from the index", f.fills)
		}
		if keys(k) != "3,4" || fileKeys(t, got.Path) != 2 {
			t.Errorf("kept %s, file holds %d keys, want 3,4", keys(k), fileKeys(t, got.Path))
		}
		if got.Path == before.Path || !got.Through.Equal(f.at(5)) {
			t.Errorf("before %+v after %+v", before, got)
		}
		if _, err := os.Stat(before.Path); err != nil {
			t.Errorf("the version a running statement may read is gone: %v", err)
		}
		if e := k.tables[ko]; e.cut != tailCutOf(newerCut(orders)) || e.fileCut != e.cut || e.from == nil || e.from.lastEventID != 2 {
			t.Errorf("entry not moved to the newer cut: %+v", e)
		}
		// And it goes on advancing over it.
		f.add("orders", "1")
		if got := refresh(t, k, newerCut(orders))[ko]; keys(k) != "1,3,4" || fileKeys(t, got.Path) != 3 || f.fills != 1 {
			t.Errorf("after the prune: kept %s, %d fills", keys(k), f.fills)
		}
	})

	t.Run("the same keys as a rebuild from the newer cut", func(t *testing.T) {
		_, pruned, _ := started(t)
		refresh(t, pruned, newerCut(orders))
		f, rebuilt, _ := started(t)
		f.anchors = false
		refresh(t, rebuilt, newerCut(orders))
		if f.fills != 2 {
			t.Fatalf("the reference was not rebuilt: %d fills", f.fills)
		}
		if keys(pruned) != keys(rebuilt) {
			t.Errorf("pruned %s, rebuilt %s", keys(pruned), keys(rebuilt))
		}
	})

	t.Run("no build: the statement gets the pruned tail at once", func(t *testing.T) {
		f, k, _ := started(t)
		f.fillGate = map[string]chan struct{}{ko: make(chan struct{})} // a build would hang
		got, err := k.refresh(ctx, []views.BaselineTable{newerCut(orders)})
		k.settle()
		if err != nil || fileKeys(t, got[ko].Path) != 2 || f.fills != 1 || len(k.builds) != 0 {
			t.Fatalf("%+v %v, %d fills, builds %v", got[ko], err, f.fills, k.builds)
		}
	})

	t.Run("pruned to nothing: no file, the cursor's instant", func(t *testing.T) {
		f, k, _ := started(t)
		f.cuts[ko] = 4
		got, ok := refresh(t, k, newerCut(orders))[ko]
		if !ok || got.Path != "" || !got.Through.Equal(f.at(5)) || f.fills != 1 {
			t.Fatalf("%+v %v, %d fills", got, ok, f.fills)
		}
	})

	// Every case below builds the tail again from the index (a second Fill).
	rebuilds := func(t *testing.T, f *fakeIndex, k *tailKeeper, table views.BaselineTable) {
		t.Helper()
		refresh(t, k, table)
		if f.fills != 2 {
			t.Errorf("filled %d times, want a rebuild", f.fills)
		}
		if e := k.tables[ko]; e != nil && e.tail != nil && e.tail.(*fakeTail).prunes != 0 {
			t.Errorf("pruned too")
		}
	}
	for name, edit := range map[string]func(f *fakeIndex, k *tailKeeper){
		"the newer cut's position cannot be read": func(f *fakeIndex, _ *tailKeeper) { f.cutWhy = map[string]string{ko: "no position"} },
		"the newer cut is in another numbering":   func(f *fakeIndex, _ *tailKeeper) { f.cutFile = "mysql-bin.000003" },
		"the cut did not move (a re-dump at the same position)": func(f *fakeIndex, _ *tailKeeper) {
			f.cutAt = map[string]query.BinlogPos{ko: {File: "binlog.000003", Pos: 1}}
		},
		"the cut moved back": func(f *fakeIndex, _ *tailKeeper) {
			f.cutAt = map[string]query.BinlogPos{ko: {File: "binlog.000002", Pos: 900}}
		},
		"the cut is past the cursor (capture behind a dump's position)": func(f *fakeIndex, _ *tailKeeper) {
			f.cutAt = map[string]query.BinlogPos{ko: {File: "binlog.000003", Pos: 501}}
			f.cutLast = map[string]uint64{ko: 0}
		},
		"the cut is past the cursor, in a later file": func(f *fakeIndex, _ *tailKeeper) {
			f.cutAt = map[string]query.BinlogPos{ko: {File: "binlog.000004", Pos: 4}}
			f.cutLast = map[string]uint64{ko: 0}
		},
		"the cut's last event of the table is past the cursor": func(f *fakeIndex, _ *tailKeeper) {
			f.cutAt = map[string]query.BinlogPos{ko: {File: "binlog.000003", Pos: 501}}
			f.cutLast = map[string]uint64{ko: 6}
		},
		"the newer cut's last event id is below the older's": func(f *fakeIndex, k *tailKeeper) {
			k.tables[ko].from.lastEventID = 3
		},
		"the tail was built with no position on record": func(_ *fakeIndex, k *tailKeeper) { k.tables[ko].from = nil },
		"a key's change cannot be ordered against the cut": func(_ *fakeIndex, k *tailKeeper) {
			k.tables[ko].tail.(*fakeTail).pruneErr = errors.New("no position")
		},
	} {
		t.Run("rebuilds: "+name, func(t *testing.T) {
			f, k, _ := started(t)
			edit(f, k)
			rebuilds(t, f, k, newerCut(orders))
		})
	}

	t.Run("rebuilds: the table's copy was written again (another file)", func(t *testing.T) {
		f, k, _ := started(t)
		redump := orders
		redump.Path = "/snap/2/shop/orders.parquet"
		rebuilds(t, f, k, redump)
	})

	t.Run("rebuilds: a frozen tail", func(t *testing.T) {
		f, k, _ := started(t)
		k.tables[ko].frozen = true
		rebuilds(t, f, k, newerCut(orders))
	})

	t.Run("no prune for a refused tail or a definition that changed", func(t *testing.T) {
		f, k, _ := started(t)
		f.changed = map[string]string{ko: "ALTER TABLE"}
		if got := refresh(t, k, newerCut(orders)); len(got) != 0 || f.fills != 1 {
			t.Fatalf("%+v, %d fills", got, f.fills)
		}
		f.changed = nil
		k.tables[ko].cut = "older"
		rebuilds(t, f, k, newerCut(orders))
	})

	t.Run("a tail kept over a newer cut after a rebuild failed still prunes from its own cut", func(t *testing.T) {
		f, k, _ := started(t)
		k.retryMin, k.retryMax = time.Hour, time.Hour // the failed rebuild is not tried again here
		f.cutWhy = map[string]string{ko: "unreadable now"}
		f.fillErr = map[string]error{ko: errors.New("timeout")}
		refresh(t, k, newerCut(orders))
		if e := k.tables[ko]; e.from == nil || e.from.lastEventID != 0 {
			t.Fatalf("the position of what the tail holds moved with a relabel: %+v", e.from)
		}
		f.cutWhy = nil
		f.cuts[ko] = 3
		third := newerCut(orders)
		third.DeltaFiles = append(third.DeltaFiles, baseline.TableDeltaFile{Upserts: "/snap/3/shop/orders.000001.upserts"})
		if got := refresh(t, k, third)[ko]; keys(k) != "4" || fileKeys(t, got.Path) != 1 || f.fills != 1 {
			t.Errorf("kept %s, %d fills", keys(k), f.fills)
		}
	})
}

// #2483 review: a cut the cursor has not reached (a dump's position, ahead of
// capture) holds changes the keeper has yet to fold. Folded later, they would
// be laid over the cut as changes after it, older values over its newer ones.
func TestTailKeeper_aCutAheadOfTheCursor(t *testing.T) {
	ctx := context.Background()
	orders := keeperTable("orders")
	ko := tailKey("shop", "orders")
	newKeeper := func(f *fakeIndex) *tailKeeper {
		k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 10}, time.Minute, time.Hour)
		k.retryMin, k.retryMax = 0, 0
		return k
	}
	refresh := func(t *testing.T, k *tailKeeper, tables ...views.BaselineTable) map[string]tailView {
		t.Helper()
		return settled(t, ctx, k, tables...)
	}
	keys := func(k *tailKeeper) string {
		var out []string
		for key := range k.tables[ko].tail.(*fakeTail).keys {
			out = append(out, key)
		}
		slices.Sort(out)
		return strings.Join(out, ",")
	}

	t.Run("a refresh's cut that ends right after the cursor's event prunes, by its last event id", func(t *testing.T) {
		f := &fakeIndex{anchors: true, cuts: map[string]uint64{}}
		for _, key := range []string{"1", "2", "3"} {
			f.add("orders", key)
		}
		k := newKeeper(f)
		refresh(t, k, orders)
		f.cuts[ko] = 3
		f.cutAt = map[string]query.BinlogPos{ko: {File: "binlog.000003", Pos: 310}} // the end of event 3
		if got, ok := refresh(t, k, newerCut(orders))[ko]; !ok || got.Path != "" || f.fills != 1 {
			t.Fatalf("%+v %v, %d fills", got, ok, f.fills)
		}
	})

	t.Run("a tail built over it folds only the changes after it, as its first read did", func(t *testing.T) {
		f := &fakeIndex{anchors: true, cuts: map[string]uint64{}}
		f.add("orders", "1")
		f.cutAt = map[string]query.BinlogPos{ko: {File: "binlog.000003", Pos: 301}} // ahead of event 1
		k := newKeeper(f)
		if got := refresh(t, k, orders)[ko]; fileKeys(t, got.Path) != 1 {
			t.Fatalf("setup: %+v", got)
		}
		f.add("orders", "2") // at 200: in the cut
		f.add("orders", "3") // at 300: in the cut
		f.add("orders", "4") // at 400: after it
		refresh(t, k, orders)
		// Key 1 came from the fake's own Fill, which floors by id, not by
		// position; what matters is that 2 and 3 were not folded.
		if keys(k) != "1,4" {
			t.Errorf("kept %s, want 2 and 3 left to the cut", keys(k))
		}
	})

	t.Run("a tail it had is not kept standing over it while it is rebuilt, nor when the rebuild fails", func(t *testing.T) {
		for name, fail := range map[string]func(f *fakeIndex){
			"in flight":    func(f *fakeIndex) { f.fillGate = map[string]chan struct{}{ko: make(chan struct{})} },
			"a read error": func(f *fakeIndex) { f.fillErr = map[string]error{ko: errors.New("gone")} },
			"no time":      func(f *fakeIndex) { f.fillWait = map[string]bool{ko: true} },
		} {
			f := &fakeIndex{anchors: true, cuts: map[string]uint64{}}
			f.add("orders", "1")
			k := newKeeper(f)
			k.buildTimeout = 20 * time.Millisecond
			k.retryMin, k.retryMax = time.Hour, time.Hour
			refresh(t, k, orders)
			f.cutAt = map[string]query.BinlogPos{ko: {File: "binlog.000003", Pos: 901}}
			fail(f)
			for i := range 2 { // while the build runs, and after it failed
				got, err := k.refresh(ctx, []views.BaselineTable{newerCut(orders)})
				if _, ok := got[ko]; ok || err != nil {
					t.Errorf("%s, statement %d: a tail laid over a cut the cursor has not reached: %+v %v", name, i, got[ko], err)
				}
				k.settle()
			}
		}
	})
}
