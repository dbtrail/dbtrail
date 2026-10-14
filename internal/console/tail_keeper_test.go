package console

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/views"
)

// fakeIndex is a tailSource over events held in memory: ids in the order
// they were indexed, a time for each.
type fakeIndex struct {
	events []query.ResultRow
	// newestErr, sinceErr fail those reads; fillErr fails Fill for a table
	// once (a rebuild that times out); startWhy, fillWhy, changed refuse.
	newestErr, sinceErr error
	fillErr             map[string]error
	startWhy, fillWhy   map[string]string
	changed             map[string]string
	// cuts is the event id each table's published cut stops at, by tailKey.
	cuts  map[string]uint64
	fills int
}

func (f *fakeIndex) Newest(context.Context) (uint64, time.Time, error) {
	if f.newestErr != nil {
		return 0, time.Time{}, f.newestErr
	}
	if len(f.events) == 0 {
		return 0, time.Time{}, nil
	}
	last := f.events[len(f.events)-1]
	return last.EventID, last.EventTimestamp, nil
}

func (f *fakeIndex) Start(_ context.Context, t views.BaselineTable) (tailTable, string, error) {
	if why := f.startWhy[tailKey(t.Schema, t.Table)]; why != "" {
		return nil, why, nil
	}
	return &fakeTail{}, "", nil
}

func (f *fakeIndex) Fill(_ context.Context, t views.BaselineTable, upTo uint64) ([]query.ResultRow, string, error) {
	key := tailKey(t.Schema, t.Table)
	if err := f.fillErr[key]; err != nil {
		delete(f.fillErr, key)
		return nil, "", err
	}
	if why := f.fillWhy[key]; why != "" {
		return nil, why, nil
	}
	f.fills++
	var out []query.ResultRow
	for _, ev := range f.events {
		if tailKey(ev.SchemaName, ev.TableName) == key && ev.EventID > f.cuts[key] && ev.EventID <= upTo {
			out = append(out, ev)
		}
	}
	return out, "", nil
}

func (f *fakeIndex) Since(_ context.Context, from, upTo uint64) ([]query.ResultRow, error) {
	if f.sinceErr != nil {
		return nil, f.sinceErr
	}
	var out []query.ResultRow
	for _, ev := range f.events {
		if ev.EventID > from && ev.EventID <= upTo {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (f *fakeIndex) Changed(context.Context, []views.BaselineTable) (map[string]string, error) {
	return f.changed, nil
}

// add indexes one change of schema.table to key, a minute after the last.
func (f *fakeIndex) add(table, key string) {
	id := uint64(len(f.events) + 1)
	f.events = append(f.events, query.ResultRow{EventID: id, SchemaName: "shop", TableName: table, PKValues: key,
		EventTimestamp: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC).Add(time.Duration(id) * time.Minute)})
}

func (f *fakeIndex) at(id uint64) time.Time { return f.events[id-1].EventTimestamp }

// fakeTail keeps the keys it was given and writes them, one per line.
type fakeTail struct {
	keys     map[string]bool
	applyErr error
}

func (t *fakeTail) Apply(page []query.ResultRow) error {
	if t.applyErr != nil {
		return t.applyErr
	}
	if t.keys == nil {
		t.keys = map[string]bool{}
	}
	for _, ev := range page {
		t.keys[ev.PKValues] = true
	}
	return nil
}
func (t *fakeTail) Keys() int           { return len(t.keys) }
func (t *fakeTail) PKColumns() []string { return []string{"id"} }
func (t *fakeTail) WriteFile(path string) (int64, error) {
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
	newKeeper := func(f *fakeIndex) *tailKeeper { return newTailKeeper(f, t.TempDir(), 3, time.Minute) }

	t.Run("nothing indexed: no tails", func(t *testing.T) {
		k := newKeeper(&fakeIndex{})
		if got, err := k.refresh(ctx, []views.BaselineTable{orders}); err != nil || len(got) != 0 {
			t.Fatalf("%v %v", got, err)
		}
	})

	t.Run("first read: the table's changes after its cut, through the newest event", func(t *testing.T) {
		f := &fakeIndex{cuts: map[string]uint64{ko: 1}}
		f.add("orders", "1") // at or before the cut
		f.add("orders", "2")
		f.add("customers", "9")
		got, err := newKeeper(f).refresh(ctx, []views.BaselineTable{orders})
		if err != nil {
			t.Fatal(err)
		}
		v := got[ko]
		if fileKeys(t, v.Path) != 1 || !v.Through.Equal(f.at(3)) || v.PK[0] != "id" {
			t.Fatalf("%+v", v)
		}
	})

	t.Run("later reads fold only what came after, into a new file; the old one stays", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		first, _ := k.refresh(ctx, []views.BaselineTable{orders})
		f.add("orders", "2")
		f.add("customers", "5") // no tail for customers: not read
		second, err := k.refresh(ctx, []views.BaselineTable{orders})
		if err != nil {
			t.Fatal(err)
		}
		if second[ko].Path == first[ko].Path || fileKeys(t, second[ko].Path) != 2 || !second[ko].Through.Equal(f.at(3)) {
			t.Fatalf("first %+v second %+v", first[ko], second[ko])
		}
		if _, err := os.Stat(first[ko].Path); err != nil {
			t.Errorf("the replaced version is gone at once: %v", err)
		}
		if f.fills != 1 {
			t.Errorf("filled %d times, want once: later reads go by the cursor", f.fills)
		}
		// Nothing new: the same file, the same instant.
		third, _ := k.refresh(ctx, []views.BaselineTable{orders})
		if third[ko].Path != second[ko].Path || !third[ko].Through.Equal(second[ko].Through) {
			t.Errorf("nothing new changed the view: %+v -> %+v", second[ko], third[ko])
		}
	})

	t.Run("an event indexed after the newest was read is not folded and does not move the instant", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		f.add("orders", "2")
		// Since sees id 2 only up to the newest read first: simulated by
		// reading Newest, then indexing a third event before Since runs.
		src := &lateIndex{fakeIndex: f}
		k.src = src
		got, err := k.refresh(ctx, []views.BaselineTable{orders})
		if err != nil {
			t.Fatal(err)
		}
		if fileKeys(t, got[ko].Path) != 2 || !got[ko].Through.Equal(f.at(2)) {
			t.Fatalf("%+v (keys %d)", got[ko], fileKeys(t, got[ko].Path))
		}
	})

	t.Run("a later read stops at the newest event read first, too", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		k.refresh(ctx, []views.BaselineTable{orders})
		f.add("orders", "2")
		k.src = &lateIndex{fakeIndex: f}
		got, err := k.refresh(ctx, []views.BaselineTable{orders})
		if err != nil {
			t.Fatal(err)
		}
		if fileKeys(t, got[ko].Path) != 2 || !got[ko].Through.Equal(f.at(2)) {
			t.Fatalf("%+v (keys %d)", got[ko], fileKeys(t, got[ko].Path))
		}
	})

	t.Run("an unchanged table has no file and the cursor's instant", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("customers", "1")
		got, _ := newKeeper(f).refresh(ctx, []views.BaselineTable{orders})
		if v, ok := got[ko]; !ok || v.Path != "" || !v.Through.Equal(f.at(1)) {
			t.Fatalf("%+v %v", v, ok)
		}
	})

	t.Run("a newer published cut rebuilds the tail from it", func(t *testing.T) {
		f := &fakeIndex{cuts: map[string]uint64{}}
		f.add("orders", "1")
		f.add("orders", "2")
		k := newKeeper(f)
		before, _ := k.refresh(ctx, []views.BaselineTable{orders})
		published := orders
		published.DeltaFiles = []baseline.TableDeltaFile{{Upserts: "/snap/2/shop/orders.000000.upserts"}}
		f.cuts[ko] = 1 // the new cut folded event 1
		after, err := k.refresh(ctx, []views.BaselineTable{published})
		if err != nil {
			t.Fatal(err)
		}
		if fileKeys(t, before[ko].Path) != 2 || fileKeys(t, after[ko].Path) != 1 {
			t.Fatalf("before %+v after %+v", before[ko], after[ko])
		}
	})

	t.Run("a rebuild that cannot read the index keeps the tail it had", func(t *testing.T) {
		f := &fakeIndex{fillErr: map[string]error{}}
		f.add("orders", "1")
		k := newKeeper(f)
		before, _ := k.refresh(ctx, []views.BaselineTable{orders})
		published := orders
		published.DeltaFiles = []baseline.TableDeltaFile{{Upserts: "/snap/2/shop/orders.000000.upserts"}}
		f.fillErr[ko] = errors.New("timeout")
		got, err := k.refresh(ctx, []views.BaselineTable{published})
		if err != nil || got[ko].Path != before[ko].Path {
			t.Fatalf("the tail it had was dropped: %+v %v", got[ko], err)
		}
		// Retried at the next statement.
		got, _ = k.refresh(ctx, []views.BaselineTable{published})
		if got[ko].Path == before[ko].Path {
			t.Error("the rebuild was not retried")
		}
	})

	t.Run("refusals: no tail, until the next published cut", func(t *testing.T) {
		f := &fakeIndex{startWhy: map[string]string{ko: "text key"}, fillWhy: map[string]string{kc: "rotated"}}
		f.add("orders", "1")
		f.add("customers", "1")
		got, _ := newKeeper(f).refresh(ctx, []views.BaselineTable{orders, customers})
		if len(got) != 0 {
			t.Fatalf("%+v", got)
		}
	})

	t.Run("a change of definition after the cut drops the tail and its file", func(t *testing.T) {
		f := &fakeIndex{changed: map[string]string{}}
		f.add("orders", "1")
		k := newKeeper(f)
		k.keepOld = 0
		before, _ := k.refresh(ctx, []views.BaselineTable{orders})
		f.changed[ko] = "ALTER TABLE"
		got, _ := k.refresh(ctx, []views.BaselineTable{orders})
		if _, ok := got[ko]; ok {
			t.Fatalf("still a tail: %+v", got[ko])
		}
		if _, err := os.Stat(before[ko].Path); !os.IsNotExist(err) {
			t.Errorf("its file stays: %v", err)
		}
	})

	t.Run("a fold that fails refuses the table", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		k.refresh(ctx, []views.BaselineTable{orders})
		k.tables[ko].tail.(*fakeTail).applyErr = errors.New("a key-changing UPDATE")
		f.add("orders", "2")
		if got, _ := k.refresh(ctx, []views.BaselineTable{orders}); len(got) != 0 {
			t.Fatalf("%+v", got)
		}
	})

	t.Run("over its budget a tail stops advancing and keeps serving what it holds", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		before, _ := k.refresh(ctx, []views.BaselineTable{orders})
		for _, key := range []string{"2", "3", "4"} {
			f.add("orders", key)
		}
		got, _ := k.refresh(ctx, []views.BaselineTable{orders})
		if got[ko].Path != before[ko].Path || !got[ko].Through.Equal(before[ko].Through) {
			t.Fatalf("a frozen tail moved: %+v -> %+v", before[ko], got[ko])
		}
		held := k.tables[ko].tail.Keys()
		f.add("orders", "5")
		if got, _ := k.refresh(ctx, []views.BaselineTable{orders}); got[ko].Path != before[ko].Path {
			t.Fatalf("a frozen tail advanced: %+v", got[ko])
		}
		if n := k.tables[ko].tail.Keys(); n != held {
			t.Errorf("a frozen tail went on growing in memory: %d keys, held %d", n, held)
		}
	})

	t.Run("a table over the budget at its first read has no tail", func(t *testing.T) {
		f := &fakeIndex{}
		for _, key := range []string{"1", "2", "3", "4"} {
			f.add("orders", key)
		}
		if got, _ := newKeeper(f).refresh(ctx, []views.BaselineTable{orders}); len(got) != 0 {
			t.Fatalf("%+v", got)
		}
	})

	t.Run("an index that holds fewer events than were read starts over", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("orders", "2")
		k := newKeeper(f)
		k.refresh(ctx, []views.BaselineTable{orders})
		f.events = f.events[:1]
		got, err := k.refresh(ctx, []views.BaselineTable{orders})
		if err != nil || fileKeys(t, got[ko].Path) != 1 || !got[ko].Through.Equal(f.at(1)) {
			t.Fatalf("%+v %v", got[ko], err)
		}
	})

	t.Run("an index that cannot be read: no tails for the statement", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		k.refresh(ctx, []views.BaselineTable{orders})
		f.add("orders", "2")
		f.sinceErr = errors.New("gone")
		if got, err := k.refresh(ctx, []views.BaselineTable{orders}); err == nil || got != nil {
			t.Fatalf("%+v %v", got, err)
		}
		f.sinceErr = nil
		if got, _ := k.refresh(ctx, []views.BaselineTable{orders}); fileKeys(t, got[ko].Path) != 2 {
			t.Errorf("not caught up after the index came back: %+v", got[ko])
		}
	})

	t.Run("replaced versions are removed once no statement can read them", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		now := time.Date(2026, 10, 14, 13, 0, 0, 0, time.UTC)
		k.now = func() time.Time { return now }
		first, _ := k.refresh(ctx, []views.BaselineTable{orders})
		f.add("orders", "2")
		second, _ := k.refresh(ctx, []views.BaselineTable{orders})
		now = now.Add(2 * time.Minute)
		k.refresh(ctx, []views.BaselineTable{orders})
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
}

// lateIndex indexes one more event between the newest being read and the
// events being read, as a running capture does.
type lateIndex struct{ *fakeIndex }

func (l *lateIndex) Newest(ctx context.Context) (uint64, time.Time, error) {
	id, at, err := l.fakeIndex.Newest(ctx)
	l.fakeIndex.add("orders", "3")
	return id, at, err
}
