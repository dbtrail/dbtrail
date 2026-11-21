package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #3049 gave the keeper one step that only a PostgreSQL source takes. This
// holds the keeper of any other source to what it did before that step
// existed: a fixed history of statements, changes, newer cuts, a budget, a
// refusal and a failed write, and for each step every read the keeper made
// of its source, in order, every call it made on each tail, in order, every
// file it wrote with the keys in it, and what it handed the statement.
//
// The transcript below was printed by this same test on the commit before
// the step was added (BINTRAIL_PRINT_KEEPER_TRANSCRIPT=1 prints it). It uses
// nothing the step added, so it runs unchanged on both sides.

// transcript records what a keeper does to its source and its tails. Reads
// that name a table are kept per table: two tables' builds run at the same
// time, and each table's own reads are in order.
type transcript struct {
	mu      sync.Mutex
	server  []string
	byTable map[string][]string
	byTail  map[string][]string
	starts  map[string]int
}

func (r *transcript) serverCall(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.server = append(r.server, fmt.Sprintf(format, args...))
}

func (r *transcript) tableCall(t views.BaselineTable, format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byTable == nil {
		r.byTable = map[string][]string{}
	}
	r.byTable[t.Table] = append(r.byTable[t.Table], fmt.Sprintf(format, args...)+" over "+transcriptCut(t))
}

func (r *transcript) tailCall(name, format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byTail == nil {
		r.byTail = map[string][]string{}
	}
	r.byTail[name] = append(r.byTail[name], fmt.Sprintf(format, args...))
}

// transcriptCut names the cut of t: its file and how many deltas it has.
func transcriptCut(t views.BaselineTable) string {
	return fmt.Sprintf("%s+%d", filepath.Base(filepath.Dir(filepath.Dir(t.Path))), len(t.DeltaFiles))
}

// take prints what was recorded since the last take, and forgets it.
func (r *transcript) take() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b strings.Builder
	for _, s := range r.server {
		b.WriteString("  source " + s + "\n")
	}
	for _, m := range []struct {
		what string
		by   map[string][]string
	}{{"table", r.byTable}, {"tail", r.byTail}} {
		names := make([]string, 0, len(m.by))
		for n := range m.by {
			names = append(names, n)
		}
		slices.Sort(names)
		for _, n := range names {
			for _, s := range m.by[n] {
				b.WriteString("  " + m.what + " " + n + ": " + s + "\n")
			}
		}
	}
	r.server, r.byTable, r.byTail = nil, nil, nil
	return b.String()
}

// recordingIndex is fakeIndex with every read recorded.
type recordingIndex struct {
	*fakeIndex
	rec *transcript
}

func transcriptTables(tables []views.BaselineTable) string {
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.Table + "@" + transcriptCut(t)
	}
	slices.Sort(names)
	return strings.Join(names, ",")
}

func (x *recordingIndex) Newest(ctx context.Context) (tailMark, error) {
	m, err := x.fakeIndex.Newest(ctx)
	x.rec.serverCall("Newest -> event %d at %s:%d, %v", m.ID, m.Pos.File, m.Pos.Pos, err)
	return m, err
}

func (x *recordingIndex) Holds(ctx context.Context, m tailMark) (bool, error) {
	ok, err := x.fakeIndex.Holds(ctx, m)
	x.rec.serverCall("Holds event %d -> %v, %v", m.ID, ok, err)
	return ok, err
}

func (x *recordingIndex) Start(ctx context.Context, t views.BaselineTable) (tailTable, string, error) {
	tail, why, err := x.fakeIndex.Start(ctx, t)
	x.rec.tableCall(t, "Start -> %q, %v", why, err)
	if tail == nil {
		return tail, why, err
	}
	x.rec.mu.Lock()
	if x.rec.starts == nil {
		x.rec.starts = map[string]int{}
	}
	x.rec.starts[t.Table]++
	name := fmt.Sprintf("%s#%d", t.Table, x.rec.starts[t.Table])
	x.rec.mu.Unlock()
	return &recordingTail{fakeTail: tail.(*fakeTail), rec: x.rec, name: name}, why, err
}

func (x *recordingIndex) Fill(ctx context.Context, t views.BaselineTable, upTo uint64, page func([]query.ResultRow) error) (string, error) {
	why, err := x.fakeIndex.Fill(ctx, t, upTo, page)
	x.rec.tableCall(t, "Fill up to %d -> %q, %v", upTo, why, err)
	return why, err
}

func (x *recordingIndex) Since(ctx context.Context, tables []views.BaselineTable, from, upTo uint64, page func([]query.ResultRow) error) error {
	err := x.fakeIndex.Since(ctx, tables, from, upTo, page)
	x.rec.serverCall("Since (%d, %d] of %s -> %v", from, upTo, transcriptTables(tables), err)
	return err
}

func (x *recordingIndex) Cut(t views.BaselineTable) (tailFloor, string) {
	c, why := x.fakeIndex.Cut(t)
	x.rec.tableCall(t, "Cut -> %s:%d after event %d, %q", c.anchor.File, c.anchor.Pos, c.lastEventID, why)
	return c, why
}

func (x *recordingIndex) BaseFile(t views.BaselineTable) string {
	id := x.fakeIndex.BaseFile(t)
	x.rec.tableCall(t, "BaseFile -> %q", id)
	return id
}

func (x *recordingIndex) Broken(ctx context.Context, tables []views.BaselineTable) (map[string]string, error) {
	changed, err := x.fakeIndex.Broken(ctx, tables)
	x.rec.serverCall("Broken of %s -> %d broken, %v", transcriptTables(tables), len(changed), err)
	return changed, err
}

// recordingTail is fakeTail with every call that changes it or writes a
// file recorded, each file with the keys it holds.
type recordingTail struct {
	*fakeTail
	rec  *transcript
	name string
}

func transcriptIDs(page []query.ResultRow) string {
	ids := make([]string, len(page))
	for i, ev := range page {
		ids[i] = fmt.Sprintf("%d(%s)", ev.EventID, ev.PKValues)
	}
	return strings.Join(ids, " ")
}

func (t *recordingTail) Apply(page []query.ResultRow) error {
	err := t.fakeTail.Apply(page)
	t.rec.tailCall(t.name, "Apply events %s -> %v", transcriptIDs(page), err)
	return err
}

func (t *recordingTail) PruneThrough(anchor query.BinlogPos, afterID uint64) (int, error) {
	n, err := t.fakeTail.PruneThrough(anchor, afterID)
	t.rec.tailCall(t.name, "PruneThrough %s:%d after event %d -> %d dropped, %v", anchor.File, anchor.Pos, afterID, n, err)
	return n, err
}

func transcriptFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return "not written"
	}
	keys := strings.Split(string(b), "\n")
	slices.Sort(keys)
	return "keys " + strings.Join(keys, ",")
}

func (t *recordingTail) WriteFile(path string) (int64, error) {
	n, err := t.fakeTail.WriteFile(path)
	t.rec.tailCall(t.name, "WriteFile %s -> %d rows, %v, %s", filepath.Base(path), n, err, transcriptFile(path))
	return n, err
}

func (t *recordingTail) WriteDelta(path string) (int64, error) {
	n, err := t.fakeTail.WriteDelta(path)
	t.rec.tailCall(t.name, "WriteDelta %s -> %d rows, %v, %s", filepath.Base(path), n, err, transcriptFile(path))
	return n, err
}

// transcriptViews prints what a statement was handed.
func transcriptViews(got map[string]tailView, err error) string {
	if err != nil {
		return fmt.Sprintf("  statement: error %v\n", err)
	}
	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b strings.Builder
	for _, k := range keys {
		v := got[k]
		files := make([]string, len(v.Paths))
		for i, p := range v.Paths {
			files[i] = filepath.Base(p)
		}
		fmt.Fprintf(&b, "  statement: %s reads [%s] by %v through %s (event %d)\n", strings.ReplaceAll(k, "\x00", "."),
			strings.Join(files, " "), v.PK, v.Through.Format("15:04"), v.ThroughID)
	}
	if len(keys) == 0 {
		b.WriteString("  statement: no tail\n")
	}
	return b.String()
}

func TestTailKeeper_anotherSourceIsDrivenAsBefore_3049(t *testing.T) {
	ctx := context.Background()
	rec := &transcript{}
	f := &fakeIndex{anchors: true, cuts: map[string]uint64{}, rowBytes: 100}
	src := &recordingIndex{fakeIndex: f, rec: rec}
	clock := time.Date(2026, 10, 14, 13, 0, 0, 0, time.UTC)
	k := newTailKeeper(src, t.TempDir(), tailBudget{Keys: 7, Bytes: 1500}, time.Minute, time.Hour)
	k.now = func() time.Time { return clock }
	k.retryMin, k.retryMax = 0, 0
	orders, items, users := keeperTable("orders"), keeperTable("items"), keeperTable("users")
	ko, ki := tailKey("shop", "orders"), tailKey("shop", "items")

	var out strings.Builder
	// step is one statement over tables: the update it makes, the builds it
	// started run to their end, and nothing else at the same time.
	step := func(what string, tables ...views.BaselineTable) {
		t.Helper()
		got, err := k.refresh(ctx, tables)
		k.settle()
		clock = clock.Add(time.Second)
		out.WriteString(what + "\n" + rec.take() + transcriptViews(got, err))
	}
	add := func(table string, keys ...string) {
		for _, key := range keys {
			f.add(table, key)
		}
	}

	add("orders", "1", "2", "3")
	add("items", "a", "b")
	add("customers", "9")
	step("the first statement starts both tails", orders, items)
	step("the next one reads them", orders, items)
	add("orders", "4", "1")
	add("items", "a")
	step("changes are folded and written as a delta", orders, items)
	step("nothing new", orders, items)
	for i := 0; i < 9; i++ {
		add("orders", "2")
		step(fmt.Sprintf("one change, write %d of a chain", i+1), orders, items)
	}
	// A newer cut of orders, which holds the index's first five events.
	f.mu.Lock()
	f.cuts[ko] = 5
	f.mu.Unlock()
	orders2 := newerCut(orders)
	step("a newer cut of one table is pruned to", orders2, items)
	add("orders", "5")
	step("and its tail goes on over it", orders2, items)
	// A cut that moved back: another copy, so the tail is built again.
	f.mu.Lock()
	f.cuts[ki] = 1
	f.cutAt = map[string]query.BinlogPos{ki: {File: "binlog.000003", Pos: 1}}
	f.mu.Unlock()
	items2 := newerCut(items)
	step("a cut that is no later point of the same copy starts a build", orders2, items2)
	step("which is read once it is in", orders2, items2)
	// A write that fails keeps the last version.
	add("items", "c")
	f.mu.Lock()
	f.writeErr = errors.New("the disk is full")
	f.mu.Unlock()
	step("a write that fails", orders2, items2)
	f.mu.Lock()
	f.writeErr = nil
	f.mu.Unlock()
	step("and one that works again", orders2, items2)
	// Past its keys budget a tail stops advancing.
	add("orders", "6", "7", "8", "9", "10")
	step("a tail over its budget of keys stops", orders2, items2)
	add("orders", "11")
	add("items", "d")
	step("and stays as it is while the other goes on", orders2, items2)
	// A table that can have no tail, and one whose definition changed.
	f.mu.Lock()
	f.startWhy = map[string]string{tailKey("shop", "users"): "its key is not made of integers"}
	f.mu.Unlock()
	step("a table that can have no tail", items2, users)
	step("is not started again", items2, users)
	f.mu.Lock()
	f.changed = map[string]string{ki: "its definition changed"}
	f.mu.Unlock()
	step("a table whose definition changed loses its tail", items2)
	// The binary log starts again: every tail is dropped and built again.
	f.mu.Lock()
	f.changed = nil
	f.events = append(f.events, query.ResultRow{EventID: uint64(len(f.events) + 1), SchemaName: "shop", TableName: "items", PKValues: "z",
		BinlogFile: "binlog.000001", StartPos: 4, EventTimestamp: f.events[len(f.events)-1].EventTimestamp.Add(time.Minute)})
	f.mu.Unlock()
	step("a source whose log started again", items2)
	step("is read again from its cut", items2)

	got := out.String()
	if os.Getenv("BINTRAIL_PRINT_KEEPER_TRANSCRIPT") != "" {
		t.Logf("\n%s", got)
	}
	if got != keeperTranscript3049 {
		gl, wl := strings.Split(got, "\n"), strings.Split(keeperTranscript3049, "\n")
		for i := 0; i < len(gl) || i < len(wl); i++ {
			var g, w string
			if i < len(gl) {
				g = gl[i]
			}
			if i < len(wl) {
				w = wl[i]
			}
			if g != w {
				t.Fatalf("the keeper of a source that is not PostgreSQL's did something else than it did before, at line %d:\n got  %q\n want %q", i+1, g, w)
			}
		}
	}
}
