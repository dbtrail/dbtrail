package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #3049 at the keeper: real PostgreSQL tails (reconstruct.PGTail) over real
// table files, driven by the keeper, with the index played from memory. The
// table is public.<name> (id, status, note); status stands for a large
// value.

var (
	pgCopyUnsent      = map[string]any{event.UnchangedToastKey: true}
	pgCopyNotCaptured = map[string]any{event.NotCapturedKey: true}
	pgCopyBase        = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
)

// pgCopyCut writes a table file of (id, status, note) rows under dir, cut at
// floor, and returns the table as a statement reads it there.
func pgCopyCut(t *testing.T, dir, table string, floor uint64, rows ...[3]string) views.BaselineTable {
	t.Helper()
	rel, err := baseline.PGRelation{OID: 17001, Columns: []baseline.PGColumn{
		{Name: "id", TypeOID: 25, TypeMod: -1, Type: "text", NotNull: true, Ordinal: 1},
		{Name: "status", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 2},
		{Name: "note", TypeOID: 25, TypeMod: -1, Type: "text", Ordinal: 3},
	}, PrimaryKey: []string{"id"}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	md := map[string]string{
		baseline.MetaKeySnapshotTimestamp: "2026-11-20T11:00:00Z",
		baseline.MetaKeyLSN:               fmt.Sprint(floor),
		baseline.MetaKeyPGRelation:        rel,
		baseline.MetaKeyRenderGUCs:        baseline.RenderGUCsPinned,
		baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
	}
	path := filepath.Join(dir, "public", table+".parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}, {Name: "note", RawText: true}}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r[:], []bool{false, false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return views.BaselineTable{Schema: "public", Table: table, Path: path}
}

// pgCopyIndex is a tailSource and a pgCopySource: the index of a PostgreSQL
// capture played from memory, and the copy read from the real files.
type pgCopyIndex struct {
	mu     sync.Mutex
	events []query.ResultRow
	floors map[string]uint64 // by table file: its cut's LSN
	fills  int
	// copies are the files CopyValues was asked to read, in order; hooks
	// run one per call, before the read, with nothing of the fake's held.
	copies []string
	hooks  []func(ctx context.Context, t views.BaselineTable) error
	real   pgTailSource
}

// change indexes one change of public.<table>, committed at lsn. status is
// a text, or pgCopyUnsent for a value PostgreSQL did not send; an UPDATE's
// earlier row holds its key only, as below REPLICA IDENTITY FULL.
func (f *pgCopyIndex) change(table string, typ event.EventType, lsn uint64, key string, status any, note string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := uint64(len(f.events) + 1)
	ev := query.ResultRow{EventID: id, SchemaName: "public", TableName: table, PKValues: key, EventType: typ,
		BinlogFile: reconstruct.PGLSNText(lsn), StartPos: lsn, EndPos: lsn, EventTimestamp: pgCopyBase.Add(time.Duration(id) * time.Second)}
	keyOnly := map[string]any{"id": key, "status": pgCopyNotCaptured, "note": pgCopyNotCaptured}
	row := map[string]any{"id": key, "status": status, "note": note}
	switch typ {
	case event.EventInsert:
		ev.RowAfter = row
	case event.EventUpdate:
		ev.RowBefore, ev.RowAfter = keyOnly, row
	case event.EventDelete:
		ev.RowBefore = keyOnly
	}
	f.events = append(f.events, ev)
}

func (f *pgCopyIndex) update(table string, lsn uint64, key string, status any, note string) {
	f.change(table, event.EventUpdate, lsn, key, status, note)
}

func (f *pgCopyIndex) lastID() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return uint64(len(f.events))
}

func (f *pgCopyIndex) Newest(ctx context.Context) (tailMark, error) {
	if err := ctx.Err(); err != nil {
		return tailMark{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.events) == 0 {
		return tailMark{}, nil
	}
	last := f.events[len(f.events)-1]
	return tailMark{ID: last.EventID, Pos: reconstruct.PGTailPos(last.StartPos), At: last.EventTimestamp}, nil
}

func (f *pgCopyIndex) Holds(_ context.Context, m tailMark) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return m.ID >= 1 && m.ID <= uint64(len(f.events)) && reconstruct.PGTailPos(f.events[m.ID-1].StartPos) == m.Pos, nil
}

func (f *pgCopyIndex) Start(_ context.Context, t views.BaselineTable) (tailTable, string, error) {
	md, err := baseline.ReadParquetMetadata(t.Path)
	if err != nil {
		return nil, "", err
	}
	pk := []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true}}
	tail, err := reconstruct.NewPGTail(t.Schema, t.Table, md, []string{"id", "status", "note"}, pk, nil)
	if err != nil {
		var r *reconstruct.TailRefusal
		if errors.As(err, &r) {
			return nil, r.Reason, nil
		}
		return nil, "", err
	}
	tail.AwaitCopyValues()
	return tail, "", nil
}

// spelled is the events of tables with from < id <= upTo and an LSN at or
// after each table's floor, as the keeper is handed them.
func (f *pgCopyIndex) spelled(tables []views.BaselineTable, from, upTo uint64, floored bool) []query.ResultRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	floor := map[string]uint64{}
	want := map[string]bool{}
	for _, t := range tables {
		want[t.Table] = true
		if floored {
			floor[t.Table] = f.floors[t.Path]
		}
	}
	var out []query.ResultRow
	for _, ev := range f.events {
		if !want[ev.TableName] || ev.EventID <= from || ev.EventID > upTo || ev.StartPos < floor[ev.TableName] {
			continue
		}
		reconstruct.PGTailEvent(&ev)
		out = append(out, ev)
	}
	return out
}

func (f *pgCopyIndex) Fill(_ context.Context, t views.BaselineTable, upTo uint64, page func([]query.ResultRow) error) (string, error) {
	f.mu.Lock()
	f.fills++
	f.mu.Unlock()
	if evs := f.spelled([]views.BaselineTable{t}, 0, upTo, true); len(evs) > 0 {
		return "", page(evs)
	}
	return "", nil
}

func (f *pgCopyIndex) Since(_ context.Context, tables []views.BaselineTable, from, upTo uint64, page func([]query.ResultRow) error) error {
	if evs := f.spelled(tables, from, upTo, false); len(evs) > 0 {
		return page(evs)
	}
	return nil
}

func (f *pgCopyIndex) Cut(t views.BaselineTable) (tailFloor, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return tailFloor{anchor: reconstruct.PGTailPos(f.floors[t.Path])}, ""
}

func (f *pgCopyIndex) BaseFile(t views.BaselineTable) string { return "pgshape:" + t.Table }

func (f *pgCopyIndex) Broken(context.Context, []views.BaselineTable) (map[string]string, error) {
	return nil, nil
}

func (f *pgCopyIndex) CopyValues(ctx context.Context, t views.BaselineTable, keyCols, keys, cols []string, maxBytes int64) (map[string]map[string]*string, error) {
	f.mu.Lock()
	f.copies = append(f.copies, t.Path)
	var hook func(context.Context, views.BaselineTable) error
	if len(f.hooks) > 0 {
		hook, f.hooks = f.hooks[0], f.hooks[1:]
	}
	f.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, t); err != nil {
			return nil, err
		}
	}
	return f.real.CopyValues(ctx, t, keyCols, keys, cols, maxBytes)
}

func (f *pgCopyIndex) hook(h func(ctx context.Context, t views.BaselineTable) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hooks = append(f.hooks, h)
}

func (f *pgCopyIndex) copied() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.copies...)
}

var (
	_ tailSource   = (*pgCopyIndex)(nil)
	_ pgCopySource = (*pgCopyIndex)(nil)
)

// pgCopyState is tbl as a statement reads it through v: "id=status/note".
func pgCopyState(t *testing.T, tbl views.BaselineTable, v tailView) []string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := pgdelta.StateSQL(tbl.Path, nil, nil)
	if len(v.Paths) > 0 {
		segs := make([]string, len(v.Paths))
		for i, p := range v.Paths {
			segs[i] = "'" + p + "'"
		}
		q = baseline.TableTailChainStateSQL(q, segs, v.PK, "")
	}
	rs, err := db.Query("SELECT id || '=' || coalesce(status, '∅') || '/' || coalesce(note, '∅') FROM (" + q + ") ORDER BY id")
	if err != nil {
		t.Fatalf("%v\n%s", err, q)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var s string
		if err := rs.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// pgCopyRig is a cut of public.orders at 0x500 holding rows 1, 2 and 3, an
// index with one plain change of key 2 at 0x600, and a keeper whose tail of
// the table is built and written: every later change is folded by an update.
type pgCopyRig struct {
	f    *pgCopyIndex
	k    *tailKeeper
	root string
	cut1 views.BaselineTable
	key  string
}

func newPGCopyRig(t *testing.T, budget tailBudget, rows ...[3]string) *pgCopyRig {
	t.Helper()
	if len(rows) == 0 {
		rows = [][3]string{{"1", "one", "note one"}, {"2", "two", "note two"}, {"3", "three", "note three"}}
	}
	r := &pgCopyRig{f: &pgCopyIndex{floors: map[string]uint64{}}, root: t.TempDir(), key: tailKey("public", "orders")}
	r.cut1 = pgCopyCut(t, filepath.Join(r.root, "snap1"), "orders", 0x500, rows...)
	r.f.floors[r.cut1.Path] = 0x500
	r.f.update("orders", 0x600, "2", "paid", "p")
	if budget.Keys == 0 {
		budget.Keys = 1000
	}
	r.k = newTailKeeper(r.f, t.TempDir(), budget, time.Minute, time.Hour)
	r.k.retryMin, r.k.retryMax = 0, 0
	t.Cleanup(r.k.close)
	got := settled(t, context.Background(), r.k, r.cut1)
	if v, ok := got[r.key]; !ok || v.ThroughID != 1 {
		t.Fatalf("setup: the table's tail is not built and written: %+v", got)
	}
	return r
}

func TestPGTailCopy_anUpdateReadsTheCopyWithoutTheLock(t *testing.T) {
	ctx := context.Background()
	r := newPGCopyRig(t, tailBudget{})
	other := pgCopyCut(t, filepath.Join(r.root, "snap1"), "items", 0x500, [3]string{"1", "i", "n"})
	r.f.floors[other.Path] = 0x500
	settled(t, ctx, r.k, r.cut1, other)
	if n := len(r.f.copied()); n != 0 {
		t.Fatalf("the copy was read %d times with nothing waiting", n)
	}

	r.f.update("orders", 0x610, "1", pgCopyUnsent, "n")
	r.f.update("items", 0x620, "1", "ii", "nn")
	locked := true
	r.f.hook(func(context.Context, views.BaselineTable) error {
		if locked = !r.k.mu.TryLock(); !locked {
			r.k.mu.Unlock()
		}
		return nil
	})
	got, err := r.k.refresh(ctx, []views.BaselineTable{r.cut1, other})
	if err != nil {
		t.Fatal(err)
	}
	if locked {
		t.Fatal("the copy was read with the keeper's lock held")
	}
	if !reflect.DeepEqual(r.f.copied(), []string{r.cut1.Path}) {
		t.Fatalf("the copy was read at %v, want once, the cut the tail lies over", r.f.copied())
	}
	v, ok := got[r.key]
	if !ok || v.ThroughID != r.f.lastID() || got[tailKey("public", "items")].ThroughID != v.ThroughID {
		t.Fatalf("the table is not written in the update that folded the change, at the instant of the other: %+v", got)
	}
	if got, want := pgCopyState(t, r.cut1, v), []string{"1=one/n", "2=paid/p", "3=three/note three"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
	// The value is now the tail's: the next change that leaves it asks nothing.
	r.f.update("orders", 0x630, "1", pgCopyUnsent, "n2")
	got, err = r.k.refresh(ctx, []views.BaselineTable{r.cut1, other})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.f.copied()) != 1 {
		t.Fatalf("the copy was read again (%v) for a value the tail holds", r.f.copied())
	}
	if got, want := pgCopyState(t, r.cut1, got[r.key]), []string{"1=one/n2", "2=paid/p", "3=three/note three"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
}

// What a build left waiting (a source that does not read the copy in its
// own Fill, or changes folded while the build caught up) is read by the
// first update after it is installed.
func TestPGTailCopy_whatABuildLeftWaiting(t *testing.T) {
	ctx := context.Background()
	f := &pgCopyIndex{floors: map[string]uint64{}}
	cut := pgCopyCut(t, t.TempDir(), "orders", 0x500, [3]string{"1", "one", "note one"}, [3]string{"2", "two", "note two"})
	f.floors[cut.Path] = 0x500
	f.update("orders", 0x400, "2", "before the cut", "x")
	f.update("orders", 0x600, "1", pgCopyUnsent, "n")
	k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 1000}, time.Minute, time.Hour)
	t.Cleanup(k.close)
	got := settled(t, ctx, k, cut)
	v, ok := got[tailKey("public", "orders")]
	if !ok || !reflect.DeepEqual(f.copied(), []string{cut.Path}) {
		t.Fatalf("after the build: views %+v, the copy read at %v", got, f.copied())
	}
	if got, want := pgCopyState(t, cut, v), []string{"1=one/n", "2=two/note two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
}

// After a prune the copy read is the newer cut: a key's earlier change may
// be in it and nowhere else.
func TestPGTailCopy_afterAPruneTheNewerCutIsRead(t *testing.T) {
	ctx := context.Background()
	r := newPGCopyRig(t, tailBudget{})
	r.f.update("orders", 0x610, "1", "S1", "N1")
	r.f.update("orders", 0x620, "2", "paid again", "p2")
	// Over the older cut a value is read from it.
	r.f.update("orders", 0x628, "3", pgCopyUnsent, "n3")
	settled(t, ctx, r.k, r.cut1)
	if !reflect.DeepEqual(r.f.copied(), []string{r.cut1.Path}) {
		t.Fatalf("before the prune the copy was read at %v, want the older cut", r.f.copied())
	}
	// A snapshot update publishes the table as of 0x618: key 1's change is
	// in it.
	cut2 := pgCopyCut(t, filepath.Join(r.root, "snap2"), "orders", 0x618, [3]string{"1", "S1", "N1"}, [3]string{"2", "paid", "p"}, [3]string{"3", "three", "note three"})
	r.f.floors[cut2.Path] = 0x618
	got := settled(t, ctx, r.k, cut2)
	if r.f.fills != 1 || r.k.tables[r.key].cut != tailCutOf(cut2) {
		t.Fatalf("the tail was not pruned to the newer cut (%d fills, cut %q)", r.f.fills, r.k.tables[r.key].cut)
	}
	if got, want := pgCopyState(t, cut2, got[r.key]), []string{"1=S1/N1", "2=paid again/p2", "3=three/n3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after the prune: state %v, want %v", got, want)
	}
	r.f.update("orders", 0x630, "1", pgCopyUnsent, "n")
	views2, err := r.k.refresh(ctx, []views.BaselineTable{cut2})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.f.copied(), []string{r.cut1.Path, cut2.Path}) {
		t.Fatalf("the copy was read at %v, want the older cut and then the newer one, %s", r.f.copied(), cut2.Path)
	}
	if got, want := pgCopyState(t, cut2, views2[r.key]), []string{"1=S1/n", "2=paid again/p2", "3=three/n3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v: the value is the older cut's", got, want)
	}
	if r.f.fills != 1 {
		t.Fatalf("the tail was built again (%d fills)", r.f.fills)
	}
}

// The cut changes while the copy is read: what was read of the older cut is
// given to no one, and the key is asked again of the newer one. Here the
// older cut's value would be wrong: between the two cuts the row's large
// value changed, and the change that waits now is a later one.
func TestPGTailCopy_theCutChangesWhileTheCopyIsRead(t *testing.T) {
	ctx := context.Background()
	r := newPGCopyRig(t, tailBudget{})
	// A: key 1 changed, its large value left as it was. It waits.
	r.f.update("orders", 0x610, "1", pgCopyUnsent, "a")
	cut2 := pgCopyCut(t, filepath.Join(r.root, "snap2"), "orders", 0x618, [3]string{"1", "b", "a2"}, [3]string{"2", "paid", "p"}, [3]string{"3", "three", "note three"})
	r.f.floors[cut2.Path] = 0x618
	r.f.hook(func(context.Context, views.BaselineTable) error {
		// While the older cut is read for A: B gives key 1 a new large
		// value, and a statement over the newer cut, which holds A and B,
		// prunes the tail to it.
		r.f.update("orders", 0x614, "1", "b", "a2")
		r.f.update("orders", 0x620, "3", "three!", "n3")
		if _, err := r.k.refresh(ctx, []views.BaselineTable{cut2}); err != nil {
			return fmt.Errorf("the statement over the newer cut: %w", err)
		}
		if e := r.k.tableEntry(r.key); e == nil || e.cut != tailCutOf(cut2) {
			return errors.New("the tail was not pruned to the newer cut while the copy was read")
		}
		// C leaves the large value again. It waits, and the update that
		// folds it cannot read the copy in time.
		r.f.update("orders", 0x630, "1", pgCopyUnsent, "c")
		short, cancel := context.WithCancel(ctx)
		defer cancel()
		r.f.hook(func(ctx context.Context, _ views.BaselineTable) error {
			cancel()
			return ctx.Err()
		})
		_, _ = r.k.refresh(short, []views.BaselineTable{cut2})
		return nil
	})
	got, err := r.k.refresh(ctx, []views.BaselineTable{cut2})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{r.cut1.Path, cut2.Path}; !reflect.DeepEqual(r.f.copied(), want) {
		t.Fatalf("the copy was read at %v, want %v", r.f.copied(), want)
	}
	if v, ok := got[r.key]; ok {
		for _, row := range pgCopyState(t, cut2, v) {
			if strings.HasPrefix(row, "1=one/") {
				t.Fatalf("key 1 reads %s: the value read from the older cut was laid over the newer one", row)
			}
			if row == "1=b/c" {
				t.Fatal("key 1 was written before its value was read from the newer cut")
			}
		}
	}
	got, err = r.k.refresh(ctx, []views.BaselineTable{cut2})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{r.cut1.Path, cut2.Path, cut2.Path}; !reflect.DeepEqual(r.f.copied(), want) {
		t.Fatalf("the copy was read at %v, want %v", r.f.copied(), want)
	}
	if got, want := pgCopyState(t, cut2, got[r.key]), []string{"1=b/c", "2=paid/p", "3=three!/n3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
}

// tableEntry is key's entry, read under the lock.
func (k *tailKeeper) tableEntry(key string) *tailEntry {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.tables[key]
}

// What keeps the copy from being read: a refusal with its cause for the
// table, never a panic, never a row with part of its values, and the other
// tables go on. A read only cut short is tried again.
func TestPGTailCopy_theCopyCannotBeRead(t *testing.T) {
	ctx := context.Background()
	for name, c := range map[string]struct {
		budget tailBudget
		rows   [][3]string
		// before runs once the tail is built, before the change that waits.
		before func(t *testing.T, r *pgCopyRig)
		hook   func(ctx context.Context, t views.BaselineTable) error
		says   string
	}{
		"the read fails": {hook: func(context.Context, views.BaselineTable) error { return errors.New("the disk does not answer") },
			says: "could not be read from its copy: the disk does not answer"},
		"the read panics": {hook: func(context.Context, views.BaselineTable) error { panic("a read that panics") },
			says: "could not be read from its copy: panic: a read that panics"},
		"the file no longer reads": {before: func(t *testing.T, r *pgCopyRig) {
			if err := os.WriteFile(r.cut1.Path, []byte("not a parquet file any more"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, says: "could not be read from its copy: "},
		"the copy has no row of the key": {rows: [][3]string{{"2", "two", "note two"}}, says: "the copy has no row under that key"},
	} {
		t.Run(name, func(t *testing.T) {
			r := newPGCopyRig(t, c.budget, c.rows...)
			other := pgCopyCut(t, filepath.Join(r.root, "snap1"), "items", 0x500, [3]string{"1", "i", "n"})
			r.f.floors[other.Path] = 0x500
			before := settled(t, ctx, r.k, r.cut1, other)[r.key]
			if c.before != nil {
				c.before(t, r)
			}
			r.f.update("orders", 0x610, "1", pgCopyUnsent, "n")
			r.f.update("items", 0x620, "1", "ii", "nn")
			if c.hook != nil {
				r.f.hook(c.hook)
			}
			got, err := r.k.refresh(ctx, []views.BaselineTable{r.cut1, other})
			if err != nil {
				t.Fatal(err)
			}
			if _, has := got[r.key]; has {
				t.Fatalf("the table has a tail after its copy could not be read: %+v", got[r.key])
			}
			if e := r.k.tableEntry(r.key); e == nil || !strings.Contains(e.refused, c.says) {
				t.Fatalf("the table's refusal is %q, want one that says %q", r.k.tableEntry(r.key).refused, c.says)
			}
			if v, ok := got[tailKey("public", "items")]; !ok || v.ThroughID != r.f.lastID() {
				t.Fatalf("the other table did not go on: %+v", got)
			}
			// Nothing was written for it: its files are the ones it had.
			for _, p := range before.Paths {
				b, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(b), event.UnchangedToastKey) {
					t.Fatalf("%s holds the marker of a value that was not sent", p)
				}
			}
			entries, err := os.ReadDir(r.k.dir)
			if err != nil {
				t.Fatal(err)
			}
			had := map[string]bool{}
			for _, p := range before.Paths {
				had[filepath.Base(p)] = true
			}
			for _, e := range entries {
				if strings.Contains(e.Name(), "orders") && !had[e.Name()] {
					t.Fatalf("a file was written for the refused table: %s", e.Name())
				}
			}
			// And the keeper goes on: its lock is free, the next update works.
			if !r.k.mu.TryLock() {
				t.Fatal("the keeper's lock was left held")
			}
			r.k.mu.Unlock()
			if _, err := r.k.refresh(ctx, []views.BaselineTable{r.cut1, other}); err != nil {
				t.Fatal(err)
			}
		})
	}

	t.Run("a read cut short is tried again, and the last version stays meanwhile", func(t *testing.T) {
		r := newPGCopyRig(t, tailBudget{})
		r.f.update("orders", 0x610, "1", pgCopyUnsent, "n")
		short, cancel := context.WithCancel(ctx)
		defer cancel()
		r.f.hook(func(ctx context.Context, _ views.BaselineTable) error {
			cancel()
			return ctx.Err()
		})
		got, _ := r.k.refresh(short, []views.BaselineTable{r.cut1})
		if e := r.k.tableEntry(r.key); e == nil || e.refused != "" {
			t.Fatalf("a read cut short refused the table: %+v", e)
		}
		if v, ok := got[r.key]; ok {
			if v.ThroughID != 1 {
				t.Fatalf("the version handed out while a value waits says it is through event %d, want the last one written (1)", v.ThroughID)
			}
			if got, want := pgCopyState(t, r.cut1, v), []string{"1=one/note one", "2=paid/p", "3=three/note three"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("state %v, want the last version %v", got, want)
			}
		}
		got, err := r.k.refresh(ctx, []views.BaselineTable{r.cut1})
		if err != nil {
			t.Fatal(err)
		}
		if v := got[r.key]; v.ThroughID != 2 || !reflect.DeepEqual(pgCopyState(t, r.cut1, v), []string{"1=one/n", "2=paid/p", "3=three/note three"}) {
			t.Fatalf("after the read was tried again: %+v", v)
		}
	})

	t.Run("a read that had the whole time for an update's reads and did not end loses its tail, and one not started waits", func(t *testing.T) {
		was, wasMin := pgCopyReadMax, pgCopyReadMin
		pgCopyReadMax, pgCopyReadMin = 20*time.Millisecond, 0
		t.Cleanup(func() { pgCopyReadMax, pgCopyReadMin = was, wasMin })
		r := newPGCopyRig(t, tailBudget{})
		// items sorts before orders: its read is the one that takes the time.
		slow := pgCopyCut(t, filepath.Join(r.root, "snap1"), "items", 0x500, [3]string{"1", "i", "n"})
		r.f.floors[slow.Path] = 0x500
		settled(t, ctx, r.k, r.cut1, slow)
		r.f.update("items", 0x610, "1", pgCopyUnsent, "n")
		r.f.update("orders", 0x620, "1", pgCopyUnsent, "n")
		r.f.hook(func(ctx context.Context, _ views.BaselineTable) error {
			<-ctx.Done()
			return ctx.Err()
		})
		got, err := r.k.refresh(ctx, []views.BaselineTable{r.cut1, slow})
		if err != nil {
			t.Fatal(err)
		}
		if e := r.k.tableEntry(tailKey("public", "items")); e == nil || !strings.Contains(e.refused, "from its copy did not end in the 20ms") {
			t.Fatalf("the slow table's refusal is %q", r.k.tableEntry(tailKey("public", "items")).refused)
		}
		if e := r.k.tableEntry(r.key); e == nil || e.refused != "" {
			t.Fatalf("the table whose read was not started was refused: %+v", e)
		}
		if v, ok := got[r.key]; !ok || v.ThroughID != 1 {
			t.Fatalf("the table whose read was not started does not keep its last version: %+v", got)
		}
		if !reflect.DeepEqual(r.f.copied(), []string{slow.Path}) {
			t.Fatalf("the copy was read at %v, want the slow table's alone", r.f.copied())
		}
		// With the time a read really has, the next update reads it.
		pgCopyReadMax, pgCopyReadMin = was, wasMin
		got, err = r.k.refresh(ctx, []views.BaselineTable{r.cut1, slow})
		if err != nil {
			t.Fatal(err)
		}
		if v := got[r.key]; v.ThroughID != r.f.lastID() || !reflect.DeepEqual(pgCopyState(t, r.cut1, v), []string{"1=one/n", "2=paid/p", "3=three/note three"}) {
			t.Fatalf("at the next update the other table is not read: %+v", v)
		}
	})
}

// pgCopyTimes sets the times an update gives its reads, for one test.
func pgCopyTimes(t *testing.T, max, margin, least time.Duration) {
	t.Helper()
	wasMax, wasMargin, wasMin := pgCopyReadMax, pgCopyReadMargin, pgCopyReadMin
	pgCopyReadMax, pgCopyReadMargin, pgCopyReadMin = max, margin, least
	t.Cleanup(func() { pgCopyReadMax, pgCopyReadMargin, pgCopyReadMin = wasMax, wasMargin, wasMin })
}

// The memory the tails may hold: with no room left a read waits for a later
// update and its table keeps its tail, whoever took the room; only a table
// that does not fit the whole budget alone is refused.
func TestPGTailCopy_noRoomWaitsAndOnlyATableTooLargeAloneIsRefused(t *testing.T) {
	ctx := context.Background()
	large := strings.Repeat("x", 600<<10)

	t.Run("alone and larger than the whole budget: refused", func(t *testing.T) {
		r := newPGCopyRig(t, tailBudget{Bytes: 1 << 20}, [3]string{"1", strings.Repeat("x", 2<<20), "note one"}, [3]string{"2", "two", "note two"})
		// No other table's tail is held: the room is the whole budget less
		// what this tail holds itself.
		r.f.update("orders", 0x610, "1", pgCopyUnsent, "n")
		got, err := r.k.refresh(ctx, []views.BaselineTable{r.cut1})
		if err != nil {
			t.Fatal(err)
		}
		e := r.k.tableEntry(r.key)
		if _, has := got[r.key]; has || e == nil || !strings.Contains(e.refused, "do not fit the tails' memory budget, with no other table's changes held (1 MiB, BINTRAIL_SQL_TAIL_MAX_BYTES)") {
			t.Fatalf("view %v, refusal %q", has, e.refused)
		}
	})

	t.Run("the first read takes the room: the second waits, is not read again for nothing, and is read once there is room", func(t *testing.T) {
		r := newPGCopyRig(t, tailBudget{Bytes: 1 << 20}, [3]string{"1", large, "note one"}, [3]string{"2", "two", "note two"})
		items := pgCopyCut(t, filepath.Join(r.root, "snap1"), "items", 0x500, [3]string{"1", large, "n"})
		r.f.floors[items.Path] = 0x500
		both := []views.BaselineTable{r.cut1, items}
		before := settled(t, ctx, r.k, both...)[r.key]
		r.f.update("items", 0x610, "1", pgCopyUnsent, "n")
		r.f.update("orders", 0x620, "1", pgCopyUnsent, "n")
		got, err := r.k.refresh(ctx, both)
		if err != nil {
			t.Fatal(err)
		}
		ki := tailKey("public", "items")
		if v, ok := got[ki]; !ok || v.ThroughID != r.f.lastID() {
			t.Fatalf("the table read first was not written: %+v", got)
		}
		e := r.k.tableEntry(r.key)
		if e == nil || e.refused != "" {
			t.Fatalf("the table that found no room was refused: %q", e.refused)
		}
		if v, ok := got[r.key]; !ok || v.ThroughID != before.ThroughID {
			t.Fatalf("the table that found no room does not keep its last version: %+v", got[r.key])
		}
		if times, room := e.tail.(pgCopyTail).CopyDeferred(); times != 1 || room <= 0 {
			t.Fatalf("the tail says it was left for later %d times, with room %d", times, room)
		}
		if want := []string{items.Path, r.cut1.Path}; !reflect.DeepEqual(r.f.copied(), want) {
			t.Fatalf("the copy was read at %v, want %v", r.f.copied(), want)
		}
		// No more room than a read already found too small: the copy is not
		// read again at every update to find that out. (One more read is
		// allowed for: the first one's values, put away, count a little less
		// than they did as they came in.)
		for range 3 {
			if _, err := r.k.refresh(ctx, both); err != nil {
				t.Fatal(err)
			}
		}
		settledAt := len(r.f.copied())
		for range 3 {
			if _, err := r.k.refresh(ctx, both); err != nil {
				t.Fatal(err)
			}
		}
		if n := len(r.f.copied()); n != settledAt || n > 3 {
			t.Fatalf("the copy was read %d times, %d of them in the last three updates; with no more room than a read found too small it is not read again", n, n-settledAt)
		}
		if e := r.k.tableEntry(r.key); e.refused != "" {
			t.Fatalf("waiting refused the table: %q", e.refused)
		}
		// Room: the read is made, and the table is current again.
		r.k.mu.Lock()
		r.k.budget.Bytes = 8 << 20
		r.k.mu.Unlock()
		got, err = r.k.refresh(ctx, both)
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := got[r.key]; !ok || v.ThroughID != r.f.lastID() || len(r.f.copied()) != settledAt+1 {
			t.Fatalf("with room again: view %+v, the copy read %d times", got[r.key], len(r.f.copied()))
		}
		if rows := pgCopyState(t, r.cut1, got[r.key]); len(rows) != 2 || rows[0] != "1="+large+"/n" {
			t.Fatalf("the row does not hold its value from the copy (%d rows)", len(rows))
		}
	})

	t.Run("no room at all, as when another tail is over the budget: the read waits", func(t *testing.T) {
		r := newPGCopyRig(t, tailBudget{Bytes: 1 << 20})
		// A key waits: its read was cut short.
		r.f.update("orders", 0x610, "1", pgCopyUnsent, "n")
		short, cancel := context.WithCancel(ctx)
		r.f.hook(func(ctx context.Context, _ views.BaselineTable) error {
			cancel()
			return ctx.Err()
		})
		_, _ = r.k.refresh(short, []views.BaselineTable{r.cut1})
		cancel()
		// The tails now hold more than their budget (what a tail that
		// stopped over it leaves: it is still held).
		r.k.mu.Lock()
		r.k.budget.Bytes = 1
		r.k.mu.Unlock()
		got, err := r.k.refresh(ctx, []views.BaselineTable{r.cut1})
		if err != nil {
			t.Fatal(err)
		}
		e := r.k.tableEntry(r.key)
		if e.refused != "" {
			t.Fatalf("with no room in the budget the table was refused: %q", e.refused)
		}
		if v, ok := got[r.key]; !ok || v.ThroughID != 1 || len(r.f.copied()) != 1 {
			t.Fatalf("view %+v, the copy read %d times; want the last version and no read", got[r.key], len(r.f.copied()))
		}
		r.k.mu.Lock()
		r.k.budget.Bytes = 1 << 20
		r.k.mu.Unlock()
		got, err = r.k.refresh(ctx, []views.BaselineTable{r.cut1})
		if err != nil {
			t.Fatal(err)
		}
		if v := got[r.key]; v.ThroughID != r.f.lastID() || !reflect.DeepEqual(pgCopyState(t, r.cut1, v), []string{"1=one/n", "2=paid/p", "3=three/note three"}) {
			t.Fatalf("with room again: %+v", v)
		}
	})
}

// An update with little time left starts no read, ends in time, and
// publishes the tables that wait for nothing.
func TestPGTailCopy_anUpdateShortOfTimeStillPublishesTheOtherTables(t *testing.T) {
	ctx := context.Background()
	r := newPGCopyRig(t, tailBudget{})
	items := pgCopyCut(t, filepath.Join(r.root, "snap1"), "items", 0x500, [3]string{"1", "i", "n"})
	r.f.floors[items.Path] = 0x500
	both := []views.BaselineTable{r.cut1, items}
	settled(t, ctx, r.k, both...)
	r.f.update("orders", 0x610, "1", pgCopyUnsent, "n")
	r.f.update("items", 0x620, "1", "ii", "nn")
	// As a statement's own update: two seconds in all. The reads may not run
	// into the last second and a half, and need a second at least.
	pgCopyTimes(t, 10*time.Second, 1500*time.Millisecond, time.Second)
	short, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	began := time.Now()
	got, err := r.k.refresh(short, both)
	if err != nil {
		t.Fatalf("the update failed for every table: %v", err)
	}
	if short.Err() != nil || time.Since(began) > time.Second {
		t.Fatalf("the update took %s of its two seconds", time.Since(began))
	}
	if n := len(r.f.copied()); n != 0 {
		t.Fatalf("a read was started with too little time for it (%d)", n)
	}
	if v, ok := got[tailKey("public", "items")]; !ok || v.ThroughID != r.f.lastID() {
		t.Fatalf("the table that waits for nothing was not published: %+v", got)
	}
	e := r.k.tableEntry(r.key)
	if v, ok := got[r.key]; e.refused != "" || !ok || v.ThroughID != 1 {
		t.Fatalf("the table whose read was not started: refusal %q, view %+v; want its last version", e.refused, got[r.key])
	}
	// A read cut short by the nearer deadline is left for later too, not
	// refused: it never had the whole time to itself.
	pgCopyTimes(t, 10*time.Second, 1500*time.Millisecond, 0)
	short2, cancel2 := context.WithTimeout(ctx, 1700*time.Millisecond)
	defer cancel2()
	r.f.hook(func(ctx context.Context, _ views.BaselineTable) error {
		<-ctx.Done()
		return ctx.Err()
	})
	got, err = r.k.refresh(short2, both)
	if err != nil || short2.Err() != nil {
		t.Fatalf("the update whose read was cut short: %v (its own deadline: %v)", err, short2.Err())
	}
	if e := r.k.tableEntry(r.key); e.refused != "" {
		t.Fatalf("a read cut short by the update's nearer deadline refused the table: %q", e.refused)
	}
	if _, ok := got[tailKey("public", "items")]; !ok {
		t.Fatalf("the other table was not published: %+v", got)
	}
	// With time, it is read.
	got, err = r.k.refresh(ctx, both)
	if err != nil {
		t.Fatal(err)
	}
	if v := got[r.key]; v.ThroughID != r.f.lastID() || !reflect.DeepEqual(pgCopyState(t, r.cut1, v), []string{"1=one/n", "2=paid/p", "3=three/note three"}) {
		t.Fatalf("with time again: %+v", v)
	}
}

// The tails that waited longest are read first: the order of the tables'
// names does not leave the same one for later at every update.
func TestPGTailCopy_theTailThatWaitedIsReadFirst(t *testing.T) {
	ctx := context.Background()
	r := newPGCopyRig(t, tailBudget{})
	items := pgCopyCut(t, filepath.Join(r.root, "snap1"), "items", 0x500, [3]string{"1", "i", "n"}, [3]string{"2", "j", "m"})
	r.f.floors[items.Path] = 0x500
	both := []views.BaselineTable{r.cut1, items}
	settled(t, ctx, r.k, both...)
	r.f.update("items", 0x610, "1", pgCopyUnsent, "n")
	r.f.update("orders", 0x620, "1", pgCopyUnsent, "n")
	// Three seconds for the reads, and a read needs one and a half: the
	// first one takes two.
	pgCopyTimes(t, 3*time.Second, 0, 1500*time.Millisecond)
	r.f.hook(func(context.Context, views.BaselineTable) error {
		time.Sleep(2 * time.Second)
		return nil
	})
	if _, err := r.k.refresh(ctx, both); err != nil {
		t.Fatal(err)
	}
	if want := []string{items.Path}; !reflect.DeepEqual(r.f.copied(), want) {
		t.Fatalf("the copy was read at %v, want items alone (first by name, and the time went on it)", r.f.copied())
	}
	if e := r.k.tableEntry(r.key); e.refused != "" {
		t.Fatalf("the table whose read was not started was refused: %q", e.refused)
	}
	// Both wait again: the one left for later goes first.
	r.f.update("items", 0x630, "2", pgCopyUnsent, "m2")
	got, err := r.k.refresh(ctx, both)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{items.Path, r.cut1.Path, items.Path}; !reflect.DeepEqual(r.f.copied(), want) {
		t.Fatalf("the copy was read at %v, want %v", r.f.copied(), want)
	}
	if v := got[r.key]; v.ThroughID != r.f.lastID() {
		t.Fatalf("the table that waited is not current: %+v", v)
	}
}

// No source but PostgreSQL's reads the copy, and no tail but PostgreSQL's
// waits for it: the keeper's step is for them alone.
func TestPGTailCopy_onlyAPostgresSourceReadsTheCopy(t *testing.T) {
	for name, src := range map[string]any{"the MySQL source": &indexTailSource{}, "the keeper tests' source": &fakeIndex{}} {
		if _, is := src.(pgCopySource); is {
			t.Errorf("%s reads the copy for its tails' values", name)
		}
	}
	for name, tail := range map[string]any{"a MySQL tail": &reconstruct.Tail{}, "the keeper tests' tail": &fakeTail{}} {
		if _, is := tail.(pgCopyTail); is {
			t.Errorf("%s waits for values from the copy", name)
		}
	}
	if _, is := any(&pgTailSource{}).(pgCopySource); !is {
		t.Error("the PostgreSQL source does not read the copy")
	}
}

// A build reads the copy at the end of its own Fill, for the tail its Start
// built over that cut and no other.
func TestPGTailSource_fillReadsTheCopyForTheTailItStarted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cut := pgCopyCut(t, filepath.Join(dir, "snap1"), "orders", 0x500, [3]string{"1", "one", "note one"}, [3]string{"2", "", "note two"})
	other := pgCopyCut(t, filepath.Join(dir, "snap2"), "orders", 0x600, [3]string{"1", "uno", "nota"})
	f := &pgCopyIndex{floors: map[string]uint64{}}
	waiting := func(key string) *reconstruct.PGTail {
		t.Helper()
		tail, _, err := f.Start(ctx, cut)
		if err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		f.events = nil
		f.mu.Unlock()
		f.update("orders", 0x700, key, pgCopyUnsent, "n")
		if err := tail.Apply(f.spelled([]views.BaselineTable{cut}, 0, 1, false)); err != nil {
			t.Fatal(err)
		}
		return tail.(*reconstruct.PGTail)
	}
	x := &pgTailSource{}
	tail := waiting("1")
	x.noteStarted(cut, tail)
	if got := x.takeStarted(other); got != nil {
		t.Fatal("the tail started for one cut was taken for another")
	}
	started := x.takeStarted(cut)
	if started != tail || x.takeStarted(cut) != nil {
		t.Fatal("the tail started for the cut is not taken once")
	}
	if why, err := x.fillStartedFromCopy(ctx, cut, started); why != "" || err != nil {
		t.Fatalf("the read at the end of Fill: %q, %v", why, err)
	}
	if keys, _ := tail.CopyWants(); len(keys) != 0 {
		t.Fatalf("after the read the tail still waits for %v", keys)
	}
	file := filepath.Join(dir, "1-orders.parquet")
	if _, err := tail.WriteFile(file); err != nil {
		t.Fatal(err)
	}
	if got, want := pgCopyState(t, cut, tailView{Paths: []string{file}, PK: tail.PKColumns()}), []string{"1=one/n", "2=/note two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
	// No tail started, or nothing waiting: nothing is read.
	if why, err := x.fillStartedFromCopy(ctx, cut, nil); why != "" || err != nil {
		t.Fatalf("with no tail started: %q, %v", why, err)
	}
	// What the copy cannot give refuses the build; a read over the budget
	// too; a read cut short is an error, tried again with the build.
	if why, err := x.fillStartedFromCopy(ctx, cut, waiting("9")); err != nil || !strings.Contains(why, "the copy has no row under that key") {
		t.Fatalf("a key the copy does not hold: %q, %v", why, err)
	}
	if why, err := x.fillStartedFromCopy(ctx, cut, waiting("2")); err != nil || !strings.Contains(why, "a NULL or an empty text") {
		t.Fatalf("an empty text in the copy: %q, %v", why, err)
	}
	small := &pgTailSource{copyBytes: 2}
	if why, err := small.fillStartedFromCopy(ctx, cut, waiting("1")); err != nil || !strings.Contains(why, "memory budget") {
		t.Fatalf("more text than the budget: %q, %v", why, err)
	}
	ended, cancel := context.WithCancel(ctx)
	cancel()
	if why, err := x.fillStartedFromCopy(ended, cut, waiting("1")); err == nil || why != "" {
		t.Fatalf("a read cut short: %q, %v; want an error and no refusal", why, err)
	}
}
