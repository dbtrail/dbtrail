package console

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// pgChainIndex is pgCopyIndex with the capability of keeping a chain across a
// prune, as the PostgreSQL source declares it.
type pgChainIndex struct {
	*pgCopyIndex
	refuse map[string]bool // by table file: a cut the chain may not cross
}

func (f *pgChainIndex) KeepsChainOver(_, t views.BaselineTable) bool { return !f.refuse[t.Path] }

var _ pgChainSource = (*pgChainIndex)(nil)

func newPGChainRig(t *testing.T) (*pgChainIndex, *tailKeeper, views.BaselineTable, string) {
	t.Helper()
	f := &pgChainIndex{pgCopyIndex: &pgCopyIndex{floors: map[string]uint64{}}, refuse: map[string]bool{}}
	root := t.TempDir()
	cut := pgCopyCut(t, filepath.Join(root, "snap1"), "orders", 0x500,
		[3]string{"1", "one", "n1"}, [3]string{"2", "two", "n2"}, [3]string{"3", "three", "n3"})
	f.floors[cut.Path] = 0x500
	f.update("orders", 0x600, "2", "paid", "p")
	k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 1000}, time.Minute, time.Hour)
	k.retryMin, k.retryMax = 0, 0
	t.Cleanup(k.close)
	settled(t, context.Background(), k, cut)
	return f, k, cut, root
}

func pgChainNextCut(t *testing.T, f *pgChainIndex, root, dir string, floor uint64, rows ...[3]string) views.BaselineTable {
	t.Helper()
	c := pgCopyCut(t, filepath.Join(root, dir), "orders", floor, rows...)
	f.floors[c.Path] = floor
	return c
}

// Only PostgreSQL's source and tail declare it: for a MySQL source the keeper
// takes the path it always took.
func TestPGTailChain_onlyPostgresDeclaresIt(t *testing.T) {
	for name, src := range map[string]any{"the MySQL source": &indexTailSource{}, "the keeper tests' source": &fakeIndex{}} {
		if _, is := src.(pgChainSource); is {
			t.Errorf("%s keeps a chain across a prune", name)
		}
	}
	for name, tail := range map[string]any{"a MySQL tail": &reconstruct.Tail{}, "the keeper tests' tail": &fakeTail{}} {
		if _, is := tail.(pgChainTail); is {
			t.Errorf("%s keeps a chain across a prune", name)
		}
	}
	if _, is := any(&pgTailSource{}).(pgChainSource); !is {
		t.Error("the PostgreSQL source does not declare it")
	}
	if _, is := any(&reconstruct.PGTail{}).(pgChainTail); !is {
		t.Error("the PostgreSQL tail does not declare it")
	}
}

// A prune writes what is pending as one more file, drops the files wholly
// below the cut, and writes nothing whole.
func TestPGTailChain_aPruneKeepsTheChain(t *testing.T) {
	ctx := context.Background()
	f, k, cut, root := newPGChainRig(t)
	key := tailKey("public", "orders")
	f.update("orders", 0x700, "1", "uno", "u")
	settled(t, ctx, k, cut)
	f.update("orders", 0x800, "3", "tres", "t") // pending at the prune

	next := pgChainNextCut(t, f, root, "snap2", 0x750,
		[3]string{"1", "uno", "u"}, [3]string{"2", "paid", "p"}, [3]string{"3", "three", "n3"})
	before := k.rowsWritten
	got, err := k.refresh(ctx, []views.BaselineTable{next})
	if err != nil {
		t.Fatal(err)
	}
	v, ok := got[key]
	if !ok {
		t.Fatal("no view over the newer cut")
	}
	if n := k.rowsWritten - before; n != 1 {
		t.Fatalf("the prune wrote %d rows, want the 1 pending", n)
	}
	e := k.tables[key]
	if e.fileCut != tailCutOf(next) || e.tail.Keys() != 1 {
		t.Fatalf("after the prune: fileCut %q, %d keys", e.fileCut, e.tail.Keys())
	}
	want := []string{"1=uno/u", "2=paid/p", "3=tres/t"}
	if s := pgCopyState(t, next, v); !reflect.DeepEqual(s, want) {
		t.Fatalf("over the newer cut: %v, want %v", s, want)
	}
	// The chain goes on with a delta.
	f.update("orders", 0x900, "2", "dos", "d")
	before = k.rowsWritten
	got, err = k.refresh(ctx, []views.BaselineTable{next})
	if err != nil {
		t.Fatal(err)
	}
	if n := k.rowsWritten - before; n != 1 {
		t.Fatalf("the write after the prune took %d rows, want 1", n)
	}
	want = []string{"1=uno/u", "2=dos/d", "3=tres/t"}
	if s := pgCopyState(t, next, got[key]); !reflect.DeepEqual(s, want) {
		t.Fatalf("after the next change: %v, want %v", s, want)
	}
}

// A cut the source says the chain may not cross takes today's path: a whole
// file over the newer cut.
func TestPGTailChain_aCutThatRefusesTakesTheWholeWrite(t *testing.T) {
	ctx := context.Background()
	f, k, cut, root := newPGChainRig(t)
	key := tailKey("public", "orders")
	f.update("orders", 0x700, "1", "uno", "u")
	settled(t, ctx, k, cut)
	next := pgChainNextCut(t, f, root, "snap2", 0x650,
		[3]string{"1", "one", "n1"}, [3]string{"2", "paid", "p"}, [3]string{"3", "three", "n3"})
	f.refuse[next.Path] = true
	got, err := k.refresh(ctx, []views.BaselineTable{next})
	if err != nil {
		t.Fatal(err)
	}
	if e := k.tables[key]; len(e.segs) != 1 || e.fileCut != tailCutOf(next) {
		t.Fatalf("%d files, fileCut %q: want one whole file over the newer cut", len(e.segs), e.fileCut)
	}
	if s, want := pgCopyState(t, next, got[key]), []string{"1=uno/u", "2=paid/p", "3=three/n3"}; !reflect.DeepEqual(s, want) {
		t.Fatalf("%v, want %v", s, want)
	}
}

// A pending change that cannot be written (its large value is not read yet)
// leaves the chain as it was and takes today's path; the answer stays right.
func TestPGTailChain_aFlushThatFailsTakesTodaysPath(t *testing.T) {
	ctx := context.Background()
	f, k, _, root := newPGChainRig(t)
	key := tailKey("public", "orders")
	f.update("orders", 0x800, "1", pgCopyUnsent, "u")
	f.hook(func(context.Context, views.BaselineTable) error { return errPGCopyReadLater })
	next := pgChainNextCut(t, f, root, "snap2", 0x700,
		[3]string{"1", "one", "n1"}, [3]string{"2", "paid", "p"}, [3]string{"3", "three", "n3"})
	if _, err := k.refresh(ctx, []views.BaselineTable{next}); err != nil {
		t.Fatal(err)
	}
	e := k.tables[key]
	if e.cut != tailCutOf(next) || e.fileCut == tailCutOf(next) {
		t.Fatalf("cut %q, fileCut %q: the chain must not have moved to the newer cut", e.cut, e.fileCut)
	}
	// The read works now: the next write is whole, over the newer cut.
	got, err := k.refresh(ctx, []views.BaselineTable{next})
	if err != nil {
		t.Fatal(err)
	}
	if s, want := pgCopyState(t, next, got[key]), []string{"1=one/u", "2=paid/p", "3=three/n3"}; !reflect.DeepEqual(s, want) {
		t.Fatalf("%v, want %v", s, want)
	}
}

// The chain crosses only cuts of one read of the source. A newer full read
// may lack a row a TRUNCATE removed with no row event, so a row of the chain
// from before it would come back.
func TestPGTailSource_keepsChainOver(t *testing.T) {
	x := &pgTailSource{s: &Server{}}
	dir := t.TempDir()
	const read = "2026-11-20T11:00:00Z"
	file := func(name string, mutate func(map[string]string)) views.BaselineTable {
		return pgCopyCutMeta(t, filepath.Join(dir, name), 0x500, mutate)
	}
	dump := file("dump", nil)
	folded := file("folded", func(m map[string]string) {
		m[baseline.MetaKeySnapshotProducer] = baseline.ProducerReconstruct
		m[baseline.MetaKeyLastDumpAt] = read
		m[baseline.MetaKeyFoldGeneration] = "1"
	})
	again := file("again", func(m map[string]string) { m[baseline.MetaKeySnapshotTimestamp] = "2026-11-20T12:00:00Z" })
	foldedAgain := file("folded-again", func(m map[string]string) {
		m[baseline.MetaKeySnapshotProducer] = baseline.ProducerReconstruct
		m[baseline.MetaKeyLastDumpAt] = "2026-11-20T12:00:00Z"
		m[baseline.MetaKeyFoldGeneration] = "1"
	})
	unknown := file("unknown", func(m map[string]string) { m[baseline.MetaKeySnapshotProducer] = baseline.ProducerReconstruct })
	gone := views.BaselineTable{Schema: "public", Table: "orders", Path: filepath.Join(dir, "gone.parquet")}
	for _, c := range []struct {
		name     string
		old, now views.BaselineTable
		want     bool
	}{
		{"a fold of the same read", dump, folded, true},
		{"a newer full read", dump, again, false},
		{"a fold over a newer full read", folded, foldedAgain, false},
		{"a file whose read is not known", dump, unknown, false},
		{"an older file whose read is not known", unknown, unknown, false},
		{"a newer file that cannot be read", dump, gone, false},
		{"an older file that cannot be read", gone, dump, false},
	} {
		if got := x.KeepsChainOver(c.old, c.now); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// A file with no rows does not stop the retiring of the files before it.
func TestPGTailChain_anEmptyFileDoesNotHoldTheChain(t *testing.T) {
	_, k, cut, _ := newPGChainRig(t)
	e := k.tables[tailKey("public", "orders")]
	e.segs = append(slices.Clip(e.segs), "empty.parquet")
	k.pgNoteSegment(e, "empty.parquet", 0)
	if got := e.segHigh["empty.parquet"]; got.high == 0 || reconstruct.PGChainFilesToRetire([]uint64{got.high}, 0x500) != 1 {
		t.Fatalf("an empty file is recorded as %+v over %s", got, cut.Path)
	}
}

func pgCopyCutMeta(t *testing.T, dir string, floor uint64, mutate func(map[string]string)) views.BaselineTable {
	t.Helper()
	plain := pgCopyCut(t, dir, "plain", floor, [3]string{"1", "a", "b"})
	md, err := baseline.ReadParquetMetadata(plain.Path)
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]string{
		baseline.MetaKeySnapshotTimestamp: "2026-11-20T11:00:00Z",
		baseline.MetaKeyLSN:               fmt.Sprint(floor),
		baseline.MetaKeyPGRelation:        md.PGRelation,
		baseline.MetaKeyRenderGUCs:        baseline.RenderGUCsPinned,
		baseline.MetaKeyLockMode:          baseline.LockStampPGRepeatableRead,
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
	}
	if mutate != nil {
		mutate(meta)
	}
	path := filepath.Join(dir, "public", "orders.parquet")
	cols := []baseline.Column{{Name: "id", RawText: true}, {Name: "status", RawText: true}, {Name: "note", RawText: true}}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "a", "b"}, []bool{false, false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return views.BaselineTable{Schema: "public", Table: "orders", Path: path}
}

// At eight files the newest ones are merged: nothing is written whole, the
// chain stays short, and the answer is the same.
func TestPGTailChain_eightFilesMergeTheNewest(t *testing.T) {
	ctx := context.Background()
	f, k, cut, _ := newPGChainRig(t)
	key := tailKey("public", "orders")
	lsn := uint64(0x700)
	var got map[string]tailView
	for i := range 30 {
		lsn += 0x10
		f.update("orders", lsn, fmt.Sprint(10+i%12), fmt.Sprintf("v%d", i), "n")
		var err error
		if got, err = k.refresh(ctx, []views.BaselineTable{cut}); err != nil {
			t.Fatal(err)
		}
		if n := len(k.tables[key].segs); n > tailMaxSegments {
			t.Fatalf("write %d: %d files", i, n)
		}
	}
	e := k.tables[key]
	var sum int64
	for _, p := range e.segs {
		sum += e.segHigh[p].rows
	}
	if sum != e.segRows {
		t.Fatalf("the chain's rows are %d by file and %d in all", sum, e.segRows)
	}
	want := map[string]string{"1": "1=one/n1", "2": "2=paid/p", "3": "3=three/n3"}
	for i := range 30 {
		want[fmt.Sprint(10+i%12)] = fmt.Sprintf("%d=v%d/n", 10+i%12, i)
	}
	state := pgCopyState(t, cut, got[key])
	if len(state) != len(want) {
		t.Fatalf("%d rows, want %d: %v", len(state), len(want), state)
	}
	for _, line := range state {
		id, _, _ := strings.Cut(line, "=")
		if want[id] != line {
			t.Fatalf("key %s reads %q, want %q", id, line, want[id])
		}
	}
}

// A merge never takes in a file from before a prune: the keys that prune
// dropped are in no later write, and would be lost with the file.
func TestPGTailChain_aMergeNeverSpansAPrune(t *testing.T) {
	ctx := context.Background()
	f, k, cut, root := newPGChainRig(t)
	key := tailKey("public", "orders")
	f.update("orders", 0x700, "1", "uno", "u")
	f.update("orders", 0x900, "3", "tres", "t")
	settled(t, ctx, k, cut)
	// The cut holds key 1's change and not key 3's, so the file that holds
	// both stays, and key 1 leaves memory.
	next := pgChainNextCut(t, f, root, "snap2", 0x800,
		[3]string{"1", "uno", "u"}, [3]string{"2", "paid", "p"}, [3]string{"3", "three", "n3"})
	if _, err := k.refresh(ctx, []views.BaselineTable{next}); err != nil {
		t.Fatal(err)
	}
	e := k.tables[key]
	if e.pgPruned != len(e.segs) || e.pgPruned == 0 {
		t.Fatalf("%d files before the last prune of %d", e.pgPruned, len(e.segs))
	}
	first := e.segs[0]
	lsn := uint64(0xA00)
	var got map[string]tailView
	for i := range 20 {
		lsn += 0x10
		f.update("orders", lsn, "2", fmt.Sprintf("d%d", i), "n")
		var err error
		if got, err = k.refresh(ctx, []views.BaselineTable{next}); err != nil {
			t.Fatal(err)
		}
		if e.pgPruned > 0 && e.segs[0] != first {
			t.Fatalf("write %d: a merge took a file from before the prune", i)
		}
	}
	if s, want := pgCopyState(t, next, got[key]), []string{"1=uno/u", "2=d19/n", "3=tres/t"}; !reflect.DeepEqual(s, want) {
		t.Fatalf("%v, want %v", s, want)
	}
}

// costTail counts what a real tail writes and why it wrote it whole.
type costTail struct {
	*reconstruct.PGTail
	k     *tailKeeper
	key   string
	costs *tailCosts
}

type tailCosts struct {
	first, prune, eight, ratio, merges, deltaRows, wholeRows, mergeRows int64
}

func (c *costTail) WriteSince(path string, afterID uint64) (int64, error) {
	n, err := c.PGTail.WriteSince(path, afterID)
	c.costs.merges++
	c.costs.mergeRows += n
	return n, err
}

func (c *costTail) WriteFile(path string) (int64, error) {
	if e := c.k.tables[c.key]; e == nil || len(e.segs) == 0 {
		c.costs.first++
	} else if e.fileCut != e.cut {
		c.costs.prune++
	} else if len(e.segs) >= tailMaxSegments {
		c.costs.eight++
	} else {
		c.costs.ratio++
	}
	n, err := c.PGTail.WriteFile(path)
	c.costs.wholeRows += n
	return n, err
}

func (c *costTail) WriteDelta(path string) (int64, error) {
	n, err := c.PGTail.WriteDelta(path)
	c.costs.deltaRows += n
	return n, err
}

type costIndex struct {
	*pgCopyIndex
	k     **tailKeeper
	costs *tailCosts
}

func (f *costIndex) Start(ctx context.Context, t views.BaselineTable) (tailTable, string, error) {
	tail, why, err := f.pgCopyIndex.Start(ctx, t)
	if err != nil || why != "" {
		return tail, why, err
	}
	return &costTail{PGTail: tail.(*reconstruct.PGTail), k: *f.k, key: tailKey(t.Schema, t.Table), costs: f.costs}, "", nil
}

type costChainIndex struct{ *costIndex }

func (f *costChainIndex) KeepsChainOver(_, _ views.BaselineTable) bool { return true }

// TestPGTailChainCost is the measurement of the PR, run by hand on the test
// machine (BT_TAIL_COST=1): a synthetic load on the real keeper and real
// files, the same with and without the chain kept across a prune.
func TestPGTailChainCost(t *testing.T) {
	if os.Getenv("BT_TAIL_COST") == "" {
		t.Skip("a measurement: set BT_TAIL_COST=1")
	}
	const keys, perCycle, cycles, every, behind = 50000, 500, 480, 120, 30
	for _, keep := range []bool{false, true} {
		ctx := context.Background()
		base := &pgCopyIndex{floors: map[string]uint64{}}
		costs := &tailCosts{}
		var kp *tailKeeper
		ci := &costIndex{pgCopyIndex: base, k: &kp, costs: costs}
		var src tailSource = ci
		if keep {
			src = &costChainIndex{ci}
		}
		root := t.TempDir()
		cut := pgCopyCut(t, filepath.Join(root, "snap0"), "orders", 0x500, [3]string{"0", "a", "b"})
		base.floors[cut.Path] = 0x500
		base.update("orders", 0x600, "1", "v", "n")
		kp = newTailKeeper(src, t.TempDir(), tailBudget{Keys: 10_000_000}, time.Minute, time.Hour)
		kp.retryMin, kp.retryMax = 0, 0
		defer kp.close()
		settled(t, ctx, kp, cut)
		lsn := uint64(0x1000)
		var locked time.Duration
		var worst time.Duration
		changes := 0
		lsnAt := map[int]uint64{}
		for c := 1; c <= cycles; c++ {
			for i := range perCycle {
				lsn += 0x10
				base.update("orders", lsn, fmt.Sprint((c*perCycle+i)*7919%keys), "v", "n")
				changes++
			}
			lsnAt[c] = lsn
			if c%every == 0 {
				cut = pgCopyCut(t, filepath.Join(root, fmt.Sprintf("snap%d", c)), "orders", lsnAt[c-behind], [3]string{"0", "a", "b"})
				base.floors[cut.Path] = lsnAt[c-behind]
			}
			start := time.Now()
			if _, err := kp.refresh(ctx, []views.BaselineTable{cut}); err != nil {
				t.Fatal(err)
			}
			d := time.Since(start)
			locked += d
			worst = max(worst, d)
		}
		t.Logf("RESULT keep_chain=%v changes=%d rows_per_change=%.2f whole_rows=%d delta_rows=%d merge_rows=%d merges=%d whole_by_cause first=%d prune=%d eight=%d ratio=%d lock_total=%s lock_worst=%s",
			keep, changes, float64(costs.wholeRows+costs.deltaRows+costs.mergeRows)/float64(changes), costs.wholeRows, costs.deltaRows, costs.mergeRows, costs.merges,
			costs.first, costs.prune, costs.eight, costs.ratio, locked.Round(time.Millisecond), worst.Round(time.Millisecond))
	}
}
