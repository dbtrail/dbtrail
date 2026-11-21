package console

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #3076, in the meantime: a build of a PostgreSQL tail whose read of the copy
// runs out of the build's time twice over one cut stops trying until the
// table's next cut.

const pgCopyLateRefusal = "it is tried again at the table's next snapshot"

// The count, at the read itself: the first read out of time is an error
// (tried again), the second over the same cut a refusal; a read cut short
// for another reason counts nothing; another cut, another table, or a read
// that ended starts from nothing.
func TestPGTailSource_aCopyReadOutOfTimeTwiceRefusesItsCut(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rows := [][3]string{{"1", "one", "note one"}, {"2", "two", "note two"}}
	cut := pgCopyCut(t, filepath.Join(dir, "snap1"), "orders", 0x500, rows...)
	other := pgCopyCut(t, filepath.Join(dir, "snap2"), "orders", 0x500, rows...)
	items := pgCopyCut(t, filepath.Join(dir, "snap1"), "items", 0x500, rows...)
	f := &pgCopyIndex{floors: map[string]uint64{}}
	// waiting is a tail of tbl whose key 1 waits for its value from the copy.
	waiting := func(tbl views.BaselineTable) *reconstruct.PGTail {
		t.Helper()
		tail, _, err := f.Start(ctx, tbl)
		if err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		f.events = nil
		f.mu.Unlock()
		f.update(tbl.Table, 0x700, "1", pgCopyUnsent, "n")
		if err := tail.Apply(f.spelled([]views.BaselineTable{tbl}, 0, 1, false)); err != nil {
			t.Fatal(err)
		}
		return tail.(*reconstruct.PGTail)
	}
	slowCopyReads(t)
	// late is a build's context whose time runs out during the read.
	late := func() context.Context {
		c, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		t.Cleanup(stop)
		return c
	}
	spent, stop := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer stop()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	read := func(x *pgTailSource, c context.Context, tbl views.BaselineTable) (string, error) {
		t.Helper()
		tail := waiting(tbl)
		if c == nil {
			c = late()
		}
		return x.fillStartedFromCopy(c, tbl, tail)
	}
	tried := func(what string, why string, err error) {
		t.Helper()
		if err == nil || why != "" {
			t.Fatalf("%s: %q, %v; want an error, tried again with the build", what, why, err)
		}
	}
	refused := func(what string, why string, err error) {
		t.Helper()
		if err != nil || !strings.Contains(why, pgCopyLateRefusal) || !strings.Contains(why, "public.orders") {
			t.Fatalf("%s: %q, %v; want the cut refused", what, why, err)
		}
	}

	x := &pgTailSource{}
	why, err := read(x, nil, cut)
	tried("the first read out of time", why, err)
	for i := 0; i < 3; i++ {
		why, err = read(x, cancelled, cut)
		tried("a read cut short by a cancel", why, err)
		why, err = read(x, spent, cut)
		tried("a build whose time was up before the read began", why, err)
	}
	why, err = read(x, nil, items)
	tried("another table's first read out of time", why, err)
	why, err = read(x, nil, cut)
	refused("the second read out of time over the same cut", why, err)
	why, err = read(x, nil, cut)
	refused("and the third", why, err)
	why, err = read(x, nil, other)
	tried("the first read out of time over the next cut", why, err)
	why, err = read(x, nil, other)
	refused("the second over the next cut", why, err)

	// A read that ended forgets the count.
	y := &pgTailSource{}
	why, err = read(y, nil, cut)
	tried("a first read out of time", why, err)
	if why, err := read(y, ctx, cut); why != "" || err != nil {
		t.Fatalf("a read with time: %q, %v", why, err)
	}
	why, err = read(y, nil, cut)
	tried("the next read out of time, after one that ended", why, err)
}

// slowCopyReads makes, for one test, a build's read of the copy wait out its
// context while the switch it returns is set (it starts set).
func slowCopyReads(t *testing.T) *atomic.Bool {
	t.Helper()
	slow := new(atomic.Bool)
	slow.Store(true)
	prev := pgCopyBuildValues
	pgCopyBuildValues = func(ctx context.Context, src pgCopySource, tbl views.BaselineTable, keyCols, keys, cols []string, maxBytes int64) (map[string]map[string]*string, error) {
		if slow.Load() {
			<-ctx.Done()
		}
		return prev(ctx, src, tbl, keyCols, keys, cols, maxBytes)
	}
	t.Cleanup(func() { pgCopyBuildValues = prev })
	return slow
}

// pgLateSource is pgCopyIndex with the real source's build path: Start notes
// the tail it built and Fill reads the copy for it at its end, as
// pgTailSource does.
type pgLateSource struct {
	*pgCopyIndex
}

func (s *pgLateSource) Start(ctx context.Context, t views.BaselineTable) (tailTable, string, error) {
	tail, why, err := s.pgCopyIndex.Start(ctx, t)
	if tail != nil {
		s.real.noteStarted(t, tail.(*reconstruct.PGTail))
	}
	return tail, why, err
}

func (s *pgLateSource) Fill(ctx context.Context, t views.BaselineTable, upTo uint64, page func([]query.ResultRow) error) (string, error) {
	started := s.real.takeStarted(t)
	if why, err := s.pgCopyIndex.Fill(ctx, t, upTo, page); why != "" || err != nil {
		return why, err
	}
	return s.real.fillStartedFromCopy(ctx, t, started)
}

// At the keeper: the first build out of time is tried again; the second over
// the same cut leaves the table refused, with no third build and its reason
// said; the next cut is built again, and once its read ends the table has
// its tail with the value read from the copy.
func TestPGTailCopy_aBuildOutOfTimeTwiceIsRefusedUntilTheNextCut(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	rows := [][3]string{{"1", "one", "note one"}, {"2", "two", "note two"}}
	f := &pgCopyIndex{floors: map[string]uint64{}}
	cut1 := pgCopyCut(t, filepath.Join(root, "snap1"), "orders", 0x500, rows...)
	cut2 := pgCopyCut(t, filepath.Join(root, "snap2"), "orders", 0x500, rows...)
	f.floors[cut1.Path], f.floors[cut2.Path] = 0x500, 0x500
	f.update("orders", 0x600, "1", pgCopyUnsent, "n")
	src := &pgLateSource{pgCopyIndex: f}
	slow := slowCopyReads(t)
	k := newTailKeeper(src, t.TempDir(), tailBudget{Keys: 1000}, time.Minute, time.Hour)
	// Enough for the build to reach its read of the copy, which then waits
	// it out.
	k.buildTimeout = 200 * time.Millisecond
	k.retryMin, k.retryMax = 0, 0
	t.Cleanup(k.close)
	key := tailKey("public", "orders")
	fills := func() int {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.fills
	}
	build := func(tbl views.BaselineTable) {
		t.Helper()
		if _, err := k.refresh(ctx, []views.BaselineTable{tbl}); err != nil {
			t.Fatal(err)
		}
		k.settle()
	}
	entry := func() *tailEntry { return k.tableEntry(key) }

	build(cut1)
	if e := entry(); fills() != 1 || (e != nil && e.refused != "") {
		t.Fatalf("after the first build out of time: %d builds, entry %+v; want one, not refused", fills(), e)
	}
	build(cut1)
	if e := entry(); fills() != 2 || e == nil || !strings.Contains(e.refused, pgCopyLateRefusal) {
		t.Fatalf("after the second build out of time over the same cut: %d builds, entry %+v; want two and the cut refused", fills(), e)
	}
	build(cut1)
	build(cut1)
	if n := fills(); n != 2 {
		t.Fatalf("the refused cut was built again: %d builds", n)
	}

	build(cut2)
	// The entry the keeper holds is still the older cut's refusal: a build
	// that failed installs nothing. None of the newer cut is refused.
	if e := entry(); fills() != 3 || (e != nil && e.cut == tailCutOf(cut2) && e.refused != "") {
		t.Fatalf("at the next cut: %d builds, entry %+v; want it built again, and not refused at its first time", fills(), e)
	}
	// A read with time to end: the engine's read of the file takes longer
	// than the short time the builds above were given.
	slow.Store(false)
	k.buildTimeout = time.Minute
	got := settled(t, ctx, k, cut2)
	v, ok := got[key]
	if !ok {
		t.Fatalf("once the read ends the table has no tail: %+v", entry())
	}
	if state, want := pgCopyState(t, cut2, v), []string{"1=one/n", "2=two/note two"}; !reflect.DeepEqual(state, want) {
		t.Fatalf("state %v, want %v", state, want)
	}
}
