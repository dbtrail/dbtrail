//go:build integration

package console

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/views"
)

// pgHeldFill holds the end of one table's Fill until released, so a test can
// move the keeper's cursor while that table's tail is being built.
type pgHeldFill struct {
	*pgTailSource
	table   string
	once    sync.Once
	reached chan struct{}
	release chan struct{}
}

func (h *pgHeldFill) Fill(ctx context.Context, t views.BaselineTable, upTo uint64, page func([]query.ResultRow) error) (string, error) {
	why, err := h.pgTailSource.Fill(ctx, t, upTo, page)
	if t.Table == h.table {
		h.once.Do(func() { close(h.reached) })
		select {
		case <-h.release:
		case <-ctx.Done():
			return why, ctx.Err()
		}
	}
	return why, err
}

// The whole failure, through the keeper. A table's tail is being built from
// event F. Meanwhile its first change after F is indexed, and the update of
// the tails that already exist reads (F, T] for THEM and finds nothing. The
// build then catches up from F for its own table: it must get that change,
// and not be installed as current without it, as it was while Since
// remembered an empty read without saying for which tables.
func TestIntegrationPGTailBuild_catchesUpPastAnotherTablesEmptyRead(t *testing.T) {
	ctx := context.Background()
	r := newPGTailRig(t)
	quiet := r.table(nil, [2]string{"1", "one"})
	built := r.table(nil, [2]string{"2", "two"})
	r.change(quiet.Table, event.EventUpdate, "1", "uno", 0x600, 20*time.Minute)
	r.checkpoint(0x600)

	src := &pgHeldFill{pgTailSource: r.k.src.(*pgTailSource), table: built.Table,
		reached: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	letGo := func() { release.Do(func() { close(src.release) }) }
	defer letGo()
	k := newTailKeeper(src, t.TempDir(), tailBudget{Keys: 1000}, time.Minute, time.Hour)
	k.retryMin, k.retryMax = 0, 0
	t.Cleanup(k.close)
	if _, err := settledRefresh(t, ctx, k, []views.BaselineTable{quiet}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.refresh(ctx, []views.BaselineTable{quiet, built}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-src.reached:
	case <-time.After(30 * time.Second):
		t.Fatal("the build did not read its window")
	}

	r.change(built.Table, event.EventUpdate, "2", "dos", 0x610, 21*time.Minute)
	r.checkpoint(0x610)
	if _, err := k.refresh(ctx, []views.BaselineTable{quiet}); err != nil {
		t.Fatal(err)
	}
	letGo()
	got, err := settledRefresh(t, ctx, k, []views.BaselineTable{quiet, built})
	if err != nil {
		t.Fatal(err)
	}
	v, ok := got[tailKey(built.Schema, built.Table)]
	if !ok {
		t.Fatal("the built table has no tail")
	}
	if state, want := merged(t, built, v), []string{"2=dos"}; !reflect.DeepEqual(state, want) {
		t.Fatalf("a statement reads %v through event %d, want %v: the build skipped a change it says it holds", state, v.ThroughID, want)
	}
}

// A read that handed nothing says nothing of any other read from the same
// event: another table, a superset, a set that overlaps it, a subset, the
// same set again, or a process that started again.
func TestIntegrationPGTailSince_anEmptyReadSpeaksForNoOtherRead(t *testing.T) {
	ctx := context.Background()
	r := newPGTailRig(t)
	a := r.table(nil, [2]string{"1", "one"})
	c := r.table(nil, [2]string{"3", "three"})
	changed := r.table(nil, [2]string{"2", "two"})
	r.change(a.Table, event.EventUpdate, "1", "uno", 0x600, 20*time.Minute)
	r.checkpoint(0x600)
	src := r.k.src.(*pgTailSource)
	from, err := src.Newest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r.change(changed.Table, event.EventUpdate, "2", "dos", 0x610, 21*time.Minute)
	r.checkpoint(0x610)
	to, err := src.Newest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	read := func(s *pgTailSource, tables ...views.BaselineTable) int {
		t.Helper()
		n := 0
		if err := s.Since(ctx, tables, from.ID, to.ID, func(page []query.ResultRow) error {
			n += len(page)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	restarted := newPGTailSource(r.s, r.b).capturing(nil)
	for _, tc := range []struct {
		name  string
		empty []views.BaselineTable
		by    *pgTailSource
		now   []views.BaselineTable
		want  int
	}{
		{"another table", []views.BaselineTable{a}, src, []views.BaselineTable{changed}, 1},
		{"a superset", []views.BaselineTable{a}, src, []views.BaselineTable{a, changed}, 1},
		{"a superset of two", []views.BaselineTable{a, c}, src, []views.BaselineTable{c, changed, a}, 1},
		{"a set that overlaps", []views.BaselineTable{a, c}, src, []views.BaselineTable{c, changed}, 1},
		{"a subset", []views.BaselineTable{a, c}, src, []views.BaselineTable{c}, 0},
		{"the same set in another order", []views.BaselineTable{a, c}, src, []views.BaselineTable{c, a}, 0},
		{"a process that started again", []views.BaselineTable{a}, restarted, []views.BaselineTable{a, changed}, 1},
	} {
		if n := read(src, tc.empty...); n != 0 {
			t.Fatalf("%s: the quiet tables were handed %d rows", tc.name, n)
		}
		if n := read(tc.by, tc.now...); n != tc.want {
			t.Errorf("%s: handed %d rows after an empty read, want %d", tc.name, n, tc.want)
		}
	}
}
