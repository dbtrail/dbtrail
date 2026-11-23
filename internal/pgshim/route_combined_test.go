package pgshim

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

func lostT(context.Context) ([]TableNotCaptured, error) {
	return []TableNotCaptured{{Schema: "public", Name: "t", Why: "capture dropped changes of public.t"}}, nil
}

// (a) A table capture did not keep whole is decided before the copy runs,
// and before it waits or is asked for its recent changes (#3086): the source
// answers it as table_not_captured, not copy_too_old.
func TestCombined_notCapturedComesBeforeTheTailsRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.tailsPending.Store(true)
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("the read by key: %+v", got)
	}
	p.l.notCaptured = lostT
	d := costPortConn(t, ctx, p.l) // a Binding with the rule
	if got := costSimple(ctx, d, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("the read by key: %+v", got)
	}
	waits, _, _ := p.w.asked()
	runs := p.l.runs.Load()
	p.l.mu.Lock()
	asked := len(p.l.needTails)
	p.l.mu.Unlock()
	want := costOutcome{"42", "", RouteSource, ReasonTableNotCaptured}
	if got := costSimple(ctx, d, p.l, "SELECT sum(n) FROM big b JOIN t ON t.id = b.id"); got != want {
		t.Errorf("%+v, want %+v", got, want)
	}
	if n := p.l.runs.Load() - runs; n != 0 {
		t.Errorf("the copy ran %d statements", n)
	}
	p.l.mu.Lock()
	n := len(p.l.needTails) - asked
	p.l.mu.Unlock()
	if n != 0 {
		t.Errorf("the copy was asked %d times", n)
	}
	if w, _, _ := p.w.asked(); w != waits {
		t.Errorf("waited %d times for the copy", w-waits)
	}
}

// (b) A shape remembered as the copy's does not send a table capture did
// not keep whole there, and going to the source for that reason is not
// remembered as a side by its plan, either way.
func TestCombined_aRememberedSideNeverSendsANotCapturedTableToTheCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	plans := maps.Clone(costPlans)
	for _, c := range []int{48000, 52000} {
		plans[fmt.Sprintf("k = %d", c)] = plan(fmt.Sprintf(`{"Node Type": "Index Scan", "Total Cost": %d, "Plan Rows": 10}`, c))
	}
	at := func(c int) string { return fmt.Sprintf("SELECT n FROM t WHERE k = %d", c) }
	sides := &CostSides{}
	l := &costLogin{src: startFakeSource(t, plans), sides: sides}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, at(52000)); got.side != RouteCopy {
		t.Fatalf("over the threshold: %+v", got)
	}
	if n := len(sides.sides); n != 1 {
		t.Fatalf("%d shapes remembered", n)
	}
	l.notCaptured = lostT
	d := costPortConn(t, ctx, l)
	runs := l.runs.Load()
	for _, cost := range []int{48000, 52000} {
		if got := costSimple(ctx, d, l, at(cost)); got.side != RouteSource || got.reason != ReasonTableNotCaptured {
			t.Errorf("cost %d: %+v, want the source as %s", cost, got, ReasonTableNotCaptured)
		}
	}
	if n := l.runs.Load() - runs; n != 0 {
		t.Errorf("the copy ran %d statements", n)
	}
	for k, v := range sides.sides {
		if v != RouteCopy {
			t.Errorf("%q remembered as %s: not capturing it is not a side", k, v)
		}
	}
	// A shape first seen on such a table remembers nothing.
	fresh := &CostSides{}
	l2 := &costLogin{src: l.src, sides: fresh, notCaptured: lostT}
	e := costPortConn(t, ctx, l2)
	if got := costSimple(ctx, e, l2, at(52000)); got.reason != ReasonTableNotCaptured {
		t.Fatalf("%+v", got)
	}
	if n := len(fresh.sides); n != 0 {
		t.Errorf("%d shapes remembered from a statement the copy did not answer", n)
	}
	// Once the table is whole again, the copy's answer is remembered.
	l3 := &costLogin{src: l.src, sides: fresh}
	f := costPortConn(t, ctx, l3)
	if got := costSimple(ctx, f, l3, at(52000)); got.side != RouteCopy {
		t.Fatalf("%+v", got)
	}
	if n := len(fresh.sides); n != 1 {
		t.Errorf("%d shapes remembered after the copy answered", n)
	}
}

// (c) A statement refused because the copy is busy leaves the connection
// usable: the next one is answered by the source, within the slot it holds,
// and a busy copy is not remembered as a side.
func TestCombined_aKeptConnectionAcrossABusyCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sides := &CostSides{}
	l := &costLogin{src: startFakeSource(t, costPlans), keep: &keptSources{}, sides: sides,
		copyErr: func(string) error { return sqlsandbox.ErrBusy }}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Fatal(got)
	}
	got := costSimple(ctx, c, l, "SELECT sum(n) FROM big")
	if got.code != busyCode || got.side != RouteRefused {
		t.Fatalf("the busy copy: %+v", got)
	}
	for k, v := range sides.sides {
		if v == RouteCopy {
			t.Errorf("%q remembered as the copy's, which refused it", k)
		}
	}
	for i := range 3 {
		if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
			t.Fatalf("after the refusal, %d: %+v", i, got)
		}
	}
	if n := l.held.Load(); n != 1 {
		t.Errorf("%d connections held, want 1", n)
	}
}

// (c) A kept connection is reset before it is used again, its settings given
// again; one that cannot be reset is ended and another opened, without an
// error; nothing is sent to a source the host now bars.
func TestCombined_aKeptConnectionIsResetBeforeItIsUsed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src, keep: &keptSources{}}
	c := costPortConn(t, ctx, l)
	discards := func() int { src.mu.Lock(); defer src.mu.Unlock(); return src.discards }
	for range 3 {
		if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
			t.Fatal(got)
		}
	}
	if n := discards(); n != 2 {
		t.Errorf("reset %d times for 2 reuses", n)
	}
	if n := l.opened.Load(); n != 1 {
		t.Errorf("opened %d", n)
	}
	src.mu.Lock()
	src.refuseDiscard = true
	src.mu.Unlock()
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Errorf("a connection that could not be reset: %+v", got)
	}
	if n := l.opened.Load(); n != 2 {
		t.Errorf("after a failed reset opened %d, want 2", n)
	}
	if n := l.held.Load(); n != 1 {
		t.Errorf("%d held", n)
	}
}
