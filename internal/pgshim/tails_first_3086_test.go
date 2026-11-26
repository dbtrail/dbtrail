package pgshim

import (
	"context"
	"testing"
	"time"
)

// #3086: on a connection the source answered about a table, a statement of
// the copy that reads it is given only when the copy read that table with
// its recent changes over it (copyOlderThanSource). The copy is told so
// before it runs (RunOptions.NeedTails): when the changes are not ready, it
// refuses without running the statement, and the source answers.
//
// Since #3116 the copy is told so on every connection that can send the
// statement to its source, as a routed MySQL statement is refused there
// (sqlsandbox.Session.StrictStar): a statement that reads another table, and
// one on a connection the source never answered, are no longer answered from
// the snapshot alone either.
func TestTailsFirst_3086(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.tailsPending.Store(true)
	const readsT = "SELECT sum(n) FROM big b JOIN t ON t.id = b.id"
	answeredAboutT := func(t *testing.T) func(string) costOutcome {
		t.Helper()
		c := costPortConn(t, ctx, p.l)
		if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
			t.Fatalf("the read by key: %+v", got)
		}
		return func(stmt string) costOutcome { return costSimple(ctx, c, p.l, stmt) }
	}
	for _, tc := range []struct {
		name      string
		stmt      string
		fresh     bool // a connection the source never answered
		want      costOutcome
		needTails bool
		runs      int32
	}{
		// The case of the issue: refused before it ran, the source answers.
		{"a table the source answered about", readsT, false, tablesTooOld, true, 0},
		{"the same table in a subquery", "SELECT sum(n) FROM big WHERE id IN (SELECT id FROM t)", false, tablesTooOld, true, 0},
		{"the same table in capitals, with its schema", "SELECT sum(n) FROM big b JOIN PUBLIC.T x ON x.id = b.id", false, tablesTooOld, true, 0},
		// Read apart from what the source answered: the snapshot alone is
		// not given either (#3116).
		{"another table", "SELECT sum(n) FROM big", false, tablesTooOld, true, 0},
		{"the same name in another schema", "SELECT sum(n) FROM big b JOIN other.t x ON x.id = b.id", false, tablesTooOld, true, 0},
		// Nothing answered by the source: the snapshot alone is still not
		// given where the source can answer (#3116).
		{"a connection the source never answered", readsT, true, tablesTooOld, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var run func(string) costOutcome
			if tc.fresh {
				c := costPortConn(t, ctx, p.l)
				run = func(stmt string) costOutcome { return costSimple(ctx, c, p.l, stmt) }
			} else {
				run = answeredAboutT(t)
			}
			runs := p.l.runs.Load()
			if got := run(tc.stmt); got != tc.want {
				t.Errorf("%+v, want %+v", got, tc.want)
			}
			if got := p.l.lastNeedTails(); got != tc.needTails {
				t.Errorf("the copy was asked NeedTails=%v, want %v", got, tc.needTails)
			}
			if d := p.l.runs.Load() - runs; d != tc.runs {
				t.Errorf("the copy ran %d statements, want %d", d, tc.runs)
			}
		})
	}

	// With the changes ready, the same statement is answered by the copy,
	// still asked to have them: the rule is the answer's, not the moment's.
	t.Run("the changes ready", func(t *testing.T) {
		p.l.tailsPending.Store(false)
		defer p.l.tailsPending.Store(true)
		p.knob.at(time.Now().Add(time.Hour))
		defer p.knob.set(snapshotAlone)
		run := answeredAboutT(t)
		if got := run(readsT); got != tablesOnCopy {
			t.Errorf("%+v, want %+v", got, tablesOnCopy)
		}
		if !p.l.lastNeedTails() {
			t.Error("the copy was not asked to have the changes")
		}
	})
}

// A connection whose source can no longer be reached (the host bars it
// after the source answered) gets the copy's answer from a snapshot alone,
// as before: there is nowhere else to answer it, so the copy is not told to
// refuse.
func TestTailsFirst_3086_aConnectionThatCannotFallBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.tailsPending.Store(true)
	why := ""
	p.l.barred = func() string { return why }
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("the read by key: %+v", got)
	}
	why = "hidden rules"
	runs := p.l.runs.Load()
	got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big b JOIN t ON t.id = b.id")
	if got.answer != "7" || got.code != "" || got.side != RouteCopy {
		t.Errorf("%+v, want the copy's 7", got)
	}
	if p.l.lastNeedTails() {
		t.Error("the copy was asked to have the changes where nothing else can answer")
	}
	if d := p.l.runs.Load() - runs; d != 1 {
		t.Errorf("the copy ran %d statements, want 1", d)
	}
}

// A prepared statement refused for its changes goes to the source for that
// execution only: once they are ready, the copy answers it again
// (transientFallback).
func TestTailsFirst_3086_aPreparedStatementIsNotKeptOnTheSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.tailsPending.Store(true)
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("the read by key: %+v", got)
	}
	if _, err := c.Prepare(ctx, "rep", "SELECT sum(n) FROM big b JOIN t ON t.id = b.id", nil); err != nil {
		t.Fatal(err)
	}
	runs := p.l.runs.Load()
	res := c.ExecPrepared(ctx, "rep", nil, nil, nil).Read()
	if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "42" {
		t.Fatalf("the changes not ready: %v %q, want the source's 42", res.Err, res.Rows)
	}
	if n := p.l.last(); n.Side != RouteSource || n.Reason != ReasonCopyTooOld {
		t.Errorf("the changes not ready: noted %s/%s, want %s/%s", n.Side, n.Reason, RouteSource, ReasonCopyTooOld)
	}
	if d := p.l.runs.Load() - runs; d != 0 {
		t.Errorf("the copy ran %d statements, want none", d)
	}
	p.l.tailsPending.Store(false)
	p.knob.at(time.Now().Add(time.Hour))
	res = c.ExecPrepared(ctx, "rep", nil, nil, nil).Read()
	if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "7" {
		t.Errorf("the changes ready: %v %q, want the copy's 7: the statement was kept on the source", res.Err, res.Rows)
	}
}
