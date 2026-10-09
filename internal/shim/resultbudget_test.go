package shim

import (
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// budgetHandler is a copy-only handler under budget b whose copy answers
// one cell of n bytes.
func budgetHandler(b *ResultBudget, n int) (*Handler, *fakeFreeSQL) {
	f := &fakeFreeSQL{res: oneCell("x", "VARCHAR", strings.Repeat("a", n)), updatedAt: time.Now()}
	h := NewHandlerWithConfig(nil, Config{ResultBudget: b}, nil)
	h.BindFreeSQL(f)
	return h, f
}

// A result counts from the moment it is built until it is released, once,
// whatever is called how many times.
func TestResultBudget_countsOneResultPerConnection(t *testing.T) {
	b := NewResultBudget(1 << 20)
	h, _ := budgetHandler(b, 1000)
	r, err := h.HandleQuery("SELECT x FROM t")
	if err != nil {
		t.Fatal(err)
	}
	size := resultBytes(r)
	if size < 1000 {
		t.Fatalf("the result measures %d bytes, want its 1000-byte cell at least", size)
	}
	if b.Held() != size {
		t.Errorf("held = %d, want the result's %d", b.Held(), size)
	}
	// A second answer before the first was released (no serving loop here)
	// replaces it, it is not added to it.
	if _, err := h.HandleQuery("SELECT x FROM t"); err != nil {
		t.Fatal(err)
	}
	if b.Held() != size || b.Charged() != 2*size {
		t.Errorf("after a second statement: held = %d (want %d), charged = %d (want %d)", b.Held(), size, b.Charged(), 2*size)
	}
	h.ReleaseResult()
	h.ReleaseResult()
	if b.Held() != 0 {
		t.Errorf("after release: held = %d, want 0", b.Held())
	}
	if _, err := h.HandleQuery("SELECT x FROM t"); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if b.Held() != 0 {
		t.Errorf("after Close: held = %d, want 0", b.Held())
	}
}

// With the budget taken by another connection, a statement is refused
// before the copy runs it, with the code of a busy copy, and runs once the
// other connection's result is written.
func TestResultBudget_refusesBeforeRunning(t *testing.T) {
	b := NewResultBudget(500)
	h1, _ := budgetHandler(b, 1000)
	h2, f2 := budgetHandler(b, 10)
	if _, err := h1.HandleQuery("SELECT x FROM t"); err != nil {
		t.Fatalf("the first statement finds the budget free and must run whatever its size: %v", err)
	}
	_, err := h2.HandleQuery("SELECT x FROM t")
	me := wantMyError(t, err, mysql.ER_TOO_MANY_USER_CONNECTIONS, "still sending", "again in a moment")
	if len(me.Message) > 512 {
		t.Errorf("the refusal is %d bytes, past the 512 a MySQL client shows", len(me.Message))
	}
	if f2.calls != 0 {
		t.Errorf("the copy ran the refused statement (%d runs)", f2.calls)
	}
	if b.Refused() != 1 {
		t.Errorf("refused = %d, want 1", b.Refused())
	}
	h1.ReleaseResult()
	if _, err := h2.HandleQuery("SELECT x FROM t"); err != nil || f2.calls != 1 {
		t.Errorf("after the first result was released: err = %v, runs = %d; want the statement answered", err, f2.calls)
	}
}

// A statement that ends in an error holds nothing.
func TestResultBudget_aRefusedResultHoldsNothing(t *testing.T) {
	b := NewResultBudget(1 << 20)
	h, f := budgetHandler(b, 1000)
	f.res.Truncated = true
	if _, err := h.HandleQuery("SELECT x FROM t"); err == nil {
		t.Fatal("a result past the row cap was returned")
	}
	f.res, f.err = sqlsandbox.Result{}, sqlsandbox.ErrResultTooLarge
	if _, err := h.HandleQuery("SELECT x FROM t"); err == nil {
		t.Fatal("a result past the size cap was returned")
	}
	if b.Held() != 0 || b.Charged() != 0 {
		t.Errorf("held = %d, charged = %d; want nothing counted", b.Held(), b.Charged())
	}
}

// No budget: nothing is counted and nothing is refused.
func TestResultBudget_nilAdmitsEverything(t *testing.T) {
	var b *ResultBudget
	if NewResultBudget(0) != nil || NewResultBudget(-1) != nil {
		t.Fatal("a budget of zero or less must be no budget")
	}
	h, _ := budgetHandler(b, 1000)
	for range 3 {
		if _, err := h.HandleQuery("SELECT x FROM t"); err != nil {
			t.Fatal(err)
		}
	}
	h.ReleaseResult()
	h.Close()
	if b.Held() != 0 || b.Charged() != 0 || b.Refused() != 0 || b.admit(nil) != nil {
		t.Error("a nil budget counted or refused something")
	}
}

// Under read routing a statement the budget turns away is MySQL's: the
// client gets its answer, and the copy is not run.
func TestResultBudget_routedStatementGoesToMySQL(t *testing.T) {
	b := NewResultBudget(500)
	holder, _ := budgetHandler(b, 1000)
	if _, err := holder.HandleQuery("SELECT x FROM t"); err != nil {
		t.Fatal(err)
	}
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("x", "VARCHAR", "copy"), updatedAt: time.Now()}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	h.cfg.ResultBudget = b
	const stmt = "SELECT a, count(*) FROM t GROUP BY a"
	if _, err := h.HandleQuery(stmt); err != nil {
		t.Fatalf("the refusal reached the client: %v", err)
	}
	if f.calls != 0 {
		t.Errorf("the copy ran the statement (%d runs)", f.calls)
	}
	if len(r.forwarded) != 1 || r.forwarded[0] != stmt {
		t.Errorf("forwarded to MySQL = %v, want the statement once", r.forwarded)
	}
	// Not a fault of the copy and not a full line for its slots: its own
	// reason and its own warning, so neither of those is hidden by it.
	if len(got) != 1 || got[0] != "mysql/copy_results_held" {
		t.Errorf("observed %v, want [mysql/copy_results_held]", got)
	}
	if len(h.routeWarned) != 1 || !h.routeWarned["results"] {
		t.Errorf("warned keys = %v, want only results", h.routeWarned)
	}
}

// Over a real connection: the result is counted while it is sent and given
// back once the client has it, for a text statement and for a prepared one
// answered by the copy, so an idle connection holds nothing.
func TestResultBudget_releasedOnceTheAnswerIsWritten(t *testing.T) {
	b := NewResultBudget(1 << 20)
	r := &fakeRouter{sessStatus: stAuto, sessKnown: true, toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("x", "VARCHAR", strings.Repeat("a", 1000)), updatedAt: time.Now()}
	addr := servePort(t, "", func() server.Handler {
		h := NewHandlerWithConfig(nil, Config{ResultBudget: b}, nil)
		h.BindFreeSQL(f)
		h.BindRouter(r, RouterConfig{MaxCopyAge: time.Hour})
		return h
	})
	c := dialPort(t, addr)
	rep, err := c.Exec("SELECT a, count(*) FROM t GROUP BY a")
	if err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 || len(rep.Rows) != 1 {
		t.Fatalf("not answered by the copy: %d runs, %d rows", f.calls, len(rep.Rows))
	}
	text := b.Charged()
	if text < 1000 {
		t.Fatalf("charged = %d after a 1000-byte result, want it counted", text)
	}
	// With no further command: the release belongs to this answer, not to
	// the next command, or an idle connection would keep its last result.
	waitReleased(t, b, "once the answer was read")

	id, _, err := c.Prepare("SELECT a, count(*) FROM t GROUP BY a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Execute(id); err != nil {
		t.Fatal(err)
	}
	if f.calls != 2 {
		t.Fatalf("the prepared statement was not answered by the copy (%d runs)", f.calls)
	}
	if b.Charged() <= text {
		t.Errorf("charged = %d after the prepared statement, want more than %d", b.Charged(), text)
	}
	waitReleased(t, b, "once the prepared answer was read")
}

// waitReleased waits for the budget to hold nothing: the port gives a result
// back right after writing it, which the client can see a moment before.
func waitReleased(t *testing.T, b *ResultBudget, when string) {
	t.Helper()
	for range 300 {
		if b.Held() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("held = %d %s, want 0", b.Held(), when)
}

// Statements can pass the first check together and then wait for a worker:
// the budget is checked again when a result is ready, so the port never
// holds more than the budget plus one result.
func TestResultBudget_checkedAgainWhenTheResultIsReady(t *testing.T) {
	b := NewResultBudget(500)
	h1, _ := budgetHandler(b, 1000)
	h2, f2 := budgetHandler(b, 1000)
	f2.res.TruncatedCells = 1 // an answer that would carry a warning
	// While h2's statement runs, h1's is answered and takes the budget.
	f2.onRun = func() {
		if _, err := h1.HandleQuery("SELECT x FROM t"); err != nil {
			t.Errorf("h1: %v", err)
		}
	}
	_, err := h2.HandleQuery("SELECT x FROM t")
	wantMyError(t, err, mysql.ER_TOO_MANY_USER_CONNECTIONS, "still sending")
	if f2.calls != 1 {
		t.Fatalf("h2's statement ran %d times, want once (it passed the first check)", f2.calls)
	}
	if held, one := b.Held(), b.Charged(); held != one || held < 1000 || held >= 2000 {
		t.Errorf("held = %d, charged = %d; want h1's result alone", held, one)
	}
	// The refused result was never served: it leaves no warning behind.
	rep, err := h2.HandleQuery("SHOW WARNINGS")
	if err != nil || len(rep.RowDatas) != 0 {
		t.Errorf("SHOW WARNINGS after the refusal: %d rows, err = %v; want none", len(rep.RowDatas), err)
	}
}

// A client that asks and stops reading does not keep its result counted for
// ever: past the write timeout its connection is closed and the budget is
// free again for the others.
func TestResultBudget_aClientThatStopsReadingIsDropped(t *testing.T) {
	prev := portWriteTimeout.Swap(int64(2 * time.Second))
	t.Cleanup(func() { portWriteTimeout.Store(prev) })
	b := NewResultBudget(1 << 20)
	port := func(f *fakeFreeSQL) string {
		return servePort(t, "", func() server.Handler {
			h := NewHandlerWithConfig(nil, Config{ResultBudget: b}, nil)
			h.BindFreeSQL(f)
			return h
		})
	}
	// Larger than what the sockets buffer, so the write blocks.
	stalled := dialPort(t, port(&fakeFreeSQL{res: oneCell("x", "VARCHAR", strings.Repeat("a", 32<<20)), updatedAt: time.Now()}))
	other := dialPort(t, port(&fakeFreeSQL{res: oneCell("x", "VARCHAR", "ok"), updatedAt: time.Now()}))
	if err := stalled.Send("SELECT x FROM t"); err != nil {
		t.Fatal(err)
	}
	// While the stalled client's answer is being written, the budget is taken.
	refused := false
	for range 200 {
		if b.Held() > 0 {
			_, err := other.Exec("SELECT x FROM t")
			refused = isWireError(err, mysql.ER_TOO_MANY_USER_CONNECTIONS)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !refused {
		t.Fatal("the other connection was not refused while the stalled one held the budget")
	}
	// Past the write timeout the stalled connection is closed, and the
	// budget is free again.
	for range 600 {
		if b.Held() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rep, err := other.Exec("SELECT x FROM t"); err != nil || len(rep.Rows) != 1 {
		t.Errorf("after the stalled client was dropped: held = %d, err = %v, rows = %d; want the statement answered", b.Held(), err, len(rep.Rows))
	}
}
