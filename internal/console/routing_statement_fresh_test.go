package console

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Each row of the per-statement table says why the router sent it to its
// side, and a row of the copy says how far behind capture the data of its
// answers was, worked out on the same path as the server's own lag.

func rowOf(t *testing.T, n RouterNumbers, server, side, statement string) RouterStatementRow {
	t.Helper()
	st := n.Servers[server].Statements
	if st == nil {
		t.Fatalf("no table for %s", server)
	}
	for _, r := range st.Rows {
		if r.Side == side && r.Statement == statement {
			return r
		}
	}
	t.Fatalf("no row %s %q in %+v", side, statement, st.Rows)
	return RouterStatementRow{}
}

func TestStatementRow_reasons(t *testing.T) {
	srv := newStatementServer(t, true)
	srv.RecordRouteAnswer("a", "mysql", "cheap_plan", "SELECT * FROM t WHERE id = 1", time.Millisecond, false, false)
	srv.RecordRouteAnswer("a", "mysql", "copy_refused", "SELECT * FROM t WHERE id = 2", time.Millisecond, false, false)
	srv.RecordRouteAnswer("a", "mysql", "cheap_plan", "SELECT * FROM t WHERE id = 3", time.Millisecond, true, false)
	srv.RecordRouteAnswer("a", "copy", "expensive_plan", "SELECT * FROM t WHERE id = 4", time.Second, false, false)
	n := srv.RouterNumbers()
	my := rowOf(t, n, "a", "mysql", "select * from t where id = ?")
	if len(my.Reasons) != 2 || my.Reasons["cheap_plan"] != 2 || my.Reasons["copy_refused"] != 1 {
		t.Errorf("mysql reasons = %v", my.Reasons)
	}
	var sum uint64
	for _, c := range my.Reasons {
		sum += c
	}
	if sum != my.Count {
		t.Errorf("reasons add up to %d, the row counts %d", sum, my.Count)
	}
	cp := rowOf(t, n, "a", "copy", "select * from t where id = ?")
	if len(cp.Reasons) != 1 || cp.Reasons["expensive_plan"] != 1 {
		t.Errorf("copy reasons = %v", cp.Reasons)
	}
	if my.Fresh != nil {
		t.Errorf("a row of MySQL carries the copy's lag: %+v", my.Fresh)
	}
}

// The copy's answers to one statement, through the real path: the answer
// hook names the statement, the decision hook commits the copy's run.
func TestStatementRow_freshThroughNoteRoute(t *testing.T) {
	base := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	at := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }
	snap := map[string][]string{"/snap/1": {"/snap/1/t.parquet"}}
	const stmt = "SELECT a, count(*) FROM t GROUP BY a"
	const shape = "select a, count(*) from t group by a"

	answer := func(srv *Server, q *SQLOnCopy, files map[string][]string, ran, tail time.Time, allTailed bool) {
		q.noteCopyAnswer(files, files != nil || allTailed, ran, tail, allTailed)
		// The shim calls the answer hook first on the copy's path, then the
		// decision hook, which commits the run.
		q.NoteAnswerShape(srv.RecordRouteAnswer("a", "copy", "expensive_plan", stmt, 10*time.Millisecond, false, false))
		q.NoteRoute("copy")
	}

	t.Run("tails: known at once, min and max per answer", func(t *testing.T) {
		srv := newStatementServer(t, true)
		q := &SQLOnCopy{s: srv, id: "a"}
		answer(srv, q, nil, at(100), at(91), true)  // 9 s
		answer(srv, q, nil, at(200), at(196), true) // 4 s
		answer(srv, q, nil, at(300), at(278), true) // 22 s, the last
		f := rowOf(t, srv.RouterNumbers(), "a", "copy", shape).Fresh
		if f == nil || f.LastAgeSeconds == nil || *f.LastAgeSeconds != 22 || *f.MinAgeSeconds != 4 || *f.MaxAgeSeconds != 22 ||
			f.Known != 3 || f.Tailed != 3 || f.Snapshot != 0 || f.Unknown != 0 || f.Pending || f.LastAsOf != at(278).Format(time.RFC3339) {
			t.Fatalf("fresh = %s", jsonOf(f))
		}
		// The server's own lag is the same path's: its last answer is this one.
		if n := srv.copyAges.now("a"); !n.known || n.age != 22*time.Second || n.worst != 22*time.Second {
			t.Errorf("server = %+v", n)
		}
	})

	t.Run("a snapshot: pending, then each answer against the snapshot's instant", func(t *testing.T) {
		srv := newStatementServer(t, true)
		q := &SQLOnCopy{s: srv, id: "a"}
		answer(srv, q, snap, at(140), time.Time{}, false)
		answer(srv, q, snap, at(298), time.Time{}, false)
		f := rowOf(t, srv.RouterNumbers(), "a", "copy", shape).Fresh
		if f == nil || !f.Pending || f.LastAgeSeconds != nil || f.MinAgeSeconds != nil || f.Snapshot != 2 || f.Known != 0 {
			t.Fatalf("before the index answered: %s", jsonOf(f))
		}
		for key := range srv.copyAges.pendingFor()["a"] {
			srv.copyAges.settle("a", key, at(0), "")
		}
		f = rowOf(t, srv.RouterNumbers(), "a", "copy", shape).Fresh
		if f.Pending || *f.LastAgeSeconds != 298 || *f.MinAgeSeconds != 140 || *f.MaxAgeSeconds != 298 || f.Known != 2 {
			t.Fatalf("after: %s", jsonOf(f))
		}
		// The snapshot is known now: a third answer over it is known at once.
		answer(srv, q, snap, at(30), time.Time{}, false)
		f = rowOf(t, srv.RouterNumbers(), "a", "copy", shape).Fresh
		if f.Pending || *f.LastAgeSeconds != 30 || *f.MinAgeSeconds != 30 || f.Known != 3 || f.Snapshot != 3 {
			t.Fatalf("third: %s", jsonOf(f))
		}
	})

	t.Run("a snapshot the index cannot place: unknown, with the fixed reason", func(t *testing.T) {
		srv := newStatementServer(t, true)
		q := &SQLOnCopy{s: srv, id: "a"}
		answer(srv, q, snap, at(10), time.Time{}, false)
		for key := range srv.copyAges.pendingFor()["a"] {
			srv.copyAges.settle("a", key, time.Time{}, "Error 1045: Access denied for user 'secret_user'")
		}
		f := rowOf(t, srv.RouterNumbers(), "a", "copy", shape).Fresh
		if f.Pending || f.LastAgeSeconds != nil || f.MaxAgeSeconds != nil || f.Unknown != 1 || f.UnknownReason != copyAgeWhyNotPlaced {
			t.Fatalf("fresh = %s", jsonOf(f))
		}
		if b := jsonOf(srv.RouterNumbers()); strings.Contains(b, "secret_user") || strings.Contains(b, "1045") {
			t.Errorf("the document carries an error's text: %s", b)
		}
	})

	t.Run("which snapshot it read is not known: unknown", func(t *testing.T) {
		srv := newStatementServer(t, true)
		q := &SQLOnCopy{s: srv, id: "a"}
		answer(srv, q, nil, at(10), time.Time{}, false)
		f := rowOf(t, srv.RouterNumbers(), "a", "copy", shape).Fresh
		if f.Unknown != 1 || f.UnknownReason != copyAgeWhyNoFiles || f.LastAgeSeconds != nil || f.Known != 0 {
			t.Fatalf("fresh = %s", jsonOf(f))
		}
	})

	t.Run("two statements over one snapshot each get their own answers", func(t *testing.T) {
		srv := newStatementServer(t, true)
		q := &SQLOnCopy{s: srv, id: "a"}
		answer(srv, q, snap, at(50), time.Time{}, false)
		q.noteCopyAnswer(snap, true, at(80), time.Time{}, false)
		q.NoteAnswerShape(srv.RecordRouteAnswer("a", "copy", "expensive_plan", "SELECT b FROM t ORDER BY b", time.Millisecond, false, false))
		q.NoteRoute("copy")
		for key := range srv.copyAges.pendingFor()["a"] {
			srv.copyAges.settle("a", key, at(0), "")
		}
		n := srv.RouterNumbers()
		if f := rowOf(t, n, "a", "copy", shape).Fresh; *f.MaxAgeSeconds != 50 || *f.MinAgeSeconds != 50 {
			t.Errorf("first statement = %s", jsonOf(f))
		}
		if f := rowOf(t, n, "a", "copy", "select b from t order by b").Fresh; *f.MaxAgeSeconds != 80 || *f.MinAgeSeconds != 80 {
			t.Errorf("second statement = %s", jsonOf(f))
		}
	})

	t.Run("an answer over a tail and a snapshot is as current as the older", func(t *testing.T) {
		srv := newStatementServer(t, true)
		q := &SQLOnCopy{s: srv, id: "a"}
		answer(srv, q, snap, at(100), at(40), false) // tail through 40, snapshot at 60 below
		for key := range srv.copyAges.pendingFor()["a"] {
			srv.copyAges.settle("a", key, at(60), "")
		}
		if f := rowOf(t, srv.RouterNumbers(), "a", "copy", shape).Fresh; *f.LastAgeSeconds != 60 || *f.MinAgeSeconds != 60 {
			t.Fatalf("fresh = %s", jsonOf(f))
		}
	})

	t.Run("a run MySQL answered is not the copy's", func(t *testing.T) {
		srv := newStatementServer(t, true)
		q := &SQLOnCopy{s: srv, id: "a"}
		q.noteCopyAnswer(nil, true, at(10), at(9), true)
		q.NoteAnswerShape("select a, count(*) from t group by a")
		q.NoteRoute("mysql")
		if f := srv.copyAges.shapeNow("a", shape); f != nil {
			t.Fatalf("counted for the copy: %s", jsonOf(f))
		}
	})

	t.Run("lag rounds up, never smaller", func(t *testing.T) {
		srv := newStatementServer(t, true)
		q := &SQLOnCopy{s: srv, id: "a"}
		answer(srv, q, nil, at(10).Add(200*time.Millisecond), at(9), true)
		if f := rowOf(t, srv.RouterNumbers(), "a", "copy", shape).Fresh; *f.LastAgeSeconds != 2 || *f.MinAgeSeconds != 2 {
			t.Fatalf("1.2 s said as %s", jsonOf(f))
		}
	})
}

// Bounds: the statements a server tracks lag for are as many as its rows, and
// the answers one statement waits on the index with are capped; past the cap
// they are unknown, never dropped.
func TestStatementRow_freshBounds(t *testing.T) {
	var c copyAges
	snap := map[string][]string{"/snap/1": {"/snap/1/t.parquet"}}
	ran := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	for i := range freshPendingPerShape + 3 {
		c.note("s", copyAnswer{side: "copy", files: snap, ok: true, ranAt: ran.Add(time.Duration(i) * time.Second), shape: "q"})
	}
	for key := range c.pendingFor()["s"] {
		c.settle("s", key, ran, "")
	}
	f := c.shapeNow("s", "q")
	if f.Known != freshPendingPerShape || f.Unknown != 3 || f.Snapshot != freshPendingPerShape+3 {
		t.Fatalf("capped: %s", jsonOf(f))
	}
	for i := range StatementTableLimit + 5 {
		c.note("s", copyAnswer{side: "copy", ranAt: ran, tailThrough: ran, allTailed: true, shape: "q" + string(rune('a'+i%26)) + strings.Repeat("x", i)})
	}
	c.mu.Lock()
	n := len(c.by["s"].shapes)
	c.mu.Unlock()
	if n > StatementTableLimit {
		t.Fatalf("%d statements tracked, the bound is %d", n, StatementTableLimit)
	}
}

// An answer through the moving "current" link reads a snapshot nobody can
// place: unknown, and not counted as one that read a snapshot.
func TestStatementRow_freshMovingLinkIsNotASnapshot(t *testing.T) {
	var c copyAges
	c.note("s", copyAnswer{side: "copy", files: map[string][]string{"/b/current": {"/b/current/t.parquet"}}, ok: true, ranAt: time.Now(), shape: "q"})
	if f := c.shapeNow("s", "q"); f.Snapshot != 0 || f.Unknown != 1 || f.UnknownReason != copyAgeWhyMoving {
		t.Fatalf("%s", jsonOf(f))
	}
}

func jsonOf(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
