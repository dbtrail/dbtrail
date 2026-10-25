package console

import (
	"testing"
	"time"
)

// #2546: fresh mode is in its warm-up from the moment it applies until the
// copy's first answer that read every table it asked for with its tail.
// Tails are built on a statement's demand, so a server that answered
// nothing since is still warming up. The warm-up ends once and does not come
// back: a later answer that reads no table, the change log or a table that
// cannot have a tail says nothing about the others.

func answerAt(ran time.Time, tailed bool) copyAnswer {
	a := copyAnswer{side: "copy", ok: true, ranAt: ran, allTailed: tailed}
	if tailed {
		a.tailThrough = ran.Add(-time.Second)
	} else {
		a.files = map[string][]string{"shop.orders": {"/copy/shop.orders/0.parquet"}}
	}
	return a
}

func TestSQLFreshWarming_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a", "b")
	s, _ := freshTestServer(t, reg, 4, 3)
	id := ids[0]
	if s.SQLModeWarming(id) {
		t.Fatal("a fast server is catching up")
	}
	before := time.Now().Add(-time.Minute)
	s.copyAges.note(id, answerAt(before, true)) // an answer from before the switch
	if _, err := reg.SetSQLFresh(id, true); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	if !s.SQLModeWarming(id) {
		t.Fatal("just turned fresh, with no answer since: not catching up")
	}
	s.copyAges.note(id, answerAt(time.Now().Add(time.Millisecond), false))
	if !s.SQLModeWarming(id) {
		t.Fatal("an answer since that read a table as of its snapshot: not catching up")
	}
	s.copyAges.note(id, answerAt(time.Now().Add(2*time.Millisecond), true))
	if s.SQLModeWarming(id) {
		t.Fatal("an answer since that read every table with its newest changes: still catching up")
	}
	s.copyAges.note(id, answerAt(time.Now().Add(3*time.Millisecond), false))
	if s.SQLModeWarming(id) {
		t.Fatal("a later answer that read no tail (SELECT 1, the change log, a table with no tail) put the warm-up back")
	}
	// Off and on again starts over.
	s.copyAges.note(id, answerAt(time.Now().Add(4*time.Millisecond), true))
	if _, err := reg.SetSQLFresh(id, false); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	if s.SQLModeWarming(id) {
		t.Fatal("a server turned fast is catching up")
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := reg.SetSQLFresh(id, true); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	if !s.SQLModeWarming(id) {
		t.Fatal("turned fresh again: the answer from the earlier time still counts")
	}
	// The other server was never switched.
	if s.SQLModeWarming(ids[1]) {
		t.Fatal("a server never switched is catching up")
	}
}

// With BINTRAIL_SQL_TAILS=on every server is fresh from the process's start.
func TestSQLFreshWarming_environment_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, _ := freshTestServer(t, reg, 0, 0)
	s.routing = newRoutingStats(time.Now(), nil)
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	if !s.SQLModeWarming(ids[0]) {
		t.Fatal("no answer since the process started: not catching up")
	}
	s.copyAges.note(ids[0], answerAt(time.Now().Add(time.Millisecond), true))
	if s.SQLModeWarming(ids[0]) {
		t.Fatal("a tailed answer since the start: still catching up")
	}
}

// The status document carries it per server, for a server listed before its
// first statement too.
// An answer that read no table (a connection pool's SELECT 1) does not end
// the warm-up.
func TestSQLFreshWarming_aNoTableAnswerDoesNotEndIt_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, _ := freshTestServer(t, reg, 4, 3)
	if _, err := reg.SetSQLFresh(ids[0], true); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	s.copyAges.note(ids[0], copyAnswer{side: "copy", ok: true, ranAt: time.Now().Add(time.Millisecond)})
	if !s.SQLModeWarming(ids[0]) {
		t.Fatal("an answer that read no table ended the warm-up")
	}
}

// With every server fresh, a server the copy answered without a routing
// decision (routing off, a <server>@copy connection) is in the numbers with
// its warm-up read, not assumed.
func TestSQLFreshWarming_environmentListsAnsweredServers_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, _ := freshTestServer(t, reg, 0, 0)
	s.routing = newRoutingStats(time.Now(), nil)
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	s.copyAges.note(ids[0], answerAt(time.Now().Add(time.Millisecond), true))
	n, ok := s.RouterNumbers().Servers[ids[0]]
	if !ok || n.SQLMode != "fresh" || n.SQLModeWarming {
		t.Fatalf("listed %v, mode %q, warming %v; want listed, fresh, warmed up", ok, n.SQLMode, n.SQLModeWarming)
	}
}

func TestSQLFreshWarming_inTheNumbers_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, _ := freshTestServer(t, reg, 4, 3)
	if _, err := reg.SetSQLFresh(ids[0], true); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	n := s.RouterNumbers().Servers[ids[0]]
	if n.SQLMode != "fresh" || !n.SQLModeWarming {
		t.Fatalf("numbers: mode %q warming %v, want fresh and catching up", n.SQLMode, n.SQLModeWarming)
	}
}
