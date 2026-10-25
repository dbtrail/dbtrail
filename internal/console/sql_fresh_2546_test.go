package console

import (
	"os"
	"slices"
	"testing"
	"time"
)

// #2546: fresh mode per server, saved on its registry entry and applied by
// the process that serves the port when it reads the registry again.

func freshTestRegistry(t *testing.T, names ...string) (*Registry, []string) {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, n := range names {
		e, err := reg.Add(ServerEntry{Name: n, DSN: "u:p@tcp(h:3306)/" + n})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, e.ID)
	}
	return reg, ids
}

// fakePool records what the slot count was set to, in order.
type fakePool struct{ set []int }

func (p *fakePool) SetMaxInFlight(n int) error { p.set = append(p.set, n); return nil }

func freshTestServer(t *testing.T, reg *Registry, fast, fresh int) (*Server, *fakePool) {
	t.Helper()
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	p := &fakePool{}
	s := &Server{cm: &connManager{reg: reg}}
	s.fresh.pool, s.fresh.fastWorkers, s.fresh.freshWorkers = p, fast, fresh
	return s, p
}

func TestSQLFresh_anEntryWithoutTheFieldIsFast_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, p := freshTestServer(t, reg, 4, 3)
	s.applySQLFresh()
	if s.tailsOnFor(ids[0]) {
		t.Fatal("a server never switched reads tails")
	}
	if len(p.set) != 0 {
		t.Fatalf("the pool was resized with no server fresh: %v", p.set)
	}
}

func TestSQLFresh_setAndClearAreTheOnlyWriter_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	if _, err := reg.SetSQLFresh(ids[0], true); err != nil {
		t.Fatal(err)
	}
	// A console edit built from any copy keeps the saved mode.
	e, _ := reg.Get(ids[0])
	e.SQLFresh = false
	e.Name = "renamed"
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	if got, _ := reg.Get(ids[0]); !got.SQLFresh {
		t.Fatal("an edit that did not mean to change the mode turned it off")
	}
	// Survives a reload from disk.
	again, err := LoadRegistry(reg.Path())
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := again.Get(ids[0]); !got.SQLFresh {
		t.Fatal("the saved mode did not survive a reload")
	}
	// A new entry never starts fresh, whatever it was built from.
	n, err := reg.Add(ServerEntry{Name: "b", DSN: "u:p@tcp(h:3306)/b", SQLFresh: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := reg.Get(n.ID); got.SQLFresh {
		t.Fatal("Add stored fresh mode; SetSQLFresh is its only writer")
	}
	if _, err := reg.SetSQLFresh("nope", true); err != ErrUnknownServer {
		t.Fatalf("an unknown server: %v, want ErrUnknownServer", err)
	}
}

// The first server turned fresh lowers the pool BEFORE its statements read
// tails; the second does not lower it again; one of two turned fast does not
// raise it; the last one does, after it stopped reading them.
func TestSQLFresh_thePoolMovesOnTheFirstAndTheLast_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a", "b")
	s, p := freshTestServer(t, reg, 4, 3)
	var seenOn bool
	p2 := &orderPool{fakePool: p, before: func() { seenOn = s.tailsOnFor(ids[0]) }}
	s.fresh.pool = p2

	mustFresh := func(i int, on bool) {
		t.Helper()
		if _, err := reg.SetSQLFresh(ids[i], on); err != nil {
			t.Fatal(err)
		}
		s.applySQLFresh()
	}
	mustFresh(0, true)
	if !slices.Equal(p.set, []int{3}) {
		t.Fatalf("first fresh: pool set %v, want [3]", p.set)
	}
	if seenOn {
		t.Fatal("the server read tails before the pool was lowered")
	}
	if !s.tailsOnFor(ids[0]) || s.tailsOnFor(ids[1]) {
		t.Fatal("after the first fresh: only that server reads tails")
	}
	mustFresh(1, true)
	mustFresh(0, false)
	if !slices.Equal(p.set, []int{3}) {
		t.Fatalf("second fresh and one of two fast: pool set %v, want only [3]", p.set)
	}
	if s.tailsOnFor(ids[0]) || !s.tailsOnFor(ids[1]) {
		t.Fatal("after one of two fast: only the other reads tails")
	}
	p2.before = func() { seenOn = s.tailsOnFor(ids[1]) }
	mustFresh(1, false)
	if !slices.Equal(p.set, []int{3, 4}) {
		t.Fatalf("last fast: pool set %v, want [3 4]", p.set)
	}
	if seenOn {
		t.Fatal("the pool was raised while the last server still read tails")
	}
}

type orderPool struct {
	*fakePool
	before func()
}

func (p *orderPool) SetMaxInFlight(n int) error {
	if p.before != nil {
		p.before()
	}
	return p.fakePool.SetMaxInFlight(n)
}

// A server deleted while fresh counts as turned fast.
func TestSQLFresh_aDeletedServerGivesBackThePool_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, p := freshTestServer(t, reg, 4, 3)
	if _, err := reg.SetSQLFresh(ids[0], true); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	if err := reg.Delete(ids[0]); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	if !slices.Equal(p.set, []int{3, 4}) {
		t.Fatalf("pool set %v, want [3 4]", p.set)
	}
	if s.tailsOnFor(ids[0]) {
		t.Fatal("a deleted server still reads tails")
	}
}

// Several switches between two readings: only where they ended counts.
func TestSQLFresh_onlyTheLastStateBetweenReadingsCounts_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, p := freshTestServer(t, reg, 4, 3)
	for _, on := range []bool{true, false, true, false} {
		if _, err := reg.SetSQLFresh(ids[0], on); err != nil {
			t.Fatal(err)
		}
	}
	s.applySQLFresh()
	if len(p.set) != 0 || s.tailsOnFor(ids[0]) {
		t.Fatalf("ended fast: pool set %v, tails %v", p.set, s.tailsOnFor(ids[0]))
	}
}

// The same number of statements either way (the cores decide, or
// --concurrency does): the pool is never touched.
func TestSQLFresh_samePoolEitherWayIsNotTouched_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, p := freshTestServer(t, reg, 2, 2)
	if _, err := reg.SetSQLFresh(ids[0], true); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	if len(p.set) != 0 || !s.tailsOnFor(ids[0]) {
		t.Fatalf("pool set %v, tails %v; want untouched and fresh", p.set, s.tailsOnFor(ids[0]))
	}
}

// BINTRAIL_SQL_TAILS=on makes every server fresh and the switches decide
// nothing; any other value (off, a typo) leaves each server's switch to
// decide. The pool is planned for fresh mode at startup then, never moved.
func TestSQLFresh_theEnvironmentDecidesOnlyWhenOn_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a", "b")
	if _, err := reg.SetSQLFresh(ids[1], true); err != nil {
		t.Fatal(err)
	}
	for v, want := range map[string][2]bool{
		"on": {true, true}, "fresh": {true, true},
		"off": {false, true}, "onn": {false, true}, "": {false, true},
	} {
		s, _ := freshTestServer(t, reg, 0, 0)
		t.Setenv("BINTRAIL_SQL_TAILS", v)
		s.applySQLFresh()
		got := [2]bool{s.tailsOnFor(ids[0]), s.tailsOnFor(ids[1])}
		if got != want {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: tails %v, want %v", v, got, want)
		}
		mode, from := s.SQLModeOf(ids[0])
		if v == "on" || v == "fresh" {
			if mode != "fresh" || from != SQLModeFromEnvironment {
				t.Errorf("BINTRAIL_SQL_TAILS=%q: mode %q from %q, want fresh from the environment", v, mode, from)
			}
		} else if mode != "fast" || from != SQLModeFromDefault {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: unswitched server mode %q from %q, want fast by default", v, mode, from)
		}
		if mode, from := s.SQLModeOf(ids[1]); v != "on" && v != "fresh" && (mode != "fresh" || from != SQLModeFromSaved) {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: switched server mode %q from %q, want fresh as saved", v, mode, from)
		}
	}
}

// A newer registry, opened read-only, refuses the switch.
func TestSQLFresh_aReadOnlyRegistryRefuses_2546(t *testing.T) {
	path := t.TempDir() + "/console-servers.yaml"
	if err := os.WriteFile(path, []byte("version: 99\nservers:\n- id: s1\n  name: prod\n  index_dsn: u:p@tcp(h:3306)/db\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetSQLFresh("s1", true); err != ErrRegistryReadOnly {
		t.Fatalf("read-only registry: %v, want ErrRegistryReadOnly", err)
	}
}

// A server turned fast stops paying for its tails: its keeper is dropped, so
// nothing follows its index every second any more.
func TestSQLFresh_turnedFastDropsTheKeeper_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, _ := freshTestServer(t, reg, 4, 3)
	s.tailCapturesHook = func(string) (func(string, string) bool, bool) { return nil, true }
	if _, err := reg.SetSQLFresh(ids[0], true); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	k := s.tailKeeperFor(ids[0], &bundle{})
	if k == nil {
		t.Fatal("fixture: no keeper for a fresh server")
	}
	if _, err := reg.SetSQLFresh(ids[0], false); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	s.tails.mu.Lock()
	_, kept := s.tails.by[ids[0]]
	s.tails.mu.Unlock()
	if kept {
		t.Fatal("the keeper of a server turned fast is still there")
	}
	if k.life.Err() == nil {
		t.Fatal("the dropped keeper was not closed: its builds and follower keep running")
	}
}

// A process started after a server was switched applies it without waiting
// for a registry reading (the capture process never reads it again).
func TestSQLFresh_appliedWhenTheServerStarts_2546(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	reg, ids := freshTestRegistry(t, "a")
	if _, err := reg.SetSQLFresh(ids[0], true); err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	if !srv.tailsOnFor(ids[0]) {
		t.Fatal("a server switched before the process started is fast")
	}
}

// A statement that passed the gate just before its server turned fast must
// not bring the dropped keeper back: it would follow the index and hold tails
// with the pool already raised, and nothing would drop it again.
func TestSQLFresh_aLateStatementDoesNotBringTheKeeperBack_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, _ := freshTestServer(t, reg, 4, 3)
	s.tailCapturesHook = func(string) (func(string, string) bool, bool) { return nil, true }
	if _, err := reg.SetSQLFresh(ids[0], true); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	if !s.tailsOnFor(ids[0]) {
		t.Fatal("fixture: the server is not fresh")
	}
	// The statement has passed the gate; the server turns fast before it
	// asks for its keeper.
	if _, err := reg.SetSQLFresh(ids[0], false); err != nil {
		t.Fatal(err)
	}
	s.applySQLFresh()
	if k := s.tailKeeperFor(ids[0], &bundle{}); k != nil {
		t.Fatal("a keeper was made for a server in fast mode")
	}
	s.tails.mu.Lock()
	_, made := s.tails.by[ids[0]]
	s.tails.mu.Unlock()
	if made {
		t.Fatal("a keeper for a server in fast mode was stored")
	}
}

// The pool is raised after the dropped keepers' builds end, but a build
// stuck on a read does not keep it small past the bound.
func TestSQLFresh_settleWaitsForBuildsWithinTheBound_2546(t *testing.T) {
	k := &tailKeeper{}
	k.wg.Add(1)
	go func() { time.Sleep(50 * time.Millisecond); k.wg.Done() }()
	start := time.Now()
	settleWithin([]*tailKeeper{k}, 5*time.Second)
	if d := time.Since(start); d < 40*time.Millisecond {
		t.Fatalf("returned after %v, before the build ended", d)
	}
	stuck := &tailKeeper{}
	stuck.wg.Add(1)
	defer stuck.wg.Done()
	start = time.Now()
	settleWithin([]*tailKeeper{stuck}, 80*time.Millisecond)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("a stuck build held it %v, past the bound", d)
	}
}
