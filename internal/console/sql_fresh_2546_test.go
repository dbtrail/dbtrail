package console

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v2"
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
	s := &Server{cm: &connManager{reg: reg}, routing: newRoutingStats(time.Now(), nil)}
	s.fresh.pool, s.fresh.fastWorkers, s.fresh.freshWorkers = p, fast, fresh
	return s, p
}

// An entry with no choice saved is fresh: the pool is lowered for it before
// its statements read tails. With Fast saved it is fast and the pool is not
// touched.
func TestSQLFresh_anEntryWithoutTheFieldIsFresh(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	s, p := freshTestServer(t, reg, 4, 3)
	s.applySQLFresh()
	if !s.tailsOnFor(ids[0]) {
		t.Fatal("a server with no choice saved does not read tails; fresh is the default")
	}
	if !slices.Equal(p.set, []int{3}) {
		t.Fatalf("the pool was set %v for the first fresh server, want [3]", p.set)
	}
	if mode, from := s.SQLModeOf(ids[0]); mode != "fresh" || from != SQLModeFromDefault {
		t.Fatalf("mode %q from %q, want fresh by default", mode, from)
	}
}

func TestSQLFresh_anEntryWithFastSavedIsFast(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	if _, err := reg.SetSQLFresh(ids[0], false); err != nil {
		t.Fatal(err)
	}
	s, p := freshTestServer(t, reg, 4, 3)
	s.applySQLFresh()
	if s.tailsOnFor(ids[0]) {
		t.Fatal("a server with Fast saved reads tails")
	}
	if len(p.set) != 0 {
		t.Fatalf("the pool was resized with no server fresh: %v", p.set)
	}
	if mode, from := s.SQLModeOf(ids[0]); mode != "fast" || from != SQLModeFromSaved {
		t.Fatalf("mode %q from %q, want fast as saved", mode, from)
	}
}

// The server given on the command line has no entry: fast unless the
// setting says on.
func TestSQLFresh_aServerWithNoEntryIsFast(t *testing.T) {
	reg, _ := freshTestRegistry(t, "a")
	s, _ := freshTestServer(t, reg, 4, 3)
	s.applySQLFresh()
	if s.tailsOnFor(bootServerID) {
		t.Fatal("the command-line server reads tails with nothing set")
	}
	if mode, from := s.SQLModeOf(bootServerID); mode != "fast" || from != SQLModeFromDefault {
		t.Fatalf("mode %q from %q, want fast by default", mode, from)
	}
}

func TestSQLFresh_setAndClearAreTheOnlyWriter_2546(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	if _, err := reg.SetSQLFresh(ids[0], false); err != nil {
		t.Fatal(err)
	}
	// A console edit built from any copy keeps the saved mode: here Fast,
	// which is not the default, against an edit that says nothing and one
	// that says Fresh.
	for _, sent := range [][2]any{{"", false}, {"fresh", true}, {"", true}} {
		e, _ := reg.Get(ids[0])
		e.SQLMode, e.SQLFresh = sent[0].(string), sent[1].(bool)
		e.Name = "renamed"
		if err := reg.Update(e); err != nil {
			t.Fatal(err)
		}
		if got, _ := reg.Get(ids[0]); got.SQLFreshOn() || !got.SQLFreshSaved() {
			t.Fatalf("an edit that sent %v and did not mean to change the mode changed it: sql_mode %q sql_fresh %v", sent, got.SQLMode, got.SQLFresh)
		}
	}
	// Survives a reload from disk.
	again, err := LoadRegistry(reg.Path())
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := again.Get(ids[0]); got.SQLFreshOn() || !got.SQLFreshSaved() {
		t.Fatal("a saved Fast did not survive a reload: it must be written to the file, not left absent")
	}
	// A new entry never starts with a saved choice, whatever it was built
	// from: it is fresh by the default.
	n, err := reg.Add(ServerEntry{Name: "b", DSN: "u:p@tcp(h:3306)/b", SQLMode: "fast", SQLFresh: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := reg.Get(n.ID); got.SQLFreshSaved() || !got.SQLFreshOn() {
		t.Fatal("Add stored a mode; SetSQLFresh is its only writer")
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
	for _, id := range ids { // both fast to begin with: Fast saved
		if _, err := reg.SetSQLFresh(id, false); err != nil {
			t.Fatal(err)
		}
	}
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

// BINTRAIL_SQL_TAILS=on makes every server fresh and =off every server
// fast, the entries deciding nothing; unset or a typo leaves each entry to
// decide, fresh when it saved nothing. Three servers: nothing saved, Fresh
// saved, Fast saved.
func TestSQLFresh_theEnvironmentDecidesWhenOnOrOff(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a", "b", "c")
	if _, err := reg.SetSQLFresh(ids[1], true); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetSQLFresh(ids[2], false); err != nil {
		t.Fatal(err)
	}
	own := [3]bool{true, true, false}
	ownFrom := [3]string{SQLModeFromDefault, SQLModeFromSaved, SQLModeFromSaved}
	for v, forced := range map[string]string{
		"on": "fresh", "fresh": "fresh", "off": "fast", "fast": "fast", "0": "fast",
		"onn": "", "": "",
	} {
		s, _ := freshTestServer(t, reg, 0, 0)
		t.Setenv("BINTRAIL_SQL_TAILS", v)
		s.applySQLFresh()
		for i, id := range ids {
			wantOn, wantFrom := own[i], ownFrom[i]
			switch forced {
			case "fresh":
				wantOn, wantFrom = true, SQLModeFromEnvironment
			case "fast":
				wantOn, wantFrom = false, SQLModeFromEnvironment
			}
			wantMode := "fast"
			if wantOn {
				wantMode = "fresh"
			}
			if got := s.tailsOnFor(id); got != wantOn {
				t.Errorf("BINTRAIL_SQL_TAILS=%q server %d: reads tails %v, want %v", v, i, got, wantOn)
			}
			if mode, from := s.SQLModeOf(id); mode != wantMode || from != wantFrom {
				t.Errorf("BINTRAIL_SQL_TAILS=%q server %d: mode %q from %q, want %q from %q", v, i, mode, from, wantMode, wantFrom)
			}
		}
	}
}

// The setting turned to off under a running process is not a case: it is
// read from the environment, which does not change. What does change is the
// list: a server whose Fresh was saved turns fast when Fast is saved, and a
// server whose saved choice is removed from the file by hand is fresh again.
func TestSQLFresh_removingTheSavedChoiceIsFreshAgain(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	if _, err := reg.SetSQLFresh(ids[0], false); err != nil {
		t.Fatal(err)
	}
	s, _ := freshTestServer(t, reg, 0, 0)
	s.applySQLFresh()
	if s.tailsOnFor(ids[0]) {
		t.Fatal("Fast saved, and the server reads tails")
	}
	data, err := os.ReadFile(reg.Path())
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, line := range strings.SplitAfter(string(data), "\n") {
		if !strings.Contains(line, "sql_fresh") {
			kept = append(kept, line)
		}
	}
	if err := os.WriteFile(reg.Path(), []byte(strings.Join(kept, "")), 0o600); err != nil {
		t.Fatal(err)
	}
	again, err := LoadRegistry(reg.Path())
	if err != nil {
		t.Fatal(err)
	}
	s2, _ := freshTestServer(t, again, 0, 0)
	s2.applySQLFresh()
	if !s2.tailsOnFor(ids[0]) {
		t.Fatal("with the saved choice gone from the file the server is not fresh")
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

// What a server's entry says, as the file holds it. sql_mode is the choice;
// sql_fresh is the key builds before the default changed wrote, read when
// sql_mode says nothing; a sql_mode this build does not know is no choice.
func TestSQLFresh_whatTheFileSays(t *testing.T) {
	for _, tc := range []struct {
		name, line string
		on, saved  bool
	}{
		{"nothing saved", "", true, false},
		{"fresh saved", "sql_mode: fresh\n", true, true},
		{"fast saved", "sql_mode: fast\n", false, true},
		{"fresh saved by an older build", "sql_fresh: true\n", true, true},
		{"a false an older build never wrote", "sql_fresh: false\n", true, false},
		{"fast here, then Fresh chosen on an older build", "sql_mode: fast\nSAME sql_fresh: true\n", false, true},
		{"a mode this build does not know", "sql_mode: slow\n", true, false},
		{"a mode in capitals is not one", "sql_mode: FAST\n", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The file a registry writes, with the line put in by hand the
			// way an older or a newer build would have left it.
			made, ids := freshTestRegistry(t, "a")
			data, err := os.ReadFile(made.Path())
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "sql_fresh") {
				t.Fatalf("a new entry's file already holds sql_fresh:\n%s", data)
			}
			var out []string
			for _, line := range strings.SplitAfter(string(data), "\n") {
				out = append(out, line)
				if trimmed := strings.TrimLeft(line, " "); tc.line != "" && strings.HasPrefix(trimmed, "name: ") {
					indent := line[:len(line)-len(trimmed)]
					// "SAME " marks a second key, at the same indentation.
					out = append(out, indent+strings.ReplaceAll(tc.line, "SAME ", indent))
				}
			}
			if err := os.WriteFile(made.Path(), []byte(strings.Join(out, "")), 0o600); err != nil {
				t.Fatal(err)
			}
			reg, err := LoadRegistry(made.Path())
			if err != nil {
				t.Fatalf("%v\n%s", err, strings.Join(out, ""))
			}
			e, ok := reg.Get(ids[0])
			if !ok {
				t.Fatal("the entry was not read")
			}
			if e.SQLFreshOn() != tc.on || e.SQLFreshSaved() != tc.saved {
				t.Fatalf("on=%v saved=%v, want on=%v saved=%v", e.SQLFreshOn(), e.SQLFreshSaved(), tc.on, tc.saved)
			}
		})
	}
	// Saving writes sql_mode either way. Fresh also writes sql_fresh: true,
	// for a router of an older build; Fast leaves that key out.
	reg, ids := freshTestRegistry(t, "a")
	for _, on := range []bool{false, true, false} {
		if _, err := reg.SetSQLFresh(ids[0], on); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(reg.Path())
		if err != nil {
			t.Fatal(err)
		}
		want := map[bool]string{true: "sql_mode: fresh", false: "sql_mode: fast"}[on]
		if !strings.Contains(string(data), want) {
			t.Fatalf("after saving %v the file does not hold %q:\n%s", on, want, data)
		}
		if strings.Contains(string(data), "sql_fresh: true") != on || strings.Contains(string(data), "sql_fresh: false") {
			t.Fatalf("after saving %v, sql_fresh in the file:\n%s", on, data)
		}
	}
}

// A build from before the default changed re-saves the file: it knows
// sql_fresh, writes it only when true, and keeps every key it does not know.
// A Fast saved here survives that, which a false in sql_fresh would not.
func TestSQLFresh_fastSurvivesAnOlderBuildsSave(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a")
	if _, err := reg.SetSQLFresh(ids[0], false); err != nil {
		t.Fatal(err)
	}
	type olderEntry struct {
		ID       string         `yaml:"id"`
		SQLFresh bool           `yaml:"sql_fresh,omitempty"`
		Extra    map[string]any `yaml:",inline"`
	}
	type olderFile struct {
		Version int            `yaml:"version"`
		Servers []olderEntry   `yaml:"servers"`
		Extra   map[string]any `yaml:",inline"`
	}
	data, err := os.ReadFile(reg.Path())
	if err != nil {
		t.Fatal(err)
	}
	var f olderFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	resaved, err := yaml.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(reg.Path(), resaved, 0o600); err != nil {
		t.Fatal(err)
	}
	again, err := LoadRegistry(reg.Path())
	if err != nil {
		t.Fatalf("%v\n%s", err, resaved)
	}
	if e, _ := again.Get(ids[0]); e.SQLFreshOn() || !e.SQLFreshSaved() {
		t.Fatalf("after an older build's save the server is no longer Fast saved:\n%s", resaved)
	}
}

// BINTRAIL_SQL_TAILS as SQLModeSetting reads it.
func TestSQLModeSetting(t *testing.T) {
	for _, tc := range []struct{ value, forced string }{
		{"", ""}, {"   ", ""},
		{"on", SQLModeForcedFresh}, {"ON", SQLModeForcedFresh}, {"true", SQLModeForcedFresh},
		{"1", SQLModeForcedFresh}, {"yes", SQLModeForcedFresh}, {" Fresh ", SQLModeForcedFresh},
		{"off", SQLModeForcedFast}, {"OFF", SQLModeForcedFast}, {"false", SQLModeForcedFast},
		{"0", SQLModeForcedFast}, {"no", SQLModeForcedFast}, {" fast ", SQLModeForcedFast},
		{"onn", ""}, {"of", ""}, {"on off", ""}, {"on\noff", ""}, {"2", ""},
	} {
		t.Setenv("BINTRAIL_SQL_TAILS", tc.value)
		forced, read := SQLModeSetting()
		if forced != tc.forced || read != strings.TrimSpace(tc.value) {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: forced %q read %q, want forced %q", tc.value, forced, read, tc.forced)
		}
		if on, _ := TailsSetting(); on != (tc.forced == SQLModeForcedFresh) {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: TailsSetting on=%v", tc.value, on)
		}
		if tailsForcedOff() != (tc.forced == SQLModeForcedFast) {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: tailsForcedOff=%v", tc.value, tailsForcedOff())
		}
	}
}
