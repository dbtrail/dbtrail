package console

import (
	"testing"
	"time"
)

// #2682: a server the copy only ever answered with nothing to route (Engine
// Routing off, a <server>@copy connection) is listed by its unrouted
// answers. It must still say its mode and where the mode comes from: an
// entry with no mode is read by the status document as an unlisted server,
// fast by default.
func TestRouterNumbers_aServerAnsweredOnlyUnroutedSaysItsMode_2682(t *testing.T) {
	for _, tc := range []struct {
		name       string
		save       *bool
		env        string
		mode, from string
	}{
		{"nothing saved", nil, "", "fresh", SQLModeFromDefault},
		{"Fast saved", boolPtr(false), "", "fast", SQLModeFromSaved},
		{"Fresh saved", boolPtr(true), "", "fresh", SQLModeFromSaved},
		{"every server fast", nil, "off", "fast", SQLModeFromEnvironment},
		{"every server fresh, Fast saved", boolPtr(false), "on", "fresh", SQLModeFromEnvironment},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg, ids := freshTestRegistry(t, "a")
			if tc.save != nil {
				if _, err := reg.SetSQLFresh(ids[0], *tc.save); err != nil {
					t.Fatal(err)
				}
			}
			s, _ := freshTestServer(t, reg, 4, 3)
			t.Setenv("BINTRAIL_SQL_TAILS", tc.env)
			s.applySQLFresh()
			(&SQLOnCopy{s: s, id: ids[0]}).NoteAnswered(200*time.Millisecond, false)

			n, ok := s.RouterNumbers().Servers[ids[0]]
			if !ok {
				t.Fatal("the server is not listed")
			}
			if n.Unrouted == nil || n.Unrouted.Count != 1 {
				t.Fatalf("the unrouted answer is not counted: %+v", n.Unrouted)
			}
			if n.SQLMode != tc.mode || n.SQLModeFrom != tc.from {
				t.Fatalf("mode %q from %q, want %q from %q", n.SQLMode, n.SQLModeFrom, tc.mode, tc.from)
			}
			if wantWarming := tc.mode == "fresh"; n.SQLModeWarming != wantWarming {
				t.Fatalf("warming %v for a %s server that has not answered with its tails", n.SQLModeWarming, tc.mode)
			}
		})
	}
}

// A server with no answer at all and Fast saved is listed too, with its
// source, and one with nothing saved is listed as fresh.
func TestRouterNumbers_aServerWithNoAnswerSaysItsMode_2682(t *testing.T) {
	reg, ids := freshTestRegistry(t, "a", "b")
	if _, err := reg.SetSQLFresh(ids[0], false); err != nil {
		t.Fatal(err)
	}
	s, _ := freshTestServer(t, reg, 4, 3)
	s.applySQLFresh()
	got := s.RouterNumbers().Servers
	if n := got[ids[0]]; n.SQLMode != "fast" || n.SQLModeFrom != SQLModeFromSaved {
		t.Fatalf("Fast saved, no answer: mode %q from %q", n.SQLMode, n.SQLModeFrom)
	}
	if n := got[ids[1]]; n.SQLMode != "fresh" || n.SQLModeFrom != SQLModeFromDefault {
		t.Fatalf("nothing saved, no answer: mode %q from %q", n.SQLMode, n.SQLModeFrom)
	}
}
