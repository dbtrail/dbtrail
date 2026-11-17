package consoleapp

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #2756: the PostgreSQL router's start-up lines say how fresh its answers
// are and who decides, in each of the three settings, and name the
// PostgreSQL servers in fresh mode.
func TestPGRouterFreshLines(t *testing.T) {
	for _, tc := range []struct {
		name, forced, read string
		fresh              []string
		want, wantNot      []string
	}{
		{"each server's own choice", "", "", []string{"crm", "shop"},
			[]string{"each server answers in its own mode, fresh unless fast was saved for it.", "Fresh: each table's newest captured changes",
				"up to a fifth of the memory", "BINTRAIL_SQL_TAIL_MAX_BYTES", "Fast: answers come from each table's last snapshot",
				"on its " + console.PageSnapshots + " page", "port mode <server> fresh", "docker compose up -d pg-router",
				"2 servers are in fresh mode: crm, shop."},
			[]string{"decides nothing", "up -d router)"}},
		{"a value that is not on or off", "", "maybe", nil,
			[]string{`BINTRAIL_SQL_TAILS="maybe" is not on or off, and decides nothing`}, []string{"in fresh mode:"}},
		{"fresh for every server", console.SQLModeForcedFresh, "on", nil,
			[]string{"fresh mode for every server (BINTRAIL_SQL_TAILS=on)", "a fifth of the memory", "Unset it"}, []string{"Fast:"}},
		{"fast for every server", console.SQLModeForcedFast, "off", nil,
			[]string{"fast mode for every server (BINTRAIL_SQL_TAILS=off)", "whatever is saved for a server"}, []string{"Fresh:", "a fifth"}},
		{"one server fresh", "", "", []string{"shop"},
			[]string{"1 server is in fresh mode: shop."}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := pgRouterFreshLines(tc.forced, tc.read, tc.fresh)
			t.Logf("start-up lines:\n%s", out)
			for _, w := range tc.want {
				if !strings.Contains(out, w) {
					t.Errorf("the lines do not say %q:\n%s", w, out)
				}
			}
			for _, w := range tc.wantNot {
				if strings.Contains(out, w) {
					t.Errorf("the lines say %q:\n%s", w, out)
				}
			}
			for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
				if !strings.HasPrefix(line, "PostgreSQL router: ") {
					t.Errorf("a line that does not say who speaks: %q", line)
				}
			}
		})
	}
	// The line above them no longer says the copy is as old as its last
	// snapshot update whatever the mode.
	out := pgRouterStartupLines(pgRouterSettings{Listen: "127.0.0.1:5433", Workers: 2, Threads: 2, MaxConns: 16}, "2GB", "the default", nil, 1, true)
	if strings.Contains(out, "as of its last snapshot update") || !strings.Contains(out, "in each server's mode") {
		t.Errorf("start-up lines:\n%s", out)
	}
}
