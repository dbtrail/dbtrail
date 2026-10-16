package consoleapp

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The per-statement table is off unless asked for (#2360), the flag wins
// over the environment, and a value that is not a yes or no stops the start.
func TestRouterSettings_statementTable(t *testing.T) {
	t.Setenv("BINTRAIL_CONSOLE_TOKEN", "t")
	for _, tc := range []struct {
		env  string
		args []string
		want bool
	}{
		{"", nil, false},
		{"1", nil, true},
		{"true", nil, true},
		{"0", nil, false},
		{"1", []string{"--statement-table=false"}, false},
		{"", []string{"--statement-table"}, true},
	} {
		t.Setenv("BINTRAIL_ROUTER_STATEMENT_TABLE", tc.env)
		st, err := maxWaitSettings(t, tc.args...)
		if err != nil || st.StatementTable != tc.want {
			t.Errorf("env %q args %v: %v, %v; want %v", tc.env, tc.args, st.StatementTable, err, tc.want)
		}
	}
	t.Setenv("BINTRAIL_ROUTER_STATEMENT_TABLE", "maybe")
	if _, err := maxWaitSettings(t); err == nil || !strings.Contains(err.Error(), "BINTRAIL_ROUTER_STATEMENT_TABLE") {
		t.Fatalf("maybe: err = %v; want a refusal naming the variable", err)
	}
}

// Turned on, the startup lines say what it shows and to whom.
func TestRouterStartupLines_statementTable(t *testing.T) {
	base := routerStartup{Listen: "127.0.0.1:3309"}
	if s := routerStartupLines(base); strings.Contains(s, "statement table") {
		t.Fatalf("off, the lines mention it:\n%s", s)
	}
	on := base
	on.StatementTable, on.MetricsAddr = true, "127.0.0.1:9091"
	s := routerStartupLines(on)
	for _, want := range []string{"statement table is ON", "127.0.0.1:9091/status", "names tables and columns", "no password"} {
		if !strings.Contains(s, want) {
			t.Errorf("on: no %q in\n%s", want, s)
		}
	}
	on.MetricsAddr = ""
	if s := routerStartupLines(on); !strings.Contains(s, "no --metrics-addr") {
		t.Errorf("on without an address: \n%s", s)
	}
}

// The switch reaches the console the router builds: on, its numbers carry
// the table; off, they do not.
func TestNewRouterConsole_statementTable(t *testing.T) {
	for _, on := range []bool{false, true} {
		f := newRouterFiles(t)
		st := f.settings("tok")
		st.StatementTable = on
		srv, runner, err := newRouterConsole(st, console.SQLPool{Workers: 1, Threads: 2})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(runner.Close)
		if got := srv.RouterNumbers().StatementTable; got != on {
			t.Errorf("switch %v: the router's numbers say %v", on, got)
		}
	}
}
