package consoleapp

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// The PostgreSQL port's status (#2770): its flags, its start-up lines, and
// the document.

func TestPGRouterStatusSettings(t *testing.T) {
	clearPGRouterEnv(t)
	t.Setenv("BINTRAIL_PG_ROUTER_STATUS_ADDR", "")
	t.Setenv("BINTRAIL_PG_ROUTER_STATUS_STATEMENTS", "")
	st, err := pgRouterSettingsFrom(pgRouterTestCmd(t))
	if err != nil {
		t.Fatal(err)
	}
	if st.StatusAddr != "" || st.StatusStatements {
		t.Fatalf("defaults: status on %q, statements %v", st.StatusAddr, st.StatusStatements)
	}
	t.Setenv("BINTRAIL_PG_ROUTER_STATUS_ADDR", "127.0.0.1:9092")
	t.Setenv("BINTRAIL_PG_ROUTER_STATUS_STATEMENTS", "true")
	if st, err = pgRouterSettingsFrom(pgRouterTestCmd(t)); err != nil || st.StatusAddr != "127.0.0.1:9092" || !st.StatusStatements {
		t.Fatalf("from the environment: %+v, %v", st, err)
	}
	if st, err = pgRouterSettingsFrom(pgRouterTestCmd(t, "--status-statements=false")); err != nil || st.StatusStatements {
		t.Fatalf("a flag over the environment: %+v, %v", st, err)
	}
	for _, tc := range []struct {
		args []string
		env  [2]string
		want string
	}{
		{[]string{"--status-addr", "9092"}, [2]string{}, "--status-addr"},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_STATUS_ADDR", "x:y"}, "BINTRAIL_PG_ROUTER_STATUS_ADDR"},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_STATUS_STATEMENTS", "maybe"}, "is not true or false"},
	} {
		t.Setenv("BINTRAIL_PG_ROUTER_STATUS_ADDR", "")
		t.Setenv("BINTRAIL_PG_ROUTER_STATUS_STATEMENTS", "")
		if tc.env[0] != "" {
			t.Setenv(tc.env[0], tc.env[1])
		}
		if _, err := pgRouterSettingsFrom(pgRouterTestCmd(t, tc.args...)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v %v: %v, want an error naming %q", tc.args, tc.env, err, tc.want)
		}
	}
}

func TestPGRouterStatusStartupLines(t *testing.T) {
	st := pgRouterSettings{Listen: "127.0.0.1:5433", Workers: 2, Threads: 2, MaxConns: 16, MaxSourceConns: 16}
	out := pgRouterStartupLines(st, "2GB", "the default", nil, 1, true)
	if strings.Contains(out, "/status") {
		t.Errorf("a status address that was not given:\n%s", out)
	}
	if !strings.Contains(out, "known to answer it as PostgreSQL does") {
		t.Errorf("the lines do not say how a statement is decided:\n%s", out)
	}
	st.StatusAddr = "127.0.0.1:9092"
	if out = pgRouterStartupLines(st, "2GB", "the default", nil, 1, true); !strings.Contains(out, "http://127.0.0.1:9092/status") || strings.Contains(out, "names tables") {
		t.Errorf("with --status-addr:\n%s", out)
	}
	st.StatusStatements = true
	if out = pgRouterStartupLines(st, "2GB", "the default", nil, 1, true); !strings.Contains(out, "names tables and columns") || !strings.Contains(out, "asks for no password") {
		t.Errorf("with --status-statements:\n%s", out)
	}
	st.StatusAddr = ""
	if out = pgRouterStartupLines(st, "2GB", "the default", nil, 1, true); !strings.Contains(out, "not served anywhere") {
		t.Errorf("--status-statements without an address:\n%s", out)
	}
}

func TestPGRouteStats(t *testing.T) {
	r := newPGRouteStats(time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC), true)
	r.note("s1", pgshim.RouteNote{Statement: "SELECT count(*) FROM t WHERE id = 5", Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, Took: time.Second})
	r.note("s1", pgshim.RouteNote{Statement: "SELECT count(*) FROM t WHERE id = 7", Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, Took: 2 * time.Second})
	r.note("s1", pgshim.RouteNote{Statement: "SELECT now()", Side: pgshim.RouteSource, Reason: pgshim.ReasonVeto, Failed: true})
	r.note("s1", pgshim.RouteNote{Statement: "INSERT INTO t VALUES ('secret')", Side: pgshim.RouteRefused, Reason: pgshim.ReasonReadOnly, Failed: true})
	d := r.doc("127.0.0.1:5433")
	n := d.Servers["s1"]
	if d.CountingSince != "2026-11-16T00:00:00Z" || n.Copy != 2 || n.Source != 1 || n.Refused != 1 {
		t.Fatalf("counts: %+v", d)
	}
	if n.Reasons[pgshim.ReasonAnswersTheSame] != 2 || n.Answers[pgshim.RouteSource].Errors != 1 || n.Answers[pgshim.RouteCopy].Seconds != 3 {
		t.Errorf("reasons and answers: %+v", n)
	}
	if len(n.Statements.Rows) != 3 || n.Statements.Rows[0].Statement != "select count(*) from t where id = ?" || n.Statements.Rows[0].Count != 2 {
		t.Errorf("statement rows: %+v", n.Statements.Rows)
	}
	for _, row := range n.Statements.Rows {
		if strings.Contains(row.Statement, "secret") {
			t.Errorf("a value in a row: %q", row.Statement)
		}
	}
	// Past the table's limit a shape is counted, not given a row.
	for i := range console.StatementTableLimit + 5 {
		r.note("s2", pgshim.RouteNote{Statement: "SELECT c" + strings.Repeat("x", i) + " FROM t", Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})
	}
	if n := r.doc("").Servers["s2"]; len(n.Statements.Rows) != console.StatementTableLimit || n.Statements.NotCounted != 5 || n.Copy != uint64(console.StatementTableLimit+5) {
		t.Errorf("full table: %d rows, %d not counted, %d copy", len(n.Statements.Rows), n.Statements.NotCounted, n.Copy)
	}
	// Without the table no shape is kept.
	plain := newPGRouteStats(time.Now(), false)
	plain.note("s1", pgshim.RouteNote{Statement: "SELECT 1", Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})
	if n := plain.doc("").Servers["s1"]; n.Statements != nil || n.Copy != 1 {
		t.Errorf("without the table: %+v", n)
	}
	var none *pgRouteStats
	if none.forServer("x") != nil {
		t.Error("a nil tally gives a callback")
	}
	none.setBarred("x", "why") // no panic
}

func TestPGForwardBarred(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	rules := ""
	barred := pgForwardBarred("srv-2770", func() string { return rules }, r, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if why := barred(); why != "" {
		t.Fatalf("no rules: %q", why)
	}
	if _, ok := r.doc("").Servers["srv-2770"]; ok {
		t.Error("a server with no rule got an entry")
	}
	rules = "this server hides some tables"
	if why := barred(); why != pgHiddenBarred {
		t.Fatalf("with rules: %q", why)
	}
	if n := r.doc("").Servers["srv-2770"]; n.ForwardingOff != pgHiddenBarred {
		t.Errorf("status: %+v", n)
	}
	rules = ""
	if why := barred(); why != "" || r.doc("").Servers["srv-2770"].ForwardingOff != "" {
		t.Errorf("rules removed: %q", why)
	}
	if barred := pgForwardBarred("srv-nil", nil, nil, slog.Default()); barred() != "" {
		t.Error("no gate bars")
	}
}
