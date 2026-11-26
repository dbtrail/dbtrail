package pgshim

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The port's status statement (#3021): SHOW dbtrail.router_status, answered
// by the port in every mode, never sent to a side, and never an answer of
// its own.

// statusLogin is a port whose copy answers one row, or fails for a table
// named "missing", and whose source (when forward) cannot be reached: a
// statement the client gets 08001 for was sent there.
type statusLogin struct {
	forward bool
	monitor bool
	// status, when set, is the binding's Status.
	status func(context.Context, LastAnswer) [][2]string
	mu     sync.Mutex
	ran    []string
	notes  []RouteNote
}

func (l *statusLogin) run(_ context.Context, st string, _ RunOptions) (sqlsandbox.Result, error) {
	l.mu.Lock()
	l.ran = append(l.ran, st)
	l.mu.Unlock()
	if strings.Contains(st, "missing") {
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Catalog Error: Table with name missing does not exist!"}
	}
	return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{int32(7)}}}, nil
}

func (l *statusLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	if l.monitor {
		return &Binding{Actor: "monitor", Schema: "public", Monitor: true}, nil
	}
	b := &Binding{Run: l.run, Actor: "server:orders", Schema: "public", Status: l.status, Routed: func(n RouteNote) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.notes = append(l.notes, n)
	}}
	if l.forward {
		// Routing by plan off (#2985): the source here cannot be reached,
		// and what this tests is what the status says of the screen's
		// decisions.
		b.Forward = &Forward{DSN: "postgres://fwd:pw@127.0.0.1:1/db?sslmode=disable&connect_timeout=2", Cost: &CostPolicy{}}
	}
	return b, nil
}

// seen is how many statements the copy ran and how many answers the host
// was told of.
func (l *statusLogin) seen() (ran, notes int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.ran), len(l.notes)
}

func (l *statusLogin) dial(t *testing.T) *statusConn {
	t.Helper()
	c, err := dialStatus(t, serveAddrCfg(t, Config{Login: l.login}), nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// statusAnswer runs the status statement and returns its rows as
// "name=value", failing the test on anything but rows, and the transaction
// status after it.
func statusAnswer(t *testing.T, c *statusConn, stmt string) (rows []string, tx string) {
	t.Helper()
	got := c.query(stmt)
	if len(got) < 3 || got[0] != "Columns name:25,value:25" || got[len(got)-2] != "CommandComplete SHOW" {
		t.Fatalf("%s: %q", stmt, got)
	}
	for _, m := range got[1 : len(got)-2] {
		cells, ok := strings.CutPrefix(m, "DataRow ")
		if !ok {
			t.Fatalf("%s: %q", stmt, got)
		}
		rows = append(rows, strings.Replace(cells, "|", "=", 1))
	}
	return rows, strings.TrimPrefix(got[len(got)-1], "ReadyForQuery ")
}

func wantRows(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s:\n got %q\nwant %q", what, got, want)
	}
}

func TestStatus_everySpellingIsAnsweredByThePort(t *testing.T) {
	for _, forward := range []bool{false, true} {
		l := &statusLogin{forward: forward}
		c := l.dial(t)
		for _, stmt := range []string{
			"SHOW dbtrail.router_status",
			"show dbtrail.router_status;",
			"  SHOW   DBTRAIL . Router_Status  ;;  ",
			"\nSHOW\n\tdbtrail.router_status\n;\n",
			"/* who answered */ SHOW dbtrail.router_status -- and why",
			"-- first\nSHOW /* in /* the */ middle */ dbtrail.router_status;",
			`SHOW "dbtrail"."router_status"`,
			`SHOW "dbtrail.router_status"`,
			`SHOW "DBTrail".router_status`,
			// An empty statement in front, as PostgreSQL takes it.
			";SHOW dbtrail.router_status",
			" ; ;\nSHOW \"dbtrail.router_status\";",
			// A comment PostgreSQL ends at a carriage return.
			"SHOW --x\rdbtrail.router_status",
			"SHOW\vdbtrail.router_status",
			// Comments that look like time travel.
			"SHOW dbtrail.router_status -- from _flashback.orders",
			"SHOW dbtrail.router_status /* as of '2026-01-01' */",
		} {
			rows, tx := statusAnswer(t, c, stmt)
			wantRows(t, fmt.Sprintf("forward=%v %q", forward, stmt), rows, "last_answer=none")
			if tx != "I" {
				t.Errorf("%q: transaction status %s", stmt, tx)
			}
		}
		if ran, notes := l.seen(); ran != 0 || notes != 0 {
			t.Errorf("forward=%v: the copy ran %d statements and the host was told of %d answers, want none of either", forward, ran, notes)
		}
	}
}

func TestStatus_saysTheLastAnswer(t *testing.T) {
	const stmt = "SHOW dbtrail.router_status"
	t.Run("the copy answers alone", func(t *testing.T) {
		l := &statusLogin{}
		c := l.dial(t)
		rows, _ := statusAnswer(t, c, stmt)
		wantRows(t, "before any statement", rows, "last_answer=none")

		c.query("SELECT n FROM t")
		answered := []string{"last_answer=copy", "last_answer_reason=no_forward_account",
			"last_answer_reason_means=the server has no forwarding account, so the copy answers alone", "last_answer_error=no"}
		rows, _ = statusAnswer(t, c, stmt)
		wantRows(t, "after the copy answered", rows, answered...)
		t.Logf("after the copy answered:\n%s", strings.Join(rows, "\n"))
		// Asking is not an answer: it says the same the second time.
		rows, _ = statusAnswer(t, c, stmt)
		wantRows(t, "asked twice", rows, answered...)
		// Nor is anything else the port answers itself.
		for _, own := range []string{"SET search_path = public", "SHOW TimeZone", "SELECT version()", "SHOW ALL", "RESET ALL"} {
			c.query(own)
		}
		rows, _ = statusAnswer(t, c, stmt)
		wantRows(t, "after the port's own statements", rows, answered...)

		c.query("SELECT * FROM missing")
		rows, _ = statusAnswer(t, c, stmt)
		wantRows(t, "after the copy refused", rows, "last_answer=copy", "last_answer_reason=no_forward_account",
			"last_answer_reason_means=the server has no forwarding account, so the copy answers alone", "last_answer_error=yes")
		if _, notes := l.seen(); notes != 2 {
			t.Errorf("the host was told of %d answers, want the 2 statements the copy was asked", notes)
		}
	})
	t.Run("with a forwarding account", func(t *testing.T) {
		l := &statusLogin{forward: true}
		c := l.dial(t)
		for _, tc := range []struct {
			stmt string
			want []string
		}{
			{"SELECT n FROM t", []string{"last_answer=copy", "last_answer_reason=answers_the_same",
				"last_answer_reason_means=everything in the statement answers on the copy as on PostgreSQL", "last_answer_error=no"}},
			// The source is down here, so what goes there fails: the status
			// still says where it went, and that the client got an error.
			{"SELECT now()", []string{"last_answer=source", "last_answer_reason=veto",
				"last_answer_reason_means=something in the statement is not known to answer on the copy as on PostgreSQL", "last_answer_error=yes"}},
			{"SELECT * FROM missing", []string{"last_answer=source", "last_answer_reason=table_not_on_copy",
				"last_answer_reason_means=the statement reads a table the copy does not hold", "last_answer_error=yes"}},
			{"INSERT INTO t VALUES (1)", []string{"last_answer=refused", "last_answer_reason=read_only",
				"last_answer_reason_means=not a read, and the port sends nothing that writes to the source", "last_answer_error=yes"}},
		} {
			c.query(tc.stmt)
			rows, tx := statusAnswer(t, c, stmt)
			wantRows(t, "after "+tc.stmt, rows, tc.want...)
			t.Logf("after %s:\n%s", tc.stmt, strings.Join(rows, "\n"))
			if tx != "I" {
				t.Errorf("after %s: transaction status %s", tc.stmt, tx)
			}
		}
		if _, notes := l.seen(); notes != 4 {
			t.Errorf("the host was told of %d answers, want 4: the status statement is not one", notes)
		}
	})
}

// In a failed block PostgreSQL answers nothing but the block's end. The
// status is answered there all the same: it is how a client reads which
// side failed the block, and the block stays failed.
func TestStatus_insideABlockAndAFailedOne(t *testing.T) {
	const stmt = "SHOW dbtrail.router_status"
	l := &statusLogin{}
	c := l.dial(t)
	c.query("BEGIN")
	rows, tx := statusAnswer(t, c, stmt)
	wantRows(t, "in a block, before any statement", rows, "last_answer=none")
	if tx != "T" {
		t.Fatalf("in a block: transaction status %s", tx)
	}
	if got := c.query("SELECT * FROM missing"); got[len(got)-1] != "ReadyForQuery E" {
		t.Fatalf("a failed statement in a block: %q", got)
	}
	failed := []string{"last_answer=copy", "last_answer_reason=no_forward_account",
		"last_answer_reason_means=the server has no forwarding account, so the copy answers alone", "last_answer_error=yes"}
	rows, tx = statusAnswer(t, c, stmt+";")
	wantRows(t, "in a failed block", rows, failed...)
	for _, spelled := range []string{";" + stmt, "SHOW --x\rdbtrail.router_status", stmt + " -- from _flashback.orders"} {
		rows, tx2 := statusAnswer(t, c, spelled)
		wantRows(t, "in a failed block: "+spelled, rows, failed...)
		if tx2 != "E" {
			t.Errorf("%q in a failed block: transaction status %s, want E", spelled, tx2)
		}
	}
	if tx != "E" {
		t.Errorf("after the status in a failed block: transaction status %s, want E", tx)
	}
	// Everything else is still refused for the block.
	for _, other := range []string{"SELECT n FROM t", "SHOW TimeZone", "SHOW ROUTER STATUS", "SELECT 1; " + stmt} {
		if got := c.query(other); !strings.HasPrefix(got[0], "Error 25P02") || got[len(got)-1] != "ReadyForQuery E" {
			t.Errorf("%s in a failed block: %q", other, got)
		}
	}
	// The extended protocol, in the failed block.
	r := &rawConn{t: t, c: c.c, fe: c.fe}
	want(t, "extended, in a failed block", r.send(&pgproto3.Parse{Query: stmt}, &pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'P'},
		&pgproto3.Execute{}, &pgproto3.Sync{}),
		"ParseComplete", "BindComplete", "RowDescription name/0,value/0", "DataRow last_answer|copy", "DataRow last_answer_reason|no_forward_account",
		"DataRow last_answer_reason_means|the server has no forwarding account, so the copy answers alone", "DataRow last_answer_error|yes",
		"CommandComplete SHOW", "ReadyForQuery E")
	if got := c.query("ROLLBACK"); got[len(got)-1] != "ReadyForQuery I" {
		t.Fatalf("ROLLBACK: %q", got)
	}
	rows, _ = statusAnswer(t, c, stmt)
	wantRows(t, "after the block", rows, failed...)
}

func TestStatus_extendedProtocol(t *testing.T) {
	const stmt = "SHOW dbtrail.router_status"
	t.Run("message by message", func(t *testing.T) {
		l := &statusLogin{forward: true}
		r := dialRaw(t, l.login)
		// Described as a statement and as a portal before it runs, as JDBC
		// and pgx do, then run.
		want(t, "a statement described, bound and run", r.send(
			&pgproto3.Parse{Name: "st", Query: stmt}, &pgproto3.Describe{ObjectType: 'S', Name: "st"},
			&pgproto3.Bind{PreparedStatement: "st"}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{}),
			"ParseComplete", "ParameterDescription []", "RowDescription name/0,value/0", "BindComplete", "RowDescription name/0,value/0",
			"DataRow last_answer|none", "CommandComplete SHOW", "ReadyForQuery I")
		// The prepared statement answers what is true when it runs.
		r.send(&pgproto3.Query{String: "SELECT n FROM t"})
		want(t, "the prepared statement, after an answer", r.send(&pgproto3.Bind{PreparedStatement: "st"}, &pgproto3.Execute{}, &pgproto3.Sync{}),
			"BindComplete", "DataRow last_answer|copy", "DataRow last_answer_reason|answers_the_same",
			"DataRow last_answer_reason_means|everything in the statement answers on the copy as on PostgreSQL", "DataRow last_answer_error|no",
			"CommandComplete SHOW", "ReadyForQuery I")
		// Asked in binary, the two text columns are the same bytes.
		want(t, "binary results", r.send(&pgproto3.Bind{PreparedStatement: "st", ResultFormatCodes: []int16{1}}, &pgproto3.Describe{ObjectType: 'P'},
			&pgproto3.Execute{MaxRows: 1}, &pgproto3.Sync{}),
			"BindComplete", "RowDescription name/1,value/1", "DataRow last_answer|copy", "PortalSuspended", "ReadyForQuery I")
		if ran, notes := l.seen(); ran != 1 || notes != 1 {
			t.Errorf("the copy ran %d statements and the host was told of %d answers, want 1 and 1", ran, notes)
		}
	})
	for name, mode := range extendedModes {
		t.Run("pgx "+name, func(t *testing.T) {
			l := &statusLogin{forward: true}
			addr := serveAddrCfg(t, Config{Login: l.login})
			cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://app:%s@%s/orders?sslmode=disable", portPassword, addr))
			if err != nil {
				t.Fatal(err)
			}
			cfg.DefaultQueryExecMode = mode
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(context.Background())
			read := func() string {
				t.Helper()
				rows, err := conn.Query(ctx, stmt)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				var out []string
				for rows.Next() {
					var name, value string
					if err := rows.Scan(&name, &value); err != nil {
						t.Fatal(err)
					}
					out = append(out, name+"="+value)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				return strings.Join(out, " ")
			}
			if got := read(); got != "last_answer=none" {
				t.Errorf("before any statement: %q", got)
			}
			var n int32
			if err := conn.QueryRow(ctx, "SELECT n FROM t").Scan(&n); err != nil || n != 7 {
				t.Fatalf("the copy's answer: %d %v", n, err)
			}
			// Twice: a cached statement answers what is true each time.
			for range 2 {
				if got := read(); !strings.HasPrefix(got, "last_answer=copy last_answer_reason=answers_the_same ") || !strings.HasSuffix(got, " last_answer_error=no") {
					t.Errorf("after the copy answered: %q", got)
				}
			}
		})
	}
}

// What would otherwise carry the statement's name to the copy and then to
// the source is refused here, with the statement to send. The source of
// these connections is down: 08001 would say a statement was sent there.
func TestStatus_neverSentOn(t *testing.T) {
	l := &statusLogin{forward: true}
	c := l.dial(t)
	for _, tc := range []struct{ stmt, want string }{
		{"SHOW ROUTER STATUS", "Error 42601 SHOW ROUTER STATUS is the MySQL port's statement; on this port it is SHOW dbtrail.router_status"},
		{"  show router   status ; ", "Error 42601 SHOW ROUTER STATUS is the MySQL port's statement; on this port it is SHOW dbtrail.router_status"},
		{"/* x */ SHOW ROUTER STATUS -- y", "Error 42601 SHOW ROUTER STATUS is the MySQL port's statement; on this port it is SHOW dbtrail.router_status"},
		{"SELECT 1; SHOW dbtrail.router_status", "Error 0A000 SHOW dbtrail.router_status is answered by the port itself and is sent alone; this message holds more than that statement, and nothing in it was run"},
		{"SHOW dbtrail.router_status; SELECT 1", "Error 0A000 SHOW dbtrail.router_status is answered by the port itself and is sent alone; this message holds more than that statement, and nothing in it was run"},
		{"SHOW dbtrail.router_status; SHOW dbtrail.router_status", "Error 0A000 SHOW dbtrail.router_status is answered by the port itself and is sent alone; this message holds more than that statement, and nothing in it was run"},
		{"BEGIN; SHOW \"dbtrail\".router_status; COMMIT", "Error 0A000 SHOW dbtrail.router_status is answered by the port itself and is sent alone; this message holds more than that statement, and nothing in it was run"},
		{"SHOW ROUTER STATUS; SELECT 1", "Error 0A000 SHOW dbtrail.router_status is answered by the port itself and is sent alone; this message holds more than that statement, and nothing in it was run"},
		{"SHOW dbtrail.router_status; SELECT $$x$$", "Error 0A000 SHOW dbtrail.router_status is answered by the port itself and is sent alone; this message holds more than that statement, and nothing in it was run"},
		{`SHOW U&"dbtrail.router_status"`, `Error 42601 SHOW of a name written with a prefix (U&"...") is not read on this port; write the name as it is, as in SHOW dbtrail.router_status`},
		{`SHOW U&"dbtrail.\0072outer_status"`, `Error 42601 SHOW of a name written with a prefix (U&"...") is not read on this port; write the name as it is, as in SHOW dbtrail.router_status`},
		{`SHOW /* x */ u&"dbtrail.\0072outer_status";`, `Error 42601 SHOW of a name written with a prefix (U&"...") is not read on this port; write the name as it is, as in SHOW dbtrail.router_status`},
		{"SELECT 1; ;SHOW dbtrail.router_status", "Error 0A000 SHOW dbtrail.router_status is answered by the port itself and is sent alone; this message holds more than that statement, and nothing in it was run"},
		{"SELECT current_setting('dbtrail.router_status')", "Error 42704 dbtrail.router_status is not a setting: it cannot be set, reset or read with current_setting; send SHOW dbtrail.router_status"},
		{"SET dbtrail.router_status = 'on'", "Error 42704 dbtrail.router_status is not a setting: it cannot be set, reset or read with current_setting; send SHOW dbtrail.router_status"},
		{"RESET dbtrail.router_status", "Error 42704 dbtrail.router_status is not a setting: it cannot be set, reset or read with current_setting; send SHOW dbtrail.router_status"},
	} {
		got := c.query(tc.stmt)
		if len(got) != 2 || got[0] != tc.want || got[1] != "ReadyForQuery I" {
			t.Errorf("%s:\n got %q\nwant %q", tc.stmt, got, tc.want)
		}
	}
	if ran, notes := l.seen(); ran != 0 || notes != 0 {
		t.Errorf("the copy ran %d statements and the host was told of %d answers, want none of either", ran, notes)
	}
	rows, _ := statusAnswer(t, c, "SHOW dbtrail.router_status")
	wantRows(t, "after the refusals", rows, "last_answer=none")

	// A mention that is not the statement is a statement like any other.
	l = &statusLogin{}
	c = l.dial(t)
	for _, stmt := range []string{
		"SELECT n FROM t WHERE 'SHOW ROUTER STATUS' = 'x'",
		"SELECT n FROM t -- SHOW dbtrail.router_status",
		"SELECT n FROM t WHERE 'a; SHOW dbtrail.router_status' = 'x'",
		`SELECT n AS "show router status" FROM t`,
	} {
		if got := c.query(stmt); len(got) != 4 || got[1] != "DataRow 7" {
			t.Errorf("%s: %q, want the copy's row", stmt, got)
		}
	}
	if ran, notes := l.seen(); ran != 4 || notes != 4 {
		t.Errorf("the copy ran %d statements and the host was told of %d answers, want 4 and 4", ran, notes)
	}
}

func TestStatus_theHostsRows(t *testing.T) {
	const stmt = "SHOW dbtrail.router_status"
	var mu sync.Mutex
	var lasts []LastAnswer
	var bounded bool
	l := &statusLogin{status: func(ctx context.Context, last LastAnswer) [][2]string {
		mu.Lock()
		defer mu.Unlock()
		lasts = append(lasts, last)
		_, bounded = ctx.Deadline()
		return append([][2]string{{"server", "orders"}}, append(last.Rows(), [2]string{"statements_copy", "1"})...)
	}}
	c := l.dial(t)
	rows, _ := statusAnswer(t, c, stmt)
	wantRows(t, "the host's rows, in its order", rows, "server=orders", "last_answer=none", "statements_copy=1")
	c.query("SELECT n FROM t")
	rows, _ = statusAnswer(t, c, stmt)
	wantRows(t, "after an answer", rows, "server=orders", "last_answer=copy", "last_answer_reason=no_forward_account",
		"last_answer_reason_means=the server has no forwarding account, so the copy answers alone", "last_answer_error=no", "statements_copy=1")
	mu.Lock()
	defer mu.Unlock()
	if len(lasts) != 2 || lasts[0] != (LastAnswer{}) || lasts[1] != (LastAnswer{Side: RouteCopy, Reason: ReasonNoForwardAccount}) {
		t.Errorf("the host was handed %+v", lasts)
	}
	if !bounded {
		t.Error("the host's context has no deadline")
	}

	// A host with nothing to say still gets the client a row.
	empty := &statusLogin{status: func(context.Context, LastAnswer) [][2]string { return nil }}
	rows, _ = statusAnswer(t, empty.dial(t), stmt)
	wantRows(t, "a host with nothing to say", rows, "status=nothing to report")
}

func TestStatus_theMonitorLoginAndTheTimeTravelPort(t *testing.T) {
	const stmt = "SHOW dbtrail.router_status"
	// A proxy's monitor login opens no server: it has no answer to tell of.
	m := &statusLogin{monitor: true}
	rows, _ := statusAnswer(t, m.dial(t), stmt)
	wantRows(t, "the monitor login", rows, "last_answer=none")

	// The time-travel port routes nothing and is as it was: the name is not
	// a setting there.
	conn := dialPGWire(t, testAuth(t), testUser, testPass)
	_, err := conn.Exec(context.Background(), stmt)
	if pe := requirePgError(t, err); pe.Code != "42704" {
		t.Errorf("the time-travel port: %s %s, want 42704", pe.Code, pe.Message)
	}
}

func TestLastAnswer_everyReasonHasItsSentence(t *testing.T) {
	for _, reason := range []string{
		ReasonAnswersTheSame, ReasonNoForwardAccount, ReasonHiddenRules, ReasonCopyCommand, ReasonInTransaction, ReasonVeto,
		ReasonSessionDiffers, ReasonNotASelect, ReasonCopyRefused, ReasonTableNotOnCopy, ReasonCopyBehindSource, ReasonCopyAgeUnknown,
		ReasonCopyQueueFull, ReasonCopyWaitTimeout, ReasonCopyColumnsDiffer, ReasonCatalogNotOnCopy, ReasonReadOnly,
		ReasonTableNotCaptured, ReasonJoinTooBig,
	} {
		rows := LastAnswer{Side: RouteSource, Reason: reason}.Rows()
		if len(rows) != 4 || rows[2][0] != "last_answer_reason_means" || rows[2][1] == "" {
			t.Errorf("%s: %q", reason, rows)
		}
	}
	// A reason a later version adds is shown by its name alone.
	rows := LastAnswer{Side: RouteCopy, Reason: "a_new_reason", Failed: true}.Rows()
	if fmt.Sprint(rows) != "[[last_answer copy] [last_answer_reason a_new_reason] [last_answer_error yes]]" {
		t.Errorf("a reason without a sentence: %q", rows)
	}
}

func TestFindStatus(t *testing.T) {
	for stmt, want := range map[string]statusUse{
		"":                                   statusNotThere,
		"SELECT 1":                           statusNotThere,
		"SHOW ROUTER STATUS":                 statusSpelledForMySQL,
		"show router status;;":               statusSpelledForMySQL,
		"SHOW ROUTER STATUS LIKE 'x'":        statusNotThere,
		"SHOW ROUTER":                        statusNotThere,
		`SHOW "ROUTER" STATUS`:               statusNotThere,
		"SELECT 'SHOW ROUTER STATUS'":        statusNotThere,
		"SELECT 1; SHOW ROUTER STATUS":       statusAmongOthers,
		"SHOW dbtrail.router_status; x":      statusAmongOthers,
		"x; SHOW DBTRAIL.ROUTER_STATUS;":     statusAmongOthers,
		`x; SHOW "dbtrail.router_status"`:    statusAmongOthers,
		"SHOW dbtrail.router_status $$":      statusAmongOthers,
		"SHOW dbtrail.router_status":         statusAlone,
		";SHOW dbtrail.router_status;;":      statusAlone,
		"SHOW --x\rdbtrail.router_status":    statusAlone,
		`SHOW U&"dbtrail.router_status"`:     statusNameUnread,
		`SELECT 1; SHOW u&"x"`:               statusNameUnread,
		`SELECT U&"router" FROM t`:           statusNotThere,
		`SHOW TABLES; SELECT U&"router"`:     statusNotThere,
		"SHOW dbtrail.router_status_x; x":    statusNotThere,
		"SHOW other.router_status; SELECT 1": statusNotThere,
		"SELECT 1; -- SHOW ROUTER STATUS":    statusNotThere,
	} {
		if got := findStatus(stmt); got != want {
			t.Errorf("findStatus(%q) = %d, want %d", stmt, got, want)
		}
	}
	for stmt, want := range map[string]bool{"": false, "route": false, "ROUTER": true, "xRoUtEr_": true, "select 1": false, "rrouter": true} {
		if got := mentionsRouter(stmt); got != want {
			t.Errorf("mentionsRouter(%q) = %v", stmt, got)
		}
	}
}
