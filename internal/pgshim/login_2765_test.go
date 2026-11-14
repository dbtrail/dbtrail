package pgshim

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The port mode (#2765): a host's own login, and ordinary SQL answered by
// the executor the login bound. A real pgx client over the wire; the
// executor is the test's.

const portPassword = "port-secret"

// portLogin accepts portPassword for any user and binds the connection to
// run, recording what it was called with.
type portLogin struct {
	mu       sync.Mutex
	users    []string
	dbs      []string
	closed   int
	answered []bool
	run      func(ctx context.Context, statement string) (sqlsandbox.Result, error)
}

func (p *portLogin) login(_ context.Context, user, database, password string, _ func()) (*Binding, *Error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.users, p.dbs = append(p.users, user), append(p.dbs, database)
	if password != portPassword {
		return nil, &Error{Code: "28P01", Message: fmt.Sprintf("password authentication failed for user %q", user)}
	}
	if database == "nowhere" {
		return nil, &Error{Code: "3D000", Message: `database "nowhere" does not exist`}
	}
	return &Binding{
		Run:    p.run,
		Actor:  "server:" + database,
		Schema: "public",
		Answered: func(_ time.Duration, refused bool) {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.answered = append(p.answered, refused)
		},
		Close: func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.closed++
		},
	}, nil
}

func dialPort(t *testing.T, p *portLogin, user, pass, database string) (*pgx.Conn, error) {
	t.Helper()
	addr := serveAddrCfg(t, Config{Login: p.login})
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s/%s", user, pass, addr, database))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		cc, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Close(cc)
	})
	return conn, nil
}

func answer(cols []sqlsandbox.Column, rows ...[]any) func(context.Context, string) (sqlsandbox.Result, error) {
	return func(context.Context, string) (sqlsandbox.Result, error) {
		return sqlsandbox.Result{Columns: cols, Rows: rows}, nil
	}
}

// textRows reads a result as text, nil for NULL.
func textRows(t *testing.T, conn *pgx.Conn, stmt string) (names []string, out [][]*string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Read by hand: the client library's ReadAll keeps the column names
	// only when the result has a row.
	mrr := conn.PgConn().Exec(ctx, stmt)
	results := 0
	for mrr.NextResult() {
		results++
		rr := mrr.ResultReader()
		for _, f := range rr.FieldDescriptions() {
			names = append(names, f.Name)
		}
		for rr.NextRow() {
			vals := rr.Values()
			row := make([]*string, len(vals))
			for i, c := range vals {
				if c != nil {
					s := string(c)
					row[i] = &s
				}
			}
			out = append(out, row)
		}
		if _, err := rr.Close(); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := mrr.Close(); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	if results != 1 {
		t.Fatalf("%s: %d results, want 1", stmt, results)
	}
	return names, out
}

func TestPort_answersAStatementFromTheBoundExecutor(t *testing.T) {
	var got []string
	p := &portLogin{}
	p.run = func(_ context.Context, statement string) (sqlsandbox.Result, error) {
		got = append(got, statement)
		return sqlsandbox.Result{
			Columns: []sqlsandbox.Column{{Name: "id", Type: "BIGINT"}, {Name: "ok", Type: "BOOLEAN"}, {Name: "at", Type: "TIMESTAMP"}, {Name: "note", Type: "VARCHAR"}, {Name: "tags", Type: "VARCHAR[]"}},
			Rows: [][]any{
				{json.Number("7"), true, "2026-03-04T05:06:07Z", "", []any{"a", "b c"}},
				{json.Number("8"), false, nil, nil, nil},
			},
		}, nil
	}
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	names, rows := textRows(t, conn, "SELECT id, ok, at, note, tags FROM t")
	if strings.Join(names, ",") != "id,ok,at,note,tags" {
		t.Fatalf("columns = %v", names)
	}
	if len(got) != 1 || got[0] != "SELECT id, ok, at, note, tags FROM t" {
		t.Fatalf("the executor was given %q", got)
	}
	want := [][]string{
		{"7", "t", "2026-03-04 05:06:07", "", `{a,"b c"}`},
		{"8", "f", "NULL", "NULL", "NULL"},
	}
	for i, r := range rows {
		for j, c := range r {
			cell := "NULL"
			if c != nil {
				cell = *c
			}
			if cell != want[i][j] {
				t.Errorf("row %d column %s = %q, want %q", i, names[j], cell, want[i][j])
			}
		}
	}
	// An empty string is a value, not NULL: the one cell a text rendering
	// loses first.
	if rows[0][3] == nil {
		t.Error("an empty string was sent as NULL")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.users) != 1 || p.users[0] != "app" || p.dbs[0] != "orders" {
		t.Errorf("the login was given user %v database %v", p.users, p.dbs)
	}
	if len(p.answered) != 1 || p.answered[0] {
		t.Errorf("answered = %v, want one statement that was not refused", p.answered)
	}
}

func TestPort_aResultWithNoRowsKeepsItsColumns(t *testing.T) {
	p := &portLogin{run: answer([]sqlsandbox.Column{{Name: "id", Type: "BIGINT"}})}
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatal(err)
	}
	names, rows := textRows(t, conn, "SELECT id FROM t WHERE false")
	if len(names) != 1 || names[0] != "id" || len(rows) != 0 {
		t.Fatalf("names %v rows %v, want the column and no row", names, rows)
	}
}

func TestPort_loginRefusalsReachTheClient(t *testing.T) {
	for _, tc := range []struct {
		name, pass, db, code string
	}{
		{"wrong password", "nope", "orders", "28P01"},
		{"unknown server", portPassword, "nowhere", "3D000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &portLogin{run: answer(nil)}
			_, err := dialPort(t, p, "app", tc.pass, tc.db)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				t.Fatalf("connect error = %v, want a PostgreSQL error", err)
			}
			if pgErr.Code != tc.code || pgErr.Severity != "FATAL" {
				t.Fatalf("got %s %s %q, want FATAL %s", pgErr.Severity, pgErr.Code, pgErr.Message, tc.code)
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.closed != 0 {
				t.Errorf("Close ran %d times for a connection that was never bound", p.closed)
			}
		})
	}
}

// A client that names no database gets its username as one, as on
// PostgreSQL: the login is never handed an empty database to resolve.
func TestPort_noDatabaseIsTheUser(t *testing.T) {
	p := &portLogin{run: answer(nil)}
	addr := serveAddrCfg(t, Config{Login: p.login})
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s user=orders password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Database = ""
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.dbs) != 1 || p.dbs[0] != "orders" {
		t.Fatalf("the login was given database %q, want the user's name", p.dbs)
	}
}

func hostOf(addr string) string { return addr[:strings.LastIndex(addr, ":")] }
func portOf(addr string) string { return addr[strings.LastIndex(addr, ":")+1:] }

func TestPort_bindingIsClosedOnceWhenTheClientLeaves(t *testing.T) {
	p := &portLogin{run: answer([]sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, []any{json.Number("1")})}
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatal(err)
	}
	textRows(t, conn, "SELECT 1")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Close(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		n := p.closed
		p.mu.Unlock()
		if n == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Close ran %d times, want 1", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPort_executorErrorsBecomeClientErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code string
		want string // in the message
		not  string // never in the message
	}{
		{"the port's own", &Error{Code: "55000", Message: "not available here"}, "55000", "not available here", ""},
		{"not a read", &sqlsandbox.RefusedError{Reason: "only a read can run here"}, "0A000", "only a read", ""},
		{"syntax, said by the sandbox", &sqlsandbox.RefusedError{Reason: "syntax error: syntax error at or near \"SELEC\""}, "42601", "SELEC", ""},
		{"syntax", &sqlsandbox.QueryError{Message: "Parser Error: syntax error at or near \"FORM\""}, "42601", "FORM", ""},
		{"no such table", &sqlsandbox.QueryError{Message: "Catalog Error: Table with name nope does not exist!"}, "42P01", "nope", ""},
		{"no such function", &sqlsandbox.QueryError{Message: "Catalog Error: Scalar Function with name nope does not exist!"}, "42704", "nope", ""},
		{"no such column", &sqlsandbox.QueryError{Message: "Binder Error: Referenced column \"x\" not found"}, "42000", "not found", ""},
		{"bad value", &sqlsandbox.QueryError{Message: "Conversion Error: Could not convert string 'a' to INT32"}, "22000", "convert", ""},
		{"engine fault", &sqlsandbox.QueryError{Message: "IO Error: cannot open file"}, "XX000", "IO Error", ""},
		{"behind the source", &sqlsandbox.BehindSourceError{Reason: "the copy is 90 s behind"}, "55000", "90 s behind", ""},
		{"copy not usable", &sqlsandbox.UnavailableError{Reason: "the copy is only on S3"}, "55000", "only on S3", ""},
		{"too slow", &sqlsandbox.TimeoutError{Limit: 30 * time.Second}, "57014", "30 s", ""},
		{"busy", sqlsandbox.ErrBusy, "53000", "busy", ""},
		{"too large", sqlsandbox.ErrResultTooLarge, "54000", "MB", ""},
		{"connection cap", context.DeadlineExceeded, "57014", "cap", ""},
		{"cancelled", context.Canceled, "57014", "cancelled", ""},
		{"worker failed", &sqlsandbox.WorkerError{Err: errors.New("open /var/lib/secret/path: denied")}, "XX000", "worker failed", "/var/lib"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := freeError(tc.err, discardLogger())
			if got.code != tc.code {
				t.Errorf("code = %s, want %s (%s)", got.code, tc.code, got.msg)
			}
			if !strings.Contains(got.msg, tc.want) {
				t.Errorf("message %q does not hold %q", got.msg, tc.want)
			}
			if tc.not != "" && strings.Contains(got.msg, tc.not) {
				t.Errorf("message %q holds %q, which the client must not be shown", got.msg, tc.not)
			}
		})
	}
}

// After an error the connection answers the next statement: the client is
// told it may go on (ReadyForQuery) on every way out.
func TestPort_anErrorLeavesTheConnectionUsable(t *testing.T) {
	calls := 0
	p := &portLogin{}
	p.run = func(context.Context, string) (sqlsandbox.Result, error) {
		calls++
		if calls == 1 {
			return sqlsandbox.Result{}, &sqlsandbox.RefusedError{Reason: "only a read can run here"}
		}
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{json.Number("1")}}}, nil
	}
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = conn.PgConn().Exec(ctx, "DELETE FROM t").ReadAll()
	if pe := requirePgError(t, err); pe.Code != "0A000" {
		t.Fatalf("code = %s, want 0A000", pe.Code)
	}
	_, rows := textRows(t, conn, "SELECT 1")
	if len(rows) != 1 || *rows[0][0] != "1" {
		t.Fatalf("the statement after the error answered %v", rows)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.answered) != 2 || !p.answered[0] || p.answered[1] {
		t.Errorf("answered = %v, want the first refused and the second not", p.answered)
	}
}

func TestPort_aCutResultIsRefusedNotReturnedShort(t *testing.T) {
	p := &portLogin{}
	p.run = func(context.Context, string) (sqlsandbox.Result, error) {
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{json.Number("1")}, {json.Number("2")}}, Truncated: true}, nil
	}
	rec := audittest.Install(t)
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = conn.PgConn().Exec(ctx, "SELECT n FROM big").ReadAll()
	pe := requirePgError(t, err)
	if pe.Code != "54000" || !strings.Contains(pe.Message, "more than 2 rows") {
		t.Fatalf("got %s %q, want 54000 naming the cap", pe.Code, pe.Message)
	}
	if evs := rec.Events(); len(evs) != 0 {
		t.Errorf("a refused result left %d audit records", len(evs))
	}
}

func TestPort_aRowOfTheWrongWidthIsAnErrorNotAShiftedRow(t *testing.T) {
	p := &portLogin{run: answer([]sqlsandbox.Column{{Name: "a", Type: "INTEGER"}, {Name: "b", Type: "INTEGER"}}, []any{json.Number("1")})}
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = conn.PgConn().Exec(ctx, "SELECT a, b FROM t").ReadAll()
	if pe := requirePgError(t, err); pe.Code != "XX000" {
		t.Fatalf("code = %s, want XX000", pe.Code)
	}
}

// Blanks and semicolons alone are an empty query: no error, and the executor
// is not asked.
func TestPort_anEmptyQueryIsNotSentToTheCopy(t *testing.T) {
	ran := 0
	p := &portLogin{}
	p.run = func(context.Context, string) (sqlsandbox.Result, error) {
		ran++
		return sqlsandbox.Result{}, nil
	}
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, stmt := range []string{"", " ", ";", " ;\n; "} {
		if _, err := conn.PgConn().Exec(ctx, stmt).ReadAll(); err != nil {
			t.Errorf("%q: %v", stmt, err)
		}
	}
	if ran != 0 {
		t.Fatalf("the executor was asked %d times for empty queries", ran)
	}
}

// Time travel has no engine on this port: a statement written for it is
// refused by name, never run as ordinary SQL on the copy as it is now.
func TestPort_timeTravelIsRefusedNotRunOnTheCopy(t *testing.T) {
	ran := false
	p := &portLogin{}
	p.run = func(context.Context, string) (sqlsandbox.Result, error) {
		ran = true
		return sqlsandbox.Result{}, nil
	}
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, stmt := range []string{
		"SELECT * FROM _flashback.orders AS OF '5 minutes ago' WHERE id = 1",
		"SELECT * FROM public.orders WHERE id = 1 AS OF '5 minutes ago'", // the bare form: AS OF closes the statement
		"SELECT * FROM _snapshot.orders AS OF 'not a time' WHERE id = 1", // malformed: still not this port's
	} {
		_, err = conn.PgConn().Exec(ctx, stmt).ReadAll()
		pe := requirePgError(t, err)
		if pe.Code != "0A000" || !strings.Contains(pe.Message, "time travel") {
			t.Errorf("%s: got %s %q", stmt, pe.Code, pe.Message)
		}
	}
	if ran {
		t.Fatal("a time-travel statement reached the copy's executor")
	}
}

func TestPort_notesReachTheClientAsNotices(t *testing.T) {
	p := &portLogin{}
	p.run = func(context.Context, string) (sqlsandbox.Result, error) {
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{json.Number("1")}},
			Note: "answered from an earlier copy"}, nil
	}
	addr := serveAddrCfg(t, Config{Login: p.login})
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://app:%s@%s/orders", portPassword, addr))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	var mu sync.Mutex
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		mu.Lock()
		defer mu.Unlock()
		notices = append(notices, n.Message)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, rows := textRows(t, conn, "SELECT 1"); len(rows) != 1 {
		t.Fatalf("rows = %v", rows)
	}
	mu.Lock()
	defer mu.Unlock()
	all := strings.Join(notices, " | ")
	if !strings.Contains(all, "earlier copy") {
		t.Fatalf("notices = %q, want the note", all)
	}
}

// A cell the copy cut is not sent on: written again as PostgreSQL writes its
// kind, a cut value would lose the marker that says it was cut.
func TestPort_aCutCellIsRefused(t *testing.T) {
	p := &portLogin{}
	p.run = func(context.Context, string) (sqlsandbox.Result, error) {
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "b", Type: "BLOB"}}, Rows: [][]any{{"0x4142 [cell cut at 1 MiB]"}}, TruncatedCells: 1}, nil
	}
	rec := audittest.Install(t)
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = conn.PgConn().Exec(ctx, "SELECT b FROM t").ReadAll()
	pe := requirePgError(t, err)
	if pe.Code != "54000" || !strings.Contains(pe.Message, "1 cell(s)") {
		t.Fatalf("got %s %q, want 54000 naming the cut cell", pe.Code, pe.Message)
	}
	if evs := rec.Events(); len(evs) != 0 {
		t.Errorf("a refused result left %d audit records", len(evs))
	}
}

func TestBoundProbe(t *testing.T) {
	for _, tc := range []struct {
		stmt    string
		tag     string // "" when not answered here or refused
		refused string // in the refusal's message
		ok      bool
	}{
		{"SET client_encoding TO 'UTF8'", "SET", "", true},
		{"set application_name = 'psql';", "SET", "", true},
		{"SET extra_float_digits = 3", "SET", "", true},
		{"BEGIN", "BEGIN", "", true},
		{"begin read only;", "BEGIN", "", true},
		{"COMMIT", "COMMIT", "", true},
		{"ROLLBACK ; ", "ROLLBACK", "", true},
		{"SET TimeZone = 'UTC'", "SET", "", true},
		{"SET TIME ZONE 'UTC'", "SET", "", true},
		{"SET SESSION timezone TO utc", "SET", "", true},
		// Not a probe: the rest of the message would be dropped.
		{"SET x = 1; SELECT * FROM orders", "", "", false},
		{"BEGIN; SELECT 1", "", "", false},
		{"begin read only; select 1; commit", "", "", false},
		{"SELECT 1", "", "", false},
		{"settle", "", "", false},
		{"", "", "", false},
		// Would change what the next statement reads: refused, by name.
		{"SET search_path TO sales", "", "SET search_path", true},
		{"set search_path = sales, public;", "", "SET search_path", true},
		{"SET SESSION search_path = 'sales'", "", "SET search_path", true},
		{"SET LOCAL search_path TO sales", "", "SET search_path", true},
		{"SET SCHEMA 'sales'", "", "SET schema", true},
		{"SET ROLE admin", "", "SET role", true},
		{"SET SESSION AUTHORIZATION admin", "", "SET session_authorization", true},
		{"SET TimeZone = 'America/Bogota'", "", "SET TimeZone", true},
		{"SET TIME ZONE 'Europe/Madrid'", "", "SET TimeZone", true},
		{"set timezone to -5", "", "SET TimeZone", true},
	} {
		tag, refusal, ok := boundProbe(tc.stmt)
		if ok != tc.ok || tag != tc.tag {
			t.Errorf("boundProbe(%q) = tag %q ok %v, want tag %q ok %v", tc.stmt, tag, ok, tc.tag, tc.ok)
		}
		switch {
		case tc.refused == "" && refusal != nil:
			t.Errorf("boundProbe(%q) refused: %s", tc.stmt, refusal.msg)
		case tc.refused != "" && (refusal == nil || refusal.code != "0A000" || !strings.Contains(refusal.msg, tc.refused)):
			t.Errorf("boundProbe(%q) refusal = %+v, want 0A000 saying %q", tc.stmt, refusal, tc.refused)
		}
	}
}

// Over the wire: a setup statement and a query in one message reach the
// executor whole (which refuses more than one statement); neither is
// answered "SET" with the query dropped.
func TestPort_aSetAndAQueryInOneMessageAreNotAnsweredSET(t *testing.T) {
	var got []string
	p := &portLogin{}
	p.run = func(_ context.Context, statement string) (sqlsandbox.Result, error) {
		got = append(got, statement)
		return sqlsandbox.Result{}, &sqlsandbox.RefusedError{Reason: "one statement at a time"}
	}
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = conn.PgConn().Exec(ctx, "SET x = 1; SELECT * FROM orders").ReadAll()
	if pe := requirePgError(t, err); pe.Code != "0A000" {
		t.Fatalf("code %s", pe.Code)
	}
	if len(got) != 1 || got[0] != "SET x = 1; SELECT * FROM orders" {
		t.Fatalf("the executor was given %q, want the whole message", got)
	}
	_, err = conn.PgConn().Exec(ctx, "SET search_path TO sales").ReadAll()
	if pe := requirePgError(t, err); pe.Code != "0A000" || !strings.Contains(pe.Message, "search_path") {
		t.Fatalf("SET search_path: %s %q", pe.Code, pe.Message)
	}
	if len(got) != 1 {
		t.Fatalf("SET search_path reached the executor: %q", got)
	}
}

// The audit contract of the port mode (audittest.OwnerPGShimUnit): a served
// statement leaves one shim/sql.run record under the actor the login bound.
// No t.Parallel(): the sink is process-wide.
func TestAuditContract_Port(t *testing.T) {
	rec := audittest.Install(t)
	p := &portLogin{run: answer([]sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, []any{json.Number("1")}, []any{json.Number("2")})}
	conn, err := dialPort(t, p, "app", portPassword, "orders")
	if err != nil {
		t.Fatal(err)
	}
	textRows(t, conn, "SELECT n FROM t")
	audittest.CheckCoverage(t, audittest.OwnerPGShimUnit, rec.Pairs())
	evs := rec.Events()
	if len(evs) != 1 {
		t.Fatalf("%d audit records, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Actor != "server:orders" {
		t.Errorf("actor = %q, want the one the login bound", ev.Actor)
	}
	for k, want := range map[string]string{"sql": "SELECT n FROM t", "rows": "2", "truncated": "false", "schema": "public", "protocol": "postgresql"} {
		if ev.Detail[k] != want {
			t.Errorf("detail[%s] = %q, want %q", k, ev.Detail[k], want)
		}
	}
}

// A client that holds no password cannot have this process set aside
// memory for it: a "password" that announces a gigabyte ends the connection
// at once. Without a ceiling the server would wait for the gigabyte, and
// this read would time out.
func TestPort_aGiantMessageBeforeLoginEndsTheConnection(t *testing.T) {
	for _, mode := range []struct {
		name string
		cfg  Config
	}{
		{"the port", Config{Login: (&portLogin{run: answer(nil)}).login}},
		{"flashback", Config{Auth: testAuth(t)}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			addr := serveAddrCfg(t, mode.cfg)
			c, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(5 * time.Second))
			// StartupMessage: length, protocol 3.0, user\0app\0\0.
			body := append([]byte{0, 3, 0, 0}, []byte("user\x00app\x00\x00")...)
			msg := binary.BigEndian.AppendUint32(nil, uint32(len(body)+4))
			if _, err := c.Write(append(msg, body...)); err != nil {
				t.Fatal(err)
			}
			// The password request: 'R', length 8, code 3.
			req := make([]byte, 9)
			if _, err := io.ReadFull(c, req); err != nil || req[0] != 'R' {
				t.Fatalf("no password request: %v %q", err, req)
			}
			// A password message that announces 1 GiB and sends four bytes.
			giant := append([]byte{'p'}, binary.BigEndian.AppendUint32(nil, 1<<30)...)
			if _, err := c.Write(append(giant, 'a', 'b', 'c', 'd')); err != nil {
				t.Fatal(err)
			}
			_, err = io.ReadAll(c)
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatal("the server waited for the announced gigabyte instead of ending the connection")
			}
		})
	}
}

// A full port says so in an error the client can read, and takes the next
// client once one leaves.
func TestPort_aFullPortRefusesWithAnError(t *testing.T) {
	p := &portLogin{run: answer([]sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, []any{json.Number("1")})}
	addr := serveAddrCfg(t, Config{Login: p.login, MaxConns: 1})
	// sslmode=disable: a client that asks for TLS first is told there is
	// none and connects again, and for a moment its first connection still
	// holds the port's only place.
	connect := func() (*pgx.Conn, error) {
		cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://app:%s@%s/orders?sslmode=disable", portPassword, addr))
		if err != nil {
			t.Fatal(err)
		}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return pgx.ConnectConfig(ctx, cfg)
	}
	first, err := connect()
	if err != nil {
		t.Fatalf("the first client: %v", err)
	}
	_, err = connect()
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "53300" || !strings.Contains(pe.Message, "limit of 1 connections") {
		t.Fatalf("the second client got %v, want 53300 naming the limit", err)
	}
	// The first still answers.
	if _, rows := textRows(t, first, "SELECT 1"); len(rows) != 1 {
		t.Fatalf("the connected client stopped answering: %v", rows)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = first.Close(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		next, err := connect()
		if err == nil {
			_ = next.Close(context.Background())
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the port did not take a client after the first one left: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A socket that connects and never logs in gives its place back: the port's
// connections are for clients, not for whatever opened a socket.
func TestPort_aClientThatNeverLogsInIsPutOut(t *testing.T) {
	old := loginTimeout
	loginTimeout = 300 * time.Millisecond
	t.Cleanup(func() { loginTimeout = old })
	p := &portLogin{run: answer([]sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, []any{json.Number("1")})}
	addr := serveAddrCfg(t, Config{Login: p.login, MaxConns: 1})
	silent, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	// It holds the only place, says nothing, and is closed by the port.
	_ = silent.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadAll(silent); err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("a socket that never logged in was left connected")
		}
	}
	// And a real client gets the place.
	deadline := time.Now().Add(5 * time.Second)
	for {
		cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://app:%s@%s/orders?sslmode=disable", portPassword, addr))
		if err != nil {
			t.Fatal(err)
		}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := pgx.ConnectConfig(ctx, cfg)
		cancel()
		if err == nil {
			// A client that logged in is not put out by the timer.
			time.Sleep(2 * loginTimeout)
			if _, rows := textRows(t, conn, "SELECT 1"); len(rows) != 1 {
				t.Fatalf("a logged-in client was put out by the login timer: %v", rows)
			}
			_ = conn.Close(context.Background())
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the place was never given back: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
