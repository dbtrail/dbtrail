package pgshim

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The extended query protocol (#2767), against an executor that answers each
// statement with the text it was sent, so a test reads exactly what the copy
// would have run.

// echoLogin binds every connection to an executor that answers one column,
// "stmt", holding the statement it ran, rows times (one row by default). A
// statement with "DELETE" in it is refused as the copy refuses a write.
type echoLogin struct {
	mu   sync.Mutex
	ran  []string
	rows int
	tt   func() *Binding // when set, the binding to hand out
	// answered counts what the binding's Answered was told.
	answered int
}

func (e *echoLogin) setRows(n int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rows = n
}

// newDeadEngine is a time-travel engine on an index nobody answers on: a
// statement that reaches it fails there.
func newDeadEngine(t *testing.T) *shim.Handler {
	return shim.NewHandlerWithConfig(deadDB(t), shim.Config{NoArchive: true, IndexDBName: "idx"}, discardLogger())
}

func (e *echoLogin) run(_ context.Context, st string) (sqlsandbox.Result, error) {
	e.mu.Lock()
	e.ran = append(e.ran, st)
	rows := e.rows
	e.mu.Unlock()
	if strings.Contains(st, "DELETE") {
		return sqlsandbox.Result{}, &sqlsandbox.RefusedError{Reason: "only a single SELECT statement can run here"}
	}
	if strings.Contains(st, "dbtrail_describe") && strings.Contains(st, "SHOW") {
		return sqlsandbox.Result{}, &sqlsandbox.RefusedError{Reason: "syntax error at or near \"SHOW\""}
	}
	if strings.Contains(st, "dbtrail_describe") && strings.Contains(st, "BUSY") {
		return sqlsandbox.Result{}, sqlsandbox.ErrBusy
	}
	if strings.Contains(st, "no_such_table") {
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Catalog Error: Table with name no_such_table does not exist!"}
	}
	if rows == 0 {
		rows = 1
	}
	res := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "stmt", Type: "VARCHAR"}}}
	for range rows {
		res.Rows = append(res.Rows, []any{st})
	}
	return res, nil
}

func (e *echoLogin) statements() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.ran...)
}

func (e *echoLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	if e.tt != nil {
		return e.tt(), nil
	}
	return &Binding{Run: e.run, Actor: "server:orders", Schema: "public", Answered: func(time.Duration, bool) {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.answered++
	}}, nil
}

func (e *echoLogin) timesAnswered() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.answered
}

func dialEcho(t *testing.T, e *echoLogin, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	addr := serveAddrCfg(t, Config{Login: e.login})
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://app:%s@%s/orders?sslmode=disable", portPassword, addr))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = mode
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

var extendedModes = map[string]pgx.QueryExecMode{
	"CacheStatement": pgx.QueryExecModeCacheStatement,
	"CacheDescribe":  pgx.QueryExecModeCacheDescribe,
	"DescribeExec":   pgx.QueryExecModeDescribeExec,
	"Exec":           pgx.QueryExecModeExec,
}

// Every mode of pgx that speaks the extended protocol gets its arguments
// written into the statement as literals, a quote in a string doubled, and
// the copy runs that text.
func TestExtended_pgxModesWriteTheArgumentsIntoTheStatement(t *testing.T) {
	for name, mode := range extendedModes {
		t.Run(name, func(t *testing.T) {
			e := &echoLogin{}
			conn := dialEcho(t, e, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var got string
			err := conn.QueryRow(ctx, "SELECT stmt FROM t WHERE id = $1 AND name = $2 AND note = $3",
				42, "x' OR '1'='1", nil).Scan(&got)
			if err != nil {
				t.Fatal(err)
			}
			// The port declares no type for an argument (ParameterDescription
			// 0), and Exec mode declares none either, so pgx sends each one as
			// text: a string literal the copy reads as the type its place
			// needs, as PostgreSQL reads an argument of unknown type.
			want := "SELECT stmt FROM t WHERE id =  '42'  AND name =  'x'' OR ''1''=''1'  AND note =  NULL "
			if got != want {
				t.Fatalf("the copy ran\n %q\nwant\n %q", got, want)
			}
			// A second execution of the same statement reads the new argument.
			if err := conn.QueryRow(ctx, "SELECT stmt FROM t WHERE id = $1 AND name = $2 AND note = $3", 7, "b", "c").Scan(&got); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(got, "'b'") || !strings.Contains(got, "'c'") {
				t.Fatalf("second execution ran %q", got)
			}
		})
	}
}

// The modes that describe a statement before they bind it learn its columns
// from the copy, through the same executor, with NULL for every argument and
// in a subquery that returns no row.
func TestExtended_describeAsksTheCopyWithNulls(t *testing.T) {
	e := &echoLogin{}
	conn := dialEcho(t, e, pgx.QueryExecModeCacheStatement)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sd, err := conn.Prepare(ctx, "s1", "SELECT stmt FROM t WHERE id = $1;")
	if err != nil {
		t.Fatal(err)
	}
	if len(sd.ParamOIDs) != 1 || sd.ParamOIDs[0] != 0 {
		t.Fatalf("parameter types %v, want one undeclared (0)", sd.ParamOIDs)
	}
	if len(sd.Fields) != 1 || sd.Fields[0].Name != "stmt" || sd.Fields[0].DataTypeOID != pgtypeText {
		t.Fatalf("fields %+v, want stmt as text", sd.Fields)
	}
	ran := e.statements()
	want := "SELECT * FROM (\nSELECT stmt FROM t WHERE id =  NULL\n) AS dbtrail_describe LIMIT 0"
	if len(ran) != 1 || ran[0] != want {
		t.Fatalf("the describe ran %q, want %q", ran, want)
	}
}

// What the simple protocol refuses the extended one refuses too, with the
// same code; and the connection goes on.
func TestExtended_refusesWhatTheSimpleProtocolRefuses(t *testing.T) {
	for name, mode := range extendedModes {
		t.Run(name, func(t *testing.T) {
			e := &echoLogin{}
			conn := dialEcho(t, e, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for _, c := range []struct{ stmt, code string }{
				{"DELETE FROM t WHERE id = $1", "0A000"},
				{"SELECT * FROM no_such_table WHERE id = $1", "42P01"},
				{"SELECT * FROM _flashback.t AS OF $1 WHERE id = 1", "0A000"},
				{"SET search_path = $1", "0A000"},
			} {
				rows, err := conn.Query(ctx, c.stmt, "x")
				if err == nil {
					rows.Close()
					err = rows.Err()
				}
				if pe := requirePgError(t, err); pe.Code != c.code {
					t.Errorf("%s: %s %q, want %s", c.stmt, pe.Code, pe.Message, c.code)
				}
				simple := strings.Replace(c.stmt, "$1", "'x'", 1)
				_, err = conn.PgConn().Exec(ctx, simple).ReadAll()
				if pe := requirePgError(t, err); pe.Code != c.code {
					t.Errorf("simple %s: %s, want %s", simple, pe.Code, c.code)
				}
			}
			var got string
			if err := conn.QueryRow(ctx, "SELECT stmt FROM t WHERE id = $1", 1).Scan(&got); err != nil {
				t.Fatalf("the connection after the refusals: %v", err)
			}
		})
	}
}

// BEGIN, COMMIT and ROLLBACK move the status ReadyForQuery reports, and a
// failed block answers nothing but its end, on either protocol.
func TestExtended_transactionStatus(t *testing.T) {
	e := &echoLogin{}
	conn := dialEcho(t, e, pgx.QueryExecModeCacheStatement)
	pc := conn.PgConn()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	exec := func(stmt string) error {
		_, err := pc.Exec(ctx, stmt).ReadAll()
		return err
	}
	if pc.TxStatus() != 'I' {
		t.Fatalf("status at start %c", pc.TxStatus())
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pc.TxStatus() != 'T' {
		t.Fatalf("status after BEGIN %c, want T", pc.TxStatus())
	}
	var got string
	if err := tx.QueryRow(ctx, "SELECT stmt FROM t WHERE id = $1", 1).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if pc.TxStatus() != 'T' {
		t.Fatalf("status after a read in the block %c, want T", pc.TxStatus())
	}
	err = tx.QueryRow(ctx, "DELETE FROM t WHERE id = $1", 1).Scan(&got)
	requirePgError(t, err)
	if pc.TxStatus() != 'E' {
		t.Fatalf("status after an error in the block %c, want E", pc.TxStatus())
	}
	err = tx.QueryRow(ctx, "SELECT stmt FROM t WHERE id = $1", 1).Scan(&got)
	if pe := requirePgError(t, err); pe.Code != "25P02" {
		t.Fatalf("extended statement in a failed block: %s, want 25P02", pe.Code)
	}
	if pe := requirePgError(t, exec("SELECT 1")); pe.Code != "25P02" {
		t.Fatalf("simple statement in a failed block: %s, want 25P02", pe.Code)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if pc.TxStatus() != 'I' {
		t.Fatalf("status after ROLLBACK %c, want I", pc.TxStatus())
	}
	// COMMIT of a failed block is a ROLLBACK.
	if err := exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	requirePgError(t, exec("DELETE FROM t"))
	res, err := pc.Exec(ctx, "COMMIT").ReadAll()
	if err != nil || len(res) != 1 || res[0].CommandTag.String() != "ROLLBACK" || pc.TxStatus() != 'I' {
		t.Fatalf("COMMIT of a failed block: %v %v status %c, want ROLLBACK and I", res, err, pc.TxStatus())
	}
	// Outside a block an error leaves the status idle.
	requirePgError(t, exec("DELETE FROM t"))
	if pc.TxStatus() != 'I' {
		t.Fatalf("status after an error outside a block %c, want I", pc.TxStatus())
	}
}

// ---- the protocol message by message --------------------------------------

// rawConn is a logged-in connection read message by message.
type rawConn struct {
	t  *testing.T
	c  net.Conn
	fe *pgproto3.Frontend
}

func dialRaw(t *testing.T, login LoginFunc) *rawConn {
	t.Helper()
	addr := serveAddrCfg(t, Config{Login: login})
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	r := &rawConn{t: t, c: c, fe: pgproto3.NewFrontend(c, c)}
	r.fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{"user": "app", "database": "orders"}})
	if err := r.fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := scramClientLogin(r.fe, portPassword, nil); err != nil {
		t.Fatalf("login: %v", err)
	}
	if got := r.until(); got[len(got)-1] != "ReadyForQuery I" {
		t.Fatalf("login: %v", got)
	}
	return r
}

// send sends msgs and reads what comes back up to ReadyForQuery (or, with no
// Sync among them, the n messages asked for).
func (r *rawConn) send(msgs ...pgproto3.FrontendMessage) []string {
	r.t.Helper()
	for _, m := range msgs {
		r.fe.Send(m)
	}
	if err := r.fe.Flush(); err != nil {
		r.t.Fatal(err)
	}
	return r.until()
}

// read reads n messages.
func (r *rawConn) read(n int) []string {
	r.t.Helper()
	var out []string
	for range n {
		m, err := r.fe.Receive()
		if err != nil {
			r.t.Fatalf("after %v: %v", out, err)
		}
		out = append(out, summarize(m))
	}
	return out
}

// until reads up to and including ReadyForQuery, or the password request.
func (r *rawConn) until() []string {
	r.t.Helper()
	var out []string
	for {
		m, err := r.fe.Receive()
		if err != nil {
			r.t.Fatalf("after %v: %v", out, err)
		}
		out = append(out, summarize(m))
		switch m.(type) {
		case *pgproto3.ReadyForQuery, *pgproto3.AuthenticationCleartextPassword:
			return out
		}
	}
}

// summarize names a backend message and what of it the tests compare.
func summarize(m pgproto3.BackendMessage) string {
	switch x := m.(type) {
	case *pgproto3.ReadyForQuery:
		return "ReadyForQuery " + string(x.TxStatus)
	case *pgproto3.ErrorResponse:
		return "Error " + x.Code
	case *pgproto3.NoticeResponse:
		return "Notice " + x.Code
	case *pgproto3.CommandComplete:
		return "CommandComplete " + string(x.CommandTag)
	case *pgproto3.DataRow:
		cells := make([]string, len(x.Values))
		for i, v := range x.Values {
			if v == nil {
				cells[i] = "NULL"
			} else {
				cells[i] = string(v)
			}
		}
		return "DataRow " + strings.Join(cells, "|")
	case *pgproto3.RowDescription:
		var names []string
		for _, f := range x.Fields {
			names = append(names, fmt.Sprintf("%s/%d", f.Name, f.Format))
		}
		return "RowDescription " + strings.Join(names, ",")
	case *pgproto3.ParameterDescription:
		return fmt.Sprintf("ParameterDescription %v", x.ParameterOIDs)
	case *pgproto3.ParameterStatus, *pgproto3.BackendKeyData:
		return "Status"
	}
	return strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3.")
}

func want(t *testing.T, what string, got []string, w ...string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(w, "\n") {
		t.Fatalf("%s:\n got %q\nwant %q", what, got, w)
	}
}

func TestExtended_messageByMessage(t *testing.T) {
	e := &echoLogin{rows: 5}
	r := dialRaw(t, e.login)
	const stmt = "SELECT stmt FROM t"
	row := "DataRow " + stmt

	// Execute with a row limit hands the rows over a few at a time.
	want(t, "Execute with a limit", r.send(
		&pgproto3.Parse{Query: stmt}, &pgproto3.Bind{}, &pgproto3.Execute{MaxRows: 2}, &pgproto3.Execute{MaxRows: 2},
		&pgproto3.Execute{}, &pgproto3.Sync{}),
		"ParseComplete", "BindComplete", row, row, "PortalSuspended", row, row, "PortalSuspended", row,
		"CommandComplete SELECT 1", "ReadyForQuery I")

	// A limit that reaches the last row exactly suspends, as on PostgreSQL:
	// the next Execute ends the portal with no row.
	want(t, "a limit that reaches the end", r.send(
		&pgproto3.Parse{Query: stmt}, &pgproto3.Bind{}, &pgproto3.Execute{MaxRows: 5}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"ParseComplete", "BindComplete", row, row, row, row, row, "PortalSuspended", "CommandComplete SELECT 0", "ReadyForQuery I")

	// The portal ended with the implicit transaction its Sync closed.
	want(t, "Execute after Sync", r.send(&pgproto3.Execute{}, &pgproto3.Sync{}), "Error 34000", "ReadyForQuery I")

	// After an error everything up to Sync is discarded, a simple query too.
	want(t, "discard until Sync", r.send(
		&pgproto3.Parse{Query: "SELECT $0"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Query{String: "SELECT 1"}, &pgproto3.Sync{}),
		"Error 42P02", "ReadyForQuery I")

	// Named statements: twice is an error, unknown is another.
	want(t, "named statement", r.send(
		&pgproto3.Parse{Name: "s1", Query: "SELECT stmt FROM t WHERE a = $1"},
		&pgproto3.Describe{ObjectType: 'S', Name: "s1"},
		&pgproto3.Parse{Name: "s1", Query: "SELECT 2"}, &pgproto3.Sync{}),
		"ParseComplete", "ParameterDescription [0]", "RowDescription stmt/0", "Error 42P05", "ReadyForQuery I")
	want(t, "unknown statement", r.send(&pgproto3.Bind{PreparedStatement: "nope"}, &pgproto3.Sync{}), "Error 26000", "ReadyForQuery I")
	want(t, "describe unknown statement", r.send(&pgproto3.Describe{ObjectType: 'S', Name: "nope"}, &pgproto3.Sync{}), "Error 26000", "ReadyForQuery I")
	want(t, "describe unknown portal", r.send(&pgproto3.Describe{ObjectType: 'P', Name: "nope"}, &pgproto3.Sync{}), "Error 34000", "ReadyForQuery I")

	// The named statement survives Sync; a Bind with the wrong count of
	// arguments is refused.
	want(t, "wrong argument count", r.send(&pgproto3.Bind{PreparedStatement: "s1"}, &pgproto3.Sync{}), "Error 08P01", "ReadyForQuery I")
	e.setRows(1)
	want(t, "bind, describe, execute", r.send(
		&pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("v")}, ResultFormatCodes: []int16{1}},
		&pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"BindComplete", "RowDescription stmt/1", "DataRow SELECT stmt FROM t WHERE a =  'v' ", "CommandComplete SELECT 1", "ReadyForQuery I")
	want(t, "too many result formats", r.send(
		&pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("v")}, ResultFormatCodes: []int16{0, 0}},
		&pgproto3.Execute{}, &pgproto3.Sync{}),
		"BindComplete", "Error 08P01", "ReadyForQuery I")
	want(t, "binary argument", r.send(
		&pgproto3.Parse{Name: "s2", Query: "SELECT stmt FROM t WHERE a = $1", ParameterOIDs: []uint32{oidInt4}},
		&pgproto3.Bind{PreparedStatement: "s2", ParameterFormatCodes: []int16{1}, Parameters: [][]byte{{0, 0, 0, 42}}},
		&pgproto3.Execute{}, &pgproto3.Sync{}),
		"ParseComplete", "BindComplete", "DataRow SELECT stmt FROM t WHERE a =  42 ", "CommandComplete SELECT 1", "ReadyForQuery I")
	want(t, "bad text argument", r.send(
		&pgproto3.Bind{PreparedStatement: "s2", Parameters: [][]byte{[]byte("4x")}}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"Error 22P02", "ReadyForQuery I")

	// A statement that returns no rows is described as such, without running.
	before := len(e.statements())
	want(t, "describe SET", r.send(
		&pgproto3.Parse{Query: "SET application_name = $1"}, &pgproto3.Describe{ObjectType: 'S'},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("x")}}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"ParseComplete", "ParameterDescription [0]", "NoData", "BindComplete", "NoData", "CommandComplete SET", "ReadyForQuery I")
	want(t, "empty query", r.send(
		&pgproto3.Parse{Query: " ; "}, &pgproto3.Describe{ObjectType: 'S'}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"ParseComplete", "ParameterDescription []", "NoData", "BindComplete", "EmptyQueryResponse", "ReadyForQuery I")
	if n := len(e.statements()); n != before {
		t.Fatalf("SET or an empty query reached the copy: %v", e.statements()[before:])
	}

	// Close: of a statement closes its portals; of what does not exist is
	// not an error.
	want(t, "close", r.send(
		&pgproto3.Bind{DestinationPortal: "p1", PreparedStatement: "s1", Parameters: [][]byte{[]byte("v")}},
		&pgproto3.Close{ObjectType: 'S', Name: "s1"}, &pgproto3.Execute{Portal: "p1"}, &pgproto3.Sync{}),
		"BindComplete", "CloseComplete", "Error 34000", "ReadyForQuery I")
	want(t, "close what is not there", r.send(&pgproto3.Close{ObjectType: 'P', Name: "zz"}, &pgproto3.Close{ObjectType: 'S', Name: "zz"}, &pgproto3.Sync{}),
		"CloseComplete", "CloseComplete", "ReadyForQuery I")

	// Flush sends what is waiting, with no ReadyForQuery.
	r.fe.Send(&pgproto3.Parse{Name: "s3", Query: "SELECT 1"})
	r.fe.Send(&pgproto3.Flush{})
	if err := r.fe.Flush(); err != nil {
		t.Fatal(err)
	}
	want(t, "flush", r.read(1), "ParseComplete")
	want(t, "sync after flush", r.send(&pgproto3.Sync{}), "ReadyForQuery I")

	// In a transaction block a portal outlives Sync, so a client can fetch
	// a few rows at a time; the block's end closes it.
	e.setRows(3)
	want(t, "BEGIN", r.send(&pgproto3.Query{String: "BEGIN"}), "CommandComplete BEGIN", "ReadyForQuery T")
	want(t, "fetch 1", r.send(
		&pgproto3.Parse{Query: stmt}, &pgproto3.Bind{DestinationPortal: "c1"}, &pgproto3.Execute{Portal: "c1", MaxRows: 2}, &pgproto3.Sync{}),
		"ParseComplete", "BindComplete", row, row, "PortalSuspended", "ReadyForQuery T")
	want(t, "fetch 2", r.send(&pgproto3.Execute{Portal: "c1", MaxRows: 2}, &pgproto3.Sync{}),
		row, "CommandComplete SELECT 1", "ReadyForQuery T")
	want(t, "fetch past the end", r.send(&pgproto3.Execute{Portal: "c1"}, &pgproto3.Sync{}),
		"CommandComplete SELECT 0", "ReadyForQuery T")
	want(t, "COMMIT", r.send(&pgproto3.Query{String: "COMMIT"}), "CommandComplete COMMIT", "ReadyForQuery I")
	want(t, "the block's portal is gone", r.send(&pgproto3.Execute{Portal: "c1"}, &pgproto3.Sync{}), "Error 34000", "ReadyForQuery I")

	// An error in a block fails it: what follows is refused until ROLLBACK,
	// a portal that ran before the error too.
	want(t, "BEGIN again", r.send(&pgproto3.Query{String: "BEGIN"}), "CommandComplete BEGIN", "ReadyForQuery T")
	want(t, "a portal before the error", r.send(
		&pgproto3.Parse{Query: stmt}, &pgproto3.Bind{DestinationPortal: "c2"}, &pgproto3.Execute{Portal: "c2", MaxRows: 1}, &pgproto3.Sync{}),
		"ParseComplete", "BindComplete", row, "PortalSuspended", "ReadyForQuery T")
	want(t, "the error", r.send(&pgproto3.Parse{Query: "DELETE FROM t"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"ParseComplete", "BindComplete", "Error 0A000", "ReadyForQuery E")
	want(t, "in the failed block", r.send(&pgproto3.Execute{Portal: "c2"}, &pgproto3.Sync{}), "Error 25P02", "ReadyForQuery E")
	want(t, "parse in the failed block", r.send(&pgproto3.Parse{Query: "SELECT 1"}, &pgproto3.Sync{}), "Error 25P02", "ReadyForQuery E")
	want(t, "ROLLBACK by the extended protocol", r.send(
		&pgproto3.Parse{Query: "ROLLBACK"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"ParseComplete", "BindComplete", "CommandComplete ROLLBACK", "ReadyForQuery I")
	want(t, "ROLLBACK outside a block", r.send(&pgproto3.Query{String: "ROLLBACK"}),
		"Notice 25P01", "CommandComplete ROLLBACK", "ReadyForQuery I")
	want(t, "BEGIN inside a block", r.send(&pgproto3.Query{String: "BEGIN"}, &pgproto3.Query{String: "BEGIN"}),
		"CommandComplete BEGIN", "ReadyForQuery T")
	want(t, "the second BEGIN", r.until(), "Notice 25001", "CommandComplete BEGIN", "ReadyForQuery T")
	want(t, "end it", r.send(&pgproto3.Query{String: "COMMIT"}), "CommandComplete COMMIT", "ReadyForQuery I")

	// A simple query ends the unnamed statement.
	r.send(&pgproto3.Parse{Query: stmt}, &pgproto3.Sync{})
	r.send(&pgproto3.Query{String: "SET x = 1"})
	want(t, "unnamed statement after a simple query", r.send(&pgproto3.Bind{}, &pgproto3.Sync{}), "Error 26000", "ReadyForQuery I")
}

// A statement described with one shape and run with another is refused in
// PostgreSQL's words, which drivers prepare again on: the client would read
// the rows into the wrong columns.
func TestExtended_aStatementThatChangesShapeIsRefused(t *testing.T) {
	var width atomic.Int32
	width.Store(1)
	login := func(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
		if !password.Matches([]string{portPassword}) {
			return nil, &Error{Code: "28P01", Message: "no"}
		}
		return &Binding{Schema: "public", Run: func(context.Context, string) (sqlsandbox.Result, error) {
			cols := int(width.Load())
			res := sqlsandbox.Result{Rows: [][]any{make([]any, cols)}}
			for i := range cols {
				res.Columns = append(res.Columns, sqlsandbox.Column{Name: fmt.Sprintf("c%d", i), Type: "VARCHAR"})
				res.Rows[0][i] = "v"
			}
			return res, nil
		}}, nil
	}
	r := dialRaw(t, login)
	want(t, "describe", r.send(&pgproto3.Parse{Name: "s", Query: "SELECT * FROM t"}, &pgproto3.Describe{ObjectType: 'S', Name: "s"}, &pgproto3.Sync{}),
		"ParseComplete", "ParameterDescription []", "RowDescription c0/0", "ReadyForQuery I")
	width.Store(2)
	want(t, "execute", r.send(&pgproto3.Bind{PreparedStatement: "s"}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"BindComplete", "Error 0A000", "ReadyForQuery I")
}

// Time travel through the extended protocol: the port's gate is asked at
// every execution of a prepared statement, not once when it was prepared, and
// a Describe meets it before it says anything about the table.
func TestExtended_timeTravelMeetsTheGateAtEveryExecution(t *testing.T) {
	var gateMu sync.Mutex
	hidden := ""
	setHidden := func(h string) { gateMu.Lock(); hidden = h; gateMu.Unlock() }
	e := &echoLogin{}
	e.tt = func() *Binding {
		return &Binding{Run: e.run, Actor: "server:orders", Schema: "public",
			// An index nobody answers on: a statement past the gate fails
			// there, which is how this test tells "passed the gate" from
			// "refused by it".
			TimeTravel: newDeadEngine(t),
			TimeTravelRefusal: func() string {
				gateMu.Lock()
				defer gateMu.Unlock()
				return hidden
			}}
	}
	r := dialRaw(t, e.login)
	const stmt = "SELECT * FROM _flashback.orders AS OF $1 WHERE id = $2"
	const why = "this server hides rows"
	ts := []byte("5 minutes ago")

	// Describe behind a closed gate: refused, and the statement is gone, so
	// it can be prepared again under its name.
	setHidden(why)
	want(t, "describe behind a closed gate", r.send(
		&pgproto3.Parse{Name: "tt", Query: stmt}, &pgproto3.Describe{ObjectType: 'S', Name: "tt"}, &pgproto3.Sync{}),
		"ParseComplete", "Error 42501", "ReadyForQuery I")
	// Behind an open gate the Describe reaches the engine's own checks (the
	// primary key, read from the index that is not there).
	setHidden("")
	want(t, "describe behind an open gate", r.send(
		&pgproto3.Parse{Name: "tt", Query: stmt}, &pgproto3.Describe{ObjectType: 'S', Name: "tt"}, &pgproto3.Sync{}),
		"ParseComplete", "Error 42601", "ReadyForQuery I")

	// Prepared once, executed twice: the gate is asked at each execution.
	want(t, "prepare", r.send(&pgproto3.Parse{Name: "tt", Query: stmt}, &pgproto3.Sync{}), "ParseComplete", "ReadyForQuery I")
	exec := &pgproto3.Bind{PreparedStatement: "tt", Parameters: [][]byte{ts, []byte("1")}}
	want(t, "an open gate", r.send(exec, &pgproto3.Execute{}, &pgproto3.Sync{}), "BindComplete", "Error 42601", "ReadyForQuery I")
	setHidden(why)
	want(t, "after the rule was saved", r.send(exec, &pgproto3.Execute{}, &pgproto3.Sync{}), "BindComplete", "Error 42501", "ReadyForQuery I")
	// An argument with a quote cannot be written for the time-travel parser.
	setHidden("")
	want(t, "a quote in a time-travel argument", r.send(
		&pgproto3.Bind{PreparedStatement: "tt", Parameters: [][]byte{ts, []byte("it's")}}, &pgproto3.Sync{}),
		"Error 22023", "ReadyForQuery I")
	for _, st := range e.statements() {
		if strings.Contains(st, "_flashback") {
			t.Fatalf("a time-travel statement reached the copy: %q", st)
		}
	}
}

// The time-travel-only front-end (bintrail-pg flashback) shares the wire: the
// extended protocol answers there too, with its own refusals.
func TestExtended_flashbackFrontEnd(t *testing.T) {
	addr := serveAddr(t, testAuth(t))
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s/public?sslmode=disable", testUser, testPass, addr))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, c := range []struct {
		stmt string
		args []any
		code string
		msg  string
	}{
		{"SELECT * FROM _flashback.orders AS OF $1", []any{"5 minutes ago"}, "0A000", "full-table"},
		{"SELECT $1::int", []any{1}, "0A000", "only _flashback"},
		{"SELECT * FROM _flashback.orders AS OF 'nonsense' WHERE id = $1", []any{1}, "42601", ""},
	} {
		rows, err := conn.Query(ctx, c.stmt, c.args...)
		if err == nil {
			rows.Close()
			err = rows.Err()
		}
		pe := requirePgError(t, err)
		if pe.Code != c.code || !strings.Contains(pe.Message, c.msg) {
			t.Errorf("%s: %s %q, want %s with %q", c.stmt, pe.Code, pe.Message, c.code, c.msg)
		}
	}
	if _, err := conn.Exec(ctx, "SET x = 1"); err != nil {
		t.Fatalf("the connection after the refusals: %v", err)
	}
}

// A Describe never runs the client's statement itself: only the subquery that
// returns no row, counted and recorded like every statement the port runs.
// A statement that cannot stand in one is described as returning no rows, and
// its Execute is what runs it.
func TestExtended_describeNeverRunsTheStatementItself(t *testing.T) {
	rec := audittest.Install(t)
	e := &echoLogin{}
	r := dialRaw(t, e.login)
	want(t, "describe what cannot stand in a subquery", r.send(
		&pgproto3.Parse{Name: "show", Query: "SHOW TABLES"}, &pgproto3.Describe{ObjectType: 'S', Name: "show"}, &pgproto3.Sync{}),
		"ParseComplete", "ParameterDescription []", "NoData", "ReadyForQuery I")
	want(t, "describe a read", r.send(
		&pgproto3.Parse{Name: "read", Query: "SELECT stmt FROM t WHERE a = $1"}, &pgproto3.Describe{ObjectType: 'S', Name: "read"}, &pgproto3.Sync{}),
		"ParseComplete", "ParameterDescription [0]", "RowDescription stmt/0", "ReadyForQuery I")
	ran := e.statements()
	if len(ran) != 2 {
		t.Fatalf("the describes ran %q, want two subqueries", ran)
	}
	for _, st := range ran {
		if !strings.HasPrefix(st, "SELECT * FROM (\n") || !strings.HasSuffix(st, "\n) AS dbtrail_describe LIMIT 0") {
			t.Fatalf("a describe ran the statement itself: %q", st)
		}
	}
	if n := e.timesAnswered(); n != 2 {
		t.Fatalf("the binding was told of %d statements, want the 2 the describes ran", n)
	}
	var recorded []string
	for _, ev := range rec.Events() {
		if ev.Surface == "shim" && ev.Action == "sql.run" {
			recorded = append(recorded, ev.Detail["sql"])
		}
	}
	if len(recorded) != 1 || recorded[0] != ran[1] {
		t.Fatalf("audit records %q, want the one describe that answered (%q)", recorded, ran[1])
	}
	// Described as no rows, the statement's Execute still answers its rows:
	// nothing was held against it.
	want(t, "execute the undescribed", r.send(&pgproto3.Bind{PreparedStatement: "show"}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"BindComplete", "DataRow SHOW TABLES", "CommandComplete SELECT 1", "ReadyForQuery I")
}

// A Describe the copy could not answer for a reason of the moment (busy,
// behind, a timeout) fails, so the client does not keep "no rows" for a
// statement that has them; one it could not answer for the statement's own
// shape or text is described as no rows.
func TestExtended_aDescribeThatFailsForTheMomentIsAnError(t *testing.T) {
	e := &echoLogin{}
	r := dialRaw(t, e.login)
	want(t, "busy", r.send(&pgproto3.Parse{Name: "b", Query: "SELECT BUSY"}, &pgproto3.Describe{ObjectType: 'S', Name: "b"}, &pgproto3.Sync{}),
		"ParseComplete", "Error 53000", "ReadyForQuery I")
	// Gone with its error: prepared again under its name.
	want(t, "again", r.send(&pgproto3.Parse{Name: "b", Query: "SELECT 1"}, &pgproto3.Sync{}), "ParseComplete", "ReadyForQuery I")
	want(t, "a missing table", r.send(&pgproto3.Parse{Name: "m", Query: "SELECT * FROM no_such_table"}, &pgproto3.Describe{ObjectType: 'S', Name: "m"}, &pgproto3.Sync{}),
		"ParseComplete", "ParameterDescription []", "NoData", "ReadyForQuery I")
}

// A time-travel row goes out in the columns the statement was described
// with, by name: a column the row lacks is NULL, one the table no longer has
// is left out with a notice. Never a value under another column's name.
func TestExtended_aTimeTravelRowGoesIntoTheDescribedColumns(t *testing.T) {
	var out strings.Builder
	s := &session{be: pgproto3.NewBackend(strings.NewReader(""), &out), logger: discardLogger()}
	cols, rows := s.intoDescribed([]string{"id", "email", "name"}, []string{"id", "name", "mail"},
		[][][]byte{{[]byte("1"), []byte("ada"), []byte("a@x")}})
	if fmt.Sprint(cols) != "[id email name]" {
		t.Fatalf("columns %v", cols)
	}
	if got := fmt.Sprintf("%q", rows[0]); got != `["1" "" "ada"]` || rows[0][1] != nil {
		t.Fatalf("row %s (email nil: %v), want id, NULL, ada", got, rows[0][1] == nil)
	}
	if err := s.be.Flush(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "mail") {
		t.Fatal("no notice named the column left out")
	}
}

// What one connection keeps open is bounded in bytes, not only in count.
func TestExtended_whatAConnectionHoldsIsBounded(t *testing.T) {
	oldS, oldP := maxStatementBytes, maxPortalBytes
	maxStatementBytes, maxPortalBytes = 64, 400
	t.Cleanup(func() { maxStatementBytes, maxPortalBytes = oldS, oldP })
	e := &echoLogin{rows: 4}
	r := dialRaw(t, e.login)
	long := "SELECT stmt FROM t WHERE a = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx'"
	want(t, "statements", r.send(&pgproto3.Parse{Name: "s1", Query: long}, &pgproto3.Parse{Name: "s2", Query: long}, &pgproto3.Sync{}),
		"ParseComplete", "Error 54000", "ReadyForQuery I")
	want(t, "begin", r.send(&pgproto3.Query{String: "BEGIN"}), "CommandComplete BEGIN", "ReadyForQuery T")
	// Each answer is 4 rows of the statement's text: one fits, a second does not.
	got := r.send(&pgproto3.Bind{DestinationPortal: "p1", PreparedStatement: "s1"}, &pgproto3.Execute{Portal: "p1", MaxRows: 1},
		&pgproto3.Bind{DestinationPortal: "p2", PreparedStatement: "s1"}, &pgproto3.Execute{Portal: "p2", MaxRows: 1},
		&pgproto3.Sync{})
	if got[len(got)-2] != "Error 54000" || got[len(got)-1] != "ReadyForQuery E" {
		t.Fatalf("portals past the budget: %q", got)
	}
}

// The rows a portal keeps between one Execute and the next count in the
// port's result budget while it keeps them, and are given back at every way
// the portal ends: its last row sent, Close, the end of the block, a failed
// block's Sync, the connection's end.
func TestExtended_aSuspendedPortalCountsInTheResultBudget(t *testing.T) {
	b := NewResultBudget(100) // less than one answer: a held one blocks the next
	e := &echoLogin{rows: 4}
	addr := serveAddrCfg(t, Config{Login: e.login, ResultBudget: b})
	dial := func() *rawConn {
		t.Helper()
		c, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		_ = c.SetDeadline(time.Now().Add(30 * time.Second))
		r := &rawConn{t: t, c: c, fe: pgproto3.NewFrontend(c, c)}
		r.fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters: map[string]string{"user": "app", "database": "orders"}})
		_ = r.fe.Flush()
		if err := scramClientLogin(r.fe, portPassword, nil); err != nil {
			t.Fatalf("login: %v", err)
		}
		r.until()
		return r
	}
	const stmt = "SELECT stmt FROM t"
	row := "DataRow " + stmt
	held := func(what string, wantHeld bool) {
		t.Helper()
		if got := b.Held() > 0; got != wantHeld {
			t.Fatalf("%s: the budget holds %d bytes, want held=%v", what, b.Held(), wantHeld)
		}
	}
	suspend := func(r *rawConn, portal string) {
		t.Helper()
		want(t, "suspend "+portal, r.send(&pgproto3.Parse{Query: stmt}, &pgproto3.Bind{DestinationPortal: portal},
			&pgproto3.Execute{Portal: portal, MaxRows: 1}, &pgproto3.Sync{}),
			"ParseComplete", "BindComplete", row, "PortalSuspended", "ReadyForQuery T")
	}
	r := dial()
	want(t, "begin", r.send(&pgproto3.Query{String: "BEGIN"}), "CommandComplete BEGIN", "ReadyForQuery T")

	// Kept between Executes: counted, and the port refuses the next statement.
	suspend(r, "c1")
	held("a suspended portal", true)
	other := dial()
	want(t, "another connection while the rows are kept", other.send(&pgproto3.Query{String: stmt}), "Error 53000", "ReadyForQuery I")
	// The rest sent: given back.
	want(t, "the rest", r.send(&pgproto3.Execute{Portal: "c1"}, &pgproto3.Sync{}), row, row, row, "CommandComplete SELECT 3", "ReadyForQuery T")
	held("after the last row", false)

	suspend(r, "c2")
	want(t, "close", r.send(&pgproto3.Close{ObjectType: 'P', Name: "c2"}, &pgproto3.Sync{}), "CloseComplete", "ReadyForQuery T")
	held("after Close", false)

	suspend(r, "c3")
	want(t, "commit", r.send(&pgproto3.Query{String: "COMMIT"}), "CommandComplete COMMIT", "ReadyForQuery I")
	held("after COMMIT", false)

	want(t, "begin again", r.send(&pgproto3.Query{String: "BEGIN"}), "CommandComplete BEGIN", "ReadyForQuery T")
	suspend(r, "c4")
	want(t, "an error in the block", r.send(&pgproto3.Parse{Query: "DELETE FROM t"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}),
		"ParseComplete", "BindComplete", "Error 53000", "ReadyForQuery E")
	held("after the failed block's Sync", false)
	want(t, "the block's portal", r.send(&pgproto3.Execute{Portal: "c4"}, &pgproto3.Sync{}), "Error 25P02", "ReadyForQuery E")
	want(t, "rollback", r.send(&pgproto3.Query{String: "ROLLBACK"}), "CommandComplete ROLLBACK", "ReadyForQuery I")

	// The connection ends with a portal kept: given back.
	last := dial()
	want(t, "begin on the last", last.send(&pgproto3.Query{String: "BEGIN"}), "CommandComplete BEGIN", "ReadyForQuery T")
	suspend(last, "c5")
	held("a portal on a connection about to end", true)
	last.c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for b.Held() != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	held("after the connection ended", false)
}
