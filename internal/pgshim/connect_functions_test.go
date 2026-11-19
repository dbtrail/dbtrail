package pgshim

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// What psycopg-based tools send on connect is answered by the port: the
// copy is DuckDB, which has no pg_backend_pid and its own settings, so the
// statement failed there and the tool did not connect (HTAP bench of
// 2026-11-18). Through pgx, which describes the statement first (the
// extended protocol) and reads the integer in binary.
func TestConnectFunctions_answeredByThePort(t *testing.T) {
	l := &copyLogin{}
	addr := serveAddrCfg(t, Config{Login: l.login})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://app:%s@%s/orders?sslmode=disable", portPassword, addr))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	l.mu.Lock()
	ranBefore := len(l.ran)
	l.mu.Unlock()

	var pid int32
	var version string
	if err := conn.QueryRow(ctx, "SELECT pg_backend_pid(), current_setting('server_version')").Scan(&pid, &version); err != nil {
		t.Fatal(err)
	}
	// The number a cancel request names this connection by.
	if uint32(pid) != conn.PgConn().PID() || pid == 0 {
		t.Errorf("pg_backend_pid() = %d, the connection's process number is %d", pid, conn.PgConn().PID())
	}
	if version != serverVersion {
		t.Errorf("server_version = %q, want %q", version, serverVersion)
	}
	// A setting the session changed is the one read back.
	if _, err := conn.Exec(ctx, "SET application_name = 'tplytics'"); err != nil {
		t.Fatal(err)
	}
	var app string
	if err := conn.QueryRow(ctx, "select current_setting('Application_Name')").Scan(&app); err != nil || app != "tplytics" {
		t.Errorf("application_name = %q, %v", app, err)
	}
	// An unknown parameter is not the port's to answer: it goes where a SHOW
	// of it goes (the copy here, the source for a forwarding role). NULL
	// with missing_ok, answered here.
	l.mu.Lock()
	before := len(l.ran)
	l.mu.Unlock()
	_ = conn.QueryRow(ctx, "SELECT current_setting('no_such_setting')").Scan(&app)
	l.mu.Lock()
	went := len(l.ran) > before
	l.mu.Unlock()
	if !went {
		t.Error("current_setting of an unknown parameter did not go on to the copy")
	}
	// A prepared current_setting($1): its Describe reads a placeholder in
	// place of $1, which is no parameter, and must not be refused as one.
	var pgErr *pgconn.PgError
	if err := conn.QueryRow(ctx, "select current_setting($1)", "application_name").Scan(&app); errors.As(err, &pgErr) && pgErr.Code == "42704" {
		t.Errorf("a prepared current_setting($1) was refused as an unknown parameter: %v", err)
	}
	l.mu.Lock()
	ranBefore = len(l.ran)
	l.mu.Unlock()
	var missing *string
	if err := conn.QueryRow(ctx, "SELECT current_setting('no_such_setting', true)").Scan(&missing); err != nil || missing != nil {
		t.Errorf("missing_ok: %v %v", missing, err)
	}
	// The simple protocol answers the same.
	var text string
	if err := conn.QueryRow(ctx, "SELECT pg_backend_pid()", pgx.QueryExecModeSimpleProtocol).Scan(&text); err != nil || text != strconv.FormatUint(uint64(conn.PgConn().PID()), 10) {
		t.Errorf("simple protocol: %q %v", text, err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, st := range l.ran[ranBefore:] {
		if strings.Contains(st, "pg_backend_pid") || strings.Contains(st, "current_setting") {
			t.Errorf("reached the copy: %s", st)
		}
	}
}

// The column names and types PostgreSQL gives them: pg_backend_pid is an
// integer, current_setting text, both named after the function.
func TestConnectFunctions_columns(t *testing.T) {
	addr := serveAddrCfg(t, Config{Login: (&copyLogin{}).login})
	r, err := dialStatus(t, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := r.query("SELECT pg_backend_pid(), current_setting('server_version')")
	if len(got) == 0 || got[0] != "Columns pg_backend_pid:23,current_setting:25" {
		t.Errorf("columns: %q", got)
	}
}

// On the time-travel port nothing else can answer it, so an unknown
// parameter is refused there as SHOW refuses it.
func TestConnectFunctions_unknownSettingOnTheTimeTravelPort(t *testing.T) {
	c := &sessionCmd{kind: cmdFunc, fns: []funcCall{{fn: "current_setting", param: "no_such_setting"}}}
	if !c.unknownSetting() {
		t.Fatal("not unknown")
	}
	if _, _, perr := (&session{}).localColumns(c); perr == nil || perr.code != "42704" {
		t.Errorf("refusal: %v", perr)
	}
	c.fns[0].missingOK = true
	if c.unknownSetting() {
		t.Error("missing_ok is not unknown")
	}
}
