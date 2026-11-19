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
	// An unknown parameter: refused as PostgreSQL refuses it, or NULL with
	// missing_ok.
	var pgErr *pgconn.PgError
	err = conn.QueryRow(ctx, "SELECT current_setting('no_such_setting')").Scan(&app)
	if !errors.As(err, &pgErr) || pgErr.Code != "42704" {
		t.Errorf("an unknown parameter: %v", err)
	}
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
