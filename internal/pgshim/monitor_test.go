package pgshim

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// A connection opened with the monitor login answers what ProxySQL's
// PostgreSQL monitor sends (an empty query as its ping, SELECT
// pg_is_in_recovery()) and the session's own statements, and refuses the
// rest: it opens no server.
func TestMonitorLogin_answersOnlyAProxysChecks(t *testing.T) {
	login := func(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
		if !password.Matches([]string{portPassword}) {
			return nil, &Error{Code: "28P01", Message: "no"}
		}
		return &Binding{Actor: "monitor", Schema: "public", Monitor: true}, nil
	}
	addr := serveAddrCfg(t, Config{Login: login})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://monitor:%s@%s/postgres?sslmode=disable", portPassword, addr))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	// ProxySQL's ping is an empty query, sent as a simple query.
	if err := conn.PgConn().Exec(ctx, "").Close(); err != nil {
		t.Errorf("the ping: %v", err)
	}
	var standby bool
	if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()", pgx.QueryExecModeSimpleProtocol).Scan(&standby); err != nil || !standby {
		t.Errorf("pg_is_in_recovery(): %v %v, want true", standby, err)
	}
	if _, err := conn.Exec(ctx, "SET application_name = 'proxysql'"); err != nil {
		t.Errorf("a SET: %v", err)
	}
	for _, q := range []string{"SELECT 1", "SELECT * FROM orders", "SELECT relname FROM pg_class"} {
		var pe *pgconn.PgError
		_, err := conn.Exec(ctx, q)
		if !errors.As(err, &pe) || pe.Code != "0A000" {
			t.Errorf("%s: %v, want the monitor login's refusal", q, err)
		}
	}
	// And the connection goes on.
	if err := conn.PgConn().Exec(ctx, "").Close(); err != nil {
		t.Errorf("the ping after the refusals: %v", err)
	}
}

// On any connection pg_is_in_recovery() is true: the port is read-only.
func TestPgIsInRecovery_onTheCopy(t *testing.T) {
	addr := serveAddrCfg(t, Config{Login: (&copyLogin{}).login})
	r, err := dialStatus(t, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := r.query("SELECT pg_is_in_recovery()")
	if len(got) < 2 || got[0] != "Columns pg_is_in_recovery:16" || got[1] != "DataRow t" {
		t.Errorf("answered %q", got)
	}
}
