package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The router's PostgreSQL-protocol port (#2765): which address it takes, and
// who its login lets in. None of the servers here has an index that answers
// (their DSN points at a closed port), so a login that passes the password
// and finds its server ends in "cannot open server" (08006): that code is
// this file's proof that the password was accepted and the server found.

func TestValidPGListen(t *testing.T) {
	for _, tc := range []struct {
		addr string
		ok   bool
		want string
	}{
		{"127.0.0.1:5433", true, ""},
		{"localhost:5433", true, ""},
		{"[::1]:5433", true, ""},
		{"127.0.0.2:5433", true, ""},
		{"0.0.0.0:5433", false, "this machine only"},
		{":5433", false, "this machine only"},
		{"[::]:5433", false, "this machine only"},
		{"10.0.0.5:5433", false, "this machine only"},
		{"db.example.com:5433", false, "this machine only"},
		{"LOCALHOST:5433", false, "this machine only"},
		{"127.0.0.1", false, "not an address"},
		{"127.0.0.1:0", false, "not a port number"},
		{"127.0.0.1:70000", false, "not a port number"},
		{"127.0.0.1:pg", false, "not a port number"},
		{" 127.0.0.1:5433", false, "this machine only"},
	} {
		err := validPGListen(tc.addr)
		if tc.ok != (err == nil) {
			t.Errorf("validPGListen(%q) = %v, want ok=%v", tc.addr, err, tc.ok)
			continue
		}
		if err != nil && !strings.Contains(err.Error(), tc.want) {
			t.Errorf("validPGListen(%q) = %q, want it to say %q", tc.addr, err, tc.want)
		}
	}
}

func TestRouterSettings_pgListen(t *testing.T) {
	clearRouterEnv(t)
	cmd := routerTestCmd(t, "--pg-listen", "0.0.0.0:5433")
	if _, err := routerSettingsFrom(cmd); err == nil || !strings.Contains(err.Error(), "--pg-listen") {
		t.Fatalf("an address other machines reach was taken: %v", err)
	}
	cmd = routerTestCmd(t, "--pg-listen", "127.0.0.1:5433")
	st, err := routerSettingsFrom(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if st.PGListen != "127.0.0.1:5433" {
		t.Fatalf("PGListen = %q", st.PGListen)
	}
	t.Setenv("BINTRAIL_ROUTER_PG_LISTEN", "localhost:6000")
	st, err = routerSettingsFrom(routerTestCmd(t))
	if err != nil {
		t.Fatal(err)
	}
	if st.PGListen != "localhost:6000" {
		t.Fatalf("from the environment, PGListen = %q", st.PGListen)
	}
	// Unset is no port at all, and no line about one.
	t.Setenv("BINTRAIL_ROUTER_PG_LISTEN", "")
	st, err = routerSettingsFrom(routerTestCmd(t))
	if err != nil || st.PGListen != "" {
		t.Fatalf("unset: PGListen = %q, err %v", st.PGListen, err)
	}
	if line := routerPGLine(""); line != "" {
		t.Fatalf("a line for a port that is not there: %q", line)
	}
	line := routerPGLine("127.0.0.1:5433")
	for _, want := range []string{"127.0.0.1:5433", "dbname=<server id or name>", "clear text"} {
		if !strings.Contains(line, want) {
			t.Errorf("the start-up line %q does not say %q", line, want)
		}
	}
}

func TestPasswordAccepted(t *testing.T) {
	for _, tc := range []struct {
		list  []string
		given string
		want  bool
	}{
		{[]string{"a", "b"}, "a", true},
		{[]string{"a", "b"}, "b", true},
		{[]string{"a", "b"}, "c", false},
		{[]string{"a", "b"}, "", false},
		{[]string{""}, "", false},
		{[]string{"", "a"}, "", false},
		{nil, "a", false},
		{[]string{"a"}, "A", false},
		{[]string{"a"}, "a ", false},
		{[]string{"a", "a"}, "a", true},
	} {
		if got := passwordAccepted(tc.list, tc.given); got != tc.want {
			t.Errorf("passwordAccepted(%q, %q) = %v, want %v", tc.list, tc.given, got, tc.want)
		}
	}
}

// pgLoginCode connects a PostgreSQL client and returns the SQLSTATE the port
// refused it with, "" when it was let in.
func pgLoginCode(t *testing.T, addr, user, password, database string) string {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s sslmode=disable", host, port))
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Password, cfg.Database = user, password, database
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err == nil {
		_ = conn.Close(ctx)
		return ""
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("%s / %s / %s: %v, want a PostgreSQL error", user, password, database, err)
	}
	return pe.Code
}

func TestRouterPGPort_login(t *testing.T) {
	f := newRouterFiles(t)
	prod, err := f.owner.Add(console.ServerEntry{Name: "prod", DSN: "u:p@tcp(127.0.0.1:1)/idx_prod"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.Add(console.ServerEntry{Name: "staging", DSN: "u:p@tcp(127.0.0.1:1)/idx_staging"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(prod.ID, "app1", "pw-one"); err != nil {
		t.Fatal(err)
	}
	f.savePortFile(t, true, "port-pw")
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- servePGPort(ctx, srv, ln, flashbackConfig{}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the PostgreSQL port did not stop with its context")
		}
	})
	addr := ln.Addr().String()

	const found = "08006" // the password passed and the server was found (see the top of the file)
	for _, tc := range []struct {
		user, pw, db, want, why string
	}{
		{"anyone", "port-pw", "prod", found, "the port's password and a server's name"},
		{"anyone", "port-pw", prod.ID, found, "the port's password and a server's id"},
		{"anyone", "port-pw", "staging", found, "the other server"},
		{"prod", "port-pw", "", found, "no database: the user's name is the database"},
		{"anyone", "port-pw", "nowhere", "3D000", "a server that does not exist, with the right password"},
		{"anyone", "port-pw", "PROD", "3D000", "a server's name in another case"},
		{"anyone", "wrong", "prod", "28P01", "a wrong password"},
		{"anyone", "wrong", "nowhere", "28P01", "a wrong password says nothing about the server"},
		{"anyone", "", "prod", "28P01", "an empty password"},
		{"anyone", "pw-one", "prod", "28P01", "a login's password under another username"},
		{"app1", "pw-one", "prod", found, "the application login with its own password"},
		{"app1", "pw-one", "staging", found, "the application login opens its own server whatever database it names"},
		{"app1", "pw-one", "nowhere", found, "even a database that does not exist"},
		{"app1", "port-pw", "prod", found, "the application login with the port's password"},
		{"App1", "pw-one", "prod", "28P01", "the login in another case"},
		{"app1", "wrong", "prod", "28P01", "the login with a wrong password"},
	} {
		if got := pgLoginCode(t, addr, tc.user, tc.pw, tc.db); got != tc.want {
			t.Errorf("%s (%s / %s / %s): SQLSTATE %q, want %q", tc.why, tc.user, tc.pw, tc.db, got, tc.want)
		}
	}

	// The port turned off: no password opens it, the token's included.
	f.savePortFile(t, false, "port-pw")
	srv.RefreshFollowedFiles()
	if got := pgLoginCode(t, addr, "anyone", "port-pw", "prod"); got != "28P01" {
		t.Errorf("with the port turned off the password still answers %q, want 28P01", got)
	}
	if got := pgLoginCode(t, addr, "app1", "pw-one", "prod"); got != "28P01" {
		t.Errorf("with the port turned off the application login still answers %q, want 28P01", got)
	}
}
