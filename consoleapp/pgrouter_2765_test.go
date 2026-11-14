package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The PostgreSQL router (#2765): which address it takes, what it starts
// with, and who its login lets in. A server's index is opened by its first
// statement, not by the login, so a client that is let in here ("") passed
// the password and named a PostgreSQL server the registry holds; none of
// these servers has an index that answers.

func pgRouterTestCmd(t *testing.T, args ...string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "pg-router"}
	addPGRouterFlags(cmd)
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatal(err)
	}
	return cmd
}

func clearPGRouterEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"BINTRAIL_PG_ROUTER_LISTEN", "BINTRAIL_PG_ROUTER_WORKERS", "BINTRAIL_PG_ROUTER_THREADS", "BINTRAIL_PG_ROUTER_MAX_CONNECTIONS", "BINTRAIL_PG_ROUTER_MAX_WAIT",
		"BINTRAIL_CONSOLE_TOKEN", "BINTRAIL_CONSOLE_SERVERS", "BINTRAIL_CONSOLE_AUTH", "BINTRAIL_CONSOLE_MCP_TOKEN_FILE", "BINTRAIL_CONSOLE_SQL_MEMORY", "BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS"} {
		t.Setenv(k, "")
	}
}

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
		err := validPGListen(tc.addr, "--listen")
		if tc.ok != (err == nil) {
			t.Errorf("validPGListen(%q) = %v, want ok=%v", tc.addr, err, tc.ok)
			continue
		}
		if err != nil && !strings.Contains(err.Error(), tc.want) {
			t.Errorf("validPGListen(%q) = %q, want it to say %q", tc.addr, err, tc.want)
		}
	}
}

func TestPGRouterSettings(t *testing.T) {
	clearPGRouterEnv(t)
	st, err := pgRouterSettingsFrom(pgRouterTestCmd(t))
	if err != nil {
		t.Fatal(err)
	}
	if st.Listen != "127.0.0.1:5433" || st.Workers != 2 || st.Threads != 2 || st.MaxConns != 16 {
		t.Fatalf("defaults: %+v", st)
	}
	if st.SQLPortMaxRows != console.DefaultSQLPortMaxRows {
		t.Fatalf("default row cap %d, want the port's %d", st.SQLPortMaxRows, console.DefaultSQLPortMaxRows)
	}

	// The environment, then a flag over it.
	t.Setenv("BINTRAIL_PG_ROUTER_LISTEN", "localhost:6000")
	t.Setenv("BINTRAIL_PG_ROUTER_WORKERS", " 4 ")
	t.Setenv("BINTRAIL_PG_ROUTER_MAX_CONNECTIONS", "100")
	st, err = pgRouterSettingsFrom(pgRouterTestCmd(t, "--workers", "3"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Listen != "localhost:6000" || st.Workers != 3 || st.MaxConns != 100 {
		t.Fatalf("environment and flag: %+v", st)
	}
	clearPGRouterEnv(t)

	for _, tc := range []struct {
		args []string
		env  [2]string
		want string
	}{
		{[]string{"--listen", "0.0.0.0:5433"}, [2]string{}, "this machine only"},
		{[]string{"--listen", "5433"}, [2]string{}, "not an address"},
		{[]string{"--workers", "0"}, [2]string{}, "--workers must be between 1 and 16"},
		{[]string{"--workers", "17"}, [2]string{}, "--workers must be between 1 and 16"},
		{[]string{"--threads", "-1"}, [2]string{}, "--threads must be between 1 and 16"},
		{[]string{"--max-connections", "0"}, [2]string{}, "--max-connections must be between 1 and 1024"},
		{[]string{"--sql-port-max-rows", "0"}, [2]string{}, "--sql-port-max-rows must be between"},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_WORKERS", "many"}, `BINTRAIL_PG_ROUTER_WORKERS="many" is not a whole number`},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_WORKERS", "99"}, "BINTRAIL_PG_ROUTER_WORKERS must be between 1 and 16"},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_LISTEN", "10.0.0.1:5433"}, `BINTRAIL_PG_ROUTER_LISTEN "10.0.0.1:5433": the PostgreSQL port listens on this machine only`},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_MAX_WAIT", "soon"}, "max-wait"},
	} {
		clearPGRouterEnv(t)
		if tc.env[0] != "" {
			t.Setenv(tc.env[0], tc.env[1])
		}
		_, err := pgRouterSettingsFrom(pgRouterTestCmd(t, tc.args...))
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
			t.Errorf("%v %v: %v, want an error saying %q", tc.args, tc.env, err, tc.want)
		}
	}
}

func TestPGRouterStartupLines(t *testing.T) {
	st := pgRouterSettings{Listen: "127.0.0.1:5433", ServersFile: "/etc/x/servers.yaml", Workers: 2, Threads: 2, MaxConns: 16, Token: "tok"}
	out := pgRouterStartupLines(st, "2GB", "the default", nil, 3, true)
	for _, want := range []string{"127.0.0.1:5433", "dbname=<server id or name>", "PostgreSQL servers only", "clear text", "the access token is accepted too", "/etc/x/servers.yaml", "at most 16 clients", "2GB", "--workers"} {
		if !strings.Contains(out, want) {
			t.Errorf("the start-up lines do not say %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "does not exist") || strings.Contains(out, "could NOT be read") {
		t.Errorf("a warning with nothing to warn about:\n%s", out)
	}
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if !strings.HasPrefix(line, "PostgreSQL router: ") {
			t.Errorf("a line that does not say who speaks: %q", line)
		}
	}
	st.Token = ""
	out = pgRouterStartupLines(st, "2GB", "the default", errors.New("unreadable"), 0, false)
	for _, want := range []string{"does not exist yet", "could NOT be read", "unreadable"} {
		if !strings.Contains(out, want) {
			t.Errorf("the start-up lines do not say %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "access token") {
		t.Errorf("a token that was not given is mentioned:\n%s", out)
	}
}

// The command is its own: the MySQL router's flags do not list this port,
// and this command takes none of the MySQL router's routing flags.
func TestPGRouterIsACommandOfItsOwn(t *testing.T) {
	if routerCmd.Flags().Lookup("pg-listen") != nil {
		t.Error("the MySQL router has a --pg-listen flag: the PostgreSQL port is a process of its own")
	}
	for _, name := range []string{"route-read-only", "route-cost-threshold", "route-scan-rows", "concurrency", "tls-cert", "statement-table"} {
		if pgRouterCmd.Flags().Lookup(name) != nil {
			t.Errorf("pg-router takes the MySQL router's --%s", name)
		}
	}
	found := false
	for _, c := range rootCmd.Commands() {
		if c == pgRouterCmd {
			found = true
		}
	}
	if !found {
		t.Error("pg-router is not registered")
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

func TestPGRouter_login(t *testing.T) {
	f := newRouterFiles(t)
	prod, err := f.owner.Add(console.ServerEntry{Name: "prod", Flavor: console.FlavorPostgres, DSN: "u:p@tcp(127.0.0.1:1)/idx_prod"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.Add(console.ServerEntry{Name: "staging", Flavor: console.FlavorPostgres, DSN: "u:p@tcp(127.0.0.1:1)/idx_staging"}); err != nil {
		t.Fatal(err)
	}
	// A MySQL server in the same registry: this port does not serve it.
	mysqlSrv, err := f.owner.Add(console.ServerEntry{Name: "shop", DSN: "u:p@tcp(127.0.0.1:1)/idx_shop"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(mysqlSrv.ID, "shopapp", "pw-shop"); err != nil {
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
	go func() { done <- servePGPort(ctx, srv, ln, 0) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the PostgreSQL port did not stop with its context")
		}
	})
	addr := ln.Addr().String()

	const found = "" // let in: the password passed and the server was found
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
		{"anyone", "port-pw", "shop", "0A000", "a MySQL server, with the right password"},
		{"anyone", "port-pw", mysqlSrv.ID, "0A000", "a MySQL server by its id"},
		{"shopapp", "pw-shop", "prod", "0A000", "a MySQL server's application login"},
		{"anyone", "wrong", "shop", "28P01", "a wrong password says nothing about the family either"},
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

// The port as the command runs it (the follower console, the port's own
// loop), with no database: a login is let in without its server's index, and
// SET is answered by the port itself.
//
// What closes it and what does not: the MySQL port's Engine Routing switched
// on or off leaves every PostgreSQL client connected (this port routes
// nothing); a replaced password puts them out.
func TestPGRouterPort_routingSwitchKeepsClientsAReplacedPasswordDoesNot(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	pgRouterNoTails()
	f := newRouterFiles(t)
	prod, err := f.owner.Add(console.ServerEntry{Name: "prod", Flavor: console.FlavorPostgres, DSN: "u:p@tcp(127.0.0.1:1)/idx_prod"})
	if err != nil {
		t.Fatal(err)
	}
	f.saveRouting(t, "port-pw", false)
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	// No tails in this process: a PostgreSQL server, fresh by its entry's
	// default, is in fast mode here, so no tail directory is made or swept.
	if mode, from := srv.SQLModeOf(prod.ID); mode != "fast" || from != console.SQLModeFromEnvironment {
		t.Fatalf("server mode %q from %q, want fast from the environment", mode, from)
	}
	if names := srv.FreshServerNames(); len(names) != 0 {
		t.Fatalf("servers in fresh mode in this process: %v", names)
	}

	port := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 4, poll: 20 * time.Millisecond, out: io.Discard}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- port.serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the port did not stop with its context")
		}
	})
	waitUntil(t, "the port to open", func() bool { return port.listening() != "" })

	connect := func(password string) (*pgconn.PgConn, error) {
		host, portNum, _ := net.SplitHostPort(port.listening())
		cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s sslmode=disable", host, portNum))
		if err != nil {
			t.Fatal(err)
		}
		cfg.User, cfg.Password, cfg.Database = "anyone", password, "prod"
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return pgconn.ConnectConfig(c, cfg)
	}
	alive := func(conn *pgconn.PgConn) bool {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := conn.Exec(c, "SET application_name = 'x'").ReadAll()
		return err == nil
	}
	conn, err := connect("port-pw")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if !alive(conn) {
		t.Fatal("the connection does not answer")
	}

	// Engine Routing switched on, then off: the MySQL port's business.
	for _, routing := range []bool{true, false} {
		f.saveRouting(t, "port-pw", routing)
		srv.RefreshFollowedFiles()
		time.Sleep(100 * time.Millisecond)
		if !alive(conn) {
			t.Fatalf("switching Engine Routing to %v put a PostgreSQL client out", routing)
		}
	}

	// The password replaced: everyone on the port is put out, and the port
	// opens again for the new one.
	f.saveRouting(t, "port-pw-2", false)
	srv.RefreshFollowedFiles()
	waitUntil(t, "the connection made with the old password to be closed", func() bool { return !alive(conn) })
	waitUntil(t, "the port to take the new password", func() bool {
		c, err := connect("port-pw-2")
		if err != nil {
			return false
		}
		_ = c.Close(context.Background())
		return true
	})
	if c, err := connect("port-pw"); err == nil {
		_ = c.Close(context.Background())
		t.Fatal("the old password still opens the port")
	}

	// The port turned off: closed, and no password opens it.
	f.savePortFile(t, false, "port-pw-2")
	srv.RefreshFollowedFiles()
	waitUntil(t, "the port to close", func() bool { return port.listening() == "" })
}
