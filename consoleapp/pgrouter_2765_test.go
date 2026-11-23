package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
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
		"BINTRAIL_PG_ROUTER_TLS_CERT", "BINTRAIL_PG_ROUTER_TLS_KEY", "BINTRAIL_PG_ROUTER_COST_THRESHOLD", "BINTRAIL_PG_ROUTER_SCAN_ROWS",
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
		// Any host since #2766: the password never crosses the network.
		{"0.0.0.0:5433", true, ""},
		{":5433", true, ""},
		{"[::]:5433", true, ""},
		{"10.0.0.5:5433", true, ""},
		{"db.example.com:5433", true, ""},
		{"127.0.0.1", false, "not an address"},
		{"127.0.0.1:0", false, "not a port number"},
		{"127.0.0.1:70000", false, "not a port number"},
		{"127.0.0.1:pg", false, "not a port number"},
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
	if st.Listen != "127.0.0.1:5433" || st.Workers != 4 || st.Threads != 2 || st.MaxConns != 256 {
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
		{[]string{"--listen", "5433"}, [2]string{}, "not an address"},
		{[]string{"--workers", "0"}, [2]string{}, "--workers must be between 1 and 16"},
		{[]string{"--workers", "17"}, [2]string{}, "--workers must be between 1 and 16"},
		{[]string{"--threads", "-1"}, [2]string{}, "--threads must be between 1 and 16"},
		{[]string{"--max-connections", "0"}, [2]string{}, "--max-connections must be between 1 and 1024"},
		{[]string{"--sql-port-max-rows", "0"}, [2]string{}, "--sql-port-max-rows must be between"},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_WORKERS", "many"}, `BINTRAIL_PG_ROUTER_WORKERS="many" is not a whole number`},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_WORKERS", "99"}, "BINTRAIL_PG_ROUTER_WORKERS must be between 1 and 16"},
		{nil, [2]string{"BINTRAIL_PG_ROUTER_LISTEN", "10.0.0.1"}, `BINTRAIL_PG_ROUTER_LISTEN "10.0.0.1" is not an address`},
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
	for _, want := range []string{"127.0.0.1:5433", "dbname=<server id or name>", "PostgreSQL servers only", "SCRAM-SHA-256", "made up at start", "answered in clear text", "the access token is accepted too", "/etc/x/servers.yaml", "at most 16 clients", "2GB", "--workers"} {
		if !strings.Contains(out, want) {
			t.Errorf("the start-up lines do not say %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "does not exist") || strings.Contains(out, "could NOT be read") || strings.Contains(out, "other machines") {
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
	for _, name := range []string{"route-read-only", "route-cost-threshold", "route-scan-rows", "concurrency", "statement-table"} {
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
		{"app1", "pw-one", "nowhere", found, "the application login with a database that is no server: its own server"},
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

// The port as the command runs it (the follower console, the port's own
// loop), with no database: a login is let in without its server's index, and
// SET is answered by the port itself.
//
// What closes it and what does not: the MySQL port's Engine Routing switched
// on or off leaves every PostgreSQL client connected (this port routes
// nothing); a replaced password puts them out.
func TestPGRouterPort_routingSwitchKeepsClientsAReplacedPasswordDoesNot(t *testing.T) {
	// Fast mode for every server: what is tested here is the copy as of
	// its snapshot, not the tails laid over it.
	t.Setenv("BINTRAIL_SQL_TAILS", "off")
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
	// BINTRAIL_SQL_TAILS=off: a PostgreSQL server, fresh by its entry's
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

// A servers file that holds both families (a hand edit: the registry takes
// one, #2815) still has its PostgreSQL servers served here, and its MySQL ones
// refused by name, after the password and never before it.
func TestPGRouter_refusesTheMySQLServersOfAMixedFile(t *testing.T) {
	f := newRouterFiles(t)
	body := `version: 1
servers:
  - id: aaaa000000000001
    name: shop
    index_dsn: u:p@tcp(127.0.0.1:1)/idx_shop
  - id: aaaa000000000002
    name: ledger
    flavor: postgres
    index_dsn: u:p@tcp(127.0.0.1:1)/idx_ledger
  - id: aaaa000000000003
    name: wiki
    flavor: mariadb
    index_dsn: u:p@tcp(127.0.0.1:1)/idx_wiki
`
	if err := os.WriteFile(f.servers, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	f.savePortFile(t, true, "port-pw")
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatalf("a mixed servers file stopped the router's console: %v", err)
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
	for _, tc := range []struct{ pw, db, want, why string }{
		{"port-pw", "ledger", "", "the PostgreSQL server of a mixed file is served"},
		{"port-pw", "aaaa000000000002", "", "by its id too"},
		{"port-pw", "shop", "0A000", "a MySQL server, with the right password"},
		{"port-pw", "aaaa000000000001", "0A000", "a MySQL server by its id"},
		{"port-pw", "wiki", "0A000", "a MariaDB server"},
		{"wrong", "shop", "28P01", "a wrong password says nothing about the family"},
		{"wrong", "ledger", "28P01", "nor about a PostgreSQL server"},
	} {
		if got := pgLoginCode(t, addr, "anyone", tc.pw, tc.db); got != tc.want {
			t.Errorf("%s (%s / %s): SQLSTATE %q, want %q", tc.why, tc.pw, tc.db, got, tc.want)
		}
	}
}

// ProxySQL's monitor logs into the PostgreSQL port with the monitor login
// saved for the port (the MySQL port's too): its password only, whatever
// database it names, and a connection that answers its checks and opens no
// server.
func TestPGRouter_monitorLogin(t *testing.T) {
	f := newRouterFiles(t)
	if _, err := f.owner.Add(console.ServerEntry{Name: "prod", Flavor: console.FlavorPostgres, DSN: "u:p@tcp(127.0.0.1:1)/idx_prod"}); err != nil {
		t.Fatal(err)
	}
	body := "version: 1\nenabled: true\npassword: port-pw\nmonitor_user: monitor\nmonitor_password: monitor-pw\n"
	if err := os.WriteFile(f.portFile, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
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
		<-done
	})
	addr := ln.Addr().String()
	for _, tc := range []struct{ user, pw, db, want, why string }{
		{"monitor", "monitor-pw", "postgres", "", "ProxySQL's monitor, with its database"},
		{"monitor", "monitor-pw", "prod", "", "any database"},
		{"monitor", "port-pw", "prod", "28P01", "the monitor login with the port's password"},
		{"monitor", "wrong", "postgres", "28P01", "a wrong password"},
		{"Monitor", "monitor-pw", "postgres", "28P01", "the username in another case"},
		{"anyone", "monitor-pw", "prod", "28P01", "the monitor's password under another username"},
		{"anyone", "port-pw", "prod", "", "the port's own password, as before"},
	} {
		if got := pgLoginCode(t, addr, tc.user, tc.pw, tc.db); got != tc.want {
			t.Errorf("%s (%s/%s/%s): %q, want %q", tc.why, tc.user, tc.pw, tc.db, got, tc.want)
		}
	}
	host, port, _ := net.SplitHostPort(addr)
	conn, err := pgconn.Connect(context.Background(), fmt.Sprintf("host=%s port=%s user=monitor password=monitor-pw dbname=postgres sslmode=disable", host, port))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	res := conn.ExecParams(context.Background(), "SELECT pg_is_in_recovery()", nil, nil, nil, nil).Read()
	if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "t" {
		t.Errorf("pg_is_in_recovery(): %v %v", res.Rows, res.Err)
	}
	if res := conn.ExecParams(context.Background(), "SELECT 1", nil, nil, nil, nil).Read(); res.Err == nil {
		t.Error("the monitor login answered SELECT 1")
	}
}

// An application login that names another server as its database is refused
// (#3087): it used to be connected to its own server with nothing said, so
// someone who wrote dbname=staging with the login of prod read prod.
func TestPGRouter_loginNamesAnotherServer(t *testing.T) {
	f := newRouterFiles(t)
	prod, err := f.owner.Add(console.ServerEntry{Name: "prod", Flavor: console.FlavorPostgres, DSN: "u:p@tcp(127.0.0.1:1)/idx_prod"})
	if err != nil {
		t.Fatal(err)
	}
	staging, err := f.owner.Add(console.ServerEntry{Name: "staging", Flavor: console.FlavorPostgres, DSN: "u:p@tcp(127.0.0.1:1)/idx_staging"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(prod.ID, "app1", "pw-one"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(staging.ID, "app2", "pw-two"); err != nil {
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

	const let, other = "", "28000"
	for _, tc := range []struct {
		user, pw, db, want, why string
	}{
		// Another server, by its name and by its id, with either password
		// the login takes.
		{"app1", "pw-one", "staging", other, "another server's name"},
		{"app1", "pw-one", staging.ID, other, "another server's id"},
		{"app1", "port-pw", "staging", other, "another server's name, with the port's password"},
		{"app2", "pw-two", "prod", other, "the other login, the other way round"},
		{"app2", "pw-two", prod.ID, other, "the other login, by id"},
		// Its own server, however it is named.
		{"app1", "pw-one", "prod", let, "its own server's name"},
		{"app1", "pw-one", prod.ID, let, "its own server's id"},
		{"app2", "pw-two", "staging", let, "the other login's own server"},
		// A name that is no server: left as it was, the login's own server.
		// A proxy passes on the database its client named.
		{"app1", "pw-one", "shop", let, "a name that is no server"},
		{"app1", "pw-one", "postgres", let, "postgres"},
		{"app1", "pw-one", "", let, "no database: the client sends the user's name"},
		{"app1", "pw-one", "app1", let, "the login's own name"},
		{"app1", "pw-one", "STAGING", let, "another server's name in another case is no server: names are matched as written"},
		{"app1", "pw-one", "staging ", let, "another server's name with a blank after it is no server"},
		{"app1", "pw-one", " staging", let, "or before it"},
		{"app1", "pw-one", "stagin", let, "a part of another server's name"},
		{"app1", "pw-one", "staging/prod", let, "two names in one"},
		// The password comes first: nothing is said about servers without it.
		{"app1", "wrong", "staging", "28P01", "a wrong password, naming another server"},
		{"app1", "", "staging", "28P01", "no password, naming another server"},
		{"app1", "pw-two", "staging", "28P01", "the other login's password"},
		// Without an application login the database name picks the server,
		// as before.
		{"anyone", "port-pw", "staging", let, "no application login: the name picks the server"},
		{"anyone", "port-pw", "prod", let, "no application login: the other one"},
	} {
		if got := pgLoginCode(t, addr, tc.user, tc.pw, tc.db); got != tc.want {
			t.Errorf("%s (%s / %s / %q): SQLSTATE %q, want %q", tc.why, tc.user, tc.pw, tc.db, got, tc.want)
		}
	}

	// The error names the login's own server and the one that was named.
	refusal := pgLoginOtherServer(srv, "app1", "staging", prod.ID)
	if refusal == nil {
		t.Fatal("no refusal")
	}
	for _, want := range []string{`user "app1"`, `application login of server "prod"`, `database "staging" is another server ("staging")`, "connect with dbname=prod"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("the refusal does not say %q: %s", want, refusal.Message)
		}
	}
	// Named by id, the other server is still said by its name.
	if r := pgLoginOtherServer(srv, "app1", staging.ID, prod.ID); r == nil || !strings.Contains(r.Message, `("staging")`) {
		t.Errorf("another server named by its id: %+v", r)
	}
	// No application login, the own server, and no server: nothing refused.
	for _, db := range []string{"prod", prod.ID, "shop", ""} {
		if r := pgLoginOtherServer(srv, "app1", db, prod.ID); r != nil {
			t.Errorf("database %q with prod's login: refused: %s", db, r.Message)
		}
	}
	if r := pgLoginOtherServer(srv, "anyone", "staging", ""); r != nil {
		t.Errorf("no application login: refused: %s", r.Message)
	}
}
