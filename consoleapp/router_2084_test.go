package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2084: `bintrail-console router`, the MySQL-protocol port and its routing
// as a process of its own, beside the one that captures.

// routerTestCmd is a command with the router's flags, as cobra hands it to
// runRouter.
func routerTestCmd(t *testing.T, args ...string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "router"}
	addRouterFlags(cmd)
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatal(err)
	}
	return cmd
}

func clearRouterEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"BINTRAIL_ROUTER_LISTEN", "BINTRAIL_CONSOLE_TOKEN", "BINTRAIL_CONSOLE_SERVERS", "BINTRAIL_CONSOLE_AUTH",
		"BINTRAIL_CONSOLE_MCP_TOKEN_FILE", "BINTRAIL_CONSOLE_SQL_MEMORY", "BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS",
		"BINTRAIL_CONSOLE_ROUTE_READ_ONLY", "BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD", "BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS",
		"BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", "BINTRAIL_CONSOLE_FLASHBACK_LISTEN"} {
		t.Setenv(k, "")
	}
}

// What the router starts with when nothing is said, from flags, and from
// the environment the capture daemon reads too (one .env for both).
func TestRouterSettings_flagsOverEnvironmentOverDefaults(t *testing.T) {
	clearRouterEnv(t)
	st, err := routerSettingsFrom(routerTestCmd(t))
	if err != nil {
		t.Fatal(err)
	}
	def := readrouter.DefaultPolicy()
	if st.Listen != "127.0.0.1:3310" || st.Token != "" || st.ReadOnly || st.SQLMemory != "" ||
		st.Policy != def || st.SQLPortMaxRows != console.DefaultSQLPortMaxRows || st.ServersFile != console.DefaultRegistryPath() {
		t.Errorf("defaults: %+v", st)
	}

	st, err = routerSettingsFrom(routerTestCmd(t, "--listen", "0.0.0.0:4000", "--token", "tok", "--servers-file", "/etc/x/servers.yaml",
		"--sql-memory", "4GB", "--sql-port-max-rows", "50", "--route-read-only", "--route-cost-threshold", "5", "--route-scan-rows", "0"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Listen != "0.0.0.0:4000" || st.Token != "tok" || st.ServersFile != "/etc/x/servers.yaml" || st.SQLMemory != "4096MiB" ||
		st.SQLPortMaxRows != 50 || !st.ReadOnly || st.Policy.CostThreshold != 5 || st.Policy.ScanRows != 0 {
		t.Errorf("from flags: %+v", st)
	}

	t.Setenv("BINTRAIL_ROUTER_LISTEN", "127.0.0.1:4001")
	t.Setenv("BINTRAIL_CONSOLE_TOKEN", "envtok")
	t.Setenv("BINTRAIL_CONSOLE_SERVERS", "/srv/servers.yaml")
	t.Setenv("BINTRAIL_CONSOLE_SQL_MEMORY", "1GB")
	t.Setenv("BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS", "77")
	t.Setenv("BINTRAIL_CONSOLE_ROUTE_READ_ONLY", "true")
	t.Setenv("BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD", "123.5")
	t.Setenv("BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS", "9")
	// The daemon's own port address and its maximum copy age are not the
	// router's: it has its own address, and no maximum.
	t.Setenv("BINTRAIL_CONSOLE_FLASHBACK_LISTEN", "127.0.0.1:3309")
	t.Setenv("BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", "15m")
	st, err = routerSettingsFrom(routerTestCmd(t))
	if err != nil {
		t.Fatal(err)
	}
	if st.Listen != "127.0.0.1:4001" || st.Token != "envtok" || st.ServersFile != "/srv/servers.yaml" || st.SQLMemory != "1024MiB" ||
		st.SQLPortMaxRows != 77 || !st.ReadOnly || st.Policy.CostThreshold != 123.5 || st.Policy.ScanRows != 9 {
		t.Errorf("from the environment: %+v", st)
	}
	// A flag wins over the environment.
	st, err = routerSettingsFrom(routerTestCmd(t, "--listen", "127.0.0.1:4002", "--route-read-only=false", "--route-scan-rows", "3"))
	if err != nil || st.Listen != "127.0.0.1:4002" || st.ReadOnly || st.Policy.ScanRows != 3 {
		t.Errorf("a flag over the environment: %+v, %v", st, err)
	}
}

// A setting that cannot mean what was typed stops the router at once and
// names the setting.
func TestRouterSettings_refusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"both thresholds off", []string{"--route-cost-threshold", "0", "--route-scan-rows", "0"}, nil, "no statement could ever go to the copy"},
		{"memory too small", []string{"--sql-memory", "10MB"}, nil, "--sql-memory"},
		{"rows not a count", []string{"--sql-port-max-rows", "0"}, nil, "--sql-port-max-rows"},
		{"read-only not a yes or no", nil, map[string]string{"BINTRAIL_CONSOLE_ROUTE_READ_ONLY": "maybe"}, "BINTRAIL_CONSOLE_ROUTE_READ_ONLY"},
		{"cost not a number", nil, map[string]string{"BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD": "cheap"}, "BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD"},
		{"scan rows not a number", nil, map[string]string{"BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS": "many"}, "BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS"},
		{"rows from the environment not a number", nil, map[string]string{"BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS": "lots"}, "BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS"},
		{"an address with no port", []string{"--listen", "localhost"}, nil, "--listen"},
		{"a port that is not a number", []string{"--listen", "127.0.0.1:abc"}, nil, "not a port number"},
		{"a port out of range", []string{"--listen", "127.0.0.1:70000"}, nil, "not a port number"},
		{"a cost that is not a number a plan can meet", []string{"--route-cost-threshold", "NaN", "--route-scan-rows", "0"}, nil, "--route-cost-threshold"},
		{"an infinite cost", nil, map[string]string{"BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD": "+Inf"}, "--route-cost-threshold"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearRouterEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := routerSettingsFrom(routerTestCmd(t, tc.args...))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one that names %q", err, tc.want)
			}
		})
	}
}

// The port's serving configuration: routing is always on, with no maximum
// copy age, and a busy copy refuses; a connection bound from it carries all
// three to its handler.
func TestRouterFlashbackConfig_reachesTheHandler(t *testing.T) {
	st := routerSettings{Policy: readrouter.Policy{CostThreshold: 7, ScanRows: 11}, ReadOnly: true}
	cfg := routerFlashbackConfig(st)
	if !cfg.RouteAnyCopyAge || !cfg.RouteBusyRefuses || !cfg.RouteRequired || cfg.RouteMaxCopyAge != 0 || !cfg.RouteReadOnly || cfg.RoutePolicy != st.Policy {
		t.Fatalf("config: %+v", cfg)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	tgt := console.FlashbackTarget{ID: "s1", SQL: &console.SQLOnCopy{}, ForwardDSN: "nobody:x@tcp(127.0.0.1:1)/none", SourceSSL: console.ServerEntry{}.SourceSSL()}
	h := shim.NewHandler(nil, nil)
	h.BindFreeSQL(routeTestFreeSQL{})
	if fw := bindReadRouter(h, srv, tgt, "s1", cfg.withDefaults(), slog.Default()); fw == nil {
		t.Fatal("no router was bound: with no maximum copy age the port read as routing off")
	}
	t.Cleanup(h.Close)
	got, routing := h.RouterConfig()
	if !routing || !got.AnyCopyAge || !got.BusyRefuses || !got.ReadOnly || got.MaxCopyAge != 0 {
		t.Errorf("the handler's routing: %+v (routing %v)", got, routing)
	}
	// And the port inside watch, which sets neither, binds as before.
	h2 := shim.NewHandler(nil, nil)
	h2.BindFreeSQL(routeTestFreeSQL{})
	bindReadRouter(h2, srv, tgt, "s1", flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: st.Policy}.withDefaults(), slog.Default())
	t.Cleanup(h2.Close)
	if got, _ := h2.RouterConfig(); got.AnyCopyAge || got.BusyRefuses || got.MaxCopyAge != time.Hour {
		t.Errorf("watch's port: %+v", got)
	}
}

// routerFiles is a config directory as the daemon keeps it: its registry,
// which the test changes as the daemon would, and the paths beside it.
type routerFiles struct {
	owner    *console.Registry
	servers  string
	portFile string
}

func newRouterFiles(t *testing.T) routerFiles {
	t.Helper()
	dir := t.TempDir()
	servers := filepath.Join(dir, "console-servers.yaml")
	owner, err := console.LoadRegistry(servers)
	if err != nil {
		t.Fatal(err)
	}
	return routerFiles{owner: owner, servers: servers, portFile: filepath.Join(dir, console.FlashbackFileName)}
}

func (f routerFiles) settings(token string) routerSettings {
	return routerSettings{Listen: "127.0.0.1:0", Token: token, ServersFile: f.servers,
		AuthFile: filepath.Join(filepath.Dir(f.servers), "console-auth.yaml"), MCPTokenFile: filepath.Join(filepath.Dir(f.servers), "console-mcp-token.yaml"),
		Policy: readrouter.DefaultPolicy(), SQLPortMaxRows: console.DefaultSQLPortMaxRows}
}

// savePortFile writes the port's saved setting as the daemon's web interface
// does.
func (f routerFiles) savePortFile(t *testing.T, enabled bool, password string) {
	t.Helper()
	body := "version: 1\nenabled: false\n"
	if enabled {
		body = "version: 1\nenabled: true\n"
	}
	body += "listen: 127.0.0.1:3309\npassword: " + password + "\n"
	tmp := f.portFile + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, f.portFile); err != nil {
		t.Fatal(err)
	}
}

// The console the router builds reads the daemon's files and writes none of
// them, sees a server the daemon adds, sizes its pool from the machine and
// lets a server's statements run at once.
func TestNewRouterConsole_readsTheDaemonsFilesAndWritesNone(t *testing.T) {
	f := newRouterFiles(t)
	if _, err := f.owner.Add(console.ServerEntry{Name: "prod", DSN: "u:p@tcp(127.0.0.1:1)/idx", SourceDSN: "repl:pw@tcp(127.0.0.1:1)/app"}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(f.servers)
	if err != nil {
		t.Fatal(err)
	}
	pool := console.SQLPool{Workers: 5, Threads: 3, Why: "test"}
	srv, runner, err := newRouterConsole(f.settings("tok"), pool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	if runner.MaxInFlight() != 5 || runner.WorkerStatements() != sqlsandbox.DefaultWorkerStatements {
		t.Errorf("the pool: %d at once, %d statements a worker", runner.MaxInFlight(), runner.WorkerStatements())
	}
	// The pool is the limit: a server's statements are not kept to one at
	// a time, as they are on the port inside watch.
	if !srv.SQLPortSharedSlots() {
		t.Error("the router keeps a server's statements on the copy to one at a time")
	}
	// A server that is there resolves although nothing listens on its
	// index address: the router serves it without the index. One that is
	// not there is unknown.
	if tgt, err := srv.ResolveFlashback(context.Background(), "prod"); err != nil || tgt.ForwardDSN == "" {
		t.Errorf("a server in the file whose index is away: %v (forward DSN %q)", err, tgt.ForwardDSN)
	}
	if _, err := srv.ResolveFlashback(context.Background(), "staging"); !errors.Is(err, console.ErrUnknownServer) {
		t.Errorf("a server not in the file: %v", err)
	}
	if _, err := f.owner.Add(console.ServerEntry{Name: "staging", DSN: "u:p@tcp(127.0.0.1:1)/idx2"}); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	if _, err := srv.ResolveFlashback(context.Background(), "staging"); err != nil {
		t.Errorf("a server the daemon added: %v, want it known", err)
	}
	// Nothing was written: the file is byte for byte what the daemon last
	// saved, and no file appeared beside it.
	saved, err := os.ReadFile(f.servers)
	if err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	if _, err := srv.ResolveFlashback(context.Background(), "prod"); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(f.servers)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, saved) || bytes.Equal(saved, before) {
		t.Errorf("the servers file after the router read it differs from what the daemon saved:\n%s", after)
	}
	entries, err := os.ReadDir(filepath.Dir(f.servers))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "console-servers.yaml" {
			t.Errorf("the router left a file in the daemon's directory: %s", e.Name())
		}
	}
}

// startRouter runs serveRouter and returns the address it listens on once it
// does (nil while it waits for a credential), and a stop that waits for it.
func startRouter(t *testing.T, srv *console.Server, addr string) (bound func() string, stop func()) {
	bound, stop, _ = startRouterSaying(t, srv, addr)
	return bound, stop
}

// startRouterSaying is startRouter with what the router printed so far.
func startRouterSaying(t *testing.T, srv *console.Server, addr string) (bound func() string, stop func(), said func() string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go srv.FollowFiles(ctx, 20*time.Millisecond)
	out := &lockedBuffer{}
	rt := &routerPort{srv: srv, addr: addr, cfg: routerFlashbackConfig(routerSettings{Policy: readrouter.DefaultPolicy()}), poll: 20 * time.Millisecond, out: out}
	done := make(chan error, 1)
	go func() { done <- rt.serve(ctx) }()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("the router ended with %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("the router did not stop")
		}
	}
	t.Cleanup(stop)
	return rt.listening, stop, out.String
}

// lockedBuffer is a buffer the router writes to from its goroutine while
// the test reads it.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// With no token and no password yet the router waits, without listening,
// and opens its port when the daemon's web interface creates the password.
// When that password is withdrawn, the connections made with it are closed
// and the port closes again.
func TestRouterPort_waitsForACredentialAndFollowsIt(t *testing.T) {
	f := newRouterFiles(t)
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	addr := freeAddr(t)
	bound, _, said := startRouterSaying(t, srv, addr)
	// It says once that it waits, and why, and does not bind meanwhile.
	waitUntil(t, "the router to say it waits for a credential", func() bool { return strings.Contains(said(), "not listening yet") })
	for range 10 {
		if bound() != "" || accepts(addr) {
			t.Fatal("the router listens with no credential that could open it")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := strings.Count(said(), "not listening yet"); n != 1 {
		t.Errorf("the router said it waits %d times", n)
	}
	f.savePortFile(t, true, "made-in-the-web")
	waitUntil(t, "the port to open once a password exists", func() bool { return bound() != "" })
	if code := mysqlLogin(t, addr, "made-in-the-web"); code != 0 {
		t.Fatalf("login with the web password: MySQL error %d", code)
	}
	if code := mysqlLogin(t, addr, "something-else"); code != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("login with a wrong password: %d, want 1045", code)
	}
	conn, err := client.Connect(addr, "default", "made-in-the-web", "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The port turned off in the web interface: that password opens
	// nothing any more, and the connection made with it does not stay.
	f.savePortFile(t, false, "made-in-the-web")
	waitUntil(t, "the connection made with the withdrawn password to be closed", func() bool { return conn.Ping() != nil })
	waitUntil(t, "the port to close with no credential left", func() bool { return bound() == "" && !accepts(addr) })
	// Turned on again: it opens again, on the same address.
	f.savePortFile(t, true, "made-in-the-web")
	waitUntil(t, "the port to open again", func() bool { return bound() != "" })
	if code := mysqlLogin(t, addr, "made-in-the-web"); code != 0 {
		t.Fatalf("login after the port was turned on again: MySQL error %d", code)
	}
}

// With a token, the token decides, as on a capture process started with an
// address for its port: the password in the web interface's saved setting
// is not accepted, whatever that file says, and nothing done to the file
// closes a connection.
func TestRouterPort_withATokenOnlyTheTokenOpensIt(t *testing.T) {
	f := newRouterFiles(t)
	f.savePortFile(t, true, "made-in-the-web")
	st := f.settings("tok")
	if got := routerPortFile(st); got != "" {
		t.Fatalf("with a token the router follows %s", got)
	}
	if got := routerPortFile(f.settings("")); got != f.portFile {
		t.Fatalf("with no token the router follows %q, want %s", got, f.portFile)
	}
	srv, runner, err := newRouterConsole(st, console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	addr := freeAddr(t)
	bound, stop := startRouter(t, srv, addr)
	waitUntil(t, "the port to open", func() bool { return bound() != "" })
	if code := mysqlLogin(t, addr, "tok"); code != 0 {
		t.Fatalf("login with the token: MySQL error %d", code)
	}
	if code := mysqlLogin(t, addr, "made-in-the-web"); code != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("login with the web interface's password on a router that has a token: %d, want 1045", code)
	}
	conn, err := client.Connect(addr, "default", "tok", "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// An unknown server is told so on its first statement, as on the
	// daemon's port, and a PING says the same: a pool that checks its
	// connections with one must not keep this one as healthy.
	_, err = conn.Execute("SELECT 1")
	var me *gomysql.MyError
	if !errors.As(err, &me) || me.Code != gomysql.ER_BAD_DB_ERROR {
		t.Fatalf("a statement for a server that does not exist: %v", err)
	}
	if err := conn.Ping(); !errors.As(err, &me) || me.Code != gomysql.ER_BAD_DB_ERROR {
		t.Fatalf("a PING on a connection for a server that does not exist: %v", err)
	}
	f.savePortFile(t, false, "made-in-the-web")
	if err := os.Remove(f.portFile); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := conn.Execute("SELECT 1"); !errors.As(err, &me) || me.Code != gomysql.ER_BAD_DB_ERROR {
		t.Fatalf("the connection after the web interface's setting was removed: %v, want it still open", err)
	}
	stop()
	if accepts(addr) {
		t.Error("the port is still open after the router stopped")
	}
}

// mysqlLoginQuiet is mysqlLogin for a polling loop: any failure is a code.
func mysqlLoginQuiet(addr, password string) uint16 {
	conn, err := client.Connect(addr, "default", password, "")
	if err != nil {
		var me *gomysql.MyError
		if errors.As(err, &me) {
			return me.Code
		}
		return 1
	}
	conn.Close()
	return 0
}

// An address that cannot be bound when the router starts ends it with the
// reason: a router that is not listening must not look like one that is.
func TestRouterPort_anAddressTakenAtStartIsAnError(t *testing.T) {
	f := newRouterFiles(t)
	srv, runner, err := newRouterConsole(f.settings("tok"), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	taken := newControl(t)
	addr := freeAddr(t)
	if err := taken.Apply(addr); err != nil {
		t.Fatal(err)
	}
	rt := &routerPort{srv: srv, addr: addr, cfg: routerFlashbackConfig(routerSettings{Policy: readrouter.DefaultPolicy()}), poll: 20 * time.Millisecond, out: &bytes.Buffer{}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rt.serve(ctx); err == nil || !strings.Contains(err.Error(), addr) {
		t.Fatalf("serve on a taken address: %v, want an error that names it", err)
	}
}

// What the router prints when it starts says where the port will be, which
// files it serves from, the pool it worked out, and how reads are routed, in
// whole sentences. The cases are the ones that are silent otherwise.
func TestRouterStartupLines_sayWhatWillHappen(t *testing.T) {
	base := routerStartup{Listen: "127.0.0.1:3310", Pool: console.SQLPool{Workers: 4, Threads: 2, Why: "8 cores allow 4"}, MemoryLimit: "2048MiB",
		Cfg: routerFlashbackConfig(routerSettings{Policy: readrouter.DefaultPolicy()}), ServersFile: "/var/lib/bintrail/console-servers.yaml", ServersExists: true, Servers: 3}
	for _, tc := range []struct {
		name    string
		edit    func(u *routerStartup)
		want    []string
		wantNot []string
	}{
		{"as started by default with a token", nil,
			[]string{"127.0.0.1:3310", "the password is the access token", "3 servers from /var/lib/bintrail/console-servers.yaml", "4 statements on the copy at once", "2 threads", "8 cores allow 4",
				"whatever the age of its snapshot", "waits up to 30s", "at most 16 waiting at once", "error 1040", "read routing is read-write"},
			[]string{"every network interface", "does not exist", "is not applied", "read routing is read-only"}},
		{"no token: the web interface's password", func(u *routerStartup) { u.PortFile = "/var/lib/bintrail/console-mysql-port.yaml" },
			[]string{"no access token was given", "/var/lib/bintrail/console-mysql-port.yaml", "only while it is on"},
			[]string{"the password is the access token"}},
		{"a servers file that is not there", func(u *routerStartup) { u.ServersExists, u.Servers = false, 0 },
			[]string{"does not exist yet", "no server can be reached", "--servers-file"},
			[]string{"0 servers"}},
		{"one server", func(u *routerStartup) { u.Servers = 1 }, []string{"1 server from"}, []string{"1 servers"}},
		{"every interface", func(u *routerStartup) { u.Listen = ":3310" }, []string{"every network interface", "Bind 127.0.0.1"}, nil},
		{"every interface, spelled out", func(u *routerStartup) { u.Listen = "0.0.0.0:3310" }, []string{"every network interface"}, nil},
		{"read-only", func(u *routerStartup) { u.Cfg.RouteReadOnly = true }, []string{"read routing is read-only (--route-read-only)", "refused and never sent to the source"}, []string{"read routing is read-write"}},
		{"settings of the capture process that do not apply", func(u *routerStartup) { u.Ignored = []string{"X=1 is not applied: because"} },
			[]string{"Router: X=1 is not applied: because."}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := base
			if tc.edit != nil {
				tc.edit(&u)
			}
			got := routerStartupLines(u)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("the start-up lines do not say %q:\n%s", want, got)
				}
			}
			for _, not := range tc.wantNot {
				if strings.Contains(got, not) {
					t.Errorf("the start-up lines say %q:\n%s", not, got)
				}
			}
			if strings.Contains(got, "—") || strings.Contains(strings.ToLower(got), "daemon") {
				t.Errorf("wording:\n%s", got)
			}
			for _, line := range strings.Split(strings.TrimSuffix(got, "\n"), "\n") {
				if !strings.HasPrefix(line, "Router: ") || !strings.HasSuffix(line, ".") {
					t.Errorf("not a sentence of the router's: %q", line)
				}
			}
		})
	}
}

// A setting of the capture process that is in the environment and does not
// apply to the router is said, with why; one that is not set is not.
func TestRouterIgnoredEnv_namesWhatDoesNotApply(t *testing.T) {
	clearRouterEnv(t)
	t.Setenv("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT", "")
	if got := routerIgnoredEnv(); len(got) != 0 {
		t.Fatalf("with neither set: %v", got)
	}
	t.Setenv("BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", "15m")
	t.Setenv("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT", "8")
	got := strings.Join(routerIgnoredEnv(), "\n")
	for _, want := range []string{"BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE=15m is not applied", "no maximum copy age", "BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT=8 is not applied", "from its cores and memory"} {
		if !strings.Contains(got, want) {
			t.Errorf("does not say %q:\n%s", want, got)
		}
	}
}

// An address that cannot be bound is said when the router starts, also when
// it has no credential yet and would only bind later.
func TestRouterPort_anAddressTakenIsSaidBeforeWaitingForACredential(t *testing.T) {
	f := newRouterFiles(t)
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	taken := newControl(t)
	addr := freeAddr(t)
	if err := taken.Apply(addr); err != nil {
		t.Fatal(err)
	}
	rt := &routerPort{srv: srv, addr: addr, cfg: routerFlashbackConfig(routerSettings{Policy: readrouter.DefaultPolicy()}), poll: 20 * time.Millisecond, out: &bytes.Buffer{}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rt.serve(ctx); err == nil || !strings.Contains(err.Error(), addr) {
		t.Fatalf("serve with no credential on a taken address: %v, want an error that names it", err)
	}
}

// Why a server cannot route is handed back to the caller, in the words the
// Connect page shows, and is empty for one that can and for a port with
// routing off.
func TestBindReadRouterWhy_handsBackTheReason(t *testing.T) {
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	ok := console.FlashbackTarget{ID: "s1", SQL: &console.SQLOnCopy{}, ForwardDSN: "nobody:x@tcp(127.0.0.1:1)/none", SourceSSL: console.ServerEntry{}.SourceSSL()}
	noSource, noSQL, badDSN := ok, ok, ok
	noSource.ForwardDSN = ""
	noSQL.SQL, noSQL.SQLUnavailable = nil, "archive access is disabled for this server"
	badDSN.ForwardDSN = "nobody:x@unix(/tmp/x.sock)/none?tls=maybe"
	router := routerFlashbackConfig(routerSettings{Policy: readrouter.DefaultPolicy()}).withDefaults()
	for _, tc := range []struct {
		name  string
		tgt   console.FlashbackTarget
		cfg   flashbackConfig
		bound bool
		why   string
	}{
		{"a server that routes", ok, router, true, ""},
		{"no source to forward to", noSource, router, false, "no source database"},
		{"no SQL on the copy", noSQL, router, false, "archive access is disabled"},
		{"an address that cannot be forwarded to", badDSN, router, false, "cannot"},
		{"routing off: nothing to say", noSource, flashbackConfig{}.withDefaults(), false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := shim.NewHandler(nil, nil)
			h.BindFreeSQL(routeTestFreeSQL{})
			t.Cleanup(h.Close)
			fw, why := bindReadRouterWhy(h, srv, tc.tgt, "s1", tc.cfg, slog.Default())
			if (fw != nil) != tc.bound || (why == "") != (tc.why == "") || !strings.Contains(why, tc.why) {
				t.Errorf("bound = %v, why = %q; want bound %v and a reason with %q", fw != nil, why, tc.bound, tc.why)
			}
		})
	}
}
