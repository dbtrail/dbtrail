package consoleapp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// routerCmd is the MySQL-protocol port and its read routing as a process of
// its own (#2084). Inside watch the port shares a process with capture: a
// daemon restart drops every routed connection, and the statements that go
// to the copy are kept to two at a time so they cannot starve capture. Here
// the same port, with the same routing rules, runs beside the daemon with
// ceilings of its own, so its pool of workers is as large as what it was
// given allows.
//
// It asks the capture process nothing. It reads the files the daemon keeps
// (the servers and the port's password), never writes them, and opens its own
// connections to each server's index and source.
var routerCmd = &cobra.Command{
	Use:   "router",
	Short: "Serve the MySQL-protocol port with read routing, as a service of its own beside the one that captures",
	Long: `Starts a MySQL-protocol port that applications connect to in place of their
MySQL server. Each statement goes to the server's source MySQL, except SELECTs
whose plan says they are expensive, which run on the copy.

It runs beside 'bintrail-console watch', not inside it: restarting capture does
not drop its connections, and a statement that runs out of memory here cannot
take capture with it. It reads the servers the web interface
manages and never writes them. Connect with the server's id or name as the
user, and the access token (or the port password created in the web interface)
as the password.

How many statements run on the copy at once, and with how many threads each,
is worked out from the cores and the memory this process may use: give the
service its own ceilings (cpus and mem_limit in Docker, CPUQuota and MemoryMax
under systemd) and it uses all of them; without them it takes half of the
machine, since capture uses the machine too. When every worker is busy a
statement waits, and past the wait the client gets error 1040.

Example:
  BINTRAIL_CONSOLE_TOKEN=... bintrail-console router --listen 127.0.0.1:3310`,
	Args: cobra.NoArgs,
	RunE: runRouter,
}

// defaultRouterListen is the router's own port: the one inside watch is
// offered on 3309, and both may run on one host.
const defaultRouterListen = "127.0.0.1:3310"

func init() {
	addRouterFlags(routerCmd)
	rootCmd.AddCommand(routerCmd)
}

func addRouterFlags(cmd *cobra.Command) {
	def := readrouter.DefaultPolicy()
	f := cmd.Flags()
	f.String("listen", defaultRouterListen, "Address the MySQL-protocol port listens on (host:port). Env BINTRAIL_ROUTER_LISTEN.")
	f.String("metrics-addr", "", "Address to expose Prometheus metrics on (e.g. 127.0.0.1:9091); empty = none. Not the capture process's address: each process serves its own. Env BINTRAIL_ROUTER_METRICS_ADDR.")
	f.String("token", "", "Access token: the password every client of the port uses. With one, only the token opens the port. Without one, the port opens when the MySQL port is turned on in the web interface, with the password created there, and closes when it is turned off. Env BINTRAIL_CONSOLE_TOKEN.")
	f.String("servers-file", "", "Path to the server registry the web interface manages (default ~/.config/bintrail/console-servers.yaml). Read, never written. Env BINTRAIL_CONSOLE_SERVERS.")
	f.String("auth-file", "", "Path to the web interface auth file, when it is not the default: only so a statement cannot read it through a copy directory that contains it. Env BINTRAIL_CONSOLE_AUTH.")
	f.String("mcp-token-file", "", "Path to the managed MCP token file, when it is not the default: for the same reason as --auth-file. Env BINTRAIL_CONSOLE_MCP_TOKEN_FILE.")
	f.String("sql-memory", "", "Memory each statement on the copy may use, e.g. 4GB; default 2GB, at least 512MB. Fewer statements run at once when each may use more. The memory saved in the web interface (Settings, MCP Server) is the capture process's and is not read here. Env BINTRAIL_CONSOLE_SQL_MEMORY.")
	f.Int("sql-port-max-rows", console.DefaultSQLPortMaxRows, "Most rows one statement returns; a result with more is refused with error 1104, not cut. Env BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS.")
	f.Bool("route-read-only", false, "Refuse every statement that is not a read, and never send it to the source. It reads the statement's text, so it cannot see a stored function that writes: the source account's grants are the guard for that. Env BINTRAIL_CONSOLE_ROUTE_READ_ONLY.")
	f.Float64("route-cost-threshold", def.CostThreshold, "A SELECT whose plan query_cost is at least this goes to the copy (a point lookup costs about 1; a full scan over 200k rows about 20000). 0 disables the cost rule. Env BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD.")
	f.Int64("route-scan-rows", def.ScanRows, "A SELECT whose plan has a full table scan over at least this many rows goes to the copy (on a MariaDB source, whose plans carry no comparable cost, a full index scan too, and a plan whose joins read at least this many rows in all). 0 disables the scan rule. Env BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS.")
}

// routerSettings is what the router starts with, flags and environment
// resolved.
type routerSettings struct {
	Listen         string
	MetricsAddr    string
	Token          string
	ServersFile    string
	AuthFile       string
	MCPTokenFile   string
	SQLMemory      string // "" = the default; else "<n>MiB"
	SQLPortMaxRows int
	Policy         readrouter.Policy
	ReadOnly       bool
}

// routerSettingsFrom resolves the router's flags with the environment, a
// flag winning over a variable, and refuses what cannot mean what was
// typed. Most variables are the capture daemon's own, so one environment
// file serves both; the address is the router's.
func routerSettingsFrom(cmd *cobra.Command) (routerSettings, error) {
	f := cmd.Flags()
	str := func(flag, env string) string {
		v, _ := f.GetString(flag)
		if !f.Changed(flag) {
			if e := os.Getenv(env); e != "" {
				v = e
			}
		}
		return v
	}
	st := routerSettings{
		Listen:       str("listen", "BINTRAIL_ROUTER_LISTEN"),
		MetricsAddr:  str("metrics-addr", "BINTRAIL_ROUTER_METRICS_ADDR"),
		Token:        str("token", "BINTRAIL_CONSOLE_TOKEN"),
		ServersFile:  str("servers-file", "BINTRAIL_CONSOLE_SERVERS"),
		AuthFile:     str("auth-file", "BINTRAIL_CONSOLE_AUTH"),
		MCPTokenFile: str("mcp-token-file", "BINTRAIL_CONSOLE_MCP_TOKEN_FILE"),
	}
	if st.ServersFile == "" {
		st.ServersFile = console.DefaultRegistryPath()
	}
	if _, port, err := net.SplitHostPort(st.Listen); err != nil {
		return st, fmt.Errorf("--listen %q is not an address: want host:port, e.g. %s", st.Listen, defaultRouterListen)
	} else if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return st, fmt.Errorf("--listen %q: %q is not a port number (1 to 65535)", st.Listen, port)
	}
	var err error
	if st.SQLMemory, err = sqlMemoryFrom(cmd); err != nil {
		return st, err
	}

	st.SQLPortMaxRows, _ = f.GetInt("sql-port-max-rows")
	rowsName := "--sql-port-max-rows"
	if !f.Changed("sql-port-max-rows") {
		if v := os.Getenv("BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS"); v != "" {
			rowsName = "BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS"
			if st.SQLPortMaxRows, err = strconv.Atoi(v); err != nil {
				return st, fmt.Errorf("%s=%q is not a number of rows", rowsName, v)
			}
		}
	}
	if st.SQLPortMaxRows < 1 {
		return st, fmt.Errorf("%s must be at least 1: it is the most rows one statement returns", rowsName)
	}

	st.ReadOnly, _ = f.GetBool("route-read-only")
	if !f.Changed("route-read-only") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ROUTE_READ_ONLY"); v != "" {
			if st.ReadOnly, err = strconv.ParseBool(v); err != nil {
				return st, fmt.Errorf("BINTRAIL_CONSOLE_ROUTE_READ_ONLY=%q is not a yes or no: use 1 or true to make the port read-only, 0 or false (or leave it unset) for read-write", v)
			}
		}
	}
	st.Policy.CostThreshold, _ = f.GetFloat64("route-cost-threshold")
	if !f.Changed("route-cost-threshold") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD"); v != "" {
			if st.Policy.CostThreshold, err = strconv.ParseFloat(v, 64); err != nil {
				return st, fmt.Errorf("BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD: %w", err)
			}
		}
	}
	st.Policy.ScanRows, _ = f.GetInt64("route-scan-rows")
	if !f.Changed("route-scan-rows") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS"); v != "" {
			if st.Policy.ScanRows, err = strconv.ParseInt(v, 10, 64); err != nil {
				return st, fmt.Errorf("BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS: %w", err)
			}
		}
	}
	if math.IsNaN(st.Policy.CostThreshold) || math.IsInf(st.Policy.CostThreshold, 0) {
		return st, fmt.Errorf("--route-cost-threshold must be a number, and %v is not one a plan's cost can be compared with", st.Policy.CostThreshold)
	}
	if st.Policy.CostThreshold <= 0 && st.Policy.ScanRows <= 0 {
		return st, fmt.Errorf("--route-cost-threshold and --route-scan-rows are both 0, so no statement could ever go to the copy; set one of them")
	}
	return st, nil
}

// routerFlashbackConfig is how the router's port serves: routing always on,
// with no maximum copy age, a busy copy answers error 1040, and a server
// that cannot route refuses its statements instead of answering them from
// the copy alone.
func routerFlashbackConfig(st routerSettings) flashbackConfig {
	return flashbackConfig{
		RoutePolicy:      st.Policy,
		RouteReadOnly:    st.ReadOnly,
		RouteAnyCopyAge:  true,
		RouteBusyRefuses: true,
		RouteRequired:    true,
		RouteStatus:      true,
	}
}

// newRouterConsole builds the console the port resolves servers and runs
// SQL through. No web interface is served from it and nothing captures: it
// is the daemon's view of its servers, read from the daemon's files by a
// registry that cannot write them, with a pool of SQL workers of the size
// given. The runner is returned so the caller can stop its workers.
func newRouterConsole(st routerSettings, pool console.SQLPool) (*console.Server, *sqlsandbox.Runner, error) {
	registry, err := console.LoadRegistryFollower(st.ServersFile)
	if err != nil {
		return nil, nil, err
	}
	srv, err := console.New(console.Config{
		Token:    st.Token,
		Registry: registry,
		Version:  appVersion,
		// The same three files the daemon keeps a statement from reading
		// through a copy directory that contains them.
		AuthPath:     st.AuthFile,
		MCPTokenPath: st.MCPTokenFile,
		// The port's password, created in the web interface: followed only
		// when the router has no token (routerPortFile). No address is
		// given here: the router binds its own.
		FlashbackPath:       routerPortFile(st),
		SQLLimits:           sqlsandbox.Limits{MemoryLimit: st.SQLMemory, Threads: pool.Threads},
		SQLMaxInFlight:      pool.Workers,
		SQLWorkerStatements: sqlsandbox.DefaultWorkerStatements,
		SQLPortSharedSlots:  true,
		SQLPortMaxRows:      st.SQLPortMaxRows,
		KillSourceThreads:   killSourceThreads,
		// A server is served while its index is away: forwarding and the
		// copy's tables do not read it. Time travel and the events view
		// do, and fail then with the reason.
		LazyIndex: true,
	})
	if err != nil {
		return nil, nil, err
	}
	return srv, srv.SQLSandbox(), nil
}

// routerCopySnapshotEvery is how often the age of each server's copy is read
// for the metric: a copy changes when a snapshot is taken, which is minutes
// apart at the closest.
const routerCopySnapshotEvery = 30 * time.Second

// exportCopySnapshots keeps bintrail_read_routing_copy_snapshot_timestamp_seconds
// in step with the copies, until ctx ends. A server whose copy can no longer
// be read loses its series, so an alert on the age does not go on reading a
// number that stopped moving.
func exportCopySnapshots(ctx context.Context, srv *console.Server, every time.Duration) {
	known := map[string]bool{}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		now := srv.CopySnapshots(ctx)
		for id, at := range now {
			observe.SetCopySnapshot(id, at)
			known[id] = true
		}
		for id := range known {
			if _, ok := now[id]; !ok {
				observe.SetCopySnapshot(id, time.Time{})
				delete(known, id)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// routerPortFile is the saved setting whose password the router accepts, ""
// when it accepts none from it. With a token, the token decides and the file
// is not read: that is the capture process's own rule when it is started
// with an address for its port, and such a process ignores the file without
// clearing it, so a password left there would open this port while opening
// nothing where it was made, and nobody could see or replace it.
func routerPortFile(st routerSettings) string {
	if st.Token != "" {
		return ""
	}
	return filepath.Join(filepath.Dir(st.ServersFile), console.FlashbackFileName)
}

// routerIgnoredEnv names the capture process's settings that are in the
// environment and do not apply to the router, each with why. One environment
// file serves both, so a setting that is silently not read here would read
// as in force.
func routerIgnoredEnv() []string {
	var out []string
	if v := os.Getenv("BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE"); v != "" {
		out = append(out, fmt.Sprintf("BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE=%s is not applied: the router has no maximum copy age, an expensive read goes to the copy whatever the age of its snapshot", v))
	}
	if v := os.Getenv("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT"); v != "" {
		out = append(out, fmt.Sprintf("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT=%s is not applied: the router works out how many statements run at once from its cores and memory", v))
	}
	return out
}

// routerPort is the router's listener over time. The port is open while a
// credential that could open it exists, and it is closed with every
// connection on it when the password from the web interface is withdrawn.
type routerPort struct {
	srv  *console.Server
	addr string
	cfg  flashbackConfig
	// poll is how often it looks for a credential while it has none.
	poll time.Duration
	out  io.Writer

	mu    sync.Mutex
	bound string
	stop  context.CancelFunc
}

// listening is the address the port is bound on now, "" while it is closed.
func (p *routerPort) listening() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bound
}

// serve runs the port until ctx ends. It returns an error only when the
// address cannot be bound the first time: a router that never listened must
// not stay up looking like one that does. After that a failure to bind
// again is logged and tried again, since something is expected to connect
// to this address.
func (p *routerPort) serve(ctx context.Context) error {
	p.srv.OnFlashbackPasswordWithdrawn(func() {
		// Every connection: the port cannot tell which of them came in
		// with the password that is gone. One made with the token
		// reconnects at once.
		p.mu.Lock()
		if p.stop != nil {
			p.stop()
		}
		p.mu.Unlock()
	})
	// Before anything else, and before waiting for a credential: an address
	// that cannot be bound is said now, not on the day a password is created.
	probe, err := net.Listen("tcp", p.addr)
	if err != nil {
		return fmt.Errorf("the router cannot listen on %s: %w", p.addr, err)
	}
	_ = probe.Close()
	opened, waiting, saidRebind := false, false, false
	for ctx.Err() == nil {
		if len(p.srv.FlashbackPasswords()) == 0 {
			if !waiting {
				waiting = true
				fmt.Fprintf(p.out, "The router is not listening yet: there is no credential that could open its port. Start it with --token (or BINTRAIL_CONSOLE_TOKEN), or turn the MySQL port on in the web interface, which creates a password; it opens %s then, without a restart. (A capture process started with --flashback-listen saves no password there: use the token.)\n", p.addr)
			}
			select {
			case <-ctx.Done():
			case <-time.After(p.poll):
			}
			continue
		}
		waiting = false
		ln, err := net.Listen("tcp", p.addr)
		if err != nil {
			if !opened {
				return fmt.Errorf("the router cannot listen on %s: %w", p.addr, err)
			}
			if !saidRebind {
				// Once: it is tried again every second until it works.
				saidRebind = true
				slog.Error("router: the port could not be opened again and the router is NOT listening; it keeps trying", "listen", p.addr, "error", err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		opened, saidRebind = true, false
		child, cancel := context.WithCancel(ctx)
		p.mu.Lock()
		p.bound, p.stop = ln.Addr().String(), cancel
		p.mu.Unlock()
		slog.Info("router: the MySQL-protocol port is open", "listen", ln.Addr().String())
		err = serveFlashback(child, p.srv, ln, p.cfg)
		cancel()
		p.mu.Lock()
		p.bound, p.stop = "", nil
		p.mu.Unlock()
		if err != nil && ctx.Err() == nil && len(p.srv.FlashbackPasswords()) > 0 {
			slog.Warn("router: the MySQL-protocol port stopped; opening it again", "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}

// routerStartup is what the router says when it starts.
type routerStartup struct {
	Listen      string
	Pool        console.SQLPool
	MemoryLimit string
	Cfg         flashbackConfig
	// ServersFile, whether it exists, and how many servers it holds now.
	ServersFile   string
	ServersExists bool
	Servers       int
	// PortFile is the saved setting whose password is accepted, "" when the
	// token decides.
	PortFile string
	// Ignored are the settings found in the environment that do not apply.
	Ignored []string
}

// routerStartupLines says, at startup, where the port will be, which files
// it serves from, the pool that was worked out and why, and what read
// routing sends where. The files are named because a wrong path is silent
// otherwise: a file that does not exist is an empty list of servers, and
// every client is then told its server does not exist.
func routerStartupLines(u routerStartup) string {
	var b strings.Builder
	host, _, _ := net.SplitHostPort(u.Listen)
	fmt.Fprintf(&b, "Router: the MySQL-protocol port is %s; connect a MySQL client with user=<server id or name>.\n", u.Listen)
	if host == "" || host == "0.0.0.0" || host == "::" {
		b.WriteString("Router: that address is every network interface of this machine, and the port forwards statements to each source on its credential alone. Bind 127.0.0.1, or keep it behind a firewall.\n")
	}
	if u.PortFile == "" {
		b.WriteString("Router: the password is the access token (--token). A port password created in the web interface is not accepted here.\n")
	} else {
		fmt.Fprintf(&b, "Router: no access token was given, so the password is the one the web interface creates when the MySQL port is turned on there (read from %s), and the port is open only while it is on.\n", u.PortFile)
	}
	switch {
	case !u.ServersExists:
		fmt.Fprintf(&b, "Router: the servers file %s does not exist yet, so no server can be reached. It is read again when it appears; if DBTrail keeps it somewhere else, give that path with --servers-file.\n", u.ServersFile)
	default:
		fmt.Fprintf(&b, "Router: %s from %s, read again when it changes.\n", countOfServers(u.Servers), u.ServersFile)
	}
	fmt.Fprintf(&b, "Router: %d statements on the copy at once, %d threads and up to %s of memory each: %s.\n", u.Pool.Workers, u.Pool.Threads, u.MemoryLimit, u.Pool.Why)
	rule := fmt.Sprintf("SELECTs with plan cost >= %.0f or a full scan over >= %d rows (on a MariaDB source, also joins that read that many rows in all) run on the copy, whatever the age of its snapshot. When every worker is busy such a statement waits up to %s, with at most %d waiting at once; past either the client gets error 1040",
		u.Cfg.RoutePolicy.CostThreshold, u.Cfg.RoutePolicy.ScanRows, sqlsandbox.DefaultMaxWait, sqlsandbox.DefaultMaxWaiters)
	if u.Cfg.RouteReadOnly {
		b.WriteString("Router: read routing is read-only (--route-read-only): reads go to each server's source MySQL with that server's forwarding account when it has one, else with its source account; " + rule + ". A statement that is not a read is refused and never sent to the source. The check reads the statement's text: what a stored function does when a SELECT calls it is up to that account's grants.\n")
	} else {
		b.WriteString("Router: read routing is read-write: statements, writes included, go to each server's source MySQL with that server's forwarding account when it has one, else with its source account; " + rule + ". Anyone holding a credential of this port can do on each source what that account can. Set --route-read-only to refuse writes, and a forwarding account on each server to bound the rest.\n")
	}
	for _, line := range u.Ignored {
		b.WriteString("Router: " + line + ".\n")
	}
	return b.String()
}

func countOfServers(n int) string {
	if n == 1 {
		return "1 server"
	}
	return fmt.Sprintf("%d servers", n)
}

func runRouter(cmd *cobra.Command, _ []string) error {
	cli.LoadEnvFile()
	st, err := routerSettingsFrom(cmd)
	if err != nil {
		return err
	}
	memoryLimit := st.SQLMemory
	if memoryLimit == "" {
		memoryLimit = sqlsandbox.DefaultLimits().MemoryLimit
	}
	pool := console.SQLPoolFor(console.ThisMachineForSQLPool(memoryLimit))
	srv, runner, err := newRouterConsole(st, pool)
	if err != nil {
		return err
	}
	if runner != nil {
		defer runner.Close()
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Read once before the port opens, so the first client finds the
	// servers and the password that are in the files now.
	srv.RefreshFollowedFiles()
	go srv.FollowFiles(ctx, 0)
	if st.MetricsAddr != "" {
		stopMetrics, err := streamrun.StartMetricsServer(st.MetricsAddr)
		if err != nil {
			return err
		}
		defer stopMetrics()
		go exportCopySnapshots(ctx, srv, routerCopySnapshotEvery)
	}

	cfg := routerFlashbackConfig(st)
	_, statErr := os.Stat(st.ServersFile)
	fmt.Fprint(os.Stderr, routerStartupLines(routerStartup{
		Listen: st.Listen, Pool: pool, MemoryLimit: memoryLimit, Cfg: cfg,
		ServersFile: st.ServersFile, ServersExists: statErr == nil, Servers: srv.ServerCount(),
		PortFile: routerPortFile(st), Ignored: routerIgnoredEnv(),
	}))
	port := &routerPort{srv: srv, addr: st.Listen, cfg: cfg, poll: console.DefaultFollowInterval, out: os.Stderr}
	return port.serve(ctx)
}
