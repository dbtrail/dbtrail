package consoleapp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
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
	Short: "Serve the MySQL-protocol port with Engine Routing, as a service of its own beside the one that captures",
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
machine, since capture uses the machine too. --concurrency sets the number
of statements by hand. When every worker is busy a
statement waits its turn, in the order it arrived, for as long as its client
stays connected: many clients at once are slower, not refused. Give
--max-wait a time (30s) to refuse one with an error past it.

Example:
  BINTRAIL_CONSOLE_TOKEN=... bintrail-console router --listen 127.0.0.1:3309`,
	Args: cobra.NoArgs,
	RunE: runRouter,
}

// defaultRouterListen is the MySQL port: the router is the only process
// that serves it.
const defaultRouterListen = "127.0.0.1:3309"

func init() {
	addRouterFlags(routerCmd)
	rootCmd.AddCommand(routerCmd)
}

func addRouterFlags(cmd *cobra.Command) {
	def := readrouter.DefaultPolicy()
	f := cmd.Flags()
	f.String("listen", defaultRouterListen, "Address the MySQL-protocol port listens on (host:port). Env BINTRAIL_ROUTER_LISTEN.")
	f.String("metrics-addr", "", "Address to expose Prometheus metrics on (e.g. 127.0.0.1:9091); empty = none. The same address answers /status, the router's numbers as JSON, which the web interface reads when it is given this address (BINTRAIL_CONSOLE_ROUTER_URL). Neither asks for a password. Not the capture process's address: each process serves its own. Env BINTRAIL_ROUTER_METRICS_ADDR.")
	f.String("token", "", "Access token: one more password for the port, beside the one created in the web interface. It does not open the port: the port is open while the MySQL port is turned on in the web interface, and closed when it is turned off there. Env BINTRAIL_CONSOLE_TOKEN.")
	f.String("servers-file", "", "Path to the server registry the web interface manages (default ~/.config/bintrail/console-servers.yaml). Read, never written. Env BINTRAIL_CONSOLE_SERVERS.")
	f.String("auth-file", "", "Path to the web interface auth file, when it is not the default: only so a statement cannot read it through a copy directory that contains it. Env BINTRAIL_CONSOLE_AUTH.")
	f.String("mcp-token-file", "", "Path to the managed MCP token file, when it is not the default: for the same reason as --auth-file. Env BINTRAIL_CONSOLE_MCP_TOKEN_FILE.")
	f.String("sql-memory", "", "Memory each statement on the copy may use, e.g. 4GB; default 2GB, at least 512MB. Fewer statements run at once when each may use more, unless --concurrency sets the number. Unset, the memory saved in the web interface (Settings, MySQL clients) is used, read when the router starts: a change there takes effect when the router is restarted, since how many statements run at once was worked out from it. Env BINTRAIL_CONSOLE_SQL_MEMORY.")
	f.String("concurrency", "", "How many statements run on the copy at once, 1 to 64, like innodb_thread_concurrency on a MySQL server. Empty or 0: worked out from the cores and the memory this process may use (one statement per two cores). A number above that is taken as said: each statement gets fewer threads, down to one, and the startup lines warn when the number can ask for more memory than there is. A higher number also divides the temporary disk statements may use past their memory into more shares, and keeps that many idle worker processes ready. The rest wait their turn. Env BINTRAIL_ROUTER_CONCURRENCY.")
	f.String("max-wait", "", "How long a statement waits for its turn on the copy when every worker is busy, e.g. 30s or 2m; past it the client gets an error (1040 on a routed connection, 1203 on one that is not). Empty or 0: no limit, it waits for as long as its client stays connected. Env BINTRAIL_ROUTER_MAX_WAIT.")
	f.Int("sql-port-max-rows", console.DefaultSQLPortMaxRows, "Most rows one statement returns; a result with more is refused with error 1104, not cut. Env BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS.")
	f.Bool("route-read-only", true, "On unless set to false (--route-read-only=false, BINTRAIL_CONSOLE_ROUTE_READ_ONLY=0). Refuse every statement that is not a read, and never send it to the source. It reads the statement's text, so it cannot see a stored function that writes: the forwarding account's grants are the guard for that. Env BINTRAIL_CONSOLE_ROUTE_READ_ONLY.")
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
	// Concurrency is how many statements run on the copy at once as the
	// operator set it, 0 when the machine decides.
	Concurrency int
	// MaxWait is how long a statement waits for a worker of the copy:
	// sqlsandbox.NoMaxWait unless a bound was given.
	MaxWait  time.Duration
	Policy   readrouter.Policy
	ReadOnly bool
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

	if st.Concurrency, err = parseRouterConcurrency(str("concurrency", "BINTRAIL_ROUTER_CONCURRENCY")); err != nil {
		return st, err
	}
	if st.MaxWait, err = parseRouterMaxWait(str("max-wait", "BINTRAIL_ROUTER_MAX_WAIT")); err != nil {
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
				return st, fmt.Errorf("BINTRAIL_CONSOLE_ROUTE_READ_ONLY=%q is not a yes or no: use 1 or true (or leave it unset) for a read-only port, 0 or false to forward writes", v)
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
// with no maximum copy age, and a busy copy answers error 1040. A server
// that cannot route is answered from the copy alone (#2341).
func routerFlashbackConfig(st routerSettings) flashbackConfig {
	return flashbackConfig{
		RoutePolicy:         st.Policy,
		RouteReadOnly:       st.ReadOnly,
		RouteAnyCopyAge:     true,
		RouteBusyRefuses:    true,
		RouteStatus:         true,
		FailedBindFailsPing: true,
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
		FlashbackPath:          routerPortFile(st),
		FlashbackFollowsSwitch: true,
		SQLLimits:              sqlsandbox.Limits{MemoryLimit: st.SQLMemory, Threads: pool.Threads},
		SQLMaxInFlight:         pool.Workers,
		SQLWorkerStatements:    sqlsandbox.DefaultWorkerStatements,
		SQLMaxWaiters:          routerMaxWaiters,
		SQLMaxWait:             st.MaxWait,
		SQLPortSharedSlots:     true,
		SQLPortMaxRows:         st.SQLPortMaxRows,
		KillSourceThreads:      killSourceThreads,
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

// routerMaxWaiters is how many statements may wait for a worker of the
// router at once (#2327). It is the only bound of a line with no time
// bound (the default, --max-wait unset): one statement more is refused at
// once with error 1040. Under --max-wait the wait is what bounds the line,
// not this number: a statement waits its turn for that long and only then
// is refused, so a refusal costs the client that wait and cannot be repeated
// in a loop. With a short line the refusal came at once, after
// the source had already been asked for the statement's plan, and clients
// that retried without a pause cost the source one EXPLAIN per attempt and
// halved what the router answered. This number only keeps a flood from
// parking without limit; each waiter is a connection that exists anyway.
const routerMaxWaiters = 4096

// routerCopySnapshotEvery is how often the age of each server's copy is read
// for the metric: a copy changes when a snapshot is taken, which is minutes
// apart at the closest.
const routerCopySnapshotEvery = 30 * time.Second

// routerCopySnapshots is what the last reading of the copies' snapshot times
// found, kept so the router's status document answers from memory: reading
// them lists folders, once per server.
type routerCopySnapshots struct {
	mu     sync.Mutex
	at     map[string]time.Time
	readAt time.Time
}

func (c *routerCopySnapshots) set(at map[string]time.Time, readAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at, c.readAt = at, readAt
}

// get is the last reading and when it was taken; a zero time is "not read
// yet", which is not the same as "no server has a copy".
func (c *routerCopySnapshots) get() (map[string]time.Time, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at, c.readAt
}

// exportCopySnapshots keeps bintrail_read_routing_copy_snapshot_timestamp_seconds
// in step with the copies, until ctx ends, and leaves each reading in last
// for the status document. A server whose copy can no longer be read loses
// its series, so an alert on the age does not go on reading a number that
// stopped moving.
func exportCopySnapshots(ctx context.Context, read func(context.Context) map[string]time.Time, every time.Duration, last *routerCopySnapshots) {
	known := map[string]bool{}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		now := read(ctx)
		last.set(now, time.Now())
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

// routerStatusPath is where the status document is served, on the address
// the metrics are on.
const routerStatusPath = "/status"

// routerStatus builds the document from the numbers and the last reading of
// the copies. A server that has a copy and was sent nothing yet gets an
// entry of zeros with its snapshot: its copy's age is a number to watch
// before the first statement.
func routerStatus(st routerSettings, numbers console.RouterNumbers, last *routerCopySnapshots) console.RouterStatusDoc {
	doc := console.RouterStatusDoc{
		Version: console.RouterStatusVersion, Listen: st.Listen, ReadOnly: st.ReadOnly,
		CostThreshold: st.Policy.CostThreshold, ScanRows: st.Policy.ScanRows,
		RouterNumbers: numbers,
	}
	if doc.Servers == nil {
		doc.Servers = map[string]console.RouterServerNumbers{}
	}
	at, readAt := last.get()
	if !readAt.IsZero() {
		doc.CopySnapshotsReadAt = readAt.UTC().Format(time.RFC3339)
	}
	for id, when := range at {
		if when.IsZero() {
			continue
		}
		n, ok := doc.Servers[id]
		if !ok {
			n = console.RouterServerNumbers{Reasons: map[string]uint64{}}
		}
		n.CopySnapshot = when.UTC().Format(time.RFC3339)
		doc.Servers[id] = n
	}
	return doc
}

// routerStatusResolveBudget is how long the status document waits for the
// copy answers' lag to be placed before it answers with what is known:
// well under what the web interface waits for the whole document, so a
// slow index never makes a live router look down.
const routerStatusResolveBudget = 750 * time.Millisecond

// resolveCopyAgesFirst places what it can of the copy answers' lag, within
// routerStatusResolveBudget of the request, then serves next.
func resolveCopyAgesFirst(resolve func(context.Context), next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), routerStatusResolveBudget)
		resolve(ctx)
		cancel()
		next.ServeHTTP(w, r)
	})
}

// resolveCopyAgesEvery calls resolve every so often until ctx ends.
func resolveCopyAgesEvery(ctx context.Context, resolve func(context.Context), every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Bounded: a hung index must not hold the next call off forever.
			rctx, cancel := context.WithTimeout(ctx, every)
			resolve(rctx)
			cancel()
		}
	}
}

// routerStatusHandler serves the document. numbers is read on every request.
func routerStatusHandler(st routerSettings, numbers func() console.RouterNumbers, last *routerCopySnapshots) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(routerStatus(st, numbers(), last)); err != nil {
			slog.Warn("router: the status document could not be written", "error", err)
		}
	})
}

// routerPortConfig is routerFlashbackConfig with the switch saved in the web
// interface deciding whether statements are routed (#2341), with or without
// a token.
func routerPortConfig(st routerSettings, routingOn func() bool) flashbackConfig {
	cfg := routerFlashbackConfig(st)
	cfg.RouteSwitch = routingOn
	return cfg
}

// routerMemory settles the memory a statement may use before the pool is
// sized from it (#2341): what the router was started with, else what the
// web interface saved, else the default (st.SQLMemory left empty). It is
// read once, here: the number of workers is worked out from it, so a value
// taken later would let workers times memory pass what the service was
// given. from says where it came from, for the startup lines; err is a
// saved setting that is there and cannot be read, which the default then
// stands in for, said loudly.
func routerMemory(st *routerSettings, savedPath string) (from string, err error) {
	if st.SQLMemory != "" {
		return "set where the router is started", nil
	}
	saved, err := console.SavedSQLMemory(savedPath)
	if err != nil {
		return "the default", err
	}
	if saved == "" {
		return "the default", nil
	}
	st.SQLMemory = saved
	return "saved in the web interface, read from " + savedPath, nil
}

// routerMemoryEvery is how often the saved SQL memory is read again, to say
// when it is no longer the one applied.
const routerMemoryEvery = 30 * time.Second

// followRouterMemory says, once per value, when the memory saved in the web
// interface is no longer the one this router was sized with: it is applied
// at the next start, and until then nothing else would say why a change
// made there did nothing here. Only when the router took the saved value or
// the default; a value set where it is started decides.
func followRouterMemory(ctx context.Context, savedPath, applied string, every time.Duration, warn func(msg string, args ...any)) {
	said := applied
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		saved, err := console.SavedSQLMemory(savedPath)
		if err != nil || saved == said {
			continue
		}
		said = saved
		if saved == applied {
			continue
		}
		now := saved
		if now == "" {
			now = "the default"
		}
		warn("router: the SQL memory saved in the web interface changed and is NOT applied here yet: restart the router to apply it (how many statements run at once is worked out from it, unless --concurrency sets the number)", "saved", now, "running_with", applied)
	}
}

// routerPortFile is the saved setting the router follows: the MySQL port's,
// which the web interface writes. Its switch opens and closes the router's
// port, its password is accepted while the port is on, and its routing says
// whether statements are routed. A token the router was started with is one
// more password while the port is on. It does not open the port: a port
// nobody turned on must not open because a token is in the environment the
// router shares with the capture process (#2341).
func routerPortFile(st routerSettings) string {
	return filepath.Join(filepath.Dir(st.ServersFile), console.FlashbackFileName)
}

// routerMetricsOffLine says that the router's numbers could not be served on
// addr, and that the MySQL port is served all the same.
func routerMetricsOffLine(addr string, err error) string {
	return fmt.Sprintf("WARNING: the router's numbers and metrics are OFF: %s could not be opened (%v). The MySQL port is served without them, and the web interface shows nothing about the router. Free that address or set another (BINTRAIL_ROUTER_METRICS_ADDR or --metrics-addr, and the same in BINTRAIL_CONSOLE_ROUTER_URL where the web interface runs), then restart the router.\n", addr, err)
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
	if v := os.Getenv("BINTRAIL_CONSOLE_FLASHBACK_LISTEN"); v != "" {
		out = append(out, fmt.Sprintf("BINTRAIL_CONSOLE_FLASHBACK_LISTEN=%s is not applied: the router listens where BINTRAIL_ROUTER_LISTEN or --listen says, 127.0.0.1:3309 unless set. To keep answering on the old address, set BINTRAIL_ROUTER_LISTEN=%s", v, v))
	}
	if v := os.Getenv("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT"); v != "" {
		out = append(out, fmt.Sprintf("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT=%s is not applied: it is the capture process's. How many statements the router runs at once is BINTRAIL_ROUTER_CONCURRENCY or --concurrency, and worked out from its cores and memory when that is not set", v))
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
// open says whether the port is to be open now: there is a password to
// take, which there is only while the switch saved in the web interface is
// on (console.Config.FlashbackFollowsSwitch).
func (p *routerPort) open() bool {
	return len(p.srv.FlashbackPasswords()) > 0
}

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
		if !p.open() {
			if !waiting {
				waiting = true
				fmt.Fprintf(p.out, "The router is not listening yet: the MySQL port is not turned on in the web interface (Settings, MySQL clients, Connect a SQL client). Turning it on there creates its password and opens %s here within a few seconds, without a restart. An access token (--token, BINTRAIL_CONSOLE_TOKEN) is one more password while the port is on; it does not open it.\n", p.addr)
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
		if err != nil && ctx.Err() == nil && p.open() {
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
	// Token: the router was started with an access token.
	Token bool
	// MemoryFrom says where MemoryLimit came from; MemoryErr is a saved
	// setting that could not be read (routerMemory).
	MemoryFrom string
	MemoryErr  error
	Cfg        flashbackConfig
	// ServersFile, whether it exists, and how many servers it holds now.
	ServersFile   string
	ServersExists bool
	Servers       int
	// PortFile is the saved setting whose password is accepted, "" when the
	// token decides.
	PortFile string
	// MaxWait is how long a statement waits for a worker of the copy.
	MaxWait time.Duration
	// Ignored are the settings found in the environment that do not apply.
	Ignored []string
	// InContainer: this process runs in a container, where listening on
	// every interface is how a port is published and not an exposure.
	InContainer bool
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
		if u.InContainer {
			// Inside a container this is how a port is published at all;
			// what reaches it is the container's port mapping.
			b.WriteString("Router: that address is every network interface of this container. What reaches it from outside is what the container publishes: the port forwards statements to each source on its credential alone, so publish it on 127.0.0.1 or behind a firewall.\n")
		} else {
			b.WriteString("Router: that address is every network interface of this machine, and the port forwards statements to each source on its credential alone. Bind 127.0.0.1, or keep it behind a firewall.\n")
		}
	}
	fmt.Fprintf(&b, "Router: the port is open only while the MySQL port is turned on in the web interface (read from %s). The password is the one created there", u.PortFile)
	if u.Token {
		b.WriteString("; the access token (--token) is accepted too")
	}
	b.WriteString(". Statements are routed only when that file says so (routing: true); otherwise the copy answers every statement.\n")
	switch {
	case !u.ServersExists:
		fmt.Fprintf(&b, "Router: the servers file %s does not exist yet, so no server can be reached. It is read again when it appears; if DBTrail keeps it somewhere else, give that path with --servers-file.\n", u.ServersFile)
	default:
		fmt.Fprintf(&b, "Router: %s from %s, read again when it changes.\n", countOfServers(u.Servers), u.ServersFile)
	}
	fmt.Fprintf(&b, "Router: %s on the copy at once, %s and up to %s of memory each: %s.\n", countOfStatements(u.Pool.Workers), countOfThreads(u.Pool.Threads), u.MemoryLimit, u.Pool.Why)
	if u.Pool.Warning != "" {
		fmt.Fprintf(&b, "Router: WARNING: %s.\n", u.Pool.Warning)
	}
	if u.MemoryFrom != "" {
		fmt.Fprintf(&b, "Router: that memory is %s. A change saved in the web interface is applied when the router is restarted.\n", u.MemoryFrom)
	}
	if u.MemoryErr != nil {
		fmt.Fprintf(&b, "Router: the SQL memory saved in the web interface could NOT be read, so the default is used: %v\n", u.MemoryErr)
	}
	rule := fmt.Sprintf("SELECTs with plan cost >= %.0f or a full scan over >= %d rows (on a MariaDB source, also joins that read that many rows in all) run on the copy, whatever the age of its snapshot. When every worker is busy such a statement waits its turn, in the order it arrived, %s",
		u.Cfg.RoutePolicy.CostThreshold, u.Cfg.RoutePolicy.ScanRows, routerWaitText(u.MaxWait))
	if u.Cfg.RouteReadOnly {
		b.WriteString("Router: Engine Routing is read-only (--route-read-only): reads go to each server's source MySQL with that server's forwarding account; a server that has none is not forwarded for and is answered from the copy alone, since the account DBTrail captures with is never used; " + rule + ". A statement that is not a read is refused and never sent to the source. The check reads the statement's text: what a stored function does when a SELECT calls it is up to that account's grants.\n")
	} else {
		b.WriteString("Router: Engine Routing is read-write: statements, writes included, go to each server's source MySQL with that server's forwarding account; a server that has none is not forwarded for and is answered from the copy alone, since the account DBTrail captures with is never used; " + rule + ". Anyone holding a credential of this port can do on each source what that account can. Set --route-read-only to refuse writes, and a forwarding account on each server to bound the rest.\n")
	}
	for _, line := range u.Ignored {
		b.WriteString("Router: " + line + ".\n")
	}
	return b.String()
}

// inContainer reports whether this process runs in a Docker or Podman
// container, by the marker file each leaves at the root.
func inContainer() bool {
	for _, marker := range []string{"/.dockerenv", "/run/.containerenv"} {
		if _, err := os.Stat(marker); err == nil {
			return true
		}
	}
	return false
}

// routerWaitText ends the sentence of the startup lines that says what a
// statement does while every worker of the copy is busy.
func routerWaitText(maxWait time.Duration) string {
	if maxWait == sqlsandbox.NoMaxWait {
		return fmt.Sprintf("for as long as its client stays connected (no time limit; --max-wait sets one). Past %d statements waiting, one more gets an error (1040 on a routed connection, 1203 on one that is not)", routerMaxWaiters)
	}
	return fmt.Sprintf("for up to %s (--max-wait); past that the client gets an error (1040 on a routed connection, 1203 on one that is not)", maxWait)
}

// routerPool sizes the router's pool of workers: from this machine, and
// with the number of statements the operator set when there is one.
func routerPool(st routerSettings, memoryLimit string) console.SQLPool {
	machine := console.ThisMachineForSQLPool(memoryLimit)
	machine.Asked = st.Concurrency
	return console.SQLPoolFor(machine)
}

// parseRouterConcurrency reads --concurrency: a whole number of statements,
// empty or 0 for the machine to decide.
func parseRouterConcurrency(v string) (int, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	switch {
	case err != nil:
		return 0, fmt.Errorf("--concurrency %q is not a whole number of statements: write one from 1 to %d, or leave it empty for the router to work it out", v, console.SQLPoolMaxWorkers)
	case n < 0:
		return 0, fmt.Errorf("--concurrency %q is negative: write a number from 1 to %d, or leave it empty for the router to work it out", v, console.SQLPoolMaxWorkers)
	case n > console.SQLPoolMaxWorkers:
		return 0, fmt.Errorf("--concurrency %q is more than %d, the most statements the router runs at once: each is a process of its own with its own memory. Write a number up to %d; more clients than that wait their turn", v, console.SQLPoolMaxWorkers, console.SQLPoolMaxWorkers)
	}
	return n, nil
}

// routerMaxWaitMost is the longest time --max-wait takes: past a day a
// limit is no limit, and the largest duration is how "no limit" is written
// inside (sqlsandbox.NoMaxWait), which a typed value must not land on.
const routerMaxWaitMost = 24 * time.Hour

// parseRouterMaxWait reads --max-wait. Empty and zero are the line with no
// time bound. A bare number is refused rather than guessed at: 30 could be
// seconds or minutes, and Go would read it as nanoseconds.
func parseRouterMaxWait(v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return sqlsandbox.NoMaxWait, nil
	}
	// A bare number, told apart from the rest so the refusal can say what
	// is missing. Digits and one dot only: strconv would also take "NaN",
	// "1e3" or "0x1p4", and "write it like NaNs" helps nobody.
	if bare := strings.TrimLeft(v, "+-"); bare != "" && strings.Trim(bare, "0123456789.") == "" && strings.Count(bare, ".") <= 1 && bare != "." {
		if strings.Trim(bare, "0.") == "" {
			return sqlsandbox.NoMaxWait, nil
		}
		if bare != strings.TrimPrefix(v, "+") {
			return 0, fmt.Errorf("--max-wait %q is negative: write a time like 30s, or leave it empty for no limit", v)
		}
		return 0, fmt.Errorf("--max-wait %q has no unit: write it with one, like %ss or %sm", v, bare, bare)
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("--max-wait %q is not a time: write it like 30s or 2m, or leave it empty for no limit", v)
	}
	switch {
	case d < 0:
		return 0, fmt.Errorf("--max-wait %q is negative: write a time like 30s, or leave it empty for no limit", v)
	case d == 0:
		return sqlsandbox.NoMaxWait, nil
	case d > routerMaxWaitMost:
		return 0, fmt.Errorf("--max-wait %q is more than %s: leave it empty for no limit", v, routerMaxWaitMost)
	case d < time.Second:
		return 0, fmt.Errorf("--max-wait %q is under a second: a statement refused that fast is sent again as fast, and each try asks the source for its plan. Write at least 1s, or leave it empty for no limit", v)
	}
	return d, nil
}

func countOfThreads(n int) string {
	if n == 1 {
		return "1 thread"
	}
	return fmt.Sprintf("%d threads", n)
}

func countOfStatements(n int) string {
	if n == 1 {
		return "1 statement"
	}
	return fmt.Sprintf("%d statements", n)
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
	startedWithMemory := st.SQLMemory != ""
	memoryFrom, savedErr := routerMemory(&st, sqlSettingsFilePath(st.ServersFile))
	memoryLimit := st.SQLMemory
	if memoryLimit == "" {
		memoryLimit = sqlsandbox.DefaultLimits().MemoryLimit
	}
	pool := routerPool(st, memoryLimit)
	if pool.Warning != "" {
		// In the log as well as in the startup lines: those are plain text
		// on stderr, which a collector that reads levels does not see.
		slog.Warn("router: --concurrency can ask for more memory than this process may use", "detail", pool.Warning)
	}
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
	if !startedWithMemory {
		go followRouterMemory(ctx, sqlSettingsFilePath(st.ServersFile), st.SQLMemory, routerMemoryEvery, slog.Warn)
	}
	// The copy answers' lag is placed in the index here, every 30 seconds,
	// never by a statement (#2369); with or without a status document, so
	// what waits on it does not pile up.
	go resolveCopyAgesEvery(ctx, srv.ResolveCopyAges, routerCopySnapshotEvery)
	if st.MetricsAddr != "" {
		last := &routerCopySnapshots{}
		stopMetrics, err := streamrun.StartMetricsServerWith(st.MetricsAddr, map[string]http.Handler{
			"GET " + routerStatusPath: resolveCopyAgesFirst(srv.ResolveCopyAges, routerStatusHandler(st, srv.RouterNumbers, last)),
		})
		if err != nil {
			// Not a reason to leave the MySQL port closed: the numbers are
			// an extra, and their address may simply be taken.
			fmt.Fprint(os.Stderr, routerMetricsOffLine(st.MetricsAddr, err))
		} else {
			defer stopMetrics()
			go exportCopySnapshots(ctx, srv.CopySnapshots, routerCopySnapshotEvery, last)
		}
	}

	cfg := routerPortConfig(st, srv.FlashbackRoutingOn)
	_, statErr := os.Stat(st.ServersFile)
	fmt.Fprint(os.Stderr, routerStartupLines(routerStartup{
		Listen: st.Listen, Pool: pool, MemoryLimit: memoryLimit, MemoryFrom: memoryFrom, MemoryErr: savedErr, Cfg: cfg,
		ServersFile: st.ServersFile, ServersExists: statErr == nil, Servers: srv.ServerCount(),
		PortFile: routerPortFile(st), Token: st.Token != "", MaxWait: st.MaxWait, Ignored: routerIgnoredEnv(), InContainer: inContainer(),
	}))
	port := &routerPort{srv: srv, addr: st.Listen, cfg: cfg, poll: console.DefaultFollowInterval, out: os.Stderr}
	return port.serve(ctx)
}
