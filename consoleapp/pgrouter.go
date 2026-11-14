package consoleapp

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// pgRouterCmd is the PostgreSQL-protocol port (#2765) as a process of its
// own: a PostgreSQL client connects to it and runs read-only SQL on the copy
// of a PostgreSQL server.
//
// It is a separate process from `router`, the MySQL-protocol port, on
// purpose, and shares no code path with it at run time: its own listener,
// its own workers and memory, its own file. Nothing a PostgreSQL client does
// here can take a worker from a MySQL client, and consoleapp/router.go does
// not know this command exists. What the two have in common is what they
// read: the servers the web interface manages and the port's passwords,
// never written from here.
//
// A client picks its server with the database name (dbname=<server id or
// name>); the password is the one the MySQL port takes. A username that is a
// server's application login opens that server alone, with that login's
// password or the port's, whatever database it names. The copy answers every
// statement: nothing is forwarded to the source, and time travel is not
// served here yet.
var pgRouterCmd = &cobra.Command{
	Use:   "pg-router",
	Short: "Serve a PostgreSQL-protocol port that runs read-only SQL on the copy of a PostgreSQL server, as a service of its own",
	Long: `Starts a PostgreSQL-protocol port that PostgreSQL clients connect to. Each
statement is answered by the copy of the server the client named, by
PostgreSQL's rules; nothing is sent to the source, and a statement that is not
a read is refused.

It runs beside 'bintrail-console watch' and reads the servers the web
interface manages, never writing them. Connect with the server's id or name as
the database (dbname), any username, and the password created for SQL clients
in the web interface (or the access token). The port is open while that
password exists, and closes with every connection when it is withdrawn.

It serves PostgreSQL servers only. It asks for the password in clear text and
has no TLS, so it listens on this machine alone (127.0.0.1, ::1, localhost).

How many statements run at once (--workers), with how many threads each
(--threads) and how much memory each (--sql-memory), is what is said here: the
defaults are small, so this process fits beside capture without asking.

Example:
  bintrail-console pg-router --listen 127.0.0.1:5433
  psql "host=127.0.0.1 port=5433 dbname=<server id or name> user=app"`,
	Args: cobra.NoArgs,
	RunE: runPGRouter,
}

const (
	defaultPGRouterListen = "127.0.0.1:5433"
	// Small on purpose: nobody sized this process for the machine, and it
	// runs beside the one that captures.
	defaultPGRouterWorkers  = 2
	defaultPGRouterThreads  = 2
	defaultPGRouterMaxConns = 16
	maxPGRouterWorkers      = 16
	maxPGRouterThreads      = 16
	maxPGRouterMaxConns     = 1024
)

func init() {
	addPGRouterFlags(pgRouterCmd)
	rootCmd.AddCommand(pgRouterCmd)
}

func addPGRouterFlags(cmd *cobra.Command) {
	f := cmd.Flags()
	f.String("listen", defaultPGRouterListen, "Address the PostgreSQL-protocol port listens on (host:port). An address of this machine only (127.0.0.1, ::1, localhost): the password is asked for in clear text and there is no TLS. Env BINTRAIL_PG_ROUTER_LISTEN.")
	f.String("token", "", "Access token: one more password for the port, beside the one created in the web interface. It does not open the port. Env BINTRAIL_CONSOLE_TOKEN.")
	f.String("servers-file", "", "Path to the server registry the web interface manages (default ~/.config/bintrail/console-servers.yaml). Read, never written. Env BINTRAIL_CONSOLE_SERVERS.")
	f.String("auth-file", "", "Path to the web interface auth file, when it is not the default: only so a statement cannot read it through a copy directory that contains it. Env BINTRAIL_CONSOLE_AUTH.")
	f.String("mcp-token-file", "", "Path to the managed MCP token file, when it is not the default: for the same reason as --auth-file. Env BINTRAIL_CONSOLE_MCP_TOKEN_FILE.")
	f.String("sql-memory", "", "Memory each statement on the copy may use, e.g. 4GB; default 2GB, at least 512MB. Unset, the memory saved in the web interface is used, read when this starts. Env BINTRAIL_CONSOLE_SQL_MEMORY.")
	f.Int("workers", defaultPGRouterWorkers, fmt.Sprintf("How many statements run on the copy at once, 1 to %d. The rest wait their turn. Env BINTRAIL_PG_ROUTER_WORKERS.", maxPGRouterWorkers))
	f.Int("threads", defaultPGRouterThreads, fmt.Sprintf("How many threads each statement may use, 1 to %d. Env BINTRAIL_PG_ROUTER_THREADS.", maxPGRouterThreads))
	f.Int("max-connections", defaultPGRouterMaxConns, fmt.Sprintf("How many clients may be connected at once, 1 to %d; one more is refused with an error. Env BINTRAIL_PG_ROUTER_MAX_CONNECTIONS.", maxPGRouterMaxConns))
	f.String("max-wait", "", "How long a statement waits for its turn when every worker is busy, e.g. 30s or 2m; past it the client gets an error. Empty or 0: it waits for as long as its client stays connected. Env BINTRAIL_PG_ROUTER_MAX_WAIT.")
	f.Int("sql-port-max-rows", console.DefaultSQLPortMaxRows, "Most rows one statement returns; a result with more is refused, not cut. Env BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS.")
}

// pgRouterSettings is what pg-router starts with, flags and environment
// resolved.
type pgRouterSettings struct {
	Listen         string
	Token          string
	ServersFile    string
	AuthFile       string
	MCPTokenFile   string
	SQLMemory      string
	Workers        int
	Threads        int
	MaxConns       int
	MaxWait        time.Duration
	SQLPortMaxRows int
}

// pgRouterSettingsFrom resolves the flags with the environment, a flag
// winning over a variable, and refuses what cannot mean what was typed.
func pgRouterSettingsFrom(cmd *cobra.Command) (pgRouterSettings, error) {
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
	// num reads a whole number from the flag, or from the variable when the
	// flag was not given, and holds it between min and max. The error names
	// where the value came from.
	num := func(flag, env string, min, max int, what string) (int, error) {
		n, _ := f.GetInt(flag)
		name := "--" + flag
		if !f.Changed(flag) {
			if v := os.Getenv(env); v != "" {
				name = env
				var err error
				if n, err = strconv.Atoi(strings.TrimSpace(v)); err != nil {
					return 0, fmt.Errorf("%s=%q is not a whole number: it is %s", name, v, what)
				}
			}
		}
		if n < min || n > max {
			return 0, fmt.Errorf("%s must be between %d and %d: it is %s", name, min, max, what)
		}
		return n, nil
	}
	st := pgRouterSettings{
		Listen:       str("listen", "BINTRAIL_PG_ROUTER_LISTEN"),
		Token:        str("token", "BINTRAIL_CONSOLE_TOKEN"),
		ServersFile:  str("servers-file", "BINTRAIL_CONSOLE_SERVERS"),
		AuthFile:     str("auth-file", "BINTRAIL_CONSOLE_AUTH"),
		MCPTokenFile: str("mcp-token-file", "BINTRAIL_CONSOLE_MCP_TOKEN_FILE"),
	}
	if st.ServersFile == "" {
		st.ServersFile = console.DefaultRegistryPath()
	}
	if err := validPGListen(st.Listen); err != nil {
		return st, err
	}
	var err error
	if st.SQLMemory, err = sqlMemoryFrom(cmd); err != nil {
		return st, err
	}
	if st.Workers, err = num("workers", "BINTRAIL_PG_ROUTER_WORKERS", 1, maxPGRouterWorkers, "how many statements run on the copy at once"); err != nil {
		return st, err
	}
	if st.Threads, err = num("threads", "BINTRAIL_PG_ROUTER_THREADS", 1, maxPGRouterThreads, "how many threads each statement may use"); err != nil {
		return st, err
	}
	if st.MaxConns, err = num("max-connections", "BINTRAIL_PG_ROUTER_MAX_CONNECTIONS", 1, maxPGRouterMaxConns, "how many clients may be connected at once"); err != nil {
		return st, err
	}
	if st.MaxWait, err = parseRouterMaxWait(str("max-wait", "BINTRAIL_PG_ROUTER_MAX_WAIT")); err != nil {
		return st, err
	}
	if st.SQLPortMaxRows, err = num("sql-port-max-rows", "BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS", 1, 1<<31-1, "the most rows one statement returns"); err != nil {
		return st, err
	}
	return st, nil
}

// pgRouterStartupLines says, at start-up, where the port will be, how a
// client reaches a server on it, and what this process may use.
func pgRouterStartupLines(st pgRouterSettings, memory, memoryFrom string, memoryErr error, servers int, serversExist bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "PostgreSQL router: the PostgreSQL-protocol port is %s; connect a PostgreSQL client with dbname=<server id or name>, any user, and the password created for SQL clients in the web interface", st.Listen)
	if st.Token != "" {
		b.WriteString(" (the access token is accepted too)")
	}
	b.WriteString(".\n")
	b.WriteString("PostgreSQL router: it serves PostgreSQL servers only. The copy answers every statement, read-only, and nothing is sent to the source.\n")
	b.WriteString("PostgreSQL router: the password is asked for in clear text and there is no TLS, which is why the port listens on this machine only.\n")
	if !serversExist {
		fmt.Fprintf(&b, "PostgreSQL router: the servers file %s does not exist yet, so no server can be reached. It is read again when it appears; if DBTrail keeps it somewhere else, give that path with --servers-file.\n", st.ServersFile)
	} else {
		fmt.Fprintf(&b, "PostgreSQL router: %s from %s, read again when it changes.\n", countOfServers(servers), st.ServersFile)
	}
	fmt.Fprintf(&b, "PostgreSQL router: %s on the copy at once, %s and up to %s of memory each (%s), and at most %d clients connected. These are set where this is started (--workers, --threads, --sql-memory, --max-connections); nothing is worked out from the machine.\n",
		countOfStatements(st.Workers), countOfThreads(st.Threads), memory, memoryFrom, st.MaxConns)
	if memoryErr != nil {
		fmt.Fprintf(&b, "PostgreSQL router: the SQL memory saved in the web interface could NOT be read, so the default is used: %v\n", memoryErr)
	}
	return b.String()
}

func runPGRouter(cmd *cobra.Command, _ []string) error {
	cli.LoadEnvFile()
	st, err := pgRouterSettingsFrom(cmd)
	if err != nil {
		return err
	}
	// The MySQL router's own start-up pieces, called as they are (router.go
	// is not edited for this command): the saved SQL memory, and the console
	// that follows the daemon's files with a pool of the size given.
	shared := routerSettings{
		Token: st.Token, ServersFile: st.ServersFile, AuthFile: st.AuthFile, MCPTokenFile: st.MCPTokenFile,
		SQLMemory: st.SQLMemory, SQLPortMaxRows: st.SQLPortMaxRows, MaxWait: st.MaxWait,
	}
	memoryFrom, memoryErr := routerMemory(&shared, sqlSettingsFilePath(st.ServersFile))
	memory := shared.SQLMemory
	if memory == "" {
		memory = sqlsandbox.DefaultLimits().MemoryLimit
	}
	srv, runner, err := newRouterConsole(shared, console.SQLPool{Workers: st.Workers, Threads: st.Threads})
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
	// What each answer's lag is measured against is placed here, on a clock,
	// never by a statement, so what waits on it does not pile up.
	go resolveCopyAgesEvery(ctx, srv.ResolveCopyAges, routerCopySnapshotEvery)

	_, statErr := os.Stat(st.ServersFile)
	fmt.Fprint(os.Stderr, pgRouterStartupLines(st, memory, memoryFrom, memoryErr, srv.ServerCount(), statErr == nil))
	port := &pgRouterPort{srv: srv, addr: st.Listen, maxConns: st.MaxConns, poll: console.DefaultFollowInterval, out: os.Stderr}
	return port.serve(ctx)
}

// pgRouterPort is the listener over time: open while a password that could
// open it exists, closed with every connection on it when the password from
// the web interface is withdrawn. The same rule as the MySQL router's port,
// in this process's own loop.
type pgRouterPort struct {
	srv      *console.Server
	addr     string
	maxConns int
	// poll is how often it looks for a password while it has none.
	poll time.Duration
	out  io.Writer

	mu    sync.Mutex
	bound string
	stop  context.CancelFunc
}

// listening is the address the port is bound on now, "" while it is closed.
func (p *pgRouterPort) listening() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bound
}

// open says whether the port is to be open now: there is a password to take.
func (p *pgRouterPort) open() bool { return len(p.srv.FlashbackPasswords()) > 0 }

// serve runs the port until ctx ends. It returns an error only when the
// address cannot be bound the first time: a process that never listened must
// not stay up looking like one that does. After that a failure to bind again
// is logged once and tried again every second.
func (p *pgRouterPort) serve(ctx context.Context) error {
	p.srv.OnFlashbackPasswordWithdrawn(func() {
		// Every connection: the port cannot tell which of them came in with
		// the password that is gone.
		p.mu.Lock()
		if p.stop != nil {
			p.stop()
		}
		p.mu.Unlock()
	})
	// Before waiting for a password: an address that cannot be bound is said
	// now, not on the day a password is created.
	probe, err := net.Listen("tcp", p.addr)
	if err != nil {
		return fmt.Errorf("the PostgreSQL router cannot listen on %s: %w", p.addr, err)
	}
	_ = probe.Close()
	opened, waiting, saidRebind := false, false, false
	for ctx.Err() == nil {
		if !p.open() {
			if !waiting {
				waiting = true
				fmt.Fprintf(p.out, "The PostgreSQL router is not listening yet: no password for SQL clients exists. Create it with `dbtrail port on`, or in the web interface (Settings, Connect a SQL client): %s opens here within a few seconds, without a restart. An access token (--token, BINTRAIL_CONSOLE_TOKEN) is one more password while the port is on; it does not open it.\n", p.addr)
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
				return fmt.Errorf("the PostgreSQL router cannot listen on %s: %w", p.addr, err)
			}
			if !saidRebind {
				saidRebind = true
				slog.Error("PostgreSQL router: the port could not be opened again and it is NOT listening; it keeps trying", "listen", p.addr, "error", err)
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
		slog.Info("PostgreSQL router: the port is open", "listen", ln.Addr().String())
		err = servePGPort(child, p.srv, ln, p.maxConns)
		cancel()
		p.mu.Lock()
		p.bound, p.stop = "", nil
		p.mu.Unlock()
		if err != nil && ctx.Err() == nil && p.open() {
			slog.Warn("PostgreSQL router: the port stopped; opening it again", "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}

// pgRouterQueryTimeout bounds each statement end to end, a wait for a worker
// included: the same five minutes as the MySQL port's.
const pgRouterQueryTimeout = defaultFlashbackQueryTimeout

// pgPortSchema is where a PostgreSQL client's unqualified names resolve on
// the copy of a PostgreSQL source: PostgreSQL's own default.
const pgPortSchema = "public"

// validPGListen refuses an address the PostgreSQL port must not take. The
// port asks for its password in clear text and has no TLS, so it listens on
// this machine alone: an address other machines reach would put the port's
// password on the network for anyone between them to read.
func validPGListen(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("--listen %q is not an address: want host:port, e.g. 127.0.0.1:5433", addr)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("--listen %q: %q is not a port number (1 to 65535)", addr, port)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("--listen %q: the PostgreSQL port listens on this machine only (127.0.0.1, ::1 or localhost); "+
		"it asks for its password in clear text and has no TLS, so an address other machines reach would show that password to the network", addr)
}

// servePGPort serves the PostgreSQL-protocol port on ln until ctx ends, with
// at most maxConns clients connected (0 for no limit).
func servePGPort(ctx context.Context, srv *console.Server, ln net.Listener, maxConns int) error {
	logger := slog.Default()
	// Accept returns when the listener is closed; the connections end with
	// ctx (pgshim closes each one's socket), and Serve waits for them.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	return pgshim.Serve(ctx, ln, pgshim.Config{Login: pgPortLogin(srv, pgRouterQueryTimeout), Logger: logger, MaxConns: maxConns})
}

// pgPortLogin is the port's login: the port's passwords, then the server.
//
// The password is checked before anything is said about the server, and a
// wrong password reads the same for every username and database, so a client
// that holds no password learns nothing about which servers exist.
func pgPortLogin(srv *console.Server, timeout time.Duration) pgshim.LoginFunc {
	return func(ctx context.Context, user, database, password string, drop func()) (*pgshim.Binding, *pgshim.Error) {
		// Before the passwords are read: a connection opened with an
		// application login is served only if no login changed since
		// (console.Server.LoginEpoch).
		epoch := srv.LoginEpoch()
		// One reading for the passwords and the server a login opens
		// (console.Server.FlashbackPasswordsFor): the connection is bound to
		// that server, never to what the username resolves to later.
		passwords, loginServer := srv.FlashbackPasswordsFor(user)
		if !passwordAccepted(passwords, password) {
			return nil, &pgshim.Error{Code: "28P01", Message: fmt.Sprintf("password authentication failed for user %q", user)}
		}
		var tgt console.FlashbackTarget
		var err error
		if loginServer != "" {
			tgt, err = srv.ResolveFlashbackLogin(ctx, loginServer)
		} else {
			tgt, err = srv.ResolveFlashback(ctx, database)
		}
		switch {
		case err == nil:
		case errors.Is(err, console.ErrUnknownServer) && loginServer != "":
			return nil, &pgshim.Error{Code: "3D000", Message: "the server of this application login was removed while connecting"}
		case errors.Is(err, console.ErrUnknownServer):
			return nil, &pgshim.Error{Code: "3D000", Message: fmt.Sprintf(
				"database %q does not exist: on this port the database name picks the server, so connect with dbname=<server id or name>", database)}
		default:
			// The connection manager already took the secrets out of an
			// open error's text.
			named := database
			if loginServer != "" {
				// The server the login opens, not whatever database the
				// client typed beside it.
				named = loginServer
			}
			return nil, &pgshim.Error{Code: "08006", Message: fmt.Sprintf("cannot open server %q: %s", named, err)}
		}
		if !srv.SourceIsPostgres(tgt.ID) {
			// One port per family: the copy of a MySQL or MariaDB server is
			// read through the MySQL port, by MySQL's rules. Answering it
			// here would be a path nothing tests. Said after the password
			// passed, like every other fact about a server.
			return nil, &pgshim.Error{Code: "0A000", Message: fmt.Sprintf(
				"server %q is a MySQL or MariaDB server, and this port serves PostgreSQL servers; connect a MySQL client to the MySQL port for it", tgt.ID)}
		}
		bound := &pgshim.Binding{Actor: "server:" + tgt.ID}
		if tgt.ViaLogin {
			// Tracked, so that changing or removing the login closes the
			// connection it opened (console.Server.TrackLoginConn).
			untrack, ok := srv.TrackLoginConn(tgt.ID, epoch, drop)
			if !ok {
				return nil, &pgshim.Error{Code: "57P03", Message: "an application login changed while connecting; reconnect"}
			}
			bound.Close = untrack
		}
		if tgt.SQL == nil {
			// The connection is made and every statement says why it cannot
			// be answered: the reason is about the server, and a client that
			// could not connect at all would not be shown it.
			why := tgt.SQLUnavailable
			bound.Run = func(context.Context, string) (sqlsandbox.Result, error) {
				return sqlsandbox.Result{}, &pgshim.Error{Code: "55000", Message: "SQL on the copy is not available for this server: " + why}
			}
			return bound, nil
		}
		copySQL := tgt.SQL
		// A PostgreSQL source's copy answers by PostgreSQL's rules, with its
		// names resolved where PostgreSQL resolves them.
		sess, schema := sqlsandbox.Session{PostgresRules: true}, pgPortSchema
		bound.Schema = schema
		bound.Answered = copySQL.NoteAnswered
		bound.Run = func(ctx context.Context, statement string) (sqlsandbox.Result, error) {
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			return copySQL.Run(ctx, statement, schema, sess)
		}
		return bound, nil
	}
}

// passwordAccepted says whether given is one of the passwords that open the
// port. An empty password opens nothing, whatever the list holds, and every
// entry is compared, in constant time, so the answer's timing does not say
// which one matched or how much of it.
func passwordAccepted(passwords []string, given string) bool {
	if given == "" {
		return false
	}
	ok := 0
	for _, p := range passwords {
		if p == "" {
			continue
		}
		ok |= subtle.ConstantTimeCompare([]byte(p), []byte(given))
	}
	return ok == 1
}
