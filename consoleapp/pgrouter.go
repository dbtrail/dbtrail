package consoleapp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/streamrun"
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
// password or the port's, whatever database it names. With the server's
// forwarding account (#2771), each statement is answered by the copy only
// when the copy is known to answer it as PostgreSQL does, and by the source
// otherwise (#2770, pgshim route.go); a transaction block the client opens
// and a catalog statement the copy does not answer go to the source too. The
// source is reached as the forwarding account, never as the account DBTrail
// captures with, and nothing that writes is sent. A server without a
// forwarding account is answered from the copy alone, as before. Time travel (_flashback,
// _snapshot, AS OF for one row) is answered on the same connection from the
// server's index and snapshots, unless the server hides rows or columns:
// time travel reads past the views that hide them.
var pgRouterCmd = &cobra.Command{
	Use:   "pg-router",
	Short: "Serve a PostgreSQL-protocol port that runs read-only SQL on the copy of a PostgreSQL server, as a service of its own",
	Long: `Starts a PostgreSQL-protocol port that PostgreSQL clients connect to. When
the server the client named has a forwarding user (bintrail-console server
forward-user), each statement is answered by the server's copy only when the
copy is known to give the answer PostgreSQL gives, and by the server's own
PostgreSQL otherwise: a function, operator or construct the copy answers
differently or not at all, the clock and the session (now(), current_user),
text sorted or compared by order, a table the copy does not hold, a statement
the copy refuses. A transaction block the client opens (BEGIN to COMMIT or
ROLLBACK), and a statement on the system catalog the copy does not answer, go
there too. The source is reached as the forwarding user, never as the user
DBTrail captures with; a server without one is answered from the copy alone.
Nothing that writes is sent: a statement that is not a read is refused, and
the connection to the source is read-only.

With --status-addr, http://<address>/status says, per server, how many
statements each side answered and why; --status-statements adds a row per
statement shape (values blanked, table and column names kept). That address
asks for no password.

It runs beside 'bintrail-console watch' and reads the servers the web
interface manages, never writing them. Connect with the server's id or name as
the database (dbname), any username, and the password created for SQL clients
in the web interface (or the access token). The port is open while that
password exists, and closes with every connection when it is withdrawn.

It serves PostgreSQL servers only. The login is SCRAM-SHA-256, so the password
never crosses the network, and a client that asks for TLS gets it: with the
certificate of --tls-cert and --tls-key, which a client can verify
(sslmode=verify-ca or verify-full), or with one made up at start, which it
cannot. A client that asks for no TLS is answered in clear text: sslmode=require
or stricter on the client is what makes it encrypted or nothing.

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
	// Nobody sized this process for the machine, and it runs beside the one
	// that captures: four statements of two threads fill the 8 cores of the
	// recommended machine and leave capture its share of the minimum one. The
	// clients are cheap, waiting for a worker: 16 refused the 50 of a
	// measured dashboard load.
	defaultPGRouterWorkers  = 4
	defaultPGRouterThreads  = 2
	defaultPGRouterMaxConns = 256
	maxPGRouterWorkers      = 16
	maxPGRouterThreads      = 16
	maxPGRouterMaxConns     = 1024
	// At most one connection to the source per client connection, held
	// while its block runs there: at most this many at once, across every
	// server, so the port cannot take the sources' own connection slots.
	defaultPGRouterMaxSourceConns = 16
	maxPGRouterMaxSourceConns     = 1024
)

func init() {
	addPGRouterFlags(pgRouterCmd)
	rootCmd.AddCommand(pgRouterCmd)
}

func addPGRouterFlags(cmd *cobra.Command) {
	f := cmd.Flags()
	f.String("listen", defaultPGRouterListen, "Address the PostgreSQL-protocol port listens on (host:port). Env BINTRAIL_PG_ROUTER_LISTEN.")
	f.String("tls-cert", "", "Certificate file (PEM, its chain after it if it has one) the PostgreSQL port offers to clients that ask for TLS, so they can verify it (sslmode=verify-ca or verify-full); the MySQL port's (router --tls-cert) serves here as it is. Requires --tls-key. Unset, the port still encrypts for a client that asks, with a certificate made up at start that no client can verify. Read when this starts: restart it to serve a renewed certificate. Env BINTRAIL_PG_ROUTER_TLS_CERT.")
	f.String("tls-key", "", "Private key file (PEM, not encrypted) of --tls-cert. Requires --tls-cert. Env BINTRAIL_PG_ROUTER_TLS_KEY.")
	f.String("token", "", "Access token: one more password for the port, beside the one created in the web interface. It does not open the port. Env BINTRAIL_CONSOLE_TOKEN.")
	f.String("servers-file", "", "Path to the server registry the web interface manages (default ~/.config/bintrail/console-servers.yaml). Read, never written. Env BINTRAIL_CONSOLE_SERVERS.")
	f.String("auth-file", "", "Path to the web interface auth file, when it is not the default: only so a statement cannot read it through a copy directory that contains it. Env BINTRAIL_CONSOLE_AUTH.")
	f.String("mcp-token-file", "", "Path to the managed MCP token file, when it is not the default: for the same reason as --auth-file. Env BINTRAIL_CONSOLE_MCP_TOKEN_FILE.")
	f.String("sql-memory", "", "Memory each statement on the copy may use, e.g. 4GB; default 2GB, at least 512MB. Unset, the memory saved in the web interface is used, read when this starts. Env BINTRAIL_CONSOLE_SQL_MEMORY.")
	f.Int("workers", defaultPGRouterWorkers, fmt.Sprintf("How many statements run on the copy at once, 1 to %d. The rest wait their turn. Env BINTRAIL_PG_ROUTER_WORKERS.", maxPGRouterWorkers))
	f.Int("threads", defaultPGRouterThreads, fmt.Sprintf("How many threads each statement may use, 1 to %d. Env BINTRAIL_PG_ROUTER_THREADS.", maxPGRouterThreads))
	f.Int("max-connections", defaultPGRouterMaxConns, fmt.Sprintf("How many clients may be connected at once, 1 to %d; one more is refused with an error. Env BINTRAIL_PG_ROUTER_MAX_CONNECTIONS.", maxPGRouterMaxConns))
	f.Int("max-source-connections", defaultPGRouterMaxSourceConns, fmt.Sprintf("How many connections to the servers' own PostgreSQL this process holds at once, across every server, 1 to %d. A client holds one while a statement the source answers, or its transaction block, runs there; past the limit, the statement that needed one waits for one to be given back, up to 30 seconds, and is then refused with an error (53300). Env BINTRAIL_PG_ROUTER_MAX_SOURCE_CONNECTIONS.", maxPGRouterMaxSourceConns))
	f.String("status-addr", "", "Address to serve the port's status on (host:port, e.g. 127.0.0.1:9092): /status, as JSON, says per server which side answered each statement and why, and /metrics this process's Prometheus metrics. Empty: none. It asks for no password. Env BINTRAIL_PG_ROUTER_STATUS_ADDR.")
	f.Bool("status-statements", false, "Keep a row per statement shape and side in /status (values blanked, table and column names kept): whoever reaches --status-addr then reads the names of the tables and columns your clients query. Env BINTRAIL_PG_ROUTER_STATUS_STATEMENTS.")
	f.Float64("cost-threshold", pgshim.DefaultCostThreshold, "With a server's forwarding user, a statement the copy answers as PostgreSQL does goes to the copy only when it is expensive on the source: when the Total Cost at the top of its plan (EXPLAIN on the source, before it runs) is at least this, or a sequential scan in the plan is estimated at --scan-rows rows or more; otherwise the source answers it, by its indexes (a read by a primary key costs about 1 to 12). 0 turns this rule off. Env BINTRAIL_PG_ROUTER_COST_THRESHOLD.")
	f.Int64("scan-rows", pgshim.DefaultScanRows, "With a server's forwarding user, a statement the copy answers as PostgreSQL does goes to the copy when a sequential scan in its plan on the source (any Seq Scan node, at any depth) is estimated at this many rows or more, whatever its cost. 0 turns this rule off. Env BINTRAIL_PG_ROUTER_SCAN_ROWS.")
	f.String("max-wait", "", "How long a statement waits for its turn when every worker is busy, e.g. 30s or 2m; past it the client gets an error (53000) to run it again, and the statement is not sent to the source. Empty or 0: no limit of its own, and the five minutes one statement may take, its wait included, is what ends it. Env BINTRAIL_PG_ROUTER_MAX_WAIT.")
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
	MaxSourceConns int
	MaxWait        time.Duration
	SQLPortMaxRows int
	// StatusAddr is where /status is served (#2770), "" for nowhere;
	// StatusStatements keeps its per-statement table.
	StatusAddr       string
	StatusStatements bool
	// Cost is when a statement the copy answers as PostgreSQL does goes
	// there rather than to the source (--cost-threshold,
	// --scan-rows).
	Cost pgshim.CostPolicy
	// TLS is the operator's certificate (--tls-cert, --tls-key), nil when
	// none is set: the port then serves one made up at start.
	TLS *shim.PortTLS
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
	listenName := "--listen"
	if !f.Changed("listen") && os.Getenv("BINTRAIL_PG_ROUTER_LISTEN") != "" {
		listenName = "BINTRAIL_PG_ROUTER_LISTEN"
	}
	if err := validPGListen(st.Listen, listenName); err != nil {
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
	if st.MaxSourceConns, err = num("max-source-connections", "BINTRAIL_PG_ROUTER_MAX_SOURCE_CONNECTIONS", 1, maxPGRouterMaxSourceConns, "how many connections to the sources this process holds at once"); err != nil {
		return st, err
	}
	if st.MaxWait, err = parseRouterMaxWait(str("max-wait", "BINTRAIL_PG_ROUTER_MAX_WAIT")); err != nil {
		return st, err
	}
	if st.SQLPortMaxRows, err = num("sql-port-max-rows", "BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS", 1, 1<<31-1, "the most rows one statement returns"); err != nil {
		return st, err
	}
	if st.StatusAddr = str("status-addr", "BINTRAIL_PG_ROUTER_STATUS_ADDR"); st.StatusAddr != "" {
		name := "--status-addr"
		if !f.Changed("status-addr") {
			name = "BINTRAIL_PG_ROUTER_STATUS_ADDR"
		}
		if err := validPGListen(st.StatusAddr, name); err != nil {
			return st, err
		}
	}
	if st.Cost, err = pgRouterCostFrom(cmd); err != nil {
		return st, err
	}
	st.StatusStatements, _ = f.GetBool("status-statements")
	if !f.Changed("status-statements") {
		if v := os.Getenv("BINTRAIL_PG_ROUTER_STATUS_STATEMENTS"); v != "" {
			if st.StatusStatements, err = strconv.ParseBool(strings.TrimSpace(v)); err != nil {
				return st, fmt.Errorf("BINTRAIL_PG_ROUTER_STATUS_STATEMENTS=%q is not true or false", v)
			}
		}
	}
	// named is str with the name the value came from, for a refusal: the
	// flag when it was given, else the variable when it is set.
	named := func(flag, env string) (name, v string) {
		v = str(flag, env)
		switch {
		case f.Changed(flag):
			return "--" + flag, v
		case os.Getenv(env) != "":
			return env, v
		}
		return "--" + flag + " (" + env + ")", v
	}
	certName, cert := named("tls-cert", "BINTRAIL_PG_ROUTER_TLS_CERT")
	keyName, key := named("tls-key", "BINTRAIL_PG_ROUTER_TLS_KEY")
	// Loaded the way the MySQL port loads its own, and refused for the same
	// things: a port that silently served a made-up certificate instead of
	// the one set would look encrypted and fail every client that verifies.
	if st.TLS, err = portTLSFiles("the PostgreSQL port", certName, cert, keyName, key); err != nil {
		return st, err
	}
	return st, nil
}

// pgVerifying is how a PostgreSQL client verifies the port's certificate.
const pgVerifying = "sslmode=verify-ca or verify-full"

// pgRouterTLSLines says at start-up what a client that asks the port for TLS
// gets, and that one that asks for none is answered in clear text: the port
// offers TLS and never requires it, as the MySQL port does. t nil is the
// certificate made up at start.
func pgRouterTLSLines(t *shim.PortTLS, now time.Time) string {
	if now.IsZero() {
		now = time.Now()
	}
	var b strings.Builder
	b.WriteString("PostgreSQL router: the login is SCRAM-SHA-256: the password never crosses the network, with TLS or without.\n")
	if t == nil {
		b.WriteString("PostgreSQL router: TLS: no certificate is set (--tls-cert and --tls-key, or BINTRAIL_PG_ROUTER_TLS_CERT and BINTRAIL_PG_ROUTER_TLS_KEY). A client that asks for TLS gets it, with a certificate made up at start that it cannot verify: the rows are encrypted, and a client that sets channel_binding=require refuses something that relays between it and the port, but verify-ca and verify-full refuse that certificate. A client that asks for none is answered in clear text.\n")
		return b.String()
	}
	fmt.Fprintf(&b, "PostgreSQL router: TLS: a client that asks for it gets the certificate %s (valid until %s); to verify it, the client needs the CA that signed it (%s, with sslrootcert). A client that asks for no TLS is still answered, in clear text: sslmode=require or stricter on the client is what makes it encrypted or nothing. A renewed certificate is served after this restarts.\n",
		t.CertFile, t.NotAfter.UTC().Format("2006-01-02"), pgVerifying)
	if p := t.ValidityProblemFor(now, pgVerifying); p != "" {
		b.WriteString("PostgreSQL router: WARNING: " + p + ".\n")
	}
	if !pgshim.ChannelBindingOffered(t.Config) {
		b.WriteString("PostgreSQL router: WARNING: channel binding (SCRAM-SHA-256-PLUS) is not offered with this certificate: its signature names no hash for it (an Ed25519 certificate, for one). A client with channel_binding=require cannot connect; one with the default (prefer) logs in with SCRAM-SHA-256.\n")
	}
	return b.String()
}

// pgRouterAddressLine says, for an address other machines reach, what they
// reach; "" for an address of this machine.
func pgRouterAddressLine(listen string, container bool) string {
	host, _, _ := net.SplitHostPort(listen)
	if host == "localhost" {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return ""
	}
	if container {
		return "PostgreSQL router: that address is reached from outside this container as the container publishes it. Whoever reaches it can try the port's password, and reads the copy of every PostgreSQL server with it.\n"
	}
	return "PostgreSQL router: that address is reached from other machines. Whoever reaches it can try the port's password, and reads the copy of every PostgreSQL server with it; keep it behind a firewall.\n"
}

// pgRouterCostFrom reads --cost-threshold and --scan-rows, or
// their variables, and refuses what no plan can be compared with.
func pgRouterCostFrom(cmd *cobra.Command) (pgshim.CostPolicy, error) {
	f := cmd.Flags()
	var pol pgshim.CostPolicy
	costName, rowsName := "--cost-threshold", "--scan-rows"
	pol.CostThreshold, _ = f.GetFloat64("cost-threshold")
	if !f.Changed("cost-threshold") {
		if v := os.Getenv("BINTRAIL_PG_ROUTER_COST_THRESHOLD"); v != "" {
			costName = "BINTRAIL_PG_ROUTER_COST_THRESHOLD"
			var err error
			if pol.CostThreshold, err = strconv.ParseFloat(strings.TrimSpace(v), 64); err != nil {
				return pol, fmt.Errorf("%s=%q is not a number: it is the plan cost from which a statement goes to the copy", costName, v)
			}
		}
	}
	pol.ScanRows, _ = f.GetInt64("scan-rows")
	if !f.Changed("scan-rows") {
		if v := os.Getenv("BINTRAIL_PG_ROUTER_SCAN_ROWS"); v != "" {
			rowsName = "BINTRAIL_PG_ROUTER_SCAN_ROWS"
			var err error
			if pol.ScanRows, err = strconv.ParseInt(strings.TrimSpace(v), 10, 64); err != nil {
				return pol, fmt.Errorf("%s=%q is not a whole number: it is the rows of a sequential scan from which a statement goes to the copy", rowsName, v)
			}
		}
	}
	switch {
	case math.IsNaN(pol.CostThreshold) || math.IsInf(pol.CostThreshold, 0):
		return pol, fmt.Errorf("%s must be a number a plan's cost can be compared with, and %v is not one", costName, pol.CostThreshold)
	case pol.CostThreshold < 0:
		return pol, fmt.Errorf("%s must be 0 or more: it is %v", costName, pol.CostThreshold)
	case pol.ScanRows < 0:
		return pol, fmt.Errorf("%s must be 0 or more: it is %d", rowsName, pol.ScanRows)
	case pol.CostThreshold == 0 && pol.ScanRows == 0:
		return pol, fmt.Errorf("%s and %s are both 0, so no statement could ever go to the copy through a forwarding user; set one of them", costName, rowsName)
	}
	return pol, nil
}

// pgCostRuleWords says when a statement goes to the copy by its plan, for
// the startup lines.
func pgCostRuleWords(pol pgshim.CostPolicy) string {
	costWords := fmt.Sprintf("%g", pol.CostThreshold)
	if pol.CostThreshold < 1e15 {
		costWords = groupThousands(int64(math.Ceil(pol.CostThreshold)))
	}
	cost := fmt.Sprintf("its plan there (EXPLAIN) costs %s or more (--cost-threshold)", costWords)
	scan := fmt.Sprintf("its plan there (EXPLAIN) has a sequential scan of %s rows or more (--scan-rows)", groupThousands(pol.ScanRows))
	switch {
	case pol.CostThreshold > 0 && pol.ScanRows > 0:
		return cost + ", or has a sequential scan of " + groupThousands(pol.ScanRows) + " rows or more (--scan-rows)"
	case pol.CostThreshold > 0:
		return cost + "; the rule on sequential scans is off (--scan-rows 0)"
	case pol.ScanRows > 0:
		return scan + "; the rule on cost is off (--cost-threshold 0)"
	}
	return "no rule is set, and every statement the copy can answer goes to the copy"
}

// groupThousands writes n with a comma between each three digits.
func groupThousands(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
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
	b.WriteString("PostgreSQL router: it serves PostgreSQL servers only. The copy answers reads, in each server's mode (fresh or fast, said below).\n")
	fmt.Fprintf(&b, "PostgreSQL router: with a server's forwarding user (bintrail-console server forward-user), each statement is answered by the copy only when the copy is known to answer it as PostgreSQL does, and by the server's own PostgreSQL otherwise, as well as a transaction block a client opens; read-only, at most %d connections there at once (--max-source-connections). Of what the copy can answer, only a statement that is expensive on the source goes to the copy, which has no index: %s. A server without a forwarding user is answered from the copy alone, unless it allows forwarding with the user DBTrail connects with (bintrail-console server forward-user <server> --capture-account): read-only, and only a SELECT whose every table is a table of the copy.\n", st.MaxSourceConns, pgCostRuleWords(st.Cost))
	switch {
	case st.StatusAddr != "" && st.StatusStatements:
		fmt.Fprintf(&b, "PostgreSQL router: http://%s/status says which side answered each statement and why, with a row per statement shape, which names tables and columns (values are blanked); that address asks for no password.\n", st.StatusAddr)
	case st.StatusAddr != "":
		fmt.Fprintf(&b, "PostgreSQL router: http://%s/status says, per server, how many statements each side answered and why; that address asks for no password.\n", st.StatusAddr)
	case st.StatusStatements:
		b.WriteString("PostgreSQL router: --status-statements is set but --status-addr is not, so the per-statement table is not served anywhere.\n")
	}
	b.WriteString(pgRouterAddressLine(st.Listen, inContainer()))
	b.WriteString(pgRouterTLSLines(st.TLS, time.Time{}))
	if !serversExist {
		fmt.Fprintf(&b, "PostgreSQL router: the servers file %s does not exist yet, so no server can be reached. It is read again when it appears; if DBTrail keeps it somewhere else, give that path with --servers-file.\n", st.ServersFile)
	} else {
		fmt.Fprintf(&b, "PostgreSQL router: %s listed in %s, read again when it changes; the PostgreSQL ones are served here.\n", countOfServers(servers), st.ServersFile)
	}
	fmt.Fprintf(&b, "PostgreSQL router: %s on the copy at once, %s and up to %s of memory each (%s), and at most %d clients connected. These are set where this is started (--workers, --threads, --sql-memory, --max-connections); nothing is worked out from the machine.\n",
		countOfStatements(st.Workers), countOfThreads(st.Threads), memory, memoryFrom, st.MaxConns)
	fmt.Fprintf(&b, "PostgreSQL router: the results being sent to clients are held in memory up to %d MB in all; past that a statement is refused with an error (53000) until clients have read their answers.\n", int64(defaultFlashbackResultBudget)>>20)
	if memoryErr != nil {
		fmt.Fprintf(&b, "PostgreSQL router: the SQL memory saved in the web interface could NOT be read, so the default is used: %v\n", memoryErr)
	}
	return b.String()
}

// pgRouterFreshLines says, at start-up, how fresh this port's answers are
// and who decides: BINTRAIL_SQL_TAILS for every server, or each server's own
// choice, fresh when nothing is saved for it. forced and read are
// console.SQLModeSetting's; fresh names the PostgreSQL servers in fresh mode
// by their choice. This process runs --workers statements at once in either
// mode: unlike the MySQL router's, its count is never lowered for the tails.
func pgRouterFreshLines(forced, read string, fresh []string) string {
	const (
		freshIs = "each table's newest captured changes are laid over its snapshot, so answers are about as far behind as capture. " +
			"Those changes are held in memory, besides the statements' own: up to a fifth of the memory this process may use for each server in fresh mode, or what BINTRAIL_SQL_TAIL_MAX_BYTES says per server"
		fastIs = "answers come from each table's last snapshot, as old as its last update"
		envIs  = "SQL_FRESH in the compose .env, then docker compose up -d pg-router; BINTRAIL_SQL_TAILS elsewhere"
	)
	var b strings.Builder
	switch forced {
	case console.SQLModeForcedFresh:
		fmt.Fprintf(&b, "PostgreSQL router: fresh mode for every server (BINTRAIL_SQL_TAILS=%s): %s. Unset it (%s) to choose each server's mode on its %s page.\n", read, freshIs, envIs, console.PageSnapshots)
	case console.SQLModeForcedFast:
		fmt.Fprintf(&b, "PostgreSQL router: fast mode for every server (BINTRAIL_SQL_TAILS=%s): %s, whatever is saved for a server. Unset it (%s) to choose each server's mode on its %s page.\n", read, fastIs, envIs, console.PageSnapshots)
	default:
		unknown := ""
		if read != "" {
			unknown = fmt.Sprintf(" (BINTRAIL_SQL_TAILS=%q is not on or off, and decides nothing)", read)
		}
		fmt.Fprintf(&b, "PostgreSQL router: each server answers in its own mode, fresh unless fast was saved for it%s. Fresh: %s. Fast: %s. Choose for a server on its %s page in the web interface (or bintrail-console port mode <server> fresh), or for every server with BINTRAIL_SQL_TAILS=on or =off (%s).\n",
			unknown, freshIs, fastIs, console.PageSnapshots, envIs)
	}
	switch len(fresh) {
	case 0:
	case 1:
		fmt.Fprintf(&b, "PostgreSQL router: 1 server is in fresh mode: %s.\n", fresh[0])
	default:
		fmt.Fprintf(&b, "PostgreSQL router: %d servers are in fresh mode: %s.\n", len(fresh), strings.Join(fresh, ", "))
	}
	return b.String()
}

func runPGRouter(cmd *cobra.Command, _ []string) error {
	cli.LoadEnvFile()
	// Before anything reads the AWS variables: this service's own S3 keys
	// (#3142) decide which token it may carry, or refuse a mixed pair.
	awsKeysLine, err := applyPGRouterAWSKeys()
	if err != nil {
		return err
	}
	st, err := pgRouterSettingsFrom(cmd)
	if err != nil {
		return err
	}
	// The MySQL router's own start-up pieces, called as they are (router.go
	// is not edited for this command): the saved SQL memory, and the console
	// that follows the daemon's files with a pool of the size given, with
	// the tails of PostgreSQL servers on (newPGRouterConsole).
	shared := routerSettings{
		Token: st.Token, ServersFile: st.ServersFile, AuthFile: st.AuthFile, MCPTokenFile: st.MCPTokenFile,
		SQLMemory: st.SQLMemory, SQLPortMaxRows: st.SQLPortMaxRows, MaxWait: st.MaxWait,
	}
	memoryFrom, memoryErr := routerMemory(&shared, sqlSettingsFilePath(st.ServersFile))
	memory := shared.SQLMemory
	if memory == "" {
		memory = sqlsandbox.DefaultLimits().MemoryLimit
	}
	srv, runner, err := newPGRouterConsole(shared, console.SQLPool{Workers: st.Workers, Threads: st.Threads})
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
	// never by a statement, so what waits on it does not pile up. The tails'
	// housekeeping rides the same clock, as in the MySQL router: a server
	// removed loses its tails, and idle ones are let go.
	go resolveCopyAgesEvery(ctx, func(ctx context.Context) {
		srv.ResolveCopyAges(ctx)
		srv.TidyTails()
	}, routerCopySnapshotEvery)

	served := st.TLS
	if served == nil {
		if served, err = shim.MadeUpPortTLS(); err != nil {
			return fmt.Errorf("the PostgreSQL router cannot serve TLS: %w", err)
		}
	}
	_, statErr := os.Stat(st.ServersFile)
	fmt.Fprint(os.Stderr, pgRouterStartupLines(st, memory, memoryFrom, memoryErr, srv.ServerCount(), statErr == nil))
	fmt.Fprint(os.Stderr, awsKeysLine)
	forced, read := console.SQLModeSetting()
	fmt.Fprint(os.Stderr, pgRouterFreshLines(forced, read, srv.FreshPostgresServerNames()))
	if p := st.TLS.ValidityProblemFor(time.Now(), pgVerifying); p != "" {
		// In the log as well: the startup lines are plain text on stderr.
		slog.Warn("PostgreSQL router: " + p)
	}
	routes := newPGRouteStats(time.Now(), st.StatusStatements && st.StatusAddr != "")
	routes.modes = srv
	// One line a minute per server in this service's log (pgrouter_statuslog.go).
	startPGPortStatusLog(ctx, func() map[string]pgRouterServerNumbers { return routes.doc(st.Listen).Servers }, srv.ServerName)
	if st.StatusAddr != "" {
		defer startPGRouterStatus(st.StatusAddr, map[string]http.Handler{
			"GET " + routerStatusPath: pgRouterStatusHandler(routes, st.Listen),
		}, os.Stderr)()
	}
	port := &pgRouterPort{srv: srv, addr: st.Listen, maxConns: st.MaxConns, poll: console.DefaultFollowInterval, out: os.Stderr, tls: served.Config,
		sources: newPGSourceSlots(st.MaxSourceConns), routes: routes, cost: &st.Cost}
	return port.serve(ctx)
}

// pgRouterStatusOffLine says that the router's status could not be served on
// addr, and that the PostgreSQL port is served all the same.
func pgRouterStatusOffLine(addr string, err error) string {
	return fmt.Sprintf("WARNING: the PostgreSQL router's status and metrics are OFF: %s could not be opened (%v). The PostgreSQL port is served without them, and the web interface cannot say which mode the router answers each server in. Free that address or set another (BINTRAIL_PG_ROUTER_STATUS_ADDR or --status-addr, and the same in BINTRAIL_CONSOLE_PG_ROUTER_URL where the web interface runs), then restart the router.\n", addr, err)
}

// startPGRouterStatus serves the router's status on addr and returns what
// stops it. An address that cannot be opened is said on out and in the log,
// and is not a reason to leave the PostgreSQL port closed (#3119): the
// status is an extra, and its address may simply be taken. The MySQL router
// does the same with its numbers.
func startPGRouterStatus(addr string, handlers map[string]http.Handler, out io.Writer) (stop func()) {
	stop, err := streamrun.StartMetricsServerWith(addr, handlers)
	if err != nil {
		fmt.Fprint(out, pgRouterStatusOffLine(addr, err))
		// In the log as well: the startup lines are plain text on stderr.
		slog.Warn("PostgreSQL router: its status and metrics are off, the address could not be opened; the port is served without them", "addr", addr, "error", err)
		return func() {}
	}
	return stop
}

// newPGRouterConsole is newRouterConsole for the PostgreSQL router: the same
// console, in a process that gives PostgreSQL servers tails (#2756), so a
// server in fresh mode answers with the changes captured since its last
// snapshot laid over its copy. Each server's mode and BINTRAIL_SQL_TAILS
// decide as they do on the MySQL port.
//
// Only this process may ask for them. A PostgreSQL table's key is text, and
// a tail is laid over the copy by comparing keys: exact where text compares
// by its bytes, as it does under PostgreSQL's rules, which every statement
// this port runs on a copy carries (sqlsandbox.Session.PostgresRules, in
// pgPortLogin), and wrong where it compares without case, as in a MySQL
// session. The MySQL router never calls this.
//
// The tails' files are in the temporary directory, which the MySQL router
// on the same machine shares. Each process sweeps there only what no live
// process holds (sweepTailDirs skips a directory whose lock is held), so
// neither removes the other's.
func newPGRouterConsole(st routerSettings, pool console.SQLPool) (*console.Server, *sqlsandbox.Runner, error) {
	console.EnablePostgresTails(true)
	return newRouterConsole(st, pool)
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
	// budget bounds the results the port holds while it sends them (#2804).
	// nil: serve makes one of the default size; a test passes its own to
	// read it.
	budget *pgshim.ResultBudget
	// tls is what a client that asks for TLS gets (#2766); nil answers 'N'.
	tls *tls.Config
	// sources bounds the connections to the servers' sources (#2771); nil:
	// serve makes one of the default size.
	sources *pgSourceSlots
	// routes counts which side answered each statement (#2770); nil
	// counts nothing.
	routes *pgRouteStats
	// cost is when a statement the copy can answer goes there rather than
	// to the source; nil for pgshim's default.
	cost *pgshim.CostPolicy

	mu    sync.Mutex
	bound string
	stop  context.CancelFunc
	// passwords are the ones that opened the port as it is bound now.
	passwords []string
}

// withdrawn closes the port, with every connection on it, when the passwords
// that open it are no longer the ones it was opened with: the port cannot
// tell which connection came in with the one that is gone.
//
// The console calls this for two things (console.Server.
// OnFlashbackPasswordWithdrawn): a password that stopped being accepted, and
// Engine Routing switched on or off, which the MySQL port closes its
// connections for so they reconnect under the new routing. Engine Routing is
// the MySQL port's, and what this port sends to the source does not depend on
// it, so that second one must not put its clients out: it is told from
// the first by the passwords, which a withdrawal always changes (one is
// replaced, or the port is turned off and there are none) and a switch of
// routing leaves as they were.
func (p *pgRouterPort) withdrawn() {
	now := p.srv.FlashbackPasswords()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stop != nil && !slices.Equal(now, p.passwords) {
		p.stop()
	}
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
	p.srv.OnFlashbackPasswordWithdrawn(p.withdrawn)
	// Before waiting for a password: an address that cannot be bound is said
	// now, not on the day a password is created.
	probe, err := net.Listen("tcp", p.addr)
	if err != nil {
		return fmt.Errorf("the PostgreSQL router cannot listen on %s: %w", p.addr, err)
	}
	_ = probe.Close()
	// One for the port, however many times it is opened again: a result
	// still being sent when the port closes is given back by its own
	// connection.
	budget := p.budget
	if budget == nil {
		budget = newPGPortResultBudget()
	}
	sources := p.sources
	if sources == nil {
		sources = newPGSourceSlots(defaultPGRouterMaxSourceConns)
	}
	opened, waiting, saidRebind := false, false, false
	for ctx.Err() == nil {
		if !p.open() {
			if !waiting {
				waiting = true
				fmt.Fprintf(p.out, "The PostgreSQL router is not listening yet: the port for SQL clients is not turned on. Turn it on with `dbtrail port on`, or in the web interface (Settings, Connect a SQL client): either creates its password, and %s opens here within a few seconds, without a restart. An access token (--token, BINTRAIL_CONSOLE_TOKEN) is one more password while the port is on; it does not open it.\n", p.addr)
			}
			select {
			case <-ctx.Done():
			case <-time.After(p.poll):
			}
			continue
		}
		waiting = false
		// Read before the port is bound: a password replaced between here
		// and there then differs from these, and closes the port.
		passwords := p.srv.FlashbackPasswords()
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
		p.bound, p.stop, p.passwords = ln.Addr().String(), cancel, passwords
		p.mu.Unlock()
		slog.Info("PostgreSQL router: the port is open", "listen", ln.Addr().String())
		err = servePGPortRouted(child, p.srv, ln, p.maxConns, budget, p.tls, sources, p.routes, p.cost)
		cancel()
		p.mu.Lock()
		p.bound, p.stop, p.passwords = "", nil, nil
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

// validPGListen refuses what is not an address the port can listen on. Any
// host is taken (#2766): the login never puts the password on the network,
// and a client that asks for TLS gets it; what an address other machines
// reach exposes is said at start-up (pgRouterAddressLine).
func validPGListen(addr, name string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s %q is not an address: want host:port, e.g. 127.0.0.1:5433", name, addr)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("%s %q: %q is not a port number (1 to 65535)", name, addr, port)
	}
	return nil
}

// servePGPort serves the PostgreSQL-protocol port on ln until ctx ends, with
// at most maxConns clients connected (0 for no limit).
func servePGPort(ctx context.Context, srv *console.Server, ln net.Listener, maxConns int) error {
	return servePGPortWithin(ctx, srv, ln, maxConns, newPGPortResultBudget(), nil)
}

// newPGPortResultBudget is the port's bound on the results it holds while it
// sends them, when nobody gave one: the size the MySQL port's has
// (defaultFlashbackResultBudget), with the same meaning. There is no flag
// for it on either port.
func newPGPortResultBudget() *pgshim.ResultBudget {
	return pgshim.NewResultBudget(defaultFlashbackResultBudget)
}

// servePGPortWithin is servePGPort with the budget its results are counted
// in (#2804): time travel's answers and the copy's alike, from the moment
// one is built until it is written or its connection ends. tlsConfig is what a
// client that asks for TLS gets; nil answers it 'N'.
func servePGPortWithin(ctx context.Context, srv *console.Server, ln net.Listener, maxConns int, budget *pgshim.ResultBudget, tlsConfig *tls.Config) error {
	return servePGPortWith(ctx, srv, ln, maxConns, budget, tlsConfig, newPGSourceSlots(defaultPGRouterMaxSourceConns))
}

// pgSourceSlots bounds the connections the port holds to the servers'
// sources, all of them together (--max-source-connections).
type pgSourceSlots struct {
	ch chan struct{}
	// wait is how long a statement waits for one when every one is taken,
	// the time a statement waits in line for the copy (#2985).
	wait time.Duration
	// idle are the connections clients keep open between their statements
	// (park, #3046), oldest first: each holds a slot, and gives it up to a
	// client that waits for one when none is free.
	mu   sync.Mutex
	idle []*pgIdleSource
	// parked is closed, and replaced, each time a connection is kept: every
	// client waiting for a slot looks at the idle ones again.
	parked chan struct{}
}

// pgIdleSource is one connection to a source kept idle by its client.
type pgIdleSource struct{ reclaim func() }

// pgSourceSlotWait is how long a statement waits for a connection to the
// source when the port holds as many as it opens: the copy's line
// (sqlsandbox.DefaultMaxWait). A slot is given back as soon as a statement
// outside a transaction block has its answer, so the wait is usually a few
// milliseconds; only blocks held open for longer make it run out. The line
// has no length of its own: each client waits for one slot at most, and the
// port's client limit (--max-connections) bounds the clients.
const pgSourceSlotWait = sqlsandbox.DefaultMaxWait

func newPGSourceSlots(n int) *pgSourceSlots {
	return &pgSourceSlots{ch: make(chan struct{}, n), wait: pgSourceSlotWait, parked: make(chan struct{})}
}

// limit is how many there may be.
func (p *pgSourceSlots) limit() int { return cap(p.ch) }

// park offers the slot of a connection its client keeps idle between
// statements (pgshim.Forward.Park): reclaim ends that connection, giving its
// slot back, and is called at most once, here, for a client that waits for a
// slot. unpark takes it back for its client and reports whether it was still
// there; once it returns, reclaim is never called.
func (p *pgSourceSlots) park(reclaim func()) (unpark func() bool) {
	e := &pgIdleSource{reclaim: reclaim}
	p.mu.Lock()
	p.idle = append(p.idle, e)
	close(p.parked)
	p.parked = make(chan struct{})
	p.mu.Unlock()
	return func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for i, x := range p.idle {
			if x == e {
				p.idle = append(p.idle[:i], p.idle[i+1:]...)
				return true
			}
		}
		return false
	}
}

// reclaimIdle ends the connection kept idle longest, if any, and reports
// whether one was.
func (p *pgSourceSlots) reclaimIdle() bool {
	p.mu.Lock()
	if len(p.idle) == 0 {
		p.mu.Unlock()
		return false
	}
	e := p.idle[0]
	p.idle = p.idle[1:]
	p.mu.Unlock()
	e.reclaim()
	return true
}

// acquire takes one, waiting for it up to p.wait while ctx lasts; release
// gives it back, once. When every one is taken and some are kept idle by
// their clients (park), the one kept longest is ended for this one first:
// a client never waits behind a connection nobody is using.
func (p *pgSourceSlots) acquire(ctx context.Context) (release func(), ok bool) {
	give := func() func() {
		var once sync.Once
		return func() { once.Do(func() { <-p.ch }) }
	}
	var expired <-chan time.Time
	if p.wait > 0 {
		t := time.NewTimer(p.wait)
		defer t.Stop()
		expired = t.C
	}
	for {
		// Taken before the idle ones are looked at: a connection kept after
		// this look closes this channel, so it is not missed.
		p.mu.Lock()
		parked := p.parked
		p.mu.Unlock()
		select {
		case p.ch <- struct{}{}:
			return give(), true
		default:
		}
		if p.reclaimIdle() {
			continue
		}
		if p.wait <= 0 {
			return nil, false
		}
		select {
		case p.ch <- struct{}{}:
			return give(), true
		case <-parked:
		case <-ctx.Done():
			return nil, false
		case <-expired:
			return nil, false
		}
	}
}

// servePGPortWith is servePGPortWithin with the bound on connections to the
// sources (#2771).
func servePGPortWith(ctx context.Context, srv *console.Server, ln net.Listener, maxConns int, budget *pgshim.ResultBudget, tlsConfig *tls.Config, sources *pgSourceSlots) error {
	return servePGPortRouted(ctx, srv, ln, maxConns, budget, tlsConfig, sources, nil, nil)
}

// servePGPortRouted is servePGPortWith with the tally of which side answered
// each statement (#2770), nil counting nothing, and when a statement the copy
// can answer goes there rather than to the source, nil for pgshim's default.
func servePGPortRouted(ctx context.Context, srv *console.Server, ln net.Listener, maxConns int, budget *pgshim.ResultBudget, tlsConfig *tls.Config, sources *pgSourceSlots, routes *pgRouteStats, cost *pgshim.CostPolicy) error {
	logger := slog.Default()
	// Accept returns when the listener is closed; the connections end with
	// ctx (pgshim closes each one's socket), and Serve waits for them.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	// One gate for the port, as the MySQL port has one: full-table time
	// travel is refused on this wire, so nothing waits on it; the engine
	// asks for it all the same.
	gate := shim.NewGate(defaultFlashbackMaxFullTable)
	return pgshim.Serve(ctx, ln, pgshim.Config{Login: pgPortLogin(srv, pgRouterQueryTimeout, gate, logger, sources, routes, cost), Logger: logger, MaxConns: maxConns, ResultBudget: budget, TLS: tlsConfig})
}

// pgPortLogin is the port's login: the port's passwords, then the server.
//
// The password is checked before anything is said about the server, and a
// wrong password reads the same for every username and database, so a client
// that holds no password learns nothing about which servers exist. It takes
// the same time too: the client's proof is checked against as many passwords
// whether or not the username is an application login (Password.Matches).
//
// sources bounds the connections to the servers' sources that the logins'
// connections share (#2771), and routes counts which side answered each of
// their statements (#2770; nil counts nothing).
func pgPortLogin(srv *console.Server, timeout time.Duration, gate *shim.Gate, logger *slog.Logger, sources *pgSourceSlots, routes *pgRouteStats, cost *pgshim.CostPolicy) pgshim.LoginFunc {
	// The side each statement shape of a server was last sent to by its
	// plan, for all the port's connections to it (#3045).
	sides := &pgCostSides{}
	return func(ctx context.Context, user, database string, password *pgshim.Password, drop func()) (*pgshim.Binding, *pgshim.Error) {
		// Before the passwords are read: a connection opened with an
		// application login is served only if no login changed since
		// (console.Server.LoginEpoch).
		epoch := srv.LoginEpoch()
		// One reading for the passwords and the server a login opens
		// (console.Server.FlashbackPasswordsFor): the connection is bound to
		// that server, never to what the username resolves to later.
		passwords, loginServer := srv.FlashbackPasswordsFor(user)
		// Every password that opens the port, for any username, derived
		// before this username's are checked: the work a login does then
		// says nothing about which usernames are application logins.
		pgshim.WarmPasswords(srv.AllFlashbackPasswords())
		// The monitor login (console.MonitorPasswordFor), the one ProxySQL's
		// health checks log in with (pgsql-monitor_username/_password, the
		// pair saved for the MySQL port's monitor too): its password only,
		// and a connection that opens no server, whatever database it names
		// (ProxySQL's is postgres). It answers what ProxySQL's PostgreSQL
		// monitor sends (lib/PgSQL_Monitor.cpp): an empty query as its ping,
		// and SELECT pg_is_in_recovery() for a server in a replication
		// hostgroup, which is true here (a reader). Everything else is
		// refused (pgshim.Binding.Monitor).
		if mp, ok, err := srv.MonitorPasswordFor(user); err != nil || ok {
			if err != nil || !password.Matches([]string{mp}) {
				return nil, &pgshim.Error{Code: "28P01", Message: fmt.Sprintf("password authentication failed for user %q", user)}
			}
			untrack, tracked := srv.TrackLoginConn(console.MonitorLoginKey, epoch, drop)
			if !tracked {
				return nil, &pgshim.Error{Code: "57P03", Message: "the monitor login changed while connecting; reconnect"}
			}
			return &pgshim.Binding{Actor: "monitor", Schema: pgPortSchema, Monitor: true, Close: untrack}, nil
		}
		if !password.Matches(passwords) {
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
				"server %q is not a PostgreSQL server, and this port serves PostgreSQL servers only; the copy of a MySQL or MariaDB server is read with a MySQL client on the MySQL port", tgt.ID)}
		}
		ages := routes.agesFor(tgt.ID)
		bound := &pgshim.Binding{Actor: "server:" + tgt.ID, Schema: pgPortSchema, Routed: ages.routed()}
		// SHOW dbtrail.router_status on this connection (#3021); it reads
		// the binding when asked, so what is bound below is in its answer.
		bound.Status = pgPortStatusFor(srv, tgt, bound, routes, ages)
		// Time travel, by the engine the MySQL port binds per connection
		// (flashbackShimConfig, called as it is): the server's index and its
		// snapshots. The server's own gate goes with it: a server that hides
		// rows or columns refuses time travel, which reads past the views
		// that apply those rules (console.FlashbackTarget.TimeTravelRefusal).
		engine := shim.NewHandlerWithConfig(tgt.IndexDB, flashbackShimConfig(tgt, flashbackConfig{QueryTimeout: timeout}.withDefaults(), gate), logger)
		engine.BindActor("server:" + tgt.ID)
		// On the engine too: this wire does not go through the engine's own
		// command loop, which is where it asks, so the one below is the gate
		// that acts here. Bound all the same, so the engine carries its
		// server's gate whichever way it is ever reached.
		engine.BindTimeTravelGate(tgt.TimeTravelRefusal)
		bound.TimeTravel, bound.TimeTravelRefusal = engine, tgt.TimeTravelRefusal
		closers := []func(){engine.Close}
		bound.Close = func() {
			for _, c := range closers {
				c()
			}
		}
		if tgt.ViaLogin {
			// Tracked, so that changing or removing the login closes the
			// connection it opened (console.Server.TrackLoginConn).
			untrack, ok := srv.TrackLoginConn(tgt.ID, epoch, drop)
			if !ok {
				engine.Close()
				return nil, &pgshim.Error{Code: "57P03", Message: "an application login changed while connecting; reconnect"}
			}
			closers = append(closers, untrack)
		}
		// What is sent to the source goes as the server's forwarding account
		// (#2771), never the capture account (#2317). The connection is
		// tracked like the MySQL port's routed ones, so that a change of the
		// account, or the server's removal, closes it at once
		// (console.Server.TrackRoutedConn); it has no MySQL thread to KILL.
		//
		// Or as the capture account itself, when the server said yes to that
		// and has no account of its own (#3054): then every statement passes
		// the capture gate first (pgshim, capture_gate.go).
		switch fwd := srv.PostgresForwardTarget(tgt.ID); {
		case fwd.DSN != "":
			untrack, ok := srv.TrackRoutedConn(tgt.ID, fwd.Gen, drop, nil)
			if !ok {
				bound.Close()
				return nil, &pgshim.Error{Code: "57P03", Message: "the server's forwarding account changed while connecting; reconnect"}
			}
			closers = append(closers, untrack)
			bound.Forward = &pgshim.Forward{DSN: fwd.DSN, CaptureAccount: fwd.CaptureAccount, Acquire: sources.acquire, Park: sources.park, Limit: sources.limit(), Timeout: timeout,
				Barred: pgForwardBarred(tgt.ID, tgt.TimeTravelRefusal, routes, logger), Cost: cost, Sides: sides.of(tgt.ID)}
		case fwd.Refused != "":
			bound.Forward = &pgshim.Forward{Refused: fwd.Refused}
		}
		if tgt.SQL == nil {
			// The connection is made and every statement says why it cannot
			// be answered: the reason is about the server, and a client that
			// could not connect at all would not be shown it.
			why := tgt.SQLUnavailable
			bound.Run = func(context.Context, string, pgshim.RunOptions) (sqlsandbox.Result, error) {
				return sqlsandbox.Result{}, &pgshim.Error{Code: "55000", Message: "SQL on the copy is not available for this server: " + why}
			}
			return bound, nil
		}
		copySQL := tgt.SQL
		// A PostgreSQL source's copy answers by PostgreSQL's rules, with its
		// names resolved where PostgreSQL resolves them, and a column the
		// statement did not name headed as PostgreSQL heads it (#2803).
		bound.Answered = copySQL.NoteAnswered
		// How far the copy's last answer on this connection reaches, so one
		// older than what the source already answered it is not given
		// (#2985): the instant of its tails when every table it read had
		// them, none when it read a snapshot alone.
		bound.RunAge = func() pgshim.CopyAge {
			a := copySQL.LastRunAge()
			age := pgshim.CopyAge{Read: a.Read()}
			if a.Tailed {
				age.Through = a.Through
			}
			return age
		}
		// A table whose capture dropped its changes is read from its
		// snapshot alone, which no later change reaches: the source answers
		// a statement that reads one (table_not_captured), where the
		// connection can send it there.
		bound.NotCaptured = func(ctx context.Context) ([]pgshim.TableNotCaptured, error) {
			if timeout > 0 {
				// An index that hangs must not hold the statement: past
				// it, the error sends the statement to the source.
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			lost, err := copySQL.PGTablesNotCaptured(ctx)
			out := make([]pgshim.TableNotCaptured, len(lost))
			for i, t := range lost {
				out[i] = pgshim.TableNotCaptured{Schema: t.Schema, Name: t.Table, Why: t.Why, SnapshotRead: t.SnapshotRead}
			}
			return out, err
		}
		// The copy waits for the source's position on a connection the
		// source answered, then answers (#2985).
		bound.CopyWait = func(ctx context.Context, lsn uint64) (pgshim.CopyWaitOutcome, string) {
			outcome, why := copySQL.WaitPGTailsPast(ctx, lsn)
			switch outcome {
			case console.PGCopyReached:
				return pgshim.CopyReached, why
			case console.PGCopyNotReached:
				return pgshim.CopyNotReached, why
			}
			return pgshim.CopyNoWait, why
		}
		bound.Run = func(ctx context.Context, statement string, opts pgshim.RunOptions) (sqlsandbox.Result, error) {
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			// The schema and time zone the client set (SET search_path, SET
			// TimeZone), the port's own schema until it sets one (#2769).
			schema := opts.Schema
			if schema == "" {
				schema = pgPortSchema
			}
			sess := sqlsandbox.Session{PostgresRules: true, PostgresNames: true, TimeZone: opts.TimeZone}
			res, err := copySQL.Run(pgRunContext(ctx, opts), statement, schema, sess)
			if err == nil && !opts.Probe {
				// How old what it read was, for /status (#2756), kept once
				// the port says the client got it (bound.Routed).
				ages.stash(copySQL.LastRunAge())
			}
			return res, err
		}
		// The system catalog's statements are answered from the copy's own
		// tables (#2769), read in UTC and in the port's schema: what the
		// catalog lists does not depend on the session.
		// The capture gate's list: the copy's views by name, as the catalog
		// lists them, with no worker of the copy.
		bound.CopyTableNames = func(ctx context.Context) ([][2]string, error) {
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			return copySQL.PGCopyViewNames(ctx)
		}
		bound.Catalog = func(ctx context.Context) ([]pgshim.CatalogTable, error) {
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			return pgCopyCatalog(ctx, copySQL, func(ctx context.Context, statement string) (sqlsandbox.Result, error) {
				return copySQL.Run(ctx, statement, pgPortSchema, sqlsandbox.Session{PostgresRules: true, PostgresNames: true})
			})
		}
		return bound, nil
	}
}

// pgRunContext is the context a statement of the port runs on the copy
// under: one whose answer the port gives only with its tables' recent
// changes (opts.NeedTails) is refused before it runs when they are not ready
// (console.WithPGNeedTails, #3086).
func pgRunContext(ctx context.Context, opts pgshim.RunOptions) context.Context {
	if opts.NeedTails {
		return console.WithPGNeedTails(ctx)
	}
	return ctx
}

// pgCostSides is a port's pgshim.CostSides, one per server: a server's
// statement shapes are another server's only by name.
type pgCostSides struct{ by sync.Map }

func (p *pgCostSides) of(id string) *pgshim.CostSides {
	v, _ := p.by.LoadOrStore(id, &pgshim.CostSides{})
	return v.(*pgshim.CostSides)
}

// pgHiddenBarred is why nothing is sent to the source of a server that hides
// tables or columns (#2770): the rules are applied by the copy's views, and a
// statement run on the source reads past them. A check of the forwarding
// account's grants could not settle it: a view runs with its owner's rights,
// as a SECURITY DEFINER function does, and a role's grants are not all in
// sight, so an account that reads none of the hidden tables itself can still
// reach them.
const pgHiddenBarred = "this server hides tables or columns, which its source would not hide, so the PostgreSQL port sends nothing to the source for it: the copy answers alone"

// pgBarredSaid is, per server id, the last reason nothing is sent to its
// source that was logged, so each one is logged once.
var pgBarredSaid sync.Map

// pgForwardBarred is pgshim.Forward.Barred for server id: refusal is the
// server's own gate on what reads past its views (console.FlashbackTarget.
// TimeTravelRefusal), asked per statement so a rule saved while a
// connection is open governs its next one. Rules that cannot be read bar it
// too. A change is logged once and shown in the port's status.
func pgForwardBarred(id string, refusal func() string, routes *pgRouteStats, logger *slog.Logger) func() string {
	return func() string {
		why := ""
		if refusal != nil && refusal() != "" {
			why = pgHiddenBarred
		}
		if prev, _ := pgBarredSaid.Load(id); prev != why {
			pgBarredSaid.Store(id, why)
			if why != "" {
				logger.Warn("PostgreSQL router: nothing is sent to this server's source", "server", id, "reason", why)
			} else if prev != nil {
				logger.Info("PostgreSQL router: this server's source answers again what the copy does not", "server", id)
			}
		}
		routes.setBarred(id, why)
		return why
	}
}
