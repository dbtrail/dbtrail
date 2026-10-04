package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/shim"
)

// shimCmd serves the time-travel SQL endpoint as an in-process
// MySQL-protocol server. Operators run this alongside the streamer
// (stream or agent) on the same host; ProxySQL routes _flashback / _diff /
// _snapshot virtual-schema queries to its --listen address.
//
// Recognised statement shapes (handled by internal/shim/parser.go):
//   - SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>
//   - SELECT * FROM _snapshot.<table>  AS OF '<ts>' WHERE <col> = <value>
//   - SELECT * FROM _diff.<table>      BETWEEN '<t1>' AND '<t2>' WHERE <col> = <value>
//
// Time-range queries are answered against the live bintrail MySQL
// index plus any S3 archives auto-discovered via archive_state — the
// same merge pipeline `bintrail query` and `bintrail recover` use.
//
// Authentication: TenantAuth validates BOTH that the connecting
// username appears in shim.yaml AND that the client's
// mysql_native_password challenge response matches the cleartext
// stored in mysql_password. ProxySQL is still the outer gate
// (validating against mysql_pass_sha1 derived from the same
// cleartext); the shim's local validation closes the gap that would
// otherwise let any direct connection to :3308 with a known username
// in. The default --listen of 127.0.0.1:3308 keeps the shim
// unreachable from the network anyway.
var shimCmd = &cobra.Command{
	Use:   "shim",
	Short: "Run the time-travel SQL MySQL-protocol server",
	Long: `Run an in-process MySQL-protocol server that answers
time-travel SQL queries against the three virtual schemas
_flashback / _snapshot / _diff. Intended to sit behind ProxySQL on the
same host; see docs/time-travel-sql.md for the end-to-end setup.

The shim auto-discovers S3 archives via archive_state, so queries that
reach back beyond the live MySQL index's retention window resolve
transparently from Parquet. Use --no-archive to disable that and stay
index-only.

By default, an archive fetch failure or a planner-detected coverage
gap aborts the customer's query with a MySQL protocol error; the
client has no stderr channel, so silent partial results are worse than
a loud failure. Use --allow-gaps to fall back to warn-and-continue
(matches bintrail recover's behaviour).

Authentication validates both username and password against shim.yaml's
tenants block (mysql_user + mysql_password). ProxySQL is still the outer
password gate against the same cleartext. The default --listen of
127.0.0.1:3308 keeps the shim unreachable from the network anyway.`,
	RunE: runShim,
}

var (
	shListen       string
	shIndexDSN     string
	shShimConfig   string
	shNoArchive    bool
	shAllowGaps    bool
	shAuthMethod   string
	shBaselineDir  string
	shBaselineS3   string
	shQueryTimeout time.Duration
	shMaxConns     int
	shMaxFullTable int
)

// monitorDeniedStats aggregates ER_ACCESS_DENIED_ERROR handshake failures
// whose username matches ProxySQL's `monitor` (case-insensitive). Per-probe
// logs stay at Debug (issue #316); this struct feeds the steady-state INFO
// summary so credential brute-force bursts that spoof the monitor username
// and ProxySQL misconfigurations (rotated mysql-monitor_password) surface
// without restoring the per-probe flood. See issues #326 and the silent-
// failure follow-up.
//
// `count` alone (the original #326 design) couldn't distinguish "one IP
// hammering with 3000 attempts" from "200 IPs each trying once" — exactly
// the forensic signal an operator needs to tell credential stuffing from a
// monitor misconfig. `distinctRemotes` is a bounded set (cap
// monitorDeniedRemoteCap) so a wide-source burst can't blow memory; once
// the cap is reached additional remotes are dropped (the metric becomes a
// floor, which is the right direction for an alerting signal). Timestamps
// bracket the interval so the emit line is self-contained.
const monitorDeniedRemoteCap = 16

var monitorDenied struct {
	mu              sync.Mutex
	count           int64
	distinctRemotes map[string]struct{}
	firstSeen       time.Time
	lastSeen        time.Time
}

// monitorDeniedInterval is the cadence at which monitorDeniedTicker
// emits and resets the counter. 5 minutes is short enough that a
// misconfig surfaces within one operator's coffee break and long enough
// that the steady-state log volume stays at "at most 1 line every 5
// minutes" — versus pre-#316's ~6-360 lines/minute monitor flood. Not
// flag-configurable on purpose: no operator will need to tune this and
// the tunable would just be one more thing to misconfigure.
const monitorDeniedInterval = 5 * time.Minute

// monitorDeniedBump records one denied monitor probe with its source
// remote (host portion only — same client at different ports counts as
// one source for the distinctRemotes signal). Called from handleConn
// after classifyHandshakeErr identifies the probe as a monitor failure.
func monitorDeniedBump(remoteAddr string) {
	if host, _, splitErr := net.SplitHostPort(remoteAddr); splitErr == nil {
		remoteAddr = host
	}
	now := time.Now()
	monitorDenied.mu.Lock()
	defer monitorDenied.mu.Unlock()
	if monitorDenied.count == 0 {
		monitorDenied.firstSeen = now
	}
	monitorDenied.lastSeen = now
	monitorDenied.count++
	if monitorDenied.distinctRemotes == nil {
		monitorDenied.distinctRemotes = make(map[string]struct{}, monitorDeniedRemoteCap)
	}
	// Bounded: a probe from a NEW remote past the cap is silently dropped
	// from the distinct set. The count still increments; the alerting
	// metric simply floors at the cap.
	if len(monitorDenied.distinctRemotes) < monitorDeniedRemoteCap {
		monitorDenied.distinctRemotes[remoteAddr] = struct{}{}
	}
}

// monitorDeniedReset clears the aggregator. Called from runShim at startup
// (defends against state inherited from a prior in-process test run) and
// from emitMonitorDenied after a snapshot.
func monitorDeniedReset() {
	monitorDenied.mu.Lock()
	defer monitorDenied.mu.Unlock()
	monitorDenied.count = 0
	clear(monitorDenied.distinctRemotes)
	monitorDenied.firstSeen = time.Time{}
	monitorDenied.lastSeen = time.Time{}
}

func init() {
	shimCmd.Flags().StringVar(&shListen, "listen", "127.0.0.1:3308", "Listen address for the MySQL protocol port (default: localhost-only; keep ProxySQL as the auth gate)")
	shimCmd.Flags().StringVar(&shIndexDSN, "index-dsn", "", "DSN of the bintrail MySQL index")
	shimCmd.Flags().StringVar(&shShimConfig, "shim-config", "shim.yaml", "Path to shim.yaml (the file produced by 'bintrail init-shim')")
	shimCmd.Flags().BoolVar(&shNoArchive, "no-archive", false, "Skip archive auto-discovery; query only the live MySQL index")
	shimCmd.Flags().BoolVar(&shAllowGaps, "allow-gaps", false, "Warn and continue when an archive source fails or the planner detects a coverage gap, instead of returning a MySQL protocol error to the client (default: strict, fail loudly)")
	shimCmd.Flags().StringVar(&shAuthMethod, "auth-method", "", "MySQL auth plugin to advertise during the handshake. Empty (default) keeps mysql_native_password for backwards compatibility. Set to 'caching_sha2_password' or 'sha256_password' on MySQL 8.4+ instances where mysql_native_password is disabled by default. Requires ProxySQL 2.7+ upstream.")
	shimCmd.Flags().StringVar(&shBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots (from 'bintrail baseline'). Enables _snapshot baseline lookup so rows untouched within the retained binlog window still resolve at AS OF. _flashback stays binlog-only. Unset (default) makes _snapshot behave like _flashback.")
	shimCmd.Flags().StringVar(&shBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline Parquet snapshots (e.g. s3://bucket/baselines/). Takes precedence over --baseline-dir. Uses the standard AWS credential chain. See --baseline-dir for what it enables.")
	shimCmd.Flags().DurationVar(&shQueryTimeout, "query-timeout", 5*time.Minute, "Per-query deadline for time-travel queries (index fetch + archive/DuckDB fetch + full-table slot wait). On expiry the client sees MySQL error 1317. 0 disables the deadline.")
	shimCmd.Flags().IntVar(&shMaxConns, "max-connections", 100, "Maximum concurrent client connections; further connections are refused with MySQL error 1040 'Too many connections'. 0 = unlimited.")
	shimCmd.Flags().IntVar(&shMaxFullTable, "max-fulltable-queries", 4, "Maximum concurrent full-table time-travel reconstructions (the heaviest queries; up to 100k buffered rows each); excess queries wait for a slot up to --query-timeout, then fail with MySQL error 1203. 0 = unlimited.")
	_ = shimCmd.MarkFlagRequired("index-dsn")
	BindCommandEnv(shimCmd)
}

func runShim(cmd *cobra.Command, args []string) error {
	tenantCfgs, err := shim.LoadTenantConfigs(shShimConfig)
	if err != nil {
		return err
	}
	users := make(map[string]string, len(tenantCfgs))
	for _, t := range tenantCfgs {
		users[t.MySQLUser] = t.MySQLPassword
	}
	// userSchemas seeds Handler.db at handshake time so fully qualified
	// time-travel queries like `SELECT * FROM _flashback.orders` work
	// without a prior `USE <db>` (issue #263). Per-tenant warnings are
	// emitted by buildUserSchemas; the partial-degradation summary
	// below catches the more interesting case where so many tenants
	// have unusable DSNs that the operator should notice at startup.
	userSchemas := buildUserSchemas(tenantCfgs)
	// Per-tenant schema allowlists (#824). Opt-in: a tenant without
	// allowed_schemas keeps the historical any-schema behaviour, but a
	// multi-tenant shim.yaml that leaves any tenant unrestricted means
	// tenants can read each other's indexed history — say so once at
	// startup instead of letting the operator discover it in an audit.
	userAllowedSchemas := buildUserAllowedSchemas(tenantCfgs)
	if unrestricted := tenantsWithoutAllowedSchemas(tenantCfgs); len(tenantCfgs) > 1 && len(unrestricted) > 0 {
		slog.Warn(
			"shim: cross-schema isolation is NOT enforced for some tenants; any of them can query every schema in the index. Add allowed_schemas to each tenant in shim.yaml to isolate them",
			"tenants", len(tenantCfgs),
			"unrestricted_users", unrestricted,
		)
	}
	if missing := len(tenantCfgs) - len(userSchemas); missing > 0 {
		// Name the affected users so a 50-tenant deployment doesn't
		// force the operator to grep the prior per-tenant warnings.
		missingUsers := make([]string, 0, missing)
		for _, t := range tenantCfgs {
			if _, ok := userSchemas[t.MySQLUser]; !ok {
				missingUsers = append(missingUsers, t.MySQLUser)
			}
		}
		slog.Warn(
			"shim: some tenants have no usable default schema; queries from those tenants will require explicit `USE <db>`",
			"tenants", len(tenantCfgs),
			"with_default_schema", len(userSchemas),
			"missing", missing,
			"missing_users", missingUsers,
		)
	}
	auth, err := shim.NewTenantAuth(users)
	if err != nil {
		return err
	}
	// The credential must advertise the same plugin the server below is
	// built with (shim.NewMySQLServer(cfg.AuthMethod)) — go-mysql
	// auth-switches the client to the credential's plugin on mismatch.
	auth = auth.WithAuthMethod(shAuthMethod)

	db, err := config.Connect(shIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index: %w", err)
	}
	defer db.Close()

	// Eager Ping so a misconfigured DSN fails at startup rather than at
	// the first _flashback query the customer actually runs. config.Connect
	// is lazy: it sets parseTime + a TCP timeout but does not exchange a
	// packet until the first query.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		pingCancel()
		return fmt.Errorf("ping index DB: %w", err)
	}
	pingCancel()

	// Idempotent schema migration (CLI-typed DSN — the one-DDL boundary):
	// the shared query engine SELECTs post-initial-schema binlog_events
	// columns (query_text/query_hash, #699). Without this a shim restarted
	// on a new binary against an index last written by an older one would
	// abort EVERY _flashback/_snapshot client query with MySQL error 1054
	// until some unrelated writer command migrated the index.
	if err := indexer.EnsureSchema(db); err != nil {
		return indexer.WrapSchemaMigrationErr(err)
	}

	listener, err := net.Listen("tcp", shListen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", shListen, err)
	}
	defer listener.Close()

	if !isLoopbackAddr(listener.Addr()) {
		slog.Warn(
			"shim is bound to a non-loopback address; ensure no other client has direct network access to this port",
			"addr", listener.Addr().String(),
		)
	}

	slog.Info("shim listening",
		"addr", listener.Addr().String(),
		"tenants", len(tenantCfgs),
		"tenants_with_default_schema", len(userSchemas),
	)

	// SIGINT/SIGTERM or parent-context cancellation (an embedding caller, a
	// wiring test) → ctx done → close listener → accept loop returns. The
	// signal-bound context is the sibling daemons' pattern (`serve`,
	// bintrail-pg flashback), and NotifyContext un-registers the handler when
	// runShim returns.
	ctx, cancel := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Usage telemetry for a months-lived process (#1362): Init's drain runs
	// once, at startup, so without this loop a daemon's beacons would spool
	// and age out undelivered. Own goroutine — never on a client connection's
	// path. The client arrives through the package seam (processClient): shim
	// is the one daemon implemented in this shared package, and each binary's
	// hook publishes the client it resolved at Start.
	go processClient.RunDaemon(ctx, cmd.Name())

	go func() {
		<-ctx.Done()
		slog.Info("shim shutting down")
		listener.Close()
	}()

	// Periodic INFO summary of denied ProxySQL monitor-user handshakes.
	// Per-probe noise stays at Debug (#316); this aggregator is the only
	// steady-state signal at INFO so a credential probe burst or a
	// ProxySQL misconfig (rotated mysql-monitor_password) surfaces
	// without restoring the per-probe log flood. See issue #326. Reset
	// to zero at startup so a fresh process never inherits stale state
	// from a previous in-process test run.
	//
	// The ticker is tracked with shimWG so runShim doesn't return until
	// the ticker's final drain has emitted. Without this, the `defer
	// cancel()` would signal cancellation but Go's runtime wouldn't
	// wait for the goroutine before main exits, dropping up to a full
	// interval's worth of counts on every clean shutdown — exactly the
	// forensic loss the periodic emit was supposed to prevent.
	monitorDeniedReset()
	var shimWG sync.WaitGroup
	shimWG.Add(1)
	go func() {
		defer shimWG.Done()
		monitorDeniedTicker(ctx, slog.Default())
	}()

	// config.Connect already parsed and pinged shIndexDSN above, so a parse
	// failure here would be a programming defect, not a configuration error
	// — surface it loud rather than degrading silently. The empty-DBName
	// check catches the more common operator mistake of pointing the shim
	// at a DSN with no database path; without it, every customer query
	// would fail at FetchMerged.validate() with an opaque wire-protocol
	// error or, under AllowGaps=true, silently skip gap detection.
	dsnCfg, err := mysqldriver.ParseDSN(shIndexDSN)
	if err != nil {
		return fmt.Errorf("parse index DSN: %w", err)
	}
	if dsnCfg.DBName == "" {
		return fmt.Errorf("index DSN must include the database name (e.g. /bintrail_index)")
	}

	cfg := shim.Config{
		AllowGaps:   shAllowGaps,
		NoArchive:   shNoArchive,
		IndexDBName: dsnCfg.DBName,
		AuthMethod:  shAuthMethod,
		BaselineDir: shBaselineDir,
		BaselineS3:  shBaselineS3,
		// #823: per-query deadline + one process-wide gate on full-table
		// reconstructions. Config is copied per connection; the *Gate is
		// shared by pointer, which is what makes it a global cap.
		QueryTimeout:  shQueryTimeout,
		FullTableGate: shim.NewGate(shMaxFullTable),
	}
	// Build the *server.Server once at startup, not per connection:
	//
	//  - typo in --auth-method fails the daemon immediately rather than
	//    silently dropping every incoming connection (which would log
	//    server-side and look like a TCP close to the client),
	//  - the caching_sha2_password cache is per-Server (the
	//    Server.cacheShaPassword sync.Map field in go-mysql v1.13.0);
	//    a per-connection Server resets the cache on every accept and
	//    the "caching" in the plugin name silently does nothing,
	//  - RSA-2048 keypair generation runs once per process (~50ms) not
	//    once per connection. *server.Server is goroutine-safe by
	//    upstream design — per-connection state lives on *server.Conn.
	srv, err := shim.NewMySQLServer(cfg.AuthMethod)
	if err != nil {
		return fmt.Errorf("auth method: %w", err)
	}
	serveLoop(ctx, listener, db, srv, auth, cfg, userSchemas, userAllowedSchemas, shMaxConns)
	// Block until the monitorDeniedTicker has run its final drain.
	// serveLoop returning means ctx is cancelled; the ticker is
	// observing the same ctx and will hit its `case <-ctx.Done()`
	// branch, emitMonitorDenied, and return. Without this Wait, the
	// process exits before the goroutine reaches the emit.
	shimWG.Wait()
	return nil
}

// serveLoop accepts MySQL protocol connections one at a time. Each
// connection runs in its own goroutine with its own Handler instance
// (Handler holds per-connection state: the currently-selected
// database).
//
// maxConns > 0 caps the concurrent connections (#823): a connection
// beyond the cap is refused with a wire-protocol 1040 "Too many
// connections" instead of being accepted into an unbounded goroutine
// pile. 0 keeps the pre-#823 unlimited behaviour.
//
// Accept errors that aren't shutdown signals (ctx cancellation, listener
// closed) are retried with exponential backoff so a transient kernel
// hiccup doesn't burn CPU and a permanent listener wedge doesn't fill
// the log at ~10 lines/sec. The backoff resets to zero on every
// successful Accept so a brief spike doesn't poison the steady state.
func serveLoop(ctx context.Context, listener net.Listener, db *sql.DB, srv *server.Server, auth shim.TenantAuth, cfg shim.Config, userSchemas map[string]string, userAllowedSchemas map[string][]string, maxConns int) {
	var wg sync.WaitGroup
	defer wg.Wait()

	var connSem chan struct{}
	if maxConns > 0 {
		connSem = make(chan struct{}, maxConns)
	}

	var backoff time.Duration
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return // graceful shutdown
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			backoff = nextAcceptBackoff(backoff)
			slog.Error("accept failed", "err", err, "backoff", backoff)
			// Sleep via select so SIGTERM can interrupt a long
			// backoff — without this, a wedged listener at the
			// 5s cap would keep the process alive for up to 5s
			// after the operator pressed Ctrl+C.
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}
		backoff = 0
		if connSem != nil {
			select {
			case connSem <- struct{}{}:
			default:
				slog.Warn("connection refused: --max-connections reached",
					"remote", conn.RemoteAddr(), "max_connections", maxConns)
				refuseTooManyConnections(conn)
				conn.Close()
				continue
			}
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			if connSem != nil {
				defer func() { <-connSem }()
			}
			handleConn(ctx, c, db, srv, auth, cfg, userSchemas, userAllowedSchemas)
		}(conn)
	}
}

// refuseTooManyConnections writes the MySQL wire ERR packet a real
// mysqld sends when max_connections is exhausted (1040 "Too many
// connections"). It is sent INSTEAD of the initial handshake packet —
// the protocol allows an ERR as the server's first packet, and client
// libraries (go-sql-driver, libmysql, ProxySQL) surface it as error
// 1040 rather than a bare TCP reset the operator can't attribute.
// Pre-handshake the CLIENT_PROTOCOL_41 capability is not negotiated
// yet, so the packet omits the '#'+SQLSTATE marker, matching mysqld's
// own pre-handshake ERR format. Caller closes the conn.
func refuseTooManyConnections(c net.Conn) {
	code := uint16(gomysql.ER_CON_COUNT_ERROR) // 1040
	msg := "Too many connections"
	payload := make([]byte, 0, 3+len(msg))
	payload = append(payload, 0xff, byte(code), byte(code>>8))
	payload = append(payload, msg...)
	pkt := make([]byte, 4, 4+len(payload))
	pkt[0] = byte(len(payload))
	pkt[1] = byte(len(payload) >> 8)
	pkt[2] = byte(len(payload) >> 16)
	pkt[3] = 0 // sequence id of the server's first packet
	pkt = append(pkt, payload...)
	// Best-effort with a short deadline: never let a slow/hostile peer
	// wedge the accept loop's goroutine on the refusal write.
	_ = c.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = c.Write(pkt)
}

const (
	// initialAcceptBackoff is the first sleep after a non-fatal Accept
	// error. Short enough that a single transient blip is invisible to
	// callers; long enough that a flapping error doesn't spin.
	initialAcceptBackoff = 100 * time.Millisecond
	// maxAcceptBackoff caps the exponential growth. 5s is the longest
	// we'll let an operator wait between "listener wedged" log lines
	// and the longest a SIGTERM can be delayed waiting on the sleep.
	maxAcceptBackoff = 5 * time.Second
)

// nextAcceptBackoff returns the next sleep interval given the current
// one. Doubles up to maxAcceptBackoff; the zero value (steady state
// after a successful Accept) seeds the first retry at
// initialAcceptBackoff.
//
// Pure function so the doubling + cap behaviour can be unit-tested
// without driving a real listener through error states.
func nextAcceptBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return initialAcceptBackoff
	}
	next := current * 2
	if next > maxAcceptBackoff {
		return maxAcceptBackoff
	}
	return next
}

// isLoopbackAddr reports whether addr resolves to a loopback address.
// Used at startup so the operator gets a loud warning if the shim is
// reachable from the network — the auth model assumes ProxySQL on the
// same host is the only legitimate caller.
func isLoopbackAddr(addr net.Addr) bool {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return false
	}
	if tcp.IP == nil || tcp.IP.IsUnspecified() {
		return false
	}
	return tcp.IP.IsLoopback()
}

// classifyHandshakeErr maps an error returned by go-mysql/server's
// handshake into the slog level and message we want to log it at.
//
// ProxySQL's monitor opens a TCP socket on the shim's listen port,
// reads nothing, then closes. go-mysql surfaces the resulting read
// failure as either a raw io.EOF / io.ErrUnexpectedEOF (rare paths
// outside packet/conn.go) or a pingcap-wrapped mysql.ErrBadConn (the
// usual case — every read in packet/conn.go is wrapped this way, and
// pingcap's Unwrap chain is errors.Is-compatible). Treat all three
// as the expected probe shape and demote to Debug so the steady-
// state log isn't a stack trace per probe.
//
// Auth failures from TenantAuth.GetCredential propagate as a
// *mysql.MyError with code ER_ACCESS_DENIED_ERROR (1045). Note that
// (*Conn).handshake also rewrites ErrAccessDeniedNoPassword (1698)
// into 1045 before returning, so checking the single 1045 code
// covers both no-password and bad-password cases — adding a 1698
// branch here would be dead code. Log auth failures at Info so an
// operator can correlate ProxySQL alerts with the shim's view of the
// failure without an alarming ERROR line per monitor probe.
//
// Exception: ProxySQL's built-in MySQL Monitor module opens a
// handshake every ~1-10s against every backend in its hostgroups
// (including the shim) using the username `monitor`. The shim does
// not — and must not — recognise this user, so the handshake fails
// with the same 1045 every probe interval, drowning real auth
// failures in the steady-state log. Demote `monitor` ER_ACCESS_DENIED
// to Debug; everything else stays at Info. See issue #316.
//
// Anything else is unexpected (bad packet, protocol mismatch, …) and
// stays at Error so it surfaces in alerting.
//
// Extracted as a pure function so the classification can be unit-
// tested with synthetic errors — without it, a future refactor that
// flips a branch would silently re-introduce the issue #262 log
// volume regression.
// classifyHandshakeErr returns (level, msg, isMonitor). The third value
// is true iff the error is a monitor-user 1045 — callers in handleConn
// use it to record the source remote into monitorDenied so the periodic
// emit carries distinct-remote forensics. Bumping inside classify would
// hide the remote (classify only sees the error).
func classifyHandshakeErr(err error) (slog.Level, string, bool) {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, gomysql.ErrBadConn):
		return slog.LevelDebug, "handshake aborted (likely TCP probe)", false
	}
	var myErr *gomysql.MyError
	if errors.As(err, &myErr) && myErr.Code == gomysql.ER_ACCESS_DENIED_ERROR {
		if isMonitorAuthFailure(myErr.Message) {
			return slog.LevelDebug, "mysql auth failed (proxysql monitor probe)", true
		}
		return slog.LevelInfo, "mysql auth failed", false
	}
	return slog.LevelError, "mysql handshake failed", false
}

// emitMonitorDenied snapshots monitorDenied, resets it, and emits an INFO
// line if the snapshot has at least one probe. Pulled out as a package-
// level function (not a closure) so unit tests can drive both code paths
// (empty and populated) directly without spinning a 5-minute ticker.
//
// The snapshot is taken under monitorDenied.mu so an in-flight bump can't
// race with the reset. Concurrent bumps that arrive AFTER the unlock land
// in the next interval — fine, the metric is steady-state aggregate, not
// per-second.
func emitMonitorDenied(logger *slog.Logger) {
	monitorDenied.mu.Lock()
	count := monitorDenied.count
	distinct := len(monitorDenied.distinctRemotes)
	firstSeen := monitorDenied.firstSeen
	lastSeen := monitorDenied.lastSeen
	monitorDenied.count = 0
	clear(monitorDenied.distinctRemotes)
	monitorDenied.firstSeen = time.Time{}
	monitorDenied.lastSeen = time.Time{}
	monitorDenied.mu.Unlock()

	if count == 0 {
		return
	}
	attrs := []any{
		"count", count,
		"distinct_remotes", distinct,
		"interval", monitorDeniedInterval.String(),
	}
	if !firstSeen.IsZero() {
		attrs = append(attrs, "first_seen", firstSeen.UTC().Format(time.RFC3339))
		attrs = append(attrs, "last_seen", lastSeen.UTC().Format(time.RFC3339))
	}
	if distinct == monitorDeniedRemoteCap {
		// Signal that the set is a floor, not the true count, so an
		// alerting query sees the saturation.
		attrs = append(attrs, "distinct_remotes_truncated_at", monitorDeniedRemoteCap)
	}
	logger.Info("monitor probes denied", attrs...)
}

// monitorDeniedTicker runs emitMonitorDenied on every tick until ctx is
// cancelled. Started as a goroutine from runShim; tied to the shim's
// serve context so shutdown is clean. A final emit on shutdown drains
// any counts accumulated since the last tick — without it, the operator
// would lose visibility into the last partial interval on a graceful
// SIGTERM.
func monitorDeniedTicker(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(monitorDeniedInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			emitMonitorDenied(logger)
			return
		case <-ticker.C:
			emitMonitorDenied(logger)
		}
	}
}

// isMonitorAuthFailure reports whether an ER_ACCESS_DENIED_ERROR
// message names ProxySQL's monitor user. The 1045 message is
// formatted as `Access denied for user '<user>'@'<host>' (using
// password: ...)` — see mysql.MySQLErrName[ER_ACCESS_DENIED_ERROR]
// in go-mysql v1.13.0. We extract the substring between the first
// pair of single quotes and compare case-insensitively against
// "monitor".
//
// The (*Conn).handshake path is the only producer of this exact
// message in the shim, so the format is stable — go-mysql doesn't
// localise mysql.MySQLErrName. A go-mysql upgrade that changes the
// format would silently route monitor failures back through the Info
// branch; the unit test below pins the parsing against the literal
// error go-mysql constructs today.
//
// Returns false on any parse failure — we'd rather over-log a probe
// than mistakenly silence a real auth failure for a tenant whose name
// happens to contain quote-adjacent metacharacters.
func isMonitorAuthFailure(msg string) bool {
	openQuote := strings.IndexByte(msg, '\'')
	if openQuote < 0 {
		return false
	}
	rest := msg[openQuote+1:]
	closeQuote := strings.IndexByte(rest, '\'')
	if closeQuote < 0 {
		return false
	}
	return strings.EqualFold(rest[:closeQuote], "monitor")
}

// buildUserSchemas derives the per-tenant default schema for shim
// connections by parsing each tenant's source_dsn /<db> path. The
// returned map is keyed by mysql_user — handleConn looks the schema
// up after handshake and seeds Handler.db so fully qualified
// time-travel queries work without a prior `USE <db>` (issue #263).
//
// A tenant whose source_dsn is empty, unparseable, or missing the
// /<db> path is omitted from the returned map (with a warning log).
// That keeps a single misconfigured tenant from blocking the rest;
// the affected user simply falls back to the pre-#263 behaviour of
// requiring an explicit USE. runShim emits a summary if the count of
// tenants-with-default-schema is below the total.
func buildUserSchemas(tenantCfgs []shim.TenantConfig) map[string]string {
	out := make(map[string]string, len(tenantCfgs))
	for _, t := range tenantCfgs {
		if t.SourceDSN == "" {
			continue
		}
		cfg, err := mysqldriver.ParseDSN(t.SourceDSN)
		if err != nil {
			slog.Warn(
				"shim.yaml: source_dsn is unparseable; fully qualified time-travel queries from this tenant will still require `USE <db>`",
				"mysql_user", t.MySQLUser, "err", err,
			)
			continue
		}
		if cfg.DBName == "" {
			slog.Warn(
				"shim.yaml: source_dsn has no database path; fully qualified time-travel queries from this tenant will still require `USE <db>`",
				"mysql_user", t.MySQLUser,
			)
			continue
		}
		out[t.MySQLUser] = cfg.DBName
	}
	return out
}

// buildUserAllowedSchemas delegates to shim.UserAllowedSchemas, which the
// PostgreSQL front-end also builds from (#1261) — one derivation, so the two
// front-ends cannot disagree about who is restricted.
//
// Original doc: maps mysql_user → allowed_schemas for the
// tenants that opted into schema isolation (#824). Tenants without the
// field are simply absent — handleConn's map lookup then yields nil,
// which Handler.BindAllowedSchemas treats as unrestricted (the exact
// pre-#824 behaviour).
func buildUserAllowedSchemas(tenantCfgs []shim.TenantConfig) map[string][]string {
	return shim.UserAllowedSchemas(tenantCfgs)
}

// tenantsWithoutAllowedSchemas lists the mysql_users that have no
// allowed_schemas allowlist. runShim warns once at startup when the
// config has more than one tenant and this list is non-empty: in a
// multi-tenant shim, an unrestricted tenant can read every other
// tenant's indexed history. Pure function so the classification is
// unit-testable without a listener.
func tenantsWithoutAllowedSchemas(tenantCfgs []shim.TenantConfig) []string {
	return shim.TenantsWithoutAllowedSchemas(tenantCfgs)
}

// handleConn wraps one accepted TCP connection in go-mysql/server's
// Conn (which performs the MySQL handshake + auth) and dispatches
// every COM_QUERY through our Handler.
//
// userSchemas seeds the handler's default DB from the authenticated
// tenant's source_dsn so fully qualified time-travel queries
// (`_flashback.<table>`) succeed without a prior `USE <db>` (issue
// #263). The seed runs only after a successful handshake — pre-auth
// we don't know the tenant — and an explicit `USE` from the client
// still wins because UseDB is called sequentially and overwrites the
// seeded value.
//
// ctx is the daemon's serve context; handleConn derives a
// per-connection context from it via shim.WatchConn (#823) and binds it to
// the Handler, so a client disconnect OR a daemon shutdown cancels any
// in-flight FetchMerged instead of letting it run to completion for a
// reader that no longer exists.
func handleConn(ctx context.Context, c net.Conn, db *sql.DB, srv *server.Server, auth shim.TenantAuth, cfg shim.Config, userSchemas map[string]string, userAllowedSchemas map[string][]string) {
	defer c.Close()

	wc, connCtx, stop := shim.WatchConn(ctx, c)
	defer stop()
	// Close the socket the moment the connection context dies — the
	// parent ctx on SIGTERM, or the pump's disconnect detection.
	// Without this, a graceful shutdown would block in wg.Wait forever
	// on idle clients parked in HandleCommand's read.
	stopCloser := context.AfterFunc(connCtx, func() { c.Close() })
	defer stopCloser()

	handler := shim.NewHandlerWithConfig(db, cfg, slog.Default())
	handler.BindConnContext(connCtx)
	mysqlConn, err := server.NewCustomizedConn(wc, srv, auth, handler)
	if err != nil {
		level, msg, isMonitor := classifyHandshakeErr(err)
		if isMonitor {
			monitorDeniedBump(c.RemoteAddr().String())
		}
		slog.Log(context.Background(), level, msg, "err", err, "remote", c.RemoteAddr())
		return
	}
	// Let the full-table _snapshot path stream its resultset over this
	// connection instead of buffering it and tripping FullTableRowCap (#998).
	handler.BindConn(mysqlConn)
	// Attribute every time-travel query on this connection to the tenant that
	// authenticated (ext.AuditEvent.Actor) — the shim is a network surface with
	// real per-tenant credentials, so it records its authenticated user rather
	// than the process owner. Post-handshake: GetUser is only meaningful here.
	handler.BindActor(mysqlConn.GetUser())
	// Bind the tenant's allowed_schemas allowlist (#824) BEFORE seeding the
	// default schema, so a source_dsn-derived default that the operator
	// excluded from allowed_schemas is refused rather than silently trusted.
	handler.BindAllowedSchemas(userAllowedSchemas[mysqlConn.GetUser()])
	if schema, ok := userSchemas[mysqlConn.GetUser()]; ok && schema != "" {
		if err := handler.UseDB(schema); err != nil {
			slog.Warn(
				"shim: tenant's default schema (derived from source_dsn) is outside its allowed_schemas; no default schema seeded — queries from this tenant must name an allowed schema",
				"mysql_user", mysqlConn.GetUser(), "schema", schema,
			)
		}
	}
	for {
		if err := mysqlConn.HandleCommand(); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Debug("connection ended", "err", err, "remote", c.RemoteAddr())
			}
			return
		}
	}
}
