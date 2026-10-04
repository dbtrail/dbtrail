package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
	// The port resolves SET time_zone names (#2035). Embedded here, in the
	// package every console binary is built from, so a host with no zoneinfo
	// (a slim container image) still knows the zones; not in internal/shim,
	// which the core CLI links too and which never resolves one there.
	_ "time/tzdata"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/shim"
)

// This file is the MySQL-protocol serving layer for the embedded time-travel
// port (issue #996). It lives in the binary — NOT in internal/console — because
// it links go-mysql and internal/shim, which the read-layer guard (#528,
// TestReadLayerDoesNotLinkGoMySQL) bars from internal/console. It consumes the
// go-mysql-free seam console.Server exposes (Token, ResolveFlashback).

// flashbackConfig tunes the embedded time-travel port. The zero value is the
// production default: strict (a coverage gap / archive-fetch failure aborts the
// client's query), a 5-minute per-query deadline, and at most 4 concurrent
// full-table reconstructions.
type flashbackConfig struct {
	// AllowGaps mirrors shim.Config.AllowGaps: false (default) fails a query
	// loudly on a coverage gap / archive failure; true downgrades to a
	// server-side warning and returns partial rows.
	AllowGaps bool
	// QueryTimeout bounds each time-travel query end-to-end. It is the ONLY
	// backstop against a runaway query on a dropped connection here — unlike the
	// standalone shim, the embedded port runs no client-disconnect detection
	// pump — so it must never be 0. withDefaults substitutes the 5-minute
	// default when this is 0.
	QueryTimeout time.Duration
	// MaxFullTable caps concurrent full-table reconstructions across every
	// connection of this port (0 → 4, the standalone default): a shared gate,
	// exactly like the standalone shim's --max-fulltable-queries.
	MaxFullTable int
	// AuthMethod selects the MySQL auth plugin the port advertises. Empty
	// (default) keeps mysql_native_password; see shim.NewMySQLServer for values.
	AuthMethod string
	// RouteMaxCopyAge turns read routing on (#2038): with a non-zero value,
	// a connection to a server that has a source DSN forwards every
	// statement to that MySQL except SELECTs whose EXPLAIN says they are
	// expensive, which run on the copy when the copy's snapshot is at most
	// this old. Zero (default) = the port serves the copy only, as before.
	RouteMaxCopyAge time.Duration
	// RoutePolicy is the threshold EXPLAIN is read against. The zero value
	// never routes to the copy; watch passes readrouter.DefaultPolicy()
	// adjusted by its flags.
	RoutePolicy readrouter.Policy
	// RouteReadOnly makes the routed port read-only (#2079): the handler
	// refuses every statement that is not a read before the source sees it.
	RouteReadOnly bool
}

const (
	defaultFlashbackQueryTimeout = 5 * time.Minute
	defaultFlashbackMaxFullTable = 4
)

// withDefaults resolves the zero-value fields to their production defaults.
// QueryTimeout is the load-bearing one: 0 means "unbounded", but the embedded
// port has no client-disconnect pump, so a runaway query on a dropped
// connection would never be reclaimed — the 5-minute default is the only
// backstop. serveFlashback always calls this, so the dangerous zero never
// reaches the query path; keeping both substitutions here (rather than one
// inline and one in a side variable) is why they can't drift apart.
func (c flashbackConfig) withDefaults() flashbackConfig {
	if c.QueryTimeout == 0 {
		c.QueryTimeout = defaultFlashbackQueryTimeout
	}
	if c.MaxFullTable == 0 {
		c.MaxFullTable = defaultFlashbackMaxFullTable
	}
	return c
}

// serveFlashback runs a MySQL-protocol time-travel server on ln, routing every
// connection to a monitored server's per-source index by the connection's
// USERNAME (issue #996). Reuse of the console's already-resolved connManager
// (via console.Server.ResolveFlashback) is the whole point: `_flashback` /
// `_snapshot` / `_diff` resolve against the same per-source index + baseline the
// console's Time-travel tab shows, with no separate container and no hand-built
// INDEX_DSN.
//
// Routing: the MySQL username selects the target server — its registry ID, its
// display Name, or "default" for the command-line boot entry. The console's
// static --token is the shared password for every server (the operator can see
// all servers in the UI, so server selection is not a security boundary; the
// token is). The client therefore connects as, e.g., `-u <server-id> -p<token>`.
//
// Auth requires a token: go-mysql validates the handshake by recomputing the
// scramble from the cleartext the credential carries (Credential.Passwords are
// raw and hashed on demand in go-mysql server's auth flow), and the console's
// bcrypt password store cannot produce that cleartext. Callers that leave
// --token empty get an error here rather than an unauthenticated port.
//
// Blocks until ctx is cancelled (which closes ln and drains in-flight
// connections) or ln fails unrecoverably.
func serveFlashback(ctx context.Context, srv *console.Server, ln net.Listener, cfg flashbackConfig) error {
	if len(srv.FlashbackPasswords()) == 0 {
		// The UI-managed MCP token (#1052) is deliberately NOT accepted here:
		// only its SHA-256 is stored, which cannot drive mysql_native_password
		// authentication. This port needs a password it can read: the STATIC
		// token, or the one the web interface generates for the port (#2101).
		_ = closeListener(ln)
		return errors.New("flashback port requires the static automation token, or a port password created in the web interface: set --token or BINTRAIL_CONSOLE_TOKEN (the web interface password store and the UI-managed MCP token cannot drive MySQL-protocol authentication)")
	}
	cfg = cfg.withDefaults()

	// One *server.Server for the whole port: it owns the caching_sha2_password
	// cache and the RSA keypair (see shim.NewMySQLServer). One shared *Gate so
	// the full-table cap is process-wide, not per-connection.
	mysrv, err := shim.NewMySQLServer(cfg.AuthMethod)
	if err != nil {
		// ln is already bound by the caller; close it so a construction failure
		// (an unsupported auth method) doesn't leak the listener nor leave the
		// caller's "listening" banner pointing at a port that rejects every
		// handshake. Unreachable from `watch` today (it passes an empty
		// AuthMethod), but a latent trap if a --flashback-auth-method flag lands.
		_ = ln.Close()
		return fmt.Errorf("flashback: %w", err)
	}
	gate := shim.NewGate(cfg.MaxFullTable)
	// The credential must advertise the same plugin mysrv was built with —
	// go-mysql auth-switches the client to the credential's plugin on mismatch.
	creds := flashbackCreds{srv: srv, authMethod: cfg.AuthMethod}
	logger := slog.Default()

	// Close the listener when the daemon context ends so Accept unblocks and
	// the loop returns; the deferred wg.Wait then drains open connections.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	// No per-connection cap (the standalone shim's --max-connections has no
	// equivalent here): this is a loopback-default, token-gated operator port,
	// and the shared FullTableGate already bounds the memory-heavy queries.
	var wg sync.WaitGroup
	defer wg.Wait()

	var backoff time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil // graceful shutdown
			}
			backoff = nextFlashbackBackoff(backoff)
			logger.Error("flashback accept failed", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			continue
		}
		backoff = 0
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			handleFlashbackConn(ctx, srv, c, mysrv, creds, gate, cfg, logger)
		}(conn)
	}
}

// handleFlashbackConn performs the handshake, then binds the connection's
// Handler to the server the authenticated username selects. The Handler cannot
// be built before the handshake — the username is only known afterwards, yet
// go-mysql binds the Handler at construction — so a routingHandler proxy is
// passed to NewCustomizedConn and its inner *shim.Handler is set once GetUser()
// reveals the target. Commands are dispatched sequentially in this goroutine
// strictly after that, so no synchronisation is needed.
func handleFlashbackConn(ctx context.Context, srv *console.Server, c net.Conn, mysrv *server.Server, creds server.AuthenticationHandler, gate *shim.Gate, cfg flashbackConfig, logger *slog.Logger) {
	defer c.Close()

	// The connection context ends when the daemon context dies (SIGTERM), when
	// this goroutine returns, or when the client hangs up, even in the middle
	// of a statement (#2033): WatchConn reads the socket on its own goroutine,
	// so the disconnect is seen while nothing else is reading. BindConnContext
	// (connCtx) below ties every in-flight statement to it, so a client that
	// leaves stops its SQL worker and frees the server's slot at once instead
	// of holding it until the query cap. The socket is closed with it, so a
	// graceful shutdown and a stalled handshake can't wedge serveFlashback's
	// wg.Wait.
	wc, connCtx, stop := shim.WatchConn(ctx, c)
	defer stop()
	stopCloser := context.AfterFunc(connCtx, func() { _ = c.Close() })
	defer stopCloser()

	proxy := &routingHandler{}
	mysqlConn, err := server.NewCustomizedConn(wc, mysrv, creds, proxy)
	if err != nil {
		level := slog.LevelWarn
		if isFlashbackProbe(err) {
			// Bare TCP probes (health checks, port scanners) close before the
			// handshake completes; don't log those at WARN.
			level = slog.LevelDebug
		}
		logger.Log(context.Background(), level, "flashback handshake failed", "err", err, "remote", c.RemoteAddr())
		return
	}

	if err := bindFlashbackHandler(connCtx, srv, proxy, mysqlConn, gate, cfg, logger); err != nil {
		// Auth already succeeded; surface the routing failure on the client's
		// first query (a typed MySQL error) rather than a bare disconnect.
		proxy.fail = err
	}
	// The handler may hold an upstream MySQL connection (read routing); it
	// ends with this one.
	defer func() {
		if proxy.inner != nil {
			proxy.inner.Close()
		}
	}()

	session := shim.NewSession(mysqlConn, proxy)
	for {
		if err := session.HandleCommand(); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				logger.Debug("flashback connection ended", "err", err, "remote", c.RemoteAddr())
			}
			return
		}
	}
}

// bindFlashbackHandler resolves the target server from the authenticated
// username (via the console's go-mysql-free seam) and wires proxy.inner to a
// shim.Handler bound to that server's per-source index + baseline. A returned
// error is a typed *mysql.MyError the caller stores on the proxy so the client
// sees it on the first query.
func bindFlashbackHandler(ctx context.Context, srv *console.Server, proxy *routingHandler, mysqlConn *server.Conn, gate *shim.Gate, cfg flashbackConfig, logger *slog.Logger) error {
	user := mysqlConn.GetUser()
	tgt, err := srv.ResolveFlashback(ctx, user)
	if err != nil {
		if errors.Is(err, console.ErrUnknownServer) {
			// Auth is the token alone (CheckUsername accepts any username), so an
			// unknown or typo'd server name is the normal way we land here —
			// report it as a missing database on the client's first query.
			return gomysql.NewError(gomysql.ER_BAD_DB_ERROR, fmt.Sprintf("flashback: no such server %q", user))
		}
		// The connManager already scrubbed DSN secrets from the open error.
		return gomysql.NewError(gomysql.ER_UNKNOWN_ERROR, fmt.Sprintf("flashback: cannot open server %q: %s", user, err))
	}

	shimCfg := shim.Config{
		IndexDBName:   tgt.IndexDBName,
		NoArchive:     tgt.NoArchive,
		AllowGaps:     cfg.AllowGaps,
		QueryTimeout:  cfg.QueryTimeout,
		FullTableGate: gate,
		AuthMethod:    cfg.AuthMethod,
		// Already split by scheme and dir-preferred by ResolveFlashback so
		// `_snapshot` matches the console's Time-travel tab.
		BaselineDir: tgt.BaselineDir,
		BaselineS3:  tgt.BaselineS3,
	}

	h := shim.NewHandlerWithConfig(tgt.IndexDB, shimCfg, logger)
	h.BindConnContext(ctx)
	// Stream full-table _snapshot resultsets over this connection instead of
	// buffering them and tripping FullTableRowCap (#998). The conn is bound to
	// the INNER handler (routingHandler forwards HandleQuery to it), so its
	// streamed rows and go-mysql's trailing EOF share one packet sequence.
	h.BindConn(mysqlConn)
	// The flashback port routes BY username: flashbackCreds accepts ANY username
	// and authenticates on the shared console token alone, so `user` is a server
	// selector (registry id/name, or the reserved boot id), not a person — every
	// human holding the token connects under whatever server name they target.
	// It is still bound as the audit identity (without it every event from this
	// port would carry the unbound-actor sentinel), but PREFIXED so a sink
	// treating Actor as a principal cannot attribute the read to a "person"
	// named after a database (#1123). The standalone shim and the PG front-end
	// bind their real per-tenant credential, unprefixed.
	h.BindActor("server:" + user)

	// Free SQL on the copy (freesql.go): the console's own executor when
	// the port can offer it, else the reason the 1235 refusal will carry.
	if tgt.SQL != nil {
		h.BindFreeSQL(tgt.SQL)
	} else {
		h.BindFreeSQLUnavailable(tgt.SQLUnavailable)
	}
	// Read routing (#2038): one upstream MySQL connection per client
	// connection, opened on first use with the registry's source
	// credentials. Bound BEFORE the schema seeds below so the upstream
	// follows them. A server that cannot route stays copy-only and says so
	// once per connection: silently serving the copy to a client who was
	// promised MySQL semantics is the one thing this must not do.
	bindReadRouter(h, srv, tgt, user, cfg, logger)
	// Seed the source schema so fully qualified `_flashback.<table>` queries
	// work without a prior `USE <db>` (mirrors the standalone shim's #263
	// behaviour). Best-effort: the boot entry has no registry SourceDSN.
	if tgt.DefaultSchema != "" {
		_ = h.UseDB(tgt.DefaultSchema)
	}
	// A default schema the client sent in the handshake (CLIENT_CONNECT_WITH_DB,
	// stashed on the proxy before inner was bound) wins over the SourceDSN seed,
	// matching the standalone shim where an explicit client USE overrides the
	// #263 seed.
	if proxy.pendingDB != "" {
		_ = h.UseDB(proxy.pendingDB)
	}
	proxy.inner = h
	return nil
}

// bindReadRouter turns h into a routing handler when read routing is on and
// the server can route, with the port's policy: the copy's maximum age and
// whether the port is read-only (cfg.RouteReadOnly, #2079). A server that
// cannot route stays copy-only, and the Connect page is told why. Its own
// function so the wiring from the port's configuration to the handler is
// tested without a database: nothing here opens a connection.
func bindReadRouter(h *shim.Handler, srv *console.Server, tgt console.FlashbackTarget, user string, cfg flashbackConfig, logger *slog.Logger) {
	if cfg.RouteMaxCopyAge <= 0 {
		return
	}
	switch {
	case tgt.SQL == nil:
		logger.Warn("read routing off for this connection: SQL on the copy unavailable", "server", user, "reason", tgt.SQLUnavailable)
		srv.RecordRouteUnavailable(tgt.ID, "SQL on the copy is unavailable ("+tgt.SQLUnavailable+")")
	case tgt.SourceDSN == "":
		logger.Warn("read routing off for this connection: the server has no source DSN to forward to", "server", user)
		srv.RecordRouteUnavailable(tgt.ID, "the server has no source database to forward to")
	default:
		fw, err := readrouter.NewForwarder(tgt.SourceDSN, cfg.RoutePolicy, cfg.QueryTimeout)
		if err != nil {
			// The DSN is the registry's own and was parsed to open the
			// source; a scheme the forwarder does not speak is the
			// realistic cause. The message carries no secret.
			logger.Warn("read routing off for this connection", "server", user, "err", err)
			srv.RecordRouteUnavailable(tgt.ID, "the source address cannot be forwarded to ("+err.Error()+")")
		} else {
			srv.RecordRouteAvailable(tgt.ID)
			// Every decision is counted twice over, by the canonical
			// server id: the Prometheus counter for dashboards and
			// the console's tally for the Connect page (#2038).
			id := tgt.ID
			h.BindRouter(fw, shim.RouterConfig{
				MaxCopyAge: cfg.RouteMaxCopyAge,
				ReadOnly:   cfg.RouteReadOnly,
				Observe: func(route shim.RouteSide, reason shim.RouteReason) {
					observe.ObserveRouteDecision(id, string(route), string(reason))
					srv.RecordRouteDecision(id, string(route), string(reason))
				},
			})
		}
	}
}

// flashbackCreds authenticates the flashback port on the shared console token
// alone: every username is accepted at the handshake (so the error code cannot
// enumerate servers), and the target server is validated AFTER the handshake in
// bindFlashbackHandler via console.Server.ResolveFlashback. It reads srv.Token()
// live rather than snapshotting it.
type flashbackCreds struct {
	srv *console.Server
	// authMethod mirrors flashbackConfig.AuthMethod ("" = native) — the
	// plugin the returned Credential advertises must match the one the
	// port's *server.Server was built with, or go-mysql auth-switches
	// every client.
	authMethod string
}

// GetCredential implements server.AuthenticationHandler. It returns the shared
// console token for EVERY username, so auth turns on the token alone — deciding
// username validity here would leak which usernames name a real server through
// the handshake's error code (unknown-user vs bad-password), letting an
// unauthenticated client enumerate monitored servers; the target server is
// validated AFTER the handshake instead (bindFlashbackHandler). found=false on
// an empty token is defence in depth behind serveFlashback's startup guard: an
// empty token must never authorise a passwordless MySQL handshake.
func (f flashbackCreds) GetCredential(username string) (server.Credential, bool, error) {
	// Read per handshake, so a password replaced in the web interface is the
	// one the next client needs.
	passwords := f.srv.FlashbackPasswords()
	if len(passwords) == 0 {
		return server.Credential{}, false, nil
	}
	plugin := f.authMethod
	if plugin == "" {
		plugin = gomysql.AUTH_NATIVE_PASSWORD
	}
	return server.Credential{Passwords: passwords, AuthPluginName: plugin}, true, nil
}

// OnAuthSuccess and OnAuthFailure are server.AuthenticationHandler lifecycle
// hooks; this port needs neither (routing happens in bindFlashbackHandler,
// failure logging in handleFlashbackConn).
func (flashbackCreds) OnAuthSuccess(*server.Conn) error  { return nil }
func (flashbackCreds) OnAuthFailure(*server.Conn, error) {}

// routingHandler is the go-mysql Handler passed to NewCustomizedConn before the
// authenticated username (and thus the target server) is known. Its inner
// *shim.Handler is set by bindFlashbackHandler once the handshake completes; if
// routing failed, fail carries the typed error returned on the first command.
// server.EmptyHandler supplies the commands shim.Handler itself does not
// implement (field-list) — identical to a bare shim.Handler, which embeds
// the same EmptyHandler.
type routingHandler struct {
	server.EmptyHandler
	inner *shim.Handler
	fail  error
	// pendingDB holds a default schema the client sent in the handshake
	// (CLIENT_CONNECT_WITH_DB): go-mysql invokes UseDB DURING the handshake,
	// before bindFlashbackHandler binds inner. Stashing it (and returning nil)
	// lets the handshake complete instead of failing it; bindFlashbackHandler
	// replays it onto the real handler. Without this, any client that connects
	// with a default schema (mysql -D, a DSN /db path, JDBC) is rejected before
	// auth even runs.
	pendingDB string
}

func (r *routingHandler) UseDB(dbName string) error {
	switch {
	case r.inner != nil:
		return r.inner.UseDB(dbName)
	case r.fail != nil:
		// Routing failed after the handshake; reject a later USE too so the
		// failure is consistent across commands.
		return r.fail
	default:
		// Pre-bind: go-mysql calls UseDB during the handshake, before inner is
		// set. Stash it (bindFlashbackHandler replays it) and let the handshake
		// complete rather than aborting the connection.
		r.pendingDB = dbName
		return nil
	}
}

func (r *routingHandler) HandleQuery(query string) (*gomysql.Result, error) {
	if r.inner == nil {
		return nil, r.unresolved()
	}
	return r.inner.HandleQuery(query)
}

// Prepared statements (#2036) go to the bound handler like queries do; the
// embedded EmptyHandler's own versions refuse them and write the statement
// and its arguments to the process log, which nothing here should do.
func (r *routingHandler) HandleStmtPrepare(query string) (int, int, any, error) {
	if r.inner == nil {
		return 0, 0, nil, r.unresolved()
	}
	return r.inner.HandleStmtPrepare(query)
}

func (r *routingHandler) HandleStmtExecute(context any, query string, args []any) (*gomysql.Result, error) {
	if r.inner == nil {
		return nil, r.unresolved()
	}
	return r.inner.HandleStmtExecute(context, query, args)
}

func (r *routingHandler) HandleStmtClose(context any) error {
	if r.inner == nil {
		return nil
	}
	return r.inner.HandleStmtClose(context)
}

func (r *routingHandler) unresolved() error {
	if r.fail != nil {
		return r.fail
	}
	return gomysql.NewError(gomysql.ER_UNKNOWN_ERROR, "flashback: no server bound to this connection")
}

// isFlashbackProbe reports whether a handshake error is a bare TCP probe
// (health check / port scan) that closed before completing — logged at Debug
// rather than Warn. Mirrors the `bintrail shim` command's classifyHandshakeErr
// probe arm (internal/cli/shim.go) without the ProxySQL-monitor aggregator,
// which does not apply to an embedded loopback port.
func isFlashbackProbe(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, gomysql.ErrBadConn)
}

const (
	initialFlashbackBackoff = 100 * time.Millisecond
	maxFlashbackBackoff     = 5 * time.Second
)

// nextFlashbackBackoff doubles the accept-retry sleep up to a cap; the zero
// value seeds the first retry. Keeps a wedged listener from spinning the CPU or
// flooding the log without delaying a SIGTERM more than the cap.
func nextFlashbackBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return initialFlashbackBackoff
	}
	if next := current * 2; next < maxFlashbackBackoff {
		return next
	}
	return maxFlashbackBackoff
}

// closeListener closes ln if there is one (tests pass nil to reach the
// password guard without binding).
func closeListener(ln net.Listener) error {
	if ln == nil {
		return nil
	}
	return ln.Close()
}

// flashbackControl opens and closes the port while the daemon runs, so the
// web interface can turn it on and off (#2101). It is console.FlashbackController.
// One port at a time: each run serves on its own child of the daemon context,
// and cancelling that child closes the listener and its connections.
type flashbackControl struct {
	ctx context.Context
	srv *console.Server
	cfg flashbackConfig
	// cfgErr is a startup setting that makes the port unservable (the read
	// routing flags contradict each other). It refuses every open, with the
	// reason, instead of stopping a daemon whose port may never be turned on.
	cfgErr error

	mu     sync.Mutex
	addr   string // as asked for; "" = closed
	bound  net.Addr
	cancel context.CancelFunc
	done   chan struct{}
}

// Apply makes the port listen on listen, or closes it when listen is empty.
// The new address is bound before the old one is given up, so a refused
// address leaves the port as it was. The one case that cannot be done in that
// order is a change of host on the same port number: there the old listener
// goes first, and comes back if the new one is refused.
func (c *flashbackControl) Apply(listen string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if listen == c.addr {
		return nil
	}
	if listen == "" {
		c.stopLocked()
		return nil
	}
	if c.cfgErr != nil {
		return c.cfgErr
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil && c.addr != "" {
		was := c.addr
		c.stopLocked()
		if ln, err = net.Listen("tcp", listen); err != nil {
			if old, oldErr := net.Listen("tcp", was); oldErr == nil {
				c.serveLocked(was, old)
			} else {
				slog.Warn("flashback port could not be reopened on its previous address", "listen", was, "error", oldErr)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("cannot bind %s: %w", listen, err)
	}
	c.stopLocked()
	c.serveLocked(listen, ln)
	return nil
}

func (c *flashbackControl) serveLocked(addr string, ln net.Listener) {
	ctx, cancel := context.WithCancel(c.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := serveFlashback(ctx, c.srv, ln, c.cfg); err != nil {
			slog.Warn("flashback port exited with error", "error", err)
		}
	}()
	c.addr, c.bound, c.cancel, c.done = addr, ln.Addr(), cancel, done
}

func (c *flashbackControl) stopLocked() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	<-c.done
	c.addr, c.bound, c.cancel, c.done = "", nil, nil, nil
}

// Close stops the port and waits for its connections: the daemon's shutdown
// drain.
func (c *flashbackControl) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopLocked()
}

// boundAddr is the address actually listened on (an ephemeral port resolved).
func (c *flashbackControl) boundAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bound
}
