// Package pgshim is the PostgreSQL wire-protocol front-end for bintrail's
// time-travel engine (#1008). It lets a PostgreSQL operator run single-row
// AS OF queries from psql / a PG driver:
//
//	psql "host=127.0.0.1 port=5433 user=<tenant> dbname=<schema>"
//	SELECT * FROM _flashback.orders AS OF '5 minutes ago' WHERE id = 42;
//	SELECT * FROM _snapshot.orders  AS OF '5 minutes ago' WHERE id = 42;
//	SELECT * FROM orders AS OF '5 minutes ago' WHERE id = 42;   -- bare form
//
// and get rows back over the PostgreSQL protocol, served from the SAME
// out-of-band index + baseline the MySQL shim uses. It reuses internal/shim
// verbatim: the parser (shim.Parse → a protocol-neutral TimeTravelQuery) and the
// wire-neutral resolve seam (Handler.ResolveFlashbackRow / ResolveSnapshotRow /
// ColumnsFor / PKColumnCheck, resolve.go). Only the render is PostgreSQL-typed
// here — RowDescription / DataRow / CommandComplete instead of *mysql.Result.
//
// Scope (matches the current PostgreSQL time-travel maturity, #593):
//   - single-row AS OF (WHERE <pk> = <value>) for _flashback and _snapshot.
//   - full-table AS OF is REFUSED with actionable remediation (the PostgreSQL
//     baseline still omits CREATE TABLE, slice E) — never a silent partial.
//   - _diff is refused (use `bintrail-pg reconstruct --history`).
//   - the simple and the extended query protocol (#2767, extended.go): named
//     and unnamed statements and portals, $n parameters written into the
//     statement as literals (params.go), Describe before Execute, Execute
//     with a row limit, and PostgreSQL's discard-until-Sync error rule.
//
// Addressing: the connect `database` parameter is the bintrail SCHEMA (the
// PostgreSQL schema name, e.g. `public`); flashback-vs-snapshot semantics are
// selected by the `_flashback.` / `_snapshot.` table prefix in the query, exactly
// like the MySQL shim. (This differs from the issue's tentative
// `dbname=_flashback` note, which the engine's schema-from-connection model does
// not support without a parser change; the query-prefix model needs none and is
// strictly more capable — one connection reaches every virtual schema.)
//
// Login is SCRAM-SHA-256 (scram.go): the client proves it knows the password,
// which never crosses the wire. With Config.TLS the port also serves TLS
// (negotiate.go) and offers SCRAM-SHA-256-PLUS on it (#2766); without, a
// request for TLS is answered 'N'. Package placement is deliberate: pgproto3 is
// github.com/jackc/pgx/v5/pgproto3, which the cliapp pgfree guard bans from the
// core bintrail binary, so this front-end is linked ONLY by cmd/bintrail-pg.
package pgshim

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/shim"
)

// pgtypeText is the PostgreSQL type OID for `text`. The first cut renders every
// column as text (the issue's conservative choice): a numeric value goes on the
// wire as its literal string, bytea/blob bytes verbatim, timestamps formatted.
// A client reads each column as a string (or text-parses it into a typed target,
// which pgx does). Refining per-column OIDs is a follow-up.
const pgtypeText = uint32(25)

// fullTableRefusalMsg is returned for a full-table AS OF (no PK-qualified
// WHERE). Full-table PostgreSQL time-travel is deferred (slice E: the PostgreSQL
// baseline omits CREATE TABLE, #593/#901), so we refuse loudly with a path
// forward rather than serve a partial/empty table.
const fullTableRefusalMsg = "full-table AS OF is not supported over the PostgreSQL wire front-end " +
	"(PostgreSQL full-table time-travel is deferred — the baseline omits CREATE TABLE, issue #593 slice E); " +
	"query a single row with WHERE <primary-key> = <value>, or use `bintrail-pg reconstruct` / the console for full-table state"

// Config carries everything the front-end needs. The engine lives in
// internal/shim; this package only speaks the wire.
type Config struct {
	// IndexDB is the open, migrated bintrail index the engine reads.
	IndexDB *sql.DB
	// ShimConfig is the engine config (IndexDBName, BaselineDir/S3, NoArchive,
	// AllowGaps, QueryTimeout). FullTableGate is unused here — full-table is
	// refused — and AuthMethod is a MySQL concept, also unused.
	ShimConfig shim.Config
	// Auth holds each tenant's password (loaded from shim.yaml), which a
	// client's SCRAM proof is checked against.
	Auth shim.TenantAuth
	// AllowedSchemas maps tenant username → allowed_schemas (#824/#1261),
	// built with shim.UserAllowedSchemas from the same shim.yaml the MySQL
	// front-end loads. A username ABSENT from the map is unrestricted, which
	// is what an empty/omitted allowed_schemas means; nil disables the gate
	// for every tenant.
	AllowedSchemas map[string][]string
	// Login, when set, makes this a port that runs SQL on the copy (login.go):
	// it decides who may connect and what the connection is bound to, and
	// IndexDB, ShimConfig, Auth and AllowedSchemas above are not read.
	Login LoginFunc
	// Logger; nil → slog.Default().
	Logger *slog.Logger
	// MaxConns caps concurrent connections; 0 = unlimited.
	MaxConns int
	// ResultBudget bounds the results every connection of the port holds
	// while it sends them (resultbudget.go); nil = no bound. One for the
	// port: the config is copied to every connection, the budget it points
	// at is shared.
	ResultBudget *ResultBudget
	// TLS, when set, is the certificate a client that asks for encryption
	// gets (sslmode=prefer or stricter), with SCRAM-SHA-256-PLUS offered on
	// it. Nil: a request for TLS is answered 'N'. The port offers TLS and
	// never requires it: a client that must not talk in clear text says so
	// (sslmode=require or stricter). Not changed by the port.
	TLS *tls.Config
}

// Serve accepts PostgreSQL wire-protocol connections until ctx is canceled or
// the listener closes. Mirrors internal/cli.serveLoop: one goroutine per
// connection, each with its own shim.Handler; exponential accept backoff so a
// transient listener hiccup does not burn CPU; an optional connection cap.
func Serve(ctx context.Context, ln net.Listener, cfg Config) error {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	pt := newPortTLS(cfg.TLS)

	var wg sync.WaitGroup
	defer wg.Wait()

	var sem chan struct{}
	if cfg.MaxConns > 0 {
		sem = make(chan struct{}, cfg.MaxConns)
	}

	// How many refusals for a full port may be in the middle of being said at
	// once; past it a connection is closed without a word.
	refusals := make(chan struct{}, 16)

	var backoff time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil // graceful shutdown
			}
			backoff = nextBackoff(backoff)
			logger.Error("pgshim accept failed", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			continue
		}
		backoff = 0
		if sem != nil {
			select {
			case sem <- struct{}{}:
			default:
				logger.Warn("pgshim connection refused: the connection limit is reached",
					"remote", conn.RemoteAddr(), "max_connections", cfg.MaxConns)
				refuseFull(conn, cfg.MaxConns, refusals, pt)
				continue
			}
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			if sem != nil {
				defer func() { <-sem }()
			}
			handleConn(ctx, c, cfg, pt, logger)
		}(conn)
	}
}

// refuseFull tells a client that the port has all the connections it takes,
// in an error it can read (53300, too_many_connections), and closes. The
// error can only be sent as the answer to the client's start-up message, so
// that is read first, for at most a few seconds, on a goroutine of its own:
// the accept loop does not wait on a client that sends nothing. When too many
// refusals are already being said the connection is closed without one. A
// client that asks for TLS gets it first, so one that requires it reads the
// refusal instead of a failed negotiation.
func refuseFull(c net.Conn, max int, refusals chan struct{}, pt *portTLS) {
	select {
	case refusals <- struct{}{}:
	default:
		_ = c.Close()
		return
	}
	go func() {
		defer func() { <-refusals }()
		defer c.Close()
		// What is read here comes from a client nobody identified: a fault
		// reading it ends this refusal, not the process.
		defer func() { _ = recover() }()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, _, err := negotiate(ctx, c, pt)
		if err != nil {
			return
		}
		defer conn.Close()
		be := pgproto3.NewBackend(conn, conn)
		if startup, err := receiveStartup(be); err != nil || startup == nil {
			return
		}
		sendFatal(be, "53300", fmt.Sprintf("the port's limit of %d connections at once is reached; try again in a moment", max))
	}()
}

// nextBackoff mirrors the MySQL serveLoop's accept backoff: 100ms → 5s cap.
func nextBackoff(cur time.Duration) time.Duration {
	const cap = 5 * time.Second
	if cur == 0 {
		return 100 * time.Millisecond
	}
	if n := cur * 2; n < cap {
		return n
	}
	return cap
}

// handleConn runs one connection: startup negotiation (TLS when the port has
// it), the SCRAM login, then the command loop over its own shim.Handler.
func handleConn(ctx context.Context, c net.Conn, cfg Config, pt *portTLS, logger *slog.Logger) {
	if cfg.Login != nil {
		handleLoginConn(ctx, c, cfg, pt, logger)
		return
	}
	defer c.Close()

	// Per-connection context: cancelling it aborts an in-flight resolve
	// (QueryTimeout is the other backstop).
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// A blocking be.Receive() / ReceiveStartupMessage() is a ctx-unaware socket
	// read, so cancelling connCtx alone cannot unblock an IDLE connection parked
	// between queries (or pre-auth). Close the socket when connCtx ends — on
	// SIGTERM shutdown or the deferred cancel on return — so the parked read
	// errors out and this goroutine exits; otherwise Serve's wg.Wait() would hang
	// graceful shutdown on every idle psql session. Mirrors the MySQL shim's
	// shim.WatchConn (internal/shim/watchconn.go). stopClose prevents the AfterFunc from
	// racing the normal-return path (it is a no-op once the func has run).
	stopClose := context.AfterFunc(connCtx, func() { _ = c.Close() })
	defer stopClose()

	// A client has loginTimeout to log in, as on the port with a login of
	// its own: one that connects and stops would otherwise hold a
	// connection for as long as it liked.
	loginTimer := time.AfterFunc(loginTimeout, func() { _ = c.Close() })
	defer loginTimer.Stop()

	conn, encrypted, err := negotiate(connCtx, c, pt)
	if err != nil {
		logNegotiateFailure(logger, err, c.RemoteAddr())
		return
	}
	defer conn.Close()
	be := pgproto3.NewBackend(conn, conn)

	startup, err := receiveStartup(be)
	if err != nil {
		logger.Debug("pgshim startup failed", "err", err, "remote", c.RemoteAddr())
		return
	}
	if startup == nil {
		return // CancelRequest / clean close
	}

	user := startup.Parameters["user"]
	database := startup.Parameters["database"]

	password, err := authenticate(be, cfg.Auth, user, encrypted, pt.bindingData())
	if err != nil {
		logger.Info("pgshim auth denied", "user", user, "remote", c.RemoteAddr(), "err", err)
		return
	}

	if !loginTimer.Stop() {
		logger.Info("pgshim login took too long", "user", user, "remote", c.RemoteAddr())
		return
	}
	sendSASLFinal(be, password)
	sendAuthOK(be)
	if err := be.Flush(); err != nil {
		return
	}

	h := shim.NewHandlerWithConfig(cfg.IndexDB, cfg.ShimConfig, logger)
	h.BindConnContext(connCtx)
	// This front-end authenticates a real per-tenant credential (shim.yaml),
	// so — exactly like the standalone MySQL shim (internal/cli/shim.go) —
	// the audit identity for every time-travel query on this connection is
	// that tenant. Post-auth: user is only trustworthy here.
	h.BindActor(user)
	// Bind the tenant's allowed_schemas allowlist (#1261) BEFORE any schema is
	// seeded, mirroring the MySQL front-end (internal/cli/shim.go handleConn).
	// The enforcement point itself is session.resolve, on the parser-resolved
	// target schema — the only place that sees every way a client can name one.
	h.BindAllowedSchemas(cfg.AllowedSchemas[user])
	// currentDB = the connect database param (the bintrail schema). Empty is
	// allowed: queries must then schema-qualify the table (<schema>.<table>).
	//
	// A connect database outside the tenant's allowlist is deliberately NOT a
	// startup refusal: denial belongs on the query, as a proper wire error the
	// client can read, rather than a connection that dies during handshake. So
	// the param still seeds session.currentDB and the per-query gate answers
	// 42501 on the first real query. UseDB's error is discarded because h.db is
	// unused on this front-end — session.currentDB is what Parse sees — and it
	// now returns one whenever the allowlist excludes the connect database.
	_ = h.UseDB(database)

	sess := &session{be: be, h: h, currentDB: database, logger: logger, budget: cfg.ResultBudget}

	commandLoop(be, sess)
}

// commandLoop answers the connection's commands until the client leaves or a
// write fails.
func commandLoop(be *pgproto3.Backend, sess *session) {
	// The connection is over when this returns: nothing it counted in the
	// port's result budget outlives it, a portal's kept rows included.
	defer sess.releaseResult()
	defer sess.dropPortals()
	for {
		msg, rerr := be.Receive()
		if rerr != nil {
			return // client disconnect / read error
		}
		if sess.ext.skip {
			// PostgreSQL's extended-query error rule: after an error every
			// message up to the client's Sync is discarded, a simple query
			// too, and the Sync releases the client with ReadyForQuery.
			switch msg.(type) {
			case *pgproto3.Sync:
				if err := sess.handleSync(); err != nil {
					return
				}
			case *pgproto3.Terminate:
				return
			}
			continue
		}
		// Replies to Parse, Bind, Describe, Execute and Close are buffered,
		// as PostgreSQL buffers them: they leave at the client's Sync or
		// Flush.
		switch m := msg.(type) {
		case *pgproto3.Query:
			if err := sess.handleSimpleQuery(m.String); err != nil {
				return
			}
		case *pgproto3.Parse:
			sess.handleParse(m)
		case *pgproto3.Bind:
			sess.handleBind(m)
		case *pgproto3.Describe:
			sess.handleDescribe(m)
		case *pgproto3.Execute:
			sess.handleExecute(m)
		case *pgproto3.Close:
			sess.handleClose(m)
		case *pgproto3.Sync:
			if err := sess.handleSync(); err != nil {
				return
			}
		case *pgproto3.Flush:
			if err := be.Flush(); err != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		case *pgproto3.FunctionCall:
			// The fast-path function call: a client waits for its
			// ReadyForQuery, as for a simple query.
			sess.fail(&pgErr{"0A000", "function calls are not supported on this port; call the function in a statement"})
			if err := sess.readyFlush(); err != nil {
				return
			}
		default:
			// A message this port does not take (COPY data outside a COPY,
			// for one): one error, then the client's Sync resynchronizes.
			sess.extFail(&pgErr{"08P01", fmt.Sprintf("unexpected message %T", m)})
			if err := be.Flush(); err != nil {
				return
			}
		}
	}
}

// authenticate runs the SCRAM exchange and checks the proof against the
// tenant's password. On failure it sends a FATAL error and returns an error;
// the caller closes the connection. An unknown user and a wrong password are
// refused with a byte-identical 28P01 message, and in the same time: the proof
// is checked against as many passwords either way (Password.Matches), so
// neither the answer nor its timing says which usernames exist.
func authenticate(be *pgproto3.Backend, auth shim.TenantAuth, user string, encrypted bool, endPoint []byte) (*Password, error) {
	password, err := scramExchange(be, encrypted, endPoint)
	if err != nil {
		return nil, err
	}
	// Every tenant's password, before the one of this username: what is
	// derived must not depend on the username.
	WarmPasswords(auth.Passwords())
	var candidates []string
	if expected, found, cerr := auth.LookupPassword(user); found && cerr == nil {
		candidates = []string{expected}
	}
	if !password.Matches(candidates) {
		sendFatal(be, "28P01", fmt.Sprintf("password authentication failed for user %q", user))
		return nil, fmt.Errorf("auth failed for user %q", user)
	}
	return password, nil
}

// sendSASLFinal sends the server's last SCRAM message, once the password
// matched: what shows the client the port knows it too.
func sendSASLFinal(be *pgproto3.Backend, password *Password) {
	be.Send(&pgproto3.AuthenticationSASLFinal{Data: password.serverFinal()})
}

// sendAuthOK finishes the handshake: AuthenticationOk, the ParameterStatus set a
// PostgreSQL client reads at startup, then ReadyForQuery(idle). BackendKeyData is
// intentionally omitted — it only backs query cancellation, which we do not
// implement.
func sendAuthOK(be *pgproto3.Backend) {
	// From here on a message is a statement: a larger ceiling, still one.
	be.SetMaxBodyLen(maxStatementMessage)
	be.Send(&pgproto3.AuthenticationOk{})
	for _, ps := range startupParameters {
		be.Send(&pgproto3.ParameterStatus{Name: ps[0], Value: ps[1]})
	}
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

// The most bytes one client message may announce: a password before the
// login, a statement after it. A longer one ends the connection.
const (
	maxPasswordMessage  = 64 << 10
	maxStatementMessage = 16 << 20
)

// startupParameters are the ParameterStatus values psql/libpq expect after
// AuthenticationOk. server_version carries a leading numeric psql parses.
var startupParameters = [][2]string{
	{"server_version", "14.0 (bintrail time-travel)"},
	{"server_encoding", "UTF8"},
	{"client_encoding", "UTF8"},
	{"DateStyle", "ISO, MDY"},
	{"standard_conforming_strings", "on"},
	{"integer_datetimes", "on"},
	{"TimeZone", "UTC"},
}

func sendFatal(be *pgproto3.Backend, code, msg string) {
	be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: code, Message: msg})
	_ = be.Flush()
}
