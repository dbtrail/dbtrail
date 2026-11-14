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
//   - simple query protocol only (psql uses it natively; a pgx client sets
//     QueryExecModeSimpleProtocol). The extended query protocol is declined with
//     a clear error and a resync, so a default-mode client is not left hanging.
//
// Addressing: the connect `database` parameter is the bintrail SCHEMA (the
// PostgreSQL schema name, e.g. `public`); flashback-vs-snapshot semantics are
// selected by the `_flashback.` / `_snapshot.` table prefix in the query, exactly
// like the MySQL shim. (This differs from the issue's tentative
// `dbname=_flashback` note, which the engine's schema-from-connection model does
// not support without a parser change; the query-prefix model needs none and is
// strictly more capable — one connection reaches every virtual schema.)
//
// Auth mirrors the MySQL shim: a cleartext credential validated per tenant
// against shim.yaml. This is loopback-default; front a TLS terminator (or run on
// loopback behind a local proxy) for a non-loopback bind — the same posture as
// the MySQL shim behind ProxySQL. Package placement is deliberate: pgproto3 is
// github.com/jackc/pgx/v5/pgproto3, which the cliapp pgfree guard bans from the
// core bintrail binary, so this front-end is linked ONLY by cmd/bintrail-pg.
package pgshim

import (
	"context"
	"crypto/subtle"
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
	// Auth validates the per-tenant cleartext password (loaded from shim.yaml).
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
				refuseFull(conn, cfg.MaxConns, refusals)
				continue
			}
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			if sem != nil {
				defer func() { <-sem }()
			}
			handleConn(ctx, c, cfg, logger)
		}(conn)
	}
}

// refuseFull tells a client that the port has all the connections it takes,
// in an error it can read (53300, too_many_connections), and closes. The
// error can only be sent as the answer to the client's start-up message, so
// that is read first, for at most a few seconds, on a goroutine of its own:
// the accept loop does not wait on a client that sends nothing. When too many
// refusals are already being said the connection is closed without one.
func refuseFull(c net.Conn, max int, refusals chan struct{}) {
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
		be := pgproto3.NewBackend(c, c)
		if startup, err := negotiateStartup(c, be); err != nil || startup == nil {
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

// handleConn runs one connection: startup negotiation (incl. the SSLRequest
// decline), cleartext auth, then a simple-query command loop over its own
// shim.Handler.
func handleConn(ctx context.Context, c net.Conn, cfg Config, logger *slog.Logger) {
	if cfg.Login != nil {
		handleLoginConn(ctx, c, cfg, logger)
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

	be := pgproto3.NewBackend(c, c)

	startup, err := negotiateStartup(c, be)
	if err != nil {
		logger.Debug("pgshim startup failed", "err", err, "remote", c.RemoteAddr())
		return
	}
	if startup == nil {
		return // CancelRequest / clean close
	}

	user := startup.Parameters["user"]
	database := startup.Parameters["database"]

	if err := authenticate(be, cfg.Auth, user); err != nil {
		logger.Info("pgshim auth denied", "user", user, "remote", c.RemoteAddr())
		return
	}

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

	sess := &session{be: be, h: h, currentDB: database, logger: logger}

	commandLoop(be, sess)
}

// commandLoop answers the connection's commands until the client leaves or a
// write fails.
func commandLoop(be *pgproto3.Backend, sess *session) {
	// skipUntilSync implements the PostgreSQL extended-query error rule: once we
	// reject an extended-protocol message we must discard everything until the
	// client's Sync, then release it with ReadyForQuery.
	skipUntilSync := false
	for {
		msg, rerr := be.Receive()
		if rerr != nil {
			return // client disconnect / read error
		}
		if skipUntilSync {
			switch msg.(type) {
			case *pgproto3.Sync:
				skipUntilSync = false
				if err := sess.readyFlush(); err != nil {
					return
				}
			case *pgproto3.Terminate:
				return
			}
			continue
		}
		switch m := msg.(type) {
		case *pgproto3.Query:
			if err := sess.handleSimpleQuery(m.String); err != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		case *pgproto3.Sync:
			// A stray Sync (some clients send one after startup): acknowledge.
			if err := sess.readyFlush(); err != nil {
				return
			}
		default:
			// Extended query protocol (Parse/Bind/Describe/Execute/Close/Flush)
			// or an unrecognised message. Not supported in the first cut; emit one
			// error and skip to the client's Sync so it is not left hanging.
			sess.errorResponse("0A000", "this endpoint supports only the simple query protocol; "+
				"psql uses it natively — with pgx set QueryExecModeSimpleProtocol")
			if err := be.Flush(); err != nil {
				return
			}
			skipUntilSync = true
		}
	}
}

// negotiateStartup returns the client's StartupMessage, declining an SSLRequest
// or GSSEncRequest with a single 'N' byte (we terminate no TLS here) and then
// re-reading. A CancelRequest returns (nil, nil): we run no query registry, so
// there is nothing to cancel — the caller closes.
func negotiateStartup(c net.Conn, be *pgproto3.Backend) (*pgproto3.StartupMessage, error) {
	for {
		msg, err := be.ReceiveStartupMessage()
		if err != nil {
			return nil, err
		}
		switch m := msg.(type) {
		case *pgproto3.StartupMessage:
			return m, nil
		case *pgproto3.SSLRequest:
			if _, err := c.Write([]byte{'N'}); err != nil {
				return nil, err
			}
		case *pgproto3.GSSEncRequest:
			if _, err := c.Write([]byte{'N'}); err != nil {
				return nil, err
			}
		case *pgproto3.CancelRequest:
			return nil, nil
		default:
			return nil, fmt.Errorf("unexpected startup message %T", m)
		}
	}
}

// authenticate performs the cleartext-password exchange and validates it against
// the tenant store. On failure it sends a FATAL 28P01 and returns an error; the
// caller closes the connection. An unknown user and a wrong password are rejected
// with a byte-identical 28P01 message, so the error content never reveals which
// usernames exist. The password check uses subtle.ConstantTimeCompare to avoid a
// per-byte timing oracle on a known user's password — the MySQL shim's
// ER_ACCESS_DENIED posture; it does not attempt to equalise the cost of the
// tenant-map lookup itself (the endpoint is loopback-default / TLS-fronted).
func authenticate(be *pgproto3.Backend, auth shim.TenantAuth, user string) error {
	password, err := readPassword(be)
	if err != nil {
		return err
	}
	expected, found, cerr := auth.LookupPassword(user)
	match := found && cerr == nil &&
		subtle.ConstantTimeCompare([]byte(password), []byte(expected)) == 1
	if !match {
		sendFatal(be, "28P01", fmt.Sprintf("password authentication failed for user %q", user))
		return fmt.Errorf("auth failed for user %q", user)
	}
	return nil
}

// readPassword asks the client for its password in clear text and returns
// what it sent. A client that answers with anything else is told so (08P01)
// and the error says the connection is over.
func readPassword(be *pgproto3.Backend) (string, error) {
	// Before anyone is authenticated a message is a password and nothing
	// else. The protocol library sizes its buffer from the length a message
	// announces and has no ceiling of its own, so without one a client that
	// holds no credential could announce two gigabytes and have them set
	// aside in this process.
	be.SetMaxBodyLen(maxPasswordMessage)
	be.Send(&pgproto3.AuthenticationCleartextPassword{})
	if err := be.Flush(); err != nil {
		return "", err
	}
	if err := be.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
		return "", err
	}
	msg, err := be.Receive()
	if err != nil {
		return "", err
	}
	pw, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		sendFatal(be, "08P01", fmt.Sprintf("expected a password message, got %T", msg))
		return "", fmt.Errorf("expected PasswordMessage, got %T", msg)
	}
	return pw.Password, nil
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
