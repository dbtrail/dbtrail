package consoleapp

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The router's PostgreSQL-protocol port (#2765): a PostgreSQL client connects
// to it and runs read-only SQL on a server's copy. It is the MySQL port's
// sibling, with the same servers, the same passwords and the same executor
// (console.SQLOnCopy); the wire is internal/pgshim's.
//
// A client picks its server with the database name (dbname=<server id or
// name>); the password is the port's own, the one the MySQL port takes. A
// username that is a server's application login opens that server alone,
// with that login's password or the port's, whatever database it names.
//
// The copy answers every statement: nothing is forwarded to the source, and
// time travel is not served here yet.

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
		return fmt.Errorf("--pg-listen %q is not an address: want host:port, e.g. 127.0.0.1:5433", addr)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("--pg-listen %q: %q is not a port number (1 to 65535)", addr, port)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("--pg-listen %q: the PostgreSQL port listens on this machine only (127.0.0.1, ::1 or localhost); "+
		"it asks for its password in clear text and has no TLS, so an address other machines reach would show that password to the network", addr)
}

// servePGPort serves the PostgreSQL-protocol port on ln until ctx ends.
func servePGPort(ctx context.Context, srv *console.Server, ln net.Listener, cfg flashbackConfig) error {
	cfg = cfg.withDefaults()
	logger := slog.Default()
	// Accept returns when the listener is closed; the connections end with
	// ctx (pgshim closes each one's socket), and Serve waits for them.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	return pgshim.Serve(ctx, ln, pgshim.Config{Login: pgPortLogin(srv, cfg), Logger: logger})
}

// pgPortLogin is the port's login: the port's passwords, then the server.
//
// The password is checked before anything is said about the server, and a
// wrong password reads the same for every username and database, so a client
// that holds no password learns nothing about which servers exist.
func pgPortLogin(srv *console.Server, cfg flashbackConfig) pgshim.LoginFunc {
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
		// names resolved where PostgreSQL resolves them. The copy of a MySQL
		// source keeps its own session and its source database.
		sess, schema := sqlsandbox.Session{}, tgt.DefaultSchema
		if copySQL.PostgresSource() {
			sess, schema = sqlsandbox.Session{PostgresRules: true}, pgPortSchema
		}
		bound.Schema = schema
		bound.Answered = copySQL.NoteAnswered
		timeout := cfg.QueryTimeout
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
