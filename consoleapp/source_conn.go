package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
)

// connectSource opens a source for a console read that REPEATS on its own
// (the capture-status read, the backup schedule's window probe), with the TLS
// capture uses for it (ssl: a ServerEntry.SourceSSL, or watch's --ssl-*
// flags), so a check never connects in a way capture would not: a server that
// only accepts encrypted connections refused the cleartext checks while
// capture, over TLS, worked.
//
// Under "preferred", a source with no TLS at all is read in cleartext, as
// capture reads it, and that is logged at Debug only: these reads run on a
// timer, and capture of the same source already warned loudly that it
// connects WITHOUT encryption when it started.
//
// A zero ssl (a request built without one) is read as the entry default,
// console.DefaultSourceSSLMode, exactly as an entry with no ssl_mode is.
func connectSource(dsn string, ssl config.SSL) (*sql.DB, error) {
	return connectSourceAt(dsn, ssl, slog.LevelDebug)
}

// connectSourceAsked is connectSource for a read a person asked for (a live
// verify, a schema snapshot, the replica check at Start): it runs once per
// ask, so a cleartext fallback is a Warn, like the startup checks'.
func connectSourceAsked(dsn string, ssl config.SSL) (*sql.DB, error) {
	return connectSourceAt(dsn, ssl, slog.LevelWarn)
}

func connectSourceAt(dsn string, ssl config.SSL, level slog.Level) (*sql.DB, error) {
	db, err := config.ConnectSSL(dsn, sourceSSLOrDefault(ssl), func(err error) {
		slog.Log(context.Background(), level,
			"console: the source offers no TLS; this check reads it WITHOUT encryption, as capture does",
			"host", config.DSNHost(dsn), "error", err)
	})
	// A setting that cannot be used is worded with the entry's field names:
	// the console has no --ssl-* flags to fix.
	if detail, fix, ok := doctor.TLSSettingsText(err); ok {
		return nil, errors.New(detail + " " + fix)
	}
	// A refusal over TLS (a server that demands encryption the connection
	// did not use, or a mode that requires TLS against a server with none)
	// is worded for a person, as the startup checks word it (#1996). The
	// driver's error stays in the chain for errors.Is/As, out of the text.
	if detail, fix, ok := doctor.SourceTLSRefusalText(err, dsn, sourceSSLOrDefault(ssl)); ok {
		return nil, &sourceTLSRefusal{msg: detail + " " + fix, err: err}
	}
	return db, err
}

// sourceTLSRefusal is a TLS refusal worded for a person, wrapping the
// driver's error.
type sourceTLSRefusal struct {
	msg string
	err error
}

func (e *sourceTLSRefusal) Error() string { return e.msg }
func (e *sourceTLSRefusal) Unwrap() error { return e.err }

// sourceEncrypted reports whether a source connection made with ssl is
// encrypted: under preferred that is the server's answer (TLS when it offers
// it, cleartext when it does not), and the full read's mydumper must make the
// same choice (#1996). It reads the session's Ssl_cipher, which MySQL and
// MariaDB both report, empty on an unencrypted connection. A seam, so the
// tests need no server.
var sourceEncrypted = func(ctx context.Context, dsn string, ssl config.SSL) (bool, error) {
	db, err := connectSourceAsked(dsn, ssl)
	if err != nil {
		return false, err
	}
	defer db.Close()
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var name, cipher string
	if err := db.QueryRowContext(qctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&name, &cipher); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, errors.New("the source does not report whether a connection is encrypted (no Ssl_cipher status variable)")
		}
		return false, err
	}
	return cipher != "", nil
}

// sourceSSLOrDefault fills an empty mode with console.DefaultSourceSSLMode.
func sourceSSLOrDefault(ssl config.SSL) config.SSL {
	if ssl.Mode == "" {
		ssl.Mode = console.DefaultSourceSSLMode
	}
	return ssl
}
