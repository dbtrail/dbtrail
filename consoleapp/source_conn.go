package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

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
	return db, err
}

// sourceSSLOrDefault fills an empty mode with console.DefaultSourceSSLMode.
func sourceSSLOrDefault(ssl config.SSL) config.SSL {
	if ssl.Mode == "" {
		ssl.Mode = console.DefaultSourceSSLMode
	}
	return ssl
}
