package consoleapp

import (
	"database/sql"
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// connectSource opens a source for a console check with the TLS capture uses
// for it (ssl: a ServerEntry.SourceSSL, or watch's --ssl-* flags), so a check
// never connects in a way capture would not: a server that only accepts
// encrypted connections refused the cleartext checks while capture, over TLS,
// worked.
//
// Under "preferred", a source with no TLS at all is read in cleartext, as
// capture reads it, and that is logged at Debug only: these checks repeat (on
// every page load, every status poll), and capture of the same source already
// warned loudly that it connects WITHOUT encryption when it started.
//
// A zero ssl (a request built without one) is read as the entry default,
// console.DefaultSourceSSLMode, exactly as an entry with no ssl_mode is.
func connectSource(dsn string, ssl config.SSL) (*sql.DB, error) {
	return config.ConnectSSL(dsn, sourceSSLOrDefault(ssl), func(err error) {
		slog.Debug("console: the source offers no TLS; this check reads it WITHOUT encryption, as capture does",
			"host", config.DSNHost(dsn), "error", err)
	})
}

// sourceSSLOrDefault fills an empty mode with console.DefaultSourceSSLMode.
func sourceSSLOrDefault(ssl config.SSL) config.SSL {
	if ssl.Mode == "" {
		ssl.Mode = console.DefaultSourceSSLMode
	}
	return ssl
}
