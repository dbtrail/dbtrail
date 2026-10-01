package ext

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/dbtrail/dbtrail/internal/config"
)

// SourceTLS is the TLS the core's own capture uses to reach a MySQL or
// MariaDB source: the registry entry's ssl_mode, ssl_ca, ssl_cert and
// ssl_key, or the daemon's --ssl-* flags for the source it was started with.
// The seams that hand a source to an extension (SourceJobInfo,
// ConsoleQueryContext, mcpext.ToolContext) carry it next to the source DSN,
// so an extension can open the source exactly the way capture does.
//
// The zero value means "preferred", the same default as an entry with no
// ssl_mode: try TLS, and fall back to cleartext only when the server offers
// none. A tls= parameter in the DSN itself always wins over these settings.
//
// Mode is one of disabled, preferred, required, verify-ca, verify-identity
// (exact spelling). CA, Cert and Key are file paths on the host the core runs
// on; Cert and Key go together. The settings apply to MySQL-family sources
// only: a Postgres source carries its TLS in its own DSN (sslmode=).
type SourceTLS struct {
	Mode string
	CA   string
	Cert string
	Key  string
}

// connectSourceSSL is config.ConnectSSL, swappable in tests.
var connectSourceSSL = config.ConnectSSL

// cleartextWarned holds the source hosts whose cleartext fallback has already
// been warned about in this process.
var cleartextWarned sync.Map

// OpenSource opens a MySQL or MariaDB source the way the core's capture
// does: dsn with tls applied (an empty Mode is "preferred"; a tls= in the
// DSN wins). The caller owns the returned handle and must Close it.
//
// Under "preferred", a server that offers no TLS at all is read in
// cleartext, as capture does; that fallback is logged as a warning the first
// time per source host in this process (at debug level after that, so a job
// that reconnects every cycle does not flood the log). A server that demands
// TLS is never retried in cleartext. A bad setting
// (unknown mode, unreadable CA file, a certificate without its key) returns
// an error that names the setting without naming any command-line flag and
// unwraps to the underlying settings error; a connect failure is returned as
// the driver reported it.
func OpenSource(dsn string, tls SourceTLS) (*sql.DB, error) {
	ssl := config.SSL(tls)
	if ssl.Mode == "" {
		ssl.Mode = config.DefaultSourceSSLMode
	}
	db, err := connectSourceSSL(dsn, ssl, func(err error) {
		host := config.DSNHost(dsn)
		level := slog.LevelDebug
		if _, seen := cleartextWarned.LoadOrStore(host, struct{}{}); !seen {
			level = slog.LevelWarn
		}
		slog.Log(context.Background(), level,
			"ext: the source offers no TLS; an extension is reading it WITHOUT encryption (credentials and data in cleartext), as capture does under ssl mode preferred",
			"host", host, "error", err)
	})
	var se *config.TLSSettingsError
	if errors.As(err, &se) {
		return nil, &sourceTLSSettingsError{se: se}
	}
	return db, err
}

// sourceTLSSettingsError rewords a settings error without its flag text,
// keeping the original reachable through errors.As.
type sourceTLSSettingsError struct{ se *config.TLSSettingsError }

func (e *sourceTLSSettingsError) Error() string {
	// Registry spelling (ssl_mode), not the flag spelling (ssl-mode).
	return fmt.Sprintf("source TLS setting %s: %s", strings.ReplaceAll(e.se.Setting, "-", "_"), e.se.Problem)
}
func (e *sourceTLSSettingsError) Unwrap() error { return e.se }
