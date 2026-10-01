package ext

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// SourceTLS is the TLS the core's own capture uses to reach a MySQL or
// MariaDB source: the registry entry's ssl_mode, ssl_ca, ssl_cert and
// ssl_key, or the --ssl-* flags (BINTRAIL_SSL_*) of `watch` and `stream` for
// the source they were started with, or BINTRAIL_SSL_* on the standalone MCP
// server. The agent has no TLS settings of its own and always hands the zero
// value: to require TLS there, put tls= in the agent's source DSN.
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

// cleartextWarned holds the source addresses (sourceKey) whose cleartext
// fallback has already been warned about in this process.
var cleartextWarned sync.Map

// ErrSourceTLSSettings matches (errors.Is) every error OpenSource returns
// because the TLS settings themselves are unusable: an unknown mode, an
// unreadable CA file, a certificate without its key. Fix the settings, not
// the server.
var ErrSourceTLSSettings = errors.New("unusable source TLS settings")

// OpenSource opens a MySQL or MariaDB source the way the core's capture
// does: dsn with tls applied (an empty Mode is "preferred"; a tls= in the
// DSN wins). The caller owns the returned handle and must Close it.
//
// The DSN is normalized like every other connection the core opens:
// parseTime=true with the UTC location (DATETIME and TIMESTAMP columns scan
// into time.Time), LOAD DATA LOCAL INFILE forbidden, the client's
// max_allowed_packet sized to the server's, a 10-second connect timeout when
// the DSN sets none, and a bounded ping before it returns.
//
// Under "preferred", a server that offers no TLS at all is read in
// cleartext, as capture does. Once that cleartext connection exists it is
// logged as a warning the first time per source address (host:port, or the
// socket path) in this process, at debug level after that so a job that
// reconnects every cycle does not flood the log; a later connection over
// TLS re-arms the warning. A server that demands TLS is never retried in
// cleartext.
//
// A bad setting returns an error that matches ErrSourceTLSSettings, names
// the setting as the registry spells it (ssl_mode, ssl_ca, ssl_cert and
// ssl_key) and names no command-line flag. A connect failure is returned as
// the driver reported it, so a MySQL error number stays matchable. A
// Postgres DSN is refused: a Postgres source carries its TLS in its own DSN.
func OpenSource(dsn string, tls SourceTLS) (*sql.DB, error) {
	if looksLikePostgresDSN(dsn) {
		return nil, errors.New("OpenSource opens MySQL/MariaDB sources only; a Postgres source carries its TLS in its own DSN (sslmode=)")
	}
	ssl := config.SSL(tls)
	if ssl.Mode == "" {
		ssl.Mode = config.DefaultSourceSSLMode
	}
	key := sourceKey(dsn)
	fellBack := false
	db, err := connectSourceSSL(dsn, ssl, func(err error) {
		// Called only once the cleartext connection has been made.
		fellBack = true
		level := slog.LevelDebug
		if _, seen := cleartextWarned.LoadOrStore(key, struct{}{}); !seen {
			level = slog.LevelWarn
		}
		slog.Log(context.Background(), level,
			"ext: the source offers no TLS; an extension is reading it WITHOUT encryption (credentials and data in cleartext), as capture does under ssl mode preferred",
			"host", config.DSNHost(dsn), "error", err)
	})
	var se *config.TLSSettingsError
	if errors.As(err, &se) {
		return nil, &sourceTLSSettingsError{se: se}
	}
	if err == nil && !fellBack {
		cleartextWarned.Delete(key)
	}
	return db, err
}

// sourceKey names the source a DSN reaches, for the warned set: host:port,
// or the socket path. A DSN that does not parse gets a key of its own (a
// digest, so no password is held), never a key shared with other DSNs.
func sourceKey(dsn string) string {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		sum := sha256.Sum256([]byte(dsn))
		return "unparsed:" + hex.EncodeToString(sum[:8])
	}
	return cfg.Net + ":" + cfg.Addr
}

// looksLikePostgresDSN reports a Postgres URL (postgres://, postgresql://)
// or keyword form (host=... with no "/", which every MySQL DSN carries).
func looksLikePostgresDSN(dsn string) bool {
	d := strings.ToLower(strings.TrimSpace(dsn))
	if strings.HasPrefix(d, "postgres://") || strings.HasPrefix(d, "postgresql://") {
		return true
	}
	return !strings.Contains(d, "/") && (strings.HasPrefix(d, "host=") || strings.Contains(d, " host=") ||
		strings.HasPrefix(d, "dbname=") || strings.Contains(d, " dbname="))
}

// sourceTLSSettingsError rewords a settings error without its flag text. It
// matches ErrSourceTLSSettings, and unwraps to the core's own settings error.
type sourceTLSSettingsError struct{ se *config.TLSSettingsError }

func (e *sourceTLSSettingsError) Error() string {
	// Registry spelling (ssl_mode), not the flag spelling (ssl-mode).
	return fmt.Sprintf("source TLS setting %s: %s", strings.ReplaceAll(e.se.Setting, "-", "_"), e.se.Problem)
}
func (e *sourceTLSSettingsError) Unwrap() error        { return e.se }
func (e *sourceTLSSettingsError) Is(target error) bool { return target == ErrSourceTLSSettings }
