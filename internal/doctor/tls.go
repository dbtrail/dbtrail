package doctor

import (
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// errSecureTransportRequired is the server refusing a connection that is not
// encrypted (ER_SECURE_TRANSPORT_REQUIRED, MySQL and MariaDB alike): the
// server runs with require_secure_transport=ON, the engine default on Amazon
// RDS for MariaDB 11.8 and RDS for MySQL 8.4.
const errSecureTransportRequired = 3159

// refusesUnencrypted reports whether err is the server refusing an
// unencrypted connection. Matched on the error number, never on its text.
func refusesUnencrypted(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == errSecureTransportRequired
}

// unencryptedRefusal words a 3159 for a person: what happened, and the one
// setting that decided it. That setting depends on how the connection was
// made, and naming the wrong one sends them to fix a thing that is fine:
//
//   - the DSN's own tls= wins over any mode (config.applyTLS): tls=false is
//     named first, any other tls= means the server offered no TLS;
//   - ssl == nil: no TLS mode at all (bintrail doctor), so the DSN is the
//     place to ask for TLS;
//   - mode disabled: DBTrail was told not to encrypt, on purpose;
//   - any other mode reached the server without TLS only because the server
//     offered none (preferred's retry), so the server's own TLS is the fix.
func unencryptedRefusal(dsn string, ssl *config.SSL) (detail, remediation string) {
	const lead = "This server only accepts encrypted (TLS) connections"
	const code = " (server error 3159)."
	switch {
	case dsnTLSOff(dsn):
		return lead + ", and the source DSN's own tls=false turned encryption off" + code,
			"Remove tls=false from the source DSN, or change it to tls=preferred."
	case config.DSNHasExplicitTLS(dsn), ssl != nil && ssl.Mode != "disabled":
		// Any other tls= in the DSN, or a mode that asks for TLS: the
		// connection went out unencrypted only because the server offered
		// no TLS (tls=preferred falls back on its own, as does our retry).
		return lead + ", but it did not offer TLS when DBTrail asked for it" + code,
			"Turn TLS on in the server's configuration (give it a certificate and key), then try again."
	case ssl == nil:
		return lead + ", and this check connected without encryption" + code,
			"Add tls=preferred to the source DSN to connect with encryption (the server's certificate is not checked)."
	default: // ssl.Mode == "disabled"
		return lead + ", and DBTrail is set to connect to it without encryption: its TLS mode is disabled" + code,
			"Set this server's TLS mode to preferred or required. For a server added in the web console, " +
				"that is the ssl_mode line of its entry in console-servers.yaml (delete the line to use preferred). " +
				"For bintrail-console watch, it is --ssl-mode."
	}
}

// dsnTLSOff reports whether the DSN itself turns TLS off (tls=false).
func dsnTLSOff(dsn string) bool {
	cfg, err := mysql.ParseDSN(dsn)
	return err == nil && cfg.TLS == nil && strings.EqualFold(cfg.TLSConfig, "false")
}
