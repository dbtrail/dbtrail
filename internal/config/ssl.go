package config

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// SSL is how a connection to a SOURCE database uses TLS: the ssl-mode
// (disabled, preferred, required, verify-ca, verify-identity) and the optional
// CA, client certificate and key files. Capture (streamrun, from --ssl-mode or
// a console entry's ssl_* fields) and every check the console runs against the
// same source read it from the same place, so a check cannot pass or fail on a
// connection capture would not make.
type SSL struct {
	Mode string
	CA   string
	Cert string
	Key  string
}

// TLSSettingsError is a TLS setting that cannot be used (an unknown mode, a CA
// or client certificate file that cannot be read): a LOCAL problem, found
// before anything dials, so no network advice applies to it. Error() keeps
// the command-line wording (--ssl-mode, --ssl-ca, ...); a surface configured
// some other way (the console's ssl_* entry fields) words it from Setting and
// Problem instead.
type TLSSettingsError struct {
	// Setting is the setting at fault, in flag spelling without the dashes:
	// "ssl-mode", "ssl-ca" or "ssl-cert" (a certificate/key pair).
	Setting string
	// Problem says what is wrong without naming a flag or a file format.
	Problem  string
	Err      error
	flagText string
}

func (e *TLSSettingsError) Error() string { return e.flagText }
func (e *TLSSettingsError) Unwrap() error { return e.Err }

// DefaultSourceSSLMode is the TLS mode a source connection uses when nothing
// sets one: try TLS, and fall back to cleartext only when the server offers
// no TLS at all. Every layer that fills an empty mode reads it here, so the
// console registry and the extension seam cannot drift apart.
const DefaultSourceSSLMode = "preferred"

// ValidSSLMode reports whether mode is one BuildTLSConfig accepts.
func ValidSSLMode(mode string) bool {
	switch mode {
	case "disabled", "preferred", "required", "verify-ca", "verify-identity":
		return true
	}
	return false
}

// ConnectSSL opens and pings dsn honoring ssl, with the semantics capture uses
// for its source and index helper connections (#946/#947):
//
//   - disabled: no TLS (a tls= in the DSN still applies, see applyTLS).
//   - preferred: try TLS (certificate not verified). ONLY when the server
//     genuinely does not support TLS (IsTLSUnsupportedError) is the
//     connection retried in cleartext (and onCleartext called once that
//     retry has connected), never the driver's own
//     silent AllowFallbackToPlaintext. Any other failure (access denied, a
//     refused port, a server that demands TLS) is returned as is.
//   - required, verify-ca, verify-identity: TLS or an error, never a retry.
//
// An empty or unknown Mode is an error, as it is for capture: the caller picks
// the default, so a typo never quietly becomes a weaker mode. A tls= in the
// DSN wins over ssl (applyTLS); with one there is no cleartext retry at all.
//
// onCleartext may be nil. It is called only after the cleartext retry has
// connected (never for a retry that failed), with the error that proved the
// server has no TLS; the caller decides how loudly to say "unencrypted" (a
// long-lived capture warns, a check that runs every few seconds should not
// flood).
func ConnectSSL(dsn string, ssl SSL, onCleartext func(error)) (*sql.DB, error) {
	return connectSSL(dsn, ssl, onCleartext, ConnectWithTLS)
}

// connectSSL is ConnectSSL with the connect step injected, so the retry rule
// is tested without a server that lacks TLS.
func connectSSL(dsn string, ssl SSL, onCleartext func(error), open func(string, *tls.Config) (*sql.DB, error)) (*sql.DB, error) {
	return ConnectSSLWith(dsn, ssl, onCleartext, open)
}

// ConnectSSLWith is ConnectSSL's rule for a connection of any kind: open is
// handed the DSN and the tls.Config the mode asks for (nil for cleartext) and
// makes the connection its own way. It exists so a second client library to
// the same source (the read router's MySQL-protocol connection, which is not
// database/sql) decides TLS by this one rule instead of a copy of it. open
// must let a tls= inside the DSN win over the config it is handed, as
// ConnectWithTLS does (applyTLS); the no-retry rule for such a DSN is
// applied here.
func ConnectSSLWith[T any](dsn string, ssl SSL, onCleartext func(error), open func(string, *tls.Config) (T, error)) (T, error) {
	var none T
	tlsCfg, err := BuildTLSConfig(ssl.Mode, ssl.CA, ssl.Cert, ssl.Key, DSNHost(dsn))
	if err != nil {
		return none, err
	}
	db, err := open(dsn, tlsCfg)
	if err == nil {
		return db, nil
	}
	// A tls= in the DSN wins over ssl on every attempt (applyTLS), so a
	// "retry in cleartext" would re-run the DSN's own setting: no retry, and
	// no claim that the connection went out unencrypted.
	if ssl.Mode != "preferred" || !IsTLSUnsupportedError(err) || DSNHasExplicitTLS(dsn) {
		return none, err
	}
	noTLS := err
	db, err = open(dsn, nil)
	if err != nil {
		return none, fmt.Errorf("cleartext retry after the server offered no TLS: %w", err)
	}
	// Reported only once the cleartext connection exists: a failed retry read
	// nothing, and its error already says the retry was in cleartext.
	if onCleartext != nil {
		onCleartext(noTLS)
	}
	return db, nil
}

// BuildTLSConfig returns a *tls.Config for the given ssl-mode, or nil for
// "disabled". serverName is the target host (used only for verify-identity).
func BuildTLSConfig(mode, ca, cert, key, serverName string) (*tls.Config, error) {
	if mode == "disabled" {
		return nil, nil
	}
	if !ValidSSLMode(mode) {
		return nil, &TLSSettingsError{Setting: "ssl-mode",
			Problem:  fmt.Sprintf("%q is not a TLS mode; use disabled, preferred, required, verify-ca or verify-identity", mode),
			flagText: fmt.Sprintf("invalid --ssl-mode %q: must be one of disabled, preferred, required, verify-ca, verify-identity", mode)}
	}
	if (cert == "") != (key == "") {
		return nil, &TLSSettingsError{Setting: "ssl-cert",
			Problem:  "a client certificate and its key must be set together (ssl_cert and ssl_key)",
			flagText: "--ssl-cert and --ssl-key must both be specified together"}
	}

	cfg := &tls.Config{}

	// Load CA pool (optional — system CAs used when empty).
	var caPool *x509.CertPool
	if ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, &TLSSettingsError{Setting: "ssl-ca", Err: err,
				Problem:  fmt.Sprintf("the CA file %q could not be read: %v", ca, err),
				flagText: fmt.Sprintf("read --ssl-ca %q: %v", ca, err)}
		}
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(pem) {
			return nil, &TLSSettingsError{Setting: "ssl-ca",
				Problem:  fmt.Sprintf("the CA file %q holds no valid certificate", ca),
				flagText: fmt.Sprintf("--ssl-ca %q: no valid certificates found", ca)}
		}
		cfg.RootCAs = caPool
	}

	// Load client certificate for mutual TLS.
	if cert != "" {
		kp, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, &TLSSettingsError{Setting: "ssl-cert", Err: err,
				Problem:  fmt.Sprintf("the client certificate or its key could not be loaded: %v", err),
				flagText: fmt.Sprintf("load --ssl-cert/--ssl-key: %v", err)}
		}
		cfg.Certificates = []tls.Certificate{kp}
	}

	switch mode {
	case "preferred", "required":
		// Encrypt the connection but skip server certificate verification.
		cfg.InsecureSkipVerify = true //nolint:gosec // intentional for these modes
	case "verify-ca":
		// Verify the certificate chain against the CA pool but not the hostname.
		cfg.InsecureSkipVerify = true //nolint:gosec // hostname check done via VerifyConnection
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("server presented no certificate")
			}
			opts := x509.VerifyOptions{
				Roots:         caPool, // nil → system CAs
				Intermediates: x509.NewCertPool(),
			}
			for _, c := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	case "verify-identity":
		// Full TLS verification: certificate chain + hostname.
		cfg.ServerName = serverName
	}

	return cfg, nil
}

// IsTLSUnsupportedError reports whether err means the server does not support
// TLS at all — the ONLY condition under which --ssl-mode=preferred may retry in
// cleartext. The go-sql-driver helper connections return the sentinel
// drivermysql.ErrNoTLS; the go-mysql binlog syncer returns a fixed message when
// the server omits the CLIENT_SSL capability (client/auth.go); a mid-handshake
// plaintext reply surfaces as tls.RecordHeaderError. Every other failure (auth
// denied, unreachable host, bad binlog position) must NOT trigger a downgrade,
// or credentials and data would be resent unencrypted on an unrelated error
// (#947).
func IsTLSUnsupportedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, mysql.ErrNoTLS) {
		return true
	}
	var rhe tls.RecordHeaderError
	if errors.As(err, &rhe) {
		return true
	}
	return strings.Contains(err.Error(), "the MySQL Server does not support TLS")
}
