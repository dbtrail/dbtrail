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

// ConnectSSL opens and pings dsn honoring ssl, with the semantics capture uses
// for its source and index helper connections (#946/#947):
//
//   - disabled: no TLS (a tls= in the DSN still applies, see applyTLS).
//   - preferred: try TLS (certificate not verified). ONLY when the server
//     genuinely does not support TLS (IsTLSUnsupportedError) is onCleartext
//     called and the connection retried in cleartext, never the driver's own
//     silent AllowFallbackToPlaintext. Any other failure (access denied, a
//     refused port, a server that demands TLS) is returned as is.
//   - required, verify-ca, verify-identity: TLS or an error, never a retry.
//
// An empty or unknown Mode is an error, as it is for capture: the caller picks
// the default, so a typo never quietly becomes a weaker mode. A tls= in the
// DSN wins over ssl (applyTLS), and survives the cleartext retry, which then
// fails closed instead of downgrading.
//
// onCleartext may be nil. It gets the error that proved the server has no
// TLS; the caller decides how loudly to say "unencrypted" (a long-lived
// capture warns, a check that runs every few seconds should not flood).
func ConnectSSL(dsn string, ssl SSL, onCleartext func(error)) (*sql.DB, error) {
	return connectSSL(dsn, ssl, onCleartext, ConnectWithTLS)
}

// connectSSL is ConnectSSL with the connect step injected, so the retry rule
// is tested without a server that lacks TLS.
func connectSSL(dsn string, ssl SSL, onCleartext func(error), open func(string, *tls.Config) (*sql.DB, error)) (*sql.DB, error) {
	tlsCfg, err := BuildTLSConfig(ssl.Mode, ssl.CA, ssl.Cert, ssl.Key, DSNHost(dsn))
	if err != nil {
		return nil, err
	}
	db, err := open(dsn, tlsCfg)
	if err == nil {
		return db, nil
	}
	if ssl.Mode != "preferred" || !IsTLSUnsupportedError(err) {
		return nil, err
	}
	if onCleartext != nil {
		onCleartext(err)
	}
	db, err = open(dsn, nil)
	if err != nil {
		return nil, fmt.Errorf("cleartext retry after the server offered no TLS: %w", err)
	}
	return db, nil
}

// BuildTLSConfig returns a *tls.Config for the given ssl-mode, or nil for
// "disabled". serverName is the target host (used only for verify-identity).
func BuildTLSConfig(mode, ca, cert, key, serverName string) (*tls.Config, error) {
	if mode == "disabled" {
		return nil, nil
	}
	switch mode {
	case "preferred", "required", "verify-ca", "verify-identity":
	default:
		return nil, fmt.Errorf("invalid --ssl-mode %q: must be one of disabled, preferred, required, verify-ca, verify-identity", mode)
	}
	if (cert == "") != (key == "") {
		return nil, fmt.Errorf("--ssl-cert and --ssl-key must both be specified together")
	}

	cfg := &tls.Config{}

	// Load CA pool (optional — system CAs used when empty).
	var caPool *x509.CertPool
	if ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("read --ssl-ca %q: %w", ca, err)
		}
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--ssl-ca %q: no valid certificates found", ca)
		}
		cfg.RootCAs = caPool
	}

	// Load client certificate for mutual TLS.
	if cert != "" {
		kp, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("load --ssl-cert/--ssl-key: %w", err)
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
