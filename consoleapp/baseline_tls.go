package consoleapp

import (
	"context"
	"crypto/sha1" //nolint:gosec // see tlsFingerprint
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// dumpTLS is how one full read's mydumper connects to the source (#1996),
// decided from the same settings capture and the pre-checks connect with.
type dumpTLS struct {
	encrypt bool
	// verify is "" (no certificate check), "ca" (the chain, against ca) or
	// "identity" (the chain and the host name).
	verify string
	// ca is the CA file mydumper checks against: ssl_ca, or the system's CA
	// bundle when ssl_ca is empty. goCA is what the Go side checks with
	// (ssl_ca, "" = the system's trusted CAs), for the mandatory-TLS check.
	ca, goCA  string
	cert, key string
	// why says what decided a cleartext dump, for the log; fellBack is set
	// when nobody chose it (preferred, against a source with no TLS).
	why      string
	fellBack bool
}

// resolveDumpTLS decides how mydumper connects, so it reaches the source the
// way config.ConnectSSL does for capture and for the pre-checks:
//
//   - a tls= in the source DSN wins over the mode, as it does for every Go
//     connection (config.applyTLS): false is no TLS, skip-verify is TLS
//     without a certificate check, preferred is preferred. tls=true checks the
//     certificate against the system's CAs, which mydumper cannot do without
//     a CA file, so it is refused rather than weakened.
//   - disabled: no TLS.
//   - preferred (the default): the source is asked once (sourceEncrypted).
//     A connection the server encrypted means mydumper encrypts, made
//     mandatory so nothing between here and the server can strip it; one it
//     did not means the server offers no TLS and the dump reads in cleartext,
//     exactly as capture does.
//   - required: TLS, no certificate check (a CA is not passed: it would make
//     the dump check what capture does not).
//   - verify-ca / verify-identity: TLS checked against the CA file. mydumper
//     needs the file; an empty ssl_ca (the system's CAs, for Go) is refused.
//
// Settings that cannot be used (an unknown mode, an unreadable CA, a
// certificate without its key) are refused with the words the pre-checks use.
func resolveDumpTLS(ctx context.Context, dsn string, ssl config.SSL) (dumpTLS, error) {
	ssl = sourceSSLOrDefault(ssl)
	if _, err := config.BuildTLSConfig(ssl.Mode, ssl.CA, ssl.Cert, ssl.Key, config.DSNHost(dsn)); err != nil {
		if detail, fix, ok := doctor.TLSSettingsText(err); ok {
			return dumpTLS{}, errors.New(detail + " " + fix)
		}
		return dumpTLS{}, err
	}
	if cfg, err := mysql.ParseDSN(dsn); err == nil && cfg.TLSConfig != "" {
		// The DSN's own tls= carries no CA or client certificate: those
		// entry settings do not apply to it on any Go connection either.
		switch cfg.TLSConfig {
		case "false":
			return dumpTLS{why: "the source DSN sets tls=false, which overrides TLS mode " + ssl.Mode}, nil
		case "skip-verify":
			return dumpTLS{encrypt: true}, nil
		case "preferred":
			return preferredDumpTLS(ctx, dsn, ssl, dumpTLS{})
		default: // "true": the system's trusted CAs and the host name
			bundle, err := caForDump("", "tls=true in the source DSN")
			if err != nil {
				return dumpTLS{}, err
			}
			return dumpTLS{encrypt: true, verify: "identity", ca: bundle}, nil
		}
	}
	t := dumpTLS{cert: ssl.Cert, key: ssl.Key}
	switch ssl.Mode {
	case "disabled":
		return dumpTLS{why: "this server's TLS mode is disabled"}, nil
	case "preferred":
		return preferredDumpTLS(ctx, dsn, ssl, t)
	case "required":
		t.encrypt = true
		return t, nil
	default: // verify-ca, verify-identity (BuildTLSConfig accepted the mode)
		ca, err := caForDump(ssl.CA, "TLS mode "+ssl.Mode)
		if err != nil {
			return dumpTLS{}, err
		}
		t.encrypt, t.ca, t.goCA = true, ca, ssl.CA
		t.verify = "ca"
		if ssl.Mode == "verify-identity" {
			t.verify = "identity"
		}
		return t, nil
	}
}

// caForDump is the CA file mydumper verifies against: ssl_ca when set, else
// the system's CA bundle, which is what Go's "system trusted CAs" read on
// Linux. mydumper takes only a file, so with neither the read is refused.
func caForDump(ca, what string) (string, error) {
	if ca != "" {
		return ca, nil
	}
	if b := systemCABundle(); b != "" {
		return b, nil
	}
	return "", fmt.Errorf("%s checks the server's certificate against the system's trusted CAs, and no CA bundle "+
		"was found on this host (looked in %s), which the full read's mydumper needs as a file. Set ssl_ca in this "+
		"server's entry in console-servers.yaml (for bintrail-console watch, --ssl-ca) to the CA that signed the "+
		"server's certificate, or to a CA bundle file", what, strings.Join(caBundlePaths, ", "))
}

// caBundlePaths are where the usual systems keep their trusted CAs as one PEM
// file, in order: Debian/Ubuntu/Alpine with ca-certificates (the console
// image installs it), RHEL/Fedora, openSUSE, then macOS/BSD/Alpine's
// cert.pem.
var caBundlePaths = []string{
	"/etc/ssl/certs/ca-certificates.crt",
	"/etc/pki/tls/certs/ca-bundle.crt",
	"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
	"/etc/ssl/ca-bundle.pem",
	"/etc/ssl/cert.pem",
}

// systemCABundle returns the first CA bundle file present, "" for none. A
// seam, so the tests do not depend on the host.
var systemCABundle = func() string {
	// SSL_CERT_FILE first: Go's own system roots honor it, so mydumper then
	// checks against the same CAs the Go side did.
	if f := os.Getenv("SSL_CERT_FILE"); f != "" {
		if b := findCABundle([]string{f}); b != "" {
			return b
		}
	}
	return findCABundle(caBundlePaths)
}

func findCABundle(paths []string) string {
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil && fi.Mode().IsRegular() {
			return p
		}
	}
	return ""
}

// preferredDumpTLS asks the source whether it encrypts a preferred
// connection, and dumps the same way. t carries the client certificate.
func preferredDumpTLS(ctx context.Context, dsn string, ssl config.SSL, t dumpTLS) (dumpTLS, error) {
	enc, err := sourceEncrypted(ctx, dsn, ssl)
	if err != nil {
		return dumpTLS{}, fmt.Errorf("could not ask the source whether the full read can encrypt its connection "+
			"(TLS mode preferred): %w", err)
	}
	if !enc {
		return dumpTLS{why: "the source offers no TLS (TLS mode preferred)", fellBack: true}, nil
	}
	t.encrypt = true
	return t, nil
}

// sourceTLSPin opens one connection to the source with TLS mandatory, checked
// the way t says (no check, the CA chain, or chain and host name), and
// returns the SHA-1 fingerprint of the certificate the server presented.
//
// Why it exists (#1996, measured): the arm64 mydumper, linked against
// MariaDB Connector/C 10.11, ignores --ssl-mode REQUIRED when the server
// offers no TLS: against a server with TLS off it exits 0 and dumps in
// cleartext. So before such a build runs, this connection proves the server
// encrypts (a server that does not is refused here, with the refusal worded
// for a person), and the fingerprint pins mydumper to the same certificate
// (ssl-fp, which Connector/C does enforce: with it, a server without TLS is
// refused, and so is any other certificate). Limits: Connector/C 10.11 takes
// only a SHA-1 fingerprint, and a server that rotates its certificate
// between this check and the dump fails the dump, loudly. A build whose
// client library could not be read gets the check but no pin, since its
// library may not know ssl-fp; an active attacker between the check and the
// dump is then not stopped. ctx is not passed down: the connection is bounded
// by the DSN's own timeout (config.ConnectWithTLS). A seam, so the tests need
// no server.
var sourceTLSPin = func(ctx context.Context, dsn string, t dumpTLS) (string, error) {
	mode := "required"
	switch t.verify {
	case "ca":
		mode = "verify-ca"
	case "identity":
		mode = "verify-identity"
	}
	host := config.DSNHost(dsn)
	cfg, err := config.BuildTLSConfig(mode, t.goCA, t.cert, t.key, host)
	if err != nil {
		return "", err
	}
	leaf := captureLeaf(cfg)
	// The DSN's own tls= was already folded into t; it must not replace the
	// mandatory config here (config.applyTLS lets a DSN's tls= win), and its
	// allowFallbackToPlaintext must not turn the mandatory TLS into a
	// silent cleartext connection.
	plain := dsn
	if c, err := mysql.ParseDSN(dsn); err == nil && (c.TLSConfig != "" || c.TLS != nil || c.AllowFallbackToPlaintext) {
		c.TLSConfig, c.TLS, c.AllowFallbackToPlaintext = "", nil, false
		plain = c.FormatDSN()
	}
	db, err := config.ConnectWithTLS(plain, cfg)
	if err != nil {
		if detail, fix, ok := doctor.SourceTLSRefusalText(err, plain, config.SSL{Mode: mode}); ok {
			return "", &sourceTLSRefusal{msg: detail + " " + fix, err: err}
		}
		return "", err
	}
	db.Close()
	if *leaf == nil {
		// Not a certificate problem: no TLS handshake happened at all (the
		// fallback is turned off above, so this is a driver surprise).
		return "", errors.New("the check connection to the source was not encrypted: no TLS handshake happened, " +
			"so the full read cannot be pinned to the server's certificate")
	}
	return tlsFingerprint(*leaf), nil
}

// captureLeaf makes cfg record the certificate the server presents (the
// LEAF, PeerCertificates[0], never an intermediate), after any check cfg
// already does. The returned pointer is filled by the handshake.
func captureLeaf(cfg *tls.Config) *[]byte {
	leaf := new([]byte)
	verify := cfg.VerifyConnection
	cfg.VerifyConnection = func(cs tls.ConnectionState) error {
		if verify != nil {
			if err := verify(cs); err != nil {
				return err
			}
		}
		if len(cs.PeerCertificates) > 0 {
			*leaf = cs.PeerCertificates[0].Raw
		}
		return nil
	}
	return leaf
}

// tlsFingerprint is the SHA-1 of a DER certificate, as Connector/C's ssl-fp
// reads it: upper-case hex pairs joined by colons.
func tlsFingerprint(der []byte) string {
	sum := sha1.Sum(der) //nolint:gosec // the only digest Connector/C 10.11's ssl-fp accepts
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(parts, ":")
}

// writeMydumperTLSPin writes a defaults file holding the pin, for mydumper's
// --defaults-extra-file, in dir (the staging folder, never the dump folder
// the conversion reads). The returned func removes it.
func writeMydumperTLSPin(dir, fp string) (string, func(), error) {
	f, err := os.CreateTemp(dir, "mydumper-tls-*.cnf")
	if err != nil {
		return "", func() {}, err
	}
	path := f.Name()
	remove := func() { _ = os.Remove(path) }
	if _, err := f.WriteString("[client]\nssl-fp=" + fp + "\n"); err != nil {
		f.Close()
		remove()
		return "", func() {}, err
	}
	if err := f.Close(); err != nil {
		remove()
		return "", func() {}, err
	}
	return path, remove, nil
}

// mydumperTLSArgs spells t for the mydumper build. The two builds of the
// pinned mydumper differ (measured on 1.0.3-1 against MariaDB 11.8 with
// require_secure_transport=ON): amd64 links the MySQL client library, which
// accepts every --ssl-mode and, given none, tries TLS on its own; arm64 links
// MariaDB Connector/C, which accepts only REQUIRED and VERIFY_IDENTITY and,
// given none, does not encrypt. VERIFY_IDENTITY with --ca checks the chain
// and the host name on both. verify-ca is VERIFY_CA with --ca, except on
// Connector/C, which lacks it and checks the chain with REQUIRED plus --ca
// (measured). "No TLS" differs too: the MySQL library needs DISABLED to stay
// in cleartext, which Connector/C refuses as unsupported. A build whose library could not be read gets no flag, which
// is cleartext on Connector/C and TLS-when-offered on the MySQL library.
func mydumperTLSArgs(t dumpTLS, lib mydumperlock.ClientLibrary) []string {
	if !t.encrypt {
		if lib == mydumperlock.LibMySQL {
			return []string{"--ssl-mode", "DISABLED"}
		}
		return nil
	}
	var args []string
	switch t.verify {
	case "identity":
		args = []string{"--ssl-mode", "VERIFY_IDENTITY", "--ca", t.ca}
	case "ca":
		// Connector/C has no VERIFY_CA: REQUIRED with --ca checks the chain
		// there (measured). Every other build is told VERIFY_CA by name, so
		// the check never rests on how a library treats REQUIRED plus a CA,
		// and a build that cannot express it refuses loudly.
		if lib == mydumperlock.LibMariaDB {
			args = []string{"--ssl-mode", "REQUIRED", "--ca", t.ca}
		} else {
			args = []string{"--ssl-mode", "VERIFY_CA", "--ca", t.ca}
		}
	default:
		args = []string{"--ssl-mode", "REQUIRED"}
	}
	if t.cert != "" {
		args = append(args, "--cert", t.cert, "--key", t.key)
	}
	return args
}

// mydumperTLSHint words a TLS failure in mydumper's output for the card,
// "" when the output holds none. The messages matched are the ones the two
// builds of the pinned mydumper print (measured): Connector/C prefixes
// "TLS/SSL error:", the MySQL library "SSL connection error:".
func mydumperTLSHint(output string, t dumpTLS) string {
	var line string
	for _, l := range strings.Split(output, "\n") {
		low := strings.ToLower(l)
		if strings.Contains(low, "tls/ssl error") || strings.Contains(low, "ssl connection error") ||
			strings.Contains(low, "insecure transport") {
			line = l
			break
		}
	}
	if line == "" {
		return ""
	}
	low := strings.ToLower(line)
	_, detail, _ := strings.Cut(line, "Error connection to database: ")
	if detail == "" {
		detail = strings.TrimSpace(line)
	}
	switch {
	case strings.Contains(low, "insecure transport"):
		return "the server only accepts encrypted (TLS) connections, and this read was not encrypted: " + t.why +
			". Set this server's TLS mode to preferred or required (the ssl_mode line of its entry in console-servers.yaml), " +
			"and remove any tls=false from the source DSN"
	case strings.Contains(low, "ssl is required"):
		return "mydumper could not connect encrypted: the server does not offer encrypted (TLS) connections, and this read " +
			"requires them. Turn TLS on in the server's configuration, or set this server's TLS mode to preferred"
	case strings.Contains(low, "fingerprint"):
		return "the server presented a different certificate to mydumper than to DBTrail's check a moment before. " +
			"If its certificate was just replaced, run the snapshot again. If it repeats, the source address may reach " +
			"several servers with different certificates (a load balancer, a reader endpoint or round-robin DNS): point " +
			"the source at one server. Otherwise something between DBTrail and the server answered: check the network path"
	case strings.Contains(low, "certificate"):
		hint := "mydumper did not trust the server's certificate: it is not signed by a CA in " + t.ca
		if t.verify == "identity" {
			hint += ", or it does not name the host name DBTrail connects to"
		}
		return hint + ". Set ssl_ca in this server's entry in console-servers.yaml to the CA that signed the server's " +
			"certificate, or set its TLS mode to required to encrypt without checking the certificate"
	default:
		return "mydumper's encrypted connection failed: " + strings.TrimSpace(detail)
	}
}
