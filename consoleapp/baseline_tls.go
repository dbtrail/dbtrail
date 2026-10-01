package consoleapp

import (
	"context"
	"errors"
	"fmt"

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
	verify    string
	ca        string
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
			return dumpTLS{why: "the source DSN sets tls=false"}, nil
		case "skip-verify":
			return dumpTLS{encrypt: true}, nil
		case "preferred":
			return preferredDumpTLS(ctx, dsn, ssl, dumpTLS{})
		default:
			return dumpTLS{}, fmt.Errorf("the source DSN sets tls=%s, which checks the server's certificate against "+
				"the system's trusted CAs, and the full read's mydumper can check a certificate only against a CA file. "+
				"Remove tls= from the source DSN and set this server's ssl_mode to verify-identity with ssl_ca naming "+
				"the CA file (or to required, to encrypt without checking the certificate)", cfg.TLSConfig)
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
		if ssl.CA == "" {
			return dumpTLS{}, fmt.Errorf("this server's TLS mode is %s, and the full read's mydumper can check the "+
				"server's certificate only against a CA file: set ssl_ca in this server's entry in console-servers.yaml "+
				"(for bintrail-console watch, --ssl-ca) to the CA that signed the server's certificate", ssl.Mode)
		}
		t.encrypt, t.ca = true, ssl.CA
		t.verify = "ca"
		if ssl.Mode == "verify-identity" {
			t.verify = "identity"
		}
		return t, nil
	}
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
