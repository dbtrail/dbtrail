package consoleapp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// stubSourceEncrypted replaces the preferred-mode probe with a fixed answer and
// counts the calls, so a mode that must not probe is caught probing.
func stubSourceEncrypted(t *testing.T, encrypted bool, err error) *int {
	t.Helper()
	calls := 0
	prev := sourceEncrypted
	sourceEncrypted = func(context.Context, string, config.SSL) (bool, error) {
		calls++
		return encrypted, err
	}
	t.Cleanup(func() { sourceEncrypted = prev })
	return &calls
}

// writeTLSFiles writes a self-signed certificate and its key as PEM files: a
// readable CA for the settings check, and a loadable client certificate pair.
func writeTLSFiles(t *testing.T) (ca, cert, key string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cert, key = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	for p, b := range map[string]*pem.Block{cert: {Type: "CERTIFICATE", Bytes: der}, key: {Type: "EC PRIVATE KEY", Bytes: kder}} {
		if err := os.WriteFile(p, pem.EncodeToMemory(b), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return cert, cert, key
}

// #1996: how each source TLS setting reaches mydumper. The dump must connect
// the way capture and the pre-checks do (config.ConnectSSL): preferred
// encrypts when the server offers TLS and reads in cleartext only when it
// offers none; required encrypts without checking the certificate;
// verify-ca/verify-identity check it against the CA file; a DSN's own tls=
// wins over the mode. Spelled so BOTH builds of the pinned mydumper accept it
// (measured against MariaDB 11.8 with require_secure_transport=ON: the amd64
// build links the MySQL client library, the arm64 one MariaDB Connector/C).
func TestMydumperTLS_ModeMapping(t *testing.T) {
	ca, crt, key := writeTLSFiles(t)
	const dsn = "u:p@tcp(db.example.com:3306)/"
	type want struct {
		mysqlLib   []string // args for a build linked against the MySQL client library
		mariadbLib []string // args for MariaDB Connector/C (and an unknown build)
		probes     int
		errHas     string
	}
	for _, tc := range []struct {
		name      string
		dsn       string
		ssl       config.SSL
		encrypted bool // the probe's answer, used only under preferred
		want      want
	}{
		{"empty mode is preferred; server offers TLS", dsn, config.SSL{}, true,
			want{[]string{"--ssl-mode", "REQUIRED"}, []string{"--ssl-mode", "REQUIRED"}, 1, ""}},
		{"preferred, server offers TLS", dsn, config.SSL{Mode: "preferred"}, true,
			want{[]string{"--ssl-mode", "REQUIRED"}, []string{"--ssl-mode", "REQUIRED"}, 1, ""}},
		{"preferred, server offers no TLS: cleartext, as capture", dsn, config.SSL{Mode: "preferred"}, false,
			want{[]string{"--ssl-mode", "DISABLED"}, nil, 1, ""}},
		{"preferred carries a client certificate", dsn, config.SSL{Mode: "preferred", Cert: crt, Key: key}, true,
			want{[]string{"--ssl-mode", "REQUIRED", "--cert", crt, "--key", key}, []string{"--ssl-mode", "REQUIRED", "--cert", crt, "--key", key}, 1, ""}},
		{"preferred does not verify, so a CA is not passed", dsn, config.SSL{Mode: "preferred", CA: ca}, true,
			want{[]string{"--ssl-mode", "REQUIRED"}, []string{"--ssl-mode", "REQUIRED"}, 1, ""}},
		{"disabled: no TLS, no probe", dsn, config.SSL{Mode: "disabled"}, true,
			want{[]string{"--ssl-mode", "DISABLED"}, nil, 0, ""}},
		{"required: TLS, no verification, no probe", dsn, config.SSL{Mode: "required", CA: ca}, false,
			want{[]string{"--ssl-mode", "REQUIRED"}, []string{"--ssl-mode", "REQUIRED"}, 0, ""}},
		{"verify-ca: TLS checked against the CA", dsn, config.SSL{Mode: "verify-ca", CA: ca}, false,
			want{[]string{"--ssl-mode", "REQUIRED", "--ca", ca}, []string{"--ssl-mode", "REQUIRED", "--ca", ca}, 0, ""}},
		{"verify-identity: CA and host name", dsn, config.SSL{Mode: "verify-identity", CA: ca, Cert: crt, Key: key}, false,
			want{[]string{"--ssl-mode", "VERIFY_IDENTITY", "--ca", ca, "--cert", crt, "--key", key},
				[]string{"--ssl-mode", "VERIFY_IDENTITY", "--ca", ca, "--cert", crt, "--key", key}, 0, ""}},
		{"verify-ca without a CA file is refused", dsn, config.SSL{Mode: "verify-ca"}, false,
			want{errHas: "ssl_ca"}},
		{"verify-identity without a CA file is refused", dsn, config.SSL{Mode: "verify-identity"}, false,
			want{errHas: "ssl_ca"}},
		{"an unknown mode is refused, never weakened", dsn, config.SSL{Mode: "require"}, false,
			want{errHas: "ssl_mode"}},
		{"an unreadable CA is refused before anything runs", dsn, config.SSL{Mode: "verify-ca", CA: "/nonexistent/ca.pem"}, false,
			want{errHas: "/nonexistent/ca.pem"}},
		{"DSN tls=false wins over the mode", dsn + "?tls=false", config.SSL{Mode: "verify-identity", CA: ca}, true,
			want{[]string{"--ssl-mode", "DISABLED"}, nil, 0, ""}},
		{"DSN tls=0 is tls=false", dsn + "?tls=0", config.SSL{Mode: "preferred"}, true,
			want{[]string{"--ssl-mode", "DISABLED"}, nil, 0, ""}},
		{"DSN tls=skip-verify is required", dsn + "?tls=skip-verify", config.SSL{Mode: "disabled"}, false,
			want{[]string{"--ssl-mode", "REQUIRED"}, []string{"--ssl-mode", "REQUIRED"}, 0, ""}},
		{"DSN tls=preferred probes", dsn + "?tls=preferred", config.SSL{Mode: "disabled"}, true,
			want{[]string{"--ssl-mode", "REQUIRED"}, []string{"--ssl-mode", "REQUIRED"}, 1, ""}},
		{"DSN tls=true cannot be expressed without a CA file", dsn + "?tls=true", config.SSL{Mode: "preferred"}, true,
			want{errHas: "tls=true"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, lib := range []mydumperlock.ClientLibrary{mydumperlock.LibMySQL, mydumperlock.LibMariaDB, mydumperlock.LibUnknown} {
				calls := stubSourceEncrypted(t, tc.encrypted, nil)
				got, err := resolveDumpTLS(context.Background(), tc.dsn, tc.ssl)
				if tc.want.errHas != "" {
					if err == nil || !strings.Contains(err.Error(), tc.want.errHas) {
						t.Fatalf("%s: err = %v, want it to mention %q", lib, err, tc.want.errHas)
					}
					return
				}
				if err != nil {
					t.Fatalf("%s: %v", lib, err)
				}
				if *calls != tc.want.probes {
					t.Errorf("%s: probed the source %d times, want %d", lib, *calls, tc.want.probes)
				}
				args := mydumperTLSArgs(got, lib)
				want := tc.want.mariadbLib
				if lib == mydumperlock.LibMySQL {
					want = tc.want.mysqlLib
				}
				if !slices.Equal(args, want) {
					t.Errorf("%s: args = %q, want %q", lib, args, want)
				}
			}
		})
	}
}

// Under preferred the dump cannot decide without asking the source; when the
// source cannot be asked, the read stops and says why instead of guessing a
// mode (a guess of "cleartext" would fail on a TLS-only server, a guess of
// "TLS" would fail on one without TLS).
func TestMydumperTLS_PreferredProbeFailureIsLoud(t *testing.T) {
	stubSourceEncrypted(t, false, errors.New("dial tcp: connection refused"))
	_, err := resolveDumpTLS(context.Background(), "u:p@tcp(db:3306)/", config.SSL{Mode: "preferred"})
	if err == nil || !strings.Contains(err.Error(), "connection refused") || !strings.Contains(err.Error(), "encrypt") {
		t.Fatalf("err = %v, want the probe's failure, worded about encryption", err)
	}
}

// The request a snapshot runs from carries the entry's TLS, empty mode read as
// preferred: one constructor for the button and the schedule.
func TestBaselineRequestFor_CarriesSourceSSL(t *testing.T) {
	r := console.BaselineRequestFor(console.ServerEntry{ID: "s", SourceDSN: "u:p@tcp(a:3306)/"})
	if r.SourceSSL.Mode != "preferred" {
		t.Errorf("entry with no ssl_mode: %q, want preferred", r.SourceSSL.Mode)
	}
	r = console.BaselineRequestFor(console.ServerEntry{ID: "s", SourceDSN: "u:p@tcp(a:3306)/", SSLMode: "verify-ca", SSLCA: "/ca.pem"})
	if r.SourceSSL.Mode != "verify-ca" || r.SourceSSL.CA != "/ca.pem" {
		t.Errorf("entry TLS not carried: %+v", r.SourceSSL)
	}
	var rr refreshRequest
	withSource(&rr, console.ServerEntry{ID: "s", SourceDSN: "u:p@tcp(a:3306)/", SSLMode: "required"})
	if rr.SourceSSL.Mode != "required" {
		t.Errorf("update request (new-tables check) TLS = %+v, want required", rr.SourceSSL)
	}
}

// realCheckMydumperPrivileges is the production seam, captured at package
// init, for tests that replace it to restore.
var realCheckMydumperPrivileges = checkMydumperPrivileges

// The TLS options reach the mydumper the run launches, for each build of the
// pinned version, and the privilege preflight of both point-consistent lock
// modes connects with the same TLS settings.
func TestRunMydumper_PassesTheSourceTLS(t *testing.T) {
	const (
		mariadbBuild = "mydumper v1.0.3-1, built against MariaDB 10.11.18 with SSL support"
		mysqlBuild   = versionModern // built against MySQL 8.4.9
	)
	for _, tc := range []struct {
		name      string
		build     string
		mode      baseline.LockMode
		encrypted bool
		want      []string // consecutive args that must appear
		absent    string
	}{
		{"TLS-only source, default mode, arm64 build, lock-all", mariadbBuild, baseline.LockModeLockAll, true, []string{"--ssl-mode", "REQUIRED"}, ""},
		{"TLS-only source, default mode, amd64 build, ftwrl", mysqlBuild, baseline.LockModeFTWRL, true, []string{"--ssl-mode", "REQUIRED"}, ""},
		{"source without TLS, default mode, amd64 build", mysqlBuild, baseline.LockModeLockAll, false, []string{"--ssl-mode", "DISABLED"}, ""},
		{"source without TLS, default mode, arm64 build", mariadbBuild, baseline.LockModeFTWRL, false, nil, "--ssl-mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := fakeConsoleMydumper(t, printsVersion(tc.build))
			stubSourceEncrypted(t, tc.encrypted, nil)
			var gotSSL []config.SSL
			checkMydumperPrivileges = func(_ context.Context, _ string, ssl config.SSL, _ baseline.LockMode, _ mydumperlock.Remedy, _ []string) error {
				gotSSL = append(gotSSL, ssl)
				return nil
			}
			t.Cleanup(func() { checkMydumperPrivileges = realCheckMydumperPrivileges })

			out := filepath.Join(t.TempDir(), "out")
			if err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", config.SSL{}, []string{"appdb"}, out, tc.mode, lockModeFromEnv); err != nil {
				t.Fatalf("runMydumper: %v", err)
			}
			args := recordedArgs(t, record)
			if tc.want != nil {
				i := slices.Index(args, tc.want[0])
				if i < 0 || i+1 >= len(args) || args[i+1] != tc.want[1] {
					t.Errorf("args %q lack %q", args, tc.want)
				}
			}
			if tc.absent != "" && slices.Contains(args, tc.absent) {
				t.Errorf("args %q carry %s, which this build refuses", args, tc.absent)
			}
			assertOutputdirLast(t, args, out)
			if len(gotSSL) != 1 || gotSSL[0] != (config.SSL{}) {
				t.Errorf("preflight got TLS %+v, want the request's (one call)", gotSSL)
			}
		})
	}
}

// A TLS setting the dump cannot honor stops the read before the privilege
// preflight and before mydumper: never a quietly weaker connection.
func TestRunMydumper_UnusableTLSStopsBeforeAnything(t *testing.T) {
	record := fakeConsoleMydumper(t, printsVersion(versionModern))
	calls := stubPreflight(t, nil)
	probes := stubSourceEncrypted(t, true, nil)
	err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "verify-ca"}, []string{"appdb"},
		filepath.Join(t.TempDir(), "out"), baseline.LockModeLockAll, lockModeFromEnv)
	if err == nil || !strings.Contains(err.Error(), "ssl_ca") {
		t.Fatalf("err = %v, want a refusal naming ssl_ca", err)
	}
	if *calls != 0 || *probes != 0 {
		t.Errorf("preflight ran %d, probe ran %d times; want neither", *calls, *probes)
	}
	assertNeverLaunched(t, record)
}

// The disk estimate and the new-tables check of an update read the source
// with the request's TLS.
func TestFullReadSeams_ForwardTheSourceTLS(t *testing.T) {
	want := config.SSL{Mode: "required"}
	var got config.SSL
	prev := dumpSizeEstimateFn
	dumpSizeEstimateFn = func(_ context.Context, _ string, ssl config.SSL, _ []string) (dumpEstimate, error) {
		got = ssl
		return dumpEstimate{}, errors.New("stop")
	}
	t.Cleanup(func() { dumpSizeEstimateFn = prev })
	s := &baselineSupervisor{ctx: context.Background(), stagingDir: t.TempDir()}
	_, _, _ = s.checkDumpDisk(console.BaselineRequest{SourceDSN: "u:p@tcp(a:3306)/", SourceSSL: want})
	if got != want {
		t.Errorf("disk estimate got %+v, want %+v", got, want)
	}

	got = config.SSL{}
	prevList := listSourceTables
	listSourceTables = func(_ context.Context, _ string, ssl config.SSL, _ []string) ([]string, bool, error) {
		got = ssl
		return nil, false, nil
	}
	t.Cleanup(func() { listSourceTables = prevList })
	s.checkNewTables(refreshRequest{SourceDSN: "u:p@tcp(a:3306)/", SourceSSL: want}, nil)
	if got != want {
		t.Errorf("new-tables check got %+v, want %+v", got, want)
	}
}
