package consoleapp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
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
	stubSystemCABundle(t, testBundle)
	const dsn = "u:p@tcp(db.example.com:3306)/"
	type want struct {
		mysqlLib   []string // args for a build linked against the MySQL client library
		mariadbLib []string // args for MariaDB Connector/C
		// An unknown build gets the MySQL library's spelling for TLS (named
		// modes, a build that lacks one refuses loudly) and no flag for none.
		probes int
		errHas string
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
			want{[]string{"--ssl-mode", "VERIFY_CA", "--ca", ca}, []string{"--ssl-mode", "REQUIRED", "--ca", ca}, 0, ""}},
		{"verify-identity: CA and host name", dsn, config.SSL{Mode: "verify-identity", CA: ca, Cert: crt, Key: key}, false,
			want{[]string{"--ssl-mode", "VERIFY_IDENTITY", "--ca", ca, "--cert", crt, "--key", key},
				[]string{"--ssl-mode", "VERIFY_IDENTITY", "--ca", ca, "--cert", crt, "--key", key}, 0, ""}},
		{"verify-ca without a CA file uses the system bundle", dsn, config.SSL{Mode: "verify-ca"}, false,
			want{[]string{"--ssl-mode", "VERIFY_CA", "--ca", testBundle}, []string{"--ssl-mode", "REQUIRED", "--ca", testBundle}, 0, ""}},
		{"verify-identity without a CA file uses the system bundle", dsn, config.SSL{Mode: "verify-identity"}, false,
			want{[]string{"--ssl-mode", "VERIFY_IDENTITY", "--ca", testBundle}, []string{"--ssl-mode", "VERIFY_IDENTITY", "--ca", testBundle}, 0, ""}},
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
		{"DSN tls=true verifies against the system bundle", dsn + "?tls=true", config.SSL{Mode: "preferred", CA: ca}, true,
			want{[]string{"--ssl-mode", "VERIFY_IDENTITY", "--ca", testBundle}, []string{"--ssl-mode", "VERIFY_IDENTITY", "--ca", testBundle}, 0, ""}},
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
				switch {
				case lib == mydumperlock.LibMySQL:
					want = tc.want.mysqlLib
				case lib == mydumperlock.LibUnknown && got.encrypt:
					want = tc.want.mysqlLib
				case lib == mydumperlock.LibUnknown:
					want = nil
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
	stubSystemCABundle(t, "") // verify-ca with no CA file and no system bundle
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

const testBundle = "/etc/ssl/certs/ca-certificates.crt"

// stubSystemCABundle fixes the system CA bundle path ("" = none found).
func stubSystemCABundle(t *testing.T, path string) {
	t.Helper()
	prev := systemCABundle
	systemCABundle = func() string { return path }
	t.Cleanup(func() { systemCABundle = prev })
}

// With no CA file set and no system bundle on the host, a verifying mode is
// refused, and the refusal says ssl_ca can name a bundle.
func TestMydumperTLS_NoSystemBundle(t *testing.T) {
	stubSystemCABundle(t, "")
	for _, tc := range []struct {
		dsn string
		ssl config.SSL
	}{
		{"u:p@tcp(db:3306)/", config.SSL{Mode: "verify-ca"}},
		{"u:p@tcp(db:3306)/", config.SSL{Mode: "verify-identity"}},
		{"u:p@tcp(db:3306)/?tls=true", config.SSL{Mode: "preferred"}},
	} {
		_, err := resolveDumpTLS(context.Background(), tc.dsn, tc.ssl)
		if err == nil || !strings.Contains(err.Error(), "ssl_ca") || !strings.Contains(err.Error(), "bundle") {
			t.Errorf("%s %+v: err = %v, want a refusal saying ssl_ca can name a CA bundle", tc.dsn, tc.ssl, err)
		}
	}
}

// The bundle probe takes the first path that exists, in order.
func TestFindCABundle(t *testing.T) {
	dir := t.TempDir()
	b := filepath.Join(dir, "b.pem")
	if err := os.WriteFile(b, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := findCABundle([]string{filepath.Join(dir, "a.pem"), b, filepath.Join(dir, "c.pem")}); got != b {
		t.Errorf("findCABundle = %q, want %q", got, b)
	}
	if got := findCABundle([]string{filepath.Join(dir, "missing")}); got != "" {
		t.Errorf("findCABundle with nothing present = %q, want empty", got)
	}
	if got := findCABundle([]string{dir}); got != "" {
		t.Errorf("a directory was taken for a bundle: %q", got)
	}
}

// stubSourceTLSPin replaces the mandatory-TLS check before mydumper with a
// fixed answer and counts the calls.
func stubSourceTLSPin(t *testing.T, fp string, err error) *int {
	t.Helper()
	calls := 0
	prev := sourceTLSPin
	sourceTLSPin = func(context.Context, string, dumpTLS) (string, error) {
		calls++
		return fp, err
	}
	t.Cleanup(func() { sourceTLSPin = prev })
	return &calls
}

// On a mydumper whose --ssl-mode REQUIRED does not enforce TLS (MariaDB
// Connector/C: measured, it dumps in cleartext from a server without TLS),
// every encrypted dump is preceded by a Go connection with TLS mandatory, in
// every lock mode, and mydumper is pinned to the certificate that connection
// saw (ssl-fp in a defaults file), which Connector/C does enforce. A build
// linked against the MySQL client library enforces REQUIRED itself.
func TestRunMydumper_EnforcesTLSWhereTheBuildDoesNot(t *testing.T) {
	const (
		mariadbBuild = "mydumper v1.0.3-1, built against MariaDB 10.11.18 with SSL support"
		unknownBuild = "mydumper v1.0.3-1"
		fp           = "AA:BB:CC"
	)
	for _, tc := range []struct {
		name    string
		build   string
		mode    baseline.LockMode
		ssl     config.SSL
		enc     bool
		pins    int
		pinFile bool
	}{
		{"Connector/C, required, no-lock", mariadbBuild, baseline.LockModeNoLock, config.SSL{Mode: "required"}, false, 1, true},
		{"Connector/C, preferred resolved to TLS, lock-all", mariadbBuild, baseline.LockModeLockAll, config.SSL{}, true, 1, true},
		{"Connector/C, preferred without TLS: nothing to enforce", mariadbBuild, baseline.LockModeFTWRL, config.SSL{}, false, 0, false},
		{"Connector/C, disabled", mariadbBuild, baseline.LockModeNoLock, config.SSL{Mode: "disabled"}, false, 0, false},
		{"MySQL library enforces REQUIRED itself", versionModern, baseline.LockModeNoLock, config.SSL{Mode: "required"}, false, 0, false},
		{"unknown build: checked, not pinned", unknownBuild, baseline.LockModeNoLock, config.SSL{Mode: "required"}, false, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := fakeConsoleMydumper(t, printsVersion(tc.build))
			// The fake also copies the defaults file it was given, which the
			// run removes once mydumper exits.
			copied := record + ".cnf"
			script, err := os.ReadFile(filepath.Join(filepath.Dir(record), "mydumper"))
			if err != nil {
				t.Fatal(err)
			}
			keep := "prev=; for a in \"$@\"; do [ \"$prev\" = --defaults-extra-file ] && { while IFS= read -r l; do printf '%s\\n' \"$l\"; done < \"$a\" > '" + copied + "'; }; prev=$a; done\n"
			script = []byte(strings.Replace(string(script), "printf '%s\\n' \"$@\"", keep+"printf '%s\\n' \"$@\"", 1))
			if err := os.WriteFile(filepath.Join(filepath.Dir(record), "mydumper"), script, 0o755); err != nil {
				t.Fatal(err)
			}
			stubPreflight(t, nil)
			stubSourceEncrypted(t, tc.enc, nil)
			pins := stubSourceTLSPin(t, fp, nil)

			out := filepath.Join(t.TempDir(), "out")
			if err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", tc.ssl, []string{"appdb"}, out, tc.mode, lockModeFromEnv); err != nil {
				t.Fatalf("runMydumper: %v", err)
			}
			if *pins != tc.pins {
				t.Errorf("mandatory-TLS check ran %d times, want %d", *pins, tc.pins)
			}
			args := recordedArgs(t, record)
			i := slices.Index(args, "--defaults-extra-file")
			if (i >= 0) != tc.pinFile {
				t.Fatalf("args %q: --defaults-extra-file present = %v, want %v", args, i >= 0, tc.pinFile)
			}
			if tc.pinFile {
				pinText, _ := os.ReadFile(copied)
				if !strings.Contains(string(pinText), "ssl-fp="+fp) {
					t.Errorf("pin file holds %q, want ssl-fp=%s", pinText, fp)
				}
			}
			if tc.pinFile {
				if _, err := os.Stat(args[i+1]); !os.IsNotExist(err) {
					t.Errorf("pin file %s left behind after the dump (stat: %v)", args[i+1], err)
				}
			}
		})
	}
}

// A server that does not encrypt when the dump must: refused before mydumper.
func TestRunMydumper_MandatoryTLSRefusalStopsTheDump(t *testing.T) {
	record := fakeConsoleMydumper(t, printsVersion("mydumper v1.0.3-1, built against MariaDB 10.11.18 with SSL support"))
	stubPreflight(t, nil)
	stubSourceTLSPin(t, "", errors.New("This server does not offer encrypted (TLS) connections"))
	err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "required"}, []string{"appdb"},
		filepath.Join(t.TempDir(), "out"), baseline.LockModeNoLock, lockModeFromEnv)
	if err == nil || !strings.Contains(err.Error(), "does not offer encrypted") {
		t.Fatalf("err = %v, want the mandatory-TLS refusal", err)
	}
	assertNeverLaunched(t, record)
}

func TestTLSFingerprint(t *testing.T) {
	if got := tlsFingerprint([]byte("abc")); got != "A9:99:3E:36:47:06:81:6A:BA:3E:25:71:78:50:C2:6C:9C:D0:D8:9D" {
		t.Errorf("tlsFingerprint = %s", got)
	}
}

// Every source read of a full read gets the request's TLS settings: a
// distinctive non-empty value, so a call site that hard-codes a zero
// config.SSL (which reads as preferred) cannot pass.
func TestRunMydumper_ForwardsTheRequestTLSToEverySeam(t *testing.T) {
	ca, _, _ := writeTLSFiles(t)
	want := config.SSL{Mode: "required", CA: ca}
	run := func(t *testing.T, build string, mode baseline.LockMode) {
		t.Helper()
		fakeConsoleMydumper(t, printsVersion(build))
		if err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", want, []string{"appdb"},
			filepath.Join(t.TempDir(), "out"), mode, lockModeFromEnv); err != nil {
			t.Fatalf("runMydumper: %v", err)
		}
	}
	t.Run("privilege preflight", func(t *testing.T) {
		var got []config.SSL
		checkMydumperPrivileges = func(_ context.Context, _ string, ssl config.SSL, _ baseline.LockMode, _ mydumperlock.Remedy, _ []string) error {
			got = append(got, ssl)
			return nil
		}
		t.Cleanup(func() { checkMydumperPrivileges = realCheckMydumperPrivileges })
		run(t, versionModern, baseline.LockModeLockAll)
		if len(got) != 1 || got[0] != want {
			t.Errorf("preflight got %+v, want [%+v]", got, want)
		}
	})
	t.Run("server version read", func(t *testing.T) {
		var got []config.SSL
		prev := sourceServerVersion
		sourceServerVersion = func(_ context.Context, _ string, ssl config.SSL) (string, error) {
			got = append(got, ssl)
			return "10.11.0-MariaDB", nil
		}
		t.Cleanup(func() { sourceServerVersion = prev })
		run(t, versionDistro, baseline.LockModeFTWRL)
		if len(got) != 1 || got[0] != want {
			t.Errorf("version read got %+v, want [%+v]", got, want)
		}
	})
	t.Run("no-lock table count", func(t *testing.T) {
		var got []config.SSL
		prev := multiTableNoLockCheck
		multiTableNoLockCheck = func(_ context.Context, _ string, ssl config.SSL, _ []string) { got = append(got, ssl) }
		t.Cleanup(func() { multiTableNoLockCheck = prev })
		run(t, versionModern, baseline.LockModeNoLock)
		if len(got) != 1 || got[0] != want {
			t.Errorf("table count got %+v, want [%+v]", got, want)
		}
	})
}

// dumpAttempt hands the request's TLS to the dump.
func TestDumpAttempt_PassesTheRequestTLS(t *testing.T) {
	want := config.SSL{Mode: "verify-identity", CA: "/ca/bundle.pem"}
	var got []config.SSL
	prev := runMydumperFunc
	runMydumperFunc = func(_ context.Context, _ string, ssl config.SSL, _ []string, _ string, _ baseline.LockMode, _ lockModeSource) error {
		got = append(got, ssl)
		return errors.New("stop")
	}
	t.Cleanup(func() { runMydumperFunc = prev })
	s := &baselineSupervisor{ctx: context.Background(), stagingDir: t.TempDir()}
	_, _ = s.dumpAttempt(console.BaselineRequest{ServerID: "s", SourceDSN: "u:p@tcp(a:3306)/", SourceSSL: want}, baseline.LockModeLockAll, lockModeFromEnv)
	if len(got) != 1 || got[0] != want {
		t.Errorf("runMydumper got %+v, want [%+v]", got, want)
	}
}

// A read made without encryption says so on the run: the live status and the
// history record carry the note, not only the daemon log.
func TestFullRead_CleartextReadIsNotedInStatusAndHistory(t *testing.T) {
	stage := t.TempDir()
	stubEstimate(t, dumpEstimate{bytes: 1, tables: 1}, nil)
	diskByPath(t, map[string]uint64{stage: 100 * gib})
	prev := runMydumperFunc
	runMydumperFunc = func(ctx context.Context, _ string, _ config.SSL, _ []string, _ string, _ baseline.LockMode, _ lockModeSource) error {
		reportTransportNote(ctx, "Read without encryption: the source offers no TLS (TLS mode preferred).")
		return errors.New("fake mydumper stopped")
	}
	t.Cleanup(func() { runMydumperFunc = prev })
	s := supWithHistory(t, stage)
	st := runFullRead(t, s, console.BaselineRequest{ServerID: "s1", ServerName: "wp", SourceDSN: "src", S3: "s3://b/p"})
	if !strings.Contains(st.TransportNote, "without encryption") {
		t.Fatalf("status = %+v, want the cleartext note", st)
	}
	runs := s.history.List("s1")
	if len(runs) != 1 || runs[0].TransportNote != st.TransportNote {
		t.Fatalf("history = %+v, want the note kept", runs)
	}
}

// runMydumper reports the cleartext read, once, with its reason; an
// encrypted read reports nothing.
func TestRunMydumper_ReportsACleartextRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		ssl  config.SSL
		dsn  string
		enc  bool
		want string
	}{
		{"preferred, no TLS on the source", config.SSL{}, "u:p@tcp(127.0.0.1:1)/", false, "Read without encryption: the source offers no TLS (TLS mode preferred)."},
		{"required overridden by the DSN", config.SSL{Mode: "required"}, "u:p@tcp(127.0.0.1:1)/?tls=false", false, "Read without encryption: the source DSN sets tls=false, which overrides TLS mode required."},
		{"preferred, TLS on the source", config.SSL{}, "u:p@tcp(127.0.0.1:1)/", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeConsoleMydumper(t, printsVersion(versionModern))
			stubPreflight(t, nil)
			stubSourceEncrypted(t, tc.enc, nil)
			var notes []string
			ctx := withTransportNote(context.Background(), func(n string) { notes = append(notes, n) })
			if err := runMydumper(ctx, tc.dsn, tc.ssl, []string{"appdb"}, filepath.Join(t.TempDir(), "out"), baseline.LockModeLockAll, lockModeFromEnv); err != nil {
				t.Fatal(err)
			}
			if tc.want == "" && len(notes) != 0 || tc.want != "" && (len(notes) != 1 || notes[0] != tc.want) {
				t.Errorf("notes = %q, want %q", notes, tc.want)
			}
		})
	}
}

// mydumper's own TLS failures (its messages as measured on both builds of
// 1.0.3-1) are worded for the card, ahead of the raw output.
func TestMydumperTLSHint(t *testing.T) {
	enc := dumpTLS{encrypt: true}
	vca := dumpTLS{encrypt: true, verify: "ca", ca: "/etc/ssl/certs/ca-certificates.crt"}
	vid := dumpTLS{encrypt: true, verify: "identity", ca: "/ca.pem"}
	plain := dumpTLS{why: "this server's TLS mode is disabled"}
	crit := "** (mydumper:7): CRITICAL **: 20:09:57.521: Error connection to database: "
	for _, tc := range []struct {
		name, out string
		t         dumpTLS
		want      []string
	}{
		{"no TLS, Connector/C", crit + "TLS/SSL error: SSL is required, but the server does not support it", enc, []string{"does not offer", "preferred"}},
		{"no TLS, MySQL library", crit + "SSL connection error: SSL is required but the server doesn't support it", enc, []string{"does not offer"}},
		{"untrusted chain", crit + "TLS/SSL error: self-signed certificate in certificate chain", vca, []string{"did not trust", "/etc/ssl/certs/ca-certificates.crt", "ssl_ca"}},
		{"verify failed, MySQL library", crit + "SSL connection error: error:0A000086:SSL routines::certificate verify failed", vid, []string{"did not trust", "host name"}},
		{"identity failed, Connector/C", crit + "TLS/SSL error: Validation of SSL server certificate failed", vid, []string{"did not trust", "host name"}},
		{"pin mismatch", crit + "TLS/SSL error: Fingerprint verification of server certificate failed", enc, []string{"different certificate"}},
		{"3159", crit + "Connections using insecure transport are prohibited while --require_secure_transport=ON.", plain, []string{"only accepts encrypted", "TLS mode is disabled"}},
		{"other TLS error", crit + "TLS/SSL error: unexpected eof", enc, []string{"encrypted connection failed", "unexpected eof"}},
		{"not TLS", crit + "Access denied for user", enc, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := mydumperTLSHint(tc.out, tc.t)
			if tc.want == nil {
				if got != "" {
					t.Fatalf("hint %q for a non-TLS failure", got)
				}
				return
			}
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("hint %q lacks %q", got, w)
				}
			}
		})
	}
}

// The worded TLS hint reaches the run's error ahead of mydumper's output.
func TestRunMydumper_TLSFailureIsWorded(t *testing.T) {
	record := fakeConsoleMydumper(t, printsVersion(versionModern))
	script := filepath.Join(filepath.Dir(record), "mydumper")
	b, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	k := strings.LastIndex(string(b), "exit 0\n") // the dump branch, after the --version one
	b = []byte(string(b[:k]) + "printf '%s\\n' '** (mydumper:7): CRITICAL **: Error connection to database: SSL connection error: SSL is required but the server doesn'\\''t support it' >&2; exit 1\n")
	if err := os.WriteFile(script, b, 0o755); err != nil {
		t.Fatal(err)
	}
	stubPreflight(t, nil)
	err = runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "required"}, []string{"appdb"},
		filepath.Join(t.TempDir(), "out"), baseline.LockModeNoLock, lockModeFromEnv)
	if err == nil {
		t.Fatal("a failed mydumper passed")
	}
	if i, j := strings.Index(err.Error(), "does not offer encrypted"), strings.Index(err.Error(), "; output:"); i < 0 || j < i {
		t.Fatalf("err = %v, want the worded hint ahead of the output", err)
	}
}

// A build whose client library is unknown is checked but not pinned: the
// run says so, not only the log.
func TestRunMydumper_UnpinnedReadIsNoted(t *testing.T) {
	fakeConsoleMydumper(t, printsVersion("mydumper v1.0.3-1"))
	stubPreflight(t, nil)
	stubSourceTLSPin(t, "AA", nil)
	var notes []string
	ctx := withTransportNote(context.Background(), func(n string) { notes = append(notes, n) })
	if err := runMydumper(ctx, "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "required"}, []string{"appdb"},
		filepath.Join(t.TempDir(), "out"), baseline.LockModeNoLock, lockModeFromEnv); err != nil {
		t.Fatal(err)
	}
	if len(notes) != 1 || !strings.Contains(notes[0], "Encrypted, not pinned") {
		t.Fatalf("notes = %q, want the not-pinned note", notes)
	}
}

// The fingerprint hint names the innocent causes too.
func TestMydumperTLSHint_FingerprintNamesInnocentCauses(t *testing.T) {
	h := mydumperTLSHint("Error connection to database: TLS/SSL error: Fingerprint verification of server certificate failed", dumpTLS{encrypt: true})
	for _, w := range []string{"load balancer", "one server", "again"} {
		if !strings.Contains(h, w) {
			t.Errorf("hint %q lacks %q", h, w)
		}
	}
}

// SSL_CERT_FILE, which Go's own system roots honor, is the bundle mydumper
// gets too.
func TestSystemCABundle_SSLCertFileFirst(t *testing.T) {
	p := filepath.Join(t.TempDir(), "roots.pem")
	if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", p)
	if got := systemCABundle(); got != p {
		t.Errorf("systemCABundle = %q, want SSL_CERT_FILE %q", got, p)
	}
}

// The pin is the LEAF certificate's, not an intermediate's: a real TLS
// handshake with a two-level chain.
func TestCaptureLeaf_TakesTheServerCertificate(t *testing.T) {
	root, rootKey := mkCert(t, "root", nil, nil, true)
	inter, interKey := mkCert(t, "inter", root, rootKey, true)
	leaf, leafKey := mkCert(t, "leaf", inter, interKey, false)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{{
		Certificate: [][]byte{leaf.Raw, inter.Raw}, PrivateKey: leafKey}}})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.(*tls.Conn).Handshake()
			c.Close()
		}
	}()
	cfg := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test
	got := captureLeaf(cfg)
	c, err := tls.Dial("tcp", ln.Addr().String(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if tlsFingerprint(*got) != tlsFingerprint(leaf.Raw) {
		t.Fatalf("pinned %s, want the leaf's %s (intermediate is %s)", tlsFingerprint(*got), tlsFingerprint(leaf.Raw), tlsFingerprint(inter.Raw))
	}
}

func mkCert(t *testing.T, cn string, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, ca bool) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: ca, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign}
	if parent == nil {
		parent, parentKey = tmpl, k
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &k.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return c, k
}

// fakeMySQLWithoutTLS answers every connection with a MySQL handshake that
// does not offer TLS (no CLIENT_SSL capability), then closes.
func fakeMySQLWithoutTLS(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var caps uint32 = 0x1 | 0x200 | 0x2000 | 0x8000 | 0x80000 // long password, protocol 41, transactions, secure conn, plugin auth
	var p []byte
	p = append(p, 10)
	p = append(p, "8.0.0-fake\x00"...)
	p = append(p, 1, 0, 0, 0)
	p = append(p, "abcdefgh"...)
	p = append(p, 0, byte(caps), byte(caps>>8), 33, 2, 0, byte(caps>>16), byte(caps>>24), 21)
	p = append(p, make([]byte, 10)...)
	p = append(p, "ijklmnopqrst\x00"...)
	p = append(p, "mysql_native_password\x00"...)
	pkt := append([]byte{byte(len(p)), byte(len(p) >> 8), byte(len(p) >> 16), 0}, p...)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_, _ = c.Write(pkt)
			buf := make([]byte, 4096)
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, _ = c.Read(buf)
			c.Close()
		}
	}()
	return ln.Addr().String()
}

// The mandatory-TLS check refuses a server that offers no TLS, worded, and a
// DSN that allows a plaintext fallback does not turn it into one.
func TestSourceTLSPin_RefusesAServerWithoutTLS(t *testing.T) {
	addr := fakeMySQLWithoutTLS(t)
	for _, dsn := range []string{
		"u:p@tcp(" + addr + ")/",
		"u:p@tcp(" + addr + ")/?allowFallbackToPlaintext=true",
		"u:p@tcp(" + addr + ")/?tls=preferred",
	} {
		_, err := realSourceTLSPin(context.Background(), dsn, dumpTLS{encrypt: true})
		if err == nil || !strings.Contains(err.Error(), "does not offer encrypted") {
			t.Errorf("%s: err = %v, want the worded no-TLS refusal", dsn, err)
		}
	}
}
