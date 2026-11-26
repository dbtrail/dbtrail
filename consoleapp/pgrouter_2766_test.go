package consoleapp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2766: pg-router serves TLS with the operator's certificate (--tls-cert,
// --tls-key, loaded as the MySQL port loads its own) or one made up at start,
// and logs in with SCRAM-SHA-256.

func TestPGRouterSettings_tls(t *testing.T) {
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	other := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	missing := filepath.Join(t.TempDir(), "missing.pem")
	unreadable := filepath.Join(t.TempDir(), "unreadable.pem")
	data, err := os.ReadFile(c.CertFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unreadable, data, 0o000); err != nil {
		t.Fatal(err)
	}

	t.Run("none set", func(t *testing.T) {
		clearPGRouterEnv(t)
		st, err := pgRouterSettingsFrom(pgRouterTestCmd(t))
		if err != nil {
			t.Fatal(err)
		}
		if st.TLS != nil {
			t.Fatalf("TLS = %+v with nothing set", st.TLS)
		}
	})
	t.Run("flags", func(t *testing.T) {
		clearPGRouterEnv(t)
		st, err := pgRouterSettingsFrom(pgRouterTestCmd(t, "--tls-cert", c.CertFile, "--tls-key", c.KeyFile))
		if err != nil {
			t.Fatal(err)
		}
		if st.TLS == nil || st.TLS.CertFile != c.CertFile || st.TLS.KeyFile != c.KeyFile {
			t.Fatalf("TLS = %+v", st.TLS)
		}
	})
	t.Run("environment, with spaces around the paths, and a flag over it", func(t *testing.T) {
		clearPGRouterEnv(t)
		t.Setenv("BINTRAIL_PG_ROUTER_TLS_CERT", " "+other.CertFile+" ")
		t.Setenv("BINTRAIL_PG_ROUTER_TLS_KEY", other.KeyFile+"\n")
		st, err := pgRouterSettingsFrom(pgRouterTestCmd(t))
		if err != nil {
			t.Fatal(err)
		}
		if st.TLS == nil || st.TLS.CertFile != other.CertFile {
			t.Fatalf("TLS = %+v", st.TLS)
		}
		st, err = pgRouterSettingsFrom(pgRouterTestCmd(t, "--tls-cert", c.CertFile, "--tls-key", c.KeyFile))
		if err != nil {
			t.Fatal(err)
		}
		if st.TLS == nil || st.TLS.CertFile != c.CertFile {
			t.Fatalf("the flags did not win over the environment: %+v", st.TLS)
		}
	})
	for _, tc := range []struct {
		name string
		args []string
		env  [2]string
		want []string
	}{
		{"a certificate without its key", []string{"--tls-cert", c.CertFile}, [2]string{}, []string{"--tls-cert is set", "--tls-key (BINTRAIL_PG_ROUTER_TLS_KEY) is not", "the PostgreSQL port needs both"}},
		{"a key without its certificate", nil, [2]string{"BINTRAIL_PG_ROUTER_TLS_KEY", c.KeyFile}, []string{"BINTRAIL_PG_ROUTER_TLS_KEY is set", "the PostgreSQL port needs both"}},
		{"a certificate that is not there", []string{"--tls-cert", missing, "--tls-key", c.KeyFile}, [2]string{}, []string{"the PostgreSQL port's certificate", "cannot be used", missing}},
		{"a key of another certificate", []string{"--tls-cert", c.CertFile, "--tls-key", other.KeyFile}, [2]string{}, []string{"cannot be used"}},
		{"only spaces", []string{"--tls-cert", "  ", "--tls-key", c.KeyFile}, [2]string{}, []string{"--tls-cert is only spaces"}},
		// A path that cannot be read as a file, for any user, root included.
		{"a folder where the certificate should be", []string{"--tls-cert", filepath.Dir(missing), "--tls-key", c.KeyFile}, [2]string{}, []string{"cannot be used", "is a directory"}},
		{"a file that is not a certificate", []string{"--tls-cert", c.KeyFile, "--tls-key", c.KeyFile}, [2]string{}, []string{"cannot be used"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearPGRouterEnv(t)
			if tc.env[0] != "" {
				t.Setenv(tc.env[0], tc.env[1])
			}
			_, err := pgRouterSettingsFrom(pgRouterTestCmd(t, tc.args...))
			if err == nil {
				t.Fatal("started with a certificate it cannot serve")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("%v, want it to say %q", err, w)
				}
			}
		})
	}
	t.Run("a file it cannot read", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root reads a file of mode 000")
		}
		clearPGRouterEnv(t)
		_, err := pgRouterSettingsFrom(pgRouterTestCmd(t, "--tls-cert", unreadable, "--tls-key", c.KeyFile))
		if err == nil || !strings.Contains(err.Error(), "permission denied") || !strings.Contains(err.Error(), unreadable) {
			t.Fatalf("err = %v, want it to name the file and say it cannot be read", err)
		}
	})
	t.Run("the MySQL port's refusals read as they did", func(t *testing.T) {
		_, err := routerPortTLS("--tls-cert", c.CertFile, "--tls-key", "")
		if err == nil || !strings.Contains(err.Error(), "the MySQL port needs both") {
			t.Fatalf("err = %v", err)
		}
		_, err = routerPortTLS("--tls-cert", missing, "--tls-key", c.KeyFile)
		if err == nil || !strings.Contains(err.Error(), "the MySQL port's certificate (--tls-cert, --tls-key) cannot be used") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestPGRouterTLSLines(t *testing.T) {
	now := time.Now()
	out := pgRouterTLSLines(nil, now)
	for _, want := range []string{"SCRAM-SHA-256", "never crosses the network", "no certificate is set", "BINTRAIL_PG_ROUTER_TLS_CERT", "made up at start", "cannot verify", "channel_binding=require", "answered in clear text"} {
		if !strings.Contains(out, want) {
			t.Errorf("without a certificate the lines do not say %q:\n%s", want, out)
		}
	}
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	pt, err := shim.LoadPortTLS(c.CertFile, c.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	out = pgRouterTLSLines(pt, now)
	for _, want := range []string{c.CertFile, pt.NotAfter.UTC().Format("2006-01-02"), "sslmode=verify-ca or verify-full", "sslrootcert", "sslmode=require or stricter", "after this restarts"} {
		if !strings.Contains(out, want) {
			t.Errorf("with a certificate the lines do not say %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "WARNING") || strings.Contains(out, "--ssl-mode") {
		t.Errorf("a warning, or the MySQL client's words, with a good certificate:\n%s", out)
	}
	for _, tc := range []struct {
		at   time.Time
		want string
	}{
		{pt.NotAfter.Add(time.Hour), "expired on"},
		{pt.NotBefore.Add(-time.Hour), "is not valid before"},
	} {
		out := pgRouterTLSLines(pt, tc.at)
		if !strings.Contains(out, "PostgreSQL router: WARNING: the certificate "+c.CertFile+" "+tc.want) || !strings.Contains(out, "(sslmode=verify-ca or verify-full)") {
			t.Errorf("at %s the lines do not warn %q in the PostgreSQL client's words:\n%s", tc.at, tc.want, out)
		}
	}
	for _, line := range strings.Split(strings.TrimRight(pgRouterTLSLines(pt, pt.NotAfter.Add(time.Hour)), "\n"), "\n") {
		if !strings.HasPrefix(line, "PostgreSQL router: ") {
			t.Errorf("a line that does not say who speaks: %q", line)
		}
	}
}

func TestPGRouterAddressLine(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:5433", "[::1]:5433", "localhost:5433", "127.0.0.2:5433"} {
		if got := pgRouterAddressLine(addr, false); got != "" {
			t.Errorf("%s: %q, want nothing for an address of this machine", addr, got)
		}
	}
	for _, addr := range []string{"0.0.0.0:5433", ":5433", "[::]:5433", "10.0.0.5:5433", "db.example.com:5433"} {
		if got := pgRouterAddressLine(addr, false); !strings.Contains(got, "other machines") || !strings.Contains(got, "firewall") {
			t.Errorf("%s: %q, want it to say other machines reach it", addr, got)
		}
		if got := pgRouterAddressLine(addr, true); !strings.Contains(got, "container publishes it") {
			t.Errorf("%s in a container: %q", addr, got)
		}
	}
}

// pgTLSLoginCode is pgLoginCode with the client's own TLS parameters.
func pgTLSLoginCode(t *testing.T, addr, params, user, password, database string) (string, bool) {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s %s", host, port, params))
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Password, cfg.Database = user, password, database
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err == nil {
		_, encrypted := conn.Conn().(*tls.Conn)
		_ = conn.Close(ctx)
		return "", encrypted
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("%s / %s / %s: %v, want a PostgreSQL error", user, password, database, err)
	}
	return pe.Code, false
}

// The port's login over TLS, verified and bound to the certificate: the same
// answers as in clear text, each one after the handshake.
func TestPGRouter_loginOverTLS(t *testing.T) {
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	pt, err := shim.LoadPortTLS(c.CertFile, c.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	f := newRouterFiles(t)
	prod, err := addServedPG(t, f.owner, console.ServerEntry{Name: "prod", DSN: "u:p@tcp(127.0.0.1:1)/idx_prod"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.SetPortLogin(prod.ID, "app1", "pw-one"); err != nil {
		t.Fatal(err)
	}
	f.savePortFile(t, true, "port-pw")
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- servePGPortWithin(ctx, srv, ln, 0, newPGPortResultBudget(), pt.Config) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the PostgreSQL port did not stop with its context")
		}
	})
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	byName := net.JoinHostPort("localhost", port)
	verified := "sslmode=verify-full channel_binding=require sslrootcert=" + c.CAFile
	for _, tc := range []struct {
		params, user, pw, db, want string
		encrypted                  bool
		why                        string
	}{
		{verified, "anyone", "port-pw", "prod", "", true, "the port's password, verified and bound"},
		{verified, "app1", "pw-one", "prod", "", true, "an application login's own password"},
		{"sslmode=disable", "anyone", "port-pw", "prod", "", false, "in clear text, the password still never on the wire"},
		{"sslmode=require", "anyone", "port-pw", "prod", "", true, "required, not verified"},
		{verified, "anyone", "wrong", "prod", "28P01", false, "a wrong password"},
		{verified, "app1", "wrong", "prod", "28P01", false, "a login with a wrong password"},
		{verified, "anyone", "wrong", "nowhere", "28P01", false, "a wrong password says nothing about the server"},
		{verified, "anyone", "port-pw", "nowhere", "3D000", false, "a server that does not exist"},
	} {
		got, encrypted := pgTLSLoginCode(t, byName, tc.params, tc.user, tc.pw, tc.db)
		if got != tc.want || encrypted != tc.encrypted {
			t.Errorf("%s (%s): SQLSTATE %q encrypted %v, want %q %v", tc.why, tc.params, got, encrypted, tc.want, tc.encrypted)
		}
	}
}

// A certificate the port cannot bind SCRAM to says so at start, and one it
// can does not.
func TestPGRouterTLSLines_channelBinding(t *testing.T) {
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{ECDSA: true})
	pt, err := shim.LoadPortTLS(c.CertFile, c.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	if out := pgRouterTLSLines(pt, time.Now()); strings.Contains(out, "channel binding") {
		t.Errorf("an ECDSA certificate is said to lose channel binding:\n%s", out)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	ed := &shim.PortTLS{CertFile: "ed.pem", NotBefore: tmpl.NotBefore, NotAfter: tmpl.NotAfter,
		Config: &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv}}}}
	if out := pgRouterTLSLines(ed, time.Now()); !strings.Contains(out, "WARNING: channel binding (SCRAM-SHA-256-PLUS) is not offered") {
		t.Errorf("an Ed25519 certificate does not say it loses channel binding:\n%s", out)
	}
}
