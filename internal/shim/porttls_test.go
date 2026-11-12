package shim

import (
	"bytes"
	"crypto/tls"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2636: the MySQL port serves the operator's certificate to the clients that
// ask for TLS. Without one it still offers TLS, with a certificate go-mysql
// makes up at start, which no client can verify.

func TestLoadPortTLS(t *testing.T) {
	rsaCert := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	ecCert := testutil.WriteTestCert(t, testutil.TestCertOptions{ECDSA: true})
	other := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	dir := t.TempDir()

	combined := filepath.Join(dir, "combined.pem")
	writeConcat(t, combined, rsaCert.CertFile, rsaCert.KeyFile)
	notPEM := filepath.Join(dir, "not.pem")
	if err := os.WriteFile(notPEM, []byte("this is not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing.pem")

	ok := []struct{ name, cert, key string }{
		{"an RSA certificate and its key", rsaCert.CertFile, rsaCert.KeyFile},
		{"an ECDSA certificate and its key", ecCert.CertFile, ecCert.KeyFile},
		{"one file holding both, given twice", combined, combined},
	}
	for _, c := range ok {
		t.Run(c.name, func(t *testing.T) {
			p, err := LoadPortTLS(c.cert, c.key)
			if err != nil {
				t.Fatalf("LoadPortTLS: %v", err)
			}
			if p.Config == nil || len(p.Config.Certificates) != 1 {
				t.Fatalf("want one certificate in the config, got %+v", p.Config)
			}
			if p.Config.MinVersion != tls.VersionTLS12 {
				t.Errorf("MinVersion = %x, want TLS 1.2", p.Config.MinVersion)
			}
			if p.CertFile != c.cert || p.KeyFile != c.key {
				t.Errorf("paths = %q, %q", p.CertFile, p.KeyFile)
			}
			if p.NotAfter.IsZero() || p.NotBefore.IsZero() || !p.NotAfter.After(p.NotBefore) {
				t.Errorf("validity %v .. %v", p.NotBefore, p.NotAfter)
			}
		})
	}

	bad := []struct {
		name, cert, key string
		want            []string // every one of these is in the error
	}{
		{"no certificate path", "", rsaCert.KeyFile, []string{"both a certificate file and a key file"}},
		{"no key path", rsaCert.CertFile, "", []string{"both a certificate file and a key file"}},
		{"a certificate file that does not exist", missing, rsaCert.KeyFile, []string{missing}},
		{"a key file that does not exist", rsaCert.CertFile, missing, []string{missing}},
		{"the key of another certificate", rsaCert.CertFile, other.KeyFile, []string{rsaCert.CertFile, other.KeyFile, "does not match"}},
		{"an ECDSA key for an RSA certificate", rsaCert.CertFile, ecCert.KeyFile, []string{ecCert.KeyFile}},
		{"a certificate file that is not PEM", notPEM, rsaCert.KeyFile, []string{notPEM}},
		{"a key file that is not PEM", rsaCert.CertFile, notPEM, []string{notPEM}},
		{"the two files swapped", rsaCert.KeyFile, rsaCert.CertFile, []string{rsaCert.KeyFile}},
	}
	keyBytes := keyMaterial(t, rsaCert.KeyFile, other.KeyFile, ecCert.KeyFile)
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			p, err := LoadPortTLS(c.cert, c.key)
			if err == nil {
				t.Fatalf("LoadPortTLS accepted it: %+v", p)
			}
			for _, w := range c.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not say %q", err, w)
				}
			}
			for _, k := range keyBytes {
				if strings.Contains(err.Error(), k) {
					t.Errorf("error carries the private key's content: %q", err)
				}
			}
		})
	}
}

func TestPortTLSValidityProblem(t *testing.T) {
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	p, err := LoadPortTLS(c.CertFile, c.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.ValidityProblem(time.Now()); got != "" {
		t.Errorf("a valid certificate: %q", got)
	}
	expired := p.ValidityProblem(p.NotAfter.Add(time.Minute))
	for _, w := range []string{"expired on", c.CertFile, "VERIFY_CA", "restarted"} {
		if !strings.Contains(expired, w) {
			t.Errorf("expired: %q does not say %q", expired, w)
		}
	}
	early := p.ValidityProblem(p.NotBefore.Add(-time.Minute))
	for _, w := range []string{"not valid before", c.CertFile, "clock"} {
		if !strings.Contains(early, w) {
			t.Errorf("not yet valid: %q does not say %q", early, w)
		}
	}
	var none *PortTLS
	if got := none.ValidityProblem(time.Now()); got != "" {
		t.Errorf("no certificate: %q", got)
	}
	t.Logf("expired: %s", expired)
	t.Logf("not yet valid: %s", early)
}

// The port with a certificate set, through real MySQL handshakes: a client
// that trusts only the test CA and checks the name connects (so the port
// served THAT certificate, not the made-up one), and a client that asks for
// no TLS still connects, in clear text. Both auth methods, both key types:
// caching_sha2_password without TLS decrypts the password with an RSA key,
// which an ECDSA certificate does not have.
func TestPortTLS_wire(t *testing.T) {
	for _, method := range []string{"", mysql.AUTH_CACHING_SHA2_PASSWORD} {
		for _, ec := range []bool{false, true} {
			name := "native"
			if method != "" {
				name = method
			}
			if ec {
				name += "/ecdsa"
			} else {
				name += "/rsa"
			}
			t.Run(name, func(t *testing.T) {
				c := testutil.WriteTestCert(t, testutil.TestCertOptions{ECDSA: ec})
				p, err := LoadPortTLS(c.CertFile, c.KeyFile)
				if err != nil {
					t.Fatal(err)
				}
				srv, err := NewMySQLServerWithTLS(method, p.Config)
				if err != nil {
					t.Fatal(err)
				}

				port := startTLSTestPort(t, srv, method)
				conn, err := client.Connect(port.addr, "u", "pw", "", withTLS(c.ClientConfig()))
				if err != nil {
					t.Fatalf("a client that verifies the certificate: %v", err)
				}
				queryOne(t, conn)
				if !port.sawTLS(t) {
					t.Error("the verified connection reached the server in clear text")
				}

				port = startTLSTestPort(t, srv, method)
				conn, err = client.Connect(port.addr, "u", "pw", "")
				if err != nil {
					t.Fatalf("a client that asks for no TLS: %v", err)
				}
				queryOne(t, conn)
				if port.sawTLS(t) {
					t.Error("a client that asked for no TLS got it")
				}
			})
		}
	}
}

// What the port does today with no certificate set, pinned: it offers TLS
// with a made-up certificate, so a client that encrypts without verifying
// connects (what --ssl-mode=REQUIRED does), and a client that verifies
// cannot.
func TestPortTLS_wireWithoutCertificate(t *testing.T) {
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	for _, method := range []string{"", mysql.AUTH_CACHING_SHA2_PASSWORD} {
		t.Run("method="+method, func(t *testing.T) {
			srv, err := NewMySQLServer(method)
			if err != nil {
				t.Fatal(err)
			}
			port := startTLSTestPort(t, srv, method)
			conn, err := client.Connect(port.addr, "u", "pw", "", withTLS(&tls.Config{InsecureSkipVerify: true}))
			if err != nil {
				t.Fatalf("a client that encrypts without verifying: %v", err)
			}
			queryOne(t, conn)
			if !port.sawTLS(t) {
				t.Error("the encrypted connection reached the server in clear text")
			}

			port = startTLSTestPort(t, srv, method)
			if conn, err := client.Connect(port.addr, "u", "pw", "", withTLS(c.ClientConfig())); err == nil {
				conn.Close()
				t.Fatal("a client that verifies connected to a port with a made-up certificate")
			} else if !strings.Contains(err.Error(), "certificate") {
				t.Errorf("the refusal is not about the certificate: %v", err)
			}
		})
	}
}

// The same with the mysql client itself, when this machine has one that
// takes --ssl-mode (MySQL's, 5.7 and later; MariaDB's does not).
func TestPortTLS_mysqlClient(t *testing.T) {
	bin := mysqlClientWithSSLMode(t)
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	p, err := LoadPortTLS(c.CertFile, c.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	withCert, err := NewMySQLServerWithTLS("", p.Config)
	if err != nil {
		t.Fatal(err)
	}
	without, err := NewMySQLServer("")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		srv     *server.Server
		args    []string
		ok, tls bool
	}{
		{"certificate set, the client verifies it and its name", withCert, []string{"--ssl-mode=VERIFY_IDENTITY", "--ssl-ca=" + c.CAFile}, true, true},
		{"certificate set, the client asks for no TLS", withCert, []string{"--ssl-mode=DISABLED"}, true, false},
		{"certificate set, the client's default (PREFERRED)", withCert, nil, true, true},
		{"no certificate, the client requires TLS", without, []string{"--ssl-mode=REQUIRED"}, true, true},
		{"no certificate, the client verifies", without, []string{"--ssl-mode=VERIFY_CA", "--ssl-ca=" + c.CAFile}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			port := startTLSTestPort(t, tc.srv, "")
			host, portNum, _ := net.SplitHostPort(port.addr)
			args := append([]string{"--no-defaults", "--protocol=TCP", "-h", host, "-P", portNum, "-u", "u", "-ppw", "-N", "-e", "SELECT 1"}, tc.args...)
			out, err := exec.Command(bin, args...).CombinedOutput()
			if tc.ok != (err == nil) {
				t.Fatalf("mysql %s: err=%v, want ok=%v\n%s", strings.Join(tc.args, " "), err, tc.ok, out)
			}
			if tc.ok && port.sawTLS(t) != tc.tls {
				t.Errorf("TLS on the server side = %v, want %v", !tc.tls, tc.tls)
			}
		})
	}
}

// tlsTestPort is a port with one MySQL user, u / pw, that answers every
// query with one row, and says for each connection whether it was TLS.
type tlsTestPort struct {
	addr string
	tls  chan bool
}

func startTLSTestPort(t *testing.T, srv *server.Server, method string) tlsTestPort {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	if method == "" {
		method = mysql.AUTH_NATIVE_PASSWORD
	}
	p := tlsTestPort{addr: ln.Addr().String(), tls: make(chan bool, 4)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				auth := server.NewInMemoryAuthenticationHandler(method)
				if err := auth.AddUser("u", "pw"); err != nil {
					return
				}
				sc, err := NewConn(c, srv, auth, oneRowHandler{})
				if err != nil {
					return
				}
				_, isTLS := sc.Conn.Conn.(*tls.Conn)
				p.tls <- isTLS
				for !sc.Closed() {
					if err := sc.HandleCommand(); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return p
}

// sawTLS is whether the connection the server let in last was TLS.
func (p tlsTestPort) sawTLS(t *testing.T) bool {
	t.Helper()
	select {
	case v := <-p.tls:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("the server let no connection in")
		return false
	}
}

type oneRowHandler struct{ server.EmptyHandler }

func (oneRowHandler) HandleQuery(string) (*mysql.Result, error) {
	rs, err := mysql.BuildSimpleTextResultset([]string{"v"}, [][]any{{int64(1)}})
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}

func withTLS(cfg *tls.Config) client.Option {
	return func(c *client.Conn) error {
		c.SetTLSConfig(cfg)
		return nil
	}
}

func queryOne(t *testing.T, conn *client.Conn) {
	t.Helper()
	defer conn.Close()
	r, err := conn.Execute("SELECT 1")
	if err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	defer r.Close()
	if n := r.RowNumber(); n != 1 {
		t.Fatalf("SELECT 1 returned %d rows", n)
	}
}

// mysqlClientWithSSLMode is the path of a mysql client that takes
// --ssl-mode, or skips the test.
func mysqlClientWithSSLMode(t *testing.T) string {
	t.Helper()
	bin, err := exec.LookPath("mysql")
	if err != nil {
		t.Skip("no mysql client on this machine")
	}
	out, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil || bytes.Contains(bytes.ToLower(out), []byte("mariadb")) {
		t.Skipf("the mysql client here does not take --ssl-mode: %s", out)
	}
	return bin
}

func writeConcat(t *testing.T, dst string, srcs ...string) {
	t.Helper()
	var b []byte
	for _, s := range srcs {
		data, err := os.ReadFile(s)
		if err != nil {
			t.Fatal(err)
		}
		b = append(b, data...)
	}
	if err := os.WriteFile(dst, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// keyMaterial is the PEM header and two runs of each key file's base64 body:
// an error that carried the key would carry one of them.
func keyMaterial(t *testing.T, files ...string) []string {
	t.Helper()
	out := []string{"-----BEGIN"}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(data), "\n")
		if len(lines) < 3 {
			t.Fatalf("%s: not a PEM key", f)
		}
		out = append(out, lines[1][:32], lines[2][:32])
	}
	return out
}
