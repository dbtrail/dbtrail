package readrouter

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/config"
)

// fakeSource is a MySQL-protocol endpoint for the forwarder to connect to:
// one that offers TLS (go-mysql's default server, with its own certificate)
// or one that does not. It records, per connection that finished its
// handshake, whether that connection was encrypted.
type fakeSource struct {
	addr string
	mu   sync.Mutex
	// handshakes: one entry per authenticated connection, true = over TLS.
	handshakes []bool
}

type fakeSourceHandler struct{ server.EmptyHandler }

func (fakeSourceHandler) HandleQuery(string) (*mysql.Result, error) {
	rs, err := mysql.BuildSimpleTextResultset([]string{"x"}, [][]any{{"1"}})
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}

func newFakeSource(t *testing.T, offersTLS bool) *fakeSource {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	conf := server.NewDefaultServer()
	if !offersTLS {
		conf = server.NewServer("8.0.11", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	}
	auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	if err := auth.AddUser("u", "p"); err != nil {
		t.Fatal(err)
	}
	fs := &fakeSource{addr: ln.Addr().String()}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				mc, err := server.NewCustomizedConn(c, conf, auth, fakeSourceHandler{})
				if err != nil {
					return
				}
				_, encrypted := mc.Conn.Conn.(*tls.Conn)
				fs.mu.Lock()
				fs.handshakes = append(fs.handshakes, encrypted)
				fs.mu.Unlock()
				for mc.HandleCommand() == nil {
				}
			}()
		}
	}()
	return fs
}

func (fs *fakeSource) seen() []bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]bool(nil), fs.handshakes...)
}

// The port's connection to the source decides TLS the way capture does for
// the same server (config.ConnectSSLWith, the rule behind config.ConnectSSL):
// one case per mode, against a source that offers TLS and one that does not.
func TestForwarder_tlsFollowsTheServersMode(t *testing.T) {
	cases := []struct {
		name      string
		offersTLS bool
		ssl       config.SSL
		dsnParams string
		wantOK    bool
		wantTLS   bool
		wantClear bool // the "no TLS, went in clear" callback fired
	}{
		{name: "preferred, source has TLS", offersTLS: true, ssl: config.SSL{Mode: "preferred"}, wantOK: true, wantTLS: true},
		{name: "required, source has TLS", offersTLS: true, ssl: config.SSL{Mode: "required"}, wantOK: true, wantTLS: true},
		{name: "disabled, source has TLS", offersTLS: true, ssl: config.SSL{Mode: "disabled"}, wantOK: true},
		{name: "verify-ca against an unknown certificate", offersTLS: true, ssl: config.SSL{Mode: "verify-ca"}},
		{name: "verify-identity against an unknown certificate", offersTLS: true, ssl: config.SSL{Mode: "verify-identity"}},
		{name: "preferred, source has no TLS: clear, and said so", ssl: config.SSL{Mode: "preferred"}, wantOK: true, wantClear: true},
		{name: "required, source has no TLS: refused", ssl: config.SSL{Mode: "required"}},
		{name: "verify-ca, source has no TLS: refused", ssl: config.SSL{Mode: "verify-ca"}},
		{name: "disabled, source has no TLS", ssl: config.SSL{Mode: "disabled"}, wantOK: true},
		// A tls= inside the DSN wins over the mode, as it does for capture.
		{name: "tls=false in the DSN wins over required", offersTLS: true, ssl: config.SSL{Mode: "required"}, dsnParams: "?tls=false", wantOK: true},
		{name: "tls=skip-verify in the DSN wins over disabled", offersTLS: true, ssl: config.SSL{Mode: "disabled"}, dsnParams: "?tls=skip-verify", wantOK: true, wantTLS: true},
		{name: "tls=skip-verify in the DSN, source has no TLS: no retry in clear", ssl: config.SSL{Mode: "preferred"}, dsnParams: "?tls=skip-verify"},
		// tls=preferred in the DSN IS the mode preferred, whatever the
		// server's mode says: encrypted when the source can, and the same
		// fallback, said out loud, when it cannot. That is what the driver
		// capture connects with does for such a DSN.
		{name: "tls=preferred in the DSN, source has TLS", offersTLS: true, ssl: config.SSL{Mode: "disabled"}, dsnParams: "?tls=preferred", wantOK: true, wantTLS: true},
		{name: "tls=preferred in the DSN, source has no TLS: clear, and said so", ssl: config.SSL{Mode: "required"}, dsnParams: "?tls=preferred", wantOK: true, wantClear: true},
		{name: "tls=PREFERRED, any case", ssl: config.SSL{Mode: "required"}, dsnParams: "?tls=PREFERRED", wantOK: true, wantClear: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newFakeSource(t, tc.offersTLS)
			f, err := NewForwarder("u:p@tcp("+src.addr+")/"+tc.dsnParams, tc.ssl, DefaultPolicy(), 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			var clear []error
			f.OnCleartext = func(err error) { clear = append(clear, err) }
			_, err = f.Forward(context.Background(), "SELECT 1", &BufferSink{})
			if tc.wantOK != (err == nil) {
				t.Fatalf("Forward: err = %v, want ok=%v", err, tc.wantOK)
			}
			seen := src.seen()
			if !tc.wantOK {
				if !IsLost(err) {
					t.Errorf("a refused connection must be the lost error, got %v", err)
				}
				// Never in clear: no connection got as far as logging in.
				if len(seen) != 0 {
					t.Errorf("the source saw %d authenticated connection(s) (encrypted: %v); want none", len(seen), seen)
				}
			} else if len(seen) != 1 || seen[0] != tc.wantTLS {
				t.Errorf("the source saw connections %v (true = encrypted), want one with encrypted=%v", seen, tc.wantTLS)
			}
			if tc.wantClear != (len(clear) == 1) {
				t.Errorf("the cleartext callback fired %d time(s), want fired=%v", len(clear), tc.wantClear)
			}
		})
	}
}

// A mode that is not one, or a CA file that cannot be read, is found when
// the forwarder is made, before anything dials.
func TestForwarder_tlsSettingsCheckedUpFront(t *testing.T) {
	for name, ssl := range map[string]config.SSL{
		"no mode":      {},
		"unknown mode": {Mode: "prefered"},
		"missing CA":   {Mode: "verify-ca", CA: "/nonexistent/ca.pem"},
		"cert, no key": {Mode: "required", Cert: "/x/cert.pem"},
	} {
		if _, err := NewForwarder("u:p@tcp(127.0.0.1:1)/", ssl, DefaultPolicy(), time.Second); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), "--ssl-") {
			t.Errorf("%s: the error names a command-line flag the console does not have: %v", name, err)
		}
	}
}

// A CA file that was readable when the forwarder was made and is gone when
// it connects: the error reaches a client of the port and the console's
// pages, so it names the server's setting, not a command-line flag.
func TestForwarder_tlsSettingsGoneAtConnect(t *testing.T) {
	src := newFakeSource(t, true)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, testCAPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := NewForwarder("u:p@tcp("+src.addr+")/", config.SSL{Mode: "verify-ca", CA: ca}, DefaultPolicy(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := os.Remove(ca); err != nil {
		t.Fatal(err)
	}
	_, err = f.Forward(context.Background(), "SELECT 1", &BufferSink{})
	t.Log(err)
	if !IsLost(err) || strings.Contains(err.Error(), "--ssl-") || !strings.Contains(err.Error(), "this server's TLS settings") || !strings.Contains(err.Error(), "ca.pem") {
		t.Errorf("got %v, want the lost error naming this server's TLS settings and the file, with no command-line flag", err)
	}
}

// The codes a source answers a login with when it turns the ACCOUNT away,
// MariaDB's own among them.
func TestAccountRefusedCodes(t *testing.T) {
	for _, code := range []uint16{1045, 1044, 1130, 1698, 1820, 1862, 3118, 4151, 1226, 1227} {
		_, refused := AccountRefused(mysql.NewError(code, "x"))
		want := code != 1226 && code != 1227
		if refused != want {
			t.Errorf("AccountRefused(%d) = %v, want %v", code, refused, want)
		}
	}
}

// The caller is told how the connection attempt ended, with the source's own
// error: a refused account is told apart from an unreachable source.
func TestForwarder_onConnectSaysWhy(t *testing.T) {
	src := newFakeSource(t, true)
	attempt := func(dsn string) (connectErr error, called int, forwardErr error) {
		f, err := NewForwarder(dsn, config.SSL{Mode: "preferred"}, DefaultPolicy(), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		f.connectTimeout = 2 * time.Second
		f.OnConnect = func(err error) { connectErr = err; called++ }
		_, forwardErr = f.Forward(context.Background(), "SELECT 1", &BufferSink{})
		_, _ = f.Forward(context.Background(), "SELECT 1", &BufferSink{}) // a second statement dials nothing
		return connectErr, called, forwardErr
	}
	if cerr, n, ferr := attempt("u:p@tcp(" + src.addr + ")/"); cerr != nil || n != 1 || ferr != nil {
		t.Errorf("good account: OnConnect(%v) called %d time(s), Forward err %v; want nil, once, nil", cerr, n, ferr)
	}
	cerr, n, ferr := attempt("u:wrong@tcp(" + src.addr + ")/")
	if n != 1 || !IsLost(ferr) {
		t.Fatalf("wrong password: OnConnect called %d time(s), Forward err %v", n, ferr)
	}
	if code, refused := AccountRefused(cerr); !refused || code != mysql.ER_ACCESS_DENIED_ERROR {
		t.Errorf("wrong password: AccountRefused(%v) = %d, %v; want 1045, true", cerr, code, refused)
	}
	if strings.Contains(cerr.Error(), "wrong") {
		t.Errorf("the connect error carries the password: %v", cerr)
	}
	cerr, n, _ = attempt("u:p@tcp(127.0.0.1:1)/")
	if _, refused := AccountRefused(cerr); n != 1 || cerr == nil || refused {
		t.Errorf("unreachable source: OnConnect(%v) called %d time(s), refused=%v; want an error that is not a refusal", cerr, n, refused)
	}
	if _, refused := AccountRefused(nil); refused {
		t.Error("nil is a refusal")
	}
	if _, refused := AccountRefused(mysql.NewError(mysql.ER_PARSE_ERROR, "x")); refused {
		t.Error("a syntax error is a refusal")
	}
}

// testCAPEM is a self-signed certificate, good enough to be read as a CA file.
func testCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
