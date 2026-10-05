package readrouter

import (
	"context"
	"crypto/tls"
	"net"
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
