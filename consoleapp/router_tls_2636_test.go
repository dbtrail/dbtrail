package consoleapp

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2636: the router's --tls-cert / --tls-key (BINTRAIL_ROUTER_TLS_CERT /
// _KEY) are the certificate its MySQL port offers to clients that ask for
// TLS.

func TestRouterSettings_tls(t *testing.T) {
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	other := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	missing := filepath.Join(t.TempDir(), "missing.pem")

	t.Run("none set", func(t *testing.T) {
		clearRouterEnv(t)
		st, err := routerSettingsFrom(routerTestCmd(t))
		if err != nil {
			t.Fatal(err)
		}
		if st.TLS != nil {
			t.Fatalf("TLS = %+v with nothing set", st.TLS)
		}
		if routerFlashbackConfig(st).TLS != nil {
			t.Fatal("the port got a certificate nobody set")
		}
	})
	t.Run("flags", func(t *testing.T) {
		clearRouterEnv(t)
		st, err := routerSettingsFrom(routerTestCmd(t, "--tls-cert", c.CertFile, "--tls-key", c.KeyFile))
		if err != nil {
			t.Fatal(err)
		}
		if st.TLS == nil || st.TLS.CertFile != c.CertFile || st.TLS.KeyFile != c.KeyFile {
			t.Fatalf("TLS = %+v", st.TLS)
		}
		if routerFlashbackConfig(st).TLS != st.TLS {
			t.Fatal("the port does not get the certificate the router loaded")
		}
	})
	t.Run("environment, with spaces around the paths", func(t *testing.T) {
		clearRouterEnv(t)
		t.Setenv("BINTRAIL_ROUTER_TLS_CERT", " "+c.CertFile+" ")
		t.Setenv("BINTRAIL_ROUTER_TLS_KEY", c.KeyFile+"\n")
		st, err := routerSettingsFrom(routerTestCmd(t))
		if err != nil {
			t.Fatal(err)
		}
		if st.TLS == nil || st.TLS.CertFile != c.CertFile {
			t.Fatalf("TLS = %+v", st.TLS)
		}
	})
	t.Run("a flag wins over the variable", func(t *testing.T) {
		clearRouterEnv(t)
		t.Setenv("BINTRAIL_ROUTER_TLS_CERT", missing)
		t.Setenv("BINTRAIL_ROUTER_TLS_KEY", missing)
		st, err := routerSettingsFrom(routerTestCmd(t, "--tls-cert", c.CertFile, "--tls-key", c.KeyFile))
		if err != nil {
			t.Fatal(err)
		}
		if st.TLS == nil || st.TLS.CertFile != c.CertFile {
			t.Fatalf("TLS = %+v", st.TLS)
		}
	})

	refusals := []struct {
		name  string
		env   map[string]string
		flags []string
		want  []string
	}{
		{"only the certificate, by flag", nil, []string{"--tls-cert", c.CertFile},
			[]string{"--tls-cert is set", "--tls-key (BINTRAIL_ROUTER_TLS_KEY) is not", "needs both"}},
		{"only the key, by variable", map[string]string{"BINTRAIL_ROUTER_TLS_KEY": c.KeyFile}, nil,
			[]string{"BINTRAIL_ROUTER_TLS_KEY is set", "--tls-cert (BINTRAIL_ROUTER_TLS_CERT) is not"}},
		{"a variable that is only spaces", map[string]string{"BINTRAIL_ROUTER_TLS_CERT": "  ", "BINTRAIL_ROUTER_TLS_KEY": c.KeyFile}, nil,
			[]string{"BINTRAIL_ROUTER_TLS_CERT is only spaces"}},
		{"a certificate file that does not exist", nil, []string{"--tls-cert", missing, "--tls-key", c.KeyFile},
			[]string{"--tls-cert", "--tls-key", missing, "cannot be used"}},
		{"the key of another certificate", map[string]string{"BINTRAIL_ROUTER_TLS_CERT": c.CertFile, "BINTRAIL_ROUTER_TLS_KEY": other.KeyFile}, nil,
			[]string{"BINTRAIL_ROUTER_TLS_CERT", "BINTRAIL_ROUTER_TLS_KEY", other.KeyFile, "does not match"}},
	}
	for _, r := range refusals {
		t.Run(r.name, func(t *testing.T) {
			clearRouterEnv(t)
			for k, v := range r.env {
				t.Setenv(k, v)
			}
			_, err := routerSettingsFrom(routerTestCmd(t, r.flags...))
			if err == nil {
				t.Fatal("the router would start")
			}
			for _, w := range r.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("%q does not say %q", err, w)
				}
			}
			t.Logf("refused: %v", err)
		})
	}
}

// The startup line, from real certificates: what a client gets, and the
// warning for a certificate whose dates are wrong.
func TestRouterTLSLine(t *testing.T) {
	none := routerTLSLine(nil, time.Time{})
	for _, w := range []string{"no certificate is set", "--tls-cert", "BINTRAIL_ROUTER_TLS_CERT", "cannot verify", "clear text"} {
		if !strings.Contains(none, w) {
			t.Errorf("no certificate: %q does not say %q", none, w)
		}
	}
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	p, err := shim.LoadPortTLS(c.CertFile, c.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	set := routerTLSLine(p, time.Time{})
	for _, w := range []string{c.CertFile, p.NotAfter.UTC().Format("2006-01-02"), "VERIFY_IDENTITY", "clear text", "--ssl-mode=REQUIRED", "restarts"} {
		if !strings.Contains(set, w) {
			t.Errorf("certificate set: %q does not say %q", set, w)
		}
	}
	if strings.Contains(set, "WARNING") {
		t.Errorf("a valid certificate warns: %q", set)
	}
	expired := routerTLSLine(p, p.NotAfter.Add(time.Hour))
	if !strings.Contains(expired, "Router: WARNING: the certificate "+c.CertFile+" expired on") {
		t.Errorf("expired: %q", expired)
	}
	// In the router's startup lines.
	all := routerStartupLines(routerStartup{Listen: "127.0.0.1:3309", TLS: p})
	if !strings.Contains(all, set) {
		t.Errorf("the startup lines do not carry the TLS line:\n%s", all)
	}
	t.Logf("\n%s%s%s", none, set, expired)
}

// The router's port, started from its settings, serves the certificate they
// name: a client that trusts only the test CA and checks the name logs in,
// and a client that asks for no TLS logs in too. Without the settings, the
// client that verifies is refused.
func TestRouterPort_servesTheCertificateSet(t *testing.T) {
	c := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	for _, set := range []bool{true, false} {
		name := "certificate set"
		if !set {
			name = "no certificate"
		}
		t.Run(name, func(t *testing.T) {
			clearRouterEnv(t)
			f := newRouterFiles(t)
			if _, err := f.owner.Add(console.ServerEntry{Name: "prod", DSN: "u:p@tcp(127.0.0.1:1)/idx_prod"}); err != nil {
				t.Fatal(err)
			}
			f.savePortFile(t, true, "port-pw")
			var flags []string
			if set {
				flags = []string{"--tls-cert", c.CertFile, "--tls-key", c.KeyFile}
			}
			st, err := routerSettingsFrom(routerTestCmd(t, flags...))
			if err != nil {
				t.Fatal(err)
			}
			srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(runner.Close)

			ctx, cancel := context.WithCancel(context.Background())
			go srv.FollowFiles(ctx, 20*time.Millisecond)
			addr := freeAddr(t)
			rt := &routerPort{srv: srv, addr: addr, cfg: routerFlashbackConfig(st), poll: 20 * time.Millisecond, out: &lockedBuffer{}}
			done := make(chan error, 1)
			go func() { done <- rt.serve(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(15 * time.Second):
					t.Error("the router did not stop")
				}
			})
			waitUntil(t, "the port to open", func() bool { return rt.listening() != "" })

			verified, err := client.Connect(addr, "prod", "port-pw", "", func(cc *client.Conn) error {
				cc.SetTLSConfig(c.ClientConfig())
				return nil
			})
			if set {
				if err != nil {
					t.Fatalf("a client that verifies the certificate set: %v", err)
				}
				if _, isTLS := verified.Conn.Conn.(*tls.Conn); !isTLS {
					t.Error("the verified connection is not TLS")
				}
				verified.Close()
			} else if err == nil {
				verified.Close()
				t.Fatal("a client that verifies logged in to a port with a made-up certificate")
			}

			plain, err := client.Connect(addr, "prod", "port-pw", "")
			if err != nil {
				t.Fatalf("a client that asks for no TLS: %v", err)
			}
			if _, isTLS := plain.Conn.Conn.(*tls.Conn); isTLS {
				t.Error("a client that asked for no TLS got it")
			}
			plain.Close()
		})
	}
}

func TestClearRouterEnv_clearsTheTLSVariables(t *testing.T) {
	// clearRouterEnv must clear the TLS variables, or a machine that has them
	// set would change what every router test starts with.
	t.Setenv("BINTRAIL_ROUTER_TLS_CERT", "/nonexistent/cert.pem")
	t.Setenv("BINTRAIL_ROUTER_TLS_KEY", "/nonexistent/key.pem")
	clearRouterEnv(t)
	for _, k := range []string{"BINTRAIL_ROUTER_TLS_CERT", "BINTRAIL_ROUTER_TLS_KEY"} {
		if v := os.Getenv(k); v != "" {
			t.Errorf("clearRouterEnv leaves %s=%s", k, v)
		}
	}
}
