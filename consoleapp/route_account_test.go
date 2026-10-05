package consoleapp

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/shim"
)

type oneRowHandler struct{ server.EmptyHandler }

func (oneRowHandler) HandleQuery(string) (*gomysql.Result, error) {
	return gomysql.NewResultReserveResultset(0), nil
}

// fakeSourceAddr serves the MySQL protocol, without TLS, with one account,
// fwd / pw.
func fakeSourceAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	// No TLS on offer, so a login that requires it must be refused.
	conf := server.NewServer("8.0.11", gomysql.DEFAULT_COLLATION_ID, gomysql.AUTH_NATIVE_PASSWORD, nil, nil)
	auth := server.NewInMemoryAuthenticationHandler(gomysql.AUTH_NATIVE_PASSWORD)
	if err := auth.AddUser("fwd", "pw"); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				mc, err := server.NewCustomizedConn(c, conf, auth, oneRowHandler{})
				if err != nil {
					return
				}
				for mc.HandleCommand() == nil {
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// TestBindReadRouterSaysWhichAccountWasRefused: a login the source refuses
// reaches the client as error 2006; the Connect page is told which of the
// server's two accounts it was and MySQL's own error, with no password, and
// the note goes when a connection logs in. bindReadRouter also reports
// whether it bound a router, which is what makes the port track the
// connection.
func TestBindReadRouterSaysWhichAccountWasRefused(t *testing.T) {
	addr := fakeSourceAddr(t)
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", FlashbackListen: "127.0.0.1:3308",
		ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	note := func() string {
		req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
		req.Header.Set("Authorization", "Bearer tok")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if strings.Contains(rec.Body.String(), "wrong-pw") {
			t.Fatalf("the page's data carries the password: %s", rec.Body.String())
		}
		var fb struct {
			Routing struct {
				Servers map[string]struct {
					AccountRefused string `json:"account_refused"`
				} `json:"servers"`
			} `json:"routing"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &fb); err != nil {
			t.Fatalf("decode %s: %v", rec.Body.String(), err)
		}
		return fb.Routing.Servers["s1"].AccountRefused
	}
	cfg := flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.DefaultPolicy(), QueryTimeout: 5 * time.Second}
	try := func(dsn string, separate bool) bool {
		h := shim.NewHandler(nil, nil)
		h.BindFreeSQL(routeTestFreeSQL{})
		t.Cleanup(h.Close)
		bound := bindReadRouter(h, srv, console.FlashbackTarget{ID: "s1", SQL: &console.SQLOnCopy{}, ForwardDSN: dsn, ForwardSeparate: separate,
			SourceSSL: config.SSL{Mode: "disabled"}}, "s1", cfg, slog.Default())
		_, _ = h.HandleQuery("DELETE FROM t") // a write is forwarded: the login happens
		return bound
	}
	if !try("fwd:wrong-pw@tcp("+addr+")/", true) {
		t.Fatal("a server that can route was not reported as bound")
	}
	got := note()
	t.Log(got)
	if !strings.Contains(got, "the forwarding account fwd") || !strings.Contains(got, "MySQL error 1045") {
		t.Errorf("the page says %q, want the forwarding account fwd and MySQL's 1045", got)
	}
	// A login clears it.
	try("fwd:pw@tcp("+addr+")/", true)
	if got := note(); got != "" {
		t.Errorf("after a connection logged in the page still says %q", got)
	}
	// The source account, on a server with no forwarding account.
	try("fwd:wrong-pw@tcp("+addr+")/", false)
	if got := note(); !strings.Contains(got, "the source account fwd") || strings.Contains(got, "the forwarding account fwd") {
		t.Errorf("the page says %q, want the source account named", got)
	}
	// A source that cannot be reached at all is not an account refused.
	try("fwd:pw@tcp("+addr+")/", true)
	try("fwd:pw@tcp(127.0.0.1:1)/", true)
	if got := note(); got != "" {
		t.Errorf("an unreachable source is reported as a refused account: %q", got)
	}
	// Not bound: routing off, and a server with nothing to forward to.
	h := shim.NewHandler(nil, nil)
	if bindReadRouter(h, srv, console.FlashbackTarget{ID: "s1", SQL: &console.SQLOnCopy{}, ForwardDSN: "fwd:pw@tcp(" + addr + ")/"}, "s1", flashbackConfig{}, slog.Default()) {
		t.Error("routing off reported a bound router")
	}
	if bindReadRouter(h, srv, console.FlashbackTarget{ID: "s1", SQL: &console.SQLOnCopy{}}, "s1", cfg, slog.Default()) {
		t.Error("a server with no source reported a bound router")
	}
}

// TestProbeRouteAccount: Test connection's login for a forwarding account
// goes through the port's own client and answers with the source's words,
// not with the error 2006 a client of the port would get; and the daemon
// hands that probe to the console.
func TestProbeRouteAccount(t *testing.T) {
	addr := fakeSourceAddr(t)
	off := config.SSL{Mode: "disabled"}
	ctx := context.Background()
	if err := probeRouteAccount(ctx, "fwd:pw@tcp("+addr+")/", off, 5*time.Second); err != nil {
		t.Errorf("the right password: %v", err)
	}
	err := probeRouteAccount(ctx, "fwd:wrong-pw@tcp("+addr+")/", off, 5*time.Second)
	t.Logf("wrong password: %v", err)
	if err == nil || !strings.Contains(err.Error(), "Access denied") || strings.Contains(err.Error(), "gone away") || strings.Contains(err.Error(), "wrong-pw") {
		t.Errorf("wrong password: %v, want the source's Access denied, without the port's 2006 and without the password", err)
	}
	if _, refused := readrouter.AccountRefused(err); !refused {
		t.Errorf("the error is not the source's own refusal: %v", err)
	}
	if err := probeRouteAccount(ctx, "fwd:pw@tcp(127.0.0.1:1)/", off, 5*time.Second); err == nil {
		t.Error("an unreachable source logged in")
	}
	// The source refuses TLS it does not offer: the server's TLS mode is
	// what the probe connects with.
	if err := probeRouteAccount(ctx, "fwd:pw@tcp("+addr+")/", config.SSL{Mode: "required"}, 5*time.Second); err == nil {
		t.Error("ssl_mode required against a source with no TLS logged in: the probe does not use the server's TLS settings")
	}
	if err := probeRouteAccount(ctx, "fwd:pw@tcp("+addr+")/", config.SSL{Mode: "no-such-mode"}, 5*time.Second); err == nil {
		t.Error("an unusable TLS mode logged in")
	}

	cfg, err := upConsoleConfig(nil, "u:p@tcp(127.0.0.1:1)/idx", consoleOpts{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RouteAccountProbe == nil {
		t.Fatal("the daemon does not give the console its login probe: Test connection would say nothing about the forwarding account")
	}
	if err := cfg.RouteAccountProbe(ctx, "fwd:wrong-pw@tcp("+addr+")/", off, 5*time.Second); err == nil {
		t.Error("the probe the console got accepts a wrong password")
	}
}
