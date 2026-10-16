package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/go-sql-driver/mysql"
)

// failureEnv is the environment and flag state newStartupFailure reads, so
// each case states it in full instead of leaking globals between cases.
type failureEnv struct {
	reached, listening bool
	env                map[string]string
	changed            map[string]bool
	listen             string // the --console-listen value (flag default unless set)
	tlsCert            string
	allowSetup         bool
}

func (fe failureEnv) build(err error) *startupFailure {
	if fe.listen == "" {
		fe.listen = "127.0.0.1:8090"
	}
	return newStartupFailure(err, startupState{
		reached:    fe.reached,
		listening:  fe.listening,
		getenv:     func(k string) string { return fe.env[k] },
		changed:    func(f string) bool { return fe.changed[f] },
		listen:     fe.listen,
		tlsCert:    fe.tlsCert,
		allowSetup: fe.allowSetup,
	})
}

var errCfg = configError{"--index-dsn must include a database name, e.g. " + indexDSNExample}

// TestStartupFailureDecision: the page replaces the exit only for a watch run
// that died before its console listened, on a failure a restart cannot fix.
func TestStartupFailureDecision(t *testing.T) {
	denied := fmt.Errorf("init: %w", &mysql.MySQLError{Number: 1045, Message: "Access denied for user 'bt'@'10.0.0.9'"})
	unreachable := fmt.Errorf("index MySQL did not become reachable: %w", context.DeadlineExceeded)
	cases := []struct {
		name string
		fe   failureEnv
		err  error
		page bool
	}{
		{"config error in watch", failureEnv{reached: true}, errCfg, true},
		{"permission error in watch", failureEnv{reached: true}, denied, true},
		{"not watch (another command)", failureEnv{reached: false}, errCfg, false},
		{"console was already listening", failureEnv{reached: true, listening: true}, errCfg, false},
		{"transient: index unreachable keeps the restart", failureEnv{reached: true}, unreachable, false},
		{"unclassified keeps the restart", failureEnv{reached: true}, errors.New("something else"), false},
		{"opt-out 0", failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_STARTUP_PAGE": "0"}}, errCfg, false},
		{"opt-out OFF", failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_STARTUP_PAGE": "OFF"}}, errCfg, false},
		{"opt-out false with spaces", failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_STARTUP_PAGE": " false "}}, errCfg, false},
		{"explicit on", failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_STARTUP_PAGE": "1"}}, errCfg, true},
		{"TLS configured by flag", failureEnv{reached: true, tlsCert: "/c.pem"}, errCfg, false},
		{"TLS configured by env", failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_TLS_CERT": "/c.pem"}}, errCfg, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.fe.build(tc.err) != nil; got != tc.page {
				t.Errorf("page = %v, want %v", got, tc.page)
			}
		})
	}
}

// TestStartupFailureListen: the page binds where the console would have, even
// when watch died before reading BINTRAIL_CONSOLE_LISTEN itself (the compose
// stack binds 0.0.0.0 inside the container through that variable; the flag
// default 127.0.0.1 would be unreachable from the host).
func TestStartupFailureListen(t *testing.T) {
	cases := []struct {
		name string
		fe   failureEnv
		want string
	}{
		{"default", failureEnv{reached: true}, "127.0.0.1:8090"},
		{"env", failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_LISTEN": "0.0.0.0:8090"}}, "0.0.0.0:8090"},
		{"flag wins over env", failureEnv{reached: true, listen: "127.0.0.1:9000", changed: map[string]bool{"console-listen": true}, env: map[string]string{"BINTRAIL_CONSOLE_LISTEN": "0.0.0.0:8090"}}, "127.0.0.1:9000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.fe.build(errCfg)
			if f == nil || f.listen != tc.want {
				t.Fatalf("listen = %+v, want %q", f, tc.want)
			}
		})
	}
}

// TestStartupFailurePageContent: what a browser sees. The full reason only
// where the bind is trusted AND the Host is not a stranger's name (a DNS
// rebinding page must not read an internal error), or when the message is one
// we wrote and know carries nothing internal.
func TestStartupFailurePageContent(t *testing.T) {
	denied := fmt.Errorf("init: %w", &mysql.MySQLError{Number: 1045, Message: "Access denied for user 'bt'@'10.0.0.9' <script>"})
	const secret = "10.0.0.9"
	cases := []struct {
		name     string
		fe       failureEnv
		err      error
		host     string
		showFull bool
	}{
		{"loopback bind, IP host", failureEnv{reached: true}, denied, "127.0.0.1:8090", true},
		{"loopback bind, localhost", failureEnv{reached: true}, denied, "localhost:8090", true},
		{"loopback bind, LOCALHOST", failureEnv{reached: true}, denied, "LOCALHOST:8090", true},
		{"loopback bind, stranger host (rebinding)", failureEnv{reached: true}, denied, "evil.example:8090", false},
		{"allowed host by env", failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_ALLOWED_HOSTS": "dbtrail.corp, other"}}, denied, "DBTRAIL.corp", true},
		{"public bind with allow-setup asserted", failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_LISTEN": "0.0.0.0:8090", "BINTRAIL_CONSOLE_ALLOW_SETUP": "1"}}, denied, "127.0.0.1:8090", true},
		{"public bind, no assertion, internal error", failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_LISTEN": "0.0.0.0:8090"}}, denied, "10.1.1.1:8090", false},
		{"public bind, no assertion, our own message", failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_LISTEN": "0.0.0.0:8090"}}, errCfg, "10.1.1.1:8090", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.fe.build(tc.err)
			if f == nil {
				t.Fatal("no page")
			}
			req := httptest.NewRequest("GET", "/", nil)
			req.Host = tc.host
			rec := httptest.NewRecorder()
			f.ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("status = %d, want 503 so health checks report it", rec.Code)
			}
			body := rec.Body.String()
			if !strings.Contains(body, "dbtrail could not start") {
				t.Errorf("page lacks its heading: %s", body)
			}
			full := strings.Contains(body, html.EscapeString(tc.err.Error()))
			if tc.err == denied {
				full = strings.Contains(body, secret)
				if strings.Contains(body, "<script>") {
					t.Error("message not escaped")
				}
			}
			if full != tc.showFull {
				t.Errorf("full reason shown = %v, want %v; body: %s", full, tc.showFull, body)
			}
			if !tc.showFull && !strings.Contains(body, "log") {
				t.Errorf("generic page does not point to the log: %s", body)
			}
		})
	}
}

// TestStartupFailureRun: the page serves until the process is told to stop,
// then exits 1 so a supervisor still sees a failed run.
func TestStartupFailureRun(t *testing.T) {
	f := failureEnv{reached: true}.build(errCfg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- f.run(ctx, ln, io.Discard) }()
	resp, err := http.Get("http://" + ln.Addr().String() + "/api/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("healthz = %d, want 503", resp.StatusCode)
	}
	cancel()
	select {
	case code := <-done:
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("page did not stop on cancel")
	}
}

// TestWatchStartupFailureWiring drives the real entry: a watch that refuses
// its configuration marks itself reached and gets a page, on the address the
// environment gives, through the process's own state.
func TestWatchStartupFailureWiring(t *testing.T) {
	origDSN := upIndexDSN
	t.Cleanup(func() { upIndexDSN = origDSN; watchReached.Store(false); watchListening.Store(false) })
	t.Setenv("BINTRAIL_CONSOLE_LISTEN", "0.0.0.0:18090")
	t.Setenv(startupPageEnv, "")
	t.Setenv("BINTRAIL_CONSOLE_TLS_CERT", "")
	// Integration tests in this package run a real watch first: it listens
	// and may set console flags. Start from what a fresh process has.
	watchReached.Store(false)
	watchListening.Store(false)
	origTLS := upConsoleTLSCert
	t.Cleanup(func() { upConsoleTLSCert = origTLS })
	upConsoleTLSCert = ""
	for _, name := range []string{"console-listen", "console-tls-cert", "console-allow-setup", "console-allowed-hosts"} {
		f := watchCmd.Flags().Lookup(name)
		was := f.Changed
		f.Changed = false
		t.Cleanup(func() { f.Changed = was })
	}
	upIndexDSN = ""
	err := runWatch(watchCmd, nil)
	if err == nil {
		t.Fatal("runWatch accepted an empty index DSN")
	}
	f := watchStartupFailure(err)
	if f == nil {
		t.Fatal("no startup page for a configuration refusal in watch")
	}
	if f.listen != "0.0.0.0:18090" || !f.public {
		t.Errorf("page = %+v, want listen 0.0.0.0:18090 and a message shown to anyone", f)
	}
	watchListening.Store(true)
	if watchStartupFailure(err) != nil {
		t.Error("a failure after the console listened still got a page")
	}
}

// TestStartupFailureReviewFixes pins what the first review found missing.
func TestStartupFailureReviewFixes(t *testing.T) {
	public := failureEnv{reached: true, env: map[string]string{"BINTRAIL_CONSOLE_LISTEN": "0.0.0.0:8090"}}
	get := func(f *startupFailure, host string) string {
		req := httptest.NewRequest("GET", "/", nil)
		req.Host = host
		rec := httptest.NewRecorder()
		f.ServeHTTP(rec, req)
		return rec.Body.String()
	}

	// A preflight refusal names its failed checks (constants), even where the
	// message itself is not shown.
	pf := fmt.Errorf("preflight failed (use --skip-doctor to bypass at your own risk): %w", &doctor.PreflightError{Checks: []string{"binlog_format", "log_bin"}})
	f := public.build(pf)
	if f == nil {
		t.Fatal("no page for a preflight refusal")
	}
	if body := get(f, "10.1.1.1:8090"); !strings.Contains(body, "<code>binlog_format</code>, <code>log_bin</code>") {
		t.Errorf("failed checks not listed: %s", body)
	}

	// Our message wrapped by text we did not write is not shown to anyone.
	wrapped := fmt.Errorf("context from elsewhere 10.0.0.9: %w", errCfg)
	if body := get(public.build(wrapped), "10.1.1.1:8090"); strings.Contains(body, "10.0.0.9") {
		t.Errorf("wrapped message shown on an untrusted bind: %s", body)
	}

	// The advice restarts the container even when nothing in compose changed.
	if body := get(public.build(errCfg), "10.1.1.1:8090"); !strings.Contains(body, "docker compose restart bintrail") || strings.Contains(body, "docker compose up -d") {
		t.Errorf("restart advice: %s", body)
	}

	// The hidden-reason hint matches why it is hidden: a hostname fix only
	// helps a trusted bind; an untrusted one points to the log.
	internal := fmt.Errorf("init: %w", &mysql.MySQLError{Number: 1045, Message: "denied"})
	if body := get(public.build(internal), "127.0.0.1:8090"); strings.Contains(body, "ALLOWED_HOSTS") || !strings.Contains(body, "no password set") {
		t.Errorf("untrusted bind hint: %s", body)
	}
	if body := get(failureEnv{reached: true}.build(internal), "evil.example"); !strings.Contains(body, "ALLOWED_HOSTS") {
		t.Errorf("trusted bind, unknown host hint: %s", body)
	}
}

// TestStartupFailureTimesOut: the page gives way after startupPageFor so the
// supervisor tries again, which is how a failure that heals with time heals.
func TestStartupFailureTimesOut(t *testing.T) {
	orig := startupPageFor
	t.Cleanup(func() { startupPageFor = orig })
	startupPageFor = 50 * time.Millisecond
	f := failureEnv{reached: true}.build(errCfg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() { done <- f.run(context.Background(), ln, io.Discard) }()
	select {
	case code := <-done:
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("page did not give way after startupPageFor")
	}
}
