package consoleapp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// TestValidateRoutePolicy: routing on with both thresholds at 0 can never
// route a statement, so the daemon refuses to start that way instead of
// filling the tally with cheap_plan; routing off accepts any thresholds.
func TestValidateRoutePolicy(t *testing.T) {
	cases := []struct {
		name    string
		maxAge  time.Duration
		cost    float64
		rows    int64
		wantErr bool
	}{
		{"off, thresholds zero", 0, 0, 0, false},
		{"on, defaults", time.Hour, 10000, 100000, false},
		{"on, cost rule only", time.Hour, 10000, 0, false},
		{"on, scan rule only", time.Hour, 0, 2, false},
		{"on, nothing could route", time.Hour, 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRoutePolicy(tc.maxAge, tc.cost, tc.rows)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestValidateRouteReadOnly: read-only mode guards what read routing sends
// to the source, so asking for it without routing (or without the port) is a
// misconfiguration the daemon names at startup instead of ignoring.
func TestValidateRouteReadOnly(t *testing.T) {
	cases := []struct {
		name     string
		readOnly bool
		listen   string
		maxAge   time.Duration
		wantErr  string
	}{
		{"off, nothing set", false, "", 0, ""},
		{"off, routing on", false, "127.0.0.1:3308", time.Hour, ""},
		{"on, routing on", true, "127.0.0.1:3308", time.Hour, ""},
		{"on, port on, routing off", true, "127.0.0.1:3308", 0, "--route-max-copy-age"},
		{"on, no port", true, "", time.Hour, "--flashback-listen"},
		{"on, nothing else", true, "", 0, "--flashback-listen"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRouteReadOnly(tc.readOnly, tc.listen, tc.maxAge)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("err = %v, want none", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), "--route-read-only") {
				t.Fatalf("err = %v, want one naming --route-read-only and %s", err, tc.wantErr)
			}
			t.Log(err)
		})
	}
}

// TestRouteStartupLine: the line printed at startup says which mode the
// port is in, and only the read-write one says the token can write.
func TestRouteStartupLine(t *testing.T) {
	cfg := flashbackConfig{RouteMaxCopyAge: 15 * time.Minute, RoutePolicy: readrouter.DefaultPolicy()}
	rw := routeStartupLine(cfg)
	cfg.RouteReadOnly = true
	ro := routeStartupLine(cfg)
	t.Log(rw)
	t.Log(ro)
	for _, want := range []string{"read-write", "writes included", "--route-read-only", "plan cost >= 10000", "a full scan over >= 100000 rows (on a MariaDB source, also joins that read that many rows in all)", "15m0s"} {
		if !strings.Contains(rw, want) {
			t.Errorf("read-write line misses %q: %s", want, rw)
		}
	}
	for _, want := range []string{"read-only (--route-read-only)", "refused and never sent to the source", "stored function", "plan cost >= 10000"} {
		if !strings.Contains(ro, want) {
			t.Errorf("read-only line misses %q: %s", want, ro)
		}
	}
	if strings.Contains(ro, "writes included") || strings.Contains(ro, "read-write") {
		t.Errorf("read-only line still promises writes: %s", ro)
	}
	for _, line := range []string{rw, ro} {
		if strings.Contains(line, "—") || !strings.HasSuffix(line, "\n") {
			t.Errorf("line holds an em dash or lacks its newline: %q", line)
		}
	}
}

// saveRouteGlobals restores the port's flag variables after a test that sets
// them, and clears the environment variables that would feed them.
func saveRouteGlobals(t *testing.T) {
	t.Helper()
	listen, age, ro, cost, rows := upConsoleFlashbackListen, upRouteMaxCopyAge, upRouteReadOnly, upRouteCostThreshold, upRouteScanRows
	t.Cleanup(func() {
		upConsoleFlashbackListen, upRouteMaxCopyAge, upRouteReadOnly, upRouteCostThreshold, upRouteScanRows = listen, age, ro, cost, rows
	})
	for _, k := range []string{"BINTRAIL_CONSOLE_FLASHBACK_LISTEN", "BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", "BINTRAIL_CONSOLE_ROUTE_READ_ONLY",
		"BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD", "BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS"} {
		t.Setenv(k, "")
	}
	upConsoleFlashbackListen, upRouteMaxCopyAge, upRouteReadOnly = "", 0, false
	upRouteCostThreshold, upRouteScanRows = readrouter.DefaultPolicy().CostThreshold, readrouter.DefaultPolicy().ScanRows
}

// routeFlagsCmd is a command carrying the port's flags bound to the same
// variables watch binds them to, so a test can set one "on the command line".
func routeFlagsCmd() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&upConsoleFlashbackListen, "flashback-listen", "", "")
	cmd.Flags().DurationVar(&upRouteMaxCopyAge, "route-max-copy-age", 0, "")
	cmd.Flags().BoolVar(&upRouteReadOnly, "route-read-only", false, "")
	return cmd
}

// carriesReadOnly reports whether BOTH things built from the resolved flags
// say read-only: what the port serves with, and what the console reports. A
// daemon where they disagree shows "read-only" while forwarding writes.
func carriesReadOnly() (serving, reported bool) {
	return flashbackConfigFromFlags().RouteReadOnly, upConsoleOpts().ReadRouting.ReadOnly
}

// TestRouteReadOnlyFlagReachesThePort: --route-read-only on the command
// line, through the real resolve path, reaches the port's configuration and
// the console's.
func TestRouteReadOnlyFlagReachesThePort(t *testing.T) {
	saveRouteGlobals(t)
	// The flag watch registers is bound to the variable the port reads.
	f := watchCmd.Flags().Lookup("route-read-only")
	if f == nil {
		t.Fatal("--route-read-only is not registered on watch")
	}
	if err := f.Value.Set("true"); err != nil || !upRouteReadOnly {
		t.Fatalf("setting watch's --route-read-only did not set the port's variable (err %v)", err)
	}
	upRouteReadOnly = false

	cmd := routeFlagsCmd()
	for name, v := range map[string]string{"flashback-listen": "127.0.0.1:3308", "route-max-copy-age": "15m", "route-read-only": "true"} {
		if err := cmd.Flags().Set(name, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := resolveRouteFlags(cmd); err != nil {
		t.Fatal(err)
	}
	if serving, reported := carriesReadOnly(); !serving || !reported {
		t.Errorf("flag set: the port serves read-only=%v, the console reports read-only=%v; want both true", serving, reported)
	}
	if cfg := flashbackConfigFromFlags(); cfg.RouteMaxCopyAge != 15*time.Minute {
		t.Errorf("max copy age = %s, want 15m", cfg.RouteMaxCopyAge)
	}

	// And without the flag neither says so.
	saveRouteGlobals(t)
	cmd = routeFlagsCmd()
	_ = cmd.Flags().Set("flashback-listen", "127.0.0.1:3308")
	_ = cmd.Flags().Set("route-max-copy-age", "15m")
	if err := resolveRouteFlags(cmd); err != nil {
		t.Fatal(err)
	}
	if serving, reported := carriesReadOnly(); serving || reported {
		t.Errorf("flag not set: serving=%v reported=%v, want both false", serving, reported)
	}
}

// TestRouteReadOnlyEnvReachesThePort: the same through the environment
// variable alone, plus the flag winning over it.
func TestRouteReadOnlyEnvReachesThePort(t *testing.T) {
	for _, v := range []string{"1", "true", "TRUE"} {
		saveRouteGlobals(t)
		t.Setenv("BINTRAIL_CONSOLE_FLASHBACK_LISTEN", "127.0.0.1:3308")
		t.Setenv("BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", "15m")
		t.Setenv("BINTRAIL_CONSOLE_ROUTE_READ_ONLY", v)
		if err := resolveRouteFlags(routeFlagsCmd()); err != nil {
			t.Fatalf("%s: %v", v, err)
		}
		if serving, reported := carriesReadOnly(); !serving || !reported {
			t.Errorf("env %s: serving=%v reported=%v, want both true", v, serving, reported)
		}
	}
	// The console path resolves it too (resolveUpConsoleEnv is what the
	// daemon's two run paths call).
	saveRouteGlobals(t)
	t.Setenv("BINTRAIL_CONSOLE_FLASHBACK_LISTEN", "127.0.0.1:3308")
	t.Setenv("BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", "15m")
	t.Setenv("BINTRAIL_CONSOLE_ROUTE_READ_ONLY", "1")
	if err := resolveUpConsoleEnv(routeFlagsCmd()); err != nil {
		t.Fatal(err)
	}
	if serving, reported := carriesReadOnly(); !serving || !reported {
		t.Errorf("resolveUpConsoleEnv: serving=%v reported=%v, want both true", serving, reported)
	}
	// An explicit --route-read-only=false wins over the variable.
	saveRouteGlobals(t)
	t.Setenv("BINTRAIL_CONSOLE_ROUTE_READ_ONLY", "1")
	cmd := routeFlagsCmd()
	_ = cmd.Flags().Set("route-read-only", "false")
	if err := resolveRouteFlags(cmd); err != nil {
		t.Fatal(err)
	}
	if serving, reported := carriesReadOnly(); serving || reported {
		t.Errorf("flag false over env 1: serving=%v reported=%v, want both false", serving, reported)
	}
}

// TestRouteReadOnlyEnvGarbage: a value that is not a yes or no stops the
// resolve with the variable's name, the value and what is accepted.
func TestRouteReadOnlyEnvGarbage(t *testing.T) {
	saveRouteGlobals(t)
	t.Setenv("BINTRAIL_CONSOLE_ROUTE_READ_ONLY", "yes please")
	err := resolveRouteFlags(routeFlagsCmd())
	if err == nil {
		t.Fatal("a garbage value was accepted")
	}
	t.Log(err)
	for _, want := range []string{"BINTRAIL_CONSOLE_ROUTE_READ_ONLY", "yes please", "1 or true", "0 or false"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error misses %q: %v", want, err)
		}
	}
	if upRouteReadOnly {
		t.Error("a garbage value turned the mode on")
	}
}

// TestRouteReadOnlyCheckedBeforeAnyConnection: the flags are checked where
// they are resolved, and runWatch resolves them before it waits for the
// index. The index DSN here points at a port nothing listens on: a check
// that ran after the wait would return the index's error, 90 seconds later.
func TestRouteReadOnlyCheckedBeforeAnyConnection(t *testing.T) {
	for name, set := range map[string]func(){
		"read-only without routing": func() {
			t.Setenv("BINTRAIL_CONSOLE_FLASHBACK_LISTEN", "127.0.0.1:3308")
			t.Setenv("BINTRAIL_CONSOLE_ROUTE_READ_ONLY", "1")
		},
		"garbage value": func() { t.Setenv("BINTRAIL_CONSOLE_ROUTE_READ_ONLY", "maybe") },
	} {
		saveRouteGlobals(t)
		set()
		if err := resolveRouteFlags(routeFlagsCmd()); err == nil || !strings.Contains(err.Error(), "ROUTE_READ_ONLY") && !strings.Contains(err.Error(), "--route-read-only") {
			t.Errorf("%s: resolveRouteFlags = %v, want the read-only error", name, err)
		}
		saveRouteGlobals(t)
		set()
		savedDSN, savedFormat := upIndexDSN, upFormat
		upIndexDSN, upFormat = "nobody:x@tcp(127.0.0.1:1)/none", "text"
		done := make(chan error, 1)
		go func() { done <- runWatch(routeFlagsCmd(), nil) }()
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "ROUTE_READ_ONLY") && !strings.Contains(err.Error(), "--route-read-only") {
				t.Errorf("%s: runWatch = %v, want the read-only error before anything connects", name, err)
			}
		case <-time.After(20 * time.Second):
			t.Errorf("%s: runWatch is still running: the check did not come before the wait for the index", name)
		}
		upIndexDSN, upFormat = savedDSN, savedFormat
	}
}

// TestStartFlashbackPortRefusesReadOnlyWithoutRouting: the port itself
// refuses to start too, whoever calls it.
func TestStartFlashbackPortRefusesReadOnlyWithoutRouting(t *testing.T) {
	saveRouteGlobals(t)
	upConsoleFlashbackListen, upRouteReadOnly = "127.0.0.1:0", true
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := startFlashbackPort(context.Background(), srv); err == nil || !strings.Contains(err.Error(), "--route-read-only") {
		t.Fatalf("startFlashbackPort = %v, want the read-only validation error", err)
	}
}

// routeTestFreeSQL is a copy that is never asked: the statements below are
// decided before it.
type routeTestFreeSQL struct{}

func (routeTestFreeSQL) Run(context.Context, string, string, sqlsandbox.Session) (sqlsandbox.Result, error) {
	return sqlsandbox.Result{}, errors.New("the copy was asked")
}
func (routeTestFreeSQL) CopyUpdatedAt(context.Context) time.Time { return time.Now() }
func (routeTestFreeSQL) RowCap() int                             { return 0 }

// TestBindReadRouterCarriesReadOnly: a connection bound from a port
// configured read-only refuses a write, and one bound from a read-write port
// sends it on. No database: the refusal comes before any connection, and the
// read-write side fails to connect to a port nothing listens on, which is
// the proof that it tried.
func TestBindReadRouterCarriesReadOnly(t *testing.T) {
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	tgt := console.FlashbackTarget{ID: "s1", SQL: &console.SQLOnCopy{}, ForwardDSN: "nobody:x@tcp(127.0.0.1:1)/none", SourceSSL: console.ServerEntry{}.SourceSSL()}
	bind := func(readOnly bool) *shim.Handler {
		h := shim.NewHandler(nil, nil)
		h.BindFreeSQL(routeTestFreeSQL{})
		bindReadRouter(h, srv, tgt, "s1", flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.DefaultPolicy(),
			RouteReadOnly: readOnly, QueryTimeout: 5 * time.Second}, slog.Default())
		t.Cleanup(h.Close)
		return h
	}
	code := func(err error) uint16 {
		var me *gomysql.MyError
		if errors.As(err, &me) {
			return me.Code
		}
		return 0
	}
	_, err = bind(true).HandleQuery("DELETE FROM t")
	if code(err) != gomysql.ER_OPTION_PREVENTS_STATEMENT || !strings.Contains(err.Error(), "--route-read-only") {
		t.Errorf("read-only port: DELETE got %v, want the 1290 refusal", err)
	}
	_, err = bind(false).HandleQuery("DELETE FROM t")
	if code(err) != readrouter.CodeUpstreamLost {
		t.Errorf("read-write port: DELETE got %v, want the forward to be attempted (2006 from the unreachable source)", err)
	}
	// Routing off: nothing is bound, whatever the read-only setting.
	h := shim.NewHandler(nil, nil)
	h.BindFreeSQL(routeTestFreeSQL{})
	bindReadRouter(h, srv, tgt, "s1", flashbackConfig{RouteReadOnly: true}, slog.Default())
	if _, err := h.HandleQuery("DELETE FROM t"); code(err) == gomysql.ER_OPTION_PREVENTS_STATEMENT || code(err) == readrouter.CodeUpstreamLost {
		t.Errorf("routing off: DELETE got %v, want the copy's own answer", err)
	}
}

// TestBindReadRouterUsesTheServersTLS: the connection's TLS comes from the
// target (the entry's ssl_* fields). A setting that cannot be used keeps the
// connection copy-only and tells the Connect page why, in the entry's own
// words, instead of connecting some other way.
func TestBindReadRouterUsesTheServersTLS(t *testing.T) {
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", FlashbackListen: "127.0.0.1:3308",
		ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	unavailable := func() string {
		req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
		req.Header.Set("Authorization", "Bearer tok")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		var fb struct {
			Routing struct {
				Servers map[string]struct {
					Unavailable string `json:"unavailable"`
				} `json:"servers"`
			} `json:"routing"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &fb); err != nil {
			t.Fatalf("decode %s: %v", rec.Body.String(), err)
		}
		return fb.Routing.Servers["s1"].Unavailable
	}
	bind := func(ssl config.SSL) *shim.Handler {
		h := shim.NewHandler(nil, nil)
		h.BindFreeSQL(routeTestFreeSQL{})
		bindReadRouter(h, srv, console.FlashbackTarget{ID: "s1", SQL: &console.SQLOnCopy{}, ForwardDSN: "nobody:x@tcp(127.0.0.1:1)/none", SourceSSL: ssl},
			"s1", flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.DefaultPolicy(), QueryTimeout: 5 * time.Second}, slog.Default())
		t.Cleanup(h.Close)
		return h
	}
	// An unknown mode: not bound (the write gets the copy's answer, not the
	// source's 2006), and the page says why without a command-line flag.
	_, err = bind(config.SSL{Mode: "prefered"}).HandleQuery("DELETE FROM t")
	var me *gomysql.MyError
	if errors.As(err, &me) && me.Code == readrouter.CodeUpstreamLost {
		t.Errorf("an unusable TLS mode still bound a router: %v", err)
	}
	why := unavailable()
	t.Log(why)
	if !strings.Contains(why, "prefered") || !strings.Contains(why, "this server's TLS settings cannot be used") || strings.Contains(why, "--ssl") {
		t.Errorf("the page says %q, want the bad TLS mode named without a flag", why)
	}
	// A usable one: bound (the forward is attempted), and the note is gone.
	_, err = bind(config.SSL{Mode: "required"}).HandleQuery("DELETE FROM t")
	if !errors.As(err, &me) || me.Code != readrouter.CodeUpstreamLost {
		t.Errorf("a usable TLS mode did not bind a router: %v", err)
	}
	if why := unavailable(); why != "" {
		t.Errorf("after a connection bound, the page still says %q", why)
	}
}
