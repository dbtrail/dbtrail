package consoleapp

import (
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// portSettingsServer is a console as the capture process builds it for the
// MySQL port: a saved setting to manage, and no address from startup.
func portSettingsServer(t *testing.T, token string) (*console.Server, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, console.FlashbackFileName)
	srv, err := console.New(console.Config{Listen: "127.0.0.1:8090", Token: token, FlashbackPath: path,
		AuthPath: filepath.Join(dir, "auth.yaml"), MCPTokenPath: filepath.Join(dir, "mcp.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	if r := srv.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	return srv, path
}

func portFree(addr string) bool {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	ln.Close()
	return true
}

// The capture process opens no MySQL port (#2341): it saves the switch and
// the password for the router to follow. A setting saved as on, at start or
// from the web interface, leaves its address free here.
func TestSQLPortSettings_theCaptureProcessBindsNothing(t *testing.T) {
	saveRouteGlobals(t)
	srv, path := portSettingsServer(t, "")
	addr := freeAddr(t)
	if err := os.WriteFile(path, []byte("version: 1\nenabled: true\nlisten: "+addr+"\npassword: saved-before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	startSQLPortSettings(srv)
	time.Sleep(50 * time.Millisecond)
	if !portFree(addr) {
		t.Fatal("the capture process bound the address of a port saved as on")
	}
	f, err := console.LoadFlashbackFile(path)
	if err != nil || !f.Enabled || f.Password != "saved-before" {
		t.Fatalf("the saved setting after start: %+v (%v), want it as it was", f, err)
	}
}

// --flashback-listen used to open the port in this process, always on. An
// installation that had it keeps its port: it is saved as on, once, with a
// password when there was none, and a setting already saved as on is left
// as it was.
func TestSQLPortSettings_flashbackListenIsSavedAsOn(t *testing.T) {
	saveRouteGlobals(t)
	srv, path := portSettingsServer(t, "tok")
	addr := freeAddr(t)
	upConsoleFlashbackListen = addr
	startSQLPortSettings(srv)
	f, err := console.LoadFlashbackFile(path)
	if err != nil || !f.Enabled || !strings.HasPrefix(f.Password, "bfp_") {
		t.Fatalf("after a start with --flashback-listen: %+v (%v), want saved as on with a password", f, err)
	}
	if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("the file holding the password: %v", err)
	}
	if !portFree(addr) {
		t.Fatal("--flashback-listen still binds its address in the capture process")
	}
	first := f.Password
	// Started again: nothing changes.
	srv2, err := console.New(console.Config{Listen: "127.0.0.1:8090", Token: "tok", FlashbackPath: path,
		AuthPath: filepath.Join(filepath.Dir(path), "auth.yaml"), MCPTokenPath: filepath.Join(filepath.Dir(path), "mcp.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	if r := srv2.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	startSQLPortSettings(srv2)
	if f, err := console.LoadFlashbackFile(path); err != nil || !f.Enabled || f.Password != first {
		t.Fatalf("after a second start: %+v (%v), want the same password", f, err)
	}

	// Turned off in the web interface, with the old setting still where
	// DBTrail starts: the next start leaves it off. The setting that exists
	// is the operator's.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "http://127.0.0.1:8090/api/flashback", strings.NewReader(`{"enabled":false}`))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	srv2.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("turn off = %d %s", rec.Code, rec.Body)
	}
	srv3, err := console.New(console.Config{Listen: "127.0.0.1:8090", Token: "tok", FlashbackPath: path,
		AuthPath: filepath.Join(filepath.Dir(path), "auth.yaml"), MCPTokenPath: filepath.Join(filepath.Dir(path), "mcp.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	if r := srv3.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	startSQLPortSettings(srv3)
	if f, err := console.LoadFlashbackFile(path); err != nil || f.Enabled {
		t.Fatalf("a port turned off in the web interface came back ON at the next start: %+v (%v)", f, err)
	}
}

// What a start says about the settings that used to act here: nothing when
// none is given, and for each one what it no longer does, what happened
// instead, and to remove it.
func TestSQLPortStartupLines(t *testing.T) {
	if got := sqlPortStartupLines("", "", 0, true, nil); got != "" {
		t.Errorf("with neither setting: %q, want nothing said", got)
	}
	got := sqlPortStartupLines("0.0.0.0:3308", "was saved as on, with a new password you can replace in the web interface", 0, true, nil)
	for _, want := range []string{"--flashback-listen", "no longer opens a port in this process", "served by the router", "the address given here (0.0.0.0:3308) is not used", "127.0.0.1:3309, this machine only", "BINTRAIL_ROUTER_LISTEN", "was saved as on", "only if the router is started with it too", "Remove the setting"} {
		if !strings.Contains(got, want) {
			t.Errorf("--flashback-listen: want %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "route-max-copy-age") {
		t.Errorf("read routing was mentioned with no --route-max-copy-age:\n%s", got)
	}
	if got := sqlPortStartupLines("0.0.0.0:3308", "is saved as on", 0, false, nil); strings.Contains(got, "access token") {
		t.Errorf("a token was mentioned with none set:\n%s", got)
	}
	got = sqlPortStartupLines("", "", 0, false, []string{"--route-read-only", "--route-scan-rows"})
	for _, want := range []string{"--route-read-only, --route-scan-rows given to this process no longer applies", "NOT in force on the port", "refuses writes unless it is told otherwise"} {
		if !strings.Contains(got, want) {
			t.Errorf("--route-* flags given here: want %q in:\n%s", want, got)
		}
	}
	got = sqlPortStartupLines("", "", 15*time.Minute, false, nil)
	for _, want := range []string{"--route-max-copy-age", "no longer turns Engine Routing on", "it is OFF", "the copy answers every statement", "no maximum copy age", "Remove the setting"} {
		if !strings.Contains(got, want) {
			t.Errorf("--route-max-copy-age: want %q in:\n%s", want, got)
		}
	}
}

// A value of a --route-* setting that does not parse is the router's to
// refuse: it must not stop the process that captures.
func TestResolveRouteFlags_neverStopsCapture(t *testing.T) {
	saveRouteGlobals(t)
	for k, v := range map[string]string{"BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE": "soon", "BINTRAIL_CONSOLE_ROUTE_READ_ONLY": "maybe",
		"BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD": "high", "BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS": "many", "BINTRAIL_CONSOLE_FLASHBACK_LISTEN": "nonsense"} {
		t.Setenv(k, v)
	}
	if err := resolveRouteFlags(routeFlagsCmd()); err != nil {
		t.Fatalf("resolveRouteFlags = %v, want no error", err)
	}
	// The value that does not parse was still asked for: it is said at
	// startup like one that parses, not dropped.
	if upRouteMaxCopyAge <= 0 || upConsoleFlashbackListen != "nonsense" {
		t.Errorf("max copy age %v, listen %q", upRouteMaxCopyAge, upConsoleFlashbackListen)
	}
	if got := sqlPortStartupLines("", "", upRouteMaxCopyAge, false, nil); !strings.Contains(got, "no longer turns Engine Routing on") {
		t.Errorf("a max copy age that does not parse is not said: %q", got)
	}
	for _, off := range []string{"", "0", "0s"} {
		upRouteMaxCopyAge = 0
		t.Setenv("BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", off)
		if err := resolveRouteFlags(routeFlagsCmd()); err != nil || upRouteMaxCopyAge != 0 {
			t.Errorf("%q asks for nothing: %v (%v)", off, upRouteMaxCopyAge, err)
		}
	}
	t.Setenv("BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", "15m")
	if err := resolveRouteFlags(routeFlagsCmd()); err != nil || upRouteMaxCopyAge != 15*time.Minute {
		t.Errorf("a value that parses: %v (%v)", upRouteMaxCopyAge, err)
	}
}

// The port turned on is answered by another process, and under a compose file
// from before the router served it, by none: that is the one case that takes
// a working port away, so it is a warning that names the port and the way
// out. A file that passes no number, or a current one, gets the plain line.
func TestSQLPortServedElsewhereLine(t *testing.T) {
	for _, tc := range []struct {
		name    string
		on      bool
		version string
		want    []string
		not     []string
	}{
		{"off", false, "2", nil, nil},
		{"off, current file", false, "4", nil, nil},
		{"on, file from before the router", true, "2", []string{"WARNING", "NOTHING ANSWERS", "3309", "(version 2)", composeDownload}, nil},
		{"on, file with the router on its old port", true, " 3 ", []string{"WARNING", "(version 3)", composeDownload}, nil},
		{"on, current file", true, "4", []string{"The router answers it"}, []string{"WARNING", composeDownload}},
		{"on, a newer file", true, "5", []string{"The router answers it"}, []string{"WARNING"}},
		{"on, no number (a package, or a file of their own)", true, "", []string{"The router answers it"}, []string{"WARNING"}},
		{"on, a number that is not one", true, "latest", []string{"The router answers it"}, []string{"WARNING"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := sqlPortServedElsewhereLine(tc.on, tc.version)
			if len(tc.want) == 0 && got != "" {
				t.Fatalf("want nothing said, got %q", got)
			}
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("missing %q in %q", w, got)
				}
			}
			for _, n := range tc.not {
				if strings.Contains(got, n) {
					t.Errorf("unexpected %q in %q", n, got)
				}
			}
			if got != "" && !strings.HasSuffix(got, "\n") {
				t.Errorf("the line does not end: %q", got)
			}
		})
	}
}

// The row cap of the MySQL port is the router's setting now. The two
// processes read one environment, so a value the router would refuse must not
// stop the process that captures: nothing here reads it any more.
func TestWatch_aBadPortRowCapDoesNotStopCapture(t *testing.T) {
	src, err := os.ReadFile("watch.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), `os.Getenv("BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS")`) || strings.Contains(string(src), "func resolveSQLPortMaxRows") {
		t.Error("watch.go reads or validates BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS again: a value the router refuses would stop capture")
	}
	if f := watchCmd.Flags().Lookup("sql-port-max-rows"); f == nil || !strings.Contains(f.Usage, "No longer acts in this process") {
		t.Error("--sql-port-max-rows must stay a flag watch accepts, and say it no longer acts there")
	}
}

// A metrics address that cannot be opened is said, with the way out, and with
// the one thing an operator must know first: the MySQL port is served.
func TestRouterMetricsOffLine(t *testing.T) {
	got := routerMetricsOffLine("127.0.0.1:9091", errors.New("listen tcp 127.0.0.1:9091: bind: address already in use"))
	for _, want := range []string{"WARNING", "127.0.0.1:9091", "address already in use", "The MySQL port is served without them", "BINTRAIL_ROUTER_METRICS_ADDR", "BINTRAIL_CONSOLE_ROUTER_URL"} {
		if !strings.Contains(got, want) {
			t.Errorf("want %q in %q", want, got)
		}
	}
	src, err := os.ReadFile("router.go")
	if err != nil {
		t.Fatal(err)
	}
	i := strings.Index(string(src), "streamrun.StartMetricsServerWith(st.MetricsAddr")
	if i < 0 {
		t.Fatal("router.go no longer starts its metrics server where this test looks")
	}
	if blk := string(src)[i : i+600]; !strings.Contains(blk, "routerMetricsOffLine(st.MetricsAddr, err)") || strings.Contains(blk, "return err") {
		t.Errorf("a metrics address that cannot be opened must be said and not end the router:\n%s", blk)
	}
}

// What a start says became of a port that --flashback-listen used to open:
// one sentence per outcome, and never "on" for a port that is off.
func TestSQLPortMigratedWords(t *testing.T) {
	for _, tc := range []struct {
		name        string
		created, on bool
		err         error
		want, not   string
	}{
		{"saved now", true, true, nil, "was saved as on, with a new password", "OFF"},
		{"already on", false, true, nil, "is saved as on, and the password created for it in the web interface is accepted", "new password you"},
		{"turned off in the web interface", false, false, nil, "is saved as OFF", "saved as on"},
		{"cannot be saved", false, false, errors.New("the file is from a newer version"), "could not be saved as on (the file is from a newer version)", "is saved as"},
	} {
		got := sqlPortMigratedWords(tc.created, tc.on, tc.err)
		if !strings.Contains(got, tc.want) || strings.Contains(got, tc.not) {
			t.Errorf("%s: %q, want %q and not %q", tc.name, got, tc.want, tc.not)
		}
	}
}

// The old-compose notice is worked out where DBTrail starts and must reach
// the console, which says it in the web interface while the port is on.
func TestUpConsoleConfig_theOldComposeNoticeReachesTheConsole(t *testing.T) {
	t.Setenv("BINTRAIL_COMPOSE_VERSION", "3")
	opts := upConsoleOpts()
	if !strings.Contains(opts.SQLPortNotice, "NOTHING ANSWERS ON IT") || !strings.Contains(opts.SQLPortNotice, composeDownload) {
		t.Fatalf("upConsoleOpts().SQLPortNotice = %q, want the notice with the way out", opts.SQLPortNotice)
	}
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/idx",
		consoleOpts{Listen: "127.0.0.1:8090", Token: "tok", SQLPortNotice: opts.SQLPortNotice}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FlashbackNotice != opts.SQLPortNotice {
		t.Errorf("Config.FlashbackNotice = %q, want the notice", cfg.FlashbackNotice)
	}
	t.Setenv("BINTRAIL_COMPOSE_VERSION", "4")
	if got := upConsoleOpts().SQLPortNotice; got != "" {
		t.Errorf("under a current file: %q, want none", got)
	}
}
