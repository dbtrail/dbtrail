package consoleapp

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// --max-wait: empty and zero are the line with no time bound, a time with a
// unit is that time, and what cannot mean what was typed is refused with the
// way to write it.
func TestParseRouterMaxWait(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    time.Duration
		wantErr string
	}{
		{in: "", want: sqlsandbox.NoMaxWait},
		{in: "   ", want: sqlsandbox.NoMaxWait},
		{in: "0", want: sqlsandbox.NoMaxWait},
		{in: "0s", want: sqlsandbox.NoMaxWait},
		{in: "0.0", want: sqlsandbox.NoMaxWait},
		{in: "30s", want: 30 * time.Second},
		{in: " 2m ", want: 2 * time.Minute},
		{in: "1s", want: time.Second},
		{in: "1h30m", want: 90 * time.Minute},
		{in: "30", wantErr: "no unit"},
		{in: "1.5", wantErr: "no unit"},
		{in: "-5s", wantErr: "negative"},
		{in: "-5", wantErr: "negative"},
		{in: "+5", wantErr: "like 5s or 5m"},
		{in: "500ms", wantErr: "under a second"},
		{in: "30S", wantErr: "not a time"},
		{in: "forever", wantErr: "not a time"},
		{in: "30 s", wantErr: "not a time"},
		{in: "-0", want: sqlsandbox.NoMaxWait},
		{in: "+30s", want: 30 * time.Second},
		{in: "24h", want: 24 * time.Hour},
		{in: "25h", wantErr: "more than 24h"},
		{in: "2562047h47m16.854775807s", wantErr: "more than 24h"},
		{in: "9999999h", wantErr: "not a time"},
		{in: "NaN", wantErr: "not a time"},
		{in: "Inf", wantErr: "not a time"},
		{in: "1e3", wantErr: "not a time"},
		{in: "0x0p0", wantErr: "not a time"},
		{in: "1.2.3", wantErr: "not a time"},
		{in: ".", wantErr: "not a time"},
	} {
		got, err := parseRouterMaxWait(tc.in)
		switch {
		case tc.wantErr != "":
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("parseRouterMaxWait(%q) = %v, %v; want an error saying %q", tc.in, got, err, tc.wantErr)
			}
		case err != nil || got != tc.want:
			t.Errorf("parseRouterMaxWait(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}
}

func maxWaitSettings(t *testing.T, args ...string) (routerSettings, error) {
	t.Helper()
	cmd := &cobra.Command{}
	addRouterFlags(cmd)
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatal(err)
	}
	return routerSettingsFrom(cmd)
}

// The router waits with no time bound unless told otherwise, the flag wins
// over the environment, and a bad value stops the start.
func TestRouterSettings_maxWait(t *testing.T) {
	t.Setenv("BINTRAIL_CONSOLE_TOKEN", "t")
	t.Setenv("BINTRAIL_ROUTER_MAX_WAIT", "")
	st, err := maxWaitSettings(t)
	if err != nil || st.MaxWait != sqlsandbox.NoMaxWait {
		t.Fatalf("unset: MaxWait = %v, %v; want no time bound", st.MaxWait, err)
	}
	t.Setenv("BINTRAIL_ROUTER_MAX_WAIT", "45s")
	if st, err = maxWaitSettings(t); err != nil || st.MaxWait != 45*time.Second {
		t.Fatalf("env: MaxWait = %v, %v; want 45s", st.MaxWait, err)
	}
	if st, err = maxWaitSettings(t, "--max-wait", "2m"); err != nil || st.MaxWait != 2*time.Minute {
		t.Fatalf("flag over env: MaxWait = %v, %v; want 2m", st.MaxWait, err)
	}
	t.Setenv("BINTRAIL_ROUTER_MAX_WAIT", "30")
	if _, err = maxWaitSettings(t); err == nil || !strings.Contains(err.Error(), "no unit") {
		t.Fatalf("env 30: err = %v; want a refusal that names the missing unit", err)
	}
}

// What the router builds its pool with is what was resolved: the runner
// behind the port waits with no time bound by default, and with the bound
// given otherwise.
func TestNewRouterConsole_theRunnerWaitsAsSet(t *testing.T) {
	for _, want := range []time.Duration{sqlsandbox.NoMaxWait, 45 * time.Second} {
		st := routerSettings{ServersFile: t.TempDir() + "/console-servers.yaml", Token: "t", MaxWait: want}
		_, runner, err := newRouterConsole(st, console.SQLPool{Workers: 2, Threads: 2, Why: "4 cores allow 2"})
		if err != nil {
			t.Fatal(err)
		}
		if got := runner.MaxWait(); got != want {
			t.Errorf("runner.MaxWait() = %v, want %v", got, want)
		}
		runner.Close()
	}
}

// The startup lines say what a statement does while the copy is busy, in
// both shapes, and never promise an error that is not coming.
func TestRouterStartupLines_sayTheWait(t *testing.T) {
	none := routerStartupLines(routerStartup{MaxWait: sqlsandbox.NoMaxWait, Pool: console.SQLPool{Workers: 2, Threads: 2, Why: "4 cores allow 2"}})
	if !strings.Contains(none, "for as long as its client stays connected") || strings.Contains(none, "for up to") {
		t.Errorf("no time bound is not said:\n%s", none)
	}
	bound := routerStartupLines(routerStartup{MaxWait: 45 * time.Second, Pool: console.SQLPool{Workers: 2, Threads: 2, Why: "4 cores allow 2"}})
	if !strings.Contains(bound, "for up to 45s (--max-wait); past that the client gets an error (1040 on a routed connection, 1203 on one that is not)") {
		t.Errorf("the bound is not said:\n%s", bound)
	}
}
