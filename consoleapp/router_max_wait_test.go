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

// --concurrency: empty and 0 leave the number to the machine, a whole number
// up to the most the router runs is taken, and the rest is refused with the
// range.
func TestParseRouterConcurrency(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    int
		wantErr string
	}{
		{in: "", want: 0},
		{in: "  ", want: 0},
		{in: "0", want: 0},
		{in: "1", want: 1},
		{in: " 16 ", want: 16},
		{in: "+8", want: 8},
		{in: "64", want: 64},
		{in: "65", wantErr: "more than 64"},
		{in: "256", wantErr: "more than 64"},
		{in: "-1", wantErr: "negative"},
		{in: "1.5", wantErr: "not a whole number"},
		{in: "eight", wantErr: "not a whole number"},
		{in: "8 workers", wantErr: "not a whole number"},
		{in: "99999999999999999999", wantErr: "not a whole number"},
	} {
		got, err := parseRouterConcurrency(tc.in)
		switch {
		case tc.wantErr != "":
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("parseRouterConcurrency(%q) = %v, %v; want an error saying %q", tc.in, got, err, tc.wantErr)
			}
		case err != nil || got != tc.want:
			t.Errorf("parseRouterConcurrency(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}
}

// Unset the machine decides, the flag wins over the environment, and a bad
// value stops the start. The capture process's own variable is not the
// router's, and the notice about it names the one that is.
func TestRouterSettings_concurrency(t *testing.T) {
	t.Setenv("BINTRAIL_CONSOLE_TOKEN", "t")
	t.Setenv("BINTRAIL_ROUTER_CONCURRENCY", "")
	t.Setenv("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT", "9")
	st, err := maxWaitSettings(t)
	if err != nil || st.Concurrency != 0 {
		t.Fatalf("unset: Concurrency = %d, %v; want 0, the machine's (the capture process's variable is not read)", st.Concurrency, err)
	}
	if notes := strings.Join(routerIgnoredEnv(), "\n"); !strings.Contains(notes, "BINTRAIL_ROUTER_CONCURRENCY or --concurrency") {
		t.Errorf("the notice about the capture process's variable does not name the router's:\n%s", notes)
	}
	t.Setenv("BINTRAIL_ROUTER_CONCURRENCY", "12")
	if st, err = maxWaitSettings(t); err != nil || st.Concurrency != 12 {
		t.Fatalf("env: Concurrency = %d, %v; want 12", st.Concurrency, err)
	}
	if st, err = maxWaitSettings(t, "--concurrency", "3"); err != nil || st.Concurrency != 3 {
		t.Fatalf("flag over env: Concurrency = %d, %v; want 3", st.Concurrency, err)
	}
	t.Setenv("BINTRAIL_ROUTER_CONCURRENCY", "200")
	if _, err = maxWaitSettings(t); err == nil || !strings.Contains(err.Error(), "more than 64") {
		t.Fatalf("env 200: err = %v; want a refusal that says the most", err)
	}
}

// The number the operator set is the number of workers the router starts
// with, on whatever machine this is; unset, it is the machine's.
func TestRouterPool_takesTheNumberSet(t *testing.T) {
	limit := sqlsandbox.DefaultLimits().MemoryLimit
	for _, n := range []int{1, 7, 64} {
		if got := routerPool(routerSettings{Concurrency: n}, limit); got.Workers != n || got.Threads < 1 || !strings.Contains(got.Why, "as set") {
			t.Errorf("Concurrency %d: %d workers x %d threads (%s), want %d workers", n, got.Workers, got.Threads, got.Why, n)
		}
	}
	auto := routerPool(routerSettings{}, limit)
	if auto.Workers < 1 || strings.Contains(auto.Why, "as set") {
		t.Errorf("unset: %d workers (%s), want the machine's own number", auto.Workers, auto.Why)
	}
}

// The startup lines say one thread in the singular, and a pool that can ask
// for more memory than there is gets a line of its own that begins with
// WARNING, where it is not the tail of the sentence before it.
func TestRouterStartupLines_sayTheNumberSet(t *testing.T) {
	got := routerStartupLines(routerStartup{MaxWait: sqlsandbox.NoMaxWait, MemoryLimit: "2048MiB",
		Pool: console.SQLPool{Workers: 8, Threads: 1, Why: "as set (the machine alone gives 3)", Warning: "8 statements at up to 2.5 GiB each can reach 20.5 GiB"}})
	for _, want := range []string{
		"Router: 8 statements on the copy at once, 1 thread and up to 2048MiB of memory each: as set (the machine alone gives 3).\n",
		"\nRouter: WARNING: 8 statements at up to 2.5 GiB each can reach 20.5 GiB.\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the startup lines do not say %q:\n%s", want, got)
		}
	}
	quiet := routerStartupLines(routerStartup{MaxWait: sqlsandbox.NoMaxWait, Pool: console.SQLPool{Workers: 2, Threads: 2, Why: "4 cores allow 2"}})
	if strings.Contains(quiet, "WARNING") || !strings.Contains(quiet, "2 threads") {
		t.Errorf("a pool with nothing to warn about:\n%s", quiet)
	}
}
