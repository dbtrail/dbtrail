package consoleapp

import (
	"runtime"
	"strings"
	"testing"
)

// #3321: with --threads auto, the vCPUs shared among the statements that can
// run at once (workers plus the one kept for short statements), rounded up.
func TestPGAutoThreads(t *testing.T) {
	for _, c := range []struct{ vcpus, slots, want int }{
		{8, 5, 2},   // the defaults on the recommended machine: 2, as before
		{8, 3, 3},   // --workers 2 on 8 vCPUs, the boards' shape
		{16, 5, 4},  // 16 vCPUs, default workers
		{16, 3, 6},  // 16 vCPUs, --workers 2: the measured best on c7i.4xlarge
		{4, 2, 2},   // --workers 1 on 4 vCPUs
		{2, 3, 1},   // a CPU limit of 1.5, which GOMAXPROCS rounds up to 2
		{1, 5, 1},   // one vCPU
		{2, 17, 1},  // more statements at once than vCPUs: one thread each
		{64, 2, 16}, // never past --threads' own ceiling
		{0, 0, 1},   // nothing known: one thread, never zero
	} {
		if got := pgAutoThreads(c.vcpus, c.slots); got != c.want {
			t.Errorf("pgAutoThreads(%d vCPUs, %d at once) = %d, want %d", c.vcpus, c.slots, got, c.want)
		}
		if got := pgAutoThreads(c.vcpus, c.slots); c.vcpus >= c.slots && c.slots > 0 && got*c.slots < c.vcpus && got < maxPGRouterThreads {
			t.Errorf("%d vCPUs, %d at once: %d threads each leave vCPUs idle", c.vcpus, c.slots, got)
		}
	}
}

func setVCPUs(t *testing.T, n int) {
	orig := pgRouterVCPUs
	t.Cleanup(func() { pgRouterVCPUs = orig })
	pgRouterVCPUs = func() int { return n }
}

// A number the operator set means exactly what it meant before: only unset,
// empty or auto is worked out.
func TestPGRouterThreadsFrom(t *testing.T) {
	setVCPUs(t, 8)
	for _, c := range []struct {
		flag    string
		changed bool
		env     string
		workers int
		want    int
		auto    bool
		err     string
	}{
		{"auto", false, "", 4, 2, true, ""},
		{"auto", false, "   ", 4, 2, true, ""},
		{"auto", false, "auto", 2, 3, true, ""},
		{"auto", false, " AUTO ", 2, 3, true, ""},
		{"auto", false, "4", 1, 4, false, ""},
		{"auto", false, " 4 ", 2, 4, false, ""},
		{"auto", false, "1", 16, 1, false, ""},
		{"3", true, "5", 2, 3, false, ""},
		{"auto", true, "5", 2, 3, true, ""},
		{"16", true, "", 4, 16, false, ""},
		{"010", true, "", 4, 8, false, ""}, // the flag reads as Go's integer flags always did
		{"0x4", true, "", 4, 4, false, ""},
		{"auto", false, "010", 4, 10, false, ""}, // the variable, in base 10 as before
		{"", true, "", 4, 2, true, ""},
		{" ", true, "", 2, 3, true, ""},
		{"auto", false, "0", 4, 0, false, "BINTRAIL_PG_ROUTER_THREADS must be between 1 and 16, or auto"},
		{"auto", false, "17", 4, 0, false, "BINTRAIL_PG_ROUTER_THREADS must be between 1 and 16, or auto"},
		{"auto", false, "many", 4, 0, false, `BINTRAIL_PG_ROUTER_THREADS="many" is not a whole number or auto`},
		{"auto", false, "2.5", 4, 0, false, "is not a whole number or auto"},
		{"-1", true, "", 4, 0, false, "--threads must be between 1 and 16"},
		{"lots", true, "", 4, 0, false, `--threads "lots" is not a whole number or auto`},
	} {
		t.Setenv("BINTRAIL_PG_ROUTER_THREADS", c.env)
		got, vcpus, err := pgRouterThreadsFrom(c.flag, c.changed, "BINTRAIL_PG_ROUTER_THREADS", c.workers)
		auto := vcpus > 0
		if auto && vcpus != 8 {
			t.Errorf("%+v: auto from %d vCPUs, want the 8 counted", c, vcpus)
		}
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("%+v: err %v, want %q", c, err, c.err)
			}
			continue
		}
		if err != nil || got != c.want || auto != c.auto {
			t.Errorf("%+v: got %d auto %v err %v", c, got, auto, err)
		}
	}
}

// Through the real flags: the default is auto, from the process's own vCPUs.
func TestPGRouterSettings_threadsAutoByDefault(t *testing.T) {
	clearPGRouterEnv(t)
	st, err := pgRouterSettingsFrom(pgRouterTestCmd(t))
	if err != nil {
		t.Fatal(err)
	}
	want := pgAutoThreads(runtime.GOMAXPROCS(0), pgCopySlots(st.Workers))
	if !st.ThreadsAuto || st.Threads != want || st.ThreadsVCPUs != runtime.GOMAXPROCS(0) {
		t.Fatalf("default threads: %+v, want auto %d", st, want)
	}
	st, err = pgRouterSettingsFrom(pgRouterTestCmd(t, "--threads", "4", "--workers", "1"))
	if err != nil || st.ThreadsAuto || st.Threads != 4 || st.Workers != 1 {
		t.Fatalf("--threads 4 --workers 1: %+v, %v", st, err)
	}
}

// The start-up lines say whether the threads were worked out and from what.
func TestPGRouterThreadsWords(t *testing.T) {
	t.Setenv("GOMAXPROCS", "")
	auto := pgRouterThreadsWords(pgRouterSettings{Workers: 2, Threads: 3, ThreadsAuto: true, ThreadsVCPUs: 8})
	if auto != "3 threads (auto at start: 8 vCPUs over 3 statements at once, the workers and the one kept for short statements)" {
		t.Errorf("auto: %q", auto)
	}
	if capped := pgRouterThreadsWords(pgRouterSettings{Workers: 1, Threads: 16, ThreadsAuto: true, ThreadsVCPUs: 64}); capped != "16 threads (auto at start: 64 vCPUs over 2 statements at once, the workers and the one kept for short statements, at most 16)" {
		t.Errorf("capped: %q", capped)
	}
	if exact := pgRouterThreadsWords(pgRouterSettings{Workers: 1, Threads: 16, ThreadsAuto: true, ThreadsVCPUs: 32}); strings.Contains(exact, "at most") {
		t.Errorf("32 vCPUs over 2 is 16 exactly, not cut: %q", exact)
	}
	t.Setenv("GOMAXPROCS", "6")
	if g := pgRouterThreadsWords(pgRouterSettings{Workers: 2, Threads: 2, ThreadsAuto: true, ThreadsVCPUs: 6}); !strings.Contains(g, "GOMAXPROCS=6 over 3 statements") {
		t.Errorf("GOMAXPROCS from the environment: %q", g)
	}
	t.Setenv("GOMAXPROCS", "abc")
	if g := pgRouterThreadsWords(pgRouterSettings{Workers: 2, Threads: 3, ThreadsAuto: true, ThreadsVCPUs: 8}); strings.Contains(g, "GOMAXPROCS") {
		t.Errorf("a GOMAXPROCS Go ignored is named: %q", g)
	}
	t.Setenv("GOMAXPROCS", "")
	if one := pgRouterThreadsWords(pgRouterSettings{Workers: 4, Threads: 1, ThreadsAuto: true, ThreadsVCPUs: 1}); !strings.Contains(one, "1 vCPU over") {
		t.Errorf("one vCPU: %q", one)
	}
	if set := pgRouterThreadsWords(pgRouterSettings{Workers: 2, Threads: 4}); set != "4 threads" {
		t.Errorf("set: %q", set)
	}
	out := pgRouterStartupLines(pgRouterSettings{Listen: "127.0.0.1:5433", Workers: 2, Threads: 3, ThreadsAuto: true, ThreadsVCPUs: 8, MaxConns: 16}, "2GB", "the default", nil, 1, true)
	if !strings.Contains(out, "3 threads (auto at start: 8 vCPUs over 3 statements at once") || !strings.Contains(out, "while --threads is auto") {
		t.Errorf("the start-up lines do not say how the threads were set:\n%s", out)
	}
}
