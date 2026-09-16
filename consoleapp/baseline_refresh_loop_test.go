package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func TestBaselineRefreshTargets(t *testing.T) {
	entries := []console.ServerEntry{
		{ID: "a", Name: "prod", DSN: "dsn-a", BaselineDir: "/b/a"},
		// S3-only destination: a refresh reads and writes snapshot FILES, so it
		// cannot refresh in place. Skipped, and warned about — silently doing
		// nothing is what an operator would misread as "it's working".
		{ID: "b", Name: "s3only", DSN: "dsn-b", BaselineS3: "s3://bucket/baselines/"},
		// No baseline destination at all.
		{ID: "c", Name: "nobaseline", DSN: "dsn-c"},
		// A view-only entry with no index DSN.
		{ID: "d", Name: "viewonly", BaselineDir: "/b/d"},
	}

	got, skipped := baselineRefreshTargets(entries, "boot-dsn", "/b/boot")
	want := map[string]string{"default": "/b/boot", "a": "/b/a"}
	if len(got) != len(want) {
		t.Fatalf("got %d target(s) %+v, want %d", len(got), got, len(want))
	}
	for _, r := range got {
		if want[r.ServerID] != r.BaselineDir {
			t.Errorf("target %q = %q, want %q", r.ServerID, r.BaselineDir, want[r.ServerID])
		}
	}
	// The skip is REPORTED, not just absent (#1579): the same computation
	// answers GET /api/baseline-refresh, and "covers every server" was wrong
	// exactly because this server vanished without a count. The no-baseline
	// and no-DSN entries are NOT in it: they are unconfigured, not skipped
	// for a reason the card should name.
	if len(skipped) != 1 || skipped[0] != "s3only" {
		t.Errorf("skippedS3Only = %v, want exactly [s3only]", skipped)
	}
}

// TestBaselineRefreshTargets_bootNeedsBoth: the boot entry is only a target when
// the daemon has both halves — a --baseline-dir with no --index-dsn (or the
// reverse) has nothing to fold.
func TestBaselineRefreshTargets_bootNeedsBoth(t *testing.T) {
	if got, _ := baselineRefreshTargets(nil, "", "/b/boot"); len(got) != 0 {
		t.Errorf("targets without an index DSN = %+v, want none", got)
	}
	if got, _ := baselineRefreshTargets(nil, "boot-dsn", ""); len(got) != 0 {
		t.Errorf("targets without a baseline dir = %+v, want none", got)
	}
}

// TestStartBaselineRefreshLoop_startupContract pins WHICH startup conditions
// refuse and which only warn — the distinction is not cosmetic.
//
// A malformed interval is the operator's typo and can only ever be a typo, so it
// refuses. "No server is refreshable yet" is NOT: every tick recomputes the
// target set, a source-less `watch` starts with no servers at all and gains them
// from the console, and per-server baseline directories live in the registry.
// Refusing there would mean a compose file carrying the interval cannot boot a
// fresh install — the operator would have to add a server through a console that
// refused to start. It warns instead.
func TestStartBaselineRefreshLoop_startupContract(t *testing.T) {
	// Cancellable: the configured cases start a real ticker goroutine, and a
	// Background context would leak one per sub-test.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)

	for _, tc := range []struct {
		name     string
		sup      *baselineSupervisor
		interval string
		dsn, dir string
		wantErr  string
	}{
		{"disabled by default", sup, "", "", "", ""},
		{"unparseable interval", sup, "sometimes", "d", "/b", "--baseline-refresh-interval"},
		{"no supervisor wired", nil, "6h", "d", "/b", "without a baseline supervisor"},
		{"nothing refreshable yet: warns, starts anyway", sup, "6h", "", "", ""},
		{"configured", sup, "6h", "dsn", "/b", ""},
		// The wiring, not the parser: minutes reach the loop only because the
		// flag stopped going through ParseRetain, which accepts hours and days
		// only. Parsing "15m" correctly in cliutil proves nothing about which
		// parser this call site actually reaches (#1469).
		{"minutes are an interval", sup, "15m", "dsn", "/b", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := startBaselineRefreshLoop(ctx, nil, tc.sup, tc.dsn, tc.dir, tc.interval, false)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr == "":
			case err == nil:
				t.Fatalf("expected an error containing %q", tc.wantErr)
			case !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestBaselineSupervisor_singleFlightIsShared is the invariant that keeps a
// refresh from folding a snapshot another job is writing underneath it. The two
// job kinds have separate status slots but ONE lock.
func TestBaselineSupervisor_singleFlightIsShared(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)

	// A dump in flight blocks a refresh for the same server...
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	if err := sup.TriggerRefresh(refreshRequest{ServerID: "a", IndexDSN: "d", BaselineDir: "/b"}, 0); err != console.ErrBaselineRunning {
		t.Fatalf("TriggerRefresh during a dump = %v, want ErrBaselineRunning", err)
	}
	// ...and not for a different one.
	if !sup.busyLocked("a") || sup.busyLocked("b") {
		t.Fatal("the single-flight is not per-server")
	}

	// A refresh in flight blocks a dump.
	sup.jobs = map[string]*console.BaselineStatus{}
	sup.refreshes["a"] = &console.BaselineStatus{State: "running"}
	if err := sup.Trigger(console.BaselineRequest{ServerID: "a"}); err != console.ErrBaselineRunning {
		t.Fatalf("Trigger during a refresh = %v, want ErrBaselineRunning", err)
	}
}

// TestBaselineSupervisor_statusSlotsAreSeparate: a manual dump must not erase
// the evidence that the automatic refresh has been failing.
func TestBaselineSupervisor_statusSlotsAreSeparate(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["a"] = &console.BaselineStatus{State: "failed", LastError: "capture gap"}
	sup.jobs["a"] = &console.BaselineStatus{State: "succeeded"}

	if got := sup.RefreshStatus("a"); got.State != "failed" || got.LastError != "capture gap" {
		t.Fatalf("RefreshStatus = %+v; a successful dump overwrote the refresh verdict", got)
	}
	if got := sup.Status("a"); got.State != "succeeded" {
		t.Fatalf("Status = %+v, want the dump's own verdict", got)
	}
	if got := sup.RefreshStatus("never-run"); got.State != "idle" {
		t.Fatalf("RefreshStatus for an unknown server = %+v, want idle", got)
	}
}

// TestRunBaselineRefreshCycle_survivesAPanic: this loop must never be able to
// take down a daemon that is also streaming replication. A refresh that stops is
// a degradation; a daemon that stops capturing is an outage.
//
// Scope, because the name is broader than the test: this covers the DISPATCH
// half only. Its panic is raised by a nil-map write inside TriggerRefresh,
// which runs synchronously, and runBaselineRefreshCycle's recover sits on the
// near side of the `go` that follows. The fold itself is guarded separately
// and covered by TestBaselineJobGoroutines_survivePanicAndReportFailure
// (#1472); reading this one as covering both is what left the fold unguarded.
func TestRunBaselineRefreshCycle_survivesAPanic(t *testing.T) {
	// A nil supervisor makes TriggerRefresh panic on the nil map write; the
	// cycle's recover must contain it.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a panic escaped the refresh cycle: %v", r)
		}
	}()
	runBaselineRefreshCycle(context.Background(), nil, &baselineSupervisor{}, "dsn", "/b", 0, false)
}

// TestRunBaselineRefreshCycle_stopsOnCancel: shutdown must not start new work.
func TestRunBaselineRefreshCycle_stopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	runBaselineRefreshCycle(ctx, nil, sup, "dsn", "/b", 0, false)
	if got := sup.RefreshStatus("default"); got.State != "idle" {
		t.Fatalf("a cancelled cycle started work: %+v", got)
	}
}

func TestSnapshotsPer30Days(t *testing.T) {
	for _, tc := range []struct {
		interval time.Duration
		want     int64
	}{
		{5 * time.Minute, 8640},
		{15 * time.Minute, 2880},
		{time.Hour, 720},
		{24 * time.Hour, 30},
		// The regime a per-DAY projection got wrong: integer division sent
		// every interval longer than a day to zero, so a 7d refresh reported
		// "0 snapshots per day", which reads as none and is the opposite of
		// what it means.
		{7 * 24 * time.Hour, 4},
		{29 * 24 * time.Hour, 1},
		{30 * 24 * time.Hour, 1}, // the exact horizon: the last interval that still projects
		// Past the horizon there is no honest integer, so the caller is told
		// to omit the figure rather than print a zero.
		{31 * 24 * time.Hour, 0},
		{0, 0}, // guarded: a zero interval never reaches here, and must not divide
	} {
		if got := snapshotsPer30Days(tc.interval); got != tc.want {
			t.Errorf("snapshotsPer30Days(%v) = %d, want %d", tc.interval, got, tc.want)
		}
	}
}

// A projection that rounds to zero must not be logged at all: "0 snapshots"
// states the opposite of the truth about a long interval, and the interval
// logged beside it already says what the rate is.
func TestDiskArgs_omitsAnUnmeaningfulProjection(t *testing.T) {
	// Values, not just keys: a projection that is present but wrong reads as
	// authoritative, and a key-only assertion cannot tell the two apart.
	val := func(args []any, key string) (any, bool) {
		for i := 0; i+1 < len(args); i += 2 {
			if k, ok := args[i].(string); ok && k == key {
				return args[i+1], true
			}
		}
		return nil, false
	}
	const projection = "full_table_snapshots_per_server_per_30d"

	short := diskArgs(15*time.Minute, nil)
	got, ok := val(short, projection)
	if !ok {
		t.Fatalf("a 15m interval logged no projection: %v", short)
	}
	if got != int64(2880) {
		t.Errorf("15m projection = %v, want 2880", got)
	}
	// Past the horizon there is no honest integer, and "0" states the opposite
	// of the truth about a long interval.
	long := diskArgs(90*24*time.Hour, nil)
	if v, ok := val(long, projection); ok {
		t.Errorf("a 90d interval logged a projection of %v, which rounds to zero", v)
	}
	for _, args := range [][]any{short, long} {
		if _, ok := val(args, "interval"); !ok {
			t.Errorf("disk warning lost the interval it always carried: %v", args)
		}
		if _, ok := val(args, "dirs"); !ok {
			t.Errorf("disk warning lost the dirs it always carried: %v", args)
		}
	}
}

// TestRunBaselineRefreshCycle_countsAServerItCouldNotStart pins the mechanism
// that REPLACED a broken one, so it is worth saying what was broken.
//
// The first version of this feature timed the dispatch loop and called the
// result "how long the cycle took". TriggerRefresh ends in
// `go s.runRefresh(...)` and returns, so that span is goroutine launch:
// microseconds, whatever the refresh costs. Its overrun warning could never
// fire and the line beside it announced a cycle had finished while the fold
// was still running. CI was green throughout, because the only test drove the
// pure comparison with hand-written durations and never touched the call site.
//
// A skip is the loop-level evidence of the same condition, observed rather
// than timed: a server still busy when the next tick arrives IS a refresh that
// outran the interval. Seeding the busy slot directly is how the sibling
// single-flight test does it, and it makes this deterministic where timing a
// real fold would not be.
func TestRunBaselineRefreshCycle_countsAServerItCouldNotStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)

	// "default" is the boot target's id; a dump in flight for it makes the
	// shared single-flight refuse the refresh.
	sup.jobs["default"] = &console.BaselineStatus{State: "running"}

	dispatched, skipped, _ := runBaselineRefreshCycle(ctx, nil, sup, "dsn", t.TempDir(), time.Minute, false)
	if dispatched != 0 || skipped != 1 {
		t.Fatalf("cycle reported dispatched=%d skipped=%d, want 0 and 1: a busy server must be COUNTED, "+
			"not swallowed at Debug where the default log level hides it", dispatched, skipped)
	}
}

// reportRefreshDuration is per REFRESH, not per tick, so it is the only place
// an overrun can be detected at all: see the test above for why timing the
// dispatch loop cannot.
func TestReportRefreshDuration_warnsOnlyOnOverrun(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	// Window zero throughout: this test is about the interval threshold, and
	// which READING an overrun gets is TestGradeRefresh's subject.
	capture := func(level slog.Level, interval, took time.Duration) string {
		var buf bytes.Buffer
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level})))
		reportRefreshDuration("srv", refreshRun{interval: interval, took: took}, refreshPace{})
		return buf.String()
	}

	if out := capture(slog.LevelWarn, time.Hour, 5*time.Minute); out != "" {
		t.Errorf("a refresh inside its interval warned: %q", out)
	}
	if out := capture(slog.LevelWarn, time.Hour, time.Hour); out != "" {
		t.Errorf("a refresh exactly at its interval warned: %q", out)
	}
	// A manual refresh has no interval to be measured against and must not warn.
	if out := capture(slog.LevelWarn, 0, 12*time.Minute); out != "" {
		t.Errorf("a refresh with no configured interval warned: %q", out)
	}

	// Captured at Debug so the quiet branch is visible to the test. At Warn the
	// non-overrun path and a DELETED non-overrun path are the same empty
	// string, which is how a whole branch stays unguarded.
	quiet := capture(slog.LevelDebug, time.Hour, 5*time.Minute)
	for _, want := range []string{"level=DEBUG", "took=5m", "server=srv"} {
		if !strings.Contains(quiet, want) {
			t.Errorf("the non-overrun line lost %q: %q", want, quiet)
		}
	}

	out := capture(slog.LevelWarn, 5*time.Minute, 12*time.Minute)
	if out == "" {
		t.Fatal("a refresh that outran its interval said nothing")
	}
	for _, want := range []string{"level=WARN", "took=12m", "interval=5m", "server=srv"} {
		if !strings.Contains(out, want) {
			t.Errorf("overrun warning does not carry %q, so it cannot be acted on: %q", want, out)
		}
	}
}

// A skipped tick is the loop-level evidence that refreshes are not keeping up.
// It used to go to slog.Debug, below the console binary's default level, which
// at a short interval hides the dominant path rather than an edge case.
func TestReportDispatch_visibleOnlyWhenSomethingWasSkipped(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	capture := func(dispatched, skipped int) string {
		var buf bytes.Buffer
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
		reportDispatch(time.Minute, dispatched, skipped, false)
		return buf.String()
	}

	if out := capture(3, 0); out != "" {
		t.Errorf("a healthy tick logged at Info; at a 1m interval that is a line a minute forever: %q", out)
	}
	out := capture(1, 2)
	if out == "" {
		t.Fatal("a tick that skipped a server said nothing at the default level")
	}
	for _, want := range []string{"skipped=2", "dispatched=1", "interval=1m"} {
		if !strings.Contains(out, want) {
			t.Errorf("dispatch line lost %q: %q", want, out)
		}
	}
}

// TestRunRefresh_doesNotAdviseTuningWhenNothingWasPublished reaches the CALL
// SITE, which is the whole point: the previous version of this guard drove
// reportRefreshDuration with hand-written durations and so could not see that
// the call site invoked it unconditionally, including for runs that refused.
// That is the same shape this PR indicts one function over.
//
// The interval is one nanosecond, so any real elapsed time exceeds it. An
// unconditional call therefore WARNS here, and the warning would tell an
// operator to raise the interval for a refresh that failed because it had no
// baseline to fold.
func TestRunRefresh_doesNotAdviseTuningWhenNothingWasPublished(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}

	// An empty baseline dir makes executeRefresh refuse: nothing to fold.
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: t.TempDir()},
		time.Now().UTC(), time.Nanosecond)

	out := buf.String()
	if !strings.Contains(out, "published nothing") {
		t.Fatalf("the refusal itself was not reported, so this test is not exercising the path it claims: %q", out)
	}
	if strings.Contains(out, "took longer than the configured interval") {
		t.Errorf("a refresh that published nothing was given scheduling advice; the capture gap or schema "+
			"change that stopped it is not fixed by raising the interval: %q", out)
	}
}

// The tick binds the two counters and hands them on, and it is the only place
// that happens. Before the seam existed the whole binding lived inside the
// ticker's closure, so swapping the arguments compiled and every test stayed
// green: both are ints, and the cycle and the reporter were only ever driven
// apart.
func TestRefreshTick_reportsTheCountersInTheRightOrder(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.jobs["default"] = &console.BaselineStatus{State: "running"}

	refreshTick(ctx, nil, sup, "dsn", t.TempDir(), time.Minute, false)

	out := buf.String()
	if !strings.Contains(out, "skipped=1") || !strings.Contains(out, "dispatched=0") {
		t.Errorf("the tick reported the counters swapped or not at all; the one busy server must read "+
			"skipped=1 dispatched=0: %q", out)
	}
}

// effectiveCarryForward is what makes the console panel able to change a
// RUNNING loop, so its precedence is the contract worth pinning: a saved
// override wins, absence falls back to the daemon flag, and an override that
// says false must beat a daemon flag that says true (which is why the registry
// stores a pointer and not a bare bool).
func TestEffectiveCarryForward(t *testing.T) {
	reg := func(t *testing.T, set *bool) *console.Registry {
		t.Helper()
		r, err := console.LoadRegistry(filepath.Join(t.TempDir(), "servers.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if set != nil {
			if err := r.SetBaselineRefresh(&console.BaselineRefreshConfig{CarryForwardUnchanged: *set}); err != nil {
				t.Fatal(err)
			}
		}
		return r
	}
	yes, no := true, false

	for _, tc := range []struct {
		name          string
		override      *bool
		daemonDefault bool
		want          bool
	}{
		{"no override falls back to the daemon flag (on)", nil, true, true},
		{"no override falls back to the daemon flag (off)", nil, false, false},
		{"an override wins when it says on", &yes, false, true},
		// The case a bare bool in the registry could not express.
		{"an override that says off beats a daemon flag that says on", &no, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveCarryForward(reg(t, tc.override), tc.daemonDefault); got != tc.want {
				t.Errorf("effectiveCarryForward = %v, want %v", got, tc.want)
			}
		})
	}

	// A registry that is not there at all is not consent to change behaviour:
	// the operator's own command line is a better answer than a silent no.
	if !effectiveCarryForward(nil, true) {
		t.Error("a nil registry discarded the daemon flag")
	}
}

// The wiring, not the resolver: the value effectiveCarryForward computes has to
// reach the requests the fold is built from. A resolver that is correct in
// isolation proves nothing about whether the loop consults it.
func TestRefreshTargetsFor_carriesTheEffectiveSettingIntoEveryRequest(t *testing.T) {
	// Every leg runs against TWO servers, and every leg that asserts the
	// setting reached the requests has a twin asserting TRUE. Both matter.
	// With one target, gating the assignment on i == 0 is invisible. With the
	// expected value false, so is dropping the assignment entirely: false is
	// the zero value, so the field is already right for the wrong reason. The
	// combination is what catches a daemon whose second and third servers
	// silently ignore the flag and the console toggle.
	newReg := func(t *testing.T, override *bool) *console.Registry {
		t.Helper()
		r, err := console.LoadRegistry(filepath.Join(t.TempDir(), "servers.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if override != nil {
			if err := r.SetBaselineRefresh(&console.BaselineRefreshConfig{CarryForwardUnchanged: *override}); err != nil {
				t.Fatal(err)
			}
		}
		for _, name := range []string{"prod", "staging"} {
			if _, err := r.Add(console.ServerEntry{
				Name: name, DSN: "u:p@tcp(h:3306)/idx", BaselineDir: t.TempDir(),
			}); err != nil {
				t.Fatal(err)
			}
		}
		return r
	}
	yes, no := true, false

	for _, tc := range []struct {
		name     string
		override *bool
		daemon   bool
		want     bool
	}{
		{"daemon flag on, nothing saved", nil, true, true},
		{"daemon flag off, nothing saved", nil, false, false},
		{"override on beats a flag saying off", &yes, false, true},
		{"override off beats a flag saying on", &no, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reqs := refreshTargetsFor(newReg(t, tc.override), "dsn", "/b", tc.daemon)
			if len(reqs) < 2 {
				t.Fatalf("got %d targets, need at least 2 or a per-index bug stays invisible", len(reqs))
			}
			for i, req := range reqs {
				if req.CarryForwardUnchanged != tc.want {
					t.Errorf("target %d (%q): CarryForwardUnchanged = %v, want %v",
						i, req.ServerName, req.CarryForwardUnchanged, tc.want)
				}
			}
		})
	}

	// A nil registry is the source-less shape; it must still carry the flag.
	for _, want := range []bool{true, false} {
		reqs := refreshTargetsFor(nil, "dsn", "/b", want)
		if len(reqs) == 0 {
			t.Fatal("no targets, so the assertion below checks nothing")
		}
		for _, req := range reqs {
			if req.CarryForwardUnchanged != want {
				t.Errorf("daemon default %v did not reach the request for %q", want, req.ServerName)
			}
		}
	}
}

// The last hop: what the request carries has to reach the fold's configuration.
// Without this, the whole chain from the console toggle down could be correct
// and the fold still run with the setting off, which is exactly what a mutation
// of this line showed.
func TestRefreshFoldConfig_carriesTheSettingAndKeepsGapsStrict(t *testing.T) {
	for _, want := range []bool{true, false} {
		cfg := refreshFoldConfig(refreshRequest{
			IndexDSN: "dsn", BaselineDir: "/b", CarryForwardUnchanged: want,
		}, time.Now(), []string{"shop.orders"})
		if cfg.CarryForwardUnchanged != want {
			t.Errorf("CarryForwardUnchanged = %v, want %v", cfg.CarryForwardUnchanged, want)
		}
		// Pinned in the same place because it is the same class of setting and
		// the opposite decision: an unattended job never publishes over a known
		// permanent capture loss, whoever asked for what.
		if cfg.AllowGaps {
			t.Error("the refresh loop would publish over a known capture gap")
		}
		if cfg.OutputFormat != reconstruct.OutputFormatParquet {
			t.Errorf("OutputFormat = %q, want parquet", cfg.OutputFormat)
		}
	}
}

// TestCountReuse: the reuse tally is the only confirmation an operator gets
// that the opt-in did anything, so it is derived from the per-table reports and
// never from the setting. Asking for reuse is not getting it: a table with
// changes, with a capture gap, or on the S3 path is folded anyway. The copied
// half narrows further: a reuse that fell back to a byte copy saved no disk,
// and folding it into the reused count is how the console came to confirm a
// saving the daemon log denied (#1578).
func TestCountReuse(t *testing.T) {
	rep := func(carried, linked bool) *reconstruct.TableReport {
		return &reconstruct.TableReport{CarriedForward: carried, CarriedByLink: linked}
	}
	cases := []struct {
		name    string
		reports []*reconstruct.TableReport
		want    reuseTally
	}{
		{"nothing folded", nil, reuseTally{}},
		{"every table rewritten", []*reconstruct.TableReport{rep(false, false), rep(false, false)}, reuseTally{}},
		{"every table reused by link", []*reconstruct.TableReport{rep(true, true), rep(true, true), rep(true, true)}, reuseTally{reused: 3}},
		{"mixed, which is the normal case", []*reconstruct.TableReport{rep(true, true), rep(false, false), rep(true, true)}, reuseTally{reused: 2}},
		{"a nil report is not a reuse", []*reconstruct.TableReport{rep(true, true), nil}, reuseTally{reused: 1}},
		{"a copied reuse counts in BOTH halves", []*reconstruct.TableReport{rep(true, false), rep(true, true)}, reuseTally{reused: 2, copied: 1}},
		// CarriedByLink without CarriedForward is a shape fulltable.go never
		// emits; if it ever appears, a link that was not a reuse must not
		// subtract from anything.
		{"linked-but-not-carried is not a reuse", []*reconstruct.TableReport{rep(false, true)}, reuseTally{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := countReuse(tc.reports); got != tc.want {
				t.Errorf("countReuse = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestCarryForwardProvenance: the value AND where it came from. The provenance
// is the point: a saved override of false beats a command line saying true, and
// without a name for that an operator watching every table get rewritten has
// nothing anywhere telling them why.
func TestCarryForwardProvenance(t *testing.T) {
	cases := []struct {
		name       string
		override   *bool
		daemon     bool
		wantOn     bool
		wantSource string
	}{
		{"no registry at all falls back to the flag", nil, true, true, "daemon flag or environment"},
		{"no override, flag off", nil, false, false, "daemon flag or environment"},
		{"override true over a flag saying false", boolPtr(true), false, true, "console setting, which overrides the daemon flag"},
		{"override FALSE over a flag saying true", boolPtr(false), true, false, "console setting, which overrides the daemon flag"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var reg *console.Registry
			if tc.override != nil {
				r, err := console.LoadRegistry(filepath.Join(t.TempDir(), "servers.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				if err := r.SetBaselineRefresh(&console.BaselineRefreshConfig{CarryForwardUnchanged: *tc.override}); err != nil {
					t.Fatal(err)
				}
				reg = r
			}
			on, src := carryForwardProvenance(reg, tc.daemon)
			if on != tc.wantOn {
				t.Errorf("value = %v, want %v", on, tc.wantOn)
			}
			if src != tc.wantSource {
				t.Errorf("source = %q, want %q", src, tc.wantSource)
			}
		})
	}
}

func boolPtr(b bool) *bool { return &b }

// TestEnvBoolOr: the one input an operator can get wrong silently. An
// unparseable value must keep the fallback, never be read as consent.
func TestEnvBoolOr(t *testing.T) {
	const name = "BINTRAIL_TEST_ENV_BOOL_OR"
	cases := []struct {
		raw      string
		fallback bool
		want     bool
	}{
		{"", false, false},
		{"", true, true},
		{"1", false, true},
		{"true", false, true},
		{"TRUE", false, true},
		{"True", false, true},
		{"t", false, true},
		{"  true  ", false, true},
		{"0", true, false},
		{"false", true, false},
		{"F", true, false},
		// Not true/false values. Each must keep the fallback in BOTH
		// directions: a typo can neither turn the setting on nor off.
		{"yes", false, false},
		{"on", false, false},
		{"enabled", false, false},
		{"tru", false, false},
		{"yes", true, true},
		{"off", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.raw+"/"+strconv.FormatBool(tc.fallback), func(t *testing.T) {
			t.Setenv(name, tc.raw)
			if got := envBoolOr(name, tc.fallback); got != tc.want {
				t.Errorf("envBoolOr(%q, fallback=%v) = %v, want %v", tc.raw, tc.fallback, got, tc.want)
			}
		})
	}
}

// TestFoldOutcome: the numbers a fold reports.
//
// This is the hop the console's reused count travels through, and until it was
// split out nothing at the unit tier could reach it: zeroing the count compiled
// and passed the whole suite, because the only caller needs a live index and a
// real baseline.
func TestFoldOutcome(t *testing.T) {
	rep := func(carried, linked bool) *reconstruct.TableReport {
		return &reconstruct.TableReport{CarriedForward: carried, CarriedByLink: linked}
	}
	tables := []string{"shop.orders", "shop.users", "shop.audit"}

	t.Run("clean run reports every table and the reused subset", func(t *testing.T) {
		// One of the two reuses is a COPY (no link), so the tally must carry
		// both halves through this hop or the console's "saved no disk"
		// qualifier never renders (#1578).
		gotT, gotR, reuse, err := foldOutcome(tables,
			[]*reconstruct.TableReport{rep(true, true), rep(false, false), rep(true, false)}, nil, nil)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if gotT != 3 || gotR != 0 || reuse != (reuseTally{reused: 2, copied: 1}) {
			t.Errorf("tables=%d refused=%d reuse=%+v, want 3/0/{reused:2 copied:1}", gotT, gotR, reuse)
		}
	})

	t.Run("a clean run that reused nothing reports zero, not the total", func(t *testing.T) {
		_, _, reuse, _ := foldOutcome(tables,
			[]*reconstruct.TableReport{rep(false, false), rep(false, false), rep(false, false)}, nil, nil)
		if reuse != (reuseTally{}) {
			t.Errorf("reuse=%+v, want zero: every table was rewritten", reuse)
		}
	})

	t.Run("a failed run still reports what the fold did", func(t *testing.T) {
		want := errors.New("capture gap")
		gotT, gotR, reuse, err := foldOutcome(tables,
			[]*reconstruct.TableReport{rep(true, true)},
			[]reconstruct.TableFailure{{}, {}}, want)
		if !errors.Is(err, want) {
			t.Fatalf("err = %v, want the run error", err)
		}
		// Publication is all-or-nothing, so nothing was published; the counts
		// still describe the attempt, and refused comes from the failures.
		if gotT != 3 || gotR != 2 || reuse != (reuseTally{reused: 1}) {
			t.Errorf("tables=%d refused=%d reuse=%+v, want 3/2/{reused:1}", gotT, gotR, reuse)
		}
	})

	t.Run("refused is zero on success even if failures were handed in", func(t *testing.T) {
		// Guards the branch, not the caller: a clean run must report zero
		// refused, so the success path cannot start leaking a failure count.
		_, gotR, _, _ := foldOutcome(tables, nil, []reconstruct.TableFailure{{}}, nil)
		if gotR != 0 {
			t.Errorf("refused=%d on a clean run, want 0", gotR)
		}
	})
}

// TestFoldRunCounts: the DURABLE half of a fold's outcome — the history
// record the Backups page reads long after the live status is overwritten.
// Both callers sit behind a `go` and a live fold, so this mapping is the one
// hop of the reuseTally chain the unit tier can reach; zeroing CarriedCopied
// in either record literal used to pass the whole suite.
func TestFoldRunCounts(t *testing.T) {
	base := console.BaselineRunRecord{Kind: console.BaselineRunRefresh, Uploaded: 4}
	got := foldRunCounts(base, 7, 2, reuseTally{reused: 3, copied: 1})
	if got.Tables != 7 || got.Refused != 2 || got.Carried != 3 || got.CarriedCopied != 1 {
		t.Errorf("counts = tables:%d refused:%d carried:%d copied:%d, want 7/2/3/1",
			got.Tables, got.Refused, got.Carried, got.CarriedCopied)
	}
	if got.Kind != console.BaselineRunRefresh || got.Uploaded != 4 {
		t.Errorf("the mapping disturbed fields it does not own: %+v", got)
	}
	// Zeroes are written, not skipped: the fields are omitempty on the wire
	// and the record is built fresh, but the mapping must not depend on that.
	zero := foldRunCounts(console.BaselineRunRecord{Tables: 9, Carried: 5, CarriedCopied: 5}, 0, 0, reuseTally{})
	if zero.Tables != 0 || zero.Carried != 0 || zero.CarriedCopied != 0 {
		t.Errorf("zero counts did not overwrite: %+v", zero)
	}
}

// TestApplyFoldStatus: what the console polls after a fold finishes.
//
// Both callers sit behind a `go` and a live fold, so this was unreachable at
// the unit tier and existed as two byte-identical copies. Dropping the reused
// count from either compiled and passed everything.
func TestApplyFoldStatus(t *testing.T) {
	t.Run("a clean run reports every count and clears the previous error", func(t *testing.T) {
		st := &console.BaselineStatus{State: "failed", LastError: "the previous run's gap"}
		applyFoldStatus(st, 7, 0, reuseTally{reused: 3, copied: 2}, nil)
		if st.State != "succeeded" {
			t.Errorf("State = %q, want succeeded", st.State)
		}
		if st.LastError != "" {
			t.Errorf("LastError = %q, want cleared: a stale error outlives the run that caused it", st.LastError)
		}
		if st.Tables != 7 || st.Refused != 0 || st.Carried != 3 || st.CarriedCopied != 2 {
			t.Errorf("tables=%d refused=%d carried=%d copied=%d, want 7/0/3/2",
				st.Tables, st.Refused, st.Carried, st.CarriedCopied)
		}
		if st.FinishedAt == "" {
			t.Error("FinishedAt is empty, so the console cannot say when this ran")
		}
	})

	t.Run("a failed run keeps the counts and names the error", func(t *testing.T) {
		st := &console.BaselineStatus{State: "running"}
		applyFoldStatus(st, 7, 2, reuseTally{reused: 1}, errors.New("capture gap at 2026-01-01T00:00:00Z"))
		if st.State != "failed" {
			t.Errorf("State = %q, want failed", st.State)
		}
		if st.LastError == "" {
			t.Error("LastError is empty on a failed run, so the console shows a failure with no cause")
		}
		if st.Tables != 7 || st.Refused != 2 || st.Carried != 1 {
			t.Errorf("tables=%d refused=%d carried=%d, want 7/2/1", st.Tables, st.Refused, st.Carried)
		}
	})

	t.Run("reused zero is written, not skipped", func(t *testing.T) {
		// Both fields are omitempty on the wire, so a stale non-zero left
		// behind by the PREVIOUS run would keep rendering. Assignment, not
		// accumulation.
		st := &console.BaselineStatus{Carried: 9, CarriedCopied: 4}
		applyFoldStatus(st, 2, 0, reuseTally{}, nil)
		if st.Carried != 0 || st.CarriedCopied != 0 {
			t.Errorf("Carried=%d CarriedCopied=%d, want 0/0: the previous run's reuse counts survived into this one",
				st.Carried, st.CarriedCopied)
		}
	})
}

// TestRefreshFoldConfig_boundsTheUnattendedFold pins the two fields whose ZERO
// value is the dangerous one. Every other budget on FullTableConfig is left at
// zero on purpose because zero is the container-safe default there; for these
// two it means "use every core" and "never warn", so absence is not a posture,
// it is an omission that looks exactly like the deliberate ones beside it.
//
// Both wanted values are non-zero, which is what makes this test discriminate:
// deleting either assignment leaves the field at its zero value and fails here.
// An expectation of 0 would have passed against a missing field.
func TestRefreshFoldConfig_boundsTheUnattendedFold(t *testing.T) {
	cfg := refreshFoldConfig(refreshRequest{
		IndexDSN: "dsn", BaselineDir: "/b",
	}, time.Now(), []string{"shop.orders"})

	if cfg.Parallelism == 0 {
		t.Error("Parallelism left at zero: the fold would inherit runtime.NumCPU() " +
			"and scale its peak memory with the host, inside the capture process")
	}
	if cfg.Parallelism != daemonFoldParallelism {
		t.Errorf("Parallelism = %d, want %d", cfg.Parallelism, daemonFoldParallelism)
	}
	if cfg.WarnEventThreshold == 0 {
		t.Error("WarnEventThreshold left at zero: shouldWarnEvents is " +
			"`threshold > 0 && n > threshold`, so the unattended fold would never warn")
	}
	if cfg.WarnEventThreshold != daemonFoldWarnEventThreshold {
		t.Errorf("WarnEventThreshold = %d, want %d", cfg.WarnEventThreshold, daemonFoldWarnEventThreshold)
	}
	if cfg.RemediationHint == "" {
		t.Error("RemediationHint left empty: the warning falls back to the CLI wording, " +
			"which names --at / --parallelism / --warn-event-threshold. bintrail-console " +
			"registers none of them, so the operator is sent after flags that do not exist")
	}
	if cfg.RemediationHint != daemonFoldRemediation {
		t.Errorf("RemediationHint = %q, want the shared constant", cfg.RemediationHint)
	}
}

// TestRefreshFoldConfig_restoreSharesTheBounds: the point-in-time restore is
// the OTHER in-daemon caller, and it reaches the same config through
// restoreFoldRequest.
//
// Scope, because the honest version is narrower than it first looks: this
// calls refreshFoldConfig directly, so it pins that a restoreFoldRequest
// survives the translation with both bounds intact. It does NOT pin the
// wiring. If someone adds a restoreFoldConfig and repoints
// baseline_restore.go at it, this test keeps calling the old builder and keeps
// passing. What catches THAT is TestEveryConsoleappFoldConfigIsBounded: a new
// builder means a third FullTableConfig literal, and the guard asserts an
// exact count, so the addition cannot land without a human deciding it belongs
// under the same budget.
func TestRefreshFoldConfig_restoreSharesTheBounds(t *testing.T) {
	at := time.Now()
	req := restoreFoldRequest(console.BaselineRestoreRequest{
		ServerID: "s1", IndexDSN: "dsn", BaselineDir: "/b", At: at,
	})
	cfg := refreshFoldConfig(req, at, []string{"shop.orders"})

	if cfg.Parallelism != daemonFoldParallelism {
		t.Errorf("restore Parallelism = %d, want %d", cfg.Parallelism, daemonFoldParallelism)
	}
	if cfg.WarnEventThreshold != daemonFoldWarnEventThreshold {
		t.Errorf("restore WarnEventThreshold = %d, want %d",
			cfg.WarnEventThreshold, daemonFoldWarnEventThreshold)
	}
}

// The single load-bearing line of #1539: the fold READS where the previous
// snapshot is and WRITES where this server keeps its local copy, and on an
// S3-backed server those are different places.
//
// It gets its own assertion because nothing else can reach it. The end-to-end
// tests stub the fold, and the stub reads only OutputDir and At, so pointing
// BaselineSrc back at the local directory passed the entire suite while the
// fold would have looked in the empty directory on exactly the shape this
// exists for.
func TestRefreshFoldConfig_readsTheBucketAndWritesTheLocalDirectory(t *testing.T) {
	for _, tc := range []struct {
		name    string
		req     refreshRequest
		wantSrc string
	}{
		{"no destination: both are the local directory",
			refreshRequest{IndexDSN: "dsn", BaselineDir: "/b"}, "/b"},
		{"backups go to S3: read the bucket, write the local directory",
			refreshRequest{IndexDSN: "dsn", BaselineDir: "/b", BaselineS3: "s3://bucket/backups/"}, "s3://bucket/backups/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := refreshFoldConfig(tc.req, time.Now(), []string{"shop.orders"})
			if cfg.BaselineSrc != tc.wantSrc {
				t.Errorf("BaselineSrc = %q, want %q", cfg.BaselineSrc, tc.wantSrc)
			}
			if cfg.OutputDir != tc.req.BaselineDir {
				t.Errorf("OutputDir = %q, want the local directory %q: the fold writes Parquet to a filesystem",
					cfg.OutputDir, tc.req.BaselineDir)
			}
		})
	}
}
