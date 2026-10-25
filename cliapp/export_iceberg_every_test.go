package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/icebergexport"
)

// --every turns the export into a loop in the same process (#2552). These
// tests pin the loop's contract: what --every accepts, that the timing is
// measured from the END of the previous run, that a failed run does not stop
// the loop, that a signal (a canceled context) ends it with success whether
// it lands during a run or during the wait, and that each run prints its own
// report.

func TestParseExportEvery(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		err  string
	}{
		{"", 0, ""}, // unset: today's one-shot
		{"   ", 0, ""},
		{"1m", time.Minute, ""}, // exactly the floor
		{" 5m ", 5 * time.Minute, ""},
		{"1h30m", 90 * time.Minute, ""},
		{"0", 0, "--every must be at least 1m0s"},
		{"0s", 0, "--every must be at least 1m0s"},
		{"-5m", 0, "--every must be at least 1m0s"},
		{"59s", 0, "--every must be at least 1m0s"},
		{"abc", 0, `--every "abc" is not a duration`},
		{"60", 0, `--every "60" is not a duration`}, // no unit
		{"5M", 0, `--every "5M" is not a duration`},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseExportEvery(tc.in)
			if tc.err == "" {
				if err != nil || got != tc.want {
					t.Fatalf("parseExportEvery(%q) = %v, %v; want %v, nil", tc.in, got, err, tc.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("parseExportEvery(%q) err = %v, want containing %q", tc.in, err, tc.err)
			}
		})
	}
}

// fakeClock is a now() the runner and the sleeper move forward by hand.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func testLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, nil))
}

// TestExportLoop_timingFromEndOfRun: a run that takes 40s of a 60s interval
// is followed by a 60s wait, not a 20s one, and the next run time it logs is
// the end of the run plus the interval. No overlap, no catch-up.
func TestExportLoop_timingFromEndOfRun(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var waits []time.Duration
	runs := 0
	run := func(context.Context) error {
		runs++
		clock.add(40 * time.Second)
		return nil
	}
	sleep := func(ctx context.Context, d time.Duration) error {
		waits = append(waits, d)
		clock.add(d)
		if len(waits) == 2 {
			cancel()
			return ctx.Err()
		}
		return nil
	}
	var logs bytes.Buffer
	if err := exportLoop(ctx, time.Minute, run, sleep, clock.now, testLogger(&logs)); err != nil {
		t.Fatalf("exportLoop = %v, want nil after a cancel", err)
	}
	if runs != 2 || len(waits) != 2 || waits[0] != time.Minute || waits[1] != time.Minute {
		t.Fatalf("runs = %d, waits = %v; want 2 runs each followed by a full 1m wait", runs, waits)
	}
	// Run 1 ends at 12:00:40, so the next run is 12:01:40.
	if !strings.Contains(logs.String(), "2026-10-25T12:01:40Z") {
		t.Fatalf("log does not name the next run as end of run + interval (12:01:40):\n%s", logs.String())
	}
}

// TestExportLoop_survivesRunError: a run-level failure is logged with the
// error and the next attempt, and the loop goes on to a second run that
// succeeds.
func TestExportLoop_survivesRunError(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runs := 0
	run := func(context.Context) error {
		runs++
		if runs == 1 {
			return errors.New("connect to index: dial tcp: connection refused")
		}
		return nil
	}
	sleep := func(ctx context.Context, d time.Duration) error {
		clock.add(d)
		if runs == 2 {
			cancel()
			return ctx.Err()
		}
		return nil
	}
	var logs bytes.Buffer
	if err := exportLoop(ctx, time.Hour, run, sleep, clock.now, testLogger(&logs)); err != nil {
		t.Fatalf("exportLoop = %v, want nil", err)
	}
	if runs != 2 {
		t.Fatalf("runs = %d, want 2: a failed run must not end the loop", runs)
	}
	out := logs.String()
	if !strings.Contains(out, "connection refused") || !strings.Contains(out, "2026-10-25T13:00:00Z") {
		t.Fatalf("the failure line must carry the error and the next attempt time:\n%s", out)
	}
}

// TestExportLoop_cancelDuringWait: a signal during the wait ends the loop at
// once with success, using the real sleeper.
func TestExportLoop_cancelDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := func(context.Context) error {
		time.AfterFunc(50*time.Millisecond, cancel)
		return nil
	}
	var logs bytes.Buffer
	start := time.Now()
	err := exportLoop(ctx, time.Hour, run, sleepCtx, time.Now, testLogger(&logs))
	if err != nil {
		t.Fatalf("exportLoop = %v, want nil", err)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("a cancel during the wait took %v to end the loop", el)
	}
	if !strings.Contains(logs.String(), "stopped") {
		t.Fatalf("no stop line:\n%s", logs.String())
	}
}

// TestExportLoop_cancelDuringRun: a signal during a run cancels that run's
// context; the loop waits for it to return, does not sleep, and ends with
// success.
func TestExportLoop_cancelDuringRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sawCancel := false
	run := func(rctx context.Context) error {
		cancel()
		<-rctx.Done()
		sawCancel = true
		return rctx.Err()
	}
	slept := false
	sleep := func(context.Context, time.Duration) error { slept = true; return nil }
	var logs bytes.Buffer
	if err := exportLoop(ctx, time.Hour, run, sleep, time.Now, testLogger(&logs)); err != nil {
		t.Fatalf("exportLoop = %v, want nil: a canceled run is a clean stop, not a failure", err)
	}
	if !sawCancel || slept {
		t.Fatalf("sawCancel = %v, slept = %v; want the run canceled and no wait after it", sawCancel, slept)
	}
	if !strings.Contains(logs.String(), "stopped") || !strings.Contains(logs.String(), "canceled") {
		t.Fatalf("the stop line must say the run in progress was canceled:\n%s", logs.String())
	}
}

// everyHarness runs the real command in --every mode with the export itself
// swapped out, and stops it after `runs` runs.
func everyHarness(t *testing.T, runs int, fake func(ctx context.Context, cfg icebergexport.Config) ([]icebergexport.Outcome, error)) (stdout, stderr string, calls []icebergexport.Config, err error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sRun, sSleep := exportRunFn, exportSleepFn
	t.Cleanup(func() { exportRunFn, exportSleepFn = sRun, sSleep })
	exportRunFn = func(ctx context.Context, cfg icebergexport.Config) ([]icebergexport.Outcome, error) {
		calls = append(calls, cfg)
		return fake(ctx, cfg)
	}
	exportSleepFn = func(ctx context.Context, d time.Duration) error {
		if len(calls) >= runs {
			cancel()
			return ctx.Err()
		}
		return nil
	}
	var out, errb bytes.Buffer
	exportIcebergCmd.SetContext(ctx)
	exportIcebergCmd.SetOut(&out)
	exportIcebergCmd.SetErr(&errb)
	t.Cleanup(func() { exportIcebergCmd.SetErr(nil) })
	err = runExportIceberg(exportIcebergCmd, nil)
	return out.String(), errb.String(), calls, err
}

func everyValid(t *testing.T) {
	eiIndexDSN = "u:p@tcp(127.0.0.1:1)/db?timeout=200ms"
	eiBaselineDir = t.TempDir()
	eiWarehouse = t.TempDir()
	eiTables = "shop.orders"
	eiFormat = "text"
	eiEvery = "1m"
}

// TestRunExportIceberg_everyJSONOneDocPerRun: with --format json each run
// prints ONE line holding one JSON document, so the stream can be read line
// by line; each run exports up to its own moment (a stale --at would make
// every run after the first a no-op); log lines stay off stdout.
func TestRunExportIceberg_everyJSONOneDocPerRun(t *testing.T) {
	resetCatalogGlobals(t)
	everyValid(t)
	eiFormat = "json"
	stdout, stderr, calls, err := everyHarness(t, 2, func(_ context.Context, cfg icebergexport.Config) ([]icebergexport.Outcome, error) {
		return []icebergexport.Outcome{{Schema: "shop", Table: "orders", Verdict: icebergexport.VerdictExported, Events: 1}}, nil
	})
	if err != nil {
		t.Fatalf("runExportIceberg = %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("export ran %d times, want 2", len(calls))
	}
	if !calls[1].At.After(calls[0].At) {
		t.Fatalf("run 2 exports up to %v, run 1 up to %v: each run must take its own moment", calls[1].At, calls[0].At)
	}
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("stdout has %d lines, want one per run:\n%s", len(lines), stdout)
	}
	for i, l := range lines {
		var doc exportJSON
		if err := json.Unmarshal([]byte(l), &doc); err != nil {
			t.Fatalf("line %d is not one JSON document: %v\n%s", i, err, l)
		}
		if len(doc.Tables) != 1 || doc.Tables[0].Verdict != "exported" {
			t.Fatalf("line %d = %+v", i, doc)
		}
	}
	if !strings.Contains(stderr, "next run") {
		t.Fatalf("the loop's log lines belong on stderr:\n%s", stderr)
	}
}

// TestRunExportIceberg_everyTextHasRunHeaders: text mode prints the usual
// summary per run, each headed with its own time.
func TestRunExportIceberg_everyTextHasRunHeaders(t *testing.T) {
	resetCatalogGlobals(t)
	everyValid(t)
	stdout, _, _, err := everyHarness(t, 2, func(_ context.Context, cfg icebergexport.Config) ([]icebergexport.Outcome, error) {
		return []icebergexport.Outcome{{Schema: "shop", Table: "orders", Verdict: icebergexport.VerdictUnchanged}}, nil
	})
	if err != nil {
		t.Fatalf("runExportIceberg = %v", err)
	}
	if n := strings.Count(stdout, "iceberg export to "); n != 2 {
		t.Fatalf("%d run headers, want 2:\n%s", n, stdout)
	}
}

// TestRunExportIceberg_everyRunFailureKeepsLooping: a run-level error and a
// run with a refused table are both logged, and the loop still exits 0 when
// stopped.
func TestRunExportIceberg_everyRunFailureKeepsLooping(t *testing.T) {
	resetCatalogGlobals(t)
	everyValid(t)
	n := 0
	_, stderr, calls, err := everyHarness(t, 2, func(_ context.Context, cfg icebergexport.Config) ([]icebergexport.Outcome, error) {
		n++
		if n == 1 {
			return nil, errors.New("catalog answered 503")
		}
		return []icebergexport.Outcome{{Schema: "shop", Table: "orders", Verdict: icebergexport.VerdictRefusedGap, Detail: "gap"}}, nil
	})
	if err != nil {
		t.Fatalf("runExportIceberg = %v, want nil: per-run failures do not end the loop", err)
	}
	if len(calls) != 2 {
		t.Fatalf("export ran %d times, want 2", len(calls))
	}
	if !strings.Contains(stderr, "catalog answered 503") || !strings.Contains(stderr, "1 of 1 table(s) did not end current") {
		t.Fatalf("both failures must be logged:\n%s", stderr)
	}
}

// TestRunExportIceberg_everyBadConfigBeforeFirstRun: a bad --every, a bad
// flag or a bad catalog exits with an error and never reaches a run.
func TestRunExportIceberg_everyBadConfigBeforeFirstRun(t *testing.T) {
	cases := []struct {
		name string
		mut  func(t *testing.T)
		want string
	}{
		{"garbage every", func(t *testing.T) { eiEvery = "abc" }, "is not a duration"},
		{"zero every", func(t *testing.T) { eiEvery = "0" }, "at least 1m0s"},
		{"every below floor", func(t *testing.T) { eiEvery = "30s" }, "at least 1m0s"},
		{"every with at", func(t *testing.T) { eiAt = "2026-10-25 12:00:00" }, "--at cannot be used with --every"},
		{"bad format", func(t *testing.T) { eiFormat = "yaml" }, "--format must be text or json"},
		{"bad catalog", func(t *testing.T) { eiCatalog = "hive" }, `unknown catalog "hive"`},
		{"no snapshot to list tables from", func(t *testing.T) { eiTables = "" }, "no tables to export"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetCatalogGlobals(t)
			everyValid(t)
			tc.mut(t)
			_, _, calls, err := everyHarness(t, 1, func(context.Context, icebergexport.Config) ([]icebergexport.Outcome, error) {
				return nil, nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
			if len(calls) != 0 {
				t.Fatalf("a bad configuration reached %d run(s)", len(calls))
			}
		})
	}
}

// TestRunExportIceberg_noEveryIsOneShot: without --every the command runs
// once and reports a run failure as its exit status, as before.
func TestRunExportIceberg_noEveryIsOneShot(t *testing.T) {
	resetCatalogGlobals(t)
	everyValid(t)
	eiEvery = ""
	_, _, calls, err := everyHarness(t, 99, func(context.Context, icebergexport.Config) ([]icebergexport.Outcome, error) {
		return nil, errors.New("catalog answered 503")
	})
	if err == nil || !strings.Contains(err.Error(), "catalog answered 503") || len(calls) != 1 {
		t.Fatalf("err = %v, calls = %d; want the one run's error", err, len(calls))
	}
}

// TestExportIceberg_envNeverMakesItLoop: --every is set on the command line
// only. An environment variable (or the env file, which loads into the
// environment) that made it loop would turn every cron-started one-shot into
// a process that never exits, and cron would start another one each hour.
func TestExportIceberg_envNeverMakesItLoop(t *testing.T) {
	resetCatalogGlobals(t)
	everyValid(t)
	eiEvery = ""
	t.Setenv("BINTRAIL_ICEBERG_EVERY", "15m")
	t.Setenv("BINTRAIL_EVERY", "15m")
	bindCommandEnv(exportIcebergCmd)
	if eiEvery != "" {
		t.Fatalf("eiEvery = %q from the environment; --every must come from the command line only", eiEvery)
	}
	_, _, calls, err := everyHarness(t, 99, func(context.Context, icebergexport.Config) ([]icebergexport.Outcome, error) {
		return []icebergexport.Outcome{{Schema: "shop", Table: "orders", Verdict: icebergexport.VerdictUnchanged}}, nil
	})
	if err != nil || len(calls) != 1 {
		t.Fatalf("err = %v, runs = %d; want one run and a return", err, len(calls))
	}
	for _, b := range cli.EnvBindings {
		if b.Flag == "every" {
			t.Fatalf("--every is bound to %s; it must not be bound to any variable", b.EnvVar)
		}
	}
}

// TestExportLoop_signalKeepsARealFailure: a run that failed for its own
// reason just as the signal arrived still gets its error logged.
func TestExportLoop_signalKeepsARealFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	run := func(context.Context) error {
		cancel()
		return errors.New("catalog answered 503")
	}
	var logs bytes.Buffer
	if err := exportLoop(ctx, time.Hour, run, sleepCtx, time.Now, testLogger(&logs)); err != nil {
		t.Fatalf("exportLoop = %v, want nil", err)
	}
	if !strings.Contains(logs.String(), "catalog answered 503") {
		t.Fatalf("the run's own error was dropped:\n%s", logs.String())
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("stdout closed") }

// TestRunOnce_reportErrorKeepsTableFailures: a report that cannot be written
// does not hide the tables that did not end current.
func TestRunOnce_reportErrorKeepsTableFailures(t *testing.T) {
	resetCatalogGlobals(t)
	sRun := exportRunFn
	t.Cleanup(func() { exportRunFn = sRun })
	exportRunFn = func(context.Context, icebergexport.Config) ([]icebergexport.Outcome, error) {
		return []icebergexport.Outcome{{Schema: "shop", Table: "orders", Verdict: icebergexport.VerdictRefusedGap}}, nil
	}
	j := &exportJob{target: "/w"}
	err := j.runOnce(context.Background(), failWriter{}, []string{"shop.orders"})
	if err == nil || !strings.Contains(err.Error(), "stdout closed") || !strings.Contains(err.Error(), "did not end current") {
		t.Fatalf("err = %v, want both the write failure and the table failure", err)
	}
}
