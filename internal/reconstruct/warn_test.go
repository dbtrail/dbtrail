package reconstruct

import (
	"bytes"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"testing"
)

// TestMaybeWarnEventVolume exercises the warning EMISSION (not just the
// predicate): a captured slog handler must see exactly one Warn record with the
// right attributes above threshold, and nothing at/below threshold or disabled.
func TestMaybeWarnEventVolume(t *testing.T) {
	cases := []struct {
		name      string
		n         int
		threshold int64
		wantWarn  bool
	}{
		{"above threshold warns", 101, 100, true},
		{"at threshold silent", 100, 100, false},
		{"below threshold silent", 99, 100, false},
		{"disabled (0) silent", 1 << 30, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			maybeWarnEventVolume("db", "orders", int64(tc.n), tc.threshold, 1, "")
			out := buf.String()

			if !tc.wantWarn {
				if strings.Contains(out, "very large event window") {
					t.Fatalf("expected no warn (n=%d threshold=%d), got %q", tc.n, tc.threshold, out)
				}
				return
			}
			if c := strings.Count(out, "very large event window"); c != 1 {
				t.Fatalf("want exactly one warn record, got %d: %q", c, out)
			}
			for _, want := range []string{
				`"schema":"db"`,
				`"table":"orders"`,
				fmt.Sprintf(`"events":%d`, tc.n),
				fmt.Sprintf(`"threshold":%d`, tc.threshold),
				"--warn-event-threshold",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("warn record missing %s:\n%s", want, out)
				}
			}
		})
	}
}

// TestMaybeWarnEventVolume_scaledByParallelism is the #842 regression guard:
// 8 tables of 4M events each, run with Parallelism=8 against the default 5M
// per-table threshold, used to pass completely silently (4M < 5M per table)
// even though the process holds ~32M events' worth of change maps at once.
// Scaling the threshold by parallelism (5M/8 ≈ 625K) must now warn.
func TestMaybeWarnEventVolume_scaledByParallelism(t *testing.T) {
	cases := []struct {
		name        string
		n           int
		threshold   int64
		parallelism int
		wantWarn    bool
	}{
		{"8-way parallel, 4M events, 5M threshold: now warns", 4_000_000, 5_000_000, 8, true},
		{"serial (parallelism=1): unchanged, silent", 4_000_000, 5_000_000, 1, false},
		{"parallelism=0 treated as no scaling (defensive)", 4_000_000, 5_000_000, 0, false},
		{"disabled threshold stays disabled regardless of parallelism", 1 << 30, 0, 8, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			maybeWarnEventVolume("db", "orders", int64(tc.n), tc.threshold, tc.parallelism, "")
			out := buf.String()

			if !tc.wantWarn {
				if strings.Contains(out, "very large event window") {
					t.Fatalf("expected no warn (n=%d threshold=%d parallelism=%d), got %q", tc.n, tc.threshold, tc.parallelism, out)
				}
				return
			}
			if c := strings.Count(out, "very large event window"); c != 1 {
				t.Fatalf("want exactly one warn record, got %d: %q", c, out)
			}
			if !strings.Contains(out, fmt.Sprintf(`"parallelism":%d`, tc.parallelism)) {
				t.Errorf("warn record missing parallelism attribute:\n%s", out)
			}
			if !strings.Contains(out, fmt.Sprintf(`"raw_threshold":%d`, tc.threshold)) {
				t.Errorf("warn record missing raw_threshold attribute:\n%s", out)
			}
		})
	}
}

func TestScaledEventThreshold(t *testing.T) {
	cases := []struct {
		name        string
		threshold   int64
		parallelism int
		want        int64
	}{
		{"disabled threshold passes through", 0, 8, 0},
		{"negative threshold passes through (disabled)", -1, 8, -1},
		{"parallelism=1 unchanged", 5_000_000, 1, 5_000_000},
		{"parallelism=0 unchanged (defensive floor)", 5_000_000, 0, 5_000_000},
		{"parallelism=8 divides evenly", 5_000_000, 8, 625_000},
		{"floors on uneven division", 10, 3, 3},
		{"never scales below 1", 5, 100, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := scaledEventThreshold(tc.threshold, tc.parallelism); got != tc.want {
				t.Errorf("scaledEventThreshold(%d,%d) = %d, want %d", tc.threshold, tc.parallelism, got, tc.want)
			}
		})
	}
}

func TestEffectiveParallelism(t *testing.T) {
	if got := effectiveParallelism(FullTableConfig{Parallelism: 4}); got != 4 {
		t.Errorf("explicit Parallelism=4: got %d, want 4", got)
	}
	if got := effectiveParallelism(FullTableConfig{Parallelism: 0}); got != runtime.NumCPU() {
		t.Errorf("Parallelism=0 must default to NumCPU: got %d, want %d", got, runtime.NumCPU())
	}
	if got := effectiveParallelism(FullTableConfig{Parallelism: -1}); got != runtime.NumCPU() {
		t.Errorf("negative Parallelism must default to NumCPU: got %d, want %d", got, runtime.NumCPU())
	}
	// A single-table run must never divide by a big-box Parallelism/NumCPU:
	// only one table can ever be in flight, so the effective divisor is 1
	// regardless of how high Parallelism defaults (#842 false-alarm guard).
	if got := effectiveParallelism(FullTableConfig{Parallelism: 16, Tables: []string{"db.one"}}); got != 1 {
		t.Errorf("single-table run must clamp to 1: got %d, want 1", got)
	}
	// Parallelism below the table count is the real bound (fewer tables can
	// run concurrently than the table count would otherwise suggest).
	if got := effectiveParallelism(FullTableConfig{Parallelism: 4, Tables: []string{"db.a", "db.b", "db.c", "db.d", "db.e", "db.f", "db.g", "db.h"}}); got != 4 {
		t.Errorf("Parallelism smaller than table count must win: got %d, want 4", got)
	}
	// 8 tables of 4M events each with Parallelism=8 (the #842 issue's own
	// example) must clamp to 8, not silently drop below it.
	tables8 := make([]string, 8)
	for i := range tables8 {
		tables8[i] = fmt.Sprintf("db.t%d", i)
	}
	if got := effectiveParallelism(FullTableConfig{Parallelism: 8, Tables: tables8}); got != 8 {
		t.Errorf("Parallelism == table count must stay unclamped: got %d, want 8", got)
	}
}

func TestShouldWarnEvents(t *testing.T) {
	const thr = 5_000_000
	cases := []struct {
		name      string
		n         int64
		threshold int64
		want      bool
	}{
		{"below threshold", thr - 1, thr, false},
		{"at threshold", thr, thr, false},
		{"above threshold", thr + 1, thr, true},
		{"threshold 0 disables", 1 << 40, 0, false},
		{"threshold negative disables", 1 << 40, -1, false},
		{"zero events never warns", 0, thr, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldWarnEvents(tc.n, tc.threshold); got != tc.want {
				t.Fatalf("shouldWarnEvents(%d,%d) = %v, want %v", tc.n, tc.threshold, got, tc.want)
			}
		})
	}
}

// TestMaybeWarnEventVolume_remediation pins WHOSE advice the warning carries.
//
// The default names --at / --parallelism / --warn-event-threshold, which is
// correct for the attended CLI commands that register them and wrong on
// bintrail-console, which registers none of the three. A warning that tells an
// operator to lower a flag their binary does not have is worse than silence: it
// sends them looking for something that is not there. So a caller on such a
// surface supplies its own, and that substitution is what this pins.
func TestMaybeWarnEventVolume_remediation(t *testing.T) {
	capture := func(remediation string) string {
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		// n over threshold at parallelism 1, so the warning always fires and
		// the test discriminates on the hint rather than on whether it emitted.
		maybeWarnEventVolume("db", "orders", 100, 10, 1, remediation)
		return buf.String()
	}

	t.Run("empty falls back to the CLI wording", func(t *testing.T) {
		out := capture("")
		// Every knob, not just one: an assertion on a single flag stays green
		// while the others are edited away, and the attended commands rely on
		// the whole list. Each name below is registered by internal/cli or
		// cliapp today; drop one there and drop it here in the same change.
		for _, flag := range []string{"--at", "--parallelism", "--warn-event-threshold", "BINTRAIL_RECONSTRUCT_WARN_EVENTS"} {
			if !strings.Contains(out, flag) {
				t.Errorf("default hint no longer names %s; the attended commands rely on it.\ngot: %s", flag, out)
			}
		}
	})

	t.Run("a caller-supplied hint replaces it entirely", func(t *testing.T) {
		out := capture("shorten the refresh interval, or refresh fewer tables")
		if !strings.Contains(out, "shorten the refresh interval") {
			t.Errorf("caller hint not used.\ngot: %s", out)
		}
		// The point of the field: the flags must be GONE, not merely joined by
		// the caller's text. Asserting only that the new text appears would
		// pass while still sending the operator after a flag that is absent.
		for _, flag := range []string{"--at", "--parallelism", "--warn-event-threshold", "BINTRAIL_RECONSTRUCT_WARN_EVENTS"} {
			if strings.Contains(out, flag) {
				t.Errorf("hint still names %s, which bintrail-console does not register.\ngot: %s", flag, out)
			}
		}
	})
}

// TestWithFoldBudgets pins the SEAM, which is where this repo's defects live.
//
// Every budget a caller sets on FullTableConfig has to survive the translation
// into the foldConfig the fold actually runs with. Before this helper existed
// those four assignments were written out at each foldEventWindow call site,
// and deleting one line at EITHER site reverted the budget with the whole test
// suite green, because nothing drives a FullTableConfig through foldConfig into
// the warning. Setting the field and READING it are different claims; the
// config-tier tests in consoleapp only ever made the first.
//
// Every wanted value below is distinct and non-zero, so a helper that copied
// the wrong field, or none, cannot pass by coincidence.
func TestWithFoldBudgets(t *testing.T) {
	cfg := FullTableConfig{
		FetchBatchSize:     4242,
		WarnEventThreshold: 777_777,
		MaxTouchedRows:     55_555,
		MaxChangeBytes:     66_666,
		Parallelism:        3,
		Tables:             []string{"a.one", "a.two", "a.three", "a.four"},
		RemediationHint:    "do the thing this binary can actually do",
	}
	// A non-empty starting foldConfig: the helper must OVERWRITE the budgets,
	// not merely fill in blanks, or a caller that pre-set a stale value keeps it.
	got := withFoldBudgets(cfg, foldConfig{
		Schema: "shop", Table: "orders",
		BatchSize: 1, WarnEventThreshold: 1, Parallelism: 1, RemediationHint: "stale",
		MaxTouchedRows: 1, MaxChangeBytes: 1,
	})
	if got.MaxTouchedRows != 55_555 || got.MaxChangeBytes != 66_666 {
		t.Errorf("MaxTouchedRows, MaxChangeBytes = %d, %d; want 55555, 66666 (#1107, #2207)", got.MaxTouchedRows, got.MaxChangeBytes)
	}

	if got.BatchSize != 4242 {
		t.Errorf("BatchSize = %d, want 4242", got.BatchSize)
	}
	if got.WarnEventThreshold != 777_777 {
		t.Errorf("WarnEventThreshold = %d, want 777777", got.WarnEventThreshold)
	}
	if got.RemediationHint != cfg.RemediationHint {
		t.Errorf("RemediationHint = %q, want %q.\nThis is the field whose whole point is that the "+
			"daemon's warning must not name CLI flags that binary does not register.",
			got.RemediationHint, cfg.RemediationHint)
	}
	// The DIVISOR, not the raw field. 4 tables against Parallelism 3 leaves 3.
	if got.Parallelism != 3 {
		t.Errorf("Parallelism = %d, want 3 (effectiveParallelism, not cfg.Parallelism verbatim)", got.Parallelism)
	}
	// Fields the helper has no business touching must survive untouched.
	if got.Schema != "shop" || got.Table != "orders" {
		t.Errorf("helper clobbered non-budget fields: schema=%q table=%q", got.Schema, got.Table)
	}

	t.Run("clamps the divisor to the table count", func(t *testing.T) {
		// One table cannot run concurrently with anything, so dividing the
		// threshold by a parallelism it can never reach would warn early.
		got := withFoldBudgets(FullTableConfig{
			Parallelism: 8, Tables: []string{"a.one"}, WarnEventThreshold: 5_000_000,
		}, foldConfig{})
		if got.Parallelism != 1 {
			t.Errorf("Parallelism = %d, want 1 for a single-table run", got.Parallelism)
		}
	})
}
