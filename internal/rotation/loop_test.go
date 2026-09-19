package rotation

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// logCapture is a slog.Handler that records every emitted record, so tests
// can assert on escalation levels and messages.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r)
	return nil
}
func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

func (c *logCapture) has(level slog.Level, substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.Level == level && strings.Contains(r.Message, substr) {
			return true
		}
	}
	return false
}

// hasAttr reports whether a record with message msg carries key=val.
func (c *logCapture) hasAttr(msg, key, val string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.Message != msg {
			continue
		}
		found := false
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key && a.Value.String() == val {
				found = true
			}
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

// hasKey reports whether a record with message msg carries key at all.
func (c *logCapture) hasKey(msg, key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.Message != msg {
			continue
		}
		found := false
		r.Attrs(func(a slog.Attr) bool {
			found = found || a.Key == key
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

// captureSlog swaps the default logger for a capturing one until cleanup.
func captureSlog(t *testing.T) *logCapture {
	t.Helper()
	c := &logCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(c))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

// TestLoopOptions is the data-loss-safety regression test: the entire safety of
// the built-in rotation reduces to loopOptions arming ProtectUnarchived (and
// never archiving). If a field regressed, `up` would silently drop unarchived
// partitions by default and the integration tests (which exercise the engine
// guard directly) could stay green. Replaces the old armsRotateGlobals test,
// which guarded the rot*-global fan-out that loopOptions superseded.
func TestLoopOptions(t *testing.T) {
	s := Settings{Enabled: true, Retain: 30 * 24 * time.Hour, RetainRaw: "30d", AddFuture: 3}
	// A drop-only target (no ArchiveS3) is the default, data-loss-safe shape.
	o := loopOptions(7*24*time.Hour, s, RotateTarget{DSN: "x"}, false)

	if !o.ProtectUnarchived {
		t.Error("ProtectUnarchived must be armed — without it the built-in rotation drops unarchived data")
	}
	if o.QuietEmpty {
		t.Error("QuietEmpty must be off unless the probe found a writerless index empty (#1715)")
	}
	if q := loopOptions(7*24*time.Hour, s, RotateTarget{DSN: "x"}, true); !q.QuietEmpty {
		t.Error("QuietEmpty must carry through from the probe's verdict")
	}
	if o.NoReplace {
		t.Error("NoReplace must be false (dropped partitions are replaced)")
	}
	if o.ArchiveDir != "" || o.ArchiveS3 != "" || o.BintrailID != "" {
		t.Errorf("a drop-only target must leave archive fields empty: dir=%q s3=%q id=%q",
			o.ArchiveDir, o.ArchiveS3, o.BintrailID)
	}
	if o.Retry {
		t.Error("Retry must be false for a drop-only target")
	}
	if o.Format != "json" {
		t.Errorf("Format = %q, want json (suppresses per-partition stdout chatter)", o.Format)
	}
	if o.AddFuture != 3 {
		t.Errorf("AddFuture = %d, want 3", o.AddFuture)
	}
	if o.RetainRaw != "30d" {
		t.Errorf("RetainRaw = %q, want 30d", o.RetainRaw)
	}
	if o.RetainDur != 7*24*time.Hour {
		t.Errorf("RetainDur = %v, want 168h (the guard-adjusted retain, not s.Retain)", o.RetainDur)
	}
}

// TestLoopOptionsArchive: a target carrying an ArchiveS3 bucket flips the cycle
// into archive-then-drop with the staging dir, bintrail_id, retry, and local
// prune all wired through from the target.
func TestLoopOptionsArchive(t *testing.T) {
	s := Settings{Enabled: true, Retain: 30 * 24 * time.Hour, RetainRaw: "30d", AddFuture: 3}
	o := loopOptions(s.Retain, s, RotateTarget{
		DSN:                "x",
		ArchiveDir:         "/staging/abc",
		ArchiveS3:          "s3://bucket/prefix/",
		ArchiveS3Region:    "us-east-1",
		BintrailID:         "uuid-123",
		ArchiveCompression: "zstd",
	}, false)
	if o.ArchiveDir != "/staging/abc" || o.ArchiveS3 != "s3://bucket/prefix/" || o.BintrailID != "uuid-123" {
		t.Errorf("archive config not threaded through: %+v", o)
	}
	if o.ArchiveS3Region != "us-east-1" || o.ArchiveCompression != "zstd" {
		t.Errorf("region/compression not threaded: region=%q codec=%q", o.ArchiveS3Region, o.ArchiveCompression)
	}
	if !o.Retry {
		t.Error("Retry must be true when archiving (skip what a prior cycle already did)")
	}
	if !o.PruneLocalAfterUpload {
		t.Error("PruneLocalAfterUpload must be true for the unattended loop")
	}
	if !o.ProtectUnarchived {
		t.Error("ProtectUnarchived stays armed")
	}
}

// TestStartLoop_disabledIsInert verifies the disabled path starts no loop and
// never consults the DSN provider.
func TestStartLoop_disabledIsInert(t *testing.T) {
	called := false
	done := StartLoop(context.Background(), func() Settings { return Settings{} }, func() []RotateTarget {
		called = true
		return nil
	})
	select {
	case <-done: // disabled path returns an already-closed channel
	default:
		t.Error("disabled rotation must return a closed done channel (no loop running)")
	}
	if called {
		t.Error("disabled rotation must not invoke the DSN provider")
	}
}

// TestStartLoop_rereadsSettingsEachCycle proves the settings provider is read
// FRESH every cycle (not captured once at start) — the property the console
// rotation panel relies on to apply an edit without a daemon restart. With no
// targets the cycle is a no-op (no DB needed); we only assert the provider keeps
// being called.
func TestStartLoop_rereadsSettingsEachCycle(t *testing.T) {
	captureSlog(t)
	var calls atomic.Int32
	s := Settings{
		Enabled: true, Retain: 24 * time.Hour, RetainRaw: "24h",
		Interval: 5 * time.Millisecond, AddFuture: 0, Explicit: true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := StartLoop(ctx, func() Settings { calls.Add(1); return s }, func() []RotateTarget {
		return nil // empty target set → cycle is a no-op, but settings() was still read
	})

	// Expect the initial gate read + first immediate cycle + several ticks.
	deadline := time.After(5 * time.Second)
	for calls.Load() < 4 {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("settings provider called only %d times; it must be read fresh each cycle", calls.Load())
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

// TestStartLoop_liveIntervalRetune pins the headline feature: when the settings
// provider returns a CHANGED interval mid-run, the loop re-tunes its ticker
// (the ticker.Reset branch) rather than staying on the boot-time cadence —
// otherwise a console interval edit would silently no-op until restart.
// TestStartLoop_rereadsSettingsEachCycle returns a constant, so it can't cover
// this branch.
func TestStartLoop_liveIntervalRetune(t *testing.T) {
	logs := captureSlog(t)
	var calls atomic.Int32
	settings := func() Settings {
		n := calls.Add(1)
		iv := 30 * time.Millisecond
		if n >= 3 {
			iv = 8 * time.Millisecond // shrink the interval after a couple reads
		}
		return Settings{
			Enabled: true, Retain: 24 * time.Hour, RetainRaw: "24h",
			Interval: iv, AddFuture: 0, Explicit: true,
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := StartLoop(ctx, settings, func() []RotateTarget { return nil })

	deadline := time.After(5 * time.Second)
	for !logs.has(slog.LevelInfo, "interval changed") {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("the ticker was never re-tuned after the provider changed the interval")
		case <-time.After(3 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

// TestStartLoop_escalatesAfterConsecutiveFailures exercises the detection half
// of the data-loss story: a rotation that fails every cycle must escalate from
// per-cycle Warns to an explicit Error after escalateAfter consecutive cycles —
// otherwise the index grows unbounded while the logs read as routine noise.
func TestStartLoop_escalatesAfterConsecutiveFailures(t *testing.T) {
	logs := captureSlog(t)

	prevN := escalateAfter
	escalateAfter = 2
	t.Cleanup(func() { escalateAfter = prevN })

	s := Settings{
		Enabled:   true,
		Retain:    24 * time.Hour,
		RetainRaw: "24h",
		Interval:  5 * time.Millisecond,
		AddFuture: 0,
		Explicit:  true, // skip the upgrade guard; we are testing escalation
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Port 1 on loopback: connection refused immediately, every cycle fails.
	done := StartLoop(ctx, func() Settings { return s }, func() []RotateTarget {
		return []RotateTarget{{DSN: "root:x@tcp(127.0.0.1:1)/nope"}}
	})

	deadline := time.After(15 * time.Second)
	for !logs.has(slog.LevelError, "made no progress for consecutive cycles") {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("rotation never escalated to Error after consecutive failing cycles")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

// TestStartLoop_onCycleCallbacks pins the #1192 notification seam: onCycle
// observes every cycle with the cycle's real health, and a panicking callback
// neither kills the loop nor suppresses the escalation Error — the callbacks
// run inside the recover guard, AFTER the streak accounting. (With the order
// reversed, the per-cycle panic would skip the streak increment and this
// test's escalation wait would time out.)
func TestStartLoop_onCycleCallbacks(t *testing.T) {
	logs := captureSlog(t)
	prevN := escalateAfter
	escalateAfter = 2
	t.Cleanup(func() { escalateAfter = prevN })

	var healthy, unhealthy atomic.Int32
	s := Settings{
		Enabled: true, Retain: 24 * time.Hour, RetainRaw: "24h",
		Interval: 5 * time.Millisecond, AddFuture: 0, Explicit: true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Port 1 on loopback: every cycle fails, so every callback must see
	// failed=true.
	done := StartLoop(ctx, func() Settings { return s },
		func() []RotateTarget { return []RotateTarget{{DSN: "root:x@tcp(127.0.0.1:1)/nope"}} },
		func(failed bool, deferred int) {
			if failed || deferred > 0 {
				unhealthy.Add(1)
			} else {
				healthy.Add(1)
			}
			panic("callback bug")
		})

	deadline := time.After(15 * time.Second)
	for unhealthy.Load() < 3 || !logs.has(slog.LevelError, "made no progress for consecutive cycles") {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("callbacks=%d escalated=%v — onCycle not driven per cycle, or a panicking callback suppressed the escalation",
				unhealthy.Load(), logs.has(slog.LevelError, "made no progress for consecutive cycles"))
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
	if healthy.Load() != 0 {
		t.Fatalf("callback reported healthy cycles from an always-failing target: %d", healthy.Load())
	}
}

// TestGuardTrips covers the pure decision behind the upgrade guard at its
// edges: a regression flipping any of these silently disables rotation on
// fresh installs (unbounded growth) or shreds history it promised to protect.
func TestGuardTrips(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	retain := 30 * 24 * time.Hour
	name := func(age time.Duration) string { return indexer.PartitionName(now.Add(-age)) }

	cases := []struct {
		desc       string
		partitions []partitionInfo
		want       bool
	}{
		{"empty list (fresh install)", nil, false},
		{"p_future only (fresh install)", []partitionInfo{{Name: "p_future"}}, false},
		{"malformed names only", []partitionInfo{{Name: "p_bogus"}, {Name: "weird"}}, false},
		{"oldest inside the window", []partitionInfo{{Name: name(10 * 24 * time.Hour)}}, false},
		{"oldest exactly AT the 2x boundary (strict >, must NOT trip)",
			[]partitionInfo{{Name: name(2 * retain)}}, false},
		{"oldest just past the 2x boundary",
			[]partitionInfo{{Name: name(2*retain + time.Hour)}}, true},
		{"deep history mixed with recent partitions and p_future",
			[]partitionInfo{{Name: name(time.Hour)}, {Name: name(100 * 24 * time.Hour)}, {Name: "p_future"}}, true},
	}
	for _, tc := range cases {
		got, _ := guardTrips(tc.partitions, retain, now)
		if got != tc.want {
			t.Errorf("%s: guardTrips = %v, want %v", tc.desc, got, tc.want)
		}
	}
}

// TestParseSettings_explicitPropagates pins that the constructor is the sole
// author of the Explicit field — the switch between "operator chose a
// retention" and "implicit default protected by the upgrade guard".
func TestParseSettings_explicitPropagates(t *testing.T) {
	for _, explicit := range []bool{true, false} {
		s, err := ParseSettings("30d", "1h", 3, explicit)
		if err != nil {
			t.Fatalf("ParseSettings: %v", err)
		}
		if s.Explicit != explicit {
			t.Errorf("Explicit = %v, want %v", s.Explicit, explicit)
		}
	}
}

// TestRunCycle_reportsFailure verifies the cycle aggregation: any DSN failing
// marks the whole cycle failed (feeding the escalation streak).
func TestRunCycle_reportsFailure(t *testing.T) {
	captureSlog(t) // silence the expected warnings

	s := Settings{
		Enabled: true, Retain: 24 * time.Hour, RetainRaw: "24h",
		Interval: time.Hour, Explicit: true,
	}
	deferred, failed := runCycle(context.Background(), s, func() []RotateTarget {
		return []RotateTarget{{DSN: "root:x@tcp(127.0.0.1:1)/nope"}, {DSN: "not-a-dsn"}}
	})
	if !failed {
		t.Error("cycle with unreachable DSNs must report failed=true")
	}
	if deferred != 0 {
		t.Errorf("deferred = %d, want 0 (nothing rotated)", deferred)
	}
}

func TestParseSettings_enabled(t *testing.T) {
	s, err := ParseSettings("30d", "1h", 3, false)
	if err != nil {
		t.Fatalf("ParseSettings: %v", err)
	}
	if !s.Enabled {
		t.Fatal("expected enabled")
	}
	if s.Retain != 30*24*time.Hour {
		t.Errorf("Retain = %v, want 720h", s.Retain)
	}
	if s.Interval != time.Hour {
		t.Errorf("Interval = %v, want 1h", s.Interval)
	}
	if s.AddFuture != 3 {
		t.Errorf("AddFuture = %d, want 3", s.AddFuture)
	}
	if s.RetainRaw != "30d" {
		t.Errorf("RetainRaw = %q, want 30d", s.RetainRaw)
	}
}

func TestParseSettings_disabledForms(t *testing.T) {
	for _, retain := range []string{"off", "0", ""} {
		s, err := ParseSettings(retain, "1h", 3, false)
		if err != nil {
			t.Errorf("ParseSettings(%q): unexpected error %v", retain, err)
			continue
		}
		if s.Enabled {
			t.Errorf("ParseSettings(%q): expected disabled", retain)
		}
	}
}

func TestParseSettings_invalidRetain(t *testing.T) {
	_, err := ParseSettings("1x", "1h", 3, false)
	if err == nil {
		t.Fatal("expected error for invalid retain unit")
	}
	if !strings.Contains(err.Error(), "off") {
		t.Errorf("error should mention the \"off\" escape hatch, got: %v", err)
	}
}

func TestParseSettings_invalidInterval(t *testing.T) {
	if _, err := ParseSettings("7d", "soon", 3, false); err == nil {
		t.Fatal("expected error for unparseable interval")
	}
	if _, err := ParseSettings("7d", "-1h", 3, false); err == nil {
		t.Fatal("expected error for non-positive interval")
	}
}

func TestParseSettings_negativeAddFuture(t *testing.T) {
	if _, err := ParseSettings("7d", "1h", -1, false); err == nil {
		t.Fatal("expected error for negative add-future")
	}
}

func TestDedupeTargets(t *testing.T) {
	in := []RotateTarget{{DSN: "a"}, {DSN: ""}, {DSN: "b"}, {DSN: "a"}, {DSN: "c"}, {DSN: "b"}}
	got := dedupeTargets(in)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("dedupeTargets = %v, want DSNs %v", got, want)
	}
	for i := range want {
		if got[i].DSN != want[i] {
			t.Fatalf("dedupeTargets[%d].DSN = %q, want %q", i, got[i].DSN, want[i])
		}
	}
}
