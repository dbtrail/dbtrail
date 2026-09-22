package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// TestTableFilter covers the nil-vs-empty semantics: no filter means "verify
// everything" (nil map, never restricts), a non-empty filter tracks what it
// has and hasn't matched via the mutable seen copy.
func TestTableFilter(t *testing.T) {
	if filter, seen := tableFilter(nil); filter != nil || seen != nil {
		t.Fatalf("no filter: got filter=%v seen=%v, want both nil", filter, seen)
	}
	filter, seen := tableFilter([]string{"wp.posts", "wp.users"})
	if len(filter) != 2 || !filter["wp.posts"] || !filter["wp.users"] {
		t.Fatalf("filter = %v, want both entries set", filter)
	}
	delete(seen, "wp.posts")
	if len(seen) != 1 || !seen["wp.users"] {
		t.Fatalf("seen after delete = %v, want only wp.users left", seen)
	}
}

// TestToWireResult covers the field mapping from the engine's TableResult to
// the console DTO, including the explainable flag being caller-controlled
// (not derived from Status alone — a live-source mismatch must never claim
// explainable, since the engine has no explain support for that mode).
func TestToWireResult(t *testing.T) {
	res := verify.TableResult{
		Schema: "wp", Table: "posts", Status: verify.StatusMismatch, Detail: "row count differs",
		SourceRows: 10, ReconstructRows: 9, Anchor: "mysql-bin.000123:456",
	}
	got := toWireResult(res, true)
	want := console.VerifyTableResult{
		Schema: "wp", Table: "posts", Status: "mismatch",
		Reason: "row count differs", Detail: "row count differs",
		SourceRows: 10, ReconstructRows: 9, Anchor: "mysql-bin.000123:456", Explainable: true,
	}
	if got != want {
		t.Errorf("toWireResult = %+v, want %+v", got, want)
	}

	// The kind must survive the engine→wire copy (#1416). Pinned with a value
	// set, not by the zero struct: the walk and the report each had their own
	// tests, and the engine-side copy stayed green with the field deleted —
	// this is the console's instance of the same seam.
	// The recover-inputs counters must survive the copy too (#1425 review:
	// the console rendered a counts column these fields never reached).
	counted := toWireResult(verify.TableResult{
		Schema: "wp", Table: "orders", Status: verify.StatusMatch,
		EventsChecked: 120, ChainsChecked: 7,
	}, false)
	if counted.EventsChecked != 120 || counted.ChainsChecked != 7 {
		t.Errorf("counters = %d/%d, want 120/7 — the walk's counts must reach the wire",
			counted.EventsChecked, counted.ChainsChecked)
	}

	quiet := toWireResult(verify.TableResult{
		Schema: "wp", Table: "quiet", Status: verify.StatusInconclusive,
		Detail: "no changes in the window", InconclusiveKind: verify.InconclusiveNoActivity,
	}, false)
	if quiet.InconclusiveKind != string(verify.InconclusiveNoActivity) {
		t.Errorf("InconclusiveKind = %q, want %q — the console renders every quiet table as attention without it",
			quiet.InconclusiveKind, verify.InconclusiveNoActivity)
	}
	if got := toWireResult(res, false); got.Explainable {
		t.Error("explainable must be exactly what the caller passed, not re-derived from Status")
	}

	// An unrecognized engine status is normalized BEFORE it reaches the wire —
	// the same verify.NormalizeStatus decision the CLI's JSON report applies,
	// so the console can never serialize a status a consumer's switch would
	// fall through (#1127).
	bad := toWireResult(verify.TableResult{Schema: "wp", Table: "x", Status: "bogus", Detail: "who knows"}, false)
	if bad.Status != string(verify.StatusError) {
		t.Errorf("unknown status serialized as %q, want %q", bad.Status, verify.StatusError)
	}
	if !strings.Contains(bad.Reason, "unrecognized verify status") || !strings.Contains(bad.Reason, "who knows") {
		t.Errorf("unknown status reason lost the cause: %q", bad.Reason)
	}
	if bad.Detail != bad.Reason {
		t.Errorf("detail (legacy alias) = %q, must equal reason %q", bad.Detail, bad.Reason)
	}
}

// TestIndexDBName covers the tolerant DSN-parse-failure handling (mirrors
// internal/cli/verify.go: a bad DSN never crashes DB-name resolution, it's
// simply left empty).
func TestIndexDBName(t *testing.T) {
	if got := indexDBName("idx:pw@tcp(127.0.0.1:3306)/binlog_index"); got != "binlog_index" {
		t.Errorf("indexDBName = %q, want binlog_index", got)
	}
	if got := indexDBName("not a dsn"); got != "" {
		t.Errorf("indexDBName(invalid) = %q, want empty", got)
	}
}

// TestVerifySupervisor_Status_idleDefault: a server never triggered here
// reports idle, never a zero-value State.
func TestVerifySupervisor_Status_idleDefault(t *testing.T) {
	s := newVerifySupervisor(context.Background(), nil, nil)
	if got := s.Status("never-triggered"); got.State != "idle" {
		t.Errorf("Status = %+v, want State=idle", got)
	}
}

// TestVerifySupervisor_Trigger_collision: a job already running for a server
// refuses a second Trigger with ErrVerifyRunning and leaves the running job
// untouched — exercises the exact single-flight guard the whole "one run at
// a time per server" invariant depends on.
func TestVerifySupervisor_Trigger_collision(t *testing.T) {
	s := newVerifySupervisor(context.Background(), nil, nil)
	s.jobs["srv1"] = &verifyJob{status: console.VerifyStatus{State: "running", Since: "t0"}}

	err := s.Trigger(console.VerifyRequest{ServerID: "srv1"})
	if !errors.Is(err, console.ErrVerifyRunning) {
		t.Fatalf("Trigger while running: err = %v, want ErrVerifyRunning", err)
	}
	if got := s.Status("srv1"); got.Since != "t0" {
		t.Errorf("a rejected Trigger must not touch the in-flight job, got %+v", got)
	}
}

// TestVerifySupervisor_appendResult_accumulatesSummary: results grow in
// call order and each status bucket (including an unrecognized status
// falling through to Error — the tally goes through verify.Summary.Count,
// the same classification the CLI's JSON report uses) is tallied correctly —
// this is the exact bookkeeping "as they land" polling depends on.
func TestVerifySupervisor_appendResult_accumulatesSummary(t *testing.T) {
	s := newVerifySupervisor(context.Background(), nil, nil)
	s.jobs["srv1"] = &verifyJob{status: console.VerifyStatus{State: "running"}}

	s.appendResult("srv1", console.VerifyTableResult{Schema: "wp", Table: "posts", Status: "match"})
	s.appendResult("srv1", console.VerifyTableResult{Schema: "wp", Table: "users", Status: "mismatch"})
	s.appendResult("srv1", console.VerifyTableResult{Schema: "wp", Table: "opts", Status: "inconclusive"})
	s.appendResult("srv1", console.VerifyTableResult{Schema: "wp", Table: "logs", Status: "inconclusive", InconclusiveKind: "no-activity"})
	s.appendResult("srv1", console.VerifyTableResult{Schema: "wp", Table: "bad", Status: "something-unrecognized"})

	got := s.Status("srv1")
	if len(got.Results) != 5 || got.Results[0].Table != "posts" || got.Results[4].Table != "bad" {
		t.Fatalf("Results = %+v, want 5 entries in append order", got.Results)
	}
	// The kind-less inconclusive counts on the attention side and the benign
	// one in the split — the tally must go through CountWithKind, or every
	// quiet table renders amber in the console summary (#1416).
	want := console.VerifySummary{Match: 1, Mismatch: 1, Inconclusive: 2, InconclusiveNothingToCheck: 1, Error: 1, Total: 5}
	if got.Summary != want {
		t.Errorf("Summary = %+v, want %+v (an unrecognized status must fall through to Error)", got.Summary, want)
	}

	// appendResult on an unknown server id (job cleared out from under it) is
	// a defensive no-op, never a panic.
	s.appendResult("unknown-server", console.VerifyTableResult{Schema: "a", Table: "b", Status: "match"})
}

// TestVerifySupervisor_setNote_doesNotChangeState: setNote must leave State
// as "running" — finish() is the sole terminal-state transition. Regression
// guard for the race where setNote setting State="succeeded" itself let a
// second concurrent Trigger in before the goroutine's own finish() ran,
// which could suppress that second run's real failure.
func TestVerifySupervisor_setNote_doesNotChangeState(t *testing.T) {
	s := newVerifySupervisor(context.Background(), nil, nil)
	s.jobs["srv1"] = &verifyJob{status: console.VerifyStatus{State: "running"}}

	s.setNote("srv1", "only one baseline exists for this server yet; nothing to compare")
	if got := s.Status("srv1"); got.State != "running" || got.Note == "" {
		t.Fatalf("after setNote: %+v, want State still running with Note set", got)
	}
	// A concurrent Trigger must still see this job as in-flight.
	if err := s.Trigger(console.VerifyRequest{ServerID: "srv1"}); !errors.Is(err, console.ErrVerifyRunning) {
		t.Errorf("Trigger after setNote (before finish): err = %v, want ErrVerifyRunning", err)
	}

	s.finish("srv1", nil)
	got := s.Status("srv1")
	if got.State != "succeeded" || got.Note == "" || got.FinishedAt == "" {
		t.Errorf("after finish: %+v, want State=succeeded with Note preserved and FinishedAt set", got)
	}
}

// TestVerifySupervisor_Explain_preconditions: all three "nothing to explain"
// cases refuse with ErrExplainUnavailable and never dial a database — no job
// for the server, a live-source run (no explain support in the engine), and
// a baseline-anchored run where the requested table was never a mismatch.
func TestVerifySupervisor_Explain_preconditions(t *testing.T) {
	s := newVerifySupervisor(context.Background(), nil, nil)

	if _, err := s.Explain("no-such-server", "wp", "posts"); !errors.Is(err, console.ErrExplainUnavailable) {
		t.Errorf("no job: err = %v, want ErrExplainUnavailable", err)
	}

	s.jobs["live"] = &verifyJob{status: console.VerifyStatus{State: "succeeded"}, mode: console.VerifyModeLiveSource}
	if _, err := s.Explain("live", "wp", "posts"); !errors.Is(err, console.ErrExplainUnavailable) {
		t.Errorf("live-source run: err = %v, want ErrExplainUnavailable (no explain support in that mode)", err)
	}

	s.jobs["baseline"] = &verifyJob{status: console.VerifyStatus{State: "succeeded"}, mode: console.VerifyModeBaselineAnchored}
	if _, err := s.Explain("baseline", "wp", "never_mismatched"); !errors.Is(err, console.ErrExplainUnavailable) {
		t.Errorf("table never cached as a mismatch: err = %v, want ErrExplainUnavailable", err)
	}
}

// explainableSupervisor returns a supervisor whose "s1" job has one cached
// mismatch pair, so Explain gets past its preconditions and starts real work.
// The index DSN points at a port nothing listens on: the reconstruction fails
// fast at connect, which is exactly what these tests want — they are about the
// job LIFECYCLE (running → finished → consumed), not about the diff engine.
func explainableSupervisor(t *testing.T) *verifySupervisor {
	t.Helper()
	s := newVerifySupervisor(context.Background(), nil, nil)
	s.jobs["s1"] = &verifyJob{
		status:   console.VerifyStatus{State: "succeeded"},
		mode:     console.VerifyModeBaselineAnchored,
		indexDSN: "u:p@tcp(127.0.0.1:1)/idx",
		pairs:    map[string]verify.BaselinePair{"wp.posts": {}},
	}
	return s
}

// waitExplain polls Explain the way the console does, returning the first
// answer that is not "still running".
func waitExplain(t *testing.T, s *verifySupervisor, schema, table string) (*console.VerifyExplanation, error) {
	t.Helper()
	for i := 0; i < 200; i++ {
		ex, err := s.Explain("s1", schema, table)
		if !errors.Is(err, console.ErrExplainRunning) {
			return ex, err
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("explain never left the running state")
	return nil, nil
}

// TestVerifySupervisor_ExplainIsAsync pins #1375's core contract: Explain
// returns immediately with ErrExplainRunning and delivers the outcome on a
// later call. A blocking implementation cannot outlive a fronting proxy's
// read timeout, which made the button look dead on exactly the large tables
// the drill-down exists for.
func TestVerifySupervisor_ExplainIsAsync(t *testing.T) {
	s := explainableSupervisor(t)

	if _, err := s.Explain("s1", "wp", "posts"); !errors.Is(err, console.ErrExplainRunning) {
		t.Fatalf("first call: err = %v, want ErrExplainRunning (it must not block on the reconstruction)", err)
	}

	// The work fails at connect (nothing listens on port 1), so the terminal
	// answer is that error — surfaced, not swallowed, and not ErrExplainRunning.
	_, err := waitExplain(t, s, "wp", "posts")
	if err == nil {
		t.Fatal("finished explain returned no error although the index is unreachable")
	}
	if errors.Is(err, console.ErrExplainRunning) || errors.Is(err, console.ErrExplainUnavailable) {
		t.Errorf("terminal err = %v, want the underlying failure", err)
	}

	// Reading a finished job consumes it, so a retry re-runs instead of
	// replaying the old failure forever.
	s.mu.Lock()
	_, still := s.explains[explainKey("s1", "wp", "posts")]
	s.mu.Unlock()
	if still {
		t.Error("finished explain stayed cached; a retry would replay the stale error instead of re-running")
	}
}

// TestVerifySupervisor_ExplainReentryGuard: polling (or an impatient second
// click) must not launch a second reconstruction of the same table — that is
// minutes of DuckDB work per extra job on a shared daemon.
func TestVerifySupervisor_ExplainReentryGuard(t *testing.T) {
	s := explainableSupervisor(t)
	// Pre-seed an in-flight job so the guard is exercised without racing a
	// real one to completion.
	key := explainKey("s1", "wp", "posts")
	s.mu.Lock()
	first := &explainJob{}
	s.explains[key] = first
	s.mu.Unlock()

	for i := 0; i < 3; i++ {
		if _, err := s.Explain("s1", "wp", "posts"); !errors.Is(err, console.ErrExplainRunning) {
			t.Fatalf("poll %d: err = %v, want ErrExplainRunning", i, err)
		}
	}
	s.mu.Lock()
	got, ok := s.explains[key]
	n := len(s.explains)
	s.mu.Unlock()
	if !ok || got != first {
		t.Error("a poll replaced the in-flight job — a second reconstruction was started")
	}
	if n != 1 {
		t.Errorf("explains has %d entries, want 1", n)
	}
}

// TestVerifySupervisor_ExplainInvalidatedByNewRun: an explanation belongs to
// the BaselinePair of the run that produced its verdict, so a new run must
// drop it. Serving it afterwards would explain a mismatch the displayed
// results no longer claim.
func TestVerifySupervisor_ExplainInvalidatedByNewRun(t *testing.T) {
	s := explainableSupervisor(t)
	s.mu.Lock()
	s.explains[explainKey("s1", "wp", "posts")] = &explainJob{done: true, result: &console.VerifyExplanation{Schema: "wp", Table: "posts"}}
	s.explains[explainKey("other", "wp", "posts")] = &explainJob{done: true, result: &console.VerifyExplanation{Schema: "wp", Table: "posts"}}
	s.mu.Unlock()

	if _, err := s.begin(console.VerifyRequest{ServerID: "s1", Mode: console.VerifyModeBaselineAnchored}, console.VerifyTriggerManual); err != nil {
		t.Fatalf("begin: %v", err)
	}

	s.mu.Lock()
	_, stale := s.explains[explainKey("s1", "wp", "posts")]
	_, otherKept := s.explains[explainKey("other", "wp", "posts")]
	s.mu.Unlock()
	if stale {
		t.Error("a new run left the previous run's drill-down cached")
	}
	if !otherKept {
		t.Error("a run on one server dropped another server's cached drill-down")
	}
}

// TestVerifySupervisor_ExplainCancelsInvalidatedWork: dropping the map entry
// is not enough. The goroutine would keep reconstructing for minutes on the
// daemon that also runs capture, for a result finishExplain is guaranteed to
// discard — and with the entry gone, the re-entry guard no longer stops a
// click on the same table from starting a SECOND one alongside it.
func TestVerifySupervisor_ExplainCancelsInvalidatedWork(t *testing.T) {
	s := explainableSupervisor(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	other, cancelOther := context.WithCancel(context.Background())
	defer cancelOther()
	s.mu.Lock()
	s.explains[explainKey("s1", "wp", "posts")] = &explainJob{cancel: cancel}
	s.explains[explainKey("other", "wp", "posts")] = &explainJob{cancel: cancelOther}
	s.mu.Unlock()

	if _, err := s.begin(console.VerifyRequest{ServerID: "s1", Mode: console.VerifyModeBaselineAnchored}, console.VerifyTriggerManual); err != nil {
		t.Fatalf("begin: %v", err)
	}

	select {
	case <-ctx.Done():
	default:
		t.Error("a new run abandoned an in-flight drill-down without canceling it; it keeps burning DuckDB for a result nobody can be served")
	}
	select {
	case <-other.Done():
		t.Error("a run on one server canceled another server's in-flight drill-down")
	default:
	}
}

// TestVerifySupervisor_ExplainWorkSeesTheCancel pins the WIRING, not the two
// halves: that the context Explain hands the work is the one a new run
// cancels. Both are easy to get right separately and still leave the cancel
// decorative, which would make explainJob.cancel's promise to stop burning
// DuckDB false while every other test stayed green.
func TestVerifySupervisor_ExplainWorkSeesTheCancel(t *testing.T) {
	s := explainableSupervisor(t)
	started := make(chan struct{})
	observed := make(chan error, 1)
	s.explainFn = func(ctx context.Context, _ string, _ bool, _, _ string, _ verify.BaselinePair) (*console.VerifyExplanation, error) {
		close(started)
		<-ctx.Done()
		observed <- ctx.Err()
		return nil, ctx.Err()
	}

	if _, err := s.Explain("s1", "wp", "posts"); !errors.Is(err, console.ErrExplainRunning) {
		t.Fatalf("Explain: err = %v, want ErrExplainRunning", err)
	}
	<-started
	if _, err := s.begin(console.VerifyRequest{ServerID: "s1", Mode: console.VerifyModeBaselineAnchored}, console.VerifyTriggerManual); err != nil {
		t.Fatalf("begin: %v", err)
	}

	select {
	case err := <-observed:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the work saw %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a new run did not reach the running drill-down: the work is on a context the purge cannot stop, so it keeps burning DuckDB for a result that will be discarded")
	}
}

// TestExplainLogVerdict: a canceled drill-down is one WE abandoned, so it
// must not be reported as a failure. Without this, every scheduled run that
// overlaps an open drill-down logs an error, and the real failures this
// logging exists to surface drown in routine noise.
func TestExplainLogVerdict(t *testing.T) {
	canceled := fmt.Errorf("explain wp.posts: %w", context.Canceled)
	for _, tc := range []struct {
		name string
		err  error
		live bool
		want slog.Level
	}{
		{"canceled by a new run", canceled, false, slog.LevelDebug},
		{"canceled at shutdown", canceled, true, slog.LevelDebug},
		{"failed and still wanted", errors.New("connect index: refused"), true, slog.LevelError},
		{"failed after superseded", errors.New("connect index: refused"), false, slog.LevelError},
		{"succeeded but superseded", nil, false, slog.LevelWarn},
		{"succeeded and delivered", nil, true, slog.LevelDebug},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := explainLogVerdict(tc.err, tc.live); got != tc.want {
				t.Errorf("level = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestVerifySupervisor_ExplainSurvivesPanic pins the guard whose failure mode
// is the worst in this file: the drill-down goroutine is bare, so before the
// recover a panic in the reconstruct path took down the process — and under
// `watch` that process is also the capture plane, so a click on Explain would
// stop binlog capture. The panic must instead land as this job's error.
func TestVerifySupervisor_ExplainSurvivesPanic(t *testing.T) {
	s := explainableSupervisor(t)
	s.explainFn = func(context.Context, string, bool, string, string, verify.BaselinePair) (*console.VerifyExplanation, error) {
		panic("boom from the reconstruct path")
	}

	if _, err := s.Explain("s1", "wp", "posts"); !errors.Is(err, console.ErrExplainRunning) {
		t.Fatalf("Explain: err = %v, want ErrExplainRunning", err)
	}
	_, err := waitExplain(t, s, "wp", "posts")
	if err == nil {
		t.Fatal("a panicking drill-down reported success")
	}
	if !strings.Contains(err.Error(), "boom from the reconstruct path") {
		t.Errorf("terminal err = %v, want the panic value surfaced to the operator", err)
	}
}

// TestVerifySupervisor_FinishExplainChecksJobIdentity is the other end of the
// invalidation invariant, and the sequence that actually happens in
// production: click Explain, let a new run start before the reconstruction
// lands. A key-presence check would pass here and publish a drill-down
// computed against the PREVIOUS run's BaselinePair under the current run's
// verdict — wrong evidence on a forensics surface, not merely stale UI.
func TestVerifySupervisor_FinishExplainChecksJobIdentity(t *testing.T) {
	s := explainableSupervisor(t)
	key := explainKey("s1", "wp", "posts")
	superseded := &explainJob{}
	stale := &console.VerifyExplanation{Schema: "wp", Table: "posts"}

	// Leg 1: a new run deleted the key (begin's purge). The late result must
	// not resurrect it — the next click has to recompute against the new pair.
	s.mu.Lock()
	s.explains[key] = superseded
	delete(s.explains, key)
	s.mu.Unlock()

	s.finishExplain(key, superseded, stale, nil)

	s.mu.Lock()
	_, resurrected := s.explains[key]
	s.mu.Unlock()
	if resurrected {
		t.Error("a superseded drill-down re-created its own cache entry; the next poll would serve the previous run's diff")
	}

	// Leg 2: the key was re-created by a LATER request. The superseded
	// goroutine must leave that job alone.
	current := &explainJob{}
	s.mu.Lock()
	s.explains[key] = current
	s.mu.Unlock()

	s.finishExplain(key, superseded, stale, nil)

	s.mu.Lock()
	got := s.explains[key]
	done, res := got.done, got.result
	s.mu.Unlock()
	if got != current {
		t.Fatal("the superseded goroutine replaced the current job")
	}
	if done || res != nil {
		t.Error("a superseded goroutine published its result into a later request's job — the operator would see a diff against the wrong snapshot pair")
	}

	// And the live case still publishes, or the identity check would be a
	// guard that never lets anything through.
	s.finishExplain(key, current, stale, nil)
	s.mu.Lock()
	done, res = s.explains[key].done, s.explains[key].result
	s.mu.Unlock()
	if !done || res != stale {
		t.Error("finishExplain did not publish to the job that requested it")
	}
}

// TestVerifySupervisor_ExplainReturnsFinishedResult covers the delivery half:
// the other tests all drive failures (the helper's index is unreachable on
// purpose), so without this one no test ever carries a real explanation back
// out through Explain.
func TestVerifySupervisor_ExplainReturnsFinishedResult(t *testing.T) {
	s := explainableSupervisor(t)
	key := explainKey("s1", "wp", "posts")
	want := &console.VerifyExplanation{Schema: "wp", Table: "posts"}
	s.mu.Lock()
	s.explains[key] = &explainJob{done: true, result: want}
	s.mu.Unlock()

	got, err := s.Explain("s1", "wp", "posts")
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if got != want {
		t.Fatalf("Explain returned %v, want the cached explanation", got)
	}
	s.mu.Lock()
	_, still := s.explains[key]
	s.mu.Unlock()
	if still {
		t.Error("a delivered explanation stayed cached; it holds rendered diff text nobody will read again")
	}
}

// TestVerifySupervisor_ExplainEvictsOnlyFinished pins the eviction policy the
// const's comment claims: dropping an in-flight entry would abandon work still
// running and make the next poll restart it — the pile-up this cache exists to
// prevent.
func TestVerifySupervisor_ExplainEvictsOnlyFinished(t *testing.T) {
	s := explainableSupervisor(t)
	inFlight := &explainJob{}
	s.mu.Lock()
	s.explains[explainKey("s1", "wp", "busy")] = inFlight
	for i := 1; i < maxCachedExplains; i++ {
		s.explains[explainKey("s1", "wp", fmt.Sprintf("t%d", i))] = &explainJob{done: true}
	}
	n := len(s.explains)
	s.mu.Unlock()
	if n != maxCachedExplains {
		t.Fatalf("seeded %d entries, want %d", n, maxCachedExplains)
	}

	// wp.posts is the one table the helper cached a pair for, so this is the
	// request that trips the eviction.
	if _, err := s.Explain("s1", "wp", "posts"); !errors.Is(err, console.ErrExplainRunning) {
		t.Fatalf("Explain: err = %v, want ErrExplainRunning", err)
	}

	s.mu.Lock()
	kept, ok := s.explains[explainKey("s1", "wp", "busy")]
	_, started := s.explains[explainKey("s1", "wp", "posts")]
	n = len(s.explains)
	s.mu.Unlock()
	if !ok || kept != inFlight {
		t.Error("eviction dropped an in-flight drill-down; its next poll would restart minutes of work already running")
	}
	if !started {
		t.Error("the request that tripped eviction did not register its own job")
	}
	if n != 2 {
		t.Errorf("explains has %d entries after eviction, want 2 (the in-flight one plus the new request)", n)
	}
}

// The read a table was compared against reaches the console under the CLI's
// name and format, and a table compared to nothing carries none.
func TestToWireResult_comparedTo(t *testing.T) {
	read := time.Date(2026, 9, 2, 3, 0, 0, 0, time.FixedZone("ART", -3*3600))
	got := toWireResult(verify.TableResult{Schema: "wp", Table: "posts", Status: verify.StatusMatch, ComparedTo: read}, false)
	if got.ComparedTo != "2026-09-02T06:00:00Z" {
		t.Errorf("compared_to = %q, want the read in RFC3339 UTC", got.ComparedTo)
	}
	if got := toWireResult(verify.TableResult{Schema: "wp", Table: "posts", Status: verify.StatusInconclusive}, false); got.ComparedTo != "" {
		t.Errorf("compared_to = %q on a table compared to nothing, want empty", got.ComparedTo)
	}
}
