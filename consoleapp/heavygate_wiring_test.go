package consoleapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/verify"
)

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCheckLane_noteWhileWaiting: a check table that has to wait for a
// snapshot job says so in the run's note, and the note is gone once the
// table runs.
func TestCheckLane_noteWhileWaiting(t *testing.T) {
	g := newHeavyGate()
	s := newVerifySupervisor(context.Background(), nil, nil)
	s.gate = g
	if _, err := s.begin(console.VerifyRequest{ServerID: "a", Mode: console.VerifyModeBaselineAnchored}, console.VerifyTriggerManual); err != nil {
		t.Fatal(err)
	}
	relSnap := mustAcquire(t, g, laneSnapshot)
	ran := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- s.inCheckLane(context.Background(), "a", func(context.Context) { ran <- s.Status("a").Note })
	}()
	eventually(t, "the waiting note", func() bool { return s.Status("a").Note == checkLaneWaitNote })
	select {
	case <-ran:
		t.Fatal("the table ran while a snapshot job held the lane")
	default:
	}
	relSnap()
	if note := <-ran; note != "" {
		t.Errorf("note while the table runs = %q, want it cleared", note)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestCheckLane_canceledWait: a check stopped while it waits ends with the
// interruption, without running the table.
func TestCheckLane_canceledWait(t *testing.T) {
	g := newHeavyGate()
	s := &verifySupervisor{gate: g, jobs: map[string]*verifyJob{}}
	defer mustAcquire(t, g, laneSnapshot)()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.inCheckLane(ctx, "a", func(context.Context) { t.Error("the table ran after the check was stopped") })
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "verify interrupted") {
		t.Fatalf("err = %v", err)
	}
}

// TestExplain_waitsForSnapshotJob: an Explain does not start while a snapshot
// job runs, and starts when it ends.
func TestExplain_waitsForSnapshotJob(t *testing.T) {
	g := newHeavyGate()
	s := newVerifySupervisor(context.Background(), nil, nil)
	s.gate = g
	started := make(chan struct{}, 1)
	s.explainFn = func(context.Context, string, bool, string, string, verify.BaselinePair) (*console.VerifyExplanation, error) {
		started <- struct{}{}
		return &console.VerifyExplanation{}, nil
	}
	if _, err := s.begin(console.VerifyRequest{ServerID: "a", Mode: console.VerifyModeBaselineAnchored}, console.VerifyTriggerManual); err != nil {
		t.Fatal(err)
	}
	s.cachePair("a", "db.t", verify.BaselinePair{Schema: "db", Table: "t"})
	relSnap := mustAcquire(t, g, laneSnapshot)
	if _, err := s.Explain("a", "db", "t"); !errors.Is(err, console.ErrExplainRunning) {
		t.Fatalf("Explain = %v, want it started", err)
	}
	select {
	case <-started:
		t.Fatal("the Explain ran while a snapshot job held the lane")
	case <-time.After(50 * time.Millisecond):
	}
	relSnap()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the Explain never ran after the snapshot job ended")
	}
}

// TestSnapshotFold_waitsForTheLane: the fold a refresh or a restore runs
// waits while another server's snapshot work holds the lane, runs when it is
// given back, and gives it back even when the fold panics.
func TestSnapshotFold_waitsForTheLane(t *testing.T) {
	g := newHeavyGate()
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.LockModeSafeNoLock)
	s.gate = g
	folded := make(chan struct{}, 2)
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folded <- struct{}{}
		return nil, nil, nil
	}
	relOther := mustAcquire(t, g, laneSnapshot)
	done := make(chan struct{})
	wait := new(time.Duration)
	go func() {
		defer close(done)
		s.foldSnapshot(refreshRequest{ServerID: "a", ServerName: "a", laneWait: wait}, time.Now(), []string{"db.t"})
	}()
	select {
	case <-folded:
		t.Fatal("the fold ran while another server's snapshot work held the lane")
	case <-time.After(50 * time.Millisecond):
	}
	relOther()
	<-folded
	<-done
	// The wait is handed back so the refresh takes it off UpdateSeconds.
	if *wait < 50*time.Millisecond {
		t.Errorf("laneWait = %v, want the time the fold stood in line (>= 50ms)", *wait)
	}

	foldTables = func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		panic("boom")
	}
	func() {
		defer func() { _ = recover() }()
		s.foldSnapshot(refreshRequest{ServerID: "a", ServerName: "a"}, time.Now(), []string{"db.t"})
	}()
	mustAcquire(t, g, laneSnapshot)() // the panicking fold gave the lane back
}

// TestSnapshotFold_stoppedWhileWaiting: a fold still waiting when DBTrail
// shuts down returns the shutdown, without folding.
func TestSnapshotFold_stoppedWhileWaiting(t *testing.T) {
	g := newHeavyGate()
	ctx, cancel := context.WithCancel(context.Background())
	s := newBaselineSupervisor(ctx, t.TempDir(), baseline.LockModeSafeNoLock)
	s.gate = g
	defer mustAcquire(t, g, laneSnapshot)()
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		t.Error("folded after shutdown")
		return nil, nil, nil
	}
	cancel()
	if _, _, _, err := s.foldSnapshot(refreshRequest{ServerID: "a", ServerName: "a"}, time.Now(), []string{"db.t"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the shutdown", err)
	}
}

// TestCompaction_stoppedWhileWaiting: a compaction still waiting for the lane
// when DBTrail shuts down does not leave its slot "running" (which would
// refuse that server's jobs until a restart).
func TestCompaction_stoppedWhileWaiting(t *testing.T) {
	g := newHeavyGate()
	ctx, cancel := context.WithCancel(context.Background())
	s := newBaselineSupervisor(ctx, t.TempDir(), baseline.LockModeSafeNoLock)
	s.gate = g
	defer mustAcquire(t, g, laneSnapshot)()
	if err := s.TriggerCompact(refreshRequest{ServerID: "b", ServerName: "b", BaselineDir: t.TempDir()}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	s.postRefresh.Wait()
	if st := s.compactStatusFor("b"); st.State != "failed" || st.LastError != errStoppedBeforeStart.Error() {
		t.Errorf("compaction after shutdown while waiting = %+v", st)
	}
	s.mu.Lock()
	busy := s.busyLocked("b")
	s.mu.Unlock()
	if busy {
		t.Error("the server is still busy after its waiting compaction stopped")
	}
}

// TestCompaction_waitsForTheLane: a compaction does not merge while another
// server's snapshot work holds the lane, and finishes once it is given back.
func TestCompaction_waitsForTheLane(t *testing.T) {
	g := newHeavyGate()
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.LockModeSafeNoLock)
	s.gate = g
	relOther := mustAcquire(t, g, laneSnapshot)
	if err := s.TriggerCompact(refreshRequest{ServerID: "b", ServerName: "b", BaselineDir: t.TempDir()}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if st := s.compactStatusFor("b"); st.State != "running" || st.FinishedAt != "" {
		t.Fatalf("compaction while another server held the lane = %+v, want still waiting", st)
	}
	relOther()
	s.postRefresh.Wait()
	if st := s.compactStatusFor("b"); st.State != "succeeded" {
		t.Errorf("compaction after the lane was given back = %+v", st)
	}
}
