package ext

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// panicLogSignal is a slog handler that closes logged when the recovered
// panic's line reaches it.
type panicLogSignal struct {
	once   sync.Once
	logged chan struct{}
}

func (h *panicLogSignal) Enabled(context.Context, slog.Level) bool { return true }
func (h *panicLogSignal) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "source job panicked") {
		h.once.Do(func() { close(h.logged) })
	}
	return nil
}
func (h *panicLogSignal) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *panicLogSignal) WithGroup(string) slog.Handler      { return h }

func TestRunSourceJobsNoRegistrationIsNoop(t *testing.T) {
	orig := sourceJobs
	sourceJobs = nil
	t.Cleanup(func() { sourceJobs = orig })

	// Must not panic.
	RunSourceJobs(context.Background(), SourceJobInfo{SourceDSN: "s", IndexDSN: "i", Flavor: "mysql"})
}

func TestRunSourceJobsRunsEveryJobWithInfo(t *testing.T) {
	orig := sourceJobs
	sourceJobs = nil
	t.Cleanup(func() { sourceJobs = orig })

	// Jobs run on their own goroutines (no ordering guarantee), so collect
	// through a channel and assert both fire with the exact SourceJobInfo.
	got := make(chan SourceJobInfo, 2)
	RegisterSourceJob(func(_ context.Context, src SourceJobInfo) { got <- src })
	RegisterSourceJob(func(_ context.Context, src SourceJobInfo) { got <- src })

	want := SourceJobInfo{SourceDSN: "user:pass@tcp(h:3306)/db", IndexDSN: "idx-dsn", Flavor: "mariadb"}
	RunSourceJobs(context.Background(), want)

	for i := range 2 {
		select {
		case src := <-got:
			if src != want {
				t.Errorf("job received %+v, want %+v", src, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for registered job %d to run", i+1)
		}
	}
}

// TestRunSourceJobsPanicDoesNotPropagate pins the H-contract the core
// enforces: a panicking job is recovered on its own goroutine (never
// propagated to the daemon) and sibling jobs still run.
func TestRunSourceJobsPanicDoesNotPropagate(t *testing.T) {
	orig := sourceJobs
	sourceJobs = nil
	t.Cleanup(func() { sourceJobs = orig })

	// The panicking job logs from its own goroutine. Without waiting for that
	// line the goroutine outlives this test and writes into whatever logger
	// the next test installed (the race detector failed the package on it).
	sig := &panicLogSignal{logged: make(chan struct{})}
	prev := slog.Default()
	slog.SetDefault(slog.New(sig))
	t.Cleanup(func() { slog.SetDefault(prev) })

	survivorRan := make(chan struct{})
	RegisterSourceJob(func(context.Context, SourceJobInfo) { panic("source job boom") })
	RegisterSourceJob(func(context.Context, SourceJobInfo) { close(survivorRan) })

	// Must not panic the caller — the panic is contained per-goroutine. An
	// uncontained goroutine panic would crash the whole test binary, so the
	// package passing at all is the real assertion.
	RunSourceJobs(context.Background(), SourceJobInfo{Flavor: "mysql"})

	select {
	case <-survivorRan:
	case <-time.After(5 * time.Second):
		t.Fatal("surviving job did not run after a sibling panicked")
	}
	select {
	case <-sig.logged:
	case <-time.After(5 * time.Second):
		t.Fatal("the recovered panic was not logged")
	}
}
