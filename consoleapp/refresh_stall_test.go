package consoleapp

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A published refresh says how long it took, how much of that it waited for
// the snapshot lane, and how much of it the process spent stalled on CPU and
// on storage, sampled on the two sides of the run: the first before the fold,
// the second after it.
func TestRunRefresh_publishedLineCarriesTheStallReadings(t *testing.T) {
	stubGateReads(t)
	root := stageBaselineRoot(t)
	injectFold(t, 0, nil)

	samples := []console.PressureSample{
		{CPU: time.Second, IOErr: "unknown: /sys/fs/cgroup/io.pressure could not be read"},
		{CPU: time.Hour, IO: time.Hour},
	}
	var calls int
	prevP := refreshPressure
	t.Cleanup(func() { refreshPressure = prevP })
	refreshPressure = func() console.PressureSample {
		calls++
		return samples[min(calls, len(samples))-1]
	}
	fold := foldTables
	var callsAtFold int
	foldTables = func(ctx context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		callsAtFold = calls
		return fold(ctx, cfg)
	}

	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: root},
		refreshAt, time.Minute)

	if callsAtFold != 1 || calls != 2 {
		t.Fatalf("pressure read %d time(s) before the fold and %d in all, want 1 and 2", callsAtFold, calls)
	}
	var line string
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.Contains(l, `msg="baseline refresh: published"`) {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no published line in:\n%s", buf.String())
	}
	t.Logf("published line: %s", line)
	for _, want := range []string{
		" took=", " lane_wait=0s",
		// The fake's CPU counter moved an hour over a run of milliseconds:
		// capped at the whole run.
		`process_waiting_for_cpu="100% of the run (`,
		// An unread counter says why, never 0%.
		`process_waiting_for_disk="unknown: /sys/fs/cgroup/io.pressure could not be read"`,
	} {
		if !strings.Contains(line, want) {
			t.Errorf("published line lacks %q", want)
		}
	}
}

// The words the line carries, from a real long run's numbers.
func TestRefreshStallArgs(t *testing.T) {
	got := refreshStallArgs(24*time.Minute, 90*time.Second,
		console.PressureSample{CPU: time.Minute, IO: 0},
		console.PressureSample{CPU: time.Minute + 3*time.Minute, IO: 12 * time.Minute})
	want := []any{"took", 24 * time.Minute, "lane_wait", 90 * time.Second,
		"process_waiting_for_cpu", "13% of the run (3m0s of 24m0s)",
		"process_waiting_for_disk", "50% of the run (12m0s of 24m0s)"}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("arg %d = %v, want %v (all: %v)", i, got[i], want[i], got)
		}
	}
}
