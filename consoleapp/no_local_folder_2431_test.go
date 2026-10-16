package consoleapp

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
)

// #2431: nothing writes a snapshot for a server with no local folder. The
// console's precheck and the schedule refuse it first; these are the belts
// inside the daemon, each refusing before a job slot is taken or a source is
// read, with the same words.

func wantNoLocalFolder(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "no local snapshot folder") {
		t.Fatalf("%s: err = %v, want the no-local-folder refusal", what, err)
	}
}

func TestNoLocalFolder_triggerTakesNoSlot(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	for _, dir := range []string{"", "  "} {
		err := sup.Trigger(console.BaselineRequest{ServerID: "s1", ServerName: "s3only", SourceDSN: "src", S3: "s3://b/p", LocalDir: dir})
		wantNoLocalFolder(t, "Trigger", err)
	}
	sup.mu.Lock()
	defer sup.mu.Unlock()
	if _, ok := sup.jobs["s1"]; ok || sup.busyLocked("s1") {
		t.Fatal("a refused trigger took the job slot")
	}
}

func TestNoLocalFolder_refreshTakesNoSlot(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	_, err := sup.TriggerRefresh(refreshRequest{ServerID: "s1", ServerName: "s3only", BaselineS3: "s3://b/p"}, time.Minute)
	wantNoLocalFolder(t, "TriggerRefresh", err)
	sup.mu.Lock()
	defer sup.mu.Unlock()
	if _, ok := sup.refreshes["s1"]; ok || sup.busyLocked("s1") {
		t.Fatal("a refused refresh took the job slot")
	}
}

func TestNoLocalFolder_fullReadsReadNothing(t *testing.T) {
	calls := countMydumper(t)
	var pgCalls atomic.Int32
	prev := pgBaselineRun
	t.Cleanup(func() { pgBaselineRun = prev })
	pgBaselineRun = func(context.Context, pgbaseline.Config) (pgbaseline.Stats, error) {
		pgCalls.Add(1)
		return pgbaseline.Stats{}, nil
	}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	_, err := sup.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: "src", S3: "s3://b/p"})
	wantNoLocalFolder(t, "execute", err)
	_, _, err = sup.executePG(console.BaselineRequest{ServerID: "pg1", Flavor: console.FlavorPostgres, S3: "s3://b/p"})
	wantNoLocalFolder(t, "executePG", err)
	if calls.Load() != 0 || pgCalls.Load() != 0 {
		t.Fatalf("mydumper ran %d times, the PostgreSQL read %d times, want neither", calls.Load(), pgCalls.Load())
	}
}
