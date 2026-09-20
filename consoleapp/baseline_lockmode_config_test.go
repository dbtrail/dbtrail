package consoleapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// TestInvalidLockModeDoesNotStopCapture is the guard for the worst way this
// feature could go wrong. Under `watch` the console process is ALSO the capture
// plane, so failing startup over a mistyped baseline setting would turn a typo
// into permanently lost binlog events — an outage of the thing dbtrail exists
// to protect, caused by a knob that only affects snapshots. The refusal has to
// land on baselines and nowhere else.
func TestInvalidLockModeDoesNotStopCapture(t *testing.T) {
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE", "FTWRL") // valid mode, wrong spelling
	defer func() {
		upConsoleBaselineLockMode = baseline.DefaultLockMode
		upConsoleBaselineLockModeErr = nil
	}()
	upConsoleBaselineLockMode = baseline.DefaultLockMode
	upConsoleBaselineLockModeErr = nil

	// A bare command: resolveUpConsoleEnv only consults Changed() on flags it
	// knows, and an absent flag reports false, which is the path under test.
	if err := resolveUpConsoleEnv(&cobra.Command{}); err != nil {
		t.Fatalf("an invalid baseline lock mode failed daemon startup (%v); capture would never run", err)
	}
	if upConsoleBaselineLockModeErr == nil {
		t.Fatal("an invalid lock mode was accepted silently; baselines would run in the default mode the operator did not ask for")
	}
	if !upConsoleBaselineLockMode.PointConsistent() {
		t.Errorf("after refusing an invalid mode the effective mode is %s, which can emit a torn snapshot", upConsoleBaselineLockMode)
	}
}

// TestBaselineSupervisorRefusesOnConfigError: the error recorded at startup has
// to reach the operator where they are looking — the baseline trigger — rather
// than only the daemon log.
func TestBaselineSupervisorRefusesOnConfigError(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.configErr = errors.New("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: unknown lock mode \"FTWRL\"")

	err := sup.Trigger(console.BaselineRequest{ServerID: "s1", ServerName: "prod"})
	if err == nil {
		t.Fatal("a misconfigured supervisor started a baseline anyway")
	}
	if !strings.Contains(err.Error(), "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE") {
		t.Errorf("refusal = %q, want it to name the variable the operator has to fix", err)
	}
	// And it must refuse BEFORE claiming a job slot, or the next trigger would
	// report "already running" for work that never started.
	if got := sup.Status("s1").State; got != "idle" {
		t.Errorf("status after a refused trigger = %q, want idle", got)
	}
}

// TestBaselineWiringCarriesConfigError is the wiring half: the supervisor
// honouring configErr proves nothing if nothing ever sets it. This drives the
// real path — env → resolveUpConsoleEnv → the constructor watch uses → Trigger.
func TestBaselineWiringCarriesConfigError(t *testing.T) {
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE", "NO_LOCK") // valid mode, wrong case
	defer func() {
		upConsoleBaselineLockMode = baseline.DefaultLockMode
		upConsoleBaselineLockModeErr = nil
	}()
	upConsoleBaselineLockMode = baseline.DefaultLockMode
	upConsoleBaselineLockModeErr = nil

	if err := resolveUpConsoleEnv(&cobra.Command{}); err != nil {
		t.Fatalf("startup failed over a baseline setting: %v", err)
	}
	sup := newBaselineSupervisorFromConfig(context.Background(), t.TempDir(), nil)
	if err := sup.Trigger(console.BaselineRequest{ServerID: "s1"}); err == nil {
		t.Fatal("a baseline ran under a mode the operator did not ask for; the config error never reached the supervisor")
	}
	// PostgreSQL is unaffected: it never consults the lock mode, so a
	// MySQL-only typo must not take away a working button.
	if err := sup.Trigger(console.BaselineRequest{ServerID: "pg1", Flavor: console.FlavorPostgres}); err != nil {
		t.Errorf("a Postgres baseline was refused over a MySQL-only setting: %v", err)
	}
	// Accepting the trigger starts REAL work in a goroutine (it will fail on
	// the empty DSN). Wait for it, or its staging writes race t.TempDir's
	// cleanup and fail the test for an unrelated reason.
	for i := 0; i < 200; i++ {
		if sup.Status("pg1").State != "running" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the Postgres baseline never finished")
}
