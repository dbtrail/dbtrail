//go:build integration

package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// guardedIndex2328 is an index the loop's upgrade guard holds back under an
// implicit window: no retention record, history 100 days deep.
func guardedIndex2328(t *testing.T) string {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	now := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{now.Add(-100 * 24 * time.Hour), now})
	return testutil.IntegrationDSN(dbName)
}

func capacityOf(t *testing.T, checks []doctor.CheckResult) doctor.CheckResult {
	t.Helper()
	for _, c := range checks {
		if c.Name == doctor.CapacityCheckName {
			return c
		}
	}
	t.Fatalf("the report never reached %q, so it proves nothing", doctor.CapacityCheckName)
	return doctor.CheckResult{}
}

const heldBack2328 = "this daemon, started with no retention of its own, drops none of it"

// #2328: the web interface's Checks report graded every source's index over
// the daemon's boot-time retention. It now asks what the loop does with THAT
// index, with the loop's live settings.
func TestMonitorDoctor_gradesTheWindowTheLoopUses_2328(t *testing.T) {
	dsn := guardedIndex2328(t)
	entry := console.ServerEntry{SourceDSN: dsn, DSN: dsn}
	implicit := rotation.Settings{Enabled: true, Retain: 48 * time.Hour, RetainRaw: "48h", Interval: time.Hour}
	explicit := implicit
	explicit.Explicit = true

	report := func(t *testing.T, s func() rotation.Settings) console.DoctorCheck {
		t.Helper()
		r, err := newMonitorSupervisor(context.Background(), dsn, nil, s).Doctor(context.Background(), entry)
		if err != nil {
			t.Fatal(err)
		}
		return checkNamed(t, r, doctor.CapacityCheckName)
	}

	t.Run("no retention chosen: the report says the history is held back", func(t *testing.T) {
		c := report(t, func() rotation.Settings { return implicit })
		if !strings.Contains(c.Detail, heldBack2328) {
			t.Fatalf("detail does not say the loop is holding the history back:\n%s", c.Detail)
		}
	})
	t.Run("a retention chosen (flag, env or saved in the web interface): the guard does not apply", func(t *testing.T) {
		c := report(t, func() rotation.Settings { return explicit })
		// Anchored on the line the check DOES print for this index, so an
		// empty or "cannot connect" detail cannot pass as "not held back".
		if !strings.Contains(c.Detail, "not enough recent history") || strings.Contains(c.Detail, "drops none of it") {
			t.Fatalf("an explicit window was reported as held back (or the check did not run):\n%s", c.Detail)
		}
	})
	t.Run("the settings are read per report, like the loop reads them per cycle", func(t *testing.T) {
		cur := implicit
		sup := newMonitorSupervisor(context.Background(), dsn, nil, func() rotation.Settings { return cur })
		first, err := sup.Doctor(context.Background(), entry)
		if err != nil {
			t.Fatal(err)
		}
		cur = explicit // the operator saves a retention in the web interface
		second, err := sup.Doctor(context.Background(), entry)
		if err != nil {
			t.Fatal(err)
		}
		a, b := checkNamed(t, first, doctor.CapacityCheckName), checkNamed(t, second, doctor.CapacityCheckName)
		if !strings.Contains(a.Detail, "drops none of it") || strings.Contains(b.Detail, "drops none of it") {
			t.Fatalf("before: %s\nafter: %s\nwant held back, then not", a.Detail, b.Detail)
		}
	})
}

// The watch preflight is on the capture boot path. Same resolution, and for
// an index that does not exist yet (a first boot) exactly the report it gave
// before it asked the index.
func TestWatchPreflight_window_2328(t *testing.T) {
	saved := []any{upSourceDSN, upIndexDSN, upSchemas, upRotationCfg, upConsoleServersFile}
	t.Cleanup(func() {
		upSourceDSN, upIndexDSN, upSchemas, upRotationCfg = saved[0].(string), saved[1].(string), saved[2].(string), saved[3].(rotation.Settings)
		upConsoleServersFile = saved[4].(string)
	})
	// A registry file of this test's own: the preflight reads the saved
	// rotation policy, and the developer's real one must not decide a test.
	regPath := filepath.Join(t.TempDir(), "console-servers.yaml")
	upConsoleServersFile = regPath
	implicit := rotation.Settings{Enabled: true, Retain: 48 * time.Hour, RetainRaw: "48h", Interval: time.Hour}

	dsn := guardedIndex2328(t)
	upSourceDSN, upIndexDSN, upSchemas, upRotationCfg = dsn, dsn, "", implicit
	if c := capacityOf(t, watchPreflight(context.Background()).Checks); !strings.Contains(c.Detail, heldBack2328) {
		t.Fatalf("the boot preflight does not say the history is held back:\n%s", c.Detail)
	}
	explicit := implicit
	explicit.Explicit = true
	upRotationCfg = explicit
	if c := capacityOf(t, watchPreflight(context.Background()).Checks); !strings.Contains(c.Detail, "not enough recent history") || strings.Contains(c.Detail, "drops none of it") {
		t.Fatalf("--rotate-retain was given and the preflight still reports the guard:\n%s", c.Detail)
	}

	// A retention saved in the web interface is an explicit choice too, and
	// the preflight sees it before the daemon loads its registry.
	upRotationCfg = implicit
	reg, err := console.LoadRegistry(regPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.SetRotation(console.RotationConfig{Retain: "7d", Interval: "1h", AddFuture: 3}); err != nil {
		t.Fatal(err)
	}
	if c := capacityOf(t, watchPreflight(context.Background()).Checks); !strings.Contains(c.Detail, "not enough recent history") || strings.Contains(c.Detail, "drops none of it") {
		t.Fatalf("7d is saved in the web interface and the preflight still reports the guard:\n%s", c.Detail)
	}
	if err := os.Remove(regPath); err != nil {
		t.Fatal(err)
	}

	// First boot: the index database is not there yet.
	missing := testutil.IntegrationDSN("bintrail_2328_never_created")
	upIndexDSN, upRotationCfg = missing, implicit
	got := watchPreflight(context.Background())
	want := doctor.Build(context.Background(), upSourceDSN, missing, "", implicit.Retain, doctor.WithSourceSSL(bootSourceSSL()))
	if got.Failed != want.Failed || got.Warnings != want.Warnings || got.Skipped != want.Skipped || got.Passed != want.Passed || len(got.Checks) != len(want.Checks) {
		t.Fatalf("a first boot's preflight changed: got %d/%d/%d/%d over %d checks, want %d/%d/%d/%d over %d",
			got.Passed, got.Failed, got.Warnings, got.Skipped, len(got.Checks), want.Passed, want.Failed, want.Warnings, want.Skipped, len(want.Checks))
	}
	for i := range got.Checks {
		if got.Checks[i].Name != want.Checks[i].Name || got.Checks[i].Status != want.Checks[i].Status {
			t.Errorf("check %q: status %s, want %s (%s)", got.Checks[i].Name, got.Checks[i].Status, want.Checks[i].Status, got.Checks[i].Detail)
		}
	}
}
