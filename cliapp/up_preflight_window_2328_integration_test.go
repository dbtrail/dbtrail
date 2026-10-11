//go:build integration

package cliapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2328: `up`'s preflight graded the index over the --rotate-retain default
// whatever the loop would do with it. On an index the upgrade guard holds
// back it now says so, and with a retention given it does not.
func TestUpPreflight_window_2328(t *testing.T) {
	saved := []any{upSourceDSN, upIndexDSN, upSchemas, upRotationCfg}
	t.Cleanup(func() {
		upSourceDSN, upIndexDSN, upSchemas, upRotationCfg = saved[0].(string), saved[1].(string), saved[2].(string), saved[3].(rotation.Settings)
	})
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	now := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{now.Add(-100 * 24 * time.Hour), now})
	dsn := testutil.IntegrationDSN(dbName)

	capacity := func(t *testing.T) doctor.CheckResult {
		t.Helper()
		for _, c := range upPreflight(context.Background()).Checks {
			if c.Name == doctor.CapacityCheckName {
				return c
			}
		}
		t.Fatalf("the preflight never reached %q", doctor.CapacityCheckName)
		return doctor.CheckResult{}
	}

	implicit := rotation.Settings{Enabled: true, Retain: 48 * time.Hour, RetainRaw: "48h", Interval: time.Hour}
	upSourceDSN, upIndexDSN, upSchemas, upRotationCfg = dsn, dsn, "", implicit
	if c := capacity(t); !strings.Contains(c.Detail, "this daemon, started with no retention of its own, drops none of it") {
		t.Fatalf("the preflight does not say the history is held back:\n%s", c.Detail)
	}
	explicit := implicit
	explicit.Explicit = true
	upRotationCfg = explicit
	if c := capacity(t); !strings.Contains(c.Detail, "not enough recent history") || strings.Contains(c.Detail, "drops none of it") {
		t.Fatalf("--rotate-retain was given and the preflight still reports the guard:\n%s", c.Detail)
	}
}
