//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// What the console tests that need a real PostgreSQL source AND a real MySQL
// index share: a source table with its publication, and capture of it as the
// daemon supervises it.

// pgTestWait polls done until it answers true, and fails with what it last
// saw when limit runs out.
func pgTestWait(t *testing.T, what string, limit time.Duration, done func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		ok, seen := done()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s; last seen: %s", what, limit, seen)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// pgTestSource connects to the test PostgreSQL and creates table (columns is
// what goes between its parentheses) at REPLICA IDENTITY FULL, with a
// publication for it. The table, the publication and the slot are dropped
// before and after.
func pgTestSource(t *testing.T, ctx context.Context, table, slot, pub, columns string) (baseDSN string, source *pgconn.PgConn, exec1 func(string)) {
	t.Helper()
	baseDSN = testutil.SkipIfNoPostgres(t)
	source, err := pgconn.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect to the source: %v", err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	exec1 = func(stmt string) {
		t.Helper()
		if _, _, err := pgText(t, source, stmt); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	dropAll := func() {
		bg := context.Background()
		_, _ = source.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub).ReadAll()
		_, _ = source.Exec(bg, "DROP TABLE IF EXISTS "+table).ReadAll()
		// The slot stays active for a moment after capture is told to stop,
		// and an active slot cannot be dropped.
		for range 50 {
			_, _ = source.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '"+slot+"' AND NOT active").ReadAll()
			left, err := source.Exec(bg, "SELECT 1 FROM pg_replication_slots WHERE slot_name = '"+slot+"'").ReadAll()
			if err != nil || len(left) == 0 || len(left[0].Rows) == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	dropAll()
	t.Cleanup(dropAll)
	exec1("CREATE TABLE " + table + " (" + columns + ")")
	exec1("ALTER TABLE " + table + " REPLICA IDENTITY FULL")
	exec1("CREATE PUBLICATION " + pub + " FOR TABLE " + table)
	return baseDSN, source, exec1
}

// pgTestMonitor is the daemon's capture supervisor over a fresh boot index.
// The one change from the daemon is the checkpoint period, 10 s there.
func pgTestMonitor(t *testing.T, ctx context.Context, checkpoint time.Duration) (mon *monitorSupervisor, bootDSN string) {
	t.Helper()
	_, bootName := testutil.CreateTestDB(t)
	bootDSN = testutil.IntegrationDSN(bootName)
	mon = newMonitorSupervisor(ctx, bootDSN, nil, nil)
	mon.pgStreamFn = func(c context.Context, cfg pgstreamrun.Config) error {
		cfg.Checkpoint = checkpoint
		return pgstreamrun.One(c, cfg)
	}
	return mon, bootDSN
}

// pgTestStart starts capture of e (whose index DSN is derived already) and
// returns once it is attached to its slot. The index database is dropped at
// the end.
func pgTestStart(t *testing.T, ctx context.Context, mon *monitorSupervisor, bootDSN string, source *pgconn.PgConn, e console.ServerEntry) {
	t.Helper()
	t.Cleanup(func() {
		if db, err := sql.Open("mysql", bootDSN); err == nil {
			_, _ = db.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + e.ID)
			_ = db.Close()
		}
	})
	if err := mon.Start(ctx, e); err != nil {
		t.Fatalf("start capture: %v", err)
	}
	t.Cleanup(func() { _ = mon.Stop(context.Background(), e.ID) })
	pgTestWait(t, "capture attached to its slot", 60*time.Second, func() (bool, string) {
		_, rows, err := pgText(t, source, "SELECT active::text FROM pg_replication_slots WHERE slot_name = '"+e.SourceSlot+"'")
		if err != nil || len(rows) != 1 {
			return false, "no slot yet"
		}
		return rows[0][0] == "true", "active=" + rows[0][0]
	})
}
