//go:build integration

package pgstreamrun_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Temporary probe: does a graceful restart index the last transaction twice
// with only FULL tables (no skipped table at all)?
func TestTmpRestartDuplicates(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	indexDB, dbName := testutil.CreateTestDB(t)
	const slot, pub, ta = "bintrail_tmpdup", "bintrail_tmpdup_pub", "tmpdup_a"
	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close(ctx)
	drop := func() {
		_, _ = pg.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(ctx, "DROP TABLE IF EXISTS "+ta)
		_, _ = pg.Exec(ctx, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	drop()
	t.Cleanup(drop)
	for _, q := range []string{"CREATE TABLE " + ta + " (id int PRIMARY KEY, v text)", "ALTER TABLE " + ta + " REPLICA IDENTITY FULL", "CREATE PUBLICATION " + pub + " FOR TABLE " + ta} {
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	cfg := pgstreamrun.Config{IndexDSN: testutil.BaseDSN() + "/" + dbName, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN, SlotName: slot, Publication: pub, ServerID: 44, BatchSize: 100, Checkpoint: 200 * time.Millisecond}
	count := func() (n int) { _ = indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name=?", ta).Scan(&n); return }
	run := func() func() {
		c, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- pgstreamrun.One(c, cfg) }()
		waitFor(t, 15*time.Second, func() bool {
			var a bool
			_ = pg.QueryRow(ctx, "SELECT active AND confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&a)
			return a
		}, "slot")
		return func() { cancel(); <-done }
	}
	stop := run()
	_, _ = pg.Exec(ctx, "INSERT INTO "+ta+" VALUES (1,'a')")
	_, _ = pg.Exec(ctx, "INSERT INTO "+ta+" VALUES (2,'b')")
	waitFor(t, 15*time.Second, func() bool { return count() == 2 }, "2 rows")
	stop()
	stop = run()
	_, _ = pg.Exec(ctx, "INSERT INTO "+ta+" VALUES (3,'c')")
	waitFor(t, 15*time.Second, func() bool {
		var n int
		_ = indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name=? AND pk_values='3'", ta).Scan(&n)
		return n == 1
	}, "row 3")
	time.Sleep(time.Second)
	stop()
	rows, _ := indexDB.Query("SELECT pk_values, COUNT(*) FROM binlog_events WHERE table_name=? GROUP BY pk_values", ta)
	defer rows.Close()
	for rows.Next() {
		var pk string
		var n int
		_ = rows.Scan(&pk, &n)
		t.Logf("PROBE pk=%s indexed %d time(s)", pk, n)
	}
}
