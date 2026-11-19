//go:build integration

package pgstreamrun_test

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestOne_NonFullTableSkipsArePersistedAndSurviveRestart: a published table that
// is not at REPLICA IDENTITY FULL no longer stops One. Its changes are dropped and
// the skip ledger rides every checkpoint into stream_state.capture_skips, where
// `status` reports capture DEGRADED and names the table. A restart seeds the ledger
// from what was persisted instead of resetting it, and once the table is altered to
// FULL its new changes are indexed.
func TestOne_NonFullTableSkipsArePersistedAndSurviveRestart(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgsr_riskip"
	const pub = "bintrail_pgsr_riskip_pub"
	const ta, tb = "pgsr_riskip_a", "pgsr_riskip_b"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+ta+", "+tb)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)
	mustExec := func(q string) {
		t.Helper()
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec("CREATE TABLE " + ta + " (id int PRIMARY KEY, v text)")
	mustExec("ALTER TABLE " + ta + " REPLICA IDENTITY FULL")
	mustExec("CREATE TABLE " + tb + " (id int PRIMARY KEY, v text)") // default replica identity
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + ta + ", " + tb)

	cfg := pgstreamrun.Config{
		IndexDSN:    indexDSN,
		ReplDSN:     replDSN(pgDSN),
		QueryDSN:    pgDSN,
		SlotName:    slot,
		Publication: pub,
		ServerID:    43,
		BatchSize:   100,
		Checkpoint:  200 * time.Millisecond,
	}
	run := func() (stop func()) {
		runCtx, cancel := context.WithCancel(ctx)
		runErr := make(chan error, 1)
		go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()
		waitFor(t, 15*time.Second, func() bool {
			select {
			case err := <-runErr:
				t.Fatalf("One returned before replication started: %v", err)
			default:
			}
			var active bool
			if err := pg.QueryRow(ctx, "SELECT active AND confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
				return false
			}
			return active
		}, "replication slot active")
		return func() {
			t.Helper()
			select {
			case err := <-runErr:
				t.Fatalf("One stopped on its own: %v", err)
			default:
			}
			cancel()
			if err := <-runErr; err != nil {
				t.Fatalf("One returned error: %v", err)
			}
		}
	}
	countRows := func(table string) int {
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ?", table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	ledger := func() (map[string]status.CaptureSkipStat, *status.StreamStateInfo) {
		st, err := status.LoadStreamState(ctx, indexDB)
		if err != nil || st == nil {
			t.Fatalf("LoadStreamState: %+v, %v", st, err)
		}
		skips, ok := st.ParseCaptureSkips()
		if !ok {
			return nil, st
		}
		return skips, st
	}
	persistedSkips := func() int64 {
		skips, _ := ledger()
		return skips[pgcapture.SkipReplicaIdentityNotFull].Count
	}

	// ── run 1 ──
	stop := run()
	mustExec("INSERT INTO " + ta + " VALUES (1, 'before')")
	mustExec("INSERT INTO " + tb + " VALUES (1, 'skipped')")
	mustExec("UPDATE " + tb + " SET v = 'skipped too' WHERE id = 1")
	mustExec("INSERT INTO " + ta + " VALUES (2, 'after')")
	waitFor(t, 20*time.Second, func() bool { return countRows(ta) == 2 && persistedSkips() == 2 },
		"A's two rows indexed and B's two changes persisted as skips")
	if n := countRows(tb); n != 0 {
		t.Fatalf("%d rows of the non-FULL table were indexed", n)
	}
	stop()

	skips, st := ledger()
	stat := skips[pgcapture.SkipReplicaIdentityNotFull]
	if len(stat.Tables) != 1 || stat.Tables[0] != "public."+tb {
		t.Errorf("ledger tables %v, want [public.%s]", stat.Tables, tb)
	}
	if stat.LastFile == "" || stat.LastPos == 0 || !strings.Contains(stat.LastDetail, "REPLICA IDENTITY FULL") {
		t.Errorf("skip not attributed: %+v", stat)
	}
	// What `status` prints from this ledger, generated from the real row.
	var out bytes.Buffer
	status.WriteStatus(&out, nil, nil, nil, nil, nil, st)
	t.Logf("status output with the persisted ledger:\n%s", out.String())
	for _, want := range []string{"DEGRADED", pgcapture.SkipReplicaIdentityNotFull, "public." + tb} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("status output does not show %q", want)
		}
	}

	// ── run 2: a restart keeps the ledger ──
	var before sql.NullString
	if err := indexDB.QueryRow("SELECT last_checkpoint FROM stream_state WHERE id = 1").Scan(&before); err != nil {
		t.Fatal(err)
	}
	stop = run()
	mustExec("INSERT INTO " + ta + " VALUES (3, 'resumed')")
	waitFor(t, 20*time.Second, func() bool { return countRows(ta) == 3 }, "A's row after the restart indexed")
	waitFor(t, 10*time.Second, func() bool {
		var now sql.NullString
		_ = indexDB.QueryRow("SELECT last_checkpoint FROM stream_state WHERE id = 1").Scan(&now)
		return now.String != before.String
	}, "a checkpoint saved by the restarted run")
	if n := persistedSkips(); n < 2 {
		t.Fatalf("the restart reset the persisted ledger: %d skips, want at least 2", n)
	}

	// ── fix B while capture runs: its new changes are indexed ──
	mustExec("ALTER TABLE " + tb + " REPLICA IDENTITY FULL")
	mustExec("INSERT INTO " + tb + " VALUES (2, 'captured')")
	waitFor(t, 20*time.Second, func() bool { return countRows(tb) == 1 }, "B's change after the fix indexed")
	if n := persistedSkips(); n < 2 {
		t.Errorf("ledger went down after the fix: %d", n)
	}
	stop()
}
