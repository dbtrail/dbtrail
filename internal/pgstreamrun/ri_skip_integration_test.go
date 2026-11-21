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

// TestOne_SkippedTableSkipsArePersistedAndSurviveRestart: a published table that
// capture does not take (here one whose replica identity leaves the primary key
// out; once, every table below REPLICA IDENTITY FULL) does not stop One. Its
// changes are dropped and the skip ledger rides every checkpoint into
// stream_state.capture_skips, where `status` reports capture DEGRADED and names the
// table. A restart seeds the ledger from what was persisted instead of resetting
// it, and once the table's identity holds the key its new changes are indexed.
//
// A table at the DEFAULT identity rides along (tc): it is indexed like the FULL
// one, the earlier row of its UPDATE with the key only, and `status` does not
// report capture degraded for it.
func TestOne_SkippedTableSkipsArePersistedAndSurviveRestart(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgsr_riskip"
	const pub = "bintrail_pgsr_riskip_pub"
	const ta, tb, tc = "pgsr_riskip_a", "pgsr_riskip_b", "pgsr_riskip_c"
	const reason = pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+ta+", "+tb+", "+tc)
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
	// B: REPLICA IDENTITY USING INDEX on an index without the primary key.
	mustExec("CREATE TABLE " + tb + " (id int PRIMARY KEY, u int NOT NULL, v text)")
	mustExec("CREATE UNIQUE INDEX " + tb + "_uk ON " + tb + " (u)")
	mustExec("ALTER TABLE " + tb + " REPLICA IDENTITY USING INDEX " + tb + "_uk")
	mustExec("CREATE TABLE " + tc + " (id int PRIMARY KEY, v text)") // default replica identity
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + ta + ", " + tb + ", " + tc)

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
	// Polled before the first checkpoint, when stream_state has no row yet:
	// that is "nothing persisted so far", not a failure.
	persistedSkips := func() int64 {
		st, err := status.LoadStreamState(ctx, indexDB)
		if err != nil {
			t.Fatalf("LoadStreamState: %v", err)
		}
		if st == nil {
			return 0
		}
		skips, _ := st.ParseCaptureSkips()
		if n := skips[pgcapture.SkipReplicaIdentityNotFull].Count; n != 0 {
			t.Fatalf("%d changes recorded under the old reason %q: nothing records it any more", n, pgcapture.SkipReplicaIdentityNotFull)
		}
		return skips[reason].Count
	}

	// ── run 1 ──
	stop := run()
	mustExec("INSERT INTO " + ta + " VALUES (1, 'before')")
	mustExec("INSERT INTO " + tb + " VALUES (1, 10, 'skipped')")
	mustExec("UPDATE " + tb + " SET v = 'skipped too' WHERE id = 1")
	mustExec("INSERT INTO " + tc + " VALUES (1, 'captured below FULL')")
	mustExec("UPDATE " + tc + " SET v = 'and its update' WHERE id = 1")
	mustExec("INSERT INTO " + ta + " VALUES (2, 'after')")
	waitFor(t, 20*time.Second, func() bool { return countRows(ta) == 2 && countRows(tc) == 2 && persistedSkips() == 2 },
		"A's two rows and C's two changes indexed, B's two changes persisted as skips")
	if n := countRows(tb); n != 0 {
		t.Fatalf("%d rows of the table whose identity lacks the key were indexed", n)
	}
	stop()

	// C's UPDATE as the index holds it: the key real, the rest not captured,
	// and no changed columns (unknown).
	var before, changed sql.NullString
	if err := indexDB.QueryRow("SELECT CAST(row_before AS CHAR), CAST(changed_columns AS CHAR) FROM binlog_events WHERE table_name = ? AND event_type = 2", tc).Scan(&before, &changed); err != nil {
		t.Fatal(err)
	}
	t.Logf("C's UPDATE in the index: row_before=%s changed_columns=%v", before.String, changed)
	if !strings.Contains(before.String, `"id": "1"`) || !strings.Contains(before.String, `"v": {"__bintrail_not_captured__": true}`) {
		t.Errorf("row_before = %s, want the key and the not-captured marker", before.String)
	}
	if changed.Valid && changed.String != "null" {
		t.Errorf("changed_columns = %q, want none (unknown)", changed.String)
	}

	skips, st := ledger()
	stat := skips[reason]
	if len(stat.Tables) != 1 || stat.Tables[0] != "public."+tb {
		t.Errorf("ledger tables %v, want [public.%s]: C, at DEFAULT, is captured and must not be there", stat.Tables, tb)
	}
	if stat.LastFile == "" || stat.LastPos == 0 || !strings.Contains(stat.LastDetail, "REPLICA IDENTITY DEFAULT") {
		t.Errorf("skip not attributed: %+v", stat)
	}
	if len(skips) != 1 {
		t.Errorf("ledger holds other reasons: %+v", skips)
	}
	// What `status` prints from this ledger, generated from the real row.
	var out bytes.Buffer
	status.WriteStatus(&out, nil, nil, nil, nil, nil, st)
	t.Logf("status output with the persisted ledger:\n%s", out.String())
	for _, want := range []string{"DEGRADED", reason, "public." + tb} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("status output does not show %q", want)
		}
	}
	if strings.Contains(out.String(), tc) {
		t.Errorf("status names the table at DEFAULT, which is captured")
	}

	// ── run 2: a restart keeps the ledger ──
	var saved sql.NullString
	if err := indexDB.QueryRow("SELECT last_checkpoint FROM stream_state WHERE id = 1").Scan(&saved); err != nil {
		t.Fatal(err)
	}
	stop = run()
	mustExec("INSERT INTO " + ta + " VALUES (3, 'resumed')")
	waitFor(t, 20*time.Second, func() bool {
		var n int
		_ = indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND pk_values = '3'", ta).Scan(&n)
		return n >= 1
	}, "A's row after the restart indexed")
	t.Logf("A rows in the index after the restart: %d", countRows(ta))
	waitFor(t, 10*time.Second, func() bool {
		var now sql.NullString
		_ = indexDB.QueryRow("SELECT last_checkpoint FROM stream_state WHERE id = 1").Scan(&now)
		return now.String != saved.String
	}, "a checkpoint saved by the restarted run")
	if n := persistedSkips(); n < 2 {
		t.Fatalf("the restart reset the persisted ledger: %d skips, want at least 2", n)
	}

	// ── fix B while capture runs: its new changes are indexed ──
	mustExec("ALTER TABLE " + tb + " REPLICA IDENTITY DEFAULT")
	mustExec("INSERT INTO " + tb + " VALUES (2, 20, 'captured')")
	waitFor(t, 20*time.Second, func() bool { return countRows(tb) == 1 }, "B's change after the fix indexed")
	if n := persistedSkips(); n < 2 {
		t.Errorf("ledger went down after the fix: %d", n)
	}
	stop()
}
