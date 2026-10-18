//go:build integration

package pgstreamrun_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestOne_DatesInferredDDLExactly_2441 drives the real stream with
// track_commit_timestamp on and checks every inferred row's instant against
// the commit time the server itself reports for the DDL: exact where the
// catalog can prove it, "at or before" where a later statement rewrote the
// row it would have read, never the later statement's time.
//
// Needs a source started with track_commit_timestamp=on (CI sets it); it
// fails, not skips, without it, so the exact path cannot go untested.
func TestOne_DatesInferredDDLExactly_2441(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName
	const slot = "bintrail_pgsr_ddlinstant"
	const pub = "bintrail_pgsr_ddlinstant_pub"
	const tbl, renamed = "pgsr_ddlinstant_t", "pgsr_ddlinstant_r"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	var setting string
	if err := pg.QueryRow(ctx, "SELECT current_setting('track_commit_timestamp')").Scan(&setting); err != nil || setting != "on" {
		t.Fatalf("the source must run with track_commit_timestamp=on for this test (got %q, %v)", setting, err)
	}
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+renamed)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)
	mustExec := func(sql string) {
		t.Helper()
		if _, err := pg.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	// committed is the server's commit time of the catalog row q selects,
	// read right after the DDL that wrote it: the truth to compare with.
	committed := func(q string) time.Time {
		t.Helper()
		var at time.Time
		if err := pg.QueryRow(ctx, q).Scan(&at); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return at.UTC()
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, a text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	cfg := pgstreamrun.Config{
		IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 45,
		Schemas: "public", BatchSize: 100, Checkpoint: 200 * time.Millisecond,
	}
	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()
	t.Cleanup(func() {
		cancel()
		if err := <-runErr; err != nil {
			t.Errorf("One returned error: %v", err)
		}
	})
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		_ = pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active)
		return active
	}, "replication slot active")
	indexed := func(table, pk string) {
		t.Helper()
		waitFor(t, 15*time.Second, func() bool {
			var n int
			_ = indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND pk_values = ?", table, pk).Scan(&n)
			return n >= 1
		}, table+" pk "+pk+" indexed")
	}
	// row is the newest inferred row of kind for table.
	row := func(kind, table string) (string, time.Time) {
		t.Helper()
		var q string
		var at time.Time
		if err := indexDB.QueryRow(`SELECT ddl_query, detected_at FROM schema_changes
			WHERE ddl_query LIKE '/* dbtrail: inferred%' AND ddl_type = ? AND table_name = ? ORDER BY id DESC LIMIT 1`, kind, table).Scan(&q, &at); err != nil {
			t.Fatalf("%s %s: %v", kind, table, err)
		}
		return q, at
	}
	wantExact := func(step, q string, detected, truth time.Time) {
		t.Helper()
		if !strings.Contains(q, "instant=exact; at="+truth.Format(time.RFC3339Nano)+";") {
			t.Fatalf("%s: want exact at %s: %s", step, truth.Format(time.RFC3339Nano), q)
		}
		// detected_at is DATETIME: the second of the truth (MySQL may round).
		if d := detected.Sub(truth); d <= -time.Second || d >= time.Second {
			t.Errorf("%s: detected_at %v, want the second of %v", step, detected, truth)
		}
	}

	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'x')", tbl))
	indexed(tbl, "1")

	// An added column, nothing after it: exact.
	mustExec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN b text", tbl))
	addB := committed(fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM pg_attribute WHERE attrelid = '%s'::regclass AND attname = 'b'", tbl))
	time.Sleep(1100 * time.Millisecond)
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'y', 'z')", tbl))
	indexed(tbl, "2")
	q, at := row("ALTER TABLE", tbl)
	wantExact("an added column", q, at, addB)

	// An added column touched again before any write: the column's row now
	// holds the second statement's time, so the row is at or before.
	mustExec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN c int", tbl))
	time.Sleep(1100 * time.Millisecond)
	mustExec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN c SET STATISTICS 200", tbl))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (3, 'y', 'z', 3)", tbl))
	indexed(tbl, "3")
	q, _ = row("ALTER TABLE", tbl)
	if !strings.Contains(q, "added c (int4)") || !strings.Contains(q, "instant=at-or-before") {
		t.Fatalf("an added column touched again: %s", q)
	}

	// A rename, then a GRANT: exact at the rename.
	mustExec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tbl, renamed))
	ren := committed(fmt.Sprintf("SELECT pg_xact_commit_timestamp(t.xmin) FROM pg_class c JOIN pg_type t ON t.oid = c.reltype WHERE c.oid = '%s'::regclass", renamed))
	time.Sleep(1100 * time.Millisecond)
	mustExec(fmt.Sprintf("GRANT SELECT ON %s TO PUBLIC", renamed))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (4, 'r', 'r', 4)", renamed))
	indexed(renamed, "4")
	q, at = row("RENAME TABLE", renamed)
	wantExact("a rename, then a GRANT", q, at, ren)

	// A dropped column: its row is never written again.
	mustExec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN c", renamed))
	drop := committed(fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM pg_attribute WHERE attrelid = '%s'::regclass AND attisdropped ORDER BY attnum DESC LIMIT 1", renamed))
	time.Sleep(1100 * time.Millisecond)
	mustExec(fmt.Sprintf("GRANT INSERT ON %s TO PUBLIC", renamed))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (5, 'd', 'd')", renamed))
	indexed(renamed, "5")
	q, at = row("ALTER TABLE", renamed)
	wantExact("a dropped column", q, at, drop)
}
