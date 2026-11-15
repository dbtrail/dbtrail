//go:build integration

package pgshim

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestPGWire_PostgresSourceRoundTrip is the PostgreSQL-source, version-dependent
// acceptance test (#1008): a live PostgreSQL change streams through
// pgstreamrun.One into the MySQL index, then a real pgx client connects over the
// pgwire front-end and a single-row _flashback AS OF returns the row with
// numeric / bytea / timestamptz values intact. It reuses the proven streaming
// harness from internal/pgstreamrun's end-to-end test.
//
// Needs BOTH a live MySQL index (BINTRAIL_TEST_DSN) and a live PostgreSQL source
// (BINTRAIL_TEST_PG_DSN); it skips on the MySQL-only jobs and runs in the
// PostgreSQL 15–18 matrix — this is the "PG CI matrix" the acceptance asks for.
func TestPGWire_PostgresSourceRoundTrip(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgwire_it"
	const pub = "bintrail_pgwire_it_pub"
	const tbl = "pgwire_it_t"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pg.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, n numeric, b bytea, ts timestamptz)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN:    indexDSN,
		ReplDSN:     withReplication(pgDSN),
		QueryDSN:    pgDSN,
		SlotName:    slot,
		Publication: pub,
		ServerID:    51,
		Tables:      "public." + tbl,
		BatchSize:   100,
		Checkpoint:  200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()

	waitForCond(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")

	// bytea via the hex literal; timestamptz mid-month so a timezone offset in the
	// walsender's pinned representation cannot shift the asserted year-month.
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 123.45, '\\xdeadbeef', '2027-03-15 12:34:56+00')", tbl))

	waitForCond(t, 15*time.Second, func() bool {
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ?", tbl).Scan(&n); err != nil {
			return false
		}
		return n >= 1
	}, "INSERT indexed into binlog_events")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("pgstreamrun.One returned error: %v", err)
	}

	// Now serve the pgwire front-end over the populated index and query it.
	addr := serveAddrWithDB(t, Config{
		IndexDB:    indexDB,
		ShimConfig: shim.Config{NoArchive: true, IndexDBName: dbName},
		Auth:       testAuth(t),
	})
	conn, err := connectPGWire(t, addr, testUser, testPass)
	if err != nil {
		t.Fatalf("connect pgwire: %v", err)
	}
	qctx, qcancel := context.WithTimeout(ctx, 10*time.Second)
	defer qcancel()

	// Anchor AS OF to the row's OWN stored event_timestamp, NOT 'now'. This began
	// as a workaround for #1025: event_timestamp is DATETIME(0) and MySQL used to
	// ROUND a row event's sub-second commit time UP (…06.885 → …07), so the stored
	// value could sit up to a second AHEAD of the true commit instant. Reconstruct
	// bounds the window at `event_timestamp <= AsOf`, so `AS OF 'now'` (time.Now(),
	// fractional) excluded the just-committed row until the wall clock crossed that
	// rounded second — a ~75% flake on a fast host, as "pgwire query returned no
	// row". #1025 fixed that at the WRITE side: the indexer now floors instead of
	// letting MySQL round (indexer.insertBatch, guarded by
	// TestInsertBatch_subSecondTimestampFloors), so stored <= true commit time and
	// that exclusion can no longer happen. The anchor stays because it is
	// deterministic irrespective of capture timing: the stored timestamp is inside
	// coverage AND >= the event by construction, and it exercises the identical
	// capture → index → pgwire → reconstruct path. (A far-future AS OF would also
	// have dodged the original flake, but trips gap-detection — an AS OF past the
	// retained history is rejected.)
	var asOf string
	if err := indexDB.QueryRow("SELECT MAX(event_timestamp) FROM binlog_events WHERE table_name = ?", tbl).Scan(&asOf); err != nil {
		t.Fatalf("read latest event_timestamp: %v", err)
	}
	rows, err := conn.Query(qctx, fmt.Sprintf("SELECT * FROM _flashback.%s AS OF '%s' WHERE id = 1", tbl, asOf))
	if err != nil {
		t.Fatalf("pgwire query: %v", err)
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatalf("pgwire rows: %v", err)
		}
		t.Fatal("pgwire query returned no row")
	}
	vals, err := rows.Values()
	if err != nil {
		t.Fatalf("pgwire values: %v", err)
	}
	got := make(map[string]string, len(fields))
	for i, f := range fields {
		got[f.Name] = fmt.Sprint(vals[i])
	}

	if got["id"] != "1" {
		t.Errorf("id = %q, want 1", got["id"])
	}
	if got["n"] != "123.45" {
		t.Errorf("numeric n = %q, want 123.45", got["n"])
	}
	if !strings.Contains(got["b"], "deadbeef") {
		t.Errorf("bytea b = %q, want to contain deadbeef", got["b"])
	}
	if !strings.Contains(got["ts"], "2027-03") {
		t.Errorf("timestamptz ts = %q, want to contain 2027-03", got["ts"])
	}
}

func withReplication(base string) string {
	if strings.Contains(base, "?") {
		return base + "&replication=database"
	}
	return base + "?replication=database"
}

func waitForCond(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
