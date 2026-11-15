//go:build integration

package pgstreamrun_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestOne_FoldWarnsForATableThatEnteredCaptureLate_2862: a real source, a
// real capture, a real snapshot and a real fold.
//
// A snapshot holds two tables. One was in the publication since before
// capture started. The other is taken out of the publication after the
// snapshot, changes while it is out, and is added back; capture had never
// seen it, so the first change after the add is the first it knows of the
// table, and it records that (#2441). The fold of the snapshot must warn for
// that table, whose change while out is in neither the snapshot nor the
// index, and must not warn for the table covered all along, whose first
// change is after the snapshot only because nothing changed in between.
//
// It runs with and without track_commit_timestamp: with it the record is an
// ALTER PUBLICATION, without it a CREATE TABLE "first seen in the stream".
func TestOne_FoldWarnsForATableThatEnteredCaptureLate_2862(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName
	const slot = "bintrail_pgsr_gap2862"
	const pub = "bintrail_pgsr_gap2862_pub"
	const covered, late = "pgsr_gap2862_covered", "pgsr_gap2862_late"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+covered)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+late)
		for range 50 {
			_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", slot)
			var n int
			if err := pg.QueryRow(bg, "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&n); err != nil || n == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	dropAll()
	t.Cleanup(dropAll)
	mustExec := func(q string) {
		t.Helper()
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	for _, n := range []string{covered, late} {
		mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", n))
		mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", n))
		mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'old')", n))
	}
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, covered))

	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		runErr <- pgstreamrun.One(runCtx, pgstreamrun.Config{
			IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
			SlotName: slot, Publication: pub, ServerID: 2862,
			Schemas: "public", BatchSize: 100, Checkpoint: 200 * time.Millisecond,
		})
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-runErr; err != nil {
			t.Errorf("One returned error: %v", err)
		}
	})
	waitFor(t, 30*time.Second, func() bool {
		var active bool
		return pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active) == nil && active
	}, "replication slot active")
	// indexed waits for a change to be in the index and behind a checkpoint:
	// a fold reads only as far as the last checkpoint.
	indexed := func(table, pk string) {
		t.Helper()
		waitFor(t, 30*time.Second, func() bool {
			var n, behind int
			err := indexDB.QueryRow(`SELECT
				(SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND pk_values = ?),
				COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
				table, pk).Scan(&n, &behind)
			return err == nil && n >= 1 && behind == 1
		}, table+" pk "+pk+" indexed and checkpointed")
	}

	// Capture is attached and has recorded where it started.
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'before the snapshot')", covered))
	indexed(covered, "2")

	// The snapshot holds both tables: the second is in the publication when
	// it is taken, and capture has seen no change of it.
	mustExec(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", pub, late))
	snapshots := t.TempDir()
	stats, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: pgDSN, ReplDSN: replDSN(pgDSN), SlotName: slot, Publication: pub,
		OutputDir: snapshots, Parallelism: 1,
	})
	if err != nil {
		t.Fatalf("the snapshot: %v", err)
	}
	if stats.TablesProcessed != 2 || stats.DeltaStartLSN == 0 {
		t.Fatalf("the snapshot holds %d tables with floor %#x, want both tables and a floor", stats.TablesProcessed, stats.DeltaStartLSN)
	}
	var from uint64
	if err := indexDB.QueryRow("SELECT COALESCE(pg_capture_from_lsn, 0) FROM stream_state WHERE id = 1").Scan(&from); err != nil || from == 0 || from > stats.DeltaStartLSN {
		t.Fatalf("capture started at %#x on record (%v), want at or before the snapshot's floor %#x: the index-wide proof this test is about", from, err, stats.DeltaStartLSN)
	}

	// Out of the publication, a change nobody captures, and back.
	mustExec(fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s", pub, late))
	mustExec(fmt.Sprintf("UPDATE %s SET v = 'lost' WHERE id = 1", late))
	mustExec(fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", pub, late))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (3, 'captured')", late))
	mustExec(fmt.Sprintf("UPDATE %s SET v = 'after the snapshot' WHERE id = 1", covered))
	indexed(late, "3")
	indexed(covered, "1")

	// The gap is real: the change made while the table was out is not in
	// the index.
	var lost int
	if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND pk_values = '1'", late).Scan(&lost); err != nil || lost != 0 {
		t.Fatalf("the change made while the table was out of the publication is in the index (%d rows, %v)", lost, err)
	}
	// And the index records the table's entry, past the snapshot's floor.
	var kind string
	var pos uint64
	if err := indexDB.QueryRow(`SELECT ddl_type, binlog_pos FROM schema_changes
		WHERE table_name = ? AND ddl_query LIKE '/* dbtrail: inferred%' ORDER BY id LIMIT 1`, late).Scan(&kind, &pos); err != nil {
		t.Fatalf("no record of %s entering capture: %v", late, err)
	}
	if (kind != "ALTER PUBLICATION" && kind != "CREATE TABLE") || pos <= stats.DeltaStartLSN {
		t.Fatalf("the entry is recorded as %q at %#x, want an add or a first sight past the floor %#x", kind, pos, stats.DeltaStartLSN)
	}
	var none int
	if err := indexDB.QueryRow(`SELECT COUNT(*) FROM schema_changes WHERE table_name = ?`, covered).Scan(&none); err != nil || none != 0 {
		t.Fatalf("the table covered all along has %d schema-change rows (%v), want none", none, err)
	}

	// Snapshot folders are named to the second.
	time.Sleep(1100 * time.Millisecond)
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	_, err = reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN: indexDSN, BaselineSrc: snapshots, Tables: []string{"public." + covered, "public." + late},
		At: time.Now().UTC(), OutputDir: snapshots, OutputFormat: reconstruct.OutputFormatParquet, Parallelism: 1,
	})
	slog.SetDefault(prev)
	if err != nil {
		t.Fatalf("the fold: %v\n%s", err, logs.String())
	}
	var warnedLate bool
	for _, line := range strings.Split(logs.String(), "\n") {
		if !strings.Contains(line, "possible gap between baseline") {
			continue
		}
		switch {
		case strings.Contains(line, "table="+late) && strings.Contains(line, "entered capture after the snapshot"):
			warnedLate = true
		default:
			t.Errorf("an unexpected gap warning:\n%s", line)
		}
	}
	if !warnedLate {
		t.Fatalf("the fold did not warn that %s entered capture after the snapshot:\n%s", late, logs.String())
	}
}
