//go:build integration

package pgstreamrun_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/pgverifysource"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// #2746: live-source verify of a PostgreSQL table on a quiet database. No
// commit follows the snapshot, so the capture's checkpoint never reaches the
// slot's consistent point; the proof is the walsender's sent position the
// capture saves. Each check must match with no "coverage unverified" note:
// once with the capture that indexed the last writes, and once after the
// capture restarted and committed nothing since.
func TestIntegrationPGVerifyQuietDatabase2746(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	hour := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, indexDB, dbName, []time.Time{hour.Add(-2 * time.Hour), hour.Add(-time.Hour), hour, hour.Add(time.Hour), hour.Add(2 * time.Hour)})

	const (
		slot = "bintrail_pgq2746"
		pub  = "bintrail_pgq2746_pub"
		tbl  = "pgq2746_t"
	)
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
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v int NOT NULL)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("INSERT INTO %s SELECT g, 0 FROM generate_series(1, 200) g", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	outDir := t.TempDir()
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: pgDSN, ReplDSN: replDSN(pgDSN), SlotName: slot, Publication: pub, OutputDir: outDir,
	}); err != nil {
		t.Fatalf("pgbaseline.Run: %v", err)
	}
	capCfg := pgstreamrun.Config{
		IndexDSN: testutil.BaseDSN() + "/" + dbName, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 92746, Tables: "public." + tbl,
		BatchSize: 200, Checkpoint: time.Second,
	}
	startCapture := func() (stop func()) {
		runCtx, cancel := context.WithCancel(ctx)
		runErr := make(chan error, 1)
		go func() { runErr <- pgstreamrun.One(runCtx, capCfg) }()
		var stopped bool
		stop = func() {
			if stopped {
				return
			}
			stopped = true
			cancel()
			select {
			case err := <-runErr:
				if err != nil {
					t.Errorf("the capture returned: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Error("the capture did not stop")
			}
		}
		t.Cleanup(stop)
		return stop
	}
	stop := startCapture()

	// A few changes, so the relation is published and a checkpoint saved;
	// then nothing more is written to this database.
	mustExec(fmt.Sprintf("UPDATE %s SET v = v + 1 WHERE id <= 20", tbl))
	mustExec(fmt.Sprintf("DELETE FROM %s WHERE id = 200", tbl))
	waitFor(t, 60*time.Second, func() bool {
		var snaps, ckpt int
		if indexDB.QueryRow("SELECT COUNT(*) FROM schema_snapshots WHERE table_name = ?", tbl).Scan(&snaps) != nil {
			return false
		}
		if indexDB.QueryRow("SELECT COUNT(*) FROM stream_state WHERE id = 1 AND binlog_position > 0").Scan(&ckpt) != nil {
			return false
		}
		return snaps > 0 && ckpt > 0
	}, "the relation published and a checkpoint saved")

	resolver, err := verify.ResolverFor(indexDB)
	if err != nil {
		t.Fatalf("ResolverFor: %v", err)
	}
	checksum, closeSource, err := pgverifysource.LiveSource(ctx, pgDSN)
	if err != nil {
		t.Fatalf("LiveSource: %v", err)
	}
	t.Cleanup(func() { _ = closeSource() })
	cfg := verify.PGLiveConfig{
		SourceChecksum: checksum, IndexDB: indexDB, Resolver: resolver,
		BaselineSource: outDir, IndexDBName: dbName, NoArchive: true,
	}
	check := func(name string) {
		t.Helper()
		start := time.Now()
		res, err := verify.VerifyTablePG(ctx, cfg, "public", tbl)
		if err != nil {
			t.Fatalf("%s: VerifyTablePG: %v", name, err)
		}
		var pos, sent int64
		if err := indexDB.QueryRow("SELECT binlog_position, COALESCE(pg_sent_through_lsn, -1) FROM stream_state WHERE id = 1").Scan(&pos, &sent); err != nil {
			t.Fatalf("%s: read stream_state: %v", name, err)
		}
		t.Logf("%s: %s in %s, anchor %s, checkpoint %d, sent through %d, detail %q",
			name, res.Status, time.Since(start).Round(time.Millisecond), res.Anchor, pos, sent, res.Detail)
		if res.Status != verify.StatusMatch {
			t.Fatalf("%s: %s (%s); a quiet table the index holds must compare equal", name, res.Status, res.Detail)
		}
		if strings.Contains(res.Detail, "coverage unverified") || strings.Contains(res.Detail, "not cut at the snapshot") {
			t.Fatalf("%s: matched without proving coverage, or without the cut: %q", name, res.Detail)
		}
		if sent <= pos {
			t.Fatalf("%s: sent through %d is not past the checkpoint %d: the proof did not come from the sent position", name, sent, pos)
		}
	}

	time.Sleep(2 * time.Second) // let the database go quiet
	check("the running capture")

	// Restart the capture and commit nothing: its checkpoint must still be
	// saved, with a sent position, for the check to be proven.
	stop()
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return !active
	}, "the capture's slot released")
	startCapture()
	check("the restarted capture")
}
