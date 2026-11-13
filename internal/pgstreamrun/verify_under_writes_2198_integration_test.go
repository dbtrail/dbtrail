//go:build integration

package pgstreamrun_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/pgverifysource"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// #2198: live-source verify of a PostgreSQL table that is written to for the
// whole check, the PostgreSQL sibling of streamrun's #2150 test. The capture
// is the real one (pgstreamrun.One on the slot the baseline created,
// checkpointing every second), the baseline is the real one (pgbaseline), and
// six writers change the table the whole time: one with no pause, five with a
// short one. Every check must compare equal: the source is read at the
// snapshot of a temporary slot, and the reconstruction stops at that slot's
// consistent point. Cut at the time the scan ended instead, the writes
// committed while the table was read are in the reconstruction and not in the
// snapshot, a false MISMATCH.

const (
	pgUnderWritesSeed    = 2000
	pgUnderWritesWriters = 6
	pgUnderWritesChecks  = 3
)

func TestIntegrationPGVerifyLiveSourceUnderWrites2198(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	hour := time.Now().UTC().Truncate(time.Hour)
	// Two hours back: a PostgreSQL baseline's changes are read from a margin
	// before its time (reconstruct.PGDeltaMargin). Two ahead: the test can
	// cross an hour.
	testutil.SetupPartitionedTable(t, indexDB, dbName, []time.Time{hour.Add(-2 * time.Hour), hour.Add(-time.Hour), hour, hour.Add(time.Hour), hour.Add(2 * time.Hour)})

	const (
		slot = "bintrail_pgvw2198"
		pub  = "bintrail_pgvw2198_pub"
		tbl  = "pgvw2198_t"
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
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v int NOT NULL, note text NOT NULL)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("INSERT INTO %s SELECT g, 0, 'seed' FROM generate_series(1, %d) g", tbl, pgUnderWritesSeed))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	// The baseline creates the capture's slot and reads the table at its
	// snapshot; the capture then streams from that slot.
	outDir := t.TempDir()
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: pgDSN, ReplDSN: replDSN(pgDSN), SlotName: slot, Publication: pub, OutputDir: outDir,
	}); err != nil {
		t.Fatalf("pgbaseline.Run: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		runErr <- pgstreamrun.One(runCtx, pgstreamrun.Config{
			IndexDSN: testutil.BaseDSN() + "/" + dbName, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
			SlotName: slot, Publication: pub, ServerID: 92198, Tables: "public." + tbl,
			BatchSize: 200, Checkpoint: time.Second,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runErr:
			if err != nil {
				t.Errorf("the capture returned: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("the capture did not stop")
		}
	})

	var commits atomic.Int64
	stopWriters := pgWriters(t, pgDSN, tbl, &commits)
	t.Cleanup(stopWriters)

	// The capture publishes the relation with its first change, and verify
	// needs both that and a checkpoint.
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

	for i := range pgUnderWritesChecks {
		time.Sleep(300 * time.Millisecond) // writes before the read, too
		before := commits.Load()
		res, err := verify.VerifyTablePG(ctx, cfg, "public", tbl)
		during := commits.Load() - before
		if err != nil {
			t.Fatalf("check %d: VerifyTablePG: %v", i+1, err)
		}
		t.Logf("check %d: %s, source rows %d, reconstructed %d, %d writes committed during it, anchor %s, detail %q",
			i+1, res.Status, res.SourceRows, res.ReconstructRows, during, res.Anchor, res.Detail)
		if during == 0 {
			t.Fatalf("check %d: no write committed while it ran, so it proves nothing about writes", i+1)
		}
		if res.Status != verify.StatusMatch {
			t.Fatalf("check %d: %s (%s): source rows %d, reconstructed %d; a table written to during the check must compare equal",
				i+1, res.Status, res.Detail, res.SourceRows, res.ReconstructRows)
		}
		if strings.Contains(res.Detail, "not cut at the snapshot") || strings.Contains(res.Detail, "coverage unverified") {
			t.Fatalf("check %d: matched without the cut, or without proving the index reached it: %q", i+1, res.Detail)
		}
	}
}

// pgWriters runs the steady traffic on tbl: single-row updates, inserts and
// deletes, and two-statement transactions, each writer on its own connection.
// Writer 0 never rests; the others rest 10 ms between writes. commits counts
// what committed.
func pgWriters(t *testing.T, dsn, tbl string, commits *atomic.Int64) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var next atomic.Int64
	next.Store(pgUnderWritesSeed + 1000)
	conns := make([]*pgx.Conn, pgUnderWritesWriters)
	for w := range conns {
		c, err := pgx.Connect(context.Background(), dsn)
		if err != nil {
			cancel()
			t.Fatalf("writer %d: connect: %v", w, err)
		}
		conns[w] = c
	}
	for w, c := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 2198))
			for ctx.Err() == nil {
				var err error
				switch op := r.IntN(10); {
				case op < 5:
					_, err = c.Exec(ctx, "UPDATE "+tbl+" SET v = v + 1, note = 'upd' WHERE id = $1", 1+r.IntN(pgUnderWritesSeed))
				case op < 7:
					_, err = c.Exec(ctx, "INSERT INTO "+tbl+" VALUES ($1, 1, 'ins')", next.Add(1))
				case op < 8:
					_, err = c.Exec(ctx, "DELETE FROM "+tbl+" WHERE id = $1", pgUnderWritesSeed+1000+1+r.Int64N(max(1, next.Load()-pgUnderWritesSeed-1000)))
				default:
					err = pgx.BeginFunc(ctx, c, func(tx pgx.Tx) error {
						if _, err := tx.Exec(ctx, "UPDATE "+tbl+" SET v = v + 10 WHERE id = $1", 1+r.IntN(pgUnderWritesSeed)); err != nil {
							return err
						}
						_, err := tx.Exec(ctx, "UPDATE "+tbl+" SET note = 'tx' WHERE id = $1", 1+r.IntN(pgUnderWritesSeed))
						return err
					})
				}
				if w > 0 {
					time.Sleep(10 * time.Millisecond)
				}
				var pgErr *pgconn.PgError
				switch {
				case err == nil:
					commits.Add(1)
				case ctx.Err() != nil:
				case errors.As(err, &pgErr) && pgErr.Code == "40P01": // deadlock between two writers
				default:
					t.Errorf("writer %d: %v", w, err)
					return
				}
			}
		}()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			wg.Wait()
			for _, c := range conns {
				_ = c.Close(context.Background())
			}
		})
	}
}
