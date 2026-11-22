//go:build integration

package pgstreamrun

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A capture that starts while a snapshot is still creating the slot (#3078):
// its first look at the slot's position fails (the slot has none yet), the
// snapshot's creation ends before replication starts, and replication then
// starts on that slot. Where this capture started must still be on record,
// because the checks that tell a snapshot older than capture apart compare
// with it, and a first run that records none leaves every resume with none.
func TestIntegrationPGCaptureFromSlotBeingCreated_3078(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	repl := pgDSN + "?replication=database"
	if strings.Contains(pgDSN, "?") {
		repl = pgDSN + "&replication=database"
	}
	indexDB, dbName := testutil.CreateTestDB(t)
	const slot, pub, tbl = "bintrail_pgfrom_3078", "bintrail_pgfrom_3078_pub", "pgfrom_3078_t"
	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
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
	for _, q := range []string{
		fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", tbl),
		fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl),
		fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl),
	} {
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	// A write transaction open before the slot's creation begins holds the
	// creation until it ends.
	hold, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect (open transaction): %v", err)
	}
	t.Cleanup(func() { hold.Close(context.Background()) })
	if _, err := hold.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := hold.Exec(ctx, "SELECT txid_current()"); err != nil {
		t.Fatal(err)
	}

	// The snapshot's creation of the slot, as AnchorOnSlot makes it.
	type made struct {
		point pglogrepl.LSN
		err   error
	}
	creating := make(chan made, 1)
	createCtx, cancelCreate := context.WithCancel(ctx)
	t.Cleanup(cancelCreate)
	go func() {
		conn, err := pgconn.Connect(createCtx, repl)
		if err != nil {
			creating <- made{err: err}
			return
		}
		defer conn.Close(context.Background())
		res, err := pglogrepl.CreateReplicationSlot(createCtx, conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
		if err != nil {
			creating <- made{err: err}
			return
		}
		point, err := pglogrepl.ParseLSN(res.ConsistentPoint)
		creating <- made{point: point, err: err}
	}()
	waitCaughtUp(t, "the slot listed, without a position", 20*time.Second, func() bool {
		var n int
		return pg.QueryRow(ctx, "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1 AND confirmed_flush_lsn IS NULL", slot).Scan(&n) == nil && n == 1
	})

	// Right before capture starts replication, after it looked at the slot
	// and found no position: the creation ends. The hook runs in capture's
	// goroutine, so it looks at the slot on a connection of its own (a pgx
	// connection is not shared between goroutines).
	watch, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect (watch the slot): %v", err)
	}
	t.Cleanup(func() { watch.Close(context.Background()) })
	var once sync.Once
	hooked := make(chan made, 1)
	restore := pgcapture.SetBeforeStartReplicationForTest(func() {
		once.Do(func() {
			var created made
			defer func() { hooked <- created }()
			if _, err := hold.Exec(context.Background(), "COMMIT"); err != nil {
				created.err = err
				return
			}
			select {
			case created = <-creating:
			case <-time.After(30 * time.Second):
				created.err = fmt.Errorf("the slot's creation did not end")
			}
			// The creator lets the slot go when its command ends.
			for range 100 {
				var active bool
				if watch.QueryRow(context.Background(), "SELECT active FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&active) == nil && !active {
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		})
	})
	t.Cleanup(restore)

	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		runErr <- One(runCtx, Config{
			IndexDSN: testutil.BaseDSN() + "/" + dbName, ReplDSN: repl, QueryDSN: pgDSN,
			SlotName: slot, Publication: pub, ServerID: 3078, Tables: "public." + tbl,
			BatchSize: 100, Checkpoint: 200 * time.Millisecond,
		})
	}()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-runErr; err != nil {
			t.Errorf("One: %v", err)
		}
	}
	t.Cleanup(stop)

	waitCaughtUp(t, "capture attached", 30*time.Second, func() bool {
		var active bool
		return pg.QueryRow(ctx, "SELECT active AND confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active) == nil && active
	})
	created := await3078stream(t, hooked)
	if created.err != nil {
		t.Fatalf("the snapshot's creation of the slot: %v", created.err)
	}
	if _, err := pg.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", tbl)); err != nil {
		t.Fatal(err)
	}
	waitCaughtUp(t, "the change indexed", 30*time.Second, func() bool {
		var n int
		return indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE pk_values = '1'").Scan(&n) == nil && n == 1
	})
	// A checkpoint after the change has been saved by now or shortly.
	var from sql.NullInt64
	waitCaughtUp(t, "a checkpoint saved", 30*time.Second, func() bool {
		var last sql.NullString
		return indexDB.QueryRow("SELECT pg_capture_from_lsn, binlog_file FROM stream_state WHERE id = 1").Scan(&from, &last) == nil && last.Valid
	})
	stop()
	if err := indexDB.QueryRow("SELECT pg_capture_from_lsn FROM stream_state WHERE id = 1").Scan(&from); err != nil {
		t.Fatalf("read the start on record: %v", err)
	}
	if !from.Valid || from.Int64 == 0 {
		t.Fatalf("a capture that started while the slot was being created recorded no start (pg_capture_from_lsn %v): the snapshot gap checks are off for this server", from)
	}
	if uint64(from.Int64) != uint64(created.point) {
		t.Fatalf("capture started at %#x on record, want the slot's consistent point %#x, where it read from", from.Int64, uint64(created.point))
	}
}

// await3078stream waits for what the hook saw; a hook that never ran means
// replication started without passing through it.
func await3078stream[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(30 * time.Second):
		t.Fatal("the capture never reached the start of replication")
		panic("unreachable")
	}
}
