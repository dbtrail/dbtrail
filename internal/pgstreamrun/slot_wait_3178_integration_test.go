//go:build integration

package pgstreamrun_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// While the slot is being created the whole stream, checkpoint ticker included,
// must not look like a capture that is up: a supervisor reads OnCheckpoint as
// that, and would show the server as running and lose the phase.
func TestOne_NoCheckpointSignalWhileCreatingSlot_3178(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	_, dbName := testutil.CreateTestDB(t)

	const (
		slot = "bintrail_pgsr_3178"
		pub  = "bintrail_pgsr_3178_pub"
		tbl  = "pgsr_3178_t"
	)
	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		for range 100 {
			_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", slot)
			var n int
			if err := pg.QueryRow(bg, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&n); err != nil || n == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
	}
	dropAll()
	t.Cleanup(dropAll)
	for _, q := range []string{
		"CREATE TABLE " + tbl + " (id int PRIMARY KEY)",
		"ALTER TABLE " + tbl + " REPLICA IDENTITY FULL",
		"CREATE PUBLICATION " + pub + " FOR TABLE " + tbl,
	} {
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	hold, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hold.Close(context.Background()) })
	for _, q := range []string{"BEGIN", "INSERT INTO " + tbl + " VALUES (1)"} {
		if _, err := hold.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	var ticks atomic.Int32
	var mu sync.Mutex
	var phases []string
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN: testutil.BaseDSN() + "/" + dbName, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 3178, Tables: "public." + tbl,
		BatchSize: 100, Checkpoint: 100 * time.Millisecond,
		Hooks: &pgstreamrun.Hooks{
			OnCheckpoint: func() { ticks.Add(1) },
			OnPhase:      func(p string) { mu.Lock(); phases = append(phases, p); mu.Unlock() },
		},
	}
	done := make(chan error, 1)
	go func() { done <- pgstreamrun.One(runCtx, cfg) }()

	waitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(phases) > 0
	}, "the creating-slot phase")
	time.Sleep(time.Second) // ten checkpoint intervals
	if n := ticks.Load(); n != 0 {
		t.Fatalf("%d liveness ticks while the slot was still being created", n)
	}
	mu.Lock()
	got := strings.Join(phases, ",")
	mu.Unlock()
	if got != pgcapture.PhaseCreatingSlot {
		t.Fatalf("phases while waiting: %q", got)
	}
	if _, err := hold.Exec(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, func() bool { return ticks.Load() > 0 }, "a liveness tick once replication started")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("One: %v", err)
	}
}
