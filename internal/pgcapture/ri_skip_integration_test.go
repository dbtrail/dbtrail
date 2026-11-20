//go:build integration

package pgcapture_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestCapturer_TableAddedNotFull_SkippedThenResumes is the bug this reason exists
// for, step by step on a live PostgreSQL. Capture runs on table A (FULL); table B is
// created at the default replica identity and added to the publication; both are
// written. Before: Run returned "relation public.b is not at REPLICA IDENTITY FULL"
// and stopped for every table, and after ALTER TABLE b REPLICA IDENTITY FULL a
// restart on the same slot failed the same way forever, because PostgreSQL decodes
// the old WAL with the replica identity B had when it was written. Now: Run keeps
// running, A's rows before and after B's change arrive, B's change is skipped and
// recorded; and a restart on the same slot, unacknowledged so PostgreSQL re-sends
// that old WAL, skips B's old change again, then captures B's changes made after
// the ALTER.
func TestCapturer_TableAddedNotFull_SkippedThenResumes(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_pgcap_riadd"
	const pub = "bintrail_pgcap_riadd_pub"
	const ta, tb = "pgcap_riadd_a", "pgcap_riadd_b"

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+ta+", "+tb)
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec("CREATE TABLE " + ta + " (id int PRIMARY KEY, v text)")
	mustExec("ALTER TABLE " + ta + " REPLICA IDENTITY FULL")
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + ta)

	skips := parser.NewSkipCounters(nil)
	start := func(resume bool) (chan event.Event, chan error, context.CancelFunc) {
		runCtx, cancel := context.WithCancel(ctx)
		cap := pgcapture.New(pgcapture.Config{
			ReplDSN:            replDSN(baseDSN),
			QueryDSN:           baseDSN,
			SlotName:           slot,
			Publication:        pub,
			ExpectExistingSlot: resume,
			Skips:              skips,
			StandbyInterval:    200 * time.Millisecond,
		})
		events := make(chan event.Event, 256)
		runErr := make(chan error, 1)
		go func() { runErr <- cap.Run(runCtx, events) }()
		waitSlotActive(t, setup, slot, runErr)
		return events, runErr, cancel
	}

	// ── run 1: B added below FULL while capture runs ──
	events, runErr, cancel := start(false)
	mustExec("CREATE TABLE " + tb + " (id int PRIMARY KEY, note text)") // default replica identity
	mustExec("ALTER PUBLICATION " + pub + " ADD TABLE " + tb)
	mustExec("INSERT INTO " + ta + " VALUES (1, 'before')")
	mustExec("INSERT INTO " + tb + " VALUES (1, 'not full')")
	mustExec("INSERT INTO " + ta + " VALUES (2, 'after')")

	got := collectUntil(t, events, runErr, 20*time.Second, func(seen map[string]bool) bool {
		return seen[key(ta, event.EventInsert, "1")] && seen[key(ta, event.EventInsert, "2")]
	})
	if got[key(tb, event.EventInsert, "1")] {
		t.Fatal("B's change made below FULL was emitted")
	}
	assertRunning(t, runErr)
	if n := skips.Count(pgcapture.SkipReplicaIdentityNotFull); n != 1 {
		t.Fatalf("want 1 recorded skip for B's change, got %d", n)
	}
	assertLedgerNames(t, skips, "public."+tb)

	// Stop WITHOUT acknowledging anything: the slot keeps the old WAL, so the
	// restart below receives B's below-FULL change again.
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned non-nil on graceful cancel: %v", err)
	}

	// ── fix B, write, restart on the same slot ──
	mustExec("ALTER TABLE " + tb + " REPLICA IDENTITY FULL")
	mustExec("INSERT INTO " + tb + " VALUES (2, 'full')")
	mustExec("UPDATE " + tb + " SET note = 'full, updated' WHERE id = 2")
	mustExec("INSERT INTO " + ta + " VALUES (3, 'after the fix')")

	events, runErr, cancel = start(true)
	defer cancel()
	var upd event.Event
	got = collectUntil(t, events, runErr, 20*time.Second, func(seen map[string]bool) bool {
		return seen[key(tb, event.EventInsert, "2")] && seen[key(tb, event.EventUpdate, "2")] && seen[key(ta, event.EventInsert, "3")]
	}, func(ev event.Event) {
		if ev.Table == tb && ev.EventType == event.EventUpdate {
			upd = ev
		}
	})
	if got[key(tb, event.EventInsert, "1")] {
		t.Fatal("B's old change, re-sent from the WAL written below FULL, was emitted")
	}
	if upd.RowBefore["note"] != "full" || upd.RowAfter["note"] != "full, updated" {
		t.Errorf("B's UPDATE after the fix must carry full images: before=%v after=%v", upd.RowBefore, upd.RowAfter)
	}
	assertRunning(t, runErr)
	if n := skips.Count(pgcapture.SkipReplicaIdentityNotFull); n != 2 {
		t.Errorf("the re-sent below-FULL change is skipped again: want 2 skips in total, got %d", n)
	}
	cancel()
	if err := <-runErr; err != nil {
		t.Errorf("Run returned non-nil on graceful cancel: %v", err)
	}
}

// ── helpers ──

func key(table string, typ event.EventType, pk string) string {
	return fmt.Sprintf("%s:%d:%s", table, typ, pk)
}

func isRow(ev event.Event) bool {
	switch ev.EventType {
	case event.EventInsert, event.EventUpdate, event.EventDelete:
		return true
	}
	return false
}

// waitSlotActive waits until Run has replication running on slot, failing at
// once if Run returns first.
func waitSlotActive(t *testing.T, conn *pgx.Conn, slot string, runErr chan error) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-runErr:
			t.Fatalf("Run returned before replication started: %v", err)
		default:
		}
		var active bool
		if err := conn.QueryRow(context.Background(),
			"SELECT active AND confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err == nil && active {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for slot %s to become active", slot)
}

// assertRunning fails if Run has returned.
func assertRunning(t *testing.T, runErr chan error) {
	t.Helper()
	select {
	case err := <-runErr:
		t.Fatalf("Run stopped: %v", err)
	default:
	}
}

// drain returns whatever is queued on ch without waiting.
func drain(ch chan event.Event) []event.Event {
	var out []event.Event
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

// collectUntil reads row events until done reports true over the keys seen,
// failing on timeout or if Run returns. each, when given, sees every row event.
func collectUntil(t *testing.T, ch chan event.Event, runErr chan error, timeout time.Duration, done func(map[string]bool) bool, each ...func(event.Event)) map[string]bool {
	t.Helper()
	seen := map[string]bool{}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for !done(seen) {
		select {
		case ev := <-ch:
			if !isRow(ev) {
				continue
			}
			seen[key(ev.Table, ev.EventType, ev.PKValues)] = true
			for _, f := range each {
				f(ev)
			}
		case err := <-runErr:
			t.Fatalf("Run stopped while rows were expected: %v (seen %v)", err, seen)
		case <-deadline.C:
			t.Fatalf("timed out; rows seen: %v", seen)
		}
	}
	// What is already queued too, so a row that must not arrive is checked.
	for _, ev := range drain(ch) {
		if isRow(ev) {
			seen[key(ev.Table, ev.EventType, ev.PKValues)] = true
		}
	}
	return seen
}

// assertLedgerNames checks the ledger's skip entry names exactly these tables.
func assertLedgerNames(t *testing.T, c *parser.SkipCounters, tables ...string) {
	t.Helper()
	raw, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]parser.SkipStat{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	st := m[pgcapture.SkipReplicaIdentityNotFull]
	if !slices.Equal(st.Tables, tables) {
		t.Errorf("ledger names %v, want %v (ledger %s)", st.Tables, tables, raw)
	}
	if st.LastFile == "" || st.LastPos == 0 {
		t.Errorf("the skip is not attributed to an LSN: %s", raw)
	}
}
