//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgCSTable = "pgcs_orders"
	pgCSOther = "pgcs_unpublished"
	pgCSSlot  = "bintrail_pgcs_slot"
	pgCSPub   = "bintrail_pgcs_pub"
)

// The capture status of a PostgreSQL server against a real source and a real
// capture (#2758). The rules are tested as a table elsewhere; what only a
// real PostgreSQL shows is whether the two numbers they compare ever meet:
// the capture's saved sent-through position and the source's flushed WAL
// position, after a change the capture indexes AND after WAL it has nothing
// to index (another table, outside the publication), which moves the
// source's position all the same.
func TestIntegrationPGCaptureStatus(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	baseDSN, source, exec1 := pgTestSource(t, ctx, pgCSTable, pgCSSlot, pgCSPub, "id int PRIMARY KEY, status text")
	t.Cleanup(func() { _, _ = source.Exec(context.Background(), "DROP TABLE IF EXISTS "+pgCSOther).ReadAll() })
	exec1("DROP TABLE IF EXISTS " + pgCSOther)
	exec1("CREATE TABLE " + pgCSOther + " (id int PRIMARY KEY, note text)")

	const checkpoint = 500 * time.Millisecond
	mon, bootDSN := pgTestMonitor(t, ctx, checkpoint)
	e := console.ServerEntry{
		ID: fmt.Sprintf("pgcs%d", time.Now().UnixNano()%1e9), Name: "pg-capture-status",
		Flavor: console.FlavorPostgres, SourceDSN: baseDSN, SourceSlot: pgCSSlot, SourcePublication: pgCSPub, Schemas: "public",
	}
	var err error
	if e.DSN, err = mon.DeriveIndexDSN(e.ID); err != nil {
		t.Fatal(err)
	}
	pgTestStart(t, ctx, mon, bootDSN, source, e)

	// settle asks the way the page does, held to its earlier read, until the
	// answer is want; it returns how long that took and fails when the
	// forbidden state is ever said.
	var held *pgCaptureSample
	settle := func(what, want, never string, limit time.Duration) time.Duration {
		t.Helper()
		start := time.Now()
		var last console.CaptureStatus
		pgTestWait(t, what, limit, func() (bool, string) {
			r := pgCaptureFromDBs(ctx, e.DSN, e.SourceDSN)
			if r.cause != "" {
				t.Fatalf("%s: a read failed: %s (%s)", what, r.detail, r.cause)
			}
			last, held = pgCaptureStatusFrom(r, held, time.Now())
			if never != "" && last.State == never {
				t.Fatalf("%s: answered %+v (sent %d, source %d)", what, last, r.sent, r.source)
			}
			return last.State == want, fmt.Sprintf("%+v (sent %d, source %d)", last, r.sent, r.source)
		})
		return time.Since(start)
	}

	// Before any change is captured nothing is saved, so nothing is compared.
	if r := pgCaptureFromDBs(ctx, e.DSN, e.SourceDSN); r.detail != captureDetailNoCapture && r.detail != captureDetailPGNoPosition {
		t.Fatalf("before the first captured change: %+v, want no capture on record or no position yet", r)
	}

	// A captured change.
	exec1("INSERT INTO " + pgCSTable + " VALUES (1, 'new')")
	took := settle("up to date after a captured change", console.CaptureStateUpToDate, console.CaptureStateBehind, 60*time.Second)
	t.Logf("up to date %s after a captured change (checkpoint every %s)", took.Round(10*time.Millisecond), checkpoint)

	// WAL the capture has nothing to index: the source's position moves, and
	// the capture's must follow it without a single row to deliver.
	exec1("INSERT INTO " + pgCSOther + " SELECT g, repeat('x', 200) FROM generate_series(1, 2000) g")
	held = nil
	took = settle("up to date after changes outside the publication", console.CaptureStateUpToDate, console.CaptureStateBehind, 90*time.Second)
	t.Logf("up to date %s after changes outside the publication", took.Round(10*time.Millisecond))

	// The same through the reporter the page asks: a real answer, no longer
	// "not compared yet".
	c := newCaptureStatusReporter("")
	// The reporter bounds its two connections at three seconds, which a
	// loaded machine under the race detector can miss; that answer expires
	// like any other, so it is asked until it is one of the two.
	pgTestWait(t, "an answer from the reporter", 2*time.Minute, func() (bool, string) {
		got := c.CaptureStatus(ctx, e)
		if got.ServerID != e.ID {
			t.Fatalf("the reporter answered for %q: %+v", got.ServerID, got)
		}
		if got.Detail == captureDetailTimedOut {
			time.Sleep(time.Second)
			return false, got.Detail
		}
		if got.State != console.CaptureStateUpToDate && got.RetryInSeconds == 0 {
			t.Fatalf("the reporter answered %+v, want up to date or a read that asks again", got)
		}
		return true, ""
	})

	// The snapshot schedule's probe (#2844), on the same server: with no
	// loss on record it confirms caught up once the two positions meet, and
	// until then it says the source is ahead. Never behind, which would
	// read the source in full.
	anchor := time.Now().Add(-time.Hour)
	pgTestWait(t, "the schedule's probe confirms caught up", 60*time.Second, func() (bool, string) {
		r := probeCapturePGFromDBs(ctx, e.DSN, e.SourceDSN, anchor, time.Time{})
		switch {
		case r.verdict == console.CaptureCaughtUp:
			return true, ""
		case r.verdict != "" || (r.detail != captureDetailPGAhead && r.detail != captureDetailSourceSlow && r.detail != captureDetailIndexSlow):
			t.Fatalf("the schedule's probe answered %+v", r)
		}
		return false, r.detail
	})

	// Capture stops and the source keeps changing: never up to date again.
	if err := mon.Stop(ctx, e.ID); err != nil {
		t.Fatalf("stop capture: %v", err)
	}
	pgTestWait(t, "capture detached from its slot", 30*time.Second, func() (bool, string) {
		_, rows, err := pgText(t, source, "SELECT active::text FROM pg_replication_slots WHERE slot_name = '"+pgCSSlot+"'")
		if err != nil || len(rows) != 1 {
			return false, "no slot"
		}
		return rows[0][0] == "false", "active=" + rows[0][0]
	})
	exec1("INSERT INTO " + pgCSTable + " VALUES (2, 'missed')")
	held = nil
	for i := range 6 {
		r := pgCaptureFromDBs(ctx, e.DSN, e.SourceDSN)
		var a console.CaptureStatus
		a, held = pgCaptureStatusFrom(r, held, time.Now())
		if a.State == console.CaptureStateUpToDate {
			t.Fatalf("read %d with capture stopped and a change it never read: %+v (sent %d, source %d)", i, a, r.sent, r.source)
		}
		time.Sleep(300 * time.Millisecond)
	}
	// And the schedule's probe: the source is ahead, which is no reason to
	// read it in full.
	if r := probeCapturePGFromDBs(ctx, e.DSN, e.SourceDSN, anchor, time.Time{}); r.verdict != "" ||
		captureClassOf(r.detail) != captureCannotAsk && captureClassOf(r.detail) != captureIndexUnread {
		t.Fatalf("the schedule's probe with capture stopped answered %+v", r)
	}
	// A loss on record since the snapshot is said as the reason, from the
	// index alone, and is not a full read of this probe's choosing either.
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if _, err := idx.ExecContext(ctx, "UPDATE stream_state SET gap_lost_at = UTC_TIMESTAMP(), gap_lost_detail = 'test: slot lost' WHERE id = 1"); err != nil {
		t.Fatalf("record a loss: %v", err)
	}
	pgTestWait(t, "the schedule's probe reads the loss", 30*time.Second, func() (bool, string) {
		r := probeCapturePGFromDBs(ctx, e.DSN, e.SourceDSN, anchor, time.Time{})
		return r.verdict == "" && r.detail == captureDetailPGLost && captureClassOf(r.detail) == captureCannotAsk, r.detail
	})
	// The same loss, from before the snapshot, is covered by the snapshot.
	if r := probeCapturePGFromDBs(ctx, e.DSN, e.SourceDSN, time.Now().Add(time.Hour), time.Time{}); r.detail == captureDetailPGLost {
		t.Fatalf("a loss before the snapshot was held against it: %+v", r)
	}
}
