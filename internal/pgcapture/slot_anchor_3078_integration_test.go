//go:build integration

package pgcapture_test

import (
	"context"
	"errors"
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

// anchor3078 is a source for the tests of AnchorOnSlot: a slot name of its
// own, a connection to look at the catalog with, and the means to keep a
// slot's creation from ending (PostgreSQL ends it only once every transaction
// that was open when it began has ended).
type anchor3078 struct {
	baseDSN, slot string
	conn          *pgx.Conn
	hold          *pgx.Conn
}

func newAnchor3078(t *testing.T, name string) *anchor3078 {
	t.Helper()
	baseDSN := testutil.SkipIfNoPostgres(t)
	a := &anchor3078{baseDSN: baseDSN, slot: "bintrail_pgcap_3078_" + name}
	var err error
	if a.conn, err = pgx.Connect(context.Background(), baseDSN); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { a.conn.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		for range 50 {
			_, _ = a.conn.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", a.slot)
			if !a.listed(t) {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	drop()
	t.Cleanup(drop)
	return a
}

func (a *anchor3078) replConnect(ctx context.Context) (*pgconn.PgConn, error) {
	return pgconn.Connect(ctx, replDSN(a.baseDSN))
}

func (a *anchor3078) listed(t *testing.T) bool {
	t.Helper()
	var n int
	if err := a.conn.QueryRow(context.Background(), "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", a.slot).Scan(&n); err != nil {
		t.Fatalf("look for the slot: %v", err)
	}
	return n > 0
}

// holdOpen opens a transaction with an id of its own and keeps it open.
func (a *anchor3078) holdOpen(t *testing.T) {
	t.Helper()
	var err error
	if a.hold, err = pgx.Connect(context.Background(), a.baseDSN); err != nil {
		t.Fatalf("connect (open transaction): %v", err)
	}
	t.Cleanup(func() { a.hold.Close(context.Background()) })
	if _, err := a.hold.Exec(context.Background(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	var xid int64
	if err := a.hold.QueryRow(context.Background(), "SELECT txid_current()").Scan(&xid); err != nil {
		t.Fatalf("give the open transaction an id: %v", err)
	}
}

func (a *anchor3078) release(t *testing.T) {
	t.Helper()
	if _, err := a.hold.Exec(context.Background(), "COMMIT"); err != nil {
		t.Fatalf("end the open transaction: %v", err)
	}
}

type created3078 struct {
	point pglogrepl.LSN
	err   error
}

// startCreating begins the slot's creation the way capture does and returns
// once PostgreSQL lists the slot, without a position. cancel gives the
// creation up; the result arrives on the channel.
func (a *anchor3078) startCreating(t *testing.T) (<-chan created3078, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	out := make(chan created3078, 1)
	go func() {
		repl, err := a.replConnect(ctx)
		if err != nil {
			out <- created3078{err: err}
			return
		}
		defer repl.Close(context.Background())
		res, err := pglogrepl.CreateReplicationSlot(ctx, repl, a.slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
		if err != nil {
			out <- created3078{err: err}
			return
		}
		point, err := pglogrepl.ParseLSN(res.ConsistentPoint)
		out <- created3078{point: point, err: err}
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var hasPosition bool
		err := a.conn.QueryRow(context.Background(), "SELECT confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name = $1", a.slot).Scan(&hasPosition)
		if err == nil {
			if hasPosition {
				t.Fatal("the slot has a position while a transaction open before its creation is still open: the test's premise does not hold on this server")
			}
			return out, cancel
		}
		if time.Now().After(deadline) {
			t.Fatalf("the slot being created was never listed: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (a *anchor3078) confirmed(t *testing.T) pglogrepl.LSN {
	t.Helper()
	var text string
	if err := a.conn.QueryRow(context.Background(), "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", a.slot).Scan(&text); err != nil {
		t.Fatalf("read confirmed_flush_lsn: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(text)
	if err != nil {
		t.Fatal(err)
	}
	return lsn
}

type anchored3078 struct {
	anchor pgcapture.SlotAnchor
	err    error
}

// anchorAsync runs AnchorOnSlot on a connection of its own.
func (a *anchor3078) anchorAsync(t *testing.T, withRepl bool, wait time.Duration) <-chan anchored3078 {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), a.baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	out := make(chan anchored3078, 1)
	go func() {
		defer conn.Close(context.Background())
		var rc func(context.Context) (*pgconn.PgConn, error)
		if withRepl {
			rc = a.replConnect
		}
		anchor, err := pgcapture.AnchorOnSlot(context.Background(), conn, a.slot, rc, wait, nil)
		out <- anchored3078{anchor, err}
	}()
	return out
}

func await3078[T any](t *testing.T, what string, ch <-chan T, limit time.Duration) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(limit):
		t.Fatalf("%s: not within %s", what, limit)
		panic("unreachable")
	}
}

// A slot that has its position, with nobody attached (capture stopped, or a
// snapshot before the first capture): no wait, and the floor is the slot's own
// confirmed position.
func TestAnchorOnSlot_ReadySlotIsNotWaitedFor_3078(t *testing.T) {
	a := newAnchor3078(t, "ready")
	first, err := pgcapture.AnchorOnSlot(context.Background(), a.conn, a.slot, a.replConnect, time.Minute, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !first.Created || first.Waited <= 0 || first.Floor == 0 || first.Floor != a.confirmed(t) {
		t.Fatalf("first call = %+v, want the slot created and its confirmed position %s", first, a.confirmed(t))
	}
	began := time.Now()
	again, err := pgcapture.AnchorOnSlot(context.Background(), a.conn, a.slot, nil, time.Minute, nil)
	if err != nil {
		t.Fatalf("existing slot: %v", err)
	}
	if again.Created || again.Waited != 0 || again.Floor != first.Floor {
		t.Errorf("second call = %+v, want the same floor %s, nothing created, no wait", again, first.Floor)
	}
	if took := time.Since(began); took > 2*time.Second {
		t.Errorf("a slot that has its position took %s", took)
	}
	if err := pgcapture.CheckSlotStillAnchors(context.Background(), a.conn, a.slot, first.Floor); err != nil {
		t.Errorf("CheckSlotStillAnchors on the same slot: %v", err)
	}
	if err := pgcapture.CheckSlotStillAnchors(context.Background(), a.conn, a.slot, first.Floor+1); err == nil || !strings.Contains(err.Error(), "not the same slot") {
		t.Errorf("a slot behind where the snapshot started: err = %v, want it refused as another slot", err)
	}
	if _, err := a.conn.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", a.slot); err != nil {
		t.Fatal(err)
	}
	if err := pgcapture.CheckSlotStillAnchors(context.Background(), a.conn, a.slot, first.Floor); err == nil || !strings.Contains(err.Error(), "no longer exists") {
		t.Errorf("a slot that was removed: err = %v, want it said", err)
	}
}

// No slot and no replication connection: the refusal it always was.
func TestAnchorOnSlot_AbsentWithoutReplConnection_3078(t *testing.T) {
	a := newAnchor3078(t, "norepl")
	_, err := pgcapture.AnchorOnSlot(context.Background(), a.conn, a.slot, nil, time.Minute, nil)
	if err == nil || !strings.Contains(err.Error(), "does not exist and no replication connection is configured") {
		t.Fatalf("err = %v", err)
	}
}

// Capture attaches during the wait: the snapshot goes on with the slot's
// position, which is where capture reads from.
func TestAnchorOnSlot_WaitsForASlotBeingCreated_3078(t *testing.T) {
	a := newAnchor3078(t, "waits")
	a.holdOpen(t)
	creating, _ := a.startCreating(t)

	if _, err := pgcapture.SlotFloorLSN(context.Background(), a.conn, a.slot); !errors.Is(err, pgcapture.ErrSlotNotReady) {
		t.Errorf("SlotFloorLSN on a slot being created: err = %v, want ErrSlotNotReady (its restart_lsn is not a position it delivers from)", err)
	}
	if err := pgcapture.CheckSlotStillAnchors(context.Background(), a.conn, a.slot, 1); !errors.Is(err, pgcapture.ErrSlotNotReady) {
		t.Errorf("CheckSlotStillAnchors on a slot being created: err = %v, want ErrSlotNotReady", err)
	}

	got := a.anchorAsync(t, true, time.Minute)
	select {
	case r := <-got:
		t.Fatalf("anchored on a slot that is still being created: %+v, err %v", r.anchor, r.err)
	case <-time.After(1500 * time.Millisecond):
	}
	a.release(t)
	c := await3078(t, "the slot's creation", creating, 30*time.Second)
	if c.err != nil {
		t.Fatalf("the creation failed: %v", c.err)
	}
	r := await3078(t, "AnchorOnSlot", got, 30*time.Second)
	if r.err != nil {
		t.Fatalf("AnchorOnSlot: %v", r.err)
	}
	if r.anchor.Created || r.anchor.Floor != c.point || r.anchor.Waited <= 0 {
		t.Errorf("anchor = %+v, want the creator's consistent point %s, not created here, a wait recorded", r.anchor, c.point)
	}
}

// Capture never attaches: refused at the cap, with what to wait for, and the
// slot being created is left to its creator.
func TestAnchorOnSlot_RefusedWhenTheSlotNeverGetsItsPosition_3078(t *testing.T) {
	a := newAnchor3078(t, "never")
	a.holdOpen(t)
	creating, _ := a.startCreating(t)

	began := time.Now()
	r := await3078(t, "AnchorOnSlot", a.anchorAsync(t, true, time.Second), 20*time.Second)
	if !errors.Is(r.err, pgcapture.ErrSlotNotReady) {
		t.Fatalf("err = %v, want ErrSlotNotReady", r.err)
	}
	for _, want := range []string{"no table was read", `"` + a.slot + `" is still being created after 1s`, "confirmed_flush_lsn", "pg_stat_activity"} {
		if !strings.Contains(r.err.Error(), want) {
			t.Errorf("the refusal does not say %q:\n%v", want, r.err)
		}
	}
	if took := time.Since(began); took > 10*time.Second {
		t.Errorf("a 1s wait took %s", took)
	}
	select {
	case c := <-creating:
		t.Fatalf("the creation in progress was disturbed: %+v", c)
	default:
	}
	a.release(t)
	if c := await3078(t, "the slot's creation", creating, 30*time.Second); c.err != nil {
		t.Fatalf("the creation failed after the refusal: %v", c.err)
	}
	// Cured by itself: the same call now goes through, with no wait.
	again, err := pgcapture.AnchorOnSlot(context.Background(), a.conn, a.slot, a.replConnect, time.Second, nil)
	if err != nil || again.Waited != 0 || again.Created {
		t.Fatalf("after the slot got its position: %+v, err %v", again, err)
	}
}

// The creator gives up (capture gives its start 30 seconds) and PostgreSQL
// removes the unfinished slot: the snapshot creates the slot itself and still
// waits for its position.
func TestAnchorOnSlot_CreatesTheSlotItsCreatorGaveUp_3078(t *testing.T) {
	a := newAnchor3078(t, "gaveup")
	a.holdOpen(t)
	creating, giveUp := a.startCreating(t)
	got := a.anchorAsync(t, true, time.Minute)
	time.Sleep(600 * time.Millisecond)
	giveUp()
	if c := await3078(t, "the abandoned creation", creating, 20*time.Second); c.err == nil {
		t.Fatal("the creation ended although its transaction is still open")
	}
	select {
	case r := <-got:
		t.Fatalf("returned while no slot can have a position: %+v, err %v", r.anchor, r.err)
	case <-time.After(1500 * time.Millisecond):
	}
	a.release(t)
	r := await3078(t, "AnchorOnSlot", got, 30*time.Second)
	if r.err != nil {
		t.Fatalf("AnchorOnSlot: %v", r.err)
	}
	if !r.anchor.Created || r.anchor.Floor == 0 || r.anchor.Floor != a.confirmed(t) {
		t.Errorf("anchor = %+v, want the slot created here at its confirmed position %s", r.anchor, a.confirmed(t))
	}
}

// No slot, and the source never lets one be created: refused at the cap, and
// no unfinished slot is left behind.
func TestAnchorOnSlot_OwnCreationRefusedAtTheCap_3078(t *testing.T) {
	a := newAnchor3078(t, "owncap")
	a.holdOpen(t)
	r := await3078(t, "AnchorOnSlot", a.anchorAsync(t, true, 1500*time.Millisecond), 20*time.Second)
	if !errors.Is(r.err, pgcapture.ErrSlotNotReady) || !strings.Contains(r.err.Error(), "could not be created within 1.5s") {
		t.Fatalf("err = %v, want the refusal of a creation that did not end", r.err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for a.listed(t) {
		if time.Now().After(deadline) {
			t.Fatal("the unfinished slot is still listed 10s after the refusal")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Two snapshots at once on a source with no slot: one creates it, both get
// the same floor.
func TestAnchorOnSlot_TwoAtOnce_3078(t *testing.T) {
	a := newAnchor3078(t, "two")
	var wg sync.WaitGroup
	results := make([]anchored3078, 2)
	for i := range results {
		ch := a.anchorAsync(t, true, time.Minute)
		wg.Add(1)
		go func() { defer wg.Done(); results[i] = <-ch }()
	}
	wg.Wait()
	created := 0
	for _, r := range results {
		if r.err != nil {
			t.Fatalf("AnchorOnSlot: %v", r.err)
		}
		if r.anchor.Created {
			created++
		}
	}
	if created != 1 || results[0].anchor.Floor != results[1].anchor.Floor || results[0].anchor.Floor != a.confirmed(t) {
		t.Fatalf("results = %+v, want one creator and one floor %s", results, a.confirmed(t))
	}
}
