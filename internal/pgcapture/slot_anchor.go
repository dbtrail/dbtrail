package pgcapture

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DefaultSlotWait is how long a snapshot waits for a replication slot that
// another session is creating to have a position before it is refused
// (AnchorOnSlot). A creation the snapshot does itself is not bounded by it.
const DefaultSlotWait = 5 * time.Minute

// slotAnchorPoll is how often a slot that is being created is looked at again.
const slotAnchorPoll = 250 * time.Millisecond

// slotNoticeEvery is how often a creation of the slot by AnchorOnSlot itself
// says, at most, that it is still waiting; slotWaitWarnAfter is how long a
// wait for another session's creation lasts before it is said at warning
// level. Variables so a test does not wait a minute.
var (
	slotNoticeEvery   = time.Minute
	slotWaitWarnAfter = 30 * time.Second
)

// slotBlockersHint names where to see what holds a slot's creation back: the
// transactions with an id of their own (PostgreSQL waits for those; a read
// with no write has none and does not hold it), oldest first.
const slotBlockersHint = "pg_stat_activity shows what holds it: the session with the oldest xact_start among those WHERE backend_xid IS NOT NULL"

// ErrSlotNotReady marks a replication slot that PostgreSQL lists but that has
// no position yet: its creation has not ended. PostgreSQL ends the creation of
// a logical slot only once every transaction that was open on the source when
// it began has ended, and until then the slot has no confirmed_flush_lsn. Such
// a slot delivers nothing that commits before its creation ends, and it is
// removed if the creation is given up, so nothing may be anchored on it
// (#3078).
var ErrSlotNotReady = errors.New("replication slot is still being created")

// SlotNotReadyError is AnchorOnSlot's refusal of a snapshot whose slot,
// created by another session, had no position within the wait. Nothing was
// read: it is returned before the snapshot transaction opens. It matches
// ErrSlotNotReady; other errors that do (a slot found being created again
// after the read, CheckSlotStillAnchors) are not this one.
type SlotNotReadyError struct {
	Slot string
	Wait time.Duration
}

func (e *SlotNotReadyError) Error() string {
	return fmt.Sprintf("pgcapture: no table was read: replication slot %q is still being created after %s. PostgreSQL finishes creating a slot only when every transaction that was open on the source when the creation began has ended, and a snapshot read before that would miss the changes committed until then. Run the snapshot again once the slot has a confirmed_flush_lsn in pg_replication_slots (capture is then attached); %s: %v",
		e.Slot, e.Wait, slotBlockersHint, ErrSlotNotReady)
}

func (e *SlotNotReadyError) Unwrap() error { return ErrSlotNotReady }

// slotUnreadableError is a catalog read of a slot that failed (a cut
// connection, a query error), as opposed to an answer about the slot.
type slotUnreadableError struct{ err error }

func (e *slotUnreadableError) Error() string { return e.err.Error() }
func (e *slotUnreadableError) Unwrap() error { return e.err }

// SlotReadFailed marks err as a failed read of a slot, as querySlotStanding
// marks its own: for a caller's seam (and its tests) that stands in for it.
func SlotReadFailed(err error) error { return &slotUnreadableError{err} }

// IsSlotUnreadable reports whether err is a failed read of a slot's catalog
// row (querySlotStanding) rather than a verdict about the slot: a read that
// may go through if asked again, where a verdict would not change.
func IsSlotUnreadable(err error) bool {
	var u *slotUnreadableError
	return errors.As(err, &u)
}

// SlotAnchor is the slot a snapshot is anchored on, as AnchorOnSlot found it.
type SlotAnchor struct {
	// Floor is the slot's confirmed_flush_lsn, read before the caller opens
	// its snapshot transaction: every transaction that commits at or after
	// it is delivered by the slot (see SlotFloorLSN).
	Floor pglogrepl.LSN
	// Created reports that this call created the slot.
	Created bool
	// Waited is how long the slot took to have a position, its creation by
	// this call included; zero when it had one at the first look.
	Waited time.Duration
}

// slotStanding is what pg_replication_slots says of one slot of this source.
type slotStanding struct {
	found     bool
	walStatus string
	// confirmed is the slot's confirmed_flush_lsn; hasPosition is false while
	// the slot is being created.
	confirmed   pglogrepl.LSN
	hasPosition bool
}

// querySlotStanding is queryScopedSlotState with the slot's position: the same
// identity rule (a pgoutput logical slot of the current database, anything
// else of that name is an error), catalog columns only. A read that failed is
// a slotUnreadableError; a slot of the wrong kind is not.
func querySlotStanding(ctx context.Context, conn *pgx.Conn, slotName string) (slotStanding, error) {
	var slotType, plugin, database, walStatus, curDB string
	var confirmed *string
	err := conn.QueryRow(ctx, `
		SELECT slot_type, coalesce(plugin, ''), coalesce(database, ''), coalesce(wal_status, ''), current_database(), confirmed_flush_lsn::text
		FROM pg_replication_slots WHERE slot_name = $1`, slotName).
		Scan(&slotType, &plugin, &database, &walStatus, &curDB, &confirmed)
	if errors.Is(err, pgx.ErrNoRows) {
		return slotStanding{}, nil
	}
	if err != nil {
		return slotStanding{}, &slotUnreadableError{fmt.Errorf("pgcapture: checking replication slot %q: %w", slotName, err)}
	}
	if slotType != "logical" || plugin != "pgoutput" || database != curDB {
		return slotStanding{}, fmt.Errorf("pgcapture: replication slot %q exists but is not a pgoutput logical slot on database %q (slot_type=%q, plugin=%q, database=%q) — it cannot anchor this source; use a different --slot name or remove the conflicting slot", slotName, curDB, slotType, plugin, database)
	}
	st := slotStanding{found: true, walStatus: walStatus}
	if confirmed != nil && *confirmed != "" {
		lsn, err := pglogrepl.ParseLSN(*confirmed)
		if err != nil {
			return slotStanding{}, fmt.Errorf("pgcapture: parsing confirmed_flush_lsn %q of replication slot %q: %w", *confirmed, slotName, err)
		}
		st.confirmed, st.hasPosition = lsn, lsn != 0
	}
	return st, nil
}

// AnchorOnSlot returns the named pgoutput slot's position once the slot can
// anchor a snapshot, creating the slot when it is absent. It is the baseline
// producer's (#593) slot seam: the snapshot transaction must open AFTER the
// slot has its position, so that position is at or before every row the
// snapshot reads and no change between the two can be missed (overlap is
// redelivered; the merge is last-write-wins).
//
// A slot that PostgreSQL lists but that has no confirmed_flush_lsn is being
// created, by capture or by another snapshot (#3078). It is not an anchor yet:
// what commits before its creation ends is never delivered by it, and it is
// gone if its creator is stopped. So this
// waits, up to wait (DefaultSlotWait when zero or negative), for that slot to
// have its position. Past the wait the snapshot is refused with a
// *SlotNotReadyError, before anything was read. A slot that already has its
// position costs one catalog read and no wait.
//
// An absent slot, or one whose creator gave up during the wait, is created
// here, and that creation is NOT bounded by wait: only ctx ends it, as it
// always has. On a source that always holds an open write transaction, giving it up at a
// cap would leave that source with neither a snapshot nor capture; capture's
// own creation has no cap either (#3178). It says at most once a minute how long it has waited
// and where to see what holds it.
//
// replConnect lazily opens a REPLICATION connection (replication=database)
// and is called only when the slot must be created; nil means none is
// configured, and a missing slot is then an actionable error. The connection
// is closed before returning.
//
// Slot identity is scoped, not name-only: a same-named physical slot, a slot
// of another plugin or of another database fails loud (querySlotStanding). A
// wal_status=lost slot fails loud too, wrapping ErrSlotLost.
func AnchorOnSlot(ctx context.Context, queryConn *pgx.Conn, slotName string, replConnect func(context.Context) (*pgconn.PgConn, error), wait time.Duration, logger *slog.Logger) (SlotAnchor, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if wait <= 0 {
		wait = DefaultSlotWait
	}
	started := time.Now()
	// The wait for another session's creation; it starts when one is first
	// seen, so time spent on a creation of our own is not counted in it.
	var waitStarted time.Time
	var anchor SlotAnchor
	said, warned := false, false
	for {
		st, err := querySlotStanding(ctx, queryConn, slotName)
		if err != nil {
			return SlotAnchor{}, err
		}
		if st.found && st.walStatus == WalStatusLost {
			return SlotAnchor{}, fmt.Errorf("pgcapture: replication slot %q is invalidated (wal_status=lost; max_slot_wal_keep_size exceeded) — drop and recreate it (the stream must also re-baseline): %w", slotName, ErrSlotLost)
		}
		if st.found && st.hasPosition {
			anchor.Floor = st.confirmed
			if said || anchor.Created {
				anchor.Waited = time.Since(started)
			}
			if said {
				logger.Info("pgcapture: the replication slot has its position; the snapshot starts", "slot", slotName, "waited", anchor.Waited.Round(time.Millisecond))
			}
			return anchor, nil
		}
		if !st.found {
			if replConnect == nil {
				return SlotAnchor{}, fmt.Errorf("pgcapture: replication slot %q does not exist and no replication connection is configured — provide --repl-dsn (replication=database) so the slot can be created, or create it first (e.g. by running `bintrail-pg stream`)", slotName)
			}
			// Said before, not after: the creation is where a busy source
			// makes this call wait.
			logger.Info("pgcapture: creating the replication slot, which PostgreSQL finishes once the write transactions open on the source have ended",
				"slot", slotName)
			created, err := createSlot(ctx, slotName, replConnect, logger)
			if err != nil {
				return SlotAnchor{}, err
			}
			if created {
				// Read again: the position comes from the catalog, as it
				// does for a slot that was already there.
				anchor.Created = true
				continue
			}
			// Another session is creating a slot of this name: wait for it,
			// from now (the wait before was for a creation that is gone).
			waitStarted = time.Time{}
		}
		if waitStarted.IsZero() {
			waitStarted = time.Now()
		}
		if !said {
			said = true
			logger.Info("pgcapture: the replication slot is still being created, which PostgreSQL finishes once the write transactions open on the source have ended; the snapshot waits for it",
				"slot", slotName, "up_to", wait)
		}
		waited := time.Since(waitStarted)
		if !warned && waited >= slotWaitWarnAfter {
			warned = true
			logger.Warn("pgcapture: the snapshot is waiting for the replication slot to finish being created; nothing is read until it has a position. "+slotBlockersHint,
				"slot", slotName, "waited", waited.Round(time.Second), "up_to", wait)
		}
		if waited >= wait {
			return SlotAnchor{}, &SlotNotReadyError{Slot: slotName, Wait: wait}
		}
		select {
		case <-ctx.Done():
			return SlotAnchor{}, ctx.Err()
		case <-time.After(slotAnchorPoll):
		}
	}
}

// createSlot creates the slot, for as long as ctx lets it: PostgreSQL ends the
// creation once the write transactions open when it began have ended, and
// nothing here gives it up before that. created is false with a nil error
// when a slot of that name appeared meanwhile (SQLSTATE 42710): the caller
// looks at it. While it waits it says so, at most every slotNoticeEvery, with
// how long it has waited. Given up by ctx, the replication connection is
// closed and the unfinished slot is gone from pg_replication_slots within
// seconds (TestAnchorOnSlot_OwnCreationCancelledLeavesNoSlot_3078 holds
// that, with the transaction that kept the creation waiting still open).
func createSlot(ctx context.Context, slotName string, replConnect func(context.Context) (*pgconn.PgConn, error), logger *slog.Logger) (created bool, err error) {
	began := time.Now()
	done := make(chan struct{})
	noticed := make(chan struct{})
	go func() {
		defer close(noticed)
		tick := time.NewTicker(slotNoticeEvery)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				logger.Warn("pgcapture: still creating the replication slot: PostgreSQL finishes it once the write transactions that were open on the source when the creation began have ended, and the snapshot is not read before. "+slotBlockersHint,
					"slot", slotName, "waited", time.Since(began).Round(time.Second))
			}
		}
	}()
	defer func() {
		close(done)
		<-noticed
	}()
	replConn, err := replConnect(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, fmt.Errorf("pgcapture: connecting to create replication slot %q: %w", slotName, err)
	}
	defer replConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err == nil {
		return true, nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.SQLState() == "42710" {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return false, fmt.Errorf("pgcapture: creating replication slot %q: %w", slotName, err)
}

// CheckSlotStillAnchors is asked when a snapshot has read its tables and
// before it is published: the slot it was anchored on (floor, from
// AnchorOnSlot) must still be there, with a position at or after floor. A
// slot that is gone, lost, being created again, or behind where it was is not
// the slot the snapshot started under, and the changes made since the
// snapshot began may not be delivered: a gap that is known now, so the
// snapshot is not published. A read of the catalog that failed is not a
// verdict: IsSlotUnreadable tells it apart, for a caller that asks again.
//
// It cannot see a slot that was dropped and created again with a position in
// the meantime; whoever holds the index can (the capture's recorded start).
func CheckSlotStillAnchors(ctx context.Context, conn *pgx.Conn, slotName string, floor pglogrepl.LSN) error {
	st, err := querySlotStanding(ctx, conn, slotName)
	if err != nil {
		return err
	}
	const tail = "the changes made on the source since the snapshot started may not be captured, so the snapshot is not published. Run it again once capture is attached to the slot"
	switch {
	case !st.found:
		return fmt.Errorf("pgcapture: replication slot %q no longer exists: it was removed while the snapshot was read; %s", slotName, tail)
	case st.walStatus == WalStatusLost:
		return fmt.Errorf("pgcapture: replication slot %q was invalidated while the snapshot was read (wal_status=lost; max_slot_wal_keep_size exceeded); %s: %w", slotName, tail, ErrSlotLost)
	case !st.hasPosition:
		return fmt.Errorf("pgcapture: replication slot %q was removed and is being created again while the snapshot was read; %s: %w", slotName, tail, ErrSlotNotReady)
	case st.confirmed < floor:
		return fmt.Errorf("pgcapture: replication slot %q stands at %s, before %s where it stood when the snapshot started: it is not the same slot; %s", slotName, st.confirmed, floor, tail)
	}
	return nil
}
