package pgstreamrun

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// capturerStart is where the capturer started reading from. A variable so a
// test can drive the loop without a replication connection.
var capturerStart = (*pgcapture.Capturer).StartedFrom

// firstRunResend recognizes what PostgreSQL sends again to a capture that has
// no saved position, when an earlier run put rows in the index and stopped
// before its first save: a crash, or a lost capture lock, which forbids that
// save (#3114). The slot did not move, so those transactions arrive again.
//
// It drops an arriving change only when the index already holds that same
// change, and it deletes nothing. A resume with a saved position can remove
// what is past it (deleteEventsPastCheckpointPG), but here nothing proves that
// a row in the index will come back: its table may have left the filter or the
// publication, and an index can hold history of another source whose positions
// overlap this slot's. A row that does not come back simply stays. A check that
// cannot answer indexes the change, so the failure is a duplicate with a
// warning, never a loss.
//
// A change is the same one when table, key, commit LSN, commit second and kind
// all match. A key can change several times in one transaction and a
// transaction past the writer's caps can be in the index in part, so each
// arrival is counted against how many copies the index holds, per transaction.
type firstRunResend struct {
	// from and through bound the commit LSNs that can be in the index: where
	// this run started, and the newest row's position. through 0 means none.
	from, through uint64
	loaded        bool
	rows          map[resendKey]*resendCount
	markers       map[resendKey]*resendCount
}

type resendKey struct {
	schema, table, pk string
	kind              string
}

type resendCount struct{ held, seen int }

// load reads the window once, on the first change of a run with no saved
// position. Rows are written in commit order, so the newest event is the
// highest position an earlier run of this capture reached. A newest row before
// from means a new slot, or history of an earlier one: nothing to recognize.
func (f *firstRunResend) load(ctx context.Context, db *sql.DB, from uint64, logger *slog.Logger) {
	if f.loaded {
		return
	}
	f.loaded = true
	rctx, cancel := context.WithTimeout(ctx, resendCheckTimeout)
	defer cancel()
	var newest uint64
	err := db.QueryRowContext(rctx, `SELECT start_pos FROM binlog_events FORCE INDEX (PRIMARY) ORDER BY event_id DESC LIMIT 1`).Scan(&newest)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return
	case err != nil:
		if ctx.Err() == nil {
			logger.Warn("pgstreamrun: could not read what an earlier run left in the index; a transaction it indexed before its first save is indexed again, so it may be in the index twice", "error", err)
		}
		return
	}
	if from != 0 && newest >= from {
		f.from, f.through = from, newest
		logger.Info("pgstreamrun: the index holds changes from a run that stopped before its first save; those the source sends again are not indexed twice",
			"from", pglogrepl.LSN(from).String(), "through", pglogrepl.LSN(newest).String())
	}
}

func (f *firstRunResend) inWindow(lsn uint64) bool {
	return f.through != 0 && lsn >= f.from && lsn <= f.through
}

// endTxn forgets the transaction that ended, and closes the window once it is
// passed, so the rest of the stream asks the index nothing.
func (f *firstRunResend) endTxn(commitLSN uint64) {
	f.rows, f.markers = nil, nil
	if f.through != 0 && commitLSN >= f.through {
		f.through = 0
	}
}

// heldRow reports whether the index already holds this arrival of ev.
func (f *firstRunResend) heldRow(ctx context.Context, db *sql.DB, ev event.Event, logger *slog.Logger) bool {
	if !f.inWindow(ev.StartPos) {
		return false
	}
	key := resendKey{schema: ev.Schema, table: ev.Table, pk: ev.PKValues, kind: string(rune(ev.EventType))}
	c := f.rows[key]
	if c == nil {
		c = &resendCount{}
		rctx, cancel := context.WithTimeout(ctx, resendCheckTimeout)
		defer cancel()
		at := ev.Timestamp.Truncate(time.Second)
		err := db.QueryRowContext(rctx, `SELECT COUNT(*) FROM binlog_events FORCE INDEX (idx_pk_hash)
			WHERE schema_name = ? AND table_name = ? AND pk_hash = SHA2(?, 256)
			  AND event_timestamp BETWEEN ? AND ?
			  AND BINARY schema_name = ? AND BINARY table_name = ?
			  AND pk_values = ? AND start_pos = ? AND event_type = ?`,
			ev.Schema, ev.Table, ev.PKValues, at.Add(-time.Second), at.Add(time.Second),
			ev.Schema, ev.Table, ev.PKValues, ev.StartPos, uint8(ev.EventType)).Scan(&c.held)
		if err != nil {
			c.held = 0
			if ctx.Err() == nil {
				logger.Warn("pgstreamrun: could not read whether the index holds this change from a run that stopped before its first save; it is indexed again, so it may be in the index twice",
					"schema", ev.Schema, "table", ev.Table, "commit_lsn", pglogrepl.LSN(ev.StartPos).String(), "error", err)
			}
		}
		if f.rows == nil {
			f.rows = map[resendKey]*resendCount{}
		}
		f.rows[key] = c
	}
	c.seen++
	return c.seen <= c.held
}

// heldMarker is heldRow for a TRUNCATE marker in schema_changes.
func (f *firstRunResend) heldMarker(ctx context.Context, db *sql.DB, ev event.Event, logger *slog.Logger) bool {
	if !f.inWindow(ev.EndPos) {
		return false
	}
	key := resendKey{schema: ev.Schema, table: ev.Table, kind: string(ev.DDLType)}
	c := f.markers[key]
	if c == nil {
		c = &resendCount{}
		rctx, cancel := context.WithTimeout(ctx, resendCheckTimeout)
		defer cancel()
		err := db.QueryRowContext(rctx, `SELECT COUNT(*) FROM schema_changes
			WHERE schema_name = ? AND table_name = ? AND BINARY schema_name = ? AND BINARY table_name = ?
			  AND binlog_pos = ? AND ddl_type = ?`,
			ev.Schema, ev.Table, ev.Schema, ev.Table, ev.EndPos, string(ev.DDLType)).Scan(&c.held)
		if err != nil {
			c.held = 0
			if ctx.Err() == nil {
				logger.Warn("pgstreamrun: could not read whether the index records this TRUNCATE from a run that stopped before its first save; it is recorded again",
					"schema", ev.Schema, "table", ev.Table, "commit_lsn", pglogrepl.LSN(ev.EndPos).String(), "error", err)
			}
		}
		if f.markers == nil {
			f.markers = map[resendKey]*resendCount{}
		}
		f.markers[key] = c
	}
	c.seen++
	return c.seen <= c.held
}
