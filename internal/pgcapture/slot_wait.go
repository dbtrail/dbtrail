package pgcapture

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PhaseCreatingSlot is the startup phase of a capture whose replication slot
// is being created (Config.OnPhase).
const PhaseCreatingSlot = "creating_slot"

// How soon the creation of a slot first looks for what holds it, how often it
// looks again, and how long one look may take. Variables so a test does not
// wait for them.
var (
	slotBlockerFirstLook = 3 * time.Second
	slotBlockerEvery     = 15 * time.Second
	slotBlockerTimeout   = 5 * time.Second
)

// slotBlocker is the oldest write transaction open on the source, reduced to
// what is safe to show on a screen and in a log. It never holds a statement
// or the name of a prepared transaction, which the application chooses and can
// carry data, nor the user or database of a transaction outside the captured
// database.
type slotBlocker struct {
	pid      int32 // its session; 0 for a prepared transaction, which has none
	prepared bool
	// otherDatabase: the session belongs to another database of the server.
	otherDatabase bool
	// user and database are set only for a session of the captured database.
	user     string
	database string
	// age is how long it has been open; negative when this role may not see it
	// (pg_stat_activity hides xact_start of other roles' sessions).
	age time.Duration
}

// describe words the blocker for the server's status and the log.
func (b slotBlocker) describe() string {
	var who string
	switch {
	case b.prepared:
		who = "a prepared transaction"
	case b.otherDatabase:
		who = fmt.Sprintf("the transaction of session %d, in another database", b.pid)
	default:
		who = fmt.Sprintf("the transaction of session %d", b.pid)
		var of []string
		if b.user != "" {
			of = append(of, "user "+b.user)
		}
		if b.database != "" {
			of = append(of, "database "+b.database)
		}
		if len(of) > 0 {
			who += " (" + strings.Join(of, ", ") + ")"
		}
	}
	text := "waiting for " + who + ", the oldest write transaction open"
	if b.age < 0 {
		text += "; this user cannot see for how long"
	} else {
		text += ", for " + b.age.Round(time.Second).String()
	}
	if b.prepared {
		text += "; pg_prepared_xacts on the source lists it"
	}
	return text
}

// readSlotBlocker finds the oldest open transaction holding a transaction id:
// PostgreSQL waits for those, across every database of the server, and a read
// with no write has none. Ordered by the age of the id, which pg_stat_activity
// shows for every session, where xact_start is hidden from other roles. A
// prepared transaction holds the creation too, with no session to show it.
//
// What must not be shown is left out here, in the statement, so it never
// reaches this process: no query text, no gid, and no user or database name
// for a transaction outside the current database.
func readSlotBlocker(ctx context.Context, conn *pgx.Conn, ownPID uint32) (slotBlocker, bool, error) {
	var (
		pid       *int32
		prepared  bool
		otherDB   bool
		user, dbn *string
		secs      *int64
	)
	err := conn.QueryRow(ctx, `
		SELECT pid, prepared, other_db, username, dbname, secs FROM (
			SELECT a.pid AS pid, false AS prepared,
			       a.datname IS DISTINCT FROM current_database() AS other_db,
			       CASE WHEN a.datname = current_database() THEN a.usename::text END AS username,
			       CASE WHEN a.datname = current_database() THEN a.datname::text END AS dbname,
			       extract(epoch FROM now() - a.xact_start)::bigint AS secs, age(a.backend_xid) AS xid_age
			FROM pg_stat_activity a
			WHERE a.backend_xid IS NOT NULL AND a.pid <> $1 AND a.pid <> pg_backend_pid()
			UNION ALL
			SELECT NULL::int, true, false, NULL::text, NULL::text,
			       extract(epoch FROM now() - p.prepared)::bigint, age(p.transaction)
			FROM pg_prepared_xacts p
		) open_xacts
		ORDER BY xid_age DESC LIMIT 1`, int32(ownPID)).Scan(&pid, &prepared, &otherDB, &user, &dbn, &secs)
	if errors.Is(err, pgx.ErrNoRows) {
		return slotBlocker{}, false, nil
	}
	if err != nil {
		return slotBlocker{}, false, fmt.Errorf("pgcapture: reading the oldest open transaction: %w", err)
	}
	b := slotBlocker{prepared: prepared, otherDatabase: otherDB, age: -1}
	if pid != nil {
		b.pid = *pid
	}
	if user != nil {
		b.user = *user
	}
	if dbn != nil {
		b.database = *dbn
	}
	if secs != nil {
		b.age = time.Duration(*secs) * time.Second
	}
	return b, true, nil
}

// createSlotWaiting creates the slot for as long as ctx lets it, as a snapshot's
// own creation does (createSlot), and says meanwhile what it is waiting for.
// Given up by ctx, the connection is closed and PostgreSQL removes the
// unfinished slot.
func (c *Capturer) createSlotWaiting(ctx context.Context, replConn *pgconn.PgConn, slot string) (pglogrepl.CreateReplicationSlotResult, error) {
	if c.cfg.OnPhase != nil {
		c.cfg.OnPhase(PhaseCreatingSlot)
		defer c.cfg.OnPhase("")
	}
	stop := c.watchSlotCreation(ctx, replConn.PID(), slot)
	defer stop()
	return pglogrepl.CreateReplicationSlot(ctx, replConn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
}

// watchSlotCreation reports, until stop is called, the oldest open write
// transaction. On a connection of its own, since the capture's are busy or must
// not be cut by a look that is given up. Advisory: a look that fails costs the
// detail and never the creation.
func (c *Capturer) watchSlotCreation(ctx context.Context, ownPID uint32, slot string) (stop func()) {
	wctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		began := time.Now()
		var conn *pgx.Conn
		defer func() {
			if conn != nil {
				closeQueryConn(conn)
			}
		}()
		var lastDetail string
		var lastLog time.Time
		lookFailed := false
		timer := time.NewTimer(slotBlockerFirstLook)
		defer timer.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-timer.C:
			}
			timer.Reset(slotBlockerEvery)

			look, cancelLook := context.WithTimeout(wctx, slotBlockerTimeout)
			var (
				b     slotBlocker
				found bool
				err   error
			)
			if conn == nil || conn.IsClosed() {
				conn, err = pgx.Connect(look, c.cfg.QueryDSN)
			}
			if err == nil {
				b, found, err = readSlotBlocker(look, conn, ownPID)
			}
			cancelLook()
			if wctx.Err() != nil {
				return
			}
			detail := ""
			switch {
			case err != nil:
				if !lookFailed {
					lookFailed = true
					c.logger.Warn("pgcapture: could not read which transaction the creation of the replication slot is waiting for; the creation goes on. "+slotBlockersHint,
						"slot", slot, "error", err)
				}
				if conn != nil && conn.IsClosed() {
					conn = nil
				}
			case found:
				detail = b.describe()
			}
			if detail != lastDetail {
				lastDetail = detail
				if c.cfg.OnPhaseDetail != nil {
					c.cfg.OnPhaseDetail(detail)
				}
			}
			if time.Since(lastLog) >= slotNoticeEvery {
				lastLog = time.Now()
				c.logger.Warn("pgcapture: still creating the replication slot: PostgreSQL finishes it once the write transactions that were open on the source when the creation began have ended, and capture starts then",
					"slot", slot, "waited", time.Since(began).Round(time.Second), "detail", detail)
			}
		}
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}
