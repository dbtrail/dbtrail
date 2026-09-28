package streamrun

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
)

// This file is the look BEFORE the resume-time cleanup (#1708). It never
// deletes, never kills and never changes what the cleanup removes: it reads the
// index server's process list, and either lets the cleanup start or waits.
//
// The outage it answers: a restart during the cleanup ends the client, but the
// driver only closes the socket, so the DELETE keeps running inside MySQL with
// its locks. The next start launched a second DELETE, which waited on those
// locks and failed at innodb_lock_wait_timeout (50 s) with an error that named
// a lock and not the cause. Every further restart added one more orphan.

// DefaultResumeCleanupWait is how long a start waits for an earlier cleanup
// before giving up. The longest cleanup measured is about 30 minutes (#1690,
// a 48 GB index with no floor), so an hour covers one that began just before
// the restart with the same again to spare.
const DefaultResumeCleanupWait = time.Hour

// ResumeCleanupWait is the effective ceiling, set from --cleanup-wait-timeout
// at startup. A var for the same reason indexer.WriteTimeout is one: a flag
// sets it and tests shorten it. Zero turns the look off and the cleanup starts
// at once, as it did before #1708; negative is rejected by One.
var ResumeCleanupWait = DefaultResumeCleanupWait

// resumeCleanupPoll is how often the wait looks again.
var resumeCleanupPoll = 5 * time.Second

// cleanupProbeTimeout bounds one read of the process list.
const cleanupProbeTimeout = 10 * time.Second

// cleanupProbeMaxFailures is how many reads in a row may fail DURING a wait
// before the wait ends and the cleanup starts as it would have without the
// look. The first read failing ends it at once (see waitForEarlierCleanup).
const cleanupProbeMaxFailures = 3

// PhaseResumeCleanupWaiting is the OnPhase value while a start waits for a
// cleanup that an earlier run left executing on the index server. No cleanup
// of this run has started yet.
const PhaseResumeCleanupWaiting = "resume_cleanup_waiting"

// ErrEarlierCleanupRunning tags the failure of a start that waited the whole
// ceiling for an earlier cleanup. Callers match it with errors.Is; the text is
// for people and may change.
var ErrEarlierCleanupRunning = errors.New("an earlier resume cleanup is still running on the index")

// EarlierCleanupError is the ceiling failure with what was seen.
type EarlierCleanupError struct {
	// ConnectionID is the index server's id for the oldest cleanup still
	// running, the value KILL takes.
	ConnectionID uint64
	// Running is how long that statement had been executing at the last look.
	Running time.Duration
	// Waited is how long this start waited before giving up.
	Waited time.Duration
	// Count is how many such statements were running at the last look.
	Count int
}

func (e *EarlierCleanupError) Error() string {
	others := ""
	if e.Count > 1 {
		others = fmt.Sprintf(" (and %d more)", e.Count-1)
	}
	return fmt.Sprintf("%s: connection id %d%s, running for %s; waited %s and capture did not start. "+
		"Let it finish, or end it on the index server with KILL %d, then start capture again. "+
		"Raise --cleanup-wait-timeout to wait longer",
		ErrEarlierCleanupRunning.Error(), e.ConnectionID, others,
		e.Running.Round(time.Second), e.Waited.Round(time.Second), e.ConnectionID)
}

func (e *EarlierCleanupError) Unwrap() error { return ErrEarlierCleanupRunning }

// processRow is one row of information_schema.PROCESSLIST.
type processRow struct {
	ID      uint64
	User    sql.NullString
	DB      sql.NullString
	Seconds int64
	Info    sql.NullString
	// Self marks the connection that ran the read.
	Self bool
}

// The statements the cleanup runs, as the process list shows them once
// whitespace is collapsed and case is folded. The position delete has two
// spellings (with and without the floor term); the GTID straggler delete has
// one. TestCleanupRecognizerMatchesTheRealStatements feeds the statements the
// cleanup actually sends through isResumeCleanupStatement, so an edit to them
// that this list does not follow turns that test red.
const (
	cleanupStmtPrefix   = "DELETE FROM BINLOG_EVENTS WHERE "
	cleanupStmtPosition = "(CHAR_LENGTH(BINLOG_FILE) > CHAR_LENGTH("
	cleanupStmtFloor    = "EVENT_ID >= "
	cleanupStmtFloorAnd = " AND ("
	cleanupStmtGTID     = "GTID IN ("
)

// isResumeCleanupStatement reports whether a process list statement is the
// resume-time cleanup. Strict on purpose: a match makes a start WAIT, so
// taking another statement for the cleanup would leave capture waiting on
// something that is not ours. A statement cut short before the part that
// tells it apart does not match, and the cleanup then starts as before.
func isResumeCleanupStatement(info string) bool {
	s := strings.ToUpper(strings.Join(strings.Fields(info), " "))
	rest, ok := strings.CutPrefix(s, cleanupStmtPrefix)
	if !ok {
		return false
	}
	if strings.HasPrefix(rest, cleanupStmtGTID) || strings.HasPrefix(rest, cleanupStmtPosition) {
		return true
	}
	// With a floor: "EVENT_ID >= <one token> AND (" and then the position
	// predicate. The token is "?" for a prepared statement and digits when the
	// driver interpolates.
	rest, ok = strings.CutPrefix(rest, cleanupStmtFloor)
	if !ok {
		return false
	}
	token, rest, ok := strings.Cut(rest, cleanupStmtFloorAnd)
	if !ok || token == "" || strings.ContainsAny(token, " ()") {
		return false
	}
	return strings.HasPrefix(rest, cleanupStmtPosition)
}

// earlierCleanups picks, from one read of the process list, the cleanups other
// connections are running as the same user on the same database as the
// connection that did the read. Oldest first.
//
// User and database are compared exactly, and both sides come from the process
// list itself, so nothing depends on how a DSN spells them. The database is
// load-bearing: a daemon keeps one index database per source under ONE user,
// and another source's cleanup does not lock this one's table.
func earlierCleanups(rows []processRow) ([]processRow, error) {
	var self *processRow
	for i := range rows {
		if rows[i].Self {
			self = &rows[i]
			break
		}
	}
	switch {
	case self == nil:
		return nil, errors.New("the process list does not show this connection")
	case !self.User.Valid || self.User.String == "":
		return nil, errors.New("the process list shows no user for this connection")
	case !self.DB.Valid || self.DB.String == "":
		return nil, errors.New("the process list shows no database for this connection")
	}
	var found []processRow
	for _, r := range rows {
		if r.Self || !r.User.Valid || r.User.String != self.User.String {
			continue
		}
		if !r.DB.Valid || r.DB.String != self.DB.String {
			continue
		}
		if !r.Info.Valid || !isResumeCleanupStatement(r.Info.String) {
			continue
		}
		found = append(found, r)
	}
	// Oldest first, and by id among equals so the one reported is stable.
	slices.SortFunc(found, func(a, b processRow) int {
		if c := cmp.Compare(b.Seconds, a.Seconds); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return found, nil
}

// cleanupProbe reads the process list once.
type cleanupProbe func(ctx context.Context) ([]processRow, error)

// processListProbe reads information_schema.PROCESSLIST on the index server.
//
// That table and not performance_schema.threads: it exists on MySQL 8.0, 8.4
// and MariaDB, and it needs no grant. Without the PROCESS privilege a user
// sees the threads of its own account, which is every connection this look is
// for. performance_schema needs a SELECT grant the index user is not asked to
// have.
func processListProbe(db *sql.DB) cleanupProbe {
	return func(ctx context.Context) ([]processRow, error) {
		ctx, cancel := context.WithTimeout(ctx, cleanupProbeTimeout)
		defer cancel()
		rows, err := db.QueryContext(ctx,
			`SELECT ID, USER, DB, TIME, INFO, ID = CONNECTION_ID() FROM information_schema.PROCESSLIST`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []processRow
		for rows.Next() {
			var r processRow
			var seconds sql.NullInt64
			if err := rows.Scan(&r.ID, &r.User, &r.DB, &seconds, &r.Info, &r.Self); err != nil {
				return nil, err
			}
			r.Seconds = seconds.Int64
			out = append(out, r)
		}
		return out, rows.Err()
	}
}

// cleanupWaitDetail is the short qualifier a supervisor shows beside the
// waiting phase.
func cleanupWaitDetail(found []processRow) string {
	d := fmt.Sprintf("connection %d, running for %s", found[0].ID, (time.Duration(found[0].Seconds) * time.Second).String())
	if len(found) > 1 {
		d += fmt.Sprintf(", and %d more", len(found)-1)
	}
	return d
}

// waitForEarlierCleanup returns nil when the resume-time cleanup may start:
// no earlier one is running, or the look could not be done. It returns an
// *EarlierCleanupError when one is still running after ceiling, and ctx's
// error when the stream is stopped while it waits.
//
// A look that fails is not a capture failure. Before #1708 there was no look
// at all, so a server that refuses it, or has no such table, starts exactly as
// it did. The same holds for cleanupProbeMaxFailures failed looks in a row
// during a wait. One failed look in the middle keeps waiting: a cleanup was
// running a moment ago, and starting a second one on a hiccup is the collision
// this exists to prevent.
func waitForEarlierCleanup(ctx context.Context, probe cleanupProbe, hooks *Hooks, ceiling, poll time.Duration) error {
	if ceiling <= 0 {
		return nil
	}
	started := time.Now()
	var last []processRow
	failures := 0
	announce := func(phase, detail string) {
		if hooks == nil {
			return
		}
		if hooks.OnPhase != nil {
			hooks.OnPhase(phase)
		}
		if hooks.OnPhaseDetail != nil {
			hooks.OnPhaseDetail(detail)
		}
	}
	for {
		rows, err := probe(ctx)
		if err != nil && ctx.Err() != nil {
			if last != nil {
				announce("", "")
			}
			return fmt.Errorf("stopped while waiting for an earlier resume cleanup: %w", ctx.Err())
		}
		var found []processRow
		if err == nil {
			found, err = earlierCleanups(rows)
		}
		switch {
		case err != nil:
			failures++
			if last == nil || failures >= cleanupProbeMaxFailures {
				slog.Warn("could not look for an earlier resume cleanup on the index; starting the cleanup without the look",
					"error", err, "failed_looks", failures)
				if last != nil {
					announce("", "")
				}
				return nil
			}
			slog.Warn("could not look again for the earlier resume cleanup; still waiting", "error", err, "failed_looks", failures)
		case len(found) == 0:
			if last != nil {
				announce("", "")
				waited := time.Since(started).Round(time.Second)
				fmt.Printf("Cleanup: the earlier cleanup finished after a %s wait ✓\n", waited)
				slog.Info("the earlier resume cleanup finished; starting this run's cleanup", "waited", waited)
			}
			return nil
		default:
			failures = 0
			if last == nil {
				fmt.Printf("Cleanup: a cleanup from a previous run is still running on the index (%s); waiting for it, up to %s\n",
					cleanupWaitDetail(found), ceiling)
				slog.Warn("a resume cleanup from a previous run is still running on the index; waiting for it instead of starting another",
					"connection_id", found[0].ID, "running_seconds", found[0].Seconds, "count", len(found), "ceiling", ceiling)
			}
			last = found
			announce(PhaseResumeCleanupWaiting, cleanupWaitDetail(found))
		}
		waited := time.Since(started)
		if waited >= ceiling {
			announce("", "")
			return &EarlierCleanupError{
				ConnectionID: last[0].ID,
				Running:      time.Duration(last[0].Seconds) * time.Second,
				Waited:       waited,
				Count:        len(last),
			}
		}
		// Never sleep past the ceiling: the last look lands on it.
		select {
		case <-time.After(min(poll, ceiling-waited)):
		case <-ctx.Done():
			announce("", "")
			return fmt.Errorf("stopped while waiting for an earlier resume cleanup: %w", ctx.Err())
		}
	}
}

// awaitEarlierCleanup is waitForEarlierCleanup as One calls it. stopped means
// the stream was stopped while it waited, and One returns without an error:
// the operator asked for the stop, nothing of this run had started, and there
// is no position to flush.
func awaitEarlierCleanup(ctx context.Context, db *sql.DB, hooks *Hooks) (stopped bool, err error) {
	err = waitForEarlierCleanup(ctx, processListProbe(db), hooks, ResumeCleanupWait, resumeCleanupPoll)
	if err != nil && ctx.Err() != nil && !errors.Is(err, ErrEarlierCleanupRunning) {
		slog.Info("stopped while waiting for an earlier resume cleanup; capture had not started")
		return true, nil
	}
	return false, err
}
