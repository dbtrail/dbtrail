package pgshim

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The copy's join check (sqlsandbox.Session.JoinNDV, internal/sqlsandbox
// joinguard.go) needs what the copy does not have: how many distinct values
// each column holds. The source has it, in pg_stats, and this port reads it
// there on the connection it already has open to ask a statement's plan
// (routeByCostPlan), once for the server every joinStatsFresh across all of
// its connections, never once per statement. Every statement the copy then
// runs on a connection that can send it to the source carries the counts
// (RunOptions.JoinNDV); one the copy refuses for its joins goes there
// (ReasonJoinTooBig), before the copy has run anything.
//
// Without a forwarding account there is nowhere to send a refused statement
// and nothing is read: the copy runs every statement as before. The capture
// account (Forward.CaptureAccount, #3054) is not used for it either: that
// account sends the source only what passes its gate. A read the source
// refuses, or that answers no statistics, leaves the check off until a later
// read succeeds.

// joinStatsSQL reads the source's count of distinct values of every column
// of its own tables, as one JSON object by lowercased column name. n_distinct
// is a count when positive and minus a fraction of the table's rows when
// negative (-1: unique); neither, or a table never analyzed (reltuples -1),
// is no count. The plan the copy checks names a column without its table, so
// a name that two tables hold is left out: the check takes it as selective.
const joinStatsSQL = `SELECT coalesce(json_object_agg(name, ndv), '{}')::text FROM (
  SELECT lower(s.attname) AS name,
         max(CASE WHEN s.n_distinct > 0 THEN s.n_distinct::float8
                  WHEN s.n_distinct < 0 AND c.reltuples > 0 THEN -s.n_distinct::float8 * c.reltuples::float8 END) AS ndv,
         count(DISTINCT c.oid) AS tables
  FROM pg_catalog.pg_stats s
  JOIN pg_catalog.pg_namespace n ON n.nspname = s.schemaname
  JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = s.tablename
  WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema') AND s.schemaname NOT LIKE 'pg\_toast%'
  GROUP BY 1) t
WHERE tables = 1 AND ndv IS NOT NULL`

// How long a server's statistics serve. Read again past joinStatsFresh, at
// the next statement whose plan is asked; used, while no newer read
// succeeds, until joinStatsKept; after a failed read, not tried again before
// joinStatsRetry. n_distinct moves with ANALYZE, which autovacuum runs as a
// table changes by a share of its rows: minutes, not seconds.
const (
	joinStatsFresh = 10 * time.Minute
	joinStatsKept  = time.Hour
	joinStatsRetry = time.Minute
)

// JoinStats is a server's column statistics for the copy's join check,
// shared by all of its connections (Forward.JoinStats). The zero value is
// ready.
type JoinStats struct {
	mu sync.Mutex
	// ndv is the last read's counts, read at readAt; tried is when a read
	// last began, reading whether one is under way.
	ndv     map[string]float64
	readAt  time.Time
	tried   time.Time
	reading bool
	// now is the clock, time.Now when nil (a test's own).
	now func() time.Time
}

func (j *JoinStats) clock() time.Time {
	if j.now != nil {
		return j.now()
	}
	return time.Now()
}

// current is the counts the check uses now, nil for none: never read, the
// last read found none, or it is older than joinStatsKept. Shared and never
// written after it is stored: the caller must not change it.
func (j *JoinStats) current() map[string]float64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.ndv == nil || j.clock().Sub(j.readAt) > joinStatsKept {
		return nil
	}
	return j.ndv
}

// begin says whether the caller reads the statistics now: they are due
// (never read, or past joinStatsFresh, and no failed read in the last
// joinStatsRetry) and no other connection is reading them. A caller told
// yes must call done.
func (j *JoinStats) begin() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	now := j.clock()
	switch {
	case j.reading:
		return false
	case !j.readAt.IsZero() && now.Sub(j.readAt) < joinStatsFresh:
		return false
	case !j.tried.IsZero() && now.Sub(j.tried) < joinStatsRetry:
		return false
	}
	j.reading, j.tried = true, now
	return true
}

// done ends a read begin allowed: ndv is what it found, err why it found
// nothing. A failed read keeps the last good counts.
func (j *JoinStats) done(ndv map[string]float64, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.reading = false
	if err != nil {
		return
	}
	j.ndv, j.readAt = ndv, j.clock()
}

// parseJoinStats reads joinStatsSQL's cell. A count that is not a positive
// number is left out; none at all is nil, no check.
func parseJoinStats(cell []byte) (map[string]float64, error) {
	var raw map[string]float64
	if err := json.Unmarshal(cell, &raw); err != nil {
		return nil, fmt.Errorf("the source's column statistics could not be read: %w", err)
	}
	var out map[string]float64
	for name, n := range raw {
		if n <= 0 || math.IsNaN(n) || math.IsInf(n, 0) {
			continue
		}
		if out == nil {
			out = map[string]float64{}
		}
		out[strings.ToLower(name)] = n
	}
	return out, nil
}

// joinStats is the server's statistics this connection reads and uses, nil
// when it has none: no forwarding account, the capture account, or a host
// that keeps none.
func (s *session) joinStats() *JoinStats {
	if s.bound == nil || s.bound.Forward == nil || s.bound.Forward.CaptureAccount {
		return nil
	}
	return s.bound.Forward.JoinStats
}

// joinNDV is the counts the copy's join check is given for the next
// statement, nil for no check: only on a connection that can send a refused
// statement to its source.
func (s *session) joinNDV() map[string]float64 {
	js := s.joinStats()
	if js == nil || !s.canFallBack() {
		return nil
	}
	return js.current()
}

// noteJoinCheck logs what the copy's join check did with a statement.
func (s *session) noteJoinCheck(res sqlsandbox.Result, err error) {
	var tooBig *sqlsandbox.JoinTooBigError
	switch {
	case errors.As(err, &tooBig):
		s.logger.Debug("pgshim: the copy's joins of a statement would explode in either order; it did not run there", "why", tooBig.Reason)
	case err != nil:
	case res.JoinOrderWritten:
		s.logger.Debug("pgshim: the copy ran a statement with its joins in the order written: in the copy's own order one was estimated far past its inputs")
	case res.JoinCheckSkipped != "":
		s.logger.Debug("pgshim: the copy's join check could not be made; the statement ran as planned", "why", res.JoinCheckSkipped)
	}
}

// readJoinStats reads the server's statistics on up, the connection to the
// source a statement's plan was just asked on, when they are due. Outside a
// block only: an error there would end the block.
func (s *session) readJoinStats(up *upstream) {
	js := s.joinStats()
	if js == nil || up == nil || up.inTx || !js.begin() {
		return
	}
	cell, perr, lost := s.cellOnSource(up, joinStatsSQL)
	switch {
	case lost != nil:
		js.done(nil, errors.New(lost.why))
		s.loseSource(lost.why)
		return
	case perr != nil:
		js.done(nil, errors.New(perr.code+": "+perr.msg))
		s.logger.Warn("pgshim: the source's column statistics could not be read; the copy runs statements without its join check until they can",
			"code", perr.code, "err", perr.msg)
		return
	}
	ndv, err := parseJoinStats(cell)
	js.done(ndv, err)
	if err != nil {
		s.logger.Warn("pgshim: the source's column statistics could not be read; the copy runs statements without its join check until they can", "err", err)
	}
}
