package pgshim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The copy's join check (sqlsandbox.Session.JoinNDV, internal/sqlsandbox
// joinguard.go) needs what the copy does not have: how many distinct values
// each column holds. The source has it, in pg_stats. The host reads it there
// (JoinStats.Read) on a connection of its own, as the forwarding account,
// never inside a client's statement or its statement_timeout: a statement
// that reaches the copy when the server's counts are due starts the read in
// the background, and is itself given the counts the server has now. Once
// for the server every joinStatsFresh, across all of its connections. Every
// statement the copy then runs on a connection that can send it to the
// source carries the counts of the tables it reads (RunOptions.JoinNDV); one
// the copy refuses for its joins goes there (ReasonJoinTooBig), before the
// copy has run anything.
//
// Without a forwarding account there is nowhere to send a refused statement
// and nothing is read: the copy runs every statement as before. The capture
// account (Forward.CaptureAccount, #3054) is not used for it either: that
// account sends the source only what passes its gate. A read that fails, or
// that finds no statistics, is said once (a WARN per server and cause), tried
// again later and later, up to joinStatsRetryMax, and every statement that
// runs without the check meanwhile is counted on /status (RouteNote.JoinCheck).

// JoinStatsSQL reads the source's count of distinct values of every column
// of its own tables, as one JSON object: {"schema.table": {"column": n}},
// names lowercased. n_distinct is a count when positive and minus a fraction
// of the table's rows when negative (-1: unique); neither, or a table never
// analyzed (reltuples -1), is no count. Each partition of a partitioned table
// is a table here, under its own name, so a statement that names the parent
// finds no counts and is not checked. pg_stats lists only the columns the
// account that reads it may read.
const JoinStatsSQL = `SELECT coalesce(json_object_agg(tbl, cols), '{}')::text FROM (
  SELECT tbl, json_object_agg(col, ndv) AS cols FROM (
    SELECT lower(s.schemaname) || '.' || lower(s.tablename) AS tbl, lower(s.attname) AS col,
           max(CASE WHEN s.n_distinct > 0 THEN s.n_distinct::float8
                    WHEN s.n_distinct < 0 AND c.reltuples > 0 THEN -s.n_distinct::float8 * c.reltuples::float8 END) AS ndv
    FROM pg_catalog.pg_stats s
    JOIN pg_catalog.pg_namespace n ON n.nspname = s.schemaname
    JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = s.tablename
    WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema') AND s.schemaname NOT LIKE 'pg\_toast%'
    GROUP BY 1, 2) a
  WHERE ndv IS NOT NULL
  GROUP BY tbl) b`

// JoinTables is a server's counts: by "schema.table", then by column, both
// lowercased.
type JoinTables map[string]map[string]float64

// How long a server's statistics serve. Read again past joinStatsFresh;
// used, while no newer read succeeds, until joinStatsKept; after a failed or
// empty read, not tried again before joinStatsRetry, doubled for each failure
// in a row up to joinStatsRetryMax. n_distinct moves with ANALYZE, which
// autovacuum runs as a table changes by a share of its rows: minutes, not
// seconds. joinStatsReadTimeout bounds one read.
const (
	joinStatsFresh       = 10 * time.Minute
	joinStatsKept        = time.Hour
	joinStatsRetry       = time.Minute
	joinStatsRetryMax    = 10 * time.Minute
	joinStatsReadTimeout = 30 * time.Second
)

// The outcomes of the join check a statement on the copy is counted under
// (RouteNote.JoinCheck), on a connection that can send it to its source. A
// statement that reads one table once is not checked and not counted; one
// the check refused is counted by its reason, ReasonJoinTooBig.
const (
	JoinCheckPlanned      = "planned"       // checked: run in the copy's own join order
	JoinCheckWrittenOrder = "written_order" // checked: run in the order written
	JoinCheckNoStatistics = "no_statistics" // not checked: the server has no counts now (not read yet, failed, none readable, too old)
	JoinCheckCouldNotRun  = "could_not_run" // not checked: the copy's plan could not be had or read
)

// JoinStats is a server's column statistics for the copy's join check,
// shared by all of its connections (Forward.JoinStats).
type JoinStats struct {
	// Read reads the counts from the source, on a connection of its own
	// (JoinStatsSQL, ParseJoinStats). Nil: never read, no check.
	Read func(ctx context.Context) (JoinTables, error)
	// Server names the server in the log; Logger is where it is said, the
	// default logger when nil.
	Server string
	Logger *slog.Logger

	mu sync.Mutex
	// tables is the last good read's counts, read at readAt; tried is when a
	// read last began, reading whether one is under way, failures how many
	// in a row failed or found none.
	tables   JoinTables
	readAt   time.Time
	tried    time.Time
	reading  bool
	failures int
	// warned holds the causes said since the last good read.
	warned map[string]bool
	// now is the clock, time.Now when nil; spawn starts a read, go when
	// nil (a test's own).
	now   func() time.Time
	spawn func(func())
}

func (j *JoinStats) clock() time.Time {
	if j.now != nil {
		return j.now()
	}
	return time.Now()
}

func (j *JoinStats) logger() *slog.Logger {
	if j.Logger != nil {
		return j.Logger
	}
	return slog.Default()
}

// current is the counts the check uses now, nil for none: never read, the
// last good read is older than joinStatsKept, or none was ever found. When
// they are due it starts a read in the background (refresh), which a later
// statement uses. Shared and never written after it is stored: the caller
// must not change it.
func (j *JoinStats) current() JoinTables {
	tables, start := j.currentLocked()
	if start {
		// Outside the lock: the read takes it when it ends.
		spawn := j.spawn
		if spawn == nil {
			spawn = func(f func()) { go f() }
		}
		spawn(j.refresh)
	}
	return tables
}

// currentLocked is current's decision: the counts, and whether a read is to
// start (marked under way already).
func (j *JoinStats) currentLocked() (JoinTables, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	now := j.clock()
	start := j.Read != nil && j.dueLocked(now)
	if start {
		j.reading, j.tried = true, now
	}
	if j.tables == nil || now.Sub(j.readAt) > joinStatsKept {
		if j.tables != nil {
			j.warnLocked("too_old", "the source's column statistics are more than an hour old and no newer read succeeded; the copy runs statements without its join check until one does")
		}
		return nil, start
	}
	return j.tables, start
}

// dueLocked says whether a read should begin now: none under way, the last
// good one past joinStatsFresh (or none), and the last failed one past its
// wait.
func (j *JoinStats) dueLocked(now time.Time) bool {
	switch {
	case j.reading:
		return false
	case !j.readAt.IsZero() && now.Sub(j.readAt) < joinStatsFresh:
		return false
	case j.failures > 0 && now.Sub(j.tried) < retryAfter(j.failures):
		return false
	}
	return true
}

// retryAfter is how long after the n-th failed read in a row the next one
// waits.
func retryAfter(n int) time.Duration {
	d := joinStatsRetry
	for i := 1; i < n && d < joinStatsRetryMax; i++ {
		d *= 2
	}
	return min(d, joinStatsRetryMax)
}

// refresh reads the counts and keeps them. A failed read, or one that finds
// none, keeps the last good counts and waits longer before the next.
func (j *JoinStats) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), joinStatsReadTimeout)
	defer cancel()
	tables, err := j.Read(ctx)
	j.mu.Lock()
	defer j.mu.Unlock()
	j.reading = false
	switch {
	case err != nil:
		j.failures++
		j.warnLocked("read_failed", "the source's column statistics could not be read; the copy runs statements without its join check until they can", "err", err, "next_try_in", retryAfter(j.failures).String())
	case len(tables) == 0:
		j.failures++
		j.warnLocked("none", "the source has no column statistics the forwarding account can read (tables never analyzed, or columns it may not read); the copy runs statements without its join check until it has", "next_try_in", retryAfter(j.failures).String())
	default:
		if j.failures > 0 || len(j.warned) > 0 {
			j.logger().Info("pgshim: the source's column statistics were read; the copy's join check is on", "server", j.Server)
		}
		j.tables, j.readAt, j.failures, j.warned = tables, j.clock(), 0, nil
	}
}

// warnLocked says msg once per cause until the next good read.
func (j *JoinStats) warnLocked(cause, msg string, args ...any) {
	if j.warned[cause] {
		return
	}
	if j.warned == nil {
		j.warned = map[string]bool{}
	}
	j.warned[cause] = true
	j.logger().Warn("pgshim: "+msg, append([]any{"server", j.Server, "cause", cause}, args...)...)
}

// warn is warnLocked for a caller that does not hold the lock.
func (j *JoinStats) warn(cause, msg string, args ...any) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.warnLocked(cause, msg, args...)
}

// ParseJoinStats reads JoinStatsSQL's cell. A count that is not a positive
// number is left out, a table left with none too; none at all is nil.
func ParseJoinStats(cell []byte) (JoinTables, error) {
	var raw map[string]map[string]float64
	if err := json.Unmarshal(cell, &raw); err != nil {
		return nil, fmt.Errorf("the source's column statistics could not be read: %w", err)
	}
	var out JoinTables
	for table, cols := range raw {
		for name, n := range cols {
			if n <= 0 || math.IsNaN(n) || math.IsInf(n, 0) {
				continue
			}
			if out == nil {
				out = JoinTables{}
			}
			t := strings.ToLower(table)
			if out[t] == nil {
				out[t] = map[string]float64{}
			}
			out[t][strings.ToLower(name)] = n
		}
	}
	return out, nil
}

// joinNDVOf is the counts of the tables names says a statement reads, by
// column: a column whose name two of them hold is left out (the copy's plan
// names columns without their table). names are as readsOf gives them, one
// to two parts, the schema optional and matched in every schema; all is
// true when the statement could not be read, and every table counts.
func (t JoinTables) joinNDVOf(names [][2]string, all bool) map[string]float64 {
	owner := map[string]string{}
	out := map[string]float64{}
	add := func(table string) {
		for col, n := range t[table] {
			switch o, seen := owner[col]; {
			case !seen:
				owner[col], out[col] = table, n
			case o != table:
				delete(out, col)
				owner[col] = "\x00"
			}
		}
	}
	if all {
		for table := range t {
			add(table)
		}
	} else {
		for table := range t {
			schema, name, _ := strings.Cut(table, ".")
			for _, n := range names {
				if strings.EqualFold(n[1], name) && (n[0] == "" || strings.EqualFold(n[0], schema)) {
					add(table)
					break
				}
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// joinStats is the server's statistics this connection uses, nil when it
// has none: no forwarding account, the capture account, or a host that
// keeps none.
func (s *session) joinStats() *JoinStats {
	if s.bound == nil || s.bound.Forward == nil || s.bound.Forward.CaptureAccount {
		return nil
	}
	return s.bound.Forward.JoinStats
}

// joinNDVFor is the counts the copy's join check is given for text, the
// statement about to run on the copy, and the outcome to count when it runs
// without one ("" when it is not meant to have one). Only on a connection
// that can send a refused statement to its source. Not for a statement that
// cannot join two inputs: one whose plan on the source reads one table and
// joins nothing (the fast lane), or whose text reads one table once.
func (s *session) joinNDVFor(text string) (map[string]float64, string) {
	js := s.joinStats()
	if js == nil || !s.canFallBack() || s.fastLane {
		return nil, ""
	}
	var names [][2]string
	reads, why := readsOf(text, copyFuncs)
	all := why != ""
	if !all {
		for _, ref := range reads.refs {
			if reads.isCTE(ref) {
				continue
			}
			switch len(ref.parts) {
			case 1:
				names = append(names, [2]string{"", ref.parts[0].text})
			case 2:
				names = append(names, [2]string{ref.parts[0].text, ref.parts[1].text})
			default:
				all = true
			}
		}
		if !all && len(names) < 2 {
			return nil, ""
		}
	}
	tables := js.current()
	if tables == nil {
		return nil, JoinCheckNoStatistics
	}
	ndv := tables.joinNDVOf(names, all)
	if ndv == nil {
		return nil, JoinCheckNoStatistics
	}
	return ndv, ""
}

// noteJoinCheck logs what the copy's join check did with a statement, and
// returns the outcome to count: want is what joinNDVFor said before it ran.
func (s *session) noteJoinCheck(res sqlsandbox.Result, err error, asked bool, want string) string {
	var tooBig *sqlsandbox.JoinTooBigError
	switch {
	case !asked:
		return want
	case errors.As(err, &tooBig):
		s.logger.Debug("pgshim: the copy's joins of a statement would explode in either order; it did not run there", "why", tooBig.Reason)
		return ""
	case err != nil:
		return ""
	case res.JoinOrderWritten:
		s.logger.Debug("pgshim: the copy ran a statement with its joins in the order written: in the copy's own order one was estimated far past its inputs")
		return JoinCheckWrittenOrder
	case res.JoinCheckSkipped != "":
		if js := s.joinStats(); js != nil {
			js.warn("could_not_run", "the copy's join check could not be made for a statement; it ran as planned (said once per server until the next statistics read)", "why", res.JoinCheckSkipped)
		}
		return JoinCheckCouldNotRun
	}
	return JoinCheckPlanned
}
