package mydumperlock

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// How an ftwrl full read waits for a quiet source (#2530).
//
// FLUSH TABLES WITH READ LOCK has to wait for every statement already running
// on the server, and once it has asked, writes to the tables those statements
// read stop until they end. Cancelling the request does not release them:
// measured on MySQL 8.0.46 with the pinned mydumper, a write still waited the
// 40 s of the blocker after mydumper and all its connections were killed at
// 5 s, because once a flush has started, new statements on a table in use wait
// for the statement using it. So the only protection is not asking while one
// runs.
//
// mydumper has a check of its own (--long-query-guard), but it is not used for
// this: it counts only Command "Query", so a prepared statement (Command
// "Execute": the Go driver, mysqli, Connector/J with server-side prepare) is
// never waited for, and it runs after the moment the caller stamps as the
// snapshot's instant, so a long wait there would label the snapshot minutes
// before its data. This wait runs before that stamp; mydumper keeps its own
// defaults (60 s, no retries) behind it.
const (
	// QuietThreshold: a statement that has run this long or more holds the
	// read back. The process list counts whole seconds.
	QuietThreshold = 1
	// QuietInterval is how often it looks again, QuietWaitLimit how long in
	// all before it refuses.
	QuietInterval  = 10 * time.Second
	QuietWaitLimit = 10 * time.Minute
)

// Busy is a statement the wait saw running: who and for how long, never its
// text, which can carry the application's data.
type Busy struct {
	ID      int64
	User    string
	Command string
	Seconds int64
}

// QuietWaitError is the refusal once QuietWaitLimit passed with the source
// never quiet: nothing was locked and nothing was read.
type QuietWaitError struct {
	Waited time.Duration
	Busy   []Busy // what the last look saw, at most three
	remedy Remedy
}

func (e *QuietWaitError) Error() string {
	seen := make([]string, 0, len(e.Busy))
	for _, b := range e.Busy {
		secs := strconv.FormatInt(b.Seconds, 10) + " s"
		if b.Seconds < 0 {
			secs = "an unreadable time"
		}
		seen = append(seen, fmt.Sprintf("connection %d of %s, %s for %s", b.ID, b.User, b.Command, secs))
	}
	return fmt.Sprintf("full read not started: for %s the source always had a statement running for %d s or more (last seen: %s), "+
		"and taking the ftwrl lock then stops writes to the tables that statement reads until it ends; nothing was locked and "+
		"nothing was read. Start it again when no long query or ALTER TABLE runs on the source, or %s: a running query does "+
		"not hold that mode up", waitWords(e.Waited), QuietThreshold, strings.Join(seen, "; "), e.remedy.forMode(baseline.LockModeLockAll))
}

// Summary is the refusal in the few words a status card shows.
func (e *QuietWaitError) Summary() string {
	return fmt.Sprintf("Not started: for %s the source always had a query running for %d s or more, and the lock would have "+
		"stopped writes behind it. Nothing was locked. Try when it is quiet, or %s.",
		waitWords(e.Waited), QuietThreshold, e.remedy.forMode(baseline.LockModeLockAll))
}

func waitWords(d time.Duration) string {
	if d >= time.Minute && d%time.Minute == 0 {
		if d == time.Minute {
			return "1 minute"
		}
		return strconv.Itoa(int(d/time.Minute)) + " minutes"
	}
	return d.String()
}

// WaitForQuietSource holds an ftwrl full read until no statement on the source
// has run for QuietThreshold or more, looking every QuietInterval, and refuses
// with a *QuietWaitError after QuietWaitLimit. Any other mode returns at once:
// LOCK_ALL is not held up by a running SELECT (#1985), the lock-free modes
// take no lock. An unknown mode sends FTWRL (MydumperValue) and waits like it.
//
// Call it as the LAST thing before stamping the snapshot's instant: the
// instant has to come after the wait.
//
// It returns how long it waited: zero when the source was quiet at the first
// look, which lets a caller keep a stamp it took just before.
func WaitForQuietSource(ctx context.Context, db *sql.DB, mode baseline.LockMode, remedy Remedy) (time.Duration, error) {
	w := &quietWait{interval: QuietInterval, total: QuietWaitLimit, sleep: sleepCtx}
	err := w.run(ctx, db, mode, remedy)
	return w.waited, err
}

type quietWait struct {
	interval, total time.Duration
	sleep           func(context.Context, time.Duration) error
	slept           time.Duration // what a test's sleep recorded
	waited          time.Duration // how long run waited
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (w *quietWait) run(ctx context.Context, db *sql.DB, mode baseline.LockMode, remedy Remedy) error {
	if mode.MydumperValue() != "FTWRL" {
		return nil
	}
	// A process list that cannot be read lets the read go ahead, loudly. The
	// account is the one mydumper connects with, so a failure here is one
	// mydumper meets and reports too, and refusing every full read on it
	// would leave the copy with no base at all.
	cantCheck := func(err error) error {
		if ctx.Err() != nil {
			// A shutdown, not a source that cannot be read: say that, and
			// start nothing.
			return ctx.Err()
		}
		slog.Warn("full read: could not check the source for long-running statements before taking the ftwrl lock; "+
			"going ahead, and if one is running, writes to the tables it reads stop until it ends", "error", err)
		return nil
	}
	// One pinned connection, so the id excluded below is the one asking.
	conn, err := db.Conn(ctx)
	if err != nil {
		return cantCheck(err)
	}
	defer conn.Close()
	var own int64
	var version string
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID(), VERSION()").Scan(&own, &version); err != nil {
		return cantCheck(err)
	}
	mariadb := strings.Contains(strings.ToLower(version), "mariadb")
	// The connection sits idle between looks; a server whose wait_timeout is
	// shorter than the interval would close it, and the next look would fail
	// and go ahead right after seeing something run. Raised for this session
	// only, best-effort.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION wait_timeout = %d", int((w.total+w.interval)/time.Second)+60)); err != nil {
		slog.Debug("full read: could not raise wait_timeout for the quiet check's connection", "error", err)
	}

	var waited time.Duration
	for {
		busy, err := busyStatements(ctx, conn, own, mariadb)
		if err != nil {
			return cantCheck(err)
		}
		if len(busy) == 0 {
			if waited > 0 {
				slog.Info("full read: the source is quiet, taking the ftwrl lock", "waited", waited)
			}
			return nil
		}
		if waited+w.interval > w.total {
			return &QuietWaitError{Waited: waited, Busy: busy, remedy: remedy}
		}
		if waited == 0 {
			slog.Info("full read: waiting for the source to have no statement running for 1 s or more before taking the "+
				"ftwrl lock, so the lock does not stop writes behind one", "running", len(busy),
				"longest_s", busy[0].Seconds, "check_every", w.interval, "up_to", w.total)
		}
		if err := w.sleep(ctx, w.interval); err != nil {
			return err
		}
		waited += w.interval
		w.waited = waited
	}
}

// busyStatements reads the process list and returns the statements that hold
// an ftwrl read back, longest first, at most three. Counted: Command "Query",
// "Execute" (a prepared statement) or "Killed" (a statement killed but still
// rolling back, with its tables still open), running QuietThreshold or more,
// not this connection, not "system user" (a replica's own threads). Idle
// connections, a binlog dump (DBTrail's own capture among them) and daemons
// never count. The whole server, not the dumped schemas: FTWRL flushes every
// table. On MariaDB, whose FTWRL does not wait for a running SELECT, a
// statement that only reads (isReadOnly) does not count; a statement whose
// text this account cannot see counts, since it cannot be told apart. A row
// whose Id or Time does not parse counts too: a row that cannot be read is no
// evidence the source is quiet.
func busyStatements(ctx context.Context, conn *sql.Conn, own int64, mariadb bool) ([]Busy, error) {
	rows, err := conn.QueryContext(ctx, "SHOW FULL PROCESSLIST")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	idx := map[string]int{}
	for i, c := range cols {
		idx[strings.ToLower(c)] = i
	}
	for _, c := range []string{"id", "user", "command", "time", "info"} {
		if _, ok := idx[c]; !ok {
			return nil, fmt.Errorf("SHOW FULL PROCESSLIST has no %s column (columns: %s)", c, strings.Join(cols, ", "))
		}
	}
	var busy []Busy
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		cmd := vals[idx["command"]].String
		if cmd != "Query" && cmd != "Execute" && cmd != "Killed" {
			continue
		}
		user := vals[idx["user"]].String
		if user == "system user" {
			continue
		}
		id, idErr := strconv.ParseInt(vals[idx["id"]].String, 10, 64)
		if idErr == nil && id == own {
			continue
		}
		secs, timeErr := strconv.ParseInt(vals[idx["time"]].String, 10, 64)
		if timeErr == nil && secs < QuietThreshold {
			continue
		}
		if info := vals[idx["info"]]; mariadb && info.Valid && isReadOnly(info.String) {
			continue
		}
		if idErr != nil {
			id = -1
		}
		if timeErr != nil {
			secs = -1 // shown as unknown; sorts last
		}
		b := Busy{ID: id, User: user, Command: cmd, Seconds: secs}
		i := 0
		for i < len(busy) && busy[i].Seconds >= secs {
			i++
		}
		busy = append(busy[:i], append([]Busy{b}, busy[i:]...)...)
		if len(busy) > 3 {
			busy = busy[:3]
		}
	}
	return busy, rows.Err()
}

// isReadOnly reports whether a statement's text starts, past leading blanks
// and comments, with a word that only reads. On MariaDB those are the ones
// FTWRL does not wait for (measured with a SELECT, docs/snapshot-lock-
// measurements.md); a write or DDL makes it wait while blocking every other
// write on the server, so anything else counts.
func isReadOnly(text string) bool {
	switch firstWord(text) {
	case "SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "DO":
		return true
	}
	return false
}

// firstWord is a statement's first word, upper-cased, past leading blanks and
// comments (/* */, "-- " and "#" to the end of the line); "" when there is
// none or a comment never closes.
func firstWord(text string) string {
	s := text
	for {
		s = strings.TrimSpace(s)
		switch {
		case strings.HasPrefix(s, "/*"):
			end := strings.Index(s, "*/")
			if end < 0 {
				return ""
			}
			s = s[end+2:]
			continue
		case strings.HasPrefix(s, "-- "), strings.HasPrefix(s, "--\t"), strings.HasPrefix(s, "#"):
			_, rest, ok := strings.Cut(s, "\n")
			if !ok {
				return ""
			}
			s = rest
			continue
		}
		break
	}
	end := strings.IndexFunc(s, func(r rune) bool { return unicode.IsSpace(r) || r == '(' || r == '/' || r == ';' })
	if end >= 0 {
		s = s[:end]
	}
	return strings.ToUpper(s)
}
