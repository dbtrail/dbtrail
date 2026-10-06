package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/query"
)

// ReadWindow is the read a binlog-renumbering check guards (#2174), for the
// readers that start at a snapshot's position: verify and the shim's
// _snapshot. The zero value is the refresh's check: the whole index, up to
// now.
type ReadWindow struct {
	// Schema and Table name the one table the read returns.
	Schema, Table string
	// Since is the time the read starts from (the snapshot's time, or a
	// verify pair's previous snapshot); the read reaches further back than
	// it (query.PositionReadFloor), and so does the check. It is also the
	// snapshot's wall time a move to another server is counted from.
	Since time.Time
	// Until is where the read stops (AS OF, the newer snapshot of a verify
	// pair). Set together with Schema and Table, the check asks exactly what
	// the read could miss:
	//   - is there a change of Schema.Table indexed after the mark, recorded
	//     between the read's floor and Until, that sorts before the mark in
	//     the binary log (check). The read drops such a change by position.
	//     A restart after Until, or whose later changes touched only other
	//     tables, leaves this read whole.
	//   - capture moving to another server refuses only when it moved at or
	//     before Until (checkSameServerUntil).
	Until time.Time
	// Notice, when set, receives the check's log lines instead of the default
	// logger. A reader that runs the check per statement passes a NoticeOnce
	// so a lasting condition (a backfilled index, a mark that names nothing)
	// is logged once, not on every statement.
	Notice func(level slog.Level, msg string, args ...any)
}

func (w *ReadWindow) bounded() bool {
	return w != nil && !w.Until.IsZero() && w.Schema != "" && w.Table != ""
}

func (w *ReadWindow) notice(level slog.Level, msg string, args ...any) {
	if w != nil && w.Notice != nil {
		w.Notice(level, msg, args...)
		return
	}
	slog.Log(context.Background(), level, msg, args...)
}

// NoticeOnce remembers which check log lines were written, so a reader that
// runs the check per statement (the shim, one handler per connection) logs a
// lasting condition (a backfilled index, a mark that names nothing) once per
// process instead of on every statement. One per reader, kept for the life
// of the process.
type NoticeOnce struct {
	mu   sync.Mutex
	seen map[string]bool
}

// noticeOnceCap bounds a NoticeOnce: past it, it starts over (a line may
// then be logged again).
const noticeOnceCap = 1024

// To returns a ReadWindow.Notice writing each distinct line (its level,
// message and arguments) once, to logger (nil: the default logger).
func (n *NoticeOnce) To(logger *slog.Logger) func(level slog.Level, msg string, args ...any) {
	return func(level slog.Level, msg string, args ...any) {
		key := fmt.Sprint(append([]any{level, msg}, args...)...)
		n.mu.Lock()
		if n.seen == nil || len(n.seen) >= noticeOnceCap {
			n.seen = map[string]bool{}
		}
		dup := n.seen[key]
		n.seen[key] = true
		n.mu.Unlock()
		if dup {
			return
		}
		l := logger
		if l == nil {
			l = slog.Default()
		}
		l.Log(context.Background(), level, msg, args...)
	}
}

// renumberChecked remembers, per snapshot mark, table and read floor, the
// part of the index a bounded check already found clean: every change with
// an id in (mark, ID] recorded between the floor and Until. Ids only grow,
// so a later statement asks only about changes indexed since, and about the
// stretch of time past the last Until. One per process.
var renumberChecked = struct {
	sync.Mutex
	m map[renumberKey]renumberClean
}{m: map[renumberKey]renumberClean{}}

// renumberCheckedCap bounds the set: past it, it starts over.
const renumberCheckedCap = 4096

type renumberKey struct {
	db            *sql.DB
	mark          string
	schema, table string
	floor         time.Time
}

type renumberClean struct {
	id    uint64
	until time.Time
}

// check is the bounded half of checkNumberingContinues: one question, asked
// of the changes the read can see. See ReadWindow.Until.
func (w *ReadWindow) check(ctx context.Context, db *sql.DB, m *EventMark, anchor query.BinlogPos) error {
	since := w.Since
	floor, err := query.PositionReadFloor(ctx, db, query.Options{Schema: w.Schema, Table: w.Table, Since: &since, SincePos: &anchor})
	if w.Since.IsZero() {
		floor, err = time.Time{}, nil
	}
	if err != nil {
		return fmt.Errorf("find how far back the read of %s.%s reaches: %w", w.Schema, w.Table, err)
	}
	var newest sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MAX(event_id) FROM binlog_events`).Scan(&newest); err != nil {
		return fmt.Errorf("read the newest indexed event: %w", err)
	}
	if !newest.Valid || uint64(newest.Int64) <= m.ID {
		return nil
	}
	hi := uint64(newest.Int64)
	key := renumberKey{db: db, mark: m.Encode(), schema: w.Schema, table: w.Table, floor: floor}
	renumberChecked.Lock()
	clean, ok := renumberChecked.m[key]
	renumberChecked.Unlock()
	if !ok {
		clean = renumberClean{id: m.ID}
	}
	until := w.Until.UTC()
	upTo := until
	if clean.until.After(upTo) {
		upTo = clean.until
	}
	// Changes indexed since the last check, over the whole stretch of time;
	// then, for the changes already checked, the time past the last Until.
	e, found, err := w.firstBelowMark(ctx, db, m, clean.id, hi, floor, false, upTo)
	if err == nil && !found && clean.id > m.ID && upTo.After(clean.until) {
		e, found, err = w.firstBelowMark(ctx, db, m, m.ID, clean.id, clean.until, true, upTo)
	}
	if err != nil {
		return err
	}
	if found && upTo.After(until) {
		// Found past this read's own end (a stretch an earlier, later-ending
		// read added): ask again of this read alone.
		e, found, err = w.firstBelowMark(ctx, db, m, m.ID, hi, floor, false, until)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
	}
	if found {
		return startedOverErr(e, m)
	}
	renumberChecked.Lock()
	if len(renumberChecked.m) >= renumberCheckedCap {
		renumberChecked.m = map[renumberKey]renumberClean{}
	}
	renumberChecked.m[key] = renumberClean{id: hi, until: upTo}
	renumberChecked.Unlock()
	return nil
}

// firstBelowMark finds a change of w's table with an id in (fromID, toID],
// recorded in [from, to] (from excluded when afterFrom; from zero: no lower
// bound), whose end sorts before the mark's in the binary log: earlier by
// file (shorter name first, as buildQuery orders files) or by position, or
// under another base name. FORCE INDEX: the table's changes by time; left
// to itself the optimizer can walk the primary key over every table.
func (w *ReadWindow) firstBelowMark(ctx context.Context, db *sql.DB, m *EventMark, fromID, toID uint64, from time.Time, afterFrom bool, to time.Time) (indexedEvent, bool, error) {
	where := []string{"schema_name = ?", "table_name = ?", "event_id > ?", "event_id <= ?",
		fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", toSeconds(to.Truncate(time.Hour).Add(time.Hour))),
		"event_timestamp <= ?"}
	args := []any{w.Schema, w.Table, fromID, toID, to}
	if !from.IsZero() {
		op := ">="
		if afterFrom {
			op = ">"
		}
		where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", toSeconds(from.Truncate(time.Hour))),
			"event_timestamp "+op+" ?")
		args = append(args, from)
	}
	below := "CHAR_LENGTH(binlog_file) < CHAR_LENGTH(?)" +
		" OR (CHAR_LENGTH(binlog_file) = CHAR_LENGTH(?) AND binlog_file < ?)" +
		" OR (binlog_file = ? AND end_pos < ?)"
	args = append(args, m.File, m.File, m.File, m.File, m.End)
	if base := BinlogBaseName(m.File); base != m.File {
		below += " OR LEFT(binlog_file, CHAR_LENGTH(?) + 1) <> CONCAT(?, '.')"
		args = append(args, base, base)
	}
	where = append(where, "("+below+")")
	var e indexedEvent
	err := db.QueryRowContext(ctx, `SELECT event_id, binlog_file, end_pos FROM binlog_events FORCE INDEX (idx_row_lookup) WHERE `+
		strings.Join(where, " AND ")+` LIMIT 1`, args...).Scan(&e.id, &e.file, &e.end)
	switch {
	case err == nil:
		return e, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return e, false, nil
	}
	return e, false, fmt.Errorf("read the changes of %s.%s indexed after the snapshot's event mark: %w", w.Schema, w.Table, err)
}

// moveMargin widens the search for a move to another server below the
// snapshot's time: that time and a record's come from different clocks.
const moveMargin = 10 * time.Minute

// checkSameServerUntil is CheckSameServer for a read that stops at until:
// capture reading another server than the mark names refuses only when
// capture moved to it at or before until. When it moved is when capture
// recorded it (captureLeftServer), from the snapshot on; no record refuses.
func checkSameServerUntil(ctx context.Context, db *sql.DB, m *EventMark, since, until time.Time) error {
	if m == nil || m.ServerUUID == "" {
		return nil
	}
	now, err := captureServerUUID(ctx, db)
	if err != nil || now == "" || now == m.ServerUUID {
		return err
	}
	left, ok, err := captureLeftServer(ctx, db, m.ServerUUID, since.Add(-moveMargin))
	if err != nil {
		return err
	}
	if ok && left.After(until) {
		return nil
	}
	return anotherServerErr(now, m)
}

// captureLeftServer returns when capture first recorded, at or after from
// (zero: ever), reading another server than uuid, from the two records
// capture keeps:
//   - bintrail_server_changes: the same address answered with another
//     server_uuid (the server record is updated in place);
//   - bintrail_servers: a record created after uuid's own and at or before
//     the one capture reads now (another address, or a server capture had
//     not seen). A record created after the one capture reads now is not on
//     the way to it and must not answer; and when that one is older than
//     from, capture returned to a server known before and nothing says when.
//
// Moves before from (a failover and a failback before the snapshot was
// taken, while its mark names the server) do not count. The earliest of the
// two; ok false when neither says.
func captureLeftServer(ctx context.Context, db *sql.DB, uuid string, from time.Time) (time.Time, bool, error) {
	fromUnix := int64(0)
	if !from.IsZero() {
		fromUnix = from.Unix()
	}
	var first time.Time
	for _, q := range []string{
		// UNIX_TIMESTAMP of a TIMESTAMP column: an instant, whatever the
		// session's time zone.
		`SELECT UNIX_TIMESTAMP(MIN(detected_at)) FROM bintrail_server_changes
			WHERE field_changed = 'server_uuid' AND old_value = ? AND UNIX_TIMESTAMP(detected_at) >= ?`,
		`SELECT UNIX_TIMESTAMP(MIN(s.created_at)) FROM bintrail_servers s
			JOIN bintrail_servers o ON o.server_uuid = ?
			JOIN stream_state st ON st.id = 1
			JOIN bintrail_servers cur ON cur.bintrail_id = st.bintrail_id
			WHERE s.server_uuid <> o.server_uuid AND s.created_at >= o.created_at
				AND s.created_at <= cur.created_at AND UNIX_TIMESTAMP(s.created_at) >= ?`,
	} {
		var at sql.NullString
		err := db.QueryRowContext(ctx, q, uuid, fromUnix).Scan(&at)
		if err != nil {
			var me *mysqldriver.MySQLError
			if errors.As(err, &me) && me.Number == 1146 {
				continue
			}
			return time.Time{}, false, fmt.Errorf("read when capture moved to another server: %w", err)
		}
		if !at.Valid {
			continue
		}
		t, err := parseUnixSeconds(at.String)
		if err != nil {
			return time.Time{}, false, err
		}
		if first.IsZero() || t.Before(first) {
			first = t
		}
	}
	return first, !first.IsZero(), nil
}

// parseUnixSeconds reads UNIX_TIMESTAMP's text ("1759658400" or, for a
// fractional column, "1759658400.000000").
func parseUnixSeconds(s string) (time.Time, error) {
	whole, _, _ := strings.Cut(s, ".")
	var sec int64
	if _, err := fmt.Sscan(whole, &sec); err != nil {
		return time.Time{}, fmt.Errorf("read when capture moved to another server: %q: %w", s, err)
	}
	return time.Unix(sec, 0).UTC(), nil
}

// liveHoldsRead reports whether binlog_events (dbName) still holds every
// change the read described by opts can see: its oldest partition starts at
// or before the read's floor (query.PositionReadFloor), so rotation, which
// drops the oldest hours, has taken nothing the read reaches. An
// unpartitioned table, or one with only the p_future catch-all, never
// rotated. A partition name this build cannot place, or no database name:
// false, the answer that keeps the whole-index check.
func liveHoldsRead(ctx context.Context, db *sql.DB, dbName string, opts query.Options) (bool, error) {
	if dbName == "" {
		return false, nil
	}
	floor, err := query.PositionReadFloor(ctx, db, opts)
	if err != nil {
		return false, fmt.Errorf("find how far back the read of %s.%s reaches: %w", opts.Schema, opts.Table, err)
	}
	var name string
	err = db.QueryRowContext(ctx, `SELECT PARTITION_NAME FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND PARTITION_NAME IS NOT NULL
		ORDER BY PARTITION_ORDINAL_POSITION LIMIT 1`, dbName).Scan(&name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("read binlog_events' oldest partition: %w", err)
	case name == "p_future":
		return true, nil
	}
	oldest, ok := query.ParsePartitionName(name)
	if !ok || floor.IsZero() {
		return false, nil
	}
	return !floor.Before(oldest), nil
}
