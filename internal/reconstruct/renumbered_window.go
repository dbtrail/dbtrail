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
)

// ReadWindow is the read a binlog-renumbering check guards (#2174), for the
// readers that start at a snapshot's position: verify and the shim's
// _snapshot. The zero value is the refresh's check: the whole index, up to
// now.
type ReadWindow struct {
	// Schema and Table name the one table the read returns.
	Schema, Table string
	// Until is where the read stops (AS OF, the newer snapshot of a verify
	// pair). Set together with Schema and Table, the check looks only at
	// what this read can see:
	//   - the probes for a numbering that started over read only Schema.Table's
	//     changes recorded at or before Until. A restart after Until, or one
	//     whose later changes touched only other tables, leaves this read in
	//     one numbering. It also keeps the probes on idx_row_lookup: probing the
	//     whole index backward from Until walks every change recorded after it.
	//   - capture moving to another server refuses only when it moved at or
	//     before Until (checkSameServerUntil).
	Until time.Time
	// Notice, when set, receives the check's log lines instead of the default
	// logger. A reader that runs the check per statement passes a NoticeOnce so
	// a lasting condition (a backfilled index, a mark that names nothing) is
	// logged once, not on every statement.
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

// To returns a ReadWindow.Notice writing each distinct line (its level,
// message and arguments) once, to logger (nil: the default logger).
func (n *NoticeOnce) To(logger *slog.Logger) func(level slog.Level, msg string, args ...any) {
	return func(level slog.Level, msg string, args ...any) {
		key := fmt.Sprint(append([]any{level, msg}, args...)...)
		n.mu.Lock()
		if n.seen == nil {
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

// markSlack widens the probes below the mark's own recorded time: a change
// committed after the mark can carry an earlier statement time.
const markSlack = time.Minute

// toSecondsOffset is TO_SECONDS('1970-01-01 00:00:00').
const toSecondsOffset = 62167219200

// probes are the first and the last change of w's table indexed after the
// mark, among those recorded in [mark's time - markSlack, Until], in recorded
// time order. markAt zero (the mark's row is gone with everything older):
// no lower bound, everything left is after the mark.
func (w *ReadWindow) probes(m *EventMark, markAt time.Time) ([]string, []any) {
	until := w.Until.UTC()
	where := `schema_name = ? AND table_name = ? AND event_id > ?
			AND binlog_file IS NOT NULL AND end_pos IS NOT NULL
			AND TO_SECONDS(event_timestamp) < ` + fmt.Sprint(until.Truncate(time.Hour).Add(time.Hour).Unix()+toSecondsOffset) + `
			AND event_timestamp <= ?`
	args := []any{w.Schema, w.Table, m.ID, until}
	if !markAt.IsZero() {
		where += ` AND event_timestamp >= ?`
		args = append(args, markAt.Add(-markSlack))
	}
	q := `SELECT event_id, binlog_file, end_pos FROM binlog_events WHERE ` + where + ` ORDER BY event_timestamp %[1]s, event_id %[1]s LIMIT 1`
	return []string{fmt.Sprintf(q, "ASC"), fmt.Sprintf(q, "DESC")}, args
}

// checkSameServerUntil is CheckSameServer for a read that stops at until:
// capture reading another server than the mark names refuses only when
// capture moved to it at or before until. When it moved is when capture
// recorded it (captureLeftServer); no record refuses.
func checkSameServerUntil(ctx context.Context, db *sql.DB, m *EventMark, until time.Time) error {
	if m == nil || m.ServerUUID == "" {
		return nil
	}
	now, err := captureServerUUID(ctx, db)
	if err != nil || now == "" || now == m.ServerUUID {
		return err
	}
	left, ok, err := captureLeftServer(ctx, db, m.ServerUUID)
	if err != nil {
		return err
	}
	if ok && left.After(until) {
		return nil
	}
	return anotherServerErr(now, m)
}

// captureLeftServer returns when capture first recorded reading another
// server than uuid, from the two records capture keeps:
//   - bintrail_server_changes: the same address answered with another
//     server_uuid (the server record is updated in place);
//   - bintrail_servers: a record created after uuid's own (another address,
//     or a server capture had not seen).
//
// The earliest of the two. ok false when neither says.
func captureLeftServer(ctx context.Context, db *sql.DB, uuid string) (time.Time, bool, error) {
	var first time.Time
	for _, q := range []string{
		// UNIX_TIMESTAMP of a TIMESTAMP column: an instant, whatever the
		// session's time zone.
		`SELECT UNIX_TIMESTAMP(MIN(detected_at)) FROM bintrail_server_changes
			WHERE field_changed = 'server_uuid' AND old_value = ?`,
		`SELECT UNIX_TIMESTAMP(MIN(s.created_at)) FROM bintrail_servers s
			JOIN bintrail_servers o ON o.server_uuid = ?
			WHERE s.server_uuid <> o.server_uuid AND s.created_at >= o.created_at`,
	} {
		var at sql.NullString
		err := db.QueryRowContext(ctx, q, uuid).Scan(&at)
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
