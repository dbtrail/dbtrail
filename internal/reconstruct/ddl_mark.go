package reconstruct

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// The DDL mark (#1912).
//
// schema_changes.id is AUTO_INCREMENT, so it says in which ORDER rows reached
// the index, which neither detected_at (when the statement ran) nor the binlog
// position (where the source wrote it) says. A snapshot records the newest row
// that was in the index when its check ran, or when its dump started. The
// next check then knows every row up to it was already accounted for:
//
//   - a fold's check ran over every one of them, placing them by time and by
//     position against the fold's own base. One it let through was before
//     that base by both, or after the fold's target by time and outside its
//     cut, and a row after the target is found by time from the next snapshot.
//   - a dump started after the statement was indexed, so after it ran on the
//     source: the dump holds its effect.
//
// So a row at or below the mark is never placed by POSITION alone. Placed by
// time it still counts, as it always did. That is what keeps an old statement
// from another numbering of the same binlog name (a failover to a server whose
// files are also called binlog.N, a RESET MASTER) from refusing every run for
// good, while a statement indexed late, after the mark, still refuses.
//
// The premise is the one ResolveSnapshotCut and the event-id floor make:
// ascending id is the order rows became visible, which holds for one writer. A
// fold stamps a mark only on an index a stream wrote. What it does not hold
// for: two writers on one index (two sources, `bintrail index` beside a
// stream), where a lower id can commit after a higher one was read; and a DDL
// row whose INSERT timed out and committed later on the server, after a newer
// row. In both a late row can sit at or below a mark. Neither is a supported
// input for a snapshot chain.

// DDLMark names one schema_changes row: its id and what the row holds, so a
// mark read against an index whose ids now name other rows (restore-index,
// the table dropped and created again) can be told apart and ignored.
type DDLMark struct {
	ID         uint64    `json:"id"`
	File       string    `json:"binlog_file"`
	Pos        uint64    `json:"binlog_pos"`
	DetectedAt time.Time `json:"detected_at"`
	Type       string    `json:"ddl_type"`
}

// Encode is the mark as baseline.MetaKeyDDLMark stores it.
func (m DDLMark) Encode() string {
	b, _ := json.Marshal(m) // a struct of strings, numbers and a time: cannot fail
	return string(b)
}

// ParseDDLMark reads a footer's mark. "" is no mark; one that does not read is
// no mark too, said out loud: no mark places more rows, never fewer.
func ParseDDLMark(raw string) *DDLMark {
	if raw == "" {
		return nil
	}
	var m DDLMark
	if err := json.Unmarshal([]byte(raw), &m); err != nil || m.ID == 0 {
		slog.Warn("baseline footer: unreadable DDL mark; statements are placed by binlog position without it",
			"value", raw, "error", err)
		return nil
	}
	return &m
}

// ReadDDLMark reads the newest schema_changes row. nil when the table is empty
// or missing: there is nothing a mark could leave out.
func ReadDDLMark(ctx context.Context, db *sql.DB) (*DDLMark, error) {
	var m DDLMark
	err := db.QueryRowContext(ctx, `SELECT id, binlog_file, binlog_pos, detected_at, ddl_type
		FROM schema_changes ORDER BY id DESC LIMIT 1`).Scan(&m.ID, &m.File, &m.Pos, &m.DetectedAt, &m.Type)
	switch {
	case err == nil:
		return &m, nil
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	default:
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && me.Number == 1146 {
			return nil, nil
		}
		return nil, fmt.Errorf("read the newest schema_changes row: %w", err)
	}
}

// markStillNamesItsRow reports whether the row the mark names is still the
// same row. A mark whose row is gone or holds something else is not used:
// the ids it compares were handed out by another table.
func markStillNamesItsRow(ctx context.Context, db *sql.DB, m *DDLMark) (bool, error) {
	var cur DDLMark
	err := db.QueryRowContext(ctx, `SELECT binlog_file, binlog_pos, detected_at, ddl_type
		FROM schema_changes WHERE id = ?`, m.ID).Scan(&cur.File, &cur.Pos, &cur.DetectedAt, &cur.Type)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read the schema_changes row the snapshot's DDL mark names: %w", err)
	}
	return cur.File == m.File && cur.Pos == m.Pos && cur.Type == m.Type &&
		cur.DetectedAt.UTC().Equal(m.DetectedAt.UTC()), nil
}

// readRunDDLMark is a fold's mark, encoded: read only on an index a stream
// wrote, where ascending id is the order rows were indexed. A read that fails
// leaves the snapshot without one, which only places more rows later.
func readRunDDLMark(ctx context.Context, db *sql.DB) string {
	captured, err := query.StreamCaptured(ctx, db)
	if err != nil || !captured {
		return ""
	}
	m, err := ReadDDLMark(ctx, db)
	if err != nil {
		slog.Warn("could not read the newest schema_changes row; the snapshot is published without a DDL mark", "error", err)
		return ""
	}
	if m == nil {
		return ""
	}
	return m.Encode()
}

// markToStamp is the mark a table's new file may carry: the run's, and only
// when this run's check placed statements by position against the file's
// base, in the same binlog sequence as the cut the new file is anchored at. A
// check that looked by time alone did not account for a statement indexed
// late, and a mark would hide it from every check after.
func markToStamp(runMark string, w DDLWindow) string {
	if runMark == "" || w.Anchor == nil || w.Cut == nil || !sameBinlogSequence(w.Anchor.File, w.Cut.File) {
		return ""
	}
	return runMark
}
