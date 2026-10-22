package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
)

// Position is a binlog coordinate of the source.
type Position struct {
	File string
	Pos  uint64
}

func (p Position) String() string { return p.File + ":" + strconv.FormatUint(p.Pos, 10) }

// Before reports whether p is strictly before q. Files compare by the number
// after their last dot (binlog.000999 is before binlog.001000), by text when
// either has none; the zero Position is before every other.
func (p Position) Before(q Position) bool {
	if p.File != q.File {
		pn, pok := fileNumber(p.File)
		qn, qok := fileNumber(q.File)
		if pok && qok {
			return pn < qn
		}
		return p.File < q.File
	}
	return p.Pos < q.Pos
}

func fileNumber(name string) (uint64, bool) {
	i := strings.LastIndexByte(name, '.')
	if i < 0 {
		return 0, false
	}
	n, err := strconv.ParseUint(name[i+1:], 10, 64)
	return n, err == nil
}

// ReadPosition reads the source's current binlog position. The generator is
// paused when it is read, so nothing of this run is in flight.
func ReadPosition(db *sql.DB) (Position, error) {
	file, pos, err := config.CurrentBinlogPosition(db)
	if err != nil {
		return Position{}, err
	}
	return Position{File: file, Pos: uint64(pos)}, nil
}

// ServerNow reads the server's clock in UTC, to the second. The cut's time
// comes from the server, not this process: event timestamps are the
// server's, and a clock ahead of this one would put the newest events past
// a cut taken here.
func ServerNow(ctx context.Context, db *sql.DB) (time.Time, error) {
	var s string
	if err := db.QueryRowContext(ctx, "SELECT CAST(UTC_TIMESTAMP() AS CHAR)").Scan(&s); err != nil {
		return time.Time{}, fmt.Errorf("read the server clock: %w", err)
	}
	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("server clock %q: %w", s, err)
	}
	return t.UTC(), nil
}

// TableRows is a table's rows as canonical text: every column in SELECT *
// order, each cell the server's text for the value (CAST AS CHAR) or "NULL".
// Nulls says which cells are NULL, for a writer that must tell a NULL from
// the text "NULL". The comparison itself cannot: a VARCHAR holding the four
// letters NULL compares equal to a NULL on both sides. The generator never
// writes that text, so the oracle does not see the difference it cannot
// tell.
type TableRows struct {
	Table   string
	Columns []string
	PK      []string
	Rows    [][]string
	Nulls   [][]bool
}

// Key renders row i's primary key, its PK cells joined with "|". The
// separator is not escaped: a composite key whose cells hold "|" could
// collide with another, and the five tables' keys are integers and
// 'kNNN' text, which never do. A table with a "|" in a key column needs an
// escaped join here first.
func (r TableRows) Key(i int) string {
	return keyOf(r.Columns, r.PK, r.Rows[i])
}

func keyOf(columns, pk []string, row []string) string {
	parts := make([]string, len(pk))
	for i, c := range pk {
		for j, col := range columns {
			if col == c {
				parts[i] = row[j]
			}
		}
	}
	return strings.Join(parts, "|")
}

// ReadSource reads schema.t from the source as canonical text, the columns
// in the order SELECT * returns them.
func ReadSource(ctx context.Context, db *sql.DB, schema string, t Table) (TableRows, error) {
	cols, err := sourceColumns(ctx, db, schema, t.Name)
	if err != nil {
		return TableRows{}, err
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("CAST(`%s` AS CHAR)", c)
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM `%s`.`%s`", strings.Join(parts, ", "), schema, t.Name))
	if err != nil {
		return TableRows{}, fmt.Errorf("read %s.%s: %w", schema, t.Name, err)
	}
	defer rows.Close()
	out := TableRows{Table: t.Name, Columns: cols, PK: t.PK}
	for rows.Next() {
		cells := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return TableRows{}, fmt.Errorf("read %s.%s: %w", schema, t.Name, err)
		}
		text := make([]string, len(cols))
		nulls := make([]bool, len(cols))
		for i, c := range cells {
			if !c.Valid {
				text[i], nulls[i] = "NULL", true
				continue
			}
			text[i] = c.String
		}
		out.Rows = append(out.Rows, text)
		out.Nulls = append(out.Nulls, nulls)
	}
	if err := rows.Err(); err != nil {
		return TableRows{}, fmt.Errorf("read %s.%s: %w", schema, t.Name, err)
	}
	return out, nil
}

func sourceColumns(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position", schema, table)
	if err != nil {
		return nil, fmt.Errorf("columns of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns for %s.%s", schema, table)
	}
	return cols, rows.Err()
}

// CheckpointPosition reads capture's checkpoint from stream_state. ok is
// false when no checkpoint has been saved yet.
func CheckpointPosition(ctx context.Context, index *sql.DB) (Position, bool, error) {
	var p Position
	err := index.QueryRowContext(ctx, "SELECT binlog_file, binlog_position FROM stream_state WHERE id = 1").Scan(&p.File, &p.Pos)
	if errors.Is(err, sql.ErrNoRows) {
		return Position{}, false, nil
	}
	if err != nil {
		return Position{}, false, fmt.Errorf("read stream_state: %w", err)
	}
	return p, true, nil
}

// WaitCaptured is the barrier: it returns once capture's checkpoint is at or
// past want, and fails after timeout naming both positions. A position, not
// an event count: an UPDATE that changes nothing writes no row event, and
// the checkpoint still moves past it. The zero want is "any checkpoint",
// the wait for capture to attach.
//
// capture, when non-nil, is the channel the capture goroutine's result
// lands on: a result there while the barrier waits fails it at once with
// that result (nil included: a capture that exited cleanly indexes nothing
// more), so a capture that died mid-run is reported as such and not as a
// deadline. The result is put back on the channel for whoever stops the
// capture; the channel must therefore be buffered.
func WaitCaptured(ctx context.Context, index *sql.DB, capture chan error, want Position, timeout time.Duration) (Position, error) {
	poll := func(ctx context.Context) (Position, bool, error) { return CheckpointPosition(ctx, index) }
	return waitPosition(ctx, withCapture(poll, capture), want, timeout, 100*time.Millisecond)
}

type pollFunc func(context.Context) (Position, bool, error)

// withCapture makes poll fail when the capture's result has landed on
// capture, putting the result back for the caller that owns the goroutine.
func withCapture(poll pollFunc, capture chan error) pollFunc {
	if capture == nil {
		return poll
	}
	return func(ctx context.Context) (Position, bool, error) {
		select {
		case err := <-capture:
			capture <- err
			if err == nil {
				return Position{}, false, errors.New("the capture stopped (it returned nil) while the barrier waited; nothing indexes the rest of the run")
			}
			return Position{}, false, fmt.Errorf("the capture stopped while the barrier waited: %w", err)
		default:
		}
		return poll(ctx)
	}
}

func waitPosition(ctx context.Context, poll pollFunc, want Position, timeout, every time.Duration) (Position, error) {
	deadline := time.Now().Add(timeout)
	for {
		have, ok, err := poll(ctx)
		if err != nil {
			return Position{}, err
		}
		if ok && !have.Before(want) {
			return have, nil
		}
		if time.Now().After(deadline) {
			at := "no checkpoint saved"
			if ok {
				at = "checkpoint at " + have.String()
			}
			return Position{}, fmt.Errorf("capture did not reach %s in %s (%s); every change before the cut must be indexed before the copy is read", want, timeout, at)
		}
		select {
		case <-ctx.Done():
			return Position{}, ctx.Err()
		case <-time.After(every):
		}
	}
}
