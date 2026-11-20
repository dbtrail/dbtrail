package recovery

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// CheckRowsUndoable is the up-front refusal every reversal script runs before
// it writes a byte: a row image holding a marker (a stand-in for a value the
// index does not hold) cannot be undone, and rendering one would write the
// marker's JSON into a real column.
//
// Two markers, two refusals:
//
//   - The not-captured marker (event.NotCapturedKey): the PostgreSQL table was
//     not at REPLICA IDENTITY FULL when the change was captured, so an UPDATE or
//     DELETE holds only the key of the earlier row. It is looked for across
//     EVERY row first and wins, because its message names the one action that
//     helps. The refusal counts every such change and the tables they are in,
//     so the operator can fix every table at once instead of one per run.
//   - The unchanged-TOAST marker (#592) gives its canonical error, unchanged.
//
// An INSERT carries neither (PostgreSQL always sends its whole row), so a
// window of INSERTs undoes normally. MySQL never produces either marker; a
// MySQL JSON column can only match one by storing exactly the marker's
// one-key object.
func CheckRowsUndoable(rows []query.ResultRow) error {
	var (
		agg       *NotCapturedRowsError
		firstMark error
		perTable  map[string]int
	)
	for _, row := range rows {
		err := event.CheckUnresolvedToast(row.SchemaName, row.TableName, row.PKValues, row.RowBefore, row.RowAfter)
		if err == nil {
			continue
		}
		var nc *event.NotCapturedError
		if !errors.As(err, &nc) {
			if firstMark == nil {
				firstMark = err
			}
			continue
		}
		if agg == nil {
			agg = &NotCapturedRowsError{First: nc}
			perTable = map[string]int{}
		}
		agg.Changes++
		name := row.TableName
		if row.SchemaName != "" {
			name = row.SchemaName + "." + row.TableName
		}
		if _, seen := perTable[name]; !seen {
			agg.Tables = append(agg.Tables, NotCapturedTable{Name: name})
		}
		perTable[name]++
	}
	if agg != nil {
		for i := range agg.Tables {
			agg.Tables[i].Changes = perTable[agg.Tables[i].Name]
		}
		return agg
	}
	return firstMark
}

// NotCapturedRowsError is CheckRowsUndoable's refusal for a window holding at
// least one change whose earlier row was only partly captured. First is the
// first such change (errors.As finds it, *event.NotCapturedError); with more
// than one, the message adds how many and in which tables.
type NotCapturedRowsError struct {
	First   *event.NotCapturedError
	Changes int
	Tables  []NotCapturedTable // in first-seen order
}

// NotCapturedTable is one table of a NotCapturedRowsError: "schema.table" and
// how many of the window's changes to it cannot be undone.
type NotCapturedTable struct {
	Name    string
	Changes int
}

func (e *NotCapturedRowsError) Error() string {
	msg := e.First.Error()
	if e.Changes <= 1 {
		return msg
	}
	parts := make([]string, len(e.Tables))
	for i, t := range e.Tables {
		parts[i] = fmt.Sprintf("%s (%d)", t.Name, t.Changes)
	}
	return fmt.Sprintf("%s. In all, %d change(s) in this window cannot be undone for the same reason, in: %s. "+
		"Set REPLICA IDENTITY FULL for each of these tables to make their later changes undoable",
		msg, e.Changes, strings.Join(parts, ", "))
}

func (e *NotCapturedRowsError) Unwrap() error { return e.First }
