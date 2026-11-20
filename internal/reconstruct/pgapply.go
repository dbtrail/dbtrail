package reconstruct

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// One row of a PostgreSQL table at a moment: the step ApplyAt, BuildHistory
// and FoldBeforeSnapshot take for a change that carries an LSN (pgRowLSN),
// in place of checkEventToast and applyEvent. A change from MySQL or MariaDB
// never comes here.
//
// It differs from that pair in one thing a table below REPLICA IDENTITY FULL
// brings: an earlier row captured with its key only (event.NotCapturedKey in
// its other columns) is taken. The row at a moment is the row the last
// change LEFT, whole, and the earlier row is not read. Undo reads it, and
// refuses. As before, a change missing from the events (a gap the caller
// went on over) is healed by the next change, which brings the whole row.
//
// What it does NOT take is a large value an UPDATE left as it was and
// PostgreSQL did not send (event.UnchangedToastKey in the row the change
// leaves). The value is the one the row held before the change, and these
// functions cannot tell whether the state they are handed is that row: their
// callers go on over gaps (allow-gaps, a table that entered capture late, a
// loss the port's reads do not look for), and a change missing before this
// one would leave an older value in the answer for good, with no change
// after it to heal it. A whole-table fold takes such a value because it
// knows when every change is in hand (pgCarry, pgBaseNotTheRow). Here it is
// refused, by name.
//
// Also refused, where a change with a whole earlier row is taken as it always
// was: one that moved the row to another key. With the whole earlier row the
// row of the new key is what the functions have always answered for the old
// one (#782's limit); it was refused for a key-only earlier row, and stays
// so.

// applyPGEvent is the state of a row after the PostgreSQL change ev, from
// its state before it. state is not modified.
func applyPGEvent(state map[string]any, ev *query.ResultRow) (map[string]any, error) {
	if err := checkPGRowMarkers(ev); err != nil {
		return nil, err
	}
	if cols := pgMovedIdentity(ev); len(cols) > 0 {
		return nil, pgRowReadErr(ev, "this change gave the row another value in "+strings.Join(cols, ", ")+
			", which identifies it, and the row before the change was captured with those columns only, so what became of the row under this key cannot be followed")
	}
	return applyEvent(state, *ev), nil
}

// foldPGEventBeforeSnapshot is FoldBeforeSnapshot's step for a PostgreSQL
// change: a change of key ends the row under this key, any other change is
// applied.
func foldPGEventBeforeSnapshot(state map[string]any, ev *query.ResultRow, pkCols []metadata.ColumnMeta) (map[string]any, error) {
	if err := checkPGRowMarkers(ev); err != nil {
		return nil, err
	}
	// The key itself not captured: the change cannot be said to be of this row.
	for _, c := range pkCols {
		if event.IsNotCapturedMarker(ev.RowBefore[c.Name]) {
			return nil, pgRowReadErr(ev, "the row before the change has no value for its key column "+c.Name)
		}
	}
	if _, _, moved := pkChangedInEvent(ev, pkCols); moved {
		return nil, nil
	}
	return applyPGEvent(state, ev)
}

// checkPGRowMarkers refuses what no single-row read builds from: either
// marker anywhere but the not-captured one in the earlier row. The
// unchanged-TOAST marker in the earlier row, or beside a whole earlier row,
// is refused in the words it always was.
func checkPGRowMarkers(ev *query.ResultRow) error {
	if cols := event.NotCapturedColumns(ev.RowAfter); len(cols) > 0 {
		return pgRowReadErr(ev, "the row as the change left it has no value for column(s) "+strings.Join(cols, ", "))
	}
	if cols := event.UnresolvedToastColumns(ev.RowBefore); len(cols) > 0 {
		return event.UnresolvedToastError(ev.SchemaName, ev.TableName, ev.PKValues, cols)
	}
	if cols := event.UnresolvedToastColumns(ev.RowAfter); len(cols) > 0 {
		if len(event.NotCapturedColumns(ev.RowBefore)) == 0 {
			return event.UnresolvedToastError(ev.SchemaName, ev.TableName, ev.PKValues, cols)
		}
		return pgRowReadErr(ev, "a change left the large value of column(s) "+strings.Join(cols, ", ")+
			" as it was and PostgreSQL did not send it, so the value is not in that change, and a read of one row cannot tell that it holds every change before it")
	}
	return nil
}

// pgMovedIdentity names, sorted, the columns an UPDATE's earlier row holds a
// value for, when that row was captured in part, and its new row holds
// another value for: the columns PostgreSQL sent of the earlier row are the
// ones that identify it.
func pgMovedIdentity(ev *query.ResultRow) []string {
	if ev.EventType != event.EventUpdate || len(event.NotCapturedColumns(ev.RowBefore)) == 0 {
		return nil
	}
	var cols []string
	for c, was := range ev.RowBefore {
		if event.IsNotCapturedMarker(was) {
			continue
		}
		if is, ok := ev.RowAfter[c]; !ok || is != was {
			cols = append(cols, c)
		}
	}
	sort.Strings(cols)
	return cols
}

// pgRowReadErr is a single-row read's refusal of a change it cannot build
// the row from. It speaks of reading the row, not of undo.
func pgRowReadErr(ev *query.ResultRow, why string) error {
	target := pgQuoteIdent(ev.SchemaName) + "." + pgQuoteIdent(ev.TableName)
	return fmt.Errorf("the row of %s (pk=%s) cannot be read through one of its changes: %s. "+
		"The table was not at REPLICA IDENTITY FULL when the change was captured; "+
		"ALTER TABLE %s REPLICA IDENTITY FULL on the source makes later changes carry the whole row",
		target, ev.PKValues, why, target)
}
