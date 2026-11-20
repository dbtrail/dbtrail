package reconstruct

import (
	"fmt"
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
// It differs from that pair in what a table below REPLICA IDENTITY FULL
// brings (the fold of a whole table takes the same: checkPGEventMarkers,
// pgCarry):
//
//   - an earlier row captured with its key only (event.NotCapturedKey in
//     its other columns) is taken: the row at a moment is built from the
//     rows the changes LEAVE, and the earlier row is not read. Undo reads
//     it, and refuses;
//   - a large value an UPDATE left as it was and PostgreSQL did not send
//     (event.UnchangedToastKey in the row the change leaves) is the value
//     the row held before the change: the state's. With no state to take
//     it from (no snapshot row and no earlier change), or a state that does
//     not hold it, the row cannot be told and the read refuses. It is never
//     NULL in the answer.
//
// The changes of a row reach here in the order they are fetched, commit time
// and then event id, as every single-row read of a PostgreSQL table has them
// (query.OrderByBinlog, OrderPostgres).

// checkPGRowMarkers refuses what no single-row read can build from: either
// marker anywhere but the two places above.
func checkPGRowMarkers(ev *query.ResultRow) error {
	if cols := event.NotCapturedColumns(ev.RowAfter); len(cols) > 0 {
		return pgRowReadErr(ev, "the row as the change left it has no value for column(s) "+strings.Join(cols, ", ")+
			": the table was not at REPLICA IDENTITY FULL when the change was captured")
	}
	if cols := event.UnresolvedToastColumns(ev.RowBefore); len(cols) > 0 {
		return event.UnresolvedToastError(ev.SchemaName, ev.TableName, ev.PKValues, cols)
	}
	// As in a fold: only an UPDATE, and only beside an earlier row that
	// says it was captured with its key only (checkPGEventMarkers).
	if cols := event.UnresolvedToastColumns(ev.RowAfter); len(cols) > 0 &&
		(ev.EventType != event.EventUpdate || len(event.NotCapturedColumns(ev.RowBefore)) == 0) {
		return event.UnresolvedToastError(ev.SchemaName, ev.TableName, ev.PKValues, cols)
	}
	return nil
}

// applyPGEvent is the state of a row after the PostgreSQL change ev, from
// its state before it. state is not modified.
func applyPGEvent(state map[string]any, ev *query.ResultRow) (map[string]any, error) {
	if err := checkPGRowMarkers(ev); err != nil {
		return nil, err
	}
	cols := event.UnresolvedToastColumns(ev.RowAfter)
	if len(cols) == 0 {
		return applyEvent(state, *ev), nil
	}
	next := copyMap(ev.RowAfter)
	for _, c := range cols {
		v, ok := state[c]
		switch {
		case state == nil:
			return nil, pgRowReadErr(ev, "the change left the large value of column "+c+" as it was, PostgreSQL did not send it "+
				"(the table was not at REPLICA IDENTITY FULL), and there is no row before this change to take it from "+
				"(no snapshot holds the row and no earlier change of it is indexed)")
		case !ok || event.IsUnchangedToastMarker(v) || event.IsNotCapturedMarker(v):
			return nil, pgRowReadErr(ev, "the change left the large value of column "+c+" as it was, PostgreSQL did not send it "+
				"(the table was not at REPLICA IDENTITY FULL), and the row before this change does not hold it")
		}
		next[c] = v
	}
	return next, nil
}

// foldPGEventBeforeSnapshot is FoldBeforeSnapshot's step for a PostgreSQL
// change: a change of key ends the row under this key, any other change is
// applied.
func foldPGEventBeforeSnapshot(state map[string]any, ev *query.ResultRow, pkCols []metadata.ColumnMeta) (map[string]any, error) {
	if _, _, moved := pkChangedInEvent(ev, pkCols); moved {
		if err := checkPGRowMarkers(ev); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return applyPGEvent(state, ev)
}

// pgRowReadErr is a single-row read's refusal of a change it cannot build
// the row from. It speaks of the row at a moment, not of undo.
func pgRowReadErr(ev *query.ResultRow, why string) error {
	target := pgQuoteIdent(ev.SchemaName) + "." + pgQuoteIdent(ev.TableName)
	return fmt.Errorf("the row of %s (pk=%s) cannot be told at that moment: %s. "+
		"ALTER TABLE %s REPLICA IDENTITY FULL on the source makes later changes carry the whole row",
		target, ev.PKValues, why, target)
}
