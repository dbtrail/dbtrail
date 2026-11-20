package reconstruct

import (
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// checkPGEventMarkers is checkEventToast for a change of a PostgreSQL table
// in a fold: the same refusals, but for one thing a fold does not need.
//
// A table below REPLICA IDENTITY FULL sends, of the row as it was before an
// UPDATE or a DELETE, its key and nothing else, or no earlier row at all.
// Capture then stores that earlier row with its key columns real and every
// other column as the not-captured marker (event.NotCapturedKey). A fold
// builds the table from the rows AFTER each change and files each change
// under its key: it never reads the earlier row's other columns, so it takes
// such a change. Undo does read them, and refuses (event.CheckUnresolvedToast).
//
// What is still refused here:
//   - the marker in the row AFTER the change: that row is what the table is
//     built from;
//   - the marker in a key column of the earlier row: the change could not be
//     filed under the row it changed;
//   - an unchanged large value (the unchanged-TOAST marker) in either row,
//     as before: nothing here resolves one yet.
func checkPGEventMarkers(ev *query.ResultRow, pkCols []metadata.ColumnMeta) error {
	if cols := event.NotCapturedColumns(ev.RowAfter); len(cols) > 0 {
		return pgNotCapturedErr(ev, "the row as the change left it has no value for column(s) "+strings.Join(cols, ", "))
	}
	if len(event.NotCapturedColumns(ev.RowBefore)) > 0 {
		var keys []string
		for _, c := range pkCols {
			if event.IsNotCapturedMarker(ev.RowBefore[c.Name]) {
				keys = append(keys, c.Name)
			}
		}
		if len(keys) > 0 {
			return pgNotCapturedErr(ev, "the row as it was before the change has no value for its key column(s) "+strings.Join(keys, ", "))
		}
	}
	for _, img := range []map[string]any{ev.RowBefore, ev.RowAfter} {
		if cols := event.UnresolvedToastColumns(img); len(cols) > 0 {
			return event.UnresolvedToastError(ev.SchemaName, ev.TableName, ev.PKValues, cols)
		}
	}
	return nil
}

// pgNotCapturedErr is a fold's refusal of a change whose images lack what a
// fold needs. It says what a fold cannot do, not what undo cannot: the
// reader is rebuilding a table.
func pgNotCapturedErr(ev *query.ResultRow, what string) error {
	return fmt.Errorf("%s.%s (pk=%s): %s: the table was not at REPLICA IDENTITY FULL when the change was captured, "+
		"and PostgreSQL sent at most the row's key. The table cannot be rebuilt through this change; "+
		"take a new snapshot of it, and run ALTER TABLE ... REPLICA IDENTITY FULL on the source for later changes to be captured whole",
		ev.SchemaName, ev.TableName, ev.PKValues, what)
}
