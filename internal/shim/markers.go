package shim

import (
	"fmt"

	"github.com/dbtrail/dbtrail/internal/event"
)

// cellMarkerError refuses a cell that holds one of the two markers standing in
// for a value the index does not hold, instead of putting the marker's JSON on
// the wire as if it were the value. nil for every real value. schema/table may
// be empty where the caller does not know them.
//
//   - The unchanged-TOAST marker gives the canonical #592 error, unchanged.
//   - The not-captured marker stands for a column of a PostgreSQL earlier row
//     that was never sent (a table below REPLICA IDENTITY FULL). The rows this
//     server serves are the state after each change, which never carries it,
//     so one here means something upstream served an earlier row it should
//     not have. It is refused in this surface's own words: the shared message
//     speaks of undo.
//
// The MySQL capture writes neither; a MySQL JSON column matches one only by
// storing exactly the marker's one-key object.
func cellMarkerError(schema, table, col string, v any) error {
	if event.IsUnchangedToastMarker(v) {
		return event.UnresolvedToastError(schema, table, "", []string{col})
	}
	if event.IsNotCapturedMarker(v) {
		where := ""
		if table != "" {
			where = " of " + table
			if schema != "" {
				where = " of " + schema + "." + table
			}
		}
		return fmt.Errorf("cannot serve column %s%s: its value was not captured, because the PostgreSQL table was not at "+
			"REPLICA IDENTITY FULL when the change was captured (only the row's key was kept). The rows served here never "+
			"need that value, so this is a fault upstream of this server; please report it", col, where)
	}
	return nil
}
