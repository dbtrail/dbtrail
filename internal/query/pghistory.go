package query

import (
	"fmt"

	"github.com/dbtrail/dbtrail/internal/event"
)

// History surfaces (query, the console's Events view, the MCP query tool) over
// an index written by PostgreSQL capture show rows of tables that were not at
// REPLICA IDENTITY FULL when the change was captured. Such a row's earlier
// image (row_before) carries only the key, every other column being the
// not-captured marker (event.NotCapturedKey), and its later image may carry
// the unchanged-TOAST marker (event.UnchangedToastKey) for a large value
// PostgreSQL did not resend. Every helper here is meaningful ONLY on a
// PostgreSQL index: a MySQL JSON column can hold exactly the marker object,
// which cannot be told apart from the marker by its shape (see
// event.NotCapturedKey), so the gate is the index's flavor
// (Options.PostgresHistory), never the content of a row.

// BeforeImageKeyOnly is the value of the before_image field of a history row
// whose earlier row carries only its key.
const BeforeImageKeyOnly = "key_only"

// The words history surfaces print in place of a marker, and in place of an
// unknown set of changed columns.
const (
	NotCapturedText           = "(not captured: REPLICA IDENTITY not FULL)"
	UnchangedLargeValueText   = "(unchanged large value)"
	ChangedColumnsUnknownText = "unknown"
)

// KeyOnlyBefore reports whether a PostgreSQL row's earlier image carries the
// not-captured marker: PostgreSQL sent at most the row's key.
func KeyOnlyBefore(r *ResultRow) bool {
	for _, v := range r.RowBefore {
		if event.IsNotCapturedMarker(v) {
			return true
		}
	}
	return false
}

// UnchangedColumns returns, sorted, the columns of a PostgreSQL row's later
// image that carry the unchanged-TOAST marker; nil when none do.
func UnchangedColumns(r *ResultRow) []string {
	return event.UnresolvedToastColumns(r.RowAfter)
}

// ChangedColumnsUnknown reports whether a PostgreSQL UPDATE's set of changed
// columns is unknown: its earlier image carries only the key, so the indexer
// stored no changed columns (event.ChangedColumns). A no-op UPDATE of a table
// at FULL also stores none, and is not unknown: its earlier image is whole.
func ChangedColumnsUnknown(r *ResultRow) bool {
	return r.EventType == event.EventUpdate && len(r.ChangedColumns) == 0 && KeyOnlyBefore(r)
}

// MayHaveChangedCount counts the rows a PostgreSQL changed-column filter
// returned only because their changed columns are unknown.
func MayHaveChangedCount(rows []ResultRow) int {
	n := 0
	for i := range rows {
		if ChangedColumnsUnknown(&rows[i]) {
			n++
		}
	}
	return n
}

// MayHaveChangedNotice is the sentence a history surface prints when a
// changed-column filter on a PostgreSQL index returned n UPDATEs whose changed
// columns are unknown. Empty when n is 0. It names no command-line flag: the
// MCP tool and the console print it too.
func MayHaveChangedNotice(column string, n int) string {
	if n == 0 {
		return ""
	}
	rows := "rows"
	if n == 1 {
		rows = "row"
	}
	return fmt.Sprintf("%d UPDATE %s may have changed %s: the table was not at REPLICA IDENTITY FULL when "+
		"they were captured, so PostgreSQL sent only the earlier row's key and which columns changed is unknown "+
		"(changed columns: %s). They are included so the filter misses nothing.",
		n, rows, column, ChangedColumnsUnknownText)
}

// historyRowNotes is the note printed under a table-format history of a
// PostgreSQL index, which shows no row images: how many rows carry an earlier
// row with only its key, and how many carry a large value PostgreSQL did not
// resend. Empty when there are none, so a table with no such row prints
// exactly what it printed before.
func historyRowNotes(rows []ResultRow) string {
	keyOnly, unchanged := 0, 0
	for i := range rows {
		if KeyOnlyBefore(&rows[i]) {
			keyOnly++
		}
		if len(UnchangedColumns(&rows[i])) > 0 {
			unchanged++
		}
	}
	var out string
	if keyOnly > 0 {
		out += fmt.Sprintf("\nNote: %d %s an earlier row with only its key; its other values are %s. "+
			"The JSON format shows which.\n", keyOnly, rowsCarry(keyOnly), NotCapturedText)
	}
	if unchanged > 0 {
		out += fmt.Sprintf("\nNote: %d %s a column marked %s: PostgreSQL did not resend it because the change "+
			"left it as it was. The JSON format shows which.\n", unchanged, rowsCarry(unchanged), UnchangedLargeValueText)
	}
	return out
}

func rowsCarry(n int) string {
	if n == 1 {
		return "row carries"
	}
	return "rows carry"
}

// pgPartialUpdateSQL is the MySQL predicate a PostgreSQL changed-column
// filter ORs with JSON_CONTAINS: an UPDATE that stored no changed columns
// (SQL NULL, or the JSON null the indexer writes for a nil list) and whose
// earlier image has a column whose value is an object with the not-captured
// key. On a PostgreSQL index every real value is text, so an object key there
// is a marker. The event type is bound as an argument.
const pgPartialUpdateSQL = "(event_type = ? AND (changed_columns IS NULL OR JSON_TYPE(changed_columns) = 'NULL') " +
	"AND JSON_CONTAINS_PATH(row_before, 'one', '$.*.\"" + event.NotCapturedKey + "\"'))"
