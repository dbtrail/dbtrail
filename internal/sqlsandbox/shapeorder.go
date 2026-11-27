package sqlsandbox

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
)

// The join order by shape (Session.JoinOrderByShape): for the MySQL port,
// a join order picked before the statement runs from the shape of its FROM
// alone, with no estimate.
//
// The join check (joinguard.go) cannot do this job on a MySQL copy. Its
// views drop deleted rows with a LEFT join and a FILTER, which DuckDB takes
// to keep a fifth of its input, so the filtered side looks small; with the
// copy's own counts of distinct values the check flagged neither Q5 nor Q8
// of the CH-benCHmark, and no bound tells the dangerous reports from the
// good ones (Q7, which answers, scores higher than Q5). On a copy of 100
// warehouses (memory_limit 2GB, 8 GB of spill) Q5, Q8 and Q21 ran out of
// memory in DuckDB's order, after 237 s, 16 s and 7 s.
//
// So, for one SELECT whose FROM is a list of tables (the shape
// joinconnected.go writes again: no JOIN, subquery, WITH, OR at the top of
// WHERE, *, COLUMNS(...) or #n):
//
//   - its order written is connected already (each table has a condition
//     with one before it): it runs in that order, the copy's join reordering
//     off. Q5 answered in 2.9 s so, and in 105 s in the connected order;
//   - otherwise it is written again in the connected order and runs in it,
//     reordering off: Q8 in 0.67 s, Q21 in 1.8 s;
//   - anything else (one table, a real cross product, a rewrite that does
//     not check out) runs as it would without the option.
//
// Over the 22 reports no answer changed, and none was more than 1.6 times
// slower (Q3, 0.46 to 0.75 s). A statement written in a connected but bad
// order can be slower than in DuckDB's; it is never run a second time in
// another order, so one that fails in the order picked fails as it would
// have (the MySQL port then sends it to MySQL).

// The orders Result.JoinOrderByShape names.
const (
	JoinOrderByShapeWritten   = "written"
	JoinOrderByShapeConnected = "connected"
	// JoinOrderByShapeFailed: the statement had the shape, and picking its
	// order failed on an error (a step of the rewrite, or turning the
	// reordering off), so it ran in the copy's own order. Logged as a
	// warning; a statement that does not have the shape is not this.
	JoinOrderByShapeFailed = "failed"
)

// shapeOrderFailedNote is the note of a rewrite that did not work: the
// step's own text can quote the engine's, and so a row's value.
const shapeOrderFailedNote = "a step of writing the statement in a connected order did not work"

// shapeOrder is what the join order by shape picked: order
// JoinOrderByShapeWritten or JoinOrderByShapeConnected, with text the
// statement to run and tables its connected order; or order "" (the shape
// does not apply) or JoinOrderByShapeFailed (an error), and note why the
// statement is left as it is.
type shapeOrder struct {
	order  string
	text   string
	tables []string
	note   string
}

// pickShapeOrder picks the join order of statement on conn, the statement's
// session with its views in place and the copy's own optimizers still on
// (the tables' estimates are read in them). It changes nothing in the
// session: the caller turns the reordering off when an order is picked.
func pickShapeOrder(ctx context.Context, conn *sql.Conn, statement string) shapeOrder {
	rw, note := connectedOrderSQL(ctx, conn, statement, sessionTableFacts(ctx, conn))
	switch {
	case note.writtenConnected:
		return shapeOrder{order: JoinOrderByShapeWritten, text: statement}
	case note.failed:
		return shapeOrder{order: JoinOrderByShapeFailed, note: shapeOrderFailedNote}
	case note.why != "":
		return shapeOrder{note: shapeNoteForLog(note.why)}
	}
	return shapeOrder{order: JoinOrderByShapeConnected, text: rw.text, tables: rw.order}
}

// shapeOrderColumnNote stands in a note for one that names a column of the
// statement (placeColumn): a MySQL client writes a string in double quotes,
// which the copy reads as a column's name, so the name can be a value.
const shapeOrderColumnNote = "a column of the WHERE is not in exactly one table of the FROM"

// shapeNoteForLog is why, a shape's why of connectedOrderSQL, as it may be
// logged: without the names of the statement's columns.
func shapeNoteForLog(why string) string {
	for _, s := range []string{"more than one table", "none of the tables", "is not a column of a table"} {
		if strings.Contains(why, s) {
			return shapeOrderColumnNote
		}
	}
	return why
}

// logJoinOrderByShapeNote says why the join order by shape left out's
// statement to the copy's own order: as a warning when picking it failed on
// an error (shapeOrderFailedNote, or the SET that turns the reordering off,
// which holds no row's value), at debug level when the shape does not apply
// (a shape's why, with no name of a column: shapeNoteForLog). Never the
// engine's text of a step that read the statement or its tables.
func logJoinOrderByShapeNote(out wireResult) {
	switch {
	case out.JoinOrderByShapeNote == "":
	case out.JoinOrderByShape == JoinOrderByShapeFailed:
		slog.Warn("sql on the copy: the join order of a statement of tables listed with commas could not be picked by its shape; it ran in the copy's own join order", "why", out.JoinOrderByShapeNote)
	default:
		slog.Debug("sql on the copy: the join order by shape left a statement to the copy's own join order", "why", out.JoinOrderByShapeNote)
	}
}
