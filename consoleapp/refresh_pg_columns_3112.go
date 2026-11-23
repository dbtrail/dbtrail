package consoleapp

import (
	"fmt"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A PostgreSQL column change no write followed (#3112).
//
// The read of the source an update makes for the tables it would leave out
// (checkNewTables, #2759) also brings each table's columns as the catalog has
// them at that moment. The fold compares them with the columns each table's
// file records and refuses a table that differs
// (reconstruct.WithPGSourceShapes), which stops the update the way a change
// of columns the stream recorded stops it: nothing is published, the refused
// tables are named on the run, and the schedule takes a full read in its
// place where it may.
//
// A change made after that read and before the snapshot is published is not
// in it: the next update's read sees it.

// pgSourceShapes splits what the source answered into the table names the
// new-tables check compares ("schema.table") and the shapes the fold
// compares, keyed by the two parts of the name. Never nil for an answer, an
// empty one included: nil means the source was not asked.
func pgSourceShapes(now []pgbaseline.SourceTable) (names []string, shapes map[reconstruct.PGTableName]reconstruct.PGSourceShape) {
	shapes = make(map[reconstruct.PGTableName]reconstruct.PGSourceShape, len(now))
	for _, t := range now {
		names = append(names, t.Schema+"."+t.Table)
		shapes[reconstruct.PGTableName{Schema: t.Schema, Table: t.Table}] = reconstruct.PGSourceShape{OID: t.OID, Columns: t.Columns}
	}
	return names, shapes
}

// pgSourceUnaskedReason is what the run says when a PostgreSQL source could
// not be asked: neither new tables nor changed columns were looked for, and
// the update goes on with what it has, as it does for the tables alone.
func pgSourceUnaskedReason(err error) string {
	return fmt.Sprintf("could not ask the source which tables it has, so a change of a table's columns that no write followed was not looked for either: %v", err)
}
