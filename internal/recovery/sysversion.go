package recovery

import (
	"fmt"
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/sysversion"
)

// sourceFlavor is the index's source flavor ("mysql", "mariadb", "postgres"
// or "" when unknown), read once.
func (g *Generator) sourceFlavor() string {
	if !g.flavorRead {
		g.flavorRead = true
		if g.db != nil {
			g.flavor = query.SourceFlavor(g.db)
		}
	}
	return g.flavor
}

// sysVersionedEnd names the ROW END column of a MariaDB system-versioned
// table (#2007), from its schema snapshot.
//
// On a MariaDB source any generated primary-key member IS the ROW END column:
// MariaDB refuses a generated column in a primary key otherwise ("Primary key
// cannot be defined upon a generated column"). A MySQL or PostgreSQL source
// has no system versioning, whatever the table's shape. Only when the flavor
// is unknown (an index read without stream_state) does the snapshot shape
// decide (sysversion.FromSnapshot).
func (g *Generator) sysVersionedEnd(tm *metadata.TableMeta) (string, bool) {
	end, gens := "", 0
	for _, c := range tm.Columns {
		if c.IsPK && c.IsGenerated {
			end = c.Name
			gens++
		}
	}
	if gens == 0 {
		return "", false // the common case: no flavor read needed
	}
	switch g.sourceFlavor() {
	case "mysql", "postgres":
		return "", false
	case "mariadb":
		return end, gens == 1
	}
	p, ok := sysversion.FromSnapshot(tm.Columns)
	return p.End, ok
}

// sysVersionedPeriodEnd is sysVersionedEnd for row's table, resolved against
// the event-time schema snapshot.
func (g *Generator) sysVersionedPeriodEnd(row query.ResultRow) (string, bool) {
	r := g.resolverForRow(row)
	if r == nil {
		return "", false
	}
	tm, err := r.Resolve(row.SchemaName, row.TableName)
	if err != nil {
		// pkWhereClause warns about the same failure for this row.
		slog.Debug("cannot resolve table to check for system versioning", "schema", row.SchemaName,
			"table", row.TableName, "error", err)
		return "", false
	}
	return g.sysVersionedEnd(tm)
}

// sysVersionedReversal decides what reversing one event means when its table
// is system-versioned (#2007). Every other table's event comes back unchanged.
//
// Such a table's binlog does not hold only changes to its rows (see package
// sysversion), so reversing each event literally is wrong in two ways that
// apply cleanly: a versioned delete (an UPDATE moving ROW END into the past)
// reversed as an UPDATE ... WHERE row_end = <past> matches nothing, so the
// deleted row is never restored; and a DELETE HISTORY reversed as INSERTs
// brings old versions back as current rows. Instead:
//
//	INSERT of a current row             → reversed (DELETE)
//	UPDATE current → current            → reversed (UPDATE)
//	UPDATE current → history            → a delete: reversed by INSERTing the row back
//	DELETE of a current row             → reversed (INSERT)
//	INSERT of a history row             → skipped: the old version the server kept
//	UPDATE history → history            → skipped: an edit of history
//	DELETE of a history row             → skipped: history cannot be written back
//
// skip is the reason an event emits no statement. An UPDATE that makes a
// history version current, or a ROW END value nothing explains, is an error:
// guessing would emit SQL that applies and is wrong.
func (g *Generator) sysVersionedReversal(row query.ResultRow) (out query.ResultRow, skip string, err error) {
	end, ok := g.sysVersionedPeriodEnd(row)
	if !ok {
		return row, "", nil
	}
	state := func(img map[string]any, which string) (sysversion.State, error) {
		if img == nil {
			return 0, fmt.Errorf("system-versioned table %s.%s: event %d has no %s image", row.SchemaName, row.TableName, row.EventID, which)
		}
		st, err := sysversion.RowEnd(img[end], row.EventTimestamp)
		if err != nil {
			return 0, fmt.Errorf("system-versioned table %s.%s: event %d: %w", row.SchemaName, row.TableName, row.EventID, err)
		}
		return st, nil
	}
	switch row.EventType {
	case event.EventInsert:
		after, err := state(row.RowAfter, "after")
		if err != nil {
			return row, "", err
		}
		if after == sysversion.History {
			return row, "this is the old version system versioning kept when the row changed; the server writes and owns it", nil
		}
	case event.EventUpdate:
		before, err := state(row.RowBefore, "before")
		if err != nil {
			return row, "", err
		}
		after, err := state(row.RowAfter, "after")
		if err != nil {
			return row, "", err
		}
		switch {
		case before == sysversion.Current && after == sysversion.History:
			row.EventType = event.EventDelete
			row.RowAfter = nil
		case before == sysversion.History && after == sysversion.History:
			return row, "this edits a history version; history cannot be written back", nil
		case before == sysversion.History:
			return row, "", fmt.Errorf("system-versioned table %s.%s: event %d makes a history version current again, "+
				"which has no reversal this tool can write", row.SchemaName, row.TableName, row.EventID)
		}
	case event.EventDelete:
		before, err := state(row.RowBefore, "before")
		if err != nil {
			return row, "", err
		}
		if before == sysversion.History {
			return row, "this removed a history version (DELETE HISTORY or partition rotation); history cannot be written back", nil
		}
	}
	return row, "", nil
}
