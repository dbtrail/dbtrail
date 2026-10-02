package recovery

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/sysversion"
)

// ErrSystemVersioned classifies a refusal to reverse an event of a MariaDB
// system-versioned table (#2007): a versioning shape this tool cannot read,
// or one it cannot rule out. It is not a damaged row image, and the error
// says so, so the operator does not go hunting corruption.
var ErrSystemVersioned = errors.New("cannot reverse an event of a system-versioned table")

type svRefusal struct{ msg string }

func (e *svRefusal) Error() string        { return e.msg }
func (e *svRefusal) Is(target error) bool { return target == ErrSystemVersioned }

func svRefuse(format string, args ...any) error { return &svRefusal{msg: fmt.Sprintf(format, args...)} }

// systemVersionedError is the whole-script refusal when one or more events
// of a system-versioned table cannot be reversed.
func systemVersionedError(failures []genFailure) error {
	parts := make([]string, 0, len(failures))
	for _, f := range failures {
		parts = append(parts, f.err.Error())
	}
	return &svRefusal{msg: fmt.Sprintf("recover: refusing to emit reversal SQL — %d matched event(s) of a "+
		"system-versioned table cannot be reversed safely, and a script without them would be a silently "+
		"incomplete recovery: %s", len(failures), strings.Join(parts, "; "))}
}

// imageHoldsMarker reports whether any value of the row's images is a
// current-row marker of MariaDB system versioning.
func imageHoldsMarker(row query.ResultRow) bool {
	for _, img := range []map[string]any{row.RowBefore, row.RowAfter} {
		for _, v := range img {
			if sysversion.IsCurrentMarker(v) {
				return true
			}
		}
	}
	return false
}

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

// markerInGeneratedKey reports whether one of tm's generated primary-key
// columns holds a current-row marker in row's images: the shape of a MariaDB
// versioned table the flavor rule did not recognize.
func (g *Generator) markerInGeneratedKey(tm *metadata.TableMeta, row query.ResultRow) bool {
	for _, c := range tm.Columns {
		if !c.IsPK || !c.IsGenerated {
			continue
		}
		for _, img := range []map[string]any{row.RowBefore, row.RowAfter} {
			if sysversion.IsCurrentMarker(img[c.Name]) {
				return true
			}
		}
	}
	return false
}

// sysVersionedTable resolves row's table against the event-time schema
// snapshot. found is false when no snapshot describes it.
func (g *Generator) sysVersionedTable(row query.ResultRow) (*metadata.TableMeta, bool) {
	r := g.resolverForRow(row)
	if r == nil {
		return nil, false
	}
	tm, err := r.Resolve(row.SchemaName, row.TableName)
	if err != nil {
		slog.Warn("no schema snapshot describes the table, so recover cannot tell whether it is system-versioned",
			"schema", row.SchemaName, "table", row.TableName, "error", err)
		return nil, false
	}
	return tm, true
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
	tm, found := g.sysVersionedTable(row)
	if !found {
		// Without a snapshot nothing says whether the table is versioned,
		// and reading a versioned table's events literally is the #2007
		// bug. A current-row marker in an image is the tell.
		if imageHoldsMarker(row) {
			return row, "", svRefuse("event %d: cannot tell whether %s.%s is system-versioned without a schema snapshot "+
				"(its row image holds MariaDB's current-row marker); take a schema snapshot first (`bintrail snapshot`)",
				row.EventID, row.SchemaName, row.TableName)
		}
		return row, "", nil
	}
	end, ok := g.sysVersionedEnd(tm)
	if !ok {
		if g.markerInGeneratedKey(tm, row) {
			return row, "", svRefuse("event %d: %s.%s has a generated primary-key column holding MariaDB's current-row "+
				"marker, but the index records its source as %q, so recover cannot tell whether the table is "+
				"system-versioned; take a schema snapshot with the current bintrail (`bintrail snapshot`) so the "+
				"source is recorded, then retry", row.EventID, row.SchemaName, row.TableName, g.sourceFlavor())
		}
		return row, "", nil
	}
	state := func(img map[string]any, which string) (sysversion.State, error) {
		if img == nil {
			return 0, svRefuse("event %d of system-versioned table %s.%s has no %s image", row.EventID, row.SchemaName, row.TableName, which)
		}
		st, err := sysversion.RowEnd(img[end], row.EventTimestamp)
		if err != nil {
			return 0, svRefuse("event %d of system-versioned table %s.%s: %v", row.EventID, row.SchemaName, row.TableName, err)
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
			return row, "this is a history version of the row (an old version kept when the row changed, or one written into history directly), not a change to its current rows", nil
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
			return row, "", svRefuse("event %d of system-versioned table %s.%s makes a history version current again, "+
				"which has no reversal this tool can write", row.EventID, row.SchemaName, row.TableName)
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
