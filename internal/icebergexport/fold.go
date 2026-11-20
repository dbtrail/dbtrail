package icebergexport

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// netOp is the net effect of a window on one primary key: the row that
// exists at the cut (Row non-nil), or nothing (Row nil). PK holds the key
// columns as the row image spelled them, which is what the equality-delete
// file carries.
type netOp struct {
	PK  map[string]any
	Row map[string]any
	// at is where the event this op came from starts in the binary log, kept
	// so a later page can tell whether its change of the key is the newer one,
	// and when it ran, so a narrowing ALTER can be placed against it (#2735).
	at query.ResultRow
}

// deleted reports whether the key does not exist at the cut.
func (o *netOp) deleted() bool { return o.Row == nil }

// fold reduces an ascending event window to one netOp per primary key, in
// first-touched order. It holds one entry per DISTINCT touched key for the
// whole window, whatever the page size: paging bounds the fetch, not the map
// (the same shape as reconstruct's change map, #1107). The change kept for a key is the one the binary log
// holds last (query.LaterInBinlog), which is not always the one that arrives
// last: pages come in (event_timestamp, event_id) order, and a statement that
// waited on a row lock is written after the change it waited for with an
// earlier time (#2151). The same rule the full-table reconstruct applies, so
// the exported table equals `reconstruct` at the same cut.
//
// It is this package's own fold rather than reconstruct's because the delete
// file needs the TYPED key columns of a DELETE, which live in its before-image,
// and the shared fold trims before-images away (retainEvent). Per-event guards
// run here on the untrimmed event for the same reason.
type fold struct {
	schema, table string
	pkCols        []metadata.ColumnMeta
	// pg marks a PostgreSQL source: its rows carry no binary log coordinate
	// to order the two halves of a key change, so such a change is refused.
	pg     bool
	ops    map[string]*netOp
	order  []string
	events int64
}

func newFold(schema, table string, pkCols []metadata.ColumnMeta, pg bool) *fold {
	return &fold{schema: schema, table: table, pkCols: pkCols, pg: pg, ops: map[string]*netOp{}}
}

// addPage folds one page. An unresolved TOAST marker (#592) refuses the table,
// because the alternative is a table that reads cleanly and is wrong. An
// UPDATE that changes the primary key is folded as what it is, a DELETE of the
// old key and an INSERT of the new one at the UPDATE's own coordinates
// (reconstruct.SplitPKChange, the full-table fold's split since #2501), so the
// exported table equals `reconstruct` at the same cut (#2619). The split keeps
// its own refusals: a key the index would spell another way, and PostgreSQL.
//
// A PostgreSQL table below REPLICA IDENTITY FULL is exported too (f.pg): its
// UPDATEs and DELETEs keep only the key of the earlier row, which is all the
// export reads from it, and an unchanged large value in an UPDATE's new row is
// carried from the key's earlier change in this fold (apply).
func (f *fold) addPage(page []query.ResultRow) error {
	for i := range page {
		ev := &page[i]
		if err := f.checkMarkers(ev); err != nil {
			return err
		}
		halves, err := reconstruct.SplitPKChange(ev, f.schema, f.table, f.pkCols, f.pg)
		if err != nil {
			// The shared message names reconstruct's remedy. Here a new
			// baseline alone changes nothing: the cursor lives in the table
			// and the next run reads this window again (#2619).
			return fmt.Errorf("%w. For the Iceberg export: the table's cursor stays before this change and every run refuses it again; "+
				"take a baseline at or after the change and remove the table directory (or drop the table from the catalog) so the next run reloads %s.%s from it",
				err, f.schema, f.table)
		}
		// A key change counts once, as the UPDATE it is: events is what the
		// binary log holds, not what the fold files.
		f.events++
		if halves == nil {
			if err := f.apply(ev); err != nil {
				return err
			}
			continue
		}
		for h := range halves {
			if err := f.apply(&halves[h]); err != nil {
				return err
			}
		}
	}
	return nil
}

// apply files one event's net effect under its key, unless the fold already
// holds a change of that key from further on in the binary log.
func (f *fold) apply(ev *query.ResultRow) error {
	var op *netOp
	switch ev.EventType {
	case event.EventInsert, event.EventUpdate:
		if ev.RowAfter == nil {
			return fmt.Errorf("event %d (type %d) for %s.%s pk %q carries no after-image", ev.EventID, ev.EventType, f.schema, f.table, ev.PKValues)
		}
		row := copyMap(ev.RowAfter)
		if f.pg {
			if err := f.carryUnchanged(ev, row); err != nil {
				return err
			}
		}
		pk, err := pkImage(row, f.pkCols)
		if err != nil {
			return fmt.Errorf("event %d for %s.%s: %w", ev.EventID, f.schema, f.table, err)
		}
		op = &netOp{PK: pk, Row: row}
	case event.EventDelete:
		if ev.RowBefore == nil {
			return fmt.Errorf("DELETE event %d for %s.%s pk %q carries no before-image", ev.EventID, f.schema, f.table, ev.PKValues)
		}
		pk, err := pkImage(ev.RowBefore, f.pkCols)
		if err != nil {
			return fmt.Errorf("DELETE event %d for %s.%s: %w", ev.EventID, f.schema, f.table, err)
		}
		if f.pg {
			// Below FULL the earlier row is the key alone; the key itself must
			// be real, or the equality delete would match nothing.
			var marked []string
			for _, c := range f.pkCols {
				if event.IsNotCapturedMarker(pk[c.Name]) || event.IsUnchangedToastMarker(pk[c.Name]) {
					marked = append(marked, c.Name)
				}
			}
			if len(marked) > 0 {
				return f.markerMisplacedErr(ev, marked)
			}
		}
		op = &netOp{PK: pk}
	default:
		// EventType 0 is the drift shape (#318): a NULL event_type column.
		// Not skippable: a skipped event is a silently wrong table.
		return fmt.Errorf("event %d for %s.%s pk %q has event type %d, which the export cannot apply", ev.EventID, f.schema, f.table, ev.PKValues, ev.EventType)
	}
	op.at = query.ResultRow{EventID: ev.EventID, BinlogFile: ev.BinlogFile, StartPos: ev.StartPos, EventTimestamp: ev.EventTimestamp}
	cur, seen := f.ops[ev.PKValues]
	if !seen {
		f.order = append(f.order, ev.PKValues)
	} else if query.LaterInBinlog(&cur.at, &op.at) {
		// Every guard above has read this event; the key keeps the change
		// from further on in the binary log.
		return nil
	}
	f.ops[ev.PKValues] = op
	return nil
}

// checkMarkers is the per-event refusal of a value the index does not hold.
//
// MySQL (and every non-PostgreSQL source) keeps the shared refusal exactly:
// either marker in either image refuses (#592). Only the not-captured message
// is reworded, because the shared one speaks of undo.
//
// PostgreSQL: the not-captured marker in row_before is expected below REPLICA
// IDENTITY FULL and the export does not need that earlier row (a DELETE's key
// is checked in apply). An unchanged large value in row_after is left for
// apply to carry. What the capture never writes still refuses: the
// not-captured marker in a new row, and an unchanged large value in an earlier
// row.
func (f *fold) checkMarkers(ev *query.ResultRow) error {
	if !f.pg {
		err := event.CheckUnresolvedToast(ev.SchemaName, ev.TableName, ev.PKValues, ev.RowBefore, ev.RowAfter)
		var nc *event.NotCapturedError
		if errors.As(err, &nc) {
			return f.markerMisplacedErr(ev, nc.Columns)
		}
		return err
	}
	if cols := event.NotCapturedColumns(ev.RowAfter); len(cols) > 0 {
		return f.markerMisplacedErr(ev, cols)
	}
	if cols := event.UnresolvedToastColumns(ev.RowBefore); len(cols) > 0 {
		return event.UnresolvedToastError(ev.SchemaName, ev.TableName, ev.PKValues, cols)
	}
	return nil
}

// carryUnchanged replaces, in row (a PostgreSQL INSERT or UPDATE's new row,
// already copied), each unchanged large value PostgreSQL did not resend with
// the value the key's earlier change in this fold left. The fold keeps one op
// per key holding the newest row it filed, never a marker (this function
// resolved or refused each), so a chain of such UPDATEs carries through. With
// no earlier row (first change of the key in the window, or the key deleted
// before it) the value is not in reach, and the table is refused: exporting
// the marker, or NULL, would be a table that reads cleanly and is wrong.
func (f *fold) carryUnchanged(ev *query.ResultRow, row map[string]any) error {
	cols := event.UnresolvedToastColumns(row)
	if len(cols) == 0 {
		return nil
	}
	cur, seen := f.ops[ev.PKValues]
	var missing []string
	for _, c := range cols {
		if ev.EventType == event.EventUpdate && seen && !cur.deleted() {
			if v, ok := cur.Row[c]; ok {
				row[c] = v
				continue
			}
		}
		missing = append(missing, c)
	}
	if len(missing) == 0 {
		return nil
	}
	// FULL comes first because it is the only lasting fix: the export cannot
	// yet read the value back from the table it already wrote, so below FULL
	// every later change that leaves a large value untouched on a row exported
	// earlier lands here again. The reload gets past this one change only.
	return fmt.Errorf("cannot export %s.%s (pk=%s): column(s) %s kept an unchanged large value that PostgreSQL did not resend "+
		"(the table was not at REPLICA IDENTITY FULL when the change was captured), no earlier change of this row in the "+
		"export window holds it, and the export cannot read it back from the table it already wrote. The fix is ALTER TABLE %s "+
		"REPLICA IDENTITY FULL on the source; until then, every later change that leaves a large value untouched on a row "+
		"exported earlier is refused the same way. To get past this change once: %s",
		f.schema, f.table, ev.PKValues, strings.Join(missing, ", "), pgQuoted(f.schema, f.table), lowerFirst(f.reloadRemedy()))
}

// lowerFirst lowercases the first letter of an ASCII sentence, to splice it
// after a colon.
func lowerFirst(s string) string {
	if s == "" || s[0] < 'A' || s[0] > 'Z' {
		return s
	}
	return string(s[0]+('a'-'A')) + s[1:]
}

// markerMisplacedErr refuses a marker where the capture never writes one: the
// not-captured marker in a new row or in a DELETE's key, or (MySQL) either
// marker anywhere. The stored row does not hold the value to export.
func (f *fold) markerMisplacedErr(ev *query.ResultRow, cols []string) error {
	return fmt.Errorf("cannot export %s.%s (pk=%s): the stored change (event %d) marks column(s) %s as a value the index does not hold, "+
		"in a place the capture never writes that mark, so the value to export is unknown. %s",
		f.schema, f.table, ev.PKValues, ev.EventID, strings.Join(cols, ", "), f.reloadRemedy())
}

// reloadRemedy is the export's way past a change it cannot apply: the cursor
// lives in the table, so only a reload from a baseline past the change helps
// (the same remedy as a refused key change, #2619).
func (f *fold) reloadRemedy() string {
	return fmt.Sprintf("The table's cursor stays before this change and every run refuses it again; take a baseline at or after "+
		"the change and remove the table directory (or drop the table from the catalog) so the next run reloads %s.%s from it",
		f.schema, f.table)
}

// pgQuoted is "schema"."table" with each PostgreSQL identifier double-quoted
// (quotes inside doubled), so the printed command is right for every name.
func pgQuoted(schema, table string) string {
	q := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
	if schema == "" {
		return q(table)
	}
	return q(schema) + "." + q(table)
}

// touched returns the net ops in first-touched order.
func (f *fold) touched() []*netOp {
	out := make([]*netOp, 0, len(f.order))
	for _, k := range f.order {
		out = append(out, f.ops[k])
	}
	return out
}

// pkImage extracts the primary key columns of a row image. A key column
// absent from the image is refused: a NULL in an equality-delete key matches
// nothing, and the old row would survive beside the new one.
func pkImage(row map[string]any, pkCols []metadata.ColumnMeta) (map[string]any, error) {
	out := make(map[string]any, len(pkCols))
	for _, c := range pkCols {
		v, ok := row[c.Name]
		if !ok {
			if v, ok = lookupFold(row, c.Name); !ok {
				return nil, fmt.Errorf("primary key column %q is absent from the row image", c.Name)
			}
		}
		out[c.Name] = v
	}
	return out, nil
}

func copyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
