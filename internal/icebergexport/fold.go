package icebergexport

import (
	"fmt"

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
	// so a later page can tell whether its change of the key is the newer one.
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
	ops           map[string]*netOp
	order         []string
	events        int64
}

func newFold(schema, table string, pkCols []metadata.ColumnMeta) *fold {
	return &fold{schema: schema, table: table, pkCols: pkCols, ops: map[string]*netOp{}}
}

// addPage folds one page. The guards match the full-table reconstruct's:
// an unresolved TOAST marker (#592) and a PK-changing UPDATE (#782) refuse the
// table, because the alternative is a table that reads cleanly and is wrong.
func (f *fold) addPage(page []query.ResultRow) error {
	for i := range page {
		ev := &page[i]
		if err := event.CheckUnresolvedToast(ev.SchemaName, ev.TableName, ev.PKValues, ev.RowBefore, ev.RowAfter); err != nil {
			return err
		}
		if before, after, ok := reconstruct.PKChangedInEvent(ev, f.pkCols); ok {
			return reconstruct.PKChangingUpdateError(f.schema, f.table, before, after)
		}
		var op *netOp
		switch ev.EventType {
		case event.EventInsert, event.EventUpdate:
			if ev.RowAfter == nil {
				return fmt.Errorf("event %d (type %d) for %s.%s pk %q carries no after-image", ev.EventID, ev.EventType, f.schema, f.table, ev.PKValues)
			}
			row := copyMap(ev.RowAfter)
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
			op = &netOp{PK: pk}
		default:
			// EventType 0 is the drift shape (#318): a NULL event_type column.
			// Not skippable: a skipped event is a silently wrong table.
			return fmt.Errorf("event %d for %s.%s pk %q has event type %d, which the export cannot apply", ev.EventID, f.schema, f.table, ev.PKValues, ev.EventType)
		}
		f.events++
		op.at = query.ResultRow{BinlogFile: ev.BinlogFile, StartPos: ev.StartPos}
		cur, seen := f.ops[ev.PKValues]
		if !seen {
			f.order = append(f.order, ev.PKValues)
		} else if query.LaterInBinlog(&cur.at, &op.at) {
			// Every guard above has read this event; the key keeps the change
			// from further on in the binary log.
			continue
		}
		f.ops[ev.PKValues] = op
	}
	return nil
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
