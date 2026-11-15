package reconstruct

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A key-changing UPDATE in a PostgreSQL window (#2760).
//
// MySQL and MariaDB fold such an UPDATE as a DELETE of the old key and an
// INSERT of the new one since #2501 (splitPKChange). PostgreSQL was left out
// there for two reasons, and this file is what answers each:
//
//   - Its rows carry an LSN, which query.LaterInBinlog does not order.
//     laterInWAL orders them, and the fold asks it (laterChange).
//   - Its before-image may hold only the key. splitPGKeyChange checks the
//     images of each event and refuses the ones it cannot prove whole.

// pgRowLSN is the commit LSN of a row a PostgreSQL stream wrote, and whether
// r is such a row with a usable one. pgcapture stamps every row of a
// transaction with the commit LSN twice: as a number in start_pos and as
// text ("%X/%X") in binlog_file (internal/pgcapture/decoder.go, rowEvent).
// Both must be there and agree. A row with one and not the other, or with a
// zero LSN, was not written that way, and nothing is assumed about its place.
func pgRowLSN(r *query.ResultRow) (uint64, bool) {
	if r == nil || r.StartPos == 0 || r.BinlogFile != lsnText(r.StartPos) {
		return 0, false
	}
	return r.StartPos, true
}

// laterInWAL reports whether a is strictly after b in a PostgreSQL source's
// write-ahead log: by commit LSN, and inside one transaction (one LSN) by
// event_id.
//
// The commit LSN is the order transactions committed in. Two transactions
// that change the same row, or that give and take the same primary key value,
// cannot commit in the other order than they ran: the second one waits for
// the first one's commit (the row lock, or the unique index's check of a key
// another transaction holds uncommitted).
//
// Inside one transaction every row has the same LSN. pgoutput delivers a
// transaction's changes in the order they were made, capture indexes them in
// the order it receives them, and the index numbers rows as they are
// inserted, so the higher event_id is the later change. That is the rule
// query.LaterInBinlog already uses for two rows at one binary log coordinate.
//
// event_timestamp does not say this: it is the commit TIME, stored to the
// second, and PostgreSQL takes a commit's time before it writes the commit
// record, so two commits can carry times in the other order than their LSNs.
//
// When either row has no LSN (pgRowLSN) the answer is false both ways and the
// caller's arrival order stands, which is what a PostgreSQL fold did for
// every row before this rule.
func laterInWAL(a, b *query.ResultRow) bool {
	la, oka := pgRowLSN(a)
	lb, okb := pgRowLSN(b)
	if !oka || !okb {
		return false
	}
	if la == lb {
		return a.EventID > b.EventID
	}
	return la > lb
}

// laterChange reports whether a is strictly after b in the source's own log:
// the binary log for MySQL and MariaDB rows (query.LaterInBinlog, unchanged),
// the write-ahead log for PostgreSQL rows (laterInWAL). A row is one or the
// other by its coordinate: a binary log file name cannot hold a slash and an
// LSN's text always does, so for two MySQL rows laterInWAL is always false
// and for two PostgreSQL rows LaterInBinlog is.
func laterChange(a, b *query.ResultRow) bool {
	return query.LaterInBinlog(a, b) || laterInWAL(a, b)
}

// splitPGKeyChange is splitPKChange for a PostgreSQL window: nil for any
// event but an UPDATE whose primary key changed, and for that one a DELETE of
// the old key and an INSERT of the new one, both at the UPDATE's own LSN and
// event_id, so laterInWAL orders each half against every other change of its
// key. The two halves never meet each other: their keys differ.
//
// It splits when, and only when, each of these holds for THIS event, and
// returns the #782 refusal with the reason otherwise:
//
//  1. The event has a place in the log: a commit LSN (pgRowLSN) and an
//     event_id. Without them the halves could not be ordered against a later
//     change of either key.
//  2. The before-image is the whole old row: it holds exactly the columns the
//     after-image holds, and every key column in it is text. The DELETE half
//     needs only the key; the whole row is asked for because of what it
//     proves about the after-image (3). Capture refuses a table that is not
//     at REPLICA IDENTITY FULL (pgcapture: at startup and on every relation
//     message), which is the only way PostgreSQL sends the whole old row.
//  3. The after-image is the whole new row: every key column is text and no
//     value is the unchanged-TOAST marker. The INSERT half writes this image
//     as the row, so a marker would be written as data. Under REPLICA
//     IDENTITY FULL capture has already replaced an unchanged large value
//     with the real one from the before-image of the same event
//     (pgcapture decodeUpdate), so a marker here means the event was captured
//     without the whole old row and there is nothing to take the value from.
//  4. The spelling proof of #2501: the old key spelled from the decoded
//     before-image equals the key the index stores for the event, and both
//     keys are valid UTF-8 text. A PostgreSQL value is stored as PostgreSQL's
//     own text and read back as a Go string for every type (integer, numeric,
//     uuid, timestamptz, text), so the new key spelled from the after-image
//     is what a later change of that row carries as its stored key. The proof
//     is still checked per event: text that is not valid UTF-8 does not
//     survive the index's JSON, and would spell two ways.
//
// The halves share nothing the writer mutates, as in splitPKChange.
func splitPGKeyChange(ev *query.ResultRow, schema, table string, pkCols []metadata.ColumnMeta) ([]query.ResultRow, error) {
	before, after, changed := pkChangedInEvent(ev, pkCols)
	if !changed {
		return nil, nil
	}
	refuse := func(why string) error { return pgKeyChangeErr(schema, table, before, after, why) }

	if _, ok := pgRowLSN(ev); !ok {
		return nil, refuse(fmt.Sprintf("the change carries no commit LSN to order it by (position %q, %d)", ev.BinlogFile, ev.StartPos))
	}
	if ev.EventID == 0 {
		return nil, refuse("the change carries no event id to order it by inside its transaction")
	}
	if cols := event.UnresolvedToastColumns(ev.RowAfter); len(cols) > 0 {
		return nil, refuse("the new row holds an unchanged large value that was not captured (column " + strings.Join(cols, ", ") +
			"); the table was not at REPLICA IDENTITY FULL when the change was captured")
	}
	if cols := event.UnresolvedToastColumns(ev.RowBefore); len(cols) > 0 {
		return nil, refuse("the old row holds an unchanged large value that was not captured (column " + strings.Join(cols, ", ") + ")")
	}
	for _, c := range pkCols {
		if why := pgKeyValueUnusable(ev.RowBefore, c.Name); why != "" {
			return nil, refuse("the old row's key column " + c.Name + " " + why)
		}
		if why := pgKeyValueUnusable(ev.RowAfter, c.Name); why != "" {
			return nil, refuse("the new row's key column " + c.Name + " " + why)
		}
	}
	if len(ev.RowBefore) != len(ev.RowAfter) {
		return nil, refuse(fmt.Sprintf("the old row holds %d columns and the new row %d, so the old row was not captured whole", len(ev.RowBefore), len(ev.RowAfter)))
	}
	for col := range ev.RowAfter {
		if _, ok := ev.RowBefore[col]; !ok {
			return nil, refuse("the old row has no column " + col + ", so it was not captured whole")
		}
	}
	if before != ev.PKValues {
		return nil, refuse(fmt.Sprintf("the index stores the old key as %q but its row image spells it %q", ev.PKValues, before))
	}

	del, ins := *ev, *ev
	del.EventType, del.PKValues, del.RowAfter, del.ChangedColumns = event.EventDelete, ev.PKValues, nil, nil
	ins.EventType, ins.PKValues, ins.RowBefore, ins.ChangedColumns = event.EventInsert, after, nil, nil
	return []query.ResultRow{del, ins}, nil
}

// pgKeyValueUnusable says why row's value for the key column col cannot
// spell a key, or "" when it can: it must be there, and be valid UTF-8 text.
// A PostgreSQL image holds text for every value and nil for NULL; a primary
// key column is never NULL, so nil here is a column the image lacks.
func pgKeyValueUnusable(row map[string]any, col string) string {
	v, ok := row[col]
	if !ok || v == nil {
		return "is missing from the image"
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("is not text in the image (%T)", v)
	}
	if !utf8.ValidString(s) {
		return "is text that is not valid UTF-8"
	}
	return ""
}

// pgKeyChangeErr is the #782 refusal, its text unchanged, followed by why
// this PostgreSQL event was not applied as a delete plus an insert (#2760).
func pgKeyChangeErr(schema, table, before, after, why string) error {
	return fmt.Errorf("%s. On PostgreSQL such a change is applied as a delete of the old key and an insert of the new one "+
		"when the change can be proven whole, and this one cannot: %s. A new snapshot of the table (`bintrail-pg baseline`, "+
		"or a full backup from the web interface) starts after the change",
		pkChangingUpdateErr(schema, table, before, after).Error(), why)
}
