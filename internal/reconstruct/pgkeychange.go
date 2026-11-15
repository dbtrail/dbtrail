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
//     Under a weaker identity PostgreSQL sends a key-only old row for a key
//     change, with every other column NULL, and that has the same columns as
//     a whole row whose other columns really are NULL. The two cannot be told
//     apart from the event, so an old row with no value outside the key is
//     taken as whole only when it cannot matter (pgOldRowMayBeKeyOnly). That
//     the old row is whole is therefore capture's gate plus these checks of
//     shape, not a proof from the event alone.
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
// One thing the event alone cannot show is checked by the fold, over the
// transaction: pgKeyTxn.
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
	if col := pgOldRowMayBeKeyOnly(ev, pkCols); col != "" {
		return nil, refuse("the old row holds no value outside the key, so it cannot be told from one captured with the key only, " +
			"and the new row's column " + col + " is NULL: with a key-only old row, an unchanged large value is stored as NULL")
	}
	if before != ev.PKValues {
		return nil, refuse(fmt.Sprintf("the index stores the old key as %q but its row image spells it %q", ev.PKValues, before))
	}

	del, ins := *ev, *ev
	del.EventType, del.PKValues, del.RowAfter, del.ChangedColumns = event.EventDelete, ev.PKValues, nil, nil
	ins.EventType, ins.PKValues, ins.RowBefore, ins.ChangedColumns = event.EventInsert, after, nil, nil
	return []query.ResultRow{del, ins}, nil
}

// pgOldRowMayBeKeyOnly names a column of ev's after-image that cannot be
// trusted because the before-image may be a key-only old row, or "" when
// there is none.
//
// Capture fills an unchanged large value in the new row from the old row of
// the same change (pgcapture decodeUpdate). From a key-only old row that
// value is NULL, stored as if it were the column's value. Capture refuses
// the tables that send key-only old rows, so no such event is expected in an
// index; this check is a second belt for that one shape.
//
// It is not a proof that the old row is whole, and nothing in the event can
// be one: that rests on capture's gate (REPLICA IDENTITY FULL, checked at
// startup and on every relation message). What this does: an old row with a
// value outside the key is taken as whole, because a key-only one has none.
// (Under REPLICA IDENTITY USING INDEX on a unique index wider than the
// primary key the old row holds that index's columns and nothing else, and
// passes here; only capture's gate keeps it out.) An old row with no value
// outside the key may be key-only. Then a NULL outside the key in the new row
// may be a large value that was lost, and the first such column (by name) is
// returned; a new row with no NULL outside the key holds only values
// PostgreSQL sent, whatever the old row was.
func pgOldRowMayBeKeyOnly(ev *query.ResultRow, pkCols []metadata.ColumnMeta) string {
	isKey := make(map[string]bool, len(pkCols))
	for _, c := range pkCols {
		isKey[c.Name] = true
	}
	for col, v := range ev.RowBefore {
		if !isKey[col] && v != nil {
			return ""
		}
	}
	suspect := ""
	for col, v := range ev.RowAfter {
		if !isKey[col] && v == nil && (suspect == "" || col < suspect) {
			suspect = col
		}
	}
	return suspect
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

// pgKeyTxn is what the fold remembers, per key, about the transaction that
// holds the key's last change: enough to refuse the one thing a split cannot
// get right (#2760).
//
// A split assumes that a key belongs to one row at every instant. Between
// transactions that is true of any primary key. Inside ONE transaction it is
// not when the key is DEFERRABLE: uniqueness is then checked at the end of
// the statement or of the transaction, and two rows can hold one key value in
// between. `UPDATE t SET id = id + 1` over rows 1, 2, 3 sends, at one commit
// LSN and in this order, 1→2, 2→3, 3→4: three rows, each moved once. A
// non-deferrable key sending 1→2 then 2→3 is ONE row moved twice. The events
// are the same, and the answers are not: the first leaves rows at 2, 3 and
// 4, the second a row at 3 only. Folding the first as the second loses rows.
//
// The fold records no fact that tells the two apart (a recorded table shape
// does not hold whether its key is deferrable), so it refuses every sequence
// that could be either: inside one transaction, a key that RECEIVES a row
// (an INSERT, or the INSERT half of a key change) and is changed again by a
// later event of that transaction, when a key change is among the events of
// that key. Then the key held a row that arrived and something else happened
// to "the row at that key", which with a deferrable key may be another row.
//
// What stays allowed is every sequence where the arrival, if any, is the
// key's last event in the transaction: any number of changes of the row that
// was there, its departure, and then a row arriving. There the key never
// holds two rows, deferrable or not. That includes the order a non-deferrable
// key needs for `SET id = id + 1` to succeed at all (3→4, 2→3, 1→2: each key
// loses its row, then receives one). A sequence with no key change in it is
// left as it always was.
//
// The price: one row whose key changes twice in one transaction, or a row
// inserted and then re-keyed in the same transaction, is refused too.
//
// "Later" is by event id, the order inside one LSN (laterInWAL). The record
// is kept beside the change map and travels with a change to disk
// (spillRecord.Txn), so events of one transaction that land on different
// pages, or on both sides of a drain, are still seen together.
type pgKeyTxn struct {
	// LSN is the transaction's commit LSN; 0 is no record.
	LSN uint64
	// MaxID is the highest event id of the transaction seen at this key.
	MaxID uint64
	// MinArrival is the lowest event id of an INSERT or INSERT half at this
	// key in the transaction, 0 when there is none.
	MinArrival uint64
	// Half reports that a half of a key change is among the events.
	Half bool
	// From and To are the keys of one such key change, for the refusal.
	From, To string
}

// pgKeyTxnOf is the record of one filed row: ev itself, or one half of it
// (half, with the key change's two keys). ok is false for a row with no
// commit LSN, which belongs to no known transaction.
func pgKeyTxnOf(ev *query.ResultRow, half bool, from, to string) (pgKeyTxn, bool) {
	lsn, ok := pgRowLSN(ev)
	if !ok {
		return pgKeyTxn{}, false
	}
	t := pgKeyTxn{LSN: lsn, MaxID: ev.EventID, Half: half}
	if ev.EventType == event.EventInsert {
		t.MinArrival = ev.EventID
	}
	if half {
		t.From, t.To = from, to
	}
	return t, true
}

// with is t after one more record of the same key: a later transaction
// replaces it, an earlier one is dropped (its result is overwritten whole by
// the later transaction's), the same transaction is added up. The order the
// records come in does not matter.
func (t pgKeyTxn) with(n pgKeyTxn) pgKeyTxn {
	switch {
	case n.LSN > t.LSN:
		return n
	case n.LSN < t.LSN:
		return t
	}
	t.MaxID = max(t.MaxID, n.MaxID)
	if n.MinArrival != 0 && (t.MinArrival == 0 || n.MinArrival < t.MinArrival) {
		t.MinArrival = n.MinArrival
	}
	if n.Half && !t.Half {
		t.Half, t.From, t.To = true, n.From, n.To
	}
	return t
}

// ambiguous reports the sequence pgKeyTxn's comment refuses: a key change
// among the key's events of the transaction, and a row arriving at the key
// before the key's last event.
func (t pgKeyTxn) ambiguous() bool {
	return t.Half && t.MinArrival != 0 && t.MinArrival < t.MaxID
}

// pgSameTxnErr is the refusal of an ambiguous sequence: the #782 text for
// the key change involved, then why.
func pgSameTxnErr(schema, table, key string, t pgKeyTxn) error {
	return pgKeyChangeErr(schema, table, t.From, t.To, fmt.Sprintf(
		"inside one transaction (commit LSN %s) the key %q received a row and was changed again afterwards. "+
			"When a primary key is DEFERRABLE two rows can hold the same key value inside a transaction, "+
			"and the captured changes do not say whether the second change is about the row that arrived or about another row that had that key",
		lsnText(t.LSN), key))
}

// notePGTxn records one filed row of a PostgreSQL fold under its key and
// refuses when the key's transaction became ambiguous (pgKeyTxn).
func (r *foldResult) notePGTxn(ev *query.ResultRow, half bool, from, to, schema, table string) error {
	n, ok := pgKeyTxnOf(ev, half, from, to)
	if !ok {
		return nil
	}
	if r.pgTxn == nil {
		r.pgTxn = map[string]pgKeyTxn{}
	}
	t := r.pgTxn[ev.PKValues].with(n)
	r.pgTxn[ev.PKValues] = t
	if t.ambiguous() {
		return pgSameTxnErr(schema, table, ev.PKValues, t)
	}
	return nil
}
