package reconstruct

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
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
	// An old row that SAYS it was captured with its key only holds the
	// not-captured marker outside the key, which is a value here: that
	// capture stores a large value that was not sent as its own marker in
	// the new row, never as NULL, so a NULL there is a NULL.
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
// loses its row, then receives one).
//
// The price: one row whose key changes twice in one transaction, or a row
// inserted and then re-keyed in the same transaction, is refused too.
//
// # The same, with no key change among the events
//
// A deferrable key lets two rows share a key value inside a transaction
// without any key change: `INSERT INTO t VALUES (1, 'new')` while the old
// row 1 is still there, then a DELETE of the OLD row. The events are
// INSERT(1) then DELETE(1) at one commit LSN; the fold keeps the last one,
// and the copy has no row 1 where the source has the new one.
//
// Here the events do carry what tells the cases apart, because every image
// is a whole row (REPLICA IDENTITY FULL): the changes of one key in one
// transaction describe ONE row exactly when each change starts from what the
// change before it left. An INSERT starts from no row; an UPDATE and a DELETE
// start from their before-image; an INSERT and an UPDATE leave their
// after-image, a DELETE leaves no row. The record keeps what the key's first
// change started from and left, and what its last change left (pgRowState: a
// hash of the image), and every further change is linked to it (pgRowLink).
// A link that does not hold refuses: a row inserted where one was left, a row
// changed or deleted where none was left, a before-image that is not the row
// the change before it left, and an image that cannot be compared (no
// before-image, a value that is not text).
//
// What is ordinary stays applied, because there every link holds: insert a
// row and delete it, insert and update it, delete a row and insert another
// under its key, update twice.
//
// One exception to a link that does not hold: the transaction delivered
// again. Capture saves its position after it commits rows to the index, so a
// crash between the two makes PostgreSQL send whole transactions a second
// time (pgstreamrun, "indexed twice after a crash"), and one cut at the
// batch size can leave its beginning behind. What the index then holds for
// the key is its changes up to some point, and then all of them again from
// the first.
//
// A second delivery is accepted only as that, change by change. A change
// that does not link but equals the key's first change of the transaction
// (same start, same result) OPENS a repeat: the changes so far are the run
// to repeat (RefLen of them, with their digest Ref), and a new run starts.
// The new run must itself link, and when it reaches the length of the run
// before it, the two digests must be equal: every change of the second run
// equals the change at the same place in the first. Only then does the
// sequence go on, from the state both runs reached. Anything else refuses:
// a second run that differs anywhere (INSERT a, UPDATE to b, INSERT a,
// DELETE a is not a transaction delivered twice: the second run's second
// change is a DELETE where the first run's was an UPDATE, and the source
// keeps the row b), a first change that comes back while a repeat is still
// open, and a repeat still open when the key's changes end (a delivery
// whose last run is shorter than the one before it; the last delivery of a
// transaction the fold reads is always whole, since the fold stops at
// capture's saved position).
//
// The repeat is checked while the changes of one key are folded one by one,
// in event id order. Records put together another way (changes of one
// transaction held on both sides of a move to disk, or reaching the fold
// against event id order) are linked end to start, and a first change coming
// back across that seam is refused, not checked: the run's digest is not
// carried over the seam (DigestLost). That refuses a legitimate second
// delivery split by a move to disk, which is the safe direction.
//
// What this cannot see is rows that are equal in EVERY column, key included:
//
//   - an exact copy of a row that already existed is inserted and the
//     original deleted: change for change, a row inserted and deleted again;
//   - the same row is inserted twice and one of the two deleted (INSERT a,
//     INSERT a, DELETE a): change for change, a transaction cut after its
//     first change and then delivered whole. The source keeps one row, the
//     copy none. The same holds for any series of changes made identically
//     to two identical rows.
//
// Only the catalog knows the key is deferrable, and the index does not
// record that. And on a database whose encoding is SQL_ASCII, text that is
// not valid UTF-8 is stored with replacement characters, so two rows that
// differ only in such bytes compare equal.
//
// This rule runs for every change of a PostgreSQL fold, halves included. The
// rule above stays beside it for sequences with a key change: there two rows
// that differ only in the key (1,'a') and (2,'a') make "1 to 2, then 2 to 3"
// link perfectly while being two rows.
//
// "Later" is by event id, the order inside one LSN (laterInWAL). The record
// is kept beside the change map and travels with a change to disk
// (spillRecord.Txn), so events of one transaction that land on different
// pages, or on both sides of a drain, are still seen together. Changes of
// one transaction reach the fold in event id order (one commit time, and the
// fetch is ordered by time and id); records are put together by their id
// ranges, so any other order ends in the same answer or, when a change falls
// inside a range already linked, in a refusal, never in a link skipped.
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

	// FirstID is the lowest event id of the transaction seen at this key.
	FirstID uint64
	// FirstNeeds and FirstLeaves are what the change with FirstID started
	// from and left; Leaves is what the change with MaxID left.
	FirstNeeds, FirstLeaves, Leaves pgRowState
	// Why is set when the changes do not describe one row: the reason, in
	// words for the refusal.
	Why string

	// The current run: the changes since the key's first change of the
	// transaction, or since that first change last came back. RunLen counts
	// them and Run is their digest, one change after the other.
	RunLen uint32
	Run    pgRunDigest
	// The run to repeat while a repeat is open (RefLen > 0): the run before
	// the current one. See pgKeyTxn's comment.
	RefLen uint32
	Ref    pgRunDigest
	// Restarted reports that the first change came back at least once.
	Restarted bool
	// DigestLost reports that Run does not cover the current run (records
	// were put together across a seam), so a repeat cannot be checked.
	DigestLost bool
}

// pgRunDigest is the digest of a run of changes, in order: each change's
// start and result folded into the digest of the changes before it.
type pgRunDigest struct{ Hi, Lo uint64 }

// then is d after one more change, which started from needs and left leaves.
func (d pgRunDigest) then(needs, leaves pgRowState) pgRunDigest {
	var b [16 + 2*17]byte
	binary.BigEndian.PutUint64(b[0:], d.Hi)
	binary.BigEndian.PutUint64(b[8:], d.Lo)
	for i, s := range []pgRowState{needs, leaves} {
		o := 16 + i*17
		b[o] = s.Kind
		binary.BigEndian.PutUint64(b[o+1:], s.Hi)
		binary.BigEndian.PutUint64(b[o+9:], s.Lo)
	}
	sum := sha256.Sum256(b[:])
	return pgRunDigest{Hi: binary.BigEndian.Uint64(sum[:8]), Lo: binary.BigEndian.Uint64(sum[8:16])}
}

const (
	pgWhyNotARepeat = "its first change appears a second time, as when capture delivers a transaction twice, " +
		"but the changes after it do not repeat the changes before it"
	pgWhyRepeatOpen = "its first change appears a second time, as when capture delivers a transaction twice, " +
		"and the changes after it stop before they repeat the changes before it"
	pgWhyRepeatSeam = "its first change appears a second time, as when capture delivers a transaction twice, " +
		"and that cannot be checked here: the changes of the transaction were put together from more than one place (a move to disk)"
)

// pgRowState is what a key holds before or after one change, as far as the
// change's images say: no row, a row (by a hash of its whole image), or
// something that cannot be compared.
type pgRowState struct {
	Kind   uint8
	Hi, Lo uint64
}

const (
	pgStateNone    uint8 = iota + 1 // no row
	pgStateRow                      // a row; Hi and Lo are its hash
	pgStateUnknown                  // an image that cannot be compared
	// pgStateRowAny is a row whose contents are not all known: an earlier
	// row captured with its key only (event.NotCapturedKey in its other
	// columns), or a row left with a large value that was not sent
	// (event.UnchangedToastKey). It says a row was there, and not which. After the others,
	// which are written to disk by their number (pgRunDigest).
	pgStateRowAny
)

// pgImageState is the state a whole row image stands for. Every value of a
// PostgreSQL image is PostgreSQL's own text, or nil for NULL; an image with
// any other value (the unchanged-TOAST marker, a number) and a missing image
// cannot be compared exactly and are pgStateUnknown. The hash is over the
// column names in order with each value's kind and length, so no two images
// share an encoding: 128 bits of SHA-256.
func pgImageState(img map[string]any) pgRowState {
	if img == nil {
		return pgRowState{Kind: pgStateUnknown}
	}
	cols := make([]string, 0, len(img))
	for c := range img {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	h := sha256.New()
	var n [binary.MaxVarintLen64 + 1]byte
	put := func(tag byte, s string) {
		n[0] = tag
		h.Write(n[:1+binary.PutUvarint(n[1:], uint64(len(s)))])
		h.Write([]byte(s))
	}
	partial := false
	for _, c := range cols {
		put('c', c)
		switch v := img[c].(type) {
		case nil:
			put('n', "")
		case string:
			put('s', v)
		default:
			// A column that was not sent, or a large value left as it was:
			// each its own tag, so the image never hashes as one that holds
			// a NULL or an empty text there.
			switch {
			case event.IsNotCapturedMarker(v):
				put('x', "")
			case event.IsUnchangedToastMarker(v):
				put('u', "")
			default:
				return pgRowState{Kind: pgStateUnknown}
			}
			partial = true
		}
	}
	sum := h.Sum(nil)
	kind := pgStateRow
	if partial {
		kind = pgStateRowAny
	}
	return pgRowState{Kind: kind, Hi: binary.BigEndian.Uint64(sum[:8]), Lo: binary.BigEndian.Uint64(sum[8:16])}
}

// pgRowLink says why a change that starts from needs cannot follow one that
// left leaves, or "" when it can: both are no row, or both are the same row.
func pgRowLink(leaves, needs pgRowState) string {
	switch {
	case leaves.Kind == pgStateUnknown || needs.Kind == pgStateUnknown ||
		leaves.Kind == 0 || needs.Kind == 0:
		return "two changes of it follow each other and their row images cannot be compared (an image is missing or holds a value that is not text)"
	case leaves.Kind == pgStateNone && needs.Kind == pgStateNone:
		return ""
	case pgStateIsRow(leaves) && needs.Kind == pgStateNone:
		return "a row is inserted where the change before it had left a row"
	case leaves.Kind == pgStateNone && pgStateIsRow(needs):
		return "a row is changed or deleted where the change before it had left no row"
	case leaves.Kind == pgStateRowAny || needs.Kind == pgStateRowAny:
		// A row on both sides, and one of them known by its key alone
		// (a table below REPLICA IDENTITY FULL): there is nothing to
		// compare the other with. An INSERT followed by an UPDATE in one
		// transaction is the ordinary case. Two things are given up, for
		// such a table only:
		//   - that the row a change started from is the row the change
		//     before it left, which is what showed a deferrable key
		//     (pgKeyTxn). Capture refuses a table with one when it starts
		//     (pgcapture.CheckDeferrableKeys), not one made so while it
		//     runs;
		//   - a transaction delivered a second time whose changes of the
		//     key are all UPDATEs is not seen as one: its first change
		//     links like any other. The whole delivery ends in the same
		//     row; a delivery cut short would not, and capture leaves none
		//     in the index since it deletes, when it resumes, what it had
		//     indexed past its saved position (pgstreamrun), which is
		//     also as far as a fold or a tail is fed.
		return ""
	case leaves == needs:
		return ""
	}
	return "a change or a delete starts from a row that is not the row the change before it left"
}

// pgStateIsRow: a row is there, known whole or by its key alone.
func pgStateIsRow(s pgRowState) bool { return s.Kind == pgStateRow || s.Kind == pgStateRowAny }

// pgKeyTxnOf is the record of one filed row: ev itself, or one half of it
// (half, with the key change's two keys). ok is false for a row with no
// commit LSN, which belongs to no known transaction.
func pgKeyTxnOf(ev *query.ResultRow, half bool, from, to string) (pgKeyTxn, bool) {
	lsn, ok := pgRowLSN(ev)
	if !ok {
		return pgKeyTxn{}, false
	}
	t := pgKeyTxn{LSN: lsn, MaxID: ev.EventID, FirstID: ev.EventID, Half: half}
	needs, leaves := pgRowState{Kind: pgStateUnknown}, pgRowState{Kind: pgStateUnknown}
	switch ev.EventType {
	case event.EventInsert:
		t.MinArrival = ev.EventID
		needs, leaves = pgRowState{Kind: pgStateNone}, pgImageState(ev.RowAfter)
	case event.EventUpdate:
		needs, leaves = pgImageState(ev.RowBefore), pgImageState(ev.RowAfter)
	case event.EventDelete:
		needs, leaves = pgImageState(ev.RowBefore), pgRowState{Kind: pgStateNone}
	}
	t.FirstNeeds, t.FirstLeaves, t.Leaves = needs, leaves, leaves
	t.RunLen, t.Run = 1, pgRunDigest{}.then(needs, leaves)
	if half {
		t.From, t.To = from, to
	}
	return t, true
}

// with is t after one more record of the same key: a later transaction
// replaces it, an earlier one is dropped (its result is overwritten whole by
// the later transaction's), the same transaction is added up: the two
// records are two runs of event ids, linked where the earlier run ends and
// the later one starts (pgKeyTxn says how, and what a run that starts like
// the transaction's first change means). Runs that overlap cannot be linked
// and set Why.
func (t pgKeyTxn) with(n pgKeyTxn) pgKeyTxn {
	switch {
	case n.LSN > t.LSN:
		return n
	case n.LSN < t.LSN:
		return t
	}
	a, b := t, n // a is the earlier run
	if n.MaxID < t.FirstID {
		a, b = n, t
	}
	// What is not about the chain is kept from t, as before.
	arrival, half, from, to := t.MinArrival, t.Half, t.From, t.To
	maxID := max(t.MaxID, n.MaxID)
	switch {
	case t.Why != "":
		a.Why = t.Why
	case n.Why != "":
		a.Why = n.Why
	case a.MaxID >= b.FirstID:
		a.Why = "its changes cannot be put in the order they were made in (two of them carry event ids that overlap)"
	case b.FirstID == b.MaxID && b.RunLen == 1 && !b.Restarted && !a.DigestLost:
		a = a.step(b)
	default:
		a = a.join(b)
	}
	t = a
	t.MinArrival, t.Half, t.From, t.To = arrival, half, from, to
	t.MaxID = maxID
	if n.MinArrival != 0 && (t.MinArrival == 0 || n.MinArrival < t.MinArrival) {
		t.MinArrival = n.MinArrival
	}
	if n.Half && !t.Half {
		t.Half, t.From, t.To = true, n.From, n.To
	}
	return t
}

// step is a after the single change e, the next one by event id: the exact
// rule of pgKeyTxn's comment, the repeat of a transaction delivered twice
// included.
func (a pgKeyTxn) step(e pgKeyTxn) pgKeyTxn {
	why := pgRowLink(a.Leaves, e.FirstNeeds)
	switch {
	case why == "":
		a.RunLen++
		a.Run = a.Run.then(e.FirstNeeds, e.FirstLeaves)
		if a.RefLen > 0 && a.RunLen == a.RefLen {
			// The second run is as long as the first: it must be the first.
			if a.Run != a.Ref {
				a.Why = pgWhyNotARepeat
			}
			a.RefLen, a.Ref = 0, pgRunDigest{}
		}
	case e.FirstNeeds != a.FirstNeeds || e.FirstLeaves != a.FirstLeaves:
		a.Why = why
	case a.RefLen > 0:
		// The first change again, while the run before it is not repeated yet.
		a.Why = pgWhyRepeatOpen
	default:
		// The first change again: the run so far is the one to repeat.
		a.Restarted = true
		a.RefLen, a.Ref = a.RunLen, a.Run
		a.RunLen, a.Run = 1, e.Run
		if a.RefLen == 1 {
			if a.Run != a.Ref {
				a.Why = pgWhyNotARepeat
			}
			a.RefLen, a.Ref = 0, pgRunDigest{}
		}
	}
	a.Leaves, a.MaxID = e.Leaves, e.MaxID
	return a
}

// join is a followed by the record b of later changes, when b is more than
// one change or a's digest is lost: the two are linked end to start, and
// nothing about a repeat can be checked across the seam, so a repeat open on
// either side, a first change that comes back at the seam, and one that came
// back inside b (judged there against b's own first change, which is not the
// transaction's) are refused.
func (a pgKeyTxn) join(b pgKeyTxn) pgKeyTxn {
	why := pgRowLink(a.Leaves, b.FirstNeeds)
	switch {
	case a.RefLen > 0 || b.RefLen > 0 || b.Restarted:
		a.Why = pgWhyRepeatSeam
	case why != "" && b.FirstNeeds == a.FirstNeeds && b.FirstLeaves == a.FirstLeaves:
		a.Why = pgWhyRepeatSeam
	case why != "":
		a.Why = why
	}
	a.RunLen += b.RunLen
	a.DigestLost = true
	a.Leaves, a.MaxID = b.Leaves, b.MaxID
	return a
}

// ambiguous reports a sequence pgKeyTxn's comment refuses: a key change
// among the key's events of the transaction and a row arriving at the key
// before the key's last event (movedTwice), or changes that do not describe
// one row (Why).
func (t pgKeyTxn) ambiguous() bool {
	return t.movedTwice() || t.Why != ""
}

func (t pgKeyTxn) movedTwice() bool {
	return t.Half && t.MinArrival != 0 && t.MinArrival < t.MaxID
}

// pgSameTxnErr is the refusal of an ambiguous sequence: the #782 text for
// the key change involved, then why.
func pgSameTxnErr(schema, table, key string, t pgKeyTxn) error {
	if !t.movedTwice() {
		// No key change to name: the changes of one key do not describe one
		// row. The reason comes first, for a page that shows the start of a
		// message.
		return fmt.Errorf("full-table reconstruct: %s.%s: inside one transaction (commit LSN %s) the changes of primary key %q do not describe one row: %s. "+
			"When a primary key is DEFERRABLE two rows can hold the same key value inside a transaction, "+
			"and the captured changes do not say which of them is left at the end. "+
			"A new snapshot of the table (`bintrail-pg baseline`, or a full backup from the web interface) starts after that transaction",
			schema, table, lsnText(t.LSN), key, t.Why)
	}
	return pgKeyChangeErr(schema, table, t.From, t.To, fmt.Sprintf(
		"inside one transaction (commit LSN %s) the key %q received a row and was changed again afterwards. "+
			"When a primary key is DEFERRABLE two rows can hold the same key value inside a transaction, "+
			"and the captured changes do not say whether the second change is about the row that arrived or about another row that had that key",
		lsnText(t.LSN), key))
}

// pgTxnEnd refuses a key whose transaction ends with a repeat still open: its
// first change came back and the changes after it stop before repeating the
// run before it (pgKeyTxn). Called once the window is folded, for the
// records still in memory; the ones that went to disk are checked when their
// group is read back.
func (r *foldResult) pgTxnEnd(schema, table string) error {
	if r.pgUSeen && r.Spill != nil {
		return pgToastSpillErr(schema, table)
	}
	return pgTxnOpenRepeat(r.pgTxn, schema, table)
}

// pgTxnOpenRepeat is the refusal for the first key of txn, in key order,
// whose record has a repeat open, or nil.
func pgTxnOpenRepeat(txn map[string]pgKeyTxn, schema, table string) error {
	first, found := "", false
	for k, t := range txn {
		if t.RefLen > 0 && (!found || k < first) {
			first, found = k, true
		}
	}
	if !found {
		return nil
	}
	t := txn[first]
	t.Why = pgWhyRepeatOpen
	return pgSameTxnErr(schema, table, first, t)
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
