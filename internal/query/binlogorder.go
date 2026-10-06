package query

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
)

// BinlogOrderReason says what OrderByBinlog decided about a set of rows.
type BinlogOrderReason int

const (
	// OrderAgrees: nothing to decide. Fewer than two rows, or binlog order and
	// the order the rows came in are the same order.
	OrderAgrees BinlogOrderReason = iota
	// OrderPostgres: every row carries a PostgreSQL LSN. Its event_timestamp is
	// the commit time, so the order the rows came in is commit order already.
	OrderPostgres
	// OrderSorted: the rows were put in binlog order, which differs from the
	// order they came in.
	OrderSorted
	// The four below keep the order the rows came in, and carry a warning:
	// binlog order could not be established, or disagrees and cannot be trusted.

	// OrderNoCoordinate: a row has no usable (file, position).
	OrderNoCoordinate
	// OrderSeveralBaseNames: the rows come from binary logs with different
	// base names, and positions of different logs do not compare.
	OrderSeveralBaseNames
	// OrderIDsUnproven: the index cannot show that its ids follow the binary
	// log, so a restart of the numbering inside the set cannot be ruled out.
	OrderIDsUnproven
	// OrderRenumbered: the position goes DOWN as the index's ids go up.
	OrderRenumbered
)

// IDProof is what an index can say about its own event_ids for a set of rows:
// whether ascending id is the order the source's binary log holds them in.
type IDProof int

const (
	// IDsUnproven: ids say nothing. A stream and `bintrail index` both wrote
	// these rows, or the index could not be asked.
	IDsUnproven IDProof = iota
	// IDsFollowStream: one stream wrote these rows, in the order it read them
	// from the source. Ascending id IS binary log order, across file
	// rotations and across a restart of the source's numbering.
	IDsFollowStream
	// IDsFollowFileIndexing: only `bintrail index` wrote this index. Ids
	// ascend in the order the FILES WERE INDEXED, which the operator chose:
	// `--all` takes a directory's files by name, `--files` in any order, over
	// any number of runs. Inside one file that is binary log order. Across
	// files it is not shown to be, and index_state cannot show it: it records
	// which file was indexed when, and a directory holding the old source's
	// binlog.000045 and the new source's binlog.000001 (a failover, a RESET
	// MASTER) is indexed 000001 first, in ascending name order, by one run,
	// with ids and positions rising together and both backwards.
	IDsFollowFileIndexing
)

// BinlogOrder is OrderByBinlog's answer.
type BinlogOrder struct {
	Reason BinlogOrderReason
	// Total is how many rows were looked at.
	Total int
	// Moved is how many rows sit at a different place in binlog order than in
	// the order they came in. Set for OrderSorted and for the three refusals
	// that had coordinates to compare.
	Moved int
	// Missing is how many rows have no usable coordinate (OrderNoCoordinate).
	Missing int
	// detail names the evidence of a refusal, for the warning.
	detail string
}

// Sorted reports whether the rows were reordered.
func (o BinlogOrder) Sorted() bool { return o.Reason == OrderSorted }

// Note is the text for the reader of a result whose rows WERE reordered, and
// "" otherwise: the times of consecutive changes are then not all ascending,
// which reads as a defect to someone who was not told.
func (o BinlogOrder) Note() string {
	if !o.Sorted() {
		return ""
	}
	return fmt.Sprintf("%d of %d changes were written to the binary log in a different order than their statements started "+
		"(a statement that waited for a row lock, or ran long). They are in binary log order, which is commit order, so their times are not all in sequence.",
		o.Moved, o.Total)
}

// statementTimeTail is what every refusal ends with: what order was kept and
// what it means for the reader.
const statementTimeTail = "The changes are in the order their statements started, as before. " +
	"That order is wrong for a row that two sessions changed close together when one of them waited for the other: " +
	"check such rows by hand, or narrow the time range."

// Warning is the text for the person reading the result when binlog order
// could not be used, and "" when there is nothing to say. It is one paragraph
// of plain text; the values it quotes come from the index and are not
// sanitized for any output format.
func (o BinlogOrder) Warning() string {
	switch o.Reason {
	case OrderNoCoordinate:
		return fmt.Sprintf("%d of %d changes carry no binary log position (they were indexed by an older build, or mixed with changes of another kind of source), "+
			"so the order of these changes cannot be taken from the binary log. %s", o.Missing, o.Total, statementTimeTail)
	case OrderSeveralBaseNames:
		return fmt.Sprintf("the binary log and the statement times disagree on the order of %d of %d changes, and the changes come from binary logs with different names (%s): "+
			"the source's binary log was renamed or the source was replaced inside this time range, and positions of different logs cannot be compared. %s",
			o.Moved, o.Total, o.detail, statementTimeTail)
	case OrderIDsUnproven:
		return fmt.Sprintf("the binary log and the statement times disagree on the order of %d of %d changes, and this index cannot show which one is right: %s. "+
			"A restart of the source's binary log numbering (a failover, RESET MASTER) inside this time range cannot be ruled out. %s",
			o.Moved, o.Total, o.detail, statementTimeTail)
	case OrderRenumbered:
		return fmt.Sprintf("the binary log and the statement times disagree on the order of %d of %d changes, and the binary log position goes down inside this time range (%s): "+
			"the source's binary log numbering restarted (a failover, RESET MASTER), or binary log files were indexed out of order. %s",
			o.Moved, o.Total, o.detail, statementTimeTail)
	}
	return ""
}

// OrderByBinlog puts rows in the order the source's binary log holds them,
// which is commit order, when that order can be established for the WHOLE set,
// and leaves them as they came otherwise. It is the rule of LaterInBinlog
// (#2151) for a caller that needs one order over many rows, not the later of
// two changes of one row (#2156): `recover`, whose script undoes the changes
// of every row and table of a window from the last one back, and the
// single-row reconstruct, which applies one row's changes from the first on.
//
// rows must be in the order every fetch returns, (event_timestamp, event_id)
// ascending. event_timestamp is the time a change's STATEMENT STARTED, so that
// order puts a change that waited on a row lock BEFORE the change it waited
// for. Undoing in that order leaves the row on the first session's value.
//
// # Why this is not a sort with LaterInBinlog
//
// LaterInBinlog answers false both ways when either row has no coordinate.
// Over a mixed set that is not an ordering (a < b and b < c say nothing about
// a and c when b has no coordinate), and a sort given such a comparison
// misorders without failing. So the sort here runs only when EVERY row has a
// coordinate, on the tuple (length of file name, file name, start position,
// event_id): a lexicographic order over four totally ordered fields, with
// event_id unique in a merged set. For two rows with coordinates it is exactly
// LaterInBinlog.
//
// # Why position order is not always taken
//
// Position order is binlog order inside ONE numbering of the source's files.
// A recover or a single-row reconstruct is bounded by TIME, so its rows can
// span a failover, a RESET MASTER or a change of the binary log base name.
// There position order is wrong for every row and time order was right, and
// the reader applies a reversal script to a database. So the position order is
// used only when the index's own ids confirm it:
//
//   - one base name across the set, and
//   - idsFollowBinlog(rows) says ascending event_id is binlog order for these
//     rows (IDsFollowBinlog: an index one stream writes, or one file of an
//     index built with `bintrail index`), and
//   - walking the rows by event_id, the position never goes down.
//
// When all three hold, position order and id order are the same order. So
// THE ORDER RETURNED IS ALWAYS ASCENDING event_id: the order the index
// received the changes in, used when that is shown to be the binary log's
// order. The position is the CROSS-CHECK of that claim, not the sort key that
// matters, and it must not be "simplified" away in either direction: sorting
// by id alone trusts an index whose files were indexed out of order, and
// sorting by position alone trusts a numbering that may have restarted inside
// the window. When one condition fails, the rows stay as they came and
// Warning says why. No path returns a third order.
//
// An index built only with `bintrail index` (IDsFollowFileIndexing) vouches
// for its ids inside ONE binary log file and no further: a set that spans
// more than one file is refused. See IDsFollowFileIndexing for why nothing
// the index holds can prove more.
//
// Nothing of this is looked at when the two orders agree, which is every
// window without one of #2151's shapes: no lookup, no warning.
//
// idsFollowBinlog may be nil: the order is then never confirmed.
func OrderByBinlog(rows []ResultRow, idsFollowBinlog func([]ResultRow) IDProof) BinlogOrder {
	o := BinlogOrder{Total: len(rows)}
	if len(rows) < 2 {
		return o
	}
	lsn := 0
	for i := range rows {
		if !hasBinlogCoordinate(&rows[i]) {
			o.Missing++
			if strings.Contains(rows[i].BinlogFile, "/") {
				lsn++
			}
		}
	}
	if lsn == len(rows) {
		o.Reason, o.Missing = OrderPostgres, 0
		return o
	}
	if o.Missing > 0 {
		o.Reason = OrderNoCoordinate
		return o
	}

	// perm is binlog order as indexes into rows. Stable, so rows the tuple
	// cannot tell apart (the same event_id twice: not a merged set) keep the
	// order they came in.
	perm := make([]int, len(rows))
	for i := range perm {
		perm[i] = i
	}
	slices.SortStableFunc(perm, func(a, b int) int { return compareBinlogCoordinate(&rows[a], &rows[b]) })
	for i, p := range perm {
		if p != i {
			o.Moved++
		}
	}
	if o.Moved == 0 {
		return o
	}

	if names := binlogBaseNames(rows); len(names) > 1 {
		o.Reason, o.detail = OrderSeveralBaseNames, strings.Join(names, ", ")
		return o
	}
	proof := IDsUnproven
	if idsFollowBinlog != nil {
		proof = idsFollowBinlog(rows)
	}
	switch proof {
	case IDsFollowStream:
	case IDsFollowFileIndexing:
		if files := distinctBinlogFiles(rows); files > 1 {
			o.Reason = OrderIDsUnproven
			o.detail = fmt.Sprintf("it was built with `bintrail index` only and these changes are in %d binary log files, so its ids follow the order the files were indexed in, which is not shown to be the order the source wrote them in", files)
			return o
		}
	default:
		o.Reason = OrderIDsUnproven
		o.detail = "binary log files were indexed into it with `bintrail index` beside the stream, or the index could not be asked, so its ids do not say in which order the changes were written"
		return o
	}
	byID := make([]int, len(rows))
	for i := range byID {
		byID[i] = i
	}
	slices.SortStableFunc(byID, func(a, b int) int { return cmp.Compare(rows[a].EventID, rows[b].EventID) })
	for i := 1; i < len(byID); i++ {
		prev, cur := &rows[byID[i-1]], &rows[byID[i]]
		if !(BinlogPos{File: prev.BinlogFile, Pos: prev.StartPos}).AtOrBefore(BinlogPos{File: cur.BinlogFile, Pos: cur.StartPos}) {
			o.Reason = OrderRenumbered
			o.detail = fmt.Sprintf("event %d is at %s:%d and the next one indexed, event %d, is at %s:%d",
				prev.EventID, prev.BinlogFile, prev.StartPos, cur.EventID, cur.BinlogFile, cur.StartPos)
			return o
		}
	}

	sorted := make([]ResultRow, len(rows))
	for i, p := range perm {
		sorted[i] = rows[p]
	}
	copy(rows, sorted)
	o.Reason = OrderSorted
	return o
}

// distinctBinlogFiles counts the binary log files the rows are in.
func distinctBinlogFiles(rows []ResultRow) int {
	seen := map[string]bool{}
	for i := range rows {
		seen[rows[i].BinlogFile] = true
	}
	return len(seen)
}

// compareBinlogCoordinate orders two rows that BOTH have a coordinate by
// (file under the BinlogPos rule, start position, event_id). See OrderByBinlog
// for why it must not be given a row without one.
func compareBinlogCoordinate(a, b *ResultRow) int {
	return cmp.Or(
		cmp.Compare(len(a.BinlogFile), len(b.BinlogFile)),
		cmp.Compare(a.BinlogFile, b.BinlogFile),
		cmp.Compare(a.StartPos, b.StartPos),
		cmp.Compare(a.EventID, b.EventID),
	)
}

// binlogBaseNames lists the distinct base names of the rows' files, sorted: a
// file name without its numeric suffix ("binlog.000042" is "binlog"). A name
// with no such suffix is its own base name.
func binlogBaseNames(rows []ResultRow) []string {
	seen := map[string]bool{}
	var names []string
	for i := range rows {
		base := binlogBaseName(rows[i].BinlogFile)
		if !seen[base] {
			seen[base] = true
			names = append(names, base)
		}
	}
	slices.Sort(names)
	return names
}

func binlogBaseName(file string) string {
	i := strings.LastIndexByte(file, '.')
	if i < 0 || i == len(file)-1 {
		return file
	}
	for _, c := range file[i+1:] {
		if c < '0' || c > '9' {
			return file
		}
	}
	return file[:i]
}

// writersKeepBinlogOrder is the premise PartitionHeads.orderProven and
// IDsFollowBinlog share: whether rows stamped at or after since were given
// ascending event_ids in binlog order.
//
// One kind of writer only (a stream alone, or binlog files alone, indexed in
// order) keeps it. A stream AND `bintrail index` do not: the files' rows get
// the newest ids with old positions. That stops mattering for rows stamped
// well after the last file indexing run ended, which only the stream can have
// written.
func writersKeepBinlogOrder(streamCaptured, fileIndexingUnfinished bool, lastFileIndexed, since time.Time) bool {
	if !streamCaptured || (lastFileIndexed.IsZero() && !fileIndexingUnfinished) {
		return true
	}
	if fileIndexingUnfinished {
		return false
	}
	return lastFileIndexed.Before(since.Add(-fileIndexingMargin))
}

// IDsFollowBinlog reports what the index can say about the event_ids of rows
// whose earliest event_timestamp is since. It reads stream_state and
// index_state, two small tables; it does not look at binlog_events.
//
// Every failure answers IDsUnproven, with the error. An index with no stream
// is IDsFollowFileIndexing whatever index_state holds: see that constant.
func IDsFollowBinlog(ctx context.Context, db *sql.DB, since time.Time) (IDProof, error) {
	w, err := readIndexWriters(ctx, db)
	if err != nil {
		return IDsUnproven, err
	}
	return w.proof(since), nil
}

// indexWriters is what IDsFollowBinlog reads from the index, once: which kinds
// of writer wrote it and when the last file indexing run ended. proof turns it
// into the answer for one since, so a caller that asks for many sets of rows
// (one per row of a table, LatestPerPKInBinlog) reads the two tables once.
type indexWriters struct {
	// known is false for a nil db: nothing can be proven.
	known                  bool
	streamCaptured         bool
	fileIndexingUnfinished bool
	lastFileIndexed        time.Time
}

func readIndexWriters(ctx context.Context, db *sql.DB) (indexWriters, error) {
	if db == nil {
		return indexWriters{}, nil
	}
	streamCaptured, err := StreamCaptured(ctx, db)
	if err != nil {
		return indexWriters{}, err
	}
	w := indexWriters{known: true, streamCaptured: streamCaptured}
	if !streamCaptured {
		return w, nil
	}
	var last sql.NullTime
	var unfinished int64
	err = db.QueryRowContext(ctx, "SELECT MAX(completed_at), COUNT(*) - COUNT(completed_at) FROM index_state").Scan(&last, &unfinished)
	if err != nil && !isMissingTableErr(err) {
		return indexWriters{}, fmt.Errorf("read index_state: %w", err)
	}
	// No index_state table: no `bintrail index` run wrote to this index.
	if err == nil && last.Valid {
		w.lastFileIndexed = last.Time.UTC()
	}
	w.fileIndexingUnfinished = err == nil && unfinished > 0
	return w, nil
}

func (w indexWriters) proof(since time.Time) IDProof {
	switch {
	case !w.known:
		return IDsUnproven
	case !w.streamCaptured:
		return IDsFollowFileIndexing
	case writersKeepBinlogOrder(true, w.fileIndexingUnfinished, w.lastFileIndexed, since):
		return IDsFollowStream
	}
	return IDsUnproven
}

// earliestTimestamp is the since of a set of rows: its earliest
// event_timestamp. rows must not be empty.
func earliestTimestamp(rows []ResultRow) time.Time {
	since := rows[0].EventTimestamp
	for i := range rows {
		if rows[i].EventTimestamp.Before(since) {
			since = rows[i].EventTimestamp
		}
	}
	return since
}

// BinlogOrderProof is the idsFollowBinlog argument of OrderByBinlog for a
// caller that holds the index: IDsFollowBinlog for the rows' earliest
// event_timestamp. A failed read is logged and answers IDsUnproven, which keeps the
// rows as they came and puts the warning in front of the reader.
func BinlogOrderProof(ctx context.Context, db *sql.DB) func([]ResultRow) IDProof {
	return func(rows []ResultRow) IDProof {
		if len(rows) == 0 {
			return IDsUnproven
		}
		proof, err := IDsFollowBinlog(ctx, db, earliestTimestamp(rows))
		if err != nil {
			slog.Warn("could not read whether this index's ids follow the binary log; the changes stay in statement-time order", "error", err)
			return IDsUnproven
		}
		return proof
	}
}

// binlogOrderProofOnce is BinlogOrderProof for a caller that asks about many
// sets of rows of one fetch: stream_state and index_state are read at the
// first question and the answer for each set is computed from that one read.
// A failed read is logged once and answers IDsUnproven for every set.
func binlogOrderProofOnce(ctx context.Context, db *sql.DB) func([]ResultRow) IDProof {
	var (
		w    indexWriters
		read bool
	)
	return func(rows []ResultRow) IDProof {
		if len(rows) == 0 {
			return IDsUnproven
		}
		if !read {
			read = true
			var err error
			if w, err = readIndexWriters(ctx, db); err != nil {
				slog.Warn("could not read whether this index's ids follow the binary log; each row's latest change is taken by statement time", "error", err)
				w = indexWriters{}
			}
		}
		return w.proof(earliestTimestamp(rows))
	}
}
