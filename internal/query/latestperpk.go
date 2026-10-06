package query

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// LatestPerPKOrder is what LatestPerPKInBinlog decided, counted over the keys
// it had to decide for.
type LatestPerPKOrder struct {
	// Disagreed is how many keys had different latest N by statement time and
	// by the order the index received their changes. On every other key the
	// latest N is the same either way.
	Disagreed int
	// Sorted is how many of them were taken in binary log order.
	Sorted int
	// Refused is how many keys kept statement-time order where binary log
	// order could not be established and may differ: most are among
	// Disagreed; on an index built with `bintrail index` only, a key whose
	// changes span two files can be refused without disagreeing.
	Refused int
	// warning is the first refused key's reason.
	warning string
}

// Note is the text for the reader of a result some of whose keys kept
// statement-time order where it may be wrong, and "" when none did. It starts
// with "order of changes unproven" so a caller's reader can find it.
func (o LatestPerPKOrder) Note() string {
	if o.Refused == 0 {
		return ""
	}
	return fmt.Sprintf("order of changes unproven: for %d row(s) the order of the changes in the binary log could not be established, "+
		"so each of those rows was taken at its change with the latest statement time. "+
		"A mismatch on a row that two sessions changed at once may be a false alarm. The first such row: %s", o.Refused, upperFirst(o.warning))
}

// ReadNote is Note for a reader that serves the rows it took (a time-travel
// read, a recovery script) rather than comparing them: the same refusal,
// without the word about a mismatch.
func (o LatestPerPKOrder) ReadNote() string {
	if o.Refused == 0 {
		return ""
	}
	return fmt.Sprintf("order of changes unproven: for %d row(s) the order of the changes in the binary log could not be established, "+
		"so each of those rows was taken at its change with the latest statement time, which is wrong for a row two sessions changed at once. "+
		"The first such row: %s", o.Refused, upperFirst(o.warning))
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// Key spans (#2156). A fetch with Options.LatestPerPKCandidates returns only
// candidates, and a rule checked over candidates alone can pass where the
// key's whole history fails it: on an index built with `bintrail index` only,
// the ids vouch for ONE file, and two candidates in one file say nothing of a
// third change in another file. So each candidate also carries two aggregates
// over ALL the changes of its key that the fetch's filter matched in its
// source: the first binary log file and the last coordinate, both under the
// BinlogPos order (length of the file name, then the name, then the
// position). They are encoded so that a plain byte comparison is that order,
// the same encoding in MySQL (CAST ... AS BINARY), DuckDB and Go:
//
//	file:       4-digit byte length of the name, then the name
//	coordinate: the file encoding, then the start position in 20 digits
//
// A missing file or position counts as "" and 0, in SQL through COALESCE, so
// the three agree on every row.

// keySpanFile is the encoded file of one row.
func keySpanFile(r *ResultRow) string {
	return fmt.Sprintf("%04d%s", len(r.BinlogFile), r.BinlogFile)
}

// keySpanCoord is the encoded coordinate of one row.
func keySpanCoord(r *ResultRow) string {
	return fmt.Sprintf("%04d%s%020d", len(r.BinlogFile), r.BinlogFile, r.StartPos)
}

// decodeKeySpanFile returns the file name of an encoded file or coordinate.
func decodeKeySpanFile(s string) string {
	if len(s) < 4 {
		return s
	}
	n, err := strconv.Atoi(s[:4])
	if err != nil || 4+n > len(s) {
		return s
	}
	return s[4 : 4+n]
}

// LatestPerPKInBinlog keeps, for each pk_values, its latest n changes in the
// order the source's binary log holds them, which is commit order: the "latest
// change of a row" that `verify` and the shim's `_snapshot` put on a snapshot
// (#2156). event_timestamp is the time a change's STATEMENT STARTED, so the
// latest by time can be a change that committed BEFORE another one (a lock
// wait, a long UPDATE, SET TIMESTAMP), and the database holds the other one.
//
// rows must hold, for every key, at least its latest n by (event_timestamp,
// event_id) and its latest n by event_id: what a fetch with
// Options.LatestPerPKCandidates returns, from MySQL, from every archive, and
// merged (a key's candidates over the whole index are among the union of its
// candidates in each source). rows must be merged: one copy per event_id.
// Rows with KeySpanFirst/KeySpanLast set carry their key's span (see above);
// rows without them are their own span. They are sorted in place; the result
// is in ascending (event_timestamp, event_id) order.
//
// # The rule
//
// The answer for a key is the one OrderByBinlog gives over ALL its changes,
// the rule `recover` applies to a whole set (#2162): binary log order when it
// is shown for them, statement time otherwise.
//
//   - When the key's latest n by time and by event_id are the same set, that
//     IS the answer either way: when binary log order is shown it is
//     ascending event_id. Nothing is fetched, nothing asked, with the one
//     exception below.
//   - When they differ, the key's whole history is needed: OrderByBinlog's
//     cross-checks (one file on an index built with `bintrail index` only,
//     one base name, a position that never goes down as the id goes up) are
//     about every change, and a change outside the candidates can fail them.
//     history fetches it, for all such keys at once; they are rare (a key
//     with one of #2151's shapes in its latest changes). nil history means
//     rows already hold every change of every key (a caller with the whole
//     window, a test).
//   - The exception: on an index built from files only, a key whose two
//     latest sets agree but whose last coordinate (its span) is not among the
//     kept changes is refused, with a note. There the latest by position is a
//     change the ids cannot place: another file, or one they put before the
//     kept change inside one file. Its answer stays the latest by time.
//   - Its own refusal: positions agree with the times while the ids do not
//     (OrderByBinlog sees nothing to decide there, and the answer is time).
//   - A PostgreSQL change (an LSN as file) keeps time order, with no note: its
//     time is its commit time.
//
// A refusal keeps the latest n by time and is counted for Note. No path keeps
// a third set.
//
// A key with no pk_values ("") is never grouped: every such row is its own
// key, as in LimitPerPK.
//
// idsFollowBinlog is OrderByBinlog's argument; nil never proves the order.
func LatestPerPKInBinlog(rows []ResultRow, n int, idsFollowBinlog func([]ResultRow) IDProof,
	history func(pks []string) ([]ResultRow, error)) ([]ResultRow, LatestPerPKOrder, error) {
	var o LatestPerPKOrder
	if n <= 0 || len(rows) == 0 {
		return rows, o, nil
	}
	slices.SortStableFunc(rows, compareStatementTime)
	byKey := make(map[string][]int, len(rows))
	var keys []string
	out := make([]ResultRow, 0, len(rows))
	for i := range rows {
		pk := rows[i].PKValues
		if pk == "" {
			out = append(out, rows[i])
			continue
		}
		if _, seen := byKey[pk]; !seen {
			keys = append(keys, pk)
		}
		byKey[pk] = append(byKey[pk], i)
	}
	refuse := func(why string) {
		o.Refused++
		if o.warning == "" {
			o.warning = why + ". " + statementTimeTail
		}
	}

	type keyCand struct {
		cand     []ResultRow
		disagree bool
	}
	cands := make(map[string]keyCand, len(keys))
	var disagreeing []string
	for _, pk := range keys {
		cand, disagree := latestPerPKCandidates(rows, byKey[pk], n)
		cands[pk] = keyCand{cand, disagree}
		if disagree {
			disagreeing = append(disagreeing, pk)
		}
	}
	// The whole history of every key that needs a decision.
	full := make(map[string][]ResultRow, len(disagreeing))
	for _, pk := range disagreeing {
		full[pk] = pick(rows, byKey[pk])
	}
	if history != nil && len(disagreeing) > 0 {
		hist, err := history(disagreeing)
		if err != nil {
			return nil, o, fmt.Errorf("read the changes of %d row(s) whose latest change by statement time is not the last one the index received: %w", len(disagreeing), err)
		}
		for _, r := range hist {
			if _, want := full[r.PKValues]; want {
				full[r.PKValues] = append(full[r.PKValues], r)
			}
		}
		for pk, all := range full {
			all, _ = MergeResultsReport(all, 0, "ASC")
			full[pk] = all
		}
	}

	for _, pk := range keys {
		kc := cands[pk]
		cand := kc.cand
		byTime := cand[len(cand)-min(n, len(cand)):]
		if allLSN(cand) {
			out = append(out, byTime...)
			continue
		}
		var proof IDProof
		asked := false
		ask := func(rs []ResultRow) IDProof {
			if !asked {
				asked = true
				proof = IDsUnproven
				if idsFollowBinlog != nil {
					proof = idsFollowBinlog(rs)
				}
			}
			return proof
		}
		if !kc.disagree {
			_, last := keySpan(rows, byKey[pk])
			if !hasCoord(byTime, last) && ask(cand) == IDsFollowFileIndexing {
				first, _ := keySpan(rows, byKey[pk])
				if ff, lf := decodeKeySpanFile(first), decodeKeySpanFile(last); ff != lf {
					refuse(fmt.Sprintf("this index was built with `bintrail index` only and this row's changes are in more than one binary log file (%s to %s), "+
						"so its ids follow the order the files were indexed in, which is not shown to be the order the source wrote them in", ff, lf))
				} else {
					refuse(fmt.Sprintf("the change at this row's last binary log position (in %s) was received by an index built with `bintrail index` only "+
						"before the change kept: the file may have been indexed out of order or twice", lf))
				}
			}
			out = append(out, byTime...)
			continue
		}

		o.Disagreed++
		all := full[pk]
		slices.SortStableFunc(all, compareStatementTime)
		latestByTime, latestByID := all[len(all)-1], maxEventID(all)
		ordered := slices.Clone(all)
		d := OrderByBinlog(ordered, ask)
		switch d.Reason {
		case OrderSorted:
			o.Sorted++
			out = append(out, ordered[len(ordered)-n:]...)
			continue
		case OrderPostgres:
		case OrderAgrees:
			// Positions agree with the times, the index's ids do not. On an
			// index whose ids follow the binary log that is a numbering that
			// restarted between the two (a failover, with the new source's
			// clock behind); on one built from files, files indexed out of
			// order. OrderByBinlog has nothing to say about it (it compares
			// positions with times), so it is said here.
			if latestByID.EventID == latestByTime.EventID {
				// n > 1: the two latest sets differ below their top.
				refuse("the index received this row's changes in a different order than their binary log positions and statement times: " +
					"the source's binary log numbering may have restarted between them (a failover, RESET MASTER), or binary log files were indexed out of order")
			} else {
				refuse(fmt.Sprintf("the index received event %d (at %s:%d) after event %d (at %s:%d), "+
					"while the binary log position and the statement time both put event %d last: the source's binary log numbering may have restarted between them "+
					"(a failover, RESET MASTER), or binary log files were indexed out of order",
					latestByID.EventID, latestByID.BinlogFile, latestByID.StartPos,
					latestByTime.EventID, latestByTime.BinlogFile, latestByTime.StartPos,
					latestByTime.EventID))
			}
		default:
			refuse(rowRefusal(d))
		}
		out = append(out, all[len(all)-n:]...)
	}
	slices.SortStableFunc(out, compareStatementTime)
	return out, o, nil
}

// rowRefusal says why OrderByBinlog refused one row's changes, in words
// about the row and without OrderByBinlog's counts, which a reader of a
// whole table would take for counts over the table.
// RowReason is why OrderByBinlog kept one row's changes in statement-time
// order, worded for that row, and "" when it did not refuse.
func (o BinlogOrder) RowReason() string {
	if o.Warning() == "" {
		return ""
	}
	return rowRefusal(o)
}

// MayDiffer reports, for a decision OrderByBinlog took over rows (left in
// the order they were handed in, (event_timestamp, event_id), since it kept
// that order), that it kept statement-time order where binary log order may
// be different: positions disagreed with the times (every refusal but one),
// or, with a change that has no position, the ids (the order capture indexed
// them in) disagree with the times. A row whose times agree with its ids and
// that has no positions gives nothing to doubt, and a reader is not told so
// on every row an older build indexed.
func (o BinlogOrder) MayDiffer(rows []ResultRow) bool {
	if o.Warning() == "" {
		return false
	}
	if o.Reason != OrderNoCoordinate {
		return true
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].EventID < rows[i-1].EventID {
			return true
		}
	}
	return false
}

func rowRefusal(d BinlogOrder) string {
	switch d.Reason {
	case OrderNoCoordinate:
		return "a change of this row carries no binary log position (indexed by an older build, or mixed with changes of another kind of source), " +
			"so the order of its changes cannot be taken from the binary log"
	case OrderSeveralBaseNames:
		return fmt.Sprintf("this row's changes come from binary logs with different names (%s): the source's binary log was renamed or the source was replaced inside this time range, "+
			"and positions of different logs cannot be compared", d.detail)
	case OrderIDsUnproven:
		return fmt.Sprintf("this index cannot show which change of this row is the latest: %s. "+
			"A restart of the source's binary log numbering (a failover, RESET MASTER) inside this time range cannot be ruled out", d.detail)
	case OrderRenumbered:
		return fmt.Sprintf("the binary log position of this row's changes goes down while the index's ids go up (%s): "+
			"the source's binary log numbering restarted (a failover, RESET MASTER), or binary log files were indexed out of order", d.detail)
	}
	return "the order of its changes could not be established"
}

// keySpan is the encoded first file and last coordinate of one key: the
// least and greatest over each row's carried span and the row itself.
func keySpan(rows []ResultRow, idx []int) (first, last string) {
	for j, i := range idx {
		r := &rows[i]
		f, c := keySpanFile(r), keySpanCoord(r)
		if r.KeySpanFirst != "" && r.KeySpanFirst < f {
			f = r.KeySpanFirst
		}
		if r.KeySpanLast > c {
			c = r.KeySpanLast
		}
		if j == 0 || f < first {
			first = f
		}
		if j == 0 || c > last {
			last = c
		}
	}
	return first, last
}

// WidenKeySpan folds the span carried by a dropped duplicate of r into r (a
// merge keeps one copy of an event; its source's span must not go with the
// other copy).
func (r *ResultRow) WidenKeySpan(dup *ResultRow) {
	if dup.KeySpanFirst != "" && (r.KeySpanFirst == "" || dup.KeySpanFirst < r.KeySpanFirst) {
		r.KeySpanFirst = dup.KeySpanFirst
	}
	if dup.KeySpanLast > r.KeySpanLast {
		r.KeySpanLast = dup.KeySpanLast
	}
}

func hasCoord(rows []ResultRow, coord string) bool {
	for i := range rows {
		if keySpanCoord(&rows[i]) == coord {
			return true
		}
	}
	return false
}

func allLSN(rows []ResultRow) bool {
	for i := range rows {
		if !strings.Contains(rows[i].BinlogFile, "/") {
			return false
		}
	}
	return true
}

// latestPerPKCandidates returns, for the rows of one key (idx, indexes into
// rows in ascending statement-time order), the union of its latest n by
// statement time and its latest n by event_id, as copies in ascending
// statement-time order, and whether the two sets differ. When they do not,
// the result is exactly the latest n by time.
func latestPerPKCandidates(rows []ResultRow, idx []int, n int) ([]ResultRow, bool) {
	if len(idx) <= n {
		return pick(rows, idx), false
	}
	byTime := idx[len(idx)-n:]
	byID := slices.Clone(idx)
	slices.SortFunc(byID, func(a, b int) int { return cmp.Compare(rows[b].EventID, rows[a].EventID) })
	byID = byID[:n]
	inTime := make(map[int]bool, n)
	for _, i := range byTime {
		inTime[i] = true
	}
	var extra []int
	for _, i := range byID {
		if !inTime[i] {
			extra = append(extra, i)
		}
	}
	if len(extra) == 0 {
		return pick(rows, byTime), false
	}
	union := append(slices.Clone(byTime), extra...)
	slices.Sort(union) // indexes into rows ascend in statement-time order
	return pick(rows, union), true
}

func maxEventID(rows []ResultRow) ResultRow {
	return slices.MaxFunc(rows, func(a, b ResultRow) int { return cmp.Compare(a.EventID, b.EventID) })
}

func pick(rows []ResultRow, idx []int) []ResultRow {
	out := make([]ResultRow, len(idx))
	for j, i := range idx {
		out[j] = rows[i]
	}
	return out
}

// compareStatementTime is the order every fetch returns: (event_timestamp,
// event_id) ascending.
func compareStatementTime(a, b ResultRow) int {
	return cmp.Or(a.EventTimestamp.Compare(b.EventTimestamp), cmp.Compare(a.EventID, b.EventID))
}
