package query

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// LatestPerPKOrder is what LatestPerPKInBinlog decided, counted over the keys
// it had to decide for.
type LatestPerPKOrder struct {
	// Disagreed is how many keys had different latest N by statement time and
	// by the order the index received their changes. Only these were looked
	// at; every other key's latest N is the same either way.
	Disagreed int
	// Sorted is how many of them were taken in binary log order.
	Sorted int
	// Refused is how many of them kept statement-time order because binary
	// log order could not be established (OrderByBinlog's refusals).
	Refused int
	// warning is the first refused key's BinlogOrder.Warning.
	warning string
}

// Note is the text for the reader of a result some of whose keys kept
// statement-time order where it may be wrong, and "" when none did. It starts
// with "order of changes unproven" so a caller's reader can find it.
func (o LatestPerPKOrder) Note() string {
	if o.Refused == 0 {
		return ""
	}
	return fmt.Sprintf("order of changes unproven: for %d row(s) the change with the latest statement time is not the change the index received last, "+
		"and the binary log order could not be established, so each of those rows was taken at its change with the latest statement time. "+
		"A mismatch on a row that two sessions changed at once may be a false alarm. The first such row: %s", o.Refused, upperFirst(o.warning))
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
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
// They are sorted in place; the result is in ascending (event_timestamp,
// event_id) order.
//
// # The rule
//
// The rule is OrderByBinlog's, applied per key and to the key's candidates:
// the union of its latest n by time and its latest n by event_id. When the two
// sets are the same, which is every key without one of #2151's shapes, nothing
// is looked up and the time order stands. When they differ, OrderByBinlog
// decides: binary log order when the index's ids are shown to follow it (an
// index a stream writes, or one binary log file of one `bintrail index` run),
// one base name, and the position never going down as the id goes up; the
// latest n of that order are kept. Otherwise the latest n by time are kept and
// the key counts as refused, which Note reports, with one exception: a
// PostgreSQL change, whose time is its commit time. One refusal is this
// function's own: positions that agree with the times while the ids do not
// (OrderByBinlog sees nothing to decide there). No path keeps a third set.
//
// Why the candidates are enough: when binary log order is proven it IS
// ascending event_id, so the latest n in it are the latest n by event_id; when
// it is not, the answer is the latest n by time. Both are candidates.
//
// What the per-key decision does not see: OrderByBinlog's cross-checks (one
// base name, positions that never go down as ids go up, one file for an index
// built with `bintrail index` only) run over the key's candidates, not over
// its whole history in the window. A renumbering between two of the key's
// OLDER changes passes unseen. It cannot change the answer: the candidates
// are the only rows that can be kept, and on them the checks run whole.
//
// A key with no pk_values ("") is never grouped: every such row is its own
// key, as in LimitPerPK.
//
// idsFollowBinlog is OrderByBinlog's argument; nil never proves the order.
func LatestPerPKInBinlog(rows []ResultRow, n int, idsFollowBinlog func([]ResultRow) IDProof) ([]ResultRow, LatestPerPKOrder) {
	var o LatestPerPKOrder
	if n <= 0 || len(rows) == 0 {
		return rows, o
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
	for _, pk := range keys {
		idx := byKey[pk]
		cand, disagree := latestPerPKCandidates(rows, idx, n)
		if !disagree {
			out = append(out, cand...)
			continue
		}
		o.Disagreed++
		latestByTime, latestByID := cand[len(cand)-1], maxEventID(cand)
		d := OrderByBinlog(cand, idsFollowBinlog)
		// cand is in binary log order when sorted, in time order otherwise:
		// either way its last n are the ones kept.
		out = append(out, cand[len(cand)-n:]...)
		var warning string
		switch d.Reason {
		case OrderSorted:
			o.Sorted++
			continue
		case OrderPostgres:
			// Its time is its commit time: time order is commit order.
			continue
		case OrderAgrees:
			// Positions agree with the times, the index's ids do not. On an
			// index whose ids follow the binary log that is a numbering that
			// restarted between the two (a failover, with the new source's
			// clock behind); on one built from files, files indexed out of
			// order. OrderByBinlog has nothing to say about it (it compares
			// positions with times), so it is said here.
			warning = fmt.Sprintf("the index received event %d (at %s:%d) after event %d (at %s:%d), "+
				"while the binary log position and the statement time both put event %d last: the source's binary log numbering may have restarted between them "+
				"(a failover, RESET MASTER), or binary log files were indexed out of order. %s",
				latestByID.EventID, latestByID.BinlogFile, latestByID.StartPos,
				latestByTime.EventID, latestByTime.BinlogFile, latestByTime.StartPos,
				latestByTime.EventID, statementTimeTail)
		default:
			warning = d.Warning()
		}
		o.Refused++
		if o.warning == "" {
			o.warning = warning
		}
	}
	slices.SortStableFunc(out, compareStatementTime)
	return out, o
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
