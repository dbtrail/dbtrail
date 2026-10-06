package reconstruct

import (
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// EventsInBinlogOrder returns the events of ONE row that ApplyAt and
// BuildHistory fold for a reconstruction as of at, in the order the source's
// binary log holds them (#2156), and what was decided about that order.
//
// events come in (event_timestamp, event_id) order, and event_timestamp is the
// time a change's STATEMENT STARTED. When session B waits on a row lock that A
// holds, A's change is in the binary log first and carries the later time:
// folded as fetched, the row ends on A's values and the history lists B before
// A, where the database holds B's.
//
// Two steps, in this order:
//
//  1. Keep the events whose event_timestamp is at or before at. The cut stays
//     a cut by statement time (a change that started before at and committed
//     after it is inside: #2156 item 6, not changed here). It has to happen
//     BEFORE the reordering: ApplyAt and BuildHistory stop at the first event
//     after at, which is only "every later event" while the input is in time
//     order. Handing them a list with nothing after at makes that stop
//     unreachable.
//  2. query.OrderByBinlog over what is left. It keeps the fetched order, and
//     says why in the decision's Warning, when binary log order cannot be
//     established: see its comment. The caller owes that warning to whoever
//     reads the result.
//
// The input is not modified. idsFollowBinlog is query.BinlogOrderProof for a
// caller that holds the index.
//
// The shim's single-row reads call ApplyAt directly and still fold in fetched
// order; they take the rule in their own slice of #2156.
func EventsInBinlogOrder(events []query.ResultRow, at time.Time, idsFollowBinlog func([]query.ResultRow) query.IDProof) ([]query.ResultRow, query.BinlogOrder) {
	selected := make([]query.ResultRow, 0, len(events))
	for _, ev := range events {
		if !ev.EventTimestamp.After(at) {
			selected = append(selected, ev)
		}
	}
	return selected, query.OrderByBinlog(selected, idsFollowBinlog)
}

// PastCutWarning is the text owed to the reader of a single-row reconstruction
// when two things hold together, and "" otherwise:
//
//   - the row's selected changes were REORDERED (order.Sorted): at least one
//     of them was committed after a change that started later, and
//   - the request named its own instant (explicitCut: `--at` / `at` was given,
//     not defaulted to now).
//
// The cut selects by the time a statement STARTED (#2156 item 6, not changed
// here). Take A, started :02 and committed at once, and B, started :00, which
// waited and committed at :03. As of :02.5 the row held A. Both are selected,
// and binary log order applies B last: the answer is B's value, which was not
// committed yet at that instant. Folded in statement-time order the answer
// was A, right for that instant and wrong for every later one. The binary log
// order is kept, because it is the right order of the changes; what cannot be
// had from the statement time is where to stop, and the reader is told.
//
// With the cut at "now" every indexed change was committed before it, so
// there is nothing to warn about.
//
// The wording names no flag: the MCP tool and the web interface show it too.
func PastCutWarning(order query.BinlogOrder, explicitCut bool) string {
	if !explicitCut || !order.Sorted() {
		return ""
	}
	return "the changes of this row were written to the binary log in a different order than their statements started. " +
		"The requested instant selects changes by the time their statement started, so a change that was committed AFTER that instant " +
		"(its statement started before it and then waited, or ran long) may be included in this answer. " +
		"Compare with the row's history around that instant before relying on the value."
}
