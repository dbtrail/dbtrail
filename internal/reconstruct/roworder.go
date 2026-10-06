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
func EventsInBinlogOrder(events []query.ResultRow, at time.Time, idsFollowBinlog func([]query.ResultRow) bool) ([]query.ResultRow, query.BinlogOrder) {
	selected := make([]query.ResultRow, 0, len(events))
	for _, ev := range events {
		if !ev.EventTimestamp.After(at) {
			selected = append(selected, ev)
		}
	}
	return selected, query.OrderByBinlog(selected, idsFollowBinlog)
}
