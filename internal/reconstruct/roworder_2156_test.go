package reconstruct

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

var roworderT0 = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

// lockWaitEvents is #2151's first shape as a fetch returns it: B (event 2)
// started at T0 and waited; A (event 1) started 2 s later and is first in the
// binary log.
func lockWaitEvents(fileA, fileB string) []query.ResultRow {
	upd := func(id uint64, file string, pos uint64, at time.Time, v string) query.ResultRow {
		return query.ResultRow{
			EventID: id, BinlogFile: file, StartPos: pos, EventTimestamp: at,
			SchemaName: "shop", TableName: "t", EventType: event.EventUpdate, PKValues: "1",
			RowAfter: map[string]any{"id": float64(1), "v": v},
		}
	}
	return []query.ResultRow{
		upd(2, fileB, 900, roworderT0, "B"),
		upd(1, fileA, 400, roworderT0.Add(2*time.Second), "A"),
	}
}

func rowYes([]query.ResultRow) query.IDProof { return query.IDsFollowStream }

func historyValues(t *testing.T, events []query.ResultRow, at time.Time) []string {
	t.Helper()
	entries, err := BuildHistory(map[string]any{"id": float64(1), "v": "seed"}, roworderT0.Add(-time.Hour), events, at)
	if err != nil {
		t.Fatalf("BuildHistory: %v", err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.State["v"].(string))
	}
	return out
}

// The lock wait: the row ends on B and the history reads seed, A, B.
func TestEventsInBinlogOrder_lockWait(t *testing.T) {
	const f = "binlog.000007"
	at := roworderT0.Add(time.Minute)
	fetched := lockWaitEvents(f, f)
	ordered, order := EventsInBinlogOrder(fetched, at, rowYes)
	if !order.Sorted() || order.Warning() != "" {
		t.Fatalf("decision = %+v", order)
	}
	if fetched[0].EventID != 2 || fetched[1].EventID != 1 {
		t.Fatalf("the caller's slice was reordered: %d, %d", fetched[0].EventID, fetched[1].EventID)
	}
	state, err := ApplyAt(map[string]any{"id": float64(1), "v": "seed"}, ordered, at)
	if err != nil || state["v"] != "B" {
		t.Fatalf("ApplyAt = %v, %v; want v=B (the change the binary log holds last)", state, err)
	}
	if got, want := historyValues(t, ordered, at), []string{"seed", "A", "B"}; !slices.Equal(got, want) {
		t.Fatalf("history = %v, want %v", got, want)
	}
}

// The cut stays a cut by statement time and happens before the reordering.
// As of a moment between the two start times only B had started: B is folded,
// A is not. Without the cut first, A (now ahead of B, and after the cut) would
// stop the fold before B and the row would read as never changed.
func TestEventsInBinlogOrder_cutByTimeComesFirst(t *testing.T) {
	const f = "binlog.000007"
	at := roworderT0.Add(time.Second)
	ordered, order := EventsInBinlogOrder(lockWaitEvents(f, f), at, rowYes)
	if len(ordered) != 1 || ordered[0].EventID != 2 {
		t.Fatalf("selected %d events (first id %v), want event 2 alone", len(ordered), ordered)
	}
	if order.Sorted() || order.Warning() != "" {
		t.Fatalf("one event left and the decision is %+v", order)
	}
	state, err := ApplyAt(map[string]any{"id": float64(1), "v": "seed"}, ordered, at)
	if err != nil || state["v"] != "B" {
		t.Fatalf("ApplyAt = %v, %v; want v=B", state, err)
	}
	// Three events, the cut after the first two start times: both are kept
	// and reordered, the third is dropped wherever it sits.
	three := append(lockWaitEvents(f, f), query.ResultRow{
		EventID: 3, BinlogFile: f, StartPos: 1500, EventTimestamp: roworderT0.Add(time.Hour),
		EventType: event.EventDelete, SchemaName: "shop", TableName: "t", PKValues: "1",
	})
	ordered, order = EventsInBinlogOrder(three, roworderT0.Add(time.Minute), rowYes)
	if len(ordered) != 2 || ordered[0].EventID != 1 || ordered[1].EventID != 2 || !order.Sorted() {
		t.Fatalf("ordered = %v, decision %+v", ordered, order)
	}
}

// Where binary log order cannot be established the fold is the fetched order
// and the caller gets a warning to show.
func TestEventsInBinlogOrder_unprovenKeepsFetchedOrderAndWarns(t *testing.T) {
	at := roworderT0.Add(time.Minute)
	for name, tc := range map[string]struct {
		events []query.ResultRow
		proof  func([]query.ResultRow) query.IDProof
		warn   string
	}{
		"no position":      {lockWaitEvents("binlog.000007", ""), rowYes, "carry no binary log position"},
		"two names":        {lockWaitEvents("a-bin.000001", "b-bin.000001"), rowYes, "different names"},
		"ids not in order": {lockWaitEvents("binlog.000007", "binlog.000007"), func([]query.ResultRow) query.IDProof { return query.IDsUnproven }, "cannot show which one is right"},
		"no index":         {lockWaitEvents("binlog.000007", "binlog.000007"), nil, "cannot show which one is right"},
	} {
		ordered, order := EventsInBinlogOrder(tc.events, at, tc.proof)
		if order.Sorted() || !strings.Contains(order.Warning(), tc.warn) {
			t.Fatalf("%s: decision %+v, warning %q", name, order, order.Warning())
		}
		if got, want := historyValues(t, ordered, at), []string{"seed", "B", "A"}; !slices.Equal(got, want) {
			t.Fatalf("%s: history = %v, want the fetched order %v", name, got, want)
		}
	}
	// PostgreSQL: fetched order is commit order, and nothing is said.
	ordered, order := EventsInBinlogOrder(lockWaitEvents("0/FFFFFFFF", "1/5"), at, nil)
	if order.Warning() != "" || order.Sorted() || ordered[0].EventID != 2 {
		t.Fatalf("PostgreSQL rows: decision %+v, first event %d", order, ordered[0].EventID)
	}
}

// A past instant between two commits: A started :02 and committed at once, B
// started :00, waited and committed after A. As of :02.5 the cut (by statement
// time) selects both and binary log order applies B last, a value not yet
// committed at that instant. The order is kept and the reader is warned, only
// when the request named its own instant and the changes were reordered.
func TestPastCutWarning(t *testing.T) {
	const f = "binlog.000007"
	at := roworderT0.Add(2500 * time.Millisecond)
	ordered, order := EventsInBinlogOrder(lockWaitEvents(f, f), at, rowYes)
	state, err := ApplyAt(map[string]any{"id": float64(1), "v": "seed"}, ordered, at)
	if err != nil || state["v"] != "B" || !order.Sorted() {
		t.Fatalf("ApplyAt = %v, %v, decision %+v; this case needs B applied last", state, err, order)
	}
	warn := PastCutWarning(order, true)
	for _, want := range []string{
		"written to the binary log in a different order than their statements started",
		"selects changes by the time their statement started",
		"committed AFTER that instant",
	} {
		if !strings.Contains(warn, want) {
			t.Fatalf("warning = %q, want it to hold %q", warn, want)
		}
	}
	if strings.Contains(warn, "--") {
		t.Fatalf("the warning names a command-line flag; the MCP tool and the web interface show it too: %q", warn)
	}
	if got := PastCutWarning(order, false); got != "" {
		t.Fatalf("a cut at now warned: %q", got)
	}
	// Not reordered: one change selected, an agreeing pair, a refusal (which
	// has its own warning and keeps the statement-time fold).
	_, one := EventsInBinlogOrder(lockWaitEvents(f, f), roworderT0.Add(time.Second), rowYes)
	_, refused := EventsInBinlogOrder(lockWaitEvents(f, f), at, nil)
	for name, o := range map[string]query.BinlogOrder{"one change": one, "refused": refused, "zero": {}} {
		if got := PastCutWarning(o, true); got != "" {
			t.Fatalf("%s: warned %q", name, got)
		}
	}
}
