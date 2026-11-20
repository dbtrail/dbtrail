package reconstruct

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// at spreads the changes of a test over time, in the order given.
func pgTimed(evs ...query.ResultRow) []query.ResultRow {
	for i := range evs {
		evs[i].EventTimestamp = foldBase.Add(time.Duration(i+1) * time.Second)
	}
	return evs
}

// One row of a PostgreSQL table at a moment, when the table was below
// REPLICA IDENTITY FULL: an earlier row captured with its key only is taken,
// and a large value an UPDATE left as it was is the one the row held.
func TestApplyAt_PGBelowFull(t *testing.T) {
	u := unsent()
	end := foldBase.Add(time.Hour)
	for name, c := range map[string]struct {
		initial map[string]any
		events  []query.ResultRow
		want    map[string]any
		refuses string
	}{
		"an earlier row captured with its key only": {
			row3("1", "s", "big"), pgTimed(upd3(1, 0x700, "t", "bigger")), row3("1", "t", "bigger"), ""},
		"a delete with a key-only earlier row": {
			row3("1", "s", "big"), pgTimed(del3(1, 0x700)), nil, ""},
		"an unsent value, from the snapshot's row": {
			row3("1", "s", "big"), pgTimed(upd3(1, 0x700, "t", u)), row3("1", "t", "big"), ""},
		"an unsent value, from the change before": {
			row3("1", "s", "big"), pgTimed(upd3(1, 0x700, "t", "new"), upd3(2, 0x710, "v", u)), row3("1", "v", "new"), ""},
		"an unsent value, from an insert with no snapshot row": {
			nil, pgTimed(ins3(1, 0x700, "s", "big"), upd3(2, 0x710, "t", u)), row3("1", "t", "big"), ""},
		"two unsent values": {
			row3("1", "S", "N"), pgTimed(upd3(1, 0x700, u, u)), row3("1", "S", "N"), ""},
		"a NULL the row held is a NULL": {
			row3("1", "s", nil), pgTimed(upd3(1, 0x700, "t", u)), row3("1", "t", nil), ""},
		"an unsent value and no row before": {
			nil, pgTimed(upd3(1, 0x700, "t", u)), nil, "there is no row before this change"},
		"an unsent value after a delete": {
			row3("1", "s", "big"), pgTimed(del3(1, 0x700), upd3(2, 0x710, "t", u)), nil, "there is no row before this change"},
		"an unsent value the row before does not hold": {
			map[string]any{"id": "1", "status": "s"}, pgTimed(upd3(1, 0x700, "t", u)), nil, "does not hold it"},
		"an unsent value beside a whole earlier row": {
			row3("1", "s", "big"), pgTimed(pgEv(1, 0x700, event.EventUpdate, row3("1", "s", "big"), row3("1", "t", u))), nil, "unchanged"},
		"an unsent value in an insert": {
			nil, pgTimed(ins3(1, 0x700, "s", u)), nil, "unchanged"},
		"a row left with a column that was not captured": {
			row3("1", "s", "big"), pgTimed(upd3(1, 0x700, "t", notCaptured())), nil, "as the change left it"},
	} {
		before := copyMap(c.initial)
		got, err := ApplyAt(c.initial, c.events, end)
		switch {
		case c.refuses != "":
			if err == nil || !strings.Contains(err.Error(), c.refuses) {
				t.Errorf("%s: %v, %v; want a refusal that says %q", name, got, err, c.refuses)
			} else if strings.Contains(err.Error(), "undo") {
				t.Errorf("%s: a single-row read's refusal speaks of undo: %v", name, err)
			}
		case err != nil || !reflect.DeepEqual(got, c.want):
			t.Errorf("%s: %v, %v; want %v", name, got, err, c.want)
		}
		if !reflect.DeepEqual(c.initial, before) {
			t.Errorf("%s: the snapshot's row was changed: %v", name, c.initial)
		}
		// The history takes the same steps, and ends on the same row.
		hist, herr := BuildHistory(c.initial, foldBase, c.events, end)
		if (herr == nil) != (err == nil) {
			t.Errorf("%s: the history answers %v and the row %v", name, herr, err)
		} else if herr == nil && !reflect.DeepEqual(hist[len(hist)-1].State, c.want) {
			t.Errorf("%s: the history ends on %v, want %v", name, hist[len(hist)-1].State, c.want)
		}
	}
}

// The page handed to a single-row read is left as it was.
func TestApplyAt_PGLeavesTheEventsAsTheyWere(t *testing.T) {
	evs := pgTimed(upd3(1, 0x700, "t", unsent()))
	if _, err := ApplyAt(row3("1", "s", "big"), evs, foldBase.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if !event.IsUnchangedToastMarker(evs[0].RowAfter["note"]) {
		t.Fatalf("the change's row was changed: %v", evs[0].RowAfter)
	}
}

// A change with no LSN is not PostgreSQL's: it is refused as it always was,
// by the check every such read has always run, in the same words.
func TestApplyAt_notPostgresAnswersAsBefore(t *testing.T) {
	for name, ev := range map[string]query.ResultRow{
		"an earlier row with a column that was not captured": upd3(1, 0x700, "t", "x"),
		"an unsent value":                                    upd3(1, 0x700, "t", unsent()),
	} {
		ev.BinlogFile, ev.StartPos = "binlog.000001", 4
		ev.EventTimestamp = foldBase.Add(time.Second)
		want := checkEventToast(ev)
		_, err := ApplyAt(row3("1", "s", "big"), []query.ResultRow{ev}, foldBase.Add(time.Hour))
		if want == nil || err == nil || err.Error() != want.Error() {
			t.Errorf("%s: %v; want %v", name, err, want)
		}
		if _, herr := BuildHistory(row3("1", "s", "big"), foldBase, []query.ResultRow{ev}, foldBase.Add(time.Hour)); herr == nil || herr.Error() != want.Error() {
			t.Errorf("%s: the history: %v; want %v", name, herr, want)
		}
		if _, _, ferr := FoldBeforeSnapshot(row3("1", "s", "big"), []query.ResultRow{ev}, foldBase.Add(time.Hour), pkColsIntID()); ferr == nil || ferr.Error() != want.Error() {
			t.Errorf("%s: the fold before a snapshot: %v; want %v", name, ferr, want)
		}
	}
}

// Changes stamped before a snapshot's time are folded into its row.
func TestFoldBeforeSnapshot_PGBelowFull(t *testing.T) {
	snap := foldBase.Add(time.Hour)
	u := unsent()
	row, rest, err := FoldBeforeSnapshot(row3("1", "s", "big"), pgTimed(upd3(1, 0x700, "t", u), upd3(2, 0x710, "v", u)), snap, pkColsIntID())
	if err != nil || len(rest) != 0 || !reflect.DeepEqual(row, row3("1", "v", "big")) {
		t.Fatalf("row %v, rest %d, %v", row, len(rest), err)
	}
	// A change of key ends the row under this key, key-only earlier row or not.
	moved := pgTimed(pgEv(1, 0x700, event.EventUpdate, map[string]any{"id": "1", "status": notCaptured(), "note": notCaptured()}, row3("2", "t", "x")))
	if row, _, err := FoldBeforeSnapshot(row3("1", "s", "big"), moved, snap, pkColsIntID()); err != nil || row != nil {
		t.Fatalf("after a change of key: %v, %v", row, err)
	}
	// With no row in the snapshot there is nothing to take the value from.
	if _, _, err := FoldBeforeSnapshot(nil, pgTimed(upd3(1, 0x700, "t", u)), snap, pkColsIntID()); err == nil || !strings.Contains(err.Error(), "there is no row before this change") {
		t.Fatalf("no row in the snapshot: %v", err)
	}
}
