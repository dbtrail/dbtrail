package reconstruct

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pgTimed spreads the changes of a test over time, in the order given.
func pgTimed(evs ...query.ResultRow) []query.ResultRow {
	for i := range evs {
		evs[i].EventTimestamp = foldBase.Add(time.Duration(i+1) * time.Second)
	}
	return evs
}

// One row of a PostgreSQL table at a moment, when the table was below
// REPLICA IDENTITY FULL: an earlier row captured with its key only is taken,
// since the row is what the last change left. A large value a change left
// unsent is refused: a read of one row cannot tell it holds every change
// before that one.
func TestApplyAt_PGBelowFull(t *testing.T) {
	u := unsent()
	end := foldBase.Add(time.Hour)
	part := map[string]any{"id": "1", "status": notCaptured(), "note": notCaptured()}
	for name, c := range map[string]struct {
		initial map[string]any
		events  []query.ResultRow
		want    map[string]any
		refuses string
	}{
		"an earlier row captured with its key only": {
			row3("1", "s", "big"), pgTimed(upd3(1, 0x700, "t", "bigger")), row3("1", "t", "bigger"), ""},
		"with no snapshot row": {
			nil, pgTimed(upd3(1, 0x700, "t", "bigger")), row3("1", "t", "bigger"), ""},
		"a delete with a key-only earlier row": {
			row3("1", "s", "big"), pgTimed(del3(1, 0x700)), nil, ""},
		"an insert, an update and a delete": {
			nil, pgTimed(ins3(1, 0x700, "s", "big"), upd3(2, 0x710, "t", "b"), del3(3, 0x720), ins3(4, 0x730, "x", "y")), row3("1", "x", "y"), ""},
		"an unsent value": {
			row3("1", "s", "big"), pgTimed(upd3(1, 0x700, "t", u)), nil, "PostgreSQL did not send it"},
		"an unsent value, also when a later change brings the whole row": {
			row3("1", "s", "big"), pgTimed(upd3(1, 0x700, "t", u), upd3(2, 0x710, "v", "w")), nil, "PostgreSQL did not send it"},
		"an unsent value beside a whole earlier row": {
			row3("1", "s", "big"), pgTimed(pgEv(1, 0x700, event.EventUpdate, row3("1", "s", "big"), row3("1", "t", u))), nil, "unresolved unchanged-TOAST"},
		"an unsent value in an insert": {
			nil, pgTimed(ins3(1, 0x700, "s", u)), nil, "unresolved unchanged-TOAST"},
		"an unsent value in the earlier row": {
			row3("1", "s", "big"), pgTimed(pgEv(1, 0x700, event.EventUpdate, row3("1", "s", u), row3("1", "t", "x"))), nil, "unresolved unchanged-TOAST"},
		"a row left with a column that was not captured": {
			row3("1", "s", "big"), pgTimed(upd3(1, 0x700, "t", notCaptured())), nil, "as the change left it"},
		"a change of key, the earlier row captured with its key only": {
			row3("1", "s", "big"), pgTimed(pgEv(1, 0x700, event.EventUpdate, part, row3("2", "t", "x"))), nil, "another value in id"},
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
		// The history takes the same steps: one entry a change after the
		// snapshot's, each the row that change left.
		hist, herr := BuildHistory(c.initial, foldBase, c.events, end)
		if (herr == nil) != (err == nil) {
			t.Errorf("%s: the history answers %v and the row %v", name, herr, err)
			continue
		}
		if herr != nil {
			continue
		}
		if len(hist) != len(c.events)+1 || !reflect.DeepEqual(hist[len(hist)-1].State, c.want) {
			t.Errorf("%s: the history has %d entries and ends on %v, want %d and %v", name, len(hist), hist[len(hist)-1].State, len(c.events)+1, c.want)
		}
		for i, ev := range c.events {
			var left map[string]any
			if ev.EventType != event.EventDelete {
				left = ev.RowAfter
			}
			if !reflect.DeepEqual(hist[i+1].State, left) {
				t.Errorf("%s: history entry %d is %v, want the row its change left, %v", name, i+1, hist[i+1].State, left)
			}
		}
	}
}

// A change that is not PostgreSQL's (no LSN) is refused as it always was, by
// the check every such read has always run, in the same words; and so is a
// PostgreSQL row whose position does not read as an LSN.
func TestApplyAt_notPostgresAnswersAsBefore(t *testing.T) {
	for name, ev := range map[string]query.ResultRow{
		"an earlier row with a column that was not captured": upd3(1, 0x700, "t", "x"),
		"an unsent value": upd3(1, 0x700, "t", unsent()),
	} {
		for pos, set := range map[string]func(*query.ResultRow){
			"a binary log position":       func(r *query.ResultRow) { r.BinlogFile, r.StartPos = "binlog.000001", 4 },
			"an LSN that is not its text": func(r *query.ResultRow) { r.BinlogFile = "pg.000001" },
			"no position":                 func(r *query.ResultRow) { r.BinlogFile, r.StartPos = "", 0 },
		} {
			ev := ev
			set(&ev)
			ev.EventTimestamp = foldBase.Add(time.Second)
			want := checkEventToast(ev)
			_, err := ApplyAt(row3("1", "s", "big"), []query.ResultRow{ev}, foldBase.Add(time.Hour))
			if want == nil || err == nil || err.Error() != want.Error() {
				t.Errorf("%s, %s: %v; want %v", name, pos, err, want)
			}
			if _, herr := BuildHistory(row3("1", "s", "big"), foldBase, []query.ResultRow{ev}, foldBase.Add(time.Hour)); herr == nil || herr.Error() != want.Error() {
				t.Errorf("%s, %s: the history: %v; want %v", name, pos, herr, want)
			}
			if _, _, ferr := FoldBeforeSnapshot(row3("1", "s", "big"), []query.ResultRow{ev}, foldBase.Add(time.Hour), pkColsIntID()); ferr == nil || ferr.Error() != want.Error() {
				t.Errorf("%s, %s: the fold before a snapshot: %v; want %v", name, pos, ferr, want)
			}
		}
	}
}

// What a PostgreSQL index holds today (whole earlier rows) is read as it
// always was: the step answers as the two it stands in for.
func TestApplyPGEvent_answersAsBeforeOnWholeRows(t *testing.T) {
	toast := unsent()
	for name, ev := range map[string]query.ResultRow{
		"an update":                        pgEv(1, 0x700, event.EventUpdate, row3("1", "s", "a"), row3("1", "t", "b")),
		"an insert":                        ins3(1, 0x700, "s", "a"),
		"a delete":                         pgEv(1, 0x700, event.EventDelete, row3("1", "s", "a"), nil),
		"a change of key":                  pgEv(1, 0x700, event.EventUpdate, row3("1", "s", "a"), row3("2", "s", "a")),
		"an unsent value in the new row":   pgEv(1, 0x700, event.EventUpdate, row3("1", "s", "a"), row3("1", "t", toast)),
		"an unsent value in the old row":   pgEv(1, 0x700, event.EventUpdate, row3("1", "s", toast), row3("1", "t", "b")),
		"an unsent value in both":          pgEv(1, 0x700, event.EventUpdate, row3("1", toast, "a"), row3("1", "t", toast)),
		"an unsent value a delete removed": pgEv(1, 0x700, event.EventDelete, row3("1", "s", toast), nil),
		"an unsent value an insert added":  ins3(1, 0x700, "s", toast),
	} {
		state := row3("1", "old", "older")
		wantErr := checkEventToast(ev)
		got, err := applyPGEvent(state, &ev)
		if (wantErr == nil) != (err == nil) || (err != nil && err.Error() != wantErr.Error()) {
			t.Errorf("%s: %v; it answered %v", name, err, wantErr)
			continue
		}
		if err == nil && !reflect.DeepEqual(got, applyEvent(state, ev)) {
			t.Errorf("%s: %v; it answered %v", name, got, applyEvent(state, ev))
		}
	}
}

// Changes stamped before a snapshot's time are folded into its row.
func TestFoldBeforeSnapshot_PGBelowFull(t *testing.T) {
	snap := foldBase.Add(time.Hour)
	part := func(id any) map[string]any { return map[string]any{"id": id, "status": notCaptured(), "note": notCaptured()} }
	given := row3("1", "s", "big")
	after := upd3(3, 0x720, "late", "later")
	after.EventTimestamp = snap.Add(time.Second)
	row, rest, err := FoldBeforeSnapshot(given, append(pgTimed(upd3(1, 0x700, "t", "b"), upd3(2, 0x710, "v", "w")), after), snap, pkColsIntID())
	if err != nil || len(rest) != 1 || rest[0].EventID != 3 || !reflect.DeepEqual(row, row3("1", "v", "w")) {
		t.Fatalf("row %v, rest %v, %v", row, rest, err)
	}
	if !reflect.DeepEqual(given, row3("1", "s", "big")) {
		t.Fatalf("the snapshot's row was changed: %v", given)
	}
	// A change of key ends the row under this key, key-only earlier row or not.
	moved := pgTimed(pgEv(1, 0x700, event.EventUpdate, part("1"), row3("2", "t", "x")))
	if row, _, err := FoldBeforeSnapshot(row3("1", "s", "big"), moved, snap, pkColsIntID()); err != nil || row != nil {
		t.Fatalf("after a change of key: %v, %v", row, err)
	}
	for name, c := range map[string]struct {
		ev   query.ResultRow
		says string
	}{
		"the key itself not captured": {pgEv(1, 0x700, event.EventUpdate, part(notCaptured()), row3("1", "t", "x")), "no value for its key column id"},
		"an unsent value":             {upd3(1, 0x700, "t", unsent()), "PostgreSQL did not send it"},
	} {
		if _, _, err := FoldBeforeSnapshot(row3("1", "s", "big"), pgTimed(c.ev), snap, pkColsIntID()); err == nil || !strings.Contains(err.Error(), c.says) {
			t.Errorf("%s: %v; want a refusal that says %q", name, err, c.says)
		}
	}
}
