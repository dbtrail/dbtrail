package reconstruct

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2750: the single-row readers and the changes-only readers make the
// full-table fold's check (#2735) on the row they return.

var foldAlterAt = time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

func foldNarrowings() *CharsetNarrowings {
	return &CharsetNarrowings{schema: "shop", table: "t", checked: []charsetNarrowing{{
		Column: "name", From: "utf8mb4", To: "latin1",
		DDL: destructiveDDL{ID: 7, Type: "ALTER TABLE", File: "binlog.000003", Pos: 1000, DetectedAt: foldAlterAt},
	}}}
}

func foldEv(typ event.EventType, start uint64, ts time.Time, after map[string]any) query.ResultRow {
	return query.ResultRow{EventID: start, EventType: typ, BinlogFile: "binlog.000003", StartPos: start,
		EventTimestamp: ts, RowAfter: after}
}

func TestCheckFold(t *testing.T) {
	lost := map[string]any{"id": 1, "name": "Ñandú ✓"}
	kept := map[string]any{"id": 1, "name": "Ñandú €"} // MySQL's latin1 is cp1252: it has €
	rewritten := map[string]any{"id": 1, "name": "Ñandú"}
	before, after := foldAlterAt.Add(-time.Minute), foldAlterAt.Add(time.Minute)
	at := foldAlterAt.Add(time.Hour)
	for _, c := range []struct {
		label   string
		initial map[string]any
		events  []query.ResultRow
		at      time.Time
		refuse  bool
	}{
		{"a baseline row, no change", lost, nil, at, true},
		{"a baseline row latin1 holds", kept, nil, at, false},
		{"a NULL value", map[string]any{"id": 1, "name": nil}, nil, at, false},
		{"the column spelled in another case", map[string]any{"id": 1, "NAME": "Ñandú ✓"}, nil, at, true},
		{"last change before the ALTER", nil, []query.ResultRow{foldEv(event.EventInsert, 400, before, lost)}, at, true},
		{"rewritten after the ALTER", lost, []query.ResultRow{foldEv(event.EventUpdate, 1200, after, rewritten)}, at, false},
		{"created after the ALTER", nil, []query.ResultRow{foldEv(event.EventInsert, 1200, after, rewritten)}, at, false},
		{"deleted at the end", lost, []query.ResultRow{foldEv(event.EventDelete, 1200, after, nil)}, at, false},
		{"deleted before, inserted after the ALTER", lost, []query.ResultRow{
			foldEv(event.EventDelete, 400, before, nil),
			foldEv(event.EventInsert, 1200, after, rewritten)}, at, false},
		{"deleted after, inserted again after the ALTER with the old text", nil, []query.ResultRow{
			foldEv(event.EventInsert, 400, before, lost),
			foldEv(event.EventDelete, 1200, after, nil),
			// Inserted after the ALTER: it went into a latin1 column, so the
			// image is what the column holds, whatever it says.
			foldEv(event.EventInsert, 1300, after, lost)}, at, false},
		{"a drift row after the real source keeps it", nil, []query.ResultRow{
			foldEv(event.EventInsert, 400, before, lost),
			foldEv(0, 1200, after, nil)}, at, true},
		{"a change past at is not folded", lost, []query.ResultRow{foldEv(event.EventUpdate, 1200, at.Add(time.Second), rewritten)}, at, true},
	} {
		err := foldNarrowings().CheckFold(c.initial, c.events, c.at)
		if c.refuse != (err != nil) {
			t.Errorf("%s: CheckFold = %v, want refuse %v", c.label, err, c.refuse)
			continue
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrDestructiveDDL) || !strings.Contains(err.Error(), "from utf8mb4 to latin1") ||
			!strings.Contains(err.Error(), "Take a new snapshot") {
			t.Errorf("%s: %v, want the baseline refusal naming the change", c.label, err)
		}
		if strings.Contains(err.Error(), "✓") || strings.Contains(err.Error(), "Ñandú") {
			t.Errorf("%s: the refusal carries the row's value: %v", c.label, err)
		}
	}

	var none *CharsetNarrowings
	if err := none.CheckFold(lost, nil, at); err != nil {
		t.Errorf("nil narrowings: %v, want nothing checked", err)
	}
	if err := (&CharsetNarrowings{}).CheckFold(lost, nil, at); err != nil {
		t.Errorf("no narrowing: %v, want nothing checked", err)
	}
}

// The fold CheckFold reads is ApplyAt's own: same state for every input.
func TestApplyAtFromMatchesApplyAt(t *testing.T) {
	a := map[string]any{"id": 1, "v": "a"}
	b := map[string]any{"id": 1, "v": "b"}
	base := time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)
	at := base.Add(time.Hour)
	seqs := [][]query.ResultRow{
		nil,
		{foldEv(event.EventInsert, 1, base, a)},
		{foldEv(event.EventInsert, 1, base, a), foldEv(event.EventDelete, 2, base, nil)},
		{foldEv(event.EventDelete, 1, base, nil), foldEv(event.EventInsert, 2, base, b)},
		{foldEv(event.EventUpdate, 1, base, b), foldEv(0, 2, base, nil)},
		{foldEv(event.EventUpdate, 1, base, b), foldEv(event.EventDDL, 2, base, nil)},
		{foldEv(event.EventUpdate, 1, base, a), foldEv(event.EventUpdate, 2, at.Add(time.Second), b)},
	}
	for i, evs := range seqs {
		for _, initial := range []map[string]any{nil, a} {
			want, wantErr := ApplyAt(initial, evs, at)
			got, src, err := applyAtFrom(initial, evs, at)
			if !reflect.DeepEqual(got, want) || (err == nil) != (wantErr == nil) {
				t.Errorf("seq %d: applyAtFrom = %v, %v; ApplyAt = %v, %v", i, got, err, want, wantErr)
			}
			if src != nil && !reflect.DeepEqual(src.RowAfter, got) {
				t.Errorf("seq %d: the source's image %v is not the state %v", i, src.RowAfter, got)
			}
			if src == nil && got != nil && !reflect.DeepEqual(got, initial) {
				t.Errorf("seq %d: no source, yet the state %v is not the initial row", i, got)
			}
		}
	}
}

func TestCheckChangesFold(t *testing.T) {
	lost := map[string]any{"id": 1, "name": "Ñandú ✓"}
	at := foldAlterAt.Add(time.Hour)
	err := foldNarrowings().CheckChangesFold([]query.ResultRow{
		foldEv(event.EventInsert, 400, foldAlterAt.Add(-time.Minute), lost)}, at)
	if !errors.Is(err, ErrDestructiveDDL) || !strings.Contains(err.Error(), "recorded changes alone") {
		t.Fatalf("last change before the ALTER: %v, want the changes-only refusal", err)
	}
	if strings.Contains(err.Error(), "snapshot this starts from") || strings.Contains(err.Error(), "--") {
		t.Errorf("the changes-only refusal names a snapshot it does not have, or a flag: %v", err)
	}
	if err := foldNarrowings().CheckChangesFold([]query.ResultRow{
		foldEv(event.EventInsert, 1200, foldAlterAt.Add(time.Minute), lost)}, at); err != nil {
		t.Errorf("inserted after the ALTER: %v", err)
	}
	if err := foldNarrowings().CheckChangesFold(nil, at); err != nil {
		t.Errorf("no change: %v", err)
	}
	if err := foldNarrowings().CheckChange(lost, nil); err != nil {
		t.Errorf("CheckChange with no change: %v, want nothing checked", err)
	}
}

func TestCheckChangesNarrowing(t *testing.T) {
	lost := map[string]any{"id": 1, "name": "Ñandú ✓"}
	before, after := foldAlterAt.Add(-time.Minute), foldAlterAt.Add(time.Minute)
	del := foldEv(event.EventDelete, 400, before, nil)
	empty := foldEv(event.EventInsert, 401, before, nil)
	old := foldEv(event.EventInsert, 402, before, lost)
	fresh := foldEv(event.EventUpdate, 1200, after, lost)
	if err := checkChangesNarrowing(foldNarrowings(), map[string]*query.ResultRow{"1": &del, "2": &empty, "3": &fresh}); err != nil {
		t.Errorf("a DELETE, an empty image and a change after the ALTER: %v, want nothing refused", err)
	}
	err := checkChangesNarrowing(foldNarrowings(), map[string]*query.ResultRow{"1": &del, "2": &old})
	if !errors.Is(err, ErrDestructiveDDL) || !strings.Contains(err.Error(), "recorded changes alone") {
		t.Errorf("a key whose last change is before the ALTER: %v, want the changes-only refusal", err)
	}
	if err := checkChangesNarrowing(nil, map[string]*query.ResultRow{"2": &old}); err != nil {
		t.Errorf("nil narrowings: %v", err)
	}
}

func TestChangesWindow(t *testing.T) {
	until := foldAlterAt.Add(time.Hour)
	if _, ok := ChangesWindow(nil, until); ok {
		t.Error("no change: ok, want nothing to check")
	}
	if _, ok := ChangesWindow([]*query.ResultRow{nil}, until); ok {
		t.Error("only nil: ok, want nothing to check")
	}
	a := &query.ResultRow{EventTimestamp: foldAlterAt.Add(10 * time.Minute)}
	b := &query.ResultRow{EventTimestamp: foldAlterAt}
	w, ok := ChangesWindow([]*query.ResultRow{a, nil, b}, until)
	if !ok || !w.Since.Equal(foldAlterAt.Add(-time.Second)) || !w.Until.Equal(until) {
		t.Errorf("window = %v..%v, %v; want one second before the oldest change to until", w.Since, w.Until, ok)
	}
	w, ok = ChangesWindow([]*query.ResultRow{a, {}}, until)
	if !ok || w.Since.Year() != 1970 {
		t.Errorf("a change with no time: since %v, want the window opened to the epoch", w.Since)
	}
}

// An ALTER that waited on a metadata lock held by an open transaction carries
// the time it started, before that transaction's change, and is in the binary
// log after it: the window must hold it by position, or the change's value is
// never checked against it.
func TestChangesWindow_lockWaitAlterPlacedByPosition(t *testing.T) {
	changeAt := foldAlterAt.Add(30 * time.Second)
	ch := &query.ResultRow{BinlogFile: "binlog.000003", StartPos: 600, EventTimestamp: changeAt}
	later := &query.ResultRow{BinlogFile: "binlog.000003", StartPos: 900, EventTimestamp: changeAt.Add(-time.Second)}
	w, ok := ChangesWindow([]*query.ResultRow{later, ch}, foldAlterAt.Add(time.Hour))
	if !ok || w.Anchor == nil || w.Anchor.File != "binlog.000003" || w.Anchor.Pos != 600 {
		t.Fatalf("anchor = %+v, want the earliest position binlog.000003:600", w.Anchor)
	}
	alter := destructiveDDL{ID: 7, File: "binlog.000003", Pos: 1000, DetectedAt: foldAlterAt,
		AfterSince: false, AtOrBeforeUntil: true}
	if w.place(alter) == ddlOutside {
		t.Error("an ALTER started before the change and recorded after it is outside the window")
	}
	before := alter
	before.Pos = 500
	if w.place(before) != ddlOutside {
		t.Error("an ALTER recorded before every change is inside the window")
	}
	if a := changesAnchor([]*query.ResultRow{{EventTimestamp: changeAt}}); a != nil {
		t.Errorf("changes with no position: anchor %+v, want none", a)
	}
}
