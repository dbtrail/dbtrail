package reconstruct

import (
	"fmt"
	"sort"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The readers of #2750: those that return ONE row folded from a baseline row
// and its changes (single-row reconstruct, the MCP reconstruct tool, the
// console's /api/reconstruct, the shim's single-row _snapshot), and those
// that write each key's last change with no baseline at all (reconstruct's
// changes-only output, the shim's _flashback and its _snapshot with no
// snapshot location). They make the same check as the full-table fold
// (CheckRow): a value written before a narrowing ALTER that holds a character
// the new set cannot is '?' in the source, so the row is refused.

// CheckFold refuses the row ApplyAt(initial, events, at) returns when it
// carries a value a narrowing ALTER replaced with '?' (CheckRow): the state
// is checked against the change it was written by, or as a baseline row when
// no change after initial wrote it. A row absent at at is not checked. The
// fold is ApplyAt's own (applyAtFrom), so a TOAST refusal (#592) comes back
// here exactly as ApplyAt returns it. A nil receiver checks nothing.
//
// A history read (BuildHistory) is checked the same way: its last entry
// stands until at, so a value the ALTER replaced would be shown as the row's
// state at at. Its earlier entries were true at their own time and are not
// what is refused.
func (n *CharsetNarrowings) CheckFold(initial map[string]any, events []query.ResultRow, at time.Time) error {
	if !n.Checked() {
		return nil
	}
	state, src, err := applyAtFrom(initial, events, at)
	if err != nil || state == nil {
		return err
	}
	return n.CheckRow(state, src)
}

// CheckChangesFold is CheckFold for a row rebuilt from its changes alone (the
// shim's single-row _flashback): there is no snapshot to start from, so the
// refusal says so (CheckChange).
func (n *CharsetNarrowings) CheckChangesFold(events []query.ResultRow, at time.Time) error {
	if !n.Checked() {
		return nil
	}
	state, src, err := applyAtFrom(nil, events, at)
	if err != nil || state == nil || src == nil {
		return err
	}
	return n.CheckChange(state, src)
}

// CheckChange is CheckRow for a row written from its last recorded change ev
// with no baseline (reconstruct's changes-only output, the shim's _flashback
// and its _snapshot with no snapshot location). The refusal names no snapshot
// to start from, since there is none, and asks for one taken after the
// ALTER. ev nil checks nothing: such a row has no change to date its values
// by, and this path has no baseline row to treat it as.
func (n *CharsetNarrowings) CheckChange(row map[string]any, ev *query.ResultRow) error {
	if ev == nil {
		return nil
	}
	c := n.lost(row, ev)
	if c == nil {
		return nil
	}
	from := c.From
	if from == "" {
		from = "a character set not recorded"
	}
	return fmt.Errorf("%w: %s.%s has no snapshot here, so its rows are rebuilt from their recorded changes alone. "+
		"ALTER TABLE on %s.%s changed column %s from %s to %s, %s, after the last recorded change of a row: "+
		"a value of %s that change wrote holds a character %s cannot store, which the server replaced with '?' without writing row changes, "+
		"so the result would keep text the table no longer has. Take a snapshot of the table, which will hold it as it is after the alter table, and read it from that snapshot",
		ErrDestructiveDDL, n.schema, n.table, n.schema, n.table, c.Column, from, c.To, c.DDL.where(), c.Column, c.To)
}

// checkChangesNarrowing runs CheckChange on every row writeBinlogOnlyChanges
// writes from changes: each key's last change, skipping a DELETE and an empty
// image as it does. Keys are read in sorted order, so the same index always
// refuses on the same row.
func checkChangesNarrowing(n *CharsetNarrowings, changes map[string]*query.ResultRow) error {
	if !n.Checked() {
		return nil
	}
	pks := make([]string, 0, len(changes))
	for pk := range changes {
		pks = append(pks, pk)
	}
	sort.Strings(pks)
	for _, pk := range pks {
		ev := changes[pk]
		if ev == nil || ev.EventType == event.EventDelete || ev.RowAfter == nil {
			continue
		}
		if err := n.CheckChange(ev.RowAfter, ev); err != nil {
			return err
		}
	}
	return nil
}

// ChangesWindow is the DDLWindow a changes-only read checks for narrowing
// ALTERs: from the oldest of the changes it returns to until. A statement
// older than all of them cannot have touched a value any of them wrote. The
// lower bound is one second earlier, as reconstructBinlogOnly's, because both
// clocks are whole seconds. ok is false when there is no change to return,
// and then there is nothing to check.
func ChangesWindow(changes []*query.ResultRow, until time.Time) (w DDLWindow, ok bool) {
	var oldest time.Time
	for _, ev := range changes {
		if ev == nil {
			continue
		}
		t := ev.EventTimestamp
		if t.IsZero() {
			// A change with no time cannot be placed, and writtenBefore counts
			// it as before every ALTER: the window then starts at the epoch,
			// so no ALTER is left out. Not the zero time.Time, which the
			// driver sends as a date MySQL does not compare as a date.
			t = time.Unix(1, 0).UTC()
		}
		if !ok || t.Before(oldest) {
			oldest, ok = t, true
		}
	}
	if !ok {
		return DDLWindow{}, false
	}
	return DDLWindow{Since: oldest.Add(-time.Second), Until: until, Anchor: changesAnchor(changes)}, true
}

// changesAnchor is the earliest binary log position among changes, as the
// DDLWindow anchor that places a statement by position. The time recorded for
// an ALTER is when it STARTED: one that waited on a metadata lock held by an
// open transaction carries a time from before that transaction's changes and
// is in the binary log after them, so by time alone it would fall before the
// window and its values would never be checked. With the anchor, a statement
// that ends after the earliest change is inside, whatever its time.
//
// The position is taken in the sequence of files of the oldest change by time
// (positions compare only within one sequence); a change in another sequence
// is placed by time. nil when no change has a position.
func changesAnchor(changes []*query.ResultRow) *query.BinlogPos {
	var first *query.ResultRow
	for _, ev := range changes {
		if ev == nil || ev.BinlogFile == "" {
			continue
		}
		if first == nil || ev.EventTimestamp.Before(first.EventTimestamp) {
			first = ev
		}
	}
	if first == nil {
		return nil
	}
	anchor := query.BinlogPos{File: first.BinlogFile, Pos: first.StartPos}
	for _, ev := range changes {
		if ev == nil || ev.BinlogFile == "" || !sameBinlogSequence(ev.BinlogFile, anchor.File) {
			continue
		}
		if p := (query.BinlogPos{File: ev.BinlogFile, Pos: ev.StartPos}); p.AtOrBefore(anchor) {
			anchor = p
		}
	}
	return &anchor
}
