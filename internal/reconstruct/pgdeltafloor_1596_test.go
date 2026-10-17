package reconstruct

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Where a reader of a PostgreSQL snapshot starts (#1596): its LSN floor, and
// nothing for a MySQL snapshot or one that never recorded a floor.
func TestPGDeltaFloor(t *testing.T) {
	if p := PGDeltaFloor(baseline.DumpMetadata{LSN: 1<<32 + 0x10}); p == nil || p.Pos != 1<<32+0x10 || p.File != "1/10" || !p.IsLSN() {
		t.Fatalf("PostgreSQL floor = %+v", p)
	}
	for name, md := range map[string]baseline.DumpMetadata{
		"no floor":             {},
		"a MySQL snapshot":     {BinlogFile: "bin.000009", BinlogPos: 500},
		"a binlog anchor wins": {BinlogFile: "bin.000009", BinlogPos: 500, LSN: 7},
	} {
		if p := PGDeltaFloor(md); p != nil {
			t.Errorf("%s: %+v, want nil", name, p)
		}
	}
}

func TestPGDeltaWindow(t *testing.T) {
	snap := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	pg := func(since string) baseline.DumpMetadata {
		return baseline.DumpMetadata{LSN: 0x500, PGDeltaSince: since}
	}
	cases := []struct {
		name    string
		md      baseline.DumpMetadata
		since   time.Time
		from    bool
		wantErr bool
	}{
		{"a MySQL snapshot reads from its time", baseline.DumpMetadata{BinlogFile: "bin.000009", BinlogPos: 500}, snap, false, false},
		{"a PostgreSQL snapshot with no floor reads from its time", baseline.DumpMetadata{}, snap, false, false},
		{"no recorded bound: the margin", pg(""), snap.Add(-PGDeltaMargin), true, false},
		{"a recorded bound further back", pg("2026-06-01T07:00:00Z"), time.Date(2026, 6, 1, 7, 0, 0, 0, time.UTC), true, false},
		{"a recorded bound closer than the margin is kept", pg("2026-06-01T11:59:00Z"), time.Date(2026, 6, 1, 11, 59, 0, 0, time.UTC), true, false},
		{"a bound after the snapshot is the snapshot's time", pg("2026-06-01T13:00:00Z"), snap, true, false},
		{"a bound with another offset", pg("2026-06-01T02:00:00-05:00"), time.Date(2026, 6, 1, 7, 0, 0, 0, time.UTC), true, false},
		{"a bound that does not parse refuses", pg("yesterday"), time.Time{}, false, true},
	}
	for _, c := range cases {
		since, from, proven, err := PGDeltaWindow(c.md, snap)
		if c.wantErr {
			if err == nil || !strings.Contains(err.Error(), "bintrail-pg baseline") {
				t.Errorf("%s: err = %v, want a refusal", c.name, err)
			}
			continue
		}
		if err != nil || !since.Equal(c.since) || (from != nil) != c.from || proven != c.from {
			t.Errorf("%s: since %s from %+v proven %v err %v; want since %s from %v", c.name, since, from, proven, err, c.since, c.from)
		}
	}
}

// The changes fetched from a PostgreSQL floor that are stamped before the
// snapshot are folded into its row, never shown as after it (#1596).
func TestFoldBeforeSnapshot(t *testing.T) {
	snap := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ev := func(typ event.EventType, d time.Duration, after map[string]any) query.ResultRow {
		return query.ResultRow{EventType: typ, EventTimestamp: snap.Add(d), SchemaName: "s", TableName: "t", PKValues: "1", RowAfter: after}
	}
	row := func(v string) map[string]any { return map[string]any{"id": 1.0, "v": v} }
	base := row("base")
	pk := []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true}}

	got, rest, err := FoldBeforeSnapshot(base, nil, snap, pk)
	if err != nil || got["v"] != "base" || len(rest) != 0 {
		t.Fatalf("no events: %v %v %v", got, rest, err)
	}
	// Before, at, after: only the one before is folded; at the snapshot's
	// time it is after (the fetch from the time was inclusive).
	evs := []query.ResultRow{ev(event.EventUpdate, -time.Minute, row("pre")), ev(event.EventUpdate, 0, row("at")), ev(event.EventUpdate, time.Minute, row("post"))}
	got, rest, err = FoldBeforeSnapshot(base, evs, snap, pk)
	if err != nil || got["v"] != "pre" || len(rest) != 2 || rest[0].RowAfter["v"] != "at" {
		t.Fatalf("mixed: %v %+v %v", got, rest, err)
	}
	if base["v"] != "base" {
		t.Error("the baseline row was modified in place")
	}
	// The commit the snapshot missed created the row.
	got, rest, err = FoldBeforeSnapshot(nil, []query.ResultRow{ev(event.EventInsert, -time.Second, row("new"))}, snap, pk)
	if err != nil || got["v"] != "new" || len(rest) != 0 {
		t.Fatalf("insert: %v %v %v", got, rest, err)
	}
	// ...or deleted it.
	got, _, err = FoldBeforeSnapshot(base, []query.ResultRow{ev(event.EventDelete, -time.Second, nil)}, snap, pk)
	if err != nil || got != nil {
		t.Fatalf("delete: %v %v", got, err)
	}
	// Order kept: the later of two folded changes wins.
	got, _, _ = FoldBeforeSnapshot(base, []query.ResultRow{ev(event.EventUpdate, -2*time.Second, row("a")), ev(event.EventUpdate, -time.Second, row("b"))}, snap, pk)
	if got["v"] != "b" {
		t.Errorf("two changes: %v, want b", got)
	}
	// An UPDATE that moved the row to another key: the row left this one.
	moved := ev(event.EventUpdate, -time.Second, map[string]any{"id": 2.0, "v": "moved"})
	moved.RowBefore = row("base")
	if got, _, err := FoldBeforeSnapshot(base, []query.ResultRow{moved}, snap, pk); err != nil || got != nil {
		t.Errorf("a key change: %v %v, want no row here", got, err)
	}
	if got, _, err := FoldBeforeSnapshot(nil, []query.ResultRow{moved}, snap, pk); err != nil || got != nil {
		t.Errorf("a key change onto a row the snapshot lacks: %v %v, want no row here", got, err)
	}
	// An UPDATE that keeps the key is applied, before-image or not.
	kept := ev(event.EventUpdate, -time.Second, row("kept"))
	kept.RowBefore = row("base")
	if got, _, _ := FoldBeforeSnapshot(base, []query.ResultRow{kept}, snap, pk); got["v"] != "kept" {
		t.Errorf("an UPDATE keeping the key: %v", got)
	}
	// An unresolved TOAST marker refuses, as ApplyAt does.
	toast := ev(event.EventUpdate, -time.Second, map[string]any{"id": 1.0, "v": map[string]any{event.UnchangedToastKey: true}})
	if _, _, err := FoldBeforeSnapshot(base, []query.ResultRow{toast}, snap, pk); err == nil {
		t.Error("a TOAST marker was folded")
	}
}
