package reconstruct

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2760: in a PostgreSQL window an UPDATE that changes a row's primary key
// is folded as a DELETE of the old key and an INSERT of the new one, each
// ordered against every other change of its key by commit LSN (laterInWAL),
// when the event can be proven whole (splitPGKeyChange), and refused with
// the #782 error and the reason otherwise.

// pgEv is one row of the transaction committed at lsn, as pgcapture stores
// it: the LSN as text in binlog_file and as a number in start_pos and
// end_pos, every value PostgreSQL's text. An UPDATE's and a DELETE's stored
// key is the before-image's.
func pgEv(id, lsn uint64, typ event.EventType, before, after map[string]any) query.ResultRow {
	ev := query.ResultRow{EventID: id, BinlogFile: lsnText(lsn), StartPos: lsn, EndPos: lsn,
		EventTimestamp: foldBase, SchemaName: "mydb", TableName: "orders", EventType: typ,
		RowBefore: before, RowAfter: after}
	if typ == event.EventInsert {
		ev.PKValues = event.BuildPKValues(pkColsIntID(), after)
	} else {
		ev.PKValues = event.BuildPKValues(pkColsIntID(), before)
	}
	return ev
}

func pgImg(id, status string) map[string]any {
	return map[string]any{"id": id, "status": status}
}

func pgUpd(id, lsn uint64, from, to, status string) query.ResultRow {
	return pgEv(id, lsn, event.EventUpdate, pgImg(from, "before"), pgImg(to, status))
}

func pgIns(id, lsn uint64, key, status string) query.ResultRow {
	return pgEv(id, lsn, event.EventInsert, nil, pgImg(key, status))
}

func pgDel(id, lsn uint64, key string) query.ResultRow {
	return pgEv(id, lsn, event.EventDelete, pgImg(key, "gone"), nil)
}

// foldPGPages folds pages into a fresh result with the PostgreSQL policy.
func foldPGPages(t *testing.T, pkCols []metadata.ColumnMeta, pages ...[]query.ResultRow) *foldResult {
	t.Helper()
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	for i, p := range pages {
		if err := foldPage(p, "mydb", "orders", pkCols, nil, foldOptions{pg: true}, res); err != nil {
			t.Fatalf("foldPage(page %d): %v", i, err)
		}
	}
	return res
}

// wantPGState checks the change map: a key mapped to "" must be dead (its
// last change a DELETE with no image), any other value is the status its
// surviving image holds, under its own key; nothing else may be in the map.
func wantPGState(t *testing.T, pkCols []metadata.ColumnMeta, res *foldResult, want map[string]string) {
	t.Helper()
	for key, status := range want {
		ev := res.Changes[key]
		if ev == nil {
			t.Errorf("key %q is not in the change map, want %q", key, status)
			continue
		}
		if ev.RowBefore != nil {
			t.Errorf("key %q kept a before-image", key)
		}
		if status == "" {
			if ev.EventType != event.EventDelete || ev.RowAfter != nil {
				t.Errorf("key %q = %v %v, want dead", key, ev.EventType, ev.RowAfter)
			}
			continue
		}
		if ev.EventType == event.EventDelete {
			t.Errorf("key %q is dead, want alive with %q", key, status)
			continue
		}
		if got := ev.RowAfter["status"]; got != status {
			t.Errorf("key %q = %q, want %q", key, got, status)
		}
		if got := event.BuildPKValues(pkCols, ev.RowAfter); got != key {
			t.Errorf("key %q holds an image whose key is %q", key, got)
		}
	}
	if len(res.Changes) != len(want) {
		t.Errorf("change map holds %d keys, want %d: %v", len(res.Changes), len(want), keysOf(res.Changes))
	}
}

// arrivals is every order the same events can reach the fold in that the
// test cares about: as written, reversed (a commit time that runs against the
// LSN, or a transaction's rows read back to front), each over one page and
// over one page per event.
func arrivals(evs []query.ResultRow) map[string][][]query.ResultRow {
	rev := slices.Clone(evs)
	slices.Reverse(rev)
	perEvent := func(in []query.ResultRow) [][]query.ResultRow {
		var out [][]query.ResultRow
		for _, e := range in {
			out = append(out, []query.ResultRow{e})
		}
		return out
	}
	return map[string][][]query.ResultRow{
		"in LSN order, one page":         {slices.Clone(evs)},
		"in LSN order, a page each":      perEvent(evs),
		"against LSN order, one page":    {rev},
		"against LSN order, a page each": perEvent(rev),
	}
}

// Every shape of the issue's list. The state each must leave is the one the
// source holds after the same statements, whatever order the events arrive
// in: the LSN decides, and inside one transaction the event id.
func TestFoldPage_pgKeyChange_shapes_2760(t *testing.T) {
	cases := []struct {
		name string
		evs  []query.ResultRow
		want map[string]string
	}{
		{"a lone key change", []query.ResultRow{pgUpd(1, 0x100, "1", "2", "moved")}, map[string]string{"1": "", "2": "moved"}},
		{"changed twice, A to B to C", []query.ResultRow{pgUpd(1, 0x100, "1", "2", "moved"), pgUpd(2, 0x200, "2", "3", "again")},
			map[string]string{"1": "", "2": "", "3": "again"}},
		{"changed and changed back, A to B to A", []query.ResultRow{pgUpd(1, 0x100, "1", "2", "away"), pgUpd(2, 0x200, "2", "1", "back")},
			map[string]string{"1": "back", "2": ""}},
		{"the new key was deleted earlier", []query.ResultRow{pgDel(1, 0x100, "2"), pgUpd(2, 0x200, "1", "2", "took its place")},
			map[string]string{"1": "", "2": "took its place"}},
		{"the new key is deleted later", []query.ResultRow{pgUpd(1, 0x100, "1", "2", "moved"), pgDel(2, 0x200, "2")},
			map[string]string{"1": "", "2": ""}},
		{"the old key is inserted again later", []query.ResultRow{pgUpd(1, 0x100, "1", "2", "moved"), pgIns(2, 0x200, "1", "reinserted")},
			map[string]string{"1": "reinserted", "2": "moved"}},
		{"the new key is updated later", []query.ResultRow{pgUpd(1, 0x100, "1", "2", "moved"), pgUpd(2, 0x200, "2", "2", "final")},
			map[string]string{"1": "", "2": "final"}},
		{"two rows swap keys through a third", []query.ResultRow{
			pgUpd(1, 0x100, "1", "99", "was one"), pgUpd(2, 0x200, "2", "1", "was two"), pgUpd(3, 0x300, "99", "2", "was one")},
			map[string]string{"1": "was two", "2": "was one", "99": ""}},
		{"one transaction: key change, a change of the moved row, the key changed back", []query.ResultRow{
			pgUpd(1, 0x100, "1", "2", "away"), pgUpd(2, 0x100, "2", "2", "touched"), pgUpd(3, 0x100, "2", "1", "back")},
			map[string]string{"1": "back", "2": ""}},
		{"one transaction: delete a row, move another onto its key", []query.ResultRow{
			pgDel(1, 0x100, "2"), pgUpd(2, 0x100, "1", "2", "took its place")},
			map[string]string{"1": "", "2": "took its place"}},
		{"an LSN past the 32-bit half is later than one below it", []query.ResultRow{
			pgUpd(1, 0xFFFFFFFF, "1", "2", "moved"), pgDel(2, 0x100000005, "2")},
			map[string]string{"1": "", "2": ""}},
	}
	for _, c := range cases {
		for how, pages := range arrivals(c.evs) {
			t.Run(c.name+"/"+how, func(t *testing.T) {
				wantPGState(t, pkColsIntID(), foldPGPages(t, pkColsIntID(), pages...), c.want)
			})
		}
	}
}

// A commit's time can run against its LSN: PostgreSQL takes the time before
// it writes the commit record. The DELETE of key 2 committed later (higher
// LSN) and carries the earlier time, so it arrives first. The LSN decides.
func TestFoldPage_pgKeyChange_commitTimeAgainstLSN_2760(t *testing.T) {
	u := pgUpd(7, 0x100, "1", "2", "moved")
	u.EventTimestamp = foldBase.Add(time.Second)
	d := pgDel(9, 0x200, "2")
	d.EventTimestamp = foldBase
	res := foldPGPages(t, pkColsIntID(), []query.ResultRow{d, u})
	wantPGState(t, pkColsIntID(), res, map[string]string{"1": "", "2": ""})
}

// A key change counts once, as the UPDATE it is (#2505), and reports both
// keys as touched.
func TestFoldPage_pgKeyChange_countsAndTouched_2760(t *testing.T) {
	var touched []string
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	opts := foldOptions{pg: true, touched: func(k string) { touched = append(touched, k) }}
	if err := foldPage([]query.ResultRow{pgUpd(1, 0x100, "1", "2", "moved")}, "mydb", "orders", pkColsIntID(), nil, opts, res); err != nil {
		t.Fatal(err)
	}
	if res.Inserts != 0 || res.Updates != 1 || res.Deletes != 0 {
		t.Errorf("counted %d inserts, %d updates, %d deletes, want one update", res.Inserts, res.Updates, res.Deletes)
	}
	if !slices.Equal(touched, []string{"1", "2"}) {
		t.Errorf("touched %v, want the old key then the new one", touched)
	}
	d, i := res.Changes["1"], res.Changes["2"]
	if d == nil || i == nil {
		t.Fatalf("change map: %v", keysOf(res.Changes))
	}
	if d.StartPos != 0x100 || i.StartPos != 0x100 || d.EventID != 1 || i.EventID != 1 || d.BinlogFile != "0/100" || i.BinlogFile != "0/100" {
		t.Errorf("the halves must keep the UPDATE's LSN and event id: delete %+v, insert %+v", d, i)
	}
	if d.ChangedColumns != nil || i.ChangedColumns != nil {
		t.Error("a DELETE and an INSERT carry no changed columns")
	}
}

// A composite key where one column changes: the old pair is dead, the new
// pair alive, and a later change of the new pair wins over the INSERT half.
func TestFoldPage_pgKeyChange_compositeKey_2760(t *testing.T) {
	pk := []metadata.ColumnMeta{
		{Name: "tenant", OrdinalPosition: 1, IsPK: true, DataType: "integer"},
		{Name: "code", OrdinalPosition: 2, IsPK: true, DataType: "text"},
	}
	img := func(tenant, code, status string) map[string]any {
		return map[string]any{"tenant": tenant, "code": code, "status": status}
	}
	ev := func(id, lsn uint64, typ event.EventType, before, after map[string]any) query.ResultRow {
		e := pgEv(id, lsn, typ, before, after)
		src := before
		if src == nil {
			src = after
		}
		e.PKValues = event.BuildPKValues(pk, src)
		return e
	}
	move := ev(1, 0x100, event.EventUpdate, img("7", "a|b", "old"), img("7", `c\d`, "moved"))
	later := ev(2, 0x200, event.EventUpdate, img("7", `c\d`, "moved"), img("7", `c\d`, "final"))
	for how, pages := range arrivals([]query.ResultRow{move, later}) {
		t.Run(how, func(t *testing.T) {
			res := foldPGPages(t, pk, pages...)
			wantPGState(t, pk, res, map[string]string{`7|a\|b`: "", `7|c\\d`: "final"})
		})
	}
}

// The spelling proof, per key type: each row is stored the way the index
// stores it (the key spelled by event.BuildPKValues from the captured text,
// the images through encoding/json) and read the way the fold reads it
// (query.UnmarshalRowImage). The new key the split files the INSERT under
// must be the key a later real change of that row is stored under. The real
// capture of these types is in the consoleapp integration test.
func TestSplitPGKeyChange_keyTypes_2760(t *testing.T) {
	pk := []metadata.ColumnMeta{{Name: "k", OrdinalPosition: 1, IsPK: true}}
	cases := []struct{ name, from, to string }{
		{"integer", "1", "2"},
		{"bigint past 2^53", "9007199254740993", "9007199254740995"},
		{"numeric keeps its scale", "10.50", "10.5"},
		{"text that reads as a number", "007", "7"},
		{"text with non-ASCII", "año", "naïve ☃ 雪"},
		{"text with the key separators", `a|b`, `a\|b`},
		{"text that reads as JSON", `[1,2]`, `{"a":1}`},
		{"empty text", "x", ""},
		{"uuid", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12"},
		{"timestamptz", "2026-03-01 10:00:00+00", "2026-03-01 10:00:00.5+00"},
		{"bytea", `\x00ff`, `\x00fe`},
		{"boolean", "t", "f"},
	}
	stored := func(m map[string]any) map[string]any {
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return query.UnmarshalRowImage(raw)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := map[string]any{"k": c.from, "v": "x"}
			after := map[string]any{"k": c.to, "v": nil}
			u := query.ResultRow{EventID: 5, BinlogFile: lsnText(0x500), StartPos: 0x500, EndPos: 0x500, EventType: event.EventUpdate,
				SchemaName: "mydb", TableName: "orders",
				PKValues: event.BuildPKValues(pk, before), RowBefore: stored(before), RowAfter: stored(after)}
			halves, err := splitPGKeyChange(&u, "mydb", "orders", pk)
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if len(halves) != 2 {
				t.Fatalf("got %d halves", len(halves))
			}
			del, ins := halves[0], halves[1]
			if del.EventType != event.EventDelete || del.PKValues != u.PKValues || del.RowAfter != nil {
				t.Errorf("the first half must be the DELETE of the stored old key: %+v", del)
			}
			// What capture stores for the next change of the moved row.
			next := event.BuildPKValues(pk, after)
			if ins.EventType != event.EventInsert || ins.PKValues != next || ins.RowBefore != nil {
				t.Errorf("the second half must be the INSERT under %q: %+v", next, ins)
			}
			if got := ins.RowAfter["k"]; got != c.to {
				t.Errorf("the INSERT's image holds key %v, want %q", got, c.to)
			}
			if v, ok := ins.RowAfter["v"]; !ok || v != nil {
				t.Errorf("the INSERT's image must keep a NULL column as NULL: %v %v", v, ok)
			}
		})
	}
}

// pgKeyRefusal asserts the #782 refusal text, unchanged, followed by why
// this event was not split and the PostgreSQL remedy.
func pgKeyRefusal(t *testing.T, err error, why string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected the refusal (%s), got nil", why)
	}
	for _, want := range []string{"PK-changing UPDATE", "mydb.orders", "would duplicate or resurrect rows", "On PostgreSQL", why, "bintrail-pg baseline"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
}

// Every case the split cannot prove keeps the refusal, with its reason, and
// leaves nothing in the change map.
func TestSplitPGKeyChange_refusals_2760(t *testing.T) {
	marker := map[string]any{event.UnchangedToastKey: true}
	cases := []struct {
		name string
		mut  func(u *query.ResultRow)
		why  string
	}{
		{"no LSN number", func(u *query.ResultRow) { u.StartPos = 0 }, "no commit LSN"},
		{"an LSN text that is not the number's", func(u *query.ResultRow) { u.BinlogFile = "0/999" }, "no commit LSN"},
		{"no LSN text", func(u *query.ResultRow) { u.BinlogFile = "" }, "no commit LSN"},
		{"a binary log coordinate", func(u *query.ResultRow) { u.BinlogFile = "binlog.000009" }, "no commit LSN"},
		{"no event id", func(u *query.ResultRow) { u.EventID = 0 }, "no event id"},
		{"a before-image with the key only", func(u *query.ResultRow) { u.RowBefore = map[string]any{"id": "1"} }, "not captured whole"},
		{"a before-image without one column", func(u *query.ResultRow) {
			u.RowBefore = map[string]any{"id": "1", "other": "x"}
		}, "the old row has no column status"},
		{"a before-image whose key is NULL", func(u *query.ResultRow) { u.RowBefore["id"] = nil }, "the old row's key column id is missing"},
		{"a before-image without the key column", func(u *query.ResultRow) { delete(u.RowBefore, "id") }, "the old row's key column id is missing"},
		{"an after-image whose key is NULL", func(u *query.ResultRow) { u.RowAfter["id"] = nil }, "the new row's key column id is missing"},
		{"an after-image without the key column", func(u *query.ResultRow) { delete(u.RowAfter, "id") }, "the new row's key column id is missing"},
		{"an unchanged-TOAST marker in the after-image", func(u *query.ResultRow) { u.RowAfter["status"] = marker }, "unchanged large value"},
		{"an unchanged-TOAST marker as the new key", func(u *query.ResultRow) { u.RowAfter["id"] = marker }, "unchanged large value"},
		{"an unchanged-TOAST marker in the before-image", func(u *query.ResultRow) { u.RowBefore["status"] = marker }, "unchanged large value"},
		{"a key that is a number in the image", func(u *query.ResultRow) { u.RowAfter["id"] = json.Number("2") }, "is not text"},
		{"an old key that is a number in the image", func(u *query.ResultRow) { u.RowBefore["id"] = json.Number("1") }, "is not text"},
		{"a new key that is not valid UTF-8", func(u *query.ResultRow) { u.RowAfter["id"] = "caf\xe9" }, "not valid UTF-8"},
		{"an old key that is not valid UTF-8", func(u *query.ResultRow) { u.RowBefore["id"] = "caf\xe9"; u.PKValues = "caf\xe9" }, "not valid UTF-8"},
		{"a stored key the before-image does not spell", func(u *query.ResultRow) { u.PKValues = "01" }, `stores the old key as "01"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := pgUpd(1, 0x100, "1", "2", "moved")
			c.mut(&u)
			halves, err := splitPGKeyChange(&u, "mydb", "orders", pkColsIntID())
			pgKeyRefusal(t, err, c.why)
			if halves != nil {
				t.Errorf("a refused event returned halves: %+v", halves)
			}
			// Through the fold, behind a change that would have overwritten it:
			// still refused, nothing filed. (The fold's own TOAST guard may
			// answer first for a marker; either way it is a refusal.)
			res := &foldResult{Changes: map[string]*query.ResultRow{}}
			u2 := pgUpd(1, 0x100, "1", "2", "moved")
			c.mut(&u2)
			page := []query.ResultRow{u2, pgDel(2, 0x200, "1"), pgDel(3, 0x300, "2")}
			if err := foldPage(page, "mydb", "orders", pkColsIntID(), nil, foldOptions{pg: true}, res); err == nil {
				t.Error("the fold applied a page holding the refused event")
			}
			if len(res.Changes) != 0 {
				t.Errorf("a refused page left %d entries", len(res.Changes))
			}
		})
	}
}

// An old row with no value outside the key has the same shape as a key-only
// old row, which PostgreSQL sends under a weaker replica identity and from
// which capture would have filled an unchanged large value with NULL. Such
// an event is split only when the new row holds no NULL outside the key.
func TestSplitPGKeyChange_oldRowThatMayBeKeyOnly_2760(t *testing.T) {
	ev := func(before, after map[string]any) query.ResultRow {
		return pgEv(1, 0x100, event.EventUpdate, before, after)
	}
	cases := []struct {
		name          string
		before, after map[string]any
		refusedOn     string // "" = split
	}{
		{"every other column NULL in both rows", map[string]any{"id": "1", "body": nil, "status": nil}, map[string]any{"id": "2", "body": nil, "status": nil}, "column body is NULL"},
		{"every other column NULL in the old row, one NULL in the new", map[string]any{"id": "1", "body": nil, "status": nil}, map[string]any{"id": "2", "body": nil, "status": "set"}, "column body is NULL"},
		{"every other column NULL in the old row, none in the new", map[string]any{"id": "1", "body": nil, "status": nil}, map[string]any{"id": "2", "body": "b", "status": "set"}, ""},
		{"one value outside the key in the old row", map[string]any{"id": "1", "body": nil, "status": "s"}, map[string]any{"id": "2", "body": nil, "status": nil}, ""},
		{"a table of key columns only", map[string]any{"id": "1"}, map[string]any{"id": "2"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := ev(c.before, c.after)
			halves, err := splitPGKeyChange(&u, "mydb", "orders", pkColsIntID())
			if c.refusedOn == "" {
				if err != nil || len(halves) != 2 {
					t.Fatalf("want the split, got %v, %v", halves, err)
				}
				return
			}
			pgKeyRefusal(t, err, "cannot be told from one captured with the key only")
			pgKeyRefusal(t, err, c.refusedOn)
		})
	}
}

// What is not a key change is not touched: an INSERT, a DELETE, an UPDATE
// that keeps its key, and an UPDATE with no before-image (stored under its
// after-image's key by capture).
func TestSplitPGKeyChange_leavesOtherEventsAlone_2760(t *testing.T) {
	noBefore := pgUpd(4, 0x400, "1", "1", "x")
	noBefore.RowBefore = nil
	for _, ev := range []query.ResultRow{pgIns(1, 0x100, "1", "x"), pgDel(2, 0x200, "1"), pgUpd(3, 0x300, "1", "1", "x"), noBefore} {
		halves, err := splitPGKeyChange(&ev, "mydb", "orders", pkColsIntID())
		if halves != nil || err != nil {
			t.Errorf("%v: got %v, %v; want nothing", ev.EventType, halves, err)
		}
	}
}

// The order of two PostgreSQL rows: by LSN, then by event id, and no order
// at all when either has no usable LSN. MySQL rows keep their own rule.
func TestLaterInWAL_2760(t *testing.T) {
	row := func(id, lsn uint64) *query.ResultRow {
		return &query.ResultRow{EventID: id, BinlogFile: lsnText(lsn), StartPos: lsn}
	}
	cases := []struct {
		name           string
		a, b           *query.ResultRow
		aLater, bLater bool
	}{
		{"a higher LSN is later", row(1, 0x200), row(9, 0x100), true, false},
		{"across the 32-bit half", row(1, 0x100000005), row(9, 0xFFFFFFFF), true, false},
		{"one LSN: the higher event id is later", row(8, 0x100), row(7, 0x100), true, false},
		{"the same row twice is not later than itself", row(7, 0x100), row(7, 0x100), false, false},
		{"no LSN on one side: no order", row(8, 0x200), &query.ResultRow{EventID: 7, BinlogFile: "0/100"}, false, false},
		{"an LSN text that disagrees with the number: no order", row(8, 0x200), &query.ResultRow{EventID: 7, BinlogFile: "0/100", StartPos: 0x300}, false, false},
		{"a MySQL row on one side: no order", row(8, 0x200), &query.ResultRow{EventID: 7, BinlogFile: "binlog.000001", StartPos: 4}, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := laterInWAL(c.a, c.b); got != c.aLater {
				t.Errorf("laterInWAL(a, b) = %v, want %v", got, c.aLater)
			}
			if got := laterInWAL(c.b, c.a); got != c.bLater {
				t.Errorf("laterInWAL(b, a) = %v, want %v", got, c.bLater)
			}
			if got := laterChange(c.a, c.b); got != c.aLater {
				t.Errorf("laterChange(a, b) = %v, want %v", got, c.aLater)
			}
		})
	}
	// Two MySQL rows: laterChange is query.LaterInBinlog, both ways, for
	// every pair of these.
	my := []*query.ResultRow{
		{EventID: 1, BinlogFile: "binlog.000001", StartPos: 4},
		{EventID: 2, BinlogFile: "binlog.000001", StartPos: 4},
		{EventID: 3, BinlogFile: "binlog.000001", StartPos: 900},
		{EventID: 4, BinlogFile: "binlog.000002", StartPos: 4},
		{EventID: 5, BinlogFile: "", StartPos: 4},
		{EventID: 6, BinlogFile: "binlog.000002", StartPos: 1<<64 - 30},
	}
	for _, a := range my {
		for _, b := range my {
			if laterInWAL(a, b) {
				t.Errorf("laterInWAL ordered two MySQL rows: %+v, %+v", a, b)
			}
			if got, want := laterChange(a, b), query.LaterInBinlog(a, b); got != want {
				t.Errorf("laterChange(%+v, %+v) = %v, LaterInBinlog says %v", a, b, got, want)
			}
		}
	}
}

// Changes written to disk are chosen again by the same rule when a group is
// read back: the first drain holds the later LSN, and keeps both keys.
func TestChangeSpill_pgKeyChange_2760(t *testing.T) {
	later := foldPGPages(t, pkColsIntID(), []query.ResultRow{pgUpd(9, 0x900, "1", "2", "moved")})
	earlier := foldPGPages(t, pkColsIntID(), []query.ResultRow{pgIns(1, 0x100, "1", "old one"), pgDel(2, 0x200, "2")})
	got := loadAll(t, spillOf(t, 10, later.Changes, earlier.Changes))
	if ev := got["1"]; ev == nil || ev.EventType != event.EventDelete || ev.RowAfter != nil {
		t.Errorf("key 1 = %+v, want the DELETE half at the later LSN", ev)
	}
	if ev := got["2"]; ev == nil || ev.EventType != event.EventInsert || ev.RowAfter["status"] != "moved" {
		t.Errorf("key 2 = %+v, want the INSERT half at the later LSN", ev)
	}
	if len(got) != 2 {
		t.Errorf("read back %d entries, want 2", len(got))
	}
}

// The MySQL rule and the PostgreSQL rule stay apart. A MySQL window never
// asks the PostgreSQL rule, which would refuse every MySQL key change for
// having no LSN; and the Iceberg export, which still passes pg to
// splitPKChange, keeps the plain #782 refusal for a PostgreSQL event the
// full-table fold now splits.
func TestSplitPKChange_mysqlAndExportUnchanged_2760(t *testing.T) {
	my := pkUpd(1, 100, 1, 2, "moved")
	if _, err := splitPGKeyChange(&my, "mydb", "orders", pkColsIntID()); err == nil {
		t.Fatal("the PostgreSQL rule split a MySQL event")
	}
	halves, err := splitPKChange(&my, "mydb", "orders", pkColsIntID(), false)
	if err != nil || len(halves) != 2 {
		t.Fatalf("the MySQL rule: %v, %v", halves, err)
	}
	res := foldPK(t, []query.ResultRow{pkUpd(1, 100, 1, 2, "moved")})
	wantState(t, res, map[int]string{1: "", 2: "moved"})

	pg := pgUpd(1, 0x100, "1", "2", "moved")
	halves, err = SplitPKChange(&pg, "mydb", "orders", pkColsIntID(), true)
	pkRefusal(t, err, "1", "2")
	if halves != nil || strings.Contains(err.Error(), "On PostgreSQL") {
		t.Errorf("the export seam must keep the plain refusal: %v\n%v", halves, err)
	}
	// And a PostgreSQL event in a MySQL fold is not ordered or split by the
	// PostgreSQL rule either: the MySQL rule sees it, as before.
	if halves, err := splitPKChange(&pg, "mydb", "orders", pkColsIntID(), false); err != nil || len(halves) != 2 {
		t.Errorf("splitPKChange(pg=false) changed: %v, %v", halves, err)
	}
}

// The refusal's own text: the #782 sentence first, word for word, then the
// reason and the remedy.
func TestPGKeyChangeErr_2760(t *testing.T) {
	base := pkChangingUpdateErr("mydb", "orders", "1", "2").Error()
	err := pgKeyChangeErr("mydb", "orders", "1", "2", "the reason")
	if !strings.HasPrefix(err.Error(), base+". ") {
		t.Errorf("the refusal must start with the #782 text:\n%s", err)
	}
	pkRefusal(t, err, "1", "2")
	pgKeyRefusal(t, err, "the reason")
}
