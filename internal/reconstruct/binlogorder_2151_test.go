package reconstruct

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2151: pages reach the fold in (event_timestamp, event_id) order, and for
// one row that order can disagree with the binary log. The row keeps the
// change the binary log holds last.

// orderEvent is an update of row pk to v, started `started` seconds into the
// window and written to the binary log at file:pos.
func orderEvent(id uint64, started int, file string, pos uint64, pk, v string) query.ResultRow {
	return query.ResultRow{
		EventID: id, BinlogFile: file, StartPos: pos, EndPos: pos + 50,
		EventTimestamp: foldBase.Add(time.Duration(started) * time.Second),
		SchemaName:     "mydb", TableName: "orders", PKValues: pk,
		EventType: event.EventUpdate,
		RowBefore: map[string]any{"id": pk, "v": "old"},
		RowAfter:  map[string]any{"id": pk, "v": v},
	}
}

func foldPages(t *testing.T, pages ...[]query.ResultRow) *foldResult {
	t.Helper()
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	for i, p := range pages {
		if err := foldPage(p, "mydb", "orders", pkColsIntID(), nil, res); err != nil {
			t.Fatalf("foldPage(page %d): %v", i, err)
		}
	}
	return res
}

func keptValue(t *testing.T, res *foldResult, pk string) any {
	t.Helper()
	ev := res.Changes[pk]
	if ev == nil {
		t.Fatalf("row %s is not in the change map", pk)
	}
	return ev.RowAfter["v"]
}

func TestFoldPage_keepsTheChangeTheBinlogHoldsLast(t *testing.T) {
	const f = "binlog.000007"
	// The shape seen on a real server: B's statement started first (second 0),
	// waited on A's row lock, and was written after A (position 300). A's
	// statement started two seconds later and is at position 100.
	b := orderEvent(2, 0, f, 300, "1", "B")
	a := orderEvent(1, 2, f, 100, "1", "A")

	t.Run("in one page", func(t *testing.T) {
		if got := keptValue(t, foldPages(t, []query.ResultRow{b, a}), "1"); got != "B" {
			t.Fatalf("row 1 = %v, want B: the change at the higher position", got)
		}
	})
	t.Run("across two pages", func(t *testing.T) {
		if got := keptValue(t, foldPages(t, []query.ResultRow{b}, []query.ResultRow{a}), "1"); got != "B" {
			t.Fatalf("row 1 = %v, want B: the change at the higher position", got)
		}
	})
	t.Run("the kept change holds its position", func(t *testing.T) {
		// A third change arrives later still and is between the two in the
		// binary log: the fold can only tell if the kept entry carries B's
		// position, which retainEvent must not blank.
		c := orderEvent(3, 5, f, 200, "1", "C")
		if got := keptValue(t, foldPages(t, []query.ResultRow{b}, []query.ResultRow{a}, []query.ResultRow{c}), "1"); got != "B" {
			t.Fatalf("row 1 = %v, want B", got)
		}
	})
	t.Run("time and position agree: the later one replaces", func(t *testing.T) {
		x := orderEvent(1, 0, f, 100, "1", "X")
		y := orderEvent(2, 2, f, 300, "1", "Y")
		if got := keptValue(t, foldPages(t, []query.ResultRow{x}, []query.ResultRow{y}), "1"); got != "Y" {
			t.Fatalf("row 1 = %v, want Y", got)
		}
	})
	t.Run("a later file wins over a higher position in an earlier one", func(t *testing.T) {
		later := orderEvent(2, 0, "binlog.000008", 4, "1", "next-file")
		earlier := orderEvent(1, 2, f, 900000, "1", "prev-file")
		if got := keptValue(t, foldPages(t, []query.ResultRow{later, earlier}), "1"); got != "next-file" {
			t.Fatalf("row 1 = %v, want next-file", got)
		}
	})
	t.Run("the file suffix grew a digit", func(t *testing.T) {
		// binlog.1000000 follows binlog.999999 and sorts before it as text.
		later := orderEvent(2, 0, "binlog.1000000", 4, "1", "next-file")
		earlier := orderEvent(1, 2, "binlog.999999", 900000, "1", "prev-file")
		if got := keptValue(t, foldPages(t, []query.ResultRow{later, earlier}), "1"); got != "next-file" {
			t.Fatalf("row 1 = %v, want next-file", got)
		}
	})
	t.Run("the same position: the one that arrives last", func(t *testing.T) {
		// Two images of one rows event, and rows with no coordinates at all.
		for _, file := range []string{f, ""} {
			x := orderEvent(1, 0, file, 100, "1", "first")
			y := orderEvent(2, 0, file, 100, "1", "second")
			if got := keptValue(t, foldPages(t, []query.ResultRow{x, y}), "1"); got != "second" {
				t.Fatalf("file %q: row 1 = %v, want second", file, got)
			}
		}
	})
	t.Run("positions that are not places: the one that arrives last", func(t *testing.T) {
		// MariaDB 11.4 rows from a build before #1180 hold 2^64 - event size
		// as their start. A bigger event gives a smaller number; neither says
		// where the event is.
		x := orderEvent(1, 0, f, 1<<64-40, "1", "first")
		y := orderEvent(2, 2, f, 1<<64-60, "1", "second")
		if got := keptValue(t, foldPages(t, []query.ResultRow{x}, []query.ResultRow{y}), "1"); got != "second" {
			t.Fatalf("row 1 = %v, want second", got)
		}
		z := orderEvent(3, 4, f, 300, "1", "third")
		if got := keptValue(t, foldPages(t, []query.ResultRow{x}, []query.ResultRow{z}), "1"); got != "third" {
			t.Fatalf("row 1 = %v, want third", got)
		}
	})
	t.Run("one compressed transaction: the higher id", func(t *testing.T) {
		// Every row event of a compressed transaction carries the payload
		// event's coordinate. A statement of it that ran under an earlier
		// SET TIMESTAMP arrives first; capture read it second.
		second := orderEvent(2, 0, f, 100, "1", "second")
		first := orderEvent(1, 3, f, 100, "1", "first")
		if got := keptValue(t, foldPages(t, []query.ResultRow{second}, []query.ResultRow{first}), "1"); got != "second" {
			t.Fatalf("row 1 = %v, want second", got)
		}
	})
	t.Run("other rows are untouched", func(t *testing.T) {
		other := orderEvent(3, 1, f, 50, "2", "other")
		res := foldPages(t, []query.ResultRow{b, other, a})
		if got := keptValue(t, res, "2"); got != "other" {
			t.Fatalf("row 2 = %v, want other", got)
		}
		if got := keptValue(t, res, "1"); got != "B" {
			t.Fatalf("row 1 = %v, want B", got)
		}
	})
}

// A change that loses to a later one in the binary log is still read by every
// guard: only what the map keeps changes.
func TestFoldPage_aSupersededChangeStillReachesTheGuards(t *testing.T) {
	const f = "binlog.000007"
	b := orderEvent(2, 0, f, 300, "1", "B")

	t.Run("a primary key change refuses", func(t *testing.T) {
		a := orderEvent(1, 2, f, 100, "1", "A")
		a.RowBefore = map[string]any{"id": float64(1), "v": "old"}
		a.RowAfter = map[string]any{"id": float64(2), "v": "A"}
		res := &foldResult{Changes: map[string]*query.ResultRow{}}
		err := foldPage([]query.ResultRow{b, a}, "mydb", "orders", pkColsIntID(), nil, res)
		if err == nil || !strings.Contains(err.Error(), "PK-changing UPDATE") {
			t.Fatalf("err = %v, want the PK-changing UPDATE refusal", err)
		}
	})
	t.Run("its columns are counted", func(t *testing.T) {
		a := orderEvent(1, 2, f, 100, "1", "A")
		delete(a.RowAfter, "v")
		delete(a.RowBefore, "v")
		res := foldPages(t, []query.ResultRow{b, a})
		if _, ok := res.ImageColumns["v"]; ok {
			t.Fatalf("ImageColumns = %v: column v is missing from the superseded change's images and must not be in the intersection", res.ImageColumns)
		}
		if got := keptValue(t, res, "1"); got != "B" {
			t.Fatalf("row 1 = %v, want B", got)
		}
	})
}

// Past the in-memory limit a row changed on both sides of a drain has one
// record from each on disk. Reading the group back makes the same choice.
func TestChangeSpill_keepsTheChangeTheBinlogHoldsLast(t *testing.T) {
	const f = "binlog.000007"
	ptr := func(r query.ResultRow) *query.ResultRow { return &r }
	b := orderEvent(2, 0, f, 300, "1", "B")
	a := orderEvent(1, 2, f, 100, "1", "A")
	c := orderEvent(3, 5, f, 200, "1", "C")

	got := loadAll(t, spillOf(t, 10,
		map[string]*query.ResultRow{"1": ptr(b)},
		map[string]*query.ResultRow{"1": ptr(a)},
		map[string]*query.ResultRow{"1": ptr(c)},
	))
	if ev := got["1"]; ev == nil || ev.RowAfter["v"] != "B" || ev.EventID != 2 {
		t.Fatalf("row 1 = %+v, want B (event 2): the record at the higher position, written first", ev)
	}

	// The fold and the spill together, the way admitPage drives them: B is
	// folded and drained, then A arrives on a later page.
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	t.Cleanup(res.close)
	for _, page := range [][]query.ResultRow{{b, orderEvent(4, 0, f, 310, "2", "x")}, {a}} {
		if err := foldPage(page, "mydb", "orders", pkColsIntID(), nil, res); err != nil {
			t.Fatal(err)
		}
		if err := res.admitPage(1, 1, true); err != nil {
			t.Fatal(err)
		}
	}
	if res.Spill == nil {
		t.Fatal("the fold did not spill")
	}
	if err := res.Spill.finish(); err != nil {
		t.Fatal(err)
	}
	if ev := loadAll(t, res.Spill)["1"]; ev == nil || ev.RowAfter["v"] != "B" {
		t.Fatalf("row 1 after a spilled fold = %+v, want B", ev)
	}
}
