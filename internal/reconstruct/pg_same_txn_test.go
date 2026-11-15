package reconstruct

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A DEFERRABLE primary key lets two rows share a key value inside one
// transaction with no key change involved: an INSERT of a new row 1 while
// the old row 1 is still there, then a DELETE of the OLD one. The fold kept
// the last change, the DELETE, and lost the new row. The changes of one key
// in one transaction (one commit LSN) are now checked to describe ONE row:
// each change must start from what the change before it left (pgKeyTxn).

const sameTxnLSN = 0x700

func stRow(status string) map[string]any { return map[string]any{"id": "1", "status": status} }

func stIns(id uint64, status string) query.ResultRow {
	return pgEv(id, sameTxnLSN, event.EventInsert, nil, stRow(status))
}

func stUpd(id uint64, from, to string) query.ResultRow {
	return pgEv(id, sameTxnLSN, event.EventUpdate, stRow(from), stRow(to))
}

func stDel(id uint64, status string) query.ResultRow {
	return pgEv(id, sameTxnLSN, event.EventDelete, stRow(status), nil)
}

// stStored sends an event's images through the index's JSON and back, the
// way a real page reaches the fold.
func stStored(t *testing.T, ev query.ResultRow) query.ResultRow {
	t.Helper()
	for _, img := range []*map[string]any{&ev.RowBefore, &ev.RowAfter} {
		if *img == nil {
			continue
		}
		raw, err := json.Marshal(*img)
		if err != nil {
			t.Fatal(err)
		}
		*img = query.UnmarshalRowImage(raw)
	}
	return ev
}

// stOrders is how one transaction's events for one key can reach the fold:
// in event id order (the only order the fetch gives them in, since they
// share a commit time), over one page or one page per event. reversed adds
// the opposite order, which the record must answer the same way for.
func stOrders(evs []query.ResultRow, reversed bool) map[string][][]query.ResultRow {
	each := func(in []query.ResultRow) [][]query.ResultRow {
		var out [][]query.ResultRow
		for _, e := range in {
			out = append(out, []query.ResultRow{e})
		}
		return out
	}
	m := map[string][][]query.ResultRow{"one page": {slices.Clone(evs)}, "a page each": each(evs)}
	if reversed {
		rev := slices.Clone(evs)
		slices.Reverse(rev)
		m["against event id order, one page"] = [][]query.ResultRow{rev}
		m["against event id order, a page each"] = each(rev)
	}
	return m
}

// stFold folds pages in memory, or with every page moved to disk right after
// it is folded (spill) and the groups read back; it returns the changes or
// the first refusal.
func stFold(t *testing.T, pages [][]query.ResultRow, spill bool) (map[string]*query.ResultRow, error) {
	t.Helper()
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	t.Cleanup(res.close)
	for _, p := range pages {
		if err := foldPage(p, "mydb", "orders", pkColsIntID(), nil, foldOptions{pg: true}, res); err != nil {
			return nil, err
		}
		if spill {
			// A limit of zero rows held: every page goes to disk.
			if res.Spill == nil {
				s, err := newChangeSpill(0)
				if err != nil {
					t.Fatal(err)
				}
				s.schema, s.table = "mydb", "orders"
				res.Spill = s
			}
			if err := res.admit(0, 0, 1, true); err != nil {
				t.Fatal(err)
			}
			if len(res.Changes) != 0 || res.pgTxn != nil {
				t.Fatalf("the page did not go to disk: %d changes, records %v", len(res.Changes), res.pgTxn)
			}
		}
	}
	if !spill {
		// What the fold checks once the window is read.
		if err := res.pgTxnEnd("mydb", "orders"); err != nil {
			return nil, err
		}
		return res.Changes, nil
	}
	if err := res.Spill.finish(); err != nil {
		t.Fatal(err)
	}
	all := map[string]*query.ResultRow{}
	for b := range spillBuckets {
		m, err := res.Spill.load(b)
		if err != nil {
			return nil, err
		}
		for k, v := range m {
			all[k] = v
		}
	}
	return all, nil
}

// stRefused asserts the refusal: the table, the transaction, the key, the
// reason, that a deferrable key is what allows it, and the remedy.
func stRefused(t *testing.T, err error, why string) {
	t.Helper()
	if err == nil {
		t.Fatalf("folded; want the refusal (%s)", why)
	}
	for _, want := range []string{"mydb.orders", "inside one transaction (commit LSN 0/700)", `primary key "1"`, "do not describe one row", why,
		"DEFERRABLE", "two rows can hold the same key value", "bintrail-pg baseline"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
	if strings.Contains(err.Error(), "PK-changing UPDATE") {
		t.Errorf("no key changed here; the refusal must not speak of one:\n%s", err)
	}
}

const (
	stWhyNotTheRow = "starts from a row that is not the row the change before it left"
	stWhyInsert    = "a row is inserted where the change before it had left a row"
	stWhyNoRow     = "a row is changed or deleted where the change before it had left no row"
	stWhyCompare   = "cannot be compared"
)

// The ordinary sequences: one row, each change starting from what the one
// before it left. Applied, in memory and through disk, in every order; the
// key ends as the last change says ("" = no row).
func TestFoldPage_pgSameTransaction_ordinary(t *testing.T) {
	cases := []struct {
		name string
		evs  []query.ResultRow
		want string
	}{
		{"insert a row and delete it", []query.ResultRow{stIns(1, "new"), stDel(2, "new")}, ""},
		{"insert a row and update it", []query.ResultRow{stIns(1, "new"), stUpd(2, "new", "changed")}, "changed"},
		{"delete a row and insert another under its key", []query.ResultRow{stDel(1, "old"), stIns(2, "new")}, "new"},
		{"update a row twice", []query.ResultRow{stUpd(1, "old", "once"), stUpd(2, "once", "twice")}, "twice"},
		{"update a row and delete it", []query.ResultRow{stUpd(1, "old", "once"), stDel(2, "once")}, ""},
		{"an update that changes nothing, twice", []query.ResultRow{stUpd(1, "old", "old"), stUpd(2, "old", "old")}, "old"},
		{"insert, update, delete, insert again", []query.ResultRow{stIns(1, "new"), stUpd(2, "new", "changed"), stDel(3, "changed"), stIns(4, "again")}, "again"},
		{"delete, insert, update, update", []query.ResultRow{stDel(1, "old"), stIns(2, "new"), stUpd(3, "new", "a"), stUpd(4, "a", "b")}, "b"},
		{"a single change of each kind", []query.ResultRow{stUpd(1, "old", "new")}, "new"},
		{"rows with a NULL and with text outside ASCII", []query.ResultRow{
			pgEv(1, sameTxnLSN, event.EventInsert, nil, map[string]any{"id": "1", "status": nil, "note": "año ☃"}),
			pgEv(2, sameTxnLSN, event.EventUpdate, map[string]any{"id": "1", "status": nil, "note": "año ☃"}, map[string]any{"id": "1", "status": "set", "note": nil}),
			pgEv(3, sameTxnLSN, event.EventUpdate, map[string]any{"id": "1", "status": "set", "note": nil}, map[string]any{"id": "1", "status": "done", "note": ""})}, "done"},
	}
	for _, c := range cases {
		for how, pages := range stOrders(c.evs, true) {
			for _, spill := range []bool{false, true} {
				name := c.name + "/" + how + "/in memory"
				if spill {
					name = c.name + "/" + how + "/through disk"
				}
				t.Run(name, func(t *testing.T) {
					for i := range pages {
						for j := range pages[i] {
							pages[i][j] = stStored(t, pages[i][j])
						}
					}
					got, err := stFold(t, pages, spill)
					if err != nil {
						t.Fatalf("an ordinary sequence was refused: %v", err)
					}
					ev := got["1"]
					if ev == nil || len(got) != 1 {
						t.Fatalf("changes: %v", keysOf(got))
					}
					if c.want == "" {
						if ev.EventType != event.EventDelete || ev.RowAfter != nil {
							t.Errorf("key 1 = %v %v, want no row", ev.EventType, ev.RowAfter)
						}
						return
					}
					if ev.EventType == event.EventDelete || ev.RowAfter["status"] != c.want {
						t.Errorf("key 1 = %v %v, want status %q", ev.EventType, ev.RowAfter, c.want)
					}
				})
			}
		}
	}
}

// Records of several changes each, put together when they are read back
// from disk: each is linked to what the one before it left.
func TestFoldPage_pgSameTransaction_ordinaryAcrossSeveralMovesToDisk(t *testing.T) {
	pages := [][]query.ResultRow{
		{stIns(1, "a"), stUpd(2, "a", "b")},
		{stUpd(3, "b", "c"), stUpd(4, "c", "d")},
		{stUpd(5, "d", "e"), stDel(6, "e")},
		{stIns(7, "f"), stUpd(8, "f", "g")},
	}
	for _, spill := range []bool{false, true} {
		got, err := stFold(t, pages, spill)
		if err != nil || got["1"] == nil || got["1"].RowAfter["status"] != "g" {
			t.Fatalf("spill %v: %v, %v", spill, got, err)
		}
	}
	// And a link that does not hold between two such records is still seen.
	pages[2] = []query.ResultRow{stUpd(5, "other", "e"), stDel(6, "e")}
	_, err := stFold(t, pages, true)
	stRefused(t, err, stWhyNotTheRow)
}

// The sequences that do not describe one row: refused, in memory and
// through disk, in every order, with the reason.
func TestFoldPage_pgSameTransaction_ambiguous(t *testing.T) {
	noBefore := stUpd(2, "x", "changed")
	noBefore.RowBefore = nil
	number := stDel(2, "new")
	number.RowBefore = map[string]any{"id": json.Number("1"), "status": "new"}
	cases := []struct {
		name string
		evs  []query.ResultRow
		why  string
	}{
		{"a new row is inserted and the OLD row deleted", []query.ResultRow{stIns(1, "new"), stDel(2, "old")}, stWhyNotTheRow},
		{"a new row is inserted and the OLD row updated", []query.ResultRow{stIns(1, "new"), stUpd(2, "old", "old changed")}, stWhyNotTheRow},
		{"a new row is inserted, the old row updated, the new row deleted", []query.ResultRow{stIns(1, "new"), stUpd(2, "old", "old changed"), stDel(3, "new")}, stWhyNotTheRow},
		{"the old row is updated, a new row inserted, the new row deleted", []query.ResultRow{stUpd(1, "old", "old changed"), stIns(2, "new"), stDel(3, "new")}, stWhyInsert},
		{"two rows inserted under one key", []query.ResultRow{stIns(1, "new"), stIns(2, "other")}, stWhyInsert},
		{"two rows deleted under one key", []query.ResultRow{stDel(1, "old"), stDel(2, "other")}, stWhyNoRow},
		{"a row deleted and another updated under its key", []query.ResultRow{stDel(1, "old"), stUpd(2, "other", "changed")}, stWhyNoRow},
		{"an update that starts from another row than the update before it left", []query.ResultRow{stUpd(1, "old", "once"), stUpd(2, "other", "twice")}, stWhyNotTheRow},
		{"the difference is a NULL against an empty text", []query.ResultRow{
			pgEv(1, sameTxnLSN, event.EventInsert, nil, map[string]any{"id": "1", "status": nil}),
			pgEv(2, sameTxnLSN, event.EventDelete, map[string]any{"id": "1", "status": ""}, nil)}, stWhyNotTheRow},
		{"the difference is a column the before-image lacks", []query.ResultRow{
			pgEv(1, sameTxnLSN, event.EventInsert, nil, map[string]any{"id": "1", "status": "new", "note": nil}),
			pgEv(2, sameTxnLSN, event.EventDelete, map[string]any{"id": "1", "status": "new"}, nil)}, stWhyNotTheRow},
		{"a before-image with the key only", []query.ResultRow{
			stIns(1, "new"), pgEv(2, sameTxnLSN, event.EventDelete, map[string]any{"id": "1", "status": nil}, nil)}, stWhyNotTheRow},
		{"an update with no before-image after another change", []query.ResultRow{stIns(1, "new"), noBefore}, stWhyCompare},
		{"a value that is not text in an image", []query.ResultRow{stIns(1, "new"), number}, stWhyCompare},
	}
	for _, c := range cases {
		for how, pages := range stOrders(c.evs, true) {
			for _, spill := range []bool{false, true} {
				name := c.name + "/" + how + "/in memory"
				if spill {
					name = c.name + "/" + how + "/through disk"
				}
				t.Run(name, func(t *testing.T) {
					_, err := stFold(t, pages, spill)
					stRefused(t, err, c.why)
				})
			}
		}
	}
}

// A transaction delivered twice. Capture saves its position after the rows,
// so a crash between the two has PostgreSQL send the transaction again, and
// one cut at the batch size can leave its beginning behind. What the index
// holds for the key is then its changes up to some point and all of them
// again from the first. Accepted when the second run repeats the first
// change by change for as long as the first ran, and only then. In event id
// order, which is how they reach the fold.
func TestFoldPage_pgSameTransaction_deliveredTwice(t *testing.T) {
	cases := []struct {
		name string
		evs  []query.ResultRow
		want string
	}{
		{"one insert, twice", []query.ResultRow{stIns(1, "new"), stIns(11, "new")}, "new"},
		{"one update, twice", []query.ResultRow{stUpd(1, "old", "new"), stUpd(11, "old", "new")}, "new"},
		{"one delete, three times", []query.ResultRow{stDel(1, "old"), stDel(11, "old"), stDel(21, "old")}, ""},
		{"insert and update, twice", []query.ResultRow{stIns(1, "new"), stUpd(2, "new", "changed"), stIns(11, "new"), stUpd(12, "new", "changed")}, "changed"},
		{"insert and update, three times", []query.ResultRow{stIns(1, "new"), stUpd(2, "new", "changed"), stIns(11, "new"), stUpd(12, "new", "changed"),
			stIns(21, "new"), stUpd(22, "new", "changed")}, "changed"},
		{"the beginning left behind, then the whole", []query.ResultRow{stIns(1, "new"), stIns(11, "new"), stUpd(12, "new", "changed"), stDel(13, "changed")}, ""},
		{"two changes left behind, then the whole", []query.ResultRow{stIns(1, "new"), stUpd(2, "new", "changed"),
			stIns(11, "new"), stUpd(12, "new", "changed"), stUpd(13, "changed", "again")}, "again"},
		{"the beginning left behind, then the whole twice", []query.ResultRow{stIns(1, "new"),
			stIns(11, "new"), stUpd(12, "new", "changed"), stIns(21, "new"), stUpd(22, "new", "changed")}, "changed"},
	}
	for _, c := range cases {
		for how, pages := range stOrders(c.evs, false) {
			for _, spill := range []bool{false, true} {
				name := c.name + "/" + how + "/in memory"
				if spill {
					// One page is one record on disk; a page each is one
					// record per change, put together one by one: both are
					// the exact rule.
					name = c.name + "/" + how + "/through disk"
				}
				t.Run(name, func(t *testing.T) {
					got, err := stFold(t, pages, spill)
					if err != nil {
						t.Fatalf("a transaction delivered twice was refused: %v", err)
					}
					ev := got["1"]
					if ev == nil {
						t.Fatal("key 1 is not in the changes")
					}
					if (c.want == "") != (ev.EventType == event.EventDelete) || (c.want != "" && ev.RowAfter["status"] != c.want) {
						t.Errorf("key 1 = %v %v, want %q", ev.EventType, ev.RowAfter, c.want)
					}
				})
			}
		}
	}
	// A second delivery is only the FIRST change again. A later change that
	// does not link and is not the first one is still refused.
	_, err := stFold(t, [][]query.ResultRow{{stIns(1, "new"), stUpd(2, "new", "changed"), stUpd(3, "new", "changed")}}, false)
	stRefused(t, err, stWhyNotTheRow)
}

const (
	stWhyNotARepeat = "but the changes after it do not repeat the changes before it"
	stWhyRepeatOpen = "stop before they repeat the changes before it"
	stWhyRepeatSeam = "that cannot be checked here"
)

// The first change coming back is not enough to be a second delivery: what
// follows must repeat what came before, change by change.
//
// A deferrable key, row 1 not there before: INSERT a, UPDATE it to b, INSERT
// a again (allowed until commit), DELETE the row that holds a. The source
// keeps (1, b). The third change equals the first, and a rule that started
// the sequence over on that alone ended on the DELETE: no row 1, no error.
// It is not a second delivery: that would repeat INSERT, UPDATE, and here
// the second run goes INSERT, DELETE.
func TestFoldPage_pgSameTransaction_firstChangeAgainIsNotARepeat(t *testing.T) {
	cases := []struct {
		name string
		evs  []query.ResultRow
		why  string
	}{
		{"insert a, update to b, insert a, delete a", []query.ResultRow{
			stIns(1, "a"), stUpd(2, "a", "b"), stIns(3, "a"), stDel(4, "a")}, stWhyNotARepeat},
		{"insert a, update to b, insert a, update a to c, delete c", []query.ResultRow{
			stIns(1, "a"), stUpd(2, "a", "b"), stIns(3, "a"), stUpd(4, "a", "c"), stDel(5, "c")}, stWhyNotARepeat},
		{"the second run differs at its third change", []query.ResultRow{
			stIns(1, "a"), stUpd(2, "a", "b"), stUpd(3, "b", "c"), stIns(4, "a"), stUpd(5, "a", "b"), stDel(6, "b")}, stWhyNotARepeat},
		{"the second run stops before the first one's length", []query.ResultRow{
			stIns(1, "a"), stUpd(2, "a", "b"), stIns(3, "a")}, stWhyRepeatOpen},
		{"the second run stops one change short", []query.ResultRow{
			stIns(1, "a"), stUpd(2, "a", "b"), stUpd(3, "b", "c"), stIns(4, "a"), stUpd(5, "a", "b")}, stWhyRepeatOpen},
		{"the first change comes back while the second run is not complete", []query.ResultRow{
			stIns(1, "a"), stUpd(2, "a", "b"), stIns(3, "a"), stIns(4, "a"), stUpd(5, "a", "b")}, stWhyRepeatOpen},
	}
	for _, c := range cases {
		for how, pages := range stOrders(c.evs, false) {
			for _, spill := range []bool{false, true} {
				name := c.name + "/" + how + "/in memory"
				if spill {
					name = c.name + "/" + how + "/through disk"
				}
				t.Run(name, func(t *testing.T) {
					got, err := stFold(t, pages, spill)
					if err == nil {
						t.Fatalf("folded to %+v", got["1"])
					}
					stRefused(t, err, c.why)
				})
			}
		}
	}
}

// Not a first change coming back at all: after a DELETE the key holds no row,
// so an INSERT of the same row links, and the sequence is one row inserted,
// changed, deleted and inserted again.
func TestFoldPage_pgSameTransaction_sameInsertAfterADeleteIsOrdinary(t *testing.T) {
	evs := []query.ResultRow{stIns(1, "a"), stUpd(2, "a", "b"), stDel(3, "b"), stIns(4, "a"), stUpd(5, "a", "b")}
	for _, spill := range []bool{false, true} {
		got, err := stFold(t, [][]query.ResultRow{evs}, spill)
		if err != nil || got["1"] == nil || got["1"].RowAfter["status"] != "b" {
			t.Fatalf("spill %v: %v, %v", spill, got, err)
		}
	}
}

// What the rule cannot tell apart, and what it does there: the same row
// inserted twice and one of the two deleted reads, change for change, like a
// transaction cut after its first change and then delivered whole (INSERT;
// INSERT, DELETE). It is applied as that: no row. On a deferrable key the
// source keeps one row. Documented in docs/postgres.md.
func TestFoldPage_pgSameTransaction_twoEqualRowsAreNotSeen(t *testing.T) {
	for _, spill := range []bool{false, true} {
		got, err := stFold(t, [][]query.ResultRow{{stIns(1, "a"), stIns(2, "a"), stDel(3, "a")}}, spill)
		if err != nil {
			t.Fatalf("spill %v: refused: %v", spill, err)
		}
		if ev := got["1"]; ev == nil || ev.EventType != event.EventDelete {
			t.Errorf("spill %v: key 1 = %+v, want the DELETE", spill, ev)
		}
	}
}

// A second delivery whose changes were put together from more than one
// record of several changes (a move to disk in the middle) cannot be
// checked, and is refused: in memory the same pages are applied.
func TestFoldPage_pgSameTransaction_deliveredTwiceAcrossAMoveToDisk(t *testing.T) {
	pages := [][]query.ResultRow{
		{stIns(1, "new"), stUpd(2, "new", "changed")},
		{stIns(11, "new"), stUpd(12, "new", "changed")},
	}
	if got, err := stFold(t, pages, false); err != nil || got["1"].RowAfter["status"] != "changed" {
		t.Fatalf("in memory: %v, %v", got, err)
	}
	_, err := stFold(t, pages, true)
	stRefused(t, err, stWhyRepeatSeam)

	// A repeat left open on one side of the move and continued on the other,
	// and one opened inside the later record (judged there against that
	// record's own first change): neither can be checked, both are refused.
	for name, pages := range map[string][][]query.ResultRow{
		"a repeat open at the move": {
			{stIns(1, "a"), stUpd(2, "a", "b"), stUpd(3, "b", "c"), stIns(11, "a")},
			{stUpd(12, "a", "b"), stUpd(13, "b", "c")},
		},
		"a first change back inside the later record": {
			{stUpd(1, "x", "a"), stUpd(2, "a", "a")},
			{stUpd(3, "a", "b"), stUpd(4, "a", "b"), stUpd(5, "b", "c")},
		},
	} {
		_, err := stFold(t, pages, true)
		if err == nil {
			t.Fatalf("%s: folded", name)
		}
		stRefused(t, err, stWhyRepeatSeam)
	}

	// The first delivery on both sides of the move, then the second: the
	// record in memory after the move starts at the third change, which the
	// first change coming back neither follows nor equals.
	pages = [][]query.ResultRow{
		{stIns(1, "new"), stUpd(2, "new", "a")},
		{stUpd(3, "a", "b"), stIns(11, "new"), stUpd(12, "new", "a"), stUpd(13, "a", "b")},
	}
	if got, err := stFold(t, pages, false); err != nil || got["1"].RowAfter["status"] != "b" {
		t.Fatalf("in memory: %v, %v", got, err)
	}
	_, err = stFold(t, pages, true)
	stRefused(t, err, stWhyInsert)
}

// A change whose event id falls inside a run already linked cannot be put in
// sequence: refused, never linked past.
func TestFoldPage_pgSameTransaction_outOfSequence(t *testing.T) {
	_, err := stFold(t, [][]query.ResultRow{{stIns(1, "new")}, {stDel(3, "new")}, {stUpd(2, "new", "new")}}, false)
	stRefused(t, err, "cannot be put in the order they were made in")
}

// Another transaction is another matter: the same two changes at two commit
// LSNs are two transactions, and a deferrable key is unique at every commit.
func TestFoldPage_pgSameTransaction_acrossTransactionsUntouched(t *testing.T) {
	ins := stIns(1, "new")
	del := pgEv(2, sameTxnLSN+1, event.EventDelete, stRow("old"), nil)
	for _, spill := range []bool{false, true} {
		got, err := stFold(t, [][]query.ResultRow{{ins}, {del}}, spill)
		if err != nil {
			t.Fatalf("spill %v: %v", spill, err)
		}
		if ev := got["1"]; ev == nil || ev.EventType != event.EventDelete {
			t.Errorf("spill %v: key 1 = %+v, want the later transaction's DELETE", spill, ev)
		}
	}
}

// MySQL and MariaDB are not touched: the same shape at one binary log
// position (a compressed transaction's row events all carry the payload
// event's coordinate) folds as it always did, and no record is kept.
func TestFoldPage_pgSameTransaction_mysqlUntouched(t *testing.T) {
	ins := pkEvent(1, 100, event.EventInsert, nil, pkImg(1, "new"))
	del := pkEvent(2, 100, event.EventDelete, pkImg(1, "old"), nil)
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	if err := foldPage([]query.ResultRow{ins, del}, "mydb", "orders", pkColsIntID(), nil, foldOptions{}, res); err != nil {
		t.Fatalf("a MySQL window was refused by the PostgreSQL rule: %v", err)
	}
	if res.pgTxn != nil {
		t.Errorf("a MySQL fold kept PostgreSQL transaction records: %v", res.pgTxn)
	}
	wantState(t, res, map[int]string{1: ""})
	// And a row with a binary log coordinate is no PostgreSQL row even in a
	// PostgreSQL fold: it belongs to no known transaction.
	if _, ok := pgKeyTxnOf(&ins, false, "", ""); ok {
		t.Error("a row with a binary log coordinate got a PostgreSQL transaction record")
	}
}

// pgImageState: equal images hash equal, and nothing else does.
func TestPGImageState(t *testing.T) {
	a := pgImageState(map[string]any{"id": "1", "v": "x", "n": nil})
	if a.Kind != pgStateRow || a != pgImageState(map[string]any{"n": nil, "v": "x", "id": "1"}) {
		t.Fatalf("the same row in another map order: %+v", a)
	}
	for name, other := range map[string]map[string]any{
		"another value":                    {"id": "1", "v": "y", "n": nil},
		"NULL against empty text":          {"id": "1", "v": "x", "n": ""},
		"NULL against the text NULL":       {"id": "1", "v": "x", "n": "NULL"},
		"a column less":                    {"id": "1", "v": "x"},
		"a column more":                    {"id": "1", "v": "x", "n": nil, "z": nil},
		"values that join the same":        {"id": "1v", "v": "x", "n": nil},
		"a value moved to the next column": {"id": "1", "v": "", "n": "x"},
		"another column name":              {"id": "1", "w": "x", "n": nil},
	} {
		if got := pgImageState(other); got == a || got.Kind != pgStateRow {
			t.Errorf("%s: %+v, want a row state that differs", name, got)
		}
	}
	for name, img := range map[string]map[string]any{
		"no image":                  nil,
		"a number":                  {"id": json.Number("1")},
		"an unchanged-TOAST marker": {"id": "1", "v": map[string]any{event.UnchangedToastKey: true}},
		"a boolean":                 {"id": "1", "v": true},
	} {
		if got := pgImageState(img); got.Kind != pgStateUnknown {
			t.Errorf("%s: %+v, want a state that cannot be compared", name, got)
		}
	}
}
