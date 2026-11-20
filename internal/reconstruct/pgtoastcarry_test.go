package reconstruct

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// unsent is the marker of a large value an UPDATE left as it was and
// PostgreSQL did not send.
func unsent() map[string]any { return map[string]any{event.UnchangedToastKey: true} }

// row3 is a row of three columns; a nil value is a NULL, unsent() the marker.
func row3(id string, status, note any) map[string]any {
	return map[string]any{"id": id, "status": status, "note": note}
}

// upd is an UPDATE of key 1 whose earlier row was captured with its key only.
func upd(id, lsn uint64, status, note any) query.ResultRow {
	return pgEv(id, lsn, event.EventUpdate, map[string]any{"id": "1", "status": notCaptured(), "note": notCaptured()}, row3("1", status, note))
}

func ins3(id, lsn uint64, status, note any) query.ResultRow {
	return pgEv(id, lsn, event.EventInsert, nil, row3("1", status, note))
}

func del3(id, lsn uint64) query.ResultRow {
	return pgEv(id, lsn, event.EventDelete, map[string]any{"id": "1", "status": notCaptured(), "note": notCaptured()}, nil)
}

// An unchanged large value is the one the row's newest earlier change holds,
// whatever order the changes arrive in; what no change holds stays unsent,
// and is never a NULL.
func TestPGCarry_takesTheValueOfTheNewestEarlierChange(t *testing.T) {
	type want struct {
		status, note any
		left         []string // columns still unsent
		gone         []string
	}
	u := unsent()
	for name, c := range map[string]struct {
		pages [][]query.ResultRow
		want  want
	}{
		"an insert, then an update that leaves the value": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", "big")}, {upd(2, 0x710, "t", u)}},
			want{status: "t", note: "big"}},
		"a change of the value, then an update that leaves it": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", "old")}, {upd(2, 0x710, "s", "new")}, {upd(3, 0x720, "t", u)}},
			want{status: "t", note: "new"}},
		"two updates, each leaving the value": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", "big")}, {upd(2, 0x710, "t", u)}, {upd(3, 0x720, "v", u)}},
			want{status: "v", note: "big"}},
		"two columns, each from its own change": {
			[][]query.ResultRow{{ins3(1, 0x700, "S", "N")}, {upd(2, 0x710, u, "N2")}, {upd(3, 0x720, u, u)}},
			want{status: "S", note: "N2"}},
		"in one transaction": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", "big"), upd(2, 0x700, "t", u)}},
			want{status: "t", note: "big"}},
		"a NULL is carried as a NULL": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", nil)}, {upd(2, 0x710, "t", u)}},
			want{status: "t", note: nil}},
		"an empty text is carried as an empty text": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", "")}, {upd(2, 0x710, "t", u)}},
			want{status: "t", note: ""}},
		"the last change arrives first": {
			[][]query.ResultRow{{upd(3, 0x720, "t", u)}, {ins3(1, 0x700, "s", "old")}, {upd(2, 0x710, "s", "new")}},
			want{status: "t", note: "new"}},
		"the last change arrives first, then the nearer one, then the farther": {
			[][]query.ResultRow{{upd(3, 0x720, "t", u)}, {upd(2, 0x710, "s", "new")}, {ins3(1, 0x700, "s", "old")}},
			want{status: "t", note: "new"}},
		"a nearer change arrives after the value was carried from a farther one": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", "old")}, {upd(3, 0x720, "t", u)}, {upd(2, 0x710, "s", "new")}},
			want{status: "t", note: "new"}},
		"a nearer change that leaves the value too changes nothing": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", "old")}, {upd(3, 0x720, "t", u)}, {upd(2, 0x710, "x", u)}},
			want{status: "t", note: "old"}},
		"a row deleted and inserted again": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", "first")}, {del3(2, 0x710)}, {ins3(3, 0x720, "s", "second")}, {upd(4, 0x730, "t", u)}},
			want{status: "t", note: "second"}},
		"a delete that arrives late takes the value back": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", "first")}, {upd(4, 0x730, "t", u)}, {del3(2, 0x710)}},
			want{status: "t", note: u, left: []string{"note"}, gone: []string{"note"}}},
		"and the insert after it gives the row's own": {
			[][]query.ResultRow{{ins3(1, 0x700, "s", "first")}, {upd(4, 0x730, "t", u)}, {del3(2, 0x710)}, {ins3(3, 0x720, "s", "second")}},
			want{status: "t", note: "second"}},
		"no change of the window holds it": {
			[][]query.ResultRow{{upd(2, 0x710, "t", u)}, {upd(3, 0x720, "v", u)}},
			want{status: "v", note: u, left: []string{"note"}}},
		"a later change that holds every value forgets the rest": {
			[][]query.ResultRow{{upd(2, 0x710, "t", u)}, {upd(3, 0x720, "v", "sent")}, {ins3(1, 0x700, "s", "old")}},
			want{status: "v", note: "sent"}},
	} {
		res := foldPGPages(t, pkColsIntID(), c.pages...)
		ev := res.Changes["1"]
		if ev == nil || ev.RowAfter == nil {
			t.Errorf("%s: no row for key 1", name)
			continue
		}
		if !reflect.DeepEqual(ev.RowAfter["status"], c.want.status) || !reflect.DeepEqual(ev.RowAfter["note"], c.want.note) {
			t.Errorf("%s: row %v, want status %v and note %v", name, ev.RowAfter, c.want.status, c.want.note)
		}
		left, n := res.pgResidualToast(0)
		var cols, gone []string
		if n > 0 {
			cols, gone = left[0].Columns, left[0].Gone
		}
		if !reflect.DeepEqual(cols, c.want.left) || !reflect.DeepEqual(gone, c.want.gone) {
			t.Errorf("%s: still unsent %v (after a delete: %v), want %v (%v)", name, cols, gone, c.want.left, c.want.gone)
		}
	}
}

// The page a fold is handed is left as it was: the row a value is carried
// into is the fold's own.
func TestPGCarry_leavesThePageAsItWas(t *testing.T) {
	page := []query.ResultRow{ins3(1, 0x700, "s", "big"), upd(2, 0x710, "t", unsent())}
	res := foldPGPages(t, pkColsIntID(), page)
	if got := res.Changes["1"].RowAfter["note"]; got != "big" {
		t.Fatalf("the fold's row holds %v", got)
	}
	if !event.IsUnchangedToastMarker(page[1].RowAfter["note"]) {
		t.Fatalf("the page's row was changed: %v", page[1].RowAfter)
	}
}

// Inside one transaction a row left with an unsent value links to what
// follows it as a row does.
func TestPGCarry_linksInsideOneTransaction(t *testing.T) {
	res := foldPGPages(t, pkColsIntID(), []query.ResultRow{
		ins3(1, 0x700, "s", "big"), upd(2, 0x700, "t", unsent()), del3(3, 0x700),
	})
	if ev := res.Changes["1"]; ev == nil || ev.EventType != event.EventDelete {
		t.Fatalf("key 1 is %+v, want it deleted", ev)
	}
	if _, n := res.pgResidualToast(0); n != 0 {
		t.Fatalf("%d rows still unsent after the delete", n)
	}
}

// A fold whose changes moved to disk refuses an unsent value: where a value
// was carried from is not kept there.
func TestPGCarry_refusesOnceTheChangesAreOnDisk(t *testing.T) {
	for name, pages := range map[string][][]query.ResultRow{
		"the value comes after the move": {{ins3(1, 0x700, "s", "big")}, {upd(2, 0x710, "t", unsent())}},
		"the value came before the move": {{upd(2, 0x710, "t", unsent())}, {ins3(3, 0x720, "s", "x")}},
	} {
		res := &foldResult{Changes: map[string]*query.ResultRow{}}
		var err error
		for i, page := range pages {
			if err = foldPage(page, "mydb", "orders", pkColsIntID(), nil, foldOptions{pg: true}, res); err != nil {
				break
			}
			if i == 0 {
				if err = res.admit(0, 1, 1, true); err != nil {
					t.Fatal(err)
				}
				if res.Spill == nil {
					t.Fatal("the changes did not move to disk")
				}
				t.Cleanup(func() { res.Spill.Close() })
			}
		}
		if err == nil {
			err = res.pgTxnEnd("mydb", "orders")
		}
		if err == nil || !strings.Contains(err.Error(), "too many to hold in memory") {
			t.Errorf("%s: %v; want the refusal of an unsent value among changes on disk", name, err)
		}
	}
}

// A fresh-mode tail carries the value into the row it writes, also when the
// row it takes it from was already packed; and what it cannot give ends the
// tail with a refusal, never a row with a NULL.
func TestPGTail_carriesAnUnchangedLargeValue(t *testing.T) {
	dir := t.TempDir()
	base, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"}, [2]string{"2", "two"})
	state := func(tail *PGTail, file string) []string {
		t.Helper()
		return readPGState(t, baseline.TableTailStateSQL(pgPublished(base), lit(file), tail.PKColumns(), ""))
	}
	part := func(key string) map[string]any { return map[string]any{"id": key, "status": notCaptured()} }
	leaves := func(id, lsn uint64, key string) query.ResultRow {
		return pgEv(id, lsn, event.EventUpdate, part(key), map[string]any{"id": key, "status": unsent()})
	}

	tail := newTestPGTail(t, md)
	// Two pages: the row the value comes from is packed when the second arrives.
	if err := tail.Apply([]query.ResultRow{pgEv(10, 0x700, event.EventUpdate, part("1"), pgImg("1", "uno"))}); err != nil {
		t.Fatal(err)
	}
	if err := tail.Apply([]query.ResultRow{leaves(11, 0x710, "1")}); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(dir, "1-orders.parquet")
	if _, err := tail.WriteFile(first); err != nil {
		t.Fatal(err)
	}
	if got, want := state(tail, first), []string{"1=uno", "2=two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
	// A nearer change that arrives late replaces the value in the packed row.
	if err := tail.Apply([]query.ResultRow{pgEv(12, 0x708, event.EventUpdate, part("1"), pgImg("1", "eins"))}); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(dir, "2-orders.parquet")
	if _, err := tail.WriteFile(second); err != nil {
		t.Fatal(err)
	}
	if got, want := state(tail, second), []string{"1=eins", "2=two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}

	// A row whose value only the copy holds: the tail refuses, and writes nothing.
	alone := newTestPGTail(t, md)
	if err := alone.Apply([]query.ResultRow{leaves(20, 0x700, "2")}); err != nil {
		t.Fatal(err)
	}
	third := filepath.Join(dir, "3-orders.parquet")
	_, err := alone.WriteFile(third)
	var refusal *TailRefusal
	if !asTailRefusal(err, &refusal) || !strings.Contains(refusal.Reason, "as it was") {
		t.Fatalf("a value only the copy holds: %v; want a refusal", err)
	}

	// After a prune the change the value came from is gone: the next change
	// that leaves it has no source, and is refused, not given the old value.
	pruned := newTestPGTail(t, md)
	if err := pruned.Apply([]query.ResultRow{pgEv(30, 0x700, event.EventUpdate, part("1"), pgImg("1", "uno"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := pruned.PruneThrough(PGTailPos(0x708), 0); err != nil {
		t.Fatal(err)
	}
	if err := pruned.Apply([]query.ResultRow{leaves(31, 0x710, "1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := pruned.WriteFile(filepath.Join(dir, "4-orders.parquet")); !asTailRefusal(err, &refusal) {
		t.Fatalf("a value whose source was pruned: %v; want a refusal", err)
	}
}
