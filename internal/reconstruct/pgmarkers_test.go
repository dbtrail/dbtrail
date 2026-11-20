package reconstruct

import (
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// notCaptured is the marker capture stores for a column of an earlier row
// that PostgreSQL did not send.
func notCaptured() map[string]any { return map[string]any{event.NotCapturedKey: true} }

// keyOnly is an earlier row captured with its key only.
func keyOnly(id string) map[string]any { return map[string]any{"id": id, "status": notCaptured()} }

// A fold of a PostgreSQL table takes a change whose earlier row was captured
// with its key only (a table below REPLICA IDENTITY FULL): it builds the
// table from the rows after each change and never reads the rest of the
// earlier one.
func TestFoldPGPage_takesAnEarlierRowCapturedWithItsKeyOnly(t *testing.T) {
	afterOf := func(t *testing.T, res *foldResult, key string) string {
		t.Helper()
		ev, ok := res.Changes[key]
		if !ok {
			return "<no change>"
		}
		if ev.EventType == event.EventDelete || ev.RowAfter == nil {
			return "<deleted>"
		}
		s, _ := ev.RowAfter["status"].(string)
		return s
	}
	t.Run("an insert and then an update, in one transaction", func(t *testing.T) {
		res := foldPGPages(t, pkColsIntID(), []query.ResultRow{
			pgIns(1, 0x700, "1", "new"),
			pgEv(2, 0x700, event.EventUpdate, keyOnly("1"), pgImg("1", "changed")),
		})
		if got := afterOf(t, res, "1"); got != "changed" {
			t.Fatalf("row 1 is %q, want the update's row", got)
		}
	})
	t.Run("an update and then a delete, in one transaction", func(t *testing.T) {
		res := foldPGPages(t, pkColsIntID(), []query.ResultRow{
			pgEv(1, 0x700, event.EventUpdate, keyOnly("1"), pgImg("1", "changed")),
			pgEv(2, 0x700, event.EventDelete, keyOnly("1"), nil),
		})
		if got := afterOf(t, res, "1"); got != "<deleted>" {
			t.Fatalf("row 1 is %q, want it deleted", got)
		}
	})
	t.Run("whole and key-only earlier rows in one transaction", func(t *testing.T) {
		// The table was at FULL, then at DEFAULT, then at FULL again.
		res := foldPGPages(t, pkColsIntID(), []query.ResultRow{
			pgIns(1, 0x700, "1", "a"),
			pgEv(2, 0x700, event.EventUpdate, pgImg("1", "a"), pgImg("1", "b")),
			pgEv(3, 0x700, event.EventUpdate, keyOnly("1"), pgImg("1", "c")),
			pgEv(4, 0x700, event.EventUpdate, pgImg("1", "c"), pgImg("1", "d")),
		})
		if got := afterOf(t, res, "1"); got != "d" {
			t.Fatalf("row 1 is %q, want the last update's row", got)
		}
	})
	t.Run("across transactions, in any order of arrival", func(t *testing.T) {
		res := foldPGPages(t, pkColsIntID(),
			[]query.ResultRow{pgEv(5, 0x720, event.EventUpdate, keyOnly("1"), pgImg("1", "later"))},
			[]query.ResultRow{pgEv(4, 0x710, event.EventUpdate, keyOnly("1"), pgImg("1", "earlier"))},
			[]query.ResultRow{pgEv(6, 0x730, event.EventDelete, keyOnly("2"), nil)},
		)
		if afterOf(t, res, "1") != "later" || afterOf(t, res, "2") != "<deleted>" {
			t.Fatalf("row 1 is %q and row 2 %q; want the later update, and 2 deleted", afterOf(t, res, "1"), afterOf(t, res, "2"))
		}
	})
	t.Run("a change of key, its old row captured with its key only", func(t *testing.T) {
		for name, status := range map[string]any{"a value outside the key": "moved", "a NULL outside the key": nil} {
			res := foldPGPages(t, pkColsIntID(), []query.ResultRow{
				pgEv(1, 0x700, event.EventUpdate, keyOnly("1"), map[string]any{"id": "2", "status": status}),
			})
			old, moved := res.Changes["1"], res.Changes["2"]
			if old == nil || old.EventType != event.EventDelete || moved == nil || moved.EventType != event.EventInsert || moved.RowAfter["status"] != status {
				t.Fatalf("%s: key 1 is %+v and key 2 %+v; want 1 deleted and 2 inserted with its row", name, old, moved)
			}
		}
	})
	t.Run("the same transaction delivered twice", func(t *testing.T) {
		once := []query.ResultRow{
			pgIns(1, 0x700, "1", "new"),
			pgEv(2, 0x700, event.EventUpdate, keyOnly("1"), pgImg("1", "changed")),
		}
		again := []query.ResultRow{
			pgIns(3, 0x700, "1", "new"),
			pgEv(4, 0x700, event.EventUpdate, keyOnly("1"), pgImg("1", "changed")),
		}
		res := foldPGPages(t, pkColsIntID(), once, again)
		if got := afterOf(t, res, "1"); got != "changed" {
			t.Fatalf("row 1 is %q after the transaction arrived twice", got)
		}
	})
}

// What a fold of a PostgreSQL table still refuses, and what a fold of any
// other table refuses as it always did.
func TestFoldPGPage_refusesWhatItCannotBuildFrom(t *testing.T) {
	fold := func(pg bool, evs ...query.ResultRow) error {
		res := &foldResult{Changes: map[string]*query.ResultRow{}}
		return foldPage(evs, "mydb", "orders", pkColsIntID(), nil, foldOptions{pg: pg}, res)
	}
	toast := map[string]any{event.UnchangedToastKey: true}
	for name, c := range map[string]struct {
		ev   query.ResultRow
		want string
	}{
		"the row after the change has a column that was not captured": {
			pgEv(1, 0x700, event.EventUpdate, keyOnly("1"), map[string]any{"id": "1", "status": notCaptured()}), "as the change left it"},
		"the earlier row has no key": {
			pgEv(1, 0x700, event.EventUpdate, map[string]any{"id": notCaptured(), "status": notCaptured()}, pgImg("1", "x")), "key column(s) id"},
		"an unchanged large value in the row after the change": {
			pgEv(1, 0x700, event.EventUpdate, keyOnly("1"), map[string]any{"id": "1", "status": toast}), "unchanged"},
		"an unchanged large value in the earlier row": {
			pgEv(1, 0x700, event.EventUpdate, map[string]any{"id": "1", "status": toast}, pgImg("1", "x")), "unchanged"},
		"a change of key whose new row holds an unchanged large value": {
			pgEv(1, 0x700, event.EventUpdate, keyOnly("1"), map[string]any{"id": "2", "status": toast}), "unchanged"},
	} {
		err := fold(true, c.ev)
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), c.want) {
			t.Errorf("%s: %v; want a refusal that says %q", name, err, c.want)
		}
		if err != nil && strings.Contains(err.Error(), "undo") {
			t.Errorf("%s: a fold's refusal speaks of undo: %v", name, err)
		}
	}
	// Not a PostgreSQL fold: the earlier row captured with its key only is
	// refused, by the check every such fold has always run.
	err := fold(false, pgEv(1, 0x700, event.EventUpdate, keyOnly("1"), pgImg("1", "x")))
	var nc *event.NotCapturedError
	if err == nil || !errors.As(err, &nc) {
		t.Fatalf("a fold that is not PostgreSQL's took a key-only earlier row: %v", err)
	}
}

// A row known by its key alone links to a row and never to no row; and its
// hash is not that of a row that holds a NULL or an empty text there.
func TestPGRowLink_aRowKnownByItsKeyAlone(t *testing.T) {
	none := pgRowState{Kind: pgStateNone}
	row, other := pgImageState(pgImg("1", "a")), pgImageState(pgImg("1", "b"))
	partial := pgImageState(keyOnly("1"))
	if partial.Kind != pgStateRowAny {
		t.Fatalf("an earlier row captured with its key only is kind %d, want a row known by its key alone", partial.Kind)
	}
	for name, img := range map[string]map[string]any{"a NULL there": {"id": "1", "status": nil}, "an empty text there": pgImg("1", "")} {
		if s := pgImageState(img); s.Kind != pgStateRow || (s.Hi == partial.Hi && s.Lo == partial.Lo) {
			t.Errorf("a row with %s has kind %d and the hash of one captured with its key only", name, s.Kind)
		}
	}
	for name, c := range map[string]struct {
		leaves, needs pgRowState
		links         bool
	}{
		"a row, then one that starts from a row known by its key":  {row, partial, true},
		"a row known by its key, then one that starts from a row":  {partial, row, true},
		"both known by their key":                                  {partial, partial, true},
		"no row, then one that starts from a row known by its key": {none, partial, false},
		"a row known by its key, then an insert":                   {partial, none, false},
		"two different rows, both known whole":                     {row, other, false},
		"the same row, known whole":                                {row, row, true},
		"an image that cannot be compared":                         {pgRowState{Kind: pgStateUnknown}, partial, false},
		"a row known by its key, then an image that cannot be compared": {partial, pgRowState{Kind: pgStateUnknown}, false},
		"no record, then a row known by its key": {pgRowState{}, partial, false},
		"a row known by its key, then no record": {partial, pgRowState{}, false},
	} {
		if why := pgRowLink(c.leaves, c.needs); (why == "") != c.links {
			t.Errorf("%s: %q, want links=%v", name, why, c.links)
		}
	}
}

// On what the index holds today (no column marked as not captured) a
// PostgreSQL fold refuses exactly what it refused, in the same words: the
// check it runs now answers as the one it ran before.
func TestCheckPGEventMarkers_answersAsBeforeWithoutTheMarker(t *testing.T) {
	toast := map[string]any{event.UnchangedToastKey: true}
	withToast := func(cols ...string) map[string]any {
		img := map[string]any{"id": "1", "status": "s", "note": "n"}
		for _, c := range cols {
			img[c] = toast
		}
		return img
	}
	for name, ev := range map[string]query.ResultRow{
		"nothing marked":                       pgEv(1, 0x700, event.EventUpdate, pgImg("1", "a"), pgImg("1", "b")),
		"an insert":                            pgIns(1, 0x700, "1", "a"),
		"a delete":                             pgEv(1, 0x700, event.EventDelete, pgImg("1", "a"), nil),
		"an unchanged large value, row after":  pgEv(1, 0x700, event.EventUpdate, withToast(), withToast("note")),
		"an unchanged large value, row before": pgEv(1, 0x700, event.EventUpdate, withToast("status"), withToast()),
		"in both rows, different columns":      pgEv(1, 0x700, event.EventUpdate, withToast("status"), withToast("note")),
		"in the row a delete removed":          pgEv(1, 0x700, event.EventDelete, withToast("note", "status"), nil),
		"in the row an insert added":           pgEv(1, 0x700, event.EventInsert, nil, withToast("note")),
		"in the key":                           pgEv(1, 0x700, event.EventUpdate, withToast("id"), withToast()),
	} {
		was, is := checkEventToast(ev), checkPGEventMarkers(&ev, pkColsIntID())
		if (was == nil) != (is == nil) || (was != nil && was.Error() != is.Error()) {
			t.Errorf("%s: the check answers %v, and answered %v", name, is, was)
		}
	}
}

// A key of two columns: the earlier row must hold both to be filed.
func TestCheckPGEventMarkers_aKeyOfTwoColumns(t *testing.T) {
	after := map[string]any{"a": "1", "b": "2", "status": "x"}
	ev := pgEv(1, 0x700, event.EventUpdate, map[string]any{"a": "1", "b": "2", "status": notCaptured()}, after)
	if err := checkPGEventMarkers(&ev, pkColsIntTwo()); err != nil {
		t.Fatalf("an earlier row with its whole key: %v", err)
	}
	for _, typ := range []event.EventType{event.EventUpdate, event.EventDelete} {
		ev := pgEv(1, 0x700, typ, map[string]any{"a": "1", "b": notCaptured(), "status": notCaptured()}, after)
		if typ == event.EventDelete {
			ev.RowAfter = nil
		}
		err := checkPGEventMarkers(&ev, pkColsIntTwo())
		if err == nil || !strings.Contains(err.Error(), "key column(s) b") || !strings.Contains(err.Error(), `ALTER TABLE "mydb"."orders" REPLICA IDENTITY FULL`) {
			t.Errorf("%s with half its key: %v", typ, err)
		}
	}
}

// A fresh-mode tail takes changes whose earlier row was captured with its
// key only, and writes the rows they leave.
func TestPGTail_takesAnEarlierRowCapturedWithItsKeyOnly(t *testing.T) {
	dir := t.TempDir()
	base, md := writePGTailBase(t, dir, nil, [2]string{"1", "one"}, [2]string{"2", "two"})
	tail := newTestPGTail(t, md)
	if err := tail.Apply([]query.ResultRow{
		pgEv(10, 0x700, event.EventUpdate, keyOnly("1"), pgImg("1", "uno")),
		pgEv(11, 0x700, event.EventUpdate, keyOnly("1"), pgImg("1", "eins")),
		pgEv(12, 0x710, event.EventDelete, keyOnly("2"), nil),
		pgIns(13, 0x720, "3", "three"),
		pgEv(14, 0x720, event.EventUpdate, keyOnly("3"), pgImg("3", "drei")),
	}); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "1-orders.parquet")
	if _, err := tail.WriteFile(file); err != nil {
		t.Fatal(err)
	}
	if got, want := readPGState(t, baseline.TableTailStateSQL(pgPublished(base), lit(file), tail.PKColumns(), "")), []string{"1=eins", "3=drei"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
}
