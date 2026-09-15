package reconstruct

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// spillOf writes drains into a fresh spill, in order, and finishes it.
func spillOf(t *testing.T, limit int64, drains ...map[string]*query.ResultRow) *changeSpill {
	t.Helper()
	s, err := newChangeSpill(limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.remove() })
	for _, d := range drains {
		if err := s.drain(d); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	return s
}

// loadAll reads every bucket back and checks each entry sits in its own bucket.
func loadAll(t *testing.T, s *changeSpill) map[string]*query.ResultRow {
	t.Helper()
	all := map[string]*query.ResultRow{}
	for b := range spillBuckets {
		m, err := s.load(b)
		if err != nil {
			t.Fatalf("load bucket %d: %v", b, err)
		}
		for pk, ev := range m {
			if spillBucket(pk) != b {
				t.Fatalf("pk %q read from bucket %d, belongs to %d", pk, b, spillBucket(pk))
			}
			all[pk] = ev
		}
	}
	return all
}

// Every value a decoded row image can hold comes back with its exact type and
// value: a number keeps its literal, an empty BLOB stays an empty BLOB (not
// NULL), an empty JSON array or object stays empty (not null).
func TestChangeSpill_roundTripsEveryImageValue(t *testing.T) {
	upd := &query.ResultRow{EventType: event.EventUpdate, EventID: 42, PKValues: `7\|a\\b`, RowAfter: map[string]any{
		"big":        json.Number("123456789012345678901234567890.000000001"),
		"neg_zero":   json.Number("-0"),
		"text":       "héllo\x00",
		"empty_text": "",
		"blob":       []byte{0, 1, 255},
		"empty_blob": []byte{},
		"yes":        true,
		"no":         false,
		"null":       nil,
		"doc": map[string]any{
			"a": []any{json.Number("1"), "x", nil, map[string]any{}, []any{}},
			"e": map[string]any{},
		},
		"empty_list": []any{},
		"empty_doc":  map[string]any{},
	}}
	del := &query.ResultRow{EventType: event.EventDelete, EventID: 43, PKValues: "8"}
	got := loadAll(t, spillOf(t, 10, map[string]*query.ResultRow{upd.PKValues: upd, del.PKValues: del}))

	if !reflect.DeepEqual(got[upd.PKValues], upd) {
		t.Errorf("update changed on the way through the spill:\n got %#v\nwant %#v", got[upd.PKValues], upd)
	}
	if !reflect.DeepEqual(got[del.PKValues], del) {
		t.Errorf("delete changed on the way through the spill:\n got %#v\nwant %#v", got[del.PKValues], del)
	}
	if len(got) != 2 {
		t.Errorf("read back %d entries, want 2", len(got))
	}
}

// Later drains win for the same row, in the order they were written, across
// every kind of event.
func TestChangeSpill_lastWriteWinsAcrossDrains(t *testing.T) {
	row := func(typ event.EventType, id uint64, pk, v string) *query.ResultRow {
		ev := &query.ResultRow{EventType: typ, EventID: id, PKValues: pk}
		if typ != event.EventDelete {
			ev.RowAfter = map[string]any{"v": v}
		}
		return ev
	}
	got := loadAll(t, spillOf(t, 10,
		map[string]*query.ResultRow{"1": row(event.EventUpdate, 1, "1", "a"), "2": row(event.EventUpdate, 2, "2", "a")},
		map[string]*query.ResultRow{"1": row(event.EventDelete, 3, "1", ""), "2": row(event.EventDelete, 4, "2", "")},
		map[string]*query.ResultRow{"1": row(event.EventInsert, 5, "1", "c")},
	))
	if ev := got["1"]; ev == nil || ev.EventType != event.EventInsert || ev.RowAfter["v"] != "c" || ev.EventID != 5 {
		t.Errorf("row 1 = %+v, want the INSERT written last", ev)
	}
	if ev := got["2"]; ev == nil || ev.EventType != event.EventDelete || ev.RowAfter != nil {
		t.Errorf("row 2 = %+v, want the DELETE written last", ev)
	}
}

// A value the spill cannot bring back exactly is an error, never a silent
// change to the backup.
func TestChangeSpill_refusesAValueItCannotRoundTrip(t *testing.T) {
	s, err := newChangeSpill(10)
	if err != nil {
		t.Fatal(err)
	}
	defer s.remove()
	err = s.drain(map[string]*query.ResultRow{"1": {EventType: event.EventUpdate, PKValues: "1",
		RowAfter: map[string]any{"when": time.Now()}}})
	if err == nil {
		err = s.finish()
	}
	if err == nil {
		t.Fatal("a time.Time in a row image was written to the spill without error")
	}
}

func TestChangeSpill_removeDeletesItsFiles(t *testing.T) {
	s := spillOf(t, 10, map[string]*query.ResultRow{"1": {EventType: event.EventDelete, PKValues: "1"}})
	if _, err := os.Stat(s.dir); err != nil {
		t.Fatalf("spill directory missing before remove: %v", err)
	}
	if err := s.remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill directory survived remove: %v", err)
	}
	if err := s.remove(); err != nil {
		t.Fatalf("a second remove failed: %v", err)
	}
}

func TestSpillBucket_spreadsKeysOverEveryBucket(t *testing.T) {
	seen := map[int]bool{}
	for i := range 5000 {
		b := spillBucket(strconv.Itoa(i))
		if b < 0 || b >= spillBuckets {
			t.Fatalf("bucket %d out of range", b)
		}
		seen[b] = true
	}
	if len(seen) != spillBuckets {
		t.Fatalf("5000 keys reached %d of %d buckets", len(seen), spillBuckets)
	}
}
