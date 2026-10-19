//go:build integration

package query

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The walk under an event-type filter answers the same page with
// idx_type_lookup (narrowed listing, branches on the type index) as without
// it (an index the background build has not reached yet), and both are the
// single statement's page.
func TestNewestWalkIntegration_typeFilterSamePageWithAndWithoutTheTypeIndex(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-4*time.Hour), 6))
	for h := -4; h <= 1; h++ { // h = 1 is p_future
		for i := range 9 {
			tbl := []string{"orders", "users", "items"}[i%3]
			et := event.EventUpdate
			switch {
			case tbl == "items" && h%2 == 0:
				et = event.EventDelete // deletes in one table, every other hour
			case i%4 == 0:
				et = event.EventInsert
			}
			putEvent(t, db, now.Add(time.Duration(h)*time.Hour+time.Duration(i)*time.Minute), tbl, et)
		}
	}
	del, ins, upd := event.EventDelete, event.EventInsert, event.EventUpdate
	since := now.Add(-3*time.Hour + 2*time.Minute)
	until := now.Add(-time.Hour + 3*time.Minute)
	recent, err := e.fetchOnce(context.Background(), Options{Limit: 40, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	sortResults(recent, "DESC")
	cursor := &EventCursor{Timestamp: recent[12].EventTimestamp, EventID: recent[12].EventID}
	cases := map[string]Options{
		"delete":                        {EventType: &del},
		"insert in a schema":            {EventType: &ins, Schema: "app"},
		"update of one table":           {EventType: &upd, Table: "users"},
		"delete of a table without any": {EventType: &del, Table: "orders"},
		"delete in a window":            {EventType: &del, Since: &since, Until: &until},
		"update, page two":              {EventType: &upd, BeforeEvent: cursor},
		"delete, table denied":          {EventType: &del, DenyTables: []SchemaTable{{"app", "users"}}},
	}
	pages := map[string][]uint64{}
	for _, withIndex := range []bool{true, false} {
		if !withIndex {
			testutil.MustExec(t, db, "ALTER TABLE binlog_events DROP INDEX idx_type_lookup")
		}
		has, err := hasTypeIndex(context.Background(), db)
		if err != nil || has != withIndex {
			t.Fatalf("hasTypeIndex = %v (%v), want %v", has, err, withIndex)
		}
		for name, o := range cases {
			o.Limit, o.Order = 5, "DESC"
			got := eventIDs(sameAsSingleStatement(t, e, o))
			if _, walked, err := fetchNewestByPartition(context.Background(), db, o); err != nil || !walked {
				t.Fatalf("%s (index %v): walked %v (%v)", name, withIndex, walked, err)
			}
			if name != "delete of a table without any" && len(got) == 0 {
				t.Fatalf("%s: the fixture must match some rows", name)
			}
			if withIndex {
				pages[name] = got
			} else if !equalIDs(pages[name], got) {
				t.Fatalf("%s: %v with the index, %v without", name, pages[name], got)
			}
		}
	}
}

func equalIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// An invisible idx_type_lookup is not forced (FORCE INDEX would answer 1176
// on every typed page): the walk runs on idx_row_lookup.
func TestNewestWalkIntegration_invisibleTypeIndexWalksWithoutIt(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-2*time.Hour), 3))
	for h := -2; h <= 0; h++ {
		putEvent(t, db, now.Add(time.Duration(h)*time.Hour), "items", event.EventDelete)
		putEvent(t, db, now.Add(time.Duration(h)*time.Hour+time.Minute), "users", event.EventUpdate)
	}
	testutil.MustExec(t, db, "ALTER TABLE binlog_events ALTER INDEX idx_type_lookup INVISIBLE")
	if has, err := hasTypeIndex(context.Background(), db); err != nil || has {
		t.Fatalf("hasTypeIndex on an invisible index = %v (%v), want false", has, err)
	}
	del := event.EventDelete
	o := Options{Limit: 2, Order: "DESC", EventType: &del}
	if got := sameAsSingleStatement(t, e, o); len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
	if _, walked, err := fetchNewestByPartition(context.Background(), db, o); err != nil || !walked {
		t.Fatalf("walked %v (%v)", walked, err)
	}
}
