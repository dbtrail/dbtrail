//go:build integration

package query

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The PostgreSQL changed-column filter against a real MySQL index, the rows
// written by the real indexer (which stores a nil changed-columns list as the
// JSON value null, not SQL NULL) plus one stored as SQL NULL by hand.
func TestChangedColumnFilterPostgresIntegration(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ts := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	nc := func() map[string]any { return map[string]any{event.NotCapturedKey: true} }
	ev := func(n int, et event.EventType, pk string, before, after map[string]any) event.Event {
		return event.Event{BinlogFile: "0/16B3748", StartPos: uint64(100 * n), EndPos: uint64(100*n + 50),
			Timestamp: ts.Add(time.Duration(n) * time.Second), Schema: "public", Table: "docs",
			EventType: et, PKValues: pk, RowBefore: before, RowAfter: after}
	}
	batch := []event.Event{
		// 1: a whole UPDATE that changed title: matches on both flavors.
		ev(1, event.EventUpdate, "1", map[string]any{"id": "1", "title": "a"}, map[string]any{"id": "1", "title": "b"}),
		// 2: an UPDATE with no earlier row: unknown, PostgreSQL only.
		ev(2, event.EventUpdate, "2", map[string]any{"id": "2", "title": nc()}, map[string]any{"id": "2", "title": "b"}),
		// 3: a no-op UPDATE at FULL: no changed columns, earlier row whole.
		ev(3, event.EventUpdate, "3", map[string]any{"id": "3", "title": "a"}, map[string]any{"id": "3", "title": "a"}),
		// 4: an INSERT.
		ev(4, event.EventInsert, "4", nil, map[string]any{"id": "4", "title": "a"}),
		// 5: a key-only DELETE: not an UPDATE, so never "may have changed".
		ev(5, event.EventDelete, "5", map[string]any{"id": "5", "title": nc()}, nil),
		// 6: an UPDATE that changed another column.
		ev(6, event.EventUpdate, "6", map[string]any{"id": "6", "body": "a"}, map[string]any{"id": "6", "body": "b"}),
		// 7: an earlier TEXT value spelled like the marker's key is not the marker.
		ev(7, event.EventUpdate, "7", map[string]any{"id": "7", "title": event.NotCapturedKey}, map[string]any{"id": "7", "title": event.NotCapturedKey}),
	}
	if _, err := indexer.New(db, 100).InsertBatch(batch); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	// 8: the same shape as 2 with changed_columns as SQL NULL.
	testutil.InsertEvent(t, db, "0/16B3748", 900, 950, ts.Add(8*time.Second).Format("2006-01-02 15:04:05"), nil,
		"public", "docs", uint8(event.EventUpdate), "8", nil,
		[]byte(`{"id": "8", "title": {"__bintrail_not_captured__": true}}`), []byte(`{"id": "8", "title": "b"}`))

	var stored string
	if err := db.QueryRow("SELECT COALESCE(JSON_TYPE(changed_columns), 'SQL NULL') FROM binlog_events WHERE pk_values = '2'").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	t.Logf("the indexer stores a partial UPDATE's changed_columns as %s", stored)

	e := New(db)
	pks := func(pg bool) []string {
		t.Helper()
		rows, err := e.Fetch(context.Background(), Options{Schema: "public", Table: "docs", ChangedColumn: "title", Limit: 100, PostgresHistory: pg})
		if err != nil {
			t.Fatalf("Fetch(pg=%v): %v", pg, err)
		}
		var out []string
		for _, r := range rows {
			out = append(out, r.PKValues)
		}
		return out
	}
	if got, want := pks(false), []string{"1"}; !slices.Equal(got, want) {
		t.Errorf("MySQL filter returned %v, want %v", got, want)
	}
	if got, want := pks(true), []string{"1", "2", "8"}; !slices.Equal(got, want) {
		t.Errorf("PostgreSQL filter returned %v, want %v", got, want)
	}

	// What Run prints for that query: the unknown rows say so.
	var b bytes.Buffer
	if _, err := e.Run(context.Background(), Options{Schema: "public", Table: "docs", ChangedColumn: "title", Limit: 100, PostgresHistory: true}, "table", &b); err != nil {
		t.Fatal(err)
	}
	t.Logf("table:\n%s", b.String())
	if strings.Count(b.String(), " unknown ") != 2 {
		t.Errorf("want two rows with unknown changed columns:\n%s", b.String())
	}
	if !strings.Contains(b.String(), "Note: 2 rows carry an earlier row with only its key") {
		t.Errorf("no note under the table:\n%s", b.String())
	}
}
