package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// --pks writes JSON through its own writer, so it needs the PostgreSQL fields
// of its own, and none of them on MySQL, where the same marker-shaped object
// is user data.
func TestWriteGroupedJSONHistoryPostgres(t *testing.T) {
	rows := []query.ResultRow{{
		EventID: 1, EventTimestamp: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC), SchemaName: "public", TableName: "docs",
		EventType: event.EventUpdate, PKValues: "2",
		RowBefore: map[string]any{"id": "2", "title": map[string]any{event.NotCapturedKey: true}},
		RowAfter:  map[string]any{"id": "2", "title": "b", "body": map[string]any{event.UnchangedToastKey: true}},
	}}
	var pg, my, plain bytes.Buffer
	if _, err := writeGroupedJSONHistory([]string{"2"}, nil, nil, rows, &pg, true); err != nil {
		t.Fatal(err)
	}
	if _, err := writeGroupedJSONHistory([]string{"2"}, nil, nil, rows, &my, false); err != nil {
		t.Fatal(err)
	}
	if _, err := writeGroupedJSON([]string{"2"}, nil, nil, rows, &plain); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{`"before_image": "key_only"`, `"unchanged_columns": [`, `"body"`} {
		if !strings.Contains(pg.String(), s) {
			t.Errorf("PostgreSQL grouped JSON lacks %s:\n%s", s, pg.String())
		}
	}
	if my.String() != plain.String() || strings.Contains(my.String(), "before_image") || strings.Contains(my.String(), "unchanged_columns") {
		t.Errorf("MySQL grouped JSON changed:\n%s", my.String())
	}
}
