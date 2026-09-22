//go:build integration

package indexer

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A DDL statement that names several tables (DROP TABLE a, b; RENAME TABLE a
// TO b) leaves one schema_changes row per table, so the destructive-DDL
// guards, which look one table up at a time, see every one of them.
func TestInsertSchemaChange_everyTableNamed(t *testing.T) {
	ddl := func(tables ...string) event.Event {
		ev := event.Event{
			BinlogFile: "binlog.000007", EndPos: 4242, Timestamp: time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC),
			EventType: event.EventDDL, DDLType: event.DDLDropTable, DDLQuery: "DROP TABLE ...",
			Schema: "shop", Table: "first",
		}
		for _, n := range tables {
			ev.DDLTables = append(ev.DDLTables, event.DDLTable{Schema: "shop", Table: n})
		}
		return ev
	}
	for _, tc := range []struct {
		name      string
		ev        event.Event
		chunkRows int
		want      []string
	}{
		{"no list: the event's own table", ddl(), 500, []string{"first"}},
		{"one name", ddl("a"), 500, []string{"a"}},
		{"several, repeats once, in order", ddl("a", "tmp", "b", "a", "tmp", "b"), 500, []string{"a", "tmp", "b"}},
		{"more than one INSERT", ddl("a", "b", "c", "d", "e"), 2, []string{"a", "b", "c", "d", "e"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			withChunkRows(t, tc.chunkRows)
			if err := InsertSchemaChange(db, tc.ev, nil); err != nil {
				t.Fatalf("InsertSchemaChange: %v", err)
			}
			got := schemaChangeRows(t, db)
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("rows %q, want %q", got, tc.want)
			}
		})
	}
}

// A failure in a later INSERT keeps the rows that landed (each is a fact of
// its own, and a guard protected on some tables beats one protected on none)
// and says how many of the tables were recorded.
func TestInsertSchemaChange_partFailureKeepsWhatLanded(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	withChunkRows(t, 2)
	ev := event.Event{
		BinlogFile: "binlog.000007", EndPos: 4242, Timestamp: time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC),
		EventType: event.EventDDL, DDLType: event.DDLDropTable, DDLQuery: "DROP TABLE ...",
		Schema: "shop", Table: "a",
		DDLTables: []event.DDLTable{
			{Schema: "shop", Table: "a"}, {Schema: "shop", Table: "b"},
			{Schema: "shop", Table: "c"}, {Schema: "shop", Table: strings.Repeat("x", 65)}, // longer than the column
		},
	}
	err := InsertSchemaChange(db, ev, nil)
	if err == nil || !strings.Contains(err.Error(), "2 of 4") {
		t.Fatalf("err = %v, want a failure saying 2 of 4 tables were recorded", err)
	}
	if got := schemaChangeRows(t, db); strings.Join(got, "|") != "a|b" {
		t.Fatalf("rows %q, want the first INSERT's a and b kept", got)
	}
}

func withChunkRows(t *testing.T, n int) {
	t.Helper()
	old := schemaChangeChunkRows
	schemaChangeChunkRows = n
	t.Cleanup(func() { schemaChangeChunkRows = old })
}

// schemaChangeRows returns the recorded table names in insertion order,
// checking every row carries the statement's own position and type.
func schemaChangeRows(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT schema_name, table_name, binlog_file, binlog_pos, ddl_type, ddl_query FROM schema_changes ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var schema, table, file, ddlType, query string
		var pos uint64
		if err := rows.Scan(&schema, &table, &file, &pos, &ddlType, &query); err != nil {
			t.Fatal(err)
		}
		if schema != "shop" || file != "binlog.000007" || pos != 4242 || ddlType != "DROP TABLE" || query != "DROP TABLE ..." {
			t.Fatalf("row for %s: %s %s:%d %s %q, want the statement's own schema, position, type and text", table, schema, file, pos, ddlType, query)
		}
		out = append(out, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
