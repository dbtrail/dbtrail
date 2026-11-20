//go:build integration

package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The MCP query tool over a PostgreSQL index: rows of a table that was not at
// REPLICA IDENTITY FULL carry before_image / unchanged_columns, on the fast
// path (no filter) as on the merge path (a changed_column filter, which also
// returns the UPDATE whose changed columns are unknown and says so in a note).
// On a MySQL index the same stored rows answer exactly as before.
func TestQueryToolPostgresHistory(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	for _, flavor := range []string{"postgres", "mysql"} {
		t.Run(flavor, func(t *testing.T) {
			db, _ := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatalf("EnsureSchema: %v", err)
			}
			testutil.MustExec(t, db, "INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id) VALUES (1, 'position', ?, NOW(), 1)", flavor)
			ts := "2026-11-20 10:00:00"
			testutil.InsertEvent(t, db, "0/16B3748", 100, 150, ts, nil, "public", "docs", 2, "1",
				[]byte(`["title"]`), []byte(`{"id": "1", "title": "a"}`), []byte(`{"id": "1", "title": "b"}`))
			testutil.InsertEvent(t, db, "0/16B3748", 200, 250, ts, nil, "public", "docs", 2, "2",
				[]byte(`null`), []byte(`{"id": "2", "title": {"__bintrail_not_captured__": true}}`),
				[]byte(`{"id": "2", "title": "b", "body": {"__bintrail_unchanged_toast__": true}}`))
			cfg := Config{Resolve: func(context.Context, string) (*Target, error) { return &Target{DB: db}, nil }}
			call := func(args QueryArgs) string {
				t.Helper()
				res, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{}, args)
				if err != nil {
					t.Fatalf("query: %v", err)
				}
				return resultText(res)
			}
			all := call(QueryArgs{Schema: "public", Table: "docs", NoArchive: true})
			filtered := call(QueryArgs{Schema: "public", Table: "docs", ChangedColumn: "title", NoArchive: true})
			table := call(QueryArgs{Schema: "public", Table: "docs", ChangedColumn: "title", Format: "table", NoArchive: true})
			t.Logf("all:\n%s\nfiltered:\n%s\ntable:\n%s", all, filtered, table)
			note := "Note: 1 UPDATE row may have changed title:"
			if flavor == "mysql" {
				for _, s := range []string{all, filtered, table} {
					if strings.Contains(s, "before_image") || strings.Contains(s, "unchanged_columns") || strings.Contains(s, note) || strings.Contains(s, "unknown") {
						t.Errorf("a MySQL index was annotated:\n%s", s)
					}
				}
				if strings.Contains(filtered, `"pk_values": "2"`) {
					t.Errorf("the MySQL filter returned the row with no changed columns:\n%s", filtered)
				}
				return
			}
			for name, s := range map[string]string{"fast path": all, "filtered": filtered} {
				if !strings.Contains(s, `"before_image": "key_only"`) || !strings.Contains(s, `"unchanged_columns": [`) {
					t.Errorf("%s lacks the PostgreSQL fields:\n%s", name, s)
				}
			}
			if !strings.Contains(filtered, `"pk_values": "2"`) || !strings.Contains(filtered, note) {
				t.Errorf("the PostgreSQL filter did not return or flag the unknown UPDATE:\n%s", filtered)
			}
			if strings.Contains(all, note) {
				t.Errorf("a note without a changed_column filter:\n%s", all)
			}
			if !strings.Contains(table, " unknown ") || !strings.Contains(table, note) || strings.Contains(table, "--") {
				t.Errorf("table format:\n%s", table)
			}
		})
	}
}
