//go:build integration

package console

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The console's /mcp query tool decides PostgreSQL history the Events view's
// way (Target.SourcePostgres), never by reading the index: a PostgreSQL boot
// server's partly captured row comes back annotated, and a MySQL one whose
// stream_state cannot be read (the table is dropped here) answers as before,
// with no flavor warning.
func TestIntegrationMCPQueryPostgresHistory(t *testing.T) {
	for _, pg := range []bool{true, false} {
		name := "mysql, unreadable flavor"
		if pg {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			srv, db, _ := seedMCPConsole(t)
			testutil.InsertEvent(t, db, "0/16B3748", 200, 250, "2026-06-01 12:10:00", nil, "app", "users", 2, "2",
				[]byte(`null`), []byte(`{"id": "2", "name": {"__bintrail_not_captured__": true}}`), []byte(`{"id": "2", "name": "bob"}`))
			if pg {
				srv.cm.boot.pgSource = true // what the startup read finds on a PostgreSQL index
			} else {
				testutil.MustExec(t, db, "DROP TABLE stream_state")
			}
			ts := httptest.NewServer(srv.Handler())
			t.Cleanup(ts.Close)
			session := mcpConnect(t, ts.URL+"/mcp", intToken)
			res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
				Name: "query", Arguments: map[string]any{"schema": "app", "table": "users", "changed_column": "name"}})
			if err != nil {
				t.Fatal(err)
			}
			text := mcpToolText(t, res)
			t.Logf("%s", text)
			if res.IsError {
				t.Fatalf("query IsError: %s", text)
			}
			if strings.Contains(text, query.FlavorUnreadableWarning) {
				t.Errorf("the console's /mcp read the index's flavor:\n%s", text)
			}
			annotated := strings.Contains(text, `"before_image": "key_only"`) && strings.Contains(text, "1 UPDATE row may have changed name")
			if annotated != pg {
				t.Errorf("annotated = %v, want %v:\n%s", annotated, pg, text)
			}
		})
	}
}
