//go:build integration

package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// On an index written by PostgreSQL capture, the recover_cascade tool refuses
// before fetching anything, in words for an MCP client: the `recover` tool by
// name, and the command line only as operator remediation in backticks.
func TestIntegrationRecoverCascadeTool_postgresIndexRefused(t *testing.T) {
	db, dbName := seedCascadeIndex(t)
	if _, err := db.Exec(`INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id)
		VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1)`); err != nil {
		t.Fatalf("stamp flavor=postgres: %v", err)
	}
	cs := cascadeSession(t, db, dbName)
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "recover_cascade",
		Arguments: map[string]any{"schema": dbName, "table": "parent"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	text := resultText(res)
	t.Logf("screen text: %s", text)
	if !res.IsError {
		t.Fatalf("want a tool error, got: %s", text)
	}
	for _, want := range []string{"cannot run on a PostgreSQL index yet", "The `recover` tool cannot build this undo yet",
		"`bintrail-pg recover --schema " + dbName + " --gtid <commit position> --suppress-triggers --dry-run`"} {
		if !strings.Contains(text, want) {
			t.Errorf("message missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "SET FOREIGN_KEY_CHECKS") {
		t.Errorf("no script may come back:\n%s", text)
	}
}
