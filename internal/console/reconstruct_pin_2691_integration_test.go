//go:build integration

package console

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The console's MCP reconstruct tool keeps the snapshot it reads from a
// cleanup that runs while it reads (#2691), with the console's own pin.
func TestIntegrationMCPReconstruct_aCleanupDuringTheReadKeepsItsSnapshot(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-05-01 00:00:00", "shop", "orders", "id", 1, "PRI", "int", "NO")
	dir := t.TempDir()
	old, newer := "2026-06-01T00-00-00Z", "2026-06-02T00-00-00Z"
	oldFile := pinFixtureSnapshot(t, dir, old)
	pinFixtureSnapshot(t, dir, newer)
	srv, err := New(Config{
		DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken,
		BaselineDir:  dir,
		MCPTokenPath: filepath.Join(t.TempDir(), "mcp-token.yaml"),
	})
	if err != nil {
		t.Fatal(err)
	}
	withPruneRule(t, 1, nil)
	results, readers := cleanupInsidePin(t, dir)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	session := mcpConnect(t, ts.URL+"/mcp", intToken)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "reconstruct",
		Arguments: map[string]any{"schema": "shop", "table": "orders", "pk": "1", "at": "2026-06-01T12:00:00Z"}})
	if err != nil {
		t.Fatal(err)
	}
	// The file is not Parquet: the tool fails after the pin. What is tested
	// is the snapshot under it.
	t.Logf("the tool answered (error %v): %s", res.IsError, mcpToolText(t, res))
	if !slices.Equal(*readers, []string{"the MCP reconstruct tool"}) {
		t.Fatalf("readers that pinned: %v, want the MCP reconstruct tool: it reads without a pin", *readers)
	}
	if len(*results) != 1 || (*results)[0].KeptPinned != 1 || len((*results)[0].Pruned) != 0 {
		t.Fatalf("the cleanup during the read: %+v, want the read's snapshot kept as pinned and nothing removed", *results)
	}
	if _, err := os.Stat(oldFile); err != nil {
		t.Fatalf("the snapshot the tool was on is gone: %v", err)
	}
	if got := baseline.Pinned(dir); len(got) != 0 {
		t.Fatalf("the call is over and %v is still pinned", got)
	}
}
