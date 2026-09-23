//go:build integration

package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationStatusToolNamesUncapturedTables (#1802): the MCP status tool
// carries the same list the Overview shows, with each fix, and a table the
// surface's deny rules withhold is counted, never named, like its rows are
// withheld from the query tool.
func TestIntegrationStatusToolNamesUncapturedTables(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	source, src := testutil.CreateTestDB(t)
	index, indexName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, index, 4, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := indexer.EnsureSchema(index); err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, source, "CREATE TABLE orders (id INT PRIMARY KEY) ENGINE=InnoDB")
	testutil.MustExec(t, source, "CREATE TABLE audit_log (v INT) ENGINE=InnoDB")
	testutil.MustExec(t, source, "CREATE TABLE secret_log (v INT) ENGINE=InnoDB")
	if _, err := metadata.TakeSnapshotExcludingInvalid(source, index, []string{src}); err != nil {
		t.Fatal(err)
	}

	call := func(deny []query.SchemaTable) string {
		cfg := Config{Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: index, DBName: indexName, DenyTables: deny}, nil
		}}
		res, _, err := MakeStatusTool(cfg)(ctx, &mcp.CallToolRequest{}, StatusArgs{})
		if err != nil || res.IsError {
			t.Fatalf("status tool: %v %s", err, resultText(res))
		}
		return resultText(res)
	}

	// The tool answers from an index whose capture it does not run, so it
	// names what was left out and claims no coverage count (nothing records
	// the per-table filter capture may run with).
	out := call(nil)
	for _, want := range []string{
		src + ".audit_log is not captured: no primary key.",
		"ALTER TABLE `" + src + "`.`audit_log` ADD COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;",
		src + ".secret_log is not captured: no primary key.",
		"is set where capture runs",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status tool output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Capturing") {
		t.Errorf("the tool claimed a coverage count it cannot verify:\n%s", out)
	}

	out = call([]query.SchemaTable{{Schema: strings.ToUpper(src), Table: "SECRET_LOG"}})
	if strings.Contains(out, "secret_log") {
		t.Errorf("a denied table is named by the status tool:\n%s", out)
	}
	if !strings.Contains(out, "1 table outside your access is not captured.") {
		t.Errorf("the denied table must still be counted:\n%s", out)
	}
	if !strings.Contains(out, src+".audit_log is not captured") {
		t.Errorf("the table the caller may read must stay named:\n%s", out)
	}
}
