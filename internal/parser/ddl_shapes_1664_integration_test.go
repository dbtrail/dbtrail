//go:build integration

package parser_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestParseFile_ddlShapesAsMySQLLogsThem is #1664 against a real binlog: the
// server writes a DDL statement as the client sent it, comments and line breaks
// included, and each of these must still arrive as a DDL event for its table.
func TestParseFile_ddlShapesAsMySQLLogsThem(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	sourceDB, sourceName := testutil.CreateTestDB(t)
	testutil.MustExec(t, sourceDB, "CREATE TABLE ddl_shapes_1664 (id INT PRIMARY KEY)")
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	statements := []struct {
		query string
		kind  parser.DDLKind
	}{
		{"/* app */ ALTER TABLE ddl_shapes_1664 ADD COLUMN c INT", parser.DDLAlterTable},
		{"ALTER\n  TABLE ddl_shapes_1664 MODIFY c BIGINT", parser.DDLAlterTable},
		{"-- migration\nALTER TABLE ddl_shapes_1664 ADD COLUMN d INT", parser.DDLAlterTable},
		{"TRUNCATE /* nightly */ TABLE ddl_shapes_1664", parser.DDLTruncateTable},
		{"rename /* gh-ost */ table ddl_shapes_1664 to ddl_shapes_1664_old", parser.DDLRenameTable},
	}
	for _, s := range statements {
		testutil.MustExec(t, sourceDB, s.query)
	}
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cpCmd := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog))
	if out, err := cpCmd.CombinedOutput(); err != nil {
		t.Fatalf("docker cp %s failed: %v\n%s", currentBinlog, err, out)
	}

	p := parser.New(tmpDir, nil, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	events := make(chan parser.Event, 100)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		errCh <- p.ParseFile(context.Background(), currentBinlog, events)
	}()
	all := drainEvents(events)
	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile: %v", err)
	}

	// The shared container's window may hold other packages' DDL; keep ours.
	var got []parser.Event
	for _, ev := range all {
		if ev.EventType == parser.EventDDL && ev.Table == "ddl_shapes_1664" {
			got = append(got, ev)
		}
	}
	if len(got) != len(statements) {
		t.Fatalf("got %d DDL events for the table, want %d: %+v", len(got), len(statements), got)
	}
	for i, s := range statements {
		if got[i].DDLType != s.kind || got[i].Schema != sourceName {
			t.Errorf("statement %q arrived as %s on %q.%q", s.query, got[i].DDLType, got[i].Schema, got[i].Table)
		}
		if got[i].DDLQuery != s.query {
			t.Errorf("the binlog carries %q, not the statement as sent %q; the shape under test was not logged", got[i].DDLQuery, s.query)
		}
	}
}
