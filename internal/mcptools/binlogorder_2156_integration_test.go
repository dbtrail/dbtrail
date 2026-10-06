//go:build integration

package mcptools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The MCP `recover` and `reconstruct` tools take a row's changes in binary log
// order (#2156): A (event 1, at 4, started 12:00:02) and then B (event 2, at
// 40, started 12:00:00: it waited on A's row lock). Guards the wiring of both
// tools; fileB in another binary log shows the refusal reaching the client.
func TestIntegrationBinlogOrder2156_tools(t *testing.T) {
	for _, tc := range []struct {
		name, fileB string
		sorted      bool
	}{
		{"one binary log", "bin.000001", true},
		{"two binary log names", "zzz-bin.000001", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "id", 1, "PRI", "int", "NO")
			testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "name", 2, "", "varchar", "YES")
			testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:02", nil, "app", "users", 2, "1",
				[]byte(`["name"]`), []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"A"}`))
			testutil.InsertEvent(t, db, tc.fileB, 40, 80, "2026-06-01 12:00:00", nil, "app", "users", 2, "1",
				[]byte(`["name"]`), []byte(`{"id":1,"name":"A"}`), []byte(`{"id":1,"name":"B"}`))
			baseDir := t.TempDir()
			writeReconstructBaseline(t, baseDir, "app", "users", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "1", "alice")
			cs := reconstructSession(t, db, dbName)

			res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "recover", Arguments: map[string]any{
				"schema": "app", "table": "users", "since": "2026-06-01 00:00:00", "until": "2026-06-02 00:00:00", "no_archive": true,
			}})
			if err != nil {
				t.Fatalf("CallTool recover: %v", err)
			}
			script := resultText(res)
			if res.IsError {
				t.Fatalf("recover returned a tool error: %s", script)
			}
			undoB, undoA := strings.Index(script, "`name` = 'A'"), strings.Index(script, "`name` = 'alice'")
			if undoB < 0 || undoA < 0 || (undoB < undoA) != tc.sorted {
				t.Fatalf("undo of B at %d, of A at %d; binlog order expected: %v\n%s", undoB, undoA, tc.sorted, script)
			}
			if strings.Contains(script, "-- WARNING: order of the changes:") == tc.sorted {
				t.Fatalf("order warning present = %v in the script:\n%s", !tc.sorted, script)
			}

			// The envelope of a response that carries no script text must
			// say it too: the header is not there to read.
			res, err = cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "recover", Arguments: map[string]any{
				"schema": "app", "table": "users", "since": "2026-06-01 00:00:00", "until": "2026-06-02 00:00:00", "no_archive": true,
				"summary_only": true,
			}})
			if err != nil || res.IsError {
				t.Fatalf("CallTool recover summary_only: %v %s", err, resultText(res))
			}
			var summary recoverResult
			if err := json.Unmarshal([]byte(resultText(res)), &summary); err != nil {
				t.Fatalf("decode summary: %v (%s)", err, resultText(res))
			}
			inEnvelope := false
			for _, w := range summary.Warnings {
				inEnvelope = inEnvelope || (strings.HasPrefix(w, "order of the changes: ") && strings.Contains(w, "different names"))
			}
			if inEnvelope == tc.sorted || summary.SQL != "" {
				t.Fatalf("order warning in the envelope = %v (sorted %v), sql %d bytes: %v", inEnvelope, tc.sorted, len(summary.SQL), summary.Warnings)
			}

			r := decodeReconstruct(t, callReconstructTool(t, cs, map[string]any{
				"schema": "app", "table": "users", "pk": "1",
				"at": "2026-06-01 13:00:00", "baseline_dir": baseDir, "history": true, "allow_gaps": true,
			}))
			var names []string
			for _, e := range r.History {
				names = append(names, fmt.Sprint(e.State["name"]))
			}
			want := "alice,B,A"
			if tc.sorted {
				want = "alice,A,B"
			}
			if strings.Join(names, ",") != want || r.EventCount != 2 {
				t.Fatalf("history = %v (event_count %d), want %s", names, r.EventCount, want)
			}
			warned := false
			for _, w := range r.Warnings {
				warned = warned || strings.HasPrefix(w, "statement_time_order: ")
			}
			if warned == tc.sorted {
				t.Fatalf("statement_time_order warning present = %v: %v", warned, r.Warnings)
			}
			// The request named its instant: a reordered answer says that the
			// cut is by statement time, in words with no command-line flag.
			cut := ""
			for _, w := range r.Warnings {
				if strings.HasPrefix(w, "cut_by_statement_time: ") {
					cut = w
				}
			}
			if (cut != "") != tc.sorted || strings.Contains(cut, "--") {
				t.Fatalf("cut_by_statement_time warning = %q (sorted %v): %v", cut, tc.sorted, r.Warnings)
			}
		})
	}
}
